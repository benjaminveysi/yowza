package query

// Phase D (Timeline-v2) — TimelineSource.
//
// A TimelineSource reads the SAME Timeline's snapshot that
// the Go TimelineHandle (or a Phase B SnapshotHandle)
// owns: its snapshot, leases and pins are the timeline's
// own, not a re-read of main. It is built in two forms:
//   - NewTimelineSourceFromSnapshot(db, snap): reads
//     through a Phase B SnapshotHandle (Scan returns a
//     PersistedScanIter with its own leases at the
//     handle's vector).
//   - NewTimelineSourceFromTimelineHandle(th): reads
//     through a TimelineHandle's layered view (base
//     snapshot + staged write set), i.e. the timeline's
//     merged state — a query under the handle sees the
//     timeline's merged state, never a mix of timeline
//     and main.
//
// Main-head isolation, by construction:
//   - Open IGNORES the query session in the context.
//     The engine's session carries main's snapshot
//     vector; honouring it here would return the query
//     to the main head. A timeline query's snapshot is
//     the handle's, full stop. (Sources that ignore
//     sessions keep their own semantics — this source's
//     semantics are the handle's.)
//   - ScanAtVector accepts ONLY the handle's own
//     vector. A foreign vector (including main's
//     current vector) returns
//     ErrTimelineSourceVectorMismatch and reads
//     nothing. There is no "close enough" vector.
//   - The iterator pages the handle's scan (one batch
//     at a time); it never materialises the whole
//     timeline, and it never re-captures a snapshot
//     mid-query. A main-head advance mid-query is
//     therefore invisible by construction, and the
//     differential test proves it (advance + flush +
//     compact under a live source, same rows).
//
// Resource limits: Budget (rows, 0 = unlimited) is
// enforced by the iterator with the typed error
// ErrTimelineQueryBudget — never a fixed-array failure
// and never silent truncation. The engine's own
// Contract budgets (MaxScannedRows / MaxResultRows)
// apply on top, with their BUDGET_EXCEEDED errors.
// Multi-shard note: the underlying PersistedScanIter
// streams shard by shard (sorted within each shard);
// TimelineSource inherits the TimelineHandle scan
// order contract (see timeline_handle.go), and ORDER
// BY in the engine sorts globally. Differential tests
// compare sorted sets unless the query sorts.

import (
	"context"
	"errors"
	"fmt"

	yowza "github.com/yowza/yowza"
)

// ErrTimelineSourceVectorMismatch is returned by
// ScanAtVector for any vector that is not exactly the
// handle's own. Compare with errors.Is.
var ErrTimelineSourceVectorMismatch = errors.New("query: timeline source vector does not match the timeline handle's snapshot vector")

// ErrTimelineQueryBudget is returned when a timeline
// query exceeds the source's row Budget. Compare with
// errors.Is. It is a typed error, never truncation.
var ErrTimelineQueryBudget = errors.New("query: timeline query row budget exceeded")

// TimelineSource is a Source over one timeline handle.
type TimelineSource struct {
	snap   yowza.SnapshotHandle
	tl     *yowza.TimelineHandle
	vector []uint64
	// Budget caps rows returned by one iterator (0 =
	// unlimited). Set by the caller before Open.
	Budget int64
}

var (
	_ Source                  = (*TimelineSource)(nil)
	_ PersistedSnapshotSource = (*TimelineSource)(nil)
)

// NewTimelineSourceFromSnapshot builds a source over a
// Phase B snapshot handle. The caller owns the handle's
// lifetime (Close it after the last query); the source
// never closes it.
func NewTimelineSourceFromSnapshot(snap yowza.SnapshotHandle) (*TimelineSource, error) {
	if snap == nil {
		return nil, fmt.Errorf("query: timeline source: nil snapshot handle")
	}
	return &TimelineSource{snap: snap, vector: snap.Vector()}, nil
}

// NewTimelineSourceFromTimelineHandle builds a source
// over a TimelineHandle's layered (merged) view.
func NewTimelineSourceFromTimelineHandle(th *yowza.TimelineHandle) (*TimelineSource, error) {
	if th == nil {
		return nil, fmt.Errorf("query: timeline source: nil timeline handle")
	}
	return &TimelineSource{tl: th, vector: th.BaseVector()}, nil
}

// Vector returns a copy of the source's snapshot vector.
func (s *TimelineSource) Vector() []uint64 {
	return append([]uint64(nil), s.vector...)
}

func (s *TimelineSource) Estimate() int64 { return -1 }

func vectorsEqual(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// SnapshotVector returns the handle's own vector. It
// does NOT capture a fresh (main) vector.
func (s *TimelineSource) SnapshotVector(ctx context.Context) ([]uint64, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.Vector(), nil
}

// segEntriesIter is the common batch interface of the
// root package's SnapshotIterator and TimelineScanIter.
type segEntriesIter interface {
	NextBatch(batchSize int) ([]yowza.SegDBScanEntry, error)
	Close() error
}

// ScanAtVector returns a RawIterator over one prefix at
// the source's own vector only. Any other vector is a
// hard error, never a main-head read.
func (s *TimelineSource) ScanAtVector(ctx context.Context, prefix string, snapshot []uint64) (RawIterator, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !vectorsEqual(snapshot, s.vector) {
		return nil, fmt.Errorf("%w: got %v, handle holds %v", ErrTimelineSourceVectorMismatch, snapshot, s.vector)
	}
	var it segEntriesIter
	var err error
	if s.tl != nil {
		it, err = s.tl.Scan(prefix)
	} else {
		it, err = s.snap.Scan([]byte(prefix))
	}
	if err != nil {
		return nil, err
	}
	return &timelineRawIter{iter: it, budget: s.Budget}, nil
}

// Open freezes nothing new: the source's snapshot is
// already the handle's. The query session in ctx is
// deliberately ignored (see the file header): honouring
// it would read main's vector. The returned iterator
// streams the handle's view at the handle's vector.
func (s *TimelineSource) Open(ctx context.Context, b Binding) (RowIterator, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	raw, err := s.ScanAtVector(ctx, b.Prefix, s.vector)
	if err != nil {
		return nil, err
	}
	return &timelineRowIter{binding: b, raw: raw}, nil
}

// timelineRawIter adapts a segEntriesIter to RawIterator
// with an optional row budget (typed error, never
// truncation).
type timelineRawIter struct {
	iter   segEntriesIter
	budget int64
	seen   int64
	closed bool
}

func (it *timelineRawIter) NextBatch(batchSize int) ([]RawEntry, error) {
	if it.closed {
		return nil, fmt.Errorf("query: timeline raw iterator closed")
	}
	if batchSize <= 0 {
		batchSize = 64
	}
	entries, err := it.iter.NextBatch(batchSize)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, nil
	}
	if it.budget > 0 {
		it.seen += int64(len(entries))
		if it.seen > it.budget {
			return nil, fmt.Errorf("%w: %d rows exceed budget %d", ErrTimelineQueryBudget, it.seen, it.budget)
		}
	}
	out := make([]RawEntry, 0, len(entries))
	for _, e := range entries {
		out = append(out, RawEntry{Key: string(e.Key), Value: e.Val})
	}
	return out, nil
}

func (it *timelineRawIter) Close() error {
	if it.closed {
		return nil
	}
	it.closed = true
	return it.iter.Close()
}

// timelineRowIter materialises rows per batch from a
// timelineRawIter.
type timelineRowIter struct {
	binding Binding
	raw     RawIterator
	closed  bool
}

func (it *timelineRowIter) NextBatch(batchSize int) ([]Row, error) {
	if it.closed {
		return nil, fmt.Errorf("query: timeline row iterator closed")
	}
	if batchSize <= 0 {
		batchSize = 64
	}
	raw, err := it.raw.NextBatch(batchSize)
	if err != nil {
		return nil, err
	}
	if len(raw) == 0 {
		return nil, nil
	}
	rows := make([]Row, 0, len(raw))
	for _, e := range raw {
		r, err := MaterializeRow(it.binding, e.Key, e.Value)
		if err != nil {
			return nil, err
		}
		rows = append(rows, r)
	}
	return rows, nil
}

func (it *timelineRowIter) Close() error {
	if it.closed {
		return nil
	}
	it.closed = true
	return it.raw.Close()
}
