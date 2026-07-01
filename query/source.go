package query

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"

	yowza "github.com/yowza/yowza"
)

// Source is the adapter boundary between the query engine and storage.
// The engine never reads a KV store, an ART, or a segment directly: it
// reads Rows from a Source, in batches, through a RowIterator.
//
// Snapshot semantics are part of this contract: Open must freeze the
// view the iterator will see. Writes committed after Open must not
// appear in the iterator. A source that cannot freeze must say so in
// its documentation and must not be used where snapshot visibility is
// required; silently streaming live state is a contract violation.
//
// Current state of this boundary (honest, see TRACKB_NOTES.md):
//   - MemorySource: in-memory rows, frozen at Open. Used by tests and
//     as the reference implementation of the contract.
//   - YowzaDBSource: adapts the existing public Go API
//     (ScanEntries) to this contract. It freezes by taking one
//     ScanEntries result at Open and iterating the copy. That is a
//     true point-in-time snapshot for a single table, but it reads
//     the live ART layer, not SegDB segments: Track A has not yet
//     exposed Go bindings for SegDB, so segment-layer tombstones are
//     not visible through this adapter yet. This is a marked,
//     temporary adapter, not a parallel read path built in secret:
//     the moment SegDBSource exists, YowzaDBSource is replaced by it
//     for production queries and kept only for the legacy ART layer.
//   - SegDBSource (segdb_source.go, integration step after Tracks A+B,
//     deep-integration stage): adapts Track A's Go SegDB bindings to
//     this contract. It reads through the layered path (hot + segments,
//     tombstone wins), so it supersedes YowzaDBSource for production
//     queries. It implements SegDBSnapshotSource below: one sequence
//     per query, shared across all tables via the epoch coordinator,
//     and streaming page-based reads.
//   - SegDBSnapshotSource (below): the interface for the layered read
//     path with snapshot visibility (SnapshotSeq + ScanAt). It is
//     implemented by SegDBSource; the JOIN shared-sequence requirement
//     it once expressed as future work is now met.
type Source interface {
	// Open freezes a snapshot for the given binding (table + alias +
	// prefix + columns) and returns an iterator over its rows.
	Open(ctx context.Context, b Binding) (RowIterator, error)
	// Estimate returns a row-count estimate for the binding, or -1
	// when unknown. The planner and verifier use it only when it is
	// non-negative; an unknown estimate defers budget enforcement to
	// the engine's runtime scan cap.
	Estimate() int64
}

// RowIterator yields batches. An empty batch with a nil error means
// end of input. Iterators are single-use: after EOF they stay at EOF.
type RowIterator interface {
	NextBatch(batchSize int) ([]Row, error)
	Close() error
}

// SegDBSnapshotSource is the precise interface Track A must expose so
// the query engine can read through the layered storage path with
// snapshot visibility and tombstone-wins semantics:
//
//   - SnapshotSeq returns the current commit sequence; a query takes
//     one sequence at its start and reads everything at that sequence.
//   - ScanAt returns an iterator over the rows of one prefix visible
//     at seq: merged across hot memtables and segments, with the
//     newest version per key winning, tombstones already filtered
//     out (a tombstone at seq hides older segment versions), and
//     writes after seq invisible.
//   - Row materialization (key/value -> columns) stays in this
//     package (MaterializeRow); the source supplies raw key/value
//     pairs, not typed rows, so schema binding remains the query
//     layer's job.
//
// Implemented by SegDBSource (segdb_source.go). YowzaDBSource remains
// only for the legacy ART layer. No other read path is permitted.
type SegDBSnapshotSource interface {
	SnapshotSeq(ctx context.Context) (uint64, error)
	ScanAt(ctx context.Context, prefix string, seq uint64) (RawIterator, error)
}

// RawIterator yields raw KV pairs for one prefix at a snapshot.
type RawIterator interface {
	NextBatch(batchSize int) ([]RawEntry, error)
	Close() error
}

// RawEntry is one KV pair as supplied by a snapshot source.
type RawEntry struct {
	Key   string
	Value []byte
}

// MaterializeRow converts one KV entry into an internal row for a
// binding. Internal column names are qualified ("alias.column").
// See Column.Source documentation in schema.go for the NULL and
// type-error rules.
func MaterializeRow(b Binding, key string, value []byte) (Row, error) {
	var cols []string
	var vals []Value
	var jsonObj map[string]json.RawMessage
	jsonParsed := false
	parseJSON := func() {
		if jsonParsed {
			return
		}
		jsonParsed = true
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(value, &obj); err == nil {
			jsonObj = obj
		}
	}
	for _, col := range b.Columns {
		var v Value
		switch {
		case col.Source == "key":
			v = String(key)
		case col.Source == "suffix":
			v = String(strings.TrimPrefix(key, b.Prefix))
		case col.Source == "value":
			switch col.Type {
			case KindString:
				v = String(string(value))
			case KindBytes:
				v = Bytes(value)
			default:
				return Row{}, fmt.Errorf("materialize %s.%s: value source requires string or bytes column, got %s", b.Table, col.Name, col.Type)
			}
		case strings.HasPrefix(col.Source, "json:"):
			field := strings.TrimPrefix(col.Source, "json:")
			parseJSON()
			if jsonObj == nil {
				v = Null()
				break
			}
			raw, ok := jsonObj[field]
			if !ok || string(raw) == "null" {
				v = Null()
				break
			}
			mv, err := jsonFieldToValue(raw, col.Type)
			if err != nil {
				return Row{}, fmt.Errorf("materialize %s.%s: %v", b.Table, col.Name, err)
			}
			v = mv
		default:
			return Row{}, fmt.Errorf("materialize %s.%s: unknown source %q", b.Table, col.Name, col.Source)
		}
		cols = append(cols, b.Alias+"."+col.Name)
		vals = append(vals, v)
	}
	return Row{Cols: cols, Vals: vals}, nil
}

func jsonFieldToValue(raw json.RawMessage, typ Kind) (Value, error) {
	switch typ {
	case KindString:
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return Null(), fmt.Errorf("field is not a string")
		}
		return String(s), nil
	case KindBool:
		var bl bool
		if err := json.Unmarshal(raw, &bl); err != nil {
			return Null(), fmt.Errorf("field is not a bool")
		}
		return Bool(bl), nil
	case KindInt:
		var f float64
		if err := json.Unmarshal(raw, &f); err != nil {
			return Null(), fmt.Errorf("field is not a number")
		}
		if math.Trunc(f) != f {
			return Null(), fmt.Errorf("field number %v is not an integer", f)
		}
		return Int(int64(f)), nil
	case KindFloat:
		var f float64
		if err := json.Unmarshal(raw, &f); err != nil {
			return Null(), fmt.Errorf("field is not a number")
		}
		return Float(f), nil
	case KindBytes:
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return Null(), fmt.Errorf("field is not a string (bytes columns read JSON strings)")
		}
		return Bytes([]byte(s)), nil
	default:
		return Null(), fmt.Errorf("unsupported column type %s", typ)
	}
}

// MemorySource is the reference Source implementation: rows are
// supplied already materialized (qualified column names), and Open
// copies the current slice, so appends after Open are invisible to
// the iterator — the snapshot contract, in miniature.
type MemorySource struct {
	rows []Row
}

func NewMemorySource(rows []Row) *MemorySource {
	return &MemorySource{rows: append([]Row(nil), rows...)}
}

// Append adds rows to the source. Iterators opened before the append
// do not see them (their copy was taken at Open).
func (s *MemorySource) Append(rows ...Row) {
	s.rows = append(s.rows, rows...)
}

func (s *MemorySource) Estimate() int64 { return int64(len(s.rows)) }

func (s *MemorySource) Open(ctx context.Context, b Binding) (RowIterator, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	frozen := append([]Row(nil), s.rows...)
	return &sliceIterator{rows: frozen}, nil
}

type sliceIterator struct {
	rows []Row
	idx  int
}

func (it *sliceIterator) NextBatch(batchSize int) ([]Row, error) {
	if batchSize <= 0 {
		batchSize = 64
	}
	if it.idx >= len(it.rows) {
		return nil, nil
	}
	end := it.idx + batchSize
	if end > len(it.rows) {
		end = len(it.rows)
	}
	batch := it.rows[it.idx:end]
	it.idx = end
	return batch, nil
}

func (it *sliceIterator) Close() error { return nil }

// YowzaDBSource adapts the existing public YowzaDB API to Source.
// TEMPORARY adapter: see the Source contract above. It freezes by
// calling ScanEntries once at Open and materializing the copy; it
// reads the live ART layer, not SegDB segments, and therefore does
// not yet see segment-layer tombstones. Do not present it as the
// layered read path.
type YowzaDBSource struct {
	DB *yowza.YowzaDB
	// LimitBufferBytes is the initial scan buffer request passed to
	// ScanEntries; the underlying scan grows it dynamically when the
	// result does not fit (see ScanEntries documentation).
	LimitBufferBytes uint32
}

func (s *YowzaDBSource) Estimate() int64 { return -1 }

func (s *YowzaDBSource) Open(ctx context.Context, b Binding) (RowIterator, error) {
	if s.DB == nil {
		return nil, fmt.Errorf("YowzaDBSource: nil database")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	limit := s.LimitBufferBytes
	if limit == 0 {
		limit = 1 << 20
	}
	entries, err := s.DB.ScanEntries(b.Prefix, limit)
	if err != nil {
		return nil, fmt.Errorf("YowzaDBSource scan %q: %w", b.Prefix, err)
	}
	rows := make([]Row, 0, len(entries))
	for _, e := range entries {
		r, err := MaterializeRow(b, e.Key, e.Value)
		if err != nil {
			return nil, err
		}
		rows = append(rows, r)
	}
	return &sliceIterator{rows: rows}, nil
}
