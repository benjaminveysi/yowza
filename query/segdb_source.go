package query

import (
	"context"
	"fmt"
	"sync"

	yowza "github.com/yowza/yowza"
)

// GATE 1 note: the epoch coordinators below are the DIRECT-Open
// legacy path (kept so existing direct-Open tests keep their
// semantics). The engine no longer uses them: engine.go creates
// one Session per Execute (query/session.go), and Open honours
// a session in the context instead of joining an epoch. That is
// what fixes concurrent queries sharing one sequence: each
// Execute gets its own session, its own sequence, and its own
// lease, held to the end of that execution.
//
// SegDBSource adapts the layered SegDB (hot ART + immutable segments +
// manifest/directory) to the query Source contract. It replaces
// YowzaDBSource for production queries: unlike that adapter, it reads
// through the layered path, so segment-layer tombstones are visible
// and a flushed-then-deleted key does not resurface.
//
// Shared snapshot for JOINs (deep-integration part 1). The engine
// (engine.go, not this file) opens the left table, then the right
// table, and the Source contract carries no query id — so the
// sharing lives here, in a per-SegDB epoch coordinator:
//   - The first Open for a SegDB in an epoch takes one sequence
//     with SnapshotSeq and starts the epoch (refcount 1).
//   - Every later Open for the same *SegDB while the epoch is
//     active reuses that same sequence instead of taking a fresh
//     one, so a write committed between the two Opens is
//     invisible to the second table exactly as it is to the
//     first. A multi-table query therefore reads every table at
//     one sequence, and JOIN snapshot semantics are no longer
//     per-source.
//   - Each iterator's Close releases one reference; when the last
//     iterator closes, the epoch ends and the next Open takes a
//     fresh sequence.
//
// The coordinator is keyed by the *SegDB pointer: JOIN sharing
// requires both table sources to reference the same *SegDB value
// (the normal construction — two SegDBSource structs over one
// SegDB share it; two distinct SegDB values over one wasm module
// are treated as different databases, which matches the
// one-SegDB-per-module rule in segdb.go).
//
// Honest limits of this design, all consequences of the missing
// query id rather than of the snapshot mechanism:
//   - Concurrent queries over one SegDB share the earliest active
//     epoch's sequence until all its iterators close. That is
//     still a valid snapshot (snapshot isolation permits reading
//     an older committed state), just not necessarily the
//     freshest one; a query that starts while another query's
//     epoch is active reads the active epoch's sequence.
//   - An iterator that is never Closed pins its epoch (its
//     sequence stays the shared one). The engine always closes
//     its iterators (deferred Closes in Execute), so this bites
//     only callers that Open directly and leak the iterator.
//   - A flush, compact, or recover between two Opens does not
//     break the epoch: segment entries keep their effective
//     sequences across flush, so ScanAt at the epoch sequence
//     still sees the same rows. The one exception is compact
//     with a tombstone barrier above the epoch sequence, which
//     is a caller error already (see segdb.h).
//
// Streaming (deep-integration part 1). Open performs NO scan: the
// iterator pulls pages on demand through SegDB.ScanPage at the
// epoch sequence, so a large table is never materialized at Open.
// The frozen-copy path survives only as a fallback for wasm
// binaries predating the scan_page export: on the first page
// call, a missing-export error switches the iterator to one
// frozen Scan at the epoch sequence (seq 0 then means "no rows",
// the literal ScanAt semantics, since a 0 epoch means the DB was
// empty when the epoch started). A binary predating the
// snapshot_seq export cannot start an epoch at all, so Open
// falls back to the old behaviour outright: one frozen Scan at
// current (seq 0) per table, i.e. per-source snapshots as before.
type SegDBSource struct {
	DB *yowza.SegDB
}

// Compile-time checks: SegDBSource is both a Source and the
// SegDBSnapshotSource defined in source.go.
var (
	_ Source              = (*SegDBSource)(nil)
	_ SegDBSnapshotSource = (*SegDBSource)(nil)
)

func (s *SegDBSource) Estimate() int64 { return -1 }

// snapshotEpoch is one shared query snapshot for one SegDB.
type snapshotEpoch struct {
	seq  uint64
	refs int
}

var (
	segEpochMu sync.Mutex
	segEpochs  = map[*yowza.SegDB]*snapshotEpoch{}
)

// SnapshotSeq returns the SegDB's current commit sequence. It is
// the SegDBSnapshotSource accessor: a query takes one sequence
// here (or, through Open, via the epoch coordinator) and reads
// every table at that sequence.
func (s *SegDBSource) SnapshotSeq(ctx context.Context) (uint64, error) {
	if s.DB == nil {
		return 0, fmt.Errorf("SegDBSource: nil database")
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return s.DB.SnapshotSeq()
}

// ScanAt returns a RawIterator over the rows of one prefix visible
// at seq: merged across hot memtables and segments, newest version
// per key winning, tombstones filtered out, writes after seq
// invisible. seq is literal (0 yields no rows); pass SnapshotSeq
// for "current". The iterator streams through the cursor pager
// and materializes nothing at ScanAt time. GATE 1: ScanAt
// registers a lease for seq (released when the iterator
// closes), so compaction cannot destroy the snapshot the
// iterator pages across — and a seq above the current sequence
// fails with a typed error (yowza.ErrSnapshotInvalid) instead
// of silently reading the current state. On a binary predating
// the snapshot_seq export there is no lease to register and the
// old unprotected behaviour is kept.
func (s *SegDBSource) ScanAt(ctx context.Context, prefix string, seq uint64) (RawIterator, error) {
	if s.DB == nil {
		return nil, fmt.Errorf("SegDBSource: nil database")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if _, err := s.DB.SnapshotSeq(); err == nil {
		id, rerr := s.DB.RegisterLease(seq)
		if rerr != nil {
			return nil, rerr
		}
		return &segRawIter{
			pager: segPager{db: s.DB, prefix: prefix, seq: seq,
				lease: &pagerLease{db: s.DB, id: id, owned: true}},
		}, nil
	} else if err != yowza.ErrSegDBSnapshotSeqMissing {
		return nil, err
	}
	return &segRawIter{
		pager: segPager{db: s.DB, prefix: prefix, seq: seq},
	}, nil
}

func (s *SegDBSource) Open(ctx context.Context, b Binding) (RowIterator, error) {
	if s.DB == nil {
		return nil, fmt.Errorf("SegDBSource: nil database")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// GATE 1 session path (used by the engine): the session
	// owns one lease (and sequence) per database for the whole
	// query execution. The iterator's Close does NOT release
	// the session lease (owned=false); Session.Close does.
	if sess := sessionFromCtx(ctx); sess != nil {
		id, seq, serr := sess.segLease(s.DB)
		if serr != nil {
			return nil, serr
		}
		return &segStreamIter{
			db:      s.DB,
			binding: b,
			pager: segPager{db: s.DB, prefix: b.Prefix, seq: seq,
				lease: &pagerLease{db: s.DB, id: id, owned: false}},
			epochDB: nil,
		}, nil
	}
	// Old binary without snapshot_seq: no epoch is possible;
	// fall back to the pre-integration behaviour (frozen Scan at
	// current, per table).
	seq, err := s.DB.SnapshotSeq()
	if err != nil {
		if err == yowza.ErrSegDBSnapshotSeqMissing {
			entries, serr := s.DB.Scan(b.Prefix, 0)
			if serr != nil {
				return nil, fmt.Errorf("SegDBSource scan %q: %w", b.Prefix, serr)
			}
			rows := make([]Row, 0, len(entries))
			for _, e := range entries {
				r, merr := MaterializeRow(b, string(e.Key), e.Val)
				if merr != nil {
					return nil, merr
				}
				rows = append(rows, r)
			}
			return &sliceIterator{rows: rows}, nil
		}
		return nil, fmt.Errorf("SegDBSource snapshot seq: %w", err)
	}
	// Join the active epoch for this SegDB, or start a new one.
	// (Direct-Open legacy path; the engine uses sessions now.)
	segEpochMu.Lock()
	ep, ok := segEpochs[s.DB]
	if !ok {
		ep = &snapshotEpoch{seq: seq}
		segEpochs[s.DB] = ep
	}
	ep.refs++
	epochSeq := ep.seq
	segEpochMu.Unlock()
	// GATE 1: the epoch iterator holds its own lease for the
	// epoch sequence (released on Close), so compaction cannot
	// destroy the snapshot it pages across.
	leaseID, rerr := s.DB.RegisterLease(epochSeq)
	if rerr != nil {
		// Roll back the epoch reference taken above: the
		// iterator was never created.
		segEpochMu.Lock()
		if ep2, ok2 := segEpochs[s.DB]; ok2 {
			ep2.refs--
			if ep2.refs <= 0 {
				delete(segEpochs, s.DB)
			}
		}
		segEpochMu.Unlock()
		return nil, rerr
	}
	return &segStreamIter{
		db:      s.DB,
		binding: b,
		pager: segPager{db: s.DB, prefix: b.Prefix, seq: epochSeq,
			lease: &pagerLease{db: s.DB, id: leaseID, owned: true}},
		epochDB: s.DB,
	}, nil
}

// pagerLease is the snapshot lease a pager pages under: db +
// lease id, and whether this pager owns the lease (releasing it
// on iterator Close) or merely borrows it (a session owns it).
// A nil lease means an unprotected pager (old binaries without
// the snapshot_seq export, where no lease can exist).
type pagerLease struct {
	db    *yowza.SegDB
	id    uint64
	owned bool
}

// segPager pages raw entries from a SegDB at one fixed sequence.
// It is the single paging primitive for both iterator kinds.
//
// GATE 0 (cursor first): the pager pages through
// SegDB.ScanCursor (exclusive continuation key, next key, and
// has-more transferred on every call), NOT through ScanPage.
// The skip path survives only as a fallback for binaries
// predating the cursor export, and as the frozen path for
// binaries predating the page export — never as the main
// streaming-query path. cursorCalls / skipCalls count the
// calls actually made on each path, so a test can prove which
// path served a scan (see gate1_cursor_test.go); they are
// read by tests in this package only.
// On every page the pager checks its lease (when it has one):
// a released or expired lease fails the page with
// ErrSnapshotExpired — never partial rows.
type segPager struct {
	db     *yowza.SegDB
	prefix string
	seq    uint64
	skip   uint64
	done   bool

	// Cursor state. mode: 0 = unprobed (first page tries the
	// cursor), 1 = cursor, 2 = skip fallback.
	mode      int
	cursorKey []byte
	hasCursor bool

	cursorCalls int
	skipCalls   int

	lease *pagerLease

	useFrozen bool
	frozen    []yowza.SegDBScanEntry
	frozenIdx int
}

// nextPage returns up to limit raw entries, or an empty slice at
// EOF. It never returns both entries and an error.
func (p *segPager) nextPage(limit int) ([]yowza.SegDBScanEntry, error) {
	if p.done {
		return nil, nil
	}
	if p.lease != nil && !p.lease.db.LeaseValid(p.lease.id) {
		return nil, fmt.Errorf("%w: pager lease %d", ErrSnapshotExpired, p.lease.id)
	}
	if p.useFrozen {
		if p.frozenIdx >= len(p.frozen) {
			p.done = true
			return nil, nil
		}
		end := p.frozenIdx + limit
		if end > len(p.frozen) {
			end = len(p.frozen)
		}
		page := p.frozen[p.frozenIdx:end]
		p.frozenIdx = end
		if p.frozenIdx >= len(p.frozen) {
			p.done = true
		}
		return page, nil
	}
	if limit <= 0 {
		limit = 64
	}
	// Cursor path (probed on the first page, then pinned).
	if p.mode == 0 || p.mode == 1 {
		entries, nextKey, hasMore, err := p.db.ScanCursor(p.prefix, p.seq, p.cursorKey, p.hasCursor, uint32(limit))
		if err == nil {
			p.mode = 1
			p.cursorCalls++
			if len(entries) == 0 {
				p.done = true
				return nil, nil
			}
			if hasMore && len(nextKey) == 0 {
				return nil, fmt.Errorf("SegDBSource cursor %q: has-more set with an empty resume key", p.prefix)
			}
			p.cursorKey = append([]byte(nil), nextKey...)
			p.hasCursor = true
			if !hasMore {
				p.done = true
			}
			return entries, nil
		}
		if err != yowza.ErrSegDBScanCursorMissing {
			return nil, fmt.Errorf("SegDBSource cursor %q: %w", p.prefix, err)
		}
		// Old binary without the cursor export: fall through
		// to the skip path once, and stay there.
		p.mode = 2
	}
	p.skipCalls++
	entries, err := p.db.ScanPage(p.prefix, p.seq, p.skip, uint32(limit))
	if err != nil {
		if err == yowza.ErrSegDBScanPageMissing {
			// Old binary: freeze once at the epoch sequence.
			if p.seq == 0 {
				p.useFrozen = true
				p.frozen = nil
				p.done = true
				return nil, nil
			}
			frozen, ferr := p.db.Scan(p.prefix, p.seq)
			if ferr != nil {
				return nil, fmt.Errorf("SegDBSource scan %q: %w", p.prefix, ferr)
			}
			p.useFrozen = true
			p.frozen = frozen
			return p.nextPage(limit)
		}
		return nil, fmt.Errorf("SegDBSource scan %q: %w", p.prefix, err)
	}
	if len(entries) == 0 {
		p.done = true
		return nil, nil
	}
	p.skip += uint64(len(entries))
	return entries, nil
}

// segRawIter is the RawIterator returned by ScanAt. Close
// releases its owned lease (if any); a NextBatch after Close
// fails with ErrSnapshotExpired, never with stale rows.
type segRawIter struct {
	pager  segPager
	closed bool
}

func (it *segRawIter) NextBatch(batchSize int) ([]RawEntry, error) {
	if it.closed {
		return nil, fmt.Errorf("%w: raw iterator closed", ErrSnapshotExpired)
	}
	if batchSize <= 0 {
		batchSize = 64
	}
	var out []RawEntry
	for len(out) < batchSize {
		page, err := it.pager.nextPage(batchSize - len(out))
		if err != nil {
			return nil, err
		}
		if len(page) == 0 {
			break
		}
		for _, e := range page {
			out = append(out, RawEntry{Key: string(e.Key), Value: e.Val})
		}
	}
	return out, nil
}

func (it *segRawIter) Close() error {
	if it.closed {
		return nil
	}
	it.closed = true
	if it.pager.lease != nil && it.pager.lease.owned {
		return it.pager.lease.db.ReleaseLease(it.pager.lease.id)
	}
	return nil
}

// segStreamIter is the RowIterator returned by Open. On the
// session path (epochDB nil) it streams rows at the session
// sequence and Close releases nothing: the session owns the
// lease. On the direct-Open epoch path it streams at the epoch
// sequence and Close releases the epoch reference and its own
// lease (idempotent; the engine defers Close to the end of
// Execute, which is what bounds the epoch to one query). A
// NextBatch after Close fails with ErrSnapshotExpired.
type segStreamIter struct {
	db      *yowza.SegDB
	binding Binding
	pager   segPager
	epochDB *yowza.SegDB
	closed  bool
}

func (it *segStreamIter) NextBatch(batchSize int) ([]Row, error) {
	if it.closed {
		return nil, fmt.Errorf("%w: stream iterator closed", ErrSnapshotExpired)
	}
	if batchSize <= 0 {
		batchSize = 64
	}
	var out []Row
	for len(out) < batchSize {
		page, err := it.pager.nextPage(batchSize - len(out))
		if err != nil {
			return nil, err
		}
		if len(page) == 0 {
			break
		}
		for _, e := range page {
			r, err := MaterializeRow(it.binding, string(e.Key), e.Val)
			if err != nil {
				return nil, err
			}
			out = append(out, r)
		}
	}
	return out, nil
}

func (it *segStreamIter) Close() error {
	if it.closed {
		return nil
	}
	it.closed = true
	if it.pager.lease != nil && it.pager.lease.owned {
		_ = it.pager.lease.db.ReleaseLease(it.pager.lease.id)
	}
	if it.epochDB == nil {
		return nil
	}
	segEpochMu.Lock()
	if ep, ok := segEpochs[it.epochDB]; ok {
		ep.refs--
		if ep.refs <= 0 {
			delete(segEpochs, it.epochDB)
		}
	}
	segEpochMu.Unlock()
	return nil
}

// ---------------------------------------------------------------------------
// Multi-shard persisted source (Reliability stage item 04).
//
// PersistedMultiShardSource adapts a PersistedSegDB — the whole
// sharded, host-backed database — to the query Source contract.
// It is the query path for persisted storage; the legacy
// PersistedSegDB.Scan (collect every shard, sort at the end) is
// NOT used by any query here, because it materializes the whole
// result before the first row is consumed.
//
// STREAMING. Open materializes nothing. The iterator pulls rows
// shard by shard, page by page, through PersistedSegDB
// ScanStreaming at the epoch's snapshot vector, and materializes
// one batch at a time. The query engine's JOIN build side is
// the only retaining consumer, and it is supposed to retain
// (through its spill store); the scan side itself retains at
// most one page plus one batch.
//
// SHARED SNAPSHOT ACROSS THE WHOLE SHARD SET. One query
// execution takes ONE snapshot vector for the entire shard set,
// via the persisted epoch coordinator below (keyed by
// *PersistedSegDB, mirroring the single-SegDB epoch coordinator
// above):
//   - The first Open for a PersistedSegDB in an epoch captures
//     the vector atomically (PersistedSegDB.SnapshotVector,
//     under the database lock, so no write can straddle the
//     capture) and starts the epoch.
//   - Every later Open for the same database while the epoch
//     is active reuses that vector, so a write committed
//     between two tables' Opens is invisible to the second
//     table exactly as it is to the first, in every shard.
//   - Each iterator's Close releases one reference; the last
//     Close ends the epoch and the next Open captures fresh.
// The same honest limits as the single-SegDB coordinator apply:
// concurrent queries over one database share the earliest
// active epoch's vector (a valid, possibly older snapshot),
// and a leaked iterator pins its epoch (the engine always
// closes its iterators).
//   - Two tables in DIFFERENT PersistedSegDBs get one vector
//     per database, captured at each database's first Open.
//     There is no atomic snapshot across databases: a write
//     landing between the two databases' captures can be
//     visible in one and not the other. Cross-database queries
//     therefore have per-database snapshot isolation, not a
//     single global instant. This is a documented property of
//     the design, not a bug to be papered over: no cross-DB
//     atomic capture primitive exists in the storage layer.
//   - Shard add/remove mid-query cannot happen (the shard
//     count is fixed at open; see the persist header comment).
//     A vector from another instance is refused by
//     ScanStreaming with ErrPersistedSnapshotMismatch, and a
//     Close mid-query surfaces as ErrPersistedClosed on the
//     next page, never as a silent truncation.
//
// WHY THERE IS NO SINGLE-SEQUENCE INTERFACE HERE. This source
// deliberately does NOT implement SegDBSnapshotSource: that
// interface's SnapshotSeq is a single per-SegDB sequence, and
// no single number is a snapshot for a sharded database (the
// host WAL sequence is not a SegDB sequence for any shard).
// PersistedSnapshotSource below is the correct interface:
// SnapshotVector + ScanAtVector.
//
// QUERY EXECUTION PLANNING ACROSS SHARDS — JOIN, IN
// PARTICULAR. A query over a sharded database is planned as
// follows, and the engine already executes exactly this plan
// (engine.go: the JOIN build side is consumed fully into a
// global spill-backed hash store before the left side is
// probed):
//   - SCAN / FILTER / PROJECT / AGGREGATE / SORT / LIMIT:
//     stream each table shard-by-shard at the epoch vector;
//     operators above the scan are shard-agnostic (they see
//     one row stream per table, not one stream per shard).
//   - JOIN: BROADCAST-BUILD, never per-shard concatenation.
//     The entire right table (all shards, at the epoch vector)
//     is built into ONE global hash table keyed by the join
//     key, then the left table is streamed shard-by-shard and
//     probed against it. Hash partitioning of keys does NOT
//     co-locate related rows: a user and its order routinely
//     live in different shards, so joining shard-by-shard and
//     concatenating would silently DROP every cross-shard
//     match. The broadcast-build cost is O(|right|) build
//     memory (spilled to temp files beyond the memory budget
//     by the engine's spill store) plus O(|left|) probes, and
//     its correctness does not depend on shard placement at
//     all — which is exactly why it is the strategy. Any
//     future "partitioned join" optimization must prove it
//     repartitions (re-hashes) both sides by the join key
//     before matching; a same-shard-only match is a bug, and
//     the cross-shard join test in multishard_source_test.go
//     exists to catch it (it asserts the naive per-shard
//     concatenation result is strictly smaller than the
//     correct result).
// PlanCrossShardJoin pins this strategy in code so a future
// planner change must confront it explicitly instead of
// silently reintroducing per-shard joins.

// CrossShardJoinStrategy describes how a JOIN over sharded
// tables is executed. Strategy is currently always
// "broadcast-build" (see PlanCrossShardJoin).
type CrossShardJoinStrategy struct {
	// Strategy is the join execution strategy.
	Strategy string
	// BuildSide is which input is built into the hash table.
	BuildSide string
	// Cost states the asymptotic cost in the sizes of the
	// two inputs.
	Cost string
	// PlacementIndependent is true when correctness does not
	// depend on which shard any row lives in.
	PlacementIndependent bool
}

// PlanCrossShardJoin returns the (only) supported cross-shard
// join strategy: build the whole right table into one global
// hash table and probe it with the streamed left table. It
// takes the shard counts only to document the inputs it was
// planned over; the strategy is intentionally independent of
// them — a strategy that depended on shard placement would be
// the per-shard-concatenation bug this function exists to
// forbid.
func PlanCrossShardJoin(leftShards, rightShards int) CrossShardJoinStrategy {
	_ = leftShards
	_ = rightShards
	return CrossShardJoinStrategy{
		Strategy:             "broadcast-build",
		BuildSide:            "right",
		Cost:                 "O(|right|) build (spill-backed) + O(|left|) probes",
		PlacementIndependent: true,
	}
}

// PersistedSnapshotSource is the snapshot interface for a
// sharded persisted database: a vector, not a scalar (see
// above). Implemented by PersistedMultiShardSource.
type PersistedSnapshotSource interface {
	SnapshotVector(ctx context.Context) ([]uint64, error)
	ScanAtVector(ctx context.Context, prefix string, snapshot []uint64) (RawIterator, error)
}

// PersistedMultiShardSource adapts a PersistedSegDB to Source.
type PersistedMultiShardSource struct {
	DB *yowza.PersistedSegDB
}

var (
	_ Source                  = (*PersistedMultiShardSource)(nil)
	_ PersistedSnapshotSource = (*PersistedMultiShardSource)(nil)
)

func (s *PersistedMultiShardSource) Estimate() int64 { return -1 }

// persistedEpoch is one shared query snapshot vector for one
// PersistedSegDB.
type persistedEpoch struct {
	snapshot []uint64
	refs     int
}

var (
	persistedEpochMu sync.Mutex
	persistedEpochs  = map[*yowza.PersistedSegDB]*persistedEpoch{}
)

// SnapshotVector returns a fresh snapshot vector for the
// database (captured atomically at call time). It does NOT
// join the query epoch: Open is the epoch-joining entry point.
// Callers that need one vector shared across tables must go
// through Open, or capture one vector and pass it to
// ScanAtVector for every table themselves.
func (s *PersistedMultiShardSource) SnapshotVector(ctx context.Context) ([]uint64, error) {
	if s.DB == nil {
		return nil, fmt.Errorf("PersistedMultiShardSource: nil database")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	vec, _, err := s.DB.SnapshotVector()
	return vec, err
}

// ScanAtVector returns a RawIterator over one prefix at an
// explicit snapshot vector. The vector must match this
// database's shard count or ScanStreaming refuses it.
func (s *PersistedMultiShardSource) ScanAtVector(ctx context.Context, prefix string, snapshot []uint64) (RawIterator, error) {
	if s.DB == nil {
		return nil, fmt.Errorf("PersistedMultiShardSource: nil database")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	it, err := s.DB.ScanStreaming(prefix, snapshot)
	if err != nil {
		return nil, err
	}
	return &persistedRawIter{iter: it}, nil
}

func (s *PersistedMultiShardSource) Open(ctx context.Context, b Binding) (RowIterator, error) {
	if s.DB == nil {
		return nil, fmt.Errorf("PersistedMultiShardSource: nil database")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// GATE 1 session path (used by the engine): the session
	// owns one snapshot vector per database for the whole query
	// execution. The iterator's Close closes only its
	// PersistedScanIter; the vector lives until Session.Close.
	// NOTE (documented gap, see session.go and GATE1_NOTES.md):
	// the session cannot acquire per-shard SegDB leases from
	// this package (shards are private to the root package), so
	// a persisted session query is snapshot-consistent but NOT
	// yet compact-protected; the root-package wiring that closes
	// this gap is specified for the parent in GATE1_NOTES.md.
	if sess := sessionFromCtx(ctx); sess != nil {
		vec, verr := sess.persistedVector(s.DB)
		if verr != nil {
			return nil, verr
		}
		it, err := s.DB.ScanStreaming(b.Prefix, vec)
		if err != nil {
			return nil, err
		}
		return &persistedStreamIter{db: s.DB, binding: b, iter: it, epochDB: nil}, nil
	}
	// Join the active epoch, or capture a fresh vector. The
	// capture happens outside the epoch lock (it calls into
	// wasm); if another Open started an epoch meanwhile, the
	// fresh vector is discarded and the active one is joined,
	// so a query never reads two vectors.
	for {
		persistedEpochMu.Lock()
		if ep, ok := persistedEpochs[s.DB]; ok {
			ep.refs++
			vec := append([]uint64(nil), ep.snapshot...)
			persistedEpochMu.Unlock()
			it, err := s.DB.ScanStreaming(b.Prefix, vec)
			if err != nil {
				// Release the reference taken above: the
				// iterator was never created.
				persistedEpochMu.Lock()
				if ep2, ok2 := persistedEpochs[s.DB]; ok2 {
					ep2.refs--
					if ep2.refs <= 0 {
						delete(persistedEpochs, s.DB)
					}
				}
				persistedEpochMu.Unlock()
				return nil, err
			}
			return &persistedStreamIter{db: s.DB, binding: b, iter: it, epochDB: s.DB}, nil
		}
		persistedEpochMu.Unlock()
		vec, _, err := s.DB.SnapshotVector()
		if err != nil {
			return nil, err
		}
		persistedEpochMu.Lock()
		if ep, ok := persistedEpochs[s.DB]; ok {
			// Lost the race: join the epoch that appeared.
			ep.refs++
			persistedEpochMu.Unlock()
			it, err := s.DB.ScanStreaming(b.Prefix, ep.snapshot)
			if err != nil {
				persistedEpochMu.Lock()
				if ep2, ok2 := persistedEpochs[s.DB]; ok2 {
					ep2.refs--
					if ep2.refs <= 0 {
						delete(persistedEpochs, s.DB)
					}
				}
				persistedEpochMu.Unlock()
				return nil, err
			}
			return &persistedStreamIter{db: s.DB, binding: b, iter: it, epochDB: s.DB}, nil
		}
		persistedEpochs[s.DB] = &persistedEpoch{snapshot: vec, refs: 1}
		persistedEpochMu.Unlock()
		it, err := s.DB.ScanStreaming(b.Prefix, vec)
		if err != nil {
			persistedEpochMu.Lock()
			if ep, ok := persistedEpochs[s.DB]; ok {
				ep.refs--
				if ep.refs <= 0 {
					delete(persistedEpochs, s.DB)
				}
			}
			persistedEpochMu.Unlock()
			return nil, err
		}
		return &persistedStreamIter{db: s.DB, binding: b, iter: it, epochDB: s.DB}, nil
	}
}

// persistedRawIter adapts a PersistedScanIter to RawIterator.
type persistedRawIter struct {
	iter *yowza.PersistedScanIter
}

func (it *persistedRawIter) NextBatch(batchSize int) ([]RawEntry, error) {
	if batchSize <= 0 {
		batchSize = 64
	}
	entries, err := it.iter.NextBatch(batchSize)
	if err != nil {
		return nil, err
	}
	out := make([]RawEntry, 0, len(entries))
	for _, e := range entries {
		out = append(out, RawEntry{Key: string(e.Key), Value: e.Val})
	}
	return out, nil
}

func (it *persistedRawIter) Close() error { return it.iter.Close() }

// persistedStreamIter is the RowIterator returned by
// PersistedMultiShardSource.Open. It streams at the epoch
// vector and releases the epoch reference on Close
// (idempotent; the engine defers Close to the end of Execute,
// which is what bounds the epoch to one query).
type persistedStreamIter struct {
	db      *yowza.PersistedSegDB
	binding Binding
	iter    *yowza.PersistedScanIter
	epochDB *yowza.PersistedSegDB
	closed  bool
}

func (it *persistedStreamIter) NextBatch(batchSize int) ([]Row, error) {
	if batchSize <= 0 {
		batchSize = 64
	}
	var out []Row
	for len(out) < batchSize {
		entries, err := it.iter.NextBatch(batchSize - len(out))
		if err != nil {
			return nil, err
		}
		if len(entries) == 0 {
			break
		}
		for _, e := range entries {
			r, err := MaterializeRow(it.binding, string(e.Key), e.Val)
			if err != nil {
				return nil, err
			}
			out = append(out, r)
		}
	}
	return out, nil
}

func (it *persistedStreamIter) Close() error {
	if it.closed {
		return nil
	}
	it.closed = true
	_ = it.iter.Close()
	if it.epochDB == nil {
		// Session path: the session owns the vector; there is
		// no epoch reference to release.
		return nil
	}
	persistedEpochMu.Lock()
	if ep, ok := persistedEpochs[it.epochDB]; ok {
		ep.refs--
		if ep.refs <= 0 {
			delete(persistedEpochs, it.epochDB)
		}
	}
	persistedEpochMu.Unlock()
	return nil
}
