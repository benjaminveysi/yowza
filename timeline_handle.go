package yowza

// Phase D (Timeline-v2) — VM / ISA / query execution timeline
// handle.
//
// A TimelineHandle is the execution context the task's VM,
// ISA and query layers take instead of the main head. Its
// read view is LAYERED, in exactly the Phase C
// SegDBTransaction sense:
//
//   handle view = base snapshot  +  private write set
//                 (C per-shard      (this handle only;
//                  timeline          last write per key
//                  primitives)       wins; a delete is a
//                                    tombstone, never an
//                                    absence)
//
// The base snapshot is a Phase B SnapshotHandle over the
// PersistedSegDB (standalone mode), or a TimelineManager
// timeline's committed view (manager mode, where the
// manager itself already layers its WAL overlay over its
// branch-point snapshot). Base reads therefore go through
// the SAME snapshot/pin rules Phases B/C established:
// standalone Get uses SnapshotHandle.Get (per-shard C
// ScanAt at the pinned vector), standalone Scan uses
// SnapshotHandle.Scan (ScanStreaming at the vector, with
// its own leases), and a read at a timeline snapshot
// NEVER silently falls back to the main head — the base
// handle's vector is fixed at construction, main-head
// advances after that are invisible, and a closed or
// expired base fails with a typed error, never with
// main-head rows.
//
// Per-shard delegation (the "one source of truth" rule):
// the standalone handle's base is read ONLY through the
// Phase B handle / Phase D per-shard primitives
// (SegDB.TimelineGetAt / TimelineScanCursor are the
// direct per-shard forms; SnapshotHandle.Get/Scan are
// the multi-shard forms over the same C scan paths).
// The handle does NOT maintain a second overlay copy of
// SegDB data: its write set holds only keys this handle
// itself wrote. The legacy TimelineManager overlay is
// untouched — manager mode reads the manager's view
// through its public Get/Scan, it does not re-implement
// it, and full delegation of the manager's own overlay
// to C primitives would require editing timeline.go,
// which is outside this phase's file set (see the honest
// limits in TIMELINE_V2_NOTES.md Phase D).
//
// Writes NEVER touch the main head or the base snapshot:
// Put/Delete stage into the write set only. Applying
// them is explicit and named:
//   - CommitToTimeline (manager mode): per-key manager
//     Put/DeleteKey, in key order. This is NOT group
//     atomic: the manager has no multi-op commit, so a
//     failure mid-apply leaves earlier keys applied and
//     the write set intact for retry/inspection. The
//     caller must not treat it as atomic.
//   - MergeToMain (standalone mode): the write set is
//     applied through ONE SegDBTransaction (Phase C
//     unified path: journal, facade Put/Delete with
//     host WAL + hot tombstone + flush-time history
//     transfer). On success the handle rebases: the old
//     base handle closes and a fresh snapshot is taken,
//     so the handle continues at the new main head
//     instead of reading a stale base under a merged
//     write set.
// Scan order (documented, not sorted): standalone scans
// stream the base iterator (shard-sequential, sorted
// within each shard — PersistedScanIter's order) with
// the write set overlaid per key, then append the
// handle's new keys (sorted) that the base never had.
// Manager-mode scans stream the manager's key-sorted
// view the same way. Callers needing global key order
// sort, or use ORDER BY in the query engine (which
// sorts). Differential tests compare sorted sets for
// exactly this reason; the order is deterministic, but
// it is not a global sort, and no test pretends it is.

import (
	"errors"
	"fmt"
	"sort"
	"sync"
)

// ErrTimelineHandleClosed is returned by reads/writes on a
// closed handle. Compare with errors.Is.
var ErrTimelineHandleClosed = errors.New("yowza timeline: handle closed")

// Absence is reported with the package's existing
// ErrTimelineKeyNotFound (timeline.go): the handle layer
// and the manager layer share that sentinel, so callers
// test one error for "absent from the timeline view".

type timelineWrite struct {
	val     []byte // nil when deleted
	deleted bool
}

// TimelineHandle is an execution timeline over a
// PersistedSegDB. Either base (standalone) or mgr +
// timelineID (manager mode) is set, never both. The zero
// value is unusable; use NewTimelineHandle or
// NewTimelineHandleFromManager.
type TimelineHandle struct {
	db  *PersistedSegDB
	base SnapshotHandle // standalone only; owned by this handle
	mgr *TimelineManager
	id  string // manager mode only

	mu      sync.Mutex
	writes  map[string]*timelineWrite
	closed  bool
	// baseVector is a copy of the standalone base vector
	// (or, in manager mode, the manager timeline's branch
	// vector captured at construction, for diagnostics and
	// for TimelineSource's vector-equality check).
	baseVector []uint64
}

// NewTimelineHandle captures the database's current
// snapshot as this handle's base and returns a handle
// whose writes stage privately. The caller owns Close.
func NewTimelineHandle(db *PersistedSegDB) (*TimelineHandle, error) {
	if db == nil {
		return nil, fmt.Errorf("yowza timeline: nil database")
	}
	h, err := db.AcquireSnapshot()
	if err != nil {
		return nil, err
	}
	return &TimelineHandle{db: db, base: h, writes: map[string]*timelineWrite{}, baseVector: h.Vector()}, nil
}

// NewTimelineHandleFromManager builds a handle over a
// live manager timeline's committed view. Staged writes
// sit on top of that view until CommitToTimeline; the
// manager timeline's own later writes ARE visible to the
// handle (it reads the manager live), which is the
// manager-mode contract — a frozen view needs the
// standalone constructor instead.
func NewTimelineHandleFromManager(db *PersistedSegDB, mgr *TimelineManager, timelineID string) (*TimelineHandle, error) {
	if db == nil || mgr == nil || timelineID == "" {
		return nil, fmt.Errorf("yowza timeline: nil database/manager or empty timeline id")
	}
	info, err := mgr.Info(timelineID)
	if err != nil {
		return nil, err
	}
	return &TimelineHandle{db: db, mgr: mgr, id: timelineID, writes: map[string]*timelineWrite{}, baseVector: append([]uint64(nil), info.BranchVector...)}, nil
}

// BaseVector returns a copy of the handle's base vector:
// the standalone snapshot vector, or the manager
// timeline's branch vector at construction.
func (h *TimelineHandle) BaseVector() []uint64 {
	return append([]uint64(nil), h.baseVector...)
}

// DB returns the underlying database.
func (h *TimelineHandle) DB() *PersistedSegDB { return h.db }

// TimelineID returns the manager timeline id, or "" in
// standalone mode.
func (h *TimelineHandle) TimelineID() string { return h.id }

func (h *TimelineHandle) checkOpenLocked() error {
	if h.closed {
		return ErrTimelineHandleClosed
	}
	return nil
}

func validateTimelineHandleKV(key string, val []byte) error {
	if len(key) < 1 || len(key) > 1023 {
		return fmt.Errorf("yowza timeline: key length %d out of range 1..1023", len(key))
	}
	if len(val) > 1024 {
		return fmt.Errorf("yowza timeline: value length %d exceeds 1024", len(val))
	}
	return nil
}

// Get reads the layered view: the private write set
// first (a staged delete hides the base, never falls
// through), then the base. It never reads the main head.
func (h *TimelineHandle) Get(key string) ([]byte, error) {
	h.mu.Lock()
	if err := h.checkOpenLocked(); err != nil {
		h.mu.Unlock()
		return nil, err
	}
	if w, ok := h.writes[key]; ok {
		h.mu.Unlock()
		if w.deleted {
			return nil, fmt.Errorf("%w: %s", ErrTimelineKeyNotFound, key)
		}
		return append([]byte(nil), w.val...), nil
	}
	base, mgr, id := h.base, h.mgr, h.id
	h.mu.Unlock()
	if mgr != nil {
		return mgr.Get(id, key)
	}
	// Standalone base reads delegate per shard to the
	// Phase D C primitive (TimelineGetAt): the key's
	// own shard, at the base vector, pinned by the
	// base handle's lease for that shard. A missing
	// export, an expired lease, or a future vector
	// fails here — it never falls back to the main
	// head, and it never falls back to the unpinned
	// path either, because that would silently drop
	// the pinning this layer promises.
	if sh, ok := base.(*snapshotHandle); ok {
		idx := h.db.ShardIndex(key)
		if idx < 0 || idx >= len(sh.vector) || idx >= len(sh.leaseIDs) {
			return nil, fmt.Errorf("yowza timeline: key %q routes outside the base vector", key)
		}
		return h.db.shards[idx].sdb.TimelineGetAt(key, sh.vector[idx], sh.leaseIDs[idx])
	}
	// Defensive fallback for a base handle built by a
	// caller outside this file (both constructors in
	// this file produce *snapshotHandle, so this path
	// is not taken by any handle this package makes).
	v, err := base.Get([]byte(key))
	if err != nil {
		if containsSub(err.Error(), "not found") {
			return nil, fmt.Errorf("%w: %s", ErrTimelineKeyNotFound, key)
		}
		return nil, err
	}
	return v, nil
}

func containsSub(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// Put stages a write in the private write set only. It
// touches neither the base snapshot nor the main head.
func (h *TimelineHandle) Put(key string, val []byte) error {
	if err := validateTimelineHandleKV(key, val); err != nil {
		return err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.checkOpenLocked(); err != nil {
		return err
	}
	h.writes[key] = &timelineWrite{val: append([]byte(nil), val...)}
	return nil
}

// Delete stages a tombstone in the private write set. A
// delete of a base-only key shadows the base value (the
// segdb_delete_absent rule, at handle level); a delete
// of an absent key stages a tombstone all the same, so a
// later base value for that key cannot resurface.
func (h *TimelineHandle) Delete(key string) error {
	if err := validateTimelineHandleKV(key, nil); err != nil {
		return err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.checkOpenLocked(); err != nil {
		return err
	}
	h.writes[key] = &timelineWrite{deleted: true}
	return nil
}

// TimelineStagedWrite is one drained write: a put (Val,
// Deleted false) or a tombstone (Deleted true, Val
// ignored). It is the drain unit for the Track 3 WASM
// host (timeline_wasm_host.go): the C VM serializes
// exactly this shape, and ApplyStagedWritesAtomic
// applies a drained set to this handle.
type TimelineStagedWrite struct {
	Key     string
	Val     []byte
	Deleted bool
}

// ApplyStagedWritesAtomic applies a drained write set
// atomically or not at all. Every entry is validated
// first (same key/value bounds as Put/Delete); then,
// under a single handle lock, the previous write set
// is snapshotted and the entries are applied in order
// (last write per key wins, as in the C stage merge).
// Any error restores the snapshot, so a failed drain
// leaves the handle unchanged — including its
// pre-existing staged writes, which are preserved
// unless overwritten by a successfully applied entry.
// A closed handle is rejected before any mutation.
func (h *TimelineHandle) ApplyStagedWritesAtomic(writes []TimelineStagedWrite) error {
	for _, w := range writes {
		var val []byte
		if !w.Deleted {
			val = w.Val
		}
		if err := validateTimelineHandleKV(w.Key, val); err != nil {
			return err
		}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.checkOpenLocked(); err != nil {
		return err
	}
	prev := make(map[string]*timelineWrite, len(h.writes))
	for k, w := range h.writes {
		nw := &timelineWrite{deleted: w.deleted}
		if w.val != nil {
			nw.val = append([]byte(nil), w.val...)
		}
		prev[k] = nw
	}
	for _, w := range writes {
		// Re-validated under the lock: the caller owns
		// the slice, so a concurrent mutation between
		// the first validation and this apply must not
		// half-apply. On any such failure the snapshot
		// above is restored and nothing is applied.
		var val []byte
		if !w.Deleted {
			val = w.Val
		}
		if err := validateTimelineHandleKV(w.Key, val); err != nil {
			h.writes = prev
			return err
		}
		if w.Deleted {
			h.writes[w.Key] = &timelineWrite{deleted: true}
		} else {
			h.writes[w.Key] = &timelineWrite{val: append([]byte(nil), w.Val...)}
		}
	}
	return nil
}

// WriteCount returns the staged write-set size (diagnostic).
func (h *TimelineHandle) WriteCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.writes)
}

// sortedWrites returns the write set in key order. The
// caller must not hold h.mu (it takes it).
func (h *TimelineHandle) sortedWrites() ([]string, map[string]*timelineWrite) {
	h.mu.Lock()
	defer h.mu.Unlock()
	keys := make([]string, 0, len(h.writes))
	for k := range h.writes {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	cp := make(map[string]*timelineWrite, len(h.writes))
	for k, w := range h.writes {
		cp[k] = w
	}
	return keys, cp
}

// CommitToTimeline applies the staged write set to the
// manager timeline (manager mode only), in key order.
// NOT group atomic (see the file header). On success the
// write set clears; on failure it is kept intact and the
// error names the failing key, so the caller can inspect
// and retry — earlier keys may already be applied.
func (h *TimelineHandle) CommitToTimeline() error {
	h.mu.Lock()
	if err := h.checkOpenLocked(); err != nil {
		h.mu.Unlock()
		return err
	}
	mgr, id := h.mgr, h.id
	h.mu.Unlock()
	if mgr == nil {
		return fmt.Errorf("yowza timeline: CommitToTimeline requires a manager handle (standalone handles merge with MergeToMain)")
	}
	keys, writes := h.sortedWrites()
	for _, k := range keys {
		w := writes[k]
		var err error
		if w.deleted {
			err = mgr.DeleteKey(id, k)
		} else {
			err = mgr.Put(id, k, w.val)
		}
		if err != nil {
			return fmt.Errorf("yowza timeline: commit to timeline %s failed at key %q: %w", id, k, err)
		}
	}
	h.mu.Lock()
	h.writes = map[string]*timelineWrite{}
	h.mu.Unlock()
	return nil
}

// MergeToMain applies the staged write set to the main
// head through ONE SegDBTransaction (standalone mode
// only), then rebases the handle onto a fresh snapshot.
// The transaction gives the merge its journal +
// facade-history guarantees (Phase C); conflict policy
// is last-write-wins through the transaction, with no
// merge-base resolution — a timeline merge with
// conflicts belongs to TimelineManager.Merge, not here.
func (h *TimelineHandle) MergeToMain() error {
	h.mu.Lock()
	if err := h.checkOpenLocked(); err != nil {
		h.mu.Unlock()
		return err
	}
	if h.mgr != nil {
		h.mu.Unlock()
		return fmt.Errorf("yowza timeline: MergeToMain requires a standalone handle (manager handles commit with CommitToTimeline)")
	}
	db := h.db
	h.mu.Unlock()
	keys, writes := h.sortedWrites()
	tx, err := BeginSegDBTransaction(db)
	if err != nil {
		return err
	}
	for _, k := range keys {
		w := writes[k]
		if w.deleted {
			if err := tx.Delete(k); err != nil {
				_ = tx.Rollback()
				return err
			}
		} else {
			if err := tx.Put(k, w.val, false); err != nil {
				_ = tx.Rollback()
				return err
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	// Rebase: the old base is now stale relative to the
	// merged main. Close it and snapshot fresh; the
	// write set is empty after a successful merge, so
	// no staged data is lost by the rebase.
	h.mu.Lock()
	old := h.base
	h.base = nil
	h.writes = map[string]*timelineWrite{}
	h.mu.Unlock()
	_ = old.Close()
	nb, err := db.AcquireSnapshot()
	if err != nil {
		h.mu.Lock()
		h.closed = true
		h.mu.Unlock()
		return fmt.Errorf("yowza timeline: merge succeeded but rebase snapshot failed (handle closed): %w", err)
	}
	h.mu.Lock()
	h.base = nb
	h.baseVector = nb.Vector()
	h.mu.Unlock()
	return nil
}

// Fork returns a new standalone handle pinned to the
// SAME base vector under fresh per-shard leases (the
// Phase D TimelineFork primitive, per shard), carrying
// a COPY of this handle's staged writes at fork time:
// the fork sees the parent's staged state as of the
// fork, never the parent's later staged writes. The
// parent handle is unaffected. Manager-mode handles
// refuse to fork while staged writes exist (those
// writes live only in this handle, and a manager fork
// would silently drop them); commit first, then fork
// from the manager timeline.
func (h *TimelineHandle) Fork() (*TimelineHandle, error) {
	h.mu.Lock()
	if err := h.checkOpenLocked(); err != nil {
		h.mu.Unlock()
		return nil, err
	}
	if h.mgr != nil {
		if len(h.writes) > 0 {
			h.mu.Unlock()
			return nil, fmt.Errorf("yowza timeline: manager handle has staged writes; CommitToTimeline before Fork")
		}
		mgr, id := h.mgr, h.id
		h.mu.Unlock()
		// Fork through the manager (public API): the new
		// timeline's committed view is the fork point.
		// The caller supplies the new id via ForkFrom
		// below; this path is unreachable without an id,
		// so ForkFrom is the manager-mode fork entry.
		_ = mgr
		_ = id
		return nil, fmt.Errorf("yowza timeline: manager-mode fork requires ForkFrom with a new timeline id")
	}
	db, vec := h.db, append([]uint64(nil), h.baseVector...)
	writesCp := make(map[string]*timelineWrite, len(h.writes))
	for k, w := range h.writes {
		nw := &timelineWrite{deleted: w.deleted}
		if w.val != nil {
			nw.val = append([]byte(nil), w.val...)
		}
		writesCp[k] = nw
	}
	h.mu.Unlock()
	leaseIDs, release, err := db.AcquireShardLeases(vec)
	if err != nil {
		return nil, err
	}
	// Single owner for the forked leases: the new
	// handle's snapshotHandle releases them on Close
	// (releaseShardLeasesLocked over the same C table).
	// The AcquireShardLeases release function is
	// therefore deliberately NOT called — calling it
	// too would free leases the handle still needs.
	// (It is idempotent, but idempotence is per
	// function, not across the two release paths.)
	_ = release
	db.mu.Lock()
	db.snapshotNext++
	snapID := SnapshotID(db.snapshotNext)
	gen := db.generation
	db.mu.Unlock()
	base := &snapshotHandle{db: db, id: snapID, vector: vec, generation: gen, leaseIDs: leaseIDs}
	return &TimelineHandle{db: db, base: base, writes: writesCp, baseVector: append([]uint64(nil), vec...)}, nil
}

// ForkFrom is the manager-mode fork: fork the manager
// timeline under newID and return a handle over it.
func (h *TimelineHandle) ForkFrom(newID string) (*TimelineHandle, error) {
	h.mu.Lock()
	if err := h.checkOpenLocked(); err != nil {
		h.mu.Unlock()
		return nil, err
	}
	if h.mgr == nil {
		h.mu.Unlock()
		return nil, fmt.Errorf("yowza timeline: ForkFrom requires a manager handle (standalone handles use Fork)")
	}
	if len(h.writes) > 0 {
		h.mu.Unlock()
		return nil, fmt.Errorf("yowza timeline: manager handle has staged writes; CommitToTimeline before ForkFrom")
	}
	mgr, id, db := h.mgr, h.id, h.db
	h.mu.Unlock()
	if _, err := mgr.Fork(id, newID); err != nil {
		return nil, err
	}
	return NewTimelineHandleFromManager(db, mgr, newID)
}

// Close releases the standalone base handle (its leases)
// and discards staged writes. Idempotent. Manager-mode
// handles hold no base handle, so Close only discards
// staged writes there.
func (h *TimelineHandle) Close() error {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil
	}
	h.closed = true
	base := h.base
	h.base = nil
	h.writes = map[string]*timelineWrite{}
	h.mu.Unlock()
	if base != nil {
		return base.Close()
	}
	return nil
}

// ---- scanning ----

// TimelineScanIter is the streaming iterator Scan
// returns. It is single-use and not safe for concurrent
// use.
type TimelineScanIter struct {
	h         *TimelineHandle
	prefix    string
	mgrMode   bool
	baseIter  SnapshotIterator // standalone: base snapshot scan
	mgrRows   []SegDBScanEntry // manager mode: manager view (materialised by the manager)
	mgrIdx    int
	baseBuf   []SegDBScanEntry
	baseIdx   int
	baseDone  bool
	seen      map[string]bool // base keys already emitted (so new keys append once)
	newKeys   []string        // staged put keys absent from the base, sorted
	newIdx    int
	writes    map[string]*timelineWrite
	closed    bool
}

// Scan returns a streaming iterator over the layered
// view filtered by prefix. It pages the base (one
// batch at a time, never the whole base) and overlays
// the write set per key; staged new keys append after
// the base stream, sorted. See the file header for the
// resulting order contract.
func (h *TimelineHandle) Scan(prefix string) (*TimelineScanIter, error) {
	h.mu.Lock()
	if err := h.checkOpenLocked(); err != nil {
		h.mu.Unlock()
		return nil, err
	}
	if len(prefix) > 1023 {
		h.mu.Unlock()
		return nil, fmt.Errorf("yowza timeline: scan prefix too long")
	}
	base, mgr, id := h.base, h.mgr, h.id
	writesCp := make(map[string]*timelineWrite, len(h.writes))
	for k, w := range h.writes {
		writesCp[k] = w
	}
	h.mu.Unlock()
	it := &TimelineScanIter{h: h, prefix: prefix, seen: map[string]bool{}, writes: writesCp}
	if mgr != nil {
		rows, err := mgr.Scan(id, prefix)
		if err != nil {
			return nil, err
		}
		it.mgrMode = true
		it.mgrRows = rows // may be nil for an empty view; mgrMode, not nil, selects the path
	} else {
		bi, err := base.Scan([]byte(prefix))
		if err != nil {
			return nil, err
		}
		it.baseIter = bi
	}
	// New keys: staged puts whose key has the prefix and
	// is not shadowed by a delete (a put is never
	// shadowed). Whether a new key is also a base key is
	// decided at stream time via `seen`; the list is
	// pre-sorted so the append phase is ordered.
	var nk []string
	for k, w := range writesCp {
		if w.deleted {
			continue
		}
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			nk = append(nk, k)
		}
	}
	sort.Strings(nk)
	it.newKeys = nk
	return it, nil
}

// NextBatch returns up to batchSize layered rows, or an
// empty slice at end of input. A batch never mixes an
// error with rows: on error it returns nil + the error.
func (it *TimelineScanIter) NextBatch(batchSize int) ([]SegDBScanEntry, error) {
	if it.closed {
		return nil, fmt.Errorf("%w: timeline scan iterator closed", ErrTimelineHandleClosed)
	}
	if batchSize <= 0 {
		batchSize = 64
	}
	var out []SegDBScanEntry
	for len(out) < batchSize {
		// Phase 1: base stream (manager rows, or the
		// snapshot iterator's pages), overlaid.
		var e *SegDBScanEntry
		if it.mgrMode {
			if it.mgrIdx < len(it.mgrRows) {
				e = &it.mgrRows[it.mgrIdx]
				it.mgrIdx++
			}
		} else if !it.baseDone {
			for it.baseIdx >= len(it.baseBuf) {
				page, err := it.baseIter.NextBatch(64)
				if err != nil {
					return nil, err
				}
				if len(page) == 0 {
					it.baseDone = true
					break
				}
				it.baseBuf = page
				it.baseIdx = 0
			}
			if !it.baseDone && it.baseIdx < len(it.baseBuf) {
				e = &it.baseBuf[it.baseIdx]
				it.baseIdx++
			}
		}
		if e != nil {
			k := string(e.Key)
			it.seen[k] = true
			if w, ok := it.writes[k]; ok {
				if w.deleted {
					continue // shadowed: consumes no output slot
				}
				out = append(out, SegDBScanEntry{Key: append([]byte(nil), e.Key...), Val: append([]byte(nil), w.val...)})
			} else {
				out = append(out, SegDBScanEntry{Key: append([]byte(nil), e.Key...), Val: append([]byte(nil), e.Val...)})
			}
			continue
		}
		// Phase 2: staged new keys not seen in the base.
		for it.newIdx < len(it.newKeys) {
			k := it.newKeys[it.newIdx]
			it.newIdx++
			if it.seen[k] {
				continue
			}
			w := it.writes[k]
			if w == nil || w.deleted {
				continue
			}
			out = append(out, SegDBScanEntry{Key: []byte(k), Val: append([]byte(nil), w.val...)})
			break
		}
		if len(out) == 0 && it.newIdx >= len(it.newKeys) {
			// No base row and no new key left: EOF.
			// (If out is non-empty, the loop condition
			// ends the batch; EOF is reported next call
			// as an empty batch, matching iterator
			// conventions in this codebase.)
			if it.mgrMode || it.baseDone {
				break
			}
		}
		// If phase 1 is exhausted and phase 2 produced
		// nothing new this round, the loop re-checks;
		// the break above ends it at true EOF.
		if (it.mgrMode && it.mgrIdx >= len(it.mgrRows) || it.baseDone) && it.newIdx >= len(it.newKeys) {
			break
		}
	}
	return out, nil
}

// Close releases the base iterator (standalone mode).
// Idempotent.
func (it *TimelineScanIter) Close() error {
	if it.closed {
		return nil
	}
	it.closed = true
	if it.baseIter != nil {
		return it.baseIter.Close()
	}
	return nil
}
