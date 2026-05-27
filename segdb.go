package yowza

// SegDB — Go bindings for the layered segdb (Track A item 5).
//
// The wasm segdb keeps its state (SegDB struct, hot arena, hot WAL,
// heap object store) in Go-managed wasm linear memory, starting at
// segDBBase (32 MiB), well above the legacy regions (arena 2 MiB,
// snapshot buffer ending at ~4.5 MiB). Layout inside the segdb area:
//   state   : sizeof(SegDB) from the wasm (queried at init), rounded up
//   arena   : 256 KiB hot ART arena
//   wal     : 64 KiB hot WAL (volatile memory driver, same semantics
//             as the native memory driver)
//   scratch : 64 KiB (key cell, value cell, out buffer, len/count cells)
//   heap    : 4 MiB self-describing object store (segments, manifests,
//             directory). The heap region bytes ARE the persistent
//             segdb state: HeapBytes copies them out, and
//             RestoreSegDBHeap writes them back before NewSegDB, whose
//             init reuses a valid heap instead of clearing it.
//
// Caveats (honest, see TRACKA_NOTES.md):
//   - The wasm heap store is bump-allocated: removed objects leak
//     heap space until re-init on a fresh heap; exhaustion fails
//     cleanly with an error, never corrupts.
//   - The hot WAL is volatile: unflushed puts do not survive a
//     module restart; flushed segments in the heap do.
//   - One SegDB per YowzaDB module instance in practice: the wasm
//     heap-store context is a single static per module, so two
//     SegDB values over one module would share one heap store.
//     Use one SegDB per module, or separate modules per SegDB.
//   - The legacy ScanEntries path can grow the snapshot buffer
//     past segDBBase only for scan outputs above ~27 MiB; do not
//     run huge legacy scans and a live SegDB in one module.

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/tetratelabs/wazero/api"
)

// ErrSegDBHistFull reports a SegDB Put/Delete whose
// history capture failed (rc=-3): the C hot history
// table reached its 1024-entry epoch bound (or its
// arena ran out) — fail-closed in C, by design. The
// PersistedSegDB facade converts it into a flush
// epoch + retry (see Put); direct SegDB callers see
// this typed error instead of a bare rc. Compare with
// errors.Is.
var ErrSegDBHistFull = errors.New("yowza segdb: hot history full (flush epoch required)")

// ErrHighCapacityRequiresWasm64 reports a non-Standard heap
// size requested on a wasm32 module. HighCapacity heaps are
// wasm64-only by design: the request is refused loudly, never
// silently served with the 8 MiB Standard heap.
// Compare with errors.Is.
var ErrHighCapacityRequiresWasm64 = errors.New("yowza segdb: high-capacity heap size requires wasm64 (wasm32 refused)")

// ErrHeapImageTooLarge reports a heap image larger than the
// loader's own heap size. It is refused loudly, never silently
// truncated to fit. Compare with errors.Is.
var ErrHeapImageTooLarge = errors.New("yowza segdb: heap image larger than loader heap size")

const (
	segDBBase      uint64 = 33554432 // 32 MiB
	segDBArenaSize uint64 = 262144
	segDBWalSize   uint64 = 65536
	segDBScratch   uint64 = 65536
	// segDBHeapSize is the wasm heap-store region per SegDB.
	// INTEGRATION FIX (Track C regression): Track C doubled
	// the heap table (4096 → 8192 slots, 622,592 → 1,245,184
	// bytes) INSIDE this fixed region, so the 4 MiB heap
	// lost 622 KiB of bump/data room (3.57 MB → 2.95 MB)
	// and two previously-green tests broke at exactly the
	// new exhaustion point (Gate1 90k flush at 81000,
	// Gate4 flush at 2749). 8 MiB restores headroom
	// (~6.75 MB data room) while keeping the region fixed
	// and the layout math unchanged. Persisted 4 MiB heap
	// images from older trees still open: see
	// segDBHeapSizeLegacy + extendHeapImageToCurrent and
	// the open path in segdb_persist.go (SHA verified over
	// the stored bytes first, then zero-extended, then the
	// wasm init migrates the legacy table in place).
	segDBHeapSize uint64 = 8388608
	// segDBHeapSizeLegacy is the pre-Track-C heap region
	// size. Only the open path accepts it, and only with
	// a matching checkpoint SHA-256 over the stored bytes.
	segDBHeapSizeLegacy uint64 = 4194304
	// segDBHeapSizeHigh64 and segDBHeapSizeHigh256 are
	// the HighCapacity profile heap sizes (worker S,
	// Yowza64 High-Capacity Profile track). They are
	// wasm64-only: NewSegDBWithHeapSize refuses them on
	// wasm32 with ErrHighCapacityRequiresWasm64. The C
	// heap engine already takes its heap size as a
	// parameter (yowza_segdb_init), so no C change is
	// needed: a larger region only grows the bump/data
	// room past the fixed 8192-slot table (1,245,184
	// bytes); the table, slot format, and object format
	// are identical at every size, which is why a
	// smaller image zero-extends cleanly into a larger
	// loader. The Query ISA heap (query/isa.go
	// isaSegHeapSize) deliberately stays at 8 MiB: ISA
	// heaps are ephemeral query-execution scratch, never
	// persisted, so the profile does not apply there.
	segDBHeapSizeHigh64  uint64 = 67108864
	segDBHeapSizeHigh256 uint64 = 268435456
)

const (
	segScratchKey uint64 = 0
	segScratchVal uint64 = 2048
	segScratchLen uint64 = 4096 // u32 cell: get out_len / scan count
	segScratchOut uint64 = 8192
	segScratchCap uint64 = 32768 // scan out capacity within scratch
	segGetOutCap  uint64 = 4096  // get out capacity (segdb vals <= 1024)
)

// SegDBScanEntry is one row of a SegDB.Scan result.
type SegDBScanEntry struct {
	Key []byte
	Val []byte
}

// SegDB is a layered (hot ART + immutable segments + manifest /
// directory) database running inside the wasm module.
type SegDB struct {
	host *YowzaDB

	stateOff   uint64
	stateSize  uint64
	arenaOff   uint64
	walOff     uint64
	scratchOff uint64
	heapOff    uint64
	// heapSize is this SegDB's heap region size in bytes
	// (Standard 8 MiB, or a HighCapacity size). Every
	// heap-sized operation on this SegDB (HeapBytes,
	// init, layout) uses it, never the package constant
	// directly, so one module can host a 256 MiB heap
	// while another keeps the Standard size.
	heapSize uint64

	// O4 UNIFIED LEASES: the C in-module lease table (segdb.c,
	// exported as yowza_segdb_lease_* in yowza_segdb.c) is the
	// SINGLE authority. The Go methods below delegate to those
	// exports whenever the running wasm binary provides them
	// (see LeaseBackend). The Go-side map survives ONLY as a
	// fallback for wasm binaries predating the exports; it is
	// never populated on the C path, so the two registries are
	// never live side by side. A lease pins one snapshot
	// sequence for one SegDB; Compact defers while any lease
	// with a sequence below the current sequence is active.
	// Lock order (adapted from Gate 1): leaseMu is always
	// taken before host.mu, never after, and never while
	// host.mu is already held. Acquire/Register hold leaseMu
	// across the sequence capture AND the C acquire call (both
	// under one host.mu hold), so a compact issued through Go
	// — or directly through the wasm compact export under
	// host.mu — cannot slip between capture and registration.
	// LeaseValid on the C path takes leaseMu then host.mu (it
	// no longer takes leaseMu alone, because validity now
	// lives in wasm memory); it must NOT be called with
	// host.mu already held. No SegDB method calls LeaseValid
	// while holding host.mu, and neither caller outside this
	// file does (query segPager, PersistedScanIter hold no
	// host.mu). Compact holds leaseMu across the wasm call
	// as before; on the C path the deferral decision itself
	// is made inside segdb_compact against the same table.
	leaseMu     sync.Mutex
	leases      map[uint64]uint64 // FALLBACK ONLY: lease id -> snapshot sequence, used only when the C lease exports are absent
	nextLeaseID uint64
	// leaseFallbackForTest is a TEST-ONLY seam (never set
	// outside tests in this package): when true, leaseFuncs
	// reports the C exports as absent, so the Go-map fallback
	// path can be exercised end-to-end without an old wasm
	// binary. It models a pre-export binary faithfully for the
	// Go side — including the documented limit that a
	// fallback lease does NOT pin a compact issued directly
	// through the C export, because it was never registered
	// in the C table.
	leaseFallbackForTest bool

	fnInit       api.Function
	fnRecover    api.Function
	fnPut        api.Function
	fnGet        api.Function
	fnDelete     api.Function
	fnFlush      api.Function
	fnCompact    api.Function
	fnScan       api.Function
	fnReconcile  api.Function
	fnIndet      api.Function
	fnSegCount   api.Function
	fnHasDir     api.Function
	fnGroupCount api.Function
}

// segDBLayout computes the segdb area offsets for a state size
// with the Standard heap size.
func segDBLayout(stateSize uint64) (state, arena, wal, scratch, heap, end uint64) {
	return segDBLayoutSize(stateSize, segDBHeapSize)
}

// segDBLayoutSize computes the segdb area offsets for a state
// size and an explicit heap size. The heap region sits last,
// so only end moves with the heap size; every other offset is
// size-independent.
func segDBLayoutSize(stateSize, heapSize uint64) (state, arena, wal, scratch, heap, end uint64) {
	state = segDBBase
	arena = state + stateSize
	wal = arena + segDBArenaSize
	scratch = wal + segDBWalSize
	heap = scratch + segDBScratch
	end = heap + heapSize
	return
}

// NewSegDB creates and initialises a SegDB over host's wasm module.
// Call RestoreSegDBHeap first (on the same host) to resume a
// previously saved heap; otherwise the heap starts empty.
func NewSegDB(host *YowzaDB) (*SegDB, error) {
	return NewSegDBWithHeapSize(host, segDBHeapSize)
}

// NewSegDBWithHeapSize is NewSegDB with an explicit heap
// region size. Only segDBHeapSize (Standard) and the two
// HighCapacity sizes are accepted; anything else is a caller
// error, never rounded. A HighCapacity size on a wasm32
// module fails with ErrHighCapacityRequiresWasm64, never
// silently falling back to the Standard heap.
func NewSegDBWithHeapSize(host *YowzaDB, heapSize uint64) (*SegDB, error) {
	host.mu.Lock()
	defer host.mu.Unlock()
	ctx := context.Background()

	fnStateSize := host.module.ExportedFunction("yowza_segdb_state_size")
	fnInit := host.module.ExportedFunction("yowza_segdb_init")
	fnRecover := host.module.ExportedFunction("yowza_segdb_recover")
	fnPut := host.module.ExportedFunction("yowza_segdb_put")
	fnGet := host.module.ExportedFunction("yowza_segdb_get")
	fnDelete := host.module.ExportedFunction("yowza_segdb_delete")
	fnFlush := host.module.ExportedFunction("yowza_segdb_flush")
	fnCompact := host.module.ExportedFunction("yowza_segdb_compact")
	fnScan := host.module.ExportedFunction("yowza_segdb_scan")
	fnReconcile := host.module.ExportedFunction("yowza_segdb_reconcile")
	fnIndet := host.module.ExportedFunction("yowza_segdb_indeterminate")
	fnSegCount := host.module.ExportedFunction("yowza_segdb_segment_count")
	fnHasDir := host.module.ExportedFunction("yowza_segdb_has_dir")
	fnGroupCount := host.module.ExportedFunction("yowza_segdb_group_count")
	for name, fn := range map[string]api.Function{
		"yowza_segdb_state_size": fnStateSize, "yowza_segdb_init": fnInit,
		"yowza_segdb_recover": fnRecover, "yowza_segdb_put": fnPut,
		"yowza_segdb_get": fnGet, "yowza_segdb_delete": fnDelete,
		"yowza_segdb_flush": fnFlush, "yowza_segdb_compact": fnCompact,
		"yowza_segdb_scan": fnScan, "yowza_segdb_reconcile": fnReconcile,
		"yowza_segdb_indeterminate": fnIndet,
		"yowza_segdb_segment_count": fnSegCount,
		"yowza_segdb_has_dir":       fnHasDir,
		"yowza_segdb_group_count":   fnGroupCount,
	} {
		if fn == nil {
			return nil, fmt.Errorf("yowza segdb: wasm binary predates segdb exports (missing %s)", name)
		}
	}

	res, err := fnStateSize.Call(ctx)
	if err != nil || len(res) == 0 {
		return nil, fmt.Errorf("yowza segdb: state_size call failed: %v", err)
	}
	if heapSize != segDBHeapSize && heapSize != segDBHeapSizeHigh64 && heapSize != segDBHeapSizeHigh256 {
		return nil, fmt.Errorf("yowza segdb: unsupported heap size %d (want %d, %d, or %d)", heapSize, segDBHeapSize, segDBHeapSizeHigh64, segDBHeapSizeHigh256)
	}
	if heapSize != segDBHeapSize && !host.isMem64 {
		return nil, fmt.Errorf("%w: heap size %d on a wasm32 module", ErrHighCapacityRequiresWasm64, heapSize)
	}
	// Round the state size up to a page and add one page of slack
	// so a small future SegDB growth does not move the layout.
	stateSize := (res[0] + 65535) &^ 65535
	stateSize += 65536
	stateOff, arenaOff, walOff, scratchOff, heapOff, end := segDBLayoutSize(stateSize, heapSize)
	if err := host.memGrowTo(end); err != nil {
		return nil, fmt.Errorf("yowza segdb: grow memory: %v", err)
	}
	initRes, err := fnInit.Call(ctx, stateOff, arenaOff, segDBArenaSize,
		walOff, uint64(uint32(segDBWalSize)), heapOff, heapSize)
	if err != nil || len(initRes) == 0 || int32(initRes[0]) != 0 {
		return nil, fmt.Errorf("yowza segdb: init failed: %v (rc=%d)", err, rcOf(initRes))
	}
	return &SegDB{
		host: host, stateOff: stateOff, stateSize: stateSize,
		arenaOff: arenaOff, walOff: walOff, scratchOff: scratchOff,
		heapOff: heapOff, heapSize: heapSize, fnInit: fnInit, fnRecover: fnRecover,
		fnPut: fnPut, fnGet: fnGet, fnDelete: fnDelete,
		fnFlush: fnFlush, fnCompact: fnCompact, fnScan: fnScan,
		fnReconcile: fnReconcile, fnIndet: fnIndet,
		fnSegCount: fnSegCount, fnHasDir: fnHasDir,
		fnGroupCount: fnGroupCount,
		leases:       map[uint64]uint64{}, nextLeaseID: 1,
	}, nil
}

func rcOf(res []uint64) int32 {
	if len(res) == 0 {
		return -999
	}
	return int32(res[0])
}

func (s *SegDB) call(fn api.Function, args ...uint64) (int32, error) {
	res, err := fn.Call(context.Background(), args...)
	if err != nil {
		return 0, err
	}
	return rcOf(res), nil
}

func (s *SegDB) writeKey(key string) error {
	if len(key) == 0 || len(key) > 1023 {
		return fmt.Errorf("yowza segdb: key length %d out of range 1..1023", len(key))
	}
	if !s.host.memWrite(s.scratchOff+segScratchKey, []byte(key)) {
		return fmt.Errorf("yowza segdb: failed to write key to wasm memory")
	}
	return nil
}

// Recover loads manifests/segments from the heap store into this
// SegDB (fresh instance over a restored heap, or same instance
// after a restart of the hot DB only).
func (s *SegDB) Recover() error {
	s.host.mu.Lock()
	defer s.host.mu.Unlock()
	rc, err := s.call(s.fnRecover, s.stateOff)
	if err != nil {
		return err
	}
	if rc != 0 {
		return fmt.Errorf("yowza segdb: recover failed (rc=%d)", rc)
	}
	return nil
}

// Put stores key/value in the hot ART (unflushed until Flush).
func (s *SegDB) Put(key string, val []byte) error {
	if len(val) > 1024 {
		return fmt.Errorf("yowza segdb: value length %d exceeds 1024", len(val))
	}
	s.host.mu.Lock()
	defer s.host.mu.Unlock()
	if err := s.writeKey(key); err != nil {
		return err
	}
	if !s.host.memWrite(s.scratchOff+segScratchVal, val) {
		return fmt.Errorf("yowza segdb: failed to write value to wasm memory")
	}
	rc, err := s.call(s.fnPut, s.stateOff, s.scratchOff+segScratchKey,
		uint64(len(key)), s.scratchOff+segScratchVal, uint64(len(val)))
	if err != nil {
		return err
	}
	if rc != 0 {
		if rc == -3 {
			return fmt.Errorf("%w: put failed (rc=-3)", ErrSegDBHistFull)
		}
		return fmt.Errorf("yowza segdb: put failed (rc=%d)", rc)
	}
	return nil
}

// ErrSegDBNotFound is the ONLY error that may be converted to
// "key absent" by a caller. Compare with errors.Is. Every other
// Get error (wasm call failure, memory read failure, length
// overflow, invalid key) is an internal error and must
// propagate fail-closed — it must never be read as absence
// (see segdb_tx.go: liveValLocked).
//
// C-LEVEL SEPARATION, CLOSED IN THE PATCH-1 INTEGRATION:
// segdb_get now returns -4 (never -1) for a segment-store
// lookup error (manifest_read failure, seg_lookup error —
// "lookup error: fail, never skip"), and yowza_segdb_get
// passes that code through (it also returns -4 when the
// value overflows the caller buffer, which is an error,
// not absence). Genuinely absent keys and tombstone
// shadows still return -1. This Go method therefore maps
// rc=-1 to ErrSegDBNotFound and any other nonzero rc to
// ErrSegDBInternal, so a segment-store I/O error can no
// longer be read as absence at this boundary. Transaction
// validation additionally fail-closes on every Go-visible
// failure (closed DB, poisoned DB, indeterminate shard,
// wasm call failure) before any journal is written.
var ErrSegDBNotFound = errors.New("yowza segdb: key not found")

// ErrSegDBInternal reports a SegDB Get failure that is NOT
// absence: a wasm call failure, a scratch-memory read
// failure, or a length overflow. Compare with errors.Is;
// callers must propagate it, never convert it to absence.
var ErrSegDBInternal = errors.New("yowza segdb: internal storage error")

// Get returns the value for key, ErrSegDBNotFound when the
// key is missing, or ErrSegDBInternal (or a key-validation
// error) for any other failure. A segment-store lookup
// error arrives as rc=-4 and maps to ErrSegDBInternal,
// never to ErrSegDBNotFound (see the ErrSegDBNotFound doc).
func (s *SegDB) Get(key string) ([]byte, error) {
	s.host.mu.Lock()
	defer s.host.mu.Unlock()
	if err := s.writeKey(key); err != nil {
		return nil, err
	}
	rc, err := s.call(s.fnGet, s.stateOff, s.scratchOff+segScratchKey,
		uint64(len(key)), s.scratchOff+segScratchOut, segGetOutCap,
		s.scratchOff+segScratchLen)
	if err != nil {
		return nil, fmt.Errorf("%w: get wasm call failed: %v", ErrSegDBInternal, err)
	}
	if rc != 0 {
		if rc != -1 {
			return nil, fmt.Errorf("%w: get failed (rc=%d)", ErrSegDBInternal, rc)
		}
		// rc=-1: absent (a segment lookup error is rc=-4
		// and was mapped to ErrSegDBInternal above).
		return nil, fmt.Errorf("%w: %q (rc=-1)", ErrSegDBNotFound, key)
	}
	lenBytes, ok := s.host.memRead(s.scratchOff+segScratchLen, 4)
	if !ok {
		return nil, fmt.Errorf("%w: failed to read get length", ErrSegDBInternal)
	}
	vlen := binary.LittleEndian.Uint32(lenBytes)
	if uint64(vlen) > segGetOutCap {
		return nil, fmt.Errorf("%w: get length %d exceeds buffer", ErrSegDBInternal, vlen)
	}
	val, ok := s.host.memRead(s.scratchOff+segScratchOut, uint64(vlen))
	if !ok {
		return nil, fmt.Errorf("%w: failed to read get value", ErrSegDBInternal)
	}
	out := make([]byte, vlen)
	copy(out, val)
	return out, nil
}

// GetLocked is Get without taking host.mu: it is the
// locked-path twin for callers that already hold host.mu.
// It exists so a future PersistedSegDB locked Get (in
// segdb_persist.go, owned by another axis) can share this
// error mapping instead of re-implementing it; segdb_tx.go
// uses SegDB.Get directly (host.mu is not held under the
// p.mu commit gate), so this helper is currently unused
// by the transaction path and is kept only to document
// the intended locked entry point. Callers must hold
// host.mu.
func (s *SegDB) getLocked(key string) ([]byte, error) {
	if err := s.writeKey(key); err != nil {
		return nil, err
	}
	rc, err := s.call(s.fnGet, s.stateOff, s.scratchOff+segScratchKey,
		uint64(len(key)), s.scratchOff+segScratchOut, segGetOutCap,
		s.scratchOff+segScratchLen)
	if err != nil {
		return nil, fmt.Errorf("%w: get wasm call failed: %v", ErrSegDBInternal, err)
	}
	if rc != 0 {
		if rc != -1 {
			return nil, fmt.Errorf("%w: get failed (rc=%d)", ErrSegDBInternal, rc)
		}
		return nil, fmt.Errorf("%w: %q (rc=-1)", ErrSegDBNotFound, key)
	}
	lenBytes, ok := s.host.memRead(s.scratchOff+segScratchLen, 4)
	if !ok {
		return nil, fmt.Errorf("%w: failed to read get length", ErrSegDBInternal)
	}
	vlen := binary.LittleEndian.Uint32(lenBytes)
	if uint64(vlen) > segGetOutCap {
		return nil, fmt.Errorf("%w: get length %d exceeds buffer", ErrSegDBInternal, vlen)
	}
	val, ok := s.host.memRead(s.scratchOff+segScratchOut, uint64(vlen))
	if !ok {
		return nil, fmt.Errorf("%w: failed to read get value", ErrSegDBInternal)
	}
	out := make([]byte, vlen)
	copy(out, val)
	return out, nil
}

// Delete removes key (hot tombstone; flushed as a segment
// tombstone on the next Flush).
func (s *SegDB) Delete(key string) error {
	s.host.mu.Lock()
	defer s.host.mu.Unlock()
	if err := s.writeKey(key); err != nil {
		return err
	}
	rc, err := s.call(s.fnDelete, s.stateOff, s.scratchOff+segScratchKey, uint64(len(key)))
	if err != nil {
		return err
	}
	if rc != 0 {
		if rc == -3 {
			return fmt.Errorf("%w: delete failed (rc=-3)", ErrSegDBHistFull)
		}
		return fmt.Errorf("yowza segdb: delete failed (rc=%d)", rc)
	}
	return nil
}

// Flush writes the hot ART as one immutable segment and publishes
// it (appending, or starting a new group past 128 segments).
func (s *SegDB) Flush() error {
	s.host.mu.Lock()
	defer s.host.mu.Unlock()
	rc, err := s.call(s.fnFlush, s.stateOff)
	if err != nil {
		return err
	}
	if rc != 0 {
		return fmt.Errorf("yowza segdb: flush failed (rc=%d)", rc)
	}
	return nil
}

// Compact merges segments (streaming, in wasm) and drops
// tombstones with commit_seq <= barrier. Hot data is untouched.
//
// O4 UNIFIED lease rule: the deferral decision is made against
// the C lease table — on the C path (see LeaseBackend) this
// method simply calls the wasm compact export and
// segdb_compact itself defers (returns 0, no merge) while any
// lease with a sequence below the current sequence is active,
// including leases acquired directly through the C export.
// On the fallback path (wasm predating the lease exports) the
// old Go-map check below performs the same deferral. Either
// way, compaction is an optimisation, never a logical
// operation, and the merge drops non-newest versions
// unconditionally, so no barrier can make it safe for a stale
// snapshot; deferring is the only correct enforcement. A lease
// exactly at the current sequence does not defer (the merge
// preserves that sequence's view). The check holds leaseMu
// across the sequence read and the wasm call, so a lease
// cannot register for a sequence a concurrent compact has
// already destroyed. Callers that need to know whether the
// merge actually ran can compare SegmentCount before and
// after.
func (s *SegDB) Compact(barrier uint64) error {
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()
	s.host.mu.Lock()
	defer s.host.mu.Unlock()
	if _, _, _, _, cOK := s.leaseFuncs(); !cOK {
		// Fallback path only: the C table does not exist, so
		// the Go map is the registry. Defer exactly as Gate 1
		// did (conservatively when the sequence is unreadable).
		if fnSeq := s.host.module.ExportedFunction("yowza_segdb_snapshot_seq"); fnSeq != nil {
			res, err := fnSeq.Call(context.Background(), s.stateOff)
			if err == nil && len(res) > 0 {
				cur := res[0]
				for _, seq := range s.leases {
					if seq < cur {
						return nil // deferred: stale lease pins compaction
					}
				}
			} else if len(s.leases) > 0 {
				return nil // cannot verify the sequence: defer conservatively
			}
		} else if len(s.leases) > 0 {
			return nil // old binary without snapshot_seq: defer conservatively
		}
	}
	rc, err := s.call(s.fnCompact, s.stateOff, barrier)
	if err != nil {
		return err
	}
	if rc != 0 {
		return fmt.Errorf("yowza segdb: compact failed (rc=%d)", rc)
	}
	return nil
}

// Scan returns entries with the given prefix at snapshotSeq
// (0 = current), in [key_len u32][key][val_len u32][val] order
// decoded here. Results are capped by the 32 KiB scratch out
// buffer; larger result sets return an error rather than a
// partial list.
func (s *SegDB) Scan(prefix string, snapshotSeq uint64) ([]SegDBScanEntry, error) {
	s.host.mu.Lock()
	defer s.host.mu.Unlock()
	if len(prefix) > 1023 {
		return nil, fmt.Errorf("yowza segdb: prefix too long")
	}
	if !s.host.memWrite(s.scratchOff+segScratchKey, []byte(prefix)) {
		return nil, fmt.Errorf("yowza segdb: failed to write prefix to wasm memory")
	}
	rc, err := s.call(s.fnScan, s.stateOff, s.scratchOff+segScratchKey,
		uint64(len(prefix)), snapshotSeq, s.scratchOff+segScratchOut,
		segScratchCap, s.scratchOff+segScratchLen)
	if err != nil {
		return nil, err
	}
	if rc != 0 {
		return nil, fmt.Errorf("yowza segdb: scan failed (rc=%d)", rc)
	}
	countBytes, ok := s.host.memRead(s.scratchOff+segScratchLen, 4)
	if !ok {
		return nil, fmt.Errorf("yowza segdb: failed to read scan count")
	}
	count := binary.LittleEndian.Uint32(countBytes)
	raw, ok := s.host.memRead(s.scratchOff+segScratchOut, segScratchCap)
	if !ok {
		return nil, fmt.Errorf("yowza segdb: failed to read scan output")
	}
	out := make([]SegDBScanEntry, 0, count)
	off := 0
	for i := uint32(0); i < count; i++ {
		if off+8 > len(raw) {
			return nil, fmt.Errorf("yowza segdb: truncated scan output")
		}
		klen := int(binary.LittleEndian.Uint32(raw[off:]))
		off += 4
		if off+klen+4 > len(raw) {
			return nil, fmt.Errorf("yowza segdb: truncated scan key")
		}
		k := make([]byte, klen)
		copy(k, raw[off:off+klen])
		off += klen
		vlen := int(binary.LittleEndian.Uint32(raw[off:]))
		off += 4
		if off+vlen > len(raw) {
			return nil, fmt.Errorf("yowza segdb: truncated scan value")
		}
		v := make([]byte, vlen)
		copy(v, raw[off:off+vlen])
		off += vlen
		out = append(out, SegDBScanEntry{Key: k, Val: v})
	}
	return out, nil
}

// Reconcile resolves an indeterminate publish (see segdb.h).
func (s *SegDB) Reconcile() error {
	s.host.mu.Lock()
	defer s.host.mu.Unlock()
	rc, err := s.call(s.fnReconcile, s.stateOff)
	if err != nil {
		return err
	}
	if rc != 0 {
		return fmt.Errorf("yowza segdb: reconcile failed (rc=%d)", rc)
	}
	return nil
}

// Indeterminate reports whether a publish left this SegDB
// indeterminate (writes refused until Reconcile).
func (s *SegDB) Indeterminate() (bool, error) {
	s.host.mu.Lock()
	defer s.host.mu.Unlock()
	rc, err := s.call(s.fnIndet, s.stateOff)
	if err != nil {
		return false, err
	}
	if rc < 0 {
		return false, fmt.Errorf("yowza segdb: indeterminate query failed (rc=%d)", rc)
	}
	return rc != 0, nil
}

// SegmentCount returns the last group's segment count (== total
// in single-manifest mode).
func (s *SegDB) SegmentCount() (int, error) {
	s.host.mu.Lock()
	defer s.host.mu.Unlock()
	rc, err := s.call(s.fnSegCount, s.stateOff)
	if err != nil {
		return 0, err
	}
	if rc < 0 {
		return 0, fmt.Errorf("yowza segdb: segment count failed (rc=%d)", rc)
	}
	return int(rc), nil
}

// HasDir reports whether the DB has transitioned to directory mode.
func (s *SegDB) HasDir() (bool, error) {
	s.host.mu.Lock()
	defer s.host.mu.Unlock()
	rc, err := s.call(s.fnHasDir, s.stateOff)
	if err != nil {
		return false, err
	}
	if rc < 0 {
		return false, fmt.Errorf("yowza segdb: has_dir failed (rc=%d)", rc)
	}
	return rc != 0, nil
}

// GroupCount returns the number of manifest groups (0 in
// single-manifest mode).
func (s *SegDB) GroupCount() (int, error) {
	s.host.mu.Lock()
	defer s.host.mu.Unlock()
	rc, err := s.call(s.fnGroupCount, s.stateOff)
	if err != nil {
		return 0, err
	}
	if rc < 0 {
		return 0, fmt.Errorf("yowza segdb: group count failed (rc=%d)", rc)
	}
	return int(rc), nil
}

// extendHeapImageToCurrent returns a heap image sized for the
// current heap region: current-size images pass through
// unchanged, legacy-size (pre-Track-C 4 MiB) images are
// zero-extended to the current size, and any other size is a
// corruption error. Callers MUST verify the checkpoint SHA-256
// over the stored bytes BEFORE extending — extension changes
// the bytes, so a SHA taken after extension would never match
// a legacy checkpoint. The wasm init then migrates a legacy
// table in place (room is guaranteed: legacy bump ≤ 4 MiB +
// 622,592-byte shift < 8 MiB); an already-current table in a
// legacy-size image (written by integrated-tree code before
// this fix) extends harmlessly — table, data, and bump are
// untouched, the tail zeros are unused.
func extendHeapImageToCurrent(heap []byte) ([]byte, error) {
	return extendHeapImageToSize(heap, segDBHeapSize)
}

// ErrHeapImageCorrupt reports a heap image whose size is not a
// heap size this engine ever wrote: neither the legacy 4 MiB
// size nor a current creation size (8/64/256 MiB). Such an image
// is corruption, not a smaller image to extend — extending an
// arbitrary size would silently bless torn or foreign bytes as
// a heap.
var ErrHeapImageCorrupt = errors.New("yowza segdb: heap image size is not a known heap size")

// extendHeapImageToSize returns a heap image sized for the
// loader's heap region: images of exactly the target size pass
// through unchanged, SMALLER images of a KNOWN heap size are
// zero-extended to the target size (the legacy-migration rule,
// generalized: the 4 MiB legacy image and the 8 MiB current
// image both extend into a 64/256 MiB loader exactly this way),
// LARGER images are a loud error, never silently truncated,
// and images of any OTHER size are a corruption error.
// Callers MUST verify the checkpoint SHA-256 over the stored
// bytes BEFORE extending — extension changes the bytes, so a
// SHA taken after extension would never match a checkpoint
// recorded at the smaller size.
func extendHeapImageToSize(heap []byte, target uint64) ([]byte, error) {
	switch uint64(len(heap)) {
	case segDBHeapSizeLegacy, segDBHeapSize, segDBHeapSizeHigh64, segDBHeapSizeHigh256:
	default:
		return nil, fmt.Errorf("%w: heap image size %d", ErrHeapImageCorrupt, len(heap))
	}
	switch {
	case uint64(len(heap)) == target:
		return heap, nil
	case uint64(len(heap)) < target:
		out := make([]byte, target)
		copy(out, heap)
		return out, nil
	default:
		return nil, fmt.Errorf("%w: heap image size %d exceeds loader heap size %d", ErrHeapImageTooLarge, len(heap), target)
	}
}

// HeapBytes copies the whole heap region out. Persist it and pass
// it to RestoreSegDBHeap on a fresh host before NewSegDB to resume
// the same segdb (segments, manifests, directory included).
// The copy is this SegDB's full heapSize, not the Standard size.
func (s *SegDB) HeapBytes() ([]byte, error) {
	s.host.mu.Lock()
	defer s.host.mu.Unlock()
	raw, ok := s.host.memRead(s.heapOff, s.heapSize)
	if !ok {
		return nil, fmt.Errorf("yowza segdb: failed to read heap region")
	}
	out := make([]byte, len(raw))
	copy(out, raw)
	return out, nil
}

// HeapSize returns this SegDB's heap region size in bytes.
func (s *SegDB) HeapSize() uint64 {
	return s.heapSize
}

// SegDBHeapInfo is the C heap store's usage snapshot, as
// reported by the yowza_segdb_heap_info export: the region
// size, the bump high-water mark, live object bytes,
// occupied slot count, and wasted bytes (bump room above
// live bytes). Bump grows faster than live data because
// heap-object growth abandons old copies as waste until
// a heap compaction; operators should watch Bump, not
// Live, for headroom (see WORKER_S_NOTES.md).
type SegDBHeapInfo struct {
	HeapSize uint64
	Bump     uint64
	Live     uint64
	Slots    uint64
	Wasted   uint64
}

// HeapInfo returns the C heap store's usage snapshot. The
// export writes six u64s into the scratch area past the
// scan-out region (scratch+60000, inside the 64 KiB
// scratch, beyond the 40960-byte scan-out end); host.mu
// is held across the call and the read, so no other
// SegDB operation can interleave on the scratch.
func (s *SegDB) HeapInfo() (SegDBHeapInfo, error) {
	s.host.mu.Lock()
	defer s.host.mu.Unlock()
	fn := s.host.module.ExportedFunction("yowza_segdb_heap_info")
	if fn == nil {
		return SegDBHeapInfo{}, fmt.Errorf("yowza segdb: wasm binary predates heap_info export")
	}
	out := s.scratchOff + 60000
	if _, err := fn.Call(context.Background(), out); err != nil {
		return SegDBHeapInfo{}, fmt.Errorf("yowza segdb: heap_info call failed: %v", err)
	}
	raw, ok := s.host.memRead(out, 48)
	if !ok {
		return SegDBHeapInfo{}, fmt.Errorf("yowza segdb: heap_info read failed")
	}
	// Copy out of the wasm view before releasing host.mu:
	// memRead aliases wasm memory, and the scratch is
	// reused by the next operation.
	vals := make([]uint64, 6)
	for i := range vals {
		vals[i] = binary.LittleEndian.Uint64(raw[i*8:])
	}
	return SegDBHeapInfo{HeapSize: vals[0], Bump: vals[1], Live: vals[2], Slots: vals[3], Wasted: vals[4]}, nil
}

// RestoreSegDBHeap writes a previously saved heap region into this
// host's segdb heap area. Call it BEFORE NewSegDB on a fresh host;
// NewSegDB's init reuses the valid heap. Calling it after NewSegDB
// would clobber the live heap and is a caller error.
func (db *YowzaDB) RestoreSegDBHeap(data []byte) error {
	return db.RestoreSegDBHeapToSize(data, segDBHeapSize)
}

// RestoreSegDBHeapToSize is RestoreSegDBHeap for an explicit
// loader heap size: a smaller image is zero-extended to the
// loader size (callers must already have SHA-verified the
// stored bytes), and a larger image fails loudly with
// ErrHeapImageTooLarge, never truncated. A HighCapacity
// target on a wasm32 host fails with
// ErrHighCapacityRequiresWasm64.
func (db *YowzaDB) RestoreSegDBHeapToSize(data []byte, target uint64) error {
	if target != segDBHeapSize && target != segDBHeapSizeHigh64 && target != segDBHeapSizeHigh256 {
		return fmt.Errorf("yowza segdb: unsupported heap size %d (want %d, %d, or %d)", target, segDBHeapSize, segDBHeapSizeHigh64, segDBHeapSizeHigh256)
	}
	if target != segDBHeapSize && !db.isMem64 {
		return fmt.Errorf("%w: heap size %d on a wasm32 module", ErrHighCapacityRequiresWasm64, target)
	}
	if uint64(len(data)) > target {
		return fmt.Errorf("%w: heap snapshot size %d exceeds loader heap size %d", ErrHeapImageTooLarge, len(data), target)
	}
	ext, err := extendHeapImageToSize(data, target)
	if err != nil {
		return err
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	ctx := context.Background()
	fnStateSize := db.module.ExportedFunction("yowza_segdb_state_size")
	if fnStateSize == nil {
		return fmt.Errorf("yowza segdb: wasm binary predates segdb exports")
	}
	res, err := fnStateSize.Call(ctx)
	if err != nil || len(res) == 0 {
		return fmt.Errorf("yowza segdb: state_size call failed: %v", err)
	}
	stateSize := (res[0] + 65535) &^ 65535
	stateSize += 65536
	_, _, _, _, heapOff, end := segDBLayoutSize(stateSize, target)
	if err := db.memGrowTo(end); err != nil {
		return fmt.Errorf("yowza segdb: grow memory: %v", err)
	}
	if !db.memWrite(heapOff, ext) {
		return fmt.Errorf("yowza segdb: failed to write heap region")
	}
	return nil
}

// ---- deep-integration additions (appended; existing methods above
// are untouched). New exports are looked up lazily so a SegDB built
// against an older wasm binary fails with a clear sentinel error
// instead of a nil-function panic, and NewSegDB's required-export
// set is unchanged.
//
// SnapshotSeq / ScanAt / ScanPage contract (see
// DEEP_SNAPSHOT_STREAM_NOTES.md):
//   - SnapshotSeq is seq_base + hot commit_seq, exactly the value
//     C segdb_scan treats as "current" for snapshot_seq 0.
//   - ScanAt uses LITERAL snapshot semantics: seq 0 yields no rows
//     (no terminal carries version 0); pass SnapshotSeq() for
//     "current". This differs deliberately from Scan, where 0
//     still means current for backward compatibility.
//   - ScanPage pages through C segdb_scan_page: skip counts live
//     merged rows, limit is clamped to segPageMaxRows so the
//     worst-case row (4+1023+4+1024 bytes) always fits the page
//     out buffer; callers page with skip and treat a short page
//     as end of input. A page call re-walks from the start, so
//     paging is O(rows) per page in time, O(sources + hot) in
//     memory — the streaming guarantee is memory, not time.

// ErrSegDBSnapshotSeqMissing / ErrSegDBScanPageMissing report a
// wasm binary that predates the deep-integration exports. They are
// returned directly (never wrapped), so callers compare with ==.
var (
	ErrSegDBSnapshotSeqMissing = fmt.Errorf("yowza segdb: wasm binary predates snapshot_seq export (missing yowza_segdb_snapshot_seq)")
	ErrSegDBScanPageMissing    = fmt.Errorf("yowza segdb: wasm binary predates scan_page export (missing yowza_segdb_scan_page)")
)

// segPageOutCap is the page out buffer within the 64 KiB scratch
// region (out starts at segScratchOut = 8192).
const segPageOutCap uint64 = segDBScratch - segScratchOut // 57344

// segPageMaxRows is the largest page that always fits
// segPageOutCap even if every row is worst-case sized
// (4 + 1023 + 4 + 1024 = 2055 bytes): 27*2055 = 55485 <= 57344.
const segPageMaxRows uint32 = 27

// SnapshotSeq returns the current commit sequence of this SegDB.
func (s *SegDB) SnapshotSeq() (uint64, error) {
	s.host.mu.Lock()
	defer s.host.mu.Unlock()
	fn := s.host.module.ExportedFunction("yowza_segdb_snapshot_seq")
	if fn == nil {
		return 0, ErrSegDBSnapshotSeqMissing
	}
	res, err := fn.Call(context.Background(), s.stateOff)
	if err != nil || len(res) == 0 {
		return 0, fmt.Errorf("yowza segdb: snapshot_seq call failed: %v", err)
	}
	return res[0], nil
}

// decodeScanPageOutput decodes art_scan-format bytes into entries.
func decodeScanPageOutput(raw []byte, count uint32) ([]SegDBScanEntry, error) {
	out := make([]SegDBScanEntry, 0, count)
	off := 0
	for i := uint32(0); i < count; i++ {
		if off+8 > len(raw) {
			return nil, fmt.Errorf("yowza segdb: truncated scan output")
		}
		klen := int(binary.LittleEndian.Uint32(raw[off:]))
		off += 4
		if off+klen+4 > len(raw) {
			return nil, fmt.Errorf("yowza segdb: truncated scan key")
		}
		k := make([]byte, klen)
		copy(k, raw[off:off+klen])
		off += klen
		vlen := int(binary.LittleEndian.Uint32(raw[off:]))
		off += 4
		if off+vlen > len(raw) {
			return nil, fmt.Errorf("yowza segdb: truncated scan value")
		}
		v := make([]byte, vlen)
		copy(v, raw[off:off+vlen])
		off += vlen
		out = append(out, SegDBScanEntry{Key: k, Val: v})
	}
	return out, nil
}

// ScanPage returns at most limit live merged rows with the given
// prefix at snapshotSeq, skipping the first skip live rows. limit
// is clamped to segPageMaxRows (see above); a returned page
// shorter than the requested limit means end of input. On a
// binary predating the export it returns ErrSegDBScanPageMissing.
func (s *SegDB) ScanPage(prefix string, snapshotSeq uint64, skip uint64, limit uint32) ([]SegDBScanEntry, error) {
	s.host.mu.Lock()
	defer s.host.mu.Unlock()
	fn := s.host.module.ExportedFunction("yowza_segdb_scan_page")
	if fn == nil {
		return nil, ErrSegDBScanPageMissing
	}
	if limit == 0 {
		return nil, nil
	}
	if limit > segPageMaxRows {
		limit = segPageMaxRows
	}
	if len(prefix) > 1023 {
		return nil, fmt.Errorf("yowza segdb: prefix too long")
	}
	if !s.host.memWrite(s.scratchOff+segScratchKey, []byte(prefix)) {
		return nil, fmt.Errorf("yowza segdb: failed to write prefix to wasm memory")
	}
	rc, err := s.call(fn, s.stateOff, s.scratchOff+segScratchKey,
		uint64(len(prefix)), snapshotSeq, skip, uint64(limit),
		s.scratchOff+segScratchOut, segPageOutCap, s.scratchOff+segScratchLen)
	if err != nil {
		return nil, err
	}
	if rc == -3 {
		return nil, fmt.Errorf("yowza segdb: scan page output buffer too small (rc=-3)")
	}
	if rc != 0 {
		return nil, fmt.Errorf("yowza segdb: scan page failed (rc=%d)", rc)
	}
	countBytes, ok := s.host.memRead(s.scratchOff+segScratchLen, 4)
	if !ok {
		return nil, fmt.Errorf("yowza segdb: failed to read scan page count")
	}
	count := binary.LittleEndian.Uint32(countBytes)
	raw, ok := s.host.memRead(s.scratchOff+segScratchOut, segPageOutCap)
	if !ok {
		return nil, fmt.Errorf("yowza segdb: failed to read scan page output")
	}
	return decodeScanPageOutput(raw, count)
}

// ScanAt returns all entries with the given prefix visible at
// snapshotSeq (literal semantics: seq 0 yields no rows), streamed
// through ScanPage so the old 8192-candidate cap does not bound
// it. On a binary predating the page export it falls back to
// Scan (seq 0 then means current, the legacy semantics, and large
// results fail at the old cap) — that fallback exists only so
// ScanAt keeps working on old binaries for small data.
func (s *SegDB) ScanAt(prefix string, snapshotSeq uint64) ([]SegDBScanEntry, error) {
	var out []SegDBScanEntry
	var skip uint64
	for {
		page, err := s.ScanPage(prefix, snapshotSeq, skip, segPageMaxRows)
		if err != nil {
			if err == ErrSegDBScanPageMissing {
				if snapshotSeq == 0 {
					return nil, nil
				}
				return s.Scan(prefix, snapshotSeq)
			}
			return nil, err
		}
		out = append(out, page...)
		if uint32(len(page)) < segPageMaxRows {
			return out, nil
		}
		skip += uint64(len(page))
	}
}

// ---- GATE 1 additions (cursor, leases, scan stats) ----
// These are the only Gate 1 additions to this file besides the
// Compact lease check above: ScanCursor (Gate 0), the lease
// registry (Gate 1.2, unified to the C table by O4 — see the
// lease methods below), and scan-stats access (Gate 0
// counters). Existing ScanPage/Scan code is untouched.

// ErrSegDBScanCursorMissing reports a wasm binary that predates
// the scan_cursor export. Returned directly (never wrapped), so
// callers compare with ==, exactly like ErrSegDBScanPageMissing.
var ErrSegDBScanCursorMissing = fmt.Errorf("yowza segdb: wasm binary predates scan_cursor export (missing yowza_segdb_scan_cursor)")

// ErrLeaseExpired reports a lease id that is unknown or already
// released. Returned directly or wrapped with %w; compare with
// errors.Is. A scan that hits an expired lease must fail with
// this error, never return partial apparently-successful rows.
var ErrLeaseExpired = fmt.Errorf("yowza segdb: snapshot lease expired or unknown")

// ErrSnapshotInvalid reports a snapshot sequence that cannot be
// leased or read: above the current sequence (a future snapshot
// does not exist yet). Compare with errors.Is.
var ErrSnapshotInvalid = fmt.Errorf("yowza segdb: snapshot sequence is invalid")

// segCursorCells are the scratch cells ScanCursor uses, inside
// the 64 KiB scratch region and clear of every other use:
// prefix key at segScratchKey (0), cursor key at segScratchVal
// (2048), count at segScratchLen (4096), next-key length at
// 4100, has-more at 4104, next-key bytes at 4112 (1024 bytes,
// ending at 5136, well before the page out buffer at 8192).
const (
	segCursorNextLen uint64 = segScratchLen + 4
	segCursorHasMore uint64 = segScratchLen + 8
	segCursorNextKey uint64 = segScratchLen + 16
)

// ScanCursor returns at most limit live merged rows with the
// given prefix at snapshotSeq, strictly after cursor (when
// hasCursor is set; otherwise from the start), in key order,
// plus the resume key (the last returned row's key, or the
// input cursor when nothing was returned and hasCursor is set)
// and whether at least one more live row exists past the page.
// limit is clamped to segPageMaxRows, so the page out buffer
// always fits. snapshotSeq is LITERAL, like ScanPage. On a
// binary predating the export it returns
// ErrSegDBScanCursorMissing.
func (s *SegDB) ScanCursor(prefix string, snapshotSeq uint64, cursor []byte, hasCursor bool, limit uint32) ([]SegDBScanEntry, []byte, bool, error) {
	s.host.mu.Lock()
	defer s.host.mu.Unlock()
	fn := s.host.module.ExportedFunction("yowza_segdb_scan_cursor")
	if fn == nil {
		return nil, nil, false, ErrSegDBScanCursorMissing
	}
	if limit == 0 {
		return nil, nil, false, nil
	}
	if limit > segPageMaxRows {
		limit = segPageMaxRows
	}
	if len(prefix) > 1023 {
		return nil, nil, false, fmt.Errorf("yowza segdb: prefix too long")
	}
	if hasCursor && len(cursor) > 1023 {
		return nil, nil, false, fmt.Errorf("yowza segdb: cursor too long")
	}
	if !s.host.memWrite(s.scratchOff+segScratchKey, []byte(prefix)) {
		return nil, nil, false, fmt.Errorf("yowza segdb: failed to write prefix to wasm memory")
	}
	if hasCursor {
		if !s.host.memWrite(s.scratchOff+segScratchVal, cursor) {
			return nil, nil, false, fmt.Errorf("yowza segdb: failed to write cursor to wasm memory")
		}
	}
	hasCur := uint64(0)
	cursorLen := uint64(0)
	if hasCursor {
		hasCur = 1
		cursorLen = uint64(len(cursor))
	}
	res, err := fn.Call(context.Background(), s.stateOff, s.scratchOff+segScratchKey,
		uint64(len(prefix)), snapshotSeq, s.scratchOff+segScratchVal, cursorLen, hasCur,
		uint64(limit), s.scratchOff+segScratchOut, segPageOutCap, s.scratchOff+segScratchLen,
		s.scratchOff+segCursorNextKey, 1024, s.scratchOff+segCursorNextLen, s.scratchOff+segCursorHasMore)
	if err != nil {
		return nil, nil, false, err
	}
	if rc := rcOf(res); rc == -3 {
		return nil, nil, false, fmt.Errorf("yowza segdb: scan cursor output buffer too small (rc=-3)")
	} else if rc != 0 {
		return nil, nil, false, fmt.Errorf("yowza segdb: scan cursor failed (rc=%d)", rc)
	}
	readU32 := func(off uint64) (uint32, bool) {
		b, ok := s.host.memRead(s.scratchOff+off, 4)
		if !ok {
			return 0, false
		}
		return binary.LittleEndian.Uint32(b), true
	}
	count, ok := readU32(segScratchLen)
	if !ok {
		return nil, nil, false, fmt.Errorf("yowza segdb: failed to read scan cursor count")
	}
	nextLen, ok := readU32(segCursorNextLen)
	if !ok {
		return nil, nil, false, fmt.Errorf("yowza segdb: failed to read scan cursor next length")
	}
	hasMoreU, ok := readU32(segCursorHasMore)
	if !ok {
		return nil, nil, false, fmt.Errorf("yowza segdb: failed to read scan cursor has-more")
	}
	raw, ok := s.host.memRead(s.scratchOff+segScratchOut, segPageOutCap)
	if !ok {
		return nil, nil, false, fmt.Errorf("yowza segdb: failed to read scan cursor output")
	}
	entries, err := decodeScanPageOutput(raw, count)
	if err != nil {
		return nil, nil, false, err
	}
	var nextKey []byte
	if nextLen > 0 {
		if nextLen > 1024 {
			return nil, nil, false, fmt.Errorf("yowza segdb: scan cursor next key length %d exceeds cap", nextLen)
		}
		nb, ok := s.host.memRead(s.scratchOff+segCursorNextKey, uint64(nextLen))
		if !ok {
			return nil, nil, false, fmt.Errorf("yowza segdb: failed to read scan cursor next key")
		}
		nextKey = append([]byte(nil), nb...)
	} else if hasCursor {
		nextKey = append([]byte(nil), cursor...)
	}
	return entries, nextKey, hasMoreU != 0, nil
}

// SegDBScanStats mirrors the C SegdbScanStats counters (see
// segdb.h): page_visits counts segment entries returned by
// seg_iter_next on the old skip path, cursor_visits the same on
// the cursor path, and the rest are cursor-path diagnostics.
type SegDBScanStats struct {
	PageVisits       uint64
	CursorVisits     uint64
	CursorBlockSkips uint64
	SeekIndexEntries uint64
	CacheHits        uint64
	CacheMisses      uint64
}

// ScanStats reads the C scan counters (6 little-endian u64s).
// On a binary predating the export it returns an error.
func (s *SegDB) ScanStats() (SegDBScanStats, error) {
	var st SegDBScanStats
	s.host.mu.Lock()
	defer s.host.mu.Unlock()
	fn := s.host.module.ExportedFunction("yowza_segdb_scan_stats")
	if fn == nil {
		return st, fmt.Errorf("yowza segdb: wasm binary predates scan_stats export (missing yowza_segdb_scan_stats)")
	}
	if _, err := fn.Call(context.Background(), s.scratchOff+segScratchOut); err != nil {
		return st, err
	}
	raw, ok := s.host.memRead(s.scratchOff+segScratchOut, 48)
	if !ok {
		return st, fmt.Errorf("yowza segdb: failed to read scan stats")
	}
	vals := make([]uint64, 6)
	for i := range vals {
		vals[i] = binary.LittleEndian.Uint64(raw[i*8:])
	}
	st.PageVisits, st.CursorVisits, st.CursorBlockSkips = vals[0], vals[1], vals[2]
	st.SeekIndexEntries, st.CacheHits, st.CacheMisses = vals[3], vals[4], vals[5]
	return st, nil
}

// ResetScanStats zeroes the C scan counters.
func (s *SegDB) ResetScanStats() error {
	s.host.mu.Lock()
	defer s.host.mu.Unlock()
	fn := s.host.module.ExportedFunction("yowza_segdb_scan_stats_reset")
	if fn == nil {
		return fmt.Errorf("yowza segdb: wasm binary predates scan_stats_reset export")
	}
	_, err := fn.Call(context.Background())
	return err
}

// ErrLeaseTableFull reports that the C lease table is full
// (SEGDB_MAX_LEASES = 64 active leases). The C acquire export
// returns 0 for both a future sequence and a full table; the
// Go methods below compare against the current sequence first,
// so a 0 after a valid sequence can only mean table-full.
// Compare with errors.Is. On the fallback path this error is
// never returned (the Go map is unbounded).
var ErrLeaseTableFull = fmt.Errorf("yowza segdb: snapshot lease table full (64 active leases)")

// leaseFuncs returns the C lease exports, or ok=false when the
// running wasm binary predates them (any one missing means
// fallback: the four exports were added together). Lookups are
// lock-free (the module's export set is fixed at
// instantiation), so this may be called with leaseMu and/or
// host.mu held. The binary never changes under a SegDB, so
// the path chosen for a given SegDB never flips mid-life.
func (s *SegDB) leaseFuncs() (acquire, release, valid, oldest api.Function, ok bool) {
	if s.leaseFallbackForTest {
		return nil, nil, nil, nil, false
	}
	acquire = s.host.module.ExportedFunction("yowza_segdb_lease_acquire")
	release = s.host.module.ExportedFunction("yowza_segdb_lease_release")
	valid = s.host.module.ExportedFunction("yowza_segdb_lease_valid")
	oldest = s.host.module.ExportedFunction("yowza_segdb_lease_oldest")
	ok = acquire != nil && release != nil && valid != nil && oldest != nil
	return
}

// LeaseBackend reports which lease registry is live for this
// SegDB: "c" when the C in-module table is the authority
// (the rebuilt wasm binaries), or "go-fallback" when the wasm
// binary predates the lease exports and the Go-side map is
// the registry. Callers can tell which path is active by
// calling this; tests assert it. On the "c" path the Go map
// is never populated, and on the "go-fallback" path no C
// lease call is ever made — never both at once.
func (s *SegDB) LeaseBackend() string {
	if s.leaseFallbackForTest {
		return "go-fallback"
	}
	if _, _, _, _, ok := s.leaseFuncs(); ok {
		return "c"
	}
	return "go-fallback"
}

// snapshotSeqLocked reads the current sequence. Callers must
// hold host.mu (it calls the wasm export directly, unlike
// SnapshotSeq which takes host.mu itself).
func (s *SegDB) snapshotSeqLocked() (uint64, error) {
	fn := s.host.module.ExportedFunction("yowza_segdb_snapshot_seq")
	if fn == nil {
		return 0, ErrSegDBSnapshotSeqMissing
	}
	res, err := fn.Call(context.Background(), s.stateOff)
	if err != nil || len(res) == 0 {
		return 0, fmt.Errorf("yowza segdb: snapshot_seq call failed: %v", err)
	}
	return res[0], nil
}

// callU64Locked calls a wasm function returning one u64.
// Callers must hold host.mu.
func (s *SegDB) callU64Locked(fn api.Function, args ...uint64) (uint64, error) {
	res, err := fn.Call(context.Background(), args...)
	if err != nil || len(res) == 0 {
		return 0, err
	}
	return res[0], nil
}

// registerLeaseLocked is the FALLBACK-ONLY registration (Go
// map). Callers must hold leaseMu. It is never called on the
// C path.
func (s *SegDB) registerLeaseLocked(seq uint64) uint64 {
	if s.leases == nil {
		s.leases = map[uint64]uint64{}
	}
	if s.nextLeaseID == 0 {
		s.nextLeaseID = 1
	}
	id := s.nextLeaseID
	s.nextLeaseID++
	s.leases[id] = seq
	return id
}

// AcquireLease captures the current sequence (atomically with
// registration under leaseMu, so a concurrent Compact cannot
// slip between the capture and the registration) and returns
// the lease id and the captured sequence. Release with
// ReleaseLease; while the lease is active with a sequence below
// the current sequence, Compact defers. On the C path the
// lease is a row of the C table, so it also pins a compact
// issued directly through the wasm compact export, and a
// lease acquired directly through the C export is visible
// here. On the fallback path it behaves exactly as Gate 1
// did. Table-full on the C path returns ErrLeaseTableFull.
func (s *SegDB) AcquireLease() (uint64, uint64, error) {
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()
	if fnAcq, _, _, _, ok := s.leaseFuncs(); ok {
		s.host.mu.Lock()
		defer s.host.mu.Unlock()
		seq, err := s.snapshotSeqLocked()
		if err != nil {
			return 0, 0, err
		}
		id, err := s.callU64Locked(fnAcq, s.stateOff, seq)
		if err != nil {
			return 0, 0, err
		}
		if id == 0 {
			return 0, 0, ErrLeaseTableFull
		}
		return id, seq, nil
	}
	seq, err := s.SnapshotSeq()
	if err != nil {
		return 0, 0, err
	}
	return s.registerLeaseLocked(seq), seq, nil
}

// RegisterLease registers an explicit snapshot sequence (for
// example a sequence a caller captured earlier) as a lease.
// It fails with ErrSnapshotInvalid when seq is above the
// current sequence. The sequence is validated against the
// current sequence under leaseMu, so the same no-slip
// guarantee as AcquireLease holds: on the C path the check
// and the C acquire share one host.mu hold. Table-full on
// the C path returns ErrLeaseTableFull. Signatures and error
// values are unchanged from Gate 1; PersistedScanIter and
// query iterators use this method unchanged, and their leases
// now also pin C-side compacts.
func (s *SegDB) RegisterLease(seq uint64) (uint64, error) {
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()
	if fnAcq, _, _, _, ok := s.leaseFuncs(); ok {
		s.host.mu.Lock()
		defer s.host.mu.Unlock()
		cur, err := s.snapshotSeqLocked()
		if err != nil {
			return 0, err
		}
		if seq > cur {
			return 0, fmt.Errorf("%w: sequence %d above current %d", ErrSnapshotInvalid, seq, cur)
		}
		id, err := s.callU64Locked(fnAcq, s.stateOff, seq)
		if err != nil {
			return 0, err
		}
		if id == 0 {
			return 0, ErrLeaseTableFull
		}
		return id, nil
	}
	cur, err := s.SnapshotSeq()
	if err != nil {
		return 0, err
	}
	if seq > cur {
		return 0, fmt.Errorf("%w: sequence %d above current %d", ErrSnapshotInvalid, seq, cur)
	}
	return s.registerLeaseLocked(seq), nil
}

// ReleaseLease deactivates a lease. It returns ErrLeaseExpired
// (via errors.Is) for an unknown or already-released id. On
// the C path this releases the C-table row, so a lease
// acquired directly through the C export can be released
// here, and vice versa (a Go-acquired lease released through
// the C export is gone here too).
func (s *SegDB) ReleaseLease(id uint64) error {
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()
	if _, fnRel, _, _, ok := s.leaseFuncs(); ok {
		s.host.mu.Lock()
		defer s.host.mu.Unlock()
		rc, err := s.call(fnRel, s.stateOff, id)
		if err != nil {
			return err
		}
		if rc != 0 {
			return fmt.Errorf("%w: lease %d", ErrLeaseExpired, id)
		}
		return nil
	}
	if _, ok := s.leases[id]; !ok {
		return fmt.Errorf("%w: lease %d", ErrLeaseExpired, id)
	}
	delete(s.leases, id)
	return nil
}

// LeaseValid reports whether id is currently an active lease.
// On the C path it takes leaseMu then host.mu (validity lives
// in wasm memory, so the wasm call must be serialised like
// every other wasm call) and must NOT be called with host.mu
// already held; on the fallback path it takes leaseMu only,
// as Gate 1 did. Audited callers (query segPager,
// PersistedScanIter) hold no host.mu, so this is safe for
// them; no SegDB method calls LeaseValid while holding
// host.mu.
func (s *SegDB) LeaseValid(id uint64) bool {
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()
	if _, _, fnValid, _, ok := s.leaseFuncs(); ok {
		s.host.mu.Lock()
		defer s.host.mu.Unlock()
		rc, err := s.call(fnValid, s.stateOff, id)
		if err != nil {
			return false
		}
		return rc != 0
	}
	_, ok := s.leases[id]
	return ok
}

// segLeaseOldestHasActive is a scratch cell for LeaseOldest's
// has_active out-parameter: offset 6000 inside the 64 KiB
// scratch region, clear of the scan-cursor cells (which end
// at 5136) and of the page out buffer (which starts at 8192).
const segLeaseOldestHasActive uint64 = 6000

// LeaseOldest returns the smallest sequence among active
// leases, and whether any lease is active (false with a 0
// sequence when none is). Diagnostic only: Compact does not
// use it (segdb_compact checks the table itself). On the
// fallback path it scans the Go map under leaseMu.
func (s *SegDB) LeaseOldest() (uint64, bool, error) {
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()
	if _, _, _, fnOldest, ok := s.leaseFuncs(); ok {
		s.host.mu.Lock()
		defer s.host.mu.Unlock()
		res, err := fnOldest.Call(context.Background(), s.stateOff, s.scratchOff+segLeaseOldestHasActive)
		if err != nil || len(res) == 0 {
			return 0, false, err
		}
		b, ok := s.host.memRead(s.scratchOff+segLeaseOldestHasActive, 4)
		if !ok {
			return 0, false, fmt.Errorf("yowza segdb: failed to read lease oldest flag")
		}
		return res[0], binary.LittleEndian.Uint32(b) != 0, nil
	}
	var best uint64
	found := false
	for _, seq := range s.leases {
		if !found || seq < best {
			best, found = seq, true
		}
	}
	return best, found, nil
}

// SegDBCursorSource is the cursor interface a per-shard pager
// must consume to replace skip paging (Gate 0, persisted
// wiring — see GATE1_NOTES.md for the exact algorithm the
// parent must apply to PersistedScanIter in segdb_persist.go).
// SegDB itself satisfies it.
type SegDBCursorSource interface {
	ScanCursor(prefix string, snapshotSeq uint64, cursor []byte, hasCursor bool, limit uint32) ([]SegDBScanEntry, []byte, bool, error)
}

var _ SegDBCursorSource = (*SegDB)(nil)
