package yowza

// Track 3 (Timeline-native in WASM) — Go host binding for the
// Axis C yowza.c timeline exports.
//
// What this host is, precisely, and what it is not:
//
// The 7 C exports (yowza_timeline_set/clear/active/snapshot,
// yowza_execute_timeline, yowza_timeline_write_count,
// yowza_timeline_write_serialize) operate on the LEGACY
// single-ART engine inside one wasm module (global_art,
// ART commit sequence). A PersistedSegDB does NOT live in
// that engine: its data lives in per-shard SegDB heap
// stores, under per-shard SegDB snapshot sequences. There
// is therefore no pointer the host could hand the C VM
// that would make it read SegDB data directly, and this
// file does not pretend otherwise. The host bridges the
// two stores explicitly, in the open:
//
//   1. VALIDATE (before any wasm execution): the caller
//      supplies an explicit SegDB snapshot vector plus
//      optional per-shard lease IDs. A vector entry above
//      the shard's current SegDB sequence is a future
//      snapshot and is rejected with
//      ErrTimelineWASMFutureSnapshot; a supplied non-zero
//      lease ID that is no longer valid is rejected with
//      ErrTimelineWASMLeaseExpired. A closed handle, a
//      manager-mode handle, or a vector whose length does
//      not match the shard count is rejected with a typed
//      error. Nothing is ever silently read from the main
//      head: the all-zero vector is the defined empty
//      base, never "current".
//   2. MATERIALIZE: the pinned view is read out of the
//      SegDB layer — the handle's layered view (base +
//      staged writes) when the snapshot equals the
//      handle's base vector, the SegDB view at the vector
//      (via ScanStreaming, which re-pins internally) for
//      any other valid vector, nothing for the empty
//      base — sorted by key, and seeded into a FRESH
//      legacy-ART wasm instance with one legacy Put per
//      key. Seeding in sorted order makes the ART commit
//      sequence exactly the seed count, so the C
//      snapshot passed to yowza_execute_timeline is the
//      seed count (0 for the empty base), every seeded
//      node version is at most that snapshot, and a C
//      GET can never hit the ART-only -11 ambiguity for
//      a seeded key. Keys that exist only in the SegDB
//      main head after the pinned vector are NOT seeded,
//      so a C GET for them correctly reports absence
//      instead of leaking a live value.
//   3. EXECUTE (one-shot only): the program runs through
//      yowza_execute_timeline on the fresh seeded
//      instance. The sticky yowza_timeline_set handle is
//      never used, so no sticky state can leak into
//      another execution. A failed program stages
//      nothing into the destination handle (the one-shot
//      export returns an error pointer, not a VMContext,
//      so there is nothing to drain after a failure).
//   4. DRAIN: staged writes are serialized out of the C
//      VM (write_count + write_serialize, one entry per
//      call) and applied to the destination handle with
//      TimelineHandle.ApplyStagedWritesAtomic: all
//      entries are validated first, then applied under a
//      single handle lock, so a failed drain leaves the
//      handle unchanged (including its pre-existing
//      staged writes).
//   5. RESULTS: the C VM exposes only one response slot
//      (response_ptr/len, overwritten by every GET), so
//      one full execution cannot yield one result per
//      GET. Each GET/TX_GET result is therefore observed
//      by a real C execution of that read's program
//      prefix on a freshly seeded instance, terminated
//      neutrally (OP_EXIT outside a transaction,
//      OP_TX_COMMIT + OP_EXIT inside one — the commit
//      terminator cannot fail for a prefix of a program
//      whose own commit succeeded, and it does not rewind
//      the arena, so the response bytes stay readable).
//      Results are never synthesized in Go: every
//      returned value was produced by the C VM.
//
// Honest limits of this bridge (not hidden):
//   - OP_SCAN under a timeline is -11 in the C VM (the
//     ART has no snapshot scan), so a program containing
//     OP_SCAN is rejected upfront with
//     ErrTimelineWASMScanUnsupported and nothing is
//     executed or drained. Timeline scans remain a Go
//     TimelineHandle.Scan operation; see the parity
//     tests, which assert exactly this split.
//   - A failed C program stages nothing, while the Go
//     TimelineVM keeps legacy puts staged before the
//     failure (documented in timeline_vm.go). The host
//     is fail-closed here, not parity-identical; the
//     tests assert the host behaviour, not parity, for
//     failed programs.
//   - The C timeline write set caps at 128 staged
//     writes (VM_TIMELINE_MAX_WRITES); a program staging
//     more fails in C and drains nothing. The Go VM has
//     no such cap.
//   - CRDT timestamps are dropped by the drain format
//     (no timestamp field), exactly as the Go TimelineVM
//     ignores them; values are parity-identical.
//   - Lease validation checks lease liveness only: the
//     C lease API exposes no sequence-binding accessor,
//     so a live lease for a different sequence passes
//     liveness while the vector's future check still
//     bounds the sequence itself.
//   - Cost: one fresh wasm module per execution plus
//     one per observed read (prefix runs), and one
//     legacy Put per seeded key. This host is a
//     correctness bridge, not a fast path.
//   - The destination handle must be a standalone
//     handle (NewTimelineHandle). Manager-mode handles
//     have no SegDB snapshot vector, so they are
//     rejected rather than guessed at.

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
)

// Typed host errors. Compare with errors.Is. A future
// snapshot also wraps ErrSnapshotInvalid and an expired
// lease also wraps ErrLeaseExpired, so callers testing
// the SegDB sentinels see the same failure.
var (
	ErrTimelineWASMExportMissing   = errors.New("yowza timeline wasm host: wasm binary predates the timeline exports")
	ErrTimelineWASMSnapshotInvalid = errors.New("yowza timeline wasm host: invalid timeline snapshot")
	ErrTimelineWASMFutureSnapshot  = errors.New("yowza timeline wasm host: snapshot is in the future")
	ErrTimelineWASMLeaseExpired    = errors.New("yowza timeline wasm host: timeline lease expired or unknown")
	ErrTimelineWASMScanUnsupported = errors.New("yowza timeline wasm host: OP_SCAN is not supported under the C timeline (ART has no snapshot scan)")
	ErrTimelineWASMExec            = errors.New("yowza timeline wasm host: C timeline execution failed")
)

// TimelineWASMSnapshot is an explicit SegDB snapshot pin
// for one host execution. Vector is per-shard, in shard
// order; LeaseIDs is parallel to Vector (a zero entry
// means that shard is unpinned by the caller — it is
// still future-checked, and materialization re-pins
// internally for the scan). An all-zero Vector is the
// defined empty base, not the current head.
type TimelineWASMSnapshot struct {
	Vector   []uint64
	LeaseIDs []uint64
}

// IsEmpty reports whether the snapshot is the defined
// empty base (a non-empty, all-zero vector).
func (s TimelineWASMSnapshot) IsEmpty() bool {
	if len(s.Vector) == 0 {
		return false
	}
	for _, v := range s.Vector {
		if v != 0 {
			return false
		}
	}
	return true
}

// TimelineWASMEmptySnapshot returns the empty-base
// snapshot for a database with numShards shards.
func TimelineWASMEmptySnapshot(numShards int) TimelineWASMSnapshot {
	return TimelineWASMSnapshot{Vector: make([]uint64, numShards)}
}

// TimelineWASMSnapshotFromHandle returns the snapshot
// pin for a standalone handle: its base vector plus its
// base leases when the base is a *snapshotHandle (the
// constructors in timeline_handle.go always produce
// one). A manager-mode handle has no SegDB vector and
// is rejected.
func TimelineWASMSnapshotFromHandle(h *TimelineHandle) (TimelineWASMSnapshot, error) {
	if h == nil {
		return TimelineWASMSnapshot{}, fmt.Errorf("%w: nil handle", ErrTimelineWASMSnapshotInvalid)
	}
	h.mu.Lock()
	mgr, base := h.mgr, h.base
	closed := h.closed
	h.mu.Unlock()
	if closed {
		return TimelineWASMSnapshot{}, ErrTimelineHandleClosed
	}
	if mgr != nil {
		return TimelineWASMSnapshot{}, fmt.Errorf("%w: manager-mode handles have no SegDB snapshot vector", ErrTimelineWASMSnapshotInvalid)
	}
	snap := TimelineWASMSnapshot{Vector: h.BaseVector()}
	if sh, ok := base.(*snapshotHandle); ok {
		snap.LeaseIDs = append([]uint64(nil), sh.leaseIDs...)
	}
	return snap, nil
}

// TimelineWASMHost connects one destination
// TimelineHandle to C timeline execution. It owns no
// live wasm instance between calls: every Execute
// creates fresh instances (seeding would otherwise
// contaminate the next run) and closes them before
// returning. The host itself therefore needs no Close.
type TimelineWASMHost struct {
	wasmBytes []byte
	dst       *TimelineHandle
}

// NewTimelineWASMHost builds a host for dst. It
// fail-fasts on a missing timeline export set by
// probing one fresh module instance (closed again
// immediately); a binary without the 7 exports is
// rejected here, never degraded to the Go VM at
// Execute time.
func NewTimelineWASMHost(wasmBytes []byte, dst *TimelineHandle) (*TimelineWASMHost, error) {
	if len(wasmBytes) == 0 {
		return nil, fmt.Errorf("%w: empty wasm binary", ErrTimelineWASMExportMissing)
	}
	if dst == nil {
		return nil, fmt.Errorf("%w: nil destination handle", ErrTimelineWASMSnapshotInvalid)
	}
	probe, err := NewYowzaDB(wasmBytes)
	if err != nil {
		return nil, err
	}
	defer probe.Close()
	if _, err := probe.timelineFuncs(); err != nil {
		return nil, err
	}
	return &TimelineWASMHost{wasmBytes: append([]byte(nil), wasmBytes...), dst: dst}, nil
}

// timelineFuncs looks up the 7 Axis C exports on a
// module, or returns ErrTimelineWASMExportMissing.
func (db *YowzaDB) timelineFuncs() (timelineExportSet, error) {
	names := []string{
		"yowza_timeline_set", "yowza_timeline_clear",
		"yowza_timeline_active", "yowza_timeline_snapshot",
		"yowza_execute_timeline", "yowza_timeline_write_count",
		"yowza_timeline_write_serialize",
	}
	fns := make(map[string]apiFunction, len(names))
	for _, n := range names {
		fn := db.module.ExportedFunction(n)
		if fn == nil {
			return timelineExportSet{}, fmt.Errorf("%w: missing %s", ErrTimelineWASMExportMissing, n)
		}
		fns[n] = fn
	}
	return timelineExportSet{
		executeTimeline: fns["yowza_execute_timeline"],
		writeCount:      fns["yowza_timeline_write_count"],
		writeSerialize:  fns["yowza_timeline_write_serialize"],
	}, nil
}

// apiFunction is the subset of api.Function this file
// uses, so timelineFuncs stays testable without
// importing wazero's api package here (the concrete
// type still comes from wazero via YowzaDB).
type apiFunction interface {
	Call(context.Context, ...uint64) ([]uint64, error)
}

type timelineExportSet struct {
	executeTimeline apiFunction
	writeCount      apiFunction
	writeSerialize  apiFunction
}

// validateSnapshot checks the destination and the
// snapshot BEFORE any wasm execution. It returns the
// destination DB for the caller's later use.
func (h *TimelineWASMHost) validateSnapshot(snap TimelineWASMSnapshot) (*PersistedSegDB, error) {
	dst := h.dst
	if dst == nil {
		return nil, fmt.Errorf("%w: nil destination handle", ErrTimelineWASMSnapshotInvalid)
	}
	dst.mu.Lock()
	closed, mgr, db := dst.closed, dst.mgr, dst.db
	dst.mu.Unlock()
	if closed {
		return nil, ErrTimelineHandleClosed
	}
	if mgr != nil {
		return nil, fmt.Errorf("%w: manager-mode handles have no SegDB snapshot vector", ErrTimelineWASMSnapshotInvalid)
	}
	if db == nil {
		return nil, fmt.Errorf("%w: handle has no database", ErrTimelineWASMSnapshotInvalid)
	}
	n := db.NumShards()
	if len(snap.Vector) != n {
		return nil, fmt.Errorf("%w: vector has %d entries, database has %d shards", ErrTimelineWASMSnapshotInvalid, len(snap.Vector), n)
	}
	if len(snap.LeaseIDs) != 0 && len(snap.LeaseIDs) != n {
		return nil, fmt.Errorf("%w: lease ids have %d entries, database has %d shards", ErrTimelineWASMSnapshotInvalid, len(snap.LeaseIDs), n)
	}
	// Future check first: a future vector must never
	// become a current head, even with valid leases.
	for i, v := range snap.Vector {
		cur, err := db.shards[i].sdb.SnapshotSeq()
		if err != nil {
			return nil, err
		}
		if v > cur {
			return nil, fmt.Errorf("%w: shard %d snapshot %d above current %d: %w", ErrTimelineWASMFutureSnapshot, i, v, cur, ErrSnapshotInvalid)
		}
	}
	for i, id := range snap.LeaseIDs {
		if id == 0 {
			continue
		}
		if !db.shards[i].sdb.LeaseValid(id) {
			return nil, fmt.Errorf("%w: shard %d lease %d: %w", ErrTimelineWASMLeaseExpired, i, id, ErrLeaseExpired)
		}
	}
	return db, nil
}

// materialize reads the pinned view. It runs after
// validation and before any wasm execution.
func (h *TimelineWASMHost) materialize(db *PersistedSegDB, snap TimelineWASMSnapshot) (map[string][]byte, error) {
	if snap.IsEmpty() {
		return map[string][]byte{}, nil
	}
	dst := h.dst
	if wasmHostEqualUint64Slice(snap.Vector, dst.BaseVector()) {
		it, err := dst.Scan("")
		if err != nil {
			return nil, err
		}
		defer it.Close()
		out := map[string][]byte{}
		for {
			page, err := it.NextBatch(64)
			if err != nil {
				return nil, err
			}
			if len(page) == 0 {
				break
			}
			for _, e := range page {
				out[string(e.Key)] = append([]byte(nil), e.Val...)
			}
		}
		return out, nil
	}
	it, err := db.ScanStreaming("", snap.Vector)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	out := map[string][]byte{}
	for {
		page, err := it.NextBatch(64)
		if err != nil {
			return nil, err
		}
		if len(page) == 0 {
			break
		}
		for _, e := range page {
			out[string(e.Key)] = append([]byte(nil), e.Val...)
		}
	}
	return out, nil
}

func wasmHostEqualUint64Slice(a, b []uint64) bool {
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

func wasmHostSortedKeys(m map[string][]byte) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// timelineRun is one C execution on a freshly seeded
// instance. The caller closes db.
type timelineRun struct {
	db     *YowzaDB
	vmPtr  uint64
	writes []TimelineStagedWrite
}

// runC executes program on a fresh instance seeded
// with view, under C snapshot cSnap, and drains its
// staged writes (parsed copies) before returning. The
// instance stays open in the returned run so the
// caller can read the response slot.
func (h *TimelineWASMHost) runC(program []byte, view map[string][]byte, cSnap uint64) (*timelineRun, error) {
	db, err := NewYowzaDB(h.wasmBytes)
	if err != nil {
		return nil, err
	}
	cleanup := func(e error) (*timelineRun, error) {
		_ = db.Close()
		return nil, e
	}
	fns, err := db.timelineFuncs()
	if err != nil {
		return cleanup(err)
	}
	for _, k := range wasmHostSortedKeys(view) {
		if err := db.Put(k, view[k]); err != nil {
			return cleanup(fmt.Errorf("yowza timeline wasm host: seeding key %q: %w", k, err))
		}
	}
	vmPtr, err := executeTimelineProgram(db, fns.executeTimeline, program, cSnap)
	if err != nil {
		return cleanup(err)
	}
	writes, err := drainTimelineWrites(db, fns, vmPtr)
	if err != nil {
		return cleanup(err)
	}
	return &timelineRun{db: db, vmPtr: vmPtr, writes: writes}, nil
}

// executeTimelineProgram writes program into the
// shared code slot and calls yowza_execute_timeline,
// decoding the error-pointer convention for both
// wasm32 and wasm64 (see YowzaDB.executeBytecode).
func executeTimelineProgram(db *YowzaDB, fn apiFunction, program []byte, cSnap uint64) (uint64, error) {
	if uint64(len(program)) > db.memOffsetCodeCap {
		return 0, fmt.Errorf("yowza timeline wasm host: bytecode too large")
	}
	if !db.memWrite(db.memOffsetCode, program) {
		return 0, fmt.Errorf("yowza timeline wasm host: failed to write bytecode to wasm memory")
	}
	res, err := fn.Call(context.Background(), uint64(db.memOffsetCode), uint64(len(program)), cSnap)
	if err != nil {
		return 0, fmt.Errorf("yowza timeline wasm host: execute_timeline call failed: %w", err)
	}
	if len(res) == 0 {
		return 0, fmt.Errorf("yowza timeline wasm host: execute_timeline returned no result")
	}
	r := res[0]
	if db.isMem64 {
		if int64(r) < 0 {
			return 0, timelineCError(int64(r))
		}
	} else {
		if uint32(r) >= 0xFFFFFFF0 {
			return 0, timelineCError(int64(int32(uint32(r))))
		}
	}
	if r == 0 {
		return 0, fmt.Errorf("yowza timeline wasm host: execute_timeline returned NULL")
	}
	return r, nil
}

// timelineCError maps a C vm_execute / glue error code
// to a typed Go error. -3 is the glue's future
// rejection; -11 is the timeline fail-closed code
// (scan, or a post-snapshot read — unreachable for
// seeded keys, but never softened into a value).
func timelineCError(code int64) error {
	switch code {
	case -3:
		return fmt.Errorf("%w: C timeline rejected a future snapshot (code -3): %w", ErrTimelineWASMFutureSnapshot, ErrSnapshotInvalid)
	case -11:
		return fmt.Errorf("%w: timeline read/scan refused (code -11)", ErrTimelineWASMExec)
	default:
		return fmt.Errorf("%w: code %d", ErrTimelineWASMExec, code)
	}
}

// readTimelineResponse reads the single response slot
// of a completed run: ptr == 0 means not found, even
// for a zero-length value (which has a non-NULL ptr).
func readTimelineResponse(db *YowzaDB, vmPtr uint64) (TimelineVMResult, error) {
	ctx := context.Background()
	lenArgs, err := db.fnResponseLen.Call(ctx, vmPtr)
	if err != nil || len(lenArgs) == 0 {
		return TimelineVMResult{}, fmt.Errorf("yowza timeline wasm host: response len call failed: %v", err)
	}
	ptrArgs, err := db.fnResponsePtr.Call(ctx, vmPtr)
	if err != nil || len(ptrArgs) == 0 {
		return TimelineVMResult{}, fmt.Errorf("yowza timeline wasm host: response ptr call failed: %v", err)
	}
	if ptrArgs[0] == 0 {
		return TimelineVMResult{Found: false}, nil
	}
	data, ok := db.memRead(ptrArgs[0], uint64(uint32(lenArgs[0])))
	if !ok {
		return TimelineVMResult{}, fmt.Errorf("yowza timeline wasm host: failed to read response from wasm memory")
	}
	return TimelineVMResult{Found: true, Value: append([]byte(nil), data...)}, nil
}

// drainTimelineWrites serializes every staged write
// out of the C VM into copies. The format is the
// Axis C drain format: [deleted u8][key_len u32 LE]
// [key][val_len u32 LE][val].
func drainTimelineWrites(db *YowzaDB, fns timelineExportSet, vmPtr uint64) ([]TimelineStagedWrite, error) {
	ctx := context.Background()
	cntArgs, err := fns.writeCount.Call(ctx, vmPtr)
	if err != nil || len(cntArgs) == 0 {
		return nil, fmt.Errorf("yowza timeline wasm host: write_count call failed: %v", err)
	}
	count := int32(uint32(cntArgs[0]))
	if count < 0 {
		return nil, fmt.Errorf("yowza timeline wasm host: write_count failed (code %d)", count)
	}
	out := make([]TimelineStagedWrite, 0, count)
	for i := int32(0); i < count; i++ {
		res, err := fns.writeSerialize.Call(ctx, vmPtr, uint64(i), uint64(db.memOffsetSnap), uint64(db.memOffsetSnapCap))
		if err != nil || len(res) == 0 {
			return nil, fmt.Errorf("yowza timeline wasm host: write_serialize(%d) call failed: %v", i, err)
		}
		n := int32(uint32(res[0]))
		if n < 0 {
			return nil, fmt.Errorf("yowza timeline wasm host: write_serialize(%d) failed (code %d)", i, n)
		}
		raw, ok := db.memRead(db.memOffsetSnap, uint64(n))
		if !ok {
			return nil, fmt.Errorf("yowza timeline wasm host: failed to read serialized write %d", i)
		}
		w, err := parseSerializedTimelineWrite(raw)
		if err != nil {
			return nil, fmt.Errorf("yowza timeline wasm host: staged write %d: %w", i, err)
		}
		out = append(out, w)
	}
	return out, nil
}

func parseSerializedTimelineWrite(raw []byte) (TimelineStagedWrite, error) {
	if len(raw) < 9 {
		return TimelineStagedWrite{}, fmt.Errorf("serialized write too short (%d bytes)", len(raw))
	}
	deleted := raw[0] != 0
	klen := binary.LittleEndian.Uint32(raw[1:5])
	if uint64(5)+uint64(klen)+4 > uint64(len(raw)) {
		return TimelineStagedWrite{}, fmt.Errorf("serialized write key length %d overruns buffer", klen)
	}
	key := string(raw[5 : 5+klen])
	off := 5 + int(klen)
	vlen := binary.LittleEndian.Uint32(raw[off : off+4])
	off += 4
	if uint64(off)+uint64(vlen) > uint64(len(raw)) {
		return TimelineStagedWrite{}, fmt.Errorf("serialized write value length %d overruns buffer", vlen)
	}
	if deleted {
		return TimelineStagedWrite{Key: key, Deleted: true}, nil
	}
	return TimelineStagedWrite{Key: key, Val: append([]byte(nil), raw[off:off+int(vlen)]...)}, nil
}

// ---- program parsing (pre-flight, no wasm) ----

type timelineWASMReadOp struct {
	op   byte // OpcodeGet or OpcodeTxGet
	key  string
	end  int  // offset just past this op
	inTx bool // true for TX_GET (terminator: commit, not exit)
}

type timelineWASMReader struct {
	b   []byte
	pc  int
	err error
}

func (r *timelineWASMReader) u8() byte {
	if r.err != nil || r.pc+1 > len(r.b) {
		r.err = fmt.Errorf("yowza timeline wasm host: truncated bytecode")
		return 0
	}
	v := r.b[r.pc]
	r.pc++
	return v
}

func (r *timelineWASMReader) u32() uint32 {
	if r.err != nil || r.pc+4 > len(r.b) {
		r.err = fmt.Errorf("yowza timeline wasm host: truncated bytecode")
		return 0
	}
	v := binary.LittleEndian.Uint32(r.b[r.pc:])
	r.pc += 4
	return v
}

func (r *timelineWASMReader) u64() uint64 {
	if r.err != nil || r.pc+8 > len(r.b) {
		r.err = fmt.Errorf("yowza timeline wasm host: truncated bytecode")
		return 0
	}
	v := binary.LittleEndian.Uint64(r.b[r.pc:])
	r.pc += 8
	return v
}

func (r *timelineWASMReader) bytes(n uint32) []byte {
	if r.err != nil || uint64(r.pc)+uint64(n) > uint64(len(r.b)) {
		r.err = fmt.Errorf("yowza timeline wasm host: truncated bytecode")
		return nil
	}
	v := append([]byte(nil), r.b[r.pc:r.pc+int(n)]...)
	r.pc += int(n)
	return v
}

// parseTimelineProgram walks the program exactly like
// the Go TimelineVM's reader (same field order per
// opcode), collecting read ops. It stops at OP_EXIT,
// ignoring trailing bytes, exactly as both VMs do. A
// scan op is a typed unsupported error, not a parse
// detail: the C VM cannot execute it under a timeline.
func parseTimelineProgram(program []byte) ([]timelineWASMReadOp, error) {
	r := &timelineWASMReader{b: program}
	var reads []timelineWASMReadOp
	txActive := false
	for r.pc < len(program) && r.err == nil {
		op := r.u8()
		switch {
		case op >= OpcodeTxBegin && op <= OpcodeTxPutCRDT:
			switch op {
			case OpcodeTxBegin:
				if txActive {
					return nil, fmt.Errorf("yowza timeline wasm host: tx begin inside a transaction")
				}
				_ = r.u64()
				_ = r.u64()
				_ = r.u32()
				_ = r.u64()
				txActive = true
			case OpcodeTxCommit, OpcodeTxAbort:
				if !txActive {
					return nil, fmt.Errorf("yowza timeline wasm host: tx commit/abort without begin")
				}
				txActive = false
			case OpcodeTxPut, OpcodeTxPutCRDT:
				if !txActive {
					return nil, fmt.Errorf("yowza timeline wasm host: tx put without begin")
				}
				if op == OpcodeTxPutCRDT {
					_ = r.u64()
				}
				_ = r.u8()
				klen := r.u32()
				_ = r.bytes(klen)
				vlen := r.u32()
				_ = r.bytes(vlen)
			case OpcodeTxDel:
				if !txActive {
					return nil, fmt.Errorf("yowza timeline wasm host: tx del without begin")
				}
				klen := r.u32()
				_ = r.bytes(klen)
			case OpcodeTxGet:
				if !txActive {
					return nil, fmt.Errorf("yowza timeline wasm host: tx get without begin")
				}
				klen := r.u32()
				key := r.bytes(klen)
				if r.err == nil {
					reads = append(reads, timelineWASMReadOp{op: op, key: string(key), end: r.pc, inTx: true})
				}
			case OpcodeTxAssert:
				if !txActive {
					return nil, fmt.Errorf("yowza timeline wasm host: tx assert without begin")
				}
				kind := r.u8()
				klen := r.u32()
				_ = r.bytes(klen)
				vlen := r.u32()
				_ = r.bytes(vlen)
				if r.err == nil && kind > 2 {
					return nil, fmt.Errorf("yowza timeline wasm host: unknown assert kind %d", kind)
				}
			default:
				return nil, fmt.Errorf("yowza timeline wasm host: unknown tx opcode 0x%02x", op)
			}
		default:
			if txActive {
				return nil, fmt.Errorf("yowza timeline wasm host: legacy op 0x%02x inside a transaction", op)
			}
			switch op {
			case OpcodePut, OpcodePutRemote, OpcodePutCRDT:
				if op == OpcodePutCRDT {
					_ = r.u64()
				}
				klen := r.u32()
				_ = r.bytes(klen)
				vlen := r.u32()
				_ = r.bytes(vlen)
			case OpcodeGet:
				klen := r.u32()
				key := r.bytes(klen)
				if r.err == nil {
					reads = append(reads, timelineWASMReadOp{op: op, key: string(key), end: r.pc, inTx: false})
				}
			case OpcodeDel, OpcodeDelRemote:
				klen := r.u32()
				_ = r.bytes(klen)
			case OpcodeScan:
				return nil, ErrTimelineWASMScanUnsupported
			case OpcodeExit:
				return reads, nil
			default:
				return nil, fmt.Errorf("yowza timeline wasm host: unknown opcode 0x%02x", op)
			}
		}
	}
	if r.err != nil {
		return nil, r.err
	}
	if txActive {
		return nil, fmt.Errorf("yowza timeline wasm host: program ended with an open transaction")
	}
	return reads, nil
}

// Execute runs one program under snap through the C
// timeline VM and drains its staged writes into the
// destination handle atomically. On any error it
// returns no results and the handle is unchanged
// (failed programs drain nothing; a failed drain
// applies nothing). Results are the program's
// GET/TX_GET observations, in program order, with the
// same TimelineVMResult shape as the Go TimelineVM.
func (h *TimelineWASMHost) Execute(program []byte, snap TimelineWASMSnapshot) ([]TimelineVMResult, error) {
	db, err := h.validateSnapshot(snap)
	if err != nil {
		return nil, err
	}
	reads, err := parseTimelineProgram(program)
	if err != nil {
		return nil, err
	}
	view, err := h.materialize(db, snap)
	if err != nil {
		return nil, err
	}
	// C snapshot: the seed count (0 for the empty base).
	// Seeding is sorted, one legacy Put per key, so the
	// ART commit sequence after seeding is exactly this.
	cSnap := uint64(len(view))
	full, err := h.runC(program, view, cSnap)
	if err != nil {
		return nil, err
	}
	// Full run's instance stays open only until its
	// writes are drained (runC already parsed them);
	// close it before the prefix runs.
	_ = full.db.Close()
	// Observe each read with a real C prefix execution.
	results := make([]TimelineVMResult, 0, len(reads))
	for _, rd := range reads {
		prefix := append([]byte(nil), program[:rd.end]...)
		if rd.inTx {
			prefix = append(prefix, OpcodeTxCommit, OpcodeExit)
		} else {
			prefix = append(prefix, OpcodeExit)
		}
		run, err := h.runC(prefix, view, cSnap)
		if err != nil {
			return nil, err
		}
		res, err := readTimelineResponse(run.db, run.vmPtr)
		_ = run.db.Close()
		if err != nil {
			return nil, err
		}
		res.Op = rd.op
		res.Key = rd.key
		results = append(results, res)
	}
	// Drain LAST, atomically: only a fully successful
	// program with fully observed results stages into
	// the destination, and only all-or-nothing.
	if err := h.dst.ApplyStagedWritesAtomic(full.writes); err != nil {
		return nil, fmt.Errorf("yowza timeline wasm host: drain failed, handle unchanged: %w", err)
	}
	return results, nil
}
