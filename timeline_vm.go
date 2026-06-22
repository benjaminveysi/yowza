package yowza

// Phase D (Timeline-v2) — VM execution under a timeline
// handle, in Go.
//
// This is the wasm-path VM: a bytecode program (the same
// opcode encoding the C VM executes: OP_PUT/GET/DEL/SCAN
// and OP_TX_*, see include/vm.h) is interpreted against a
// TimelineHandle instead of the main head. GET/PUT/DELETE
// resolve against the handle's layered view (base
// snapshot + private write set); a transaction's writes
// stage in the VM's own transaction write set and, on
// commit, merge into the handle's write set with history
// capture on the NEXT unified commit (CommitToTimeline /
// MergeToMain, which run through the manager WAL or the
// Phase C SegDBTransaction facade path respectively).
//
// What this is NOT: it is not a second VM implementation
// competing with vm.c. The C VM timeline mode exists
// natively (vm_set_timeline + staged writeset in src/vm.c,
// include/vm.h), and the patch-1 Axis C work added the
// wasm binding for it (yowza_timeline_set/clear/active/
// snapshot, yowza_execute_timeline, and write-drain
// exports in src/yowza.c, proven by the native C test).
// The Go host side is now wired in timeline_wasm_host.go
// (Track 3): TimelineWASMHost validates an explicit
// SegDB snapshot vector + leases, materializes the
// pinned view into a fresh C ART instance (the C ART
// cannot read SegDB heap data directly, so the bridge
// is explicit seeding, not a pointer handoff), runs the
// program through yowza_execute_timeline, and drains
// staged writes into the destination handle with
// ApplyStagedWritesAtomic. THIS interpreter remains the
// reference Go path and the only path that scans under
// a timeline (the C VM refuses OP_SCAN with -11) and
// that keeps legacy puts staged before a failed program
// (a failed C program drains nothing). The two share
// opcode encodings and staged-write semantics by
// construction; see timeline_wasm_host.go for the full
// list of parity divergences, stated, not hidden.
//
// Semantics, precisely:
//   - Legacy ops inside an open transaction are a
//     protocol error (as in vm.c, -7 equivalent).
//   - EXIT (or program end) with an open transaction
//     discards it and errors.
//   - TX_GET reads tx writes, then the handle view.
//   - TX_ASSERT is checked at assert time against
//     tx writes over the handle view, and UNIQUE puts
//     are re-checked at commit against the handle view
//     (a key the transaction itself wrote does not
//     satisfy its own uniqueness check; a key present
//     in the handle view but overwritten by the tx
//     still fails UNIQUE, matching tx.c's rule that
//     uniqueness is about committed state, not the
//     projection).
//   - TX_COMMIT merges the tx write set into the
//     handle's write set (last write per key wins);
//     it does NOT write the main head, the base, or
//     the manager timeline. Applying to a durable
//     layer is the caller's explicit CommitToTimeline
//     / MergeToMain afterwards.
//   - OP_SCAN returns the layered view for the prefix
//     (handle Scan, paged internally); the C VM's
//     timeline mode refuses SCAN (fail-closed, see
//     vm.h) because the ART has no snapshot scan —
//     this Go path is the one that scans timelines.
// Results: Execute returns one TimelineVMResult per
// GET/SCAN/TX_GET operation, in program order, so a
// test can assert exactly what the program observed.
// A failed program returns an error and no results;
// the handle's write set is unchanged by a failed
// program except for legacy puts/deletes already
// staged before the failure (legacy ops stage
// immediately, as in vm.c; only tx writes roll back
// with the transaction).

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// TimelineVMResult is one observed read from a program.
type TimelineVMResult struct {
	Op    byte   // OpcodeGet, OpcodeScan, or OpcodeTxGet
	Key   string // for GET/TX_GET
	Found bool
	Value []byte // for GET/TX_GET when Found
	Rows  []SegDBScanEntry // for SCAN
}

// TimelineVM executes bytecode against a handle.
type TimelineVM struct {
	h *TimelineHandle
}

// NewTimelineVM binds a VM to a handle. The handle must
// stay open for the VM's lifetime.
func NewTimelineVM(h *TimelineHandle) *TimelineVM {
	return &TimelineVM{h: h}
}

type vmTx struct {
	active bool
	writes map[string]*timelineWrite
	// asserts recorded in order; checked at assert time
	// and UNIQUE re-checked at commit.
	asserts []vmAssert
	// unique holds keys flagged TX_WRITE_UNIQUE (0x01)
	// by a TX_PUT in this transaction.
	unique []string
}

type vmAssert struct {
	kind byte // 0 absent, 1 present, 2 value-equals
	key  string
	val  []byte
}

type vmReader struct {
	b   []byte
	pc  int
	err error
}

func (r *vmReader) u8() byte {
	if r.err != nil || r.pc+1 > len(r.b) {
		r.err = fmt.Errorf("yowza timeline vm: truncated bytecode")
		return 0
	}
	v := r.b[r.pc]
	r.pc++
	return v
}

func (r *vmReader) u32() uint32 {
	if r.err != nil || r.pc+4 > len(r.b) {
		r.err = fmt.Errorf("yowza timeline vm: truncated bytecode")
		return 0
	}
	v := binary.LittleEndian.Uint32(r.b[r.pc:])
	r.pc += 4
	return v
}

func (r *vmReader) u64() uint64 {
	if r.err != nil || r.pc+8 > len(r.b) {
		r.err = fmt.Errorf("yowza timeline vm: truncated bytecode")
		return 0
	}
	v := binary.LittleEndian.Uint64(r.b[r.pc:])
	r.pc += 8
	return v
}

func (r *vmReader) bytes(n uint32) []byte {
	if r.err != nil || uint64(r.pc)+uint64(n) > uint64(len(r.b)) {
		r.err = fmt.Errorf("yowza timeline vm: truncated bytecode")
		return nil
	}
	v := append([]byte(nil), r.b[r.pc:r.pc+int(n)]...)
	r.pc += int(n)
	return v
}

// Execute runs one program to completion (or the first
// error) and returns its observed reads.
func (v *TimelineVM) Execute(program []byte) ([]TimelineVMResult, error) {
	r := &vmReader{b: program}
	var results []TimelineVMResult
	var tx *vmTx
	layered := func(key string) ([]byte, bool, error) {
		if tx != nil && tx.active {
			if w, ok := tx.writes[key]; ok {
				if w.deleted {
					return nil, false, nil
				}
				return w.val, true, nil
			}
		}
		val, err := v.h.Get(key)
		if err != nil {
			// Absence in the handle view is not an error
			// here; every other error is.
			if isTimelineNotFound(err) {
				return nil, false, nil
			}
			return nil, false, err
		}
		return val, true, nil
	}
	checkAssert := func(a vmAssert, view func(string) ([]byte, bool, error)) error {
		val, ok, err := view(a.key)
		if err != nil {
			return err
		}
		switch a.kind {
		case 0:
			if ok {
				return fmt.Errorf("yowza timeline vm: assert absent failed for %q", a.key)
			}
		case 1:
			if !ok {
				return fmt.Errorf("yowza timeline vm: assert present failed for %q", a.key)
			}
		case 2:
			if !ok || string(val) != string(a.val) {
				return fmt.Errorf("yowza timeline vm: assert value-equals failed for %q", a.key)
			}
		default:
			return fmt.Errorf("yowza timeline vm: unknown assert kind %d", a.kind)
		}
		return nil
	}
	for r.pc < len(program) && r.err == nil {
		op := r.u8()
		switch {
		case op >= OpcodeTxBegin && op <= OpcodeTxPutCRDT:
			switch op {
			case OpcodeTxBegin:
				if tx != nil && tx.active {
					return nil, fmt.Errorf("yowza timeline vm: tx begin inside a transaction")
				}
				_ = r.u64() // tx_id
				_ = r.u64() // origin
				_ = r.u32() // consistency
				_ = r.u64() // schema
				tx = &vmTx{active: true, writes: map[string]*timelineWrite{}}
			case OpcodeTxCommit:
				if tx == nil || !tx.active {
					return nil, fmt.Errorf("yowza timeline vm: tx commit without begin")
				}
				// UNIQUE re-check against the handle
				// view (not the tx projection): a tx
				// write flagged unique fails if its key
				// is present in the handle view.
				for _, k := range tx.uniqueKeys() {
					if _, ok, err := handleView(v.h, k); err != nil {
						return nil, err
					} else if ok {
						tx = nil
						return nil, fmt.Errorf("yowza timeline vm: unique put failed for %q", k)
					}
				}
				for k, w := range tx.writes {
					if w.deleted {
						if err := v.h.Delete(k); err != nil {
							return nil, err
						}
					} else {
						if err := v.h.Put(k, w.val); err != nil {
							return nil, err
						}
					}
				}
				tx = nil
			case OpcodeTxAbort:
				if tx == nil || !tx.active {
					return nil, fmt.Errorf("yowza timeline vm: tx abort without begin")
				}
				tx = nil
			case OpcodeTxPut, OpcodeTxPutCRDT:
				if tx == nil || !tx.active {
					return nil, fmt.Errorf("yowza timeline vm: tx put without begin")
				}
				if op == OpcodeTxPutCRDT {
					_ = r.u64() // timestamp (LWW not enforced at handle level)
				}
				flags := r.u8()
				klen := r.u32()
				key := r.bytes(klen)
				vlen := r.u32()
				val := r.bytes(vlen)
				if r.err != nil {
					break
				}
				if flags&0x01 != 0 {
					tx.unique = append(tx.unique, string(key))
				}
				tx.writes[string(key)] = &timelineWrite{val: val}
			case OpcodeTxDel:
				if tx == nil || !tx.active {
					return nil, fmt.Errorf("yowza timeline vm: tx del without begin")
				}
				klen := r.u32()
				key := r.bytes(klen)
				if r.err != nil {
					break
				}
				tx.writes[string(key)] = &timelineWrite{deleted: true}
			case OpcodeTxGet:
				if tx == nil || !tx.active {
					return nil, fmt.Errorf("yowza timeline vm: tx get without begin")
				}
				klen := r.u32()
				key := r.bytes(klen)
				if r.err != nil {
					break
				}
				val, ok, err := layered(string(key))
				if err != nil {
					return nil, err
				}
				results = append(results, TimelineVMResult{Op: op, Key: string(key), Found: ok, Value: val})
			case OpcodeTxAssert:
				if tx == nil || !tx.active {
					return nil, fmt.Errorf("yowza timeline vm: tx assert without begin")
				}
				kind := r.u8()
				klen := r.u32()
				key := r.bytes(klen)
				vlen := r.u32()
				val := r.bytes(vlen)
				if r.err != nil {
					break
				}
				a := vmAssert{kind: kind, key: string(key), val: val}
				if err := checkAssert(a, layered); err != nil {
					return nil, err
				}
				tx.asserts = append(tx.asserts, a)
			default:
				return nil, fmt.Errorf("yowza timeline vm: unknown tx opcode 0x%02x", op)
			}
		default:
			if tx != nil && tx.active {
				return nil, fmt.Errorf("yowza timeline vm: legacy op 0x%02x inside a transaction", op)
			}
			switch op {
			case OpcodePut, OpcodePutRemote, OpcodePutCRDT:
				if op == OpcodePutCRDT {
					_ = r.u64()
				}
				klen := r.u32()
				key := r.bytes(klen)
				vlen := r.u32()
				val := r.bytes(vlen)
				if r.err != nil {
					break
				}
				if err := v.h.Put(string(key), val); err != nil {
					return nil, err
				}
			case OpcodeGet:
				klen := r.u32()
				key := r.bytes(klen)
				if r.err != nil {
					break
				}
				val, ok, err := layered(string(key))
				if err != nil {
					return nil, err
				}
				results = append(results, TimelineVMResult{Op: op, Key: string(key), Found: ok, Value: val})
			case OpcodeDel, OpcodeDelRemote:
				klen := r.u32()
				key := r.bytes(klen)
				if r.err != nil {
					break
				}
				if err := v.h.Delete(string(key)); err != nil {
					return nil, err
				}
			case OpcodeScan:
				plen := r.u32()
				prefix := r.bytes(plen)
				_ = r.u32() // requested out-buffer size (Go path grows; not a cap)
				if r.err != nil {
					break
				}
				it, err := v.h.Scan(string(prefix))
				if err != nil {
					return nil, err
				}
				var rows []SegDBScanEntry
				for {
					page, err := it.NextBatch(64)
					if err != nil {
						_ = it.Close()
						return nil, err
					}
					if len(page) == 0 {
						break
					}
					rows = append(rows, page...)
				}
				_ = it.Close()
				results = append(results, TimelineVMResult{Op: op, Rows: rows})
			case OpcodeExit:
				if r.err != nil {
					break
				}
				if tx != nil && tx.active {
					return nil, fmt.Errorf("yowza timeline vm: exit with an open transaction")
				}
				return results, nil
			default:
				return nil, fmt.Errorf("yowza timeline vm: unknown opcode 0x%02x", op)
			}
		}
	}
	if r.err != nil {
		return nil, r.err
	}
	if tx != nil && tx.active {
		return nil, fmt.Errorf("yowza timeline vm: program ended with an open transaction")
	}
	return results, nil
}

func (t *vmTx) uniqueKeys() []string { return t.unique }

// handleView reads the handle view without tx staging
// (used for UNIQUE re-checks at commit).
func handleView(h *TimelineHandle, key string) ([]byte, bool, error) {
	val, err := h.Get(key)
	if err != nil {
		if isTimelineNotFound(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return val, true, nil
}

func isTimelineNotFound(err error) bool {
	if err == nil {
		return false
	}
	// Both layers report absence through the same
	// sentinel; errors.Is is the check, and the string
	// fallback covers the snapshot layer's plain error
	// (which TimelineHandle.Get already normalises).
	if errors.Is(err, ErrTimelineKeyNotFound) {
		return true
	}
	return containsSub(err.Error(), "not found")
}
