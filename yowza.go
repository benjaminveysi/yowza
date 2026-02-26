package yowza

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"
	"sync"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
)

// YowzaDB represents the Go interface to the Wasm engine.
type YowzaDB struct {
	mu       sync.RWMutex
	runtime  wazero.Runtime
	module   api.Module
	memory   api.Memory
	memory64 api.Memory64
	isMem64  bool
	bytecode []byte

	// Wasm Exports
	fnInit        api.Function
	fnExecute     api.Function
	fnResponsePtr api.Function
	fnResponseLen api.Function
	fnWalLen      api.Function
	fnClearWal    api.Function
	fnSnapCreate  api.Function
	fnSnapRestore api.Function
	fnCompact     api.Function
	// fnScan is optional: it is nil on binaries built before yowza_scan
	// was exported (see ScanEntries). It must never be added to the
	// required-function check in NewYowzaDB, or old binaries would stop
	// loading even though the OP_SCAN path still works for them.
	fnScan api.Function

	// Memory Layout configuration (managed by Go)
	memOffsetArena   uint64
	memOffsetWal     uint64
	memOffsetCode    uint64
	memOffsetCodeCap uint64
	memOffsetSnap    uint64
	memOffsetSnapCap uint64
}

const (
	OpcodeExit      byte = 0x00
	OpcodePut       byte = 0x01
	OpcodeGet       byte = 0x02
	OpcodeDel       byte = 0x03
	OpcodeScan      byte = 0x04
	OpcodePutRemote byte = 0x05
	OpcodeDelRemote byte = 0x06
	OpcodePutCRDT   byte = 0x07
	// Phase 1 transactional opcodes (see tx.h / PHASE1_NOTES.md).
	OpcodeTxBegin   byte = 0x10
	OpcodeTxCommit  byte = 0x11
	OpcodeTxAbort   byte = 0x12
	OpcodeTxPut     byte = 0x13
	OpcodeTxDel     byte = 0x14
	OpcodeTxGet     byte = 0x15
	OpcodeTxAssert  byte = 0x16
	OpcodeTxPutCRDT byte = 0x17
	// WAL transaction frame marker (see tx.c frame layout).
	OpcodeTxFrame byte = 0xF0
)

// wasm page size in bytes.
const wasmPageSize = 65536

// readUleb128 reads one unsigned LEB128 value from b starting at *off.
// It reports false if the value is truncated or overflows a uint64.
func readUleb128(b []byte, off *int) (uint64, bool) {
	var result uint64
	var shift uint
	for {
		if *off >= len(b) || shift >= 64 {
			return 0, false
		}
		c := b[*off]
		*off++
		result |= uint64(c&0x7F) << shift
		if c&0x80 == 0 {
			return result, true
		}
		shift += 7
	}
}

// wasmIsMemory64 reports whether the wasm binary declares a memory64 memory.
// It parses the memory section (section id 5) and checks bit 2 (0x04) of each
// memory's limits flags. It returns false on any parse failure, i.e. it treats
// an unparseable binary as memory32; callers must not call the 32-bit Size()
// on a module that turned out to be memory64, so a valid binary is assumed.
// This is deliberately independent of api.MemoryDefinition, which exposes
// only Min/Max and has no IsMemory64 accessor.
func wasmIsMemory64(wasmBytes []byte) bool {
	// Wasm magic + version.
	if len(wasmBytes) < 8 || wasmBytes[0] != 0x00 || wasmBytes[1] != 'a' ||
		wasmBytes[2] != 's' || wasmBytes[3] != 'm' {
		return false
	}
	off := 8
	for off < len(wasmBytes) {
		sectionID, ok := readUleb128(wasmBytes, &off)
		if !ok {
			return false
		}
		sectionSize, ok := readUleb128(wasmBytes, &off)
		if !ok || uint64(len(wasmBytes)-off) < sectionSize {
			return false
		}
		sectionEnd := off + int(sectionSize)
		if sectionID == 5 {
			// Memory section: count, then one limits entry per memory.
			inner := off
			count, ok := readUleb128(wasmBytes, &inner)
			if !ok {
				return false
			}
			for i := uint64(0); i < count; i++ {
				flags, ok := readUleb128(wasmBytes, &inner)
				if !ok {
					return false
				}
				if flags&0x04 != 0 {
					return true
				}
				// Skip min, and max if present (bit 0).
				if _, ok := readUleb128(wasmBytes, &inner); !ok {
					return false
				}
				if flags&0x01 != 0 {
					if _, ok := readUleb128(wasmBytes, &inner); !ok {
						return false
					}
				}
				// Note: shared memories (bit 1) have no extra fields here.
			}
			return false
		}
		off = sectionEnd
	}
	return false
}

// memSize returns the current linear-memory size in bytes.
func (db *YowzaDB) memSize() uint64 {
	if db.isMem64 && db.memory64 != nil {
		return db.memory64.Size64()
	}
	return uint64(db.memory.Size())
}

// memGrow grows linear memory by deltaPages pages (65536 bytes each) and
// reports whether the runtime accepted the growth. The caller must check
// the return value: an ignored growth is an error, not something to retry
// in a loop.
func (db *YowzaDB) memGrow(deltaPages uint64) bool {
	if db.isMem64 && db.memory64 != nil {
		_, ok := db.memory64.Grow64(deltaPages)
		return ok
	}
	if deltaPages > math.MaxUint32 {
		return false
	}
	_, ok := db.memory.Grow(uint32(deltaPages))
	return ok
}

// memGrowTo grows linear memory until it is at least targetBytes, growing
// *to the target* instead of blindly adding a fixed page count. If memory
// already covers the target it is a no-op. It returns an error if growth
// is refused, so callers never spin forever on a failed Grow.
func (db *YowzaDB) memGrowTo(targetBytes uint64) error {
	size := db.memSize()
	if size >= targetBytes {
		return nil
	}
	need := (targetBytes - size + wasmPageSize - 1) / wasmPageSize
	if !db.memGrow(need) {
		return fmt.Errorf("failed to grow wasm memory to %d bytes (current %d bytes, need %d pages)", targetBytes, size, need)
	}
	if db.memSize() < targetBytes {
		return fmt.Errorf("wasm memory growth accepted but still below %d bytes (now %d bytes)", targetBytes, db.memSize())
	}
	return nil
}

// memRead reads byteCount bytes at a uint64 offset from linear memory.
// On the memory32 path, offsets/counts above 2^32-1 are out of range.
func (db *YowzaDB) memRead(offset, byteCount uint64) ([]byte, bool) {
	if db.isMem64 && db.memory64 != nil {
		return db.memory64.Read64(offset, byteCount)
	}
	if offset > math.MaxUint32 || byteCount > math.MaxUint32 {
		return nil, false
	}
	return db.memory.Read(uint32(offset), uint32(byteCount))
}

// memWrite writes val at a uint64 offset into linear memory.
func (db *YowzaDB) memWrite(offset uint64, val []byte) bool {
	if db.isMem64 && db.memory64 != nil {
		return db.memory64.Write64(offset, val)
	}
	if offset > math.MaxUint32 {
		return false
	}
	return db.memory.Write(uint32(offset), val)
}

func NewYowzaDB(wasmBytes []byte) (*YowzaDB, error) {
	ctx := context.Background()

	// Dual-mode runtime: memory64 is enabled, but the same config also
	// instantiates memory32 modules (yowza.wasm), which simply do not use
	// the feature. Do not remove this: without it a memory64 module fails
	// validation at Instantiate time.
	features := api.CoreFeaturesV2.SetEnabled(api.CoreFeatureMemory64, true)
	// Deliberately NOT WithMemoryCapacityFromMax(true) (the old code used
	// it): for a module with no declared max, that flag makes wazero set
	// Cap to the 65536-page limit, and the memory32 path then does
	// make([]byte, minBytes, 4GiB-cap) — a 4GiB backing array per DB,
	// which made creating several DBs in one process slow and then
	// SIGKILLed (OOM). With the default (capacity = min), the buffer
	// starts at 18 pages and grows on demand via memGrowTo, which is
	// exactly how Yowza already drives growth.
	r := wazero.NewRuntimeWithConfig(ctx, wazero.NewRuntimeConfig().WithCoreFeatures(features))

	// Since we compile without standard library, but we might have undefined symbols
	// if we accidentally depend on something, wazero requires us to either satisfy them
	// or not have them. Clang might emit env imports depending on flags. Let's explicitly instantiate empty env if needed.
	_, err := r.NewHostModuleBuilder("env").Instantiate(ctx)
	if err != nil {
		r.Close(ctx)
		return nil, fmt.Errorf("failed to instantiate env: %v", err)
	}

	mod, err := r.Instantiate(ctx, wasmBytes)
	if err != nil {
		r.Close(ctx)
		return nil, fmt.Errorf("failed to instantiate Wasm: %v", err)
	}

	mem := mod.Memory()
	if mem == nil {
		r.Close(ctx)
		return nil, fmt.Errorf("wasm module does not export memory")
	}

	// Dual-mode memory selection, via api.Module.Memory64() (added in the
	// Wazero64 step-2 patches): it returns nil for a memory32 module and
	// the MemoryInstance for a memory64 module, so a non-nil result is the
	// detection. Never call mem.Size() before isMem64 is known, because
	// the 32-bit methods panic on a memory64 module. As a cross-check, if
	// the binary's memory section declares memory64 but Memory64() is nil,
	// fail instead of silently running in 32-bit mode.
	mem64 := mod.Memory64()
	isMem64 := mem64 != nil
	if !isMem64 && wasmIsMemory64(wasmBytes) {
		r.Close(ctx)
		return nil, fmt.Errorf("wasm module declares memory64 but Memory64() returned nil")
	}

	db := &YowzaDB{
		runtime:  r,
		module:   mod,
		memory:   mem,
		memory64: mem64,
		isMem64:  isMem64,
		// Fixed layout for this demo
		memOffsetArena:   0,       // 0 to 1MB for DB Data
		memOffsetWal:     1048576, // 1MB to 1MB+128KB for WAL
		memOffsetCode:    1179648, // 1MB+128KB for Bytecode
		memOffsetCodeCap: 131072,  // 128KB for Bytecode
	}

	db.fnInit = mod.ExportedFunction("yowza_init")
	db.fnExecute = mod.ExportedFunction("yowza_execute")
	db.fnResponsePtr = mod.ExportedFunction("yowza_get_response_ptr")
	db.fnResponseLen = mod.ExportedFunction("yowza_get_response_len")
	db.fnWalLen = mod.ExportedFunction("yowza_get_wal_len")
	db.fnClearWal = mod.ExportedFunction("yowza_clear_wal")
	db.fnSnapCreate = mod.ExportedFunction("yowza_snapshot_create")
	db.fnSnapRestore = mod.ExportedFunction("yowza_snapshot_restore")
	db.fnCompact = mod.ExportedFunction("yowza_compact")
	db.fnScan = mod.ExportedFunction("yowza_scan")

	if db.fnInit == nil || db.fnExecute == nil || db.fnSnapCreate == nil || db.fnCompact == nil {
		r.Close(ctx)
		return nil, fmt.Errorf("missing exported Wasm functions")
	}

	// Initialize the C engine (Memory Buffer: 1MB, WAL Buffer: 128KB)
	// We MUST pass a non-zero address to arena memory, otherwise C considers it NULL and fails
	// Let's shift arena start to 1024 to avoid 0x0 (NULL pointer issue in C)
	// Let's also use an exported memory allocator to get a safe memory region from Wasm,
	// or allocate inside Go safely. Wait, wazero allows direct writing, but C expects an address
	// within its memory space. A statically compiled Wasm usually puts data at low addresses
	// and stack grows down from a predefined high address, and heap grows up from data end.
	// We should just use a high memory address well beyond static data and stack.
	// The stack size is set to 1MB in Makefile. Let's place our data at 2MB boundary.
	db.memOffsetArena = 2097152
	db.memOffsetWal = db.memOffsetArena + 1048576
	db.memOffsetCode = db.memOffsetWal + 131072
	db.memOffsetSnap = db.memOffsetCode + 131072
	db.memOffsetSnapCap = 1048576 // 1MB snapshot buffer

	// Ensure memory is large enough, growing *to* each target instead of
	// blindly adding pages. The old code did: start at 18 pages, Grow(32)
	// -> 50 pages, then Grow(80) -> 130 pages, when the real target was
	// 80 pages total (5242880 bytes). Growing to the target gives:
	// 18 pages -> 32 pages (2097152 bytes) -> 68 pages (4456448 bytes,
	// exactly memOffsetSnap + memOffsetSnapCap), with no overshoot.
	if err := db.memGrowTo(2097152); err != nil {
		r.Close(ctx)
		return nil, fmt.Errorf("failed to grow wasm memory for arena: %v", err)
	}
	if err := db.memGrowTo(db.memOffsetSnap + db.memOffsetSnapCap); err != nil {
		r.Close(ctx)
		return nil, fmt.Errorf("failed to grow wasm memory for snapshot buffer: %v", err)
	}

	initRes, err := db.fnInit.Call(ctx, uint64(db.memOffsetArena), 1048576, uint64(db.memOffsetWal), 131072)
	if err != nil {
		r.Close(ctx)
		return nil, fmt.Errorf("failed to init db: %v", err)
	}

	if initRes[0] != 0 {
		r.Close(ctx)
		return nil, fmt.Errorf("failed to init db engine: C returned %d", int32(initRes[0]))
	}

	return db, nil
}

// Close shuts down the runtime. It takes the exclusive lock, because the
// shared bytecode slot (memOffsetCode) and the static VMContext in C are
// not thread-safe: closing while another call is in flight would race it.
func (db *YowzaDB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.runtime.Close(context.Background())
}

// Internal execution engine
func (db *YowzaDB) executeBytecode(ctx context.Context, bytecode []byte) (uint64, error) {
	if len(bytecode) > int(db.memOffsetCodeCap) {
		return 0, fmt.Errorf("bytecode too large")
	}

	// Zero-copy: Write the query bytecode directly into Wasm Linear Memory
	if !db.memWrite(db.memOffsetCode, bytecode) {
		return 0, fmt.Errorf("failed to write bytecode to Wasm memory")
	}

	// Call the Wasm Interpreter
	res, err := db.fnExecute.Call(ctx, uint64(db.memOffsetCode), uint64(len(bytecode)))
	if err != nil {
		return 0, fmt.Errorf("execution panic: %v", err)
	}
	if len(res) == 0 {
		return 0, fmt.Errorf("engine execution returned no result. Bytecode: %x", bytecode)
	}

	// In C: return (VMContext*)(size_t)result on error.
	// Wasm pointer math: Wasm memory starts at 0. But we know our VMContext is statically allocated
	// in the Wasm data segment, which usually isn't 0. Let's assume values < 256 are error codes,
	// while valid pointers are larger.
	if res[0] == 0 {
		return 0, fmt.Errorf("engine execution failed (returned NULL). Bytecode: %x", bytecode)
	}

	// Check for our custom error codes returned from C.
	// In wasm64, C casts a negative int to (VMContext*)(size_t)result with
	// a 64-bit size_t, so -1 becomes 0xFFFFFFFFFFFFFFFF: as an int64 it
	// is < 0, and as a uint64 it is >= 0xFFFFFFFFFFFFFFFC. Either test
	// catches -1..-4 (and the step-1 codes -5, -6).
	// In wasm32 the same codes arrive as 32-bit values (0xFFFFFFFF,
	// 0xFFFFFFFE, ..., 0xFFFFFFFA for -1..-6), which are positive as
	// int64, so the 32-bit test below is still needed on that path.
	if int64(res[0]) < 0 || res[0] >= 0xFFFFFFFFFFFFFFFC {
		return 0, fmt.Errorf("engine execution failed with code %d. Bytecode: %x", int64(res[0]), bytecode)
	}
	// Phase 1: the threshold must cover the tx codes -7/-8/-9 too
	// (0xFFFFFFF9/8/7), not just the old -1..-6 range. A valid
	// VMContext pointer in wasm32 linear memory is far below
	// 0xFFFFFFF0, so this cannot mistake a real pointer for an error.
	if !db.isMem64 && uint32(res[0]) >= 0xFFFFFFF0 {
		return 0, fmt.Errorf("engine execution failed with code %d. Bytecode: %x", int32(uint32(res[0])), bytecode)
	}

	return res[0], nil
}

// WalEntry represents a single mutation operation from the Write-Ahead Log.
// Timestamp is only meaningful for OpcodePutCRDT records, where the C WAL
// format is [Op(1)][Timestamp u64 LE][KeyLen(4)][Key][ValLen(4)][Val];
// for all other opcodes it is 0. It is also the field Raft log entries
// carry for CRDT operations (see cluster/raft.go).
type WalEntry struct {
	Opcode    byte   `json:"opcode"`
	Key       []byte `json:"key"`
	Value     []byte `json:"value"`
	Timestamp uint64 `json:"timestamp,omitempty"`
	// Phase 1: set only for entries expanded from a transaction
	// frame (0 for legacy per-op records).
	TransactionID   uint64 `json:"transaction_id,omitempty"`
	LogicalSequence uint64 `json:"logical_sequence,omitempty"`
}

// parseWAL parses the binary WAL format into entries. It stops at the first
// truncated record instead of erroring, matching the old FetchWAL behaviour:
// a partially written tail is dropped, the complete records before it are kept.
func parseWAL(data []byte, walLen uint32) []WalEntry {
	var entries []WalEntry
	offset := uint32(0)

	// Parse WAL format:
	//   PUT/DEL: [Op(1)][KeyLen(4)][Key][ValLen(4)][Val]
	//   CRDT:    [Op(1)][Timestamp u64 LE][KeyLen(4)][Key][ValLen(4)][Val]
	for offset < walLen {
		if offset+1 > walLen {
			break
		}
		op := data[offset]

		// Phase 1 transaction frame: [0xF0]["YWTX"][frame_len u32]
		// [tx_id u64][seq u64][op_count u32][consistency u32]
		// [envelope+key+val]... [commit 0xC1][checksum u64 FNV-1a].
		// A frame is expanded only whole and only if its checksum
		// and commit marker verify; anything else stops the parse
		// exactly like a truncated legacy tail (see tx_replay_wal).
		if op == OpcodeTxFrame {
			if offset+33 > walLen {
				break
			}
			if string(data[offset+1:offset+5]) != "YWTX" {
				break
			}
			frameLen := binary.LittleEndian.Uint32(data[offset+5 : offset+9])
			if frameLen < 42 || offset+frameLen > walLen {
				break
			}
			frame := data[offset : offset+frameLen]
			if frame[frameLen-9] != 0xC1 {
				break
			}
			h := fnv.New64a()
			h.Write(frame[:frameLen-8])
			if h.Sum64() != binary.LittleEndian.Uint64(frame[frameLen-8:]) {
				break
			}
			txID := binary.LittleEndian.Uint64(frame[9:17])
			seq := binary.LittleEndian.Uint64(frame[17:25])
			opCount := binary.LittleEndian.Uint32(frame[25:29])
			pos := uint32(33)
			ok := true
			var frameEntries []WalEntry
			for i := uint32(0); i < opCount; i++ {
				// Envelope: 68 fixed bytes + 8*dep_count, then
				// key_len/key/val_len/val. Bounds are checked at
				// every step; any failure rejects the whole frame.
				if pos+68 > frameLen-9 {
					ok = false
					break
				}
				ep := pos
				if binary.LittleEndian.Uint32(frame[ep:ep+4]) != 1 {
					ok = false
					break
				}
				ep += 4 + 8 + 8 + 8 + 8
				opType := binary.LittleEndian.Uint32(frame[ep : ep+4])
				ep += 4
				if opType != 0x01 && opType != 0x03 {
					ok = false
					break
				}
				ep += 4 + 4
				ts := binary.LittleEndian.Uint64(frame[ep : ep+8])
				ep += 8
				depCount := binary.LittleEndian.Uint32(frame[ep : ep+4])
				ep += 4
				if depCount > 1024 || ep+depCount*8+8 > frameLen-9 {
					ok = false
					break
				}
				ep += depCount * 8
				payloadExpect := binary.LittleEndian.Uint64(frame[ep : ep+8])
				ep += 8
				// Payload checksum: envelope fields before the
				// checksum + key_len + key + val_len + val.
				ph := fnv.New64a()
				ph.Write(frame[pos : pos+60+depCount*8])
				if ep+4 > frameLen-9 {
					ok = false
					break
				}
				kl := binary.LittleEndian.Uint32(frame[ep : ep+4])
				ph.Write(frame[ep : ep+4])
				ep += 4
				if ep+kl > frameLen-9 {
					ok = false
					break
				}
				key := make([]byte, kl)
				copy(key, frame[ep:ep+kl])
				ph.Write(frame[ep : ep+kl])
				ep += kl
				if ep+4 > frameLen-9 {
					ok = false
					break
				}
				vl := binary.LittleEndian.Uint32(frame[ep : ep+4])
				ph.Write(frame[ep : ep+4])
				ep += 4
				if ep+vl > frameLen-9 {
					ok = false
					break
				}
				var val []byte
				if vl > 0 {
					val = make([]byte, vl)
					copy(val, frame[ep:ep+vl])
					ph.Write(frame[ep : ep+vl])
				}
				ep += vl
				if ph.Sum64() != payloadExpect {
					ok = false
					break
				}
				frameEntries = append(frameEntries, WalEntry{
					Opcode: byte(opType), Key: key, Value: val,
					Timestamp: ts, TransactionID: txID, LogicalSequence: seq,
				})
				pos = ep
			}
			if !ok || pos != frameLen-9 {
				break
			}
			entries = append(entries, frameEntries...)
			offset += frameLen
			continue
		}
		offset += 1

		var ts uint64
		if op == OpcodePutCRDT {
			if offset+8 > walLen {
				break
			}
			ts = binary.LittleEndian.Uint64(data[offset : offset+8])
			offset += 8
		}

		if offset+4 > walLen {
			break
		}
		kl := binary.LittleEndian.Uint32(data[offset : offset+4])
		offset += 4

		if offset+kl > walLen {
			break
		}
		key := make([]byte, kl)
		copy(key, data[offset:offset+kl])
		offset += kl

		if offset+4 > walLen {
			break
		}
		vl := binary.LittleEndian.Uint32(data[offset : offset+4])
		offset += 4

		var val []byte
		if vl > 0 {
			if offset+vl > walLen {
				break
			}
			val = make([]byte, vl)
			copy(val, data[offset:offset+vl])
			offset += vl
		}

		entries = append(entries, WalEntry{
			Opcode:    op,
			Key:       key,
			Value:     val,
			Timestamp: ts,
		})
	}

	return entries
}

// PeekWAL extracts all recent mutations from the engine's Delta Buffer and
// parses them into a Go slice, WITHOUT clearing the internal Wasm buffer
// and without compacting. Callers must call AckWAL only after the entries
// have been durably saved/published, so a failed save does not lose WAL
// records: the transactional pattern is Peek -> Save/Publish -> Ack.
func (db *YowzaDB) PeekWAL() ([]WalEntry, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	ctx := context.Background()

	// Retrieve current length of WAL
	lenArgs, err := db.fnWalLen.Call(ctx)
	if err != nil || len(lenArgs) == 0 {
		return nil, fmt.Errorf("failed to call yowza_get_wal_len: %v", err)
	}

	walLen := uint32(lenArgs[0])
	if walLen == 0 {
		return nil, nil // Empty WAL
	}

	// Read WAL from linear memory
	data, ok := db.memRead(db.memOffsetWal, uint64(walLen))
	if !ok {
		return nil, fmt.Errorf("failed to read WAL buffer from wasm memory")
	}

	return parseWAL(data, walLen), nil
}

// AckWAL clears the internal Wasm WAL buffer and runs the internal
// compaction that used to run inside FetchWAL, after the caller has
// durably saved/published the peeked entries. The WAL is cleared first, so
// if compaction fails the caller may safely retry AckWAL (clear is a no-op
// the second time, compaction runs again); it must not re-publish the
// entries, which were already saved.
func (db *YowzaDB) AckWAL() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	ctx := context.Background()

	// Clear WAL in C Engine
	if _, err := db.fnClearWal.Call(ctx); err != nil {
		return fmt.Errorf("failed to clear WAL in C engine: %v", err)
	}

	// Automated internal compaction triggered on WAL flush.
	// This prevents the Arena from leaking orphaned node memory between sweeps.
	return db.compactLocked(ctx)
}

// FetchWAL is now peek-only: it returns the current WAL entries WITHOUT
// clearing the internal Wasm buffer and without compacting. This is a
// deliberate breaking change from the old FetchWAL, which cleared on read
// (so a failed save after a fetch silently lost records). Use the
// transactional pattern instead: PeekWAL -> Save/Publish -> AckWAL.
// FetchWAL is kept as a peek-only alias for compatibility; it is exactly
// PeekWAL.
func (db *YowzaDB) FetchWAL() ([]WalEntry, error) {
	return db.PeekWAL()
}

// Developer API

func (db *YowzaDB) Put(key string, val []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	bytecode := make([]byte, 0, 1+4+len(key)+4+len(val)+1)
	bytecode = append(bytecode, OpcodePut)

	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(len(key)))
	bytecode = append(bytecode, buf...)
	bytecode = append(bytecode, key...)

	binary.LittleEndian.PutUint32(buf, uint32(len(val)))
	bytecode = append(bytecode, buf...)
	bytecode = append(bytecode, val...)

	bytecode = append(bytecode, OpcodeExit)

	_, err := db.executeBytecode(context.Background(), bytecode)
	return err
}

func (db *YowzaDB) PutRemote(key string, val []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	bytecode := make([]byte, 0, 1+4+len(key)+4+len(val)+1)
	bytecode = append(bytecode, OpcodePutRemote)

	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(len(key)))
	bytecode = append(bytecode, buf...)
	bytecode = append(bytecode, key...)

	binary.LittleEndian.PutUint32(buf, uint32(len(val)))
	bytecode = append(bytecode, buf...)
	bytecode = append(bytecode, val...)

	bytecode = append(bytecode, OpcodeExit)

	_, err := db.executeBytecode(context.Background(), bytecode)
	return err
}

func (db *YowzaDB) PutCRDT(key string, val []byte, timestamp uint64) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	bytecode := make([]byte, 0, 1+8+4+len(key)+4+len(val)+1)
	bytecode = append(bytecode, OpcodePutCRDT)

	buf8 := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf8, timestamp)
	bytecode = append(bytecode, buf8...)

	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(len(key)))
	bytecode = append(bytecode, buf...)
	bytecode = append(bytecode, key...)

	binary.LittleEndian.PutUint32(buf, uint32(len(val)))
	bytecode = append(bytecode, buf...)
	bytecode = append(bytecode, val...)

	bytecode = append(bytecode, OpcodeExit)

	_, err := db.executeBytecode(context.Background(), bytecode)
	return err
}

func (db *YowzaDB) DeleteRemote(key string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	bytecode := make([]byte, 0, 1+4+len(key)+1)
	bytecode = append(bytecode, OpcodeDelRemote)

	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(len(key)))
	bytecode = append(bytecode, buf...)
	bytecode = append(bytecode, key...)

	bytecode = append(bytecode, OpcodeExit)

	_, err := db.executeBytecode(context.Background(), bytecode)
	return err
}

// PutIfAbsent atomically inserts (key, value) only if key does not
// currently exist, under a single db.mu.Lock() acquisition. It returns
// (true, nil) if the key was inserted, or (false, nil) if it already
// existed. This is the idempotency primitive for framework and adapters:
// the old Get-then-Put pattern used two separate locks, so two concurrent
// webhooks could both observe "missing" and both run the handler.
//
// Implementation note: this must NOT call the public Get/Put methods,
// which take db.mu.Lock() themselves and would deadlock (sync.RWMutex
// is not reentrant). Instead it builds the same OpcodeGet/OpcodePut
// bytecodes Get/Put build and drives executeBytecode + fnResponsePtr
// directly, exactly as Get does for the existence check (response_ptr
// != 0 means found, even for an empty value).
func (db *YowzaDB) PutIfAbsent(key string, value []byte) (bool, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	ctx := context.Background()
	buf := make([]byte, 4)

	// Step 1: existence check, under the same lock as step 2.
	getCode := make([]byte, 0, 1+4+len(key)+1)
	getCode = append(getCode, OpcodeGet)
	binary.LittleEndian.PutUint32(buf, uint32(len(key)))
	getCode = append(getCode, buf...)
	getCode = append(getCode, key...)
	getCode = append(getCode, OpcodeExit)

	vmCtxPtr, err := db.executeBytecode(ctx, getCode)
	if err != nil {
		return false, err
	}
	resPtrArgs, err := db.fnResponsePtr.Call(ctx, vmCtxPtr)
	if err != nil || len(resPtrArgs) == 0 {
		return false, fmt.Errorf("failed to call response ptr helper")
	}
	if resPtrArgs[0] != 0 {
		return false, nil // Key already exists.
	}

	// Step 2: insert, still under the same lock acquisition.
	putCode := make([]byte, 0, 1+4+len(key)+4+len(value)+1)
	putCode = append(putCode, OpcodePut)
	binary.LittleEndian.PutUint32(buf, uint32(len(key)))
	putCode = append(putCode, buf...)
	putCode = append(putCode, key...)
	binary.LittleEndian.PutUint32(buf, uint32(len(value)))
	putCode = append(putCode, buf...)
	putCode = append(putCode, value...)
	putCode = append(putCode, OpcodeExit)

	if _, err := db.executeBytecode(ctx, putCode); err != nil {
		return false, err
	}
	return true, nil
}

func (db *YowzaDB) Get(key string) ([]byte, error) {
	// Exclusive lock, not RLock: the C engine keeps one static VMContext
	// and Go keeps one shared bytecode slot (memOffsetCode), so even a
	// read-only call mutates shared state. See the step-1 comment in
	// src/yowza.c about the static context not being thread-safe.
	db.mu.Lock()
	defer db.mu.Unlock()

	bytecode := make([]byte, 0, 1+4+len(key)+1)
	bytecode = append(bytecode, OpcodeGet)

	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(len(key)))
	bytecode = append(bytecode, buf...)
	bytecode = append(bytecode, key...)

	bytecode = append(bytecode, OpcodeExit)

	ctx := context.Background()
	vmCtxPtr, err := db.executeBytecode(ctx, bytecode)
	if err != nil {
		return nil, err
	}

	resLenArgs, err := db.fnResponseLen.Call(ctx, vmCtxPtr)
	if err != nil || len(resLenArgs) == 0 {
		return nil, fmt.Errorf("failed to call response len helper")
	}
	resLen := uint32(resLenArgs[0])

	resPtrArgs, err := db.fnResponsePtr.Call(ctx, vmCtxPtr)
	if err != nil || len(resPtrArgs) == 0 {
		return nil, fmt.Errorf("failed to call response ptr helper")
	}
	// Full uint64 pointer: truncating to uint32 would corrupt any
	// response above 4 GiB in a memory64 module.
	resPtr := resPtrArgs[0]

	// Found vs not-found is decided by response_ptr, not by length:
	// the C engine sets response_ptr to NULL on a miss, but a hit for a
	// key whose value is empty leaves a non-NULL arena pointer with
	// length 0. Treating resLen == 0 as "not found" wrongly reported
	// empty values as missing.
	if resPtr == 0 {
		return nil, nil // Not found
	}

	// Zero-copy read from Wasm linear memory
	data, ok := db.memRead(resPtr, uint64(resLen))
	if !ok {
		return nil, fmt.Errorf("failed to read from wasm memory")
	}

	// We must return a copy of the data because Wasm memory might be overwritten
	// For resLen == 0 this is a non-nil empty slice, distinct from the
	// nil returned for "not found" above.
	copiedData := make([]byte, resLen)
	copy(copiedData, data)

	return copiedData, nil
}

// deleteKeyOnly executes a plain OP_DEL without any index cleanup and
// without acquiring the lock itself: it assumes the caller holds db.mu
// (or that the caller is Delete, which drives the lock dance itself).
// It exists so Delete's index cleanup cannot recurse into Delete.
func (db *YowzaDB) deleteKeyOnly(key string) error {
	bytecode := make([]byte, 0, 1+4+len(key)+1)
	bytecode = append(bytecode, OpcodeDel)

	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(len(key)))
	bytecode = append(bytecode, buf...)
	bytecode = append(bytecode, key...)

	bytecode = append(bytecode, OpcodeExit)

	_, err := db.executeBytecode(context.Background(), bytecode)
	return err
}

func (db *YowzaDB) Delete(key string) error {
	// Deleting a primary record must also delete its secondary indexes
	// (see index.go): collect the reverse mappings first, while no lock
	// is held, so the lookups below cannot deadlock on db.mu.
	idxKeys, revKeys := db.indexKeysForPrimary(key)

	db.mu.Lock()
	err := db.deleteKeyOnly(key)
	db.mu.Unlock()
	if err != nil {
		return err
	}

	// Primary is gone; now remove its index entries and reverse mappings.
	// Failures here are best-effort: the primary delete already succeeded,
	// and a stale index entry is repaired on the next ScanByIndex lookup
	// (it skips missing primaries) and on the next Delete of this key.
	for _, idxKey := range idxKeys {
		db.mu.Lock()
		_ = db.deleteKeyOnly(idxKey)
		db.mu.Unlock()
	}
	for _, revKey := range revKeys {
		db.mu.Lock()
		_ = db.deleteKeyOnly(revKey)
		db.mu.Unlock()
	}
	return nil
}

// ScanEntry is one row of a ScanEntries result: the full key and its value.
type ScanEntry struct {
	Key   string
	Value []byte
}

// parseScanData parses the sequential scan result format
// [KeyLen u32][Key][ValLen u32][Val]... . A malformed tail ends the parse
// (break, not an error), exactly as the original ScanEntries loop did;
// callers must not change this to an error without checking the index
// tests, which rely on the tolerant behaviour.
func parseScanData(data []byte, resLen uint32) []ScanEntry {
	var results []ScanEntry
	offset := uint32(0)
	for offset < resLen {
		if offset+4 > resLen {
			break
		}
		kl := binary.LittleEndian.Uint32(data[offset : offset+4])
		offset += 4

		if offset+kl > resLen {
			break
		}
		keyCopy := make([]byte, kl)
		copy(keyCopy, data[offset:offset+kl])
		offset += kl

		if offset+4 > resLen {
			break
		}
		vl := binary.LittleEndian.Uint32(data[offset : offset+4])
		offset += 4

		if offset+vl > resLen {
			break
		}
		valCopy := make([]byte, vl)
		copy(valCopy, data[offset:offset+vl])
		results = append(results, ScanEntry{Key: string(keyCopy), Value: valCopy})

		offset += vl
	}

	return results
}

// ScanEntries returns key+value pairs for a prefix scan. Unlike Scan it
// does not discard keys; index.go's prefix lookup is built on it.
//
// limitBufferBytes is an INITIAL requested output capacity, not a hard
// cap: on the yowza_scan path the buffer grows dynamically (same +6.25
// MiB step as CreateSnapshot) when the result does not fit, so a scan
// larger than limitBufferBytes no longer fails with the old 64 KiB VM
// cap. On the legacy OP_SCAN path (binaries without yowza_scan) the old
// 64 KiB static VM buffer still applies.
func (db *YowzaDB) ScanEntries(prefix string, limitBufferBytes uint32) ([]ScanEntry, error) {
	// Exclusive lock, not RLock: same shared bytecode slot / static
	// VMContext reason as Get.
	db.mu.Lock()
	defer db.mu.Unlock()

	// Direct yowza_scan path (binaries built after the C-side item-4
	// patch). The contract, agreed with the C track, is:
	// yowza_scan(prefix_ptr, prefix_len, out_buf, out_cap) -> int:
	// bytes written, 0 when nothing matches, -1 when the output buffer
	// is too small (or the C scan itself fails, see the guard below).
	if db.fnScan != nil {
		ctx := context.Background()

		// art_scan rejects prefixes longer than YOWZA_MAX_KEY_LEN
		// (1023) with -1, and growing the buffer cannot fix that, so
		// fail fast instead of looping on -1 forever.
		if len(prefix) > 1023 {
			return nil, fmt.Errorf("scan prefix too long (%d > 1023)", len(prefix))
		}
		if uint64(len(prefix)) > db.memOffsetCodeCap {
			return nil, fmt.Errorf("scan prefix exceeds capacity (%d > %d)", len(prefix), db.memOffsetCodeCap)
		}
		if len(prefix) > 0 && !db.memWrite(db.memOffsetCode, []byte(prefix)) {
			return nil, fmt.Errorf("failed to write scan prefix to wasm memory")
		}

		// Honour the caller's initial capacity request, then grow on -1.
		if uint64(limitBufferBytes) > db.memOffsetSnapCap {
			if err := db.memGrowTo(db.memOffsetSnap + uint64(limitBufferBytes)); err != nil {
				return nil, err
			}
			db.memOffsetSnapCap = uint64(limitBufferBytes)
		}

		// art_scan conflates "buffer too small" with other failures
		// (e.g. an empty tree, whose root is NULL, also returns -1),
		// so an unbounded grow-on--1 loop is unsafe. Cap total growth
		// at 256 MiB; a genuine scan larger than that is an error here
		// rather than an infinite loop.
		const maxScanCap uint64 = 256 << 20
		for {
			res, err := db.fnScan.Call(ctx, uint64(db.memOffsetCode), uint64(len(prefix)), uint64(db.memOffsetSnap), uint64(db.memOffsetSnapCap))
			if err != nil {
				return nil, fmt.Errorf("failed to call yowza_scan: %v", err)
			}
			if len(res) == 0 {
				return nil, fmt.Errorf("yowza_scan returned no result")
			}
			// Same int32 conversion as CreateSnapshot: C int is 32-bit
			// even on wasm64, so -1 arrives as 0xFFFFFFFF (or sign
			// extended to 0xFFFFFFFFFFFFFFFF).
			scanLen := int32(uint32(res[0]))
			if scanLen == -1 {
				newCap := db.memOffsetSnapCap + 6553600
				if newCap > maxScanCap {
					return nil, fmt.Errorf("scan result exceeds maximum scan buffer (%d bytes)", maxScanCap)
				}
				if err := db.memGrowTo(db.memOffsetSnap + newCap); err != nil {
					return nil, fmt.Errorf("scan buffer too small and wasm memory cannot grow to %d bytes: %v", db.memOffsetSnap+newCap, err)
				}
				db.memOffsetSnapCap = newCap
				continue
			}
			if scanLen < 0 {
				return nil, fmt.Errorf("yowza_scan failed with code %d", scanLen)
			}
			if scanLen == 0 {
				return nil, nil
			}
			data, ok := db.memRead(db.memOffsetSnap, uint64(scanLen))
			if !ok {
				return nil, fmt.Errorf("failed to read scan buffer from wasm memory")
			}
			return parseScanData(data, uint32(scanLen)), nil
		}
	}

	bytecode := make([]byte, 0, 1+4+len(prefix)+4+1)
	bytecode = append(bytecode, OpcodeScan)

	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(len(prefix)))
	bytecode = append(bytecode, buf...)
	bytecode = append(bytecode, prefix...)

	binary.LittleEndian.PutUint32(buf, limitBufferBytes)
	bytecode = append(bytecode, buf...)

	bytecode = append(bytecode, OpcodeExit)

	ctx := context.Background()
	vmCtxPtr, err := db.executeBytecode(ctx, bytecode)
	if err != nil {
		return nil, err
	}

	resLenArgs, err := db.fnResponseLen.Call(ctx, vmCtxPtr)
	if err != nil || len(resLenArgs) == 0 {
		return nil, fmt.Errorf("failed to get scan response len")
	}
	resLen := uint32(resLenArgs[0])

	if resLen == 0 {
		return nil, nil // Nothing found
	}

	resPtrArgs, err := db.fnResponsePtr.Call(ctx, vmCtxPtr)
	if err != nil || len(resPtrArgs) == 0 {
		return nil, fmt.Errorf("failed to get scan response ptr")
	}
	// Full uint64 pointer: do not truncate to uint32 (memory64).
	resPtr := resPtrArgs[0]

	data, ok := db.memRead(resPtr, uint64(resLen))
	if !ok {
		return nil, fmt.Errorf("failed to read from wasm memory")
	}

	// Parse the sequential result: [KeyLen][Key][ValLen][Val]
	return parseScanData(data, resLen), nil
}

func (db *YowzaDB) Scan(prefix string, limitBufferBytes uint32) ([][]byte, error) {
	// Compatibility wrapper: returns only values, discarding keys.
	// New code that needs keys should use ScanEntries instead.
	entries, err := db.ScanEntries(prefix, limitBufferBytes)
	if err != nil {
		return nil, err
	}
	var results [][]byte
	for _, e := range entries {
		results = append(results, e.Value)
	}
	return results, nil
}

func (db *YowzaDB) CreateSnapshot() ([]byte, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	ctx := context.Background()

	// Dynamic snapshot allocation to break the 1MB glass ceiling.
	// We retry if the C engine indicates truncation (-1), but each retry
	// must actually grow the memory: an unchecked Grow that fails would
	// spin forever here, so a failed grow is now a hard error.
	for {
		res, err := db.fnSnapCreate.Call(ctx, uint64(db.memOffsetSnap), uint64(db.memOffsetSnapCap))
		if err != nil {
			return nil, fmt.Errorf("failed to call snapshot create: %v", err)
		}

		snapLen := int32(uint32(res[0]))

		if snapLen == -1 {
			// Buffer was too small. Grow Wasm memory to cover the
			// enlarged snapshot buffer, then retry once with the new cap.
			// 100 pages = ~6.5MB growth per retry.
			newCap := db.memOffsetSnapCap + 6553600
			if err := db.memGrowTo(db.memOffsetSnap + newCap); err != nil {
				return nil, fmt.Errorf("snapshot buffer too small and wasm memory cannot grow to %d bytes: %v", db.memOffsetSnap+newCap, err)
			}
			db.memOffsetSnapCap = newCap
			continue
		}

		if snapLen < 0 {
			return nil, fmt.Errorf("snapshot creation failed internally (OOM or truncated)")
		}
		if snapLen == 0 {
			return []byte{}, nil
		}

		data, ok := db.memRead(db.memOffsetSnap, uint64(snapLen))
		if !ok {
			return nil, fmt.Errorf("failed to read snapshot buffer from wasm memory")
		}

		copied := make([]byte, snapLen)
		copy(copied, data)

		return copied, nil
	}
}

// compactLocked performs the actual compaction without acquiring the mutex.
// Assumes the caller holds db.mu.Lock(). It is called from AckWAL and
// from Compact, which is why AckWAL used to fail on any database larger
// than the initial 1 MiB snapshot buffer: compactLocked had no dynamic
// growth, unlike CreateSnapshot.
//
// Only the -2 code grows and retries: -2 is returned by yowza_compact
// in step 1, before arena_reset, when temp_buffer is too small, so
// retrying after growing memOffsetSnapCap loses no data. Any other
// non-zero code is returned as an error immediately; looping on it
// would spin forever, because growing the temp buffer cannot fix a
// step-2/3 failure.
func (db *YowzaDB) compactLocked(ctx context.Context) error {
	for {
		res, err := db.fnCompact.Call(ctx, uint64(db.memOffsetSnap), uint64(db.memOffsetSnapCap))
		if err != nil {
			return fmt.Errorf("failed to call yowza_compact: %v", err)
		}
		if len(res) == 0 {
			return fmt.Errorf("yowza_compact returned no result")
		}
		rc := int32(uint32(res[0]))
		if rc == 0 {
			return nil
		}
		if rc == -2 {
			// Temp buffer too small in step 1 (Arena NOT reset yet).
			// Same +6.25 MiB step as CreateSnapshot.
			newCap := db.memOffsetSnapCap + 6553600
			if err := db.memGrowTo(db.memOffsetSnap + newCap); err != nil {
				return fmt.Errorf("compaction buffer too small and wasm memory cannot grow to %d bytes: %v", db.memOffsetSnap+newCap, err)
			}
			db.memOffsetSnapCap = newCap
			continue
		}
		return fmt.Errorf("compaction failed internally (code %d)", rc)
	}
}

// Compact reclaims leaked string values from the Arena by fully serializing the live tree
// to a temporary buffer and restoring it to a freshly reset Arena.
func (db *YowzaDB) Compact() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.compactLocked(context.Background())
}

func (db *YowzaDB) RestoreSnapshot(snapshot []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if uint64(len(snapshot)) > db.memOffsetSnapCap {
		return fmt.Errorf("snapshot too large for current memory configuration")
	}

	if !db.memWrite(db.memOffsetSnap, snapshot) {
		return fmt.Errorf("failed to write snapshot to wasm memory")
	}

	ctx := context.Background()
	res, err := db.fnSnapRestore.Call(ctx, uint64(db.memOffsetSnap), uint64(len(snapshot)))
	if err != nil {
		return fmt.Errorf("failed to call snapshot restore: %v", err)
	}

	if int32(uint32(res[0])) != 0 {
		return fmt.Errorf("snapshot restore failed internally (invalid format or OOM)")
	}

	return nil
}
