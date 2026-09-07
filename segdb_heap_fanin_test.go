package yowza

// Gate 3 Go tests: fan-in-bounded scan through the Go wrapper
// (segdb_fanin.go) and through the wasm exports directly.
//
//   - TestFaninOver128Sources runs through NewSegDB (4 MiB heap)
//     on both binaries: 132 segments, which the old cursor path
//     refuses (rc=-1) and the fanin path scans completely, with
//     overwrite/delete/hot/snapshot checks mirroring the C fanin
//     test, plus invalid-cursor coverage and stats proving the
//     source count exceeded the old 128 cap.
//   - TestFanin900k drives the wasm exports directly (scanperf
//     layout: state 64 MiB, arena 4 MiB, WAL 1 MiB, scratch 1 MiB,
//     heap 256 MiB) because NewSegDB's fixed 4 MiB heap cannot
//     hold 900,000 rows. It builds 900,000 rows (220 segments),
//     proves the old cursor path fails on the same DB, pages the
//     full result with the fanin export on both binaries with
//     row-for-row verification, and records heap/memory behaviour
//     as the segment count grows (not just wall time).
//   - TestHeapReclaimDirect drives the heap exports directly on
//     both binaries with a 16 MiB heap: cycles with per-cycle
//     reclaim, corrupt-slot failure with no partial compaction,
//     and orphan deletion that keeps live/current/gen-1 objects.
//     The demanding 200-cycle proof lives in the C test
//     (test_heap_compact_cycles); this test covers the same
//     exports through wasm.

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"os"
	"testing"
	"time"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
)

// ---------------- bindings test: >128 segments ----------------

func TestFaninOver128Sources(t *testing.T) {
	for _, bin := range []string{"build/yowza.wasm", "build/yowza64.wasm"} {
		t.Run(bin, func(t *testing.T) {
			wasmBytes, err := os.ReadFile(bin)
			if err != nil {
				t.Fatalf("read %s: %v", bin, err)
			}
			db, err := NewYowzaDB(wasmBytes)
			if err != nil {
				t.Fatalf("NewYowzaDB: %v", err)
			}
			defer db.Close()
			sdb, err := NewSegDB(db)
			if err != nil {
				t.Fatalf("NewSegDB: %v", err)
			}
			// 130 flushes x 10 keys = 1300 rows, then an
			// overwrite flush (100..109) and a delete flush
			// (500..509): 132 segments, 1290 live rows.
			for f := 0; f < 130; f++ {
				for j := 0; j < 10; j++ {
					i := f*10 + j
					if err := sdb.Put(fmt.Sprintf("h:%06d", i), []byte(fmt.Sprintf("v:%d", i))); err != nil {
						t.Fatalf("Put %d: %v", i, err)
					}
				}
				if err := sdb.Flush(); err != nil {
					t.Fatalf("Flush %d: %v", f, err)
				}
			}
			for j := 100; j <= 109; j++ {
				if err := sdb.Put(fmt.Sprintf("h:%06d", j), []byte(fmt.Sprintf("w:%d", j))); err != nil {
					t.Fatalf("Put overwrite %d: %v", j, err)
				}
			}
			if err := sdb.Flush(); err != nil {
				t.Fatalf("Flush overwrite: %v", err)
			}
			for j := 500; j <= 509; j++ {
				if err := sdb.Delete(fmt.Sprintf("h:%06d", j)); err != nil {
					t.Fatalf("Delete %d: %v", j, err)
				}
			}
			if err := sdb.Flush(); err != nil {
				t.Fatalf("Flush delete: %v", err)
			}
			if n, err := sdb.SegmentCount(); err != nil || n != 4 {
				// Last group's count: 132 segments span two
				// manifest groups (128 + 4); segment_count
				// reports the last group only (see the export
				// comment in yowza_segdb.c). The fanin stats
				// assertion below proves all 132 are scanned.
				t.Fatalf("SegmentCount = %d, err %v, want 4 (last group of 128+4)", n, err)
			}
			snapOld, err := sdb.SnapshotSeq()
			if err != nil {
				t.Fatalf("SnapshotSeq: %v", err)
			}
			// The old cursor path must fail at >128 sources.
			if _, _, _, err := sdb.ScanCursor("", snapOld, nil, false, 27); err == nil {
				t.Fatalf("ScanCursor on 132 segments succeeded, want rc=-1 failure")
			}
			// Model check for the frozen snapshot: 1290 rows.
			checkModel := func(rows []SegDBScanEntry, want int) {
				if len(rows) != want {
					t.Fatalf("fanin rows = %d, want %d", len(rows), want)
				}
				expect := 0
				for _, e := range rows {
					for expect < 1300 && ((expect >= 500 && expect <= 509)) {
						expect++
					}
					if expect >= 1300 {
						t.Fatalf("extra row %q", e.Key)
					}
					wantK := fmt.Sprintf("h:%06d", expect)
					wantV := fmt.Sprintf("v:%d", expect)
					if expect >= 100 && expect <= 109 {
						wantV = fmt.Sprintf("w:%d", expect)
					}
					if string(e.Key) != wantK || string(e.Val) != wantV {
						t.Fatalf("row %d: got %q=%q want %q=%q", expect, e.Key, e.Val, wantK, wantV)
					}
					expect++
				}
			}
			rows, err := sdb.ScanFanin("", snapOld)
			if err != nil {
				t.Fatalf("ScanFanin old snapshot: %v", err)
			}
			checkModel(rows, 1290)
			// Hot changes after the snapshot.
			for j := 1300; j <= 1304; j++ {
				if err := sdb.Put(fmt.Sprintf("h:%06d", j), []byte(fmt.Sprintf("n:%d", j))); err != nil {
					t.Fatalf("Put hot %d: %v", j, err)
				}
			}
			if err := sdb.Put("h:000000", []byte("hot0")); err != nil {
				t.Fatalf("Put hot overwrite: %v", err)
			}
			if err := sdb.Delete("h:000001"); err != nil {
				t.Fatalf("Delete hot: %v", err)
			}
			snapNew, err := sdb.SnapshotSeq()
			if err != nil {
				t.Fatalf("SnapshotSeq new: %v", err)
			}
			rows, err = sdb.ScanFanin("", snapOld)
			if err != nil {
				t.Fatalf("ScanFanin old snapshot after hot: %v", err)
			}
			checkModel(rows, 1290)
			rows, err = sdb.ScanFanin("", snapNew)
			if err != nil {
				t.Fatalf("ScanFanin new snapshot: %v", err)
			}
			if len(rows) != 1294 {
				t.Fatalf("fanin new snapshot rows = %d, want 1294", len(rows))
			}
			byKey := map[string]string{}
			for _, e := range rows {
				byKey[string(e.Key)] = string(e.Val)
			}
			if byKey["h:000000"] != "hot0" {
				t.Fatalf("h:000000 = %q, want hot0", byKey["h:000000"])
			}
			if _, ok := byKey["h:000001"]; ok {
				t.Fatalf("h:000001 present, want hot-deleted")
			}
			if byKey["h:001300"] != "n:1300" {
				t.Fatalf("h:001300 = %q, want n:1300", byKey["h:001300"])
			}
			rows, err = sdb.ScanFanin("h:0001", snapNew)
			if err != nil {
				t.Fatalf("ScanFanin prefix: %v", err)
			}
			if len(rows) != 100 {
				t.Fatalf("prefix rows = %d, want 100", len(rows))
			}
			for i, e := range rows {
				wantK := fmt.Sprintf("h:%06d", 100+i)
				wantV := fmt.Sprintf("v:%d", 100+i)
				if 100+i <= 109 {
					wantV = fmt.Sprintf("w:%d", 100+i)
				}
				if string(e.Key) != wantK || string(e.Val) != wantV {
					t.Fatalf("prefix row %d: got %q=%q want %q=%q", i, e.Key, e.Val, wantK, wantV)
				}
			}
			// Invalid cursors: typed error, no rows.
			if _, _, _, err := sdb.ScanFaninPage("", snapNew, []byte{}, true, 27); err != ErrSegDBFaninInvalid {
				t.Fatalf("empty cursor with hasCursor: err %v, want ErrSegDBFaninInvalid", err)
			}
			big := make([]byte, 2000)
			if _, _, _, err := sdb.ScanFaninPage("", snapNew, big, true, 27); err != ErrSegDBFaninInvalid {
				t.Fatalf("2000-byte cursor: err %v, want ErrSegDBFaninInvalid", err)
			}
			if page, _, _, err := sdb.ScanFaninPage("", snapNew, nil, false, 0); err != nil || len(page) != 0 {
				t.Fatalf("limit 0: page %d err %v, want empty nil", len(page), err)
			}
			// Stats prove the scan enumerated past the old cap.
			if err := sdb.FaninStatsReset(); err != nil {
				t.Fatalf("FaninStatsReset: %v", err)
			}
			if _, err := sdb.ScanFanin("", snapNew); err != nil {
				t.Fatalf("ScanFanin for stats: %v", err)
			}
			sources, windows, seeks, entries, err := sdb.FaninStats()
			if err != nil {
				t.Fatalf("FaninStats: %v", err)
			}
			if sources != 132 {
				t.Fatalf("fanin sources = %d, want 132 (all segments across both groups)", sources)
			}
			t.Logf("fanin stats: sources=%d windows=%d seeks=%d entries=%d", sources, windows, seeks, entries)
		})
	}
}

// ---------------- direct-export harness ----------------

const faninPageSize = 65536

type faninHarness struct {
	ctx     context.Context
	mod     api.Module
	mem     api.Memory
	mem64   api.Memory64
	is64    bool
	state   uint64
	arena   uint64
	wal     uint64
	scratch uint64
	heap    uint64
	heapMB  uint64

	fnInit       api.Function
	fnPut        api.Function
	fnGet        api.Function
	fnDelete     api.Function
	fnFlush      api.Function
	fnCompact    api.Function
	fnSnapshot   api.Function
	fnScanCursor api.Function
	fnScanFanin  api.Function
	fnFaninStats api.Function
	fnFaninReset api.Function
	fnHeapInfo   api.Function
	fnHeapCmp    api.Function
	fnHeapThresh api.Function
	fnHeapRecl   api.Function
	fnHeapReclSt api.Function
	fnHeapIdxRb  api.Function
}

func (h *faninHarness) memSize() uint64 {
	if h.is64 && h.mem64 != nil {
		return h.mem64.Size64()
	}
	return uint64(h.mem.Size())
}

func (h *faninHarness) memGrowTo(target uint64) error {
	size := h.memSize()
	if size >= target {
		return nil
	}
	need := (target - size + faninPageSize - 1) / faninPageSize
	if h.is64 && h.mem64 != nil {
		if _, ok := h.mem64.Grow64(need); !ok {
			return fmt.Errorf("grow64 refused")
		}
	} else {
		if _, ok := h.mem.Grow(uint32(need)); !ok {
			return fmt.Errorf("grow refused")
		}
	}
	if h.memSize() < target {
		return fmt.Errorf("growth accepted but still below target")
	}
	return nil
}

func (h *faninHarness) memRead(off, n uint64) ([]byte, bool) {
	if h.is64 && h.mem64 != nil {
		return h.mem64.Read64(off, n)
	}
	if off > 0xFFFFFFFF || n > 0xFFFFFFFF {
		return nil, false
	}
	return h.mem.Read(uint32(off), uint32(n))
}

func (h *faninHarness) memWrite(off uint64, val []byte) bool {
	if h.is64 && h.mem64 != nil {
		return h.mem64.Write64(off, val)
	}
	if off > 0xFFFFFFFF {
		return false
	}
	return h.mem.Write(uint32(off), val)
}

func (h *faninHarness) call(fn api.Function, args ...uint64) (int32, error) {
	res, err := fn.Call(h.ctx, args...)
	if err != nil {
		return 0, err
	}
	if len(res) == 0 {
		return 0, fmt.Errorf("empty result")
	}
	return int32(res[0]), nil
}

// callVoid calls a void wasm export (no results): heap_info,
// heap_compact_threshold, reclaim_stats, fanin_stats, and the
// stats resets. Using call for those would fail spuriously with
// "empty result".
func (h *faninHarness) callVoid(fn api.Function, args ...uint64) error {
	_, err := fn.Call(h.ctx, args...)
	return err
}

func (h *faninHarness) callU64(fn api.Function, args ...uint64) (uint64, error) {
	res, err := fn.Call(h.ctx, args...)
	if err != nil {
		return 0, err
	}
	if len(res) == 0 {
		return 0, fmt.Errorf("empty result")
	}
	return res[0], nil
}

// Scratch layout (1 MiB region).
const (
	fhKey     = 0
	fhVal     = 2048
	fhCount   = 4096
	fhNext    = 6144 // 1024 bytes
	fhNextLn  = 8192
	fhMore    = 8200
	fhStats   = 8208 // 32 bytes (fanin) / 48 bytes (heap info)
	fhOut     = 16384
	fhOutCap  = 1048576 - fhOut
)

func (h *faninHarness) put(key, val string) error {
	if !h.memWrite(h.scratch+fhKey, []byte(key)) {
		return fmt.Errorf("write key")
	}
	if !h.memWrite(h.scratch+fhVal, []byte(val)) {
		return fmt.Errorf("write val")
	}
	rc, err := h.call(h.fnPut, h.state, h.scratch+fhKey, uint64(len(key)), h.scratch+fhVal, uint64(len(val)))
	if err != nil {
		return err
	}
	if rc != 0 {
		return fmt.Errorf("put %s rc=%d", key, rc)
	}
	return nil
}

func (h *faninHarness) get(key string) (string, error) {
	if !h.memWrite(h.scratch+fhKey, []byte(key)) {
		return "", fmt.Errorf("write key")
	}
	rc, err := h.call(h.fnGet, h.state, h.scratch+fhKey, uint64(len(key)), h.scratch+fhOut, 4096, h.scratch+fhCount)
	if err != nil {
		return "", err
	}
	if rc != 0 {
		return "", fmt.Errorf("get %s rc=%d", key, rc)
	}
	raw, ok := h.memRead(h.scratch+fhCount, 4)
	if !ok {
		return "", fmt.Errorf("read get len")
	}
	n := binary.LittleEndian.Uint32(raw)
	out, ok := h.memRead(h.scratch+fhOut, uint64(n))
	if !ok {
		return "", fmt.Errorf("read get val")
	}
	return string(out), nil
}

func (h *faninHarness) delete(key string) error {
	if !h.memWrite(h.scratch+fhKey, []byte(key)) {
		return fmt.Errorf("write key")
	}
	rc, err := h.call(h.fnDelete, h.state, h.scratch+fhKey, uint64(len(key)))
	if err != nil {
		return err
	}
	if rc != 0 {
		return fmt.Errorf("delete %s rc=%d", key, rc)
	}
	return nil
}

func (h *faninHarness) flush() error {
	rc, err := h.call(h.fnFlush, h.state)
	if err != nil {
		return err
	}
	if rc != 0 {
		return fmt.Errorf("flush rc=%d", rc)
	}
	return nil
}

func (h *faninHarness) compact() error {
	rc, err := h.call(h.fnCompact, h.state, 0)
	if err != nil {
		return err
	}
	if rc != 0 {
		return fmt.Errorf("compact rc=%d", rc)
	}
	return nil
}

func (h *faninHarness) snapshot() (uint64, error) {
	return h.callU64(h.fnSnapshot, h.state)
}

func (h *faninHarness) heapInfo() (size, bump, live, slots, waste, thresh uint64, err error) {
	if err := h.callVoid(h.fnHeapInfo, h.scratch+fhStats); err != nil {
		return 0, 0, 0, 0, 0, 0, err
	}
	raw, ok := h.memRead(h.scratch+fhStats, 48)
	if !ok {
		return 0, 0, 0, 0, 0, 0, fmt.Errorf("read heap info")
	}
	return binary.LittleEndian.Uint64(raw[0:8]), binary.LittleEndian.Uint64(raw[8:16]),
		binary.LittleEndian.Uint64(raw[16:24]), binary.LittleEndian.Uint64(raw[24:32]),
		binary.LittleEndian.Uint64(raw[32:40]), binary.LittleEndian.Uint64(raw[40:48]), nil
}

func (h *faninHarness) heapCompact() (int32, error) {
	return h.call(h.fnHeapCmp)
}

func (h *faninHarness) heapThreshold(waste uint64) error {
	return h.callVoid(h.fnHeapThresh, waste)
}

func (h *faninHarness) heapReclaim() (int32, error) {
	return h.call(h.fnHeapRecl, h.state)
}

func (h *faninHarness) heapReclaimStats() (deleted, bytes uint64, err error) {
	if err := h.callVoid(h.fnHeapReclSt, h.scratch+fhStats); err != nil {
		return 0, 0, err
	}
	raw, ok := h.memRead(h.scratch+fhStats, 16)
	if !ok {
		return 0, 0, fmt.Errorf("read reclaim stats")
	}
	return binary.LittleEndian.Uint64(raw[0:8]), binary.LittleEndian.Uint64(raw[8:16]), nil
}

func (h *faninHarness) heapIndexRebuild() (int32, error) {
	return h.call(h.fnHeapIdxRb)
}

func (h *faninHarness) faninStats() (sources, windows, seeks, entries uint64, err error) {
	if err := h.callVoid(h.fnFaninStats, h.scratch+fhStats); err != nil {
		return 0, 0, 0, 0, err
	}
	raw, ok := h.memRead(h.scratch+fhStats, 32)
	if !ok {
		return 0, 0, 0, 0, fmt.Errorf("read fanin stats")
	}
	return binary.LittleEndian.Uint64(raw[0:8]), binary.LittleEndian.Uint64(raw[8:16]),
		binary.LittleEndian.Uint64(raw[16:24]), binary.LittleEndian.Uint64(raw[24:32]), nil
}

func (h *faninHarness) faninReset() error {
	return h.callVoid(h.fnFaninReset)
}

// faninPage runs one fanin call; on rc=-3 the partial rows are
// returned with hasMore=true (contract: resume with nextKey).
func (h *faninHarness) faninPage(prefix string, snap uint64, cursor []byte, hasCursor bool, limit uint32) (entries [][2]string, nextKey []byte, hasMore bool, err error) {
	if !h.memWrite(h.scratch+fhKey, []byte(prefix)) {
		return nil, nil, false, fmt.Errorf("write prefix")
	}
	var curPtr uint64 = h.scratch + fhNext
	var curLen uint64
	var hasCur uint64
	if hasCursor {
		if !h.memWrite(curPtr, cursor) {
			return nil, nil, false, fmt.Errorf("write cursor")
		}
		curLen = uint64(len(cursor))
		hasCur = 1
	}
	rc, err := h.call(h.fnScanFanin, h.state, h.scratch+fhKey, uint64(len(prefix)), snap,
		curPtr, curLen, hasCur, uint64(limit),
		h.scratch+fhOut, fhOutCap, h.scratch+fhCount,
		h.scratch+fhNext, 1024, h.scratch+fhNextLn, h.scratch+fhMore)
	if err != nil {
		return nil, nil, false, err
	}
	if rc != 0 && rc != -3 {
		return nil, nil, false, fmt.Errorf("scan_fanin rc=%d", rc)
	}
	raw, ok := h.memRead(h.scratch+fhCount, 4)
	if !ok {
		return nil, nil, false, fmt.Errorf("read count")
	}
	cnt := binary.LittleEndian.Uint32(raw)
	out, ok := h.memRead(h.scratch+fhOut, fhOutCap)
	if !ok {
		return nil, nil, false, fmt.Errorf("read out")
	}
	off := 0
	for i := uint32(0); i < cnt; i++ {
		if off+8 > len(out) {
			return nil, nil, false, fmt.Errorf("truncated page")
		}
		klen := int(binary.LittleEndian.Uint32(out[off:]))
		off += 4
		if off+klen+4 > len(out) {
			return nil, nil, false, fmt.Errorf("truncated key")
		}
		k := string(out[off : off+klen])
		off += klen
		vlen := int(binary.LittleEndian.Uint32(out[off:]))
		off += 4
		if off+vlen > len(out) {
			return nil, nil, false, fmt.Errorf("truncated val")
		}
		v := string(out[off : off+vlen])
		off += vlen
		entries = append(entries, [2]string{k, v})
	}
	lnRaw, ok := h.memRead(h.scratch+fhNextLn, 4)
	if !ok {
		return nil, nil, false, fmt.Errorf("read next len")
	}
	nkLen := binary.LittleEndian.Uint32(lnRaw)
	moreRaw, ok := h.memRead(h.scratch+fhMore, 4)
	if !ok {
		return nil, nil, false, fmt.Errorf("read has_more")
	}
	hasMore = binary.LittleEndian.Uint32(moreRaw) != 0
	if nkLen > 0 {
		nk, ok := h.memRead(h.scratch+fhNext, uint64(nkLen))
		if !ok {
			return nil, nil, false, fmt.Errorf("read next key")
		}
		nextKey = append([]byte(nil), nk...)
	}
	if rc == -3 && cnt == 0 {
		return nil, nil, false, fmt.Errorf("fanin -3 with zero rows")
	}
	return entries, nextKey, hasMore, nil
}

// faninFull pages the whole result, calling check for each row
// in order, and returns the row count, an FNV hash of the
// key/value stream (for snapshot-equality comparisons), and the
// number of wasm calls made (the C fanin stats accumulate across
// calls, so sources == segments * calls). It pages
// with a huge limit so each call returns one out-buffer worth of
// rows (~28k small rows) via the -3 partial-page contract: paging
// with a small limit would re-discover all sources on every call
// (the C API is stateless across calls, so enumeration + index
// checksum verification repeat per call), which multiplies the
// scan cost by the page count without changing the result.
func (h *faninHarness) faninFull(prefix string, snap uint64, check func(idx int, k, v string) error) (int, uint64, int, error) {
	hh := fnv.New64a()
	var cursor []byte
	hasCursor := false
	total := 0
	calls := 0
	for {
		page, nextKey, hasMore, err := h.faninPage(prefix, snap, cursor, hasCursor, 1<<30)
		calls++
		if err != nil {
			return total, 0, calls, err
		}
		for _, e := range page {
			if check != nil {
				if err := check(total, e[0], e[1]); err != nil {
					return total, 0, calls, err
				}
			}
			hh.Write([]byte(e[0]))
			hh.Write([]byte{0})
			hh.Write([]byte(e[1]))
			hh.Write([]byte{0})
			total++
		}
		if !hasMore {
			return total, hh.Sum64(), calls, nil
		}
		if len(page) == 0 {
			return total, 0, calls, fmt.Errorf("fanin made no progress (empty page with hasMore set)")
		}
		cursor, hasCursor = nextKey, true
	}
}

// slotNames parses the heap table and returns live object names.
func (h *faninHarness) slotNames() (map[string]bool, error) {
	heapBytes, ok := h.memRead(h.heap, h.heapMB<<20)
	if !ok {
		return nil, fmt.Errorf("read heap bytes")
	}
	names := map[string]bool{}
	for i := 0; i < 4096; i++ {
		s := heapBytes[32+i*152:]
		if s[0] == 0 {
			continue
		}
		end := 0
		for end < 128 && s[end] != 0 {
			end++
		}
		names[string(s[:end])] = true
	}
	return names, nil
}

func newFaninHarness(t *testing.T, wasmPath string, heapMB uint64) *faninHarness {
	t.Helper()
	wasmBytes, err := os.ReadFile(wasmPath)
	if err != nil {
		t.Fatalf("read %s: %v", wasmPath, err)
	}
	ctx := context.Background()
	features := api.CoreFeaturesV2.SetEnabled(api.CoreFeatureMemory64, true)
	r := wazero.NewRuntimeWithConfig(ctx, wazero.NewRuntimeConfig().WithCoreFeatures(features))
	t.Cleanup(func() { r.Close(ctx) })
	if _, err := r.NewHostModuleBuilder("env").Instantiate(ctx); err != nil {
		t.Fatalf("env: %v", err)
	}
	mod, err := r.Instantiate(ctx, wasmBytes)
	if err != nil {
		t.Fatalf("instantiate %s: %v", wasmPath, err)
	}
	h := &faninHarness{ctx: ctx, mod: mod, mem: mod.Memory(), mem64: mod.Memory64(), heapMB: heapMB}
	h.is64 = h.mem64 != nil
	get := func(name string) api.Function {
		fn := mod.ExportedFunction(name)
		if fn == nil {
			t.Fatalf("missing export %s in %s", name, wasmPath)
		}
		return fn
	}
	h.fnInit = get("yowza_segdb_init")
	h.fnPut = get("yowza_segdb_put")
	h.fnGet = get("yowza_segdb_get")
	h.fnDelete = get("yowza_segdb_delete")
	h.fnFlush = get("yowza_segdb_flush")
	h.fnCompact = get("yowza_segdb_compact")
	h.fnSnapshot = get("yowza_segdb_snapshot_seq")
	h.fnScanCursor = get("yowza_segdb_scan_cursor")
	h.fnScanFanin = get("yowza_segdb_scan_fanin")
	h.fnFaninStats = get("yowza_segdb_scan_fanin_stats")
	h.fnFaninReset = get("yowza_segdb_scan_fanin_stats_reset")
	h.fnHeapInfo = get("yowza_segdb_heap_info")
	h.fnHeapCmp = get("yowza_segdb_heap_compact")
	h.fnHeapThresh = get("yowza_segdb_heap_compact_threshold")
	h.fnHeapRecl = get("yowza_segdb_heap_reclaim")
	h.fnHeapReclSt = get("yowza_segdb_heap_reclaim_stats")
	h.fnHeapIdxRb = get("yowza_segdb_heap_index_rebuild")
	res, err := get("yowza_segdb_state_size").Call(ctx)
	if err != nil || len(res) == 0 {
		t.Fatalf("state_size: %v", err)
	}
	stateSize := (res[0] + 65535) &^ 65535
	stateSize += 65536
	base := uint64(64 << 20)
	h.state = base
	h.arena = h.state + stateSize
	h.wal = h.arena + (4 << 20)
	h.scratch = h.wal + (1 << 20)
	h.heap = h.scratch + (1 << 20)
	end := h.heap + (heapMB << 20)
	if err := h.memGrowTo(end); err != nil {
		t.Fatalf("grow: %v", err)
	}
	rc, err := h.call(h.fnInit, h.state, h.arena, 4<<20, h.wal, uint64(uint32(1<<20)), h.heap, heapMB<<20)
	if err != nil || rc != 0 {
		t.Fatalf("init rc=%d err=%v", rc, err)
	}
	return h
}

// ---------------- 900,000-row fanin scan ----------------

func TestFanin900k(t *testing.T) {
	for _, bin := range []string{"build/yowza.wasm", "build/yowza64.wasm"} {
		t.Run(bin, func(t *testing.T) {
			h := newFaninHarness(t, bin, 256)
			buildStart := time.Now()
			flushes := 0
			for i := 0; i < 900000; i++ {
				if err := h.put(fmt.Sprintf("k:%08d", i), fmt.Sprintf("v:%d", i)); err != nil {
					t.Fatalf("put %d: %v", i, err)
				}
				if (i+1)%4096 == 0 {
					if err := h.flush(); err != nil {
						t.Fatalf("flush after row %d: %v", i+1, err)
					}
					flushes++
					if flushes%25 == 0 {
						_, bump, live, slots, waste, _, err := h.heapInfo()
						if err != nil {
							t.Fatalf("heapInfo: %v", err)
						}
						t.Logf("segments=%d heap bump=%d live=%d slots=%d waste=%d wasm_mem=%d",
							flushes, bump, live, slots, waste, h.memSize())
					}
				}
			}
			if 900000%4096 != 0 {
				if err := h.flush(); err != nil {
					t.Fatalf("final flush: %v", err)
				}
				flushes++
			}
			if flushes != 220 {
				t.Fatalf("flushes = %d, want 220", flushes)
			}
			buildDur := time.Since(buildStart)
			snap, err := h.snapshot()
			if err != nil {
				t.Fatalf("snapshot: %v", err)
			}
			t.Logf("built 900000 rows in %s, snapshot=%d, wasm_mem=%d", buildDur.Round(time.Millisecond), snap, h.memSize())
			// The old cursor path must fail on this DB (>128 sources).
			if !h.memWrite(h.scratch+fhKey, []byte("k:")) {
				t.Fatalf("write prefix")
			}
			rc, err := h.call(h.fnScanCursor, h.state, h.scratch+fhKey, 2, snap,
				h.scratch+fhNext, 0, 0, 10, h.scratch+fhOut, fhOutCap, h.scratch+fhCount,
				h.scratch+fhNext, 1024, h.scratch+fhNextLn, h.scratch+fhMore)
			if err != nil {
				t.Fatalf("scan_cursor call: %v", err)
			}
			if rc != -1 {
				t.Fatalf("scan_cursor on 220 segments rc=%d, want -1", rc)
			}
			// Full fanin scan with row-for-row verification.
			if err := h.faninReset(); err != nil {
				t.Fatalf("faninReset: %v", err)
			}
			scanStart := time.Now()
			total, hash, calls, err := h.faninFull("k:", snap, func(idx int, k, v string) error {
				wantK := fmt.Sprintf("k:%08d", idx)
				wantV := fmt.Sprintf("v:%d", idx)
				if k != wantK || v != wantV {
					return fmt.Errorf("row %d: got %q=%q want %q=%q", idx, k, v, wantK, wantV)
				}
				return nil
			})
			if err != nil {
				t.Fatalf("faninFull: %v", err)
			}
			scanDur := time.Since(scanStart)
			if total != 900000 {
				t.Fatalf("fanin rows = %d, want 900000", total)
			}
			sources, windows, seeks, entries, err := h.faninStats()
			if err != nil {
				t.Fatalf("faninStats: %v", err)
			}
			t.Logf("fanin scan: rows=%d time=%s sources=%d windows=%d seeks=%d entries=%d wasm_mem=%d",
				total, scanDur.Round(time.Millisecond), sources, windows, seeks, entries, h.memSize())
			if sources != uint64(220*calls) {
				t.Fatalf("fanin sources = %d, want 220 segments x %d calls = %d", sources, calls, 220*calls)
			}
			// Hot changes after the snapshot: old snapshot
			// unchanged (same hash), new snapshot reflects hot.
			for j := 900000; j <= 900004; j++ {
				if err := h.put(fmt.Sprintf("k:%08d", j), fmt.Sprintf("v:%d", j)); err != nil {
					t.Fatalf("put hot %d: %v", j, err)
				}
			}
			if err := h.put("k:00000000", "hot0"); err != nil {
				t.Fatalf("put hot overwrite: %v", err)
			}
			if err := h.delete("k:00000001"); err != nil {
				t.Fatalf("delete hot: %v", err)
			}
			snapNew, err := h.snapshot()
			if err != nil {
				t.Fatalf("snapshot new: %v", err)
			}
			totalOld, hashOld, _, err := h.faninFull("k:", snap, nil)
			if err != nil {
				t.Fatalf("faninFull old snapshot: %v", err)
			}
			if totalOld != 900000 || hashOld != hash {
				t.Fatalf("old snapshot after hot: rows=%d hash=%x, want 900000 %x", totalOld, hashOld, hash)
			}
			totalNew, _, _, err := h.faninFull("k:", snapNew, nil)
			if err != nil {
				t.Fatalf("faninFull new snapshot: %v", err)
			}
			if totalNew != 900004 {
				t.Fatalf("new snapshot rows = %d, want 900004", totalNew)
			}
			// Spot checks at the new snapshot via prefix scans.
			page, _, _, err := h.faninPage("k:0000000", snapNew, nil, false, 1000)
			if err != nil {
				t.Fatalf("prefix scan k:0000000: %v", err)
			}
			if len(page) != 9 || page[0][0] != "k:00000000" || page[0][1] != "hot0" || page[1][0] != "k:00000002" {
				t.Fatalf("prefix k:0000000: %v, want 9 rows starting hot0, k:00000002", page)
			}
			page, _, _, err = h.faninPage("k:0090000", snapNew, nil, false, 1000)
			if err != nil {
				t.Fatalf("prefix scan k:0090000: %v", err)
			}
			if len(page) != 5 || page[0][0] != "k:00900000" || page[4][0] != "k:00900004" {
				t.Fatalf("prefix k:0090000: %v, want 5 rows k:00900000..k:00900004", page)
			}
		})
	}
}

// ---------------- heap exports through wasm ----------------

func TestHeapReclaimDirect(t *testing.T) {
	for _, bin := range []string{"build/yowza.wasm", "build/yowza64.wasm"} {
		t.Run(bin, func(t *testing.T) {
			h := newFaninHarness(t, bin, 16)
			if err := h.heapThreshold(4096); err != nil {
				t.Fatalf("heapThreshold: %v", err)
			}
			// 20 cycles: put 120, overwrite 10 of the previous
			// cycle, flush, compact, delete evens, flush,
			// reclaim at the boundary.
			for c := 0; c < 20; c++ {
				for i := 0; i < 120; i++ {
					if err := h.put(fmt.Sprintf("k:%06d", c*120+i), fmt.Sprintf("v:%d:%d", c, i)); err != nil {
						t.Fatalf("cycle %d put: %v", c, err)
					}
				}
				if c > 0 {
					for i := 0; i < 10; i++ {
						if err := h.put(fmt.Sprintf("k:%06d", (c-1)*120+i), fmt.Sprintf("w:%d", c)); err != nil {
							t.Fatalf("cycle %d overwrite: %v", c, err)
						}
					}
				}
				if err := h.flush(); err != nil {
					t.Fatalf("cycle %d flush: %v", c, err)
				}
				if err := h.compact(); err != nil {
					t.Fatalf("cycle %d compact: %v", c, err)
				}
				for i := 0; i < 120; i += 2 {
					if err := h.delete(fmt.Sprintf("k:%06d", c*120+i)); err != nil {
						t.Fatalf("cycle %d delete: %v", c, err)
					}
				}
				if err := h.flush(); err != nil {
					t.Fatalf("cycle %d flush2: %v", c, err)
				}
				if rc, err := h.heapReclaim(); err != nil || rc != 0 {
					t.Fatalf("cycle %d reclaim rc=%d err=%v", c, rc, err)
				}
			}
			if rc, err := h.heapReclaim(); err != nil || rc != 0 {
				t.Fatalf("final reclaim rc=%d err=%v", rc, err)
			}
			if rc, err := h.heapCompact(); err != nil || rc != 0 {
				t.Fatalf("final heap compact rc=%d err=%v", rc, err)
			}
			_, bump, _, slots, waste, _, err := h.heapInfo()
			if err != nil {
				t.Fatalf("heapInfo: %v", err)
			}
			if bump >= 16<<20 || waste >= 65536 || slots >= 128 {
				t.Fatalf("heap after cycles: bump=%d waste=%d slots=%d, want bump<16MiB waste<64KiB slots<128", bump, waste, slots)
			}
			// Byte-for-byte verification of the 2400 keys.
			total, _, _, err := h.faninFull("k:", mustSnapshot(t, h), func(idx int, k, v string) error {
				// Rows arrive in key order; derive the model
				// from the key index (parsed manually: k:%06d).
				if len(k) != 8 || k[0] != 'k' || k[1] != ':' {
					return fmt.Errorf("bad key %q", k)
				}
				gi := 0
				for _, ch := range k[2:] {
					if ch < '0' || ch > '9' {
						return fmt.Errorf("bad key %q", k)
					}
					gi = gi*10 + int(ch-'0')
				}
				c, i := gi/120, gi%120
				var want string
				if c < 19 && i < 10 {
					want = fmt.Sprintf("w:%d", c+1)
				} else if i%2 == 0 {
					return fmt.Errorf("key %q should be deleted", k)
				} else {
					want = fmt.Sprintf("v:%d:%d", c, i)
				}
				if v != want {
					return fmt.Errorf("key %q: got %q want %q", k, v, want)
				}
				return nil
			})
			if err != nil {
				t.Fatalf("faninFull cycles verification: %v", err)
			}
			// Live rows: 20 cycles x 60 odds = 1200, minus the
			// 10 overwritten evens of cycles 0..18 that were
			// resurrected by the next cycle's overwrite... the
			// model above counts them live, so: odds (1200)
			// + resurrected even indices 0..9 of cycles 0..18
			// that are even (0,2,4,6,8 = 5 per cycle x 19 = 95).
			if total != 1295 {
				t.Fatalf("cycles live rows = %d, want 1295", total)
			}
			// Corrupt-slot test: build a small DB on a fresh
			// harness (separate heap image), corrupt one segment
			// slot, prove compact fails and moves nothing, then
			// repair and prove compact succeeds with data intact.
			h2 := newFaninHarness(t, bin, 16)
			for j := 0; j < 50; j++ {
				if err := h2.put(fmt.Sprintf("a:%03d", j), fmt.Sprintf("av:%d", j)); err != nil {
					t.Fatalf("put a: %v", err)
				}
			}
			if err := h2.flush(); err != nil {
				t.Fatalf("flush a: %v", err)
			}
			for j := 0; j < 50; j++ {
				if err := h2.put(fmt.Sprintf("b:%03d", j), fmt.Sprintf("bv:%d", j)); err != nil {
					t.Fatalf("put b: %v", err)
				}
			}
			if err := h2.flush(); err != nil {
				t.Fatalf("flush b: %v", err)
			}
			heapBefore, ok := h2.memRead(h2.heap, 16<<20)
			if !ok {
				t.Fatalf("read heap before")
			}
			heapBefore = append([]byte(nil), heapBefore...)
			segSlot := -1
			for i := 0; i < 4096; i++ {
				s := heapBefore[32+i*152:]
				if s[0] != 0 && len(s) >= 4 && string(s[:4]) == "seg." {
					segSlot = i
					break
				}
			}
			if segSlot < 0 {
				t.Fatalf("no segment slot found")
			}
			slotOff := h2.heap + 32 + uint64(segSlot)*152
			saved := append([]byte(nil), heapBefore[32+uint64(segSlot)*152:32+uint64(segSlot)*152+152]...)
			bad := make([]byte, 8)
			binary.LittleEndian.PutUint64(bad, (16<<20)+4096)
			if !h2.memWrite(slotOff+128, bad) {
				t.Fatalf("write corrupt offset")
			}
			if rc, err := h2.heapCompact(); err != nil || rc != -1 {
				t.Fatalf("compact with corrupt slot rc=%d err=%v, want -1", rc, err)
			}
			heapAfter, ok := h2.memRead(h2.heap, 16<<20)
			if !ok {
				t.Fatalf("read heap after")
			}
			for i := range heapAfter {
				want := heapBefore[i]
				if i >= 32+segSlot*152+128 && i < 32+segSlot*152+136 {
					want = bad[i-(32+segSlot*152+128)]
				}
				if heapAfter[i] != want {
					t.Fatalf("heap byte %d changed by failed compact: got %d want %d", i, heapAfter[i], want)
				}
			}
			// The corrupted slot may hold either segment, so
			// prove at least one full group stays readable
			// while corrupted (never a wrong value: a key
			// either reads its exact value or errors).
			aOK, bOK := 0, 0
			for j := 0; j < 50; j++ {
				if v, err := h2.get(fmt.Sprintf("a:%03d", j)); err == nil && v == fmt.Sprintf("av:%d", j) {
					aOK++
				}
				if v, err := h2.get(fmt.Sprintf("b:%03d", j)); err == nil && v == fmt.Sprintf("bv:%d", j) {
					bOK++
				}
			}
			if aOK != 50 && bOK != 50 {
				t.Fatalf("while corrupted: readable a=%d/50 b=%d/50, want one full group of 50", aOK, bOK)
			}
			if !h2.memWrite(slotOff, saved) {
				t.Fatalf("restore slot")
			}
			if rc, err := h2.heapIndexRebuild(); err != nil || rc != 0 {
				t.Fatalf("index rebuild rc=%d err=%v, want 0", rc, err)
			}
			if rc, err := h2.heapCompact(); err != nil || rc != 0 {
				t.Fatalf("compact after repair rc=%d err=%v, want 0", rc, err)
			}
			for j := 0; j < 50; j++ {
				if v, err := h2.get(fmt.Sprintf("a:%03d", j)); err != nil || v != fmt.Sprintf("av:%d", j) {
					t.Fatalf("get a:%03d = %q err %v", j, v, err)
				}
				if v, err := h2.get(fmt.Sprintf("b:%03d", j)); err != nil || v != fmt.Sprintf("bv:%d", j) {
					t.Fatalf("get b:%03d = %q err %v", j, v, err)
				}
			}
			// Orphan test on the cycles DB: a compact leaves
			// .dat/.idx temps as orphans; reclaim must delete
			// them while keeping CURRENT, the current manifest,
			// and the gen-1 fallback manifest.
			if err := h.compact(); err != nil {
				t.Fatalf("orphan-setup compact: %v", err)
			}
			before, err := h.slotNames()
			if err != nil {
				t.Fatalf("slotNames before: %v", err)
			}
			temps := 0
			for name := range before {
				if len(name) > 4 && (name[len(name)-4:] == ".dat" || name[len(name)-4:] == ".idx") {
					temps++
				}
			}
			if temps == 0 {
				t.Fatalf("no .dat/.idx temps present before reclaim, cannot prove orphan deletion")
			}
			if rc, err := h.heapReclaim(); err != nil || rc != 0 {
				t.Fatalf("orphan reclaim rc=%d err=%v", rc, err)
			}
			del, _, err := h.heapReclaimStats()
			if err != nil {
				t.Fatalf("reclaim stats: %v", err)
			}
			if del == 0 {
				t.Fatalf("reclaim deleted 0 objects, want > 0")
			}
			after, err := h.slotNames()
			if err != nil {
				t.Fatalf("slotNames after: %v", err)
			}
			for name := range after {
				if len(name) > 4 && (name[len(name)-4:] == ".dat" || name[len(name)-4:] == ".idx") {
					t.Fatalf("temp %q survived reclaim", name)
				}
			}
			if !after["CURRENT"] {
				t.Fatalf("CURRENT deleted by reclaim")
			}
			// Live data still fully readable after reclaim.
			total2, _, _, err := h.faninFull("k:", mustSnapshot(t, h), nil)
			if err != nil {
				t.Fatalf("faninFull after reclaim: %v", err)
			}
			if total2 != 1295 {
				t.Fatalf("rows after reclaim = %d, want 1295", total2)
			}
		})
	}
}

func mustSnapshot(t *testing.T, h *faninHarness) uint64 {
	t.Helper()
	snap, err := h.snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	return snap
}
