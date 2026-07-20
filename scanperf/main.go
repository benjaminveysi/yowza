// Command scanperf benchmarks Yowza64 segdb scans through the wasm
// exports directly (no dependency on the Go SegDB bindings, so it
// can use a heap larger than the bindings' fixed 4 MiB region and
// can call the new cursor + stats exports).
//
// Usage:
//
//	go run ./scanperf -wasm yowza.wasm -rows 9000 -page 1000
//	go run ./scanperf -wasm yowza64.wasm -rows 90000 -page 1000
//
// It builds a DB of -rows keys (k:%08u, flushed every
// -flushbatch rows), then pages the whole result twice at a
// frozen snapshot: once with the old skip path
// (yowza_segdb_scan_page) and once with the new cursor path
// (yowza_segdb_scan_cursor), verifying row order/content on
// the fly and reporting wall time plus C-side work counters
// (iterator visits, block-parse skips, sparse-index entries,
// discovery-cache hits/misses, heap-index lookups/probes).
//
// Scales: the scan APIs cap segment sources at 128, and a flush
// holds at most 4096 entries, so a DB of more than 128*4096 =
// 524288 rows cannot be fully scanned by either path (the scan
// fails with rc=-1 before reading any row). The harness
// reports that honestly instead of silently scanning a prefix.
package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
)

const pageSize = 65536

type harness struct {
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

	fnInit       api.Function
	fnPut        api.Function
	fnFlush      api.Function
	fnSnapshot   api.Function
	fnScanPage   api.Function
	fnScanCursor api.Function
	fnHeapStats  api.Function
	fnHeapReset  api.Function
	fnScanStats  api.Function
	fnScanReset  api.Function
}

func (h *harness) memSize() uint64 {
	if h.is64 && h.mem64 != nil {
		return h.mem64.Size64()
	}
	return uint64(h.mem.Size())
}

func (h *harness) memGrowTo(target uint64) error {
	size := h.memSize()
	if size >= target {
		return nil
	}
	need := (target - size + pageSize - 1) / pageSize
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

func (h *harness) memRead(off, n uint64) ([]byte, bool) {
	if h.is64 && h.mem64 != nil {
		return h.mem64.Read64(off, n)
	}
	if off > 0xFFFFFFFF || n > 0xFFFFFFFF {
		return nil, false
	}
	return h.mem.Read(uint32(off), uint32(n))
}

func (h *harness) memWrite(off uint64, val []byte) bool {
	if h.is64 && h.mem64 != nil {
		return h.mem64.Write64(off, val)
	}
	if off > 0xFFFFFFFF {
		return false
	}
	return h.mem.Write(uint32(off), val)
}

func (h *harness) call(fn api.Function, args ...uint64) (int32, error) {
	res, err := fn.Call(h.ctx, args...)
	if err != nil {
		return 0, err
	}
	if len(res) == 0 {
		return 0, fmt.Errorf("empty result")
	}
	return int32(res[0]), nil
}

func (h *harness) callU64(fn api.Function, args ...uint64) (uint64, error) {
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
	scrKey    = 0
	scrVal    = 2048
	scrCount  = 4096
	scrNext   = 6144
	scrNextLn = 8192 + 0 // separate cells below out region start
	scrMore   = 8192 + 8
	scrStats  = 8192 + 16
	scrOut    = 16384
	scrOutCap = 1048576 - scrOut
)

func (h *harness) put(key, val string) error {
	if !h.memWrite(h.scratch+scrKey, []byte(key)) {
		return fmt.Errorf("write key")
	}
	if !h.memWrite(h.scratch+scrVal, []byte(val)) {
		return fmt.Errorf("write val")
	}
	rc, err := h.call(h.fnPut, h.state, h.scratch+scrKey, uint64(len(key)),
		h.scratch+scrVal, uint64(len(val)))
	if err != nil {
		return err
	}
	if rc != 0 {
		return fmt.Errorf("put %s rc=%d", key, rc)
	}
	return nil
}

func (h *harness) flush() error {
	rc, err := h.call(h.fnFlush, h.state)
	if err != nil {
		return err
	}
	if rc != 0 {
		return fmt.Errorf("flush rc=%d", rc)
	}
	return nil
}

func (h *harness) snapshot() (uint64, error) {
	return h.callU64(h.fnSnapshot, h.state)
}

func (h *harness) readStats() (heap, scan []uint64, err error) {
	if h.fnHeapStats != nil {
		h.fnHeapStats.Call(h.ctx, h.scratch+scrStats)
		raw, ok := h.memRead(h.scratch+scrStats, 48)
		if !ok {
			return nil, nil, fmt.Errorf("read heap stats")
		}
		heap = make([]uint64, 6)
		for i := range heap {
			heap[i] = binary.LittleEndian.Uint64(raw[i*8:])
		}
	}
	if h.fnScanStats != nil {
		h.fnScanStats.Call(h.ctx, h.scratch+scrStats)
		raw, ok := h.memRead(h.scratch+scrStats, 48)
		if !ok {
			return nil, nil, fmt.Errorf("read scan stats")
		}
		scan = make([]uint64, 6)
		for i := range scan {
			scan[i] = binary.LittleEndian.Uint64(raw[i*8:])
		}
	}
	return heap, scan, nil
}

func (h *harness) resetStats() {
	if h.fnHeapReset != nil {
		h.fnHeapReset.Call(h.ctx)
	}
	if h.fnScanReset != nil {
		h.fnScanReset.Call(h.ctx)
	}
}

// decodeAndCheck verifies one page's art_scan-format output:
// keys must be exactly k:%08u for expected.. in order.
func (h *harness) decodeAndCheck(raw []byte, count uint32, expected *int) error {
	off := 0
	for i := uint32(0); i < count; i++ {
		if off+8 > len(raw) {
			return fmt.Errorf("truncated page")
		}
		klen := int(binary.LittleEndian.Uint32(raw[off:]))
		off += 4
		if off+klen+4 > len(raw) {
			return fmt.Errorf("truncated key")
		}
		key := string(raw[off : off+klen])
		off += klen
		vlen := int(binary.LittleEndian.Uint32(raw[off:]))
		off += 4
		if off+vlen > len(raw) {
			return fmt.Errorf("truncated val")
		}
		val := string(raw[off : off+vlen])
		off += vlen
		want := fmt.Sprintf("k:%08d", *expected)
		wantV := fmt.Sprintf("v:%d", *expected)
		if key != want || val != wantV {
			return fmt.Errorf("row %d: got %q/%q want %q/%q", *expected, key, val, want, wantV)
		}
		*expected++
	}
	return nil
}

func (h *harness) pageSkip(snap uint64, pageLimit uint32) (uint64, time.Duration, error) {
	if !h.memWrite(h.scratch+scrKey, []byte("k:")) {
		return 0, 0, fmt.Errorf("write prefix")
	}
	h.resetStats()
	var skip uint64
	var total uint64
	expected := 0
	start := time.Now()
	for {
		rc, err := h.call(h.fnScanPage, h.state, h.scratch+scrKey, 2, snap, skip,
			uint64(pageLimit), h.scratch+scrOut, scrOutCap, h.scratch+scrCount)
		if err != nil {
			return total, 0, err
		}
		if rc != 0 {
			return total, 0, fmt.Errorf("scan_page rc=%d skip=%d", rc, skip)
		}
		raw, ok := h.memRead(h.scratch+scrCount, 4)
		if !ok {
			return total, 0, fmt.Errorf("read count")
		}
		cnt := binary.LittleEndian.Uint32(raw)
		out, ok := h.memRead(h.scratch+scrOut, scrOutCap)
		if !ok {
			return total, 0, fmt.Errorf("read out")
		}
		if err := h.decodeAndCheck(out, cnt, &expected); err != nil {
			return total, 0, err
		}
		total += uint64(cnt)
		skip += uint64(cnt)
		if cnt < pageLimit {
			break
		}
	}
	return total, time.Since(start), nil
}

func (h *harness) pageCursor(snap uint64, pageLimit uint32) (uint64, time.Duration, error) {
	if !h.memWrite(h.scratch+scrKey, []byte("k:")) {
		return 0, 0, fmt.Errorf("write prefix")
	}
	h.resetStats()
	var total uint64
	expected := 0
	var cur []byte
	hasCur := uint64(0)
	start := time.Now()
	for {
		curPtr := h.scratch + scrNext
		if len(cur) > 0 {
			if !h.memWrite(curPtr, cur) {
				return total, 0, fmt.Errorf("write cursor")
			}
		}
		rc, err := h.call(h.fnScanCursor, h.state, h.scratch+scrKey, 2, snap,
			curPtr, uint64(len(cur)), hasCur, uint64(pageLimit),
			h.scratch+scrOut, scrOutCap, h.scratch+scrCount,
			h.scratch+scrNext, 1024, h.scratch+scrNextLn, h.scratch+scrMore)
		if err != nil {
			return total, 0, err
		}
		if rc != 0 {
			return total, 0, fmt.Errorf("scan_cursor rc=%d total=%d", rc, total)
		}
		raw, ok := h.memRead(h.scratch+scrCount, 4)
		if !ok {
			return total, 0, fmt.Errorf("read count")
		}
		cnt := binary.LittleEndian.Uint32(raw)
		out, ok := h.memRead(h.scratch+scrOut, scrOutCap)
		if !ok {
			return total, 0, fmt.Errorf("read out")
		}
		if err := h.decodeAndCheck(out, cnt, &expected); err != nil {
			return total, 0, err
		}
		total += uint64(cnt)
		lnRaw, ok := h.memRead(h.scratch+scrNextLn, 4)
		if !ok {
			return total, 0, fmt.Errorf("read next len")
		}
		nkLen := binary.LittleEndian.Uint32(lnRaw)
		moreRaw, ok := h.memRead(h.scratch+scrMore, 4)
		if !ok {
			return total, 0, fmt.Errorf("read has_more")
		}
		hasMore := binary.LittleEndian.Uint32(moreRaw)
		if cnt == 0 {
			if hasMore != 0 {
				return total, 0, fmt.Errorf("empty page with has_more set")
			}
			break
		}
		nk, ok := h.memRead(h.scratch+scrNext, uint64(nkLen))
		if !ok {
			return total, 0, fmt.Errorf("read next key")
		}
		cur = append([]byte(nil), nk...)
		hasCur = 1
		if hasMore == 0 {
			break
		}
	}
	return total, time.Since(start), nil
}

func main() {
	wasmPath := flag.String("wasm", "yowza.wasm", "wasm binary path")
	rows := flag.Int("rows", 9000, "rows to build")
	pageLimit := flag.Uint("page", 1000, "page limit for both paths")
	flushBatch := flag.Int("flushbatch", 1000, "rows per flush")
	heapMB := flag.Uint64("heapmb", 256, "heap region size in MiB")
	skipOnly := flag.Bool("skiponly", false, "only run the skip path")
	cursorOnly := flag.Bool("cursoronly", false, "only run the cursor path")
	flag.Parse()

	wasmBytes, err := os.ReadFile(*wasmPath)
	if err != nil {
		fmt.Println("read wasm:", err)
		os.Exit(1)
	}
	ctx := context.Background()
	features := api.CoreFeaturesV2.SetEnabled(api.CoreFeatureMemory64, true)
	r := wazero.NewRuntimeWithConfig(ctx, wazero.NewRuntimeConfig().WithCoreFeatures(features))
	defer r.Close(ctx)
	if _, err := r.NewHostModuleBuilder("env").Instantiate(ctx); err != nil {
		fmt.Println("env:", err)
		os.Exit(1)
	}
	mod, err := r.Instantiate(ctx, wasmBytes)
	if err != nil {
		fmt.Println("instantiate:", err)
		os.Exit(1)
	}
	h := &harness{ctx: ctx, mod: mod, mem: mod.Memory(), mem64: mod.Memory64()}
	h.is64 = h.mem64 != nil
	get := func(name string) api.Function {
		fn := mod.ExportedFunction(name)
		if fn == nil {
			fmt.Printf("missing export %s\n", name)
			os.Exit(1)
		}
		return fn
	}
	opt := func(name string) api.Function { return mod.ExportedFunction(name) }
	h.fnInit = get("yowza_segdb_init")
	h.fnPut = get("yowza_segdb_put")
	h.fnFlush = get("yowza_segdb_flush")
	h.fnSnapshot = get("yowza_segdb_snapshot_seq")
	h.fnScanPage = get("yowza_segdb_scan_page")
	h.fnScanCursor = get("yowza_segdb_scan_cursor")
	h.fnHeapStats = opt("yowza_segdb_heap_stats")
	h.fnHeapReset = opt("yowza_segdb_heap_stats_reset")
	h.fnScanStats = opt("yowza_segdb_scan_stats")
	h.fnScanReset = opt("yowza_segdb_scan_stats_reset")

	// Layout: state at 64 MiB, then arena 4 MiB, WAL 1 MiB,
	// scratch 1 MiB, heap -heapmb MiB.
	res, err := get("yowza_segdb_state_size").Call(ctx)
	if err != nil || len(res) == 0 {
		fmt.Println("state_size:", err)
		os.Exit(1)
	}
	stateSize := (res[0] + 65535) &^ 65535
	stateSize += 65536
	base := uint64(64 << 20)
	h.state = base
	h.arena = h.state + stateSize
	h.wal = h.arena + (4 << 20)
	h.scratch = h.wal + (1 << 20)
	h.heap = h.scratch + (1 << 20)
	end := h.heap + (*heapMB << 20)
	if err := h.memGrowTo(end); err != nil {
		fmt.Println("grow:", err)
		os.Exit(1)
	}
	rc, err := h.call(h.fnInit, h.state, h.arena, 4<<20, h.wal, uint64(uint32(1<<20)), h.heap, *heapMB<<20)
	if err != nil || rc != 0 {
		fmt.Println("init rc:", rc, err)
		os.Exit(1)
	}
	buildStart := time.Now()
	for i := 0; i < *rows; i++ {
		if err := h.put(fmt.Sprintf("k:%08d", i), fmt.Sprintf("v:%d", i)); err != nil {
			fmt.Println("put:", err)
			os.Exit(1)
		}
		if (i+1)%*flushBatch == 0 {
			if err := h.flush(); err != nil {
				fmt.Printf("flush after row %d: %v\n", i+1, err)
				os.Exit(1)
			}
		}
	}
	if *rows%*flushBatch != 0 {
		if err := h.flush(); err != nil {
			fmt.Println("final flush:", err)
			os.Exit(1)
		}
	}
	buildDur := time.Since(buildStart)
	snap, err := h.snapshot()
	if err != nil {
		fmt.Println("snapshot:", err)
		os.Exit(1)
	}
	fmt.Printf("wasm=%s rows=%d page=%d flushbatch=%d build=%s snapshot=%d\n",
		*wasmPath, *rows, *pageLimit, *flushBatch, buildDur.Round(time.Millisecond), snap)

	if !*cursorOnly {
		total, dur, err := h.pageSkip(snap, uint32(*pageLimit))
		heap, scan, _ := h.readStats()
		if err != nil {
			fmt.Printf("SKIP FAILED total=%d err=%v\n", total, err)
		} else {
			fmt.Printf("SKIP rows=%d time=%s page_visits=%d\n", total, dur.Round(time.Millisecond), scan[0])
			fmt.Printf("  heap lookups=%d probes=%d rebuilds=%d stale=%d fallbacks=%d\n",
				heap[0], heap[1], heap[2], heap[3], heap[4])
		}
	}
	if !*skipOnly {
		total, dur, err := h.pageCursor(snap, uint32(*pageLimit))
		heap, scan, _ := h.readStats()
		if err != nil {
			fmt.Printf("CURSOR FAILED total=%d err=%v\n", total, err)
		} else {
			fmt.Printf("CURSOR rows=%d time=%s cursor_visits=%d block_skips=%d seek_index=%d cache_hits=%d cache_misses=%d\n",
				total, dur.Round(time.Millisecond), scan[1], scan[2], scan[3], scan[4], scan[5])
			fmt.Printf("  heap lookups=%d probes=%d rebuilds=%d stale=%d fallbacks=%d\n",
				heap[0], heap[1], heap[2], heap[3], heap[4])
		}
	}
}
