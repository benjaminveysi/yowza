package yowza

// Track C Go tests: >4096 fan-in through the REAL wasm path.
//
// TestTrackCHierFaninWasm builds 5002 real segments through the
// wasm exports (5000 single-key flushes + one overwrite flush +
// one delete flush) and scans them through the new
// yowza_segdb_scan_fanin_hier export on BOTH binaries, verifying
// the result row-for-row against the reference model. The
// level-traversal proof is the hier stats read after the first
// page: groups >= 2 (multiple manifest levels walked),
// sources == 5002 (> 4096), max_group == groups-1. The flat
// fan-in export must still refuse the same DB with rc=-1.

import (
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/tetratelabs/wazero/api"
)

// hierPage runs one hier scan call, mirroring faninPage's page
// protocol (same argument order, same out encoding, same -3
// partial-page contract).
func hierPage(h *faninHarness, fn api.Function, prefix string, snap uint64, cursor []byte, hasCursor bool, limit uint32) (entries [][2]string, nextKey []byte, hasMore bool, err error) {
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
	rc, err := h.call(fn, h.state, h.scratch+fhKey, uint64(len(prefix)), snap,
		curPtr, curLen, hasCur, uint64(limit),
		h.scratch+fhOut, fhOutCap, h.scratch+fhCount,
		h.scratch+fhNext, 1024, h.scratch+fhNextLn, h.scratch+fhMore)
	if err != nil {
		return nil, nil, false, err
	}
	if rc != 0 && rc != -3 {
		return nil, nil, false, fmt.Errorf("scan_fanin_hier rc=%d", rc)
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
		return nil, nil, false, fmt.Errorf("hier -3 with zero rows")
	}
	return entries, nextKey, hasMore, nil
}

func hierStats(h *faninHarness, fn api.Function) (groups, sources, maxGroup, windows uint64, err error) {
	if err := h.callVoid(fn, h.scratch+fhStats); err != nil {
		return 0, 0, 0, 0, err
	}
	raw, ok := h.memRead(h.scratch+fhStats, 32)
	if !ok {
		return 0, 0, 0, 0, fmt.Errorf("read hier stats")
	}
	return binary.LittleEndian.Uint64(raw[0:8]), binary.LittleEndian.Uint64(raw[8:16]),
		binary.LittleEndian.Uint64(raw[16:24]), binary.LittleEndian.Uint64(raw[24:32]), nil
}

func TestTrackCHierFaninWasm(t *testing.T) {
	for _, bin := range []string{"build/yowza.wasm", "build/yowza64.wasm"} {
		t.Run(bin, func(t *testing.T) {
			h := newFaninHarness(t, bin, 256)
			fnHier := h.mod.ExportedFunction("yowza_segdb_scan_fanin_hier")
			if fnHier == nil {
				t.Fatalf("missing export yowza_segdb_scan_fanin_hier in %s", bin)
			}
			fnHierStats := h.mod.ExportedFunction("yowza_segdb_scan_fanin_hier_stats")
			if fnHierStats == nil {
				t.Fatalf("missing export yowza_segdb_scan_fanin_hier_stats in %s", bin)
			}
			fnHierReset := h.mod.ExportedFunction("yowza_segdb_scan_fanin_hier_stats_reset")
			if fnHierReset == nil {
				t.Fatalf("missing export yowza_segdb_scan_fanin_hier_stats_reset in %s", bin)
			}
			buildStart := time.Now()
			for i := 0; i < 5000; i++ {
				if err := h.put(fmt.Sprintf("h:%06d", i), fmt.Sprintf("v:%d", i)); err != nil {
					t.Fatalf("put %d: %v", i, err)
				}
				if err := h.flush(); err != nil {
					t.Fatalf("flush %d: %v", i, err)
				}
			}
			for j := 100; j <= 109; j++ {
				if err := h.put(fmt.Sprintf("h:%06d", j), fmt.Sprintf("w:%d", j)); err != nil {
					t.Fatalf("put overwrite %d: %v", j, err)
				}
			}
			if err := h.flush(); err != nil {
				t.Fatalf("flush overwrite: %v", err)
			}
			for j := 200; j <= 209; j++ {
				if err := h.delete(fmt.Sprintf("h:%06d", j)); err != nil {
					t.Fatalf("delete %d: %v", j, err)
				}
			}
			if err := h.flush(); err != nil {
				t.Fatalf("flush delete: %v", err)
			}
			snap, err := h.snapshot()
			if err != nil {
				t.Fatalf("snapshot: %v", err)
			}
			t.Logf("build 5002 segments: %v", time.Since(buildStart))

			// Flat fan-in must still refuse this DB: >4096
			// sources in one flat call is rc=-1 by contract.
			rc, err := h.call(h.fnScanFanin, h.state, h.scratch+fhKey, uint64(0), snap,
				h.scratch+fhNext, uint64(0), uint64(0), uint64(10),
				h.scratch+fhOut, fhOutCap, h.scratch+fhCount,
				h.scratch+fhNext, 1024, h.scratch+fhNextLn, h.scratch+fhMore)
			if err != nil {
				t.Fatalf("flat fanin call: %v", err)
			}
			if rc != -1 {
				t.Fatalf("flat fanin on 5002 segments: rc=%d, want -1", rc)
			}

			// Hierarchical scan, row-for-row against the model.
			if err := h.callVoid(fnHierReset); err != nil {
				t.Fatalf("hier stats reset: %v", err)
			}
			scanStart := time.Now()
			var cursor []byte
			hasCursor := false
			total := 0
			expect := 0
			calls := 0
			var firstGroups, firstSources, firstMax, firstWindows uint64
			for {
				page, nextKey, hasMore, err := hierPage(h, fnHier, "", snap, cursor, hasCursor, 1<<30)
				if err != nil {
					t.Fatalf("hier page %d: %v", calls, err)
				}
				calls++
				if calls == 1 {
					firstGroups, firstSources, firstMax, firstWindows, err = hierStats(h, fnHierStats)
					if err != nil {
						t.Fatalf("hier stats: %v", err)
					}
				}
				for _, e := range page {
					for expect < 5000 && expect >= 200 && expect <= 209 {
						expect++
					}
					if expect >= 5000 {
						t.Fatalf("extra row %q", e[0])
					}
					wantK := fmt.Sprintf("h:%06d", expect)
					wantV := fmt.Sprintf("v:%d", expect)
					if expect >= 100 && expect <= 109 {
						wantV = fmt.Sprintf("w:%d", expect)
					}
					if e[0] != wantK || e[1] != wantV {
						t.Fatalf("row %d: got %q=%q want %q=%q", expect, e[0], e[1], wantK, wantV)
					}
					expect++
					total++
				}
				if !hasMore {
					break
				}
				if len(page) == 0 {
					t.Fatalf("hier made no progress (empty page with hasMore set)")
				}
				cursor, hasCursor = nextKey, true
			}
			// total counts scanned live rows (4990); expect
			// is the model index, which ends at 5000 after
			// skipping the 10 tombstoned indices.
			if total != 4990 {
				t.Fatalf("hier rows = %d, want 4990", total)
			}
			if expect != 5000 {
				t.Fatalf("hier model index = %d, want 5000", expect)
			}
			t.Logf("hier scan: %d rows in %d calls, %v", total, calls, time.Since(scanStart))
			t.Logf("hier stats after first page: groups=%d sources=%d max_group=%d windows=%d",
				firstGroups, firstSources, firstMax, firstWindows)
			// Level-traversal proof: more than one manifest
			// level was walked, all 5002 sources were
			// enumerated (> 4096), and max_group is the last
			// group index.
			if firstGroups < 2 {
				t.Fatalf("hier groups = %d, want >= 2 (multi-level traversal)", firstGroups)
			}
			if firstSources != 5002 {
				t.Fatalf("hier sources = %d, want 5002", firstSources)
			}
			if firstMax != firstGroups-1 {
				t.Fatalf("hier max_group = %d, want groups-1 = %d", firstMax, firstGroups-1)
			}
			if firstWindows == 0 {
				t.Fatalf("hier windows = 0, want > 0")
			}
		})
	}
}
