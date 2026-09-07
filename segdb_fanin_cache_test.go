package yowza

// O3 Go tests: cross-call fan-in discovery cache (see
// include/segdb_scan.h for the contract). The cache is implicit
// inside the existing yowza_segdb_scan_fanin export: page calls
// with limit <= 4096 reuse the cached discovery for the same
// (database, prefix, snapshot); larger-limit calls stay on the
// original stateless path. No new wasm export is involved (only
// yowza_* exports reach wasm), so these tests drive the cache
// through the ordinary ScanFaninPage wrapper and prove reuse
// with the existing FaninStats counters: sources counts
// DISCOVERED sources, so a cache-hit page adds 0 to it.
//
//   - TestFaninCacheSmallLimitPages: repeated small-limit pages
//     over one snapshot discover once (page 1 sources ==
//     segment count) and then add 0 per page, with rows
//     identical to the stateless full scan.
//   - TestFaninCacheFlushBetweenPages: a flush between pages
//     (hot keys flushed, then new keys flushed) is
//     transparently re-discovered — rows stay exactly the old
//     snapshot's rows, no error, no mixed rows.
//   - TestFaninCacheCompactBetweenPages: a compact between
//     pages (old snapshot and current snapshot) fails the
//     continuation with the typed ErrSegDBFaninStale, never
//     mixed rows; a fresh cursorless scan afterwards succeeds,
//     proving the stale state is session-scoped.
//   - TestFaninCachePagingBenchmark: paging time and
//     source-visit counts, cached vs a stateless-equivalent
//     paging pattern (session evicted between pages).
//   - TestFaninCacheCorruptSegment (direct exports): a segment
//     corrupted after discovery — header bytes and data-block
//     bytes — still fails the next cached page with an error,
//     never returning cached rows.

import (
	"encoding/binary"
	"fmt"
	"os"
	"testing"
	"time"
)

// pageCached pages one result with ScanFaninPage at the given
// limit, recording the FaninStats delta (sources, seeks) of
// every page. It returns all rows in order, the per-page source
// deltas, the per-page seek deltas, and the per-page durations.
func pageCached(t *testing.T, sdb *SegDB, prefix string, snap uint64, limit uint32) ([]SegDBScanEntry, []uint64, []uint64, []time.Duration) {
	t.Helper()
	var rows []SegDBScanEntry
	var srcDeltas, seekDeltas []uint64
	var durs []time.Duration
	var cursor []byte
	hasCursor := false
	for {
		if err := sdb.FaninStatsReset(); err != nil {
			t.Fatalf("FaninStatsReset: %v", err)
		}
		start := time.Now()
		page, nextKey, hasMore, err := sdb.ScanFaninPage(prefix, snap, cursor, hasCursor, limit)
		durs = append(durs, time.Since(start))
		if err != nil {
			t.Fatalf("ScanFaninPage (cursor=%q hasCursor=%v): %v", cursor, hasCursor, err)
		}
		sources, _, seeks, _, err := sdb.FaninStats()
		if err != nil {
			t.Fatalf("FaninStats: %v", err)
		}
		srcDeltas = append(srcDeltas, sources)
		seekDeltas = append(seekDeltas, seeks)
		rows = append(rows, page...)
		if !hasMore {
			return rows, srcDeltas, seekDeltas, durs
		}
		if len(page) == 0 {
			t.Fatalf("fanin page made no progress (empty page with hasMore set)")
		}
		cursor, hasCursor = nextKey, true
	}
}

func checkRowsEqual(t *testing.T, got, want []SegDBScanEntry) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("rows = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if string(got[i].Key) != string(want[i].Key) || string(got[i].Val) != string(want[i].Val) {
			t.Fatalf("row %d: got %q=%q want %q=%q", i, got[i].Key, got[i].Val, want[i].Key, want[i].Val)
		}
	}
}

func TestFaninCacheSmallLimitPages(t *testing.T) {
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
			// 30 segments x 20 keys = 600 rows.
			for f := 0; f < 30; f++ {
				for j := 0; j < 20; j++ {
					i := f*20 + j
					if err := sdb.Put(fmt.Sprintf("p:%06d", i), []byte(fmt.Sprintf("v:%d", i))); err != nil {
						t.Fatalf("Put %d: %v", i, err)
					}
				}
				if err := sdb.Flush(); err != nil {
					t.Fatalf("Flush %d: %v", f, err)
				}
			}
			snap, err := sdb.SnapshotSeq()
			if err != nil {
				t.Fatalf("SnapshotSeq: %v", err)
			}
			full, err := sdb.ScanFanin("", snap)
			if err != nil {
				t.Fatalf("ScanFanin: %v", err)
			}
			if len(full) != 600 {
				t.Fatalf("full rows = %d, want 600", len(full))
			}
			rows, srcDeltas, seekDeltas, durs := pageCached(t, sdb, "", snap, 10)
			checkRowsEqual(t, rows, full)
			if len(srcDeltas) != 60 {
				t.Fatalf("pages = %d, want 60 (600 rows / limit 10)", len(srcDeltas))
			}
			if srcDeltas[0] != 30 {
				t.Fatalf("page 1 sources delta = %d, want 30 (one discovery of every segment)", srcDeltas[0])
			}
			for p := 1; p < len(srcDeltas); p++ {
				if srcDeltas[p] != 0 {
					t.Fatalf("page %d sources delta = %d, want 0 (cache hit)", p+1, srcDeltas[p])
				}
			}
			var seekLater uint64
			for p := 1; p < len(seekDeltas); p++ {
				seekLater += seekDeltas[p]
			}
			// Timing is logged, not asserted (wasm timing is
			// noisy); the exact counter proof is the sources
			// delta above. The seeks deltas are logged to show
			// positioning still happens once per source per
			// window — what the cache removes is the discovery
			// walk behind each seek, not the seek call itself.
			var laterDur time.Duration
			for p := 1; p < len(durs); p++ {
				laterDur += durs[p]
			}
			t.Logf("cached paging: page1 sources=%d seeks=%d dur=%s; later pages sources=0 seeks total=%d mean dur=%s",
				srcDeltas[0], seekDeltas[0], durs[0].Round(time.Microsecond),
				seekLater, (laterDur / time.Duration(len(durs)-1)).Round(time.Microsecond))
			// A cursorless restart discovers fresh again (the
			// new scan replaces the slot, it does not reuse a
			// stale one).
			if err := sdb.FaninStatsReset(); err != nil {
				t.Fatalf("FaninStatsReset: %v", err)
			}
			if _, _, _, err := sdb.ScanFaninPage("", snap, nil, false, 10); err != nil {
				t.Fatalf("restart page: %v", err)
			}
			sources, _, _, _, err := sdb.FaninStats()
			if err != nil {
				t.Fatalf("FaninStats: %v", err)
			}
			if sources != 30 {
				t.Fatalf("restart page sources = %d, want 30 (fresh discovery)", sources)
			}
		})
	}
}

func TestFaninCacheFlushBetweenPages(t *testing.T) {
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
			// Base: 20 segments x 20 keys = 400 rows.
			for f := 0; f < 20; f++ {
				for j := 0; j < 20; j++ {
					i := f*20 + j
					if err := sdb.Put(fmt.Sprintf("q:%06d", i), []byte(fmt.Sprintf("v:%d", i))); err != nil {
						t.Fatalf("Put %d: %v", i, err)
					}
				}
				if err := sdb.Flush(); err != nil {
					t.Fatalf("Flush %d: %v", f, err)
				}
			}
			// Hot keys, still unflushed when the snapshot is taken.
			for i := 400; i < 420; i++ {
				if err := sdb.Put(fmt.Sprintf("q:%06d", i), []byte(fmt.Sprintf("v:%d", i))); err != nil {
					t.Fatalf("Put hot %d: %v", i, err)
				}
			}
			snap, err := sdb.SnapshotSeq()
			if err != nil {
				t.Fatalf("SnapshotSeq: %v", err)
			}
			full, err := sdb.ScanFanin("", snap)
			if err != nil {
				t.Fatalf("ScanFanin: %v", err)
			}
			if len(full) != 420 {
				t.Fatalf("full rows = %d, want 420", len(full))
			}
			// Page with two flushes interleaved: after page 1
			// the hot keys are flushed (hot -> segment, same
			// effective sequences); after page 2 brand-new keys
			// (sequences above the snapshot) are put + flushed.
			// Both are flush-appends, so both continuations must
			// succeed transparently and the rows must stay
			// exactly the old snapshot's rows.
			var rows []SegDBScanEntry
			var cursor []byte
			hasCursor := false
			var srcDeltas []uint64
			pageNo := 0
			for {
				if err := sdb.FaninStatsReset(); err != nil {
					t.Fatalf("FaninStatsReset: %v", err)
				}
				page, nextKey, hasMore, err := sdb.ScanFaninPage("", snap, cursor, hasCursor, 25)
				if err != nil {
					t.Fatalf("page %d: %v", pageNo+1, err)
				}
				sources, _, _, _, err := sdb.FaninStats()
				if err != nil {
					t.Fatalf("FaninStats: %v", err)
				}
				srcDeltas = append(srcDeltas, sources)
				rows = append(rows, page...)
				pageNo++
				if pageNo == 1 {
					if err := sdb.Flush(); err != nil {
						t.Fatalf("Flush hot: %v", err)
					}
				}
				if pageNo == 2 {
					for i := 420; i < 440; i++ {
						if err := sdb.Put(fmt.Sprintf("q:%06d", i), []byte(fmt.Sprintf("n:%d", i))); err != nil {
							t.Fatalf("Put new %d: %v", i, err)
						}
					}
					if err := sdb.Flush(); err != nil {
						t.Fatalf("Flush new: %v", err)
					}
				}
				if !hasMore {
					break
				}
				if len(page) == 0 {
					t.Fatalf("page %d made no progress", pageNo)
				}
				cursor, hasCursor = nextKey, true
			}
			checkRowsEqual(t, rows, full)
			// Page 1 discovers 20 segments; the page after each
			// flush re-discovers once (21, then 22 segments);
			// every other page is a cache hit (0).
			if len(srcDeltas) < 3 || srcDeltas[0] != 20 {
				t.Fatalf("page 1 sources = %v, want first delta 20", srcDeltas)
			}
			if srcDeltas[1] != 21 {
				t.Fatalf("page after hot flush sources = %d, want 21 (transparent rediscovery)", srcDeltas[1])
			}
			if srcDeltas[2] != 22 {
				t.Fatalf("page after new-keys flush sources = %d, want 22 (transparent rediscovery)", srcDeltas[2])
			}
			for p := 3; p < len(srcDeltas); p++ {
				if srcDeltas[p] != 0 {
					t.Fatalf("page %d sources = %d, want 0 (cache hit)", p+1, srcDeltas[p])
				}
			}
			t.Logf("flush paging: pages=%d sources deltas=%v", len(srcDeltas), srcDeltas)
		})
	}
}

func TestFaninCacheCompactBetweenPages(t *testing.T) {
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
			// Old snapshot: 100 keys flushed, snapshot taken,
			// then 50 of them overwritten + flushed. Paging at
			// the old snapshot, then compacting (which merges
			// the two segments under new names and drops the
			// v1 versions), must fail the continuation with
			// the typed stale error — never mixed rows.
			for i := 0; i < 100; i++ {
				if err := sdb.Put(fmt.Sprintf("r:%06d", i), []byte(fmt.Sprintf("v:%d", i))); err != nil {
					t.Fatalf("Put %d: %v", i, err)
				}
			}
			if err := sdb.Flush(); err != nil {
				t.Fatalf("Flush base: %v", err)
			}
			snapOld, err := sdb.SnapshotSeq()
			if err != nil {
				t.Fatalf("SnapshotSeq old: %v", err)
			}
			for i := 0; i < 50; i++ {
				if err := sdb.Put(fmt.Sprintf("r:%06d", i), []byte(fmt.Sprintf("w:%d", i))); err != nil {
					t.Fatalf("Put overwrite %d: %v", i, err)
				}
			}
			if err := sdb.Flush(); err != nil {
				t.Fatalf("Flush overwrite: %v", err)
			}
			page1, nextKey, hasMore, err := sdb.ScanFaninPage("", snapOld, nil, false, 25)
			if err != nil {
				t.Fatalf("old-snapshot page 1: %v", err)
			}
			if !hasMore || len(page1) != 25 || string(page1[0].Key) != "r:000000" || string(page1[0].Val) != "v:0" {
				t.Fatalf("old-snapshot page 1: hasMore=%v rows=%d first=%q=%q", hasMore, len(page1), page1[0].Key, page1[0].Val)
			}
			if err := sdb.Compact(0); err != nil {
				t.Fatalf("Compact: %v", err)
			}
			page2, _, _, err := sdb.ScanFaninPage("", snapOld, nextKey, true, 25)
			if err != ErrSegDBFaninStale {
				t.Fatalf("page after compact: err %v, want ErrSegDBFaninStale", err)
			}
			if len(page2) != 0 {
				t.Fatalf("page after compact returned %d rows with a stale error, want 0", len(page2))
			}
			// The stale state is session-scoped: a fresh
			// cursorless scan at the same snapshot succeeds
			// (over the merged segments, the overwritten keys'
			// v1 versions are gone, so the first row is now
			// r:000050 — the compact genuinely changed what an
			// old snapshot can see, which is exactly why the
			// continuation refused to mix rows).
			fresh, _, _, err := sdb.ScanFaninPage("", snapOld, nil, false, 25)
			if err != nil {
				t.Fatalf("fresh scan after compact: %v", err)
			}
			if len(fresh) == 0 || string(fresh[0].Key) != "r:000050" {
				t.Fatalf("fresh scan first row = %q, want r:000050", fresh[0].Key)
			}
			// Current snapshot, two segments, compact between
			// pages: the strict rule fails this too (compact
			// always replaces the source set; only flush-append
			// is transparent), even though the current view
			// would survive the merge.
			db2, err := NewYowzaDB(wasmBytes)
			if err != nil {
				t.Fatalf("NewYowzaDB 2: %v", err)
			}
			defer db2.Close()
			sdb2, err := NewSegDB(db2)
			if err != nil {
				t.Fatalf("NewSegDB 2: %v", err)
			}
			for f := 0; f < 2; f++ {
				for j := 0; j < 50; j++ {
					i := f*50 + j
					if err := sdb2.Put(fmt.Sprintf("s:%06d", i), []byte(fmt.Sprintf("v:%d", i))); err != nil {
						t.Fatalf("Put2 %d: %v", i, err)
					}
				}
				if err := sdb2.Flush(); err != nil {
					t.Fatalf("Flush2 %d: %v", f, err)
				}
			}
			snapCur, err := sdb2.SnapshotSeq()
			if err != nil {
				t.Fatalf("SnapshotSeq cur: %v", err)
			}
			_, nextKey2, hasMore2, err := sdb2.ScanFaninPage("", snapCur, nil, false, 25)
			if err != nil || !hasMore2 {
				t.Fatalf("current-snapshot page 1: err %v hasMore %v", err, hasMore2)
			}
			if err := sdb2.Compact(0); err != nil {
				t.Fatalf("Compact 2: %v", err)
			}
			if _, _, _, err := sdb2.ScanFaninPage("", snapCur, nextKey2, true, 25); err != ErrSegDBFaninStale {
				t.Fatalf("current-snapshot page after compact: err %v, want ErrSegDBFaninStale", err)
			}
		})
	}
}

// TestFaninCachePartialPage covers the -3 partial-page path
// through the cache: with limit at the cache boundary (4096)
// and ~3000 small rows, the first page fills the 57 KiB
// bindings out buffer before the limit and returns -3 with
// hasMore; the resume page must then be a cache hit (sources
// delta 0) and the concatenated rows must equal the full scan.
// This proves the copy-out on -3 persists a resumable session.
func TestFaninCachePartialPage(t *testing.T) {
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
			for f := 0; f < 30; f++ {
				for j := 0; j < 100; j++ {
					i := f*100 + j
					if err := sdb.Put(fmt.Sprintf("t:%06d", i), []byte(fmt.Sprintf("v:%d", i))); err != nil {
						t.Fatalf("Put %d: %v", i, err)
					}
				}
				if err := sdb.Flush(); err != nil {
					t.Fatalf("Flush %d: %v", f, err)
				}
			}
			snap, err := sdb.SnapshotSeq()
			if err != nil {
				t.Fatalf("SnapshotSeq: %v", err)
			}
			full, err := sdb.ScanFanin("", snap)
			if err != nil {
				t.Fatalf("ScanFanin: %v", err)
			}
			if len(full) != 3000 {
				t.Fatalf("full rows = %d, want 3000", len(full))
			}
			rows, srcDeltas, _, _ := pageCached(t, sdb, "", snap, 4096)
			checkRowsEqual(t, rows, full)
			if len(srcDeltas) != 2 {
				t.Fatalf("pages = %d, want 2 (one -3 partial page + resume)", len(srcDeltas))
			}
			if srcDeltas[0] != 30 || srcDeltas[1] != 0 {
				t.Fatalf("sources deltas = %v, want [30 0] (partial page discovers, resume is a cache hit)", srcDeltas)
			}
		})
	}
}

func TestFaninCachePagingBenchmark(t *testing.T) {
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
			// 20 segments x 500 keys = 10,000 rows; discovery
			// walks every segment's blocks, so re-discovery
			// per page dominates the stateless pattern.
			for f := 0; f < 20; f++ {
				for j := 0; j < 500; j++ {
					i := f*500 + j
					if err := sdb.Put(fmt.Sprintf("b:%06d", i), []byte(fmt.Sprintf("v:%d", i))); err != nil {
						t.Fatalf("Put %d: %v", i, err)
					}
				}
				if err := sdb.Flush(); err != nil {
					t.Fatalf("Flush %d: %v", f, err)
				}
			}
			snap, err := sdb.SnapshotSeq()
			if err != nil {
				t.Fatalf("SnapshotSeq: %v", err)
			}
			full, err := sdb.ScanFanin("", snap)
			if err != nil {
				t.Fatalf("ScanFanin: %v", err)
			}
			if len(full) != 10000 {
				t.Fatalf("full rows = %d, want 10000", len(full))
			}
			// Mode A: cached paging, limit 100 -> 100 pages.
			rowsA, srcA, _, dursA := pageCached(t, sdb, "", snap, 100)
			checkRowsEqual(t, rowsA, full)
			var totalSrcA uint64
			for _, d := range srcA {
				totalSrcA += d
			}
			var totalDurA time.Duration
			for _, d := range dursA {
				totalDurA += d
			}
			if totalSrcA != 20 {
				t.Fatalf("cached paging sources total = %d, want 20 (one discovery)", totalSrcA)
			}
			// Mode B: stateless-equivalent paging — between
			// "" pages, a cursorless page with a disjoint
			// prefix ("\xFF", beyond every key) evicts the
			// session, so every "" continuation page pays a
			// fresh discovery. Only "" page time is counted.
			var rowsB []SegDBScanEntry
			var totalSrcB uint64
			var totalDurB time.Duration
			var cursor []byte
			hasCursor := false
			for {
				if err := sdb.FaninStatsReset(); err != nil {
					t.Fatalf("FaninStatsReset B: %v", err)
				}
				start := time.Now()
				page, nextKey, hasMore, err := sdb.ScanFaninPage("", snap, cursor, hasCursor, 100)
				totalDurB += time.Since(start)
				if err != nil {
					t.Fatalf("stateless-equivalent page: %v", err)
				}
				sources, _, _, _, err := sdb.FaninStats()
				if err != nil {
					t.Fatalf("FaninStats B: %v", err)
				}
				totalSrcB += sources
				rowsB = append(rowsB, page...)
				if !hasMore {
					break
				}
				if len(page) == 0 {
					t.Fatalf("stateless-equivalent page made no progress")
				}
				cursor, hasCursor = nextKey, true
				// Evict the "" session with a disjoint prefix.
				if _, _, _, err := sdb.ScanFaninPage("\xFF", snap, nil, false, 100); err != nil {
					t.Fatalf("eviction page: %v", err)
				}
			}
			checkRowsEqual(t, rowsB, full)
			if totalSrcB != 20*100 {
				t.Fatalf("stateless-equivalent paging sources total = %d, want %d (20 per page x 100 pages)", totalSrcB, 20*100)
			}
			t.Logf("paging benchmark (%s): cached %d pages sources=%d time=%s (%.1f us/page); stateless-equivalent sources=%d time=%s (%.1f us/page)",
				bin, len(srcA), totalSrcA, totalDurA.Round(time.Millisecond),
				float64(totalDurA.Microseconds())/float64(len(srcA)),
				totalSrcB, totalDurB.Round(time.Millisecond),
				float64(totalDurB.Microseconds())/100.0)
		})
	}
}

// ---------------- direct-export corruption tests ----------------

// cacheSegSlot finds the first heap slot whose object name starts
// with "seg." and returns its object offset/length within the
// heap buffer. Heap slot layout (src/yowza_segdb.c): 32-byte
// header, then 4096 slots x 152 bytes: name[128] | offset u64 |
// length u64 | capacity u64.
func cacheSegSlot(t *testing.T, h *faninHarness) (uint64, uint64) {
	t.Helper()
	heapBytes, ok := h.memRead(h.heap, h.heapMB<<20)
	if !ok {
		t.Fatalf("read heap bytes")
	}
	for i := 0; i < 4096; i++ {
		s := heapBytes[32+i*152:]
		if s[0] == 0 {
			continue
		}
		end := 0
		for end < 128 && s[end] != 0 {
			end++
		}
		name := string(s[:end])
		if len(name) >= 4 && name[:4] == "seg." {
			off := binary.LittleEndian.Uint64(s[128:136])
			ln := binary.LittleEndian.Uint64(s[136:144])
			return off, ln
		}
	}
	t.Fatalf("no seg.* heap slot found")
	return 0, 0
}

func cacheFlipByte(t *testing.T, h *faninHarness, absOff uint64) {
	t.Helper()
	raw, ok := h.memRead(absOff, 1)
	if !ok || len(raw) != 1 {
		t.Fatalf("read byte at %d", absOff)
	}
	if !h.memWrite(absOff, []byte{raw[0] ^ 0xFF}) {
		t.Fatalf("write flipped byte at %d", absOff)
	}
}

func TestFaninCacheCorruptSegment(t *testing.T) {
	for _, bin := range []string{"build/yowza.wasm", "build/yowza64.wasm"} {
		t.Run(bin+"/block", func(t *testing.T) {
			h := newFaninHarness(t, bin, 16)
			// One segment, 500 keys ("c:%06d", 8 bytes).
			for i := 0; i < 500; i++ {
				if err := h.put(fmt.Sprintf("c:%06d", i), fmt.Sprintf("v:%d", i)); err != nil {
					t.Fatalf("put %d: %v", i, err)
				}
			}
			if err := h.flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}
			snap, err := h.snapshot()
			if err != nil {
				t.Fatalf("snapshot: %v", err)
			}
			page1, nextKey, hasMore, err := h.faninPage("", snap, nil, false, 10)
			if err != nil || len(page1) != 10 || !hasMore {
				t.Fatalf("page1: rows=%d hasMore=%v err=%v", len(page1), hasMore, err)
			}
			page2, nextKey2, hasMore2, err := h.faninPage("", snap, nextKey, true, 10)
			if err != nil || len(page2) != 10 || !hasMore2 {
				t.Fatalf("page2 (control): rows=%d hasMore=%v err=%v", len(page2), hasMore2, err)
			}
			// Corrupt one byte inside the first data block's
			// entry bytes: header is 64 + first key (8) + last
			// key (8) = 80 bytes; +50 lands inside entries.
			// Page 3 streams that block (rows 21..30 live in
			// it) and its block checksum must fail.
			off, _ := cacheSegSlot(t, h)
			magic, ok := h.memRead(h.heap+off, 6)
			if !ok || string(magic) != "YWSEG1" {
				t.Fatalf("segment magic = %q, want YWSEG1 (heap layout assumption broken)", magic)
			}
			cacheFlipByte(t, h, h.heap+off+80+50)
			page3, _, _, err := h.faninPage("", snap, nextKey2, true, 10)
			if err == nil {
				t.Fatalf("page3 after block corruption succeeded with %d rows, want an error (never cached rows)", len(page3))
			}
			if len(page3) != 0 {
				t.Fatalf("page3 after block corruption returned %d rows with an error, want 0", len(page3))
			}
			t.Logf("page3 after block corruption: err=%v (typed scan failure, no cached rows)", err)
		})
		t.Run(bin+"/header", func(t *testing.T) {
			h := newFaninHarness(t, bin, 16)
			for i := 0; i < 500; i++ {
				if err := h.put(fmt.Sprintf("c:%06d", i), fmt.Sprintf("v:%d", i)); err != nil {
					t.Fatalf("put %d: %v", i, err)
				}
			}
			if err := h.flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}
			snap, err := h.snapshot()
			if err != nil {
				t.Fatalf("snapshot: %v", err)
			}
			page1, nextKey, hasMore, err := h.faninPage("", snap, nil, false, 10)
			if err != nil || len(page1) != 10 || !hasMore {
				t.Fatalf("page1: rows=%d hasMore=%v err=%v", len(page1), hasMore, err)
			}
			// Corrupt a header byte: the cache-hit seg_open
			// header-checksum verification must fail the page.
			off, _ := cacheSegSlot(t, h)
			cacheFlipByte(t, h, h.heap+off+10)
			page2, _, _, err := h.faninPage("", snap, nextKey, true, 10)
			if err == nil {
				t.Fatalf("page2 after header corruption succeeded with %d rows, want an error", len(page2))
			}
			if len(page2) != 0 {
				t.Fatalf("page2 after header corruption returned %d rows with an error, want 0", len(page2))
			}
			t.Logf("page2 after header corruption: err=%v (typed scan failure, no cached rows)", err)
		})
	}
}
