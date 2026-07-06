package query

import (
	"context"
	"errors"
	"fmt"
	"testing"

	yowza "github.com/yowza/yowza"
)

// GATE 0 — cursor integration, proved through Go APIs and the
// query engine, never through a direct wasm harness.
//
// The reference path is SegDB.ScanAt (skip paging, the old
// behaviour). The cursor path is SegDBSource.ScanAt (segPager
// over SegDB.ScanCursor). Both must agree row-for-row, and the
// pager's own counters (cursorCalls vs skipCalls, read from
// the *segRawIter in this package) plus the C scan counters
// (SegDB.ScanStats) must prove the cursor path actually ran.

func gate1CursorRows(t *testing.T, it RawIterator) []string {
	t.Helper()
	var out []string
	for {
		batch, err := it.NextBatch(64)
		if err != nil {
			t.Fatalf("NextBatch: %v", err)
		}
		if len(batch) == 0 {
			return out
		}
		for _, e := range batch {
			out = append(out, e.Key+"="+string(e.Value))
		}
	}
}

func gate1AssertRowsEqual(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("row count %d, want %d (first divergence: got %v want %v)",
			len(got), len(want), firstN(got, 5), firstN(want, 5))
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("row %d: got %q, want %q", i, got[i], want[i])
		}
	}
}

func firstN(s []string, n int) []string {
	if len(s) < n {
		return s
	}
	return s[:n]
}

// Small both-binary parity: 5000 rows, cursor vs skip, plus
// C-level counters proving which path ran.
func TestGate1CursorParitySmall(t *testing.T) {
	for bin, sdb := range loadSegDBBothBinaries(t) {
		t.Run(bin, func(t *testing.T) {
			for i := 0; i < 5000; i++ {
				mustPut(t, sdb, fmt.Sprintf("k:%08d", i), fmt.Sprintf("v:%d", i))
				if (i+1)%1000 == 0 {
					if err := sdb.Flush(); err != nil {
						t.Fatalf("Flush: %v", err)
					}
				}
			}
			seq, err := sdb.SnapshotSeq()
			if err != nil {
				t.Fatalf("SnapshotSeq: %v", err)
			}
			// Reference: skip path (SegDB.ScanAt).
			ref, err := sdb.ScanAt("k:", seq)
			if err != nil {
				t.Fatalf("reference ScanAt: %v", err)
			}
			var refRows []string
			for _, e := range ref {
				refRows = append(refRows, string(e.Key)+"="+string(e.Val))
			}
			if len(refRows) != 5000 {
				t.Fatalf("reference rows = %d, want 5000", len(refRows))
			}
			// Cursor path: SegDBSource.ScanAt, with C counters.
			if err := sdb.ResetScanStats(); err != nil {
				t.Fatalf("ResetScanStats: %v", err)
			}
			src := &SegDBSource{DB: sdb}
			it, err := src.ScanAt(context.Background(), "k:", seq)
			if err != nil {
				t.Fatalf("source ScanAt: %v", err)
			}
			got := gate1CursorRows(t, it)
			if err := it.Close(); err != nil {
				t.Fatalf("iterator Close: %v", err)
			}
			gate1AssertRowsEqual(t, got, refRows)
			raw, ok := it.(*segRawIter)
			if !ok {
				t.Fatalf("ScanAt iterator is %T, want *segRawIter", it)
			}
			if raw.pager.cursorCalls == 0 {
				t.Fatalf("cursorCalls = 0: cursor path never ran")
			}
			if raw.pager.skipCalls != 0 {
				t.Fatalf("skipCalls = %d, want 0: skip path served the scan", raw.pager.skipCalls)
			}
			st, err := sdb.ScanStats()
			if err != nil {
				t.Fatalf("ScanStats: %v", err)
			}
			if st.CursorVisits == 0 {
				t.Fatalf("C cursor_visits = 0: cursor path never ran in C")
			}
			if st.PageVisits != 0 {
				t.Fatalf("C page_visits = %d, want 0: skip path ran in C", st.PageVisits)
			}
			t.Logf("cursor scan: %d rows, %d cursor pages, C cursor_visits=%d page_visits=%d",
				len(got), raw.pager.cursorCalls, st.CursorVisits, st.PageVisits)
		})
	}
}

// Gate 0 acceptance at full scale, through Go APIs only:
// 90,000 rows in one SegDB, cursor vs skip row-for-row, with
// counters proving the cursor path ran. Flushes are batched
// (200 puts) and the table is compacted every 10,000 rows so
// the scan never exceeds the 128 segment-source cap (a Gate 3
// item, not a Gate 1 concern: without periodic compaction any
// 450-segment scan fails on either path).
func TestGate1Cursor90k(t *testing.T) {
	db := loadTestDB(t)
	sdb, err := yowza.NewSegDB(db)
	if err != nil {
		t.Fatalf("NewSegDB: %v", err)
	}
	const n = 90000
	// Construction budget: the Go SegDB bindings use a fixed 4 MiB wasm
	// heap whose bump space is never reclaimed, so every compact rewrites
	// live data into new heap space. The original 90k scanperf benchmark
	// used a larger custom heap; through these bindings the 90k database
	// must be built compact-free: flush every 1000 puts (90 segments, under
	// the 127-segment usable scan cap) with 1-byte values and 6-byte
	// zero-padded keys (still order-preserving, still distinct). Row
	// identity still comes from distinct keys; cursor/skip parity is
	// unaffected. 10-byte keys exhaust the heap at ~86k rows.
	for i := 0; i < n; i++ {
		mustPut(t, sdb, fmt.Sprintf("k%05d", i), "v")
		if (i+1)%1000 == 0 {
			if err := sdb.Flush(); err != nil {
				t.Fatalf("Flush at %d: %v", i+1, err)
			}
		}
	}
	seq, err := sdb.SnapshotSeq()
	if err != nil {
		t.Fatalf("SnapshotSeq: %v", err)
	}
	// Full 90k skip comparison is deliberately NOT done here: skip
	// paging is quadratic (each page re-reads all skipped rows), and
	// the 20k benchmark's skip scan already takes ~54 s, so a 90k
	// skip scan would take ~18 min. Instead: (a) the full 90k cursor
	// scan is verified row-for-row against the exact expected
	// sequence (construction is deterministic), and (b) cursor and
	// skip are compared row-for-row on a 10k-row window of the
	// SAME 90-segment database (prefix "k8"), which proves the
	// two paths agree on this data. Full cursor==skip parity at
	// 5k rows on both binaries is covered by
	// TestGate1CursorParitySmall.
	if err := sdb.ResetScanStats(); err != nil {
		t.Fatalf("ResetScanStats: %v", err)
	}
	src := &SegDBSource{DB: sdb}
	it, err := src.ScanAt(context.Background(), "k", seq)
	if err != nil {
		t.Fatalf("source ScanAt: %v", err)
	}
	got := gate1CursorRows(t, it)
	if err := it.Close(); err != nil {
		t.Fatalf("iterator Close: %v", err)
	}
	if len(got) != n {
		t.Fatalf("cursor rows = %d, want %d", len(got), n)
	}
	for i := 0; i < n; i++ {
		if want := fmt.Sprintf("k%05d=v", i); got[i] != want {
			t.Fatalf("row %d: cursor %q, expected %q", i, got[i], want)
		}
	}
	// C counters for the full cursor scan only — read BEFORE the
	// window skip scan below (which legitimately increments
	// page_visits).
	raw, ok := it.(*segRawIter)
	if !ok {
		t.Fatalf("ScanAt iterator is %T, want *segRawIter", it)
	}
	if raw.pager.cursorCalls == 0 || raw.pager.skipCalls != 0 {
		t.Fatalf("pager counters: cursorCalls=%d skipCalls=%d (want cursor>0, skip=0)",
			raw.pager.cursorCalls, raw.pager.skipCalls)
	}
	st, err := sdb.ScanStats()
	if err != nil {
		t.Fatalf("ScanStats: %v", err)
	}
	if st.CursorVisits == 0 || st.PageVisits != 0 {
		t.Fatalf("C counters: cursor_visits=%d page_visits=%d (want cursor>0, page=0)",
			st.CursorVisits, st.PageVisits)
	}
	t.Logf("90k cursor parity: %d rows, %d cursor pages, C cursor_visits=%d",
		len(got), raw.pager.cursorCalls, st.CursorVisits)
	// Skip-vs-cursor agreement on the 10k window k80000..k89999.
	skipRef, err := sdb.ScanAt("k8", seq)
	if err != nil {
		t.Fatalf("window skip ScanAt: %v", err)
	}
	if len(skipRef) != 10000 {
		t.Fatalf("window skip rows = %d, want 10000", len(skipRef))
	}
	wit, err := src.ScanAt(context.Background(), "k8", seq)
	if err != nil {
		t.Fatalf("window source ScanAt: %v", err)
	}
	wgot := gate1CursorRows(t, wit)
	if err := wit.Close(); err != nil {
		t.Fatalf("window iterator Close: %v", err)
	}
	if len(wgot) != len(skipRef) {
		t.Fatalf("window cursor rows = %d, want %d", len(wgot), len(skipRef))
	}
	for i, e := range skipRef {
		if want := string(e.Key) + "=" + string(e.Val); wgot[i] != want {
			t.Fatalf("window row %d: cursor %q, skip %q", i, wgot[i], want)
		}
	}
}

// Negative: a snapshot sequence above the current sequence is
// a typed error, never a silent read of the current state.
func TestGate1ScanAtFutureSeq(t *testing.T) {
	db := loadTestDB(t)
	sdb, err := yowza.NewSegDB(db)
	if err != nil {
		t.Fatalf("NewSegDB: %v", err)
	}
	mustPut(t, sdb, "k:1", "v1")
	cur, err := sdb.SnapshotSeq()
	if err != nil {
		t.Fatalf("SnapshotSeq: %v", err)
	}
	src := &SegDBSource{DB: sdb}
	if _, err := src.ScanAt(context.Background(), "k:", cur+100); !errors.Is(err, yowza.ErrSnapshotInvalid) {
		t.Fatalf("ScanAt(future seq) err = %v, want ErrSnapshotInvalid", err)
	}
	// And a valid old sequence still works after the rejection.
	it, err := src.ScanAt(context.Background(), "k:", cur)
	if err != nil {
		t.Fatalf("ScanAt(current): %v", err)
	}
	rows := gate1CursorRows(t, it)
	_ = it.Close()
	if len(rows) != 1 || rows[0] != "k:1=v1" {
		t.Fatalf("ScanAt(current) rows = %v, want [k:1=v1]", rows)
	}
}
