package query

import (
	"context"
	"errors"
	"os"
	"testing"
)

func gate4Row(i int) Row {
	return Row{Cols: []string{"users.key", "users.name"}, Vals: []Value{String("user:" + string(rune('a'+i%26))), String("Name")}}
}

func TestGate4SpillQuotaAndCleanup(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	mgr, err := NewSpillManager(dir, 300, false)
	if err != nil {
		t.Fatalf("NewSpillManager: %v", err)
	}
	run, err := mgr.NewRun(ctx)
	if err != nil {
		t.Fatalf("NewRun: %v", err)
	}
	// Write until the quota refuses: each row is ~60 bytes
	// of JSON, so a 300-byte quota refuses within a few rows.
	wrote := 0
	var quotaErr error
	for i := 0; i < 100; i++ {
		if err := run.WriteRow(ctx, gate4Row(i)); err != nil {
			quotaErr = err
			break
		}
		wrote++
	}
	if quotaErr == nil {
		t.Fatalf("wrote 100 rows under a 300-byte quota without refusal")
	}
	if !errors.Is(quotaErr, ErrSpillQuotaExceeded) {
		t.Fatalf("quota error = %v, want ErrSpillQuotaExceeded", quotaErr)
	}
	if wrote == 0 {
		t.Fatalf("quota refused the very first row; quota accounting is wrong")
	}
	if err := run.Close(); err != nil {
		t.Fatalf("run Close: %v", err)
	}
	if n, err := spillDirEntries(dir); err != nil || n != 0 {
		t.Fatalf("spill files after run Close: %d (err %v), want 0", n, err)
	}
	if err := mgr.Close(); err != nil {
		t.Fatalf("manager Close: %v", err)
	}
	if err := mgr.Close(); err != nil {
		t.Fatalf("manager Close must be idempotent: %v", err)
	}
}

func TestGate4SpillPagedRead(t *testing.T) {
	ctx := context.Background()
	mgr, err := NewSpillManager("", 0, false)
	if err != nil {
		t.Fatalf("NewSpillManager: %v", err)
	}
	defer func() { _ = mgr.Close() }()
	run, err := mgr.NewRun(ctx)
	if err != nil {
		t.Fatalf("NewRun: %v", err)
	}
	const n = 150
	for i := 0; i < n; i++ {
		if err := run.WriteRow(ctx, Row{Cols: []string{"k"}, Vals: []Value{Int(int64(i))}}); err != nil {
			t.Fatalf("WriteRow %d: %v", i, err)
		}
	}
	if err := run.Finish(ctx); err != nil {
		t.Fatalf("Finish: %v", err)
	}
	rd, err := run.NewReader()
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer func() { _ = rd.Close() }()
	var got int
	var pages []int
	for {
		page, err := rd.NextPage(ctx, 64)
		if err != nil {
			t.Fatalf("NextPage: %v", err)
		}
		if len(page) == 0 {
			break
		}
		pages = append(pages, len(page))
		for j, r := range page {
			if r.Vals[0].I != int64(got+j) {
				t.Fatalf("row %d = %d, want in-order values", got+j, r.Vals[0].I)
			}
		}
		got += len(page)
	}
	if got != n {
		t.Fatalf("read %d rows, want %d", got, n)
	}
	if len(pages) != 3 || pages[0] != 64 || pages[1] != 64 || pages[2] != 22 {
		t.Fatalf("pages = %v, want [64 64 22]", pages)
	}
	// Writing after Finish is a typed error, not silent success.
	if err := run.WriteRow(ctx, gate4Row(0)); err == nil {
		t.Fatalf("WriteRow after Finish must fail")
	}
}

func TestGate4SpillCleanupOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	dir := t.TempDir()
	mgr, err := NewSpillManager(dir, 0, false)
	if err != nil {
		t.Fatalf("NewSpillManager: %v", err)
	}
	run, err := mgr.NewRun(ctx)
	if err != nil {
		t.Fatalf("NewSpillManager: %v", err)
	}
	if err := run.WriteRow(ctx, gate4Row(0)); err != nil {
		t.Fatalf("WriteRow: %v", err)
	}
	cancel()
	if err := run.WriteRow(ctx, gate4Row(1)); err == nil {
		t.Fatalf("WriteRow after cancel must fail")
	}
	if err := run.Close(); err != nil {
		t.Fatalf("run Close after cancel: %v", err)
	}
	if err := mgr.Close(); err != nil {
		t.Fatalf("manager Close after cancel: %v", err)
	}
	if n, err := spillDirEntries(dir); err != nil || n != 0 {
		t.Fatalf("spill files after cancelled query cleanup: %d (err %v), want 0", n, err)
	}
}

func TestGate4SpillSyncDurable(t *testing.T) {
	ctx := context.Background()
	mgr, err := NewSpillManager("", 0, true)
	if err != nil {
		t.Fatalf("NewSpillManager: %v", err)
	}
	defer func() { _ = mgr.Close() }()
	run, err := mgr.NewRun(ctx)
	if err != nil {
		t.Fatalf("NewRun: %v", err)
	}
	defer func() { _ = run.Close() }()
	if err := run.WriteRow(ctx, gate4Row(0)); err != nil {
		t.Fatalf("WriteRow: %v", err)
	}
	// Finish with sync=true must fsync without error.
	if err := run.Finish(ctx); err != nil {
		t.Fatalf("Finish (sync): %v", err)
	}
	if _, err := os.Stat(run.Path()); err != nil {
		t.Fatalf("spill file missing after Finish: %v", err)
	}
}
