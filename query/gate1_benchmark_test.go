package query

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	yowza "github.com/yowza/yowza"
)

// GATE 1 — real Go-source benchmark: cursor scan vs skip scan
// over the same SegDB, through Go APIs only (no wasm harness).
// The plain test below runs one scan of each and logs the
// timings; the Benchmark functions below run under -bench.

func gate1BenchDB(b *testing.B) (*yowza.SegDB, uint64) {
	b.Helper()
	wasmBytes, err := os.ReadFile("../build/yowza.wasm")
	if err != nil {
		b.Skipf("built wasm binary absent (run make all first): %v", err)
	}
	db, err := yowza.NewYowzaDB(wasmBytes)
	if err != nil {
		b.Fatalf("NewYowzaDB: %v", err)
	}
	b.Cleanup(func() { db.Close() })
	sdb, err := yowza.NewSegDB(db)
	if err != nil {
		b.Fatalf("NewSegDB: %v", err)
	}
	const n = 20000
	for i := 0; i < n; i++ {
		if err := sdb.Put(fmt.Sprintf("k:%08d", i), []byte(fmt.Sprintf("v:%d", i))); err != nil {
			b.Fatalf("Put %d: %v", i, err)
		}
		if (i+1)%1000 == 0 {
			if err := sdb.Flush(); err != nil {
				b.Fatalf("Flush at %d: %v", i+1, err)
			}
		}
	}
	seq, err := sdb.SnapshotSeq()
	if err != nil {
		b.Fatalf("SnapshotSeq: %v", err)
	}
	return sdb, seq
}

func gate1ConsumeCursor(b *testing.B, sdb *yowza.SegDB, seq uint64) int {
	b.Helper()
	src := &SegDBSource{DB: sdb}
	it, err := src.ScanAt(context.Background(), "k:", seq)
	if err != nil {
		b.Fatalf("source ScanAt: %v", err)
	}
	defer it.Close()
	n := 0
	for {
		batch, err := it.NextBatch(64)
		if err != nil {
			b.Fatalf("NextBatch: %v", err)
		}
		if len(batch) == 0 {
			return n
		}
		n += len(batch)
	}
}

func gate1ConsumeSkip(b *testing.B, sdb *yowza.SegDB, seq uint64) int {
	b.Helper()
	ents, err := sdb.ScanAt("k:", seq)
	if err != nil {
		b.Fatalf("ScanAt: %v", err)
	}
	return len(ents)
}

func BenchmarkGate1GoSourceCursor(b *testing.B) {
	sdb, seq := gate1BenchDB(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if n := gate1ConsumeCursor(b, sdb, seq); n != 20000 {
			b.Fatalf("cursor rows = %d, want 20000", n)
		}
	}
}

func BenchmarkGate1GoSourceSkip(b *testing.B) {
	sdb, seq := gate1BenchDB(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if n := gate1ConsumeSkip(b, sdb, seq); n != 20000 {
			b.Fatalf("skip rows = %d, want 20000", n)
		}
	}
}

// Plain-test form of the benchmark (runs under go test
// without -bench): one timed cursor scan and one timed skip
// scan over 20,000 rows through the Go source, row counts
// asserted, timings logged.
func TestGate1GoSourceBenchmark(t *testing.T) {
	// Reuse the benchmark builder via a testing.B shim: the
	// builder only needs Helper/Skipf/Fatalf/Cleanup, which
	// testing.T does not provide for Cleanup — so build here
	// directly with the same shape (20,000 rows, flush/1000).
	wasmBytes, err := os.ReadFile("../build/yowza.wasm")
	if err != nil {
		t.Skipf("built wasm binary absent (run make all first): %v", err)
	}
	db, err := yowza.NewYowzaDB(wasmBytes)
	if err != nil {
		t.Fatalf("NewYowzaDB: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	sdb, err := yowza.NewSegDB(db)
	if err != nil {
		t.Fatalf("NewSegDB: %v", err)
	}
	const n = 20000
	for i := 0; i < n; i++ {
		mustPut(t, sdb, fmt.Sprintf("k:%08d", i), fmt.Sprintf("v:%d", i))
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
	start := time.Now()
	src := &SegDBSource{DB: sdb}
	it, err := src.ScanAt(context.Background(), "k:", seq)
	if err != nil {
		t.Fatalf("source ScanAt: %v", err)
	}
	cn := 0
	for {
		batch, err := it.NextBatch(64)
		if err != nil {
			t.Fatalf("cursor NextBatch: %v", err)
		}
		if len(batch) == 0 {
			break
		}
		cn += len(batch)
	}
	_ = it.Close()
	cursorDur := time.Since(start)
	start = time.Now()
	ents, err := sdb.ScanAt("k:", seq)
	if err != nil {
		t.Fatalf("skip ScanAt: %v", err)
	}
	skipDur := time.Since(start)
	if cn != n || len(ents) != n {
		t.Fatalf("rows: cursor=%d skip=%d, want %d", cn, len(ents), n)
	}
	t.Logf("Go-source 20k scan: cursor %v (%d rows), skip %v (%d rows)",
		cursorDur, cn, skipDur, len(ents))
}
