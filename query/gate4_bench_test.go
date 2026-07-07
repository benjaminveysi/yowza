package query

// Gate 4 §5c benchmarks: every scale path side by side,
// with metrics per binary per path. Setup lives outside
// the timed loop (b.ResetTimer after the load); the heavy
// benches are meant to run with -benchtime=1x (see
// GATE4_NOTES.md for the recorded numbers).
//
// Paths:
//   - CCursor: direct C cursor (segdb_scan_cursor in wasm)
//     through SegDBSource.ScanAt, per binary.
//   - GoSource: the pure Go executor over MemorySource
//     (no wasm in the timed loop).
//   - ISAHostScan: scan-only ISA program through ISAHost,
//     per binary.
//   - MultiShardSort: coordinator global sort over 2 hosts
//     (in-memory) vs SpillSort (MaxSortRows=500, external
//     merge sort through host spill runs).
//   - Checkpoint: PersistedSegDB Put+Flush (checkpoint
//     publish) on a fresh temp dir per iteration, wasm32.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	yowza "github.com/yowza/yowza"
)

func gate4BenchWasm(b *testing.B, name string) []byte {
	b.Helper()
	wasmBytes, err := os.ReadFile(name)
	if err != nil {
		b.Skipf("wasm binary %s absent: %v", name, err)
	}
	return wasmBytes
}

func gate4BenchSegDB(b *testing.B, wasmBytes []byte, n int) (*yowza.SegDB, uint64) {
	b.Helper()
	db, err := yowza.NewYowzaDB(wasmBytes)
	if err != nil {
		b.Fatalf("NewYowzaDB: %v", err)
	}
	b.Cleanup(func() { db.Close() })
	sdb, err := yowza.NewSegDB(db)
	if err != nil {
		b.Fatalf("NewSegDB: %v", err)
	}
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

func gate4BenchConsumeCursor(b *testing.B, sdb *yowza.SegDB, seq uint64) int {
	b.Helper()
	src := &SegDBSource{DB: sdb}
	it, err := src.ScanAt(context.Background(), "k:", seq)
	if err != nil {
		b.Fatalf("ScanAt: %v", err)
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

func BenchmarkGate4CCursor32(b *testing.B) {
	sdb, seq := gate4BenchSegDB(b, gate4BenchWasm(b, "../build/yowza.wasm"), 20000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if n := gate4BenchConsumeCursor(b, sdb, seq); n != 20000 {
			b.Fatalf("cursor rows = %d, want 20000", n)
		}
	}
}

func BenchmarkGate4CCursor64(b *testing.B) {
	sdb, seq := gate4BenchSegDB(b, gate4BenchWasm(b, "../build/yowza64.wasm"), 20000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if n := gate4BenchConsumeCursor(b, sdb, seq); n != 20000 {
			b.Fatalf("cursor rows = %d, want 20000", n)
		}
	}
}

func BenchmarkGate4GoSource(b *testing.B) {
	schema := testSchema()
	binding := bindingForBench(schema)
	var rows []Row
	for i := 0; i < 20000; i++ {
		mr, err := MaterializeRow(binding, fmt.Sprintf("user:%05d", i),
			[]byte(fmt.Sprintf(`{"name":"U%05d","age":%d,"city":"C","active":true,"score":1.0}`, i, i%100)))
		if err != nil {
			b.Fatalf("MaterializeRow %d: %v", i, err)
		}
		rows = append(rows, mr)
	}
	plan, err := Compile("SELECT key, name FROM users", schema)
	if err != nil {
		b.Fatalf("compile: %v", err)
	}
	src := NewMemorySource(rows)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, _, err := Execute(context.Background(), plan, Contract{}, map[string]Source{"users": src})
		if err != nil {
			b.Fatalf("Execute: %v", err)
		}
		if len(out) != 20000 {
			b.Fatalf("rows = %d, want 20000", len(out))
		}
	}
}

func bindingForBench(schema *Schema) Binding {
	tbl, _ := schema.TableByName("users")
	return Binding{Table: tbl.Name, Alias: tbl.Name, Prefix: tbl.Prefix, Columns: tbl.Columns}
}

func gate4BenchISAHost(b *testing.B, wasmBytes []byte, n int) (*ISAHost, uint64, *Plan, []byte) {
	b.Helper()
	host, err := NewISAHost(wasmBytes)
	if err != nil {
		b.Fatalf("NewISAHost: %v", err)
	}
	b.Cleanup(func() { host.Close() })
	for i := 0; i < n; i++ {
		if err := host.Put(fmt.Sprintf("user:%05d", i),
			[]byte(fmt.Sprintf(`{"name":"U%05d","age":%d,"city":"C","active":true,"score":1.0}`, i, i%100))); err != nil {
			b.Fatalf("Put %d: %v", i, err)
		}
		if (i+1)%500 == 0 {
			if err := host.Flush(); err != nil {
				b.Fatalf("Flush at %d: %v", i+1, err)
			}
		}
	}
	if err := host.Flush(); err != nil {
		b.Fatalf("final Flush: %v", err)
	}
	snap, err := host.Snapshot()
	if err != nil {
		b.Fatalf("Snapshot: %v", err)
	}
	plan, err := Compile("SELECT key, name FROM users", testSchema())
	if err != nil {
		b.Fatalf("compile: %v", err)
	}
	prog, err := CompileISA(plan, Contract{})
	if err != nil {
		b.Fatalf("CompileISA: %v", err)
	}
	return host, snap, plan, prog
}

func BenchmarkGate4ISAHostScan32(b *testing.B) {
	host, snap, plan, prog := gate4BenchISAHost(b, gate4BenchWasm(b, "../build/yowza.wasm"), 20000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, _, err := host.ExecuteISAAt(prog, plan, snap)
		if err != nil {
			b.Fatalf("ExecuteISAAt: %v", err)
		}
		if len(rows) != 20000 {
			b.Fatalf("rows = %d, want 20000", len(rows))
		}
	}
}

func BenchmarkGate4ISAHostScan64(b *testing.B) {
	host, snap, plan, prog := gate4BenchISAHost(b, gate4BenchWasm(b, "../build/yowza64.wasm"), 20000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, _, err := host.ExecuteISAAt(prog, plan, snap)
		if err != nil {
			b.Fatalf("ExecuteISAAt: %v", err)
		}
		if len(rows) != 20000 {
			b.Fatalf("rows = %d, want 20000", len(rows))
		}
	}
}

func gate4BenchCoord(b *testing.B, policy ResourcePolicy) (*ISAMultiShardCoordinator, []uint64, *Plan, []byte) {
	b.Helper()
	wasmBytes := gate4BenchWasm(b, "../build/yowza.wasm")
	var hosts []*ISAHost
	var snaps []uint64
	for h := 0; h < 2; h++ {
		host, err := NewISAHost(wasmBytes)
		if err != nil {
			b.Fatalf("NewISAHost %d: %v", h, err)
		}
		b.Cleanup(func() { host.Close() })
		for i := 0; i < 3000; i++ {
			global := h*3000 + i
			if err := host.Put(fmt.Sprintf("user:%05d", global),
				[]byte(fmt.Sprintf(`{"name":"U%05d","age":%d,"city":"C","active":true,"score":1.0}`, global, (global*37)%100))); err != nil {
				b.Fatalf("host %d Put %d: %v", h, i, err)
			}
			if (i+1)%500 == 0 {
				if err := host.Flush(); err != nil {
					b.Fatalf("host %d Flush at %d: %v", h, i, err)
				}
			}
		}
		if err := host.Flush(); err != nil {
			b.Fatalf("host %d final Flush: %v", h, err)
		}
		snap, err := host.Snapshot()
		if err != nil {
			b.Fatalf("host %d Snapshot: %v", h, err)
		}
		hosts = append(hosts, host)
		snaps = append(snaps, snap)
	}
	coord, err := NewISAMultiShardCoordinator(hosts,
		PartitionMetadata{ShardCount: 2, Disjoint: true, Verified: true}, ModeReject, policy)
	if err != nil {
		b.Fatalf("NewISAMultiShardCoordinator: %v", err)
	}
	plan, err := Compile("SELECT name, age FROM users ORDER BY age", testSchema())
	if err != nil {
		b.Fatalf("compile: %v", err)
	}
	prog, err := CompileISA(plan, Contract{})
	if err != nil {
		b.Fatalf("CompileISA: %v", err)
	}
	return coord, snaps, plan, prog
}

func BenchmarkGate4MultiShardSort(b *testing.B) {
	coord, snaps, plan, prog := gate4BenchCoord(b, DefaultResourcePolicy())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, _, err := coord.Execute(context.Background(), prog, plan, snaps)
		if err != nil {
			b.Fatalf("Execute: %v", err)
		}
		if len(rows) != 6000 {
			b.Fatalf("rows = %d, want 6000", len(rows))
		}
	}
}

func BenchmarkGate4SpillSort(b *testing.B) {
	pol := DefaultResourcePolicy()
	pol.MaxSortRows = 500
	coord, snaps, plan, prog := gate4BenchCoord(b, pol)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, tr, err := coord.Execute(context.Background(), prog, plan, snaps)
		if err != nil {
			b.Fatalf("Execute: %v", err)
		}
		if len(rows) != 6000 || !tr.Spilled {
			b.Fatalf("rows = %d spilled = %v, want 6000 and spill", len(rows), tr.Spilled)
		}
	}
}

func BenchmarkGate4Checkpoint(b *testing.B) {
	wasmBytes := gate4BenchWasm(b, "../build/yowza.wasm")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dir := filepath.Join(b.TempDir(), "db")
		p, err := yowza.OpenPersistedSegDB(dir, wasmBytes, yowza.PersistedOptions{NumShards: 1, Durability: yowza.DurabilityNoSync})
		if err != nil {
			b.Fatalf("OpenPersistedSegDB: %v", err)
		}
		for j := 0; j < 500; j++ {
			if err := p.Put(fmt.Sprintf("k:%05d", j), []byte(fmt.Sprintf("v:%d", j))); err != nil {
				b.Fatalf("Put %d: %v", j, err)
			}
		}
		if err := p.Flush(); err != nil {
			b.Fatalf("Flush (checkpoint): %v", err)
		}
		if err := p.Close(); err != nil {
			b.Fatalf("Close: %v", err)
		}
	}
}
