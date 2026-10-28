package yowza

import (
	"context"
	"os"
	"testing"
)

// TestCompactLockedGrowsTempBuffer exercises the -2 retry path in
// compactLocked directly: with memOffsetSnapCap forced tiny, the first
// yowza_compact call must return -2 (temp buffer too small, before any
// arena reset), compactLocked must grow the buffer and retry, and the
// data must survive. This is reachable even though a normal Put flow
// cannot make the live snapshot exceed 1 MiB (the arena itself is 1 MiB),
// which is why the -2 path is a safety net rather than a common case.
func TestCompactLockedGrowsTempBuffer(t *testing.T) {
	wasmBytes, err := os.ReadFile("build/yowza.wasm")
	if err != nil {
		t.Fatalf("Failed to read Wasm binary: %v", err)
	}
	db, err := NewYowzaDB(wasmBytes)
	if err != nil {
		t.Fatalf("Failed to initialize YowzaDB: %v", err)
	}
	defer db.Close()

	if err := db.Put("growkey", []byte("growvalue")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Force the temp buffer tiny (below the 20-byte minimum in
	// yowza_compact) so compact must return -2 first.
	db.memOffsetSnapCap = 10
	if err := db.compactLocked(context.Background()); err != nil {
		t.Fatalf("compactLocked with tiny buffer failed: %v", err)
	}
	if db.memOffsetSnapCap <= 10 {
		t.Fatalf("compactLocked did not grow the temp buffer, cap still %d", db.memOffsetSnapCap)
	}
	val, err := db.Get("growkey")
	if err != nil || string(val) != "growvalue" {
		t.Fatalf("data lost after compact: val=%q err=%v", val, err)
	}
}
