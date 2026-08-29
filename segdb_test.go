package yowza

import (
	"fmt"
	"os"
	"testing"
)

// Track A item 5 Go tests, run against both wasm binaries: the
// wasm32 binary exercises the 32-bit pointer path and the wasm64
// binary the memory64 path, through the same Go bindings.
func TestSegDBBasic(t *testing.T) {
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
			if err := sdb.Put("a", []byte("1")); err != nil {
				t.Fatalf("Put a: %v", err)
			}
			if err := sdb.Put("b", []byte("2")); err != nil {
				t.Fatalf("Put b: %v", err)
			}
			if v, err := sdb.Get("a"); err != nil || string(v) != "1" {
				t.Fatalf("Get a = %q, err %v, want 1", v, err)
			}
			if err := sdb.Flush(); err != nil {
				t.Fatalf("Flush: %v", err)
			}
			if n, err := sdb.SegmentCount(); err != nil || n != 1 {
				t.Fatalf("SegmentCount = %d, err %v, want 1", n, err)
			}
			if err := sdb.Put("a", []byte("10")); err != nil {
				t.Fatalf("Put a=10: %v", err)
			}
			if err := sdb.Delete("b"); err != nil {
				t.Fatalf("Delete b: %v", err)
			}
			if err := sdb.Flush(); err != nil {
				t.Fatalf("Flush 2: %v", err)
			}
			if v, err := sdb.Get("a"); err != nil || string(v) != "10" {
				t.Fatalf("Get a = %q, err %v, want 10", v, err)
			}
			if _, err := sdb.Get("b"); err == nil {
				t.Fatalf("Get b succeeded, want missing")
			}
			entries, err := sdb.Scan("", 0)
			if err != nil {
				t.Fatalf("Scan: %v", err)
			}
			if len(entries) != 1 || string(entries[0].Key) != "a" || string(entries[0].Val) != "10" {
				t.Fatalf("Scan = %+v, want single a=10", entries)
			}
			// Compact with a barrier at 0 drops nothing here
			// (no segment tombstones at seq <= 0), but merges
			// the two segments into one.
			if err := sdb.Compact(0); err != nil {
				t.Fatalf("Compact: %v", err)
			}
			if n, err := sdb.SegmentCount(); err != nil || n != 1 {
				t.Fatalf("SegmentCount after compact = %d, err %v, want 1", n, err)
			}
			if v, err := sdb.Get("a"); err != nil || string(v) != "10" {
				t.Fatalf("Get a after compact = %q, err %v, want 10", v, err)
			}
			if ind, err := sdb.Indeterminate(); err != nil || ind {
				t.Fatalf("Indeterminate = %v, err %v, want false", ind, err)
			}
		})
	}
}

func TestSegDBHeapPersistence(t *testing.T) {
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
			sdb, err := NewSegDB(db)
			if err != nil {
				t.Fatalf("NewSegDB: %v", err)
			}
			if err := sdb.Put("segkey", []byte("old")); err != nil {
				t.Fatalf("Put segkey: %v", err)
			}
			if err := sdb.Flush(); err != nil {
				t.Fatalf("Flush: %v", err)
			}
			// Flushed tombstone: delete + flush, so the delete
			// itself is in the heap, not just hot.
			if err := sdb.Delete("segkey"); err != nil {
				t.Fatalf("Delete segkey: %v", err)
			}
			if err := sdb.Flush(); err != nil {
				t.Fatalf("Flush tombstone: %v", err)
			}
			// Unflushed hot put: must NOT survive a module
			// restart (volatile hot WAL — documented).
			if err := sdb.Put("hotkey", []byte("hot")); err != nil {
				t.Fatalf("Put hotkey: %v", err)
			}
			heap, err := sdb.HeapBytes()
			if err != nil {
				t.Fatalf("HeapBytes: %v", err)
			}
			db.Close()

			db2, err := NewYowzaDB(wasmBytes)
			if err != nil {
				t.Fatalf("NewYowzaDB 2: %v", err)
			}
			defer db2.Close()
			if err := db2.RestoreSegDBHeap(heap); err != nil {
				t.Fatalf("RestoreSegDBHeap: %v", err)
			}
			sdb2, err := NewSegDB(db2)
			if err != nil {
				t.Fatalf("NewSegDB 2: %v", err)
			}
			if err := sdb2.Recover(); err != nil {
				t.Fatalf("Recover: %v", err)
			}
			if _, err := sdb2.Get("segkey"); err == nil {
				t.Fatalf("Get segkey succeeded after restore, want missing (tombstone)")
			}
			if _, err := sdb2.Get("hotkey"); err == nil {
				t.Fatalf("Get hotkey succeeded after restore, want missing (volatile WAL)")
			}
		})
	}
}

func TestSegDBMultiLevel(t *testing.T) {
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
			for i := 0; i < 130; i++ {
				k := fmt.Sprintf("m%03d", i)
				if err := sdb.Put(k, []byte(fmt.Sprintf("v%03d", i))); err != nil {
					t.Fatalf("Put %s: %v", k, err)
				}
				if err := sdb.Flush(); err != nil {
					t.Fatalf("Flush %s: %v", k, err)
				}
			}
			hasDir, err := sdb.HasDir()
			if err != nil || !hasDir {
				t.Fatalf("HasDir = %v, err %v, want true", hasDir, err)
			}
			groups, err := sdb.GroupCount()
			if err != nil || groups != 2 {
				t.Fatalf("GroupCount = %d, err %v, want 2", groups, err)
			}
			entries, err := sdb.Scan("", 0)
			if err != nil {
				t.Fatalf("Scan: %v", err)
			}
			if len(entries) != 130 {
				t.Fatalf("Scan count = %d, want 130", len(entries))
			}
			for i := 0; i < 130; i++ {
				k := fmt.Sprintf("m%03d", i)
				if v, err := sdb.Get(k); err != nil || string(v) != fmt.Sprintf("v%03d", i) {
					t.Fatalf("Get %s = %q, err %v", k, v, err)
				}
			}
			if err := sdb.Compact(0); err != nil {
				t.Fatalf("Compact: %v", err)
			}
			groups, err = sdb.GroupCount()
			if err != nil || groups != 1 {
				t.Fatalf("GroupCount after compact = %d, err %v, want 1", groups, err)
			}
			entries, err = sdb.Scan("", 0)
			if err != nil {
				t.Fatalf("Scan after compact: %v", err)
			}
			if len(entries) != 130 {
				t.Fatalf("Scan count after compact = %d, want 130", len(entries))
			}
		})
	}
}
