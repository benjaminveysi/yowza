package yowza

import (
	"bytes"
	"fmt"
	"os"
	"sync"
	"testing"
)

func TestGoWasmBridge(t *testing.T) {
	// 1. Read the compiled Wasm binary
	wasmBytes, err := os.ReadFile("build/yowza.wasm")
	if err != nil {
		t.Fatalf("Failed to read Wasm binary (did you run 'make yowza.wasm'?): %v", err)
	}

	// 2. Initialize Go Wasm Host
	db, err := NewYowzaDB(wasmBytes)
	if err != nil {
		t.Fatalf("Failed to initialize YowzaDB: %v", err)
	}
	defer db.Close()

	// 3. Test Put and Get
	err = db.Put("user:1", []byte("Alice"))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	err = db.Put("user:2", []byte("Bob"))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	val, err := db.Get("user:1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(val) != "Alice" {
		t.Errorf("Expected 'Alice', got '%s'", val)
	}

	// 4. Test Not Found
	val, err = db.Get("user:99")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if val != nil {
		t.Errorf("Expected nil for missing key, got '%s'", val)
	}

	// 5. Test Delete
	err = db.Delete("user:2")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	val, err = db.Get("user:2")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if val != nil {
		t.Errorf("Expected nil after delete, got '%s'", val)
	}

	// 6. Test Scan
	db.Put("user:3", []byte("Charlie"))

	results, err := db.Scan("user:", 1024)
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("Expected 2 results from scan, got %d", len(results))
	}

	// Order is not strictly guaranteed by LCRS without sorting, but let's check contents
	foundAlice := false
	foundCharlie := false
	for _, res := range results {
		if bytes.Equal(res, []byte("Alice")) {
			foundAlice = true
		}
		if bytes.Equal(res, []byte("Charlie")) {
			foundCharlie = true
		}
	}

	if !foundAlice || !foundCharlie {
		t.Errorf("Scan didn't return expected results. Got: %v", results)
	}

	// 7. Test WAL
	wal, err := db.FetchWAL()
	if err != nil {
		t.Fatalf("FetchWAL failed: %v", err)
	}

	// Should have: Put(user:1), Put(user:2), Delete(user:2), Put(user:3)
	if len(wal) != 4 {
		t.Fatalf("Expected 4 WAL entries, got %d", len(wal))
	}

	if wal[0].Opcode != OpcodePut || string(wal[0].Key) != "user:1" {
		t.Errorf("First WAL entry incorrect: %+v", wal[0])
	}

	if wal[2].Opcode != OpcodeDel || string(wal[2].Key) != "user:2" {
		t.Errorf("Third WAL entry incorrect: %+v", wal[2])
	}

	// FetchWAL is peek-only since the step-3 WAL fix: fetching again must
	// still return the same 4 entries, and only AckWAL clears them.
	wal2, err := db.FetchWAL()
	if err != nil {
		t.Fatalf("FetchWAL 2 failed: %v", err)
	}
	if len(wal2) != 4 {
		t.Fatalf("Expected 4 WAL entries on second peek, got %d", len(wal2))
	}
	if err := db.AckWAL(); err != nil {
		t.Fatalf("AckWAL failed: %v", err)
	}
	wal3, err := db.FetchWAL()
	if err != nil {
		t.Fatalf("FetchWAL 3 failed: %v", err)
	}
	if len(wal3) != 0 {
		t.Fatalf("Expected empty WAL after AckWAL, got %d", len(wal3))
	}

	// 8. Test Snapshot
	snap, err := db.CreateSnapshot()
	if err != nil {
		t.Fatalf("CreateSnapshot failed: %v", err)
	}
	if len(snap) == 0 {
		t.Fatalf("Snapshot is empty")
	}

	// Make a fresh DB and restore
	db2, err := NewYowzaDB(wasmBytes)
	if err != nil {
		t.Fatalf("Failed to initialize YowzaDB 2: %v", err)
	}
	defer db2.Close()

	err = db2.RestoreSnapshot(snap)
	if err != nil {
		t.Fatalf("RestoreSnapshot failed: %v", err)
	}

	// Verify restored data
	val3, err := db2.Get("user:3")
	if err != nil {
		t.Fatalf("Get on restored DB failed: %v", err)
	}
	if string(val3) != "Charlie" {
		t.Errorf("Expected 'Charlie' in restored DB, got '%s'", val3)
	}
}

func TestGoWasmBridgeConcurrency(t *testing.T) {
	wasmBytes, err := os.ReadFile("build/yowza.wasm")
	if err != nil {
		t.Fatalf("Failed to read Wasm binary: %v", err)
	}

	db, err := NewYowzaDB(wasmBytes)
	if err != nil {
		t.Fatalf("Failed to init YowzaDB: %v", err)
	}
	defer db.Close()

	// Spin up 10 goroutines writing to different keys concurrently
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				key := fmt.Sprintf("goroutine:%d:%d", idx, j)
				val := []byte(fmt.Sprintf("val:%d:%d", idx, j))

				if err := db.Put(key, val); err != nil {
					t.Errorf("Concurrent Put failed: %v", err)
				}

				retVal, err := db.Get(key)
				if err != nil {
					t.Errorf("Concurrent Get failed: %v", err)
				}

				if !bytes.Equal(retVal, val) {
					t.Errorf("Concurrent Get mismatch, expected %s, got %s", val, retVal)
				}
			}
		}(i)
	}

	wg.Wait()

	// Ensure no crashes and WAL was collected correctly
	wal, err := db.FetchWAL()
	if err != nil {
		t.Fatalf("FetchWAL failed after concurrency: %v", err)
	}

	if len(wal) != 500 {
		t.Fatalf("Expected exactly 500 operations in WAL, got %d", len(wal))
	}
}

func TestGoWasmBridgeCompactionThreshold(t *testing.T) {
	wasmBytes, err := os.ReadFile("build/yowza.wasm")
	if err != nil {
		t.Fatalf("Failed to read Wasm binary: %v", err)
	}

	db, err := NewYowzaDB(wasmBytes)
	if err != nil {
		t.Fatalf("Failed to init YowzaDB: %v", err)
	}
	defer db.Close()

	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("threshold_key_%d", i)
		val := []byte("value")
		err := db.Put(key, val)
		if err != nil {
			t.Fatalf("Failed to put: %v", err)
		}
	}

	err = db.Compact()
	if err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	val, err := db.Get("threshold_key_10")
	if err != nil || string(val) != "value" {
		t.Fatalf("Data lost after compaction")
	}
}

func TestCRDTLWW(t *testing.T) {
	wasmBytes, err := os.ReadFile("build/yowza.wasm")
	if err != nil {
		t.Fatalf("Failed to read Wasm binary: %v", err)
	}

	db, err := NewYowzaDB(wasmBytes)
	if err != nil {
		t.Fatalf("Failed to init DB: %v", err)
	}
	defer db.Close()

	// 1. Initial Insert with Timestamp 100
	err = db.PutCRDT("crdt_key", []byte("version_1"), 100)
	if err != nil { t.Fatalf("PutCRDT failed: %v", err) }

	val, _ := db.Get("crdt_key")
	if string(val) != "version_1" { t.Fatalf("Expected version_1") }

	// 2. Insert with older Timestamp 50 (should be ignored)
	err = db.PutCRDT("crdt_key", []byte("version_older"), 50)
	if err != nil { t.Fatalf("PutCRDT failed: %v", err) }

	val, _ = db.Get("crdt_key")
	if string(val) != "version_1" { t.Fatalf("CRDT LWW failed. Expected version_1, got %v", string(val)) }

	// 3. Insert with newer Timestamp 200 (should update)
	err = db.PutCRDT("crdt_key", []byte("version_2"), 200)
	if err != nil { t.Fatalf("PutCRDT failed: %v", err) }

	val, _ = db.Get("crdt_key")
	if string(val) != "version_2" { t.Fatalf("CRDT LWW failed. Expected version_2, got %v", string(val)) }
}
