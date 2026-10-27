package yowza

import (
	"bytes"
	"os"
	"testing"
)

func TestSecondaryIndex(t *testing.T) {
	wasmBytes, err := os.ReadFile("build/yowza.wasm")
	if err != nil {
		t.Fatalf("Failed to read Wasm binary: %v", err)
	}

	db, err := NewYowzaDB(wasmBytes)
	if err != nil {
		t.Fatalf("Failed to initialize YowzaDB: %v", err)
	}
	defer db.Close()

	// 1. Put primary data and secondary index
	err = db.PutWithIndex("user:uuid-1234", []byte(`{"name":"Alice", "age":30}`), "email", "alice@example.com")
	if err != nil {
		t.Fatalf("PutWithIndex failed: %v", err)
	}

	err = db.PutWithIndex("user:uuid-5678", []byte(`{"name":"Bob", "age":25}`), "email", "bob@example.com")
	if err != nil {
		t.Fatalf("PutWithIndex failed: %v", err)
	}

	// Add another Alice for scan testing
	db.PutWithIndex("user:uuid-9999", []byte(`{"name":"Alice2", "age":31}`), "email", "alice2@example.com")

	// 2. Search exact by index
	res, err := db.SearchByIndex("email", "bob@example.com")
	if err != nil {
		t.Fatalf("SearchByIndex failed: %v", err)
	}
	if !bytes.Contains(res, []byte("Bob")) {
		t.Errorf("Expected Bob's JSON, got %s", res)
	}

	// Search missing index
	res, err = db.SearchByIndex("email", "notfound@example.com")
	if err != nil {
		t.Fatalf("SearchByIndex failed: %v", err)
	}
	if res != nil {
		t.Errorf("Expected nil for missing index, got %s", res)
	}

	// 3. Scan by partial index
	scanRes, err := db.ScanByIndex("email", "alice", 1024)
	if err != nil {
		t.Fatalf("ScanByIndex failed: %v", err)
	}

	if len(scanRes) != 2 {
		t.Errorf("Expected 2 results for alice prefix search, got %d", len(scanRes))
	}

	foundAlice1 := false
	foundAlice2 := false
	for _, r := range scanRes {
		if bytes.Contains(r, []byte("Alice\"")) {
			foundAlice1 = true
		}
		if bytes.Contains(r, []byte("Alice2\"")) {
			foundAlice2 = true
		}
	}

	if !foundAlice1 || !foundAlice2 {
		t.Errorf("ScanByIndex didn't return both Alices. Got: %v", scanRes)
	}
}

func TestSecondaryIndexDuplicateValue(t *testing.T) {
	wasmBytes, err := os.ReadFile("build/yowza.wasm")
	if err != nil {
		t.Fatalf("Failed to read Wasm binary: %v", err)
	}

	db, err := NewYowzaDB(wasmBytes)
	if err != nil {
		t.Fatalf("Failed to initialize YowzaDB: %v", err)
	}
	defer db.Close()

	// Two primaries sharing one index value: the second must NOT
	// overwrite the first one's index entry (old layout did exactly that).
	if err := db.PutWithIndex("user:shared-1", []byte(`{"name":"SharedOne"}`), "email", "shared@example.com"); err != nil {
		t.Fatalf("PutWithIndex failed: %v", err)
	}
	if err := db.PutWithIndex("user:shared-2", []byte(`{"name":"SharedTwo"}`), "email", "shared@example.com"); err != nil {
		t.Fatalf("PutWithIndex failed: %v", err)
	}

	scanRes, err := db.ScanByIndex("email", "shared@example.com", 1024)
	if err != nil {
		t.Fatalf("ScanByIndex failed: %v", err)
	}
	if len(scanRes) != 2 {
		t.Fatalf("Expected 2 results for shared index value, got %d: %v", len(scanRes), scanRes)
	}
	foundOne, foundTwo := false, false
	for _, r := range scanRes {
		if bytes.Contains(r, []byte("SharedOne")) {
			foundOne = true
		}
		if bytes.Contains(r, []byte("SharedTwo")) {
			foundTwo = true
		}
	}
	if !foundOne || !foundTwo {
		t.Errorf("ScanByIndex didn't return both primaries sharing the index value. Got: %v", scanRes)
	}

	// Exact search for a shared value returns one of the primaries
	// (scan order decides which); it must not return nil.
	res, err := db.SearchByIndex("email", "shared@example.com")
	if err != nil {
		t.Fatalf("SearchByIndex failed: %v", err)
	}
	if res == nil {
		t.Errorf("Expected a result for shared index value, got nil")
	}
}

func TestSecondaryIndexDeleteCleansIndex(t *testing.T) {
	wasmBytes, err := os.ReadFile("build/yowza.wasm")
	if err != nil {
		t.Fatalf("Failed to read Wasm binary: %v", err)
	}

	db, err := NewYowzaDB(wasmBytes)
	if err != nil {
		t.Fatalf("Failed to initialize YowzaDB: %v", err)
	}
	defer db.Close()

	if err := db.PutWithIndex("user:del-1", []byte(`{"name":"ToDelete"}`), "email", "delete-me@example.com"); err != nil {
		t.Fatalf("PutWithIndex failed: %v", err)
	}
	if err := db.PutWithIndex("user:keep-1", []byte(`{"name":"KeepMe"}`), "email", "keep-me@example.com"); err != nil {
		t.Fatalf("PutWithIndex failed: %v", err)
	}

	if err := db.Delete("user:del-1"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Primary is gone.
	if val, err := db.Get("user:del-1"); err != nil || val != nil {
		t.Errorf("Expected deleted primary to be gone, got %v (err %v)", val, err)
	}

	// Its index entries are gone too: exact search and prefix scan
	// must not resolve to the deleted record.
	if res, err := db.SearchByIndex("email", "delete-me@example.com"); err != nil || res != nil {
		t.Errorf("Expected nil for deleted record's index, got %s (err %v)", res, err)
	}
	if scanRes, err := db.ScanByIndex("email", "delete-me", 1024); err != nil || len(scanRes) != 0 {
		t.Errorf("Expected no results for deleted record's index prefix, got %v (err %v)", scanRes, err)
	}

	// The other record's index is untouched.
	if res, err := db.SearchByIndex("email", "keep-me@example.com"); err != nil || !bytes.Contains(res, []byte("KeepMe")) {
		t.Errorf("Expected KeepMe via index after unrelated delete, got %s (err %v)", res, err)
	}
}

func TestGetEmptyValue(t *testing.T) {
	wasmBytes, err := os.ReadFile("build/yowza.wasm")
	if err != nil {
		t.Fatalf("Failed to read Wasm binary: %v", err)
	}

	db, err := NewYowzaDB(wasmBytes)
	if err != nil {
		t.Fatalf("Failed to initialize YowzaDB: %v", err)
	}
	defer db.Close()

	// An empty value is a found value: Get must return a non-nil empty
	// slice, not the nil that means "not found".
	if err := db.Put("emptykey", []byte{}); err != nil {
		t.Fatalf("Put empty value failed: %v", err)
	}
	val, err := db.Get("emptykey")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if val == nil {
		t.Errorf("Expected non-nil empty slice for empty value, got nil (reported as not found)")
	}
	if len(val) != 0 {
		t.Errorf("Expected length 0 for empty value, got %d", len(val))
	}

	// A genuinely missing key still returns nil.
	missing, err := db.Get("does-not-exist")
	if err != nil {
		t.Fatalf("Get missing failed: %v", err)
	}
	if missing != nil {
		t.Errorf("Expected nil for missing key, got %v", missing)
	}
}
