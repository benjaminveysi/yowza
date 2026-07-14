package cloud

import (
	"os"
	"testing"

	"github.com/yowza/yowza"
)

func TestCloudManager(t *testing.T) {
	// Clean up files before and after
	walFile := "test_cloud.wal"
	snapFile := "test_cloud.snap"
	os.Remove(walFile)
	os.Remove(snapFile)
	defer os.Remove(walFile)
	defer os.Remove(snapFile)

	wasmBytes, err := os.ReadFile("../build/yowza.wasm")
	if err != nil {
		t.Fatalf("Failed to read Wasm binary: %v", err)
	}

	db, err := yowza.NewYowzaDB(wasmBytes)
	if err != nil {
		t.Fatalf("Failed to init DB: %v", err)
	}

	backend := NewLocalFileBackend(walFile, snapFile)
	manager := NewManager(db, backend)

	// 1. Initial Bootstrap (Should be empty)
	err = manager.Bootstrap()
	if err != nil {
		t.Fatalf("Initial bootstrap failed: %v", err)
	}

	// 2. Perform some mutations
	db.Put("cloud_key", []byte("cloud_value"))
	db.Put("temp_key", []byte("temp_value"))
	db.Delete("temp_key")

	// 3. Sync WAL
	err = manager.SyncWAL()
	if err != nil {
		t.Fatalf("SyncWAL failed: %v", err)
	}

	// Check WAL file exists
	if _, err := os.Stat(walFile); os.IsNotExist(err) {
		t.Fatalf("WAL file was not created by backend")
	}

	// 4. Compact Snapshot
	err = manager.CompactSnapshot()
	if err != nil {
		t.Fatalf("CompactSnapshot failed: %v", err)
	}

	// Check Snapshot exists and WAL is cleared
	if _, err := os.Stat(snapFile); os.IsNotExist(err) {
		t.Fatalf("Snapshot file was not created")
	}
	if _, err := os.Stat(walFile); !os.IsNotExist(err) {
		t.Fatalf("WAL file should have been deleted by Compaction")
	}

	// 5. Simulate Crash & Restart (Cold Start)
	db.Close()

	db2, err := yowza.NewYowzaDB(wasmBytes)
	if err != nil {
		t.Fatalf("Failed to init DB2: %v", err)
	}
	defer db2.Close()

	manager2 := NewManager(db2, backend)

	err = manager2.Bootstrap()
	if err != nil {
		t.Fatalf("Cold start bootstrap failed: %v", err)
	}

	// 6. Verify data recovered from Cloud (LocalFileBackend)
	val, err := db2.Get("cloud_key")
	if err != nil || string(val) != "cloud_value" {
		t.Fatalf("Recovered value incorrect. Expected 'cloud_value', got '%v'", string(val))
	}

	val, err = db2.Get("temp_key")
	if val != nil {
		t.Fatalf("Deleted key was incorrectly recovered")
	}
}
