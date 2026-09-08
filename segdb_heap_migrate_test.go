package yowza

// Integration tests for the Track C heap-growth fix (see
// segDBHeapSize in segdb.go): Track C doubled the wasm heap
// table (4096 → 8192 slots) inside the fixed heap region, which
// shrank data room and broke two previously-green tests at
// exactly the new exhaustion point. The fix grows the region
// (4 MiB → 8 MiB) and keeps old 4 MiB persisted heap images
// openable (SHA verified over stored bytes, then
// zero-extended, then wasm init migrates the legacy table).
//
// Coverage split, stated honestly: this file proves the Go
// size-extension wiring + init migration of a legacy table
// through the real wasm init (empty legacy heap, then
// Put/Get/Flush on the migrated heap). Data-carrying table
// migration (live slots, payload bytes intact) is proven at
// C level by test_trackc_heap_migrate, not duplicated here.

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
)

func TestHeapImageExtensionSizes(t *testing.T) {
	cur := make([]byte, segDBHeapSize)
	for i := range cur {
		cur[i] = byte(i)
	}
	out, err := extendHeapImageToCurrent(cur)
	if err != nil {
		t.Fatalf("current size: %v", err)
	}
	if len(out) != len(cur) {
		t.Fatalf("current size len %d, want %d", len(out), len(cur))
	}
	for i := range cur {
		if out[i] != cur[i] {
			t.Fatalf("current size byte %d changed", i)
		}
	}
	legacy := make([]byte, segDBHeapSizeLegacy)
	for i := range legacy {
		legacy[i] = byte(255 - i)
	}
	ext, err := extendHeapImageToCurrent(legacy)
	if err != nil {
		t.Fatalf("legacy size: %v", err)
	}
	if uint64(len(ext)) != segDBHeapSize {
		t.Fatalf("extended len %d, want %d", len(ext), segDBHeapSize)
	}
	for i := range legacy {
		if ext[i] != legacy[i] {
			t.Fatalf("extended byte %d changed", i)
		}
	}
	for i := uint64(len(legacy)); i < uint64(len(ext)); i++ {
		if ext[i] != 0 {
			t.Fatalf("extended tail byte %d = %d, want 0", i, ext[i])
		}
	}
	if _, err := extendHeapImageToCurrent(make([]byte, 12345)); err == nil {
		t.Fatal("odd size accepted, want corruption error")
	}
}

// A hand-built legacy heap image: magic "YWHEAP01", table
// count 4096, bump at the legacy data start, no live slots.
// Through extend + Restore + NewSegDB, wasm init must
// migrate the table (count becomes 8192) and the heap must
// then serve Put/Get/Flush normally.
func TestLegacyHeapImageMigratesOnInit(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			legacy := make([]byte, segDBHeapSizeLegacy)
			copy(legacy, "YWHEAP01")
			binary.LittleEndian.PutUint32(legacy[8:12], 4096)
			// Legacy data start: 32 + 4096*152 = 622624.
			binary.LittleEndian.PutUint64(legacy[16:24], 622624)
			ext, err := extendHeapImageToCurrent(legacy)
			if err != nil {
				t.Fatalf("extend: %v", err)
			}
			wasmBytes, err := os.ReadFile(bin)
			if err != nil {
				t.Fatalf("read wasm: %v", err)
			}
			db, err := NewYowzaDB(wasmBytes)
			if err != nil {
				t.Fatalf("NewYowzaDB: %v", err)
			}
			defer db.Close()
			if err := db.RestoreSegDBHeap(ext); err != nil {
				t.Fatalf("RestoreSegDBHeap: %v", err)
			}
			sdb, err := NewSegDB(db)
			if err != nil {
				t.Fatalf("NewSegDB (migrated): %v", err)
			}
			heap, err := sdb.HeapBytes()
			if err != nil {
				t.Fatalf("HeapBytes: %v", err)
			}
			if got := binary.LittleEndian.Uint32(heap[8:12]); got != 8192 {
				t.Fatalf("migrated table count %d, want 8192", got)
			}
			if err := sdb.Put("migkey", []byte("migval")); err != nil {
				t.Fatalf("Put after migrate: %v", err)
			}
			if err := sdb.Flush(); err != nil {
				t.Fatalf("Flush after migrate: %v", err)
			}
			got, err := sdb.Get("migkey")
			if err != nil || string(got) != "migval" {
				t.Fatalf("Get after migrate = %q, err %v", got, err)
			}
		})
	}
}

// persistValidateHeapFiles accepts a legacy-size image
// when its SHA-256 matches the checkpoint descriptor.
func TestValidateHeapFilesAcceptsLegacySize(t *testing.T) {
	dir := t.TempDir()
	legacy := make([]byte, segDBHeapSizeLegacy)
	for i := range legacy {
		legacy[i] = byte(i % 251)
	}
	sum := sha256.Sum256(legacy)
	name := persistCheckpointHeapFileName(1, 0)
	if err := os.WriteFile(filepath.Join(dir, name), legacy, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	ckpt := &persistCheckpoint{
		NumShards:  1,
		Generation: 1,
		HeapSHA256: []string{hex.EncodeToString(sum[:])},
	}
	files, err := persistValidateHeapFiles(dir, ckpt)
	if err != nil {
		t.Fatalf("validate legacy size: %v", err)
	}
	if len(files) != 1 || files[0] != name {
		t.Fatalf("files %v, want [%s]", files, name)
	}
	// A wrong-size file is still corruption.
	if err := os.WriteFile(filepath.Join(dir, name), make([]byte, 999), 0o644); err != nil {
		t.Fatalf("overwrite: %v", err)
	}
	if _, err := persistValidateHeapFiles(dir, ckpt); err == nil {
		t.Fatal("wrong-size file accepted, want ErrCorruptHeap")
	}
}
