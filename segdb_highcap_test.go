package yowza

// Yowza64 High-Capacity Profile track (worker S): configurable
// wasm heap sizes (Standard 8 MiB, HighCapacity 64/256 MiB),
// the checkpoint HeapSize record, loader rules in both
// directions, sparse heap files, generation behaviour, crash
// durability, and publish latency (gate 6).
//
// All tests use wasm64 for HighCapacity sizes (wasm32 refusal
// is tested explicitly) and one shard unless noted, so the
// heap size under test is exactly one shard's heap.

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

const (
	highCapWasm64 = "build/yowza64.wasm"
	highCapWasm32 = "build/yowza.wasm"
)

// highCapMapGets asserts every key in want reads back
// exactly (full key comparison via Get only).
func highCapMapGets(t *testing.T, db *PersistedSegDB, want map[string]string) {
	t.Helper()
	for k, v := range want {
		got, err := db.Get(k)
		if err != nil || string(got) != v {
			t.Fatalf("Get %q = %q, err %v, want %q", k, got, err, v)
		}
	}
}

// highCapMap asserts every key in want reads back exactly,
// and that a full scan finds no extra keys.
func highCapMap(t *testing.T, db *PersistedSegDB, want map[string]string) {
	t.Helper()
	highCapMapGets(t, db, want)
	entries, err := db.Scan("")
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(entries) != len(want) {
		t.Fatalf("Scan found %d keys, want %d", len(entries), len(want))
	}
	for _, e := range entries {
		if want[string(e.Key)] != string(e.Val) {
			t.Fatalf("Scan key %q = %q, want %q", e.Key, e.Val, want[string(e.Key)])
		}
	}
}

func highCapCheckpoint(t *testing.T, dir string) persistCheckpoint {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dir, "checkpoint.json"))
	if err != nil {
		t.Fatalf("read checkpoint.json: %v", err)
	}
	var ck persistCheckpoint
	if err := json.Unmarshal(raw, &ck); err != nil {
		t.Fatalf("unmarshal checkpoint.json: %v", err)
	}
	return ck
}

// highCapFileUsage returns apparent size and allocated
// (blocks x 512) bytes for path.
func highCapFileUsage(t *testing.T, path string) (apparent, blocks int64) {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	blocks = 0
	if st, ok := fi.Sys().(*syscall.Stat_t); ok {
		blocks = st.Blocks * 512
	}
	return fi.Size(), blocks
}

func TestHighCapCreation(t *testing.T) {
	for _, tc := range []struct {
		name    string
		profile HeapProfile
		size    uint64
	}{
		{"high64", HeapProfileHigh64, segDBHeapSizeHigh64},
		{"high256", HeapProfileHigh256, segDBHeapSizeHigh256},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			db := persistOpenForTest(t, highCapWasm64, dir, PersistedOptions{HeapProfile: tc.profile, Durability: DurabilitySync})
			if db.HeapSize() != tc.size {
				t.Fatalf("HeapSize %d, want %d", db.HeapSize(), tc.size)
			}
			want := map[string]string{}
			for i := 0; i < 200; i++ {
				k := fmt.Sprintf("hc-%03d", i)
				want[k] = fmt.Sprintf("v-%03d", i)
				if err := db.Put(k, []byte(want[k])); err != nil {
					t.Fatalf("Put %s: %v", k, err)
				}
			}
			if err := db.Flush(); err != nil {
				t.Fatalf("Flush: %v", err)
			}
			if ck := highCapCheckpoint(t, dir); ck.HeapSize != tc.size {
				t.Fatalf("checkpoint heap_size %d, want %d", ck.HeapSize, tc.size)
			}
			if err := db.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			// Reopen with Default: the stored size must
			// survive (never shrink back to Standard).
			db2 := persistOpenForTest(t, highCapWasm64, dir, PersistedOptions{})
			defer db2.Close()
			if db2.HeapSize() != tc.size {
				t.Fatalf("reopen HeapSize %d, want stored %d", db2.HeapSize(), tc.size)
			}
			highCapMap(t, db2, want)
		})
	}
}

func TestHighCapStandardUnchanged(t *testing.T) {
	dir := t.TempDir()
	db := persistOpenForTest(t, highCapWasm64, dir, PersistedOptions{Durability: DurabilitySync})
	if db.HeapSize() != segDBHeapSize {
		t.Fatalf("HeapSize %d, want Standard %d", db.HeapSize(), segDBHeapSize)
	}
	if err := db.Put("std", []byte("1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if ck := highCapCheckpoint(t, dir); ck.HeapSize != segDBHeapSize {
		t.Fatalf("checkpoint heap_size %d, want %d", ck.HeapSize, segDBHeapSize)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	db2 := persistOpenForTest(t, highCapWasm64, dir, PersistedOptions{})
	defer db2.Close()
	persistMustGet(t, db2, "std", "1")
}

func TestHighCapRefusesWasm32(t *testing.T) {
	wasm32, err := os.ReadFile(highCapWasm32)
	if err != nil {
		t.Fatal(err)
	}
	for _, profile := range []HeapProfile{HeapProfileHigh64, HeapProfileHigh256} {
		dir := t.TempDir()
		// Use a fresh subdir so "not created" is checkable:
		// validation runs before MkdirAll.
		fresh := filepath.Join(dir, "fresh")
		if _, err := OpenPersistedSegDB(fresh, wasm32, PersistedOptions{HeapProfile: profile}); !errors.Is(err, ErrHighCapacityRequiresWasm64) {
			t.Fatalf("profile %d on wasm32: err %v, want ErrHighCapacityRequiresWasm64", profile, err)
		}
		if _, err := os.Stat(fresh); !os.IsNotExist(err) {
			t.Fatalf("profile %d on wasm32 created %s, want no directory", profile, fresh)
		}
	}
	// Direct SegDB level: same refusal, never a silent
	// Standard heap.
	host, err := NewYowzaDB(wasm32)
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	if _, err := NewSegDBWithHeapSize(host, segDBHeapSizeHigh64); !errors.Is(err, ErrHighCapacityRequiresWasm64) {
		t.Fatalf("NewSegDBWithHeapSize wasm32: err %v, want ErrHighCapacityRequiresWasm64", err)
	}
	if err := host.RestoreSegDBHeapToSize(make([]byte, segDBHeapSizeHigh64), segDBHeapSizeHigh64); !errors.Is(err, ErrHighCapacityRequiresWasm64) {
		t.Fatalf("RestoreSegDBHeapToSize wasm32: err %v, want ErrHighCapacityRequiresWasm64", err)
	}
	// Unknown profile is rejected too.
	wasm64, err := os.ReadFile(highCapWasm64)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := OpenPersistedSegDB(t.TempDir(), wasm64, PersistedOptions{HeapProfile: HeapProfile(99)}); !errors.Is(err, ErrInvalidHeapProfile) {
		t.Fatalf("unknown profile: err %v, want ErrInvalidHeapProfile", err)
	}
}

func TestHighCapLoaderSmallerExtends(t *testing.T) {
	dir := t.TempDir()
	db := persistOpenForTest(t, highCapWasm64, dir, PersistedOptions{Durability: DurabilitySync})
	want := map[string]string{}
	for i := 0; i < 50; i++ {
		k := fmt.Sprintf("up-%02d", i)
		want[k] = fmt.Sprintf("v-%02d", i)
		if err := db.Put(k, []byte(want[k])); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Reopen with a larger profile: the 8 MiB image
	// extends to 64 MiB (SHA verified over the stored
	// 8 MiB bytes first, inside Open).
	db2 := persistOpenForTest(t, highCapWasm64, dir, PersistedOptions{HeapProfile: HeapProfileHigh64, Durability: DurabilitySync})
	if db2.HeapSize() != segDBHeapSizeHigh64 {
		t.Fatalf("upgraded HeapSize %d, want %d", db2.HeapSize(), segDBHeapSizeHigh64)
	}
	highCapMap(t, db2, want)
	if err := db2.Put("up-new", []byte("n")); err != nil {
		t.Fatalf("Put after upgrade: %v", err)
	}
	if err := db2.Flush(); err != nil {
		t.Fatalf("Flush after upgrade: %v", err)
	}
	if ck := highCapCheckpoint(t, dir); ck.HeapSize != segDBHeapSizeHigh64 {
		t.Fatalf("checkpoint heap_size after upgrade %d, want %d", ck.HeapSize, segDBHeapSizeHigh64)
	}
	if err := db2.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	db3 := persistOpenForTest(t, highCapWasm64, dir, PersistedOptions{})
	defer db3.Close()
	if db3.HeapSize() != segDBHeapSizeHigh64 {
		t.Fatalf("reopen HeapSize %d, want upgraded %d", db3.HeapSize(), segDBHeapSizeHigh64)
	}
	want["up-new"] = "n"
	highCapMap(t, db3, want)
}

func TestHighCapExtendImageToSize(t *testing.T) {
	// Smaller extends with zero tail, exact passes through,
	// larger is a loud error (never truncated).
	small := make([]byte, segDBHeapSizeLegacy)
	for i := range small {
		small[i] = byte(i)
	}
	ext, err := extendHeapImageToSize(small, segDBHeapSizeHigh64)
	if err != nil {
		t.Fatalf("extend 4MiB->64MiB: %v", err)
	}
	if uint64(len(ext)) != segDBHeapSizeHigh64 {
		t.Fatalf("extended len %d", len(ext))
	}
	for i := range small {
		if ext[i] != small[i] {
			t.Fatalf("extended byte %d changed", i)
		}
	}
	for i := uint64(len(small)); i < uint64(len(ext)); i++ {
		if ext[i] != 0 {
			t.Fatalf("extended tail byte %d = %d, want 0", i, ext[i])
		}
	}
	cur := make([]byte, segDBHeapSize)
	if out, err := extendHeapImageToSize(cur, segDBHeapSizeHigh256); err != nil || uint64(len(out)) != segDBHeapSizeHigh256 {
		t.Fatalf("extend 8MiB->256MiB: out %d, err %v", len(out), err)
	}
	if out, err := extendHeapImageToSize(cur, segDBHeapSize); err != nil || len(out) != len(cur) {
		t.Fatalf("exact passthrough: %v", err)
	}
	big := make([]byte, segDBHeapSizeHigh256)
	if _, err := extendHeapImageToSize(big, segDBHeapSizeHigh64); !errors.Is(err, ErrHeapImageTooLarge) {
		t.Fatalf("larger image: err %v, want ErrHeapImageTooLarge", err)
	}
}

func TestHighCapLoaderLargerRefused(t *testing.T) {
	dir := t.TempDir()
	db := persistOpenForTest(t, highCapWasm64, dir, PersistedOptions{HeapProfile: HeapProfileHigh64, Durability: DurabilitySync})
	if err := db.Put("big", []byte("1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	wasm64, err := os.ReadFile(highCapWasm64)
	if err != nil {
		t.Fatal(err)
	}
	// Explicit Standard on a 64 MiB directory: loud
	// refusal, never a silent truncate to 8 MiB.
	if _, err := OpenPersistedSegDB(dir, wasm64, PersistedOptions{HeapProfile: HeapProfileStandard}); !errors.Is(err, ErrHeapImageTooLarge) {
		t.Fatalf("Standard reopen of 64MiB DB: err %v, want ErrHeapImageTooLarge", err)
	}
	// Upgrade to 256 MiB still works after the refusal
	// (the refusal released the directory lock). The
	// Flush publishes a 256 MiB checkpoint, so the
	// stored size is 256 MiB before the next reopen
	// (an upgrade open alone does not publish).
	db2 := persistOpenForTest(t, highCapWasm64, dir, PersistedOptions{HeapProfile: HeapProfileHigh256})
	if db2.HeapSize() != segDBHeapSizeHigh256 {
		t.Fatalf("upgraded HeapSize %d", db2.HeapSize())
	}
	persistMustGet(t, db2, "big", "1")
	if err := db2.Flush(); err != nil {
		t.Fatalf("Flush at 256MiB: %v", err)
	}
	if ck := highCapCheckpoint(t, dir); ck.HeapSize != segDBHeapSizeHigh256 {
		t.Fatalf("checkpoint heap_size %d, want 256MiB", ck.HeapSize)
	}
	if err := db2.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// And 256 MiB refuses a 64 MiB loader.
	if _, err := OpenPersistedSegDB(dir, wasm64, PersistedOptions{HeapProfile: HeapProfileHigh64}); !errors.Is(err, ErrHeapImageTooLarge) {
		t.Fatalf("64MiB reopen of 256MiB DB: err %v, want ErrHeapImageTooLarge", err)
	}
	// SegDB level: a 64 MiB image into an 8 MiB loader.
	host, err := NewYowzaDB(wasm64)
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	sdb, err := NewSegDBWithHeapSize(host, segDBHeapSizeHigh64)
	if err != nil {
		t.Fatal(err)
	}
	hb, err := sdb.HeapBytes()
	if err != nil {
		t.Fatal(err)
	}
	host2, err := NewYowzaDB(wasm64)
	if err != nil {
		t.Fatal(err)
	}
	defer host2.Close()
	if err := host2.RestoreSegDBHeapToSize(hb, segDBHeapSize); !errors.Is(err, ErrHeapImageTooLarge) {
		t.Fatalf("Restore 64MiB into 8MiB loader: err %v, want ErrHeapImageTooLarge", err)
	}
}

func TestHighCapLegacy4MiBExtendsTo64(t *testing.T) {
	// Hand-built legacy heap (same shape as
	// TestLegacyHeapImageMigratesOnInit): magic, 4096
	// slots, bump at the legacy data start, no live
	// slots. Into a 64 MiB wasm64 loader it must
	// migrate its table (count becomes 8192) and serve
	// Put/Get/Flush normally.
	legacy := make([]byte, segDBHeapSizeLegacy)
	copy(legacy, "YWHEAP01")
	binary.LittleEndian.PutUint32(legacy[8:12], 4096)
	binary.LittleEndian.PutUint64(legacy[16:24], 622624)
	wasm64, err := os.ReadFile(highCapWasm64)
	if err != nil {
		t.Fatal(err)
	}
	host, err := NewYowzaDB(wasm64)
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	if err := host.RestoreSegDBHeapToSize(legacy, segDBHeapSizeHigh64); err != nil {
		t.Fatalf("Restore legacy into 64MiB loader: %v", err)
	}
	sdb, err := NewSegDBWithHeapSize(host, segDBHeapSizeHigh64)
	if err != nil {
		t.Fatalf("NewSegDB 64MiB (migrated): %v", err)
	}
	hb, err := sdb.HeapBytes()
	if err != nil {
		t.Fatal(err)
	}
	if got := binary.LittleEndian.Uint32(hb[8:12]); got != 8192 {
		t.Fatalf("migrated table count %d, want 8192", got)
	}
	if err := sdb.Put("mig64", []byte("v")); err != nil {
		t.Fatalf("Put after migrate: %v", err)
	}
	if err := sdb.Flush(); err != nil {
		t.Fatalf("Flush after migrate: %v", err)
	}
	got, err := sdb.Get("mig64")
	if err != nil || string(got) != "v" {
		t.Fatalf("Get after migrate = %q, err %v", got, err)
	}
}

func TestHighCapSparseWriter(t *testing.T) {
	dir := t.TempDir()
	// Nonzero head + 10 MiB zero run + nonzero tail.
	data := make([]byte, 10<<20)
	for i := 0; i < 4096; i++ {
		data[i] = byte(i)
	}
	for i := 0; i < 4096; i++ {
		data[len(data)-4096+i] = byte(255 - i)
	}
	if err := persistWriteHeapAtomic(dir, "sparse.bin", data, DurabilitySync); err != nil {
		t.Fatalf("sparse write: %v", err)
	}
	back, err := os.ReadFile(filepath.Join(dir, "sparse.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if len(back) != len(data) || sha256.Sum256(back) != sha256.Sum256(data) {
		t.Fatal("sparse round-trip bytes differ")
	}
	apparent, blocks := highCapFileUsage(t, filepath.Join(dir, "sparse.bin"))
	t.Logf("sparse 10MiB pattern: apparent=%d blocks=%d", apparent, blocks)
	if apparent != int64(len(data)) {
		t.Fatalf("apparent %d, want %d", apparent, len(data))
	}
	if blocks >= 1<<20 {
		t.Fatalf("blocks %d, want < 1MiB for a 10MiB mostly-zero file", blocks)
	}
	if _, err := os.Stat(filepath.Join(dir, "sparse.bin.tmp")); !os.IsNotExist(err) {
		t.Fatal("tmp file left behind after sparse write")
	}
	// All-zero image: apparent size full, blocks ~zero.
	zero := make([]byte, 5<<20)
	if err := persistWriteHeapAtomic(dir, "zero.bin", zero, DurabilitySync); err != nil {
		t.Fatalf("zero sparse write: %v", err)
	}
	apparent, blocks = highCapFileUsage(t, filepath.Join(dir, "zero.bin"))
	t.Logf("sparse all-zero 5MiB: apparent=%d blocks=%d", apparent, blocks)
	if apparent != int64(len(zero)) || blocks >= 64<<10 {
		t.Fatalf("all-zero file: apparent=%d blocks=%d", apparent, blocks)
	}
}

func TestHighCapDiskUsage(t *testing.T) {
	dir := t.TempDir()
	db := persistOpenForTest(t, highCapWasm64, dir, PersistedOptions{HeapProfile: HeapProfileHigh256, Durability: DurabilitySync})
	if err := db.Flush(); err != nil {
		t.Fatalf("Flush empty: %v", err)
	}
	ck := highCapCheckpoint(t, dir)
	if len(ck.HeapFiles) != 1 {
		t.Fatalf("heap files %v", ck.HeapFiles)
	}
	heapPath := filepath.Join(dir, ck.HeapFiles[0])
	apparent, blocks := highCapFileUsage(t, heapPath)
	t.Logf("empty 256MiB DB heap file: apparent=%d blocks=%d", apparent, blocks)
	if apparent != int64(segDBHeapSizeHigh256) {
		t.Fatalf("apparent %d, want %d", apparent, segDBHeapSizeHigh256)
	}
	if blocks >= 64<<20 {
		t.Fatalf("empty 256MiB heap costs %d blocks, want < 64MiB", blocks)
	}
	for i := 0; i < 1000; i++ {
		if err := db.Put(fmt.Sprintf("d-%04d", i), []byte("small-value")); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("Flush after small load: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	ck = highCapCheckpoint(t, dir)
	heapPath = filepath.Join(dir, ck.HeapFiles[0])
	apparent, blocks = highCapFileUsage(t, heapPath)
	var dirBlocks int64
	ents, _ := os.ReadDir(dir)
	for _, e := range ents {
		_, b := highCapFileUsage(t, filepath.Join(dir, e.Name()))
		dirBlocks += b
	}
	t.Logf("256MiB DB after 1000 small puts: current heap apparent=%d blocks=%d; whole dir blocks=%d", apparent, blocks, dirBlocks)
	if apparent != int64(segDBHeapSizeHigh256) {
		t.Fatalf("apparent %d, want %d", apparent, segDBHeapSizeHigh256)
	}
	if blocks >= 64<<20 {
		t.Fatalf("small-load 256MiB heap costs %d blocks, want < 64MiB", blocks)
	}
}

func TestHighCapHeapInfo(t *testing.T) {
	dir := t.TempDir()
	db := persistOpenForTest(t, highCapWasm64, dir, PersistedOptions{HeapProfile: HeapProfileHigh64, Durability: DurabilityNoSync})
	defer db.Close()
	for i := 0; i < 100; i++ {
		if err := db.Put(fmt.Sprintf("hi-%03d", i), []byte("v")); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	info, err := db.HeapInfo()
	if err != nil {
		t.Fatalf("HeapInfo: %v", err)
	}
	t.Logf("heap info: size=%d bump=%d live=%d slots=%d wasted=%d", info.HeapSize, info.Bump, info.Live, info.Slots, info.Wasted)
	if info.HeapSize != segDBHeapSizeHigh64 || info.Bump == 0 || info.Live == 0 || info.Slots == 0 {
		t.Fatalf("implausible heap info: %+v", info)
	}
}

// highCapBenchWorkload is the documented write-heavy
// workload for the generation benchmark: 3800 distinct
// keys, 512-byte values, 6 rounds of full overwrites =
// 22,800 puts. The driver flushes on hot-WAL-full
// (rc=-6: the 64 KiB hot WAL, not the heap) and reports
// a heap-exhaustion brick when the flush itself fails;
// it does NOT compact after a brick, because compaction
// needs merge headroom that no longer exists at that
// point (measured: compact also fails rc=-1 once
// bricked). Generations are checkpoint generations
// (Generation()).
func highCapBenchWorkload(t *testing.T, db *PersistedSegDB, rounds int) (completed bool, brickGen uint64, okMap map[string]string) {
	t.Helper()
	val := make([]byte, 512)
	for i := range val {
		val[i] = 'x'
	}
	okMap = map[string]string{}
	for round := 0; round < rounds; round++ {
		for k := 0; k < 3800; k++ {
			key := fmt.Sprintf("k-%04d", k)
			if err := db.Put(key, val); err != nil {
				if ferr := db.Flush(); ferr != nil {
					// Brick: the failed put's WAL record
					// was aborted, so okMap is exactly
					// the reopenable state (modulo WAL
					// replay, proven by the caller).
					return false, db.Generation(), okMap
				}
				if err := db.Put(key, val); err != nil {
					t.Fatalf("put after flush failed: %v", err)
				}
			}
			okMap[key] = string(val)
		}
	}
	return true, db.Generation(), okMap
}

func highCapBenchWant() map[string]string {
	val := strings.Repeat("x", 512)
	want := make(map[string]string, 3800)
	for k := 0; k < 3800; k++ {
		want[fmt.Sprintf("k-%04d", k)] = val
	}
	return want
}

// highCapMapStream verifies a full map through the PAGED
// streaming scan (ScanStreaming + NextBatch), which is
// the API designed for result sets larger than the
// one-shot Scan scratch-out. Batch size 32 keeps every
// page under the 32 KiB scratch-out even for 512-byte
// values (32 x ~526 B ≈ 17 KiB).
func highCapMapStream(t *testing.T, db *PersistedSegDB, want map[string]string) {
	t.Helper()
	vec, _, err := db.SnapshotVector()
	if err != nil {
		t.Fatalf("SnapshotVector: %v", err)
	}
	it, err := db.ScanStreaming("", vec)
	if err != nil {
		t.Fatalf("ScanStreaming: %v", err)
	}
	defer it.Close()
	got := map[string]string{}
	for {
		batch, err := it.NextBatch(32)
		if err != nil {
			t.Fatalf("NextBatch: %v", err)
		}
		if len(batch) == 0 {
			break
		}
		for _, e := range batch {
			got[string(e.Key)] = string(e.Val)
		}
	}
	if len(got) != len(want) {
		t.Fatalf("streaming scan found %d keys, want %d", len(got), len(want))
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("streaming scan key %q = %q, want %q", k, got[k], v)
		}
	}
}

func TestHighCapGenerationBenchmark(t *testing.T) {
	// 8 MiB leg: expected to BRICK (heap exhaustion),
	// not to complete — see WORKER_S_NOTES.md §5.
	dir8 := t.TempDir()
	db8 := persistOpenForTest(t, highCapWasm64, dir8, PersistedOptions{HeapProfile: HeapProfileStandard, Durability: DurabilityNoSync, WALRotateBytes: -1})
	completed8, brickGen, okMap8 := highCapBenchWorkload(t, db8, 6)
	info8, _ := db8.HeapInfo()
	t.Logf("BENCH 8MiB: completed=%v brickGen=%d heapInfo=%+v okKeys=%d", completed8, brickGen, info8, len(okMap8))
	if completed8 {
		t.Fatalf("8MiB workload completed unexpectedly (gen %d); the brick is the measured result", brickGen)
	}
	if err := db8.Close(); err != nil {
		t.Fatalf("Close 8MiB: %v", err)
	}
	// The brick is not corruption: the bricked directory
	// reopens with exactly the successfully-put keys
	// (WAL replay over the last good checkpoint).
	// Get-only comparison plus the streaming scan
	// (below): one-shot Scan cannot return this result
	// set — it emits into a 32 KiB scratch-out
	// (segScratchCap) and fails with rc=-1 once the
	// output exceeds it (src/segdb.c emit loop), and
	// 3800 x 512-byte values need ~2 MB. That is a
	// pre-existing one-shot Scan capacity limit, not
	// heap exhaustion or data loss: Gets work, and the
	// paged streaming scan returns the full map.
	db8r := persistOpenForTest(t, highCapWasm64, dir8, PersistedOptions{})
	highCapMapGets(t, db8r, okMap8)
	highCapMapStream(t, db8r, okMap8)
	if err := db8r.Close(); err != nil {
		t.Fatalf("Close 8MiB reopen: %v", err)
	}

	// 64 MiB and 256 MiB legs: must complete with the
	// same generation count (flush-driven, heap-size
	// invariant) and survive a reopen with full key
	// comparison.
	gens := map[HeapProfile]uint64{}
	for _, tc := range []struct {
		name    string
		profile HeapProfile
	}{
		{"high64", HeapProfileHigh64},
		{"high256", HeapProfileHigh256},
	} {
		dir := t.TempDir()
		db := persistOpenForTest(t, highCapWasm64, dir, PersistedOptions{HeapProfile: tc.profile, Durability: DurabilityNoSync, WALRotateBytes: -1})
		completed, gen, _ := highCapBenchWorkload(t, db, 6)
		if !completed {
			t.Fatalf("%s workload bricked at gen %d, want completion", tc.name, gen)
		}
		gens[tc.profile] = gen
		info, _ := db.HeapInfo()
		t.Logf("BENCH %s: completed gen=%d heapInfo=%+v", tc.name, gen, info)
		if err := db.Close(); err != nil {
			t.Fatalf("Close %s: %v", tc.name, err)
		}
		db2 := persistOpenForTest(t, highCapWasm64, dir, PersistedOptions{})
		// Full verification, in two stages. As-reopened,
		// Get-per-key proves every key/value exactly:
		// no scan API can return this state, which is
		// TWO pre-existing engine caps, not data loss.
		// One-shot Scan emits into a 32 KiB
		// scratch-out (segScratchCap) and fails once
		// the ~2 MB result set exceeds it (src/segdb.c
		// emit loop). The cursor/skip paging scans
		// refuse more than SEGDB_STREAM_SOURCES (128)
		// manifest segments summed over all groups
		// (src/segdb.c); measured on the reopened
		// 64 MiB DB: 2 groups, last group 55
		// segments, cursor fails rc=-1 as-reopened.
		// After Compact collapses the structure
		// (last-group count 55 -> 1), the streaming
		// scan verifies the full map — proving both
		// that the data is intact and that the
		// as-reopened scan failure is structural.
		// Raising engine scan caps is Track C work,
		// out of scope here.
		highCapMapGets(t, db2, highCapBenchWant())
		if err := db2.Compact(0); err != nil {
			t.Fatalf("Compact %s: %v", tc.name, err)
		}
		highCapMapStream(t, db2, highCapBenchWant())
		if err := db2.Close(); err != nil {
			t.Fatalf("Close %s reopen: %v", tc.name, err)
		}
	}
	if gens[HeapProfileHigh64] != gens[HeapProfileHigh256] {
		t.Fatalf("generation counts differ: 64MiB=%d 256MiB=%d, want equal (flush-driven)", gens[HeapProfileHigh64], gens[HeapProfileHigh256])
	}
	if brickGen >= gens[HeapProfileHigh64] {
		t.Fatalf("8MiB brick gen %d not below completed gen %d", brickGen, gens[HeapProfileHigh64])
	}
	t.Logf("BENCH summary: 8MiB bricked at gen %d (incomplete); 64MiB gen %d; 256MiB gen %d", brickGen, gens[HeapProfileHigh64], gens[HeapProfileHigh256])
}

func TestHighCapCrashBetweenPublishes(t *testing.T) {
	for _, tc := range []struct {
		name    string
		profile HeapProfile
	}{
		{"high64", HeapProfileHigh64},
		{"high256", HeapProfileHigh256},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			db := persistOpenForTest(t, highCapWasm64, dir, PersistedOptions{HeapProfile: tc.profile, Durability: DurabilitySync})
			want := map[string]string{}
			for i := 0; i < 100; i++ {
				k := fmt.Sprintf("a-%03d", i)
				want[k] = "old"
				if err := db.Put(k, []byte("old")); err != nil {
					t.Fatalf("Put: %v", err)
				}
			}
			if err := db.Flush(); err != nil {
				t.Fatalf("Flush gen1: %v", err)
			}
			// Second wave: no flush — Close without a
			// checkpoint is the crash-between-publishes
			// shape (Close never checkpoints; only the
			// synced WAL covers these writes).
			for i := 0; i < 100; i++ {
				k := fmt.Sprintf("b-%03d", i)
				want[k] = "new"
				if err := db.Put(k, []byte("new")); err != nil {
					t.Fatalf("Put b: %v", err)
				}
			}
			for i := 0; i < 20; i++ {
				k := fmt.Sprintf("a-%03d", i)
				delete(want, k)
				if err := db.Delete(k); err != nil {
					t.Fatalf("Delete: %v", err)
				}
			}
			if err := db.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			db2 := persistOpenForTest(t, highCapWasm64, dir, PersistedOptions{})
			defer db2.Close()
			highCapMap(t, db2, want)
		})
	}
}

func TestHighCapCrashMidPublish(t *testing.T) {
	// Fault-hook kills at every publish stage on a
	// 64 MiB DB. After each kill, reopen must present
	// exactly ONE whole generation's map — never a
	// per-key mix of old and new. With Sync WAL, the
	// expected side is the NEW map in every case: a
	// kill before publish leaves the old checkpoint,
	// and WAL replay then advances it to the new
	// writes; a kill after the checkpoint rename
	// leaves the new checkpoint directly. (An "old
	// map" outcome would require WAL loss as well,
	// which is a different failure than a torn
	// publish.) So the test asserts: reopen succeeds,
	// and the whole map equals the new map exactly.
	stages := []string{
		"heap-write", "heap-sync", "heap-rename",
		"checkpoint-write", "checkpoint-sync", "checkpoint-rename",
		"checkpoint-dir-sync",
	}
	for _, stage := range stages {
		t.Run(stage, func(t *testing.T) {
			dir := t.TempDir()
			db := persistOpenForTest(t, highCapWasm64, dir, PersistedOptions{HeapProfile: HeapProfileHigh64, Durability: DurabilitySync})
			oldMap := map[string]string{}
			for i := 0; i < 50; i++ {
				k := fmt.Sprintf("m-%02d", i)
				oldMap[k] = "old"
				if err := db.Put(k, []byte("old")); err != nil {
					t.Fatalf("Put: %v", err)
				}
			}
			if err := db.Flush(); err != nil {
				t.Fatalf("Flush gen1: %v", err)
			}
			newMap := map[string]string{}
			for k, v := range oldMap {
				newMap[k] = v
			}
			for i := 0; i < 25; i++ {
				k := fmt.Sprintf("m-%02d", i)
				newMap[k] = "new"
				if err := db.Put(k, []byte("new")); err != nil {
					t.Fatalf("Put new: %v", err)
				}
			}
			for i := 50; i < 75; i++ {
				k := fmt.Sprintf("m-%02d", i)
				newMap[k] = "added"
				if err := db.Put(k, []byte("added")); err != nil {
					t.Fatalf("Put added: %v", err)
				}
			}
			persistFaultHook = func(point string) error {
				if point == stage {
					return fmt.Errorf("injected fault at %s", point)
				}
				return nil
			}
			defer func() { persistFaultHook = nil }()
			flushErr := db.Flush()
			persistFaultHook = nil
			if flushErr == nil {
				t.Fatalf("Flush with injected %s fault succeeded, want error", stage)
			}
			t.Logf("stage %s: flush err: %v (gen in-memory %d)", stage, flushErr, db.Generation())
			if err := db.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			db2 := persistOpenForTest(t, highCapWasm64, dir, PersistedOptions{})
			defer db2.Close()
			// Whole-map equality to exactly one side.
			equals := func(want map[string]string) bool {
				entries, err := db2.Scan("")
				if err != nil || len(entries) != len(want) {
					return false
				}
				for _, e := range entries {
					if want[string(e.Key)] != string(e.Val) {
						return false
					}
				}
				return true
			}
			isOld, isNew := equals(oldMap), equals(newMap)
			if isOld && isNew {
				t.Fatalf("stage %s: old and new maps both match (test bug: maps not distinct)", stage)
			}
			// A mix equals neither side; a torn publish
			// would land here (or fail the open).
			if !isOld && !isNew {
				t.Fatalf("stage %s: reopened map equals neither old nor new map: torn generation presented", stage)
			}
			// With synced WAL the non-torn outcome is
			// always the new map (see the test header).
			if !isNew {
				t.Fatalf("stage %s: reopened map is the OLD map, but synced WAL replay should advance it to the new map", stage)
			}
		})
	}
}

func TestHighCapTornHeapFallsBack(t *testing.T) {
	// A heap file torn under a published checkpoint's
	// name (simulated by flipping a byte after a
	// successful publish) must never be presented as
	// valid: open falls back to the previous generation
	// and WAL replay still yields exactly the new map.
	dir := t.TempDir()
	db := persistOpenForTest(t, highCapWasm64, dir, PersistedOptions{HeapProfile: HeapProfileHigh64, Durability: DurabilitySync})
	want := map[string]string{}
	for i := 0; i < 50; i++ {
		k := fmt.Sprintf("t-%02d", i)
		want[k] = "one"
		if err := db.Put(k, []byte("one")); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("Flush gen1: %v", err)
	}
	for i := 0; i < 50; i++ {
		k := fmt.Sprintf("t-%02d", i)
		want[k] = "two"
		if err := db.Put(k, []byte("two")); err != nil {
			t.Fatalf("Put two: %v", err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("Flush gen2: %v", err)
	}
	ck := highCapCheckpoint(t, dir)
	if len(ck.HeapFiles) != 1 {
		t.Fatalf("heap files %v", ck.HeapFiles)
	}
	heapPath := filepath.Join(dir, ck.HeapFiles[0])
	raw, err := os.ReadFile(heapPath)
	if err != nil {
		t.Fatal(err)
	}
	// Flip a byte in the live region (past the 1.25 MB
	// table) so the SHA check must fail.
	raw[2000000] ^= 0xff
	if err := os.WriteFile(heapPath, raw, 0644); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	db2 := persistOpenForTest(t, highCapWasm64, dir, PersistedOptions{})
	defer db2.Close()
	if !db2.RecoveredViaFallback() {
		t.Fatalf("torn heap did not trigger fallback (gen %d)", db2.Generation())
	}
	t.Logf("torn heap: fallback to gen %d, map intact", db2.FallbackGeneration())
	highCapMap(t, db2, want)
}

// highCapPublishNumbers measures one publish's split:
// HeapBytes copy, SHA-256, sparse write, and the
// validation re-read + SHA, for a mostly-empty heap
// (100 small keys) at the given size.
type highCapPublishSplit struct {
	heapBytes time.Duration
	sha       time.Duration
	write     time.Duration
	reReadSHA time.Duration
	flush     time.Duration
}

func highCapMeasurePublishSplit(t *testing.T, db *PersistedSegDB, reps int) highCapPublishSplit {
	t.Helper()
	sh := db.shards[0]
	var out highCapPublishSplit
	for i := 0; i < reps; i++ {
		t0 := time.Now()
		hb, err := sh.sdb.HeapBytes()
		if err != nil {
			t.Fatal(err)
		}
		out.heapBytes += time.Since(t0)
		t0 = time.Now()
		sum := sha256.Sum256(hb)
		_ = sum
		out.sha += time.Since(t0)
		t0 = time.Now()
		scratch := t.TempDir()
		if err := persistWriteHeapAtomic(scratch, "split.bin", hb, DurabilitySync); err != nil {
			t.Fatal(err)
		}
		out.write += time.Since(t0)
		t0 = time.Now()
		back, err := os.ReadFile(filepath.Join(scratch, "split.bin"))
		if err != nil {
			t.Fatal(err)
		}
		if sha256.Sum256(back) != sum {
			t.Fatal("split round-trip SHA mismatch")
		}
		out.reReadSHA += time.Since(t0)
	}
	for i := 0; i < reps; i++ {
		t0 := time.Now()
		if err := db.Flush(); err != nil {
			t.Fatal(err)
		}
		out.flush += time.Since(t0)
	}
	n := time.Duration(reps)
	out.heapBytes /= n
	out.sha /= n
	out.write /= n
	out.reReadSHA /= n
	out.flush /= n
	return out
}

func highCapPercentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(p/100*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// TestHighCapPublishLatency measures the publish stall
// visible to writers (gate 6) BEFORE any publish-path
// optimization: a writer goroutine issuing 500 distinct
// small puts while the main goroutine runs 10 back-to-back
// Flushes, plus the hash/write split. SPIKE BUDGET (set
// before measuring, see WORKER_S_NOTES.md §6): p99 writer
// stall <= 100 ms (interactive facade: a single Put
// blocking longer is user-perceptible; batch ingest
// shares the facade, so the interactive bound governs).
// If p99 exceeds the budget for a shipped heap size, a
// publish-path improvement is required (this test then
// documents the before-numbers; the optimization lands
// with its own re-measurement test).
// TestHighCapPublishBlockedPut measures the publish stall
// deterministically: a Flush is started in a goroutine,
// the test waits until it is inside the publish phase
// (sleep chosen well inside the measured publish time),
// then times a single Put. Before background publish
// the Put waited for p.mu; after it, only for the
// snapshot copy (plus its own work).
// NoSync mode isolates the publish stall from WAL fsync
// cost (Sync fsync on this VM is ~35 ms per Put and
// would swamp the signal). Before background publish,
// the blocked Put waits out (nearly) the whole publish;
// after it, it waits only for the heap snapshot copy.
func TestHighCapPublishBlockedPut(t *testing.T) {
	for _, tc := range []struct {
		name    string
		profile HeapProfile
		delay   time.Duration
	}{
		{"standard8", HeapProfileStandard, 50 * time.Millisecond},
		{"high64", HeapProfileHigh64, 200 * time.Millisecond},
		{"high256", HeapProfileHigh256, 500 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			db := persistOpenForTest(t, highCapWasm64, dir, PersistedOptions{HeapProfile: tc.profile, Durability: DurabilityNoSync})
			defer db.Close()
			for i := 0; i < 100; i++ {
				if err := db.Put(fmt.Sprintf("bp-%03d", i), []byte("v")); err != nil {
					t.Fatalf("Put: %v", err)
				}
			}
			if err := db.Flush(); err != nil {
				t.Fatalf("Flush warmup: %v", err)
			}
			for i := 0; i < 5; i++ {
				done := make(chan error, 1)
				go func() { done <- db.Flush() }()
				time.Sleep(tc.delay)
				t0 := time.Now()
				err := db.Put(fmt.Sprintf("blocked-%d", i), []byte("b"))
				d := time.Since(t0)
				if err != nil {
					t.Fatalf("blocked Put: %v", err)
				}
				ferr := <-done
				if ferr != nil {
					t.Fatalf("background Flush: %v", ferr)
				}
				t.Logf("BLOCKED %s sample %d: put latency %v", tc.name, i, d)
			}
		})
	}
}

func TestHighCapPublishLatency(t *testing.T) {
	for _, tc := range []struct {
		name    string
		profile HeapProfile
		size    uint64
	}{
		{"standard8", HeapProfileStandard, segDBHeapSize},
		{"high64", HeapProfileHigh64, segDBHeapSizeHigh64},
		{"high256", HeapProfileHigh256, segDBHeapSizeHigh256},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			db := persistOpenForTest(t, highCapWasm64, dir, PersistedOptions{HeapProfile: tc.profile, Durability: DurabilitySync})
			defer db.Close()
			for i := 0; i < 100; i++ {
				if err := db.Put(fmt.Sprintf("p-%03d", i), []byte("v")); err != nil {
					t.Fatalf("Put: %v", err)
				}
			}
			if err := db.Flush(); err != nil {
				t.Fatalf("Flush warmup: %v", err)
			}
			split := highCapMeasurePublishSplit(t, db, 3)
			t.Logf("PUBLISH %s (heap %d): heapBytes=%v sha=%v sparseWrite=%v reReadSHA=%v flush=%v",
				tc.name, tc.size, split.heapBytes, split.sha, split.write, split.reReadSHA, split.flush)
			// Writer stall: 500 distinct puts against 10
			// back-to-back flushes. 500 x ~50B << 64KiB
			// hot WAL, so no hot-WAL flush fires inside
			// the writer; before background publish
			// every stall was publish time (p.mu held
			// across the publish); after it, stalls
			// come only from the snapshot copy and
			// WAL fsync in Sync mode.
			var wg sync.WaitGroup
			latencies := make([]time.Duration, 0, 500)
			var latMu sync.Mutex
			start := make(chan struct{})
			wg.Add(2)
			go func() {
				defer wg.Done()
				<-start
				for i := 0; i < 10; i++ {
					if err := db.Flush(); err != nil {
						t.Errorf("Flush during stall test: %v", err)
						return
					}
				}
			}()
			go func() {
				defer wg.Done()
				<-start
				for i := 0; i < 500; i++ {
					t0 := time.Now()
					err := db.Put(fmt.Sprintf("stall-%04d", i), []byte("s"))
					d := time.Since(t0)
					if err != nil {
						t.Errorf("stall writer Put: %v", err)
						return
					}
					latMu.Lock()
					latencies = append(latencies, d)
					latMu.Unlock()
				}
			}()
			close(start)
			wg.Wait()
			sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
			median := highCapPercentile(latencies, 50)
			p99 := highCapPercentile(latencies, 99)
			max := time.Duration(0)
			if len(latencies) > 0 {
				max = latencies[len(latencies)-1]
			}
			t.Logf("STALL %s (heap %d): n=%d median=%v p99=%v max=%v",
				tc.name, tc.size, len(latencies), median, p99, max)
		})
	}
}
