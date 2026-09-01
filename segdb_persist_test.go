package yowza

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

var persistTestBins = []string{"build/yowza.wasm", "build/yowza64.wasm"}

func persistOpenForTest(t *testing.T, bin, dir string, opts PersistedOptions) *PersistedSegDB {
	t.Helper()
	wasmBytes, err := os.ReadFile(bin)
	if err != nil {
		t.Fatalf("read %s: %v", bin, err)
	}
	p, err := OpenPersistedSegDB(dir, wasmBytes, opts)
	if err != nil {
		t.Fatalf("OpenPersistedSegDB(%s): %v", dir, err)
	}
	return p
}

func persistMustGet(t *testing.T, p *PersistedSegDB, key, want string) {
	t.Helper()
	v, err := p.Get(key)
	if err != nil || string(v) != want {
		t.Fatalf("Get %q = %q, err %v, want %q", key, v, err, want)
	}
}

func persistMustMissing(t *testing.T, p *PersistedSegDB, key string) {
	t.Helper()
	if v, err := p.Get(key); err == nil {
		t.Fatalf("Get %q = %q, want missing", key, v)
	}
}

// (1) put + flush + close + reopen, including multi-level segments
// (flush twice, compact, reopen).
func TestPersistedFlushCloseReopen(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			if err := p.Put("a", []byte("1")); err != nil {
				t.Fatalf("Put a: %v", err)
			}
			if err := p.Put("b", []byte("2")); err != nil {
				t.Fatalf("Put b: %v", err)
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("Flush 1: %v", err)
			}
			if err := p.Put("a", []byte("10")); err != nil {
				t.Fatalf("Put a=10: %v", err)
			}
			if err := p.Delete("b"); err != nil {
				t.Fatalf("Delete b: %v", err)
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("Flush 2: %v", err)
			}
			if err := p.Compact(0); err != nil {
				t.Fatalf("Compact: %v", err)
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("Flush 3: %v", err)
			}
			if p.CheckpointSeq() == 0 {
				t.Fatalf("CheckpointSeq = 0 after flushes")
			}
			if err := p.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			p2 := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			defer p2.Close()
			persistMustGet(t, p2, "a", "10")
			persistMustMissing(t, p2, "b")
			entries, err := p2.Scan("")
			if err != nil {
				t.Fatalf("Scan: %v", err)
			}
			if len(entries) != 1 || string(entries[0].Key) != "a" || string(entries[0].Val) != "10" {
				t.Fatalf("Scan = %+v, want single a=10", entries)
			}
			counts, err := p2.SegmentCounts()
			if err != nil || len(counts) != 1 || counts[0] != 1 {
				t.Fatalf("SegmentCounts = %v, err %v, want [1]", counts, err)
			}
		})
	}
}

// (2) put WITHOUT flush + close + reopen: data present via host
// WAL replay (the persistent-WAL proof). Includes a segment-
// resident key deleted while unflushed: the replayed delete must
// tombstone-shadow the segment value.
func TestPersistedWALReplayNoFlush(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 2, Durability: DurabilitySync})
			if err := p.Put("segkey", []byte("old")); err != nil {
				t.Fatalf("Put segkey: %v", err)
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("Flush: %v", err)
			}
			if err := p.Put("hotkey", []byte("hot")); err != nil {
				t.Fatalf("Put hotkey: %v", err)
			}
			if err := p.Delete("segkey"); err != nil {
				t.Fatalf("Delete segkey: %v", err)
			}
			// No flush here: hotkey and the segkey tombstone
			// exist only in the host WAL at close time.
			if err := p.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			p2 := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 2, Durability: DurabilitySync})
			defer p2.Close()
			persistMustGet(t, p2, "hotkey", "hot")
			persistMustMissing(t, p2, "segkey")
		})
	}
}

// (3) delete a segment-resident key + flush + reopen: the old
// value must not resurface.
func TestPersistedDeleteSegmentNoResurface(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			if err := p.Put("gone", []byte("old-value")); err != nil {
				t.Fatalf("Put gone: %v", err)
			}
			if err := p.Put("kept", []byte("stay")); err != nil {
				t.Fatalf("Put kept: %v", err)
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("Flush 1: %v", err)
			}
			if err := p.Delete("gone"); err != nil {
				t.Fatalf("Delete gone: %v", err)
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("Flush 2: %v", err)
			}
			if err := p.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			p2 := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			defer p2.Close()
			persistMustMissing(t, p2, "gone")
			persistMustGet(t, p2, "kept", "stay")
		})
	}
}

// (4) corrupt/truncate the WAL tail + reopen: explicit error,
// never partial silent data; RepairWALTail + reopen recovers the
// checkpoint state plus valid pre-corruption records.
func TestPersistedCorruptWALTail(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin+"/corrupt-byte", func(t *testing.T) {
			dir := t.TempDir()
			p := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			if err := p.Put("ckpt", []byte("safe")); err != nil {
				t.Fatalf("Put ckpt: %v", err)
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("Flush: %v", err)
			}
			if err := p.Put("tail", []byte("doomed")); err != nil {
				t.Fatalf("Put tail: %v", err)
			}
			if err := p.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			wal, err := os.ReadFile(filepath.Join(dir, "wal.log"))
			if err != nil {
				t.Fatalf("read wal: %v", err)
			}
			// Flip the last byte: inside the final record's CRC.
			wal[len(wal)-1] ^= 0xFF
			if err := os.WriteFile(filepath.Join(dir, "wal.log"), wal, 0644); err != nil {
				t.Fatalf("rewrite wal: %v", err)
			}
			wasmBytes, _ := os.ReadFile(bin)
			_, err = OpenPersistedSegDB(dir, wasmBytes, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			var corruptErr *CorruptWALError
			if !errors.As(err, &corruptErr) {
				t.Fatalf("reopen after WAL corruption: err %v, want *CorruptWALError", err)
			}
			lastValid, err := RepairWALTail(dir)
			if err != nil {
				t.Fatalf("RepairWALTail: %v", err)
			}
			if lastValid == 0 {
				t.Fatalf("RepairWALTail lastValid = 0, want checkpoint seq")
			}
			p2 := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			defer p2.Close()
			persistMustGet(t, p2, "ckpt", "safe")
			persistMustMissing(t, p2, "tail")
		})
		t.Run(bin+"/truncate", func(t *testing.T) {
			dir := t.TempDir()
			p := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			if err := p.Put("ckpt", []byte("safe")); err != nil {
				t.Fatalf("Put ckpt: %v", err)
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("Flush: %v", err)
			}
			if err := p.Put("tail", []byte("doomed")); err != nil {
				t.Fatalf("Put tail: %v", err)
			}
			if err := p.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			st, err := os.Stat(filepath.Join(dir, "wal.log"))
			if err != nil {
				t.Fatalf("stat wal: %v", err)
			}
			if err := os.Truncate(filepath.Join(dir, "wal.log"), st.Size()-5); err != nil {
				t.Fatalf("truncate wal: %v", err)
			}
			wasmBytes, _ := os.ReadFile(bin)
			_, err = OpenPersistedSegDB(dir, wasmBytes, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			var corruptErr *CorruptWALError
			if !errors.As(err, &corruptErr) {
				t.Fatalf("reopen after WAL truncation: err %v, want *CorruptWALError", err)
			}
			if _, err := RepairWALTail(dir); err != nil {
				t.Fatalf("RepairWALTail: %v", err)
			}
			p2 := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			defer p2.Close()
			persistMustGet(t, p2, "ckpt", "safe")
			persistMustMissing(t, p2, "tail")
		})
	}
}

// (5) corrupt a segment/manifest object (the heap image) or the
// checkpoint + reopen: explicit failure, never silent partial
// reads.
func TestPersistedCorruptHeapOrCheckpoint(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin+"/heap", func(t *testing.T) {
			dir := t.TempDir()
			p := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			if err := p.Put("a", []byte("1")); err != nil {
				t.Fatalf("Put a: %v", err)
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("Flush: %v", err)
			}
			// HeapPath(0) names the generational file of the
			// current checkpoint (heap-<gen>-0.bin); the old
			// literal "heap-0.bin" no longer exists after the
			// generational protocol (reliability stage 01).
			heapPath := p.HeapPath(0)
			if err := p.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			heap, err := os.ReadFile(heapPath)
			if err != nil {
				t.Fatalf("read heap: %v", err)
			}
			heap[len(heap)/2] ^= 0xFF
			if err := os.WriteFile(heapPath, heap, 0644); err != nil {
				t.Fatalf("rewrite heap: %v", err)
			}
			wasmBytes, _ := os.ReadFile(bin)
			_, err = OpenPersistedSegDB(dir, wasmBytes, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			if !errors.Is(err, ErrCorruptHeap) {
				t.Fatalf("reopen after heap corruption: err %v, want ErrCorruptHeap", err)
			}
		})
		t.Run(bin+"/checkpoint", func(t *testing.T) {
			dir := t.TempDir()
			p := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			if err := p.Put("a", []byte("1")); err != nil {
				t.Fatalf("Put a: %v", err)
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("Flush: %v", err)
			}
			if err := p.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			if err := os.WriteFile(filepath.Join(dir, "checkpoint.json"), []byte("{not json"), 0644); err != nil {
				t.Fatalf("rewrite checkpoint: %v", err)
			}
			wasmBytes, _ := os.ReadFile(bin)
			_, err := OpenPersistedSegDB(dir, wasmBytes, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			if !errors.Is(err, ErrCorruptCheckpoint) {
				t.Fatalf("reopen after checkpoint corruption: err %v, want ErrCorruptCheckpoint", err)
			}
		})
	}
}

// (6) large dataset: 9000 keys x 1000-byte values = 9,000,000
// bytes of values alone, exceeding the 8,388,608-byte wasm heap
// of any single SegDB. (1000, not 1024: the hot path builds the
// mutation in a fixed 1024-byte bytecode buffer, so key+value
// must be <= 1014; see segdb_persist.go.) Sharded 4 ways,
// flushed in 75 batches (many segments), closed, reopened, and
// verified key by key.
// NOTE (integration fix, Track C follow-on): the heap was raised
// from 4,194,304 to 8,388,608 bytes when Track C doubled the wasm
// heap table, so the old 5,000,000-byte dataset no longer exceeded
// it and the test's own guard fired ("test bug"). The dataset was
// scaled UP to 9,000,000 bytes to restore the beyond-heap premise;
// the test is stricter than before, not weaker.
func TestPersistedLargeDatasetBeyondHeap(t *testing.T) {
	const nKeys = 9000
	const valSize = 1000
	const shards = 4
	// Guard up front (it does not depend on the binary): a dataset
	// that fits in the heap cannot prove beyond-heap persistence,
	// and failing here avoids building the whole dataset first.
	if totalValueBytes := nKeys * valSize; totalValueBytes <= int(segDBHeapSize) {
		t.Fatalf("test bug: total value bytes %d <= heap %d", totalValueBytes, segDBHeapSize)
	}
	// Batch is small on purpose: between flushes, each shard's
	// hot WAL is only 64 KiB, so ~60 x 1000-byte puts per shard
	// fill it (a 500-key batch overflows it at ~256 puts with
	// rc=-6). 120 keys/batch averages 30/shard, safely under.
	const batch = 120
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: shards, Durability: DurabilitySync})
			mkVal := func(i int) []byte {
				v := make([]byte, valSize)
				for j := range v {
					v[j] = byte((i + j) % 251)
				}
				return v
			}
			for start := 0; start < nKeys; start += batch {
				end := start + batch
				if end > nKeys {
					end = nKeys
				}
				for i := start; i < end; i++ {
					if err := p.Put(fmt.Sprintf("big-%05d", i), mkVal(i)); err != nil {
						t.Fatalf("Put %d: %v", i, err)
					}
				}
				if err := p.Flush(); err != nil {
					t.Fatalf("Flush batch %d: %v", start/batch, err)
				}
			}
			counts, err := p.SegmentCounts()
			if err != nil {
				t.Fatalf("SegmentCounts: %v", err)
			}
			totalSegs := 0
			for _, c := range counts {
				totalSegs += c
			}
			if totalSegs < 100 {
				t.Fatalf("total segments = %d, want >= 100 (many segments)", totalSegs)
			}
			if err := p.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			p2 := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: shards, Durability: DurabilitySync})
			defer p2.Close()
			for i := 0; i < nKeys; i++ {
				v, err := p2.Get(fmt.Sprintf("big-%05d", i))
				if err != nil {
					t.Fatalf("Get %d: %v", i, err)
				}
				want := mkVal(i)
				if len(v) != len(want) {
					t.Fatalf("Get %d len %d, want %d", i, len(v), len(want))
				}
				for j := range want {
					if v[j] != want[j] {
						t.Fatalf("Get %d byte %d = %d, want %d", i, j, v[j], want[j])
					}
				}
			}
		})
	}
}

// NoSync mode round-trips the same data, and a shard-count
// mismatch against a stored directory is refused explicitly.
func TestPersistedNoSyncAndShardMismatch(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilityNoSync})
			if err := p.Put("x", []byte("y")); err != nil {
				t.Fatalf("Put x: %v", err)
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("Flush: %v", err)
			}
			if err := p.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			p2 := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilityNoSync})
			persistMustGet(t, p2, "x", "y")
			if err := p2.Close(); err != nil {
				t.Fatalf("Close 2: %v", err)
			}
			wasmBytes, _ := os.ReadFile(bin)
			_, err := OpenPersistedSegDB(dir, wasmBytes, PersistedOptions{NumShards: 2, Durability: DurabilityNoSync})
			if !errors.Is(err, ErrPersistedShardCount) {
				t.Fatalf("reopen with wrong shard count: err %v, want ErrPersistedShardCount", err)
			}
		})
	}
}
