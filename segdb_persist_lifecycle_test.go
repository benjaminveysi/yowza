package yowza

// GATE 2 — Crash-safe storage lifecycle tests.
//
// Every test runs against both wasm binaries (persistTestBins).
// Helpers (persistOpenForTest, persistMustGet,
// persistMustMissing, persistFailOnce, persistReadFile,
// persistDirNames) live in segdb_persist_crash_test.go.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func lifecycleOpts(shards int) PersistedOptions {
	return PersistedOptions{NumShards: shards, Durability: DurabilitySync, KeepPreviousGeneration: true}
}

// shardIndexOf returns which shard key routes to, by pointer
// comparison with the DB's shard list.
func shardIndexOf(p *PersistedSegDB, key string) int {
	target := p.shardFor(key)
	for i, sh := range p.shards {
		if sh == target {
			return i
		}
	}
	return -1
}

// findKeyForShard returns a key routing to shard want.
func findKeyForShard(t *testing.T, p *PersistedSegDB, want int, prefix string) string {
	t.Helper()
	for i := 0; i < 10000; i++ {
		k := fmt.Sprintf("%s-%d", prefix, i)
		if shardIndexOf(p, k) == want {
			return k
		}
	}
	t.Fatalf("no key found routing to shard %d", want)
	return ""
}

// verifyCheckpointHashes reads checkpoint.json and checks
// every named heap file hashes to its recorded SHA — the
// full-checksum comparison path for mixed checkpoints.
func verifyCheckpointHashes(t *testing.T, dir string) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dir, "checkpoint.json"))
	if err != nil {
		t.Fatalf("read checkpoint.json: %v", err)
	}
	var ckpt persistCheckpoint
	if err := json.Unmarshal(raw, &ckpt); err != nil {
		t.Fatalf("unmarshal checkpoint: %v", err)
	}
	files := persistCheckpointHeapFiles(&ckpt)
	if len(files) != len(ckpt.HeapSHA256) {
		t.Fatalf("checkpoint names %d files, %d shas", len(files), len(ckpt.HeapSHA256))
	}
	for i, name := range files {
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatalf("read heap %s: %v", name, err)
		}
		sum := sha256.Sum256(data)
		if hex.EncodeToString(sum[:]) != ckpt.HeapSHA256[i] {
			t.Fatalf("heap %s SHA mismatch: file %x, checkpoint %s", name, sum, ckpt.HeapSHA256[i])
		}
	}
}

// 2.1 — Explicit rotation: sealed segment renamed, new
// current segment created, reopen replays exactly.
func TestLifecycleRotateAndReopen(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p := persistOpenForTest(t, bin, dir, lifecycleOpts(2))
			defer p.Close()
			for k, v := range map[string]string{"a": "1", "b": "2", "c": "3"} {
				if err := p.Put(k, []byte(v)); err != nil {
					t.Fatalf("Put %s: %v", k, err)
				}
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("Flush gen1: %v", err)
			}
			// One write between gen1 and the rotation, so
			// the sealed segment holds a record the
			// previous checkpoint does NOT cover (seq 4 >
			// gen1 seq 3) and retention must keep it.
			// (A sealed segment fully covered by the
			// retained previous checkpoint is legitimately
			// reclaimed at rotation — see GATE2_NOTES.md.)
			if err := p.Put("d", []byte("4")); err != nil {
				t.Fatalf("Put d: %v", err)
			}
			genBefore := p.Generation()
			if err := p.RotateWAL(); err != nil {
				t.Fatalf("RotateWAL: %v", err)
			}
			if p.Generation() != genBefore+1 {
				t.Fatalf("Generation after RotateWAL = %d, want %d (rotate checkpoints first)", p.Generation(), genBefore+1)
			}
			segs := p.WALSegments()
			if len(segs) != 2 {
				t.Fatalf("WALSegments = %+v, want 2 segments", segs)
			}
			if segs[0].File != "wal-1.log" {
				t.Fatalf("sealed segment = %s, want wal-1.log", segs[0].File)
			}
			if segs[0].LastSeq != 4 {
				t.Fatalf("sealed segment lastSeq = %d, want 4", segs[0].LastSeq)
			}
			if !strings.HasSuffix(p.WALPath(), segs[1].File) {
				t.Fatalf("WALPath = %s, want current segment %s", p.WALPath(), segs[1].File)
			}
			if err := p.Put("e", []byte("5")); err != nil {
				t.Fatalf("Put e after rotate: %v", err)
			}
			if err := p.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			p2 := persistOpenForTest(t, bin, dir, lifecycleOpts(2))
			defer p2.Close()
			for k, v := range map[string]string{"a": "1", "b": "2", "c": "3", "d": "4", "e": "5"} {
				persistMustGet(t, p2, k, v)
			}
			if p2.RecoveredViaFallback() {
				t.Fatalf("reopen after clean rotation reported a fallback, want none")
			}
		})
	}
}

// 2.1 — Automatic rotation bounds the current segment
// under continuous operation (small threshold here).
func TestLifecycleAutoRotation(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			opts := lifecycleOpts(1)
			opts.WALRotateBytes = 512
			p := persistOpenForTest(t, bin, dir, opts)
			defer p.Close()
			val := strings.Repeat("x", 100)
			for i := 0; i < 30; i++ {
				if err := p.Put(fmt.Sprintf("k%02d", i), []byte(val)); err != nil {
					t.Fatalf("Put k%02d: %v", i, err)
				}
			}
			if err := p.AutoCheckpointError(); err != nil {
				t.Fatalf("AutoCheckpointError = %v, want nil", err)
			}
			if len(p.WALSegments()) < 2 {
				t.Fatalf("WALSegments = %+v, want >= 2 after auto-rotation", p.WALSegments())
			}
			if p.Generation() < 1 {
				t.Fatalf("Generation = %d, want >= 1 after auto-checkpoints", p.Generation())
			}
			if err := p.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			p2 := persistOpenForTest(t, bin, dir, opts)
			defer p2.Close()
			for i := 0; i < 30; i++ {
				persistMustGet(t, p2, fmt.Sprintf("k%02d", i), val)
			}
		})
	}
}

// 2.3 — Mixed checkpoint: the unchanged shard reuses its
// previous immutable heap file byte-for-byte; the changed
// shard gets a new file; full-checksum verification passes;
// reopen state is exact.
func TestLifecycleMixedCheckpoint(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			opts := lifecycleOpts(2)
			opts.MixedCheckpoint = true
			p := persistOpenForTest(t, bin, dir, opts)
			defer p.Close()
			k0 := findKeyForShard(t, p, 0, "shard0")
			k1 := findKeyForShard(t, p, 1, "shard1")
			if err := p.Put(k0, []byte("v0")); err != nil {
				t.Fatalf("Put %s: %v", k0, err)
			}
			if err := p.Put(k1, []byte("v1")); err != nil {
				t.Fatalf("Put %s: %v", k1, err)
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("Flush gen1: %v", err)
			}
			gen1Files := []string{p.HeapPath(0), p.HeapPath(1)}
			gen1Bytes := [][]byte{persistReadFile(t, gen1Files[0]), persistReadFile(t, gen1Files[1])}
			// Touch shard 0 only, then checkpoint.
			if err := p.Put(k0, []byte("v0b")); err != nil {
				t.Fatalf("Put %s again: %v", k0, err)
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("Flush gen2: %v", err)
			}
			if p.Generation() != 2 {
				t.Fatalf("Generation = %d, want 2", p.Generation())
			}
			if p.HeapPath(1) != gen1Files[1] {
				t.Fatalf("unchanged shard heap = %s, want reuse of %s", p.HeapPath(1), gen1Files[1])
			}
			if got := persistReadFile(t, p.HeapPath(1)); string(got) != string(gen1Bytes[1]) {
				t.Fatalf("unchanged shard heap bytes changed across mixed checkpoint")
			}
			if p.HeapPath(0) == gen1Files[0] {
				t.Fatalf("changed shard heap reused %s, want a new file", gen1Files[0])
			}
			// Full-checksum comparison path.
			verifyCheckpointHashes(t, dir)
			if err := p.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			p2 := persistOpenForTest(t, bin, dir, opts)
			defer p2.Close()
			persistMustGet(t, p2, k0, "v0b")
			persistMustGet(t, p2, k1, "v1")
		})
	}
}

// 2.2 — Deliberately corrupt the PUBLISHED generation's
// heap after a successful checkpoint: reopen must recover
// via the previous generation + WAL replay, and REPORT it.
func TestLifecycleHeapCorruptionFallback(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p := persistOpenForTest(t, bin, dir, lifecycleOpts(2))
			if err := p.Put("a", []byte("1")); err != nil {
				t.Fatalf("Put a: %v", err)
			}
			if err := p.Put("b", []byte("2")); err != nil {
				t.Fatalf("Put b: %v", err)
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("Flush gen1: %v", err)
			}
			if err := p.Put("a", []byte("10")); err != nil {
				t.Fatalf("Put a=10: %v", err)
			}
			if err := p.Put("c", []byte("3")); err != nil {
				t.Fatalf("Put c: %v", err)
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("Flush gen2: %v", err)
			}
			gen2Heap := p.HeapPath(0)
			if err := p.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			heap := persistReadFile(t, gen2Heap)
			heap[len(heap)/2] ^= 0xFF
			if err := os.WriteFile(gen2Heap, heap, 0644); err != nil {
				t.Fatalf("corrupt gen2 heap: %v", err)
			}
			wasmBytes, _ := os.ReadFile(bin)
			p2, err := OpenPersistedSegDB(dir, wasmBytes, lifecycleOpts(2))
			if err != nil {
				t.Fatalf("reopen after gen2 heap corruption: %v (want fallback recovery, not an error)", err)
			}
			defer p2.Close()
			if !p2.RecoveredViaFallback() {
				t.Fatalf("fallback NOT reported after gen2 heap corruption")
			}
			if p2.FallbackGeneration() != 1 {
				t.Fatalf("FallbackGeneration = %d, want 1", p2.FallbackGeneration())
			}
			if p2.RecoveryReason() == "" {
				t.Fatalf("RecoveryReason empty on fallback open")
			}
			// Final state = gen1 heap + WAL replay of
			// the gen2-era mutations.
			persistMustGet(t, p2, "a", "10")
			persistMustGet(t, p2, "b", "2")
			persistMustGet(t, p2, "c", "3")
			// The next checkpoint heals the directory:
			// corrupt gen2 files are unreferenced after
			// it, and a clean reopen reports no fallback.
			if err := p2.Flush(); err != nil {
				t.Fatalf("Flush after fallback: %v", err)
			}
			if err := p2.Close(); err != nil {
				t.Fatalf("Close 2: %v", err)
			}
			p3 := persistOpenForTest(t, bin, dir, lifecycleOpts(2))
			defer p3.Close()
			if p3.RecoveredViaFallback() {
				t.Fatalf("clean reopen after healing flush reported a fallback")
			}
			persistMustGet(t, p3, "a", "10")
			persistMustGet(t, p3, "c", "3")
		})
		// Without KeepPreviousGeneration there is no
		// fallback: the same corruption must still fail
		// with ErrCorruptHeap (the old behaviour).
		// O1: this pins the old regime explicitly via
		// PolicyOff — a plain default options value
		// would now resolve to ON and fall back
		// instead (see TestLifecycleDefaultRetentionFallback).
		t.Run(bin+"/no-keepprev", func(t *testing.T) {
			dir := t.TempDir()
			opts := PersistedOptions{NumShards: 1, Durability: DurabilitySync, KeepPreviousGenerationPolicy: PolicyOff}
			p := persistOpenForTest(t, bin, dir, opts)
			if err := p.Put("a", []byte("1")); err != nil {
				t.Fatalf("Put a: %v", err)
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("Flush: %v", err)
			}
			hp := p.HeapPath(0)
			if err := p.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			heap := persistReadFile(t, hp)
			heap[len(heap)/2] ^= 0xFF
			if err := os.WriteFile(hp, heap, 0644); err != nil {
				t.Fatalf("corrupt heap: %v", err)
			}
			wasmBytes, _ := os.ReadFile(bin)
			_, err := OpenPersistedSegDB(dir, wasmBytes, opts)
			if !errors.Is(err, ErrCorruptHeap) {
				t.Fatalf("reopen without KeepPrevious: err %v, want ErrCorruptHeap", err)
			}
		})
	}
}

// O1 — Defaults are ON: a plain PersistedOptions value (no
// policy field set, no legacy bool set) must retain the
// previous generation AND fall back to it after heap
// corruption, with the fallback reported. This is the
// TestLifecycleHeapCorruptionFallback scenario with the
// options left at their zero value, proving that Default
// resolves to ON rather than to the old reclaim regime.
func TestLifecycleDefaultRetentionFallback(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			def := PersistedOptions{NumShards: 2, Durability: DurabilitySync}
			p := persistOpenForTest(t, bin, dir, def)
			if !p.KeepPreviousGeneration() || !p.MixedCheckpoint() {
				t.Fatalf("default options resolved to keepPrev=%v mixed=%v, want both true",
					p.KeepPreviousGeneration(), p.MixedCheckpoint())
			}
			if err := p.Put("a", []byte("1")); err != nil {
				t.Fatalf("Put a: %v", err)
			}
			if err := p.Put("b", []byte("2")); err != nil {
				t.Fatalf("Put b: %v", err)
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("Flush gen1: %v", err)
			}
			gen1Heaps := []string{p.HeapPath(0), p.HeapPath(1)}
			if err := p.Put("a", []byte("10")); err != nil {
				t.Fatalf("Put a=10: %v", err)
			}
			if err := p.Put("c", []byte("3")); err != nil {
				t.Fatalf("Put c: %v", err)
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("Flush gen2: %v", err)
			}
			// Retention under defaults: the previous
			// generation's heaps must still be present
			// after a definite gen2 publish.
			for _, hp := range gen1Heaps {
				if _, err := os.Stat(hp); err != nil {
					t.Fatalf("gen1 heap %s missing after gen2 publish under defaults: %v", hp, err)
				}
			}
			gen2Heap := p.HeapPath(0)
			if err := p.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			heap := persistReadFile(t, gen2Heap)
			heap[len(heap)/2] ^= 0xFF
			if err := os.WriteFile(gen2Heap, heap, 0644); err != nil {
				t.Fatalf("corrupt gen2 heap: %v", err)
			}
			p2 := persistOpenForTest(t, bin, dir, def)
			defer p2.Close()
			if !p2.RecoveredViaFallback() {
				t.Fatalf("fallback NOT reported after gen2 heap corruption under defaults")
			}
			if p2.FallbackGeneration() != 1 {
				t.Fatalf("FallbackGeneration = %d, want 1", p2.FallbackGeneration())
			}
			if p2.RecoveryReason() == "" {
				t.Fatalf("RecoveryReason empty on fallback open under defaults")
			}
			persistMustGet(t, p2, "a", "10")
			persistMustGet(t, p2, "b", "2")
			persistMustGet(t, p2, "c", "3")
		})
	}
}

// O1 — Defaults are ON: a plain PersistedOptions value must
// produce a mixed checkpoint in which the unchanged shard
// really reuses the same immutable heap file. This mirrors
// TestLifecycleMixedCheckpoint with the options left at
// their zero value.
func TestLifecycleDefaultMixedCheckpoint(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			def := PersistedOptions{NumShards: 2, Durability: DurabilitySync}
			p := persistOpenForTest(t, bin, dir, def)
			defer p.Close()
			k0 := findKeyForShard(t, p, 0, "shard0")
			k1 := findKeyForShard(t, p, 1, "shard1")
			if err := p.Put(k0, []byte("v0")); err != nil {
				t.Fatalf("Put %s: %v", k0, err)
			}
			if err := p.Put(k1, []byte("v1")); err != nil {
				t.Fatalf("Put %s: %v", k1, err)
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("Flush gen1: %v", err)
			}
			gen1Files := []string{p.HeapPath(0), p.HeapPath(1)}
			gen1Bytes := [][]byte{persistReadFile(t, gen1Files[0]), persistReadFile(t, gen1Files[1])}
			// Touch shard 0 only, then checkpoint.
			if err := p.Put(k0, []byte("v0b")); err != nil {
				t.Fatalf("Put %s again: %v", k0, err)
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("Flush gen2: %v", err)
			}
			if p.Generation() != 2 {
				t.Fatalf("Generation = %d, want 2", p.Generation())
			}
			if p.HeapPath(1) != gen1Files[1] {
				t.Fatalf("unchanged shard heap = %s, want reuse of %s under defaults", p.HeapPath(1), gen1Files[1])
			}
			if got := persistReadFile(t, p.HeapPath(1)); string(got) != string(gen1Bytes[1]) {
				t.Fatalf("unchanged shard heap bytes changed across mixed checkpoint under defaults")
			}
			if p.HeapPath(0) == gen1Files[0] {
				t.Fatalf("changed shard heap reused %s, want a new file", gen1Files[0])
			}
			// Full-checksum comparison path.
			verifyCheckpointHashes(t, dir)
		})
	}
}

// O1 — Precedence rule (resolvePolicy), proven through real
// opens: an explicit Policy always beats the legacy bool;
// a legacy true still means on; a legacy false means
// "unset" (the default, ON), never "off".
func TestPersistedPolicyPrecedence(t *testing.T) {
	cases := []struct {
		name string
		opts PersistedOptions
		want bool
	}{
		{"default", PersistedOptions{NumShards: 1, Durability: DurabilitySync}, true},
		{"policyOff-beats-legacyTrue", PersistedOptions{NumShards: 1, Durability: DurabilitySync,
			KeepPreviousGenerationPolicy: PolicyOff, KeepPreviousGeneration: true}, false},
		{"policyOn-beats-legacyFalse", PersistedOptions{NumShards: 1, Durability: DurabilitySync,
			KeepPreviousGenerationPolicy: PolicyOn}, true},
		{"legacyTrue", PersistedOptions{NumShards: 1, Durability: DurabilitySync,
			KeepPreviousGeneration: true}, true},
		{"legacyFalse-is-unset", PersistedOptions{NumShards: 1, Durability: DurabilitySync,
			KeepPreviousGeneration: false}, true},
	}
	for _, bin := range persistTestBins {
		for _, tc := range cases {
			t.Run(bin+"/"+tc.name, func(t *testing.T) {
				dir := t.TempDir()
				p := persistOpenForTest(t, bin, dir, tc.opts)
				defer p.Close()
				if got := p.KeepPreviousGeneration(); got != tc.want {
					t.Fatalf("KeepPreviousGeneration() = %v, want %v (case %s)", got, tc.want, tc.name)
				}
			})
		}
	}
}

// 2.2 — Corrupt published heap AND delete the WAL segment
// needed to replay from the previous generation: the open
// must FAIL with a typed error, never return the older
// state as if it were the latest.
func TestLifecycleFallbackWithMissingReplaySegment(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p := persistOpenForTest(t, bin, dir, lifecycleOpts(1))
			if err := p.Put("a", []byte("1")); err != nil {
				t.Fatalf("Put a: %v", err)
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("Flush gen1: %v", err)
			}
			if err := p.RotateWAL(); err != nil {
				t.Fatalf("RotateWAL: %v", err)
			}
			if err := p.Put("a", []byte("10")); err != nil {
				t.Fatalf("Put a=10: %v", err)
			}
			if err := p.Put("b", []byte("2")); err != nil {
				t.Fatalf("Put b: %v", err)
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("Flush gen2: %v", err)
			}
			gen2Heap := p.HeapPath(0)
			curSeg := p.WALPath()
			if err := p.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			heap := persistReadFile(t, gen2Heap)
			heap[len(heap)/2] ^= 0xFF
			if err := os.WriteFile(gen2Heap, heap, 0644); err != nil {
				t.Fatalf("corrupt gen2 heap: %v", err)
			}
			if err := os.Remove(curSeg); err != nil {
				t.Fatalf("delete replay segment %s: %v", curSeg, err)
			}
			wasmBytes, _ := os.ReadFile(bin)
			_, err := OpenPersistedSegDB(dir, wasmBytes, lifecycleOpts(1))
			if err == nil {
				t.Fatalf("reopen with corrupt heap + missing replay segment: got success, want a typed error")
			}
			if !errors.Is(err, ErrPersistedWALGap) && !errors.Is(err, ErrCorruptHeap) && !errors.Is(err, ErrMissingWAL) {
				t.Fatalf("reopen error = %v, want WALGap/CorruptHeap/MissingWAL", err)
			}
		})
	}
}

// 2.2 — Corrupt checkpoint.json itself: recovery via
// checkpoint.prev.json + WAL replay, reported.
func TestLifecycleCheckpointCorruptFallback(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p := persistOpenForTest(t, bin, dir, lifecycleOpts(1))
			if err := p.Put("a", []byte("1")); err != nil {
				t.Fatalf("Put a: %v", err)
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("Flush gen1: %v", err)
			}
			if err := p.Put("a", []byte("10")); err != nil {
				t.Fatalf("Put a=10: %v", err)
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("Flush gen2: %v", err)
			}
			if err := p.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			if err := os.WriteFile(filepath.Join(dir, "checkpoint.json"), []byte("{not json"), 0644); err != nil {
				t.Fatalf("corrupt checkpoint.json: %v", err)
			}
			wasmBytes, _ := os.ReadFile(bin)
			p2, err := OpenPersistedSegDB(dir, wasmBytes, lifecycleOpts(1))
			if err != nil {
				t.Fatalf("reopen after checkpoint.json corruption: %v (want prev.json fallback)", err)
			}
			defer p2.Close()
			if !p2.RecoveredViaFallback() {
				t.Fatalf("fallback NOT reported after checkpoint.json corruption")
			}
			persistMustGet(t, p2, "a", "10")
		})
	}
}

// 2.1/2.2 retention — after three checkpoints, only the
// newest and the previous generation's heaps survive;
// reopen state is exact.
func TestLifecycleRetentionKeepsOnlyTwoGenerations(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p := persistOpenForTest(t, bin, dir, lifecycleOpts(1))
			defer p.Close()
			if err := p.Put("a", []byte("1")); err != nil {
				t.Fatalf("Put a: %v", err)
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("Flush gen1: %v", err)
			}
			gen1Heap := p.HeapPath(0)
			if err := p.Put("b", []byte("2")); err != nil {
				t.Fatalf("Put b: %v", err)
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("Flush gen2: %v", err)
			}
			gen2Heap := p.HeapPath(0)
			if err := p.Put("c", []byte("3")); err != nil {
				t.Fatalf("Put c: %v", err)
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("Flush gen3: %v", err)
			}
			if _, err := os.Stat(gen1Heap); !os.IsNotExist(err) {
				t.Fatalf("gen1 heap %s still present after gen3 publish (err %v)", gen1Heap, err)
			}
			if _, err := os.Stat(gen2Heap); err != nil {
				t.Fatalf("gen2 heap %s missing after gen3 publish (previous must be retained): %v", gen2Heap, err)
			}
			if err := p.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			p2 := persistOpenForTest(t, bin, dir, lifecycleOpts(1))
			defer p2.Close()
			persistMustGet(t, p2, "a", "1")
			persistMustGet(t, p2, "b", "2")
			persistMustGet(t, p2, "c", "3")
		})
	}
}

// 2.4a — Second opener and repair-under-lock are rejected.
func TestLifecycleDirectoryLock(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p := persistOpenForTest(t, bin, dir, lifecycleOpts(1))
			wasmBytes, _ := os.ReadFile(bin)
			_, err := OpenPersistedSegDB(dir, wasmBytes, lifecycleOpts(1))
			if !errors.Is(err, ErrDatabaseLocked) {
				t.Fatalf("second open: err %v, want ErrDatabaseLocked", err)
			}
			if _, err := RepairWALTail(dir); !errors.Is(err, ErrDatabaseLocked) {
				t.Fatalf("RepairWALTail under lock: err %v, want ErrDatabaseLocked", err)
			}
			if err := p.Put("a", []byte("1")); err != nil {
				t.Fatalf("Put on locked DB: %v", err)
			}
			if err := p.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			// Lock released: reopen and repair both work.
			p2 := persistOpenForTest(t, bin, dir, lifecycleOpts(1))
			defer p2.Close()
			persistMustGet(t, p2, "a", "1")
		})
	}
}

// 2.1/negative — fault injection at every rotation stage
// and at the prev-checkpoint stage: reopen always yields
// the old valid or the new valid state, never partial.
func TestLifecycleRotateFaultPoints(t *testing.T) {
	points := []string{persistFaultRotateRename, persistFaultRotateCreate, persistFaultRotateDirSync, persistFaultPrevWrite}
	for _, bin := range persistTestBins {
		for _, point := range points {
			t.Run(bin+"/"+point, func(t *testing.T) {
				dir := t.TempDir()
				p := persistOpenForTest(t, bin, dir, lifecycleOpts(1))
				if err := p.Put("a", []byte("1")); err != nil {
					t.Fatalf("Put a: %v", err)
				}
				if err := p.Flush(); err != nil {
					t.Fatalf("Flush gen1: %v", err)
				}
				expectedA := "1"
				if point == persistFaultPrevWrite {
					// Prev-write fault fires during the
					// next Flush (keepPrev mode writes
					// checkpoint.prev.json before publish).
					if err := p.Put("a", []byte("10")); err != nil {
						t.Fatalf("Put a=10: %v", err)
					}
					expectedA = "10"
					persistFailOnce(t, point)
					if err := p.Flush(); err == nil {
						t.Fatalf("Flush with injected %s fault: want error, got nil", point)
					}
					if p.Generation() != 1 {
						t.Fatalf("Generation after prev-write fault = %d, want 1 (old checkpoint intact)", p.Generation())
					}
					if err := p.Flush(); err != nil {
						t.Fatalf("retry Flush after prev-write fault: %v", err)
					}
				} else {
					persistFailOnce(t, point)
					if err := p.RotateWAL(); err == nil {
						t.Fatalf("RotateWAL with injected %s fault: want error, got nil", point)
					}
					// rotate-dir-sync fires after the
					// switch: the new segment is already
					// the in-process truth, so a retry
					// rotation is a (harmless) no-op over
					// an empty segment; other faults
					// leave rotation retryable.
					if err := p.RotateWAL(); err != nil {
						t.Fatalf("retry RotateWAL after %s fault: %v", point, err)
					}
				}
				if err := p.Put("b", []byte("2")); err != nil {
					t.Fatalf("Put b after fault recovery: %v", err)
				}
				if err := p.Close(); err != nil {
					t.Fatalf("Close: %v", err)
				}
				p2 := persistOpenForTest(t, bin, dir, lifecycleOpts(1))
				defer p2.Close()
				persistMustGet(t, p2, "a", expectedA)
				persistMustGet(t, p2, "b", "2")
			})
		}
	}
}

// Negative — a segment renamed to a wrong first sequence
// must fail the open, never replay short.
func TestLifecycleInvalidSegmentSequence(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p := persistOpenForTest(t, bin, dir, lifecycleOpts(1))
			if err := p.Put("a", []byte("1")); err != nil {
				t.Fatalf("Put a: %v", err)
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("Flush: %v", err)
			}
			// A record between gen1 and the rotation, so
			// the sealed segment is NOT fully covered by
			// the retained previous checkpoint and survives
			// reclaim (see the rotate test's note).
			if err := p.Put("b", []byte("2")); err != nil {
				t.Fatalf("Put b: %v", err)
			}
			if err := p.RotateWAL(); err != nil {
				t.Fatalf("RotateWAL: %v", err)
			}
			segs := p.WALSegments()
			if len(segs) != 2 {
				t.Fatalf("WALSegments = %+v, want 2", segs)
			}
			if err := p.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			// Rename the sealed segment so its name
			// claims a first sequence its contents
			// do not have.
			if err := os.Rename(filepath.Join(dir, segs[0].File), filepath.Join(dir, "wal-99.log")); err != nil {
				t.Fatalf("rename segment: %v", err)
			}
			wasmBytes, _ := os.ReadFile(bin)
			_, err := OpenPersistedSegDB(dir, wasmBytes, lifecycleOpts(1))
			if err == nil {
				t.Fatalf("open with misnamed segment: got success, want an error")
			}
			if !errors.Is(err, ErrWALSegment) {
				t.Fatalf("open error = %v, want ErrWALSegment", err)
			}
		})
	}
}

// A WAL shorter than the checkpoint is tolerated when
// the heap covers the missing range (the pre-Gate-2
// tolerance, preserved): truncating the WAL to a record
// boundary below the checkpoint sequence still opens
// with the CORRECT full state — never a partial one.
// (Deleting an UNcheckpointed tail is a different,
// pre-existing limit: without a head pointer anywhere,
// a cleanly deleted tail is undetectable — see
// GATE2_NOTES.md. The gap error fires where detection
// IS possible: the fallback range, covered by
// TestLifecycleFallbackWithMissingReplaySegment.)
func TestLifecycleShortWALTolerated(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p := persistOpenForTest(t, bin, dir, lifecycleOpts(1))
			if err := p.Put("a", []byte("1")); err != nil {
				t.Fatalf("Put a: %v", err)
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("Flush gen1: %v", err)
			}
			// Boundary after a's record (the WAL holds
			// exactly one record at this point).
			st, err := os.Stat(p.WALPath())
			if err != nil {
				t.Fatalf("stat WAL: %v", err)
			}
			boundary := st.Size()
			if err := p.Put("b", []byte("2")); err != nil {
				t.Fatalf("Put b: %v", err)
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("Flush gen2: %v", err)
			}
			walPath := p.WALPath()
			if err := p.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			// Truncate b's record away: the WAL now ends
			// below the gen2 checkpoint sequence, but
			// the gen2 heap already covers b.
			if err := os.Truncate(walPath, boundary); err != nil {
				t.Fatalf("truncate WAL: %v", err)
			}
			wasmBytes, _ := os.ReadFile(bin)
			p2, err := OpenPersistedSegDB(dir, wasmBytes, lifecycleOpts(1))
			if err != nil {
				t.Fatalf("open after boundary truncation: %v (heap covers the range, want success)", err)
			}
			defer p2.Close()
			persistMustGet(t, p2, "a", "1")
			persistMustGet(t, p2, "b", "2")
		})
	}
}

// Replay over rotations + deletes + overwrites must equal
// a reference map exactly.
func TestLifecycleReplayEqualsReference(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p := persistOpenForTest(t, bin, dir, lifecycleOpts(2))
			defer p.Close()
			ref := map[string]string{}
			apply := func(k, v string) {
				if err := p.Put(k, []byte(v)); err != nil {
					t.Fatalf("Put %s: %v", k, err)
				}
				ref[k] = v
			}
			del := func(k string) {
				if err := p.Delete(k); err != nil {
					t.Fatalf("Delete %s: %v", k, err)
				}
				delete(ref, k)
			}
			apply("a", "1")
			apply("b", "2")
			if err := p.Flush(); err != nil {
				t.Fatalf("Flush 1: %v", err)
			}
			if err := p.RotateWAL(); err != nil {
				t.Fatalf("RotateWAL 1: %v", err)
			}
			apply("a", "10")
			del("b")
			apply("c", "3")
			if err := p.Flush(); err != nil {
				t.Fatalf("Flush 2: %v", err)
			}
			if err := p.RotateWAL(); err != nil {
				t.Fatalf("RotateWAL 2: %v", err)
			}
			apply("d", "4")
			del("a")
			if err := p.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			p2 := persistOpenForTest(t, bin, dir, lifecycleOpts(2))
			defer p2.Close()
			for k, v := range ref {
				persistMustGet(t, p2, k, v)
			}
			for _, k := range []string{"a", "b"} {
				if _, ok := ref[k]; !ok {
					persistMustMissing(t, p2, k)
				}
			}
			entries, err := p2.Scan("")
			if err != nil {
				t.Fatalf("Scan: %v", err)
			}
			if len(entries) != len(ref) {
				t.Fatalf("Scan returned %d entries, want %d (ref %+v)", len(entries), len(ref), ref)
			}
		})
	}
}

// Dirty bits are never cleared by a failed checkpoint or
// by poisoning; they clear only on definite publish.
func TestLifecycleDirtyBitSurvivesFailures(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			opts := lifecycleOpts(1)
			opts.MixedCheckpoint = true
			p := persistOpenForTest(t, bin, dir, opts)
			defer p.Close()
			if err := p.Put("a", []byte("1")); err != nil {
				t.Fatalf("Put a: %v", err)
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("Flush gen1: %v", err)
			}
			if p.shardDirty[0] {
				t.Fatalf("dirty bit set after definite publish, want cleared")
			}
			if err := p.Put("a", []byte("10")); err != nil {
				t.Fatalf("Put a=10: %v", err)
			}
			if !p.shardDirty[0] {
				t.Fatalf("dirty bit NOT set before/after mutation, want set")
			}
			persistFailOnce(t, persistFaultHeapWrite)
			if err := p.Flush(); err == nil {
				t.Fatalf("Flush with heap-write fault: want error, got nil")
			}
			if !p.shardDirty[0] {
				t.Fatalf("dirty bit cleared by failed checkpoint, want still set")
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("retry Flush: %v", err)
			}
			if p.shardDirty[0] {
				t.Fatalf("dirty bit set after definite publish, want cleared")
			}
			// Poisoning also leaves the bit set.
			if err := p.Put("b", []byte("2")); err != nil {
				t.Fatalf("Put b: %v", err)
			}
			persistFailOnce(t, persistFaultWALWrite)
			if err := p.Put("c", []byte("3")); err == nil {
				t.Fatalf("Put with wal-write fault: want error, got nil")
			}
			if p.Poisoned() == nil {
				t.Fatalf("Poisoned() = nil after WAL failure, want non-nil")
			}
			if !p.shardDirty[0] {
				t.Fatalf("dirty bit cleared by poisoning, want still set")
			}
		})
	}
}

// Segmented repair: corruption in the sealed first
// segment truncates it and DROPS the later segment (its
// records are unreachable past the corruption); reopen
// lands on the checkpoint state, never on a longer fake
// history. (a is in the gen1 heap, so it survives; b was
// WAL-only in the dropped later segment, so it must not.)
func TestLifecycleSegmentedRepair(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p := persistOpenForTest(t, bin, dir, lifecycleOpts(1))
			if err := p.Put("a", []byte("1")); err != nil {
				t.Fatalf("Put a: %v", err)
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("Flush gen1: %v", err)
			}
			// A record between gen1 and the rotation, so
			// the sealed segment survives reclaim (its
			// last record is not covered by gen1).
			if err := p.Put("x", []byte("2")); err != nil {
				t.Fatalf("Put x: %v", err)
			}
			if err := p.RotateWAL(); err != nil {
				t.Fatalf("RotateWAL: %v", err)
			}
			if err := p.Put("b", []byte("3")); err != nil {
				t.Fatalf("Put b: %v", err)
			}
			segs := p.WALSegments()
			if len(segs) != 2 {
				t.Fatalf("WALSegments = %+v, want 2", segs)
			}
			sealedSeg := filepath.Join(dir, segs[0].File)
			laterSeg := filepath.Join(dir, segs[1].File)
			if err := p.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			// Corrupt the sealed segment's LAST byte: its
			// last record (x, seq 2) becomes corrupt while
			// its first record (a, seq 1) stays valid.
			data := persistReadFile(t, sealedSeg)
			data[len(data)-1] ^= 0xFF
			if err := os.WriteFile(sealedSeg, data, 0644); err != nil {
				t.Fatalf("corrupt sealed segment: %v", err)
			}
			wasmBytes, _ := os.ReadFile(bin)
			_, err := OpenPersistedSegDB(dir, wasmBytes, lifecycleOpts(1))
			if err == nil {
				t.Fatalf("open with corrupt segment: got success, want CorruptWALError")
			}
			var corruptErr *CorruptWALError
			if !errors.As(err, &corruptErr) {
				t.Fatalf("open error = %v, want *CorruptWALError", err)
			}
			last, err := RepairWALTail(dir)
			if err != nil {
				t.Fatalf("RepairWALTail: %v", err)
			}
			if last != 1 {
				t.Fatalf("RepairWALTail last = %d, want 1 (sealed segment's first record survives)", last)
			}
			if _, err := os.Stat(laterSeg); !os.IsNotExist(err) {
				t.Fatalf("later segment %s still present after repair (err %v)", laterSeg, err)
			}
			p2 := persistOpenForTest(t, bin, dir, lifecycleOpts(1))
			defer p2.Close()
			persistMustGet(t, p2, "a", "1")
			persistMustGet(t, p2, "x", "2")
			persistMustMissing(t, p2, "b")
		})
	}
}
