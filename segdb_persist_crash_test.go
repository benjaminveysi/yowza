package yowza

// Reliability stage item 01 — crash consistency + WAL contract
// tests for PersistedSegDB. These tests use the package-level
// fault-injection hook (persistFaultHook) to interrupt the
// wrapper at exactly one write / sync / rename / directory-sync
// stage, then verify recovery yields the old valid version or
// the new valid version, never an unrecoverable one.
//
// Nothing here touches the C engine, query/, or segdb.go: all
// faults are injected at the host-wrapper file-operation layer.

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// persistFailOnce arms persistFaultHook to fail exactly once at
// point, then go inert (later calls at any point return nil).
// Cleanup clears the hook at test end.
func persistFailOnce(t *testing.T, point string) {
	t.Helper()
	fired := false
	persistFaultHook = func(p string) error {
		if p == point && !fired {
			fired = true
			return fmt.Errorf("injected fault at %s", point)
		}
		return nil
	}
	t.Cleanup(func() { persistFaultHook = nil })
}

func persistReadFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return data
}

func persistDirNames(t *testing.T, dir string) map[string]bool {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir %s: %v", dir, err)
	}
	out := map[string]bool{}
	for _, e := range entries {
		out[e.Name()] = true
	}
	return out
}

// (a)+(d) Generational checkpoint crash points. Setup: two
// puts + flush = generation 1, checkpoint seq 2. Then one more
// put (seq 3) + a flush interrupted at exactly one stage.
// Pre-publish interruptions must leave generation 1, its heap
// files, and checkpoint.json byte-identical, and a retry flush
// must then publish generation 2. The checkpoint-dir-sync
// interruption happens after the rename, so the reopened
// database must already be generation 2 — and the generation-1
// heaps must still be on disk, because reclaim is skipped when
// the directory sync did not confirm.
func TestPersistedGenerationalCrashPoints(t *testing.T) {
	points := []string{
		persistFaultHeapWrite, persistFaultHeapSync, persistFaultHeapRename, persistFaultHeapDirSync,
		persistFaultCheckpointWrite, persistFaultCheckpointSync, persistFaultCheckpointRename,
		persistFaultCheckpointDirSync,
	}
	for _, bin := range persistTestBins {
		for _, point := range points {
			t.Run(bin+"/"+point, func(t *testing.T) {
				dir := t.TempDir()
				// O1: this test pins the OLD reclaim regime
				// (definite publish reclaims the old
				// generation), so it explicitly requests
				// both policies OFF. Under the new
				// defaults (ON) the "old heap absent"
				// assertion below would be false by
				// design, not by a bug.
				p := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 2, Durability: DurabilitySync,
					KeepPreviousGenerationPolicy: PolicyOff, MixedCheckpointPolicy: PolicyOff})
				if err := p.Put("a", []byte("1")); err != nil {
					t.Fatalf("Put a: %v", err)
				}
				if err := p.Put("b", []byte("2")); err != nil {
					t.Fatalf("Put b: %v", err)
				}
				if err := p.Flush(); err != nil {
					t.Fatalf("Flush gen1: %v", err)
				}
				if p.Generation() != 1 || p.CheckpointSeq() != 2 {
					t.Fatalf("after gen1 flush: generation %d checkpointSeq %d, want 1/2", p.Generation(), p.CheckpointSeq())
				}
				oldHeapPaths := []string{p.HeapPath(0), p.HeapPath(1)}
				oldHeaps := [][]byte{persistReadFile(t, oldHeapPaths[0]), persistReadFile(t, oldHeapPaths[1])}
				oldCkpt := persistReadFile(t, p.CheckpointPath())
				if err := p.Put("a", []byte("10")); err != nil {
					t.Fatalf("Put a=10: %v", err)
				}
				persistFailOnce(t, point)
				flushErr := p.Flush()
				if flushErr == nil {
					t.Fatalf("Flush with injected %s fault: want error, got nil", point)
				}
				// A checkpoint failure must NOT poison: the
				// old checkpoint + WAL are intact.
				if p.Poisoned() != nil {
					t.Fatalf("Flush failure at %s poisoned the DB, want usable: %v", point, p.Poisoned())
				}
				if point == persistFaultCheckpointDirSync {
					// Rename already happened: the new
					// checkpoint is the on-disk truth.
					if p.Generation() != 2 || p.CheckpointSeq() != 3 {
						t.Fatalf("after checkpoint-dir-sync fault: generation %d checkpointSeq %d, want 2/3", p.Generation(), p.CheckpointSeq())
					}
					// Old-generation heaps must NOT have
					// been reclaimed.
					for _, hp := range oldHeapPaths {
						if _, err := os.Stat(hp); err != nil {
							t.Fatalf("old heap %s missing after checkpoint-dir-sync fault: %v", hp, err)
						}
					}
				} else {
					// Pre-publish: old checkpoint + heaps
					// byte-identical, generation unchanged.
					if p.Generation() != 1 || p.CheckpointSeq() != 2 {
						t.Fatalf("after %s fault: generation %d checkpointSeq %d, want 1/2", point, p.Generation(), p.CheckpointSeq())
					}
					for i, hp := range oldHeapPaths {
						now := persistReadFile(t, hp)
						if string(now) != string(oldHeaps[i]) {
							t.Fatalf("old heap %s changed by interrupted checkpoint at %s", hp, point)
						}
					}
					if now := persistReadFile(t, p.CheckpointPath()); string(now) != string(oldCkpt) {
						t.Fatalf("checkpoint.json changed by interrupted checkpoint at %s", point)
					}
					// Retry (hook now inert) must succeed
					// and publish generation 2.
					if err := p.Flush(); err != nil {
						t.Fatalf("retry Flush after %s fault: %v", point, err)
					}
					if p.Generation() != 2 || p.CheckpointSeq() != 3 {
						t.Fatalf("after retry: generation %d checkpointSeq %d, want 2/3", p.Generation(), p.CheckpointSeq())
					}
					// After a definite publish, the old
					// generation is reclaimed.
					for _, hp := range oldHeapPaths {
						if _, err := os.Stat(hp); !os.IsNotExist(err) {
							t.Fatalf("old heap %s still present after definite gen2 publish (err %v)", hp, err)
						}
					}
				}
				if err := p.Close(); err != nil {
					t.Fatalf("Close: %v", err)
				}
				p2 := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 2, Durability: DurabilitySync,
					KeepPreviousGenerationPolicy: PolicyOff, MixedCheckpointPolicy: PolicyOff})
				defer p2.Close()
				persistMustGet(t, p2, "a", "10")
				persistMustGet(t, p2, "b", "2")
				if p2.CheckpointSeq() != 3 {
					t.Fatalf("reopened CheckpointSeq = %d, want 3", p2.CheckpointSeq())
				}
				if p2.Generation() != 2 {
					t.Fatalf("reopened Generation = %d, want 2", p2.Generation())
				}
			})
		}
	}
}

// (b) Poisoned / indeterminate state after a WAL failure.
// wal-write: the frame never lands, so reopen shows the put
// absent. wal-sync: the frame was written before the sync
// failure, so reopen shows the "failed" put present — the
// proof that the outcome was genuinely indeterminate. In both
// cases the instance refuses further Put/Delete/Flush/Compact
// until close + reopen, while Get keeps working.
func TestPersistedWALPoison(t *testing.T) {
	for _, bin := range persistTestBins {
		for _, tc := range []struct {
			point       string
			afterReopen string // "absent" or "present"
		}{
			{persistFaultWALWrite, "absent"},
			{persistFaultWALSync, "present"},
		} {
			t.Run(bin+"/"+tc.point, func(t *testing.T) {
				dir := t.TempDir()
				p := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
				if err := p.Put("good", []byte("1")); err != nil {
					t.Fatalf("Put good: %v", err)
				}
				if err := p.Flush(); err != nil {
					t.Fatalf("Flush: %v", err)
				}
				seqBefore := p.LastSeq()
				persistFailOnce(t, tc.point)
				err := p.Put("doomed", []byte("x"))
				var ind *IndeterminateWALError
				if !errors.As(err, &ind) {
					t.Fatalf("failed Put: err %v, want *IndeterminateWALError", err)
				}
				if ind.Seq != seqBefore+1 {
					t.Fatalf("indeterminate seq = %d, want %d", ind.Seq, seqBefore+1)
				}
				if p.Poisoned() == nil || p.PoisonSeq() != seqBefore+1 {
					t.Fatalf("Poisoned() = %v, PoisonSeq() = %d, want poison at %d", p.Poisoned(), p.PoisonSeq(), seqBefore+1)
				}
				// Blind retry and further writes are refused.
				if err := p.Put("retry", []byte("y")); !errors.Is(err, ErrPersistedPoisoned) {
					t.Fatalf("Put while poisoned: err %v, want ErrPersistedPoisoned", err)
				}
				if err := p.Delete("good"); !errors.Is(err, ErrPersistedPoisoned) {
					t.Fatalf("Delete while poisoned: err %v, want ErrPersistedPoisoned", err)
				}
				if err := p.Flush(); !errors.Is(err, ErrPersistedPoisoned) {
					t.Fatalf("Flush while poisoned: err %v, want ErrPersistedPoisoned", err)
				}
				if err := p.Compact(0); !errors.Is(err, ErrPersistedPoisoned) {
					t.Fatalf("Compact while poisoned: err %v, want ErrPersistedPoisoned", err)
				}
				// Reads still work on the coherent
				// in-memory state.
				persistMustGet(t, p, "good", "1")
				// The failed sequence must not be reused by
				// a later sequence in this instance: the
				// poison blocks all writers, so LastSeq is
				// unchanged since the failed put.
				if p.LastSeq() != seqBefore+1 {
					t.Fatalf("LastSeq after poison = %d, want %d", p.LastSeq(), seqBefore+1)
				}
				if err := p.Close(); err != nil {
					t.Fatalf("Close: %v", err)
				}
				p2 := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
				defer p2.Close()
				persistMustGet(t, p2, "good", "1")
				if tc.afterReopen == "present" {
					persistMustGet(t, p2, "doomed", "x")
				} else {
					persistMustMissing(t, p2, "doomed")
				}
				if p2.Poisoned() != nil {
					t.Fatalf("reopened instance still poisoned: %v", p2.Poisoned())
				}
				if err := p2.Put("after", []byte("ok")); err != nil {
					t.Fatalf("Put after recovery: %v", err)
				}
				persistMustGet(t, p2, "after", "ok")
			})
		}
		// Abort-append failure poisons the same way: the
		// abort record is what stops the failed mutation
		// from replaying, so losing it is indeterminate.
		t.Run(bin+"/abort", func(t *testing.T) {
			dir := t.TempDir()
			p := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			if err := p.Put("good", []byte("1")); err != nil {
				t.Fatalf("Put good: %v", err)
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("Flush: %v", err)
			}
			seqBefore := p.LastSeq()
			persistFailOnce(t, persistFaultWALSync)
			err := p.appendAbort(seqBefore)
			var ind *IndeterminateWALError
			if !errors.As(err, &ind) {
				t.Fatalf("failed appendAbort: err %v, want *IndeterminateWALError", err)
			}
			if ind.Seq != seqBefore+1 || ind.Op != "abort" {
				t.Fatalf("abort indeterminate = seq %d op %q, want seq %d op abort", ind.Seq, ind.Op, seqBefore+1)
			}
			if err := p.Put("later", []byte("y")); !errors.Is(err, ErrPersistedPoisoned) {
				t.Fatalf("Put after abort failure: err %v, want ErrPersistedPoisoned", err)
			}
			if err := p.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			p2 := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			defer p2.Close()
			persistMustGet(t, p2, "good", "1")
			if p2.Poisoned() != nil {
				t.Fatalf("reopened instance still poisoned: %v", p2.Poisoned())
			}
		})
	}
}

// (c) Durability mode validation: only the two defined modes
// are accepted; anything else is rejected before any directory
// is created, and a stored directory cannot be reopened with an
// invalid mode.
func TestPersistedDurabilityValidation(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			wasmBytes, err := os.ReadFile(bin)
			if err != nil {
				t.Fatalf("read %s: %v", bin, err)
			}
			for _, bad := range []DurabilityMode{DurabilityMode(42), DurabilityMode(-1), DurabilityMode(2), DurabilityMode(100)} {
				dir := filepath.Join(t.TempDir(), "db")
				_, err := OpenPersistedSegDB(dir, wasmBytes, PersistedOptions{NumShards: 1, Durability: bad})
				if !errors.Is(err, ErrInvalidDurabilityMode) {
					t.Fatalf("open with durability %d: err %v, want ErrInvalidDurabilityMode", int(bad), err)
				}
				if _, statErr := os.Stat(dir); !os.IsNotExist(statErr) {
					t.Fatalf("invalid durability %d created directory %s (stat err %v)", int(bad), dir, statErr)
				}
			}
			for _, good := range []DurabilityMode{DurabilitySync, DurabilityNoSync} {
				dir := t.TempDir()
				p := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: good})
				if p.Durability() != good {
					t.Fatalf("Durability() = %d, want %d", int(p.Durability()), int(good))
				}
				if err := p.Put("k", []byte("v")); err != nil {
					t.Fatalf("Put with durability %d: %v", int(good), err)
				}
				if err := p.Close(); err != nil {
					t.Fatalf("Close: %v", err)
				}
			}
			// Existing directory + invalid mode on reopen.
			dir := t.TempDir()
			p := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			if err := p.Put("k", []byte("v")); err != nil {
				t.Fatalf("Put: %v", err)
			}
			if err := p.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			_, err = OpenPersistedSegDB(dir, wasmBytes, PersistedOptions{NumShards: 1, Durability: DurabilityMode(42)})
			if !errors.Is(err, ErrInvalidDurabilityMode) {
				t.Fatalf("reopen with durability 42: err %v, want ErrInvalidDurabilityMode", err)
			}
			p2 := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			defer p2.Close()
			persistMustGet(t, p2, "k", "v")
		})
	}
}

// (a) Legacy checkpoints (pre-generational heap-<i>.bin, no
// generation / heap_files fields) still open, and the next
// checkpoint migrates them to the generational layout while
// reclaiming the legacy files.
func TestPersistedLegacyCheckpointMigration(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			// O1: this test pins the OLD reclaim regime
			// (the legacy heap is reclaimed by the
			// migration publish), so it explicitly
			// requests both policies OFF. Under the new
			// defaults (ON) the legacy checkpoint IS
			// the retained previous generation and its
			// heap must survive — see
			// TestLifecycleDefaultRetentionFallback.
			p := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync,
				KeepPreviousGenerationPolicy: PolicyOff, MixedCheckpointPolicy: PolicyOff})
			if err := p.Put("a", []byte("1")); err != nil {
				t.Fatalf("Put a: %v", err)
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("Flush gen1: %v", err)
			}
			genPath := p.HeapPath(0)
			if err := p.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			// Downgrade on disk to the legacy layout.
			legacyPath := filepath.Join(dir, "heap-0.bin")
			if err := os.Rename(genPath, legacyPath); err != nil {
				t.Fatalf("rename to legacy: %v", err)
			}
			raw := persistReadFile(t, filepath.Join(dir, "checkpoint.json"))
			var ckpt map[string]interface{}
			if err := json.Unmarshal(raw, &ckpt); err != nil {
				t.Fatalf("unmarshal checkpoint: %v", err)
			}
			delete(ckpt, "generation")
			delete(ckpt, "heap_files")
			raw, err := json.MarshalIndent(ckpt, "", "  ")
			if err != nil {
				t.Fatalf("marshal legacy checkpoint: %v", err)
			}
			if err := os.WriteFile(filepath.Join(dir, "checkpoint.json"), raw, 0644); err != nil {
				t.Fatalf("write legacy checkpoint: %v", err)
			}
			p2 := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync,
				KeepPreviousGenerationPolicy: PolicyOff, MixedCheckpointPolicy: PolicyOff})
			persistMustGet(t, p2, "a", "1")
			if p2.Generation() != 0 {
				t.Fatalf("legacy reopen Generation = %d, want 0", p2.Generation())
			}
			if p2.HeapPath(0) != legacyPath {
				t.Fatalf("legacy HeapPath = %s, want %s", p2.HeapPath(0), legacyPath)
			}
			if err := p2.Put("b", []byte("2")); err != nil {
				t.Fatalf("Put b: %v", err)
			}
			if err := p2.Flush(); err != nil {
				t.Fatalf("Flush after legacy: %v", err)
			}
			if p2.Generation() != 1 {
				t.Fatalf("after migration flush Generation = %d, want 1", p2.Generation())
			}
			if _, err := os.Stat(legacyPath); !os.IsNotExist(err) {
				t.Fatalf("legacy heap %s still present after migration publish (err %v)", legacyPath, err)
			}
			if err := p2.Close(); err != nil {
				t.Fatalf("Close 2: %v", err)
			}
			p3 := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync,
				KeepPreviousGenerationPolicy: PolicyOff, MixedCheckpointPolicy: PolicyOff})
			defer p3.Close()
			persistMustGet(t, p3, "a", "1")
			persistMustGet(t, p3, "b", "2")
		})
	}
}

// (d) Repair interruption: RepairWALTail truncates first and
// syncs after, so an interruption at its file-sync or
// dir-sync stage still leaves a valid WAL prefix and the
// reopen succeeds with checkpoint data intact.
func TestPersistedRepairFaultInterruption(t *testing.T) {
	for _, bin := range persistTestBins {
		for _, point := range []string{persistFaultRepairFileSync, persistFaultRepairDirSync} {
			t.Run(bin+"/"+point, func(t *testing.T) {
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
				walPath := filepath.Join(dir, "wal.log")
				wal := persistReadFile(t, walPath)
				wal[len(wal)-1] ^= 0xFF
				if err := os.WriteFile(walPath, wal, 0644); err != nil {
					t.Fatalf("corrupt wal: %v", err)
				}
				persistFailOnce(t, point)
				if _, err := RepairWALTail(dir); err == nil {
					t.Fatalf("RepairWALTail with injected %s fault: want error, got nil", point)
				}
				p2 := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
				defer p2.Close()
				persistMustGet(t, p2, "ckpt", "safe")
				persistMustMissing(t, p2, "tail")
			})
		}
	}
}
