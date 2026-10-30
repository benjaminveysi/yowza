package yowza

// Phase E, E5 tests: numeric criteria harnesses —
// WAL poison, snapshot stability across Flush +
// Compact under a pin, pre-E directory compat, and
// benchmarks (numbers recorded in the notes).

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestPhaseEWALSyncPoison(t *testing.T) {
	// Indeterminate Write/Sync poisons later
	// operations: a failing WAL sync must poison the
	// instance (later writes refused), and close +
	// reopen must decide the outcome (replay).
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			if err := p.Put("before", []byte("b")); err != nil {
				t.Fatalf("put before: %v", err)
			}
			oldHook := persistFaultHook
			persistFaultHook = func(point string) error {
				if point == persistFaultWALSync {
					return fmt.Errorf("injected sync failure")
				}
				return nil
			}
			err := p.Put("during", []byte("d"))
			persistFaultHook = oldHook
			if err == nil {
				t.Fatalf("put during injected sync failure: want error")
			}
			if p.Poisoned() == nil {
				t.Fatalf("instance not poisoned after indeterminate sync failure")
			}
			if err := p.Put("after", []byte("a")); err == nil {
				t.Fatalf("put after poison: want refusal")
			}
			// Reads of committed state still work on a poisoned instance.
			phaseEMustDBGet(t, p, "before", "b")
			p.Close()
			p2 := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			defer p2.Close()
			phaseEMustDBGet(t, p2, "before", "b")
			if err := p2.Put("recovered", []byte("r")); err != nil {
				t.Fatalf("put after recovery: %v", err)
			}
			phaseEMustDBGet(t, p2, "recovered", "r")
		})
	}
}

func TestPhaseESnapshotStableAcrossFlushCompact(t *testing.T) {
	// A pre-update/delete snapshot gives the same
	// result against Flush + Compact while its pin
	// is alive.
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			p, _ := phaseBOpen(t, bin, 2)
			for i := 0; i < 20; i++ {
				phaseBPut(t, p, fmt.Sprintf("sk:%02d", i), fmt.Sprintf("sv%d", i))
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}
			h, err := p.AcquireSnapshot()
			if err != nil {
				t.Fatalf("snapshot: %v", err)
			}
			defer h.Close()
			pin, err := p.PinSnapshot(h, "Test", "snap-pin", "")
			if err != nil {
				t.Fatalf("pin snapshot: %v", err)
			}
			_ = pin
			// Advance main: overwrite half, delete the rest, flush, compact.
			for i := 0; i < 20; i++ {
				if i%2 == 0 {
					phaseBPut(t, p, fmt.Sprintf("sk:%02d", i), "NEW")
				} else {
					if err := p.Delete(fmt.Sprintf("sk:%02d", i)); err != nil {
						t.Fatalf("delete: %v", err)
					}
				}
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("flush 2: %v", err)
			}
			if err := p.Compact(0); err != nil {
				t.Fatalf("compact: %v", err)
			}
			// The snapshot still reads the ORIGINAL 20 rows.
			it, err := h.Scan([]byte("sk:"))
			if err != nil {
				t.Fatalf("snapshot scan: %v", err)
			}
			seen := map[string]string{}
			for {
				batch, err := it.NextBatch(7)
				if err != nil {
					t.Fatalf("snapshot next: %v", err)
				}
				if len(batch) == 0 {
					break
				}
				for _, e := range batch {
					seen[string(e.Key)] = string(e.Val)
				}
			}
			it.Close()
			if len(seen) != 20 {
				t.Fatalf("snapshot rows %d, want 20", len(seen))
			}
			for i := 0; i < 20; i++ {
				if seen[fmt.Sprintf("sk:%02d", i)] != fmt.Sprintf("sv%d", i) {
					t.Fatalf("snapshot sk:%02d = %q, want sv%d", i, seen[fmt.Sprintf("sk:%02d", i)], i)
				}
			}
		})
	}
}

func TestPhaseEPreEDirectoryCompat(t *testing.T) {
	// A directory without any Phase E files (the
	// pre-E shape: no spill log, no delta log) opens
	// with data and timelines intact, then adopts the
	// new formats on first publish.
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			p, mgr, dir := phaseEGlobalSetup(t, bin, 1)
			if err := p.Put("legacy", []byte("kept")); err != nil {
				t.Fatalf("put: %v", err)
			}
			if _, err := mgr.ForkFromDB("old"); err != nil {
				t.Fatalf("fork: %v", err)
			}
			timelineMustPut(t, mgr, "old", "legacy2", "kept2")
			mgr.Close()
			p.Close()
			// Simulate the true pre-E shape: a CURRENT
			// full registry (pre-E code rewrote it on
			// every publish), then strip every Phase E
			// file (spill log + generations, delta log).
			// The WALs + pins stay.
			doc, exists, _, _, err := timelineLoadRegistryV2(dir)
			if err != nil || !exists {
				t.Fatalf("load registry for pre-E simulation: doc %v err %v", exists, err)
			}
			if _, err := timelinePublishRegistryWithStatus(dir, doc); err != nil {
				t.Fatalf("write pre-E registry: %v", err)
			}
			ents, err := os.ReadDir(dir)
			if err != nil {
				t.Fatalf("readdir: %v", err)
			}
			for _, e := range ents {
				name := e.Name()
				if strings.HasPrefix(name, "history-") || strings.HasPrefix(name, "timeline-registry-delta") {
					// Note: history-manifest-gen-* also starts with history-.
					if err := os.Remove(filepath.Join(dir, name)); err != nil {
						t.Fatalf("strip %s: %v", name, err)
					}
				}
			}
			p2 := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			defer p2.Close()
			phaseEMustDBGet(t, p2, "legacy", "kept")
			mgr2, err := NewTimelineManager(p2)
			if err != nil {
				t.Fatalf("reopen manager on stripped dir: %v", err)
			}
			defer mgr2.Close()
			timelineMustGet(t, mgr2, "old", "legacy2", "kept2")
			// And the manager is fully operable afterwards (new formats adopted).
			timelineMustPut(t, mgr2, "old", "legacy3", "kept3")
			timelineMustGet(t, mgr2, "old", "legacy3", "kept3")
		})
	}
}

func TestPhaseEOpenTimeCheckpointPlusTail(t *testing.T) {
	// Recovery open time depends on checkpoint + active
	// tail, not on rebuilding unbounded history: open a
	// DB with a checkpoint + a 2000-record tail and
	// assert it opens promptly with all data (the
	// timing is logged, not asserted tightly — wall
	// clocks are not a correctness proof).
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			for i := 0; i < 200; i++ {
				if err := p.Put(fmt.Sprintf("ot:%04d", i), []byte("v")); err != nil {
					t.Fatalf("put: %v", err)
				}
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("flush (checkpoint): %v", err)
			}
			for i := 200; i < 2200; i++ {
				if err := p.Put(fmt.Sprintf("ot:%04d", i), []byte("v")); err != nil {
					t.Fatalf("tail put: %v", err)
				}
			}
			p.Close()
			start := time.Now()
			p2 := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			elapsed := time.Since(start)
			defer p2.Close()
			t.Logf("open with checkpoint + 2000-record tail: %s", elapsed)
			phaseEMustDBGet(t, p2, "ot:2199", "v")
			phaseEMustDBGet(t, p2, "ot:0000", "v")
			if elapsed > 60*time.Second {
				t.Fatalf("open took %s, want < 60s", elapsed)
			}
		})
	}
}

// Benchmarks: fork cost vs shard count (not key
// count) and merge cost vs change count. Run with
// -bench; numbers are recorded in TIMELINE_V2_NOTES.md.
func BenchmarkPhaseEForkScaling(b *testing.B) {
	for _, shards := range []int{1, 4} {
		b.Run(fmt.Sprintf("shards-%d", shards), func(b *testing.B) {
			bin := persistTestBins[0]
			dir := b.TempDir()
			wasmBytes, err := os.ReadFile(bin)
			if err != nil {
				b.Fatalf("read %s: %v", bin, err)
			}
			p, err := OpenPersistedSegDB(dir, wasmBytes, PersistedOptions{NumShards: shards, Durability: DurabilityNoSync})
			if err != nil {
				b.Fatalf("open: %v", err)
			}
			defer p.Close()
			for i := 0; i < 500; i++ {
				_ = p.Put(fmt.Sprintf("bk:%04d", i), []byte("v"))
			}
			mgr, err := NewTimelineManager(p)
			if err != nil {
				b.Fatalf("manager: %v", err)
			}
			defer mgr.Close()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := mgr.ForkFromDB(fmt.Sprintf("bench-%d", i)); err != nil {
					b.Fatalf("fork: %v", err)
				}
			}
		})
	}
}

func BenchmarkPhaseEGlobalMergeScaling(b *testing.B) {
	for _, n := range []int{10, 200} {
		b.Run(fmt.Sprintf("changes-%d", n), func(b *testing.B) {
			bin := persistTestBins[0]
			dir := b.TempDir()
			wasmBytes, err := os.ReadFile(bin)
			if err != nil {
				b.Fatalf("read %s: %v", bin, err)
			}
			p, err := OpenPersistedSegDB(dir, wasmBytes, PersistedOptions{NumShards: 2, Durability: DurabilityNoSync})
			if err != nil {
				b.Fatalf("open: %v", err)
			}
			defer p.Close()
			mgr, err := NewTimelineManager(p)
			if err != nil {
				b.Fatalf("manager: %v", err)
			}
			defer mgr.Close()
			if _, err := mgr.ForkFromDB("bench"); err != nil {
				b.Fatalf("fork: %v", err)
			}
			for i := 0; i < n; i++ {
				_ = mgr.Put("bench", fmt.Sprintf("mk:%04d", i), []byte("v"))
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Each iteration merges a fresh timeline copy is expensive to
				// set up; instead measure the merge of the same delta set
				// after resetting main by deleting the keys (approximate
				// scaling signal, not a pure merge cost).
				b.StopTimer()
				for j := 0; j < n; j++ {
					_ = p.Delete(fmt.Sprintf("mk:%04d", j))
				}
				b.StartTimer()
				if _, err := mgr.MergeToMain("bench", TimelineConflictFail, nil); err != nil {
					b.Fatalf("merge: %v", err)
				}
			}
		})
	}
}

var _ = errors.Is
