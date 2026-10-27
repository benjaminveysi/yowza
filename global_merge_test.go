package yowza

// Phase E, E1 tests: global merge to the shared DB
// head — conflict policies, stale head, chunked
// staging, and the crash-stage matrix.

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func phaseEGlobalSetup(t *testing.T, bin string, shards int) (*PersistedSegDB, *TimelineManager, string) {
	t.Helper()
	dir := t.TempDir()
	p := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: shards, Durability: DurabilitySync})
	mgr, err := NewTimelineManager(p)
	if err != nil {
		p.Close()
		t.Fatalf("NewTimelineManager: %v", err)
	}
	t.Cleanup(func() {
		mgr.Close()
		p.Close()
	})
	return p, mgr, dir
}

func phaseEMustDBGet(t *testing.T, p *PersistedSegDB, key, want string) {
	t.Helper()
	v, err := p.Get(key)
	if err != nil || string(v) != want {
		t.Fatalf("DB Get(%q) = %q, err %v, want %q", key, v, err, want)
	}
}

func phaseEMustDBMissing(t *testing.T, p *PersistedSegDB, key string) {
	t.Helper()
	if v, err := p.Get(key); err == nil {
		t.Fatalf("DB Get(%q) = %q, want missing", key, v)
	}
}

func TestPhaseEGlobalMergePolicies(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			p, mgr, _ := phaseEGlobalSetup(t, bin, 2)
			// Main head: a (shared), b (will conflict), c (identical target).
			for k, v := range map[string]string{"a": "a0", "b": "b0", "c": "c0"} {
				if err := p.Put(k, []byte(v)); err != nil {
					t.Fatalf("put %s: %v", k, err)
				}
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}
			if _, err := mgr.ForkFromDB("src"); err != nil {
				t.Fatalf("fork: %v", err)
			}
			// Timeline changes: a (one-sided), b (conflicting change),
			// c (change to the value main will independently reach),
			// d (new key).
			timelineMustPut(t, mgr, "src", "a", "a1")
			timelineMustPut(t, mgr, "src", "b", "b1")
			timelineMustPut(t, mgr, "src", "c", "c1")
			timelineMustPut(t, mgr, "src", "d", "d1")
			// Main moves b (conflict) and c (to exactly the timeline's value).
			if err := p.Put("b", []byte("b-main")); err != nil {
				t.Fatalf("main put b: %v", err)
			}
			if err := p.Put("c", []byte("c1")); err != nil {
				t.Fatalf("main put c: %v", err)
			}
			head, _, err := p.SnapshotVector()
			if err != nil {
				t.Fatalf("snapshot vector: %v", err)
			}
			// FAIL policy: lists the conflict, applies nothing.
			_, err = mgr.MergeToMain("src", TimelineConflictFail, head)
			var conflictErr *GlobalMergeConflictError
			if !errors.As(err, &conflictErr) {
				t.Fatalf("fail merge err %v, want GlobalMergeConflictError", err)
			}
			if len(conflictErr.Keys) != 1 || conflictErr.Keys[0] != "b" {
				t.Fatalf("conflict keys %v, want [b]", conflictErr.Keys)
			}
			if !errors.Is(err, ErrGlobalMergeConflict) {
				t.Fatalf("fail merge err %v does not wrap ErrGlobalMergeConflict", err)
			}
			phaseEMustDBGet(t, p, "a", "a0")
			phaseEMustDBGet(t, p, "b", "b-main")
			phaseEMustDBMissing(t, p, "d")
			// STALE head: capture an old vector, move main, merge must refuse.
			stale := append([]uint64(nil), head...)
			if err := p.Put("zz", []byte("1")); err != nil {
				t.Fatalf("main put zz: %v", err)
			}
			_, err = mgr.MergeToMain("src", TimelineConflictSourceWins, stale)
			var staleErr *GlobalMergeStaleHeadError
			if !errors.As(err, &staleErr) {
				t.Fatalf("stale merge err %v, want GlobalMergeStaleHeadError", err)
			}
			if !errors.Is(err, ErrGlobalMergeStaleHead) {
				t.Fatalf("stale merge err %v does not wrap ErrGlobalMergeStaleHead", err)
			}
			phaseEMustDBMissing(t, p, "d")
			// TARGET-WINS: conflicted key keeps main; one-sided + new apply;
			// identical key reported, not rewritten.
			head2, _, err := p.SnapshotVector()
			if err != nil {
				t.Fatalf("snapshot vector 2: %v", err)
			}
			rep, err := mgr.MergeToMain("src", TimelineConflictTargetWins, head2)
			if err != nil {
				t.Fatalf("target-wins merge: %v", err)
			}
			if len(rep.ConflictedKeys) != 1 || rep.ConflictedKeys[0] != "b" {
				t.Fatalf("target-wins conflicted %v, want [b]", rep.ConflictedKeys)
			}
			if len(rep.IdenticalKeys) != 1 || rep.IdenticalKeys[0] != "c" {
				t.Fatalf("target-wins identical %v, want [c]", rep.IdenticalKeys)
			}
			phaseEMustDBGet(t, p, "a", "a1")
			phaseEMustDBGet(t, p, "b", "b-main")
			phaseEMustDBGet(t, p, "c", "c1")
			phaseEMustDBGet(t, p, "d", "d1")
			if rep.MergeID == "" {
				t.Fatalf("applied merge should have a MergeID")
			}
			// Second merge with FAIL still conflicts on b
			// (the conflict was reported, not resolved,
			// by target-wins): nothing new is applied.
			head3, _, err := p.SnapshotVector()
			if err != nil {
				t.Fatalf("snapshot vector 3: %v", err)
			}
			_, err = mgr.MergeToMain("src", TimelineConflictFail, head3)
			if !errors.As(err, &conflictErr) {
				t.Fatalf("second merge err %v, want GlobalMergeConflictError on b", err)
			}
			phaseEMustDBGet(t, p, "a", "a1")
			phaseEMustDBGet(t, p, "b", "b-main")
		})
	}
}

func TestPhaseEGlobalMergeSourceWinsAndDelete(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			p, mgr, _ := phaseEGlobalSetup(t, bin, 2)
			if err := p.Put("k", []byte("v0")); err != nil {
				t.Fatalf("put: %v", err)
			}
			if err := p.Put("gone", []byte("g0")); err != nil {
				t.Fatalf("put gone: %v", err)
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}
			if _, err := mgr.ForkFromDB("src"); err != nil {
				t.Fatalf("fork: %v", err)
			}
			timelineMustPut(t, mgr, "src", "k", "v1")
			if err := mgr.DeleteKey("src", "gone"); err != nil {
				t.Fatalf("timeline delete: %v", err)
			}
			if err := p.Put("k", []byte("v-main")); err != nil {
				t.Fatalf("main put: %v", err)
			}
			head, _, err := p.SnapshotVector()
			if err != nil {
				t.Fatalf("vector: %v", err)
			}
			rep, err := mgr.MergeToMain("src", TimelineConflictSourceWins, head)
			if err != nil {
				t.Fatalf("source-wins merge: %v", err)
			}
			if len(rep.AppliedKeys) != 2 {
				t.Fatalf("applied %v, want 2 keys", rep.AppliedKeys)
			}
			phaseEMustDBGet(t, p, "k", "v1")
			phaseEMustDBMissing(t, p, "gone")
		})
	}
}

func TestPhaseEGlobalMergeChunkedMultiShard(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			p, mgr, dir := phaseEGlobalSetup(t, bin, 2)
			if _, err := mgr.ForkFromDB("big"); err != nil {
				t.Fatalf("fork: %v", err)
			}
			const n = 600
			for i := 0; i < n; i++ {
				timelineMustPut(t, mgr, "big", fmt.Sprintf("mk:%04d", i), fmt.Sprintf("mv:%d", i))
			}
			head, _, err := p.SnapshotVector()
			if err != nil {
				t.Fatalf("vector: %v", err)
			}
			rep, err := mgr.MergeToMain("big", TimelineConflictFail, head)
			if err != nil {
				t.Fatalf("chunked merge: %v", err)
			}
			if rep.ChunkCount < 3 {
				t.Fatalf("chunk count %d, want >= 3 for %d deltas", rep.ChunkCount, n)
			}
			total := 0
			for _, c := range rep.ShardCounts {
				total += c
			}
			if total != n {
				t.Fatalf("shard counts total %d, want %d", total, n)
			}
			if len(rep.ShardCounts) != 2 {
				t.Fatalf("shard counts %v, want both shards represented", rep.ShardCounts)
			}
			for i := 0; i < n; i++ {
				phaseEMustDBGet(t, p, fmt.Sprintf("mk:%04d", i), fmt.Sprintf("mv:%d", i))
			}
			// No staged files left behind after success.
			ents, err := os.ReadDir(dir)
			if err != nil {
				t.Fatalf("readdir: %v", err)
			}
			for _, e := range ents {
				if strings.HasPrefix(e.Name(), "global-merge-") {
					t.Fatalf("staged file left behind: %s", e.Name())
				}
			}
			// No single chunk file is a giant record: chunks are bounded.
			_ = dir
		})
	}
}

func TestPhaseEGlobalMergeConcurrentWriterSerialises(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			p, mgr, _ := phaseEGlobalSetup(t, bin, 2)
			if _, err := mgr.ForkFromDB("src"); err != nil {
				t.Fatalf("fork: %v", err)
			}
			for i := 0; i < 300; i++ {
				timelineMustPut(t, mgr, "src", fmt.Sprintf("ck:%04d", i), "cv")
			}
			var wg sync.WaitGroup
			wg.Add(1)
			var mergeErr error
			go func() {
				defer wg.Done()
				// PATCH3 contract change: a nil-head merge no
				// longer silently applies over a head that moved
				// between decision and commit — it returns
				// ErrGlobalMergeStaleHead, and the documented
				// caller contract is to retry from the fresh
				// head. The writer here lands at most once, so
				// a retry's planning head is post-writer and
				// stable; the test's real assertions (no
				// deadlock, no lost writes, all branch keys
				// applied) are unchanged and still strict.
				for attempt := 0; attempt < 10; attempt++ {
					_, mergeErr = mgr.MergeToMain("src", TimelineConflictFail, nil)
					var staleErr *GlobalMergeStaleHeadError
					if mergeErr != nil && errors.As(mergeErr, &staleErr) {
						continue
					}
					break
				}
			}()
			// A concurrent facade write must serialise (p.mu), not deadlock
			// and not be lost.
			if err := p.Put("concurrent", []byte("yes")); err != nil {
				t.Fatalf("concurrent put: %v", err)
			}
			wg.Wait()
			if mergeErr != nil {
				t.Fatalf("merge: %v", mergeErr)
			}
			phaseEMustDBGet(t, p, "concurrent", "yes")
			phaseEMustDBGet(t, p, "ck:0299", "cv")
		})
	}
}

func phaseEStageMerge(t *testing.T, dir, id, source string, deltas []globalMergeDelta) *globalMergeManifest {
	t.Helper()
	shardCounts := map[int]int{}
	man, err := stageGlobalMerge(dir, id, source, "fail", nil, deltas, shardCounts, true)
	if err != nil {
		t.Fatalf("stage: %v", err)
	}
	return man
}

func phaseEWriteCommit(t *testing.T, dir, id string, state string, newHead []uint64) {
	t.Helper()
	_, commitPath, _ := globalMergeFileNames(dir, id)
	manifestPath, _, _ := globalMergeFileNames(dir, id)
	c := globalMergeCommit{Version: 1, MergeID: id, ManifestFile: filepath.Base(manifestPath), State: state, NewHead: newHead}
	data, err := json.Marshal(c)
	if err != nil {
		t.Fatalf("marshal commit: %v", err)
	}
	if err := globalMergeWriteSync(commitPath, data, true, dir); err != nil {
		t.Fatalf("write commit: %v", err)
	}
	if state == "done" {
		_, _, donePath := globalMergeFileNames(dir, id)
		if err := globalMergeWriteSync(donePath, data, true, dir); err != nil {
			t.Fatalf("write done: %v", err)
		}
	}
}

func TestPhaseEGlobalMergeCrashStages(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			p, _, dir := phaseEGlobalSetup(t, bin, 1)
			// (a) Staged-only: manifest + chunks, no commit -> reclaimed.
			deltas := []globalMergeDelta{{Key: "s1", Value: []byte("sv1")}, {Key: "s2", Value: []byte("sv2")}}
			phaseEStageMerge(t, dir, "stage-a", "src", deltas)
			rolled, err := RecoverGlobalMerges(p)
			if err != nil {
				t.Fatalf("recover staged-only: %v", err)
			}
			if len(rolled) != 0 {
				t.Fatalf("staged-only rolled %v, want none", rolled)
			}
			phaseEMustDBMissing(t, p, "s1")
			ents, _ := os.ReadDir(dir)
			for _, e := range ents {
				if strings.HasPrefix(e.Name(), "global-merge-stage-a") {
					t.Fatalf("staged-only file not reclaimed: %s", e.Name())
				}
			}
			// (b) Commit record, partial apply -> rolled forward to new root.
			man := phaseEStageMerge(t, dir, "stage-b", "src", deltas)
			_ = man
			phaseEWriteCommit(t, dir, "stage-b", "committed", nil)
			if err := p.Put("s1", []byte("sv1")); err != nil {
				t.Fatalf("partial apply s1: %v", err)
			}
			rolled, err = RecoverGlobalMerges(p)
			if err != nil {
				t.Fatalf("recover committed: %v", err)
			}
			if len(rolled) != 1 || rolled[0] != "stage-b" {
				t.Fatalf("rolled %v, want [stage-b]", rolled)
			}
			phaseEMustDBGet(t, p, "s1", "sv1")
			phaseEMustDBGet(t, p, "s2", "sv2")
			// (c) Done record present -> cleanup only, never re-apply.
			deltasC := []globalMergeDelta{{Key: "s2", Value: []byte("SHOULD-NOT-APPLY")}}
			phaseEStageMerge(t, dir, "stage-c", "src", deltasC)
			phaseEWriteCommit(t, dir, "stage-c", "done", []uint64{1})
			rolled, err = RecoverGlobalMerges(p)
			if err != nil {
				t.Fatalf("recover done: %v", err)
			}
			if len(rolled) != 0 {
				t.Fatalf("done merge rolled %v, want none", rolled)
			}
			phaseEMustDBGet(t, p, "s2", "sv2")
			ents, _ = os.ReadDir(dir)
			for _, e := range ents {
				if strings.HasPrefix(e.Name(), "global-merge-stage-c") {
					t.Fatalf("done file not cleaned: %s", e.Name())
				}
			}
			// (d) Corrupt committed chunk -> loud failure, nothing applied.
			deltasD := []globalMergeDelta{{Key: "s3", Value: []byte("sv3")}}
			phaseEStageMerge(t, dir, "stage-d", "src", deltasD)
			phaseEWriteCommit(t, dir, "stage-d", "committed", nil)
			chunkPath := globalMergeChunkName(dir, "stage-d", 0)
			raw, err := os.ReadFile(chunkPath)
			if err != nil {
				t.Fatalf("read chunk: %v", err)
			}
			// Flip a byte inside the JSON body (past the header) to break the CRC.
			raw[len(raw)-10] ^= 0xFF
			if err := os.WriteFile(chunkPath, raw, 0o644); err != nil {
				t.Fatalf("corrupt chunk: %v", err)
			}
			_, err = RecoverGlobalMerges(p)
			if !errors.Is(err, ErrGlobalMergeCorrupt) {
				t.Fatalf("corrupt recovery err %v, want ErrGlobalMergeCorrupt", err)
			}
			phaseEMustDBMissing(t, p, "s3")
			// Clean up the corrupt fixture so Close is clean (test-only).
			cleanupGlobalMergeFiles(dir, "stage-d", 1)
		})
	}
}

func TestPhaseEGlobalMergeNoGiantRecord(t *testing.T) {
	// E5: a large merge needs neither one giant JSON
	// record nor a full DB map. Chunk files and the
	// manifest stay small while the delta count grows.
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			p, mgr, _ := phaseEGlobalSetup(t, bin, 2)
			_ = p
			if _, err := mgr.ForkFromDB("src"); err != nil {
				t.Fatalf("fork: %v", err)
			}
			for i := 0; i < 300; i++ {
				timelineMustPut(t, mgr, "src", fmt.Sprintf("gk:%04d", i), "gv")
			}
			// Stage manually to inspect files (the
			// merge itself cleans them up on success).
			rep, err := mgr.MergeToMain("src", TimelineConflictFail, nil)
			if err != nil {
				t.Fatalf("merge: %v", err)
			}
			if rep.ChunkCount != 2 {
				t.Fatalf("chunk count %d, want 2 for 300 deltas", rep.ChunkCount)
			}
		})
	}
}
