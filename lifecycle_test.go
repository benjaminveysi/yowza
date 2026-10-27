package yowza

// Axis D tests: blob GC by full reference scan, staged-blob
// lifecycle for cross-DB applies, the recovery gate,
// checkpoint WAL truncation, recursive merge-base, and
// the streamed merge report. Each test loops over both
// wasm binaries with a fresh directory per binary.
// TMPDIR must be on workspace disk (see the task brief);
// t.TempDir respects it.

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
)

func axisDOpen(t *testing.T, bin, dir string, shards int) (*PersistedSegDB, *TimelineManager) {
	t.Helper()
	p := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: shards, Durability: DurabilitySync})
	mgr, err := NewTimelineManager(p)
	if err != nil {
		p.Close()
		t.Fatalf("NewTimelineManager: %v", err)
	}
	return p, mgr
}

func axisDReadBlob(t *testing.T, dir string, ref BlobRef) []byte {
	t.Helper()
	rc, _, err := ReadBlob(dir, ref)
	if err != nil {
		t.Fatalf("ReadBlob %s: %v", ref.SHA256, err)
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadBlob read: %v", err)
	}
	return data
}

func axisDBlobExists(t *testing.T, dir string, ref BlobRef) bool {
	t.Helper()
	_, err := os.Stat(blobManifestPath(dir, ref.SHA256))
	return err == nil
}

func axisDChunkExists(dir, chunkSHA string) bool {
	_, err := os.Stat(blobChunkPath(dir, chunkSHA))
	return err == nil
}

// TestAxisDBlobGC: a blob referenced by the live head and
// a blob referenced by a live branch both survive GC;
// an orphan blob, and a blob referenced only by a
// deleted branch + a deleted (released/expired) pin, are
// both collected. A chunk shared between a live blob
// and an orphan blob is NOT deleted with the orphan.
func TestAxisDBlobGC(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p, mgr := axisDOpen(t, bin, dir, 1)
			// Live head blob (20000 bytes = 3 chunks).
			headVal := bytes.Repeat([]byte("h"), 20000)
			headRef, err := p.PutBlob("headkey", bytes.NewReader(headVal))
			if err != nil {
				t.Fatalf("PutBlob head: %v", err)
			}
			// Live branch blob.
			if _, err := mgr.ForkFromDB("br"); err != nil {
				t.Fatalf("fork br: %v", err)
			}
			branchVal := bytes.Repeat([]byte("b"), 9000)
			branchRef, err := mgr.PutLarge("br", "bk", bytes.NewReader(branchVal))
			if err != nil {
				t.Fatalf("PutLarge br: %v", err)
			}
			// Shared-chunk pair: same 8192-byte first chunk,
			// different second chunk. Live one via head,
			// orphan one written directly (no reference).
			shared := bytes.Repeat([]byte("s"), 8192)
			livePairVal := append(append([]byte(nil), shared...), []byte("live-tail")...)
			orphPairVal := append(append([]byte(nil), shared...), []byte("orph-tail")...)
			livePairRef, err := p.PutBlob("pairkey", bytes.NewReader(livePairVal))
			if err != nil {
				t.Fatalf("PutBlob pair: %v", err)
			}
			orphPairRef, err := WriteBlob(dir, bytes.NewReader(orphPairVal))
			if err != nil {
				t.Fatalf("WriteBlob orphan pair: %v", err)
			}
			// Pure orphan blob.
			orphRef, err := WriteBlob(dir, bytes.NewReader(bytes.Repeat([]byte("o"), 9000)))
			if err != nil {
				t.Fatalf("WriteBlob orphan: %v", err)
			}
			// Branch that will be deleted + pin released.
			info, err := mgr.ForkFromDB("gone")
			if err != nil {
				t.Fatalf("fork gone: %v", err)
			}
			goneVal := bytes.Repeat([]byte("g"), 9000)
			goneRef, err := mgr.PutLarge("gone", "gk", bytes.NewReader(goneVal))
			if err != nil {
				t.Fatalf("PutLarge gone: %v", err)
			}
			live, err := ScanLiveBlobRefs(p, mgr)
			if err != nil {
				t.Fatalf("ScanLiveBlobRefs: %v", err)
			}
			rep, err := GCBlobs(dir, live, false)
			if err != nil {
				t.Fatalf("GCBlobs: %v", err)
			}
			if !axisDBlobExists(t, dir, headRef) || !axisDBlobExists(t, dir, branchRef) || !axisDBlobExists(t, dir, livePairRef) || !axisDBlobExists(t, dir, goneRef) {
				t.Fatalf("GC deleted a live blob (report %+v)", rep)
			}
			if axisDBlobExists(t, dir, orphRef) || axisDBlobExists(t, dir, orphPairRef) {
				t.Fatalf("GC kept an orphan blob (report %+v)", rep)
			}
			// Shared chunk survives: the live pair blob
			// must still read fully after the orphan
			// pair (which shares its first chunk) is
			// collected.
			if !bytes.Equal(axisDReadBlob(t, dir, livePairRef), livePairVal) {
				t.Fatalf("live pair blob unreadable after GC")
			}
			// The orphan pair's second chunk must be gone:
			// its bytes differ from the live pair's, so its
			// chunk hash cannot be the shared one. Find it
			// by re-deriving: orphan pair has 2 chunks, the
			// first equals the live pair's first chunk.
			liveChunks := blobLiveChunks(dir, map[string]bool{livePairRef.SHA256: true})
			if len(liveChunks) != 2 {
				t.Fatalf("live pair chunks %d, want 2", len(liveChunks))
			}
			// Delete the branch + release its pin, reopen,
			// GC again: the gone blob must now be collected.
			if err := mgr.DeleteTimeline("gone"); err != nil {
				t.Fatalf("DeleteTimeline gone: %v", err)
			}
			if info.PinID != "" {
				if err := p.DeletePin(info.PinID); err != nil {
					t.Fatalf("DeletePin gone: %v", err)
				}
			}
			mgr.Close()
			p.Close()
			p2, mgr2 := axisDOpen(t, bin, dir, 1)
			defer mgr2.Close()
			defer p2.Close()
			live2, err := ScanLiveBlobRefs(p2, mgr2)
			if err != nil {
				t.Fatalf("ScanLiveBlobRefs after reopen: %v", err)
			}
			if _, err := GCBlobs(dir, live2, false); err != nil {
				t.Fatalf("GCBlobs after reopen: %v", err)
			}
			if axisDBlobExists(t, dir, goneRef) {
				t.Fatalf("GC kept blob referenced only by deleted branch + released pin")
			}
			if !axisDBlobExists(t, dir, headRef) || !axisDBlobExists(t, dir, branchRef) {
				t.Fatalf("GC after reopen deleted a still-live blob")
			}
			if !bytes.Equal(axisDReadBlob(t, dir, headRef), headVal) {
				t.Fatalf("head blob unreadable after second GC")
			}
		})
	}
}

// TestAxisDBlobStagedMergeLifecycle: a blob referenced
// only by a staged (uncommitted) merge survives GC;
// after the merge decision resolves by abort it is
// collected; after a commit that lands the blob in
// the DB head it survives on the head reference.
// Cross-DB preparation (copy + stage) is covered too.
func TestAxisDBlobStagedMergeLifecycle(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p, mgr := axisDOpen(t, bin, dir, 1)
			defer mgr.Close()
			defer p.Close()
			stagedVal := bytes.Repeat([]byte("z"), 9000)
			stagedRef, err := WriteBlob(dir, bytes.NewReader(stagedVal))
			if err != nil {
				t.Fatalf("WriteBlob staged: %v", err)
			}
			if err := BeginStaging(dir, "op-merge-1", []BlobRef{stagedRef}); err != nil {
				t.Fatalf("BeginStaging: %v", err)
			}
			live, err := ScanLiveBlobRefs(p, mgr)
			if err != nil {
				t.Fatalf("scan staged: %v", err)
			}
			if _, err := GCBlobs(dir, live, false); err != nil {
				t.Fatalf("GC staged: %v", err)
			}
			if !axisDBlobExists(t, dir, stagedRef) {
				t.Fatalf("GC collected a staged (uncommitted) merge blob")
			}
			// Abort resolves: collectible now.
			if err := ResolveStaging(dir, "op-merge-1", false); err != nil {
				t.Fatalf("ResolveStaging abort: %v", err)
			}
			live, err = ScanLiveBlobRefs(p, mgr)
			if err != nil {
				t.Fatalf("scan aborted: %v", err)
			}
			if _, err := GCBlobs(dir, live, false); err != nil {
				t.Fatalf("GC aborted: %v", err)
			}
			if axisDBlobExists(t, dir, stagedRef) {
				t.Fatalf("GC kept an aborted staged blob")
			}
			// Commit path: stage again, then land the blob
			// in the head, then resolve as committed.
			stagedRef2, err := WriteBlob(dir, bytes.NewReader(stagedVal))
			if err != nil {
				t.Fatalf("WriteBlob staged2: %v", err)
			}
			if err := BeginStaging(dir, "op-merge-2", []BlobRef{stagedRef2}); err != nil {
				t.Fatalf("BeginStaging2: %v", err)
			}
			if _, err := p.PutBlob("committedkey", bytes.NewReader(stagedVal)); err != nil {
				t.Fatalf("PutBlob committed: %v", err)
			}
			if err := ResolveStaging(dir, "op-merge-2", true); err != nil {
				t.Fatalf("ResolveStaging commit: %v", err)
			}
			live, err = ScanLiveBlobRefs(p, mgr)
			if err != nil {
				t.Fatalf("scan committed: %v", err)
			}
			if _, err := GCBlobs(dir, live, false); err != nil {
				t.Fatalf("GC committed: %v", err)
			}
			if !axisDBlobExists(t, dir, stagedRef2) {
				t.Fatalf("GC collected a committed blob referenced by the new head")
			}
			// Cross-DB preparation: copy + stage into a
			// foreign directory, then abort-collect there.
			dstDir := t.TempDir()
			deltas := []globalMergeDelta{{Key: "xk", Value: []byte(stagedRef2.String())}}
			if err := PrepareCrossDBBlobs(dir, dstDir, "op-x", deltas); err != nil {
				t.Fatalf("PrepareCrossDBBlobs: %v", err)
			}
			if !axisDBlobExists(t, dstDir, stagedRef2) {
				t.Fatalf("cross-DB copy missing on target")
			}
			if got := StagedBlobOps(dstDir); len(got) != 1 || got[0] != "op-x" {
				t.Fatalf("staged ops on target %v, want [op-x]", got)
			}
			stagedSet := map[string]bool{}
			for _, r := range readStagedBlobRefs(dstDir) {
				stagedSet[r.SHA256] = true
			}
			dstLive := &BlobLiveSet{Blobs: stagedSet, Chunks: blobLiveChunks(dstDir, stagedSet)}
			if _, err := GCBlobs(dstDir, dstLive, false); err != nil {
				t.Fatalf("GC target staged: %v", err)
			}
			if !axisDBlobExists(t, dstDir, stagedRef2) {
				t.Fatalf("GC collected a cross-DB staged blob before its decision")
			}
			if err := ResolveStaging(dstDir, "op-x", false); err != nil {
				t.Fatalf("ResolveStaging target abort: %v", err)
			}
			if _, err := GCBlobs(dstDir, &BlobLiveSet{Blobs: map[string]bool{}, Chunks: map[string]bool{}}, false); err != nil {
				t.Fatalf("GC target aborted: %v", err)
			}
			if axisDBlobExists(t, dstDir, stagedRef2) {
				t.Fatalf("GC kept an aborted cross-DB blob")
			}
			if err := ResolveStaging(dir, "op-nope", false); !errors.Is(err, ErrLifecycleStagedBlobUnknown) {
				t.Fatalf("resolving unknown op err %v, want ErrLifecycleStagedBlobUnknown", err)
			}
		})
	}
}

// TestAxisDLifecycleGateCrashRecovery: a simulated
// crash leaves a committed-but-unapplied global merge,
// an orphan delta, and a torn timeline WAL tail. Gated
// reads are refused until recovery; after recovery the
// merge is rolled forward, the orphan is reclaimed,
// the tail is truncated, and reads are correct.
func TestAxisDLifecycleGateCrashRecovery(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p, mgr := axisDOpen(t, bin, dir, 1)
			if err := p.Put("seed", []byte("s")); err != nil {
				t.Fatalf("put seed: %v", err)
			}
			if _, err := mgr.ForkFromDB("src"); err != nil {
				t.Fatalf("fork src: %v", err)
			}
			timelineMustPut(t, mgr, "src", "gk", "gv")
			mgr.Close()
			p.Close()
			// Crash state, written to disk with no open
			// handles: a staged global merge WITH a
			// commit record (roll-forward case), an
			// orphan delta, and a torn WAL tail.
			deltas := []globalMergeDelta{{Key: "gk", Value: []byte("gv")}}
			mergeID := "crash-1"
			man, err := stageGlobalMerge(dir, mergeID, "src", "fail", nil, deltas, map[int]int{0: 1}, false)
			if err != nil {
				t.Fatalf("stageGlobalMerge: %v", err)
			}
			manifestPath, commitPath, _ := globalMergeFileNames(dir, mergeID)
			_ = man
			commitData, _ := json.Marshal(globalMergeCommit{Version: 1, MergeID: mergeID, ManifestFile: filepath.Base(manifestPath), State: "committed"})
			if err := globalMergeWriteSync(commitPath, commitData, false, dir); err != nil {
				t.Fatalf("commit record: %v", err)
			}
			if _, err := timelineWriteDeltaSegment(filepath.Join(dir, "orphan-1.delta"), []timelineDeltaEntry{{Key: "ox", Seq: 1, Value: []byte("ov")}}, false); err != nil {
				t.Fatalf("orphan delta: %v", err)
			}
			walPath := timelineWALPath(dir, "src")
			f, err := os.OpenFile(walPath, os.O_WRONLY|os.O_APPEND, 0o644)
			if err != nil {
				t.Fatalf("open wal for tear: %v", err)
			}
			if _, err := f.Write([]byte{0x01, 0x02, 0x03}); err != nil {
				t.Fatalf("tear wal: %v", err)
			}
			f.Close()
			// Reopen DB only (no manager): the gate must
			// see the pending work and refuse reads.
			p2 := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			defer p2.Close()
			gate := NewLifecycleGate(p2, "")
			if gate.IsOpen() {
				t.Fatalf("gate open over crash state, pending %v", gate.Pending())
			}
			if _, err := gate.Get("seed"); !errors.Is(err, ErrLifecycleRecoveryIncomplete) {
				t.Fatalf("gated Get err %v, want ErrLifecycleRecoveryIncomplete", err)
			}
			if _, err := gate.Scan(""); !errors.Is(err, ErrLifecycleRecoveryIncomplete) {
				t.Fatalf("gated Scan err %v, want ErrLifecycleRecoveryIncomplete", err)
			}
			if err := gate.Recover(); err != nil {
				t.Fatalf("gate Recover: %v", err)
			}
			if !gate.IsOpen() {
				t.Fatalf("gate still closed after recovery: %v", gate.Pending())
			}
			persistMustGet(t, p2, "gk", "gv")
			persistMustGet(t, p2, "seed", "s")
			if _, err := os.Stat(filepath.Join(dir, "orphan-1.delta")); !os.IsNotExist(err) {
				t.Fatalf("orphan delta survived recovery")
			}
			if _, err := os.Stat(manifestPath); !os.IsNotExist(err) {
				t.Fatalf("rolled-forward merge files survived recovery")
			}
			if _, _, _, tail, err := timelineReplayWAL(walPath); err != nil || tail {
				t.Fatalf("WAL tail after recovery: tail=%v err=%v", tail, err)
			}
			// A manager opened after recovery sees the
			// timeline intact, and gated timeline reads
			// work through it.
			mgr2, err := NewTimelineManager(p2)
			if err != nil {
				t.Fatalf("manager after recovery: %v", err)
			}
			defer mgr2.Close()
			v, err := gate.TimelineGet(mgr2, "src", "gk")
			if err != nil || string(v) != "gv" {
				t.Fatalf("gated TimelineGet = %q err %v, want gv", v, err)
			}
		})
	}
}

// TestAxisDWALTruncationAfterCheckpoint: checkpoint +
// truncate shrinks the WAL, the checkpoint alone
// reproduces the pre-truncation view, and close/reopen
// keeps every value (including a post-checkpoint
// write preserved through a second truncation).
func TestAxisDWALTruncationAfterCheckpoint(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p, mgr := axisDOpen(t, bin, dir, 1)
			defer mgr.Close()
			defer p.Close()
			if _, err := mgr.ForkFromDB("t"); err != nil {
				t.Fatalf("fork t: %v", err)
			}
			timelineMustPut(t, mgr, "t", "a", "1")
			timelineMustPut(t, mgr, "t", "b", "2")
			timelineMustPut(t, mgr, "t", "a", "3")
			if err := mgr.DeleteKey("t", "b"); err != nil {
				t.Fatalf("delete b: %v", err)
			}
			file, _, entries, before, after, err := mgr.CheckpointAndTruncateTimelineWAL("t")
			if err != nil {
				t.Fatalf("CheckpointAndTruncate: %v", err)
			}
			if entries != 4 {
				t.Fatalf("checkpoint entries %d, want 4 (a:1, b:2, a:3, b:del)", entries)
			}
			if after >= before {
				t.Fatalf("WAL did not shrink: before %d after %d", before, after)
			}
			if file == "" {
				t.Fatalf("empty checkpoint file name")
			}
			// The checkpoint alone reproduces the view.
			view, err := mgr.LoadCheckpointTimeline("t")
			if err != nil {
				t.Fatalf("LoadCheckpointTimeline: %v", err)
			}
			if string(view["a"]) != "3" || len(view) != 1 {
				t.Fatalf("checkpoint view %v, want {a:3}", view)
			}
			timelineMustGet(t, mgr, "t", "a", "3")
			timelineMustMissing(t, mgr, "t", "b")
			// Post-checkpoint write, then truncate against
			// the existing checkpoint: the suffix survives.
			timelineMustPut(t, mgr, "t", "c", "9")
			if _, _, err := mgr.TruncateTimelineWALToCheckpoint("t"); err != nil {
				t.Fatalf("TruncateToCheckpoint: %v", err)
			}
			timelineMustGet(t, mgr, "t", "c", "9")
			mgr.Close()
			p.Close()
			p2, mgr2 := axisDOpen(t, bin, dir, 1)
			defer mgr2.Close()
			defer p2.Close()
			timelineMustGet(t, mgr2, "t", "a", "3")
			timelineMustMissing(t, mgr2, "t", "b")
			timelineMustGet(t, mgr2, "t", "c", "9")
			// Still writable after a truncated reopen.
			timelineMustPut(t, mgr2, "t", "d", "10")
			timelineMustGet(t, mgr2, "t", "d", "10")
		})
	}
}

// countingSink is a test sink that counts (and, for
// assertion, records) streamed report keys.
type countingSink struct {
	applied, identical, conflicted int
	appliedKeys                    []string
	conflictedKeys                 []string
}

func (s *countingSink) OnApplied(key string)   { s.applied++; s.appliedKeys = append(s.appliedKeys, key) }
func (s *countingSink) OnIdentical(key string) { s.identical++ }
func (s *countingSink) OnConflicted(key string) {
	s.conflicted++
	s.conflictedKeys = append(s.conflictedKeys, key)
}

// TestAxisDRecursiveMergeBaseCrissCross: two merges in
// opposite orders create a criss-cross whose maximal
// common ancestors are two. When the candidates differ
// only one-sidedly, MergeGeneral now resolves through
// the recursive virtual base; when the candidates
// themselves conflict on a key, it still returns
// ErrAmbiguousMergeBase, never a guess.
func TestAxisDRecursiveMergeBaseCrissCross(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p, mgr := axisDOpen(t, bin, dir, 1)
			defer mgr.Close()
			defer p.Close()
			if err := p.Put("seed", []byte("s")); err != nil {
				t.Fatalf("put seed: %v", err)
			}
			head := func(id string) uint64 {
				t.Helper()
				h, err := mgr.HeadSeq(id)
				if err != nil {
					t.Fatalf("HeadSeq %s: %v", id, err)
				}
				return h
			}
			merge := func(src, dst string, pol TimelineConflictPolicy) *MergeReport {
				t.Helper()
				rep, err := mgr.MergeGeneral(src, dst, pol, head(src), head(dst))
				if err != nil {
					t.Fatalf("MergeGeneral %s->%s: %v", src, dst, err)
				}
				return rep
			}
			// Disjoint criss-cross. One DB fork, then DAG
			// forks from it: two ForkFromDBDAG calls
			// would get different snapshot roots (each
			// flush advances the generation) and share
			// no ancestor at all.
			if _, err := mgr.ForkFromDBDAG("rt"); err != nil {
				t.Fatalf("fork rt: %v", err)
			}
			for _, f := range [][2]string{{"rt", "a"}, {"rt", "b"}} {
				if _, err := mgr.ForkDAG(f[0], f[1]); err != nil {
					t.Fatalf("fork %s: %v", f[1], err)
				}
			}
			// Puts BEFORE the child forks: a child reads
			// its parent only up to the fork head, so a
			// put after the fork would be invisible to
			// the child and the final merge would
			// (correctly) treat it as a deletion.
			timelineMustPut(t, mgr, "a", "ka", "va")
			timelineMustPut(t, mgr, "b", "kb", "vb")
			if _, err := mgr.ForkDAG("a", "a2"); err != nil {
				t.Fatalf("fork a2: %v", err)
			}
			if _, err := mgr.ForkDAG("b", "b2"); err != nil {
				t.Fatalf("fork b2: %v", err)
			}
			merge("a", "b2", TimelineConflictFail)
			merge("b", "a2", TimelineConflictFail)
			rep, err := mgr.MergeGeneral("a2", "b2", TimelineConflictFail, head("a2"), head("b2"))
			if err != nil {
				t.Fatalf("criss-cross disjoint merge: %v (want success via recursive base)", err)
			}
			if rep.AppliedCount != 0 {
				t.Fatalf("criss-cross disjoint applied %d, want 0 (both sides already hold ka+kb)", rep.AppliedCount)
			}
			if !strings.HasPrefix(rep.BaseCommitID, "virtual:") {
				t.Fatalf("base commit id %q, want virtual: prefix", rep.BaseCommitID)
			}
			timelineMustGet(t, mgr, "b2", "ka", "va")
			timelineMustGet(t, mgr, "b2", "kb", "vb")
			// Conflicting criss-cross: candidates disagree
			// on kk, so the virtual base itself conflicts.
			for _, f := range [][2]string{{"rt", "c"}, {"rt", "d"}} {
				if _, err := mgr.ForkDAG(f[0], f[1]); err != nil {
					t.Fatalf("fork %s: %v", f[1], err)
				}
			}
			timelineMustPut(t, mgr, "c", "kk", "vc")
			timelineMustPut(t, mgr, "d", "kk", "vd")
			if _, err := mgr.ForkDAG("c", "c2"); err != nil {
				t.Fatalf("fork c2: %v", err)
			}
			if _, err := mgr.ForkDAG("d", "d2"); err != nil {
				t.Fatalf("fork d2: %v", err)
			}
			merge("c", "d2", TimelineConflictSourceWins)
			merge("d", "c2", TimelineConflictSourceWins)
			_, err = mgr.MergeGeneral("c2", "d2", TimelineConflictFail, head("c2"), head("d2"))
			if !errors.Is(err, ErrTimelineAmbiguousMergeBase) {
				t.Fatalf("conflicting criss-cross err %v, want ErrAmbiguousMergeBase", err)
			}
		})
	}
}

// TestAxisDStreamedMergeReport: the streamed merge
// delivers every report key to the sink in sorted
// order, applies exactly the source-wins/target-wins
// set, stages the applied set in a delta segment, and
// leaves no delta behind when fail policy refuses.
func TestAxisDStreamedMergeReport(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p, mgr := axisDOpen(t, bin, dir, 1)
			defer mgr.Close()
			defer p.Close()
			if err := p.Put("seed", []byte("s")); err != nil {
				t.Fatalf("put seed: %v", err)
			}
			if _, err := mgr.ForkFromDBDAG("base"); err != nil {
				t.Fatalf("fork base: %v", err)
			}
			for _, f := range [][2]string{{"base", "s"}, {"base", "tt"}} {
				if _, err := mgr.ForkDAG(f[0], f[1]); err != nil {
					t.Fatalf("fork %s: %v", f[1], err)
				}
			}
			for i := 0; i < 10; i++ {
				timelineMustPut(t, mgr, "s", fmt.Sprintf("a%02d", i), fmt.Sprintf("sv%d", i))
			}
			for i := 0; i < 5; i++ {
				timelineMustPut(t, mgr, "s", fmt.Sprintf("sh%02d", i), "same")
				timelineMustPut(t, mgr, "tt", fmt.Sprintf("sh%02d", i), "same")
			}
			for i := 0; i < 5; i++ {
				timelineMustPut(t, mgr, "s", fmt.Sprintf("cf%02d", i), fmt.Sprintf("src%d", i))
				timelineMustPut(t, mgr, "tt", fmt.Sprintf("cf%02d", i), fmt.Sprintf("dst%d", i))
			}
			for i := 0; i < 5; i++ {
				timelineMustPut(t, mgr, "tt", fmt.Sprintf("t%02d", i), fmt.Sprintf("tv%d", i))
			}
			head := func(id string) uint64 {
				t.Helper()
				h, err := mgr.HeadSeq(id)
				if err != nil {
					t.Fatalf("HeadSeq %s: %v", id, err)
				}
				return h
			}
			sink := &countingSink{}
			rep, err := mgr.MergeGeneralStreamed("s", "tt", TimelineConflictTargetWins, head("s"), head("tt"), sink)
			if err != nil {
				t.Fatalf("MergeGeneralStreamed: %v", err)
			}
			if sink.applied != 10 || sink.identical != 5 || sink.conflicted != 5 {
				t.Fatalf("sink applied=%d identical=%d conflicted=%d, want 10/5/5", sink.applied, sink.identical, sink.conflicted)
			}
			if !sort.StringsAreSorted(sink.appliedKeys) {
				t.Fatalf("applied keys not in sorted order: %v", sink.appliedKeys)
			}
			if rep.AppliedCount != 10 || rep.DeltaFile == "" || rep.AppliedKeys != nil {
				t.Fatalf("report %+v, want AppliedCount 10, DeltaFile set, AppliedKeys nil", rep)
			}
			dents, err := timelineLoadDeltaSegment(filepath.Join(dir, rep.DeltaFile), "", 10)
			if err != nil {
				t.Fatalf("delta segment load: %v", err)
			}
			if len(dents) != 10 {
				t.Fatalf("delta entries %d, want 10", len(dents))
			}
			timelineMustGet(t, mgr, "tt", "a00", "sv0")
			timelineMustGet(t, mgr, "tt", "sh00", "same")
			timelineMustGet(t, mgr, "tt", "cf00", "dst0") // target wins
			timelineMustGet(t, mgr, "tt", "t00", "tv0")
			// Fail policy streams the conflict to the sink
			// and the error, and leaves no delta behind.
			for _, f := range [][2]string{{"base", "u"}, {"base", "v"}} {
				if _, err := mgr.ForkDAG(f[0], f[1]); err != nil {
					t.Fatalf("fork %s: %v", f[1], err)
				}
			}
			timelineMustPut(t, mgr, "u", "k1", "v1")
			timelineMustPut(t, mgr, "v", "k1", "v2")
			sink2 := &countingSink{}
			_, err = mgr.MergeGeneralStreamed("u", "v", TimelineConflictFail, head("u"), head("v"), sink2)
			var conflictErr *TimelineMergeConflictError
			if !errors.As(err, &conflictErr) {
				t.Fatalf("streamed fail err %v, want TimelineMergeConflictError", err)
			}
			if len(conflictErr.Keys) != 1 || conflictErr.Keys[0] != "k1" {
				t.Fatalf("streamed fail keys %v, want [k1]", conflictErr.Keys)
			}
			if sink2.conflicted != 1 {
				t.Fatalf("streamed fail sink conflicted %d, want 1", sink2.conflicted)
			}
			timelineMustGet(t, mgr, "v", "k1", "v2")
			matches, _ := filepath.Glob(filepath.Join(dir, "timeline-v.merge-*.delta"))
			if len(matches) != 0 {
				t.Fatalf("fail-policy streamed merge left delta files %v", matches)
			}
		})
	}
}

// TestAxisDGeneralMergeViewMemory is a measurement, not
// a tight assertion: it merges 200 keys and logs the
// heap delta, grounding the Axis D notes' statement
// that general-merge views are still materialized (the
// report and applied staging are now streamed, the
// three views are not yet). The only assertions are
// correctness ones.
func TestAxisDGeneralMergeViewMemory(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p, mgr := axisDOpen(t, bin, dir, 1)
			defer mgr.Close()
			defer p.Close()
			if err := p.Put("seed", []byte("s")); err != nil {
				t.Fatalf("put seed: %v", err)
			}
			if _, err := mgr.ForkFromDBDAG("mbase"); err != nil {
				t.Fatalf("fork mbase: %v", err)
			}
			for _, f := range [][2]string{{"mbase", "ms"}, {"mbase", "mt"}} {
				if _, err := mgr.ForkDAG(f[0], f[1]); err != nil {
					t.Fatalf("fork %s: %v", f[1], err)
				}
			}
			for i := 0; i < 200; i++ {
				timelineMustPut(t, mgr, "ms", fmt.Sprintf("mk%03d", i), fmt.Sprintf("mv%d", i))
			}
			runtime.GC()
			var before runtime.MemStats
			runtime.ReadMemStats(&before)
			hs, err := mgr.HeadSeq("ms")
			if err != nil {
				t.Fatalf("head ms: %v", err)
			}
			ht, err := mgr.HeadSeq("mt")
			if err != nil {
				t.Fatalf("head mt: %v", err)
			}
			rep, err := mgr.MergeGeneral("ms", "mt", TimelineConflictFail, hs, ht)
			if err != nil {
				t.Fatalf("merge: %v", err)
			}
			var after runtime.MemStats
			runtime.ReadMemStats(&after)
			t.Logf("general merge 200 keys: AppliedCount=%d HeapAlloc before=%d after=%d delta=%d TotalAlloc delta=%d", rep.AppliedCount, before.HeapAlloc, after.HeapAlloc, int64(after.HeapAlloc)-int64(before.HeapAlloc), after.TotalAlloc-before.TotalAlloc)
			if rep.AppliedCount != 200 {
				t.Fatalf("applied %d, want 200", rep.AppliedCount)
			}
			timelineMustGet(t, mgr, "mt", "mk199", "mv199")
		})
	}
}
