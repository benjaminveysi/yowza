package yowza

// Phase C tests: durable-pin wiring (C0) and the commit
// DAG (C2). Each test loops over both wasm binaries and
// uses a fresh directory per binary. TMPDIR must be on
// workspace disk (see the task brief); t.TempDir
// respects it.

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// TestPhaseCPinWiringSurvivesReopenAndCompact: a fork's
// durable pin survives close/reopen + an intervening
// Compact with the old branch rows intact, and the
// timeline keeps working afterwards.
func TestPhaseCPinWiringSurvivesReopenAndCompact(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p, mgr := timelineOpenForTest(t, bin, dir, 2)
			for i := 0; i < 20; i++ {
				if err := p.Put(fmt.Sprintf("w:%03d", i), []byte(fmt.Sprintf("old-%d", i))); err != nil {
					t.Fatalf("put: %v", err)
				}
			}
			info, err := mgr.ForkFromDB("br")
			if err != nil {
				t.Fatalf("ForkFromDB: %v", err)
			}
			if info.PinID == "" {
				t.Fatalf("fork recorded no durable pin")
			}
			pin, err := p.GetPin(info.PinID)
			if err != nil || pin.Status != PinStatusActive {
				t.Fatalf("pin %s not active: %+v err %v", info.PinID, pin, err)
			}
			// Advance the main DB, then close everything.
			for i := 0; i < 20; i++ {
				if err := p.Put(fmt.Sprintf("w:%03d", i), []byte(fmt.Sprintf("new-%d", i))); err != nil {
					t.Fatalf("put2: %v", err)
				}
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}
			mgr.Close()
			p.Close()
			// Reopen: the recovered pin must defer the
			// compact, and the branch still reads old rows.
			p2 := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 2, Durability: DurabilitySync})
			defer p2.Close()
			mgr2, err := NewTimelineManager(p2)
			if err != nil {
				t.Fatalf("reopen manager: %v", err)
			}
			defer mgr2.Close()
			info2, err := mgr2.Info("br")
			if err != nil || info2.PinID != info.PinID {
				t.Fatalf("reopen info %+v err %v, want pin %s", info2, err, info.PinID)
			}
			if err := p2.Compact(0); err != nil {
				t.Fatalf("compact: %v", err)
			}
			for i := 0; i < 20; i++ {
				timelineMustGet(t, mgr2, "br", fmt.Sprintf("w:%03d", i), fmt.Sprintf("old-%d", i))
			}
			// Branch still writable after the reopen.
			timelineMustPut(t, mgr2, "br", "w:999", "branch-only")
			timelineMustGet(t, mgr2, "br", "w:999", "branch-only")
		})
	}
}

// TestPhaseCOrphanPinCleanupOnManagerOpen: a pin whose
// registry commit is missing (crash between pin publish
// and registry publish) is removed on manager open,
// branch DATA is untouched, and no ghost timeline appears.
func TestPhaseCOrphanPinCleanupOnManagerOpen(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p, mgr := timelineOpenForTest(t, bin, dir, 1)
			if err := p.Put("g:1", []byte("ghost-data")); err != nil {
				t.Fatalf("put: %v", err)
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}
			orphan, err := p.CreateDurablePin("Timeline", "ghost", "")
			if err != nil {
				t.Fatalf("orphan pin: %v", err)
			}
			mgr.Close()
			p.Close()
			// Reopen with a manager: the orphan pin must
			// be cleaned by the registry check, and the
			// underlying data must still be there.
			p2 := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			defer p2.Close()
			mgr2, err := NewTimelineManager(p2)
			if err != nil {
				t.Fatalf("reopen manager: %v", err)
			}
			defer mgr2.Close()
			// CleanupOrphanPins marks the pin deleted
			// (tombstone stays in pins.json); GetPin
			// therefore returns either a deleted record
			// or ErrPinNotFound — never an active pin.
			if pin, err := p2.GetPin(orphan.ID); err == nil && pin.Status == PinStatusActive {
				t.Fatalf("orphan pin %s still active after manager open", orphan.ID)
			} else if err != nil && !errors.Is(err, ErrPinNotFound) {
				t.Fatalf("GetPin orphan: %v", err)
			}
			if _, err := mgr2.Info("ghost"); err == nil {
				t.Fatalf("ghost timeline appeared after orphan cleanup")
			}
			persistMustGet(t, p2, "g:1", "ghost-data")
		})
	}
}

// TestPhaseCDeleteTimelineDeletesPin: registry tombstone
// first, then DeletePin — after a delete the pin is
// gone (deleted, never active) and the WAL is removed.
func TestPhaseCDeleteTimelineDeletesPin(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p, mgr := timelineOpenForTest(t, bin, dir, 1)
			if err := p.Put("d:1", []byte("v")); err != nil {
				t.Fatalf("put: %v", err)
			}
			info, err := mgr.ForkFromDB("del")
			if err != nil {
				t.Fatalf("fork: %v", err)
			}
			if err := mgr.DeleteTimeline("del"); err != nil {
				t.Fatalf("delete: %v", err)
			}
			if pin, err := p.GetPin(info.PinID); err == nil && pin.Status == PinStatusActive {
				t.Fatalf("pin still active after timeline delete: %+v", pin)
			}
			if _, err := os.Stat(filepath.Join(dir, "timeline-del.wal")); !os.IsNotExist(err) {
				t.Fatalf("WAL still exists after delete (err %v)", err)
			}
		})
	}
}

// TestPhaseCV1RegistryMigration: a registry document
// without any Phase C field (the v1 shape: no pin_id,
// no dag_commits, no snapshot roots) opens, migrates
// its timelines to durable pins, and loses no timeline
// data or ID/number.
func TestPhaseCV1RegistryMigration(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p, mgr := timelineOpenForTest(t, bin, dir, 1)
			if err := p.Put("m:1", []byte("keep")); err != nil {
				t.Fatalf("put: %v", err)
			}
			info, err := mgr.ForkFromDB("mig")
			if err != nil {
				t.Fatalf("fork: %v", err)
			}
			timelineMustPut(t, mgr, "mig", "m:2", "branch")
			mgr.Close()
			p.Close()
			// Strip every Phase C field from the registry,
			// producing a genuine v1 document.
			raw, err := os.ReadFile(filepath.Join(dir, "timeline-registry.json"))
			if err != nil {
				t.Fatalf("read registry: %v", err)
			}
			var doc map[string]any
			if err := json.Unmarshal(raw, &doc); err != nil {
				t.Fatalf("parse registry: %v", err)
			}
			delete(doc, "dag_commits")
			delete(doc, "snapshot_roots")
			if tls, ok := doc["timelines"].([]any); ok {
				for _, rec := range tls {
					if m, ok := rec.(map[string]any); ok {
						delete(m, "pin_id")
						delete(m, "head_commit_id")
						delete(m, "snapshot_root_id")
						delete(m, "checkpoint_file")
						delete(m, "checkpoint_gen")
					}
				}
			}
			raw, err = json.Marshal(doc)
			if err != nil {
				t.Fatalf("marshal v1 registry: %v", err)
			}
			if err := os.WriteFile(filepath.Join(dir, "timeline-registry.json"), raw, 0o644); err != nil {
				t.Fatalf("write v1 registry: %v", err)
			}
			p2 := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			defer p2.Close()
			mgr2, err := NewTimelineManager(p2)
			if err != nil {
				t.Fatalf("reopen v1 registry: %v", err)
			}
			defer mgr2.Close()
			info2, err := mgr2.Info("mig")
			if err != nil {
				t.Fatalf("migrated timeline missing: %v", err)
			}
			if info2.Number != info.Number {
				t.Fatalf("timeline number changed: %d -> %d", info.Number, info2.Number)
			}
			if info2.PinID == "" {
				t.Fatalf("migration did not assign a durable pin")
			}
			timelineMustGet(t, mgr2, "mig", "m:1", "keep")
			timelineMustGet(t, mgr2, "mig", "m:2", "branch")
			// The migrated timeline is fully operable.
			timelineMustPut(t, mgr2, "mig", "m:3", "after-migration")
			timelineMustGet(t, mgr2, "mig", "m:3", "after-migration")
		})
	}
}

// TestPhaseCDAGForkNoScan: forking a large database via
// the DAG costs no full-view materialization: the
// scanAtVector counter does not move across either DAG
// fork, and both forks still resolve reads correctly.
func TestPhaseCDAGForkNoScan(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p, mgr := timelineOpenForTest(t, bin, dir, 2)
			for i := 0; i < 1500; i++ {
				if err := p.Put(fmt.Sprintf("big:%05d", i), []byte(fmt.Sprintf("v-%d", i))); err != nil {
					t.Fatalf("put %d: %v", i, err)
				}
			}
			before := TimelineScanAtVectorCalls()
			if _, err := mgr.ForkFromDBDAG("dag1"); err != nil {
				t.Fatalf("ForkFromDBDAG: %v", err)
			}
			if got := TimelineScanAtVectorCalls(); got != before {
				t.Fatalf("DAG fork from DB scanned: counter %d -> %d", before, got)
			}
			// Main advances; the DAG branch keeps old values.
			if err := p.Put("big:00000", []byte("new")); err != nil {
				t.Fatalf("main put: %v", err)
			}
			timelineMustGet(t, mgr, "dag1", "big:00000", "v-0")
			timelineMustGet(t, mgr, "dag1", "big:01499", "v-1499")
			// A DAG fork from a DAG timeline also scans nothing.
			timelineMustPut(t, mgr, "dag1", "dag-only", "yes")
			before = TimelineScanAtVectorCalls()
			if _, err := mgr.ForkDAG("dag1", "dag2"); err != nil {
				t.Fatalf("ForkDAG: %v", err)
			}
			if got := TimelineScanAtVectorCalls(); got != before {
				t.Fatalf("DAG fork from timeline scanned: counter %d -> %d", before, got)
			}
			timelineMustGet(t, mgr, "dag2", "dag-only", "yes")
			// Parent advances after the child fork; the
			// child does not see it (fork-head limit).
			timelineMustPut(t, mgr, "dag1", "dag-only", "changed")
			timelineMustGet(t, mgr, "dag2", "dag-only", "yes")
		})
	}
}

// TestPhaseCDAGGeneralMergeGrandparent: a grandchild
// merges into its grandparent through the DAG base
// (the grandparent's head at the middle fork), with
// dual head control and a delta-segment commit.
func TestPhaseCDAGGeneralMergeGrandparent(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p, mgr := timelineOpenForTest(t, bin, dir, 1)
			if err := p.Put("base", []byte("db")); err != nil {
				t.Fatalf("put: %v", err)
			}
			if _, err := mgr.ForkFromDBDAG("a"); err != nil {
				t.Fatalf("fork a: %v", err)
			}
			timelineMustPut(t, mgr, "a", "k-a", "va")
			if _, err := mgr.ForkDAG("a", "b"); err != nil {
				t.Fatalf("fork b: %v", err)
			}
			timelineMustPut(t, mgr, "b", "k-b", "vb")
			if _, err := mgr.ForkDAG("b", "c"); err != nil {
				t.Fatalf("fork c: %v", err)
			}
			timelineMustPut(t, mgr, "c", "k-c", "vc")
			aHead, _ := mgr.HeadSeq("a")
			cHead, _ := mgr.HeadSeq("c")
			// Wrong source head: refused, nothing applied.
			if _, err := mgr.MergeGeneral("c", "a", TimelineConflictFail, cHead+9, aHead); err == nil {
				t.Fatalf("merge with stale source head accepted")
			} else if !errors.Is(err, ErrTimelineStaleHead) {
				t.Fatalf("stale source head err %v, want StaleHead", err)
			}
			timelineMustMissing(t, mgr, "a", "k-c")
			// Wrong target head: refused.
			if _, err := mgr.MergeGeneral("c", "a", TimelineConflictFail, cHead, aHead+9); err == nil {
				t.Fatalf("merge with stale target head accepted")
			} else if !errors.Is(err, ErrTimelineStaleHead) {
				t.Fatalf("stale target head err %v, want StaleHead", err)
			}
			// The real grandparent merge: base is a's head
			// at b's fork (which already holds k-a), so
			// only k-b and k-c are one-sided source changes.
			rep, err := mgr.MergeGeneral("c", "a", TimelineConflictFail, cHead, aHead)
			if err != nil {
				t.Fatalf("MergeGeneral grandparent: %v", err)
			}
			if rep.AppliedCount != 2 || rep.DeltaFile == "" {
				t.Fatalf("merge report %+v, want AppliedCount 2 + delta file", rep)
			}
			if rep.BaseCommitID == "" {
				t.Fatalf("merge report has no DAG base commit")
			}
			if _, err := os.Stat(filepath.Join(dir, rep.DeltaFile)); err != nil {
				t.Fatalf("delta file %s missing: %v", rep.DeltaFile, err)
			}
			timelineMustGet(t, mgr, "a", "k-b", "vb")
			timelineMustGet(t, mgr, "a", "k-c", "vc")
			timelineMustGet(t, mgr, "a", "k-a", "va")
			// The merge-ref commit survives reopen: the
			// delta expands from the WAL record alone.
			mgr.Close()
			p.Close()
			p2 := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			defer p2.Close()
			mgr2, err := NewTimelineManager(p2)
			if err != nil {
				t.Fatalf("reopen after DAG merge: %v", err)
			}
			defer mgr2.Close()
			timelineMustGet(t, mgr2, "a", "k-b", "vb")
			timelineMustGet(t, mgr2, "a", "k-c", "vc")
		})
	}
}

// TestPhaseCDAGAmbiguousMergeBase: a criss-cross graph
// (two merges in opposite orders) has two maximal common
// ancestors, and findMergeBase refuses to guess one.
func TestPhaseCDAGAmbiguousMergeBase(t *testing.T) {
	m := &TimelineManager{dagCommits: map[string]*TimelineCommit{}}
	add := func(id string, parents ...string) {
		m.dagCommits[id] = &TimelineCommit{ID: id, ParentIDs: parents}
	}
	add("root")
	add("p", "root")
	add("q", "root")
	add("m1", "p", "q")
	add("m2", "q", "p")
	_, err := m.findMergeBase("m1", "m2")
	if err == nil {
		t.Fatalf("criss-cross merge base accepted")
	}
	var amb *TimelineAmbiguousMergeBaseError
	if !errors.As(err, &amb) {
		t.Fatalf("err %v, want ErrAmbiguousMergeBase", err)
	}
	if len(amb.Candidates) != 2 {
		t.Fatalf("candidates %v, want exactly [p q]", amb.Candidates)
	}
	// And the unambiguous case still resolves: merging
	// m1 into a descendant of m1 finds m1 itself.
	add("d1", "m1")
	base, err := m.findMergeBase("m1", "d1")
	if err != nil || base.ID != "m1" {
		t.Fatalf("base(m1,d1) = %+v err %v, want m1", base, err)
	}
}

// TestPhaseCDAGLargeMergeOrphanDelta: a staged delta
// without a commit record is an orphan — cleaned on
// the next open, never partially applied — while a
// committed large merge's delta is kept and replays.
func TestPhaseCDAGLargeMergeOrphanDelta(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p, mgr := timelineOpenForTest(t, bin, dir, 1)
			if err := p.Put("seed", []byte("s")); err != nil {
				t.Fatalf("put: %v", err)
			}
			if _, err := mgr.ForkFromDBDAG("a"); err != nil {
				t.Fatalf("fork a: %v", err)
			}
			if _, err := mgr.ForkDAG("a", "b"); err != nil {
				t.Fatalf("fork b: %v", err)
			}
			// A committed large merge (300 keys) via the
			// delta protocol.
			for i := 0; i < 300; i++ {
				timelineMustPut(t, mgr, "b", fmt.Sprintf("big:%03d", i), fmt.Sprintf("v%d", i))
			}
			bHead, _ := mgr.HeadSeq("b")
			aHead, _ := mgr.HeadSeq("a")
			rep, err := mgr.MergeGeneral("b", "a", TimelineConflictFail, bHead, aHead)
			if err != nil {
				t.Fatalf("large merge: %v", err)
			}
			if rep.AppliedCount != 300 {
				t.Fatalf("applied count %d, want 300", rep.AppliedCount)
			}
			// Stage an orphan delta (crash between delta
			// write and commit record), then reopen: the
			// orphan must be gone and nothing applied.
			orphan := "timeline-a.merge-orphan.delta"
			if _, err := timelineWriteDeltaSegment(filepath.Join(dir, orphan),
				[]timelineDeltaEntry{{Key: "orphan-key", Seq: 999, Value: []byte("o")}}, false); err != nil {
				t.Fatalf("stage orphan delta: %v", err)
			}
			mgr.Close()
			mgr2, err := NewTimelineManager(p)
			if err != nil {
				t.Fatalf("reopen manager: %v", err)
			}
			defer mgr2.Close()
			if _, err := os.Stat(filepath.Join(dir, orphan)); !os.IsNotExist(err) {
				t.Fatalf("orphan delta still present after open (err %v)", err)
			}
			if _, err := os.Stat(filepath.Join(dir, rep.DeltaFile)); err != nil {
				t.Fatalf("committed delta %s removed by orphan cleanup: %v", rep.DeltaFile, err)
			}
			timelineMustMissing(t, mgr2, "a", "orphan-key")
			timelineMustGet(t, mgr2, "a", "big:299", "v299")
		})
	}
}

// TestPhaseCDAGCheckpoint: a delta-segment checkpoint
// reproduces the live view exactly, survives reopen,
// and is not cleaned as an orphan.
func TestPhaseCDAGCheckpoint(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p, mgr := timelineOpenForTest(t, bin, dir, 1)
			if err := p.Put("seed", []byte("s")); err != nil {
				t.Fatalf("put: %v", err)
			}
			if _, err := mgr.ForkFromDBDAG("a"); err != nil {
				t.Fatalf("fork: %v", err)
			}
			timelineMustPut(t, mgr, "a", "x", "1")
			timelineMustPut(t, mgr, "a", "y", "2")
			timelineMustPut(t, mgr, "a", "gone", "g")
			if err := mgr.DeleteKey("a", "gone"); err != nil {
				t.Fatalf("delete: %v", err)
			}
			file, gen, count, err := mgr.CheckpointTimeline("a")
			if err != nil {
				t.Fatalf("checkpoint: %v", err)
			}
			if gen != 1 || count != 4 {
				t.Fatalf("checkpoint gen %d count %d, want 1 / 4", gen, count)
			}
			view, err := mgr.LoadCheckpointTimeline("a")
			if err != nil {
				t.Fatalf("load checkpoint: %v", err)
			}
			if len(view) != 3 || string(view["x"]) != "1" || string(view["y"]) != "2" || string(view["seed"]) != "s" {
				t.Fatalf("checkpoint view %v, want x/1 y/2 seed/s", view)
			}
			if _, ok := view["gone"]; ok {
				t.Fatalf("checkpoint view still holds deleted key")
			}
			// Retention floor is defined while pins exist.
			if _, n, err := mgr.OldestPinnedSequence(); err != nil || n < 1 {
				t.Fatalf("oldest pinned: floor err %v count %d", err, n)
			}
			mgr.Close()
			mgr2, err := NewTimelineManager(p)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			defer mgr2.Close()
			if _, err := os.Stat(filepath.Join(dir, file)); err != nil {
				t.Fatalf("checkpoint file cleaned on reopen: %v", err)
			}
			view2, err := mgr2.LoadCheckpointTimeline("a")
			if err != nil {
				t.Fatalf("load checkpoint after reopen: %v", err)
			}
			if len(view2) != 3 || string(view2["x"]) != "1" {
				t.Fatalf("checkpoint view after reopen %v", view2)
			}
			// A corrupted checkpoint fails loudly, never partially.
			if err := os.WriteFile(filepath.Join(dir, file), []byte("corrupt\n"), 0o644); err != nil {
				t.Fatalf("corrupt checkpoint: %v", err)
			}
			if _, err := mgr2.LoadCheckpointTimeline("a"); err == nil {
				t.Fatalf("corrupted checkpoint loaded without error")
			}
		})
	}
}
