package yowza

// Timeline tests (both wasm binaries, real PersistedSegDB).
// Each test loops over persistTestBins and uses a fresh
// directory per binary, so the two binaries never share state.

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
)

func timelineOpenForTest(t *testing.T, bin, dir string, shards int) (*PersistedSegDB, *TimelineManager) {
	t.Helper()
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
	return p, mgr
}

func timelineMustGet(t *testing.T, mgr *TimelineManager, id, key, want string) {
	t.Helper()
	v, err := mgr.Get(id, key)
	if err != nil || string(v) != want {
		t.Fatalf("Get(%s,%q) = %q, err %v, want %q", id, key, v, err, want)
	}
}

func timelineMustMissing(t *testing.T, mgr *TimelineManager, id, key string) {
	t.Helper()
	if v, err := mgr.Get(id, key); err == nil {
		t.Fatalf("Get(%s,%q) = %q, want missing", id, key, v)
	} else if !errors.Is(err, ErrTimelineKeyNotFound) {
		t.Fatalf("Get(%s,%q) err %v, want ErrTimelineKeyNotFound", id, key, err)
	}
}

func timelineMustPut(t *testing.T, mgr *TimelineManager, id, key, val string) {
	t.Helper()
	if err := mgr.Put(id, key, []byte(val)); err != nil {
		t.Fatalf("Put(%s,%q): %v", id, key, err)
	}
}

// (a) Fork from a specific OLD version: the child still sees
// the branch-point state after the parent advances, and a
// grandchild sees the parent view at fork time, not later.
func TestTimelineForkFromOldVersion(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p, mgr := timelineOpenForTest(t, bin, dir, 1)
			if err := p.Put("a", []byte("v1")); err != nil {
				t.Fatalf("main put: %v", err)
			}
			if err := p.Put("b", []byte("keep")); err != nil {
				t.Fatalf("main put: %v", err)
			}
			oldVec, _, err := p.SnapshotVector()
			if err != nil {
				t.Fatalf("snapshot vector: %v", err)
			}
			if _, err := mgr.ForkFromDB("child"); err != nil {
				t.Fatalf("ForkFromDB: %v", err)
			}
			// Parent (main DB) advances after the fork.
			if err := p.Put("a", []byte("v2")); err != nil {
				t.Fatalf("main put: %v", err)
			}
			if err := p.Delete("b"); err != nil {
				t.Fatalf("main delete: %v", err)
			}
			if err := p.Put("c", []byte("new")); err != nil {
				t.Fatalf("main put: %v", err)
			}
			timelineMustGet(t, mgr, "child", "a", "v1")
			timelineMustGet(t, mgr, "child", "b", "keep")
			timelineMustMissing(t, mgr, "child", "c")
			// Explicit fork from the pinned old vector works.
			if _, err := mgr.ForkAt(oldVec, "explicit"); err != nil {
				t.Fatalf("ForkAt old pinned vector: %v", err)
			}
			timelineMustGet(t, mgr, "explicit", "a", "v1")
			timelineMustGet(t, mgr, "explicit", "b", "keep")
			// Grandchild: parent timeline writes, fork, then
			// parent writes again — grandchild sees only the
			// fork-time parent view.
			timelineMustPut(t, mgr, "child", "x", "childx")
			if _, err := mgr.Fork("child", "grand"); err != nil {
				t.Fatalf("Fork grand: %v", err)
			}
			timelineMustPut(t, mgr, "child", "y", "childy")
			timelineMustGet(t, mgr, "grand", "x", "childx")
			timelineMustMissing(t, mgr, "grand", "y")
			timelineMustGet(t, mgr, "grand", "a", "v1")
		})
	}
}

// (b) Independent changes in two timelines: neither sees the
// other's writes, and the main database is untouched.
func TestTimelineIndependentChanges(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p, mgr := timelineOpenForTest(t, bin, dir, 1)
			if err := p.Put("k", []byte("base")); err != nil {
				t.Fatalf("main put: %v", err)
			}
			if _, err := mgr.ForkFromDB("ta"); err != nil {
				t.Fatalf("fork ta: %v", err)
			}
			if _, err := mgr.ForkFromDB("tb"); err != nil {
				t.Fatalf("fork tb: %v", err)
			}
			timelineMustPut(t, mgr, "ta", "k", "va")
			timelineMustPut(t, mgr, "ta", "onlya", "1")
			timelineMustPut(t, mgr, "tb", "k", "vb")
			timelineMustPut(t, mgr, "tb", "onlyb", "2")
			timelineMustGet(t, mgr, "ta", "k", "va")
			timelineMustMissing(t, mgr, "ta", "onlyb")
			timelineMustGet(t, mgr, "tb", "k", "vb")
			timelineMustMissing(t, mgr, "tb", "onlya")
			persistMustGet(t, p, "k", "base")
			if v, err := p.Get("onlya"); err == nil {
				t.Fatalf("main DB leaked timeline write onlya=%q", v)
			}
		})
	}
}

// (c) Compare/diff correctness, including tombstone-vs-absent:
// a deleted base key shows Deleted, a never-existing key the
// timeline deletes shows Deleted against an Absent base, and
// an untouched absent key does not appear at all.
func TestTimelineCompareTombstoneVsAbsent(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			_, mgr := timelineOpenForTest(t, bin, dir, 1)
			p := mgr.db
			if err := p.Put("a", []byte("aval")); err != nil {
				t.Fatalf("main put: %v", err)
			}
			if err := p.Put("b", []byte("bval")); err != nil {
				t.Fatalf("main put: %v", err)
			}
			if _, err := mgr.ForkFromDB("t1"); err != nil {
				t.Fatalf("fork: %v", err)
			}
			if err := mgr.DeleteKey("t1", "a"); err != nil {
				t.Fatalf("deletekey: %v", err)
			}
			if err := mgr.DeleteKey("t1", "ghost"); err != nil {
				t.Fatalf("deletekey ghost: %v", err)
			}
			timelineMustPut(t, mgr, "t1", "d", "dval")
			diff, err := mgr.CompareToBranchPoint("t1")
			if err != nil {
				t.Fatalf("CompareToBranchPoint: %v", err)
			}
			byKey := map[string]TimelineDiffEntry{}
			for _, e := range diff {
				byKey[e.Key] = e
			}
			if len(diff) != 3 {
				t.Fatalf("diff = %v, want exactly a, ghost, d", diff)
			}
			a := byKey["a"]
			if a.Base.State != TimelinePresent || string(a.Base.Value) != "aval" || a.SideB.State != TimelineDeleted {
				t.Fatalf("a diff = %+v, want Base Present aval, SideB Deleted", a)
			}
			g := byKey["ghost"]
			if g.Base.State != TimelineAbsent || g.SideB.State != TimelineDeleted {
				t.Fatalf("ghost diff = %+v, want Base Absent, SideB Deleted (tombstone != absent)", g)
			}
			d := byKey["d"]
			if d.Base.State != TimelineAbsent || d.SideB.State != TimelinePresent || string(d.SideB.Value) != "dval" {
				t.Fatalf("d diff = %+v, want Base Absent, SideB Present dval", d)
			}
			if _, ok := byKey["b"]; ok {
				t.Fatalf("untouched key b must not appear in diff")
			}
			// Sorted order.
			if diff[0].Key != "a" || diff[1].Key != "d" || diff[2].Key != "ghost" {
				t.Fatalf("diff not key-sorted: %v", diff)
			}
		})
	}
}

// (d) Clean three-way merge: one-sided + identical changes.
func TestTimelineCleanMerge(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p, mgr := timelineOpenForTest(t, bin, dir, 1)
			if err := p.Put("shared", []byte("s0")); err != nil {
				t.Fatalf("main put: %v", err)
			}
			if err := p.Put("keep", []byte("k0")); err != nil {
				t.Fatalf("main put: %v", err)
			}
			if _, err := mgr.ForkFromDB("parent"); err != nil {
				t.Fatalf("fork parent: %v", err)
			}
			if _, err := mgr.Fork("parent", "child"); err != nil {
				t.Fatalf("fork child: %v", err)
			}
			timelineMustPut(t, mgr, "parent", "ponly", "p")
			timelineMustPut(t, mgr, "parent", "shared", "same")
			timelineMustPut(t, mgr, "child", "conly", "c")
			timelineMustPut(t, mgr, "child", "shared", "same")
			head, err := mgr.HeadSeq("parent")
			if err != nil {
				t.Fatalf("head: %v", err)
			}
			rep, err := mgr.Merge("child", "parent", TimelineConflictFail, head)
			if err != nil {
				t.Fatalf("merge: %v", err)
			}
			if len(rep.ConflictedKeys) != 0 {
				t.Fatalf("conflicts = %v, want none", rep.ConflictedKeys)
			}
			if len(rep.AppliedKeys) != 1 || rep.AppliedKeys[0] != "conly" {
				t.Fatalf("applied = %v, want [conly]", rep.AppliedKeys)
			}
			if len(rep.IdenticalKeys) != 1 || rep.IdenticalKeys[0] != "shared" {
				t.Fatalf("identical = %v, want [shared]", rep.IdenticalKeys)
			}
			if rep.MergedTimeline != "parent" || rep.NewHead != head+1 {
				t.Fatalf("report = %+v, want merged parent new head %d", rep, head+1)
			}
			timelineMustGet(t, mgr, "parent", "conly", "c")
			timelineMustGet(t, mgr, "parent", "ponly", "p")
			timelineMustGet(t, mgr, "parent", "shared", "same")
			timelineMustGet(t, mgr, "parent", "keep", "k0")
		})
	}
}

// (e) Conflicting merge under fail policy: typed error,
// NOTHING applied, both timelines and both heads unchanged.
func TestTimelineConflictingMergeFailsAtomic(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p, mgr := timelineOpenForTest(t, bin, dir, 1)
			if err := p.Put("k", []byte("base")); err != nil {
				t.Fatalf("main put: %v", err)
			}
			if _, err := mgr.ForkFromDB("parent"); err != nil {
				t.Fatalf("fork parent: %v", err)
			}
			if _, err := mgr.Fork("parent", "child"); err != nil {
				t.Fatalf("fork child: %v", err)
			}
			timelineMustPut(t, mgr, "parent", "k", "parentval")
			timelineMustPut(t, mgr, "child", "k", "childval")
			pHead, _ := mgr.HeadSeq("parent")
			cHead, _ := mgr.HeadSeq("child")
			_, err := mgr.Merge("child", "parent", TimelineConflictFail, pHead)
			var cerr *TimelineMergeConflictError
			if !errors.As(err, &cerr) {
				t.Fatalf("merge err = %v, want *TimelineMergeConflictError", err)
			}
			if !errors.Is(err, ErrTimelineMergeConflict) {
				t.Fatalf("merge err %v does not wrap ErrTimelineMergeConflict", err)
			}
			if len(cerr.Keys) != 1 || cerr.Keys[0] != "k" {
				t.Fatalf("conflict keys = %v, want [k]", cerr.Keys)
			}
			timelineMustGet(t, mgr, "parent", "k", "parentval")
			timelineMustGet(t, mgr, "child", "k", "childval")
			if h, _ := mgr.HeadSeq("parent"); h != pHead {
				t.Fatalf("parent head moved %d -> %d on failed merge", pHead, h)
			}
			if h, _ := mgr.HeadSeq("child"); h != cHead {
				t.Fatalf("child head moved %d -> %d on failed merge", cHead, h)
			}
		})
	}
}

// (f) Resolution policies with full MergeReports.
func TestTimelineMergeResolution(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			for _, tc := range []struct {
				policy  TimelineConflictPolicy
				want    string
				applied bool
			}{
				{TimelineConflictSourceWins, "childval", true},
				{TimelineConflictTargetWins, "parentval", false},
			} {
				t.Run(tc.policy.String(), func(t *testing.T) {
					dir := t.TempDir()
					p, mgr := timelineOpenForTest(t, bin, dir, 1)
					if err := p.Put("k", []byte("base")); err != nil {
						t.Fatalf("main put: %v", err)
					}
					if _, err := mgr.ForkFromDB("parent"); err != nil {
						t.Fatalf("fork: %v", err)
					}
					if _, err := mgr.Fork("parent", "child"); err != nil {
						t.Fatalf("fork: %v", err)
					}
					timelineMustPut(t, mgr, "parent", "k", "parentval")
					timelineMustPut(t, mgr, "child", "k", "childval")
					head, _ := mgr.HeadSeq("parent")
					rep, err := mgr.Merge("child", "parent", tc.policy, head)
					if err != nil {
						t.Fatalf("merge: %v", err)
					}
					if len(rep.ConflictedKeys) != 1 || rep.ConflictedKeys[0] != "k" {
						t.Fatalf("conflicted = %v, want [k]", rep.ConflictedKeys)
					}
					if rep.Policy != tc.policy {
						t.Fatalf("policy = %v, want %v", rep.Policy, tc.policy)
					}
					hasK := false
					for _, k := range rep.AppliedKeys {
						if k == "k" {
							hasK = true
						}
					}
					if hasK != tc.applied {
						t.Fatalf("applied contains k = %v, want %v (applied=%v)", hasK, tc.applied, rep.AppliedKeys)
					}
					timelineMustGet(t, mgr, "parent", "k", tc.want)
				})
			}
		})
	}
}

// (g) Fork + merge after close/reopen: merged state survives;
// a torn WAL tail is ignored (never half); a merge record
// fsynced but not registry-published resolves to post-merge.
func TestTimelineMergeAfterReopen(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p, mgr := timelineOpenForTest(t, bin, dir, 1)
			// NOTE: timelineOpenForTest registers a cleanup
			// closing mgr+p at test end; reopen cycles below
			// manage their own handles and re-register by
			// closing the cleanup handles early via explicit
			// closes (the cleanup Close is idempotent).
			if err := p.Put("k", []byte("base")); err != nil {
				t.Fatalf("main put: %v", err)
			}
			if _, err := mgr.ForkFromDB("parent"); err != nil {
				t.Fatalf("fork: %v", err)
			}
			if _, err := mgr.Fork("parent", "child"); err != nil {
				t.Fatalf("fork: %v", err)
			}
			timelineMustPut(t, mgr, "child", "merged", "yes")
			head, _ := mgr.HeadSeq("parent")
			if _, err := mgr.Merge("child", "parent", TimelineConflictFail, head); err != nil {
				t.Fatalf("merge: %v", err)
			}
			// Abrupt close: no FlushTimeline call.
			if err := mgr.Close(); err != nil {
				t.Fatalf("mgr close: %v", err)
			}
			if err := p.Close(); err != nil {
				t.Fatalf("db close: %v", err)
			}
			p2 := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			mgr2, err := NewTimelineManager(p2)
			if err != nil {
				t.Fatalf("reopen manager: %v", err)
			}
			timelineMustGet(t, mgr2, "parent", "merged", "yes")
			timelineMustGet(t, mgr2, "child", "merged", "yes")
			// Torn tail: garbage appended to the parent WAL
			// must be truncated on the next open, state intact.
			if err := mgr2.Close(); err != nil {
				t.Fatalf("mgr2 close: %v", err)
			}
			if err := p2.Close(); err != nil {
				t.Fatalf("db2 close: %v", err)
			}
			f, err := os.OpenFile(filepath.Join(dir, "timeline-parent.wal"), os.O_WRONLY|os.O_APPEND, 0o644)
			if err != nil {
				t.Fatalf("open wal: %v", err)
			}
			if _, err := f.Write([]byte{0x00, 0x01, 0x02, 0x03, 0xff}); err != nil {
				t.Fatalf("write garbage: %v", err)
			}
			f.Close()
			p3 := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			mgr3, err := NewTimelineManager(p3)
			if err != nil {
				t.Fatalf("reopen manager3: %v", err)
			}
			// Defer order: db defer registered first, so
			// the manager closes first (manager-before-DB).
			defer p3.Close()
			defer mgr3.Close()
			timelineMustGet(t, mgr3, "parent", "merged", "yes")
			// Unfinalized merge: a complete merge WAL record
			// with NO registry publish (crash between the two
			// commit steps) resolves to post-merge on reopen.
			timelineMustPut(t, mgr3, "child", "late", "lateval")
			head3, _ := mgr3.HeadSeq("parent")
			// Craft the merge record by hand (same-package
			// white-box crash simulation), append + fsync,
			// but do NOT publish the registry or touch the
			// in-memory overlay/head.
			ts := mgr3.timelines["parent"]
			payload := &timelineMergePayload{
				SourceID: "child", TargetID: "parent",
				SourceHead: 2, TargetBefore: head3, NewHead: head3 + 1,
				Policy:  "fail",
				Applied: []timelineMergeAppliedKey{{Key: "late", Value: []byte("lateval")}},
				// Phase A replay validates merge payloads:
				// a hand-crafted record must carry a
				// well-formed base fingerprint like a real
				// merge record does (64 hex chars).
				BaseFP: timelineFingerprint(map[string][]byte{}),
			}
			rec, err := timelineEncodeRecord(head3+1, timelineWALMerge, "", nil, false, payload)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			ts.mu.Lock()
			if _, err := ts.wal.Write(rec); err != nil {
				ts.mu.Unlock()
				t.Fatalf("write merge record: %v", err)
			}
			if err := ts.wal.Sync(); err != nil {
				ts.mu.Unlock()
				t.Fatalf("sync merge record: %v", err)
			}
			ts.mu.Unlock()
			if err := mgr3.Close(); err != nil {
				t.Fatalf("mgr3 close: %v", err)
			}
			if err := p3.Close(); err != nil {
				t.Fatalf("db3 close: %v", err)
			}
			p4 := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			mgr4, err := NewTimelineManager(p4)
			if err != nil {
				t.Fatalf("reopen manager4: %v", err)
			}
			defer p4.Close()
			defer mgr4.Close()
			// Either pre-merge (record rolled back) or
			// post-merge is allowed; half-merged is not.
			// This implementation adopts the durable record:
			// post-merge.
			timelineMustGet(t, mgr4, "parent", "late", "lateval")
			timelineMustGet(t, mgr4, "parent", "merged", "yes")
		})
	}
}

// (h) Fork from a gone branch point fails with a typed error,
// never silently from a newer version.
func TestTimelineForkAtGoneHistoryFails(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p, mgr := timelineOpenForTest(t, bin, dir, 1)
			if err := p.Put("a", []byte("v1")); err != nil {
				t.Fatalf("main put: %v", err)
			}
			if err := p.Put("b", []byte("keep")); err != nil {
				t.Fatalf("main put: %v", err)
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}
			oldVec, _, err := p.SnapshotVector()
			if err != nil {
				t.Fatalf("snapshot vector: %v", err)
			}
			// Advance + flush + compact with NO timeline
			// pinning oldVec: the old versions are dropped.
			if err := p.Put("a", []byte("v2")); err != nil {
				t.Fatalf("main put: %v", err)
			}
			if err := p.Delete("b"); err != nil {
				t.Fatalf("main delete: %v", err)
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}
			if err := p.Compact(0); err != nil {
				t.Fatalf("compact: %v", err)
			}
			_, err = mgr.ForkAt(oldVec, "doomed")
			if !errors.Is(err, ErrTimelineBranchPointGone) {
				t.Fatalf("ForkAt gone vector err = %v, want ErrTimelineBranchPointGone", err)
			}
			// Forking from the current vector still works.
			if _, err := mgr.ForkFromDB("live"); err != nil {
				t.Fatalf("ForkFromDB current: %v", err)
			}
			timelineMustGet(t, mgr, "live", "a", "v2")
			timelineMustMissing(t, mgr, "live", "b")
		})
	}
}

// (i) Delete-with-children refused; IDs (and numbers) never reused.
func TestTimelineDeleteWithChildrenRefused(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			_, mgr := timelineOpenForTest(t, bin, dir, 1)
			p := mgr.db
			if err := p.Put("k", []byte("v")); err != nil {
				t.Fatalf("main put: %v", err)
			}
			pi, err := mgr.ForkFromDB("parent")
			if err != nil {
				t.Fatalf("fork parent: %v", err)
			}
			if _, err := mgr.Fork("parent", "child"); err != nil {
				t.Fatalf("fork child: %v", err)
			}
			err = mgr.DeleteTimeline("parent")
			var herr *TimelineHasChildrenError
			if !errors.As(err, &herr) {
				t.Fatalf("delete err = %v, want *TimelineHasChildrenError", err)
			}
			if len(herr.Children) != 1 || herr.Children[0] != "child" {
				t.Fatalf("children = %v, want [child]", herr.Children)
			}
			if err := mgr.DeleteTimeline("child"); err != nil {
				t.Fatalf("delete child: %v", err)
			}
			if err := mgr.DeleteTimeline("parent"); err != nil {
				t.Fatalf("delete parent after child: %v", err)
			}
			if _, err := mgr.ForkFromDB("child"); !errors.Is(err, ErrTimelineIDReuse) {
				t.Fatalf("reused id err = %v, want ErrTimelineIDReuse", err)
			}
			if _, err := mgr.ForkFromDB("parent"); !errors.Is(err, ErrTimelineIDReuse) {
				t.Fatalf("reused id err = %v, want ErrTimelineIDReuse", err)
			}
			// Numbers are never reused either: the next
			// timeline gets a fresh number above both.
			ni, err := mgr.ForkFromDB("fresh")
			if err != nil {
				t.Fatalf("fork fresh: %v", err)
			}
			if ni.Number <= pi.Number {
				t.Fatalf("fresh number %d not above parent %d", ni.Number, pi.Number)
			}
			// Invalid IDs rejected.
			for _, bad := range []string{"", "has space", "a/b", "main", "root", "-lead", "x" + string(make([]byte, 65))} {
				if _, err := mgr.ForkFromDB(bad); !errors.Is(err, ErrTimelineIDInvalid) {
					t.Fatalf("id %q err = %v, want ErrTimelineIDInvalid", bad, err)
				}
			}
		})
	}
}

// (j) Concurrent timelines: parallel goroutines writing
// different timelines, then a stale-head merge race where
// exactly one merge wins and the loser gets StaleHead.
func TestTimelineConcurrent(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			_, mgr := timelineOpenForTest(t, bin, dir, 1)
			p := mgr.db
			if err := p.Put("base", []byte("b")); err != nil {
				t.Fatalf("main put: %v", err)
			}
			if _, err := mgr.ForkFromDB("parent"); err != nil {
				t.Fatalf("fork parent: %v", err)
			}
			const n = 4
			const per = 25
			ids := make([]string, n)
			for i := 0; i < n; i++ {
				ids[i] = fmt.Sprintf("w%d", i)
				if _, err := mgr.Fork("parent", ids[i]); err != nil {
					t.Fatalf("fork %s: %v", ids[i], err)
				}
			}
			var wg sync.WaitGroup
			for i := 0; i < n; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					for j := 0; j < per; j++ {
						k := fmt.Sprintf("w%d-k%02d", i, j)
						if err := mgr.Put(ids[i], k, []byte(fmt.Sprintf("v%d", j))); err != nil {
							t.Errorf("put %s: %v", k, err)
							return
						}
					}
				}(i)
			}
			wg.Wait()
			for i := 0; i < n; i++ {
				for j := 0; j < per; j++ {
					timelineMustGet(t, mgr, ids[i], fmt.Sprintf("w%d-k%02d", i, j), fmt.Sprintf("v%d", j))
				}
				// No cross-talk: w0's keys are absent in w1.
				if i > 0 {
					timelineMustMissing(t, mgr, ids[i], "w0-k00")
				}
			}
			// Merge race: two goroutines merge different
			// children into parent with the SAME expected
			// head. Exactly one wins; the loser sees
			// StaleHead; the parent ends up with exactly
			// one child's keys (never a mixture of both
			// merges' partial effects — merges are atomic).
			head, _ := mgr.HeadSeq("parent")
			results := make([]error, 2)
			var wg2 sync.WaitGroup
			for i := 0; i < 2; i++ {
				wg2.Add(1)
				go func(i int) {
					defer wg2.Done()
					_, results[i] = mgr.Merge(ids[i], "parent", TimelineConflictFail, head)
				}(i)
			}
			wg2.Wait()
			wins := 0
			for _, err := range results {
				if err == nil {
					wins++
				} else if !errors.Is(err, ErrTimelineStaleHead) {
					t.Fatalf("merge race err = %v, want nil or StaleHead", err)
				}
			}
			if wins != 1 {
				t.Fatalf("merge race wins = %d, want exactly 1 (errs %v)", wins, results)
			}
			// Parent has exactly one child's full key set.
			have := 0
			for i := 0; i < 2; i++ {
				rows, err := mgr.Scan("parent", fmt.Sprintf("w%d-", i))
				if err != nil {
					t.Fatalf("scan: %v", err)
				}
				if len(rows) == per {
					have++
				} else if len(rows) != 0 {
					t.Fatalf("parent has %d of child %d keys: partial merge", len(rows), i)
				}
			}
			if have != 1 {
				t.Fatalf("parent child key sets present = %d, want 1", have)
			}
		})
	}
}

// Stale-head merge refusal on the ordinary (non-race) path.
func TestTimelineStaleHeadRefused(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			_, mgr := timelineOpenForTest(t, bin, dir, 1)
			p := mgr.db
			if err := p.Put("k", []byte("v")); err != nil {
				t.Fatalf("main put: %v", err)
			}
			if _, err := mgr.ForkFromDB("parent"); err != nil {
				t.Fatalf("fork: %v", err)
			}
			if _, err := mgr.Fork("parent", "child"); err != nil {
				t.Fatalf("fork: %v", err)
			}
			timelineMustPut(t, mgr, "child", "x", "1")
			head, _ := mgr.HeadSeq("parent")
			timelineMustPut(t, mgr, "parent", "y", "2")
			_, err := mgr.Merge("child", "parent", TimelineConflictFail, head)
			var serr *TimelineStaleHeadError
			if !errors.As(err, &serr) {
				t.Fatalf("merge err = %v, want *TimelineStaleHeadError", err)
			}
			timelineMustMissing(t, mgr, "parent", "x")
		})
	}
}

// Manager close after database close must be safe, not a
// crash: with C-backed leases, ReleaseLease executes wasm
// code, and executing it in a closed runtime segfaults
// (wasm64). The manager skips lease release when the
// database is already closed.
func TestTimelineManagerCloseAfterDBClose(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			mgr, err := NewTimelineManager(p)
			if err != nil {
				t.Fatalf("manager: %v", err)
			}
			if err := p.Put("k", []byte("v")); err != nil {
				t.Fatalf("put: %v", err)
			}
			if _, err := mgr.ForkFromDB("tl"); err != nil {
				t.Fatalf("fork: %v", err)
			}
			if err := p.Close(); err != nil {
				t.Fatalf("db close: %v", err)
			}
			if err := mgr.Close(); err != nil {
				t.Fatalf("manager close after db close: %v", err)
			}
		})
	}
}

// No-common-base merges are refused, not silently rebased.
func TestTimelineNoCommonBaseRefused(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			_, mgr := timelineOpenForTest(t, bin, dir, 1)
			p := mgr.db
			if err := p.Put("k", []byte("v")); err != nil {
				t.Fatalf("main put: %v", err)
			}
			if _, err := mgr.ForkFromDB("a"); err != nil {
				t.Fatalf("fork a: %v", err)
			}
			if _, err := mgr.ForkFromDB("b"); err != nil {
				t.Fatalf("fork b: %v", err)
			}
			timelineMustPut(t, mgr, "a", "x", "1")
			head, _ := mgr.HeadSeq("b")
			_, err := mgr.Merge("a", "b", TimelineConflictFail, head)
			if !errors.Is(err, ErrTimelineNoCommonBase) {
				t.Fatalf("merge err = %v, want ErrTimelineNoCommonBase", err)
			}
		})
	}
}

func sortedKeysForTest(m map[string][]byte) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

var _ = sortedKeysForTest

// Benchmarks: fork cost (dominated by the DB flush + full
// branch-view scan + fingerprint) and merge cost vs data size.
func BenchmarkTimelineForkMerge(b *testing.B) {
	for _, n := range []int{100, 1000} {
		b.Run(fmt.Sprintf("keys%d", n), func(b *testing.B) {
			dir := b.TempDir()
			wasmBytes, err := os.ReadFile(persistTestBins[0])
			if err != nil {
				b.Fatalf("read wasm: %v", err)
			}
			p, err := OpenPersistedSegDB(dir, wasmBytes, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			if err != nil {
				b.Fatalf("open: %v", err)
			}
			defer p.Close()
			for i := 0; i < n; i++ {
				if err := p.Put(fmt.Sprintf("k%05d", i), []byte("v")); err != nil {
					b.Fatalf("put: %v", err)
				}
			}
			mgr, err := NewTimelineManager(p)
			if err != nil {
				b.Fatalf("manager: %v", err)
			}
			defer mgr.Close()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				id := fmt.Sprintf("bench%d", i)
				if _, err := mgr.ForkFromDB(id); err != nil {
					b.Fatalf("fork: %v", err)
				}
				if err := mgr.Put(id, "extra", []byte("x")); err != nil {
					b.Fatalf("put: %v", err)
				}
				head, _ := mgr.HeadSeq(id)
				_ = head
				if err := mgr.DeleteTimeline(id); err != nil {
					b.Fatalf("delete: %v", err)
				}
			}
		})
	}
}
