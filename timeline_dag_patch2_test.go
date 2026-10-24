package yowza

// Patch 2 (Track 4) tests: the exact recursive merge-base
// rule in timeline_dag.go, plus the legacy-base branchView
// error propagation fix. Every test loops over both wasm
// binaries with a fresh directory per binary (TMPDIR must
// be on workspace disk, as for the other timeline tests).
//
// The merge-base rule under test, stated exactly:
//   - 2 maximal candidates: fold over their recursive
//     base; conflict-free folds resolve (virtual base),
//     conflicting folds return ErrAmbiguousMergeBase.
//   - 3-4 maximal candidates: the virtual base is used
//     ONLY when folding in EVERY candidate order yields
//     the same conflict-free view; any conflict, any
//     disagreement across orders, or any ambiguous
//     pairwise base returns ErrAmbiguousMergeBase.
//   - 5+ maximal candidates: ErrAmbiguousMergeBase
//     (temporary safe fallback, not a permanent
//     replacement for general merge-base support).
// There is NO first-candidate fallback anywhere in the
// general path: MergeGeneral and MergeGeneralStreamed
// both resolve their base through mergeGeneralBaseLocked,
// so both are covered by the tests below.

import (
	"errors"
	"sort"
	"strings"
	"testing"
)

func patch2Head(t *testing.T, mgr *TimelineManager, id string) uint64 {
	t.Helper()
	h, err := mgr.HeadSeq(id)
	if err != nil {
		t.Fatalf("HeadSeq %s: %v", id, err)
	}
	return h
}

func patch2Merge(t *testing.T, mgr *TimelineManager, src, dst string, pol TimelineConflictPolicy) *MergeReport {
	t.Helper()
	rep, err := mgr.MergeGeneral(src, dst, pol, patch2Head(t, mgr, src), patch2Head(t, mgr, dst))
	if err != nil {
		t.Fatalf("MergeGeneral %s->%s: %v", src, dst, err)
	}
	return rep
}

// patch2DisjointCrissCross builds a 2-candidate disjoint
// criss-cross between two fresh DAG timelines x and y:
// both fork from rt, both get one disjoint key, then
// child forks cross-merge in opposite orders. The caller
// supplies distinct timeline IDs per scenario.
func patch2DisjointCrissCross(t *testing.T, mgr *TimelineManager, rt, a, b, a2, b2, ka, kb string) {
	t.Helper()
	if _, err := mgr.ForkFromDBDAG(rt); err != nil {
		t.Fatalf("fork %s: %v", rt, err)
	}
	for _, f := range [][2]string{{rt, a}, {rt, b}} {
		if _, err := mgr.ForkDAG(f[0], f[1]); err != nil {
			t.Fatalf("fork %s: %v", f[1], err)
		}
	}
	timelineMustPut(t, mgr, a, ka, "v-"+ka)
	timelineMustPut(t, mgr, b, kb, "v-"+kb)
	if _, err := mgr.ForkDAG(a, a2); err != nil {
		t.Fatalf("fork %s: %v", a2, err)
	}
	if _, err := mgr.ForkDAG(b, b2); err != nil {
		t.Fatalf("fork %s: %v", b2, err)
	}
	patch2Merge(t, mgr, a, b2, TimelineConflictFail)
	patch2Merge(t, mgr, b, a2, TimelineConflictFail)
}

// TestPatch2TwoCandidateCrissCrossExact: the exactly
// determined 2-candidate case still resolves through a
// virtual base, with the right applied set — both for
// MergeGeneral and for MergeGeneralStreamed — and a
// conflicting 2-candidate criss-cross still refuses.
func TestPatch2TwoCandidateCrissCrossExact(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p, mgr := axisDOpen(t, bin, dir, 1)
			defer mgr.Close()
			defer p.Close()
			if err := p.Put("seed", []byte("s")); err != nil {
				t.Fatalf("put seed: %v", err)
			}
			// Disjoint 2-candidate criss-cross: final
			// merge applies nothing (both sides already
			// hold both keys) through a virtual base.
			patch2DisjointCrissCross(t, mgr, "rt", "a", "b", "a2", "b2", "ka", "kb")
			rep, err := mgr.MergeGeneral("a2", "b2", TimelineConflictFail, patch2Head(t, mgr, "a2"), patch2Head(t, mgr, "b2"))
			if err != nil {
				t.Fatalf("disjoint 2-candidate merge: %v (want success via virtual base)", err)
			}
			if rep.AppliedCount != 0 {
				t.Fatalf("disjoint 2-candidate applied %d, want 0", rep.AppliedCount)
			}
			if !strings.HasPrefix(rep.BaseCommitID, "virtual:") {
				t.Fatalf("base commit id %q, want virtual: prefix", rep.BaseCommitID)
			}
			timelineMustGet(t, mgr, "b2", "ka", "v-ka")
			timelineMustGet(t, mgr, "b2", "kb", "v-kb")
			// A later one-sided key still merges with
			// the correct applied set.
			timelineMustPut(t, mgr, "a2", "kn", "vn")
			rep, err = mgr.MergeGeneral("a2", "b2", TimelineConflictFail, patch2Head(t, mgr, "a2"), patch2Head(t, mgr, "b2"))
			if err != nil {
				t.Fatalf("post-resolution merge: %v", err)
			}
			if rep.AppliedCount != 1 {
				t.Fatalf("post-resolution applied %d, want 1 (kn)", rep.AppliedCount)
			}
			timelineMustGet(t, mgr, "b2", "kn", "vn")
			// Streamed form, same rule: disjoint
			// criss-cross plus one new source key.
			patch2DisjointCrissCross(t, mgr, "rt2", "c", "d", "c2", "d2", "kc", "kd")
			timelineMustPut(t, mgr, "c2", "ks", "vs")
			sink := &countingSink{}
			rep, err = mgr.MergeGeneralStreamed("c2", "d2", TimelineConflictFail, patch2Head(t, mgr, "c2"), patch2Head(t, mgr, "d2"), sink)
			if err != nil {
				t.Fatalf("streamed disjoint 2-candidate merge: %v", err)
			}
			if sink.applied != 1 || len(sink.appliedKeys) != 1 || sink.appliedKeys[0] != "ks" {
				t.Fatalf("streamed applied=%d keys=%v, want 1 [ks]", sink.applied, sink.appliedKeys)
			}
			if sink.conflicted != 0 || rep.AppliedCount != 1 {
				t.Fatalf("streamed conflicted=%d appliedCount=%d, want 0/1", sink.conflicted, rep.AppliedCount)
			}
			if !strings.HasPrefix(rep.BaseCommitID, "virtual:") {
				t.Fatalf("streamed base commit id %q, want virtual: prefix", rep.BaseCommitID)
			}
			timelineMustGet(t, mgr, "d2", "ks", "vs")
			// Conflicting 2-candidate criss-cross still
			// refuses, in both merge forms.
			patch2DisjointCrissCross(t, mgr, "rt3", "e", "f", "e2", "f2", "ke", "kf")
			timelineMustPut(t, mgr, "e", "kk", "ve")
			timelineMustPut(t, mgr, "f", "kk", "vf")
			// Rebuild the cross-merge with the conflict
			// in place: fresh children of e/f cross-merge
			// under SourceWins so the merge nodes exist.
			if _, err := mgr.ForkDAG("e", "e3"); err != nil {
				t.Fatalf("fork e3: %v", err)
			}
			if _, err := mgr.ForkDAG("f", "f3"); err != nil {
				t.Fatalf("fork f3: %v", err)
			}
			patch2Merge(t, mgr, "e", "f3", TimelineConflictSourceWins)
			patch2Merge(t, mgr, "f", "e3", TimelineConflictSourceWins)
			_, err = mgr.MergeGeneral("e3", "f3", TimelineConflictFail, patch2Head(t, mgr, "e3"), patch2Head(t, mgr, "f3"))
			if !errors.Is(err, ErrTimelineAmbiguousMergeBase) {
				t.Fatalf("conflicting 2-candidate err %v, want ErrAmbiguousMergeBase", err)
			}
			sink2 := &countingSink{}
			_, err = mgr.MergeGeneralStreamed("e3", "f3", TimelineConflictFail, patch2Head(t, mgr, "e3"), patch2Head(t, mgr, "f3"), sink2)
			if !errors.Is(err, ErrTimelineAmbiguousMergeBase) {
				t.Fatalf("streamed conflicting 2-candidate err %v, want ErrAmbiguousMergeBase", err)
			}
			if sink2.applied != 0 || sink2.conflicted != 0 {
				t.Fatalf("streamed ambiguous merge delivered applied=%d conflicted=%d, want 0/0 (base failure precedes any sink delivery)", sink2.applied, sink2.conflicted)
			}
		})
	}
}

// patch2TripleTargets builds two target timelines x and y
// that each accumulate the three source timelines a, b, c
// (in the given orders), so the final x/y heads share
// exactly the three source heads as maximal common
// ancestors. Sources must already exist with their keys
// put; x and y are fresh forks of rt.
func patch2TripleTargets(t *testing.T, mgr *TimelineManager, rt, x, y string, orderX, orderY []string) {
	t.Helper()
	if _, err := mgr.ForkDAG(rt, x); err != nil {
		t.Fatalf("fork %s: %v", x, err)
	}
	if _, err := mgr.ForkDAG(rt, y); err != nil {
		t.Fatalf("fork %s: %v", y, err)
	}
	for _, src := range orderX {
		patch2Merge(t, mgr, src, x, TimelineConflictSourceWins)
	}
	for _, src := range orderY {
		patch2Merge(t, mgr, src, y, TimelineConflictSourceWins)
	}
}

// TestPatch2ThreeCandidateCrissCross: >2-candidate cases.
// Disjoint triples resolve (every fold order gives the
// same union, so the virtual base is exactly
// determined) in both merge forms; conflicting triples
// return ErrAmbiguousMergeBase in both forms. The test
// first proves the candidate set really has 3 members
// by calling findMergeBase directly.
func TestPatch2ThreeCandidateCrissCross(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p, mgr := axisDOpen(t, bin, dir, 1)
			defer mgr.Close()
			defer p.Close()
			if err := p.Put("seed", []byte("s")); err != nil {
				t.Fatalf("put seed: %v", err)
			}
			if _, err := mgr.ForkFromDBDAG("rt"); err != nil {
				t.Fatalf("fork rt: %v", err)
			}
			for _, f := range []string{"a", "b", "c"} {
				if _, err := mgr.ForkDAG("rt", f); err != nil {
					t.Fatalf("fork %s: %v", f, err)
				}
			}
			timelineMustPut(t, mgr, "a", "ka", "va")
			timelineMustPut(t, mgr, "b", "kb", "vb")
			timelineMustPut(t, mgr, "c", "kc", "vc")
			patch2TripleTargets(t, mgr, "rt", "x", "y", []string{"a", "b", "c"}, []string{"c", "b", "a"})
			// Prove the >2 setup: exactly 3 maximal
			// common ancestors, the three source heads.
			xHead := mgr.timelines["x"].dagHead
			yHead := mgr.timelines["y"].dagHead
			_, err := mgr.findMergeBase(xHead, yHead)
			var amb *TimelineAmbiguousMergeBaseError
			if !errors.As(err, &amb) {
				t.Fatalf("findMergeBase(x,y) err %v, want ambiguous", err)
			}
			want := []string{mgr.timelines["a"].dagHead, mgr.timelines["b"].dagHead, mgr.timelines["c"].dagHead}
			sort.Strings(want)
			got := append([]string(nil), amb.Candidates...)
			sort.Strings(got)
			if len(got) != 3 || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
				t.Fatalf("candidates %v, want %v (3 source heads)", got, want)
			}
			// Disjoint triple resolves via the
			// order-independent virtual base.
			rep, err := mgr.MergeGeneral("x", "y", TimelineConflictFail, patch2Head(t, mgr, "x"), patch2Head(t, mgr, "y"))
			if err != nil {
				t.Fatalf("disjoint 3-candidate merge: %v (want success via virtual base)", err)
			}
			if rep.AppliedCount != 0 {
				t.Fatalf("disjoint 3-candidate applied %d, want 0", rep.AppliedCount)
			}
			if !strings.HasPrefix(rep.BaseCommitID, "virtual:") {
				t.Fatalf("3-candidate base commit id %q, want virtual: prefix", rep.BaseCommitID)
			}
			for _, kv := range [][2]string{{"ka", "va"}, {"kb", "vb"}, {"kc", "vc"}} {
				timelineMustGet(t, mgr, "y", kv[0], kv[1])
			}
			// Streamed form on a second disjoint triple.
			for _, f := range []string{"d", "e", "f"} {
				if _, err := mgr.ForkDAG("rt", f); err != nil {
					t.Fatalf("fork %s: %v", f, err)
				}
			}
			timelineMustPut(t, mgr, "d", "kd", "vd")
			timelineMustPut(t, mgr, "e", "ke", "ve")
			timelineMustPut(t, mgr, "f", "kf", "vf")
			patch2TripleTargets(t, mgr, "rt", "u", "v", []string{"d", "e", "f"}, []string{"f", "d", "e"})
			timelineMustPut(t, mgr, "u", "ku", "vu")
			sink := &countingSink{}
			rep, err = mgr.MergeGeneralStreamed("u", "v", TimelineConflictFail, patch2Head(t, mgr, "u"), patch2Head(t, mgr, "v"), sink)
			if err != nil {
				t.Fatalf("streamed disjoint 3-candidate merge: %v", err)
			}
			if sink.applied != 1 || len(sink.appliedKeys) != 1 || sink.appliedKeys[0] != "ku" {
				t.Fatalf("streamed 3-candidate applied=%d keys=%v, want 1 [ku]", sink.applied, sink.appliedKeys)
			}
			if !strings.HasPrefix(rep.BaseCommitID, "virtual:") {
				t.Fatalf("streamed 3-candidate base commit id %q, want virtual: prefix", rep.BaseCommitID)
			}
			timelineMustGet(t, mgr, "v", "ku", "vu")
			// Conflicting triple: all three sources set
			// kk differently, so every fold conflicts.
			for _, f := range []string{"g", "h", "i"} {
				if _, err := mgr.ForkDAG("rt", f); err != nil {
					t.Fatalf("fork %s: %v", f, err)
				}
			}
			timelineMustPut(t, mgr, "g", "kk", "vg")
			timelineMustPut(t, mgr, "h", "kk", "vh")
			timelineMustPut(t, mgr, "i", "kk", "vi")
			patch2TripleTargets(t, mgr, "rt", "p", "q", []string{"g", "h", "i"}, []string{"i", "h", "g"})
			_, err = mgr.MergeGeneral("p", "q", TimelineConflictFail, patch2Head(t, mgr, "p"), patch2Head(t, mgr, "q"))
			if !errors.Is(err, ErrTimelineAmbiguousMergeBase) {
				t.Fatalf("conflicting 3-candidate err %v, want ErrAmbiguousMergeBase", err)
			}
			sink2 := &countingSink{}
			_, err = mgr.MergeGeneralStreamed("p", "q", TimelineConflictFail, patch2Head(t, mgr, "p"), patch2Head(t, mgr, "q"), sink2)
			if !errors.Is(err, ErrTimelineAmbiguousMergeBase) {
				t.Fatalf("streamed conflicting 3-candidate err %v, want ErrAmbiguousMergeBase", err)
			}
		})
	}
}

// TestPatch2FindMergeBaseThreeCandidates is the
// wasm-free unit check that a 3-way criss-cross graph
// really enumerates 3 maximal common ancestors.
func TestPatch2FindMergeBaseThreeCandidates(t *testing.T) {
	m := &TimelineManager{dagCommits: map[string]*TimelineCommit{}}
	add := func(id string, parents ...string) {
		m.dagCommits[id] = &TimelineCommit{ID: id, ParentIDs: parents}
	}
	add("root")
	add("p", "root")
	add("q", "root")
	add("r", "root")
	add("x", "p", "q", "r")
	add("y", "r", "q", "p")
	_, err := m.findMergeBase("x", "y")
	var amb *TimelineAmbiguousMergeBaseError
	if !errors.As(err, &amb) {
		t.Fatalf("err %v, want ErrAmbiguousMergeBase", err)
	}
	if len(amb.Candidates) != 3 {
		t.Fatalf("candidates %v, want exactly [p q r]", amb.Candidates)
	}
}

// TestPatch2LegacyMergesAndBranchViewError covers the
// regression side and the audit item together:
// direct-parent and sibling legacy merges still work,
// and a branchView failure inside the legacy rule
// surfaces as that failure — NOT as a silent
// "rule does not apply" fall-through to NoCommonBase.
func TestPatch2LegacyMergesAndBranchViewError(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p, mgr := axisDOpen(t, bin, dir, 1)
			defer mgr.Close()
			defer p.Close()
			if err := p.Put("seed", []byte("s")); err != nil {
				t.Fatalf("put seed: %v", err)
			}
			// Direct-parent legacy merge: child into
			// its parent. Legacy timelines (ForkFromDB
			// / Fork) carry no shared DAG ancestor, so
			// this reaches legacyBaseView.
			if _, err := mgr.ForkFromDB("par"); err != nil {
				t.Fatalf("fork par: %v", err)
			}
			if _, err := mgr.Fork("par", "ch"); err != nil {
				t.Fatalf("fork ch: %v", err)
			}
			timelineMustPut(t, mgr, "ch", "ck", "vc")
			rep := patch2Merge(t, mgr, "ch", "par", TimelineConflictFail)
			if rep.AppliedCount != 1 {
				t.Fatalf("direct-parent legacy applied %d, want 1 (ck)", rep.AppliedCount)
			}
			timelineMustGet(t, mgr, "par", "ck", "vc")
			// Sibling legacy merge: two children forked
			// from par at the same head share the same
			// branch vector, so the sibling rule applies.
			if _, err := mgr.Fork("par", "s1"); err != nil {
				t.Fatalf("fork s1: %v", err)
			}
			if _, err := mgr.Fork("par", "s2"); err != nil {
				t.Fatalf("fork s2: %v", err)
			}
			timelineMustPut(t, mgr, "s1", "sk", "vs")
			rep = patch2Merge(t, mgr, "s1", "s2", TimelineConflictFail)
			if rep.AppliedCount != 1 {
				t.Fatalf("sibling legacy applied %d, want 1 (sk)", rep.AppliedCount)
			}
			timelineMustGet(t, mgr, "s2", "sk", "vs")
			// BranchView failure: corrupt the child's
			// branch vector (wrong length for the
			// 1-shard DB) so ScanStreaming fails inside
			// branchView. The merge must surface that
			// snapshot-mismatch error — under the old
			// code it was swallowed and reported as
			// NoCommonBase instead.
			if _, err := mgr.ForkFromDB("par2"); err != nil {
				t.Fatalf("fork par2: %v", err)
			}
			if _, err := mgr.Fork("par2", "ch2"); err != nil {
				t.Fatalf("fork ch2: %v", err)
			}
			timelineMustPut(t, mgr, "ch2", "ck2", "vc2")
			saved := append([]uint64(nil), mgr.timelines["ch2"].branchVector...)
			mgr.timelines["ch2"].branchVector = []uint64{}
			_, err := mgr.MergeGeneral("ch2", "par2", TimelineConflictFail, patch2Head(t, mgr, "ch2"), patch2Head(t, mgr, "par2"))
			if err == nil {
				t.Fatalf("corrupt branch vector merge accepted")
			}
			if errors.Is(err, ErrTimelineNoCommonBase) {
				t.Fatalf("branchView failure surfaced as NoCommonBase (%v): the failure was swallowed into 'rule does not apply'", err)
			}
			if !errors.Is(err, ErrPersistedSnapshotMismatch) {
				t.Fatalf("branchView failure err %v, want ErrPersistedSnapshotMismatch", err)
			}
			// Control: restoring the vector makes the
			// same legacy merge succeed, proving the
			// failure above was the injected scan
			// error and nothing else broke.
			mgr.timelines["ch2"].branchVector = saved
			rep = patch2Merge(t, mgr, "ch2", "par2", TimelineConflictFail)
			if rep.AppliedCount != 1 {
				t.Fatalf("restored legacy applied %d, want 1 (ck2)", rep.AppliedCount)
			}
			timelineMustGet(t, mgr, "par2", "ck2", "vc2")
		})
	}
}
