package yowza

// Merge-time regression tests for patch3 item 1 (MergeToMain with a
// fixed head). The user's patch3 implements planningHead capture and
// a commit-time re-check even when expectedHead is nil, but ships no
// test that injects a writer exactly between decision and commit.
// These tests use the merge-time-only seam patch3MergeDecisionHook
// (nil in production, always cleared via defer) to place the writer
// deterministically in that window — not a sleep-and-hope race.

import (
	"errors"
	"os"
	"strings"
	"testing"
)

func mergedAssertNoStagedMergeFiles(t *testing.T, dir string) {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	for _, e := range ents {
		if strings.HasPrefix(e.Name(), "global-merge-") {
			t.Fatalf("staged merge file left behind: %s", e.Name())
		}
	}
}

// The user's fresh defect, reproduced as an execution: branch
// prepares v1 for k, MergeToMain decides from the base head, a
// writer lands exactly between decision and commit changing main
// to v2, and the merge must refuse with ErrGlobalMergeStaleHead —
// never silently apply the planned v1 over v2.
func TestMergedPatch3MergeHeadNilHeadWriterBetweenDecisionAndCommit(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			p, mgr, dir := phaseEGlobalSetup(t, bin, 1)
			if err := p.Put("k", []byte("base")); err != nil {
				t.Fatalf("put base: %v", err)
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}
			if _, err := mgr.ForkFromDB("src"); err != nil {
				t.Fatalf("fork: %v", err)
			}
			timelineMustPut(t, mgr, "src", "k", "v1")
			patch3MergeDecisionHook = func() {
				if err := p.Put("k", []byte("v2")); err != nil {
					t.Errorf("injected writer put: %v", err)
				}
			}
			defer func() { patch3MergeDecisionHook = nil }()
			_, err := mgr.MergeToMain("src", TimelineConflictFail, nil)
			var staleErr *GlobalMergeStaleHeadError
			if !errors.As(err, &staleErr) {
				t.Fatalf("merge err %v, want GlobalMergeStaleHeadError", err)
			}
			if !errors.Is(err, ErrGlobalMergeStaleHead) {
				t.Fatalf("merge err %v does not wrap ErrGlobalMergeStaleHead", err)
			}
			// The writer's value stands; the planned branch value
			// was never applied over it.
			phaseEMustDBGet(t, p, "k", "v2")
			mergedAssertNoStagedMergeFiles(t, dir)
		})
	}
}

// Same window on the no-op path: a branch with no deltas must not
// return success if the head moved while it was being evaluated.
func TestMergedPatch3MergeHeadNoOpWriterForcesStale(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			p, mgr, _ := phaseEGlobalSetup(t, bin, 1)
			if err := p.Put("k", []byte("base")); err != nil {
				t.Fatalf("put base: %v", err)
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}
			if _, err := mgr.ForkFromDB("src"); err != nil {
				t.Fatalf("fork: %v", err)
			}
			patch3MergeDecisionHook = func() {
				if err := p.Put("k", []byte("v2")); err != nil {
					t.Errorf("injected writer put: %v", err)
				}
			}
			defer func() { patch3MergeDecisionHook = nil }()
			_, err := mgr.MergeToMain("src", TimelineConflictFail, nil)
			var staleErr *GlobalMergeStaleHeadError
			if !errors.As(err, &staleErr) {
				t.Fatalf("no-op merge err %v, want GlobalMergeStaleHeadError", err)
			}
			phaseEMustDBGet(t, p, "k", "v2")
		})
	}
}

// Control: with no hook and no intervening writer, the same
// nil-head merge applies the branch value — proving the hook
// itself does not cause the refusal above.
func TestMergedPatch3MergeHeadNilHeadHappyPathApplies(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			p, mgr, _ := phaseEGlobalSetup(t, bin, 1)
			if err := p.Put("k", []byte("base")); err != nil {
				t.Fatalf("put base: %v", err)
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}
			if _, err := mgr.ForkFromDB("src"); err != nil {
				t.Fatalf("fork: %v", err)
			}
			timelineMustPut(t, mgr, "src", "k", "v1")
			if _, err := mgr.MergeToMain("src", TimelineConflictFail, nil); err != nil {
				t.Fatalf("merge: %v", err)
			}
			phaseEMustDBGet(t, p, "k", "v1")
		})
	}
}
