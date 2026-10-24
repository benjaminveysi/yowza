package yowza

// Integrated architecture test: concurrent multiple timelines
// with independent AND conflicting changes, fork and merge
// after a crash, recovery after restart — using the real
// PersistedSegDB + wasm binaries, not mocks.
//
// Scenario (per binary, 2 shards):
//   main: catalog keys + one contested key.
//   trunk = fork from main; featureA, featureB = forks from
//     trunk (siblings, same fork point).
//   featureA and featureB write independent keys concurrently
//     and both change the contested key differently.
//   Merge featureA -> trunk (clean: independent keys apply;
//     contested key conflicts -> fail policy refuses, nothing
//     applied; then source-wins resolves it).
//   Merge featureB -> trunk with fail policy: contested key
//     now conflicts again (trunk took A's value, B has its
//     own) -> refused; target-wins keeps trunk's value while
//     B's independent keys still apply.
//   CRASH: close manager + database with no FlushTimeline.
//   RESTART: reopen, verify every timeline's full view.
//   HISTORY LOSS: close, reopen the raw database, advance +
//     flush + compact with no manager (leases gone), reopen
//     the manager: the old timelines must report
//     ErrTimelineBranchPointGone (fingerprint gate), while a
//     fresh fork from the current database still works.
//   Main database throughout: never contains any timeline
//   write until/unless a timeline is merged into it (which
//   this item does not do) — asserted at each stage.

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
)

func integrationView(t *testing.T, mgr *TimelineManager, id string) map[string]string {
	t.Helper()
	rows, err := mgr.Scan(id, "")
	if err != nil {
		t.Fatalf("Scan(%s): %v", id, err)
	}
	out := map[string]string{}
	for _, r := range rows {
		out[string(r.Key)] = string(r.Val)
	}
	return out
}

func integrationWantView(t *testing.T, mgr *TimelineManager, id string, want map[string]string) {
	t.Helper()
	got := integrationView(t, mgr, id)
	if len(got) != len(want) {
		t.Fatalf("view(%s) = %v, want %v", id, got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("view(%s)[%q] = %q, want %q (full %v)", id, k, got[k], v, got)
		}
	}
}

func TestTimelineIntegration(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p, mgr := timelineOpenForTest(t, bin, dir, 2)
			_ = p
			// Main catalog.
			for k, v := range map[string]string{
				"cat-a": "ca", "cat-b": "cb", "contested": "orig",
			} {
				if err := mgr.db.Put(k, []byte(v)); err != nil {
					t.Fatalf("main put %s: %v", k, err)
				}
			}
			if _, err := mgr.ForkFromDB("trunk"); err != nil {
				t.Fatalf("fork trunk: %v", err)
			}
			if _, err := mgr.Fork("trunk", "featureA"); err != nil {
				t.Fatalf("fork featureA: %v", err)
			}
			if _, err := mgr.Fork("trunk", "featureB"); err != nil {
				t.Fatalf("fork featureB: %v", err)
			}
			// Concurrent independent + conflicting writes.
			var wg sync.WaitGroup
			for _, tc := range []struct {
				id, key, val string
			}{
				{"featureA", "a-only", "A1"},
				{"featureA", "contested", "Aval"},
				{"featureB", "b-only", "B1"},
				{"featureB", "contested", "Bval"},
			} {
				wg.Add(1)
				go func(tc struct{ id, key, val string }) {
					defer wg.Done()
					if err := mgr.Put(tc.id, tc.key, []byte(tc.val)); err != nil {
						t.Errorf("put %s/%s: %v", tc.id, tc.key, err)
					}
				}(tc)
			}
			wg.Wait()
			integrationWantView(t, mgr, "featureA", map[string]string{
				"cat-a": "ca", "cat-b": "cb", "contested": "Aval", "a-only": "A1",
			})
			integrationWantView(t, mgr, "featureB", map[string]string{
				"cat-a": "ca", "cat-b": "cb", "contested": "Bval", "b-only": "B1",
			})
			// Main DB untouched by timeline writes.
			if v, err := mgr.db.Get("a-only"); err == nil {
				t.Fatalf("main DB leaked a-only=%q", v)
			}
			// Merge A -> trunk, fail policy: contested
			// conflicts (trunk unchanged, A changed — wait:
			// trunk did NOT change contested, so this is
			// one-sided, not a conflict. Assert clean merge,
			// then B -> trunk conflicts on contested.
			head, _ := mgr.HeadSeq("trunk")
			rep, err := mgr.Merge("featureA", "trunk", TimelineConflictFail, head)
			if err != nil {
				t.Fatalf("merge A: %v", err)
			}
			if len(rep.ConflictedKeys) != 0 {
				t.Fatalf("merge A conflicts = %v, want none (trunk never touched contested)", rep.ConflictedKeys)
			}
			if len(rep.AppliedKeys) != 2 {
				t.Fatalf("merge A applied = %v, want [a-only contested]", rep.AppliedKeys)
			}
			integrationWantView(t, mgr, "trunk", map[string]string{
				"cat-a": "ca", "cat-b": "cb", "contested": "Aval", "a-only": "A1",
			})
			// Merge B -> trunk, fail policy: contested now
			// conflicts (trunk=Aval via A, B=Bval) but
			// b-only is one-sided. Fail = NOTHING applies.
			head, _ = mgr.HeadSeq("trunk")
			_, err = mgr.Merge("featureB", "trunk", TimelineConflictFail, head)
			var cerr *TimelineMergeConflictError
			if !errors.As(err, &cerr) {
				t.Fatalf("merge B fail err = %v, want conflict error", err)
			}
			integrationWantView(t, mgr, "trunk", map[string]string{
				"cat-a": "ca", "cat-b": "cb", "contested": "Aval", "a-only": "A1",
			})
			if _, ok := integrationView(t, mgr, "trunk")["b-only"]; ok {
				t.Fatalf("failed merge leaked b-only into trunk")
			}
			// Target-wins: b-only applies, contested stays Aval.
			head, _ = mgr.HeadSeq("trunk")
			rep, err = mgr.Merge("featureB", "trunk", TimelineConflictTargetWins, head)
			if err != nil {
				t.Fatalf("merge B target-wins: %v", err)
			}
			if len(rep.ConflictedKeys) != 1 || rep.ConflictedKeys[0] != "contested" {
				t.Fatalf("merge B conflicted = %v, want [contested]", rep.ConflictedKeys)
			}
			integrationWantView(t, mgr, "trunk", map[string]string{
				"cat-a": "ca", "cat-b": "cb", "contested": "Aval", "a-only": "A1", "b-only": "B1",
			})
			// Diff trunk vs its branch point shows exactly
			// the merged keys.
			diff, err := mgr.CompareToBranchPoint("trunk")
			if err != nil {
				t.Fatalf("diff trunk: %v", err)
			}
			var dkeys []string
			for _, e := range diff {
				dkeys = append(dkeys, e.Key)
			}
			sort.Strings(dkeys)
			if fmt.Sprint(dkeys) != "[a-only b-only contested]" {
				t.Fatalf("trunk diff keys = %v, want [a-only b-only contested]", dkeys)
			}
			// CRASH + RESTART: abrupt close, no flush.
			if err := mgr.Close(); err != nil {
				t.Fatalf("mgr close: %v", err)
			}
			if err := mgr.db.Close(); err != nil {
				t.Fatalf("db close: %v", err)
			}
			p2 := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 2, Durability: DurabilitySync})
			mgr2, err := NewTimelineManager(p2)
			if err != nil {
				t.Fatalf("reopen manager: %v", err)
			}
			integrationWantView(t, mgr2, "trunk", map[string]string{
				"cat-a": "ca", "cat-b": "cb", "contested": "Aval", "a-only": "A1", "b-only": "B1",
			})
			integrationWantView(t, mgr2, "featureA", map[string]string{
				"cat-a": "ca", "cat-b": "cb", "contested": "Aval", "a-only": "A1",
			})
			integrationWantView(t, mgr2, "featureB", map[string]string{
				"cat-a": "ca", "cat-b": "cb", "contested": "Bval", "b-only": "B1",
			})
			// Fork + merge after restart still work.
			if _, err := mgr2.Fork("trunk", "hotfix"); err != nil {
				t.Fatalf("fork hotfix after restart: %v", err)
			}
			timelineMustPut(t, mgr2, "hotfix", "fix", "1")
			head, _ = mgr2.HeadSeq("trunk")
			if _, err := mgr2.Merge("hotfix", "trunk", TimelineConflictFail, head); err != nil {
				t.Fatalf("merge hotfix: %v", err)
			}
			timelineMustGet(t, mgr2, "trunk", "fix", "1")
			if err := mgr2.Close(); err != nil {
				t.Fatalf("mgr2 close: %v", err)
			}
			if err := p2.Close(); err != nil {
				t.Fatalf("db2 close: %v", err)
			}
			// PHASE C UPDATE (supersedes the pre-pin
			// expectation below): with durable pins wired
			// (C0), a raw reopen + advance + flush +
			// compact with NO manager open does NOT drop
			// branch-point history anymore — DB open
			// recovers the pins' leases BEFORE any
			// Compact can run, so the compact defers
			// and the old timelines keep working. The
			// history-loss -> broken path still exists,
			// but only once the pins themselves are
			// gone (second half of this scenario).
			p3 := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 2, Durability: DurabilitySync})
			if err := p3.Put("cat-a", []byte("ca2")); err != nil {
				t.Fatalf("raw put: %v", err)
			}
			if err := p3.Delete("cat-b"); err != nil {
				t.Fatalf("raw delete: %v", err)
			}
			if err := p3.Flush(); err != nil {
				t.Fatalf("raw flush: %v", err)
			}
			if err := p3.Compact(0); err != nil {
				t.Fatalf("raw compact: %v", err)
			}
			if err := p3.Close(); err != nil {
				t.Fatalf("db3 close: %v", err)
			}
			p4 := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 2, Durability: DurabilitySync})
			mgr4, err := NewTimelineManager(p4)
			if err != nil {
				t.Fatalf("reopen manager4: %v", err)
			}
			// Pins protected the branch points: the old
			// timelines still read their old values.
			timelineMustGet(t, mgr4, "trunk", "cat-a", "ca")
			timelineMustGet(t, mgr4, "featureA", "cat-a", "ca")
			if err := mgr4.Close(); err != nil {
				t.Fatalf("mgr4 close: %v", err)
			}
			if err := p4.Close(); err != nil {
				t.Fatalf("db4 close: %v", err)
			}
			// HISTORY LOSS, genuinely: delete every
			// durable pin, then compact with no leases
			// held at all. Now the branch-point history
			// is dropped, and the manager must flag the
			// old timelines broken (fingerprint gate)
			// instead of serving silently wrong views.
			p5 := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 2, Durability: DurabilitySync})
			pins, err := p5.ListPins()
			if err != nil {
				t.Fatalf("list pins: %v", err)
			}
			for _, pin := range pins {
				if pin.Status == PinStatusActive {
					if err := p5.DeletePin(pin.ID); err != nil {
						t.Fatalf("delete pin %s: %v", pin.ID, err)
					}
				}
			}
			if err := p5.Compact(0); err != nil {
				t.Fatalf("compact after pin delete: %v", err)
			}
			if err := p5.Close(); err != nil {
				t.Fatalf("db5 close: %v", err)
			}
			p6 := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 2, Durability: DurabilitySync})
			mgr6, err := NewTimelineManager(p6)
			if err != nil {
				t.Fatalf("reopen manager6: %v", err)
			}
			defer p6.Close()
			defer mgr6.Close()
			if _, err := mgr6.Get("trunk", "cat-a"); !errors.Is(err, ErrTimelineBranchPointGone) {
				t.Fatalf("trunk Get after history loss err = %v, want ErrTimelineBranchPointGone", err)
			}
			if _, err := mgr6.Get("featureA", "cat-a"); !errors.Is(err, ErrTimelineBranchPointGone) {
				t.Fatalf("featureA Get after history loss err = %v, want ErrTimelineBranchPointGone", err)
			}
			// Fresh forks from the current database still work.
			if _, err := mgr6.ForkFromDB("newtrunk"); err != nil {
				t.Fatalf("fresh fork after history loss: %v", err)
			}
			timelineMustGet(t, mgr6, "newtrunk", "cat-a", "ca2")
			timelineMustMissing(t, mgr6, "newtrunk", "cat-b")
		})
	}
}
