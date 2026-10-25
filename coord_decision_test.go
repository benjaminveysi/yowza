package yowza

// Item 1 tests — coordinated SegDBTransaction commit and
// coordinated MergeToMain through the CoordDecider layer
// (coord_decision.go). Every test here is per-path, not
// just happy-path: tokenless refusal, stale/released
// tokens, idempotent retry, crash windows, fixed-head
// preservation, and authorized recovery are each proven
// by execution. The plain-path refusals this layer
// replaces (patch3) are covered by patch3_closure_test.go
// and are re-asserted here only where the new API adds
// a refusal of its own (nil decider, wrong coordinator).
//
// NOTE on hooks: patch3MergeDecisionHook,
// coordTxAfterDecideHook and coordMergeAfterDecideHook
// are package-global test seams. Tests here never run
// in parallel, and every hook is cleared via defer in
// the subtest that sets it, so no hook can leak into
// another test.

import (
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// coordDecisionLocalSetup opens a fresh DB, seeds it
// (plain facade writes, BEFORE fencing — a fenced DB
// refuses facade writes by design), acquires a
// coordinator in a fresh directory, fences the DB under
// that session, and returns a local decider for it.
// Cleanup (single func, explicit order) unregisters,
// releases, and closes.
func coordDecisionLocalSetup(t *testing.T, bin string, seed map[string]string) (*PersistedSegDB, *CoordinatorSession, string, CoordDecider) {
	t.Helper()
	dir := t.TempDir()
	p := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
	for k, v := range seed {
		if err := p.Put(k, []byte(v)); err != nil {
			p.Close()
			t.Fatalf("seed Put %q: %v", k, err)
		}
	}
	coordDir := t.TempDir()
	sess, err := AcquireCoordinator(coordDir, "item1-owner")
	if err != nil {
		p.Close()
		t.Fatalf("acquire coordinator: %v", err)
	}
	if err := RegisterCoordFence(p, sess); err != nil {
		sess.Release()
		p.Close()
		t.Fatalf("register fence: %v", err)
	}
	dec, err := NewLocalCoordDecider(sess, coordDir)
	if err != nil {
		UnregisterCoordFence(p)
		sess.Release()
		p.Close()
		t.Fatalf("new local decider: %v", err)
	}
	t.Cleanup(func() {
		UnregisterCoordFence(p)
		sess.Release()
		p.Close()
	})
	return p, sess, coordDir, dec
}

// coordDecisionMergeSetup is coordDecisionLocalSetup plus
// a TimelineManager over the same DB (manager dir == DB
// dir, as NewTimelineManager guarantees). Seeds are
// written and flushed BEFORE fencing, so ForkFromDB
// in the test sees a flushed base head.
func coordDecisionMergeSetup(t *testing.T, bin string, seed map[string]string) (*PersistedSegDB, *TimelineManager, *CoordinatorSession, string, CoordDecider) {
	t.Helper()
	dir := t.TempDir()
	p := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
	for k, v := range seed {
		if err := p.Put(k, []byte(v)); err != nil {
			p.Close()
			t.Fatalf("seed Put %q: %v", k, err)
		}
	}
	if len(seed) > 0 {
		if err := p.Flush(); err != nil {
			p.Close()
			t.Fatalf("seed Flush: %v", err)
		}
	}
	mgr, err := NewTimelineManager(p)
	if err != nil {
		p.Close()
		t.Fatalf("NewTimelineManager: %v", err)
	}
	coordDir := t.TempDir()
	sess, err := AcquireCoordinator(coordDir, "item1-merge-owner")
	if err != nil {
		mgr.Close()
		p.Close()
		t.Fatalf("acquire coordinator: %v", err)
	}
	if err := RegisterCoordFence(p, sess); err != nil {
		sess.Release()
		mgr.Close()
		p.Close()
		t.Fatalf("register fence: %v", err)
	}
	dec, err := NewLocalCoordDecider(sess, coordDir)
	if err != nil {
		UnregisterCoordFence(p)
		sess.Release()
		mgr.Close()
		p.Close()
		t.Fatalf("new local decider: %v", err)
	}
	t.Cleanup(func() {
		UnregisterCoordFence(p)
		sess.Release()
		mgr.Close()
		p.Close()
	})
	return p, mgr, sess, coordDir, dec
}

func coordDecisionHead(t *testing.T, p *PersistedSegDB) []uint64 {
	t.Helper()
	vec, _, err := p.SnapshotVector()
	if err != nil {
		t.Fatalf("SnapshotVector: %v", err)
	}
	return vec
}

func coordDecisionTxMustCommit(t *testing.T, p *PersistedSegDB, dec CoordDecider, opID string, kvs map[string]string) {
	t.Helper()
	tx, err := BeginSegDBTransaction(p)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	for k, v := range kvs {
		if err := tx.Put(k, []byte(v), false); err != nil {
			t.Fatalf("Put %q: %v", k, err)
		}
	}
	if err := tx.CommitCoordinated(dec, opID); err != nil {
		t.Fatalf("CommitCoordinated %s: %v", opID, err)
	}
}

func coordDecisionAssertNoCoordRoot(t *testing.T, coordDir, opID string) {
	t.Helper()
	if _, err := os.Stat(coordRootPath(coordDir, opID)); err == nil {
		t.Fatalf("coordinator root for %s still present", opID)
	}
}

func coordDecisionAssertNoStagedMergeFiles(t *testing.T, dir string) {
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

// Coordinated tx commit succeeds end-to-end and is
// visible after close + reopen; the completion record
// (not the root) is what survives success.
func TestCoordDecisionTxEndToEndAndReopen(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			p, _, coordDir, dec := coordDecisionLocalSetup(t, bin, nil)
			dir := p.Dir()
			coordDecisionTxMustCommit(t, p, dec, "tx-1", map[string]string{"a": "1", "b": "2"})
			persistMustGet(t, p, "a", "1")
			persistMustGet(t, p, "b", "2")
			coordDecisionAssertNoCoordRoot(t, coordDir, "tx-1")
			if _, err := os.Stat(coordDecisionCompletedPath(coordDir, "tx-1")); err != nil {
				t.Fatalf("completion record missing after success: %v", err)
			}
			// Close + reopen: the fence is not
			// re-registered here, so plain reads
			// serve — the point is the VALUES
			// survived, via the normal DB files.
			// (The manager/close order in the setup
			// cleanup runs at test end, after this
			// reopen, so close explicitly first and
			// let cleanup close the reopened handle
			// via its own idempotent path: instead
			// reopen on a SECOND handle is impossible
			// while the first is open (exclusive
			// dir lock), so close, reopen, and
			// re-register the fence for the retry
			// below.)
			p.Close()
			p2 := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			t.Cleanup(func() { p2.Close() })
			persistMustGet(t, p2, "a", "1")
			persistMustGet(t, p2, "b", "2")
		})
	}
}

// Tokenless commits stay refused on a fenced DB — plain
// Commit, nil-decider CommitCoordinated, and a decider
// for a different coordinator directory — and apply
// nothing.
func TestCoordDecisionTxTokenlessStillRefused(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			p, sess, coordDir, dec := coordDecisionLocalSetup(t, bin, nil)
			_ = sess
			_ = coordDir
			tx, err := BeginSegDBTransaction(p)
			if err != nil {
				t.Fatal(err)
			}
			if err := tx.Put("must-not-commit", []byte("x"), false); err != nil {
				t.Fatal(err)
			}
			if err := tx.Commit(); !errors.Is(err, ErrCoordFenceBypass) {
				t.Fatalf("plain Commit err %v, want ErrCoordFenceBypass", err)
			}
			persistMustMissing(t, p, "must-not-commit")
			tx2, err := BeginSegDBTransaction(p)
			if err != nil {
				t.Fatal(err)
			}
			if err := tx2.Put("must-not-commit", []byte("x"), false); err != nil {
				t.Fatal(err)
			}
			if err := tx2.CommitCoordinated(nil, "tx-nil"); !errors.Is(err, ErrCoordReleased) {
				t.Fatalf("nil-decider CommitCoordinated err %v, want ErrCoordReleased", err)
			}
			persistMustMissing(t, p, "must-not-commit")
			// Wrong coordinator directory: a live
			// decider, but for another coordinator
			// than the one this DB is fenced under.
			otherDir := t.TempDir()
			sessB, err := AcquireCoordinator(otherDir, "item1-other-owner")
			if err != nil {
				t.Fatal(err)
			}
			defer sessB.Release()
			decB, err := NewLocalCoordDecider(sessB, otherDir)
			if err != nil {
				t.Fatal(err)
			}
			tx3, err := BeginSegDBTransaction(p)
			if err != nil {
				t.Fatal(err)
			}
			if err := tx3.Put("must-not-commit", []byte("x"), false); err != nil {
				t.Fatal(err)
			}
			if err := tx3.CommitCoordinated(decB, "tx-wrong"); !errors.Is(err, ErrCoordFenceBypass) {
				t.Fatalf("wrong-dir CommitCoordinated err %v, want ErrCoordFenceBypass", err)
			}
			persistMustMissing(t, p, "must-not-commit")
			coordDecisionAssertNoCoordRoot(t, otherDir, "tx-wrong")
			// The authorized decider still works
			// after the refusals (the fence was not
			// wedged by them).
			coordDecisionTxMustCommit(t, p, dec, "tx-ok", map[string]string{"ok": "yes"})
			persistMustGet(t, p, "ok", "yes")
		})
	}
}

// Stale-epoch and released-session commits are refused
// at COMMIT time — both transactions Begin while the
// session is live, so a Begin-time-only check would
// have passed them. Nothing is applied either way.
func TestCoordDecisionTxStaleEpochAndReleased(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			t.Run("stale-epoch", func(t *testing.T) {
				p, sess1, coordDir, dec1 := coordDecisionLocalSetup(t, bin, nil)
				tx, err := BeginSegDBTransaction(p)
				if err != nil {
					t.Fatal(err)
				}
				if err := tx.Put("stale-key", []byte("v"), false); err != nil {
					t.Fatal(err)
				}
				// Takeover: release sess1, acquire
				// sess2 in the same directory (epoch
				// moves). dec1 is now stale.
				if err := sess1.Release(); err != nil {
					t.Fatal(err)
				}
				sess2, err := AcquireCoordinator(coordDir, "item1-owner-2")
				if err != nil {
					t.Fatal(err)
				}
				defer sess2.Release()
				if err := tx.CommitCoordinated(dec1, "tx-stale"); !errors.Is(err, ErrCoordStaleEpoch) {
					t.Fatalf("stale CommitCoordinated err %v, want ErrCoordStaleEpoch", err)
				}
				persistMustMissing(t, p, "stale-key")
				coordDecisionAssertNoCoordRoot(t, coordDir, "tx-stale")
			})
			t.Run("released", func(t *testing.T) {
				p, sess, _, dec := coordDecisionLocalSetup(t, bin, nil)
				tx, err := BeginSegDBTransaction(p)
				if err != nil {
					t.Fatal(err)
				}
				if err := tx.Put("released-key", []byte("v"), false); err != nil {
					t.Fatal(err)
				}
				if err := sess.Release(); err != nil {
					t.Fatal(err)
				}
				if err := tx.CommitCoordinated(dec, "tx-released"); !errors.Is(err, ErrCoordReleased) {
					t.Fatalf("released CommitCoordinated err %v, want ErrCoordReleased", err)
				}
				persistMustMissing(t, p, "released-key")
			})
		})
	}
}

// Idempotent retry: same opID + same writes returns
// success with exactly-once visible effect (head
// unchanged across the retry); same opID + different
// writes is refused and the original values stand.
func TestCoordDecisionTxIdempotentRetry(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			p, _, _, dec := coordDecisionLocalSetup(t, bin, nil)
			coordDecisionTxMustCommit(t, p, dec, "tx-idem", map[string]string{"a": "1", "b": "2"})
			before := coordDecisionHead(t, p)
			// Same opID, same writes: success, no
			// re-apply (head identical).
			coordDecisionTxMustCommit(t, p, dec, "tx-idem", map[string]string{"a": "1", "b": "2"})
			persistMustGet(t, p, "a", "1")
			persistMustGet(t, p, "b", "2")
			after := coordDecisionHead(t, p)
			if !equalUint64Slice(before, after) {
				t.Fatalf("head moved on idempotent retry: %v -> %v", before, after)
			}
			// Same opID, different writes: refused,
			// original values stand, head unchanged.
			tx, err := BeginSegDBTransaction(p)
			if err != nil {
				t.Fatal(err)
			}
			if err := tx.Put("a", []byte("CHANGED"), false); err != nil {
				t.Fatal(err)
			}
			if err := tx.CommitCoordinated(dec, "tx-idem"); !errors.Is(err, ErrCoordOpExists) {
				t.Fatalf("different-payload retry err %v, want ErrCoordOpExists", err)
			}
			persistMustGet(t, p, "a", "1")
			after2 := coordDecisionHead(t, p)
			if !equalUint64Slice(before, after2) {
				t.Fatalf("head moved on refused retry: %v -> %v", before, after2)
			}
		})
	}
}

// Crash between decision and apply, through the full
// Tx API (hook aborts after Decide, before journal /
// apply): the commit reports indeterminate, nothing is
// applied, the decision stands (root present), and
// coordinated recovery rolls the FULL write set
// forward. A later retry with the same opID then
// returns success without re-applying.
func TestCoordDecisionTxCrashBetweenDecisionAndApply(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			p, _, coordDir, dec := coordDecisionLocalSetup(t, bin, map[string]string{"k": "old"})
			tx, err := BeginSegDBTransaction(p)
			if err != nil {
				t.Fatal(err)
			}
			if err := tx.Put("k", []byte("new"), false); err != nil {
				t.Fatal(err)
			}
			if err := tx.Put("k2", []byte("new2"), false); err != nil {
				t.Fatal(err)
			}
			coordTxAfterDecideHook = func() error { return errors.New("injected crash after decision") }
			defer func() { coordTxAfterDecideHook = nil }()
			err = tx.CommitCoordinated(dec, "tx-crash")
			if !errors.Is(err, ErrSegDBTxIndeterminate) {
				t.Fatalf("hook-aborted commit err %v, want ErrSegDBTxIndeterminate", err)
			}
			if !tx.Indeterminate() {
				t.Fatal("tx.Indeterminate() false after decision-stands abort")
			}
			// Nothing applied yet (all-old).
			persistMustGet(t, p, "k", "old")
			persistMustMissing(t, p, "k2")
			if _, err := os.Stat(coordRootPath(coordDir, "tx-crash")); err != nil {
				t.Fatalf("coordinator root missing after decision-stands abort: %v", err)
			}
			coordTxAfterDecideHook = nil
			rolled, err := RecoverCoordinatedTx(p, dec)
			if err != nil {
				t.Fatalf("RecoverCoordinatedTx: %v", err)
			}
			if len(rolled) != 1 || rolled[0] != "tx-crash" {
				t.Fatalf("rolled %v, want [tx-crash]", rolled)
			}
			persistMustGet(t, p, "k", "new")
			persistMustGet(t, p, "k2", "new2")
			coordDecisionAssertNoCoordRoot(t, coordDir, "tx-crash")
			// Retry after recovery: success, no
			// re-apply (head unchanged).
			before := coordDecisionHead(t, p)
			coordDecisionTxMustCommit(t, p, dec, "tx-crash", map[string]string{"k": "new", "k2": "new2"})
			after := coordDecisionHead(t, p)
			if !equalUint64Slice(before, after) {
				t.Fatalf("head moved on post-recovery retry: %v -> %v", before, after)
			}
			// Second recovery is a no-op.
			rolled, err = RecoverCoordinatedTx(p, dec)
			if err != nil {
				t.Fatalf("second RecoverCoordinatedTx: %v", err)
			}
			if len(rolled) != 0 {
				t.Fatalf("second recovery rolled %v, want none", rolled)
			}
		})
	}
}

// Crash BEFORE any decision: staging without a root is
// reclaimed by recovery (all-old), the annotation is
// swept, and the opID is free for a later real commit
// (a no-decision never reserves an opID).
func TestCoordDecisionTxCrashBeforeDecisionReclaimed(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			p, sess, coordDir, dec := coordDecisionLocalSetup(t, bin, nil)
			writes := map[string][]globalMergeDelta{coordDeciderShard: {{Key: "pre", Value: []byte("v")}}}

			if err := PrepareMultiShard(sess, coordDir, "tx-pre", writes); err != nil {
				t.Fatal(err)
			}
			if _, err := coordDecisionWriteMeta(coordDir, "tx-pre", &coordDecisionMeta{OpID: "tx-pre", Kind: CoordDecisionTx, DBDir: p.Dir(), Fingerprint: coordDecisionFingerprint(writes)}); err != nil {
				t.Fatal(err)
			}
			rolled, err := RecoverCoordinatedTx(p, dec)
			if err != nil {
				t.Fatalf("RecoverCoordinatedTx: %v", err)
			}
			if len(rolled) != 0 {
				t.Fatalf("recovery rolled %v for an undecided op, want none", rolled)
			}
			persistMustMissing(t, p, "pre")
			if _, err := os.Stat(coordStageManifestPath(coordDir, "tx-pre")); err == nil {
				t.Fatal("staged manifest not reclaimed")
			}
			if _, err := os.Stat(coordDecisionMetaPath(coordDir, "tx-pre")); err == nil {
				t.Fatal("staged annotation not swept")
			}
			// The opID is free: a real commit with
			// it now succeeds.
			coordDecisionTxMustCommit(t, p, dec, "tx-pre", map[string]string{"pre": "v"})
			persistMustGet(t, p, "pre", "v")
		})
	}
}

// Existing-decision roll-forward through the full Tx
// API: Decide publishes the decision without applying,
// then a Tx commit with the same opID + writes rolls
// it forward to success.
func TestCoordDecisionTxExistingRollForward(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			p, _, coordDir, dec := coordDecisionLocalSetup(t, bin, nil)
			writes := map[string][]globalMergeDelta{coordDeciderShard: {{Key: "ex", Value: []byte("1")}}}
			status, err := dec.Decide("tx-exist", CoordDecisionTx, p.Dir(), "", writes)
			if err != nil {
				t.Fatal(err)
			}
			if status != CoordDecisionNew {
				t.Fatalf("Decide status %v, want New", status)
			}
			persistMustMissing(t, p, "ex")
			coordDecisionTxMustCommit(t, p, dec, "tx-exist", map[string]string{"ex": "1"})
			persistMustGet(t, p, "ex", "1")
			coordDecisionAssertNoCoordRoot(t, coordDir, "tx-exist")
		})
	}
}

// Concurrent coordinated commits (distinct opIDs) all
// succeed with all values visible, while concurrent
// tokenless commits are all refused. -race target.
func TestCoordDecisionTxConcurrent(t *testing.T) {
	bin := persistTestBins[0]
	p, _, _, dec := coordDecisionLocalSetup(t, bin, nil)
	const n = 8
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tx, err := BeginSegDBTransaction(p)
			if err != nil {
				errs[i] = err
				return
			}
			key := "conc-" + string(rune('a'+i))
			if err := tx.Put(key, []byte("v"), false); err != nil {
				errs[i] = err
				return
			}
			errs[i] = tx.CommitCoordinated(dec, "tx-conc-"+string(rune('a'+i)))
		}(i)
	}
	plainErrs := make([]error, 4)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tx, err := BeginSegDBTransaction(p)
			if err != nil {
				plainErrs[i] = err
				return
			}
			if err := tx.Put("plain-conc", []byte("x"), false); err != nil {
				plainErrs[i] = err
				return
			}
			plainErrs[i] = tx.Commit()
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("coordinated commit %d: %v", i, err)
		}
	}
	for i, err := range plainErrs {
		if !errors.Is(err, ErrCoordFenceBypass) {
			t.Fatalf("plain commit %d err %v, want ErrCoordFenceBypass", i, err)
		}
	}
	for i := 0; i < n; i++ {
		persistMustGet(t, p, "conc-"+string(rune('a'+i)), "v")
	}
	persistMustMissing(t, p, "plain-conc")
}

// Coordinated merge succeeds end-to-end and is visible
// after reopen; the completion record (not the root)
// survives success.
func TestCoordDecisionMergeEndToEnd(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			p, mgr, _, coordDir, dec := coordDecisionMergeSetup(t, bin, map[string]string{"k": "base"})
			dir := p.Dir()
			if _, err := mgr.ForkFromDB("src"); err != nil {
				t.Fatal(err)
			}
			timelineMustPut(t, mgr, "src", "k", "v1")
			rep, err := mgr.MergeToMainCoordinated(dec, "mg-1", "src", TimelineConflictFail, nil)
			if err != nil {
				t.Fatalf("MergeToMainCoordinated: %v", err)
			}
			if len(rep.AppliedKeys) != 1 || rep.AppliedKeys[0] != "k" {
				t.Fatalf("report AppliedKeys %v, want [k]", rep.AppliedKeys)
			}
			if rep.MergeID == "" {
				t.Fatal("report MergeID empty on fresh coordinated merge")
			}
			persistMustGet(t, p, "k", "v1")
			coordDecisionAssertNoCoordRoot(t, coordDir, "mg-1")
			if _, err := os.Stat(coordDecisionCompletedPath(coordDir, "mg-1")); err != nil {
				t.Fatalf("completion record missing after merge success: %v", err)
			}
			coordDecisionAssertNoStagedMergeFiles(t, dir)
			mgr.Close()
			p.Close()
			p2 := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			t.Cleanup(func() { p2.Close() })
			persistMustGet(t, p2, "k", "v1")
		})
	}
}

// Plain MergeToMain on a fenced DB stays refused and
// applies nothing; a decider for another coordinator
// is likewise refused.
func TestCoordDecisionMergePlainStillRefused(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			p, mgr, _, _, dec := coordDecisionMergeSetup(t, bin, map[string]string{"k": "base"})
			_ = dec
			if _, err := mgr.ForkFromDB("src"); err != nil {
				t.Fatal(err)
			}
			timelineMustPut(t, mgr, "src", "k", "v1")
			if _, err := mgr.MergeToMain("src", TimelineConflictFail, nil); !errors.Is(err, ErrCoordFenceBypass) {
				t.Fatalf("plain MergeToMain err %v, want ErrCoordFenceBypass", err)
			}
			persistMustGet(t, p, "k", "base")
			if _, err := mgr.MergeToMainCoordinated(nil, "mg-nil", "src", TimelineConflictFail, nil); !errors.Is(err, ErrCoordReleased) {
				t.Fatalf("nil-decider MergeToMainCoordinated err %v, want ErrCoordReleased", err)
			}
			persistMustGet(t, p, "k", "base")
			otherDir := t.TempDir()
			sessB, err := AcquireCoordinator(otherDir, "item1-merge-other")
			if err != nil {
				t.Fatal(err)
			}
			defer sessB.Release()
			decB, err := NewLocalCoordDecider(sessB, otherDir)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := mgr.MergeToMainCoordinated(decB, "mg-wrong", "src", TimelineConflictFail, nil); !errors.Is(err, ErrCoordFenceBypass) {
				t.Fatalf("wrong-dir MergeToMainCoordinated err %v, want ErrCoordFenceBypass", err)
			}
			persistMustGet(t, p, "k", "base")
		})
	}
}

// Fixed-head discipline, coordinated edition: a writer
// landing between the planning decision and the commit
// (via the patch3 hook) yields StaleHead on BOTH the
// applied path and the no-op path — never a silent
// apply — and publishes NO coordinator decision.
// The injected writer uses the locked apply path
// because the facade is fenced in this test; what
// matters for the guarantee is that the head moved.
func TestCoordDecisionMergeStaleHead(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			t.Run("applied", func(t *testing.T) {
				p, mgr, _, coordDir, dec := coordDecisionMergeSetup(t, bin, map[string]string{"k": "base"})
				if _, err := mgr.ForkFromDB("src"); err != nil {
					t.Fatal(err)
				}
				timelineMustPut(t, mgr, "src", "k", "v1")
				patch3MergeDecisionHook = func() {
					p.mu.Lock()
					_ = applyDeltasLocked(p, []globalMergeDelta{{Key: "k", Value: []byte("v2")}})
					p.mu.Unlock()
				}
				defer func() { patch3MergeDecisionHook = nil }()
				_, err := mgr.MergeToMainCoordinated(dec, "mg-stale", "src", TimelineConflictFail, nil)
				var staleErr *GlobalMergeStaleHeadError
				if !errors.As(err, &staleErr) || !errors.Is(err, ErrGlobalMergeStaleHead) {
					t.Fatalf("merge err %v, want GlobalMergeStaleHeadError", err)
				}
				// Writer's value stands; planned
				// value never applied over it; no
				// decision was published; no staging
				// left behind.
				persistMustGet(t, p, "k", "v2")
				coordDecisionAssertNoCoordRoot(t, coordDir, "mg-stale")
				if _, err := os.Stat(coordDecisionCompletedPath(coordDir, "mg-stale")); err == nil {
					t.Fatal("completion record present for a stale merge")
				}
				coordDecisionAssertNoStagedMergeFiles(t, p.Dir())
			})
			t.Run("noop", func(t *testing.T) {
				p, mgr, _, _, dec := coordDecisionMergeSetup(t, bin, map[string]string{"k": "base"})
				if _, err := mgr.ForkFromDB("src"); err != nil {
					t.Fatal(err)
				}
				patch3MergeDecisionHook = func() {
					p.mu.Lock()
					_ = applyDeltasLocked(p, []globalMergeDelta{{Key: "k", Value: []byte("v2")}})
					p.mu.Unlock()
				}
				defer func() { patch3MergeDecisionHook = nil }()
				_, err := mgr.MergeToMainCoordinated(dec, "mg-stale-noop", "src", TimelineConflictFail, nil)
				if !errors.Is(err, ErrGlobalMergeStaleHead) {
					t.Fatalf("no-op merge err %v, want ErrGlobalMergeStaleHead", err)
				}
				persistMustGet(t, p, "k", "v2")
			})
		})
	}
}

// Crash between coordinator decision and apply, through
// the full merge API (hook aborts after Decide, before
// the global-merge commit record): the merge reports
// committed-but-incomplete, nothing is applied, the
// decision stands, and coordinated recovery rolls the
// merge forward and cleans its staging.
func TestCoordDecisionMergeCrashAfterDecision(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			p, mgr, _, coordDir, dec := coordDecisionMergeSetup(t, bin, map[string]string{"k": "base"})
			if _, err := mgr.ForkFromDB("src"); err != nil {
				t.Fatal(err)
			}
			timelineMustPut(t, mgr, "src", "k", "v1")
			coordMergeAfterDecideHook = func() error { return errors.New("injected crash after decision") }
			defer func() { coordMergeAfterDecideHook = nil }()
			_, err := mgr.MergeToMainCoordinated(dec, "mg-crash", "src", TimelineConflictFail, nil)
			if !errors.Is(err, ErrGlobalMergeCommitted) {
				t.Fatalf("hook-aborted merge err %v, want ErrGlobalMergeCommitted", err)
			}
			persistMustGet(t, p, "k", "base")
			if _, err := os.Stat(coordRootPath(coordDir, "mg-crash")); err != nil {
				t.Fatalf("coordinator root missing after decision-stands abort: %v", err)
			}
			coordMergeAfterDecideHook = nil
			rolled, err := RecoverCoordinatedMerges(p, dec)
			if err != nil {
				t.Fatalf("RecoverCoordinatedMerges: %v", err)
			}
			if len(rolled) != 1 || rolled[0] != "mg-crash" {
				t.Fatalf("rolled %v, want [mg-crash]", rolled)
			}
			persistMustGet(t, p, "k", "v1")
			coordDecisionAssertNoCoordRoot(t, coordDir, "mg-crash")
			coordDecisionAssertNoStagedMergeFiles(t, p.Dir())
			rolled, err = RecoverCoordinatedMerges(p, dec)
			if err != nil {
				t.Fatalf("second RecoverCoordinatedMerges: %v", err)
			}
			if len(rolled) != 0 {
				t.Fatalf("second recovery rolled %v, want none", rolled)
			}
		})
	}
}

// Merge idempotent retry through the full API: after a
// source-wins merge and a later writer, retrying the
// SAME opID returns success WITHOUT re-applying over
// the later writer (head unchanged, writer stands).
// This is the live-API shape of the decider's
// Completed status for merges (a plain recompute
// after a clean success collapses to no-op; the
// source-wins + later-writer shape is what actually
// reaches Decide with a completed op).
func TestCoordDecisionMergeIdempotentRetry(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			p, mgr, _, coordDir, dec := coordDecisionMergeSetup(t, bin, map[string]string{"k": "base"})
			if _, err := mgr.ForkFromDB("src"); err != nil {
				t.Fatal(err)
			}
			timelineMustPut(t, mgr, "src", "k", "v1")
			if _, err := mgr.MergeToMainCoordinated(dec, "mg-1", "src", TimelineConflictSourceWins, nil); err != nil {
				t.Fatalf("first merge: %v", err)
			}
			persistMustGet(t, p, "k", "v1")
			// Later writer (coordinated tx, since the
			// facade is fenced).
			coordDecisionTxMustCommit(t, p, dec, "tx-writer", map[string]string{"k": "v2"})
			persistMustGet(t, p, "k", "v2")
			before := coordDecisionHead(t, p)
			rep, err := mgr.MergeToMainCoordinated(dec, "mg-1", "src", TimelineConflictSourceWins, nil)
			if err != nil {
				t.Fatalf("idempotent merge retry: %v", err)
			}
			if rep.MergeID != "" {
				t.Fatalf("completed-retry report MergeID %q, want empty (no new merge)", rep.MergeID)
			}
			persistMustGet(t, p, "k", "v2")
			after := coordDecisionHead(t, p)
			if !equalUint64Slice(before, after) {
				t.Fatalf("head moved on idempotent merge retry: %v -> %v", before, after)
			}
			coordDecisionAssertNoCoordRoot(t, coordDir, "mg-1")
		})
	}
}

// Authorized recovery, merge edition: a decided merge
// rolls forward, an undecided staging is reclaimed,
// domain recovery skips decider ops (it must not fail
// on them and must not consume them), and a second
// coordinated recovery is a no-op.
func TestCoordDecisionMergeRecoveryAndDomainSkip(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			p, _, sess, coordDir, dec := coordDecisionMergeSetup(t, bin, map[string]string{"k": "base"})
			// Decided merge op (no apply yet).
			mgWrites := map[string][]globalMergeDelta{coordDeciderShard: {{Key: "mk", Value: []byte("mv")}}}
			status, err := dec.Decide("mg-rec", CoordDecisionMerge, p.Dir(), "merge-staging-1", mgWrites)
			if err != nil {
				t.Fatal(err)
			}
			if status != CoordDecisionNew {
				t.Fatalf("Decide status %v, want New", status)
			}
			// Undecided staging for another op.
			stagedWrites := map[string][]globalMergeDelta{coordDeciderShard: {{Key: "sk", Value: []byte("sv")}}}
			if err := PrepareMultiShard(sess, coordDir, "mg-staged", stagedWrites); err != nil {
				t.Fatal(err)
			}
			if _, err := coordDecisionWriteMeta(coordDir, "mg-staged", &coordDecisionMeta{OpID: "mg-staged", Kind: CoordDecisionMerge, DBDir: p.Dir(), Fingerprint: coordDecisionFingerprint(stagedWrites)}); err != nil {
				t.Fatal(err)
			}
			// Domain recovery over the same directory
			// must skip both decider ops: no error,
			// nothing rolled, decided op unconsumed.
			// (The staged op's files are reclaimed by
			// RecoverMultiShard's staged-only sweep
			// inside domain recovery — that is the
			// correct no-decision outcome, not a
			// domain apply.)
			pOther := persistOpenForTest(t, bin, t.TempDir(), PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			defer pOther.Close()
			dom, err := NewTimelineDomain("dom1", coordDir, DomainParticipant{DBID: "other", DB: pOther})
			if err != nil {
				t.Fatal(err)
			}
			rolledDom, err := dom.RecoverCoordinatedDomainOps(coordDir)
			if err != nil {
				t.Fatalf("domain recovery over decider ops: %v", err)
			}
			if len(rolledDom) != 0 {
				t.Fatalf("domain recovery rolled %v, want none", rolledDom)
			}
			if _, err := os.Stat(coordRootPath(coordDir, "mg-rec")); err != nil {
				t.Fatalf("domain recovery consumed the decided merge op: %v", err)
			}
			if _, err := os.Stat(coordStageManifestPath(coordDir, "mg-staged")); err == nil {
				t.Fatal("domain recovery left an undecided staging behind")
			}
			// Coordinated recovery: decided merge
			// rolls forward; undecided annotation is
			// swept; nothing else happens.
			rolled, err := RecoverCoordinatedMerges(p, dec)
			if err != nil {
				t.Fatalf("RecoverCoordinatedMerges: %v", err)
			}
			if len(rolled) != 1 || rolled[0] != "mg-rec" {
				t.Fatalf("rolled %v, want [mg-rec]", rolled)
			}
			persistMustGet(t, p, "mk", "mv")
			persistMustMissing(t, p, "sk")
			if _, err := os.Stat(coordDecisionMetaPath(coordDir, "mg-staged")); err == nil {
				t.Fatal("undecided annotation not swept by coordinated recovery")
			}
			coordDecisionAssertNoCoordRoot(t, coordDir, "mg-rec")
			rolled, err = RecoverCoordinatedMerges(p, dec)
			if err != nil {
				t.Fatalf("second RecoverCoordinatedMerges: %v", err)
			}
			if len(rolled) != 0 {
				t.Fatalf("second recovery rolled %v, want none", rolled)
			}
		})
	}
}

// Raft mode, same decision contract: a single-node
// cluster decides a coordinated tx, retry is
// idempotent, and a decided-but-unapplied op rolls
// forward through recovery. (DB binary is wasm32
// only here: the Raft decision path is Go-side and
// both binaries are covered by the local tests.)
func TestCoordDecisionRaftTx(t *testing.T) {
	bin := persistTestBins[0]
	dir := t.TempDir()
	p := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
	defer p.Close()
	cluster := startRaftTestCluster(t, []string{"n1"})
	leaderID := cluster.waitLeader(30 * time.Second)
	leader := cluster.session(leaderID)
	if err := RegisterCoordFenceValidator(p, leader); err != nil {
		t.Fatalf("register raft fence: %v", err)
	}
	defer UnregisterCoordFence(p)
	dec, err := NewRaftCoordDecider(leader)
	if err != nil {
		t.Fatal(err)
	}
	coordDecisionTxMustCommit(t, p, dec, "raft-tx-1", map[string]string{"r": "1"})
	persistMustGet(t, p, "r", "1")
	before := coordDecisionHead(t, p)
	coordDecisionTxMustCommit(t, p, dec, "raft-tx-1", map[string]string{"r": "1"})
	after := coordDecisionHead(t, p)
	if !equalUint64Slice(before, after) {
		t.Fatalf("head moved on raft idempotent retry: %v -> %v", before, after)
	}
	// Decided but unapplied, then recovered.
	raftWrites := map[string][]globalMergeDelta{coordDeciderShard: {{Key: "r2", Value: []byte("2")}}}
	status, err := dec.Decide("raft-tx-2", CoordDecisionTx, p.Dir(), "", raftWrites)
	if err != nil {
		t.Fatal(err)
	}
	if status != CoordDecisionNew {
		t.Fatalf("raft Decide status %v, want New", status)
	}
	persistMustMissing(t, p, "r2")
	rolled, err := RecoverCoordinatedTx(p, dec)
	if err != nil {
		t.Fatalf("RecoverCoordinatedTx (raft): %v", err)
	}
	if len(rolled) != 1 || rolled[0] != "raft-tx-2" {
		t.Fatalf("rolled %v, want [raft-tx-2]", rolled)
	}
	persistMustGet(t, p, "r2", "2")
	// Second recovery is a no-op even though the
	// Raft log still holds the entry (the completion
	// record, not the log, is what marks it done).
	rolled, err = RecoverCoordinatedTx(p, dec)
	if err != nil {
		t.Fatalf("second RecoverCoordinatedTx (raft): %v", err)
	}
	if len(rolled) != 0 {
		t.Fatalf("second raft recovery rolled %v, want none", rolled)
	}
}

// Raft fencing: a proposal through a follower session
// is refused with ErrRaftNotLeader (the Raft analogue
// of the local stale/released refusal — leadership,
// not a local epoch, is the fencing authority).
func TestCoordDecisionRaftFollowerRefused(t *testing.T) {
	bin := persistTestBins[0]
	p := persistOpenForTest(t, bin, t.TempDir(), PersistedOptions{NumShards: 1, Durability: DurabilitySync})
	defer p.Close()
	cluster := startRaftTestCluster(t, []string{"n1", "n2", "n3"})
	leaderID := cluster.waitLeader(30 * time.Second)
	var follower *RaftCoordinatorSession
	for _, sess := range cluster.sessions {
		if sess.Node().ID() != leaderID {
			follower = sess
			break
		}
	}
	if follower == nil {
		t.Fatal("no follower session in 3-node cluster")
	}
	if err := RegisterCoordFenceValidator(p, follower); err != nil {
		t.Fatalf("register raft fence: %v", err)
	}
	defer UnregisterCoordFence(p)
	dec, err := NewRaftCoordDecider(follower)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := BeginSegDBTransaction(p)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Put("follower-key", []byte("v"), false); err != nil {
		t.Fatal(err)
	}
	if err := tx.CommitCoordinated(dec, "raft-follow"); !errors.Is(err, ErrRaftNotLeader) {
		t.Fatalf("follower CommitCoordinated err %v, want ErrRaftNotLeader", err)
	}
	persistMustMissing(t, p, "follower-key")
}
