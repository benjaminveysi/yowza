package yowza

// PATCH2 Track 1 tests: MergeToMain Get-error separation
// and recovery-gate wiring to real reads.
//
// Design notes (see TIMELINE_V2_PATCH2_NOTES.md):
//   - mergeMainValue's three branches are tested with a
//     real DB: present, absent (ErrSegDBNotFound), and
//     a real non-NotFound error (closed DB). The
//     C-level -4 path (manifest/lookup error ->
//     ErrSegDBInternal) is proven by patch1's C test
//     and Go raw-export test; this helper treats any
//     such error identically via its default branch,
//     and a deterministic storage-error Get through
//     MergeToMain is not reachable via public Go APIs
//     (open verifies segment integrity first, and
//     reads stay available on indeterminate shards).
//   - The per-DB txPartialApply flag is set ONLY by
//     commitGate's rollback-failed path. Forcing a
//     real rollback failure via public APIs is not
//     reachable (rollback applies small pre-images
//     that succeed), so the flag mechanics are tested
//     white-box and the branch itself is covered by
//     inspection; this is stated, not hidden.

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestPatch2MergeMainValueErrorSeparation(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			if err := p.Put("mk", []byte("mv")); err != nil {
				t.Fatalf("put: %v", err)
			}
			// Present.
			v, ok, err := mergeMainValue(p, "mk")
			if err != nil || !ok || string(v) != "mv" {
				t.Fatalf("present: val=%q ok=%v err=%v", v, ok, err)
			}
			// Absent: only ErrSegDBNotFound converts.
			v, ok, err = mergeMainValue(p, "no-such-key")
			if err != nil || ok || v != nil {
				t.Fatalf("absent: val=%q ok=%v err=%v", v, ok, err)
			}
			// Real non-NotFound error: must propagate,
			// never convert to "absent".
			p.Close()
			_, ok, err = mergeMainValue(p, "mk")
			if err == nil || ok {
				t.Fatalf("closed DB: ok=%v err=%v, want a propagated error", ok, err)
			}
			if !errors.Is(err, ErrPersistedClosed) {
				t.Fatalf("closed DB err %v, want ErrPersistedClosed", err)
			}
		})
	}
}

func TestPatch2MergeToMainStagesNothingOnEarlyFailure(t *testing.T) {
	// Structural proof for the acceptance rule: the
	// conflict decision (including any Get error)
	// happens in the loop BEFORE stageGlobalMerge is
	// reached, so a failing merge can never stage or
	// publish. A Fail-policy conflict is the
	// reachable early failure; the Get-error branch
	// returns from the same loop position.
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p, mgr := timelineOpenForTest(t, bin, dir, 1)
			if err := p.Put("ck", []byte("base")); err != nil {
				t.Fatalf("main put: %v", err)
			}
			if _, err := mgr.ForkFromDB("src"); err != nil {
				t.Fatalf("fork: %v", err)
			}
			if _, err := mgr.Fork("src", "child"); err != nil {
				t.Fatalf("fork child: %v", err)
			}
			timelineMustPut(t, mgr, "child", "ck", "branch")
			// Intervening main change -> conflict.
			if err := p.Put("ck", []byte("main2")); err != nil {
				t.Fatalf("main change: %v", err)
			}
			_, err := mgr.MergeToMain("child", TimelineConflictFail, nil)
			if err == nil {
				t.Fatalf("conflicting merge accepted under Fail policy")
			}
			var conflictErr *GlobalMergeConflictError
			if !errors.As(err, &conflictErr) {
				t.Fatalf("merge err %v, want GlobalMergeConflictError", err)
			}
			ents, err := os.ReadDir(dir)
			if err != nil {
				t.Fatalf("readdir: %v", err)
			}
			for _, e := range ents {
				if len(e.Name()) > 13 && e.Name()[:13] == "global-merge-" {
					t.Fatalf("merge staged file %s after early failure", e.Name())
				}
			}
			mgr.Close()
			p.Close()
		})
	}
}

func TestPatch2RecoveryGateWiredToFacadeReads(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			if err := p.Put("seed", []byte("s")); err != nil {
				t.Fatalf("put seed: %v", err)
			}
			p.Close()
			// Crash state, written with no open handles:
			// a staged-only global merge + an orphan
			// delta. Both are gate-pending items.
			deltas := []globalMergeDelta{{Key: "gk", Value: []byte("gv")}}
			if _, err := stageGlobalMerge(dir, "stage-1", "src", "fail", nil, deltas, map[int]int{0: 1}, false); err != nil {
				t.Fatalf("stage: %v", err)
			}
			if _, err := timelineWriteDeltaSegment(filepath.Join(dir, "orphan-1.delta"), []timelineDeltaEntry{{Key: "ox", Seq: 1, Value: []byte("ov")}}, false); err != nil {
				t.Fatalf("orphan delta: %v", err)
			}
			// Reopen: the gate attached at open must
			// see the pending work and refuse RAW
			// facade reads — not just gate-wrapper
			// reads (the patch1 gap).
			p2 := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			defer p2.Close()
			gate := p2.RecoveryGate()
			if gate == nil || gate.IsOpen() {
				t.Fatalf("attached gate open over crash state")
			}
			if _, err := p2.Get("seed"); !errors.Is(err, ErrLifecycleRecoveryIncomplete) {
				t.Fatalf("facade Get err %v, want ErrLifecycleRecoveryIncomplete", err)
			}
			if _, err := p2.Scan(""); !errors.Is(err, ErrLifecycleRecoveryIncomplete) {
				t.Fatalf("facade Scan err %v, want ErrLifecycleRecoveryIncomplete", err)
			}
			if _, err := p2.ScanStreaming("", []uint64{0}); !errors.Is(err, ErrLifecycleRecoveryIncomplete) {
				t.Fatalf("facade ScanStreaming err %v, want ErrLifecycleRecoveryIncomplete", err)
			}
			if _, err := p2.AcquireSnapshot(); !errors.Is(err, ErrLifecycleRecoveryIncomplete) {
				t.Fatalf("facade AcquireSnapshot err %v, want ErrLifecycleRecoveryIncomplete", err)
			}
			// Recovery through the attached gate must
			// release facade reads (and reclaim the
			// staged merge + orphan delta).
			if err := gate.Recover(); err != nil {
				t.Fatalf("gate Recover: %v", err)
			}
			if !gate.IsOpen() {
				t.Fatalf("gate still closed after recovery: %v", gate.Pending())
			}
			persistMustGet(t, p2, "seed", "s")
			if _, err := os.Stat(filepath.Join(dir, "orphan-1.delta")); !os.IsNotExist(err) {
				t.Fatalf("orphan delta survived recovery")
			}
		})
	}
}

func TestPatch2RecoveryGateWiredToManagerReads(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p, mgr := timelineOpenForTest(t, bin, dir, 1)
			if _, err := mgr.ForkFromDB("parent"); err != nil {
				t.Fatalf("fork: %v", err)
			}
			timelineMustPut(t, mgr, "parent", "k", "v")
			// Fresh crash artifact while the manager
			// is alive, then a fresh attached gate:
			// manager reads must consult the CURRENT
			// attached gate, not the open-time one.
			if _, err := timelineWriteDeltaSegment(filepath.Join(dir, "orphan-2.delta"), []timelineDeltaEntry{{Key: "ox", Seq: 1, Value: []byte("ov")}}, false); err != nil {
				t.Fatalf("orphan delta: %v", err)
			}
			gate := p.AttachRecoveryGate()
			if gate.IsOpen() {
				t.Fatalf("fresh attached gate open over orphan delta")
			}
			if _, err := mgr.Get("parent", "k"); !errors.Is(err, ErrLifecycleRecoveryIncomplete) {
				t.Fatalf("manager Get err %v, want ErrLifecycleRecoveryIncomplete", err)
			}
			if _, err := mgr.Scan("parent", ""); !errors.Is(err, ErrLifecycleRecoveryIncomplete) {
				t.Fatalf("manager Scan err %v, want ErrLifecycleRecoveryIncomplete", err)
			}
			if err := gate.Recover(); err != nil {
				t.Fatalf("gate Recover: %v", err)
			}
			timelineMustGet(t, mgr, "parent", "k", "v")
			mgr.Close()
			p.Close()
		})
	}
}

func TestPatch2ManagerReopenHealsTornTailDespiteGate(t *testing.T) {
	// Guard against over-gating: a torn WAL tail is
	// healed by manager init itself, and init
	// refreshes the gate, so a healthy reopen must
	// NOT refuse reads.
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p, mgr := timelineOpenForTest(t, bin, dir, 1)
			if _, err := mgr.ForkFromDB("parent"); err != nil {
				t.Fatalf("fork: %v", err)
			}
			timelineMustPut(t, mgr, "parent", "k", "v")
			mgr.Close()
			p.Close()
			f, err := os.OpenFile(filepath.Join(dir, "timeline-parent.wal"), os.O_WRONLY|os.O_APPEND, 0o644)
			if err != nil {
				t.Fatalf("open wal: %v", err)
			}
			if _, err := f.Write([]byte{0x00, 0x01, 0x02, 0x03, 0xff}); err != nil {
				t.Fatalf("tear wal: %v", err)
			}
			f.Close()
			p2 := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			mgr2, err := NewTimelineManager(p2)
			if err != nil {
				t.Fatalf("reopen manager: %v", err)
			}
			defer mgr2.Close()
			defer p2.Close()
			timelineMustGet(t, mgr2, "parent", "k", "v")
		})
	}
}

func TestPatch2TxPartialApplyFlagRefusesReadsAndBegin(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			defer p.Close()
			if err := p.Put("seed", []byte("s")); err != nil {
				t.Fatalf("put seed: %v", err)
			}
			// White-box: the flag is set only by
			// commitGate's rollback-failed path (see
			// the file header note); here we prove
			// the flag mechanics: reads + Begin
			// refuse, recovery clears, reads + Begin
			// work again.
			p.mu.Lock()
			p.txPartialApply = true
			p.mu.Unlock()
			if _, err := p.Get("seed"); !errors.Is(err, ErrSegDBTxRecoveryRequired) {
				t.Fatalf("flagged Get err %v, want ErrSegDBTxRecoveryRequired", err)
			}
			if _, err := p.Scan(""); !errors.Is(err, ErrSegDBTxRecoveryRequired) {
				t.Fatalf("flagged Scan err %v, want ErrSegDBTxRecoveryRequired", err)
			}
			if _, err := BeginSegDBTransaction(p); !errors.Is(err, ErrSegDBTxRecoveryRequired) {
				t.Fatalf("flagged Begin err %v, want ErrSegDBTxRecoveryRequired", err)
			}
			if _, err := RecoverSegDBTransactions(p); err != nil {
				t.Fatalf("recovery: %v", err)
			}
			persistMustGet(t, p, "seed", "s")
			tx, err := BeginSegDBTransaction(p)
			if err != nil {
				t.Fatalf("begin after recovery: %v", err)
			}
			_ = tx.Rollback()
		})
	}
}
