package yowza

// MERGED patch3 recovery tests (carried over from the stopped
// agent's work, against the user's patch3 tree). Contract under
// test (user-ordered, lower-risk fix with NO journal format
// change): no ordinary read runs before recovery. An "applying"
// or corrupt leftover transaction journal keeps the recovery gate
// closed until recovery has run, so a crash mid-apply can never
// leak a partial prefix to a facade reader as if it were a valid
// commit. Durable apply-progress records (which would let reads
// distinguish "nothing applied" from "a prefix applied") remain a
// FUTURE item, recorded as a limit, not built here: until then,
// reads without full recovery are refused, not served with a
// best-effort old state.
//
// DELIBERATELY EXCLUDED from the agent's file: its
// Aborted-journal and stray-.tmp tests. The agent's scanPending
// treated aborted journals and .tmp files as gate-pending; the
// user's patch3 does not (aborted is terminal and non-pending,
// .tmp is not a journal). Those two tests encode the agent's
// semantics, not the user's, so they are dropped — not edited
// to pass — and the difference is recorded in the merge notes.

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestPatch3MidApplyCrashPrefixNotObservable: the
// patch2 residual gap, closed. A transaction whose
// first key was already written to the DB by the
// crashed commit (simulated here by a direct Put
// before the journal is planted) and whose second
// key was not is exactly the state a crash mid-apply
// leaves: journal "applying", prefix on disk. After
// reopen, the prefix must NOT be observable as a
// valid commit — reads are refused, not served with
// the prefix — and recovery must roll the FULL set
// forward, not merely confirm the prefix.
func TestPatch3MidApplyCrashPrefixNotObservable(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			if err := p.Put("r:keep", []byte("keep")); err != nil {
				t.Fatalf("seed: %v", err)
			}
			// Direct means: the crashed commit had
			// already applied this key before dying.
			if err := p.Put("r:1", []byte("one")); err != nil {
				t.Fatalf("simulate applied prefix: %v", err)
			}
			data, err := encodeTxJournal(777101, "applying", []*SegDBTxWrite{
				{Key: "r:1", Value: []byte("one")},
				{Key: "r:2", Value: []byte("two")},
			})
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			if err := os.WriteFile(filepath.Join(dir, "segdb-tx-777101.journal"), data, 0o644); err != nil {
				t.Fatalf("write journal: %v", err)
			}
			p.Close()
			p2 := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			defer p2.Close()
			gate := p2.RecoveryGate()
			if gate == nil || gate.IsOpen() {
				t.Fatalf("attached gate open over mid-apply journal")
			}
			// The prefix is on disk, but it must not
			// be served: a refused read is the only
			// correct pre-recovery answer, because
			// "r:1 present, r:2 absent" is not a
			// state any committed transaction ever
			// produced.
			if _, err := p2.Get("r:1"); !errors.Is(err, ErrLifecycleRecoveryIncomplete) {
				t.Fatalf("pre-recovery Get(r:1) err %v, want ErrLifecycleRecoveryIncomplete (prefix must not be observable)", err)
			}
			if _, err := p2.Get("r:keep"); !errors.Is(err, ErrLifecycleRecoveryIncomplete) {
				t.Fatalf("pre-recovery Get(r:keep) err %v, want ErrLifecycleRecoveryIncomplete", err)
			}
			if _, err := p2.Scan("r:"); !errors.Is(err, ErrLifecycleRecoveryIncomplete) {
				t.Fatalf("pre-recovery Scan err %v, want ErrLifecycleRecoveryIncomplete", err)
			}
			if err := gate.Recover(); err != nil {
				t.Fatalf("gate Recover: %v", err)
			}
			if !gate.IsOpen() {
				t.Fatalf("gate still closed after recovery: %v", gate.Pending())
			}
			// Full set, idempotent re-apply of r:1
			// plus the previously missing r:2.
			persistMustGet(t, p2, "r:1", "one")
			persistMustGet(t, p2, "r:2", "two")
			persistMustGet(t, p2, "r:keep", "keep")
		})
	}
}

// TestPatch3CorruptJournalKeepsGateClosed: a journal
// with a flipped value byte (JSON still parses, so
// this exercises the checksum-mismatch path, not
// just the unparseable path) keeps the gate closed
// at open, makes recovery FAIL, and leaves reads
// refused afterwards. The gate never opens on
// corruption, and the journal file is left in place
// for inspection — recovery must not delete
// evidence it could not validate.
func TestPatch3CorruptJournalKeepsGateClosed(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			if err := p.Put("seed", []byte("s")); err != nil {
				t.Fatalf("seed: %v", err)
			}
			data, err := encodeTxJournal(777102, "applying", []*SegDBTxWrite{
				{Key: "c:1", Value: []byte("one")},
			})
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			// Flip one value byte: values are
			// base64 in the journal JSON ("one" =
			// "b25l"), and the checksum covers
			// {id, writes}, so flipping a base64
			// character to another valid base64
			// character must fail decode with a
			// checksum mismatch while the JSON
			// itself stays valid and parseable.
			idx := bytes.Index(data, []byte("b25l"))
			if idx < 0 {
				t.Fatalf("encoded journal lacks value bytes: %s", data)
			}
			data[idx] = 'x'
			journalPath := filepath.Join(dir, "segdb-tx-777102.journal")
			if err := os.WriteFile(journalPath, data, 0o644); err != nil {
				t.Fatalf("write journal: %v", err)
			}
			p.Close()
			p2 := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			defer p2.Close()
			gate := p2.RecoveryGate()
			if gate == nil || gate.IsOpen() {
				t.Fatalf("attached gate open over corrupt journal")
			}
			namedCorrupt := false
			for _, pend := range gate.Pending() {
				// Merge-time wording adaptation only: the user's
				// patch3 phrases this pending entry as
				// "transaction journal corrupt: ..."; the
				// assertion (a corrupt entry naming the journal
				// file) is unchanged.
				if strings.HasPrefix(pend, "transaction journal corrupt: segdb-tx-777102.journal") {
					namedCorrupt = true
				}
			}
			if !namedCorrupt {
				t.Fatalf("gate pending %v, want a corrupt entry naming segdb-tx-777102.journal", gate.Pending())
			}
			if _, err := p2.Get("seed"); !errors.Is(err, ErrLifecycleRecoveryIncomplete) {
				t.Fatalf("pre-recovery Get err %v, want ErrLifecycleRecoveryIncomplete", err)
			}
			// Recovery must FAIL on the corrupt
			// journal, not skip it and open.
			if err := gate.Recover(); err == nil {
				t.Fatalf("gate Recover accepted a corrupt journal")
			}
			if gate.IsOpen() {
				t.Fatalf("gate opened after failed recovery on corrupt journal")
			}
			if _, err := os.Stat(journalPath); err != nil {
				t.Fatalf("corrupt journal removed by failed recovery: %v", err)
			}
			// Reads are STILL refused after the
			// failed attempt: a failed recovery
			// never opens the gate.
			if _, err := p2.Get("seed"); !errors.Is(err, ErrLifecycleRecoveryIncomplete) {
				t.Fatalf("post-failure Get err %v, want ErrLifecycleRecoveryIncomplete", err)
			}
			if _, err := p2.Scan(""); !errors.Is(err, ErrLifecycleRecoveryIncomplete) {
				t.Fatalf("post-failure Scan err %v, want ErrLifecycleRecoveryIncomplete", err)
			}
		})
	}
}

// TestPatch3ManagerOpenRecoversTxBeforeValidation:
// a DB directory with an applying journal plus an
// existing timeline manager state. NewTimelineManager
// must open successfully: tx recovery runs before
// branch validation, and the gate is open after
// init with the recovered values visible.
//
// LIMIT, STATED HONESTLY: fingerprint validation
// cannot be made to depend on the recovered VALUES
// for a ForkFromDB timeline, because its branch view
// is scanAtVector(branchVector) — snapshot-isolated
// to the fork-time vector, which recovered writes
// (a newer generation) cannot enter. What IS proven
// here is the gate-mediated dependence, which is
// the ordering that actually matters: validation
// reads through the gated facade (branchView ->
// scanAtVector -> ScanStreaming ->
// checkReadsAllowed), so if recovery had NOT run
// before validation, validation's ScanStreaming
// would fail with ErrLifecycleRecoveryIncomplete
// and the open would fail. The test makes that
// counterfactual concrete by asserting the gated
// ScanStreaming refusal BEFORE the manager open,
// then asserting open success, gate open, journal
// removal, and recovered values visible through
// both DB and manager reads.
func TestPatch3ManagerOpenRecoversTxBeforeValidation(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p, mgr := timelineOpenForTest(t, bin, dir, 1)
			if err := p.Put("db:seed", []byte("base")); err != nil {
				t.Fatalf("seed db: %v", err)
			}
			info, err := mgr.ForkFromDB("parent")
			if err != nil {
				t.Fatalf("fork: %v", err)
			}
			if info.BranchFingerprint == "" {
				t.Fatalf("fork produced no branch fingerprint, so validation would not exercise branchView")
			}
			timelineMustPut(t, mgr, "parent", "k", "v")
			mgr.Close()
			p.Close()
			// Crash state, written with no open
			// handles: an applying journal.
			data, err := encodeTxJournal(777103, "applying", []*SegDBTxWrite{
				{Key: "m:1", Value: []byte("one")},
				{Key: "m:2", Value: []byte("two")},
			})
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			if err := os.WriteFile(filepath.Join(dir, "segdb-tx-777103.journal"), data, 0o644); err != nil {
				t.Fatalf("write journal: %v", err)
			}
			p2 := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			defer p2.Close()
			gate := p2.RecoveryGate()
			if gate == nil || gate.IsOpen() {
				t.Fatalf("attached gate open over pending applying journal")
			}
			// The exact read validation will make:
			// gated, so it must refuse while the
			// gate is closed. If manager init ran
			// validation before tx recovery, this
			// is the error that would fail open.
			if it, err := p2.ScanStreaming("", info.BranchVector); err == nil {
				_ = it.Close()
				t.Fatalf("gated ScanStreaming at branch vector succeeded with gate closed")
			} else if !errors.Is(err, ErrLifecycleRecoveryIncomplete) {
				t.Fatalf("gated ScanStreaming err %v, want ErrLifecycleRecoveryIncomplete", err)
			}
			// Manager open must SUCCEED: init runs
			// tx recovery before validation, then
			// refreshes the gate.
			mgr2, err := NewTimelineManager(p2)
			if err != nil {
				t.Fatalf("NewTimelineManager over pending journal: %v", err)
			}
			defer mgr2.Close()
			if !gate.IsOpen() {
				t.Fatalf("gate still closed after manager init: %v", gate.Pending())
			}
			if _, err := os.Stat(filepath.Join(dir, "segdb-tx-777103.journal")); !os.IsNotExist(err) {
				t.Fatalf("journal survived manager-init recovery")
			}
			persistMustGet(t, p2, "m:1", "one")
			persistMustGet(t, p2, "m:2", "two")
			timelineMustGet(t, mgr2, "parent", "k", "v")
			timelineMustGet(t, mgr2, "parent", "db:seed", "base")
		})
	}
}
