package yowza

import (
	"errors"
	"path/filepath"
	"testing"
)

// Patch 3: applying transaction journals must close raw facade reads
// after an open/re-attach, and recovery must reopen the gate only after
// the entire journal has rolled forward.
func TestPatch3RecoveryGateDetectsApplyingTxJournal(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			defer p.Close()
			if err := p.Put("seed", []byte("old")); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, "segdb-tx-987654321.journal")
			if _, err := writeTxJournalFile(path, 987654321, "applying", []*SegDBTxWrite{{Key: "replayed", Value: []byte("complete")}}, true); err != nil {
				t.Fatal(err)
			}
			gate := p.AttachRecoveryGate()
			if gate.IsOpen() {
				t.Fatal("gate open with applying transaction journal")
			}
			if _, err := p.Get("seed"); !errors.Is(err, ErrLifecycleRecoveryIncomplete) {
				t.Fatalf("Get before recovery err=%v", err)
			}
			if err := gate.Recover(); err != nil {
				t.Fatalf("Recover: %v", err)
			}
			if !gate.IsOpen() {
				t.Fatalf("gate remained closed: %v", gate.Pending())
			}
			got, err := p.Get("replayed")
			if err != nil || string(got) != "complete" {
				t.Fatalf("replayed=%q err=%v", got, err)
			}
		})
	}
}

func TestPatch3SegDBTransactionCannotBypassCoordinator(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			p := persistOpenForTest(t, bin, t.TempDir(), PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			defer p.Close()
			sess, err := AcquireCoordinator(t.TempDir(), "patch3-owner")
			if err != nil {
				t.Fatal(err)
			}
			defer sess.Release()
			if err := RegisterCoordFence(p, sess); err != nil {
				t.Fatal(err)
			}
			defer UnregisterCoordFence(p)
			tx, err := BeginSegDBTransaction(p)
			if err != nil {
				t.Fatal(err)
			}
			if err := tx.Put("must-not-commit", []byte("x"), false); err != nil {
				t.Fatal(err)
			}
			if err := tx.Commit(); !errors.Is(err, ErrCoordFenceBypass) {
				t.Fatalf("Commit err=%v, want ErrCoordFenceBypass", err)
			}
			if _, err := p.Get("must-not-commit"); !errors.Is(err, ErrSegDBNotFound) {
				t.Fatalf("unexpected committed value, err=%v", err)
			}
		})
	}
}

func TestPatch3PlainDomainCannotBypassCoordinator(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			p := persistOpenForTest(t, bin, t.TempDir(), PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			defer p.Close()
			sess, err := AcquireCoordinator(t.TempDir(), "patch3-domain-owner")
			if err != nil {
				t.Fatal(err)
			}
			defer sess.Release()
			if err := RegisterCoordFence(p, sess); err != nil {
				t.Fatal(err)
			}
			defer UnregisterCoordFence(p)
			d, err := NewTimelineDomain("patch3-domain", t.TempDir(), DomainParticipant{DBID: "db", DB: p})
			if err != nil {
				t.Fatal(err)
			}
			err = d.ApplyAtomic("bypass", map[string][]globalMergeDelta{"db": {{Key: "must-not-apply", Value: []byte("x")}}}, nil)
			if !errors.Is(err, ErrCoordFenceBypass) {
				t.Fatalf("ApplyAtomic err=%v, want ErrCoordFenceBypass", err)
			}
			if _, err := p.Get("must-not-apply"); !errors.Is(err, ErrSegDBNotFound) {
				t.Fatalf("unexpected committed value, err=%v", err)
			}
		})
	}
}
