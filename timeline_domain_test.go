package yowza

// Phase E, E2 tests: cross-DB domain, global
// snapshot token, refusal outside a domain, and
// coordinator-log recovery.

import (
	"errors"
	"fmt"
	"sync"
	"testing"
)

func phaseEDomainSetup(t *testing.T, bin string) (*PersistedSegDB, *PersistedSegDB, *TimelineDomain, string) {
	t.Helper()
	dirA := t.TempDir()
	dirB := t.TempDir()
	coord := t.TempDir()
	pa := persistOpenForTest(t, bin, dirA, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
	pb := persistOpenForTest(t, bin, dirB, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
	t.Cleanup(func() {
		pa.Close()
		pb.Close()
	})
	dom, err := NewTimelineDomain("dom-e", coord,
		DomainParticipant{DBID: "a", DB: pa},
		DomainParticipant{DBID: "b", DB: pb})
	if err != nil {
		t.Fatalf("NewTimelineDomain: %v", err)
	}
	return pa, pb, dom, coord
}

func TestPhaseEDomainSnapshotAndRefusal(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			pa, pb, dom, _ := phaseEDomainSetup(t, bin)
			if err := pa.Put("x", []byte("x0")); err != nil {
				t.Fatalf("pa put: %v", err)
			}
			if err := pb.Put("y", []byte("y0")); err != nil {
				t.Fatalf("pb put: %v", err)
			}
			tok, err := dom.CaptureGlobalSnapshot()
			if err != nil {
				t.Fatalf("capture: %v", err)
			}
			// Advance both DBs after capture: the token must still read old values.
			if err := pa.Put("x", []byte("x1")); err != nil {
				t.Fatalf("pa advance: %v", err)
			}
			if err := pb.Put("y", []byte("y1")); err != nil {
				t.Fatalf("pb advance: %v", err)
			}
			v, ok, err := dom.ReadGlobalAt(tok, "a", "x")
			if err != nil || !ok || string(v) != "x0" {
				t.Fatalf("token read a/x = %q, ok %v, err %v, want x0", v, ok, err)
			}
			v, ok, err = dom.ReadGlobalAt(tok, "b", "y")
			if err != nil || !ok || string(v) != "y0" {
				t.Fatalf("token read b/y = %q, ok %v, err %v, want y0", v, ok, err)
			}
			// A token from another domain is refused, reading nothing.
			dom2, err := NewTimelineDomain("dom-other", t.TempDir(),
				DomainParticipant{DBID: "a", DB: pa},
				DomainParticipant{DBID: "b", DB: pb})
			if err != nil {
				t.Fatalf("second domain: %v", err)
			}
			if err := dom2.ValidateToken(tok); !errors.Is(err, ErrGlobalSnapshotOutsideDomain) {
				t.Fatalf("foreign token validate err %v, want ErrGlobalSnapshotOutsideDomain", err)
			}
			if _, _, err := dom2.ReadGlobalAt(tok, "a", "x"); !errors.Is(err, ErrGlobalSnapshotOutsideDomain) {
				t.Fatalf("foreign token read err %v, want ErrGlobalSnapshotOutsideDomain", err)
			}
			// A tampered token (vector changed, checksum recomputed by a
			// white-box caller) fails the pin check: the pin's vector
			// no longer matches the token's.
			tampered := *tok
			tampered.Participants = append([]TokenParticipant(nil), tok.Participants...)
			tampered.Participants[0].Vector = []uint64{99999}
			tampered.Checksum = domainTokenChecksum(tampered.DomainID, tampered.Epoch, tampered.Participants)
			if err := dom.ValidateToken(&tampered); !errors.Is(err, ErrGlobalSnapshotOutsideDomain) {
				t.Fatalf("tampered token validate err %v, want ErrGlobalSnapshotOutsideDomain", err)
			}
			// A token naming an unknown participant is refused.
			unknown := *tok
			unknown.Participants = append([]TokenParticipant(nil), tok.Participants...)
			unknown.Participants = append(unknown.Participants, TokenParticipant{DBID: "c", Vector: []uint64{0}, PinID: "pin-zzz"})
			unknown.Checksum = domainTokenChecksum(unknown.DomainID, unknown.Epoch, unknown.Participants)
			if err := dom.ValidateToken(&unknown); !errors.Is(err, ErrGlobalSnapshotOutsideDomain) {
				t.Fatalf("unknown-participant token validate err %v, want ErrGlobalSnapshotOutsideDomain", err)
			}
			// Releasing the token retires it: reads then fail validation.
			if err := dom.ReleaseGlobalSnapshot(tok); err != nil {
				t.Fatalf("release: %v", err)
			}
			if err := dom.ValidateToken(tok); !errors.Is(err, ErrGlobalSnapshotOutsideDomain) {
				t.Fatalf("released token validate err %v, want ErrGlobalSnapshotOutsideDomain", err)
			}
		})
	}
}

func TestPhaseEDomainAtomicApplyAndRecovery(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			pa, pb, dom, _ := phaseEDomainSetup(t, bin)
			writes := map[string][]globalMergeDelta{
				"a": {{Key: "da", Value: []byte("va")}},
				"b": {{Key: "db", Value: []byte("vb")}},
			}
			vecA, _, err := pa.SnapshotVector()
			if err != nil {
				t.Fatalf("vec a: %v", err)
			}
			vecB, _, err := pb.SnapshotVector()
			if err != nil {
				t.Fatalf("vec b: %v", err)
			}
			// Stale expected vector: nothing staged, logged, or applied.
			if err := dom.ApplyAtomic("op-stale", writes, map[string][]uint64{"a": {99999}, "b": vecB}); !errors.Is(err, ErrDomainStaleVector) {
				t.Fatalf("stale apply err %v, want ErrDomainStaleVector", err)
			}
			phaseEMustDBMissing(t, pa, "da")
			phaseEMustDBMissing(t, pb, "db")
			// Clean apply with correct vectors.
			if err := dom.ApplyAtomic("op-1", writes, map[string][]uint64{"a": vecA, "b": vecB}); err != nil {
				t.Fatalf("apply: %v", err)
			}
			phaseEMustDBGet(t, pa, "da", "va")
			phaseEMustDBGet(t, pb, "db", "vb")
			// Prepare-only crash: stage without commit -> recovery reclaims.
			writes2 := map[string][]globalMergeDelta{
				"a": {{Key: "da2", Value: []byte("va2")}},
			}
			if err := dom.stageOp("op-prepare-only", writes2); err != nil {
				t.Fatalf("stage: %v", err)
			}
			rolled, err := dom.RecoverDomainOps()
			if err != nil {
				t.Fatalf("recover prepare-only: %v", err)
			}
			if len(rolled) != 0 {
				t.Fatalf("prepare-only rolled %v, want none", rolled)
			}
			phaseEMustDBMissing(t, pa, "da2")
			// Commit-logged crash, partial apply -> recovery rolls forward.
			writes3 := map[string][]globalMergeDelta{
				"a": {{Key: "da3", Value: []byte("va3")}},
				"b": {{Key: "db3", Value: []byte("vb3")}},
			}
			if err := dom.stageOp("op-crash", writes3); err != nil {
				t.Fatalf("stage crash op: %v", err)
			}
			if err := dom.appendLog("commit", "op-crash", map[string]string{"op": "op-crash"}); err != nil {
				t.Fatalf("commit log: %v", err)
			}
			if err := pa.Put("da3", []byte("va3")); err != nil {
				t.Fatalf("partial apply: %v", err)
			}
			rolled, err = dom.RecoverDomainOps()
			if err != nil {
				t.Fatalf("recover committed: %v", err)
			}
			if len(rolled) != 1 || rolled[0] != "op-crash" {
				t.Fatalf("rolled %v, want [op-crash]", rolled)
			}
			phaseEMustDBGet(t, pa, "da3", "va3")
			phaseEMustDBGet(t, pb, "db3", "vb3")
			// Second recovery is a no-op (done logged).
			rolled, err = dom.RecoverDomainOps()
			if err != nil {
				t.Fatalf("second recover: %v", err)
			}
			if len(rolled) != 0 {
				t.Fatalf("second recover rolled %v, want none", rolled)
			}
		})
	}
}

// Axis B race tests (goroutine simulation is explicit
// here: the race under test is in-process — an ordinary
// facade writer and a domain apply sharing the same
// PersistedSegDB instances and therefore the same
// p.mu. Cross-process races cannot share p.mu at all
// and are covered by the coordinator tests in
// coord_axisb_test.go, not by goroutines here).

// TestAxisBApplyAtomicRaceWriter races one ordinary
// Put against one domain apply on the same key, with
// expected vectors captured before the race. The
// invariant that closes the old post-commit window:
// the writer's value is NEVER lost to a successful
// domain apply. Under the old code a writer landing
// between the commit log and the apply was overwritten
// (domain success + final == domain value, writer
// lost). Under the fixed code every schedule ends
// with final == writer value: writer-before-recheck
// makes the domain stale (writer preserved, domain
// applied nothing — checked on the other participant
// too), writer-after-apply is correct last-writer
// order, and no writer can land between commit and
// apply because all p.mu are held. The domain result
// must therefore be nil or ErrDomainStaleVector, and
// the other participant's write must be present
// exactly when the domain succeeded (no partial
// multi-DB apply).
func TestAxisBApplyAtomicRaceWriter(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			for iter := 0; iter < 20; iter++ {
				pa, pb, dom, _ := phaseEDomainSetup(t, bin)
				if err := pa.Put("k", []byte("base")); err != nil {
					t.Fatalf("iter %d base put: %v", iter, err)
				}
				vecA, _, err := pa.SnapshotVector()
				if err != nil {
					t.Fatalf("iter %d vec a: %v", iter, err)
				}
				vecB, _, err := pb.SnapshotVector()
				if err != nil {
					t.Fatalf("iter %d vec b: %v", iter, err)
				}
				writerVal := fmt.Sprintf("w-%d", iter)
				writes := map[string][]globalMergeDelta{
					"a": {{Key: "k", Value: []byte("dom")}},
					"b": {{Key: "kb", Value: []byte("vb")}},
				}
				expected := map[string][]uint64{"a": vecA, "b": vecB}
				start := make(chan struct{})
				var wg sync.WaitGroup
				var writerErr, domErr error
				wg.Add(2)
				go func() {
					defer wg.Done()
					<-start
					writerErr = pa.Put("k", []byte(writerVal))
				}()
				go func() {
					defer wg.Done()
					<-start
					domErr = dom.ApplyAtomic("op-race", writes, expected)
				}()
				close(start)
				wg.Wait()
				if writerErr != nil {
					t.Fatalf("iter %d writer err: %v", iter, writerErr)
				}
				if domErr != nil && !errors.Is(domErr, ErrDomainStaleVector) {
					t.Fatalf("iter %d domain err %v, want nil or ErrDomainStaleVector", iter, domErr)
				}
				v, err := pa.Get("k")
				if err != nil || string(v) != writerVal {
					t.Fatalf("iter %d final k = %q err %v, want %q (writer lost to domain apply)", iter, v, err, writerVal)
				}
				if domErr == nil {
					phaseEMustDBGet(t, pb, "kb", "vb")
				} else {
					phaseEMustDBMissing(t, pb, "kb")
				}
				// Close per iteration, not just via the
				// setup's t.Cleanup at test end: 40
				// iterations would otherwise hold 80
				// open DBs (wasm runtimes) at once,
				// which killed a -race run with
				// "signal: killed" and no race warning
				// (resource exhaustion, not a data
				// race). Close is idempotent, so the
				// Cleanup close afterwards is harmless.
				_ = pa.Close()
				_ = pb.Close()
			}
		})
	}
}

// TestAxisBApplyAtomicConcurrentDomains races two
// domain operations through DIFFERENT TimelineDomain
// objects (different d.mu, so the domain mutex cannot
// serialise them) over the same DBs, with the same
// expected vectors and overlapping writes. Exactly
// one must succeed and the final value must be the
// winner's: under the old code both could succeed
// (each pre-checked before either applied), silently
// losing the loser's write. Under the fixed code the
// second operation's authoritative re-check under
// held p.mu sees the first operation's apply and
// returns ErrDomainStaleVector with nothing applied.
func TestAxisBApplyAtomicConcurrentDomains(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			for iter := 0; iter < 20; iter++ {
				pa, pb, domA, _ := phaseEDomainSetup(t, bin)
				domB, err := NewTimelineDomain("dom-b", t.TempDir(),
					DomainParticipant{DBID: "a", DB: pa},
					DomainParticipant{DBID: "b", DB: pb})
				if err != nil {
					t.Fatalf("iter %d second domain: %v", iter, err)
				}
				if err := pa.Put("k", []byte("base")); err != nil {
					t.Fatalf("iter %d base put: %v", iter, err)
				}
				vecA, _, err := pa.SnapshotVector()
				if err != nil {
					t.Fatalf("iter %d vec a: %v", iter, err)
				}
				vecB, _, err := pb.SnapshotVector()
				if err != nil {
					t.Fatalf("iter %d vec b: %v", iter, err)
				}
				expected := map[string][]uint64{"a": vecA, "b": vecB}
				start := make(chan struct{})
				var wg sync.WaitGroup
				var errA, errB error
				wg.Add(2)
				go func() {
					defer wg.Done()
					<-start
					errA = domA.ApplyAtomic("op-a", map[string][]globalMergeDelta{"a": {{Key: "k", Value: []byte("VA")}}}, expected)
				}()
				go func() {
					defer wg.Done()
					<-start
					errB = domB.ApplyAtomic("op-b", map[string][]globalMergeDelta{"a": {{Key: "k", Value: []byte("VB")}}}, expected)
				}()
				close(start)
				wg.Wait()
				succeeded := 0
				var winner string
				if errA == nil {
					succeeded++
					winner = "VA"
				} else if !errors.Is(errA, ErrDomainStaleVector) {
					t.Fatalf("iter %d domA err %v, want nil or stale", iter, errA)
				}
				if errB == nil {
					succeeded++
					winner = "VB"
				} else if !errors.Is(errB, ErrDomainStaleVector) {
					t.Fatalf("iter %d domB err %v, want nil or stale", iter, errB)
				}
				if succeeded != 1 {
					t.Fatalf("iter %d succeeded=%d errA=%v errB=%v, want exactly one success", iter, succeeded, errA, errB)
				}
				v, err := pa.Get("k")
				if err != nil || string(v) != winner {
					t.Fatalf("iter %d final k = %q err %v, want winner %q", iter, v, err, winner)
				}
				// Per-iteration close, as in the writer
				// race test: bounds open DBs to one
				// pair at a time (see there for why
				// Cleanup-only closing killed a -race
				// run). Close is idempotent.
				_ = pa.Close()
				_ = pb.Close()
			}
		})
	}
}
