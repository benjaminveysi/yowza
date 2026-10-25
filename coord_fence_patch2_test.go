package yowza

// Patch 2, Track 2 fence tests.
//
// Honesty note, stated in the test itself: the facade
// Put/Delete insertion lives in segdb_persist.go, which
// this track may not edit, so these tests exercise the
// hook through exactly the seam the parent inserts —
// the same calls, in the same order, under the same
// p.mu hold — via patch2SimulatedFacadeWrite, which
// mirrors the insertion snippet line for line (mu lock,
// CheckCoordWriteFence, return before any sequence).
// They prove the hook's contract; the parent's insertion
// turns the seam into the real Put/Delete path, and the
// snippet is in coord_fence.go. What is NOT claimed
// here: that an unedited Put already refuses — it does
// not, until the insertion lands.

import (
	"errors"
	"testing"
	"time"
)

// patch2SimulatedFacadeWrite mirrors the parent insertion
// snippet for PersistedSegDB.Put/Delete verbatim in
// shape: hold p.mu, run the write fence, return before
// any sequence is consumed. The body after the check is
// deliberately absent: a refused write must never reach
// it, and an allowed write's body is the real Put/Delete
// (tested by the existing persist suites).
func patch2SimulatedFacadeWrite(p *PersistedSegDB) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := CheckCoordWriteFence(p); err != nil {
		return err
	}
	return nil
}

func TestPatch2CoordFenceFacadeBypass(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			defer p.Close()
			coordDir := t.TempDir()
			sess, err := AcquireCoordinator(coordDir, "fence-owner")
			if err != nil {
				t.Fatalf("acquire: %v", err)
			}
			defer sess.Release()

			// Plain mode: the seam allows the write.
			if err := patch2SimulatedFacadeWrite(p); err != nil {
				t.Fatalf("plain-mode seam write err %v, want nil", err)
			}
			if CoordFenceRegistered(p) {
				t.Fatalf("unregistered DB reports coordinated")
			}
			// Register: double registration is refused loudly.
			if err := RegisterCoordFence(p, sess); err != nil {
				t.Fatalf("register: %v", err)
			}
			if err := RegisterCoordFence(p, sess); !errors.Is(err, ErrCoordFenceAlreadyRegistered) {
				t.Fatalf("double register err %v, want ErrCoordFenceAlreadyRegistered", err)
			}
			// Coordinated mode, session LIVE: a facade
			// write is STILL refused outright with the
			// typed bypass error (see coord_fence.go:
			// a facade Put carries no token, so live
			// session cannot distinguish it from a
			// bypass — refusal is outright).
			if err := patch2SimulatedFacadeWrite(p); !errors.Is(err, ErrCoordFenceBypass) {
				t.Fatalf("coordinated seam write err %v, want ErrCoordFenceBypass", err)
			}
			if err := CheckCoordWriteFence(p); !errors.Is(err, ErrCoordFenceBypass) {
				t.Fatalf("CheckCoordWriteFence err %v, want ErrCoordFenceBypass", err)
			}
			// Maintenance with a live session passes
			// and returns a hold.
			release, err := CheckCoordMaintenanceFence(p)
			if err != nil || release == nil {
				t.Fatalf("live maintenance fence err %v release nil=%v, want nil/non-nil", err, release == nil)
			}
			release()
			// Unregister returns the DB to plain mode,
			// and the real facade works again (proving
			// the refusal was the fence, not a broken DB).
			UnregisterCoordFence(p)
			UnregisterCoordFence(p) // idempotent
			if err := patch2SimulatedFacadeWrite(p); err != nil {
				t.Fatalf("post-unregister seam write err %v, want nil", err)
			}
			if err := p.Put("fence-ok", []byte("v")); err != nil {
				t.Fatalf("real Put after unregister: %v", err)
			}
		})
	}
}

// TestPatch2CoordFenceRaceHold proves the race property:
// a fence revocation attempted between the fence check
// and the mutation's effect cannot publish — because
// the check HOLDS the session mutex across the
// mutation, Release (which needs the same mutex, and
// whose flock release is what makes a takeover
// possible) blocks until the hold is dropped. After
// the hold drops, the next fence check fails loudly.
func TestPatch2CoordFenceRaceHold(t *testing.T) {
	bin := persistTestBins[0]
	dir := t.TempDir()
	p := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
	defer p.Close()
	coordDir := t.TempDir()
	sess, err := AcquireCoordinator(coordDir, "race-owner")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if err := RegisterCoordFence(p, sess); err != nil {
		t.Fatalf("register: %v", err)
	}
	defer UnregisterCoordFence(p)

	// Hold the fence exactly as a maintenance operation
	// would (CheckCoordMaintenanceFence returns the hold).
	hold, err := CheckCoordMaintenanceFence(p)
	if err != nil || hold == nil {
		t.Fatalf("maintenance fence hold err %v", err)
	}
	// A Release attempted while the hold is live must
	// NOT complete: if it did, a takeover could land
	// between check and effect — the forbidden window.
	released := make(chan struct{})
	go func() {
		_ = sess.Release()
		close(released)
	}()
	select {
	case <-released:
		hold()
		t.Fatalf("Release completed while the fence hold was live: revocation interleaved with a checked mutation")
	case <-time.After(300 * time.Millisecond):
		// Expected: Release is blocked on the session mutex.
	}
	// Drop the hold; the pending Release now completes.
	hold()
	select {
	case <-released:
	case <-time.After(5 * time.Second):
		t.Fatalf("Release did not complete after the fence hold was dropped")
	}
	// The session is now released (no takeover yet):
	// the next fence check fails with Released, and a
	// facade write in coordinated mode still refuses
	// with the bypass error (outright, regardless).
	if _, err := CheckCoordMaintenanceFence(p); !errors.Is(err, ErrCoordReleased) {
		t.Fatalf("post-release maintenance fence err %v, want ErrCoordReleased", err)
	}
	if err := patch2SimulatedFacadeWrite(p); !errors.Is(err, ErrCoordFenceBypass) {
		t.Fatalf("post-release seam write err %v, want ErrCoordFenceBypass", err)
	}
}

// TestPatch2CoordFenceStaleAfterTakeover proves the
// maintenance fence fails with the stale-epoch error
// (not a silent pass) once another session has taken
// over, and that the old session's held-check path
// (ValidateFenceLocked) reports the same.
func TestPatch2CoordFenceStaleAfterTakeover(t *testing.T) {
	bin := persistTestBins[0]
	dir := t.TempDir()
	p := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
	defer p.Close()
	coordDir := t.TempDir()
	old, err := AcquireCoordinator(coordDir, "old-owner")
	if err != nil {
		t.Fatalf("acquire old: %v", err)
	}
	if err := RegisterCoordFence(p, old); err != nil {
		t.Fatalf("register: %v", err)
	}
	defer UnregisterCoordFence(p)
	if err := old.Release(); err != nil {
		t.Fatalf("release old: %v", err)
	}
	next, err := AcquireCoordinator(coordDir, "new-owner")
	if err != nil {
		t.Fatalf("acquire new: %v", err)
	}
	defer next.Release()
	if next.Epoch() != old.Epoch()+1 {
		t.Fatalf("takeover epoch %d, want %d", next.Epoch(), old.Epoch()+1)
	}
	// Registry still points at the old session (the DB
	// was registered under it): maintenance now fails
	// stale, and the old session's fence-locked check
	// fails stale without consuming anything.
	if _, err := CheckCoordMaintenanceFence(p); !errors.Is(err, ErrCoordStaleEpoch) {
		t.Fatalf("stale maintenance fence err %v, want ErrCoordStaleEpoch", err)
	}
	if _, err := old.ValidateFenceLocked(); !errors.Is(err, ErrCoordStaleEpoch) {
		t.Fatalf("stale ValidateFenceLocked err %v, want ErrCoordStaleEpoch", err)
	}
	// The new session's fence check passes (it is live);
	// the returned hold must be released (a second
	// ValidateFenceLocked without releasing would
	// deadlock on the non-reentrant session mutex).
	rel, err := next.ValidateFenceLocked()
	if err != nil {
		t.Fatalf("live ValidateFenceLocked err %v, want nil", err)
	}
	if rel == nil {
		t.Fatalf("live ValidateFenceLocked returned nil hold")
	}
	rel()
}
