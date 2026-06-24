package yowza

// Axis B — COORDINATED DOMAIN APPLY.
//
// ApplyAtomicCoordinated is ApplyAtomic plus the
// coordinator contract, in this order (all steps
// after the vector re-check happen while every
// participant's p.mu is held — see
// applyAtomicLocked):
//
//   1. Early fencing check (Validate). This is an
//      early refusal only; the authoritative fencing
//      check is step 4, so ownership lost between
//      step 1 and step 4 is still caught.
//   2. PrepareMultiShard: stage the per-DB payloads
//      in the coordinator directory (no decision yet).
//   3. Domain stage + authoritative vector re-check
//      under all p.mu (the plain core).
//   4. preCommit, still under all p.mu: append one
//      shared-WAL frame for the operation (sequence
//      authority — a frame alone is NOT a decision),
//      then PublishRoot (the coordinator decision).
//      PublishRoot re-validates fencing under the
//      session mutex and re-validates every staged
//      payload before publishing, so a stale session
//      or corrupt staging publishes nothing.
//   5. Domain commit log (second decision record),
//      apply, domain done log, domain stage removal
//      (the plain core).
//   6. MarkDone: delete the coordinator root and
//      staged files. If this fails, the operation
//      IS applied and the domain is done, but the
//      coordinator root remains, so the error
//      returned wraps ErrDomainOpCommitted and the
//      caller must run coordinated recovery — which
//      will re-apply the same values idempotently
//      and then clean up. The failure is reported,
//      never swallowed into a clean success.
//
// Crash outcomes for a coordinated operation:
//   - before the root: all-old. Coordinator recovery
//     reclaims the staged files; plain domain
//     recovery reclaims the domain stage (no domain
//     commit record exists). Neither applies anything.
//   - after the root (whether or not the domain
//     commit log landed): all-new, via EITHER
//     recovery. Coordinated recovery rolls the
//     coordinator payloads forward; plain domain
//     recovery rolls the domain stage forward if a
//     domain commit record exists. Both applies are
//     idempotent puts/deletes, so running both
//     recoveries, in either order, still ends
//     all-new. Plain recovery ALONE does not complete
//     a root-only operation (no domain commit record
//     yet) — coordinated recovery is required for
//     that case, and this is stated, not hidden.
//   - after full success: both recoveries are no-ops
//     (domain done logged + stage removed, coordinator
//     root + staged files deleted).
//
// Opt-in and loud failure: a nil session, a stale
// session, a released session, or an unavailable
// coordinator (acquire would fail with
// ErrCoordLocked) all refuse BEFORE any apply. The
// coordinated path never silently degrades to the
// plain path: there is no fallback in this file that
// drops the coordinator checks on error. Any error
// from a coordinated apply (including a directory
// sync failure after the root rename, where the root
// file itself is already present) must be followed by
// coordinated recovery, never by a fresh retry under
// a new opID with the same writes: recovery is
// idempotent, a fresh retry is not deduplicated
// against an indeterminate publish.
//
// What this path still does NOT do (honest limits):
// it applies to DB instances held by THIS process.
// It does not apply writes to a DB owned by another
// process — that would need a participant-apply
// protocol (each participant applying its shard
// after root publish, or RPC to the owning process)
// on top of the root, which is not built here. The
// exclusive per-DB directory lock is unchanged, so
// the multi-process shape this path supports today
// is: distinct processes own distinct DB directories
// and agree through one coordinator directory, with
// the process that holds all participant handles
// performing the coordinated apply, while any other
// process holding a stale session is fenced out of
// publishing. Cross-machine use needs a distributed
// coordinator (see coord_coordinator.go).

import (
	"fmt"
	"os"
	"sort"
)

// ApplyAtomicCoordinated applies a domain operation
// under coordinator fencing (see the file header).
// coordDir is the coordinator directory the session
// was acquired in; it must match sess.Dir() (a
// mismatch is refused rather than silently writing
// staged files to a directory the session does not
// own). opID must be a valid coordinator operation
// name (see coordValidName); writes use DB IDs as
// shard IDs, so DB IDs must also be valid names.
func (d *TimelineDomain) ApplyAtomicCoordinated(sess *CoordinatorSession, coordDir, opID string, writes map[string][]globalMergeDelta, expectedVectors map[string][]uint64) error {
	return d.ApplyAtomicCoordinatedWithBlobs(sess, coordDir, opID, writes, expectedVectors, nil)
}

// ApplyAtomicCoordinatedWithBlobs is
// ApplyAtomicCoordinated plus the Patch 2 Track 2 blob
// wiring (typed manifest only — see blob_wiring_patch2.go;
// a nil manifest stages no blobs). Blob copy + staging
// happens FIRST, before PrepareMultiShard, so the blobs
// are durable before either decision (coordinator root
// or domain commit log). On a no-decision error (early
// fencing failure, coordinator staging failure,
// preCommit/root failure, stale vector) the blob staging
// is abort-resolved and the coordinator staging is
// aborted; on a published decision (root published, or
// domain commit logged — ErrDomainOpCommitted) the blob
// staging is LEFT for coordinated recovery, which rolls
// forward and then commit-resolves it. On full success
// the blob staging is commit-resolved after MarkDone;
// a resolve failure there is reported as
// ErrDomainOpCommitted (data in, recovery finishes
// cleanup — plain domain recovery's sweep
// commit-resolves staging for done operations, since
// the domain done log exists; the coordinated sweep
// deliberately skips domain-committed ops).
func (d *TimelineDomain) ApplyAtomicCoordinatedWithBlobs(sess *CoordinatorSession, coordDir, opID string, writes map[string][]globalMergeDelta, expectedVectors map[string][]uint64, man *BlobOpManifest) error {
	if d == nil {
		return fmt.Errorf("yowza domain: nil domain")
	}
	if sess == nil {
		return fmt.Errorf("%w: nil coordinator session (coordinated apply requires a live session, it never falls back to plain apply)", ErrCoordReleased)
	}
	if coordDir != sess.Dir() {
		return fmt.Errorf("yowza coordinator: coord dir %q does not match session dir %q", coordDir, sess.Dir())
	}
	if !coordValidName(opID) {
		return fmt.Errorf("yowza coordinator: invalid operation id %q", opID)
	}
	// Early fencing refusal (authoritative check is
	// inside preCommit, under all p.mu).
	if err := sess.Validate(); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for dbID := range writes {
		if _, ok := d.parts[dbID]; !ok {
			return fmt.Errorf("yowza domain: write set for unknown participant %q", dbID)
		}
		if !coordValidName(dbID) {
			return fmt.Errorf("yowza coordinator: participant id %q is not a valid coordinator shard name", dbID)
		}
	}
	// Blob staging FIRST (durable before any decision).
	// The manifest wrapper below would stage again, so
	// stage here and pass the staged dirs through the
	// local core call instead of the plain wrapper: the
	// core call is applyAtomicLocked via a small local
	// sequence that mirrors applyAtomicWithManifestLocked
	// without double-staging. To keep one staging site,
	// validate + stage here, then run the core, then
	// resolve here.
	targetDirOf := func(dbID string) (string, bool) {
		db, ok := d.parts[dbID]
		if !ok || db == nil {
			return "", false
		}
		return db.Dir(), true
	}
	stagedDirs, err := prepareBlobsFromManifest(man, opID, writes, targetDirOf)
	if err != nil {
		return err
	}
	abortBlobs := func() {
		_ = resolveBlobStagingDirs(stagedDirs, opID, false)
	}
	// Coordinator staging (no decision yet). A failure
	// here leaves nothing staged in the domain either
	// (the domain stage is written inside the core).
	if err := PrepareMultiShard(sess, coordDir, opID, writes); err != nil {
		abortBlobs()
		return err
	}
	// Collect the operation's keys for the WAL frame
	// (sorted, deduplicated across participants).
	keySet := map[string]bool{}
	for _, deltas := range writes {
		for _, dl := range deltas {
			keySet[dl.Key] = true
		}
	}
	var keys []string
	for k := range keySet {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	preCommit := func() (bool, error) {
		// WAL frame first (sequence, not decision),
		// then the root (decision). Both run under
		// all p.mu AND the session mutex (PublishRoot
		// takes it; AppendWALRecord takes it), so no
		// concurrent session use can interleave a
		// second decision between them.
		if _, err := sess.AppendWALRecord("domain-commit", opID, keys); err != nil {
			return false, err
		}
		if err := PublishRoot(sess, coordDir, opID); err != nil {
			return false, err
		}
		return true, nil
	}
	published, err := d.applyAtomicLocked(opID, writes, expectedVectors, preCommit)
	if err != nil {
		if !published {
			// No decision anywhere: reclaim the
			// coordinator staging (the domain stage
			// was already removed by the core on
			// pre-check / preCommit failure paths,
			// and a domain-commit-log failure with
			// published=false means the log never
			// landed — but see below for the
			// published=true case, which must NOT
			// be reclaimed). Blob staging is
			// abort-resolved on the same no-decision
			// condition; on a published decision it
			// is LEFT for coordinated recovery.
			_ = AbortMultiShard(coordDir, opID)
			abortBlobs()
		}
		return err
	}
	// Success: clean up the coordinator decision.
	// A cleanup failure is loud (see file header):
	// the data is in, but recovery must still run to
	// finish the cleanup, and re-applying is harmless.
	if err := MarkDone(coordDir, opID); err != nil {
		return fmt.Errorf("%w: op %s applied but coordinator cleanup failed: %v", ErrDomainOpCommitted, opID, err)
	}
	// Commit-resolve blob staging last: the data is in
	// and the head reference protects the blobs. A
	// resolve failure here is reported as committed-
	// but-incomplete (recovery drops the leftover
	// staging for a done operation), never as a plain
	// failure inviting a non-idempotent retry.
	if err := resolveBlobStagingDirs(stagedDirs, opID, true); err != nil {
		return fmt.Errorf("%w: op %s applied but blob staging resolve failed: %v", ErrDomainOpCommitted, opID, err)
	}
	return nil
}

// RecoverCoordinatedDomainOps rolls forward every
// coordinator-committed operation for this domain's
// participants and cleans each one up. It returns the
// op IDs rolled forward, sorted. It must not run
// concurrently with an active ApplyAtomic or
// ApplyAtomicCoordinated (d.mu is held throughout).
// A committed operation naming an unknown participant
// fails loudly BEFORE any of its shards are applied:
// a half-applied coordinated operation is never
// produced by this function (a later recovery retry
// after the participant set is fixed will still find
// the root and complete it).
func (d *TimelineDomain) RecoverCoordinatedDomainOps(coordDir string) ([]string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	committed, err := RecoverMultiShard(coordDir)
	if err != nil {
		return nil, err
	}
	var rolled []string
	for _, op := range committed {
		// Item 1: decider operations (Tx/merge) share a
		// coordinator directory only by operator
		// choice; they are not domain operations and
		// their own recovery (RecoverCoordinatedTx /
		// RecoverCoordinatedMerges) owns them.
		// Skipping them here keeps domain recovery
		// from failing loudly on a shard it was never
		// meant to apply — while genuinely unknown
		// domain shards still fail below.
		if op.Kind == CoordDecisionTx || op.Kind == CoordDecisionMerge {
			continue
		}
		for dbID := range op.Writes {
			if _, ok := d.parts[dbID]; !ok {
				return rolled, fmt.Errorf("%w: coordinated op %s names unknown participant %q", ErrCoordCorrupt, op.OpID, dbID)
			}
		}
		rollErr := func() error {
			unlock := d.lockParticipantsLocked()
			defer unlock()
			for _, dbID := range d.order {
				deltas, ok := op.Writes[dbID]
				if !ok || len(deltas) == 0 {
					continue
				}
				if applyErr := applyDeltasLocked(d.parts[dbID], deltas); applyErr != nil {
					return fmt.Errorf("%w: roll-forward of coordinated op %s on %s: %v", ErrDomainOpCommitted, op.OpID, dbID, applyErr)
				}
			}
			return nil
		}()
		if rollErr != nil {
			return rolled, rollErr
		}
		rolled = append(rolled, op.OpID)
		if err := MarkDone(coordDir, op.OpID); err != nil {
			return rolled, fmt.Errorf("%w: coordinated op %s applied but coordinator cleanup failed: %v", ErrDomainOpCommitted, op.OpID, err)
		}
		// Patch 2 Track 2: the rolled-forward op's
		// blobs are now in the head, so its blob
		// staging resolves as committed (lenient).
		// A roll-forward failure above returns early
		// WITHOUT resolving, so staging survives for
		// the next recovery attempt.
		for _, dir := range d.participantDirsLocked() {
			resolveBlobStagingLenient(dir, op.OpID, true)
		}
	}
	// Patch 2 Track 2 — blob staging sweep for
	// no-decision coordinated leftovers: a staged blob
	// operation with NO domain commit and NO coordinator
	// root in coordDir will never be applied by any
	// recovery, so it is abort-resolved (collectible).
	// Operations with either decision are SKIPPED here
	// (a domain-committed op belongs to plain domain
	// recovery, a rooted op was rolled above or will
	// be on a retry) — resolving one early would let
	// a GC pass delete bytes a roll-forward still
	// needs. The domain log is read best-effort: if it
	// cannot be read, only the root check guards the
	// sweep, and domain-committed ops are then skipped
	// only if they also have no root — the sweep stays
	// conservative in the other direction (it may leave
	// a staging record for plain recovery, never abort
	// a rooted one).
	domCommitted, _, domErr := scanDomainLog(d.logPath())
	rolledSet := map[string]bool{}
	for _, id := range rolled {
		rolledSet[id] = true
	}
	for _, dir := range d.participantDirsLocked() {
		for _, blobOp := range StagedBlobOps(dir) {
			if rolledSet[blobOp] {
				continue
			}
			if _, err := os.Stat(coordRootPath(coordDir, blobOp)); err == nil {
				continue
			}
			if domErr == nil && domCommitted[blobOp] {
				continue
			}
			resolveBlobStagingLenient(dir, blobOp, false)
		}
	}
	sort.Strings(rolled)
	return rolled, nil
}
