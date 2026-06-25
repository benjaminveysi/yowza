package yowza

// Patch 2, Track 2 — COORDINATOR FENCING OF FACADE WRITERS.
//
// Axis B built the coordinator (commit lock + fencing epoch,
// shared lease table, shared WAL, multi-shard root) and the
// opt-in coordinated domain path, but left one hole open by
// name: ordinary facade writers (PersistedSegDB.Put/Delete
// and maintenance) never consulted the coordinator, so a
// writer that bypassed the coordinated path was not fenced
// at all. This file closes the coordinator side of that hole
// and defines the hook the segdb owner inserts into the
// facade paths. It deliberately does NOT edit segdb.go or
// segdb_persist.go (outside this track's file set): the
// registry below is keyed by DB instance pointer, so no
// struct field is needed, and the exact insertion points
// and snippets for the parent/integration owner are at
// the bottom of this comment and in
// TIMELINE_V2_PATCH_NOTES.md (Patch 2, Track 2 section).
//
// DESIGN — TWO DIFFERENT CHECKS, BECAUSE THE TWO WRITER
// CLASSES HAVE DIFFERENT CORRECT REFUSALS:
//
//  1. Facade data writes (Put/Delete, and PutBlob through
//     Put): in coordinated mode these are REFUSED OUTRIGHT
//     with ErrCoordFenceBypass, whether or not the session
//     is currently live. Reason: a facade Put carries no
//     coordinator token and no operation ID, so at the DB
//     layer a "coordinated" direct Put is indistinguishable
//     from a bypassing one — the only refusal that is
//     sound without changing Put's signature is to refuse
//     all of them once the DB is registered as coordinated.
//     Coordinated data writes go through the domain core
//     (applyDeltasLocked under all participant p.mu plus
//     the coordinator preCommit), never through the facade.
//     A caller that genuinely needs a single-DB facade
//     write must UnregisterCoordFence first — an explicit,
//     loud mode change, never a silent fallback.
//
//  2. Maintenance (Flush, Compact, RotateWAL, and the pin
//     mutations CreateDurablePin/DeletePin/
//     CleanupOrphanPins): these are NOT domain writes, so
//     outright refusal would make a coordinated DB
//     unmaintainable. They instead require a LIVE session:
//     CheckCoordMaintenanceFence validates fencing and
//     returns a hold (see below). A stale or released
//     session refuses with ErrCoordStaleEpoch /
//     ErrCoordReleased before any file is reclaimed or
//     checkpoint published.
//
// THE ATOMICITY ARGUMENT (why a separate Validate() before
// the mutation is NOT sufficient, and what is instead):
//
// The forbidden pattern is: Validate() -> (ownership
// revoked here) -> sequence consumed -> WAL frame
// published. The hook defeats it differently for the two
// classes:
//
//   - Facade writes: the check is a registry lookup under
//     the DB's own p.mu (the snippet inserts it right
//     after p.mu.Lock(), before any p.seq++ or WAL
//     append). Refusal therefore happens before any
//     sequence is consumed, and acceptance never happens
//     at all in coordinated mode — there is no
//     check-then-publish window to race, because the
//     coordinated path never publishes through the facade.
//
//   - Maintenance: the check HOLDS the session mutex
//     across the whole maintenance operation (the snippet
//     defers the returned release func). Lock order is
//     always p.mu -> session mu (the same order the
//     coordinated domain path already uses: p.mu held,
//     then PublishRoot/AppendWALRecord take the session
//     mu), never the reverse. Release() takes only the
//     session mu, and a cross-process takeover requires
//     the flock release that only Release() performs, so
//     a Release — and therefore a takeover — cannot
//     interleave between the maintenance fence check and
//     the maintenance effect: it blocks on the session mu
//     until the operation finishes, and the next fence
//     check after it fails loudly. In-process, two
//     goroutines cannot interleave either: the session mu
//     serialises them. The maintenance operation is
//     therefore validated under the same hold that
//     protects its effect, exactly as the task requires
//     ("validated under the same db.mu hold that issues
//     the WAL sequence" generalised: the fence hold spans
//     the whole mutation, and db.mu is held throughout
//     the facade method, so the two holds nest, never
//     race).
//
//   - Coordinated domain writes (for contrast): the
//     authoritative fencing check is PublishRoot, which
//     re-validates under the session mu while all
//     participant p.mu are held (coord_domain.go
//     preCommit), i.e. under the same holds that publish
//     the decision — again no validate-then-publish gap.
//
// CROSS-PROCESS SCOPE, STATED PLAINLY: this registry is
// in-process (a map keyed by live DB pointer). It cannot
// fence a writer in another process directly — but no
// such writer can hold the SAME DB directory (the
// exclusive per-DB directory lock forbids it), so for
// one DB directory the in-process registry plus the
// directory lock covers every possible holder. A writer
// in another process on a DIFFERENT DB directory in the
// same domain is fenced only at the domain layer (it
// must use ApplyAtomicCoordinated with a live session;
// its plain ApplyAtomic is unfenced — see "Paths still
// unfenced" below). This file does not claim otherwise.
//
// REGISTRATION LIFECYCLE: RegisterCoordFence(db, sess)
// puts one DB into coordinated mode; it refuses a second
// registration for the same DB (ErrCoordFenceAlreadyRegistered)
// so a mode change is never silent. UnregisterCoordFence
// returns the DB to plain mode. The parent MUST call
// UnregisterCoordFence in the Close path (see snippet);
// a DB closed without unregistering keeps a stale
// registry entry keyed by a dead pointer (a new DB opened
// at the same address would inherit coordinated mode —
// the registry therefore also drops entries whose DB is
// closed when looked up, but that is a backstop, not a
// substitute for unregistering on Close).
//
// ERRORS (all typed, errors.Is-compatible, never silent):
//   - ErrCoordFenceBypass: a facade Put/Delete on a
//     coordinated-mode DB. The write was refused and
//     applied nothing; use the coordinated domain path.
//   - ErrCoordStaleEpoch / ErrCoordReleased (existing):
//     a maintenance fence check against a session that
//     lost ownership / was released. Nothing was done.
//   - ErrCoordFenceAlreadyRegistered: double registration.

import (
	"errors"
	"fmt"
	"sync"
)

// ErrCoordFenceBypass reports a facade data write
// (Put/Delete, or PutBlob through Put) attempted on a
// DB registered as coordinated. The write was refused
// and applied nothing. Coordinated writes must go
// through the coordinated domain path; see the file
// header for why the refusal is outright rather than
// "live session passes".
var ErrCoordFenceBypass = errors.New("yowza coordinator: facade write refused: database is in coordinated mode, use the coordinated domain path")

// ErrCoordFenceAlreadyRegistered reports a second
// RegisterCoordFence for a DB already in coordinated
// mode. Unregister first; mode changes are explicit.
var ErrCoordFenceAlreadyRegistered = errors.New("yowza coordinator: database already registered for coordinator fencing")

// FenceValidator is the seam the fence hook validates
// through. ValidateFenceLocked acquires the validator's
// internal lock, validates live fencing, and — on
// success — returns an unlock function the caller MUST
// defer: the returned hold is what makes the check
// atomic with the mutation that follows (see the file
// header). On error the lock is already released and
// the returned func is nil. CoordinatorSession
// implements it; test fakes may implement it to prove
// the race property without a real coordinator dir.
type FenceValidator interface {
	// ValidateFenceLocked validates fencing and, on
	// success, returns a release func holding the
	// validator's lock until called. Callers must
	// defer the release; dropping it deadlocks the
	// session's next use, which is fail-loud, not
	// fail-silent.
	ValidateFenceLocked() (release func(), err error)
}

// ValidateFenceLocked implements FenceValidator for a
// live coordinator session. It locks the session mutex
// and validates under it (owner record still names this
// owner at this epoch, session still active — epoch
// mismatch before released, as in Validate). On success
// the mutex stays locked until the caller calls the
// returned func, so Release (which needs the same
// mutex) and any other session use cannot interleave
// between this check and the caller's mutation.
func (s *CoordinatorSession) ValidateFenceLocked() (func(), error) {
	if s == nil {
		return nil, fmt.Errorf("%w: nil session", ErrCoordReleased)
	}
	s.mu.Lock()
	if err := s.validateLocked(); err != nil {
		s.mu.Unlock()
		return nil, err
	}
	return s.mu.Unlock, nil
}

var (
	coordFenceMu   sync.Mutex
	coordFenceByDB map[*PersistedSegDB]FenceValidator
)

func coordFenceRegistry() map[*PersistedSegDB]FenceValidator {
	if coordFenceByDB == nil {
		coordFenceByDB = map[*PersistedSegDB]FenceValidator{}
	}
	return coordFenceByDB
}

// RegisterCoordFence puts db into coordinated mode
// under sess. sess must be non-nil; a nil session can
// never fence anything, so registering one is refused
// rather than creating a coordinated DB that every
// maintenance check then fails open on. Re-registering
// the same DB is refused (see ErrCoordFenceAlreadyRegistered).
func RegisterCoordFence(db *PersistedSegDB, sess *CoordinatorSession) error {
	if db == nil {
		return fmt.Errorf("yowza coordinator: cannot fence a nil database")
	}
	if sess == nil {
		return fmt.Errorf("%w: nil session", ErrCoordReleased)
	}
	coordFenceMu.Lock()
	defer coordFenceMu.Unlock()
	if _, ok := coordFenceRegistry()[db]; ok {
		return fmt.Errorf("%w: database %s", ErrCoordFenceAlreadyRegistered, db.Dir())
	}
	coordFenceRegistry()[db] = sess
	return nil
}

// RegisterCoordFenceValidator is the seam variant of
// RegisterCoordFence for a custom FenceValidator (test
// fakes, future validator kinds). Production callers
// use RegisterCoordFence.
func RegisterCoordFenceValidator(db *PersistedSegDB, v FenceValidator) error {
	if db == nil {
		return fmt.Errorf("yowza coordinator: cannot fence a nil database")
	}
	if v == nil {
		return fmt.Errorf("yowza coordinator: cannot fence with a nil validator")
	}
	coordFenceMu.Lock()
	defer coordFenceMu.Unlock()
	if _, ok := coordFenceRegistry()[db]; ok {
		return fmt.Errorf("%w: database %s", ErrCoordFenceAlreadyRegistered, db.Dir())
	}
	coordFenceRegistry()[db] = v
	return nil
}

// UnregisterCoordFence returns db to plain mode. It is
// idempotent (unregistering an unregistered DB is a
// no-op, so Close-path cleanup cannot fail a close).
func UnregisterCoordFence(db *PersistedSegDB) {
	if db == nil {
		return
	}
	coordFenceMu.Lock()
	defer coordFenceMu.Unlock()
	delete(coordFenceRegistry(), db)
}

// CoordFenceRegistered reports whether db is currently
// in coordinated mode. It is a status query, not a
// fence check: a registered DB still refuses facade
// writes even when its session is live (see header).
func CoordFenceRegistered(db *PersistedSegDB) bool {
	if db == nil {
		return false
	}
	coordFenceMu.Lock()
	defer coordFenceMu.Unlock()
	_, ok := coordFenceRegistry()[db]
	return ok
}

func coordFenceFor(db *PersistedSegDB) FenceValidator {
	if db == nil {
		return nil
	}
	coordFenceMu.Lock()
	defer coordFenceMu.Unlock()
	return coordFenceRegistry()[db]
}

// CheckCoordWriteFence is the facade-write hook. The
// parent inserts it at the top of PersistedSegDB.Put
// and Delete (after p.mu.Lock(), before any sequence
// consumption — see snippets below). It returns nil
// for an unregistered (plain-mode) DB, and
// ErrCoordFenceBypass for a coordinated-mode DB: the
// caller must return that error without consuming a
// sequence, writing a WAL frame, or touching a shard.
// It takes no lock itself (the caller already holds
// p.mu); it only reads the registry.
func CheckCoordWriteFence(db *PersistedSegDB) error {
	if coordFenceFor(db) == nil {
		return nil
	}
	return fmt.Errorf("%w: database %s", ErrCoordFenceBypass, db.Dir())
}

// CheckCoordMaintenanceFence is the maintenance hook.
// The parent inserts it at the top of Flush, Compact,
// RotateWAL, and the pin mutations (after p.mu.Lock()).
// For an unregistered DB it returns (nil, nil). For a
// coordinated DB it validates live fencing under the
// validator's lock and returns the hold: the caller
// MUST defer release() so the hold spans the whole
// maintenance operation (see the file header for why
// the hold, not a bare Validate(), is the atomicity
// mechanism). On error release is nil and the caller
// must do nothing.
func CheckCoordMaintenanceFence(db *PersistedSegDB) (func(), error) {
	v := coordFenceFor(db)
	if v == nil {
		return nil, nil
	}
	release, err := v.ValidateFenceLocked()
	if err != nil {
		return nil, err
	}
	return release, nil
}

// ---------------------------------------------------------------------------
// EXACT INSERTION POINTS FOR THE PARENT / INTEGRATION OWNER.
// This track may not edit segdb_persist.go, segdb.go,
// segdb_pins.go, or segdb_persist_lifecycle.go, so these
// snippets are the integration contract: copy them
// verbatim at the named sites (line numbers are from
// the working copy at the time of this patch; match on
// the surrounding code, not only the number).
//
// 1. segdb_persist.go, PersistedSegDB.Put (line ~1334):
//    immediately after "p.mu.Lock()" / "defer p.mu.Unlock()",
//    BEFORE "if p.closed": insert
//        if err := CheckCoordWriteFence(p); err != nil {
//            return err
//        }
//    PutBlob (blob_store.go) calls Put, so it is fenced
//    by this one insertion; no separate PutBlob edit.
//
// 2. segdb_persist.go, PersistedSegDB.Delete (line ~1433):
//    same position, same snippet (return err).
//
// 3. segdb_persist.go, PersistedSegDB.Flush (line ~1709)
//    and Compact (line ~1728): immediately after
//    "p.mu.Lock()" / "defer p.mu.Unlock()", BEFORE
//    "if p.closed": insert
//        release, err := CheckCoordMaintenanceFence(p)
//        if err != nil {
//            return err
//        }
//        if release != nil {
//            defer release()
//        }
//    Flush's error return is "error"; Compact's is the
//    same shape. The deferred release runs after the
//    method's own deferred p.mu.Unlock in LIFO order
//    only if release is deferred AFTER p.mu's defer —
//    which this snippet does, since it sits below the
//    p.mu defer line. Lock order stays p.mu -> session
//    mu throughout; never invert it.
//
// 4. segdb_persist_lifecycle.go, PersistedSegDB.RotateWAL
//    (line ~605): same maintenance snippet as (3), in
//    the same position (after p.mu lock, before work).
//    RotateWAL's exact lock shape must be checked by
//    the inserter: if it does not hold p.mu for its
//    whole body, the fence hold must still span the
//    whole body, and p.mu (if taken) must be taken
//    BEFORE the fence check.
//
// 5. segdb_pins.go, CreateDurablePin (line ~289),
//    DeletePin (line ~367), CleanupOrphanPins
//    (line ~401): same maintenance snippet as (3),
//    adapted to each method's lock shape with the same
//    rule — p.mu (where held) before the fence check,
//    fence hold spanning the whole mutation. Pin
//    creation/deletion changes GC liveness, so a stale
//    owner must not be able to pin or unpin.
//
// 6. segdb_persist.go, PersistedSegDB.Close (line ~1771):
//    after the close completes (after closeLocked,
//    before return, or as the first line of Close —
//    either is safe because Close takes p.mu and the
//    registry needs no lock ordering with it):
//        UnregisterCoordFence(p)
//    Without this, a closed DB's registry entry lingers
//    (backstopped by lookup-time handling, but the
//    explicit unregister is the contract).
//
// PATHS THIS HOOK DOES NOT FENCE (named exactly, so
// the parent does not over-claim — the opt-in rule
// "all coordinated writers use the coordinated path;
// coordinator unavailability fails loudly" remains
// load-bearing for each of them):
//   - SegDBTransaction.Commit (segdb_tx.go): applies
//     through applyTxWritesLocked, never through the
//     facade, so Put/Delete insertion does not touch
//     it. A raw transaction commit on a coordinated DB
//     publishes without a coordinator session today.
//     Future hook (same registry, same shape): a
//     live-session maintenance-style check at the top
//     of commitGate, under db.mu, before the journal
//     is written.
//   - applyDeltasLocked direct callers: the domain core
//     itself (fenced only when entered via
//     ApplyAtomicCoordinated/WithBlobs), RecoverDomainOps
//     / RecoverCoordinatedDomainOps (recovery, fenced
//     by the operator running the coordinated recovery
//     with a live session — the recovery functions
//     themselves take no session), RecoverGlobalMerges,
//     and TimelineManager.MergeToMain's apply
//     (global_merge.go). A direct MergeToMain on a
//     coordinated DB is therefore unfenced today; it
//     needs a coordinated-merge wrapper (future work),
//     not a facade edit.
//   - TimelineManager.Put (timeline.go): overlay-only,
//     not a DB head write; it becomes a head write
//     only via MergeToMain (see above).
//   - File-level operator steps: GCBlobs, CopyBlob,
//     RepairWALTail, ReclaimPinnedFiles. ReclaimPinnedFiles
//     already consults the live lease table; the others
//     are operator steps that must be run by the live
//     owner by procedure, not by a hook in this file.
//   - A second process on a different DB directory in
//     the same domain using plain ApplyAtomic: fenced
//     only by the opt-in rule (it must use the
//     coordinated path); the DB registry is in-process
//     and cannot see it. The exclusive per-DB directory
//     lock is what keeps this from also being a
//     same-directory hole.
