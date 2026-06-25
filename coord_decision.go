package yowza

// Item 1 — ONE COMMIT-DECISION ABSTRACTION OVER LOCAL AND RAFT MODES.
//
// Problem (patch3 leftover): on a coordinator-fenced DB, plain
// SegDBTransaction commits and plain TimelineManager.MergeToMain are
// REFUSED with ErrCoordFenceBypass, pending "a transaction-aware
// coordinated commit protocol". This file is that protocol's decision
// layer, shared by the coordinated Tx commit (segdb_tx.go) and the
// coordinated merge (global_merge.go) — one abstraction, two modes,
// no per-caller decision re-implementation.
//
// THE CONTRACT, IN ONE PARAGRAPH. A caller that wants a coordinated
// write presents an explicit token — a CoordDecider plus an operation
// ID — never an ambient coordinator and never a tokenless fallback:
// on a fenced DB a tokenless commit stays refused with
// ErrCoordFenceBypass. Inside the DB's commit gate (db.mu for Tx,
// p.mu for merge), AFTER validation and BEFORE any write is applied,
// the caller calls Decide(opID, kind, dbDir, mergeID, writes). Decide
// durably publishes exactly one decision for that opID and returns
// New (fresh decision, proceed to apply), Existing (the same opID
// with the same payload was already decided but not completed — a
// crash-interrupted attempt — proceed to apply, i.e. roll forward),
// or Completed (the same opID with the same payload already applied
// and completed — return success WITHOUT re-applying). A decision
// for the same opID with a DIFFERENT payload is refused loudly with
// ErrCoordOpExists: an opID is one operation, never two. A stale or
// released session is refused with ErrCoordStaleEpoch /
// ErrCoordReleased (via the session's own validation, inside the
// gate — a Begin-time check alone is never sufficient, and none is
// relied on here). Once Decide has succeeded, a later failure is
// NEVER a clean failure: the decision stands, and only coordinated
// recovery (RecoverCoordinatedTx / RecoverCoordinatedMerges) may
// complete it, by re-applying the decided write set idempotently.
// There is no "recovery mode" flag any caller can set: recovery is
// these two functions, and they complete ONLY operations whose
// decision is durable in the coordinator log (root present locally,
// majority-committed log entry for Raft), nothing else. Staged-only
// operations (no decision) are reclaimed, never applied.
//
// THE TWO MODES, AND WHY THEY SHARE THIS INTERFACE.
//
//   - Local (LocalCoordDecider): the decision is the existing
//     multi-shard root protocol — PrepareMultiShard stages the
//     payload, PublishRoot atomically publishes coord-root-<opID>
//     .json (tmp + sync + rename + dir sync). The decision file
//     itself carries no kind/DB identity (it predates this layer),
//     so Decide additionally writes a small annotation file,
//     coord-decision-<opID>.json (CRC-sealed: opID, kind, DB dir,
//     merge ID, write-set fingerprint), BEFORE staging. The
//     annotation is what lets recovery tell a Tx op from a merge
//     op from a legacy domain op (which has no annotation and is
//     left to domain recovery), and what lets recovery resolve
//     merge blob staging (via the merge ID) and clean global-merge
//     staging files. The annotation is written with O_EXCL: a
//     second Decide for the same opID with a different payload
//     cannot overwrite the first op's annotation. It is deleted
//     only by Complete, after the completion record below is
//     durable, so a committed op always has either its annotation
//     or its completion record — never neither (which is what
//     makes the "legacy op, skip it" rule in CommittedOps sound).
//     Completion is a separate durable record,
//     coord-completed-<opID>.json, written by Complete BEFORE
//     MarkDone deletes the root and staging. MarkDone alone would
//     erase the only trace of the operation, and a later retry
//     with the same opID would look like a fresh operation and
//     re-apply; the completion record is what makes retry
//     idempotent after success. Crash between the completion
//     record and MarkDone leaves root + completion: recovery
//     cleans the root without re-applying (the apply already
//     happened — Complete is only called after a successful
//     apply), which is still exactly-once visible effect,
//     because every apply in this layer is idempotent
//     puts/deletes.
//
//   - Raft (RaftCoordDecider): the decision is a majority-committed
//     Raft log entry, via the Track B decision API EXTENDED to
//     Tx/Merge payloads (RaftCoordinatorSession.ProposeCoordDecision
//     in raft_coord_api.go), not bypassed. No new Raft entry kind
//     was needed: the machine already carries arbitrary multi-shard
//     write sets under kind "multishard-commit", and the extension
//     is metadata on that payload (kind, DB dir, merge ID,
//     fingerprint) — the same fields the local annotation carries,
//     so both modes produce the same CoordMultiShardOp shape for
//     recovery. A Raft decision is durable when the entry is
//     majority-committed; Decide additionally waits for local
//     apply (so the committed-op record is visible to this node)
//     and verifies the committed payload matches the proposed one
//     (a concurrent same-opID proposal from another process that
//     committed first wins deterministically in the machine; a
//     loser with a different payload gets ErrCoordOpExists, never
//     a silent apply of the wrong payload). The completion record
//     is the same coord-completed-<opID>.json file in the Raft
//     node's directory: MarkRaftDone deletes only the local
//     projection file, and a node restart re-derives it from the
//     log, so without the completion record a retry after a
//     restart would re-apply — the record is what prevents that.
//     Raft CommittedOps reads the live state machine (not the
//     projection files), because the machine is authoritative and
//     survives MarkRaftDone; file-based recovery alone would miss
//     a MarkRaftDone'd op until a node restart re-derives it.
//
// LOCK ORDER (read before touching this file). Commit gates hold
// db.mu (Tx) or p.mu (merge) outermost. Inside the gate, Decide /
// Complete / DiscardUndecided take the per-coordinator-directory
// mutex (coordDecisionDirLock) next, and only then the session
// mutex (local PublishRoot / Validate) or the Raft propose path.
// No code path ever takes the directory mutex while holding a
// session mutex, and recovery never holds db.mu while taking the
// directory mutex (CommittedOps, then apply under db.mu, then
// Complete — strictly sequential). The session mutex is NOT held
// across the apply itself (PublishRoot/AppendWALRecord take it
// only for the publish, exactly as the coordinated domain path's
// preCommit does): fencing authority is the decision, published
// under the gate before any apply; a takeover after the decision
// cannot retract it, and recovery completes it. Holding the
// session mutex across the apply would deadlock against
// PublishRoot (which takes it) and is not attempted.
//
// OPID UNIQUENESS IS A CALLER CONTRACT. Operation IDs must be
// unique per coordinator directory across ALL coordinator users
// (domain ops, Tx ops, merge ops). The in-process directory mutex
// serialises Decide calls in this process, and the local flock
// serialises local decisions across processes (only the session
// owner can decide), but two Raft clients in different processes
// proposing the same opID concurrently with different payloads
// resolve to first-committed-wins in the machine, with the loser
// refused (see above) — the same first-wins rule the machine
// already applies to duplicate multi-shard commits. Reusing an
// opID for a different operation is a caller error, refused
// loudly wherever it is detectable, never silently merged.
//
// WHAT THIS LAYER DELIBERATELY DOES NOT DO. It does not change
// the coordinated domain path (coord_domain.go): that path already
// has its own Prepare/PublishRoot/MarkDone discipline and its own
// tests, and refactoring it onto this interface would risk a
// working path for no behavioural gain. The local decider reuses
// its exact primitives (PrepareMultiShard, PublishRoot,
// RecoverMultiShard), so decision semantics are identical; a
// future refactor of the domain path onto CoordDecider is safe
// but not required by this layer. It does not implement cross-DB
// 2PC: there is no multi-DB transaction API, and this layer is
// per-DB by construction (one DB dir per op). Domain ops that
// share a coordinator directory with Tx/merge ops are skipped by
// decider recovery (no annotation / empty kind) and skipped by
// domain recovery in the other direction (see
// RecoverCoordinatedDomainOps), so sharing a directory is
// fail-loud only for genuinely unknown shards, never silently
// cross-applied.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// ErrCoordCleanupPending reports an operation whose write set is
// fully applied but whose coordinator cleanup (completion record
// or MarkDone) failed. The data is in; only coordinated recovery
// is needed to finish the cleanup, and re-running it is harmless
// (it re-applies idempotently and retries the cleanup). It is NOT
// a refusal and NOT a clean failure: the caller must not retry
// the operation under a new opID.
var ErrCoordCleanupPending = errors.New("yowza coordinator: operation applied but coordinator cleanup failed; run coordinated recovery")

// CoordDecisionKind names what kind of operation a coordinator
// decision carries. Legacy domain decisions have the empty kind.
type CoordDecisionKind string

const (
	// CoordDecisionTx is a single-DB SegDBTransaction commit.
	CoordDecisionTx CoordDecisionKind = "tx"
	// CoordDecisionMerge is a single-DB TimelineManager merge.
	CoordDecisionMerge CoordDecisionKind = "merge"
)

// CoordDecisionStatus is what Decide reports about an opID.
type CoordDecisionStatus int

const (
	// CoordDecisionNew means Decide just published a fresh
	// decision for this opID. Proceed to apply, then Complete.
	CoordDecisionNew CoordDecisionStatus = iota
	// CoordDecisionExisting means the same opID with the same
	// payload was already decided but not completed (a
	// crash-interrupted attempt). Proceed to apply (roll
	// forward), then Complete. Do NOT treat this as an
	// error and do NOT use a fresh opID.
	CoordDecisionExisting
	// CoordDecisionCompleted means the same opID with the same
	// payload already applied and completed. Return success
	// WITHOUT applying anything.
	CoordDecisionCompleted
)

// coordDeciderShard is the single shard label used for
// single-DB (Tx / merge) decisions. The shard label is NOT the
// DB identity — recovery filters by the annotation's DB dir,
// never by this label alone — so two DBs sharing a coordinator
// directory cannot be confused: their ops carry different DB
// dirs even though both use this label.
const coordDeciderShard = "db"

// CoordDecider is the single commit-decision abstraction. A
// caller constructs exactly one (local or Raft) and presents it
// explicitly with every coordinated commit; there is no ambient
// decider, no global, and no tokenless path on a fenced DB.
// All methods are safe for concurrent use by goroutines in the
// owning process (serialised per coordinator directory); they
// must not be called while holding a session mutex (see the file
// header for the lock order).
type CoordDecider interface {
	// CoordDir returns the coordinator directory this decider
	// decides in (local session dir, or Raft node dir).
	CoordDir() string
	// Validate is the early liveness/fencing check (local:
	// session owner + epoch; Raft: current-leader belief).
	// It is an early refusal only; the authoritative check
	// is inside Decide.
	Validate() error
	// Decide durably publishes the decision for opID, or
	// reports its existing state (see CoordDecisionStatus).
	// kind, dbDir and mergeID annotate the decision (mergeID
	// is "" for Tx ops and is a per-attempt staging ID for
	// merges — it is NOT part of op identity, so a retry
	// with a fresh staging ID still matches). writes is the
	// full write set; for single-DB ops it has exactly the
	// coordDeciderShard entry, but Decide accepts any map
	// whose shard names are valid coordinator names.
	Decide(opID string, kind CoordDecisionKind, dbDir, mergeID string, writes map[string][]globalMergeDelta) (CoordDecisionStatus, error)
	// Complete records completion AFTER a successful apply,
	// then deletes the decision (MarkDone / MarkRaftDone)
	// and the local annotation. It is idempotent: calling
	// it twice for the same opID + payload converges.
	Complete(opID string, kind CoordDecisionKind, dbDir, mergeID string, writes map[string][]globalMergeDelta) error
	// DiscardUndecided reclaims a staged-but-undecided
	// operation (definite no-decision only). If a decision
	// is durable for opID it returns an error wrapping
	// ErrCoordOpCommitted and reclaims NOTHING — the
	// caller must then report committed-but-incomplete
	// and run recovery, never a clean failure.
	// DiscardUndecided is PAYLOAD-BLIND (it takes only
	// an opID): callers must NEVER call it after a
	// Decide failure that wraps ErrCoordOpExists — that
	// refusal already means the opID belongs to a
	// different operation, and discarding would
	// misreport a clean refusal as decision-stands.
	DiscardUndecided(opID string) error
	// IsCompleted reports whether a completion record
	// exists for opID (regardless of payload — callers
	// that need payload matching use Decide/Complete).
	IsCompleted(opID string) (bool, error)
	// CommittedOps returns every decided-but-not-cleaned
	// operation in this decider's directory that carries
	// a Tx/merge annotation, sorted by opID, with kind,
	// DB dir, merge ID and fingerprint populated. As a
	// side effect on the local path it reclaims
	// staged-only operations (via RecoverMultiShard) and
	// sweeps orphan annotation files (see the local
	// implementation). Legacy/domain operations (no
	// annotation, empty kind) are SKIPPED, not failed:
	// they belong to domain recovery. A corrupt
	// annotation or fingerprint mismatch on a Tx/merge
	// op fails the whole call loudly and applies
	// nothing — the caller must not proceed past it.
	CommittedOps() ([]CoordMultiShardOp, error)
}

// coordDecisionFingerprint is the write-set identity: SHA-256
// (hex) over the canonical JSON of the writes map (encoding/json
// sorts map keys, and Tx/merge callers pass deterministically
// ordered delta slices, so the same write set always hashes the
// same, across processes and across JSON round-trips).
func coordDecisionFingerprint(writes map[string][]globalMergeDelta) string {
	body, _ := json.Marshal(struct {
		Writes map[string][]globalMergeDelta `json:"writes"`
	}{Writes: writes})
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

// coordDecisionMeta is the local annotation file payload.
type coordDecisionMeta struct {
	OpID        string            `json:"op_id"`
	Kind        CoordDecisionKind `json:"kind"`
	DBDir       string            `json:"db_dir"`
	MergeID     string            `json:"merge_id,omitempty"`
	Fingerprint string            `json:"fingerprint"`
	Checksum    uint32            `json:"checksum"`
}

func coordDecisionMetaChecksum(m *coordDecisionMeta) uint32 {
	body, _ := json.Marshal(struct {
		OpID        string            `json:"op_id"`
		Kind        CoordDecisionKind `json:"kind"`
		DBDir       string            `json:"db_dir"`
		MergeID     string            `json:"merge_id,omitempty"`
		Fingerprint string            `json:"fingerprint"`
	}{OpID: m.OpID, Kind: m.Kind, DBDir: m.DBDir, MergeID: m.MergeID, Fingerprint: m.Fingerprint})
	return crc32.ChecksumIEEE(body)
}

func coordDecisionMetaEncode(m *coordDecisionMeta) ([]byte, error) {
	m.Checksum = coordDecisionMetaChecksum(m)
	return json.Marshal(m)
}

func coordDecisionMetaDecode(data []byte) (*coordDecisionMeta, error) {
	var m coordDecisionMeta
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("%w: decision annotation unparseable: %v", ErrCoordCorrupt, err)
	}
	if m.OpID == "" || (m.Kind != CoordDecisionTx && m.Kind != CoordDecisionMerge) || m.DBDir == "" || m.Fingerprint == "" {
		return nil, fmt.Errorf("%w: decision annotation fields invalid", ErrCoordCorrupt)
	}
	if coordDecisionMetaChecksum(&m) != m.Checksum {
		return nil, fmt.Errorf("%w: decision annotation checksum mismatch", ErrCoordCorrupt)
	}
	return &m, nil
}

// coordDecisionCompleted is the completion-record payload.
type coordDecisionCompleted struct {
	OpID        string            `json:"op_id"`
	Kind        CoordDecisionKind `json:"kind"`
	DBDir       string            `json:"db_dir"`
	MergeID     string            `json:"merge_id,omitempty"`
	Fingerprint string            `json:"fingerprint"`
	Checksum    uint32            `json:"checksum"`
}

func coordDecisionCompletedChecksum(c *coordDecisionCompleted) uint32 {
	body, _ := json.Marshal(struct {
		OpID        string            `json:"op_id"`
		Kind        CoordDecisionKind `json:"kind"`
		DBDir       string            `json:"db_dir"`
		MergeID     string            `json:"merge_id,omitempty"`
		Fingerprint string            `json:"fingerprint"`
	}{OpID: c.OpID, Kind: c.Kind, DBDir: c.DBDir, MergeID: c.MergeID, Fingerprint: c.Fingerprint})
	return crc32.ChecksumIEEE(body)
}

func coordDecisionCompletedEncode(c *coordDecisionCompleted) ([]byte, error) {
	c.Checksum = coordDecisionCompletedChecksum(c)
	return json.Marshal(c)
}

func coordDecisionCompletedDecode(data []byte) (*coordDecisionCompleted, error) {
	var c coordDecisionCompleted
	if err := json.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("%w: decision completion record unparseable: %v", ErrCoordCorrupt, err)
	}
	if c.OpID == "" || (c.Kind != CoordDecisionTx && c.Kind != CoordDecisionMerge) || c.DBDir == "" || c.Fingerprint == "" {
		return nil, fmt.Errorf("%w: decision completion record fields invalid", ErrCoordCorrupt)
	}
	if coordDecisionCompletedChecksum(&c) != c.Checksum {
		return nil, fmt.Errorf("%w: decision completion record checksum mismatch", ErrCoordCorrupt)
	}
	return &c, nil
}

func coordDecisionMetaPath(dir, opID string) string {
	return filepath.Join(dir, "coord-decision-"+opID+".json")
}

func coordDecisionCompletedPath(dir, opID string) string {
	return filepath.Join(dir, "coord-completed-"+opID+".json")
}

// Per-directory mutexes for decider operations (see the file
// header for lock order: directory mutex before session mutex,
// never the reverse).
var (
	coordDecisionDirMu    sync.Mutex
	coordDecisionDirLocks = map[string]*sync.Mutex{}
)

func coordDecisionDirLock(dir string) func() {
	coordDecisionDirMu.Lock()
	mu, ok := coordDecisionDirLocks[dir]
	if !ok {
		mu = &sync.Mutex{}
		coordDecisionDirLocks[dir] = mu
	}
	coordDecisionDirMu.Unlock()
	mu.Lock()
	return mu.Unlock
}

// coordDecisionReadMeta returns nil, nil when no annotation
// file exists (legacy/domain op), and a loud error when one
// exists but fails validation.
func coordDecisionReadMeta(dir, opID string) (*coordDecisionMeta, error) {
	data, err := os.ReadFile(coordDecisionMetaPath(dir, opID))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	return coordDecisionMetaDecode(data)
}

func coordDecisionReadCompleted(dir, opID string) (*coordDecisionCompleted, error) {
	data, err := os.ReadFile(coordDecisionCompletedPath(dir, opID))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	return coordDecisionCompletedDecode(data)
}

// coordDecisionMetaMatches reports whether an annotation
// describes the same operation as the caller's parameters.
// MergeID is deliberately NOT compared: it is a per-attempt
// staging ID, and a retry stages under a fresh one.
func coordDecisionMetaMatches(m *coordDecisionMeta, kind CoordDecisionKind, dbDir, fingerprint string) bool {
	return m != nil && m.Kind == kind && m.DBDir == dbDir && m.Fingerprint == fingerprint
}

func coordDecisionCompletedMatches(c *coordDecisionCompleted, kind CoordDecisionKind, dbDir, fingerprint string) bool {
	return c != nil && c.Kind == kind && c.DBDir == dbDir && c.Fingerprint == fingerprint
}

// coordDecisionWriteMeta creates the annotation with O_EXCL.
// If one already exists with identical identity fields it is
// left alone (concurrent same-op retry) and created=false is
// returned; if one exists with different identity fields the
// opID is already taken and ErrCoordOpExists is returned, and
// the existing file is never overwritten.
func coordDecisionWriteMeta(dir, opID string, meta *coordDecisionMeta) (created bool, err error) {
	data, err := coordDecisionMetaEncode(meta)
	if err != nil {
		return false, err
	}
	f, err := os.OpenFile(coordDecisionMetaPath(dir, opID), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		if !os.IsExist(err) {
			return false, err
		}
		existing, rerr := coordDecisionReadMeta(dir, opID)
		if rerr != nil {
			return false, rerr
		}
		if existing == nil {
			return false, fmt.Errorf("%w: op %s annotation disappeared", ErrCoordCorrupt, opID)
		}
		if !coordDecisionMetaMatches(existing, meta.Kind, meta.DBDir, meta.Fingerprint) {
			return false, fmt.Errorf("%w: op %s already decided for a different operation", ErrCoordOpExists, opID)
		}
		return false, nil
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return false, err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return false, err
	}
	if err := f.Close(); err != nil {
		return false, err
	}
	if err := persistSyncDir(dir); err != nil {
		return true, err
	}
	return true, nil
}

// coordDecisionWriteCompleted writes (or overwrites, with the
// same identity) the completion record atomically. It refuses
// to overwrite a completion record for a DIFFERENT operation
// under the same opID.
func coordDecisionWriteCompleted(dir, opID string, rec *coordDecisionCompleted) error {
	existing, err := coordDecisionReadCompleted(dir, opID)
	if err != nil {
		return err
	}
	if existing != nil && !coordDecisionCompletedMatches(existing, rec.Kind, rec.DBDir, rec.Fingerprint) {
		return fmt.Errorf("%w: op %s already completed for a different operation", ErrCoordOpExists, opID)
	}
	data, err := coordDecisionCompletedEncode(rec)
	if err != nil {
		return err
	}
	return coordWriteAtomic(dir, coordDecisionCompletedPath(dir, opID), data)
}

// coordDecisionLoadStaged loads and fingerprints the staged
// payload for opID (manifest + every shard). It is the
// Decide-time proof that a pre-existing root/staging belongs
// to this exact write set, not merely to this opID.
func coordDecisionLoadStaged(dir, opID string) (map[string][]globalMergeDelta, string, error) {
	man, err := coordLoadManifest(dir, opID)
	if err != nil {
		return nil, "", err
	}
	writes := map[string][]globalMergeDelta{}
	for _, sh := range man.Shards {
		deltas, err := coordLoadShard(dir, opID, sh)
		if err != nil {
			return nil, "", err
		}
		writes[sh] = deltas
	}
	return writes, coordDecisionFingerprint(writes), nil
}

func coordDecisionValidateArgs(opID string, kind CoordDecisionKind, dbDir string, writes map[string][]globalMergeDelta) error {
	if !coordValidName(opID) {
		return fmt.Errorf("yowza coordinator: invalid operation id %q", opID)
	}
	if kind != CoordDecisionTx && kind != CoordDecisionMerge {
		return fmt.Errorf("yowza coordinator: invalid decision kind %q", kind)
	}
	if dbDir == "" {
		return fmt.Errorf("yowza coordinator: empty database dir for op %s", opID)
	}
	if len(writes) == 0 {
		return fmt.Errorf("yowza coordinator: empty write set for op %s", opID)
	}
	for sh := range writes {
		if !coordValidName(sh) {
			return fmt.Errorf("yowza coordinator: invalid shard id %q", sh)
		}
	}
	return nil
}

// --- Local decider ---

// LocalCoordDecider decides through the local coordinator
// (flock + epoch + root protocol). The session must already
// be acquired; the constructor refuses a coordDir that does
// not match the session's dir (the same rule the coordinated
// domain path enforces — a decision must land in the
// directory its session owns).
type LocalCoordDecider struct {
	sess *CoordinatorSession
	dir  string
}

// NewLocalCoordDecider builds a local decider for an acquired
// session. A nil session is refused (a nil session can never
// fence anything, so a decider built on one would fail open).
func NewLocalCoordDecider(sess *CoordinatorSession, coordDir string) (*LocalCoordDecider, error) {
	if sess == nil {
		return nil, fmt.Errorf("%w: nil session", ErrCoordReleased)
	}
	if coordDir != sess.Dir() {
		return nil, fmt.Errorf("yowza coordinator: coord dir %q does not match session dir %q", coordDir, sess.Dir())
	}
	return &LocalCoordDecider{sess: sess, dir: coordDir}, nil
}

// CoordDir returns the coordinator directory.
func (d *LocalCoordDecider) CoordDir() string { return d.dir }

// Validate is the session's fencing check.
func (d *LocalCoordDecider) Validate() error {
	if d == nil || d.sess == nil {
		return fmt.Errorf("%w: nil decider session", ErrCoordReleased)
	}
	return d.sess.Validate()
}

// Decide implements CoordDecider for the local path. Order:
// completed record -> existing root -> existing staging ->
// fresh stage + publish. A PublishRoot failure is NOT
// automatically a no-decision: if the root file in fact
// landed (rename done, dir-sync failed), the decision
// stands and Existing is returned so the caller proceeds
// to apply; only a failure with no root present is
// returned as an error (and the caller reclaims via
// DiscardUndecided).
func (d *LocalCoordDecider) Decide(opID string, kind CoordDecisionKind, dbDir, mergeID string, writes map[string][]globalMergeDelta) (CoordDecisionStatus, error) {
	if d == nil || d.sess == nil {
		return CoordDecisionNew, fmt.Errorf("%w: nil decider session", ErrCoordReleased)
	}
	if err := coordDecisionValidateArgs(opID, kind, dbDir, writes); err != nil {
		return CoordDecisionNew, err
	}
	fingerprint := coordDecisionFingerprint(writes)
	releaseDir := coordDecisionDirLock(d.dir)
	defer releaseDir()
	if err := d.sess.Validate(); err != nil {
		return CoordDecisionNew, err
	}
	// Completed first: a completed op never re-decides,
	// even if its root somehow still exists.
	completed, err := coordDecisionReadCompleted(d.dir, opID)
	if err != nil {
		return CoordDecisionNew, err
	}
	if completed != nil {
		if !coordDecisionCompletedMatches(completed, kind, dbDir, fingerprint) {
			return CoordDecisionNew, fmt.Errorf("%w: op %s already completed for a different operation", ErrCoordOpExists, opID)
		}
		return CoordDecisionCompleted, nil
	}
	// Existing root: the decision already stands. Prove
	// it belongs to this exact operation before
	// reporting Existing.
	if _, err := os.Stat(coordRootPath(d.dir, opID)); err == nil {
		meta, err := coordDecisionReadMeta(d.dir, opID)
		if err != nil {
			return CoordDecisionNew, err
		}
		if meta == nil {
			return CoordDecisionNew, fmt.Errorf("%w: op %s already has a root for a different (legacy/domain) operation", ErrCoordOpExists, opID)
		}
		if !coordDecisionMetaMatches(meta, kind, dbDir, fingerprint) {
			return CoordDecisionNew, fmt.Errorf("%w: op %s already decided for a different operation", ErrCoordOpExists, opID)
		}
		if _, stagedFP, err := coordDecisionLoadStaged(d.dir, opID); err != nil {
			return CoordDecisionNew, err
		} else if stagedFP != fingerprint {
			return CoordDecisionNew, fmt.Errorf("%w: op %s staged payload differs from decided payload", ErrCoordOpExists, opID)
		}
		return CoordDecisionExisting, nil
	}
	// Staged but undecided (crash between a previous
	// Decide's staging and its root publish, or a
	// Prepare left by a crashed caller): if the staging
	// is ours, skip re-staging and publish the root
	// directly; otherwise the opID is taken.
	skipPrepare := false
	if _, err := os.Stat(coordStageManifestPath(d.dir, opID)); err == nil {
		meta, err := coordDecisionReadMeta(d.dir, opID)
		if err != nil {
			return CoordDecisionNew, err
		}
		if meta == nil {
			return CoordDecisionNew, fmt.Errorf("%w: op %s already staged for a different (legacy/domain) operation", ErrCoordOpExists, opID)
		}
		if !coordDecisionMetaMatches(meta, kind, dbDir, fingerprint) {
			return CoordDecisionNew, fmt.Errorf("%w: op %s already staged for a different operation", ErrCoordOpExists, opID)
		}
		if _, stagedFP, err := coordDecisionLoadStaged(d.dir, opID); err != nil {
			return CoordDecisionNew, err
		} else if stagedFP != fingerprint {
			return CoordDecisionNew, fmt.Errorf("%w: op %s staged payload differs", ErrCoordOpExists, opID)
		}
		skipPrepare = true
	}
	meta := &coordDecisionMeta{OpID: opID, Kind: kind, DBDir: dbDir, MergeID: mergeID, Fingerprint: fingerprint}
	metaCreated, err := coordDecisionWriteMeta(d.dir, opID, meta)
	if err != nil {
		return CoordDecisionNew, err
	}
	if !skipPrepare {
		if err := PrepareMultiShard(d.sess, d.dir, opID, writes); err != nil {
			// Definite no-decision cleanup, only for
			// a meta file this call created and only
			// while no root/manifest appeared (an
			// external writer's files are never
			// deleted here — DiscardUndecided and
			// recovery own those cases).
			if metaCreated {
				if _, rerr := os.Stat(coordRootPath(d.dir, opID)); os.IsNotExist(rerr) {
					if _, merr := os.Stat(coordStageManifestPath(d.dir, opID)); os.IsNotExist(merr) {
						_ = os.Remove(coordDecisionMetaPath(d.dir, opID))
					}
				}
			}
			return CoordDecisionNew, err
		}
	}
	if err := PublishRoot(d.sess, d.dir, opID); err != nil {
		// The root may have landed despite the error
		// (rename done, dir-sync failed). That IS a
		// decision: report Existing so the caller
		// applies rather than silently dropping a
		// decided operation. Anything else is a true
		// no-decision error for DiscardUndecided.
		if _, rerr := os.Stat(coordRootPath(d.dir, opID)); rerr == nil {
			return CoordDecisionExisting, nil
		}
		return CoordDecisionNew, err
	}
	return CoordDecisionNew, nil
}

// Complete implements CoordDecider for the local path:
// completion record first (so a crash after it still
// counts as completed), then MarkDone, then annotation
// deletion. A MarkDone failure is reported (the caller
// wraps it as cleanup-pending); the completion record
// already durable means a retry converges.
func (d *LocalCoordDecider) Complete(opID string, kind CoordDecisionKind, dbDir, mergeID string, writes map[string][]globalMergeDelta) error {
	if d == nil || d.sess == nil {
		return fmt.Errorf("%w: nil decider session", ErrCoordReleased)
	}
	if err := coordDecisionValidateArgs(opID, kind, dbDir, writes); err != nil {
		return err
	}
	releaseDir := coordDecisionDirLock(d.dir)
	defer releaseDir()
	if err := coordDecisionWriteCompleted(d.dir, opID, &coordDecisionCompleted{OpID: opID, Kind: kind, DBDir: dbDir, MergeID: mergeID, Fingerprint: coordDecisionFingerprint(writes)}); err != nil {
		return err
	}
	if err := MarkDone(d.dir, opID); err != nil {
		return err
	}
	_ = os.Remove(coordDecisionMetaPath(d.dir, opID))
	return nil
}

// DiscardUndecided implements CoordDecider for the local
// path: a durable decision (root, or completion record)
// refuses discarding; otherwise the staging and the
// annotation are reclaimed.
func (d *LocalCoordDecider) DiscardUndecided(opID string) error {
	if d == nil {
		return fmt.Errorf("%w: nil decider", ErrCoordReleased)
	}
	if !coordValidName(opID) {
		return fmt.Errorf("yowza coordinator: invalid operation id %q", opID)
	}
	releaseDir := coordDecisionDirLock(d.dir)
	defer releaseDir()
	if completed, err := coordDecisionReadCompleted(d.dir, opID); err != nil {
		return err
	} else if completed != nil {
		return fmt.Errorf("%w: op %s already completed, run recovery", ErrCoordOpCommitted, opID)
	}
	if _, err := os.Stat(coordRootPath(d.dir, opID)); err == nil {
		return fmt.Errorf("%w: op %s already decided, run recovery", ErrCoordOpCommitted, opID)
	}
	if err := AbortMultiShard(d.dir, opID); err != nil {
		return err
	}
	_ = os.Remove(coordDecisionMetaPath(d.dir, opID))
	return nil
}

// IsCompleted implements CoordDecider for the local path.
func (d *LocalCoordDecider) IsCompleted(opID string) (bool, error) {
	if d == nil {
		return false, fmt.Errorf("%w: nil decider", ErrCoordReleased)
	}
	if !coordValidName(opID) {
		return false, fmt.Errorf("yowza coordinator: invalid operation id %q", opID)
	}
	releaseDir := coordDecisionDirLock(d.dir)
	defer releaseDir()
	completed, err := coordDecisionReadCompleted(d.dir, opID)
	if err != nil {
		return false, err
	}
	return completed != nil, nil
}

// CommittedOps implements CoordDecider for the local path.
// RecoverMultiShard reclaims staged-only operations as a
// side effect (that is its documented behaviour, and it is
// the correct no-decision outcome for decider stagings
// too); the sweep below then deletes annotations whose
// operation can no longer be decided (completed, so the
// annotation is redundant) or never was (no root, so the
// annotation is stale staging metadata). Annotations for
// root-present, uncompleted ops are KEPT: they are live
// decisions in flight.
func (d *LocalCoordDecider) CommittedOps() ([]CoordMultiShardOp, error) {
	if d == nil {
		return nil, fmt.Errorf("%w: nil decider", ErrCoordReleased)
	}
	releaseDir := coordDecisionDirLock(d.dir)
	defer releaseDir()
	committed, err := RecoverMultiShard(d.dir)
	if err != nil {
		return nil, err
	}
	var out []CoordMultiShardOp
	for _, op := range committed {
		if op.Kind != CoordDecisionTx && op.Kind != CoordDecisionMerge {
			// Legacy/domain operation: not ours, and
			// domain recovery owns it. Skipped, not
			// failed — failing here would make
			// decider recovery unusable on any
			// directory that ever held a domain op.
			// (Kind is populated by RecoverMultiShard
			// from the annotation file; absent
			// annotation means legacy/domain.)
			continue
		}
		if coordDecisionFingerprint(op.Writes) != op.Fingerprint {
			return nil, fmt.Errorf("%w: committed op %s fingerprint differs from its annotation", ErrCoordCorrupt, op.OpID)
		}
		out = append(out, op)
	}
	// Sweep orphan annotations (best effort: a sweep
	// failure leaves a stale annotation file, which
	// the next recovery sweeps again — never a wrong
	// apply, since CommittedOps only returns ops
	// with a live root).
	if ents, err := os.ReadDir(d.dir); err == nil {
		for _, e := range ents {
			name := e.Name()
			if !strings.HasPrefix(name, "coord-decision-") || !strings.HasSuffix(name, ".json") {
				continue
			}
			opID := strings.TrimSuffix(strings.TrimPrefix(name, "coord-decision-"), ".json")
			if _, err := os.Stat(coordDecisionCompletedPath(d.dir, opID)); err == nil {
				_ = os.Remove(coordDecisionMetaPath(d.dir, opID))
				continue
			}
			if _, err := os.Stat(coordRootPath(d.dir, opID)); os.IsNotExist(err) {
				_ = os.Remove(coordDecisionMetaPath(d.dir, opID))
			}
		}
	}
	return out, nil
}

// --- Raft decider ---

// RaftCoordDecider decides through a Raft cluster. The
// session must already be built (and, for proposes to
// succeed, started with a stable leader — see the Raft
// tests' waitLeader pattern). The node's directory is
// the coordinator directory for completion records.
type RaftCoordDecider struct {
	sess *RaftCoordinatorSession
}

// NewRaftCoordDecider builds a Raft decider for a Raft
// coordinator session. A nil session is refused.
func NewRaftCoordDecider(sess *RaftCoordinatorSession) (*RaftCoordDecider, error) {
	if sess == nil || sess.Node() == nil {
		return nil, fmt.Errorf("%w: nil raft session", ErrCoordReleased)
	}
	return &RaftCoordDecider{sess: sess}, nil
}

// CoordDir returns the Raft node's directory.
func (d *RaftCoordDecider) CoordDir() string { return d.sess.Dir() }

// Validate is the Raft session's leader-belief check
// (early only; the authoritative check is the propose
// inside Decide).
func (d *RaftCoordDecider) Validate() error {
	if d == nil || d.sess == nil {
		return fmt.Errorf("%w: nil raft decider session", ErrCoordReleased)
	}
	return d.sess.Validate()
}

// Decide implements CoordDecider for the Raft path.
// Completion record first, then the live machine:
// a committed entry with a matching payload is
// Existing (or Completed, if the record exists);
// a committed entry with a different payload is
// ErrCoordOpExists (first-committed-wins in the
// machine — the loser never applies its payload).
// Otherwise the decision is proposed (majority
// commit), local apply is awaited, and the committed
// payload is re-verified before New is returned, so
// a concurrent same-opID proposal that committed
// first cannot make this caller apply a payload it
// did not propose.
func (d *RaftCoordDecider) Decide(opID string, kind CoordDecisionKind, dbDir, mergeID string, writes map[string][]globalMergeDelta) (CoordDecisionStatus, error) {
	if d == nil || d.sess == nil {
		return CoordDecisionNew, fmt.Errorf("%w: nil raft decider session", ErrCoordReleased)
	}
	if err := coordDecisionValidateArgs(opID, kind, dbDir, writes); err != nil {
		return CoordDecisionNew, err
	}
	fingerprint := coordDecisionFingerprint(writes)
	releaseDir := coordDecisionDirLock(d.sess.Dir())
	defer releaseDir()
	if err := d.sess.Validate(); err != nil {
		return CoordDecisionNew, err
	}
	completed, err := coordDecisionReadCompleted(d.sess.Dir(), opID)
	if err != nil {
		return CoordDecisionNew, err
	}
	if completed != nil {
		if !coordDecisionCompletedMatches(completed, kind, dbDir, fingerprint) {
			return CoordDecisionNew, fmt.Errorf("%w: op %s already completed for a different operation", ErrCoordOpExists, opID)
		}
		return CoordDecisionCompleted, nil
	}
	if op, ok := d.sess.Machine().CommittedOp(opID); ok {
		if op.Kind != kind || op.DBDir != dbDir || op.Fingerprint != fingerprint {
			return CoordDecisionNew, fmt.Errorf("%w: op %s already decided for a different operation", ErrCoordOpExists, opID)
		}
		return CoordDecisionExisting, nil
	}
	owner := d.sess.Node().ID()
	if err := d.sess.ProposeCoordDecision(opID, owner, kind, dbDir, mergeID, writes); err != nil {
		return CoordDecisionNew, err
	}
	// The entry is majority-committed, but local
	// apply is async: wait for it, so the committed
	// record is visible before the caller applies.
	// A timeout here is indeterminate (the entry may
	// still commit/apply): the caller must retry the
	// SAME opID, which will then find it committed.
	if err := d.sess.waitApplied(5 * time.Second); err != nil {
		return CoordDecisionNew, err
	}
	op, ok := d.sess.Machine().CommittedOp(opID)
	if !ok {
		return CoordDecisionNew, fmt.Errorf("yowza raft: op %s committed but not present in machine after apply wait", opID)
	}
	if op.Kind != kind || op.DBDir != dbDir || op.Fingerprint != fingerprint {
		return CoordDecisionNew, fmt.Errorf("%w: op %s committed for a different operation (concurrent proposal won)", ErrCoordOpExists, opID)
	}
	return CoordDecisionNew, nil
}

// Complete implements CoordDecider for the Raft path:
// completion record first, then MarkRaftDone (which
// deletes only the local projection — a restart
// re-derives it from the log, and the completion
// record is what keeps that re-derivation from
// becoming a re-apply).
func (d *RaftCoordDecider) Complete(opID string, kind CoordDecisionKind, dbDir, mergeID string, writes map[string][]globalMergeDelta) error {
	if d == nil || d.sess == nil {
		return fmt.Errorf("%w: nil raft decider session", ErrCoordReleased)
	}
	if err := coordDecisionValidateArgs(opID, kind, dbDir, writes); err != nil {
		return err
	}
	releaseDir := coordDecisionDirLock(d.sess.Dir())
	defer releaseDir()
	if err := coordDecisionWriteCompleted(d.sess.Dir(), opID, &coordDecisionCompleted{OpID: opID, Kind: kind, DBDir: dbDir, MergeID: mergeID, Fingerprint: coordDecisionFingerprint(writes)}); err != nil {
		return err
	}
	return MarkRaftDone(d.sess.Dir(), opID)
}

// DiscardUndecided implements CoordDecider for the Raft
// path: there is no staging to reclaim (the Raft entry
// IS the payload delivery), so this only refuses when
// a decision or completion already exists.
func (d *RaftCoordDecider) DiscardUndecided(opID string) error {
	if d == nil {
		return fmt.Errorf("%w: nil decider", ErrCoordReleased)
	}
	if !coordValidName(opID) {
		return fmt.Errorf("yowza coordinator: invalid operation id %q", opID)
	}
	releaseDir := coordDecisionDirLock(d.sess.Dir())
	defer releaseDir()
	if completed, err := coordDecisionReadCompleted(d.sess.Dir(), opID); err != nil {
		return err
	} else if completed != nil {
		return fmt.Errorf("%w: op %s already completed, run recovery", ErrCoordOpCommitted, opID)
	}
	if _, ok := d.sess.Machine().CommittedOp(opID); ok {
		return fmt.Errorf("%w: op %s already decided, run recovery", ErrCoordOpCommitted, opID)
	}
	return nil
}

// IsCompleted implements CoordDecider for the Raft path.
func (d *RaftCoordDecider) IsCompleted(opID string) (bool, error) {
	if d == nil {
		return false, fmt.Errorf("%w: nil decider", ErrCoordReleased)
	}
	if !coordValidName(opID) {
		return false, fmt.Errorf("yowza coordinator: invalid operation id %q", opID)
	}
	releaseDir := coordDecisionDirLock(d.sess.Dir())
	defer releaseDir()
	completed, err := coordDecisionReadCompleted(d.sess.Dir(), opID)
	if err != nil {
		return false, err
	}
	return completed != nil, nil
}

// CommittedOps implements CoordDecider for the Raft path.
// It reads the live machine (authoritative, survives
// MarkRaftDone), not the projection files, and returns
// only Tx/merge ops with a verified fingerprint. Legacy
// domain ops (empty kind) are skipped, exactly as on
// the local path.
func (d *RaftCoordDecider) CommittedOps() ([]CoordMultiShardOp, error) {
	if d == nil {
		return nil, fmt.Errorf("%w: nil decider", ErrCoordReleased)
	}
	releaseDir := coordDecisionDirLock(d.sess.Dir())
	defer releaseDir()
	var out []CoordMultiShardOp
	for _, op := range d.sess.Machine().CommittedOps() {
		if op.Kind != CoordDecisionTx && op.Kind != CoordDecisionMerge {
			continue
		}
		if op.Fingerprint == "" {
			return nil, fmt.Errorf("%w: committed raft op %s carries a decision kind but no fingerprint", ErrCoordCorrupt, op.OpID)
		}
		if coordDecisionFingerprint(op.Writes) != op.Fingerprint {
			return nil, fmt.Errorf("%w: committed raft op %s fingerprint differs from its payload", ErrCoordCorrupt, op.OpID)
		}
		out = append(out, CoordMultiShardOp{
			OpID: op.OpID, Writes: op.Writes, Kind: op.Kind,
			DBDir: op.DBDir, MergeID: op.MergeID, Fingerprint: op.Fingerprint,
		})
	}
	return out, nil
}

// coordFenceDir returns the coordinator directory a DB
// is fenced under, when its registered validator
// exposes one (local and Raft sessions do; test fakes
// may not, in which case the dir check is skipped —
// the decider's own validation still applies). The
// second return is false when the DB is unfenced or
// its validator has no directory.
func coordFenceDir(db *PersistedSegDB) (string, bool) {
	v := coordFenceFor(db)
	if v == nil {
		return "", false
	}
	if d, ok := v.(interface{ Dir() string }); ok {
		return d.Dir(), true
	}
	return "", false
}

// --- Authorized recovery (specific and limited) ---
//
// These two functions are the ONLY recovery entry points
// for coordinated Tx/merge operations. They complete
// exactly the operations CommittedOps returns (durable
// decision, Tx/merge kind, this DB's dir) and nothing
// else: staged-only operations were already reclaimed
// by CommittedOps (local path) or never existed (Raft
// path), legacy/domain operations are skipped by
// CommittedOps, and no flag, mode, or caller choice
// can make them apply anything else. They must not run
// concurrently with an active commit on the same DB
// (the same rule as every other recovery function in
// this tree). Each operation's apply runs under db.mu;
// applies are idempotent puts/deletes, so running
// these recoveries in either order, or twice, or
// alongside RecoverSegDBTransactions /
// RecoverGlobalMerges, still ends in the same full
// state — never a prefix, never a double effect on
// values (a merge re-apply advances snapshot
// sequences, exactly as global-merge recovery's
// re-apply does — values are identical).

// RecoverCoordinatedTx rolls forward every decided,
// uncompleted Tx operation for db, then completes
// each one. It returns the opIDs it applied (not
// those it merely cleaned up as already completed),
// sorted. A completed operation is cleaned up
// without re-applying.
func RecoverCoordinatedTx(db *PersistedSegDB, dec CoordDecider) ([]string, error) {
	if db == nil {
		return nil, fmt.Errorf("%w: nil database", ErrTimelineInvalid)
	}
	if dec == nil {
		return nil, fmt.Errorf("%w: nil coordinator decider", ErrCoordReleased)
	}
	ops, err := dec.CommittedOps()
	if err != nil {
		return nil, err
	}
	var rolled []string
	for _, op := range ops {
		if op.Kind != CoordDecisionTx || op.DBDir != db.Dir() {
			continue
		}
		deltas, ok := op.Writes[coordDeciderShard]
		if !ok || len(op.Writes) != 1 {
			return rolled, fmt.Errorf("%w: coordinated tx op %s has unexpected shard set", ErrCoordCorrupt, op.OpID)
		}
		completed, err := dec.IsCompleted(op.OpID)
		if err != nil {
			return rolled, err
		}
		if completed {
			if err := dec.Complete(op.OpID, CoordDecisionTx, db.Dir(), op.MergeID, op.Writes); err != nil {
				return rolled, fmt.Errorf("%w: coordinated tx op %s cleanup failed: %v", ErrCoordCleanupPending, op.OpID, err)
			}
			continue
		}
		writes := make([]*SegDBTxWrite, 0, len(deltas))
		for _, dl := range deltas {
			writes = append(writes, &SegDBTxWrite{Key: dl.Key, Deleted: dl.Deleted, Value: append([]byte(nil), dl.Value...)})
		}
		db.mu.Lock()
		if db.closed {
			db.mu.Unlock()
			return rolled, ErrPersistedClosed
		}
		if werr := db.checkWritableLocked(); werr != nil {
			db.mu.Unlock()
			return rolled, werr
		}
		_, aerr := applyTxWritesLocked(db, writes)
		db.mu.Unlock()
		if aerr != nil {
			return rolled, fmt.Errorf("%w: roll-forward of coordinated tx op %s failed: %v", ErrDomainOpCommitted, op.OpID, aerr)
		}
		rolled = append(rolled, op.OpID)
		if err := dec.Complete(op.OpID, CoordDecisionTx, db.Dir(), op.MergeID, op.Writes); err != nil {
			return rolled, fmt.Errorf("%w: coordinated tx op %s applied but cleanup failed: %v", ErrCoordCleanupPending, op.OpID, err)
		}
	}
	db.RefreshRecoveryGate()
	return rolled, nil
}

// RecoverCoordinatedMerges rolls forward every decided,
// uncompleted merge operation for db, then completes
// each one. It returns the opIDs it applied, sorted.
// A merge's blob staging (if any) is commit-resolved
// after the apply and its global-merge staging files
// are cleaned up, mirroring what RecoverGlobalMerges
// does for a done merge — so a later global-merge
// recovery finds nothing left to do for this merge.
// The head is NOT re-checked here: the decision was
// published under the fixed-head check at commit
// time, and no writer has run since the crash (the
// same assumption global-merge recovery makes).
func RecoverCoordinatedMerges(db *PersistedSegDB, dec CoordDecider) ([]string, error) {
	if db == nil {
		return nil, fmt.Errorf("%w: nil database", ErrTimelineInvalid)
	}
	if dec == nil {
		return nil, fmt.Errorf("%w: nil coordinator decider", ErrCoordReleased)
	}
	ops, err := dec.CommittedOps()
	if err != nil {
		return nil, err
	}
	var rolled []string
	for _, op := range ops {
		if op.Kind != CoordDecisionMerge || op.DBDir != db.Dir() {
			continue
		}
		deltas, ok := op.Writes[coordDeciderShard]
		if !ok || len(op.Writes) != 1 {
			return rolled, fmt.Errorf("%w: coordinated merge op %s has unexpected shard set", ErrCoordCorrupt, op.OpID)
		}
		completed, err := dec.IsCompleted(op.OpID)
		if err != nil {
			return rolled, err
		}
		if completed {
			if err := dec.Complete(op.OpID, CoordDecisionMerge, db.Dir(), op.MergeID, op.Writes); err != nil {
				return rolled, fmt.Errorf("%w: coordinated merge op %s cleanup failed: %v", ErrCoordCleanupPending, op.OpID, err)
			}
			continue
		}
		db.mu.Lock()
		if db.closed {
			db.mu.Unlock()
			return rolled, ErrPersistedClosed
		}
		if werr := db.checkWritableLocked(); werr != nil {
			db.mu.Unlock()
			return rolled, werr
		}
		aerr := applyDeltasLocked(db, deltas)
		db.mu.Unlock()
		if aerr != nil {
			return rolled, fmt.Errorf("%w: roll-forward of coordinated merge op %s failed: %v", ErrDomainOpCommitted, op.OpID, aerr)
		}
		rolled = append(rolled, op.OpID)
		if op.MergeID != "" {
			// Best effort, mirroring the done-loop
			// in RecoverGlobalMerges: a resolve
			// failure leaves a safe leak (GC keeps
			// the blob), retried on the next
			// recovery — never a dangle, never a
			// reason to report the merge unapplied.
			_ = ResolveMergeBlobs(db.Dir(), op.MergeID, true)
			chunkCount := 0
			manifestPath, _, _ := globalMergeFileNames(db.Dir(), op.MergeID)
			if data, rerr := os.ReadFile(manifestPath); rerr == nil {
				if man, derr := globalMergeDecodeManifest(data); derr == nil {
					chunkCount = len(man.Chunks)
				}
			}
			cleanupGlobalMergeFiles(db.Dir(), op.MergeID, chunkCount)
		}
		if err := dec.Complete(op.OpID, CoordDecisionMerge, db.Dir(), op.MergeID, op.Writes); err != nil {
			return rolled, fmt.Errorf("%w: coordinated merge op %s applied but cleanup failed: %v", ErrCoordCleanupPending, op.OpID, err)
		}
	}
	db.RefreshRecoveryGate()
	return rolled, nil
}
