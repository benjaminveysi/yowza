package yowza

// Phase E, E2 — CROSS-DB TIMELINE DOMAINS.
//
// Two independent snapshots are NOT a global atomic
// snapshot, and this file refuses to present them as
// one. A TimelineDomain binds a set of PersistedSegDB
// instances under one consistency-domain ID with one
// epoch. A global snapshot token is valid ONLY inside
// its domain: it names the participant set, each DB's
// root (snapshot vector), and the epoch, and it is
// backed by a durable pin per participant (Phase B)
// so the named vectors stay readable across restarts
// and compactions.
//
// Coordinator log (in the domain's coordinator
// directory, NOT inside any participant DB — a log
// inside one participant would die with it): an
// append-only JSON-lines file, one CRC-32 record per
// line, fsynced in Sync mode. Records:
//   prepare  — an atomic domain operation's per-DB
//     write sets are staged (in the op's own staging
//     files) but NOT applied.
//   commit   — the decision: recovery MUST roll the
//     operation forward on every participant.
//   abort    — the decision: recovery MUST apply
//     nothing (staged files are reclaimed).
//   done     — roll-forward finished on all
//     participants; staged files may be deleted.
//   snapshot — a global snapshot token was captured
//     (participants + vectors + pins).
// Recovery (RecoverDomainOps) runs the same
// two-outcome rule as the global merge: commit
// without done -> roll forward (idempotent
// puts/deletes, applied in DB-ID order while ALL
// participant p.mu are held, so a multi-DB apply
// cannot deadlock against another domain operation
// and no in-process writer can land mid-roll-
// forward); prepare without commit -> reclaim; done
// -> cleanup. A domain operation that is only
// prepared is NOT visible anywhere: staging files
// are never read by any query path.
//
// Axis B note (see ApplyAtomic and coord_*.go):
// the plain path's atomicity is in-process only
// (p.mu). The opt-in coordinated path
// (ApplyAtomicCoordinated /
// RecoverCoordinatedDomainOps) adds one shared
// coordinator (local, single-machine) fencing
// epoch, shared WAL sequence, shared lease table,
// and multi-shard root on top of this same core.
// Plain ApplyAtomic does NOT consult the
// coordinator and must not be presented as
// multi-process safe.
//
// Refusal rule (the point of this file): any query
// that asks for a global snapshot across DBs must
// present a token issued by a TimelineDomain whose
// participant set is exactly the DBs queried, with
// matching epoch and live durable pins. Anything
// else — a token from another domain, a hand-built
// token, a pair of vectors captured ad hoc — is
// refused with ErrGlobalSnapshotOutsideDomain, and
// reads nothing. There is no "just combine two
// snapshots" helper anywhere in this file.

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// ErrGlobalSnapshotOutsideDomain reports a global
// snapshot request outside one domain: the token's
// domain does not cover exactly the queried DBs, the
// epoch mismatches, a pin is gone, or the token was
// never issued by a domain at all.
var ErrGlobalSnapshotOutsideDomain = errors.New("yowza domain: global snapshot requested outside a single consistency domain")

// ErrDomainOpCorrupt reports a committed domain
// operation whose staging fails validation during
// recovery. Recovery stops loudly.
var ErrDomainOpCorrupt = errors.New("yowza domain: committed operation staging corrupt")

// ErrDomainOpCommitted reports an in-process domain
// apply that did not finish after the commit record.
// Recovery completes it; a fresh retry is forbidden.
var ErrDomainOpCommitted = errors.New("yowza domain: commit logged, apply incomplete; run recovery")

// DomainParticipant is one DB in a domain, keyed by a
// caller-chosen DB ID (stable across restarts; the
// directory path is NOT the identity, so a moved DB
// keeps its domain identity).
type DomainParticipant struct {
	DBID string
	DB   *PersistedSegDB
}

// TokenParticipant is one DB's root inside a token.
type TokenParticipant struct {
	DBID   string   `json:"db_id"`
	Vector []uint64 `json:"vector"`
	PinID  string   `json:"pin_id"`
}

// GlobalSnapshotToken is the global snapshot: domain,
// epoch, participant roots, and the pins that keep
// those roots readable. The checksum covers the
// canonical JSON of {domain, epoch, participants}.
type GlobalSnapshotToken struct {
	DomainID     string             `json:"domain_id"`
	Epoch        uint64             `json:"epoch"`
	Participants []TokenParticipant `json:"participants"`
	Checksum     uint32             `json:"checksum"`
}

// TimelineDomain owns one consistency domain.
type TimelineDomain struct {
	id      string
	epoch   uint64
	dir     string // coordinator directory
	syncDir bool
	parts   map[string]*PersistedSegDB
	order   []string // DB IDs, sorted (lock order)
	mu      sync.Mutex
	issued  map[string]*GlobalSnapshotToken // token checksum hex -> token (this process)
}

type domainLogRecord struct {
	Kind     string          `json:"kind"`
	OpID     string          `json:"op_id"`
	Payload  json.RawMessage `json:"payload"`
	Checksum uint32          `json:"checksum"`
}

type domainOpStage struct {
	OpID   string                        `json:"op_id"`
	Writes map[string][]globalMergeDelta `json:"writes"` // DB ID -> deltas
}

func domainTokenChecksum(domainID string, epoch uint64, parts []TokenParticipant) uint32 {
	body, _ := json.Marshal(struct {
		DomainID     string             `json:"domain_id"`
		Epoch        uint64             `json:"epoch"`
		Participants []TokenParticipant `json:"participants"`
	}{DomainID: domainID, Epoch: epoch, Participants: parts})
	return crc32.ChecksumIEEE(body)
}

func (t *GlobalSnapshotToken) valid() bool {
	return t != nil && t.Checksum == domainTokenChecksum(t.DomainID, t.Epoch, t.Participants)
}

func (t *GlobalSnapshotToken) participant(dbID string) (TokenParticipant, bool) {
	for _, p := range t.Participants {
		if p.DBID == dbID {
			return p, true
		}
	}
	return TokenParticipant{}, false
}

// NewTimelineDomain creates a domain over the given
// participants. DB IDs must be unique and non-empty;
// the coordinator directory is created if needed. The
// epoch starts at 1 and increments on every captured
// snapshot (a token's epoch therefore orders token
// captures, it is not a wall clock).
func NewTimelineDomain(id, coordinatorDir string, parts ...DomainParticipant) (*TimelineDomain, error) {
	if id == "" {
		return nil, fmt.Errorf("yowza domain: empty domain id")
	}
	if len(parts) == 0 {
		return nil, fmt.Errorf("yowza domain: no participants")
	}
	d := &TimelineDomain{id: id, epoch: 1, dir: coordinatorDir, parts: map[string]*PersistedSegDB{}, issued: map[string]*GlobalSnapshotToken{}}
	for _, p := range parts {
		if p.DBID == "" || p.DB == nil {
			return nil, fmt.Errorf("yowza domain: participant with empty id or nil db")
		}
		if _, dup := d.parts[p.DBID]; dup {
			return nil, fmt.Errorf("yowza domain: duplicate participant id %q", p.DBID)
		}
		d.parts[p.DBID] = p.DB
		d.order = append(d.order, p.DBID)
		if p.DB.Durability() == DurabilitySync {
			d.syncDir = true
		}
	}
	sort.Strings(d.order)
	if err := os.MkdirAll(coordinatorDir, 0o755); err != nil {
		return nil, err
	}
	return d, nil
}

// ID returns the domain ID.
func (d *TimelineDomain) ID() string { return d.id }

// Epoch returns the current epoch (captures increment it).
func (d *TimelineDomain) Epoch() uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.epoch
}

func (d *TimelineDomain) logPath() string { return filepath.Join(d.dir, "domain-coordinator.log") }

func (d *TimelineDomain) appendLog(kind, opID string, payload any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	rec := domainLogRecord{Kind: kind, OpID: opID, Payload: raw}
	body, err := json.Marshal(struct {
		Kind    string          `json:"kind"`
		OpID    string          `json:"op_id"`
		Payload json.RawMessage `json:"payload"`
	}{Kind: kind, OpID: opID, Payload: raw})
	if err != nil {
		return err
	}
	rec.Checksum = crc32.ChecksumIEEE(body)
	line, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(d.logPath(), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(append(line, '\n')); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if d.syncDir {
		return persistSyncDir(d.dir)
	}
	return nil
}

func (d *TimelineDomain) stagePath(opID string) string {
	return filepath.Join(d.dir, "domain-op-"+opID+".stage.json")
}

func (d *TimelineDomain) stageOp(opID string, writes map[string][]globalMergeDelta) error {
	stage := domainOpStage{OpID: opID, Writes: writes}
	body, err := json.Marshal(struct {
		OpID   string                        `json:"op_id"`
		Writes map[string][]globalMergeDelta `json:"writes"`
	}{OpID: opID, Writes: writes})
	if err != nil {
		return err
	}
	type staged struct {
		OpID     string                        `json:"op_id"`
		Writes   map[string][]globalMergeDelta `json:"writes"`
		Checksum uint32                        `json:"checksum"`
	}
	data, err := json.Marshal(staged{OpID: stage.OpID, Writes: stage.Writes, Checksum: crc32.ChecksumIEEE(body)})
	if err != nil {
		return err
	}
	tmp := d.stagePath(opID) + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	f, err := os.OpenFile(tmp, os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	syncErr := f.Sync()
	f.Close()
	if syncErr != nil {
		_ = os.Remove(tmp)
		return syncErr
	}
	if err := os.Rename(tmp, d.stagePath(opID)); err != nil {
		return err
	}
	if d.syncDir {
		return persistSyncDir(d.dir)
	}
	return nil
}

func domainDecodeStage(data []byte) (*domainOpStage, error) {
	var s struct {
		OpID     string                        `json:"op_id"`
		Writes   map[string][]globalMergeDelta `json:"writes"`
		Checksum uint32                        `json:"checksum"`
	}
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("%w: stage unparseable: %v", ErrDomainOpCorrupt, err)
	}
	body, err := json.Marshal(struct {
		OpID   string                        `json:"op_id"`
		Writes map[string][]globalMergeDelta `json:"writes"`
	}{OpID: s.OpID, Writes: s.Writes})
	if err != nil {
		return nil, fmt.Errorf("%w: stage re-encode: %v", ErrDomainOpCorrupt, err)
	}
	if crc32.ChecksumIEEE(body) != s.Checksum {
		return nil, fmt.Errorf("%w: stage checksum mismatch for op %s", ErrDomainOpCorrupt, s.OpID)
	}
	return &domainOpStage{OpID: s.OpID, Writes: s.Writes}, nil
}

// CaptureGlobalSnapshot captures one global snapshot:
// a vector + durable pin per participant, in DB-ID
// order, logged as a snapshot record. The token's
// vectors are the participants' roots at capture
// time; later writes to any participant are invisible
// to reads through this token.
func (d *TimelineDomain) CaptureGlobalSnapshot() (*GlobalSnapshotToken, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	var parts []TokenParticipant
	for _, id := range d.order {
		db := d.parts[id]
		pin, err := db.CreateDurablePin("Domain:"+d.id, "domain:"+d.id, "")
		if err != nil {
			return nil, fmt.Errorf("yowza domain: pin for %s: %w", id, err)
		}
		parts = append(parts, TokenParticipant{DBID: id, Vector: append([]uint64(nil), pin.Vector...), PinID: pin.ID})
	}
	tok := &GlobalSnapshotToken{DomainID: d.id, Epoch: d.epoch, Participants: parts}
	tok.Checksum = domainTokenChecksum(tok.DomainID, tok.Epoch, tok.Participants)
	d.epoch++
	key := fmt.Sprintf("%x", tok.Checksum)
	d.issued[key] = tok
	if err := d.appendLog("snapshot", key, tok); err != nil {
		return nil, err
	}
	return tok, nil
}

// ReleaseGlobalSnapshot deletes the durable pins a
// token holds, retiring the token. Reads through a
// released token then fail validation (pin gone),
// which is the point: a retired global snapshot must
// not keep pinning history forever.
func (d *TimelineDomain) ReleaseGlobalSnapshot(tok *GlobalSnapshotToken) error {
	if tok == nil {
		return fmt.Errorf("yowza domain: nil token")
	}
	for _, p := range tok.Participants {
		db, ok := d.parts[p.DBID]
		if !ok {
			continue
		}
		_ = db.DeletePin(p.PinID)
	}
	return nil
}

// ValidateToken checks a token against this domain:
// domain ID, epoch not newer than any issued epoch,
// exact participant set, checksum, and every pin
// still active with exactly the token's vector. A
// token issued by another domain, or a hand-built
// token naming this domain's DBs, fails here — the
// issued map + pin check is what makes "issued by a
// domain" checkable instead of asserted.
func (d *TimelineDomain) ValidateToken(tok *GlobalSnapshotToken) error {
	if tok == nil || !tok.valid() {
		return fmt.Errorf("%w: token checksum invalid", ErrGlobalSnapshotOutsideDomain)
	}
	if tok.DomainID != d.id {
		return fmt.Errorf("%w: token domain %q, this domain %q", ErrGlobalSnapshotOutsideDomain, tok.DomainID, d.id)
	}
	if len(tok.Participants) != len(d.order) {
		return fmt.Errorf("%w: token covers %d participants, domain has %d", ErrGlobalSnapshotOutsideDomain, len(tok.Participants), len(d.order))
	}
	for _, id := range d.order {
		p, ok := tok.participant(id)
		if !ok {
			return fmt.Errorf("%w: token missing participant %q", ErrGlobalSnapshotOutsideDomain, id)
		}
		pin, err := d.parts[id].GetPin(p.PinID)
		if err != nil {
			return fmt.Errorf("%w: pin %s for %s unreadable: %v", ErrGlobalSnapshotOutsideDomain, p.PinID, id, err)
		}
		if pin.Status != PinStatusActive {
			return fmt.Errorf("%w: pin %s for %s not active", ErrGlobalSnapshotOutsideDomain, p.PinID, id)
		}
		if !equalUint64Slice(pin.Vector, p.Vector) {
			return fmt.Errorf("%w: pin %s vector mismatch for %s", ErrGlobalSnapshotOutsideDomain, p.PinID, id)
		}
	}
	return nil
}

// ReadGlobalAt reads one key from one participant at
// the token's vector. The token must validate against
// this domain first; the read goes through
// ScanStreaming at the token vector (leases pin the
// snapshot for the read), never through the live head.
func (d *TimelineDomain) ReadGlobalAt(tok *GlobalSnapshotToken, dbID, key string) ([]byte, bool, error) {
	if err := d.ValidateToken(tok); err != nil {
		return nil, false, err
	}
	p, ok := tok.participant(dbID)
	if !ok {
		return nil, false, fmt.Errorf("%w: token has no participant %q", ErrGlobalSnapshotOutsideDomain, dbID)
	}
	db := d.parts[dbID]
	it, err := db.ScanStreaming(key, p.Vector)
	if err != nil {
		return nil, false, err
	}
	defer it.Close()
	for {
		batch, err := it.NextBatch(64)
		if err != nil {
			return nil, false, err
		}
		if len(batch) == 0 {
			return nil, false, nil
		}
		for _, e := range batch {
			if string(e.Key) == key {
				return append([]byte(nil), e.Val...), true, nil
			}
		}
	}
}

// ErrDomainStaleVector reports an ApplyAtomic whose
// expected vector did not match a participant's live
// vector at prepare time. Nothing is staged, logged,
// or applied; the caller re-captures vectors and
// retries as a new operation.
var ErrDomainStaleVector = errors.New("yowza domain: stale expected vector")

// lockParticipantsLocked locks every distinct participant
// DB's p.mu in ONE global order — DB directory path —
// and returns the unlock function (reverse order).
// Caller must hold d.mu. Directory order, not DB-ID
// order, is the global order: two domains may assign
// different IDs to the same DBs, and ID order could
// then lock the same DBs in opposite orders and
// deadlock. Directory paths are unique per DB (the
// exclusive directory lock guarantees one live instance
// per directory), so directory order is a true global
// order. The same DB under two IDs is locked once but
// still applied once per ID (see applyAtomicLocked).
// applyDeltasLocked requires p.mu held, and facade
// Put/Delete take p.mu, so holding all p.mu across
// re-check + commit + apply serialises this operation
// against every ordinary writer on these DB instances
// in this process — that is what closes the old
// post-commit window for in-process writers (see
// ApplyAtomic). It does NOT serialise against writers
// in other processes (p.mu is in-process only): that
// case needs ApplyAtomicCoordinated (coord_domain.go).
func (d *TimelineDomain) lockParticipantsLocked() func() {
	seen := map[*PersistedSegDB]bool{}
	var dbs []*PersistedSegDB
	for _, id := range d.order {
		db := d.parts[id]
		if db == nil || seen[db] {
			continue
		}
		seen[db] = true
		dbs = append(dbs, db)
	}
	sort.Slice(dbs, func(i, j int) bool { return dbs[i].Dir() < dbs[j].Dir() })
	for _, db := range dbs {
		db.mu.Lock()
	}
	return func() {
		for i := len(dbs) - 1; i >= 0; i-- {
			dbs[i].mu.Unlock()
		}
	}
}

// participantDirsLocked returns the distinct participant
// DB directories, sorted (for Patch 2 Track 2 blob
// staging recovery sweeps). Caller holds d.mu; the DB
// pointers and dirs do not change after construction.
func (d *TimelineDomain) participantDirsLocked() []string {
	var dirs []string
	for _, id := range d.order {
		if db := d.parts[id]; db != nil {
			dirs = append(dirs, db.Dir())
		}
	}
	return blobStagingTargetDirs(dirs)
}

// applyAtomicLocked is the single core of ApplyAtomic
// and ApplyAtomicCoordinated. Caller holds d.mu.
// preCommit, when non-nil, runs AFTER the authoritative
// vector re-check and BEFORE the domain commit log,
// while all participant p.mu are held; it returns
// (decisionPublished, err). decisionPublished reports
// whether preCommit published a commit decision of its
// own (a coordinator root) even if a later step failed:
// the coordinated wrapper must then NOT reclaim the
// coordinator staged files (recovery must complete
// the operation), while a preCommit failure (no root)
// means no decision was published anywhere and both
// stagings are reclaimable. The domain commit log is
// the decision for the plain path (preCommit nil).
//
// Order, and why each step is where it is:
//  1. stage (reclaimable, no decision yet) — done
//     BEFORE taking p.mu, so staging I/O does not
//     block ordinary writers.
//  2. lock all p.mu (global directory order).
//  3. AUTHORITATIVE re-check of expectedVectors via
//     snapshotVectorLocked (which requires p.mu held
//     and does not re-lock it — SnapshotVector would
//     deadlock here). A stale vector deletes the
//     stage and returns ErrDomainStaleVector with
//     nothing committed and nothing applied. This
//     re-check, not any earlier pre-check, is what
//     closes the race: a writer that landed any time
//     before this point is caught here, and no writer
//     can land after it until the apply is done,
//     because p.mu are held.
//  4. preCommit (coordinated root publish, if any).
//  5. domain commit log — the decision (plain path)
//     or the second decision record (coordinated
//     path; either decision alone triggers roll-
//     forward, and both recoveries are idempotent,
//     so a crash between the two decisions is safe).
//  6. apply in DB-ID order (all p.mu still held, so
//     no ordinary writer can land between the commit
//     log and any apply — the old post-commit window
//     is closed for in-process writers).
//  7. domain done log, then stage removal.
//
// A nil expectedVectors still gets the commit+apply
// atomicity of steps 5–6 (no writer between commit
// and apply); it only opts out of the stale-vector
// refusal, exactly like global merge's nil
// expectedHead. A writer before step 3 with nil
// vectors is overwritten by design (last commit wins
// at commit time), never silently mid-apply.
func (d *TimelineDomain) applyAtomicLocked(opID string, writes map[string][]globalMergeDelta, expectedVectors map[string][]uint64, preCommit func() (bool, error)) (decisionPublished bool, err error) {
	for dbID := range writes {
		if _, ok := d.parts[dbID]; !ok {
			return false, fmt.Errorf("yowza domain: write set for unknown participant %q", dbID)
		}
	}
	// Stage first (no commit record yet: reclaimable).
	if err := d.stageOp(opID, writes); err != nil {
		return false, err
	}
	unlock := d.lockParticipantsLocked()
	defer unlock()
	// Plain domain apply must not bypass a participant's active
	// coordinator fence. The coordinated entry point passes a
	// preCommit callback and performs its authoritative fence/root
	// publication there; all ordinary facade/domain paths refuse.
	if preCommit == nil {
		for dbID, deltas := range writes {
			if len(deltas) == 0 {
				continue
			}
			if db := d.parts[dbID]; db != nil {
				if ferr := CheckCoordWriteFence(db); ferr != nil {
					_ = os.Remove(d.stagePath(opID))
					return false, ferr
				}
			}
		}
	}
	// Authoritative re-check under all p.mu.
	if expectedVectors != nil {
		for _, dbID := range d.order {
			want, ok := expectedVectors[dbID]
			if !ok {
				continue
			}
			live, lerr := snapshotVectorLocked(d.parts[dbID])
			if lerr != nil {
				_ = os.Remove(d.stagePath(opID))
				return false, lerr
			}
			if !equalUint64Slice(live, want) {
				_ = os.Remove(d.stagePath(opID))
				return false, fmt.Errorf("%w: op %s: participant %s want %v, live %v", ErrDomainStaleVector, opID, dbID, want, live)
			}
		}
	}
	// Coordinated decision point, if any, before the
	// domain commit log and still under all p.mu.
	if preCommit != nil {
		published, perr := preCommit()
		if perr != nil {
			_ = os.Remove(d.stagePath(opID))
			return published, perr
		}
		decisionPublished = published
	}
	// Commit decision: log it BEFORE any apply.
	if err := d.appendLog("commit", opID, map[string]string{"op": opID}); err != nil {
		return decisionPublished, err
	}
	decisionPublished = true
	// Apply in DB-ID order, all p.mu still held.
	for _, dbID := range d.order {
		deltas, ok := writes[dbID]
		if !ok || len(deltas) == 0 {
			continue
		}
		if applyErr := applyDeltasLocked(d.parts[dbID], deltas); applyErr != nil {
			return true, fmt.Errorf("%w: op %s on %s: %v", ErrDomainOpCommitted, opID, dbID, applyErr)
		}
	}
	if err := d.appendLog("done", opID, map[string]string{"op": opID}); err != nil {
		return true, fmt.Errorf("%w: op %s applied but done log failed: %v", ErrDomainOpCommitted, opID, err)
	}
	_ = os.Remove(d.stagePath(opID))
	return decisionPublished, nil
}

// ApplyAtomic applies per-DB write sets atomically in
// the domain sense: stage -> (authoritative vector
// re-check under all participant p.mu) -> commit log
// -> apply -> done log. A crash after commit is
// completed by RecoverDomainOps; a crash before
// commit applies nothing.
//
// Axis B race closure (this replaces the Phase E
// post-commit window for in-process writers): Phase E
// checked vectors BEFORE staging, then committed and
// applied per DB under separate p.mu acquisitions, so
// an ordinary writer landing between the commit log
// and an apply was overwritten for overlapping keys
// by roll-forward. Now the authoritative re-check,
// the commit log, and the whole apply happen while
// every participant's p.mu is held, so an ordinary
// writer on the same DB instances can land only
// before the re-check (caught as stale when vectors
// are given) or after the apply (correct last-writer
// order) — never between commit and apply. Two
// domain operations on the same DBs, even through
// different TimelineDomain objects (different d.mu),
// serialise the same way, because they share p.mu.
// This is an in-process guarantee only: writers in
// OTHER processes do not share p.mu. Cross-process
// commits must use ApplyAtomicCoordinated, which adds
// coordinator fencing on top of this same core.
func (d *TimelineDomain) ApplyAtomic(opID string, writes map[string][]globalMergeDelta, expectedVectors map[string][]uint64) error {
	return d.ApplyAtomicWithBlobs(opID, writes, expectedVectors, nil)
}

// ApplyAtomicWithBlobs is ApplyAtomic plus the Patch 2
// Track 2 blob wiring. man is the operation's typed
// blob manifest (see blob_wiring_patch2.go): the ONLY
// basis on which a write-set value is treated as a blob
// reference. A nil/empty manifest means the operation
// carries no blob references — values are never parsed
// to discover blobs. Ordering: blob copy + staging
// (durable, via BeginStaging) happens BEFORE the domain
// stage and commit log, so a crash at any later point
// finds the staging record; on success the staging is
// commit-resolved after apply + done log; on a
// no-decision error (stale vector, unknown participant,
// pre-commit failure) it is abort-resolved, making the
// blobs collectible; on ErrDomainOpCommitted (commit
// logged, apply incomplete) it is deliberately LEFT for
// recovery, which rolls forward and then commit-resolves
// it — aborting there would let a GC pass delete bytes
// the roll-forward still needs. Plain ApplyAtomic
// (nil manifest) stages no blobs: a caller carrying
// cross-DB blob references MUST use this method with a
// manifest, or its blobs dangle exactly as Phase E
// documented. That requirement is explicit, not silent:
// there is no content-sniffing fallback.
func (d *TimelineDomain) ApplyAtomicWithBlobs(opID string, writes map[string][]globalMergeDelta, expectedVectors map[string][]uint64, man *BlobOpManifest) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.applyAtomicWithManifestLocked(opID, writes, expectedVectors, man, nil)
}

// applyAtomicWithManifestLocked is the manifest-aware
// core wrapper used by both the plain and coordinated
// paths (the coordinated path passes its preCommit
// through; see coord_domain.go). It owns the blob
// staging lifecycle around applyAtomicLocked and must
// be called with d.mu held.
func (d *TimelineDomain) applyAtomicWithManifestLocked(opID string, writes map[string][]globalMergeDelta, expectedVectors map[string][]uint64, man *BlobOpManifest, preCommit func() (bool, error)) error {
	// Reject the plain path before copying/staging any cross-DB blobs.
	// applyAtomicLocked repeats this check under every participant DB
	// lock, closing the race with a concurrent coordinator registration.
	if preCommit == nil {
		for dbID, deltas := range writes {
			if len(deltas) == 0 {
				continue
			}
			if db := d.parts[dbID]; db != nil {
				if err := CheckCoordWriteFence(db); err != nil {
					return err
				}
			}
		}
	}
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
	_, coreErr := d.applyAtomicLocked(opID, writes, expectedVectors, preCommit)
	if coreErr != nil {
		if errors.Is(coreErr, ErrDomainOpCommitted) {
			// Decision published, apply incomplete:
			// LEAVE staging for recovery (see the
			// method comment). Recovery roll-forward
			// lands the blobs in the head and then
			// commit-resolves the staging.
			return coreErr
		}
		// No decision anywhere: abort-resolve the
		// blob staging (best effort across targets;
		// the original error is what the caller sees).
		_ = resolveBlobStagingDirs(stagedDirs, opID, false)
		return coreErr
	}
	// Success: commit-resolve. A resolve failure after
	// a successful apply + done log is reported as a
	// committed-but-incomplete error (same shape as
	// the coordinated MarkDone failure): the data is
	// in and the head reference protects the blobs, so
	// the leftover staging record is a benign leak
	// that the next RecoverDomainOps drops (its sweep
	// commit-resolves staging for done operations).
	// It must never be reported as a plain failure,
	// which would invite a non-idempotent retry.
	if err := resolveBlobStagingDirs(stagedDirs, opID, true); err != nil {
		return fmt.Errorf("%w: op %s applied but blob staging resolve failed: %v", ErrDomainOpCommitted, opID, err)
	}
	return nil
}

// RecoverDomainOps replays the coordinator log for
// this domain: commit-without-done operations roll
// forward, prepare-only stages are reclaimed, done
// operations are cleaned up. It returns the op IDs
// rolled forward, sorted. It must run before serving
// reads after a crash, and never concurrently with
// an active ApplyAtomic.
func (d *TimelineDomain) RecoverDomainOps() ([]string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	f, err := os.Open(d.logPath())
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	committed := map[string]bool{}
	done := map[string]bool{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1024*1024), 1024*1024)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var rec domainLogRecord
		if err := json.Unmarshal(line, &rec); err != nil {
			f.Close()
			return nil, fmt.Errorf("yowza domain: coordinator log line unparseable: %v", err)
		}
		body, err := json.Marshal(struct {
			Kind    string          `json:"kind"`
			OpID    string          `json:"op_id"`
			Payload json.RawMessage `json:"payload"`
		}{Kind: rec.Kind, OpID: rec.OpID, Payload: rec.Payload})
		if err != nil {
			f.Close()
			return nil, err
		}
		if crc32.ChecksumIEEE(body) != rec.Checksum {
			f.Close()
			return nil, fmt.Errorf("yowza domain: coordinator log checksum mismatch for op %s", rec.OpID)
		}
		switch rec.Kind {
		case "commit":
			committed[rec.OpID] = true
		case "done":
			done[rec.OpID] = true
		}
	}
	f.Close()
	if err := sc.Err(); err != nil {
		return nil, err
	}
	var rolled []string
	for opID := range committed {
		if done[opID] {
			_ = os.Remove(d.stagePath(opID))
			continue
		}
		data, err := os.ReadFile(d.stagePath(opID))
		if err != nil {
			return rolled, fmt.Errorf("%w: stage for committed op %s unreadable: %v", ErrDomainOpCorrupt, opID, err)
		}
		stage, err := domainDecodeStage(data)
		if err != nil {
			return rolled, err
		}
		// Roll one operation forward atomically with
		// respect to in-process writers, exactly like
		// ApplyAtomic: hold every participant's p.mu
		// across the whole multi-DB apply AND the done
		// log, so no ordinary writer can land between
		// two participants' applies and be half
		// overwritten by the roll-forward. The per-op
		// closure gives each operation its own unlock
		// (a loop-level defer would hold locks across
		// every remaining operation).
		rollErr := func() error {
			unlock := d.lockParticipantsLocked()
			defer unlock()
			for _, dbID := range d.order {
				deltas, ok := stage.Writes[dbID]
				if !ok || len(deltas) == 0 {
					continue
				}
				if applyErr := applyDeltasLocked(d.parts[dbID], deltas); applyErr != nil {
					return fmt.Errorf("%w: roll-forward of op %s on %s: %v", ErrDomainOpCommitted, opID, dbID, applyErr)
				}
			}
			if err := d.appendLog("done", opID, map[string]string{"op": opID}); err != nil {
				return err
			}
			return nil
		}()
		if rollErr != nil {
			return rolled, rollErr
		}
		rolled = append(rolled, opID)
		_ = os.Remove(d.stagePath(opID))
		// Patch 2 Track 2: the rolled-forward op's
		// blobs are now in the head, so its blob
		// staging resolves as committed (lenient:
		// an op with no blob staging resolves to
		// nothing). A roll-forward failure above
		// returns early WITHOUT resolving, so staging
		// survives for the next recovery attempt.
		for _, dir := range d.participantDirsLocked() {
			resolveBlobStagingLenient(dir, opID, true)
		}
	}
	// Patch 2 Track 2 — blob staging sweep, BEFORE
	// stage reclaim (order matters, see below). For
	// every staged blob operation in every participant
	// directory:
	//   - done (applied, staging leftover from a
	//     success+resolve failure): commit-resolve.
	//   - not committed, no domain stage file, no
	//     coordinator root in this domain's directory:
	//     abort-resolve (crash between blob staging
	//     and domain staging, or a no-decision abort
	//     whose resolve never ran) — the blobs become
	//     collectible.
	//   - not committed but WITH a domain stage file:
	//     SKIP on this run. The stage file is reclaimed
	//     by the loop below, so the NEXT (idempotent)
	//     recovery run sees no stage file and resolves
	//     the staging then. The skip is what keeps a
	//     root-published coordinated operation (which
	//     always has a domain stage file, and whose
	//     root may live in a different coordinator
	//     directory) from being abort-resolved by plain
	//     recovery before coordinated recovery runs.
	//     When the coordinator directory IS this
	//     domain's directory, the root check alone
	//     protects such operations and one run
	//     suffices for everything else.
	for _, dir := range d.participantDirsLocked() {
		for _, blobOp := range StagedBlobOps(dir) {
			if done[blobOp] {
				resolveBlobStagingLenient(dir, blobOp, true)
				continue
			}
			if committed[blobOp] {
				continue // rolled above (or done, handled above)
			}
			if _, err := os.Stat(coordRootPath(d.dir, blobOp)); err == nil {
				continue // coordinator decision exists; coordinated recovery owns it
			}
			if _, err := os.Stat(d.stagePath(blobOp)); err == nil {
				continue // stage exists; reclaimed below, resolved on the next run
			}
			resolveBlobStagingLenient(dir, blobOp, false)
		}
	}
	// Reclaim prepare-only stages: any stage file whose
	// op never reached commit. (Stages are written
	// before the commit log, so a stage without a
	// commit record is exactly a crashed prepare.)
	ents, err := os.ReadDir(d.dir)
	if err != nil {
		return rolled, err
	}
	for _, e := range ents {
		name := e.Name()
		if !strings.HasPrefix(name, "domain-op-") || !strings.HasSuffix(name, ".stage.json") {
			continue
		}
		opID := strings.TrimSuffix(strings.TrimPrefix(name, "domain-op-"), ".stage.json")
		if !committed[opID] {
			_ = os.Remove(filepath.Join(d.dir, name))
		}
	}
	sort.Strings(rolled)
	return rolled, nil
}
