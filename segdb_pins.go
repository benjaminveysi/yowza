package yowza

// Phase B2 — DURABLE snapshot pins.
//
// Problem (PARALLEL_TIMELINE_NOTES §6.4): timeline branch
// leases lived only in the C lease table and vanished when
// the runtime closed, so a manager reopening after an
// intervening Compact could meet ErrTimelineBranchPointGone
// for a branch whose data was compacted away between close
// and reopen. The fix is a durable pin in PersistedSegDB's
// own persistent metadata (this file), NOT in the Timeline
// registry:
//
//   pins.json — atomic publish (tmp + sync + rename +
//   directory sync in Sync mode), same commit pattern as
//   checkpoint.json. One record per pin:
//     id, owner ("Timeline" or a query-session owner),
//     vector (all shards), generation (PersistedSegDB
//     checkpoint generation at pin time — the root /
//     manifest generation the vector belongs to),
//     checkpoint_seq, host_seq (host WAL seq at pin time),
//     created_unix_nano, status ("active"/"deleted"),
//     timeline_id, transaction_id (recovery/cleanup keys).
//
// Lifecycle and crash orderings (the ordering the
// TimelineManager call-site MUST follow; wiring is a later
// integration stage, but this API makes the order
// expressible):
//   1. CreateDurablePin / PinSnapshot publishes the pin
//      FIRST (pins.json rename definite before return).
//   2. Only then does the caller commit the Timeline
//      registry entry that references the pin.
//   Crash between (1) and (2) leaves an ORPHAN pin: extra
//   space, never silent loss — the pin's leases still pin
//   the branch data, and CleanupOrphanPins (registry check
//   supplied by the caller) removes only pins whose
//   timeline_id is absent from the caller's registry.
//   Crash before (1) leaves no pin and no registry entry:
//   the branch simply never existed.
//   Deletion order is the reverse: the registry tombstone
//   is published first by the caller, then DeletePin marks
//   the pin deleted (pins.json publish) and releases its
//   leases. A crash between those two leaves a pin whose
//   timeline is already tombstoned — again an orphan,
//   cleaned up by the same registry check.
//
// Recovery BEFORE Compact: OpenPersistedSegDB calls
// recoverDurablePins (see the hook in segdb_persist.go)
// after WAL replay and BEFORE returning, i.e. before any
// Compact can run against the reopened instance. Each
// active pin's per-shard leases are re-registered there,
// so the C compact lease check defers any merge that
// would drop a pinned snapshot — without TimelineManager
// having to be the first consumer after Open. Leases die
// with the wasm modules on Close and are re-registered
// on the next open; the pins.json record is the durable
// truth, the in-memory leases are just its live shadow.
//
// Limits, stated plainly: pins share the C lease table
// (64 entries per shard) with snapshot handles and scan
// iterators, so a database with more than 64 concurrent
// live pins/handles/iterators per shard fails pin/handle
// creation with ErrLeaseTableFull rather than silently
// unpinning. Deleted pins stay in pins.json as tombstones
// (like timeline registry tombstones) so a later cleanup
// pass can distinguish "never existed" from "deleted".

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// DurableSnapshotPin is one durable pin record. Vector is
// index-aligned with the database's shards.
type DurableSnapshotPin struct {
	ID              string   `json:"id"`
	Owner           string   `json:"owner"`
	Vector          []uint64 `json:"vector"`
	Generation      uint64   `json:"generation"`
	CheckpointSeq   uint64   `json:"checkpoint_seq"`
	HostSeq         uint64   `json:"host_seq"`
	CreatedUnixNano int64    `json:"created_unix_nano"`
	Status          string   `json:"status"` // "active" or "deleted"
	TimelineID      string   `json:"timeline_id,omitempty"`
	TransactionID   string   `json:"transaction_id,omitempty"`
}

// PinStatusActive / PinStatusDeleted are the only two
// pin statuses.
const (
	PinStatusActive  = "active"
	PinStatusDeleted = "deleted"
)

// ErrPinNotFound is returned by GetPin/DeletePin for an
// unknown pin ID.
var ErrPinNotFound = errors.New("yowza persist: durable pin not found")

// ErrDurablePinsCorrupt is returned by OpenPersistedSegDB
// when neither pins.json nor its backup can be resolved to
// a verified pin set (both corrupt, or primary corrupt
// with no backup, or primary missing with a corrupt
// backup). The open fails: the gate stays closed rather
// than Compact ever running against an unverified or
// silently-dropped pin set. Callers test it with
// errors.Is.
var ErrDurablePinsCorrupt = errors.New("yowza persist: durable pins corrupt and unrecoverable")

// PinRecoveryReport describes how the pin set was
// recovered at open time. It is the fallback-reporting
// channel: a caller that needs to know the primary was
// bypassed reads it from the opened instance via
// PinRecoveryReport() — a fallback is NEVER silent.
// Source is one of "primary", "legacy-primary",
// "backup", "legacy-backup", or "none" (both files
// missing: the empty set, the pre-existing behaviour).
// UsedBackup is true exactly when the set came from the
// backup file (primary corrupt or missing). LegacyFormat
// is true when the set came from a pre-checksum file
// (see the migration rule in loadDurablePinsWithFallback).
type PinRecoveryReport struct {
	Source       string
	UsedBackup   bool
	LegacyFormat bool
}

// ErrDurablePinActive is returned when an operation would
// drop pinned data (reserved for callers that must refuse
// rather than defer; PersistedSegDB.Compact itself defers
// via the C lease check, so it does not return this).
var ErrDurablePinActive = errors.New("yowza persist: active durable pin pins an old snapshot")

type durablePinState struct {
	pin      *DurableSnapshotPin
	leaseIDs []uint64
}

// Pins hardening (checksum + backup descriptor).
//
// pins.json is written as a version-2 envelope:
//
//	{ "version": 2, "pins": [...], "checksum": "<hex sha256>" }
//
// The checksum is SHA-256 (chosen over CRC-32: stronger
// than the required minimum, collision-resistant, and
// still trivial cost at pins.json scale) over the FULL
// pin-set payload — the canonical JSON of the pin array
// with records sorted by ID (nil set normalised to []),
// never just a header. Every record and every field is
// therefore covered; flipping any byte of any record
// changes the checksum.
//
// pins.bak is a same-generation replica, not a lagging
// generation: every save writes the SAME bytes to the
// backup and to the primary, each via the same
// discipline (tmp + file sync + rename + directory sync
// in Sync mode). Write order, and why it is this way:
//
//  1. Backup FIRST (pins.bak.tmp -> pins.bak).
//  2. Primary SECOND (pins.json.tmp -> pins.json).
//
// A crash at any point leaves either the old verified
// set or the new verified set, never a torn set
// presented as valid, because neither file is ever
// written in place: each rename is atomic, so each file
// is at every instant either its old content or its new
// content, and both contents carry a valid checksum.
// Backup-first additionally guarantees the migration
// case: a pre-checksum primary is the ONLY copy until
// step 1 completes, and step 1 does not touch it, so a
// crash mid-migration leaves a loadable set (the legacy
// primary, or the new backup, or both).
//
// One asymmetry is deliberate and safe: if step 2 fails
// (or the process dies) after step 1, the backup holds
// a set the primary does not — a pin whose creation
// reported failure. That set is consulted ONLY if the
// primary later becomes corrupt/missing, and over-
// pinning is the safe direction (extra retention, never
// silent loss); orphan cleanup removes such pins.
//
// Open behaviour (loadDurablePinsWithFallback): the
// primary is trusted only if it parses AND verifies (or
// is a legacy file, below). Otherwise the backup is
// consulted; a verified backup is used and the fallback
// is reported in PinRecoveryReport. If neither file
// resolves to a verified set, open fails loudly with
// ErrDurablePinsCorrupt. A valid primary is used as-is
// even if the backup is corrupt/stale/missing; the next
// pin write refreshes the backup. Open never rewrites
// the pin files: a fallback persists (reported, on every
// open) until the next pin write heals the primary.
//
// Migration: a pre-checksum file (version 1, or no
// version field, and no checksum) still loads, and is
// reported as legacy. It is migrated on the NEXT pin
// write, which writes backup-first (above), so the only
// copy is never destroyed during migration. A file that
// claims version >= 2 but carries no checksum is NOT
// treated as legacy: that shape means a checksum was
// stripped, and silently accepting it would defeat the
// checksum. It is corrupt, and the backup path applies.
//
// I/O errors reading either file (anything other than
// "not exist") fail loudly without consulting the other
// file: an unreadable primary is not evidence of
// corruption, and masking it with the backup would hide
// an operator-visible fault.

type durablePinFile struct {
	Version  int                   `json:"version"`
	Pins     []*DurableSnapshotPin `json:"pins"`
	Checksum string                `json:"checksum,omitempty"`
}

const durablePinFileName = "pins.json"

// durablePinBackupFileName is the backup descriptor:
// a same-generation replica of pins.json.
const durablePinBackupFileName = "pins.bak"

func (p *PersistedSegDB) pinPath() string {
	return filepath.Join(p.dir, durablePinFileName)
}

func (p *PersistedSegDB) pinBackupPath() string {
	return filepath.Join(p.dir, durablePinBackupFileName)
}

// PinRecoveryReport returns the open-time pin recovery
// report for this instance (see PinRecoveryReport).
func (p *PersistedSegDB) PinRecoveryReport() PinRecoveryReport {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.pinRecovery
}

// pinIDOrEmpty is the sort key for a pin record; nil
// records sort first and never panic the sort.
func pinIDOrEmpty(pin *DurableSnapshotPin) string {
	if pin == nil {
		return ""
	}
	return pin.ID
}

// durablePinChecksum is the SHA-256 hex digest of the
// canonical JSON of the pin array: records sorted by ID,
// nil set normalised to []. Save and verify use exactly
// this function, so a stored checksum is reproducible
// from the stored records alone.
func durablePinChecksum(pins []*DurableSnapshotPin) string {
	norm := pins
	if norm == nil {
		norm = []*DurableSnapshotPin{}
	}
	sorted := append([]*DurableSnapshotPin(nil), norm...)
	sort.SliceStable(sorted, func(i, j int) bool { return pinIDOrEmpty(sorted[i]) < pinIDOrEmpty(sorted[j]) })
	data, err := json.Marshal(sorted)
	if err != nil {
		// Plain structs with no custom marshallers:
		// unreachable in practice; an empty digest
		// can never match a stored checksum, so a
		// failure here fails verification loudly.
		return ""
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// pinFileParse is the outcome of parsing one pin file.
type pinFileParse struct {
	pins   []*DurableSnapshotPin
	legacy bool // no checksum, version <= 1: pre-change file
	err    error
}

// parseDurablePinFile parses and, when a checksum is
// present, verifies one pin file's bytes. Unparseable
// bytes, a checksum mismatch, and a version >= 2 file
// without a checksum are all corruption (err != nil);
// a checksum-less version <= 1 file is legacy, not
// corruption.
func parseDurablePinFile(data []byte) pinFileParse {
	var f durablePinFile
	if err := json.Unmarshal(data, &f); err != nil {
		return pinFileParse{err: fmt.Errorf("unparseable: %w", err)}
	}
	if f.Checksum != "" {
		if durablePinChecksum(f.Pins) != f.Checksum {
			return pinFileParse{err: fmt.Errorf("checksum mismatch")}
		}
		return pinFileParse{pins: f.Pins}
	}
	if f.Version >= 2 {
		return pinFileParse{err: fmt.Errorf("version %d file without checksum", f.Version)}
	}
	return pinFileParse{pins: f.Pins, legacy: true}
}

// loadDurablePinsWithFallback resolves the pin set from
// dir, following the open behaviour documented above.
// Both files missing yields the empty set with source
// "none". Any other unresolvable state yields
// ErrDurablePinsCorrupt.
func loadDurablePinsWithFallback(dir string) ([]*DurableSnapshotPin, PinRecoveryReport, error) {
	primaryPath := filepath.Join(dir, durablePinFileName)
	backupPath := filepath.Join(dir, durablePinBackupFileName)
	primaryData, primaryReadErr := os.ReadFile(primaryPath)
	if primaryReadErr != nil && !os.IsNotExist(primaryReadErr) {
		return nil, PinRecoveryReport{}, fmt.Errorf("%w: cannot read %s: %v", ErrDurablePinsCorrupt, primaryPath, primaryReadErr)
	}
	backupData, backupReadErr := os.ReadFile(backupPath)
	if backupReadErr != nil && !os.IsNotExist(backupReadErr) {
		return nil, PinRecoveryReport{}, fmt.Errorf("%w: cannot read %s: %v", ErrDurablePinsCorrupt, backupPath, backupReadErr)
	}
	primaryExists := primaryReadErr == nil
	backupExists := backupReadErr == nil
	if primaryExists {
		parsed := parseDurablePinFile(primaryData)
		if parsed.err == nil {
			source := "primary"
			if parsed.legacy {
				source = "legacy-primary"
			}
			return parsed.pins, PinRecoveryReport{Source: source, LegacyFormat: parsed.legacy}, nil
		}
		// Primary corrupt: fall through to the backup,
		// keeping the primary detail for the loud error.
		if !backupExists {
			return nil, PinRecoveryReport{}, fmt.Errorf("%w: primary corrupt (%v) and no backup present", ErrDurablePinsCorrupt, parsed.err)
		}
		backupParsed := parseDurablePinFile(backupData)
		if backupParsed.err != nil {
			return nil, PinRecoveryReport{}, fmt.Errorf("%w: primary corrupt (%v) and backup corrupt (%v)", ErrDurablePinsCorrupt, parsed.err, backupParsed.err)
		}
		source := "backup"
		if backupParsed.legacy {
			source = "legacy-backup"
		}
		return backupParsed.pins, PinRecoveryReport{Source: source, UsedBackup: true, LegacyFormat: backupParsed.legacy}, nil
	}
	// Primary missing.
	if !backupExists {
		return nil, PinRecoveryReport{Source: "none"}, nil
	}
	backupParsed := parseDurablePinFile(backupData)
	if backupParsed.err != nil {
		return nil, PinRecoveryReport{}, fmt.Errorf("%w: primary missing and backup corrupt (%v)", ErrDurablePinsCorrupt, backupParsed.err)
	}
	source := "backup"
	if backupParsed.legacy {
		source = "legacy-backup"
	}
	return backupParsed.pins, PinRecoveryReport{Source: source, UsedBackup: true, LegacyFormat: backupParsed.legacy}, nil
}

// writeAtomicPinFile publishes data as dir/name via tmp
// + file sync + rename + directory sync in Sync mode —
// the pre-existing pins.json discipline, factored so the
// backup and the primary share it exactly. On any error
// the previous content of dir/name is untouched (a
// leftover .tmp orphan is ignored by the loader).
func writeAtomicPinFile(dir, name string, data []byte, syncDir bool) error {
	path := filepath.Join(dir, name)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return err
	}
	f, err := os.OpenFile(tmp, os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	syncErr := f.Sync()
	f.Close()
	if syncErr != nil {
		return syncErr
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	if syncDir {
		if err := persistSyncDir(dir); err != nil {
			return err
		}
	}
	return nil
}

// saveDurablePinsFileLocked publishes the current pin set
// atomically, backup FIRST then primary, with identical
// version-2 checksummed bytes (see the header above).
// Caller holds p.mu. A backup failure returns before the
// primary is touched, so the old primary stays definite;
// a primary failure after a backup success leaves the
// old primary definite and the backup holding the newer
// (safe-direction) set. On any error the old pins.json
// content is untouched.
func (p *PersistedSegDB) saveDurablePinsFileLocked() error {
	pins := make([]*DurableSnapshotPin, 0, len(p.pins))
	for _, st := range p.pins {
		cp := *st.pin
		cp.Vector = append([]uint64(nil), st.pin.Vector...)
		pins = append(pins, &cp)
	}
	sort.Slice(pins, func(i, j int) bool { return pins[i].ID < pins[j].ID })
	data, err := json.Marshal(durablePinFile{Version: 2, Pins: pins, Checksum: durablePinChecksum(pins)})
	if err != nil {
		return err
	}
	syncDir := p.mode == DurabilitySync
	if err := writeAtomicPinFile(p.dir, durablePinBackupFileName, data, syncDir); err != nil {
		return fmt.Errorf("yowza persist: durable pin backup publish failed: %w", err)
	}
	if err := writeAtomicPinFile(p.dir, durablePinFileName, data, syncDir); err != nil {
		return fmt.Errorf("yowza persist: durable pin primary publish failed: %w", err)
	}
	return nil
}

// recoverDurablePins loads pins.json and re-registers the
// leases of every active pin, BEFORE the opened instance
// can accept a Compact (called at the end of
// OpenPersistedSegDB). A pin whose vector does not match
// the shard count, or whose lease registration fails,
// fails the open loudly — a half-recovered pin set would
// be exactly the silent-loss hazard pins exist to close.
// On failure, leases registered for earlier pins are
// released.
func (p *PersistedSegDB) recoverDurablePins() error {
	records, report, err := loadDurablePinsWithFallback(p.dir)
	if err != nil {
		return err
	}
	p.pinRecovery = report
	p.pins = make(map[string]*durablePinState)
	var registered []*durablePinState
	for _, rec := range records {
		if rec == nil || rec.ID == "" {
			continue
		}
		cp := *rec
		cp.Vector = append([]uint64(nil), rec.Vector...)
		st := &durablePinState{pin: &cp}
		if n := pinNumber(cp.ID); n > p.pinNext {
			p.pinNext = n
		}
		if cp.Status == PinStatusActive {
			if len(cp.Vector) != len(p.shards) {
				for _, prev := range registered {
					_ = p.releaseShardLeasesLocked(prev.leaseIDs)
				}
				return fmt.Errorf("yowza persist: durable pin %s vector has %d entries, database has %d shards", cp.ID, len(cp.Vector), len(p.shards))
			}
			leaseIDs, err := p.registerLeasesForVectorLocked(cp.Vector)
			if err != nil {
				for _, prev := range registered {
					_ = p.releaseShardLeasesLocked(prev.leaseIDs)
				}
				return fmt.Errorf("yowza persist: durable pin %s lease recovery: %w", cp.ID, err)
			}
			st.leaseIDs = leaseIDs
			registered = append(registered, st)
		}
		p.pins[cp.ID] = st
	}
	return nil
}

// pinNumber parses the numeric suffix of a "pin-<n>" ID,
// or 0 for any other shape (foreign IDs never collide with
// the counter, they just do not advance it).
func pinNumber(id string) uint64 {
	if !strings.HasPrefix(id, "pin-") {
		return 0
	}
	n, err := strconv.ParseUint(strings.TrimPrefix(id, "pin-"), 10, 64)
	if err != nil {
		return 0
	}
	return n
}

// createPinLocked publishes one pin record for an
// already-captured vector + leases. Caller holds p.mu.
// The pins.json publish happens BEFORE the pin is visible
// in memory to any other caller (p.mu is held throughout),
// and on publish failure the leases are released and no
// pin is returned — a pin is definite exactly when its
// file record is definite.
func (p *PersistedSegDB) createPinLocked(vector, leaseIDs []uint64, generation uint64, owner, timelineID, txID string) (*DurableSnapshotPin, error) {
	p.pinNext++
	id := fmt.Sprintf("pin-%d", p.pinNext)
	pin := &DurableSnapshotPin{
		ID:              id,
		Owner:           owner,
		Vector:          append([]uint64(nil), vector...),
		Generation:      generation,
		CheckpointSeq:   p.checkpointSeq,
		HostSeq:         p.seq,
		CreatedUnixNano: time.Now().UnixNano(),
		Status:          PinStatusActive,
		TimelineID:      timelineID,
		TransactionID:   txID,
	}
	st := &durablePinState{pin: pin, leaseIDs: append([]uint64(nil), leaseIDs...)}
	p.pins[id] = st
	if err := p.saveDurablePinsFileLocked(); err != nil {
		delete(p.pins, id)
		_ = p.releaseShardLeasesLocked(leaseIDs)
		return nil, fmt.Errorf("yowza persist: durable pin %s publish failed: %w", id, err)
	}
	cp := *pin
	cp.Vector = append([]uint64(nil), pin.Vector...)
	return &cp, nil
}

// CreateDurablePin publishes a durable pin for the CURRENT
// snapshot (vector + leases captured atomically under
// p.mu, exactly like AcquireSnapshot) and returns it. This
// is step 1 of the branch-creation order: call this FIRST,
// then commit the Timeline registry entry that references
// pin.ID. A crash after this returns leaves an orphan pin
// (see CleanupOrphanPins), never a registry entry without
// a pin.
func (p *PersistedSegDB) CreateDurablePin(owner, timelineID, txID string) (*DurableSnapshotPin, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	// PATCH2 Track 2: maintenance fencing — pin
	// changes affect GC liveness, so they hold the
	// live fence for the whole operation.
	release, err := CheckCoordMaintenanceFence(p)
	if err != nil {
		return nil, err
	}
	if release != nil {
		defer release()
	}
	if p.closed {
		return nil, ErrPersistedClosed
	}
	vec, leaseIDs, err := p.snapshotVectorAndLeasesLocked()
	if err != nil {
		return nil, err
	}
	return p.createPinLocked(vec, leaseIDs, p.generation, owner, timelineID, txID)
}

// PinSnapshot publishes a durable pin for an existing
// snapshot handle's vector, with its OWN leases (the
// handle's leases are untouched: closing the handle never
// drops the pin). Required call order is the same as
// CreateDurablePin: pin first, registry commit second.
// The handle must belong to this database and be open.
func (p *PersistedSegDB) PinSnapshot(handle SnapshotHandle, owner, timelineID, txID string) (*DurableSnapshotPin, error) {
	h, ok := handle.(*snapshotHandle)
	if !ok || h.db != p {
		return nil, fmt.Errorf("yowza persist: snapshot handle does not belong to this database")
	}
	if err := h.checkOpen(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, ErrPersistedClosed
	}
	leaseIDs, err := p.registerLeasesForVectorLocked(h.vector)
	if err != nil {
		return nil, err
	}
	return p.createPinLocked(h.vector, leaseIDs, h.generation, owner, timelineID, txID)
}

// ListPins returns copies of all pin records (active and
// deleted tombstones), sorted by ID.
func (p *PersistedSegDB) ListPins() ([]*DurableSnapshotPin, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, ErrPersistedClosed
	}
	out := make([]*DurableSnapshotPin, 0, len(p.pins))
	for _, st := range p.pins {
		cp := *st.pin
		cp.Vector = append([]uint64(nil), st.pin.Vector...)
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// GetPin returns a copy of one pin record, or
// ErrPinNotFound.
func (p *PersistedSegDB) GetPin(id string) (*DurableSnapshotPin, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, ErrPersistedClosed
	}
	st, ok := p.pins[id]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrPinNotFound, id)
	}
	cp := *st.pin
	cp.Vector = append([]uint64(nil), st.pin.Vector...)
	return &cp, nil
}

// DeletePin marks a pin deleted in pins.json (publish
// first) and then releases its leases. It is idempotent:
// deleting an already-deleted pin is a no-op. A publish
// failure leaves the pin active with its leases intact.
func (p *PersistedSegDB) DeletePin(id string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	// PATCH2 Track 2: maintenance fencing, same hold
	// rule as CreateDurablePin above.
	release, err := CheckCoordMaintenanceFence(p)
	if err != nil {
		return err
	}
	if release != nil {
		defer release()
	}
	if p.closed {
		return ErrPersistedClosed
	}
	st, ok := p.pins[id]
	if !ok {
		return fmt.Errorf("%w: %s", ErrPinNotFound, id)
	}
	if st.pin.Status == PinStatusDeleted {
		return nil
	}
	st.pin.Status = PinStatusDeleted
	if err := p.saveDurablePinsFileLocked(); err != nil {
		st.pin.Status = PinStatusActive
		return fmt.Errorf("yowza persist: durable pin %s delete publish failed: %w", id, err)
	}
	leaseIDs := st.leaseIDs
	st.leaseIDs = nil
	_ = p.releaseShardLeasesLocked(leaseIDs)
	return nil
}

// CleanupOrphanPins removes (marks deleted) every ACTIVE
// pin with a non-empty TimelineID for which keep returns
// false — i.e. pins whose Timeline registry entry is
// missing, the crash-between-pin-and-registry outcome.
// Pins with an empty TimelineID (query-session pins) and
// pins keep() accepts are untouched. It returns the IDs
// removed, sorted. The branch data itself is NOT touched:
// cleanup removes only the pin record (and its leases);
// a caller that still needs the data must not call this
// for a live timeline. keep must not be nil.
func (p *PersistedSegDB) CleanupOrphanPins(keep func(timelineID string) bool) ([]string, error) {
	if keep == nil {
		return nil, fmt.Errorf("yowza persist: CleanupOrphanPins requires a registry check, refusing to guess")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	// PATCH2 Track 2: maintenance fencing, same hold
	// rule as CreateDurablePin above.
	release, err := CheckCoordMaintenanceFence(p)
	if err != nil {
		return nil, err
	}
	if release != nil {
		defer release()
	}
	if p.closed {
		return nil, ErrPersistedClosed
	}
	var removed []string
	var toRelease [][]uint64
	held := map[string][]uint64{}
	for _, st := range p.pins {
		if st.pin.Status != PinStatusActive || st.pin.TimelineID == "" {
			continue
		}
		if keep(st.pin.TimelineID) {
			continue
		}
		st.pin.Status = PinStatusDeleted
		removed = append(removed, st.pin.ID)
		held[st.pin.ID] = st.leaseIDs
		toRelease = append(toRelease, st.leaseIDs)
		st.leaseIDs = nil
	}
	if len(removed) == 0 {
		return nil, nil
	}
	if err := p.saveDurablePinsFileLocked(); err != nil {
		// Publish failed: the file still says active, so
		// restore the in-memory state (including leases)
		// to match it rather than reporting deleted pins
		// that are not.
		for _, st := range p.pins {
			if ids, ok := held[st.pin.ID]; ok {
				st.pin.Status = PinStatusActive
				st.leaseIDs = ids
			}
		}
		return nil, fmt.Errorf("yowza persist: orphan cleanup publish failed: %w", err)
	}
	for _, ids := range toRelease {
		_ = p.releaseShardLeasesLocked(ids)
	}
	sort.Strings(removed)
	return removed, nil
}
