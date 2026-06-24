package yowza

// Axis B — LOCAL MULTI-PROCESS COORDINATOR (single machine only).
//
// This file is the ownership + fencing half of the coordinator.
// It is deliberately NOT a distributed protocol: ownership is a
// non-blocking flock on coord.lock in one coordinator directory
// plus a persisted owner record (coord-owner.json) carrying a
// monotonically increasing epoch. A local file lock does NOT
// replace a distributed coordination protocol, so running this
// across several machines later needs a real distributed
// protocol (consensus / leased ownership with fencing tokens
// served by a quorum, not a local flock). Stating that plainly
// is part of the contract: nothing here claims cross-machine
// safety.
//
// What the four coordinator components share (the point of
// this axis): ONE ownership/fencing contract. A commit, a WAL
// append, a lease acquisition, and a multi-shard root publish
// are all valid only while the caller's session is the current
// owner at the current epoch. The checks are:
//
//   - the session is still active (not released), AND
//   - the persisted owner record still names this session's
//     owner ID at this session's epoch.
//
// An old process that lost ownership — released, then another
// session took over at epoch+1 — therefore fails every later
// commit path with ErrCoordStaleEpoch, loudly, never silently
// degraded. A session that was released but not yet superseded
// fails with ErrCoordReleased. A second session that tries to
// acquire while one is live fails immediately with
// ErrCoordLocked (non-blocking flock), never blocks forever
// and never silently proceeds without ownership.
//
// The exclusive PersistedSegDB directory lock is NOT weakened
// by this file. Two processes still cannot open the SAME DB
// directory. The coordinator's multi-process shape is:
//
//   - one process owns each DB directory (its own exclusive
//     lock, unchanged), and
//   - processes that must agree on domain operations share
//     ONE coordinator directory (fencing, WAL sequence,
//     leases, multi-shard root) — the coordinator directory
//     is the shared storage, not any DB directory.
//
// The coordinated domain path (coord_domain.go) is opt-in:
// plain TimelineDomain.ApplyAtomic stays a single-process
// path (its in-process race is closed by holding every
// participant's p.mu across re-check + commit + apply — see
// timeline_domain.go), and ApplyAtomicCoordinated is the
// path that additionally requires a live coordinator session.
// Callers that do not opt in get no coordinator protection
// and must not expect it.

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

// Coordinator sentinel errors. Compare with errors.Is.
var (
	// ErrCoordLocked reports a coordinator acquire attempt
	// while another session holds the coordinator lock.
	ErrCoordLocked = errors.New("yowza coordinator: coordinator already held by another session")
	// ErrCoordStaleEpoch reports a commit-path attempt by a
	// session whose epoch is no longer current (ownership
	// moved on). The operation was refused and applied
	// nothing.
	ErrCoordStaleEpoch = errors.New("yowza coordinator: stale fencing epoch, ownership lost")
	// ErrCoordReleased reports use of a session after
	// Release. The operation was refused and applied
	// nothing.
	ErrCoordReleased = errors.New("yowza coordinator: session released")
	// ErrCoordCorrupt reports a coordinator file whose
	// checksum or structure fails validation. Recovery
	// stops loudly and applies nothing further.
	ErrCoordCorrupt = errors.New("yowza coordinator: coordinator file corrupt")
	// ErrCoordOpExists reports an attempt to prepare or
	// publish an operation ID that already has staged
	// files or a published root. The caller must recover,
	// not overwrite the existing operation.
	ErrCoordOpExists = errors.New("yowza coordinator: operation already staged or committed")
	// ErrCoordOpCommitted reports an attempt to abort an
	// operation whose root is already published. A
	// committed operation must be recovered, never
	// aborted.
	ErrCoordOpCommitted = errors.New("yowza coordinator: operation already committed, run recovery")
	// ErrCoordLeaseNotOwner reports a lease release by a
	// caller that is not the lease's owner.
	ErrCoordLeaseNotOwner = errors.New("yowza coordinator: lease release by non-owner refused")
	// ErrCoordLeaseStaleEpoch reports a lease release
	// whose epoch does not match the lease's epoch.
	ErrCoordLeaseStaleEpoch = errors.New("yowza coordinator: lease release with stale epoch refused")
	// ErrCoordLeaseNotFound reports a release (or query)
	// for a lease ID with no record.
	ErrCoordLeaseNotFound = errors.New("yowza coordinator: lease not found")
	// ErrCoordLeaseExists reports a lease acquisition for
	// an ID already held by a live lease.
	ErrCoordLeaseExists = errors.New("yowza coordinator: lease id already held by a live lease")
	// ErrCoordWALCorrupt reports a shared-WAL frame whose
	// checksum, sequence, or payload fails validation.
	ErrCoordWALCorrupt = errors.New("yowza coordinator: shared WAL corrupt")
)

// coordOwner is the persisted owner record. The checksum
// covers the canonical JSON of {version, owner_id, epoch,
// acquired_unix_nano} (checksum field itself excluded).
type coordOwner struct {
	Version          int    `json:"version"`
	OwnerID          string `json:"owner_id"`
	Epoch            uint64 `json:"epoch"`
	AcquiredUnixNano int64  `json:"acquired_unix_nano"`
	Checksum         uint32 `json:"checksum"`
}

func coordOwnerChecksum(o *coordOwner) uint32 {
	body, _ := json.Marshal(struct {
		Version          int    `json:"version"`
		OwnerID          string `json:"owner_id"`
		Epoch            uint64 `json:"epoch"`
		AcquiredUnixNano int64  `json:"acquired_unix_nano"`
	}{Version: o.Version, OwnerID: o.OwnerID, Epoch: o.Epoch, AcquiredUnixNano: o.AcquiredUnixNano})
	return crc32.ChecksumIEEE(body)
}

func coordOwnerEncode(o *coordOwner) ([]byte, error) {
	o.Checksum = coordOwnerChecksum(o)
	return json.Marshal(o)
}

func coordOwnerDecode(data []byte) (*coordOwner, error) {
	var o coordOwner
	if err := json.Unmarshal(data, &o); err != nil {
		return nil, fmt.Errorf("%w: owner record unparseable: %v", ErrCoordCorrupt, err)
	}
	if o.Version != 1 || o.OwnerID == "" || o.Epoch == 0 {
		return nil, fmt.Errorf("%w: owner record version/owner/epoch invalid", ErrCoordCorrupt)
	}
	if coordOwnerChecksum(&o) != o.Checksum {
		return nil, fmt.Errorf("%w: owner record checksum mismatch", ErrCoordCorrupt)
	}
	return &o, nil
}

// CoordinatorSession is one live ownership session. It holds
// the coordinator flock until Release. It is safe for
// concurrent use by goroutines in the owning process: the
// session mutex serialises WAL appends and root publishes,
// so concurrent appends from one session form a single
// ordered stream, never interleaved frames.
type CoordinatorSession struct {
	dir      string
	ownerID  string
	epoch    uint64
	lockFile *os.File
	mu       sync.Mutex
	active   bool
	nextSeq  uint64 // last shared-WAL sequence issued (0 = none)
}

// Dir returns the coordinator directory.
func (s *CoordinatorSession) Dir() string { return s.dir }

// OwnerID returns the session's owner identity.
func (s *CoordinatorSession) OwnerID() string { return s.ownerID }

// Epoch returns the session's fencing epoch.
func (s *CoordinatorSession) Epoch() uint64 { return s.epoch }

func (s *CoordinatorSession) lockPath() string  { return filepath.Join(s.dir, "coord.lock") }
func (s *CoordinatorSession) ownerPath() string { return filepath.Join(s.dir, "coord-owner.json") }
func (s *CoordinatorSession) walPath() string   { return filepath.Join(s.dir, "coord-wal.log") }

// coordWriteAtomic writes data via tmp + sync + rename +
// dir sync. Decision files (owner, manifest, root) are
// always fsynced: coordinator durability does not degrade
// with any DB's durability mode.
func coordWriteAtomic(dir, path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
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
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	return persistSyncDir(dir)
}

// coordReadOwner reads and validates the persisted owner
// record. A missing record is ErrCoordCorrupt (operator
// damage: ownership state must never be silently invented).
func coordReadOwner(dir string) (*coordOwner, error) {
	data, err := os.ReadFile(filepath.Join(dir, "coord-owner.json"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%w: owner record missing", ErrCoordCorrupt)
		}
		return nil, err
	}
	return coordOwnerDecode(data)
}

// CurrentCoordOwner reports the current owner without
// acquiring the coordinator. It is what lease liveness
// checks read: a lease is live exactly when its owner and
// epoch still match this record.
func CurrentCoordOwner(dir string) (ownerID string, epoch uint64, err error) {
	o, err := coordReadOwner(dir)
	if err != nil {
		return "", 0, err
	}
	return o.OwnerID, o.Epoch, nil
}

// AcquireCoordinator takes coordinator ownership in dir.
// ownerID must be non-empty and stable for the session's
// lifetime (it is the identity leases and WAL frames
// record). The call fails loudly with ErrCoordLocked if
// another session holds the coordinator, and with
// ErrCoordWALCorrupt / ErrCoordCorrupt if persisted
// coordinator state fails validation — it never proceeds
// on unchecked state. A torn WAL tail (truncated LAST
// frame only) is truncated here, before any new sequence
// is issued, so the tail can never be followed by a valid
// frame that would make the corruption look like a middle
// frame.
func AcquireCoordinator(dir, ownerID string) (*CoordinatorSession, error) {
	if ownerID == "" {
		return nil, fmt.Errorf("yowza coordinator: empty owner id")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	lockFile, err := os.OpenFile(filepath.Join(dir, "coord.lock"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lockFile.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("%w: %s", ErrCoordLocked, dir)
		}
		return nil, err
	}
	releaseLock := func() {
		_ = syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN)
		_ = lockFile.Close()
	}
	// Previous owner record, if any, determines the next epoch.
	var prevEpoch uint64
	if data, err := os.ReadFile(filepath.Join(dir, "coord-owner.json")); err == nil {
		prev, err := coordOwnerDecode(data)
		if err != nil {
			releaseLock()
			return nil, err
		}
		prevEpoch = prev.Epoch
	} else if !os.IsNotExist(err) {
		releaseLock()
		return nil, err
	}
	// Validate + truncate a torn WAL tail before issuing
	// any sequence. A corrupt middle frame fails the
	// acquire outright.
	_, lastSeq, tailTruncated, lastValidOffset, err := scanCoordWAL(filepath.Join(dir, "coord-wal.log"))
	if err != nil {
		releaseLock()
		return nil, err
	}
	if tailTruncated {
		if err := coordTruncateWAL(filepath.Join(dir, "coord-wal.log"), lastValidOffset); err != nil {
			releaseLock()
			return nil, err
		}
	}
	next := &coordOwner{Version: 1, OwnerID: ownerID, Epoch: prevEpoch + 1, AcquiredUnixNano: time.Now().UnixNano()}
	data, err := coordOwnerEncode(next)
	if err != nil {
		releaseLock()
		return nil, err
	}
	if err := coordWriteAtomic(dir, filepath.Join(dir, "coord-owner.json"), data); err != nil {
		releaseLock()
		return nil, err
	}
	return &CoordinatorSession{dir: dir, ownerID: ownerID, epoch: next.Epoch, lockFile: lockFile, active: true, nextSeq: lastSeq}, nil
}

// Validate checks fencing for this session: the persisted
// owner record must still name this owner at this epoch.
// Epoch mismatch is checked BEFORE the released flag, so a
// session released and then superseded reports the stale
// epoch (the takeover is the fencing event), while a
// session released without a takeover reports Released.
// Either way the caller must not commit.
func (s *CoordinatorSession) Validate() error {
	if s == nil {
		return fmt.Errorf("%w: nil session", ErrCoordReleased)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.validateLocked()
}

// validateLocked is Validate under the session mutex.
// It never consults the flock (a held flock proves this
// process holds the lock file, not that the epoch is
// still current — the owner record is the authority).
func (s *CoordinatorSession) validateLocked() error {
	cur, err := coordReadOwner(s.dir)
	if err != nil {
		return err
	}
	if cur.OwnerID != s.ownerID || cur.Epoch != s.epoch {
		return fmt.Errorf("%w: session %s epoch %d, current %s epoch %d", ErrCoordStaleEpoch, s.ownerID, s.epoch, cur.OwnerID, cur.Epoch)
	}
	if !s.active {
		return fmt.Errorf("%w: session %s epoch %d", ErrCoordReleased, s.ownerID, s.epoch)
	}
	return nil
}

// Release gives up ownership. It is idempotent: releasing
// twice is not an error, but a released session must never
// commit again (Validate reports Released, or Stale after
// a takeover — see Validate).
func (s *CoordinatorSession) Release() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.active {
		return nil
	}
	s.active = false
	f := s.lockFile
	s.lockFile = nil
	_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	_ = f.Close()
	return nil
}
