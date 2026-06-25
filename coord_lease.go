package yowza

// Axis B — SHARED LEASE TABLE (cross-process pins).
//
// Snapshot pins and branches must be identifiable across
// processes: the C lease table is per-process, so a second
// process's compaction (or reclamation) cannot see the
// first process's pins from that table alone. The
// coordinator lease table (coord-leases.json in the
// coordinator directory) is the shared record: one JSON
// document, CRC-32 sealed, rewritten atomically
// (tmp + sync + rename + dir sync) under a short flock
// on coord-leases.lock for every mutation.
//
// A lease carries owner identity + epoch:
//
//	{lease_id, owner_id, epoch, pinned[], created_unix_nano}
//
// Rules, all loud:
//
//   - Acquire requires a live coordinator session
//     (fencing re-checked). A duplicate ID held by a
//     LIVE lease is refused (ErrCoordLeaseExists).
//     A duplicate ID held only by a STALE lease (its
//     owner/epoch no longer match the current owner
//     record — ownership moved on) may be replaced:
//     a stale lease no longer protects anything, and
//     refusing forever would leak the ID after every
//     takeover.
//   - Release requires the caller to present exactly
//     the lease's owner ID and epoch. A non-owner
//     release is refused with ErrCoordLeaseNotOwner
//     and the lease stays in place; a wrong-epoch
//     release (including the original owner after a
//     takeover) is refused with ErrCoordLeaseStaleEpoch
//     and the lease stays in place. A release by
//     another process therefore cannot cause unwanted
//     data deletion: the lease, and the data it pins,
//     survive the refused attempt untouched.
//   - Reclamation (ReclaimPinnedFiles) deletes only
//     files pinned by NO live lease. A lease is live
//     exactly when its owner/epoch match the current
//     owner record. A file pinned by a live lease is
//     kept even when another process performs the
//     reclamation — the reclaimer reads the same table,
//     under the same lease lock, so it cannot miss a
//     live pin that was acquired before the reclaim
//     started. (A pin acquired concurrently with a
//     reclaim is serialised by the lease lock: either
//     the pin lands first and the file is kept, or the
//     reclaim deletes first and the later pin names a
//     file that is already gone — the pin acquirer
//     must check the file still exists after acquiring,
//     which AcquireCoordLease does not do for it: the
//     caller pins only files it has just verified, and
//     reclamation is an operator step, not a concurrent
//     background sweep. This boundary is stated, not
//     hidden.)
//
// Pinned entries are basenames in ONE data directory
// supplied to ReclaimPinnedFiles. The lease does not
// itself delete or protect files outside that
// directory: the caller passes the directory whose
// files the lease is about.

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"syscall"
	"time"
)

// CoordLease is one shared lease record.
type CoordLease struct {
	ID              string   `json:"lease_id"`
	OwnerID         string   `json:"owner_id"`
	Epoch           uint64   `json:"epoch"`
	Pinned          []string `json:"pinned"`
	CreatedUnixNano int64    `json:"created_unix_nano"`
}

type coordLeaseTable struct {
	Version  int          `json:"version"`
	Leases   []CoordLease `json:"leases"`
	Checksum uint32       `json:"checksum"`
}

func coordLeaseTableChecksum(t *coordLeaseTable) uint32 {
	body, _ := json.Marshal(struct {
		Version int          `json:"version"`
		Leases  []CoordLease `json:"leases"`
	}{Version: t.Version, Leases: t.Leases})
	return crc32.ChecksumIEEE(body)
}

func coordLeaseTableEncode(t *coordLeaseTable) ([]byte, error) {
	t.Checksum = coordLeaseTableChecksum(t)
	return json.Marshal(t)
}

func coordLeaseTableDecode(data []byte) (*coordLeaseTable, error) {
	var t coordLeaseTable
	if err := json.Unmarshal(data, &t); err != nil {
		return nil, fmt.Errorf("%w: lease table unparseable: %v", ErrCoordCorrupt, err)
	}
	if t.Version != 1 {
		return nil, fmt.Errorf("%w: lease table version %d", ErrCoordCorrupt, t.Version)
	}
	if t.Leases == nil {
		t.Leases = []CoordLease{}
	}
	if coordLeaseTableChecksum(&t) != t.Checksum {
		return nil, fmt.Errorf("%w: lease table checksum mismatch", ErrCoordCorrupt)
	}
	return &t, nil
}

func coordLeaseLockPath(dir string) string  { return filepath.Join(dir, "coord-leases.lock") }
func coordLeaseTablePath(dir string) string { return filepath.Join(dir, "coord-leases.json") }

// withCoordLeaseTable runs fn with the lease table held
// under the short lease flock. fn's returned table, when
// non-nil, is published atomically; a nil table means
// read-only (no publish). The lock is held for the whole
// read-mutate-publish, so no two processes can interleave
// a lease acquire/release/reclaim decision.
func withCoordLeaseTable(dir string, fn func(t *coordLeaseTable) (*coordLeaseTable, error)) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	lf, err := os.OpenFile(coordLeaseLockPath(dir), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer lf.Close()
	if err := syscall.Flock(int(lf.Fd()), syscall.LOCK_EX); err != nil {
		return err
	}
	defer syscall.Flock(int(lf.Fd()), syscall.LOCK_UN)
	var tbl *coordLeaseTable
	if data, err := os.ReadFile(coordLeaseTablePath(dir)); err == nil {
		tbl, err = coordLeaseTableDecode(data)
		if err != nil {
			return err
		}
	} else if !os.IsNotExist(err) {
		return err
	} else {
		tbl = &coordLeaseTable{Version: 1, Leases: []CoordLease{}}
	}
	out, err := fn(tbl)
	if err != nil {
		return err
	}
	if out == nil {
		return nil
	}
	data, err := coordLeaseTableEncode(out)
	if err != nil {
		return err
	}
	return coordWriteAtomic(dir, coordLeaseTablePath(dir), data)
}

// coordLeaseLiveLocked reports whether lease l is live
// against the current owner record (caller need not hold
// the lease lock for this pure comparison; callers that
// already hold it pass the record they read).
func coordLeaseLive(l CoordLease, ownerID string, epoch uint64) bool {
	return l.OwnerID == ownerID && l.Epoch == epoch
}

// AcquireCoordLease records a lease pinning the given
// basenames under this session's owner/epoch. Pinned
// names must be plain basenames (no separators); a name
// with a separator is refused rather than recorded, so a
// lease can never pin a path outside its data directory
// by construction.
func AcquireCoordLease(sess *CoordinatorSession, dir, leaseID string, pinned []string) (*CoordLease, error) {
	if sess == nil {
		return nil, fmt.Errorf("%w: nil session", ErrCoordReleased)
	}
	if leaseID == "" {
		return nil, fmt.Errorf("yowza coordinator: empty lease id")
	}
	for _, p := range pinned {
		if p == "" || p == "." || p == ".." || filepath.Base(p) != p {
			return nil, fmt.Errorf("yowza coordinator: pinned name %q is not a plain basename", p)
		}
	}
	if err := sess.Validate(); err != nil {
		return nil, err
	}
	var acquired *CoordLease
	err := withCoordLeaseTable(dir, func(tbl *coordLeaseTable) (*coordLeaseTable, error) {
		ownerID, epoch, err := CurrentCoordOwner(dir)
		if err != nil {
			return nil, err
		}
		// The session validated just above; if ownership
		// moved between validation and this lock, the
		// owner record no longer matches the session and
		// the acquire must fail here, not record a lease
		// under a dead epoch.
		if ownerID != sess.ownerID || epoch != sess.epoch {
			return nil, fmt.Errorf("%w: session %s epoch %d, current %s epoch %d", ErrCoordStaleEpoch, sess.ownerID, sess.epoch, ownerID, epoch)
		}
		pins := append([]string(nil), pinned...)
		sort.Strings(pins)
		// Deduplicate pinned names.
		uniq := pins[:0]
		for i, p := range pins {
			if i == 0 || p != pins[i-1] {
				uniq = append(uniq, p)
			}
		}
		nl := CoordLease{ID: leaseID, OwnerID: sess.ownerID, Epoch: sess.epoch, Pinned: uniq, CreatedUnixNano: coordNowNano()}
		for i, l := range tbl.Leases {
			if l.ID != leaseID {
				continue
			}
			if coordLeaseLive(l, ownerID, epoch) {
				return nil, fmt.Errorf("%w: lease %s held by %s epoch %d", ErrCoordLeaseExists, leaseID, l.OwnerID, l.Epoch)
			}
			// Stale duplicate: replace it (see file header).
			tbl.Leases[i] = nl
			cp := nl
			acquired = &cp
			return tbl, nil
		}
		tbl.Leases = append(tbl.Leases, nl)
		cp := nl
		acquired = &cp
		return tbl, nil
	})
	if err != nil {
		return nil, err
	}
	return acquired, nil
}

// ReleaseCoordLease deletes a lease, but only when the
// caller presents exactly the lease's owner ID and
// epoch. Every other caller is refused and the lease
// (and its pinned data) survives untouched.
func ReleaseCoordLease(dir, leaseID, ownerID string, epoch uint64) error {
	if leaseID == "" {
		return fmt.Errorf("yowza coordinator: empty lease id")
	}
	return withCoordLeaseTable(dir, func(tbl *coordLeaseTable) (*coordLeaseTable, error) {
		for i, l := range tbl.Leases {
			if l.ID != leaseID {
				continue
			}
			if l.OwnerID != ownerID {
				return nil, fmt.Errorf("%w: lease %s owned by %s, release attempted by %s", ErrCoordLeaseNotOwner, leaseID, l.OwnerID, ownerID)
			}
			if l.Epoch != epoch {
				return nil, fmt.Errorf("%w: lease %s epoch %d, release epoch %d", ErrCoordLeaseStaleEpoch, leaseID, l.Epoch, epoch)
			}
			tbl.Leases = append(tbl.Leases[:i], tbl.Leases[i+1:]...)
			return tbl, nil
		}
		return nil, fmt.Errorf("%w: lease %s", ErrCoordLeaseNotFound, leaseID)
	})
}

// ReleaseCoordLeaseForSession releases a lease held by
// this session (fencing re-checked first).
func ReleaseCoordLeaseForSession(sess *CoordinatorSession, dir, leaseID string) error {
	if sess == nil {
		return fmt.Errorf("%w: nil session", ErrCoordReleased)
	}
	if err := sess.Validate(); err != nil {
		return err
	}
	return ReleaseCoordLease(dir, leaseID, sess.ownerID, sess.epoch)
}

// ListCoordLeases returns a copy of the current lease
// table (read under the lease lock, so the snapshot is
// not torn by a concurrent mutation).
func ListCoordLeases(dir string) ([]CoordLease, error) {
	var out []CoordLease
	err := withCoordLeaseTable(dir, func(tbl *coordLeaseTable) (*coordLeaseTable, error) {
		out = append([]CoordLease(nil), tbl.Leases...)
		return nil, nil
	})
	if err != nil {
		return nil, err
	}
	if out == nil {
		out = []CoordLease{}
	}
	return out, nil
}

// IsCoordLeaseLive reports whether the named lease is
// live (its owner/epoch still match the current owner
// record). An unknown lease is not live (found=false).
// An owner-record read failure is a loud error, never a
// silent "not live" that would authorise deletion.
func IsCoordLeaseLive(dir, leaseID string) (live bool, found bool, err error) {
	ownerID, epoch, err := CurrentCoordOwner(dir)
	if err != nil {
		return false, false, err
	}
	leases, err := ListCoordLeases(dir)
	if err != nil {
		return false, false, err
	}
	for _, l := range leases {
		if l.ID == leaseID {
			return coordLeaseLive(l, ownerID, epoch), true, nil
		}
	}
	return false, false, nil
}

// ReclaimPinnedFiles deletes, from dataDir, exactly the
// basenames that are pinned by NO live lease, and keeps
// every basename pinned by a live lease. The live set is
// computed under the lease lock and the deletions happen
// under the same lock, so a live pin acquired before the
// reclaim started cannot be missed. Missing files count
// as deleted (nothing remains to delete). A deletion
// failure for any other reason stops loudly with the
// partial deleted/kept lists in the error text's caller
// values (returned alongside the error).
func ReclaimPinnedFiles(coordDir, dataDir string, basenames []string) (deleted, kept []string, err error) {
	err = withCoordLeaseTable(coordDir, func(tbl *coordLeaseTable) (*coordLeaseTable, error) {
		ownerID, epoch, err := CurrentCoordOwner(coordDir)
		if err != nil {
			return nil, err
		}
		livePinned := map[string]bool{}
		for _, l := range tbl.Leases {
			if !coordLeaseLive(l, ownerID, epoch) {
				continue
			}
			for _, p := range l.Pinned {
				livePinned[p] = true
			}
		}
		for _, base := range basenames {
			if filepath.Base(base) != base || base == "." || base == ".." {
				return nil, fmt.Errorf("yowza coordinator: reclaim name %q is not a plain basename", base)
			}
			if livePinned[base] {
				kept = append(kept, base)
				continue
			}
			if err := os.Remove(filepath.Join(dataDir, base)); err != nil && !errors.Is(err, os.ErrNotExist) {
				return nil, fmt.Errorf("yowza coordinator: reclaim %s: %w", base, err)
			}
			deleted = append(deleted, base)
		}
		return nil, nil
	})
	if err != nil {
		return deleted, kept, err
	}
	sort.Strings(deleted)
	sort.Strings(kept)
	return deleted, kept, nil
}

func coordNowNano() int64 {
	// time.Now is used only for diagnostics (lease
	// creation order); fencing never depends on the
	// clock, only on owner + epoch.
	return time.Now().UnixNano()
}
