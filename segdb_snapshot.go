package yowza

// Phase B (Timeline-v2) — shared snapshot and mutation contract.
//
// B1: a snapshot is an OWNED object, not a bare vector plus a
// separately registered lease. AcquireSnapshot captures the
// per-shard vector and registers ALL shards' leases under p.mu
// in ONE atomic operation and returns the handle only after the
// whole operation succeeds; if lease registration on one shard
// fails, all previously registered leases are released. The
// handle owns those leases until Close (idempotent). A handle
// closed after its database is already closed skips release
// (releasing through a closed wasm runtime is unsafe — the
// Timeline close-after-DB incident in PARALLEL_TIMELINE_NOTES
// §6.8); the leases die with the module anyway.
//
// B2 (handle side): a SnapshotHandle is the temporary-lease
// half of the contract. The DURABLE half is the pin in
// segdb_pins.go: PinSnapshot takes a handle's vector and
// registers a SEPARATE set of leases owned by the pin, so
// closing the handle never drops a published pin.
//
// B4/O4: AcquireShardLeases is the public multi-shard lease
// API for external callers: no caller outside this package
// needs to reach a map or a private field to pin a vector.
// It returns the per-shard lease IDs and an idempotent
// release function (safe after DB close: it then no-ops).
//
// Locking: p.mu -> shard leaseMu -> host.mu, exactly the
// order ScanStreaming already uses. No method here holds p.mu
// across a wasm scan call; vector capture and lease
// registration happen under p.mu, scans happen after it is
// released (lease validity is re-checked per page by the
// iterator / per call by Get).

import (
	"errors"
	"fmt"
	"sync"
)

// SnapshotID identifies one owned snapshot handle. IDs are
// per-database, monotonically increasing, never reused within
// one open instance.
type SnapshotID uint64

// SnapshotIterator is the page interface a snapshot scan
// returns. PersistedScanIter satisfies it (NextBatch/Close).
type SnapshotIterator interface {
	NextBatch(batchSize int) ([]SegDBScanEntry, error)
	Close() error
}

// SnapshotHandle is the owned snapshot object (B1). Vector is
// a copy; mutating the returned slice never affects the
// handle. Get reads ONE key at the snapshot (the key's own
// shard only). Scan returns a streaming iterator at the
// snapshot; the iterator registers its OWN leases, so it
// survives the handle's Close.
type SnapshotHandle interface {
	ID() SnapshotID
	Vector() []uint64
	Get(key []byte) ([]byte, error)
	Scan(prefix []byte) (SnapshotIterator, error)
	Close() error
}

var _ SnapshotHandle = (*snapshotHandle)(nil)
var _ SnapshotIterator = (*PersistedScanIter)(nil)

// ErrSnapshotHandleClosed is returned by Get/Scan on a
// closed handle.
var ErrSnapshotHandleClosed = errors.New("yowza persist: snapshot handle closed")

type snapshotHandle struct {
	db         *PersistedSegDB
	id         SnapshotID
	vector     []uint64
	generation uint64 // PersistedSegDB checkpoint generation at capture
	leaseIDs   []uint64
	mu         sync.Mutex
	closed     bool
}

func (h *snapshotHandle) ID() SnapshotID { return h.id }

func (h *snapshotHandle) Vector() []uint64 {
	return append([]uint64(nil), h.vector...)
}

// snapshotVectorAndLeasesLocked captures the vector and
// registers every shard's lease. Caller holds p.mu. On any
// failure, previously registered leases are released and a
// nil vector is returned. This is the single atomic
// acquire used by AcquireSnapshot, AcquireShardLeases and
// pin creation, so the three paths cannot drift apart.
func (p *PersistedSegDB) snapshotVectorAndLeasesLocked() ([]uint64, []uint64, error) {
	if p.closed {
		return nil, nil, ErrPersistedClosed
	}
	vec := make([]uint64, len(p.shards))
	for i, sh := range p.shards {
		seq, err := sh.sdb.SnapshotSeq()
		if err != nil {
			return nil, nil, fmt.Errorf("yowza persist: shard %d snapshot seq: %w", i, err)
		}
		vec[i] = seq
	}
	leaseIDs := make([]uint64, len(p.shards))
	for i, sh := range p.shards {
		id, err := sh.sdb.RegisterLease(vec[i])
		if err != nil {
			for j := 0; j < i; j++ {
				_ = p.shards[j].sdb.ReleaseLease(leaseIDs[j])
			}
			return nil, nil, fmt.Errorf("yowza persist: shard %d lease for snapshot %d: %w", i, vec[i], err)
		}
		leaseIDs[i] = id
	}
	return vec, leaseIDs, nil
}

// registerLeasesForVectorLocked registers leases for an
// already-captured vector (e.g. a handle's vector, for a pin
// that must outlive the handle). Caller holds p.mu; on
// failure, previously registered leases are released.
func (p *PersistedSegDB) registerLeasesForVectorLocked(vector []uint64) ([]uint64, error) {
	if p.closed {
		return nil, ErrPersistedClosed
	}
	if len(vector) != len(p.shards) {
		return nil, fmt.Errorf("%w: vector has %d entries, database has %d shards", ErrPersistedSnapshotMismatch, len(vector), len(p.shards))
	}
	leaseIDs := make([]uint64, len(p.shards))
	for i, sh := range p.shards {
		id, err := sh.sdb.RegisterLease(vector[i])
		if err != nil {
			for j := 0; j < i; j++ {
				_ = p.shards[j].sdb.ReleaseLease(leaseIDs[j])
			}
			return nil, fmt.Errorf("yowza persist: shard %d lease for snapshot %d: %w", i, vector[i], err)
		}
		leaseIDs[i] = id
	}
	return leaseIDs, nil
}

// releaseShardLeasesLocked releases one lease per shard.
// Caller holds p.mu (or the DB is being closed and hosts are
// still alive). Errors are collected; the first is returned.
// Callers that must never touch a closed runtime check
// p.closed BEFORE calling this (see snapshotHandle.Close
// and the AcquireShardLeases release function).
func (p *PersistedSegDB) releaseShardLeasesLocked(leaseIDs []uint64) error {
	var first error
	for i, id := range leaseIDs {
		if i >= len(p.shards) {
			break
		}
		if err := p.shards[i].sdb.ReleaseLease(id); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// AcquireSnapshot captures the current snapshot vector and
// registers all shards' leases atomically under p.mu, and
// returns the owned handle. See the file header.
func (p *PersistedSegDB) AcquireSnapshot() (SnapshotHandle, error) {
	// PATCH2 Track 1: a snapshot must not pin a state
	// whose recovery is incomplete.
	if err := p.checkReadsAllowed(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	vec, leaseIDs, err := p.snapshotVectorAndLeasesLocked()
	if err != nil {
		return nil, err
	}
	p.snapshotNext++
	return &snapshotHandle{
		db:         p,
		id:         SnapshotID(p.snapshotNext),
		vector:     vec,
		generation: p.generation,
		leaseIDs:   leaseIDs,
	}, nil
}

// AcquireShardLeases (O4) pins an explicit snapshot vector
// from OUTSIDE this package: it returns the per-shard lease
// IDs (index-aligned with the vector) and an idempotent
// release function. Registration is atomic: on any shard
// failure, earlier leases are released and no IDs are
// returned. The release function is safe to call after the
// database is closed — it then no-ops, because the leases
// died with the wasm modules and a release through a closed
// runtime is unsafe.
func (p *PersistedSegDB) AcquireShardLeases(vector []uint64) ([]uint64, func() error, error) {
	p.mu.Lock()
	leaseIDs, err := p.registerLeasesForVectorLocked(vector)
	p.mu.Unlock()
	if err != nil {
		return nil, nil, err
	}
	out := append([]uint64(nil), leaseIDs...)
	var once sync.Mutex
	released := false
	release := func() error {
		once.Lock()
		defer once.Unlock()
		if released {
			return nil
		}
		released = true
		p.mu.Lock()
		defer p.mu.Unlock()
		if p.closed {
			return nil
		}
		return p.releaseShardLeasesLocked(out)
	}
	return out, release, nil
}

func (h *snapshotHandle) checkOpen() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return ErrSnapshotHandleClosed
	}
	return nil
}

// Get reads one key at the handle's snapshot. Only the
// key's own shard is consulted (keys route to exactly one
// shard, so there is no cross-shard duplicate to resolve).
// The read goes through ScanAt on that shard at the
// snapshot sequence and filters to the exact key, because
// SegDB exposes no Get-at-sequence export; a prefix match
// that is not the exact key is never returned. A lease that
// is no longer valid fails with ErrLeaseExpired, never
// partial data.
func (h *snapshotHandle) Get(key []byte) ([]byte, error) {
	if err := h.checkOpen(); err != nil {
		return nil, err
	}
	if len(key) == 0 || len(key) > 1023 {
		return nil, fmt.Errorf("yowza persist: snapshot get key length %d out of range", len(key))
	}
	h.db.mu.Lock()
	closed := h.db.closed
	h.db.mu.Unlock()
	if closed {
		return nil, ErrPersistedClosed
	}
	idx := h.db.ShardIndex(string(key))
	sh := h.db.shards[idx]
	if !sh.sdb.LeaseValid(h.leaseIDs[idx]) {
		return nil, fmt.Errorf("%w: snapshot shard %d lease", ErrLeaseExpired, idx)
	}
	entries, err := sh.sdb.ScanAt(string(key), h.vector[idx])
	if err != nil {
		return nil, fmt.Errorf("yowza persist: snapshot shard %d scan: %w", idx, err)
	}
	for _, e := range entries {
		if string(e.Key) == string(key) {
			return append([]byte(nil), e.Val...), nil
		}
	}
	return nil, fmt.Errorf("yowza persist: key not found at snapshot %d", h.id)
}

// Scan returns a streaming iterator over prefix at the
// handle's snapshot. The iterator registers its OWN shard
// leases (via ScanStreaming), so closing the handle does
// not invalidate an already-returned iterator.
func (h *snapshotHandle) Scan(prefix []byte) (SnapshotIterator, error) {
	if err := h.checkOpen(); err != nil {
		return nil, err
	}
	if len(prefix) > 1023 {
		return nil, fmt.Errorf("yowza persist: snapshot scan prefix too long")
	}
	return h.db.ScanStreaming(string(prefix), h.Vector())
}

// Close releases the handle's leases exactly once. It is
// idempotent. If the database is already closed, release is
// skipped (leases die with the module; see file header).
func (h *snapshotHandle) Close() error {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil
	}
	h.closed = true
	leaseIDs := append([]uint64(nil), h.leaseIDs...)
	h.mu.Unlock()
	h.db.mu.Lock()
	defer h.db.mu.Unlock()
	if h.db.closed {
		return nil
	}
	return h.db.releaseShardLeasesLocked(leaseIDs)
}
