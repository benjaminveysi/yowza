package query

// Phase D (Timeline-v2) — Query ISA under a timeline
// handle.
//
// The C ISA engine is single-shard: it reads one SegDB
// at one literal snapshot sequence (see
// yowza_queryisa_execute_at). An ISATimelineHandle is
// that snapshot PLUS its lease, and ExecuteISAAtTimeline
// enforces both halves before any row is produced:
//   - the lease must be live in the C lease table
//     (yowza_segdb_lease_valid), or the query fails
//     with yowza.ErrLeaseExpired — never with rows from
//     a snapshot whose pin is gone;
//   - the snapshot must not exceed the host SegDB's
//     current sequence, or it fails with
//     yowza.ErrSnapshotInvalid — never by silently
//     reading the current head.
// The checks and the execution run under the host lock
// (executeISALockedPolicy, the same locked path
// ExecuteISAAt uses), so a main-head advance between
// the check and the execution cannot leak in: the
// engine still reads the handle's literal snapshot.
// A query WITHOUT a handle (ExecuteISA / ExecuteISATrace)
// takes a fresh snapshot and therefore reads main —
// that is the no-handle path, and the differential test
// proves it does not see timeline-only state (timeline
// writes live in the Go handle's write set, never in
// the ISA host's SegDB, so there is nothing for it to
// see; the negative test asserts exactly that).
//
// Multi-shard timelines do NOT run through this file:
// the C engine has no multi-shard snapshot, so a
// multi-shard timeline query runs through the Go
// engine over TimelineSource (timeline_source.go),
// which is the coordinator for that shape. Claiming
// this single-shard handle covers multi-shard ISA
// would be false, and this file does not claim it.

import (
	"context"
	"fmt"

	yowza "github.com/yowza/yowza"
)

// ISATimelineHandle is one ISA query's snapshot point:
// a literal sequence plus the C lease pinning it. A
// zero LeaseID means "caller owns pinning" (e.g. a
// durable pin); the future-sequence check still applies.
type ISATimelineHandle struct {
	Snapshot uint64
	LeaseID  uint64
}

// AcquireISATimelineHandle captures the host's current
// sequence and pins it with a fresh C lease. The
// caller owns ReleaseISATimelineHandle.
func (h *ISAHost) AcquireISATimelineHandle() (ISATimelineHandle, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	snap, err := h.callU64(h.fnSnapshot, h.stateOff)
	if err != nil {
		return ISATimelineHandle{}, fmt.Errorf("isa timeline: snapshot call failed: %v", err)
	}
	fn := h.module.ExportedFunction("yowza_segdb_lease_acquire")
	if fn == nil {
		return ISATimelineHandle{}, fmt.Errorf("isa timeline: wasm binary predates lease exports (missing yowza_segdb_lease_acquire)")
	}
	lease, err := h.callU64(fn, h.stateOff, snap)
	if err != nil || lease == 0 {
		return ISATimelineHandle{}, fmt.Errorf("isa timeline: lease acquire failed for snapshot %d (table full?): %v", snap, err)
	}
	return ISATimelineHandle{Snapshot: snap, LeaseID: lease}, nil
}

// ReleaseISATimelineHandle releases a handle's lease.
// Idempotence is the C table's (double release returns
// an error there, so callers release once).
func (h *ISAHost) ReleaseISATimelineHandle(handle ISATimelineHandle) error {
	if handle.LeaseID == 0 {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	fn := h.module.ExportedFunction("yowza_segdb_lease_release")
	if fn == nil {
		return fmt.Errorf("isa timeline: wasm binary predates lease exports (missing yowza_segdb_lease_release)")
	}
	rc, err := h.call(fn, h.stateOff, handle.LeaseID)
	if err != nil {
		return err
	}
	if rc != 0 {
		return fmt.Errorf("isa timeline: lease release failed (rc=%d)", rc)
	}
	return nil
}

// ExecuteISAAtTimeline runs a compiled ISA program at
// a timeline handle's snapshot, under an optional
// resource policy. It fails before executing when the
// handle is expired or futuristic (see the file
// header); on success it returns rows + trace exactly
// as ExecuteISAAtWithPolicy does.
func (h *ISAHost) ExecuteISAAtTimeline(program []byte, plan *Plan, handle ISATimelineHandle, policy *ResourcePolicy) ([]Row, ISATrace, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if policy != nil {
		if err := policy.Validate(); err != nil {
			return nil, ISATrace{}, err
		}
	}
	// Lease check first: an expired pin fails before
	// any row, never with partial data.
	if handle.LeaseID != 0 {
		fn := h.module.ExportedFunction("yowza_segdb_lease_valid")
		if fn == nil {
			return nil, ISATrace{}, fmt.Errorf("isa timeline: wasm binary predates lease exports (missing yowza_segdb_lease_valid)")
		}
		valid, err := h.callU64(fn, h.stateOff, handle.LeaseID)
		if err != nil {
			return nil, ISATrace{}, err
		}
		if valid != 1 {
			return nil, ISATrace{}, fmt.Errorf("%w: isa timeline lease %d", yowza.ErrLeaseExpired, handle.LeaseID)
		}
	}
	// Future check second: a snapshot above current
	// fails, never reads as current.
	cur, err := h.callU64(h.fnSnapshot, h.stateOff)
	if err != nil {
		return nil, ISATrace{}, fmt.Errorf("isa timeline: snapshot call failed: %v", err)
	}
	if handle.Snapshot > cur {
		return nil, ISATrace{}, fmt.Errorf("%w: isa timeline snapshot %d above current %d", yowza.ErrSnapshotInvalid, handle.Snapshot, cur)
	}
	return h.executeISALockedPolicy(program, plan, handle.Snapshot, policy)
}

// ExecuteISAAtTimelineContext is the context-aware
// wrapper (context cancellation is honoured before
// execution starts; the locked execution itself is
// synchronous, as with the other ISAHost methods).
func (h *ISAHost) ExecuteISAAtTimelineContext(ctx context.Context, program []byte, plan *Plan, handle ISATimelineHandle, policy *ResourcePolicy) ([]Row, ISATrace, error) {
	if err := ctx.Err(); err != nil {
		return nil, ISATrace{}, err
	}
	return h.ExecuteISAAtTimeline(program, plan, handle, policy)
}
