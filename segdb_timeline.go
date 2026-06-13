package yowza

// Phase D (Timeline-v2) — per-shard timeline primitives in Go.
//
// These methods are thin wrappers over the C/WASM timeline
// exports in src/yowza_segdb.c (yowza_segdb_timeline_get_at,
// yowza_segdb_timeline_scan_cursor, yowza_segdb_timeline_fork).
// They are the per-shard primitives the multi-shard
// coordinator (TimelineHandle, timeline_handle.go) delegates
// to for its BASE reads. They add exactly one rule on top of
// the existing ScanCursor/ScanPage paths: a timeline read is
// pinned to (snapshot_seq, lease_id) and NEVER falls back
// to the main head — a future sequence or an expired lease
// fails with a typed error before any row is returned, and
// a missing export fails with ErrTimelineExportMissing
// instead of silently degrading to the current-head Get.
//
// A shard on its own has no timeline overlay: segdb_put /
// segdb_delete write the shard's main head. The overlay
// (writes after the branch point) lives in the Go
// TimelineHandle's private write set, layered over these
// reads — the same layering SegDBTransaction uses over a
// Phase B snapshot. Nothing here calls segdb_put for a
// timeline write, because that would write main head data
// under a timeline name.

import (
	"context"
	"encoding/binary"
	"fmt"
)

// ErrTimelineExportMissing reports a wasm binary predating
// the Phase D timeline exports. Returned directly (never
// wrapped), so callers compare with ==. A caller that gets
// this error must NOT fall back to a current-head read: the
// timeline point cannot be proven, so the read refuses.
var ErrTimelineExportMissing = fmt.Errorf("yowza segdb: wasm binary predates timeline exports (missing yowza_segdb_timeline_*)")

// timelineGetAtRC maps a C timeline_get_at return code to
// a Go error. rc 0 means found (the caller reads out_len
// and the value bytes itself); rc -1 means absence at
// this snapshot (bad arguments are validated by the
// caller first, so they cannot reach here in practice);
// the pinned codes are typed.
func timelineGetAtRC(rc int32, key string) error {
	switch rc {
	case 0:
		return nil
	case -10:
		return fmt.Errorf("%w: timeline lease", ErrLeaseExpired)
	case -11:
		return fmt.Errorf("%w: timeline snapshot in the future", ErrSnapshotInvalid)
	case -13:
		return fmt.Errorf("yowza segdb: timeline get value exceeds output buffer")
	case -14:
		return fmt.Errorf("yowza segdb: timeline get scan failed")
	default:
		return fmt.Errorf("%w: %s", ErrTimelineKeyNotFound, key)
	}
}

// TimelineGetAt returns the value of key at snapshotSeq on
// this shard, pinned by leaseID (0 = caller owns pinning,
// e.g. through a durable pin; the future-sequence check
// still applies). It never reads the current head: a
// snapshot above the shard's current sequence returns
// ErrSnapshotInvalid, an expired lease returns
// ErrLeaseExpired, and an absent key returns an error —
// never a main-head value.
func (s *SegDB) TimelineGetAt(key string, snapshotSeq uint64, leaseID uint64) ([]byte, error) {
	s.host.mu.Lock()
	defer s.host.mu.Unlock()
	fn := s.host.module.ExportedFunction("yowza_segdb_timeline_get_at")
	if fn == nil {
		return nil, ErrTimelineExportMissing
	}
	if err := s.writeKey(key); err != nil {
		return nil, err
	}
	res, err := fn.Call(context.Background(), s.stateOff, s.scratchOff+segScratchKey,
		uint64(len(key)), snapshotSeq, leaseID,
		s.scratchOff+segScratchOut, segGetOutCap, s.scratchOff+segScratchLen)
	if err != nil {
		return nil, err
	}
	rc := rcOf(res)
	if rc != 0 {
		return nil, timelineGetAtRC(rc, key)
	}
	lenBytes, ok := s.host.memRead(s.scratchOff+segScratchLen, 4)
	if !ok {
		return nil, fmt.Errorf("yowza segdb: failed to read timeline get length")
	}
	vlen := binary.LittleEndian.Uint32(lenBytes)
	if uint64(vlen) > segGetOutCap {
		return nil, fmt.Errorf("yowza segdb: timeline get length %d exceeds buffer", vlen)
	}
	val, ok := s.host.memRead(s.scratchOff+segScratchOut, uint64(vlen))
	if !ok {
		return nil, fmt.Errorf("yowza segdb: failed to read timeline get value")
	}
	out := make([]byte, vlen)
	copy(out, val)
	return out, nil
}

// TimelineScanCursor is ScanCursor pinned to a timeline
// point: the same lease and future-sequence checks as
// TimelineGetAt run in C before any row is returned. On a
// binary predating the export it returns
// ErrTimelineExportMissing, never the unpinned cursor.
func (s *SegDB) TimelineScanCursor(prefix string, snapshotSeq uint64, leaseID uint64, cursor []byte, hasCursor bool, limit uint32) ([]SegDBScanEntry, []byte, bool, error) {
	s.host.mu.Lock()
	defer s.host.mu.Unlock()
	fn := s.host.module.ExportedFunction("yowza_segdb_timeline_scan_cursor")
	if fn == nil {
		return nil, nil, false, ErrTimelineExportMissing
	}
	if limit == 0 {
		return nil, nil, false, nil
	}
	if limit > segPageMaxRows {
		limit = segPageMaxRows
	}
	if len(prefix) > 1023 {
		return nil, nil, false, fmt.Errorf("yowza segdb: prefix too long")
	}
	if hasCursor && len(cursor) > 1023 {
		return nil, nil, false, fmt.Errorf("yowza segdb: cursor too long")
	}
	if !s.host.memWrite(s.scratchOff+segScratchKey, []byte(prefix)) {
		return nil, nil, false, fmt.Errorf("yowza segdb: failed to write prefix to wasm memory")
	}
	hasCur := uint64(0)
	cursorLen := uint64(0)
	if hasCursor {
		hasCur = 1
		cursorLen = uint64(len(cursor))
		if !s.host.memWrite(s.scratchOff+segScratchVal, cursor) {
			return nil, nil, false, fmt.Errorf("yowza segdb: failed to write cursor to wasm memory")
		}
	}
	res, err := fn.Call(context.Background(), s.stateOff, s.scratchOff+segScratchKey,
		uint64(len(prefix)), snapshotSeq, leaseID, s.scratchOff+segScratchVal, cursorLen, hasCur,
		uint64(limit), s.scratchOff+segScratchOut, segPageOutCap, s.scratchOff+segScratchLen,
		s.scratchOff+segCursorNextKey, 1024, s.scratchOff+segCursorNextLen, s.scratchOff+segCursorHasMore)
	if err != nil {
		return nil, nil, false, err
	}
	rc := rcOf(res)
	switch rc {
	case 0:
	case -10:
		return nil, nil, false, fmt.Errorf("%w: timeline lease", ErrLeaseExpired)
	case -11:
		return nil, nil, false, fmt.Errorf("%w: timeline snapshot in the future", ErrSnapshotInvalid)
	case -3:
		return nil, nil, false, fmt.Errorf("yowza segdb: timeline scan cursor output buffer too small (rc=-3)")
	default:
		return nil, nil, false, fmt.Errorf("yowza segdb: timeline scan cursor failed (rc=%d)", rc)
	}
	readU32 := func(off uint64) (uint32, bool) {
		b, ok := s.host.memRead(s.scratchOff+off, 4)
		if !ok {
			return 0, false
		}
		return binary.LittleEndian.Uint32(b), true
	}
	count, ok := readU32(segScratchLen)
	if !ok {
		return nil, nil, false, fmt.Errorf("yowza segdb: failed to read timeline scan count")
	}
	nextLen, ok := readU32(segCursorNextLen)
	if !ok {
		return nil, nil, false, fmt.Errorf("yowza segdb: failed to read timeline scan next length")
	}
	hasMoreU, ok := readU32(segCursorHasMore)
	if !ok {
		return nil, nil, false, fmt.Errorf("yowza segdb: failed to read timeline scan has-more")
	}
	raw, ok := s.host.memRead(s.scratchOff+segScratchOut, segPageOutCap)
	if !ok {
		return nil, nil, false, fmt.Errorf("yowza segdb: failed to read timeline scan output")
	}
	entries, err := decodeScanPageOutput(raw, count)
	if err != nil {
		return nil, nil, false, err
	}
	var nextKey []byte
	if nextLen > 0 {
		if nextLen > 1024 {
			return nil, nil, false, fmt.Errorf("yowza segdb: timeline scan next key length %d exceeds cap", nextLen)
		}
		nb, ok := s.host.memRead(s.scratchOff+segCursorNextKey, uint64(nextLen))
		if !ok {
			return nil, nil, false, fmt.Errorf("yowza segdb: failed to read timeline scan next key")
		}
		nextKey = append([]byte(nil), nb...)
	} else if hasCursor {
		nextKey = append([]byte(nil), cursor...)
	}
	return entries, nextKey, hasMoreU != 0, nil
}

// TimelineFork pins the same snapshot point under a new
// lease and returns that lease id (0 + error on failure:
// future snapshot, expired source lease, or the 64-entry
// table full). The fork reads exactly the source point;
// later main-head writes are invisible to both, and the
// fork's own writes live in the caller's overlay.
func (s *SegDB) TimelineFork(snapshotSeq uint64, leaseID uint64) (uint64, error) {
	s.host.mu.Lock()
	defer s.host.mu.Unlock()
	fn := s.host.module.ExportedFunction("yowza_segdb_timeline_fork")
	if fn == nil {
		return 0, ErrTimelineExportMissing
	}
	res, err := fn.Call(context.Background(), s.stateOff, snapshotSeq, leaseID)
	if err != nil || len(res) == 0 {
		return 0, fmt.Errorf("yowza segdb: timeline fork call failed: %v", err)
	}
	if res[0] == 0 {
		// Distinguish the causes the C layer folds into 0:
		// a future sequence is a caller error, anything
		// else at a valid sequence is table-full (or an
		// expired source lease, which LeaseValid proves).
		seq, serr := s.snapshotSeqLocked()
		if serr != nil {
			return 0, serr
		}
		if snapshotSeq > seq {
			return 0, fmt.Errorf("%w: timeline fork snapshot in the future", ErrSnapshotInvalid)
		}
		if leaseID != 0 {
			if _, _, valid, _, ok := s.leaseFuncs(); ok {
				if vres, verr := s.callU64Locked(valid, s.stateOff, leaseID); verr == nil && vres != 1 {
					return 0, fmt.Errorf("%w: timeline fork source lease", ErrLeaseExpired)
				}
			}
		}
		return 0, ErrLeaseTableFull
	}
	return res[0], nil
}
