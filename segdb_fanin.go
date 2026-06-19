package yowza

// ---- GATE 3 additions: fan-in-bounded scan wrapper ----
// This file wraps ONLY the new yowza_segdb_scan_fanin export (C
// segdb_scan_fanin in src/segdb_scan.c); segdb.go is untouched.
// The fan-in scan removes the old 128-source cap: it pages through
// an arbitrary number of segment sources with a bounded window
// (256 keys) and forward-resume seeks, so a 900,000-row database
// (220 segments at 4096 rows each) scans completely, where the old
// cursor path fails with rc=-1 before reading any row.
//
// Contract (mirrors include/segdb_scan.h):
//   - Snapshot semantics are LITERAL, as in ScanPage/ScanAt: seq 0
//     yields no rows; pass SnapshotSeq() for "current".
//   - The cursor is an EXCLUSIVE lower bound: a page resumed with
//     the previous page's next key never repeats a row.
//   - Return codes from the export: 0 ok, -1 scan error, -3 next
//     row does not fit the out buffer (partial page: the rows
//     written ARE valid, resume with the returned next key),
//     -4 invalid argument (bad prefix/cursor/limit). A -4 never
//     writes a row, so it can never masquerade as a partial page.
//   - ScanFaninPage returns a -4 as ErrSegDBFaninInvalid, returned
//     directly (never wrapped) so callers compare with ==.
//   - ScanFanin pages with a huge limit and follows -3 partial
//     pages, so callers get the whole result with one call; each
//     wasm call still returns at most one out-buffer worth of rows
//     (~57 KiB), so memory stays bounded as source count grows.
//   - O3 cross-call discovery cache (C side, segdb_scan.c): page
//     calls with limit <= 4096 reuse a cached discovery for the
//     same (database, prefix, snapshot) instead of re-discovering
//     every source per call; larger-limit calls stay stateless,
//     exactly as before. A continuation page whose source set
//     changed by anything other than a flush-append returns
//     ErrSegDBFaninStale (see its doc); a cursorless page never
//     does. FaninStats sources counts DISCOVERED sources, so a
//     cache-hit page adds 0 to it while a stateless page adds
//     the full source count — that delta is the cache proof.

import (
	"encoding/binary"
	"fmt"
)

// ErrSegDBFaninMissing reports a wasm binary that predates the
// fan-in scan export. Returned directly (never wrapped), so
// callers compare with ==, exactly like ErrSegDBScanPageMissing.
var ErrSegDBFaninMissing = fmt.Errorf("yowza segdb: wasm binary predates fan-in scan export (missing yowza_segdb_scan_fanin)")

// ErrSegDBFaninInvalid reports an invalid fan-in scan argument:
// a prefix or cursor longer than 1023 bytes, a cursor supplied
// without hasCursor, or a zero-length cursor with hasCursor.
// The C function writes no row in this case, so a caller that
// gets this error has received no partial data. Returned
// directly (never wrapped), so callers compare with ==.
var ErrSegDBFaninInvalid = fmt.Errorf("yowza segdb: invalid fan-in scan argument (prefix/cursor/limit)")

// ErrSegDBFaninStale reports a cached fan-in page sequence whose
// source set changed in a way the cache cannot transparently
// follow: O3's invalidation rule (see include/segdb_scan.h).
// Only a flush-append (cached segment list is an exact prefix
// of the current list) is transparently re-discovered; any
// other change — compact replacing segments, recovery to a
// different manifest — fails the continuation page with this
// error, and the session stays dead, so the caller must start a
// new scan from the beginning rather than resuming into mixed
// rows. A cursorless page never returns this error: it always
// discovers fresh. Returned directly (never wrapped), so
// callers compare with ==.
var ErrSegDBFaninStale = fmt.Errorf("yowza segdb: fan-in scan source set changed (compact/recovery); restart the scan")

// Fan-in scan scratch cells inside the 64 KiB scratch region.
// Prefix reuses the key cell, the cursor reuses the value cell
// (both <= 1023 bytes, both cells are 2048 bytes), the count
// reuses the len cell, the out buffer is the page out area, and
// the next-key / next-len / has-more cells sit in otherwise
// unused scratch between the len cell and the out area.
const (
	segFaninNextKey    uint64 = 4100 // 1024 bytes, ends at 5124
	segFaninNextKeyCap uint64 = 1024
	segFaninNextLen    uint64 = 5124 // u32 cell
	segFaninHasMore    uint64 = 5128 // u32 cell
	segFaninHugeLimit  uint32 = 1 << 30
)

// ScanFaninPage returns at most limit live merged rows with the
// given prefix at snapshotSeq, strictly greater than cursor when
// hasCursor is set. It also returns the resume key (the last
// row's key, or the input cursor when the page is empty) and
// whether at least one more live row exists past this page.
// A page shorter than limit with hasMore == false means end of
// input. A -3 (out buffer full) still returns the rows written
// plus hasMore == true; the caller resumes with the returned
// next key. On a binary predating the export it returns
// ErrSegDBFaninMissing; on an invalid argument it returns
// ErrSegDBFaninInvalid with no rows.
func (s *SegDB) ScanFaninPage(prefix string, snapshotSeq uint64, cursor []byte, hasCursor bool, limit uint32) ([]SegDBScanEntry, []byte, bool, error) {
	s.host.mu.Lock()
	defer s.host.mu.Unlock()
	fn := s.host.module.ExportedFunction("yowza_segdb_scan_fanin")
	if fn == nil {
		return nil, nil, false, ErrSegDBFaninMissing
	}
	if limit == 0 {
		return nil, nil, false, nil
	}
	if len(prefix) > 1023 || len(cursor) > 1023 || (hasCursor && len(cursor) == 0) {
		return nil, nil, false, ErrSegDBFaninInvalid
	}
	var hasCursorU uint64
	if hasCursor {
		hasCursorU = 1
	}
	if !s.host.memWrite(s.scratchOff+segScratchKey, []byte(prefix)) {
		return nil, nil, false, fmt.Errorf("yowza segdb: failed to write prefix to wasm memory")
	}
	if hasCursor && !s.host.memWrite(s.scratchOff+segScratchVal, cursor) {
		return nil, nil, false, fmt.Errorf("yowza segdb: failed to write cursor to wasm memory")
	}
	rc, err := s.call(fn, s.stateOff, s.scratchOff+segScratchKey,
		uint64(len(prefix)), snapshotSeq, s.scratchOff+segScratchVal,
		uint64(len(cursor)), hasCursorU, uint64(limit),
		s.scratchOff+segScratchOut, segPageOutCap, s.scratchOff+segScratchLen,
		s.scratchOff+segFaninNextKey, segFaninNextKeyCap,
		s.scratchOff+segFaninNextLen, s.scratchOff+segFaninHasMore)
	if err != nil {
		return nil, nil, false, err
	}
	if rc == -4 {
		return nil, nil, false, ErrSegDBFaninInvalid
	}
	if rc == -5 {
		return nil, nil, false, ErrSegDBFaninStale
	}
	if rc == -1 {
		return nil, nil, false, fmt.Errorf("yowza segdb: fan-in scan failed (rc=-1)")
	}
	if rc != 0 && rc != -3 {
		return nil, nil, false, fmt.Errorf("yowza segdb: fan-in scan failed (rc=%d)", rc)
	}
	countBytes, ok := s.host.memRead(s.scratchOff+segScratchLen, 4)
	if !ok {
		return nil, nil, false, fmt.Errorf("yowza segdb: failed to read fan-in scan count")
	}
	count := binary.LittleEndian.Uint32(countBytes)
	nextLenBytes, ok := s.host.memRead(s.scratchOff+segFaninNextLen, 4)
	if !ok {
		return nil, nil, false, fmt.Errorf("yowza segdb: failed to read fan-in next length")
	}
	nextLen := binary.LittleEndian.Uint32(nextLenBytes)
	hasMoreBytes, ok := s.host.memRead(s.scratchOff+segFaninHasMore, 4)
	if !ok {
		return nil, nil, false, fmt.Errorf("yowza segdb: failed to read fan-in has-more flag")
	}
	hasMore := binary.LittleEndian.Uint32(hasMoreBytes) != 0
	var nextKey []byte
	if nextLen > 0 {
		nk, ok := s.host.memRead(s.scratchOff+segFaninNextKey, uint64(nextLen))
		if !ok {
			return nil, nil, false, fmt.Errorf("yowza segdb: failed to read fan-in next key")
		}
		nextKey = append([]byte(nil), nk...)
	}
	raw, ok := s.host.memRead(s.scratchOff+segScratchOut, segPageOutCap)
	if !ok {
		return nil, nil, false, fmt.Errorf("yowza segdb: failed to read fan-in scan output")
	}
	entries, err := decodeScanPageOutput(raw, count)
	if err != nil {
		return nil, nil, false, err
	}
	if rc == -3 && count == 0 {
		// Impossible by contract: the out buffer always fits one
		// worst-case row (2055 bytes). Treat as an error, never
		// as an empty page the caller might mistake for end of
		// input while hasMore is set.
		return nil, nil, false, fmt.Errorf("yowza segdb: fan-in scan out of buffer with zero rows (rc=-3)")
	}
	return entries, nextKey, hasMore, nil
}

// ScanFanin returns all entries with the given prefix visible at
// snapshotSeq (literal semantics: seq 0 yields no rows), paged
// through ScanFaninPage. The per-call out buffer bounds memory;
// the number of wasm calls grows with result size, not with
// source count, and each call resumes forward from the last key
// (no re-walk from the start, unlike skip paging).
func (s *SegDB) ScanFanin(prefix string, snapshotSeq uint64) ([]SegDBScanEntry, error) {
	var out []SegDBScanEntry
	var cursor []byte
	hasCursor := false
	for {
		page, nextKey, hasMore, err := s.ScanFaninPage(prefix, snapshotSeq, cursor, hasCursor, segFaninHugeLimit)
		if err != nil {
			return nil, err
		}
		out = append(out, page...)
		if !hasMore {
			return out, nil
		}
		if len(page) == 0 {
			return nil, fmt.Errorf("yowza segdb: fan-in scan made no progress (empty page with hasMore set)")
		}
		cursor, hasCursor = nextKey, true
	}
}

// FaninStats returns the C fan-in scan counters (sources
// enumerated, windows collected, per-source seeks, segment
// entries examined) for the most recent ScanFaninPage/ScanFanin
// activity on this database. On a binary predating the stats
// export it returns ErrSegDBFaninMissing.
func (s *SegDB) FaninStats() (sources, windows, seeks, entries uint64, err error) {
	s.host.mu.Lock()
	defer s.host.mu.Unlock()
	fn := s.host.module.ExportedFunction("yowza_segdb_scan_fanin_stats")
	if fn == nil {
		return 0, 0, 0, 0, ErrSegDBFaninMissing
	}
	if _, err := s.call(fn, s.scratchOff+segScratchOut); err != nil {
		return 0, 0, 0, 0, err
	}
	raw, ok := s.host.memRead(s.scratchOff+segScratchOut, 32)
	if !ok {
		return 0, 0, 0, 0, fmt.Errorf("yowza segdb: failed to read fan-in scan stats")
	}
	return binary.LittleEndian.Uint64(raw[0:8]),
		binary.LittleEndian.Uint64(raw[8:16]),
		binary.LittleEndian.Uint64(raw[16:24]),
		binary.LittleEndian.Uint64(raw[24:32]), nil
}

// FaninStatsReset zeroes the C fan-in scan counters. On a binary
// predating the reset export it returns ErrSegDBFaninMissing.
func (s *SegDB) FaninStatsReset() error {
	s.host.mu.Lock()
	defer s.host.mu.Unlock()
	fn := s.host.module.ExportedFunction("yowza_segdb_scan_fanin_stats_reset")
	if fn == nil {
		return ErrSegDBFaninMissing
	}
	_, err := s.call(fn)
	return err
}
