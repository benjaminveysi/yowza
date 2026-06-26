package yowza

// Axis B — SHARED WAL (one single stream).
//
// The coordinator WAL (coord-wal.log in the coordinator
// directory) is the single sequence authority for
// coordinated commits. Frames are:
//
//	[u32 len][u32 crc32][payload]
//
// with payload JSON {seq, op, op_id, epoch, owner, keys}
// and the CRC over the payload. Sequences start at 1 and
// are strictly contiguous: a gap, a duplicate, a bad CRC,
// or an unparseable payload in the middle of the file is
// ErrCoordWALCorrupt, loudly, never skipped. Only a
// truncated LAST frame is an incomplete tail: Acquire
// truncates it before issuing any new sequence, so a torn
// write can never be followed by a valid frame.
//
// Two processes cannot create interleaved frames or
// duplicate sequences, by construction, not by luck:
//
//   - only the live coordinator owner may append (every
//     append re-validates fencing under the session
//     mutex), and
//   - a second session cannot even be acquired while
//     one is live (ErrCoordLocked), so there is never
//     a second live appender.
//
// What this WAL is NOT: it does not replace any DB's own
// host WAL. Each PersistedSegDB keeps its own WAL under
// its own directory lock. The coordinator WAL sequences
// coordinated operations (which operation, at which
// fencing epoch, touching which keys); the DB WALs still
// record the per-DB mutations themselves. A coordinated
// WAL frame on its own is NOT a commit decision — the
// multi-shard root (coord_multishard.go) or the domain
// commit log is the decision. A WAL frame without a
// decision is an attempted sequence, ignored by
// recovery, never rolled forward on its own. That
// distinction is what makes a crash between WAL append
// and root publish resolve to all-old.

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
)

// CoordWALFrame is one validated shared-WAL frame.
type CoordWALFrame struct {
	Seq   uint64   `json:"seq"`
	Op    string   `json:"op"`
	OpID  string   `json:"op_id"`
	Epoch uint64   `json:"epoch"`
	Owner string   `json:"owner"`
	Keys  []string `json:"keys,omitempty"`
}

// scanCoordWAL validates the WAL at path and returns its
// frames, last sequence, whether the tail is truncated,
// the byte offset of the last valid frame end, and an
// error for any middle corruption. A missing file is an
// empty WAL (no frames, lastSeq 0), not an error.
func scanCoordWAL(path string) (frames []CoordWALFrame, lastSeq uint64, tailTruncated bool, lastValidOffset int64, err error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0, false, 0, nil
		}
		return nil, 0, false, 0, err
	}
	offset := int64(0)
	expect := uint64(1)
	for offset < int64(len(data)) {
		// Header needs 8 bytes; fewer at end = torn tail.
		if int64(len(data))-offset < 8 {
			return frames, lastSeq, true, offset, nil
		}
		ln := binary.BigEndian.Uint32(data[offset : offset+4])
		wantCRC := binary.BigEndian.Uint32(data[offset+4 : offset+8])
		if int64(len(data))-offset-8 < int64(ln) {
			return frames, lastSeq, true, offset, nil
		}
		payload := data[offset+8 : offset+8+int64(ln)]
		if crc32.ChecksumIEEE(payload) != wantCRC {
			return frames, lastSeq, false, offset, fmt.Errorf("%w: frame at offset %d checksum mismatch", ErrCoordWALCorrupt, offset)
		}
		var fr CoordWALFrame
		if err := json.Unmarshal(payload, &fr); err != nil {
			return frames, lastSeq, false, offset, fmt.Errorf("%w: frame at offset %d unparseable: %v", ErrCoordWALCorrupt, offset, err)
		}
		if fr.Seq != expect {
			return frames, lastSeq, false, offset, fmt.Errorf("%w: frame at offset %d seq %d, want %d", ErrCoordWALCorrupt, offset, fr.Seq, expect)
		}
		frames = append(frames, fr)
		lastSeq = fr.Seq
		expect++
		offset += 8 + int64(ln)
	}
	return frames, lastSeq, false, offset, nil
}

// coordTruncateWAL truncates a torn WAL tail to the last
// valid offset, then syncs the file and its directory.
func coordTruncateWAL(path string, offset int64) error {
	if err := os.Truncate(path, offset); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	syncErr := f.Sync()
	f.Close()
	if syncErr != nil {
		return syncErr
	}
	// A dir-sync failure is still reported so callers do
	// not mistake it for a clean truncate.
	return persistSyncDir(filepath.Dir(path))
}

// ScanCoordWAL is the exported WAL reader: validated
// frames in sequence order, the last sequence, and
// whether the file ends in a truncated tail. A corrupt
// middle frame returns an error and no frames are
// trusted for sequencing (the caller must not append
// after corruption — Acquire refuses outright).
func ScanCoordWAL(dir string) ([]CoordWALFrame, uint64, bool, error) {
	frames, lastSeq, tailTruncated, _, err := scanCoordWAL(filepath.Join(dir, "coord-wal.log"))
	return frames, lastSeq, tailTruncated, err
}

// AppendWALRecord appends one frame under this session's
// fencing: the session mutex serialises concurrent
// goroutine appends into one ordered stream, the
// fencing re-check refuses a stale or released session
// before any byte is written, and the sequence is
// advanced only after a synced write. A failed write or
// sync therefore consumes no sequence. Keys are copied
// and sorted for a stable record.
func (s *CoordinatorSession) AppendWALRecord(op, opID string, keys []string) (uint64, error) {
	if s == nil {
		return 0, fmt.Errorf("%w: nil session", ErrCoordReleased)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.validateLocked(); err != nil {
		return 0, err
	}
	seq := s.nextSeq + 1
	keysCopy := append([]string(nil), keys...)
	sort.Strings(keysCopy)
	fr := CoordWALFrame{Seq: seq, Op: op, OpID: opID, Epoch: s.epoch, Owner: s.ownerID, Keys: keysCopy}
	payload, err := json.Marshal(fr)
	if err != nil {
		return 0, err
	}
	frame := make([]byte, 8+len(payload))
	binary.BigEndian.PutUint32(frame[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(frame[4:8], crc32.ChecksumIEEE(payload))
	copy(frame[8:], payload)
	f, err := os.OpenFile(s.walPath(), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return 0, err
	}
	if _, err := f.Write(frame); err != nil {
		f.Close()
		return 0, err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return 0, err
	}
	if err := f.Close(); err != nil {
		return 0, err
	}
	if err := persistSyncDir(s.dir); err != nil {
		// The frame is durable in the file; a dir-sync
		// failure only affects the directory entry for
		// a newly created file. Report it loudly rather
		// than advancing past an uncertain publish.
		return 0, err
	}
	s.nextSeq = seq
	return seq, nil
}

// NextSeq reports the last shared-WAL sequence this
// session has issued (0 before any append, or the WAL
// tail sequence restored at acquire time).
func (s *CoordinatorSession) NextSeq() uint64 {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nextSeq
}
