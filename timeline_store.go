package yowza

// Timeline persistence: the registry file and the per-timeline
// overlay WALs. This file owns on-disk formats only; the
// manager logic lives in timeline.go.
//
// Registry (timeline-registry.json, in the PersistedSegDB
// directory): one JSON document listing every timeline ever
// created in this directory, including tombstones for deleted
// timelines. Tombstones are what make "never reuse an ID (or a
// timeline number)" enforceable across restarts: a deleted ID
// stays in the registry forever as deleted=true, so a later
// fork with the same ID fails with ErrTimelineIDReuse instead
// of silently resurrecting or colliding with the old WAL.
// Published atomically: tmp file + fsync + rename + directory
// fsync (the same single-commit-point pattern Gate 2 uses for
// checkpoint.json), under the manager write lock. A crash
// mid-publish leaves either the old registry or the new one,
// never a half-written one.
//
// Overlay WAL (timeline-<id>.wal): append-only records, one
// per timeline write, plus one record per effective merge.
// Record framing: [u32 totalLen][u32 crc32IEEE(payload)]
// [payload]. Payload: [u64 seq][u8 type] then type fields.
//   type 1 put:    [u32 klen][key][u32 vlen][val]
//   type 2 delete: [u32 klen][key]
//   type 3 merge:  [u32 jsonLen][json MergePayload]
// A record is committed iff it is fully present with a valid
// CRC and the expected next sequence number. Replay
// distinguishes an incomplete tail (truncatable, see the WAL
// RECOVERY TABLE on timelineReplayWAL) from corruption or a
// sequence gap (both fatal to manager open, file left for
// inspection, nothing truncated). A merge is therefore
// atomic by construction: its applied key set lives inside a
// single record, so replay applies all of it or none of it,
// never a prefix of it.
//
// The registry is NOT the commit point for ordinary timeline
// writes: a Put is acknowledged after its WAL record is
// fsynced (Sync mode), and the registry head is healed from
// WAL replay on the next open. The registry IS the commit
// point for lineage (fork/delete) and the acknowledgement
// gate for merges: Merge returns only after the merge WAL
// record is fsynced AND the registry is published. A crash
// between those two steps resolves to post-merge on reopen
// (the merge record replays, the registry head is healed),
// which is one of the two allowed outcomes — never half.

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const (
	timelineRegistryFile    = "timeline-registry.json"
	timelineRegistryTmpFile = "timeline-registry.json.tmp"
	timelineRegistryVersion = 1
)

const (
	timelineWALPut    = 1
	timelineWALDelete = 2
	timelineWALMerge  = 3
	// timelineWALMergeRef (Phase C, C2) is a LARGE-merge commit
	// record: it does NOT embed the applied key set. The applied
	// keys live in a sorted, checksummed delta-segment file
	// (see timeline_dag.go); this record is the small commit
	// point naming that file. A delta file without a
	// merge-ref record is an orphan (crash between delta
	// staging and commit); a merge-ref record whose delta
	// file is missing or corrupt fails manager open with
	// corruption (a committed merge must fully replay or
	// stop, never half-apply).
	timelineWALMergeRef = 4
	// timelineWALCheckpointBase (Axis D) is a WAL-TRUNCATION
	// base record: it MUST be the first record of a timeline
	// WAL, and it names the checkpoint delta segment that
	// holds every overlay entry at or below ThroughSeq.
	// The WAL's suffix (records after the base record) holds
	// only records with seq > ThroughSeq. On replay the
	// base record's checkpoint entries are synthesized as
	// put/delete entries (in the checkpoint's (key, seq)
	// order, which is seq-ascending per key), so the
	// replayed overlay is identical to the pre-truncation
	// overlay. The old code path (timeline.go open) needs
	// no change: applyReplayed ignores a type-5 entry, and
	// the synthesized entries replay exactly like the
	// records they replace. A base record anywhere but
	// first, a base record naming an unsafe file, or a
	// checkpoint that fails to load is corruption, never
	// a silent short read. Both sides of the truncation
	// rename are valid replays (old full WAL, or base +
	// suffix), so a crash mid-truncation cannot lose data.
	timelineWALCheckpointBase = 5
)

// timelineCheckpointBasePayload is the JSON body of a
// checkpoint-base WAL record.
type timelineCheckpointBasePayload struct {
	CheckpointFile string `json:"checkpoint_file"`
	ThroughSeq     uint64 `json:"through_seq"`
}

// timelineMergeRefPayload is the JSON body of a merge-ref WAL
// record. Applied keys are NOT listed here; DeltaFile holds
// them, sorted by key, checksummed. Conflicted/identical
// keys ARE listed (they are the report, and are bounded by
// the conflict set, not by the merged key count — see the
// honest limit in timeline_dag.go: the conflict list is
// still collected in memory for MergeGeneral).
type timelineMergeRefPayload struct {
	MergeID      string   `json:"merge_id"`
	SourceID     string   `json:"source_id"`
	TargetID     string   `json:"target_id"`
	SourceHead   uint64   `json:"source_head"`
	TargetBefore uint64   `json:"target_before"`
	NewHead      uint64   `json:"new_head"`
	Policy       string   `json:"policy"`
	DeltaFile    string   `json:"delta_file"`
	DeltaSHA256  string   `json:"delta_sha256"`
	DeltaKeys    int      `json:"delta_keys"`
	BaseFP       string   `json:"base_fingerprint"`
	BaseCommitID string   `json:"base_commit_id,omitempty"`
	Conflicted   []string `json:"conflicted,omitempty"`
	Identical    []string `json:"identical,omitempty"`
}

// timelineMergeAppliedKey is one key written by a merge.
type timelineMergeAppliedKey struct {
	Key     string `json:"key"`
	Deleted bool   `json:"deleted"`
	Value   []byte `json:"value,omitempty"`
}

// timelineMergePayload is the JSON body of a merge WAL record.
// It is self-contained: replaying it reproduces the merge
// exactly, without consulting any other record.
type timelineMergePayload struct {
	SourceID     string                    `json:"source_id"`
	TargetID     string                    `json:"target_id"`
	SourceHead   uint64                    `json:"source_head"`
	TargetBefore uint64                    `json:"target_before"`
	NewHead      uint64                    `json:"new_head"`
	Policy       string                    `json:"policy"`
	Applied      []timelineMergeAppliedKey `json:"applied"`
	Conflicted   []string                  `json:"conflicted,omitempty"`
	Identical    []string                  `json:"identical,omitempty"`
	BaseFP       string                    `json:"base_fingerprint"`
}

// timelineRegistryRecord is one timeline's durable lineage.
// Phase C fields (PinID, HeadCommitID, CheckpointFile,
// CheckpointGen) are omitempty: a v1 registry without them
// loads unchanged, and a Phase C registry still loads as a
// valid v1 document for every field Phase A/B readers use.
type timelineRegistryRecord struct {
	ID                string       `json:"id"`
	Number            uint64       `json:"number"`
	ParentID          string       `json:"parent_id"`
	BranchVector      []uint64     `json:"branch_vector"`
	BranchParentHead  uint64       `json:"branch_parent_head"`
	BranchFingerprint string       `json:"branch_fingerprint"`
	HeadSeq           uint64       `json:"head_seq"`
	Deleted           bool         `json:"deleted,omitempty"`
	LastMerge         *MergeReport `json:"last_merge,omitempty"`
	PinID             string       `json:"pin_id,omitempty"`
	HeadCommitID      string       `json:"head_commit_id,omitempty"`
	SnapshotRootID    string       `json:"snapshot_root_id,omitempty"`
	CheckpointFile    string       `json:"checkpoint_file,omitempty"`
	CheckpointGen     uint64       `json:"checkpoint_gen,omitempty"`
}

// timelineRegistryDoc is the whole registry file.
// LastDeltaSeq (Phase E) is checkpoint bookkeeping:
// the sequence of the last delta-log line included
// in this document. v1 documents load with 0, and
// the field is omitempty so a v1-shaped document
// (without it) is still a valid checkpoint.
type timelineRegistryDoc struct {
	Version       int                      `json:"version"`
	NextNumber    uint64                   `json:"next_number"`
	Timelines     []timelineRegistryRecord `json:"timelines"`
	DagCommits    []*TimelineCommit        `json:"dag_commits,omitempty"`
	SnapshotRoots []*TimelineSnapshotRoot  `json:"snapshot_roots,omitempty"`
	LastDeltaSeq  uint64                   `json:"last_delta_seq,omitempty"`
}

func timelineWALPath(dir, id string) string {
	return filepath.Join(dir, "timeline-"+id+".wal")
}

func timelineSyncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

// timelineSyncDirFunc is the directory-sync step of a registry
// publish, indirected only so tests can fail exactly the
// post-rename stage (the PublishedIndeterminate case) without
// a real filesystem fault. Production always uses
// timelineSyncDir.
var timelineSyncDirFunc = timelineSyncDir

// TimelinePublishStatus is the three-state result of a
// registry publish (Phase A, A4). The old single-error return
// could not distinguish "rename never happened" from "rename
// happened but durability is unknown", and callers rolled
// both back the same way — deleting files that might already
// be the durable generation. The commit point is the rename:
//   - NotPublished: the rename did NOT happen (failure at
//     tmp write, tmp sync, tmp close, or rename itself).
//     The old registry is still the durable one; a definite
//     rollback (delete the new timeline/WAL, unmark a
//     delete) is safe.
//   - PublishedDurable: rename happened AND the directory
//     sync succeeded. The new registry is durable.
//   - PublishedIndeterminate: rename happened but the
//     directory sync failed (or its error is unknown). The
//     registry file on disk is already the new one in the
//     live OS view, but after a crash it may revert to the
//     old one. Blind rollback is FORBIDDEN: the new
//     generation's files (WAL, tmp-era records) must not be
//     deleted, and the manager freezes mutations until a
//     recovery (close + reopen) determines the outcome.
//
// Registry generations, in these terms: the live registry
// file is the old generation, the tmp file is the new
// generation (immutable once synced, never edited in
// place), and the rename of the single filename
// timeline-registry.json is the one commit point that swaps
// the pointer from the old generation to the new one. The
// format stays registry v1 (see timelineLoadRegistry);
// no v2 format exists yet, so "migration from v1" is:
// v1 documents load unchanged, with the stricter record
// validation below (every v1 document the writer produces
// satisfies it).
type TimelinePublishStatus int

const (
	TimelinePublishNotPublished TimelinePublishStatus = iota
	TimelinePublishDurable
	TimelinePublishIndeterminate
)

func (s TimelinePublishStatus) String() string {
	switch s {
	case TimelinePublishDurable:
		return "published-durable"
	case TimelinePublishIndeterminate:
		return "published-indeterminate"
	default:
		return "not-published"
	}
}

// timelinePublishRegistryWithStatus atomically publishes doc
// to dir and reports which side of the rename commit point
// the failure (if any) fell on. Caller must hold the manager
// write lock.
func timelinePublishRegistryWithStatus(dir string, doc *timelineRegistryDoc) (TimelinePublishStatus, error) {
	data, err := json.Marshal(doc)
	if err != nil {
		return TimelinePublishNotPublished, err
	}
	tmp := filepath.Join(dir, timelineRegistryTmpFile)
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return TimelinePublishNotPublished, err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return TimelinePublishNotPublished, err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return TimelinePublishNotPublished, err
	}
	if err := f.Close(); err != nil {
		return TimelinePublishNotPublished, err
	}
	if err := os.Rename(tmp, filepath.Join(dir, timelineRegistryFile)); err != nil {
		return TimelinePublishNotPublished, err
	}
	// Commit point passed. Only the directory sync remains;
	// its failure leaves durability unknown, not absent.
	if err := timelineSyncDirFunc(dir); err != nil {
		return TimelinePublishIndeterminate, fmt.Errorf("%w: directory sync after registry rename failed: %v", ErrTimelinePublishIndeterminate, err)
	}
	return TimelinePublishDurable, nil
}

// timelinePublishRegistry is the old single-error shape,
// kept for callers that only need success/failure. It
// preserves the indeterminate error (errors.Is) so a caller
// that does distinguish can still tell the states apart.
func timelinePublishRegistry(dir string, doc *timelineRegistryDoc) error {
	_, err := timelinePublishRegistryWithStatus(dir, doc)
	return err
}

func timelineLoadRegistry(dir string) (*timelineRegistryDoc, bool, error) {
	data, err := os.ReadFile(filepath.Join(dir, timelineRegistryFile))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	var doc timelineRegistryDoc
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, false, fmt.Errorf("%w: registry unparseable: %v", ErrTimelineCorrupt, err)
	}
	if doc.Version != timelineRegistryVersion {
		return nil, false, fmt.Errorf("%w: registry version %d, want %d", ErrTimelineCorrupt, doc.Version, timelineRegistryVersion)
	}
	// Stricter record validation (Phase A): a v1 registry the
	// writer produces always satisfies these, so no valid v1
	// file is rejected; a hand-edited or torn-then-repaired
	// file that violates them is corruption, not a registry
	// to trust. Duplicate IDs would break the never-reuse
	// rule; a zero number or an invalid ID can never have
	// come from createTimelineLocked.
	seen := map[string]bool{}
	for _, rec := range doc.Timelines {
		if rec.Number == 0 {
			return nil, false, fmt.Errorf("%w: registry timeline %q has number 0", ErrTimelineCorrupt, rec.ID)
		}
		if err := validateTimelineID(rec.ID); err != nil {
			return nil, false, fmt.Errorf("%w: registry timeline id invalid: %v", ErrTimelineCorrupt, err)
		}
		if seen[rec.ID] {
			return nil, false, fmt.Errorf("%w: registry has duplicate timeline %q", ErrTimelineCorrupt, rec.ID)
		}
		seen[rec.ID] = true
	}
	return &doc, true, nil
}

// timelineWALEntry is one replayed WAL record.
type timelineWALEntry struct {
	Seq      uint64
	Type     byte
	Key      string
	Value    []byte
	Deleted  bool
	Merge    *timelineMergePayload
	MergeRef *timelineMergeRefPayload
}

func timelineEncodeRecord(seq uint64, typ byte, key string, val []byte, deleted bool, merge *timelineMergePayload) ([]byte, error) {
	var payload bytes.Buffer
	var tmp8 [8]byte
	binary.LittleEndian.PutUint64(tmp8[:], seq)
	payload.Write(tmp8[:])
	payload.WriteByte(typ)
	switch typ {
	case timelineWALPut:
		var tmp4 [4]byte
		binary.LittleEndian.PutUint32(tmp4[:], uint32(len(key)))
		payload.Write(tmp4[:])
		payload.WriteString(key)
		binary.LittleEndian.PutUint32(tmp4[:], uint32(len(val)))
		payload.Write(tmp4[:])
		payload.Write(val)
	case timelineWALDelete:
		var tmp4 [4]byte
		binary.LittleEndian.PutUint32(tmp4[:], uint32(len(key)))
		payload.Write(tmp4[:])
		payload.WriteString(key)
	case timelineWALMerge:
		raw, err := json.Marshal(merge)
		if err != nil {
			return nil, err
		}
		var tmp4 [4]byte
		binary.LittleEndian.PutUint32(tmp4[:], uint32(len(raw)))
		payload.Write(tmp4[:])
		payload.Write(raw)
	default:
		return nil, fmt.Errorf("yowza timeline: unknown WAL record type %d", typ)
	}
	_ = deleted // delete records carry no value; the type says it
	body := payload.Bytes()
	if len(body) > 0xFFFFFFFF {
		return nil, fmt.Errorf("yowza timeline: WAL record body %d bytes exceeds u32 framing", len(body))
	}
	out := make([]byte, 8+len(body))
	binary.LittleEndian.PutUint32(out[0:4], uint32(len(body)))
	binary.LittleEndian.PutUint32(out[4:8], crc32.ChecksumIEEE(body))
	copy(out[8:], body)
	return out, nil
}

// timelineEncodeCheckpointBaseRecord encodes one checkpoint-base
// record (type 5): the small commit point for a WAL truncation.
// Framing and CRC are identical to timelineEncodeRecord; only
// the payload JSON differs. The framing sequence of a base
// record is ThroughSeq itself, so replay's lastSeq for a
// base-only WAL is exactly the truncated-through head.
func timelineEncodeCheckpointBaseRecord(through uint64, checkpointFile string) ([]byte, error) {
	if checkpointFile == "" || strings.Contains(checkpointFile, "/") || strings.Contains(checkpointFile, "\\") || checkpointFile == "." || checkpointFile == ".." {
		return nil, fmt.Errorf("yowza timeline: unsafe checkpoint file name %q", checkpointFile)
	}
	var payload bytes.Buffer
	var tmp8 [8]byte
	binary.LittleEndian.PutUint64(tmp8[:], through)
	payload.Write(tmp8[:])
	payload.WriteByte(timelineWALCheckpointBase)
	raw, err := json.Marshal(timelineCheckpointBasePayload{CheckpointFile: checkpointFile, ThroughSeq: through})
	if err != nil {
		return nil, err
	}
	var tmp4 [4]byte
	binary.LittleEndian.PutUint32(tmp4[:], uint32(len(raw)))
	payload.Write(tmp4[:])
	payload.Write(raw)
	body := payload.Bytes()
	out := make([]byte, 8+len(body))
	binary.LittleEndian.PutUint32(out[0:4], uint32(len(body)))
	binary.LittleEndian.PutUint32(out[4:8], crc32.ChecksumIEEE(body))
	copy(out[8:], body)
	return out, nil
}

// timelineWALSuffixAfter returns the raw bytes of the WAL
// records with sequence > cutoff, in file order, validating
// framing and CRCs of every record it scans. A leading
// checkpoint-base record contributes no bytes (its entries
// live in the checkpoint segment) but sets the baseline:
// records at or below the base's ThroughSeq are already
// covered. Any corrupt or sequence-gapped record is an
// error, never a skipped prefix: truncation must never
// silently drop a record it cannot account for.
func timelineWALSuffixAfter(path string, cutoff uint64) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var out []byte
	var off int64
	var expect uint64 = 1
	size := int64(len(data))
	for off < size {
		if size-off < 8 {
			return nil, fmt.Errorf("%w: WAL %s has an incomplete tail record at offset %d", ErrTimelineCorrupt, path, off)
		}
		bodyLen := int64(binary.LittleEndian.Uint32(data[off : off+4]))
		wantCRC := binary.LittleEndian.Uint32(data[off+4 : off+8])
		if bodyLen <= 0 || off+8+bodyLen > size {
			return nil, fmt.Errorf("%w: WAL %s has an incomplete or illegal record at offset %d", ErrTimelineCorrupt, path, off)
		}
		body := data[off+8 : off+8+bodyLen]
		if crc32.ChecksumIEEE(body) != wantCRC {
			return nil, fmt.Errorf("%w: WAL %s record at offset %d fails CRC", ErrTimelineCorrupt, path, off)
		}
		if len(body) < 9 {
			return nil, fmt.Errorf("%w: WAL %s record at offset %d too short", ErrTimelineCorrupt, path, off)
		}
		seq := binary.LittleEndian.Uint64(body[0:8])
		typ := body[8]
		if typ == timelineWALCheckpointBase {
			if off != 0 {
				return nil, fmt.Errorf("%w: WAL %s checkpoint-base record not first", ErrTimelineCorrupt, path)
			}
			rest := body[9:]
			if len(rest) < 4 {
				return nil, fmt.Errorf("%w: WAL %s checkpoint-base record truncated", ErrTimelineCorrupt, path)
			}
			jlen := int(binary.LittleEndian.Uint32(rest[0:4]))
			rest = rest[4:]
			if jlen < 0 || len(rest) != jlen {
				return nil, fmt.Errorf("%w: WAL %s checkpoint-base payload length mismatch", ErrTimelineCorrupt, path)
			}
			var base timelineCheckpointBasePayload
			if err := json.Unmarshal(rest[:jlen], &base); err != nil {
				return nil, fmt.Errorf("%w: WAL %s checkpoint-base payload unparseable: %v", ErrTimelineCorrupt, path, err)
			}
			if seq != base.ThroughSeq {
				return nil, fmt.Errorf("%w: WAL %s checkpoint-base framing seq %d != through %d", ErrTimelineCorrupt, path, seq, base.ThroughSeq)
			}
			expect = base.ThroughSeq + 1
			off += 8 + bodyLen
			continue
		}
		if seq != expect {
			return nil, fmt.Errorf("%w: WAL %s record at offset %d has sequence %d, want %d", ErrTimelineWALSequence, path, off, seq, expect)
		}
		if seq > cutoff {
			out = append(out, data[off:off+8+bodyLen]...)
		}
		expect++
		off += 8 + bodyLen
	}
	return out, nil
}

// timelineEncodeMergeRefRecord encodes one merge-ref record
// (type 4): the small commit point for a large merge whose
// applied keys live in a delta-segment file, not in this
// record. Framing and CRC are identical to
// timelineEncodeRecord; only the payload JSON differs.
func timelineEncodeMergeRefRecord(seq uint64, ref *timelineMergeRefPayload) ([]byte, error) {
	if err := validateTimelineMergeRefPayload(ref, seq); err != nil {
		return nil, err
	}
	var payload bytes.Buffer
	var tmp8 [8]byte
	binary.LittleEndian.PutUint64(tmp8[:], seq)
	payload.Write(tmp8[:])
	payload.WriteByte(timelineWALMergeRef)
	raw, err := json.Marshal(ref)
	if err != nil {
		return nil, err
	}
	var tmp4 [4]byte
	binary.LittleEndian.PutUint32(tmp4[:], uint32(len(raw)))
	payload.Write(tmp4[:])
	payload.Write(raw)
	body := payload.Bytes()
	if len(body) > 0xFFFFFFFF {
		return nil, fmt.Errorf("yowza timeline: WAL record body %d bytes exceeds u32 framing", len(body))
	}
	out := make([]byte, 8+len(body))
	binary.LittleEndian.PutUint32(out[0:4], uint32(len(body)))
	binary.LittleEndian.PutUint32(out[4:8], crc32.ChecksumIEEE(body))
	copy(out[8:], body)
	return out, nil
}

// timelineReplayWAL reads path and returns the valid record
// prefix, the byte length of that prefix, the last valid
// sequence (0 when empty), whether the file ends in a
// truncatable incomplete tail, and an error.
//
// WAL RECOVERY TABLE (Phase A, A2) — the whole point is that
// "corrupt" and "incomplete" are different outcomes and must
// never be conflated, because truncating a corrupt record
// silently deletes acknowledged records after it:
//
//	Last record's header incomplete to EOF (< 8 bytes
//	  left, or header present but body short of the
//	  declared length)  -> incompleteTail=true, err=nil.
//	  ONLY this tail may be truncated on open.
//	Body complete but CRC mismatch -> ErrTimelineCorrupt,
//	  incompleteTail=false. NEVER truncated, NEVER treated
//	  as a torn tail: a failed CRC on a complete body is
//	  corruption, and later records (possibly acknowledged)
//	  must not be deleted to hide it. File left for
//	  inspection; manager open fails.
//	Sequence != expected (gap, duplicate, or out of order)
//	  -> ErrTimelineWALSequence, incompleteTail=false.
//	  The suffix is never accepted as a valid WAL.
//	Structurally invalid payload (illegal lengths, length
//	  overflow, extra bytes after the declared fields,
//	  invalid key/value sizes, unknown record type, bad
//	  merge JSON, or inconsistent merge values)
//	  -> ErrTimelineCorrupt, incompleteTail=false.
//	Healthy file -> full replay, err=nil,
//	  incompleteTail=false; replay continues from the
//	  next sequence.
//
// A length-incomplete last record MAY be treated as an
// incomplete tail (a torn write can leave a true header
// plus a partial body, and the declared length is the only
// evidence of intent). A failed CRC must NEVER be assumed
// to be an incomplete tail: if the body is fully present,
// its CRC is checkable, and a mismatch is corruption.
func timelineReplayWAL(path string) ([]timelineWALEntry, int64, uint64, bool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0, 0, false, nil
		}
		return nil, 0, 0, false, err
	}
	corrupt := func(format string, args ...any) ([]timelineWALEntry, int64, uint64, bool, error) {
		return nil, 0, 0, false, fmt.Errorf("%w: %s", ErrTimelineCorrupt, fmt.Sprintf(format, args...))
	}
	var entries []timelineWALEntry
	var off int64
	var expect uint64 = 1
	size := int64(len(data))
	for off < size {
		// Header incomplete to EOF: the only truncatable
		// header case. (off == size is clean EOF, handled
		// by the loop condition.)
		if size-off < 8 {
			return entries, off, expect - 1, true, nil
		}
		bodyLen := int64(binary.LittleEndian.Uint32(data[off : off+4]))
		wantCRC := binary.LittleEndian.Uint32(data[off+4 : off+8])
		// Illegal lengths: zero can never be a record (a
		// payload needs at least seq+type), and the
		// end-offset computation must not overflow.
		if bodyLen <= 0 {
			return corrupt("record at offset %d has illegal body length %d", off, bodyLen)
		}
		if bodyLen > size || off+8 > size || off+8+bodyLen < off+8 {
			// Declared body runs past EOF: length-incomplete
			// last record -> truncatable tail. (bodyLen >
			// size already implies past EOF; the arithmetic
			// is in int64 so the sum cannot wrap.)
			return entries, off, expect - 1, true, nil
		}
		if off+8+bodyLen > size {
			return entries, off, expect - 1, true, nil
		}
		body := data[off+8 : off+8+bodyLen]
		// Body fully present: CRC is checkable, so a
		// mismatch is corruption, never a torn tail.
		if crc32.ChecksumIEEE(body) != wantCRC {
			return corrupt("record at offset %d (seq candidate) fails CRC", off)
		}
		if len(body) < 9 {
			return corrupt("record at offset %d body %d bytes, want at least 9 (seq+type)", off, len(body))
		}
		seq := binary.LittleEndian.Uint64(body[0:8])
		typ := body[8]
		// Axis D: a checkpoint-base record may ONLY be first.
		// It names the checkpoint segment holding every
		// overlay entry at or below ThroughSeq; those
		// entries are synthesized here (per-key seq order
		// is preserved by the checkpoint's (key, seq)
		// sort), and the suffix sequence expectation jumps
		// to ThroughSeq+1. A corrupt or missing checkpoint
		// fails replay loudly: the truncated prefix must
		// never replay as an empty overlay.
		if typ == timelineWALCheckpointBase {
			if off != 0 {
				return corrupt("checkpoint-base record at offset %d is not first", off)
			}
			rest0 := body[9:]
			if len(rest0) < 4 {
				return corrupt("checkpoint-base record truncated")
			}
			jlen0 := int(binary.LittleEndian.Uint32(rest0[0:4]))
			rest0 = rest0[4:]
			if jlen0 < 0 || len(rest0) != jlen0 {
				return corrupt("checkpoint-base payload length mismatch")
			}
			var base timelineCheckpointBasePayload
			if err := json.Unmarshal(rest0[:jlen0], &base); err != nil {
				return corrupt("checkpoint-base payload unparseable: %v", err)
			}
			if base.CheckpointFile == "" || strings.Contains(base.CheckpointFile, "/") || strings.Contains(base.CheckpointFile, "\\") {
				return corrupt("checkpoint-base names unsafe file %q", base.CheckpointFile)
			}
			if seq != base.ThroughSeq {
				return corrupt("checkpoint-base framing seq %d != through %d", seq, base.ThroughSeq)
			}
			dents, err := timelineLoadDeltaSegment(filepath.Join(filepath.Dir(path), base.CheckpointFile), "", -1)
			if err != nil {
				return corrupt("checkpoint-base segment %s unreadable: %v", base.CheckpointFile, err)
			}
			for _, d := range dents {
				if d.Seq > base.ThroughSeq {
					return corrupt("checkpoint-base segment entry %q seq %d exceeds through %d", d.Key, d.Seq, base.ThroughSeq)
				}
				et := byte(timelineWALPut)
				if d.Deleted {
					et = byte(timelineWALDelete)
				}
				entries = append(entries, timelineWALEntry{Seq: d.Seq, Type: et, Key: d.Key, Value: append([]byte(nil), d.Value...), Deleted: d.Deleted})
			}
			expect = base.ThroughSeq + 1
			off += 8 + bodyLen
			continue
		}
		if seq != expect {
			return nil, 0, 0, false, fmt.Errorf("%w: record at offset %d has sequence %d, want %d (gap or duplicate; suffix not accepted)", ErrTimelineWALSequence, off, seq, expect)
		}
		rest := body[9:]
		ent := timelineWALEntry{Seq: seq, Type: typ}
		switch typ {
		case timelineWALPut, timelineWALDelete:
			if len(rest) < 4 {
				return corrupt("record seq %d: truncated key-length field", seq)
			}
			klen := int(binary.LittleEndian.Uint32(rest[0:4]))
			rest = rest[4:]
			if klen < 1 || klen > 1023 || len(rest) < klen {
				return corrupt("record seq %d: illegal key length %d", seq, klen)
			}
			ent.Key = string(rest[:klen])
			rest = rest[klen:]
			if typ == timelineWALDelete {
				ent.Deleted = true
				if len(rest) != 0 {
					return corrupt("record seq %d: delete record has %d extra bytes", seq, len(rest))
				}
			} else {
				if len(rest) < 4 {
					return corrupt("record seq %d: truncated value-length field", seq)
				}
				vlen := int(binary.LittleEndian.Uint32(rest[0:4]))
				rest = rest[4:]
				if vlen < 0 || vlen > 1024 || len(rest) < vlen {
					return corrupt("record seq %d: illegal value length %d", seq, vlen)
				}
				if klen+vlen > 1014 {
					return corrupt("record seq %d: key+value length %d exceeds 1014", seq, klen+vlen)
				}
				ent.Value = append([]byte(nil), rest[:vlen]...)
				rest = rest[vlen:]
				if len(rest) != 0 {
					return corrupt("record seq %d: put record has %d extra bytes", seq, len(rest))
				}
			}
		case timelineWALMerge:
			if len(rest) < 4 {
				return corrupt("record seq %d: truncated merge-length field", seq)
			}
			jlen := int(binary.LittleEndian.Uint32(rest[0:4]))
			rest = rest[4:]
			if jlen < 0 || len(rest) != jlen {
				return corrupt("record seq %d: merge length %d does not match remaining body %d (extra or short payload)", seq, jlen, len(rest))
			}
			var mp timelineMergePayload
			if err := json.Unmarshal(rest[:jlen], &mp); err != nil {
				return corrupt("record seq %d: merge payload unparseable: %v", seq, err)
			}
			if err := validateTimelineMergePayload(&mp, seq); err != nil {
				return corrupt("record seq %d: %v", seq, err)
			}
			ent.Merge = &mp
		case timelineWALMergeRef:
			if len(rest) < 4 {
				return corrupt("record seq %d: truncated merge-ref-length field", seq)
			}
			jlen := int(binary.LittleEndian.Uint32(rest[0:4]))
			rest = rest[4:]
			if jlen < 0 || len(rest) != jlen {
				return corrupt("record seq %d: merge-ref length %d does not match remaining body %d", seq, jlen, len(rest))
			}
			var ref timelineMergeRefPayload
			if err := json.Unmarshal(rest[:jlen], &ref); err != nil {
				return corrupt("record seq %d: merge-ref payload unparseable: %v", seq, err)
			}
			if err := validateTimelineMergeRefPayload(&ref, seq); err != nil {
				return corrupt("record seq %d: %v", seq, err)
			}
			ent.MergeRef = &ref
		default:
			return corrupt("record seq %d: unknown WAL record type %d", seq, typ)
		}
		entries = append(entries, ent)
		off += 8 + bodyLen
		expect++
	}
	return entries, off, expect - 1, false, nil
}

// validateTimelineMergePayload checks a decoded merge record
// for internal consistency. The writer's invariants are:
// one self-contained applied set (non-empty, keys strictly
// increasing, values within timeline caps); NewHead == the
// record sequence and TargetBefore == NewHead-1; policy one
// of the three known strings; conflicted/identical sets
// sorted, unique, and disjoint from each other and from
// applied except that source-wins resolutions legitimately
// list conflicted keys among applied; fail policy never
// writes a record with conflicts (it fails first), and
// target-wins never writes a conflicted key as applied.
func validateTimelineMergePayload(mp *timelineMergePayload, seq uint64) error {
	if mp.SourceID == "" || mp.TargetID == "" {
		return fmt.Errorf("merge payload has empty source or target id")
	}
	if err := validateTimelineID(mp.SourceID); err != nil {
		return fmt.Errorf("merge payload source id invalid: %v", err)
	}
	if err := validateTimelineID(mp.TargetID); err != nil {
		return fmt.Errorf("merge payload target id invalid: %v", err)
	}
	if mp.SourceID == mp.TargetID {
		return fmt.Errorf("merge payload merges %q into itself", mp.SourceID)
	}
	if mp.NewHead != seq {
		return fmt.Errorf("merge payload new head %d != record sequence %d", mp.NewHead, seq)
	}
	if mp.TargetBefore+1 != mp.NewHead {
		return fmt.Errorf("merge payload target-before %d + 1 != new head %d", mp.TargetBefore, mp.NewHead)
	}
	switch mp.Policy {
	case "fail", "source-wins", "target-wins":
	default:
		return fmt.Errorf("merge payload has unknown policy %q", mp.Policy)
	}
	if len(mp.BaseFP) != 64 {
		return fmt.Errorf("merge payload base fingerprint %q is not 64 hex chars", mp.BaseFP)
	}
	if _, err := hex.DecodeString(mp.BaseFP); err != nil {
		return fmt.Errorf("merge payload base fingerprint not hex: %v", err)
	}
	if len(mp.Applied) == 0 {
		return fmt.Errorf("merge payload has empty applied set")
	}
	applied := map[string]bool{}
	prev := ""
	for i, k := range mp.Applied {
		if len(k.Key) < 1 || len(k.Key) > 1023 {
			return fmt.Errorf("merge payload applied[%d] has illegal key length %d", i, len(k.Key))
		}
		if i > 0 && k.Key <= prev {
			return fmt.Errorf("merge payload applied keys not strictly increasing (%q after %q)", k.Key, prev)
		}
		prev = k.Key
		if len(k.Value) > 1024 || len(k.Key)+len(k.Value) > 1014 {
			return fmt.Errorf("merge payload applied[%d] value exceeds timeline caps", i)
		}
		applied[k.Key] = true
	}
	sortedUnique := func(name string, keys []string) (map[string]bool, error) {
		out := map[string]bool{}
		prev := ""
		for i, k := range keys {
			if len(k) < 1 || len(k) > 1023 {
				return nil, fmt.Errorf("merge payload %s[%d] has illegal key length %d", name, i, len(k))
			}
			if i > 0 && k <= prev {
				return nil, fmt.Errorf("merge payload %s keys not strictly increasing (%q after %q)", name, k, prev)
			}
			prev = k
			out[k] = true
		}
		return out, nil
	}
	conflicted, err := sortedUnique("conflicted", mp.Conflicted)
	if err != nil {
		return err
	}
	identical, err := sortedUnique("identical", mp.Identical)
	if err != nil {
		return err
	}
	for k := range identical {
		if applied[k] {
			return fmt.Errorf("merge payload key %q is both identical and applied", k)
		}
		if conflicted[k] {
			return fmt.Errorf("merge payload key %q is both identical and conflicted", k)
		}
	}
	switch mp.Policy {
	case "fail":
		if len(conflicted) != 0 {
			return fmt.Errorf("merge payload with fail policy lists conflicted keys %v", mp.Conflicted)
		}
	case "target-wins":
		for k := range conflicted {
			if applied[k] {
				return fmt.Errorf("merge payload with target-wins policy applies conflicted key %q", k)
			}
		}
	case "source-wins":
		for k := range conflicted {
			if !applied[k] {
				return fmt.Errorf("merge payload with source-wins policy does not apply conflicted key %q", k)
			}
		}
	}
	return nil
}

// validateTimelineMergeRefPayload checks a decoded merge-ref
// record for internal consistency. It mirrors the merge
// validator's invariants for the fields a merge-ref shares
// with a merge record (heads, policy, base fingerprint,
// sorted/unique conflicted/identical sets), plus its own:
// a non-empty merge ID, a delta file name that is a plain
// file name (no path separators — delta files live in the
// timeline directory, never outside it), a 64-hex delta
// checksum, and a non-negative delta key count. The delta
// file's existence and content checksum are NOT checked
// here (replay has no directory context); the manager
// checks them when expanding the record, and fails open
// on a missing or corrupt delta for a committed merge-ref.
func validateTimelineMergeRefPayload(ref *timelineMergeRefPayload, seq uint64) error {
	if ref.MergeID == "" {
		return fmt.Errorf("merge-ref payload has empty merge id")
	}
	if ref.SourceID == "" || ref.TargetID == "" {
		return fmt.Errorf("merge-ref payload has empty source or target id")
	}
	if err := validateTimelineID(ref.SourceID); err != nil {
		return fmt.Errorf("merge-ref payload source id invalid: %v", err)
	}
	if err := validateTimelineID(ref.TargetID); err != nil {
		return fmt.Errorf("merge-ref payload target id invalid: %v", err)
	}
	if ref.SourceID == ref.TargetID {
		return fmt.Errorf("merge-ref payload merges %q into itself", ref.SourceID)
	}
	if ref.NewHead != seq {
		return fmt.Errorf("merge-ref payload new head %d != record sequence %d", ref.NewHead, seq)
	}
	if ref.TargetBefore+1 != ref.NewHead {
		return fmt.Errorf("merge-ref payload target-before %d + 1 != new head %d", ref.TargetBefore, ref.NewHead)
	}
	switch ref.Policy {
	case "fail", "source-wins", "target-wins":
	default:
		return fmt.Errorf("merge-ref payload has unknown policy %q", ref.Policy)
	}
	if len(ref.BaseFP) != 64 {
		return fmt.Errorf("merge-ref payload base fingerprint %q is not 64 hex chars", ref.BaseFP)
	}
	if _, err := hex.DecodeString(ref.BaseFP); err != nil {
		return fmt.Errorf("merge-ref payload base fingerprint not hex: %v", err)
	}
	if ref.DeltaFile == "" || strings.Contains(ref.DeltaFile, "/") || strings.Contains(ref.DeltaFile, "\\") || ref.DeltaFile == "." || ref.DeltaFile == ".." {
		return fmt.Errorf("merge-ref payload has unsafe delta file name %q", ref.DeltaFile)
	}
	if len(ref.DeltaSHA256) != 64 {
		return fmt.Errorf("merge-ref payload delta checksum %q is not 64 hex chars", ref.DeltaSHA256)
	}
	if _, err := hex.DecodeString(ref.DeltaSHA256); err != nil {
		return fmt.Errorf("merge-ref payload delta checksum not hex: %v", err)
	}
	if ref.DeltaKeys < 0 {
		return fmt.Errorf("merge-ref payload has negative delta key count %d", ref.DeltaKeys)
	}
	if ref.Policy == "fail" && len(ref.Conflicted) != 0 {
		return fmt.Errorf("merge-ref payload with fail policy lists conflicted keys")
	}
	sortedUnique := func(name string, keys []string) error {
		prev := ""
		for i, k := range keys {
			if len(k) < 1 || len(k) > 1023 {
				return fmt.Errorf("merge-ref payload %s[%d] has illegal key length %d", name, i, len(k))
			}
			if i > 0 && k <= prev {
				return fmt.Errorf("merge-ref payload %s keys not strictly increasing (%q after %q)", name, k, prev)
			}
			prev = k
		}
		return nil
	}
	if err := sortedUnique("conflicted", ref.Conflicted); err != nil {
		return err
	}
	if err := sortedUnique("identical", ref.Identical); err != nil {
		return err
	}
	for _, k := range ref.Conflicted {
		for _, j := range ref.Identical {
			if k == j {
				return fmt.Errorf("merge-ref payload key %q is both identical and conflicted", k)
			}
		}
	}
	return nil
}

// timelineFingerprint hashes a materialized view deterministically:
// keys sorted, each as [u32 klen][key][u32 vlen][val]. Two views
// with the same fingerprint hold the same keys with the same
// values; a view that lost history to compaction hashes
// differently (the probe in development showed a post-compact
// scan at an old vector returns empty, not the old state).
func timelineFingerprint(view map[string][]byte) string {
	keys := make([]string, 0, len(view))
	for k := range view {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	h := sha256.New()
	var tmp4 [4]byte
	for _, k := range keys {
		v := view[k]
		binary.LittleEndian.PutUint32(tmp4[:], uint32(len(k)))
		h.Write(tmp4[:])
		h.Write([]byte(k))
		binary.LittleEndian.PutUint32(tmp4[:], uint32(len(v)))
		h.Write(tmp4[:])
		h.Write(v)
	}
	return hex.EncodeToString(h.Sum(nil))
}
