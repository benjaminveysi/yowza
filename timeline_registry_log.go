package yowza

// Phase E, E4 — REGISTRY CHECKPOINT + DELTA LOG.
//
// The old registry rewrote the full JSON document on
// EVERY publish (fork, merge, delete, flush). That
// is O(registry size) per change and grows with the
// timeline count forever. The new shape keeps the
// same document and the same tombstone invariant
// (IDs/numbers never reused — tombstones stay in
// the document forever), but publishes incrementally:
//
//   - timeline-registry.json IS the checkpoint: a
//     full document, written only on checkpoint
//     publishes (the first publish, and every 64th
//     publish after that). It is written with the
//     existing three-state publisher
//     (timelinePublishRegistryWithStatus), so the
//     A4 commit-point semantics are unchanged for
//     checkpoints. The checkpoint document carries
//     LastDeltaSeq: the sequence of the last delta
//     included in it.
//   - timeline-registry-delta.log is the delta log:
//     one JSON line per non-checkpoint publish,
//     each line naming ONLY the records that changed
//     since the last published document (timeline
//     records by ID, DAG commits by ID, snapshot
//     roots by ID, plus NextNumber), with a CRC-32
//     over the canonical body. Lines are appended +
//     fsynced; the directory sync after the append
//     is the durability step, exactly as the rename
//     + dir sync was for full publishes. A publish
//     whose document is unchanged from the last
//     published document writes NOTHING (the state
//     is already durable) and reports Durable.
//   - After a checkpoint publish, the delta log is
//     reset (atomic replace with an empty file, best
//     effort: if the reset fails, old lines remain
//     but are skipped on load by their seq, so a
//     failed reset is never a correctness problem).
//
// Load (timelineLoadRegistryV2): the checkpoint
// document is the base; delta lines with seq >
// LastDeltaSeq apply in order, requiring contiguity
// (a gap is corruption, loud). A torn LAST line
// (unparseable or bad CRC, with no complete line
// after it) is an incomplete tail and is truncated
// — it can only come from a publish that failed
// before acknowledgement. A corrupt MIDDLE line is
// corruption, loud, never truncated. Legacy
// directories (a checkpoint file, no delta log, no
// LastDeltaSeq) load exactly as before: v1 documents
// are readable and migratable, and the first publish
// after such a load is a checkpoint publish, which
// moves the directory to the new shape without
// losing any record.
//
// A4 mapping for delta publishes: append/sync
// failure before the line is complete = NotPublished
// (safe rollback, as before); directory-sync failure
// after a complete append = PublishedIndeterminate
// (the manager freezes, reopen resolves: line
// present -> new state, line absent -> old state +
// WAL heal, exactly the old rename semantics).

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const registryDeltaLogFile = "timeline-registry-delta.log"
const registryCheckpointEvery = 64

type registryDeltaLine struct {
	Seq           uint64                   `json:"seq"`
	NextNumber    uint64                   `json:"next_number"`
	Timelines     []timelineRegistryRecord `json:"timelines,omitempty"`
	DagCommits    []*TimelineCommit        `json:"dag_commits,omitempty"`
	SnapshotRoots []*TimelineSnapshotRoot  `json:"snapshot_roots,omitempty"`
	Checksum      uint32                   `json:"checksum"`
}

func registryDeltaPath(dir string) string {
	return filepath.Join(dir, registryDeltaLogFile)
}

func registryDocCopy(doc *timelineRegistryDoc) *timelineRegistryDoc {
	if doc == nil {
		return nil
	}
	data, err := json.Marshal(doc)
	if err != nil {
		return nil
	}
	var cp timelineRegistryDoc
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil
	}
	return &cp
}

func registryRecordJSON(r timelineRegistryRecord) string {
	data, _ := json.Marshal(r)
	return string(data)
}

func registryDeltaChecksum(seq, nextNumber uint64, timelines []timelineRegistryRecord, dag []*TimelineCommit, roots []*TimelineSnapshotRoot) uint32 {
	body, _ := json.Marshal(struct {
		Seq           uint64                   `json:"seq"`
		NextNumber    uint64                   `json:"next_number"`
		Timelines     []timelineRegistryRecord `json:"timelines,omitempty"`
		DagCommits    []*TimelineCommit        `json:"dag_commits,omitempty"`
		SnapshotRoots []*TimelineSnapshotRoot  `json:"snapshot_roots,omitempty"`
	}{Seq: seq, NextNumber: nextNumber, Timelines: timelines, DagCommits: dag, SnapshotRoots: roots})
	return crc32.ChecksumIEEE(body)
}

// registryDiff returns the delta from old to new:
// records/commits/roots whose JSON changed or that
// are new. Records never disappear (tombstones), so
// there is no deletion list.
func registryDiff(old, new *timelineRegistryDoc) (timelines []timelineRegistryRecord, dag []*TimelineCommit, roots []*TimelineSnapshotRoot) {
	oldRecs := map[string]string{}
	if old != nil {
		for _, r := range old.Timelines {
			oldRecs[r.ID] = registryRecordJSON(r)
		}
	}
	for _, r := range new.Timelines {
		if oldRecs[r.ID] != registryRecordJSON(r) {
			timelines = append(timelines, r)
		}
	}
	oldDag := map[string]string{}
	if old != nil {
		for _, c := range old.DagCommits {
			if c != nil {
				data, _ := json.Marshal(c)
				oldDag[c.ID] = string(data)
			}
		}
	}
	for _, c := range new.DagCommits {
		if c == nil {
			continue
		}
		data, _ := json.Marshal(c)
		if oldDag[c.ID] != string(data) {
			dag = append(dag, c)
		}
	}
	oldRoots := map[string]string{}
	if old != nil {
		for _, r := range old.SnapshotRoots {
			if r != nil {
				data, _ := json.Marshal(r)
				oldRoots[r.ID] = string(data)
			}
		}
	}
	for _, r := range new.SnapshotRoots {
		if r == nil {
			continue
		}
		data, _ := json.Marshal(r)
		if oldRoots[r.ID] != string(data) {
			roots = append(roots, r)
		}
	}
	return timelines, dag, roots
}

// registryApplyDelta applies one delta line to doc
// (in place): upsert records/commits/roots by ID,
// set NextNumber.
func registryApplyDelta(doc *timelineRegistryDoc, line *registryDeltaLine) {
	doc.NextNumber = line.NextNumber
	byID := map[string]int{}
	for i, r := range doc.Timelines {
		byID[r.ID] = i
	}
	for _, r := range line.Timelines {
		if i, ok := byID[r.ID]; ok {
			doc.Timelines[i] = r
		} else {
			byID[r.ID] = len(doc.Timelines)
			doc.Timelines = append(doc.Timelines, r)
		}
	}
	dagByID := map[string]int{}
	for i, c := range doc.DagCommits {
		if c != nil {
			dagByID[c.ID] = i
		}
	}
	for _, c := range line.DagCommits {
		if c == nil {
			continue
		}
		if i, ok := dagByID[c.ID]; ok {
			doc.DagCommits[i] = c
		} else {
			dagByID[c.ID] = len(doc.DagCommits)
			doc.DagCommits = append(doc.DagCommits, c)
		}
	}
	rootByID := map[string]int{}
	for i, r := range doc.SnapshotRoots {
		if r != nil {
			rootByID[r.ID] = i
		}
	}
	for _, r := range line.SnapshotRoots {
		if r == nil {
			continue
		}
		if i, ok := rootByID[r.ID]; ok {
			doc.SnapshotRoots[i] = r
		} else {
			rootByID[r.ID] = len(doc.SnapshotRoots)
			doc.SnapshotRoots = append(doc.SnapshotRoots, r)
		}
	}
	sort.Slice(doc.Timelines, func(i, j int) bool { return doc.Timelines[i].Number < doc.Timelines[j].Number })
}

// timelineLoadRegistryV2 loads the registry through
// the checkpoint + delta log. It returns the
// document, whether any registry exists, the number
// of delta lines applied, the last delta seq applied
// (or the checkpoint's), and an error. A torn last
// delta line is truncated (incomplete tail only).
func timelineLoadRegistryV2(dir string) (*timelineRegistryDoc, bool, int, uint64, error) {
	doc, exists, err := timelineLoadRegistry(dir)
	if err != nil {
		return nil, false, 0, 0, err
	}
	logPath := registryDeltaPath(dir)
	data, err := os.ReadFile(logPath)
	if err != nil {
		if os.IsNotExist(err) {
			if !exists {
				return nil, false, 0, 0, nil
			}
			return doc, true, 0, doc.LastDeltaSeq, nil
		}
		return nil, false, 0, 0, err
	}
	if !exists {
		// Delta log without a checkpoint document is
		// operator damage (deltas are diffs, not a
		// full document): fail loudly, never invent
		// a base document.
		if len(bytes.TrimSpace(data)) > 0 {
			return nil, false, 0, 0, fmt.Errorf("%w: delta log present without a registry checkpoint", ErrTimelineCorrupt)
		}
		return nil, false, 0, 0, nil
	}
	// Split into lines, tracking byte offsets so a
	// torn tail can be truncated precisely.
	type rawLine struct {
		off  int
		data []byte
	}
	var lines []rawLine
	off := 0
	for off < len(data) {
		nl := bytes.IndexByte(data[off:], '\n')
		if nl < 0 {
			lines = append(lines, rawLine{off: off, data: data[off:]})
			break
		}
		lines = append(lines, rawLine{off: off, data: data[off : off+nl]})
		off += nl + 1
	}
	applied := 0
	lastSeq := doc.LastDeltaSeq
	for i, ln := range lines {
		if len(bytes.TrimSpace(ln.data)) == 0 {
			continue
		}
		var line registryDeltaLine
		isLast := i == len(lines)-1
		if err := json.Unmarshal(ln.data, &line); err != nil {
			if isLast {
				// Incomplete tail: truncate and stop.
				if terr := os.Truncate(logPath, int64(ln.off)); terr != nil {
					return nil, false, 0, 0, terr
				}
				break
			}
			return nil, false, 0, 0, fmt.Errorf("%w: delta line %d unparseable: %v", ErrTimelineCorrupt, i, err)
		}
		if registryDeltaChecksum(line.Seq, line.NextNumber, line.Timelines, line.DagCommits, line.SnapshotRoots) != line.Checksum {
			if isLast {
				if terr := os.Truncate(logPath, int64(ln.off)); terr != nil {
					return nil, false, 0, 0, terr
				}
				break
			}
			return nil, false, 0, 0, fmt.Errorf("%w: delta line %d checksum mismatch", ErrTimelineCorrupt, i)
		}
		if line.Seq <= doc.LastDeltaSeq {
			// Already included in the checkpoint
			// (crash between checkpoint write and
			// log reset): skip, never re-apply.
			continue
		}
		if line.Seq != lastSeq+1 {
			return nil, false, 0, 0, fmt.Errorf("%w: delta seq gap: got %d, want %d", ErrTimelineCorrupt, line.Seq, lastSeq+1)
		}
		registryApplyDelta(doc, &line)
		lastSeq = line.Seq
		applied++
	}
	doc.LastDeltaSeq = lastSeq
	return doc, true, applied, lastSeq, nil
}

// publishRegistryDeltaLocked publishes doc through
// the checkpoint + delta log. Caller holds the
// manager write lock (or is single-threaded at
// open). It updates the manager's published-state
// tracking on durable success only. The returned
// status follows the A4 three-state contract.
func (m *TimelineManager) publishRegistryDeltaLocked(doc *timelineRegistryDoc) (TimelinePublishStatus, error) {
	// No change since the last durable publish:
	// nothing to write, state already durable.
	// Normalised comparison: strip LastDeltaSeq from
	// both sides (it is bookkeeping, not state).
	oldNorm := registryDocCopy(m.lastRegistryDoc)
	newNorm := registryDocCopy(doc)
	if oldNorm != nil && newNorm != nil {
		oldNorm.LastDeltaSeq = 0
		newNorm.LastDeltaSeq = 0
		oldData, _ := json.Marshal(oldNorm)
		newData, _ := json.Marshal(newNorm)
		if string(oldData) == string(newData) {
			return TimelinePublishDurable, nil
		}
	}
	needCheckpoint := m.lastRegistryDoc == nil || m.registryDeltas >= registryCheckpointEvery
	if needCheckpoint {
		// Checkpoint publish: full document (which
		// becomes the new checkpoint), LastDeltaSeq
		// naming the last delta it includes.
		doc.LastDeltaSeq = m.registryLastDeltaSeq
		status, err := timelinePublishRegistryWithStatus(m.dir, doc)
		if err != nil {
			if status == TimelinePublishIndeterminate {
				m.publishIndeterminate = &TimelinePublishIndeterminateError{Op: "registry-checkpoint", Err: err}
			}
			return status, err
		}
		// Best-effort log reset (old lines are
		// skipped by seq on load even if it fails).
		logPath := registryDeltaPath(m.dir)
		tmp := logPath + ".tmp"
		if f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644); err == nil {
			_ = f.Sync()
			f.Close()
			_ = os.Rename(tmp, logPath)
			// Fault-injectable hook, as in the
			// checkpoint path (best effort here).
			_ = timelineSyncDirFunc(m.dir)
		}
		m.lastRegistryDoc = registryDocCopy(doc)
		m.registryDeltas = 0
		return status, nil
	}
	// Delta publish: append one line.
	timelines, dag, roots := registryDiff(m.lastRegistryDoc, doc)
	seq := m.registryLastDeltaSeq + 1
	line := registryDeltaLine{Seq: seq, NextNumber: doc.NextNumber, Timelines: timelines, DagCommits: dag, SnapshotRoots: roots}
	line.Checksum = registryDeltaChecksum(line.Seq, line.NextNumber, line.Timelines, line.DagCommits, line.SnapshotRoots)
	lineData, err := json.Marshal(line)
	if err != nil {
		return TimelinePublishNotPublished, err
	}
	logPath := registryDeltaPath(m.dir)
	f, err := os.OpenFile(logPath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return TimelinePublishNotPublished, err
	}
	if _, err := f.Write(append(lineData, '\n')); err != nil {
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
	if err := timelineSyncDirFunc(m.dir); err != nil {
		m.publishIndeterminate = &TimelinePublishIndeterminateError{Op: "registry-delta", Err: err}
		return TimelinePublishIndeterminate, m.publishIndeterminate
	}
	doc.LastDeltaSeq = seq
	m.lastRegistryDoc = registryDocCopy(doc)
	m.registryLastDeltaSeq = seq
	m.registryDeltas++
	return TimelinePublishDurable, nil
}

// registryDeltaLineCount is a diagnostic for tests:
// the number of lines currently in the delta log.
func registryDeltaLineCount(dir string) (int, error) {
	f, err := os.Open(registryDeltaPath(dir))
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	defer f.Close()
	n := 0
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1024*1024), 1024*1024)
	for sc.Scan() {
		if len(strings.TrimSpace(sc.Text())) > 0 {
			n++
		}
	}
	return n, sc.Err()
}
