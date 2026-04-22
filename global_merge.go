package yowza

// Phase E, E1 — GLOBAL MERGE TO THE SHARED DB HEAD.
//
// This is NOT TimelineHandle.MergeToMain (Phase D: one
// SegDBTransaction, last-write-wins, no conflict
// resolution) and NOT a loop of facade Puts (which would
// publish each key separately, leaving readers to see a
// half-merged state between keys). The protocol here is:
//
//   1. PREPARE + VALIDATE (before anything is staged):
//      the source timeline's change set is computed from
//      its overlay chain (candidate keys = union of
//      chain overlay keys; the branch value of each
//      candidate comes from ONE streaming scan of the
//      branch vector, filtered to candidates — memory
//      bounded by the candidate set, never by the DB
//      size). For each changed key the CURRENT main
//      value is read (one Get per changed key) and
//      compared with the branch value. Conflict
//      policy is the Phase C merge semantics:
//      fail (default, applies nothing, lists every
//      conflicting key), source-wins, target-wins.
//   2. STAGE: deltas are written in chunk files of at
//      most globalMergeChunkKeys deltas each (JSON +
//      CRC-32, fsynced), plus a small coordinator
//      manifest naming every chunk, the per-shard
//      delta counts, the source, the policy, and the
//      expected head vector. No single record holds
//      the whole merge, and no full DB map is built.
//   3. COMMIT POINT: under p.mu (so no facade writer
//      can interleave — readers holding p.mu see the
//      old head before and the new head after, never
//      a prefix), the expected head is re-checked
//      against the live shard sequences, then ONE
//      commit record (tmp + sync + rename + dir sync
//      in Sync mode) is published. That record IS the
//      global root for this merge: the manifest
//      points at ALL new shard deltas, and the commit
//      record points at the manifest. Shards are
//      never published separately.
//   4. APPLY (still under p.mu): every delta goes
//      through the host WAL + shard put/delete, the
//      same per-record path facade Put uses (dirty
//      bit, WAL frame, abort record on a shard
//      failure), then a done record is published.
//   5. RECOVERY (RecoverGlobalMerges, which
//      NewTimelineManager runs at open and DB-only
//      callers must run before serving reads after a
//      crash): staged-only merges (manifest, no
//      commit) are RECLAIMED (chunks + manifest
//      deleted, nothing applied); commit-recorded
//      merges without a done record are ROLLED
//      FORWARD (all deltas re-applied idempotently —
//      safe because no writer has run since the
//      crash); done merges are cleaned up. A corrupt
//      committed chunk/manifest fails recovery
//      loudly and applies nothing further, never a
//      half state. Crash outcomes are therefore
//      exactly: old root (before the commit record)
//      or new root (after it, completed by recovery).
//
// Crash matrix (stage -> outcome on recovery):
//   staged chunks, no manifest  -> reclaimed
//   manifest, no commit record  -> reclaimed
//   commit record, apply partial -> rolled forward to new root
//   commit record, apply done, no done record -> rolled forward (idempotent)
//   done record present         -> cleanup only, no re-apply
//   in-process apply failure after commit -> the merge is
//     REPORTED as committed-but-incomplete
//     (ErrGlobalMergeCommitted); recovery completes it.
//     The caller must NOT retry the merge as a fresh
//     merge (that would double-apply under a new
//     expected head); it must run recovery.
//
// Multi-shard note: the per-shard "roots" after a
// merge are the post-apply snapshot vector, recorded
// in the done record. There is no per-shard publish:
// the commit record is the single commit point for
// all shards, and the done record's vector is only
// diagnostic (recovery re-derives the live vector).

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// ErrGlobalMergeConflict reports a fail-policy global
// merge refused because of conflicting keys. Compare
// with errors.Is; the *GlobalMergeConflictError lists
// every conflicting key and nothing was applied.
var ErrGlobalMergeConflict = errors.New("yowza global merge: conflicting keys with main head")

// ErrGlobalMergeStaleHead reports an expected-head
// mismatch: the DB head moved since the caller captured
// it. Nothing was staged or applied; the caller must
// re-capture the head and re-prepare, never silently
// rebase onto the new head.
var ErrGlobalMergeStaleHead = errors.New("yowza global merge: stale expected head")

// ErrGlobalMergeCommitted reports a merge whose commit
// record is durable but whose in-process apply did not
// finish. Recovery (RecoverGlobalMerges) completes it;
// a fresh retry is forbidden.
var ErrGlobalMergeCommitted = errors.New("yowza global merge: commit record durable, apply incomplete; run recovery")

// ErrGlobalMergeCorrupt reports a committed merge whose
// staged chunks or manifest fail validation during
// recovery. Recovery stops loudly; no partial apply.
var ErrGlobalMergeCorrupt = errors.New("yowza global merge: committed merge staging corrupt")

// GlobalMergeConflictError lists every conflicting key.
type GlobalMergeConflictError struct {
	SourceID string
	Keys     []string
}

func (e *GlobalMergeConflictError) Error() string {
	return fmt.Sprintf("yowza global merge: %d conflicting keys with main head (source %s): %s", len(e.Keys), e.SourceID, strings.Join(e.Keys, ", "))
}

func (e *GlobalMergeConflictError) Unwrap() error { return ErrGlobalMergeConflict }

// GlobalMergeStaleHeadError names the expected vs live head.
type GlobalMergeStaleHeadError struct {
	Expected []uint64
	Actual   []uint64
}

func (e *GlobalMergeStaleHeadError) Error() string {
	return fmt.Sprintf("yowza global merge: expected head %v, live head %v", e.Expected, e.Actual)
}

func (e *GlobalMergeStaleHeadError) Unwrap() error { return ErrGlobalMergeStaleHead }

// GlobalMergeReport is the merge outcome. It is never a
// bare boolean: applied / identical / conflicted keys,
// the policy, both heads, and the staging shape are
// all reported.
type GlobalMergeReport struct {
	MergeID        string
	SourceID       string
	AppliedKeys    []string
	IdenticalKeys  []string
	ConflictedKeys []string
	Policy         TimelineConflictPolicy
	ExpectedHead   []uint64
	NewHead        []uint64
	ChunkCount     int
	ShardCounts    map[int]int
}

type globalMergeDelta struct {
	Key     string `json:"key"`
	Deleted bool   `json:"deleted,omitempty"`
	Value   []byte `json:"value,omitempty"`
}

type globalMergeChunkFile struct {
	Version  int                `json:"version"`
	MergeID  string             `json:"merge_id"`
	Index    int                `json:"index"`
	Deltas   []globalMergeDelta `json:"deltas"`
	Checksum uint32             `json:"checksum"`
}

type globalMergeChunkRef struct {
	File     string `json:"file"`
	Count    int    `json:"count"`
	Checksum uint32 `json:"checksum"`
}

type globalMergeManifest struct {
	Version      int                   `json:"version"`
	MergeID      string                `json:"merge_id"`
	SourceID     string                `json:"source_id"`
	Policy       string                `json:"policy"`
	ExpectedHead []uint64              `json:"expected_head"`
	TotalDeltas  int                   `json:"total_deltas"`
	ShardCounts  map[string]int        `json:"shard_counts"`
	Chunks       []globalMergeChunkRef `json:"chunks"`
	Checksum     uint32                `json:"checksum"`
}

type globalMergeCommit struct {
	Version      int      `json:"version"`
	MergeID      string   `json:"merge_id"`
	ManifestFile string   `json:"manifest_file"`
	State        string   `json:"state"` // "committed" or "done"
	NewHead      []uint64 `json:"new_head,omitempty"`
}

const globalMergeChunkKeys = 256

var globalMergeNextID uint64

func globalMergeFileNames(dir, id string) (manifest, commit, done string) {
	return filepath.Join(dir, "global-merge-"+id+".manifest.json"),
		filepath.Join(dir, "global-merge-"+id+".commit.json"),
		filepath.Join(dir, "global-merge-"+id+".done.json")
}

func globalMergeChunkName(dir, id string, index int) string {
	return filepath.Join(dir, fmt.Sprintf("global-merge-%s.chunk-%d.json", id, index))
}

// globalMergeID allocates a merge ID unique within this
// process; the timestamp component keeps it unique
// across restarts for the same directory (a restart
// reuses at most the staged files of a crashed merge,
// which recovery reclaims or completes before any new
// merge is staged).
func globalMergeID(sourceID string) string {
	n := atomic.AddUint64(&globalMergeNextID, 1)
	return fmt.Sprintf("%d-%d-%s", currentUnixNano(), n, sourceID)
}

func currentUnixNano() int64 {
	return time.Now().UnixNano()
}

func globalMergeChecksum(body []byte) uint32 { return crc32.ChecksumIEEE(body) }

func globalMergeWriteSync(path string, data []byte, syncDir bool, dir string) error {
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
	if syncDir {
		return persistSyncDir(dir)
	}
	return nil
}

func globalMergeEncodeChunk(id string, index int, deltas []globalMergeDelta) ([]byte, uint32, error) {
	body, err := json.Marshal(struct {
		MergeID string             `json:"merge_id"`
		Index   int                `json:"index"`
		Deltas  []globalMergeDelta `json:"deltas"`
	}{MergeID: id, Index: index, Deltas: deltas})
	if err != nil {
		return nil, 0, err
	}
	sum := globalMergeChecksum(body)
	f := globalMergeChunkFile{Version: 1, MergeID: id, Index: index, Deltas: deltas, Checksum: sum}
	data, err := json.Marshal(f)
	if err != nil {
		return nil, 0, err
	}
	return data, sum, nil
}

func globalMergeDecodeChunk(data []byte, wantID string, wantIndex int) ([]globalMergeDelta, error) {
	var f globalMergeChunkFile
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("%w: chunk unparseable: %v", ErrGlobalMergeCorrupt, err)
	}
	if f.Version != 1 || f.MergeID != wantID || f.Index != wantIndex {
		return nil, fmt.Errorf("%w: chunk version/id/index mismatch", ErrGlobalMergeCorrupt)
	}
	body, err := json.Marshal(struct {
		MergeID string             `json:"merge_id"`
		Index   int                `json:"index"`
		Deltas  []globalMergeDelta `json:"deltas"`
	}{MergeID: f.MergeID, Index: f.Index, Deltas: f.Deltas})
	if err != nil {
		return nil, fmt.Errorf("%w: chunk re-encode: %v", ErrGlobalMergeCorrupt, err)
	}
	if globalMergeChecksum(body) != f.Checksum {
		return nil, fmt.Errorf("%w: chunk %d checksum mismatch", ErrGlobalMergeCorrupt, wantIndex)
	}
	return f.Deltas, nil
}

func globalMergeEncodeManifest(m *globalMergeManifest) ([]byte, error) {
	body, err := json.Marshal(struct {
		MergeID      string                `json:"merge_id"`
		SourceID     string                `json:"source_id"`
		Policy       string                `json:"policy"`
		ExpectedHead []uint64              `json:"expected_head"`
		TotalDeltas  int                   `json:"total_deltas"`
		ShardCounts  map[string]int        `json:"shard_counts"`
		Chunks       []globalMergeChunkRef `json:"chunks"`
	}{MergeID: m.MergeID, SourceID: m.SourceID, Policy: m.Policy, ExpectedHead: m.ExpectedHead, TotalDeltas: m.TotalDeltas, ShardCounts: m.ShardCounts, Chunks: m.Chunks})
	if err != nil {
		return nil, err
	}
	m.Checksum = globalMergeChecksum(body)
	return json.Marshal(m)
}

func globalMergeDecodeManifest(data []byte) (*globalMergeManifest, error) {
	var m globalMergeManifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("%w: manifest unparseable: %v", ErrGlobalMergeCorrupt, err)
	}
	if m.Version != 1 {
		return nil, fmt.Errorf("%w: manifest version %d", ErrGlobalMergeCorrupt, m.Version)
	}
	body, err := json.Marshal(struct {
		MergeID      string                `json:"merge_id"`
		SourceID     string                `json:"source_id"`
		Policy       string                `json:"policy"`
		ExpectedHead []uint64              `json:"expected_head"`
		TotalDeltas  int                   `json:"total_deltas"`
		ShardCounts  map[string]int        `json:"shard_counts"`
		Chunks       []globalMergeChunkRef `json:"chunks"`
	}{MergeID: m.MergeID, SourceID: m.SourceID, Policy: m.Policy, ExpectedHead: m.ExpectedHead, TotalDeltas: m.TotalDeltas, ShardCounts: m.ShardCounts, Chunks: m.Chunks})
	if err != nil {
		return nil, fmt.Errorf("%w: manifest re-encode: %v", ErrGlobalMergeCorrupt, err)
	}
	if globalMergeChecksum(body) != m.Checksum {
		return nil, fmt.Errorf("%w: manifest checksum mismatch", ErrGlobalMergeCorrupt)
	}
	return &m, nil
}

func globalMergeDecodeCommit(data []byte) (*globalMergeCommit, error) {
	var c globalMergeCommit
	if err := json.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("%w: commit record unparseable: %v", ErrGlobalMergeCorrupt, err)
	}
	if c.Version != 1 || (c.State != "committed" && c.State != "done") {
		return nil, fmt.Errorf("%w: commit record version/state invalid", ErrGlobalMergeCorrupt)
	}
	return &c, nil
}

// snapshotVectorLocked captures the per-shard sequences.
// Caller holds p.mu (the same capture SnapshotVector
// does, minus the lock).
func snapshotVectorLocked(p *PersistedSegDB) ([]uint64, error) {
	vec := make([]uint64, len(p.shards))
	for i, sh := range p.shards {
		seq, err := sh.sdb.SnapshotSeq()
		if err != nil {
			return nil, fmt.Errorf("yowza global merge: shard %d snapshot seq: %w", i, err)
		}
		vec[i] = seq
	}
	return vec, nil
}

// applyDeltasLocked applies deltas through the host WAL +
// shard path, one record per delta, exactly as facade
// Put/Delete do (dirty bit, WAL frame, abort record on
// a shard failure, spill-log pre-image when the version
// log is enabled). Caller holds p.mu for the whole
// call, so no facade writer can interleave: the merge
// is atomic with respect to every other writer and
// reader. A shard failure returns an error after the
// abort record; the caller (commit path) treats that
// as committed-but-incomplete, never as not-applied.
func applyDeltasLocked(p *PersistedSegDB, deltas []globalMergeDelta) error {
	if p.closed {
		return ErrPersistedClosed
	}
	if err := p.checkWritableLocked(); err != nil {
		return err
	}
	attempt := func(d globalMergeDelta) error {
		// Phase E epoch guard (see epoch_guard.go):
		// a merge delta must not push the epoch
		// past a C bound either. A guard flush here
		// is safe: the merge commit record is already
		// published and the WAL still replays the
		// applied prefix if a crash intervenes.
		if err := p.epochGuardLocked(d.Key); err != nil {
			return err
		}
		sh := p.shardFor(d.Key)
		// Pre-image for the spill version log, captured
		// before the mutation (same package, direct
		// shard read — no p.mu re-entry). The spill
		// record is written BEFORE the WAL frame (see
		// spill_history.go: an orphan record is
		// harmless, a missing record for a replayed
		// mutation is not). A spill failure returns
		// the sequence: no WAL record was written yet.
		var preVal []byte
		preOK := false
		if p.spill != nil {
			// PATCH2 Track 1 audit: the old code
			// silently recorded "no pre-image" on ANY
			// Get error. Absence is preOK=false;
			// any other error aborts the apply, and
			// recovery rolls the committed merge
			// forward — a wrong spill pre-image must
			// never be recorded as history.
			if v, err := sh.sdb.Get(d.Key); err == nil {
				preVal, preOK = append([]byte(nil), v...), true
			} else if !errors.Is(err, ErrSegDBNotFound) {
				return fmt.Errorf("yowza global merge: spill pre-image read for %q: %w", d.Key, err)
			}
		}
		p.markShardDirtyLocked(d.Key)
		p.seq++
		seq := p.seq
		if p.spill != nil {
			if err := p.spill.recordLocked(p.dir, seq, d.Key, preVal, preOK, p.mode == DurabilitySync); err != nil {
				p.seq--
				return fmt.Errorf("yowza global merge: spill record for %q: %w", d.Key, err)
			}
		}
		var frame []byte
		var op string
		if d.Deleted {
			frame = persistEncodeRecord(seq, persistOpDel, []byte(d.Key), nil)
			op = "delete"
		} else {
			frame = persistEncodeRecord(seq, persistOpPut, []byte(d.Key), d.Value)
			op = "put"
		}
		if err := p.appendWAL(seq, op, frame); err != nil {
			return err
		}
		var applyErr error
		if d.Deleted {
			applyErr = sh.sdb.Delete(d.Key)
		} else {
			applyErr = sh.sdb.Put(d.Key, d.Value)
		}
		if applyErr != nil {
			if abortErr := p.appendAbort(seq); abortErr != nil {
				return fmt.Errorf("yowza global merge: apply %q failed in wasm: %v; abort record also failed: %w", d.Key, applyErr, abortErr)
			}
			return fmt.Errorf("yowza global merge: apply %q failed in wasm: %w", d.Key, applyErr)
		}
		p.epochNoteLocked(p.ShardIndex(d.Key), d.Key)
		return nil
	}
	for _, d := range deltas {
		if err := attempt(d); err != nil {
			// Phase E epoch rollover, as in facade
			// Put: a history-full delta is aborted,
			// flushed into segments, and retried once.
			if errors.Is(err, ErrSegDBHistFull) {
				if ferr := p.flushLocked(); ferr != nil {
					return fmt.Errorf("yowza global merge: history-full flush failed: %v (original: %w)", ferr, err)
				}
				if err := attempt(d); err != nil {
					return err
				}
				continue
			}
			return err
		}
	}
	p.maybeAutoCheckpointLocked()
	return nil
}

// stageGlobalMerge writes chunk files then the manifest.
// It writes nothing else and publishes no commit record:
// staged files without a commit record are reclaimable.
func stageGlobalMerge(dir, id, sourceID, policy string, expectedHead []uint64, deltas []globalMergeDelta, shardCounts map[int]int, syncDir bool) (*globalMergeManifest, error) {
	man := &globalMergeManifest{
		Version: 1, MergeID: id, SourceID: sourceID, Policy: policy,
		ExpectedHead: append([]uint64(nil), expectedHead...),
		TotalDeltas:  len(deltas),
		ShardCounts:  map[string]int{},
	}
	for shard, n := range shardCounts {
		man.ShardCounts[strconv.Itoa(shard)] = n
	}
	for i := 0; i < len(deltas); i += globalMergeChunkKeys {
		end := i + globalMergeChunkKeys
		if end > len(deltas) {
			end = len(deltas)
		}
		idx := i / globalMergeChunkKeys
		data, sum, err := globalMergeEncodeChunk(id, idx, deltas[i:end])
		if err != nil {
			return nil, err
		}
		path := globalMergeChunkName(dir, id, idx)
		if err := globalMergeWriteSync(path, data, false, dir); err != nil {
			return nil, err
		}
		man.Chunks = append(man.Chunks, globalMergeChunkRef{File: filepath.Base(path), Count: end - i, Checksum: sum})
	}
	data, err := globalMergeEncodeManifest(man)
	if err != nil {
		return nil, err
	}
	manifestPath, _, _ := globalMergeFileNames(dir, id)
	if err := globalMergeWriteSync(manifestPath, data, syncDir, dir); err != nil {
		return nil, err
	}
	return man, nil
}

func loadGlobalMergeDeltas(dir string, man *globalMergeManifest) ([]globalMergeDelta, error) {
	var out []globalMergeDelta
	for i, ref := range man.Chunks {
		data, err := os.ReadFile(filepath.Join(dir, ref.File))
		if err != nil {
			return nil, fmt.Errorf("%w: chunk %d unreadable: %v", ErrGlobalMergeCorrupt, i, err)
		}
		deltas, err := globalMergeDecodeChunk(data, man.MergeID, i)
		if err != nil {
			return nil, err
		}
		if len(deltas) != ref.Count {
			return nil, fmt.Errorf("%w: chunk %d count %d, manifest says %d", ErrGlobalMergeCorrupt, i, len(deltas), ref.Count)
		}
		out = append(out, deltas...)
	}
	if len(out) != man.TotalDeltas {
		return nil, fmt.Errorf("%w: total deltas %d, manifest says %d", ErrGlobalMergeCorrupt, len(out), man.TotalDeltas)
	}
	return out, nil
}

func cleanupGlobalMergeFiles(dir, id string, chunkCount int) {
	manifestPath, commitPath, donePath := globalMergeFileNames(dir, id)
	_ = os.Remove(manifestPath)
	_ = os.Remove(commitPath)
	_ = os.Remove(donePath)
	for i := 0; i < chunkCount; i++ {
		_ = os.Remove(globalMergeChunkName(dir, id, i))
	}
	_ = os.Remove(manifestPath + ".tmp")
	_ = os.Remove(commitPath + ".tmp")
	_ = os.Remove(donePath + ".tmp")
}

// RecoverGlobalMerges completes or reclaims staged global
// merges in db's directory. It MUST run before serving
// reads after a crash (NewTimelineManager runs it at
// open); it must NOT run concurrently with an active
// merge. It returns the merge IDs rolled forward, sorted.
// A corrupt committed merge fails loudly with
// ErrGlobalMergeCorrupt and applies nothing further.
func RecoverGlobalMerges(db *PersistedSegDB) ([]string, error) {
	if db == nil {
		return nil, fmt.Errorf("yowza global merge: nil database")
	}
	dir := db.Dir()
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	// Index files by merge ID.
	commits := map[string]string{} // id -> commit path
	dones := map[string]bool{}
	manifests := map[string]string{}
	chunks := map[string]int{} // id -> max chunk index + 1 (approx via names)
	for _, e := range ents {
		name := e.Name()
		if !strings.HasPrefix(name, "global-merge-") {
			continue
		}
		rest := strings.TrimPrefix(name, "global-merge-")
		if strings.HasSuffix(name, ".commit.json") {
			commits[strings.TrimSuffix(rest, ".commit.json")] = filepath.Join(dir, name)
		} else if strings.HasSuffix(name, ".done.json") {
			dones[strings.TrimSuffix(rest, ".done.json")] = true
		} else if strings.HasSuffix(name, ".manifest.json") {
			manifests[strings.TrimSuffix(rest, ".manifest.json")] = filepath.Join(dir, name)
		} else if strings.Contains(name, ".chunk-") {
			// name: global-merge-<id>.chunk-<n>.json
			idx := strings.LastIndex(rest, ".chunk-")
			id := rest[:idx]
			numStr := strings.TrimSuffix(rest[idx+len(".chunk-"):], ".json")
			if n, err := strconv.Atoi(numStr); err == nil && n+1 > chunks[id] {
				chunks[id] = n + 1
			}
		}
	}
	var rolled []string
	// Done merges: cleanup only, never re-apply (a later
	// writer may already have overwritten a merged key).
	for id := range dones {
		n := chunks[id]
		if mp, ok := manifests[id]; ok {
			if data, err := os.ReadFile(mp); err == nil {
				if man, err := globalMergeDecodeManifest(data); err == nil {
					n = len(man.Chunks)
				}
			}
		}
		// PATCH2 Track 2 (integration addition): a
		// done merge's blobs are head-protected, so
		// any leftover staging record (e.g. the live
		// path's commit-resolve failed) is dropped
		// here. Best effort: a resolve failure leaves
		// a safe leak (GC keeps the blob), retried
		// on the next recovery — never a dangle.
		_ = ResolveMergeBlobs(dir, id, true)
		cleanupGlobalMergeFiles(dir, id, n)
	}
	// Committed merges without a done record: roll forward.
	for id, commitPath := range commits {
		if dones[id] {
			continue
		}
		data, err := os.ReadFile(commitPath)
		if err != nil {
			return rolled, err
		}
		commit, err := globalMergeDecodeCommit(data)
		if err != nil {
			return rolled, err
		}
		manPath := filepath.Join(dir, commit.ManifestFile)
		manData, err := os.ReadFile(manPath)
		if err != nil {
			return rolled, fmt.Errorf("%w: manifest for committed merge %s unreadable: %v", ErrGlobalMergeCorrupt, id, err)
		}
		man, err := globalMergeDecodeManifest(manData)
		if err != nil {
			return rolled, err
		}
		deltas, err := loadGlobalMergeDeltas(dir, man)
		if err != nil {
			return rolled, err
		}
		db.mu.Lock()
		applyErr := applyDeltasLocked(db, deltas)
		var newHead []uint64
		if applyErr == nil {
			newHead, _ = snapshotVectorLocked(db)
		}
		db.mu.Unlock()
		if applyErr != nil {
			return rolled, fmt.Errorf("%w: roll-forward of merge %s failed: %v", ErrGlobalMergeCommitted, id, applyErr)
		}
		done := globalMergeCommit{Version: 1, MergeID: id, ManifestFile: commit.ManifestFile, State: "done", NewHead: newHead}
		doneData, err := json.Marshal(done)
		if err != nil {
			return rolled, err
		}
		_, _, donePath := globalMergeFileNames(dir, id)
		if err := globalMergeWriteSync(donePath, doneData, db.Durability() == DurabilitySync, dir); err != nil {
			return rolled, err
		}
		// PATCH2 Track 2: rolled forward + done, so
		// the head protects the blobs; drop staging.
		// Best effort (see the done loop above).
		_ = ResolveMergeBlobs(dir, id, true)
		rolled = append(rolled, id)
		cleanupGlobalMergeFiles(dir, id, len(man.Chunks))
	}
	// Staged-only merges (manifest, no commit, no done):
	// reclaim — the merge never committed, so nothing
	// it staged may become visible.
	for id, manPath := range manifests {
		if commits[id] != "" || dones[id] {
			continue
		}
		n := chunks[id]
		if data, err := os.ReadFile(manPath); err == nil {
			if man, err := globalMergeDecodeManifest(data); err == nil {
				n = len(man.Chunks)
			}
		}
		cleanupGlobalMergeFiles(dir, id, n)
		// PATCH2 Track 2: staged-only means never
		// committed — staged blobs become collectible.
		// Best effort: a resolve failure leaves a safe
		// leak (GC keeps the blob), never a dangle.
		_ = ResolveMergeBlobs(dir, id, false)
	}
	// Orphan chunks (crash mid-staging, before the
	// manifest): reclaim. A chunk whose merge has a
	// manifest/commit/done was already handled above;
	// anything left here belongs to no record.
	for id, n := range chunks {
		if manifests[id] != "" || commits[id] != "" || dones[id] {
			continue
		}
		cleanupGlobalMergeFiles(dir, id, n)
	}
	sort.Strings(rolled)
	// PATCH2 Track 1: recovery completed, so the DB's
	// gate snapshot is stale if it was attached before
	// this call (e.g. at open on a crashed directory):
	// refresh it so reads are not refused for work
	// that no longer exists. A caller that recovers
	// via gate.Recover gets a re-scan there as well.
	db.RefreshRecoveryGate()
	return rolled, nil
}

// branchValuesForKeys streams the branch-vector view once
// and keeps only candidates. Memory is bounded by the
// candidate set plus one page, never by the DB size.
func (m *TimelineManager) branchValuesForKeys(vec []uint64, candidates map[string]bool) (map[string][]byte, error) {
	it, err := m.db.ScanStreaming("", vec)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	out := map[string][]byte{}
	for {
		batch, err := it.NextBatch(64)
		if err != nil {
			return nil, err
		}
		if len(batch) == 0 {
			break
		}
		for _, e := range batch {
			if candidates[string(e.Key)] {
				out[string(e.Key)] = append([]byte(nil), e.Val...)
			}
		}
	}
	return out, nil
}

// sourceFinalValue resolves one key through the chain
// overlays (leaf-first resolution, same rule as Get):
// the newest overlay entry at or below the fork-head
// limit wins; no entry means the branch value stands.
// mergeMainValue reads the current main value for merge
// conflict checking. PATCH2 Track 1: a Get error is NOT
// absence. Only ErrSegDBNotFound means "key absent"; any
// other error (storage, corruption, indeterminate state,
// closed DB) aborts the whole merge BEFORE staging or
// publish, so no journal or observable change is ever
// created on the assumption that the key is absent.
// This matches the Get-error separation from patch1;
// the old inline `mainOK := mainErr == nil` treated
// every error as absence. Kept as a separate helper so
// all three branches are directly unit-testable: a
// deterministic storage-error Get through MergeToMain
// is not reachable via public Go APIs (open verifies
// segment integrity first, and reads stay available on
// indeterminate shards), while the C-level -4 path
// (manifest/lookup error -> ErrSegDBInternal) is proven
// by patch1's C test and Go raw-export test — and this
// helper treats any such error identically via default.
func mergeMainValue(db *PersistedSegDB, key string) ([]byte, bool, error) {
	v, err := db.Get(key)
	switch {
	case err == nil:
		return append([]byte(nil), v...), true, nil
	case errors.Is(err, ErrSegDBNotFound):
		return nil, false, nil
	default:
		return nil, false, err
	}
}

func sourceFinalValue(chain []*timelineState, ts *timelineState, key string, branchVal []byte, branchOK bool) ([]byte, bool) {
	for i := len(chain) - 1; i >= 0; i-- {
		anc := chain[i]
		var limit uint64 = ^uint64(0)
		if anc != ts {
			limit = chain[i+1].branchParentHead
		}
		entries := anc.overlay[key]
		found := false
		var val []byte
		var deleted bool
		for _, e := range entries {
			if e.seq > limit {
				break
			}
			found = true
			val, deleted = e.val, e.deleted
		}
		if found {
			if deleted {
				return nil, false
			}
			return append([]byte(nil), val...), true
		}
	}
	return branchVal, branchOK
}

// patch3MergeDecisionHook is a test-only seam, added at merge time
// (it is NOT part of the user's patch3): when non-nil it runs after
// MergeToMain has computed its decision from planningHead and before
// the commit lock is taken. Production code leaves it nil. Tests set
// it to inject a writer exactly into the decision-to-commit window
// and always clear it via defer, so the nil-head regression test
// proves refusal by execution rather than by code reading.
var patch3MergeDecisionHook func()

// coordMergeAfterDecideHook is a test-only seam, nil in
// production (same pattern as patch3MergeDecisionHook): when
// non-nil it runs in the coordinated merge commit section
// AFTER the coordinator decision is published and BEFORE the
// global-merge commit record, and its error (if any) aborts
// the merge with the decision standing — exactly the
// crash-between-decision-and-apply window. Tests set it to
// inject that window deterministically and always clear it
// via defer. It must not call any DB or manager method:
// the commit section holds m.mu, the timeline locks, and
// p.mu, so such a call would deadlock. Production code
// leaves it nil, in which case the merge proceeds.
var coordMergeAfterDecideHook func() error

// coordMergeCall carries the coordinated-merge token for
// mergeToMainInternal: nil means the plain path.
type coordMergeCall struct {
	dec  CoordDecider
	opID string
}

// MergeToMain merges a timeline into the shared DB head
// as ONE atomic global merge (see the file header).
// expectedHead, when non-nil, must match the head at
// planning start. Even when nil, this method captures an
// internal planning head and rechecks it at commit, so all
// conflict decisions refer to one coherent head. A changed
// head returns ErrGlobalMergeStaleHead and nothing is applied.
func (m *TimelineManager) MergeToMain(sourceID string, policy TimelineConflictPolicy, expectedHead []uint64) (*GlobalMergeReport, error) {
	return m.mergeToMainInternal(sourceID, policy, expectedHead, nil)
}

// MergeToMainCoordinated is MergeToMain through the
// coordinator (Item 1): the caller presents an explicit
// decider + operation ID, and the merge publishes its
// decision in the coordinator log (under p.mu, after
// the fixed-head re-check, before the global-merge
// commit record and the apply) instead of being refused
// on a fenced DB. EVERY patch3 fixed-head guarantee is
// preserved: planningHead capture, caller expectedHead
// matching, stale-head rejection under p.mu in ALL
// cases including nil expectedHead, no-op path recheck,
// and staged-file/blob-pin reclaim only on definite
// no-decision outcomes. A nil decider is refused
// (tokenless coordinated merge does not exist); a
// decider for a different coordinator directory than
// the one this DB is fenced under is refused with
// ErrCoordFenceBypass. Retrying the same opID with
// the same merge payload after success returns success
// without re-applying (the decider's completion
// record, with an empty MergeID and the live head);
// retrying it with a different payload is refused with
// ErrCoordOpExists. Once the coordinator decision is
// published, every later failure is committed-but-
// incomplete (ErrGlobalMergeCommitted or
// ErrCoordCleanupPending): only coordinated recovery
// completes the merge, never a fresh retry.
func (m *TimelineManager) MergeToMainCoordinated(dec CoordDecider, opID string, sourceID string, policy TimelineConflictPolicy, expectedHead []uint64) (*GlobalMergeReport, error) {
	if dec == nil {
		return nil, fmt.Errorf("%w: nil coordinator decider (coordinated merge requires an explicit decider, it never falls back to plain merge)", ErrCoordReleased)
	}
	if !coordValidName(opID) {
		return nil, fmt.Errorf("yowza coordinator: invalid operation id %q", opID)
	}
	return m.mergeToMainInternal(sourceID, policy, expectedHead, &coordMergeCall{dec: dec, opID: opID})
}

// mergeToMainInternal is the shared MergeToMain /
// MergeToMainCoordinated implementation. coord == nil
// is the plain path (fence refusal, global-merge commit
// record as the only decision); coord != nil is the
// coordinated path (coordinator decision under p.mu
// after the head re-check, then the same global-merge
// commit record + apply). The decision computation
// (candidates, conflicts, report) is identical for
// both: fixed-head discipline and fencing discipline
// compose here, and neither is weakened to serve the
// other — the coordinated path adds a decision step,
// it never removes a check.
func (m *TimelineManager) mergeToMainInternal(sourceID string, policy TimelineConflictPolicy, expectedHead []uint64, coord *coordMergeCall) (*GlobalMergeReport, error) {
	if policy != TimelineConflictFail && policy != TimelineConflictSourceWins && policy != TimelineConflictTargetWins {
		return nil, fmt.Errorf("%w: unknown conflict policy %d", ErrTimelineInvalid, policy)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkMutableLocked(); err != nil {
		return nil, err
	}
	src, err := m.lookupLocked(sourceID)
	if err != nil {
		return nil, err
	}
	chain := m.chain(src)
	unlock := lockTimelinesOrdered(chain...)
	defer unlock()
	if src.poisoned != nil {
		return nil, src.poisoned
	}
	if src.broken != nil {
		return nil, src.broken
	}
	if src.deleted {
		return nil, fmt.Errorf("%w: %s", ErrTimelineDeleted, sourceID)
	}
	// Capture one authoritative planning head even when the caller
	// omitted expectedHead. The per-key reads below may interleave
	// with ordinary DB writers; the final equality check below makes
	// any such interleaving stale rather than allowing a mixed-head
	// decision to publish.
	planningHead, _, err := m.db.SnapshotVector()
	if err != nil {
		return nil, err
	}
	if expectedHead != nil && !equalUint64Slice(planningHead, expectedHead) {
		return nil, &GlobalMergeStaleHeadError{Expected: append([]uint64(nil), expectedHead...), Actual: planningHead}
	}
	// Plain path: a registered coordinated DB may only be
	// mutated by a coordinated path. Refuse early; the same
	// check is repeated under p.mu immediately before
	// publishing the decision. Coordinated path: the
	// decider IS the authorized path, so the fence
	// refusal is replaced by a live-token check plus a
	// coordinator-directory match (a decider for a
	// different coordinator than the DB is fenced under
	// is a bypass attempt); the authoritative
	// fence/decision step still happens under p.mu.
	if coord == nil {
		if err := CheckCoordWriteFence(m.db); err != nil {
			return nil, err
		}
	} else {
		if fdir, ok := coordFenceDir(m.db); ok && fdir != coord.dec.CoordDir() {
			return nil, fmt.Errorf("%w: decider coordinator dir %q does not match fenced coordinator dir %q", ErrCoordFenceBypass, coord.dec.CoordDir(), fdir)
		}
		if err := coord.dec.Validate(); err != nil {
			return nil, err
		}
	}
	// Candidate keys: exactly the keys the chain ever
	// wrote. Every other key is identical between the
	// branch view and the source view by construction.
	candidates := map[string]bool{}
	for _, anc := range chain {
		for k := range anc.overlay {
			candidates[k] = true
		}
	}
	branchVals, err := m.branchValuesForKeys(src.branchVector, candidates)
	if err != nil {
		return nil, err
	}
	var applied []globalMergeDelta
	var appliedKeys, identicalKeys, conflictedKeys []string
	shardCounts := map[int]int{}
	for k := range candidates {
		branchVal, branchOK := branchVals[k]
		finalVal, finalOK := sourceFinalValue(chain, src, k, branchVal, branchOK)
		changed := !(finalOK == branchOK && (!finalOK || string(finalVal) == string(branchVal)))
		if !changed {
			continue
		}
		// Current main value (one Get per changed key).
		mainVal, mainOK, err := mergeMainValue(m.db, k)
		if err != nil {
			return nil, err
		}
		mainMatchesBranch := mainOK == branchOK && (!mainOK || string(mainVal) == string(branchVal))
		if mainMatchesBranch {
			// No intervening main change: apply.
			if finalOK {
				applied = append(applied, globalMergeDelta{Key: k, Value: append([]byte(nil), finalVal...)})
			} else {
				applied = append(applied, globalMergeDelta{Key: k, Deleted: true})
			}
			appliedKeys = append(appliedKeys, k)
			shardCounts[m.db.ShardIndex(k)]++
			continue
		}
		// Main moved since the branch point: conflict,
		// unless main already equals the source final.
		if mainOK == finalOK && (!mainOK || string(mainVal) == string(finalVal)) {
			identicalKeys = append(identicalKeys, k)
			continue
		}
		conflictedKeys = append(conflictedKeys, k)
	}
	sort.Strings(appliedKeys)
	sort.Strings(identicalKeys)
	sort.Strings(conflictedKeys)
	if len(conflictedKeys) > 0 && policy == TimelineConflictFail {
		// Do not report a conflict computed from mixed live heads.
		// A monotonically advancing snapshot vector makes this check
		// sufficient to detect any write that interleaved the reads.
		current, _, headErr := m.db.SnapshotVector()
		if headErr != nil {
			return nil, headErr
		}
		if !equalUint64Slice(current, planningHead) {
			return nil, &GlobalMergeStaleHeadError{Expected: append([]uint64(nil), planningHead...), Actual: current}
		}
		return nil, &GlobalMergeConflictError{SourceID: sourceID, Keys: append([]string(nil), conflictedKeys...)}
	}
	if len(conflictedKeys) > 0 && policy == TimelineConflictSourceWins {
		// Resolve conflicted keys to the source final.
		// (Target-wins keeps main: conflicted keys are
		// reported but not applied.)
		for _, k := range conflictedKeys {
			branchVal, branchOK := branchVals[k]
			finalVal, finalOK := sourceFinalValue(chain, src, k, branchVal, branchOK)
			if finalOK {
				applied = append(applied, globalMergeDelta{Key: k, Value: append([]byte(nil), finalVal...)})
			} else {
				applied = append(applied, globalMergeDelta{Key: k, Deleted: true})
			}
			appliedKeys = append(appliedKeys, k)
			shardCounts[m.db.ShardIndex(k)]++
		}
		sort.Strings(appliedKeys)
	}
	sort.Slice(applied, func(i, j int) bool { return applied[i].Key < applied[j].Key })
	report := &GlobalMergeReport{
		SourceID: sourceID, AppliedKeys: appliedKeys, IdenticalKeys: identicalKeys,
		ConflictedKeys: conflictedKeys, Policy: policy,
		ExpectedHead: append([]uint64(nil), planningHead...), NewHead: append([]uint64(nil), planningHead...),
		ShardCounts: shardCounts,
	}
	if len(applied) == 0 {
		// A no-op still needs a coherent decision point; do not return
		// success if the head moved while conflicts/identical keys were
		// being evaluated.
		if patch3MergeDecisionHook != nil {
			patch3MergeDecisionHook()
		}
		p := m.db
		p.mu.Lock()
		live, lerr := snapshotVectorLocked(p)
		if lerr == nil && !equalUint64Slice(live, planningHead) {
			lerr = &GlobalMergeStaleHeadError{Expected: append([]uint64(nil), planningHead...), Actual: live}
		}
		// Coordinated no-op: no decision is published
		// (there is nothing to decide), but the token
		// must still be live at the decision point —
		// a stale/released token must not be able to
		// claim a coordinated success.
		if lerr == nil && coord != nil {
			lerr = coord.dec.Validate()
		}
		p.mu.Unlock()
		if lerr != nil {
			return nil, lerr
		}
		report.NewHead = append([]uint64(nil), live...)
		return report, nil
	}
	// STAGE (no commit record yet: reclaimable).
	mergeID := globalMergeID(sourceID)
	syncDir := m.db.Durability() == DurabilitySync
	man, err := stageGlobalMerge(m.dir, mergeID, sourceID, policy.String(), planningHead, applied, shardCounts, syncDir)
	if err != nil {
		cleanupGlobalMergeFiles(m.dir, mergeID, (len(applied)+globalMergeChunkKeys-1)/globalMergeChunkKeys)
		return nil, fmt.Errorf("yowza global merge: staging failed: %w", err)
	}
	report.MergeID = mergeID
	report.ChunkCount = len(man.Chunks)
	// PATCH2 Track 2: stage registry-recorded blob
	// references for this merge BEFORE the commit
	// record exists, so a crash between staging and
	// commit resolves via recovery, never dangles.
	// Only keys recorded in blob-key-registry.json
	// (typed, written at write time) are staged —
	// values are never content-sniffed. A staging
	// failure aborts the merge before any commit.
	if err := StageMergeBlobs(m.dir, mergeID, applied); err != nil {
		_ = ResolveMergeBlobs(m.dir, mergeID, false)
		cleanupGlobalMergeFiles(m.dir, mergeID, len(man.Chunks))
		return nil, fmt.Errorf("yowza global merge: blob staging failed: %w", err)
	}
	manifestPath, commitPath, _ := globalMergeFileNames(m.dir, mergeID)
	// COMMIT + APPLY under p.mu: the commit record is
	// published only after the expected-head re-check,
	// and the apply cannot interleave with any facade
	// writer or reader.
	if patch3MergeDecisionHook != nil {
		patch3MergeDecisionHook()
	}
	p := m.db
	p.mu.Lock()
	// Plain path: the fence re-check under p.mu closes
	// the registration race. Coordinated path: the
	// decider is the authorized path, so the fence
	// refusal is replaced by the coordinator-dir
	// re-check (registration may have changed since
	// the early check); the decision itself is
	// published below, still under p.mu.
	if coord == nil {
		if ferr := CheckCoordWriteFence(p); ferr != nil {
			p.mu.Unlock()
			_ = ResolveMergeBlobs(m.dir, mergeID, false)
			cleanupGlobalMergeFiles(m.dir, mergeID, len(man.Chunks))
			return nil, ferr
		}
	} else {
		if fdir, ok := coordFenceDir(p); ok && fdir != coord.dec.CoordDir() {
			p.mu.Unlock()
			_ = ResolveMergeBlobs(m.dir, mergeID, false)
			cleanupGlobalMergeFiles(m.dir, mergeID, len(man.Chunks))
			return nil, fmt.Errorf("%w: decider coordinator dir %q does not match fenced coordinator dir %q", ErrCoordFenceBypass, coord.dec.CoordDir(), fdir)
		}
	}
	live, lerr := snapshotVectorLocked(p)
	if lerr != nil {
		p.mu.Unlock()
		// PATCH2 Track 2: no-decision failure — the
		// staged blobs become collectible.
		_ = ResolveMergeBlobs(m.dir, mergeID, false)
		cleanupGlobalMergeFiles(m.dir, mergeID, len(man.Chunks))
		return nil, lerr
	}
	if !equalUint64Slice(live, planningHead) {
		p.mu.Unlock()
		// PATCH2 Track 2: no-decision failure — the
		// staged blobs become collectible.
		_ = ResolveMergeBlobs(m.dir, mergeID, false)
		cleanupGlobalMergeFiles(m.dir, mergeID, len(man.Chunks))
		return nil, &GlobalMergeStaleHeadError{Expected: append([]uint64(nil), planningHead...), Actual: live}
	}
	// Coordinated path: the coordinator decision is
	// published HERE, under p.mu, AFTER the head
	// re-check and BEFORE the global-merge commit
	// record and the apply. A writer landing between
	// the planning decision (patch3MergeDecisionHook)
	// and this point is therefore caught by the
	// re-check above, before any decision exists —
	// never after it. No writer can land between the
	// decision and the apply: p.mu is held throughout,
	// and no other process can hold this DB directory.
	if coord != nil {
		decWrites := map[string][]globalMergeDelta{coordDeciderShard: applied}
		status, derr := coord.dec.Decide(coord.opID, CoordDecisionMerge, p.Dir(), mergeID, decWrites)
		if derr != nil {
			// An opID-taken refusal (ErrCoordOpExists)
			// is already the final answer: the opID
			// belongs to a DIFFERENT operation,
			// whose decision / staging must not be
			// touched — discarding here would
			// misreport a clean refusal as a
			// decision-stands committed outcome.
			if errors.Is(derr, ErrCoordOpExists) {
				p.mu.Unlock()
				_ = ResolveMergeBlobs(m.dir, mergeID, false)
				cleanupGlobalMergeFiles(m.dir, mergeID, len(man.Chunks))
				return nil, derr
			}
			if derr2 := coord.dec.DiscardUndecided(coord.opID); derr2 != nil {
				p.mu.Unlock()
				// Decision stands: leave staging +
				// blobs for coordinated recovery.
				// Do NOT reclaim, do NOT resolve.
				return nil, fmt.Errorf("%w: merge %s: coordinator decision stands (original: %v); run coordinated recovery", ErrGlobalMergeCommitted, mergeID, derr)
			}
			p.mu.Unlock()
			_ = ResolveMergeBlobs(m.dir, mergeID, false)
			cleanupGlobalMergeFiles(m.dir, mergeID, len(man.Chunks))
			return nil, derr
		}
		if status == CoordDecisionCompleted {
			p.mu.Unlock()
			// Idempotent retry: the values are
			// already in the head from the earlier
			// application. Reclaim THIS attempt's
			// staging (it must not be mistaken for
			// a new merge) and report the already-
			// applied write set against the live
			// head, with no new merge ID.
			_ = ResolveMergeBlobs(m.dir, mergeID, false)
			cleanupGlobalMergeFiles(m.dir, mergeID, len(man.Chunks))
			report.MergeID = ""
			report.ChunkCount = 0
			report.NewHead = append([]uint64(nil), live...)
			return report, nil
		}
		if coordMergeAfterDecideHook != nil {
			if herr := coordMergeAfterDecideHook(); herr != nil {
				p.mu.Unlock()
				// Decision stands, nothing applied:
				// leave staging + blobs for
				// coordinated recovery.
				return nil, fmt.Errorf("%w: merge %s: coordinator decision stands (hook aborted commit: %v); run coordinated recovery", ErrGlobalMergeCommitted, mergeID, herr)
			}
		}
	}
	commit := globalMergeCommit{Version: 1, MergeID: mergeID, ManifestFile: filepath.Base(manifestPath), State: "committed"}
	commitData, _ := json.Marshal(commit)
	if err := globalMergeWriteSync(commitPath, commitData, syncDir, m.dir); err != nil {
		p.mu.Unlock()
		if coord == nil {
			// PATCH2 Track 2: no-decision failure (the
			// commit record did not publish) — staged
			// blobs become collectible. Recovery's
			// staged-only reclaim also resolves, and if
			// the record in fact landed (rename done,
			// dir-sync failed), recovery rolls forward
			// and commit-resolves instead; either way
			// no blob dangles.
			_ = ResolveMergeBlobs(m.dir, mergeID, false)
			return nil, fmt.Errorf("yowza global merge: commit record publish failed (merge NOT committed; staged files remain for recovery to reclaim): %w", err)
		}
		// Coordinated: the coordinator decision
		// stands, so this is NOT a no-decision
		// failure. Staged files + blob staging are
		// deliberately left for coordinated recovery
		// (which commit-resolves blobs after
		// applying): resolving false here would make
		// blobs collectible while the decided merge
		// still needs them. If the commit record in
		// fact landed, global-merge recovery also
		// rolls forward; both recoveries are
		// idempotent, so either order converges.
		return nil, fmt.Errorf("yowza global merge: commit record publish failed (coordinator decision stands for merge %s; run coordinated recovery): %w", mergeID, err)
	}
	// Commit record durable: from here the merge WILL
	// stand (in this process or via recovery).
	// PATCH2 Track 2: blob staging is deliberately
	// NOT resolved on the apply-failure path below
	// (ErrGlobalMergeCommitted) — recovery resolves
	// it after rolling forward.
	applyErr := applyDeltasLocked(p, applied)
	var newHead []uint64
	if applyErr == nil {
		newHead, _ = snapshotVectorLocked(p)
	}
	p.mu.Unlock()
	if applyErr != nil {
		return nil, fmt.Errorf("%w: merge %s: %v", ErrGlobalMergeCommitted, mergeID, applyErr)
	}
	done := globalMergeCommit{Version: 1, MergeID: mergeID, ManifestFile: filepath.Base(manifestPath), State: "done", NewHead: newHead}
	doneData, _ := json.Marshal(done)
	_, _, donePath := globalMergeFileNames(m.dir, mergeID)
	if err := globalMergeWriteSync(donePath, doneData, syncDir, m.dir); err != nil {
		// Done record lost but apply finished: recovery
		// will roll forward idempotently; report the
		// merge as committed (it is — the data is in).
		return nil, fmt.Errorf("%w: merge %s applied but done record publish failed: %v", ErrGlobalMergeCommitted, mergeID, err)
	}
	// PATCH2 Track 2: apply + done are durable, so
	// the head now protects the blobs; drop the
	// staging record. A resolve failure here leaves
	// a safe leak (GC keeps the blob) that the next
	// recovery's done loop drops — never a dangle.
	if err := ResolveMergeBlobs(m.dir, mergeID, true); err != nil {
		return nil, fmt.Errorf("%w: merge %s applied but blob staging resolve failed: %v", ErrGlobalMergeCommitted, mergeID, err)
	}
	report.NewHead = newHead
	cleanupGlobalMergeFiles(m.dir, mergeID, len(man.Chunks))
	if coord != nil {
		if cerr := coord.dec.Complete(coord.opID, CoordDecisionMerge, m.db.Dir(), mergeID, map[string][]globalMergeDelta{coordDeciderShard: applied}); cerr != nil {
			return nil, fmt.Errorf("%w: merge %s applied but coordinator cleanup failed: %v", ErrCoordCleanupPending, mergeID, cerr)
		}
	}
	return report, nil
}
