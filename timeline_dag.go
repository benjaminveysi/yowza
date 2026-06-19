package yowza

// Phase C, C2 — PARALLEL TIMELINE V2: COMMIT DAG.
//
// This file replaces the "materialize everything" shape for
// NEW operations (DAG forks, general merges, delta-segment
// checkpoints) while keeping the existing timeline
// operations, WAL format, and registry v1 format readable
// and working. Nothing here deletes a current branch:
// registry v1 documents load unchanged (Phase C fields
// are omitempty), WAL types 1-3 replay unchanged, and
// migration to the DAG happens per timeline, with tests
// and a backup-friendly layout (registry + WALs + pins
// + delta files are all plain files in the same
// directory; copying the directory before opening with
// a Phase C manager is a complete backup).
//
// MODEL. An immutable commit graph:
//
//	TimelineCommit{
//	  ID, TimelineID, ParentIDs[], BaseSnapshotID,
//	  Sequence, DeltaManifest, StateRoot, Checksum }
//	- a normal (fork/head) commit has one parent (or none
//	  for a DB root / legacy boundary node);
//	- a merge commit has two parents (source head state,
//	  target head state).
//	- DeltaManifest points to an immutable set of sorted
//	  changes (for merge commits: the delta-segment file;
//	  for fork/head commits it is empty — the changes
//	  themselves still live in the timeline WAL, which
//	  remains the authority for per-key history).
//	- StateRoot identifies the logical state identity:
//	  for a root commit it is the snapshot-root digest;
//	  otherwise it is a hash over the parents' state
//	  roots, the delta digest, and the sequence.
//	- Checksum is a hash over the canonical commit record
//	  (integrity of the node itself, distinct from
//	  StateRoot, which identifies the state it names).
//
// NODE IDENTITY (canonical, and this is what makes
// recovery exact): the state of timeline number N at
// head sequence S is named "c-state-N-S". A timeline's
// overlay is append-only, so (N, S) determines the
// state: two nodes with the same canonical ID name the
// same state, and a node can be re-derived after a
// crash from a WAL record alone (merge-ref records
// carry source/target head sequences, so the merge
// node's parents are re-derivable — see
// healDAGMergeCommit). A DB root node is
// "c-root-<snapshot-root-id>". Legacy timelines whose
// pre-DAG history was never a DAG node get a BOUNDARY
// node (parents empty, BaseSnapshotID empty): a
// boundary node claims NO ancestry it does not have,
// so merge-base search may fail conservatively
// (NoCommonBase, or the legacy direct-parent/sibling
// fallback) but never invents a base.
//
// FORK COST. A DAG fork records a reference to a commit
// + a base snapshot and never scans the database:
// ForkFromDBDAG / ForkDAG call Flush (a per-shard heap
// checkpoint — cost scales with shard count and heap
// size, not with a key scan), SnapshotVector +
// Generation + CheckpointSeq (metadata), createBranchPin
// (metadata + pins.json publish), and the registry
// publish. They never call scanAtVector, and the
// fingerprint is NOT computed (branchFingerprint is
// empty for DAG timelines). The open-time branch
// identity check for such timelines is metadata + pin
// (pin exists, active, vector matches) + registry
// commit checksum, without a full view rebuild. The
// fingerprint is kept for legacy timelines as extra
// validation until the root identity is proven — it is
// not suddenly replaced by an unproven rule.
//
// SNAPSHOT ROOT. A snapshot needs a durable ID + a
// logical digest + the manifest root of all shards +
// effective sequences + a durable pin:
//
//	TimelineSnapshotRoot{
//	  ID, Vector, Generation, CheckpointSeq, Digest }
//	Digest = SHA-256 over (shard count, per-shard
//	vector entries, generation, checkpoint sequence).
// This is a VERSIONED-POINT identity, not a content
// hash: it names "the database at this checkpoint
// generation and vector", and the durable pin is what
// keeps that point's data alive across flush and
// compaction (the pin's leases defer compacts that
// would drop it, and pins.json + lease recovery on
// DB open re-establish the pin after a restart).
// Honest limit: the digest does not by itself prove
// two different databases hold the same keys — that
// is what the legacy fingerprint does, and why DAG
// timelines keep the pin + registry commit as their
// identity check rather than claiming content proof.
//
// DELTA SEGMENTS + CHECKPOINTS. A delta segment is a
// file of JSON lines, one entry per change, sorted by
// (key, sequence), plus a final {"checksum": ...} line
// whose value is the SHA-256 of all preceding bytes.
// timelineWriteDeltaSegment streams entries to a temp
// file, fsyncs, renames to the final name, and (in
// Sync mode) syncs the directory — the rename is the
// commit point for the segment. timelineLoadDeltaSegment
// re-verifies order, count (when given), and checksum,
// and fails loudly on any violation. A checkpoint
// (CheckpointTimeline) is a delta segment holding a
// timeline's whole overlay; the WAL remains
// authoritative and is NOT truncated by checkpointing
// in this phase (deleting a WAL prefix needs a
// WAL-segment format with an explicit starting
// sequence, which the timeline WAL does not yet have
// — see the honest limits in TIMELINE_V2_NOTES.md).
// The checkpoint is therefore a validated, durable,
// sorted snapshot of the overlay that a future phase
// can truncate against, and LoadCheckpoint proves it
// reproduces the live view exactly.
// AXIS D CHANGE: truncation now exists, via a type-5
// checkpoint-base WAL record naming the checkpoint
// segment (see timeline_store.go): the truncated WAL
// replays to the identical overlay by expanding the
// checkpoint, so both sides of the truncation rename
// are valid replays. Use
// CheckpointAndTruncateTimelineWAL or
// TruncateTimelineWALToCheckpoint; truncation
// additionally verifies the checkpoint against the
// live overlay before cutting.
//
// RETENTION. OldestPinnedSequence reports the retention
// floor: the minimum checkpoint sequence over all
// active durable pins (the scalar diagnostic) — the
// precise floor is per shard, and it IS the pins'
// vectors: while the oldest pinned vector is V_min,
// for each key the latest version at or before V_min
// and all versions after it that any live snapshot or
// lineage needs must stay recoverable, and any older
// version needed by no live snapshot/lineage is
// reclaimable. Compaction already defers on pinned
// leases; this function names the floor so a future
// reclaimer can use it.
//
// LARGE MERGES. MergeGeneral stages the merge's applied
// keys as a sorted immutable delta segment in temp
// storage, validates it (checksum + count + order),
// then records ONE small commit record (a type-4 WAL
// merge-ref record: merge ID, parent heads, policy,
// digest, delta manifest — no applied keys inside it).
// That commit record is the commit point: deltas
// without a commit record are orphans, cleaned on the
// next open, and never partially applied; deltas with
// a valid commit record must fully replay (delta
// missing/corrupt fails open) or the merge is not
// applied at all. On a WAL-append failure the staged
// delta file is deliberately LEFT in place: the append
// may be indeterminate (poison), and if the record
// replays on reopen its delta must be there; if the
// record does not replay, the delta is an orphan and
// is cleaned then. Deleting it on append failure would
// be the half-apply hazard this protocol exists to
// close.
//
// MERGE BASE. findMergeBase works on the commit graph:
// ancestors of both heads, common ancestors, maximal
// common ancestors (a common ancestor that is not an
// ancestor of any other common ancestor). One maximal
// common ancestor -> normal 3-way merge. Several
// maximal common ancestors -> ErrAmbiguousMergeBase
// from findMergeBase itself. PATCH 2 (Track 4): MergeGeneral
// falls back to a recursive virtual base
// (recursiveBaseView) ONLY when that base is exactly
// determined: the 2-candidate fold is symmetric, and a
// 3- or 4-candidate base is accepted ONLY when folding
// the candidates in EVERY order yields the same
// conflict-free view, so no candidate ordering (and no
// arbitrary representative pick) can decide the result.
// Any other ambiguous case — a conflicting fold, folds
// that disagree across orders, or 5+ candidates (too
// many orders to check) — returns
// ErrAmbiguousMergeBase, never a guess. No common
// ancestor -> the legacy
// direct-parent/sibling rules are tried as special
// cases (they are simple instances of the general
// algorithm, not a separate feature), and only if
// those also fail is ErrTimelineNoCommonBase returned.
// MergeGeneral controls BOTH expectedSourceHead and
// expectedTargetHead: a merge on a changed source
// does not silently rebase.
//
// HONEST LIMITS (this file, see also the notes):
// - Views are still materialized for a general merge
//   (base/source/target views are full maps): the DAG
//   contribution here is base finding, dual head
//   control, and the delta commit protocol, not a
//   streaming view merge. A streaming merge over
//   sorted delta segments is future work.
// - The conflicted/identical key lists in a
//   MergeGeneral report are still collected in memory;
//   MergeGeneralStreamed (Axis D) is the bounded form:
//   keys go to a caller sink in sorted order and the
//   applied delta streams to disk, with counts only in
//   the report (fail-policy errors still name their
//   keys). The APPLIED set is what moves to the delta
//   segment in both forms.
// - Checkpointing does not yet truncate WALs (above).
//   AXIS D: superseded — truncation exists (see above).

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"os"
	"sort"
	"strings"
	"sync/atomic"
)

// ErrTimelineAmbiguousMergeBase: several maximal common
// ancestors exist and no unambiguous base can be chosen.
// The caller must select a base explicitly (or merge
// stepwise); the merge is refused, never guessed.
var ErrTimelineAmbiguousMergeBase = errors.New("yowza timeline: ambiguous merge base: multiple maximal common ancestors")

// TimelineAmbiguousMergeBaseError lists the candidates.
type TimelineAmbiguousMergeBaseError struct {
	SourceID   string
	TargetID   string
	Candidates []string
}

func (e *TimelineAmbiguousMergeBaseError) Error() string {
	return fmt.Sprintf("yowza timeline: merge %s -> %s has ambiguous base, candidates %v", e.SourceID, e.TargetID, e.Candidates)
}

func (e *TimelineAmbiguousMergeBaseError) Unwrap() error {
	return ErrTimelineAmbiguousMergeBase
}

// TimelineSnapshotRoot is one durable snapshot identity
// (see the file header). Vector is index-aligned with
// the database's shards.
type TimelineSnapshotRoot struct {
	ID            string   `json:"id"`
	Vector        []uint64 `json:"vector"`
	Generation    uint64   `json:"generation"`
	CheckpointSeq uint64   `json:"checkpoint_seq"`
	Digest        string   `json:"digest"`
}

// DeltaSegmentRef names one immutable sorted delta segment.
type DeltaSegmentRef struct {
	File     string `json:"file"`
	Keys     int    `json:"keys"`
	Checksum string `json:"checksum"`
	FirstKey string `json:"first_key,omitempty"`
	LastKey  string `json:"last_key,omitempty"`
}

// DeltaManifest points to an immutable set of sorted changes.
type DeltaManifest struct {
	Segments  []DeltaSegmentRef `json:"segments,omitempty"`
	TotalKeys int               `json:"total_keys"`
	Digest    string            `json:"digest,omitempty"`
}

// TimelineCommit is one immutable commit-graph node.
type TimelineCommit struct {
	ID             string        `json:"id"`
	TimelineID     string        `json:"timeline_id"`
	ParentIDs      []string      `json:"parent_ids,omitempty"`
	BaseSnapshotID string        `json:"base_snapshot_id,omitempty"`
	Sequence       uint64        `json:"sequence"`
	DeltaManifest  DeltaManifest `json:"delta_manifest"`
	StateRoot      string        `json:"state_root"`
	Checksum       string        `json:"checksum"`
}

// timelineScanAtVectorCalls counts scanAtVector calls
// process-wide. DAG-fork tests assert it does not move
// across a DAG fork of a large database.
var timelineScanAtVectorCalls int64

func timelineScanCounterAdd(n int64) { atomic.AddInt64(&timelineScanAtVectorCalls, n) }

// TimelineScanAtVectorCalls returns the current counter
// (test/diagnostic use).
func TimelineScanAtVectorCalls() int64 { return atomic.LoadInt64(&timelineScanAtVectorCalls) }

func dagHash(parts ...string) string {
	h := sha256.New()
	for _, p := range parts {
		h.Write([]byte(p))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

func dagStateID(number uint64, seq uint64) string {
	return fmt.Sprintf("c-state-%d-%d", number, seq)
}

// snapshotRootFor builds a snapshot-root identity from
// metadata only (no scan). Generation and checkpoint
// sequence come from the PersistedSegDB, so the root
// names a specific checkpoint generation + vector.
func snapshotRootFor(vec []uint64, generation, checkpointSeq uint64) *TimelineSnapshotRoot {
	h := sha256.New()
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], uint64(len(vec)))
	h.Write(tmp[:])
	for _, v := range vec {
		binary.LittleEndian.PutUint64(tmp[:], v)
		h.Write(tmp[:])
	}
	binary.LittleEndian.PutUint64(tmp[:], generation)
	h.Write(tmp[:])
	binary.LittleEndian.PutUint64(tmp[:], checkpointSeq)
	h.Write(tmp[:])
	digest := hex.EncodeToString(h.Sum(nil))
	return &TimelineSnapshotRoot{
		ID:            "snap-" + digest[:16],
		Vector:        append([]uint64(nil), vec...),
		Generation:    generation,
		CheckpointSeq: checkpointSeq,
		Digest:        digest,
	}
}

func (m *TimelineManager) sortedDAGCommits() []*TimelineCommit {
	out := make([]*TimelineCommit, 0, len(m.dagCommits))
	for _, c := range m.dagCommits {
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (m *TimelineManager) sortedSnapshotRoots() []*TimelineSnapshotRoot {
	out := make([]*TimelineSnapshotRoot, 0, len(m.snapshotRoots))
	for _, r := range m.snapshotRoots {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// dagCommitChecksum hashes the canonical commit record
// (every field except Checksum itself).
func dagCommitChecksum(c *TimelineCommit) string {
	raw, _ := json.Marshal(struct {
		ID             string        `json:"id"`
		TimelineID     string        `json:"timeline_id"`
		ParentIDs      []string      `json:"parent_ids,omitempty"`
		BaseSnapshotID string        `json:"base_snapshot_id,omitempty"`
		Sequence       uint64        `json:"sequence"`
		DeltaManifest  DeltaManifest `json:"delta_manifest"`
		StateRoot      string        `json:"state_root"`
	}{
		ID: c.ID, TimelineID: c.TimelineID, ParentIDs: c.ParentIDs,
		BaseSnapshotID: c.BaseSnapshotID, Sequence: c.Sequence,
		DeltaManifest: c.DeltaManifest, StateRoot: c.StateRoot,
	})
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// newDAGCommit creates, checksums, and registers a commit
// node. Caller holds the manager write lock.
func (m *TimelineManager) newDAGCommit(id, timelineID string, parents []string, baseSnapshotID string, seq uint64, manifest DeltaManifest) *TimelineCommit {
	roots := make([]string, 0, len(parents))
	for _, pid := range parents {
		if pc, ok := m.dagCommits[pid]; ok {
			roots = append(roots, pc.StateRoot)
		} else {
			roots = append(roots, "missing:"+pid)
		}
	}
	digest := manifest.Digest
	if digest == "" && len(manifest.Segments) > 0 {
		digest = manifest.Segments[0].Checksum
	}
	stateRoot := dagHash(append(append([]string{}, roots...), digest, fmt.Sprintf("%d", seq))...)
	c := &TimelineCommit{
		ID: id, TimelineID: timelineID,
		ParentIDs: append([]string(nil), parents...),
		BaseSnapshotID: baseSnapshotID, Sequence: seq,
		DeltaManifest: manifest, StateRoot: stateRoot,
	}
	c.Checksum = dagCommitChecksum(c)
	m.dagCommits[c.ID] = c
	return c
}

// ensureHeadCommit returns the DAG node for ts's current
// head state, creating it if needed. Caller holds the
// manager write lock (and ts's lock for headSeq reads
// in the Fork/Merge paths, which hold the chain locks).
// A node whose ID already exists is returned unchanged:
// (timeline, sequence) determines the state, so an
// existing node for that state IS the head node.
func (m *TimelineManager) ensureHeadCommit(ts *timelineState) *TimelineCommit {
	id := dagStateID(ts.number, ts.headSeq)
	if c, ok := m.dagCommits[id]; ok {
		ts.dagHead = id
		return c
	}
	var parents []string
	if ts.dagHead != "" {
		parents = []string{ts.dagHead}
	}
	c := m.newDAGCommit(id, ts.id, parents, ts.snapshotRootID, ts.headSeq, DeltaManifest{})
	ts.dagHead = id
	return c
}

// healDAGMergeCommit re-derives a merge DAG node from a
// committed merge-ref WAL record (crash between WAL
// fsync and registry publish). Parent node IDs are
// canonical (c-state-<number>-<seq>), so they are
// exact, not guessed; a missing parent is created as a
// boundary stub (no parents), which truncates ancestry
// conservatively rather than inventing it.
func (m *TimelineManager) healDAGMergeCommit(ts *timelineState, ref *timelineMergeRefPayload) {
	src, okSrc := m.timelines[ref.SourceID]
	if !okSrc {
		return
	}
	id := dagStateID(ts.number, ref.NewHead)
	if _, ok := m.dagCommits[id]; ok {
		ts.dagHead = id
		return
	}
	srcParent := dagStateID(src.number, ref.SourceHead)
	if _, ok := m.dagCommits[srcParent]; !ok {
		m.newDAGCommit(srcParent, ref.SourceID, nil, "", ref.SourceHead, DeltaManifest{})
	}
	dstParent := dagStateID(ts.number, ref.TargetBefore)
	if _, ok := m.dagCommits[dstParent]; !ok {
		m.newDAGCommit(dstParent, ts.id, nil, "", ref.TargetBefore, DeltaManifest{})
	}
	manifest := DeltaManifest{
		Segments:  []DeltaSegmentRef{{File: ref.DeltaFile, Keys: ref.DeltaKeys, Checksum: ref.DeltaSHA256}},
		TotalKeys: ref.DeltaKeys,
		Digest:    ref.DeltaSHA256,
	}
	m.newDAGCommit(id, ts.id, []string{srcParent, dstParent}, ref.BaseCommitID, ref.NewHead, manifest)
	ts.dagHead = id
}

// dagAncestors returns the ancestor set of nodeID
// (including nodeID itself) by following parent links.
func (m *TimelineManager) dagAncestors(nodeID string) map[string]bool {
	out := map[string]bool{}
	stack := []string{nodeID}
	for len(stack) > 0 {
		n := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if out[n] {
			continue
		}
		out[n] = true
		if c, ok := m.dagCommits[n]; ok {
			stack = append(stack, c.ParentIDs...)
		}
	}
	return out
}

// findMergeBase implements the general merge-base
// algorithm (see the file header). It returns
// (base, nil) for an unambiguous base,
// (nil, *TimelineAmbiguousMergeBaseError) when several
// maximal common ancestors exist, and
// (nil, ErrTimelineNoCommonBase-wrapped) when there is
// no common ancestor at all.
func (m *TimelineManager) findMergeBase(srcHeadID, dstHeadID string) (*TimelineCommit, error) {
	if srcHeadID == "" || dstHeadID == "" {
		return nil, fmt.Errorf("%w: missing DAG head", ErrTimelineNoCommonBase)
	}
	if srcHeadID == dstHeadID {
		if c, ok := m.dagCommits[srcHeadID]; ok {
			return c, nil
		}
		return nil, fmt.Errorf("%w: head commit %s unknown", ErrTimelineNoCommonBase, srcHeadID)
	}
	srcAnc := m.dagAncestors(srcHeadID)
	dstAnc := m.dagAncestors(dstHeadID)
	var common []string
	for id := range srcAnc {
		if dstAnc[id] {
			common = append(common, id)
		}
	}
	if len(common) == 0 {
		return nil, fmt.Errorf("%w: %s and %s share no commit ancestor", ErrTimelineNoCommonBase, srcHeadID, dstHeadID)
	}
	// Maximal common ancestors: a common ancestor that
	// is not an ancestor of any OTHER common ancestor.
	// Ancestor sets are computed once per common node;
	// criss-cross histories are small (branch graphs),
	// and correctness here matters more than speed.
	ancOf := map[string]map[string]bool{}
	for _, id := range common {
		ancOf[id] = m.dagAncestors(id)
	}
	var maximal []string
	for _, id := range common {
		dominated := false
		for _, other := range common {
			if other != id && ancOf[other][id] {
				dominated = true
				break
			}
		}
		if !dominated {
			maximal = append(maximal, id)
		}
	}
	sort.Strings(maximal)
	if len(maximal) == 1 {
		c, ok := m.dagCommits[maximal[0]]
		if !ok {
			return nil, fmt.Errorf("%w: base commit %s unknown", ErrTimelineNoCommonBase, maximal[0])
		}
		return c, nil
	}
	return nil, &TimelineAmbiguousMergeBaseError{SourceID: srcHeadID, TargetID: dstHeadID, Candidates: maximal}
}

// mergeGeneralBaseLocked resolves MergeGeneral's base view.
// Unique base -> that commit's view. Ambiguous base ->
// Patch 2 recursive fallback (recursiveBaseView): a virtual
// base used ONLY when exactly determined (see
// recursiveBaseView); every other ambiguous case returns
// ErrAmbiguousMergeBase (the original findMergeBase error,
// with its candidate list, not a synthesized one). No
// common ancestor -> the legacy special cases, as before,
// EXCEPT a branchView failure inside the legacy rule is
// a real error and propagates (see legacyBaseView): it
// is never silently converted to "rule does not apply".
// Caller holds the chain locks. Both MergeGeneral and
// MergeGeneralStreamed resolve their base here, so this
// rule governs both paths identically.
func (m *TimelineManager) mergeGeneralBaseLocked(src, dst *timelineState, srcHeadNode, dstHeadNode *TimelineCommit) (map[string][]byte, string, string, error) {
	baseCommit, baseErr := m.findMergeBase(srcHeadNode.ID, dstHeadNode.ID)
	if baseErr != nil {
		if errors.Is(baseErr, ErrTimelineAmbiguousMergeBase) {
			vbase, vfp, vid, verr := m.recursiveBaseView(srcHeadNode.ID, dstHeadNode.ID, 0)
			if verr != nil {
				if errors.Is(verr, ErrTimelineAmbiguousMergeBase) {
					return nil, "", "", baseErr
				}
				return nil, "", "", verr
			}
			return vbase, vfp, vid, nil
		}
		legacy, ok, lerr := m.legacyBaseView(src, dst)
		if lerr != nil {
			return nil, "", "", lerr
		}
		if !ok {
			return nil, "", "", baseErr
		}
		return legacy, timelineFingerprint(legacy), "", nil
	}
	bv, err := m.materializeBaseView(baseCommit)
	if err != nil {
		return nil, "", "", err
	}
	return bv, timelineFingerprint(bv), baseCommit.ID, nil
}

// mergeTwoViewsOverBase is the per-key 3-way rule for
// virtual-base construction: one-sided change wins, both
// sides identical wins, both sides changed differently
// is a conflict (conflict=true, no guess). It is the
// same rule as mergeViews3Way's classification, without
// a policy: a virtual base has no policy to appeal to,
// so a conflict is a refusal, not a resolution.
func mergeTwoViewsOverBase(base, a, b map[string][]byte) (map[string][]byte, bool) {
	keys := map[string]bool{}
	for k := range base {
		keys[k] = true
	}
	for k := range a {
		keys[k] = true
	}
	for k := range b {
		keys[k] = true
	}
	out := map[string][]byte{}
	conflict := false
	for k := range keys {
		baseVal, baseOK := base[k]
		aVal, aOK := a[k]
		bVal, bOK := b[k]
		aChanged := !(aOK == baseOK && (!aOK || string(aVal) == string(baseVal)))
		bChanged := !(bOK == baseOK && (!bOK || string(bVal) == string(baseVal)))
		switch {
		case !aChanged && !bChanged:
			if baseOK {
				out[k] = append([]byte(nil), baseVal...)
			}
		case aChanged && !bChanged:
			if aOK {
				out[k] = append([]byte(nil), aVal...)
			}
		case !aChanged && bChanged:
			if bOK {
				out[k] = append([]byte(nil), bVal...)
			}
		case aOK == bOK && (!aOK || string(aVal) == string(bVal)):
			if aOK {
				out[k] = append([]byte(nil), aVal...)
			}
		default:
			conflict = true
		}
	}
	return out, conflict
}

// patch2ViewsEqual reports whether two views hold the same
// keys with the same values (deletions are absences).
func patch2ViewsEqual(a, b map[string][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for k, av := range a {
		bv, ok := b[k]
		if !ok || string(av) != string(bv) {
			return false
		}
	}
	return true
}

// patch2FoldVirtualBase folds candidate views in the given
// order: start from the order's leading candidate's view, then merge
// each next candidate's view over the recursive base of
// (first, next) — the same fold definition for every order,
// so folds under different orders are comparable. It
// returns (view, false, nil) on a conflict-free fold,
// (nil, true, nil) when a pairwise merge conflicts, and
// (nil, false, err) for a materialization/recursion error,
// which the caller propagates rather than treating as a
// conflict. The pairwise bases are cached per unordered
// pair so a permutation check does not re-derive them.
func (m *TimelineManager) patch2FoldVirtualBase(order []string, views map[string]map[string][]byte, pairBase map[string]map[string][]byte, depth int) (map[string][]byte, bool, error) {
	virtual := map[string][]byte{}
	for k, v := range views[order[0]] {
		virtual[k] = append([]byte(nil), v...)
	}
	rep := order[0]
	for _, nextID := range order[1:] {
		pairKey := rep + "\x00" + nextID
		if rep > nextID {
			pairKey = nextID + "\x00" + rep
		}
		bview, ok := pairBase[pairKey]
		if !ok {
			var err error
			bview, _, _, err = m.recursiveBaseView(rep, nextID, depth+1)
			if err != nil {
				return nil, false, err
			}
			pairBase[pairKey] = bview
		}
		merged, conflict := mergeTwoViewsOverBase(bview, virtual, views[nextID])
		if conflict {
			return nil, true, nil
		}
		virtual = merged
	}
	return virtual, false, nil
}

// patch2Permutations returns every ordering of items in
// deterministic lexicographic generation order.
func patch2Permutations(items []string) [][]string {
	if len(items) == 0 {
		return [][]string{{}}
	}
	var out [][]string
	var gen func(prefix []string, rest []string)
	gen = func(prefix []string, rest []string) {
		if len(rest) == 0 {
			cp := append([]string(nil), prefix...)
			out = append(out, cp)
			return
		}
		for i, v := range rest {
			next := append([]string(nil), rest[:i]...)
			next = append(next, rest[i+1:]...)
			gen(append(append([]string(nil), prefix...), v), next)
		}
	}
	gen(nil, append([]string(nil), items...))
	return out
}

// recursiveBaseView returns the merge-base view for two
// heads, resolving criss-cross histories recursively
// (git-style virtual base), or an
// ErrTimelineAmbiguousMergeBase error when the base
// cannot be determined exactly, without guessing.
// Rules, exactly (Patch 2, Track 4 — this supersedes
// the Axis D first-candidate approximation, which is
// REMOVED: no code path below picks a candidate
// because it happens to come first):
//   - unique maximal common ancestor -> its view;
//   - exactly 2 maximal candidates -> fold their views
//     over their own recursive base. The fold is
//     symmetric in the two candidates (mergeTwoViews-
//     OverBase treats its two sides identically, and
//     the pairwise base is itself order-independent),
//     so the result cannot depend on candidate order;
//     a conflicting fold is ambiguous, never a guess;
//   - 3 or 4 maximal candidates -> fold in EVERY
//     candidate order and accept the virtual base ONLY
//     when every fold is conflict-free AND every fold
//     yields the identical view. Any conflict, any
//     disagreement across orders, or any ambiguous
//     pairwise base -> ErrAmbiguousMergeBase. This is
//     the exactness test: the accepted base is the
//     unique order-independent result, not a pick;
//   - 5+ maximal candidates -> ErrAmbiguousMergeBase
//     (temporary safe fallback, NOT a permanent
//     replacement for general merge-base support:
//     checking every order grows factorially, so the
//     exact algorithm for large candidate sets is
//     still open — see the remaining limits in the
//     Track 4 report);
//   - depth cap 16: deeper criss-cross nesting is
//     refused as ambiguous rather than recursed
//     forever (DAG ancestry is finite, but the cap
//     keeps a pathological graph from blowing the
//     stack; it is a stated limit, not a silent cut,
//     because the error is the same typed refusal).
//
// The returned ID for a virtual base is
// "virtual:<hash-of-sorted-candidate-IDs>": it names no
// real commit and must never be looked up as one (merge
// nodes store it only as a report trace; their state
// is still identified by their own commit ID). The ID
// hashes the SORTED candidate list, so it too is
// order-independent.
func (m *TimelineManager) recursiveBaseView(srcHeadID, dstHeadID string, depth int) (map[string][]byte, string, string, error) {
	if depth > 16 {
		return nil, "", "", fmt.Errorf("%w: recursive merge-base depth exceeded for %s and %s", ErrTimelineAmbiguousMergeBase, srcHeadID, dstHeadID)
	}
	base, err := m.findMergeBase(srcHeadID, dstHeadID)
	if err == nil {
		v, err := m.materializeBaseView(base)
		if err != nil {
			return nil, "", "", err
		}
		return v, timelineFingerprint(v), base.ID, nil
	}
	var amb *TimelineAmbiguousMergeBaseError
	if !errors.As(err, &amb) {
		return nil, "", "", err
	}
	if len(amb.Candidates) < 2 {
		return nil, "", "", err
	}
	if len(amb.Candidates) > 4 {
		// Temporary safe fallback for large candidate
		// sets (see the doc comment above): refuse
		// rather than pick a representative.
		return nil, "", "", amb
	}
	commitOf := func(id string) (*TimelineCommit, error) {
		c, ok := m.dagCommits[id]
		if !ok {
			return nil, fmt.Errorf("%w: base commit %s unknown", ErrTimelineNoCommonBase, id)
		}
		return c, nil
	}
	views := map[string]map[string][]byte{}
	sorted := append([]string(nil), amb.Candidates...)
	sort.Strings(sorted)
	for _, id := range sorted {
		c, err := commitOf(id)
		if err != nil {
			return nil, "", "", err
		}
		v, err := m.materializeBaseView(c)
		if err != nil {
			return nil, "", "", err
		}
		views[id] = v
	}
	pairBase := map[string]map[string][]byte{}
	if len(sorted) == 2 {
		virtual, conflict, err := m.patch2FoldVirtualBase(sorted, views, pairBase, depth)
		if err != nil {
			return nil, "", "", err
		}
		if conflict {
			return nil, "", "", amb
		}
		return virtual, timelineFingerprint(virtual), "virtual:" + dagHash(sorted...), nil
	}
	var accepted map[string][]byte
	for _, order := range patch2Permutations(sorted) {
		virtual, conflict, err := m.patch2FoldVirtualBase(order, views, pairBase, depth)
		if err != nil {
			// A pairwise base that is itself ambiguous
			// (or a materialization error) is not a
			// conflict to paper over: ambiguous stays
			// ambiguous, other errors propagate.
			if errors.Is(err, ErrTimelineAmbiguousMergeBase) {
				return nil, "", "", amb
			}
			return nil, "", "", err
		}
		if conflict {
			return nil, "", "", amb
		}
		if accepted == nil {
			accepted = virtual
		} else if !patch2ViewsEqual(accepted, virtual) {
			// The result depends on candidate order:
			// refuse rather than pick an order.
			return nil, "", "", amb
		}
	}
	return accepted, timelineFingerprint(accepted), "virtual:" + dagHash(sorted...), nil
}

// --- Streaming merge report + streaming delta (Axis D) ---

// MergeReportSink receives a streamed merge's per-key
// report in sorted key order. It is the bounded-memory
// replacement for the collected conflicted/identical
// lists: a merge with a million conflicting keys calls
// OnConflicted a million times but never holds the
// million keys in the report. The sink must not retain
// keys itself if bounded memory is the goal; a sink
// that appends everything reintroduces the bound it
// was given to remove, and that is the caller's choice,
// not this method's behaviour.
type MergeReportSink interface {
	OnConflicted(key string)
	OnIdentical(key string)
	OnApplied(key string)
}

// timelineDeltaSegmentWriter streams delta-segment
// entries to a temp file with the same format and
// checksum rule as timelineWriteDeltaSegment
// (JSON lines sorted by (key, sequence) + final
// {"checksum": sha256-of-entry-bytes} line, fsync,
// rename, directory sync in Sync mode), but without
// holding the entry list in memory. Write enforces
// strict (key, sequence) order; Close returns the
// segment SHA. On any error the temp file is removed
// and no final file appears.
type timelineDeltaSegmentWriter struct {
	f       *os.File
	tmp     string
	final   string
	h       hash.Hash
	prevKey string
	prevSeq uint64
	first   bool
	count   int
	syncDir bool
}

func newTimelineDeltaSegmentWriter(path string, syncDir bool) (*timelineDeltaSegmentWriter, error) {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	return &timelineDeltaSegmentWriter{f: f, tmp: tmp, final: path, h: sha256.New(), first: true, syncDir: syncDir}, nil
}

func (w *timelineDeltaSegmentWriter) Write(e timelineDeltaEntry) error {
	if !w.first && (e.Key < w.prevKey || (e.Key == w.prevKey && e.Seq <= w.prevSeq)) {
		return fmt.Errorf("yowza timeline: delta entries not strictly sorted by (key, sequence) at %q seq %d", e.Key, e.Seq)
	}
	w.first = false
	w.prevKey, w.prevSeq = e.Key, e.Seq
	raw, err := json.Marshal(e)
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	if _, err := w.f.Write(raw); err != nil {
		return err
	}
	w.h.Write(raw)
	w.count++
	return nil
}

func (w *timelineDeltaSegmentWriter) Abort() {
	if w.f != nil {
		w.f.Close()
		w.f = nil
	}
	_ = os.Remove(w.tmp)
}

func (w *timelineDeltaSegmentWriter) Close() (string, error) {
	sum := hex.EncodeToString(w.h.Sum(nil))
	chk, err := json.Marshal(map[string]string{"checksum": sum})
	if err != nil {
		w.Abort()
		return "", err
	}
	if _, err := w.f.Write(append(chk, '\n')); err != nil {
		w.Abort()
		return "", err
	}
	if err := w.f.Sync(); err != nil {
		w.Abort()
		return "", err
	}
	if err := w.f.Close(); err != nil {
		w.f = nil
		w.Abort()
		return "", err
	}
	w.f = nil
	if err := os.Rename(w.tmp, w.final); err != nil {
		_ = os.Remove(w.tmp)
		return "", err
	}
	if w.syncDir {
		if err := timelineSyncDir(dirOf(w.final)); err != nil {
			return "", fmt.Errorf("yowza timeline: delta segment %s renamed but directory sync failed: %w", w.final, err)
		}
	}
	return sum, nil
}

// MergeGeneralStreamed is MergeGeneral with a bounded
// report: conflicted/identical/applied keys are
// delivered to sink in sorted order as they are
// classified, and the applied delta segment is written
// streaming (one entry at a time, never a slice of all
// applied keys). The returned report carries counts
// (AppliedCount) and the delta file, with the key
// slices nil — the keys live in the sink and the
// segment, not in the report. Per-key semantics are
// identical to mergeViews3Way (same one-sided /
// identical / conflict rules, same source-wins
// resolution); fail policy still applies nothing,
// because the delta temp file is aborted (never
// renamed) when a conflict is found, before any
// commit record exists. Views themselves are still
// materialized (see the Axis D notes for the measured
// cost and the streaming-view design): this method
// bounds the REPORT and the APPLIED staging, which
// were the two in-memory lists this axis could close
// without refactoring the timeline.go view paths.
// Caller holds no locks; this method takes them.
func (m *TimelineManager) MergeGeneralStreamed(sourceID, targetID string, policy TimelineConflictPolicy, expectedSourceHead, expectedTargetHead uint64, sink MergeReportSink) (*MergeReport, error) {
	if policy != TimelineConflictFail && policy != TimelineConflictSourceWins && policy != TimelineConflictTargetWins {
		return nil, fmt.Errorf("%w: unknown conflict policy %d", ErrTimelineInvalid, policy)
	}
	if sourceID == targetID {
		return nil, fmt.Errorf("%w: cannot merge a timeline into itself", ErrTimelineInvalid)
	}
	if sink == nil {
		return nil, fmt.Errorf("%w: MergeGeneralStreamed requires a non-nil sink", ErrTimelineInvalid)
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
	dst, err := m.lookupLocked(targetID)
	if err != nil {
		return nil, err
	}
	chainSrc := m.chain(src)
	chainDst := m.chain(dst)
	all := append(append([]*timelineState{}, chainSrc...), chainDst...)
	unlock := lockTimelinesOrdered(all...)
	defer unlock()
	if src.poisoned != nil {
		return nil, src.poisoned
	}
	if dst.poisoned != nil {
		return nil, dst.poisoned
	}
	if src.headSeq != expectedSourceHead {
		return nil, fmt.Errorf("%w: merge source %s expected head %d, actual %d", ErrTimelineStaleHead, sourceID, expectedSourceHead, src.headSeq)
	}
	if dst.headSeq != expectedTargetHead {
		return nil, &TimelineStaleHeadError{TargetID: targetID, Expected: expectedTargetHead, Actual: dst.headSeq}
	}
	srcHeadNode := m.ensureHeadCommit(src)
	dstHeadNode := m.ensureHeadCommit(dst)
	base, baseFP, baseCommitID, err := m.mergeGeneralBaseLocked(src, dst, srcHeadNode, dstHeadNode)
	if err != nil {
		return nil, err
	}
	srcView, err := m.viewLocked(src, chainSrc, true)
	if err != nil {
		return nil, err
	}
	dstView, err := m.viewLocked(dst, chainDst, true)
	if err != nil {
		return nil, err
	}
	// Sorted union of keys: the single pass that
	// classifies, reports, and (for non-fail policies)
	// stages. Iteration order is sorted, so staged
	// applied entries are already delta-segment order.
	keys := map[string]bool{}
	for k := range base {
		keys[k] = true
	}
	for k := range srcView {
		keys[k] = true
	}
	for k := range dstView {
		keys[k] = true
	}
	sorted := make([]string, 0, len(keys))
	for k := range keys {
		sorted = append(sorted, k)
	}
	sort.Strings(sorted)
	newHead := dst.headSeq + 1
	mergeID := fmt.Sprintf("m-%s-%d", targetID, newHead)
	deltaFile := fmt.Sprintf("timeline-%s.merge-%s.delta", targetID, mergeID)
	writer, err := newTimelineDeltaSegmentWriter(filepathJoin(m.dir, deltaFile), m.sync)
	if err != nil {
		return nil, fmt.Errorf("timeline merge delta staging failed: %w", err)
	}
	var conflicted, identical, applied int
	// overlayApplied is held transiently for the in-memory
	// overlay update after the commit record. That is not
	// a second copy in asymptotic terms: the overlay
	// itself grows by exactly these entries on apply, as
	// in MergeGeneral. What streaming removes is the
	// separate applied slice for the delta segment and
	// the collected report lists.
	var overlayApplied []timelineMergeAppliedKey
	var failConflicted []string // fail policy only: the error must still name its keys
	fail := false
	for _, k := range sorted {
		baseVal, baseOK := base[k]
		srcVal, srcOK := srcView[k]
		dstVal, dstOK := dstView[k]
		srcChanged := !(srcOK == baseOK && (!srcOK || string(srcVal) == string(baseVal)))
		dstChanged := !(dstOK == baseOK && (!dstOK || string(dstVal) == string(baseVal)))
		switch {
		case !srcChanged && !dstChanged:
		case srcChanged && !dstChanged:
			if srcOK {
				overlayApplied = append(overlayApplied, timelineMergeAppliedKey{Key: k, Value: append([]byte(nil), srcVal...)})
				if err := writer.Write(timelineDeltaEntry{Key: k, Seq: newHead, Value: append([]byte(nil), srcVal...)}); err != nil {
					writer.Abort()
					return nil, err
				}
			} else {
				overlayApplied = append(overlayApplied, timelineMergeAppliedKey{Key: k, Deleted: true})
				if err := writer.Write(timelineDeltaEntry{Key: k, Seq: newHead, Deleted: true}); err != nil {
					writer.Abort()
					return nil, err
				}
			}
			applied++
			sink.OnApplied(k)
		case !srcChanged && dstChanged:
		case srcOK == dstOK && (!srcOK || string(srcVal) == string(dstVal)):
			identical++
			sink.OnIdentical(k)
		default:
			conflicted++
			sink.OnConflicted(k)
			if policy == TimelineConflictFail {
				failConflicted = append(failConflicted, k)
			}
			if policy == TimelineConflictSourceWins {
				if srcOK {
					overlayApplied = append(overlayApplied, timelineMergeAppliedKey{Key: k, Value: append([]byte(nil), srcVal...)})
					if err := writer.Write(timelineDeltaEntry{Key: k, Seq: newHead, Value: append([]byte(nil), srcVal...)}); err != nil {
						writer.Abort()
						return nil, err
					}
				} else {
					overlayApplied = append(overlayApplied, timelineMergeAppliedKey{Key: k, Deleted: true})
					if err := writer.Write(timelineDeltaEntry{Key: k, Seq: newHead, Deleted: true}); err != nil {
						writer.Abort()
						return nil, err
					}
				}
				applied++
				sink.OnApplied(k)
			} else if policy == TimelineConflictFail {
				fail = true
			}
		}
	}
	if fail {
		writer.Abort()
		return nil, &TimelineMergeConflictError{SourceID: sourceID, TargetID: targetID, Keys: failConflicted}
	}
	report := &MergeReport{
		MergedTimeline: targetID, SourceID: sourceID, TargetID: targetID,
		SourceHead: src.headSeq, TargetHeadBefore: dst.headSeq, NewHead: dst.headSeq,
		Policy: policy, BaseFingerprint: baseFP, BaseCommitID: baseCommitID,
	}
	if applied == 0 {
		writer.Abort()
		return report, nil
	}
	deltaSHA, err := writer.Close()
	if err != nil {
		return nil, fmt.Errorf("timeline merge delta staging failed: %w", err)
	}
	conflictedKeys := []string(nil)
	identicalKeys := []string(nil)
	// The commit record carries the report lists for
	// replay compatibility; for the streamed path those
	// lists are intentionally not rebuilt (that would
	// reintroduce the bound). Replay expands the delta
	// segment, not these lists, so nil is safe here:
	// merge-ref replay uses DeltaFile/DeltaSHA256, and
	// validateTimelineMergeRefPayload accepts empty
	// lists (fail policy only forbids non-empty
	// conflicted lists, and fail never reaches here
	// with conflicts).
	ref := &timelineMergeRefPayload{
		MergeID: mergeID, SourceID: sourceID, TargetID: targetID,
		SourceHead: src.headSeq, TargetBefore: dst.headSeq, NewHead: newHead,
		Policy: policy.String(), DeltaFile: deltaFile, DeltaSHA256: deltaSHA,
		DeltaKeys: applied, BaseFP: baseFP, BaseCommitID: baseCommitID,
		Conflicted: conflictedKeys, Identical: identicalKeys,
	}
	rec, err := timelineEncodeMergeRefRecord(newHead, ref)
	if err != nil {
		_ = os.Remove(filepathJoin(m.dir, deltaFile))
		return nil, err
	}
	if err := dst.appendRecordLocked(m.sync, newHead, rec); err != nil {
		return nil, err
	}
	for _, k := range overlayApplied {
		dst.overlay[k.Key] = append(dst.overlay[k.Key], timelineOverlayEntry{seq: newHead, deleted: k.Deleted, val: append([]byte(nil), k.Value...)})
	}
	dst.headSeq = newHead
	report.NewHead = newHead
	report.AppliedCount = applied
	report.DeltaFile = deltaFile
	dst.lastMerge = report
	mergeNode := m.newDAGCommit(dagStateID(dst.number, newHead), targetID,
		[]string{srcHeadNode.ID, dstHeadNode.ID}, baseCommitID, newHead,
		DeltaManifest{
			Segments:  []DeltaSegmentRef{{File: deltaFile, Keys: applied, Checksum: deltaSHA}},
			TotalKeys: applied, Digest: deltaSHA,
		})
	dst.dagHead = mergeNode.ID
	status, err := m.publishRegistryLocked("merge-dag")
	if err != nil {
		return nil, fmt.Errorf("timeline merge registry publish failed (status %s; merge-ref record %d is durable and will stand on reopen): %w", status, newHead, err)
	}
	return report, nil
}

// viewAtHeadLocked materializes ts's view with ts's own
// overlay limited to seqLimit (ancestors limited by
// their fork heads, as in viewLocked). Caller holds the
// chain's timeline locks (or is single-threaded at
// open); see viewLocked's locking contract. This is
// what makes a DAG commit's state materializable:
// (timeline, sequence) -> view, for any historical head.
func (m *TimelineManager) viewAtHeadLocked(ts *timelineState, seqLimit uint64) (map[string][]byte, error) {
	view, err := m.scanAtVector(ts.branchVector)
	if err != nil {
		return nil, err
	}
	chain := m.chain(ts)
	for i, anc := range chain {
		var limit uint64
		if anc == ts {
			limit = seqLimit
		} else {
			limit = chain[i+1].branchParentHead
		}
		applyOverlay(view, anc.overlay, limit)
	}
	return view, nil
}

// materializeBaseView returns the view named by a DAG
// commit: a root commit's snapshot vector, or a
// timeline commit's view at its sequence.
func (m *TimelineManager) materializeBaseView(c *TimelineCommit) (map[string][]byte, error) {
	if c.TimelineID == "" {
		root, ok := m.snapshotRoots[c.BaseSnapshotID]
		if !ok {
			return nil, fmt.Errorf("%w: snapshot root %s unknown for base commit %s", ErrTimelineNoCommonBase, c.BaseSnapshotID, c.ID)
		}
		return m.scanAtVector(root.Vector)
	}
	ts, ok := m.timelines[c.TimelineID]
	if !ok {
		return nil, fmt.Errorf("%w: base commit timeline %s unknown", ErrTimelineNoCommonBase, c.TimelineID)
	}
	return m.viewAtHeadLocked(ts, c.Sequence)
}

// legacyBaseView is the old direct-parent/sibling rule,
// kept as the special case of the general algorithm
// when the DAG has no common ancestor (e.g. two legacy
// timelines whose pre-DAG history was never graphed).
// It returns (view, true, nil) when the legacy rule
// applies, (nil, false, nil) when the rule does not
// apply to this pair, and (nil, false, err) when the
// rule DOES apply but branchView fails. That third
// case is the Patch 2 (Track 4) fix: a branchView
// failure (storage/corruption, e.g. a snapshot-vector
// mismatch from ScanStreaming) is a real error and
// MUST propagate to the MergeGeneral caller. The old
// code returned (v, err == nil) at all three sites,
// silently converting such a failure into "rule does
// not apply", after which mergeGeneralBaseLocked fell
// through to the NoCommonBase error — the same
// error-swallowing shape as the MergeToMain Get bug.
// Rule-inapplicability (parent IDs do not match) and
// branchView failure are now distinguished by the
// error return, never conflated.
func (m *TimelineManager) legacyBaseView(src, dst *timelineState) (map[string][]byte, bool, error) {
	if dst.id == src.parentID {
		v, err := m.branchView(src)
		if err != nil {
			return nil, false, err
		}
		return v, true, nil
	}
	if src.id == dst.parentID {
		v, err := m.branchView(dst)
		if err != nil {
			return nil, false, err
		}
		return v, true, nil
	}
	if src.parentID != "" && src.parentID == dst.parentID &&
		src.branchParentHead == dst.branchParentHead &&
		equalUint64Slice(src.branchVector, dst.branchVector) {
		v, err := m.branchView(src)
		if err != nil {
			return nil, false, err
		}
		return v, true, nil
	}
	return nil, false, nil
}

// mergeViews3Way is the three-way key logic shared in
// rules with Merge (one-sided -> take source; both
// identical -> identical; both different -> conflict).
// It is deliberately a pure function over views, so the
// DAG merge and the legacy merge cannot drift in
// per-key semantics without a test noticing.
func mergeViews3Way(base, srcView, dstView map[string][]byte, policy TimelineConflictPolicy) (applied []timelineMergeAppliedKey, appliedKeys, identicalKeys, conflictedKeys []string) {
	keys := map[string]bool{}
	for k := range base {
		keys[k] = true
	}
	for k := range srcView {
		keys[k] = true
	}
	for k := range dstView {
		keys[k] = true
	}
	sorted := make([]string, 0, len(keys))
	for k := range keys {
		sorted = append(sorted, k)
	}
	sort.Strings(sorted)
	for _, k := range sorted {
		baseVal, baseOK := base[k]
		srcVal, srcOK := srcView[k]
		dstVal, dstOK := dstView[k]
		srcChanged := !(srcOK == baseOK && (!srcOK || string(srcVal) == string(baseVal)))
		dstChanged := !(dstOK == baseOK && (!dstOK || string(dstVal) == string(baseVal)))
		switch {
		case !srcChanged && !dstChanged:
		case srcChanged && !dstChanged:
			if srcOK {
				applied = append(applied, timelineMergeAppliedKey{Key: k, Value: append([]byte(nil), srcVal...)})
			} else {
				applied = append(applied, timelineMergeAppliedKey{Key: k, Deleted: true})
			}
			appliedKeys = append(appliedKeys, k)
		case !srcChanged && dstChanged:
		case srcOK == dstOK && (!srcOK || string(srcVal) == string(dstVal)):
			identicalKeys = append(identicalKeys, k)
		default:
			conflictedKeys = append(conflictedKeys, k)
		}
	}
	if len(conflictedKeys) > 0 && policy == TimelineConflictSourceWins {
		for _, k := range conflictedKeys {
			srcVal, srcOK := srcView[k]
			if srcOK {
				applied = append(applied, timelineMergeAppliedKey{Key: k, Value: append([]byte(nil), srcVal...)})
			} else {
				applied = append(applied, timelineMergeAppliedKey{Key: k, Deleted: true})
			}
			appliedKeys = append(appliedKeys, k)
		}
	}
	sort.Slice(applied, func(i, j int) bool { return applied[i].Key < applied[j].Key })
	sort.Strings(appliedKeys)
	return applied, appliedKeys, identicalKeys, conflictedKeys
}

// --- Delta segments ---

// timelineDeltaEntry is one sorted change in a delta
// segment or checkpoint.
type timelineDeltaEntry struct {
	Key     string `json:"k"`
	Seq     uint64 `json:"s"`
	Deleted bool   `json:"d,omitempty"`
	Value   []byte `json:"v,omitempty"`
}

// timelineWriteDeltaSegment streams entries (already
// sorted by (key, sequence)) to path via a temp file,
// fsyncs, renames to path, and syncs the directory in
// Sync mode. It returns the SHA-256 of the entry bytes
// (everything before the final checksum line), which
// is the segment's identity in manifests and merge-ref
// records. A failure removes the temp file and leaves
// no final file behind.
func timelineWriteDeltaSegment(path string, entries []timelineDeltaEntry, syncDir bool) (string, error) {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return "", err
	}
	h := sha256.New()
	fail := func(err error) (string, error) {
		f.Close()
		_ = os.Remove(tmp)
		return "", err
	}
	prevKey := ""
	var prevSeq uint64
	first := true
	for _, e := range entries {
		if !first && (e.Key < prevKey || (e.Key == prevKey && e.Seq <= prevSeq)) {
			return fail(fmt.Errorf("yowza timeline: delta entries not strictly sorted by (key, sequence) at %q seq %d", e.Key, e.Seq))
		}
		first = false
		prevKey, prevSeq = e.Key, e.Seq
		raw, err := json.Marshal(e)
		if err != nil {
			return fail(err)
		}
		raw = append(raw, '\n')
		if _, err := f.Write(raw); err != nil {
			return fail(err)
		}
		h.Write(raw)
	}
	sum := hex.EncodeToString(h.Sum(nil))
	chk, err := json.Marshal(map[string]string{"checksum": sum})
	if err != nil {
		return fail(err)
	}
	if _, err := f.Write(append(chk, '\n')); err != nil {
		return fail(err)
	}
	if err := f.Sync(); err != nil {
		return fail(err)
	}
	if err := f.Close(); err != nil {
		return fail(err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return "", err
	}
	if syncDir {
		if err := timelineSyncDir(dirOf(path)); err != nil {
			return "", fmt.Errorf("yowza timeline: delta segment %s renamed but directory sync failed: %w", path, err)
		}
	}
	return sum, nil
}

func dirOf(path string) string {
	if i := strings.LastIndex(path, "/"); i >= 0 {
		return path[:i]
	}
	return "."
}

// timelineLoadDeltaSegment reads, checksum-verifies, and
// order-verifies a delta segment. When expectedSHA is
// non-empty it must equal the file's checksum (a
// committed merge-ref's digest); when expectedCount >= 0
// it must equal the entry count. Any violation is
// corruption, never a partial result.
func timelineLoadDeltaSegment(path, expectedSHA string, expectedCount int) ([]timelineDeltaEntry, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	// Split off the final checksum line.
	lastNL := strings.LastIndex(string(data), "\n")
	if lastNL < 0 {
		return nil, fmt.Errorf("yowza timeline: delta segment %s has no checksum line", path)
	}
	body := data[:lastNL]
	lineEnd := strings.LastIndex(string(body), "\n")
	var chkLine []byte
	var entryBytes []byte
	if lineEnd < 0 {
		chkLine = body
		entryBytes = nil
	} else {
		chkLine = body[lineEnd+1:]
		entryBytes = body[:lineEnd+1]
	}
	var chk struct {
		Checksum string `json:"checksum"`
	}
	if err := json.Unmarshal(chkLine, &chk); err != nil || chk.Checksum == "" {
		return nil, fmt.Errorf("yowza timeline: delta segment %s has an invalid checksum line", path)
	}
	sum := sha256.Sum256(entryBytes)
	if hex.EncodeToString(sum[:]) != chk.Checksum {
		return nil, fmt.Errorf("yowza timeline: delta segment %s checksum mismatch", path)
	}
	if expectedSHA != "" && expectedSHA != chk.Checksum {
		return nil, fmt.Errorf("yowza timeline: delta segment %s checksum %s != expected %s", path, chk.Checksum, expectedSHA)
	}
	var entries []timelineDeltaEntry
	if len(entryBytes) > 0 {
		for _, line := range strings.Split(strings.TrimSuffix(string(entryBytes), "\n"), "\n") {
			if line == "" {
				continue
			}
			var e timelineDeltaEntry
			if err := json.Unmarshal([]byte(line), &e); err != nil {
				return nil, fmt.Errorf("yowza timeline: delta segment %s unparseable entry: %v", path, err)
			}
			entries = append(entries, e)
		}
	}
	prevKey := ""
	var prevSeq uint64
	for i, e := range entries {
		if i > 0 && (e.Key < prevKey || (e.Key == prevKey && e.Seq <= prevSeq)) {
			return nil, fmt.Errorf("yowza timeline: delta segment %s entries not strictly sorted at index %d", path, i)
		}
		prevKey, prevSeq = e.Key, e.Seq
	}
	if expectedCount >= 0 && len(entries) != expectedCount {
		return nil, fmt.Errorf("yowza timeline: delta segment %s has %d entries, want %d", path, len(entries), expectedCount)
	}
	return entries, nil
}

// cleanupOrphanDeltas removes delta temp files and delta
// files no committed merge-ref record references and no
// live timeline checkpoints (best effort, open-time).
func (m *TimelineManager) cleanupOrphanDeltas(referenced map[string]bool) {
	keep := map[string]bool{}
	for f := range referenced {
		keep[f] = true
	}
	for _, ts := range m.timelines {
		if ts.checkpointFile != "" {
			keep[ts.checkpointFile] = true
		}
	}
	ents, err := os.ReadDir(m.dir)
	if err != nil {
		return
	}
	for _, e := range ents {
		name := e.Name()
		if strings.HasSuffix(name, ".delta.tmp") {
			_ = os.Remove(filepathJoin(m.dir, name))
			continue
		}
		if strings.HasSuffix(name, ".delta") && !keep[name] {
			_ = os.Remove(filepathJoin(m.dir, name))
		}
	}
}

// --- DAG forks ---

// publishDAGForkOrRollback publishes the registry after a
// DAG fork's nodes were added. On NotPublished it rolls
// the whole fork back (timeline, WAL, pin, DAG nodes);
// on Indeterminate it keeps everything (the manager
// freezes, and recovery decides). It mirrors
// createTimelineLocked's publish contract.
func (m *TimelineManager) publishDAGForkOrRollback(ts *timelineState, pinID string, nodeIDs []string) error {
	status, err := m.publishRegistryLocked("fork-dag")
	if err != nil {
		if status == TimelinePublishIndeterminate {
			return err
		}
		delete(m.timelines, ts.id)
		m.nextNumber--
		if ts.wal != nil {
			ts.wal.Close()
			ts.wal = nil
			ts.walIO = nil
		}
		_ = os.Remove(ts.walPath)
		for _, id := range nodeIDs {
			delete(m.dagCommits, id)
		}
		m.releaseBranchLeases(ts)
		_ = m.db.DeletePin(pinID)
		return err
	}
	return nil
}

// ForkFromDBDAG branches a new timeline from the
// database's current state WITHOUT scanning the
// database: cost depends on shard count + root
// metadata, not total keys (see the file header).
// The branch point is a snapshot root + durable pin;
// no fingerprint is recorded (branchFingerprint empty),
// and open-time identity checks use the pin + registry
// commit instead of a full view rebuild.
func (m *TimelineManager) ForkFromDBDAG(id string) (*TimelineInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkMutableLocked(); err != nil {
		return nil, err
	}
	if err := m.checkNewIDLocked(id); err != nil {
		return nil, err
	}
	if err := m.db.Flush(); err != nil {
		return nil, fmt.Errorf("timeline fork flush: %w", err)
	}
	vec, _, err := m.db.SnapshotVector()
	if err != nil {
		return nil, err
	}
	root := snapshotRootFor(vec, m.db.Generation(), m.db.CheckpointSeq())
	if _, ok := m.snapshotRoots[root.ID]; !ok {
		m.snapshotRoots[root.ID] = root
	}
	pin, err := m.createBranchPin(vec, id)
	if err != nil {
		return nil, fmt.Errorf("timeline %s durable pin: %w", id, err)
	}
	leaseIDs, err := m.registerBranchLeases(vec)
	if err != nil {
		_ = m.db.DeletePin(pin.ID)
		return nil, fmt.Errorf("timeline %s branch lease: %w", id, err)
	}
	ts, status, err := m.createTimelineLocked(id, "", vec, 0, "", leaseIDs, pin.ID)
	if err != nil {
		if status != TimelinePublishIndeterminate {
			m.releaseBranchLeases(&timelineState{leaseIDs: leaseIDs})
			_ = m.db.DeletePin(pin.ID)
		}
		return nil, err
	}
	ts.snapshotRootID = root.ID
	rootNodeID := "c-root-" + root.ID
	var nodeIDs []string
	if _, ok := m.dagCommits[rootNodeID]; !ok {
		m.newDAGCommit(rootNodeID, "", nil, root.ID, 0, DeltaManifest{Digest: root.Digest})
		nodeIDs = append(nodeIDs, rootNodeID)
	}
	forkNode := m.newDAGCommit(dagStateID(ts.number, 0), id, []string{rootNodeID}, root.ID, 0, DeltaManifest{})
	nodeIDs = append(nodeIDs, forkNode.ID)
	ts.dagHead = forkNode.ID
	if err := m.publishDAGForkOrRollback(ts, pin.ID, nodeIDs); err != nil {
		return nil, err
	}
	return m.infoOf(ts), nil
}

// ForkDAG branches a new timeline from a live timeline's
// current head WITHOUT scanning either timeline's view:
// the child records a reference to the parent's head
// commit + the shared base snapshot, and reads resolve
// through the parent's WAL prefix + pinned root at
// read time, exactly as legacy forks do — only the
// fork-time materialization is gone.
func (m *TimelineManager) ForkDAG(parentID, id string) (*TimelineInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkMutableLocked(); err != nil {
		return nil, err
	}
	parent, err := m.lookupLocked(parentID)
	if err != nil {
		return nil, err
	}
	if err := m.checkNewIDLocked(id); err != nil {
		return nil, err
	}
	unlock := lockTimelinesOrdered(m.chain(parent)...)
	if parent.poisoned != nil {
		poison := parent.poisoned
		unlock()
		return nil, poison
	}
	parentHead := parent.headSeq
	branchVector := append([]uint64(nil), parent.branchVector...)
	parentRootID := parent.snapshotRootID
	unlock()
	// The parent's head commit must exist before the
	// child references it; ensureHeadCommit creates the
	// canonical c-state node for the parent's head.
	parentHeadNode := m.ensureHeadCommit(parent)
	pin, err := m.createBranchPin(branchVector, id)
	if err != nil {
		return nil, fmt.Errorf("timeline %s durable pin: %w", id, err)
	}
	leaseIDs, err := m.registerBranchLeases(branchVector)
	if err != nil {
		_ = m.db.DeletePin(pin.ID)
		return nil, fmt.Errorf("timeline %s branch lease: %w", id, err)
	}
	ts, status, err := m.createTimelineLocked(id, parentID, branchVector, parentHead, "", leaseIDs, pin.ID)
	if err != nil {
		if status != TimelinePublishIndeterminate {
			m.releaseBranchLeases(&timelineState{leaseIDs: leaseIDs})
			_ = m.db.DeletePin(pin.ID)
		}
		return nil, err
	}
	ts.snapshotRootID = parentRootID
	forkNode := m.newDAGCommit(dagStateID(ts.number, 0), id, []string{parentHeadNode.ID}, parentRootID, 0, DeltaManifest{})
	ts.dagHead = forkNode.ID
	if err := m.publishDAGForkOrRollback(ts, pin.ID, []string{forkNode.ID}); err != nil {
		return nil, err
	}
	return m.infoOf(ts), nil
}

// --- General merge ---

// MergeGeneral merges source into target using the
// commit DAG: a general merge base (grandparents
// included), dual head control, and the delta-segment
// commit protocol. Legacy direct-parent/sibling merges
// are special cases (their base is found by the DAG
// when both timelines have DAG nodes, and by the
// legacy fallback otherwise). Under fail policy a
// conflict returns *TimelineMergeConflictError and
// applies nothing, exactly as Merge does.
func (m *TimelineManager) MergeGeneral(sourceID, targetID string, policy TimelineConflictPolicy, expectedSourceHead, expectedTargetHead uint64) (*MergeReport, error) {
	if policy != TimelineConflictFail && policy != TimelineConflictSourceWins && policy != TimelineConflictTargetWins {
		return nil, fmt.Errorf("%w: unknown conflict policy %d", ErrTimelineInvalid, policy)
	}
	if sourceID == targetID {
		return nil, fmt.Errorf("%w: cannot merge a timeline into itself", ErrTimelineInvalid)
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
	dst, err := m.lookupLocked(targetID)
	if err != nil {
		return nil, err
	}
	chainSrc := m.chain(src)
	chainDst := m.chain(dst)
	all := append(append([]*timelineState{}, chainSrc...), chainDst...)
	unlock := lockTimelinesOrdered(all...)
	defer unlock()
	if src.poisoned != nil {
		return nil, src.poisoned
	}
	if dst.poisoned != nil {
		return nil, dst.poisoned
	}
	// Dual head control: BOTH heads are checked, so a
	// merge computed against a moved source cannot
	// silently rebase onto the new source head.
	if src.headSeq != expectedSourceHead {
		return nil, fmt.Errorf("%w: merge source %s expected head %d, actual %d", ErrTimelineStaleHead, sourceID, expectedSourceHead, src.headSeq)
	}
	if dst.headSeq != expectedTargetHead {
		return nil, &TimelineStaleHeadError{TargetID: targetID, Expected: expectedTargetHead, Actual: dst.headSeq}
	}
	srcHeadNode := m.ensureHeadCommit(src)
	dstHeadNode := m.ensureHeadCommit(dst)
	base, baseFP, baseCommitID, err := m.mergeGeneralBaseLocked(src, dst, srcHeadNode, dstHeadNode)
	if err != nil {
		return nil, err
	}
	srcView, err := m.viewLocked(src, chainSrc, true)
	if err != nil {
		return nil, err
	}
	dstView, err := m.viewLocked(dst, chainDst, true)
	if err != nil {
		return nil, err
	}
	applied, appliedKeys, identicalKeys, conflictedKeys := mergeViews3Way(base, srcView, dstView, policy)
	if len(conflictedKeys) > 0 && policy == TimelineConflictFail {
		return nil, &TimelineMergeConflictError{SourceID: sourceID, TargetID: targetID, Keys: append([]string(nil), conflictedKeys...)}
	}
	report := &MergeReport{
		MergedTimeline:   targetID,
		SourceID:         sourceID,
		TargetID:         targetID,
		SourceHead:       src.headSeq,
		TargetHeadBefore: dst.headSeq,
		NewHead:          dst.headSeq,
		AppliedKeys:      appliedKeys,
		IdenticalKeys:    identicalKeys,
		ConflictedKeys:   conflictedKeys,
		Policy:           policy,
		BaseFingerprint:  baseFP,
		BaseCommitID:     baseCommitID,
	}
	if len(applied) == 0 {
		return report, nil
	}
	// (1) Stage the merge as a sorted immutable delta
	// segment in temp storage, then rename to its final
	// name (the segment commit point).
	newHead := dst.headSeq + 1
	mergeID := fmt.Sprintf("m-%s-%d", targetID, newHead)
	deltaFile := fmt.Sprintf("timeline-%s.merge-%s.delta", targetID, mergeID)
	entries := make([]timelineDeltaEntry, 0, len(applied))
	for _, k := range applied {
		entries = append(entries, timelineDeltaEntry{Key: k.Key, Seq: newHead, Deleted: k.Deleted, Value: append([]byte(nil), k.Value...)})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Key < entries[j].Key })
	deltaSHA, err := timelineWriteDeltaSegment(filepathJoin(m.dir, deltaFile), entries, m.sync)
	if err != nil {
		return nil, fmt.Errorf("timeline merge delta staging failed: %w", err)
	}
	// (2) The small commit record: a type-4 merge-ref
	// WAL record naming the delta manifest. This record
	// is the commit point: without it the delta is an
	// orphan; with it the merge must fully replay.
	ref := &timelineMergeRefPayload{
		MergeID: mergeID, SourceID: sourceID, TargetID: targetID,
		SourceHead: src.headSeq, TargetBefore: dst.headSeq, NewHead: newHead,
		Policy: policy.String(), DeltaFile: deltaFile, DeltaSHA256: deltaSHA,
		DeltaKeys: len(applied), BaseFP: baseFP, BaseCommitID: baseCommitID,
		Conflicted: conflictedKeys, Identical: identicalKeys,
	}
	rec, err := timelineEncodeMergeRefRecord(newHead, ref)
	if err != nil {
		_ = os.Remove(filepathJoin(m.dir, deltaFile))
		return nil, err
	}
	if err := dst.appendRecordLocked(m.sync, newHead, rec); err != nil {
		// DELIBERATELY leave the delta file in place:
		// an indeterminate append may replay on reopen,
		// and then its delta must be there; if it does
		// not replay, the delta is an orphan cleaned at
		// open. Deleting it here could create the
		// half-applied merge this protocol forbids.
		return nil, err
	}
	for _, k := range applied {
		dst.overlay[k.Key] = append(dst.overlay[k.Key], timelineOverlayEntry{seq: newHead, deleted: k.Deleted, val: append([]byte(nil), k.Value...)})
	}
	dst.headSeq = newHead
	report.NewHead = newHead
	report.AppliedCount = len(applied)
	report.DeltaFile = deltaFile
	report.AppliedKeys = nil // the applied set lives in the delta segment, not in this report
	dst.lastMerge = report
	mergeNode := m.newDAGCommit(dagStateID(dst.number, newHead), targetID,
		[]string{srcHeadNode.ID, dstHeadNode.ID}, baseCommitID, newHead,
		DeltaManifest{
			Segments:  []DeltaSegmentRef{{File: deltaFile, Keys: len(applied), Checksum: deltaSHA}},
			TotalKeys: len(applied), Digest: deltaSHA,
		})
	dst.dagHead = mergeNode.ID
	status, err := m.publishRegistryLocked("merge-dag")
	if err != nil {
		// Same contract as Merge: the WAL record is
		// durable and the merge stands on reopen (replay
		// expands the delta and heals the DAG node);
		// report the publish error, do not pretend the
		// merge did not happen.
		return nil, fmt.Errorf("timeline merge registry publish failed (status %s; merge-ref record %d is durable and will stand on reopen): %w", status, newHead, err)
	}
	return report, nil
}

// --- Checkpoints + retention ---

// checkpointTimelineLocked is CheckpointTimeline's body with
// locks already held (m.mu write + ts.mu). Refactored
// unchanged in behaviour so CheckpointAndTruncate can take
// a checkpoint and truncate under the same locks, with no
// write interleaving between the checkpoint and the cut.
func (m *TimelineManager) checkpointTimelineLocked(ts *timelineState) (string, uint64, int, error) {
	if ts.poisoned != nil {
		return "", 0, 0, ts.poisoned
	}
	var entries []timelineDeltaEntry
	for k, list := range ts.overlay {
		for _, e := range list {
			entries = append(entries, timelineDeltaEntry{Key: k, Seq: e.seq, Deleted: e.deleted, Value: append([]byte(nil), e.val...)})
		}
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Key != entries[j].Key {
			return entries[i].Key < entries[j].Key
		}
		return entries[i].Seq < entries[j].Seq
	})
	gen := ts.checkpointGen + 1
	file := fmt.Sprintf("timeline-%s.ckpt.%d.delta", ts.id, gen)
	if _, err := timelineWriteDeltaSegment(filepathJoin(m.dir, file), entries, m.sync); err != nil {
		return "", 0, 0, err
	}
	prevFile, prevGen := ts.checkpointFile, ts.checkpointGen
	ts.checkpointFile, ts.checkpointGen = file, gen
	status, err := m.publishRegistryLocked("checkpoint")
	if err != nil {
		if status != TimelinePublishIndeterminate {
			ts.checkpointFile, ts.checkpointGen = prevFile, prevGen
			_ = os.Remove(filepathJoin(m.dir, file))
		}
		return "", 0, 0, err
	}
	return file, gen, len(entries), nil
}

// CheckpointTimeline writes the timeline's whole overlay
// as a sorted, checksummed delta segment and publishes
// its manifest in the registry. The WAL is NOT
// truncated by this call (see CheckpointAndTruncate for
// the truncating form). It returns the checkpoint file
// name, generation, and key count.
func (m *TimelineManager) CheckpointTimeline(id string) (string, uint64, int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkMutableLocked(); err != nil {
		return "", 0, 0, err
	}
	ts, err := m.lookupLocked(id)
	if err != nil {
		return "", 0, 0, err
	}
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return m.checkpointTimelineLocked(ts)
}

// checkpointEntriesEqual reports whether a checkpoint
// segment's entries are exactly a timeline's live
// overlay (same keys, same per-key (seq, deleted,
// value) lists). This is the "proven unneeded" gate for
// WAL truncation: the prefix is dropped only when the
// checkpoint reproduces the overlay bit-for-bit, not
// merely because a checkpoint file exists.
func checkpointEntriesEqual(entries []timelineDeltaEntry, overlay map[string][]timelineOverlayEntry) bool {
	if len(entries) != 0 {
		// Count overlay entries; a mismatch in count is
		// already inequality, checked below per key too.
	}
	total := 0
	for _, list := range overlay {
		total += len(list)
	}
	if len(entries) != total {
		return false
	}
	byKey := map[string][]timelineDeltaEntry{}
	for _, e := range entries {
		byKey[e.Key] = append(byKey[e.Key], e)
	}
	if len(byKey) != len(overlay) {
		return false
	}
	for k, list := range overlay {
		got := byKey[k]
		if len(got) != len(list) {
			return false
		}
		for i, e := range list {
			g := got[i]
			if g.Seq != e.seq || g.Deleted != e.deleted || string(g.Value) != string(e.val) {
				return false
			}
		}
	}
	return true
}

// replaceWALWithBaseLocked rewrites ts's WAL as
// [checkpoint-base record][suffix records with seq >
// cutoff], atomically by tmp + sync + rename + dir sync
// (in Sync mode). Caller holds m.mu write + ts.mu, and
// ts.wal is closed and reopened around the rename: the
// old append handle points at the old inode and must
// not survive the swap. The checkpoint file and cutoff
// must already be verified by the caller. Crash safety:
// the rename is the commit point, and BOTH sides replay
// to the same overlay (old full WAL, or base +
// suffix), so a crash at any point loses nothing.
// On reopen failure the in-memory WAL handle is left
// nil and an error is returned: the on-disk state is
// still valid, and the next manager open recovers it.
func (m *TimelineManager) replaceWALWithBaseLocked(ts *timelineState, checkpointFile string, cutoff uint64) error {
	suffix, err := timelineWALSuffixAfter(ts.walPath, cutoff)
	if err != nil {
		return err
	}
	base, err := timelineEncodeCheckpointBaseRecord(cutoff, checkpointFile)
	if err != nil {
		return err
	}
	tmp := ts.walPath + ".trunc.tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(base); err != nil {
		f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if _, err := f.Write(suffix); err != nil {
		f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if ts.wal != nil {
		ts.wal.Close()
		ts.wal = nil
		ts.walIO = nil
	}
	if err := os.Rename(tmp, ts.walPath); err != nil {
		return err
	}
	if m.sync {
		if err := timelineSyncDir(m.dir); err != nil {
			return err
		}
	}
	nf, err := os.OpenFile(ts.walPath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	ts.wal = nf
	ts.walIO = timelineOSWAL{f: nf}
	if st, err := nf.Stat(); err == nil {
		ts.walSize = st.Size()
	} else {
		ts.walSize = int64(len(base) + len(suffix))
	}
	return nil
}

// CheckpointAndTruncateTimelineWAL checkpoints a timeline
// and then truncates its WAL to a checkpoint-base record
// (plus any suffix written after the checkpoint — none,
// because both steps run under the same locks). It
// returns the checkpoint file, generation, entry count,
// and the WAL size before and after the cut. The
// pre-truncation overlay is recoverable from the
// checkpoint by construction: the checkpoint segment is
// checksummed, and replay expands it (see
// timeline_store.go). A timeline with no overlay
// entries still truncates (base record only).
func (m *TimelineManager) CheckpointAndTruncateTimelineWAL(id string) (file string, gen uint64, entries int, walBefore, walAfter int64, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkMutableLocked(); err != nil {
		return "", 0, 0, 0, 0, err
	}
	ts, err := m.lookupLocked(id)
	if err != nil {
		return "", 0, 0, 0, 0, err
	}
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if st, err := os.Stat(ts.walPath); err == nil {
		walBefore = st.Size()
	}
	file, gen, entries, err = m.checkpointTimelineLocked(ts)
	if err != nil {
		return "", 0, 0, 0, 0, err
	}
	// The checkpoint was just taken from the live overlay
	// under lock, so cutoff = current head is exact, and
	// no suffix exists yet. Verify the segment loads
	// before cutting: a checkpoint that cannot be read
	// back must never become the only copy of the prefix.
	segEntries, err := timelineLoadDeltaSegment(filepathJoin(m.dir, file), "", -1)
	if err != nil {
		return "", 0, 0, 0, 0, fmt.Errorf("yowza timeline: checkpoint %s fails reload before truncation: %w", file, err)
	}
	if !checkpointEntriesEqual(segEntries, ts.overlay) {
		return "", 0, 0, 0, 0, fmt.Errorf("yowza timeline: checkpoint %s does not reproduce the live overlay; refusing truncation", file)
	}
	if err := m.replaceWALWithBaseLocked(ts, file, ts.headSeq); err != nil {
		return "", 0, 0, 0, 0, err
	}
	if st, err := os.Stat(ts.walPath); err == nil {
		walAfter = st.Size()
	}
	return file, gen, entries, walBefore, walAfter, nil
}

// TruncateTimelineWALToCheckpoint truncates a timeline's
// WAL against its EXISTING latest checkpoint: the
// checkpoint segment must reproduce the live overlay
// for every entry at or below the checkpoint's max
// sequence, and WAL records after that sequence are
// kept as the suffix. Writes since the checkpoint are
// therefore preserved, and the truncated prefix is
// exactly the checkpoint's entries. If the checkpoint
// does not match the live overlay prefix, truncation
// is refused loudly and nothing is cut.
func (m *TimelineManager) TruncateTimelineWALToCheckpoint(id string) (walBefore, walAfter int64, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkMutableLocked(); err != nil {
		return 0, 0, err
	}
	ts, err := m.lookupLocked(id)
	if err != nil {
		return 0, 0, err
	}
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if ts.poisoned != nil {
		return 0, 0, ts.poisoned
	}
	if ts.checkpointFile == "" {
		return 0, 0, fmt.Errorf("%w: timeline %s has no checkpoint to truncate against", ErrTimelineInvalid, id)
	}
	if st, err := os.Stat(ts.walPath); err == nil {
		walBefore = st.Size()
	}
	segEntries, err := timelineLoadDeltaSegment(filepathJoin(m.dir, ts.checkpointFile), "", -1)
	if err != nil {
		return 0, 0, err
	}
	var cutoff uint64
	prefix := map[string][]timelineOverlayEntry{}
	for _, e := range segEntries {
		if e.Seq > cutoff {
			cutoff = e.Seq
		}
		prefix[e.Key] = append(prefix[e.Key], timelineOverlayEntry{seq: e.Seq, deleted: e.Deleted, val: append([]byte(nil), e.Value...)})
	}
	// Every live overlay entry at or below cutoff must
	// equal the checkpoint's list for that key, and the
	// checkpoint must list every key that has such an
	// entry. Entries above cutoff are the suffix and
	// are untouched by this comparison.
	for k, list := range ts.overlay {
		var want []timelineOverlayEntry
		for _, e := range list {
			if e.seq <= cutoff {
				want = append(want, e)
			}
		}
		got := prefix[k]
		if len(got) != len(want) {
			return 0, 0, fmt.Errorf("yowza timeline: checkpoint prefix mismatch for key %q (checkpoint %d, live %d); refusing truncation", k, len(got), len(want))
		}
		for i := range want {
			if got[i].seq != want[i].seq || got[i].deleted != want[i].deleted || string(got[i].val) != string(want[i].val) {
				return 0, 0, fmt.Errorf("yowza timeline: checkpoint prefix mismatch for key %q entry %d; refusing truncation", k, i)
			}
		}
		delete(prefix, k)
	}
	for k := range prefix {
		return 0, 0, fmt.Errorf("yowza timeline: checkpoint lists key %q with no live prefix entry; refusing truncation", k)
	}
	if err := m.replaceWALWithBaseLocked(ts, ts.checkpointFile, cutoff); err != nil {
		return 0, 0, err
	}
	if st, err := os.Stat(ts.walPath); err == nil {
		walAfter = st.Size()
	}
	return walBefore, walAfter, nil
}

// LoadCheckpointTimeline loads and validates a
// timeline's checkpoint segment and rebuilds the view
// it names (branch view + checkpoint entries), proving
// the checkpoint alone reproduces the overlay state it
// was taken from. It does not mutate the timeline.
func (m *TimelineManager) LoadCheckpointTimeline(id string) (map[string][]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ts, err := m.lookupLocked(id)
	if err != nil {
		return nil, err
	}
	if ts.checkpointFile == "" {
		return nil, fmt.Errorf("%w: timeline %s has no checkpoint", ErrTimelineInvalid, id)
	}
	entries, err := timelineLoadDeltaSegment(filepathJoin(m.dir, ts.checkpointFile), "", -1)
	if err != nil {
		return nil, err
	}
	unlock := lockTimelinesOrdered(m.chain(ts)...)
	defer unlock()
	view, err := m.branchView(ts)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if e.Deleted {
			delete(view, e.Key)
		} else {
			view[e.Key] = append([]byte(nil), e.Value...)
		}
	}
	return view, nil
}

// OldestPinnedSequence reports the retention floor: the
// minimum checkpoint sequence over all active durable
// pins, plus the active pin count. The precise floor is
// per shard (the pins' vectors); callers that reclaim
// must use the vectors, not just this scalar.
func (m *TimelineManager) OldestPinnedSequence() (uint64, int, error) {
	pins, err := m.db.ListPins()
	if err != nil {
		return 0, 0, err
	}
	var floor uint64
	count := 0
	first := true
	for _, pin := range pins {
		if pin.Status != PinStatusActive {
			continue
		}
		count++
		if first || pin.CheckpointSeq < floor {
			floor = pin.CheckpointSeq
			first = false
		}
	}
	if count == 0 {
		return 0, 0, nil
	}
	return floor, count, nil
}
