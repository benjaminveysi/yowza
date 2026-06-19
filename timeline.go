package yowza

// Parallel timelines over PersistedSegDB — Go core layer.
//
// LAYERING (stated honestly): timelines live in the Go core
// layer, the same layer PersistedSegDB and the query engine
// live in, over the wasm-backed SegDB. A timeline is NOT a
// wrapper over the KV store and NOT a display API: it is a
// versioned entity with its own identity, lineage, overlay
// WAL, and head, persisted in the same directory as the
// database it branches from. Wasm-native timeline branching
// inside the C VM is NOT part of this item: the C engine
// knows nothing about timelines; branch isolation is enforced
// here, by never writing timeline writes into the shared
// PersistedSegDB head. Timeline writes go only to the
// timeline's own overlay WAL and in-memory overlay. The
// shared database therefore never sees another timeline's
// unmerged writes, and a timeline read never sees the
// parent's current head — only the parent state at the
// branch point (see read resolution below).
//
// READ RESOLUTION. A timeline's view is, in order:
//   1. its own overlay (latest entry per key; a tombstone
//      hides everything below it),
//   2. each ancestor timeline's overlay, restricted to
//      entries with sequence <= the fork head recorded when
//      the next timeline down branched (so a parent's later
//      writes are invisible to an already-forked child),
//   3. the underlying database state at the branch-point
//      snapshot vector, read via ScanStreaming at that
//      vector (never the parent's current head).
// Step 2 is why fork-from-a-timeline is sound even though
// the parent's overlay is not in the database: the ancestor
// WALs are append-only and sequence-numbered, so "parent at
// fork time" is a prefix of the parent WAL, recoverable
// after restart by replay. Step 3 is why the root snapshot
// vector is inherited down a lineage unchanged.
//
// BRANCH-POINT PINNING. At fork time the manager registers
// one SegDB lease per shard for the branch vector, held for
// the timeline's lifetime (released only on delete). While
// any such lease is stale (sequence below current),
// SegDB.Compact defers entirely (Gate 1 rule), so
// compaction cannot drop the history a live timeline still
// needs. Two further guards, because leases are in-memory
// and therefore do not survive a restart on their own:
//   - ForkFromDB / ForkAt flush the underlying database
//     first, so the branch point is in flushed segments
//     (flushed history survives restart; unflushed hot
//     history does not — Gate 1 limit).
//   - Every fork records a fingerprint (SHA-256 over the
//     sorted branch view). On manager open, each live
//     timeline's branch view is recomputed and compared;
//     a mismatch means the history the timeline needs is
//     gone (compaction outside the manager, or restart
//     loss of unflushed history), and that timeline is
//     marked broken: every operation on it fails with
//     ErrTimelineBranchPointGone, while other timelines
//     keep working. A fork from an explicit vector is
//     likewise refused unless that vector is the current
//     vector or is pinned by a live timeline's branch
//     vector: an unpinned old vector is unverifiable —
//     Gate 1 says old snapshots without a lease are
//     best-effort — and forking from it could silently
//     fork from a newer, compacted-away state. Forking
//     from a live timeline needs no such check: the
//     parent's view is materialized live from its WAL
//     prefix plus its pinned root snapshot.
//
// MERGE. Three-way, base = branch point, supported only
// where the base is unambiguous: the target is the source's
// direct parent, or source and target are siblings forked
// from the same parent at the same parent head. Anything
// else (e.g. merging into a grandparent, whose view at the
// source's fork time is not a single recorded view) fails
// with ErrTimelineNoCommonBase; merge stepwise through the
// chain instead. Per key: only one side changed -> take
// it; both changed identically -> take it (already present
// in the target, reported as identical, not re-written);
// both changed differently -> CONFLICT. Default policy is
// fail: the merge applies NOTHING and returns a typed
// *TimelineMergeConflictError listing every conflicting
// key. Source-wins / target-wins resolve only the
// conflicted keys and still report them. A merge into a
// target whose head moved since the caller read it fails
// with ErrTimelineStaleHead; there is no silent rebase.
// An effective merge is one WAL record (self-contained
// applied set) fsynced, then a registry publish, then
// acknowledgement. A crash between the WAL fsync and the
// registry publish resolves to post-merge on reopen (the
// record replays, the registry head is healed) — one of
// the two allowed outcomes, never half, because the record
// is checksummed and applied atomically on replay.
//
// LOCKING (Phase A, A1 — the rules, enforced by the code
// below, not just documented).
//   1. Manager before timeline before database: m.mu is
//      always acquired before any ts.mu, and any database
//      lock (p.mu, leaseMu, host.mu) is always acquired
//      after timeline locks, never before. No path takes
//      them in reverse.
//   2. Normal operations (Put, DeleteKey, Get, Scan,
//      Compare, Info, HeadSeq, ListTimelines) hold the
//      manager READ lock from lookup through the END of
//      WAL/overlay/snapshot use. They never release it
//      early: the old release-then-lock pattern let Close
//      (write lock) close a WAL while a Put was still
//      walking to it, racing on ts.wal, and let Get
//      deadlock against Merge (Get: child then parent;
//      Merge: parent then child). Holding RLock makes
//      structural operations wait until the operation
//      fully ends, and makes Close safe without taking
//      each timeline's lock (nothing can be mid-WAL-use
//      once Close holds the write lock).
//   3. Structural operations (Fork, ForkFromDB, ForkAt,
//      Merge, DeleteTimeline, FlushTimeline, Close) hold
//      the manager WRITE lock for their whole duration.
//      They exclude normal operations, not each other
//      beyond the mutex itself.
//   4. Multiple timeline locks are ALWAYS acquired in
//      ascending timeline-number order (creation order;
//      a parent's number is always below its children's),
//      via lockTimelinesOrdered, and released in reverse.
//      No ID-order locking, no leaf-first locking, no
//      caller-held subset plus viewLocked acquiring the
//      rest: viewLocked/branchView/commonBaseView assume
//      the caller already holds every timeline lock they
//      read, and acquire none themselves. (At manager open
//      the process is single-threaded, so no locks are
//      needed there.)
//   5. Concurrency between independent timelines is KEPT:
//      normal operations share the read lock and work on
//      their own WAL under their own timeline lock; only
//      structural operations needing coordination are
//      exclusive.
//
// POISON (Phase A, A3). See IndeterminateTimelineWALError
// and appendRecordLocked: a failed append whose outcome
// cannot be proven (partial write or failed sync that
// cannot be truncated back) poisons that timeline for
// writes, merges, and structural changes until a valid
// recovery (close + reopen, which replays the WAL and
// determines whether the record is present or absent).
// Reads of the unchanged in-memory view stay available.
//
// PUBLISH (Phase A, A4). See TimelinePublishStatus:
// registry publish reports NotPublished /
// PublishedDurable / PublishedIndeterminate. In the third
// state the manager freezes all mutations (but not reads
// or Close) until recovery, and no new-generation file is
// deleted.
//
// OPENING (Phase A, A5). See NewTimelineManager: a
// per-directory "opening" reservation serialises
// concurrent opens; the second caller waits for the first
// init to end and then either gets ErrTimelineManagerExists
// or retries its own open if the first init failed. The
// reservation is freed on every exit, and init errors
// close every WAL opened so far and release every lease
// registered so far.
//
// NOT SUPPORTED (honest limits, see also the report):
// timelines across independent PersistedSegDB instances
// (a manager is bound to one open database); multi-process
// use (the database directory lock already forbids a
// second process opening the same database, and a second
// in-process manager on the same directory is refused);
// merging a timeline back into the shared database head
// (merge is timeline-to-timeline; materialising a timeline
// into PersistedSegDB is a separate operation, not here);
// timelines whose values exceed PersistedSegDB limits
// (key 1..1023 bytes, value <= 1024 bytes, key+value
// <= 1014, the same parity cap PersistedSegDB.Put
// enforces, so any timeline value stays database-writable).

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
)

// Sentinel errors. Compare with errors.Is; the typed errors
// below wrap these.
var (
	ErrTimelineManagerClosed   = errors.New("yowza timeline: manager closed")
	ErrTimelineManagerExists   = errors.New("yowza timeline: a manager is already open for this directory")
	ErrTimelineNotFound        = errors.New("yowza timeline: unknown timeline")
	ErrTimelineDeleted         = errors.New("yowza timeline: timeline is deleted")
	ErrTimelineIDInvalid       = errors.New("yowza timeline: invalid timeline id")
	ErrTimelineIDReuse         = errors.New("yowza timeline: timeline id was used before and can never be reused")
	ErrTimelineHasChildren     = errors.New("yowza timeline: timeline has live children")
	ErrTimelineBranchPointGone = errors.New("yowza timeline: branch-point history is gone")
	ErrTimelineNoCommonBase    = errors.New("yowza timeline: timelines share no unambiguous merge base")
	ErrTimelineStaleHead       = errors.New("yowza timeline: target head moved since the merge was computed")
	ErrTimelineKeyNotFound     = errors.New("yowza timeline: key not found in timeline view")
	ErrTimelineMergeConflict   = errors.New("yowza timeline: merge conflict")
	ErrTimelineCorrupt         = errors.New("yowza timeline: timeline storage corrupt")
	ErrTimelineInvalid         = errors.New("yowza timeline: invalid argument")
	// ErrTimelineWALSequence: a WAL record's sequence is not
	// the expected next sequence (gap, duplicate, or out of
	// order). The suffix is never accepted as a valid WAL.
	ErrTimelineWALSequence = errors.New("yowza timeline: WAL sequence gap or duplicate")
	// ErrTimelineWALIndeterminate: a WAL append failed in a
	// way whose outcome cannot be proven (see
	// IndeterminateTimelineWALError). Blind retry is
	// forbidden: the record may or may not be on disk.
	ErrTimelineWALIndeterminate = errors.New("yowza timeline: WAL append outcome indeterminate")
	// ErrTimelinePublishIndeterminate: a registry rename
	// succeeded but final durability (directory sync) is
	// unknown (see TimelinePublishIndeterminateError).
	ErrTimelinePublishIndeterminate = errors.New("yowza timeline: registry publish indeterminate")
)

// IndeterminateTimelineWALError is returned when a WAL append
// cannot be proven to have happened or not happened. It
// names the timeline, the sequence, and the failure stage,
// because recovery (and a human reading the log) needs all
// three: which state is indeterminate, which sequence must
// not be reused, and where the proof broke down. Stages:
// "write" (write itself failed, truncate also failed),
// "sync" (sync failed, truncate also failed),
// "truncate-after-write" / "truncate-after-sync" (the
// truncate step failed), "sync-after-truncate" (truncate
// succeeded but the sync that would make the truncation
// durable failed — the record may reappear after a crash).
// A "definite failure" is NEVER this error: it is only
// returned when truncate + sync both succeed, the record is
// provably absent, and the sequence may be reused.
type IndeterminateTimelineWALError struct {
	TimelineID string
	Seq        uint64
	Stage      string
	Err        error
}

func (e *IndeterminateTimelineWALError) Error() string {
	return fmt.Sprintf("yowza timeline: indeterminate WAL state for timeline %s seq %d at stage %s: %v (writes, merges and structural changes on this timeline are forbidden until recovery by close and reopen)", e.TimelineID, e.Seq, e.Stage, e.Err)
}

func (e *IndeterminateTimelineWALError) Unwrap() error { return ErrTimelineWALIndeterminate }

// TimelinePublishIndeterminateError is returned when a
// registry publish reached the rename but the final
// directory sync failed. Op names the operation that was
// publishing ("fork", "delete", "merge", "flush", "open").
// The manager enters indeterminate mode on this error:
// all mutations are refused until recovery (close +
// reopen), while reads and Close still work.
type TimelinePublishIndeterminateError struct {
	Op  string
	Err error
}

func (e *TimelinePublishIndeterminateError) Error() string {
	return fmt.Sprintf("yowza timeline: registry publish indeterminate during %s: rename succeeded but directory sync failed (%v); manager mutations are frozen until recovery by close and reopen", e.Op, e.Err)
}

func (e *TimelinePublishIndeterminateError) Unwrap() error {
	return ErrTimelinePublishIndeterminate
}

// timelineWALWriter is the append path's view of a timeline
// WAL file: write, fsync, truncate. Production uses the
// real *os.File; tests replace it with a fault-injecting
// wrapper to prove both outcomes of a sync failure (record
// present vs record absent) without a real filesystem
// fault. The file's lifecycle (open/close) still uses
// ts.wal directly.
type timelineWALWriter interface {
	Write([]byte) (int, error)
	Sync() error
	Truncate(int64) error
}

type timelineOSWAL struct{ f *os.File }

func (w timelineOSWAL) Write(p []byte) (int, error) { return w.f.Write(p) }
func (w timelineOSWAL) Sync() error                 { return w.f.Sync() }
func (w timelineOSWAL) Truncate(n int64) error      { return w.f.Truncate(n) }

// TimelineMergeConflictError lists every conflicting key of a
// failed (fail-policy) merge. Nothing was applied.
type TimelineMergeConflictError struct {
	SourceID string
	TargetID string
	Keys     []string
}

func (e *TimelineMergeConflictError) Error() string {
	return fmt.Sprintf("yowza timeline: merge %s -> %s conflicts on keys %v", e.SourceID, e.TargetID, e.Keys)
}

func (e *TimelineMergeConflictError) Unwrap() error { return ErrTimelineMergeConflict }

// TimelineStaleHeadError reports a refused stale-head merge.
type TimelineStaleHeadError struct {
	TargetID string
	Expected uint64
	Actual   uint64
}

func (e *TimelineStaleHeadError) Error() string {
	return fmt.Sprintf("yowza timeline: merge into %s expected head %d, actual %d", e.TargetID, e.Expected, e.Actual)
}

func (e *TimelineStaleHeadError) Unwrap() error { return ErrTimelineStaleHead }

// TimelineHasChildrenError lists the live children blocking a delete.
type TimelineHasChildrenError struct {
	TimelineID string
	Children   []string
}

func (e *TimelineHasChildrenError) Error() string {
	return fmt.Sprintf("yowza timeline: %s has live children %v", e.TimelineID, e.Children)
}

func (e *TimelineHasChildrenError) Unwrap() error { return ErrTimelineHasChildren }

// TimelineConflictPolicy selects merge conflict handling.
type TimelineConflictPolicy int

const (
	// TimelineConflictFail (the zero value, and the safe
	// default): any conflict fails the whole merge and
	// applies nothing. Rationale: a timeline merge is a
	// version-control operation, not a last-writer-wins
	// store write; silently picking a side would destroy
	// one timeline's intentional change without its author
	// ever seeing the loss. Fail loudly, list every
	// conflict, let the caller choose explicitly.
	TimelineConflictFail TimelineConflictPolicy = iota
	// TimelineConflictSourceWins resolves only conflicted
	// keys to the source view; all other keys merge by
	// the normal three-way rules.
	TimelineConflictSourceWins
	// TimelineConflictTargetWins resolves only conflicted
	// keys to the target view (i.e. keeps them); all other
	// keys merge by the normal three-way rules.
	TimelineConflictTargetWins
)

func (p TimelineConflictPolicy) String() string {
	switch p {
	case TimelineConflictSourceWins:
		return "source-wins"
	case TimelineConflictTargetWins:
		return "target-wins"
	default:
		return "fail"
	}
}

// TimelineValueState distinguishes the three things a key
// can be in a timeline view: never present, present with a
// value, or explicitly deleted by a timeline tombstone.
// Tombstone vs absent is a real distinction: a tombstone is
// an intentional change (it deletes a base value on merge),
// while absent means "untouched, inherits the base".
type TimelineValueState int

const (
	TimelineAbsent TimelineValueState = iota
	TimelinePresent
	TimelineDeleted
)

// TimelineValue is one side of a diff entry.
type TimelineValue struct {
	State TimelineValueState
	Value []byte // set only when State == TimelinePresent
}

// TimelineDiffEntry is one key of a diff: base value, side-A
// value, side-B value. For Compare(a, b), A is a's view and
// B is b's view and Base is their common branch-point view.
// For CompareToBranchPoint(t), A is the branch-point view
// and B is t's view (Base repeats A).
type TimelineDiffEntry struct {
	Key   string
	Base  TimelineValue
	SideA TimelineValue
	SideB TimelineValue
}

// MergeReport is the result of a merge — never a bare bool.
// AppliedKeys are keys whose target view actually changed
// (one-sided source changes, plus source-wins resolutions).
// IdenticalKeys are keys both sides changed identically
// (already present in the target, not re-written).
// ConflictedKeys are keys both sides changed differently,
// whatever the policy did with them. Under fail policy a
// MergeReport is never returned: the typed conflict error
// is, and nothing was applied.
type MergeReport struct {
	MergedTimeline   string                 `json:"merged_timeline"`
	SourceID         string                 `json:"source_id"`
	TargetID         string                 `json:"target_id"`
	SourceHead       uint64                 `json:"source_head"`
	TargetHeadBefore uint64                 `json:"target_head_before"`
	NewHead          uint64                 `json:"new_head"`
	AppliedKeys      []string               `json:"applied_keys"`
	IdenticalKeys    []string               `json:"identical_keys"`
	ConflictedKeys   []string               `json:"conflicted_keys"`
	Policy           TimelineConflictPolicy `json:"policy"`
	BaseFingerprint  string                 `json:"base_fingerprint"`
	// Phase C (MergeGeneral only): the DAG base commit,
	// the delta-segment file holding the applied set
	// (AppliedKeys is then nil — the set lives in the
	// segment, not the report), and its key count.
	BaseCommitID string `json:"base_commit_id,omitempty"`
	DeltaFile    string `json:"delta_file,omitempty"`
	AppliedCount int    `json:"applied_count,omitempty"`
}

// TimelineInfo describes one timeline (live or, via the
// registry, deleted — Info itself reports live timelines
// only and returns ErrTimelineDeleted for tombstones).
type TimelineInfo struct {
	ID                string
	Number            uint64
	ParentID          string
	HeadSeq           uint64
	BranchVector      []uint64
	BranchParentHead  uint64
	BranchFingerprint string
	LastMerge         *MergeReport
	// Phase C: the durable pin for this branch point (empty
	// for unmigrated v1 timelines) and the newest commit-DAG
	// node (empty when the timeline has no DAG node yet).
	PinID        string
	HeadCommitID string
}

type timelineOverlayEntry struct {
	seq     uint64
	deleted bool
	val     []byte
}

type timelineState struct {
	mu                sync.Mutex
	id                string
	number            uint64
	parentID          string
	branchVector      []uint64
	branchParentHead  uint64
	branchFingerprint string
	headSeq           uint64
	// overlay keeps the full per-key write history (append-only
	// lists in sequence order), not just the latest entry:
	// reading an ancestor at a fork head needs older entries
	// for keys the ancestor later overwrote.
	overlay  map[string][]timelineOverlayEntry
	wal      *os.File
	walIO    timelineWALWriter // append path; nil until the WAL is opened
	walSize  int64             // tracked WAL size; Stat() is preferred when it works
	walPath  string
	leaseIDs []uint64
	deleted  bool
	// pinID is the Phase B durable pin for this timeline's branch
	// vector (Phase C, C0), published BEFORE the registry entry
	// that references it. Empty for pre-Phase-C (v1) timelines
	// until the manager migrates them on open (see
	// newTimelineManagerInit). The pin's leases are owned by
	// PersistedSegDB (segdb_pins.go), NOT by leaseIDs above:
	// branch leases are released on manager close, pin leases
	// only by DeletePin / orphan cleanup, so a durable pin
	// survives a manager close with the database still open.
	pinID string
	// dagHead is the newest commit-DAG node for this timeline
	// (Phase C, C2), empty for timelines that have no DAG node
	// yet (legacy timelines, or a timeline whose only commits
	// predate the DAG).
	dagHead string
	// snapshotRootID is the snapshot root this timeline's
	// branch point belongs to (Phase C, C2); empty for
	// legacy timelines whose root was never recorded.
	snapshotRootID string
	// checkpointFile / checkpointGen name the latest validated
	// delta-segment checkpoint of this timeline's overlay
	// (Phase C, C2); empty when no checkpoint exists. The WAL
	// remains authoritative — the checkpoint is an additional
	// durable, checksummed snapshot of the overlay, not a
	// replacement for WAL replay (see timeline_dag.go).
	checkpointFile string
	checkpointGen  uint64
	broken         error // non-nil when branch history was lost; ops fail
	// poisoned is non-nil after an indeterminate WAL append
	// (A3): writes, merges, and structural changes on this
	// timeline are forbidden until recovery by reopen.
	// The in-memory overlay/headSeq are unchanged by the
	// failed append, so reads stay available.
	poisoned  *IndeterminateTimelineWALError
	lastMerge *MergeReport
}

// TimelineManager owns all timelines branching from one open
// PersistedSegDB. It does NOT take its own directory flock:
// the database already holds an exclusive flock on dir/LOCK,
// / and a second flock from the same process would fail
// against it. Single-manager-per-directory is enforced
// in-process by the package-level guard below; a second
// process cannot open the database at all (ErrDatabaseLocked).
type TimelineManager struct {
	db         *PersistedSegDB
	dir        string
	sync       bool
	mu         sync.RWMutex // write: fork/merge/delete/close/publish; read: single-timeline ops
	timelines  map[string]*timelineState
	nextNumber uint64
	closed     bool
	// publishIndeterminate is set by publishRegistryLocked
	// when a publish reaches PublishedIndeterminate (A4).
	// While set, checkMutableLocked refuses every mutation;
	// reads and Close still work. Only a fresh manager
	// (close + reopen) clears it.
	publishIndeterminate error
	// dagCommits is the Phase C commit DAG (timeline_dag.go):
	// commit ID -> commit. Persisted in the registry as
	// dag_commits (optional field; v1 registries load with
	// an empty DAG). snapshotRoots maps snapshot-root ID ->
	// root metadata for DAG fork commits.
	dagCommits    map[string]*TimelineCommit
	snapshotRoots map[string]*TimelineSnapshotRoot
	// Phase E registry checkpoint + delta log
	// (timeline_registry_log.go): lastRegistryDoc is
	// the last durably published document (a copy);
	// registryLastDeltaSeq is the last delta seq
	// published (or the checkpoint's, when no delta
	// has been published since it); registryDeltas
	// counts delta publishes since the checkpoint.
	// These are only meaningful after a successful
	// load/publish; a nil lastRegistryDoc means "no
	// checkpoint published yet" and forces the next
	// publish to be a checkpoint.
	lastRegistryDoc      *timelineRegistryDoc
	registryLastDeltaSeq uint64
	registryDeltas       int
}

var (
	timelineManagersMu sync.Mutex
	timelineManagers   = map[string]*TimelineManager{}
	// timelineManagersOpening is the A5 reservation: dir ->
	// channel closed when that directory's manager init
	// ends (success or failure). A second opener waits on
	// the channel instead of racing the first init, then
	// re-checks: existing manager -> ErrTimelineManagerExists,
	// init failed -> it tries its own open.
	timelineManagersOpening = map[string]chan struct{}{}
)

// lockTimelinesOrdered locks the given timeline states in
// ascending timeline-number order (deduped) and returns an
// unlock function releasing them in reverse order. This is
// the ONLY way to hold more than one timeline lock: number
// order is a fixed global order (parents always precede
// children), so two operations locking overlapping sets in
// this order can never deadlock against each other.
func lockTimelinesOrdered(tss ...*timelineState) func() {
	seen := map[*timelineState]bool{}
	uniq := make([]*timelineState, 0, len(tss))
	for _, ts := range tss {
		if ts != nil && !seen[ts] {
			seen[ts] = true
			uniq = append(uniq, ts)
		}
	}
	sort.Slice(uniq, func(i, j int) bool { return uniq[i].number < uniq[j].number })
	for _, ts := range uniq {
		ts.mu.Lock()
	}
	return func() {
		for i := len(uniq) - 1; i >= 0; i-- {
			uniq[i].mu.Unlock()
		}
	}
}

var timelineReservedIDs = map[string]bool{
	"main": true, "root": true, "registry": true,
}

// validateTimelineID enforces the identity format: 1..64
// chars, first [A-Za-z0-9], rest [A-Za-z0-9_-], no reserved
// names. The charset is filename-safe (the WAL file is
// timeline-<id>.wal) and excludes '/' and '.' outright.
func validateTimelineID(id string) error {
	if len(id) == 0 || len(id) > 64 {
		return fmt.Errorf("%w: id length %d out of range 1..64", ErrTimelineIDInvalid, len(id))
	}
	if timelineReservedIDs[id] {
		return fmt.Errorf("%w: %q is reserved", ErrTimelineIDInvalid, id)
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		ok := (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' || c == '-'
		if i == 0 && (c == '_' || c == '-') {
			ok = false
		}
		if !ok {
			return fmt.Errorf("%w: %q contains disallowed character %q", ErrTimelineIDInvalid, id, c)
		}
	}
	return nil
}

func validateTimelineValue(key string, val []byte) error {
	if len(key) == 0 || len(key) > 1023 {
		return fmt.Errorf("%w: key length %d out of range 1..1023", ErrTimelineInvalid, len(key))
	}
	if len(val) > 1024 {
		return fmt.Errorf("%w: value length %d exceeds 1024", ErrTimelineInvalid, len(val))
	}
	if len(key)+len(val) > 1014 {
		return fmt.Errorf("%w: key+value length %d exceeds hot-path budget 1014", ErrTimelineInvalid, len(key)+len(val))
	}
	return nil
}

func equalUint64Slice(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// NewTimelineManager opens (or creates) the timeline registry
// in db's directory and replays every live timeline's WAL. It
// re-registers branch-point leases, validates branch
// fingerprints (mismatches mark that timeline broken, see the
// file header), heals registry heads from WAL replay, and
// removes orphan WAL files. It must be closed before the
// database is closed; closing it releases all leases.
//
// CONCURRENT OPEN (A5): the old code checked the manager map
// under the global lock, released it, and only added the new
// manager AFTER replay + validation — so two goroutines
// could both pass the check and open the same directory's
// registry + WALs simultaneously. Now the first opener
// reserves the directory ("opening" state) before init
// starts; a second opener waits for init to end, then gets
// ErrTimelineManagerExists if the first init succeeded, or
// retries its own open if the first init failed. The
// reservation is freed on every exit (success, failure, or
// panic-free error return), and init failures clean up all
// WALs opened and leases registered so far (see
// newTimelineManagerInit's error defer).
func NewTimelineManager(db *PersistedSegDB) (*TimelineManager, error) {
	if db == nil {
		return nil, fmt.Errorf("%w: nil database", ErrTimelineInvalid)
	}
	dir := db.Dir()
	for {
		timelineManagersMu.Lock()
		if _, ok := timelineManagers[dir]; ok {
			timelineManagersMu.Unlock()
			return nil, fmt.Errorf("%w: %s", ErrTimelineManagerExists, dir)
		}
		if done, ok := timelineManagersOpening[dir]; ok {
			timelineManagersMu.Unlock()
			<-done
			continue
		}
		done := make(chan struct{})
		timelineManagersOpening[dir] = done
		timelineManagersMu.Unlock()

		m, err := newTimelineManagerInit(db, dir)
		timelineManagersMu.Lock()
		delete(timelineManagersOpening, dir)
		if err == nil {
			timelineManagers[dir] = m
		}
		close(done)
		timelineManagersMu.Unlock()
		return m, err
	}
}

func newTimelineManagerInit(db *PersistedSegDB, dir string) (m *TimelineManager, err error) {
	m = &TimelineManager{
		db:            db,
		dir:           dir,
		sync:          db.Durability() == DurabilitySync,
		timelines:     map[string]*timelineState{},
		nextNumber:    1,
		dagCommits:    map[string]*TimelineCommit{},
		snapshotRoots: map[string]*TimelineSnapshotRoot{},
	}
	// A5 cleanup: on ANY init error, close every WAL opened
	// so far and release every lease registered so far, so
	// a retry (or a later manager) starts from a clean
	// slate. closeFilesLocked is safe to call with nothing
	// opened yet (nil WALs skipped, nil lease lists skipped).
	// NOTE: error returns below use `return nil, err`, which
	// sets the named m to nil — so the defer must capture
	// the manager value itself, not the named return.
	mgr := m
	defer func() {
		if err != nil && mgr != nil {
			mgr.closeFilesLocked()
		}
	}()
	// Phase E, E1: complete or reclaim staged global
	// merges BEFORE any timeline view is read: a
	// committed merge must be visible (rolled
	// forward) before branch fingerprints are
	// validated against the DB, and a staged-only
	// merge must be reclaimed before it can leak
	// into a view. A corrupt committed merge fails
	// the open loudly, never half-applied.
	if _, err := RecoverGlobalMerges(db); err != nil {
		return nil, fmt.Errorf("timeline manager global merge recovery: %w", err)
	}
	// Phase E, E4: load through the registry
	// checkpoint + delta log (v1 documents load
	// unchanged when no delta log exists).
	doc, exists, appliedDeltas, lastDeltaSeq, err := timelineLoadRegistryV2(dir)
	if err != nil {
		return nil, err
	}
	if exists {
		m.lastRegistryDoc = registryDocCopy(doc)
		m.registryLastDeltaSeq = lastDeltaSeq
		m.registryDeltas = appliedDeltas
	}
	if exists {
		m.nextNumber = doc.NextNumber
		if m.nextNumber == 0 {
			m.nextNumber = 1
		}
		for _, rec := range doc.Timelines {
			ts := &timelineState{
				id:                rec.ID,
				number:            rec.Number,
				parentID:          rec.ParentID,
				branchVector:      append([]uint64(nil), rec.BranchVector...),
				branchParentHead:  rec.BranchParentHead,
				branchFingerprint: rec.BranchFingerprint,
				headSeq:           rec.HeadSeq,
				overlay:           map[string][]timelineOverlayEntry{},
				walPath:           timelineWALPath(dir, rec.ID),
				deleted:           rec.Deleted,
				lastMerge:         rec.LastMerge,
				pinID:             rec.PinID,
				dagHead:           rec.HeadCommitID,
				snapshotRootID:    rec.SnapshotRootID,
				checkpointFile:    rec.CheckpointFile,
				checkpointGen:     rec.CheckpointGen,
			}
			m.timelines[rec.ID] = ts
		}
		for _, c := range doc.DagCommits {
			if c == nil || c.ID == "" {
				continue
			}
			cp := *c
			cp.ParentIDs = append([]string(nil), c.ParentIDs...)
			m.dagCommits[cp.ID] = &cp
		}
		for _, r := range doc.SnapshotRoots {
			if r == nil || r.ID == "" {
				continue
			}
			cp := *r
			cp.Vector = append([]uint64(nil), r.Vector...)
			m.snapshotRoots[cp.ID] = &cp
		}
	}
	// Replay live WALs (single-threaded: no other goroutine
	// has this manager yet).
	heal := false
	// referencedDeltas collects delta-segment files named by
	// committed merge-ref WAL records, so open-time orphan
	// cleanup (below) deletes only unreferenced delta files.
	referencedDeltas := map[string]bool{}
	for _, ts := range m.timelines {
		if ts.deleted {
			continue
		}
		entries, validLen, lastSeq, incompleteTail, err := timelineReplayWAL(ts.walPath)
		if err != nil {
			// A2: corruption / sequence gap is fatal to
			// open. Nothing is truncated (the error defer
			// above only closes files and releases leases);
			// the file is left exactly as found, for
			// inspection, and the next open will fail the
			// same way until an operator resolves it.
			return nil, err
		}
		if incompleteTail {
			// ONLY an incomplete tail may be truncated:
			// the last record's header or body ran short
			// of EOF, so no acknowledged record is lost.
			// Truncating here also keeps the next append
			// from resurrecting tail bytes after valid ones.
			if st, err := os.Stat(ts.walPath); err != nil {
				return nil, err
			} else if st.Size() > validLen {
				if err := os.Truncate(ts.walPath, validLen); err != nil {
					return nil, err
				}
			}
		}
		for _, ent := range entries {
			if ent.MergeRef != nil {
				// Phase C merge-ref: the commit point is
				// this record; its applied keys live in
				// the named delta file. A missing or
				// corrupt delta for a COMMITTED record
				// fails open (never half-apply); the
				// delta is expanded into the overlay
				// exactly like a type-3 merge record.
				referencedDeltas[ent.MergeRef.DeltaFile] = true
				dents, err := timelineLoadDeltaSegment(filepathJoin(dir, ent.MergeRef.DeltaFile), ent.MergeRef.DeltaSHA256, ent.MergeRef.DeltaKeys)
				if err != nil {
					return nil, fmt.Errorf("%w: timeline %s merge-ref seq %d delta %s: %v", ErrTimelineCorrupt, ts.id, ent.Seq, ent.MergeRef.DeltaFile, err)
				}
				for _, d := range dents {
					ts.overlay[d.Key] = append(ts.overlay[d.Key], timelineOverlayEntry{seq: ent.Seq, deleted: d.Deleted, val: append([]byte(nil), d.Value...)})
				}
				// Heal a DAG merge commit the registry
				// never published (crash between WAL
				// fsync and registry publish): the WAL
				// record is the truth.
				m.healDAGMergeCommit(ts, ent.MergeRef)
				continue
			}
			ts.applyReplayed(ent)
		}
		if lastSeq != ts.headSeq {
			if lastSeq < ts.headSeq {
				// Registry claims records the WAL does not
				// have: operator damage, not a torn tail.
				ts.broken = fmt.Errorf("%w: registry head %d beyond WAL head %d", ErrTimelineCorrupt, ts.headSeq, lastSeq)
			} else {
				// WAL has fsynced records the registry never
				// published (crash after WAL fsync, before
				// registry publish, e.g. a merge): the WAL
				// is the truth; heal the head.
				ts.headSeq = lastSeq
				heal = true
			}
		}
		f, err := os.OpenFile(ts.walPath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
		if err != nil {
			return nil, err
		}
		ts.wal = f
		ts.walIO = timelineOSWAL{f: f}
		ts.walSize = validLen
	}
	// PATCH2 Track 1: orphan cleanup MOVED BEFORE
	// validation (it ran at the end of init before).
	// Validation reads branch views through the gated
	// facade, so every gate-pending item init heals
	// itself — global merges (RecoverGlobalMerges
	// above), torn WAL tails (truncated in the replay
	// loop above), orphan WALs/deltas (here) — must be
	// resolved AND the gate re-scanned before the
	// first validation scan. Otherwise a healthy
	// reopen with a torn tail failed validation on
	// the gate's stale open-time pending list, even
	// though init had already truncated the tail.
	// Orphan files are unreferenced by definition, so
	// removing them here cannot change any view
	// validation reads.
	// PATCH3 Track 2: transaction-journal recovery
	// joins that list, in the same position — AFTER
	// RecoverGlobalMerges + WAL replay + orphan
	// cleanup above, BEFORE the first validation
	// scan below, followed by the gate refresh.
	// The order matters, not just the presence:
	// branch validation calls branchView, which
	// reads through the gated facade (scanAtVector
	// → ScanStreaming → checkReadsAllowed), and a
	// leftover "applying" journal is now a
	// gate-pending item (see lifecycle.go
	// scanPending). If tx recovery ran after
	// validation, the gate would still be closed
	// at validation and a healthy reopen with a
	// pending journal would fail open with a
	// branch-validation error instead of healing.
	// Running it here rolls the journal forward
	// first, and the refresh below then opens the
	// gate on a clean re-scan. A corrupt journal
	// fails the open loudly (same as a corrupt
	// committed merge above), never half-applied.
	// The existing merge / WAL / orphan steps are
	// NOT reordered; this step is appended after
	// them. The refresh targets the DB's ATTACHED
	// gate (m.db.RefreshRecoveryGate rescans
	// p.gate), exactly as patch2's refresh did —
	// so it releases the gate facade reads
	// actually consult, even if some other gate
	// object was constructed over the directory.
	m.cleanupOrphanWALs()
	m.cleanupOrphanDeltas(referencedDeltas)
	if _, err := RecoverSegDBTransactions(db); err != nil {
		return nil, fmt.Errorf("timeline manager tx journal recovery: %w", err)
	}
	m.db.RefreshRecoveryGate()
	// Pin every live timeline's branch point before any
	// validation scan or user operation can race a compact.
	for _, ts := range m.timelines {
		if ts.deleted || ts.broken != nil {
			continue
		}
		ids, err := m.registerBranchLeases(ts.branchVector)
		if err != nil {
			return nil, fmt.Errorf("timeline %s branch lease: %w", ts.id, err)
		}
		ts.leaseIDs = ids
	}
	// Phase C, C0 — consult durable pins on reopen, in the
	// documented order. Orphan pins (pin published, registry
	// commit missing — crash between pin publish and
	// registry publish, or tombstone published but DeletePin
	// never ran) are removed by CleanupOrphanPins; the
	// branch DATA is never touched by cleanup, only the pin
	// record and its leases. keep() is exactly the live
	// registry set (tombstoned timelines do NOT keep pins).
	liveIDs := map[string]bool{}
	for _, ts := range m.timelines {
		if !ts.deleted {
			liveIDs[ts.id] = true
		}
	}
	if _, err := m.db.CleanupOrphanPins(func(timelineID string) bool { return liveIDs[timelineID] }); err != nil {
		return nil, fmt.Errorf("timeline manager orphan pin cleanup: %w", err)
	}
	// Phase C, C0 — migrate v1 timelines (no pinID) to durable
	// pins. A pin publish failure here does NOT fail open:
	// the branch leases registered above still protect this
	// session exactly as in Phase A, and the next open
	// retries the migration. A migrated pin is published in
	// the registry below (heal path), so a later open finds
	// pinID set and does not re-pin.
	migrated := false
	for _, ts := range m.orderedTimelines() {
		if ts.deleted || ts.broken != nil || ts.pinID != "" {
			continue
		}
		// Adopt an existing active pin for this timeline
		// first (e.g. a v1-era pin whose registry entry
		// lost its pin_id): creating a second pin for
		// the same branch point would only leak a
		// lease slot, never add protection.
		adopted := ""
		if pins, err := m.db.ListPins(); err == nil {
			for _, pin := range pins {
				if pin.Status == PinStatusActive && pin.TimelineID == ts.id && equalUint64Slice(pin.Vector, ts.branchVector) {
					adopted = pin.ID
					break
				}
			}
		}
		if adopted != "" {
			ts.pinID = adopted
			migrated = true
			continue
		}
		pin, err := m.createBranchPin(ts.branchVector, ts.id)
		if err != nil {
			continue
		}
		ts.pinID = pin.ID
		migrated = true
	}
	// Validate fingerprints in creation order (parents always
	// precede children, since numbers increase at creation):
	// a broken ancestor breaks its descendants too, because
	// their branch views are built on the ancestor's WAL
	// prefix plus a root snapshot the ancestor validated.
	ordered := m.orderedTimelines()
	for _, ts := range ordered {
		if ts.deleted || ts.broken != nil {
			continue
		}
		broken := false
		if ts.parentID != "" {
			if p, ok := m.timelines[ts.parentID]; ok && p.broken != nil {
				ts.broken = fmt.Errorf("%w: parent %s broken: %v", ErrTimelineBranchPointGone, ts.parentID, p.broken)
				broken = true
			}
		}
		if broken {
			continue
		}
		// Phase C, C2 — DAG forks record no fingerprint
		// (they never materialized a view). Their branch
		// identity check is metadata + pin, not a full view
		// rebuild: the durable pin must exist, be active,
		// and name exactly this branch vector. A missing or
		// mismatched pin marks the timeline broken, the
		// same way a fingerprint mismatch does for legacy
		// timelines. Legacy timelines (fingerprint present)
		// keep the full fingerprint check as extra
		// validation until the root identity is proven.
		if ts.branchFingerprint == "" && ts.pinID != "" {
			pin, err := m.db.GetPin(ts.pinID)
			if err != nil || pin.Status != PinStatusActive || !equalUint64Slice(pin.Vector, ts.branchVector) {
				ts.broken = fmt.Errorf("%w: durable pin %s for branch vector is missing, deleted, or mismatched", ErrTimelineBranchPointGone, ts.pinID)
			}
			continue
		}
		view, err := m.branchView(ts)
		if err != nil {
			return nil, fmt.Errorf("timeline %s branch validation: %w", ts.id, err)
		}
		if timelineFingerprint(view) != ts.branchFingerprint {
			ts.broken = fmt.Errorf("%w: branch view no longer matches fork-time fingerprint", ErrTimelineBranchPointGone)
		}
	}
	if heal || migrated || !exists {
		// Open-time publish failure (either status) fails
		// the open: nothing has been acknowledged yet, so
		// failing here is safe, and the error defer cleans
		// up WALs/leases. An indeterminate open publish
		// does NOT leave a frozen manager behind — the
		// caller retries the open, which replays the WALs
		// and re-determines the registry state.
		if _, err := m.publishRegistryLocked("open"); err != nil {
			return nil, err
		}
	}
	// PATCH2 Track 1: orphan cleanup and the gate
	// refresh now happen BEFORE fingerprint validation
	// (see the comment at the replay loop above); the
	// end-of-init refresh they replaced is gone, so a
	// healthy open pays for exactly one gate re-scan
	// (which replays every live WAL once). PATCH3
	// Track 2 adds tx-journal recovery to the same
	// pre-validation block (it runs just before
	// that refresh, so it is covered by the same
	// single re-scan). Nothing between that refresh
	// and here creates a gate-pending item:
	// registry publish writes registry files only,
	// and pin migration writes pins.json only.
	// (Tx recovery itself removes the journals it
	// rolls forward, so it creates no pending item
	// either — except a corrupt journal, which
	// fails the open before reaching here.)
	return m, nil
}

func (m *TimelineManager) closeFilesLocked() {
	for _, ts := range m.timelines {
		if ts.wal != nil {
			ts.wal.Close()
			ts.wal = nil
		}
		m.releaseBranchLeases(ts)
	}
}

func (m *TimelineManager) cleanupOrphanWALs() {
	ents, err := os.ReadDir(m.dir)
	if err != nil {
		return
	}
	for _, e := range ents {
		name := e.Name()
		if !strings.HasPrefix(name, "timeline-") || !strings.HasSuffix(name, ".wal") {
			continue
		}
		id := strings.TrimSuffix(strings.TrimPrefix(name, "timeline-"), ".wal")
		ts, ok := m.timelines[id]
		if !ok || ts.deleted {
			_ = os.Remove(filepathJoin(m.dir, name))
		}
	}
}

func filepathJoin(dir, name string) string {
	// Small local join to keep cleanupOrphanWALs free of an
	// extra import cycle concern; identical to filepath.Join
	// for a single-element join.
	if strings.HasSuffix(dir, "/") {
		return dir + name
	}
	return dir + "/" + name
}

func (m *TimelineManager) orderedTimelines() []*timelineState {
	out := make([]*timelineState, 0, len(m.timelines))
	for _, ts := range m.timelines {
		out = append(out, ts)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].number < out[j].number })
	return out
}

func (ts *timelineState) applyReplayed(ent timelineWALEntry) {
	switch ent.Type {
	case timelineWALPut:
		ts.overlay[ent.Key] = append(ts.overlay[ent.Key], timelineOverlayEntry{seq: ent.Seq, val: append([]byte(nil), ent.Value...)})
	case timelineWALDelete:
		ts.overlay[ent.Key] = append(ts.overlay[ent.Key], timelineOverlayEntry{seq: ent.Seq, deleted: true})
	case timelineWALMerge:
		if ent.Merge != nil {
			for _, k := range ent.Merge.Applied {
				ts.overlay[k.Key] = append(ts.overlay[k.Key], timelineOverlayEntry{seq: ent.Seq, deleted: k.Deleted, val: append([]byte(nil), k.Value...)})
			}
		}
	}
}

// appendRecordLocked appends one record to ts's WAL and fsyncs
// when the database runs in Sync mode. Caller holds ts.mu
// (and the manager read or write lock, per the locking rules).
// The caller updates the in-memory overlay only after this
// returns nil, so a failed append leaves no in-memory trace —
// but the on-disk tail may still be indeterminate, and that
// is what this function now tracks (A3).
//
// WRITE/SYNC FAILURE SEMANTICS. A write may be partial; a
// full record may be written but its fsync may fail; the
// caller gets an error while overlay and headSeq are
// unchanged, so the next operation would reuse the same
// sequence and write after the previous tail — and recovery
// could then see a duplicate sequence and lose later
// operations. This function prevents that by deciding, for
// every failure, whether it is DEFINITE or INDETERMINATE:
//   - DEFINITE: the failed record is provably absent —
//     truncate back to the start offset AND sync that
//     truncation, both succeeding. Only then is the error
//     returned as a plain (non-poison) error and the
//     sequence reusable.
//   - INDETERMINATE: any other failure (write failed and
//     truncate failed; sync failed and truncate failed;
//     truncate succeeded but its sync failed, so the
//     record may reappear after a crash). The timeline is
//     poisoned with an IndeterminateTimelineWALError naming
//     the timeline, the sequence, and the stage; every
//     later write, merge, or structural change on that
//     timeline returns that error until a valid recovery.
//     Recovery is close + reopen: replay determines whether
//     the record is present (head heals forward, the
//     sequence is consumed) or absent (head unchanged, the
//     sequence is reusable). An indeterminate result must
//     NOT be solved by blind retry: retrying would write
//     a second record with the same sequence after a tail
//     that may already contain the first.
//
// The start offset is the file's current size (Stat), with
// the tracked walSize as fallback if Stat fails. Truncation
// is always followed by a sync, even in non-Sync mode:
// without it, a crash could resurrect the truncated record
// and the "definite" claim would be false.
func (ts *timelineState) appendRecordLocked(doSync bool, seq uint64, rec []byte) error {
	if ts.poisoned != nil {
		return ts.poisoned
	}
	if ts.wal == nil || ts.walIO == nil {
		return fmt.Errorf("%w: timeline %s WAL not open", ErrTimelineCorrupt, ts.id)
	}
	start := ts.walSize
	if st, err := ts.wal.Stat(); err == nil {
		start = st.Size()
	}
	// writeFull: loop until all bytes are written or an
	// error occurs; a zero-byte nil-error write is a short
	// write, not progress.
	written := 0
	var werr error
	for written < len(rec) {
		n, err := ts.walIO.Write(rec[written:])
		written += n
		if err != nil {
			werr = err
			break
		}
		if n == 0 {
			werr = fmt.Errorf("timeline %s WAL short write at seq %d", ts.id, seq)
			break
		}
	}
	poison := func(stage string, cause error) error {
		ts.poisoned = &IndeterminateTimelineWALError{TimelineID: ts.id, Seq: seq, Stage: stage, Err: cause}
		return ts.poisoned
	}
	// Rollback proving the record absent is truncate to the
	// start offset + sync of that truncation, attempted
	// inline in each failure path below so the poison
	// stage can name exactly which step failed.
	if werr != nil || written != len(rec) {
		if werr == nil {
			werr = fmt.Errorf("timeline %s WAL short write at seq %d (%d of %d bytes)", ts.id, seq, written, len(rec))
		}
		if err := ts.walIO.Truncate(start); err != nil {
			return poison("truncate-after-write", fmt.Errorf("write failed: %w; rollback truncate failed: %v", werr, err))
		}
		if err := ts.walIO.Sync(); err != nil {
			return poison("sync-after-truncate", fmt.Errorf("write failed: %w; rollback truncate succeeded but its sync failed: %v", werr, err))
		}
		ts.walSize = start
		return fmt.Errorf("timeline %s WAL append seq %d failed (definite, record truncated): %w", ts.id, seq, werr)
	}
	if doSync {
		if err := ts.walIO.Sync(); err != nil {
			// The record may be fully on disk with only
			// its durability unproven. Truncate it back;
			// only a truncate + sync that both succeed
			// makes this a definite failure.
			if terr := ts.walIO.Truncate(start); terr != nil {
				return poison("truncate-after-sync", fmt.Errorf("sync failed: %w; rollback truncate failed: %v", err, terr))
			}
			if serr := ts.walIO.Sync(); serr != nil {
				return poison("sync-after-truncate", fmt.Errorf("sync failed: %w; rollback truncate succeeded but its sync failed: %v", err, serr))
			}
			ts.walSize = start
			return fmt.Errorf("timeline %s WAL sync seq %d failed (definite, record truncated): %w", ts.id, seq, err)
		}
	}
	ts.walSize = start + int64(len(rec))
	return nil
}

// registerBranchLeases pins vec on every shard. Caller must
// NOT hold p.mu (this takes it). On failure, already-
// registered leases are released.
func (m *TimelineManager) registerBranchLeases(vec []uint64) ([]uint64, error) {
	p := m.db
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, ErrPersistedClosed
	}
	if len(vec) != len(p.shards) {
		return nil, fmt.Errorf("%w: vector has %d entries, database has %d shards", ErrPersistedSnapshotMismatch, len(vec), len(p.shards))
	}
	ids := make([]uint64, len(p.shards))
	for i, sh := range p.shards {
		id, err := sh.sdb.RegisterLease(vec[i])
		if err != nil {
			for j := 0; j < i; j++ {
				_ = p.shards[j].sdb.ReleaseLease(ids[j])
			}
			return nil, err
		}
		ids[i] = id
	}
	return ids, nil
}

func (m *TimelineManager) releaseBranchLeases(ts *timelineState) {
	if ts.leaseIDs == nil {
		return
	}
	p := m.db
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		// The lease registry lives in the wasm module
		// (C-backed leases on rebuilt binaries) or in the
		// shard's Go map; either way it died with the
		// runtime. Calling ReleaseLease now would execute
		// wasm code in a closed runtime — on wasm64 that
		// segfaults the process instead of returning an
		// error. Skipping is correct, not a leak: there is
		// nothing left to release into. Normal order is
		// still manager-before-database; this branch only
		// makes the wrong order safe instead of fatal.
		ts.leaseIDs = nil
		return
	}
	for i, id := range ts.leaseIDs {
		if i < len(p.shards) {
			_ = p.shards[i].sdb.ReleaseLease(id)
		}
	}
	ts.leaseIDs = nil
}

// scanAtVector materializes the database state at vec as a
// key->value map, via ScanStreaming (which pins the vector
// with transient leases for the scan's duration).
// Phase C counts every call (timelineScanAtVectorCalls):
// DAG forks (ForkFromDBDAG / ForkDAG) must NOT call this —
// their fork-cost proof is that the counter does not move
// while forking a large database.
func (m *TimelineManager) scanAtVector(vec []uint64) (map[string][]byte, error) {
	timelineScanCounterAdd(1)
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
			out[string(e.Key)] = append([]byte(nil), e.Val...)
		}
	}
	return out, nil
}

// chain returns ts's lineage oldest-first (earliest timeline
// ancestor first, ts last). Caller holds a manager lock.
func (m *TimelineManager) chain(ts *timelineState) []*timelineState {
	var rev []*timelineState
	for cur := ts; cur != nil; {
		rev = append(rev, cur)
		if cur.parentID == "" {
			break
		}
		cur = m.timelines[cur.parentID]
	}
	for i, j := 0, len(rev)-1; i < j; i, j = i+1, j-1 {
		rev[i], rev[j] = rev[j], rev[i]
	}
	return rev
}

// applyOverlay applies one timeline's overlay onto view,
// honouring the fork-head limit. Entries per key are stored
// in sequence order, so in-order application is correct.
func applyOverlay(view map[string][]byte, overlay map[string][]timelineOverlayEntry, limit uint64) {
	for k, entries := range overlay {
		for _, e := range entries {
			if e.seq > limit {
				continue
			}
			if e.deleted {
				delete(view, k)
			} else {
				view[k] = append([]byte(nil), e.val...)
			}
		}
	}
}

// viewLocked materializes a timeline view. chain is ts's
// lineage, built by the caller while holding a manager lock.
// LOCKING CONTRACT: viewLocked acquires NO timeline lock
// itself. The caller must already hold every timeline lock
// whose overlay this reads, acquired via
// lockTimelinesOrdered (ascending number order) — or be
// single-threaded at manager open, where no lock is needed.
// The old version acquired ancestor locks itself while
// callers held arbitrary subsets, which is exactly how
// Get (leaf-first) deadlocked against Merge (ID order).
// includeSelf=false yields the branch-point view (ancestors
// only, each limited to the fork head of the next timeline
// down); includeSelf=true yields the current view.
func (m *TimelineManager) viewLocked(ts *timelineState, chain []*timelineState, includeSelf bool) (map[string][]byte, error) {
	view, err := m.scanAtVector(ts.branchVector)
	if err != nil {
		return nil, err
	}
	for i, anc := range chain {
		if anc == ts && !includeSelf {
			continue
		}
		var limit uint64
		if anc == ts {
			limit = ^uint64(0)
		} else {
			limit = chain[i+1].branchParentHead
		}
		applyOverlay(view, anc.overlay, limit)
	}
	return view, nil
}

// branchView is the view a timeline branched from. Caller
// holds the chain's timeline locks (or is single-threaded
// at open); see viewLocked.
func (m *TimelineManager) branchView(ts *timelineState) (map[string][]byte, error) {
	return m.viewLocked(ts, m.chain(ts), false)
}

// publishRegistryLocked publishes the registry built from
// current in-memory state and returns the three-state
// result (A4). Caller holds the manager write lock (or is
// single-threaded at open). On PublishedIndeterminate it
// also sets m.publishIndeterminate, freezing later
// mutations until recovery; callers must then NOT roll
// back (no file deletion, no lease release, no in-memory
// unmarking that destroys the new generation) — except
// DeleteTimeline's in-memory deleted mark, which is
// reverted because the tombstone itself is what is
// indeterminate (see DeleteTimeline).
func (m *TimelineManager) publishRegistryLocked(op string) (TimelinePublishStatus, error) {
	doc := &timelineRegistryDoc{Version: timelineRegistryVersion, NextNumber: m.nextNumber}
	for _, ts := range m.orderedTimelines() {
		rec := timelineRegistryRecord{
			ID:                ts.id,
			Number:            ts.number,
			ParentID:          ts.parentID,
			BranchVector:      append([]uint64(nil), ts.branchVector...),
			BranchParentHead:  ts.branchParentHead,
			BranchFingerprint: ts.branchFingerprint,
			HeadSeq:           ts.headSeq,
			Deleted:           ts.deleted,
			LastMerge:         ts.lastMerge,
			PinID:             ts.pinID,
			HeadCommitID:      ts.dagHead,
			SnapshotRootID:    ts.snapshotRootID,
			CheckpointFile:    ts.checkpointFile,
			CheckpointGen:     ts.checkpointGen,
		}
		doc.Timelines = append(doc.Timelines, rec)
	}
	for _, c := range m.sortedDAGCommits() {
		cp := *c
		cp.ParentIDs = append([]string(nil), c.ParentIDs...)
		doc.DagCommits = append(doc.DagCommits, &cp)
	}
	for _, r := range m.sortedSnapshotRoots() {
		cp := *r
		cp.Vector = append([]uint64(nil), r.Vector...)
		doc.SnapshotRoots = append(doc.SnapshotRoots, &cp)
	}
	// Phase E: publish through the checkpoint + delta
	// log (timeline_registry_log.go). The checkpoint
	// path still uses timelinePublishRegistryWithStatus
	// (same three-state commit point); delta publishes
	// append one line. The op name is preserved in
	// the indeterminate error below for callers that
	// log it; the delta publisher sets its own
	// indeterminate error with its own op label when
	// it is the failing path.
	status, err := m.publishRegistryDeltaLocked(doc)
	if status == TimelinePublishIndeterminate {
		// The delta publisher already set
		// m.publishIndeterminate (with its path as
		// the op label); preserve the caller's op
		// only when no indeterminate error was set.
		if m.publishIndeterminate == nil {
			m.publishIndeterminate = &TimelinePublishIndeterminateError{Op: op, Err: err}
		}
		return status, m.publishIndeterminate
	}
	return status, err
}

// checkMutableLocked refuses a mutation when the manager is
// closed or frozen by an indeterminate publish (A4).
// Callers hold a manager lock (read for normal ops, write
// for structural ops); the flag is only ever set under the
// write lock, so a read-lock holder sees a stable value.
// Reads never call this.
func (m *TimelineManager) checkMutableLocked() error {
	if m.closed {
		return ErrTimelineManagerClosed
	}
	if m.publishIndeterminate != nil {
		return m.publishIndeterminate
	}
	return nil
}

// lookupLocked fetches a live, unbroken timeline. Caller holds
// a manager lock.
func (m *TimelineManager) lookupLocked(id string) (*timelineState, error) {
	if m.closed {
		return nil, ErrTimelineManagerClosed
	}
	ts, ok := m.timelines[id]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrTimelineNotFound, id)
	}
	if ts.deleted {
		return nil, fmt.Errorf("%w: %s", ErrTimelineDeleted, id)
	}
	if ts.broken != nil {
		return nil, ts.broken
	}
	return ts, nil
}

// createTimelineLocked registers a new timeline (record + WAL
// file + registry publish). Caller holds the manager write
// lock and has already validated, fingerprinted, and leased
// the branch point. Publish outcome (A4):
//   - Durable: the timeline is registered; return it.
//   - NotPublished: commit point not reached — roll back
//     fully (unregister, restore nextNumber, close and
//     remove the WAL); the caller then releases the leases.
//   - PublishedIndeterminate: the rename happened but
//     durability is unknown — do NOT roll back. The
//     timeline stays registered with its WAL and leases
//     intact, the manager is frozen by
//     publishRegistryLocked, and the caller must NOT
//     release the leases. Recovery (close + reopen)
//     determines whether the registry on disk names this
//     timeline (durable) or not (orphan WAL cleaned).
//
// createBranchPin publishes a Phase B durable pin for vec
// (Phase C, C0) and returns it. This is step 1 of branch
// creation: the pin is definite (pins.json rename done)
// before this call returns, and the caller commits the
// Timeline registry entry only afterwards. On publish
// failure createPinLocked releases the leases itself and
// no pin is returned. The pin's leases are separate from
// the timeline's branch leases (registerBranchLeases):
// branch leases die with the manager, pin leases only
// with DeletePin / orphan cleanup.
func (m *TimelineManager) createBranchPin(vec []uint64, timelineID string) (*DurableSnapshotPin, error) {
	p := m.db
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, ErrPersistedClosed
	}
	if len(vec) != len(p.shards) {
		return nil, fmt.Errorf("%w: vector has %d entries, database has %d shards", ErrPersistedSnapshotMismatch, len(vec), len(p.shards))
	}
	leaseIDs, err := p.registerLeasesForVectorLocked(vec)
	if err != nil {
		return nil, err
	}
	return p.createPinLocked(vec, leaseIDs, p.generation, "Timeline", timelineID, "")
}

func (m *TimelineManager) createTimelineLocked(id, parentID string, branchVector []uint64, branchParentHead uint64, fingerprint string, leaseIDs []uint64, pinID string) (*timelineState, TimelinePublishStatus, error) {
	walPath := timelineWALPath(m.dir, id)
	// O_APPEND is load-bearing, not stylistic (Phase A):
	// appendRecordLocked truncates back to the start offset
	// on a definite failure, and Truncate does NOT reset
	// the file descriptor's offset. Without O_APPEND, the
	// next append would write at the stale post-truncate
	// offset, leaving a zero hole where the truncated
	// record was and a valid record after it — which
	// replay then (correctly) rejects as corruption.
	// O_APPEND makes every write land at the true EOF.
	f, err := os.OpenFile(walPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL|os.O_APPEND, 0o644)
	if err != nil {
		return nil, TimelinePublishNotPublished, err
	}
	ts := &timelineState{
		id:                id,
		number:            m.nextNumber,
		parentID:          parentID,
		branchVector:      append([]uint64(nil), branchVector...),
		branchParentHead:  branchParentHead,
		branchFingerprint: fingerprint,
		overlay:           map[string][]timelineOverlayEntry{},
		wal:               f,
		walIO:             timelineOSWAL{f: f},
		walPath:           walPath,
		leaseIDs:          leaseIDs,
		pinID:             pinID,
	}
	m.timelines[id] = ts
	m.nextNumber++
	status, err := m.publishRegistryLocked("fork")
	if err != nil {
		if status == TimelinePublishIndeterminate {
			return ts, status, err
		}
		delete(m.timelines, id)
		m.nextNumber--
		f.Close()
		_ = os.Remove(walPath)
		return nil, status, err
	}
	return ts, status, nil
}

func (m *TimelineManager) checkNewIDLocked(id string) error {
	if err := validateTimelineID(id); err != nil {
		return err
	}
	if _, ok := m.timelines[id]; ok {
		// Present in the registry, live or tombstoned: the
		// ID (and its number) can never be reused.
		return fmt.Errorf("%w: %s", ErrTimelineIDReuse, id)
	}
	return nil
}

// ForkFromDB branches a new timeline from the database's
// current state. The database is flushed first so the branch
// point is in flushed segments and survives a restart.
func (m *TimelineManager) ForkFromDB(id string) (*TimelineInfo, error) {
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
	// Phase C, C0 — durable pin FIRST, registry commit
	// second. A crash after the pin returns and before the
	// registry publish leaves an orphan pin, cleaned on
	// the next open; it never leaves a registry entry
	// without a pin.
	pin, err := m.createBranchPin(vec, id)
	if err != nil {
		return nil, fmt.Errorf("timeline %s durable pin: %w", id, err)
	}
	view, err := m.scanAtVector(vec)
	if err != nil {
		_ = m.db.DeletePin(pin.ID)
		return nil, err
	}
	leaseIDs, err := m.registerBranchLeases(vec)
	if err != nil {
		_ = m.db.DeletePin(pin.ID)
		return nil, fmt.Errorf("timeline %s branch lease: %w", id, err)
	}
	ts, status, err := m.createTimelineLocked(id, "", vec, 0, timelineFingerprint(view), leaseIDs, pin.ID)
	if err != nil {
		// Indeterminate: the timeline stays registered
		// with its WAL and leases; do NOT release them,
		// and do NOT delete the pin (the registry entry
		// may stand on reopen). NotPublished: roll the
		// pin back too, so a failed fork leaves no
		// orphan behind in this session.
		if status != TimelinePublishIndeterminate {
			m.releaseBranchLeases(&timelineState{leaseIDs: leaseIDs})
			_ = m.db.DeletePin(pin.ID)
		}
		return nil, err
	}
	return m.infoOf(ts), nil
}

// Fork branches a new timeline from a live timeline's current
// view. The child sees the parent state at fork time, never
// the parent's later writes.
func (m *TimelineManager) Fork(parentID, id string) (*TimelineInfo, error) {
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
	// The write lock already excludes normal operations,
	// so the parent's view and head cannot move while
	// this runs; the chain lock (number order) is still
	// taken for the overlay reads, per the locking rules.
	unlock := lockTimelinesOrdered(m.chain(parent)...)
	if parent.poisoned != nil {
		// Forking from an indeterminate parent would pin
		// a child to a branch view whose last write is
		// unresolved; refuse until recovery.
		poison := parent.poisoned
		unlock()
		return nil, poison
	}
	view, err := m.viewLocked(parent, m.chain(parent), true)
	parentHead := parent.headSeq
	branchVector := append([]uint64(nil), parent.branchVector...)
	unlock()
	if err != nil {
		return nil, err
	}
	pin, err := m.createBranchPin(branchVector, id)
	if err != nil {
		return nil, fmt.Errorf("timeline %s durable pin: %w", id, err)
	}
	leaseIDs, err := m.registerBranchLeases(branchVector)
	if err != nil {
		_ = m.db.DeletePin(pin.ID)
		return nil, fmt.Errorf("timeline %s branch lease: %w", id, err)
	}
	ts, status, err := m.createTimelineLocked(id, parentID, branchVector, parentHead, timelineFingerprint(view), leaseIDs, pin.ID)
	if err != nil {
		if status != TimelinePublishIndeterminate {
			m.releaseBranchLeases(&timelineState{leaseIDs: leaseIDs})
			_ = m.db.DeletePin(pin.ID)
		}
		return nil, err
	}
	return m.infoOf(ts), nil
}

// ForkAt branches a new timeline from an explicit snapshot
// vector of the underlying database. The vector must be the
// current vector or a vector pinned by a live timeline's
// branch point; any other (unpinned, possibly compacted-away)
// old vector fails with ErrTimelineBranchPointGone rather
// than silently forking from a newer state.
func (m *TimelineManager) ForkAt(vec []uint64, id string) (*TimelineInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkMutableLocked(); err != nil {
		return nil, err
	}
	if err := m.checkNewIDLocked(id); err != nil {
		return nil, err
	}
	cur, _, err := m.db.SnapshotVector()
	if err != nil {
		return nil, err
	}
	pinned := equalUint64Slice(vec, cur)
	if !pinned {
		for _, ts := range m.timelines {
			if !ts.deleted && ts.broken == nil && equalUint64Slice(vec, ts.branchVector) {
				pinned = true
				break
			}
		}
	}
	// Phase C, C0 — a durable pin is also a valid branch
	// point: its vector survived restart by construction
	// (pins.json + lease recovery on DB open), unlike an
	// unpinned old vector.
	if !pinned {
		if pins, err := m.db.ListPins(); err == nil {
			for _, pin := range pins {
				if pin.Status == PinStatusActive && equalUint64Slice(vec, pin.Vector) {
					pinned = true
					break
				}
			}
		}
	}
	if !pinned {
		return nil, fmt.Errorf("%w: explicit branch vector is not the current vector and is not pinned by any live timeline or durable pin", ErrTimelineBranchPointGone)
	}
	if err := m.db.Flush(); err != nil {
		return nil, fmt.Errorf("timeline fork flush: %w", err)
	}
	pin, err := m.createBranchPin(vec, id)
	if err != nil {
		return nil, fmt.Errorf("timeline %s durable pin: %w", id, err)
	}
	view, err := m.scanAtVector(vec)
	if err != nil {
		_ = m.db.DeletePin(pin.ID)
		return nil, err
	}
	leaseIDs, err := m.registerBranchLeases(vec)
	if err != nil {
		_ = m.db.DeletePin(pin.ID)
		return nil, fmt.Errorf("timeline %s branch lease: %w", id, err)
	}
	ts, status, err := m.createTimelineLocked(id, "", vec, 0, timelineFingerprint(view), leaseIDs, pin.ID)
	if err != nil {
		if status != TimelinePublishIndeterminate {
			m.releaseBranchLeases(&timelineState{leaseIDs: leaseIDs})
			_ = m.db.DeletePin(pin.ID)
		}
		return nil, err
	}
	return m.infoOf(ts), nil
}

func (m *TimelineManager) infoOf(ts *timelineState) *TimelineInfo {
	return &TimelineInfo{
		ID:                ts.id,
		Number:            ts.number,
		ParentID:          ts.parentID,
		HeadSeq:           ts.headSeq,
		BranchVector:      append([]uint64(nil), ts.branchVector...),
		BranchParentHead:  ts.branchParentHead,
		BranchFingerprint: ts.branchFingerprint,
		LastMerge:         ts.lastMerge,
		PinID:             ts.pinID,
		HeadCommitID:      ts.dagHead,
	}
}

// Info returns a live timeline's info.
func (m *TimelineManager) Info(id string) (*TimelineInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ts, err := m.lookupLocked(id)
	if err != nil {
		return nil, err
	}
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return m.infoOf(ts), nil
}

// HeadSeq returns a live timeline's current head sequence.
func (m *TimelineManager) HeadSeq(id string) (uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ts, err := m.lookupLocked(id)
	if err != nil {
		return 0, err
	}
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return ts.headSeq, nil
}

// ListTimelines returns live timelines in creation (number) order.
func (m *TimelineManager) ListTimelines() ([]*TimelineInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		return nil, ErrTimelineManagerClosed
	}
	var out []*TimelineInfo
	for _, ts := range m.orderedTimelines() {
		if ts.deleted || ts.broken != nil {
			continue
		}
		ts.mu.Lock()
		out = append(out, m.infoOf(ts))
		ts.mu.Unlock()
	}
	return out, nil
}

// Put writes a key in a timeline only. The shared database
// head is untouched. The WAL record is fsynced (Sync mode)
// before Put returns. The manager read lock is held for the
// whole operation (A1): Close and structural operations
// wait until this Put fully ends, so no Put can ever face
// a WAL that Close closed under it. A poisoned timeline
// (A3) or a frozen manager (A4) refuses the write.
func (m *TimelineManager) Put(id, key string, val []byte) error {
	if err := validateTimelineValue(key, val); err != nil {
		return err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := m.checkMutableLocked(); err != nil {
		return err
	}
	ts, err := m.lookupLocked(id)
	if err != nil {
		return err
	}
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if ts.deleted {
		return fmt.Errorf("%w: %s", ErrTimelineDeleted, id)
	}
	if ts.poisoned != nil {
		return ts.poisoned
	}
	seq := ts.headSeq + 1
	rec, err := timelineEncodeRecord(seq, timelineWALPut, key, val, false, nil)
	if err != nil {
		return err
	}
	if err := ts.appendRecordLocked(m.sync, seq, rec); err != nil {
		return err
	}
	ts.overlay[key] = append(ts.overlay[key], timelineOverlayEntry{seq: seq, val: append([]byte(nil), val...)})
	ts.headSeq = seq
	return nil
}

// DeleteKey tombstones a key in a timeline only. Same
// locking and poison/freeze rules as Put.
func (m *TimelineManager) DeleteKey(id, key string) error {
	if len(key) == 0 || len(key) > 1023 {
		return fmt.Errorf("%w: key length %d out of range 1..1023", ErrTimelineInvalid, len(key))
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := m.checkMutableLocked(); err != nil {
		return err
	}
	ts, err := m.lookupLocked(id)
	if err != nil {
		return err
	}
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if ts.deleted {
		return fmt.Errorf("%w: %s", ErrTimelineDeleted, id)
	}
	if ts.poisoned != nil {
		return ts.poisoned
	}
	seq := ts.headSeq + 1
	rec, err := timelineEncodeRecord(seq, timelineWALDelete, key, nil, true, nil)
	if err != nil {
		return err
	}
	if err := ts.appendRecordLocked(m.sync, seq, rec); err != nil {
		return err
	}
	ts.overlay[key] = append(ts.overlay[key], timelineOverlayEntry{seq: seq, deleted: true})
	ts.headSeq = seq
	return nil
}

// Get resolves one key: own overlay, then ancestor overlays
// (fork-head limited), then the branch-point snapshot. The
// manager read lock is held for the whole operation (A1),
// and the chain's timeline locks are taken in ascending
// number order (A1 rule 4) — never leaf-first. The old
// leaf-first-then-ancestors order deadlocked against Merge
// (which held parent and waited for child) once Get had
// released the manager lock; holding RLock plus number
// order removes both halves of that deadlock. Reads are
// allowed on a poisoned timeline and on a frozen manager:
// the in-memory view is unchanged by a failed append.
func (m *TimelineManager) Get(id, key string) ([]byte, error) {
	// PATCH2 Track 1: recovery-gated read, via the
	// DB's per-DB gate state (checked before taking
	// m.mu, so no m.mu -> p.mu nesting is introduced
	// beyond the existing order).
	if err := m.db.checkReadsAllowed(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	ts, err := m.lookupLocked(id)
	if err != nil {
		return nil, err
	}
	chain := m.chain(ts)
	unlock := lockTimelinesOrdered(chain...)
	defer unlock()
	if ts.deleted {
		return nil, fmt.Errorf("%w: %s", ErrTimelineDeleted, id)
	}
	// Walk leaf-first for RESOLUTION (which overlay wins),
	// but all locks were already taken in number order
	// above, so the walk itself locks nothing.
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
				return nil, fmt.Errorf("%w: %s", ErrTimelineKeyNotFound, key)
			}
			return append([]byte(nil), val...), nil
		}
	}
	// Branch-point snapshot, exact-key filter (ScanStreaming
	// is prefix-based, so a prefix hit is not an exact hit).
	it, err := m.db.ScanStreaming(key, ts.branchVector)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	for {
		batch, err := it.NextBatch(64)
		if err != nil {
			return nil, err
		}
		if len(batch) == 0 {
			break
		}
		for _, e := range batch {
			if string(e.Key) == key {
				return append([]byte(nil), e.Val...), nil
			}
		}
	}
	return nil, fmt.Errorf("%w: %s", ErrTimelineKeyNotFound, key)
}

// Scan returns a timeline's full view filtered by prefix,
// key-sorted.
func (m *TimelineManager) Scan(id, prefix string) ([]SegDBScanEntry, error) {
	// PATCH2 Track 1: recovery-gated read (see Get).
	if err := m.db.checkReadsAllowed(); err != nil {
		return nil, err
	}
	// Hold the read lock across viewLocked: it walks the
	// lineage map, which a concurrent fork would mutate.
	m.mu.RLock()
	defer m.mu.RUnlock()
	ts, err := m.lookupLocked(id)
	if err != nil {
		return nil, err
	}
	unlock := lockTimelinesOrdered(m.chain(ts)...)
	view, err := m.viewLocked(ts, m.chain(ts), true)
	unlock()
	if err != nil {
		return nil, err
	}
	var out []SegDBScanEntry
	for k, v := range view {
		if strings.HasPrefix(k, prefix) {
			out = append(out, SegDBScanEntry{Key: []byte(k), Val: append([]byte(nil), v...)})
		}
	}
	sort.Slice(out, func(i, j int) bool { return string(out[i].Key) < string(out[j].Key) })
	return out, nil
}

// FlushTimeline fsyncs a timeline's WAL and publishes the
// registry (checkpointing its head). Ordinary writes do not
// need it — they are durable via the fsynced WAL record —
// but it is the explicit checkpoint operation. A poisoned
// timeline or a frozen manager refuses the flush: publishing
// a registry while the WAL tail is indeterminate could
// checkpoint a head the recovery has not determined yet.
func (m *TimelineManager) FlushTimeline(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkMutableLocked(); err != nil {
		return err
	}
	ts, err := m.lookupLocked(id)
	if err != nil {
		return err
	}
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if ts.poisoned != nil {
		return ts.poisoned
	}
	if ts.wal != nil {
		if err := ts.wal.Sync(); err != nil {
			return err
		}
	}
	_, err = m.publishRegistryLocked("flush")
	return err
}

// DeleteTimeline deletes a timeline only when no live timeline
// branches from it. The alternative (explicit re-parenting)
// was rejected: silently re-pointing children at a new branch
// point would change what their views mean. Deletion releases
// the timeline's leases and removes its WAL file, but the
// registry keeps its tombstone forever (ID/number never reused).
//
// PUBLISH ORDER (A4 — this was wrong before): the old code
// marked the timeline deleted in memory, closed the WAL,
// and released the leases BEFORE publishing the tombstone,
// so a publish failure left in-memory state and the disk
// registry diverged (and the WAL/leases were already gone).
// Now the order is: publish the tombstone FIRST, and only
// on PublishedDurable close the WAL, release the leases,
// and remove the WAL file. On NotPublished the in-memory
// deleted mark is reverted and nothing is closed or
// released — the timeline is fully intact and the delete
// is simply not acknowledged. On PublishedIndeterminate
// the mark is likewise reverted and the WAL/leases are
// kept (new-generation files must not be deleted until
// the outcome is determined); the manager freezes, and
// recovery decides: tombstone durable -> timeline stays
// deleted on reopen; tombstone lost -> timeline reappears.
// A poisoned timeline refuses delete outright: deleting
// would destroy the WAL whose replay is the recovery.
func (m *TimelineManager) DeleteTimeline(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkMutableLocked(); err != nil {
		return err
	}
	ts, err := m.lookupLocked(id)
	if err != nil {
		return err
	}
	var children []string
	for _, other := range m.timelines {
		if !other.deleted && other.parentID == id {
			children = append(children, other.id)
		}
	}
	if len(children) > 0 {
		sort.Strings(children)
		return &TimelineHasChildrenError{TimelineID: id, Children: children}
	}
	ts.mu.Lock()
	if ts.poisoned != nil {
		poison := ts.poisoned
		ts.mu.Unlock()
		return poison
	}
	ts.deleted = true
	_, err = m.publishRegistryLocked("delete")
	if err != nil {
		// Either status: the tombstone is not durably
		// established. Revert the in-memory mark so the
		// live manager stays consistent with "delete not
		// acknowledged". On NotPublished nothing else
		// changed; on Indeterminate the WAL and leases
		// were deliberately never touched above, and the
		// manager is now frozen until recovery.
		ts.deleted = false
		ts.mu.Unlock()
		return err
	}
	// Durable tombstone: only now close, release, remove.
	// Phase C, C0 — deletion order is tombstone FIRST,
	// then DeletePin. A crash between the two leaves a pin
	// whose timeline is already tombstoned: an orphan,
	// cleaned by the registry check on the next open. A
	// DeletePin failure is reported but does not resurrect
	// the timeline: the tombstone is already durable, and
	// the orphan pass will finish the pin cleanup.
	pinID := ts.pinID
	walPath := ts.walPath
	if ts.wal != nil {
		ts.wal.Close()
		ts.wal = nil
		ts.walIO = nil
	}
	m.releaseBranchLeases(ts)
	ts.mu.Unlock()
	var pinErr error
	if pinID != "" {
		pinErr = m.db.DeletePin(pinID)
	}
	// A crash before this leaves an orphan WAL, cleaned on
	// the next open (tombstoned timelines' WALs are removed).
	_ = os.Remove(walPath)
	if pinErr != nil {
		return fmt.Errorf("timeline %s deleted (tombstone durable) but durable pin %s cleanup failed (orphan pass will retry): %w", id, pinID, pinErr)
	}
	return nil
}

// Close releases all leases and WAL files. It does not close
// the database. Idempotent. Close the manager BEFORE the
// database in normal use; closing it after the database is
// also safe (lease release is skipped — the lease registry
// died with the runtime), it just cannot release leases that
// no longer exist.
func (m *TimelineManager) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	// Safe without taking any timeline lock: Close holds
	// the manager write lock, and every normal operation
	// holds the read lock through the end of its WAL use
	// (A1 rule 2), so no operation can be mid-WAL-use
	// now; structural operations are serialized by the
	// same write lock. Close is also the recovery path
	// out of a frozen (indeterminate) manager, so it
	// deliberately does not check checkMutableLocked.
	m.closeFilesLocked()
	m.mu.Unlock()
	timelineManagersMu.Lock()
	if cur, ok := timelineManagers[m.dir]; ok && cur == m {
		delete(timelineManagers, m.dir)
	}
	timelineManagersMu.Unlock()
	return nil
}

// --- Compare ---

func valueStateFromView(view map[string][]byte, overlay map[string][]timelineOverlayEntry, key string) TimelineValue {
	if entries, ok := overlay[key]; ok && len(entries) > 0 {
		last := entries[len(entries)-1]
		if last.deleted {
			return TimelineValue{State: TimelineDeleted}
		}
		return TimelineValue{State: TimelinePresent, Value: append([]byte(nil), last.val...)}
	}
	if v, ok := view[key]; ok {
		return TimelineValue{State: TimelinePresent, Value: append([]byte(nil), v...)}
	}
	return TimelineValue{State: TimelineAbsent}
}

func sameTimelineValue(a, b TimelineValue) bool {
	if a.State != b.State {
		return false
	}
	if a.State == TimelinePresent {
		return string(a.Value) == string(b.Value)
	}
	return true
}

// commonBaseView returns the unambiguous three-way base view
// for a and b, or ErrTimelineNoCommonBase. Supported cases:
// target is the source's direct parent (base = source's branch
// view), or a and b are siblings forked from the same parent
// at the same parent head (base = that shared branch view).
func (m *TimelineManager) commonBaseView(a, b *timelineState) (map[string][]byte, error) {
	if b.id == a.parentID {
		return m.branchView(a)
	}
	if a.id == b.parentID {
		return m.branchView(b)
	}
	if a.parentID != "" && a.parentID == b.parentID &&
		a.branchParentHead == b.branchParentHead &&
		equalUint64Slice(a.branchVector, b.branchVector) {
		return m.branchView(a)
	}
	return nil, fmt.Errorf("%w: %s and %s", ErrTimelineNoCommonBase, a.id, b.id)
}

func diffViews(base, viewA, viewB map[string][]byte, overlayA, overlayB map[string][]timelineOverlayEntry) []TimelineDiffEntry {
	keys := map[string]bool{}
	for k := range base {
		keys[k] = true
	}
	for k := range viewA {
		keys[k] = true
	}
	for k := range viewB {
		keys[k] = true
	}
	for k := range overlayA {
		keys[k] = true
	}
	for k := range overlayB {
		keys[k] = true
	}
	sorted := make([]string, 0, len(keys))
	for k := range keys {
		sorted = append(sorted, k)
	}
	sort.Strings(sorted)
	var out []TimelineDiffEntry
	for _, k := range sorted {
		var bv TimelineValue
		if v, ok := base[k]; ok {
			bv = TimelineValue{State: TimelinePresent, Value: append([]byte(nil), v...)}
		} else {
			bv = TimelineValue{State: TimelineAbsent}
		}
		av := valueStateFromView(viewA, overlayA, k)
		bvv := valueStateFromView(viewB, overlayB, k)
		if sameTimelineValue(bv, av) && sameTimelineValue(bv, bvv) && sameTimelineValue(av, bvv) {
			continue
		}
		out = append(out, TimelineDiffEntry{Key: k, Base: bv, SideA: av, SideB: bvv})
	}
	return out
}

// Compare diffs two timelines (key-sorted). See
// commonBaseView for which pairs have a base.
func (m *TimelineManager) Compare(aID, bID string) ([]TimelineDiffEntry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	a, err := m.lookupLocked(aID)
	if err != nil {
		return nil, err
	}
	b, err := m.lookupLocked(bID)
	if err != nil {
		return nil, err
	}
	if a == b {
		return nil, nil
	}
	// Lock the union of both chains in ascending number
	// order (A1 rule 4): Compare(x, y) and Compare(y, x)
	// running concurrently take the same order and cannot
	// deadlock, and neither can deadlock against Get,
	// Scan, or Merge, which all use the same order.
	chainA := m.chain(a)
	chainB := m.chain(b)
	all := append(append([]*timelineState{}, chainA...), chainB...)
	unlock := lockTimelinesOrdered(all...)
	defer unlock()
	base, err := m.commonBaseView(a, b)
	if err != nil {
		return nil, err
	}
	viewA, err := m.viewLocked(a, chainA, true)
	if err != nil {
		return nil, err
	}
	viewB, err := m.viewLocked(b, chainB, true)
	if err != nil {
		return nil, err
	}
	return diffViews(base, viewA, viewB, a.overlay, b.overlay), nil
}

// CompareToBranchPoint diffs a timeline against the view it
// branched from. SideA is the branch-point side, SideB the
// timeline side, Base repeats SideA.
func (m *TimelineManager) CompareToBranchPoint(id string) ([]TimelineDiffEntry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ts, err := m.lookupLocked(id)
	if err != nil {
		return nil, err
	}
	chain := m.chain(ts)
	unlock := lockTimelinesOrdered(chain...)
	defer unlock()
	base, err := m.branchView(ts)
	if err != nil {
		return nil, err
	}
	view, err := m.viewLocked(ts, chain, true)
	if err != nil {
		return nil, err
	}
	var out []TimelineDiffEntry
	keys := map[string]bool{}
	for k := range base {
		keys[k] = true
	}
	for k := range view {
		keys[k] = true
	}
	for k := range ts.overlay {
		keys[k] = true
	}
	sorted := make([]string, 0, len(keys))
	for k := range keys {
		sorted = append(sorted, k)
	}
	sort.Strings(sorted)
	for _, k := range sorted {
		var bv TimelineValue
		if v, ok := base[k]; ok {
			bv = TimelineValue{State: TimelinePresent, Value: append([]byte(nil), v...)}
		} else {
			bv = TimelineValue{State: TimelineAbsent}
		}
		tv := valueStateFromView(view, ts.overlay, k)
		if sameTimelineValue(bv, tv) {
			continue
		}
		out = append(out, TimelineDiffEntry{Key: k, Base: bv, SideA: bv, SideB: tv})
	}
	return out, nil
}

// --- Merge ---

// Merge merges source into target with the given conflict
// policy. expectedTargetHead must equal target's current head
// or the merge is refused with ErrTimelineStaleHead. Under
// fail policy a conflict returns *TimelineMergeConflictError
// and applies nothing. A no-op merge (nothing differs from
// the target view) writes nothing and returns a report with
// NewHead == TargetHeadBefore.
func (m *TimelineManager) Merge(sourceID, targetID string, policy TimelineConflictPolicy, expectedTargetHead uint64) (*MergeReport, error) {
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
	// Lock the union of both chains in ascending number
	// order for the whole compute+write+publish (A1 rule
	// 4), so the stale-head check cannot race a target
	// write and no lock-order deadlock against Get/Scan/
	// Compare is possible. A poisoned source or target
	// refuses the merge: merging from or into
	// indeterminate state would bake the unresolved
	// sequence into a durable merge record.
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
	if dst.headSeq != expectedTargetHead {
		return nil, &TimelineStaleHeadError{TargetID: targetID, Expected: expectedTargetHead, Actual: dst.headSeq}
	}
	base, err := m.commonBaseView(src, dst)
	if err != nil {
		return nil, err
	}
	baseFP := timelineFingerprint(base)
	srcView, err := m.viewLocked(src, chainSrc, true)
	if err != nil {
		return nil, err
	}
	dstView, err := m.viewLocked(dst, chainDst, true)
	if err != nil {
		return nil, err
	}
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
	var applied []timelineMergeAppliedKey
	var appliedKeys, identicalKeys, conflictedKeys []string
	for _, k := range sorted {
		baseVal, baseOK := base[k]
		srcVal, srcOK := srcView[k]
		dstVal, dstOK := dstView[k]
		srcChanged := !(srcOK == baseOK && (!srcOK || string(srcVal) == string(baseVal)))
		dstChanged := !(dstOK == baseOK && (!dstOK || string(dstVal) == string(baseVal)))
		switch {
		case !srcChanged && !dstChanged:
			// Neither side moved: nothing to do.
		case srcChanged && !dstChanged:
			// One-sided source change: take it.
			if srcOK {
				applied = append(applied, timelineMergeAppliedKey{Key: k, Value: append([]byte(nil), srcVal...)})
			} else {
				applied = append(applied, timelineMergeAppliedKey{Key: k, Deleted: true})
			}
			appliedKeys = append(appliedKeys, k)
		case !srcChanged && dstChanged:
			// One-sided target change: keep it.
		case srcOK == dstOK && (!srcOK || string(srcVal) == string(dstVal)):
			// Both changed identically: target already has it.
			identicalKeys = append(identicalKeys, k)
		default:
			// Both changed differently: conflict.
			conflictedKeys = append(conflictedKeys, k)
		}
	}
	if len(conflictedKeys) > 0 && policy == TimelineConflictFail {
		return nil, &TimelineMergeConflictError{SourceID: sourceID, TargetID: targetID, Keys: append([]string(nil), conflictedKeys...)}
	}
	if len(conflictedKeys) > 0 {
		for _, k := range conflictedKeys {
			if policy == TimelineConflictSourceWins {
				srcVal, srcOK := srcView[k]
				if srcOK {
					applied = append(applied, timelineMergeAppliedKey{Key: k, Value: append([]byte(nil), srcVal...)})
				} else {
					applied = append(applied, timelineMergeAppliedKey{Key: k, Deleted: true})
				}
				appliedKeys = append(appliedKeys, k)
			}
			// Target-wins: keep the target view; the key is
			// reported as conflicted but not written.
		}
	}
	sort.Strings(appliedKeys)
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
	}
	if len(applied) == 0 {
		// No-op merge: nothing differs from the target view.
		// Write nothing (not even a WAL record); the report
		// still records that the merge was computed clean.
		return report, nil
	}
	sort.Slice(applied, func(i, j int) bool { return applied[i].Key < applied[j].Key })
	newHead := dst.headSeq + 1
	payload := &timelineMergePayload{
		SourceID:     sourceID,
		TargetID:     targetID,
		SourceHead:   src.headSeq,
		TargetBefore: dst.headSeq,
		NewHead:      newHead,
		Policy:       policy.String(),
		Applied:      applied,
		Conflicted:   conflictedKeys,
		Identical:    identicalKeys,
		BaseFP:       baseFP,
	}
	rec, err := timelineEncodeRecord(newHead, timelineWALMerge, "", nil, false, payload)
	if err != nil {
		return nil, err
	}
	// Commit order: WAL record fsync, then registry publish,
	// then acknowledgement. Crash between the two resolves
	// to post-merge on reopen (record replays, head healed).
	// An append failure here (including poison) leaves
	// overlay/head untouched: the merge applied nothing.
	if err := dst.appendRecordLocked(m.sync, newHead, rec); err != nil {
		return nil, err
	}
	for _, k := range applied {
		dst.overlay[k.Key] = append(dst.overlay[k.Key], timelineOverlayEntry{seq: newHead, deleted: k.Deleted, val: append([]byte(nil), k.Value...)})
	}
	dst.headSeq = newHead
	report.NewHead = newHead
	dst.lastMerge = report
	status, err := m.publishRegistryLocked("merge")
	if err != nil {
		// WAL record is durable but the registry publish
		// failed: the merge is NOT acknowledged. On reopen
		// the record replays and the merge stands (post-
		// merge outcome); in this live instance the overlay
		// already reflects it, so report the publish error
		// rather than pretending the merge did not happen.
		// On NotPublished the registry on disk is simply
		// the pre-merge one (WAL replay heals it); on
		// PublishedIndeterminate the manager is now frozen
		// (publishRegistryLocked set the flag) and later
		// mutations are refused until recovery — the
		// in-memory post-merge state is consistent with
		// every reopen outcome, since replay heals to it.
		return nil, fmt.Errorf("timeline merge registry publish failed (status %s; merge record %d is durable and will stand on reopen): %w", status, newHead, err)
	}
	return report, nil
}
