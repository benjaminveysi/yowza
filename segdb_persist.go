package yowza

// PersistedSegDB — host-backed persistent storage for the wasm SegDB
// path (deep-integration stage, part 2).
//
// Problem: inside wasm, SegDB keeps its segments/manifests/directory
// in a fixed-size heap region (Standard 8 MiB, or a
// HighCapacity size under HeapProfile), and its hot WAL is
// volatile across module restarts. Copying HeapBytes out to
// the host is a snapshot, not a backend: the total dataset
// stays bounded by that heap.
//
// This file is a pure wrapper/adapter around the existing public Go
// SegDB API (NewSegDB, Put, Get, Delete, Flush, Compact, Recover,
// Reconcile, HeapBytes, RestoreSegDBHeap). It touches no other file
// and changes no SegDB/C behaviour. Persistence lives entirely on
// the host (the Go process), so the contract is identical for the
// wasm32 and wasm64 binaries — only the module inside differs.
//
// Layout of a PersistedSegDB directory:
//
//	wal.log                  append-only host WAL (see format below)
//	heap-<gen>-<i>.bin       heap image for shard i at
//	                         checkpoint generation <gen> (exact
//	                         SegDB heap, written sparsely —
//	                         see persistWriteHeapAtomic).
//	                         Heap files are
//	                         GENERATIONAL and IMMUTABLE: a
//	                         checkpoint never overwrites the heap
//	                         files its predecessor points at.
//	                         (Directories created before the
//	                         generational protocol used the
//	                         legacy name heap-<i>.bin; those are
//	                         still opened, see Open.)
//	checkpoint.json          commit point: generation, last WAL
//	                         seq covered by the heap images,
//	                         per-shard heap SHA-256, per-shard
//	                         heap file names, wasm variant
//
// Sharding: keys are routed by FNV-1a(key) % numShards to one shard,
// each shard being its own wasm module + SegDB with its own heap
// file. A single shard is still bounded by its heap size, but the
// *database* is not: total dataset size scales with the shard
// count, and only one shard's working set lives in wasm memory at
// a time per operation (operations touch exactly one shard, except
// Flush/Compact/Scan which visit each shard in turn). The default
// shard count is 1 (single-shard semantics); callers that want a
// dataset larger than one heap must open with NumShards > 1.
//
// WAL format (host file, little-endian):
//
//	Each record: [frameLen u32][payload][crc64 u64]
//	payload:     [seq u64][op u8][keyLen u32][key][valLen u32][val]
//	crc64:       FNV-1a over payload only.
//	op:          1 = PUT, 2 = DELETE, 3 = ABORT.
//	ABORT records carry keyLen 0 and val = abortedSeq u64: they mark
//	a preceding PUT/DELETE whose wasm application failed AFTER its
//	WAL record was appended (see Put). Replay skips aborted seqs.
//
// Sequence numbers are global and strictly +1 per record (ABORTs
// included), starting at 1. A record is never rewritten in place;
// the file is only appended to (never truncated by this code —
// see the WAL-growth limit in DEEP_HOSTBACKED_NOTES.md).
//
// Checkpoint protocol, GENERATIONAL (commit point =
// checkpoint.json rename; reliability stage item 01):
//
//  1. Flush (or Compact) every shard inside wasm.
//  2. For every shard: HeapBytes -> heap-<newgen>-<i>.tmp.bin,
//     fsync (Sync mode), rename to heap-<newgen>-<i>.bin,
//     directory fsync (Sync mode). The previous generation's
//     heap files are NEVER touched by this step.
//  3. Re-read every new-generation heap file and verify size
//     and SHA-256 against the in-memory images. Only if ALL
//     new-generation files validate, proceed.
//  4. Write checkpoint.json.tmp (new generation, new
//     checkpointSeq = current seq, per-shard heap SHA-256,
//     per-shard heap file names, wasm variant), fsync, rename
//     to checkpoint.json, directory fsync (Sync mode).
//  5. ONLY after the checkpoint publish is definite (rename
//     done, and directory sync done in Sync mode), reclaim
//     heap files not named by the new checkpoint (previous
//     generations and orphans from interrupted checkpoints).
//     Reclaim is best-effort: a failed unlink never fails the
//     checkpoint, because the new checkpoint is already the
//     commit point and extra files are harmless orphans.
//
// A crash before step 4 leaves the old checkpoint authoritative
// AND its heap files byte-identical to before the attempt:
// the new-generation files are unreferenced orphans and are
// ignored on open, and the old heap + WAL replay reconstructs
// the same logical state (replaying PUT/DELETE records already
// covered by a heap image, in original order, yields the same
// final state — PUT overwrites, DELETE tombstones). The old
// protocol overwrote heap-<i>.bin in place before publishing
// the checkpoint, so a crash in that window left the old
// checkpoint pointing at hashes of heap content that no longer
// existed (ErrCorruptHeap, old version unrecoverable); that
// window no longer exists. A crash during step 4 leaves either
// the old or the new checkpoint.json (rename is atomic); the
// heap files named by whichever checkpoint survived are
// checksum-verified. A crash after the checkpoint rename but
// before its directory sync (Sync mode) is reported as
// ErrDurabilityUnavailable, but the new checkpoint is already
// the in-process truth and old-generation heaps are NOT
// reclaimed in that case, so either checkpoint that survives
// the crash still has intact heaps.
//
// POISONED / INDETERMINATE STATE (reliability stage item 01):
// if a WAL append fails (write or fsync), the outcome of that
// sequence is indeterminate — a partial frame may be on disk,
// or a fully written frame may replay even though the caller
// was told the operation failed. The database enters an
// explicit poisoned state: Put/Delete/Flush/Compact refuse
// with ErrPersistedPoisoned (carrying the sequence) until
// recovery. Recovery is Close + reopen: reopen re-parses the
// WAL, and if the tail is corrupt, RepairWALTail + reopen
// decides what was actually recorded. Blind retry of a failed
// mutation inside the poisoned instance is forbidden, because
// it could issue a later sequence over a corrupt tail or
// duplicate a mutation that later replays. A WAL-append
// failure inside appendAbort poisons the same way. Checkpoint
// failures do NOT poison: the old checkpoint + WAL are intact
// and the next Flush/Compact may retry with a fresh
// generation.
//
// FAULT INJECTION (reliability stage item 01): the package-level
// persistFaultHook, when non-nil, is consulted at each wrapper
// write / sync / rename / directory-sync stage (points named
// in persistFaultPoint* constants). Tests set it to fail exactly
// one stage and then verify recovery yields the old valid or
// the new valid version, never an unrecoverable one. It is nil
// in production use.
//
// Recovery on open:
//
//  1. Read checkpoint.json if present (corrupt -> explicit error,
//     never a silent fallback to empty).
//  2. For each shard: verify heap file size (exactly the SegDB
//     heap size) and SHA-256 against the checkpoint; restore,
//     NewSegDB, Recover; reconcile once if indeterminate.
//     A missing/corrupt heap with a checkpoint present is an
//     explicit error — a heap file is never silently skipped.
//     Heap files WITHOUT a checkpoint are orphans (crash before
//     the commit point) and are ignored.
//  3. Parse the whole WAL, verifying every record's checksum and
//     the +1 sequence chain. The first bad/truncated record stops
//     the open with a *CorruptWALError (offset + last valid seq);
//     it is never silently skipped. Use RepairWALTail to truncate
//     to the last valid record and then reopen.
//  4. Replay records with seq > checkpointSeq, in order, skipping
//     aborted seqs, onto the restored shards (PUT/DELETE through
//     the SegDB API, which builds proper hot tombstones, so a
//     replayed delete of a segment-resident key shadows it).
//
// Durability modes:
//
//	DurabilitySync   (default): every WAL append is fsynced before
//	                 the mutation is applied/acknowledged, and every
//	                 checkpoint step fsyncs file + directory. If any
//	                 fsync fails, the operation (or the open) fails
//	                 with ErrDurabilityUnavailable — durability is
//	                 NEVER silently weakened.
//	DurabilityNoSync: no fsync calls. Data is written (and normally
//	                 reaches disk on close/process exit), but a host
//	                 crash or power loss may lose acknowledged
//	                 mutations. Request this explicitly; it is the
//	                 only weaker mode, and it is never chosen by
//	                 default or by fallback.
//
// Single-opener rule: one PersistedSegDB per directory at a time.
// Opening takes an exclusive non-blocking flock on the directory's
// LOCK file (see segdb_persist_lifecycle.go); a second opener gets
// ErrDatabaseLocked before it reads or writes anything.
//
// GATE 2 — Crash-safe storage lifecycle (see
// segdb_persist_lifecycle.go and GATE2_NOTES.md): WAL
// segmentation + rotation + retention, previous-generation
// retention with heap-corruption fallback, mixed checkpoints,
// and the directory lock above. Since O1 (see
// O1_SEGLOOKUP_NOTES.md) BOTH retention behaviours default
// to ON via the tri-state Policy fields in PersistedOptions
// (explicit Policy wins; legacy bool true still means on;
// legacy false means "unset", not "off"; only PolicyOff
// requests the old reclaim behaviour). The old reclaim regime
// is therefore opt-out, not opt-in. Rotation, the lock, and
// segment-coverage validation are always on. Cost note (O1):
// retaining a previous generation costs the previous heap
// files plus the WAL needed to replay from them until
// fallback is no longer needed; in workloads where every
// shard changes on every checkpoint this extra space can be
// notable. This is NOT a substitute for an independent
// backup, and only ONE previous generation is kept — if
// both generations are damaged there is no full-recovery
// guarantee.

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// DurabilityMode selects the fsync behaviour of a PersistedSegDB.
type DurabilityMode int

const (
	// DurabilitySync fsyncs every WAL append and every checkpoint
	// step. This is the default and the only mode that survives a
	// host crash / power loss for acknowledged mutations.
	DurabilitySync DurabilityMode = iota
	// DurabilityNoSync performs no fsync. Acknowledged mutations
	// may be lost on host crash. Use only where that is acceptable.
	DurabilityNoSync
)

// WAL record op codes.
const (
	persistOpPut   = 1
	persistOpDel   = 2
	persistOpAbort = 3
)

// persistWALMaxPayload bounds one record: 8 (seq) + 1 (op) +
// 4 (keyLen) + 1023 (key) + 4 (valLen) + 1024 (val) = 2064.
const persistWALMaxPayload = 2064

// Policy is a three-state option for a PersistedSegDB behaviour:
// Default resolves to that behaviour's documented default, On
// forces it on, Off forces it off. A plain bool cannot express
// this distinction in Go: the zero value false cannot tell
// "unset, use the default" apart from "the user explicitly
// wants this off". That is why the retention policies below
// are Policy fields, not bools, since O1 (see
// O1_SEGLOOKUP_NOTES.md). Default for BOTH retention policies
// is ON.
type Policy int

const (
	// PolicyDefault resolves to the behaviour's default (ON
	// for KeepPreviousGeneration and MixedCheckpoint).
	PolicyDefault Policy = iota
	// PolicyOn forces the behaviour on.
	PolicyOn
	// PolicyOff forces the behaviour off.
	PolicyOff
)

// resolvePolicy applies the single precedence rule for a
// behaviour that has both a Policy field and a legacy bool
// field: an explicit Policy (On or Off) always wins; otherwise
// a legacy bool set to true still means on (backwards
// compatibility for callers written before O1); otherwise the
// documented default applies. Note what this implies for the
// legacy bools: a legacy false NO LONGER means "off" — it
// means "unset", and the default (ON) applies. The ONLY way to
// request off is PolicyOff. There are not two ways to set the
// same feature with an ambiguous outcome: Policy beats bool,
// and bool-true is only a legacy spelling of PolicyOn.
func resolvePolicy(p Policy, legacyBool bool, defaultOn bool) bool {
	switch p {
	case PolicyOn:
		return true
	case PolicyOff:
		return false
	default:
		if legacyBool {
			return true
		}
		return defaultOn
	}
}

// HeapProfile selects a PersistedSegDB's wasm heap size at
// creation time (worker S, High-Capacity Profile track).
// HeapProfileDefault (the zero value, so every existing
// caller is unchanged) means: keep the stored heap size on
// an existing directory, Standard on a fresh one.
// HeapProfileStandard forces the Standard 8 MiB heap: on a
// directory whose stored heap is larger it fails loudly
// with ErrHeapImageTooLarge, never truncating.
// HeapProfileHigh64 / HeapProfileHigh256 are the opt-in
// HighCapacity profiles (64 / 256 MiB). They are wasm64
// ONLY: requesting one with a wasm32 binary fails with
// ErrHighCapacityRequiresWasm64 before the directory is
// created. On an existing directory with a smaller stored
// heap, a HighCapacity profile UPGRADES it (the smaller
// image extends exactly as the legacy migration does);
// on one with a larger stored heap it fails loudly.
// A heap size, once recorded in a checkpoint, is the DB's
// size until an explicit larger profile upgrades it —
// reopening with Default never shrinks it.
type HeapProfile int

const (
	HeapProfileDefault HeapProfile = iota
	HeapProfileStandard
	HeapProfileHigh64
	HeapProfileHigh256
)

// heapProfileSize maps a profile to its heap size in bytes.
// Default maps to 0 ("no explicit size": keep stored, else
// Standard); callers resolve 0 before use.
func heapProfileSize(p HeapProfile) (uint64, error) {
	switch p {
	case HeapProfileDefault:
		return 0, nil
	case HeapProfileStandard:
		return segDBHeapSize, nil
	case HeapProfileHigh64:
		return segDBHeapSizeHigh64, nil
	case HeapProfileHigh256:
		return segDBHeapSizeHigh256, nil
	default:
		return 0, fmt.Errorf("%w: heap profile %d", ErrInvalidHeapProfile, int(p))
	}
}

// ErrInvalidHeapProfile reports an unknown HeapProfile value.
var ErrInvalidHeapProfile = errors.New("yowza persist: invalid heap profile")

// PersistedOptions configures OpenPersistedSegDB.
type PersistedOptions struct {
	// HeapProfile selects the heap size (see HeapProfile).
	// The zero value is HeapProfileDefault.
	HeapProfile HeapProfile
	// NumShards is the shard count. 0 means 1. On an existing
	// directory it must match the stored count (or be 0).
	NumShards int
	// Durability selects the fsync mode. The zero value is
	// DurabilitySync.
	Durability DurabilityMode
	// KeepPreviousGenerationPolicy selects the Gate 2.2
	// retention regime; KeepPreviousGeneration (below) is
	// the legacy bool spelling. Resolved by resolvePolicy:
	// explicit Policy wins, else legacy true means on,
	// else the default applies — and the default is ON
	// (O1 decision: retain one valid previous generation
	// and enable heap-corruption fallback on open). To
	// request the old reclaim behaviour explicitly, set
	// PolicyOff here; a legacy false does NOT mean off.
	KeepPreviousGenerationPolicy Policy
	// MixedCheckpointPolicy selects the Gate 2.3 regime;
	// MixedCheckpoint (below) is the legacy bool spelling.
	// Resolved by resolvePolicy with default ON (O1
	// decision: a checkpoint reuses the previous immutable
	// heap file for a shard whose heap SHA is unchanged,
	// instead of writing a fresh 4 MiB image per shard
	// per checkpoint). To request full heap images every
	// checkpoint, set PolicyOff here.
	MixedCheckpointPolicy Policy
	// SpillChunkSize caps the records per version-log
	// chunk (Phase E spill history). 0 means the
	// default (4096). Smaller values rotate chunks
	// sooner (more manifest publishes, more files);
	// larger values keep longer chunks. It never caps
	// total history: chunks are unbounded in count.
	SpillChunkSize int
	// KeepPreviousGeneration (Gate 2.2, LEGACY — prefer
	// KeepPreviousGenerationPolicy) retains the previous
	// checkpoint — its descriptor (in checkpoint.json and
	// checkpoint.prev.json), its heap files, and the WAL
	// segments needed to replay from it — and enables
	// heap-corruption fallback on open. True still means
	// on; false means "unset" (the default, ON, applies),
	// NOT "off" — see resolvePolicy. Deprecated: new code
	// should use KeepPreviousGenerationPolicy.
	KeepPreviousGeneration bool
	// MixedCheckpoint (Gate 2.3, LEGACY — prefer
	// MixedCheckpointPolicy) lets a checkpoint reuse
	// the previous immutable heap file for a shard whose
	// heap SHA is unchanged, instead of writing a fresh
	// 4 MiB image per shard per checkpoint. Reused files
	// are re-read and checksum-verified before publish.
	// True still means on; false means "unset" (the
	// default, ON, applies), NOT "off" — see
	// resolvePolicy. Deprecated: new code should use
	// MixedCheckpointPolicy.
	MixedCheckpoint bool
	// WALRotateBytes is the current-segment size threshold
	// for automatic checkpoint + rotation after a Put/
	// Delete (Gate 2.1). 0 selects DefaultWALRotateBytes
	// (8 MiB); a negative value disables automatic
	// rotation (the current segment is then unbounded by
	// explicit operator choice). Automatic failures never
	// fail the mutation; see AutoCheckpointError.
	WALRotateBytes int64
}

// Fault-injection points for the wrapper's write / sync /
// rename / directory-sync stages. A hook returning a non-nil
// error simulates a failure at exactly that stage; the stage's
// normal effect up to that point has already happened (e.g. a
// "wal-sync" failure means the frame was written but not
// synced; a "heap-rename" failure means the tmp file was
// written and synced but not renamed).
const (
	persistFaultWALWrite          = "wal-write"
	persistFaultWALSync           = "wal-sync"
	persistFaultHeapWrite         = "heap-write"
	persistFaultHeapSync          = "heap-sync"
	persistFaultHeapRename        = "heap-rename"
	persistFaultHeapDirSync       = "heap-dir-sync"
	persistFaultCheckpointWrite   = "checkpoint-write"
	persistFaultCheckpointSync    = "checkpoint-sync"
	persistFaultCheckpointRename  = "checkpoint-rename"
	persistFaultCheckpointDirSync = "checkpoint-dir-sync"
	persistFaultOpenWALSync       = "open-wal-sync"
	persistFaultOpenDirSync       = "open-dir-sync"
	persistFaultRepairFileSync    = "repair-file-sync"
	persistFaultRepairDirSync     = "repair-dir-sync"
)

// persistFaultHook is nil in production. Tests set it to fail a
// single stage (see the header comment). It must be cleared
// after use; concurrent tests must not share it.
var persistFaultHook func(point string) error

func persistFault(point string) error {
	if persistFaultHook != nil {
		return persistFaultHook(point)
	}
	return nil
}

// Sentinel errors. CorruptWALError is returned as a value type
// (*CorruptWALError) so callers can inspect offset/seq.
var (
	ErrPersistedClosed          = errors.New("yowza persist: database closed")
	ErrPersistedPoisoned        = errors.New("yowza persist: database poisoned after WAL failure: outcome indeterminate, no further writes until recovery (close and reopen)")
	ErrCorruptHeap              = errors.New("yowza persist: heap image corrupt or missing for a checkpointed shard")
	ErrCorruptCheckpoint        = errors.New("yowza persist: checkpoint file corrupt")
	ErrMissingWAL               = errors.New("yowza persist: WAL file missing for a checkpointed database")
	ErrDurabilityUnavailable    = errors.New("yowza persist: requested durability mode unavailable (fsync failed)")
	ErrWasmVariantMismatch      = errors.New("yowza persist: directory was created with a different wasm variant (wasm32 vs wasm64)")
	ErrPersistedShardCount      = errors.New("yowza persist: shard count does not match the stored database")
	ErrPersistedWALGap          = errors.New("yowza persist: WAL sequence gap")
	ErrPersistedWALRecordTooBig = errors.New("yowza persist: WAL record too large")
	ErrInvalidDurabilityMode    = errors.New("yowza persist: invalid durability mode")
	ErrInvalidPolicy            = errors.New("yowza persist: invalid policy")
)

// IndeterminateWALError reports a WAL sequence whose outcome
// cannot be determined: the append failed, so the record may
// be absent, partial, or fully written and replayable despite
// the caller having been told the operation failed. Seq is the
// sequence involved; callers must NOT blindly retry the
// mutation in the same instance — the instance is poisoned
// (see ErrPersistedPoisoned) until close + reopen decides what
// was actually recorded.
type IndeterminateWALError struct {
	Seq    uint64 // WAL sequence whose outcome is indeterminate
	Op     string // "put", "delete", or "abort"
	Err    error  // underlying write/sync error
	Reason string // "write" or "sync"
}

func (e *IndeterminateWALError) Error() string {
	return fmt.Sprintf("yowza persist: WAL seq %d (%s) outcome indeterminate after %s failure: %v", e.Seq, e.Op, e.Reason, e.Err)
}

func (e *IndeterminateWALError) Unwrap() error { return e.Err }

// CorruptWALError reports a truncated or checksum-bad WAL record.
// The database is NOT opened when this is returned: the caller can
// inspect LastValidSeq, then either investigate or call
// RepairWALTail (truncate to Offset) and reopen, recovering the
// checkpoint state plus all valid records before the bad one.
type CorruptWALError struct {
	Offset       int64  // byte offset of the first bad record
	LastValidSeq uint64 // seq of the last fully valid record (0 if none)
	Reason       string
}

func (e *CorruptWALError) Error() string {
	return fmt.Sprintf("yowza persist: corrupt WAL at offset %d (last valid seq %d): %s", e.Offset, e.LastValidSeq, e.Reason)
}

// persistCheckpoint is the on-disk checkpoint.json content.
// Generation + HeapFiles are the generational protocol:
// Generation 0 with no HeapFiles is the legacy layout (heap
// files heap-<i>.bin, written by pre-generational code); any
// checkpoint written by generational code has Generation >= 1
// and HeapFiles naming heap-<gen>-<i>.bin per shard.
type persistCheckpoint struct {
	Magic         string   `json:"magic"`
	Version       int      `json:"version"`
	NumShards     int      `json:"num_shards"`
	CheckpointSeq uint64   `json:"checkpoint_seq"`
	HeapSHA256    []string `json:"heap_sha256"`
	Wasm64        bool     `json:"wasm64"`
	// HeapSize is this checkpoint's heap image size in
	// bytes (worker S, High-Capacity Profile track).
	// omitempty keeps old checkpoints parsing: a missing
	// value means the Standard 8 MiB heap (whose images
	// may still be the 4 MiB legacy size, accepted by
	// the open path). New checkpoints always record it,
	// so the loader never has to guess a DB's size, and
	// a checkpoint's heap files must match its own
	// recorded size (see persistValidateHeapFiles).
	HeapSize   uint64   `json:"heap_size,omitempty"`
	Generation uint64   `json:"generation,omitempty"`
	HeapFiles  []string `json:"heap_files,omitempty"`
	// Previous is the retained previous checkpoint
	// descriptor (Gate 2.2, KeepPreviousGeneration only):
	// exactly one generation deep (its own Previous is
	// always nil). WALSegments is the WAL segment list
	// at publish time (file, first seq, last seq then);
	// the live segment list is authoritative after later
	// appends. Both fields are omitempty, so checkpoints
	// written by older code still parse, and checkpoints
	// written here still parse under older code (which
	// ignores unknown fields).
	Previous    *persistCheckpoint      `json:"previous,omitempty"`
	WALSegments []persistWALSegmentMeta `json:"wal_segments,omitempty"`
}

const persistCheckpointMagic = "YWPSC1"

// persistShard is one wasm module + SegDB pair.
type persistShard struct {
	host *YowzaDB
	sdb  *SegDB
}

// PersistedSegDB is a host-backed, sharded SegDB.
type PersistedSegDB struct {
	mu        sync.Mutex
	dir       string
	mode      DurabilityMode
	wasmBytes []byte
	shards    []*persistShard
	wasm64    bool
	// heapSize is this DB's wasm heap region size in
	// bytes, resolved at open (stored checkpoint size,
	// upgraded by an explicit larger profile, or the
	// profile size on a fresh directory). It is the
	// loader's max: images larger than it are refused,
	// smaller ones extend to it. It is never shrunk by
	// a reopen: Default keeps it, and an explicit
	// smaller profile refuses instead (see
	// OpenPersistedSegDB).
	heapSize      uint64
	seq           uint64 // last WAL seq issued (incl. ABORTs)
	checkpointSeq uint64
	heapSHA       []string
	generation    uint64   // generation of the loaded/last-published checkpoint (0 = legacy or none)
	heapFiles     []string // heap file basenames named by the current checkpoint (nil before first checkpoint)
	walFile       *os.File
	lockFile      *os.File // exclusive flock on dir/LOCK, held until Close
	closed        bool
	// Gate 2 lifecycle state (see segdb_persist_lifecycle.go).
	keepPrev       bool
	mixed          bool
	walRotateBytes int64
	walSegs        []*persistWALSegment // sorted by firstSeq; last is the append target
	walSegSize     int64                // bytes in the current segment
	shardDirty     []bool               // set before a shard is mutated, cleared only on definite publish
	curCkpt        *persistCheckpoint   // authoritative in-memory checkpoint (current, or fallback on fallback opens)
	prevCkpt       *persistCheckpoint   // retained previous descriptor, if any
	fallback       bool
	fallbackGen    uint64
	fallbackReason string
	autoErr        error
	// Background publish (gate 6): publishCh is a
	// capacity-1 channel mutex serializing publishes
	// (see segdb_persist.go checkpoint section);
	// publishPending records a snapshot dropped
	// because a publish was already in flight. Both
	// are set up in OpenPersistedSegDB (the only
	// construction site).
	publishCh      chan struct{}
	publishPending bool
	// Poisoned state (see header comment). Set only by a WAL
	// append failure; cleared only by discarding the instance
	// (close + reopen). While poisoned, Put/Delete/Flush/
	// Compact refuse; reads (Get) and Close remain allowed.
	poisoned  bool
	poisonErr error
	poisonSeq uint64
	// PATCH2 Track 1: per-DB recovery-gate state.
	// gate is the LifecycleGate attached at open (nil only
	// before open completes; see lifecycle.go). Every
	// public read path (Get, Scan, ScanStreaming,
	// AcquireSnapshot, SnapshotVector, ScanOpen, plus
	// TimelineManager.Get/Scan) consults it via
	// checkReadsAllowed before reading.
	// txPartialApply is set ONLY by commitGate's
	// rollback-failed path, where a partial prefix may
	// actually be applied; it is cleared only by
	// RecoverSegDBTransactions success. The other two
	// indeterminate paths (dir-sync failed after rename:
	// nothing applied; rollback succeeded but marking
	// aborted failed: old state restored) provably leave
	// the old state, so they do NOT set it — and an
	// "applying" journal alone is NOT a gate-pending
	// item either, because it cannot distinguish those
	// cases from a crash mid-apply (see the notes for
	// the documented residual gap and its fix).
	gate           *LifecycleGate
	txPartialApply bool
	// Phase B (Timeline-v2) state. pins is the durable-pin
	// set from pins.json (see segdb_pins.go); its leases
	// are re-registered on open before any Compact can
	// run. scanSessions/scanPolicy are the O3 streaming
	// scan sessions (see segdb_scan_session.go), guarded
	// by scanMu (never p.mu, so a session page never
	// blocks unrelated DB work on p.mu).
	snapshotNext uint64
	pinNext      uint64
	pins         map[string]*durablePinState
	// pinRecovery is the open-time pin recovery report
	// (see segdb_pins.go); set by recoverDurablePins
	// during Open, read via PinRecoveryReport().
	pinRecovery  PinRecoveryReport
	scanMu       sync.Mutex
	scanSessions map[uint64]*scanSession
	scanNextID   uint64
	scanPolicy   ScanPolicy
	// spill is the Phase E spillable version log
	// (spill_history.go): every facade mutation records
	// its pre-image here before applying, so history is
	// not capped at the C hot table's 1024 entries and
	// survives restart. Nil only before open completes.
	spill *spillHistory
	// epochCounts / epochHistEst are the Phase E
	// flush-epoch guard (epoch_guard.go): per-shard
	// mutation counts in the current epoch and the
	// conservative history-entry estimate. Reset by
	// every successful flushLocked.
	epochCounts  []map[string]int
	epochHistEst []int
}

// persistWALRecord is one parsed WAL record.
type persistWALRecord struct {
	seq        uint64
	op         byte
	key        []byte
	val        []byte
	abortedSeq uint64 // for op == persistOpAbort
}

// persistEncodeRecord builds one WAL frame for seq/op/key/val.
func persistEncodeRecord(seq uint64, op byte, key []byte, val []byte) []byte {
	payload := make([]byte, 0, 13+len(key)+4+len(val))
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], seq)
	payload = append(payload, tmp[:]...)
	payload = append(payload, op)
	var tmp4 [4]byte
	binary.LittleEndian.PutUint32(tmp4[:], uint32(len(key)))
	payload = append(payload, tmp4[:]...)
	payload = append(payload, key...)
	binary.LittleEndian.PutUint32(tmp4[:], uint32(len(val)))
	payload = append(payload, tmp4[:]...)
	payload = append(payload, val...)
	h := fnv.New64a()
	h.Write(payload)
	frame := make([]byte, 0, 4+len(payload)+8)
	binary.LittleEndian.PutUint32(tmp4[:], uint32(len(payload)))
	frame = append(frame, tmp4[:]...)
	frame = append(frame, payload...)
	binary.LittleEndian.PutUint64(tmp[:], h.Sum64())
	frame = append(frame, tmp[:]...)
	return frame
}

// persistParseWAL parses and fully validates a WAL byte slice. On
// success it returns all records in order. On the first bad record
// it returns a *CorruptWALError plus the valid prefix.
func persistParseWAL(data []byte) ([]persistWALRecord, error) {
	return persistParseWALSegmented(data, 1)
}

// persistParseWALSegmented is persistParseWAL with a configurable
// starting sequence: a rotated WAL segment's first record must
// equal its segment's first sequence, not 1 (Gate 2.1).
func persistParseWALSegmented(data []byte, startSeq uint64) ([]persistWALRecord, error) {
	var records []persistWALRecord
	expectSeq := startSeq
	off := 0
	for off < len(data) {
		recOff := off
		if len(data)-off < 4 {
			return records, &CorruptWALError{Offset: int64(recOff), LastValidSeq: expectSeq - 1, Reason: "truncated record header"}
		}
		frameLen := int(binary.LittleEndian.Uint32(data[off:]))
		off += 4
		if frameLen <= 0 || frameLen > persistWALMaxPayload {
			return records, &CorruptWALError{Offset: int64(recOff), LastValidSeq: expectSeq - 1, Reason: fmt.Sprintf("implausible record length %d", frameLen)}
		}
		if len(data)-off < frameLen+8 {
			return records, &CorruptWALError{Offset: int64(recOff), LastValidSeq: expectSeq - 1, Reason: "truncated record body"}
		}
		payload := data[off : off+frameLen]
		storedCRC := binary.LittleEndian.Uint64(data[off+frameLen:])
		h := fnv.New64a()
		h.Write(payload)
		if h.Sum64() != storedCRC {
			return records, &CorruptWALError{Offset: int64(recOff), LastValidSeq: expectSeq - 1, Reason: "checksum mismatch"}
		}
		off += frameLen + 8
		if len(payload) < 13 {
			return records, &CorruptWALError{Offset: int64(recOff), LastValidSeq: expectSeq - 1, Reason: "payload too short"}
		}
		seq := binary.LittleEndian.Uint64(payload[0:])
		op := payload[8]
		if seq != expectSeq {
			return records, &CorruptWALError{Offset: int64(recOff), LastValidSeq: expectSeq - 1, Reason: fmt.Sprintf("sequence gap: got %d, want %d", seq, expectSeq)}
		}
		keyLen := int(binary.LittleEndian.Uint32(payload[9:]))
		if keyLen < 0 || 13+keyLen+4 > len(payload) {
			return records, &CorruptWALError{Offset: int64(recOff), LastValidSeq: expectSeq - 1, Reason: "key length out of bounds"}
		}
		key := payload[13 : 13+keyLen]
		rest := payload[13+keyLen:]
		valLen := int(binary.LittleEndian.Uint32(rest[:]))
		if valLen < 0 || 4+valLen != len(rest) {
			return records, &CorruptWALError{Offset: int64(recOff), LastValidSeq: expectSeq - 1, Reason: "value length out of bounds"}
		}
		val := rest[4:]
		rec := persistWALRecord{seq: seq, op: op}
		switch op {
		case persistOpPut:
			if len(key) == 0 || len(key) > 1023 || len(val) > 1024 {
				return records, &CorruptWALError{Offset: int64(recOff), LastValidSeq: expectSeq - 1, Reason: "put key/value length out of range"}
			}
			rec.key = append([]byte(nil), key...)
			rec.val = append([]byte(nil), val...)
		case persistOpDel:
			if len(key) == 0 || len(key) > 1023 || len(val) != 0 {
				return records, &CorruptWALError{Offset: int64(recOff), LastValidSeq: expectSeq - 1, Reason: "delete record malformed"}
			}
			rec.key = append([]byte(nil), key...)
		case persistOpAbort:
			if len(key) != 0 || len(val) != 8 {
				return records, &CorruptWALError{Offset: int64(recOff), LastValidSeq: expectSeq - 1, Reason: "abort record malformed"}
			}
			rec.abortedSeq = binary.LittleEndian.Uint64(val)
			if rec.abortedSeq == 0 || rec.abortedSeq >= seq {
				return records, &CorruptWALError{Offset: int64(recOff), LastValidSeq: expectSeq - 1, Reason: "abort references invalid seq"}
			}
		default:
			return records, &CorruptWALError{Offset: int64(recOff), LastValidSeq: expectSeq - 1, Reason: fmt.Sprintf("unknown op %d", op)}
		}
		records = append(records, rec)
		expectSeq++
	}
	return records, nil
}

// persistSyncDir fsyncs a directory (best-effort on platforms
// where opening a directory fails; on Linux it must succeed in
// Sync mode — callers treat an error as fatal).
func persistSyncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

// errCheckpointDirSyncAfterRename marks a checkpoint publish
// whose rename already happened but whose directory sync then
// failed. Only checkpointLocked interprets it (see there); for
// every other caller it is just an error.
var errCheckpointDirSyncAfterRename = errors.New("yowza persist: checkpoint renamed but directory sync failed")

// persistWriteAtomic writes data to dir/name via tmp + fsync +
// rename + dir fsync (the fsync steps only in Sync mode).
// stage selects the fault-injection points ("heap" or
// "checkpoint"): "<stage>-write" is consulted before writing,
// "<stage>-sync" after writing (file synced in Sync mode, or
// after the write in NoSync mode — the hook still models a
// failure at that stage), "<stage>-rename" after file sync +
// close but before rename, "<stage>-dir-sync" after rename.
// A hook error aborts before the next stage; in particular a
// heap-file failure never touches any checkpointed file.
func persistWriteAtomic(dir, name string, data []byte, mode DurabilityMode, stage string) error {
	tmpPath := filepath.Join(dir, name+".tmp")
	finalPath := filepath.Join(dir, name)
	if err := persistFault(stage + "-write"); err != nil {
		return err
	}
	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if mode == DurabilitySync {
		if err := f.Sync(); err != nil {
			f.Close()
			return fmt.Errorf("%w: heap/checkpoint file sync: %v", ErrDurabilityUnavailable, err)
		}
	}
	if err := persistFault(stage + "-sync"); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := persistFault(stage + "-rename"); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		return err
	}
	if mode == DurabilitySync {
		if err := persistSyncDir(dir); err != nil {
			if stage == "checkpoint" {
				return fmt.Errorf("%w: %w: directory sync after rename: %v", errCheckpointDirSyncAfterRename, ErrDurabilityUnavailable, err)
			}
			return fmt.Errorf("%w: directory sync after rename: %v", ErrDurabilityUnavailable, err)
		}
	}
	if err := persistFault(stage + "-dir-sync"); err != nil {
		if stage == "checkpoint" {
			return fmt.Errorf("%w: %w", errCheckpointDirSyncAfterRename, err)
		}
		return err
	}
	return nil
}

// persistWriteHeapAtomic is persistWriteAtomic for heap
// images, with ONE difference: the disk representation.
// A heap image is mostly zeros (table slack, unused bump
// room), so writing it densely would charge a 256 MiB
// heap 256 MiB of disk per checkpoint. Instead this
// writer scans in 64 KiB chunks and SEEKs over all-zero
// chunks, leaving filesystem holes; a final Truncate to
// the full length sets the apparent size, so readers
// (os.ReadFile, SHA checks, the loader) see exactly the
// same bytes as a dense write — holes read back as
// zeros — while allocated blocks track only live bytes.
// Everything else is identical to persistWriteAtomic:
// tmp file + fsync (Sync mode) + rename + dir fsync,
// the same "heap-*" fault-injection stages in the same
// order, so a crash during a heap write can never
// present a partial file as a valid image: the final
// name appears only via the atomic rename, and the
// checkpoint that would reference it publishes only
// after the file is re-read and SHA-validated.
// Sparse-file note: holes are a filesystem property,
// not a format property. On a filesystem without sparse
// support the file simply costs its full apparent size;
// correctness never depends on sparseness (the bytes
// are identical either way), only the disk saving does.
// Trimmed persistence (store only a used prefix) was
// rejected: it would change the checkpoint format, the
// validation rule, and the mixed-reuse rule, and a
// truncated file would be indistinguishable on disk
// from a torn full image without extra metadata.
func persistWriteHeapAtomic(dir, name string, data []byte, mode DurabilityMode) error {
	tmpPath := filepath.Join(dir, name+".tmp")
	finalPath := filepath.Join(dir, name)
	if err := persistFault("heap-write"); err != nil {
		return err
	}
	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	const chunk = 64 << 10
	for off := 0; off < len(data); {
		end := off + chunk
		if end > len(data) {
			end = len(data)
		}
		zero := true
		for _, b := range data[off:end] {
			if b != 0 {
				zero = false
				break
			}
		}
		if zero {
			if _, err := f.Seek(int64(end-off), io.SeekCurrent); err != nil {
				f.Close()
				return err
			}
		} else {
			if _, err := f.Write(data[off:end]); err != nil {
				f.Close()
				return err
			}
		}
		off = end
	}
	// Trailing holes (and the all-zero image) leave the
	// file short until truncated: the apparent size must
	// be exactly len(data) for readers and SHA checks.
	if err := f.Truncate(int64(len(data))); err != nil {
		f.Close()
		return err
	}
	if mode == DurabilitySync {
		if err := f.Sync(); err != nil {
			f.Close()
			return fmt.Errorf("%w: heap/checkpoint file sync: %v", ErrDurabilityUnavailable, err)
		}
	}
	if err := persistFault("heap-sync"); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := persistFault("heap-rename"); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		return err
	}
	if mode == DurabilitySync {
		if err := persistSyncDir(dir); err != nil {
			return fmt.Errorf("%w: directory sync after rename: %v", ErrDurabilityUnavailable, err)
		}
	}
	if err := persistFault("heap-dir-sync"); err != nil {
		return err
	}
	return nil
}

// persistCheckpointHeapFileName is the generational heap file
// basename for generation gen, shard i.
func persistCheckpointHeapFileName(gen uint64, i int) string {
	return fmt.Sprintf("heap-%d-%d.bin", gen, i)
}

// persistLegacyHeapFileName is the pre-generational heap file
// basename for shard i.
func persistLegacyHeapFileName(i int) string {
	return fmt.Sprintf("heap-%d.bin", i)
}

func (p *PersistedSegDB) heapPath(i int) string {
	if p.heapFiles != nil && i >= 0 && i < len(p.heapFiles) {
		return filepath.Join(p.dir, p.heapFiles[i])
	}
	return filepath.Join(p.dir, persistLegacyHeapFileName(i))
}

// poisonLocked records a WAL append failure (caller holds
// p.mu) and returns the indeterminate error for that sequence.
func (p *PersistedSegDB) poisonLocked(seq uint64, op, reason string, err error) error {
	ind := &IndeterminateWALError{Seq: seq, Op: op, Err: err, Reason: reason}
	p.poisoned = true
	p.poisonSeq = seq
	p.poisonErr = ind
	return ind
}

// checkWritableLocked refuses normal writes while poisoned
// (caller holds p.mu). Checkpoint failures do not poison; only
// WAL append failures do.
func (p *PersistedSegDB) checkWritableLocked() error {
	if p.poisoned {
		return fmt.Errorf("%w: poisoned at WAL seq %d: %v", ErrPersistedPoisoned, p.poisonSeq, p.poisonErr)
	}
	return nil
}

// Poisoned reports the poison state: nil when usable, otherwise
// the IndeterminateWALError that poisoned the instance. Recovery
// is Close + reopen (plus RepairWALTail + reopen if the reopen
// reports a corrupt tail).
func (p *PersistedSegDB) Poisoned() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.poisoned {
		return nil
	}
	return p.poisonErr
}

// PoisonSeq returns the WAL sequence whose indeterminate outcome
// poisoned the instance, or 0 when not poisoned.
func (p *PersistedSegDB) PoisonSeq() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.poisonSeq
}

// HeapSize returns this DB's wasm heap region size in bytes
// (Standard 8 MiB, or a HighCapacity size).
func (p *PersistedSegDB) HeapSize() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.heapSize
}

// HeapInfo returns shard 0's C heap usage snapshot (see
// SegDB.HeapInfo). Benchmarks in this track use one
// shard; multi-shard callers should read per-shard info
// through their own shard access, since this facade
// method reports shard 0 only.
func (p *PersistedSegDB) HeapInfo() (SegDBHeapInfo, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || len(p.shards) == 0 {
		return SegDBHeapInfo{}, ErrPersistedClosed
	}
	return p.shards[0].sdb.HeapInfo()
}

// Generation returns the generation of the current checkpoint
// (0 = legacy checkpoint or none yet). It drains any
// in-flight background publish first, so it always
// reports post-publish state.
func (p *PersistedSegDB) Generation() uint64 {
	p.drainPublish()
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.generation
}

func (p *PersistedSegDB) walPath() string {
	return filepath.Join(p.dir, "wal.log")
}

func (p *PersistedSegDB) checkpointPath() string {
	return filepath.Join(p.dir, "checkpoint.json")
}

// WALPath returns the CURRENT WAL segment path (for tests /
// operators): wal.log before the first rotation, the
// wal-<firstSeq>.log segment afterwards. Older segments
// are listed by WALSegments.
func (p *PersistedSegDB) WALPath() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.walPathLocked()
}

// CheckpointPath returns the checkpoint file path.
func (p *PersistedSegDB) CheckpointPath() string { return p.checkpointPath() }

// HeapPath returns the heap image path for shard i: the file
// named by the current checkpoint (generational, or legacy
// heap-<i>.bin for a pre-generational checkpoint). Before the
// first checkpoint it returns the legacy name, which does not
// exist yet. It drains any in-flight background publish
// first, so it always names the published checkpoint's
// file (and never races the advance's heapFiles swap).
func (p *PersistedSegDB) HeapPath(i int) string {
	p.drainPublish()
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.heapPath(i)
}

// Dir returns the host directory.
func (p *PersistedSegDB) Dir() string { return p.dir }

// NumShards returns the shard count.
func (p *PersistedSegDB) NumShards() int { return len(p.shards) }

// Durability returns the configured durability mode.
func (p *PersistedSegDB) Durability() DurabilityMode { return p.mode }

// LastSeq returns the last WAL seq issued.
func (p *PersistedSegDB) LastSeq() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.seq
}

// CheckpointSeq returns the seq covered by the last checkpoint.
// It drains any in-flight background publish first, like
// Generation.
func (p *PersistedSegDB) CheckpointSeq() uint64 {
	p.drainPublish()
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.checkpointSeq
}

func (p *PersistedSegDB) shardFor(key string) *persistShard {
	h := fnv.New64a()
	h.Write([]byte(key))
	return p.shards[h.Sum64()%uint64(len(p.shards))]
}

func (p *PersistedSegDB) closeLocked() {
	// Phase E: publish the spill log's open chunk
	// before the WAL file closes (best effort on
	// failure paths; nil before open completes).
	if p.spill != nil {
		_ = p.spill.closeLocked()
	}
	if p.walFile != nil {
		p.walFile.Close()
		p.walFile = nil
	}
	for _, sh := range p.shards {
		if sh.host != nil {
			sh.host.Close()
		}
	}
	// Releasing the lock file releases the flock (the
	// explicit unlock keeps the intent readable). This
	// runs on Close and on every open-failure path, so a
	// failed open never leaves the directory locked.
	persistUnlockDir(p.lockFile)
	p.lockFile = nil
	p.closed = true
}

// OpenPersistedSegDB opens (or creates) a host-backed SegDB in dir.
// wasmBytes is the wasm binary (wasm32 or wasm64) each shard runs.
// A directory created with one wasm variant cannot be opened with
// the other: heap images are treated as variant-bound here (the
// cross-variant portability of the heap object format is not
// proven by this stage, so it is refused explicitly, not assumed).
func OpenPersistedSegDB(dir string, wasmBytes []byte, opts PersistedOptions) (*PersistedSegDB, error) {
	// Durability validation FIRST, before creating anything:
	// any value other than the two defined modes is rejected,
	// never silently treated as NoSync.
	if opts.Durability != DurabilitySync && opts.Durability != DurabilityNoSync {
		return nil, fmt.Errorf("%w: %d", ErrInvalidDurabilityMode, int(opts.Durability))
	}
	// Policy validation, next to the durability validation and
	// BEFORE creating the directory or taking the directory
	// lock: only PolicyDefault, PolicyOn, and PolicyOff are
	// accepted. Any other value (e.g. Policy(42) or
	// Policy(-1)) is rejected with ErrInvalidPolicy instead
	// of silently falling through to the default branch of
	// resolvePolicy, which would treat it like
	// PolicyDefault. This validation touches nothing: no
	// directory is created and no lock is taken on error.
	// It does not change resolvePolicy's behaviour for the
	// three valid values, the defaults (both ON), or the
	// precedence rule (explicit Policy beats legacy bool;
	// legacy true = on; legacy false = unset -> default ON).
	if opts.KeepPreviousGenerationPolicy != PolicyDefault && opts.KeepPreviousGenerationPolicy != PolicyOn && opts.KeepPreviousGenerationPolicy != PolicyOff {
		return nil, fmt.Errorf("%w: KeepPreviousGenerationPolicy %d", ErrInvalidPolicy, int(opts.KeepPreviousGenerationPolicy))
	}
	if opts.MixedCheckpointPolicy != PolicyDefault && opts.MixedCheckpointPolicy != PolicyOn && opts.MixedCheckpointPolicy != PolicyOff {
		return nil, fmt.Errorf("%w: MixedCheckpointPolicy %d", ErrInvalidPolicy, int(opts.MixedCheckpointPolicy))
	}
	// Heap profile validation, also before creating
	// anything: an unknown profile is rejected, and a
	// HighCapacity profile with a wasm32 binary is
	// refused loudly here (ErrHighCapacityRequires-
	// Wasm64), never served with a silent 8 MiB heap.
	// wasmIsMemory64 parses the binary's memory
	// section, so this needs no module instantiation.
	requestedHeapSize, err := heapProfileSize(opts.HeapProfile)
	if err != nil {
		return nil, err
	}
	if requestedHeapSize != 0 && requestedHeapSize != segDBHeapSize && !wasmIsMemory64(wasmBytes) {
		return nil, fmt.Errorf("%w: heap profile %d (%d bytes) with a wasm32 binary", ErrHighCapacityRequiresWasm64, int(opts.HeapProfile), requestedHeapSize)
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	p := &PersistedSegDB{dir: dir, mode: opts.Durability, wasmBytes: wasmBytes,
		keepPrev:       resolvePolicy(opts.KeepPreviousGenerationPolicy, opts.KeepPreviousGeneration, true),
		mixed:          resolvePolicy(opts.MixedCheckpointPolicy, opts.MixedCheckpoint, true),
		walRotateBytes: opts.WALRotateBytes,
		publishCh:      make(chan struct{}, 1)}
	// --- single-opener lock (Gate 2.4a) ---
	// Taken BEFORE any read or write: a second opener gets
	// ErrDatabaseLocked and touches nothing. Released by
	// closeLocked (Close and every open-failure path).
	lockFile, err := persistLockDir(dir)
	if err != nil {
		return nil, err
	}
	p.lockFile = lockFile

	// --- checkpoint (commit point) ---
	// A checkpoint.json that cannot be read or validated
	// falls back to checkpoint.prev.json (Gate 2.2) when
	// that file parses: the fallback descriptor is
	// reported, never hidden. Without a usable fallback,
	// the original checkpoint error is returned.
	var ckpt *persistCheckpoint
	ckpt, ckptErr := persistReadCheckpointFile(p.checkpointPath())
	if ckptErr != nil && !os.IsNotExist(ckptErr) {
		prev, prevErr := persistReadCheckpointFile(filepath.Join(dir, "checkpoint.prev.json"))
		if prevErr == nil {
			ckpt = prev
			p.fallback = true
			p.fallbackReason = fmt.Sprintf("checkpoint.json unusable (%v); recovered from checkpoint.prev.json generation %d", ckptErr, prev.Generation)
		} else {
			// Open-failure path: release the directory
			// lock like every other post-lock failure
			// path, so a later valid open is not
			// spuriously refused with ErrDatabaseLocked.
			p.closeLocked()
			return nil, ckptErr
		}
	} else if ckptErr != nil {
		// No checkpoint.json yet: fresh database. (A
		// checkpoint.prev.json without a checkpoint.json
		// is an orphan from an interrupted first publish
		// and is ignored.)
		ckpt = nil
	}

	numShards := opts.NumShards
	if numShards == 0 {
		if ckpt != nil {
			numShards = ckpt.NumShards
		} else {
			numShards = 1
		}
	}
	if numShards <= 0 {
		p.closeLocked()
		return nil, fmt.Errorf("yowza persist: shard count %d out of range", numShards)
	}
	if ckpt != nil && numShards != ckpt.NumShards {
		p.closeLocked()
		return nil, fmt.Errorf("%w: want %d, stored %d", ErrPersistedShardCount, numShards, ckpt.NumShards)
	}

	// --- heap size resolution (High-Capacity track) ---
	// stored is the size this directory's current
	// checkpoint records (Standard when unrecorded, as
	// in all pre-track checkpoints). Rules, all loud:
	// Default keeps stored (fresh: Standard); an
	// explicit profile equal to stored is a no-op; a
	// larger explicit profile UPGRADES (the smaller
	// stored image extends at restore, exactly as the
	// legacy migration does); a smaller explicit
	// profile REFUSES — a larger image is never
	// silently truncated to fit a smaller loader.
	// The effective size is the loader's max for the
	// restore loop below; on a fallback open it still
	// holds (a smaller fallback image extends to it,
	// a larger one is refused there too).
	storedHeapSize := segDBHeapSize
	if ckpt != nil && ckpt.HeapSize != 0 {
		storedHeapSize = ckpt.HeapSize
	}
	if storedHeapSize != segDBHeapSize && storedHeapSize != segDBHeapSizeHigh64 && storedHeapSize != segDBHeapSizeHigh256 {
		p.closeLocked()
		return nil, fmt.Errorf("%w: stored heap size %d is not a supported size", ErrCorruptCheckpoint, storedHeapSize)
	}
	switch {
	case requestedHeapSize == 0:
		p.heapSize = storedHeapSize
	case requestedHeapSize == storedHeapSize:
		p.heapSize = storedHeapSize
	case requestedHeapSize > storedHeapSize:
		p.heapSize = requestedHeapSize
	default:
		p.closeLocked()
		return nil, fmt.Errorf("%w: stored heap size %d exceeds requested profile heap size %d (refusing to truncate)", ErrHeapImageTooLarge, storedHeapSize, requestedHeapSize)
	}
	// (For a fresh directory storedHeapSize is already
	// Standard, so Default above yields Standard and an
	// explicit profile above yields its own size.)

	// --- authoritative checkpoint + heap fallback (Gate 2.2) ---
	// auth is the descriptor whose heaps will be restored.
	// When KeepPreviousGeneration is set and the current
	// descriptor's heaps fail validation, auth becomes its
	// Previous descriptor (validated too); a failure of
	// both returns the ORIGINAL heap error. Without
	// KeepPreviousGeneration there is no fallback attempt,
	// preserving the old ErrCorruptHeap behaviour.
	auth := ckpt
	var authPrev *persistCheckpoint // retained previous descriptor for a normal (non-fallback) open
	if ckpt != nil {
		if _, herr := persistValidateHeapFiles(dir, ckpt); herr != nil {
			if p.keepPrev && ckpt.Previous != nil && !p.fallback {
				if _, perr := persistValidateHeapFiles(dir, ckpt.Previous); perr == nil {
					if ckpt.Previous.NumShards == numShards {
						p.fallback = true
						p.fallbackGen = ckpt.Previous.Generation
						p.fallbackReason = fmt.Sprintf("current generation %d heap invalid (%v); recovered from previous generation %d", ckpt.Generation, herr, ckpt.Previous.Generation)
						auth = ckpt.Previous
					}
				}
			}
			if auth == ckpt {
				p.closeLocked()
				return nil, herr
			}
		}
		if auth == ckpt && ckpt.Previous != nil && p.keepPrev {
			authPrev = persistCheckpointCopy(ckpt.Previous)
		}
		// Resolve the authoritative checkpoint's heap files
		// BEFORE restoring shards: generational names when
		// present, derived generational names when only a
		// generation is stored, legacy names for
		// pre-generational checkpoints.
		p.generation = auth.Generation
		p.heapFiles = persistCheckpointHeapFiles(auth)
		p.curCkpt = persistCheckpointCopy(auth)
		if auth == ckpt {
			p.curCkpt.Previous = authPrev
		} else {
			// Fallback open: the recovered descriptor IS
			// the authoritative checkpoint; there is no
			// valid older generation retained behind it.
			p.curCkpt.Previous = nil
		}
		p.prevCkpt = authPrev
		if p.fallback {
			p.fallbackGen = auth.Generation
		}
	}
	p.shardDirty = make([]bool, numShards)
	for i := range p.shardDirty {
		p.shardDirty[i] = true // unknown after open: SHA comparison decides reuse
	}

	// --- shards: restore heaps, recover ---
	for i := 0; i < numShards; i++ {
		host, err := NewYowzaDB(wasmBytes)
		if err != nil {
			p.closeLocked()
			return nil, err
		}
		sh := &persistShard{host: host}
		p.shards = append(p.shards, sh)
		if i == 0 {
			p.wasm64 = host.isMem64
		}
		if auth != nil && p.wasm64 != auth.Wasm64 {
			p.closeLocked()
			return nil, fmt.Errorf("%w: stored wasm64=%v, binary wasm64=%v", ErrWasmVariantMismatch, auth.Wasm64, p.wasm64)
		}
		// Belt-and-braces for a crafted checkpoint whose
		// Wasm64 flag agrees with a wasm32 binary while
		// its recorded heap is HighCapacity: the size
		// itself refuses, independent of the flag.
		if p.heapSize != segDBHeapSize && !p.wasm64 {
			p.closeLocked()
			return nil, fmt.Errorf("%w: heap size %d on a wasm32 module", ErrHighCapacityRequiresWasm64, p.heapSize)
		}
		if auth != nil {
			heap, err := os.ReadFile(p.heapPath(i))
			if err != nil {
				p.closeLocked()
				return nil, fmt.Errorf("%w: shard %d: %v", ErrCorruptHeap, i, err)
			}
			// Loader rule (generalized from the Track
			// C heap growth fix): a heap image up to
			// the loader's own heap size is accepted —
			// a legacy 4 MiB image, an 8 MiB current
			// image, or any smaller image into a larger
			// loader — with its checkpoint SHA-256
			// verified over the STORED bytes first,
			// then zero-extended to the loader size
			// before restore; wasm init migrates a
			// legacy table in place. An image LARGER
			// than the loader is refused loudly with
			// ErrHeapImageTooLarge (also wrapped as
			// ErrCorruptHeap, since for this loader it
			// is not a valid image), never truncated.
			if uint64(len(heap)) > p.heapSize {
				p.closeLocked()
				return nil, fmt.Errorf("%w: shard %d size %d exceeds loader heap size %d: %w", ErrCorruptHeap, i, len(heap), p.heapSize, ErrHeapImageTooLarge)
			}
			sum := sha256.Sum256(heap)
			if hex.EncodeToString(sum[:]) != auth.HeapSHA256[i] {
				p.closeLocked()
				return nil, fmt.Errorf("%w: shard %d checksum mismatch", ErrCorruptHeap, i)
			}
			heap, err = extendHeapImageToSize(heap, p.heapSize)
			if err != nil {
				p.closeLocked()
				return nil, fmt.Errorf("%w: shard %d: %v", ErrCorruptHeap, i, err)
			}
			if err := host.RestoreSegDBHeapToSize(heap, p.heapSize); err != nil {
				p.closeLocked()
				return nil, err
			}
		}
		sdb, err := NewSegDBWithHeapSize(host, p.heapSize)
		if err != nil {
			p.closeLocked()
			return nil, err
		}
		sh.sdb = sdb
		if auth != nil {
			if err := sdb.Recover(); err != nil {
				p.closeLocked()
				return nil, fmt.Errorf("yowza persist: shard %d recover: %v", i, err)
			}
			if ind, err := sdb.Indeterminate(); err != nil {
				p.closeLocked()
				return nil, err
			} else if ind {
				// The wasm heap store never returns indeterminate
				// (no post-rename sync step), but if it ever did,
				// reconcile once; a still-indeterminate DB refuses
				// writes, so opening it would be misleading.
				if err := sdb.Reconcile(); err != nil {
					p.closeLocked()
					return nil, fmt.Errorf("yowza persist: shard %d reconcile: %v", i, err)
				}
				if ind2, err := sdb.Indeterminate(); err != nil {
					p.closeLocked()
					return nil, err
				} else if ind2 {
					p.closeLocked()
					return nil, fmt.Errorf("yowza persist: shard %d still indeterminate after reconcile", i)
				}
			}
		}
	}
	if auth != nil {
		p.checkpointSeq = auth.CheckpointSeq
		p.heapSHA = append([]string(nil), auth.HeapSHA256...)
		p.seq = auth.CheckpointSeq
	} else {
		p.heapSHA = make([]string, numShards)
	}

	// --- WAL segments: scan, validate coverage, replay ---
	// Segments are scanned from the directory itself (Gate
	// 2.1); a segment whose contents do not match its name
	// is a hard error. A checkpointed database with no WAL
	// segments at all fails with ErrMissingWAL (the old
	// wal.log-missing rule, generalized to segments).
	segs, serr := persistScanWALSegments(dir)
	if serr != nil {
		p.closeLocked()
		return nil, serr
	}
	if auth != nil && len(segs) == 0 {
		p.closeLocked()
		return nil, fmt.Errorf("%w: no WAL segment in %s", ErrMissingWAL, dir)
	}
	p.walSegs = segs
	// Coverage: replay from the authoritative checkpoint
	// needs records > checkpointSeq; when a previous
	// checkpoint is retained, replay from THAT checkpoint
	// must also be possible (fallback soundness), so the
	// floor is the previous checkpoint's sequence. The
	// walk's target is the live WAL head (max segment
	// lastSeq): a WAL shorter than the checkpoint — e.g.
	// after an authorised repair truncation — is NOT an
	// error, because the heap already covers those
	// records (the pre-Gate-2 tolerance, preserved). The
	// one exception is a FALLBACK open: the corrupt
	// generation's heap cannot supply its range, so
	// replay from the fallback checkpoint must reach the
	// corrupt generation's checkpoint sequence, and a
	// deleted segment in that range is a typed gap
	// error, never a silently older state.
	coverFrom := p.checkpointSeq
	if p.keepPrev && p.prevCkpt != nil && p.prevCkpt.CheckpointSeq < coverFrom {
		coverFrom = p.prevCkpt.CheckpointSeq
	}
	var latestSeq uint64
	for _, s := range segs {
		if s.lastSeq > latestSeq {
			latestSeq = s.lastSeq
		}
	}
	if p.fallback && ckpt != nil && ckpt.CheckpointSeq > latestSeq {
		latestSeq = ckpt.CheckpointSeq
	}
	if err := persistValidateCoverage(segs, coverFrom, latestSeq); err != nil {
		p.closeLocked()
		return nil, err
	}
	// Append handle on the last segment (or a fresh legacy
	// segment when the directory has none yet).
	var walTarget string
	if len(segs) == 0 {
		walTarget = filepath.Join(dir, "wal.log")
		segs = []*persistWALSegment{{file: "wal.log", firstSeq: 1, lastSeq: 0, size: 0}}
		p.walSegs = segs
	} else {
		walTarget = filepath.Join(dir, segs[len(segs)-1].file)
		p.walSegSize = segs[len(segs)-1].size
	}
	walFile, err := os.OpenFile(walTarget, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		p.closeLocked()
		return nil, err
	}
	p.walFile = walFile
	aborted := map[uint64]bool{}
	for _, s := range segs {
		for _, r := range s.records {
			if r.op == persistOpAbort {
				aborted[r.abortedSeq] = true
			}
		}
	}
	for _, s := range segs {
		for _, r := range s.records {
			if r.seq > p.seq {
				p.seq = r.seq
			}
			if r.seq <= p.checkpointSeq || aborted[r.seq] {
				continue
			}
			sh := p.shardFor(string(r.key))
			switch r.op {
			case persistOpPut:
				if err := sh.sdb.Put(string(r.key), r.val); err != nil {
					p.closeLocked()
					return nil, fmt.Errorf("yowza persist: WAL replay put seq %d: %v", r.seq, err)
				}
				// Phase E epoch guard: the replayed
				// tail IS the current epoch's history,
				// so count it (no guard flush here —
				// replay must not flush mid-open).
				p.epochNoteLocked(p.ShardIndex(string(r.key)), string(r.key))
			case persistOpDel:
				if err := sh.sdb.Delete(string(r.key)); err != nil {
					p.closeLocked()
					return nil, fmt.Errorf("yowza persist: WAL replay delete seq %d: %v", r.seq, err)
				}
				p.epochNoteLocked(p.ShardIndex(string(r.key)), string(r.key))
			}
		}
	}
	// A WAL shorter than the checkpoint (operator truncation to a
	// clean boundary) is not an error: the heap already covers
	// those records. New seqs must still not reuse checkpointed
	// seqs, so seq stays at least checkpointSeq (already set).
	if p.seq < p.checkpointSeq {
		p.seq = p.checkpointSeq
	}

	// --- durability availability probe (Sync mode only) ---
	if p.mode == DurabilitySync {
		if err := persistFault(persistFaultOpenWALSync); err != nil {
			p.closeLocked()
			return nil, fmt.Errorf("%w: WAL fsync: %v", ErrDurabilityUnavailable, err)
		}
		if err := p.walFile.Sync(); err != nil {
			p.closeLocked()
			return nil, fmt.Errorf("%w: WAL fsync: %v", ErrDurabilityUnavailable, err)
		}
		if err := persistFault(persistFaultOpenDirSync); err != nil {
			p.closeLocked()
			return nil, fmt.Errorf("%w: directory fsync: %v", ErrDurabilityUnavailable, err)
		}
		if err := persistSyncDir(dir); err != nil {
			p.closeLocked()
			return nil, fmt.Errorf("%w: directory fsync: %v", ErrDurabilityUnavailable, err)
		}
	}
	// --- Phase B2: durable pins, recovered BEFORE Compact ---
	// This runs after WAL replay and before the instance is
	// returned, so no Compact can run against the reopened
	// DB before pinned snapshots are re-leased. A failure
	// here fails the open loudly (see segdb_pins.go).
	p.scanSessions = make(map[uint64]*scanSession)
	p.scanPolicy = DefaultScanPolicy
	if err := p.recoverDurablePins(); err != nil {
		p.closeLocked()
		return nil, err
	}
	// Phase E: spillable version log, recovered after
	// pins (its retention floor reads the pin set).
	sp, err := newSpillHistory(dir, opts.SpillChunkSize, p.mode == DurabilitySync)
	if err != nil {
		p.closeLocked()
		return nil, err
	}
	p.spill = sp
	// PATCH2 Track 1: attach the recovery gate at open,
	// so crash leftovers present in the directory (staged
	// or committed global merges, orphan deltas, WAL
	// incomplete tails, domain commit-without-done)
	// refuse facade reads until gate.Recover() runs.
	// NewTimelineManagerInit refreshes it after its own
	// recovery + healing, so a manager open does not
	// inherit a stale-closed gate.
	p.AttachRecoveryGate()
	return p, nil
}

// appendWAL appends one frame for seq/op and, in Sync mode,
// fsyncs before returning, so a returned mutation is durable
// in the host WAL. ANY failure poisons the instance (caller
// holds p.mu): a write failure may have partially written the
// frame, and a sync failure may have left a fully written frame
// that later replays — in both cases the sequence's outcome is
// indeterminate, so the error returned is an
// *IndeterminateWALError and no further normal write may
// proceed until close + reopen decides what was recorded.
// appendWAL never returns a bare write/sync error.
func (p *PersistedSegDB) appendWAL(seq uint64, op string, frame []byte) error {
	if err := persistFault(persistFaultWALWrite); err != nil {
		return p.poisonLocked(seq, op, "write", err)
	}
	if _, err := p.walFile.Write(frame); err != nil {
		return p.poisonLocked(seq, op, "write", err)
	}
	if err := persistFault(persistFaultWALSync); err != nil {
		return p.poisonLocked(seq, op, "sync", err)
	}
	if p.mode == DurabilitySync {
		if err := p.walFile.Sync(); err != nil {
			return p.poisonLocked(seq, op, "sync", fmt.Errorf("%w: WAL append sync: %v", ErrDurabilityUnavailable, err))
		}
	}
	// Successful append: track the current segment's size
	// (Gate 2.1 auto-rotation threshold) and its last seq.
	// On a poisoned append above, neither is updated: the
	// segment's true tail is indeterminate until reopen.
	p.walSegSize += int64(len(frame))
	if cur := p.currentSegmentLocked(); cur != nil {
		cur.size = p.walSegSize
		if seq > cur.lastSeq {
			cur.lastSeq = seq
		}
	}
	return nil
}

// appendAbort records that failedSeq failed to apply in wasm, so
// replay skips it. The abort record itself consumes a new seq.
// Unlike the old code, an abort-append failure is NOT
// swallowed: without the abort record, the failed mutation may
// replay on reopen, so that outcome is indeterminate and the
// instance is poisoned by appendWAL; the error is returned so
// the caller sees both the wasm failure and the abort failure.
func (p *PersistedSegDB) appendAbort(failedSeq uint64) error {
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], failedSeq)
	p.seq++
	abortFrame := persistEncodeRecord(p.seq, persistOpAbort, nil, tmp[:])
	return p.appendWAL(p.seq, "abort", abortFrame)
}

// Put stores key/value: WAL append (+fsync in Sync mode) first,
// then the wasm SegDB put. If the wasm put fails, an ABORT record
// is appended so replay skips the failed mutation; the error is
// still returned, and the caller must treat the mutation as
// indeterminate, not as definitely absent (a crash between the
// WAL append and the ABORT append could still replay it). If
// the ABORT append itself fails, the instance is poisoned (see
// header comment) and both errors are returned. While poisoned,
// Put refuses immediately without issuing a new sequence.
//
// Phase E epoch rollover: a history-full failure
// (ErrSegDBHistFull — the C hot history table reached
// its 1024-entry epoch bound) is NOT a user error.
// The failed attempt is aborted as above, a flush
// epoch moves the hot history into segments and
// clears the table, and the put is retried exactly
// once under a fresh sequence. The C table stays an
// epoch bound; the facade no longer fails at 1024
// versions, and the spill log (below) retains every
// version across epochs.
func (p *PersistedSegDB) Put(key string, val []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	// PATCH2 Track 2: coordinator fencing. In
	// coordinated mode a facade Put carries no
	// coordinator token or op ID, so it is
	// indistinguishable from a bypass and is refused
	// outright with ErrCoordFenceBypass — before any
	// sequence is consumed or any WAL frame written.
	// Coordinated writes go through the domain core,
	// never the facade. PutBlob calls Put, so it is
	// fenced by this one insertion.
	if err := CheckCoordWriteFence(p); err != nil {
		return err
	}
	if p.closed {
		return ErrPersistedClosed
	}
	if err := p.checkWritableLocked(); err != nil {
		return err
	}
	// Phase E epoch guard: flush proactively before
	// this mutation could breach a C epoch bound
	// (see epoch_guard.go). Validation runs first
	// inside the attempt, so an invalid put never
	// triggers a guard flush via this path — the
	// guard below runs after a cheap length check.
	if len(key) == 0 || len(key) > 1023 || len(val) > 1024 || len(key)+len(val) > 1014 {
		return p.putAttemptLocked(key, val)
	}
	if err := p.epochGuardLocked(key); err != nil {
		return err
	}
	err := p.putAttemptLocked(key, val)
	if err != nil && errors.Is(err, ErrSegDBHistFull) {
		if ferr := p.flushLocked(); ferr != nil {
			return fmt.Errorf("yowza persist: history-full flush failed: %v (original: %w)", ferr, err)
		}
		err = p.putAttemptLocked(key, val)
	}
	if err == nil {
		p.epochNoteLocked(p.ShardIndex(key), key)
	}
	return err
}

// putAttemptLocked is one Put attempt: validation,
// spill pre-image, WAL frame, shard put, abort on
// failure. Caller holds p.mu.
func (p *PersistedSegDB) putAttemptLocked(key string, val []byte) error {
	if len(key) == 0 || len(key) > 1023 {
		return fmt.Errorf("yowza persist: key length %d out of range 1..1023", len(key))
	}
	if len(val) > 1024 {
		return fmt.Errorf("yowza persist: value length %d exceeds 1024", len(val))
	}
	// Inherited engine ceiling, checked here so a predictably
	// failing put never pollutes the WAL: yowza_db_put_raw builds
	// the mutation in a fixed 1024-byte bytecode buffer
	// (1 + 4 + keyLen + 4 + valLen + 1 <= 1024), so the true
	// hot-path budget is keyLen + valLen <= 1014, not a full
	// 1024-byte value for every key length.
	if len(key)+len(val) > 1014 {
		return fmt.Errorf("yowza persist: key+value length %d exceeds hot-path budget 1014", len(key)+len(val))
	}
	// Dirty BEFORE the mutation (Gate 2.3): the bit stays
	// set if the WAL append fails (poison) or the wasm
	// put fails, and is cleared only by a definite
	// checkpoint publish.
	p.markShardDirtyLocked(key)
	// Phase E spill pre-image, captured before the
	// mutation (direct shard read: no p.mu re-entry).
	var preVal []byte
	preOK := false
	if p.spill != nil {
		if v, err := p.shardFor(key).sdb.Get(key); err == nil {
			preVal, preOK = append([]byte(nil), v...), true
		}
	}
	p.seq++
	if p.spill != nil {
		// Spill record BEFORE the WAL frame: a crash
		// here leaves at most a harmless orphan record
		// (see spill_history.go). A spill failure
		// returns the sequence (no WAL record was
		// written, so reuse is safe and WAL
		// contiguity is preserved).
		if err := p.spill.recordLocked(p.dir, p.seq, key, preVal, preOK, p.mode == DurabilitySync); err != nil {
			p.seq--
			return err
		}
	}
	frame := persistEncodeRecord(p.seq, persistOpPut, []byte(key), val)
	if err := p.appendWAL(p.seq, "put", frame); err != nil {
		return err
	}
	failedSeq := p.seq
	if err := p.shardFor(key).sdb.Put(key, val); err != nil {
		if abortErr := p.appendAbort(failedSeq); abortErr != nil {
			return fmt.Errorf("yowza persist: put seq %d failed in wasm: %v; abort record also failed (outcome indeterminate): %w", failedSeq, err, abortErr)
		}
		return err
	}
	p.maybeAutoCheckpointLocked()
	return nil
}

// Delete removes key (hot tombstone, as in SegDB.Delete). Same
// poison rules as Put, and the same Phase E epoch
// rollover on a history-full failure (flush, retry
// once).
func (p *PersistedSegDB) Delete(key string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	// PATCH2 Track 2: coordinator fencing, same rule
	// as Put above — refused outright in coordinated
	// mode, before any sequence consumption.
	if err := CheckCoordWriteFence(p); err != nil {
		return err
	}
	if p.closed {
		return ErrPersistedClosed
	}
	if err := p.checkWritableLocked(); err != nil {
		return err
	}
	if len(key) == 0 || len(key) > 1023 {
		return p.deleteAttemptLocked(key)
	}
	if err := p.epochGuardLocked(key); err != nil {
		return err
	}
	err := p.deleteAttemptLocked(key)
	if err != nil && errors.Is(err, ErrSegDBHistFull) {
		if ferr := p.flushLocked(); ferr != nil {
			return fmt.Errorf("yowza persist: history-full flush failed: %v (original: %w)", ferr, err)
		}
		err = p.deleteAttemptLocked(key)
	}
	if err == nil {
		p.epochNoteLocked(p.ShardIndex(key), key)
	}
	return err
}

// deleteAttemptLocked is one Delete attempt. Caller holds p.mu.
func (p *PersistedSegDB) deleteAttemptLocked(key string) error {
	if len(key) == 0 || len(key) > 1023 {
		return fmt.Errorf("yowza persist: key length %d out of range 1..1023", len(key))
	}
	// Dirty BEFORE the mutation (Gate 2.3; see Put).
	p.markShardDirtyLocked(key)
	var preVal []byte
	preOK := false
	if p.spill != nil {
		if v, err := p.shardFor(key).sdb.Get(key); err == nil {
			preVal, preOK = append([]byte(nil), v...), true
		}
	}
	p.seq++
	if p.spill != nil {
		if err := p.spill.recordLocked(p.dir, p.seq, key, preVal, preOK, p.mode == DurabilitySync); err != nil {
			p.seq--
			return err
		}
	}
	frame := persistEncodeRecord(p.seq, persistOpDel, []byte(key), nil)
	if err := p.appendWAL(p.seq, "delete", frame); err != nil {
		return err
	}
	failedSeq := p.seq
	if err := p.shardFor(key).sdb.Delete(key); err != nil {
		if abortErr := p.appendAbort(failedSeq); abortErr != nil {
			return fmt.Errorf("yowza persist: delete seq %d failed in wasm: %v; abort record also failed (outcome indeterminate): %w", failedSeq, err, abortErr)
		}
		return err
	}
	p.maybeAutoCheckpointLocked()
	return nil
}

// Get returns the value for key, or an error when missing (same
// semantics as SegDB.Get).
func (p *PersistedSegDB) Get(key string) ([]byte, error) {
	// PATCH2 Track 1: recovery-gated read.
	if err := p.checkReadsAllowed(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, ErrPersistedClosed
	}
	return p.shardFor(key).sdb.Get(key)
}

// Scan returns entries with the given prefix across all shards,
// merged in key order. A shard scan failure returns an error, never
// a partial list.
func (p *PersistedSegDB) Scan(prefix string) ([]SegDBScanEntry, error) {
	// PATCH2 Track 1: recovery-gated read.
	if err := p.checkReadsAllowed(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, ErrPersistedClosed
	}
	var all []SegDBScanEntry
	for _, sh := range p.shards {
		entries, err := sh.sdb.Scan(prefix, 0)
		if err != nil {
			return nil, err
		}
		all = append(all, entries...)
	}
	sort.Slice(all, func(i, j int) bool {
		return string(all[i].Key) < string(all[j].Key)
	})
	return all, nil
}

// checkpointLocked persists every shard heap + the checkpoint
// commit point, generationally. Caller must hold p.mu. (The
// reclaim helper lives in segdb_persist_lifecycle.go.)
//
//   - New generation = current generation + 1; its heap files
//     are written, synced, renamed, and then RE-READ and
//     validated (size + SHA-256) before any checkpoint is
//     published. Any failure before publish leaves the old
//     checkpoint and its heap files byte-identical, and does
//     NOT poison the database: the next Flush/Compact retries
//     with the same new generation number (its files are
//     still unreferenced orphans, so overwriting them is
//     safe).
//   - Mixed checkpoints (Gate 2.3, MixedCheckpoint option):
//     a shard whose heap SHA equals the authoritative
//     checkpoint's SHA reuses that checkpoint's immutable
//     heap file instead of a fresh write. The reused file
//     is re-read and checksum-verified before publish; on
//     mismatch a fresh generation file is written with the
//     same bytes (self-healing, never a corrupt publish).
//     Dirty bits do not decide reuse — SHA comparison
//     does — so a wrong dirty bit costs a write, never
//     correctness; dirty bits are cleared only on definite
//     publish (see segdb_persist_lifecycle.go).
//   - Previous retention (Gate 2.2, KeepPreviousGeneration
//     option): the new checkpoint records its predecessor
//     (one generation deep), and checkpoint.prev.json is
//     written with that predecessor BEFORE the new
//     checkpoint publishes, so a corrupt checkpoint.json
//     does not take the fallback descriptor with it. A
//     prev-write failure aborts the checkpoint with the
//     old checkpoint intact.
//   - Checkpoint publish: tmp + sync + rename + dir sync. A
//     failure before the rename aborts with the old checkpoint
//     intact. A failure at the checkpoint dir-sync stage,
//     after the rename, is special: the new checkpoint is
//     already the on-disk truth for any later open, so the
//     in-process state advances to it, old generations are
//     NOT reclaimed (either checkpoint that survives a crash
//     must keep its heaps), and the error is still returned
//     as ErrDurabilityUnavailable (Sync mode) / the hook
//     error, so durability is never silently claimed.
//   - Only after a fully definite publish are old generations
//     reclaimed (heaps not named by the new or — when
//     retained — the previous checkpoint; WAL segments
//     fully covered by the retained floor). Dirty bits are
//     cleared only here (and in the dir-sync-after-rename
//     case, where the new checkpoint is already the truth).
// Background publish (worker S, gate 6). A checkpoint
// publish hashes and writes O(heap) bytes — at 256 MiB
// that was ~11 s holding p.mu, stalling every writer.
// The publish is therefore split in three phases:
//
//   - snapshotCheckpointLocked (caller holds p.mu):
//     HeapBytes copies (the only O(heap) step still on
//     the writer lock; a copy is unavoidable because
//     wasm memory is mutable) + sequence/manifest
//     capture. SHAs are computed off-lock in
//     publishSnapshot from the immutable copies, and
//     mixed-reuse names are resolved there under a
//     brief p.mu section (curCkpt cannot change under
//     publishCh, so the resolution is identical).
//   - publishSnapshot (NO p.mu held): sparse heap
//     writes, validation re-read + SHA, prev backup,
//     checkpoint.json rename. Writers proceed during
//     all of it; the snapshot bytes are immutable.
//   - advanceCheckpointLocked (caller holds p.mu):
//     in-memory state moves to the published checkpoint
//     + reclaim.
//
// Serialization: publishCh is a capacity-1 channel
// mutex. The wait path (Flush/Compact/RotateWAL)
// acquires it BEFORE p.mu; the background path
// (auto-checkpoint, epoch guard, HistFull retry)
// try-acquires it while holding p.mu and DROPS the
// snapshot (setting publishPending) when busy — it
// never blocks on it while holding p.mu, so the lock
// order publishCh -> p.mu is never violated and no
// deadlock is possible. Generations advance in order
// because a snapshot's newGen is p.generation+1 taken
// under p.mu, and p.generation changes only under
// publishCh (open, wait path, background advance).
// Dropping a snapshot loses nothing logically: WAL
// replay covers every write the snapshot would have
// checkpointed, and the next trigger publishes newer
// state. Publish errors on background paths are
// recorded in autoErr (the auto-checkpoint rule),
// never fail the mutation — exactly as
// auto-checkpoints behaved before.
//
// The one semantic change to note: advance clears
// dirty bits even for puts that landed DURING the
// publish (after the snapshot). Dirty bits are an
// optimization hint only (see
// segdb_persist_lifecycle.go: checkpointLocked always
// SHA-compares, so a wrong bit costs a write, never
// correctness), and the next publish SHA-compares and
// writes fresh heaps for changed shards regardless.

// persistPublishSnapshot is one checkpoint's immutable
// snapshot: heap bytes + SHAs + resolved file names +
// the sequence and WAL-segment list at snapshot time.
// rotateAfter asks the background completer to run WAL
// rotation after a definite publish (auto-checkpoint
// only; explicit RotateWAL rotates synchronously).
type persistPublishSnapshot struct {
	heaps       [][]byte
	shas        []string
	newGen      uint64
	newFiles    []string
	newPrev     *persistCheckpoint
	seq         uint64
	walMetas    []persistWALSegmentMeta
	rotateAfter bool
}

// publishAcquire takes the publish mutex (blocking).
// Callers must not hold p.mu when calling it.
func (p *PersistedSegDB) publishAcquire() {
	p.publishCh <- struct{}{}
}

// publishRelease releases the publish mutex.
func (p *PersistedSegDB) publishRelease() {
	<-p.publishCh
}

// publishTryAcquire takes the publish mutex without
// blocking. Safe to call while holding p.mu.
func (p *PersistedSegDB) publishTryAcquire() bool {
	select {
	case p.publishCh <- struct{}{}:
		return true
	default:
		return false
	}
}

// drainPublish waits for any in-flight publish (and its
// rotation, which precedes the mutex release) to finish.
// Callers must not hold p.mu when calling it. The
// published-state accessors (Generation,
// CheckpointSeq, WALSegments, AutoCheckpointError)
// drain first, so they always report post-publish state.
func (p *PersistedSegDB) drainPublish() {
	p.publishAcquire()
	p.publishRelease()
}

// snapshotCheckpointLocked captures one checkpoint's
// snapshot. Caller holds p.mu. The wasm flush/compact
// that precedes it must already have run. It copies
// heap bytes only — the SHA pass (O(heap) CPU, ~2 s
// at 256 MiB on this VM) runs off-lock in
// publishSnapshot, because the copies are already
// immutable; holding p.mu across the SHA would stall
// every writer for the whole hash, defeating the
// background publish. Mixed-reuse names are likewise
// resolved in publishSnapshot, not here.
func (p *PersistedSegDB) snapshotCheckpointLocked() (*persistPublishSnapshot, error) {
	newGen := p.generation + 1
	snap := &persistPublishSnapshot{
		heaps:    make([][]byte, len(p.shards)),
		newFiles: make([]string, len(p.shards)),
		newGen:   newGen,
		seq:      p.seq,
		walMetas: persistSegmentMetas(p.walSegs),
	}
	if p.keepPrev && p.curCkpt != nil {
		snap.newPrev = persistCheckpointCopy(p.curCkpt)
	}
	for i, sh := range p.shards {
		heap, err := sh.sdb.HeapBytes()
		if err != nil {
			return nil, err
		}
		snap.heaps[i] = heap
		snap.newFiles[i] = persistCheckpointHeapFileName(newGen, i)
	}
	return snap, nil
}

// publishSnapshot writes a snapshot's heap files and
// publishes its checkpoint.json. Caller must NOT hold
// p.mu (writers proceed during this whole phase, and
// publishSnapshot itself takes p.mu briefly for
// mixed-reuse resolution, so a caller holding p.mu
// would deadlock).
// It returns the checkpoint record and whether the
// checkpoint.json rename happened. A dir-sync failure
// AFTER the rename returns (ckpt, true, err): the new
// checkpoint is already the on-disk truth, so the
// caller must still advance (without reclaim) and
// return the error. Any other error returns (nil,
// false, err): nothing published, old checkpoint
// intact, safe to retry with the same generation.
func (p *PersistedSegDB) publishSnapshot(snap *persistPublishSnapshot) (*persistCheckpoint, bool, error) {
	// SHA pass, off-lock: the snapshot bytes are
	// immutable copies, so hashing them needs no lock.
	// (Hashing under p.mu stalled writers for the
	// whole pass — the reason this moved here.)
	snap.shas = make([]string, len(snap.heaps))
	for i, heap := range snap.heaps {
		sum := sha256.Sum256(heap)
		snap.shas[i] = hex.EncodeToString(sum[:])
	}
	// Mixed-reuse resolution, under a brief p.mu
	// section: same SHA as the authoritative checkpoint
	// and the named file still exists. curCkpt cannot
	// have changed since the snapshot (it moves only
	// at advance, which requires publishCh, held
	// here), so this reads the same state the snapshot
	// would have. Reused files are verified below with
	// all others; a failed verification falls back to
	// a fresh write of the snapshot bytes.
	p.mu.Lock()
	for i := range snap.heaps {
		if p.mixed && p.curCkpt != nil && i < len(p.curCkpt.HeapSHA256) &&
			snap.shas[i] == p.curCkpt.HeapSHA256[i] {
			reuse := persistCheckpointHeapFiles(p.curCkpt)[i]
			if _, err := os.Stat(filepath.Join(p.dir, reuse)); err == nil {
				snap.newFiles[i] = reuse
			}
		}
	}
	p.mu.Unlock()
	for i, heap := range snap.heaps {
		if snap.newFiles[i] == persistCheckpointHeapFileName(snap.newGen, i) {
			// Fresh file (not a mixed reuse): sparse
			// write, same tmp + sync + rename + dir
			// sync discipline as before; zero runs are
			// seeked over, so a mostly-empty 256 MiB
			// heap costs blocks only for live bytes.
			if err := persistWriteHeapAtomic(p.dir, snap.newFiles[i], heap, p.mode); err != nil {
				return nil, false, err
			}
		}
	}
	// Validate ALL files named by the new checkpoint
	// before publishing. A reused file that fails
	// verification is rewritten fresh from the
	// SNAPSHOT bytes (never a live re-read: the live
	// heap may have moved on during this publish)
	// and re-validated; only a second failure aborts.
	for i := range snap.heaps {
		back, err := os.ReadFile(filepath.Join(p.dir, snap.newFiles[i]))
		if err != nil || uint64(len(back)) != p.heapSize || sha256Sum(back) != snap.shas[i] {
			fresh := persistCheckpointHeapFileName(snap.newGen, i)
			if err := persistWriteHeapAtomic(p.dir, fresh, snap.heaps[i], p.mode); err != nil {
				return nil, false, err
			}
			back2, err := os.ReadFile(filepath.Join(p.dir, fresh))
			if err != nil {
				return nil, false, fmt.Errorf("yowza persist: new-generation heap shard %d unreadable before publish: %v", i, err)
			}
			if uint64(len(back2)) != p.heapSize || sha256Sum(back2) != snap.shas[i] {
				return nil, false, fmt.Errorf("yowza persist: new-generation heap shard %d checksum mismatch before publish", i)
			}
			snap.newFiles[i] = fresh
		}
	}
	ckpt := persistCheckpoint{
		Magic:         persistCheckpointMagic,
		Version:       1,
		NumShards:     len(p.shards),
		CheckpointSeq: snap.seq,
		HeapSHA256:    snap.shas,
		Wasm64:        p.wasm64,
		HeapSize:      p.heapSize,
		Generation:    snap.newGen,
		HeapFiles:     snap.newFiles,
		Previous:      snap.newPrev,
		WALSegments:   snap.walMetas,
	}
	data, err := json.MarshalIndent(ckpt, "", "  ")
	if err != nil {
		return nil, false, err
	}
	// Previous-descriptor backup BEFORE the commit point.
	if p.keepPrev && snap.newPrev != nil {
		prevData, err := json.MarshalIndent(snap.newPrev, "", "  ")
		if err != nil {
			return nil, false, err
		}
		if err := persistWriteAtomic(p.dir, "checkpoint.prev.json", prevData, p.mode, "prev"); err != nil {
			return nil, false, err
		}
	}
	if err := persistWriteAtomic(p.dir, "checkpoint.json", data, p.mode, "checkpoint"); err != nil {
		// persistWriteAtomic marks a post-rename dir-sync
		// failure with errCheckpointDirSyncAfterRename
		// (the only stage after rename), so that case
		// means the new checkpoint is already published.
		if errors.Is(err, errCheckpointDirSyncAfterRename) {
			return &ckpt, true, err
		}
		return nil, false, err
	}
	return &ckpt, true, nil
}

// advanceCheckpointLocked moves the in-memory checkpoint
// state to a published checkpoint and clears dirty bits
// (caller holds p.mu; called on definite publish and on
// the dir-sync-after-rename case, where the new checkpoint
// is already the on-disk truth). checkpointSeq comes
// from the published record, NOT live p.seq: puts that
// landed during a background publish advanced p.seq
// past the snapshot, and the published checkpoint
// covers only its own sequence.
func (p *PersistedSegDB) advanceCheckpointLocked(ckpt *persistCheckpoint, newFiles, shas []string, newGen uint64, newPrev *persistCheckpoint) {
	p.checkpointSeq = ckpt.CheckpointSeq
	p.heapSHA = shas
	p.generation = newGen
	p.heapFiles = append([]string(nil), newFiles...)
	p.curCkpt = persistCheckpointCopy(ckpt)
	p.curCkpt.Previous = newPrev
	p.prevCkpt = newPrev
	for i := range p.shardDirty {
		p.shardDirty[i] = false
	}
}

// advanceAndReclaimLocked advances to a published
// snapshot and reclaims old generations (caller holds
// p.mu). The dir-sync-after-rename case advances
// WITHOUT reclaim: either checkpoint may survive a
// crash if the new directory entry was not synced.
func (p *PersistedSegDB) advanceAndReclaimLocked(ckpt *persistCheckpoint, snap *persistPublishSnapshot, reclaim bool) {
	p.advanceCheckpointLocked(ckpt, snap.newFiles, snap.shas, snap.newGen, snap.newPrev)
	if !reclaim {
		return
	}
	keepHeaps := append([]string(nil), snap.newFiles...)
	if p.keepPrev && snap.newPrev != nil {
		keepHeaps = append(keepHeaps, persistCheckpointHeapFiles(snap.newPrev)...)
	}
	p.reclaimHeapFilesLocked(keepHeaps)
	p.reclaimSegmentsLocked()
}

// publishWaitLocked runs a snapshot's publish inline and
// advances (caller holds publishCh but NOT p.mu during
// the publish; p.mu is taken only for the brief reuse
// resolution inside publishSnapshot and for the advance).
// It returns the publish error, if any.
func (p *PersistedSegDB) publishWaitLocked(snap *persistPublishSnapshot) error {
	ckpt, published, err := p.publishSnapshot(snap)
	if !published {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	// Dir-sync-after-rename: published with an error —
	// advance without reclaim, then report the error.
	p.advanceAndReclaimLocked(ckpt, snap, err == nil)
	return err
}

// publishBackground launches a snapshot's publish in a
// goroutine, or drops it (setting publishPending) when
// a publish is already in flight. Caller holds p.mu.
// Publish errors land in autoErr; a dropped snapshot
// is picked up by the next trigger's fresh snapshot.
func (p *PersistedSegDB) publishBackground(snap *persistPublishSnapshot) {
	if !p.publishTryAcquire() {
		p.publishPending = true
		return
	}
	go func() {
		defer p.publishRelease()
		ckpt, published, err := p.publishSnapshot(snap)
		p.mu.Lock()
		defer p.mu.Unlock()
		if published {
			p.advanceAndReclaimLocked(ckpt, snap, err == nil)
			if snap.rotateAfter && err == nil {
				// Auto-checkpoint rotation, after a
				// definite publish only (same rule as
				// before: rotation never runs on a
				// failed flush). Rotation reads live
				// p.seq/walSegs: puts that landed
				// during the publish went to the
				// current segment, whose lastSeq is
				// past the published floor, so the
				// reclaim rule keeps it — sealing it
				// is sound, replay still covers it.
				if rerr := p.rotateLocked(); rerr != nil {
					p.autoErr = rerr
				}
			}
		}
		if err != nil {
			p.autoErr = err
		}
	}()
}

// wasmFlushLocked flushes every shard inside wasm
// (caller holds p.mu). It is the shared first step of
// the wait path (Flush) and the background path
// (flushLocked).
func (p *PersistedSegDB) wasmFlushLocked() error {
	for i, sh := range p.shards {
		if err := sh.sdb.Flush(); err != nil {
			return fmt.Errorf("yowza persist: shard %d flush: %v", i, err)
		}
	}
	return nil
}

// Flush flushes every shard inside wasm, then publishes
// a checkpoint synchronously: it returns only after
// the publish completes, preserving Flush's contract.
// Writers are unblocked during the publish itself —
// only the snapshot copy (HeapBytes) holds p.mu.
// If a wasm flush or snapshot fails, no checkpoint is
// written and the error is returned; disk state stays
// at the old checkpoint and WAL replay still
// reconstructs the logical state.
func (p *PersistedSegDB) Flush() error {
	p.publishAcquire()
	defer p.publishRelease()
	p.mu.Lock()
	// PATCH2 Track 2: maintenance fencing. The hold
	// spans the whole operation (fence release is
	// deferred to return, after the publish): a bare
	// Validate() before the operation would leave a
	// check → revocation → effect window. Lock order
	// is always publishCh -> p.mu -> session mu.
	release, err := CheckCoordMaintenanceFence(p)
	if err != nil {
		p.mu.Unlock()
		return err
	}
	if release != nil {
		defer release()
	}
	if p.closed {
		p.mu.Unlock()
		return ErrPersistedClosed
	}
	if err := p.checkWritableLocked(); err != nil {
		p.mu.Unlock()
		return err
	}
	if err := p.wasmFlushLocked(); err != nil {
		p.mu.Unlock()
		return err
	}
	snap, err := p.snapshotCheckpointLocked()
	if err != nil {
		p.mu.Unlock()
		return err
	}
	p.epochResetLocked()
	p.mu.Unlock()
	return p.publishWaitLocked(snap)
}

// Compact merges segments in every shard (barrier semantics as in
// SegDB.Compact), then checkpoints. A failed shard compact aborts
// the checkpoint; compaction is logically a no-op, so the old
// checkpoint + WAL replay still yields the same logical state.
// Compaction rewrites segment layout, so every shard is
// marked dirty BEFORE compacting (dirty bits are never
// cleared on a failed compact).
func (p *PersistedSegDB) Compact(barrier uint64) error {
	p.publishAcquire()
	defer p.publishRelease()
	p.mu.Lock()
	// PATCH2 Track 2: maintenance fencing, same hold
	// rule as Flush above.
	release, err := CheckCoordMaintenanceFence(p)
	if err != nil {
		p.mu.Unlock()
		return err
	}
	if release != nil {
		defer release()
	}
	if p.closed {
		p.mu.Unlock()
		return ErrPersistedClosed
	}
	if err := p.checkWritableLocked(); err != nil {
		p.mu.Unlock()
		return err
	}
	for i := range p.shardDirty {
		p.shardDirty[i] = true
	}
	for i, sh := range p.shards {
		if err := sh.sdb.Compact(barrier); err != nil {
			p.mu.Unlock()
			return fmt.Errorf("yowza persist: shard %d compact: %v", i, err)
		}
	}
	// Compact does NOT reset the epoch (it leaves hot
	// history untouched); the snapshot below covers the
	// compacted segments as of this point.
	snap, err := p.snapshotCheckpointLocked()
	if err != nil {
		p.mu.Unlock()
		return err
	}
	p.mu.Unlock()
	return p.publishWaitLocked(snap)
}

// SegmentCounts returns the per-shard segment count (last group's
// count per shard, as in SegDB.SegmentCount).
func (p *PersistedSegDB) SegmentCounts() ([]int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, ErrPersistedClosed
	}
	out := make([]int, len(p.shards))
	for i, sh := range p.shards {
		n, err := sh.sdb.SegmentCount()
		if err != nil {
			return nil, err
		}
		out[i] = n
	}
	return out, nil
}

// Close closes the WAL file and every shard's wasm module. It
// does NOT flush or checkpoint: unflushed mutations survive via
// WAL replay on the next open, which is the point of the host
// WAL. Close is idempotent. It drains any in-flight
// background publish first (never while holding p.mu):
// closing under a live publish would leave the publish
// writing to a closed DB's files and shards.
func (p *PersistedSegDB) Close() error {
	p.drainPublish()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closeLocked()
	// PATCH2 Track 2: a closed DB must not stay
	// registered for fencing (a later reopen is a
	// new instance with its own registration).
	UnregisterCoordFence(p)
	return nil
}

// RepairWALTail truncates the WAL to the end of the last fully
// valid record, fsyncs file + directory, and returns that
// record's seq (0 if no valid record). If the WAL is already
// clean it is a no-op returning the last seq. Use it after an
// open failed with *CorruptWALError, then reopen: recovery
// lands on the checkpoint state plus all valid
// pre-corruption records.
//
// Segmented WAL (Gate 2.1): segments are repaired in
// first-sequence order. The first corrupt segment is
// truncated at its corrupt offset, and every LATER segment
// is deleted (its records are unreachable without the
// corrupted middle — keeping them would fake a longer
// valid history). Repair takes the directory lock
// (Gate 2.4a): repairing under a live opener returns
// ErrDatabaseLocked instead of racing its appends.
func RepairWALTail(dir string) (uint64, error) {
	lockFile, err := persistLockDir(dir)
	if err != nil {
		return 0, err
	}
	defer persistUnlockDir(lockFile)
	// List segment files in first-sequence order.
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, err
	}
	type segFile struct {
		name  string
		first uint64
	}
	var files []segFile
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if first, ok := persistWALSegmentName(e.Name()); ok {
			files = append(files, segFile{e.Name(), first})
		}
	}
	if len(files) == 0 {
		return 0, fmt.Errorf("yowza persist: no WAL segment in %s", dir)
	}
	sort.Slice(files, func(i, j int) bool { return files[i].first < files[j].first })
	var last uint64
	for idx, sf := range files {
		data, err := os.ReadFile(filepath.Join(dir, sf.name))
		if err != nil {
			return 0, err
		}
		records, _, perr := persistParseWALSegment(data, sf.first)
		if perr == nil {
			if len(records) > 0 {
				last = records[len(records)-1].seq
			}
			continue
		}
		var corrupt *CorruptWALError
		if !errors.As(perr, &corrupt) {
			return 0, perr
		}
		path := filepath.Join(dir, sf.name)
		if err := os.Truncate(path, corrupt.Offset); err != nil {
			return 0, err
		}
		f, err := os.OpenFile(path, os.O_WRONLY, 0644)
		if err != nil {
			return 0, err
		}
		syncErr := f.Sync()
		f.Close()
		if syncErr != nil {
			return 0, syncErr
		}
		// Later segments are unreachable past the
		// corruption: delete them, then sync the
		// directory (which also persists the deletions).
		for _, later := range files[idx+1:] {
			_ = os.Remove(filepath.Join(dir, later.name))
		}
		// Fault points for repair's sync stages. Note the
		// truncate above has already happened when these
		// fire, so a repair interrupted here still leaves
		// a valid WAL prefix (the truncation point IS a
		// valid record boundary); reopening succeeds
		// either way.
		if err := persistFault(persistFaultRepairFileSync); err != nil {
			return 0, err
		}
		if err := persistSyncDir(dir); err != nil {
			return 0, err
		}
		if err := persistFault(persistFaultRepairDirSync); err != nil {
			return 0, err
		}
		return corrupt.LastValidSeq, nil
	}
	return last, nil
}

// Multi-shard streaming query support (Reliability stage item 04).
//
// This file is the ONLY item-04 addition to the persisted-storage
// layer, and it is deliberately read-only with respect to the
// persistence protocol: it changes no checkpoint, WAL, poison, or
// durability logic in segdb_persist.go. Everything here is a new
// read path over the existing shard set.
//
// SHARED SNAPSHOT POLICY — the snapshot vector.
//
// A PersistedSegDB has no single snapshot sequence: each shard is
// its own SegDB with its own commit sequence, and the host WAL
// sequence (p.seq) is NOT a SegDB snapshot sequence for any shard
// (it counts WAL records, including ABORTs, across all shards).
// Treating p.seq as a per-shard snapshot would be wrong, so this
// item does not do it. Instead, one query execution takes one
// SNAPSHOT VECTOR: one per-shard SnapshotSeq, captured atomically
// under p.mu (SnapshotVector). Writes hold p.mu for their whole
// Put/Delete, so no write can land between two shards' sequence
// captures: the vector is a single point in the database's
// history, even though its components are per-shard counters.
//
// Consequences, stated precisely:
//   - Every shard of the query reads at its vector entry, via
//     SegDB.ScanPage at that fixed sequence. A write committed
//     after the vector was captured is invisible in EVERY shard,
//     including a write landing "between" two shards' reads or
//     between two tables' Opens (the query layer shares one
//     vector per query execution; see query/segdb_source.go).
//   - A write committed BEFORE the capture is visible, subject
//     to the inherited hot-layer limitation (shared with the
//     single-shard path, item 02): an old snapshot cannot
//     resurrect a pre-update value for a key that was updated
//     while still hot (unflushed) — the updated key simply
//     disappears at the old snapshot. Values flushed to segments
//     before the snapshot are exact at that snapshot.
//   - Shards added or removed mid-query: IMPOSSIBLE through the
//     public API. The shard count is fixed at open (a stored
//     directory refuses a different count with
//     ErrPersistedShardCount) and there is no add/remove-shard
//     operation. ScanStreaming therefore refuses a vector whose
//     length does not match the current shard count with
//     ErrPersistedSnapshotMismatch, rather than silently
//     reading a subset. If the database is Closed mid-query,
//     the next page fails with ErrPersistedClosed — a
//     checkable error, never a silently truncated result.
//   - Reads are allowed while the instance is poisoned (poison
//     blocks writes only): a poisoned instance's committed
//     state is still readable at a snapshot vector.
//
// TOMBSTONE-WINS ACROSS SHARDS. Keys route by hash to exactly
// one shard (ShardIndex), and the shard count never changes, so
// one key can never live in two shards: there is no cross-shard
// duplicate to resolve and no cross-shard resurrection. Within
// a shard, ScanPage already merges hot + segments with the
// newest version winning and tombstones filtering older segment
// versions, so a deleted key stays deleted at every snapshot
// taken after its delete, in every shard.
//
// ORDER. The iterator walks shards in shard-index order, and
// within a shard in key order (ScanPage order). The multi-shard
// stream is therefore NOT in global key order (hash partitioning
// does not preserve key order). A query that needs global order
// must use ORDER BY; the query engine's SORT operator sorts the
// full result, so this costs nothing in correctness. Scan
// results without ORDER BY have no order guarantee in the
// query contract anyway.

// ErrPersistedSnapshotMismatch is returned when a snapshot
// vector does not match the database it is used against: wrong
// length for the current shard count. Callers must take a fresh
// vector from the same database instance, never reuse a vector
// from another instance or an older shard layout.
var ErrPersistedSnapshotMismatch = errors.New("yowza persist: snapshot vector length does not match shard count")

// ShardIndex returns the shard index a key routes to, using the
// same hash as shardFor. It is public so query planners and
// tests can reason about shard placement (e.g. proving that a
// join test's related rows really live in different shards)
// without reimplementing the hash.
func (p *PersistedSegDB) ShardIndex(key string) int {
	h := fnv.New64a()
	h.Write([]byte(key))
	return int(h.Sum64() % uint64(len(p.shards)))
}

// SnapshotVector captures one snapshot vector for the whole
// shard set, atomically under p.mu (see the file header for
// why a vector, not a scalar, and what it guarantees). It
// returns the per-shard sequences and the host WAL sequence at
// capture time (the latter is diagnostic only: it is NOT a
// snapshot sequence for any shard and must never be passed to
// ScanPage). The returned slice is a fresh copy.
func (p *PersistedSegDB) SnapshotVector() ([]uint64, uint64, error) {
	// PATCH2 Track 1: recovery-gated read. Internal
	// callers use snapshotVectorLocked and bypass this.
	if err := p.checkReadsAllowed(); err != nil {
		return nil, 0, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, 0, ErrPersistedClosed
	}
	vec := make([]uint64, len(p.shards))
	for i, sh := range p.shards {
		seq, err := sh.sdb.SnapshotSeq()
		if err != nil {
			return nil, 0, fmt.Errorf("yowza persist: shard %d snapshot seq: %w", i, err)
		}
		vec[i] = seq
	}
	return vec, p.seq, nil
}

// ScanStreaming returns a streaming iterator over one prefix
// across all shards at the given snapshot vector. It
// materializes NOTHING at creation: rows are pulled shard by
// shard, page by page, and a NextBatch call returns at most
// batchSize entries. The iterator holds at most one page plus
// the batch being assembled. An empty batch with a nil error
// means end of input; after EOF the iterator stays at EOF.
//
// Integration wiring (closing contracts, Gate 1): paging is
// cursor-first — each shard is paged through SegDB.ScanCursor
// (exclusive continuation key, next key, has-more), with the
// skip path only as a fallback for binaries predating the
// cursor export. At creation, one snapshot lease per shard is
// registered for the snapshot vector, so compaction cannot
// drop data this iterator still needs; leases are released
// when the iterator closes (or reaches EOF). A lease that is
// no longer valid fails the next page with ErrLeaseExpired —
// never partial rows.
func (p *PersistedSegDB) ScanStreaming(prefix string, snapshot []uint64) (*PersistedScanIter, error) {
	// PATCH2 Track 1: recovery-gated read. In-flight
	// iterators are not re-gated: they read pinned
	// snapshot data, which recovery does not rewrite.
	if err := p.checkReadsAllowed(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, ErrPersistedClosed
	}
	if len(snapshot) != len(p.shards) {
		return nil, fmt.Errorf("%w: vector has %d entries, database has %d shards", ErrPersistedSnapshotMismatch, len(snapshot), len(p.shards))
	}
	if len(prefix) > 1023 {
		return nil, fmt.Errorf("yowza persist: prefix too long")
	}
	leaseIDs := make([]uint64, len(p.shards))
	for i, sh := range p.shards {
		id, err := sh.sdb.RegisterLease(snapshot[i])
		if err != nil {
			for j := 0; j < i; j++ {
				_ = p.shards[j].sdb.ReleaseLease(leaseIDs[j])
			}
			return nil, fmt.Errorf("yowza persist: shard %d lease for snapshot %d: %w", i, snapshot[i], err)
		}
		leaseIDs[i] = id
	}
	return &PersistedScanIter{
		db:       p,
		prefix:   prefix,
		snapshot: append([]uint64(nil), snapshot...),
		leaseIDs: leaseIDs,
	}, nil
}

// PersistedScanIter streams one prefix across the shard set at
// a fixed snapshot vector. It is single-use and not safe for
// concurrent use (like every other iterator in this codebase).
type PersistedScanIter struct {
	db       *PersistedSegDB
	prefix   string
	snapshot []uint64
	shard    int    // current shard index
	skip     uint64 // rows already consumed in the current shard (skip-fallback path only)
	consumed uint64 // rows consumed in the current shard, either path
	// Cursor state. mode: 0 = unprobed (first page tries the
	// cursor), 1 = cursor, 2 = skip fallback.
	mode      int
	cursorKey []byte
	hasCursor bool
	leaseIDs  []uint64
	done      bool
	closed    bool
}

// releaseLeases releases all shard leases exactly once.
func (it *PersistedScanIter) releaseLeases() {
	if it.leaseIDs == nil {
		return
	}
	for i, id := range it.leaseIDs {
		_ = it.db.shards[i].sdb.ReleaseLease(id)
	}
	it.leaseIDs = nil
}

// NextBatch returns up to batchSize entries (batchSize <= 0
// means 64), or an empty slice at EOF. It never returns both
// entries and an error. Cursor-first paging: each shard is
// paged through SegDB.ScanCursor; a short cursor page with
// hasMore false, or an empty page, means end of that shard.
// The skip path is used only if the cursor export is missing
// (ErrSegDBScanCursorMissing), in which case a page shorter
// than the page size requested from ScanPage means end of
// that shard (the same convention SegDB.ScanAt uses), so the
// loop always terminates. A lease that is no longer valid
// fails the page with ErrLeaseExpired — never partial rows.
func (it *PersistedScanIter) NextBatch(batchSize int) ([]SegDBScanEntry, error) {
	if it.done {
		return nil, nil
	}
	if batchSize <= 0 {
		batchSize = 64
	}
	it.db.mu.Lock()
	closed := it.db.closed
	it.db.mu.Unlock()
	if closed {
		return nil, ErrPersistedClosed
	}
	var out []SegDBScanEntry
	for len(out) < batchSize && it.shard < len(it.db.shards) {
		want := batchSize - len(out)
		if want > int(segPageMaxRows) {
			want = int(segPageMaxRows)
		}
		sh := it.db.shards[it.shard]
		if it.leaseIDs != nil && !sh.sdb.LeaseValid(it.leaseIDs[it.shard]) {
			return nil, fmt.Errorf("%w: persisted shard %d lease", ErrLeaseExpired, it.shard)
		}
		advanceShard := func() {
			it.shard++
			it.skip = 0
			it.consumed = 0
			it.cursorKey = nil
			it.hasCursor = false
			it.mode = 0
		}
		if it.mode == 0 || it.mode == 1 {
			page, nextKey, hasMore, err := sh.sdb.ScanCursor(it.prefix, it.snapshot[it.shard], it.cursorKey, it.hasCursor, uint32(want))
			if err == nil {
				it.mode = 1
				if len(page) == 0 {
					advanceShard()
					continue
				}
				out = append(out, page...)
				it.consumed += uint64(len(page))
				if hasMore && len(nextKey) == 0 {
					return nil, fmt.Errorf("yowza persist: shard %d cursor has-more with empty resume key", it.shard)
				}
				it.cursorKey = append([]byte(nil), nextKey...)
				it.hasCursor = true
				if !hasMore {
					advanceShard()
				}
				continue
			}
			if err != ErrSegDBScanCursorMissing {
				return nil, fmt.Errorf("yowza persist: shard %d scan cursor: %w", it.shard, err)
			}
			// Old binary without the cursor export: fall
			// through to the skip path once, and stay there.
			it.mode = 2
			it.skip = it.consumed
		}
		page, err := sh.sdb.ScanPage(it.prefix, it.snapshot[it.shard], it.skip, uint32(want))
		if err != nil {
			return nil, fmt.Errorf("yowza persist: shard %d scan page: %w", it.shard, err)
		}
		if len(page) == 0 {
			advanceShard()
			continue
		}
		out = append(out, page...)
		it.skip += uint64(len(page))
		it.consumed += uint64(len(page))
		if len(page) < want {
			// Short page: this shard is exhausted.
			advanceShard()
		}
	}
	if it.shard >= len(it.db.shards) {
		it.done = true
		it.releaseLeases()
	}
	return out, nil
}

// Close marks the iterator done and releases its shard leases.
// It is idempotent; it does not close the database (the caller
// owns that).
func (it *PersistedScanIter) Close() error {
	it.done = true
	it.closed = true
	it.releaseLeases()
	return nil
}
