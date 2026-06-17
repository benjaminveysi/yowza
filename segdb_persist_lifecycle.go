package yowza

// GATE 2 — Crash-safe storage lifecycle as ONE unified protocol.
//
// This file owns the lifecycle machinery that segdb_persist.go's
// checkpoint/WAL core did not have: WAL segmentation + rotation +
// retention, previous-generation retention + heap-corruption
// fallback, mixed checkpoints (via segdb_persist.go's
// snapshot/publish/advance split), the directory exclusive
// lock, and the
// recovery reporting. The design rules, stated once so every
// piece below can be checked against them:
//
// COMMIT POINT. There is exactly one commit point per
// checkpoint: the checkpoint.json rename (see
// publishSnapshot). WAL rotation is NOT a second commit
// point: it runs only after a checkpoint publish is definite,
// and it only seals a segment boundary. A crash anywhere in
// rotation leaves a parseable directory (the old segment is
// still appendable, or the new segment is empty), and open
// rebuilds the segment list from the directory itself, never
// from an assumed layout.
//
// WAL SEGMENTS. The WAL is a sequence of segment files, in
// first-sequence order:
//   - "wal.log" is the legacy segment: its first sequence is 1.
//   - "wal-<firstSeq>.log" is a rotated segment: its first
//     record's sequence MUST equal <firstSeq>, and records
//     inside a segment are strictly +1 (persistParseWALSegment
//     enforces both). No segment is ever assumed to start at
//     sequence 1 except wal.log.
// A segment is sealed by rotation; sealing never rewrites a
// byte of it, so a sealed segment's checksum chain stays
// valid and its last sequence is learned by parsing it.
//
// RETENTION (one rule for heaps and WAL together). After a
// checkpoint publish, the retained checkpoints are the new
// checkpoint and, when KeepPreviousGeneration is set, its
// previous descriptor. Let floor be the previous
// checkpoint's sequence in that mode, else the new
// checkpoint's sequence. Then:
//   - heap files not named by a retained checkpoint are
//     reclaimed (orphans from interrupted checkpoints too);
//   - WAL segments with lastSeq <= floor are reclaimed,
//     EXCEPT the current append segment (which may still
//     receive records above floor).
// This rule is what makes fallback sound: every record a
// retained previous checkpoint needs for replay (seq >
// previous checkpointSeq) lives in a retained segment, and
// open VERIFIES that coverage (see persistValidateCoverage)
// instead of assuming it. A segment deleted outside this
// rule (operator deletion, a torn directory) is therefore a
// typed ErrPersistedWALGap at open, never a silently short
// replay. When rotation is disabled by WALRotateBytes < 0,
// the current segment is unbounded by explicit operator
// choice; that is the only unbounded mode, and it is
// documented, not default.
//
// FALLBACK. When KeepPreviousGeneration is set, each
// checkpoint records its predecessor (Previous) and a copy
// of that predecessor is also kept in checkpoint.prev.json,
// written BEFORE the new checkpoint publishes, so a corrupt
// checkpoint.json does not take the fallback descriptor with
// it. On open, if the current checkpoint's heap fails
// validation (missing, wrong size, checksum mismatch) or
// checkpoint.json itself is unparseable, open retries with
// the previous descriptor and replays the WAL from there.
// A fallback open is REPORTED (RecoveredViaFallback,
// FallbackGeneration, RecoveryReason) and the in-memory
// authoritative checkpoint becomes the previous one, so the
// next checkpoint's predecessor is the fallback descriptor,
// never the corrupt generation. If no previous descriptor
// exists, or its heaps also fail, open fails with the
// original ErrCorruptHeap / ErrCorruptCheckpoint — a
// single-checkpoint database has no fallback, and that is
// reported as an error, not an empty database.
// Fallback is intentionally NOT the default: two existing
// reliability tests assert the old reclaim behaviour
// (old-generation heaps deleted after a definite publish),
// so flipping the default requires updating those tests.
// See GATE2_NOTES.md. The machinery is complete; the
// default is the conservative choice, not an omission.
//
// MIXED CHECKPOINTS. The publish reuses a previous
// immutable heap file for a shard whose current heap SHA
// equals the checkpointed SHA (MixedCheckpoint option).
// The reused file is re-read and checksum-verified before
// publish; a mismatch does not reuse it — a fresh
// generation file is written with the same (verified)
// bytes instead, so external corruption of a reused file
// self-heals at checkpoint time rather than failing the
// next open. A full-checksum comparison path is kept in
// tests: after a mixed checkpoint, every heap file named
// by the checkpoint must hash to its recorded SHA.
//
// DIRTY BITS. shardDirty[i] is set BEFORE a mutation
// touches shard i (Put/Delete mark it before the WAL
// append; Compact marks all shards before compacting;
// replay marks touched shards). It is cleared ONLY after
// a checkpoint publish (definite publish, including the
// dir-sync-after-rename case, where the new checkpoint
// is already the on-disk truth). With background
// publish, advance also clears bits for puts that landed
// DURING the publish (after the snapshot): that is safe
// only because bits are a hint (see below), never a
// skip decision. It is NEVER cleared on a checkpoint
// failure, a WAL failure, or poisoning: an
// indeterminate mutation leaves the bit set, so the
// next checkpoint writes a fresh heap for that shard.
// Dirty bits are an optimization hint only —
// the publish always compares heap SHAs, so a wrong
// dirty bit can cost a write, never correctness.
//
// SINGLE-OPENER LOCK. Opening a directory takes an
// exclusive non-blocking flock on the "LOCK" file inside
// the directory, held until Close. A second opener (same
// process or another) gets ErrDatabaseLocked before it
// reads or writes anything. The LOCK file is never
// deleted (deleting it would reopen the race) and is
// never reclaimed. RepairWALTail takes the same lock
// briefly, so repair cannot race a live opener.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
)

// sha256Sum returns the hex SHA-256 of data (the checkpoint
// heap-hash format).
func sha256Sum(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// Sentinel errors for the lifecycle protocol.
var (
	// ErrDatabaseLocked is returned when a directory is already
	// open in this or another process, and by RepairWALTail
	// when the directory is locked.
	ErrDatabaseLocked = errors.New("yowza persist: database directory is already open (exclusive lock held)")
	// ErrWALSegment is returned for a WAL segment file whose
	// name does not match its contents (bad first sequence,
	// unparsable name) — never silently skipped.
	ErrWALSegment = errors.New("yowza persist: WAL segment file inconsistent with its name or contents")
)

// DefaultWALRotateBytes is the default current-segment size
// threshold for automatic checkpoint + rotation (8 MiB).
// PersistedOptions.WALRotateBytes == 0 selects this default;
// a negative value disables automatic rotation.
const DefaultWALRotateBytes = int64(8 << 20)

// Fault-injection points for WAL rotation and the previous-
// checkpoint file. They follow the persistFault convention:
// the hook fires before the stage's effect, except
// rotate-dir-sync, which fires after the rename/create it
// follows (mirroring checkpoint-dir-sync: the new segment
// is already the in-process truth when it fires).
const (
	persistFaultRotateRename  = "rotate-rename"
	persistFaultRotateCreate  = "rotate-create"
	persistFaultRotateDirSync = "rotate-dir-sync"
	persistFaultPrevWrite     = "prev-write"
	persistFaultPrevSync      = "prev-sync"
	persistFaultPrevRename    = "prev-rename"
	persistFaultPrevDirSync   = "prev-dir-sync"
)

// persistWALSegmentMeta is the on-disk record of one WAL
// segment at checkpoint-publish time (persistCheckpoint.
// WALSegments). lastSeq is stale after later appends; the
// live segment list (persistWALSegment) is authoritative.
type persistWALSegmentMeta struct {
	File     string `json:"file"`
	FirstSeq uint64 `json:"first_seq"`
	LastSeq  uint64 `json:"last_seq"`
}

// persistWALSegment is one live WAL segment.
type persistWALSegment struct {
	file     string // basename
	firstSeq uint64
	lastSeq  uint64 // firstSeq-1 when the segment is empty
	size     int64  // bytes on disk (current segment tracked live)
	records  []persistWALRecord
}

// persistParseWALSegment parses one segment's bytes, requiring
// its first record's sequence to equal firstSeq (or no records
// at all for an empty segment). It returns the records plus
// the last sequence (firstSeq-1 when empty).
func persistParseWALSegment(data []byte, firstSeq uint64) ([]persistWALRecord, uint64, error) {
	if len(data) == 0 {
		return nil, firstSeq - 1, nil
	}
	records, err := persistParseWALSegmented(data, firstSeq)
	if err != nil {
		return nil, 0, err
	}
	last := firstSeq - 1
	if len(records) > 0 {
		last = records[len(records)-1].seq
	}
	return records, last, nil
}

// persistWALSegmentName parses a WAL segment basename:
// "wal.log" -> firstSeq 1, "wal-<n>.log" -> firstSeq n.
// It reports whether the name is a WAL segment at all.
func persistWALSegmentName(name string) (uint64, bool) {
	if name == "wal.log" {
		return 1, true
	}
	if !strings.HasPrefix(name, "wal-") || !strings.HasSuffix(name, ".log") {
		return 0, false
	}
	mid := name[len("wal-") : len(name)-len(".log")]
	n, err := strconv.ParseUint(mid, 10, 64)
	if err != nil || n == 0 {
		return 0, false
	}
	return n, true
}

// persistScanWALSegments lists and parses every WAL segment
// in dir, sorted by firstSeq. A segment file whose name is
// not a valid segment name is ignored (it is not a WAL
// file); a segment whose contents do not match its name
// (first record != firstSeq, checksum/chain failure) is a
// hard error carrying the segment file in its reason.
func persistScanWALSegments(dir string) ([]*persistWALSegment, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var names []string
	firsts := map[string]uint64{}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if first, ok := persistWALSegmentName(e.Name()); ok {
			names = append(names, e.Name())
			firsts[e.Name()] = first
		}
	}
	sort.Slice(names, func(i, j int) bool { return firsts[names[i]] < firsts[names[j]] })
	var segs []*persistWALSegment
	for _, name := range names {
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return nil, err
		}
		records, last, err := persistParseWALSegment(data, firsts[name])
		if err != nil {
			// Annotate with the segment file; both the
			// ErrWALSegment sentinel and the underlying
			// *CorruptWALError stay reachable via errors
			// .Is/.As (double %w).
			return nil, fmt.Errorf("%w: segment %s: %w", ErrWALSegment, name, err)
		}
		segs = append(segs, &persistWALSegment{
			file:     name,
			firstSeq: firsts[name],
			lastSeq:  last,
			size:     int64(len(data)),
			records:  records,
		})
	}
	return segs, nil
}

// persistValidateCoverage verifies that the live segments
// cover every sequence a replay from checkpointSeq needs,
// contiguously, up to latestSeq (the live WAL head, or the
// corrupt generation's sequence on a fallback open — see
// OpenPersistedSegDB). Segments entirely at or below
// checkpointSeq are skipped (their records are already in
// the heap). The first segment that reaches past
// checkpointSeq must start at or before checkpointSeq+1,
// and each following segment must start exactly where the
// previous ended (+1). An empty trailing segment (the
// current segment after rotation) is skipped by the same
// rule. A gap inside the covered range is
// ErrPersistedWALGap, never a short replay. Note the
// deliberate non-goal: a WAL shorter than the checkpoint
// (live head below checkpointSeq) passes, because the
// heap covers that range — only ranges the heap cannot
// supply (the fallback head) are required to be present.
func persistValidateCoverage(segs []*persistWALSegment, checkpointSeq, latestSeq uint64) error {
	expect := checkpointSeq + 1
	for _, s := range segs {
		if s.lastSeq < expect {
			continue // fully covered by the checkpoint (or empty trailing)
		}
		if s.firstSeq > expect {
			return fmt.Errorf("%w: records %d..%d missing (segment %s starts at %d)",
				ErrPersistedWALGap, expect, s.firstSeq-1, s.file, s.firstSeq)
		}
		expect = s.lastSeq + 1
	}
	if expect-1 < latestSeq {
		return fmt.Errorf("%w: records %d..%d missing (no segment covers them)", ErrPersistedWALGap, expect, latestSeq)
	}
	return nil
}

// persistLockDir takes the exclusive non-blocking flock on
// dir/LOCK, creating the file if needed. The returned file
// must be closed (which releases the lock) by the caller.
func persistLockDir(dir string) (*os.File, error) {
	f, err := os.OpenFile(filepath.Join(dir, "LOCK"), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("%w: %s", ErrDatabaseLocked, dir)
		}
		return nil, err
	}
	return f, nil
}

// persistUnlockDir releases a lock taken by persistLockDir.
// Closing the file releases the flock; the explicit unlock
// keeps the intent readable.
func persistUnlockDir(f *os.File) {
	if f == nil {
		return
	}
	_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	_ = f.Close()
}

// persistCheckpointCopy returns a deep copy of c with its
// Previous link stripped (retention is exactly one
// generation deep) and its WALSegments preserved.
func persistCheckpointCopy(c *persistCheckpoint) *persistCheckpoint {
	if c == nil {
		return nil
	}
	cp := *c
	cp.HeapSHA256 = append([]string(nil), c.HeapSHA256...)
	cp.HeapFiles = append([]string(nil), c.HeapFiles...)
	cp.WALSegments = append([]persistWALSegmentMeta(nil), c.WALSegments...)
	cp.Previous = nil
	return &cp
}

// persistReadCheckpointFile reads and structurally validates
// one checkpoint file (checkpoint.json or
// checkpoint.prev.json). A missing file is reported as
// os.IsNotExist so callers can distinguish "no fallback
// descriptor" from "corrupt fallback descriptor".
func persistReadCheckpointFile(path string) (*persistCheckpoint, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, err
		}
		return nil, fmt.Errorf("%w: read: %v", ErrCorruptCheckpoint, err)
	}
	var c persistCheckpoint
	if err := json.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCorruptCheckpoint, err)
	}
	if err := persistCheckpointValid(&c, 0); err != nil {
		return nil, err
	}
	return &c, nil
}

// persistCheckpointValid checks the structural fields of a
// checkpoint descriptor (magic, version, shard list). Heap
// checksums are validated separately by the caller.
func persistCheckpointValid(c *persistCheckpoint, numShards int) error {
	if c == nil {
		return fmt.Errorf("%w: nil checkpoint", ErrCorruptCheckpoint)
	}
	if c.Magic != persistCheckpointMagic || c.Version != 1 || c.NumShards <= 0 ||
		len(c.HeapSHA256) != c.NumShards {
		return fmt.Errorf("%w: bad magic/version/shard list", ErrCorruptCheckpoint)
	}
	if c.HeapFiles != nil && len(c.HeapFiles) != c.NumShards {
		return fmt.Errorf("%w: heap file list length %d != shards %d", ErrCorruptCheckpoint, len(c.HeapFiles), c.NumShards)
	}
	if numShards > 0 && c.NumShards != numShards {
		return fmt.Errorf("%w: want %d, stored %d", ErrPersistedShardCount, numShards, c.NumShards)
	}
	return nil
}

// persistCheckpointHeapFiles resolves the heap basenames a
// checkpoint descriptor names (generational, derived, or
// legacy), mirroring the resolution in OpenPersistedSegDB.
func persistCheckpointHeapFiles(c *persistCheckpoint) []string {
	if c.HeapFiles != nil {
		return append([]string(nil), c.HeapFiles...)
	}
	out := make([]string, c.NumShards)
	for i := range out {
		if c.Generation > 0 {
			out[i] = persistCheckpointHeapFileName(c.Generation, i)
		} else {
			out[i] = persistLegacyHeapFileName(i)
		}
	}
	return out
}

// persistValidateHeapFiles checks every heap file a
// checkpoint names (size + SHA-256) and returns the
// resolved basenames. Any failure is ErrCorruptHeap.
// The size rule is per-descriptor: a checkpoint's heap
// files must be exactly its recorded HeapSize — except
// unrecorded (pre-track) checkpoints, which accept the
// Standard size or the legacy 4 MiB image (SHA still
// checked over the stored bytes below, as in the Track
// C fix). Only the open path extends a smaller image
// to the loader size; this function only validates, so
// a file larger than its descriptor's size is corrupt
// here, and a file larger than the loader is refused
// again at restore with ErrHeapImageTooLarge.
func persistValidateHeapFiles(dir string, c *persistCheckpoint) ([]string, error) {
	files := persistCheckpointHeapFiles(c)
	recorded := c.HeapSize
	if recorded == 0 {
		recorded = segDBHeapSize
	}
	for i, name := range files {
		heap, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return nil, fmt.Errorf("%w: shard %d (%s): %v", ErrCorruptHeap, i, name, err)
		}
		if uint64(len(heap)) != recorded && !(c.HeapSize == 0 && uint64(len(heap)) == segDBHeapSizeLegacy) {
			return nil, fmt.Errorf("%w: shard %d size %d is neither recorded heap size (%d) nor legacy (%d)", ErrCorruptHeap, i, len(heap), recorded, segDBHeapSizeLegacy)
		}
		sum := sha256Sum(heap)
		if sum != c.HeapSHA256[i] {
			return nil, fmt.Errorf("%w: shard %d checksum mismatch", ErrCorruptHeap, i)
		}
	}
	return files, nil
}

// persistSegmentMetas snapshots the live segment list for a
// checkpoint record.
func persistSegmentMetas(segs []*persistWALSegment) []persistWALSegmentMeta {
	out := make([]persistWALSegmentMeta, 0, len(segs))
	for _, s := range segs {
		out = append(out, persistWALSegmentMeta{File: s.file, FirstSeq: s.firstSeq, LastSeq: s.lastSeq})
	}
	return out
}

// markShardDirtyLocked sets the dirty bit for key's shard.
// Caller holds p.mu. It is called BEFORE the WAL append for
// the mutation, so any failure (poison, wasm failure) leaves
// the bit set; only a definite checkpoint publish clears it.
func (p *PersistedSegDB) markShardDirtyLocked(key string) {
	if len(p.shards) == 0 {
		return
	}
	target := p.shardFor(key)
	for i, sh := range p.shards {
		if sh == target && i < len(p.shardDirty) {
			p.shardDirty[i] = true
		}
	}
}

// currentSegmentLocked returns the live append target (the
// segment with the highest firstSeq), or nil when no
// segment exists. Caller holds p.mu.
func (p *PersistedSegDB) currentSegmentLocked() *persistWALSegment {
	if len(p.walSegs) == 0 {
		return nil
	}
	return p.walSegs[len(p.walSegs)-1]
}

// walPathLocked returns the current segment's path, or the
// legacy wal.log path when no segment exists yet. Caller
// holds p.mu (WALPath takes the lock itself).
func (p *PersistedSegDB) walPathLocked() string {
	if seg := p.currentSegmentLocked(); seg != nil {
		return filepath.Join(p.dir, seg.file)
	}
	return filepath.Join(p.dir, "wal.log")
}

// RecoveredViaFallback reports whether this open recovered
// through a previous-generation checkpoint (Gate 2.2). It
// is false for a normal open and for a single-checkpoint
// database (which has no fallback and fails instead).
func (p *PersistedSegDB) RecoveredViaFallback() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.fallback
}

// FallbackGeneration returns the generation recovered from
// on a fallback open, or 0 when the open was not a fallback.
func (p *PersistedSegDB) FallbackGeneration() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.fallback {
		return 0
	}
	return p.fallbackGen
}

// RecoveryReason describes why a fallback open happened
// (empty for a normal open).
func (p *PersistedSegDB) RecoveryReason() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.fallbackReason
}

// AutoCheckpointError returns the last automatic
// checkpoint/rotation error swallowed by a Put/Delete
// (nil when the last automatic attempt succeeded or none
// ran). An automatic failure never fails the mutation
// itself — the WAL record is already durable — but it is
// not hidden: it is reported here and retried by the next
// automatic trigger or explicit Flush.
func (p *PersistedSegDB) AutoCheckpointError() error {
	p.drainPublish()
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.autoErr
}

// WALSegments returns a snapshot of the live WAL segment
// list (basename, first seq, last seq) for inspection.
// It drains any in-flight background publish (and its
// rotation) first, so a caller right after puts sees
// the rotated list.
func (p *PersistedSegDB) WALSegments() []persistWALSegmentMeta {
	p.drainPublish()
	p.mu.Lock()
	defer p.mu.Unlock()
	return persistSegmentMetas(p.walSegs)
}

// KeepPreviousGeneration reports the retention mode.
func (p *PersistedSegDB) KeepPreviousGeneration() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.keepPrev
}

// MixedCheckpoint reports whether checkpoints reuse the
// previous immutable heap file for unchanged shards.
func (p *PersistedSegDB) MixedCheckpoint() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.mixed
}

// flushLocked flushes every shard inside wasm, then
// publishes the checkpoint in the BACKGROUND (see
// segdb_persist.go). Caller holds p.mu. This is the
// body of automatic checkpoints (auto-checkpoint,
// epoch guard, HistFull retry, tx/global-merge
// retries) — NOT of explicit Flush/Compact/RotateWAL,
// which publish synchronously. The epoch guard resets
// at SNAPSHOT time: the C epoch already ended at the
// wasm flush (hot history cleared), so resetting then
// is correct even if the publish later fails. Publish
// errors land in autoErr and never fail the mutation
// (the auto-checkpoint rule); only wasm-flush and
// snapshot errors return here. A snapshot dropped
// because a publish is in flight (publishPending) is
// picked up by the next trigger's fresh snapshot.
func (p *PersistedSegDB) flushLocked() error {
	return p.flushLockedWithRotate(false)
}

// flushLockedWithRotate is flushLocked with the
// auto-checkpoint rotation flag carried on the snapshot
// BEFORE it is handed to the background publisher (the
// goroutine reads it, so setting it afterwards would
// race). rotateAfter=true is only for
// maybeAutoCheckpointLocked; every other flushLocked
// caller rotates never (explicit RotateWAL rotates
// synchronously in its own wait path).
func (p *PersistedSegDB) flushLockedWithRotate(rotateAfter bool) error {
	if err := p.wasmFlushLocked(); err != nil {
		return err
	}
	snap, err := p.snapshotCheckpointLocked()
	if err != nil {
		return err
	}
	p.epochResetLocked()
	snap.rotateAfter = rotateAfter
	p.publishBackground(snap)
	return nil
}

// maybeAutoCheckpointLocked runs an automatic checkpoint +
// rotation when the current WAL segment reached the
// configured threshold. Caller holds p.mu and has just
// completed a successful mutation. Failures are recorded
// in p.autoErr and never fail the mutation.
func (p *PersistedSegDB) maybeAutoCheckpointLocked() {
	threshold := p.walRotateBytes
	if threshold == 0 {
		threshold = DefaultWALRotateBytes
	}
	if threshold < 0 {
		return // explicitly disabled by the operator
	}
	if p.walSegSize < threshold {
		return
	}
	// Publish runs in the background; rotation runs in
	// the same goroutine after a definite publish only
	// (see publishBackground). Accessors drain the
	// publish, so tests reading Generation/WALSegments/
	// AutoCheckpointError right after puts still
	// observe the rotated state.
	if err := p.flushLockedWithRotate(true); err != nil {
		p.autoErr = err
		return
	}
	p.autoErr = nil
}

// RotateWAL seals the current WAL segment and starts a new
// one at the next sequence, after the latest checkpoint.
// It refuses while poisoned (a poisoned tail must be
// resolved by close + reopen first) and while closed.
// A rotation interrupted at any stage leaves a valid
// directory: the old segment is still parseable and
// appendable, or the new segment is empty. RotateWAL is
// NOT a commit point — the checkpoint publish before it
// is; rotation only changes segment boundaries, so a
// crash mid-rotation replays exactly the same records.
func (p *PersistedSegDB) RotateWAL() error {
	p.publishAcquire()
	defer p.publishRelease()
	p.mu.Lock()
	// PATCH2 Track 2: maintenance fencing, same hold
	// rule as Flush (spans flush + publish + rotate).
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
	// Rotate only over a checkpointed state: flush first so
	// the sealed segment's records are covered by a heap.
	// (An explicit RotateWAL on a never-checkpointed DB
	// still works: the checkpoint it takes has seq = the
	// current seq, and replay from it needs nothing older.)
	// Synchronous like Flush: the publish completes
	// before rotation runs, as before.
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
	if err := p.publishWaitLocked(snap); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.rotateLocked()
}

// rotateLocked seals the current segment and opens a new
// append segment starting at p.seq+1. Caller holds p.mu
// and has just published a checkpoint covering p.seq.
// Fault points: rotate-rename (old segment rename, when
// the current segment is still wal.log), rotate-create
// (new segment creation), rotate-dir-sync (directory sync
// after the switch; on failure the new segment is already
// the in-process truth, mirroring checkpoint-dir-sync).
func (p *PersistedSegDB) rotateLocked() error {
	cur := p.currentSegmentLocked()
	nextSeq := p.seq + 1
	nextFile := fmt.Sprintf("wal-%d.log", nextSeq)
	// Close the old append handle before renaming its file.
	if p.walFile != nil {
		_ = p.walFile.Close()
		p.walFile = nil
	}
	// An empty current segment is already a fresh start:
	// rotating it would either collide with its own
	// successor name (wal.log with seq 0 -> wal-1.log, then
	// create wal-1.log) or create a second empty segment.
	// Rotation is a no-op until the segment holds records.
	if cur != nil && cur.lastSeq < cur.firstSeq {
		if err := p.reopenCurrentSegmentLocked(); err != nil {
			return err
		}
		return nil
	}
	if cur != nil && cur.file == "wal.log" {
		if err := persistFault(persistFaultRotateRename); err != nil {
			// Reopen the old segment for appending: the
			// rename did not happen, state is unchanged.
			if err2 := p.reopenCurrentSegmentLocked(); err2 != nil {
				return err2
			}
			return err
		}
		if err := os.Rename(filepath.Join(p.dir, "wal.log"), filepath.Join(p.dir, nextFileForRename(cur))); err != nil {
			if err2 := p.reopenCurrentSegmentLocked(); err2 != nil {
				return err2
			}
			return err
		}
		cur.file = nextFileForRename(cur)
		// The sealed segment keeps its records; only its
		// name changed. Its list entry stays in place
		// (it is no longer the last segment once the new
		// segment is appended below).
	}
	if err := persistFault(persistFaultRotateCreate); err != nil {
		if err2 := p.reopenCurrentSegmentLocked(); err2 != nil {
			return err2
		}
		return err
	}
	nf, err := os.OpenFile(filepath.Join(p.dir, nextFile), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		if err2 := p.reopenCurrentSegmentLocked(); err2 != nil {
			return err2
		}
		return err
	}
	if p.mode == DurabilitySync {
		if err := nf.Sync(); err != nil {
			nf.Close()
			if err2 := p.reopenCurrentSegmentLocked(); err2 != nil {
				return err2
			}
			return fmt.Errorf("%w: new WAL segment sync: %v", ErrDurabilityUnavailable, err)
		}
	}
	nf.Close()
	// Open the new segment for appending.
	walFile, err := os.OpenFile(filepath.Join(p.dir, nextFile), os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		if err2 := p.reopenCurrentSegmentLocked(); err2 != nil {
			return err2
		}
		return err
	}
	p.walFile = walFile
	p.walSegs = append(p.walSegs, &persistWALSegment{file: nextFile, firstSeq: nextSeq, lastSeq: nextSeq - 1, size: 0})
	p.walSegSize = 0
	if p.mode == DurabilitySync {
		if err := persistSyncDir(p.dir); err != nil {
			// Mirror checkpoint-dir-sync: the switch
			// already happened in-process; report the
			// durability failure without rolling back.
			return fmt.Errorf("%w: directory sync after WAL rotation: %v", ErrDurabilityUnavailable, err)
		}
	}
	if err := persistFault(persistFaultRotateDirSync); err != nil {
		return err
	}
	// Segment reclaim runs after the switch (never before):
	// only now is the sealed segment no longer the append
	// target, so deleting it (when covered) cannot race an
	// append.
	p.reclaimSegmentsLocked()
	return nil
}

// nextFileForRename returns the sealed name for the legacy
// wal.log segment: wal-<firstSeq>.log.
func nextFileForRename(seg *persistWALSegment) string {
	return fmt.Sprintf("wal-%d.log", seg.firstSeq)
}

// reopenCurrentSegmentLocked reopens the append handle on
// the current (last) segment after a failed rotation step.
// Caller holds p.mu. It is used only on rotation failure
// paths, where the segment list is unchanged.
func (p *PersistedSegDB) reopenCurrentSegmentLocked() error {
	cur := p.currentSegmentLocked()
	if cur == nil {
		// No segment existed: create the legacy segment.
		f, err := os.OpenFile(filepath.Join(p.dir, "wal.log"), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			return err
		}
		p.walFile = f
		p.walSegs = []*persistWALSegment{{file: "wal.log", firstSeq: 1, lastSeq: p.seq, size: p.walSegSize}}
		return nil
	}
	f, err := os.OpenFile(filepath.Join(p.dir, cur.file), os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	p.walFile = f
	return nil
}

// reclaimSegmentsLocked deletes sealed WAL segments fully
// covered by a retained checkpoint, per the retention rule
// in the file header. Caller holds p.mu, after a definite
// checkpoint publish. The current append segment is never
// deleted. Failures are ignored (extra segments are
// harmless; they are re-examined at the next reclaim).
func (p *PersistedSegDB) reclaimSegmentsLocked() {
	if p.curCkpt == nil {
		return
	}
	floor := p.curCkpt.CheckpointSeq
	if p.keepPrev && p.prevCkpt != nil {
		floor = p.prevCkpt.CheckpointSeq
	}
	cur := p.currentSegmentLocked()
	var kept []*persistWALSegment
	for _, s := range p.walSegs {
		if s != cur && s.lastSeq <= floor {
			_ = os.Remove(filepath.Join(p.dir, s.file))
			continue
		}
		kept = append(kept, s)
	}
	p.walSegs = kept
}

// reclaimHeapFilesLocked deletes heap image files and stale
// tmp files not named by keep (caller holds p.mu). It runs
// ONLY after a checkpoint publish is definite. Failures are
// ignored: extra files are harmless orphans, and failing
// the checkpoint after its commit point would misreport
// success as failure. The LOCK file and checkpoint.prev.json
// are never touched here.
func (p *PersistedSegDB) reclaimHeapFilesLocked(keep []string) {
	keepSet := map[string]bool{}
	for _, name := range keep {
		keepSet[name] = true
	}
	entries, err := os.ReadDir(p.dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		name := e.Name()
		if keepSet[name] {
			continue
		}
		// Generational + legacy heap images, and tmp files
		// from interrupted writes (heap-*.tmp / checkpoint
		// tmp). Never touch wal.log, WAL segments,
		// checkpoint.json, checkpoint.prev.json, or LOCK.
		if (len(name) > 5 && name[:5] == "heap-" && (filepath.Ext(name) == ".bin" || filepath.Ext(name) == ".tmp")) ||
			name == "checkpoint.json.tmp" {
			_ = os.Remove(filepath.Join(p.dir, name))
		}
	}
}
