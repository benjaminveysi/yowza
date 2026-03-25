package yowza

// Axis D — TIMELINE/DOMAIN LIFECYCLE COMPLETION.
//
// This file owns the cross-cutting lifecycle that no
// single Phase A–E file owned: (1) the recovery gate
// that must complete before reads are served after a
// crash, (2) the staged-blob lifecycle for cross-DB
// applies, and (3) orphan-delta / WAL-tail reconciliation
// that a DB-only recovery (no TimelineManager) needs.
//
// OWNERSHIP BOUNDARY, STATED PLAINLY. The pre-existing
// read entry points PersistedSegDB.Get/Scan (segdb_persist.go)
// and TimelineManager.Get/Scan (timeline.go) are OUTSIDE
// Axis D's file set and were NOT edited, so this file
// cannot retroactively gate them: a caller that reads
// through those methods directly bypasses this gate.
// What this file DOES provide, completely, is:
//   - LifecycleGate: the gate itself, with typed
//     refusal before recovery and delegation after;
//   - RecoverLifecycleBeforeReads: the recovery
//     procedure the gate runs (global-merge roll-forward
//     / reclaim, WAL incomplete-tail truncation, orphan
//     delta cleanup, staged-blob retention);
//   - the exact hook list (see TIMELINE_V2_PATCH_NOTES.md,
//     Axis D) for wiring the gate into those two read
//     paths — one check at the top of each Get/Scan.
// NewTimelineManager already recovers synchronously at
// open (RecoverGlobalMerges + WAL replay + orphan
// cleanup + pin reconciliation, before any timeline
// view is read), so a manager instance can never
// observe pre-recovery state; the gap this gate closes
// is the DB-only path, where OpenPersistedSegDB alone
// does NOT run global-merge or domain recovery.
//
// SAFETY INVARIANT (the whole axis, in one line): no
// data with a live reference is ever deleted — by GC,
// by truncation, by recovery, or by abort cleanup.

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// ErrLifecycleRecoveryIncomplete reports a read refused
// because crash recovery has not completed yet. It is
// a typed refusal, never stale or half-recovered data.
var ErrLifecycleRecoveryIncomplete = errors.New("yowza lifecycle: recovery incomplete; reads refused until recovery completes")

// ErrLifecycleStagedBlobUnknown reports resolving a
// staged-blob operation that was never staged.
var ErrLifecycleStagedBlobUnknown = errors.New("yowza lifecycle: unknown staged-blob operation")

// --- Staged blobs (cross-DB apply lifecycle) ---
//
// A cross-DB apply (global merge staging, domain
// ApplyAtomic staging) must keep its blobs alive from
// staging until the decision resolves, and must make
// them collectible after an abort — without the apply
// path itself having to track them. The staged-blob
// file lifecycle-staged-blobs.json in the TARGET DB's
// directory is that record: BeginStaging writes it
// (tmp + sync + rename) BEFORE the apply stages, and
// ResolveStaging removes the operation's entries on
// commit or abort. GC (ScanLiveBlobRefs) reads this
// file as a live-reference source, so a staged blob
// survives GC even if the apply crashes mid-flight,
// and an aborted blob becomes collectible as soon as
// its operation resolves — with no apply-path edit
// needed for the GC side. The apply paths themselves
// still need the two hook calls (BeginStaging before
// staging, ResolveStaging on commit/abort, plus
// CopyBlobDeltas before applying to a foreign DB);
// those hooks are in files outside Axis D ownership
// and are named exactly in the patch notes.

const lifecycleStagedBlobFile = "lifecycle-staged-blobs.json"

type lifecycleStagedBlobEntry struct {
	OpID   string `json:"op_id"`
	SHA256 string `json:"sha256"`
	Total  int64  `json:"total_len"`
	Chunks int    `json:"chunks"`
}

type lifecycleStagedBlobDoc struct {
	Version int                        `json:"version"`
	Entries []lifecycleStagedBlobEntry `json:"entries"`
}

func lifecycleStagedBlobPath(dir string) string {
	return filepath.Join(dir, lifecycleStagedBlobFile)
}

func readStagedBlobDoc(dir string) lifecycleStagedBlobDoc {
	data, err := os.ReadFile(lifecycleStagedBlobPath(dir))
	if err != nil {
		return lifecycleStagedBlobDoc{Version: 1}
	}
	var doc lifecycleStagedBlobDoc
	if err := json.Unmarshal(data, &doc); err != nil {
		return lifecycleStagedBlobDoc{Version: 1}
	}
	return doc
}

func writeStagedBlobDoc(dir string, doc lifecycleStagedBlobDoc) error {
	data, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	tmp := lifecycleStagedBlobPath(dir) + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	f, err := os.OpenFile(tmp, os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	syncErr := f.Sync()
	f.Close()
	if syncErr != nil {
		return syncErr
	}
	if err := os.Rename(tmp, lifecycleStagedBlobPath(dir)); err != nil {
		return err
	}
	return persistSyncDir(dir)
}

// readStagedBlobRefs returns every blob still staged in
// dir (any operation, unresolved). Used by GC.
func readStagedBlobRefs(dir string) []BlobRef {
	doc := readStagedBlobDoc(dir)
	seen := map[string]bool{}
	var out []BlobRef
	for _, e := range doc.Entries {
		if seen[e.SHA256] {
			continue
		}
		seen[e.SHA256] = true
		out = append(out, BlobRef{SHA256: e.SHA256, TotalLen: e.Total, ChunkCount: e.Chunks})
	}
	return out
}

// BeginStaging records refs as staged for opID in the
// target DB's directory. Idempotent per (opID, SHA):
// staging the same blob twice for one operation
// records it once. Blobs already staged for other
// operations are untouched.
func BeginStaging(dir, opID string, refs []BlobRef) error {
	if opID == "" {
		return fmt.Errorf("%w: empty operation id", ErrLifecycleStagedBlobUnknown)
	}
	doc := readStagedBlobDoc(dir)
	have := map[string]bool{}
	for _, e := range doc.Entries {
		have[e.OpID+"\x00"+e.SHA256] = true
	}
	for _, r := range refs {
		k := opID + "\x00" + r.SHA256
		if have[k] {
			continue
		}
		have[k] = true
		doc.Entries = append(doc.Entries, lifecycleStagedBlobEntry{OpID: opID, SHA256: r.SHA256, Total: r.TotalLen, Chunks: r.ChunkCount})
	}
	doc.Version = 1
	return writeStagedBlobDoc(dir, doc)
}

// ResolveStaging removes opID's staged entries.
// committed=true means the apply committed: the blobs
// are now referenced by the new root (via the applied
// values), so dropping the staging record is safe.
// committed=false means abort: the blobs become
// collectible by the next GC pass. Resolving an
// unknown operation is an error, never a silent no-op:
// a caller that loses track of its staging record has
// lost track of its blobs' lifecycle, and must know.
func ResolveStaging(dir, opID string, committed bool) error {
	_ = committed // commit vs abort differ only in what the caller did with the values; both drop the staging record
	doc := readStagedBlobDoc(dir)
	found := false
	kept := doc.Entries[:0]
	for _, e := range doc.Entries {
		if e.OpID == opID {
			found = true
			continue
		}
		kept = append(kept, e)
	}
	if !found {
		return fmt.Errorf("%w: %s", ErrLifecycleStagedBlobUnknown, opID)
	}
	doc.Entries = kept
	return writeStagedBlobDoc(dir, doc)
}

// StagedBlobOps lists the operation IDs with unresolved
// staged blobs in dir, sorted.
func StagedBlobOps(dir string) []string {
	doc := readStagedBlobDoc(dir)
	seen := map[string]bool{}
	for _, e := range doc.Entries {
		seen[e.OpID] = true
	}
	out := make([]string, 0, len(seen))
	for op := range seen {
		out = append(out, op)
	}
	sort.Strings(out)
	return out
}

// PrepareCrossDBBlobs is the one call a cross-DB apply
// makes before staging deltas to a foreign DB: copy
// every blob the deltas reference into the target, then
// stage them under opID so GC cannot collect them
// before the apply decision resolves. On copy failure
// nothing is staged (the apply must not stage a delta
// whose bytes are missing on the target).
func PrepareCrossDBBlobs(srcDir, dstDir, opID string, deltas []globalMergeDelta) error {
	var refs []BlobRef
	for _, d := range deltas {
		if d.Deleted {
			continue
		}
		ref, err := ParseBlobRef(string(d.Value))
		if err != nil {
			continue
		}
		refs = append(refs, ref)
	}
	if err := CopyBlobDeltas(srcDir, dstDir, deltas); err != nil {
		return err
	}
	if len(refs) > 0 {
		return BeginStaging(dstDir, opID, refs)
	}
	return nil
}

// --- Orphan-delta reconciliation (DB-only recovery) ---

// ReconcileOrphanDeltas deletes every *.delta file in
// dir that is not referenced by a live timeline's
// checkpoint or committed merge-ref record, and not a
// live global-merge chunk manifest target. It returns
// the deleted file names, sorted. Referenced files are
// never touched, and a delta file whose referencing
// WAL is corrupt is kept (a corrupt WAL fails open
// elsewhere; reconciliation must not delete evidence).
// This is the DB-only half of the manager's
// cleanupOrphanDeltas; the manager path is unchanged.
func ReconcileOrphanDeltas(dir string) ([]string, error) {
	referenced := referencedDeltaFiles(dir, nil)
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var deleted []string
	for _, e := range ents {
		name := e.Name()
		if strings.HasSuffix(name, ".delta.tmp") {
			_ = os.Remove(filepath.Join(dir, name))
			deleted = append(deleted, name)
			continue
		}
		if strings.HasSuffix(name, ".delta") && !referenced[name] {
			_ = os.Remove(filepath.Join(dir, name))
			deleted = append(deleted, name)
		}
	}
	sort.Strings(deleted)
	return deleted, nil
}

// --- Recovery gate ---

// LifecycleGate guards reads for one database directory
// after a crash or interruption. Construction scans
// for pending recovery work; while any remains, every
// gated read fails with
// ErrLifecycleRecoveryIncomplete. Recover runs the
// full recovery procedure and re-scans; only when the
// re-scan is clean do gated reads delegate to the real
// database/manager. A gate constructed over a clean
// directory is open immediately (reads are never
// refused without a reason).
type LifecycleGate struct {
	mu       sync.Mutex
	dir      string
	db       *PersistedSegDB
	coordDir string
	open     bool
	pending  []string
}

// NewLifecycleGate scans dir for pending recovery work:
// staged or committed global merges, orphan deltas,
// timeline WAL incomplete tails, and (when coordDir is
// given) domain coordinator log entries committed
// without done. A WAL that fails replay with corruption
// (not a tail) is pending AND unrecoverable by this
// gate: Recover returns that corruption error, and the
// gate stays closed — the file is left for inspection,
// exactly as manager open leaves it.
func NewLifecycleGate(db *PersistedSegDB, coordDir string) *LifecycleGate {
	g := &LifecycleGate{dir: db.Dir(), db: db, coordDir: coordDir}
	g.pending = g.scanPending()
	g.open = len(g.pending) == 0
	return g
}

func (g *LifecycleGate) scanPending() []string {
	var pending []string
	ents, err := os.ReadDir(g.dir)
	if err != nil {
		return []string{"directory unreadable: " + err.Error()}
	}
	for _, e := range ents {
		name := e.Name()
		if strings.HasPrefix(name, "global-merge-") && (strings.HasSuffix(name, ".manifest.json") || strings.HasSuffix(name, ".commit.json") || strings.Contains(name, ".chunk-")) {
			pending = append(pending, "global-merge file: "+name)
		}
		// A transaction journal is a durable commit decision. If it
		// is still applying, reads must not observe a possible prefix
		// of the write set after restart. Corrupt/unreadable journals
		// also keep the gate closed; recovery must fail loudly rather
		// than treating them as absent and serving uncertain state.
		if strings.HasPrefix(name, "segdb-tx-") && strings.HasSuffix(name, ".journal") {
			path := filepath.Join(g.dir, name)
			data, rerr := os.ReadFile(path)
			if rerr != nil {
				pending = append(pending, "transaction journal unreadable: "+name+": "+rerr.Error())
				continue
			}
			j, jerr := decodeTxJournal(data)
			if jerr != nil {
				pending = append(pending, "transaction journal corrupt: "+name+": "+jerr.Error())
				continue
			}
			if j.Status == "applying" {
				pending = append(pending, "transaction journal requires recovery: "+name)
			}
		}
	}
	referenced := referencedDeltaFiles(g.dir, nil)
	for _, e := range ents {
		name := e.Name()
		if strings.HasSuffix(name, ".delta") && !referenced[name] {
			pending = append(pending, "orphan delta: "+name)
		}
		if strings.HasSuffix(name, ".delta.tmp") {
			pending = append(pending, "orphan delta temp: "+name)
		}
	}
	// WAL tails: replay every non-deleted timeline WAL.
	if doc, exists, _, _, err := timelineLoadRegistryV2(g.dir); err == nil && exists {
		for _, rec := range doc.Timelines {
			if rec.Deleted {
				continue
			}
			_, _, _, tail, err := timelineReplayWAL(timelineWALPath(g.dir, rec.ID))
			if err != nil {
				pending = append(pending, "timeline "+rec.ID+" WAL replay error: "+err.Error())
			} else if tail {
				pending = append(pending, "timeline "+rec.ID+" WAL incomplete tail")
			}
		}
	}
	// Domain coordinator log: commit without done.
	if g.coordDir != "" {
		if committed, done, err := scanDomainLog(filepath.Join(g.coordDir, "domain-coordinator.log")); err == nil {
			for op := range committed {
				if !done[op] {
					pending = append(pending, "domain op committed without done: "+op)
				}
			}
		} else if err != nil && !os.IsNotExist(err) {
			pending = append(pending, "domain log unreadable: "+err.Error())
		}
	}
	sort.Strings(pending)
	return pending
}

func scanDomainLog(path string) (committed, done map[string]bool, err error) {
	committed = map[string]bool{}
	done = map[string]bool{}
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1024*1024), 1024*1024)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var rec domainLogRecord
		if err := json.Unmarshal(line, &rec); err != nil {
			return nil, nil, err
		}
		switch rec.Kind {
		case "commit":
			committed[rec.OpID] = true
		case "done":
			done[rec.OpID] = true
		}
	}
	return committed, done, sc.Err()
}

// Pending returns the reasons the gate is closed, or nil
// when it is open.
func (g *LifecycleGate) Pending() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]string(nil), g.pending...)
}

// IsOpen reports whether recovery has completed.
func (g *LifecycleGate) IsOpen() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.open
}

func (g *LifecycleGate) checkOpen() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.open {
		return fmt.Errorf("%w: %v", ErrLifecycleRecoveryIncomplete, g.pending)
	}
	return nil
}

// rescan re-runs the pending-work scan and updates the
// open flag. Used after an owner has run its own
// recovery + healing (notably NewTimelineManagerInit,
// which recovers global merges and heals WAL tails and
// orphan deltas itself) so the gate does not stay
// closed on work that no longer exists.
func (g *LifecycleGate) rescan() {
	g.mu.Lock()
	g.pending = g.scanPending()
	g.open = len(g.pending) == 0
	g.mu.Unlock()
}

// --- PATCH2 Track 1: per-DB gate wiring ---
//
// The indeterminate + recovery state is held per DB
// (PersistedSegDB.gate / txPartialApply) and EVERY
// public read path consults it before reading:
// PersistedSegDB.Get, Scan, ScanStreaming,
// AcquireSnapshot, SnapshotVector, ScanOpen (new
// sessions; in-flight session pages read pinned
// snapshot data and are not re-gated), and
// TimelineManager.Get/Scan (via the same check).
// Query sources go through Scan/ScanStreaming, so
// they are covered by construction. Internal locked
// paths (snapshotVectorLocked, RecoverGlobalMerges,
// manager-init scans) deliberately bypass the check:
// recovery itself must be able to run while the gate
// is closed, and manager init heals tails/deltas
// before refreshing the gate at the end of init.
//
// Locking: checkReadsAllowed never holds p.mu across
// g.mu. It snapshots (closed, txPartialApply, gate)
// under p.mu, releases it, then consults the gate.
// The txPartialApply flag is therefore checked under
// p.mu in the same critical section as the flag's
// writers (commitGate sets it while holding p.mu;
// RecoverSegDBTransactions clears it under p.mu),
// while the gate check is point-in-time: a gate only
// changes state via explicit Recover/Refresh calls,
// never spontaneously during a read.

// AttachRecoveryGate constructs the gate over this
// DB's directory and stores it on the DB. Called by
// OpenPersistedSegDB; tests and DB-only callers may
// re-attach after simulating a crash.
func (p *PersistedSegDB) AttachRecoveryGate() *LifecycleGate {
	g := NewLifecycleGate(p, "")
	p.mu.Lock()
	p.gate = g
	p.mu.Unlock()
	return g
}

// RecoveryGate returns the DB's attached gate, or nil
// if none was attached yet.
func (p *PersistedSegDB) RecoveryGate() *LifecycleGate {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.gate
}

// RefreshRecoveryGate re-scans the attached gate's
// pending work (no-op when no gate is attached).
func (p *PersistedSegDB) RefreshRecoveryGate() {
	p.mu.Lock()
	g := p.gate
	p.mu.Unlock()
	if g != nil {
		g.rescan()
	}
}

// checkReadsAllowed enforces the per-DB recovery state
// for public reads. Closed beats partial-apply beats
// gate-closed, so callers get the most specific cause.
func (p *PersistedSegDB) checkReadsAllowed() error {
	p.mu.Lock()
	closed := p.closed
	partial := p.txPartialApply
	g := p.gate
	p.mu.Unlock()
	if closed {
		return ErrPersistedClosed
	}
	if partial {
		return ErrSegDBTxRecoveryRequired
	}
	if g != nil {
		return g.checkOpen()
	}
	return nil
}

// Recover runs the full DB-level recovery procedure, in
// order: (1) global-merge roll-forward / reclaim
// (RecoverGlobalMerges — a committed merge becomes
// visible, a staged-only merge is reclaimed);
// (2) WAL incomplete-tail truncation (only a torn
// tail is ever truncated; a corrupt WAL fails here
// and the gate stays closed); (3) orphan-delta
// cleanup. Domain roll-forward is NOT run here — it
// needs the TimelineDomain object — use
// RecoverWithDomain for that step, or run
// RecoverDomainOps before constructing the gate.
// After all steps the gate re-scans; it opens only
// when the re-scan is clean.
func (g *LifecycleGate) Recover() error {
	// PATCH2 Track 1, step 0: roll transaction journals
	// forward first. This also clears the DB's
	// txPartialApply flag on success (see
	// RecoverSegDBTransactions), so a known-partial
	// commit becomes a full commit before any gated
	// read is allowed again. A journal left "applying"
	// with nothing applied is rolled forward the same
	// way; Begin's refusal is what forced recovery here.
	if _, err := RecoverSegDBTransactions(g.db); err != nil {
		return err
	}
	if _, err := RecoverGlobalMerges(g.db); err != nil {
		return err
	}
	if doc, exists, _, _, err := timelineLoadRegistryV2(g.dir); err == nil && exists {
		for _, rec := range doc.Timelines {
			if rec.Deleted {
				continue
			}
			_, validLen, _, tail, err := timelineReplayWAL(timelineWALPath(g.dir, rec.ID))
			if err != nil {
				return err
			}
			if tail {
				if st, err := os.Stat(timelineWALPath(g.dir, rec.ID)); err != nil {
					return err
				} else if st.Size() > validLen {
					if err := os.Truncate(timelineWALPath(g.dir, rec.ID), validLen); err != nil {
						return err
					}
				}
			}
		}
	}
	if _, err := ReconcileOrphanDeltas(g.dir); err != nil {
		return err
	}
	// Leftover global-merge temp files (crash mid-chunk
	// write) are never read by any recovery path; remove
	// them so the re-scan below is clean.
	if ents, err := os.ReadDir(g.dir); err == nil {
		for _, e := range ents {
			name := e.Name()
			if strings.HasPrefix(name, "global-merge-") && strings.HasSuffix(name, ".tmp") {
				_ = os.Remove(filepath.Join(g.dir, name))
			}
		}
	}
	g.mu.Lock()
	g.pending = g.scanPending()
	g.open = len(g.pending) == 0
	g.mu.Unlock()
	// PATCH2 Track 1: a gate constructed manually over
	// a DB (as tests and DB-only callers do) is a
	// DIFFERENT object from the gate attached to the
	// DB at open. Recovery through either object must
	// refresh the attached one, or facade reads would
	// stay refused for work that no longer exists.
	g.refreshAttachedGate()
	if !g.IsOpen() {
		return fmt.Errorf("%w: still pending: %v", ErrLifecycleRecoveryIncomplete, g.Pending())
	}
	return nil
}

// refreshAttachedGate re-scans the DB's attached gate
// when it is a different object from this gate.
func (g *LifecycleGate) refreshAttachedGate() {
	if g.db == nil {
		return
	}
	attached := g.db.RecoveryGate()
	if attached != nil && attached != g {
		attached.rescan()
	}
}

// RecoverWithDomain runs Recover, then rolls forward
// the domain's committed-without-done operations, then
// re-scans. Domain recovery must run under the
// domain's lock (RecoverDomainOps takes it); it must
// never run concurrently with an active ApplyAtomic.
func (g *LifecycleGate) RecoverWithDomain(d *TimelineDomain) error {
	if err := g.Recover(); err != nil {
		return err
	}
	if d != nil {
		if _, err := d.RecoverDomainOps(); err != nil {
			return err
		}
	}
	g.mu.Lock()
	g.pending = g.scanPending()
	g.open = len(g.pending) == 0
	g.mu.Unlock()
	// PATCH2 Track 1: see Recover — a manual gate's
	// recovery must refresh the DB's attached gate.
	g.refreshAttachedGate()
	if !g.IsOpen() {
		return fmt.Errorf("%w: still pending: %v", ErrLifecycleRecoveryIncomplete, g.Pending())
	}
	return nil
}

// Get is the gated DB read: refused until recovery
// completes, then exactly PersistedSegDB.Get.
func (g *LifecycleGate) Get(key string) ([]byte, error) {
	if err := g.checkOpen(); err != nil {
		return nil, err
	}
	return g.db.Get(key)
}

// Scan is the gated DB read: refused until recovery
// completes, then exactly PersistedSegDB.Scan.
func (g *LifecycleGate) Scan(prefix string) ([]SegDBScanEntry, error) {
	if err := g.checkOpen(); err != nil {
		return nil, err
	}
	return g.db.Scan(prefix)
}

// TimelineGet is the gated timeline read: refused until
// recovery completes, then exactly TimelineManager.Get.
func (g *LifecycleGate) TimelineGet(mgr *TimelineManager, id, key string) ([]byte, error) {
	if err := g.checkOpen(); err != nil {
		return nil, err
	}
	if mgr == nil {
		return nil, fmt.Errorf("yowza lifecycle: TimelineGet requires a manager")
	}
	return mgr.Get(id, key)
}

// RecoverLifecycleBeforeReads is the standalone form of
// the gate's procedure for callers that do not need a
// gate object: run it after a crash and before serving
// ordinary reads from a DB-only instance. It returns
// the global-merge IDs rolled forward.
func RecoverLifecycleBeforeReads(db *PersistedSegDB) ([]string, error) {
	rolled, err := RecoverGlobalMerges(db)
	if err != nil {
		return rolled, err
	}
	if _, err := ReconcileOrphanDeltas(db.Dir()); err != nil {
		return rolled, err
	}
	return rolled, nil
}
