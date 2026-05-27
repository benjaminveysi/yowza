package yowza

// Phase C, C1 — UNIFIED SegDBTransaction contract.
//
// ONE mutation layer for SegDB that all supported Go
// paths pass through. The problem it closes: before
// Phase C, SegDB mutations arrived through several
// paths with different guarantees — the PersistedSegDB
// facade (WAL + per-op durability), the C tx commit
// path (history hook, but a delete of a hot-absent
// segment-only key left no tombstone, so the old
// segment value resurfaced), VM txs on vm->tx (no
// SegDB history at all), tx_replay_wal (its own
// interpreter of PUT/DELETE), and direct ART callers
// (no history, no WAL). This file does not pretend
// to rewire the C VM or to give PersistedSegDB a
// multi-operation WAL frame it does not have (see
// the honest limits below); it gives the Go layer
// one transaction adapter with one set of rules, and
// the C layer one matching rule change (tx.c: a tx
// delete of a hot-absent key now creates a shadowing
// tombstone, exactly as segdb_delete_absent does —
// so commit, frame replay, and the SegDB facade agree
// on what DELETE means).
//
// CONTRACT (SegDBTransaction):
//   - Begin captures an owned snapshot (Phase B
//     AcquireSnapshot): Get reads a layered view at
//     that snapshot — the snapshot's value, overlaid
//     by this transaction's own write set.
//   - Put/Delete write ONLY to a private write set
//     (last write per key wins; the write set is
//     ordered by first-write order, applied in key
//     order at commit for determinism).
//   - Assert/Unique + validation run on the EFFECTIVE
//     state, not just hot ART: asserts are checked
//     against the snapshot + write set at assert time,
//     and RE-CHECKED at commit against the LIVE state
//     (so an intervening commit that changes the key
//     fails the transaction before anything is
//     applied). A Unique Put fails at commit if the
//     key exists in live committed state then.
//   - Commit records needed history + tombstone
//     through the facade's LOCKED attempt path
//     (applyTxWriteLocked, the db.mu twin of
//     PersistedSegDB.Put/Delete — same epoch guard,
//     history-full retry, WAL, hot tombstone
//     including the segment-only shadow tombstone
//     via C segdb_delete, and flush-time history
//     transfer). A tx Delete of a segment-only
//     key therefore produces the tombstone that keeps
//     the old segment value from resurfacing — never
//     an empty art_delete left to chance. The public
//     Put/Delete wrappers are NOT used inside a
//     commit: they re-take db.mu and would deadlock
//     under the commit gate.
//   - Commit gate (Axis A): validation, journal
//     recording, and application of ALL writes run
//     under db.mu, so live readers and writers see
//     the old state before the gate and the full new
//     state after it, never a stepwise prefix. This
//     is in-process visibility atomicity; crash
//     atomicity is still journal + roll-forward
//     (see HONEST LIMITS), not a single WAL frame.
//   - Commit journal: before applying anything, the
//     full write set is written to a journal file
//     (segdb-tx-<id>.journal: canonical encoding +
//     CRC-32, fsynced, directory-synced in Sync
//     mode — a directory-sync failure in Sync mode
//     is an indeterminate error, never ignored).
//     The journal is the commit record: if the
//     process crashes mid-apply,
//     RecoverSegDBTransactions re-applies the whole
//     journal (roll-forward — the journal was durable
//     before any apply, so roll-forward IS the atomic
//     outcome). In-process apply failure triggers
//     compensating rollback of ONLY the applied
//     prefix to captured pre-images and marks the
//     journal aborted; a rollback failure leaves the
//     journal "applying" for roll-forward instead.
//   - Error separation: ONLY ErrSegDBNotFound
//     converts to "absent" anywhere in this file
//     (live validation, unique checks, pre-images).
//     Every other Get error propagates and aborts
//     the commit before any journal is written.
//     Snapshot reads propagate likewise, except the
//     snapshot layer's untyped not-found message
//     (see effectiveLocked). The residual C-level
//     rc=-1 conflation (absent vs segment lookup
//     error) is documented at ErrSegDBNotFound in
//     segdb.go and is NOT claimed fixed here.
//   - Rollback discards the write set, closes the
//     snapshot handle, and removes any journal. It
//     rolls back state, history, and temp writes:
//     nothing was ever written to the facade before
//     commit, so there is no history to undo on
//     rollback — and the journal is the only temp
//     write, deleted by Rollback.
//   - Snapshots that must survive restart need
//     DURABLE history: a transaction's snapshot
//     handle is in-memory leases, but committing
//     through the facade + Flush moves history into
//     versioned segments, and a durable pin
//     (PinSnapshot / CreateDurablePin, Phase B) pins
//     the snapshot's vector across restarts. A mere
//     in-memory hook at commit is NOT sufficient,
//     and this layer does not claim it is: durability
//     of a pre-commit snapshot is the pin's job,
//     documented at PinSnapshotForTx below.
//
// SEPARATION (raw ART vs DB guarantees): art_insert /
// art_delete are structural primitives of the C ART;
// snapshot + WAL guarantees belong to the DB level
// (PersistedSegDB facade + SegDB C layer). The Go
// public database API exposes ONLY the facade
// (PersistedSegDB.Put/Delete/Get, SegDB.Put/Delete/
// Get, and this transaction layer); there is no Go
// export that mutates the ART directly. C direct
// art_* callers remain low-level primitives by
// construction (they are not a database API), and
// tx.h documents the buffering/apply exception rule.
//
// SINGLE DECODER (Go side): PersistedSegDB WAL frames
// are decoded ONLY by persistParseWALSegmented
// (segdb_persist.go) — this file does not add a
// second WAL interpreter. Transaction journals are a
// different file format (JSON + CRC), decoded ONLY
// by decodeTxJournal, and applied ONLY through
// applyTxWritesLocked, the same apply path Commit uses — so
// a journal replay and a live commit cannot disagree
// about what a write set means. The C side's single
// frame decoder is replay_frame in tx.c, now with
// the same shadow-tombstone delete semantics as
// tx_apply (see tx.h).
//
// HONEST LIMITS (what this file does NOT do):
//   - Multi-key atomicity at the PersistedSegDB level
//     is (a) in-process visibility atomicity via the
//     db.mu commit gate, plus (b) crash atomicity via
//     journal + roll-forward, NOT a single WAL
//     frame: PersistedSegDB has no multi-operation
//     frame. A crash mid-apply leaves a partial state
//     UNTIL RecoverSegDBTransactions runs
//     (roll-forward completes it); callers that need
//     crash-atomic transactions must run recovery
//     before serving reads after a crash. This is
//     stated here, not hidden behind the word
//     "atomic". Relatedly, this file cannot block
//     ordinary PersistedSegDB.Get reads after an
//     indeterminate commit inside the same process:
//     Get lives in segdb_persist.go (owned by
//     another axis) and has no hook for a
//     transaction-indeterminate flag, so the
//     read-side enforcement is a caller obligation
//     (run recovery first), enforced for NEW
//     transactions by Begin's pending-journal check
//     but not for raw facade reads. That missing
//     hook is recorded as a remaining item in
//     TIMELINE_V2_PATCH_NOTES.md (Axis A section).
//     Snapshot-handle reads during a gate are
//     isolated by sequence (they read their own
//     snapshot, not live state), so they cannot
//     observe a half-applied live set either.
//   - VM/wasm program txs (OP_TX_BEGIN/COMMIT on
//     vm->tx) do NOT pass through this layer: the VM
//     owns its own ART, not a PersistedSegDB, and no
//     SegDB context exists at OP_TX_COMMIT in vm.c.
//     What DID change for VM txs is the tx layer
//     itself (tx.c shadow tombstones), so a VM tx
//     delete now has the same ART-level semantics —
//     but VM txs still do not touch SegDB history,
//     snapshots, or the PersistedSegDB WAL. Wiring
//     the VM to a SegDB context is a separate,
//     larger change (a SegDB-backed VM mode), not
//     claimed here.
//   - Direct C art_insert/art_delete callers are
//     unchanged primitives: they are not transactions
//     and carry no history/WAL guarantee. Callers
//     that need history must use the facade or a
//     SegDBTransaction, not raw ART calls.
//   - Legacy per-op records in tx_replay_wal keep
//     their old no-op delete (see tx.c): only tx
//     FRAMES share the unified shadow semantics.
//     Legacy records predate frames and are replayed
//     by SegDB's own delete_absent record path at
//     the SegDB level, which is the unified path
//     there.

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// ErrSegDBTxNotFound is returned by SegDBTransaction.Get
// for a key absent from the layered view (snapshot +
// write set), including keys tombstoned by this tx.
var ErrSegDBTxNotFound = errors.New("yowza segdb tx: key not found in transaction view")

// ErrSegDBTxNotActive is returned by any operation on a
// committed, rolled-back, or failed transaction.
var ErrSegDBTxNotActive = errors.New("yowza segdb tx: transaction is not active")

// ErrSegDBTxAssert is returned when an assert or unique
// check fails (at assert time or at commit re-check).
var ErrSegDBTxAssert = errors.New("yowza segdb tx: assert or uniqueness check failed")

// ErrSegDBTxRecoveryRequired is returned by
// BeginSegDBTransaction when an "applying" journal from an
// earlier commit is still pending in the database
// directory. That journal is a commit whose outcome is not
// yet decided in this process (crash or indeterminate
// failure after the journal was written): starting a new
// transaction before recovery could interleave with the
// roll-forward, so Begin refuses until the caller runs
// RecoverSegDBTransactions. "aborted" journals do not
// block Begin: their rollback already completed.
var ErrSegDBTxRecoveryRequired = errors.New("yowza segdb tx: applying journal pending; run RecoverSegDBTransactions before starting a transaction")

// ErrSegDBTxIndeterminate is returned when a commit
// failed AFTER its journal was durably written (or after
// a directory sync failed), so the commit's outcome is
// not decided by the return value: the journal is left
// "applying" and recovery roll-forward decides it (full
// commit). The transaction object is marked indeterminate
// (see SegDBTransaction.Indeterminate) and the caller
// must run RecoverSegDBTransactions before serving
// ordinary reads as if the commit had or had not
// happened. Compare with errors.Is.
var ErrSegDBTxIndeterminate = errors.New("yowza segdb tx: commit outcome indeterminate; journal left applying for recovery")

var segdbTxNextID uint64

// SegDBTxWrite is one entry of a transaction's private
// write set.
type SegDBTxWrite struct {
	Key     string `json:"key"`
	Deleted bool   `json:"deleted,omitempty"`
	Value   []byte `json:"value,omitempty"`
	Unique  bool   `json:"unique,omitempty"`
}

type segdbTxAssert struct {
	kind  uint8 // TxAssertAbsent / Present / ValueEquals
	key   string
	value []byte
}

// SegDBTransaction is one layered transaction over a
// PersistedSegDB. It is NOT safe for concurrent use by
// multiple goroutines: one goroutine owns it (the same
// rule as the C TxState, which is driven by one VM).
type SegDBTransaction struct {
	mu        sync.Mutex
	db        *PersistedSegDB
	id        uint64
	handle    SnapshotHandle
	writes    map[string]*SegDBTxWrite
	order     []string
	asserts   []segdbTxAssert
	active    bool
	failed    error // first assert/commit failure, sticky
	committed bool
	// indeterminate is set when Commit failed after the
	// journal was written (see ErrSegDBTxIndeterminate):
	// the journal is still "applying" and only recovery
	// decides the outcome. Read via Indeterminate().
	indeterminate bool
}

// Indeterminate reports whether this transaction's Commit
// left an "applying" journal for recovery to decide (see
// ErrSegDBTxIndeterminate). False for every other outcome,
// including a clean commit, a clean rollback, and a
// validation failure that never wrote a journal.
func (tx *SegDBTransaction) Indeterminate() bool {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return tx.indeterminate
}

// BeginSegDBTransaction captures an owned snapshot and
// returns a new active transaction over db. It refuses
// with ErrSegDBTxRecoveryRequired when an "applying"
// journal is pending. The pending-journal check and the
// snapshot capture share one db.mu hold, so the check
// cannot mistake another transaction's in-flight commit
// for a leftover journal: a commit gate holds db.mu
// across its whole journal + apply, so any "applying"
// journal visible under db.mu is a leftover, never an
// in-flight commit. (AcquireSnapshot is not used here
// precisely because it would re-take db.mu; the
// snapshot capture below is the same locked call it
// performs, in the same order.)
func BeginSegDBTransaction(db *PersistedSegDB) (*SegDBTransaction, error) {
	if db == nil {
		return nil, fmt.Errorf("%w: nil database", ErrTimelineInvalid)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil, ErrPersistedClosed
	}
	// PATCH2 Track 1: a known-partial commit (flag set
	// by commitGate's rollback-failed path) refuses new
	// transactions exactly like a pending applying
	// journal does: only RecoverSegDBTransactions may
	// resolve it, into the full commit.
	if db.txPartialApply {
		return nil, ErrSegDBTxRecoveryRequired
	}
	pending, err := pendingApplyingTxJournalLocked(db)
	if err != nil {
		return nil, err
	}
	if pending {
		return nil, ErrSegDBTxRecoveryRequired
	}
	vec, leaseIDs, err := db.snapshotVectorAndLeasesLocked()
	if err != nil {
		return nil, err
	}
	db.snapshotNext++
	h := &snapshotHandle{
		db:         db,
		id:         SnapshotID(db.snapshotNext),
		vector:     vec,
		generation: db.generation,
		leaseIDs:   leaseIDs,
	}
	return &SegDBTransaction{
		db:     db,
		id:     atomic.AddUint64(&segdbTxNextID, 1),
		handle: h,
		writes: map[string]*SegDBTxWrite{},
		active: true,
	}, nil
}

// pendingApplyingTxJournalLocked reports whether any
// "applying" journal file exists in db's directory.
// Caller holds db.mu. A corrupt journal fails loudly
// (it blocks Begin exactly as it blocks recovery);
// "aborted" journals do not count as pending.
func pendingApplyingTxJournalLocked(db *PersistedSegDB) (bool, error) {
	ents, err := os.ReadDir(db.Dir())
	if err != nil {
		return false, err
	}
	for _, e := range ents {
		name := e.Name()
		if !strings.HasPrefix(name, "segdb-tx-") || !strings.HasSuffix(name, ".journal") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(db.Dir(), name))
		if err != nil {
			return false, err
		}
		j, err := decodeTxJournal(data)
		if err != nil {
			return false, err
		}
		if j.Status == "applying" {
			return true, nil
		}
	}
	return false, nil
}

// ID returns the transaction's journal/identity number.
func (tx *SegDBTransaction) ID() uint64 { return tx.id }

func (tx *SegDBTransaction) journalPath() string {
	return filepath.Join(tx.db.Dir(), fmt.Sprintf("segdb-tx-%d.journal", tx.id))
}

func validateSegDBTxValue(key string, val []byte) error {
	if len(key) == 0 || len(key) > 1023 {
		return fmt.Errorf("yowza segdb tx: key length %d out of range 1..1023", len(key))
	}
	if len(val) > 1024 {
		return fmt.Errorf("yowza segdb tx: value length %d exceeds 1024", len(val))
	}
	if len(key)+len(val) > 1014 {
		return fmt.Errorf("yowza segdb tx: key+value length %d exceeds hot-path budget 1014", len(key)+len(val))
	}
	return nil
}

// effectiveLocked returns the layered view value for key:
// the write set first, then the snapshot. Caller holds
// tx.mu. The second return is false when the key is
// absent or tombstoned in the effective view.
//
// FAIL-CLOSED (Axis A fix): a snapshot read error is
// propagated, NEVER converted to absence — the old code
// returned (nil, false, nil) for every handle.Get error,
// so a closed handle, an expired lease, or a snapshot
// scan failure read as "key absent" for asserts and
// value-equals checks. Only the snapshot layer's
// not-found case converts to absence. That case has no
// typed sentinel in segdb_snapshot.go (owned by another
// axis): snapshotHandle.Get returns a plain
// "key not found at snapshot <id>" error, so the match
// here is by that exact message text, and it is the
// ONLY string-matched error in this file. If
// segdb_snapshot.go gains a typed ErrSnapshotNotFound,
// this match must be replaced by errors.Is — recorded
// as a remaining item in TIMELINE_V2_PATCH_NOTES.md
// (Axis A section). ErrSnapshotHandleClosed and
// ErrLeaseExpired are typed and propagate via
// errors.Is-independent explicit handling below (they
// are returned as-is, like every other error).
func (tx *SegDBTransaction) effectiveLocked(key string) ([]byte, bool, error) {
	if w, ok := tx.writes[key]; ok {
		if w.Deleted {
			return nil, false, nil
		}
		return append([]byte(nil), w.Value...), true, nil
	}
	v, err := tx.handle.Get([]byte(key))
	if err != nil {
		if strings.Contains(err.Error(), "key not found at snapshot") {
			return nil, false, nil
		}
		return nil, false, err
	}
	return v, true, nil
}

// Get reads the layered view at the transaction's
// snapshot, overlaid by its own write set.
func (tx *SegDBTransaction) Get(key string) ([]byte, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if !tx.active {
		return nil, ErrSegDBTxNotActive
	}
	v, ok, err := tx.effectiveLocked(key)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrSegDBTxNotFound, key)
	}
	return v, nil
}

// Put records a put in the private write set. Unique
// means: fail at commit if the key exists in live
// committed state at commit time.
func (tx *SegDBTransaction) Put(key string, val []byte, unique bool) error {
	if err := validateSegDBTxValue(key, val); err != nil {
		return err
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if !tx.active {
		return ErrSegDBTxNotActive
	}
	if _, ok := tx.writes[key]; !ok {
		tx.order = append(tx.order, key)
	}
	tx.writes[key] = &SegDBTxWrite{Key: key, Value: append([]byte(nil), val...), Unique: unique}
	return nil
}

// Delete records a tombstone in the private write set.
// At commit this goes through the facade, so a
// segment-only key gets a real shadowing tombstone.
func (tx *SegDBTransaction) Delete(key string) error {
	if len(key) == 0 || len(key) > 1023 {
		return fmt.Errorf("yowza segdb tx: key length %d out of range 1..1023", len(key))
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if !tx.active {
		return ErrSegDBTxNotActive
	}
	if _, ok := tx.writes[key]; !ok {
		tx.order = append(tx.order, key)
	}
	tx.writes[key] = &SegDBTxWrite{Key: key, Deleted: true}
	return nil
}

// Assert records an assert against the effective state
// and checks it immediately (snapshot + write set).
// It is re-checked against live state at commit.
func (tx *SegDBTransaction) Assert(kind uint8, key string, val []byte) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if !tx.active {
		return ErrSegDBTxNotActive
	}
	tx.asserts = append(tx.asserts, segdbTxAssert{kind: kind, key: key, value: append([]byte(nil), val...)})
	if err := tx.checkAssertLocked(kind, key, val); err != nil {
		tx.failed = err
		return err
	}
	return nil
}

func (tx *SegDBTransaction) checkAssertLocked(kind uint8, key string, val []byte) error {
	v, ok, err := tx.effectiveLocked(key)
	if err != nil {
		// A read failure is not an assert outcome: it
		// fails the assert (and, via Assert, sticks
		// tx.failed so Commit refuses) instead of
		// being read as absence/presence.
		return err
	}
	switch kind {
	case TxAssertAbsent:
		if ok {
			return fmt.Errorf("%w: assert absent failed for %q (present)", ErrSegDBTxAssert, key)
		}
	case TxAssertPresent:
		if !ok {
			return fmt.Errorf("%w: assert present failed for %q (absent)", ErrSegDBTxAssert, key)
		}
	case TxAssertValueEquals:
		if !ok || string(v) != string(val) {
			return fmt.Errorf("%w: assert value-equals failed for %q", ErrSegDBTxAssert, key)
		}
	default:
		return fmt.Errorf("yowza segdb tx: unknown assert kind %d", kind)
	}
	return nil
}

// sortedWrites returns the write set in key order
// (deterministic apply order at commit).
func (tx *SegDBTransaction) sortedWrites() []*SegDBTxWrite {
	out := make([]*SegDBTxWrite, 0, len(tx.writes))
	for _, w := range tx.writes {
		cp := *w
		cp.Value = append([]byte(nil), w.Value...)
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

// txJournalFile is the on-disk journal format. The
// checksum covers the canonical JSON of {id, writes}
// (not the checksum field itself).
type txJournalFile struct {
	Version  int             `json:"version"`
	ID       uint64          `json:"id"`
	Status   string          `json:"status"` // "applying", "aborted"
	Writes   []*SegDBTxWrite `json:"writes"`
	Checksum uint32          `json:"checksum"`
}

func encodeTxJournal(id uint64, status string, writes []*SegDBTxWrite) ([]byte, error) {
	body, err := json.Marshal(struct {
		ID     uint64          `json:"id"`
		Writes []*SegDBTxWrite `json:"writes"`
	}{ID: id, Writes: writes})
	if err != nil {
		return nil, err
	}
	f := txJournalFile{Version: 1, ID: id, Status: status, Writes: writes, Checksum: crc32.ChecksumIEEE(body)}
	return json.Marshal(f)
}

func decodeTxJournal(data []byte) (*txJournalFile, error) {
	var f txJournalFile
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("yowza segdb tx: journal unparseable: %w", err)
	}
	if f.Version != 1 {
		return nil, fmt.Errorf("yowza segdb tx: journal version %d, want 1", f.Version)
	}
	body, err := json.Marshal(struct {
		ID     uint64          `json:"id"`
		Writes []*SegDBTxWrite `json:"writes"`
	}{ID: f.ID, Writes: f.Writes})
	if err != nil {
		return nil, err
	}
	if crc32.ChecksumIEEE(body) != f.Checksum {
		return nil, fmt.Errorf("yowza segdb tx: journal checksum mismatch for tx %d", f.ID)
	}
	return &f, nil
}

// liveValLocked reads one live committed value. Caller
// holds db.mu (the commit gate). It does NOT call
// db.Get: that public wrapper re-takes db.mu and would
// deadlock under the gate. It reads the key's shard
// directly (SegDB.Get takes host.mu, not db.mu, so the
// lock order db.mu -> host.mu is preserved). ONLY
// ErrSegDBNotFound converts to absence; every other
// error — closed/poisoned state is checked by the gate
// before any read, wasm call failures, memory failures,
// and (once the C export separates them) storage
// errors — propagates. Results are cached in cache so
// validation, unique checks, and pre-image capture for
// one commit read each distinct key exactly once
// (the old code read assert keys, unique keys, and
// pre-images separately: up to three live Gets per
// key, plus the facade's own spill pre-image Get per
// applied key — see the performance section in
// TIMELINE_V2_PATCH_NOTES.md, Axis A).
func (tx *SegDBTransaction) liveValLocked(db *PersistedSegDB, key string, cache map[string]txLiveEntry) ([]byte, bool, error) {
	if e, ok := cache[key]; ok {
		return e.val, e.ok, nil
	}
	v, err := db.shardFor(key).sdb.Get(key)
	if err != nil {
		if errors.Is(err, ErrSegDBNotFound) {
			cache[key] = txLiveEntry{ok: false}
			return nil, false, nil
		}
		return nil, false, err
	}
	cache[key] = txLiveEntry{val: append([]byte(nil), v...), ok: true}
	return v, true, nil
}

type txLiveEntry struct {
	val []byte
	ok  bool
}

// applyTxWriteLocked applies ONE write through the
// facade's locked attempt path. Caller holds db.mu.
// It is the locked twin of PersistedSegDB.Put/Delete:
// same epoch guard, same history-full flush + retry,
// same epoch note on success, same auto-checkpoint
// behaviour inside the attempt — but it never takes
// db.mu itself, so the commit gate can hold db.mu
// across a whole write set. Returns the raw attempt
// error (validation, history-full after a failed
// retry flush, wasm failure) for the caller to act on.
func applyTxWriteLocked(db *PersistedSegDB, w *SegDBTxWrite) error {
	if w.Deleted {
		if len(w.Key) == 0 || len(w.Key) > 1023 {
			return db.deleteAttemptLocked(w.Key)
		}
		if err := db.epochGuardLocked(w.Key); err != nil {
			return err
		}
		err := db.deleteAttemptLocked(w.Key)
		if err != nil && errors.Is(err, ErrSegDBHistFull) {
			if ferr := db.flushLocked(); ferr != nil {
				return fmt.Errorf("yowza persist: history-full flush failed: %v (original: %w)", ferr, err)
			}
			err = db.deleteAttemptLocked(w.Key)
		}
		if err == nil {
			db.epochNoteLocked(db.ShardIndex(w.Key), w.Key)
		}
		return err
	}
	if len(w.Key) == 0 || len(w.Key) > 1023 || len(w.Value) > 1024 || len(w.Key)+len(w.Value) > 1014 {
		return db.putAttemptLocked(w.Key, w.Value)
	}
	if err := db.epochGuardLocked(w.Key); err != nil {
		return err
	}
	err := db.putAttemptLocked(w.Key, w.Value)
	if err != nil && errors.Is(err, ErrSegDBHistFull) {
		if ferr := db.flushLocked(); ferr != nil {
			return fmt.Errorf("yowza persist: history-full flush failed: %v (original: %w)", ferr, err)
		}
		err = db.putAttemptLocked(w.Key, w.Value)
	}
	if err == nil {
		db.epochNoteLocked(db.ShardIndex(w.Key), w.Key)
	}
	return err
}

// applyTxWritesLocked applies a write set in key order
// through applyTxWriteLocked. Caller holds db.mu. It
// returns the number of writes successfully applied
// before the first failure (0..len(writes)): the
// caller compensates exactly that applied prefix, and
// never touches keys the failure left unapplied. This
// replaces the old applyTxWrites facade loop, which
// took db.mu per operation (so readers and writers
// could observe a stepwise partial commit) and whose
// compensating rollback restored EVERY write key,
// including untouched ones.
func applyTxWritesLocked(db *PersistedSegDB, writes []*SegDBTxWrite) (int, error) {
	for i, w := range writes {
		if err := applyTxWriteLocked(db, w); err != nil {
			return i, fmt.Errorf("yowza segdb tx: apply %q: %w", w.Key, err)
		}
	}
	return len(writes), nil
}

// writeTxJournalFile writes one journal file (tmp +
// fsync + rename, plus a directory sync in Sync mode)
// and reports whether the rename already happened.
// A directory-sync failure after rename leaves a
// durable-looking "applying" journal whose rename may
// not itself be durable: the caller must treat that
// as indeterminate, never as "no journal".
func writeTxJournalFile(path string, id uint64, status string, writes []*SegDBTxWrite, syncMode bool) (renamed bool, err error) {
	data, err := encodeTxJournal(id, status, writes)
	if err != nil {
		return false, err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return false, err
	}
	f, err := os.OpenFile(tmp, os.O_WRONLY, 0o644)
	if err != nil {
		_ = os.Remove(tmp)
		return false, err
	}
	syncErr := f.Sync()
	f.Close()
	if syncErr != nil {
		_ = os.Remove(tmp)
		return false, syncErr
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return false, err
	}
	if syncMode {
		if err := persistSyncDir(filepath.Dir(path)); err != nil {
			// Renamed, but the rename's durability is
			// unproven: indeterminate, NOT ignored.
			// (The old code ignored this error with
			// `_ = persistSyncDir(...)`.)
			return true, err
		}
	}
	return true, nil
}

// Commit validates (asserts + unique, re-checked
// against LIVE state), journals the write set, applies
// it, and closes the snapshot. The whole validation +
// journal + apply runs under the commit gate (db.mu):
// validation, journal recording, and application of ALL
// writes are one critical section against concurrent
// reads (PersistedSegDB.Get/Put/Delete/Scan all take
// db.mu) and concurrent commits, so no live reader can
// ever observe a half-applied write set as a commit —
// the old code took db.mu per facade operation, so
// several Puts became visible stepwise. The gate is
// held by commitGate below; Commit itself only owns
// tx.mu, and the snapshot handle is closed AFTER the
// gate releases (handle.Close takes db.mu and would
// deadlock under the gate).
//
// Fail-closed validation: pre-images of ALL required
// keys (assert keys, unique keys, and every write
// key) are read through liveValLocked BEFORE any
// journal is written. Any non-NotFound read error
// aborts the commit with no journal and no apply.
// On an in-process apply failure, only the applied
// prefix is compensated to its pre-images (reverse
// order); untouched keys are never written, so a
// failed commit cannot tombstone a key it never
// applied — the old code restored every write key,
// which, combined with the old error-as-absence
// conflation, could replace an existing value with
// a tombstone. If compensating rollback itself fails,
// or the journal directory sync fails, the journal
// is left "applying" and Commit returns
// ErrSegDBTxIndeterminate: recovery roll-forward
// (full commit) decides the outcome, and the caller
// must run recovery before serving ordinary reads.
func (tx *SegDBTransaction) Commit() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if !tx.active {
		return ErrSegDBTxNotActive
	}
	if tx.failed != nil {
		err := tx.failed
		tx.active = false
		_ = tx.handle.Close()
		return err
	}
	committed, err := tx.commitGateInternal(nil, "")
	tx.active = false
	if committed {
		tx.committed = true
	}
	if err != nil && errors.Is(err, ErrSegDBTxIndeterminate) {
		tx.indeterminate = true
	}
	// Handle close AFTER the gate releases (see above).
	_ = tx.handle.Close()
	return err
}

// coordTxAfterDecideHook is a test-only seam, nil in
// production (same pattern as patch3MergeDecisionHook):
// when non-nil it runs in the coordinated commit gate
// AFTER the coordinator decision is published and
// BEFORE the journal write / apply, and its error (if
// any) aborts the commit with the decision standing —
// exactly the crash-between-decision-and-apply window.
// Tests set it to inject that window deterministically
// and always clear it via defer. It must not call any
// DB method: the gate holds db.mu, so a DB call from
// the hook would deadlock. Production code leaves it
// nil, in which case the gate proceeds uninterrupted.
var coordTxAfterDecideHook func() error

// CommitCoordinated commits through the coordinator
// (Item 1): the caller presents an explicit decider +
// operation ID, and the commit gate validates fencing
// at commit time (inside the gate, under db.mu — a
// Begin-time check is never relied on), publishes the
// coordinator decision BEFORE any write is applied,
// then applies, then completes the decision. See
// commitGateCoordinatedLocked for the failure
// semantics: validation failures before the decision
// are clean failures, but once the decision is
// published every later failure is committed-but-
// incomplete (decision stands, recovery completes
// it) — there is deliberately no compensating
// rollback after a decision, because a rollback
// would contradict the durable decision and
// recovery would roll the full set forward anyway.
// A nil decider is refused (tokenless coordinated
// commit does not exist); on a fenced DB the plain
// Commit stays refused with ErrCoordFenceBypass.
// Retrying the same opID with the same write set
// after success returns success without re-applying
// (the decider's completion record); retrying it
// with a different write set is refused with
// ErrCoordOpExists.
func (tx *SegDBTransaction) CommitCoordinated(dec CoordDecider, opID string) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if !tx.active {
		return ErrSegDBTxNotActive
	}
	if tx.failed != nil {
		err := tx.failed
		tx.active = false
		_ = tx.handle.Close()
		return err
	}
	if dec == nil {
		return fmt.Errorf("%w: nil coordinator decider (coordinated commit requires an explicit decider, it never falls back to plain commit)", ErrCoordReleased)
	}
	committed, err := tx.commitGateInternal(dec, opID)
	tx.active = false
	if committed {
		tx.committed = true
	}
	if err != nil && errors.Is(err, ErrSegDBTxIndeterminate) {
		tx.indeterminate = true
	}
	// Handle close AFTER the gate releases (see above).
	_ = tx.handle.Close()
	return err
}

// commitGateInternal runs validation, journaling, and
// apply under db.mu. Caller holds tx.mu but NOT db.mu.
// dec == nil is the plain path; dec != nil is the
// coordinated path (see commitGateCoordinatedLocked,
// called after shared validation + pre-image capture).
// Returns (committed, err): committed is true only
// when the write set is fully applied, even if err is
// non-nil (the journal-cleanup-failure case: the state
// is committed, but the lingering journal needs a
// recovery pass to remove, and recovery's re-apply is
// idempotent). On every other error committed is false.
func (tx *SegDBTransaction) commitGateInternal(dec CoordDecider, opID string) (bool, error) {
	db := tx.db
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return false, ErrPersistedClosed
	}
	// A transaction is also a writer. In coordinated mode a
	// TOKENLESS commit must not bypass the coordinator by
	// committing directly through the per-DB journal, so
	// the plain path refuses before validation, journal,
	// or apply. The coordinated path (dec != nil) takes
	// the fence branch below instead: its authorization
	// is the decider's validation + decision, performed
	// inside this same gate.
	if dec == nil {
		if err := CheckCoordWriteFence(db); err != nil {
			return false, err
		}
	}
	if err := db.checkWritableLocked(); err != nil {
		return false, err
	}
	// A shard left indeterminate by a failed publish
	// refuses writes until Reconcile (segdb.h): do not
	// journal a commit that cannot apply.
	for i, sh := range db.shards {
		ind, err := sh.sdb.Indeterminate()
		if err != nil {
			return false, fmt.Errorf("yowza segdb tx: shard %d indeterminate check: %w", i, err)
		}
		if ind {
			return false, fmt.Errorf("yowza segdb tx: shard %d indeterminate: reconcile before commit", i)
		}
	}
	// Re-check asserts + unique against LIVE state, so
	// an intervening commit fails the transaction before
	// anything is applied. Any live read error aborts
	// the commit: it is NOT absence (see liveValLocked).
	cache := map[string]txLiveEntry{}
	for _, a := range tx.asserts {
		v, ok, err := tx.liveValLocked(db, a.key, cache)
		if err != nil {
			return false, err
		}
		switch a.kind {
		case TxAssertAbsent:
			if ok {
				return false, fmt.Errorf("%w: commit re-check: assert absent failed for %q (committed by another writer)", ErrSegDBTxAssert, a.key)
			}
		case TxAssertPresent:
			if !ok {
				return false, fmt.Errorf("%w: commit re-check: assert present failed for %q", ErrSegDBTxAssert, a.key)
			}
		case TxAssertValueEquals:
			if !ok || string(v) != string(a.value) {
				return false, fmt.Errorf("%w: commit re-check: assert value-equals failed for %q", ErrSegDBTxAssert, a.key)
			}
		}
	}
	writes := tx.sortedWrites()
	for _, w := range writes {
		if w.Unique && !w.Deleted {
			_, ok, err := tx.liveValLocked(db, w.Key, cache)
			if err != nil {
				return false, err
			}
			if ok {
				return false, fmt.Errorf("%w: commit re-check: unique put failed for %q (already exists in committed state)", ErrSegDBTxAssert, w.Key)
			}
		}
	}
	// Pre-images of ALL write keys, read successfully
	// BEFORE any journal exists: a failure here aborts
	// with nothing journalled and nothing applied.
	// (liveValLocked caches, so keys already read for
	// asserts/unique are not read twice.)
	pre := map[string]txLiveEntry{}
	for _, w := range writes {
		v, ok, err := tx.liveValLocked(db, w.Key, cache)
		if err != nil {
			return false, err
		}
		pre[w.Key] = txLiveEntry{val: v, ok: ok}
	}
	// Coordinated path, under the same db.mu hold:
	// shared validation + pre-image capture above are
	// identical to the plain path (a live read error
	// aborts before any decision exists). The helper
	// below owns the decision + apply; pre itself is
	// not needed there — a decided operation is never
	// compensating-rolled-back (see the helper).
	if dec != nil {
		return tx.commitGateCoordinatedLocked(db, dec, opID, writes)
	}
	// Journal BEFORE any apply: the journal is the
	// commit record for crash recovery (roll-forward).
	if len(writes) > 0 {
		renamed, err := writeTxJournalFile(tx.journalPath(), tx.id, "applying", writes, db.Durability() == DurabilitySync)
		if err != nil {
			if renamed {
				return false, fmt.Errorf("%w: journal directory sync failed after rename: %v; journal left applying for recovery", ErrSegDBTxIndeterminate, err)
			}
			return false, err
		}
	}
	applied, applyErr := applyTxWritesLocked(db, writes)
	if applyErr != nil {
		// Compensate ONLY the applied prefix, in
		// reverse order, to captured pre-images.
		var rollbackErr error
		for i := applied - 1; i >= 0; i-- {
			w := writes[i]
			pi := pre[w.Key]
			var rerr error
			if pi.ok {
				rerr = applyTxWriteLocked(db, &SegDBTxWrite{Key: w.Key, Value: pi.val})
			} else {
				rerr = applyTxWriteLocked(db, &SegDBTxWrite{Key: w.Key, Deleted: true})
			}
			if rerr != nil && rollbackErr == nil {
				rollbackErr = rerr
			}
		}
		if rollbackErr != nil {
			// Rollback incomplete: do NOT mark the
			// journal aborted (that would declare a
			// rolled-back state that does not hold).
			// Leave it "applying": recovery
			// roll-forward completes the full write
			// set, which is the one defined outcome
			// available from here.
			// PATCH2 Track 1: this is the ONLY
			// indeterminate path that may leave a
			// partial prefix applied, so it is the
			// only one that sets the per-DB
			// txPartialApply flag: facade reads are
			// refused until RecoverSegDBTransactions
			// rolls the full set forward. The other
			// indeterminate paths provably leave the
			// old state (nothing applied, or rollback
			// complete), so they do not set it.
			db.txPartialApply = true
			return false, fmt.Errorf("%w: commit apply failed (%v) and compensating rollback failed (%v); journal left applying for recovery roll-forward", ErrSegDBTxIndeterminate, applyErr, rollbackErr)
		}
		if len(writes) > 0 {
			if _, merr := writeTxJournalFile(tx.journalPath(), tx.id, "aborted", writes, db.Durability() == DurabilitySync); merr != nil {
				return false, fmt.Errorf("%w: commit apply failed (%v), rollback succeeded, but marking the journal aborted failed (%v); journal left applying for recovery", ErrSegDBTxIndeterminate, applyErr, merr)
			}
		}
		return false, fmt.Errorf("yowza segdb tx: commit apply failed, compensating rollback applied: %w", applyErr)
	}
	if len(writes) > 0 {
		if err := os.Remove(tx.journalPath()); err != nil {
			// State IS committed; only journal
			// cleanup failed. Report it honestly
			// rather than silently claiming a clean
			// commit: recovery removes the journal
			// (its re-apply is idempotent).
			return true, fmt.Errorf("yowza segdb tx: commit applied but journal removal failed (%v); run recovery to clean up", err)
		}
	}
	return true, nil
}

// commitGateCoordinatedLocked is the coordinated half
// of commitGateInternal. Caller holds db.mu (the
// commit gate) and has already run shared validation
// + pre-image capture. writes are in key order.
//
// Order, and why each step is where it is:
//  1. opID + fenced-coordinator-dir checks (a decider
//     for a DIFFERENT coordinator directory than the
//     one this DB is fenced under is a bypass attempt,
//     refused with ErrCoordFenceBypass before any
//     decision exists; an unfenced DB skips the dir
//     check but still decides through the decider).
//  2. Empty write set: validate the token and succeed
//     with no decision (there is nothing to decide).
//  3. Decide (durable decision BEFORE any apply).
//     Completed -> return committed without journal
//     or apply (idempotent retry). Existing -> roll
//     forward (proceed). A Decide error is reclaimed
//     via DiscardUndecided: if the decision in fact
//     stands, the error becomes committed-but-
//     incomplete (indeterminate), never a clean
//     failure; otherwise the original error stands
//     and nothing was applied.
//  4. Test hook (coordTxAfterDecideHook), then the tx
//     journal (second decision record), then apply.
//     From here on, EVERY failure is committed-but-
//     incomplete: the decision stands, so the only
//     defined outcome is the full write set, now or
//     via coordinated recovery. In particular there
//     is NO compensating rollback after a decision:
//     rolling back live state that a durable decision
//     says is committed would contradict the decision
//     and recovery would roll it forward anyway. An
//     apply failure therefore sets txPartialApply
//     (facade reads refused until recovery, exactly
//     like the plain rollback-failed path) and leaves
//     the journal "applying".
//  5. Journal removal, then Complete. A journal
//     removal failure still attempts Complete (the
//     data is in); a Complete failure is reported as
//     cleanup-pending with committed=true, and
//     recovery finishes it.
func (tx *SegDBTransaction) commitGateCoordinatedLocked(db *PersistedSegDB, dec CoordDecider, opID string, writes []*SegDBTxWrite) (bool, error) {
	if !coordValidName(opID) {
		return false, fmt.Errorf("yowza coordinator: invalid operation id %q", opID)
	}
	if fdir, ok := coordFenceDir(db); ok && fdir != dec.CoordDir() {
		return false, fmt.Errorf("%w: decider coordinator dir %q does not match fenced coordinator dir %q", ErrCoordFenceBypass, dec.CoordDir(), fdir)
	}
	if len(writes) == 0 {
		if err := dec.Validate(); err != nil {
			return false, err
		}
		return true, nil
	}
	deltas := make([]globalMergeDelta, 0, len(writes))
	for _, w := range writes {
		deltas = append(deltas, globalMergeDelta{Key: w.Key, Deleted: w.Deleted, Value: append([]byte(nil), w.Value...)})
	}
	decWrites := map[string][]globalMergeDelta{coordDeciderShard: deltas}
	status, derr := dec.Decide(opID, CoordDecisionTx, db.Dir(), "", decWrites)
	if derr != nil {
		// An opID-taken refusal (ErrCoordOpExists) is
		// already the final answer: the opID belongs
		// to a DIFFERENT operation, whose decision /
		// staging must not be touched — discarding
		// here would misreport a clean refusal as a
		// decision-stands indeterminate outcome.
		if errors.Is(derr, ErrCoordOpExists) {
			return false, derr
		}
		if derr2 := dec.DiscardUndecided(opID); derr2 != nil {
			if errors.Is(derr2, ErrCoordOpCommitted) {
				return false, fmt.Errorf("%w: coordinator decision stands for op %s (original error: %v); run coordinated recovery", ErrSegDBTxIndeterminate, opID, derr)
			}
			return false, fmt.Errorf("%w: coordinator discard failed for op %s: %v (original error: %v); run coordinated recovery", ErrSegDBTxIndeterminate, opID, derr2, derr)
		}
		return false, derr
	}
	if status == CoordDecisionCompleted {
		return true, nil
	}
	if coordTxAfterDecideHook != nil {
		if herr := coordTxAfterDecideHook(); herr != nil {
			return false, fmt.Errorf("%w: coordinator decision stands for op %s (hook aborted commit: %v); run coordinated recovery", ErrSegDBTxIndeterminate, opID, herr)
		}
	}
	renamed, err := writeTxJournalFile(tx.journalPath(), tx.id, "applying", writes, db.Durability() == DurabilitySync)
	if err != nil {
		// The decision stands regardless of the journal
		// outcome (renamed or not): coordinated
		// recovery completes the full write set.
		_ = renamed
		return false, fmt.Errorf("%w: journal write failed after coordinator decision for op %s (%v); decision stands, run coordinated recovery", ErrSegDBTxIndeterminate, opID, err)
	}
	_, applyErr := applyTxWritesLocked(db, writes)
	if applyErr != nil {
		// No compensating rollback after a decision
		// (see the function comment): the prefix may
		// be visible, so refuse facade reads until
		// recovery rolls the full set forward.
		db.txPartialApply = true
		return false, fmt.Errorf("%w: commit apply failed (%v); coordinator decision stands for op %s, journal left applying for recovery roll-forward", ErrSegDBTxIndeterminate, applyErr, opID)
	}
	journalErr := os.Remove(tx.journalPath())
	if cerr := dec.Complete(opID, CoordDecisionTx, db.Dir(), "", decWrites); cerr != nil {
		return true, fmt.Errorf("%w: op %s applied but coordinator cleanup failed: %v", ErrCoordCleanupPending, opID, cerr)
	}
	if journalErr != nil {
		return true, fmt.Errorf("yowza segdb tx: coordinated commit applied but journal removal failed (%v); run recovery to clean up", journalErr)
	}
	return true, nil
}

// Rollback discards the write set and closes the
// snapshot. Nothing was ever written to the facade,
// so there is no history to undo; the only temp write
// is a journal, which exists only after Commit starts
// (Rollback after Commit began is refused by the
// active check — use recovery for that case).
func (tx *SegDBTransaction) Rollback() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if !tx.active {
		return ErrSegDBTxNotActive
	}
	tx.active = false
	tx.writes = map[string]*SegDBTxWrite{}
	tx.order = nil
	tx.asserts = nil
	return tx.handle.Close()
}

// RecoverSegDBTransactions replays durable transaction
// journals in db's directory: "applying" journals are
// rolled forward through the same locked apply path
// Commit uses (idempotent full puts/deletes, under
// db.mu so no live reader or writer can observe a
// half-rolled-forward journal), then removed;
// "aborted" journals are removed without applying. A
// corrupt journal fails loudly and applies nothing
// further. It returns the IDs rolled forward, sorted.
//
// LIMIT, STATED: a journal carries no pre-images, so
// a roll-forward that itself fails mid-apply leaves a
// partial state and the journal in place; the caller
// must fix the cause and re-run recovery (re-apply is
// idempotent and completes the set). Recovery must run
// before ordinary reads are served after a crash or
// an indeterminate commit — it is not an automatic
// open-time hook in OpenPersistedSegDB (same caller
// obligation as the global-merge recovery in
// TIMELINE_V2_NOTES.md §E1). Within this function,
// each journal's apply is gated, but journals are
// processed one at a time, so a failure on journal N
// leaves journals after N unprocessed.
func RecoverSegDBTransactions(db *PersistedSegDB) ([]uint64, error) {
	if db == nil {
		return nil, fmt.Errorf("%w: nil database", ErrTimelineInvalid)
	}
	ents, err := os.ReadDir(db.Dir())
	if err != nil {
		return nil, err
	}
	var rolled []uint64
	for _, e := range ents {
		name := e.Name()
		if !strings.HasPrefix(name, "segdb-tx-") || !strings.HasSuffix(name, ".journal") {
			continue
		}
		path := filepath.Join(db.Dir(), name)
		data, err := os.ReadFile(path)
		if err != nil {
			return rolled, err
		}
		j, err := decodeTxJournal(data)
		if err != nil {
			return rolled, err
		}
		switch j.Status {
		case "applying":
			db.mu.Lock()
			if db.closed {
				db.mu.Unlock()
				return rolled, ErrPersistedClosed
			}
			if werr := db.checkWritableLocked(); werr != nil {
				db.mu.Unlock()
				return rolled, werr
			}
			_, aerr := applyTxWritesLocked(db, j.Writes)
			db.mu.Unlock()
			if aerr != nil {
				return rolled, fmt.Errorf("yowza segdb tx: recovery roll-forward of tx %d failed: %w", j.ID, aerr)
			}
			rolled = append(rolled, j.ID)
			_ = os.Remove(path)
		case "aborted":
			_ = os.Remove(path)
		default:
			return rolled, fmt.Errorf("yowza segdb tx: journal %s has unknown status %q", name, j.Status)
		}
	}
	sort.Slice(rolled, func(i, j int) bool { return rolled[i] < rolled[j] })
	// PATCH2 Track 1: recovery completed successfully,
	// so a known-partial state (if any) is now a full
	// commit: clear the per-DB flag. On any error
	// return above, the flag stays set.
	db.mu.Lock()
	db.txPartialApply = false
	db.mu.Unlock()
	// MERGE fix (patch3 wiring gap): transaction journals are
	// gate-pending since patch3, so a successful roll-forward
	// must also release the attached gate — otherwise the gate
	// keeps its stale pre-recovery pending list and facade reads
	// stay refused forever after a recovery that already
	// succeeded. Refresh re-scans the directory: a journal that
	// is still there (corrupt, or an error above) keeps the gate
	// closed, so this opens the gate only on a clean re-scan.
	// Callers that already refreshed explicitly (manager open)
	// just re-scan twice; the second scan is a no-op change.
	db.RefreshRecoveryGate()
	return rolled, nil
}

// PinSnapshotForTx publishes a durable pin for a
// transaction's snapshot, so the snapshot survives
// restart (see the file header: handles alone do
// not). The pin must be created BEFORE any registry
// or record that references it, per the Phase B
// call order.
func PinSnapshotForTx(db *PersistedSegDB, tx *SegDBTransaction) (*DurableSnapshotPin, error) {
	if db == nil || tx == nil {
		return nil, fmt.Errorf("%w: nil database or transaction", ErrTimelineInvalid)
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if !tx.active {
		return nil, ErrSegDBTxNotActive
	}
	return db.PinSnapshot(tx.handle, "Timeline", "", fmt.Sprintf("tx-%d", tx.id))
}
