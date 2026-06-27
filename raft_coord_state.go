package yowza

// Track B — RAFT STATE MACHINE FOR THE COORDINATOR.
//
// The Raft log (raft_coord.go) is the replicated
// record; this file is what committed entries DO.
// It is deliberately total and deterministic: every
// node applies the same committed entries in the
// same order and reaches the same state, including
// for entries whose proposer-side outcome is a
// refusal (a refused lease acquire still commits
// as an entry; the refusal is read back from the
// post-commit state, never from a second round
// trip that could diverge).
//
// ENTRY KINDS:
//   - "multishard-commit": {op_id, owner, writes}
//     The multi-shard root decision, as a log entry
//     instead of a coord-root-*.json file. The entry
//     CARRIES the full per-shard write sets, so no
//     separate staging files are needed on this
//     path: participants learn the decision (and
//     its payload) from the committed entry itself.
//     Crash before commit -> the entry is
//     uncommitted, never applied anywhere, and a
//     new leader overwrites it -> all-old. Crash
//     after commit -> every participant's recovery
//     (log replay on restart) re-records the op ->
//     all-new. The committed-op file written on
//     apply (raft-committed-<opID>.json) is the
//     participant-visible record, analogous to the
//     local coord-root file; the log itself is the
//     decision, the file is its local projection.
//     MarkRaftDone deletes the local projection
//     after apply; a restart re-derives it from
//     the log and re-apply is idempotent
//     (puts/deletes), exactly like the local
//     MarkDone-failure path.
//   - "lease-acquire": {lease_id, owner, pinned,
//     created_unix_nano, expires_unix_nano}
//     First committed acquire for an ID wins while
//     the existing lease is live. "Live" here is
//     present-and-unexpired (see below); the epoch
//     recorded is the ENTRY'S TERM (the fencing
//     epoch at commit time).
//   - "lease-release": {lease_id, owner, epoch}
//     Deletes only on an exact owner+epoch match.
//     A non-owner release is refused and the lease
//     survives; a wrong-epoch release (including
//     the original owner after its epoch was
//     superseded) is refused and the lease
//     survives. Same rules as ReleaseCoordLease.
//   - "wal-record": {op, op_id, owner, keys}
//     A sequenced coordinator WAL record. Its
//     sequence IS the log index: the replicated
//     log replaces coord-wal.log on this path
//     (ScanRaftWAL derives frames from committed
//     entries, so there is no second, local-only
//     sequence authority to diverge).
//
// LEASE LIVENESS, AND HOW IT DIFFERS FROM THE
// LOCAL TABLE (stated, not hidden): the local
// table calls a lease live exactly when its
// owner/epoch match the current owner record
// (coord_lease.go). There is no single owner
// record in Raft — the log is the record — so
// liveness here is present-and-unexpired, and
// leases SURVIVE leader change (a new leader's
// higher term does not stale existing leases).
// What leader change DOES fence is the grant and
// release paths themselves: only a current-term
// leader's entries can commit, so a stale leader
// can neither grant nor release anything after
// takeover. Expiry is advisory liveness (a dead
// holder's lease becomes replaceable without a
// takeover); the fencing epoch remains the hard
// guarantee, and it never depends on clocks.
// Expiry timestamps are set by the PROPOSER
// (leader clock at propose time) and stored
// verbatim in the entry, so every node records
// the identical expiry — expiry is never computed
// from a follower's local clock at apply time.
// A TTL of zero means no expiry.
//
// PERSISTENCE: the machine snapshot
// (raft-machine.json, CRC-sealed, atomic) is a
// cache; the log is authoritative. On start, the
// node replays committed entries the machine has
// not yet recorded (lastApplied lagging
// commitIndex), and apply is idempotent for every
// kind, so replay after any crash interleaving
// converges to the same state. Committed-op
// files are rewritten from the machine map on
// every apply and on explicit Rebuild.

import (
	"encoding/json"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// Raft entry kinds.
const (
	RaftKindMultiShardCommit = "multishard-commit"
	RaftKindLeaseAcquire     = "lease-acquire"
	RaftKindLeaseRelease     = "lease-release"
	RaftKindWALRecord        = "wal-record"
)

// RaftMultiShardCommit is the payload of a
// multishard-commit entry. Writes reuse
// globalMergeDelta so a participant can apply
// them with applyDeltasLocked unchanged.
//
// Item 1 extension: Kind, DBDir, MergeID and
// Fingerprint annotate decisions made through the
// CoordDecider layer (coord_decision.go) — the same
// metadata the local annotation file carries, so both
// modes produce the same CoordMultiShardOp shape for
// recovery. All four are omitempty, so legacy domain
// decisions (proposed via ProposeMultiShardCommit)
// encode byte-identically to before: empty kind
// means a legacy domain decision, never a Tx/merge
// decision.
type RaftMultiShardCommit struct {
	OpID   string                        `json:"op_id"`
	Owner  string                        `json:"owner"`
	Writes map[string][]globalMergeDelta `json:"writes"`
	// Kind is CoordDecisionTx / CoordDecisionMerge
	// for decider decisions, "" for legacy domain.
	Kind CoordDecisionKind `json:"kind,omitempty"`
	// DBDir is the single DB directory a Tx/merge
	// decision applies to.
	DBDir string `json:"db_dir,omitempty"`
	// MergeID is the global-merge staging ID for
	// merge decisions.
	MergeID string `json:"merge_id,omitempty"`
	// Fingerprint is the SHA-256 hex of the
	// canonical write-set JSON.
	Fingerprint string `json:"fingerprint,omitempty"`
}

// RaftLeaseAcquire is the payload of a
// lease-acquire entry. Epoch is NOT in the
// payload: it is the entry's term at commit.
type RaftLeaseAcquire struct {
	LeaseID         string   `json:"lease_id"`
	Owner           string   `json:"owner"`
	Pinned          []string `json:"pinned"`
	CreatedUnixNano int64    `json:"created_unix_nano"`
	ExpiresUnixNano int64    `json:"expires_unix_nano"` // 0 = no expiry
}

// RaftLeaseRelease is the payload of a
// lease-release entry.
type RaftLeaseRelease struct {
	LeaseID string `json:"lease_id"`
	Owner   string `json:"owner"`
	Epoch   uint64 `json:"epoch"`
}

// RaftWALRecord is the payload of a wal-record
// entry.
type RaftWALRecord struct {
	Op    string   `json:"op"`
	OpID  string   `json:"op_id"`
	Owner string   `json:"owner"`
	Keys  []string `json:"keys,omitempty"`
}

// RaftLease is one lease in the replicated table.
type RaftLease struct {
	ID              string   `json:"lease_id"`
	OwnerID         string   `json:"owner_id"`
	Epoch           uint64   `json:"epoch"`
	Pinned          []string `json:"pinned"`
	CreatedUnixNano int64    `json:"created_unix_nano"`
	ExpiresUnixNano int64    `json:"expires_unix_nano"` // 0 = no expiry
}

// LiveAt reports whether the lease is live at the
// given time (present, and either unexpiring or
// not yet expired). Clock use is confined to this
// advisory check; fencing never depends on it.
func (l *RaftLease) LiveAt(nowUnixNano int64) bool {
	if l == nil {
		return false
	}
	return l.ExpiresUnixNano == 0 || nowUnixNano < l.ExpiresUnixNano
}

// Live reports liveness at the current time.
func (l *RaftLease) Live() bool { return l.LiveAt(time.Now().UnixNano()) }

// RaftWALFrame is one committed WAL record, with
// Seq = the log index of its entry.
type RaftWALFrame struct {
	Seq   uint64   `json:"seq"`
	Op    string   `json:"op"`
	OpID  string   `json:"op_id"`
	Epoch uint64   `json:"epoch"`
	Owner string   `json:"owner"`
	Keys  []string `json:"keys,omitempty"`
}

type raftMachineSnapshot struct {
	Version    int                             `json:"version"`
	Leases     []RaftLease                     `json:"leases"`
	Committed  map[string]RaftMultiShardCommit `json:"committed"`
	CommitIdx  map[string]uint64               `json:"commit_idx"`
	CommitTerm map[string]uint64               `json:"commit_term"`
	WAL        []RaftWALFrame                  `json:"wal"`
	Checksum   uint32                          `json:"checksum"`
}

func raftMachineChecksum(s *raftMachineSnapshot) uint32 {
	body, _ := json.Marshal(struct {
		Version    int                             `json:"version"`
		Leases     []RaftLease                     `json:"leases"`
		Committed  map[string]RaftMultiShardCommit `json:"committed"`
		CommitIdx  map[string]uint64               `json:"commit_idx"`
		CommitTerm map[string]uint64               `json:"commit_term"`
		WAL        []RaftWALFrame                  `json:"wal"`
	}{Version: s.Version, Leases: s.Leases, Committed: s.Committed, CommitIdx: s.CommitIdx, CommitTerm: s.CommitTerm, WAL: s.WAL})
	return crc32.ChecksumIEEE(body)
}

// raftCommittedFile is the participant-visible
// projection of one committed multi-shard op.
type raftCommittedFile struct {
	OpID        string                        `json:"op_id"`
	Owner       string                        `json:"owner"`
	Writes      map[string][]globalMergeDelta `json:"writes"`
	Kind        CoordDecisionKind             `json:"kind,omitempty"`
	DBDir       string                        `json:"db_dir,omitempty"`
	MergeID     string                        `json:"merge_id,omitempty"`
	Fingerprint string                        `json:"fingerprint,omitempty"`
	LogIndex    uint64                        `json:"log_index"`
	Term        uint64                        `json:"term"`
	Checksum    uint32                        `json:"checksum"`
}

func raftCommittedChecksum(f *raftCommittedFile) uint32 {
	body, _ := json.Marshal(struct {
		OpID        string                        `json:"op_id"`
		Owner       string                        `json:"owner"`
		Writes      map[string][]globalMergeDelta `json:"writes"`
		Kind        CoordDecisionKind             `json:"kind,omitempty"`
		DBDir       string                        `json:"db_dir,omitempty"`
		MergeID     string                        `json:"merge_id,omitempty"`
		Fingerprint string                        `json:"fingerprint,omitempty"`
		LogIndex    uint64                        `json:"log_index"`
		Term        uint64                        `json:"term"`
	}{OpID: f.OpID, Owner: f.Owner, Writes: f.Writes, Kind: f.Kind, DBDir: f.DBDir, MergeID: f.MergeID, Fingerprint: f.Fingerprint, LogIndex: f.LogIndex, Term: f.Term})
	return crc32.ChecksumIEEE(body)
}

func raftCommittedPath(dir, opID string) string {
	return filepath.Join(dir, "raft-committed-"+opID+".json")
}

// RaftCoordMachine is the Raft state machine for
// one node. It is safe for concurrent use; Apply
// is called by exactly one node goroutine in log
// order, but readers (status, leases, committed)
// may run concurrently.
type RaftCoordMachine struct {
	dir        string
	mu         sync.Mutex
	leases     map[string]*RaftLease
	committed  map[string]RaftMultiShardCommit
	commitIdx  map[string]uint64 // opID -> log index
	commitTerm map[string]uint64 // opID -> entry term
	wal        []RaftWALFrame
}

func newRaftCoordMachine(dir string) *RaftCoordMachine {
	return &RaftCoordMachine{
		dir:        dir,
		leases:     map[string]*RaftLease{},
		committed:  map[string]RaftMultiShardCommit{},
		commitIdx:  map[string]uint64{},
		commitTerm: map[string]uint64{},
	}
}

func (m *RaftCoordMachine) snapshotPath() string {
	return filepath.Join(m.dir, "raft-machine.json")
}

// persistLocked writes the machine snapshot
// atomically. Caller holds m.mu.
func (m *RaftCoordMachine) persistLocked() error {
	leases := make([]RaftLease, 0, len(m.leases))
	for _, l := range m.leases {
		leases = append(leases, *l)
	}
	sort.Slice(leases, func(i, j int) bool { return leases[i].ID < leases[j].ID })
	committed := make(map[string]RaftMultiShardCommit, len(m.committed))
	for k, v := range m.committed {
		committed[k] = v
	}
	idx := make(map[string]uint64, len(m.commitIdx))
	for k, v := range m.commitIdx {
		idx[k] = v
	}
	term := make(map[string]uint64, len(m.commitTerm))
	for k, v := range m.commitTerm {
		term[k] = v
	}
	snap := &raftMachineSnapshot{
		Version: 1, Leases: leases, Committed: committed,
		CommitIdx: idx, CommitTerm: term,
		WAL: append([]RaftWALFrame(nil), m.wal...),
	}
	data, err := func() ([]byte, error) {
		snap.Checksum = raftMachineChecksum(snap)
		return json.Marshal(snap)
	}()
	if err != nil {
		return err
	}
	return coordWriteAtomic(m.dir, m.snapshotPath(), data)
}

// writeCommittedFileLocked writes (or rewrites)
// the participant-visible file for op. Caller
// holds m.mu. It is idempotent: same content for
// the same committed op on every node and every
// replay.
func (m *RaftCoordMachine) writeCommittedFileLocked(op RaftMultiShardCommit, index, term uint64) error {
	f := &raftCommittedFile{
		OpID: op.OpID, Owner: op.Owner, Writes: op.Writes,
		Kind: op.Kind, DBDir: op.DBDir, MergeID: op.MergeID,
		Fingerprint: op.Fingerprint,
		LogIndex:    index, Term: term,
	}
	f.Checksum = raftCommittedChecksum(f)
	data, err := json.Marshal(f)
	if err != nil {
		return err
	}
	return coordWriteAtomic(m.dir, raftCommittedPath(m.dir, op.OpID), data)
}

// RebuildCommittedFiles rewrites every committed-op
// file from the in-memory map (used after a
// restart/replay so a participant that crashed
// between commit and file write still sees the op).
func (m *RaftCoordMachine) RebuildCommittedFiles() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for opID, op := range m.committed {
		if err := m.writeCommittedFileLocked(op, m.commitIdx[opID], m.commitTerm[opID]); err != nil {
			return err
		}
	}
	return nil
}

// Apply applies one committed entry, in log order.
// It is total over known kinds and idempotent on
// replay: re-applying a committed entry reproduces
// the identical state (same lease record, same
// committed-op file, no duplicate WAL frame — WAL
// frames are keyed by log index and skipped when
// already recorded).
func (m *RaftCoordMachine) Apply(entry RaftLogEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch entry.Kind {
	case RaftKindMultiShardCommit:
		var p RaftMultiShardCommit
		if err := json.Unmarshal(entry.Data, &p); err != nil {
			return fmt.Errorf("yowza raft: multishard-commit entry %d unparseable: %v", entry.Index, err)
		}
		if !coordValidName(p.OpID) {
			return fmt.Errorf("yowza raft: multishard-commit entry %d invalid op id %q", entry.Index, p.OpID)
		}
		// Item 1: a decision kind, when present, must
		// be one the decider layer defines; anything
		// else fails loudly rather than being stored
		// as an unclassifiable decision. Empty stays
		// legal (legacy domain decisions).
		if p.Kind != "" && p.Kind != CoordDecisionTx && p.Kind != CoordDecisionMerge {
			return fmt.Errorf("yowza raft: multishard-commit entry %d unknown decision kind %q", entry.Index, p.Kind)
		}
		for sh := range p.Writes {
			if !coordValidName(sh) {
				return fmt.Errorf("yowza raft: multishard-commit entry %d invalid shard %q", entry.Index, sh)
			}
		}
		if _, ok := m.committed[p.OpID]; !ok {
			m.committed[p.OpID] = p
			m.commitIdx[p.OpID] = entry.Index
			m.commitTerm[p.OpID] = entry.Term
		}
		// WAL frame for the commit (sequence = index),
		// appended once per log index.
		m.appendWALFrameLocked(entry, "domain-commit", p.OpID, p.Owner, raftCommitKeys(p.Writes))
		if err := m.writeCommittedFileLocked(p, entry.Index, entry.Term); err != nil {
			return err
		}
		return m.persistLocked()
	case RaftKindLeaseAcquire:
		var p RaftLeaseAcquire
		if err := json.Unmarshal(entry.Data, &p); err != nil {
			return fmt.Errorf("yowza raft: lease-acquire entry %d unparseable: %v", entry.Index, err)
		}
		for _, pin := range p.Pinned {
			if pin == "" || pin == "." || pin == ".." || filepath.Base(pin) != pin {
				return fmt.Errorf("yowza raft: lease-acquire entry %d pinned name %q is not a plain basename", entry.Index, pin)
			}
		}
		now := time.Now().UnixNano()
		if cur, ok := m.leases[p.LeaseID]; !ok || !cur.LiveAt(now) {
			// Free, or expired: first committed
			// acquire wins; a replay of the same
			// entry rewrites the identical record.
			pins := append([]string(nil), p.Pinned...)
			sort.Strings(pins)
			m.leases[p.LeaseID] = &RaftLease{
				ID: p.LeaseID, OwnerID: p.Owner, Epoch: entry.Term,
				Pinned: pins, CreatedUnixNano: p.CreatedUnixNano,
				ExpiresUnixNano: p.ExpiresUnixNano,
			}
		}
		// A live existing lease is left untouched:
		// this entry lost the race deterministically
		// on every node, and the proposer reads that
		// back as ErrCoordLeaseExists.
		return m.persistLocked()
	case RaftKindLeaseRelease:
		var p RaftLeaseRelease
		if err := json.Unmarshal(entry.Data, &p); err != nil {
			return fmt.Errorf("yowza raft: lease-release entry %d unparseable: %v", entry.Index, err)
		}
		if cur, ok := m.leases[p.LeaseID]; ok && cur.OwnerID == p.Owner && cur.Epoch == p.Epoch {
			delete(m.leases, p.LeaseID)
		}
		// Any other state (missing, non-owner,
		// stale epoch) leaves the lease untouched,
		// deterministically on every node; the
		// proposer reads the outcome back.
		return m.persistLocked()
	case RaftKindWALRecord:
		var p RaftWALRecord
		if err := json.Unmarshal(entry.Data, &p); err != nil {
			return fmt.Errorf("yowza raft: wal-record entry %d unparseable: %v", entry.Index, err)
		}
		m.appendWALFrameLocked(entry, p.Op, p.OpID, p.Owner, p.Keys)
		return m.persistLocked()
	default:
		return fmt.Errorf("yowza raft: unknown entry kind %q at index %d", entry.Kind, entry.Index)
	}
}

func (m *RaftCoordMachine) appendWALFrameLocked(entry RaftLogEntry, op, opID, owner string, keys []string) {
	for _, f := range m.wal {
		if f.Seq == entry.Index {
			return
		}
	}
	keysCopy := append([]string(nil), keys...)
	sort.Strings(keysCopy)
	m.wal = append(m.wal, RaftWALFrame{
		Seq: entry.Index, Op: op, OpID: opID, Epoch: entry.Term, Owner: owner, Keys: keysCopy,
	})
}

func raftCommitKeys(writes map[string][]globalMergeDelta) []string {
	set := map[string]bool{}
	for _, deltas := range writes {
		for _, d := range deltas {
			set[d.Key] = true
		}
	}
	keys := make([]string, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// Lease returns a copy of the named lease, if any.
func (m *RaftCoordMachine) Lease(leaseID string) (*RaftLease, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	l, ok := m.leases[leaseID]
	if !ok {
		return nil, false
	}
	cp := *l
	cp.Pinned = append([]string(nil), l.Pinned...)
	return &cp, true
}

// ListLeases returns copies of all leases, sorted
// by ID.
func (m *RaftCoordMachine) ListLeases() []RaftLease {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]RaftLease, 0, len(m.leases))
	for _, l := range m.leases {
		cp := *l
		cp.Pinned = append([]string(nil), l.Pinned...)
		out = append(out, cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// CommittedOp returns a committed multi-shard op,
// if this node has applied it.
func (m *RaftCoordMachine) CommittedOp(opID string) (RaftMultiShardCommit, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	op, ok := m.committed[opID]
	return op, ok
}

// CommittedOps returns all committed ops, sorted
// by op ID.
func (m *RaftCoordMachine) CommittedOps() []RaftMultiShardCommit {
	m.mu.Lock()
	defer m.mu.Unlock()
	ids := make([]string, 0, len(m.committed))
	for id := range m.committed {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]RaftMultiShardCommit, 0, len(ids))
	for _, id := range ids {
		out = append(out, m.committed[id])
	}
	return out
}

// WALFrames returns committed WAL frames in
// sequence (log index) order.
func (m *RaftCoordMachine) WALFrames() []RaftWALFrame {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := append([]RaftWALFrame(nil), m.wal...)
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out
}

// readRaftCommittedFile validates one
// participant-visible committed-op file.
func readRaftCommittedFile(path string) (*raftCommittedFile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var f raftCommittedFile
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("%w: raft committed file %s unparseable: %v", ErrCoordCorrupt, path, err)
	}
	if f.OpID == "" || raftCommittedChecksum(&f) != f.Checksum {
		return nil, fmt.Errorf("%w: raft committed file %s invalid", ErrCoordCorrupt, path)
	}
	return &f, nil
}

// RecoverRaftMultiShard is the distributed-path
// analogue of RecoverMultiShard: it reads every
// participant-visible committed-op file in dir
// and returns the validated ops, sorted by op ID.
// A corrupt committed file fails loudly and
// returns nothing — a participant must not apply
// a prefix of a corrupt decision. Files are
// re-derived from the Raft log on node restart,
// so a file deleted by MarkRaftDone after a
// successful apply reappears on restart and
// re-apply is idempotent (see file header).
func RecoverRaftMultiShard(dir string) ([]CoordMultiShardOp, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var ops []CoordMultiShardOp
	for _, e := range ents {
		name := e.Name()
		if !strings.HasPrefix(name, "raft-committed-") || !strings.HasSuffix(name, ".json") {
			continue
		}
		f, err := readRaftCommittedFile(filepath.Join(dir, name))
		if err != nil {
			return nil, err
		}
		ops = append(ops, CoordMultiShardOp{OpID: f.OpID, Writes: f.Writes, Kind: f.Kind, DBDir: f.DBDir, MergeID: f.MergeID, Fingerprint: f.Fingerprint})
	}
	sort.Slice(ops, func(i, j int) bool { return ops[i].OpID < ops[j].OpID })
	return ops, nil
}

// MarkRaftDone deletes a participant's local
// committed-op projection after apply (best
// effort per file, mirroring MarkDone). It does
// NOT delete the Raft decision: the log still
// holds the entry, and a node restart re-derives
// the file. Callers must therefore treat
// re-appearance as a harmless idempotent
// re-apply, never as a new operation.
func MarkRaftDone(dir, opID string) error {
	if !coordValidName(opID) {
		return fmt.Errorf("yowza raft: invalid operation id %q", opID)
	}
	if err := os.Remove(raftCommittedPath(dir, opID)); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
