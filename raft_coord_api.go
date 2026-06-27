package yowza

// Track B — RAFT COORDINATOR SESSION + HTTP API.
//
// RaftCoordinatorSession is the Go API for the
// distributed coordinator path. It is the
// distributed analogue of CoordinatorSession, but
// it is NOT a drop-in replacement and does not
// pretend to be: the local session's authority is
// a flock + owner record in ONE directory, while
// this session's authority is a majority of Raft
// nodes. The two paths share error sentinels
// where the rule is the same (stale fencing epoch,
// lease owner/epoch refusals) so callers can
// check fencing uniformly with errors.Is.
//
// Propose outcomes for leases are read back from
// the post-commit machine state (see
// raft_coord_state.go): a committed lease entry
// whose effect lost a deterministic race still
// counts as committed, and the proposer reports
// the race outcome (ErrCoordLeaseExists /
// ErrCoordLeaseNotOwner / ErrCoordLeaseStaleEpoch)
// from that state. That keeps every node's state
// identical without a second consensus round.
//
// RaftCoordServer exposes the same operations
// over HTTP for multi-process tests and operators
// (the cmd/raftcoord binary). Its listener is
// SEPARATE from the peer-RPC listener: peer
// traffic and client/admin traffic never share a
// port, so blocking a peer (partition simulation)
// never blocks the test harness's API calls.

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// RaftCoordinatorSession is one node's handle on
// the distributed coordinator. It owns a RaftNode
// and its state machine; Start begins elections
// and replication, Stop halts them (persisted
// state survives for restart).
type RaftCoordinatorSession struct {
	node    *RaftNode
	machine *RaftCoordMachine
	// fenceMu backs ValidateFenceLocked (Item 1):
	// it serialises fence-hold callers against each
	// other in this process. It is NOT a term hold:
	// leadership belief can change while the hold is
	// out (see ValidateFenceLocked) — the hard
	// fencing check on this path is the propose
	// inside Decide, exactly as Validate is only an
	// early check for the local session.
	fenceMu sync.Mutex
}

// NewRaftCoordinator builds a session for cfg. The
// state machine is the node's apply function;
// cfg.Apply, if set by the caller, is ignored in
// favour of the machine (documented, not silent:
// the machine IS the coordinator state).
func NewRaftCoordinator(cfg RaftConfig) (*RaftCoordinatorSession, error) {
	machine, err := loadRaftCoordMachine(cfg.Dir)
	if err != nil {
		return nil, err
	}
	cfg.Apply = machine.Apply
	node, err := NewRaftNode(cfg)
	if err != nil {
		return nil, err
	}
	return &RaftCoordinatorSession{node: node, machine: machine}, nil
}

// loadRaftCoordMachine loads the machine snapshot
// cache if present (CRC-validated; corruption is
// a loud error, never a silent fresh start), or
// starts empty. The log remains authoritative:
// snapshot/machine divergence is repaired by
// idempotent replay on start.
func loadRaftCoordMachine(dir string) (*RaftCoordMachine, error) {
	m := newRaftCoordMachine(dir)
	data, err := readFileIfExists(m.snapshotPath())
	if err != nil {
		return nil, err
	}
	if data == nil {
		return m, nil
	}
	var snap raftMachineSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, fmt.Errorf("%w: raft machine snapshot unparseable: %v", ErrCoordCorrupt, err)
	}
	if snap.Version != 1 || raftMachineChecksum(&snap) != snap.Checksum {
		return nil, fmt.Errorf("%w: raft machine snapshot invalid", ErrCoordCorrupt)
	}
	for _, l := range snap.Leases {
		cp := l
		cp.Pinned = append([]string(nil), l.Pinned...)
		m.leases[cp.ID] = &cp
	}
	for id, op := range snap.Committed {
		m.committed[id] = op
	}
	// Per-op log positions are in the snapshot, so
	// RebuildCommittedFiles after start rewrites
	// byte-identical committed-op files (an earlier
	// draft stored no positions here and rewrote
	// index 0 after restart — fixed by persisting
	// them; replay still corrects any drift).
	for id, idx := range snap.CommitIdx {
		m.commitIdx[id] = idx
	}
	for id, term := range snap.CommitTerm {
		m.commitTerm[id] = term
	}
	m.wal = append([]RaftWALFrame(nil), snap.WAL...)
	return m, nil
}

func readFileIfExists(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	return data, nil
}

// Start begins the node (peer RPC, elections,
// replication, apply) and rewrites committed-op
// files from the loaded machine state.
func (s *RaftCoordinatorSession) Start() error {
	if err := s.node.Start(); err != nil {
		return err
	}
	// Best-effort rebuild is not good enough here:
	// a participant that restarts must see its
	// committed ops; surface a rebuild failure.
	return s.machine.RebuildCommittedFiles()
}

// Stop halts the node. Persisted Raft + machine
// state survive; a new session on the same dir
// resumes from them.
func (s *RaftCoordinatorSession) Stop() { s.node.Stop() }

// Dir returns the Raft node's directory (the
// coordinator directory for this session: Raft
// state, machine snapshot, committed-op files, and
// Item 1 completion records all live here).
func (s *RaftCoordinatorSession) Dir() string {
	if s == nil || s.node == nil {
		return ""
	}
	return s.node.dir
}

// ValidateFenceLocked implements FenceValidator for
// the Raft path (Item 1), so a DB can be fenced in
// Raft mode via RegisterCoordFenceValidator and
// plain paths refuse with ErrCoordFenceBypass.
// LIMIT, STATED: unlike the local session, whose
// hold blocks Release (and therefore takeover) for
// the hold's duration, this hold only serialises
// in-process fence holders and checks current
// leader belief at acquisition. A leader can lose
// leadership mid-hold; that does not retract a
// decision already majority-committed through
// Decide, and maintenance operations (the fence
// hold's main users) are per-DB operations, not
// coordinator decisions. Callers that need hard
// fencing on this path must decide through
// RaftCoordDecider, never through the fence hold
// alone.
func (s *RaftCoordinatorSession) ValidateFenceLocked() (func(), error) {
	if s == nil || s.node == nil {
		return nil, fmt.Errorf("%w: nil raft session", ErrCoordReleased)
	}
	s.fenceMu.Lock()
	if err := s.Validate(); err != nil {
		s.fenceMu.Unlock()
		return nil, err
	}
	return s.fenceMu.Unlock, nil
}

// Node returns the underlying Raft node (for
// BlockPeer/UnblockPeer partition control and
// status in tests).
func (s *RaftCoordinatorSession) Node() *RaftNode { return s.node }

// Machine returns the state machine (for reads).
func (s *RaftCoordinatorSession) Machine() *RaftCoordMachine { return s.machine }

// Status returns the node's Raft status.
func (s *RaftCoordinatorSession) Status() RaftStatus { return s.node.Status() }

// IsLeader reports current leadership belief.
func (s *RaftCoordinatorSession) IsLeader() bool { return s.node.IsLeader() }

// Validate reports whether this session is
// currently the leader. It is an early check
// only: the authoritative fencing check is the
// propose itself (a partitioned leader still
// believes it leads until a higher term reaches
// it or its propose fails for no majority).
func (s *RaftCoordinatorSession) Validate() error {
	st := s.node.Status()
	if st.State != "leader" {
		if st.LeaderID == "" {
			return fmt.Errorf("%w: no leader known", ErrRaftNotLeader)
		}
		return fmt.Errorf("%w: leader is %s", ErrRaftNotLeader, st.LeaderID)
	}
	return nil
}

// ProposeMultiShardCommit proposes a multi-shard
// root decision as ONE replicated entry and waits
// for majority commit. This is the distributed
// replacement for PrepareMultiShard + PublishRoot:
// there is no staging step on this path because
// the entry itself carries the full write sets.
// A stale-leader attempt fails with
// ErrRaftStaleTerm (wrapping
// ErrCoordStaleEpoch); a minority attempt fails
// with ErrRaftNoMajority; a non-leader attempt
// fails fast with ErrRaftNotLeader.
func (s *RaftCoordinatorSession) ProposeMultiShardCommit(opID, owner string, writes map[string][]globalMergeDelta) (uint64, uint64, error) {
	if !coordValidName(opID) {
		return 0, 0, fmt.Errorf("yowza raft: invalid operation id %q", opID)
	}
	if owner == "" {
		return 0, 0, fmt.Errorf("yowza raft: empty owner id")
	}
	if len(writes) == 0 {
		return 0, 0, fmt.Errorf("yowza raft: empty multi-shard operation %s", opID)
	}
	for sh := range writes {
		if !coordValidName(sh) {
			return 0, 0, fmt.Errorf("yowza raft: invalid shard id %q", sh)
		}
	}
	data, err := json.Marshal(RaftMultiShardCommit{OpID: opID, Owner: owner, Writes: writes})
	if err != nil {
		return 0, 0, err
	}
	return s.node.Propose(RaftKindMultiShardCommit, data, 0)
}

// ProposeCoordDecision proposes one Tx/merge commit
// decision (Item 1 extension of the Track B decision
// API to Tx/Merge payloads). It reuses the
// multishard-commit entry kind and the machine's
// existing handling: the extension is the decision
// metadata (kind, DB dir, merge ID, fingerprint)
// carried on the payload, so a participant can
// apply the decision with applyDeltasLocked /
// applyTxWritesLocked unchanged and recovery can
// tell a Tx decision from a merge decision from a
// legacy domain decision. The fingerprint is
// computed here, from the exact writes proposed, so
// the machine's stored copy and the decider's
// expectation can never disagree about what was
// decided. A stale-leader attempt fails with
// ErrRaftStaleTerm (wrapping ErrCoordStaleEpoch);
// a minority attempt fails with ErrRaftNoMajority
// (indeterminate — see the decider: retry the SAME
// opID, never a fresh one).
func (s *RaftCoordinatorSession) ProposeCoordDecision(opID, owner string, kind CoordDecisionKind, dbDir, mergeID string, writes map[string][]globalMergeDelta) error {
	if !coordValidName(opID) {
		return fmt.Errorf("yowza raft: invalid operation id %q", opID)
	}
	if owner == "" {
		return fmt.Errorf("yowza raft: empty owner id")
	}
	if kind != CoordDecisionTx && kind != CoordDecisionMerge {
		return fmt.Errorf("yowza raft: invalid decision kind %q", kind)
	}
	if dbDir == "" {
		return fmt.Errorf("yowza raft: empty database dir for op %s", opID)
	}
	if len(writes) == 0 {
		return fmt.Errorf("yowza raft: empty decision operation %s", opID)
	}
	for sh := range writes {
		if !coordValidName(sh) {
			return fmt.Errorf("yowza raft: invalid shard id %q", sh)
		}
	}
	data, err := json.Marshal(RaftMultiShardCommit{
		OpID: opID, Owner: owner, Writes: writes,
		Kind: kind, DBDir: dbDir, MergeID: mergeID,
		Fingerprint: coordDecisionFingerprint(writes),
	})
	if err != nil {
		return err
	}
	_, _, err = s.node.Propose(RaftKindMultiShardCommit, data, 0)
	return err
}

// ProposeLeaseAcquire proposes a lease grant and
// waits for commit, then reads back the outcome:
// success only if the post-commit lease is this
// owner's at the entry's term; a live lease held
// by another owner/epoch is ErrCoordLeaseExists.
func (s *RaftCoordinatorSession) ProposeLeaseAcquire(leaseID, owner string, pinned []string, ttl time.Duration) (*RaftLease, error) {
	if leaseID == "" {
		return nil, fmt.Errorf("yowza raft: empty lease id")
	}
	if owner == "" {
		return nil, fmt.Errorf("yowza raft: empty owner id")
	}
	for _, p := range pinned {
		if p == "" || p == "." || p == ".." || filepath.Base(p) != p {
			return nil, fmt.Errorf("yowza raft: pinned name %q is not a plain basename", p)
		}
	}
	var expires int64
	if ttl > 0 {
		expires = time.Now().UnixNano() + int64(ttl)
	}
	payload := RaftLeaseAcquire{
		LeaseID: leaseID, Owner: owner, Pinned: append([]string(nil), pinned...),
		CreatedUnixNano: time.Now().UnixNano(), ExpiresUnixNano: expires,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	_, entryTerm, err := s.node.Propose(RaftKindLeaseAcquire, data, 0)
	if err != nil {
		return nil, err
	}
	// Wait briefly for local apply to catch up
	// with the commit (apply is async); the entry
	// is committed, so apply is guaranteed to
	// follow in log order.
	if err := s.waitApplied(5 * time.Second); err != nil {
		return nil, err
	}
	lease, ok := s.machine.Lease(leaseID)
	if !ok {
		return nil, fmt.Errorf("yowza raft: lease %s not present after committed acquire", leaseID)
	}
	if lease.OwnerID != owner || lease.Epoch != entryTerm {
		return nil, fmt.Errorf("%w: lease %s held by %s epoch %d", ErrCoordLeaseExists, leaseID, lease.OwnerID, lease.Epoch)
	}
	return lease, nil
}

// ProposeLeaseRelease proposes a lease release and
// waits for commit, then reads back the outcome:
// absent = released; present with a different
// owner = ErrCoordLeaseNotOwner; present with a
// different epoch = ErrCoordLeaseStaleEpoch.
func (s *RaftCoordinatorSession) ProposeLeaseRelease(leaseID, owner string, epoch uint64) error {
	if leaseID == "" {
		return fmt.Errorf("yowza raft: empty lease id")
	}
	data, err := json.Marshal(RaftLeaseRelease{LeaseID: leaseID, Owner: owner, Epoch: epoch})
	if err != nil {
		return err
	}
	if _, _, err := s.node.Propose(RaftKindLeaseRelease, data, 0); err != nil {
		return err
	}
	if err := s.waitApplied(5 * time.Second); err != nil {
		return err
	}
	lease, ok := s.machine.Lease(leaseID)
	if !ok {
		return nil
	}
	if lease.OwnerID != owner {
		return fmt.Errorf("%w: lease %s owned by %s, release attempted by %s", ErrCoordLeaseNotOwner, leaseID, lease.OwnerID, owner)
	}
	if lease.Epoch != epoch {
		return fmt.Errorf("%w: lease %s epoch %d, release epoch %d", ErrCoordLeaseStaleEpoch, leaseID, lease.Epoch, epoch)
	}
	return fmt.Errorf("yowza raft: lease %s still present after committed release by its owner", leaseID)
}

// ProposeWALRecord proposes a sequenced WAL record
// (sequence = committed log index) and waits for
// commit. It is the distributed analogue of
// AppendWALRecord: the replicated log is the WAL.
func (s *RaftCoordinatorSession) ProposeWALRecord(op, opID, owner string, keys []string) (uint64, error) {
	data, err := json.Marshal(RaftWALRecord{Op: op, OpID: opID, Owner: owner, Keys: keys})
	if err != nil {
		return 0, err
	}
	index, _, err := s.node.Propose(RaftKindWALRecord, data, 0)
	return index, err
}

// waitApplied waits until lastApplied catches up
// with commitIndex (local apply lag after a
// commit this node already knows about).
func (s *RaftCoordinatorSession) waitApplied(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		st := s.node.Status()
		if st.LastApplied >= st.CommitIndex {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("yowza raft: local apply lag: applied %d, committed %d", st.LastApplied, st.CommitIndex)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// PublishRootViaRaft is the minimal wiring from
// the existing coordinator vocabulary to the
// distributed path: it publishes a multi-shard
// root decision through Raft consensus instead of
// through PublishRoot's local root file. It does
// not touch coord.lock, coord-owner.json, or any
// DB directory lock.
func PublishRootViaRaft(sess *RaftCoordinatorSession, opID, owner string, writes map[string][]globalMergeDelta) error {
	if sess == nil {
		return fmt.Errorf("%w: nil raft session", ErrCoordReleased)
	}
	_, _, err := sess.ProposeMultiShardCommit(opID, owner, writes)
	return err
}

// --- HTTP server (client/admin API) ---

type raftHTTPError struct {
	Error string `json:"error"`
}

func raftWriteErr(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, ErrRaftStaleTerm) || errors.Is(err, ErrCoordStaleEpoch):
		status = http.StatusConflict
	case errors.Is(err, ErrRaftNotLeader):
		status = http.StatusConflict
	case errors.Is(err, ErrRaftNoMajority):
		status = http.StatusServiceUnavailable
	case errors.Is(err, ErrCoordLeaseExists):
		status = http.StatusConflict
	case errors.Is(err, ErrCoordLeaseNotOwner):
		status = http.StatusForbidden
	case errors.Is(err, ErrCoordLeaseNotFound):
		status = http.StatusNotFound
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(raftHTTPError{Error: err.Error()})
}

func raftWriteJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

// RaftCoordServer serves client/admin HTTP for one
// session on its own listener.
type RaftCoordServer struct {
	sess *RaftCoordinatorSession
	ln   net.Listener
	srv  *http.Server
}

// StartRaftCoordServer starts the client/admin API
// on addr ("127.0.0.1:0" picks a free port).
func StartRaftCoordServer(sess *RaftCoordinatorSession, addr string) (*RaftCoordServer, error) {
	s := &RaftCoordServer{sess: sess}
	mux := http.NewServeMux()
	mux.HandleFunc("/raft/status", s.handleStatus)
	mux.HandleFunc("/raft/propose/commit", s.handleProposeCommit)
	mux.HandleFunc("/raft/propose/lease-acquire", s.handleProposeLeaseAcquire)
	mux.HandleFunc("/raft/propose/lease-release", s.handleProposeLeaseRelease)
	mux.HandleFunc("/raft/propose/wal", s.handleProposeWAL)
	mux.HandleFunc("/raft/leases", s.handleLeases)
	mux.HandleFunc("/raft/committed", s.handleCommitted)
	mux.HandleFunc("/raft/wal", s.handleWAL)
	mux.HandleFunc("/raft/admin/block", s.handleBlock)
	mux.HandleFunc("/raft/admin/unblock", s.handleBlock)
	mux.HandleFunc("/raft/health", func(w http.ResponseWriter, r *http.Request) {
		raftWriteJSON(w, map[string]bool{"ok": true})
	})
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	s.ln = ln
	s.srv = &http.Server{Handler: mux}
	go func() { _ = s.srv.Serve(ln) }()
	return s, nil
}

// Addr returns the API listen address.
func (s *RaftCoordServer) Addr() string { return s.ln.Addr().String() }

// Close stops the API server (not the Raft node).
func (s *RaftCoordServer) Close() error { return s.srv.Close() }

func (s *RaftCoordServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	raftWriteJSON(w, struct {
		Node      RaftStatus `json:"node"`
		Leases    int        `json:"leases"`
		Committed int        `json:"committed"`
	}{
		Node: s.sess.Status(), Leases: len(s.sess.machine.ListLeases()),
		Committed: len(s.sess.machine.CommittedOps()),
	})
}

type raftProposeCommitRequest struct {
	OpID   string                        `json:"op_id"`
	Owner  string                        `json:"owner"`
	Writes map[string][]globalMergeDelta `json:"writes"`
}

func (s *RaftCoordServer) handleProposeCommit(w http.ResponseWriter, r *http.Request) {
	var req raftProposeCommitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		raftWriteErr(w, err)
		return
	}
	index, term, err := s.sess.ProposeMultiShardCommit(req.OpID, req.Owner, req.Writes)
	if err != nil {
		raftWriteErr(w, err)
		return
	}
	raftWriteJSON(w, map[string]any{"committed": true, "index": index, "term": term})
}

type raftLeaseAcquireRequest struct {
	LeaseID    string   `json:"lease_id"`
	Owner      string   `json:"owner"`
	Pinned     []string `json:"pinned"`
	TTLSeconds float64  `json:"ttl_seconds"`
}

func (s *RaftCoordServer) handleProposeLeaseAcquire(w http.ResponseWriter, r *http.Request) {
	var req raftLeaseAcquireRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		raftWriteErr(w, err)
		return
	}
	lease, err := s.sess.ProposeLeaseAcquire(req.LeaseID, req.Owner, req.Pinned, time.Duration(req.TTLSeconds*float64(time.Second)))
	if err != nil {
		raftWriteErr(w, err)
		return
	}
	raftWriteJSON(w, lease)
}

type raftLeaseReleaseRequest struct {
	LeaseID string `json:"lease_id"`
	Owner   string `json:"owner"`
	Epoch   uint64 `json:"epoch"`
}

func (s *RaftCoordServer) handleProposeLeaseRelease(w http.ResponseWriter, r *http.Request) {
	var req raftLeaseReleaseRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		raftWriteErr(w, err)
		return
	}
	if err := s.sess.ProposeLeaseRelease(req.LeaseID, req.Owner, req.Epoch); err != nil {
		raftWriteErr(w, err)
		return
	}
	raftWriteJSON(w, map[string]bool{"released": true})
}

type raftWALRequest struct {
	Op    string   `json:"op"`
	OpID  string   `json:"op_id"`
	Owner string   `json:"owner"`
	Keys  []string `json:"keys"`
}

func (s *RaftCoordServer) handleProposeWAL(w http.ResponseWriter, r *http.Request) {
	var req raftWALRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		raftWriteErr(w, err)
		return
	}
	index, err := s.sess.ProposeWALRecord(req.Op, req.OpID, req.Owner, req.Keys)
	if err != nil {
		raftWriteErr(w, err)
		return
	}
	raftWriteJSON(w, map[string]any{"committed": true, "index": index})
}

func (s *RaftCoordServer) handleLeases(w http.ResponseWriter, r *http.Request) {
	leases := s.sess.machine.ListLeases()
	if leases == nil {
		leases = []RaftLease{}
	}
	raftWriteJSON(w, leases)
}

func (s *RaftCoordServer) handleCommitted(w http.ResponseWriter, r *http.Request) {
	ops := s.sess.machine.CommittedOps()
	if ops == nil {
		ops = []RaftMultiShardCommit{}
	}
	raftWriteJSON(w, ops)
}

func (s *RaftCoordServer) handleWAL(w http.ResponseWriter, r *http.Request) {
	frames := s.sess.machine.WALFrames()
	if frames == nil {
		frames = []RaftWALFrame{}
	}
	raftWriteJSON(w, frames)
}

func (s *RaftCoordServer) handleBlock(w http.ResponseWriter, r *http.Request) {
	var req struct {
		PeerID string `json:"peer_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		raftWriteErr(w, err)
		return
	}
	if r.URL.Path == "/raft/admin/block" {
		s.sess.node.BlockPeer(req.PeerID)
	} else {
		s.sess.node.UnblockPeer(req.PeerID)
	}
	raftWriteJSON(w, map[string]bool{"ok": true})
}

// --- HTTP client (for tests and operators) ---

// RaftCoordClient talks to a RaftCoordServer.
type RaftCoordClient struct {
	base string
	http *http.Client
}

// DialRaftCoord connects to an API address as
// reported by RaftCoordServer.Addr().
func DialRaftCoord(addr string) *RaftCoordClient {
	return &RaftCoordClient{base: "http://" + addr, http: &http.Client{Timeout: 30 * time.Second}}
}

func (c *RaftCoordClient) post(path string, req any, out any) error {
	data, err := json.Marshal(req)
	if err != nil {
		return err
	}
	resp, err := c.http.Post(c.base+path, "application/json", bytes.NewReader(data))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		var e raftHTTPError
		if json.Unmarshal(body, &e) == nil && e.Error != "" {
			return fmt.Errorf("yowza raft api %s: %s", path, e.Error)
		}
		return fmt.Errorf("yowza raft api %s: status %d: %s", path, resp.StatusCode, string(body))
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(body, out)
}

func (c *RaftCoordClient) get(path string, out any) error {
	resp, err := c.http.Get(c.base + path)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("yowza raft api %s: status %d: %s", path, resp.StatusCode, string(body))
	}
	return json.Unmarshal(body, out)
}

// RaftAPIStatus is the /raft/status payload.
type RaftAPIStatus struct {
	Node      RaftStatus `json:"node"`
	Leases    int        `json:"leases"`
	Committed int        `json:"committed"`
}

// Status fetches /raft/status.
func (c *RaftCoordClient) Status() (*RaftAPIStatus, error) {
	var out RaftAPIStatus
	if err := c.get("/raft/status", &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// Health checks /raft/health.
func (c *RaftCoordClient) Health() error {
	var out map[string]bool
	return c.get("/raft/health", &out)
}

// ProposeCommit proposes a multi-shard commit via
// the API server.
func (c *RaftCoordClient) ProposeCommit(opID, owner string, writes map[string][]globalMergeDelta) error {
	var out map[string]any
	return c.post("/raft/propose/commit", raftProposeCommitRequest{OpID: opID, Owner: owner, Writes: writes}, &out)
}

// ProposeLeaseAcquire proposes a lease grant via
// the API server.
func (c *RaftCoordClient) ProposeLeaseAcquire(leaseID, owner string, pinned []string, ttl time.Duration) (*RaftLease, error) {
	var out RaftLease
	err := c.post("/raft/propose/lease-acquire", raftLeaseAcquireRequest{
		LeaseID: leaseID, Owner: owner, Pinned: pinned, TTLSeconds: ttl.Seconds(),
	}, &out)
	return &out, err
}

// ProposeLeaseRelease proposes a lease release via
// the API server.
func (c *RaftCoordClient) ProposeLeaseRelease(leaseID, owner string, epoch uint64) error {
	var out map[string]bool
	return c.post("/raft/propose/lease-release", raftLeaseReleaseRequest{LeaseID: leaseID, Owner: owner, Epoch: epoch}, &out)
}

// Leases lists leases via the API server.
func (c *RaftCoordClient) Leases() ([]RaftLease, error) {
	var out []RaftLease
	if err := c.get("/raft/leases", &out); err != nil {
		return nil, err
	}
	return out, nil
}

// Committed lists committed ops via the API server.
func (c *RaftCoordClient) Committed() ([]RaftMultiShardCommit, error) {
	var out []RaftMultiShardCommit
	if err := c.get("/raft/committed", &out); err != nil {
		return nil, err
	}
	return out, nil
}

// WAL lists WAL frames via the API server.
func (c *RaftCoordClient) WAL() ([]RaftWALFrame, error) {
	var out []RaftWALFrame
	if err := c.get("/raft/wal", &out); err != nil {
		return nil, err
	}
	return out, nil
}

// BlockPeer blocks peer sends on the target node
// (partition simulation).
func (c *RaftCoordClient) BlockPeer(peerID string) error {
	var out map[string]bool
	return c.post("/raft/admin/block", map[string]string{"peer_id": peerID}, &out)
}

// UnblockPeer reverses BlockPeer.
func (c *RaftCoordClient) UnblockPeer(peerID string) error {
	var out map[string]bool
	return c.post("/raft/admin/unblock", map[string]string{"peer_id": peerID}, &out)
}
