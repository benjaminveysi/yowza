package yowza

// Track B — DISTRIBUTED COORDINATOR: RAFT (stdlib-only).
//
// This file is the Raft core for coordinator commit
// decisions across machines. It replaces the local
// coordinator's commit authority (coord.lock flock +
// persisted owner record, coord_coordinator.go) for
// the distributed path ONLY: the local coordinator
// files are untouched, and nothing here weakens any
// DB's exclusive directory lock. A Raft node never
// opens a DB directory; it decides coordinator
// operations (multi-shard commits, leases, WAL
// records) and records them in its replicated log.
//
// What Raft guarantees here, in one paragraph: at
// most one leader per term (one vote per term per
// voter, candidate must hold an up-to-date log);
// a log entry is committed only after a majority of
// nodes hold it, and committed entries are never
// lost or rewritten (a new leader must hold every
// committed entry, because it won a majority that
// intersects the committing majority); followers
// apply committed entries in log order; a stale
// leader (old term) cannot commit anything anywhere,
// because every follower rejects its AppendEntries
// with its higher current term.
//
// FENCING = TERM. The fencing epoch of the local
// coordinator (owner record epoch) maps to the Raft
// term: terms are persisted per node before any RPC
// reply that depends on them, they only increase,
// and a commit attempt by a leader whose term has
// been superseded fails with ErrRaftStaleTerm (which
// also wraps ErrCoordStaleEpoch, so existing
// fencing checks via errors.Is keep working).
// The old local coord.lock is NOT consulted on this
// path at all — a local flock file cannot be the
// commit authority across machines, and pretending
// otherwise would be exactly the hole this track
// closes.
//
// MEMBERSHIP: a fixed initial peer set (static
// config). Dynamic membership changes (joint
// consensus, AddServer/RemoveServer) are NOT
// implemented — stated plainly, not hidden. The
// peer set is the config the node starts with.
//
// SNAPSHOTS / COMPACTION: NOT implemented. The log
// is retained in full and the state machine is
// rebuilt by replaying committed entries on start
// (see raft_coord_state.go). That bounds this
// implementation to coordinator decision logs,
// which are small (one entry per commit/lease
// operation), not to data-plane logs. Log
// compaction is future work, not a silent
// omission: a deployment that proposes millions
// of coordinator operations will grow
// raft-state.json without bound.
//
// TRANSPORT: HTTP + JSON on loopback/TCP, stdlib
// only (net/http), mirroring timeline_rpc.go's
// deliberately boring transport choice. No
// external dependencies: nothing in this file
// imports anything outside the standard library
// and this package.
//
// PARTITION TESTING HOOK: BlockPeer/UnblockPeer
// refuse to SEND to a peer (both directions are
// blocked by blocking on both sides). This is how
// tests isolate a minority without firewall rules.
// It is a test/operator control, not a safety
// mechanism: blocking never makes an unsafe
// commit possible, it only drops messages, and a
// minority leader's propose then fails with
// ErrRaftNoMajority (or ErrRaftStaleTerm once a
// higher term is observed).

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"hash/fnv"
	"math/rand"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Raft sentinel errors. Compare with errors.Is.
var (
	// ErrRaftNotLeader reports a propose to a node
	// that is not the current leader. The leader hint
	// (if known) is in the error text; the caller
	// should retry against the leader.
	ErrRaftNotLeader = errors.New("yowza raft: node is not the leader")
	// ErrRaftStaleTerm reports a commit attempt by a
	// leader whose term has been superseded. It also
	// wraps ErrCoordStaleEpoch: a stale Raft leader
	// is a stale fencing epoch by the same rule as
	// the local coordinator.
	ErrRaftStaleTerm = errors.New("yowza raft: stale term, leadership lost")
	// ErrRaftNoMajority reports a propose that could
	// not reach a majority within the timeout
	// (partitioned minority, or too many nodes
	// down). Nothing was committed by this call;
	// the entry may still be in the leader's log
	// as uncommitted and may be overwritten later.
	ErrRaftNoMajority = errors.New("yowza raft: no majority within propose timeout")
	// ErrRaftStopped reports use of a stopped node.
	ErrRaftStopped = errors.New("yowza raft: node stopped")
)

// RaftPeer is one static cluster member.
type RaftPeer struct {
	ID   string `json:"id"`
	Addr string `json:"addr"` // host:port for HTTP peer RPC
}

// RaftConfig configures one node. Peers must include
// this node itself (its own ID/Addr); the node finds
// itself by ID. ElectionTimeoutMin/Max and
// HeartbeatInterval default when zero; ProposeTimeout
// defaults to 5s when zero.
type RaftConfig struct {
	ID                 string
	Peers              []RaftPeer
	Dir                string
	ListenAddr         string // e.g. "127.0.0.1:18101"; :0 allowed
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration
	ProposeTimeout     time.Duration
	Apply              func(entry RaftLogEntry) error // state-machine apply, in log order
}

// RaftLogEntry is one replicated log entry. Index is
// 1-based and contiguous; Term is the leader term
// that created it; Kind selects the state-machine
// operation (see raft_coord_state.go); Data is the
// operation payload (JSON).
type RaftLogEntry struct {
	Index uint64          `json:"index"`
	Term  uint64          `json:"term"`
	Kind  string          `json:"kind"`
	Data  json.RawMessage `json:"data"`
}

// raftState is the persisted node state (term, vote,
// log, commit/applied progress), CRC-sealed like the
// other coordinator files.
type raftState struct {
	Version     int            `json:"version"`
	CurrentTerm uint64         `json:"current_term"`
	VotedFor    string         `json:"voted_for"`
	Log         []RaftLogEntry `json:"log"`
	CommitIndex uint64         `json:"commit_index"`
	LastApplied uint64         `json:"last_applied"`
	Checksum    uint32         `json:"checksum"`
}

func raftStateChecksum(s *raftState) uint32 {
	body, _ := json.Marshal(struct {
		Version     int            `json:"version"`
		CurrentTerm uint64         `json:"current_term"`
		VotedFor    string         `json:"voted_for"`
		Log         []RaftLogEntry `json:"log"`
		CommitIndex uint64         `json:"commit_index"`
		LastApplied uint64         `json:"last_applied"`
	}{Version: s.Version, CurrentTerm: s.CurrentTerm, VotedFor: s.VotedFor, Log: s.Log, CommitIndex: s.CommitIndex, LastApplied: s.LastApplied})
	return crc32.ChecksumIEEE(body)
}

func (s *raftState) encode() ([]byte, error) {
	s.Checksum = raftStateChecksum(s)
	return json.Marshal(s)
}

func raftStateDecode(data []byte) (*raftState, error) {
	var s raftState
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("%w: raft state unparseable: %v", ErrCoordCorrupt, err)
	}
	if s.Version != 1 {
		return nil, fmt.Errorf("%w: raft state version %d", ErrCoordCorrupt, s.Version)
	}
	if s.Log == nil {
		s.Log = []RaftLogEntry{}
	}
	if raftStateChecksum(&s) != s.Checksum {
		return nil, fmt.Errorf("%w: raft state checksum mismatch", ErrCoordCorrupt)
	}
	// Log contiguity is structural: index i+1 at position i.
	for i, e := range s.Log {
		if e.Index != uint64(i+1) {
			return nil, fmt.Errorf("%w: raft log index %d at position %d", ErrCoordCorrupt, e.Index, i)
		}
	}
	if s.LastApplied > s.CommitIndex {
		return nil, fmt.Errorf("%w: raft lastApplied %d beyond commit %d", ErrCoordCorrupt, s.LastApplied, s.CommitIndex)
	}
	if s.CommitIndex > uint64(len(s.Log)) {
		return nil, fmt.Errorf("%w: raft commit %d beyond log %d", ErrCoordCorrupt, s.CommitIndex, len(s.Log))
	}
	return &s, nil
}

// RaftNode is one Raft peer. The zero value is not
// usable; construct with NewRaftNode, then Start.
type RaftNode struct {
	cfg    RaftConfig
	dir    string
	peers  map[string]RaftPeer // by ID, including self
	apply  func(entry RaftLogEntry) error
	client *http.Client

	mu          sync.Mutex
	currentTerm uint64
	votedFor    string
	log         []RaftLogEntry
	commitIndex uint64
	lastApplied uint64
	state       string // "follower", "candidate", "leader"
	leaderID    string
	votes       map[string]bool
	nextIndex   map[string]uint64
	matchIndex  map[string]uint64
	blocked     map[string]bool // peer IDs this node refuses to send to
	stopped     bool

	resetCh     chan struct{}
	replicateCh chan struct{}
	applyCh     chan struct{}
	stopCh      chan struct{}
	wg          sync.WaitGroup

	ln   net.Listener
	srv  *http.Server
	addr string
	rng  *rand.Rand
}

// NewRaftNode loads persisted state (or starts fresh)
// but does not start serving or elections; call
// Start. It never consults any local coordinator
// flock: the replicated log is the authority.
func NewRaftNode(cfg RaftConfig) (*RaftNode, error) {
	if cfg.ID == "" {
		return nil, fmt.Errorf("yowza raft: empty node id")
	}
	if len(cfg.Peers) == 0 {
		return nil, fmt.Errorf("yowza raft: empty peer set")
	}
	if cfg.Dir == "" {
		return nil, fmt.Errorf("yowza raft: empty dir")
	}
	if cfg.ListenAddr == "" {
		return nil, fmt.Errorf("yowza raft: empty listen addr")
	}
	if cfg.Apply == nil {
		return nil, fmt.Errorf("yowza raft: nil apply function")
	}
	if cfg.ElectionTimeoutMin <= 0 {
		cfg.ElectionTimeoutMin = 300 * time.Millisecond
	}
	if cfg.ElectionTimeoutMax <= cfg.ElectionTimeoutMin {
		cfg.ElectionTimeoutMax = 600 * time.Millisecond
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = 100 * time.Millisecond
	}
	if cfg.ProposeTimeout <= 0 {
		cfg.ProposeTimeout = 5 * time.Second
	}
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, err
	}
	peers := map[string]RaftPeer{}
	for _, p := range cfg.Peers {
		if p.ID == "" || p.Addr == "" {
			return nil, fmt.Errorf("yowza raft: peer with empty id or addr")
		}
		peers[p.ID] = p
	}
	if _, ok := peers[cfg.ID]; !ok {
		return nil, fmt.Errorf("yowza raft: peer set does not include self %q", cfg.ID)
	}
	n := &RaftNode{
		cfg: cfg, dir: cfg.Dir, peers: peers, apply: cfg.Apply,
		client:      &http.Client{Timeout: 2 * time.Second},
		log:         []RaftLogEntry{},
		state:       "follower",
		votes:       map[string]bool{},
		nextIndex:   map[string]uint64{},
		matchIndex:  map[string]uint64{},
		blocked:     map[string]bool{},
		resetCh:     make(chan struct{}, 1),
		replicateCh: make(chan struct{}, 1),
		applyCh:     make(chan struct{}, 1),
		stopCh:      make(chan struct{}),
		rng:         rand.New(rand.NewSource(raftSeed(cfg.ID))),
	}
	if data, err := os.ReadFile(n.statePath()); err == nil {
		st, err := raftStateDecode(data)
		if err != nil {
			return nil, err
		}
		n.currentTerm = st.CurrentTerm
		n.votedFor = st.VotedFor
		n.log = st.Log
		n.commitIndex = st.CommitIndex
		n.lastApplied = st.LastApplied
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	return n, nil
}

func (n *RaftNode) statePath() string { return filepath.Join(n.dir, "raft-state.json") }

// raftSeed mixes wall time with an FNV hash of the
// node ID, so two same-length IDs started in the
// same instant still randomise differently (the
// earlier time+len(ID) seed could synchronise
// their election timers and prolong campaigns).
func raftSeed(id string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(id))
	return time.Now().UnixNano() ^ int64(h.Sum64())
}

// persistLocked writes current persistent state
// atomically. Caller holds n.mu. It is called
// BEFORE any RPC reply that depends on the new
// state, so a crash never resurrects an old term,
// vote, or log prefix.
func (n *RaftNode) persistLocked() error {
	st := &raftState{
		Version: 1, CurrentTerm: n.currentTerm, VotedFor: n.votedFor,
		Log: n.log, CommitIndex: n.commitIndex, LastApplied: n.lastApplied,
	}
	data, err := st.encode()
	if err != nil {
		return err
	}
	return coordWriteAtomic(n.dir, n.statePath(), data)
}

// Addr returns the listen address after Start.
func (n *RaftNode) Addr() string { return n.addr }

// ID returns the node ID.
func (n *RaftNode) ID() string { return n.cfg.ID }

// BlockPeer refuses to send RPCs to peerID until
// UnblockPeer. Used to simulate partitions.
func (n *RaftNode) BlockPeer(peerID string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.blocked[peerID] = true
}

// UnblockPeer reverses BlockPeer.
func (n *RaftNode) UnblockPeer(peerID string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.blocked, peerID)
}

func (n *RaftNode) isBlockedLocked(peerID string) bool { return n.blocked[peerID] }

// RaftStatus is a point-in-time node report.
type RaftStatus struct {
	ID           string `json:"id"`
	State        string `json:"state"`
	Term         uint64 `json:"term"`
	LeaderID     string `json:"leader_id"`
	CommitIndex  uint64 `json:"commit_index"`
	LastApplied  uint64 `json:"last_applied"`
	LastLogIndex uint64 `json:"last_log_index"`
	LastLogTerm  uint64 `json:"last_log_term"`
}

// Status returns the node's current status.
func (n *RaftNode) Status() RaftStatus {
	n.mu.Lock()
	defer n.mu.Unlock()
	li, lt := n.lastLogLocked()
	return RaftStatus{
		ID: n.cfg.ID, State: n.state, Term: n.currentTerm, LeaderID: n.leaderID,
		CommitIndex: n.commitIndex, LastApplied: n.lastApplied,
		LastLogIndex: li, LastLogTerm: lt,
	}
}

// IsLeader reports whether this node currently
// believes it is leader.
func (n *RaftNode) IsLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state == "leader" && !n.stopped
}

func (n *RaftNode) lastLogLocked() (index uint64, term uint64) {
	if len(n.log) == 0 {
		return 0, 0
	}
	last := n.log[len(n.log)-1]
	return last.Index, last.Term
}

func (n *RaftNode) signalReset() {
	select {
	case n.resetCh <- struct{}{}:
	default:
	}
}

func (n *RaftNode) signalReplicate() {
	select {
	case n.replicateCh <- struct{}{}:
	default:
	}
}

func (n *RaftNode) signalApply() {
	select {
	case n.applyCh <- struct{}{}:
	default:
	}
}

func (n *RaftNode) majority() int { return len(n.peers)/2 + 1 }

// Start serves peer RPC and starts election,
// leader-replication, and apply loops.
func (n *RaftNode) Start() error {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return fmt.Errorf("%w: start after stop", ErrRaftStopped)
	}
	if n.srv != nil {
		n.mu.Unlock()
		return fmt.Errorf("yowza raft: node already started")
	}
	n.mu.Unlock()
	ln, err := net.Listen("tcp", n.cfg.ListenAddr)
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/raft/vote", n.handleVote)
	mux.HandleFunc("/raft/append", n.handleAppend)
	n.ln = ln
	n.addr = ln.Addr().String()
	n.srv = &http.Server{Handler: mux}
	n.wg.Add(4)
	go func() { defer n.wg.Done(); _ = n.srv.Serve(ln) }()
	go func() { defer n.wg.Done(); n.electionLoop() }()
	go func() { defer n.wg.Done(); n.leaderLoop() }()
	go func() { defer n.wg.Done(); n.applyLoop() }()
	// A restarted node re-applies nothing itself:
	// the apply loop replays committed entries the
	// state machine has not yet recorded only if
	// lastApplied lags commitIndex; the machine
	// itself rebuilds from the log on start (see
	// raft_coord_state.go), so a replay here is
	// idempotent by construction.
	n.signalApply()
	return nil
}

// Stop halts all loops and the listener. It does not
// delete persisted state: a restart reloads term,
// vote, log, and commit progress.
func (n *RaftNode) Stop() {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return
	}
	n.stopped = true
	n.mu.Unlock()
	close(n.stopCh)
	if n.srv != nil {
		_ = n.srv.Close()
	}
	n.wg.Wait()
}

func (n *RaftNode) electionTimeout() time.Duration {
	span := n.cfg.ElectionTimeoutMax - n.cfg.ElectionTimeoutMin
	if span <= 0 {
		return n.cfg.ElectionTimeoutMin
	}
	return n.cfg.ElectionTimeoutMin + time.Duration(n.rng.Int63n(int64(span)))
}

func (n *RaftNode) electionLoop() {
	for {
		timer := time.NewTimer(n.electionTimeout())
		select {
		case <-n.stopCh:
			timer.Stop()
			return
		case <-n.resetCh:
			timer.Stop()
			continue
		case <-timer.C:
		}
		n.mu.Lock()
		if n.stopped || n.state == "leader" {
			n.mu.Unlock()
			continue
		}
		n.mu.Unlock()
		n.startElection()
	}
}

func (n *RaftNode) startElection() {
	n.mu.Lock()
	if n.stopped || n.state == "leader" {
		n.mu.Unlock()
		return
	}
	n.currentTerm++
	n.votedFor = n.cfg.ID
	n.state = "candidate"
	n.votes = map[string]bool{n.cfg.ID: true}
	term := n.currentTerm
	li, lt := n.lastLogLocked()
	if err := n.persistLocked(); err != nil {
		// Persist failure is loud: a candidate that
		// cannot persist its vote must not campaign
		// on a term it cannot prove. Step back to
		// follower and wait for a leader.
		n.state = "follower"
		n.mu.Unlock()
		return
	}
	peers := make([]RaftPeer, 0, len(n.peers))
	for _, p := range n.peers {
		if p.ID != n.cfg.ID {
			peers = append(peers, p)
		}
	}
	n.mu.Unlock()
	if len(n.peers) == 1 {
		n.mu.Lock()
		if n.state == "candidate" && n.currentTerm == term {
			n.becomeLeaderLocked()
		}
		n.mu.Unlock()
		return
	}
	for _, p := range peers {
		go n.requestVote(p, term, li, lt)
	}
}

func (n *RaftNode) becomeLeaderLocked() {
	n.state = "leader"
	n.leaderID = n.cfg.ID
	for id := range n.peers {
		if id == n.cfg.ID {
			continue
		}
		n.nextIndex[id] = uint64(len(n.log)) + 1
		n.matchIndex[id] = 0
	}
	n.signalReplicate()
}

func (n *RaftNode) leaderLoop() {
	ticker := time.NewTicker(n.cfg.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-n.replicateCh:
		case <-ticker.C:
		}
		n.mu.Lock()
		if n.stopped || n.state != "leader" {
			n.mu.Unlock()
			continue
		}
		peers := make([]RaftPeer, 0, len(n.peers))
		for _, p := range n.peers {
			if p.ID != n.cfg.ID {
				peers = append(peers, p)
			}
		}
		n.mu.Unlock()
		for _, p := range peers {
			go n.replicateTo(p)
		}
	}
}

// stepDownLocked moves to follower at newTerm
// (newTerm > currentTerm). Caller holds n.mu.
func (n *RaftNode) stepDownLocked(newTerm uint64) {
	if newTerm <= n.currentTerm {
		if n.state != "follower" {
			n.state = "follower"
			n.leaderID = ""
		}
		return
	}
	n.currentTerm = newTerm
	n.votedFor = ""
	n.state = "follower"
	n.leaderID = ""
	_ = n.persistLocked()
	n.signalReset()
}

// --- Peer RPC wire types ---

type raftVoteRequest struct {
	Term         uint64 `json:"term"`
	CandidateID  string `json:"candidate_id"`
	LastLogIndex uint64 `json:"last_log_index"`
	LastLogTerm  uint64 `json:"last_log_term"`
}

type raftVoteResponse struct {
	Term    uint64 `json:"term"`
	Granted bool   `json:"granted"`
}

type raftAppendRequest struct {
	Term         uint64         `json:"term"`
	LeaderID     string         `json:"leader_id"`
	PrevLogIndex uint64         `json:"prev_log_index"`
	PrevLogTerm  uint64         `json:"prev_log_term"`
	Entries      []RaftLogEntry `json:"entries"`
	LeaderCommit uint64         `json:"leader_commit"`
}

type raftAppendResponse struct {
	Term    uint64 `json:"term"`
	Success bool   `json:"success"`
}

func (n *RaftNode) postJSON(peer RaftPeer, path string, req any, out any) error {
	n.mu.Lock()
	blocked := n.isBlockedLocked(peer.ID)
	n.mu.Unlock()
	if blocked {
		return fmt.Errorf("yowza raft: peer %s blocked (partition simulation)", peer.ID)
	}
	data, err := json.Marshal(req)
	if err != nil {
		return err
	}
	url := "http://" + peer.Addr + path
	resp, err := n.client.Post(url, "application/json", bytes.NewReader(data))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("yowza raft: peer %s %s status %d", peer.ID, path, resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func (n *RaftNode) requestVote(peer RaftPeer, term, lastIndex, lastTerm uint64) {
	var out raftVoteResponse
	err := n.postJSON(peer, "/raft/vote", raftVoteRequest{
		Term: term, CandidateID: n.cfg.ID, LastLogIndex: lastIndex, LastLogTerm: lastTerm,
	}, &out)
	if err != nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if out.Term > n.currentTerm {
		n.stepDownLocked(out.Term)
		return
	}
	if n.state != "candidate" || n.currentTerm != term {
		return
	}
	if out.Granted {
		n.votes[peer.ID] = true
		if len(n.votes) >= n.majority() {
			n.becomeLeaderLocked()
		}
	}
}

func (n *RaftNode) replicateTo(peer RaftPeer) {
	n.mu.Lock()
	if n.state != "leader" || n.stopped {
		n.mu.Unlock()
		return
	}
	term := n.currentTerm
	next := n.nextIndex[peer.ID]
	if next == 0 {
		next = uint64(len(n.log)) + 1
	}
	var prevIndex, prevTerm uint64
	if next > 1 {
		prevIndex = next - 1
		if int(prevIndex) <= len(n.log) {
			prevTerm = n.log[prevIndex-1].Term
		} else {
			// nextIndex beyond our log: nothing to
			// send yet; the heartbeat below still
			// carries leaderCommit.
			next = uint64(len(n.log)) + 1
			prevIndex = uint64(len(n.log))
			if prevIndex > 0 {
				prevTerm = n.log[prevIndex-1].Term
			}
		}
	}
	var entries []RaftLogEntry
	for i := next; i <= uint64(len(n.log)); i++ {
		entries = append(entries, n.log[i-1])
	}
	leaderCommit := n.commitIndex
	n.mu.Unlock()

	var out raftAppendResponse
	err := n.postJSON(peer, "/raft/append", raftAppendRequest{
		Term: term, LeaderID: n.cfg.ID, PrevLogIndex: prevIndex, PrevLogTerm: prevTerm,
		Entries: entries, LeaderCommit: leaderCommit,
	}, &out)
	if err != nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if out.Term > n.currentTerm {
		n.stepDownLocked(out.Term)
		return
	}
	if n.state != "leader" || n.currentTerm != term {
		return
	}
	if out.Success {
		sent := prevIndex + uint64(len(entries))
		if sent > n.matchIndex[peer.ID] {
			n.matchIndex[peer.ID] = sent
		}
		n.nextIndex[peer.ID] = sent + 1
		n.advanceCommitLocked()
	} else {
		// Log inconsistency: back nextIndex down one
		// and retry on the next heartbeat/replicate.
		// It never goes below 1.
		if n.nextIndex[peer.ID] > 1 {
			n.nextIndex[peer.ID]--
		}
		n.signalReplicate()
	}
}

// advanceCommitLocked implements the Raft commit
// rule: advance to N only if a majority holds N and
// log[N] is from the CURRENT term (older-term
// entries commit indirectly when a current-term
// entry commits past them). Caller holds n.mu.
func (n *RaftNode) advanceCommitLocked() {
	for idx := uint64(len(n.log)); idx > n.commitIndex; idx-- {
		if n.log[idx-1].Term != n.currentTerm {
			continue
		}
		count := 1 // leader holds it
		for id, m := range n.matchIndex {
			if id != n.cfg.ID && m >= idx {
				count++
			}
		}
		if count >= n.majority() {
			n.commitIndex = idx
			_ = n.persistLocked()
			n.signalApply()
			return
		}
	}
}

// Propose appends an entry as leader and waits until
// it is committed by a majority, the term is
// superseded (stale fencing error), or the timeout
// fires (no majority). Only a leader may propose;
// followers fail fast with ErrRaftNotLeader.
// The returned term is the entry's term (the fencing
// epoch the state machine records for the entry).
func (n *RaftNode) Propose(kind string, data []byte, timeout time.Duration) (index uint64, entryTerm uint64, err error) {
	if timeout <= 0 {
		timeout = n.cfg.ProposeTimeout
	}
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return 0, 0, fmt.Errorf("%w", ErrRaftStopped)
	}
	if n.state != "leader" {
		leader := n.leaderID
		n.mu.Unlock()
		if leader == "" {
			return 0, 0, fmt.Errorf("%w: no leader known", ErrRaftNotLeader)
		}
		return 0, 0, fmt.Errorf("%w: leader is %s", ErrRaftNotLeader, leader)
	}
	entryTerm = n.currentTerm
	index = uint64(len(n.log)) + 1
	n.log = append(n.log, RaftLogEntry{Index: index, Term: entryTerm, Kind: kind, Data: append([]byte(nil), data...)})
	if err := n.persistLocked(); err != nil {
		// Roll back the in-memory append: the entry
		// was never made durable, so it must not be
		// replicated or committed from this node.
		n.log = n.log[:len(n.log)-1]
		n.mu.Unlock()
		return 0, 0, err
	}
	// Single-node cluster: there is no peer reply
	// to trigger commit advancement, so advance
	// here (a no-op for multi-node clusters, where
	// the entry is not yet majority-held).
	if n.majority() == 1 {
		n.advanceCommitLocked()
	}
	n.mu.Unlock()
	n.signalReplicate()
	deadline := time.Now().Add(timeout)
	for {
		n.mu.Lock()
		if n.stopped {
			n.mu.Unlock()
			return 0, 0, fmt.Errorf("%w", ErrRaftStopped)
		}
		// Commit first: an entry that committed
		// stays committed even if leadership changed
		// immediately after (checking term first
		// would misreport a committed entry as
		// stale — a real bug, not a race).
		if n.commitIndex >= index {
			n.mu.Unlock()
			return index, entryTerm, nil
		}
		if n.currentTerm > entryTerm {
			newTerm := n.currentTerm
			n.mu.Unlock()
			return 0, entryTerm, fmt.Errorf("%w: entry term %d superseded by term %d: %w", ErrRaftStaleTerm, entryTerm, newTerm, ErrCoordStaleEpoch)
		}
		n.mu.Unlock()
		if time.Now().After(deadline) {
			return 0, entryTerm, fmt.Errorf("%w: entry %d term %d not committed", ErrRaftNoMajority, index, entryTerm)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// applyLoop applies committed entries in log order.
// A machine error is logged and the entry is
// SKIPPED (lastApplied still advances): a committed
// entry cannot be uncommitted, and every node
// applies the same log in the same order, so a
// deterministic skip preserves convergence while
// a stuck loop would halt all later commits.
// Machine apply functions are therefore written to
// be total over committed entries (see
// raft_coord_state.go); a skip here is a loud
// bug signal, never a silent data path.
func (n *RaftNode) applyLoop() {
	for {
		select {
		case <-n.stopCh:
			return
		case <-n.applyCh:
		case <-time.After(20 * time.Millisecond):
		}
		for {
			n.mu.Lock()
			if n.stopped {
				n.mu.Unlock()
				return
			}
			if n.lastApplied >= n.commitIndex || int(n.lastApplied) >= len(n.log) {
				n.mu.Unlock()
				break
			}
			entry := n.log[n.lastApplied]
			n.mu.Unlock()
			if err := n.apply(entry); err != nil {
				// Loud, then skip (see header above).
				fmt.Fprintf(os.Stderr, "yowza raft: apply of entry %d kind %q failed, skipping: %v\n", entry.Index, entry.Kind, err)
			}
			n.mu.Lock()
			if entry.Index == n.lastApplied+1 {
				n.lastApplied = entry.Index
				_ = n.persistLocked()
			}
			n.mu.Unlock()
		}
	}
}

// --- RPC handlers ---

func (n *RaftNode) handleVote(w http.ResponseWriter, r *http.Request) {
	var req raftVoteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	resp := raftVoteResponse{Term: n.currentTerm}
	if req.Term < n.currentTerm {
		_ = json.NewEncoder(w).Encode(resp)
		return
	}
	if req.Term > n.currentTerm {
		n.stepDownLocked(req.Term)
		resp.Term = n.currentTerm
	}
	// One vote per term per voter, and only for an
	// up-to-date candidate (Raft §5.4.1).
	li, lt := n.lastLogLocked()
	upToDate := req.LastLogTerm > lt || (req.LastLogTerm == lt && req.LastLogIndex >= li)
	if (n.votedFor == "" || n.votedFor == req.CandidateID) && upToDate {
		n.votedFor = req.CandidateID
		_ = n.persistLocked()
		resp.Granted = true
		n.signalReset()
	}
	_ = json.NewEncoder(w).Encode(resp)
}

func (n *RaftNode) handleAppend(w http.ResponseWriter, r *http.Request) {
	var req raftAppendRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	resp := raftAppendResponse{Term: n.currentTerm}
	if req.Term < n.currentTerm {
		// Stale leader: refused everywhere, with
		// our higher term so it steps down.
		_ = json.NewEncoder(w).Encode(resp)
		return
	}
	if req.Term > n.currentTerm {
		n.stepDownLocked(req.Term)
		resp.Term = n.currentTerm
	}
	// A current-term leader is recognised even if
	// the log check below fails: reset the election
	// timer so a catching-up follower does not
	// start a spurious election mid-catch-up.
	n.state = "follower"
	n.leaderID = req.LeaderID
	n.signalReset()
	// Prev-log consistency check (Raft §5.3).
	if req.PrevLogIndex > 0 {
		if uint64(len(n.log)) < req.PrevLogIndex {
			_ = json.NewEncoder(w).Encode(resp)
			return
		}
		if n.log[req.PrevLogIndex-1].Term != req.PrevLogTerm {
			_ = json.NewEncoder(w).Encode(resp)
			return
		}
	}
	// Append new entries, truncating any conflicting
	// suffix: this is how a healed follower's
	// UNCOMMITTED entries are overwritten by the
	// leader's log, while committed entries (which
	// the leader must hold) are never truncated.
	changed := false
	for _, e := range req.Entries {
		if e.Index <= uint64(len(n.log)) {
			if n.log[e.Index-1].Term != e.Term {
				n.log = append(append([]RaftLogEntry(nil), n.log[:e.Index-1]...), e)
				changed = true
			}
			continue
		}
		if e.Index == uint64(len(n.log))+1 {
			n.log = append(n.log, e)
			changed = true
			continue
		}
		// Gap after a passing prev check cannot
		// happen for a well-formed leader; refuse
		// rather than inventing a log.
		_ = json.NewEncoder(w).Encode(resp)
		return
	}
	if changed {
		_ = n.persistLocked()
	}
	if req.LeaderCommit > n.commitIndex {
		lastNew := uint64(len(n.log))
		if req.LeaderCommit < lastNew {
			lastNew = req.LeaderCommit
		}
		n.commitIndex = lastNew
		_ = n.persistLocked()
		n.signalApply()
	}
	resp.Success = true
	_ = json.NewEncoder(w).Encode(resp)
}
