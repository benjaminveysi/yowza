package yowza

// Track B tests — in-process clusters (real HTTP peer
// RPC on loopback, distinct dirs/ports per node).
// The REAL-process guarantees (kill -9, restart,
// partition-by-process) live in
// raft_coord_process_test.go; these tests cover the
// protocol rules quickly in-process.

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func raftFreePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

type raftTestCluster struct {
	t        *testing.T
	sessions []*RaftCoordinatorSession
	servers  []*RaftCoordServer
	clients  []*RaftCoordClient
	dirs     []string
	ids      []string
}

func startRaftTestCluster(t *testing.T, ids []string) *raftTestCluster {
	t.Helper()
	base := t.TempDir()
	peerPorts := make([]int, len(ids))
	apiPorts := make([]int, len(ids))
	for i := range ids {
		peerPorts[i] = raftFreePort(t)
		apiPorts[i] = raftFreePort(t)
	}
	var peers []RaftPeer
	for i, id := range ids {
		peers = append(peers, RaftPeer{ID: id, Addr: fmt.Sprintf("127.0.0.1:%d", peerPorts[i])})
	}
	c := &raftTestCluster{t: t, ids: ids}
	for i, id := range ids {
		dir := filepath.Join(base, id)
		cfg := RaftConfig{
			ID: id, Peers: peers, Dir: dir,
			ListenAddr: fmt.Sprintf("127.0.0.1:%d", peerPorts[i]),
		}
		sess, err := NewRaftCoordinator(cfg)
		if err != nil {
			t.Fatalf("new raft coordinator %s: %v", id, err)
		}
		if err := sess.Start(); err != nil {
			t.Fatalf("start raft coordinator %s: %v", id, err)
		}
		srv, err := StartRaftCoordServer(sess, fmt.Sprintf("127.0.0.1:%d", apiPorts[i]))
		if err != nil {
			t.Fatalf("start api server %s: %v", id, err)
		}
		c.sessions = append(c.sessions, sess)
		c.servers = append(c.servers, srv)
		c.clients = append(c.clients, DialRaftCoord(srv.Addr()))
		c.dirs = append(c.dirs, dir)
	}
	t.Cleanup(func() {
		for _, srv := range c.servers {
			_ = srv.Close()
		}
		for _, sess := range c.sessions {
			sess.Stop()
		}
	})
	return c
}

func (c *raftTestCluster) session(id string) *RaftCoordinatorSession {
	for i, want := range c.ids {
		if want == id {
			return c.sessions[i]
		}
	}
	c.t.Fatalf("unknown node %s", id)
	return nil
}

func (c *raftTestCluster) client(id string) *RaftCoordClient {
	for i, want := range c.ids {
		if want == id {
			return c.clients[i]
		}
	}
	c.t.Fatalf("unknown node %s", id)
	return nil
}

func (c *raftTestCluster) dir(id string) string {
	for i, want := range c.ids {
		if want == id {
			return c.dirs[i]
		}
	}
	c.t.Fatalf("unknown node %s", id)
	return ""
}

// waitLeader returns the ID of a node reporting
// leader state, waiting up to timeout. It requires
// stability: the same node must report leader on
// three consecutive polls, because nodes starting
// sequentially churn leadership (a lone starter
// campaigns at climbing terms) before heartbeats
// settle. Callers that propose still retry on
// NotLeader/StaleTerm.
func (c *raftTestCluster) waitLeader(timeout time.Duration) string {
	deadline := time.Now().Add(timeout)
	stableID := ""
	stableCount := 0
	for time.Now().Before(deadline) {
		found := ""
		for _, sess := range c.sessions {
			if sess == nil {
				continue
			}
			if sess.IsLeader() {
				found = sess.Node().ID()
				break
			}
		}
		if found != "" && found == stableID {
			stableCount++
			if stableCount >= 3 {
				return found
			}
		} else {
			stableID = found
			stableCount = 0
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.t.Fatalf("no stable leader elected within %v", timeout)
	return ""
}

// waitApplied waits until a session's local apply
// has caught up with its commit index (propose
// waits for commit, not for local apply).
func waitApplied(t *testing.T, sess *RaftCoordinatorSession, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		st := sess.Status()
		if st.LastApplied >= st.CommitIndex {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("local apply lag: %+v", sess.Status())
}

// waitLeaderAmong waits for a leader among the
// given IDs only (used after a leader is stopped),
// with the same stability requirement as
// waitLeader: a transient leader that steps down
// before its first propose must not be returned.
func (c *raftTestCluster) waitLeaderAmong(ids []string, timeout time.Duration) string {
	deadline := time.Now().Add(timeout)
	stableID := ""
	stableCount := 0
	for time.Now().Before(deadline) {
		found := ""
		for _, id := range ids {
			sess := c.session(id)
			if sess != nil && sess.IsLeader() {
				found = id
				break
			}
		}
		if found != "" && found == stableID {
			stableCount++
			if stableCount >= 3 {
				return found
			}
		} else {
			stableID = found
			stableCount = 0
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.t.Fatalf("no stable leader among %v within %v", ids, timeout)
	return ""
}

// waitCommitted waits until every listed node has
// applied opID (or fails).
func (c *raftTestCluster) waitCommitted(ids []string, opID string, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok := true
		for _, id := range ids {
			sess := c.session(id)
			if sess == nil {
				ok = false
				break
			}
			if _, found := sess.Machine().CommittedOp(opID); !found {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	c.t.Fatalf("op %s not committed on all of %v within %v", opID, ids, timeout)
}

// proposeViaLeader proposes through whoever is
// leader, retrying across leadership changes.
func (c *raftTestCluster) proposeViaLeader(opID string, writes map[string][]globalMergeDelta) {
	c.t.Helper()
	var lastErr error
	for attempt := 0; attempt < 10; attempt++ {
		leader := c.waitLeader(10 * time.Second)
		_, _, err := c.session(leader).ProposeMultiShardCommit(opID, "test-owner", writes)
		if err == nil {
			return
		}
		lastErr = err
		// Leadership churn during startup can make
		// a picked leader stale before its entry
		// commits; the entry is then uncommitted
		// and safe to retry via the new leader
		// (machine apply dedups by opID anyway).
		if errors.Is(err, ErrRaftNotLeader) || errors.Is(err, ErrRaftStaleTerm) {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		c.t.Fatalf("propose %s via leader %s: %v", opID, leader, err)
	}
	c.t.Fatalf("propose %s never succeeded, last: %v", opID, lastErr)
}

func raftTestWrites() map[string][]globalMergeDelta {
	return map[string][]globalMergeDelta{
		"shardA": {{Key: "k1", Value: []byte("v1")}},
		"shardB": {{Key: "k2", Value: []byte("v2")}, {Key: "k3", Deleted: true}},
	}
}

func TestRaftElectionAndCommit(t *testing.T) {
	c := startRaftTestCluster(t, []string{"n1", "n2", "n3"})
	leader := c.waitLeader(10 * time.Second)
	t.Logf("leader: %s term %d", leader, c.session(leader).Status().Term)
	c.proposeViaLeader("op1", raftTestWrites())
	c.waitCommitted([]string{"n1", "n2", "n3"}, "op1", 10*time.Second)
	// Committed-op files identical on every node.
	var first []byte
	for _, id := range []string{"n1", "n2", "n3"} {
		data, err := os.ReadFile(filepath.Join(c.dir(id), "raft-committed-op1.json"))
		if err != nil {
			t.Fatalf("node %s committed file: %v", id, err)
		}
		if first == nil {
			first = data
		} else if !bytes.Equal(first, data) {
			t.Fatalf("node %s committed file differs from n1", id)
		}
	}
	// WAL frames: contiguous sequences, identical everywhere.
	var firstWAL []RaftWALFrame
	for _, id := range []string{"n1", "n2", "n3"} {
		frames := c.session(id).Machine().WALFrames()
		if len(frames) == 0 {
			t.Fatalf("node %s has no WAL frames", id)
		}
		for i, f := range frames {
			if f.Seq != uint64(i+1) {
				t.Fatalf("node %s WAL frame %d has seq %d", id, i, f.Seq)
			}
		}
		if firstWAL == nil {
			firstWAL = frames
		} else if len(frames) != len(firstWAL) {
			t.Fatalf("node %s WAL length %d != %d", id, len(frames), len(firstWAL))
		}
	}
	// Participant recovery view.
	ops, err := RecoverRaftMultiShard(c.dir("n2"))
	if err != nil {
		t.Fatalf("RecoverRaftMultiShard: %v", err)
	}
	if len(ops) != 1 || ops[0].OpID != "op1" {
		t.Fatalf("RecoverRaftMultiShard = %+v, want [op1]", ops)
	}
	if len(ops[0].Writes) != 2 {
		t.Fatalf("RecoverRaftMultiShard writes = %d shards, want 2", len(ops[0].Writes))
	}
}

func TestRaftLeaderDeathReelection(t *testing.T) {
	c := startRaftTestCluster(t, []string{"n1", "n2", "n3"})
	leader := c.waitLeader(10 * time.Second)
	c.proposeViaLeader("op1", raftTestWrites())
	c.waitCommitted([]string{"n1", "n2", "n3"}, "op1", 10*time.Second)
	// Stop (not kill — the kill case is the process
	// test) the leader; a new leader must serve.
	leaderSess := c.session(leader)
	leaderSess.Stop()
	remaining := []string{}
	for _, id := range []string{"n1", "n2", "n3"} {
		if id != leader {
			remaining = append(remaining, id)
		}
	}
	newLeader := c.waitLeaderAmong(remaining, 10*time.Second)
	if newLeader == leader {
		t.Fatalf("dead leader %s still reported as leader", leader)
	}
	_, _, err := c.session(newLeader).ProposeMultiShardCommit("op2", "test-owner", raftTestWrites())
	if err != nil {
		t.Fatalf("propose via new leader %s: %v", newLeader, err)
	}
	c.waitCommitted(remaining, "op2", 10*time.Second)
	// Restart the old leader on the same dir and
	// listen address: it must rejoin as follower
	// and converge to both ops. The old API server
	// still points at the stopped session, so this
	// section checks convergence through the new
	// session directly (API-level restart is the
	// process test's job).
	idx := -1
	for i, id := range c.ids {
		if id == leader {
			idx = i
		}
	}
	oldCfg := leaderSess.Node().cfg
	restarted, err := NewRaftCoordinator(RaftConfig{
		ID: oldCfg.ID, Peers: oldCfg.Peers, Dir: oldCfg.Dir,
		ListenAddr: oldCfg.ListenAddr,
	})
	if err != nil {
		t.Fatalf("new restarted session: %v", err)
	}
	if err := restarted.Start(); err != nil {
		t.Fatalf("restart session: %v", err)
	}
	t.Cleanup(func() { restarted.Stop() })
	c.sessions[idx] = restarted
	c.waitCommitted([]string{"n1", "n2", "n3"}, "op1", 10*time.Second)
	c.waitCommitted([]string{"n1", "n2", "n3"}, "op2", 10*time.Second)
}

func TestRaftVoteRulesOneVotePerTermUpToDate(t *testing.T) {
	dir := t.TempDir()
	node, err := NewRaftNode(RaftConfig{
		ID: "v1", Dir: dir, ListenAddr: "127.0.0.1:0",
		Peers: []RaftPeer{{ID: "v1", Addr: "127.0.0.1:1"}},
		Apply: func(entry RaftLogEntry) error { return nil },
	})
	if err != nil {
		t.Fatalf("new node: %v", err)
	}
	node.mu.Lock()
	node.currentTerm = 5
	node.log = []RaftLogEntry{{Index: 1, Term: 5, Kind: "wal-record"}}
	node.mu.Unlock()
	callVote := func(term uint64, cand string, lastIdx, lastTerm uint64) raftVoteResponse {
		body, _ := json.Marshal(raftVoteRequest{Term: term, CandidateID: cand, LastLogIndex: lastIdx, LastLogTerm: lastTerm})
		req := httptest.NewRequest("POST", "/raft/vote", bytes.NewReader(body))
		rec := httptest.NewRecorder()
		node.handleVote(rec, req)
		var out raftVoteResponse
		if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
			t.Fatalf("decode vote response: %v", err)
		}
		return out
	}
	// Stale log candidate in a higher term: denied.
	if out := callVote(6, "candA", 0, 0); out.Granted {
		t.Fatalf("granted vote to stale-log candidate")
	}
	// Up-to-date candidate in term 6: granted.
	if out := callVote(6, "candB", 1, 5); !out.Granted {
		t.Fatalf("denied vote to up-to-date candidate")
	}
	// Second candidate in the same term: denied
	// (one vote per term per voter).
	if out := callVote(6, "candC", 1, 5); out.Granted {
		t.Fatalf("granted second vote in same term")
	}
	// Old term: denied outright.
	if out := callVote(5, "candB", 1, 5); out.Granted {
		t.Fatalf("granted vote for old term")
	}
}

func TestRaftLeaseCoordination(t *testing.T) {
	c := startRaftTestCluster(t, []string{"n1", "n2", "n3"})
	leader := c.waitLeader(10 * time.Second)
	ls := c.session(leader)
	lease, err := ls.ProposeLeaseAcquire("lease1", "ownerA", []string{"heap-a", "heap-b"}, 0)
	if err != nil {
		t.Fatalf("acquire lease1: %v", err)
	}
	if lease.Epoch == 0 {
		t.Fatalf("lease epoch is 0")
	}
	// Visible on every node, identical.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		all := true
		for _, id := range []string{"n1", "n2", "n3"} {
			l, ok := c.session(id).Machine().Lease("lease1")
			if !ok || l.OwnerID != "ownerA" || l.Epoch != lease.Epoch {
				all = false
				break
			}
		}
		if all {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	for _, id := range []string{"n1", "n2", "n3"} {
		l, ok := c.session(id).Machine().Lease("lease1")
		if !ok || l.OwnerID != "ownerA" || l.Epoch != lease.Epoch {
			t.Fatalf("node %s lease mismatch: %+v ok=%v", id, l, ok)
		}
	}
	// Duplicate live lease: refused.
	if _, err := ls.ProposeLeaseAcquire("lease1", "ownerB", []string{"heap-c"}, 0); !errors.Is(err, ErrCoordLeaseExists) {
		t.Fatalf("duplicate acquire err = %v, want ErrCoordLeaseExists", err)
	}
	// Non-owner release: refused, lease survives.
	if err := ls.ProposeLeaseRelease("lease1", "ownerB", lease.Epoch); !errors.Is(err, ErrCoordLeaseNotOwner) {
		t.Fatalf("non-owner release err = %v, want ErrCoordLeaseNotOwner", err)
	}
	if _, ok := ls.Machine().Lease("lease1"); !ok {
		t.Fatalf("lease deleted by refused non-owner release")
	}
	// Wrong-epoch release: refused, lease survives.
	if err := ls.ProposeLeaseRelease("lease1", "ownerA", lease.Epoch+99); !errors.Is(err, ErrCoordLeaseStaleEpoch) {
		t.Fatalf("wrong-epoch release err = %v, want ErrCoordLeaseStaleEpoch", err)
	}
	if _, ok := ls.Machine().Lease("lease1"); !ok {
		t.Fatalf("lease deleted by refused stale-epoch release")
	}
	// Correct release: gone everywhere.
	if err := ls.ProposeLeaseRelease("lease1", "ownerA", lease.Epoch); err != nil {
		t.Fatalf("correct release: %v", err)
	}
	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		gone := true
		for _, id := range []string{"n1", "n2", "n3"} {
			if _, ok := c.session(id).Machine().Lease("lease1"); ok {
				gone = false
				break
			}
		}
		if gone {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	for _, id := range []string{"n1", "n2", "n3"} {
		if _, ok := c.session(id).Machine().Lease("lease1"); ok {
			t.Fatalf("node %s still holds released lease", id)
		}
	}
	// Expiry: short TTL, then the ID is reusable.
	_, err = ls.ProposeLeaseAcquire("lease-exp", "ownerA", []string{"heap-x"}, 400*time.Millisecond)
	if err != nil {
		t.Fatalf("acquire expiring lease: %v", err)
	}
	time.Sleep(600 * time.Millisecond)
	if _, err := ls.ProposeLeaseAcquire("lease-exp", "ownerB", []string{"heap-y"}, 0); err != nil {
		t.Fatalf("re-acquire after expiry: %v", err)
	}
}

func TestRaftMinorityCannotCommitAndHealConverges(t *testing.T) {
	c := startRaftTestCluster(t, []string{"n1", "n2", "n3"})
	c.waitLeader(10 * time.Second)
	c.proposeViaLeader("op-base", raftTestWrites())
	c.waitCommitted([]string{"n1", "n2", "n3"}, "op-base", 10*time.Second)
	// Re-pick the leader AFTER the cluster has
	// settled on a commit: the first leader seen
	// during startup churn may no longer lead.
	leader := c.waitLeader(10 * time.Second)
	others := []string{}
	for _, id := range []string{"n1", "n2", "n3"} {
		if id != leader {
			others = append(others, id)
		}
	}
	// Isolate the leader symmetrically. The
	// isolated node must still believe it leads:
	// otherwise the propose below would fail for
	// the wrong reason (not leader) instead of
	// proving a minority cannot commit.
	ls := c.session(leader)
	if !ls.IsLeader() {
		t.Fatalf("picked leader %s no longer leads before isolation", leader)
	}
	for _, o := range others {
		ls.Node().BlockPeer(o)
		c.session(o).Node().BlockPeer(leader)
	}
	// Minority propose must fail (no majority).
	_, _, err := ls.ProposeMultiShardCommit("op-minority", "test-owner", raftTestWrites())
	if err == nil {
		t.Fatalf("minority commit unexpectedly succeeded")
	}
	if !errors.Is(err, ErrRaftNoMajority) && !errors.Is(err, ErrRaftStaleTerm) {
		t.Fatalf("minority commit err = %v, want NoMajority or StaleTerm", err)
	}
	// The minority op is committed nowhere.
	for _, id := range []string{"n1", "n2", "n3"} {
		if _, ok := c.session(id).Machine().CommittedOp("op-minority"); ok {
			t.Fatalf("node %s holds minority op", id)
		}
	}
	// Majority side elects a new leader and commits.
	newLeader := c.waitLeaderAmong(others, 10*time.Second)
	_, _, err = c.session(newLeader).ProposeMultiShardCommit("op-majority", "test-owner", raftTestWrites())
	if err != nil {
		t.Fatalf("majority propose: %v", err)
	}
	c.waitCommitted(others, "op-majority", 10*time.Second)
	// Heal: the old leader's uncommitted entry must
	// be overwritten, never committed anywhere.
	for _, o := range others {
		ls.Node().UnblockPeer(o)
		c.session(o).Node().UnblockPeer(leader)
	}
	c.waitCommitted([]string{"n1", "n2", "n3"}, "op-majority", 10*time.Second)
	for _, id := range []string{"n1", "n2", "n3"} {
		if _, ok := c.session(id).Machine().CommittedOp("op-minority"); ok {
			t.Fatalf("node %s committed the minority op after heal", id)
		}
		if _, err := os.Stat(filepath.Join(c.dir(id), "raft-committed-op-minority.json")); err == nil {
			t.Fatalf("node %s has a committed file for the minority op", id)
		}
	}
	// A post-heal commit lands identically everywhere.
	c.proposeViaLeader("op-after-heal", raftTestWrites())
	c.waitCommitted([]string{"n1", "n2", "n3"}, "op-after-heal", 10*time.Second)
}

func TestRaftStaleLeaderFencing(t *testing.T) {
	c := startRaftTestCluster(t, []string{"n1", "n2", "n3"})
	oldLeader := c.waitLeader(10 * time.Second)
	oldTerm := c.session(oldLeader).Status().Term
	c.proposeViaLeader("op-base", raftTestWrites())
	c.waitCommitted([]string{"n1", "n2", "n3"}, "op-base", 10*time.Second)
	others := []string{}
	for _, id := range []string{"n1", "n2", "n3"} {
		if id != oldLeader {
			others = append(others, id)
		}
	}
	// Partition the old leader away; the majority
	// elects a new leader at a higher term.
	oldSess := c.session(oldLeader)
	for _, o := range others {
		oldSess.Node().BlockPeer(o)
		c.session(o).Node().BlockPeer(oldLeader)
	}
	newLeader := c.waitLeaderAmong(others, 10*time.Second)
	if c.session(newLeader).Status().Term <= oldTerm {
		t.Fatalf("new leader term %d not above old term %d",
			c.session(newLeader).Status().Term, oldTerm)
	}
	_, _, err := c.session(newLeader).ProposeMultiShardCommit("op-newleader", "test-owner", raftTestWrites())
	if err != nil {
		t.Fatalf("new leader propose: %v", err)
	}
	c.waitCommitted(others, "op-newleader", 10*time.Second)
	// Heal the old leader's SEND path only: its
	// AppendEntries now reach followers, who reply
	// with the higher term, so its propose must
	// fail with a fencing error — not time out
	// silently, and not commit.
	for _, o := range others {
		oldSess.Node().UnblockPeer(o)
	}
	_, _, err = oldSess.ProposeMultiShardCommit("op-stale", "test-owner", raftTestWrites())
	if err == nil {
		t.Fatalf("stale leader commit unexpectedly succeeded")
	}
	if !errors.Is(err, ErrRaftStaleTerm) {
		t.Fatalf("stale leader err = %v, want ErrRaftStaleTerm", err)
	}
	if !errors.Is(err, ErrCoordStaleEpoch) {
		t.Fatalf("stale leader err = %v, want it to wrap ErrCoordStaleEpoch", err)
	}
	if _, ok := oldSess.Machine().CommittedOp("op-stale"); ok {
		t.Fatalf("stale op committed on old leader")
	}
	for _, o := range others {
		if _, ok := c.session(o).Machine().CommittedOp("op-stale"); ok {
			t.Fatalf("stale op committed on %s", o)
		}
	}
	// Full heal: old leader converges, no divergence.
	for _, o := range others {
		c.session(o).Node().UnblockPeer(oldLeader)
	}
	c.waitCommitted([]string{"n1", "n2", "n3"}, "op-newleader", 10*time.Second)
	if got := oldSess.Status().State; got == "leader" {
		// It may briefly still believe it leads
		// until the next heartbeat; wait for
		// follower state.
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) && oldSess.Status().State == "leader" {
			time.Sleep(25 * time.Millisecond)
		}
		if oldSess.Status().State == "leader" {
			t.Fatalf("old leader still leader after full heal")
		}
	}
}

func TestRaftSingleNodeAndWAL(t *testing.T) {
	c := startRaftTestCluster(t, []string{"only"})
	leader := c.waitLeader(10 * time.Second)
	if leader != "only" {
		t.Fatalf("single node leader = %s", leader)
	}
	sess := c.session("only")
	if _, err := sess.ProposeWALRecord("domain-commit", "op-wal", "owner", []string{"a", "b"}); err != nil {
		t.Fatalf("wal record: %v", err)
	}
	_, _, err := sess.ProposeMultiShardCommit("op-wal-commit", "owner", raftTestWrites())
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	// Propose waits for commit, not local apply.
	waitApplied(t, sess, 10*time.Second)
	frames := sess.Machine().WALFrames()
	if len(frames) != 2 {
		t.Fatalf("WAL frames = %d, want 2", len(frames))
	}
	if frames[0].Seq != 1 || frames[1].Seq != 2 {
		t.Fatalf("WAL seqs = %d,%d, want 1,2", frames[0].Seq, frames[1].Seq)
	}
	if frames[0].OpID != "op-wal" || frames[1].OpID != "op-wal-commit" {
		t.Fatalf("WAL opIDs = %q,%q", frames[0].OpID, frames[1].OpID)
	}
}

func TestRaftPersistedStateSurvivesRestart(t *testing.T) {
	// Direct node-level restart: same dir, same
	// config, new session object.
	base := t.TempDir()
	peerPorts := []int{raftFreePort(t), raftFreePort(t), raftFreePort(t)}
	peers := []RaftPeer{
		{ID: "n1", Addr: fmt.Sprintf("127.0.0.1:%d", peerPorts[0])},
		{ID: "n2", Addr: fmt.Sprintf("127.0.0.1:%d", peerPorts[1])},
		{ID: "n3", Addr: fmt.Sprintf("127.0.0.1:%d", peerPorts[2])},
	}
	mkSession := func(id string, port int) *RaftCoordinatorSession {
		sess, err := NewRaftCoordinator(RaftConfig{
			ID: id, Peers: peers, Dir: filepath.Join(base, id),
			ListenAddr: fmt.Sprintf("127.0.0.1:%d", port),
		})
		if err != nil {
			t.Fatalf("new session %s: %v", id, err)
		}
		return sess
	}
	sessions := []*RaftCoordinatorSession{
		mkSession("n1", peerPorts[0]), mkSession("n2", peerPorts[1]), mkSession("n3", peerPorts[2]),
	}
	for _, s := range sessions {
		if err := s.Start(); err != nil {
			t.Fatalf("start: %v", err)
		}
	}
	defer func() {
		for _, s := range sessions {
			s.Stop()
		}
	}()
	// Wait for a STABLE leader and a commit
	// (startup churn can transiently elect a node
	// that steps down before its propose lands).
	var leader *RaftCoordinatorSession
	deadline := time.Now().Add(15 * time.Second)
	stableID := ""
	stableCount := 0
	for time.Now().Before(deadline) && leader == nil {
		var found *RaftCoordinatorSession
		for _, s := range sessions {
			if s.IsLeader() {
				found = s
				break
			}
		}
		if found != nil && found.Node().ID() == stableID {
			stableCount++
			if stableCount >= 3 {
				leader = found
			}
		} else if found != nil {
			stableID = found.Node().ID()
			stableCount = 0
		} else {
			stableID = ""
			stableCount = 0
		}
		time.Sleep(50 * time.Millisecond)
	}
	if leader == nil {
		t.Fatalf("no stable leader")
	}
	// Retry across any last leadership change.
	proposed := false
	for attempt := 0; attempt < 10 && !proposed; attempt++ {
		if _, _, err := leader.ProposeMultiShardCommit("op1", "owner", raftTestWrites()); err == nil {
			proposed = true
		} else if errors.Is(err, ErrRaftNotLeader) || errors.Is(err, ErrRaftStaleTerm) {
			time.Sleep(100 * time.Millisecond)
			for _, s := range sessions {
				if s.IsLeader() {
					leader = s
					break
				}
			}
		} else {
			t.Fatalf("propose: %v", err)
		}
	}
	if !proposed {
		t.Fatalf("propose never succeeded across leadership changes")
	}
	time.Sleep(500 * time.Millisecond)
	// Committed-op file bytes before restart, for
	// the snapshot-position regression check below
	// (a restarted node must rewrite byte-identical
	// files, not index-0 files).
	fileBefore, err := os.ReadFile(filepath.Join(leader.Node().cfg.Dir, "raft-committed-op1.json"))
	if err != nil {
		t.Fatalf("committed file before restart: %v", err)
	}
	termBefore := leader.Status().Term
	logBefore := leader.Status().LastLogIndex
	leaderAddr := leader.Node().cfg.ListenAddr
	leader.Stop()
	// Restart the same node: fresh session on the
	// same dir and the SAME listen address the
	// leader actually used (not another node's).
	restarted, err := NewRaftCoordinator(RaftConfig{
		ID: leader.Node().ID(), Peers: peers, Dir: leader.Node().cfg.Dir,
		ListenAddr: leaderAddr,
	})
	if err != nil {
		t.Fatalf("new restarted session: %v", err)
	}
	// Find which slot the leader occupied.
	for i, s := range sessions {
		if s == leader {
			sessions[i] = restarted
		}
	}
	if err := restarted.Start(); err != nil {
		t.Fatalf("restart: %v", err)
	}
	st := restarted.Status()
	if st.Term < termBefore {
		t.Fatalf("term regressed: %d < %d", st.Term, termBefore)
	}
	if st.LastLogIndex < logBefore {
		t.Fatalf("log shrank on restart: %d < %d", st.LastLogIndex, logBefore)
	}
	// The restarted node must still hold the commit.
	deadline = time.Now().Add(10 * time.Second)
	found := false
	for time.Now().Before(deadline) && !found {
		_, found = restarted.Machine().CommittedOp("op1")
		if !found {
			time.Sleep(25 * time.Millisecond)
		}
	}
	if !found {
		t.Fatalf("restarted node lost committed op1")
	}
	// Snapshot positions must survive restart:
	// the rewritten file is byte-identical.
	fileAfter, err := os.ReadFile(filepath.Join(restarted.Node().cfg.Dir, "raft-committed-op1.json"))
	if err != nil {
		t.Fatalf("committed file after restart: %v", err)
	}
	if !bytes.Equal(fileBefore, fileAfter) {
		t.Fatalf("committed file changed across restart:\nbefore %s\nafter %s", fileBefore, fileAfter)
	}
}
