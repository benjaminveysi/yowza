package yowza

// Track B acceptance tests — REAL SEPARATE PROCESSES.
//
// Each node is a raftcoord binary (cmd/raftcoord)
// with its own directory and ports, started with
// os/exec. Leader death is SIGKILL (a crash, not
// a graceful stop); partitions are admin-API peer
// blocks; convergence is checked through the API
// and through committed-op files on disk after
// restart. Nothing here is an in-process
// simulation of the core guarantees: the processes
// are real, their Raft state is on disk, and a
// killed leader's in-memory leadership is gone.
//
// These tests build the binary with `go build`
// (stdlib + already-cached module deps only).
// If the build fails, the test fails loudly —
// there is no fallback to in-process nodes.

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func buildRaftcoordBinary(t *testing.T) string {
	t.Helper()
	out := filepath.Join(t.TempDir(), "raftcoord")
	goBin := filepath.Join(os.Getenv("HOME"), "workspace", "tools", "go", "bin")
	cmd := exec.Command("go", "build", "-o", out, "./cmd/raftcoord")
	cmd.Env = append(os.Environ(), "PATH="+goBin+":"+os.Getenv("PATH"))
	var stderr strings.Builder
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("build raftcoord: %v: %s", err, stderr.String())
	}
	return out
}

type raftProcCluster struct {
	t         *testing.T
	bin       string
	base      string
	ids       []string
	peers     []RaftPeer
	peerAddrs map[string]string
	apiAddrs  map[string]string
	dirs      map[string]string
	procs     map[string]*exec.Cmd
	clients   map[string]*RaftCoordClient
}

func startRaftProcCluster(t *testing.T, bin string, ids []string) *raftProcCluster {
	t.Helper()
	c := &raftProcCluster{
		t: t, bin: bin, base: t.TempDir(), ids: ids,
		peerAddrs: map[string]string{}, apiAddrs: map[string]string{},
		dirs: map[string]string{}, procs: map[string]*exec.Cmd{},
		clients: map[string]*RaftCoordClient{},
	}
	for _, id := range ids {
		c.peerAddrs[id] = fmt.Sprintf("127.0.0.1:%d", raftFreePort(t))
		c.apiAddrs[id] = fmt.Sprintf("127.0.0.1:%d", raftFreePort(t))
		c.dirs[id] = filepath.Join(c.base, id)
		c.peers = append(c.peers, RaftPeer{ID: id, Addr: c.peerAddrs[id]})
	}
	for _, id := range ids {
		c.startProc(id)
	}
	t.Cleanup(func() {
		for _, id := range ids {
			c.killProc(id)
		}
	})
	return c
}

func (c *raftProcCluster) peerFlag() string {
	parts := make([]string, 0, len(c.peers))
	for _, p := range c.peers {
		parts = append(parts, p.ID+"="+p.Addr)
	}
	return strings.Join(parts, ",")
}

func (c *raftProcCluster) startProc(id string) {
	c.t.Helper()
	if c.procs[id] != nil {
		c.t.Fatalf("proc %s already running", id)
	}
	logPath := filepath.Join(c.base, id+".log")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		c.t.Fatalf("open proc log %s: %v", id, err)
	}
	cmd := exec.Command(c.bin,
		"--id", id,
		"--peer-addr", c.peerAddrs[id],
		"--api-addr", c.apiAddrs[id],
		"--dir", c.dirs[id],
		"--peers", c.peerFlag(),
	)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		c.t.Fatalf("start proc %s: %v", id, err)
	}
	c.procs[id] = cmd
	c.clients[id] = DialRaftCoord(c.apiAddrs[id])
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if err := c.clients[id].Health(); err == nil {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.t.Fatalf("proc %s never became healthy (see %s)", id, logPath)
}

// killProc SIGKILLs the process (crash simulation)
// and waits for it. It is safe to call on a
// non-running proc.
func (c *raftProcCluster) killProc(id string) {
	cmd := c.procs[id]
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = cmd.Process.Kill()
	_ = cmd.Wait()
	c.procs[id] = nil
}

func (c *raftProcCluster) blockSymmetric(a, b string) {
	c.t.Helper()
	if err := c.clients[a].BlockPeer(b); err != nil {
		c.t.Fatalf("block %s->%s: %v", a, b, err)
	}
	if err := c.clients[b].BlockPeer(a); err != nil {
		c.t.Fatalf("block %s->%s: %v", b, a, err)
	}
}

func (c *raftProcCluster) unblockSymmetric(a, b string) {
	c.t.Helper()
	if err := c.clients[a].UnblockPeer(b); err != nil {
		c.t.Fatalf("unblock %s->%s: %v", a, b, err)
	}
	if err := c.clients[b].UnblockPeer(a); err != nil {
		c.t.Fatalf("unblock %s->%s: %v", b, a, err)
	}
}

func (c *raftProcCluster) waitLeader(ids []string, timeout time.Duration) string {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	stableID := ""
	stableCount := 0
	for time.Now().Before(deadline) {
		found := ""
		for _, id := range ids {
			st, err := c.clients[id].Status()
			if err != nil {
				continue
			}
			if st.Node.State == "leader" {
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

func (c *raftProcCluster) waitCommitted(ids []string, opID string, timeout time.Duration) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok := true
		for _, id := range ids {
			ops, err := c.clients[id].Committed()
			if err != nil {
				ok = false
				break
			}
			found := false
			for _, op := range ops {
				if op.OpID == opID {
					found = true
					break
				}
			}
			if !found {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.t.Fatalf("op %s not committed on all of %v within %v", opID, ids, timeout)
}

func (c *raftProcCluster) assertNotCommitted(ids []string, opID string) {
	c.t.Helper()
	for _, id := range ids {
		ops, err := c.clients[id].Committed()
		if err != nil {
			c.t.Fatalf("committed %s: %v", id, err)
		}
		for _, op := range ops {
			if op.OpID == opID {
				c.t.Fatalf("node %s holds op %s, want absent", id, opID)
			}
		}
		if _, err := os.Stat(filepath.Join(c.dirs[id], "raft-committed-"+opID+".json")); err == nil {
			c.t.Fatalf("node %s has a committed file for op %s", id, opID)
		}
	}
}

func (c *raftProcCluster) proposeViaLeader(opID string) {
	c.t.Helper()
	var lastErr error
	for attempt := 0; attempt < 15; attempt++ {
		leader := c.waitLeader(c.ids, 30*time.Second)
		err := c.clients[leader].ProposeCommit(opID, "proc-owner", raftTestWrites())
		if err == nil {
			return
		}
		lastErr = err
		if strings.Contains(err.Error(), "not the leader") || strings.Contains(err.Error(), "stale") {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		c.t.Fatalf("propose %s via leader %s: %v", opID, leader, err)
	}
	c.t.Fatalf("propose %s never succeeded, last: %v", opID, lastErr)
}

func TestRaftProcessLeaderDeathAndReelection(t *testing.T) {
	bin := buildRaftcoordBinary(t)
	c := startRaftProcCluster(t, bin, []string{"n1", "n2", "n3"})
	leader := c.waitLeader(c.ids, 30*time.Second)
	t.Logf("initial leader: %s", leader)
	c.proposeViaLeader("op1")
	c.waitCommitted(c.ids, "op1", 30*time.Second)
	// SIGKILL the leader process: a crash, not a
	// graceful stop. Its in-memory leadership dies
	// with it; its disk state survives.
	c.killProc(leader)
	remaining := []string{}
	for _, id := range c.ids {
		if id != leader {
			remaining = append(remaining, id)
		}
	}
	newLeader := c.waitLeader(remaining, 30*time.Second)
	if newLeader == leader {
		t.Fatalf("killed leader %s still reported", leader)
	}
	t.Logf("new leader after SIGKILL: %s", newLeader)
	if err := c.clients[newLeader].ProposeCommit("op2", "proc-owner", raftTestWrites()); err != nil {
		t.Fatalf("propose via new leader: %v", err)
	}
	c.waitCommitted(remaining, "op2", 30*time.Second)
	// Committed op1 survives the leader change.
	c.waitCommitted(remaining, "op1", 30*time.Second)
	// Restart the killed leader from its persisted
	// dir: it must rejoin and converge to both ops.
	c.startProc(leader)
	c.waitCommitted(c.ids, "op1", 30*time.Second)
	c.waitCommitted(c.ids, "op2", 30*time.Second)
	// Committed-op files identical across processes.
	var first []byte
	for _, id := range c.ids {
		data, err := os.ReadFile(filepath.Join(c.dirs[id], "raft-committed-op2.json"))
		if err != nil {
			t.Fatalf("node %s committed file: %v", id, err)
		}
		if first == nil {
			first = data
		} else if string(first) != string(data) {
			t.Fatalf("node %s committed file differs", id)
		}
	}
}

func TestRaftProcessMinorityStaleAndHeal(t *testing.T) {
	bin := buildRaftcoordBinary(t)
	c := startRaftProcCluster(t, bin, []string{"n1", "n2", "n3"})
	c.proposeViaLeader("op-base")
	c.waitCommitted(c.ids, "op-base", 30*time.Second)
	leader := c.waitLeader(c.ids, 30*time.Second)
	others := []string{}
	for _, id := range c.ids {
		if id != leader {
			others = append(others, id)
		}
	}
	// Isolate the leader symmetrically.
	for _, o := range others {
		c.blockSymmetric(leader, o)
	}
	// Minority commit attempt must fail, and the
	// op must be committed nowhere.
	err := c.clients[leader].ProposeCommit("op-minority", "proc-owner", raftTestWrites())
	if err == nil {
		t.Fatalf("minority commit unexpectedly succeeded")
	}
	t.Logf("minority commit failed as required: %v", err)
	c.assertNotCommitted(c.ids, "op-minority")
	// Majority side elects a new leader and commits.
	newLeader := c.waitLeader(others, 30*time.Second)
	if err := c.clients[newLeader].ProposeCommit("op-majority", "proc-owner", raftTestWrites()); err != nil {
		t.Fatalf("majority propose: %v", err)
	}
	c.waitCommitted(others, "op-majority", 30*time.Second)
	// Stale leader attempt: heal the old leader's
	// SEND path only, so its AppendEntries reach
	// followers who reply with the higher term.
	// Its propose must fail with a fencing error.
	for _, o := range others {
		if err := c.clients[leader].UnblockPeer(o); err != nil {
			t.Fatalf("unblock %s->%s: %v", leader, o, err)
		}
	}
	err = c.clients[leader].ProposeCommit("op-stale", "proc-owner", raftTestWrites())
	if err == nil {
		t.Fatalf("stale leader commit unexpectedly succeeded")
	}
	if !strings.Contains(err.Error(), "stale") {
		t.Fatalf("stale leader err = %v, want it to contain 'stale'", err)
	}
	t.Logf("stale leader refused as required: %v", err)
	c.assertNotCommitted(c.ids, "op-stale")
	// Full heal: old leader converges, minority
	// op never appears anywhere.
	for _, o := range others {
		if err := c.clients[o].UnblockPeer(leader); err != nil {
			t.Fatalf("unblock %s->%s: %v", o, leader, err)
		}
	}
	c.waitCommitted(c.ids, "op-majority", 30*time.Second)
	c.assertNotCommitted(c.ids, "op-minority")
	c.assertNotCommitted(c.ids, "op-stale")
	// Committed sets identical across nodes.
	var want []string
	for _, id := range c.ids {
		ops, err := c.clients[id].Committed()
		if err != nil {
			t.Fatalf("committed %s: %v", id, err)
		}
		var got []string
		for _, op := range ops {
			got = append(got, op.OpID)
		}
		if want == nil {
			want = got
		} else if strings.Join(want, ",") != strings.Join(got, ",") {
			t.Fatalf("node %s committed %v, want %v", id, got, want)
		}
	}
	if len(want) != 2 {
		t.Fatalf("committed sets = %v, want [op-base op-majority]", want)
	}
}

func TestRaftProcessCrashAtomicity(t *testing.T) {
	bin := buildRaftcoordBinary(t)
	c := startRaftProcCluster(t, bin, []string{"n1", "n2", "n3"})
	c.proposeViaLeader("op-base")
	c.waitCommitted(c.ids, "op-base", 30*time.Second)
	leader := c.waitLeader(c.ids, 30*time.Second)
	others := []string{}
	for _, id := range c.ids {
		if id != leader {
			others = append(others, id)
		}
	}
	// Crash BEFORE commit -> all-old: isolate the
	// leader, fail a propose, SIGKILL it, heal and
	// restart. The op must exist nowhere.
	for _, o := range others {
		c.blockSymmetric(leader, o)
	}
	err := c.clients[leader].ProposeCommit("op-pre", "proc-owner", raftTestWrites())
	if err == nil {
		t.Fatalf("isolated propose unexpectedly succeeded")
	}
	c.killProc(leader)
	for _, o := range others {
		if err := c.clients[o].UnblockPeer(leader); err != nil {
			t.Fatalf("unblock %s->%s: %v", o, leader, err)
		}
	}
	c.startProc(leader)
	c.waitLeader(c.ids, 30*time.Second)
	c.assertNotCommitted(c.ids, "op-pre")
	// Crash AFTER commit -> all-new via recovery:
	// commit, SIGKILL every process, restart every
	// process, and the op must be present on all.
	c.proposeViaLeader("op-post")
	c.waitCommitted(c.ids, "op-post", 30*time.Second)
	for _, id := range c.ids {
		c.killProc(id)
	}
	for _, id := range c.ids {
		c.startProc(id)
	}
	c.waitLeader(c.ids, 30*time.Second)
	c.waitCommitted(c.ids, "op-post", 30*time.Second)
	for _, id := range c.ids {
		if _, err := os.Stat(filepath.Join(c.dirs[id], "raft-committed-op-post.json")); err != nil {
			t.Fatalf("node %s missing committed file after full restart: %v", id, err)
		}
	}
}
