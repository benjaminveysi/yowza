package yowza

// Axis B acceptance tests.
//
// Process-vs-goroutine honesty (stated per test):
//   - Cross-process exclusion, lease visibility, and
//     WAL continuation are proven with REAL separate
//     processes (TestCoordSubprocessHelper, re-execing
//     the test binary) wherever the contract is about
//     processes.
//   - In-process races (concurrent appends on one
//     session, concurrent domain applies) are proven
//     with goroutines, because the race itself is
//     in-process (shared p.mu / session mutex); the
//     test names say so.
//   - The stale-owner commit attempt is proven
//     in-process (release + takeover + old-session
//     commit) because a released subprocess has no
//     session object left to call Commit on; the
//     subprocess tests prove the exclusion half
//     (a live owner blocks a second acquire) that
//     makes the in-process stale test the right
//     complement, not a substitute.

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// coordHelperEnv builds the environment for a helper
// subprocess invocation. The helper is selected by
// -test.run, so a normal full-suite run executes it
// as a no-op (no env set) and passes immediately.
func coordRunHelper(t *testing.T, mode string, extra map[string]string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run", "^TestCoordSubprocessHelper$", "-test.count=1")
	env := append([]string{}, os.Environ()...)
	env = append(env, "YOWZA_COORD_HELPER_MODE="+mode)
	for k, v := range extra {
		env = append(env, "YOWZA_COORD_HELPER_"+k+"="+v)
	}
	cmd.Env = env
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Start(); err != nil {
		t.Fatalf("start helper %s: %v", mode, err)
	}
	// Stash output for a later Wait failure diagnostic.
	t.Cleanup(func() {
		_ = out.String()
	})
	cmd.WaitDelay = 0
	// Attach the buffer via a wrapper: callers use
	// coordWaitHelper for the diagnostic.
	coordHelperOutputs[cmd] = &out
	return cmd
}

var coordHelperOutputs = map[*exec.Cmd]*bytes.Buffer{}

func coordWaitHelper(t *testing.T, cmd *exec.Cmd) {
	t.Helper()
	if err := cmd.Wait(); err != nil {
		out := ""
		if b, ok := coordHelperOutputs[cmd]; ok {
			out = b.String()
		}
		t.Fatalf("helper exit: %v\noutput:\n%s", err, out)
	}
}

func coordWaitFile(t *testing.T, path string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", path)
}

// TestCoordSubprocessHelper is NOT a standalone test:
// without YOWZA_COORD_HELPER_MODE it returns at once.
// With it, it performs one coordinator action as a
// separate process (see coordRunHelper callers).
func TestCoordSubprocessHelper(t *testing.T) {
	mode := os.Getenv("YOWZA_COORD_HELPER_MODE")
	if mode == "" {
		return
	}
	dir := os.Getenv("YOWZA_COORD_HELPER_DIR")
	owner := os.Getenv("YOWZA_COORD_HELPER_OWNER")
	ready := os.Getenv("YOWZA_COORD_HELPER_READY")
	done := os.Getenv("YOWZA_COORD_HELPER_DONE")
	sess, err := AcquireCoordinator(dir, owner)
	if err != nil {
		t.Fatalf("helper acquire: %v", err)
	}
	waitDone := func() {
		deadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(deadline) {
			if _, err := os.Stat(done); err == nil {
				return
			}
			time.Sleep(25 * time.Millisecond)
		}
		t.Fatalf("helper timed out waiting for done file")
	}
	switch mode {
	case "hold":
		if err := os.WriteFile(ready, []byte("ready"), 0o644); err != nil {
			t.Fatalf("helper ready: %v", err)
		}
		waitDone()
		if err := sess.Release(); err != nil {
			t.Fatalf("helper release: %v", err)
		}
	case "lease-hold":
		leaseID := os.Getenv("YOWZA_COORD_HELPER_LEASE")
		pinned := os.Getenv("YOWZA_COORD_HELPER_PINNED")
		if _, err := AcquireCoordLease(sess, dir, leaseID, []string{pinned}); err != nil {
			t.Fatalf("helper lease acquire: %v", err)
		}
		if err := os.WriteFile(ready, []byte("ready"), 0o644); err != nil {
			t.Fatalf("helper ready: %v", err)
		}
		waitDone()
		if err := ReleaseCoordLeaseForSession(sess, dir, leaseID); err != nil {
			t.Fatalf("helper lease release: %v", err)
		}
		if err := sess.Release(); err != nil {
			t.Fatalf("helper release: %v", err)
		}
	case "append":
		count := 5
		if v := os.Getenv("YOWZA_COORD_HELPER_COUNT"); v != "" {
			fmt.Sscanf(v, "%d", &count)
		}
		for i := 0; i < count; i++ {
			if _, err := sess.AppendWALRecord("helper-append", fmt.Sprintf("helper-%d", i), []string{"hk"}); err != nil {
				t.Fatalf("helper append %d: %v", i, err)
			}
		}
		if err := sess.Release(); err != nil {
			t.Fatalf("helper release: %v", err)
		}
	default:
		t.Fatalf("unknown helper mode %q", mode)
	}
}

func TestAxisBCoordFencing(t *testing.T) {
	dir := t.TempDir()
	sessA, err := AcquireCoordinator(dir, "owner-a")
	if err != nil {
		t.Fatalf("acquire A: %v", err)
	}
	if sessA.Epoch() != 1 {
		t.Fatalf("A epoch %d, want 1", sessA.Epoch())
	}
	if seq, err := sessA.AppendWALRecord("commit", "op-a1", []string{"k1"}); err != nil || seq != 1 {
		t.Fatalf("A append seq %d, err %v, want 1", seq, err)
	}
	// Second acquire while A is live: loud refusal, not a block.
	if _, err := AcquireCoordinator(dir, "owner-b"); !errors.Is(err, ErrCoordLocked) {
		t.Fatalf("second acquire err %v, want ErrCoordLocked", err)
	}
	// Stale-owner simulation: A releases, B takes over.
	if err := sessA.Release(); err != nil {
		t.Fatalf("release A: %v", err)
	}
	sessB, err := AcquireCoordinator(dir, "owner-b")
	if err != nil {
		t.Fatalf("acquire B: %v", err)
	}
	defer sessB.Release()
	if sessB.Epoch() != 2 {
		t.Fatalf("B epoch %d, want 2", sessB.Epoch())
	}
	// Old owner commit attempts (WAL + prepare + root)
	// are all refused with the stale epoch, and apply
	// nothing: the WAL gains no frame from A.
	if _, err := sessA.AppendWALRecord("commit", "op-stale", []string{"k2"}); !errors.Is(err, ErrCoordStaleEpoch) {
		t.Fatalf("stale append err %v, want ErrCoordStaleEpoch", err)
	}
	if err := PrepareMultiShard(sessA, dir, "op-stale", map[string][]globalMergeDelta{"a": {{Key: "k", Value: []byte("v")}}}); !errors.Is(err, ErrCoordStaleEpoch) {
		t.Fatalf("stale prepare err %v, want ErrCoordStaleEpoch", err)
	}
	if seq, err := sessB.AppendWALRecord("commit", "op-b1", []string{"k3"}); err != nil || seq != 2 {
		t.Fatalf("B append seq %d, err %v, want 2 (stale append consumed no sequence)", seq, err)
	}
	frames, lastSeq, _, err := ScanCoordWAL(dir)
	if err != nil {
		t.Fatalf("scan WAL: %v", err)
	}
	if lastSeq != 2 || len(frames) != 2 {
		t.Fatalf("WAL frames %v lastSeq %d, want 2 frames ending at 2", frames, lastSeq)
	}
	// Released-without-takeover: a fresh session that
	// is released reports Released, not a silent pass.
	sessC, err := AcquireCoordinator(dir, "owner-c")
	if err != nil {
		// B still holds the coordinator; this acquire
		// must fail locked, which also proves the point
		// (no second live session). Release B first for
		// the released-only check below.
		if !errors.Is(err, ErrCoordLocked) {
			t.Fatalf("acquire C while B live err %v, want ErrCoordLocked", err)
		}
		if err := sessB.Release(); err != nil {
			t.Fatalf("release B: %v", err)
		}
		sessC, err = AcquireCoordinator(dir, "owner-c")
		if err != nil {
			t.Fatalf("acquire C: %v", err)
		}
	} else {
		t.Fatalf("acquire C succeeded while B live; want ErrCoordLocked")
	}
	if err := sessC.Release(); err != nil {
		t.Fatalf("release C: %v", err)
	}
	if _, err := sessC.AppendWALRecord("commit", "op-c1", nil); !errors.Is(err, ErrCoordReleased) {
		t.Fatalf("released append err %v, want ErrCoordReleased", err)
	}
}

func TestAxisBCoordFencingCrossProcess(t *testing.T) {
	coordDir := t.TempDir()
	ready := filepath.Join(t.TempDir(), "ready")
	done := filepath.Join(t.TempDir(), "done")
	cmd := coordRunHelper(t, "hold", map[string]string{
		"DIR": dirValue(coordDir), "OWNER": "holder", "READY": ready, "DONE": done,
	})
	coordWaitFile(t, ready, 10*time.Second)
	// While the holder process is live, a second
	// acquire (this process) fails loudly.
	if _, err := AcquireCoordinator(coordDir, "parent"); !errors.Is(err, ErrCoordLocked) {
		t.Fatalf("acquire while holder live err %v, want ErrCoordLocked", err)
	}
	if err := os.WriteFile(done, []byte("done"), 0o644); err != nil {
		t.Fatalf("write done: %v", err)
	}
	coordWaitHelper(t, cmd)
	// After the holder exits, acquire succeeds at the
	// next epoch (holder was epoch 1).
	sess, err := AcquireCoordinator(coordDir, "parent")
	if err != nil {
		t.Fatalf("acquire after holder exit: %v", err)
	}
	defer sess.Release()
	if sess.Epoch() != 2 {
		t.Fatalf("parent epoch %d, want 2", sess.Epoch())
	}
}

func dirValue(s string) string { return s }

func TestAxisBCoordLease(t *testing.T) {
	coordDir := t.TempDir()
	dataDir := t.TempDir()
	pinnedFile := filepath.Join(dataDir, "pinned.dat")
	otherFile := filepath.Join(dataDir, "other.dat")
	if err := os.WriteFile(pinnedFile, []byte("pinned"), 0o644); err != nil {
		t.Fatalf("write pinned: %v", err)
	}
	if err := os.WriteFile(otherFile, []byte("other"), 0o644); err != nil {
		t.Fatalf("write other: %v", err)
	}
	sess, err := AcquireCoordinator(coordDir, "owner-a")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer sess.Release()
	if _, err := AcquireCoordLease(sess, coordDir, "lease-1", []string{"pinned.dat"}); err != nil {
		t.Fatalf("acquire lease: %v", err)
	}
	// Non-owner release is refused and the lease survives.
	if err := ReleaseCoordLease(coordDir, "lease-1", "intruder", 999); !errors.Is(err, ErrCoordLeaseNotOwner) {
		t.Fatalf("non-owner release err %v, want ErrCoordLeaseNotOwner", err)
	}
	// Wrong-epoch release (even by the right owner id)
	// is refused and the lease survives.
	if err := ReleaseCoordLease(coordDir, "lease-1", "owner-a", 999); !errors.Is(err, ErrCoordLeaseStaleEpoch) {
		t.Fatalf("stale-epoch release err %v, want ErrCoordLeaseStaleEpoch", err)
	}
	live, found, err := IsCoordLeaseLive(coordDir, "lease-1")
	if err != nil || !found || !live {
		t.Fatalf("lease live=%v found=%v err=%v, want live+found", live, found, err)
	}
	// Live-owner pin blocks reclamation of its data
	// even when another (simulated) process compacts:
	// the reclaimer is a plain function call with no
	// session, exactly as another process would call it.
	deleted, kept, err := ReclaimPinnedFiles(coordDir, dataDir, []string{"pinned.dat", "other.dat"})
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if len(kept) != 1 || kept[0] != "pinned.dat" {
		t.Fatalf("reclaim kept %v, want [pinned.dat]", kept)
	}
	if len(deleted) != 1 || deleted[0] != "other.dat" {
		t.Fatalf("reclaim deleted %v, want [other.dat]", deleted)
	}
	if _, err := os.Stat(pinnedFile); err != nil {
		t.Fatalf("pinned file gone after blocked reclaim: %v", err)
	}
	if _, err := os.Stat(otherFile); !os.IsNotExist(err) {
		t.Fatalf("unpinned file still present after reclaim")
	}
	// Owner release frees the pin; reclaim then deletes it.
	if err := ReleaseCoordLeaseForSession(sess, coordDir, "lease-1"); err != nil {
		t.Fatalf("owner release: %v", err)
	}
	deleted, kept, err = ReclaimPinnedFiles(coordDir, dataDir, []string{"pinned.dat"})
	if err != nil {
		t.Fatalf("reclaim after release: %v", err)
	}
	if len(deleted) != 1 || len(kept) != 0 {
		t.Fatalf("reclaim after release deleted=%v kept=%v, want deleted=[pinned.dat] kept=[]", deleted, kept)
	}
	if _, err := os.Stat(pinnedFile); !os.IsNotExist(err) {
		t.Fatalf("pinned file still present after owner release + reclaim")
	}
}

func TestAxisBCoordLeaseCrossProcess(t *testing.T) {
	coordDir := t.TempDir()
	dataDir := t.TempDir()
	pinnedFile := filepath.Join(dataDir, "pinned.dat")
	if err := os.WriteFile(pinnedFile, []byte("pinned"), 0o644); err != nil {
		t.Fatalf("write pinned: %v", err)
	}
	ready := filepath.Join(t.TempDir(), "ready")
	done := filepath.Join(t.TempDir(), "done")
	cmd := coordRunHelper(t, "lease-hold", map[string]string{
		"DIR": coordDir, "OWNER": "holder", "READY": ready, "DONE": done,
		"LEASE": "lease-x", "PINNED": "pinned.dat",
	})
	coordWaitFile(t, ready, 10*time.Second)
	// Lease written by the helper process is visible
	// here and blocks this process's reclamation.
	live, found, err := IsCoordLeaseLive(coordDir, "lease-x")
	if err != nil || !found || !live {
		t.Fatalf("cross-process lease live=%v found=%v err=%v, want live+found", live, found, err)
	}
	if err := ReleaseCoordLease(coordDir, "lease-x", "intruder", 1); !errors.Is(err, ErrCoordLeaseNotOwner) {
		t.Fatalf("cross-process non-owner release err %v, want ErrCoordLeaseNotOwner", err)
	}
	deleted, kept, err := ReclaimPinnedFiles(coordDir, dataDir, []string{"pinned.dat"})
	if err != nil {
		t.Fatalf("cross-process reclaim: %v", err)
	}
	if len(kept) != 1 || len(deleted) != 0 {
		t.Fatalf("cross-process reclaim kept=%v deleted=%v, want kept=[pinned.dat] deleted=[]", kept, deleted)
	}
	if _, err := os.Stat(pinnedFile); err != nil {
		t.Fatalf("pinned file deleted while helper lease live: %v", err)
	}
	if err := os.WriteFile(done, []byte("done"), 0o644); err != nil {
		t.Fatalf("write done: %v", err)
	}
	coordWaitHelper(t, cmd)
	// Helper released its lease before exiting, so a
	// new owner can now reclaim the file.
	sess, err := AcquireCoordinator(coordDir, "parent")
	if err != nil {
		t.Fatalf("acquire after helper: %v", err)
	}
	defer sess.Release()
	deleted, kept, err = ReclaimPinnedFiles(coordDir, dataDir, []string{"pinned.dat"})
	if err != nil {
		t.Fatalf("reclaim after helper release: %v", err)
	}
	if len(deleted) != 1 || len(kept) != 0 {
		t.Fatalf("reclaim after helper release deleted=%v kept=%v, want deleted=[pinned.dat]", deleted, kept)
	}
}

func TestAxisBCoordSharedWAL(t *testing.T) {
	coordDir := t.TempDir()
	sess, err := AcquireCoordinator(coordDir, "owner-a")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	// Concurrent appends from goroutines on ONE live
	// session (goroutine simulation is explicit here:
	// the serialisation under test is the session
	// mutex, an in-process object).
	const goroutines = 8
	const perGoroutine = 25
	var wg sync.WaitGroup
	seqCh := make(chan uint64, goroutines*perGoroutine)
	errCh := make(chan error, goroutines*perGoroutine)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				seq, err := sess.AppendWALRecord("append", fmt.Sprintf("op-%d-%d", g, i), []string{"k"})
				if err != nil {
					errCh <- err
					continue
				}
				seqCh <- seq
			}
		}(g)
	}
	wg.Wait()
	close(seqCh)
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent append err: %v", err)
	}
	seen := map[uint64]bool{}
	for seq := range seqCh {
		if seen[seq] {
			t.Fatalf("duplicate sequence %d", seq)
		}
		seen[seq] = true
	}
	if len(seen) != goroutines*perGoroutine {
		t.Fatalf("sequences seen %d, want %d", len(seen), goroutines*perGoroutine)
	}
	frames, lastSeq, _, err := ScanCoordWAL(coordDir)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if lastSeq != goroutines*perGoroutine || len(frames) != goroutines*perGoroutine {
		t.Fatalf("WAL lastSeq %d frames %d, want %d", lastSeq, len(frames), goroutines*perGoroutine)
	}
	for i, fr := range frames {
		if fr.Seq != uint64(i+1) {
			t.Fatalf("frame %d seq %d, want %d (interleaved or out-of-order stream)", i, fr.Seq, i+1)
		}
	}
	// Stale-owner appends racing live appends: stale
	// attempts error cleanly and consume no sequence,
	// live appends stay contiguous.
	if err := sess.Release(); err != nil {
		t.Fatalf("release: %v", err)
	}
	sessB, err := AcquireCoordinator(coordDir, "owner-b")
	if err != nil {
		t.Fatalf("acquire B: %v", err)
	}
	defer sessB.Release()
	var wg2 sync.WaitGroup
	staleErrs := make(chan error, 10)
	liveErrs := make(chan error, 10)
	liveSeqs := make(chan uint64, 10)
	for i := 0; i < 10; i++ {
		wg2.Add(2)
		go func() {
			defer wg2.Done()
			_, err := sess.AppendWALRecord("stale", "stale-op", nil)
			staleErrs <- err
		}()
		go func(i int) {
			defer wg2.Done()
			seq, err := sessB.AppendWALRecord("live", fmt.Sprintf("live-%d", i), nil)
			if err != nil {
				liveErrs <- err
				return
			}
			liveSeqs <- seq
		}(i)
	}
	wg2.Wait()
	close(staleErrs)
	close(liveErrs)
	close(liveSeqs)
	for err := range staleErrs {
		if !errors.Is(err, ErrCoordStaleEpoch) {
			t.Fatalf("stale attempt err %v, want ErrCoordStaleEpoch", err)
		}
	}
	for err := range liveErrs {
		t.Fatalf("live attempt err %v, want no error", err)
	}
	var live []uint64
	for seq := range liveSeqs {
		live = append(live, seq)
	}
	sort.Slice(live, func(i, j int) bool { return live[i] < live[j] })
	if len(live) != 10 {
		t.Fatalf("live sequences %v, want 10", live)
	}
	for i, seq := range live {
		if seq != uint64(goroutines*perGoroutine+1+i) {
			t.Fatalf("live sequence %d at position %d, want %d (stale attempt consumed a sequence or interleaved)", seq, i, goroutines*perGoroutine+1+i)
		}
	}
}

func TestAxisBCoordSharedWALCrossProcess(t *testing.T) {
	coordDir := t.TempDir()
	// Two sequential helper processes append; the
	// second continues the first's sequence with no
	// duplicate and no gap (a live holder would block
	// the second acquire outright — see the fencing
	// cross-process test — so sequential continuation
	// is the cross-process stream proof here).
	for run := 0; run < 2; run++ {
		cmd := coordRunHelper(t, "append", map[string]string{
			"DIR": coordDir, "OWNER": "helper-owner", "COUNT": "5",
		})
		coordWaitHelper(t, cmd)
	}
	frames, lastSeq, _, err := ScanCoordWAL(coordDir)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if lastSeq != 10 || len(frames) != 10 {
		t.Fatalf("cross-process WAL lastSeq %d frames %d, want 10", lastSeq, len(frames))
	}
	for i, fr := range frames {
		if fr.Seq != uint64(i+1) {
			t.Fatalf("frame %d seq %d, want %d", i, fr.Seq, i+1)
		}
		if fr.Owner != "helper-owner" {
			t.Fatalf("frame %d owner %q, want helper-owner", i, fr.Owner)
		}
	}
	if frames[0].Epoch != 1 || frames[9].Epoch != 2 {
		t.Fatalf("epochs first=%d last=%d, want 1 then 2", frames[0].Epoch, frames[9].Epoch)
	}
}

func TestAxisBCoordMultiShardCrash(t *testing.T) {
	coordDir := t.TempDir()
	sess, err := AcquireCoordinator(coordDir, "owner-a")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer sess.Release()
	writes := map[string][]globalMergeDelta{
		"a": {{Key: "ka", Value: []byte("va")}},
		"b": {{Key: "kb", Value: []byte("vb")}},
	}
	// Crash before root publish: staged only.
	if err := PrepareMultiShard(sess, coordDir, "op-old", writes); err != nil {
		t.Fatalf("prepare op-old: %v", err)
	}
	committed, err := RecoverMultiShard(coordDir)
	if err != nil {
		t.Fatalf("recover staged-only: %v", err)
	}
	if len(committed) != 0 {
		t.Fatalf("staged-only recovered as committed %v, want none (all-old)", committed)
	}
	if _, err := os.Stat(coordStageManifestPath(coordDir, "op-old")); !os.IsNotExist(err) {
		t.Fatalf("staged manifest not reclaimed after staged-only recovery")
	}
	// Crash after root publish: committed, not yet applied.
	if err := PrepareMultiShard(sess, coordDir, "op-new", writes); err != nil {
		t.Fatalf("prepare op-new: %v", err)
	}
	if err := PublishRoot(sess, coordDir, "op-new"); err != nil {
		t.Fatalf("publish op-new: %v", err)
	}
	committed, err = RecoverMultiShard(coordDir)
	if err != nil {
		t.Fatalf("recover committed: %v", err)
	}
	if len(committed) != 1 || committed[0].OpID != "op-new" {
		t.Fatalf("committed %v, want [op-new] (all-new via recovery)", committed)
	}
	if len(committed[0].Writes) != 2 || string(committed[0].Writes["a"][0].Value) != "va" || string(committed[0].Writes["b"][0].Value) != "vb" {
		t.Fatalf("committed payloads %v, want both shards va/vb", committed[0].Writes)
	}
	// Recovery before MarkDone re-returns the op
	// (idempotent roll-forward shape); after MarkDone
	// it is a no-op.
	committed, err = RecoverMultiShard(coordDir)
	if err != nil || len(committed) != 1 {
		t.Fatalf("second recover before MarkDone committed=%v err=%v, want [op-new]", committed, err)
	}
	if err := MarkDone(coordDir, "op-new"); err != nil {
		t.Fatalf("mark done: %v", err)
	}
	committed, err = RecoverMultiShard(coordDir)
	if err != nil || len(committed) != 0 {
		t.Fatalf("recover after MarkDone committed=%v err=%v, want none", committed, err)
	}
	// Corrupt committed staging: loud error, nothing
	// applied or reclaimed by this call.
	if err := PrepareMultiShard(sess, coordDir, "op-bad", writes); err != nil {
		t.Fatalf("prepare op-bad: %v", err)
	}
	if err := PublishRoot(sess, coordDir, "op-bad"); err != nil {
		t.Fatalf("publish op-bad: %v", err)
	}
	shardPath := coordStageShardPath(coordDir, "op-bad", "a")
	data, err := os.ReadFile(shardPath)
	if err != nil {
		t.Fatalf("read staged shard: %v", err)
	}
	// Flip a byte in the payload (not the checksum
	// field itself, so the CRC check must catch it).
	corrupted := append([]byte(nil), data...)
	for i := len(corrupted) - 1; i >= 0; i-- {
		if corrupted[i] == 'v' {
			corrupted[i] = 'X'
			break
		}
	}
	if err := os.WriteFile(shardPath, corrupted, 0o644); err != nil {
		t.Fatalf("corrupt staged shard: %v", err)
	}
	if _, err := RecoverMultiShard(coordDir); !errors.Is(err, ErrCoordCorrupt) {
		t.Fatalf("corrupt committed recover err %v, want ErrCoordCorrupt", err)
	}
	if _, err := os.Stat(coordRootPath(coordDir, "op-bad")); err != nil {
		t.Fatalf("corrupt committed root was reclaimed or lost: %v (recovery must leave it for the operator)", err)
	}
}

func TestAxisBCoordDomainApply(t *testing.T) {
	bin := persistTestBins[0]
	pa, pb, dom, _ := phaseEDomainSetup(t, bin)
	coordDir := t.TempDir()
	sess, err := AcquireCoordinator(coordDir, "owner-a")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer sess.Release()
	vecA, _, err := pa.SnapshotVector()
	if err != nil {
		t.Fatalf("vec a: %v", err)
	}
	vecB, _, err := pb.SnapshotVector()
	if err != nil {
		t.Fatalf("vec b: %v", err)
	}
	writes := map[string][]globalMergeDelta{
		"a": {{Key: "ca", Value: []byte("va")}},
		"b": {{Key: "cb", Value: []byte("vb")}},
	}
	// Nil session and wrong directory refuse loudly
	// and apply nothing (no silent fallback to plain).
	if err := dom.ApplyAtomicCoordinated(nil, coordDir, "op-nil", writes, map[string][]uint64{"a": vecA, "b": vecB}); !errors.Is(err, ErrCoordReleased) {
		t.Fatalf("nil-session coordinated err %v, want ErrCoordReleased", err)
	}
	phaseEMustDBMissing(t, pa, "ca")
	if err := dom.ApplyAtomicCoordinated(sess, t.TempDir(), "op-wrongdir", writes, map[string][]uint64{"a": vecA, "b": vecB}); err == nil || !strings.Contains(err.Error(), "does not match session dir") {
		t.Fatalf("wrong-dir coordinated err %v, want session-dir mismatch", err)
	}
	phaseEMustDBMissing(t, pa, "ca")
	// Clean coordinated apply.
	if err := dom.ApplyAtomicCoordinated(sess, coordDir, "op-1", writes, map[string][]uint64{"a": vecA, "b": vecB}); err != nil {
		t.Fatalf("coordinated apply: %v", err)
	}
	phaseEMustDBGet(t, pa, "ca", "va")
	phaseEMustDBGet(t, pb, "cb", "vb")
	// Coordinated recovery is a no-op after success.
	rolled, err := dom.RecoverCoordinatedDomainOps(coordDir)
	if err != nil || len(rolled) != 0 {
		t.Fatalf("coordinated recover after success rolled=%v err=%v, want none", rolled, err)
	}
	// Stale session after takeover cannot publish:
	// release sess, take over, then the old session's
	// coordinated apply is refused and applies nothing.
	if err := sess.Release(); err != nil {
		t.Fatalf("release sess: %v", err)
	}
	sessB, err := AcquireCoordinator(coordDir, "owner-b")
	if err != nil {
		t.Fatalf("acquire B: %v", err)
	}
	defer sessB.Release()
	vecA2, _, err := pa.SnapshotVector()
	if err != nil {
		t.Fatalf("vec a2: %v", err)
	}
	vecB2, _, err := pb.SnapshotVector()
	if err != nil {
		t.Fatalf("vec b2: %v", err)
	}
	writes2 := map[string][]globalMergeDelta{
		"a": {{Key: "ca2", Value: []byte("va2")}},
		"b": {{Key: "cb2", Value: []byte("vb2")}},
	}
	if err := dom.ApplyAtomicCoordinated(sess, coordDir, "op-stale", writes2, map[string][]uint64{"a": vecA2, "b": vecB2}); !errors.Is(err, ErrCoordStaleEpoch) {
		t.Fatalf("stale coordinated err %v, want ErrCoordStaleEpoch", err)
	}
	phaseEMustDBMissing(t, pa, "ca2")
	phaseEMustDBMissing(t, pb, "cb2")
	// The live session can still apply.
	if err := dom.ApplyAtomicCoordinated(sessB, coordDir, "op-2", writes2, map[string][]uint64{"a": vecA2, "b": vecB2}); err != nil {
		t.Fatalf("live coordinated apply: %v", err)
	}
	phaseEMustDBGet(t, pa, "ca2", "va2")
	phaseEMustDBGet(t, pb, "cb2", "vb2")
}

func TestAxisBCoordDomainCrashRecovery(t *testing.T) {
	bin := persistTestBins[0]
	pa, pb, dom, _ := phaseEDomainSetup(t, bin)
	coordDir := t.TempDir()
	sess, err := AcquireCoordinator(coordDir, "owner-a")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer sess.Release()
	// Crash before root publish (prepare only):
	// coordinated recovery applies nothing (all-old)
	// and reclaims the staged files.
	writesOld := map[string][]globalMergeDelta{
		"a": {{Key: "oa", Value: []byte("voa")}},
		"b": {{Key: "ob", Value: []byte("vob")}},
	}
	if err := PrepareMultiShard(sess, coordDir, "op-crash-old", writesOld); err != nil {
		t.Fatalf("prepare crash-old: %v", err)
	}
	rolled, err := dom.RecoverCoordinatedDomainOps(coordDir)
	if err != nil || len(rolled) != 0 {
		t.Fatalf("crash-old recover rolled=%v err=%v, want none", rolled, err)
	}
	phaseEMustDBMissing(t, pa, "oa")
	phaseEMustDBMissing(t, pb, "ob")
	// Crash after root publish, with a partial apply
	// (only participant a applied — the crash point):
	// coordinated recovery completes every shard
	// (all-new), and a second recovery is a no-op.
	writesNew := map[string][]globalMergeDelta{
		"a": {{Key: "na", Value: []byte("vna")}},
		"b": {{Key: "nb", Value: []byte("vnb")}},
	}
	if err := PrepareMultiShard(sess, coordDir, "op-crash-new", writesNew); err != nil {
		t.Fatalf("prepare crash-new: %v", err)
	}
	if err := PublishRoot(sess, coordDir, "op-crash-new"); err != nil {
		t.Fatalf("publish crash-new: %v", err)
	}
	if err := pa.Put("na", []byte("vna")); err != nil {
		t.Fatalf("partial apply: %v", err)
	}
	phaseEMustDBMissing(t, pb, "nb")
	rolled, err = dom.RecoverCoordinatedDomainOps(coordDir)
	if err != nil {
		t.Fatalf("crash-new recover: %v", err)
	}
	if len(rolled) != 1 || rolled[0] != "op-crash-new" {
		t.Fatalf("crash-new rolled %v, want [op-crash-new]", rolled)
	}
	phaseEMustDBGet(t, pa, "na", "vna")
	phaseEMustDBGet(t, pb, "nb", "vnb")
	rolled, err = dom.RecoverCoordinatedDomainOps(coordDir)
	if err != nil || len(rolled) != 0 {
		t.Fatalf("second crash-new recover rolled=%v err=%v, want none", rolled, err)
	}
}
