package yowza

// PATCH2 integration-gate tests: the connected paths,
// not helpers alone.
//
//   - Fencing is proven on the REAL facade (Put /
//     Delete / Flush / pin mutations), not only on
//     the seam Track 2 tested. Until this file, a
//     real Put on a registered DB was never shown
//     refused.
//   - Merge blob staging is proven through a real
//     MergeToMain (record → stage → commit → resolve),
//     not only through StageMergeBlobs /
//     ResolveMergeBlobs directly.

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPatch2IntegrationFenceWiredToFacade(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			defer p.Close()
			if err := p.Put("seed", []byte("s")); err != nil {
				t.Fatalf("plain put: %v", err)
			}
			coordDir := t.TempDir()
			sess, err := AcquireCoordinator(coordDir, "fence-owner")
			if err != nil {
				t.Fatalf("acquire coordinator: %v", err)
			}
			defer sess.Release()
			if err := RegisterCoordFence(p, sess); err != nil {
				t.Fatalf("register fence: %v", err)
			}
			defer UnregisterCoordFence(p)
			// Facade data writes: refused outright,
			// even with a LIVE session — a facade
			// Put carries no coordinator token, so
			// it is indistinguishable from a bypass.
			if err := p.Put("x", []byte("y")); !errors.Is(err, ErrCoordFenceBypass) {
				t.Fatalf("fenced Put err %v, want ErrCoordFenceBypass", err)
			}
			if err := p.Delete("seed"); !errors.Is(err, ErrCoordFenceBypass) {
				t.Fatalf("fenced Delete err %v, want ErrCoordFenceBypass", err)
			}
			// Reads are not fenced, and maintenance
			// under a live fence succeeds.
			persistMustGet(t, p, "seed", "s")
			if err := p.Flush(); err != nil {
				t.Fatalf("fenced Flush with live session: %v", err)
			}
			pin, err := p.CreateDurablePin("test", "", "")
			if err != nil {
				t.Fatalf("fenced CreateDurablePin with live session: %v", err)
			}
			if err := p.DeletePin(pin.ID); err != nil {
				t.Fatalf("fenced DeletePin with live session: %v", err)
			}
			// Released session: maintenance refuses
			// with ErrCoordReleased; data writes
			// still refuse with the bypass error.
			if err := sess.Release(); err != nil {
				t.Fatalf("release: %v", err)
			}
			if err := p.Flush(); !errors.Is(err, ErrCoordReleased) {
				t.Fatalf("released Flush err %v, want ErrCoordReleased", err)
			}
			if err := p.Put("x", []byte("y")); !errors.Is(err, ErrCoordFenceBypass) {
				t.Fatalf("released Put err %v, want ErrCoordFenceBypass", err)
			}
			// Unregister restores plain-mode writes.
			UnregisterCoordFence(p)
			if err := p.Put("x", []byte("y")); err != nil {
				t.Fatalf("put after unregister: %v", err)
			}
			persistMustGet(t, p, "x", "y")
		})
	}
}

func TestPatch2IntegrationFenceUnregisteredOnClose(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			coordDir := t.TempDir()
			sess, err := AcquireCoordinator(coordDir, "close-owner")
			if err != nil {
				t.Fatalf("acquire coordinator: %v", err)
			}
			defer sess.Release()
			if err := RegisterCoordFence(p, sess); err != nil {
				t.Fatalf("register fence: %v", err)
			}
			if !CoordFenceRegistered(p) {
				t.Fatalf("fence not registered after RegisterCoordFence")
			}
			if err := p.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}
			if CoordFenceRegistered(p) {
				t.Fatalf("fence still registered after Close")
			}
		})
	}
}

func TestPatch2IntegrationMergeToMainBlobStaging(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p, mgr := timelineOpenForTest(t, bin, dir, 1)
			// Main head holds blob A under a recorded
			// key (typed registry, written at write
			// time — never content-sniffed).
			blobA := bytes.Repeat([]byte("a-blob-content-"), 8192)
			refA, err := p.PutBlobRecorded("bk", bytes.NewReader(blobA))
			if err != nil {
				t.Fatalf("PutBlobRecorded A: %v", err)
			}
			if _, err := mgr.ForkFromDB("src"); err != nil {
				t.Fatalf("fork: %v", err)
			}
			if _, err := mgr.Fork("src", "child"); err != nil {
				t.Fatalf("fork child: %v", err)
			}
			// Branch replaces the blob with blob B
			// (registry upserted to B by the
			// recorded write).
			blobB := bytes.Repeat([]byte("b-blob-content-"), 8192)
			refB, err := mgr.PutLargeRecorded("child", "bk", bytes.NewReader(blobB))
			if err != nil {
				t.Fatalf("PutLargeRecorded B: %v", err)
			}
			if refB.SHA256 == refA.SHA256 {
				t.Fatalf("blobs A and B collide")
			}
			// Main has not moved: the branch change
			// applies (no conflict), so MergeToMain
			// must stage B's registry ref, commit,
			// apply, publish done, and resolve the
			// staging record — all on the real path.
			report, err := mgr.MergeToMain("child", TimelineConflictFail, nil)
			if err != nil {
				t.Fatalf("MergeToMain: %v", err)
			}
			if report.MergeID == "" {
				t.Fatalf("empty MergeID")
			}
			// Head now holds B, byte-identical.
			rc, _, err := p.GetBlob("bk")
			if err != nil {
				t.Fatalf("GetBlob after merge: %v", err)
			}
			got, err := io.ReadAll(rc)
			rc.Close()
			if err != nil || !bytes.Equal(got, blobB) {
				t.Fatalf("GetBlob after merge: len=%d err=%v, want blob B", len(got), err)
			}
			// No leftover staging record for this
			// merge: the resolve ran on the live
			// path, not just in a helper test.
			stagedPath := filepath.Join(dir, lifecycleStagedBlobFile)
			if data, err := os.ReadFile(stagedPath); err == nil {
				var doc lifecycleStagedBlobDoc
				if err := json.Unmarshal(data, &doc); err != nil {
					t.Fatalf("staged blob doc: %v", err)
				}
				for _, e := range doc.Entries {
					if strings.HasPrefix(e.OpID, "merge:") {
						t.Fatalf("leftover merge staging after MergeToMain: %s", e.OpID)
					}
				}
			} else if !os.IsNotExist(err) {
				t.Fatalf("read staged blob doc: %v", err)
			}
			mgr.Close()
			p.Close()
		})
	}
}
