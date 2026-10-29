package yowza

// Patch 2, Track 2 blob-wiring tests.
//
// Contract under test (see blob_wiring_patch2.go): a
// domain-write value is a blob reference ONLY when a
// typed manifest says so. No test below relies on
// parsing a delta value to discover a blob; the one
// negative test proves the opposite direction — a
// value that merely LOOKS like a blob reference, with
// no manifest entry, gets NO copy and NO staging.

import (
	"bytes"
	"errors"
	"io"
	"os"
	"testing"
)

func patch2BlobSetup(t *testing.T, bin string) (pa, pb *PersistedSegDB, dom *TimelineDomain, coord string) {
	t.Helper()
	dirA := t.TempDir()
	dirB := t.TempDir()
	coord = t.TempDir()
	pa = persistOpenForTest(t, bin, dirA, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
	pb = persistOpenForTest(t, bin, dirB, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
	t.Cleanup(func() {
		pa.Close()
		pb.Close()
	})
	var err error
	dom, err = NewTimelineDomain("dom-patch2", coord,
		DomainParticipant{DBID: "a", DB: pa},
		DomainParticipant{DBID: "b", DB: pb})
	if err != nil {
		t.Fatalf("NewTimelineDomain: %v", err)
	}
	return pa, pb, dom, coord
}

func patch2WriteSrcBlob(t *testing.T, dir string, data []byte) BlobRef {
	t.Helper()
	ref, err := WriteBlob(dir, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("WriteBlob: %v", err)
	}
	return ref
}

func patch2BlobExists(dir string, ref BlobRef) bool {
	_, err := os.Stat(blobManifestPath(dir, ref.SHA256))
	return err == nil
}

func patch2ReadBlob(t *testing.T, dir string, ref BlobRef) []byte {
	t.Helper()
	rc, _, err := ReadBlob(dir, ref)
	if err != nil {
		t.Fatalf("ReadBlob: %v", err)
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadBlob read: %v", err)
	}
	return data
}

// patch2GCCollectible reports whether GC (full scan +
// collect) removes ref's manifest from dir, using the
// given open DB for the scan.
func patch2GCCollectible(t *testing.T, db *PersistedSegDB, dir string, ref BlobRef) bool {
	t.Helper()
	live, err := ScanLiveBlobRefs(db, nil)
	if err != nil {
		t.Fatalf("ScanLiveBlobRefs: %v", err)
	}
	if _, err := GCBlobs(dir, live, false); err != nil {
		t.Fatalf("GCBlobs: %v", err)
	}
	return !patch2BlobExists(dir, ref)
}

func TestPatch2BlobWiringCommit(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			pa, pb, dom, _ := patch2BlobSetup(t, bin)
			data := bytes.Repeat([]byte("q"), 9000)
			ref := patch2WriteSrcBlob(t, pa.Dir(), data)
			man := &BlobOpManifest{Entries: []BlobManifestEntry{
				{DBID: "b", Key: "bk", Ref: ref, SourceDir: pa.Dir()},
			}}
			writes := map[string][]globalMergeDelta{
				"b": {{Key: "bk", Value: []byte(ref.String())}},
			}
			if err := dom.ApplyAtomicWithBlobs("op-blob-commit", writes, nil, man); err != nil {
				t.Fatalf("ApplyAtomicWithBlobs commit: %v", err)
			}
			// Landed in the head, reachable, staged resolved.
			if v, err := pb.Get("bk"); err != nil || string(v) != ref.String() {
				t.Fatalf("pb bk = %q err %v, want ref %s", v, err, ref.String())
			}
			if !patch2BlobExists(pb.Dir(), ref) {
				t.Fatalf("committed blob copy missing on target")
			}
			if got := patch2ReadBlob(t, pb.Dir(), ref); !bytes.Equal(got, data) {
				t.Fatalf("committed blob bytes mismatch")
			}
			if ops := StagedBlobOps(pb.Dir()); len(ops) != 0 {
				t.Fatalf("staged blob ops after commit %v, want none", ops)
			}
			// GC keeps a committed, head-referenced blob.
			if patch2GCCollectible(t, pb, pb.Dir(), ref) {
				t.Fatalf("GC collected a committed head-referenced blob")
			}
		})
	}
}

func TestPatch2BlobWiringNoSniffing(t *testing.T) {
	// A delta value that LOOKS like a blob reference,
	// with NO manifest entry, gets no copy and no
	// staging: the value string is stored, but the
	// wiring does not discover blobs from content.
	bin := persistTestBins[0]
	pa, pb, dom, _ := patch2BlobSetup(t, bin)
	data := bytes.Repeat([]byte("s"), 9000)
	ref := patch2WriteSrcBlob(t, pa.Dir(), data)
	writes := map[string][]globalMergeDelta{
		"b": {{Key: "bk", Value: []byte(ref.String())}},
	}
	if err := dom.ApplyAtomic("op-no-manifest", writes, nil); err != nil {
		t.Fatalf("plain ApplyAtomic: %v", err)
	}
	if v, err := pb.Get("bk"); err != nil || string(v) != ref.String() {
		t.Fatalf("pb bk = %q err %v, want ref string stored", v, err)
	}
	if patch2BlobExists(pb.Dir(), ref) {
		t.Fatalf("blob bytes copied to target without a manifest: content-sniffing")
	}
	if ops := StagedBlobOps(pb.Dir()); len(ops) != 0 {
		t.Fatalf("staged blob ops without a manifest %v, want none", ops)
	}
	// A manifest inconsistent with the write set is
	// refused before anything is staged or applied.
	bad := &BlobOpManifest{Entries: []BlobManifestEntry{
		{DBID: "b", Key: "bk", Ref: BlobRef{SHA256: ref.SHA256, TotalLen: ref.TotalLen + 1, ChunkCount: ref.ChunkCount}, SourceDir: pa.Dir()},
	}}
	writes2 := map[string][]globalMergeDelta{
		"b": {{Key: "bk2", Value: []byte(ref.String())}},
	}
	if err := dom.ApplyAtomicWithBlobs("op-bad-manifest", writes2, nil, bad); err == nil {
		t.Fatalf("inconsistent manifest accepted, want refusal")
	}
	if _, err := pb.Get("bk2"); err == nil {
		t.Fatalf("bk2 applied despite manifest refusal")
	}
	if ops := StagedBlobOps(pb.Dir()); len(ops) != 0 {
		t.Fatalf("staged blob ops after manifest refusal %v, want none", ops)
	}
}

func TestPatch2BlobWiringAbort(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			pa, pb, dom, _ := patch2BlobSetup(t, bin)
			data := bytes.Repeat([]byte("a"), 9000)
			ref := patch2WriteSrcBlob(t, pa.Dir(), data)
			man := &BlobOpManifest{Entries: []BlobManifestEntry{
				{DBID: "b", Key: "bk", Ref: ref, SourceDir: pa.Dir()},
			}}
			writes := map[string][]globalMergeDelta{
				"b": {{Key: "bk", Value: []byte(ref.String())}},
			}
			// Stale expected vector: the apply must
			// refuse, apply nothing, and abort-resolve
			// the blob staging (copies remain on disk
			// but unstaged, hence collectible).
			expected := map[string][]uint64{"b": {999999}}
			err := dom.ApplyAtomicWithBlobs("op-blob-abort", writes, expected, man)
			if !errors.Is(err, ErrDomainStaleVector) {
				t.Fatalf("stale apply err %v, want ErrDomainStaleVector", err)
			}
			if _, err := pb.Get("bk"); err == nil {
				t.Fatalf("bk applied despite stale-vector abort")
			}
			if ops := StagedBlobOps(pb.Dir()); len(ops) != 0 {
				t.Fatalf("staged blob ops after abort %v, want none", ops)
			}
			if !patch2GCCollectible(t, pb, pb.Dir(), ref) {
				t.Fatalf("GC kept an aborted staged blob")
			}
		})
	}
}

func TestPatch2BlobWiringCrashRecovery(t *testing.T) {
	bin := persistTestBins[0]
	pa, pb, dom, _ := patch2BlobSetup(t, bin)
	data := bytes.Repeat([]byte("c"), 9000)
	ref := patch2WriteSrcBlob(t, pa.Dir(), data)
	writes := map[string][]globalMergeDelta{
		"b": {{Key: "bk", Value: []byte(ref.String())}},
	}

	// (a) Crash after blob staging + domain stage +
	// commit log, before apply/done: recovery rolls
	// forward, lands the blob in the head, and
	// commit-resolves the staging.
	if err := CopyBlob(pa.Dir(), pb.Dir(), ref); err != nil {
		t.Fatalf("crash copy: %v", err)
	}
	if err := BeginStaging(pb.Dir(), "op-crash-commit", []BlobRef{ref}); err != nil {
		t.Fatalf("crash BeginStaging: %v", err)
	}
	if err := dom.stageOp("op-crash-commit", writes); err != nil {
		t.Fatalf("crash stageOp: %v", err)
	}
	if err := dom.appendLog("commit", "op-crash-commit", map[string]string{"op": "op-crash-commit"}); err != nil {
		t.Fatalf("crash commit log: %v", err)
	}
	rolled, err := dom.RecoverDomainOps()
	if err != nil {
		t.Fatalf("recovery (a): %v", err)
	}
	found := false
	for _, id := range rolled {
		if id == "op-crash-commit" {
			found = true
		}
	}
	if !found {
		t.Fatalf("recovery (a) rolled %v, want op-crash-commit", rolled)
	}
	if v, err := pb.Get("bk"); err != nil || string(v) != ref.String() {
		t.Fatalf("recovery (a) pb bk = %q err %v, want ref", v, err)
	}
	if ops := StagedBlobOps(pb.Dir()); len(ops) != 0 {
		t.Fatalf("recovery (a) staged ops %v, want none", ops)
	}
	if patch2GCCollectible(t, pb, pb.Dir(), ref) {
		t.Fatalf("recovery (a): GC collected a rolled-forward committed blob")
	}

	// (b) Crash between blob staging and domain
	// staging (no stage file, no commit): one recovery
	// run abort-resolves the staging; GC collects.
	ref2 := patch2WriteSrcBlob(t, pa.Dir(), bytes.Repeat([]byte("d"), 9000))
	if err := CopyBlob(pa.Dir(), pb.Dir(), ref2); err != nil {
		t.Fatalf("crash-b copy: %v", err)
	}
	if err := BeginStaging(pb.Dir(), "op-crash-stageonly", []BlobRef{ref2}); err != nil {
		t.Fatalf("crash-b BeginStaging: %v", err)
	}
	if _, err := dom.RecoverDomainOps(); err != nil {
		t.Fatalf("recovery (b): %v", err)
	}
	if ops := StagedBlobOps(pb.Dir()); len(ops) != 0 {
		t.Fatalf("recovery (b) staged ops %v, want none", ops)
	}
	if !patch2GCCollectible(t, pb, pb.Dir(), ref2) {
		t.Fatalf("recovery (b): GC kept an uncommitted staged blob")
	}

	// (c) Crash after domain stage but before commit:
	// the first recovery run reclaims the stage but
	// conservatively SKIPS the blob staging (see
	// RecoverDomainOps: a stage-having op might be a
	// root-published coordinated op); the second,
	// idempotent run resolves it, and GC collects.
	ref3 := patch2WriteSrcBlob(t, pa.Dir(), bytes.Repeat([]byte("e"), 9000))
	writes3 := map[string][]globalMergeDelta{
		"b": {{Key: "bk3", Value: []byte(ref3.String())}},
	}
	if err := CopyBlob(pa.Dir(), pb.Dir(), ref3); err != nil {
		t.Fatalf("crash-c copy: %v", err)
	}
	if err := BeginStaging(pb.Dir(), "op-crash-stage", []BlobRef{ref3}); err != nil {
		t.Fatalf("crash-c BeginStaging: %v", err)
	}
	if err := dom.stageOp("op-crash-stage", writes3); err != nil {
		t.Fatalf("crash-c stageOp: %v", err)
	}
	if _, err := dom.RecoverDomainOps(); err != nil {
		t.Fatalf("recovery (c) run 1: %v", err)
	}
	if _, err := os.Stat(dom.stagePath("op-crash-stage")); err == nil {
		t.Fatalf("recovery (c) run 1 did not reclaim the prepare-only stage")
	}
	if ops := StagedBlobOps(pb.Dir()); len(ops) != 1 || ops[0] != "op-crash-stage" {
		t.Fatalf("recovery (c) run 1 staged ops %v, want [op-crash-stage] (conservative skip)", ops)
	}
	if _, err := dom.RecoverDomainOps(); err != nil {
		t.Fatalf("recovery (c) run 2: %v", err)
	}
	if ops := StagedBlobOps(pb.Dir()); len(ops) != 0 {
		t.Fatalf("recovery (c) run 2 staged ops %v, want none", ops)
	}
	if !patch2GCCollectible(t, pb, pb.Dir(), ref3) {
		t.Fatalf("recovery (c): GC kept an uncommitted staged blob after run 2")
	}
}

func TestPatch2BlobWiringCoordinated(t *testing.T) {
	bin := persistTestBins[0]
	pa, pb, dom, coord := patch2BlobSetup(t, bin)
	// Normal setup: the domain directory IS the
	// coordinator directory, so one recovery run
	// resolves everything (see the header rule).
	sess, err := AcquireCoordinator(coord, "patch2-owner")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer sess.Release()
	data := bytes.Repeat([]byte("k"), 9000)
	ref := patch2WriteSrcBlob(t, pa.Dir(), data)
	man := &BlobOpManifest{Entries: []BlobManifestEntry{
		{DBID: "b", Key: "bk", Ref: ref, SourceDir: pa.Dir()},
	}}
	writes := map[string][]globalMergeDelta{
		"b": {{Key: "bk", Value: []byte(ref.String())}},
	}
	if err := dom.ApplyAtomicCoordinatedWithBlobs(sess, coord, "op-coord-blob", writes, nil, man); err != nil {
		t.Fatalf("coordinated WithBlobs: %v", err)
	}
	if v, err := pb.Get("bk"); err != nil || string(v) != ref.String() {
		t.Fatalf("coordinated pb bk = %q err %v, want ref", v, err)
	}
	if ops := StagedBlobOps(pb.Dir()); len(ops) != 0 {
		t.Fatalf("coordinated staged ops %v, want none", ops)
	}
	if ops, err := RecoverMultiShard(coord); err != nil || len(ops) != 0 {
		t.Fatalf("coordinated root leftover ops %v err %v, want none", ops, err)
	}
	// Stale session: nothing applied, blob staging
	// abort-resolved (no-decision error path).
	if err := sess.Release(); err != nil {
		t.Fatalf("release: %v", err)
	}
	next, err := AcquireCoordinator(coord, "patch2-next")
	if err != nil {
		t.Fatalf("takeover acquire: %v", err)
	}
	defer next.Release()
	ref2 := patch2WriteSrcBlob(t, pa.Dir(), bytes.Repeat([]byte("m"), 9000))
	man2 := &BlobOpManifest{Entries: []BlobManifestEntry{
		{DBID: "b", Key: "bk2", Ref: ref2, SourceDir: pa.Dir()},
	}}
	writes2 := map[string][]globalMergeDelta{
		"b": {{Key: "bk2", Value: []byte(ref2.String())}},
	}
	err = dom.ApplyAtomicCoordinatedWithBlobs(sess, coord, "op-coord-stale", writes2, nil, man2)
	if !errors.Is(err, ErrCoordStaleEpoch) {
		t.Fatalf("stale coordinated err %v, want ErrCoordStaleEpoch", err)
	}
	if _, err := pb.Get("bk2"); err == nil {
		t.Fatalf("bk2 applied despite stale session")
	}
	if ops := StagedBlobOps(pb.Dir()); len(ops) != 0 {
		t.Fatalf("stale coordinated staged ops %v, want none", ops)
	}
}

func TestPatch2BlobWiringMergeHelper(t *testing.T) {
	// No DB needed: the merge helper works off the
	// typed registry + staging file in one directory.
	dir := t.TempDir()
	data := bytes.Repeat([]byte("z"), 9000)
	ref, err := WriteBlob(dir, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("WriteBlob: %v", err)
	}
	if err := RecordBlobKey(dir, "mk", ref); err != nil {
		t.Fatalf("RecordBlobKey: %v", err)
	}
	deltas := []globalMergeDelta{{Key: "mk", Value: []byte(ref.String())}}
	if err := StageMergeBlobs(dir, "m1", deltas); err != nil {
		t.Fatalf("StageMergeBlobs: %v", err)
	}
	if ops := StagedBlobOps(dir); len(ops) != 1 || ops[0] != "merge:m1" {
		t.Fatalf("merge staged ops %v, want [merge:m1]", ops)
	}
	if err := ResolveMergeBlobs(dir, "m1", false); err != nil {
		t.Fatalf("ResolveMergeBlobs abort: %v", err)
	}
	if ops := StagedBlobOps(dir); len(ops) != 0 {
		t.Fatalf("merge staged ops after abort %v, want none", ops)
	}
	// A key absent from the registry stages nothing
	// (never inferred from the delta value).
	unrecorded := []globalMergeDelta{{Key: "ghost", Value: []byte(ref.String())}}
	if err := StageMergeBlobs(dir, "m2", unrecorded); err != nil {
		t.Fatalf("StageMergeBlobs unrecorded: %v", err)
	}
	if ops := StagedBlobOps(dir); len(ops) != 0 {
		t.Fatalf("merge staged ops for unrecorded key %v, want none", ops)
	}
	// Commit resolve is idempotent for unknown ops.
	if err := ResolveMergeBlobs(dir, "m2", true); err != nil {
		t.Fatalf("ResolveMergeBlobs unknown: %v", err)
	}
	// Recorded write path: PutBlobRecorded records the
	// key, so a later merge stages it (uses a real DB
	// only for the Put step).
	bin := persistTestBins[0]
	pdir := t.TempDir()
	p := persistOpenForTest(t, bin, pdir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
	defer p.Close()
	refR, err := p.PutBlobRecorded("rk", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("PutBlobRecorded: %v", err)
	}
	rdeltas := []globalMergeDelta{{Key: "rk", Value: []byte(refR.String())}}
	if err := StageMergeBlobs(pdir, "m3", rdeltas); err != nil {
		t.Fatalf("StageMergeBlobs recorded: %v", err)
	}
	if ops := StagedBlobOps(pdir); len(ops) != 1 || ops[0] != "merge:m3" {
		t.Fatalf("recorded merge staged ops %v, want [merge:m3]", ops)
	}
}
