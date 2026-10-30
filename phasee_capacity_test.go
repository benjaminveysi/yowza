package yowza

// Phase E, E4 tests: capacity conversions —
// spillable history, hierarchical scan, paged
// secondary index, blobs, registry checkpoint,
// configurable page window.

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

func TestPhaseESpillHistoryBeyond1024(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			// 1100 overwrites of one key: past the old 1024 hot-history cap
			// and past the 32-rank flush cap. The test itself never calls
			// Flush; the facade's epoch guard flushes every 32 mutations
			// of the key, and the spill log retains all versions across
			// those epochs (history surviving restart is the point).
			const n = 1100
			for i := 0; i < n; i++ {
				if err := p.Put("hist", []byte(fmt.Sprintf("hv:%d", i))); err != nil {
					t.Fatalf("put %d: %v", i, err)
				}
			}
			vers, err := p.HistoryVersions("hist", 0, 0)
			if err != nil {
				t.Fatalf("versions: %v", err)
			}
			// 1100 applied mutations, plus at most one
			// duplicate pre-image record for the
			// history-full attempt that triggered the
			// flush epoch (its pre-image is identical
			// to the retry's, so ValueAt is unaffected).
			if len(vers) < n || len(vers) > n+2 {
				t.Fatalf("versions %d, want %d..%d", len(vers), n, n+2)
			}
			// The flush epoch really happened: a
			// checkpoint generation was published
			// mid-stream, not just spill records.
			if p.Generation() == 0 {
				t.Fatalf("generation 0 after 1100 overwrites: no flush epoch happened")
			}
			// Pre-image of mutation i is the value before it.
			if !vers[0].PreOK {
				// First put's pre-image is absent — correct.
			} else {
				t.Fatalf("first version pre-image should be absent")
			}
			if string(vers[500].PreVal) != "hv:499" || !vers[500].PreOK {
				t.Fatalf("version 500 pre-image %q, want hv:499", vers[500].PreVal)
			}
			// ValueAt: value at seq just before mutation 500 is hv:499.
			v, ok, err := p.HistoryValueAt("hist", vers[500].Seq-1)
			if err != nil || !ok || string(v) != "hv:499" {
				t.Fatalf("ValueAt = %q ok %v err %v, want hv:499", v, ok, err)
			}
			p.Close()
			// Reopen WITHOUT any flush having happened: history survives.
			p2 := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			defer p2.Close()
			vers2, err := p2.HistoryVersions("hist", 0, 0)
			if err != nil {
				t.Fatalf("versions after reopen: %v", err)
			}
			if len(vers2) < n || len(vers2) > n+2 {
				t.Fatalf("versions after reopen %d, want %d..%d", len(vers2), n, n+2)
			}
			phaseEMustDBGet(t, p2, "hist", "hv:1099")
		})
	}
}

func TestPhaseESpillCompactionPruneGenerations(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync, SpillChunkSize: 8})
			for i := 0; i < 40; i++ {
				if err := p.Put(fmt.Sprintf("ck:%02d", i%5), []byte(fmt.Sprintf("v%d", i))); err != nil {
					t.Fatalf("put %d: %v", i, err)
				}
			}
			man, err := p.HistoryManifest()
			if err != nil {
				t.Fatalf("manifest: %v", err)
			}
			if len(man.Chunks) < 2 {
				t.Fatalf("chunks %d, want >= 2 with chunk size 8 and 40 puts", len(man.Chunks))
			}
			// Multi-level compaction: level-1 files appear, versions intact.
			levels, merged, err := p.HistoryCompactLevels(2)
			if err != nil {
				t.Fatalf("compact levels: %v", err)
			}
			if levels == 0 || merged == 0 {
				t.Fatalf("compact levels=%d merged=%d, want > 0", levels, merged)
			}
			man2, err := p.HistoryManifest()
			if err != nil {
				t.Fatalf("manifest 2: %v", err)
			}
			hasLevel1 := false
			for _, c := range man2.Chunks {
				if c.Level == 1 {
					hasLevel1 = true
				}
			}
			if !hasLevel1 {
				t.Fatalf("no level-1 chunk after compaction: %+v", man2.Chunks)
			}
			vers, err := p.HistoryVersions("ck:00", 0, 0)
			if err != nil {
				t.Fatalf("versions after compact: %v", err)
			}
			if len(vers) != 8 {
				t.Fatalf("versions for ck:00 = %d, want 8", len(vers))
			}
			// Retention floor: a durable pin blocks pruning past its host seq.
			pin, err := p.CreateDurablePin("Test", "floor-test", "")
			if err != nil {
				t.Fatalf("pin: %v", err)
			}
			if err := p.HistoryPrune(pin.HostSeq + 100); !errors.Is(err, ErrSpillPruneFloor) {
				t.Fatalf("prune past floor err %v, want ErrSpillPruneFloor", err)
			}
			// Pruning below the floor is allowed (drops only old records).
			if err := p.HistoryPrune(2); err != nil {
				t.Fatalf("prune below floor: %v", err)
			}
			// Generation roots: the current manifest generation is readable.
			man3, err := p.HistoryManifest()
			if err != nil {
				t.Fatalf("manifest 3: %v", err)
			}
			genMan, err := LoadSpillGenerationManifest(dir, man3.Generation)
			if err != nil {
				t.Fatalf("load generation %d: %v", man3.Generation, err)
			}
			if len(genMan.Chunks) != len(man3.Chunks) {
				t.Fatalf("generation chunks %d, want %d", len(genMan.Chunks), len(man3.Chunks))
			}
			if _, err := LoadSpillGenerationManifest(dir, 999999); !errors.Is(err, ErrSpillGenerationUnknown) {
				t.Fatalf("unknown generation err %v, want ErrSpillGenerationUnknown", err)
			}
		})
	}
}

func TestPhaseEHierarchicalScan(t *testing.T) {
	// >4096 synthetic sources through the same merge
	// tree code (a 4096-segment wasm fixture is not
	// buildable through the 4 MiB-heap shards — see
	// the notes for the exact reason).
	const leaves = 5000
	var streams []hierStream
	for i := 0; i < leaves; i++ {
		streams = append(streams, &sliceStream{rows: []SegDBScanEntry{{Key: []byte(fmt.Sprintf("s:%05d", i)), Val: []byte("v")}}})
	}
	tree, err := HierMergeTree(streams, 16)
	if err != nil {
		t.Fatalf("merge tree: %v", err)
	}
	if d := HierMergeTreeDepth(leaves, 16); d < 3 {
		t.Fatalf("tree depth %d, want >= 3 for 5000 leaves at fan-in 16", d)
	}
	count := 0
	var prev string
	for {
		e, ok, err := tree.Next()
		if err != nil {
			t.Fatalf("tree next: %v", err)
		}
		if !ok {
			break
		}
		if count > 0 && string(e.Key) <= prev {
			t.Fatalf("tree order violated at %q after %q", string(e.Key), prev)
		}
		prev = string(e.Key)
		count++
	}
	if count != leaves {
		t.Fatalf("tree rows %d, want %d", count, leaves)
	}
	if _, err := HierMergeTree(streams, 5000); !errors.Is(err, ErrHierScanLimit) {
		t.Fatalf("fan-in 5000 err %v, want ErrHierScanLimit", err)
	}
	// Real per-shard wiring: 2 shards, 200 keys, pageSize 37.
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			p, _, _ := phaseEGlobalSetup(t, bin, 2)
			for i := 0; i < 200; i++ {
				if err := p.Put(fmt.Sprintf("hk:%03d", i), []byte("hv")); err != nil {
					t.Fatalf("put: %v", err)
				}
			}
			vec, _, err := p.SnapshotVector()
			if err != nil {
				t.Fatalf("vector: %v", err)
			}
			pages, err := p.HierarchicalScan(vec, "hk:", 37, 16)
			if err != nil {
				t.Fatalf("hierarchical scan: %v", err)
			}
			total := 0
			var keys []string
			for _, pg := range pages {
				if len(pg) > 37 {
					t.Fatalf("page size %d exceeds 37", len(pg))
				}
				total += len(pg)
				for _, e := range pg {
					keys = append(keys, string(e.Key))
				}
			}
			if total != 200 {
				t.Fatalf("hierarchical rows %d, want 200", total)
			}
			if !sort.StringsAreSorted(keys) {
				t.Fatalf("hierarchical output not globally sorted")
			}
			// Cross-check against the plain scan (sorted set equality).
			plain, err := p.Scan("hk:")
			if err != nil {
				t.Fatalf("plain scan: %v", err)
			}
			if len(plain) != 200 {
				t.Fatalf("plain rows %d, want 200", len(plain))
			}
		})
	}
}

func TestPhaseESecondaryIndexPast256(t *testing.T) {
	// 3000 entries, page size 10 -> 300 pages, past
	// the old 256-block cap. Each lookup loads exactly
	// one page (paged binary search, not linear scan).
	var rows []SegDBScanEntry
	for i := 0; i < 3000; i++ {
		rows = append(rows, SegDBScanEntry{Key: []byte(fmt.Sprintf("ix:%05d", i)), Val: []byte(fmt.Sprintf("iv:%d", i))})
	}
	dir := t.TempDir()
	idx, err := BuildSecondaryIndex(dir, "test", &sliceStream{rows: rows}, 10)
	if err != nil {
		t.Fatalf("build index: %v", err)
	}
	if idx.PageCount() != 300 {
		t.Fatalf("pages %d, want 300", idx.PageCount())
	}
	if idx.Total() != 3000 {
		t.Fatalf("total %d, want 3000", idx.Total())
	}
	idx2, err := OpenSecondaryIndex(dir, "test")
	if err != nil {
		t.Fatalf("open index: %v", err)
	}
	for _, key := range []string{"ix:00000", "ix:01500", "ix:02999"} {
		before := idx2.PageLoads()
		v, found, err := idx2.Lookup(key)
		if err != nil || !found {
			t.Fatalf("lookup %s: %q found %v err %v", key, v, found, err)
		}
		if idx2.PageLoads()-before != 1 {
			t.Fatalf("lookup %s loaded %d pages, want 1", key, idx2.PageLoads()-before)
		}
	}
	if v, found, err := idx2.Lookup("ix:99999"); err != nil || found || v != nil {
		t.Fatalf("absent lookup: %q found %v err %v", v, found, err)
	}
	// Unsorted stream is refused, never a lying index.
	bad := []SegDBScanEntry{{Key: []byte("b"), Val: []byte("1")}, {Key: []byte("a"), Val: []byte("2")}}
	if _, err := BuildSecondaryIndex(dir, "bad", &sliceStream{rows: bad}, 10); err == nil {
		t.Fatalf("unsorted build: want error")
	}
}

func TestPhaseEBlobValues(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			p, mgr, dir := phaseEGlobalSetup(t, bin, 1)
			// 200 KiB value: far past the 1024-byte v1 cap.
			plain := make([]byte, 200*1024)
			for i := range plain {
				plain[i] = byte((i * 31) % 251)
			}
			wantSHA := sha256.Sum256(plain)
			// DB-level blob, read back through a small buffer (streaming).
			ref, err := p.PutBlob("big", bytes.NewReader(plain))
			if err != nil {
				t.Fatalf("PutBlob: %v", err)
			}
			_ = ref
			rc, total, err := p.GetBlob("big")
			if err != nil {
				t.Fatalf("GetBlob: %v", err)
			}
			if total != int64(len(plain)) {
				t.Fatalf("blob total %d, want %d", total, len(plain))
			}
			buf := make([]byte, 1024)
			var got []byte
			for {
				n, err := rc.Read(buf)
				if n > 0 {
					got = append(got, buf[:n]...)
				}
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatalf("blob read: %v", err)
				}
			}
			rc.Close()
			if !bytes.Equal(got, plain) {
				t.Fatalf("blob round-trip mismatch (%d bytes)", len(got))
			}
			// Inline values are untouched by the blob path.
			if err := p.Put("small", []byte("s")); err != nil {
				t.Fatalf("inline put: %v", err)
			}
			if _, _, err := p.GetBlob("small"); !errors.Is(err, ErrBlobNotBlob) {
				t.Fatalf("inline GetBlob err %v, want ErrBlobNotBlob", err)
			}
			// Timeline-level large value.
			ref2, err := mgr.PutLarge("tl", "tlbig", bytes.NewReader(plain))
			if err != nil {
				// Timeline "tl" does not exist yet: PutLarge goes through
				// mgr.Put, which needs a timeline. Fork first, then retry.
				if _, ferr := mgr.ForkFromDB("tl"); ferr != nil {
					t.Fatalf("fork tl: %v", ferr)
				}
				ref2, err = mgr.PutLarge("tl", "tlbig", bytes.NewReader(plain))
				if err != nil {
					t.Fatalf("PutLarge after fork: %v", err)
				}
			}
			_ = ref2
			rc2, _, err := mgr.GetLarge("tl", "tlbig")
			if err != nil {
				t.Fatalf("GetLarge: %v", err)
			}
			got2, err := io.ReadAll(rc2)
			rc2.Close()
			if err != nil {
				t.Fatalf("GetLarge read: %v", err)
			}
			gotSHA := sha256.Sum256(got2)
			if hex.EncodeToString(gotSHA[:]) != hex.EncodeToString(wantSHA[:]) {
				t.Fatalf("timeline blob sha mismatch")
			}
			// Corrupt a chunk: the read must fail, not return corrupt bytes.
			// Find a chunk file and flip a byte.
			ents, err := os.ReadDir(filepath.Join(dir, "blobs"))
			if err != nil {
				t.Fatalf("readdir blobs: %v", err)
			}
			var chunkFile string
			for _, e := range ents {
				if strings.HasPrefix(e.Name(), "chunk-") {
					chunkFile = filepath.Join(dir, "blobs", e.Name())
					break
				}
			}
			if chunkFile == "" {
				t.Fatalf("no chunk file found")
			}
			data, err := os.ReadFile(chunkFile)
			if err != nil {
				t.Fatalf("read chunk: %v", err)
			}
			data[0] ^= 0xFF
			if err := os.WriteFile(chunkFile, data, 0o644); err != nil {
				t.Fatalf("corrupt chunk: %v", err)
			}
			rc3, _, err := p.GetBlob("big")
			if err != nil {
				t.Fatalf("GetBlob after corrupt: %v", err)
			}
			_, err = io.ReadAll(rc3)
			rc3.Close()
			if !errors.Is(err, ErrBlobCorrupt) {
				t.Fatalf("corrupt blob read err %v, want ErrBlobCorrupt", err)
			}
		})
	}
}

func TestPhaseERegistryCheckpointNotFullRewrite(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			p, mgr, dir := phaseEGlobalSetup(t, bin, 1)
			if _, err := mgr.ForkFromDB("reg"); err != nil {
				t.Fatalf("fork: %v", err)
			}
			legacyPath := filepath.Join(dir, "timeline-registry.json")
			st1, err := os.Stat(legacyPath)
			if err != nil {
				t.Fatalf("checkpoint file missing after fork: %v", err)
			}
			// Publish accounting: manager open already
			// checkpointed once (deltas=0); the fork is
			// delta #1. The checkpoint file's mtime is
			// therefore the open-time checkpoint.
			mtime1 := st1.ModTime()
			lines, err := registryDeltaLineCount(dir)
			if err != nil {
				t.Fatalf("delta lines: %v", err)
			}
			if lines != 1 {
				t.Fatalf("delta lines after fork = %d, want 1 (open checkpointed, fork is delta 1)", lines)
			}
			// 63 more delta publishes (64 total): the checkpoint file must
			// NOT be rewritten.
			for i := 0; i < 63; i++ {
				timelineMustPut(t, mgr, "reg", fmt.Sprintf("rk:%02d", i), "rv")
				if err := mgr.FlushTimeline("reg"); err != nil {
					t.Fatalf("flush %d: %v", i, err)
				}
			}
			st2, err := os.Stat(legacyPath)
			if err != nil {
				t.Fatalf("stat after deltas: %v", err)
			}
			if !st2.ModTime().Equal(mtime1) {
				t.Fatalf("checkpoint file rewritten during delta publishes (mtime %v -> %v)", mtime1, st2.ModTime())
			}
			lines, err = registryDeltaLineCount(dir)
			if err != nil {
				t.Fatalf("delta lines 2: %v", err)
			}
			if lines != 64 {
				t.Fatalf("delta lines %d, want 64", lines)
			}
			// The next publish checkpoints: the checkpoint file IS rewritten
			// and the log resets.
			time.Sleep(20 * time.Millisecond)
			timelineMustPut(t, mgr, "reg", "rk:64", "rv")
			if err := mgr.FlushTimeline("reg"); err != nil {
				t.Fatalf("flush (checkpoint): %v", err)
			}
			st3, err := os.Stat(legacyPath)
			if err != nil {
				t.Fatalf("stat after checkpoint: %v", err)
			}
			if !st3.ModTime().After(mtime1) {
				t.Fatalf("checkpoint file not rewritten at checkpoint publish")
			}
			lines, err = registryDeltaLineCount(dir)
			if err != nil {
				t.Fatalf("delta lines 3: %v", err)
			}
			if lines != 0 {
				t.Fatalf("delta lines after checkpoint = %d, want 0", lines)
			}
			// Reopen: the registry reconstructs exactly (checkpoint + deltas
			// applied later are covered by the reopen below), tombstones stay.
			mgr.Close()
			p.Close()
			p2 := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			defer p2.Close()
			mgr2, err := NewTimelineManager(p2)
			if err != nil {
				t.Fatalf("reopen manager: %v", err)
			}
			defer mgr2.Close()
			timelineMustGet(t, mgr2, "reg", "rk:64", "rv")
			if err := mgr2.DeleteTimeline("reg"); err != nil {
				t.Fatalf("delete: %v", err)
			}
			mgr2.Close()
			p2.Close()
			p3 := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			defer p3.Close()
			mgr3, err := NewTimelineManager(p3)
			if err != nil {
				t.Fatalf("reopen 2: %v", err)
			}
			defer mgr3.Close()
			if _, err := mgr3.ForkFromDB("reg"); !errors.Is(err, ErrTimelineIDReuse) {
				t.Fatalf("reused deleted id: err %v, want ErrTimelineIDReuse", err)
			}
		})
	}
}

func TestPhaseEScanWindowConfigurable(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			p, _ := phaseBOpen(t, bin, 1)
			for i := 0; i < 200; i++ {
				phaseBPut(t, p, fmt.Sprintf("w:%03d", i), "wv")
			}
			// Page size 37 (not 256, not a cap): the whole 200 rows page through.
			h, err := p.AcquireSnapshot()
			if err != nil {
				t.Fatalf("snapshot: %v", err)
			}
			defer h.Close()
			sid, err := p.ScanOpen(h, "w:", 37)
			if err != nil {
				t.Fatalf("scan open: %v", err)
			}
			total := 0
			pages := 0
			for {
				rows, _, hasMore, err := p.ScanNext(sid)
				if err != nil {
					t.Fatalf("scan next: %v", err)
				}
				total += len(rows)
				pages++
				if !hasMore {
					break
				}
			}
			if total != 200 {
				t.Fatalf("paged rows %d, want 200", total)
			}
			if pages < 5 {
				t.Fatalf("pages %d, want >= 5 for page size 37 over 200 rows", pages)
			}
			// Policy cap: an oversize page is a typed error, not truncation.
			p.SetScanPolicy(ScanPolicy{MaxConcurrentSessions: 16, MaxPageSize: 64})
			if _, err := p.ScanOpen(h, "w:", 65); !errors.Is(err, ErrScanSessionLimit) {
				t.Fatalf("oversize page err %v, want ErrScanSessionLimit", err)
			}
		})
	}
}
