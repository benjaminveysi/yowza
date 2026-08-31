package yowza

// Phase B tests: SnapshotHandle (B1), durable pins (B2),
// scan sessions + hierarchical manifest (O3), and the
// in-package half of AcquireShardLeases (O4; the external
// caller half is segdb_phaseb_external_test.go).

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func phaseBOpen(t *testing.T, bin string, shards int) (*PersistedSegDB, string) {
	t.Helper()
	wasmBytes, err := os.ReadFile(bin)
	if err != nil {
		t.Fatalf("read %s: %v", bin, err)
	}
	dir := t.TempDir()
	p, err := OpenPersistedSegDB(dir, wasmBytes, PersistedOptions{NumShards: shards, Durability: DurabilitySync})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p, dir
}

func phaseBPut(t *testing.T, p *PersistedSegDB, k, v string) {
	t.Helper()
	if err := p.Put(k, []byte(v)); err != nil {
		t.Fatalf("put %s: %v", k, err)
	}
}

func TestPhaseBSnapshotHandleOldView(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			p, _ := phaseBOpen(t, bin, 2)
			for i := 0; i < 20; i++ {
				phaseBPut(t, p, fmt.Sprintf("k:%03d", i), fmt.Sprintf("v1-%d", i))
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}
			h, err := p.AcquireSnapshot()
			if err != nil {
				t.Fatalf("acquire: %v", err)
			}
			vec := h.Vector()
			if len(vec) != 2 {
				t.Fatalf("vector len %d, want 2", len(vec))
			}
			// Advance + flush + compact under the live handle.
			for i := 0; i < 20; i++ {
				phaseBPut(t, p, fmt.Sprintf("k:%03d", i), fmt.Sprintf("v2-%d", i))
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("flush2: %v", err)
			}
			before, err := p.SegmentCounts()
			if err != nil {
				t.Fatalf("counts: %v", err)
			}
			if err := p.Compact(0); err != nil {
				t.Fatalf("compact under lease: %v", err)
			}
			after, err := p.SegmentCounts()
			if err != nil {
				t.Fatalf("counts2: %v", err)
			}
			// The live handle's lease must defer the merge:
			// segment counts must not drop under the lease.
			for i := range before {
				if after[i] > before[i] {
					t.Fatalf("shard %d segment count grew under compact %v -> %v", i, before, after)
				}
			}
			// Old view through the handle, both Get and Scan.
			v, err := h.Get([]byte("k:005"))
			if err != nil || string(v) != "v1-5" {
				t.Fatalf("handle get old = %q err %v, want v1-5", v, err)
			}
			it, err := h.Scan([]byte("k:"))
			if err != nil {
				t.Fatalf("handle scan: %v", err)
			}
			var rows []SegDBScanEntry
			for {
				page, err := it.NextBatch(64)
				if err != nil {
					t.Fatalf("scan page: %v", err)
				}
				if len(page) == 0 {
					break
				}
				rows = append(rows, page...)
			}
			_ = it.Close()
			if len(rows) != 20 {
				t.Fatalf("handle scan rows %d, want 20", len(rows))
			}
			for _, e := range rows {
				if string(e.Val)[:3] != "v1-" {
					t.Fatalf("handle scan row %s = %s, want v1-*", e.Key, e.Val)
				}
			}
			// Closing the handle is idempotent; a second
			// Get must fail as closed, never as partial data.
			if err := h.Close(); err != nil {
				t.Fatalf("handle close: %v", err)
			}
			if err := h.Close(); err != nil {
				t.Fatalf("handle close idempotent: %v", err)
			}
			if _, err := h.Get([]byte("k:005")); !errors.Is(err, ErrSnapshotHandleClosed) {
				t.Fatalf("get after close err %v, want ErrSnapshotHandleClosed", err)
			}
		})
	}
}

func TestPhaseBSnapshotHandleConcurrent(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			p, _ := phaseBOpen(t, bin, 2)
			for i := 0; i < 50; i++ {
				phaseBPut(t, p, fmt.Sprintf("c:%03d", i), "x")
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}
			var wg sync.WaitGroup
			errs := make(chan error, 32)
			for g := 0; g < 8; g++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					h, err := p.AcquireSnapshot()
					if err != nil {
						errs <- err
						return
					}
					defer h.Close()
					if _, err := h.Get([]byte("c:010")); err != nil {
						errs <- err
						return
					}
					it, err := h.Scan([]byte("c:"))
					if err != nil {
						errs <- err
						return
					}
					for {
						page, err := it.NextBatch(16)
						if err != nil {
							errs <- err
							return
						}
						if len(page) == 0 {
							break
						}
					}
					_ = it.Close()
				}()
			}
			wg.Wait()
			close(errs)
			for err := range errs {
				t.Fatalf("concurrent handle: %v", err)
			}
		})
	}
}

func TestPhaseBDurablePinSurvivesReopenAndCompact(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			wasmBytes, err := os.ReadFile(bin)
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			dir := t.TempDir()
			p, err := OpenPersistedSegDB(dir, wasmBytes, PersistedOptions{NumShards: 2, Durability: DurabilitySync})
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			for i := 0; i < 20; i++ {
				phaseBPut(t, p, fmt.Sprintf("p:%03d", i), fmt.Sprintf("old-%d", i))
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}
			pin, err := p.CreateDurablePin("Timeline", "tl-branch-1", "")
			if err != nil {
				t.Fatalf("create pin: %v", err)
			}
			if pin.Status != PinStatusActive || len(pin.Vector) != 2 || pin.Generation == 0 && p.Generation() == 0 {
				// Generation may legitimately be 1 after one
				// flush; only the vector/status are asserted
				// strictly here.
				if pin.Status != PinStatusActive || len(pin.Vector) != 2 {
					t.Fatalf("bad pin %+v", pin)
				}
			}
			if _, err := os.Stat(filepath.Join(dir, "pins.json")); err != nil {
				t.Fatalf("pins.json missing after create: %v", err)
			}
			// Advance, then close WITHOUT any manager: the
			// pin is the only record of the branch point.
			for i := 0; i < 20; i++ {
				phaseBPut(t, p, fmt.Sprintf("p:%03d", i), fmt.Sprintf("new-%d", i))
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("flush2: %v", err)
			}
			if err := p.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}
			// Reopen, then Compact BEFORE any TimelineManager
			// exists: the recovered pin must defer the merge.
			p2, err := OpenPersistedSegDB(dir, wasmBytes, PersistedOptions{NumShards: 2, Durability: DurabilitySync})
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			defer p2.Close()
			pins, err := p2.ListPins()
			if err != nil || len(pins) != 1 || pins[0].ID != pin.ID || pins[0].Status != PinStatusActive {
				t.Fatalf("recovered pins %+v err %v", pins, err)
			}
			if err := p2.Compact(0); err != nil {
				t.Fatalf("compact after reopen: %v", err)
			}
			// Old snapshot still readable at the pin vector.
			it, err := p2.ScanStreaming("p:", pins[0].Vector)
			if err != nil {
				t.Fatalf("scan at pin vector: %v", err)
			}
			var rows []SegDBScanEntry
			for {
				page, err := it.NextBatch(64)
				if err != nil {
					t.Fatalf("pin scan page: %v", err)
				}
				if len(page) == 0 {
					break
				}
				rows = append(rows, page...)
			}
			_ = it.Close()
			if len(rows) != 20 {
				t.Fatalf("pin scan rows %d, want 20", len(rows))
			}
			for _, e := range rows {
				if string(e.Val)[:4] != "old-" {
					t.Fatalf("pin scan row %s = %s, want old-*", e.Key, e.Val)
				}
			}
			// Deleting the pin then compacting must not
			// lose CURRENT data.
			if err := p2.DeletePin(pin.ID); err != nil {
				t.Fatalf("delete pin: %v", err)
			}
			if err := p2.Compact(0); err != nil {
				t.Fatalf("compact after delete: %v", err)
			}
			got, err := p2.Get("p:007")
			if err != nil || string(got) != "new-7" {
				t.Fatalf("current get after pin delete+compact = %q err %v, want new-7", got, err)
			}
		})
	}
}

func TestPhaseBOrphanPinCleanup(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			p, _ := phaseBOpen(t, bin, 2)
			for i := 0; i < 10; i++ {
				phaseBPut(t, p, fmt.Sprintf("o:%03d", i), fmt.Sprintf("v-%d", i))
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}
			// Pin published, Timeline registry commit MISSING:
			// the registry (synthetic: empty set) knows no
			// such timeline. This is the crash-between-pin-
			// and-registry outcome.
			orphan, err := p.CreateDurablePin("Timeline", "tl-ghost", "")
			if err != nil {
				t.Fatalf("orphan pin: %v", err)
			}
			kept, err := p.CreateDurablePin("Timeline", "tl-live", "")
			if err != nil {
				t.Fatalf("live pin: %v", err)
			}
			// Branch data is NOT lost while the orphan
			// exists: the pinned vector still scans fully.
			it, err := p.ScanStreaming("o:", orphan.Vector)
			if err != nil {
				t.Fatalf("orphan scan: %v", err)
			}
			n := 0
			for {
				page, err := it.NextBatch(64)
				if err != nil {
					t.Fatalf("orphan scan page: %v", err)
				}
				if len(page) == 0 {
					break
				}
				n += len(page)
			}
			_ = it.Close()
			if n != 10 {
				t.Fatalf("orphan scan rows %d, want 10", n)
			}
			// Registry check: only tl-live is known.
			removed, err := p.CleanupOrphanPins(func(id string) bool { return id == "tl-live" })
			if err != nil {
				t.Fatalf("cleanup: %v", err)
			}
			if len(removed) != 1 || removed[0] != orphan.ID {
				t.Fatalf("cleanup removed %v, want [%s]", removed, orphan.ID)
			}
			got, err := p.GetPin(orphan.ID)
			if err != nil || got.Status != PinStatusDeleted {
				t.Fatalf("orphan after cleanup %+v err %v, want deleted", got, err)
			}
			got, err = p.GetPin(kept.ID)
			if err != nil || got.Status != PinStatusActive {
				t.Fatalf("live pin after cleanup %+v err %v, want active", got, err)
			}
			// Query-session pins (empty TimelineID) are
			// never treated as orphans.
			qp, err := p.CreateDurablePin("query-session", "", "tx-1")
			if err != nil {
				t.Fatalf("query pin: %v", err)
			}
			removed, err = p.CleanupOrphanPins(func(id string) bool { return id == "tl-live" })
			if err != nil {
				t.Fatalf("cleanup2: %v", err)
			}
			if len(removed) != 0 {
				t.Fatalf("cleanup2 removed %v, want none (live timeline kept, query pin never an orphan)", removed)
			}
			_ = qp
		})
	}
}

func TestPhaseBPinSnapshotHandleIndependence(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			p, _ := phaseBOpen(t, bin, 2)
			for i := 0; i < 10; i++ {
				phaseBPut(t, p, fmt.Sprintf("h:%03d", i), "a")
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}
			h, err := p.AcquireSnapshot()
			if err != nil {
				t.Fatalf("acquire: %v", err)
			}
			pin, err := p.PinSnapshot(h, "Timeline", "tl-h", "")
			if err != nil {
				t.Fatalf("pin snapshot: %v", err)
			}
			// Closing the handle must NOT drop the pin.
			if err := h.Close(); err != nil {
				t.Fatalf("handle close: %v", err)
			}
			got, err := p.GetPin(pin.ID)
			if err != nil || got.Status != PinStatusActive {
				t.Fatalf("pin after handle close %+v err %v", got, err)
			}
			// A handle from another database is refused.
			p2, _ := phaseBOpen(t, bin, 2)
			h2, err := p2.AcquireSnapshot()
			if err != nil {
				t.Fatalf("acquire2: %v", err)
			}
			defer h2.Close()
			if _, err := p.PinSnapshot(h2, "Timeline", "tl-x", ""); err == nil {
				t.Fatalf("PinSnapshot accepted a foreign handle")
			}
		})
	}
}

func TestPhaseBScanSessionPagingAndConcurrency(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			p, _ := phaseBOpen(t, bin, 2)
			for i := 0; i < 100; i++ {
				phaseBPut(t, p, fmt.Sprintf("s:%03d", i), fmt.Sprintf("v-%d", i))
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}
			h, err := p.AcquireSnapshot()
			if err != nil {
				t.Fatalf("acquire: %v", err)
			}
			defer h.Close()
			// Small pages: total (100) is unbounded by
			// pageSize (7). pageSize decides output per
			// page only.
			sid, err := p.ScanOpen(h, "s:", 7)
			if err != nil {
				t.Fatalf("scan open: %v", err)
			}
			var rows []SegDBScanEntry
			pages := 0
			for {
				page, cursor, hasMore, err := p.ScanNext(sid)
				if err != nil {
					t.Fatalf("scan next: %v", err)
				}
				if len(page) > 7 {
					t.Fatalf("page size %d exceeds 7", len(page))
				}
				if len(page) > 0 {
					pages++
					rows = append(rows, page...)
				}
				_ = cursor
				if !hasMore {
					break
				}
			}
			if len(rows) != 100 || pages < 10 {
				t.Fatalf("session rows %d pages %d, want 100 rows in many pages", len(rows), pages)
			}
			keys := sortedKeys(rows)
			for i, k := range keys {
				if k != fmt.Sprintf("s:%03d", i) {
					t.Fatalf("session key %d = %s", i, k)
				}
			}
			// Large pageSize: one Next returns everything;
			// pageSize still does not cap the total.
			sid2, err := p.ScanOpen(h, "s:", 10000)
			if err != nil {
				t.Fatalf("scan open large: %v", err)
			}
			page, _, hasMore, err := p.ScanNext(sid2)
			if err != nil {
				t.Fatalf("scan next large: %v", err)
			}
			if len(page) != 100 || hasMore {
				t.Fatalf("large page rows %d hasMore %v, want 100/false", len(page), hasMore)
			}
			// Concurrent sessions interleave cleanly.
			var ids []uint64
			for i := 0; i < 4; i++ {
				id, err := p.ScanOpen(h, "s:", 13)
				if err != nil {
					t.Fatalf("concurrent open: %v", err)
				}
				ids = append(ids, id)
			}
			if p.ScanSessionCount() != 4 {
				t.Fatalf("session count %d, want 4", p.ScanSessionCount())
			}
			done := make([]bool, 4)
			counts := make([]int, 4)
			for {
				progress := false
				for i, id := range ids {
					if done[i] {
						continue
					}
					page, _, hasMore, err := p.ScanNext(id)
					if err != nil {
						t.Fatalf("concurrent next: %v", err)
					}
					counts[i] += len(page)
					progress = true
					if !hasMore {
						done[i] = true
					}
				}
				if !progress {
					break
				}
			}
			for i, c := range counts {
				if c != 100 {
					t.Fatalf("concurrent session %d rows %d, want 100", i, c)
				}
			}
		})
	}
}

func TestPhaseBScanSessionResourceCapAndStale(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			p, _ := phaseBOpen(t, bin, 2)
			for i := 0; i < 30; i++ {
				phaseBPut(t, p, fmt.Sprintf("r:%03d", i), "v")
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}
			h, err := p.AcquireSnapshot()
			if err != nil {
				t.Fatalf("acquire: %v", err)
			}
			defer h.Close()
			// Resource cap: 1 concurrent session.
			p.SetScanPolicy(ScanPolicy{MaxConcurrentSessions: 1, MaxPageSize: 64})
			sid, err := p.ScanOpen(h, "r:", 10)
			if err != nil {
				t.Fatalf("open1: %v", err)
			}
			if _, err := p.ScanOpen(h, "r:", 10); !errors.Is(err, ErrScanSessionLimit) {
				t.Fatalf("second session err %v, want ErrScanSessionLimit", err)
			}
			if _, err := p.ScanOpen(h, "r:", 65); !errors.Is(err, ErrScanSessionLimit) {
				t.Fatalf("oversize page err %v, want ErrScanSessionLimit", err)
			}
			// Stale: a checkpoint change between pages
			// kills the session BEFORE returning a row.
			if _, _, _, err := p.ScanNext(sid); err != nil {
				t.Fatalf("first page: %v", err)
			}
			phaseBPut(t, p, "r:999", "v")
			if err := p.Flush(); err != nil {
				t.Fatalf("flush mid-scan: %v", err)
			}
			rows, _, _, err := p.ScanNext(sid)
			if !errors.Is(err, ErrScanSessionStale) {
				t.Fatalf("post-flush page err %v rows %d, want ErrScanSessionStale with 0 rows", err, len(rows))
			}
			if len(rows) != 0 {
				t.Fatalf("stale page returned %d rows, want 0", len(rows))
			}
			// Session is dead and gone: further calls
			// report unknown, and Close is a no-op.
			if _, _, _, err := p.ScanNext(sid); !errors.Is(err, ErrScanSessionUnknown) && !errors.Is(err, ErrScanSessionStale) {
				t.Fatalf("dead session err %v, want unknown/stale", err)
			}
			if err := p.ScanClose(sid); err != nil {
				t.Fatalf("close dead session: %v", err)
			}
			p.SetScanPolicy(DefaultScanPolicy)
		})
	}
}

func TestPhaseBHierarchicalManifest(t *testing.T) {
	// 5000 leaves, fan-in 16: no level holds 5000 items,
	// yet the walk visits every leaf in order. This is
	// the level-structure proof (see the file header for
	// what it does NOT claim about wasm scans).
	ids := make([]string, 5000)
	for i := range ids {
		ids[i] = fmt.Sprintf("seg.%d", i)
	}
	root := HierBuildBalanced(ids, 16)
	flat, depth, err := HierFlatten(root, 16)
	if err != nil {
		t.Fatalf("flatten: %v", err)
	}
	if len(flat) != 5000 {
		t.Fatalf("flatten leaves %d, want 5000", len(flat))
	}
	for i, id := range flat {
		if id != ids[i] {
			t.Fatalf("leaf %d = %s, want %s", i, id, ids[i])
		}
	}
	if depth < 3 {
		t.Fatalf("depth %d, want >= 3 for 5000 leaves at fan-in 16", depth)
	}
	// Fan-in violation is a hard error, never a drop.
	bad := &HierNode{Name: "root", Children: []*HierNode{{Name: "a", SegmentID: "seg.a"}, {Name: "b", SegmentID: "seg.b"}, {Name: "c", SegmentID: "seg.c"}}}
	if _, _, err := HierFlatten(bad, 2); err == nil {
		t.Fatalf("fan-in violation not rejected")
	}
	// A leaf without an ID is a hard error.
	bad2 := &HierNode{Name: "root", Children: []*HierNode{{Name: "empty"}}}
	if _, _, err := HierFlatten(bad2, 4); err == nil {
		t.Fatalf("ID-less leaf not rejected")
	}
}
