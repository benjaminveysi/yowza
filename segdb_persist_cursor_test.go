package yowza

import (
	"fmt"
	"testing"
)

// Integration wiring (closing contracts, Gate 1): PersistedScanIter
// must page through SegDB.ScanCursor (not skip paging), and the
// shard leases it registers at creation must defer compaction
// while the iterator is alive.
func TestPersistedScanIterCursor(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 2, Durability: DurabilityNoSync})
			defer p.Close()
			want := map[string]string{}
			for i := 0; i < 150; i++ {
				k := fmt.Sprintf("user:%04d", i)
				v := fmt.Sprintf("v-%d", i)
				if err := p.Put(k, []byte(v)); err != nil {
					t.Fatalf("Put %s: %v", k, err)
				}
				want[k] = v
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("Flush: %v", err)
			}
			vec, _, err := p.SnapshotVector()
			if err != nil {
				t.Fatalf("SnapshotVector: %v", err)
			}
			// Reset module scan counters, then read only
			// through the persisted iterator: cursor visits
			// must be > 0 and skip-path page visits must be 0.
			if err := p.shards[0].sdb.ResetScanStats(); err != nil {
				t.Fatalf("ResetScanStats: %v", err)
			}
			it, err := p.ScanStreaming("user:", vec)
			if err != nil {
				t.Fatalf("ScanStreaming: %v", err)
			}
			got := map[string]string{}
			for {
				page, err := it.NextBatch(64)
				if err != nil {
					t.Fatalf("NextBatch: %v", err)
				}
				if len(page) == 0 {
					break
				}
				for _, e := range page {
					got[string(e.Key)] = string(e.Val)
				}
			}
			if err := it.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			if len(got) != len(want) {
				t.Fatalf("rows = %d, want %d", len(got), len(want))
			}
			for k, v := range want {
				if got[k] != v {
					t.Fatalf("key %s = %q, want %q", k, got[k], v)
				}
			}
			st, err := p.shards[0].sdb.ScanStats()
			if err != nil {
				t.Fatalf("ScanStats: %v", err)
			}
			if st.CursorVisits == 0 {
				t.Fatalf("CursorVisits = 0: persisted iterator did not use the cursor path")
			}
			if st.PageVisits != 0 {
				t.Fatalf("PageVisits = %d, want 0: persisted iterator fell back to skip paging", st.PageVisits)
			}
		})
	}
}

func TestPersistedScanIterLeaseDefersCompact(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilityNoSync})
			defer p.Close()
			for b := 0; b < 2; b++ {
				for i := 0; i < 100; i++ {
					k := fmt.Sprintf("k%04d", b*100+i)
					if err := p.Put(k, []byte("v")); err != nil {
						t.Fatalf("Put %s: %v", k, err)
					}
				}
				if err := p.Flush(); err != nil {
					t.Fatalf("Flush batch %d: %v", b, err)
				}
			}
			sh := p.shards[0].sdb
			n, err := sh.SegmentCount()
			if err != nil || n != 2 {
				t.Fatalf("SegmentCount = %d, err %v, want 2 before lease test", n, err)
			}
			vec, _, err := p.SnapshotVector()
			if err != nil {
				t.Fatalf("SnapshotVector: %v", err)
			}
			it, err := p.ScanStreaming("", vec)
			if err != nil {
				t.Fatalf("ScanStreaming: %v", err)
			}
			// Advance the sequence past the lease, then try
			// to compact: the lease must defer the merge.
			for i := 0; i < 10; i++ {
				if err := p.Put(fmt.Sprintf("new%02d", i), []byte("n")); err != nil {
					t.Fatalf("Put new: %v", err)
				}
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("Flush new: %v", err)
			}
			if n, err := sh.SegmentCount(); err != nil || n != 3 {
				t.Fatalf("SegmentCount after new flush = %d, err %v, want 3", n, err)
			}
			if err := sh.Compact(0); err != nil {
				t.Fatalf("Compact under lease: %v", err)
			}
			if n, err := sh.SegmentCount(); err != nil || n != 3 {
				t.Fatalf("SegmentCount after deferred compact = %d, err %v, want 3 (lease must defer)", n, err)
			}
			// The iterator must still see only the old
			// snapshot, not the new keys.
			seen := map[string]bool{}
			for {
				page, err := it.NextBatch(64)
				if err != nil {
					t.Fatalf("NextBatch: %v", err)
				}
				if len(page) == 0 {
					break
				}
				for _, e := range page {
					seen[string(e.Key)] = true
				}
			}
			if len(seen) != 200 {
				t.Fatalf("leased iterator rows = %d, want 200 old keys", len(seen))
			}
			if seen["new00"] {
				t.Fatalf("leased iterator saw post-snapshot key new00")
			}
			if err := it.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			// After release, compaction may merge.
			if err := sh.Compact(0); err != nil {
				t.Fatalf("Compact after release: %v", err)
			}
			if n, err := sh.SegmentCount(); err != nil || n >= 3 {
				t.Fatalf("SegmentCount after released compact = %d, err %v, want < 3", n, err)
			}
		})
	}
}
