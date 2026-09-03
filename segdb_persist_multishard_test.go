package yowza

import (
	"errors"
	"fmt"
	"testing"
)

// Item 04, persisted layer: streaming multi-shard scan at a
// snapshot vector, shard distribution, tombstone-wins across
// shards, snapshot-vector mismatch refusal, and closed-DB
// behaviour. Query-level tests live in query/multishard_test.go.
func TestPersistedStreamingScan(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 4, Durability: DurabilityNoSync})
			defer p.Close()
			const n = 200
			want := map[string]string{}
			seenShard := map[int]bool{}
			for i := 0; i < n; i++ {
				k := fmt.Sprintf("user:%04d", i)
				v := fmt.Sprintf("v-%d", i)
				if err := p.Put(k, []byte(v)); err != nil {
					t.Fatalf("Put %s: %v", k, err)
				}
				want[k] = v
				seenShard[p.ShardIndex(k)] = true
			}
			if len(seenShard) != 4 {
				t.Fatalf("keys spread over %d shards, want 4 (shard routing broken or fixture degenerate)", len(seenShard))
			}
			// ShardIndex must be deterministic.
			for k := range want {
				if p.ShardIndex(k) != p.ShardIndex(k) {
					t.Fatalf("ShardIndex non-deterministic for %q", k)
				}
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("Flush: %v", err)
			}
			vec, _, err := p.SnapshotVector()
			if err != nil {
				t.Fatalf("SnapshotVector: %v", err)
			}
			if len(vec) != 4 {
				t.Fatalf("vector len = %d, want 4", len(vec))
			}
			it, err := p.ScanStreaming("user:", vec)
			if err != nil {
				t.Fatalf("ScanStreaming: %v", err)
			}
			// Streaming proof (white-box, same package): the
			// first small batch must NOT have advanced past
			// shard 0. A materializing scan would have read
			// all shards before returning anything.
			first, err := it.NextBatch(10)
			if err != nil {
				t.Fatalf("NextBatch(10): %v", err)
			}
			if len(first) != 10 {
				t.Fatalf("first batch = %d rows, want 10", len(first))
			}
			if it.shard != 0 {
				t.Fatalf("after first batch iterator is on shard %d, want 0 (scan materialized past shard 0)", it.shard)
			}
			got := map[string]string{}
			for _, e := range first {
				got[string(e.Key)] = string(e.Val)
			}
			for {
				page, err := it.NextBatch(64)
				if err != nil {
					t.Fatalf("NextBatch: %v", err)
				}
				if len(page) == 0 {
					break
				}
				for _, e := range page {
					k := string(e.Key)
					if _, dup := got[k]; dup {
						t.Fatalf("duplicate key %q across shards", k)
					}
					got[k] = string(e.Val)
				}
			}
			if len(got) != n {
				t.Fatalf("streamed %d rows, want %d", len(got), n)
			}
			for k, v := range want {
				if got[k] != v {
					t.Fatalf("key %q = %q, want %q", k, got[k], v)
				}
			}
			if err := it.Close(); err != nil {
				t.Fatalf("Close iterator: %v", err)
			}
			// Tombstone-wins across shards: delete 5 keys
			// spread over shards, flush, rescan at a fresh
			// vector — deleted keys must not resurface.
			var deleted []string
			i := 0
			for k := range want {
				if i%40 == 0 {
					deleted = append(deleted, k)
				}
				i++
			}
			for _, k := range deleted {
				if err := p.Delete(k); err != nil {
					t.Fatalf("Delete %s: %v", k, err)
				}
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("Flush after delete: %v", err)
			}
			vec2, _, err := p.SnapshotVector()
			if err != nil {
				t.Fatalf("SnapshotVector 2: %v", err)
			}
			it2, err := p.ScanStreaming("user:", vec2)
			if err != nil {
				t.Fatalf("ScanStreaming 2: %v", err)
			}
			got2 := map[string]bool{}
			for {
				page, err := it2.NextBatch(64)
				if err != nil {
					t.Fatalf("NextBatch 2: %v", err)
				}
				if len(page) == 0 {
					break
				}
				for _, e := range page {
					got2[string(e.Key)] = true
				}
			}
			for _, k := range deleted {
				if got2[k] {
					t.Fatalf("deleted key %q resurfaced in multi-shard scan", k)
				}
			}
			if len(got2) != n-len(deleted) {
				t.Fatalf("after delete streamed %d rows, want %d", len(got2), n-len(deleted))
			}
			// Old vector still sees the pre-delete state:
			// snapshot semantics, not live state.
			itOld, err := p.ScanStreaming("user:", vec)
			if err != nil {
				t.Fatalf("ScanStreaming old vector: %v", err)
			}
			oldCount := 0
			for {
				page, err := itOld.NextBatch(64)
				if err != nil {
					t.Fatalf("NextBatch old: %v", err)
				}
				if len(page) == 0 {
					break
				}
				oldCount += len(page)
			}
			if oldCount != n {
				t.Fatalf("old vector streamed %d rows, want %d (pre-delete state)", oldCount, n)
			}
		})
	}
}

func TestPersistedStreamingSnapshotErrors(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 4, Durability: DurabilityNoSync})
			if err := p.Put("user:1", []byte("a")); err != nil {
				t.Fatalf("Put: %v", err)
			}
			// Wrong-length vector is refused, not silently
			// read as a subset.
			if _, err := p.ScanStreaming("user:", []uint64{1, 2}); !errors.Is(err, ErrPersistedSnapshotMismatch) {
				t.Fatalf("short vector: err %v, want ErrPersistedSnapshotMismatch", err)
			}
			if _, err := p.ScanStreaming("user:", nil); !errors.Is(err, ErrPersistedSnapshotMismatch) {
				t.Fatalf("nil vector: err %v, want ErrPersistedSnapshotMismatch", err)
			}
			// Mid-query Close surfaces as a checkable error
			// on the next page, not a silent truncation.
			vec, _, err := p.SnapshotVector()
			if err != nil {
				t.Fatalf("SnapshotVector: %v", err)
			}
			it, err := p.ScanStreaming("user:", vec)
			if err != nil {
				t.Fatalf("ScanStreaming: %v", err)
			}
			if err := p.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			if _, err := it.NextBatch(10); !errors.Is(err, ErrPersistedClosed) {
				t.Fatalf("page after Close: err %v, want ErrPersistedClosed", err)
			}
			if _, _, err := p.SnapshotVector(); !errors.Is(err, ErrPersistedClosed) {
				t.Fatalf("SnapshotVector after Close: err %v, want ErrPersistedClosed", err)
			}
		})
	}
}

// Mid-query writes are invisible at a captured vector, in
// every shard: the vector is taken, then new keys are put and
// an old key is deleted, and a scan at the old vector still
// returns exactly the pre-write state.
func TestPersistedStreamingMidQueryWriteInvisible(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 4, Durability: DurabilityNoSync})
			defer p.Close()
			for i := 0; i < 40; i++ {
				if err := p.Put(fmt.Sprintf("user:%04d", i), []byte("old")); err != nil {
					t.Fatalf("Put: %v", err)
				}
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("Flush: %v", err)
			}
			vec, _, err := p.SnapshotVector()
			if err != nil {
				t.Fatalf("SnapshotVector: %v", err)
			}
			it, err := p.ScanStreaming("user:", vec)
			if err != nil {
				t.Fatalf("ScanStreaming: %v", err)
			}
			// Pull one batch, then write mid-iteration.
			first, err := it.NextBatch(10)
			if err != nil || len(first) != 10 {
				t.Fatalf("first batch = %d, err %v", len(first), err)
			}
			for i := 100; i < 110; i++ {
				if err := p.Put(fmt.Sprintf("user:%04d", i), []byte("new")); err != nil {
					t.Fatalf("mid-query Put: %v", err)
				}
			}
			if err := p.Delete("user:0000"); err != nil {
				t.Fatalf("mid-query Delete: %v", err)
			}
			got := map[string]bool{}
			for _, e := range first {
				got[string(e.Key)] = true
			}
			for {
				page, err := it.NextBatch(64)
				if err != nil {
					t.Fatalf("NextBatch: %v", err)
				}
				if len(page) == 0 {
					break
				}
				for _, e := range page {
					got[string(e.Key)] = true
				}
			}
			if len(got) != 40 {
				t.Fatalf("mid-query scan returned %d rows, want 40 (pre-write state)", len(got))
			}
			for i := 100; i < 110; i++ {
				if got[fmt.Sprintf("user:%04d", i)] {
					t.Fatalf("mid-query write user:%04d leaked into old-vector scan", i)
				}
			}
			if !got["user:0000"] {
				t.Fatalf("mid-query delete hid user:0000 from old-vector scan (should stay visible)")
			}
		})
	}
}
