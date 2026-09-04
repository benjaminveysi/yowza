package yowza

import "testing"

// Regression test for the O2/hp_enumerate interaction
// (parallel-timeline patch): a shard that flushes empty
// publishes nothing (empty flush is a no-op), so its heap store
// is genuinely virgin. With the heap store's enumerate hook
// missing, O2's wired segdb_recover could not prove virginity
// and failed reopen with rc=-1, breaking every multi-shard DB
// with a keyless shard (see TestPersistedWALReplayNoFlush).
// This test pins the reopen path: one key lands on one shard
// only, the other shard flushes empty, and the whole DB must
// reopen cleanly with the data intact.
func TestPersistedEmptyShardReopen(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 2, Durability: DurabilitySync})
			if err := p.Put("onlykey", []byte("v")); err != nil {
				t.Fatalf("Put: %v", err)
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("Flush: %v", err)
			}
			if err := p.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			p2 := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 2, Durability: DurabilitySync})
			defer p2.Close()
			persistMustGet(t, p2, "onlykey", "v")
		})
	}
}
