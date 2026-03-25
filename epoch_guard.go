package yowza

// Phase E, E4 — FLUSH-EPOCH GUARD (the design that
// replaces the 1024-history / 32-rank caps as
// failure modes).
//
// Two C bounds interact to brick a database if
// reached in one flush epoch:
//   (a) SEGDB_HIST_MAX = 1024 hot history entries
//       per shard per epoch — a further overwrite
//       fails with rc=-3 (history-full);
//   (b) SEGDB_HIST_SEG_MAX = 32 version ranks per
//       flush — a flush with any key holding more
//       than 32 superseded versions in the epoch
//       fails with -1 (rank overflow).
// Once (a) is reached with (b) also exceeded (the
// single-key case: 1024 overwrites of one key),
// neither Put nor Flush can proceed: the epoch is
// stuck, and no C operation recovers it (compact
// merges segments, not hot history). The guard below
// makes that state UNREACHABLE for facade traffic
// by flushing proactively, before either bound can
// be approached:
//
//   - Per key per shard, at most epochMaxPerKey
//     mutations per epoch (32): a 33rd mutation
//     first flushes the epoch (ranks per key stay
//     <= 31, under the 32 cap).
//   - Per shard, at most epochMaxHistEst estimated
//     history entries per epoch (900, under 1024
//     with margin for non-facade C captures, e.g.
//     direct VM tx history hooks).
//   - Per shard, at most epochMaxDistinct distinct
//     hot terminals per epoch (3800, under the
//     4096 SEGDB_FLUSH_MAX_ENTRIES cap).
//
// The estimates are CONSERVATIVE upper bounds: a
// mutation is counted as superseding a hot terminal
// whenever its key was already mutated in the
// epoch, even when the previous mutation was
// flushed by an auto-checkpoint in between (the
// count then overestimates by one). Overestimation
// flushes slightly early; it can never flush late.
// WAL-replayed tail mutations are counted at open
// (see the replay loop), so the guard is exact
// across restarts too.
//
// What the guard does NOT fix (honest limit): a
// WAL tail written BEFORE this guard existed (or by
// a non-facade writer) that already exceeds 32
// versions of one key in one epoch cannot be
// flushed by the C engine — the next guarded write
// for that key attempts the flush and fails loudly
// with the C rank error (no silent loss, no silent
// drop of history). The remaining C work for that
// case is a multi-group history flush (history
// ranks split across group manifests, each within
// the 128-segment group cap); it is designed-but-
// remaining, not claimed here.

import (
	"fmt"
)

// epochMaxPerKey caps mutations of one key per
// shard per epoch (ranks stay <= 31 < 32).
const epochMaxPerKey = 32

// epochMaxHistEst caps estimated history entries
// per shard per epoch (900 < 1024).
const epochMaxHistEst = 900

// epochMaxDistinct caps distinct hot terminals per
// shard per epoch (3800 < 4096).
const epochMaxDistinct = 3800

// epochInitLocked allocates the epoch counters.
// Caller holds p.mu (or is single-threaded at open).
func (p *PersistedSegDB) epochInitLocked() {
	if p.epochCounts == nil {
		p.epochCounts = make([]map[string]int, len(p.shards))
		p.epochHistEst = make([]int, len(p.shards))
		for i := range p.epochCounts {
			p.epochCounts[i] = map[string]int{}
		}
	}
}

// epochResetLocked clears all epoch counters (a
// flush epoch just ended). Caller holds p.mu.
func (p *PersistedSegDB) epochResetLocked() {
	p.epochInitLocked()
	for i := range p.epochCounts {
		p.epochCounts[i] = map[string]int{}
		p.epochHistEst[i] = 0
	}
}

// epochNoteLocked records one successful mutation
// in the current epoch (facade write or WAL replay).
// Caller holds p.mu (or is single-threaded at open).
func (p *PersistedSegDB) epochNoteLocked(shard int, key string) {
	p.epochInitLocked()
	if shard < 0 || shard >= len(p.epochCounts) {
		return
	}
	before := p.epochCounts[shard][key]
	p.epochCounts[shard][key] = before + 1
	if before >= 1 {
		// This mutation superseded a hot terminal
		// (conservative: see the file header).
		p.epochHistEst[shard]++
	}
}

// epochGuardLocked flushes the epoch when the next
// mutation of key would breach a bound. Caller
// holds p.mu. A flush failure is returned (the
// mutation is not attempted); a successful flush
// resets the counters, so the mutation then starts
// a fresh epoch.
func (p *PersistedSegDB) epochGuardLocked(key string) error {
	p.epochInitLocked()
	si := p.ShardIndex(key)
	counts := p.epochCounts[si]
	trip := counts[key] >= epochMaxPerKey ||
		p.epochHistEst[si] >= epochMaxHistEst ||
		len(counts) >= epochMaxDistinct
	if !trip {
		return nil
	}
	if err := p.flushLocked(); err != nil {
		return fmt.Errorf("yowza persist: epoch-guard flush failed: %w", err)
	}
	// flushLocked resets the counters (see
	// segdb_persist_lifecycle.go); the mutation
	// that follows starts a fresh epoch.
	return nil
}
