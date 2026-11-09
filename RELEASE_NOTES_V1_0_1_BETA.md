# Yowza64 — Version 1.0.1-beta

Delta over 1.0.0-beta: the Wasm64 High-Capacity Profile. The beta tag
keeps the same boundary as 1.0.0-beta: Raft is proven on single-VM
processes only (no real-network proof, no compaction/membership yet),
so multi-machine Raft deployment stays experimental.

## What is new

- Named, opt-in profiles: Standard (exactly the old behavior, still
  the default) and HighCapacity (wasm64 only, refused loudly on
  wasm32, never a silent downgrade).
- Host-owned dynamic Grow64 for QBATCH operator state, with a
  fail-before-consume discipline in the C operators (capacity is
  checked before appending, so a state-full retry cannot double-add).
  Query budget is host-aware: min(configured, host-RAM fraction,
  cgroup limit). Over-budget failure is loud (`ErrWasmBudgetExceeded`
  or `QISA_ERR_OOM`), never a silent truncate, never a host OOM.
- Runtime row/group caps: QBATCH_MAX_ROWS 262144 → 1048576 and
  QBATCH_MAX_GROUPS 65536 → 262144 under HighCapacity, passed as
  operator-init parameters (Standard behavior at the old caps is
  bit-identical, except the deliberate change below).
- Configurable heap size at DB creation: 8 MiB (Standard), 64 MiB,
  256 MiB (HighCapacity, wasm64 only). Loader rules: smaller known
  sizes extend (SHA verified first), larger sizes are refused loudly
  (`ErrHeapImageTooLarge`), unknown sizes are corruption
  (`ErrHeapImageCorrupt`, added in this release after the full suite
  caught the generalized extend rule accepting a 12345-byte image).
  Empty 256 MiB heap files occupy 65,536 bytes of blocks (sparse).
- Module memory Max for wasm64: 16 GiB VA (MAP_NORESERVE).
- Background publish: p99 writer stall was over the 100 ms budget at
  all heap sizes before (8 MiB 113 ms, 64 MiB 171 ms, 256 MiB 142 ms,
  max up to 22 s); after background publish, p99 is 70/84/82 ms and
  deterministic blocked-Put is sub-5 ms at every size. Median
  (~31–34 ms) is unchanged: the fix moved lock placement, not work.

## Proven (see HIGHCAP_NOTES.md §§6–8 for the numbers)

- 100,000-row join (~23 MiB bump): wasm32 out of memory, 0 rows;
  wasm64-HighCapacity 100,000 rows identical to Go (join state grown
  to 32 MiB). 80,000-group aggregation: wasm32 LIMIT, 0 rows;
  wasm64-HighCapacity identical to Go.
- Over-budget join: loud `ErrWasmBudgetExceeded`, 0 rows, executor
  memory within budget, no double-add on retry.
- Crash between publishes, crash mid-publish at all 7 fault stages,
  and torn-heap fallback: pass at 64/256 MiB.
- `make test`: 136 Running, All Tests Passed. Full Go suite: one
  failure found and fixed (see below); targeted re-verification
  green (1362.826 s); `go vet` / `gofmt` clean.

## Two honest corrections (not buried)

- Generation count is NOT reduced by a bigger heap. For a fixed
  completed workload, 64 MiB and 256 MiB both complete at exactly
  183 generations: generations are driven by the 64 KiB hot WAL and
  epoch caps, not heap size. What the bigger heap measurably does
  is prevent bricks: the same workload BRICKED at 8 MiB in round 3
  (bump 99.6% full, flush and reactive compact both rc=-1) and
  completes at 64/256 MiB. Claimed effect: bricks 1 → 0 plus
  headroom, not fewer generations.
- Standard's failure mode at the exact exhaustion boundary changed
  deliberately: a clean preflight OOM instead of a partial append.
  That IS the fail-before-consume fix; Standard may OOM one batch
  earlier than the old code would have failed anyway, while
  HighCapacity absorbs it via growth.
- The full-suite failure and fix: the suite caught
  `TestHeapImageExtensionSizes` failing because the new loader
  accepted any smaller size. Fixed with the allowlist + 
  `ErrHeapImageCorrupt` above; the full root package was not
  re-run after that five-line fix, and that is stated in
  HIGHCAP_NOTES.md §8 rather than implied.
