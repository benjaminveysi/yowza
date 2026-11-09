# Yowza64 — Version 1.0.0-beta

First versioned release. Named 1.0.0-beta (not 1.0.0 final) for one honest
reason, stated below: the distributed layer's real-network proof is still
pending. Everything else in the claimed scope is built and green.

## What is in this release

- Versioned storage engine: generational heaps, Segment v1 (`YWSEG1`),
  Manifest v2 (`YWMAN2`), multi-level manifest (`YWMNDR`), segmented WAL,
  checkpoint.json as the single commit point, directory exclusive lock.
- Parallel Timeline: branches, three-way merge (direct-parent/sibling),
  merge DAG with generational WALs and delta segments, `ErrAmbiguousMergeBase`
  for 5+ candidates, permanent ID tombstones.
- Transactions: `SegDBTransaction` with private write set, MVCC validation,
  CRC-32 commit journal, recovery-before-read via the lifecycle gate,
  poisoned-WAL handling (`IndeterminateWALError`, `ErrPersistedPoisoned`).
- Coordinated protocol: token + opID transaction commits with the commit
  decision durable in the coordinator log before any apply, idempotent retry,
  fence validated at commit time under the same hold; coordinated MergeToMain
  preserving the full patch3 fixed-head discipline; tokenless writes on fenced
  DBs still refused with `ErrCoordFenceBypass`; recovery limited to
  log-durable decisions.
- Query: YowzaQL + Typed IR + Plan Verifier, Query ISA v1/v2, QBATCH
  vectorized execution in C/WASM (filter with three-valued NULL logic,
  project, hash-join, stable sort, GROUP BY with COUNT/SUM/AVG/MIN/MAX).
  LIMIT is deliberately applied Go-side, after global semantics.
- Storage scale-up on C/WASM: hierarchical fan-in (4096 becomes a per-level
  cap), proven at 5002 segments / 4990 rows in one call on both binaries;
  full 900,000-row scans on both binaries.
- Durability hardening: pins.json with SHA-256 checksum + backup descriptor,
  reported fallback on corrupt primary, loud failure if both are corrupt,
  backward-compatible migration of checksum-less files.
- Raft (stdlib-only): election, minority-partition non-commit, survival of
  committed entries across leader change, stale-leader fencing, a real
  `raftcoord` process binary with peer RPC, HTTP API and persisted state.

## Verification (assistant-reported; user verification pending)

- `make test`: exit 0, 136 Running, All Tests Passed (fresh post-restart run).
- Full `go test ./... -timeout 60m`: EXIT=0, every package ok
  (root 1987.447 s, query 289.511 s).
- `-race` on the coordinated + pins sets: exit 0, 0 DATA RACE (370.4 s).
- `go vet` clean; `make clean && make all` zero warnings.
- `yowza.wasm` 255,149 B, `yowza64.wasm` 286,544 B; root vs `build/`
  hashes identical inside the archive.

## Why beta, not final

The beta tag marks exactly one boundary: Raft is proven on separate
processes on a single VM only — not on a real multi-machine network — and
Raft log compaction and membership changes are not built yet. Until those
land with real-network proof, multi-machine Raft deployment is experimental.
Single-node and locally-coordinated use is the proven scope of this release.

## Known limits (all loud, none silent)

- Track A logical spill does not bound total wasm memory like Go's disk
  spill: input over the host state buffer fails with `QISA_ERR_OOM`, never
  a silent truncate.
- Track C ceilings: 8192 sources / 2048 outputs.
- Merge views are still materialized (~3.5 KiB/key).
- Coordinated MergeToMain and coordinated transactions are the protocol
  shipped here; cross-DB atomic transactions (2PC) do not exist as an API
  and are not claimed.
- Indeterminate registry publish = full mutation freeze; poison is
  per-instance, not persisted; pins.json corruption of both copies fails
  open loudly (gate closed, no silent loss).
