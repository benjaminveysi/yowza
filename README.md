# Yowza!: Fast, Offline-First Data Engine

[![Go 1.25](https://img.shields.io/badge/go-1.25-blue.svg)](https://go.dev/)
[![Version 1.0.1-beta](https://img.shields.io/badge/version-1.0.1--beta-green.svg)]()

A fast, **offline-first data engine** — a persistent store with a WAL, transactions, and Timeline versioning, built on a C core and a Go host, with its own query language (YowzaQL) and an optional Raft cluster.

Designed for edge and serverless, where the database has to survive with no local disk, no always-on server, and no second chance.

## Why Yowza!?

- **Offline-first, crash-safe:** A Shadow WAL and Timeline versioning keep every write durable and replayable — crash mid-publish at any of 7 fault stages, and the store still recovers.
- **A real query engine, not a key-value store:** YowzaQL with a custom Query ISA, an opcode VM, and an Adaptive Radix Tree (ART) index — O(k) prefix lookups, joins, and aggregations.
- **HighCapacity for wasm64:** The 1.0.1-beta profile grows the heap to 64/256 MiB on wasm64 only, so a 100,000-row join that OOMs on wasm32 completes identically to Go.
- **Loud failures, never silent:** Over-budget joins fail with `ErrWasmBudgetExceeded`, unknown heap images are corruption (`ErrHeapImageCorrupt`), oversized ones are refused (`ErrHeapImageTooLarge`). Nothing truncates silently.

## Verified numbers (1.0.1-beta)

**1. HighCapacity vs. wasm32 (100,000-row join, ~23 MiB bump)**
- wasm32: out of memory, **0 rows**.
- wasm64-HighCapacity: **100,000 rows, identical to Go** (join state 32 MiB).
- 80,000-group aggregation: wasm32 LIMIT, 0 rows; wasm64-HighCapacity identical to Go.

**2. Background publish (p99 writer stall)**
- Before: 8 MiB **113 ms**, 64 MiB **171 ms**, 256 MiB **142 ms** — over the 100 ms budget.
- After: **70 / 84 / 82 ms**, deterministic blocked-Put under 5 ms at every size. Median unchanged.

**3. Test suite**
- `make test`: **136 tests, all passed**. Full Go suite re-verified green; `go vet` / `gofmt` clean.

## Quickstart

```bash
git clone https://github.com/benjaminveysi/yowza.git
cd yowza
make test   # C core test suite: 136 tests

# Build the Wasm binaries and the daemon
make        # yowza.wasm, yowza64.wasm, yowzad
```

> **Notes:** `third_party_wazero64` (the vendored wazero runtime) is gitignored and not in this repo. `go.mod` replaces `github.com/tetratelabs/wazero` with `./third_party_wazero64`, so restore that directory before `go build` / `go test`. Raft is proven on single-VM processes only — multi-machine deployment stays experimental. And, honestly: the full root package was not re-run after the five-line heap-loader fix in this beta (see `HIGHCAP_NOTES.md` §8).
