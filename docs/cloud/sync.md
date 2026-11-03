# WAL Syncing and Cloud Snapshots

## The Storage Backend Interface
Yowza!'s `cloud.StorageBackend` is a pluggable interface responsible for distributed durability. We provide built-in backends for Redis, S3, and Local File systems.

## The Shadow WAL
Every mutation in Yowza! is recorded in a Write-Ahead Log. Unlike traditional databases that flush to a local disk, Yowza!'s Shadow WAL is kept in memory (in WebAssembly) or on an ephemeral RAM disk.

The Cloud `Syncer` runs a background goroutine (or runs inline at the end of a serverless invocation) that batches these WAL entries and pushes them to the `StorageBackend`.

## Compaction and Snapshots
Over time, the WAL grows. The `Manager` routinely tracks the size of the WAL. When it exceeds a threshold:
1. A zero-copy `CreateSnapshot()` is triggered, producing a compacted memory dump.
2. The snapshot is uploaded to the cloud backend (e.g., `snapshot_v2.bin`).
3. The backend updates the manifest to point to the new snapshot.
4. The cloud WAL is truncated.

## Cold-Start Bootstrapping
When a new edge node spins up:
1. It queries the `StorageBackend` for the latest manifest.
2. It downloads the base Snapshot (`snapshot_v2.bin`).
3. It directly maps the snapshot into the Wasm memory space (`RestoreSnapshot`).
4. It downloads any WAL operations that occurred *after* the snapshot and replays them via the Opcode VM.
5. The node is now fully synchronized and ready to serve traffic.
