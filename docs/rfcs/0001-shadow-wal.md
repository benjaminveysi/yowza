# RFC 0001: Shadow WAL Architecture

## Motivation
Traditional databases use an `fsync`-heavy Write-Ahead Log to guarantee durability. In serverless environments (AWS Lambda), writing to local disk (`/tmp`) is lost when the container is destroyed. We need a way to persist state to the cloud without blocking the main execution thread.

## Proposal: The Shadow WAL
1. **In-Memory Buffer:** The database engine will maintain an append-only memory buffer (the "Shadow WAL").
2. **Opcode Serialization:** Every mutation will be serialized into this buffer immediately.
3. **Asynchronous Syncing:** A decoupled Go process (the Cloud Syncer) will read the Shadow WAL at regular intervals (or end of execution) and push it to a durable backend (Redis/S3).
4. **Idempotent Replay:** On cold start, a node downloads the WAL segments it missed and replays them through the VM interpreter.

## Drawbacks
- Potential data loss if the node crashes *before* the Cloud Syncer pushes the buffer.
- Mitigated by triggering a sync immediately upon the completion of a serverless request lifecycle.
