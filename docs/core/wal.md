# Write-Ahead Log (WAL)

The Write-Ahead Log is the backbone of Yowza's durability guarantees, ensuring that no data is lost between serverless invocations or sudden daemon crashes.

## WAL Format
To ensure high-speed parsing across boundaries (C -> Wasm -> Go), the WAL uses a highly compact, zero-padding binary format. Every mutation appended to the log follows this exact structure:

```text
[Opcode(1 byte)] [KeyLen(4 bytes)] [Key(KeyLen bytes)] [ValLen(4 bytes)] [Value(ValLen bytes)]
```

- **Opcode**: Specifies the operation (`0x01` for PUT, `0x03` for DEL).
- All lengths are stored in **Little-Endian** format.
- For `DEL` operations, `ValLen` is `0` and `Value` is omitted.

## Entry Lifecycle
1. **Mutation:** A developer calls `db.Put()`. The Query Compiler generates bytecode and triggers the C VM.
2. **Index Update:** The VM updates the Adaptive Radix Tree (ART) in memory.
3. **WAL Append:** If the update is successful, the VM formats the mutation and passes it to the `StorageDriver` HAL.
4. **Storage:**
   - *Native:* The `StorageDriver` calls `fwrite` to append the binary entry to the `.wal` file on disk.
   - *Wasm:* The `StorageDriver` appends the entry to a pre-allocated segment of the Wasm Linear Memory.
5. **Extraction (Wasm Only):** Post-request, the Go `CloudManager` asynchronously calls `FetchWAL()`. The Go host reads the Wasm memory, parses the binary format into `WalEntry` structs, and immediately resets the C-side WAL offset to zero.
6. **Cloud Push:** The Go host serializes the structs (e.g., via JSON or Protobuf) and pushes them to the remote `StorageBackend` (e.g., Redis Stream, S3 Object).

## Recovery Algorithm & Crash Recovery
When Yowza! boots, it goes through a strict sequence:
1. **Load Snapshot:** If a snapshot exists, it is loaded into the Wasm memory (or Native Arena), instantly rebuilding the ART.
2. **Replay WAL:** The system locates any pending WAL entries (either from a local `.wal` file or fetched remotely from the Cloud Adapter) that were recorded *after* the snapshot was created.
3. **Replay Semantics:** The system executes the WAL entries sequentially against the tree. Because the entries represent exact end-states (e.g., PUT X Y), replaying them is purely idempotent.

## Checksums and Security
In secure mode, WAL entries are not left in plaintext. Before the Go Cloud Manager pushes a batch of `WalEntry` objects to an external `StorageBackend`, the entire batch payload is passed through `ChaCha20-Poly1305` encryption. This produces an authenticated ciphertext that guarantees both **Confidentiality** (Data-at-Rest encryption) and **Integrity** (preventing tampering during transit or storage).

## Sync Guarantees
- **Native Mode:** Provides strict durability guarantees based on the frequency of `fflush()`/`fsync` implementations.
- **Serverless/Wasm Mode:** Provides *Eventual Consistency*. If a cloud worker node crashes unexpectedly *before* the async `CloudManager.SyncWAL()` completes, the WAL entries resident only in the ephemeral Wasm memory will be lost. To mitigate this, High-Availability setups should configure the Cloud Manager to `SyncWAL()` synchronously before returning a `200 OK` to the end client.
