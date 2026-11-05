# The Opcode VM Interpreter

## Why a VM?
To ensure maximum safety and determinism when executing operations—especially those replicated across a cluster or executed in WebAssembly—Yowza! does not process native C function calls for data mutation directly. Instead, all mutations are serialized into byte-sized Opcodes and executed by our internal Virtual Machine (`vm.c`).

## Opcode Set
The VM supports a strict, minimal set of operations:
- `OP_PUT`: Insert or update a key-value pair.
- `OP_GET`: Retrieve a value by key.
- `OP_DEL`: Delete a key-value pair.
- `OP_SCAN`: Iterate over keys with a specific prefix.
- `OP_PUT_REMOTE` / `OP_DEL_REMOTE`: Specialized opcodes utilized by the Cloud Syncer to apply changes received from the cluster without triggering a re-broadcast (preventing infinite loops).

## Execution Pipeline
1. **Request:** A user calls `DB.Put("key", "value")` in Go.
2. **Translation:** The Go SDK translates this into a byte buffer: `[OP_PUT, len("key"), "key", len("value"), "value"]`.
3. **Execution:** The buffer is passed to `vm_execute()`.
4. **Processing:** The VM decodes the payload, performs the actual memory manipulation inside the Arena, and updates the ART index.
5. **Durability:** If the operation is a mutation, the raw byte buffer is immediately appended to the Shadow WAL.

This architecture ensures that the WAL exactly mirrors the execution sequence, guaranteeing deterministic replay during recovery.
