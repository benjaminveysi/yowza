# Architecture

Yowza! is not just a database; it is a multi-layered ecosystem designed to bridge the gap between high-level application logic (Go) and low-level, high-performance data storage (C/Wasm).

## High-Level Topology

```text
      Applications
           ↓
       Yowza API
           ↓
     Database Core
           ↓
    WAL + ART + VM
           ↓
    Storage Drivers
           ↓
     POSIX / Memory
           ↓
     Cluster + Sync
           ↓
     Cloud Adapters
```

## Layer Breakdown

### 1. The Application & API Layer (Go / C Native)
The entry point. Developers interact with Yowza! using either the high-level Go SDK or the Embedded C API. This layer provides simple CRUD and Scanning interfaces. When using Go, this layer acts as a "Query Compiler," translating `db.Put("key", "val")` into a compact byte array of Opcodes.

### 2. The Database Core (C / Wasm)
This is the heart of the system.
- **The VM (Opcode Interpreter):** A fast `switch-case` loop that executes the compiled bytecodes directly against the memory index without string parsing overhead.
- **The Index (True ART):** An Adaptive Radix Tree (utilizing Node4, Node16, Node48, Node256). It provides $O(k)$ lookup times and handles dense key environments like UUIDs effortlessly. It rests on top of a custom Arena memory allocator to prevent fragmentation.
- **The WAL (Write-Ahead Log):** Every successful mutation is serialized and appended to a sequential log buffer.

### 3. The Hardware Abstraction Layer (Storage Drivers)
Yowza! features a pluggable HAL (`StorageDriver` interface).
- **POSIX Driver:** Appends the WAL directly to physical files (`fopen`, `fwrite`) for Native environments.
- **Memory Driver (Shadow WAL):** Appends the WAL to a dedicated offset within the Wasm Linear Memory, specifically designed for serverless execution.

### 4. The Cloud & Synchronization Layer
The final layer handles data durability for ephemeral environments.
- **Cloud Manager:** Orchestrates cold starts (Bootstrapping), asynchronous WAL pushes (`SyncWAL()`), and periodic snapshot creation/compaction.
- **Cluster Coordinator (Raft):** Integrates Hashicorp's Raft consensus algorithm. In a distributed edge scenario, the FSM receives remote logs and bypasses the local WAL to apply distributed mutations safely, ensuring strong consistency across the fleet.
- **Cloud Adapters:** Concrete implementations (AWS S3, Redis) that persist the extracted Shadow WAL streams out into the real world.
