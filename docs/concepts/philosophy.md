# Philosophy

Traditional databases assume stable infrastructure. Yowza! assumes unreliable networks, intermittent connectivity, and resource-constrained edge environments.

## The Problem
Modern web applications have evolved. With the rise of Serverless computing (e.g., AWS Lambda, Cloudflare Workers), applications no longer reside on long-running instances. Instead, they spin up, handle a request, and die in milliseconds.

Traditional databases (like PostgreSQL, MySQL, or even standalone Redis) struggle with this paradigm for two reasons:
1. **Connection Overhead**: Setting up TLS and TCP handshakes takes longer than the serverless execution itself.
2. **State Ephemerality**: Serverless environments provide no persistent disk.

## The Yowza! Approach
Yowza! is built on three core pillars:

### 1. Dual Deployment (Write Once, Run Anywhere)
Yowza! provides a unified abstraction. If deployed on a Native Server, it utilizes lightning-fast POSIX APIs to persist data directly to a local disk. If compiled to WebAssembly (Wasm) and deployed to the Edge, it strips away OS dependencies and operates entirely within an isolated Linear Memory buffer, later pushing changes to the cloud asynchronously.

### 2. Zero-Copy Performance
The database engine is written in C and compiled to bare-metal Wasm (`nostdlib`). The host environment (Go) interacts with the engine not through heavy TCP/REST queries, but by translating API calls into raw binary opcodes and executing them directly on the Wasm shared memory. This eliminates the heavy Lexing/Parsing overhead of traditional SQL queries.

### 3. Asynchronous Durability (Shadow WAL)
At the edge, synchronous disk I/O is impossible. Yowza! achieves durability by pushing mutations into a lightweight Write-Ahead Log (WAL) inside the Wasm memory. The Go host asynchronously extracts this WAL and persists it to remote storage (e.g., S3 or Redis) *after* responding to the user. If a cold start occurs, Yowza! can fetch a highly-compressed Snapshot and restore its entire internal memory tree in sub-milliseconds.

### Yowza! — Your database shouldn't care where it runs.
