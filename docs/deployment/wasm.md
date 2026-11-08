# WebAssembly Deployment

## The Wazero Host
Yowza! treats WebAssembly as a first-class citizen. By compiling the C core using `clang --target=wasm32-unknown-unknown`, we generate a highly portable `yowza.wasm` module.

In our Go SDK (`yowza.go`), we utilize `tetratelabs/wazero`, a pure Go WebAssembly runtime. This means the engine runs without any CGO overhead or dependencies on the host OS.

## Memory Mapping
The true magic lies in the WASM memory boundary. By exposing the Arena pointer via Wasm exports (`yowza_get_arena_ptr`), the Go host can read and write directly into the database's memory space.

When a snapshot is restored, Go writes the entire byte array directly into the Wasm memory. The C engine instantly recognizes the state—no JSON parsing, no deserialization loops. Just instant memory mapping.
