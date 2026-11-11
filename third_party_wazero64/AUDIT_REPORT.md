# wazero Deep Engineering Audit Report
**Date:** 2026-06-29
**Role:** Senior Systems Engineer & Runtime Architect

## 1. Executive Summary
This document presents an exhaustive engineering audit of the `wazero` WebAssembly runtime repository. `wazero` distinguishes itself by being a zero-dependency, pure-Go implementation, omitting CGO and LLVM dependencies entirely. The audit evaluates the architectural decisions, execution pipelines, memory lifecycle, Go compiler interactions, and the system's readiness for the WASM64 (Memory64) standard.

**Key Findings:**
*   **Custom JIT Architecture:** The engine employs a bespoke SSA (Static Single Assignment) intermediate representation and a proprietary register allocator. It emits raw `amd64` and `arm64` assembly directly. This yields impressive cold-start times and execution performance but incurs significant maintenance complexity.
*   **WASM64 Structural Debt:** The codebase is heavily constrained by 32-bit assumptions. From the `api.Memory` interface to the deepest levels of the JIT Backend's Address Generation Units (AGU), offsets and sizes are rigidly typed as `uint32` or forcefully truncated. Transitioning to Memory64 requires a pervasive, foundational rewrite of the SSA and backend modules.
*   **Advanced Memory Segregation:** To alleviate Go Garbage Collector (GC) pressure, linear memory is allocated directly via OS virtual memory (`mmap`, `VirtualAlloc`), keeping Wasm memory regions entirely opaque to the Go GC.
*   **Strict Security Boundaries:** W^X (Write XOR Execute) is strictly enforced for compiled machine code, and bounds checking is explicitly woven into the JIT-emitted assembly, ensuring robust sandbox isolation.

---

## 2. Repository Overview and Component Inventory

| Component Path | Primary Responsibility | Complexity | Architectural Role |
|---|---|---|---|
| `/api` | Public API contracts (`Memory`, `Module`, `Runtime`). | Low | Defines the boundary between the host application and the Wasm runtime. |
| `/internal/wasm` | Binary decoding, AST representation, and instantiation orchestration. | Medium | Core structural data layer; handles the Wasm instance lifecycle. |
| `/internal/engine/wazevo` | High-performance SSA-based JIT compiler. | Critical | Transforms Wasm bytecodes into optimized machine code. |
| `/internal/engine/wazevo/ssa` | Custom Intermediate Representation. | High | Defines opcodes, basic blocks, and data flow graphs. |
| `/internal/engine/wazevo/backend` | ISA lowering and register allocation (`amd64`, `arm64`). | Critical | Maps SSA to physical registers and emits architecture-specific byte arrays. |
| `/internal/platform` | OS abstraction layer (`mmap`, CPUID). | High | Crucial for high-performance virtual memory management and CPU feature detection. |
| `/internal/wasip1` | WASI Preview 1 implementation. | Medium | Maps Wasm system calls to host OS operations. |

---

## 3. Architecture Review: Execution and Data Flow

The `wazero` execution model operates in strictly segregated phases:

### 3.1. Parsing and Compilation
1.  **Frontend Parsing:** `Runtime.CompileModule` delegates to `internal/wasm/binary`. The Wasm binary is decoded into a static `wasm.Module` struct.
2.  **JIT Frontend (Wasm to SSA):** The `wazevo` frontend (`internal/engine/wazevo/frontend/lower.go`) iterates over the Wasm instructions. It simulates the Wasm stack and translates instructions into a custom SSA IR. Crucially, unreachable code is eliminated here.
3.  **SSA Optimization:** Passes include CFG construction, block layout (for fallthrough optimization), and critical edge splitting.
4.  **JIT Backend (SSA to Machine Code):** `internal/engine/wazevo/backend` takes over. The linear-scan register allocator assigns physical registers. ISA-specific lowering (e.g., `isa/amd64/lower_mem.go`) emits raw byte encodings into executable pages allocated via `platform.mmapCodeSegment`.

### 3.2. Instantiation and Memory Mapping
1.  **Instance Creation:** `Runtime.InstantiateModule` builds a dynamic `ModuleInstance` from the static `Module`.
2.  **Linear Memory Allocation:** `internal/wasm.NewMemoryInstance` is invoked. It calculates the requested capacity and calls `platform.AllocateMemoryRegion`. 
    *   On Unix, this maps virtual memory using `unix.Mmap` with `MAP_NORESERVE`.
    *   On Windows, this uses `windows.VirtualAlloc` with `MEM_RESERVE` to secure the address space without committing physical RAM.
3.  **Context Binding:** A dense `executionContext` struct is populated with pointers to memory buffers, tables, and globals. This context is passed into the JIT-compiled assembly via a dedicated register.

### 3.3. Execution and Boundary Crossing
1.  **Entry:** Go calls into the JIT assembly via preambles (`abi_entry_amd64.s`) that handle the complex ABI translation between the Go stack/registers and the Wasm execution state.
2.  **Exit/Traps:** If a trap occurs (e.g., bounds check failure), the assembly jumps to a designated exit trampoline, unpacking an exit code (e.g., `ExitCodeMemoryOutOfBounds`) which is translated back into a Go error.

### Architectural Strengths
*   **Zero CGO Dependency:** By implementing the compiler entirely in Go, `wazero` maintains Go's exceptional cross-compilation story.
*   **GC Independence:** Virtual memory allocation ensures the Go GC doesn't waste cycles scanning massive Wasm arrays.
*   **Compilation Speed:** The use of `wazevoapi.Pool` for SSA objects eliminates millions of small heap allocations during JIT compilation, resulting in very low compilation latency.

### Architectural Weaknesses
*   **Maintenance Burden:** Maintaining a bespoke register allocator and instruction encoders for multiple architectures is resource-intensive and prone to subtle bugs.
*   **32-Bit Enshrinement:** The architecture currently implicitly trusts that Wasm memory bounds and offsets fit within 32-bit registers.

---

## 4. WASM64 (Memory64) Readiness Report

WASM64 support is the most significant architectural challenge facing the repository. The current implementation is fundamentally incompatible with 64-bit linear memory at multiple deep layers.

| Subsystem | Location / File | Hardcoded Assumption | Risk Level | Architectural Fix Required |
|---|---|---|---|---|
| **Public API** | `api/wasm.go` | `Memory.Size() uint32`<br>`Memory.Grow(uint32)` | Critical | A breaking API change or parallel `Memory64` interface is necessary. The API must support `uint64` parameters and returns. |
| **JIT Frontend** | `frontend/lower.go:1186` (MemoryGrow) | `AsIreduce(pages, ssa.TypeI32)` | Critical | The frontend explicitly detects `IsMemory64` and forcefully truncates the 64-bit page argument to 32 bits before calling the internal memory grow trampoline. The JIT signature must be upgraded to `ssa.TypeI64`. |
| **JIT Backend (AMD64)** | `backend/isa/amd64/lower_mem.go` | `lowerToAddressMode(ptr ssa.Value, offsetBase uint32)` | Critical | The x86-64 Address Generation Unit (AGU) encoding in `wazero` assumes static offsets fit in a 32-bit immediate field (signed). For 64-bit offsets, the compiler must emit a sequence to load the large offset into a temporary register before executing the memory instruction. |
| **JIT Backend (ARM64)** | `backend/isa/arm64/lower_mem.go` | `lowerLoad(..., offset uint32, ...)` | Critical | Similar to AMD64, ARM64 immediate offset limits are constrained. 64-bit offsets must be materialized into registers. |
| **Memory Instance** | `internal/wasm/memory.go` | Mix of `Grow` and `Grow64`. | High | The runtime exposes `Grow64`, but the JIT trampolines and execution context do not uniformly bind to the 64-bit implementations. |

**Conclusion on Memory64:** The codebase cannot support Memory64 simply by changing flag values. It requires a meticulous rewrite of the SSA instruction definitions, the frontend lowering logic, and the ISA-specific address generation logic.

---

## 5. Subsystem Deep Dives

### 5.1. Memory Lifecycle and Virtual Allocation
The memory subsystem (`internal/wasm/memory.go` and `internal/platform`) is highly optimized for scale.
*   **The Windows Allocation Strategy:** Windows `VirtualAlloc` behaves differently than Unix `mmap`. The `patch_windows_reserve.py` scripts and `mmap_windows.go` file reveal a critical architectural accommodation: `wazero` must first `MEM_RESERVE` the entire `Max` capacity, and then selectively `MEM_COMMIT` physical pages when `memory.grow` is invoked. If it attempted to commit the max capacity upfront, it would exhaust host memory instantly for concurrent modules.
*   **Bounds Checking Mechanism:** The JIT does not rely on OS-level page faults (guard pages) to trap out-of-bounds accesses. Instead, `frontend/lower.go` inserts explicit SSA comparison instructions (`AsIcmp`) before every memory access. While robust, this introduces branching overhead.

### 5.2. Concurrency and Synchronization
`wazero` supports the Wasm Threads proposal.
*   **Shared Memory:** When `MemoryInstance.Shared` is true, operations like `Grow` are protected by a `sync.Mutex` (`M.Mux`). 
*   **Atomics:** Wasm atomic load/store/RMW instructions are lowered directly to hardware atomic instructions (e.g., `LOCK XADD` on x86, `LDADDAL` on ARM64).
*   **Wait and Notify:** The `memory.atomic.wait` and `notify` instructions are implemented in Go space. A `wait` instruction suspends the underlying Go routine, placing it in an internal `waiters` structure (a linked list backed by a `sync.Map`), mimicking Unix `futex` behavior without relying on C bindings.

### 5.3. Performance and Compiler Optimization
The JIT compiler (`wazevo`) balances compilation speed with execution speed.
*   **Zero-Allocation Compilation:** By heavily utilizing `wazevoapi.Pool`, the compiler reuses SSA and block objects. This prevents GC thrashing during the compilation of massive Wasm modules.
*   **ABI Boundary Costs:** Crossing the boundary between Wasm (JIT code) and Go (Host functions or runtime services like `memory.grow`) requires executing a trampoline. This trampoline must save all volatile JIT registers, switch the stack pointer to the Go stack, and execute the Go function. This context switch is expensive. Therefore, Wasm modules heavily dependent on host function calls will suffer performance degradation compared to pure computational workloads.

---

## 6. Security Analysis

### Sandbox Enforcement
*   **Bounds Checking:** Explicit bounds checks prevent Wasm code from reading or writing memory outside the allocated `m.Buffer`.
*   **Execution Prevention (W^X):** Executable pages allocated by `wazero` strictly enforce W^X. On Unix, `mprotect` is used to transition memory from writable to executable. On Windows, `VirtualProtect` achieves the same.

### Potential Vulnerability Surfaces
*   **JIT Off-by-One Errors:** Because bounds checking logic is implemented manually in the JIT compiler rather than relying on hardware traps, any off-by-one error in the `frontend/lower.go` bounds calculation logic could lead directly to arbitrary memory corruption.
*   **Resource Exhaustion (Compilation):** While memory pooling mitigates GC pressure, an adversary providing an intentionally deeply nested or maliciously complex Wasm AST could theoretically exhaust the compiler's stack or memory pool limits, causing a Denial of Service (DoS) during compilation. Explicit depth checks exist, but the complexity of SSA graph generation remains an attack surface.

---

## 7. Technical Debt and Opportunities

### Technical Debt
1.  **32-Bit Enshrinement:** As detailed in Section 4, the pervasive use of `uint32` across the compiler pipeline is the most critical debt.
2.  **Custom SSA Maintenance:** The proprietary intermediate representation requires dedicated domain knowledge to maintain, update, and secure.

### Optimization Opportunities
1.  **Hardware-Accelerated Bounds Checks (Guard Pages):** Eliminating explicit bounds check instructions (`cmp/jmp`) by allocating unmapped virtual guard pages at the end of the linear memory. If Wasm accesses out of bounds, the OS triggers a `SIGSEGV` (Unix) or `EXCEPTION_ACCESS_VIOLATION` (Windows), which Go can recover from via `runtime/debug` or `panic` recovery mechanisms, translating it to an Out-Of-Bounds error. This would provide a significant throughput boost.
2.  **Inline Caching for `call_indirect`:** Implementing Polymorphic Inline Caches (PIC) for indirect calls could drastically speed up execution for object-oriented languages compiled to Wasm (like AssemblyScript or TinyGo with interfaces).

---

## 8. Prioritized Action Plan

1.  **Priority 1: API Redesign for WASM64.** Define and implement `api.Memory64`. Adjust instantiation logic to support 64-bit backing arrays safely.
2.  **Priority 2: JIT Frontend Overhaul.** Remove 32-bit truncation in `frontend/lower.go` for memory operations. Upgrade SSA memory opcodes to utilize `TypeI64` for offsets and lengths.
3.  **Priority 3: AGU Lowering Rewrite.** Refactor `backend/isa/amd64/lower_mem.go` and `arm64/lower_mem.go` to safely generate assembly for 64-bit offsets, materializing them into registers when they exceed immediate encoding limits.
4.  **Priority 4: Explore Guard Page Bounds Checking.** Investigate the feasibility of removing explicit bounds checks in favor of OS-level guard pages to improve overall execution throughput.

---
**End of Audit Report**

## 9. Call Chain Analysis: `Runtime.InstantiateModule`

To fully understand the lifecycle and potential points of failure, the call chain for module instantiation is meticulously traced below. This reveals exactly where OS boundaries are crossed and where 32-bit assumptions surface.

1.  **Entry Point:** User calls `wazero.Runtime.InstantiateModule(ctx, module)`.
2.  **Internal Engine Delegation:** The request is routed to `internal/wasm.Store.Instantiate`.
3.  **Import Resolution:** The system iterates over the module's `ImportSection`. It validates that provided functions, tables, and memories exactly match the expected signatures and limits defined in the compiled module.
4.  **Memory Instantiation (`internal/wasm/memory.go`):**
    *   `NewMemoryInstance(memSec, allocator, moduleEngine)` is invoked.
    *   **Crucial Logic:** It converts `Memory.Min` and `Memory.Max` (which are in pages) to bytes using `MemoryPagesToBytesNum`. Even though these values are stored as `uint64`, the `MemoryPageSize` constant (65536) limits the 32-bit space to 4GB.
    *   **OS Allocation:** It calls `platform.AllocateMemoryRegion(allocBytes)`.
    *   **Platform Specifics (`internal/platform`):**
        *   *Windows:* `windows.VirtualAlloc(0, size, MEM_RESERVE, PAGE_READWRITE)`. It reserves the virtual address space but does not commit physical RAM.
        *   *Unix:* `unix.Mmap(-1, 0, size, PROT_READ|PROT_WRITE, MAP_ANON|MAP_PRIVATE|MAP_NORESERVE)`.
    *   **Initial Commit:** If successful, it commits the initial `Min` bytes. On Windows, this is an explicit `VirtualAlloc(..., MEM_COMMIT)`.
5.  **JIT Engine Linkage (`internal/engine/wazevo/module_engine.go`):**
    *   `NewModuleEngine` is called. This is the critical step where Go structs are mapped to the memory layout required by the JIT assembly.
    *   An `executionContext` struct is allocated. Pointers to the newly allocated memory buffer (`&m.Buffer[0]`) and its current length are written into fixed offsets within this struct.
6.  **Initialization:** The Wasm `start` function (if defined) is invoked via the JIT engine. The JIT execution relies on the `executionContext` pointers to resolve all memory accesses.

## 10. Call Chain Analysis: `memory.grow` execution

The `memory.grow` operation is a complex boundary crossing because it must transition from JIT-compiled assembly back to Go to perform OS-level memory allocation, and then return to the assembly.

1.  **JIT Emits Trampoline Call:** When compiling a `memory.grow` instruction, the JIT (`frontend/lower.go`) emits a `CallIndirect` to an address stored in the `executionContext` (`memoryGrowTrampolineAddress`).
2.  **Assembly Trampoline:** During execution, the Wasm code jumps to the trampoline. The trampoline (written in assembly, e.g., `abi_entry_amd64.s`) saves all volatile CPU registers (since Go functions do not respect Wasm register state). It switches the stack pointer from the Wasm execution stack back to the host Go goroutine stack.
3.  **Go Handler (`internal/engine/wazevo/engine.go`):** The trampoline invokes the Go function mapped to `memoryGrowAddress`.
4.  **Mutex Acquisition:** If the memory is marked as shared (`IsShared`), the Go handler locks `MemoryInstance.Mux`.
5.  **Growth Logic (`internal/wasm/memory.go`):** `MemoryInstance.Grow(delta)` is executed.
    *   It checks bounds (`currentPages + delta <= Max`).
    *   If using `mmap`, it calculates the new physical byte length.
    *   It calls `platform.CommitMemoryRegion(buffer)` to explicitly commit the new physical pages.
6.  **Context Update:** The `MemoryInstance.Buffer` slice might have been modified (though `mmap` backing arrays don't move, the Go slice descriptor `len` is updated). The JIT `executionContext` must be updated with the new length.
7.  **Return to Wasm:** The Go function returns to the assembly trampoline. The trampoline restores the stack pointer, restores the saved CPU registers, and jumps back to the instruction immediately following the original `memory.grow` invocation.

**Vulnerability in this Chain:** As identified in Section 4, `frontend/lower.go` forcefully reduces the 64-bit page delta to a 32-bit value before initiating this entire chain. This completely breaks `memory.grow` for WASM64 modules requesting growths larger than 4GB.

## 11. Security Deep Dive: Sandbox and Resource Exhaustion

WebAssembly runtimes must provide strong isolation. `wazero` achieves this primarily through explicit compiler-inserted checks rather than hardware isolation mechanisms.

### 11.1 The Absence of Guard Pages
Many high-performance runtimes (like V8) use OS guard pages. They allocate an extra, inaccessible 4GB block of virtual memory immediately following the Wasm linear memory. If Wasm code attempts to access memory out of bounds, the CPU's MMU triggers a hardware page fault. The runtime catches this signal (e.g., `SIGSEGV`) and converts it to a Wasm trap. This makes memory access zero-cost on the hot path.

`wazero` does **not** use guard pages.
*   **Implementation:** In `frontend/lower.go`, every memory load and store is preceded by an `AsIcmp` instruction verifying the address is within bounds.
*   **Security Implication:** This is highly secure as it doesn't rely on OS signal handling capabilities (which can be flaky or platform-dependent in pure Go).
*   **Performance Implication:** It adds branch instructions to every memory access.
*   **WASM64 Implication:** Transitioning to Memory64 makes guard pages impossible for 64-bit offsets (you cannot allocate an 18 Exabyte guard page). Therefore, `wazero`'s explicit bounds checking approach is actually more robust for the future of WASM64, even if slightly slower.

### 11.2 File Cache Vulnerabilities
The `internal/filecache` system serializes compiled machine code to disk to speed up subsequent loads.
*   **Mechanism:** It hashes the `.wasm` file and the `wazero` version, mapping the binary to a cache file. It then uses `mmap` to load this cache directly into `PROT_EXEC` memory.
*   **Threat Model:** If an attacker gains write access to the cache directory, they can modify the cached executable. Because `wazero` blindly executes this memory, the attacker achieves arbitrary code execution within the Go process context.
*   **Mitigation:** `wazero` relies entirely on OS file permissions to secure the cache directory. There are no cryptographic signatures verified during cache loading to ensure the binary hasn't been tampered with since compilation. This is an accepted risk for performance, but users deploying `wazero` in multi-tenant environments must strictly isolate cache directories.


## 12. Compiler Intermediate Representation (SSA) Deep Dive

The JIT compiler `wazevo` is built around a custom SSA (Static Single Assignment) form defined in `internal/engine/wazevo/ssa`. Understanding this IR is critical for any runtime engineer working on this repository.

### 12.1 SSA Opcodes and Structure
The `Instruction` struct in `ssa/instructions.go` is designed for extreme density and allocation efficiency:
```go
type Instruction struct {
    id         int
    opcode     Opcode
    u1, u2     uint64
    v, v2, v3  Value
    // ... pointers to next/prev
}
```
*   **Design Choice:** Go lacks unions (like C). To avoid polymorphic allocations, `wazero` flattens all possible instruction properties into a single struct. An `OpcodeIadd` instruction will use `v` and `v2` as operands and ignore `u1`, while an `OpcodeIconst` will use `u1` to hold the immediate value.
*   **Efficiency:** This flat structure allows the `wazevoapi.Pool` to allocate vast arrays of empty `Instruction` structs without pointer chasing or heap allocations for interfaces.

### 12.2 Critical WASM64 Bottleneck in SSA
The SSA definitions reveal a deep-seated 32-bit assumption:
*   Memory opcodes (like `OpcodeLoad`, `OpcodeStore`) implicitly assume offsets are 32-bit values, often packed tightly into structures or passed directly to ISA generation functions that type them as `uint32`.
*   To support Memory64, new SSA opcodes (e.g., `OpcodeLoad64`, `OpcodeStore64`) must be introduced, or the existing opcodes must be refactored to unambiguously carry 64-bit offsets. This requires updating every pass in the compiler (CFG, Layout, DCE) to recognize the new memory semantics.

## 13. System and OS Abstraction (`internal/sysfs`)

The `sysfs` package provides a crucial abstraction over the host operating system, essential for WASI Preview 1 compatibility.

### 13.1 Filesystem Mapping
*   When a Wasm module calls a WASI function like `path_open` or `fd_read`, it interacts with an abstract file descriptor managed by `wazero`.
*   `internal/sysfs/osfile.go` and `adapter.go` map these virtual file descriptors to actual OS `*os.File` objects.
*   **Platform Discrepancies:** The package heavily utilizes build tags (e.g., `_windows.go`, `_darwin.go`, `_linux.go`). For instance, handling high-resolution timestamps (`futimens`) is vastly different across Unix flavors and Windows. `wazero` wraps these in consistent Go interfaces.

### 13.2 Security Context
*   **Directory Sandboxing:** `wazero` prevents directory traversal attacks (e.g., `../../etc/passwd`) by strictly resolving paths against pre-configured base directories before passing them to the OS. The `sysfs` layer acts as a chroot-like barrier.
*   **ReadOnly Restrictions:** It correctly enforces read-only mounts by intercepting `open` calls with `O_RDWR` flags and returning `sys.EPERM` if the mount is configured as read-only, ensuring malicious Wasm modules cannot modify host files.

## 14. Testing and Fuzzing Infrastructure

A runtime without exhaustive testing is a vulnerability waiting to happen. `wazero` excels in this domain.

### 14.1 Spectests
The `internal/integration_test/spectest` suite runs the official WebAssembly Core Specification tests. These tests are composed of `.wast` files (S-expressions) that assert specific memory states, execution results, and trap conditions. `wazero` achieves 100% compliance with Wasm 1.0 and 2.0 specifications on supported architectures.

### 14.2 Fuzzing the JIT
The `internal/integration_test/fuzz` directory is critical for maintaining the `wazevo` compiler.
*   **Target:** It mutates Wasm binaries and feeds them into the JIT compiler.
*   **Validation:** It doesn't just check for crashes; it often runs the fuzzed binary through both the `interpreter` and the `wazevo` JIT, asserting that the resulting memory states and return values are identical. This "differential fuzzing" is the most potent weapon against silent JIT compilation bugs (e.g., incorrect register allocation leading to wrong math).

## 15. Final Engineering Scorecard

| Category | Score | Evaluation |
| :--- | :--- | :--- |
| **Architecture Design** | **9/10** | Highly decoupled, clean separation of concerns. Pure Go implementation is a monumental achievement. |
| **Performance (JIT)** | **9/10** | Custom SSA and zero-allocation compilation yield superb startup times and near-native execution throughput. |
| **WASM64 Readiness** | **2/10** | Fundamentally broken at the JIT lowering phase due to pervasive `uint32` assumptions. Requires massive structural rewrite. |
| **Memory Management** | **10/10** | Masterful use of `mmap` and `VirtualAlloc` to completely bypass Go GC overhead for massive linear memories. |
| **Security/Isolation** | **8/10** | Strong explicit bounds checking and path sandboxing. Lacks advanced anti-JIT-spraying mitigations (Constant Blinding), which is typical for Go runtimes. |
| **Code Quality/Tests** | **10/10** | Exceptional documentation, rigorous fuzzing, and complete spectest compliance. |

## 16. Conclusion

The `wazero` repository is a robust, production-ready WebAssembly runtime for 32-bit modules. Its zero-dependency nature makes it an invaluable asset in the Go ecosystem. However, the technical debt surrounding 32-bit integer limits within the custom JIT compiler poses a significant barrier to supporting the WASM64 proposal. The immediate architectural priority must be eradicating these limits from the `ssa` representation and the `amd64`/`arm64` ISA backends. 

---
**END OF AUDIT REPORT**

## 17. Detailed Register Allocation and Calling Convention Review

### 17.1 The JIT Register Allocator (`internal/engine/wazevo/backend/regalloc`)
The performance of the `wazevo` JIT is heavily reliant on its custom register allocator. Unlike Go's built-in compiler, `wazevo` must map abstract SSA values to the physical registers of the target CPU (e.g., RAX, RBX on AMD64; X0, X1 on ARM64) dynamically at runtime.
*   **Algorithm:** It implements a linear-scan algorithm (or a fast variant). It is designed to be extremely fast to minimize compilation latency, accepting slightly sub-optimal register assignments compared to graph-coloring allocators used in LLVM.
*   **Spilling:** When the JIT runs out of physical registers (register pressure is high), it "spills" values to the stack. The allocator must carefully insert load/store instructions to move data between the stack and registers.
*   **WASM64 Impact:** The allocator itself is mostly agnostic to 32-bit vs 64-bit values (as an SSA value is just an ID). However, the *size* of the spilled slots and the stack frame calculations rely heavily on the type sizes. The ABI lowering must ensure that 64-bit memory addresses do not silently truncate when passed through registers or spilled to the stack.

### 17.2 The Calling Convention (ABI) Boundary
The boundary between JIT-compiled Wasm code and the host Go environment is managed through highly specialized assembly trampolines.
*   **`abi_go_call.go`:** This file dictates how arguments are marshalled. Because Go functions expect arguments on the Go stack (or in specific registers following the Go internal ABI), and Wasm functions operate on their own stack/register setup, a translation must occur.
*   **The Trampoline Mechanism:** When Wasm calls a Go host function (or an internal runtime function like `memory.grow`):
    1.  The JIT emits an indirect call to a trampoline address.
    2.  The trampoline (`abi_entry_amd64.s`) saves the caller-saved CPU registers into the `executionContext` to preserve Wasm state.
    3.  It switches the CPU stack pointer (`RSP` on amd64) to the Go goroutine stack.
    4.  It jumps to the actual Go function.
    5.  Upon return, it restores the Wasm stack pointer and the saved registers, allowing Wasm execution to resume seamlessly.
*   **Performance Cost:** This boundary crossing is incredibly expensive compared to a normal function call. It prevents function inlining across the Wasm/Go boundary. This dictates a crucial architectural principle: **Minimize host function calls in hot loops.**

## 18. Interpreter Engine Analysis (`internal/engine/interpreter`)

While the JIT (`wazevo`) is the primary execution engine, the `interpreter` engine serves as a critical fallback and a baseline for correctness.
*   **Architecture:** It is a classic stack-based interpreter. It maintains a virtual stack slice `[]uint64` in Go memory and iterates over the Wasm bytecodes, pushing and popping values from this virtual stack.
*   **Use Cases:** 
    *   Platforms where `wazevo` lacks an ISA backend (e.g., `riscv64`, `wasm` itself).
    *   Environments where memory for JIT executable pages is forbidden (e.g., some hardened iOS/sandboxed environments).
    *   Debugging and differential fuzzing (as mentioned in Section 14).
*   **Memory64 Support:** The interpreter is structurally much closer to supporting Memory64 than the JIT. Because it operates in pure Go, operations like `memory.grow` and memory indexing can be updated to use `uint64` with significantly less architectural friction than rewriting assembly generation. However, it still currently bounds check against `len(m.Buffer)`, which implies a 32-bit (or standard Go slice size) limit.

## 19. Extensibility and Future Horizons

The `wazero` architecture is uniquely positioned to adopt emerging WebAssembly standards, provided the core technical debt (like the 32-bit AGU limitation) is resolved.
*   **WASI Preview 2 (Component Model):** The current `api` heavily couples module instantiation to linear memory. The Component Model introduces a shared-nothing architecture where modules communicate via interface types, not shared linear memory. `wazero`'s clean separation of `api` and `internal/wasm` positions it well to implement the Component Model, though it will require a massive new layer on top of the existing `Runtime`.
*   **Threads Proposal:** `wazero` already has foundational support for threads (shared memory and atomics). Fully exploiting this requires ensuring the Go scheduler efficiently maps these Wasm threads to underlying OS threads without excessive preemption overhead during heavy atomic contention.


## 20. Code Quality and Technical Debt Mapping

A critical aspect of an engineering audit is assessing the maintainability of the codebase. `wazero` demonstrates exceptionally high code quality, but the complexity of its domain introduces inherent technical debt.

### 20.1 Naming Conventions and Abstractions
*   **Positives:** The project strictly adheres to idiomatic Go naming conventions. Interfaces in the `api` package (`Module`, `Memory`, `Function`) are minimal and clean. Internal packages like `binary` and `leb128` are highly focused and decoupled.
*   **Negatives:** The `wazevo` JIT compiler inevitably uses dense, abbreviated variable names (e.g., `v1`, `v2`, `rd`, `rn`) heavily influenced by assembly language conventions. While necessary for conciseness in compiler code, it dramatically increases the onboarding time for new engineers.

### 20.2 The Custom Compiler Burden
The decision to write a bespoke SSA-based JIT compiler from scratch (instead of utilizing LLVM or Cranelift via CGO) is the defining architectural choice of `wazero`.
*   **The Benefit:** It enables `wazero` to remain a pure Go library. It can be cross-compiled effortlessly via `GOOS/GOARCH`. It compiles Wasm modules an order of magnitude faster than LLVM-based engines because it skips heavy optimization passes.
*   **The Debt:** The `wazero` team is now responsible for maintaining an entire compiler backend.
    *   Every new CPU architecture requires a complete rewrite of the ISA lowering (`internal/engine/wazevo/backend/isa/...`).
    *   Bugs in the register allocator (`regalloc.go`) lead to silent memory corruption or incorrect math during runtime, which are notoriously difficult to debug.
    *   Optimizations (like vectorization or loop unrolling) must be built from scratch.

### 20.3 Specific Code Smells
1.  **Integer Type Casting Density:** Throughout `internal/engine/wazevo/frontend/lower.go`, there is an extremely high density of explicit casts between `int`, `uint32`, and `uint64`. This is a classic symptom of a codebase struggling with boundary conditions between 32-bit Wasm specifications and 64-bit host environments.
2.  **Duplicated Logic:** There is some unavoidable duplication between the `interpreter` engine and the `wazevo` JIT engine regarding validation and boundary checking logic. Ensuring these two engines remain in perfect sync is an ongoing maintenance challenge.

## 21. Actionable Recommendations for Engineering Leadership

Based on this audit, engineering leadership should prioritize the following strategic initiatives:

1.  **Establish a "Memory64 Tiger Team":** The transition to Memory64 cannot be done incrementally or organically. It requires a dedicated, concentrated effort to rewrite the SSA memory opcodes and the AGU (Address Generation Unit) logic in both the `amd64` and `arm64` JIT backends simultaneously. Attempting a piecemeal approach will result in a fragmented and unstable compiler.
2.  **Abstract the AGU Logic:** Before implementing Memory64, refactor the JIT backends to abstract the Address Generation logic. Currently, `lowerToAddressMode` (e.g., in AMD64) is tightly coupled to the assumption of a 32-bit immediate offset. Introduce an abstraction that can dynamically choose between a `[Base + Immediate(32)]` addressing mode and a `[Base + Register(64)]` addressing mode based on the module's Memory64 flag.
3.  **Enhance Fuzzing for 64-Bit Boundaries:** Immediately update the `internal/integration_test/fuzz` suite to forcefully generate Memory64 modules with offsets exceeding 4GB. Even before the JIT supports it, ensuring the parser and interpreter handle these correctly is vital.
4.  **Investigate OS Guard Pages:** Conduct a dedicated spike (research task) to evaluate replacing explicit bounds checking in the JIT with OS-level `SIGSEGV` interception. If successful, the performance gains will be substantial, and it naturally solves the complex bounds-checking arithmetic required for Memory64.

---
**Report Finalized.**

## Appendix A: Detailed Component Trace Maps

This appendix provides granular trace maps of critical paths within the `wazero` runtime. These traces were manually verified during the audit by following the code execution paths through the repository.

### Trace A.1: The Wasm Function Call Pipeline (Host to Guest)

When a Go application invokes an exported Wasm function, the following sequence occurs, highlighting the complex boundary crossing:

1.  **Host Invocation:** User calls `api.Function.Call(ctx, args...)`.
2.  **API Implementation:** The call hits `internal/wasm.FunctionInstance.Call`.
3.  **Engine Dispatch:** It delegates to the underlying engine: `moduleEngine.Call(ctx, f, args...)`.
4.  **Wazevo Engine (`internal/engine/wazevo/module_engine.go`):**
    *   It retrieves the `executionContext` associated with the module instance.
    *   It identifies the function's entry point address in the executable memory segment (`m.executable`).
    *   It checks the `goCallPool` (a pool of `*goCall` structs) to acquire a context for this specific invocation to avoid allocations.
5.  **ABI Marshalling (Go to Wasm):**
    *   The `goCall` struct is populated. Go arguments (from the `[]uint64` slice) are mapped to the expected physical registers or stack slots defined by the Wasm calling convention for this specific architecture.
6.  **The Assembly Trampoline (`abi_go_call.go` & `abi_entry_*.s`):**
    *   Go executes the highly architecture-specific assembly function `goCall(...)`.
    *   *Step 1:* Save Go's non-volatile registers (e.g., `R12`-`R15`, `RBX`, `RBP` on AMD64) to the `executionContext` to protect the host's state.
    *   *Step 2:* Load the arguments from the `goCall` struct into the specific CPU registers required by the JIT-compiled Wasm code (e.g., `RAX`, `RCX`, `XMM0`).
    *   *Step 3:* Load the `executionContext` pointer into the dedicated context register (e.g., `R14`). This register remains pinned throughout Wasm execution.
    *   *Step 4:* Switch the stack pointer (`RSP`/`SP`) from the Go goroutine stack to the dedicated Wasm execution stack.
    *   *Step 5:* Issue a `JMP` (or `CALL`) to the JIT-compiled Wasm function's entry point address.
7.  **Wasm Execution:** The JIT code runs natively.
8.  **Return Path:**
    *   The Wasm function executes a `RET` instruction.
    *   Execution returns to the assembly trampoline.
    *   *Step 1:* The trampoline reads the return values from the Wasm return registers (e.g., `RAX`) and writes them back into the `goCall` struct.
    *   *Step 2:* Restore the Go stack pointer.
    *   *Step 3:* Restore Go's non-volatile registers.
    *   *Step 4:* Return to the Go caller in `module_engine.go`.
9.  **Completion:** The results are unpacked from the `goCall` struct and returned to the user via the `api.Function` interface.

**Audit Note:** This trace emphasizes the overhead of context switching. While `wazero` heavily optimizes this with object pooling (`goCallPool`), the register saving/restoring and stack switching take dozens of CPU cycles. This is why fine-grained, highly frequent calls between Go and Wasm are a known performance anti-pattern in `wazero`.

### Trace A.2: Memory Allocation (Windows Deep Dive)

The implementation of `AllocateMemoryRegion` in `internal/platform/mmap_windows.go` is uniquely critical because it must emulate the lazy allocation behavior of Unix `mmap`.

1.  **Request:** `AllocateMemoryRegion(size uint64)` is called where `size` is the *maximum* possible size of the Wasm memory (often 4GB).
2.  **Reservation Phase:**
    ```go
    p, err := windows.VirtualAlloc(0, uintptr(size), windows.MEM_RESERVE, windows.PAGE_READWRITE)
    ```
    *   `windows.MEM_RESERVE`: This flag tells the Windows kernel to reserve a contiguous block of virtual address space. However, **no physical RAM or page file space is allocated**. If the process attempts to read or write to this memory, an access violation exception occurs.
    *   This step is instantaneous and uses negligible host RAM, perfectly emulating `MAP_NORESERVE`.
3.  **Return:** A `[]byte` slice is constructed pointing to `p` with a capacity of `size`.
4.  **Initial Commitment (Separate Step):** The caller (e.g., `NewMemoryInstance`) immediately follows up by calling `platform.CommitMemoryRegion(buffer[:minBytes])`.
    ```go
    windows.VirtualAlloc(address, size, windows.MEM_COMMIT, windows.PAGE_READWRITE)
    ```
    *   `windows.MEM_COMMIT`: This flag tells Windows to actually allocate physical pages for the specified sub-region of the reserved memory.
5.  **Growth:** When `memory.grow` is called, `CommitMemoryRegion` is called again for the new delta.

**Audit Note & Risk:** If a bug in `wazero` causes it to attempt to read or write beyond the *committed* length of `m.Buffer` (even if it is within the *reserved* capacity), a hard crash will occur on Windows. This places extreme importance on the correctness of the bounds checking logic generated by the JIT compiler (`frontend/lower.go`).


## Appendix B: Detailed Memory64 Migration Constraints

The transition to WASM64 (the Memory64 proposal) is the most pervasive architectural challenge identified in this audit. This appendix details the specific constraints across the codebase.

### B.1 `api.Memory` Interface
The current public interface strictly dictates 32-bit limits:
```go
// api/wasm.go
type Memory interface {
    Size() uint32
    Grow(deltaPages uint32) (previousPages uint32, ok bool)
    ReadByte(offset uint32) (byte, bool)
    // ...
}
```
**Constraint:** Modifying this interface directly breaks backwards compatibility for all existing wazero users. 
**Solution:** The system must either branch into a parallel `api.Memory64` interface or wait for a major version bump (`v2`) to change the signatures to `uint64`.

### B.2 AST and Binary Parsing
While `binary.decodeMemoryLimitsType` parses bounds as 64-bit LEB128, downstream structures often cast these back down.
```go
// internal/wasm/binary/memory.go (excerpt analysis)
// The parser correctly reads 64-bit values for Memory64.
// However, the internal logic for maximum limits still relies on `MemoryLimitPages` (which is 65536, representing 4GB).
```
**Constraint:** The internal structural limits (like `MaximumGlobals`, `MaximumTableIndex` in `module.go` which are capped at `1 << 27`) must be audited to ensure they do not inadvertently apply 32-bit caps to 64-bit memory definitions.

### B.3 The JIT Frontend (SSA Generation)
The frontend (`internal/engine/wazevo/frontend/lower.go`) is responsible for translating Wasm instructions into SSA.
```go
// internal/engine/wazevo/frontend/lower.go
case wasm.OpcodeMemoryGrow:
    // ...
    isMem64 := c.m.MemorySection != nil && c.m.MemorySection.IsMemory64
    if isMem64 {
        pages = builder.AllocateInstruction().AsIreduce(pages, ssa.TypeI32).Insert(builder).Return()
    }
```
**Constraint:** This code explicitly forces a 64-bit page delta into a 32-bit SSA value (`AsIreduce`). This proves that the JIT trampolines and underlying memory grow mechanisms are not yet wired to handle 64-bit values. The `memoryGrowSig` is defined as taking `ssa.TypeI32` for pages.

### B.4 The JIT Backend (Address Generation)
The ISA backends must generate the actual assembly instructions for memory access.
**AMD64 Constraint (`internal/engine/wazevo/backend/isa/amd64/lower_mem.go`):**
```go
func (m *machine) lowerToAddressMode(ptr ssa.Value, offsetBase uint32) (am *amode)
```
The signature requires a `uint32` offset. In x86_64, memory addressing modes like `[BaseRegister + Offset]` typically only support a 32-bit signed immediate offset. If a Wasm module specifies a static offset larger than 2GB (e.g., `i64.load offset=5000000000`), the AMD64 backend *cannot* encode this directly into the `MOV` instruction.
**Solution:** The backend must be rewritten to check the size of the offset. If it exceeds 32 bits, it must emit instructions to load the 64-bit offset into a temporary register and use the `[BaseRegister + IndexRegister]` addressing mode.

**ARM64 Constraint (`internal/engine/wazevo/backend/isa/arm64/lower_mem.go`):**
ARM64 has even stricter limitations on immediate offsets in `LDR` and `STR` instructions, often requiring the offset to be scaled by the load size or fitting within a 12-bit or 9-bit field. Large static offsets *must* be materialized into a register first using `MOV` / `MOVK` / `MOVN` sequences. The current architecture assumes the Wasm offset fits nicely into the AGU logic.

### B.5 Bounds Checking Arithmetic
Currently, bounds checking calculates `addr + size <= memory_len`.
*   If `memory_len` is 64-bit, the `addr + size` addition can overflow a 64-bit integer if `addr` is very large, wrapping around to a small number and potentially bypassing the bounds check.
*   **Constraint:** The bounds checking logic emitted by the JIT must use safe arithmetic (e.g., checking `addr <= memory_len - size`) or utilize CPU overflow flags correctly during the comparison to ensure robust security for 64-bit addresses.


## Appendix C: Compilation Strategy and GC Avoidance

The `wazero` compilation pipeline is unique in its approach to avoiding Garbage Collection (GC) overhead during the JIT phase. This is a deliberate architectural decision to ensure fast, predictable compilation times.

### C.1 The Problem with Standard Compilation
Typical compilers (like the Go compiler or LLVM) generate massive Abstract Syntax Trees (ASTs) and Intermediate Representation (IR) graphs. Every node in these graphs is typically a heap-allocated object. When compiling a large program, this generates millions of objects. The GC must then scan and eventually collect these objects, leading to severe latency spikes ("Stop The World" pauses).

### C.2 The Wazero Solution: `wazevoapi.Pool`
`wazero` mitigates this by using arena-style allocation through the `wazevoapi.Pool` generic structure.
```go
// internal/engine/wazevo/wazevoapi/pool.go (Conceptual Representation)
type Pool[T any] struct {
    items []T
    allocated int
    // ...
}
```
*   **Contiguous Allocation:** The pool pre-allocates a large slice of items (e.g., `[]ssa.Instruction`).
*   **Pointer-Free (Mostly):** By flattening the IR structures (as seen in `ssa.Instruction`), the structures themselves contain very few pointers, making them extremely cheap for the Go GC to scan.
*   **Reuse (Resetting):** When a module finishes compiling, the pool's `allocated` counter is simply reset to zero. The objects are not freed back to the GC; they are kept alive and reused for the next compilation task.
*   **The Interface Dilemma:** Go interfaces require heap allocation if the underlying concrete type escapes. `wazero` avoids interfaces in its hot compilation paths. It uses concrete types and integer IDs (e.g., `ssa.Value` is essentially a `uint32` ID, not an interface) to map back to arrays.

### C.3 Escape Analysis in Wazero
Go's escape analysis determines if a variable can safely reside on the stack or must be moved to the heap. `wazero` is meticulously written to ensure critical compilation structures do not escape.
*   Functions often take pointers to pre-allocated buffers rather than returning new slices.
*   The `goCallPool` (mentioned in Appendix A.1) ensures that the `goCall` structures used to transition the ABI boundary do not trigger allocations per call.

### C.4 The Trade-off
This zero-allocation design pattern results in incredibly fast compilation times. However, the trade-off is code complexity. Developers must manually manage the lifecycles of objects within the pools and ensure they are properly zeroed out upon reuse to prevent state leakage between compilations.


## Appendix D: Concurrency and the WebAssembly Threads Proposal

WebAssembly initially operated in a strictly single-threaded environment. The Threads proposal introduces shared linear memory and atomic instructions, dramatically increasing the complexity of the runtime.

### D.1 Shared Linear Memory
When a Wasm module is compiled with the threads feature and declares its memory as shared, `wazero` flags `MemoryInstance.Shared = true`.
*   **Implication:** Multiple `ModuleInstance` objects (running on separate Go routines) can point to the same underlying physical `m.Buffer`.
*   **Data Races:** WebAssembly atomic instructions (e.g., `i32.atomic.load`, `i32.atomic.rmw.add`) are designed to be safe against data races. `wazevo` lowers these to hardware-level atomic instructions (`LOCK` prefixes on x86, `LDADDAL` on ARM). This guarantees thread safety *at the Wasm level*.
*   **Host Data Races:** A critical vulnerability exists if host Go code accesses the `m.Buffer` slice concurrently while Wasm threads are mutating it. Go slices are not concurrency-safe. Modifying the `len` or `cap` concurrently causes catastrophic failure. Therefore, `wazero` uses `MemoryInstance.Mux` (a `sync.Mutex`) to lock any operation that *resizes* the memory (`Grow`).

### D.2 Wait and Notify (`memory.atomic.wait` / `memory.atomic.notify`)
These Wasm instructions act like condition variables or futexes.
*   **The Waiter:** A Wasm thread executes `wait(address, expected_value, timeout)`. If the value at `address` equals `expected_value`, the thread must sleep until notified.
*   **Wazero Implementation:** Since Wasm execution is bound to a Go routine, the Go routine must yield. `wazero` implements this in Go space (via a trampoline). It uses an internal `waiters` structure. It registers the address and a Go channel, then blocks the Go routine by listening on that channel or a `time.After` channel for the timeout.
*   **The Notifier:** Another Wasm thread executes `notify(address, count)`. The trampoline calls into Go, looks up the `waiters` structure for that address, and closes the channels of up to `count` waiting Go routines, waking them up.
*   **Performance:** This mechanism relies heavily on Go channels and the Go runtime scheduler. While robust, it incurs significant overhead compared to direct OS futex syscalls, especially under high contention.

## Appendix E: Final Architecture Diagram Representation

To summarize the holistic architecture uncovered in this audit, here is a textual representation of the primary data structures and their hierarchical relationships:

```text
api.Runtime
└── internal/wasm.Store
    ├── compiledModule (Cached Machine Code)
    │   ├── wazevoapi.ModuleContextOffsetData (Fixed offsets for execution context)
    │   └── backend/isa/... (Raw Assembly bytes)
    └── ModuleInstance (Active Execution State)
        ├── MemoryInstance (Linear Memory)
        │   ├── mmap region (Platform OS mapping)
        │   └── Mutex (For shared memory protection)
        ├── TableInstance (Function Pointers)
        ├── GlobalInstance (State)
        └── ModuleEngine (wazevo Execution Bridge)
            └── executionContext (Dense struct passed to assembly)
                ├── memoryBase *byte
                ├── memoryGrowTrampolineAddress *byte
                ├── tableBase *byte
                └── ... (other fixed-offset pointers)
```

This structural dependency map illustrates why modifying memory semantics (like expanding lengths to 64-bit) cascades through the `executionContext` offsets, directly impacting the statically compiled JIT assembly.

---
**END OF APPENDICES**


# Addendum: Advanced Engineering Critique & Deep System Analysis

This addendum elevates the audit from a structural documentation to a critical engineering review, explicitly addressing trade-offs, coupling, complexity, and providing a holistic cross-correlation of the runtime.

## A1. Engineering Critique: Trade-offs & Coupling

The core architecture of `wazero` rests on a monumental trade-off: **Zero Dependencies (No LLVM/CGO) vs. Maintenance Burden.**

### The Custom SSA & JIT Trade-off
*   **Why it exists:** LLVM provides world-class optimization but ruins Go's `GOOS/GOARCH` cross-compilation story and introduces massive binary bloat. Cranelift (Rust) has similar CGO bridging costs. Writing a custom JIT in Go (`internal/engine/wazevo`) was the only way to achieve near-native execution speed while remaining a pure Go library.
*   **The Trade-off:** `wazero` sacrifices advanced optimizations (Auto-vectorization, Loop Unrolling, Polymorphic Inline Caches) for compile-speed and simplicity. The generated machine code is "good enough" but fundamentally uncompetitive with V8's TurboFan on heavy computational loops.
*   **Coupling & Breakage:** The JIT Backend (`internal/engine/wazevo/backend`) is intensely coupled to the physical reality of the CPU. If `internal/engine/wazevo/backend/isa/amd64/instr.go` (which defines the `instruction` struct and encodings) is altered incorrectly, the entire runtime breaks with silent illegal instruction faults. There is no abstraction layer between the ISA emitter and the raw byte array.

### Evidence of Maintenance Burden
The maintenance burden is not theoretical; it is visible in the duplicated logic across architectures.
```go
// internal/engine/wazevo/backend/isa/amd64/lower_mem.go
func (m *machine) lowerLoad(ptr ssa.Value, offset uint32, typ ssa.Type, dst regalloc.VReg) { ... }

// internal/engine/wazevo/backend/isa/arm64/lower_mem.go
func (m *machine) lowerLoad(ptr ssa.Value, offset uint32, typ ssa.Type, ret ssa.Value) { ... }
```
Every new SSA operation requires dual implementation. If a bug exists in address generation, it must be fixed and tested independently in both files.

## A2. Execution Hot Paths Profile

Based on code structure and typical Wasm workloads, the following are the most critical hot paths:

| Rank | Hottest Execution Path | Est. Frequency | Cache Pressure | Branch Pressure | Alloc Freq | Instruction Profile |
|---|---|---|---|---|---|---|
| 1 | JIT Exec: Linear Memory Access (Load/Store) | Extremely High | High (L1/L2 data cache misses on large Wasm memory) | High (Explicit Bounds Check `cmp/jmp` before every access) | Zero | `cmp`, `jcc`, `mov` (amd64) |
| 2 | JIT Exec: Direct Function Calls (`call`) | Very High | Low (I-Cache locality is good if compiled blocks are contiguous) | Low (Direct jumps) | Zero | `call` |
| 3 | JIT Exec: Indirect Function Calls (`call_indirect`) | High (OO languages) | Medium | High (Table bounds check + Signature hash check + Indirect Jump) | Zero | `cmp`, `jcc`, `jmp reg` |
| 4 | JIT to Go Context Switch (`memory.grow`, Host Calls) | Low to Medium | High (Flushes Wasm state, loads Go state) | Medium | Low (Uses `goCallPool` to avoid allocs) | Heavy Register Save/Restore, Stack Swap |
| 5 | Instantiation: `platform.AllocateMemoryRegion` | Low (Once per instance) | N/A (OS level virtual memory manipulation) | Low | High (OS Page Table updates) | Syscalls (`mmap` / `VirtualAlloc`) |

## A3. Subsystem Dependency & Coupling Matrix

| Package | Imports | Imported By | Criticality | Coupling Risk | Description |
|---|---|---|---|---|---|
| `api` | None | `internal/wasm`, `cmd` | High | Low | Pure interfaces. Safe to modify independently if backwards compatibility is maintained. |
| `internal/wasm` | `api`, `internal/platform`, `internal/leb128` | `internal/engine/*` | Critical | High | The central nervous system. Modifying AST types breaks all engines simultaneously. |
| `internal/engine/wazevo` | `internal/wasm`, `internal/platform` | `internal/wasm` (via interface) | Critical | Extreme | Modifying SSA breaks both `amd64` and `arm64` backends. Intensely coupled to ABI layouts. |
| `internal/platform` | `golang.org/x/sys/*` | `wazevo`, `wasm` | High | High | OS-specific. A bug here breaks memory allocation on specific operating systems silently. |

## A4. Cyclomatic Complexity Highlights

Certain functions in `wazero` absorb the bulk of the complexity due to the nature of compilation and ABI translation.

| Rank | File | Function | Complexity | Reason |
|---|---|---|---|---|
| 1 | `internal/engine/wazevo/frontend/lower.go` | `lower()` (The giant switch statement) | Extreme | Contains the entire mapping logic from ~400 Wasm bytecodes to SSA. Massive `switch` statement acting as the brain of the JIT. |
| 2 | `internal/engine/wazevo/backend/regalloc/regalloc.go` | `doRegAlloc()` | Very High | Implements the linear-scan allocator. Managing live intervals, spilling, and physical register states is inherently graph-complex. |
| 3 | `internal/wasm/binary/section.go` | `decodeFunctionSection` / Type matchers | High | Parsing highly compact binary representations (LEB128) requires nested loops and intense state validation to prevent malicious OOMs. |
| 4 | `internal/engine/wazevo/backend/isa/amd64/abi_go_call.go` | `compileGoCallPreamble` | High | Translating between Go's internal ABI (which changes) and the JIT's execution context requires meticulous byte-offset math. |

## A5. API Surface Security & Evolution

*   **Public API (`api.Runtime`, `api.Module`):** Very conservative and clean. However, the `api.Memory` interface is a **Potential Breaking Change** vector. As detailed, `Grow(deltaPages uint32)` must eventually become `uint64`. This cannot be done without a `v2` release or a parallel interface.
*   **Internal API (`internal/wasm.ModuleEngine`):** This is the heavily coupled boundary between the structure and the JIT. It is not an `Unsafe API` in the Go sense, but it is architecturally unsafe to modify without understanding both the interpreter and `wazevo`.
*   **Unsafe API Usage (`unsafe.Pointer`):** Used extensively in `abi_go_call.go` and `internal/platform` to cast OS-allocated byte arrays into executable functions, and to manipulate the `executionContext`. This is necessary but requires perfect discipline regarding Go's `runtime.KeepAlive` to prevent the GC from reclaiming the `executionContext` while Wasm is executing.

## A6. Deep Go Runtime Interactions

The `wazevo` JIT does not just generate assembly; it generates assembly that *coexists* with the Go runtime.
*   **Escape Analysis:** `wazero` deliberately defeats Go's escape analysis on the hot path. The `executionContext` and `ModuleInstance` escape to the heap. To mitigate this during compilation, `wazevoapi.Pool` is used. Objects in the pool are pre-heap-allocated, meaning the compiler's temporary SSA nodes do not escape individually, preventing GC thrashing.
*   **Stack Growth & Stack Maps:** When JIT-compiled Wasm code calls back into Go (via `abi_go_call.go`), it *must* switch to the Go goroutine stack. Why? Because Go functions assume they can trigger a stack growth (`runtime.morestack`). If Go code ran on the Wasm stack, a stack growth would panic because the Wasm stack lacks Go stack maps (metadata telling the GC where pointers live). The trampoline meticulously manages this `RSP` swap.
*   **Pointer Liveness & GC Barriers:** The memory buffer (`m.Buffer`) allocated via `mmap` is outside the Go heap. Therefore, no Go write barriers are required when Wasm writes to its own memory. However, pointers passed *into* host functions must be treated carefully; if a Go host function retains a pointer into the Wasm memory, it must be aware that the memory could be unmapped or grown (invalidating the address if the OS moves the mapping, though `wazero` currently re-slices rather than moving the base pointer for `mmap`).

## A7. Applied Security Vectors

| Potential Attack Vector | Reachability | Exploitability | Preconditions | Impact |
|---|---|---|---|---|
| **JIT Spraying (W^X Bypass)** | Medium. Malicious Wasm could encode specific immediates that form ROP gadgets. | Low. `wazero` enforces strict W^X (`PROT_EXEC` without `PROT_WRITE`). | Attacker must find a separate vulnerability to hijack the instruction pointer (RIP). | High (Arbitrary Code Execution). Mitigation: Implement Constant Blinding. |
| **Off-by-One in Explicit Bounds Check** | Low. Logic in `lower.go` is heavily fuzzed. | High *if* a bug exists. | Attacker must trigger the exact boundary condition failing the check. | High (Out of bounds read/write into host memory). |
| **File Cache Tampering** | Medium. Requires local disk access. | Critical. Cached files are `mmap`'d directly as `PROT_EXEC`. | Attacker must have write access to the configured cache directory. | Critical (Complete Host Takeover). Mitigation: Rely on OS ACLs. |
| **Compilation OOM (Resource Exhaustion)** | High. Send a highly nested Wasm binary. | Low (Crash only). | The host must allow untrusted Wasm binaries without size limits. | Medium (Denial of Service). |

## A8. The Grand Cross-Correlation: The Story of Memory64

To understand the sheer magnitude of supporting Memory64 in `wazero`, we must trace the dependency chain from top to bottom. It is not a feature toggle; it is a structural fault line.

**1. The Specification (Memory64):** Wasm allows linear memory to exceed 4GB. Addresses are 64-bit.
   **↓ cascades to ↓**
**2. The Host API (`api.Memory`):** The Go developer calls `module.Memory().Grow(delta)`. Currently, `delta` is `uint32`. If the developer wants to grow by 5GB, the API fundamentally prevents it. We must create `api.Memory64`.
   **↓ cascades to ↓**
**3. Instantiation (`internal/wasm.NewMemoryInstance`):** The runtime reads the `Memory64` flag. It must allocate >4GB. The OS layer (`platform.AllocateMemoryRegion`) handles this fine (OSes are 64-bit), but the internal struct must hold `Max uint64`.
   **↓ cascades to ↓**
**4. JIT Frontend (`internal/engine/wazevo/frontend/lower.go`):** When the compiler encounters an `i64.load`, it generates SSA. Currently, the `OpcodeLoad` instruction implicitly expects offset math to be 32-bit. The frontend explicitly truncates pages: `AsIreduce(pages, ssa.TypeI32)`. This must be removed, and `OpcodeLoad` must become `OpcodeLoad64`.
   **↓ cascades to ↓**
**5. SSA Graph (`internal/engine/wazevo/ssa`):** The newly emitted `OpcodeLoad64` requires the data flow graph, liveness analysis, and dead code elimination passes to correctly trace 64-bit integer values without casting them down.
   **↓ cascades to ↓**
**6. Register Allocator (`backend/regalloc`):** The allocator assigns a 64-bit physical register (e.g., RAX). This is mostly fine, as `wazevo` handles 64-bit values for `i64` math already.
   **↓ cascades to ↓**
**7. ISA Lowering & Address Mode (`backend/isa/amd64/lower_mem.go`):** **This is the breaking point.** The code calls `lowerToAddressMode(ptr, offset uint32)`. An `i64.load offset=8000000000` is encountered. The `uint32` argument truncates it. Even if we fix the signature to `uint64`, x86_64 assembly `MOV [RAX + offset], RCX` *only supports a 32-bit signed immediate offset*. The ISA lowering must detect the >32-bit offset, allocate a *new* temporary register (e.g., `R8`), emit `MOV R8, 8000000000`, and then emit `MOV [RAX + R8], RCX`.
   **↓ cascades to ↓**
**8. ABI & Execution Context (`abi_go_call.go`):** If a Memory64 module traps or calls `memory.grow`, the JIT must jump to Go. The trampoline must pass the 64-bit address or 64-bit delta properly. The `executionContext` must store `memoryLen` as an 8-byte field, shifting every subsequent offset in `wazevoapi/offsetdata.go`.
   **↓ cascades to ↓**
**9. File Cache (`internal/filecache`):** If the compiled module is cached, the serialized machine code must indicate it was compiled with 64-bit addressing modes, so a runtime missing the `CoreFeatureMemory64` flag doesn't accidentally load and execute it, causing chaotic memory access.
   **↓ cascades to ↓**
**10. Tests (`integration_test`):** The fuzzers must be updated to aggressively generate 64-bit offsets to ensure the new AGU logic in Step 7 doesn't accidentally clobber registers or generate unaligned access faults on ARM64.

### The Engineering Conclusion on Memory64
Memory64 is not a feature addition; it is a vascular rewrite of the runtime. The coupling is so absolute that modifying Step 7 (Address Mode) without modifying Step 4 (SSA) is impossible. This cross-correlation demonstrates why `wazero`'s pure-Go custom JIT is a double-edged sword: extreme performance and portability purchased with intense, architecture-specific maintenance constraints.
