#include "vm.h"

/* Wasm export marker: under a wasm target this is the
 * export_name attribute (identical to the previous
 * spelling, so the wasm export list is unchanged);
 * natively it expands to nothing, so this file can be
 * compiled into the native test runner without a flood
 * of unknown-attribute warnings. */
#ifdef __wasm__
#define YOWZA_EXPORT(name) __attribute__((export_name(name)))
#else
#define YOWZA_EXPORT(name)
#endif

// Persistent state in WebAssembly linear memory
static Arena global_arena;
static ART global_art;
static StorageDriver global_storage;
static uint8_t is_initialized = 0;

/* Axis C — sticky timeline handle for the C VM timeline
 * mode (Phase D §D3b's missing glue, now present).
 * vm_init clears VMContext.timeline on every call — that
 * native contract is deliberately UNCHANGED, so the
 * non-timeline path stays byte-identical and native
 * callers keep setting the timeline after vm_init with
 * vm_set_timeline. At the yowza.c layer, the handle
 * lives here instead: yowza_timeline_set stores an
 * EXPLICIT snapshot sequence, and yowza_execute
 * re-applies it after vm_init, so the handle survives
 * init across programs until yowza_timeline_clear.
 * The snapshot is explicit, never "current": a set
 * sequence above the ART's current commit sequence is
 * rejected (a future snapshot must never be read as
 * the current head), and a timeline program that
 * touches a post-snapshot key fails with -11 in the
 * VM's layered read rather than returning the live
 * value. Snapshot 0 is the defined empty base (see
 * vm.h), not "current". yowza_execute_timeline is the
 * one-shot form: it takes the snapshot as an argument
 * for a single program and does not change the sticky
 * handle. */
static int timeline_pending_active = 0;
static uint64_t timeline_pending_seq = 0;

/* Validate an explicit snapshot against the ART clock.
 * Returns 0 when the sequence may be used as a timeline
 * snapshot, -1 for a future sequence (non-zero and above
 * the current commit sequence). */
static int timeline_snapshot_valid(uint64_t snapshot_seq) {
    if (snapshot_seq == 0) return 0; /* empty base, defined */
    return snapshot_seq <= global_art.commit_seq ? 0 : -1;
}

YOWZA_EXPORT("yowza_timeline_set")
int yowza_timeline_set(uint64_t snapshot_seq) {
    if (!is_initialized) return -1;
    if (timeline_snapshot_valid(snapshot_seq) != 0) return -1;
    timeline_pending_active = 1;
    timeline_pending_seq = snapshot_seq;
    return 0;
}

YOWZA_EXPORT("yowza_timeline_clear")
int yowza_timeline_clear(void) {
    timeline_pending_active = 0;
    timeline_pending_seq = 0;
    return 0;
}

YOWZA_EXPORT("yowza_timeline_active")
int yowza_timeline_active(void) {
    return timeline_pending_active;
}

YOWZA_EXPORT("yowza_timeline_snapshot")
uint64_t yowza_timeline_snapshot(void) {
    return timeline_pending_seq;
}

// Export an init function so Go can allocate the memory once and pass the pointer
YOWZA_EXPORT("yowza_init")
int yowza_init(uint8_t* memory_buffer, size_t memory_size, uint8_t* wal_buffer, uint32_t wal_capacity) {
    if (is_initialized) return 0; // Already initialized

    if (memory_buffer == NULL) return -1;

    arena_init(&global_arena, memory_buffer, memory_size);
    art_init(&global_art, &global_arena);

    global_storage = storage_memory_create(wal_buffer, wal_capacity);

    is_initialized = 1;
    return 0;
}

// Export WAL length to Go
YOWZA_EXPORT("yowza_get_wal_len")
uint32_t yowza_get_wal_len() {
    return storage_memory_get_offset(&global_storage);
}

// Clear the WAL after Go has read it
YOWZA_EXPORT("yowza_clear_wal")
void yowza_clear_wal() {
    storage_memory_clear(&global_storage);
}

// Export snapshot serialization
YOWZA_EXPORT("yowza_snapshot_create")
int yowza_snapshot_create(uint8_t* out_buffer, uint32_t out_buffer_size) {
    if (!is_initialized) return -1;
    return art_serialize(&global_art, out_buffer, out_buffer_size);
}

// Export prefix scan directly into a caller-supplied linear-memory
// buffer, so the Go host can grow the output buffer beyond the VM's
// static 64 KiB scan buffer without touching the Arena. This does NOT
// replace OP_SCAN inside the VM (that path is untouched); it is a
// separate entry point with the same semantics as art_scan:
// returns bytes written (>= 0, 0 when the prefix matches nothing), or
// -1 on error, including when out_buffer is too small (the host is
// expected to grow the buffer and retry, as with snapshot creation).
YOWZA_EXPORT("yowza_scan")
int yowza_scan(const uint8_t* prefix, uint32_t prefix_len, uint8_t* out_buffer, uint32_t out_buffer_size) {
    if (!is_initialized) return -1;
    return art_scan(&global_art, prefix, prefix_len, out_buffer, out_buffer_size);
}

// Overlap test for compaction: temp_buffer must not overlap the arena
// buffer, otherwise serializing into it overwrites the tree being read
// (previously this segfaulted). Returns 1 if the ranges overlap.
static int ranges_overlap(const uint8_t* a, size_t a_len, const uint8_t* b, size_t b_len) {
    if (!a || !b || a_len == 0 || b_len == 0) return 0;
    return a < b + b_len && b < a + a_len;
}

// Export snapshot deserialization (for cold starts)
YOWZA_EXPORT("yowza_snapshot_restore")
int yowza_snapshot_restore(const uint8_t* in_buffer, uint32_t in_buffer_size) {
    if (!is_initialized) return -1;
    /* Step 1: validate first. The old code deserialized straight into
     * the live tree, so restoring over a non-empty DB left stale keys
     * behind (state pollution) and a corrupt snapshot could half-fill it. */
    if (art_validate_snapshot(in_buffer, in_buffer_size) != 0) return -1;
    /* Step 1b: pre-reset fit guard. If the snapshot's values alone
     * exceed the arena, deserialize definitely cannot fit, so reject
     * BEFORE arena_reset — otherwise a valid-but-oversized external
     * snapshot (e.g. from a DB with a larger arena) would wipe the
     * live data and then fail. This is a necessary, not sufficient,
     * condition (node overhead is not counted); see
     * art_snapshot_value_bytes. */
    {
        uint64_t value_bytes = 0;
        if (art_snapshot_value_bytes(in_buffer, in_buffer_size, &value_bytes) != 0 ||
            value_bytes > (uint64_t)global_arena.size) {
            return -1;
        }
    }
    /* Step 2: reset, so the restored DB is exactly the snapshot. */
    arena_reset(&global_arena);
    art_init(&global_art, &global_arena);
    /* Step 3: a validated, value-fitting snapshot should only fail
     * here on node-overhead OOM in pathological cases, in which case
     * the DB is (documented) partially restored. Snapshots produced
     * by art_serialize from an arena of this size always fit. */
    return art_deserialize(&global_art, in_buffer, in_buffer_size);
}

// Export database compaction to reclaim leaked value strings
YOWZA_EXPORT("yowza_compact")
int yowza_compact(uint8_t* temp_buffer, uint32_t temp_size) {
    if (!is_initialized) return -1;

    if (ranges_overlap(temp_buffer, temp_size, global_arena.buffer, global_arena.size)) {
        return -1; /* caller must supply a non-overlapping temp buffer */
    }

    // Step 1: Serialize (before touching the Arena, so a failure here
    // loses nothing). Return -2 when the temp buffer is too small, so
    // the Go host (compactLocked) can grow memOffsetSnapCap and retry
    // safely. Distinguishing the cause: art_serialize returns -1 for
    // a buffer smaller than 20 bytes, for buffer exhaustion during
    // traversal, and for a traversal key-length failure. The last case
    // cannot occur for a tree built through art_insert, which rejects
    // keys longer than YOWZA_MAX_KEY_LEN (1023) fail-fast, so for any
    // valid tree the only remaining step-1 failure is buffer size.
    // Retrying with a larger buffer is safe either way: if the cause
    // were not size, the retry fails again with -2 (before any reset)
    // instead of corrupting data, and the host gives up when it can no
    // longer grow memory.
    if (temp_size < 20) return -2; /* not even header + checksum fit */
    int serialized_len = art_serialize(&global_art, temp_buffer, temp_size);
    if (serialized_len < 0) return -2;
    if (art_validate_snapshot(temp_buffer, (uint32_t)serialized_len) != 0) return -1;

    // Step 2: Reset Arena
    arena_reset(&global_arena);
    art_init(&global_art, &global_arena);

    // Step 3: Restore. The serialized buffer was validated above, so a
    // failure here is essentially OOM; temp_buffer still holds the last
    // good snapshot and the caller can restore from it.
    return art_deserialize(&global_art, temp_buffer, (uint32_t)serialized_len);
}

// Helper to retrieve the response length after execution
YOWZA_EXPORT("yowza_get_response_len")
uint32_t yowza_get_response_len(VMContext* vm) {
    if (!vm) return 0;
    return vm->response_len;
}

// Helper to retrieve the response pointer
YOWZA_EXPORT("yowza_get_response_ptr")
uint8_t* yowza_get_response_ptr(VMContext* vm) {
    if (!vm) return NULL;
    return vm->response_ptr;
}

// Expose the main execution function to WebAssembly
// This is the Zero-Copy entry point called by the Go Host.
YOWZA_EXPORT("yowza_execute")
VMContext* yowza_execute(uint8_t* bytecode, size_t length) {
    if (!is_initialized) return (VMContext*)-1;
    if (bytecode == NULL) return (VMContext*)-2;

    // We can allocate the VMContext on the arena for safe returning to host,
    // or just use a static context since Wasm is single threaded by default.
    // Let's use a static context to avoid eating arena memory per request.
    // DOCUMENTED LIMIT (step 1): this static context is NOT thread-safe,
    // and neither is the Go host's shared bytecode slot (memOffsetCode).
    // Wasm itself is single-threaded, so this is safe only if the host
    // serializes calls. Step 3 fixes the host side (exclusive Lock for
    // Get/Scan/Close, or a per-call slot); do not call yowza_execute
    // concurrently from the host until then.
    static VMContext vm;

    vm_init(&vm, &global_art, bytecode, length, &global_storage);

    /* Axis C: re-apply the sticky timeline handle AFTER
     * vm_init (which clears it). Without this line the
     * handle would be silently dropped on every call and
     * a timeline program would run against the live head
     * — exactly the silent fallback this glue exists to
     * prevent. With no handle set, this is the plain
     * non-timeline path, unchanged. */
    if (timeline_pending_active) {
        vm_set_timeline(&vm, timeline_pending_seq);
    }

    // Run the bytecode interpreter
    int result = vm_execute(&vm);

    if (result != 0) {
        // We'll return the error code casted to pointer for debugging
        return (VMContext*)(size_t)result;
    }

    return &vm;
}

// One-shot timeline execution: run one program under an
// explicit snapshot without changing the sticky handle.
// The snapshot is validated the same way (future rejected).
// Returns the VMContext on success, or (VMContext*)-1 / -2 /
// error code cast to pointer, mirroring yowza_execute.
YOWZA_EXPORT("yowza_execute_timeline")
VMContext* yowza_execute_timeline(uint8_t* bytecode, size_t length, uint64_t snapshot_seq) {
    if (!is_initialized) return (VMContext*)-1;
    if (bytecode == NULL) return (VMContext*)-2;
    if (timeline_snapshot_valid(snapshot_seq) != 0) return (VMContext*)-3;

    static VMContext vm;

    vm_init(&vm, &global_art, bytecode, length, &global_storage);
    vm_set_timeline(&vm, snapshot_seq);

    int result = vm_execute(&vm);

    if (result != 0) {
        return (VMContext*)(size_t)result;
    }

    return &vm;
}

// Drain one staged timeline write into a caller buffer, so
// a wasm host can apply the program's staged writes through
// the unified layer (Go TimelineHandle / SegDB transaction)
// without pointer juggling across the wasm boundary.
// Format: [deleted u8][key_len u32 LE][key][val_len u32 LE][val]
// (val_len 0 and no bytes for a delete). Returns bytes
// written, or -1 on a bad argument, an out-of-range index,
// or a buffer too small.
YOWZA_EXPORT("yowza_timeline_write_serialize")
int yowza_timeline_write_serialize(VMContext* vm, uint32_t idx, uint8_t* out, uint32_t out_cap) {
    const uint8_t* key = NULL;
    uint32_t key_len = 0;
    const uint8_t* val = NULL;
    uint32_t val_len = 0;
    int deleted = 0;
    uint32_t off = 0, i;
    if (!vm || !out) return -1;
    if (vm_timeline_write_at(vm, idx, &key, &key_len, &val, &val_len, &deleted) != 0) return -1;
    if ((uint64_t)1 + 4 + key_len + 4 + val_len > out_cap) return -1;
    out[off++] = deleted ? 1 : 0;
    out[off++] = (uint8_t)(key_len & 0xFF);
    out[off++] = (uint8_t)((key_len >> 8) & 0xFF);
    out[off++] = (uint8_t)((key_len >> 16) & 0xFF);
    out[off++] = (uint8_t)((key_len >> 24) & 0xFF);
    for (i = 0; i < key_len; i++) out[off++] = key[i];
    out[off++] = (uint8_t)(val_len & 0xFF);
    out[off++] = (uint8_t)((val_len >> 8) & 0xFF);
    out[off++] = (uint8_t)((val_len >> 16) & 0xFF);
    out[off++] = (uint8_t)((val_len >> 24) & 0xFF);
    for (i = 0; i < val_len; i++) out[off++] = val[i];
    return (int)off;
}

YOWZA_EXPORT("yowza_timeline_write_count")
int yowza_timeline_write_count(VMContext* vm) {
    if (!vm) return -1;
    return vm_timeline_write_count(vm);
}
