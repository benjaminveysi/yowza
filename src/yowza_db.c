#include "yowza_db.h"

// Custom string length for zero dependencies
static size_t string_length(const char* str) {
    size_t len = 0;
    while (str[len] != '\0') len++;
    return len;
}

int yowza_db_init(YowzaDB* db, void* memory_buffer, size_t memory_size, StorageDriver storage) {
    if (!db || !memory_buffer) return -1;

    arena_init(&db->arena, memory_buffer, memory_size);
    art_init(&db->index, &db->arena);
    tx_init_state(&db->tx);

    db->storage = storage;

    return 0;
}

// We reuse the VM logic internally to guarantee that Native API calls and Wasm API calls
// follow the exact same WAL and memory rules.
static int execute_op(YowzaDB* db, uint8_t* bytecode, size_t length, VMContext* vm_out) {
    vm_init(vm_out, &db->index, bytecode, length, &db->storage);
    int res = vm_execute(vm_out);
    /* Phase 2, Track C: deliberately NO second sync here. Durability
     * is inside vm_execute, not layered on top of it: a recorded
     * legacy op appends its WAL record and syncs before vm_execute
     * returns success (and undoes itself on a sync failure), and
     * tx_commit syncs its frame. The old code called sync again here
     * and IGNORED its result — that was the bug: a sync failure was
     * silently swallowed, and the extra sync could not undo anything
     * vm_execute had already reported as committed. If vm_execute
     * ever reports success for a write without having synced, that
     * is a bug in vm.c, not something this layer papers over — the
     * Track C tests pin this with a failing-sync driver (sync
     * failures propagate as -6 from vm_execute itself, and a
     * successful op performs exactly one sync, not two). Reads and
     * scans need no sync at all. */
    return res;
}

static void write_u32(uint8_t* buf, size_t* offset, uint32_t val) {
    buf[(*offset)++] = val & 0xFF;
    buf[(*offset)++] = (val >> 8) & 0xFF;
    buf[(*offset)++] = (val >> 16) & 0xFF;
    buf[(*offset)++] = (val >> 24) & 0xFF;
}

int yowza_db_put_raw(YowzaDB* db, const void* key, size_t key_len, const void* val, size_t val_len) {
    if (!db || !key || key_len == 0) return -1;
    /* Phase 1 guard: a live mutation interleaved with a buffered
     * transaction would break the abort invariant (see yowza_db.h).
     * Use yowza_db_tx_put inside a transaction instead. */
    if (db->tx.active) return -7;

    /* Bounds check to prevent stack buffer overflow. Step 1: bound each
     * length first, then sum -- summing unbounded size_t values first
     * could wrap around and pass the check (SIZE_MAX - 5 + ...). */
    if (key_len > 1024 || val_len > 1024) return -1;
    if (1 + 4 + key_len + 4 + val_len + 1 > 1024) return -1;

    // Construct bytecode for OP_PUT
    uint8_t bytecode[1024];
    size_t pc = 0;
    const uint8_t* k = (const uint8_t*)key;

    bytecode[pc++] = OP_PUT;
    write_u32(bytecode, &pc, (uint32_t)key_len);
    for (size_t i = 0; i < key_len; i++) bytecode[pc++] = k[i];
    write_u32(bytecode, &pc, (uint32_t)val_len);
    if (val) {
        const uint8_t* v = (const uint8_t*)val;
        for (size_t i = 0; i < val_len; i++) bytecode[pc++] = v[i];
    }
    bytecode[pc++] = OP_EXIT;

    VMContext vm;
    return execute_op(db, bytecode, pc, &vm);
}

int yowza_db_put(YowzaDB* db, const char* key, const void* val, size_t val_len) {
    if (!db || !key) return -1;
    return yowza_db_put_raw(db, key, string_length(key), val, val_len);
}

int yowza_db_get_raw(YowzaDB* db, const void* key, size_t key_len, void** out_val, size_t* out_val_len) {
    if (!db || !key || key_len == 0 || !out_val || !out_val_len) return -1;

    // Bounds check to prevent stack buffer overflow (bound first, then sum)
    if (key_len > 1024) return -1;
    if (1 + 4 + key_len + 1 > 1024) return -1;

    uint8_t bytecode[1024];
    size_t pc = 0;
    const uint8_t* k = (const uint8_t*)key;

    bytecode[pc++] = OP_GET;
    write_u32(bytecode, &pc, (uint32_t)key_len);
    for (size_t i = 0; i < key_len; i++) bytecode[pc++] = k[i];
    bytecode[pc++] = OP_EXIT;

    VMContext vm;
    int res = execute_op(db, bytecode, pc, &vm);

    /* Step 1: found-ness is response_ptr != NULL, not length > 0. A
     * key with an empty value (val_len == 0) used to be reported as
     * "not found" because its length is 0. */
    if (res == 0 && vm.response_ptr != NULL) {
        *out_val = vm.response_ptr;
        *out_val_len = vm.response_len;
        return 0;
    }

    return -1; // Not found
}

int yowza_db_get(YowzaDB* db, const char* key, void** out_val, size_t* out_val_len) {
    if (!db || !key) return -1;
    return yowza_db_get_raw(db, key, string_length(key), out_val, out_val_len);
}

int yowza_db_delete_raw(YowzaDB* db, const void* key, size_t key_len) {
    if (!db || !key || key_len == 0) return -1;
    if (db->tx.active) return -7; /* same guard as yowza_db_put_raw */

    // Bounds check to prevent stack buffer overflow (bound first, then sum)
    if (key_len > 1024) return -1;
    if (1 + 4 + key_len + 1 > 1024) return -1;

    uint8_t bytecode[1024];
    size_t pc = 0;
    const uint8_t* k = (const uint8_t*)key;

    bytecode[pc++] = OP_DEL;
    write_u32(bytecode, &pc, (uint32_t)key_len);
    for (size_t i = 0; i < key_len; i++) bytecode[pc++] = k[i];
    bytecode[pc++] = OP_EXIT;

    VMContext vm;
    return execute_op(db, bytecode, pc, &vm);
}

int yowza_db_delete(YowzaDB* db, const char* key) {
    if (!db || !key) return -1;
    return yowza_db_delete_raw(db, key, string_length(key));
}

int yowza_db_scan(YowzaDB* db, const char* prefix, void* out_buffer, size_t out_buffer_size, YowzaScanResult* result) {
    if (!db || !prefix || !out_buffer || !result) return -1;

    size_t prefix_len = string_length(prefix);

    // Bounds check to prevent stack buffer overflow (bound first, then sum)
    if (prefix_len > 1024) return -1;
    if (1 + 4 + prefix_len + 4 + 1 > 1024) return -1;
    if (out_buffer_size > 0xFFFFFFFFULL) return -1;

    uint8_t bytecode[1024];
    size_t pc = 0;

    bytecode[pc++] = OP_SCAN;
    write_u32(bytecode, &pc, (uint32_t)prefix_len);
    for (size_t i = 0; i < prefix_len; i++) bytecode[pc++] = prefix[i];
    write_u32(bytecode, &pc, (uint32_t)out_buffer_size);
    bytecode[pc++] = OP_EXIT;

    VMContext vm;
    int res = execute_op(db, bytecode, pc, &vm);

    if (res == 0) {
        // Because OP_SCAN in VM allocated from Arena temporarily, we need to copy it
        // to the user's out_buffer to provide a clean C API.
        result->total_bytes = vm.response_len;
        result->buffer = (uint8_t*)out_buffer;

        // Count entries, with bounds checks: a malformed response
        // must not make this loop read past response_len.
        uint32_t count = 0;
        uint32_t offset = 0;
        while (offset < vm.response_len) {
            if (offset + 4 > vm.response_len) return -1;
            uint32_t kl = (uint32_t)vm.response_ptr[offset] | ((uint32_t)vm.response_ptr[offset+1] << 8) | ((uint32_t)vm.response_ptr[offset+2] << 16) | ((uint32_t)vm.response_ptr[offset+3] << 24);
            offset += 4;
            if (kl > vm.response_len - offset) return -1;
            offset += kl;
            if (offset + 4 > vm.response_len) return -1;
            uint32_t vl = (uint32_t)vm.response_ptr[offset] | ((uint32_t)vm.response_ptr[offset+1] << 8) | ((uint32_t)vm.response_ptr[offset+2] << 16) | ((uint32_t)vm.response_ptr[offset+3] << 24);
            offset += 4;
            if (vl > vm.response_len - offset) return -1;
            offset += vl;
            count++;
        }
        result->count = count;

        for(size_t i = 0; i < vm.response_len; i++) {
            ((uint8_t*)out_buffer)[i] = vm.response_ptr[i];
        }

        return 0;
    }
    return -1;
}

int yowza_db_snapshot_create(YowzaDB* db, void* out_buffer, size_t out_buffer_size) {
    if (!db || !out_buffer) return -1;
    return art_serialize(&db->index, (uint8_t*)out_buffer, out_buffer_size);
}

int yowza_db_snapshot_restore(YowzaDB* db, const void* in_buffer, size_t in_buffer_size) {
    if (!db || !in_buffer) return -1;
    if (db->tx.active) return -1; /* never reset under an open transaction */
    if (in_buffer_size > 0xFFFFFFFFULL) return -1;
    /* Validate first, then reset: otherwise restoring over a live DB
     * pollutes it with stale keys, and a corrupt snapshot half-fills it. */
    if (art_validate_snapshot((const uint8_t*)in_buffer, (uint32_t)in_buffer_size) != 0) return -1;
    /* Pre-reset fit guard (same as yowza_snapshot_restore in
     * src/yowza.c): a snapshot whose values alone exceed the arena
     * cannot fit, so reject it BEFORE arena_reset wipes live data. */
    {
        uint64_t value_bytes = 0;
        if (art_snapshot_value_bytes((const uint8_t*)in_buffer, (uint32_t)in_buffer_size, &value_bytes) != 0 ||
            value_bytes > (uint64_t)db->arena.size) {
            return -1;
        }
    }
    arena_reset(&db->arena);
    art_init(&db->index, &db->arena);
    return art_deserialize(&db->index, (const uint8_t*)in_buffer, (uint32_t)in_buffer_size);
}

int yowza_db_compact(YowzaDB* db, void* temp_buffer, size_t temp_size) {
    if (!db || !temp_buffer) return -1;
    if (db->tx.active) return -1; /* never reset under an open transaction */
    if (temp_size > 0xFFFFFFFFULL) return -1;

    /* Temp buffer must not overlap the arena, otherwise serializing
     * into it overwrites the tree being read (previously a segfault). */
    const uint8_t* arena_start = db->arena.buffer;
    const uint8_t* arena_end = db->arena.buffer ? db->arena.buffer + db->arena.size : NULL;
    const uint8_t* temp_start = (const uint8_t*)temp_buffer;
    const uint8_t* temp_end = temp_start + temp_size;
    if (arena_start && temp_start < arena_end && arena_start < temp_end) return -1;

    // To perform compaction safely without requiring malloc:
    // 1. Serialize the entire valid ART state into the temporary buffer.
    // 2. Clear the current Arena and free_list completely.
    // 3. Re-deserialize from the temporary buffer.

    // Step 1: return -2 when the temp buffer is too small (before
    // arena_reset), matching yowza_compact's contract in src/yowza.c.
    // A serialize failure after validation is still -1, and overlap
    // (above) is still -1; only the pre-reset size failure is -2.
    if (temp_size < 20) return -2; /* not even header + checksum fit */
    int serialized_len = art_serialize(&db->index, (uint8_t*)temp_buffer, temp_size);
    if (serialized_len < 0) {
        return -2; // Temp buffer too small to hold the live data
    }
    if (art_validate_snapshot((uint8_t*)temp_buffer, (uint32_t)serialized_len) != 0) return -1;

    // Step 2
    arena_reset(&db->arena);
    art_init(&db->index, &db->arena); // Reinitialize roots and clear free-list

    // Step 3 (validated above, so failure here is essentially OOM;
    // temp_buffer still holds the last good snapshot for a manual restore)
    return art_deserialize(&db->index, (const uint8_t*)temp_buffer, (uint32_t)serialized_len);
}

/* Phase 1 persistent transaction API: thin wrappers over tx_*.
 * Bounds follow the same 1024-byte bytecode-era limits as the legacy
 * raw API for keys/values (the VM program path has the same limits
 * through its bytecode buffer); the write-set cap (128 ops) is in tx.h. */
int yowza_db_tx_begin(YowzaDB* db, uint64_t tx_id, uint64_t origin_node_id,
                      uint32_t consistency_model, uint64_t schema_version) {
    if (!db) return -1;
    return tx_begin(&db->tx, &db->index, tx_id, origin_node_id, consistency_model, schema_version);
}

int yowza_db_tx_put_raw(YowzaDB* db, const void* key, size_t key_len, const void* val, size_t val_len, uint8_t flags) {
    if (!db || !key || key_len == 0 || key_len > 1024 || val_len > 1024) return -1;
    return tx_put(&db->tx, &db->index, (const uint8_t*)key, (uint32_t)key_len,
                  (const uint8_t*)val, (uint32_t)val_len, flags, 0);
}

int yowza_db_tx_put(YowzaDB* db, const char* key, const void* val, size_t val_len, uint8_t flags) {
    if (!db || !key) return -1;
    return yowza_db_tx_put_raw(db, key, string_length(key), val, val_len, flags);
}

int yowza_db_tx_delete_raw(YowzaDB* db, const void* key, size_t key_len) {
    if (!db || !key || key_len == 0 || key_len > 1024) return -1;
    return tx_del(&db->tx, &db->index, (const uint8_t*)key, (uint32_t)key_len);
}

int yowza_db_tx_delete(YowzaDB* db, const char* key) {
    if (!db || !key) return -1;
    return yowza_db_tx_delete_raw(db, key, string_length(key));
}

int yowza_db_tx_get_raw(YowzaDB* db, const void* key, size_t key_len, void** out_val, size_t* out_val_len) {
    if (!db || !key || key_len == 0 || !out_val || !out_val_len) return -1;
    uint8_t* v = NULL;
    uint32_t vl = 0;
    int rc = tx_get(&db->tx, &db->index, (const uint8_t*)key, (uint32_t)key_len, &v, &vl);
    if (rc == 0) {
        *out_val = v;
        *out_val_len = vl;
        return 0;
    }
    return -1; /* not found (-1) or tx error: caller checks tx_active / return via commit */
}

int yowza_db_tx_get(YowzaDB* db, const char* key, void** out_val, size_t* out_val_len) {
    if (!db || !key) return -1;
    return yowza_db_tx_get_raw(db, key, string_length(key), out_val, out_val_len);
}

int yowza_db_tx_assert_raw(YowzaDB* db, uint8_t kind, const void* key, size_t key_len, const void* val, size_t val_len) {
    if (!db || !key || key_len == 0 || key_len > 1024 || val_len > 1024) return -1;
    return tx_assert(&db->tx, &db->index, kind, (const uint8_t*)key, (uint32_t)key_len,
                     (const uint8_t*)val, (uint32_t)val_len);
}

int yowza_db_tx_commit(YowzaDB* db) {
    if (!db) return -1;
    return tx_commit(&db->tx, &db->index, &db->storage);
}

int yowza_db_tx_abort(YowzaDB* db) {
    if (!db) return -1;
    return tx_abort(&db->tx, &db->index);
}

int yowza_db_tx_active(YowzaDB* db) {
    if (!db) return 0;
    return db->tx.active;
}
