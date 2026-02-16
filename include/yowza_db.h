#ifndef YOWZA_DB_H
#define YOWZA_DB_H

#include "vm.h"

// High-level native C API for developers who want to embed Yowza!
// natively (like SQLite) without dealing with WebAssembly or Go.

typedef struct {
    Arena arena;
    ART index;
    StorageDriver storage;
    /* Phase 1 persistent transaction state (native C API only).
     * The Wasm path keeps its TxState in VMContext per program
     * instead; both use the same tx_* functions. While this is
     * active, legacy yowza_db_put/delete return -7: a live mutation
     * interleaved with a buffered transaction would break the abort
     * invariant (see tx.h). */
    TxState tx;
} YowzaDB;

// Result structure for Scan operations
typedef struct {
    uint32_t count;
    uint32_t total_bytes;
    uint8_t* buffer;
} YowzaScanResult;

// Initialize a new Native Database instance with a specific storage driver
// Returns 0 on success, -1 on failure
int yowza_db_init(YowzaDB* db, void* memory_buffer, size_t memory_size, StorageDriver storage);

// Insert or update a Key-Value pair
int yowza_db_put(YowzaDB* db, const char* key, const void* val, size_t val_len);
// Length-based variant: the key may contain '\0' bytes. The plain
// yowza_db_put/get/delete are thin wrappers over these, using
// string_length(key) for the key length.
int yowza_db_put_raw(YowzaDB* db, const void* key, size_t key_len, const void* val, size_t val_len);

// Retrieve a Value by Key
// If found, populates out_val and out_val_len and returns 0. Else returns -1.
int yowza_db_get(YowzaDB* db, const char* key, void** out_val, size_t* out_val_len);
int yowza_db_get_raw(YowzaDB* db, const void* key, size_t key_len, void** out_val, size_t* out_val_len);

// Delete a Key-Value pair
int yowza_db_delete(YowzaDB* db, const char* key);
int yowza_db_delete_raw(YowzaDB* db, const void* key, size_t key_len);

// Scan for keys matching a prefix
// The out_buffer must be provided by the user.
int yowza_db_scan(YowzaDB* db, const char* prefix, void* out_buffer, size_t out_buffer_size, YowzaScanResult* result);

// Serialize the database into a snapshot buffer
int yowza_db_snapshot_create(YowzaDB* db, void* out_buffer, size_t out_buffer_size);

// Restore the database from a snapshot buffer
int yowza_db_snapshot_restore(YowzaDB* db, const void* in_buffer, size_t in_buffer_size);

// Compact the database arena to reclaim memory from deleted or updated values.
// Requires a temporary secondary memory buffer.
int yowza_db_compact(YowzaDB* db, void* temp_buffer, size_t temp_size);

// Phase 1 persistent transactions (native C API). A transaction may
// span calls: begin, any number of tx_put/tx_del/tx_get/tx_assert,
// then exactly one of tx_commit / tx_abort. tx_get reads the
// transaction's own view (private writes first, then committed
// state) and records the read for validation; plain yowza_db_get
// always reads committed state only. Returns tx.h error codes
// (0, -3, -6, -7, -8, -9) or -1 for bad arguments.
int yowza_db_tx_begin(YowzaDB* db, uint64_t tx_id, uint64_t origin_node_id,
                      uint32_t consistency_model, uint64_t schema_version);
int yowza_db_tx_put_raw(YowzaDB* db, const void* key, size_t key_len, const void* val, size_t val_len, uint8_t flags);
int yowza_db_tx_put(YowzaDB* db, const char* key, const void* val, size_t val_len, uint8_t flags);
int yowza_db_tx_delete_raw(YowzaDB* db, const void* key, size_t key_len);
int yowza_db_tx_delete(YowzaDB* db, const char* key);
int yowza_db_tx_get_raw(YowzaDB* db, const void* key, size_t key_len, void** out_val, size_t* out_val_len);
int yowza_db_tx_get(YowzaDB* db, const char* key, void** out_val, size_t* out_val_len);
int yowza_db_tx_assert_raw(YowzaDB* db, uint8_t kind, const void* key, size_t key_len, const void* val, size_t val_len);
int yowza_db_tx_commit(YowzaDB* db);
int yowza_db_tx_abort(YowzaDB* db);
int yowza_db_tx_active(YowzaDB* db);

#endif // YOWZA_DB_H
