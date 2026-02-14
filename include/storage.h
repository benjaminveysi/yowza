#ifndef YOWZA_STORAGE_H
#define YOWZA_STORAGE_H

#include "arena.h"

// Hardware Abstraction Layer (HAL) for Yowza!
// This interface allows the engine to decouple computation from I/O.

typedef struct StorageDriver {
    // Arbitrary state pointer for the driver implementation
    void* state;

    // Appends binary data to the storage backend (Disk or Memory).
    // Returns number of bytes written, or -1 on error.
    int (*append)(struct StorageDriver* self, const uint8_t* data, uint32_t length);

    // Step 1: optional capacity pre-check, used by the VM *before* it
    // mutates the ART, so a full WAL fails fast instead of silently
    // dropping the record after a successful PUT. Returns 0 if `length`
    // more bytes can be appended, -1 otherwise. NULL means "unknown":
    // the VM then appends and checks its result instead.
    int (*check_capacity)(struct StorageDriver* self, uint32_t length);

    // Flushes/Syncs the storage to ensure durability.
    int (*sync)(struct StorageDriver* self);

    // Phase 1 (optional): current log length in bytes, or -1 if unknown.
    // Used with truncate to roll back a partially appended frame.
    int (*log_length)(struct StorageDriver* self);

    // Phase 1 (optional): truncate the log back to `length` bytes.
    // Used when a WAL transaction frame fails part-way through
    // appending, so a partial frame never shadows later frames.
    // NULL means "unsupported": callers then rely on the capacity
    // pre-check and treat a mid-frame failure as fatal to that frame.
    // Returns 0 on success, -1 otherwise.
    int (*truncate)(struct StorageDriver* self, uint32_t length);

    // Reads data from storage (useful for crash recovery / cold starts).
    // Returns bytes read, or -1 on error.
    int (*read)(struct StorageDriver* self, uint8_t* buffer, uint32_t length, uint32_t offset);

    // Closes the storage driver
    void (*close)(struct StorageDriver* self);
} StorageDriver;

// Factory function for Wasm Memory-based driver
// Implemented in storage_memory.c
StorageDriver storage_memory_create(uint8_t* wal_buffer, uint32_t capacity);

// Expose WAL offset for the memory driver directly
uint32_t storage_memory_get_offset(StorageDriver* driver);
void storage_memory_clear(StorageDriver* driver);

#ifndef __wasm__
// Factory function for Native POSIX Disk-based driver
// Implemented in storage_posix.c
StorageDriver storage_posix_create(const char* filepath);
#endif

#endif // YOWZA_STORAGE_H
