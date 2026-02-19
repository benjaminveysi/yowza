#ifndef __wasm__

#include "storage.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

// State structure for POSIX file driver
typedef struct {
    FILE* fp;
} PosixState;

static int posix_append(StorageDriver* self, const uint8_t* data, uint32_t length) {
    if (!self || !self->state || !data || length == 0) return -1;

    PosixState* st = (PosixState*)self->state;
    if (!st->fp) return -1;

    size_t written = fwrite(data, 1, length, st->fp);
    return (int)written;
}

static int posix_check_capacity(StorageDriver* self, uint32_t length) {
    (void)length;
    /* A file can grow (subject to disk space, which append reports). */
    if (!self || !self->state) return -1;
    return 0;
}

static int posix_sync(StorageDriver* self) {
    if (!self || !self->state) return -1;
    PosixState* st = (PosixState*)self->state;
    if (!st->fp) return -1;

    /* Step 1: fflush alone only moves data to the OS buffer; it does
     * NOT make it crash-durable. fsync pushes it to the device. */
    if (fflush(st->fp) != 0) return -1;
    return fsync(fileno(st->fp)) == 0 ? 0 : -1;
}

static int posix_read(StorageDriver* self, uint8_t* buffer, uint32_t length, uint32_t offset) {
    if (!self || !self->state || !buffer) return -1;
    PosixState* st = (PosixState*)self->state;
    if (!st->fp) return -1;

    if (fseek(st->fp, offset, SEEK_SET) != 0) return -1;

    size_t r = fread(buffer, 1, length, st->fp);
    return (int)r;
}

static int posix_log_length(StorageDriver* self) {
    if (!self || !self->state) return -1;
    PosixState* st = (PosixState*)self->state;
    if (!st->fp) return -1;
    if (fflush(st->fp) != 0) return -1;
    if (fseek(st->fp, 0, SEEK_END) != 0) return -1;
    long pos = ftell(st->fp);
    return pos < 0 ? -1 : (int)pos;
}

static int posix_truncate(StorageDriver* self, uint32_t length) {
    if (!self || !self->state) return -1;
    PosixState* st = (PosixState*)self->state;
    if (!st->fp) return -1;
    if (fflush(st->fp) != 0) return -1;
    return ftruncate(fileno(st->fp), (off_t)length) == 0 ? 0 : -1;
}

static void posix_close(StorageDriver* self) {
    if (!self || !self->state) return;
    PosixState* st = (PosixState*)self->state;

    if (st->fp) {
        fclose(st->fp);
        st->fp = NULL;
    }
    // We expect the caller to manage the StorageDriver struct itself,
    // but we dynamically allocate the state to keep the header clean.
    // In a strict memory environment, we'd use Arena here too.
    // For Native Server, malloc is fine.
    free(st);
    self->state = NULL;
}

StorageDriver storage_posix_create(const char* filepath) {
    StorageDriver driver = {0};

    PosixState* st = (PosixState*)malloc(sizeof(PosixState));
    if (st) {
        // Open file in append/binary mode. It creates it if it doesn't exist.
        // We use a+b to allow reading as well (for crash recovery).
        st->fp = fopen(filepath, "a+b");
        if (st->fp) {
            driver.state = st;
            driver.append = posix_append;
            driver.check_capacity = posix_check_capacity;
            driver.sync = posix_sync;
            driver.log_length = posix_log_length;
            driver.truncate = posix_truncate;
            driver.read = posix_read;
            driver.close = posix_close;
        } else {
            free(st);
        }
    }

    return driver;
}

#endif // __wasm__
