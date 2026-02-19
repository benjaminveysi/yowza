#include "storage.h"

// State structure for Memory (Wasm) driver
typedef struct {
    uint8_t* buffer;
    uint32_t capacity;
    uint32_t offset;
} MemoryState;

// Since we can't use malloc in Wasm (nostdlib), we use a static pool.
// The old code had a single static MemoryState, so creating a second
// driver silently clobbered the first driver's WAL buffer and offset.
// The pool keeps that from happening for up to 16 concurrent drivers;
// beyond that, create returns a driver with state == NULL and the
// caller must treat it as a failed creation (VM checks storage->append).
static MemoryState g_mem_states[16];
static uint8_t g_mem_state_used[16];

static int mem_append(StorageDriver* self, const uint8_t* data, uint32_t length) {
    if (!self || !self->state || !data || length == 0) return -1;

    MemoryState* st = (MemoryState*)self->state;
    if (!st->buffer || st->capacity == 0) return -1;

    /* Overflow-free: offset <= capacity is an invariant, so
     * capacity - offset cannot underflow. */
    if (length > st->capacity - st->offset) return -1; // OOM in WAL buffer

    for (uint32_t i = 0; i < length; i++) {
        st->buffer[st->offset + i] = data[i];
    }

    st->offset += length;
    return (int)length;
}

static int mem_check_capacity(StorageDriver* self, uint32_t length) {
    if (!self || !self->state) return -1;
    MemoryState* st = (MemoryState*)self->state;
    if (!st->buffer) return -1;
    return length <= st->capacity - st->offset ? 0 : -1;
}

static int mem_sync(StorageDriver* self) {
    (void)self;
    // Memory is instantly synchronous by nature
    return 0;
}

static int mem_read(StorageDriver* self, uint8_t* buffer, uint32_t length, uint32_t offset) {
    if (!self || !self->state || !buffer) return -1;
    MemoryState* st = (MemoryState*)self->state;

    if (offset >= st->offset) return 0; // EOF

    uint32_t available = st->offset - offset;
    uint32_t to_read = (length < available) ? length : available;

    for (uint32_t i = 0; i < to_read; i++) {
        buffer[i] = st->buffer[offset + i];
    }

    return (int)to_read;
}

static int mem_log_length(StorageDriver* self) {
    if (!self || !self->state) return -1;
    return (int)((MemoryState*)self->state)->offset;
}

static int mem_truncate(StorageDriver* self, uint32_t length) {
    if (!self || !self->state) return -1;
    MemoryState* st = (MemoryState*)self->state;
    if (length > st->offset) return -1; /* never grow by truncating */
    st->offset = length;
    return 0;
}

static void mem_close(StorageDriver* self) {
    if (!self || !self->state) return;

    // Release this driver's slot in the static pool so a later
    // create can reuse it. In Wasm, state is statically allocated.
    // We don't free it to avoid stdlib dependencies.
    for (int i = 0; i < 16; i++) {
        if (self->state == (void*)&g_mem_states[i]) {
            g_mem_state_used[i] = 0;
            break;
        }
    }
    self->state = NULL;
}

StorageDriver storage_memory_create(uint8_t* wal_buffer, uint32_t capacity) {
    StorageDriver driver = {0};

    for (int i = 0; i < 16; i++) {
        if (!g_mem_state_used[i]) {
            g_mem_state_used[i] = 1;
            g_mem_states[i].buffer = wal_buffer;
            g_mem_states[i].capacity = capacity;
            g_mem_states[i].offset = 0;
            driver.state = &g_mem_states[i];
            driver.append = mem_append;
            driver.check_capacity = mem_check_capacity;
            driver.sync = mem_sync;
            driver.log_length = mem_log_length;
            driver.truncate = mem_truncate;
            driver.read = mem_read;
            driver.close = mem_close;
            return driver;
        }
    }

    return driver; /* pool exhausted: state == NULL, append == NULL */
}

uint32_t storage_memory_get_offset(StorageDriver* driver) {
    if (!driver || !driver->state) return 0;
    MemoryState* st = (MemoryState*)driver->state;
    return st->offset;
}

void storage_memory_clear(StorageDriver* driver) {
    if (!driver || !driver->state) return;
    MemoryState* st = (MemoryState*)driver->state;
    st->offset = 0;
}
