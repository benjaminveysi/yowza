#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include "vm.h"

// Helper to write a 32-bit integer to a byte array (little-endian)
void write_u32(uint8_t* buf, size_t* offset, uint32_t val) {
    buf[(*offset)++] = val & 0xFF;
    buf[(*offset)++] = (val >> 8) & 0xFF;
    buf[(*offset)++] = (val >> 16) & 0xFF;
    buf[(*offset)++] = (val >> 24) & 0xFF;
}

void test_vm_execute_put() {
    printf("Running test_vm_execute_put...\n");
    uint8_t bytecode[256];
    size_t pc = 0;

    // OP_PUT
    bytecode[pc++] = OP_PUT;

    // Key Length (4)
    write_u32(bytecode, &pc, 4);
    // Key ("test")
    memcpy(&bytecode[pc], "test", 4);
    pc += 4;

    // Value Length (5)
    write_u32(bytecode, &pc, 5);
    // Value ("hello")
    memcpy(&bytecode[pc], "hello", 5);
    pc += 5;

    // OP_EXIT
    bytecode[pc++] = OP_EXIT;

    Arena arena;
    uint8_t buffer[4096];
    arena_init(&arena, buffer, 4096);
    ART art;
    art_init(&art, &arena);

    uint8_t wal_buf[1024];
    StorageDriver storage = storage_memory_create(wal_buf, 1024);

    VMContext vm;
    vm_init(&vm, &art, bytecode, pc, &storage);

    int result = vm_execute(&vm);
    assert(result == 0);

    // Check WAL
    assert(storage_memory_get_offset(&storage) > 0);
    assert(wal_buf[0] == OP_PUT);

    // Verify it actually went into the tree
    uint8_t* out_val;
    uint32_t out_val_len;
    assert(art_search(&art, (const uint8_t*)"test", 4, &out_val, &out_val_len) == 0);
    assert(out_val_len == 5);
    assert(memcmp(out_val, "hello", 5) == 0);

    printf("Passed.\n");
}

void test_vm_execute_scan() {
    printf("Running test_vm_execute_scan...\n");
    uint8_t bytecode[256];
    size_t pc = 0;

    // OP_SCAN
    bytecode[pc++] = OP_SCAN;
    // We write prefix length directly
    bytecode[pc++] = 4;
    bytecode[pc++] = 0;
    bytecode[pc++] = 0;
    bytecode[pc++] = 0;

    // Prefix
    memcpy(&bytecode[pc], "user", 4);
    pc += 4;

    // out buffer size
    bytecode[pc++] = 0;
    bytecode[pc++] = 4;
    bytecode[pc++] = 0;
    bytecode[pc++] = 0; // 1024

    // OP_EXIT
    bytecode[pc++] = OP_EXIT;

    Arena arena;
    uint8_t buffer[4096];
    arena_init(&arena, buffer, 4096);
    ART art;
    art_init(&art, &arena);

    // Seed data
    art_insert(&art, (const uint8_t*)"user:1", 6, (const uint8_t*)"Alice", 5, 0);

    uint8_t wal_buf[1024];
    StorageDriver storage = storage_memory_create(wal_buf, 1024);

    VMContext vm;
    vm_init(&vm, &art, bytecode, pc, &storage);

    int result = vm_execute(&vm);
    assert(result == 0);
    assert(vm.response_len > 0);

    printf("Passed.\n");
}

void test_vm_execute_get_del() {
    printf("Running test_vm_execute_get_del...\n");
    uint8_t bytecode[256];
    size_t pc = 0;

    // OP_GET
    bytecode[pc++] = OP_GET;
    write_u32(bytecode, &pc, 4); // Key Length
    memcpy(&bytecode[pc], "key1", 4); // Key
    pc += 4;

    // OP_DEL
    bytecode[pc++] = OP_DEL;
    write_u32(bytecode, &pc, 4); // Key Length
    memcpy(&bytecode[pc], "key2", 4); // Key
    pc += 4;

    // OP_EXIT
    bytecode[pc++] = OP_EXIT;

    Arena arena;
    uint8_t buffer[4096];
    arena_init(&arena, buffer, 4096);
    ART art;
    art_init(&art, &arena);

    // Pre-seed the ART index
    art_insert(&art, (const uint8_t*)"key1", 4, (const uint8_t*)"val1", 4, 0);
    art_insert(&art, (const uint8_t*)"key2", 4, (const uint8_t*)"val2", 4, 0);

    uint8_t wal_buf[1024];
    StorageDriver storage = storage_memory_create(wal_buf, 1024);

    VMContext vm;
    vm_init(&vm, &art, bytecode, pc, &storage);

    int result = vm_execute(&vm);
    assert(result == 0);

    // Check GET result
    assert(vm.response_len == 4);
    assert(memcmp(vm.response_ptr, "val1", 4) == 0);

    // Check DEL result (key2 should be gone)
    uint8_t* out_val;
    uint32_t out_val_len;
    assert(art_search(&art, (const uint8_t*)"key2", 4, &out_val, &out_val_len) == -1);

    printf("Passed.\n");
}

void test_vm_execute_unknown_opcode() {
    printf("Running test_vm_execute_unknown_opcode...\n");
    uint8_t bytecode[256];
    size_t pc = 0;

    // Unknown Opcode
    bytecode[pc++] = 0x99;

    Arena arena;
    uint8_t buffer[1024];
    arena_init(&arena, buffer, 1024);
    ART art;
    art_init(&art, &arena);

    uint8_t wal_buf[1024];
    StorageDriver storage = storage_memory_create(wal_buf, 1024);

    VMContext vm;
    vm_init(&vm, &art, bytecode, pc, &storage);

    int result = vm_execute(&vm);
    assert(result == -2); // Expecting unknown opcode error

    printf("Passed.\n");
}
