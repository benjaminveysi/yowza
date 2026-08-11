#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include "arena.h"

void test_arena_initialization() {
    printf("Running test_arena_initialization...\n");
    Arena arena;
    uint8_t buffer[1024];

    arena_init(&arena, buffer, 1024);
    assert(arena.buffer == buffer);
    assert(arena.size == 1024);
    assert(arena.offset == 0);
    printf("Passed.\n");
}

void test_arena_allocation() {
    printf("Running test_arena_allocation...\n");
    Arena arena;
    uint8_t buffer[1024];

    arena_init(&arena, buffer, 1024);

    // Allocate 10 bytes
    void* ptr1 = arena_alloc(&arena, 10);
    assert(ptr1 != NULL);
    assert(ptr1 == &buffer[0]);
    // Offset should be aligned to 8 (so 10 bytes aligned up to 16)
    assert(arena.offset == 16);

    // Allocate 4 bytes
    void* ptr2 = arena_alloc(&arena, 4);
    assert(ptr2 != NULL);
    assert(ptr2 == &buffer[16]);
    // Offset should be aligned to 8 (4 aligned up to 8 + 16 = 24)
    assert(arena.offset == 24);

    printf("Passed.\n");
}

void test_arena_out_of_memory() {
    printf("Running test_arena_out_of_memory...\n");
    Arena arena;
    uint8_t buffer[32]; // Small buffer

    arena_init(&arena, buffer, 32);

    void* ptr1 = arena_alloc(&arena, 24); // 24 bytes (aligned to 24)
    assert(ptr1 != NULL);
    assert(arena.offset == 24);

    void* ptr2 = arena_alloc(&arena, 16); // Need 16 bytes, but only 8 left
    assert(ptr2 == NULL);
    assert(arena.offset == 24); // Offset shouldn't change

    printf("Passed.\n");
}

void test_arena_reset() {
    printf("Running test_arena_reset...\n");
    Arena arena;
    uint8_t buffer[1024];

    arena_init(&arena, buffer, 1024);

    void* ptr1 = arena_alloc(&arena, 100);
    assert(ptr1 != NULL);
    assert(arena.offset > 0);

    arena_reset(&arena);
    assert(arena.offset == 0);

    void* ptr2 = arena_alloc(&arena, 100);
    assert(ptr2 != NULL);
    assert(ptr1 == ptr2); // Should allocate at the same spot

    printf("Passed.\n");
}
