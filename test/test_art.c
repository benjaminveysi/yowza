#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include "art.h"

void test_art_insert_and_search() {
    printf("Running test_art_insert_and_search...\n");
    Arena arena;
    uint8_t buffer[4096];
    arena_init(&arena, buffer, 4096);

    ART art;
    art_init(&art, &arena);

    // Insert
    const uint8_t key1[] = "hello";
    const uint8_t val1[] = "world";
    int res = art_insert(&art, key1, 5, val1, 5, 0);
    assert(res == 0);

    const uint8_t key2[] = "helm";
    const uint8_t val2[] = "boat";
    res = art_insert(&art, key2, 4, val2, 4, 0);
    assert(res == 0);

    // Search
    uint8_t* out_val;
    uint32_t out_val_len;

    res = art_search(&art, key1, 5, &out_val, &out_val_len);
    assert(res == 0);
    assert(out_val_len == 5);
    assert(memcmp(out_val, "world", 5) == 0);

    res = art_search(&art, key2, 4, &out_val, &out_val_len);
    assert(res == 0);
    assert(out_val_len == 4);
    assert(memcmp(out_val, "boat", 4) == 0);

    // Search not found
    const uint8_t key3[] = "help";
    res = art_search(&art, key3, 4, &out_val, &out_val_len);
    assert(res == -1);

    printf("Passed.\n");
}

void test_art_scan_and_snapshot() {
    printf("Running test_art_scan_and_snapshot...\n");
    Arena arena;
    uint8_t buffer[8192];
    arena_init(&arena, buffer, 8192);

    ART art;
    art_init(&art, &arena);

    art_insert(&art, (const uint8_t*)"user:1:name", 11, (const uint8_t*)"Alice", 5, 0);
    art_insert(&art, (const uint8_t*)"user:1:age", 10, (const uint8_t*)"30", 2, 0);
    art_insert(&art, (const uint8_t*)"user:2:name", 11, (const uint8_t*)"Bob", 3, 0);

    uint8_t scan_buf[1024];
    int written = art_scan(&art, (const uint8_t*)"user:1:", 7, scan_buf, 1024);
    assert(written > 0); // Should find Alice and 30

    // Test Serialize
    uint8_t snap_buf[1024];
    int snap_len = art_serialize(&art, snap_buf, 1024);
    assert(snap_len > 0);

    // Test Deserialize
    Arena arena2;
    uint8_t buffer2[8192];
    arena_init(&arena2, buffer2, 8192);

    ART art2;
    art_init(&art2, &arena2);

    int res = art_deserialize(&art2, snap_buf, snap_len);
    assert(res == 0);

    uint8_t* out_val;
    uint32_t out_val_len;
    assert(art_search(&art2, (const uint8_t*)"user:2:name", 11, &out_val, &out_val_len) == 0);
    assert(out_val_len == 3);
    assert(memcmp(out_val, "Bob", 3) == 0);

    printf("Passed.\n");
}

void test_art_delete() {
    printf("Running test_art_delete...\n");
    Arena arena;
    uint8_t buffer[4096];
    arena_init(&arena, buffer, 4096);

    ART art;
    art_init(&art, &arena);

    const uint8_t key[] = "delete_me";
    const uint8_t val[] = "data";
    art_insert(&art, key, 9, val, 4, 0);

    uint8_t* out_val;
    uint32_t out_val_len;
    int res = art_search(&art, key, 9, &out_val, &out_val_len);
    assert(res == 0); // Found

    res = art_delete(&art, key, 9);
    assert(res == 0); // Deleted successfully

    res = art_search(&art, key, 9, &out_val, &out_val_len);
    assert(res == -1); // Not found anymore

    printf("Passed.\n");
}
