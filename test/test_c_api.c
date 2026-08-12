#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include "yowza_db.h"

void test_c_api() {
    printf("Running test_c_api...\n");

    // Pre-allocate memory buffers
    uint8_t memory_buffer[8192];

    // Create native disk storage
    StorageDriver storage = storage_posix_create("test_native.wal");

    YowzaDB db;
    int res = yowza_db_init(&db, memory_buffer, sizeof(memory_buffer), storage);
    assert(res == 0);

    // 1. Test Put
    res = yowza_db_put(&db, "user:1", "Alice", 5);
    assert(res == 0);

    res = yowza_db_put(&db, "user:2", "Bob", 3);
    assert(res == 0);

    // Verify WAL was written to disk
    FILE* f = fopen("test_native.wal", "rb");
    assert(f != NULL);
    fseek(f, 0, SEEK_END);
    long fsize = ftell(f);
    assert(fsize > 0);
    fclose(f);

    // 2. Test Get
    void* val;
    size_t val_len;
    res = yowza_db_get(&db, "user:1", &val, &val_len);
    assert(res == 0);
    assert(val_len == 5);
    assert(memcmp(val, "Alice", 5) == 0);

    res = yowza_db_get(&db, "user:99", &val, &val_len);
    assert(res == -1); // Not found

    // 3. Test Delete
    res = yowza_db_delete(&db, "user:2");
    assert(res == 0);

    res = yowza_db_get(&db, "user:2", &val, &val_len);
    assert(res == -1); // Bob should be deleted

    // 4. Test Scan
    res = yowza_db_put(&db, "user:3", "Charlie", 7);
    assert(res == 0);

    uint8_t scan_buffer[1024];
    YowzaScanResult scan_res;
    res = yowza_db_scan(&db, "user:", scan_buffer, sizeof(scan_buffer), &scan_res);
    assert(res == 0);
    assert(scan_res.count == 2); // Alice and Charlie
    assert(scan_res.total_bytes > 0);

    printf("C API Test Passed!\n");
}
