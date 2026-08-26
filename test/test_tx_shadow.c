/* Phase C tests: tx DELETE of a hot-absent key creates a
 * shadowing tombstone (tx.c unified delete), in both the
 * live commit path and frame replay. Self-registers
 * through a constructor (same pattern as
 * test_phaseb_dir_root.c): test/main.c is owned by
 * another agent, so this file runs its tests when the
 * test_runner binary starts.
 *
 * What is proven, at the ART level (the level the bug
 * lived on): after a tx deletes a key that never had a
 * hot terminal, art_find_terminal still finds a terminal
 * for it, with is_end == 0 and timestamp > 0 — a real
 * tombstone that shadows any segment value below it at
 * new snapshots, exactly as segdb_delete_absent's
 * tombstone does. A plain art_delete no-op would leave
 * no terminal at all (case 1) or an intermediate-only
 * node with timestamp 0 (case 2). A failed tx must
 * leave no stray tombstone (case 3, abort path).
 */

#include <assert.h>
#include <stdio.h>
#include <string.h>

#include "art.h"
#include "storage.h"
#include "tx.h"
#include "yowza_db.h"

static void shadow_mkdb(YowzaDB* db, uint8_t* mem, size_t mem_size, uint8_t* wal, uint32_t wal_size) {
    StorageDriver st = storage_memory_create(wal, wal_size);
    assert(yowza_db_init(db, mem, mem_size, st) == 0);
}

static void shadow_check_tombstone(YowzaDB* db, const char* key) {
    ARTNode* term = art_find_terminal(&db->index, (const uint8_t*)key, (uint32_t)strlen(key));
    assert(term != NULL);
    assert(term->is_end == 0);
    assert(term->timestamp > 0);
}

static void test_tx_shadow_delete_absent(void) {
    printf("Running test_tx_shadow_delete_absent...\n");
    /* Case 1: key never existed anywhere in the hot tree. */
    {
        uint8_t mem[16384], wal[16384];
        YowzaDB db; shadow_mkdb(&db, mem, sizeof(mem), wal, sizeof(wal));
        assert(yowza_db_tx_begin(&db, 201, 1, TX_CONSISTENCY_LOCAL, 1) == 0);
        assert(yowza_db_tx_delete(&db, "ghost") == 0);
        assert(yowza_db_tx_commit(&db) == 0);
        shadow_check_tombstone(&db, "ghost");
        /* And the key still reads as missing (a tombstone
         * is not a value). */
        void* val = NULL; size_t len = 0;
        assert(yowza_db_get(&db, "ghost", &val, &len) != 0);
        if (db.storage.close) db.storage.close(&db.storage);
    }
    /* Case 2: intermediate-only node (a prefix of a live key). */
    {
        uint8_t mem[16384], wal[16384];
        YowzaDB db; shadow_mkdb(&db, mem, sizeof(mem), wal, sizeof(wal));
        assert(yowza_db_put(&db, "ab", "V", 1) == 0);
        /* "a" has a path node but no terminal value. */
        assert(yowza_db_tx_begin(&db, 202, 1, TX_CONSISTENCY_LOCAL, 1) == 0);
        assert(yowza_db_tx_delete(&db, "a") == 0);
        assert(yowza_db_tx_commit(&db) == 0);
        shadow_check_tombstone(&db, "a");
        /* The live key under it is untouched. */
        void* val = NULL; size_t len = 0;
        assert(yowza_db_get(&db, "ab", &val, &len) == 0 && len == 1 && memcmp(val, "V", 1) == 0);
        if (db.storage.close) db.storage.close(&db.storage);
    }
    /* Case 3: an aborted tx delete leaves NO tombstone. */
    {
        uint8_t mem[16384], wal[16384];
        YowzaDB db; shadow_mkdb(&db, mem, sizeof(mem), wal, sizeof(wal));
        assert(yowza_db_tx_begin(&db, 203, 1, TX_CONSISTENCY_LOCAL, 1) == 0);
        assert(yowza_db_tx_delete(&db, "ghost2") == 0);
        assert(yowza_db_tx_abort(&db) == 0);
        assert(art_find_terminal(&db.index, (const uint8_t*)"ghost2", 6) == NULL);
        if (db.storage.close) db.storage.close(&db.storage);
    }
    printf("Passed.\n");
}

static void test_tx_shadow_frame_replay(void) {
    printf("Running test_tx_shadow_frame_replay...\n");
    uint8_t mem[16384], wal[16384];
    YowzaDB db; shadow_mkdb(&db, mem, sizeof(mem), wal, sizeof(wal));
    /* Only tx operations, so the WAL is exactly one frame. */
    assert(yowza_db_tx_begin(&db, 204, 1, TX_CONSISTENCY_LOCAL, 1) == 0);
    assert(yowza_db_tx_delete(&db, "ghost") == 0);
    assert(yowza_db_tx_commit(&db) == 0);
    uint32_t wal_len = storage_memory_get_offset(&db.storage);
    assert(wal_len > 0 && wal[0] == 0xF0);
    if (db.storage.close) db.storage.close(&db.storage);
    /* Replay into a fresh tree: the frame's DELETE must
     * create the same shadowing tombstone, not evaporate. */
    uint8_t mem2[16384], wal2[16384];
    YowzaDB db2; shadow_mkdb(&db2, mem2, sizeof(mem2), wal2, sizeof(wal2));
    uint32_t applied = 0;
    assert(tx_replay_wal(&db2.index, wal, wal_len, &applied) == 0);
    assert(applied == 1);
    shadow_check_tombstone(&db2, "ghost");
    if (db2.storage.close) db2.storage.close(&db2.storage);
    printf("Passed.\n");
}

__attribute__((constructor)) static void test_tx_shadow_register(void) {
    test_tx_shadow_delete_absent();
    test_tx_shadow_frame_replay();
}
