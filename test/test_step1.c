#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include "yowza_db.h"

/* Step-1 regression tests. Each test mirrors a finding from the audit:
 * arena overflow, ART timestamp loss on promotion, CRDT snapshot loss,
 * zombie resurrection, empty values, malformed bytecode, WAL-full
 * silent success, large WAL records, snapshot corruption, legacy
 * snapshot compatibility, compact overlap, restore pollution, and the
 * shared static memory-driver state. */

static void s1_u32(uint8_t* buf, size_t* off, uint32_t v) {
    buf[(*off)++] = v & 0xFF;
    buf[(*off)++] = (v >> 8) & 0xFF;
    buf[(*off)++] = (v >> 16) & 0xFF;
    buf[(*off)++] = (v >> 24) & 0xFF;
}

static void s1_u64(uint8_t* buf, size_t* off, uint64_t v) {
    for (int i = 0; i < 8; i++) buf[(*off)++] = (uint8_t)((v >> (i * 8)) & 0xFF);
}

static void s1_put_bc(uint8_t* bc, size_t* pc, const char* k, const char* v) {
    bc[(*pc)++] = OP_PUT;
    s1_u32(bc, pc, (uint32_t)strlen(k));
    memcpy(&bc[*pc], k, strlen(k)); *pc += strlen(k);
    s1_u32(bc, pc, (uint32_t)strlen(v));
    memcpy(&bc[*pc], v, strlen(v)); *pc += strlen(v);
    bc[(*pc)++] = OP_EXIT;
}

void test_step1_arena_overflow() {
    printf("Running test_step1_arena_overflow...\n");
    uint8_t buf[1024];
    Arena arena;
    arena_init(&arena, buf, sizeof(buf));
    assert(arena_alloc(&arena, 16) != NULL);
    assert(arena.offset == 16);
    /* Old code wrapped 16 + (SIZE_MAX-8) to 8 and "succeeded". */
    assert(arena_alloc(&arena, (size_t)-8) == NULL);
    assert(arena.offset == 16);
    printf("Passed.\n");
}

void test_step1_art_timestamp_promotion() {
    printf("Running test_step1_art_timestamp_promotion...\n");
    /* Poison the arena like the audit harness did: 0xAA everywhere. */
    static uint8_t buf[65536];
    memset(buf, 0xAA, sizeof(buf));
    Arena arena;
    arena_init(&arena, buf, sizeof(buf));
    ART art;
    art_init(&art, &arena);

    assert(art_insert(&art, (const uint8_t*)"a", 1, (const uint8_t*)"v100", 4, 100) == 0);
    /* Five children force the 'a' node from Node4 to Node16. */
    assert(art_insert(&art, (const uint8_t*)"ab", 2, (const uint8_t*)"x", 1, 0) == 0);
    assert(art_insert(&art, (const uint8_t*)"ac", 2, (const uint8_t*)"x", 1, 0) == 0);
    assert(art_insert(&art, (const uint8_t*)"ad", 2, (const uint8_t*)"x", 1, 0) == 0);
    assert(art_insert(&art, (const uint8_t*)"ae", 2, (const uint8_t*)"x", 1, 0) == 0);
    assert(art_insert(&art, (const uint8_t*)"af", 2, (const uint8_t*)"x", 1, 0) == 0);

    uint8_t* out; uint32_t out_len;
    /* Stale write must be ignored after promotion. */
    assert(art_insert(&art, (const uint8_t*)"a", 1, (const uint8_t*)"stale", 5, 50) == 0);
    assert(art_search(&art, (const uint8_t*)"a", 1, &out, &out_len) == 0);
    assert(out_len == 4 && memcmp(out, "v100", 4) == 0);
    /* Newer write must win. */
    assert(art_insert(&art, (const uint8_t*)"a", 1, (const uint8_t*)"v200", 4, 200) == 0);
    assert(art_search(&art, (const uint8_t*)"a", 1, &out, &out_len) == 0);
    assert(out_len == 4 && memcmp(out, "v200", 4) == 0);
    printf("Passed.\n");
}

void test_step1_crdt_snapshot_and_zombie() {
    printf("Running test_step1_crdt_snapshot_and_zombie...\n");
    uint8_t buf[16384];
    Arena arena; arena_init(&arena, buf, sizeof(buf));
    ART art; art_init(&art, &arena);
    assert(art_insert(&art, (const uint8_t*)"k1", 2, (const uint8_t*)"new", 3, 100) == 0);

    uint8_t snap[4096];
    int snap_len = art_serialize(&art, snap, sizeof(snap));
    assert(snap_len > 0);
    assert(snap[0] == 'Y' && snap[1] == 'W' && snap[2] == 'Z' && snap[3] == '6');
    assert(art_validate_snapshot(snap, (uint32_t)snap_len) == 0);

    uint8_t buf2[16384];
    Arena arena2; arena_init(&arena2, buf2, sizeof(buf2));
    ART art2; art_init(&art2, &arena2);
    assert(art_deserialize(&art2, snap, (uint32_t)snap_len) == 0);

    uint8_t* out; uint32_t out_len;
    /* Stale write after restore must not overwrite the newer value. */
    assert(art_insert(&art2, (const uint8_t*)"k1", 2, (const uint8_t*)"old", 3, 50) == 0);
    assert(art_search(&art2, (const uint8_t*)"k1", 2, &out, &out_len) == 0);
    assert(out_len == 3 && memcmp(out, "new", 3) == 0);

    /* Zombie: delete at ts=100, then a stale ts=10 write must not revive. */
    assert(art_insert(&art2, (const uint8_t*)"del", 3, (const uint8_t*)"gone", 4, 100) == 0);
    assert(art_delete(&art2, (const uint8_t*)"del", 3) == 0);
    assert(art_insert(&art2, (const uint8_t*)"del", 3, (const uint8_t*)"zombie", 6, 10) == 0);
    assert(art_search(&art2, (const uint8_t*)"del", 3, &out, &out_len) == -1);
    printf("Passed.\n");
}

void test_step1_snapshot_corruption_and_legacy() {
    printf("Running test_step1_snapshot_corruption_and_legacy...\n");
    uint8_t buf[16384];
    Arena arena; arena_init(&arena, buf, sizeof(buf));
    ART art; art_init(&art, &arena);
    assert(art_insert(&art, (const uint8_t*)"k", 1, (const uint8_t*)"v", 1, 42) == 0);
    uint8_t snap[4096];
    int snap_len = art_serialize(&art, snap, sizeof(snap));
    assert(snap_len > 0);

    uint8_t bad[4096];
    memcpy(bad, snap, (size_t)snap_len);
    bad[20] ^= 0xFF; /* corrupt one entry byte: checksum must catch it */
    assert(art_validate_snapshot(bad, (uint32_t)snap_len) == -1);

    uint8_t buf2[16384];
    Arena arena2; arena_init(&arena2, buf2, sizeof(buf2));
    ART art2; art_init(&art2, &arena2);
    assert(art_deserialize(&art2, bad, (uint32_t)snap_len) == -1);

    /* Legacy format (no header, no timestamp) must still load, with ts=0. */
    uint8_t legacy[64]; size_t off = 0;
    s1_u32(legacy, &off, 3); memcpy(&legacy[off], "old", 3); off += 3;
    s1_u32(legacy, &off, 3); memcpy(&legacy[off], "val", 3); off += 3;
    assert(art_validate_snapshot(legacy, (uint32_t)off) == 0);
    uint8_t buf3[16384];
    Arena arena3; arena_init(&arena3, buf3, sizeof(buf3));
    ART art3; art_init(&art3, &arena3);
    assert(art_deserialize(&art3, legacy, (uint32_t)off) == 0);
    uint8_t* out; uint32_t out_len;
    assert(art_search(&art3, (const uint8_t*)"old", 3, &out, &out_len) == 0);
    assert(out_len == 3 && memcmp(out, "val", 3) == 0);
    printf("Passed.\n");
}

void test_step1_vm_malformed_and_wal() {
    printf("Running test_step1_vm_malformed_and_wal...\n");
    /* Malformed: OP_PUT claims key_len 32 but only 5 bytes exist. */
    {
        uint8_t bc[5] = { OP_PUT, 32, 0, 0, 0 };
        uint8_t buf[4096]; Arena arena; arena_init(&arena, buf, sizeof(buf));
        ART art; art_init(&art, &arena);
        uint8_t wal[1024]; StorageDriver st = storage_memory_create(wal, sizeof(wal));
        VMContext vm; vm_init(&vm, &art, bc, sizeof(bc), &st);
        assert(vm_execute(&vm) == -5);
        if (st.close) st.close(&st);
    }
    /* WAL full: first small PUT fits in 16 bytes, second must fail
     * with -6 and must NOT appear in the tree. */
    {
        uint8_t bc[512]; size_t pc = 0;
        s1_put_bc(bc, &pc, "k1", "v1");
        uint8_t buf[4096]; Arena arena; arena_init(&arena, buf, sizeof(buf));
        ART art; art_init(&art, &arena);
        uint8_t wal[16]; StorageDriver st = storage_memory_create(wal, sizeof(wal));
        VMContext vm; vm_init(&vm, &art, bc, pc, &st);
        assert(vm_execute(&vm) == 0);
        pc = 0; s1_put_bc(bc, &pc, "k2", "v2");
        vm_init(&vm, &art, bc, pc, &st);
        assert(vm_execute(&vm) == -6);
        uint8_t* out; uint32_t out_len;
        assert(art_search(&art, (const uint8_t*)"k2", 2, &out, &out_len) == -1);
        if (st.close) st.close(&st);
    }
    /* Large WAL record (>1024 bytes) must be written in pieces, not dropped. */
    {
        static uint8_t bc[4096]; size_t pc = 0;
        static char bigval[2000];
        memset(bigval, 'x', sizeof(bigval));
        bc[pc++] = OP_PUT;
        s1_u32(bc, &pc, 3); memcpy(&bc[pc], "big", 3); pc += 3;
        s1_u32(bc, &pc, 2000); memcpy(&bc[pc], bigval, 2000); pc += 2000;
        bc[pc++] = OP_EXIT;
        uint8_t buf[16384]; Arena arena; arena_init(&arena, buf, sizeof(buf));
        ART art; art_init(&art, &arena);
        static uint8_t wal[8192]; StorageDriver st = storage_memory_create(wal, sizeof(wal));
        VMContext vm; vm_init(&vm, &art, bc, pc, &st);
        assert(vm_execute(&vm) == 0);
        assert(storage_memory_get_offset(&st) == (uint32_t)(1 + 4 + 3 + 4 + 2000));
        uint8_t* out; uint32_t out_len;
        assert(art_search(&art, (const uint8_t*)"big", 3, &out, &out_len) == 0);
        assert(out_len == 2000);
        if (st.close) st.close(&st);
    }
    printf("Passed.\n");
}

void test_step1_c_api_empty_compact_restore_pool() {
    printf("Running test_step1_c_api_empty_compact_restore_pool...\n");
    /* Empty value must be found, not reported as missing. */
    {
        uint8_t mem[16384]; uint8_t wal[1024];
        StorageDriver st = storage_memory_create(wal, sizeof(wal));
        YowzaDB db; assert(yowza_db_init(&db, mem, sizeof(mem), st) == 0);
        assert(yowza_db_put(&db, "empty", "", 0) == 0);
        void* val = NULL; size_t val_len = 999;
        assert(yowza_db_get(&db, "empty", &val, &val_len) == 0);
        assert(val_len == 0 && val != NULL);
        /* Huge val_len must fail fast, not overflow the bytecode buffer. */
        assert(yowza_db_put(&db, "k", "v", (size_t)-5) == -1);
        /* Key over the step-1 limit must fail fast, not snapshot-fail later. */
        char longkey[1025]; memset(longkey, 'k', sizeof(longkey) - 1); longkey[1024] = '\0';
        assert(yowza_db_put(&db, longkey, "v", 1) == -1);
        if (db.storage.close) db.storage.close(&db.storage);
    }
    /* Compact with an overlapping temp buffer must fail without wiping. */
    {
        uint8_t mem[16384]; uint8_t wal[1024];
        StorageDriver st = storage_memory_create(wal, sizeof(wal));
        YowzaDB db; assert(yowza_db_init(&db, mem, sizeof(mem), st) == 0);
        assert(yowza_db_put(&db, "a", "1", 1) == 0);
        assert(yowza_db_compact(&db, mem, sizeof(mem)) == -1);
        void* val; size_t val_len;
        assert(yowza_db_get(&db, "a", &val, &val_len) == 0);
        uint8_t temp[16384];
        assert(yowza_db_compact(&db, temp, sizeof(temp)) == 0);
        assert(yowza_db_get(&db, "a", &val, &val_len) == 0);
        assert(val_len == 1 && memcmp(val, "1", 1) == 0);
        if (db.storage.close) db.storage.close(&db.storage);
    }
    /* Restore must replace, not pollute, the live tree. */
    {
        uint8_t mem1[16384], mem2[16384], wal1[1024], wal2[1024], snap[4096];
        StorageDriver st1 = storage_memory_create(wal1, sizeof(wal1));
        YowzaDB db1; assert(yowza_db_init(&db1, mem1, sizeof(mem1), st1) == 0);
        assert(yowza_db_put(&db1, "newkey", "new", 3) == 0);
        int snap_len = yowza_db_snapshot_create(&db1, snap, sizeof(snap));
        assert(snap_len > 0);
        StorageDriver st2 = storage_memory_create(wal2, sizeof(wal2));
        YowzaDB db2; assert(yowza_db_init(&db2, mem2, sizeof(mem2), st2) == 0);
        assert(yowza_db_put(&db2, "oldkey", "old", 3) == 0);
        assert(yowza_db_snapshot_restore(&db2, snap, (size_t)snap_len) == 0);
        void* val; size_t val_len;
        assert(yowza_db_get(&db2, "oldkey", &val, &val_len) == -1);
        assert(yowza_db_get(&db2, "newkey", &val, &val_len) == 0);
        if (db1.storage.close) db1.storage.close(&db1.storage);
        if (db2.storage.close) db2.storage.close(&db2.storage);
    }
    /* Two memory drivers must not share one static state. */
    {
        uint8_t wal1[64], wal2[64];
        StorageDriver d1 = storage_memory_create(wal1, sizeof(wal1));
        StorageDriver d2 = storage_memory_create(wal2, sizeof(wal2));
        assert(d1.state != NULL && d2.state != NULL && d1.state != d2.state);
        assert(d1.append(&d1, (const uint8_t*)"AAA", 3) == 3);
        assert(d2.append(&d2, (const uint8_t*)"BBB", 3) == 3);
        assert(storage_memory_get_offset(&d1) == 3 && storage_memory_get_offset(&d2) == 3);
        assert(memcmp(wal1, "AAA", 3) == 0 && memcmp(wal2, "BBB", 3) == 0);
        if (d1.close) d1.close(&d1);
        if (d2.close) d2.close(&d2);
    }
    printf("Passed.\n");
}

void test_step1_crdt_tombstone_max_timestamp() {
    printf("Running test_step1_crdt_tombstone_max_timestamp...\n");
    uint8_t buf[16384];
    Arena arena; arena_init(&arena, buf, sizeof(buf));
    ART art; art_init(&art, &arena);
    /* art_init must start the clock at 1, not 0 (see art.h). */
    assert(art.max_timestamp == 1);

    /* Establish a clock: a CRDT write at ts=100 on an unrelated key. */
    assert(art_insert(&art, (const uint8_t*)"clock", 5, (const uint8_t*)"c", 1, 100) == 0);
    assert(art.max_timestamp == 100);
    /* A plain PUT (ts=0) must not move the clock. */
    assert(art_insert(&art, (const uint8_t*)"victim", 6, (const uint8_t*)"v", 1, 0) == 0);
    assert(art.max_timestamp == 100);

    /* Plain PUT -> DELETE. Before the max_timestamp fix the tombstone
     * kept timestamp 0, so the LWW check (which needs current->timestamp
     * > 0) never fired and ANY later CRDT write resurrected the key. */
    assert(art_delete(&art, (const uint8_t*)"victim", 6) == 0);
    uint8_t* out; uint32_t out_len;
    assert(art_search(&art, (const uint8_t*)"victim", 6, &out, &out_len) == -1);

    /* A stale CRDT write (50 < max 100) must NOT resurrect it. */
    assert(art_insert(&art, (const uint8_t*)"victim", 6, (const uint8_t*)"zombie", 6, 50) == 0);
    assert(art_search(&art, (const uint8_t*)"victim", 6, &out, &out_len) == -1);

    /* A strictly newer CRDT write (200 > max) legitimately wins over
     * the tombstone under LWW: delete is a write at the highest
     * observed clock, not a permanent ban. */
    assert(art_insert(&art, (const uint8_t*)"victim", 6, (const uint8_t*)"revived", 7, 200) == 0);
    assert(art_search(&art, (const uint8_t*)"victim", 6, &out, &out_len) == 0);
    assert(out_len == 7 && memcmp(out, "revived", 7) == 0);
    assert(art.max_timestamp == 200);

    /* Tombstones ARE now serialised (YOWZA_TOMBSTONE_FLAG): after a
     * snapshot restore, both the clock and the tombstone survive.
     * Delete victim again (tombstone at max_timestamp=200), snapshot,
     * restore: max must be 200 (not 100 from the live "clock" entry
     * alone), a stale ts=50 write must stay not-found, and a
     * duplicate ts=200 packet (tie against the tombstone) must also
     * lose, while a strictly newer ts=201 write wins. */
    {
        uint8_t snap[4096];
        /* Delete victim again, then snapshot the tree. */
        assert(art_delete(&art, (const uint8_t*)"victim", 6) == 0);
        int snap_len = art_serialize(&art, snap, sizeof(snap));
        assert(snap_len > 0);
        uint8_t buf2[16384];
        Arena arena2; arena_init(&arena2, buf2, sizeof(buf2));
        ART art2; art_init(&art2, &arena2);
        assert(art_deserialize(&art2, snap, (uint32_t)snap_len) == 0);
        assert(art2.max_timestamp == 200);
        assert(art_insert(&art2, (const uint8_t*)"victim", 6, (const uint8_t*)"zombie2", 7, 50) == 0);
        assert(art_search(&art2, (const uint8_t*)"victim", 6, &out, &out_len) == -1);
        /* Duplicate pre-delete packet at exactly the tombstone clock. */
        assert(art_insert(&art2, (const uint8_t*)"victim", 6, (const uint8_t*)"zombie2", 7, 200) == 0);
        assert(art_search(&art2, (const uint8_t*)"victim", 6, &out, &out_len) == -1);
        assert(art_insert(&art2, (const uint8_t*)"victim", 6, (const uint8_t*)"revived2", 8, 201) == 0);
        assert(art_search(&art2, (const uint8_t*)"victim", 6, &out, &out_len) == 0);
        assert(out_len == 8 && memcmp(out, "revived2", 8) == 0);
    }

    /* Former DEGENERATE CASE, now fixed: with no CRDT clock ever seen
     * (max_timestamp == 1), a plain PUT + DELETE leaves a tombstone
     * at 1, and a CRDT write at ts=1 now TIES against the tombstone
     * and loses (only a strictly newer write revives the key). */
    {
        uint8_t buf3[16384];
        Arena arena3; arena_init(&arena3, buf3, sizeof(buf3));
        ART art3; art_init(&art3, &arena3);
        assert(art_insert(&art3, (const uint8_t*)"plain", 5, (const uint8_t*)"p", 1, 0) == 0);
        assert(art_delete(&art3, (const uint8_t*)"plain", 5) == 0);
        /* Tie at ts=1 loses against tombstone at ts=1; ts=2 wins. */
        assert(art_insert(&art3, (const uint8_t*)"plain", 5, (const uint8_t*)"back", 4, 1) == 0);
        assert(art_search(&art3, (const uint8_t*)"plain", 5, &out, &out_len) == -1);
        assert(art_insert(&art3, (const uint8_t*)"plain", 5, (const uint8_t*)"back", 4, 2) == 0);
        assert(art_search(&art3, (const uint8_t*)"plain", 5, &out, &out_len) == 0);
        assert(out_len == 4 && memcmp(out, "back", 4) == 0);
    }

    /* Free-list exercise (step 4): force Node4->Node16->Node48->Node256
     * upgrades, which push retired nodes onto free_list[4]; then build
     * a second fan-out that must reuse those nodes, snapshot, restore,
     * and verify live keys, a restored tombstone, and a stale write
     * against that tombstone. This runs inside this existing test so
     * test/main.c needs no new registration. */
    {
        uint8_t buf4[65536];
        Arena arena4; arena_init(&arena4, buf4, sizeof(buf4));
        ART art4; art_init(&art4, &arena4);
        uint8_t k[2];
        for (int i = 0; i < 60; i++) {
            k[0] = 'x'; k[1] = (uint8_t)i;
            assert(art_insert(&art4, k, 2, (const uint8_t*)"v", 1, 10) == 0);
        }
        /* Second fan-out reuses the retired Node4/Node16/Node48. */
        for (int i = 0; i < 60; i++) {
            k[0] = 'y'; k[1] = (uint8_t)i;
            assert(art_insert(&art4, k, 2, (const uint8_t*)"w", 1, 10) == 0);
        }
        for (int i = 0; i < 60; i++) {
            k[0] = 'x'; k[1] = (uint8_t)i;
            assert(art_search(&art4, k, 2, &out, &out_len) == 0);
            k[0] = 'y'; k[1] = (uint8_t)i;
            assert(art_search(&art4, k, 2, &out, &out_len) == 0);
        }
        uint8_t delk[2] = {'y', 7};
        assert(art_delete(&art4, delk, 2) == 0);
        uint8_t snap4[16384];
        int snap4_len = art_serialize(&art4, snap4, sizeof(snap4));
        assert(snap4_len > 0);
        uint8_t buf5[65536];
        Arena arena5; arena_init(&arena5, buf5, sizeof(buf5));
        ART art5; art_init(&art5, &arena5);
        assert(art_deserialize(&art5, snap4, (uint32_t)snap4_len) == 0);
        assert(art_search(&art5, delk, 2, &out, &out_len) == -1);
        /* Stale CRDT write against the restored tombstone loses. */
        assert(art_insert(&art5, delk, 2, (const uint8_t*)"z", 1, 5) == 0);
        assert(art_search(&art5, delk, 2, &out, &out_len) == -1);
        for (int i = 0; i < 60; i++) {
            if (i == 7) continue;
            k[0] = 'y'; k[1] = (uint8_t)i;
            assert(art_search(&art5, k, 2, &out, &out_len) == 0);
        }
    }
    printf("Passed.\n");
}

void test_step1_serialize_too_small() {
    printf("Running test_step1_serialize_too_small...\n");
    /* Underpins yowza_compact's -2: for a tree built through
     * art_insert, step-1 serialize failure means the buffer is too
     * small (keys are already capped at 1023 by art_insert). */
    uint8_t buf[16384];
    Arena arena; arena_init(&arena, buf, sizeof(buf));
    ART art; art_init(&art, &arena);
    assert(art_insert(&art, (const uint8_t*)"k", 1, (const uint8_t*)"v", 1, 42) == 0);
    uint8_t tiny[20];
    assert(art_serialize(&art, tiny, sizeof(tiny)) == -1);
    uint8_t ok[4096];
    assert(art_serialize(&art, ok, sizeof(ok)) > 0);
    printf("Passed.\n");
}

void test_step1_crdt_wal_carries_timestamp() {
    printf("Running test_step1_crdt_wal_carries_timestamp...\n");
    uint8_t bc[128]; size_t pc = 0;
    bc[pc++] = OP_PUT_CRDT;
    s1_u64(bc, &pc, 12345);
    s1_u32(bc, &pc, 2); memcpy(&bc[pc], "ck", 2); pc += 2;
    s1_u32(bc, &pc, 2); memcpy(&bc[pc], "cv", 2); pc += 2;
    bc[pc++] = OP_EXIT;

    uint8_t buf[4096]; Arena arena; arena_init(&arena, buf, sizeof(buf));
    ART art; art_init(&art, &arena);
    uint8_t wal[1024]; StorageDriver st = storage_memory_create(wal, sizeof(wal));
    VMContext vm; vm_init(&vm, &art, bc, pc, &st);
    assert(vm_execute(&vm) == 0);
    /* WAL record: opcode, timestamp (12345 LE), key_len, key, val_len, val */
    assert(wal[0] == OP_PUT_CRDT);
    assert(wal[1] == 0x39 && wal[2] == 0x30); /* 12345 = 0x3039 */
    uint8_t* out; uint32_t out_len;
    assert(art_search(&art, (const uint8_t*)"ck", 2, &out, &out_len) == 0);
    if (st.close) st.close(&st);
    printf("Passed.\n");
}

/* FNV-1a 64, same algorithm as art.c's static fnv1a_64 (not exported),
 * for hand-crafting v2 snapshots in the tests below. If this were
 * wrong, art_validate_snapshot's checksum check would fail the test. */
static uint64_t s1_fnv1a(const uint8_t* data, size_t len) {
    uint64_t h = 14695981039346656037ULL;
    for (size_t i = 0; i < len; i++) {
        h ^= data[i];
        h *= 1099511628211ULL;
    }
    return h;
}

/* Build a v2 snapshot with exactly the given entries (live or
 * tombstone) in the given order, with a correct checksum. Returns
 * the total length, or -1 on overflow. */
static int s1_build_v2(uint8_t* out, size_t out_cap,
                       const uint8_t* keys[], const uint32_t key_lens[],
                       const uint8_t* vals[], const uint32_t val_lens[],
                       const uint64_t timestamps[], const int is_tomb[],
                       uint32_t count) {
    size_t off = 0;
    if (out_cap < 20) return -1;
    out[off++] = 'Y'; out[off++] = 'W'; out[off++] = 'Z'; out[off++] = '6';
    s1_u32(out, &off, 1);
    s1_u32(out, &off, count);
    for (uint32_t i = 0; i < count; i++) {
        s1_u32(out, &off, key_lens[i]);
        memcpy(&out[off], keys[i], key_lens[i]); off += key_lens[i];
        if (is_tomb[i]) {
            s1_u32(out, &off, YOWZA_TOMBSTONE_FLAG);
        } else {
            s1_u32(out, &off, val_lens[i]);
            memcpy(&out[off], vals[i], val_lens[i]); off += val_lens[i];
        }
        s1_u64(out, &off, timestamps[i]);
    }
    if (off + 8 > out_cap) return -1;
    s1_u64(out, &off, s1_fnv1a(out, off));
    return (int)off;
}

void test_step1_restore_tombstone_lww_duplicates() {
    printf("Running test_step1_restore_tombstone_lww_duplicates...\n");
    uint8_t* out; uint32_t out_len;
    /* Order 1: live ts=200, then stale tombstone ts=50. The stale
     * tombstone must LOSE (guard in art_restore_tombstone), leaving
     * the live value — not overwrite it with a dead key. */
    {
        const uint8_t* keys[] = {(const uint8_t*)"dup", (const uint8_t*)"dup"};
        const uint32_t klens[] = {3, 3};
        const uint8_t* vals[] = {(const uint8_t*)"alive", NULL};
        const uint32_t vlens[] = {5, 0};
        const uint64_t tss[] = {200, 50};
        const int tombs[] = {0, 1};
        uint8_t snap[256];
        int len = s1_build_v2(snap, sizeof(snap), keys, klens, vals, vlens, tss, tombs, 2);
        assert(len > 0);
        uint8_t buf[16384]; Arena arena; arena_init(&arena, buf, sizeof(buf));
        ART art; art_init(&art, &arena);
        assert(art_deserialize(&art, snap, (uint32_t)len) == 0);
        assert(art_search(&art, (const uint8_t*)"dup", 3, &out, &out_len) == 0);
        assert(out_len == 5 && memcmp(out, "alive", 5) == 0);
    }
    /* Order 2: stale tombstone ts=50, then live ts=200. Must converge
     * to the same live result (commutativity). */
    {
        const uint8_t* keys[] = {(const uint8_t*)"dup", (const uint8_t*)"dup"};
        const uint32_t klens[] = {3, 3};
        const uint8_t* vals[] = {NULL, (const uint8_t*)"alive"};
        const uint32_t vlens[] = {0, 5};
        const uint64_t tss[] = {50, 200};
        const int tombs[] = {1, 0};
        uint8_t snap[256];
        int len = s1_build_v2(snap, sizeof(snap), keys, klens, vals, vlens, tss, tombs, 2);
        assert(len > 0);
        uint8_t buf[16384]; Arena arena; arena_init(&arena, buf, sizeof(buf));
        ART art; art_init(&art, &arena);
        assert(art_deserialize(&art, snap, (uint32_t)len) == 0);
        assert(art_search(&art, (const uint8_t*)"dup", 3, &out, &out_len) == 0);
        assert(out_len == 5 && memcmp(out, "alive", 5) == 0);
    }
    /* Tie orders: live ts=200 then tombstone ts=200, and tombstone
     * ts=200 then live ts=200 — both must converge to dead (the
     * delete wins the tie in both directions). */
    {
        const uint8_t* keys[] = {(const uint8_t*)"dup", (const uint8_t*)"dup"};
        const uint32_t klens[] = {3, 3};
        const uint8_t* vals[] = {(const uint8_t*)"alive", NULL};
        const uint32_t vlens[] = {5, 0};
        const uint64_t tss[] = {200, 200};
        const int tombs[] = {0, 1};
        uint8_t snap[256];
        int len = s1_build_v2(snap, sizeof(snap), keys, klens, vals, vlens, tss, tombs, 2);
        assert(len > 0);
        uint8_t buf[16384]; Arena arena; arena_init(&arena, buf, sizeof(buf));
        ART art; art_init(&art, &arena);
        assert(art_deserialize(&art, snap, (uint32_t)len) == 0);
        assert(art_search(&art, (const uint8_t*)"dup", 3, &out, &out_len) == -1);
    }
    {
        const uint8_t* keys[] = {(const uint8_t*)"dup", (const uint8_t*)"dup"};
        const uint32_t klens[] = {3, 3};
        const uint8_t* vals[] = {NULL, (const uint8_t*)"alive"};
        const uint32_t vlens[] = {0, 5};
        const uint64_t tss[] = {200, 200};
        const int tombs[] = {1, 0};
        uint8_t snap[256];
        int len = s1_build_v2(snap, sizeof(snap), keys, klens, vals, vlens, tss, tombs, 2);
        assert(len > 0);
        uint8_t buf[16384]; Arena arena; arena_init(&arena, buf, sizeof(buf));
        ART art; art_init(&art, &arena);
        assert(art_deserialize(&art, snap, (uint32_t)len) == 0);
        assert(art_search(&art, (const uint8_t*)"dup", 3, &out, &out_len) == -1);
    }
    printf("Passed.\n");
}

void test_step1_restore_oversized_snapshot_preserves_data() {
    printf("Running test_step1_restore_oversized_snapshot_preserves_data...\n");
    /* Source DB (16 KiB arena) with 20 x 400-byte values = 8000
     * value bytes total. (A single 8000-byte value cannot be put
     * via yowza_db_put: its fixed 1024-byte bytecode buffer rejects
     * val_len > 1024, so many medium values are the way to build an
     * oversized snapshot through the C API.) */
    uint8_t mem1[16384], wal1[16384], snap[16384];
    StorageDriver st1 = storage_memory_create(wal1, sizeof(wal1));
    YowzaDB db1; assert(yowza_db_init(&db1, mem1, sizeof(mem1), st1) == 0);
    uint8_t bigval[400]; memset(bigval, 'B', sizeof(bigval));
    for (int i = 0; i < 20; i++) {
        char key[8]; snprintf(key, sizeof(key), "k%02d", i);
        assert(yowza_db_put(&db1, key, bigval, sizeof(bigval)) == 0);
    }
    int snap_len = yowza_db_snapshot_create(&db1, snap, sizeof(snap));
    assert(snap_len > 0);
    if (db1.storage.close) db1.storage.close(&db1.storage);
    /* Target DB with only a 4 KiB arena and live data of its own. */
    uint8_t mem2[4096], wal2[1024];
    StorageDriver st2 = storage_memory_create(wal2, sizeof(wal2));
    YowzaDB db2; assert(yowza_db_init(&db2, mem2, sizeof(mem2), st2) == 0);
    assert(yowza_db_put(&db2, "keep", "K", 1) == 0);
    /* Restore must FAIL (values alone exceed the arena) WITHOUT
     * wiping the live data: "keep" must still be readable. */
    assert(yowza_db_snapshot_restore(&db2, snap, (size_t)snap_len) == -1);
    void* val; size_t val_len;
    assert(yowza_db_get(&db2, "keep", &val, &val_len) == 0);
    assert(val_len == 1 && memcmp(val, "K", 1) == 0);
    assert(yowza_db_get(&db2, "k00", &val, &val_len) == -1);
    if (db2.storage.close) db2.storage.close(&db2.storage);
    /* And the same snapshot restores fine into a 16 KiB arena. */
    uint8_t mem3[16384], wal3[1024];
    StorageDriver st3 = storage_memory_create(wal3, sizeof(wal3));
    YowzaDB db3; assert(yowza_db_init(&db3, mem3, sizeof(mem3), st3) == 0);
    assert(yowza_db_snapshot_restore(&db3, snap, (size_t)snap_len) == 0);
    assert(yowza_db_get(&db3, "k00", &val, &val_len) == 0);
    assert(val_len == 400 && memcmp(val, bigval, 400) == 0);
    assert(yowza_db_get(&db3, "k19", &val, &val_len) == 0);
    assert(val_len == 400);
    if (db3.storage.close) db3.storage.close(&db3.storage);
    printf("Passed.\n");
}
