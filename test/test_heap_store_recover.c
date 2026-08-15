/* Heap-store recovery regression tests (parallel-timeline patch).
 * The wasm heap store's enumerate hook was NULL, so O2's wired
 * segdb_recover could not prove a heap store virgin: a shard
 * that flushed empty (empty flush publishes nothing) failed
 * reopen with rc=-1 instead of recovering empty, breaking every
 * multi-shard DB with a keyless shard (see
 * TestPersistedWALReplayNoFlush). hp_enumerate (yowza_segdb.c)
 * closes that gap; these tests pin the heap-store recover paths
 * through the yowza_segdb_* export surface: virgin heap recovers
 * empty, and a flushed heap copied to a fresh state recovers its
 * manifest. Heap-store triple loss (pointers deleted, segment
 * surviving) is pinned on the shared logic by
 * test_recovery_wire_triple_loss_fails on the mem store — the
 * only difference is the vtable, and enumerate now makes the
 * heap store behave the same way there. SegDB structs are
 * ~1.2 MB: static storage, never on the stack. */
#include <stdio.h>
#include <assert.h>
#include <string.h>
#include "segdb.h"

uint64_t yowza_segdb_state_size(void);
int yowza_segdb_init(SegDB* state, uint8_t* arena_buf, uint64_t arena_size,
                     uint8_t* wal_buf, uint32_t wal_size,
                     uint8_t* heap_buf, uint64_t heap_size);
int yowza_segdb_put(SegDB* state, const uint8_t* key, uint32_t key_len,
                    const uint8_t* val, uint32_t val_len);
int yowza_segdb_flush(SegDB* state);
int yowza_segdb_recover(SegDB* state);

static SegDB hs_db1, hs_db2;
static uint8_t hs_arena1[262144], hs_wal1[65536], hs_heap1[4194304];
static uint8_t hs_arena2[262144], hs_wal2[65536], hs_heap2[4194304];

void test_heap_store_recover_virgin() {
    printf("Running test_heap_store_recover_virgin...\n");
    memset(hs_heap1, 0, sizeof(hs_heap1));
    assert(yowza_segdb_init(&hs_db1, hs_arena1, sizeof(hs_arena1),
                            hs_wal1, sizeof(hs_wal1),
                            hs_heap1, sizeof(hs_heap1)) == 0);
    /* Fresh heap store: no objects at all (no flush ever ran).
     * Recover must open empty, not fail ambiguous. */
    assert(yowza_segdb_recover(&hs_db1) == 0);
    assert(hs_db1.manifest.segment_count == 0);
    assert(hs_db1.has_manifest == 0);
    printf("Passed.\n");
}

void test_heap_store_recover_flushed_copy() {
    printf("Running test_heap_store_recover_flushed_copy...\n");
    memset(hs_heap1, 0, sizeof(hs_heap1));
    assert(yowza_segdb_init(&hs_db1, hs_arena1, sizeof(hs_arena1),
                            hs_wal1, sizeof(hs_wal1),
                            hs_heap1, sizeof(hs_heap1)) == 0);
    assert(yowza_segdb_put(&hs_db1, (const uint8_t*)"segkey", 6,
                           (const uint8_t*)"old", 3) == 0);
    assert(yowza_segdb_flush(&hs_db1) == 0);
    /* Host checkpoint restore: copy the heap image to a fresh
     * state, exactly as PersistedSegDB does on reopen. */
    memcpy(hs_heap2, hs_heap1, sizeof(hs_heap1));
    assert(yowza_segdb_init(&hs_db2, hs_arena2, sizeof(hs_arena2),
                            hs_wal2, sizeof(hs_wal2),
                            hs_heap2, sizeof(hs_heap2)) == 0);
    assert(yowza_segdb_recover(&hs_db2) == 0);
    assert(hs_db2.manifest.segment_count == 1);
    printf("Passed.\n");
}
