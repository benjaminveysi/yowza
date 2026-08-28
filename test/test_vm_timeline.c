/* Phase D tests: VMContext timeline mode (native C API).
 * Self-registers through a constructor (same pattern as
 * test_tx_shadow.c): test/main.c is owned by another
 * agent, so this file runs when test_runner starts.
 *
 * Proven here, at the ART level:
 *  - Timeline PUT/DEL stage into the write set ONLY:
 *    the ART has no terminal for a timeline-put key and
 *    the WAL offset does not move.
 *  - Timeline GET reads the write set first, then a
 *    base value whose version is at most the snapshot.
 *  - Timeline GET of a key overwritten after the
 *    snapshot FAILS with -11 instead of returning the
 *    new main-head value or guessing the old one (the
 *    ART-only layer cannot tell creation-after from
 *    overwrite-after; the Go TimelineHandle over SegDB
 *    snapshots is the full-history path).
 *  - A timeline TX commits into the write set, not the
 *    ART/WAL, and TX_GET reads its own writes.
 *  - OP_SCAN under a timeline returns -11 (fail closed:
 *    no snapshot scan exists on the ART, and a
 *    main-head scan under a timeline name would violate
 *    isolation).
 *  - The host drain/re-stage protocol works across
 *    vm_init calls (vm_init clears the timeline, so the
 *    host drains with vm_timeline_write_at and re-stages
 *    with vm_timeline_stage after the next vm_init).
 * The wasm glue for this mode (a yowza.c export) is
 * NOT in this phase, so wasm programs use the Go
 * TimelineVM; this test proves the C mode itself, and
 * the Phase D notes say the glue is outstanding rather
 * than claiming wasm C-VM timeline execution.
 */

#include <assert.h>
#include <stdio.h>
#include <string.h>

#include "vm.h"

static void tl_w32(uint8_t* buf, size_t* off, uint32_t v) {
    buf[(*off)++] = (uint8_t)(v & 0xFF);
    buf[(*off)++] = (uint8_t)((v >> 8) & 0xFF);
    buf[(*off)++] = (uint8_t)((v >> 16) & 0xFF);
    buf[(*off)++] = (uint8_t)((v >> 24) & 0xFF);
}

static void tl_w64(uint8_t* buf, size_t* off, uint64_t v) {
    int i;
    for (i = 0; i < 8; i++) buf[(*off)++] = (uint8_t)((v >> (i * 8)) & 0xFF);
}

static void tl_put_op(uint8_t* buf, size_t* off, const char* k, const char* v) {
    buf[(*off)++] = OP_PUT;
    tl_w32(buf, off, (uint32_t)strlen(k));
    memcpy(&buf[*off], k, strlen(k));
    *off += strlen(k);
    tl_w32(buf, off, (uint32_t)strlen(v));
    memcpy(&buf[*off], v, strlen(v));
    *off += strlen(v);
}

static void tl_get_op(uint8_t* buf, size_t* off, const char* k) {
    buf[(*off)++] = OP_GET;
    tl_w32(buf, off, (uint32_t)strlen(k));
    memcpy(&buf[*off], k, strlen(k));
    *off += strlen(k);
}

static void tl_del_op(uint8_t* buf, size_t* off, const char* k) {
    buf[(*off)++] = OP_DEL;
    tl_w32(buf, off, (uint32_t)strlen(k));
    memcpy(&buf[*off], k, strlen(k));
    *off += strlen(k);
}

/* Run one program under the timeline and return
 * vm_execute's result, leaving vm.response_* set. */
static int tl_run(VMContext* vm, ART* art, uint8_t* prog, size_t len,
                  StorageDriver* storage, uint64_t snap) {
    vm_init(vm, art, prog, len, storage);
    vm_set_timeline(vm, snap);
    return vm_execute(vm);
}

static void test_vm_timeline_mode(void) {
    printf("Running test_vm_timeline_mode...\n");
    uint8_t arena_buf[65536];
    uint8_t wal_buf[16384];
    Arena arena;
    ART art;
    StorageDriver storage;
    VMContext vm;
    uint64_t snap;
    uint32_t wal_before;
    uint8_t prog[512];
    size_t pc;

    arena_init(&arena, arena_buf, sizeof(arena_buf));
    art_init(&art, &arena);
    storage = storage_memory_create(wal_buf, sizeof(wal_buf));

    /* Base state, then the snapshot point. */
    assert(art_insert(&art, (const uint8_t*)"stable:k", 8, (const uint8_t*)"stable", 6, 0) == 0);
    assert(art_insert(&art, (const uint8_t*)"base:k", 6, (const uint8_t*)"old", 3, 0) == 0);
    snap = art.commit_seq;
    /* Main head advances past the snapshot. */
    assert(art_insert(&art, (const uint8_t*)"base:k", 6, (const uint8_t*)"new", 3, 0) == 0);
    assert(art_insert(&art, (const uint8_t*)"main:only", 9, (const uint8_t*)"m", 1, 0) == 0);
    wal_before = storage_memory_get_offset(&storage);

    /* 1. Base value at/under the snapshot reads fine. */
    pc = 0;
    tl_get_op(prog, &pc, "stable:k");
    prog[pc++] = OP_EXIT;
    assert(tl_run(&vm, &art, prog, pc, &storage, snap) == 0);
    assert(vm.response_ptr != NULL && vm.response_len == 6 &&
           memcmp(vm.response_ptr, "stable", 6) == 0);

    /* 2. Overwritten-after-snapshot fails -11, never "new". */
    pc = 0;
    tl_get_op(prog, &pc, "base:k");
    prog[pc++] = OP_EXIT;
    assert(tl_run(&vm, &art, prog, pc, &storage, snap) == -11);

    /* 3. Created-after-snapshot is likewise ambiguous here. */
    pc = 0;
    tl_get_op(prog, &pc, "main:only");
    prog[pc++] = OP_EXIT;
    assert(tl_run(&vm, &art, prog, pc, &storage, snap) == -11);

    /* 4. Timeline PUT stages only: GET sees it, ART and
     * WAL do not. */
    pc = 0;
    tl_put_op(prog, &pc, "tl:x", "X");
    tl_get_op(prog, &pc, "tl:x");
    prog[pc++] = OP_EXIT;
    assert(tl_run(&vm, &art, prog, pc, &storage, snap) == 0);
    assert(vm.response_ptr != NULL && vm.response_len == 1 &&
           memcmp(vm.response_ptr, "X", 1) == 0);
    assert(art_find_terminal(&art, (const uint8_t*)"tl:x", 4) == NULL);
    assert(storage_memory_get_offset(&storage) == wal_before);
    assert(vm_timeline_write_count(&vm) == 1);
    {
        const uint8_t* k = NULL;
        uint32_t klen = 0;
        const uint8_t* v = NULL;
        uint32_t vlen = 0;
        int del = -1;
        assert(vm_timeline_write_at(&vm, 0, &k, &klen, &v, &vlen, &del) == 0);
        assert(klen == 4 && memcmp(k, "tl:x", 4) == 0);
        assert(del == 0 && vlen == 1 && memcmp(v, "X", 1) == 0);
    }

    /* 5. Timeline DEL stages a tombstone: GET after DEL
     * is not-found, ART still untouched. */
    pc = 0;
    tl_put_op(prog, &pc, "tl:y", "Y");
    tl_del_op(prog, &pc, "tl:y");
    tl_get_op(prog, &pc, "tl:y");
    prog[pc++] = OP_EXIT;
    assert(tl_run(&vm, &art, prog, pc, &storage, snap) == 0);
    assert(vm.response_ptr == NULL && vm.response_len == 0);
    assert(art_find_terminal(&art, (const uint8_t*)"tl:y", 4) == NULL);
    assert(storage_memory_get_offset(&storage) == wal_before);
    /* PUT then DEL merge in place: one tombstone entry. */
    assert(vm_timeline_write_count(&vm) == 1);

    /* 6. Host protocol: drain, re-init, re-stage, read. */
    {
        /* Drain program 5's write set into locals. */
        char dk[2][16];
        uint32_t dklen[2];
        char dv[2][16];
        uint32_t dvlen[2];
        int ddel[2];
        int i, n = vm_timeline_write_count(&vm);
        assert(n == 1);
        for (i = 0; i < n; i++) {
            const uint8_t* k = NULL;
            uint32_t klen = 0;
            const uint8_t* v = NULL;
            uint32_t vlen = 0;
            int del = 0;
            assert(vm_timeline_write_at(&vm, (uint32_t)i, &k, &klen, &v, &vlen, &del) == 0);
            memcpy(dk[i], k, klen);
            dklen[i] = klen;
            if (vlen > 0) memcpy(dv[i], v, vlen);
            dvlen[i] = vlen;
            ddel[i] = del;
        }
        /* Fresh program, re-staged writes. */
        vm_init(&vm, &art, prog, 0, &storage);
        vm_set_timeline(&vm, snap);
        for (i = 0; i < n; i++) {
            assert(vm_timeline_stage(&vm, (const uint8_t*)dk[i], dklen[i],
                                     (const uint8_t*)dv[i], dvlen[i], ddel[i], 0) == 0);
        }
        pc = 0;
        tl_get_op(prog, &pc, "tl:y");
        prog[pc++] = OP_EXIT;
        /* Re-stage cleared the program buffer pointer in
         * vm (vm_init above took prog, len 0); set the
         * real program now without re-initing. */
        vm.bytecode = prog;
        vm.length = pc;
        vm.pc = 0;
        assert(vm_execute(&vm) == 0);
        assert(vm.response_ptr == NULL); /* tombstone hides */
    }

    /* 7. Timeline TX: commit stages, TX_GET reads own
     * writes, ART/WAL untouched. */
    pc = 0;
    prog[pc++] = OP_TX_BEGIN;
    tl_w64(prog, &pc, 77);
    tl_w64(prog, &pc, 1);
    tl_w32(prog, &pc, 0);
    tl_w64(prog, &pc, 1);
    prog[pc++] = OP_TX_PUT;
    prog[pc++] = 0; /* flags */
    tl_w32(prog, &pc, 4);
    memcpy(&prog[pc], "tx:k", 4);
    pc += 4;
    tl_w32(prog, &pc, 1);
    memcpy(&prog[pc], "V", 1);
    pc += 1;
    prog[pc++] = OP_TX_GET;
    tl_w32(prog, &pc, 4);
    memcpy(&prog[pc], "tx:k", 4);
    pc += 4;
    prog[pc++] = OP_TX_COMMIT;
    prog[pc++] = OP_EXIT;
    assert(tl_run(&vm, &art, prog, pc, &storage, snap) == 0);
    assert(art_find_terminal(&art, (const uint8_t*)"tx:k", 4) == NULL);
    assert(storage_memory_get_offset(&storage) == wal_before);
    /* The staged tx write is drainable. */
    {
        int found = 0, i, n = vm_timeline_write_count(&vm);
        for (i = 0; i < n; i++) {
            const uint8_t* k = NULL;
            uint32_t klen = 0;
            const uint8_t* v = NULL;
            uint32_t vlen = 0;
            int del = 0;
            assert(vm_timeline_write_at(&vm, (uint32_t)i, &k, &klen, &v, &vlen, &del) == 0);
            if (klen == 4 && memcmp(k, "tx:k", 4) == 0 && del == 0 &&
                vlen == 1 && memcmp(v, "V", 1) == 0) found = 1;
        }
        assert(found);
    }

    /* 8. SCAN under a timeline fails closed (-11). */
    pc = 0;
    prog[pc++] = OP_SCAN;
    tl_w32(prog, &pc, 0);
    tl_w32(prog, &pc, 64);
    prog[pc++] = OP_EXIT;
    assert(tl_run(&vm, &art, prog, pc, &storage, snap) == -11);

    /* 9. No timeline: the normal path is unchanged. */
    pc = 0;
    tl_put_op(prog, &pc, "plain:k", "P");
    prog[pc++] = OP_EXIT;
    vm_init(&vm, &art, prog, pc, &storage);
    assert(vm_execute(&vm) == 0);
    {
        uint8_t* out = NULL;
        uint32_t out_len = 0;
        assert(art_search(&art, (const uint8_t*)"plain:k", 7, &out, &out_len) == 0);
        assert(out_len == 1 && memcmp(out, "P", 1) == 0);
    }
    assert(storage_memory_get_offset(&storage) > wal_before);

    if (storage.close) storage.close(&storage);
    printf("Passed.\n");
}

__attribute__((constructor)) static void test_vm_timeline_register(void) {
    test_vm_timeline_mode();
}
