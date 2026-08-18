/* Patch-1 integration tests: segdb_get / yowza_segdb_get must
 * return -4 (never -1) for segment lookup errors, so the Go
 * boundary can no longer read an I/O error as absence.
 * Self-registers through a constructor (same pattern as
 * test_axis_c.c): test/main.c is not edited.
 *
 * Proven here (direct SegDB values are capped at 1024 by
 * segdb_put, so the get_buf-overflow route cannot be
 * reached through Put — the error route is exercised
 * through a corrupted segment instead):
 *  1. A small flushed value reads back (0); a genuinely
 *     absent key returns -1.
 *  2. yowza_segdb_get with an out_cap smaller than the
 *     value returns -4 (was -1: out-capacity overflow
 *     used to collapse into absence); adequate cap is 0.
 *  3. After flipping one byte inside a segment block, the
 *     live segdb_get returns -4 (candidate-block checksum
 *     failure), not -1 and not the stale value.
 *  4. The wrapper passes that -4 through with an adequate
 *     cap (the pass-through line itself, not just the
 *     out-cap overflow line).
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <stdint.h>

#include "segdb.h"
#include "storage2.h"

/* yowza_segdb_get has no public header; it is linked from
 * src/yowza_segdb.c into the test binary. Declare it here
 * rather than inventing a header. */
extern int yowza_segdb_get(SegDB* state, const uint8_t* key, uint32_t key_len,
                           uint8_t* out, uint32_t out_cap, uint32_t* out_len);

static SegDB p1_db;
static uint8_t p1_arena[262144];
static uint8_t p1_wal[262144];

static void p1_flip(YowzaStore* st, const char* obj, uint64_t off) {
    uint8_t b = 0;
    assert(st->vt->read_at(st->state, obj, off, &b, 1) == 0);
    b = (uint8_t)(b ^ 0xFF);
    assert(st->vt->write_at(st->state, obj, off, &b, 1) == 0);
}

static void test_patch1_geterr(void) {
    static uint8_t small_out[16];
    char seg_obj[128];
    uint8_t* out = NULL;
    uint32_t len = 0, out_len = 0;
    YowzaStore* st;
    printf("Running test_patch1_geterr...\n");
    st = yowza_store_mem_create();
    assert(st != NULL);
    assert(segdb_init(&p1_db, st, p1_arena, sizeof(p1_arena), p1_wal, sizeof(p1_wal)) == 0);
    assert(segdb_put(&p1_db, (const uint8_t*)"small", 5, (const uint8_t*)"sv", 2) == 0);
    assert(segdb_put(&p1_db, (const uint8_t*)"other", 5, (const uint8_t*)"ov", 2) == 0);
    assert(segdb_flush(&p1_db) == 0);
    assert(p1_db.manifest.segment_count >= 1);
    assert(strlen(p1_db.manifest.segments[0].obj) < sizeof(seg_obj));
    strcpy(seg_obj, p1_db.manifest.segments[0].obj);
    /* Baseline: present -> 0, absent -> -1. */
    assert(segdb_get(&p1_db, (const uint8_t*)"small", 5, &out, &len) == 0);
    assert(len == 2 && memcmp(out, "sv", 2) == 0);
    assert(segdb_get(&p1_db, (const uint8_t*)"nope", 4, &out, &len) == -1);
    /* Wrapper out_cap overflow is -4, adequate cap is 0. */
    assert(yowza_segdb_get(&p1_db, (const uint8_t*)"small", 5, small_out, 1, &out_len) == -4);
    assert(yowza_segdb_get(&p1_db, (const uint8_t*)"small", 5, small_out, sizeof(small_out), &out_len) == 0);
    assert(out_len == 2 && memcmp(small_out, "sv", 2) == 0);
    assert(yowza_segdb_get(&p1_db, (const uint8_t*)"nope", 4, small_out, sizeof(small_out), &out_len) == -1);
    /* Corrupt one byte inside a segment block (offset 100,
     * past the 64-byte header, inside entry bytes). The
     * live lookup must fail with -4, never -1, and the
     * wrapper must pass that -4 through. */
    p1_flip(st, seg_obj, 100);
    assert(segdb_get(&p1_db, (const uint8_t*)"small", 5, &out, &len) == -4);
    assert(yowza_segdb_get(&p1_db, (const uint8_t*)"small", 5, small_out, sizeof(small_out), &out_len) == -4);
    segdb_close(&p1_db);
    yowza_store_close(st);
    printf("Passed.\n");
}

__attribute__((constructor)) static void test_patch1_geterr_register(void) {
    test_patch1_geterr();
}
