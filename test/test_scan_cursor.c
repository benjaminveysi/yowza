/* Reliability stage item 03 tests: cursor (continuation-key)
 * scan and the heap hash index.
 *
 * OWNERSHIP NOTE (same pattern as test_stream_snapshot.c):
 * test/main.c is owned by another agent, so this file
 * self-registers through a constructor: the tests below run
 * when the test_runner binary starts, before main's suite.
 * If main.c later gains extern declarations + calls for these
 * functions, delete the constructor at the bottom to avoid
 * running them twice.
 */
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include "segdb.h"

/* Heap-index exports (plain functions in the native build). */
extern void yowza_segdb_heap_stats(uint8_t* out);
extern void yowza_segdb_heap_stats_reset(void);
extern int yowza_segdb_heap_index_rebuild(void);
extern int yowza_segdb_heap_index_corrupt_for_test(const char* obj);
extern int yowza_segdb_heap_index_drop_for_test(const char* obj);
extern int yowza_segdb_init(SegDB* state, uint8_t* arena_buf, uint64_t arena_size,
                            uint8_t* wal_buf, uint32_t wal_size,
                            uint8_t* heap_buf, uint64_t heap_size);

static SegDB g_sc_db;
static uint8_t g_sc_arena[1048576];
static uint8_t g_sc_wal[262144];

static void sc_put(SegDB* db, const char* k, const char* v) {
    int rc = segdb_put(db, (const uint8_t*)k, (uint32_t)strlen(k),
                       (const uint8_t*)v, (uint32_t)strlen(v));
    assert(rc == 0);
}

static void sc_del(SegDB* db, const char* k) {
    int rc = segdb_delete(db, (const uint8_t*)k, (uint32_t)strlen(k));
    assert(rc == 0);
}

static void sc_key(char* buf, uint32_t idx) {
    int n = snprintf(buf, 32, "k:%08u", idx);
    assert(n > 0);
}

static void sc_val(char* buf, uint32_t idx) {
    int n = snprintf(buf, 32, "v:%u", idx);
    assert(n > 0);
}

static int sc_decode(const uint8_t* buf, uint32_t total, uint32_t* off,
                     char* kbuf, uint32_t kcap, uint32_t* klen,
                     char* vbuf, uint32_t vcap, uint32_t* vlen) {
    uint32_t kl, vl, i;
    if (*off + 8 > total) return -1;
    kl = (uint32_t)buf[*off] | ((uint32_t)buf[*off + 1] << 8) |
         ((uint32_t)buf[*off + 2] << 16) | ((uint32_t)buf[*off + 3] << 24);
    *off += 4;
    if (*off + kl + 4 > total || kl >= kcap) return -1;
    for (i = 0; i < kl; i++) kbuf[i] = (char)buf[*off + i];
    kbuf[kl] = '\0';
    *klen = kl;
    *off += kl;
    vl = (uint32_t)buf[*off] | ((uint32_t)buf[*off + 1] << 8) |
         ((uint32_t)buf[*off + 2] << 16) | ((uint32_t)buf[*off + 3] << 24);
    *off += 4;
    if (*off + vl > total || vl >= vcap) return -1;
    for (i = 0; i < vl; i++) vbuf[i] = (char)buf[*off + i];
    vbuf[vl] = '\0';
    *vlen = vl;
    *off += vl;
    return 0;
}

/* One collected row (copies, so pages can be compared). */
typedef struct {
    char key[64];
    char val[64];
} ScRow;

#define SC_MAX_ROWS 10000
static ScRow g_sc_rows_a[SC_MAX_ROWS];
static ScRow g_sc_rows_b[SC_MAX_ROWS];

/* Collect a whole result via the old skip path. Returns count. */
static uint32_t sc_collect_skip(SegDB* db, uint64_t snap, uint32_t limit) {
    static uint8_t out[65536];
    uint64_t skip = 0;
    uint32_t count = 0;
    for (;;) {
        uint32_t cnt = 0;
        uint32_t off = 0;
        uint32_t j;
        int rc = segdb_scan_page(db, (const uint8_t*)"k:", 2, snap, skip,
                                 limit, out, (uint32_t)sizeof(out), &cnt);
        assert(rc == 0);
        if (cnt == 0) break;
        for (j = 0; j < cnt; j++) {
            uint32_t kl, vl;
            assert(count + j < SC_MAX_ROWS);
            assert(sc_decode(out, (uint32_t)sizeof(out), &off,
                             g_sc_rows_a[count + j].key, sizeof(g_sc_rows_a[count + j].key), &kl,
                             g_sc_rows_a[count + j].val, sizeof(g_sc_rows_a[count + j].val), &vl) == 0);
        }
        count += cnt;
        skip += cnt;
        if (cnt < limit) break;
    }
    return count;
}

/* Collect a whole result via the cursor path. Returns count.
 * has_more/next_key resume is exercised on every page. */
static uint32_t sc_collect_cursor(SegDB* db, uint64_t snap, uint32_t limit) {
    static uint8_t out[65536];
    uint8_t cur[1024];
    uint32_t cur_len = 0;
    int has_cur = 0;
    uint32_t count = 0;
    for (;;) {
        uint32_t cnt = 0;
        uint32_t off = 0;
        uint32_t j;
        uint8_t nk[1024];
        uint32_t nk_len = 0;
        uint32_t has_more = 0;
        int rc = segdb_scan_cursor(db, (const uint8_t*)"k:", 2, snap,
                                   cur, cur_len, has_cur, limit,
                                   out, (uint32_t)sizeof(out), &cnt,
                                   nk, (uint32_t)sizeof(nk), &nk_len, &has_more);
        assert(rc == 0);
        if (cnt == 0) {
            assert(has_more == 0);
            break;
        }
        for (j = 0; j < cnt; j++) {
            uint32_t kl, vl;
            assert(count + j < SC_MAX_ROWS);
            assert(sc_decode(out, (uint32_t)sizeof(out), &off,
                             g_sc_rows_b[count + j].key, sizeof(g_sc_rows_b[count + j].key), &kl,
                             g_sc_rows_b[count + j].val, sizeof(g_sc_rows_b[count + j].val), &vl) == 0);
        }
        count += cnt;
        /* Resume key must be exactly the last row's key. */
        assert(nk_len == strlen(g_sc_rows_b[count - 1].key) &&
               memcmp(nk, g_sc_rows_b[count - 1].key, nk_len) == 0);
        memcpy(cur, nk, nk_len);
        cur_len = nk_len;
        has_cur = 1;
        if (!has_more) break;
    }
    return count;
}

static void sc_assert_rows_equal(uint32_t na, uint32_t nb) {
    uint32_t i;
    assert(na == nb);
    for (i = 0; i < na; i++) {
        assert(strcmp(g_sc_rows_a[i].key, g_sc_rows_b[i].key) == 0);
        assert(strcmp(g_sc_rows_a[i].val, g_sc_rows_b[i].val) == 0);
    }
}

/* Build a DB with 500 keys across many small segments, with
 * updates and flushed tombstones. Returns the snapshot after
 * the build (all rows visible). */
static uint64_t sc_build_mixed(SegDB* db) {
    uint32_t i;
    for (i = 0; i < 500; i++) {
        char kb[32], vb[32];
        sc_key(kb, i);
        sc_val(vb, i);
        sc_put(db, kb, vb);
        if ((i + 1) % 37 == 0) assert(segdb_flush(db) == 0);
    }
    assert(segdb_flush(db) == 0);
    /* Updates + deletes, flushed as their own segments. */
    for (i = 0; i < 500; i += 23) {
        char kb[32];
        sc_key(kb, i);
        sc_put(db, kb, "v:UPDATED");
    }
    assert(segdb_flush(db) == 0);
    for (i = 0; i < 500; i += 17) {
        char kb[32];
        sc_key(kb, i);
        sc_del(db, kb);
    }
    assert(segdb_flush(db) == 0);
    return segdb_snapshot_seq(db);
}

void test_scan_cursor_equals_skip() {
    YowzaStore* st;
    uint64_t snap;
    uint32_t na, nb;
    printf("Running test_scan_cursor_equals_skip...\n");
    fflush(stdout);
    st = yowza_store_mem_create();
    assert(st != NULL);
    assert(segdb_init(&g_sc_db, st, g_sc_arena, sizeof(g_sc_arena),
                      g_sc_wal, sizeof(g_sc_wal)) == 0);
    snap = sc_build_mixed(&g_sc_db);
    /* Row-for-row equality at several page sizes, including
     * sizes unaligned with segment boundaries (37-row
     * segments) and a 1-row page. */
    na = sc_collect_skip(&g_sc_db, snap, 64);
    nb = sc_collect_cursor(&g_sc_db, snap, 64);
    sc_assert_rows_equal(na, nb);
    assert(na > 400 && na < 500); /* deletes removed some rows */
    na = sc_collect_skip(&g_sc_db, snap, 13);
    nb = sc_collect_cursor(&g_sc_db, snap, 13);
    sc_assert_rows_equal(na, nb);
    na = sc_collect_skip(&g_sc_db, snap, 1);
    nb = sc_collect_cursor(&g_sc_db, snap, 1);
    sc_assert_rows_equal(na, nb);
    /* Prefix filtering through the cursor path. */
    {
        static uint8_t out[65536];
        uint8_t nk[1024];
        uint32_t nk_len = 0, has_more = 0, cnt = 0;
        int rc = segdb_scan_cursor(&g_sc_db, (const uint8_t*)"k:00000", 7, snap,
                                   NULL, 0, 0, 100,
                                   out, (uint32_t)sizeof(out), &cnt,
                                   nk, (uint32_t)sizeof(nk), &nk_len, &has_more);
        assert(rc == 0 && cnt == 100 && has_more == 1);
    }
    segdb_close(&g_sc_db);
    yowza_store_close(st);
    printf("Passed.\n"); fflush(stdout);
}

void test_scan_cursor_snapshot_visibility() {
    YowzaStore* st;
    uint64_t snap_old, snap_new;
    uint32_t na, nb;
    printf("Running test_scan_cursor_snapshot_visibility...\n");
    fflush(stdout);
    st = yowza_store_mem_create();
    assert(st != NULL);
    assert(segdb_init(&g_sc_db, st, g_sc_arena, sizeof(g_sc_arena),
                      g_sc_wal, sizeof(g_sc_wal)) == 0);
    sc_put(&g_sc_db, "k:a", "old-a");
    sc_put(&g_sc_db, "k:b", "old-b");
    sc_put(&g_sc_db, "k:c", "old-c");
    assert(segdb_flush(&g_sc_db) == 0);
    snap_old = segdb_snapshot_seq(&g_sc_db);
    /* Writes after the snapshot: a new key, an update, and a
     * delete (flushed tombstone) — all invisible at snap_old. */
    sc_put(&g_sc_db, "k:d", "new-d");
    sc_put(&g_sc_db, "k:a", "new-a");
    sc_del(&g_sc_db, "k:b");
    assert(segdb_flush(&g_sc_db) == 0);
    snap_new = segdb_snapshot_seq(&g_sc_db);
    na = sc_collect_skip(&g_sc_db, snap_old, 2);
    nb = sc_collect_cursor(&g_sc_db, snap_old, 2);
    sc_assert_rows_equal(na, nb);
    assert(na == 3);
    assert(strcmp(g_sc_rows_a[0].val, "old-a") == 0);
    assert(strcmp(g_sc_rows_a[1].val, "old-b") == 0);
    na = sc_collect_skip(&g_sc_db, snap_new, 2);
    nb = sc_collect_cursor(&g_sc_db, snap_new, 2);
    sc_assert_rows_equal(na, nb);
    assert(na == 3); /* new-a, old-c, new-d; k:b tombstoned */
    assert(strcmp(g_sc_rows_a[0].val, "new-a") == 0);
    /* Hot-only (unflushed) writes after the snapshot are
     * invisible at the old snapshot through the cursor path. */
    sc_put(&g_sc_db, "k:e", "hot-e");
    nb = sc_collect_cursor(&g_sc_db, snap_old, 100);
    assert(nb == 3);
    segdb_close(&g_sc_db);
    yowza_store_close(st);
    printf("Passed.\n"); fflush(stdout);
}

void test_scan_cursor_small_buffer_and_limit0() {
    YowzaStore* st;
    uint64_t snap;
    static uint8_t tiny[4];
    uint8_t nk[1024];
    uint32_t nk_len = 0, has_more = 0, cnt = 0;
    int rc;
    printf("Running test_scan_cursor_small_buffer_and_limit0...\n");
    fflush(stdout);
    st = yowza_store_mem_create();
    assert(st != NULL);
    assert(segdb_init(&g_sc_db, st, g_sc_arena, sizeof(g_sc_arena),
                      g_sc_wal, sizeof(g_sc_wal)) == 0);
    sc_put(&g_sc_db, "k:x", "vx");
    sc_put(&g_sc_db, "k:y", "vy");
    assert(segdb_flush(&g_sc_db) == 0);
    snap = segdb_snapshot_seq(&g_sc_db);
    /* -3: a row does not fit the tiny buffer; retry with the
     * same (input) cursor and a large buffer succeeds. */
    rc = segdb_scan_cursor(&g_sc_db, (const uint8_t*)"k:", 2, snap,
                           NULL, 0, 0, 10,
                           tiny, (uint32_t)sizeof(tiny), &cnt,
                           nk, (uint32_t)sizeof(nk), &nk_len, &has_more);
    assert(rc == -3 && cnt == 0 && has_more == 1);
    /* limit 0 probes existence without returning rows. */
    rc = segdb_scan_cursor(&g_sc_db, (const uint8_t*)"k:", 2, snap,
                           (const uint8_t*)"k:y", 3, 1, 0,
                           tiny, (uint32_t)sizeof(tiny), &cnt,
                           nk, (uint32_t)sizeof(nk), &nk_len, &has_more);
    assert(rc == 0 && cnt == 0 && has_more == 0); /* nothing past k:y */
    rc = segdb_scan_cursor(&g_sc_db, (const uint8_t*)"k:", 2, snap,
                           (const uint8_t*)"k:x", 3, 1, 0,
                           tiny, (uint32_t)sizeof(tiny), &cnt,
                           nk, (uint32_t)sizeof(nk), &nk_len, &has_more);
    assert(rc == 0 && cnt == 0 && has_more == 1); /* k:y past k:x */
    segdb_close(&g_sc_db);
    yowza_store_close(st);
    printf("Passed.\n"); fflush(stdout);
}

/* 5000 keys in 200-row segments (25 segments, under the 128
 * source cap), with updates + flushed tombstones. */
static uint64_t sc_build_large(SegDB* db) {
    uint32_t i;
    for (i = 0; i < 5000; i++) {
        char kb[32], vb[32];
        sc_key(kb, i);
        sc_val(vb, i);
        sc_put(db, kb, vb);
        if ((i + 1) % 200 == 0) assert(segdb_flush(db) == 0);
    }
    for (i = 0; i < 5000; i += 23) {
        char kb[32];
        sc_key(kb, i);
        sc_put(db, kb, "v:UPDATED");
    }
    assert(segdb_flush(db) == 0);
    for (i = 0; i < 5000; i += 17) {
        char kb[32];
        sc_key(kb, i);
        sc_del(db, kb);
    }
    assert(segdb_flush(db) == 0);
    return segdb_snapshot_seq(db);
}

void test_scan_cursor_work_reduction() {
    YowzaStore* st;
    uint64_t snap;
    SegdbScanStats a, b;
    uint32_t na, nb;
    printf("Running test_scan_cursor_work_reduction...\n");
    fflush(stdout);
    st = yowza_store_mem_create();
    assert(st != NULL);
    assert(segdb_init(&g_sc_db, st, g_sc_arena, sizeof(g_sc_arena),
                      g_sc_wal, sizeof(g_sc_wal)) == 0);
    snap = sc_build_large(&g_sc_db);
    /* Same paging (limit 64) through both paths, counters
     * reset before each: the cursor path must visit strictly
     * fewer segment entries than the skip path re-walking
     * from the start on every page — counting iterator
     * returns AND the prime's in-memory block skips together,
     * so the comparison cannot hide work in the cheaper
     * counter. */
    segdb_scan_stats_reset();
    na = sc_collect_skip(&g_sc_db, snap, 64);
    segdb_scan_stats_get(&a);
    segdb_scan_stats_reset();
    nb = sc_collect_cursor(&g_sc_db, snap, 64);
    segdb_scan_stats_get(&b);
    sc_assert_rows_equal(na, nb);
    assert(na > 4000);
    assert(a.page_visits > 0 && b.cursor_visits > 0);
    assert(b.cursor_visits < a.page_visits);
    assert(b.cursor_visits + b.cursor_block_skips < a.page_visits);
    /* Second cursor pass hits the discovery cache (first pass
     * populated it); misses must not grow on a repeat pass. */
    segdb_scan_stats_reset();
    nb = sc_collect_cursor(&g_sc_db, snap, 64);
    segdb_scan_stats_get(&b);
    assert(nb == na);
    assert(b.cache_hits > 0 && b.cache_misses == 0);
    segdb_close(&g_sc_db);
    yowza_store_close(st);
    printf("Passed.\n"); fflush(stdout);
}

/* ---- heap index tests (wasm heap store, native build) ---- */
static SegDB g_hp_db;
static uint8_t g_hp_arena[1048576];
static uint8_t g_hp_wal[262144];
static uint8_t g_hp_heap[4194304];

static uint64_t hp_u64(const uint8_t* p) {
    uint64_t v = 0;
    int i;
    for (i = 0; i < 8; i++) v |= (uint64_t)p[i] << (i * 8);
    return v;
}

static void hp_get(SegDB* db, const char* k, char* vbuf, uint32_t vcap) {
    uint8_t* v = NULL;
    uint32_t vlen = 0;
    uint32_t i;
    assert(segdb_get(db, (const uint8_t*)k, (uint32_t)strlen(k), &v, &vlen) == 0);
    assert(vlen < vcap);
    for (i = 0; i < vlen; i++) vbuf[i] = (char)v[i];
    vbuf[vlen] = '\0';
}

void test_heap_index_correctness_and_stats() {
    uint8_t stats[48];
    uint32_t i;
    char vb[64];
    printf("Running test_heap_index_correctness_and_stats...\n");
    fflush(stdout);
    memset(g_hp_heap, 0, sizeof(g_hp_heap));
    assert(yowza_segdb_init(&g_hp_db, g_hp_arena, sizeof(g_hp_arena),
                            g_hp_wal, sizeof(g_hp_wal),
                            g_hp_heap, sizeof(g_hp_heap)) == 0);
    for (i = 0; i < 200; i++) {
        char kb[32], vv[32];
        sc_key(kb, i);
        sc_val(vv, i);
        sc_put(&g_hp_db, kb, vv);
        if ((i + 1) % 50 == 0) assert(segdb_flush(&g_hp_db) == 0);
    }
    yowza_segdb_heap_stats_reset();
    for (i = 0; i < 200; i++) {
        char kb[32], ev[32];
        sc_key(kb, i);
        sc_val(ev, i);
        hp_get(&g_hp_db, kb, vb, sizeof(vb));
        assert(strcmp(vb, ev) == 0);
    }
    yowza_segdb_heap_stats(stats);
    /* lookups > 0, average probes per lookup small (the old
     * linear scan averaged hundreds of slots here: objects sit
     * at slots spread over the table, and every segment read
     * re-looked them up). */
    assert(hp_u64(stats) > 0);
    assert(hp_u64(stats + 8) < hp_u64(stats) * 20);
    assert(stats[40] == 1); /* index valid */
    segdb_close(&g_hp_db);
    printf("Passed.\n"); fflush(stdout);
}

void test_heap_index_heap_wins() {
    uint8_t stats_before[48], stats_after[48];
    char vb[64];
    printf("Running test_heap_index_heap_wins...\n");
    fflush(stdout);
    memset(g_hp_heap, 0, sizeof(g_hp_heap));
    assert(yowza_segdb_init(&g_hp_db, g_hp_arena, sizeof(g_hp_arena),
                            g_hp_wal, sizeof(g_hp_wal),
                            g_hp_heap, sizeof(g_hp_heap)) == 0);
    sc_put(&g_hp_db, "heapkey", "heapval");
    assert(segdb_flush(&g_hp_db) == 0);
    hp_get(&g_hp_db, "heapkey", vb, sizeof(vb));
    assert(strcmp(vb, "heapval") == 0);
    /* Corrupt the index entry for seg.1 (points at a different
     * occupied slot): the next segment read must still return
     * heapkey's own data, never the wrong slot's. */
    assert(yowza_segdb_heap_index_corrupt_for_test("seg.1") == 0);
    yowza_segdb_heap_stats(stats_before);
    hp_get(&g_hp_db, "heapkey", vb, sizeof(vb));
    assert(strcmp(vb, "heapval") == 0);
    yowza_segdb_heap_stats(stats_after);
    assert(hp_u64(stats_after + 24) > hp_u64(stats_before + 24)); /* stale rebuilds grew */
    /* Drop the index entry for seg.1 entirely: the linear
     * fallback must still find the heap object. */
    assert(yowza_segdb_heap_index_drop_for_test("seg.1") == 0);
    yowza_segdb_heap_stats(stats_before);
    hp_get(&g_hp_db, "heapkey", vb, sizeof(vb));
    assert(strcmp(vb, "heapval") == 0);
    yowza_segdb_heap_stats(stats_after);
    assert(hp_u64(stats_after + 32) > hp_u64(stats_before + 32)); /* linear fallbacks grew */
    /* Rebuild is a no-op for correctness: data still reads. */
    assert(yowza_segdb_heap_index_rebuild() == 0);
    hp_get(&g_hp_db, "heapkey", vb, sizeof(vb));
    assert(strcmp(vb, "heapval") == 0);
    segdb_close(&g_hp_db);
    printf("Passed.\n"); fflush(stdout);
}

void test_heap_index_many_objects_collision() {
    uint8_t stats[48];
    uint32_t i;
    char vb[64];
    printf("Running test_heap_index_many_objects_collision...\n");
    fflush(stdout);
    /* 300 single-row flushes = 300 segments + 300 manifests
     * (plus CURRENT objects): several hundred heap objects,
     * so bucket collisions in the 8192-bucket index are
     * essentially certain. Every key must still read back:
     * a probe must never stop early on a colliding entry.
     * (This is the regression test for the flush-25 bug: the
     * first index design returned "stale" on the first
     * non-matching chain entry, which made colliding lookups
     * report "not found" and failed a flush.) */
    /* NOTE: a 16 MiB heap, not the shared 4 MiB one: 300 flushes
     * leak bump space (orphan manifests + replaced group
     * manifests are never reclaimed in the heap store — a
     * documented limit), and the 4 MiB heap exhausts at flush
     * ~232 (bump 4193312/4194304, verified). The collision
     * behaviour under test is independent of heap size. */
    {
        static uint8_t big_heap[16777216];
        memset(big_heap, 0, sizeof(big_heap));
        assert(yowza_segdb_init(&g_hp_db, g_hp_arena, sizeof(g_hp_arena),
                                g_hp_wal, sizeof(g_hp_wal),
                                big_heap, sizeof(big_heap)) == 0);
    }
    for (i = 0; i < 300; i++) {
        char kb[32], vv[32];
        snprintf(kb, sizeof(kb), "c:%08u", i);
        snprintf(vv, sizeof(vv), "cv:%u", i);
        sc_put(&g_hp_db, kb, vv);
        {
            int frc = segdb_flush(&g_hp_db);
            if (frc != 0) {
                fprintf(stderr, "collision test flush %u rc=%d segs=%u has_dir=%d groups=%u\n",
                        i + 1, frc, g_hp_db.manifest.segment_count,
                        g_hp_db.has_dir, g_hp_db.group_count);
                fflush(stderr);
            }
            assert(frc == 0);
        }
    }
    yowza_segdb_heap_stats_reset();
    for (i = 0; i < 300; i++) {
        char kb[32], ev[32];
        snprintf(kb, sizeof(kb), "c:%08u", i);
        snprintf(ev, sizeof(ev), "cv:%u", i);
        hp_get(&g_hp_db, kb, vb, sizeof(vb));
        assert(strcmp(vb, ev) == 0);
    }
    yowza_segdb_heap_stats(stats);
    assert(hp_u64(stats) > 0);
    /* No lookup may fall back to a linear scan on a healthy
     * index, and no stale rebuild may occur: collisions are
     * resolved by probing past them, not by rebuilding. */
    assert(hp_u64(stats + 24) == 0); /* stale */
    assert(hp_u64(stats + 32) == 0); /* linear fallbacks */
    segdb_close(&g_hp_db);
    printf("Passed.\n"); fflush(stdout);
}

void test_heap_index_rebuild_on_restore_and_cursor_resume() {
    static uint8_t heap_copy[4194304];
    static uint8_t arena2[1048576];
    static uint8_t wal2[262144];
    SegDB db2;
    uint8_t cur[1024];
    uint32_t cur_len = 0;
    int has_cur = 0;
    uint32_t total = 0;
    uint64_t snap;
    printf("Running test_heap_index_rebuild_on_restore_and_cursor_resume...\n");
    fflush(stdout);
    memset(g_hp_heap, 0, sizeof(g_hp_heap));
    assert(yowza_segdb_init(&g_hp_db, g_hp_arena, sizeof(g_hp_arena),
                            g_hp_wal, sizeof(g_hp_wal),
                            g_hp_heap, sizeof(g_hp_heap)) == 0);
    {
        uint32_t i;
        for (i = 0; i < 120; i++) {
            char kb[32], vv[32];
            sc_key(kb, i);
            sc_val(vv, i);
            sc_put(&g_hp_db, kb, vv);
            if ((i + 1) % 40 == 0) assert(segdb_flush(&g_hp_db) == 0);
        }
    }
    snap = segdb_snapshot_seq(&g_hp_db);
    /* Page halfway through with the cursor, then restore the
     * heap bytes into a fresh DB and resume from the saved
     * cursor: the tail must be exactly the remaining rows. */
    {
        static uint8_t out[65536];
        uint32_t pages = 0;
        for (pages = 0; pages < 2; pages++) {
            uint8_t nk[1024];
            uint32_t nk_len = 0, has_more = 0, cnt = 0;
            int rc = segdb_scan_cursor(&g_hp_db, (const uint8_t*)"k:", 2, snap,
                                       cur, cur_len, has_cur, 20,
                                       out, (uint32_t)sizeof(out), &cnt,
                                       nk, (uint32_t)sizeof(nk), &nk_len, &has_more);
            assert(rc == 0 && cnt == 20 && has_more == 1);
            memcpy(cur, nk, nk_len);
            cur_len = nk_len;
            has_cur = 1;
            total += cnt;
        }
        assert(total == 40);
    }
    memcpy(heap_copy, g_hp_heap, sizeof(g_hp_heap));
    segdb_close(&g_hp_db);
    /* Fresh DB over the copied heap: init rebuilds the index
     * from the table, recover adopts the manifest. */
    assert(yowza_segdb_init(&db2, arena2, sizeof(arena2),
                            wal2, sizeof(wal2),
                            heap_copy, sizeof(heap_copy)) == 0);
    assert(segdb_recover(&db2) == 0);
    {
        static uint8_t out[65536];
        uint32_t tail = 0;
        for (;;) {
            uint8_t nk[1024];
            uint32_t nk_len = 0, has_more = 0, cnt = 0, off = 0, j;
            int rc = segdb_scan_cursor(&db2, (const uint8_t*)"k:", 2, snap,
                                       cur, cur_len, has_cur, 20,
                                       out, (uint32_t)sizeof(out), &cnt,
                                       nk, (uint32_t)sizeof(nk), &nk_len, &has_more);
            assert(rc == 0);
            if (cnt == 0) {
                assert(has_more == 0);
                break;
            }
            for (j = 0; j < cnt; j++) {
                char kb[64], vb[64], ekb[32], evb[32];
                uint32_t kl, vl;
                assert(sc_decode(out, (uint32_t)sizeof(out), &off,
                                 kb, sizeof(kb), &kl, vb, sizeof(vb), &vl) == 0);
                sc_key(ekb, total + tail + j);
                sc_val(evb, total + tail + j);
                assert(strcmp(kb, ekb) == 0 && strcmp(vb, evb) == 0);
            }
            tail += cnt;
            memcpy(cur, nk, nk_len);
            cur_len = nk_len;
            has_cur = 1;
            if (!has_more) break;
        }
        assert(tail == 80);
    }
    segdb_close(&db2);
    printf("Passed.\n"); fflush(stdout);
}

/* Self-registration: see the ownership note at the top. */
__attribute__((constructor)) static void test_scan_cursor_register(void) {
    test_scan_cursor_equals_skip();
    test_scan_cursor_snapshot_visibility();
    test_scan_cursor_small_buffer_and_limit0();
    test_scan_cursor_work_reduction();
    test_heap_index_correctness_and_stats();
    test_heap_index_heap_wins();
    test_heap_index_many_objects_collision();
    test_heap_index_rebuild_on_restore_and_cursor_resume();
}
