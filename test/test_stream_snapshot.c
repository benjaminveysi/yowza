/* Deep-integration part 1 C tests: SnapshotSeq / literal ScanAt
 * semantics and the truly streaming page scan (segdb_scan_page).
 *
 * OWNERSHIP NOTE: test/main.c is owned by another agent, so this
 * file self-registers through a constructor: the tests below run
 * when the test_runner binary starts, before main's suite, and
 * print in the same "Running ... / Passed." style. If main.c
 * later gains extern declarations + calls for these functions,
 * delete the constructor at the bottom of this file to avoid
 * running them twice (the tests are self-contained and would
 * still pass twice, just slower).
 */
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include "segdb.h"

static SegDB g_st_db;
static uint8_t g_st_arena[1048576];
static uint8_t g_st_wal[262144];

static void st_put(SegDB* db, const char* k, const char* v) {
    int rc = segdb_put(db, (const uint8_t*)k, (uint32_t)strlen(k),
                       (const uint8_t*)v, (uint32_t)strlen(v));
    if (rc != 0) {
        fprintf(stderr, "st_put %s failed rc=%d\n", k, rc);
        fflush(stderr);
    }
    assert(rc == 0);
}

static void st_key(char* buf, uint32_t idx) {
    /* Zero-padded so lexicographic order == numeric order. */
    int n = snprintf(buf, 32, "k:%08u", idx);
    assert(n > 0);
}

static void st_val(char* buf, uint32_t idx) {
    int n = snprintf(buf, 32, "v:%u", idx);
    assert(n > 0);
}

/* Decode one art_scan-format entry from buf at *off; returns 0 on
 * success, -1 on truncation. Key/val are copied to callers' buffers. */
static int st_decode(const uint8_t* buf, uint32_t total, uint32_t* off,
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

/* Page the whole result at snapshot_seq in pages of `limit` and
 * verify count + order + values against the k:%08u / v:%u scheme
 * (with an optional single overridden value for key 0). Returns
 * the row count, or -1 on any mismatch (asserts inside). */
static int st_page_all(SegDB* db, uint64_t snap, uint32_t limit,
                       uint32_t expect, const char* override0) {
    static uint8_t out[65536];
    uint64_t skip = 0;
    uint32_t count = 0;
    for (;;) {
        uint32_t cnt = 0;
        uint32_t off = 0;
        int rc = segdb_scan_page(db, (const uint8_t*)"k:", 2, snap, skip,
                                 limit, out, (uint32_t)sizeof(out), &cnt);
        uint32_t j;
        assert(rc == 0);
        if (cnt == 0) break;
        for (j = 0; j < cnt; j++) {
            char kb[64], vb[64], ekb[32], evb[32];
            uint32_t kl, vl;
            assert(st_decode(out, (uint32_t)sizeof(out), &off,
                             kb, sizeof(kb), &kl, vb, sizeof(vb), &vl) == 0);
            st_key(ekb, count + j);
            if (count + j == 0 && override0) {
                assert(strcmp(kb, ekb) == 0 && strcmp(vb, override0) == 0);
            } else {
                st_val(evb, count + j);
                assert(strcmp(kb, ekb) == 0 && strcmp(vb, evb) == 0);
            }
        }
        count += cnt;
        skip += cnt;
        if (cnt < limit) break;
    }
    assert(count == expect);
    return (int)count;
}

void test_stream_snapshot_seq() {
    YowzaStore* st;
    uint64_t s0, s1, s2;
    printf("Running test_stream_snapshot_seq...\n");
    fflush(stdout);
    st = yowza_store_mem_create();
    assert(st != NULL);
    assert(segdb_init(&g_st_db, st, g_st_arena, sizeof(g_st_arena),
                      g_st_wal, sizeof(g_st_wal)) == 0);
    /* Virgin DB: current sequence is 0. */
    assert(segdb_snapshot_seq(&g_st_db) == 0);
    st_put(&g_st_db, "a", "1");
    st_put(&g_st_db, "b", "2");
    s0 = segdb_snapshot_seq(&g_st_db);
    assert(s0 == 2); /* one per committed put */
    assert(segdb_flush(&g_st_db) == 0);
    s1 = segdb_snapshot_seq(&g_st_db);
    assert(s1 == s0); /* flush preserves the sum */
    st_put(&g_st_db, "c", "3");
    s2 = segdb_snapshot_seq(&g_st_db);
    assert(s2 == s1 + 1);
    /* Literal page semantics: seq 0 yields nothing, an old seq
     * hides the newer key, the new seq shows it. */
    {
        static uint8_t out[4096];
        uint32_t cnt = 0;
        assert(segdb_scan_page(&g_st_db, (const uint8_t*)"", 0, 0, 0, 100,
                               out, (uint32_t)sizeof(out), &cnt) == 0 && cnt == 0);
        assert(segdb_scan_page(&g_st_db, (const uint8_t*)"", 0, s0, 0, 100,
                               out, (uint32_t)sizeof(out), &cnt) == 0 && cnt == 2);
        assert(segdb_scan_page(&g_st_db, (const uint8_t*)"", 0, s2, 0, 100,
                               out, (uint32_t)sizeof(out), &cnt) == 0 && cnt == 3);
    }
    segdb_close(&g_st_db);
    yowza_store_close(st);
    printf("Passed.\n"); fflush(stdout);
}

void test_stream_page_snapshot_visibility() {
    YowzaStore* st;
    uint64_t snap;
    static uint8_t out[4096];
    uint32_t cnt = 0;
    printf("Running test_stream_page_snapshot_visibility...\n");
    fflush(stdout);
    st = yowza_store_mem_create();
    assert(st != NULL);
    assert(segdb_init(&g_st_db, st, g_st_arena, sizeof(g_st_arena),
                      g_st_wal, sizeof(g_st_wal)) == 0);
    st_put(&g_st_db, "k:a", "old-a");
    st_put(&g_st_db, "k:b", "old-b");
    assert(segdb_flush(&g_st_db) == 0);
    snap = segdb_snapshot_seq(&g_st_db);
    /* Update one key, delete the other, flush the tombstone. */
    st_put(&g_st_db, "k:a", "new-a");
    assert(segdb_delete(&g_st_db, (const uint8_t*)"k:b", 3) == 0);
    assert(segdb_flush(&g_st_db) == 0);
    /* At the old snapshot: both old values, tombstone invisible. */
    assert(segdb_scan_page(&g_st_db, (const uint8_t*)"k:", 2, snap, 0, 100,
                           out, (uint32_t)sizeof(out), &cnt) == 0 && cnt == 2);
    {
        uint32_t off = 0;
        char kb[64], vb[64];
        uint32_t kl, vl;
        assert(st_decode(out, (uint32_t)sizeof(out), &off, kb, sizeof(kb), &kl,
                         vb, sizeof(vb), &vl) == 0);
        assert(strcmp(kb, "k:a") == 0 && strcmp(vb, "old-a") == 0);
        assert(st_decode(out, (uint32_t)sizeof(out), &off, kb, sizeof(kb), &kl,
                         vb, sizeof(vb), &vl) == 0);
        assert(strcmp(kb, "k:b") == 0 && strcmp(vb, "old-b") == 0);
    }
    /* At current: new-a only; the tombstone hides old-b. */
    assert(segdb_scan_page(&g_st_db, (const uint8_t*)"k:", 2,
                           segdb_snapshot_seq(&g_st_db), 0, 100,
                           out, (uint32_t)sizeof(out), &cnt) == 0 && cnt == 1);
    segdb_close(&g_st_db);
    yowza_store_close(st);
    printf("Passed.\n"); fflush(stdout);
}

void test_stream_page_newer_smallest_key() {
    YowzaStore* st;
    uint64_t snap;
    static uint8_t out[4096];
    uint32_t cnt = 0;
    printf("Running test_stream_page_newer_smallest_key...\n");
    st = yowza_store_mem_create();
    assert(st != NULL);
    assert(segdb_init(&g_st_db, st, g_st_arena, sizeof(g_st_arena),
                      g_st_wal, sizeof(g_st_wal)) == 0);
    st_put(&g_st_db, "k:m", "m");
    st_put(&g_st_db, "k:z", "z");
    assert(segdb_flush(&g_st_db) == 0);
    snap = segdb_snapshot_seq(&g_st_db);
    /* New key sorting BEFORE every older key: at the old snapshot
     * it must be skipped without truncating the older rows (the
     * page winner extraction must move on to the next key group,
     * not report end of segments). */
    st_put(&g_st_db, "k:a", "a");
    assert(segdb_scan_page(&g_st_db, (const uint8_t*)"k:", 2, snap, 0, 100,
                           out, (uint32_t)sizeof(out), &cnt) == 0 && cnt == 2);
    assert(segdb_scan_page(&g_st_db, (const uint8_t*)"k:", 2,
                           segdb_snapshot_seq(&g_st_db), 0, 100,
                           out, (uint32_t)sizeof(out), &cnt) == 0 && cnt == 3);
    segdb_close(&g_st_db);
    yowza_store_close(st);
    printf("Passed.\n"); fflush(stdout);
}

void test_stream_page_large() {
    YowzaStore* st;
    uint64_t snap;
    uint32_t i;
    printf("Running test_stream_page_large...\n");
    fflush(stdout);
    st = yowza_store_mem_create();
    assert(st != NULL);
    assert(segdb_init(&g_st_db, st, g_st_arena, sizeof(g_st_arena),
                      g_st_wal, sizeof(g_st_wal)) == 0);
    /* 9000 rows > old 8192 candidate cap, flushed in 1000-row
     * batches (flush cap is 4096; 1000 puts fit the WAL) so the
     * bulk lives in segments. */
    for (i = 0; i < 9000; i++) {
        char kb[32], vb[32];
        st_key(kb, i);
        st_val(vb, i);
        st_put(&g_st_db, kb, vb);
        if ((i + 1) % 1000 == 0) assert(segdb_flush(&g_st_db) == 0);
    }
    snap = segdb_snapshot_seq(&g_st_db);
    /* Legacy scan must fail on this size (candidate cap); the
     * page scan streams it fully, in small pages. */
    {
        static uint8_t big[262144];
        uint32_t cnt = 0;
        assert(segdb_scan(&g_st_db, (const uint8_t*)"k:", 2, 0,
                          big, (uint32_t)sizeof(big), &cnt) != 0);
    }
    assert(st_page_all(&g_st_db, snap, 64, 9000, NULL) == 9000);
    /* Mid-stream write at the frozen sequence: a new row and an
     * update are both invisible when paging at snap, and the
     * update is visible at the new sequence. */
    st_put(&g_st_db, "k:00009000", "v:9000");
    st_put(&g_st_db, "k:00000000", "v:CHANGED");
    assert(st_page_all(&g_st_db, snap, 64, 9000, NULL) == 9000);
    assert(st_page_all(&g_st_db, segdb_snapshot_seq(&g_st_db), 64, 9001,
                       "v:CHANGED") == 9001);
    segdb_close(&g_st_db);
    yowza_store_close(st);
    printf("Passed.\n"); fflush(stdout);
}

/* Self-registration: see the ownership note at the top. */
__attribute__((constructor)) static void test_stream_register(void) {
    test_stream_snapshot_seq();
    test_stream_page_snapshot_visibility();
    test_stream_page_newer_smallest_key();
    test_stream_page_large();
}
