/* GATE 1 tests: hot-layer history, history transfer at flush,
 * and snapshot leases pinning compaction.
 *
 * OWNERSHIP NOTE (same pattern as test_scan_cursor.c):
 * test/main.c is owned by another agent, so this file
 * self-registers through a constructor: the tests below run
 * when the test_runner binary starts, before main's suite.
 * If main.c later gains extern declarations + calls for these
 * functions, delete the constructor at the bottom to avoid
 * running them twice.
 *
 * NOTE on test_segdb_art_iterate_sorted: that pre-existing test
 * asserts a hot delete + flush yields exactly 1 segment. Gate 1
 * history transfer necessarily yields 2 segments there (one
 * history segment for the deleted key's prior value, one
 * current segment with the tombstone) — the two requirements
 * cannot both hold, and the parent must update that test's
 * expectations (see GATE1_NOTES.md). This file does not touch
 * it; it proves the Gate 1 behaviour itself.
 */
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include "segdb.h"

static SegDB g_hl_db;
static SegDB g_hl_db2;
static uint8_t g_hl_arena[65536];
static uint8_t g_hl_wal[65536];
static uint8_t g_hl_arena2[65536];
static uint8_t g_hl_wal2[65536];
static uint8_t g_hl_out[65536];

static void hl_put(SegDB* db, const char* k, const char* v) {
    int rc = segdb_put(db, (const uint8_t*)k, (uint32_t)strlen(k),
                       (const uint8_t*)v, (uint32_t)strlen(v));
    assert(rc == 0);
}

static void hl_del(SegDB* db, const char* k) {
    int rc = segdb_delete(db, (const uint8_t*)k, (uint32_t)strlen(k));
    assert(rc == 0);
}

/* Collect one page scan at snap into a "k=v;" string. Returns count. */
static uint32_t hl_scan_page(SegDB* db, uint64_t snap, char* buf, uint32_t cap) {
    uint32_t count = 0;
    uint32_t off = 0;
    int rc = segdb_scan_page(db, (const uint8_t*)"", 0, snap, 0, 64,
                             g_hl_out, (uint32_t)sizeof(g_hl_out), &count);
    uint32_t i, pos = 0;
    assert(rc == 0);
    for (i = 0; i < count; i++) {
        uint32_t kl, vl, j;
        assert(off + 8 <= sizeof(g_hl_out));
        kl = (uint32_t)g_hl_out[off] | ((uint32_t)g_hl_out[off + 1] << 8) |
             ((uint32_t)g_hl_out[off + 2] << 16) | ((uint32_t)g_hl_out[off + 3] << 24);
        off += 4;
        assert(off + kl + 4 <= sizeof(g_hl_out));
        assert(pos + kl + 1 < cap);
        for (j = 0; j < kl; j++) buf[pos++] = (char)g_hl_out[off + j];
        off += kl;
        vl = (uint32_t)g_hl_out[off] | ((uint32_t)g_hl_out[off + 1] << 8) |
             ((uint32_t)g_hl_out[off + 2] << 16) | ((uint32_t)g_hl_out[off + 3] << 24);
        off += 4;
        assert(off + vl <= sizeof(g_hl_out));
        assert(pos + vl + 2 < cap);
        buf[pos++] = '=';
        for (j = 0; j < vl; j++) buf[pos++] = (char)g_hl_out[off + j];
        off += vl;
        buf[pos++] = ';';
    }
    buf[pos] = '\0';
    return count;
}

/* Collect one cursor scan at snap (all pages) into a "k=v;" string. */
static uint32_t hl_scan_cursor(SegDB* db, uint64_t snap, char* buf, uint32_t cap) {
    uint8_t cur[1024];
    uint32_t cur_len = 0;
    int has_cur = 0;
    uint32_t total = 0;
    uint32_t pos = 0;
    for (;;) {
        uint32_t count = 0, off = 0, i;
        uint8_t nk[1024];
        uint32_t nk_len = 0, has_more = 0;
        int rc = segdb_scan_cursor(db, (const uint8_t*)"", 0, snap,
                                   cur, cur_len, has_cur, 64,
                                   g_hl_out, (uint32_t)sizeof(g_hl_out), &count,
                                   nk, (uint32_t)sizeof(nk), &nk_len, &has_more);
        assert(rc == 0);
        if (count == 0) {
            assert(has_more == 0);
            break;
        }
        for (i = 0; i < count; i++) {
            uint32_t kl, vl, j;
            kl = (uint32_t)g_hl_out[off] | ((uint32_t)g_hl_out[off + 1] << 8) |
                 ((uint32_t)g_hl_out[off + 2] << 16) | ((uint32_t)g_hl_out[off + 3] << 24);
            off += 4;
            assert(pos + kl + 1 < cap);
            for (j = 0; j < kl; j++) buf[pos++] = (char)g_hl_out[off + j];
            off += kl;
            vl = (uint32_t)g_hl_out[off] | ((uint32_t)g_hl_out[off + 1] << 8) |
                 ((uint32_t)g_hl_out[off + 2] << 16) | ((uint32_t)g_hl_out[off + 3] << 24);
            off += 4;
            assert(pos + vl + 2 < cap);
            buf[pos++] = '=';
            for (j = 0; j < vl; j++) buf[pos++] = (char)g_hl_out[off + j];
            off += vl;
            buf[pos++] = ';';
        }
        total += count;
        memcpy(cur, nk, nk_len);
        cur_len = nk_len;
        has_cur = 1;
        if (!has_more) break;
    }
    buf[pos] = '\0';
    return total;
}

static YowzaStore* hl_store(void) {
    YowzaStore* s = yowza_store_mem_create();
    assert(s != NULL);
    return s;
}

/* Gate 1.1, hot: update + delete keep old snapshots readable
 * through page AND cursor scans, before any flush. */
void test_hot_lease_history_hot() {
    YowzaStore* st;
    uint64_t s1, s2, cur;
    char buf[1024];
    printf("Running test_hot_lease_history_hot...\n");
    fflush(stdout);
    st = hl_store();
    assert(segdb_init(&g_hl_db, st, g_hl_arena, sizeof(g_hl_arena),
                      g_hl_wal, sizeof(g_hl_wal)) == 0);
    hl_put(&g_hl_db, "k", "v1");
    hl_put(&g_hl_db, "other", "o1");
    s1 = segdb_snapshot_seq(&g_hl_db);
    hl_put(&g_hl_db, "k", "v2");
    s2 = segdb_snapshot_seq(&g_hl_db);
    /* Old snapshot sees v1, new sees v2, on both scan paths. */
    assert(hl_scan_page(&g_hl_db, s1, buf, sizeof(buf)) == 2);
    assert(strcmp(buf, "k=v1;other=o1;") == 0);
    assert(hl_scan_cursor(&g_hl_db, s1, buf, sizeof(buf)) == 2);
    assert(strcmp(buf, "k=v1;other=o1;") == 0);
    assert(hl_scan_page(&g_hl_db, s2, buf, sizeof(buf)) == 2);
    assert(strcmp(buf, "k=v2;other=o1;") == 0);
    /* Delete: old snapshots still read the prior value. */
    hl_del(&g_hl_db, "k");
    cur = segdb_snapshot_seq(&g_hl_db);
    assert(hl_scan_page(&g_hl_db, s1, buf, sizeof(buf)) == 2);
    assert(strcmp(buf, "k=v1;other=o1;") == 0);
    assert(hl_scan_cursor(&g_hl_db, s2, buf, sizeof(buf)) == 2);
    assert(strcmp(buf, "k=v2;other=o1;") == 0);
    assert(hl_scan_page(&g_hl_db, cur, buf, sizeof(buf)) == 1);
    assert(strcmp(buf, "other=o1;") == 0);
    /* Multi-update chain: v1 -> v2 -> v3, each snapshot exact. */
    hl_put(&g_hl_db, "m", "a");
    {
        uint64_t sa = segdb_snapshot_seq(&g_hl_db);
        hl_put(&g_hl_db, "m", "b");
        uint64_t sb = segdb_snapshot_seq(&g_hl_db);
        hl_put(&g_hl_db, "m", "c");
        assert(hl_scan_page(&g_hl_db, sa, buf, sizeof(buf)) == 2);
        assert(strcmp(buf, "m=a;other=o1;") == 0);
        assert(hl_scan_page(&g_hl_db, sb, buf, sizeof(buf)) == 2);
        assert(strcmp(buf, "m=b;other=o1;") == 0);
    }
    segdb_close(&g_hl_db);
    yowza_store_close(st);
    printf("Passed.\n");
}

/* Gate 1.1, flush transfer: history survives flush AND recover
 * (fresh SegDB over the same store reads old snapshots from
 * segments, not from memory). */
void test_hot_lease_history_flush_recover() {
    YowzaStore* st;
    uint64_t s1, cur;
    char buf[1024];
    printf("Running test_hot_lease_history_flush_recover...\n");
    fflush(stdout);
    st = hl_store();
    assert(segdb_init(&g_hl_db, st, g_hl_arena, sizeof(g_hl_arena),
                      g_hl_wal, sizeof(g_hl_wal)) == 0);
    hl_put(&g_hl_db, "k", "v1");
    s1 = segdb_snapshot_seq(&g_hl_db);
    hl_put(&g_hl_db, "k", "v2");
    assert(segdb_history_count(&g_hl_db) == 1);
    assert(segdb_flush(&g_hl_db) == 0);
    assert(segdb_history_count(&g_hl_db) == 0);
    /* History segment + current segment: 2 segments. */
    assert(g_hl_db.manifest.segment_count == 2);
    assert(hl_scan_page(&g_hl_db, s1, buf, sizeof(buf)) == 1);
    assert(strcmp(buf, "k=v1;") == 0);
    cur = segdb_snapshot_seq(&g_hl_db);
    assert(hl_scan_page(&g_hl_db, cur, buf, sizeof(buf)) == 1);
    assert(strcmp(buf, "k=v2;") == 0);
    /* Recover into a fresh SegDB: old snapshot still exact. */
    segdb_close(&g_hl_db);
    assert(segdb_init(&g_hl_db2, st, g_hl_arena2, sizeof(g_hl_arena2),
                      g_hl_wal2, sizeof(g_hl_wal2)) == 0);
    assert(segdb_recover(&g_hl_db2) == 0);
    assert(hl_scan_page(&g_hl_db2, s1, buf, sizeof(buf)) == 1);
    assert(strcmp(buf, "k=v1;") == 0);
    assert(hl_scan_cursor(&g_hl_db2, s1, buf, sizeof(buf)) == 1);
    assert(strcmp(buf, "k=v1;") == 0);
    assert(hl_scan_page(&g_hl_db2, cur, buf, sizeof(buf)) == 1);
    assert(strcmp(buf, "k=v2;") == 0);
    segdb_close(&g_hl_db2);
    yowza_store_close(st);
    printf("Passed.\n");
}

/* Gate 1.2: a stale lease defers compaction (returns 0, merges
 * nothing, leased data intact afterwards); after release,
 * compaction proceeds. Invalid leases fail loudly. */
void test_hot_lease_compact_defers() {
    YowzaStore* st;
    uint64_t s1, lease;
    char buf[1024];
    int has = 0;
    printf("Running test_hot_lease_compact_defers...\n");
    fflush(stdout);
    st = hl_store();
    assert(segdb_init(&g_hl_db, st, g_hl_arena, sizeof(g_hl_arena),
                      g_hl_wal, sizeof(g_hl_wal)) == 0);
    hl_put(&g_hl_db, "k", "v1");
    hl_put(&g_hl_db, "x", "1");
    assert(segdb_flush(&g_hl_db) == 0);
    s1 = segdb_snapshot_seq(&g_hl_db);
    hl_put(&g_hl_db, "k", "v2");
    assert(segdb_flush(&g_hl_db) == 0);
    assert(g_hl_db.manifest.segment_count == 2);
    /* Lease the old snapshot, then compact with a barrier that
     * would normally merge everything: it must defer. */
    lease = segdb_lease_acquire(&g_hl_db, s1);
    assert(lease != 0);
    assert(segdb_lease_valid(&g_hl_db, lease) == 1);
    assert(segdb_lease_oldest(&g_hl_db, &has) == s1 && has == 1);
    assert(segdb_compact(&g_hl_db, (uint64_t)-1) == 0); /* deferred */
    assert(g_hl_db.manifest.segment_count == 2); /* nothing merged */
    assert(hl_scan_page(&g_hl_db, s1, buf, sizeof(buf)) == 2);
    assert(strcmp(buf, "k=v1;x=1;") == 0);
    /* A lease at the current sequence does NOT defer. */
    assert(segdb_lease_release(&g_hl_db, lease) == 0);
    assert(segdb_lease_valid(&g_hl_db, lease) == 0);
    assert(segdb_lease_release(&g_hl_db, lease) != 0); /* twice: error */
    lease = segdb_lease_acquire(&g_hl_db, segdb_snapshot_seq(&g_hl_db));
    assert(lease != 0);
    assert(segdb_compact(&g_hl_db, 0) == 0); /* proceeds */
    assert(g_hl_db.manifest.segment_count == 1); /* merged */
    assert(segdb_lease_release(&g_hl_db, lease) == 0);
    /* Invalid leases: future sequence, unknown id. */
    assert(segdb_lease_acquire(&g_hl_db, segdb_snapshot_seq(&g_hl_db) + 100) == 0);
    assert(segdb_lease_release(&g_hl_db, 999999) != 0);
    assert(segdb_lease_valid(&g_hl_db, 999999) == 0);
    /* Current state intact after the real compact. */
    assert(hl_scan_page(&g_hl_db, segdb_snapshot_seq(&g_hl_db), buf, sizeof(buf)) == 2);
    assert(strcmp(buf, "k=v2;x=1;") == 0);
    segdb_close(&g_hl_db);
    yowza_store_close(st);
    printf("Passed.\n");
}

__attribute__((constructor)) static void test_hot_lease_register(void) {
    test_hot_lease_history_hot();
    test_hot_lease_history_flush_recover();
    test_hot_lease_compact_defers();
}
