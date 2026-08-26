/* TX + HOT HISTORY integration tests (SegDB end-to-end).
 *
 * The production tx path (tx_commit on an ART) cannot name a SegDB:
 * it receives only an ART*. The integration therefore lives in the
 * TxState history hook (see tx.h): tx_commit captures every
 * superseded committed version through the hook BEFORE applying,
 * fail-closed, and rolls captures back on any later failure.
 * These tests wire the SegDB-side adapter for that hook — the exact
 * adapter segdb.c's registration must install (arena key copy,
 * seq = seq_base + ART-local old version, tombstone flag, value
 * pointer into the bump arena) — onto a real SegDB, then prove the
 * full path through segdb_scan / segdb_scan_page / segdb_flush.
 * The adapter code here duplicates segdb_history_capture's
 * semantics deliberately: segdb.c is read-only for this track, so
 * the test is the executable specification of the registration
 * that must land there (see the report's covered/not-covered
 * list). SegDB structs are ~300 KiB: they live in static storage,
 * never on the stack. */
#include <stdio.h>
#include <assert.h>
#include <string.h>
#include "segdb.h"
#include "storage.h"
#include "yowza_db.h"
#include "tx.h"

/* ---------- SegDB-side history adapter (hook implementation) ---------- */
static int h_cap_calls;
static int h_rb_calls;

static int h_capture(void* ctx, const uint8_t* key, uint32_t key_len,
                     uint64_t old_version, int tomb,
                     const uint8_t* val, uint32_t val_len, uint64_t crdt_ts) {
    SegDB* db = (SegDB*)ctx;
    uint8_t* kcopy;
    h_cap_calls++;
    if (db->hist_count >= SEGDB_HIST_MAX) return -3;
    kcopy = (uint8_t*)arena_alloc(&db->hot.arena, key_len);
    if (!kcopy) return -3;
    memcpy(kcopy, key, key_len);
    db->hist[db->hist_count].key = kcopy;
    db->hist[db->hist_count].key_len = key_len;
    db->hist[db->hist_count].tomb = tomb;
    db->hist[db->hist_count].val = val;
    db->hist[db->hist_count].val_len = val_len;
    db->hist[db->hist_count].seq = db->seq_base + old_version;
    db->hist[db->hist_count].crdt_ts = crdt_ts;
    db->hist_count++;
    return 0;
}
static int h_rollback(void* ctx, uint32_t count) {
    SegDB* db = (SegDB*)ctx;
    h_rb_calls++;
    if (count > db->hist_count) return -1;
    db->hist_count -= count;
    return 0;
}
static void h_wire(SegDB* db) {
    db->hot.tx.history_capture = h_capture;
    db->hot.tx.history_ctx = db;
    db->hot.tx.history_rollback = h_rollback;
}

/* ---------- helpers ---------- */
static void h_put(SegDB* db, const char* k, const char* v) {
    assert(segdb_put(db, (const uint8_t*)k, (uint32_t)strlen(k),
                     (const uint8_t*)v, (uint32_t)strlen(v)) == 0);
}
static uint64_t h_snap(SegDB* db) {
    return db->seq_base + db->hot.index.commit_seq;
}
static int h_has(SegDB* db, const char* k, const char* want) {
    uint8_t* out = NULL;
    uint32_t len = 0;
    if (segdb_get(db, (const uint8_t*)k, (uint32_t)strlen(k), &out, &len) != 0) return 0;
    return len == strlen(want) && (len == 0 || memcmp(out, want, len) == 0);
}
static int h_missing(SegDB* db, const char* k) {
    uint8_t* out = NULL;
    uint32_t len = 0;
    return segdb_get(db, (const uint8_t*)k, (uint32_t)strlen(k), &out, &len) != 0;
}
/* Parse art_scan-format output and check exact pairs (sorted order). */
static int h_scan_matches(const uint8_t* buf, uint32_t count,
                          const char* const* keys, const char* const* vals,
                          uint32_t expect) {
    uint32_t off = 0;
    uint32_t i;
    if (count != expect) return 0;
    for (i = 0; i < expect; i++) {
        uint32_t kl, vl;
        kl = (uint32_t)buf[off] | ((uint32_t)buf[off+1] << 8) |
             ((uint32_t)buf[off+2] << 16) | ((uint32_t)buf[off+3] << 24);
        off += 4;
        if (kl != strlen(keys[i]) || memcmp(&buf[off], keys[i], kl) != 0) return 0;
        off += kl;
        vl = (uint32_t)buf[off] | ((uint32_t)buf[off+1] << 8) |
             ((uint32_t)buf[off+2] << 16) | ((uint32_t)buf[off+3] << 24);
        off += 4;
        if (vl != strlen(vals[i]) || (vl > 0 && memcmp(&buf[off], vals[i], vl) != 0)) return 0;
        off += vl;
    }
    return 1;
}
static void h_tx_put(SegDB* db, const char* k, const char* v) {
    assert(yowza_db_tx_put(&db->hot, k, v, strlen(v), 0) == 0);
}
static void h_tx_del(SegDB* db, const char* k) {
    assert(yowza_db_tx_delete(&db->hot, k) == 0);
}

static SegDB h_db;
static uint8_t h_arena[65536];
static uint8_t h_wal[65536];
static YowzaStore* h_cur_store = NULL;

static void h_begin_db(void) {
    YowzaStore* st = yowza_store_mem_create();
    assert(st != NULL);
    h_cur_store = st;
    assert(segdb_init(&h_db, st, h_arena, sizeof(h_arena), h_wal, sizeof(h_wal)) == 0);
    h_wire(&h_db);
    h_cap_calls = 0;
    h_rb_calls = 0;
}
static void h_end_db(void) {
    segdb_close(&h_db);
    if (h_cur_store) yowza_store_close(h_cur_store);
    h_cur_store = NULL;
}

/* (b) snapshot-before-tx sees the pre-tx value after a put-overwrite. */
void test_tx_hist_snapshot_put_overwrite() {
    printf("Running test_tx_hist_snapshot_put_overwrite...\n");
    static uint8_t out[65536];
    uint32_t count = 0;
    uint64_t snap;
    h_begin_db();
    h_put(&h_db, "k", "v1");
    h_put(&h_db, "o", "o1");
    snap = h_snap(&h_db);
    assert(snap == 2);
    assert(yowza_db_tx_begin(&h_db.hot, 200, 1, TX_CONSISTENCY_LOCAL, 1) == 0);
    h_tx_put(&h_db, "k", "v2");
    h_tx_put(&h_db, "n", "n1"); /* brand-new key: no capture */
    assert(yowza_db_tx_commit(&h_db.hot) == 0);
    assert(segdb_history_count(&h_db) == 1); /* only k superseded */
    assert(h_has(&h_db, "k", "v2") && h_has(&h_db, "n", "n1") && h_has(&h_db, "o", "o1"));
    {
        const char* keys[] = {"k", "n", "o"};
        const char* vals[] = {"v2", "n1", "o1"};
        assert(segdb_scan(&h_db, (const uint8_t*)"", 0, 0, out, sizeof(out), &count) == 0);
        assert(h_scan_matches(out, count, keys, vals, 3));
    }
    {
        const char* keys[] = {"k", "o"};
        const char* vals[] = {"v1", "o1"};
        assert(segdb_scan(&h_db, (const uint8_t*)"", 0, snap, out, sizeof(out), &count) == 0);
        assert(h_scan_matches(out, count, keys, vals, 2));
    }
    /* Second tx on the same TxState: the hook survives tx_clear
     * (commit), so its capture lands in history too, and the
     * first tx's snapshot still reads correctly afterwards. */
    {
        uint64_t snap2 = h_snap(&h_db);
        const char* keys[] = {"k", "n", "o"};
        const char* vals[] = {"v2", "n1", "o1"};
        assert(yowza_db_tx_begin(&h_db.hot, 208, 1, TX_CONSISTENCY_LOCAL, 1) == 0);
        h_tx_put(&h_db, "o", "o2");
        assert(yowza_db_tx_commit(&h_db.hot) == 0);
        assert(segdb_history_count(&h_db) == 2);
        assert(h_has(&h_db, "o", "o2"));
        assert(segdb_scan(&h_db, (const uint8_t*)"", 0, snap2, out, sizeof(out), &count) == 0);
        assert(h_scan_matches(out, count, keys, vals, 3));
    }
    h_end_db();
    printf("Passed.\n");
}

/* Production wiring: NO manual h_wire() call. segdb_init_common
 * must register the TxState history hook itself, so a plain
 * segdb_init'd DB running yowza_db_tx_* on hot captures history
 * — including across a flush (the hook survives flush, which
 * never touches db->hot.tx). Without the registration this test
 * fails at the first hist_count assertion. */
static void h_begin_db_production(void) {
    YowzaStore* st = yowza_store_mem_create();
    assert(st != NULL);
    h_cur_store = st;
    assert(segdb_init(&h_db, st, h_arena, sizeof(h_arena), h_wal, sizeof(h_wal)) == 0);
}
void test_tx_hist_production_wiring() {
    printf("Running test_tx_hist_production_wiring...\n");
    static uint8_t out[65536];
    uint32_t count = 0;
    uint64_t snap, snap2;
    h_begin_db_production();
    assert(h_db.hot.tx.history_capture != NULL);
    assert(h_db.hot.tx.history_rollback != NULL);
    h_put(&h_db, "k", "v1");
    h_put(&h_db, "o", "o1");
    snap = h_snap(&h_db);
    assert(snap == 2);
    assert(yowza_db_tx_begin(&h_db.hot, 300, 1, TX_CONSISTENCY_LOCAL, 1) == 0);
    h_tx_put(&h_db, "k", "v2");
    assert(yowza_db_tx_commit(&h_db.hot) == 0);
    assert(segdb_history_count(&h_db) == 1);
    assert(h_has(&h_db, "k", "v2"));
    {
        const char* keys[] = {"k", "o"};
        const char* vals[] = {"v1", "o1"};
        assert(segdb_scan(&h_db, (const uint8_t*)"", 0, snap, out, sizeof(out), &count) == 0);
        assert(h_scan_matches(out, count, keys, vals, 2));
    }
    /* Hook survives flush (still registered), and the post-flush
     * snapshot still reads pre-tx values — but note the capture
     * expectation changes after flush: "o" is now
     * segment-resident, not a hot terminal, so tx_commit (like
     * segdb_put) captures NOTHING hot for it; the old value
     * survives in the segment and the snapshot reads it from
     * there. hist_count therefore stays 0, and that is correct. */
    assert(segdb_flush(&h_db) == 0);
    assert(segdb_history_count(&h_db) == 0);
    assert(h_db.hot.tx.history_capture != NULL);
    snap2 = h_snap(&h_db);
    assert(yowza_db_tx_begin(&h_db.hot, 301, 1, TX_CONSISTENCY_LOCAL, 1) == 0);
    h_tx_put(&h_db, "o", "o2");
    assert(yowza_db_tx_commit(&h_db.hot) == 0);
    assert(segdb_history_count(&h_db) == 0); /* segment holds o1 */
    assert(h_has(&h_db, "o", "o2"));
    {
        const char* keys[] = {"k", "o"};
        const char* vals[] = {"v2", "o1"};
        assert(segdb_scan(&h_db, (const uint8_t*)"", 0, snap2, out, sizeof(out), &count) == 0);
        assert(h_scan_matches(out, count, keys, vals, 2));
    }
    h_end_db();
    printf("Passed.\n");
}

/* (b) snapshot-before-tx sees the pre-tx value after a delete. */
void test_tx_hist_snapshot_delete() {
    printf("Running test_tx_hist_snapshot_delete...\n");
    static uint8_t out[65536];
    uint32_t count = 0;
    uint64_t snap;
    h_begin_db();
    h_put(&h_db, "d", "v2");
    h_put(&h_db, "k", "v1");
    snap = h_snap(&h_db);
    assert(snap == 2);
    assert(yowza_db_tx_begin(&h_db.hot, 201, 1, TX_CONSISTENCY_LOCAL, 1) == 0);
    h_tx_del(&h_db, "k");
    h_tx_put(&h_db, "d", "v3");
    assert(yowza_db_tx_commit(&h_db.hot) == 0);
    assert(segdb_history_count(&h_db) == 2); /* k's tombstone pre-image + d's old value */
    assert(h_missing(&h_db, "k") && h_has(&h_db, "d", "v3"));
    {
        const char* keys[] = {"d"};
        const char* vals[] = {"v3"};
        assert(segdb_scan(&h_db, (const uint8_t*)"", 0, 0, out, sizeof(out), &count) == 0);
        assert(h_scan_matches(out, count, keys, vals, 1));
    }
    {
        const char* keys[] = {"d", "k"};
        const char* vals[] = {"v2", "v1"};
        assert(segdb_scan(&h_db, (const uint8_t*)"", 0, snap, out, sizeof(out), &count) == 0);
        assert(h_scan_matches(out, count, keys, vals, 2));
    }
    h_end_db();
    printf("Passed.\n");
}

/* (c) an aborted tx leaves no history and no value change; a
 * validation-failing tx likewise captures nothing. */
void test_tx_hist_abort_leaves_nothing() {
    printf("Running test_tx_hist_abort_leaves_nothing...\n");
    static uint8_t out[65536];
    uint32_t count = 0;
    uint64_t snap;
    h_begin_db();
    h_put(&h_db, "k", "v1");
    snap = h_snap(&h_db);
    assert(yowza_db_tx_begin(&h_db.hot, 202, 1, TX_CONSISTENCY_LOCAL, 1) == 0);
    h_tx_put(&h_db, "k", "v2");
    h_tx_del(&h_db, "k"); /* last write wins: the tx would delete k */
    assert(yowza_db_tx_abort(&h_db.hot) == 0);
    assert(segdb_history_count(&h_db) == 0);
    assert(h_has(&h_db, "k", "v1"));
    {
        const char* keys[] = {"k"};
        const char* vals[] = {"v1"};
        assert(segdb_scan(&h_db, (const uint8_t*)"", 0, snap, out, sizeof(out), &count) == 0);
        assert(h_scan_matches(out, count, keys, vals, 1));
    }
    /* Conflict path: capture runs after validation, so a tx that
     * fails validation captures nothing either. */
    assert(yowza_db_tx_begin(&h_db.hot, 203, 1, TX_CONSISTENCY_LOCAL, 1) == 0);
    {
        void* v = NULL;
        size_t vl = 0;
        assert(yowza_db_tx_get(&h_db.hot, "k", &v, &vl) == 0); /* records version */
    }
    assert(art_insert(&h_db.hot.index, (const uint8_t*)"k", 1,
                      (const uint8_t*)"vX", 2, 0) == 0); /* concurrent writer */
    h_tx_put(&h_db, "other", "mine");
    assert(yowza_db_tx_commit(&h_db.hot) == TX_ERR_CONFLICT);
    assert(segdb_history_count(&h_db) == 0);
    assert(h_has(&h_db, "k", "vX"));
    assert(h_missing(&h_db, "other"));
    h_end_db();
    printf("Passed.\n");
}

/* (d) fail-closed history-full path: with the table full, a tx that
 * would supersede a live version fails WITHOUT applying, and the
 * table is left exactly full (no partial capture). A tx that
 * supersedes nothing (brand-new key) still commits. */
void test_tx_hist_history_full_fail_closed() {
    printf("Running test_tx_hist_history_full_fail_closed...\n");
    static SegDB f_db;
    static uint8_t f_arena[1 << 20];
    static uint8_t f_wal[1 << 20];
    YowzaStore* st = yowza_store_mem_create();
    char key[16];
    int i;
    assert(st != NULL);
    assert(segdb_init(&f_db, st, f_arena, sizeof(f_arena), f_wal, sizeof(f_wal)) == 0);
    f_db.hot.tx.history_capture = h_capture;
    f_db.hot.tx.history_ctx = &f_db;
    f_db.hot.tx.history_rollback = h_rollback;
    for (i = 0; i < (int)SEGDB_HIST_MAX; i++) {
        snprintf(key, sizeof(key), "h%04d", i);
        h_put(&f_db, key, "v");
        h_put(&f_db, key, "w"); /* overwrite captures one version */
    }
    assert(segdb_history_count(&f_db) == SEGDB_HIST_MAX);
    h_put(&f_db, "victim", "V0"); /* new key: no capture, table stays full */
    assert(segdb_history_count(&f_db) == SEGDB_HIST_MAX);
    assert(yowza_db_tx_begin(&f_db.hot, 204, 1, TX_CONSISTENCY_LOCAL, 1) == 0);
    h_tx_put(&f_db, "victim", "V1");
    assert(yowza_db_tx_commit(&f_db.hot) == TX_ERR_OOM);
    assert(yowza_db_tx_active(&f_db.hot) == 0);
    assert(segdb_history_count(&f_db) == SEGDB_HIST_MAX);
    assert(h_has(&f_db, "victim", "V0"));
    /* A tx superseding nothing is unaffected by the full table. */
    assert(yowza_db_tx_begin(&f_db.hot, 205, 1, TX_CONSISTENCY_LOCAL, 1) == 0);
    h_tx_put(&f_db, "brandnew", "B");
    assert(yowza_db_tx_commit(&f_db.hot) == 0);
    assert(h_has(&f_db, "brandnew", "B"));
    segdb_close(&f_db);
    yowza_store_close(st);
    printf("Passed.\n");
}

/* (d) mid-apply OOM: the first applied op is undone AND its history
 * capture is rolled back (rollback observed: at least one capture
 * succeeded before the failure, and a rollback ran for it). */
void test_tx_hist_apply_oom_rolls_back_history() {
    printf("Running test_tx_hist_apply_oom_rolls_back_history...\n");
    static SegDB o_db;
    static uint8_t o_arena[1536];
    static uint8_t o_wal[16384];
    YowzaStore* st = yowza_store_mem_create();
    uint8_t v100[100];
    uint8_t v300[300];
    int cap_before, rb_before;
    assert(st != NULL);
    assert(segdb_init(&o_db, st, o_arena, sizeof(o_arena), o_wal, sizeof(o_wal)) == 0);
    o_db.hot.tx.history_capture = h_capture;
    o_db.hot.tx.history_ctx = &o_db;
    o_db.hot.tx.history_rollback = h_rollback;
    memset(v100, 'x', sizeof(v100));
    memset(v300, 'y', sizeof(v300));
    h_put(&o_db, "a", "A");
    cap_before = h_cap_calls;
    rb_before = h_rb_calls;
    assert(yowza_db_tx_begin(&o_db.hot, 206, 1, TX_CONSISTENCY_LOCAL, 1) == 0);
    assert(yowza_db_tx_put_raw(&o_db.hot, "a", 1, v100, sizeof(v100), 0) == 0);
    assert(yowza_db_tx_put_raw(&o_db.hot, "b", 1, v300, sizeof(v300), 0) == 0);
    assert(yowza_db_tx_put_raw(&o_db.hot, "c", 1, v300, sizeof(v300), 0) == 0);
    assert(yowza_db_tx_commit(&o_db.hot) == TX_ERR_OOM);
    assert(h_cap_calls > cap_before); /* a capture succeeded pre-apply */
    assert(h_rb_calls > rb_before);   /* ...and was rolled back */
    assert(segdb_history_count(&o_db) == 0);
    assert(h_has(&o_db, "a", "A"));
    assert(h_missing(&o_db, "b"));
    assert(h_missing(&o_db, "c"));
    assert(yowza_db_tx_active(&o_db.hot) == 0);
    segdb_close(&o_db);
    yowza_store_close(st);
    printf("Passed.\n");
}

/* (e) flush after a tx transfers the tx history into segments, and
 * the old snapshot still reads correctly (scan and page paths). */
void test_tx_hist_flush_transfers_tx_history() {
    printf("Running test_tx_hist_flush_transfers_tx_history...\n");
    static uint8_t out[65536];
    uint32_t count = 0;
    uint64_t snap;
    h_begin_db();
    h_put(&h_db, "k", "v1");
    h_put(&h_db, "x", "x1");
    snap = h_snap(&h_db);
    assert(snap == 2);
    assert(yowza_db_tx_begin(&h_db.hot, 207, 1, TX_CONSISTENCY_LOCAL, 1) == 0);
    h_tx_put(&h_db, "k", "v2");
    assert(yowza_db_tx_commit(&h_db.hot) == 0);
    assert(segdb_history_count(&h_db) == 1);
    assert(segdb_flush(&h_db) == 0);
    assert(segdb_history_count(&h_db) == 0); /* transferred, then cleared */
    {
        const char* keys[] = {"k", "x"};
        const char* vals[] = {"v2", "x1"};
        assert(segdb_scan(&h_db, (const uint8_t*)"", 0, 0, out, sizeof(out), &count) == 0);
        assert(h_scan_matches(out, count, keys, vals, 2));
    }
    {
        const char* keys[] = {"k", "x"};
        const char* vals[] = {"v1", "x1"};
        assert(segdb_scan(&h_db, (const uint8_t*)"", 0, snap, out, sizeof(out), &count) == 0);
        assert(h_scan_matches(out, count, keys, vals, 2));
        /* Same snapshot through the page path (literal snapshot). */
        assert(segdb_scan_page(&h_db, (const uint8_t*)"", 0, snap, 0, 10,
                               out, sizeof(out), &count) == 0);
        assert(h_scan_matches(out, count, keys, vals, 2));
    }
    h_end_db();
    printf("Passed.\n");
}
