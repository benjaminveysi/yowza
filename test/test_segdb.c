/* Phase 2, Track C tests — layered DB + legacy-path unification.
 *
 * Standalone build (Tracks A/B present in this working copy, so no
 * segment/manifest stubs are needed here — only the failing WAL
 * driver for the legacy tests is local to this file):
 *   clang -O2 -Iinclude -Wall -Wextra -DSEGDB_STANDALONE_MAIN \
 *     test/test_segdb.c src/arena.c src/art.c src/storage_memory.c \
 *     src/storage_posix.c src/tx.c src/vm.c src/yowza_db.c \
 *     src/segdb.c src/segment.c src/manifest.c src/storage2.c \
 *     -o /tmp/test_segdb && /tmp/test_segdb
 * Integrator: declare the test_segdb_* functions in test/main.c and
 * call them; compile WITHOUT -DSEGDB_STANDALONE_MAIN (no main here
 * then, so no duplicate-main conflict). SegDB structs are ~300 KiB:
 * they live in static storage in this file, never on the stack. */
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include "segdb.h"
#include "storage.h"
#include "yowza_db.h"
#include "vm.h"

/* ---------- small helpers ---------- */
static void s_put(SegDB* db, const char* k, const char* v) {
    assert(segdb_put(db, (const uint8_t*)k, (uint32_t)strlen(k),
                     (const uint8_t*)v, (uint32_t)strlen(v)) == 0);
}

static void s_putn(SegDB* db, const char* k, const uint8_t* v, uint32_t vlen) {
    assert(segdb_put(db, (const uint8_t*)k, (uint32_t)strlen(k), v, vlen) == 0);
}

static int s_has(SegDB* db, const char* k, const char* want) {
    uint8_t* out = NULL;
    uint32_t len = 0;
    if (segdb_get(db, (const uint8_t*)k, (uint32_t)strlen(k), &out, &len) != 0) return 0;
    if (len != strlen(want)) return 0;
    return len == 0 || memcmp(out, want, len) == 0;
}

static int s_missing(SegDB* db, const char* k) {
    uint8_t* out = NULL;
    uint32_t len = 0;
    return segdb_get(db, (const uint8_t*)k, (uint32_t)strlen(k), &out, &len) != 0;
}

/* Parse art_scan-format output and check it against expected pairs
 * (already in sorted key order). Returns 1 on exact match. */
static int scan_matches(const uint8_t* buf, uint32_t count,
                        const char* const* keys, const char* const* vals,
                        uint32_t expect) {
    uint32_t off = 0;
    uint32_t i;
    if (count != expect) return 0;
    for (i = 0; i < expect; i++) {
        uint32_t kl, vl;
        if (off + 4 > 0xFFFFFFFFu) return 0;
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

/* ---------- failing WAL driver (legacy-path tests) ---------- */
typedef struct {
    uint8_t* buf;
    uint32_t cap;
    uint32_t off;
    int fail_append;
    int fail_sync;
    int fail_sync_on_call; /* 0 = never; else fail exactly that sync call */
    int sync_calls;
} FailState;

static int f_append(StorageDriver* self, const uint8_t* data, uint32_t len) {
    FailState* st = (FailState*)self->state;
    if (st->fail_append) return -1;
    if (len > st->cap - st->off) return -1;
    memcpy(&st->buf[st->off], data, len);
    st->off += len;
    return (int)len;
}

static int f_check_capacity(StorageDriver* self, uint32_t len) {
    FailState* st = (FailState*)self->state;
    if (st->fail_append) return -1;
    return len <= st->cap - st->off ? 0 : -1;
}

static int f_sync(StorageDriver* self) {
    FailState* st = (FailState*)self->state;
    st->sync_calls++;
    if (st->fail_sync) return -1;
    if (st->fail_sync_on_call > 0 && st->sync_calls == st->fail_sync_on_call) return -1;
    return 0;
}

static int f_log_length(StorageDriver* self) {
    return (int)((FailState*)self->state)->off;
}

static int f_truncate(StorageDriver* self, uint32_t len) {
    FailState* st = (FailState*)self->state;
    if (len > st->off) return -1;
    st->off = len;
    return 0;
}

static int f_read(StorageDriver* self, uint8_t* buf, uint32_t len, uint32_t off) {
    FailState* st = (FailState*)self->state;
    uint32_t avail;
    if (off >= st->off) return 0;
    avail = st->off - off;
    if (len > avail) len = avail;
    memcpy(buf, &st->buf[off], len);
    return (int)len;
}

static StorageDriver f_driver(FailState* st) {
    StorageDriver d;
    memset(&d, 0, sizeof(d));
    d.state = st;
    d.append = f_append;
    d.check_capacity = f_check_capacity;
    d.sync = f_sync;
    d.log_length = f_log_length;
    d.truncate = f_truncate;
    d.read = f_read;
    return d;
}

void test_segdb_legacy_fail_append() {
    printf("Running test_segdb_legacy_fail_append...\n");
    static uint8_t mem[16384];
    static uint8_t wal[16384];
    FailState st;
    YowzaDB db;
    memset(&st, 0, sizeof(st));
    st.buf = wal; st.cap = sizeof(wal);
    assert(yowza_db_init(&db, mem, sizeof(mem), f_driver(&st)) == 0);
    assert(yowza_db_put(&db, "k", "old", 3) == 0);
    {
        uint32_t wal_len = st.off;
        void* v = NULL; size_t vl = 0;
        st.fail_append = 1;
        assert(yowza_db_put(&db, "k", "new", 3) == -6);
        assert(st.off == wal_len); /* WAL exactly as before */
        assert(yowza_db_get(&db, "k", &v, &vl) == 0 && vl == 3 && memcmp(v, "old", 3) == 0);
        assert(yowza_db_put(&db, "fresh", "x", 1) == -6);
        assert(st.off == wal_len);
        assert(yowza_db_get(&db, "fresh", &v, &vl) != 0);
        st.fail_append = 0;
        assert(yowza_db_put(&db, "k", "new", 3) == 0);
        assert(yowza_db_get(&db, "k", &v, &vl) == 0 && memcmp(v, "new", 3) == 0);
    }
    printf("Passed.\n");
}

void test_segdb_legacy_fail_sync() {
    printf("Running test_segdb_legacy_fail_sync...\n");
    static uint8_t mem[16384];
    static uint8_t wal[16384];
    FailState st;
    YowzaDB db;
    memset(&st, 0, sizeof(st));
    st.buf = wal; st.cap = sizeof(wal);
    assert(yowza_db_init(&db, mem, sizeof(mem), f_driver(&st)) == 0);
    assert(yowza_db_put(&db, "k", "old", 3) == 0);
    {
        uint32_t wal_len = st.off;
        void* v = NULL; size_t vl = 0;
        st.fail_sync = 1;
        /* Overwrite + new key both fail with -6 and leave ART+WAL
         * exactly as before (terminal restored, WAL truncated). */
        assert(yowza_db_put(&db, "k", "new", 3) == -6);
        assert(st.off == wal_len);
        assert(yowza_db_get(&db, "k", &v, &vl) == 0 && memcmp(v, "old", 3) == 0);
        assert(yowza_db_put(&db, "fresh", "x", 1) == -6);
        assert(st.off == wal_len);
        assert(yowza_db_get(&db, "fresh", &v, &vl) != 0);
        /* Delete under failing sync: same contract. */
        assert(yowza_db_delete(&db, "k") == -6);
        assert(st.off == wal_len);
        assert(yowza_db_get(&db, "k", &v, &vl) == 0 && memcmp(v, "old", 3) == 0);
        st.fail_sync = 0;
        assert(yowza_db_delete(&db, "k") == 0);
        assert(yowza_db_get(&db, "k", &v, &vl) != 0);
    }
    printf("Passed.\n");
}

void test_segdb_legacy_del_absent_and_no_second_sync() {
    printf("Running test_segdb_legacy_del_absent_and_no_second_sync...\n");
    static uint8_t mem[16384];
    static uint8_t wal[16384];
    FailState st;
    YowzaDB db;
    memset(&st, 0, sizeof(st));
    st.buf = wal; st.cap = sizeof(wal);
    assert(yowza_db_init(&db, mem, sizeof(mem), f_driver(&st)) == 0);
    /* Absent delete: preserved behaviour — success, no WAL record. */
    assert(yowza_db_delete(&db, "ghost") == 0);
    assert(st.off == 0);
    /* No second sync in execute_op: sync fails ONLY on call #2, so
     * a successful put must still succeed with exactly one sync.
     * (If execute_op still did its old ignored second sync, the
     * count would be 2; if vm_execute did not sync at all, the put
     * would still succeed but the count would be 0.) */
    st.fail_sync_on_call = 2;
    st.sync_calls = 0;
    assert(yowza_db_put(&db, "k", "v", 1) == 0);
    assert(st.sync_calls == 1);
    /* And a failure on the FIRST sync propagates as -6 from vm. */
    st.fail_sync_on_call = 1;
    st.sync_calls = 0;
    assert(yowza_db_put(&db, "k2", "v", 1) == -6);
    assert(st.sync_calls == 1);
    printf("Passed.\n");
}

/* ---------- segdb tests ---------- */
static SegDB g_db;
static uint8_t g_arena[65536];
static uint8_t g_wal[65536];

static YowzaStore* new_mem_store(void) {
    YowzaStore* s = yowza_store_mem_create();
    assert(s != NULL);
    return s;
}

void test_segdb_get_precedence() {
    printf("Running test_segdb_get_precedence...\n");
    YowzaStore* st = new_mem_store();
    assert(segdb_init(&g_db, st, g_arena, sizeof(g_arena), g_wal, sizeof(g_wal)) == 0);
    s_put(&g_db, "k1", "segval");
    s_put(&g_db, "k2", "segonly");
    s_put(&g_db, "empty", "");
    assert(segdb_flush(&g_db) == 0);
    assert(s_has(&g_db, "k1", "segval"));
    assert(s_has(&g_db, "k2", "segonly"));
    /* Empty live value is found, not missing. */
    {
        uint8_t* out = NULL; uint32_t len = 99;
        assert(segdb_get(&g_db, (const uint8_t*)"empty", 5, &out, &len) == 0 && len == 0);
    }
    /* Hot overwrite wins over the segment. */
    s_put(&g_db, "k1", "hotval");
    assert(s_has(&g_db, "k1", "hotval"));
    /* Hot tombstone wins over the segment value: NOT FOUND, and no
     * fall-through to the older live value. */
    assert(segdb_delete(&g_db, (const uint8_t*)"k1", 2) == 0);
    assert(s_missing(&g_db, "k1"));
    /* Segment-only delete in hot: tombstone shadows segment. */
    assert(segdb_delete(&g_db, (const uint8_t*)"k2", 2) == 0);
    assert(s_missing(&g_db, "k2"));
    segdb_close(&g_db);
    yowza_store_close(st);
    printf("Passed.\n");
}

void test_segdb_scan_merge_snapshot() {
    printf("Running test_segdb_scan_merge_snapshot...\n");
    YowzaStore* st = new_mem_store();
    static uint8_t out[65536];
    uint32_t count = 0;
    uint64_t snap;
    assert(segdb_init(&g_db, st, g_arena, sizeof(g_arena), g_wal, sizeof(g_wal)) == 0);
    s_put(&g_db, "a", "1");
    s_put(&g_db, "b", "2");
    assert(segdb_flush(&g_db) == 0);
    snap = g_db.seq_base + g_db.hot.index.commit_seq; /* == checkpoint 2 */
    assert(snap == 2);
    s_put(&g_db, "a", "10");
    s_put(&g_db, "c", "3");
    assert(segdb_delete(&g_db, (const uint8_t*)"b", 1) == 0);
    /* Current scan: merged, deduped, tombstone dropped. */
    {
        const char* keys[] = {"a", "c"};
        const char* vals[] = {"10", "3"};
        assert(segdb_scan(&g_db, (const uint8_t*)"", 0, 0, out, sizeof(out), &count) == 0);
        assert(scan_matches(out, count, keys, vals, 2));
    }
    /* Snapshot scan at seq 2: post-snapshot writes invisible, and
     * the pre-snapshot live values show through the later tombstone
     * (b live again at seq 2, a at its old value, c absent). */
    {
        const char* keys[] = {"a", "b"};
        const char* vals[] = {"1", "2"};
        assert(segdb_scan(&g_db, (const uint8_t*)"", 0, snap, out, sizeof(out), &count) == 0);
        assert(scan_matches(out, count, keys, vals, 2));
    }
    /* Prefix scan + snapshot above current behaves as current. */
    {
        const char* keys[] = {"a"};
        const char* vals[] = {"10"};
        assert(segdb_scan(&g_db, (const uint8_t*)"a", 1, 0, out, sizeof(out), &count) == 0);
        assert(scan_matches(out, count, keys, vals, 1));
        assert(segdb_scan(&g_db, (const uint8_t*)"", 0, 9999, out, sizeof(out), &count) == 0);
        assert(count == 2);
    }
    segdb_close(&g_db);
    yowza_store_close(st);
    printf("Passed.\n");
}

void test_segdb_flush_restart() {
    printf("Running test_segdb_flush_restart...\n");
    YowzaStore* st = new_mem_store();
    static SegDB db2;
    static uint8_t arena2[65536];
    static uint8_t wal2[65536];
    static SegDB db3;
    static uint8_t arena3[65536];
    static uint8_t wal3[65536];
    assert(segdb_init(&g_db, st, g_arena, sizeof(g_arena), g_wal, sizeof(g_wal)) == 0);
    s_put(&g_db, "x", "1");
    s_put(&g_db, "y", "2");
    assert(segdb_flush(&g_db) == 0);
    assert(g_db.manifest.segment_count == 1);
    segdb_close(&g_db);
    /* Restart over the same store: recover, read, then write + flush
     * again — the new segment must not overwrite the old one. */
    assert(segdb_init(&db2, st, arena2, sizeof(arena2), wal2, sizeof(wal2)) == 0);
    assert(segdb_recover(&db2) == 0);
    assert(db2.manifest.segment_count == 1);
    assert(db2.next_seg_id == 2);
    assert(s_has(&db2, "x", "1") && s_has(&db2, "y", "2"));
    /* Second recover on the same SegDB is rejected (guard). */
    assert(segdb_recover(&db2) == -1);
    s_put(&db2, "z", "3");
    assert(segdb_flush(&db2) == 0);
    assert(db2.manifest.segment_count == 2);
    segdb_close(&db2);
    assert(segdb_init(&db3, st, arena3, sizeof(arena3), wal3, sizeof(wal3)) == 0);
    assert(segdb_recover(&db3) == 0);
    assert(s_has(&db3, "x", "1") && s_has(&db3, "y", "2") && s_has(&db3, "z", "3"));
    segdb_close(&db3);
    yowza_store_close(st);
    printf("Passed.\n");
}

void test_segdb_flush_empty_and_virgin_recover() {
    printf("Running test_segdb_flush_empty_and_virgin_recover...\n");
    YowzaStore* st = new_mem_store();
    assert(segdb_init(&g_db, st, g_arena, sizeof(g_arena), g_wal, sizeof(g_wal)) == 0);
    assert(segdb_flush(&g_db) == 0); /* empty hot: no-op */
    assert(g_db.manifest.segment_count == 0);
    segdb_close(&g_db);
    assert(segdb_init(&g_db, st, g_arena, sizeof(g_arena), g_wal, sizeof(g_wal)) == 0);
    assert(segdb_recover(&g_db) == 0); /* virgin store: empty success */
    assert(g_db.manifest.segment_count == 0);
    segdb_close(&g_db);
    yowza_store_close(st);
    printf("Passed.\n");
}

void test_segdb_large_hot_budget() {
    printf("Running test_segdb_large_hot_budget...\n");
    YowzaStore* st = new_mem_store();
    static uint8_t arena[16384];
    static uint8_t wal[16384];
    static uint8_t out[65536];
    uint8_t val[128];
    uint32_t count = 0;
    int batch, k;
    uint64_t total_data = 0;
    memset(val, 'v', sizeof(val));
    assert(segdb_init(&g_db, st, arena, sizeof(arena), wal, sizeof(wal)) == 0);
    /* 200 keys in 5 flushes of 40. Total key+value bytes
     * 200 * (4 + 128) = 26400 > 16384 arena: the DB is larger than
     * the hot budget, and every key stays readable without loading
     * everything into the arena (proven by arena < total data). */
    for (batch = 0; batch < 5; batch++) {
        for (k = 0; k < 40; k++) {
            char key[8];
            int n = batch * 40 + k;
            key[0] = 'k'; key[1] = (char)('0' + (n / 100) % 10);
            key[2] = (char)('0' + (n / 10) % 10); key[3] = (char)('0' + n % 10);
            key[4] = '\0';
            s_putn(&g_db, key, val, sizeof(val));
            total_data += 4 + sizeof(val);
        }
        assert(segdb_flush(&g_db) == 0);
    }
    assert(g_db.manifest.segment_count == 5);
    assert(total_data > sizeof(arena));
    for (k = 0; k < 200; k++) {
        char key[8];
        uint8_t* outv = NULL; uint32_t len = 0;
        key[0] = 'k'; key[1] = (char)('0' + (k / 100) % 10);
        key[2] = (char)('0' + (k / 10) % 10); key[3] = (char)('0' + k % 10);
        key[4] = '\0';
        assert(segdb_get(&g_db, (const uint8_t*)key, 4, &outv, &len) == 0);
        assert(len == sizeof(val) && memcmp(outv, val, sizeof(val)) == 0);
    }
    assert(segdb_scan(&g_db, (const uint8_t*)"", 0, 0, out, sizeof(out), &count) == 0);
    assert(count == 200);
    segdb_close(&g_db);
    yowza_store_close(st);
    printf("Passed.\n");
}

/* Count entries for one key across all manifest segments (via the
 * real segment reader), and report whether a tombstone for it exists. */
static void seg_key_probe(SegDB* db, const char* key, uint32_t* total, int* has_tomb) {
    uint32_t i;
    *total = 0; *has_tomb = 0;
    for (i = 0; i < db->manifest.segment_count; i++) {
        SegReader r;
        SegEntry e;
        uint8_t kb[1024], vb[1024];
        int rc;
        assert(seg_open(db->store, db->manifest.segments[i].obj, &r) == 0);
        while ((rc = seg_iter_next(&r, &e, kb, sizeof(kb), vb, sizeof(vb))) == 0) {
            if (e.key_len == strlen(key) && memcmp(e.key, key, e.key_len) == 0) {
                (*total)++;
                if (e.is_tombstone) *has_tomb = 1;
            }
        }
        assert(rc == 1);
        seg_close(&r);
    }
}

static uint32_t seg_total_entries(SegDB* db) {
    uint32_t i, sum = 0;
    for (i = 0; i < db->manifest.segment_count; i++) sum += db->manifest.segments[i].entry_count;
    return sum;
}

void test_segdb_compact_tombstone_barrier() {
    printf("Running test_segdb_compact_tombstone_barrier...\n");
    /* Below barrier: tombstone dropped. */
    {
        YowzaStore* st = new_mem_store();
        uint64_t tomb_seq;
        uint32_t total; int has_tomb;
        assert(segdb_init(&g_db, st, g_arena, sizeof(g_arena), g_wal, sizeof(g_wal)) == 0);
        s_put(&g_db, "A", "a"); s_put(&g_db, "B", "b"); s_put(&g_db, "C", "c");
        assert(segdb_flush(&g_db) == 0);
        assert(segdb_delete(&g_db, (const uint8_t*)"B", 1) == 0);
        assert(segdb_flush(&g_db) == 0);
        tomb_seq = g_db.manifest.checkpoint_seq; /* the delete's eff seq */
        s_put(&g_db, "D", "d");
        assert(segdb_flush(&g_db) == 0);
        assert(seg_total_entries(&g_db) == 5); /* A,B,C + B-tomb + D */
        assert(segdb_compact(&g_db, tomb_seq) == 0);
        assert(s_has(&g_db, "A", "a") && s_has(&g_db, "C", "c") && s_has(&g_db, "D", "d"));
        assert(s_missing(&g_db, "B"));
        assert(seg_total_entries(&g_db) == 3);
        seg_key_probe(&g_db, "B", &total, &has_tomb);
        assert(total == 0 && !has_tomb);
        /* Hot stays untouched by compact (it was empty here, but
         * the compact-then-flush path still works). */
        s_put(&g_db, "E", "e");
        assert(segdb_flush(&g_db) == 0);
        assert(s_has(&g_db, "E", "e"));
        segdb_close(&g_db);
        yowza_store_close(st);
    }
    /* Above barrier: tombstone retained. */
    {
        YowzaStore* st = new_mem_store();
        uint32_t total; int has_tomb;
        assert(segdb_init(&g_db, st, g_arena, sizeof(g_arena), g_wal, sizeof(g_wal)) == 0);
        s_put(&g_db, "E", "e"); s_put(&g_db, "F", "f");
        assert(segdb_flush(&g_db) == 0);
        assert(segdb_delete(&g_db, (const uint8_t*)"E", 1) == 0);
        assert(segdb_flush(&g_db) == 0);
        assert(segdb_compact(&g_db, 2) == 0); /* tomb seq is 3 > 2 */
        assert(s_missing(&g_db, "E"));
        assert(s_has(&g_db, "F", "f"));
        seg_key_probe(&g_db, "E", &total, &has_tomb);
        assert(total == 1 && has_tomb);
        segdb_close(&g_db);
        yowza_store_close(st);
    }
    printf("Passed.\n");
}

void test_segdb_compact_hot_untouched() {
    printf("Running test_segdb_compact_hot_untouched...\n");
    YowzaStore* st = new_mem_store();
    assert(segdb_init(&g_db, st, g_arena, sizeof(g_arena), g_wal, sizeof(g_wal)) == 0);
    s_put(&g_db, "seg", "s");
    assert(segdb_flush(&g_db) == 0);
    /* Hot-only key must survive compaction untouched (compact
     * merges segments only; it must NOT flush hot). */
    s_put(&g_db, "hot", "h");
    assert(segdb_compact(&g_db, 0) == 0);
    assert(g_db.manifest.segment_count == 1); /* still just the seg */
    assert(s_has(&g_db, "hot", "h") && s_has(&g_db, "seg", "s"));
    assert(segdb_flush(&g_db) == 0);
    assert(g_db.manifest.segment_count == 2);
    segdb_close(&g_db);
    yowza_store_close(st);
    printf("Passed.\n");
}

void test_segdb_sparse_gt4g() {
    printf("Running test_segdb_sparse_gt4g...\n");
    /* Track A's sparse memory backend (yowza_store_mem_create),
     * used directly: a write/read at an offset above UINT32_MAX. */
    YowzaStore* st = new_mem_store();
    uint64_t off = 0x100000001ULL; /* 4 GiB + 1 */
    uint64_t size = 0;
    uint8_t buf[2];
    assert(st->vt->write_at(st->state, "sparse.obj", off, (const uint8_t*)"hi", 2) == 0);
    assert(st->vt->size(st->state, "sparse.obj", &size) == 0);
    assert(size == off + 2 && size > 0xFFFFFFFFULL);
    assert(st->vt->read_at(st->state, "sparse.obj", off, buf, 2) == 0);
    assert(buf[0] == 'h' && buf[1] == 'i');
    /* The hole before it reads back as zeroes, sparsely. */
    assert(st->vt->read_at(st->state, "sparse.obj", 0, buf, 2) == 0);
    assert(buf[0] == 0 && buf[1] == 0);
    yowza_store_close(st);
    printf("Passed.\n");
}

void test_segdb_art_iterate_sorted() {
    printf("Running test_segdb_art_iterate_sorted...\n");
    YowzaStore* st = new_mem_store();
    assert(segdb_init(&g_db, st, g_arena, sizeof(g_arena), g_wal, sizeof(g_wal)) == 0);
    s_put(&g_db, "z", "1");
    s_put(&g_db, "a", "2");
    s_put(&g_db, "m", "3");
    s_put(&g_db, "aa", "4");
    assert(segdb_delete(&g_db, (const uint8_t*)"m", 1) == 0);
    /* art_iterate (Track B) visits sorted, live + tombstone. Verify
     * through the flush: flushed segment entries must be strictly
     * sorted (seg_write rejects unsorted input), the entry count
     * must include the tombstone, and first/last keys must be the
     * sorted extremes.
     * Gate 1 history semantics (integration update): flush
     * transfers hot history into its own key-sorted segment
     * ahead of the current segment, so this hot delete + flush
     * yields TWO segments — segment 0 holds the deleted key's
     * prior value ('m' = "3"), segment 1 holds the 4 current
     * entries including the tombstone. A single segment cannot
     * hold two versions of one key (strictly sorted, no
     * duplicates), so the old 1-segment expectation is
     * impossible under the new required semantics. */
    assert(segdb_flush(&g_db) == 0);
    assert(g_db.manifest.segment_count == 2);
    assert(g_db.manifest.segments[0].entry_count == 1);
    assert(g_db.manifest.segments[0].first_key_len == 1 &&
           g_db.manifest.segments[0].first_key[0] == 'm');
    assert(g_db.manifest.segments[1].entry_count == 4); /* incl. tomb */
    assert(g_db.manifest.segments[1].first_key_len == 1 &&
           g_db.manifest.segments[1].first_key[0] == 'a');
    assert(g_db.manifest.segments[1].last_key_len == 1 &&
           g_db.manifest.segments[1].last_key[0] == 'z');
    segdb_close(&g_db);
    yowza_store_close(st);
    printf("Passed.\n");
}

void test_segdb_preflush_hot_lost_on_mem_restart() {
    printf("Running test_segdb_preflush_hot_lost_on_mem_restart...\n");
    /* Documented limitation (see report): the memory WAL driver's
     * offset lives in driver state, not in the WAL buffer, so a
     * fresh SegDB over the same memory store cannot replay hot
     * entries that were never flushed. Segment (flushed) data
     * survives; unflushed hot data does not. This is the memory
     * backend's process-local semantics, not a segment bug. */
    YowzaStore* st = new_mem_store();
    static SegDB db2;
    static uint8_t arena2[65536];
    static uint8_t wal2[65536];
    assert(segdb_init(&g_db, st, g_arena, sizeof(g_arena), g_wal, sizeof(g_wal)) == 0);
    s_put(&g_db, "flushed", "yes");
    assert(segdb_flush(&g_db) == 0);
    s_put(&g_db, "unflushed", "no");
    segdb_close(&g_db);
    assert(segdb_init(&db2, st, arena2, sizeof(arena2), wal2, sizeof(wal2)) == 0);
    assert(segdb_recover(&db2) == 0);
    assert(s_has(&db2, "flushed", "yes"));
    assert(s_missing(&db2, "unflushed"));
    segdb_close(&db2);
    yowza_store_close(st);
    printf("Passed.\n");
}

#ifdef SEGDB_STANDALONE_MAIN
int main(void) {
    test_segdb_legacy_fail_append();
    test_segdb_legacy_fail_sync();
    test_segdb_legacy_del_absent_and_no_second_sync();
    test_segdb_get_precedence();
    test_segdb_scan_merge_snapshot();
    test_segdb_flush_restart();
    test_segdb_flush_empty_and_virgin_recover();
    test_segdb_large_hot_budget();
    test_segdb_compact_tombstone_barrier();
    test_segdb_compact_hot_untouched();
    test_segdb_sparse_gt4g();
    test_segdb_art_iterate_sorted();
    test_segdb_preflush_hot_lost_on_mem_restart();
    printf("\nAll SegDB Tests Passed!\n");
    return 0;
}
#endif
