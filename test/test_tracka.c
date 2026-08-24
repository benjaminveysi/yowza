/* Track A tests: three-state publish, status split, CURRENT.bak,
 * persistent WAL + tombstone-aware replay, streaming compaction,
 * multi-level manifest, Go/wasm segdb (Go side in segdb_test.go).
 * Native only for the POSIX-WAL test (guarded); the rest is
 * wasm-safe in style (no libc in production paths, but this test
 * file itself uses stdio/stdlib like the other test files). */
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include "segdb.h"

static void t_put(SegDB* db, const char* k, const char* v) {
    assert(segdb_put(db, (const uint8_t*)k, (uint32_t)strlen(k),
                     (const uint8_t*)v, (uint32_t)strlen(v)) == 0);
}

static int t_has(SegDB* db, const char* k, const char* want) {
    uint8_t* out = NULL;
    uint32_t len = 0;
    if (segdb_get(db, (const uint8_t*)k, (uint32_t)strlen(k), &out, &len) != 0) return 0;
    return len == strlen(want) && (len == 0 || memcmp(out, want, len) == 0);
}

static int t_missing(SegDB* db, const char* k) {
    uint8_t* out = NULL;
    uint32_t len = 0;
    return segdb_get(db, (const uint8_t*)k, (uint32_t)strlen(k), &out, &len) != 0;
}

/* ---------- fault-injecting store wrapper (Track A) ---------- */
enum { TA_OFF = 0, TA_INDET_AT_CURRENT, TA_DEFINITE_MANIFEST, TA_IOERR_CURRENT };

typedef struct {
    YowzaStore* inner;
    int mode;
    int fired;
} TaState;

static int ta_name_eq(const char* a, const char* b) { return strcmp(a, b) == 0; }
static int ta_starts(const char* s, const char* pfx) {
    size_t i = 0;
    while (pfx[i] != '\0') { if (s[i] != pfx[i]) return 0; i++; }
    return 1;
}

static int ta_write_at(void* vs, const char* obj, uint64_t off, const uint8_t* data, uint64_t len) {
    TaState* fs = (TaState*)vs;
    return fs->inner->vt->write_at(fs->inner->state, obj, off, data, len);
}
static int ta_read_at(void* vs, const char* obj, uint64_t off, uint8_t* out, uint64_t len) {
    TaState* fs = (TaState*)vs;
    return fs->inner->vt->read_at(fs->inner->state, obj, off, out, len);
}
static int ta_size(void* vs, const char* obj, uint64_t* out) {
    TaState* fs = (TaState*)vs;
    return fs->inner->vt->size(fs->inner->state, obj, out);
}
static int ta_size_status(void* vs, const char* obj, uint64_t* out) {
    TaState* fs = (TaState*)vs;
    if (fs->mode == TA_IOERR_CURRENT && ta_name_eq(obj, "CURRENT")) return YOWZA_STORE_IO_ERROR;
    if (fs->inner->vt->size_status) return fs->inner->vt->size_status(fs->inner->state, obj, out);
    return fs->inner->vt->size(fs->inner->state, obj, out) == 0 ? YOWZA_STORE_OK : YOWZA_STORE_UNKNOWN;
}
static int ta_sync_obj(void* vs, const char* obj) {
    TaState* fs = (TaState*)vs;
    return fs->inner->vt->sync_obj(fs->inner->state, obj);
}
static int ta_sync_dir(void* vs) {
    TaState* fs = (TaState*)vs;
    return fs->inner->vt->sync_dir(fs->inner->state);
}
static int ta_publish(void* vs, const char* tmp_obj, const char* final_obj) {
    TaState* fs = (TaState*)vs;
    int rc;
    if (fs->mode == TA_DEFINITE_MANIFEST && !fs->fired && ta_starts(final_obj, "manifest.")) {
        fs->fired = 1;
        return -1; /* rename never happens: definite failure */
    }
    rc = fs->inner->vt->publish(fs->inner->state, tmp_obj, final_obj);
    if (fs->mode == TA_INDET_AT_CURRENT && !fs->fired && ta_name_eq(final_obj, "CURRENT") && rc == 0) {
        /* Rename happened, durability sync "failed": indeterminate. */
        fs->fired = 1;
        return -2;
    }
    return rc;
}
static int ta_remove(void* vs, const char* obj) {
    TaState* fs = (TaState*)vs;
    return fs->inner->vt->remove(fs->inner->state, obj);
}
static int ta_truncate(void* vs, const char* obj, uint64_t len) {
    TaState* fs = (TaState*)vs;
    return fs->inner->vt->truncate(fs->inner->state, obj, len);
}
static void ta_close(void* vs) { (void)vs; }

static const YowzaStoreVTable ta_vt = {
    ta_write_at, ta_read_at, ta_size, ta_sync_obj, ta_sync_dir,
    ta_publish, ta_remove, ta_truncate, ta_close, ta_size_status,
    /* enumerate: NULL = this mock backend cannot enumerate, so
     * callers must treat enumeration as unknown, never empty
     * (Gate 2 semantics). */
    NULL
};

static SegDB g_ta_db;
static uint8_t g_ta_arena[65536];
static uint8_t g_ta_wal[65536];
static SegDB g_ta_db2;
static uint8_t g_ta_arena2[65536];
static uint8_t g_ta_wal2[65536];

void test_tracka_publish_indeterminate_current() {
    printf("Running test_tracka_publish_indeterminate_current...\n");
    YowzaStore* inner = yowza_store_mem_create();
    TaState fs;
    YowzaStore wrap;
    int rc;
    assert(inner != NULL);
    fs.inner = inner; fs.mode = TA_OFF; fs.fired = 0;
    wrap.vt = &ta_vt; wrap.state = &fs;
    assert(segdb_init(&g_ta_db, &wrap, g_ta_arena, sizeof(g_ta_arena), g_ta_wal, sizeof(g_ta_wal)) == 0);
    t_put(&g_ta_db, "A", "a");
    assert(segdb_flush(&g_ta_db) == 0);
    t_put(&g_ta_db, "B", "b");
    fs.mode = TA_INDET_AT_CURRENT; fs.fired = 0;
    rc = segdb_flush(&g_ta_db);
    assert(rc == SEGDB_INDETERMINATE && fs.fired);
    assert(segdb_indeterminate(&g_ta_db) == 1);
    /* Writes are refused while indeterminate; reads still work. */
    assert(segdb_put(&g_ta_db, (const uint8_t*)"C", 1, (const uint8_t*)"c", 1) == SEGDB_INDETERMINATE);
    assert(t_has(&g_ta_db, "B", "b"));
    /* Reconcile: CURRENT was renamed, so the pending manifest wins. */
    assert(segdb_reconcile(&g_ta_db) == 0);
    assert(segdb_indeterminate(&g_ta_db) == 0);
    assert(t_has(&g_ta_db, "B", "b")); /* now from the segment, hot reset */
    assert(t_has(&g_ta_db, "A", "a"));
    t_put(&g_ta_db, "C", "c");
    assert(t_has(&g_ta_db, "C", "c"));
    segdb_close(&g_ta_db);
    yowza_store_close(inner);
    printf("Passed.\n");
}

void test_tracka_publish_definite_failure() {
    printf("Running test_tracka_publish_definite_failure...\n");
    YowzaStore* inner = yowza_store_mem_create();
    TaState fs;
    YowzaStore wrap;
    int rc;
    assert(inner != NULL);
    fs.inner = inner; fs.mode = TA_OFF; fs.fired = 0;
    wrap.vt = &ta_vt; wrap.state = &fs;
    assert(segdb_init(&g_ta_db, &wrap, g_ta_arena, sizeof(g_ta_arena), g_ta_wal, sizeof(g_ta_wal)) == 0);
    t_put(&g_ta_db, "A", "a");
    assert(segdb_flush(&g_ta_db) == 0);
    t_put(&g_ta_db, "B", "b");
    fs.mode = TA_DEFINITE_MANIFEST; fs.fired = 0;
    rc = segdb_flush(&g_ta_db);
    assert(rc == -1 && fs.fired);
    assert(segdb_indeterminate(&g_ta_db) == 0);
    assert(segdb_reconcile(&g_ta_db) == 0); /* not indeterminate: no-op */
    /* Hot intact, old manifest current: B still visible from hot,
     * and a retry with the fault off succeeds. */
    assert(t_has(&g_ta_db, "B", "b"));
    fs.mode = TA_OFF;
    assert(segdb_flush(&g_ta_db) == 0);
    assert(t_has(&g_ta_db, "B", "b"));
    segdb_close(&g_ta_db);
    yowza_store_close(inner);
    printf("Passed.\n");
}

void test_tracka_current_bak_recovery() {
    printf("Running test_tracka_current_bak_recovery...\n");
    YowzaStore* inner = yowza_store_mem_create();
    assert(inner != NULL);
    assert(segdb_init(&g_ta_db, inner, g_ta_arena, sizeof(g_ta_arena), g_ta_wal, sizeof(g_ta_wal)) == 0);
    t_put(&g_ta_db, "A", "a");
    assert(segdb_flush(&g_ta_db) == 0);
    t_put(&g_ta_db, "B", "b");
    assert(segdb_flush(&g_ta_db) == 0);
    segdb_close(&g_ta_db);
    /* Lose exactly CURRENT: backup must recover gen 2 (A + B). */
    assert(inner->vt->remove(inner->state, "CURRENT") == 0);
    assert(segdb_init(&g_ta_db2, inner, g_ta_arena2, sizeof(g_ta_arena2), g_ta_wal2, sizeof(g_ta_wal2)) == 0);
    assert(segdb_recover(&g_ta_db2) == 0);
    assert(t_has(&g_ta_db2, "A", "a"));
    assert(t_has(&g_ta_db2, "B", "b"));
    segdb_close(&g_ta_db2);
    yowza_store_close(inner);
    printf("Passed.\n");
}

void test_tracka_io_error_no_silent_fallback() {
    printf("Running test_tracka_io_error_no_silent_fallback...\n");
    YowzaStore* inner = yowza_store_mem_create();
    TaState fs;
    YowzaStore wrap;
    assert(inner != NULL);
    fs.inner = inner; fs.mode = TA_OFF; fs.fired = 0;
    wrap.vt = &ta_vt; wrap.state = &fs;
    assert(segdb_init(&g_ta_db, &wrap, g_ta_arena, sizeof(g_ta_arena), g_ta_wal, sizeof(g_ta_wal)) == 0);
    t_put(&g_ta_db, "A", "a");
    assert(segdb_flush(&g_ta_db) == 0);
    segdb_close(&g_ta_db);
    /* Now make CURRENT metadata reads fail with IO_ERROR: recovery
     * must fail explicitly, never succeed empty or fall back. */
    fs.mode = TA_IOERR_CURRENT;
    assert(segdb_init(&g_ta_db2, &wrap, g_ta_arena2, sizeof(g_ta_arena2), g_ta_wal2, sizeof(g_ta_wal2)) == 0);
    assert(segdb_recover(&g_ta_db2) == -1);
    segdb_close(&g_ta_db2);
    yowza_store_close(inner);
    printf("Passed.\n");
}

void test_tracka_double_loss_is_virgin_indistinguishable() {
    printf("Running test_tracka_double_loss_is_virgin_indistinguishable...\n");
    YowzaStore* inner = yowza_store_mem_create();
    assert(inner != NULL);
    assert(segdb_init(&g_ta_db, inner, g_ta_arena, sizeof(g_ta_arena), g_ta_wal, sizeof(g_ta_wal)) == 0);
    t_put(&g_ta_db, "A", "a");
    assert(segdb_flush(&g_ta_db) == 0);
    segdb_close(&g_ta_db);
    /* Remove CURRENT, CURRENT.bak, and every manifest object.
     * O2 CHANGE (was: virgin success): with the Gate 2 helpers
     * wired into segdb_recover, a surviving segment object is
     * prior content, so recovery FAILS (-1) instead of opening
     * a virgin empty DB that would silently drop "A". The old
     * virgin-success expectation is superseded by
     * test_recovery_wire_triple_loss_fails; this test now pins
     * the new failure behaviour. */
    assert(inner->vt->remove(inner->state, "CURRENT") == 0);
    assert(inner->vt->remove(inner->state, "CURRENT.bak") == 0);
    assert(inner->vt->remove(inner->state, "manifest.1") == 0);
    assert(segdb_init(&g_ta_db2, inner, g_ta_arena2, sizeof(g_ta_arena2), g_ta_wal2, sizeof(g_ta_wal2)) == 0);
    assert(segdb_recover(&g_ta_db2) == -1);
    segdb_close(&g_ta_db2);
    yowza_store_close(inner);
    printf("Passed.\n");
}

#ifndef __wasm__
void test_tracka_persistent_wal_tombstone() {
    printf("Running test_tracka_persistent_wal_tombstone...\n");
    const char* walpath = "/tmp/yowza_tracka_wal_test.wal";
    YowzaStore* inner = yowza_store_mem_create();
    StorageDriver drv;
    assert(inner != NULL);
    remove(walpath);
    drv = storage_posix_create(walpath);
    assert(drv.append != NULL);
    assert(segdb_init_with_driver(&g_ta_db, inner, g_ta_arena, sizeof(g_ta_arena), drv) == 0);
    t_put(&g_ta_db, "segkey", "old");
    assert(segdb_flush(&g_ta_db) == 0);
    t_put(&g_ta_db, "hotkey", "hot");
    /* Delete of a segment-resident key: hot tombstone + legacy DEL
     * in the PERSISTENT WAL (not flushed). */
    assert(segdb_delete(&g_ta_db, (const uint8_t*)"segkey", 6) == 0);
    assert(t_missing(&g_ta_db, "segkey"));
    segdb_close(&g_ta_db); /* closes the POSIX WAL driver; file persists */
    /* Full process-exit equivalent: fresh SegDB, fresh WAL driver
     * over the same file, same store. */
    drv = storage_posix_create(walpath);
    assert(drv.append != NULL);
    assert(segdb_init_with_driver(&g_ta_db2, inner, g_ta_arena2, sizeof(g_ta_arena2), drv) == 0);
    assert(segdb_recover(&g_ta_db2) == 0);
    /* The old segment value must NOT resurface: the tombstone was
     * reconstructed from the persistent WAL. */
    assert(t_missing(&g_ta_db2, "segkey"));
    /* And the unflushed put survives through the persistent WAL. */
    assert(t_has(&g_ta_db2, "hotkey", "hot"));
    segdb_close(&g_ta_db2);
    yowza_store_close(inner);
    remove(walpath);
    printf("Passed.\n");
}
#endif

/* ---------- streaming compaction (Track A item 3) ---------- */
static uint8_t g_ta_big_arena[1048576];
static uint8_t g_ta_big_wal[524288];
static uint8_t g_ta_scan_out[65536];

static void ta_big_key(char* buf, uint32_t idx) {
    /* "k%06d" */
    uint32_t d[6];
    int i;
    for (i = 5; i >= 0; i--) { d[i] = idx % 10; idx /= 10; }
    buf[0] = 'k';
    for (i = 0; i < 6; i++) buf[1 + i] = (char)('0' + d[i]);
    buf[7] = '\0';
}

static void ta_big_val(char* buf, uint32_t idx) {
    uint32_t d[6];
    int i;
    for (i = 5; i >= 0; i--) { d[i] = idx % 10; idx /= 10; }
    buf[0] = 'v';
    for (i = 0; i < 6; i++) buf[1 + i] = (char)('0' + d[i]);
    buf[7] = '\0';
}

/* ---------- multi-level manifest (Track A item 3) ---------- */
static void ta_m_key(char* buf, uint32_t idx) {
    buf[0] = 'm';
    buf[1] = (char)('0' + (idx / 100) % 10);
    buf[2] = (char)('0' + (idx / 10) % 10);
    buf[3] = (char)('0' + idx % 10);
    buf[4] = '\0';
}

static void ta_m_val(char* buf, uint32_t idx) {
    buf[0] = 'v';
    buf[1] = (char)('0' + (idx / 100) % 10);
    buf[2] = (char)('0' + (idx / 10) % 10);
    buf[3] = (char)('0' + idx % 10);
    buf[4] = '\0';
}

void test_tracka_multi_level_manifest() {
    printf("Running test_tracka_multi_level_manifest...\n");
    {
        YowzaStore* inner = yowza_store_mem_create();
        uint32_t i;
        uint32_t count = 0;
        char kbuf[8], vbuf[8];
        assert(inner != NULL);
        assert(segdb_init(&g_ta_db, inner, g_ta_arena, sizeof(g_ta_arena),
                          g_ta_wal, sizeof(g_ta_wal)) == 0);
        /* 128 single-entry flushes fill the single manifest. */
        for (i = 0; i < 128; i++) {
            ta_m_key(kbuf, i);
            ta_m_val(vbuf, i);
            t_put(&g_ta_db, kbuf, vbuf);
            assert(segdb_flush(&g_ta_db) == 0);
        }
        assert(g_ta_db.manifest.segment_count == 128);
        assert(g_ta_db.has_dir == 0);
        /* 129th flush transitions to directory mode. */
        ta_m_key(kbuf, 128);
        ta_m_val(vbuf, 128);
        t_put(&g_ta_db, kbuf, vbuf);
        assert(segdb_flush(&g_ta_db) == 0);
        assert(g_ta_db.has_dir == 1);
        assert(g_ta_db.group_count == 2);
        assert(g_ta_db.manifest.segment_count == 1);
        /* 130th flush appends to the last group. */
        ta_m_key(kbuf, 129);
        ta_m_val(vbuf, 129);
        t_put(&g_ta_db, kbuf, vbuf);
        assert(segdb_flush(&g_ta_db) == 0);
        assert(g_ta_db.group_count == 2);
        assert(g_ta_db.manifest.segment_count == 2);
        for (i = 0; i < 130; i++) {
            ta_m_key(kbuf, i);
            ta_m_val(vbuf, i);
            assert(t_has(&g_ta_db, kbuf, vbuf));
        }
        assert(segdb_scan(&g_ta_db, (const uint8_t*)"", 0, 0,
                          g_ta_scan_out, sizeof(g_ta_scan_out), &count) == 0);
        assert(count == 130);
        /* Recover into a fresh SegDB: directory mode survives. */
        segdb_close(&g_ta_db);
        assert(segdb_init(&g_ta_db2, inner, g_ta_arena2, sizeof(g_ta_arena2),
                          g_ta_wal2, sizeof(g_ta_wal2)) == 0);
        assert(segdb_recover(&g_ta_db2) == 0);
        assert(g_ta_db2.has_dir == 1);
        assert(g_ta_db2.group_count == 2);
        for (i = 0; i < 130; i++) {
            ta_m_key(kbuf, i);
            ta_m_val(vbuf, i);
            assert(t_has(&g_ta_db2, kbuf, vbuf));
        }
        /* Compact in directory mode: 130 entries -> 1 segment,
         * 1 group; every key still readable. */
        assert(segdb_compact(&g_ta_db2, 0) == 0);
        assert(g_ta_db2.has_dir == 1);
        assert(g_ta_db2.group_count == 1);
        assert(g_ta_db2.manifest.segment_count == 1);
        for (i = 0; i < 130; i++) {
            ta_m_key(kbuf, i);
            ta_m_val(vbuf, i);
            assert(t_has(&g_ta_db2, kbuf, vbuf));
        }
        assert(segdb_scan(&g_ta_db2, (const uint8_t*)"", 0, 0,
                          g_ta_scan_out, sizeof(g_ta_scan_out), &count) == 0);
        assert(count == 130);
        segdb_close(&g_ta_db2);
        yowza_store_close(inner);
    }
    printf("Passed.\n");
}

void test_tracka_transition_indeterminate() {
    printf("Running test_tracka_transition_indeterminate...\n");
    {
        YowzaStore* inner = yowza_store_mem_create();
        TaState fs;
        YowzaStore wrap;
        uint32_t i;
        char kbuf[8], vbuf[8];
        assert(inner != NULL);
        fs.inner = inner; fs.mode = TA_OFF; fs.fired = 0;
        wrap.vt = &ta_vt; wrap.state = &fs;
        assert(segdb_init(&g_ta_db, &wrap, g_ta_arena, sizeof(g_ta_arena),
                          g_ta_wal, sizeof(g_ta_wal)) == 0);
        for (i = 0; i < 128; i++) {
            ta_m_key(kbuf, i);
            ta_m_val(vbuf, i);
            t_put(&g_ta_db, kbuf, vbuf);
            assert(segdb_flush(&g_ta_db) == 0);
        }
        /* Arm the fault: the transition's CURRENT publish renames
         * but reports -2 (indeterminate). */
        fs.mode = TA_INDET_AT_CURRENT;
        fs.fired = 0;
        ta_m_key(kbuf, 128);
        ta_m_val(vbuf, 128);
        t_put(&g_ta_db, kbuf, vbuf);
        assert(segdb_flush(&g_ta_db) == SEGDB_INDETERMINATE);
        assert(fs.fired);
        assert(segdb_indeterminate(&g_ta_db) == 1);
        /* Writes refused while indeterminate; reads still work. */
        assert(segdb_put(&g_ta_db, (const uint8_t*)"zz", 2,
                         (const uint8_t*)"z", 1) == SEGDB_INDETERMINATE);
        assert(t_has(&g_ta_db, "m000", "v000"));
        /* Reconcile: CURRENT really does name manifest.dir, so the
         * transition is adopted (hot reset, new key in a segment). */
        assert(segdb_reconcile(&g_ta_db) == 0);
        assert(segdb_indeterminate(&g_ta_db) == 0);
        assert(g_ta_db.has_dir == 1);
        assert(g_ta_db.group_count == 2);
        assert(t_has(&g_ta_db, "m128", "v128"));
        assert(t_has(&g_ta_db, "m000", "v000"));
        /* Further flushes (group append) work after reconcile. */
        t_put(&g_ta_db, "m129", "v129");
        fs.mode = TA_OFF;
        assert(segdb_flush(&g_ta_db) == 0);
        assert(t_has(&g_ta_db, "m129", "v129"));
        segdb_close(&g_ta_db);
        yowza_store_close(inner);
    }
    printf("Passed.\n");
}

void test_tracka_streaming_compact_big() {
    printf("Running test_tracka_streaming_compact_big...\n");
    {
        YowzaStore* inner = yowza_store_mem_create();
        uint32_t batch, i;
        uint32_t count = 0;
        uint64_t barrier;
        char kbuf[8], vbuf[8];
        assert(inner != NULL);
        assert(segdb_init(&g_ta_db, inner, g_ta_big_arena, sizeof(g_ta_big_arena),
                          g_ta_big_wal, sizeof(g_ta_big_wal)) == 0);
        /* 3 flushes x 4000 entries = 12000 entries: above the old
         * 8192-candidate cap, so the old compact would fail here. */
        for (batch = 0; batch < 3; batch++) {
            for (i = 0; i < 4000; i++) {
                uint32_t idx = batch * 4000 + i;
                ta_big_key(kbuf, idx);
                ta_big_val(vbuf, idx);
                t_put(&g_ta_db, kbuf, vbuf);
            }
            assert(segdb_flush(&g_ta_db) == 0);
        }
        assert(g_ta_db.manifest.segment_count == 3);
        /* Segment-resident tombstone: delete + flush, then compact
         * with a barrier at/above its sequence so it is dropped. */
        assert(segdb_delete(&g_ta_db, (const uint8_t*)"k006000", 7) == 0);
        assert(segdb_flush(&g_ta_db) == 0);
        assert(g_ta_db.manifest.segment_count == 4);
        barrier = g_ta_db.seq_base; /* checkpoint after the tombstone flush */
        assert(segdb_compact(&g_ta_db, barrier) == 0);
        /* 11999 live entries -> ceil(11999/4096) = 3 segments. */
        assert(g_ta_db.manifest.segment_count == 3);
        assert(t_missing(&g_ta_db, "k006000"));
        assert(t_has(&g_ta_db, "k000000", "v000000"));
        assert(t_has(&g_ta_db, "k005999", "v005999"));
        assert(t_has(&g_ta_db, "k006001", "v006001"));
        assert(t_has(&g_ta_db, "k011999", "v011999"));
        /* Prefix scans (within the remaining scan cap) verify
         * whole ranges, not just spot keys. */
        assert(segdb_scan(&g_ta_db, (const uint8_t*)"k001", 4, 0,
                          g_ta_scan_out, sizeof(g_ta_scan_out), &count) == 0);
        assert(count == 1000);
        assert(segdb_scan(&g_ta_db, (const uint8_t*)"k011", 4, 0,
                          g_ta_scan_out, sizeof(g_ta_scan_out), &count) == 0);
        assert(count == 1000);
        segdb_close(&g_ta_db);
        yowza_store_close(inner);
    }
    printf("Passed.\n");
}
