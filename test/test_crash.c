/* Phase 2, Track C crash simulation — fault-injecting store wrapper
 * INSIDE this test file, wrapping a real Track A YowzaStore (the
 * memory backend). No production code is touched by the faults.
 *
 * Standalone build:
 *   clang -O2 -Iinclude -Wall -Wextra -DSEGDB_STANDALONE_MAIN \
 *     test/test_crash.c src/arena.c src/art.c src/storage_memory.c \
 *     src/storage_posix.c src/tx.c src/vm.c src/yowza_db.c \
 *     src/segdb.c src/segment.c src/manifest.c src/storage2.c \
 *     -o /tmp/test_crash && /tmp/test_crash
 * Integrator: declare the test_crash_* functions in test/main.c and
 * call them; compile WITHOUT -DSEGDB_STANDALONE_MAIN.
 *
 * Oracle for every fault test: build a DB, flush cleanly (old state
 * = keys A), put more keys (new state = A + B), run flush/compact
 * with the fault armed, then segdb_recover on a FRESH SegDB over the
 * same inner store and assert the recovered state is EXACTLY the old
 * valid state OR exactly the new valid state, never a mix. The
 * individual tests assert the stronger exact outcome their fault
 * point implies (before CURRENT publish => old; after CURRENT
 * publish => new). */
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include "segdb.h"

/* ---------- helpers ---------- */
static void c_put(SegDB* db, const char* k, const char* v) {
    assert(segdb_put(db, (const uint8_t*)k, (uint32_t)strlen(k),
                     (const uint8_t*)v, (uint32_t)strlen(v)) == 0);
}

static int c_has(SegDB* db, const char* k, const char* want) {
    uint8_t* out = NULL;
    uint32_t len = 0;
    if (segdb_get(db, (const uint8_t*)k, (uint32_t)strlen(k), &out, &len) != 0) return 0;
    return len == strlen(want) && (len == 0 || memcmp(out, want, len) == 0);
}

static int c_missing(SegDB* db, const char* k) {
    uint8_t* out = NULL;
    uint32_t len = 0;
    return segdb_get(db, (const uint8_t*)k, (uint32_t)strlen(k), &out, &len) != 0;
}

/* Flip one byte of an object through the inner store's vtable. */
static void flip_byte(YowzaStore* st, const char* obj, uint64_t off) {
    uint8_t b = 0;
    assert(st->vt->read_at(st->state, obj, off, &b, 1) == 0);
    b = (uint8_t)(b ^ 0xFF);
    assert(st->vt->write_at(st->state, obj, off, &b, 1) == 0);
}

/* ---------- fault-injecting wrapper ---------- */
enum {
    F_NONE = 0,
    F_MID_SEG_WRITE,      /* (a) fail a write_at to seg.* mid seg_write */
    F_BEFORE_MANIFEST,    /* (b) fail manifest tmp write (after seg sync) */
    F_DURING_PUBLISH,     /* (c) fail manifest publish (rename) */
    F_AFTER_CURRENT,      /* (d) fail sync_dir after CURRENT published */
    F_BEFORE_REMOVE       /* (e) fail old-segment removal in compact */
};

typedef struct {
    YowzaStore* inner;
    int mode;
    int fired;
    int current_published;
} FaultState;

static int name_starts(const char* s, const char* pfx) {
    size_t i = 0;
    while (pfx[i] != '\0') {
        if (s[i] != pfx[i]) return 0;
        i++;
    }
    return 1;
}

static int name_ends(const char* s, const char* sfx) {
    size_t sl = strlen(s), fl = strlen(sfx);
    if (fl > sl) return 0;
    return strcmp(s + sl - fl, sfx) == 0;
}

static int f_write_at(void* vstate, const char* obj, uint64_t off,
                      const uint8_t* data, uint64_t len) {
    FaultState* fs = (FaultState*)vstate;
    YowzaStore* in = fs->inner;
    if (fs->mode == F_MID_SEG_WRITE && !fs->fired && name_starts(obj, "seg.")) {
        /* Simulate a torn mid-block write: write the first half,
         * then report failure. seg_write must fail and the partial
         * object must stay unreferenced. */
        uint64_t half = len / 2;
        fs->fired = 1;
        if (half > 0) {
            (void)in->vt->write_at(in->state, obj, off, data, half);
        }
        return -1;
    }
    if (fs->mode == F_BEFORE_MANIFEST && !fs->fired &&
        name_starts(obj, "manifest.") && name_ends(obj, ".tmp")) {
        fs->fired = 1;
        return -1; /* manifest tmp never written: no publish possible */
    }
    return in->vt->write_at(in->state, obj, off, data, len);
}

static int f_read_at(void* vstate, const char* obj, uint64_t off, uint8_t* out, uint64_t len) {
    FaultState* fs = (FaultState*)vstate;
    return fs->inner->vt->read_at(fs->inner->state, obj, off, out, len);
}

static int f_size(void* vstate, const char* obj, uint64_t* out_size) {
    FaultState* fs = (FaultState*)vstate;
    return fs->inner->vt->size(fs->inner->state, obj, out_size);
}

static int f_sync_obj(void* vstate, const char* obj) {
    FaultState* fs = (FaultState*)vstate;
    return fs->inner->vt->sync_obj(fs->inner->state, obj);
}

static int f_sync_dir(void* vstate) {
    FaultState* fs = (FaultState*)vstate;
    /* Fire ONLY after CURRENT was published: manifest_publish's own
     * internal sync_dir calls happen before its publish returns, so
     * they see current_published == 0 and succeed. The segdb-level
     * sync_dir after CURRENT publish is the fault point (d). */
    if (fs->mode == F_AFTER_CURRENT && fs->current_published && !fs->fired) {
        fs->fired = 1;
        return -1;
    }
    return fs->inner->vt->sync_dir(fs->inner->state);
}

static int f_publish(void* vstate, const char* tmp_obj, const char* final_obj) {
    FaultState* fs = (FaultState*)vstate;
    int rc;
    if (fs->mode == F_DURING_PUBLISH && !fs->fired && name_starts(final_obj, "manifest.")) {
        fs->fired = 1;
        return -1; /* rename fails: CURRENT still points at old manifest */
    }
    rc = fs->inner->vt->publish(fs->inner->state, tmp_obj, final_obj);
    if (rc == 0 && strcmp(final_obj, "CURRENT") == 0) fs->current_published = 1;
    return rc;
}

static int f_remove(void* vstate, const char* obj) {
    FaultState* fs = (FaultState*)vstate;
    if (fs->mode == F_BEFORE_REMOVE && !fs->fired) {
        fs->fired = 1;
        return -1; /* first old-segment removal fails, after CURRENT */
    }
    return fs->inner->vt->remove(fs->inner->state, obj);
}

static int f_truncate(void* vstate, const char* obj, uint64_t len) {
    FaultState* fs = (FaultState*)vstate;
    return fs->inner->vt->truncate(fs->inner->state, obj, len);
}

static void f_close(void* vstate) {
    (void)vstate; /* inner store is closed by the test, not the wrapper */
}

static int f_size_status(void* vstate, const char* obj, uint64_t* out_size) {
    FaultState* fs = (FaultState*)vstate;
    if (fs->inner->vt->size_status)
        return fs->inner->vt->size_status(fs->inner->state, obj, out_size);
    return YOWZA_STORE_UNKNOWN;
}

static const YowzaStoreVTable fault_vtable = {
    f_write_at, f_read_at, f_size, f_sync_obj, f_sync_dir,
    f_publish, f_remove, f_truncate, f_close, f_size_status,
    /* enumerate: NULL = this mock backend cannot enumerate, so
     * callers must treat enumeration as unknown, never empty
     * (Gate 2 semantics; same explicit NULL as the product
     * heap-store vtable in yowza_segdb.c). */
    NULL
};

/* Build a segdb over the fault wrapper, flush keys A cleanly, then
 * arm `mode` and attempt a flush of keys B. Returns the flush rc. */
static SegDB g_db;
static uint8_t g_arena[65536];
static uint8_t g_wal[65536];
static SegDB g_db2;
static uint8_t g_arena2[65536];
static uint8_t g_wal2[65536];

static int crash_setup_flush_b(YowzaStore* inner, YowzaStore* wrap, FaultState* fs,
                               int mode) {
    int rc;
    assert(segdb_init(&g_db, wrap, g_arena, sizeof(g_arena), g_wal, sizeof(g_wal)) == 0);
    fs->mode = F_NONE;
    c_put(&g_db, "A", "a");
    assert(segdb_flush(&g_db) == 0);
    c_put(&g_db, "B", "b");
    fs->mode = mode;
    fs->fired = 0;
    fs->current_published = 0;
    rc = segdb_flush(&g_db);
    segdb_close(&g_db);
    (void)inner;
    return rc;
}

static void crash_recover(YowzaStore* inner) {
    assert(segdb_init(&g_db2, inner, g_arena2, sizeof(g_arena2), g_wal2, sizeof(g_wal2)) == 0);
    assert(segdb_recover(&g_db2) == 0);
}

void test_crash_mid_seg_write() {
    printf("Running test_crash_mid_seg_write...\n");
    YowzaStore* inner = yowza_store_mem_create();
    FaultState fs;
    YowzaStore wrap;
    int rc;
    assert(inner != NULL);
    fs.inner = inner; fs.mode = F_NONE; fs.fired = 0; fs.current_published = 0;
    wrap.vt = &fault_vtable; wrap.state = &fs;
    rc = crash_setup_flush_b(inner, &wrap, &fs, F_MID_SEG_WRITE);
    assert(rc != 0 && fs.fired);
    crash_recover(inner);
    /* Old state exactly: A present, B absent — never a mix. */
    assert(c_has(&g_db2, "A", "a"));
    assert(c_missing(&g_db2, "B"));
    segdb_close(&g_db2);
    yowza_store_close(inner);
    printf("Passed.\n");
}

void test_crash_before_manifest_publish() {
    printf("Running test_crash_before_manifest_publish...\n");
    YowzaStore* inner = yowza_store_mem_create();
    FaultState fs;
    YowzaStore wrap;
    int rc;
    assert(inner != NULL);
    fs.inner = inner; fs.mode = F_NONE; fs.fired = 0; fs.current_published = 0;
    wrap.vt = &fault_vtable; wrap.state = &fs;
    rc = crash_setup_flush_b(inner, &wrap, &fs, F_BEFORE_MANIFEST);
    assert(rc != 0 && fs.fired);
    crash_recover(inner);
    assert(c_has(&g_db2, "A", "a"));
    assert(c_missing(&g_db2, "B"));
    segdb_close(&g_db2);
    yowza_store_close(inner);
    printf("Passed.\n");
}

void test_crash_during_publish() {
    printf("Running test_crash_during_publish...\n");
    YowzaStore* inner = yowza_store_mem_create();
    FaultState fs;
    YowzaStore wrap;
    int rc;
    assert(inner != NULL);
    fs.inner = inner; fs.mode = F_NONE; fs.fired = 0; fs.current_published = 0;
    wrap.vt = &fault_vtable; wrap.state = &fs;
    rc = crash_setup_flush_b(inner, &wrap, &fs, F_DURING_PUBLISH);
    assert(rc != 0 && fs.fired);
    crash_recover(inner);
    assert(c_has(&g_db2, "A", "a"));
    assert(c_missing(&g_db2, "B"));
    segdb_close(&g_db2);
    yowza_store_close(inner);
    printf("Passed.\n");
}

void test_crash_after_current_before_wal_clear() {
    printf("Running test_crash_after_current_before_wal_clear...\n");
    YowzaStore* inner = yowza_store_mem_create();
    FaultState fs;
    YowzaStore wrap;
    int rc;
    assert(inner != NULL);
    fs.inner = inner; fs.mode = F_NONE; fs.fired = 0; fs.current_published = 0;
    wrap.vt = &fault_vtable; wrap.state = &fs;
    /* Fault (d) in the assignment is "after CURRENT publish but
     * before WAL clear". In segdb_flush the WAL clear happens after
     * the segdb-level sync_dir, so failing that sync_dir is the
     * exact boundary: CURRENT is already durable, the flush reports
     * failure, and recovery must see the NEW state. (The old DB's
     * hot WAL is uncleared in-process, but a fresh DB recovers from
     * segments, which is the durable truth.) */
    rc = crash_setup_flush_b(inner, &wrap, &fs, F_AFTER_CURRENT);
    assert(rc != 0 && fs.fired);
    crash_recover(inner);
    assert(c_has(&g_db2, "A", "a"));
    assert(c_has(&g_db2, "B", "b"));
    segdb_close(&g_db2);
    yowza_store_close(inner);
    printf("Passed.\n");
}

void test_crash_before_old_segment_removal() {
    printf("Running test_crash_before_old_segment_removal...\n");
    YowzaStore* inner = yowza_store_mem_create();
    FaultState fs;
    YowzaStore wrap;
    int rc;
    assert(inner != NULL);
    fs.inner = inner; fs.mode = F_NONE; fs.fired = 0; fs.current_published = 0;
    wrap.vt = &fault_vtable; wrap.state = &fs;
    assert(segdb_init(&g_db, &wrap, g_arena, sizeof(g_arena), g_wal, sizeof(g_wal)) == 0);
    c_put(&g_db, "A", "a");
    assert(segdb_flush(&g_db) == 0);
    c_put(&g_db, "B", "b");
    assert(segdb_flush(&g_db) == 0);
    assert(g_db.manifest.segment_count == 2);
    fs.mode = F_BEFORE_REMOVE;
    fs.fired = 0;
    fs.current_published = 0;
    rc = segdb_compact(&g_db, 0);
    assert(rc != 0 && fs.fired); /* remove failed, but CURRENT is new */
    segdb_close(&g_db);
    crash_recover(inner);
    /* New state exactly: both keys, merged into one segment. */
    assert(c_has(&g_db2, "A", "a"));
    assert(c_has(&g_db2, "B", "b"));
    assert(g_db2.manifest.segment_count == 1);
    segdb_close(&g_db2);
    yowza_store_close(inner);
    printf("Passed.\n");
}

void test_crash_manifest_corruption_fallback() {
    printf("Running test_crash_manifest_corruption_fallback...\n");
    YowzaStore* inner = yowza_store_mem_create();
    char cur[128];
    assert(inner != NULL);
    assert(segdb_init(&g_db, inner, g_arena, sizeof(g_arena), g_wal, sizeof(g_wal)) == 0);
    c_put(&g_db, "A", "a");
    assert(segdb_flush(&g_db) == 0); /* gen 1 */
    c_put(&g_db, "B", "b");
    assert(segdb_flush(&g_db) == 0); /* gen 2 */
    assert(manifest_read_current(inner, "CURRENT", cur, sizeof(cur)) == 0);
    segdb_close(&g_db);
    /* Corrupt the current (gen 2) manifest: recovery must fall back
     * to gen 1 — A only — never partial gen-2 data. */
    flip_byte(inner, cur, 20);
    assert(segdb_init(&g_db2, inner, g_arena2, sizeof(g_arena2), g_wal2, sizeof(g_wal2)) == 0);
    assert(segdb_recover(&g_db2) == 0);
    assert(g_db2.manifest.generation == 1);
    assert(c_has(&g_db2, "A", "a"));
    assert(c_missing(&g_db2, "B"));
    segdb_close(&g_db2);
    /* Corrupt gen 1 as well: recovery must now fail explicitly. */
    flip_byte(inner, "manifest.1", 20);
    assert(segdb_init(&g_db2, inner, g_arena2, sizeof(g_arena2), g_wal2, sizeof(g_wal2)) == 0);
    assert(segdb_recover(&g_db2) == -1);
    segdb_close(&g_db2);
    yowza_store_close(inner);
    printf("Passed.\n");
}

void test_crash_segment_corruption() {
    printf("Running test_crash_segment_corruption...\n");
    /* Header flip: seg_open itself must fail. */
    {
        YowzaStore* inner = yowza_store_mem_create();
        char seg_obj[128];
        SegReader r;
        assert(inner != NULL);
        assert(segdb_init(&g_db, inner, g_arena, sizeof(g_arena), g_wal, sizeof(g_wal)) == 0);
        c_put(&g_db, "A", "a");
        c_put(&g_db, "B", "b");
        assert(segdb_flush(&g_db) == 0);
        assert(strlen(g_db.manifest.segments[0].obj) < sizeof(seg_obj));
        strcpy(seg_obj, g_db.manifest.segments[0].obj);
        segdb_close(&g_db);
        flip_byte(inner, seg_obj, 10); /* inside the 64-byte header */
        assert(seg_open(inner, seg_obj, &r) == -1);
        assert(segdb_init(&g_db2, inner, g_arena2, sizeof(g_arena2), g_wal2, sizeof(g_wal2)) == 0);
        assert(segdb_recover(&g_db2) == -1);
        segdb_close(&g_db2);
        yowza_store_close(inner);
    }
    /* Block flip: seg_open may pass (header/footer intact), but
     * full discovery / iteration must fail, and so must recovery. */
    {
        YowzaStore* inner = yowza_store_mem_create();
        char seg_obj[128];
        SegReader r;
        SegEntry e;
        uint8_t kb[1024], vb[1024];
        assert(inner != NULL);
        assert(segdb_init(&g_db, inner, g_arena, sizeof(g_arena), g_wal, sizeof(g_wal)) == 0);
        c_put(&g_db, "A", "a");
        c_put(&g_db, "B", "b");
        assert(segdb_flush(&g_db) == 0);
        strcpy(seg_obj, g_db.manifest.segments[0].obj);
        segdb_close(&g_db);
        flip_byte(inner, seg_obj, 100); /* inside block entry bytes */
        assert(seg_open(inner, seg_obj, &r) == 0);
        assert(seg_ensure_discovered(&r) == -1);
        seg_close(&r);
        assert(seg_open(inner, seg_obj, &r) == 0);
        assert(seg_iter_next(&r, &e, kb, sizeof(kb), vb, sizeof(vb)) == -1);
        seg_close(&r);
        assert(segdb_init(&g_db2, inner, g_arena2, sizeof(g_arena2), g_wal2, sizeof(g_wal2)) == 0);
        assert(segdb_recover(&g_db2) == -1);
        segdb_close(&g_db2);
        yowza_store_close(inner);
    }
    printf("Passed.\n");
}

#ifdef SEGDB_STANDALONE_MAIN
int main(void) {
    test_crash_mid_seg_write();
    test_crash_before_manifest_publish();
    test_crash_during_publish();
    test_crash_after_current_before_wal_clear();
    test_crash_before_old_segment_removal();
    test_crash_manifest_corruption_fallback();
    test_crash_segment_corruption();
    printf("\nAll Crash Tests Passed!\n");
    return 0;
}
#endif
