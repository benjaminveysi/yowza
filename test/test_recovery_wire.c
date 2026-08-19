/* O2 tests: segdb_recover wired to the Gate 2 helpers
 * (manifest_recover_pointer / manifest_recover_manifest).
 * Registered in test/main.c by the parent (plain tests, no
 * main here). Conventions follow the existing C tests:
 * assert() for hard failures, printf() banners, file-scope
 * statics for SegDBs (Manifest structs are ~285 KiB and
 * must never sit on a small stack — SegDB itself embeds
 * one, hence the static DBs below).
 *
 * These tests pin the O2 change, including the one
 * intentional semantic break: the old Track A double-loss
 * test expected CURRENT + CURRENT.bak + manifest.1 deleted
 * with a segment left behind to open as a virgin empty DB.
 * Under the wired helpers that store now FAILS recovery
 * (ambiguous: prior content survives its pointers), and
 * only an authoritatively empty store opens virgin. The
 * old test_tracka_double_loss_is_virgin_indistinguishable
 * expectation is therefore superseded by
 * test_recovery_wire_triple_loss_fails below; it is NOT
 * updated here because test_tracka.c is outside O2's file
 * set — the parent must update or remove that one test.
 */

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include "segdb.h"
#include "manifest.h"
#include "storage2.h"

static SegDB g_rw_db;
static uint8_t g_rw_arena[65536];
static uint8_t g_rw_wal[65536];
static SegDB g_rw_db2;
static uint8_t g_rw_arena2[65536];
static uint8_t g_rw_wal2[65536];

static void rw_put(SegDB* db, const char* k, const char* v) {
    assert(segdb_put(db, (const uint8_t*)k, (uint32_t)strlen(k),
                     (const uint8_t*)v, (uint32_t)strlen(v)) == 0);
}

static int rw_has(SegDB* db, const char* k, const char* want) {
    uint8_t* out = NULL;
    uint32_t len = 0;
    if (segdb_get(db, (const uint8_t*)k, (uint32_t)strlen(k), &out, &len) != 0) return 0;
    return len == strlen(want) && (len == 0 || memcmp(out, want, len) == 0);
}

static int rw_missing(SegDB* db, const char* k) {
    uint8_t* out = NULL;
    uint32_t len = 0;
    return segdb_get(db, (const uint8_t*)k, (uint32_t)strlen(k), &out, &len) != 0;
}

static void rw_flip_byte(YowzaStore* st, const char* obj, uint64_t off) {
    uint8_t b = 0;
    assert(st->vt->read_at(st->state, obj, off, &b, 1) == 0);
    b = (uint8_t)(b ^ 0xFF);
    assert(st->vt->write_at(st->state, obj, off, &b, 1) == 0);
}

/* (b) CURRENT + CURRENT.bak + manifest.dir gone, but a
 * segment object survives -> recovery FAILS, never a
 * virgin empty DB. Two sub-cases: manifest.1 also deleted
 * (the old double-loss setup) and manifest.1 still present
 * (the old pointerless manifest.1 fallback must NOT adopt
 * a possibly stale first generation either). */
void test_recovery_wire_triple_loss_fails() {
    printf("Running test_recovery_wire_triple_loss_fails...\n");
    {
        YowzaStore* inner = yowza_store_mem_create();
        assert(inner != NULL);
        assert(segdb_init(&g_rw_db, inner, g_rw_arena, sizeof(g_rw_arena),
                          g_rw_wal, sizeof(g_rw_wal)) == 0);
        rw_put(&g_rw_db, "A", "a");
        assert(segdb_flush(&g_rw_db) == 0);
        segdb_close(&g_rw_db);
        /* Sub-case 1: pointers + manifest.1 deleted, segment
         * object left behind. */
        assert(inner->vt->remove(inner->state, "CURRENT") == 0);
        assert(inner->vt->remove(inner->state, "CURRENT.bak") == 0);
        assert(inner->vt->remove(inner->state, "manifest.1") == 0);
        assert(segdb_init(&g_rw_db2, inner, g_rw_arena2, sizeof(g_rw_arena2),
                          g_rw_wal2, sizeof(g_rw_wal2)) == 0);
        assert(segdb_recover(&g_rw_db2) == -1);
        segdb_close(&g_rw_db2);
        yowza_store_close(inner);
    }
    {
        YowzaStore* inner = yowza_store_mem_create();
        assert(inner != NULL);
        assert(segdb_init(&g_rw_db, inner, g_rw_arena, sizeof(g_rw_arena),
                          g_rw_wal, sizeof(g_rw_wal)) == 0);
        rw_put(&g_rw_db, "A", "a");
        assert(segdb_flush(&g_rw_db) == 0);
        segdb_close(&g_rw_db);
        /* Sub-case 2: only the pointers deleted; manifest.1
         * and the segment both survive. Still a failure:
         * manifest.1 may be a stale generation, and the
         * helpers offer no pointerless single-manifest
         * salvage. */
        assert(inner->vt->remove(inner->state, "CURRENT") == 0);
        assert(inner->vt->remove(inner->state, "CURRENT.bak") == 0);
        assert(segdb_init(&g_rw_db2, inner, g_rw_arena2, sizeof(g_rw_arena2),
                          g_rw_wal2, sizeof(g_rw_wal2)) == 0);
        assert(segdb_recover(&g_rw_db2) == -1);
        segdb_close(&g_rw_db2);
        yowza_store_close(inner);
    }
    printf("Passed.\n");
}

/* (c) Previous-generation fallback through segdb_recover:
 * corrupt the current (gen 2) manifest; recovery adopts
 * gen 1 (A only). Corrupt gen 1 as well; recovery fails. */
void test_recovery_wire_generation_fallback() {
    printf("Running test_recovery_wire_generation_fallback...\n");
    {
        YowzaStore* inner = yowza_store_mem_create();
        char cur[128];
        assert(inner != NULL);
        assert(segdb_init(&g_rw_db, inner, g_rw_arena, sizeof(g_rw_arena),
                          g_rw_wal, sizeof(g_rw_wal)) == 0);
        rw_put(&g_rw_db, "A", "a");
        assert(segdb_flush(&g_rw_db) == 0); /* gen 1 */
        rw_put(&g_rw_db, "B", "b");
        assert(segdb_flush(&g_rw_db) == 0); /* gen 2 */
        assert(manifest_read_current(inner, "CURRENT", cur, sizeof(cur)) == 0);
        segdb_close(&g_rw_db);
        rw_flip_byte(inner, cur, 20);
        assert(segdb_init(&g_rw_db2, inner, g_rw_arena2, sizeof(g_rw_arena2),
                          g_rw_wal2, sizeof(g_rw_wal2)) == 0);
        assert(segdb_recover(&g_rw_db2) == 0);
        assert(g_rw_db2.manifest.generation == 1);
        assert(rw_has(&g_rw_db2, "A", "a"));
        assert(rw_missing(&g_rw_db2, "B"));
        segdb_close(&g_rw_db2);
        rw_flip_byte(inner, "manifest.1", 20);
        assert(segdb_init(&g_rw_db2, inner, g_rw_arena2, sizeof(g_rw_arena2),
                          g_rw_wal2, sizeof(g_rw_wal2)) == 0);
        assert(segdb_recover(&g_rw_db2) == -1);
        segdb_close(&g_rw_db2);
        yowza_store_close(inner);
    }
    printf("Passed.\n");
}

/* (d) Genuinely virgin store still recovers empty. */
void test_recovery_wire_virgin_ok() {
    printf("Running test_recovery_wire_virgin_ok...\n");
    {
        YowzaStore* inner = yowza_store_mem_create();
        assert(inner != NULL);
        assert(segdb_init(&g_rw_db, inner, g_rw_arena, sizeof(g_rw_arena),
                          g_rw_wal, sizeof(g_rw_wal)) == 0);
        assert(segdb_recover(&g_rw_db) == 0);
        assert(g_rw_db.manifest.segment_count == 0);
        assert(g_rw_db.has_manifest == 0);
        assert(rw_missing(&g_rw_db, "A"));
        segdb_close(&g_rw_db);
        yowza_store_close(inner);
    }
    printf("Passed.\n");
}

/* (e) I/O-error / unknown backends never become NOT_FOUND
 * in recovery. Sub-case 1: a wrapper whose size_status
 * reports IO_ERROR for CURRENT (enumerate NULL, like the
 * wasm heap store) over a real flushed DB. Sub-case 2: a
 * backend with no size_status hook and a failing legacy
 * size() — even with no objects at all, it cannot prove
 * emptiness, so recovery must fail, never open virgin. */
typedef struct {
    YowzaStore* inner;
    int ioerr_current;
} RwWrapState;

static int rw_wrap_size_status(void* vs, const char* obj, uint64_t* out) {
    RwWrapState* s = (RwWrapState*)vs;
    if (s->ioerr_current && strcmp(obj, "CURRENT") == 0) return YOWZA_STORE_IO_ERROR;
    if (s->inner->vt->size_status)
        return s->inner->vt->size_status(s->inner->state, obj, out);
    return YOWZA_STORE_UNKNOWN;
}
static int rw_wrap_write_at(void* vs, const char* obj, uint64_t off,
                            const uint8_t* data, uint64_t len) {
    RwWrapState* s = (RwWrapState*)vs;
    return s->inner->vt->write_at(s->inner->state, obj, off, data, len);
}
static int rw_wrap_read_at(void* vs, const char* obj, uint64_t off,
                           uint8_t* out, uint64_t len) {
    RwWrapState* s = (RwWrapState*)vs;
    return s->inner->vt->read_at(s->inner->state, obj, off, out, len);
}
static int rw_wrap_size(void* vs, const char* obj, uint64_t* out) {
    RwWrapState* s = (RwWrapState*)vs;
    return s->inner->vt->size(s->inner->state, obj, out);
}
static int rw_wrap_sync_obj(void* vs, const char* obj) {
    RwWrapState* s = (RwWrapState*)vs;
    return s->inner->vt->sync_obj(s->inner->state, obj);
}
static int rw_wrap_sync_dir(void* vs) {
    RwWrapState* s = (RwWrapState*)vs;
    return s->inner->vt->sync_dir(s->inner->state);
}
static int rw_wrap_publish(void* vs, const char* tmp_obj, const char* final_obj) {
    RwWrapState* s = (RwWrapState*)vs;
    return s->inner->vt->publish(s->inner->state, tmp_obj, final_obj);
}
static int rw_wrap_remove(void* vs, const char* obj) {
    RwWrapState* s = (RwWrapState*)vs;
    return s->inner->vt->remove(s->inner->state, obj);
}
static int rw_wrap_truncate(void* vs, const char* obj, uint64_t len) {
    RwWrapState* s = (RwWrapState*)vs;
    return s->inner->vt->truncate(s->inner->state, obj, len);
}
static void rw_wrap_close(void* vs) { (void)vs; }

static const YowzaStoreVTable rw_wrap_vt = {
    rw_wrap_write_at, rw_wrap_read_at, rw_wrap_size, rw_wrap_sync_obj,
    rw_wrap_sync_dir, rw_wrap_publish, rw_wrap_remove, rw_wrap_truncate,
    rw_wrap_close, rw_wrap_size_status,
    /* enumerate: NULL = cannot prove emptiness. */
    NULL
};

static int rw_fail_size(void* st, const char* obj, uint64_t* out) {
    (void)st; (void)obj; (void)out;
    return -1;
}
static int rw_fail_write_at(void* st, const char* obj, uint64_t off,
                            const uint8_t* data, uint64_t len) {
    (void)st; (void)obj; (void)off; (void)data; (void)len;
    return -1;
}
static int rw_fail_read_at(void* st, const char* obj, uint64_t off,
                           uint8_t* out, uint64_t len) {
    (void)st; (void)obj; (void)off; (void)out; (void)len;
    return -1;
}
static void rw_fail_close(void* st) { (void)st; }

void test_recovery_wire_io_error_never_not_found() {
    printf("Running test_recovery_wire_io_error_never_not_found...\n");
    {
        YowzaStore* inner = yowza_store_mem_create();
        RwWrapState ws;
        YowzaStore wrap;
        assert(inner != NULL);
        assert(segdb_init(&g_rw_db, inner, g_rw_arena, sizeof(g_rw_arena),
                          g_rw_wal, sizeof(g_rw_wal)) == 0);
        rw_put(&g_rw_db, "A", "a");
        assert(segdb_flush(&g_rw_db) == 0);
        segdb_close(&g_rw_db);
        /* Sub-case 1: CURRENT metadata reads fail with
         * IO_ERROR through the wrapper. */
        ws.inner = inner; ws.ioerr_current = 1;
        wrap.vt = &rw_wrap_vt; wrap.state = &ws;
        assert(segdb_init(&g_rw_db2, &wrap, g_rw_arena2, sizeof(g_rw_arena2),
                          g_rw_wal2, sizeof(g_rw_wal2)) == 0);
        assert(segdb_recover(&g_rw_db2) == -1);
        segdb_close(&g_rw_db2);
        yowza_store_close(inner);
    }
    {
        /* Sub-case 2: no size_status hook, failing legacy
         * size(), no enumerate hook: UNKNOWN throughout,
         * so recovery fails rather than opening virgin. */
        static const YowzaStoreVTable fail_vt = {
            rw_fail_write_at, rw_fail_read_at, rw_fail_size,
            NULL, NULL, NULL, NULL, NULL, rw_fail_close, NULL, NULL
        };
        YowzaStore* st = yowza_store_attach(&fail_vt, NULL);
        assert(st != NULL);
        assert(segdb_init(&g_rw_db, st, g_rw_arena, sizeof(g_rw_arena),
                          g_rw_wal, sizeof(g_rw_wal)) == 0);
        assert(segdb_recover(&g_rw_db) == -1);
        segdb_close(&g_rw_db);
        yowza_store_close(st);
    }
    printf("Passed.\n");
}

/* Preserved directory-mode behaviour, pinned at O2:
 * (1) a resolved pointer naming manifest.dir recovers
 * groups; (2) when both pointers are gone but the
 * directory object survives, the pointerless salvage in
 * segdb_recover still recovers it; (3) when the directory
 * object is gone too (with group manifest + segment
 * surviving), recovery fails — the directory-mode
 * triple loss. The directory here is hand-built with one
 * group (the gen-1 manifest), which is exactly what the
 * single->directory transition produces for its first
 * group; segdb_recover_directory verifies the group
 * manifest and its segment. */
void test_recovery_wire_directory_modes() {
    printf("Running test_recovery_wire_directory_modes...\n");
    {
        YowzaStore* inner = yowza_store_mem_create();
        ManifestDir dir;
        assert(inner != NULL);
        assert(segdb_init(&g_rw_db, inner, g_rw_arena, sizeof(g_rw_arena),
                          g_rw_wal, sizeof(g_rw_wal)) == 0);
        rw_put(&g_rw_db, "A", "a");
        assert(segdb_flush(&g_rw_db) == 0);
        /* Hand-build manifest.dir naming manifest.1. */
        memset(&dir, 0, sizeof(dir));
        dir.generation = 1;
        dir.checkpoint_seq = g_rw_db.manifest.checkpoint_seq;
        dir.schema_version = 0;
        dir.group_count = 1;
        strcpy(dir.groups[0], "manifest.1");
        assert(manifest_dir_write(inner, "manifest.dir.tmp", &dir) == 0);
        assert(manifest_publish(inner, "manifest.dir.tmp", "manifest.dir") == 0);
        segdb_close(&g_rw_db);
        /* (1) Resolved pointer -> manifest.dir recovers. */
        assert(manifest_write_current(inner, "CURRENT.tmp", "manifest.dir") == 0);
        assert(manifest_publish(inner, "CURRENT.tmp", "CURRENT") == 0);
        assert(segdb_init(&g_rw_db2, inner, g_rw_arena2, sizeof(g_rw_arena2),
                          g_rw_wal2, sizeof(g_rw_wal2)) == 0);
        assert(segdb_recover(&g_rw_db2) == 0);
        assert(g_rw_db2.has_dir == 1);
        assert(g_rw_db2.group_count == 1);
        assert(rw_has(&g_rw_db2, "A", "a"));
        segdb_close(&g_rw_db2);
        /* (2) Both pointers gone, directory survives:
         * pointerless salvage recovers the same state. */
        assert(inner->vt->remove(inner->state, "CURRENT") == 0);
        assert(inner->vt->remove(inner->state, "CURRENT.bak") == 0);
        assert(segdb_init(&g_rw_db2, inner, g_rw_arena2, sizeof(g_rw_arena2),
                          g_rw_wal2, sizeof(g_rw_wal2)) == 0);
        assert(segdb_recover(&g_rw_db2) == 0);
        assert(g_rw_db2.has_dir == 1);
        assert(rw_has(&g_rw_db2, "A", "a"));
        segdb_close(&g_rw_db2);
        /* (3) Directory object gone too, group manifest +
         * segment survive: triple loss fails, never virgin
         * and never a silently dropped group set. */
        assert(inner->vt->remove(inner->state, "manifest.dir") == 0);
        assert(segdb_init(&g_rw_db2, inner, g_rw_arena2, sizeof(g_rw_arena2),
                          g_rw_wal2, sizeof(g_rw_wal2)) == 0);
        assert(segdb_recover(&g_rw_db2) == -1);
        segdb_close(&g_rw_db2);
        yowza_store_close(inner);
    }
    printf("Passed.\n");
}
