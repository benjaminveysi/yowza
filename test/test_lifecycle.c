/* Gate 2 lifecycle tests (C side, 2.4b): storage enumeration,
 * prior-content detection, and manifest pointer/manifest
 * recovery. Registered in test/main.c by the parent at
 * integration; this file defines the test functions only
 * (no main). Compiled into test_runner via the Makefile
 * wildcard, and runnable standalone via a harness that
 * calls the test_lifecycle_* functions.
 *
 * Conventions follow the existing C tests: assert() for
 * hard failures, printf() banners. Manifest structs are
 * ~285 KiB: file-scope statics, never stack.
 */

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifndef __wasm__
#include <unistd.h>
#endif

#include "manifest.h"
#include "storage2.h"

/* ---- enumeration collection callback ---- */

typedef struct {
    char names[16][MANIFEST_MAX_OBJ_LEN + 1];
    int count;
    int stopped_early;
} LifeEnumCtx;

static int life_collect_cb(void* ctx, const char* name) {
    LifeEnumCtx* c = (LifeEnumCtx*)ctx;
    size_t n;
    if (!c || !name) return 0;
    if (c->count >= 16) return 1; /* stop early on overflow */
    n = strlen(name);
    if (n > MANIFEST_MAX_OBJ_LEN) return 1;
    memcpy(c->names[c->count], name, n + 1);
    c->count++;
    return 0;
}

static int life_stop_at_first_cb(void* ctx, const char* name) {
    (void)name;
    if (ctx) *(int*)ctx = 1;
    return 1; /* stop early: 1, not an error */
}

static int life_has_name(LifeEnumCtx* c, const char* want) {
    int i;
    for (i = 0; i < c->count; i++) {
        if (strcmp(c->names[i], want) == 0) return 1;
    }
    return 0;
}

/* ---- manifest helpers (file-scope statics: ~285 KiB each) ---- */

static Manifest life_man, life_man_out;

static void life_fill_manifest(Manifest* m, uint64_t gen, uint64_t seq) {
    memset(m, 0, sizeof(*m));
    m->generation = gen;
    m->checkpoint_seq = seq;
    m->schema_version = 1;
    m->segment_count = 1;
    memset(&m->segments[0], 0, sizeof(m->segments[0]));
    strncpy(m->segments[0].obj, "seg.1", MANIFEST_MAX_OBJ_LEN);
    m->segments[0].first_key_len = 1;
    m->segments[0].first_key[0] = 'a';
    m->segments[0].last_key_len = 1;
    m->segments[0].last_key[0] = 'b';
    m->segments[0].min_seq = 1;
    m->segments[0].max_seq = 2;
    m->segments[0].entry_count = 10;
    m->segments[0].index_type = 0;
    m->segments[0].model_version = 0;
    m->segments[0].byte_size = 100;
    m->segments[0].checksum = 42;
}

/* Publish a manifest generation + point CURRENT at it, on a
 * mem store. Returns 0 on success. */
static int life_publish_gen(YowzaStore* st, uint64_t gen, uint64_t seq) {
    char tmp[64], final[64];
    snprintf(tmp, sizeof(tmp), "manifest.%llu.tmp",
             (unsigned long long)gen);
    snprintf(final, sizeof(final), "manifest.%llu",
             (unsigned long long)gen);
    life_fill_manifest(&life_man, gen, seq);
    if (manifest_write(st, tmp, &life_man) != 0) return -1;
    if (manifest_publish(st, tmp, final) != 0) return -1;
    if (manifest_write_current(st, "CURRENT.tmp", final) != 0) return -1;
    if (manifest_publish(st, "CURRENT.tmp", "CURRENT") != 0) return -1;
    return 0;
}

void test_lifecycle_enumerate_mem() {
    printf("Running test_lifecycle_enumerate_mem...\n");
    {
        YowzaStore* st = yowza_store_mem_create();
        LifeEnumCtx ctx;
        uint8_t data[4] = {1, 2, 3, 4};
        int stopped = 0;
        assert(st);
        assert(yowza_store_write_all(st, "alpha", data, 4) == 0);
        assert(yowza_store_write_all(st, "beta", data, 2) == 0);
        assert(yowza_store_write_all(st, "gamma", data, 0) == 0);
        memset(&ctx, 0, sizeof(ctx));
        assert(yowza_store_enumerate(st, life_collect_cb, &ctx) == 0);
        assert(ctx.count == 3);
        assert(life_has_name(&ctx, "alpha"));
        assert(life_has_name(&ctx, "beta"));
        assert(life_has_name(&ctx, "gamma"));
        /* Early stop reports 1, never -1. */
        assert(yowza_store_enumerate(st, life_stop_at_first_cb, &stopped) == 1);
        assert(stopped == 1);
        yowza_store_close(st);
    }
    printf("Passed.\n");
}

void test_lifecycle_enumerate_posix() {
    printf("Running test_lifecycle_enumerate_posix...\n");
#ifndef __wasm__
    {
        char dir_template[] = "/tmp/yowza_lifecycle_XXXXXX";
        char* dir = mkdtemp(dir_template);
        YowzaStore* st;
        LifeEnumCtx ctx;
        uint8_t data[4] = {5, 6, 7, 8};
        assert(dir != NULL);
        st = yowza_store_posix_create(dir);
        assert(st);
        assert(yowza_store_write_all(st, "obj1", data, 4) == 0);
        assert(yowza_store_write_all(st, "obj2", data, 4) == 0);
        memset(&ctx, 0, sizeof(ctx));
        assert(yowza_store_enumerate(st, life_collect_cb, &ctx) == 0);
        assert(ctx.count == 2);
        assert(life_has_name(&ctx, "obj1"));
        assert(life_has_name(&ctx, "obj2"));
        assert(st->vt->remove(st->state, "obj1") == 0);
        assert(st->vt->remove(st->state, "obj2") == 0);
        yowza_store_close(st);
        assert(rmdir(dir) == 0);
    }
#else
    /* WASM builds have no POSIX backend; nothing to test there. */
#endif
    printf("Passed.\n");
}

void test_lifecycle_detect_prior_content() {
    printf("Running test_lifecycle_detect_prior_content...\n");
    {
        YowzaStore* st = yowza_store_mem_create();
        uint8_t data[2] = {9, 9};
        int has = -99;
        assert(st);
        /* Authoritatively empty. */
        assert(yowza_store_detect_prior_content(st, &has) == 0);
        assert(has == 0);
        assert(yowza_store_write_all(st, "seg.1", data, 2) == 0);
        assert(yowza_store_detect_prior_content(st, &has) == 0);
        assert(has == 1);
        /* Removing the only object restores proven-empty. */
        assert(st->vt->remove(st->state, "seg.1") == 0);
        assert(yowza_store_detect_prior_content(st, &has) == 0);
        assert(has == 0);
        yowza_store_close(st);
    }
    printf("Passed.\n");
}

/* ---- backend without enumerate / failing size: the
 * UNKNOWN path must never become NOT_FOUND ---- */

static int life_fail_size(void* st, const char* obj, uint64_t* out_size) {
    (void)st;
    (void)obj;
    (void)out_size;
    return -1; /* every size probe fails: I/O error, not absence */
}

static int life_fail_write_at(void* st, const char* obj, uint64_t off,
                              const uint8_t* data, uint64_t len) {
    (void)st;
    (void)obj;
    (void)off;
    (void)data;
    (void)len;
    return -1;
}

static int life_fail_read_at(void* st, const char* obj, uint64_t off,
                             uint8_t* out, uint64_t len) {
    (void)st;
    (void)obj;
    (void)off;
    (void)out;
    (void)len;
    return -1;
}

static void life_noop_close(void* st) {
    (void)st;
}

void test_lifecycle_unknown_backend_never_not_found() {
    printf("Running test_lifecycle_unknown_backend_never_not_found...\n");
    {
        /* VTable with NO size_status hook and NO enumerate
         * hook, and a failing legacy size(): absence and
         * I/O error are indistinguishable here, so the
         * wrappers must report UNKNOWN / -1, never
         * NOT_FOUND / empty. */
        static const YowzaStoreVTable fail_vt = {
            life_fail_write_at, life_fail_read_at, life_fail_size,
            NULL, NULL, NULL, NULL, NULL, life_noop_close, NULL, NULL
        };
        YowzaStore* st = yowza_store_attach(&fail_vt, NULL);
        uint64_t sz = 0;
        int has = -99;
        int status;
        assert(st);
        status = yowza_store_size_status(st, "CURRENT", &sz);
        assert(status == YOWZA_STORE_UNKNOWN);
        assert(status != YOWZA_STORE_NOT_FOUND);
        /* Detection cannot prove emptiness without
         * enumeration: -1, and has_content untouched. */
        assert(yowza_store_detect_prior_content(st, &has) == -1);
        assert(has == -99);
        /* Manifest pointer recovery on this backend is
         * ambiguous (-2), never virgin (-1). */
        {
            char name[MANIFEST_MAX_OBJ_LEN + 1];
            int fb = 0;
            assert(manifest_recover_pointer(st, name, sizeof(name), &fb) == -2);
        }
        yowza_store_close(st);
    }
    printf("Passed.\n");
}

void test_lifecycle_manifest_pointer_recovery() {
    printf("Running test_lifecycle_manifest_pointer_recovery...\n");
    {
        YowzaStore* st = yowza_store_mem_create();
        char name[MANIFEST_MAX_OBJ_LEN + 1];
        int fb = -1;
        assert(st);
        /* Virgin store: -1, safe to initialise fresh. */
        assert(manifest_recover_pointer(st, name, sizeof(name), &fb) == -1);
        /* Normal CURRENT path: fallback flag 0. */
        assert(life_publish_gen(st, 7, 100) == 0);
        assert(manifest_recover_pointer(st, name, sizeof(name), &fb) == 0);
        assert(fb == 0);
        assert(strcmp(name, "manifest.7") == 0);
        /* Lose CURRENT, keep CURRENT.bak: fallback flag 1. */
        assert(st->vt->remove(st->state, "CURRENT") == 0);
        assert(manifest_write_current(st, "CURRENT.bak.tmp", "manifest.7") == 0);
        assert(manifest_publish(st, "CURRENT.bak.tmp", "CURRENT.bak") == 0);
        fb = -1;
        assert(manifest_recover_pointer(st, name, sizeof(name), &fb) == 0);
        assert(fb == 1);
        assert(strcmp(name, "manifest.7") == 0);
        /* Lose BOTH pointers with objects remaining: -2,
         * never virgin. This is the closed double-loss hole. */
        assert(st->vt->remove(st->state, "CURRENT.bak") == 0);
        assert(manifest_recover_pointer(st, name, sizeof(name), &fb) == -2);
        yowza_store_close(st);
    }
    printf("Passed.\n");
}

void test_lifecycle_manifest_generation_fallback() {
    printf("Running test_lifecycle_manifest_generation_fallback...\n");
    {
        YowzaStore* st = yowza_store_mem_create();
        int fb = -1;
        uint8_t garbage[8] = {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF};
        assert(st);
        /* Generation 4 (valid) and 5 (will be corrupted). */
        assert(life_publish_gen(st, 4, 40) == 0);
        /* Publish gen 5 WITHOUT moving CURRENT yet, then
         * point CURRENT at it, so both generations exist. */
        {
            char tmp[64];
            snprintf(tmp, sizeof(tmp), "manifest.5.tmp");
            life_fill_manifest(&life_man, 5, 50);
            assert(manifest_write(st, tmp, &life_man) == 0);
            assert(manifest_publish(st, tmp, "manifest.5") == 0);
            assert(manifest_write_current(st, "CURRENT.tmp", "manifest.5") == 0);
            assert(manifest_publish(st, "CURRENT.tmp", "CURRENT") == 0);
        }
        /* Corrupt the gen-5 manifest object in place. */
        assert(st->vt->write_at(st->state, "manifest.5", 0, garbage, sizeof(garbage)) == 0);
        memset(&life_man_out, 0, sizeof(life_man_out));
        assert(manifest_recover_manifest(st, &life_man_out, &fb) == 0);
        assert(fb == 1);
        assert(life_man_out.generation == 4);
        assert(life_man_out.checkpoint_seq == 40);
        /* And the clean path still works on a fresh store. */
        yowza_store_close(st);
        st = yowza_store_mem_create();
        assert(st);
        assert(life_publish_gen(st, 3, 30) == 0);
        fb = -1;
        memset(&life_man_out, 0, sizeof(life_man_out));
        assert(manifest_recover_manifest(st, &life_man_out, &fb) == 0);
        assert(fb == 0);
        assert(life_man_out.generation == 3);
        yowza_store_close(st);
    }
    printf("Passed.\n");
}

void test_lifecycle_manifest_current_status() {
    printf("Running test_lifecycle_manifest_current_status...\n");
    {
        YowzaStore* st = yowza_store_mem_create();
        char name[MANIFEST_MAX_OBJ_LEN + 1];
        assert(st);
        /* Authoritatively absent -> 1. */
        assert(manifest_current_status(st, "CURRENT", name, sizeof(name)) == 1);
        /* Present -> 0. */
        assert(life_publish_gen(st, 2, 20) == 0);
        assert(manifest_current_status(st, "CURRENT", name, sizeof(name)) == 0);
        assert(strcmp(name, "manifest.2") == 0);
        /* Present but unreadable (truncated to empty) -> 2,
         * never "missing". */
        assert(st->vt->truncate(st->state, "CURRENT", 0) == 0);
        assert(manifest_current_status(st, "CURRENT", name, sizeof(name)) == 2);
        yowza_store_close(st);
    }
    printf("Passed.\n");
}
