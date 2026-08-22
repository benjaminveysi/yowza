/* Yowza64 — Storage ABI v2 tests (Phase 2, Track A).
 * Plain assert-style, like the existing test files. Functions are
 * named test_storage2_*; the integrator registers them in
 * test/main.c (this file must NOT be edited into main.c by this
 * track).
 *
 * Standalone run (does not need main.c):
 *   clang -O2 -Iinclude -Wall -Wextra test/test_storage2.c \
 *       src/storage2.c -o /tmp/t_storage2 && /tmp/t_storage2
 * That works because main() here is WEAK: linked with test/main.c
 * (which provides a strong main), the strong one wins and these
 * tests are simply uncalled until registered; linked standalone,
 * this weak main runs them. */

#include <stdio.h>
#include <assert.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include "storage2.h"

#ifndef __wasm__
#include <unistd.h>
#include <sys/stat.h>
#endif

/* Shorthand: call through the vtable exactly as other tracks will. */
#define W(s, o, off, d, l) ((s)->vt->write_at((s)->state, (o), (off), (d), (l)))
#define R(s, o, off, b, l) ((s)->vt->read_at((s)->state, (o), (off), (b), (l)))
#define SZ(s, o, out) ((s)->vt->size((s)->state, (o), (out)))

void test_storage2_mem_roundtrip(void) {
    YowzaStore* s = yowza_store_mem_create();
    uint64_t sz = 0;
    uint8_t buf[32];
    assert(s);
    assert(W(s, "a", 0, (const uint8_t*)"hello", 5) == 0);
    assert(SZ(s, "a", &sz) == 0 && sz == 5);
    memset(buf, 0, sizeof buf);
    assert(R(s, "a", 0, buf, 5) == 0 && memcmp(buf, "hello", 5) == 0);
    /* Partial overwrite + gap zeroes. */
    assert(W(s, "a", 10, (const uint8_t*)"XY", 2) == 0);
    assert(SZ(s, "a", &sz) == 0 && sz == 12);
    memset(buf, 0, sizeof buf);
    assert(R(s, "a", 0, buf, 12) == 0);
    assert(memcmp(buf, "hello", 5) == 0);
    assert(buf[5] == 0 && buf[9] == 0); /* unwritten gap */
    assert(buf[10] == 'X' && buf[11] == 'Y');
    /* sync contract for the mem backend: no-ops returning 0. */
    assert(s->vt->sync_obj(s->state, "a") == 0);
    assert(s->vt->sync_dir(s->state) == 0);
    assert(s->vt->sync_obj(s->state, "missing") == -1);
    /* Zero-length write creates an empty object, changes nothing. */
    assert(W(s, "empty", 0, NULL, 0) == 0);
    assert(SZ(s, "empty", &sz) == 0 && sz == 0);
    assert(R(s, "empty", 0, buf, 0) == 0);
    yowza_store_close(s);
    printf("test_storage2_mem_roundtrip passed\n");
}

void test_storage2_sparse_above_4gib(void) {
    YowzaStore* s = yowza_store_mem_create();
    /* 5 GiB offset: 4294967296 + 12345. Only kilobytes are touched. */
    const uint64_t off = (uint64_t)4294967296ULL + 12345ULL;
    uint64_t sz = 0;
    uint8_t buf[16];
    assert(s);
    assert(W(s, "sparse", off, (const uint8_t*)"SPARSE!", 7) == 0);
    assert(SZ(s, "sparse", &sz) == 0);
    assert(sz == off + 7);
    assert(sz > (uint64_t)UINT32_MAX); /* size reports > UINT32_MAX */
    memset(buf, 0, sizeof buf);
    assert(R(s, "sparse", off, buf, 7) == 0 && memcmp(buf, "SPARSE!", 7) == 0);
    /* Untouched ranges read back as zeroes without allocating them. */
    memset(buf, 0xAA, sizeof buf);
    assert(R(s, "sparse", 0, buf, 8) == 0);
    assert(buf[0] == 0 && buf[7] == 0);
    memset(buf, 0xAA, sizeof buf);
    assert(R(s, "sparse", (uint64_t)1073741824ULL, buf, 8) == 0); /* 1 GiB */
    assert(buf[0] == 0 && buf[7] == 0);
    yowza_store_close(s);
    printf("test_storage2_sparse_above_4gib passed\n");
}

void test_storage2_short_read(void) {
    YowzaStore* s = yowza_store_mem_create();
    uint8_t buf[16];
    uint64_t sz = 0;
    assert(s);
    assert(W(s, "a", 0, (const uint8_t*)"12345", 5) == 0);
    /* Read exactly to the end: ok. One byte past: fails. */
    assert(R(s, "a", 0, buf, 5) == 0);
    assert(R(s, "a", 0, buf, 6) == -1);
    assert(R(s, "a", 4, buf, 2) == -1);
    assert(R(s, "a", 5, buf, 1) == -1);
    /* Missing object: size and read both fail, not silent partial. */
    assert(SZ(s, "nope", &sz) == -1);
    assert(R(s, "nope", 0, buf, 1) == -1);
    /* NULL / invalid arguments fail (via the helpers, which check). */
    assert(yowza_store_read_all(NULL, "a", buf, 1) == -1);
    assert(yowza_store_read_all(s, "a", NULL, 1) == -1);
    yowza_store_close(s);
    printf("test_storage2_short_read passed\n");
}

void test_storage2_truncate(void) {
    YowzaStore* s = yowza_store_mem_create();
    uint64_t sz = 0;
    uint8_t buf[16];
    assert(s);
    assert(W(s, "a", 0, (const uint8_t*)"abcdefgh", 8) == 0);
    assert(s->vt->truncate(s->state, "a", 3) == 0);
    assert(SZ(s, "a", &sz) == 0 && sz == 3);
    memset(buf, 0, sizeof buf);
    assert(R(s, "a", 0, buf, 3) == 0 && memcmp(buf, "abc", 3) == 0);
    /* Grow: extension reads back as zeroes. */
    assert(s->vt->truncate(s->state, "a", 6) == 0);
    memset(buf, 0xAA, sizeof buf);
    assert(R(s, "a", 0, buf, 6) == 0);
    assert(memcmp(buf, "abc", 3) == 0 && buf[3] == 0 && buf[5] == 0);
    /* Shrink then re-extend: no stale bytes resurrected. */
    assert(s->vt->truncate(s->state, "a", 2) == 0);
    assert(s->vt->truncate(s->state, "a", 6) == 0);
    memset(buf, 0xAA, sizeof buf);
    assert(R(s, "a", 0, buf, 6) == 0);
    assert(buf[0] == 'a' && buf[1] == 'b');
    assert(buf[2] == 0 && buf[5] == 0);
    assert(s->vt->truncate(s->state, "missing", 1) == -1);
    yowza_store_close(s);
    printf("test_storage2_truncate passed\n");
}

void test_storage2_publish_atomicity(void) {
    YowzaStore* s = yowza_store_mem_create();
    uint64_t sz = 0;
    uint8_t buf[16];
    assert(s);
    assert(W(s, "final", 0, (const uint8_t*)"OLD", 3) == 0);
    assert(W(s, "tmp", 0, (const uint8_t*)"NEW!", 4) == 0);
    /* Old object intact until publish. */
    memset(buf, 0, sizeof buf);
    assert(R(s, "final", 0, buf, 3) == 0 && memcmp(buf, "OLD", 3) == 0);
    /* Publish of a missing temp fails and leaves final intact. */
    assert(s->vt->publish(s->state, "ghost", "final") == -1);
    memset(buf, 0, sizeof buf);
    assert(R(s, "final", 0, buf, 3) == 0 && memcmp(buf, "OLD", 3) == 0);
    /* Publish: new content after, temp gone. */
    assert(s->vt->publish(s->state, "tmp", "final") == 0);
    assert(SZ(s, "tmp", &sz) == -1);
    assert(SZ(s, "final", &sz) == 0 && sz == 4);
    memset(buf, 0, sizeof buf);
    assert(R(s, "final", 0, buf, 4) == 0 && memcmp(buf, "NEW!", 4) == 0);
    /* Publish onto the same name is a no-op success. */
    assert(s->vt->publish(s->state, "final", "final") == 0);
    assert(SZ(s, "final", &sz) == 0 && sz == 4);
    yowza_store_close(s);
    printf("test_storage2_publish_atomicity passed\n");
}

void test_storage2_names_and_remove(void) {
    YowzaStore* s = yowza_store_mem_create();
    uint64_t sz = 0;
    assert(s);
    /* Flat-namespace rejections, identical on every backend. */
    assert(W(s, "", 0, (const uint8_t*)"x", 1) == -1);
    assert(W(s, ".", 0, (const uint8_t*)"x", 1) == -1);
    assert(W(s, "..", 0, (const uint8_t*)"x", 1) == -1);
    assert(W(s, "a/b", 0, (const uint8_t*)"x", 1) == -1);
    assert(W(s, NULL, 0, (const uint8_t*)"x", 1) == -1);
    assert(SZ(s, "a/b", &sz) == -1);
    /* Remove semantics. */
    assert(W(s, "gone", 0, (const uint8_t*)"x", 1) == 0);
    assert(s->vt->remove(s->state, "gone") == 0);
    assert(SZ(s, "gone", &sz) == -1);
    assert(s->vt->remove(s->state, "gone") == -1); /* already gone */
    yowza_store_close(s);
    yowza_store_close(NULL); /* tolerated */
    printf("test_storage2_names_and_remove passed\n");
}

void test_storage2_posix_roundtrip_publish_sync(void) {
#ifndef __wasm__
    char dir_template[] = "/tmp/yowza_storage2_XXXXXX";
    char* dir = mkdtemp(dir_template);
    YowzaStore* s;
    uint64_t sz = 0;
    uint8_t buf[32];
    assert(dir != NULL);
    s = yowza_store_posix_create(dir);
    assert(s);
    assert(W(s, "obj", 0, (const uint8_t*)"posix-data", 10) == 0);
    assert(s->vt->sync_obj(s->state, "obj") == 0);
    assert(s->vt->sync_dir(s->state) == 0);
    assert(SZ(s, "obj", &sz) == 0 && sz == 10);
    memset(buf, 0, sizeof buf);
    assert(R(s, "obj", 0, buf, 10) == 0 && memcmp(buf, "posix-data", 10) == 0);
    /* Short read on the POSIX backend also fails, not partial. */
    assert(R(s, "obj", 0, buf, 11) == -1);
    /* Publish path: temp file + fsync + rename + dir fsync. */
    assert(W(s, "obj.tmp", 0, (const uint8_t*)"v2", 2) == 0);
    memset(buf, 0, sizeof buf);
    assert(R(s, "obj", 0, buf, 10) == 0 && memcmp(buf, "posix-data", 10) == 0);
    assert(s->vt->publish(s->state, "obj.tmp", "obj") == 0);
    assert(SZ(s, "obj.tmp", &sz) == -1);
    assert(SZ(s, "obj", &sz) == 0 && sz == 2);
    memset(buf, 0, sizeof buf);
    assert(R(s, "obj", 0, buf, 2) == 0 && memcmp(buf, "v2", 2) == 0);
    /* Truncate + remove on the POSIX backend. */
    assert(s->vt->truncate(s->state, "obj", 1) == 0);
    assert(SZ(s, "obj", &sz) == 0 && sz == 1);
    assert(s->vt->remove(s->state, "obj") == 0);
    assert(SZ(s, "obj", &sz) == -1);
    yowza_store_close(s);
    /* A store on a nonexistent directory fails to create. */
    assert(yowza_store_posix_create("/tmp/yowza_storage2_does_not_exist_zzz") == NULL);
    assert(rmdir(dir) == 0);
#else
    /* WASM builds have no POSIX backend; nothing to test there. */
#endif
    printf("test_storage2_posix_roundtrip_publish_sync passed\n");
}

void test_storage2_write_all_read_all_1mib(void) {
    YowzaStore* s = yowza_store_mem_create();
    const uint64_t n = (uint64_t)1024 * 1024; /* 1 MiB, in 64 KiB chunks */
    uint8_t* data = (uint8_t*)malloc((size_t)n);
    uint8_t* back = (uint8_t*)malloc((size_t)n);
    uint64_t i, sz = 0;
    assert(s && data && back);
    for (i = 0; i < n; i++) data[i] = (uint8_t)((i * 31 + 7) & 0xFF);
    assert(yowza_store_write_all(s, "big", data, n) == 0);
    assert(SZ(s, "big", &sz) == 0 && sz == n);
    memset(back, 0, (size_t)n);
    assert(yowza_store_read_all(s, "big", back, n) == 0);
    assert(memcmp(data, back, (size_t)n) == 0);
    /* read_all asking for more than the object holds fails. */
    assert(yowza_store_read_all(s, "big", back, n + 1) == -1);
    /* write_all with a shorter payload shrinks the object. */
    assert(yowza_store_write_all(s, "big", data, 100) == 0);
    assert(SZ(s, "big", &sz) == 0 && sz == 100);
    memset(back, 0, (size_t)n);
    assert(yowza_store_read_all(s, "big", back, 100) == 0);
    assert(memcmp(data, back, 100) == 0);
    /* write_all of length 0 leaves an empty object. */
    assert(yowza_store_write_all(s, "big", NULL, 0) == 0);
    assert(SZ(s, "big", &sz) == 0 && sz == 0);
    free(data);
    free(back);
    yowza_store_close(s);
    printf("test_storage2_write_all_read_all_1mib passed\n");
}

/* Weak main: see the header comment. A strong main (test/main.c)
 * wins when both are linked; standalone, this runs the tests. */
__attribute__((weak)) int main(void) {
    printf("Starting Storage2 Tests...\n");
    test_storage2_mem_roundtrip();
    test_storage2_sparse_above_4gib();
    test_storage2_short_read();
    test_storage2_truncate();
    test_storage2_publish_atomicity();
    test_storage2_names_and_remove();
    test_storage2_posix_roundtrip_publish_sync();
    test_storage2_write_all_read_all_1mib();
    printf("All Storage2 Tests Passed!\n");
    return 0;
}
