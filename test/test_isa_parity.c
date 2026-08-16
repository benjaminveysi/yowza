/* Gate 4 native parity tests: the C ISA engine under the
 * resource policy, driven directly (no wasm host).
 *
 * OWNERSHIP NOTE (same pattern as test_scan_cursor.c):
 * test/main.c is owned by another agent, so this file
 * self-registers through a constructor: the tests below run
 * when the test_runner binary starts, before main's suite.
 * If main.c later gains extern declarations + calls for
 * these functions, delete the constructor at the bottom to
 * avoid running them twice. test_gate4_isa_parity() is
 * idempotent (a static done flag), so a later direct call
 * after the constructor is a no-op, not a double run.
 */
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <stdint.h>
#include "segdb.h"
#include "query_isa.h"

extern int yowza_queryisa_test_checked_sum(int64_t a, int64_t b, int64_t* out);

static SegDB g4_db;
static uint8_t g4_arena[1048576];
static uint8_t g4_wal[262144];
static uint8_t g4_work[33554432]; /* 32 MiB, the engine's work buffer */
static uint8_t g4_out[1048576];

static void g4_put(SegDB* db, const char* k, const char* v) {
    int rc = segdb_put(db, (const uint8_t*)k, (uint32_t)strlen(k),
                       (const uint8_t*)v, (uint32_t)strlen(v));
    assert(rc == 0);
}

/* Load 130 long-name rows (keys user:000..user:129) plus
 * 3 short rows (user:200..user:202), flush, snapshot. */
static uint64_t g4_build(SegDB* db) {
    int i;
    char key[32], val[512];
    for (i = 0; i < 130; i++) {
        int n = snprintf(key, sizeof(key), "user:%03d", i);
        char name[128];
        assert(n > 0);
        memset(name, 'N', 96);
        n = snprintf(name + 96, sizeof(name) - 96, "%03d", i);
        assert(n > 0);
        n = snprintf(val, sizeof(val),
                     "{\"name\":\"%s\",\"age\":%d,\"city\":\"C\",\"active\":true,\"score\":1.0}",
                     name, i % 100);
        assert(n > 0);
        g4_put(db, key, val);
        if ((i + 1) % 50 == 0) assert(segdb_flush(db) == 0);
    }
    g4_put(db, "user:200", "{\"name\":\"Alice\",\"age\":30,\"city\":\"C\",\"active\":true,\"score\":1.0}");
    g4_put(db, "user:201", "{\"name\":\"Bob\",\"age\":25,\"city\":\"C\",\"active\":true,\"score\":1.0}");
    g4_put(db, "user:202", "{\"name\":\"Carol\",\"age\":35,\"city\":\"C\",\"active\":true,\"score\":1.0}");
    assert(segdb_flush(db) == 0);
    return segdb_snapshot_seq(db);
}

/* ---- tiny program builder (layout: see query_isa.h) ---- */
static uint8_t g4_prog[8192];
static uint32_t g4_pos;

static void g4_wu8(uint8_t v) { assert(g4_pos + 1 <= sizeof(g4_prog)); g4_prog[g4_pos++] = v; }
static void g4_wu32(uint32_t v) {
    int i;
    assert(g4_pos + 4 <= sizeof(g4_prog));
    for (i = 0; i < 4; i++) g4_prog[g4_pos++] = (uint8_t)(v >> (8 * i));
}
static void g4_wu64(uint64_t v) {
    int i;
    assert(g4_pos + 8 <= sizeof(g4_prog));
    for (i = 0; i < 8; i++) g4_prog[g4_pos++] = (uint8_t)(v >> (8 * i));
}
static void g4_wstr(const char* s) {
    uint32_t n = (uint32_t)strlen(s), i;
    g4_wu32(n);
    assert(g4_pos + n <= sizeof(g4_prog));
    for (i = 0; i < n; i++) g4_prog[g4_pos++] = (uint8_t)s[i];
}
static void g4_wcol(const char* name, uint8_t type, const char* src) {
    g4_wstr(name);
    g4_wu8(type);
    g4_wstr(src);
}
/* Header + the one users table (key, name, age). */
static void g4_header(uint64_t max_memory) {
    g4_pos = 0;
    g4_prog[g4_pos++] = 'Y';
    g4_prog[g4_pos++] = 'W';
    g4_prog[g4_pos++] = 'Q';
    g4_prog[g4_pos++] = 'I';
    g4_wu32(2); /* version */
    g4_wu64(0); /* max_scanned */
    g4_wu64(0); /* max_result */
    g4_wu64(max_memory);
    g4_wu32(1); /* one table */
    g4_wstr("users");
    g4_wstr("user:");
    g4_wu32(3);
    g4_wcol("key", QISA_KIND_STRING, "key");
    g4_wcol("name", QISA_KIND_STRING, "json:name");
    g4_wcol("age", QISA_KIND_INT, "json:age");
}
static void g4_scan(void) { g4_wu8(0x01); g4_wu32(0); }
static void g4_project2(uint32_t a, uint32_t b, const char* na, const char* nb) {
    g4_wu8(0x05);
    g4_wu32(2);
    g4_wu32(a);
    g4_wu32(b);
    g4_wstr(na);
    g4_wstr(nb);
}
static void g4_project1(uint32_t a, const char* na) {
    g4_wu8(0x05);
    g4_wu32(1);
    g4_wu32(a);
    g4_wstr(na);
}
static void g4_sort1(uint32_t pos, uint8_t desc) {
    g4_wu8(0x06);
    g4_wu32(1);
    g4_wu32(pos);
    g4_wu8(desc);
}
static void g4_end(void) { g4_wu8(0xFF); }

/* ---- result decoding ---- */
static uint32_t g4_ru32(const uint8_t* buf, uint32_t* pos) {
    uint32_t v = 0;
    int i;
    for (i = 0; i < 4; i++) v |= (uint32_t)buf[(*pos)++] << (8 * i);
    return v;
}
/* Decode one string value into dst (NUL-terminated). The
 * caller has already consumed the kind byte. */
static void g4_rstr(const uint8_t* buf, uint32_t* pos, char* dst, uint32_t cap) {
    uint32_t n = g4_ru32(buf, pos), i;
    assert(n + 1 <= cap);
    for (i = 0; i < n; i++) dst[i] = (char)buf[(*pos)++];
    dst[n] = '\0';
}

static int g4_run(SegDB* db, uint64_t snap, const QisaPolicy* pol,
                  uint32_t* out_len, uint64_t* scanned, uint64_t* mem_peak,
                  uint32_t* spilled) {
    return yowza_queryisa_run_policy(db, g4_prog, g4_pos, g4_out,
                                     (uint32_t)sizeof(g4_out), out_len,
                                     scanned, mem_peak, spilled, snap,
                                     g4_work, (uint64_t)sizeof(g4_work), pol);
}

void test_gate4_isa_parity(void) {
    static int done = 0;
    YowzaStore* st;
    uint64_t snap, scanned = 0, mem_peak = 0;
    uint32_t out_len = 0, spilled = 0, pos, count, i;
    QisaPolicy pol;
    int rc;
    int64_t sum_out = 0;
    if (done) return;
    done = 1;
    printf("Running test_gate4_isa_parity...\n");
    fflush(stdout);

    /* Checked SUM (the -8 path): no ISA program can feed
     * exact near-int64-max values (JSON ingestion cannot
     * represent them), so the test hook drives it. */
    assert(yowza_queryisa_test_checked_sum(9223372036854775807LL, 1, &sum_out) == QISA_ERR_OVERFLOW);
    assert(yowza_queryisa_test_checked_sum(-9223372036854775807LL - 1, -1, &sum_out) == QISA_ERR_OVERFLOW);
    assert(yowza_queryisa_test_checked_sum(100, 23, &sum_out) == QISA_OK && sum_out == 123);

    st = yowza_store_mem_create();
    assert(st != NULL);
    assert(segdb_init(&g4_db, st, g4_arena, sizeof(g4_arena),
                      g4_wal, sizeof(g4_wal)) == 0);
    snap = g4_build(&g4_db);

    /* Default policy: SCAN + PROJECT key,name returns all
     * 133 rows, in key order, with correct values. */
    g4_header(0);
    g4_scan();
    g4_project2(0, 1, "key", "name");
    g4_end();
    rc = g4_run(&g4_db, snap, NULL, &out_len, &scanned, &mem_peak, &spilled);
    assert(rc == 0);
    assert(scanned == 133);
    pos = 0;
    count = g4_ru32(g4_out, &pos);
    assert(count == 133);
    {
        char key[64], name[256];
        int found_alice = 0, found_bob = 0, found_carol = 0;
        for (i = 0; i < count; i++) {
            assert(g4_out[pos++] == QISA_KIND_STRING);
            g4_rstr(g4_out, &pos, key, sizeof(key));
            assert(g4_out[pos++] == QISA_KIND_STRING);
            g4_rstr(g4_out, &pos, name, sizeof(name));
            if (strcmp(key, "user:200") == 0) {
                assert(strcmp(name, "Alice") == 0);
                found_alice = 1;
            }
            if (strcmp(key, "user:201") == 0) {
                assert(strcmp(name, "Bob") == 0);
                found_bob = 1;
            }
            if (strcmp(key, "user:202") == 0) {
                assert(strcmp(name, "Carol") == 0);
                found_carol = 1;
            }
        }
        assert(pos == out_len);
        assert(found_alice && found_bob && found_carol);
    }

    /* Policy width below the table width: QISA_ERR_LIMIT
     * at parse time, with no rows. */
    memset(&pol, 0, sizeof(pol));
    pol.max_width = 2;
    g4_header(0);
    g4_scan();
    g4_project2(0, 1, "key", "name");
    g4_end();
    rc = g4_run(&g4_db, snap, &pol, &out_len, &scanned, &mem_peak, &spilled);
    assert(rc == QISA_ERR_LIMIT);

    /* Policy sort cap below the row count: SORT over 133
     * rows with max_sort_rows=2 is QISA_ERR_LIMIT. */
    memset(&pol, 0, sizeof(pol));
    pol.max_sort_rows = 2;
    g4_header(0);
    g4_scan();
    g4_project2(1, 2, "name", "age");
    g4_sort1(1, 0);
    g4_end();
    rc = g4_run(&g4_db, snap, &pol, &out_len, &scanned, &mem_peak, &spilled);
    assert(rc == QISA_ERR_LIMIT);

    /* Spill under a small memory budget: sort 133 rows of
     * ~100-byte names by name with max_memory=10000. One
     * 64-row page (~8700 B) fits the budget but two pages
     * do not, so the engine must spill and still return
     * all rows in sorted order, with spilled set. */
    g4_header(10000);
    g4_scan();
    g4_project1(1, "name");
    g4_sort1(0, 0);
    g4_end();
    spilled = 0;
    mem_peak = 0;
    rc = g4_run(&g4_db, snap, NULL, &out_len, &scanned, &mem_peak, &spilled);
    assert(rc == 0);
    assert(spilled == 1);
    assert(mem_peak > 0);
    pos = 0;
    count = g4_ru32(g4_out, &pos);
    assert(count == 133);
    {
        char prev[256] = "", name[256];
        for (i = 0; i < count; i++) {
            assert(g4_out[pos++] == QISA_KIND_STRING);
            g4_rstr(g4_out, &pos, name, sizeof(name));
            assert(strcmp(prev, name) <= 0);
            strcpy(prev, name);
        }
        assert(pos == out_len);
    }

    segdb_close(&g4_db);
    yowza_store_close(st);
    printf("Passed.\n");
    fflush(stdout);
}

/* Self-registration: see the ownership note at the top. */
__attribute__((constructor)) static void test_gate4_isa_parity_register(void) {
    test_gate4_isa_parity();
}
