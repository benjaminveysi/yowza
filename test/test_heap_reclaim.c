/* Gate 3 tests: heap compaction + reclamation (wasm heap store,
 * via yowza_segdb_init) and the fan-in-bounded scan (native
 * memory store, via segdb_scan_fanin).
 *
 * OWNERSHIP NOTE (same pattern as test_scan_cursor.c):
 * test/main.c is owned by another agent, so this file
 * self-registers through a constructor: the tests below run
 * when the test_runner binary starts, before main's suite.
 * If main.c later gains extern declarations + calls for these
 * functions, delete the constructor at the bottom to avoid
 * running them twice.
 */
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include "segdb.h"
#include "segdb_scan.h"

/* New heap exports (plain functions in the native build). */
extern int yowza_segdb_init(SegDB* state, uint8_t* arena_buf, uint64_t arena_size,
                            uint8_t* wal_buf, uint32_t wal_size,
                            uint8_t* heap_buf, uint64_t heap_size);
extern int yowza_segdb_heap_index_rebuild(void);
extern void yowza_segdb_heap_info(uint8_t* out);
extern int yowza_segdb_heap_compact(void);
extern void yowza_segdb_heap_compact_threshold(uint64_t waste_bytes);
extern int yowza_segdb_heap_reclaim(SegDB* state);
extern void yowza_segdb_heap_reclaim_stats(uint8_t* out);

/* Heap layout (mirrors src/yowza_segdb.c; the test owns the heap
 * buffer, so it may parse the table directly). */
#define HC_HEAP_SIZE 4194304u
#define HC_TABLE_SLOTS 4096u
#define HC_SLOT_SIZE 152u
#define HC_HEADER_SIZE 32u
#define HC_DATA_START (HC_HEADER_SIZE + HC_TABLE_SLOTS * HC_SLOT_SIZE)

static SegDB g_hc_db;
static uint8_t g_hc_arena[1048576];
static uint8_t g_hc_wal[262144];
static uint8_t g_hc_heap[HC_HEAP_SIZE];
static uint8_t g_hc_heap_copy[HC_HEAP_SIZE];

/* Cycles-test heap: 16 MiB. A 4 MiB heap cannot hold this test's working
 * set even with perfect compaction: during a compact the old segments,
 * the new segment's data temp, and the new final object coexist (~3x
 * live bytes), and every compact leaves its data/index temps as orphans
 * by design (segment.c) until reclaim runs. 16 MiB holds the peak with
 * margin; reclaim at every cycle boundary is what keeps the heap bounded
 * across 200 cycles. */
#define CYC_HEAP_SIZE 16777216u
static SegDB g_cyc_db;
static uint8_t g_cyc_arena[1048576];
static uint8_t g_cyc_wal[262144];
static uint8_t g_cyc_heap[CYC_HEAP_SIZE];

static uint64_t hc_rd64(const uint8_t* p) {
    uint64_t v = 0;
    int i;
    for (i = 0; i < 8; i++) v |= (uint64_t)p[i] << (i * 8);
    return v;
}

static void hc_wr64(uint8_t* p, uint64_t v) {
    int i;
    for (i = 0; i < 8; i++) p[i] = (uint8_t)((v >> (i * 8)) & 0xFF);
}

static void hc_reclaim_stats(uint64_t* deleted, uint64_t* bytes) {
    uint8_t buf[16];
    yowza_segdb_heap_reclaim_stats(buf);
    *deleted = hc_rd64(buf + 0);
    *bytes = hc_rd64(buf + 8);
}

static void hc_info(uint64_t* heap_size, uint64_t* bump, uint64_t* live,
                    uint64_t* slots, uint64_t* waste) {
    uint8_t buf[48];
    yowza_segdb_heap_info(buf);
    *heap_size = hc_rd64(buf + 0);
    *bump = hc_rd64(buf + 8);
    *live = hc_rd64(buf + 16);
    *slots = hc_rd64(buf + 24);
    *waste = hc_rd64(buf + 32);
}

static void hc_init(void) {
    memset(g_hc_heap, 0, sizeof(g_hc_heap));
    assert(yowza_segdb_init(&g_hc_db, g_hc_arena, sizeof(g_hc_arena),
                            g_hc_wal, sizeof(g_hc_wal),
                            g_hc_heap, sizeof(g_hc_heap)) == 0);
}

static void hc_put(SegDB* db, const char* k, const char* v) {
    assert(segdb_put(db, (const uint8_t*)k, (uint32_t)strlen(k),
                     (const uint8_t*)v, (uint32_t)strlen(v)) == 0);
}

static void hc_del(SegDB* db, const char* k) {
    assert(segdb_delete(db, (const uint8_t*)k, (uint32_t)strlen(k)) == 0);
}

static int hc_get_eq(SegDB* db, const char* k, const char* want) {
    uint8_t* out = NULL;
    uint32_t len = 0;
    if (segdb_get(db, (const uint8_t*)k, (uint32_t)strlen(k), &out, &len) != 0) return 0;
    if (len != strlen(want)) return 0;
    return len == 0 || memcmp(out, want, len) == 0;
}

static int hc_missing(SegDB* db, const char* k) {
    uint8_t* out = NULL;
    uint32_t len = 0;
    return segdb_get(db, (const uint8_t*)k, (uint32_t)strlen(k), &out, &len) != 0;
}

/* Find a heap slot by object name; -1 when absent. */
static int hc_slot_find(const char* name) {
    uint32_t i;
    for (i = 0; i < HC_TABLE_SLOTS; i++) {
        uint8_t* s = g_hc_heap + HC_HEADER_SIZE + (uint64_t)i * HC_SLOT_SIZE;
        if (s[0] != 0 && strcmp((const char*)s, name) == 0) return (int)i;
    }
    return -1;
}

static uint8_t* hc_slot(int idx) {
    return g_hc_heap + HC_HEADER_SIZE + (uint64_t)idx * HC_SLOT_SIZE;
}

/* ---------- Test 1: 200 create -> overwrite -> flush -> compact ->
 * delete -> flush cycles, byte-for-byte final verification. This is
 * deliberately far more demanding than the 300-segment collision
 * test: 400 flushes, 200 segment compacts, 24,000 live-tracked
 * keys, with heap space reclaimed by the threshold + OOM auto
 * paths during each cycle and by heap reclaim at every cycle
 * boundary (the documented maintenance point: reclaim deletes the
 * compact temps and superseded segments/manifests that compaction
 * alone can never remove, since they are still live objects).
 * The heap is 16 MiB: the compact working set (old segments +
 * data temp + final object, ~3x live bytes) does not fit a 4 MiB
 * heap at 24,000 keys however perfectly compaction packs. ---------- */
static void test_heap_compact_cycles(void) {
    static uint8_t live[24000];
    uint32_t c, i;
    uint64_t hs, bump, lv, slots, waste, max_waste = 0;
    uint64_t tot_del = 0, tot_bytes = 0, d, b;
    printf("Running test_heap_compact_cycles...\n");
    memset(g_cyc_heap, 0, sizeof(g_cyc_heap));
    assert(yowza_segdb_init(&g_cyc_db, g_cyc_arena, sizeof(g_cyc_arena),
                            g_cyc_wal, sizeof(g_cyc_wal),
                            g_cyc_heap, sizeof(g_cyc_heap)) == 0);
    yowza_segdb_heap_compact_threshold(4096);
    memset(live, 0, sizeof(live));
    for (c = 0; c < 200; c++) {
        char k[32], v[40];
        /* New keys for this cycle. */
        for (i = 0; i < 120; i++) {
            uint32_t g = c * 120 + i;
            snprintf(k, sizeof(k), "k:%06u", g);
            snprintf(v, sizeof(v), "v:%u:%u", c, i);
            hc_put(&g_cyc_db, k, v);
            live[g] = 1;
        }
        /* Overwrite 10 keys from the previous cycle (resurrecting
         * the even ones this cycle's predecessor deleted). */
        if (c > 0) {
            for (i = 0; i < 10; i++) {
                uint32_t g = (c - 1) * 120 + i;
                snprintf(k, sizeof(k), "k:%06u", g);
                snprintf(v, sizeof(v), "w:%u", c);
                hc_put(&g_cyc_db, k, v);
                live[g] = 1;
            }
        }
        assert(segdb_flush(&g_cyc_db) == 0);
        assert(segdb_compact(&g_cyc_db, 0) == 0);
        /* Delete the even-indexed keys of this cycle. */
        for (i = 0; i < 120; i += 2) {
            uint32_t g = c * 120 + i;
            /* Keys 0..9 of this cycle were already overwritten by
             * the NEXT cycle only; at this point in this cycle
             * they are still the values just written. Deleting
             * them here is exactly what the next cycle's
             * overwrite resurrects — except for cycle 199. */
            snprintf(k, sizeof(k), "k:%06u", g);
            hc_del(&g_cyc_db, k);
            live[g] = 0;
        }
        assert(segdb_flush(&g_cyc_db) == 0);
        /* Cycle boundary: no writer is active, so reclaim may delete
         * every object no live manifest/directory/checkpoint depends
         * on — the compact temps and superseded segments/manifests.
         * Without this step the temps alone (one data temp per
         * compact, ~live size each) grow without bound and no bounded
         * heap survives 200 cycles; that is the reclaim mechanism's
         * job, not compaction's. */
        assert(yowza_segdb_heap_reclaim(&g_cyc_db) == 0);
        hc_reclaim_stats(&d, &b);
        tot_del += d;
        tot_bytes += b;
        if ((c + 1) % 25 == 0) {
            hc_info(&hs, &bump, &lv, &slots, &waste);
            if (waste > max_waste) max_waste = waste;
            printf("  cycle %u: bump=%llu live=%llu slots=%llu waste=%llu reclaimed_tot=%llu objs/%llu bytes\n",
                   c + 1, (unsigned long long)bump, (unsigned long long)lv,
                   (unsigned long long)slots, (unsigned long long)waste,
                   (unsigned long long)tot_del, (unsigned long long)tot_bytes);
        }
    }
    /* Explicit final reclaim + compaction, then verify every key. */
    assert(yowza_segdb_heap_reclaim(&g_cyc_db) == 0);
    hc_reclaim_stats(&d, &b);
    tot_del += d;
    tot_bytes += b;
    assert(yowza_segdb_heap_compact() == 0);
    hc_info(&hs, &bump, &lv, &slots, &waste);
    printf("  final: bump=%llu live=%llu slots=%llu waste=%llu max_waste=%llu reclaimed_tot=%llu objs/%llu bytes\n",
           (unsigned long long)bump, (unsigned long long)lv,
           (unsigned long long)slots, (unsigned long long)waste,
           (unsigned long long)max_waste,
           (unsigned long long)tot_del, (unsigned long long)tot_bytes);
    assert(bump < CYC_HEAP_SIZE);
    assert(waste < 65536); /* tight after an explicit compact */
    assert(tot_del > 0); /* reclaim must actually have removed orphans */
    assert(slots < 128); /* only live segments/manifests/CURRENT remain */
    for (c = 0; c < 200; c++) {
        for (i = 0; i < 120; i++) {
            uint32_t g = c * 120 + i;
            char k[32], v[40];
            int expect_live;
            snprintf(k, sizeof(k), "k:%06u", g);
            if (c < 199 && i < 10) {
                /* Overwritten by the next cycle. */
                expect_live = 1;
                snprintf(v, sizeof(v), "w:%u", c + 1);
            } else if ((i % 2) == 0) {
                expect_live = 0;
                v[0] = 0;
            } else {
                expect_live = 1;
                snprintf(v, sizeof(v), "v:%u:%u", c, i);
            }
            if (expect_live) {
                assert(hc_get_eq(&g_cyc_db, k, v));
            } else {
                assert(hc_missing(&g_cyc_db, k));
            }
            assert(live[g] == (uint8_t)expect_live);
        }
    }
    segdb_close(&g_cyc_db);
    printf("Passed.\n");
}

/* ---------- Test 2: corrupt slot range -> compact fails typed,
 * moves nothing, live data intact after repair. ---------- */
static void test_heap_compact_corrupt_no_partial(void) {
    int seg_slot = -1, other_seg_slot = -1;
    uint32_t i;
    uint8_t saved[HC_SLOT_SIZE];
    uint64_t bad_off;
    printf("Running test_heap_compact_corrupt_no_partial...\n");
    hc_init();
    {
        char k[32], v[32];
        uint32_t j;
        for (j = 0; j < 50; j++) {
            snprintf(k, sizeof(k), "a:%03u", j);
            snprintf(v, sizeof(v), "av:%u", j);
            hc_put(&g_hc_db, k, v);
        }
        assert(segdb_flush(&g_hc_db) == 0);
        for (j = 0; j < 50; j++) {
            snprintf(k, sizeof(k), "b:%03u", j);
            snprintf(v, sizeof(v), "bv:%u", j);
            hc_put(&g_hc_db, k, v);
        }
        assert(segdb_flush(&g_hc_db) == 0);
    }
    memcpy(g_hc_heap_copy, g_hc_heap, sizeof(g_hc_heap));
    /* Find two distinct segment slots. */
    for (i = 0; i < HC_TABLE_SLOTS; i++) {
        uint8_t* s = hc_slot((int)i);
        if (s[0] != 0 && strncmp((const char*)s, "seg.", 4) == 0) {
            if (seg_slot < 0) seg_slot = (int)i;
            else if (other_seg_slot < 0) { other_seg_slot = (int)i; break; }
        }
    }
    assert(seg_slot >= 0 && other_seg_slot >= 0);
    memcpy(saved, hc_slot(seg_slot), HC_SLOT_SIZE);
    /* Corrupt the range: offset beyond the heap. */
    bad_off = HC_HEAP_SIZE + 4096;
    hc_wr64(hc_slot(seg_slot) + 128, bad_off);
    assert(yowza_segdb_heap_compact() == -1);
    /* No partial compaction: the heap must equal the snapshot
     * with exactly this corruption applied. */
    {
        uint8_t* expect = g_hc_heap_copy;
        /* Apply the same corruption to the snapshot copy, then
         * the whole heap must compare equal. */
        uint8_t* es = expect + HC_HEADER_SIZE + (uint64_t)seg_slot * HC_SLOT_SIZE;
        hc_wr64(es + 128, bad_off);
        assert(memcmp(g_hc_heap, expect, sizeof(g_hc_heap)) == 0);
        /* Restore the snapshot's slot for the next step. */
        hc_wr64(es + 128, hc_rd64(saved + 128));
    }
    /* The uncorrupted segment's keys are still readable while the
     * other segment's slot is corrupt (at least one group must
     * be fully readable: the corrupted slot belongs to exactly
     * one of the two flushes). */
    {
        char k[32], v[32];
        uint32_t j, a_ok = 1, b_ok = 1;
        for (j = 0; j < 50; j++) {
            snprintf(k, sizeof(k), "a:%03u", j);
            snprintf(v, sizeof(v), "av:%u", j);
            if (!hc_get_eq(&g_hc_db, k, v)) a_ok = 0;
            snprintf(k, sizeof(k), "b:%03u", j);
            snprintf(v, sizeof(v), "bv:%u", j);
            if (!hc_get_eq(&g_hc_db, k, v)) b_ok = 0;
        }
        assert(a_ok || b_ok);
    }
    /* Repair, rebuild the index, compact must now succeed, and
     * every live key must be byte-for-byte intact. */
    memcpy(hc_slot(seg_slot), saved, HC_SLOT_SIZE);
    assert(yowza_segdb_heap_index_rebuild() == 0);
    assert(yowza_segdb_heap_compact() == 0);
    {
        char k[32], v[32];
        uint32_t j;
        for (j = 0; j < 50; j++) {
            snprintf(k, sizeof(k), "a:%03u", j);
            snprintf(v, sizeof(v), "av:%u", j);
            assert(hc_get_eq(&g_hc_db, k, v));
            snprintf(k, sizeof(k), "b:%03u", j);
            snprintf(v, sizeof(v), "bv:%u", j);
            assert(hc_get_eq(&g_hc_db, k, v));
        }
    }
    segdb_close(&g_hc_db);
    printf("Passed.\n");
}

/* ---------- Test 3: orphan deletion never deletes a referenced
 * object; corrupt CURRENT fails reclaim with nothing deleted. ---------- */
static void test_heap_reclaim_orphan_and_referenced(void) {
    uint8_t stats[16];
    uint64_t deleted;
    printf("Running test_heap_reclaim_orphan_and_referenced...\n");
    hc_init();
    {
        char k[32], v[32];
        uint32_t j;
        for (j = 0; j < 20; j++) {
            snprintf(k, sizeof(k), "r:%03u", j);
            snprintf(v, sizeof(v), "rv:%u", j);
            hc_put(&g_hc_db, k, v);
        }
        assert(segdb_flush(&g_hc_db) == 0);
        for (j = 20; j < 40; j++) {
            snprintf(k, sizeof(k), "r:%03u", j);
            snprintf(v, sizeof(v), "rv:%u", j);
            hc_put(&g_hc_db, k, v);
        }
        assert(segdb_flush(&g_hc_db) == 0);
        for (j = 40; j < 50; j++) {
            snprintf(k, sizeof(k), "r:%03u", j);
            snprintf(v, sizeof(v), "rv:%u", j);
            hc_put(&g_hc_db, k, v);
        }
        assert(segdb_flush(&g_hc_db) == 0);
    }
    /* Two genuine orphans, written straight through the store. */
    assert(g_hc_db.store->vt->write_at(g_hc_db.store->state, "orphan.test", 0,
                                       (const uint8_t*)"junk", 4) == 0);
    assert(g_hc_db.store->vt->write_at(g_hc_db.store->state, "orphan2.test", 0,
                                       (const uint8_t*)"junk2", 5) == 0);
    assert(hc_slot_find("orphan.test") >= 0);
    assert(hc_slot_find("orphan2.test") >= 0);
    /* Generation-1 manifest (manifest.2) is referenced fallback
     * and must survive; the current manifest (manifest.3) too. */
    assert(hc_slot_find("manifest.2") >= 0);
    assert(hc_slot_find("manifest.3") >= 0);
    assert(yowza_segdb_heap_reclaim(&g_hc_db) == 0);
    yowza_segdb_heap_reclaim_stats(stats);
    deleted = hc_rd64(stats);
    printf("  reclaim deleted=%llu\n", (unsigned long long)deleted);
    assert(deleted >= 2);
    assert(hc_slot_find("orphan.test") < 0);
    assert(hc_slot_find("orphan2.test") < 0);
    assert(hc_slot_find("manifest.2") >= 0); /* referenced: kept */
    assert(hc_slot_find("manifest.3") >= 0); /* current: kept */
    assert(hc_slot_find("CURRENT") >= 0);
    {
        char k[32], v[32];
        uint32_t j;
        for (j = 0; j < 50; j++) {
            snprintf(k, sizeof(k), "r:%03u", j);
            snprintf(v, sizeof(v), "rv:%u", j);
            assert(hc_get_eq(&g_hc_db, k, v));
        }
    }
    /* Corrupt CURRENT: reclaim must fail and delete nothing. */
    {
        static char names_before[64][128];
        static char names_after[64][128];
        uint32_t nb = 0, na = 0;
        uint32_t i;
        for (i = 0; i < HC_TABLE_SLOTS && nb < 64; i++) {
            uint8_t* s = hc_slot((int)i);
            if (s[0] != 0) {
                strncpy(names_before[nb], (const char*)s, 127);
                names_before[nb][127] = 0;
                nb++;
            }
        }
        /* CURRENT's content is "manifest.3" (10 bytes); overwrite
         * it with a name no manifest can parse. */
        assert(g_hc_db.store->vt->write_at(g_hc_db.store->state, "CURRENT", 0,
                                           (const uint8_t*)"garbage!!!", 10) == 0);
        assert(yowza_segdb_heap_reclaim(&g_hc_db) == -1);
        for (i = 0; i < HC_TABLE_SLOTS && na < 64; i++) {
            uint8_t* s = hc_slot((int)i);
            if (s[0] != 0) {
                strncpy(names_after[na], (const char*)s, 127);
                names_after[na][127] = 0;
                na++;
            }
        }
        assert(na == nb);
        for (i = 0; i < nb; i++) assert(strcmp(names_before[i], names_after[i]) == 0);
    }
    segdb_close(&g_hc_db);
    printf("Passed.\n");
}

/* ---------- Fan-in scan on the native memory store ---------- */
static SegDB g_fn_db;
static uint8_t g_fn_arena[1048576];
static uint8_t g_fn_wal[262144];
static uint8_t g_fn_out[131072];

/* Page through a fanin scan, verifying rows against a checker
 * callback that receives each row in order. Returns row count,
 * or -1 on protocol error. */
typedef int (*fn_row_cb)(void* ctx, const uint8_t* k, uint32_t klen,
                         const uint8_t* v, uint32_t vlen, uint32_t idx);

static int fn_page_all(SegDB* db, const uint8_t* prefix, uint32_t prefix_len,
                       uint64_t snap, uint32_t limit, fn_row_cb cb, void* ctx,
                       uint32_t* last_has_more) {
    uint8_t cursor[1024];
    uint32_t cursor_len = 0, has_cursor = 0, total = 0;
    for (;;) {
        uint32_t cnt = 0, next_len = 0, has_more = 0;
        uint32_t off = 0, i;
        int rc = segdb_scan_fanin(db, prefix, prefix_len, snap,
                                  cursor, cursor_len, (int)has_cursor, limit,
                                  g_fn_out, sizeof(g_fn_out), &cnt,
                                  cursor, sizeof(cursor), &next_len, &has_more);
        if (rc != 0) return -1;
        for (i = 0; i < cnt; i++) {
            uint32_t kl, vl;
            const uint8_t* k;
            const uint8_t* v;
            if (off + 8 > sizeof(g_fn_out)) return -1;
            kl = (uint32_t)g_fn_out[off] | ((uint32_t)g_fn_out[off + 1] << 8) |
                 ((uint32_t)g_fn_out[off + 2] << 16) | ((uint32_t)g_fn_out[off + 3] << 24);
            off += 4;
            k = &g_fn_out[off];
            off += kl;
            vl = (uint32_t)g_fn_out[off] | ((uint32_t)g_fn_out[off + 1] << 8) |
                 ((uint32_t)g_fn_out[off + 2] << 16) | ((uint32_t)g_fn_out[off + 3] << 24);
            off += 4;
            v = &g_fn_out[off];
            off += vl;
            if (cb && cb(ctx, k, kl, v, vl, total) != 0) return -1;
            total++;
        }
        if (last_has_more) *last_has_more = has_more;
        if (cnt == 0) {
            if (has_more != 0) return -1;
            break;
        }
        /* Cursor for the next page is the returned next key. The
         * export wrote it into `cursor` already (same buffer was
         * passed as next_key); keep its length. */
        cursor_len = next_len;
        has_cursor = 1;
        if (has_more == 0) break;
        if (cnt < limit && has_more == 0) break;
    }
    return (int)total;
}

typedef struct {
    uint32_t next; /* next expected index, or UINT32_MAX for gaps */
    int ok;
    char prev[64];
    uint32_t prev_len;
} FnExpectCtx;

/* Expected model for the 202-segment DB: keys f:%06u for
 * i in 0..12799, values v:%u, except 100..199 -> w:%u and
 * 500..509 deleted. The checker walks i in order, skipping the
 * deleted range. */
static int fn_check_model(void* vctx, const uint8_t* k, uint32_t klen,
                          const uint8_t* v, uint32_t vlen, uint32_t idx) {
    FnExpectCtx* c = (FnExpectCtx*)vctx;
    char ek[32], ev[32];
    uint32_t i = c->next;
    (void)idx;
    while (i >= 500 && i <= 509) i++;
    if (i >= 12800) { c->ok = 0; return -1; }
    snprintf(ek, sizeof(ek), "f:%06u", i);
    if (i >= 100 && i <= 199) snprintf(ev, sizeof(ev), "w:%u", i);
    else snprintf(ev, sizeof(ev), "v:%u", i);
    if (klen != strlen(ek) || memcmp(k, ek, klen) != 0) { c->ok = 0; return -1; }
    if (vlen != strlen(ev) || memcmp(v, ev, vlen) != 0) { c->ok = 0; return -1; }
    if (c->prev_len > 0 && memcmp(c->prev, k, c->prev_len < klen ? c->prev_len : klen) == 0 &&
        c->prev_len >= klen) {
        /* Non-increasing order (equal keys would also fail here
         * unless strictly longer, which cannot happen for fixed
         * -width keys). */
        if (c->prev_len == klen) { c->ok = 0; return -1; }
    }
    memcpy(c->prev, k, klen);
    c->prev_len = klen;
    c->next = i + 1;
    return 0;
}

static void test_fanin_over_128_sources(void) {
    YowzaStore* st;
    uint64_t snap_old, snap_new;
    FnExpectCtx ctx;
    int total;
    uint32_t has_more = 99;
    printf("Running test_fanin_over_128_sources...\n");
    st = yowza_store_mem_create();
    assert(st != NULL);
    assert(segdb_init(&g_fn_db, st, g_fn_arena, sizeof(g_fn_arena),
                      g_fn_wal, sizeof(g_fn_wal)) == 0);
    {
        uint32_t f, j;
        char k[32], v[32];
        for (f = 0; f < 200; f++) {
            for (j = 0; j < 64; j++) {
                uint32_t i = f * 64 + j;
                snprintf(k, sizeof(k), "f:%06u", i);
                snprintf(v, sizeof(v), "v:%u", i);
                hc_put(&g_fn_db, k, v);
            }
            assert(segdb_flush(&g_fn_db) == 0);
        }
        for (j = 100; j <= 199; j++) {
            snprintf(k, sizeof(k), "f:%06u", j);
            snprintf(v, sizeof(v), "w:%u", j);
            hc_put(&g_fn_db, k, v);
        }
        assert(segdb_flush(&g_fn_db) == 0);
        for (j = 500; j <= 509; j++) {
            snprintf(k, sizeof(k), "f:%06u", j);
            hc_del(&g_fn_db, k);
        }
        assert(segdb_flush(&g_fn_db) == 0);
    }
    snap_old = segdb_snapshot_seq(&g_fn_db);
    /* The old cursor path must fail at >128 sources (the bound
     * this gate removes); the fanin path must not. */
    {
        uint32_t cnt = 0, nl = 0, hm = 0;
        uint8_t nk[1024];
        int rc = segdb_scan_cursor(&g_fn_db, NULL, 0, snap_old, NULL, 0, 0, 10,
                                   g_fn_out, sizeof(g_fn_out), &cnt,
                                   nk, sizeof(nk), &nl, &hm);
        assert(rc == -1);
    }
    memset(&ctx, 0, sizeof(ctx));
    ctx.ok = 1;
    ctx.next = 0;
    total = fn_page_all(&g_fn_db, NULL, 0, snap_old, 1000, fn_check_model, &ctx, &has_more);
    assert(total == 12790);
    assert(ctx.ok);
    assert(ctx.next == 12800);
    assert(has_more == 0);
    /* Hot changes after the snapshot: new keys, an overwrite, a
     * delete. Old snapshot must be unaffected; current must
     * reflect hot. */
    {
        char k[32], v[32];
        uint32_t j;
        for (j = 12800; j <= 12804; j++) {
            snprintf(k, sizeof(k), "f:%06u", j);
            snprintf(v, sizeof(v), "n:%u", j);
            hc_put(&g_fn_db, k, v);
        }
        hc_put(&g_fn_db, "f:000000", "hot0");
        hc_del(&g_fn_db, "f:000001");
    }
    snap_new = segdb_snapshot_seq(&g_fn_db);
    memset(&ctx, 0, sizeof(ctx));
    ctx.ok = 1;
    ctx.next = 0;
    total = fn_page_all(&g_fn_db, NULL, 0, snap_old, 1000, fn_check_model, &ctx, NULL);
    assert(total == 12790);
    assert(ctx.ok);
    /* Current snapshot: key 0 overwritten by hot, key 1 deleted
     * by hot, 5 new keys appended. Verify with a dedicated pass. */
    {
        uint8_t cursor[1024];
        uint32_t cursor_len = 0, has_cursor = 0, got = 0, hm = 1;
        int seen_hot0 = 0, seen_new = 0;
        while (hm) {
            uint32_t cnt = 0, nl = 0;
            uint32_t off = 0, i;
            int rc = segdb_scan_fanin(&g_fn_db, NULL, 0, snap_new,
                                      cursor, cursor_len, (int)has_cursor, 1000,
                                      g_fn_out, sizeof(g_fn_out), &cnt,
                                      cursor, sizeof(cursor), &nl, &hm);
            assert(rc == 0);
            for (i = 0; i < cnt; i++) {
                uint32_t kl, vl;
                const uint8_t* k;
                const uint8_t* v;
                kl = (uint32_t)g_fn_out[off] | ((uint32_t)g_fn_out[off + 1] << 8) |
                     ((uint32_t)g_fn_out[off + 2] << 16) | ((uint32_t)g_fn_out[off + 3] << 24);
                off += 4;
                k = &g_fn_out[off];
                off += kl;
                vl = (uint32_t)g_fn_out[off] | ((uint32_t)g_fn_out[off + 1] << 8) |
                     ((uint32_t)g_fn_out[off + 2] << 16) | ((uint32_t)g_fn_out[off + 3] << 24);
                off += 4;
                v = &g_fn_out[off];
                off += vl;
                if (kl == 8 && memcmp(k, "f:000000", 8) == 0) {
                    assert(vl == 4 && memcmp(v, "hot0", 4) == 0);
                    seen_hot0 = 1;
                }
                if (kl == 8 && memcmp(k, "f:000001", 8) == 0) assert(0);
                if (kl == 8 && memcmp(k, "f:012800", 8) == 0) seen_new = 1;
                got++;
            }
            cursor_len = nl;
            has_cursor = 1;
            if (cnt == 0) break;
        }
        assert(got == 12794);
        assert(seen_hot0 && seen_new);
    }
    /* Prefix subset: f:0001 -> keys 100..199 with w: values. */
    {
        uint8_t cursor[1024];
        uint32_t cursor_len = 0, has_cursor = 0, got = 0, hm = 1;
        while (hm) {
            uint32_t cnt = 0, nl = 0;
            uint32_t off = 0, i;
            int rc = segdb_scan_fanin(&g_fn_db, (const uint8_t*)"f:0001", 6, snap_new,
                                      cursor, cursor_len, (int)has_cursor, 1000,
                                      g_fn_out, sizeof(g_fn_out), &cnt,
                                      cursor, sizeof(cursor), &nl, &hm);
            assert(rc == 0);
            for (i = 0; i < cnt; i++) {
                uint32_t kl, vl;
                const uint8_t* k;
                const uint8_t* v;
                char ek[32], ev[32];
                kl = (uint32_t)g_fn_out[off] | ((uint32_t)g_fn_out[off + 1] << 8) |
                     ((uint32_t)g_fn_out[off + 2] << 16) | ((uint32_t)g_fn_out[off + 3] << 24);
                off += 4;
                k = &g_fn_out[off];
                off += kl;
                vl = (uint32_t)g_fn_out[off] | ((uint32_t)g_fn_out[off + 1] << 8) |
                     ((uint32_t)g_fn_out[off + 2] << 16) | ((uint32_t)g_fn_out[off + 3] << 24);
                off += 4;
                v = &g_fn_out[off];
                off += vl;
                snprintf(ek, sizeof(ek), "f:%06u", 100 + got);
                snprintf(ev, sizeof(ev), "w:%u", 100 + got);
                assert(kl == strlen(ek) && memcmp(k, ek, kl) == 0);
                assert(vl == strlen(ev) && memcmp(v, ev, vl) == 0);
                got++;
            }
            cursor_len = nl;
            has_cursor = 1;
            if (cnt == 0) break;
        }
        assert(got == 100);
    }
    segdb_close(&g_fn_db);
    printf("Passed.\n");
}

static void test_fanin_invalid_cursor(void) {
    YowzaStore* st;
    static SegDB db2;
    static uint8_t arena2[1048576];
    static uint8_t wal2[262144];
    uint8_t curbuf[2048];
    uint8_t nk[1024];
    uint32_t cnt = 0, nl = 0, hm = 0;
    int rc;
    printf("Running test_fanin_invalid_cursor...\n");
    st = yowza_store_mem_create();
    assert(st != NULL);
    assert(segdb_init(&db2, st, arena2, sizeof(arena2), wal2, sizeof(wal2)) == 0);
    {
        char k[32], v[32];
        uint32_t j;
        for (j = 0; j < 20; j++) {
            snprintf(k, sizeof(k), "g:%03u", j);
            snprintf(v, sizeof(v), "gv:%u", j);
            hc_put(&db2, k, v);
            if (j == 9) assert(segdb_flush(&db2) == 0);
        }
        assert(segdb_flush(&db2) == 0);
    }
    {
        uint64_t snap = segdb_snapshot_seq(&db2);
        /* has_cursor with an empty cursor: typed -4, no rows,
         * output buffer untouched. */
        memset(g_fn_out, 0xA5, sizeof(g_fn_out));
        cnt = 777;
        rc = segdb_scan_fanin(&db2, NULL, 0, snap, NULL, 0, 1, 10,
                              g_fn_out, sizeof(g_fn_out), &cnt,
                              nk, sizeof(nk), &nl, &hm);
        assert(rc == -4 && cnt == 0);
        for (uint32_t i = 0; i < sizeof(g_fn_out); i++) assert(g_fn_out[i] == 0xA5);
        /* Over-long cursor (2000 > 1023): typed -4, no rows. */
        memset(curbuf, 'x', sizeof(curbuf));
        memset(g_fn_out, 0xA5, sizeof(g_fn_out));
        cnt = 777;
        rc = segdb_scan_fanin(&db2, NULL, 0, snap, curbuf, 2000, 1, 10,
                              g_fn_out, sizeof(g_fn_out), &cnt,
                              nk, sizeof(nk), &nl, &hm);
        assert(rc == -4 && cnt == 0);
        for (uint32_t i = 0; i < sizeof(g_fn_out); i++) assert(g_fn_out[i] == 0xA5);
        /* limit 0: typed -4. */
        rc = segdb_scan_fanin(&db2, NULL, 0, snap, NULL, 0, 0, 0,
                              g_fn_out, sizeof(g_fn_out), &cnt,
                              nk, sizeof(nk), &nl, &hm);
        assert(rc == -4 && cnt == 0);
        /* Valid cursor resumes exclusively after the key. */
        {
            uint32_t got = 0;
            uint8_t cur[1024];
            uint32_t clen = 0, hc2 = 0, hmv = 1;
            memcpy(cur, "g:005", 5);
            clen = 5;
            hc2 = 1;
            while (hmv) {
                uint32_t c2 = 0, n2 = 0;
                uint32_t off = 0, i;
                rc = segdb_scan_fanin(&db2, NULL, 0, snap, cur, clen, (int)hc2, 100,
                                      g_fn_out, sizeof(g_fn_out), &c2,
                                      cur, sizeof(cur), &n2, &hmv);
                assert(rc == 0);
                for (i = 0; i < c2; i++) {
                    uint32_t kl, vl;
                    const uint8_t* k;
                    char ek[32];
                    kl = (uint32_t)g_fn_out[off] | ((uint32_t)g_fn_out[off + 1] << 8) |
                         ((uint32_t)g_fn_out[off + 2] << 16) | ((uint32_t)g_fn_out[off + 3] << 24);
                    off += 4;
                    k = &g_fn_out[off];
                    off += kl;
                    vl = (uint32_t)g_fn_out[off] | ((uint32_t)g_fn_out[off + 1] << 8) |
                         ((uint32_t)g_fn_out[off + 2] << 16) | ((uint32_t)g_fn_out[off + 3] << 24);
                    off += 4 + vl;
                    snprintf(ek, sizeof(ek), "g:%03u", 6 + got);
                    assert(kl == strlen(ek) && memcmp(k, ek, kl) == 0);
                    got++;
                }
                clen = n2;
                hc2 = 1;
                if (c2 == 0) break;
            }
            assert(got == 14);
        }
    }
    /* Empty DB: zero rows, has_more 0. */
    {
        static SegDB db3;
        static uint8_t arena3[1048576];
        static uint8_t wal3[262144];
        YowzaStore* st3 = yowza_store_mem_create();
        assert(segdb_init(&db3, st3, arena3, sizeof(arena3), wal3, sizeof(wal3)) == 0);
        cnt = 777;
        rc = segdb_scan_fanin(&db3, NULL, 0, segdb_snapshot_seq(&db3), NULL, 0, 0, 10,
                              g_fn_out, sizeof(g_fn_out), &cnt,
                              nk, sizeof(nk), &nl, &hm);
        assert(rc == 0 && cnt == 0 && hm == 0 && nl == 0);
        segdb_close(&db3);
    }
    segdb_close(&db2);
    printf("Passed.\n");
}

__attribute__((constructor)) static void test_heap_reclaim_register(void) {
    test_heap_compact_cycles();
    test_heap_compact_corrupt_no_partial();
    test_heap_reclaim_orphan_and_referenced();
    test_fanin_over_128_sources();
    test_fanin_invalid_cursor();
}
