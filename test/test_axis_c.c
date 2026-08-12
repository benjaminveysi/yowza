/* Axis C tests (Timeline-v2 patch, C capacity + timeline-native
 * runtime). Self-registers through a constructor (same pattern
 * as test_vm_timeline.c): test/main.c is not edited.
 *
 * Proven here:
 *  1. History overflow: 1100 overwrites of one key (past the
 *     old 1024-entry fixed array) capture all 1100 versions,
 *     spill 76 entries into the overflow list, read the old
 *     value back at a mid-stream snapshot before AND after
 *     flush, and flush successfully (which, at 1100 ranks,
 *     also exercises the multi-group path: 1101 segments
 *     over 9 groups).
 *  2. Multi-group flush, dedicated: 150 versions of one key
 *     on a fresh DB (151 segments > the 128-segment group
 *     cap) flush into a 2-group directory and survive
 *     close/recover with both the current value and the
 *     pre-flush snapshot value intact.
 *  3. Retention floor (lease): a leased snapshot survives
 *     flush + a compact attempt (compact defers while the
 *     lease is live); the current head is unaffected.
 *  4. Paged sparse index: a >256-block segment (36000
 *     entries, ~340 blocks) — the old full view refuses
 *     it (cap 256), the paged view (64 entries/page)
 *     answers every sampled lookup byte-for-byte like the
 *     linear path, and the benchmark loop measures fewer
 *     index entries examined on the paged path.
 *  5. yowza.c timeline glue: the sticky handle set via
 *     yowza_timeline_set survives yowza_execute's vm_init
 *     across programs (snapshot reads, post-snapshot
 *     writes invisible with -11, staged writes drainable
 *     and absent from the ART), a future snapshot is
 *     rejected, the one-shot yowza_execute_timeline does
 *     not disturb the sticky handle, and clearing the
 *     handle restores the normal path.
 * The wasm-path Go test (host calling the new exports
 * through wazero) is NOT in this file: *.go files are
 * outside this axis's file set. The exports' presence
 * in the rebuilt wasm binary is checked by the patch
 * notes' export-section listing instead.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <stdint.h>
#include <time.h>

#include "segdb.h"
#include "storage2.h"
#include "segment.h"
#include "manifest.h"
#include "vm.h"

/* ---- shared SegDB helpers ---- */

static void ax_put(SegDB* db, const char* k, const char* v) {
    assert(segdb_put(db, (const uint8_t*)k, (uint32_t)strlen(k),
                     (const uint8_t*)v, (uint32_t)strlen(v)) == 0);
}

static int ax_has(SegDB* db, const char* k, const char* want) {
    uint8_t* out = NULL;
    uint32_t len = 0;
    if (segdb_get(db, (const uint8_t*)k, (uint32_t)strlen(k), &out, &len) != 0) return 0;
    return len == strlen(want) && (len == 0 || memcmp(out, want, len) == 0);
}

/* Parse one art_scan-format scan result and assert it is
 * exactly one entry (key, val) under the given prefix. */
static void ax_scan_single(SegDB* db, uint64_t snap, const char* prefix, const char* ek, const char* ev) {
    static uint8_t out[65536];
    uint32_t count = 0, kl, vl, off = 0;
    assert(segdb_scan(db, (const uint8_t*)prefix, (uint32_t)strlen(prefix), snap, out, sizeof(out), &count) == 0);
    assert(count == 1);
    kl = (uint32_t)out[off] | ((uint32_t)out[off + 1] << 8) |
         ((uint32_t)out[off + 2] << 16) | ((uint32_t)out[off + 3] << 24);
    off += 4;
    assert(kl == strlen(ek) && memcmp(&out[off], ek, kl) == 0);
    off += kl;
    vl = (uint32_t)out[off] | ((uint32_t)out[off + 1] << 8) |
         ((uint32_t)out[off + 2] << 16) | ((uint32_t)out[off + 3] << 24);
    off += 4;
    assert(vl == strlen(ev) && (vl == 0 || memcmp(&out[off], ev, vl) == 0));
}

/* ---- 1. history overflow (+ multi-group via 1100 ranks) ---- */

static SegDB ax_db1;
static uint8_t ax_arena1[262144];
static uint8_t ax_wal1[262144];

static void test_axis_c_history_overflow(void) {
    printf("Running test_axis_c_history_overflow...\n");
    YowzaStore* st = yowza_store_mem_create();
    uint64_t snap = 0;
    int i;
    SegdbHistEntry he;
    assert(st != NULL);
    assert(segdb_init(&ax_db1, st, ax_arena1, sizeof(ax_arena1), ax_wal1, sizeof(ax_wal1)) == 0);
    ax_put(&ax_db1, "k", "v0000");
    for (i = 1; i <= 1100; i++) {
        char vb[8];
        snprintf(vb, sizeof(vb), "v%04d", i);
        ax_put(&ax_db1, "k", vb);
        if (i == 500) snap = segdb_snapshot_seq(&ax_db1);
    }
    /* 1100 captures: past the old 1024 fixed array, with
     * 76 spilled into the overflow list, nothing lost. */
    assert(segdb_history_count(&ax_db1) == 1100);
    assert(segdb_history_overflow_count(&ax_db1) == 76);
    assert(segdb_history_at(&ax_db1, 0, &he) == 0);
    assert(he.val_len == 5 && memcmp(he.val, "v0000", 5) == 0);
    assert(segdb_history_at(&ax_db1, 1024, &he) == 0); /* first overflow entry */
    assert(he.val_len == 5 && memcmp(he.val, "v1024", 5) == 0);
    assert(segdb_history_at(&ax_db1, 1099, &he) == 0);
    assert(he.val_len == 5 && memcmp(he.val, "v1099", 5) == 0);
    assert(segdb_history_at(&ax_db1, 1100, &he) != 0); /* out of range, loud */
    /* Snapshot read before flush: the mid-stream value. */
    ax_scan_single(&ax_db1, snap, "", "k", "v0500");
    /* Flush: 1100 rank segments + current = 1101 segments
     * over ceil(1101/128) = 9 fresh groups. */
    assert(segdb_flush(&ax_db1) == 0);
    assert(ax_db1.has_dir == 1);
    assert(ax_db1.group_count == 9);
    assert(segdb_history_count(&ax_db1) == 0);
    assert(ax_has(&ax_db1, "k", "v1100"));
    /* Same snapshot read after flush, from segments. */
    ax_scan_single(&ax_db1, snap, "", "k", "v0500");
    segdb_close(&ax_db1);
    yowza_store_close(st);
    printf("Passed. (history=1100, overflow=76, groups=9)\n");
}

/* ---- 2. multi-group flush, dedicated + recover ---- */

static SegDB ax_db2, ax_db2r;
static uint8_t ax_arena2[65536];
static uint8_t ax_wal2[65536];
static uint8_t ax_arena2r[65536];
static uint8_t ax_wal2r[65536];

static void test_axis_c_multigroup_flush(void) {
    printf("Running test_axis_c_multigroup_flush...\n");
    YowzaStore* st = yowza_store_mem_create();
    uint64_t snap0;
    int i;
    assert(st != NULL);
    assert(segdb_init(&ax_db2, st, ax_arena2, sizeof(ax_arena2), ax_wal2, sizeof(ax_wal2)) == 0);
    ax_put(&ax_db2, "m", "m000");
    snap0 = segdb_snapshot_seq(&ax_db2);
    for (i = 1; i <= 150; i++) {
        char vb[8];
        snprintf(vb, sizeof(vb), "m%03d", i);
        ax_put(&ax_db2, "m", vb);
    }
    ax_put(&ax_db2, "other", "o");
    assert(segdb_history_count(&ax_db2) == 150);
    /* 150 rank segments + current = 151 > 128: the flush
     * must split into fresh groups, not fail with -1. */
    assert(segdb_flush(&ax_db2) == 0);
    assert(ax_db2.has_dir == 1);
    assert(ax_db2.group_count == 2);
    assert(ax_has(&ax_db2, "m", "m150"));
    assert(ax_has(&ax_db2, "other", "o"));
    ax_scan_single(&ax_db2, snap0, "", "m", "m000");
    segdb_close(&ax_db2);
    /* Recover over the same store: directory mode, both
     * groups, current + snapshot values intact. */
    assert(segdb_init(&ax_db2r, st, ax_arena2r, sizeof(ax_arena2r), ax_wal2r, sizeof(ax_wal2r)) == 0);
    assert(segdb_recover(&ax_db2r) == 0);
    assert(ax_db2r.has_dir == 1);
    assert(ax_db2r.group_count == 2);
    assert(ax_has(&ax_db2r, "m", "m150"));
    assert(ax_has(&ax_db2r, "other", "o"));
    ax_scan_single(&ax_db2r, snap0, "", "m", "m000");
    segdb_close(&ax_db2r);
    yowza_store_close(st);
    printf("Passed. (ranks=150, segments=151, groups=2)\n");
}

/* ---- 3. retention floor: lease pins history across flush+compact ---- */

static SegDB ax_db3;
static uint8_t ax_arena3[65536];
static uint8_t ax_wal3[65536];

static void test_axis_c_retention_lease(void) {
    printf("Running test_axis_c_retention_lease...\n");
    YowzaStore* st = yowza_store_mem_create();
    uint64_t snap, lease;
    assert(st != NULL);
    assert(segdb_init(&ax_db3, st, ax_arena3, sizeof(ax_arena3), ax_wal3, sizeof(ax_wal3)) == 0);
    ax_put(&ax_db3, "a", "v1");
    ax_put(&ax_db3, "b", "x");
    snap = segdb_snapshot_seq(&ax_db3);
    lease = segdb_lease_acquire(&ax_db3, snap);
    assert(lease > 0);
    ax_put(&ax_db3, "a", "v2");
    ax_put(&ax_db3, "a", "v3");
    ax_put(&ax_db3, "a", "v4");
    assert(segdb_flush(&ax_db3) == 0);
    /* Compact defers while the lease is live: the leased
     * snapshot must still read v1 afterwards. */
    assert(segdb_compact(&ax_db3, 0) == 0);
    ax_scan_single(&ax_db3, snap, "a", "a", "v1");
    assert(ax_has(&ax_db3, "a", "v4"));
    assert(ax_has(&ax_db3, "b", "x"));
    assert(segdb_lease_release(&ax_db3, lease) == 0);
    /* After release, compact may merge; current head is
     * unaffected either way. */
    assert(segdb_compact(&ax_db3, 0) == 0);
    assert(ax_has(&ax_db3, "a", "v4"));
    assert(ax_has(&ax_db3, "b", "x"));
    segdb_close(&ax_db3);
    yowza_store_close(st);
    printf("Passed.\n");
}

/* ---- 4. paged sparse index ---- */

/* Minimal in-test memory store for the segment API. */
#define AX_MAX_OBJS 8
typedef struct {
    char name[256];
    uint8_t* data;
    uint64_t len;
    uint64_t cap;
    int used;
} AxObj;
typedef struct {
    AxObj objs[AX_MAX_OBJS];
} AxStoreState;

static AxObj* ax_find(AxStoreState* s, const char* name) {
    int i;
    for (i = 0; i < AX_MAX_OBJS; i++)
        if (s->objs[i].used && strcmp(s->objs[i].name, name) == 0) return &s->objs[i];
    return NULL;
}
static AxObj* ax_find_or_create(AxStoreState* s, const char* name) {
    AxObj* o = ax_find(s, name);
    int i;
    if (o) return o;
    for (i = 0; i < AX_MAX_OBJS; i++) {
        if (!s->objs[i].used) {
            s->objs[i].used = 1;
            s->objs[i].len = 0;
            s->objs[i].cap = 0;
            s->objs[i].data = NULL;
            strncpy(s->objs[i].name, name, 255);
            s->objs[i].name[255] = '\0';
            return &s->objs[i];
        }
    }
    return NULL;
}
static int ax_grow(AxObj* o, uint64_t need) {
    uint64_t ncap;
    uint8_t* nd;
    if (need <= o->cap) return 0;
    ncap = o->cap ? o->cap : 1024;
    while (ncap < need) ncap *= 2;
    nd = (uint8_t*)realloc(o->data, (size_t)ncap);
    if (!nd) return -1;
    memset(nd + o->cap, 0, (size_t)(ncap - o->cap));
    o->data = nd;
    o->cap = ncap;
    return 0;
}
static int ax_write_at(void* stp, const char* obj, uint64_t off, const uint8_t* data, uint64_t len) {
    AxStoreState* s = (AxStoreState*)stp;
    AxObj* o = ax_find_or_create(s, obj);
    if (!o) return -1;
    if (ax_grow(o, off + len) != 0) return -1;
    memcpy(o->data + off, data, (size_t)len);
    if (off + len > o->len) o->len = off + len;
    return 0;
}
static int ax_read_at(void* stp, const char* obj, uint64_t off, uint8_t* buf, uint64_t len) {
    AxStoreState* s = (AxStoreState*)stp;
    AxObj* o = ax_find(s, obj);
    if (!o) return -1;
    if (off + len > o->len) return -1;
    memcpy(buf, o->data + off, (size_t)len);
    return 0;
}
static int ax_size(void* stp, const char* obj, uint64_t* out) {
    AxStoreState* s = (AxStoreState*)stp;
    AxObj* o = ax_find(s, obj);
    if (!o) return -1;
    *out = o->len;
    return 0;
}
static int ax_sync_obj(void* stp, const char* obj) { (void)stp; (void)obj; return 0; }
static int ax_sync_dir(void* stp) { (void)stp; return 0; }
static int ax_publish(void* stp, const char* tmp_obj, const char* final_obj) {
    AxStoreState* s = (AxStoreState*)stp;
    AxObj* tmp = ax_find(s, tmp_obj);
    AxObj* fin;
    if (!tmp) return -1;
    fin = ax_find_or_create(s, final_obj);
    if (!fin) return -1;
    if (ax_grow(fin, tmp->len) != 0) return -1;
    memcpy(fin->data, tmp->data, (size_t)tmp->len);
    fin->len = tmp->len;
    tmp->used = 0;
    free(tmp->data);
    tmp->data = NULL;
    tmp->len = 0;
    tmp->cap = 0;
    return 0;
}
static int ax_remove(void* stp, const char* obj) {
    AxStoreState* s = (AxStoreState*)stp;
    AxObj* o = ax_find(s, obj);
    if (!o) return -1;
    o->used = 0;
    free(o->data);
    o->data = NULL;
    o->len = 0;
    o->cap = 0;
    return 0;
}
static int ax_truncate(void* stp, const char* obj, uint64_t len) {
    AxStoreState* s = (AxStoreState*)stp;
    AxObj* o = ax_find(s, obj);
    if (!o) return -1;
    if (len > o->len) {
        if (ax_grow(o, len) != 0) return -1;
        memset(o->data + o->len, 0, (size_t)(len - o->len));
    }
    o->len = len;
    return 0;
}
static void ax_close_store(void* stp) {
    AxStoreState* s = (AxStoreState*)stp;
    int i;
    for (i = 0; i < AX_MAX_OBJS; i++) {
        free(s->objs[i].data);
        s->objs[i].data = NULL;
    }
}

static const YowzaStoreVTable ax_vt = {
    ax_write_at, ax_read_at, ax_size, ax_sync_obj, ax_sync_dir,
    ax_publish, ax_remove, ax_truncate, ax_close_store, NULL, NULL
};

#define AX_SEG_N 36000
static char ax_seg_keys[AX_SEG_N][8];
static char ax_seg_vals[AX_SEG_N][8];
static SegEntry ax_seg_entries[AX_SEG_N];
static uint64_t ax_page_offs[1024];
static uint64_t ax_view256[256];

static void ax_seg_lookup_cmp(SegReader* r, const char* key, int* rc_out,
                              char* val_out, uint32_t* vlen_out, uint64_t* seq_out) {
    SegEntry e;
    uint8_t vb[64];
    int rc = seg_lookup(r, (const uint8_t*)key, (uint32_t)strlen(key), &e, vb, sizeof(vb));
    *rc_out = rc;
    if (rc == 0) {
        *vlen_out = e.val_len;
        *seq_out = e.commit_seq;
        if (e.val_len > 0) memcpy(val_out, e.val, e.val_len);
        val_out[e.val_len] = '\0';
    } else {
        *vlen_out = 0;
        *seq_out = 0;
        val_out[0] = '\0';
    }
}

static void test_axis_c_paged_index(void) {
    printf("Running test_axis_c_paged_index...\n");
    static AxStoreState sst;
    static YowzaStore store;
    SegReader r;
    uint64_t out_bytes = 0;
    int i, pages;
    uint64_t paged_entries, linear_entries;
    double paged_secs, linear_secs;
    clock_t t0, t1;
    memset(&sst, 0, sizeof(sst));
    store.vt = &ax_vt;
    store.state = &sst;
    for (i = 0; i < AX_SEG_N; i++) {
        snprintf(ax_seg_keys[i], sizeof(ax_seg_keys[i]), "k%06d", i);
        snprintf(ax_seg_vals[i], sizeof(ax_seg_vals[i]), "v%06d", i);
        ax_seg_entries[i].key = (const uint8_t*)ax_seg_keys[i];
        ax_seg_entries[i].key_len = 7;
        ax_seg_entries[i].val = (const uint8_t*)ax_seg_vals[i];
        ax_seg_entries[i].val_len = 7;
        ax_seg_entries[i].is_tombstone = 0;
        ax_seg_entries[i].commit_seq = (uint64_t)i + 1;
        ax_seg_entries[i].crdt_ts = 0;
    }
    assert(seg_write(&store, "bigseg", ax_seg_entries, AX_SEG_N, 0, 0, 1, &out_bytes) == 0);
    assert(seg_open(&store, "bigseg", &r) == 0);
    assert(seg_ensure_discovered(&r) == 0);
    printf("  blocks=%u (must exceed 256)\n", r.block_count);
    assert(r.block_count > 256);
    /* The old full view cannot hold this segment. */
    assert(seg_index_view(&r, ax_view256, 256) == -1);
    pages = seg_index_view_paged(&r, ax_page_offs, 1024, SEG_INDEX_PAGE_SIZE);
    assert(pages > 0);
    assert(pages == (int)((r.block_count + SEG_INDEX_PAGE_SIZE - 1) / SEG_INDEX_PAGE_SIZE));
    printf("  pages=%d page_size=%d\n", pages, SEG_INDEX_PAGE_SIZE);
    /* Correctness: paged vs linear, byte-for-byte, on a
     * sample spanning first/middle/last + absent keys. */
    {
        int sample_idx[] = {0, 1, 63, 64, 100, 997, 5000, 15000, 17999, 30000, 35998, 35999};
        const char* absent[] = {"a", "k-1", "k36000", "zzzz"};
        int si;
        for (si = 0; si < (int)(sizeof(sample_idx) / sizeof(sample_idx[0])); si++) {
            int idx = sample_idx[si];
            int rc_p, rc_l;
            char vp[64], vl[64];
            uint32_t lp, ll;
            uint64_t sp, sl;
            r.idx_page_offs = ax_page_offs;
            r.idx_page_count = (uint32_t)pages;
            r.idx_page_size = SEG_INDEX_PAGE_SIZE;
            ax_seg_lookup_cmp(&r, ax_seg_keys[idx], &rc_p, vp, &lp, &sp);
            r.idx_page_offs = NULL;
            r.idx_page_count = 0;
            r.idx_page_size = 0;
            ax_seg_lookup_cmp(&r, ax_seg_keys[idx], &rc_l, vl, &ll, &sl);
            assert(rc_p == 0 && rc_l == 0);
            assert(lp == ll && lp == 7 && memcmp(vp, vl, 7) == 0);
            assert(memcmp(vp, ax_seg_vals[idx], 7) == 0);
            assert(sp == sl && sp == (uint64_t)idx + 1);
        }
        for (si = 0; si < 4; si++) {
            int rc_p, rc_l;
            char vp[64], vl[64];
            uint32_t lp, ll;
            uint64_t sp, sl;
            r.idx_page_offs = ax_page_offs;
            r.idx_page_count = (uint32_t)pages;
            r.idx_page_size = SEG_INDEX_PAGE_SIZE;
            ax_seg_lookup_cmp(&r, absent[si], &rc_p, vp, &lp, &sp);
            r.idx_page_offs = NULL;
            r.idx_page_count = 0;
            r.idx_page_size = 0;
            ax_seg_lookup_cmp(&r, absent[si], &rc_l, vl, &ll, &sl);
            assert(rc_p == rc_l && rc_p == 1);
        }
        /* Torn paged view (wrong page count) must fall
         * back to linear, never to a false absent. */
        r.idx_page_offs = ax_page_offs;
        r.idx_page_count = (uint32_t)pages - 1;
        r.idx_page_size = SEG_INDEX_PAGE_SIZE;
        {
            int rc;
            char vv[64];
            uint32_t lv;
            uint64_t sq;
            ax_seg_lookup_cmp(&r, ax_seg_keys[15000], &rc, vv, &lv, &sq);
            assert(rc == 0 && lv == 7 && memcmp(vv, ax_seg_vals[15000], 7) == 0);
        }
        r.idx_page_offs = NULL;
        r.idx_page_count = 0;
        r.idx_page_size = 0;
    }
    /* Benchmark: 5000 distinct present keys, each path.
     * Primary metric is deterministic (index entries
     * examined); wall time is reported, not asserted. */
    r.idx_page_offs = ax_page_offs;
    r.idx_page_count = (uint32_t)pages;
    r.idx_page_size = SEG_INDEX_PAGE_SIZE;
    seg_lookup_index_entries_reset();
    t0 = clock();
    for (i = 0; i < 5000; i++) {
        int idx = (i * 7) % AX_SEG_N;
        int rc;
        char vv[64];
        uint32_t lv;
        uint64_t sq;
        ax_seg_lookup_cmp(&r, ax_seg_keys[idx], &rc, vv, &lv, &sq);
        assert(rc == 0 && lv == 7 && memcmp(vv, ax_seg_vals[idx], 7) == 0);
    }
    t1 = clock();
    paged_entries = seg_lookup_index_entries();
    paged_secs = (double)(t1 - t0) / CLOCKS_PER_SEC;
    r.idx_page_offs = NULL;
    r.idx_page_count = 0;
    r.idx_page_size = 0;
    seg_lookup_index_entries_reset();
    t0 = clock();
    for (i = 0; i < 5000; i++) {
        int idx = (i * 7) % AX_SEG_N;
        int rc;
        char vv[64];
        uint32_t lv;
        uint64_t sq;
        ax_seg_lookup_cmp(&r, ax_seg_keys[idx], &rc, vv, &lv, &sq);
        assert(rc == 0);
    }
    t1 = clock();
    linear_entries = seg_lookup_index_entries();
    linear_secs = (double)(t1 - t0) / CLOCKS_PER_SEC;
    printf("  paged: %llu index entries, %.3f s; linear: %llu index entries, %.3f s\n",
           (unsigned long long)paged_entries, paged_secs,
           (unsigned long long)linear_entries, linear_secs);
    assert(paged_entries < linear_entries);
    seg_close(&r);
    ax_close_store(&sst);
    printf("Passed.\n");
}

/* ---- 5. yowza.c timeline glue ---- */

/* The glue lives in src/yowza.c, which the Makefile
 * excludes from the native test build only because its
 * wasm export attributes warn natively. Axis C gates
 * those attributes behind __wasm__ (see yowza.c), so
 * including the file here compiles warning-free and
 * exercises the real glue code, globals included. */
#include "../src/yowza.c"

static uint8_t ax_y_arena[65536];
static uint8_t ax_y_wal[16384];

static void ax_y_w32(uint8_t* buf, size_t* off, uint32_t v) {
    buf[(*off)++] = (uint8_t)(v & 0xFF);
    buf[(*off)++] = (uint8_t)((v >> 8) & 0xFF);
    buf[(*off)++] = (uint8_t)((v >> 16) & 0xFF);
    buf[(*off)++] = (uint8_t)((v >> 24) & 0xFF);
}
static void ax_y_put(uint8_t* buf, size_t* off, const char* k, const char* v) {
    buf[(*off)++] = OP_PUT;
    ax_y_w32(buf, off, (uint32_t)strlen(k));
    memcpy(&buf[*off], k, strlen(k));
    *off += strlen(k);
    ax_y_w32(buf, off, (uint32_t)strlen(v));
    memcpy(&buf[*off], v, strlen(v));
    *off += strlen(v);
}
static void ax_y_get(uint8_t* buf, size_t* off, const char* k) {
    buf[(*off)++] = OP_GET;
    ax_y_w32(buf, off, (uint32_t)strlen(k));
    memcpy(&buf[*off], k, strlen(k));
    *off += strlen(k);
}

static void test_axis_c_yowza_timeline_glue(void) {
    printf("Running test_axis_c_yowza_timeline_glue...\n");
    uint8_t prog[256];
    size_t pc;
    VMContext* ret;
    uint64_t snap;
    assert(yowza_init(ax_y_arena, sizeof(ax_y_arena), ax_y_wal, sizeof(ax_y_wal)) == 0);
    /* Base state via the normal path. */
    pc = 0;
    ax_y_put(prog, &pc, "base:k", "old");
    ax_y_put(prog, &pc, "stable:k", "stable");
    prog[pc++] = OP_EXIT;
    ret = yowza_execute(prog, pc);
    assert((intptr_t)ret > 0x1000);
    snap = global_art.commit_seq;
    assert(snap > 0);
    /* Main head advances past the snapshot. */
    pc = 0;
    ax_y_put(prog, &pc, "base:k", "new");
    ax_y_put(prog, &pc, "main:only", "m");
    prog[pc++] = OP_EXIT;
    ret = yowza_execute(prog, pc);
    assert((intptr_t)ret > 0x1000);
    /* Sticky handle: set once, survives vm_init across
     * programs (yowza_execute re-applies it after init). */
    assert(yowza_timeline_set(snap) == 0);
    assert(yowza_timeline_active() == 1);
    assert(yowza_timeline_snapshot() == snap);
    /* Program 1 under the sticky handle: base value at
     * or under the snapshot reads fine. */
    pc = 0;
    ax_y_get(prog, &pc, "stable:k");
    prog[pc++] = OP_EXIT;
    ret = yowza_execute(prog, pc);
    assert((intptr_t)ret > 0x1000);
    assert(yowza_get_response_len(ret) == 6 &&
           memcmp(yowza_get_response_ptr(ret), "stable", 6) == 0);
    /* Program 2 under the SAME sticky handle (a fresh
     * vm_init happened inside yowza_execute): the
     * overwritten key must NOT return the live value. */
    pc = 0;
    ax_y_get(prog, &pc, "base:k");
    prog[pc++] = OP_EXIT;
    ret = yowza_execute(prog, pc);
    assert((intptr_t)ret == -11);
    /* Program 3: a post-snapshot creation is likewise
     * invisible (fails -11, never returns "m"). */
    pc = 0;
    ax_y_get(prog, &pc, "main:only");
    prog[pc++] = OP_EXIT;
    ret = yowza_execute(prog, pc);
    assert((intptr_t)ret == -11);
    /* Program 4: timeline PUT stages only, drains, and
     * leaves the ART untouched. */
    pc = 0;
    ax_y_put(prog, &pc, "tl:z", "Z");
    ax_y_get(prog, &pc, "tl:z");
    prog[pc++] = OP_EXIT;
    ret = yowza_execute(prog, pc);
    assert((intptr_t)ret > 0x1000);
    assert(yowza_get_response_len(ret) == 1 &&
           memcmp(yowza_get_response_ptr(ret), "Z", 1) == 0);
    assert(yowza_timeline_write_count(ret) == 1);
    assert(art_find_terminal(&global_art, (const uint8_t*)"tl:z", 4) == NULL);
    {
        uint8_t sbuf[64];
        int n = yowza_timeline_write_serialize(ret, 0, sbuf, sizeof(sbuf));
        assert(n == 1 + 4 + 4 + 4 + 1);
        assert(sbuf[0] == 0); /* not a delete */
        assert(sbuf[1] == 4 && memcmp(&sbuf[5], "tl:z", 4) == 0);
        assert(sbuf[9] == 1 && sbuf[13] == 'Z');
        assert(yowza_timeline_write_serialize(ret, 1, sbuf, sizeof(sbuf)) == -1);
    }
    /* A future snapshot is rejected and leaves the
     * sticky handle unchanged. */
    assert(yowza_timeline_set(snap + 1000) == -1);
    assert(yowza_timeline_active() == 1);
    assert(yowza_timeline_snapshot() == snap);
    /* One-shot execution does not disturb the sticky
     * handle. */
    pc = 0;
    ax_y_get(prog, &pc, "base:k");
    prog[pc++] = OP_EXIT;
    ret = yowza_execute_timeline(prog, pc, snap);
    assert((intptr_t)ret == -11);
    assert(yowza_timeline_active() == 1);
    assert(yowza_timeline_snapshot() == snap);
    pc = 0;
    ax_y_get(prog, &pc, "base:k");
    prog[pc++] = OP_EXIT;
    ret = yowza_execute_timeline(prog, pc, snap + 1000);
    assert((intptr_t)ret == -3);
    /* Clearing restores the normal path: the live
     * value returns again. */
    assert(yowza_timeline_clear() == 0);
    assert(yowza_timeline_active() == 0);
    pc = 0;
    ax_y_get(prog, &pc, "base:k");
    prog[pc++] = OP_EXIT;
    ret = yowza_execute(prog, pc);
    assert((intptr_t)ret > 0x1000);
    assert(yowza_get_response_len(ret) == 3 &&
           memcmp(yowza_get_response_ptr(ret), "new", 3) == 0);
    if (global_storage.close) global_storage.close(&global_storage);
    printf("Passed.\n");
}

__attribute__((constructor)) static void test_axis_c_register(void) {
    test_axis_c_history_overflow();
    test_axis_c_multigroup_flush();
    test_axis_c_retention_lease();
    test_axis_c_paged_index();
    test_axis_c_yowza_timeline_glue();
}
