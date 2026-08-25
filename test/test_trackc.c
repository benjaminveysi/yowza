/* Track C tests — storage scale-up (see track-c-notes.md).
 *
 * Self-registers through a constructor (same pattern as
 * test_scan_cursor.c): test/main.c is owned by another
 * agent, so these tests run when the test_runner binary
 * starts, before main's suite.
 *
 *   test_trackc_paged_lookup_bounded — a 30000-entry
 *     segment (>256 blocks) looked up through the paged
 *     index: page loads bounded by log2(pages)+2, values
 *     exact, full view provably unavailable (blocks > 256).
 *   test_trackc_segdb_paged_lookup_and_prime — SegDB
 *     point lookup AND cursor prime over a ~1024-block
 *     segment: both use the paged path (counters prove
 *     it), rows exact.
 *   test_trackc_hier_levels — 5002 real segments in 40
 *     manifest groups through segdb_scan_fanin_hier:
 *     byte-identical to the reference model, stats prove
 *     multi-level traversal (groups == 40, sources ==
 *     5002, max_group == 39), flat fan-in still refuses
 *     the same DB with -1, and a tiny out buffer yields
 *     the -3 partial-page contract.
 *   test_trackc_multilevel_compact — multi-group DB
 *   test_trackc_heap_migrate — legacy 4096-slot heap
 *     compacted: merged view byte-identical before/after
 *     (FNV hash + row-for-row), every post-compact
 *     segment checksum-validated against its manifest
 *     meta, and the view survives recovery; a pinned
 *     lease defers the compact (group/segment shape
 *     unchanged while leased).
 */
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include "segdb.h"
#include "segdb_scan.h"
#include "segment.h"
#include "manifest.h"
#include "storage2.h"

static SegDB g_tc_db;
static SegDB g_tc_db2;
static uint8_t g_tc_arena[8 * 1024 * 1024];
static uint8_t g_tc_arena2[8 * 1024 * 1024];
/* 8 MiB WAL: the paged test puts 4096 x ~900-byte values
 * before its single flush, so the WAL must hold ~3.9 MB
 * of records at once. */
static uint8_t g_tc_wal[8 * 1024 * 1024];
static uint8_t g_tc_wal2[8 * 1024 * 1024];

/* FNV-1a-64 over a byte stream (test-side reference hash). */
static uint64_t tc_fnv(uint64_t h, const uint8_t* p, uint32_t n) {
    uint32_t i;
    for (i = 0; i < n; i++) {
        h ^= (uint64_t)p[i];
        h *= 1099511628211ULL;
    }
    return h;
}

static void tc_put(SegDB* db, const char* k, const uint8_t* v, uint32_t vlen) {
    int rc = segdb_put(db, (const uint8_t*)k, (uint32_t)strlen(k), v, vlen);
    assert(rc == 0);
}

static void tc_puts(SegDB* db, const char* k, const char* v) {
    tc_put(db, k, (const uint8_t*)v, (uint32_t)strlen(v));
}

static void tc_key(char* buf, uint32_t i) {
    /* Zero-padded: lexicographic order == numeric order. */
    snprintf(buf, 32, "h:%06u", i);
}

static void tc_val(char* buf, uint32_t i) {
    snprintf(buf, 32, "v:%u", i);
}

/* ---------- 1: paged point lookup on one large segment ---------- */

#define TC_BIG_ENTRIES 30000u
static SegEntry g_tc_entries[TC_BIG_ENTRIES];
static char g_tc_keys[TC_BIG_ENTRIES][16];
static char g_tc_vals[TC_BIG_ENTRIES][16];

void test_trackc_paged_lookup_bounded(void) {
    printf("Running test_trackc_paged_lookup_bounded...\n");
    fflush(stdout);
    {
        YowzaStore* st = yowza_store_mem_create();
        SegReader r;
        uint32_t i;
        uint64_t page_offs[1024];
        int np, nv;
        assert(st != NULL);
        for (i = 0; i < TC_BIG_ENTRIES; i++) {
            snprintf(g_tc_keys[i], sizeof(g_tc_keys[i]), "k:%06u", i);
            snprintf(g_tc_vals[i], sizeof(g_tc_vals[i]), "v:%u", i);
            g_tc_entries[i].key = (const uint8_t*)g_tc_keys[i];
            g_tc_entries[i].key_len = (uint32_t)strlen(g_tc_keys[i]);
            g_tc_entries[i].val = (const uint8_t*)g_tc_vals[i];
            g_tc_entries[i].val_len = (uint32_t)strlen(g_tc_vals[i]);
            g_tc_entries[i].is_tombstone = 0;
            g_tc_entries[i].commit_seq = (uint64_t)i + 1;
            g_tc_entries[i].crdt_ts = 0;
        }
        assert(seg_write(st, "bigseg", g_tc_entries, TC_BIG_ENTRIES,
                         0, 0, 0, NULL) == 0);
        assert(seg_open(st, "bigseg", &r) == 0);
        assert(seg_ensure_discovered(&r) == 0);
        printf("  big segment: entries=%u blocks=%u\n",
               r.entry_count, r.block_count);
        fflush(stdout);
        assert(r.block_count > SEG_INDEX_VIEW_MAX);
        /* Full view provably unavailable: it cannot hold
         * this many blocks. */
        nv = seg_index_view(&r, page_offs, SEG_INDEX_VIEW_MAX);
        assert(nv < 0);
        np = seg_index_view_paged(&r, page_offs, 1024, SEG_INDEX_PAGE_SIZE);
        assert(np > 0);
        assert((uint32_t)np ==
               (r.block_count + SEG_INDEX_PAGE_SIZE - 1) / SEG_INDEX_PAGE_SIZE);
        printf("  paged view: pages=%d (page size %u)\n", np, SEG_INDEX_PAGE_SIZE);
        fflush(stdout);
        r.idx_page_offs = page_offs;
        r.idx_page_count = (uint32_t)np;
        r.idx_page_size = SEG_INDEX_PAGE_SIZE;
        /* Lookups at several scales: first, middle, last,
         * and a missing key inside the range. Each must
         * touch at most log2(pages)+2 pages (binary-search
         * probes + the within-page scan page) and must
         * return the exact value (or absent for the miss). */
        {
            static const char* probes[] = {
                "k:000000", "k:007500", "k:015000", "k:022500", "k:029999",
                "k:015000a" /* missing, inside the range */
            };
            static const int want_found[] = { 1, 1, 1, 1, 1, 0 };
            uint32_t pi;
            for (pi = 0; pi < (uint32_t)(sizeof(probes) / sizeof(probes[0])); pi++) {
                SegEntry out;
                uint8_t vb[64];
                int lrc;
                uint64_t pages, entries;
                uint64_t bound;
                seg_lookup_index_entries_reset();
                lrc = seg_lookup(&r, (const uint8_t*)probes[pi],
                                 (uint32_t)strlen(probes[pi]),
                                 &out, vb, (uint32_t)sizeof(vb));
                pages = seg_lookup_paged_pages();
                entries = seg_lookup_index_entries();
                /* probes <= ceil(log2(pages)) + 1, plus the
                 * within-page scan page: bound log2+2. */
                bound = 2;
                {
                    uint32_t p = (uint32_t)np;
                    while (p > 1) { bound++; p = (p + 1) / 2; }
                }
                printf("  lookup %s: rc=%d pages=%llu entries=%llu bound=%llu\n",
                       probes[pi], lrc,
                       (unsigned long long)pages,
                       (unsigned long long)entries,
                       (unsigned long long)bound);
                fflush(stdout);
                if (want_found[pi]) {
                    char wantv[32];
                    uint32_t idx = 0;
                    assert(lrc == 0);
                    assert(sscanf(probes[pi], "k:%u", &idx) == 1);
                    snprintf(wantv, sizeof(wantv), "v:%u", idx);
                    assert(out.val_len == strlen(wantv));
                    assert(memcmp(vb, wantv, out.val_len) == 0);
                } else {
                    assert(lrc == 1);
                }
                assert(pages <= bound);
                assert(pages > 0);
                assert(entries <= pages + SEG_INDEX_PAGE_SIZE);
            }
        }
        seg_close(&r);
        yowza_store_close(st);
    }
    printf("Passed.\n"); fflush(stdout);
}

/* ---------- 2: SegDB point lookup + cursor prime, paged ---------- */

void test_trackc_segdb_paged_lookup_and_prime(void) {
    printf("Running test_trackc_segdb_paged_lookup_and_prime...\n");
    fflush(stdout);
    {
        YowzaStore* st = yowza_store_mem_create();
        static uint8_t bigval[900];
        static uint8_t out[65536];
        uint8_t nk[1024];
        uint32_t nk_len = 0, has_more = 0, cnt = 0;
        uint64_t snap;
        uint32_t i;
        int rc;
        assert(st != NULL);
        memset(bigval, 'x', sizeof(bigval));
        assert(segdb_init(&g_tc_db, st, g_tc_arena, sizeof(g_tc_arena),
                          g_tc_wal, sizeof(g_tc_wal)) == 0);
        /* 4096 keys x ~900-byte values: one flush segment
         * with ~1024 blocks (16 pages), far past the
         * 256-block full-view limit. */
        for (i = 0; i < 4096; i++) {
            char kb[32];
            snprintf(kb, sizeof(kb), "p:%06u", i);
            /* Distinct tail per key so values are checkable. */
            bigval[0] = (uint8_t)('A' + (i % 26));
            bigval[1] = (uint8_t)('0' + ((i / 26) % 10));
            tc_put(&g_tc_db, kb, bigval, (uint32_t)sizeof(bigval));
        }
        assert(segdb_flush(&g_tc_db) == 0);
        assert(g_tc_db.manifest.segment_count == 1);
        snap = segdb_snapshot_seq(&g_tc_db);
        /* Point lookups through segdb_get: paged path must
         * engage (pages > 0) and stay bounded. */
        {
            static const char* probes[] = { "p:000000", "p:002000", "p:004095" };
            uint32_t pi;
            for (pi = 0; pi < 3; pi++) {
                uint8_t* got = NULL;
                uint32_t got_len = 0;
                uint64_t pages, within;
                segdb_paged_stats_reset();
                seg_lookup_index_entries_reset();
                rc = segdb_get(&g_tc_db, (const uint8_t*)probes[pi],
                               (uint32_t)strlen(probes[pi]), &got, &got_len);
                segdb_paged_stats_get(&pages, &within);
                /* Note: segdb_get's paged counter is the
                 * cursor-prime counter; the lookup path
                 * counter lives in segment.c. Either way the
                 * segment-level paged counter must be > 0
                 * and bounded: 16 pages -> <= 5 probes +
                 * within page = 6. */
                printf("  get %s: rc=%d len=%u seg_paged_pages=%llu seg_idx_entries=%llu\n",
                       probes[pi], rc, got_len,
                       (unsigned long long)seg_lookup_paged_pages(),
                       (unsigned long long)seg_lookup_index_entries());
                fflush(stdout);
                assert(rc == 0);
                assert(got_len == sizeof(bigval));
                assert(seg_lookup_paged_pages() > 0);
                assert(seg_lookup_paged_pages() <= 6);
                (void)pages; (void)within;
            }
        }
        /* Cursor prime: seek to a cursor near the end of
         * the same segment. The paged seek must touch a
         * bounded number of pages, not ~1024 index entries. */
        segdb_paged_stats_reset();
        segdb_scan_stats_reset();
        rc = segdb_scan_cursor(&g_tc_db, (const uint8_t*)"", 0, snap,
                               (const uint8_t*)"p:004000", 8, 1, 10,
                               out, (uint32_t)sizeof(out), &cnt,
                               nk, (uint32_t)sizeof(nk), &nk_len, &has_more);
        {
            uint64_t pages, within;
            SegdbScanStats sst;
            segdb_paged_stats_get(&pages, &within);
            segdb_scan_stats_get(&sst);
            printf("  cursor prime: rc=%d cnt=%d has_more=%u prime_pages=%llu within=%llu seek_entries=%llu\n",
                   rc, cnt, has_more,
                   (unsigned long long)pages,
                   (unsigned long long)within,
                   (unsigned long long)sst.seek_index_entries);
            fflush(stdout);
            assert(rc == 0 && cnt == 10 && has_more == 1);
            /* 16 pages -> <= 4 probes + 1 within page = 5;
             * bound 6 leaves slack for the seek's first-page
             * handling. Linear would be ~1000 entries. */
            assert(pages <= 6);
            assert(pages > 0);
            assert(within <= SEG_INDEX_PAGE_SIZE);
            assert(sst.seek_index_entries <= 128);
            /* Rows are exactly p:004001..p:004010. */
            {
                uint32_t off = 0, j;
                for (j = 0; j < 10; j++) {
                    uint32_t kl, vl;
                    char ekb[32];
                    assert(off + 4 <= sizeof(out));
                    kl = (uint32_t)out[off] | ((uint32_t)out[off+1] << 8) |
                         ((uint32_t)out[off+2] << 16) | ((uint32_t)out[off+3] << 24);
                    off += 4;
                    snprintf(ekb, sizeof(ekb), "p:%06u", 4001 + j);
                    assert(kl == strlen(ekb));
                    assert(memcmp(&out[off], ekb, kl) == 0);
                    off += kl;
                    vl = (uint32_t)out[off] | ((uint32_t)out[off+1] << 8) |
                         ((uint32_t)out[off+2] << 16) | ((uint32_t)out[off+3] << 24);
                    off += 4;
                    assert(vl == sizeof(bigval));
                    off += vl;
                }
            }
            assert(nk_len == 8 && memcmp(nk, "p:004010", 8) == 0);
        }
        segdb_close(&g_tc_db);
        yowza_store_close(st);
    }
    printf("Passed.\n"); fflush(stdout);
}

/* ---------- hier scan helpers ---------- */

/* Page the whole hier result with limit-500 pages, calling
 * check(idx, key, key_len, val, val_len) per row in order.
 * Returns the row count. */
typedef void (*tc_row_cb)(uint32_t idx, const uint8_t* k, uint32_t kl,
                          const uint8_t* v, uint32_t vl);

static uint32_t tc_hier_collect(SegDB* db, uint64_t snap, tc_row_cb cb) {
    static uint8_t out[262144];
    uint8_t cur[1024];
    uint32_t cur_len = 0, has_more = 0, total = 0;
    int has_cur = 0;
    for (;;) {
        uint8_t nk[1024];
        uint32_t nk_len = 0, hm = 0, cnt = 0, off = 0, j;
        int rc = segdb_scan_fanin_hier(db, (const uint8_t*)"", 0, snap,
                                       cur, cur_len, has_cur, 5000,
                                       out, (uint32_t)sizeof(out), &cnt,
                                       nk, (uint32_t)sizeof(nk), &nk_len, &hm);
        assert(rc == 0);
        for (j = 0; j < cnt; j++) {
            uint32_t kl, vl;
            const uint8_t* kb;
            const uint8_t* vb;
            assert(off + 4 <= sizeof(out));
            kl = (uint32_t)out[off] | ((uint32_t)out[off+1] << 8) |
                 ((uint32_t)out[off+2] << 16) | ((uint32_t)out[off+3] << 24);
            off += 4;
            kb = &out[off]; off += kl;
            vl = (uint32_t)out[off] | ((uint32_t)out[off+1] << 8) |
                 ((uint32_t)out[off+2] << 16) | ((uint32_t)out[off+3] << 24);
            off += 4;
            vb = &out[off]; off += vl;
            if (cb) cb(total, kb, kl, vb, vl);
            total++;
        }
        has_more = hm;
        if (!has_more) break;
        assert(nk_len > 0 && nk_len <= sizeof(cur));
        memcpy(cur, nk, nk_len);
        cur_len = nk_len;
        has_cur = 1;
    }
    return total;
}

static uint64_t g_tc_hash;
static uint32_t g_tc_expect_idx;

static void tc_hash_row(uint32_t idx, const uint8_t* k, uint32_t kl,
                        const uint8_t* v, uint32_t vl) {
    (void)idx;
    g_tc_hash = tc_fnv(g_tc_hash, k, kl);
    g_tc_hash = tc_fnv(g_tc_hash, (const uint8_t*)"\0", 1);
    g_tc_hash = tc_fnv(g_tc_hash, v, vl);
    g_tc_hash = tc_fnv(g_tc_hash, (const uint8_t*)"\0", 1);
}

/* Reference model for the hier test: keys h:000000..h:004999
 * except h:000200..h:000209 (deleted), with h:000100..h:000109
 * overwritten to w:<i>. Rows arrive in key order. */
static void tc_model_row(uint32_t idx, const uint8_t* k, uint32_t kl,
                         const uint8_t* v, uint32_t vl) {
    uint32_t expect = g_tc_expect_idx;
    char ekb[32], evb[32];
    for (;;) {
        if (expect >= 5000) {
            assert(0 && "extra row past model");
            return;
        }
        if (expect >= 200 && expect <= 209) { expect++; continue; }
        break;
    }
    snprintf(ekb, sizeof(ekb), "h:%06u", expect);
    if (expect >= 100 && expect <= 109) snprintf(evb, sizeof(evb), "w:%u", expect);
    else snprintf(evb, sizeof(evb), "v:%u", expect);
    assert(kl == strlen(ekb) && memcmp(k, ekb, kl) == 0);
    assert(vl == strlen(evb) && memcmp(v, evb, vl) == 0);
    g_tc_expect_idx = expect + 1;
    (void)idx;
}

/* ---------- direct directory-mode construction ----------
 * Building 5002 segments through segdb_flush takes minutes
 * on the native memory store (flush cost grows with the
 * object count — a pre-existing store characteristic, see
 * track-c-notes.md), so these scale tests build the exact
 * same on-disk shape directly: real segments via seg_write,
 * real group manifests, a real YWMNDR directory, and a
 * CURRENT pointer, then segdb_recover. The recovered DB
 * state (has_dir, group_count, group manifests, segments)
 * is identical to a flush-built DB's; the flush-built path
 * itself is already covered by the Track A multi-level
 * test (130 flushes). Segment names use the tcseg. prefix
 * so compact's seg./segtmp. outputs can never collide
 * with them, whatever next_seg_id recovery computes. */

typedef struct {
    SegEntry entries[16];
    uint32_t count;
} TcSeg;

/* One segment's entries are filled by the caller into
 * ents[j]; keys/values point at caller-owned buffers that
 * must outlive the seg_write call only. */
static Manifest g_tc_manifest;
static char g_tc_segkeys[5200][16];
static char g_tc_segvals[5200][16];

static void tc_build_dir_db(YowzaStore* st, SegDB* db,
                            uint8_t* arena, uint32_t arena_size,
                            uint8_t* wal, uint32_t wal_size,
                            TcSeg* segs, uint32_t nsegs,
                            uint64_t checkpoint, uint32_t want_groups) {
    uint32_t j, g, ngroups;
    ngroups = (nsegs + 127) / 128;
    assert(ngroups == want_groups);
    for (j = 0; j < nsegs; j++) {
        char name[64];
        uint64_t bytes = 0, cks = 0;
        snprintf(name, sizeof(name), "tcseg.%u", j);
        assert(seg_write(st, name, segs[j].entries, segs[j].count,
                         0, 0, 0, &bytes) == 0);
        (void)bytes;
        (void)cks;
    }
    for (g = 0; g < ngroups; g++) {
        uint32_t cnt = nsegs - g * 128;
        uint32_t s;
        char final_name[64];
        if (cnt > 128) cnt = 128;
        memset(&g_tc_manifest, 0, sizeof(g_tc_manifest));
        g_tc_manifest.generation = g + 1;
        g_tc_manifest.checkpoint_seq = checkpoint;
        g_tc_manifest.schema_version = 0;
        g_tc_manifest.segment_count = cnt;
        for (s = 0; s < cnt; s++) {
            SegMeta* m = &g_tc_manifest.segments[s];
            const SegEntry* first = &segs[g * 128 + s].entries[0];
            const SegEntry* last =
                &segs[g * 128 + s].entries[segs[g * 128 + s].count - 1];
            char name[64];
            uint64_t bytes = 0, cks = 0;
            uint64_t min_seq = first->commit_seq, max_seq = first->commit_seq;
            uint32_t e;
            snprintf(name, sizeof(name), "tcseg.%u", g * 128 + s);
            for (e = 0; e < segs[g * 128 + s].count; e++) {
                if (segs[g * 128 + s].entries[e].commit_seq < min_seq)
                    min_seq = segs[g * 128 + s].entries[e].commit_seq;
                if (segs[g * 128 + s].entries[e].commit_seq > max_seq)
                    max_seq = segs[g * 128 + s].entries[e].commit_seq;
            }
            assert(seg_file_checksum(st, name, &cks) == 0);
            {
                uint64_t sz = 0;
                assert(st->vt->size(st->state, name, &sz) == 0);
                bytes = sz;
            }
            snprintf(m->obj, sizeof(m->obj), "%s", name);
            m->first_key_len = first->key_len;
            memcpy(m->first_key, first->key, first->key_len);
            m->last_key_len = last->key_len;
            memcpy(m->last_key, last->key, last->key_len);
            m->min_seq = min_seq;
            m->max_seq = max_seq;
            m->entry_count = segs[g * 128 + s].count;
            m->index_type = 0;
            m->model_version = 0;
            m->byte_size = bytes;
            m->checksum = cks;
        }
        snprintf(final_name, sizeof(final_name), "manifest.%u", g + 1);
        assert(manifest_write(st, "manifest.tmp", &g_tc_manifest) == 0);
        assert(manifest_publish(st, "manifest.tmp", final_name) == 0);
    }
    {
        ManifestDir dir;
        uint32_t gg;
        memset(&dir, 0, sizeof(dir));
        dir.generation = 1;
        dir.checkpoint_seq = checkpoint;
        dir.schema_version = 0;
        dir.group_count = ngroups;
        for (gg = 0; gg < ngroups; gg++)
            snprintf(dir.groups[gg], sizeof(dir.groups[gg]),
                     "manifest.%u", gg + 1);
        assert(manifest_dir_write(st, "manifest.dir.tmp", &dir) == 0);
        assert(manifest_publish(st, "manifest.dir.tmp", "manifest.dir") == 0);
    }
    assert(manifest_write_current(st, "current.tmp", "manifest.dir") == 0);
    assert(manifest_publish(st, "current.tmp", "CURRENT") == 0);
    assert(segdb_init(db, st, arena, arena_size, wal, wal_size) == 0);
    assert(segdb_recover(db) == 0);
    assert(db->has_dir == 1);
    assert(db->group_count == ngroups);
}

/* ---------- 3: hierarchical scan over 5002 segments ---------- */

static TcSeg g_tc_hier_segs[5002];

void test_trackc_hier_levels(void) {
    printf("Running test_trackc_hier_levels...\n");
    fflush(stdout);
    {
        YowzaStore* st = yowza_store_mem_create();
        uint64_t snap = 100000;
        uint32_t i, total;
        uint64_t groups, sources, max_group, windows;
        assert(st != NULL);
        /* 5000 single-entry base segments. */
        for (i = 0; i < 5000; i++) {
            tc_key(g_tc_segkeys[i], i);
            tc_val(g_tc_segvals[i], i);
            g_tc_hier_segs[i].count = 1;
            g_tc_hier_segs[i].entries[0].key =
                (const uint8_t*)g_tc_segkeys[i];
            g_tc_hier_segs[i].entries[0].key_len =
                (uint32_t)strlen(g_tc_segkeys[i]);
            g_tc_hier_segs[i].entries[0].val =
                (const uint8_t*)g_tc_segvals[i];
            g_tc_hier_segs[i].entries[0].val_len =
                (uint32_t)strlen(g_tc_segvals[i]);
            g_tc_hier_segs[i].entries[0].is_tombstone = 0;
            g_tc_hier_segs[i].entries[0].commit_seq = (uint64_t)i + 1;
            g_tc_hier_segs[i].entries[0].crdt_ts = 0;
        }
        /* One 10-entry overwrite segment (keys 100..109,
         * higher sequences win) and one 10-entry tombstone
         * segment (keys 200..209) in the last group. */
        g_tc_hier_segs[5000].count = 10;
        /* Overwrite/tombstone entries use their own static
         * key buffers (the segment-indexed key arrays only
         * cover the 5000 base segments). */
        {
            static char ow_keys[10][16];
            static char ow_vals[10][16];
            static char del_keys[10][16];
            uint32_t k;
            for (k = 0; k < 10; k++) {
                tc_key(ow_keys[k], 100 + k);
                snprintf(ow_vals[k], sizeof(ow_vals[k]), "w:%u", 100 + k);
                g_tc_hier_segs[5000].entries[k].key =
                    (const uint8_t*)ow_keys[k];
                g_tc_hier_segs[5000].entries[k].key_len =
                    (uint32_t)strlen(ow_keys[k]);
                g_tc_hier_segs[5000].entries[k].val =
                    (const uint8_t*)ow_vals[k];
                g_tc_hier_segs[5000].entries[k].val_len =
                    (uint32_t)strlen(ow_vals[k]);
                g_tc_hier_segs[5000].entries[k].is_tombstone = 0;
                g_tc_hier_segs[5000].entries[k].commit_seq = 6000 + k;
                g_tc_hier_segs[5000].entries[k].crdt_ts = 0;
                tc_key(del_keys[k], 200 + k);
                g_tc_hier_segs[5001].count = 10;
                g_tc_hier_segs[5001].entries[k].key =
                    (const uint8_t*)del_keys[k];
                g_tc_hier_segs[5001].entries[k].key_len =
                    (uint32_t)strlen(del_keys[k]);
                g_tc_hier_segs[5001].entries[k].val = NULL;
                g_tc_hier_segs[5001].entries[k].val_len = 0;
                g_tc_hier_segs[5001].entries[k].is_tombstone = 1;
                g_tc_hier_segs[5001].entries[k].commit_seq = 7000 + k;
                g_tc_hier_segs[5001].entries[k].crdt_ts = 0;
            }
        }
        tc_build_dir_db(st, &g_tc_db, g_tc_arena, sizeof(g_tc_arena),
                        g_tc_wal, sizeof(g_tc_wal),
                        g_tc_hier_segs, 5002, 8000, 40);
        (void)snap;
        /* Flat fan-in still refuses this DB: >4096 sources
         * in one flat call is -1 by contract. */
        {
            static uint8_t out[4096];
            uint8_t nk[1024];
            uint32_t nk_len = 0, hm = 0, cnt = 0;
            int rc = segdb_scan_fanin(&g_tc_db, (const uint8_t*)"", 0, snap,
                                      NULL, 0, 0, 10,
                                      out, (uint32_t)sizeof(out), &cnt,
                                      nk, (uint32_t)sizeof(nk), &nk_len, &hm);
            printf("  flat fanin on 5002 segments: rc=%d (want -1)\n", rc);
            fflush(stdout);
            assert(rc == -1);
        }
        /* Hierarchical scan, row-for-row against the model. */
        segdb_scan_fanin_hier_stats_reset();
        g_tc_expect_idx = 0;
        total = tc_hier_collect(&g_tc_db, snap, tc_model_row);
        printf("  hier rows=%u expect_idx=%u\n", total, g_tc_expect_idx);
        fflush(stdout);
        assert(total == 4990);
        assert(g_tc_expect_idx == 5000);
        /* Level-traversal proof: stats from the scan's pages
         * (last page call still enumerates every group). */
        segdb_scan_fanin_hier_stats_get(&groups, &sources, &max_group, &windows);
        printf("  hier stats: groups=%llu sources=%llu max_group=%llu windows=%llu\n",
               (unsigned long long)groups, (unsigned long long)sources,
               (unsigned long long)max_group, (unsigned long long)windows);
        fflush(stdout);
        assert(groups == 40);
        assert(sources == 5002);
        assert(max_group == 39);
        assert(windows > 0);
        /* Tiny out buffer: -3 partial page, rows valid,
         * resume with the next key. */
        {
            static uint8_t tiny[64];
            uint8_t nk[1024];
            uint32_t nk_len = 0, hm = 0, cnt = 0;
            int rc = segdb_scan_fanin_hier(&g_tc_db, (const uint8_t*)"", 0, snap,
                                           NULL, 0, 0, 100,
                                           tiny, (uint32_t)sizeof(tiny), &cnt,
                                           nk, (uint32_t)sizeof(nk), &nk_len, &hm);
            printf("  hier tiny-buffer: rc=%d cnt=%u has_more=%u\n", rc, cnt, hm);
            fflush(stdout);
            assert(rc == -3);
            assert(cnt > 0 && hm == 1 && nk_len > 0);
        }
        segdb_close(&g_tc_db);
        yowza_store_close(st);
    }
    printf("Passed.\n"); fflush(stdout);
}

/* ---------- 4: multi-level compaction, byte-identical view ---------- */

static void tc_validate_all_checksums(SegDB* db, YowzaStore* st) {
    uint32_t gi;
    assert(db->has_dir == 1);
    for (gi = 0; gi < db->group_count; gi++) {
        const Manifest* gm;
        uint32_t si;
        if (gi == db->group_count - 1) {
            gm = &db->manifest;
        } else {
            static Manifest gms;
            assert(manifest_read(st, db->group_objs[gi], &gms) == 0);
            gm = &gms;
        }
        for (si = 0; si < gm->segment_count; si++) {
            uint64_t cks = 0;
            assert(seg_file_checksum(st, gm->segments[si].obj, &cks) == 0);
            assert(cks == gm->segments[si].checksum);
        }
    }
}

void test_trackc_multilevel_compact(void) {
    printf("Running test_trackc_multilevel_compact...\n");
    fflush(stdout);
    {
        YowzaStore* st = yowza_store_mem_create();
        uint64_t snap, snap_old, hash_before, hash_after, hash_recovered;
        uint32_t i, total_before, total_after, total_recovered;
        uint32_t groups_before;
        uint64_t lease;
        assert(st != NULL);
        assert(segdb_init(&g_tc_db, st, g_tc_arena, sizeof(g_tc_arena),
                          g_tc_wal, sizeof(g_tc_wal)) == 0);
        /* 300 flushes: 3 groups (128+128+44), with an
         * overwrite + a delete to exercise winner/tombstone
         * handling through the compact. */
        for (i = 0; i < 300; i++) {
            char kb[32], vb[32];
            tc_key(kb, i);
            tc_val(vb, i);
            tc_puts(&g_tc_db, kb, vb);
            assert(segdb_flush(&g_tc_db) == 0);
        }
        /* Snapshot BEFORE the overwrite/delete flushes: a
         * lease at this older sequence is "stale" and must
         * pin (defer) compaction. */
        snap_old = segdb_snapshot_seq(&g_tc_db);
        for (i = 50; i <= 59; i++) {
            char kb[32], vb[32];
            tc_key(kb, i);
            snprintf(vb, sizeof(vb), "w:%u", i);
            tc_puts(&g_tc_db, kb, vb);
        }
        assert(segdb_flush(&g_tc_db) == 0);
        for (i = 100; i <= 109; i++) {
            char kb[32];
            tc_key(kb, i);
            assert(segdb_delete(&g_tc_db, (const uint8_t*)kb,
                                (uint32_t)strlen(kb)) == 0);
        }
        assert(segdb_flush(&g_tc_db) == 0);
        assert(g_tc_db.has_dir == 1);
        assert(g_tc_db.group_count == 3);
        groups_before = g_tc_db.group_count;
        snap = segdb_snapshot_seq(&g_tc_db);
        assert(snap_old < snap);
        g_tc_hash = 14695981039346656037ULL;
        total_before = tc_hier_collect(&g_tc_db, snap, tc_hash_row);
        hash_before = g_tc_hash;
        tc_validate_all_checksums(&g_tc_db, st);
        printf("  before compact: groups=%u rows=%u hash=%llx\n",
               groups_before, total_before, (unsigned long long)hash_before);
        fflush(stdout);
        assert(total_before == 290);
        /* A pinned (stale) lease defers the compact: shape unchanged. */
        lease = segdb_lease_acquire(&g_tc_db, snap_old);
        assert(lease != 0);
        assert(segdb_compact(&g_tc_db, 0) == 0);
        assert(g_tc_db.group_count == groups_before);
        assert(segdb_lease_release(&g_tc_db, lease) == 0);
        /* Real compact: merged view must be byte-identical. */
        assert(segdb_compact(&g_tc_db, 0) == 0);
        printf("  after compact: groups=%u segments=%u\n",
               g_tc_db.group_count, g_tc_db.manifest.segment_count);
        fflush(stdout);
        g_tc_hash = 14695981039346656037ULL;
        total_after = tc_hier_collect(&g_tc_db, snap, tc_hash_row);
        hash_after = g_tc_hash;
        printf("  after compact: rows=%u hash=%llx\n",
               total_after, (unsigned long long)hash_after);
        fflush(stdout);
        assert(total_after == total_before);
        assert(hash_after == hash_before);
        tc_validate_all_checksums(&g_tc_db, st);
        /* Spot point lookups still exact (overwrite, delete,
         * ordinary). */
        {
            uint8_t* out = NULL;
            uint32_t len = 0;
            assert(segdb_get(&g_tc_db, (const uint8_t*)"h:000055", 8, &out, &len) == 0);
            assert(len == 4 && memcmp(out, "w:55", 4) == 0);
            assert(segdb_get(&g_tc_db, (const uint8_t*)"h:000105", 8, &out, &len) != 0);
            assert(segdb_get(&g_tc_db, (const uint8_t*)"h:000299", 8, &out, &len) == 0);
            assert(len == 5 && memcmp(out, "v:299", 5) == 0);
        }
        /* Recovery preserves the compacted view. */
        segdb_close(&g_tc_db);
        assert(segdb_init(&g_tc_db2, st, g_tc_arena2, sizeof(g_tc_arena2),
                          g_tc_wal2, sizeof(g_tc_wal2)) == 0);
        assert(segdb_recover(&g_tc_db2) == 0);
        snap = segdb_snapshot_seq(&g_tc_db2);
        g_tc_hash = 14695981039346656037ULL;
        total_recovered = tc_hier_collect(&g_tc_db2, snap, tc_hash_row);
        hash_recovered = g_tc_hash;
        printf("  after recover: rows=%u hash=%llx\n",
               total_recovered, (unsigned long long)hash_recovered);
        fflush(stdout);
        assert(total_recovered == total_before);
        assert(hash_recovered == hash_before);
        tc_validate_all_checksums(&g_tc_db2, st);
        segdb_close(&g_tc_db2);
        yowza_store_close(st);
    }
    printf("Passed.\n"); fflush(stdout);
}

/* ---------- 5: legacy 4096-slot heap migrates in place ---------- */

/* yowza_segdb_init is defined in src/yowza_segdb.c without a
 * public header (it is a wasm export); declare it here for
 * the migration test. Layout constants mirror the heap
 * format: 32-byte header (magic "YWHEAP01", table_count u32
 * at +8, bump u64 at +16), 152-byte slots (name[128], offset
 * u64 at +128, len u64 at +136, cap u64 at +144). */
extern int yowza_segdb_init(SegDB* state, uint8_t* arena_buf,
                            uint64_t arena_size, uint8_t* wal_buf,
                            uint32_t wal_size, uint8_t* heap_buf,
                            uint64_t heap_size);

static void tc_wr64(uint8_t* p, uint64_t v) {
    int i;
    for (i = 0; i < 8; i++) p[i] = (uint8_t)((v >> (i * 8)) & 0xFF);
}

static void tc_wr32(uint8_t* p, uint32_t v) {
    p[0] = (uint8_t)(v & 0xFF);
    p[1] = (uint8_t)((v >> 8) & 0xFF);
    p[2] = (uint8_t)((v >> 16) & 0xFF);
    p[3] = (uint8_t)((v >> 24) & 0xFF);
}

static uint64_t tc_rd64(const uint8_t* p) {
    uint64_t v = 0;
    int i;
    for (i = 0; i < 8; i++) v |= (uint64_t)p[i] << (i * 8);
    return v;
}

static uint32_t tc_rd32(const uint8_t* p) {
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) |
           ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}

static void test_trackc_heap_migrate(void) {
    /* Legacy geometry: 32 + 4096*152 = 622624; new geometry:
     * 32 + 8192*152 = 1245216; shift = 622592. */
    static uint8_t heap[1245216 + 65536];
    static uint8_t arena2[8 * 1024 * 1024];
    static uint8_t wal2[8 * 1024 * 1024];
    static SegDB dbm;
    const uint64_t legacy_start = 32u + 4096u * 152u;
    const uint64_t new_start = 32u + 8192u * 152u;
    const uint64_t shift = new_start - legacy_start;
    const char* payload = "migration-payload-0123456789";
    uint32_t plen = (uint32_t)strlen(payload);
    uint8_t* slot;
    uint64_t bump;
    int rc;
    printf("Running test_trackc_heap_migrate...\n"); fflush(stdout);
    memset(heap, 0, sizeof(heap));
    memcpy(heap, "YWHEAP01", 8);
    tc_wr32(heap + 8, 4096);
    bump = legacy_start + plen;
    tc_wr64(heap + 16, bump);
    tc_wr64(heap + 24, 0);
    slot = heap + 32 + 0 * 152;
    memcpy(slot, "migseg.1", 8);
    tc_wr64(slot + 128, legacy_start);
    tc_wr64(slot + 136, plen);
    tc_wr64(slot + 144, plen);
    memcpy(heap + legacy_start, payload, plen);
    /* A second live slot in the middle of the legacy table,
     * plus an empty slot, to prove the shift is per-slot. */
    slot = heap + 32 + 2000 * 152;
    memcpy(slot, "migseg.2", 8);
    tc_wr64(slot + 128, legacy_start + plen);
    tc_wr64(slot + 136, 4);
    tc_wr64(slot + 144, 4);
    memcpy(heap + legacy_start + plen, "ABCD", 4);
    tc_wr64(heap + 16, legacy_start + plen + 4);

    rc = yowza_segdb_init(&dbm, arena2, sizeof(arena2),
                          wal2, (uint32_t)sizeof(wal2),
                          heap, (uint64_t)sizeof(heap));
    printf("  migrate init: rc=%d\n", rc); fflush(stdout);
    assert(rc == 0);
    /* New header + shifted slots + intact payload bytes. */
    assert(tc_rd32(heap + 8) == 8192);
    assert(tc_rd64(heap + 16) == legacy_start + plen + 4 + shift);
    slot = heap + 32 + 0 * 152;
    assert(memcmp(slot, "migseg.1", 8) == 0);
    assert(tc_rd64(slot + 128) == legacy_start + shift);
    assert(tc_rd64(slot + 136) == plen);
    assert(memcmp(heap + legacy_start + shift, payload, plen) == 0);
    slot = heap + 32 + 2000 * 152;
    assert(memcmp(slot, "migseg.2", 8) == 0);
    assert(tc_rd64(slot + 128) == legacy_start + plen + shift);
    assert(memcmp(heap + legacy_start + plen + shift, "ABCD", 4) == 0);
    /* New slots beyond the legacy table are zeroed. */
    slot = heap + 32 + 4096 * 152;
    assert(slot[0] == 0);
    slot = heap + 32 + 8191 * 152;
    assert(slot[0] == 0);
    printf("  migrated: table=8192 bump=%llu payload intact\n",
           (unsigned long long)tc_rd64(heap + 16)); fflush(stdout);

    /* No-room legacy heap: init fails loudly and leaves the
     * buffer untouched (never wipes a heap it cannot
     * migrate). */
    {
        static uint8_t small[700000];
        static uint8_t snapcopy[700000];
        static SegDB dbm2;
        memset(small, 0, sizeof(small));
        memcpy(small, "YWHEAP01", 8);
        tc_wr32(small + 8, 4096);
        /* Bump near the end: shifted bump exceeds the buffer. */
        tc_wr64(small + 16, 690000);
        slot = small + 32;
        memcpy(slot, "migseg.1", 8);
        tc_wr64(slot + 128, legacy_start);
        tc_wr64(slot + 136, 16);
        tc_wr64(slot + 144, 16);
        memcpy(snapcopy, small, sizeof(small));
        rc = yowza_segdb_init(&dbm2, arena2, sizeof(arena2),
                              wal2, (uint32_t)sizeof(wal2),
                              small, (uint64_t)sizeof(small));
        printf("  no-room init: rc=%d (want -1)\n", rc); fflush(stdout);
        assert(rc == -1);
        assert(memcmp(small, snapcopy, sizeof(small)) == 0);
    }
    printf("Passed.\n"); fflush(stdout);
}

/* Self-registration: see the ownership note at the top. */
__attribute__((constructor)) static void test_trackc_register(void) {
    test_trackc_paged_lookup_bounded();
    test_trackc_segdb_paged_lookup_and_prime();
    test_trackc_hier_levels();
    test_trackc_multilevel_compact();
    test_trackc_heap_migrate();
}
