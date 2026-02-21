/* Gate 3 — fan-in-bounded scan. See include/segdb_scan.h for the
 * design and the return-code contract. NEW functions only: nothing
 * in segdb.c is modified, and the existing scan_page / scan_cursor
 * paths are untouched.
 *
 * No libc, no malloc: only manual byte loops and static scratch,
 * so this builds for both wasm targets as well as natively.
 * Single-threaded invariant (as in segdb.c): the static scratch
 * is shared by all SegDBs; the host serializes callers. */
#include "segdb_scan.h"

/* ---- manual byte helpers ---- */
static void fn_memcpy(uint8_t* dst, const uint8_t* src, uint32_t len) {
    uint32_t i;
    if (!dst) return;
    for (i = 0; i < len; i++) dst[i] = src ? src[i] : 0;
}

static int fn_memcmp(const uint8_t* a, const uint8_t* b, uint32_t len) {
    uint32_t i;
    for (i = 0; i < len; i++) {
        if (a[i] != b[i]) return (int)a[i] - (int)b[i];
    }
    return 0;
}

static int fn_key_cmp(const uint8_t* a, uint32_t a_len, const uint8_t* b, uint32_t b_len) {
    uint32_t m = a_len < b_len ? a_len : b_len;
    int c = fn_memcmp(a, b, m);
    if (c != 0) return c;
    if (a_len < b_len) return -1;
    if (a_len > b_len) return 1;
    return 0;
}

static int fn_prefix_match(const uint8_t* key, uint32_t key_len,
                           const uint8_t* prefix, uint32_t prefix_len) {
    if (prefix_len == 0) return 1;
    if (key_len < prefix_len) return 0;
    return fn_memcmp(key, prefix, prefix_len) == 0;
}

/* 1 if key is strictly beyond the prefix range (sorted order
 * guarantees no later key can match), else 0. */
static int fn_prefix_beyond(const uint8_t* key, uint32_t key_len,
                            const uint8_t* prefix, uint32_t prefix_len) {
    int c;
    if (prefix_len == 0) return 0;
    if (key_len < prefix_len) {
        c = fn_memcmp(key, prefix, key_len);
        if (c == 0) return 0; /* key is a proper prefix of prefix */
        return c > 0;
    }
    c = fn_memcmp(key, prefix, prefix_len);
    return c > 0;
}

static uint32_t fn_strlen(const char* s) {
    uint32_t n = 0;
    while (s[n] != '\0') n++;
    return n;
}

static uint32_t fn_rd32(const uint8_t* p) {
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) |
           ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}

static uint64_t fn_rd64(const uint8_t* p) {
    uint64_t v = 0;
    int i;
    for (i = 0; i < 8; i++) v |= (uint64_t)p[i] << (i * 8);
    return v;
}

/* ---- stats ---- */
static uint64_t g_fn_stat_sources = 0;
static uint64_t g_fn_stat_windows = 0;
static uint64_t g_fn_stat_seeks = 0;
static uint64_t g_fn_stat_entries = 0;

void segdb_scan_fanin_stats_reset(void) {
    g_fn_stat_sources = 0;
    g_fn_stat_windows = 0;
    g_fn_stat_seeks = 0;
    g_fn_stat_entries = 0;
}

void segdb_scan_fanin_stats_get(uint64_t* sources_out, uint64_t* windows_out,
                                uint64_t* seeks_out, uint64_t* entries_out) {
    if (sources_out) *sources_out = g_fn_stat_sources;
    if (windows_out) *windows_out = g_fn_stat_windows;
    if (seeks_out) *seeks_out = g_fn_stat_seeks;
    if (entries_out) *entries_out = g_fn_stat_entries;
}

/* ---- source metadata (structural, no row data) ---- */
typedef struct {
    uint32_t name_off, name_len;
    uint32_t first_off, first_len;
    uint32_t last_off, last_len;
    uint64_t index_offset, index_len;
    uint32_t block_count, entry_count;
    uint64_t min_seq, max_seq;
    uint64_t idx_pos; /* resume position in the sparse index */
    int done;         /* last key at/below the current window bound */
    int skipped;      /* contributes nothing at this snapshot/prefix */
    /* O3 cache identity: seg_open-validated total object length
     * and the manifest whole-file checksum (SegMeta.checksum).
     * Skipped sources are never opened: total_len is 0 for them
     * and only the manifest fields identify them. */
    uint64_t total_len;
    uint64_t file_checksum;
} FnSrc;

#define FN_NAMES_ARENA 524288u
static FnSrc g_fn_srcs[SEGDB_FANIN_MAX_SOURCES];
static uint32_t g_fn_nsrc = 0;
static uint8_t g_fn_names[FN_NAMES_ARENA];
static uint32_t g_fn_names_off = 0;
static Manifest g_fn_group; /* group-manifest scratch for enumeration */

/* Copy bytes into the names arena; returns offset, or -1 on overflow. */
static int fn_arena_copy(const uint8_t* data, uint32_t len) {
    uint32_t off;
    if (g_fn_names_off + len + 1 > FN_NAMES_ARENA) return -1;
    off = g_fn_names_off;
    fn_memcpy(&g_fn_names[off], data, len);
    g_fn_names[off + len] = 0;
    g_fn_names_off += len + 1;
    return (int)off;
}

/* ---- readers ---- */
static SegReader g_fn_reader;        /* window iteration reader */
static uint8_t g_fn_kb[1024];
static uint8_t g_fn_vb[1024];
static uint8_t g_fn_last_key[1024];
static uint32_t g_fn_last_key_len = 0;

/* Prepare a reader for a source whose sparse index was already
 * verified at enumeration (seg_ensure_discovered there checks the
 * index checksum against the footer). Bloom is left disabled:
 * iteration never consults it (it is only a pre-filter for
 * seg_lookup, which this file no longer uses). */
static void fn_reader_prime(SegReader* r, FnSrc* s) {
    r->discovered = 1;
    r->index_offset = s->index_offset;
    r->index_len = s->index_len;
    r->block_count = s->block_count;
    r->bloom_offset = 0;
    r->bloom_bytes = 0;
    r->iter_index_pos = s->index_offset;
    r->iter_index_end = s->index_offset + s->index_len;
    r->iter_in_block = 0;
    r->iter_returned = 0;
    r->iter_block_offset = 0;
    r->iter_block_len = 0;
    r->iter_entry_pos = 0;
    r->iter_block_end = 0;
}

/* Enumerate one segment into the source table. The segment is
 * opened and fully discovered here (one pass, verifying the sparse
 * index checksum); later seeks reuse the verified offsets. */
static int fn_add_source(SegDB* db, const SegMeta* meta,
                         const uint8_t* prefix, uint32_t prefix_len,
                         uint64_t snapshot_seq) {
    FnSrc* s;
    const char* obj;
    int noff, foff, loff;
    if (g_fn_nsrc >= SEGDB_FANIN_MAX_SOURCES) return -1;
    obj = meta->obj;
    noff = fn_arena_copy((const uint8_t*)obj, fn_strlen(obj));
    if (noff < 0) return -1;
    foff = fn_arena_copy(meta->first_key, meta->first_key_len);
    if (foff < 0) return -1;
    loff = fn_arena_copy(meta->last_key, meta->last_key_len);
    if (loff < 0) return -1;
    s = &g_fn_srcs[g_fn_nsrc];
    s->name_off = (uint32_t)noff;
    s->name_len = fn_strlen(obj);
    s->first_off = (uint32_t)foff;
    s->first_len = meta->first_key_len;
    s->last_off = (uint32_t)loff;
    s->last_len = meta->last_key_len;
    s->min_seq = meta->min_seq;
    s->max_seq = meta->max_seq;
    s->entry_count = meta->entry_count;
    s->idx_pos = 0;
    s->done = 0;
    s->skipped = 0;
    s->total_len = 0;
    s->file_checksum = meta->checksum;
    /* Prove this source can contribute nothing, and skip it for
     * the whole call: empty, all versions newer than the snapshot,
     * or its key range disjoint from the prefix range. */
    if (meta->entry_count == 0 || meta->min_seq > snapshot_seq) {
        s->skipped = 1;
    } else if (prefix_len > 0) {
        if (fn_key_cmp(meta->last_key, meta->last_key_len, prefix, prefix_len) < 0 ||
            fn_prefix_beyond(meta->first_key, meta->first_key_len, prefix, prefix_len)) {
            s->skipped = 1;
        }
    }
    if (!s->skipped) {
        /* Open + discover: verifies header/footer and the sparse
         * index checksum, so later seeks can trust the offsets. */
        if (seg_open(db->store, obj, &g_fn_reader) != 0) return -1;
        if (seg_ensure_discovered(&g_fn_reader) != 0) return -1;
        s->index_offset = g_fn_reader.index_offset;
        s->index_len = g_fn_reader.index_len;
        s->block_count = g_fn_reader.block_count;
        s->total_len = g_fn_reader.total_len;
        s->idx_pos = s->index_offset;
    } else {
        s->index_offset = 0;
        s->index_len = 0;
        s->block_count = 0;
        s->idx_pos = 0;
    }
    g_fn_nsrc++;
    return 0;
}

static int fn_enumerate(SegDB* db, const uint8_t* prefix, uint32_t prefix_len,
                        uint64_t snapshot_seq) {
    uint32_t gi, si;
    g_fn_nsrc = 0;
    g_fn_names_off = 0;
    if (db->has_dir) {
        uint32_t n_groups = db->group_count;
        for (gi = 0; gi < n_groups; gi++) {
            const Manifest* gm;
            if (gi == n_groups - 1) {
                gm = &db->manifest;
            } else {
                if (manifest_read(db->store, db->group_objs[gi], &g_fn_group) != 0) return -1;
                gm = &g_fn_group;
            }
            for (si = 0; si < gm->segment_count; si++) {
                if (fn_add_source(db, &gm->segments[si], prefix, prefix_len, snapshot_seq) != 0)
                    return -1;
            }
        }
    } else {
        for (si = 0; si < db->manifest.segment_count; si++) {
            if (fn_add_source(db, &db->manifest.segments[si], prefix, prefix_len, snapshot_seq) != 0)
                return -1;
        }
    }
    g_fn_stat_sources += g_fn_nsrc;
    return 0;
}

/* ---- hot candidates (collected once per call, sorted) ---- */
#define FN_HOT_MAX 8192u
typedef struct {
    const uint8_t* key;
    uint32_t key_len;
    const uint8_t* val;
    uint32_t val_len;
    uint64_t seq;
    int tomb;
} FnHot;

static FnHot g_fn_hot[FN_HOT_MAX];
static uint32_t g_fn_hot_count = 0;
static int g_fn_hot_overflow = 0;

/* Hot keys MUST be copied: art_iterate reconstructs each key in a
 * shared traversal buffer, so the key pointer handed to the callback
 * is stale by the next visit (observed: all 7 hot entries reporting
 * the last-inserted key). Values are arena pointers and stable, so
 * they are kept by reference, exactly as the cursor path does. The
 * copy arena mirrors the cursor path's SEGDB_SCAN_DATA (256 KiB);
 * a hot key set larger than that fails the scan with a typed
 * error (never silent truncation) and the caller flushes to
 * shrink hot. */
#define FN_HOT_KEY_ARENA 262144u
static uint8_t g_fn_hot_keys[FN_HOT_KEY_ARENA];
static uint32_t g_fn_hot_keys_off = 0;

typedef struct {
    const uint8_t* prefix;
    uint32_t prefix_len;
    uint64_t seq_base;
} FnHotCtx;

static int fn_hot_cb(void* vctx, const uint8_t* key, uint32_t key_len,
                     const uint8_t* val, uint32_t val_len,
                     int is_tombstone, uint64_t timestamp, uint64_t version) {
    FnHotCtx* c = (FnHotCtx*)vctx;
    FnHot* h;
    (void)timestamp;
    if (!fn_prefix_match(key, key_len, c->prefix, c->prefix_len)) {
        /* Still collect: the window merge applies the prefix filter
         * per window, and collecting first keeps the sort total.
         * (Prefix pruning for hot is a per-window filter, not a
         * collection filter, because hot is small.) */
    }
    if (g_fn_hot_count >= FN_HOT_MAX) {
        g_fn_hot_overflow = 1;
        return 1; /* stop iteration */
    }
    if (g_fn_hot_keys_off + key_len > FN_HOT_KEY_ARENA) {
        g_fn_hot_overflow = 1;
        return 1; /* stop iteration: fail closed, never truncate */
    }
    h = &g_fn_hot[g_fn_hot_count++];
    fn_memcpy(&g_fn_hot_keys[g_fn_hot_keys_off], key, key_len);
    h->key = &g_fn_hot_keys[g_fn_hot_keys_off];
    g_fn_hot_keys_off += key_len;
    h->key_len = key_len;
    h->val = val;
    h->val_len = val_len;
    h->seq = c->seq_base + version;
    h->tomb = is_tombstone;
    return 0;
}

/* Shell sort by key (hot is bounded; no libc qsort in wasm). */
static void fn_hot_sort(void) {
    uint32_t gap, i, j;
    static const uint32_t gaps[] = { 701, 301, 132, 57, 23, 10, 4, 1 };
    uint32_t gi;
    for (gi = 0; gi < (uint32_t)(sizeof(gaps) / sizeof(gaps[0])); gi++) {
        gap = gaps[gi];
        for (i = gap; i < g_fn_hot_count; i++) {
            FnHot tmp = g_fn_hot[i];
            j = i;
            while (j >= gap &&
                   fn_key_cmp(g_fn_hot[j - gap].key, g_fn_hot[j - gap].key_len,
                              tmp.key, tmp.key_len) > 0) {
                g_fn_hot[j] = g_fn_hot[j - gap];
                j -= gap;
            }
            g_fn_hot[j] = tmp;
        }
    }
}

/* First hot index with key strictly greater than bound (or
 * count when none). bound == NULL means index 0. */
static uint32_t fn_hot_first_gt(const uint8_t* bound, uint32_t bound_len, int has_bound) {
    uint32_t lo = 0, hi = g_fn_hot_count;
    if (!has_bound) return 0;
    while (lo < hi) {
        uint32_t mid = lo + (hi - lo) / 2;
        if (fn_key_cmp(g_fn_hot[mid].key, g_fn_hot[mid].key_len, bound, bound_len) <= 0)
            lo = mid + 1;
        else
            hi = mid;
    }
    return lo;
}

/* ---- window winner map (sorted by key; values fetched in a
 * second phase, see fn_fetch_window_values) ---- */
#define FN_KEY_ARENA 1048576u
#define FN_VAL_ARENA 262144u /* provably sufficient: at most
 * SEGDB_FANIN_WINDOW_KEYS winners, each value <= 1024 bytes
 * (the segdb_put cap), so 256 * 1024 = 262144 exactly. */
typedef struct {
    uint32_t key_off, key_len, key_cap;
    uint64_t seq;
    uint32_t src; /* index into g_fn_srcs (publication order) */
    int tomb;
    uint32_t val_off, val_len; /* into g_fn_vals; val_off =
     * 0xFFFFFFFF when unset (tombstones carry no value) */
} FnWin;

static FnWin g_fn_win[SEGDB_FANIN_WINDOW_KEYS];
static uint32_t g_fn_win_count = 0;
static uint8_t g_fn_keys[FN_KEY_ARENA];
static uint32_t g_fn_keys_off = 0;
static uint8_t g_fn_vals[FN_VAL_ARENA];
static uint32_t g_fn_vals_off = 0;

/* Binary search the window map: returns index if found, else
 * -(insertion point) - 1. */
static int fn_win_find(const uint8_t* key, uint32_t key_len) {
    uint32_t lo = 0, hi = g_fn_win_count;
    while (lo < hi) {
        uint32_t mid = lo + (hi - lo) / 2;
        FnWin* e = &g_fn_win[mid];
        int c = fn_key_cmp(&g_fn_keys[e->key_off], e->key_len, key, key_len);
        if (c == 0) return (int)mid;
        if (c < 0) lo = mid + 1;
        else hi = mid;
    }
    return -((int)lo) - 1;
}

/* Insert or update a segment winner.
 * Returns 0 inserted/updated, 1 caller must stop this source
 * (key larger than the full map's maximum), -1 key-arena overflow. */
static int fn_win_insert(const uint8_t* key, uint32_t key_len, uint64_t seq,
                         uint32_t src, int tomb) {
    int f = fn_win_find(key, key_len);
    uint32_t pos, i;
    FnWin ne;
    if (f >= 0) {
        FnWin* e = &g_fn_win[(uint32_t)f];
        if (seq > e->seq || (seq == e->seq && src > e->src)) {
            e->seq = seq;
            e->src = src;
            e->tomb = tomb;
        }
        return 0;
    }
    pos = (uint32_t)(-f - 1);
    if (g_fn_win_count < SEGDB_FANIN_WINDOW_KEYS) {
        if (g_fn_keys_off + key_len + 1 > FN_KEY_ARENA) return -1;
        ne.key_off = g_fn_keys_off;
        ne.key_len = key_len;
        ne.key_cap = key_len;
        fn_memcpy(&g_fn_keys[ne.key_off], key, key_len);
        g_fn_keys[ne.key_off + key_len] = 0;
        g_fn_keys_off += key_len + 1;
        ne.seq = seq;
        ne.src = src;
        ne.tomb = tomb;
        ne.val_off = 0xFFFFFFFFu;
        ne.val_len = 0;
        for (i = g_fn_win_count; i > pos; i--) g_fn_win[i] = g_fn_win[i - 1];
        g_fn_win[pos] = ne;
        g_fn_win_count++;
        return 0;
    }
    /* Map full: a key larger than the maximum cannot belong to
     * this window — the source is sorted, so it stops here. */
    if (fn_key_cmp(key, key_len,
                   &g_fn_keys[g_fn_win[g_fn_win_count - 1].key_off],
                   g_fn_win[g_fn_win_count - 1].key_len) > 0) {
        return 1;
    }
    /* Evict the maximum; reuse its key buffer when it fits. */
    {
        FnWin ev = g_fn_win[g_fn_win_count - 1];
        if (key_len <= ev.key_cap) {
            ne.key_off = ev.key_off;
            ne.key_cap = ev.key_cap;
        } else {
            if (g_fn_keys_off + key_len + 1 > FN_KEY_ARENA) return -1;
            ne.key_off = g_fn_keys_off;
            ne.key_cap = key_len;
            g_fn_keys_off += key_len + 1;
        }
        fn_memcpy(&g_fn_keys[ne.key_off], key, key_len);
        g_fn_keys[ne.key_off + key_len] = 0;
        ne.key_len = key_len;
        ne.seq = seq;
        ne.src = src;
        ne.tomb = tomb;
        ne.val_off = 0xFFFFFFFFu;
        ne.val_len = 0;
        for (i = g_fn_win_count - 1; i > pos; i--) g_fn_win[i] = g_fn_win[i - 1];
        g_fn_win[pos] = ne;
        return 0;
    }
}

/* ---- per-source seek + window slice ---- */
/* Read one sparse-index entry at pos: returns 0 and fills
 * key (into kb, capped at 1024), klen, boff, blen, next_pos;
 * -1 on any read/malformed entry. */
static int fn_index_entry(SegDB* db, const char* obj, uint64_t pos,
                          uint8_t* kb, uint32_t* klen_out,
                          uint64_t* boff_out, uint64_t* blen_out,
                          uint64_t* next_out) {
    uint8_t b4[4], b8[8];
    uint32_t klen;
    if (db->store->vt->read_at(db->store->state, obj, pos, b4, 4) != 0) return -1;
    klen = fn_rd32(b4);
    if (klen == 0 || klen > 1024) return -1;
    if (db->store->vt->read_at(db->store->state, obj, pos + 4, kb, klen) != 0) return -1;
    if (db->store->vt->read_at(db->store->state, obj, pos + 4 + klen, b8, 8) != 0) return -1;
    *boff_out = fn_rd64(b8);
    if (db->store->vt->read_at(db->store->state, obj, pos + 4 + klen + 8, b8, 8) != 0) return -1;
    *blen_out = fn_rd64(b8);
    *klen_out = klen;
    *next_out = pos + 4 + klen + 16;
    return 0;
}

/* Position the iteration reader at the block that can contain
 * keys > lo (last block whose first key <= lo, or the first block
 * when there is no bound / the bound precedes the segment).
 * The source's idx_pos resumes from the previous window's chosen
 * block, so index scanning only moves forward across windows.
 * Returns 0, or -1 on a malformed index. */
static int fn_seek(SegDB* db, FnSrc* s, const uint8_t* lo, uint32_t lo_len, int has_lo) {
    const char* obj = (const char*)&g_fn_names[s->name_off];
    uint64_t pos = s->idx_pos;
    uint64_t end = s->index_offset + s->index_len;
    uint64_t chosen_pos = s->index_offset;
    int have_chosen = 0;
    if (!has_lo) {
        /* First block: idx_pos already points at it on the first
         * window; on later windows lo is always set. */
        chosen_pos = s->index_offset;
        have_chosen = 1;
    } else {
        uint8_t kb[1024];
        while (pos < end) {
            uint32_t klen = 0;
            uint64_t boff = 0, blen = 0, next = 0;
            int cmp;
            if (fn_index_entry(db, obj, pos, kb, &klen, &boff, &blen, &next) != 0) return -1;
            cmp = fn_key_cmp(kb, klen, lo, lo_len);
            if (cmp <= 0) {
                chosen_pos = pos;
                have_chosen = 1;
                pos = next;
            } else {
                break;
            }
        }
        if (!have_chosen) {
            /* Bound precedes the segment: start at the first block. */
            chosen_pos = s->index_offset;
            have_chosen = 1;
        }
    }
    if (!have_chosen) return -1;
    s->idx_pos = chosen_pos;
    /* Open a fresh reader and point its iterator at the chosen block. */
    if (seg_open(db->store, obj, &g_fn_reader) != 0) return -1;
    fn_reader_prime(&g_fn_reader, s);
    g_fn_reader.iter_index_pos = chosen_pos;
    g_fn_stat_seeks++;
    return 0;
}

/* Collect one window: all segment winners for distinct keys
 * strictly greater than lo (when has_lo), up to WINDOW_KEYS keys.
 * Sets *window_final = 1 iff no source stopped on a full map
 * (i.e. every source was consumed for this window, so there are
 * no more keys after it). Returns 0, or -1 on error. */
static int fn_collect_window(SegDB* db, const uint8_t* prefix, uint32_t prefix_len,
                             uint64_t snapshot_seq,
                             const uint8_t* lo, uint32_t lo_len, int has_lo,
                             int* window_final) {
    uint32_t si;
    int cap_stop = 0;
    g_fn_win_count = 0;
    g_fn_keys_off = 0;
    g_fn_vals_off = 0;
    *window_final = 0;
    for (si = 0; si < g_fn_nsrc; si++) {
        FnSrc* s = &g_fn_srcs[si];
        if (s->done || s->skipped) continue;
        if (has_lo && fn_key_cmp(&g_fn_names[s->last_off], s->last_len, lo, lo_len) <= 0) {
            s->done = 1; /* every key of this source is at/below lo */
            continue;
        }
        if (fn_seek(db, s, lo, lo_len, has_lo) != 0) return -1;
        for (;;) {
            SegEntry ent;
            int rc = seg_iter_next(&g_fn_reader, &ent, g_fn_kb, (uint32_t)sizeof(g_fn_kb),
                                   g_fn_vb, (uint32_t)sizeof(g_fn_vb));
            if (rc == 1) break; /* naturally exhausted */
            if (rc == -1) {
                /* Mid-segment seek leaves iter_returned below
                 * entry_count, so exhaustion surfaces as -1 when
                 * the index is exhausted and no block is open;
                 * any other -1 is a real error, never a silent
                 * truncation. Every block seg_iter_next loaded was
                 * checksum-verified before its entries were
                 * returned. */
                if (g_fn_reader.iter_index_pos >= g_fn_reader.iter_index_end &&
                    !g_fn_reader.iter_in_block) {
                    break;
                }
                return -1;
            }
            g_fn_stat_entries++;
            if (has_lo && fn_key_cmp(ent.key, ent.key_len, lo, lo_len) <= 0) continue;
            if (fn_prefix_beyond(ent.key, ent.key_len, prefix, prefix_len)) break;
            if (!fn_prefix_match(ent.key, ent.key_len, prefix, prefix_len)) continue;
            if (ent.commit_seq > snapshot_seq) continue;
            {
                int ins = fn_win_insert(ent.key, ent.key_len, ent.commit_seq, si,
                                        ent.is_tombstone);
                if (ins < 0) return -1;
                if (ins == 1) {
                    cap_stop = 1;
                    break;
                }
            }
        }
    }
    *window_final = cap_stop ? 0 : 1;
    g_fn_stat_windows++;
    return 0;
}

/* ---- second phase: fetch winner values by forward streaming ----
 * Phase 1 (fn_collect_window) records winner KEYS only. Fetching
 * each value with seg_lookup would re-scan the sparse index
 * linearly and re-verify a whole block's checksum per row — for a
 * 900,000-row scan that was 900k linear index scans plus ~2 GB of
 * repeated block hashing in wasm (first 900k run: >11 min on the
 * first scan alone). Instead, each source with winners in this
 * window is streamed forward exactly once, and every winner value
 * is copied into g_fn_vals as its entry streams past. Blocks are
 * therefore loaded (and checksum-verified) once per window per
 * source, not once per row, and the value arena is provably
 * sufficient (at most WINDOW_KEYS winners × 1024 bytes).
 * Matching rules (fail closed, never a wrong value): streamed
 * entries are first filtered exactly as phase 1 filtered them —
 * at/below the window bound, prefix-non-matching, or newer than
 * the snapshot — and skipped. A remaining entry below the
 * current winner key is skipped iff its key is in the window map
 * (a winner owned by another source, an already-fetched winner,
 * or a tombstone winner); otherwise it is an inconsistency and
 * fails. An entry matching the winner key must carry exactly the
 * winner sequence; anything else fails. A winner its own source's
 * stream never produces fails. Tombstone winners carry no value
 * and are not fetched. Returns 0, or -1 on any inconsistency or
 * arena overflow (overflow is unreachable for values <= 1024,
 * the segdb_put cap, since the arena holds WINDOW_KEYS
 * worst-case values exactly). */
static int fn_fetch_window_values(SegDB* db, const uint8_t* prefix, uint32_t prefix_len,
                                  uint64_t snapshot_seq,
                                  const uint8_t* lo, uint32_t lo_len, int has_lo) {
    uint32_t si;
    for (si = 0; si < g_fn_nsrc; si++) {
        uint32_t wj;
        FnSrc* s;
        /* First unfetched winner of this source, if any. */
        for (wj = 0; wj < g_fn_win_count; wj++) {
            if (g_fn_win[wj].src == si && !g_fn_win[wj].tomb) break;
        }
        if (wj >= g_fn_win_count) continue;
        s = &g_fn_srcs[si];
        /* Seek to the first winner key. fn_seek scans forward from
         * the source's resume position and falls back to the first
         * block when the bound precedes it, so this is correct
         * regardless of where phase 1 left idx_pos. */
        if (fn_seek(db, s, &g_fn_keys[g_fn_win[wj].key_off],
                    g_fn_win[wj].key_len, 1) != 0) return -1;
        for (;;) {
            SegEntry ent;
            int rc, c;
            /* Advance past winners of other sources; live winners
             * of this source are matched in key order below. */
            while (wj < g_fn_win_count &&
                   (g_fn_win[wj].src != si || g_fn_win[wj].tomb)) wj++;
            if (wj >= g_fn_win_count) break;
            rc = seg_iter_next(&g_fn_reader, &ent, g_fn_kb,
                               (uint32_t)sizeof(g_fn_kb),
                               g_fn_vb, (uint32_t)sizeof(g_fn_vb));
            if (rc == 1) return -1; /* winner missing: inconsistency */
            if (rc == -1) {
                if (g_fn_reader.iter_index_pos >= g_fn_reader.iter_index_end &&
                    !g_fn_reader.iter_in_block) return -1;
                return -1;
            }
            g_fn_stat_entries++;
            /* Phase-1 skip rules, in the same order: entries at
             * or below the window bound (re-streaming from the
             * starting block meets these), prefix-non-matching
             * entries, and versions newer than the snapshot were
             * never inserted into the map, so they are skipped
             * here rather than treated as inconsistencies. */
            if (has_lo && fn_key_cmp(ent.key, ent.key_len, lo, lo_len) <= 0) continue;
            if (!fn_prefix_match(ent.key, ent.key_len, prefix, prefix_len)) continue;
            if (ent.commit_seq > snapshot_seq) continue;
            c = fn_key_cmp(ent.key, ent.key_len,
                           &g_fn_keys[g_fn_win[wj].key_off],
                           g_fn_win[wj].key_len);
            if (c < 0) {
                /* Entry precedes the winner: skip iff its key is
                 * a window winner (owned elsewhere, already
                 * fetched, or a tombstone winner); otherwise the
                 * entry should have been inserted by phase 1. */
                if (fn_win_find(ent.key, ent.key_len) >= 0) continue;
                return -1;
            }
            if (c > 0) return -1; /* winner missing: inconsistency */
            /* Same key: must be exactly the winner version. */
            if (ent.commit_seq != g_fn_win[wj].seq) return -1;
            if (ent.val_len > 1024) return -1;
            if (g_fn_vals_off + ent.val_len > FN_VAL_ARENA) return -1;
            fn_memcpy(&g_fn_vals[g_fn_vals_off], ent.val, ent.val_len);
            g_fn_win[wj].val_off = g_fn_vals_off;
            g_fn_win[wj].val_len = ent.val_len;
            g_fn_vals_off += ent.val_len;
            wj++;
        }
    }
    return 0;
}

/* ---- merge + emission for one window ----
 * Merge return codes (emit mode):
 *  0 merge exhausted, emitted < limit (caller advances a window
 *    or finishes when the window was final);
 *  1 a live row remains past the limit inside this window;
 *  2 merge exhausted exactly at the limit and the window was
 *    final (no more rows at all);
 *  3 merge exhausted exactly at the limit and the window was not
 *    final (caller must peek at later windows for has_more);
 *  -1 error (bad lookup / inconsistent winner);
 *  -3 next row does not fit out_size.
 * Probe mode returns 1 on the first live row, 0 when the merge is
 * exhausted, -1 on error (never -3, nothing is written). */
static int fn_merge_run(SegDB* db, const uint8_t* prefix, uint32_t prefix_len,
                        uint64_t snapshot_seq,
                        const uint8_t* lo, uint32_t lo_len, int has_lo,
                        int window_final, const uint8_t* hi, uint32_t hi_len,
                        uint8_t* out_buffer, uint32_t out_size, uint32_t* out_off,
                        uint32_t* emitted, uint32_t limit, int probe) {
    uint32_t wi = 0;
    uint32_t hj = fn_hot_first_gt(lo, lo_len, has_lo);
    (void)db; /* values come from phase 2 (fn_fetch_window_values) */
    (void)prefix; /* prefix filtering for segments is done at collect;
     * hot entries are filtered per entry below. */
    for (;;) {
        int hot_avail = 0, seg_avail = 0;
        const uint8_t* hk = NULL;
        uint32_t hk_len = 0;
        /* Advance hot past invisible entries (newer than the
         * snapshot, or not prefix-matching). A hot entry newer
         * than the snapshot must NOT shadow a segment winner for
         * the same key, so skipping it here is what lets the
         * segment version win below. */
        while (hj < g_fn_hot_count) {
            FnHot* h = &g_fn_hot[hj];
            if (!window_final && fn_key_cmp(h->key, h->key_len, hi, hi_len) > 0) break;
            if (fn_prefix_beyond(h->key, h->key_len, prefix, prefix_len)) break;
            if (!fn_prefix_match(h->key, h->key_len, prefix, prefix_len) ||
                h->seq > snapshot_seq) {
                hj++;
                continue;
            }
            hot_avail = 1;
            hk = h->key;
            hk_len = h->key_len;
            break;
        }
        seg_avail = wi < g_fn_win_count;
        if (!seg_avail && !hot_avail) {
            if (probe) return 0;
            if (*emitted < limit) return 0;
            return window_final ? 2 : 3;
        }
        {
            int take_seg = 0, take_hot = 0, is_tomb = 0;
            const uint8_t* ek = NULL;
            uint32_t ek_len = 0;
            const uint8_t* ev = NULL;
            uint32_t ev_len = 0;
            int win_is_seg = 0;
            if (seg_avail && hot_avail) {
                FnWin* w = &g_fn_win[wi];
                const uint8_t* wk = &g_fn_keys[w->key_off];
                int c = fn_key_cmp(wk, w->key_len, hk, hk_len);
                if (c < 0) {
                    take_seg = 1;
                } else if (c > 0) {
                    take_hot = 1;
                } else {
                    take_seg = 1;
                    take_hot = 1;
                    if (g_fn_hot[hj].seq >= w->seq) {
                        win_is_seg = 0;
                    } else {
                        win_is_seg = 1;
                    }
                }
            } else if (seg_avail) {
                take_seg = 1;
                win_is_seg = 1;
            } else {
                take_hot = 1;
                win_is_seg = 0;
            }
            if (take_seg && take_hot) {
                FnWin* w = &g_fn_win[wi];
                if (win_is_seg) {
                    is_tomb = w->tomb;
                    ek = &g_fn_keys[w->key_off];
                    ek_len = w->key_len;
                } else {
                    is_tomb = g_fn_hot[hj].tomb;
                    ek = hk;
                    ek_len = hk_len;
                    ev = g_fn_hot[hj].val;
                    ev_len = g_fn_hot[hj].val_len;
                }
            } else if (take_seg) {
                FnWin* w = &g_fn_win[wi];
                is_tomb = w->tomb;
                ek = &g_fn_keys[w->key_off];
                ek_len = w->key_len;
                win_is_seg = 1;
            } else {
                is_tomb = g_fn_hot[hj].tomb;
                ek = hk;
                ek_len = hk_len;
                ev = g_fn_hot[hj].val;
                ev_len = g_fn_hot[hj].val_len;
                win_is_seg = 0;
            }
            if (is_tomb) {
                /* Tombstone winner: the key is consumed (both
                 * sides on an equal key) and dropped. */
                if (take_seg) wi++;
                if (take_hot) hj++;
                continue;
            }
            if (probe) return 1; /* a live row exists */
            if (*emitted >= limit) return 1; /* live row past limit */
            if (win_is_seg) {
                /* Value fetched in phase 2 by forward streaming
                 * (see fn_fetch_window_values). A winner without
                 * a fetched value is an inconsistency: fail,
                 * never emit a wrong row. */
                FnWin* w = &g_fn_win[wi];
                if (w->val_off == 0xFFFFFFFFu) return -1;
                ev = &g_fn_vals[w->val_off];
                ev_len = w->val_len;
            }
            {
                uint64_t need = (uint64_t)4 + ek_len + 4 + ev_len;
                if ((uint64_t)*out_off + need > out_size) return -3;
                out_buffer[*out_off + 0] = (uint8_t)(ek_len & 0xFF);
                out_buffer[*out_off + 1] = (uint8_t)((ek_len >> 8) & 0xFF);
                out_buffer[*out_off + 2] = (uint8_t)((ek_len >> 16) & 0xFF);
                out_buffer[*out_off + 3] = (uint8_t)((ek_len >> 24) & 0xFF);
                fn_memcpy(&out_buffer[*out_off + 4], ek, ek_len);
                out_buffer[*out_off + 4 + ek_len + 0] = (uint8_t)(ev_len & 0xFF);
                out_buffer[*out_off + 4 + ek_len + 1] = (uint8_t)((ev_len >> 8) & 0xFF);
                out_buffer[*out_off + 4 + ek_len + 2] = (uint8_t)((ev_len >> 16) & 0xFF);
                out_buffer[*out_off + 4 + ek_len + 3] = (uint8_t)((ev_len >> 24) & 0xFF);
                fn_memcpy(&out_buffer[*out_off + 8 + ek_len], ev, ev_len);
                *out_off += (uint32_t)need;
            }
            if (ek_len <= sizeof(g_fn_last_key)) {
                fn_memcpy(g_fn_last_key, ek, ek_len);
                g_fn_last_key_len = ek_len;
            } else {
                g_fn_last_key_len = 0;
            }
            (*emitted)++;
            if (take_seg) wi++;
            if (take_hot) hj++;
        }
    }
}

/* Peek: does any live row exist strictly past lo? Collects
 * windows from lo until a live row is found or the final window
 * is exhausted. Returns 1 (live row exists), 0 (none), -1 (error). */
static int fn_peek_has_more(SegDB* db, const uint8_t* prefix, uint32_t prefix_len,
                            uint64_t snapshot_seq,
                            const uint8_t* lo, uint32_t lo_len, int has_lo) {
    uint8_t cur_lo[1024];
    uint32_t cur_len = 0;
    int cur_has = has_lo;
    if (has_lo) {
        fn_memcpy(cur_lo, lo, lo_len);
        cur_len = lo_len;
    }
    for (;;) {
        int final_win = 0;
        int pr;
        if (fn_collect_window(db, prefix, prefix_len, snapshot_seq,
                              cur_lo, cur_len, cur_has, &final_win) != 0) return -1;
        if (g_fn_win_count == 0 && !final_win) return -1; /* impossible state */
        pr = fn_merge_run(db, prefix, prefix_len, snapshot_seq,
                          cur_lo, cur_len, cur_has, final_win,
                          g_fn_win_count > 0
                              ? &g_fn_keys[g_fn_win[g_fn_win_count - 1].key_off]
                              : NULL,
                          g_fn_win_count > 0 ? g_fn_win[g_fn_win_count - 1].key_len : 0,
                          NULL, 0, NULL, NULL, 0, 1);
        if (pr < 0) return -1;
        if (pr == 1) return 1;
        if (final_win) return 0;
        /* Advance past this window's maximum key. */
        {
            FnWin* mx = &g_fn_win[g_fn_win_count - 1];
            fn_memcpy(cur_lo, &g_fn_keys[mx->key_off], mx->key_len);
            cur_len = mx->key_len;
            cur_has = 1;
        }
    }
}

/* ---- cross-call discovery cache (O3) ----
 * One slot, separate storage from the working globals above, so a
 * stateless (large-limit) call can never clobber a cached session
 * and a cached page can never clobber a stateless call's sources:
 * a cached page copies the slot into the working globals, runs
 * the shared core below, then copies the updated resume state
 * (idx_pos / done) back. Names are copied with the sources;
 * window/hot arenas stay per-call scratch. */
static FnSrc g_fn_cache_srcs[SEGDB_FANIN_MAX_SOURCES];
static uint8_t g_fn_cache_names[FN_NAMES_ARENA];
static uint32_t g_fn_cache_nsrc = 0;
static uint32_t g_fn_cache_names_off = 0;
static SegDB* g_fn_cache_db = NULL;
static uint8_t g_fn_cache_prefix[1024];
static uint32_t g_fn_cache_prefix_len = 0;
static uint64_t g_fn_cache_snapshot = 0;
static uint64_t g_fn_cache_fp = 0;
static int g_fn_cache_alive = 0;
static int g_fn_cache_dead = 0; /* killed by error/stale: a matching
 * continuation must fail -5, never silently rediscover */
static Manifest g_fn_fp_group; /* fingerprint/extension group scratch */
static uint64_t g_fn_peek_idx[SEGDB_FANIN_MAX_SOURCES];
static uint8_t g_fn_peek_done[SEGDB_FANIN_MAX_SOURCES];

static uint64_t fn_fp_mix(uint64_t h, const uint8_t* data, uint32_t len) {
    uint32_t i;
    for (i = 0; i < len; i++) {
        h ^= data[i];
        h *= 1099511628211ULL;
    }
    return h;
}

static uint64_t fn_fp_u64(uint64_t h, uint64_t v) {
    uint8_t b[8];
    int i;
    for (i = 0; i < 8; i++) b[i] = (uint8_t)((v >> (i * 8)) & 0xFF);
    return fn_fp_mix(h, b, 8);
}

static uint64_t fn_fp_u32(uint64_t h, uint32_t v) {
    uint8_t b[4];
    int i;
    for (i = 0; i < 4; i++) b[i] = (uint8_t)((v >> (i * 8)) & 0xFF);
    return fn_fp_mix(h, b, 4);
}

/* Fold one manifest's identity into the fingerprint. */
static uint64_t fn_fp_manifest(uint64_t h, const Manifest* m) {
    uint32_t si;
    h = fn_fp_u64(h, m->generation);
    h = fn_fp_u64(h, m->checkpoint_seq);
    h = fn_fp_u32(h, m->segment_count);
    for (si = 0; si < m->segment_count; si++) {
        const SegMeta* s = &m->segments[si];
        h = fn_fp_mix(h, (const uint8_t*)s->obj, fn_strlen(s->obj));
        h = fn_fp_u64(h, s->checksum);
        h = fn_fp_u32(h, s->entry_count);
        h = fn_fp_u64(h, s->min_seq);
        h = fn_fp_u64(h, s->max_seq);
        h = fn_fp_mix(h, s->first_key, s->first_key_len);
        h = fn_fp_mix(h, s->last_key, s->last_key_len);
    }
    return h;
}

/* Generation fingerprint of the CURRENT engine state (see the
 * header contract). Returns 0, or -1 when a group manifest
 * cannot be read (fail closed: the caller must not trust a
 * cached source set it cannot validate against). */
static int fn_fingerprint(SegDB* db, uint64_t* out) {
    uint64_t h = 14695981039346656037ULL;
    uint32_t gi;
    h = fn_fp_u32(h, (uint32_t)db->has_dir);
    h = fn_fp_u32(h, (uint32_t)db->has_manifest);
    h = fn_fp_u32(h, db->group_count);
    h = fn_fp_u64(h, db->seq_base);
    h = fn_fp_mix(h, (const uint8_t*)db->dir_obj, fn_strlen(db->dir_obj));
    if (db->has_dir) {
        for (gi = 0; gi < db->group_count; gi++) {
            const Manifest* gm;
            h = fn_fp_mix(h, (const uint8_t*)db->group_objs[gi],
                          fn_strlen(db->group_objs[gi]));
            if (gi == db->group_count - 1) {
                gm = &db->manifest;
            } else {
                if (manifest_read(db->store, db->group_objs[gi], &g_fn_fp_group) != 0)
                    return -1;
                gm = &g_fn_fp_group;
            }
            h = fn_fp_manifest(h, gm);
        }
    } else {
        h = fn_fp_manifest(h, &db->manifest);
    }
    *out = h;
    return 0;
}

static int fn_cache_key_matches(SegDB* db, const uint8_t* prefix, uint32_t prefix_len,
                                uint64_t snapshot_seq) {
    if (db != g_fn_cache_db) return 0;
    if (snapshot_seq != g_fn_cache_snapshot) return 0;
    if (prefix_len != g_fn_cache_prefix_len) return 0;
    if (prefix_len > 0 && fn_memcmp(prefix, g_fn_cache_prefix, prefix_len) != 0) return 0;
    return 1;
}

static void fn_cache_kill(void) {
    g_fn_cache_alive = 0;
    g_fn_cache_dead = 1; /* key fields retained for the -5 check */
}

/* Compare one cached source against a current manifest entry. */
static int fn_cache_src_matches(const FnSrc* c, const SegMeta* m) {
    const uint8_t* cn = &g_fn_cache_names[c->name_off];
    const uint8_t* cf = &g_fn_cache_names[c->first_off];
    const uint8_t* cl = &g_fn_cache_names[c->last_off];
    if (c->name_len != fn_strlen(m->obj)) return 0;
    if (fn_memcmp(cn, (const uint8_t*)m->obj, c->name_len) != 0) return 0;
    if (c->first_len != m->first_key_len) return 0;
    if (c->first_len > 0 && fn_memcmp(cf, m->first_key, c->first_len) != 0) return 0;
    if (c->last_len != m->last_key_len) return 0;
    if (c->last_len > 0 && fn_memcmp(cl, m->last_key, c->last_len) != 0) return 0;
    if (c->min_seq != m->min_seq || c->max_seq != m->max_seq) return 0;
    if (c->entry_count != m->entry_count) return 0;
    if (c->file_checksum != m->checksum) return 0;
    return 1;
}

/* 1 iff the current segment list (manifest fields only, no
 * discovery) has the cached list as an exact prefix: the
 * flush-append case, and only that case. A compact's merged
 * segments carry new names, so they never match here. */
static int fn_current_prefix_extension(SegDB* db) {
    uint32_t seen = 0;
    uint32_t gi, si;
    if (db->has_dir) {
        for (gi = 0; gi < db->group_count; gi++) {
            const Manifest* gm;
            if (gi == db->group_count - 1) {
                gm = &db->manifest;
            } else {
                if (manifest_read(db->store, db->group_objs[gi], &g_fn_fp_group) != 0)
                    return 0;
                gm = &g_fn_fp_group;
            }
            for (si = 0; si < gm->segment_count; si++) {
                if (seen < g_fn_cache_nsrc &&
                    !fn_cache_src_matches(&g_fn_cache_srcs[seen], &gm->segments[si]))
                    return 0;
                seen++;
            }
        }
    } else {
        for (si = 0; si < db->manifest.segment_count; si++) {
            if (seen < g_fn_cache_nsrc &&
                !fn_cache_src_matches(&g_fn_cache_srcs[seen], &db->manifest.segments[si]))
                return 0;
            seen++;
        }
    }
    return seen >= g_fn_cache_nsrc;
}

/* Re-verify every non-skipped cached source against its own
 * bytes before a cache-hit page returns any row: seg_open
 * (header checksum, footer magic, total length) plus an FNV-1a
 * recomputation over the sparse-index bytes against the footer
 * index checksum. Data-block checksums are deliberately NOT
 * re-verified here — seg_iter_next verifies every block it
 * loads during the page itself, so a corrupted block still
 * fails the page before its entries are returned. */
static int fn_cache_verify_hit(SegDB* db) {
    uint32_t si;
    for (si = 0; si < g_fn_cache_nsrc; si++) {
        FnSrc* s = &g_fn_cache_srcs[si];
        const char* obj;
        uint64_t fnv, pos;
        uint8_t chunk[256];
        if (s->skipped) continue;
        obj = (const char*)&g_fn_cache_names[s->name_off];
        if (seg_open(db->store, obj, &g_fn_reader) != 0) return -1;
        if (g_fn_reader.entry_count != s->entry_count) return -1;
        if (g_fn_reader.total_len != s->total_len) return -1;
        fnv = 14695981039346656037ULL;
        pos = 0;
        while (pos < s->index_len) {
            uint64_t n = s->index_len - pos;
            uint32_t i;
            if (n > sizeof(chunk)) n = sizeof(chunk);
            if (db->store->vt->read_at(db->store->state, obj, s->index_offset + pos,
                                      chunk, n) != 0)
                return -1;
            for (i = 0; i < (uint32_t)n; i++) {
                fnv ^= chunk[i];
                fnv *= 1099511628211ULL;
            }
            pos += n;
        }
        if (fnv != g_fn_reader.footer_index_checksum) return -1;
    }
    return 0;
}

/* Field-by-field source copy: a struct assignment here would
 * lower to a memcpy call, which the wasm32 env module does not
 * provide (observed as an instantiation failure); scalar copies
 * always lower to plain loads/stores. */
static void fn_src_copy(FnSrc* dst, const FnSrc* src) {
    dst->name_off = src->name_off;
    dst->name_len = src->name_len;
    dst->first_off = src->first_off;
    dst->first_len = src->first_len;
    dst->last_off = src->last_off;
    dst->last_len = src->last_len;
    dst->index_offset = src->index_offset;
    dst->index_len = src->index_len;
    dst->block_count = src->block_count;
    dst->entry_count = src->entry_count;
    dst->min_seq = src->min_seq;
    dst->max_seq = src->max_seq;
    dst->idx_pos = src->idx_pos;
    dst->done = src->done;
    dst->skipped = src->skipped;
    dst->total_len = src->total_len;
    dst->file_checksum = src->file_checksum;
}

static void fn_cache_copy_in(void) {
    uint32_t i;
    g_fn_nsrc = g_fn_cache_nsrc;
    g_fn_names_off = g_fn_cache_names_off;
    for (i = 0; i < g_fn_cache_nsrc; i++) fn_src_copy(&g_fn_srcs[i], &g_fn_cache_srcs[i]);
    for (i = 0; i < g_fn_cache_names_off; i++) g_fn_names[i] = g_fn_cache_names[i];
}

static void fn_cache_copy_out(void) {
    uint32_t i;
    /* Names never change during a page; only resume state does. */
    g_fn_cache_nsrc = g_fn_nsrc;
    for (i = 0; i < g_fn_nsrc; i++) fn_src_copy(&g_fn_cache_srcs[i], &g_fn_srcs[i]);
}

/* Store the just-enumerated working sources as the session.
 * The caller has already enumerated; this copies the sources
 * and records the key + current fingerprint. Returns 0, or -1
 * when the fingerprint cannot be computed (session not stored). */
static int fn_cache_store(SegDB* db, const uint8_t* prefix, uint32_t prefix_len,
                          uint64_t snapshot_seq) {
    uint64_t fp;
    uint32_t i;
    if (fn_fingerprint(db, &fp) != 0) return -1;
    g_fn_cache_nsrc = g_fn_nsrc;
    g_fn_cache_names_off = g_fn_names_off;
    for (i = 0; i < g_fn_nsrc; i++) fn_src_copy(&g_fn_cache_srcs[i], &g_fn_srcs[i]);
    for (i = 0; i < g_fn_names_off; i++) g_fn_cache_names[i] = g_fn_names[i];
    g_fn_cache_db = db;
    if (prefix_len > 0) fn_memcpy(g_fn_cache_prefix, prefix, prefix_len);
    g_fn_cache_prefix_len = prefix_len;
    g_fn_cache_snapshot = snapshot_seq;
    g_fn_cache_fp = fp;
    g_fn_cache_alive = 1;
    g_fn_cache_dead = 0;
    return 0;
}

/* Shared page core: hot collection + window loop. The caller
 * has validated arguments and populated g_fn_srcs (by fresh
 * enumeration or by cache copy-in). */
static int fn_run_scan(SegDB* db, const uint8_t* prefix, uint32_t prefix_len,
                       uint64_t snapshot_seq,
                       const uint8_t* cursor_key, uint32_t cursor_len, int has_cursor,
                       uint32_t limit,
                       uint8_t* out_buffer, uint32_t out_size,
                       uint32_t* count_out,
                       uint8_t* next_key, uint32_t next_key_cap, uint32_t* next_len_out,
                       uint32_t* has_more_out) {
    FnHotCtx hctx;
    uint8_t lo[1024];
    uint32_t lo_len = 0;
    int has_lo = 0;
    uint32_t emitted = 0;
    uint32_t out_off = 0;
    int rc_final = 0; /* 0 ok, -1 err, -3 nospace */
    uint32_t has_more = 0;
    if (has_cursor) {
        fn_memcpy(lo, cursor_key, cursor_len);
        lo_len = cursor_len;
        has_lo = 1;
    }
    g_fn_hot_count = 0;
    g_fn_hot_overflow = 0;
    g_fn_hot_keys_off = 0;
    hctx.prefix = prefix;
    hctx.prefix_len = prefix_len;
    hctx.seq_base = db->seq_base;
    if (art_iterate(&db->hot.index, fn_hot_cb, &hctx) != 0 || g_fn_hot_overflow) {
        *count_out = 0;
        return SEGDB_FANIN_ERR;
    }
    fn_hot_sort();
    g_fn_last_key_len = 0;
    while (emitted < limit) {
        int final_win = 0;
        int mr;
        const uint8_t* hi = NULL;
        uint32_t hi_len = 0;
        if (fn_collect_window(db, prefix, prefix_len, snapshot_seq,
                              lo, lo_len, has_lo, &final_win) != 0) {
            rc_final = -1;
            break;
        }
        if (g_fn_win_count == 0 && !final_win) {
            rc_final = -1; /* impossible state: fail closed */
            break;
        }
        if (g_fn_win_count > 0) {
            hi = &g_fn_keys[g_fn_win[g_fn_win_count - 1].key_off];
            hi_len = g_fn_win[g_fn_win_count - 1].key_len;
        }
        if (g_fn_win_count > 0 &&
            fn_fetch_window_values(db, prefix, prefix_len, snapshot_seq,
                                   lo, lo_len, has_lo) != 0) {
            rc_final = -1;
            break;
        }
        mr = fn_merge_run(db, prefix, prefix_len, snapshot_seq,
                          lo, lo_len, has_lo, final_win, hi, hi_len,
                          out_buffer, out_size, &out_off, &emitted, limit, 0);
        if (mr == -1) {
            rc_final = -1;
            break;
        }
        if (mr == -3) {
            rc_final = -3;
            has_more = 1;
            break;
        }
        if (mr == 1) {
            has_more = 1;
            break;
        }
        if (mr == 2) {
            has_more = 0;
            break;
        }
        if (mr == 3) {
            /* Merge exhausted exactly at the limit, window not
             * final: peek at later windows for a live row. The
             * peek advances sources past this page (it collects
             * later windows, marking sources done when their
             * last key falls inside a peeked window), so its
             * resume state is restored afterwards: on a cached
             * page the persisted state must describe the page
             * actually returned, never a peeked window the
             * caller has not consumed yet (a source marked done
             * by the peek could still hold rows past the page's
             * last emitted key). On a stateless page this
             * restore is a no-op: the next call re-enumerates. */
            uint32_t pi;
            int pk;
            for (pi = 0; pi < g_fn_nsrc; pi++) {
                g_fn_peek_idx[pi] = g_fn_srcs[pi].idx_pos;
                g_fn_peek_done[pi] = (uint8_t)g_fn_srcs[pi].done;
            }
            pk = fn_peek_has_more(db, prefix, prefix_len, snapshot_seq,
                                  hi, hi_len, 1);
            for (pi = 0; pi < g_fn_nsrc; pi++) {
                g_fn_srcs[pi].idx_pos = g_fn_peek_idx[pi];
                g_fn_srcs[pi].done = (int)g_fn_peek_done[pi];
            }
            if (pk < 0) {
                rc_final = -1;
            } else {
                has_more = (uint32_t)pk;
            }
            break;
        }
        /* mr == 0: window exhausted, emitted < limit. */
        if (final_win) {
            has_more = 0;
            break;
        }
        /* Advance past this window's maximum key. Note: when the
         * map is empty and the window is not final, the guard
         * above already failed closed; when the map is empty and
         * final, we broke out just above. So hi exists here. */
        fn_memcpy(lo, hi, hi_len);
        lo_len = hi_len;
        has_lo = 1;
    }
    *count_out = emitted;
    if (rc_final == -3) {
        *has_more_out = 1;
    } else if (rc_final == -1) {
        *has_more_out = 0;
    } else {
        *has_more_out = has_more;
    }
    if (emitted > 0) {
        if (g_fn_last_key_len > 0 && g_fn_last_key_len <= next_key_cap) {
            fn_memcpy(next_key, g_fn_last_key, g_fn_last_key_len);
            *next_len_out = g_fn_last_key_len;
        } else {
            *next_len_out = 0;
        }
    } else if (has_cursor) {
        if (cursor_len <= next_key_cap) {
            fn_memcpy(next_key, cursor_key, cursor_len);
            *next_len_out = cursor_len;
        } else {
            *next_len_out = 0;
        }
    } else {
        *next_len_out = 0;
    }
    return rc_final == 0 ? 0 : rc_final;
}

int segdb_scan_fanin(SegDB* db, const uint8_t* prefix, uint32_t prefix_len,
                     uint64_t snapshot_seq,
                     const uint8_t* cursor_key, uint32_t cursor_len, int has_cursor,
                     uint32_t limit,
                     uint8_t* out_buffer, uint32_t out_size,
                     uint32_t* count_out,
                     uint8_t* next_key, uint32_t next_key_cap, uint32_t* next_len_out,
                     uint32_t* has_more_out) {
    int rc;
    /* Validation first, before any cache or session mutation:
     * a -4 writes no row and leaves the slot exactly as it was. */
    if (!db || !count_out || !next_len_out || !has_more_out) return SEGDB_FANIN_INVAL;
    if (!out_buffer && out_size > 0) return SEGDB_FANIN_INVAL;
    if (!next_key || next_key_cap == 0) return SEGDB_FANIN_INVAL;
    *count_out = 0;
    *next_len_out = 0;
    *has_more_out = 0;
    if (prefix_len > 1023 || (prefix_len > 0 && !prefix)) return SEGDB_FANIN_INVAL;
    if (limit == 0) return SEGDB_FANIN_INVAL;
    if (has_cursor) {
        if (cursor_len == 0 || cursor_len > 1023) return SEGDB_FANIN_INVAL;
        if (!cursor_key) return SEGDB_FANIN_INVAL;
    } else {
        if (cursor_len != 0) return SEGDB_FANIN_INVAL;
    }
    /* Large-limit calls stay on the original stateless path. */
    if (limit > SEGDB_FANIN_CACHE_MAX_LIMIT) {
        if (fn_enumerate(db, prefix, prefix_len, snapshot_seq) != 0) {
            *count_out = 0;
            return SEGDB_FANIN_ERR;
        }
        return fn_run_scan(db, prefix, prefix_len, snapshot_seq,
                           cursor_key, cursor_len, has_cursor, limit,
                           out_buffer, out_size, count_out,
                           next_key, next_key_cap, next_len_out, has_more_out);
    }
    /* Cached path. A cursorless page is a new scan: it always
     * discovers fresh and replaces the slot, never failing -5. */
    if (!has_cursor) {
        if (fn_enumerate(db, prefix, prefix_len, snapshot_seq) != 0) {
            *count_out = 0;
            return SEGDB_FANIN_ERR;
        }
        if (fn_cache_store(db, prefix, prefix_len, snapshot_seq) != 0) {
            *count_out = 0;
            return SEGDB_FANIN_ERR;
        }
        rc = fn_run_scan(db, prefix, prefix_len, snapshot_seq,
                         cursor_key, cursor_len, has_cursor, limit,
                         out_buffer, out_size, count_out,
                         next_key, next_key_cap, next_len_out, has_more_out);
        if (rc == 0 || rc == -3) fn_cache_copy_out();
        else fn_cache_kill();
        return rc;
    }
    /* Continuation page with a matching live slot: validate
     * before returning any row. */
    if (g_fn_cache_alive &&
        fn_cache_key_matches(db, prefix, prefix_len, snapshot_seq)) {
        uint64_t fp;
        if (fn_fingerprint(db, &fp) != 0) {
            fn_cache_kill();
            *count_out = 0;
            return SEGDB_FANIN_ERR;
        }
        if (fp == g_fn_cache_fp) {
            if (fn_cache_verify_hit(db) != 0) {
                fn_cache_kill();
                *count_out = 0;
                return SEGDB_FANIN_ERR;
            }
            fn_cache_copy_in();
            rc = fn_run_scan(db, prefix, prefix_len, snapshot_seq,
                             cursor_key, cursor_len, has_cursor, limit,
                             out_buffer, out_size, count_out,
                             next_key, next_key_cap, next_len_out, has_more_out);
            if (rc == 0 || rc == -3) fn_cache_copy_out();
            else fn_cache_kill();
            return rc;
        }
        /* Source set changed. Transparent rediscovery ONLY for
         * a flush-append (cached list is an exact prefix of the
         * current list); anything else (compact, recovery to a
         * different manifest) is a typed stale failure — never
         * rows mixed across source sets. */
        if (fn_current_prefix_extension(db)) {
            if (fn_enumerate(db, prefix, prefix_len, snapshot_seq) != 0) {
                fn_cache_kill();
                *count_out = 0;
                return SEGDB_FANIN_ERR;
            }
            if (fn_cache_store(db, prefix, prefix_len, snapshot_seq) != 0) {
                fn_cache_kill();
                *count_out = 0;
                return SEGDB_FANIN_ERR;
            }
            rc = fn_run_scan(db, prefix, prefix_len, snapshot_seq,
                             cursor_key, cursor_len, has_cursor, limit,
                             out_buffer, out_size, count_out,
                             next_key, next_key_cap, next_len_out, has_more_out);
            if (rc == 0 || rc == -3) fn_cache_copy_out();
            else fn_cache_kill();
            return rc;
        }
        fn_cache_kill();
        *count_out = 0;
        return SEGDB_FANIN_STALE;
    }
    /* Continuation against a killed slot with the same key: the
     * session is dead by an earlier error/stale failure; do not
     * silently rediscover into the middle of a failed page
     * sequence. Any other continuation (no slot, evicted slot,
     * first resume without a prior cached page) discovers fresh. */
    if (g_fn_cache_dead &&
        fn_cache_key_matches(db, prefix, prefix_len, snapshot_seq)) {
        *count_out = 0;
        return SEGDB_FANIN_STALE;
    }
    if (fn_enumerate(db, prefix, prefix_len, snapshot_seq) != 0) {
        *count_out = 0;
        return SEGDB_FANIN_ERR;
    }
    if (fn_cache_store(db, prefix, prefix_len, snapshot_seq) != 0) {
        *count_out = 0;
        return SEGDB_FANIN_ERR;
    }
    rc = fn_run_scan(db, prefix, prefix_len, snapshot_seq,
                     cursor_key, cursor_len, has_cursor, limit,
                     out_buffer, out_size, count_out,
                     next_key, next_key_cap, next_len_out, has_more_out);
    if (rc == 0 || rc == -3) fn_cache_copy_out();
    else fn_cache_kill();
    return rc;
}

/* ---- Track C: hierarchical fan-in scan ----
 * The flat fan-in path above caps one call's source table at
 * SEGDB_FANIN_MAX_SOURCES (4096). That cap is now a PER-LEVEL
 * (per-group, per-page-call-chunk) cap, never a global one:
 * this hierarchical path enumerates ONE manifest group at a
 * time (at most MANIFEST_MAX_SEGMENTS = 128 sources per chunk,
 * far below the flat cap), collects that chunk's keys into the
 * SHARED window map, and merges chunks by global publication
 * ordinal (group order, then segment order within the group),
 * so a later group wins a sequence tie exactly as a later
 * source does in the flat path. A DB with 64 groups x 128
 * segments = 8192 sources scans completely through this path;
 * the flat path is unchanged and still refuses >4096 sources
 * in one call with -1 (its contract is untouched).
 *
 * Why chunked collection is sound (the window-map argument):
 * the window map keeps the SMALLEST keys seen so far. Once
 * full, its maximum only ever DECREASES (an insert larger
 * than the maximum stops its source; an insert smaller than
 * the maximum evicts the maximum). A source stopped at key K
 * therefore has all remaining keys > K > every later maximum,
 * so no later chunk's eviction can pull a missed key of that
 * source back inside the window. A chunk processed later sees
 * the same or a smaller maximum and stops no later than it
 * would have under the original maximum. The window's maximum
 * is a real key that strictly advances the next window's
 * bound, so progress is guaranteed exactly as in the flat
 * path. Cross-group winner resolution is by (commit_seq,
 * global ordinal): fn_win_insert's tie-break (later source
 * wins) is fed the global ordinal, so within-group and
 * cross-group ties resolve identically to the flat path.
 *
 * Resume state across windows of ONE page call lives in
 * g_hier[] (indexed by global ordinal, at most 8192 entries):
 * the sparse-index resume position and done flag a chunk
 * source had at the end of the previous window. It is reset
 * at the start of every page call and never persists across
 * calls: each page call re-enumerates from its cursor, so a
 * compact/flush between pages can never corrupt a resumed
 * window — the next page simply scans the current manifest
 * from the cursor (stateless across calls, like the flat
 * path's large-limit calls; there is no -5 stale contract
 * here because no session is kept). Within a single page
 * call the store cannot change (single-threaded invariant,
 * host serializes callers), so resume reuse is safe.
 *
 * Values are fetched per winner with seg_lookup (full view
 * for <=256 blocks, paged view above — the same wiring as
 * the point-lookup path), never by re-streaming whole
 * segments: a winner whose looked-up entry is absent, is a
 * tombstone, or carries a different commit_seq than the
 * window recorded fails the call with -1 (fail closed,
 * never a wrong value). Block checksums are verified by
 * seg_lookup's candidate-block check before any value is
 * trusted. Segment discovery (seg_open + full discovery)
 * verifies the sparse-index checksum on the FIRST window's
 * chunk enumeration, exactly as the flat path does; later
 * windows reuse that geometry through the identity-checked
 * cache in fn_hier_add_source / fn_hier_seek /
 * fn_hier_fetch_window_values (segment objects are
 * immutable, so a matching manifest checksum + byte_size
 * pins the bytes; anything else re-discovers). Index and
 * block bytes are still read live from the store on every
 * window — only the geometry is cached.
 *
 * Memory per page call is bounded independent of total
 * sources and total rows: one chunk's source metadata
 * (<=128 sources), the 256-key window map, the 256 KiB
 * value arena, the hot set, and the 8192-entry resume
 * table (32 bytes each, fixed). The out buffer bound is the
 * caller's (a -3 partial page is returned when the next row
 * does not fit, same contract as the flat path).
 *
 * Stats (see segdb_scan_fanin_hier_stats_get): groups,
 * sources and max_group describe the LAST page call's FIRST
 * window (every window of a call enumerates the same groups,
 * so the first window already proves the level traversal);
 * windows counts windows collected by the last call. Reset
 * before a measurement, read after. */
#define SEGDB_HIER_MAX_SOURCES (64u * 128u)

typedef struct {
    uint64_t idx_pos;
    uint32_t block_count;
    int done;
    int seen;
    /* Per-page-call discovery cache (see fn_hier_add_source):
     * index geometry + identity of the segment bytes it was
     * discovered from. A cache hit requires the CURRENT
     * manifest's whole-file checksum AND byte_size to match:
     * segments are immutable objects, so identical
     * (name-independent) identity pins identical bytes, and
     * identical bytes discover identically. Any mismatch
     * re-discovers from the store. The cache is reset at
     * every page-call start, like the resume state. */
    uint64_t index_offset;
    uint64_t index_len;
    uint64_t total_len;
    uint64_t file_checksum;
    int disc_ok;
} HierResume;

static HierResume g_hier[SEGDB_HIER_MAX_SOURCES];
/* Per-page-call group shape cache (the store is immutable
 * during a page call, so caching counts is safe): segment
 * counts per group, filled once at page-call start. This
 * keeps ordinal -> group mapping O(groups) additions
 * instead of O(groups) manifest reads per lookup. */
static uint32_t g_hier_group_counts[64];
static uint32_t g_hier_ngroups_cached = 0;
static int g_hier_counts_ready = 0;
static uint64_t g_hier_stat_groups = 0;
static uint64_t g_hier_stat_sources = 0;
static uint64_t g_hier_stat_max_group = 0;
static uint64_t g_hier_stat_windows = 0;
static int g_hier_first_window = 1;

void segdb_scan_fanin_hier_stats_reset(void) {
    g_hier_stat_groups = 0;
    g_hier_stat_sources = 0;
    g_hier_stat_max_group = 0;
    g_hier_stat_windows = 0;
}

void segdb_scan_fanin_hier_stats_get(uint64_t* groups_out,
                                     uint64_t* sources_out,
                                     uint64_t* max_group_out,
                                     uint64_t* windows_out) {
    if (groups_out) *groups_out = g_hier_stat_groups;
    if (sources_out) *sources_out = g_hier_stat_sources;
    if (max_group_out) *max_group_out = g_hier_stat_max_group;
    if (windows_out) *windows_out = g_hier_stat_windows;
}

/* Number of manifest groups (levels) of this DB: 1 in
 * single-manifest mode, group_count in directory mode. */
static int fn_hier_ngroups(SegDB* db, uint32_t* n_out) {
    if (!db || !n_out) return -1;
    if (db->has_dir) {
        if (db->group_count == 0 ||
            db->group_count > MANIFEST_DIR_MAX_GROUPS) return -1;
        *n_out = db->group_count;
    } else {
        *n_out = 1;
    }
    return 0;
}

/* Load group gi's manifest: the last group is db->manifest
 * (in-memory); earlier groups are read into g_fn_group.
 * Returns 0 with *gm_out set, or -1. */
static int fn_hier_group_manifest(SegDB* db, uint32_t gi,
                                  const Manifest** gm_out) {
    uint32_t n;
    if (fn_hier_ngroups(db, &n) != 0) return -1;
    if (gi >= n) return -1;
    if (!db->has_dir) {
        *gm_out = &db->manifest;
        return 0;
    }
    if (gi == n - 1) {
        *gm_out = &db->manifest;
        return 0;
    }
    if (manifest_read(db->store, db->group_objs[gi], &g_fn_group) != 0)
        return -1;
    *gm_out = &g_fn_group;
    return 0;
}

/* Global publication ordinal of the first segment of group
 * gi (segments of earlier groups, in order). Returns 0, or
 * -1 when the shape is inconsistent. Uses the per-call
 * count cache when ready (no manifest reads); otherwise
 * reads the earlier groups' manifests (slow path, only
 * used before the cache is filled). */
static int fn_hier_group_base(SegDB* db, uint32_t gi, uint32_t* base_out) {
    uint32_t n, g;
    uint32_t base = 0;
    if (fn_hier_ngroups(db, &n) != 0) return -1;
    if (gi >= n) return -1;
    if (g_hier_counts_ready && g_hier_ngroups_cached == n) {
        for (g = 0; g < gi; g++) {
            base += g_hier_group_counts[g];
            if (base > SEGDB_HIER_MAX_SOURCES) return -1;
        }
        *base_out = base;
        return 0;
    }
    for (g = 0; g < gi; g++) {
        const Manifest* gm;
        if (fn_hier_group_manifest(db, g, &gm) != 0) return -1;
        base += gm->segment_count;
        if (base > SEGDB_HIER_MAX_SOURCES) return -1;
    }
    *base_out = base;
    return 0;
}

/* Fill the per-call group count cache (one manifest read
 * per group). Must run at page-call start, before any
 * window collection. */
static int fn_hier_cache_counts(SegDB* db) {
    uint32_t n, g;
    if (fn_hier_ngroups(db, &n) != 0) return -1;
    if (n > 64) return -1;
    for (g = 0; g < n; g++) {
        const Manifest* gm;
        if (fn_hier_group_manifest(db, g, &gm) != 0) return -1;
        g_hier_group_counts[g] = gm->segment_count;
    }
    g_hier_ngroups_cached = n;
    g_hier_counts_ready = 1;
    return 0;
}

/* Enumerate exactly group gi's segments into g_fn_srcs
 * (replacing any previous chunk), applying the per-call
 * resume state for ordinals already seen in this page call.
 * On the first window of a call this also records the
 * level-traversal stats (groups, sources, max group). */
/* Hierarchical source add: identical FnSrc construction to
 * fn_add_source (same arena copies, same skipped proof), but
 * a non-skipped source whose discovery is cached in g_hier[]
 * for the CURRENT manifest identity (whole-file checksum +
 * byte_size) skips seg_open + seg_ensure_discovered entirely:
 * the first window of the page call discovers it (verifying
 * header/footer, every block checksum, the index checksum
 * and first/last keys against actual entries), and later
 * windows reuse that verified geometry. The identity check
 * is what makes reuse sound: segment objects are never
 * rewritten in place, so a manifest entry with the same
 * checksum and length names the same bytes, and the same
 * bytes discover to the same geometry. A mismatch (torn
 * store, replaced segment) falls through to full discovery,
 * which fails the call rather than trusting stale offsets.
 * Index bytes and block bytes are still read LIVE from the
 * store on every window (fn_hier_seek's index walk and
 * seg_iter_next); only the geometry (offsets/lengths) is
 * cached, and every candidate block's checksum is verified
 * before any of its entries is returned. */
static int fn_hier_add_source(SegDB* db, const SegMeta* meta,
                              const uint8_t* prefix, uint32_t prefix_len,
                              uint64_t snapshot_seq, uint32_t ord) {
    HierResume* hc = &g_hier[ord];
    FnSrc* s;
    if (hc->disc_ok && hc->file_checksum == meta->checksum &&
        hc->total_len == meta->byte_size) {
        int noff, foff, loff;
        if (g_fn_nsrc >= SEGDB_FANIN_MAX_SOURCES) return -1;
        noff = fn_arena_copy((const uint8_t*)meta->obj,
                             fn_strlen(meta->obj));
        if (noff < 0) return -1;
        foff = fn_arena_copy(meta->first_key, meta->first_key_len);
        if (foff < 0) return -1;
        loff = fn_arena_copy(meta->last_key, meta->last_key_len);
        if (loff < 0) return -1;
        s = &g_fn_srcs[g_fn_nsrc];
        s->name_off = (uint32_t)noff;
        s->name_len = fn_strlen(meta->obj);
        s->first_off = (uint32_t)foff;
        s->first_len = meta->first_key_len;
        s->last_off = (uint32_t)loff;
        s->last_len = meta->last_key_len;
        s->min_seq = meta->min_seq;
        s->max_seq = meta->max_seq;
        s->entry_count = meta->entry_count;
        s->idx_pos = 0;
        s->done = 0;
        s->skipped = 0;
        s->total_len = hc->total_len;
        s->file_checksum = meta->checksum;
        if (meta->entry_count == 0 || meta->min_seq > snapshot_seq) {
            s->skipped = 1;
        } else if (prefix_len > 0) {
            if (fn_key_cmp(meta->last_key, meta->last_key_len,
                           prefix, prefix_len) < 0 ||
                fn_prefix_beyond(meta->first_key, meta->first_key_len,
                                 prefix, prefix_len)) {
                s->skipped = 1;
            }
        }
        if (!s->skipped) {
            s->index_offset = hc->index_offset;
            s->index_len = hc->index_len;
            s->block_count = hc->block_count;
            s->idx_pos = hc->index_offset;
        } else {
            s->index_offset = 0;
            s->index_len = 0;
            s->block_count = 0;
            s->idx_pos = 0;
        }
        g_fn_nsrc++;
        return 0;
    }
    if (fn_add_source(db, meta, prefix, prefix_len, snapshot_seq) != 0)
        return -1;
    s = &g_fn_srcs[g_fn_nsrc - 1];
    if (!s->skipped) {
        hc->index_offset = s->index_offset;
        hc->index_len = s->index_len;
        hc->block_count = s->block_count;
        hc->total_len = s->total_len;
        hc->file_checksum = meta->checksum;
        hc->disc_ok = 1;
    }
    return 0;
}

static int fn_hier_enumerate_group(SegDB* db, uint32_t gi,
                                   const uint8_t* prefix,
                                   uint32_t prefix_len,
                                   uint64_t snapshot_seq,
                                   uint32_t base) {
    const Manifest* gm;
    uint32_t si;
    if (fn_hier_group_manifest(db, gi, &gm) != 0) return -1;
    g_fn_nsrc = 0;
    g_fn_names_off = 0;
    for (si = 0; si < gm->segment_count; si++) {
        uint32_t ord = base + si;
        FnSrc* s;
        if (ord >= SEGDB_HIER_MAX_SOURCES) return -1;
        if (fn_hier_add_source(db, &gm->segments[si], prefix, prefix_len,
                               snapshot_seq, ord) != 0)
            return -1;
        s = &g_fn_srcs[g_fn_nsrc - 1];
        if (g_hier[ord].seen && !s->skipped) {
            s->idx_pos = g_hier[ord].idx_pos;
            s->done = g_hier[ord].done;
        }
        if (g_hier_first_window) {
            g_hier_stat_sources++;
        }
    }
    if (g_hier_first_window) {
        g_hier_stat_groups++;
        if (gi > g_hier_stat_max_group) g_hier_stat_max_group = gi;
    }
    return 0;
}

/* Hierarchical seek: fn_seek's index walk (index bytes read
 * LIVE from the store, starting at the resumed position),
 * but the reader is primed from the FnSrc's verified
 * discovery geometry instead of seg_open + rediscovery:
 * every hier source's geometry was verified at enumeration
 * (fresh discovery, or the identity-checked cache), and
 * seg_iter_next still verifies each candidate block's
 * checksum from the store before returning its entries.
 * The flat path keeps fn_seek unchanged. */
static int fn_hier_seek(SegDB* db, FnSrc* s, const uint8_t* lo,
                        uint32_t lo_len, int has_lo) {
    const char* obj = (const char*)&g_fn_names[s->name_off];
    uint64_t pos = s->idx_pos;
    uint64_t end = s->index_offset + s->index_len;
    uint64_t chosen_pos = s->index_offset;
    int have_chosen = 0;
    uint32_t i;
    SegReader* r;
    if (!has_lo) {
        chosen_pos = s->index_offset;
        have_chosen = 1;
    } else {
        uint8_t kb[1024];
        while (pos < end) {
            uint32_t klen = 0;
            uint64_t boff = 0, blen = 0, next = 0;
            int cmp;
            if (fn_index_entry(db, obj, pos, kb, &klen, &boff, &blen,
                               &next) != 0)
                return -1;
            cmp = fn_key_cmp(kb, klen, lo, lo_len);
            if (cmp <= 0) {
                chosen_pos = pos;
                have_chosen = 1;
                pos = next;
            } else {
                break;
            }
        }
        if (!have_chosen) {
            chosen_pos = s->index_offset;
            have_chosen = 1;
        }
    }
    if (!have_chosen) return -1;
    s->idx_pos = chosen_pos;
    r = &g_fn_reader;
    for (i = 0; i < (uint32_t)sizeof(*r); i++)
        ((uint8_t*)r)[i] = 0;
    r->st = db->store;
    for (i = 0; i < s->name_len && i < SEG_MAX_OBJ_LEN; i++)
        r->obj[i] = obj[i];
    r->obj[i] = '\0';
    r->open = 1;
    r->entry_count = s->entry_count;
    r->first_key_len = s->first_len;
    r->last_key_len = s->last_len;
    r->min_seq = s->min_seq;
    r->max_seq = s->max_seq;
    r->header_len = (uint64_t)SEG_HEADER_SIZE + s->first_len + s->last_len;
    r->total_len = s->total_len;
    r->flags = 0; /* no Bloom on the primed path: seg_iter_next
                   * never consults it, and skipping it is
                   * always correct (it is only a pre-filter) */
    fn_reader_prime(r, s);
    r->iter_index_pos = chosen_pos;
    g_fn_stat_seeks++;
    return 0;
}

/* Collect one chunk's keys into the shared window map (the
 * map is NOT reset here — the caller resets it once per
 * window). Mirrors fn_collect_window's per-source loop, but
 * with global ordinals for the winner tie-break, and writes
 * each source's resume state back to g_hier[] afterwards. */
static int fn_hier_collect_chunk(SegDB* db, const uint8_t* prefix,
                                 uint32_t prefix_len,
                                 uint64_t snapshot_seq,
                                 const uint8_t* lo, uint32_t lo_len,
                                 int has_lo, uint32_t base,
                                 int* cap_stop_out) {
    uint32_t si;
    for (si = 0; si < g_fn_nsrc; si++) {
        FnSrc* s = &g_fn_srcs[si];
        uint32_t ord = base + si;
        if (s->done || s->skipped) continue;
        if (has_lo && fn_key_cmp(&g_fn_names[s->last_off], s->last_len,
                                 lo, lo_len) <= 0) {
            s->done = 1;
            if (ord < SEGDB_HIER_MAX_SOURCES) {
                g_hier[ord].done = 1;
                g_hier[ord].seen = 1;
            }
            continue;
        }
        if (fn_hier_seek(db, s, lo, lo_len, has_lo) != 0) return -1;
        for (;;) {
            SegEntry ent;
            int rc = seg_iter_next(&g_fn_reader, &ent, g_fn_kb,
                                   (uint32_t)sizeof(g_fn_kb),
                                   g_fn_vb, (uint32_t)sizeof(g_fn_vb));
            if (rc == 1) break;
            if (rc == -1) {
                if (g_fn_reader.iter_index_pos >= g_fn_reader.iter_index_end &&
                    !g_fn_reader.iter_in_block) {
                    break;
                }
                return -1;
            }
            g_fn_stat_entries++;
            if (has_lo && fn_key_cmp(ent.key, ent.key_len, lo, lo_len) <= 0) continue;
            if (fn_prefix_beyond(ent.key, ent.key_len, prefix, prefix_len)) break;
            if (!fn_prefix_match(ent.key, ent.key_len, prefix, prefix_len)) continue;
            if (ent.commit_seq > snapshot_seq) continue;
            {
                int ins = fn_win_insert(ent.key, ent.key_len, ent.commit_seq,
                                        ord, ent.is_tombstone);
                if (ins < 0) return -1;
                if (ins == 1) {
                    *cap_stop_out = 1;
                    break;
                }
            }
        }
        if (ord < SEGDB_HIER_MAX_SOURCES) {
            g_hier[ord].idx_pos = s->idx_pos;
            g_hier[ord].done = s->done;
            g_hier[ord].seen = 1;
            g_hier[ord].block_count = s->block_count;
        }
    }
    return 0;
}

/* Collect one window across ALL groups, in publication
 * order, into the shared window map. Sets *window_final = 1
 * iff no source in any group stopped on a full map. */
static int fn_hier_collect_window(SegDB* db, const uint8_t* prefix,
                                  uint32_t prefix_len,
                                  uint64_t snapshot_seq,
                                  const uint8_t* lo, uint32_t lo_len,
                                  int has_lo, int* window_final) {
    uint32_t n, gi;
    int cap_stop = 0;
    g_fn_win_count = 0;
    g_fn_keys_off = 0;
    g_fn_vals_off = 0;
    *window_final = 0;
    if (fn_hier_ngroups(db, &n) != 0) return -1;
    for (gi = 0; gi < n; gi++) {
        uint32_t base;
        if (fn_hier_group_base(db, gi, &base) != 0) return -1;
        if (fn_hier_enumerate_group(db, gi, prefix, prefix_len,
                                    snapshot_seq, base) != 0)
            return -1;
        if (fn_hier_collect_chunk(db, prefix, prefix_len, snapshot_seq,
                                  lo, lo_len, has_lo, base, &cap_stop) != 0)
            return -1;
    }
    *window_final = cap_stop ? 0 : 1;
    g_fn_stat_windows++;
    g_hier_stat_windows++;
    g_hier_first_window = 0;
    return 0;
}

/* Locate the segment meta for global ordinal ord: its object
 * name plus the manifest fields a primed fetch reader needs.
 * Returns 0, or -1 when ord is out of range. */
static int fn_hier_ordinal_meta(SegDB* db, uint32_t ord,
                                char* obj_out, uint32_t cap,
                                uint32_t* first_len_out,
                                uint32_t* last_len_out,
                                uint32_t* entry_count_out,
                                uint64_t* byte_size_out,
                                uint64_t* checksum_out) {
    uint32_t n, gi;
    uint32_t base = 0;
    const Manifest* gm;
    const SegMeta* m;
    uint32_t i = 0;
    if (fn_hier_ngroups(db, &n) != 0) return -1;
    for (gi = 0; gi < n; gi++) {
        uint32_t cnt;
        if (g_hier_counts_ready && g_hier_ngroups_cached == n) {
            cnt = g_hier_group_counts[gi];
        } else {
            if (fn_hier_group_manifest(db, gi, &gm) != 0) return -1;
            cnt = gm->segment_count;
        }
        if (ord >= base && ord < base + cnt) {
            if (fn_hier_group_manifest(db, gi, &gm) != 0) return -1;
            m = &gm->segments[ord - base];
            while (m->obj[i] != '\0') {
                if (i + 1 >= cap) return -1;
                obj_out[i] = m->obj[i];
                i++;
            }
            obj_out[i] = '\0';
            *first_len_out = m->first_key_len;
            *last_len_out = m->last_key_len;
            *entry_count_out = m->entry_count;
            *byte_size_out = m->byte_size;
            *checksum_out = m->checksum;
            return 0;
        }
        base += cnt;
    }
    return -1;
}

/* Fetch winner values for the current window with
 * seg_lookup (full view <=256 blocks, paged view above —
 * the point-lookup wiring). A winner whose entry is absent,
 * tombstoned, or carries a different commit_seq than the
 * window recorded fails with -1: fail closed, never a
 * wrong value. Tombstone winners carry no value. The
 * reader is primed from the identity-checked discovery
 * cache when it covers this winner (same argument as
 * fn_hier_add_source); otherwise seg_open + full discovery
 * runs and records the cache for later windows. Either way
 * seg_lookup verifies the candidate block's checksum from
 * the store before returning a value, and the seq check
 * below fails the call on any mismatch. */
static int fn_hier_fetch_window_values(SegDB* db) {
    uint32_t wi;
    for (wi = 0; wi < g_fn_win_count; wi++) {
        FnWin* w = &g_fn_win[wi];
        char obj[128];
        SegReader r;
        SegEntry ent;
        uint64_t view[SEG_INDEX_VIEW_MAX];
        uint64_t page_view[1024];
        uint32_t first_len = 0, last_len = 0, entry_count = 0;
        uint64_t byte_size = 0, checksum = 0;
        uint32_t i;
        int lrc;
        if (w->tomb) continue;
        if (fn_hier_ordinal_meta(db, w->src, obj, (uint32_t)sizeof(obj),
                                 &first_len, &last_len, &entry_count,
                                 &byte_size, &checksum) != 0)
            return -1;
        if (w->src < SEGDB_HIER_MAX_SOURCES &&
            g_hier[w->src].disc_ok &&
            g_hier[w->src].file_checksum == checksum &&
            g_hier[w->src].total_len == byte_size) {
            for (i = 0; i < (uint32_t)sizeof(r); i++)
                ((uint8_t*)&r)[i] = 0;
            r.st = db->store;
            for (i = 0; obj[i] != '\0' && i < SEG_MAX_OBJ_LEN; i++)
                r.obj[i] = obj[i];
            r.obj[i] = '\0';
            r.open = 1;
            r.entry_count = entry_count;
            r.first_key_len = first_len;
            r.last_key_len = last_len;
            r.header_len = (uint64_t)SEG_HEADER_SIZE + first_len + last_len;
            r.total_len = byte_size;
            r.flags = 0; /* no Bloom on the primed path */
            r.discovered = 1;
            r.index_offset = g_hier[w->src].index_offset;
            r.index_len = g_hier[w->src].index_len;
            r.block_count = g_hier[w->src].block_count;
            r.iter_index_pos = r.index_offset;
            r.iter_index_end = r.index_offset + r.index_len;
        } else {
            if (seg_open(db->store, obj, &r) != 0) return -1;
            if (seg_ensure_discovered(&r) != 0) {
                seg_close(&r);
                return -1;
            }
            if (w->src < SEGDB_HIER_MAX_SOURCES) {
                g_hier[w->src].index_offset = r.index_offset;
                g_hier[w->src].index_len = r.index_len;
                g_hier[w->src].block_count = r.block_count;
                g_hier[w->src].total_len = r.total_len;
                g_hier[w->src].file_checksum = checksum;
                g_hier[w->src].disc_ok = 1;
            }
        }
        if (r.block_count > 0 && r.block_count <= SEG_INDEX_VIEW_MAX) {
            int nv = seg_index_view(&r, view, SEG_INDEX_VIEW_MAX);
            if (nv >= 0 && (uint32_t)nv == r.block_count) {
                r.idx_offs = view;
                r.idx_offs_count = (uint32_t)nv;
            }
        } else if (r.block_count > SEG_INDEX_VIEW_MAX) {
            int np = seg_index_view_paged(&r, page_view, 1024,
                                          SEG_INDEX_PAGE_SIZE);
            if (np >= 0 &&
                (uint32_t)np ==
                    (r.block_count + SEG_INDEX_PAGE_SIZE - 1) /
                        SEG_INDEX_PAGE_SIZE) {
                r.idx_page_offs = page_view;
                r.idx_page_count = (uint32_t)np;
                r.idx_page_size = SEG_INDEX_PAGE_SIZE;
            }
        }
        lrc = seg_lookup(&r, &g_fn_keys[w->key_off], w->key_len,
                         &ent, g_fn_vb, (uint32_t)sizeof(g_fn_vb));
        if (lrc != 0 || ent.is_tombstone || ent.commit_seq != w->seq) {
            seg_close(&r);
            return -1;
        }
        if (ent.val_len > 1024) {
            seg_close(&r);
            return -1;
        }
        if (g_fn_vals_off + ent.val_len > FN_VAL_ARENA) {
            seg_close(&r);
            return -1;
        }
        fn_memcpy(&g_fn_vals[g_fn_vals_off], ent.val, ent.val_len);
        w->val_off = g_fn_vals_off;
        w->val_len = ent.val_len;
        g_fn_vals_off += ent.val_len;
        seg_close(&r);
    }
    return 0;
}

/* Peek: does any live row exist strictly past lo? Collects
 * hier windows from lo until a live row is found or the
 * final window is exhausted. Resume state advanced by the
 * peek is discarded with the page call (stateless across
 * calls), so no restore is needed. */
static int fn_hier_peek_has_more(SegDB* db, const uint8_t* prefix,
                                 uint32_t prefix_len,
                                 uint64_t snapshot_seq,
                                 const uint8_t* lo, uint32_t lo_len,
                                 int has_lo) {
    uint8_t cur_lo[1024];
    uint32_t cur_len = 0;
    int cur_has = has_lo;
    if (has_lo) {
        fn_memcpy(cur_lo, lo, lo_len);
        cur_len = lo_len;
    }
    for (;;) {
        int final_win = 0;
        int pr;
        if (fn_hier_collect_window(db, prefix, prefix_len, snapshot_seq,
                                   cur_lo, cur_len, cur_has, &final_win) != 0)
            return -1;
        if (g_fn_win_count == 0 && !final_win) return -1;
        pr = fn_merge_run(db, prefix, prefix_len, snapshot_seq,
                          cur_lo, cur_len, cur_has, final_win,
                          g_fn_win_count > 0
                              ? &g_fn_keys[g_fn_win[g_fn_win_count - 1].key_off]
                              : NULL,
                          g_fn_win_count > 0 ? g_fn_win[g_fn_win_count - 1].key_len : 0,
                          NULL, 0, NULL, NULL, 0, 1);
        if (pr < 0) return -1;
        if (pr == 1) return 1;
        if (final_win) return 0;
        {
            FnWin* mx = &g_fn_win[g_fn_win_count - 1];
            fn_memcpy(cur_lo, &g_fn_keys[mx->key_off], mx->key_len);
            cur_len = mx->key_len;
            cur_has = 1;
        }
    }
}

/* Shared hier page core: hot collection + window loop.
 * Mirrors fn_run_scan, with hier collect/fetch/peek. */
static int fn_hier_run_scan(SegDB* db, const uint8_t* prefix,
                            uint32_t prefix_len,
                            uint64_t snapshot_seq,
                            const uint8_t* cursor_key, uint32_t cursor_len,
                            int has_cursor, uint32_t limit,
                            uint8_t* out_buffer, uint32_t out_size,
                            uint32_t* count_out,
                            uint8_t* next_key, uint32_t next_key_cap,
                            uint32_t* next_len_out,
                            uint32_t* has_more_out) {
    FnHotCtx hctx;
    uint8_t lo[1024];
    uint32_t lo_len = 0;
    int has_lo = 0;
    uint32_t emitted = 0;
    uint32_t out_off = 0;
    int rc_final = 0;
    uint32_t has_more = 0;
    uint32_t oi;
    if (has_cursor) {
        fn_memcpy(lo, cursor_key, cursor_len);
        lo_len = cursor_len;
        has_lo = 1;
    }
    for (oi = 0; oi < SEGDB_HIER_MAX_SOURCES; oi++) {
        g_hier[oi].idx_pos = 0;
        g_hier[oi].block_count = 0;
        g_hier[oi].done = 0;
        g_hier[oi].seen = 0;
        g_hier[oi].index_offset = 0;
        g_hier[oi].index_len = 0;
        g_hier[oi].total_len = 0;
        g_hier[oi].file_checksum = 0;
        g_hier[oi].disc_ok = 0;
    }
    g_hier_counts_ready = 0;
    g_hier_ngroups_cached = 0;
    if (fn_hier_cache_counts(db) != 0) {
        *count_out = 0;
        return SEGDB_FANIN_ERR;
    }
    g_hier_stat_groups = 0;
    g_hier_stat_sources = 0;
    g_hier_stat_max_group = 0;
    g_hier_stat_windows = 0;
    g_hier_first_window = 1;
    g_fn_hot_count = 0;
    g_fn_hot_overflow = 0;
    g_fn_hot_keys_off = 0;
    hctx.prefix = prefix;
    hctx.prefix_len = prefix_len;
    hctx.seq_base = db->seq_base;
    if (art_iterate(&db->hot.index, fn_hot_cb, &hctx) != 0 || g_fn_hot_overflow) {
        *count_out = 0;
        return SEGDB_FANIN_ERR;
    }
    fn_hot_sort();
    g_fn_last_key_len = 0;
    while (emitted < limit) {
        int final_win = 0;
        int mr;
        const uint8_t* hi = NULL;
        uint32_t hi_len = 0;
        if (fn_hier_collect_window(db, prefix, prefix_len, snapshot_seq,
                                   lo, lo_len, has_lo, &final_win) != 0) {
            rc_final = -1;
            break;
        }
        if (g_fn_win_count == 0 && !final_win) {
            rc_final = -1;
            break;
        }
        if (g_fn_win_count > 0) {
            hi = &g_fn_keys[g_fn_win[g_fn_win_count - 1].key_off];
            hi_len = g_fn_win[g_fn_win_count - 1].key_len;
        }
        if (g_fn_win_count > 0 &&
            fn_hier_fetch_window_values(db) != 0) {
            rc_final = -1;
            break;
        }
        mr = fn_merge_run(db, prefix, prefix_len, snapshot_seq,
                          lo, lo_len, has_lo, final_win, hi, hi_len,
                          out_buffer, out_size, &out_off, &emitted, limit, 0);
        if (mr == -1) {
            rc_final = -1;
            break;
        }
        if (mr == -3) {
            rc_final = -3;
            has_more = 1;
            break;
        }
        if (mr == 1) {
            has_more = 1;
            break;
        }
        if (mr == 2) {
            has_more = 0;
            break;
        }
        if (mr == 3) {
            int pk = fn_hier_peek_has_more(db, prefix, prefix_len,
                                           snapshot_seq, hi, hi_len, 1);
            if (pk < 0) {
                rc_final = -1;
            } else {
                has_more = (uint32_t)pk;
            }
            break;
        }
        if (final_win) {
            has_more = 0;
            break;
        }
        fn_memcpy(lo, hi, hi_len);
        lo_len = hi_len;
        has_lo = 1;
    }
    *count_out = emitted;
    if (rc_final == -3) {
        *has_more_out = 1;
    } else if (rc_final == -1) {
        *has_more_out = 0;
    } else {
        *has_more_out = has_more;
    }
    if (emitted > 0) {
        if (g_fn_last_key_len > 0 && g_fn_last_key_len <= next_key_cap) {
            fn_memcpy(next_key, g_fn_last_key, g_fn_last_key_len);
            *next_len_out = g_fn_last_key_len;
        } else {
            *next_len_out = 0;
        }
    } else if (has_cursor) {
        if (cursor_len <= next_key_cap) {
            fn_memcpy(next_key, cursor_key, cursor_len);
            *next_len_out = cursor_len;
        } else {
            *next_len_out = 0;
        }
    } else {
        *next_len_out = 0;
    }
    return rc_final == 0 ? 0 : rc_final;
}

int segdb_scan_fanin_hier(SegDB* db, const uint8_t* prefix,
                          uint32_t prefix_len, uint64_t snapshot_seq,
                          const uint8_t* cursor_key, uint32_t cursor_len,
                          int has_cursor, uint32_t limit,
                          uint8_t* out_buffer, uint32_t out_size,
                          uint32_t* count_out,
                          uint8_t* next_key, uint32_t next_key_cap,
                          uint32_t* next_len_out,
                          uint32_t* has_more_out) {
    if (!db || !count_out || !next_len_out || !has_more_out)
        return SEGDB_FANIN_INVAL;
    if (!out_buffer && out_size > 0) return SEGDB_FANIN_INVAL;
    if (!next_key || next_key_cap == 0) return SEGDB_FANIN_INVAL;
    *count_out = 0;
    *next_len_out = 0;
    *has_more_out = 0;
    if (prefix_len > 1023 || (prefix_len > 0 && !prefix))
        return SEGDB_FANIN_INVAL;
    if (limit == 0) return SEGDB_FANIN_INVAL;
    if (has_cursor) {
        if (cursor_len == 0 || cursor_len > 1023) return SEGDB_FANIN_INVAL;
        if (!cursor_key) return SEGDB_FANIN_INVAL;
    } else {
        if (cursor_len != 0) return SEGDB_FANIN_INVAL;
    }
    return fn_hier_run_scan(db, prefix, prefix_len, snapshot_seq,
                            cursor_key, cursor_len, has_cursor, limit,
                            out_buffer, out_size, count_out,
                            next_key, next_key_cap, next_len_out,
                            has_more_out);
}
