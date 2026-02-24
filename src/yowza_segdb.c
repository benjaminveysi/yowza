/* Track A item 5 — layered segdb through wasm exports.
 *
 * The SegDB struct itself lives in Go-managed wasm linear memory
 * (NOT as a wasm static: sizeof(SegDB) is ~1.2 MB, and a static
 * would push the wasm stack/BSS past the Go host's 2 MiB arena
 * offset). The Go host passes pointers for state, hot arena, hot
 * WAL, and a heap region; this file only holds a tiny heap-store
 * context (pointer + size) as a static.
 *
 * Heap store: a self-describing object store inside the heap
 * region, so the Go host can persist segdb by copying the heap
 * region bytes out and writing them back before a later init:
 *   header (32 bytes): magic "YWHEAP01" | table_count u32=8192 |
 *     bump u64 | reserved u64
 *   table (8192 slots x 152 bytes): name[128] | offset u64 |
 *     len u64 | cap u64
 *   data: bump-allocated, starting right after the table.
 * Object cap: 8192 objects (segments + one manifest object per
 * flush generation + pointers + directory). Track C raised
 * the cap from 4096 (a single wasm DB could not hold the
 * >4096 segments the hierarchical scan exists for) and
 * migrates legacy 4096-slot heaps in place on init (see
 * hp_migrate_legacy_heap): the table starts at the same
 * offset in both layouts, so slots keep their indices and
 * only the data region shifts right by one old table size.
 * A legacy heap that cannot migrate (too little room in
 * the host buffer) fails init with -1 instead of being
 * wiped: fail closed, never silent data loss. Single-manifest mode
 * keeps every manifest generation as an orphan (gen-1 fallback
 * needs only the previous one, but nothing removes older ones),
 * so slot use grows with flush count; exhaustion fails cleanly
 * with -1, never corrupts. Removed objects free their slots
 * (compact frees old segments), so compacting keeps a wasm DB
 * under the cap; the native stores have no such cap and are the
 * path for larger DBs. Slot lookup is a linear scan (hashing
 * the table is future work).
 * yowza_segdb_init reuses an existing valid heap (magic + bounds
 * check) instead of clearing it, which is what makes
 * host-side heap persistence work. Removed objects leak heap
 * space until re-init on a fresh heap (bump-only, no free list);
 * that is a documented limit, not silent corruption: allocation
 * fails cleanly (-1) when the heap is exhausted.
 *
 * Durability note: the heap store is in wasm linear memory, so
 * sync_obj/sync_dir are no-ops (there is no disk to sync); the
 * three-state publish protocol still holds structurally (this
 * backend never returns -2, like the memory backend). Real
 * durability for wasm segdb is the host persisting the heap
 * region (see segdb.go HeapBytes/RestoreHeap). The hot WAL
 * region is the volatile memory WAL driver (same semantics as
 * the native memory driver): unflushed hot data does not survive
 * a module restart, flushed segments in the heap do.
 *
 * No libc, no malloc: only manual byte loops, so this builds for
 * both wasm targets as well as natively (native builds simply
 * never call these exports). */
#include "segdb.h"
#include "segdb_scan.h"

#ifdef __wasm__
#define SEGDB_EXPORT(name) __attribute__((export_name(name)))
#else
#define SEGDB_EXPORT(name)
#endif

/* ---- heap layout constants ---- */
#define HEAP_MAGIC_0 'Y'
#define HEAP_MAGIC_1 'W'
#define HEAP_MAGIC_2 'H'
#define HEAP_MAGIC_3 'E'
#define HEAP_MAGIC_4 'A'
#define HEAP_MAGIC_5 'P'
#define HEAP_MAGIC_6 '0'
#define HEAP_MAGIC_7 '1'
#define HEAP_TABLE_SLOTS 8192u
#define HEAP_TABLE_SLOTS_LEGACY 4096u
#define HEAP_DATA_START_LEGACY (HEAP_HEADER_SIZE + HEAP_TABLE_SLOTS_LEGACY * HEAP_SLOT_SIZE)
#define HEAP_SLOT_NAME 128u
#define HEAP_SLOT_SIZE (128u + 8u + 8u + 8u) /* 152 */
#define HEAP_HEADER_SIZE 32u
#define HEAP_DATA_START (HEAP_HEADER_SIZE + HEAP_TABLE_SLOTS * HEAP_SLOT_SIZE)

typedef struct {
    uint8_t* heap;
    uint64_t heap_size;
} HeapCtx;

static HeapCtx g_heap_ctx;
/* The store object itself must outlive segdb_init: SegDB keeps a
 * POINTER to it (db->store), so a stack YowzaStore here would
 * dangle the moment init returns (this exact bug segfaulted the
 * native harness and failed wasm flushes). */
static YowzaStore g_seg_store;

/* Gate 3 heap compaction (defined at the end of this file):
 * hp_validate_heap checks every slot before anything moves, so a
 * corrupt slot fails with -1 and moves nothing; hp_compact_internal
 * then compacts live objects and tightens caps to lengths.
 * g_hp_compact_threshold: when > 0, a heap growth that would
 * otherwise succeed first compacts if wasted bytes (bump space
 * minus live bytes) are at or above the threshold; 0 (default)
 * means compact only when an allocation would otherwise fail
 * with OOM. Either way a flush never fails with OOM while
 * reclaimable space exists. */
static int hp_validate_heap(HeapCtx* c, uint64_t* live_out, uint32_t* slots_out);
static int hp_compact_internal(HeapCtx* c, uint64_t* out_reclaimed);
static uint64_t g_hp_compact_threshold = 0;

/* ---- byte helpers ---- */
static uint64_t hp_rd64(const uint8_t* p) {
    uint64_t v = 0;
    int i;
    for (i = 0; i < 8; i++) v |= (uint64_t)p[i] << (i * 8);
    return v;
}

static uint32_t hp_rd32(const uint8_t* p) {
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) |
           ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}

static void hp_wr64(uint8_t* p, uint64_t v) {
    int i;
    for (i = 0; i < 8; i++) p[i] = (uint8_t)((v >> (i * 8)) & 0xFF);
}

static void hp_wr32(uint8_t* p, uint32_t v) {
    p[0] = (uint8_t)(v & 0xFF);
    p[1] = (uint8_t)((v >> 8) & 0xFF);
    p[2] = (uint8_t)((v >> 16) & 0xFF);
    p[3] = (uint8_t)((v >> 24) & 0xFF);
}

static void hp_copy(uint8_t* dst, const uint8_t* src, uint64_t len) {
    uint64_t i;
    for (i = 0; i < len; i++) dst[i] = src[i];
}

static void hp_zero(uint8_t* dst, uint64_t len) {
    uint64_t i;
    for (i = 0; i < len; i++) dst[i] = 0;
}

static int hp_name_eq(const char* a, const uint8_t* b) {
    uint32_t i = 0;
    while (a[i] != '\0') {
        if (a[i] != (char)b[i]) return 0;
        i++;
        if (i >= HEAP_SLOT_NAME) return 0;
    }
    return b[i] == 0;
}

static uint32_t hp_name_len(const char* s) {
    uint32_t n = 0;
    while (s[n] != '\0') {
        n++;
        if (n >= HEAP_SLOT_NAME) return HEAP_SLOT_NAME;
    }
    return n;
}

static uint8_t* hp_slot(HeapCtx* c, uint32_t idx) {
    return c->heap + HEAP_HEADER_SIZE + (uint64_t)idx * HEAP_SLOT_SIZE;
}

/* ---- heap hash index (Reliability stage item 03, Layer 1) ----
 * The old hp_find linearly scanned all 4096 table slots on every
 * read/write/size, and segment accesses repeat that per object,
 * so a scan over S segments paid O(S * 4096) slot comparisons
 * per page. This index maps object name -> slot with open
 * addressing (8192 buckets, FNV-1a), so a hit costs a handful of
 * probes. It is a pure cache, NEVER a source of truth:
 *  - a hit is verified against the heap slot's actual name, so
 *    a wrong slot is never returned;
 *  - a probe never stops early on a non-matching entry (that
 *    entry may be a hash collision, with the wanted entry
 *    further along the chain — an early stale return here was
 *    a real bug: it made colliding lookups report "not found"
 *    and failed a flush once enough objects collided);
 *  - a miss falls back to a full linear scan of the heap; if the
 *    heap contains the object, the index is rebuilt (it was
 *    incomplete/stale) and the heap slot is returned; only a heap
 *    miss returns -1.
 * The index is NOT persisted in the heap bytes: it is rebuilt
 * from the table on every yowza_segdb_init (including the
 * host-restore path, where init reuses a valid heap) and on any
 * detected disagreement. It is updated incrementally on create /
 * publish (rename) / remove. Counters are exposed through
 * yowza_segdb_heap_stats for benchmarks and tests. */
/* Track C: 16384 buckets for up to 8192 slots keeps the
 * load factor at or below 0.5 at full occupancy (the index
 * is rebuilt from the heap on every init and never
 * persisted, so the bucket count is not format). */
#define HP_INDEX_SIZE 16384u
#define HP_INDEX_EMPTY (-1)
#define HP_INDEX_DELETED (-2)
static int g_hp_index[HP_INDEX_SIZE];
static int g_hp_index_valid = 0;
static uint64_t g_hp_lookups = 0;
static uint64_t g_hp_probes = 0;
static uint64_t g_hp_rebuilds = 0;
static uint64_t g_hp_stale = 0;
static uint64_t g_hp_linear_fallbacks = 0;

static uint64_t hp_hash(const char* s) {
    uint64_t h = 14695981039346656037ULL;
    uint32_t i = 0;
    while (s[i] != '\0') {
        h ^= (uint64_t)(uint8_t)s[i];
        h *= 1099511628211ULL;
        i++;
    }
    return h;
}

static void hp_index_insert_slot(HeapCtx* c, uint32_t slot) {
    uint8_t* s = hp_slot(c, slot);
    uint32_t h, i;
    if (s[0] == 0) return;
    h = (uint32_t)(hp_hash((const char*)s) & (HP_INDEX_SIZE - 1));
    for (i = 0; i < HP_INDEX_SIZE; i++) {
        uint32_t b = (h + i) & (HP_INDEX_SIZE - 1);
        int e = g_hp_index[b];
        if (e == HP_INDEX_EMPTY || e == HP_INDEX_DELETED) {
            g_hp_index[b] = (int)slot;
            return;
        }
        if (e >= 0 && hp_name_eq((const char*)s, hp_slot(c, (uint32_t)e))) return; /* already indexed */
    }
}

static void hp_index_rebuild(HeapCtx* c) {
    uint32_t i;
    for (i = 0; i < HP_INDEX_SIZE; i++) g_hp_index[i] = HP_INDEX_EMPTY;
    g_hp_index_valid = 1;
    g_hp_rebuilds++;
    for (i = 0; i < HEAP_TABLE_SLOTS; i++) {
        if (hp_slot(c, i)[0] != 0) hp_index_insert_slot(c, i);
    }
}

static void hp_index_remove_name(HeapCtx* c, const char* obj) {
    uint32_t h, i;
    if (!g_hp_index_valid) return;
    h = (uint32_t)(hp_hash(obj) & (HP_INDEX_SIZE - 1));
    for (i = 0; i < HP_INDEX_SIZE; i++) {
        uint32_t b = (h + i) & (HP_INDEX_SIZE - 1);
        int e = g_hp_index[b];
        if (e == HP_INDEX_EMPTY) return;
        if (e >= 0 && hp_name_eq(obj, hp_slot(c, (uint32_t)e))) {
            g_hp_index[b] = HP_INDEX_DELETED;
            return;
        }
    }
}

/* Index probe: 1 = verified hit (*out_slot set), 0 = miss.
 * A bucket entry naming a different heap slot is NOT treated
 * as stale here: it may be a plain hash collision (a different
 * name sharing this bucket), and the wanted entry may sit
 * further along the same probe chain. The probe therefore
 * keeps going past non-matching entries until an empty
 * bucket (miss) or the verified hit. A genuinely stale entry
 * (pointing at a slot the heap renamed) is likewise skipped,
 * and the miss below falls back to the heap scan, which
 * rebuilds the index when it finds the object — so the heap
 * still wins in every case, just via the fallback instead of
 * an early (unsound) stale return. */
static int hp_index_probe(HeapCtx* c, const char* obj, int* out_slot) {
    uint32_t h, i;
    h = (uint32_t)(hp_hash(obj) & (HP_INDEX_SIZE - 1));
    for (i = 0; i < HP_INDEX_SIZE; i++) {
        uint32_t b = (h + i) & (HP_INDEX_SIZE - 1);
        int e = g_hp_index[b];
        g_hp_probes++;
        if (e == HP_INDEX_EMPTY) return 0;
        if (e >= 0 && hp_name_eq(obj, hp_slot(c, (uint32_t)e))) {
            *out_slot = e;
            return 1;
        }
        /* Non-matching entry (collision or stale) or deleted
         * bucket: keep probing. */
    }
    return 0;
}

static int hp_find_linear(HeapCtx* c, const char* obj) {
    uint32_t i;
    for (i = 0; i < HEAP_TABLE_SLOTS; i++) {
        uint8_t* s = hp_slot(c, i);
        if (s[0] != 0 && hp_name_eq(obj, s)) return (int)i;
    }
    return -1;
}

static int hp_find(HeapCtx* c, const char* obj) {
    int slot = -1;
    if (!obj || obj[0] == '\0') return -1;
    g_hp_lookups++;
    if (!g_hp_index_valid) hp_index_rebuild(c);
    if (hp_index_probe(c, obj, &slot) == 1) return slot;
    /* Index miss: confirm against the heap itself, so an
     * incomplete or stale index can never hide an existing
     * object (or invent one). A heap hit here means the index
     * disagreed with the heap: count it as stale and rebuild. */
    g_hp_linear_fallbacks++;
    slot = hp_find_linear(c, obj);
    if (slot >= 0) {
        g_hp_stale++;
        hp_index_rebuild(c);
    }
    return slot;
}

static uint64_t hp_bump(HeapCtx* c) { return hp_rd64(c->heap + 16); }
static void hp_set_bump(HeapCtx* c, uint64_t v) { hp_wr64(c->heap + 16, v); }

/* Grow a slot's capacity to at least `need` (bump allocation;
 * old data is copied, old region leaks until heap compaction).
 * Gate 3: when growth would fail with OOM, compact the heap once
 * and retry with freshly re-read slot fields (compaction moves
 * objects, so fields read before it are stale); when a threshold
 * is configured and wasted space is at or above it, compact
 * proactively before growing. A failed compaction fails the
 * write (-1) rather than writing into a heap that could not be
 * proven valid. Returns 0/-1. */
/* Growth policy: double the capacity, but if the doubled region does not
 * fit in the free tail while the exact need does, grow exactly to need.
 * Doubling is an amortisation policy, not a correctness requirement; the
 * exact-fit fallback avoids a spurious OOM when the heap is nearly full
 * (observed: a 127 KiB object whose doubled 254 KiB region exceeded a
 * 146 KiB free tail while the 127 KiB need itself fit, after a correct
 * compaction had already reclaimed everything reclaimable). */
static uint64_t hp_grow_cap(uint64_t cap, uint64_t need, uint64_t free) {
    uint64_t ncap = cap == 0 ? 1024 : cap * 2;
    while (ncap < need) ncap *= 2;
    if (ncap > free && need <= free) ncap = need;
    return ncap;
}

static int hp_ensure_cap(HeapCtx* c, uint32_t idx, uint64_t need) {
    uint8_t* s = hp_slot(c, idx);
    uint64_t off = hp_rd64(s + 128);
    uint64_t len = hp_rd64(s + 136);
    uint64_t cap = hp_rd64(s + 144);
    uint64_t ncap, bump, free;
    if (cap >= need) return 0;
    bump = hp_bump(c);
    free = bump <= c->heap_size ? c->heap_size - bump : 0;
    ncap = hp_grow_cap(cap, need, free);
    if (bump > c->heap_size || ncap > free) {
        if (hp_compact_internal(c, NULL) != 0) return -1;
        s = hp_slot(c, idx);
        off = hp_rd64(s + 128);
        len = hp_rd64(s + 136);
        cap = hp_rd64(s + 144);
        if (cap >= need) return 0;
        bump = hp_bump(c);
        free = bump <= c->heap_size ? c->heap_size - bump : 0;
        ncap = hp_grow_cap(cap, need, free);
        if (bump > c->heap_size || ncap > free) return -1;
    } else if (g_hp_compact_threshold > 0) {
        uint64_t live = 0;
        uint32_t nslots = 0;
        if (hp_validate_heap(c, &live, &nslots) != 0) return -1;
        if (bump >= HEAP_DATA_START && bump - HEAP_DATA_START >= live &&
            bump - HEAP_DATA_START - live >= g_hp_compact_threshold) {
            if (hp_compact_internal(c, NULL) != 0) return -1;
            s = hp_slot(c, idx);
            off = hp_rd64(s + 128);
            len = hp_rd64(s + 136);
            cap = hp_rd64(s + 144);
            if (cap >= need) return 0;
            bump = hp_bump(c);
            free = bump <= c->heap_size ? c->heap_size - bump : 0;
            ncap = hp_grow_cap(cap, need, free);
            if (bump > c->heap_size || ncap > free) return -1;
        }
    }
    if (len > 0 && off != 0) hp_copy(c->heap + bump, c->heap + off, len);
    hp_wr64(s + 128, bump);
    hp_wr64(s + 144, ncap);
    hp_set_bump(c, bump + ncap);
    return 0;
}

/* ---- YowzaStoreVTable over the heap ---- */
static int hp_write_at(void* vs, const char* obj, uint64_t off, const uint8_t* data, uint64_t len) {
    HeapCtx* c = (HeapCtx*)vs;
    int idx;
    uint8_t* s;
    uint64_t slen, nlen;
    if (!c || !c->heap || !obj) return -1;
    if (len > 0 && !data) return -1;
    idx = hp_find(c, obj);
    if (idx < 0) {
        uint32_t i;
        idx = -1;
        for (i = 0; i < HEAP_TABLE_SLOTS; i++) {
            if (hp_slot(c, i)[0] == 0) { idx = (int)i; break; }
        }
        if (idx < 0) return -1;
        s = hp_slot(c, (uint32_t)idx);
        hp_zero(s, HEAP_SLOT_SIZE);
        {
            uint32_t n = hp_name_len(obj), j;
            if (n >= HEAP_SLOT_NAME) return -1;
            for (j = 0; j < n; j++) s[j] = (uint8_t)obj[j];
            s[n] = 0;
            hp_index_insert_slot(c, (uint32_t)idx);
        }
    } else {
        s = hp_slot(c, (uint32_t)idx);
    }
    if (off > c->heap_size || len > c->heap_size - off) return -1;
    nlen = off + len;
    if (hp_ensure_cap(c, (uint32_t)idx, nlen) != 0) return -1;
    s = hp_slot(c, (uint32_t)idx);
    if (len > 0) hp_copy(c->heap + hp_rd64(s + 128) + off, data, len);
    slen = hp_rd64(s + 136);
    if (nlen > slen) hp_wr64(s + 136, nlen);
    return 0;
}

static int hp_read_at(void* vs, const char* obj, uint64_t off, uint8_t* out, uint64_t len) {
    HeapCtx* c = (HeapCtx*)vs;
    int idx;
    uint8_t* s;
    uint64_t slen;
    if (!c || !c->heap || !obj || !out) return -1;
    idx = hp_find(c, obj);
    if (idx < 0) return -1;
    s = hp_slot(c, (uint32_t)idx);
    slen = hp_rd64(s + 136);
    if (off > slen || len > slen - off) return -1;
    if (len > 0) hp_copy(out, c->heap + hp_rd64(s + 128) + off, len);
    return 0;
}

static int hp_size(void* vs, const char* obj, uint64_t* out_size) {
    HeapCtx* c = (HeapCtx*)vs;
    int idx;
    if (!c || !c->heap || !obj || !out_size) return -1;
    idx = hp_find(c, obj);
    if (idx < 0) return -1;
    *out_size = hp_rd64(hp_slot(c, (uint32_t)idx) + 136);
    return 0;
}

static int hp_size_status(void* vs, const char* obj, uint64_t* out_size) {
    HeapCtx* c = (HeapCtx*)vs;
    int idx;
    if (!c || !c->heap || !obj) return YOWZA_STORE_IO_ERROR;
    idx = hp_find(c, obj);
    if (idx < 0) return YOWZA_STORE_NOT_FOUND;
    if (out_size) *out_size = hp_rd64(hp_slot(c, (uint32_t)idx) + 136);
    return YOWZA_STORE_OK;
}

static int hp_sync_obj(void* vs, const char* obj) {
    (void)vs; (void)obj;
    return 0; /* linear memory: no disk sync exists */
}

static int hp_sync_dir(void* vs) {
    (void)vs;
    return 0;
}

static int hp_publish(void* vs, const char* tmp_obj, const char* final_obj) {
    HeapCtx* c = (HeapCtx*)vs;
    int tidx, fidx;
    uint8_t* s;
    uint32_t n, j;
    if (!c || !c->heap || !tmp_obj || !final_obj) return -1;
    tidx = hp_find(c, tmp_obj);
    if (tidx < 0) return -1;
    fidx = hp_find(c, final_obj);
    if (fidx >= 0 && fidx != tidx) {
        uint8_t* f = hp_slot(c, (uint32_t)fidx);
        hp_index_remove_name(c, final_obj);
        hp_zero(f, HEAP_SLOT_SIZE); /* replaced object leaks heap */
    }
    s = hp_slot(c, (uint32_t)tidx);
    n = hp_name_len(final_obj);
    if (n >= HEAP_SLOT_NAME) return -1;
    hp_index_remove_name(c, tmp_obj);
    hp_zero(s, HEAP_SLOT_NAME);
    for (j = 0; j < n; j++) s[j] = (uint8_t)final_obj[j];
    s[n] = 0;
    hp_index_insert_slot(c, (uint32_t)tidx);
    return 0;
}

static int hp_remove(void* vs, const char* obj) {
    HeapCtx* c = (HeapCtx*)vs;
    int idx;
    if (!c || !c->heap || !obj) return -1;
    idx = hp_find(c, obj);
    if (idx < 0) return -1;
    hp_index_remove_name(c, obj);
    hp_zero(hp_slot(c, (uint32_t)idx), HEAP_SLOT_SIZE);
    return 0;
}

static int hp_truncate(void* vs, const char* obj, uint64_t len) {
    HeapCtx* c = (HeapCtx*)vs;
    int idx;
    uint8_t* s;
    uint64_t slen, off;
    if (!c || !c->heap || !obj) return -1;
    idx = hp_find(c, obj);
    if (idx < 0) return -1;
    s = hp_slot(c, (uint32_t)idx);
    slen = hp_rd64(s + 136);
    if (len < slen) {
        hp_wr64(s + 136, len);
        return 0;
    }
    if (len == slen) return 0;
    if (hp_ensure_cap(c, (uint32_t)idx, len) != 0) return -1;
    s = hp_slot(c, (uint32_t)idx);
    off = hp_rd64(s + 128);
    hp_zero(c->heap + off + slen, len - slen);
    hp_wr64(s + 136, len);
    return 0;
}

static void hp_close(void* vs) { (void)vs; }

/* Heap-store enumeration (parallel-timeline patch: this was the
 * missing piece that made O2's recover wiring fail on
 * legitimately virgin heap stores). A live slot is exactly a
 * slot whose first name byte is non-zero — the same invariant
 * hp_index_rebuild uses; freed slots are zeroed whole, and
 * published names are NUL-terminated within HEAP_SLOT_NAME, so
 * the slot bytes are a valid C string. Convention matches
 * mem_enumerate: 0 = walk completed, 1 = callback stopped it
 * early, -1 = bad arguments. The walk is read-only and does not
 * touch the hash index, so it is safe on a host-restored heap
 * even before any lookup rebuilds the index. With this hook,
 * yowza_store_detect_prior_content can prove a heap store
 * virgin (no objects) versus triple-loss (segment/manifest
 * objects survive without pointers) — without it, O2's
 * fail-on-ambiguous rule wrongly failed every genuinely empty
 * shard (empty flush publishes nothing, so its heap is virgin). */
static int hp_enumerate(void* vs, int (*cb)(void* ctx, const char* name), void* ctx) {
    HeapCtx* c = (HeapCtx*)vs;
    uint32_t i;
    if (!c || !c->heap || !cb) return -1;
    for (i = 0; i < HEAP_TABLE_SLOTS; i++) {
        uint8_t* s = hp_slot(c, i);
        if (s[0] != 0) {
            if (cb(ctx, (const char*)s) != 0) return 1;
        }
    }
    return 0;
}

static const YowzaStoreVTable hp_vtable = {
    hp_write_at, hp_read_at, hp_size, hp_sync_obj, hp_sync_dir,
    hp_publish, hp_remove, hp_truncate, hp_close, hp_size_status,
    hp_enumerate,
};

/* Track C legacy-heap migration (4096 -> 8192 slots).
 * Returns 0 when the buffer held a VALID legacy heap and it
 * was migrated in place, 1 when the buffer is not a valid
 * legacy heap (caller falls through to the normal path),
 * -1 when it IS a valid legacy heap but there is no room
 * to migrate (caller must fail init, never wipe it).
 * The buffer is untouched on 1 and -1. Slot indices are
 * identical in both layouts (the table starts at offset 32
 * in both), so only the data region shifts right by one
 * old table size and each live slot's offset grows by the
 * same shift; the new slots (4096..8191) are zeroed. */
static int hp_migrate_legacy_heap(uint8_t* heap_buf, uint64_t heap_size) {
    uint64_t bump, shift, new_bump, i;
    if (!heap_buf) return 1;
    if (heap_buf[0] != HEAP_MAGIC_0 || heap_buf[1] != HEAP_MAGIC_1 ||
        heap_buf[2] != HEAP_MAGIC_2 || heap_buf[3] != HEAP_MAGIC_3 ||
        heap_buf[4] != HEAP_MAGIC_4 || heap_buf[5] != HEAP_MAGIC_5 ||
        heap_buf[6] != HEAP_MAGIC_6 || heap_buf[7] != HEAP_MAGIC_7) return 1;
    if (hp_rd32(heap_buf + 8) != HEAP_TABLE_SLOTS_LEGACY) return 1;
    bump = hp_rd64(heap_buf + 16);
    if (bump < HEAP_DATA_START_LEGACY || bump > heap_size) return 1;
    for (i = 0; i < HEAP_TABLE_SLOTS_LEGACY; i++) {
        uint8_t* s = heap_buf + HEAP_HEADER_SIZE + i * HEAP_SLOT_SIZE;
        uint64_t off, len, cap;
        if (s[0] == 0) continue;
        off = hp_rd64(s + 128);
        len = hp_rd64(s + 136);
        cap = hp_rd64(s + 144);
        if (off < HEAP_DATA_START_LEGACY || len > cap ||
            cap > heap_size - off) return 1;
    }
    shift = (uint64_t)HEAP_DATA_START - (uint64_t)HEAP_DATA_START_LEGACY;
    new_bump = bump + shift;
    if (new_bump > heap_size) return -1;
    if (bump > HEAP_DATA_START_LEGACY) {
        uint64_t p = bump;
        while (p > HEAP_DATA_START_LEGACY) {
            p--;
            heap_buf[p + shift] = heap_buf[p];
        }
    }
    for (i = 0; i < HEAP_TABLE_SLOTS_LEGACY; i++) {
        uint8_t* s = heap_buf + HEAP_HEADER_SIZE + i * HEAP_SLOT_SIZE;
        uint64_t off;
        if (s[0] == 0) continue;
        off = hp_rd64(s + 128);
        hp_wr64(s + 128, off + shift);
    }
    hp_zero(heap_buf + HEAP_HEADER_SIZE +
                (uint64_t)HEAP_TABLE_SLOTS_LEGACY * HEAP_SLOT_SIZE,
            (uint64_t)(HEAP_TABLE_SLOTS - HEAP_TABLE_SLOTS_LEGACY) *
                HEAP_SLOT_SIZE);
    hp_wr32(heap_buf + 8, HEAP_TABLE_SLOTS);
    hp_wr64(heap_buf + 16, new_bump);
    return 0;
}

/* Validate a heap header/table found in memory (reuse path). */
static int hp_validate(HeapCtx* c) {
    uint32_t i;
    uint64_t bump;
    if (c->heap[0] != HEAP_MAGIC_0 || c->heap[1] != HEAP_MAGIC_1 ||
        c->heap[2] != HEAP_MAGIC_2 || c->heap[3] != HEAP_MAGIC_3 ||
        c->heap[4] != HEAP_MAGIC_4 || c->heap[5] != HEAP_MAGIC_5 ||
        c->heap[6] != HEAP_MAGIC_6 || c->heap[7] != HEAP_MAGIC_7) return -1;
    if (hp_rd32(c->heap + 8) != HEAP_TABLE_SLOTS) return -1;
    bump = hp_bump(c);
    if (bump < HEAP_DATA_START || bump > c->heap_size) return -1;
    for (i = 0; i < HEAP_TABLE_SLOTS; i++) {
        uint8_t* s = hp_slot(c, i);
        uint64_t off, len, cap;
        if (s[0] == 0) continue;
        off = hp_rd64(s + 128);
        len = hp_rd64(s + 136);
        cap = hp_rd64(s + 144);
        if (off < HEAP_DATA_START || off > c->heap_size) return -1;
        if (len > cap || cap > c->heap_size - off) return -1;
    }
    return 0;
}

/* ---- wasm exports ---- */

SEGDB_EXPORT("yowza_segdb_state_size")
uint64_t yowza_segdb_state_size(void) {
    return (uint64_t)sizeof(SegDB);
}

SEGDB_EXPORT("yowza_segdb_init")
int yowza_segdb_init(SegDB* state, uint8_t* arena_buf, uint64_t arena_size,
                     uint8_t* wal_buf, uint32_t wal_size,
                     uint8_t* heap_buf, uint64_t heap_size) {
    if (!state || !arena_buf || arena_size == 0 || !wal_buf || wal_size == 0 ||
        !heap_buf || heap_size < HEAP_DATA_START + 1024) return -1;
    g_heap_ctx.heap = heap_buf;
    g_heap_ctx.heap_size = heap_size;
    if (hp_validate(&g_heap_ctx) != 0) {
        /* Track C: a valid legacy (4096-slot) heap migrates
         * in place instead of being wiped; a legacy heap with
         * no room to migrate fails init loudly (-1). Only a
         * buffer that is neither valid-current nor
         * valid-legacy takes the fresh/corrupt path below. */
        int mig = hp_migrate_legacy_heap(heap_buf, heap_size);
        if (mig < 0) return -1;
        if (mig == 0) {
            if (hp_validate(&g_heap_ctx) != 0) return -1;
        } else {
        /* Fresh heap (or corrupt header): initialise empty. A
         * corrupt header is treated as fresh, matching the
         * host-restore contract (the host only writes back heaps
         * it previously read from a valid init). */
        uint32_t i;
        hp_zero(heap_buf, HEAP_HEADER_SIZE + (uint64_t)HEAP_TABLE_SLOTS * HEAP_SLOT_SIZE);
        heap_buf[0] = HEAP_MAGIC_0; heap_buf[1] = HEAP_MAGIC_1;
        heap_buf[2] = HEAP_MAGIC_2; heap_buf[3] = HEAP_MAGIC_3;
        heap_buf[4] = HEAP_MAGIC_4; heap_buf[5] = HEAP_MAGIC_5;
        heap_buf[6] = HEAP_MAGIC_6; heap_buf[7] = HEAP_MAGIC_7;
        hp_wr32(heap_buf + 8, HEAP_TABLE_SLOTS);
        hp_wr64(heap_buf + 16, HEAP_DATA_START);
        hp_wr64(heap_buf + 24, 0);
        for (i = 0; i < HEAP_TABLE_SLOTS; i++) hp_zero(hp_slot(&g_heap_ctx, i), HEAP_SLOT_SIZE);
        }
    }
    /* (Re)build the hash index from the heap table: this is what
     * makes a host-restored heap (valid header, reused by init)
     * searchable without carrying any index bytes in the heap. */
    hp_index_rebuild(&g_heap_ctx);
    g_seg_store.vt = &hp_vtable;
    g_seg_store.state = &g_heap_ctx;
    return segdb_init(state, &g_seg_store, arena_buf, (size_t)arena_size, wal_buf, wal_size);
}

SEGDB_EXPORT("yowza_segdb_recover")
int yowza_segdb_recover(SegDB* state) {
    if (!state) return -1;
    return segdb_recover(state);
}

SEGDB_EXPORT("yowza_segdb_put")
int yowza_segdb_put(SegDB* state, const uint8_t* key, uint32_t key_len,
                    const uint8_t* val, uint32_t val_len) {
    if (!state || !key || key_len == 0) return -1;
    return segdb_put(state, key, key_len, val, val_len);
}

SEGDB_EXPORT("yowza_segdb_get")
int yowza_segdb_get(SegDB* state, const uint8_t* key, uint32_t key_len,
                    uint8_t* out, uint32_t out_cap, uint32_t* out_len) {
    uint8_t* v = 0;
    uint32_t vlen = 0;
    uint64_t i;
    int grc;
    if (!state || !key || key_len == 0 || !out || !out_len) return -1;
    grc = segdb_get(state, key, key_len, &v, &vlen);
    if (grc != 0) return grc; /* pass -4 lookup errors through; -1 stays absence */
    if (vlen > out_cap) return -4; /* value overflows the caller buffer: error, not absence */
    for (i = 0; i < vlen; i++) out[i] = v[i];
    *out_len = vlen;
    return 0;
}

SEGDB_EXPORT("yowza_segdb_delete")
int yowza_segdb_delete(SegDB* state, const uint8_t* key, uint32_t key_len) {
    if (!state || !key || key_len == 0) return -1;
    return segdb_delete(state, key, key_len);
}

SEGDB_EXPORT("yowza_segdb_flush")
int yowza_segdb_flush(SegDB* state) {
    if (!state) return -1;
    return segdb_flush(state);
}

SEGDB_EXPORT("yowza_segdb_compact")
int yowza_segdb_compact(SegDB* state, uint64_t drop_barrier) {
    if (!state) return -1;
    return segdb_compact(state, drop_barrier);
}

SEGDB_EXPORT("yowza_segdb_scan")
int yowza_segdb_scan(SegDB* state, const uint8_t* prefix, uint32_t prefix_len,
                     uint64_t snapshot_seq, uint8_t* out, uint32_t out_cap,
                     uint32_t* count_out) {
    if (!state || !out || !count_out) return -1;
    if (prefix_len > 0 && !prefix) return -1;
    return segdb_scan(state, prefix, prefix_len, snapshot_seq, out, out_cap, count_out);
}

SEGDB_EXPORT("yowza_segdb_reconcile")
int yowza_segdb_reconcile(SegDB* state) {
    if (!state) return -1;
    return segdb_reconcile(state);
}

SEGDB_EXPORT("yowza_segdb_indeterminate")
int yowza_segdb_indeterminate(SegDB* state) {
    if (!state) return -1;
    return segdb_indeterminate(state);
}

SEGDB_EXPORT("yowza_segdb_segment_count")
int yowza_segdb_segment_count(SegDB* state) {
    if (!state) return -1;
    /* Last group's segment count (== total in single-manifest
     * mode; in directory mode use group_count + scan to see the
     * whole DB). */
    return (int)state->manifest.segment_count;
}

SEGDB_EXPORT("yowza_segdb_has_dir")
int yowza_segdb_has_dir(SegDB* state) {
    if (!state) return -1;
    return state->has_dir ? 1 : 0;
}

SEGDB_EXPORT("yowza_segdb_group_count")
int yowza_segdb_group_count(SegDB* state) {
    if (!state) return -1;
    return (int)state->group_count;
}


/* ---- deep-integration exports (appended; existing exports above
 * are untouched) ---- */

SEGDB_EXPORT("yowza_segdb_snapshot_seq")
uint64_t yowza_segdb_snapshot_seq(SegDB* state) {
    if (!state) return 0;
    return segdb_snapshot_seq(state);
}

SEGDB_EXPORT("yowza_segdb_scan_page")
int yowza_segdb_scan_page(SegDB* state, const uint8_t* prefix, uint32_t prefix_len,
                          uint64_t snapshot_seq, uint64_t skip, uint32_t limit,
                          uint8_t* out, uint32_t out_cap, uint32_t* count_out) {
    if (!state || !out || !count_out) return -1;
    if (prefix_len > 0 && !prefix) return -1;
    return segdb_scan_page(state, prefix, prefix_len, snapshot_seq, skip, limit,
                           out, out_cap, count_out);
}

/* ---- Reliability stage item 03 exports (appended) ---- */
/* Heap-index stats, 6 u64 LE values: lookups, probes,
 * rebuilds, stale_rebuilds, linear_fallbacks, index_valid (0/1).
 * Probes count index buckets examined (a hit on an existing
 * object is typically 1-3); the old linear scan examined up to
 * 4096 slots per lookup, so probes/lookups is the honest
 * work-reduction metric for Layer 1. */
SEGDB_EXPORT("yowza_segdb_heap_stats")
void yowza_segdb_heap_stats(uint8_t* out) {
    uint64_t v[6];
    int i, j;
    if (!out) return;
    v[0] = g_hp_lookups; v[1] = g_hp_probes; v[2] = g_hp_rebuilds;
    v[3] = g_hp_stale; v[4] = g_hp_linear_fallbacks;
    v[5] = g_hp_index_valid ? 1 : 0;
    for (i = 0; i < 6; i++)
        for (j = 0; j < 8; j++) out[i * 8 + j] = (uint8_t)((v[i] >> (j * 8)) & 0xFF);
}

SEGDB_EXPORT("yowza_segdb_heap_stats_reset")
void yowza_segdb_heap_stats_reset(void) {
    g_hp_lookups = 0; g_hp_probes = 0; g_hp_rebuilds = 0;
    g_hp_stale = 0; g_hp_linear_fallbacks = 0;
}

/* Rebuild the hash index from the current heap table. For hosts
 * that restore heap bytes in place without re-running init. */
SEGDB_EXPORT("yowza_segdb_heap_index_rebuild")
int yowza_segdb_heap_index_rebuild(void) {
    if (!g_heap_ctx.heap) return -1;
    hp_index_rebuild(&g_heap_ctx);
    return 0;
}

/* TEST-ONLY: deliberately corrupt the in-memory index entry for
 * `obj` so it points at a different occupied slot. The next
 * lookup of obj must still return obj's own heap slot (heap
 * wins, index rebuilt) — never the corrupted slot's data. This
 * corrupts only the volatile index, never heap bytes. */
SEGDB_EXPORT("yowza_segdb_heap_index_corrupt_for_test")
int yowza_segdb_heap_index_corrupt_for_test(const char* obj) {
    int slot, other = -1;
    uint32_t h, i, j;
    if (!g_heap_ctx.heap || !obj || !g_hp_index_valid) return -1;
    slot = hp_find(&g_heap_ctx, obj);
    if (slot < 0) return -1;
    for (j = 0; j < HEAP_TABLE_SLOTS; j++) {
        if ((int)j != slot && hp_slot(&g_heap_ctx, j)[0] != 0) { other = (int)j; break; }
    }
    if (other < 0) return -1;
    h = (uint32_t)(hp_hash(obj) & (HP_INDEX_SIZE - 1));
    for (i = 0; i < HP_INDEX_SIZE; i++) {
        uint32_t b = (h + i) & (HP_INDEX_SIZE - 1);
        int e = g_hp_index[b];
        if (e == HP_INDEX_EMPTY) return -1;
        if (e == slot) {
            g_hp_index[b] = (int)other;
            return 0;
        }
    }
    return -1;
}

/* TEST-ONLY: drop the in-memory index entry for `obj` without
 * touching heap bytes. The next lookup must still find obj's
 * heap slot via the linear fallback (heap wins, index rebuilt)
 * — never report it missing. */
SEGDB_EXPORT("yowza_segdb_heap_index_drop_for_test")
int yowza_segdb_heap_index_drop_for_test(const char* obj) {
    uint32_t h, i;
    if (!g_heap_ctx.heap || !obj || !g_hp_index_valid) return -1;
    if (hp_find(&g_heap_ctx, obj) < 0) return -1;
    h = (uint32_t)(hp_hash(obj) & (HP_INDEX_SIZE - 1));
    for (i = 0; i < HP_INDEX_SIZE; i++) {
        uint32_t b = (h + i) & (HP_INDEX_SIZE - 1);
        int e = g_hp_index[b];
        if (e == HP_INDEX_EMPTY) return -1;
        if (e >= 0 && hp_name_eq(obj, hp_slot(&g_heap_ctx, (uint32_t)e))) {
            g_hp_index[b] = HP_INDEX_DELETED;
            return 0;
        }
    }
    return -1;
}

/* Scan stats, 6 u64 LE values: page_iter_visits (old skip path),
 * cursor_iter_visits (new cursor path), cursor_block_skips
 * (in-memory prime-parse skips, see segdb.h), seek_index_entries,
 * cache_hits, cache_misses. See segdb_scan_stats_get. */
SEGDB_EXPORT("yowza_segdb_scan_stats")
void yowza_segdb_scan_stats(uint8_t* out) {
    SegdbScanStats st;
    int i, j;
    uint64_t v[6];
    if (!out) return;
    segdb_scan_stats_get(&st);
    v[0] = st.page_visits; v[1] = st.cursor_visits;
    v[2] = st.cursor_block_skips; v[3] = st.seek_index_entries;
    v[4] = st.cache_hits; v[5] = st.cache_misses;
    for (i = 0; i < 6; i++)
        for (j = 0; j < 8; j++) out[i * 8 + j] = (uint8_t)((v[i] >> (j * 8)) & 0xFF);
}

SEGDB_EXPORT("yowza_segdb_scan_stats_reset")
void yowza_segdb_scan_stats_reset(void) {
    segdb_scan_stats_reset();
}

SEGDB_EXPORT("yowza_segdb_scan_cursor")
int yowza_segdb_scan_cursor(SegDB* state, const uint8_t* prefix, uint32_t prefix_len,
                            uint64_t snapshot_seq,
                            const uint8_t* cursor_key, uint32_t cursor_len, uint32_t has_cursor,
                            uint32_t limit,
                            uint8_t* out, uint32_t out_cap, uint32_t* count_out,
                            uint8_t* next_key, uint32_t next_key_cap, uint32_t* next_len_out,
                            uint32_t* has_more_out) {
    if (!state || !out || !count_out || !next_len_out || !has_more_out) return -1;
    if (prefix_len > 0 && !prefix) return -1;
    if (has_cursor && cursor_len > 0 && !cursor_key) return -1;
    return segdb_scan_cursor(state, prefix, prefix_len, snapshot_seq,
                             cursor_key, cursor_len, has_cursor != 0, limit,
                             out, out_cap, count_out,
                             next_key, next_key_cap, next_len_out, has_more_out);
}

/* ---- Gate 3: heap compaction + orphan reclamation ----
 * Bump allocation leaks space: removing an object frees its slot
 * but not its bytes, and replacing an object (publish) or growing
 * one (ensure_cap) abandons the old region. Without compaction a
 * DB with few live objects exhausts its heap after repeated
 * flush/compact/delete cycles. Compaction here is validate-then-
 * move: hp_validate_heap proves every slot's range (and that
 * allocated intervals do not overlap) BEFORE any byte moves, so a
 * corrupt slot fails with -1 and leaves every live object exactly
 * where it was. Only then does hp_compact_internal pack live
 * objects down from HEAP_DATA_START in offset order (destinations
 * never exceed sources, so the forward copy is safe), tighten each
 * cap to its length, zero the freed tail, publish the new bump,
 * and rebuild the name index from the table — the index stays a
 * temporary cache, never the source of truth (a rebuild from the
 * table is the last step, so a stale index can never survive a
 * compaction).
 * Nothing outside the heap store references heap offsets: segments
 * and manifests are found by object NAME through the store
 * vtable, and segdb re-reads slot offsets on every access, so
 * moving objects between vtable calls is transparent to segdb.
 * Orphan reclamation (hp_reclaim) deletes only objects proven
 * unreferenced: it first validates the heap, then builds the
 * reference set from CURRENT (+ its generation-1 fallback
 * manifest), CURRENT.bak, the directory and its groups, and the
 * in-memory SegDB state (current + pending manifests and group
 * lists). Anything not in that set is deleted, then the heap is
 * compacted. A missing/corrupt CURRENT with live objects, or a
 * corrupt referenced manifest, fails closed (-1) with nothing
 * deleted. Unreferenced manifests from crashed publishes are
 * safe to delete: recovery only ever falls back to generation-1,
 * and reconcile decides from CURRENT + the in-memory pending
 * manifest struct, not from an unreferenced manifest object. */

/* Sort order for allocated slots (by offset), for validation. */
static uint32_t g_hp_order[HEAP_TABLE_SLOTS];

static int hp_validate_heap(HeapCtx* c, uint64_t* live_out, uint32_t* slots_out) {
    uint32_t i, n = 0, nalloc = 0;
    uint64_t bump, live = 0;
    if (!c || !c->heap) return -1;
    if (c->heap[0] != HEAP_MAGIC_0 || c->heap[1] != HEAP_MAGIC_1 ||
        c->heap[2] != HEAP_MAGIC_2 || c->heap[3] != HEAP_MAGIC_3 ||
        c->heap[4] != HEAP_MAGIC_4 || c->heap[5] != HEAP_MAGIC_5 ||
        c->heap[6] != HEAP_MAGIC_6 || c->heap[7] != HEAP_MAGIC_7) return -1;
    if (hp_rd32(c->heap + 8) != HEAP_TABLE_SLOTS) return -1;
    bump = hp_bump(c);
    if (bump < HEAP_DATA_START || bump > c->heap_size) return -1;
    for (i = 0; i < HEAP_TABLE_SLOTS; i++) {
        uint8_t* s = hp_slot(c, i);
        uint64_t off, len, cap;
        uint32_t j;
        int has_nul = 0;
        if (s[0] == 0) continue;
        n++;
        for (j = 0; j < HEAP_SLOT_NAME; j++) {
            if (s[j] == 0) { has_nul = 1; break; }
        }
        if (!has_nul) return -1; /* unterminated name: corrupt */
        off = hp_rd64(s + 128);
        len = hp_rd64(s + 136);
        cap = hp_rd64(s + 144);
        if (len == 0 && cap == 0) continue; /* never allocated: no bytes */
        if (off < HEAP_DATA_START || off > c->heap_size) return -1;
        if (len > cap || cap > c->heap_size - off) return -1;
        if (off + cap > bump) return -1; /* outside the bump region */
        live += len;
        g_hp_order[nalloc++] = i;
    }
    /* Insertion sort by offset (allocated slots only), then the
     * overlap check: intervals [off, off+cap) must not overlap. */
    for (i = 1; i < nalloc; i++) {
        uint32_t v = g_hp_order[i];
        uint64_t voff = hp_rd64(hp_slot(c, v) + 128);
        uint32_t k = i;
        while (k > 0) {
            uint64_t poff = hp_rd64(hp_slot(c, g_hp_order[k - 1]) + 128);
            if (poff <= voff) break;
            g_hp_order[k] = g_hp_order[k - 1];
            k--;
        }
        g_hp_order[k] = v;
    }
    for (i = 1; i < nalloc; i++) {
        uint8_t* p = hp_slot(c, g_hp_order[i - 1]);
        uint8_t* q = hp_slot(c, g_hp_order[i]);
        if (hp_rd64(q + 128) < hp_rd64(p + 128) + hp_rd64(p + 144)) return -1;
    }
    if (live_out) *live_out = live;
    if (slots_out) *slots_out = n;
    return 0;
}

static int hp_compact_internal(HeapCtx* c, uint64_t* out_reclaimed) {
    uint64_t old_bump, new_bump, i;
    uint32_t nalloc = 0, k;
    if (hp_validate_heap(c, NULL, NULL) != 0) return -1;
    /* Re-derive the offset order (validate already sorted
     * g_hp_order, but recompute the count the same way so this
     * function does not depend on validate's side effects). */
    for (i = 0; i < HEAP_TABLE_SLOTS; i++) {
        uint8_t* s = hp_slot(c, (uint32_t)i);
        if (s[0] == 0) continue;
        if (hp_rd64(s + 136) == 0 && hp_rd64(s + 144) == 0) continue;
        g_hp_order[nalloc++] = (uint32_t)i;
    }
    /* g_hp_order is already in offset order from validation (same
     * scan, same sort); re-sort defensively in case validate's
     * order is ever separated from this function. */
    for (k = 1; k < nalloc; k++) {
        uint32_t v = g_hp_order[k];
        uint64_t voff = hp_rd64(hp_slot(c, v) + 128);
        uint32_t j = k;
        while (j > 0) {
            uint64_t poff = hp_rd64(hp_slot(c, g_hp_order[j - 1]) + 128);
            if (poff <= voff) break;
            g_hp_order[j] = g_hp_order[j - 1];
            j--;
        }
        g_hp_order[j] = v;
    }
    old_bump = hp_bump(c);
    new_bump = HEAP_DATA_START;
    for (k = 0; k < nalloc; k++) {
        uint8_t* s = hp_slot(c, g_hp_order[k]);
        uint64_t off = hp_rd64(s + 128);
        uint64_t len = hp_rd64(s + 136);
        if (len > 0 && off != new_bump) hp_copy(c->heap + new_bump, c->heap + off, len);
        hp_wr64(s + 128, new_bump);
        hp_wr64(s + 144, len); /* tighten cap to length */
        new_bump += len;
    }
    if (old_bump > new_bump) hp_zero(c->heap + new_bump, old_bump - new_bump);
    hp_set_bump(c, new_bump);
    hp_index_rebuild(c);
    if (out_reclaimed) *out_reclaimed = old_bump - new_bump;
    return 0;
}

/* ---- orphan reference set ---- */
#define HP_REF_ARENA 131072u
#define HP_REF_MAX 8192u
static uint8_t g_hp_ref_names[HP_REF_ARENA];
static uint32_t g_hp_ref_off[HP_REF_MAX];
static uint32_t g_hp_ref_count = 0;
static uint32_t g_hp_ref_bytes = 0;
static uint64_t g_hp_reclaim_deleted = 0;
static uint64_t g_hp_reclaim_bytes = 0;
static Manifest g_hp_ref_manifest;
static ManifestDir g_hp_ref_dir;

static int hp_ref_has(const char* name) {
    uint32_t i;
    for (i = 0; i < g_hp_ref_count; i++) {
        if (hp_name_eq(name, &g_hp_ref_names[g_hp_ref_off[i]])) return 1;
    }
    return 0;
}

static int hp_ref_add(const char* name) {
    uint32_t n, j;
    if (!name || name[0] == '\0') return -1;
    if (hp_ref_has(name)) return 0;
    if (g_hp_ref_count >= HP_REF_MAX) return -1;
    n = hp_name_len(name);
    if (n >= HEAP_SLOT_NAME) return -1;
    if (g_hp_ref_bytes + n + 1 > HP_REF_ARENA) return -1;
    g_hp_ref_off[g_hp_ref_count] = g_hp_ref_bytes;
    for (j = 0; j < n; j++) g_hp_ref_names[g_hp_ref_bytes + j] = (uint8_t)name[j];
    g_hp_ref_names[g_hp_ref_bytes + n] = 0;
    g_hp_ref_bytes += n + 1;
    g_hp_ref_count++;
    return 0;
}

static int hp_ref_add_manifest_segments(const Manifest* m) {
    uint32_t i;
    for (i = 0; i < m->segment_count; i++) {
        if (hp_ref_add(m->segments[i].obj) != 0) return -1;
    }
    return 0;
}

/* Add a manifest object + its segments, or a directory + its
 * groups' manifests + their segments, to the reference set.
 * Returns 0/-1 (a referenced object that cannot be read fails
 * the whole reclaim: fail closed, delete nothing). */
static int hp_ref_add_chain(const char* obj) {
    char cur[MANIFEST_MAX_OBJ_LEN + 1];
    uint32_t n, j;
    if (!obj || obj[0] == '\0') return -1;
    n = hp_name_len(obj);
    if (n > MANIFEST_MAX_OBJ_LEN) return -1;
    for (j = 0; j < n; j++) cur[j] = obj[j];
    cur[n] = 0;
    if (hp_ref_add(cur) != 0) return -1;
    if (hp_name_eq("manifest.dir", (const uint8_t*)cur)) {
        uint32_t gi;
        if (manifest_dir_read(&g_seg_store, cur, &g_hp_ref_dir) != 0) return -1;
        for (gi = 0; gi < g_hp_ref_dir.group_count; gi++) {
            const char* gobj = g_hp_ref_dir.groups[gi];
            if (hp_ref_add(gobj) != 0) return -1;
            if (manifest_read(&g_seg_store, gobj, &g_hp_ref_manifest) != 0) return -1;
            if (hp_ref_add_manifest_segments(&g_hp_ref_manifest) != 0) return -1;
        }
        return 0;
    }
    /* Must be manifest.<gen>: read it, add segments, and also the
     * generation-1 fallback manifest (recovery's only fallback)
     * when it exists. A present-but-unreadable gen-1 fails closed;
     * a missing gen-1 simply means there is no fallback. */
    if (manifest_read(&g_seg_store, cur, &g_hp_ref_manifest) != 0) return -1;
    if (hp_ref_add_manifest_segments(&g_hp_ref_manifest) != 0) return -1;
    {
        /* Parse the generation number after "manifest.". */
        uint64_t gen = 0;
        uint32_t p = 9; /* strlen("manifest.") */
        int digits = 0;
        if (hp_name_len(cur) <= 9) return 0;
        while (cur[p] != '\0') {
            if (cur[p] < '0' || cur[p] > '9') return 0; /* not a gen name */
            gen = gen * 10 + (uint64_t)(cur[p] - '0');
            p++;
            digits = 1;
        }
        if (!digits || gen <= 1) return 0;
        {
            char prev[MANIFEST_MAX_OBJ_LEN + 1];
            uint32_t pos = 0, d;
            uint64_t g = gen - 1, tmp = g;
            char rev[21];
            int rn = 0, ri;
            if (tmp == 0) { rev[rn++] = '0'; }
            while (tmp > 0) { rev[rn++] = (char)('0' + (tmp % 10)); tmp /= 10; }
            {
                const char* pre = "manifest.";
                while (pre[pos] != '\0') { prev[pos] = pre[pos]; pos++; }
            }
            for (ri = rn - 1; ri >= 0; ri--) prev[pos++] = rev[ri];
            prev[pos] = 0;
            d = (uint32_t)g_seg_store.vt->size_status(g_seg_store.state, prev, NULL);
            if (d == YOWZA_STORE_NOT_FOUND) return 0;
            if (d != YOWZA_STORE_OK) return -1;
            if (hp_ref_add(prev) != 0) return -1;
            if (manifest_read(&g_seg_store, prev, &g_hp_ref_manifest) != 0) return -1;
            if (hp_ref_add_manifest_segments(&g_hp_ref_manifest) != 0) return -1;
        }
    }
    return 0;
}

static int hp_u64_dec(uint64_t v, char* buf, uint32_t buf_size) {
    char tmp[21];
    int nn = 0, i;
    if (!buf || buf_size == 0) return -1;
    if (v == 0) {
        if (buf_size < 2) return -1;
        buf[0] = '0'; buf[1] = '\0';
        return 1;
    }
    while (v > 0) {
        tmp[nn++] = (char)('0' + (v % 10));
        v /= 10;
    }
    if ((uint32_t)nn + 1 > buf_size) return -1;
    for (i = 0; i < nn; i++) buf[i] = tmp[nn - 1 - i];
    buf[nn] = '\0';
    return nn;
}

static int hp_reclaim_internal(SegDB* state) {
    uint32_t i;
    uint64_t reclaimed = 0;
    uint32_t deleted = 0;
    int have_live = 0;
    if (!g_heap_ctx.heap) return -1;
    /* 1. Validate first: a corrupt slot fails with nothing deleted. */
    if (hp_validate_heap(&g_heap_ctx, NULL, NULL) != 0) return -1;
    for (i = 0; i < HEAP_TABLE_SLOTS; i++) {
        if (hp_slot(&g_heap_ctx, i)[0] != 0) { have_live = 1; break; }
    }
    g_hp_ref_count = 0;
    g_hp_ref_bytes = 0;
    /* 2. Pointer objects are always referenced. */
    if (hp_ref_add("CURRENT") != 0 || hp_ref_add("CURRENT.tmp") != 0 ||
        hp_ref_add("CURRENT.bak") != 0 || hp_ref_add("manifest.dir") != 0 ||
        hp_ref_add("manifest.dir.tmp") != 0) return -1;
    if (have_live) {
        char cur[MANIFEST_MAX_OBJ_LEN + 1];
        char bak[MANIFEST_MAX_OBJ_LEN + 1];
        /* CURRENT must parse, or reclaim cannot prove anything. */
        if (manifest_read_current(&g_seg_store, "CURRENT", cur,
                                  MANIFEST_MAX_OBJ_LEN + 1) != 0) return -1;
        if (hp_ref_add_chain(cur) != 0) return -1;
        /* CURRENT.bak, when present, names the previous pointer. */
        if (g_seg_store.vt->size_status(g_seg_store.state, "CURRENT.bak", NULL) ==
            YOWZA_STORE_OK) {
            if (manifest_read_current(&g_seg_store, "CURRENT.bak", bak,
                                      MANIFEST_MAX_OBJ_LEN + 1) != 0) return -1;
            if (hp_ref_add_chain(bak) != 0) return -1;
        }
        /* In-memory state: current + pending manifests and groups.
         * Pending names are added (never deleted) but never
         * required to load: a pending list may name objects whose
         * publish is still in flight in the caller's terms. */
        if (state) {
            uint32_t gi;
            if (state->has_dir) {
                if (hp_ref_add(state->dir_obj) != 0) return -1;
                for (gi = 0; gi < state->group_count; gi++) {
                    if (hp_ref_add(state->group_objs[gi]) != 0) return -1;
                    if (gi != state->group_count - 1) {
                        if (manifest_read(&g_seg_store, state->group_objs[gi],
                                          &g_hp_ref_manifest) != 0) return -1;
                        if (hp_ref_add_manifest_segments(&g_hp_ref_manifest) != 0) return -1;
                    }
                }
            }
            if (state->has_manifest) {
                if (state->has_dir && state->group_count > 0) {
                    /* Last group manifest: its segments are in
                     * state->manifest already. */
                } else {
                    char mn[MANIFEST_MAX_OBJ_LEN + 1];
                    char num[21];
                    uint32_t pos = 0, k;
                    const char* pre = "manifest.";
                    if (hp_u64_dec(state->manifest.generation, num, sizeof(num)) < 0) return -1;
                    while (pre[pos] != '\0') { mn[pos] = pre[pos]; pos++; }
                    for (k = 0; num[k] != '\0'; k++) mn[pos++] = num[k];
                    mn[pos] = 0;
                    if (hp_ref_add(mn) != 0) return -1;
                }
                if (hp_ref_add_manifest_segments(&state->manifest) != 0) return -1;
            }
            if (state->has_pending) {
                if (hp_ref_add_manifest_segments(&state->pending_manifest) != 0) return -1;
                if (hp_ref_add_manifest_segments(&state->pending_group_manifest) != 0) return -1;
                if (hp_ref_add_manifest_segments(&state->pending_old_group_manifest) != 0) return -1;
                for (gi = 0; gi < state->pending_group_count; gi++) {
                    if (hp_ref_add(state->pending_group_objs[gi]) != 0) return -1;
                }
                for (gi = 0; gi < state->pending_old_group_count; gi++) {
                    if (hp_ref_add(state->pending_old_group_objs[gi]) != 0) return -1;
                }
            }
        }
    }
    /* 3. Delete everything not proven referenced. */
    for (i = 0; i < HEAP_TABLE_SLOTS; i++) {
        uint8_t* s = hp_slot(&g_heap_ctx, i);
        if (s[0] == 0) continue;
        if (!hp_ref_has((const char*)s)) {
            hp_index_remove_name(&g_heap_ctx, (const char*)s);
            hp_zero(s, HEAP_SLOT_SIZE);
            deleted++;
        }
    }
    /* 4. Compact what is left (re-validates; deletions above
     * preserve validity, so this cannot newly fail on a heap that
     * passed step 1 — if it ever did, the deletions stand but the
     * error is still reported). */
    if (hp_compact_internal(&g_heap_ctx, &reclaimed) != 0) {
        g_hp_reclaim_deleted = deleted;
        g_hp_reclaim_bytes = 0;
        return -1;
    }
    g_hp_reclaim_deleted = deleted;
    g_hp_reclaim_bytes = reclaimed;
    return 0;
}

/* ---- Gate 3 exports (appended; existing exports above untouched) ---- */

SEGDB_EXPORT("yowza_segdb_heap_info")
void yowza_segdb_heap_info(uint8_t* out) {
    uint64_t v[6];
    uint64_t live = 0;
    uint32_t slots = 0;
    int i, j;
    if (!out) return;
    v[0] = 0; v[1] = 0; v[2] = 0; v[3] = 0; v[4] = 0;
    v[5] = g_hp_compact_threshold;
    if (g_heap_ctx.heap) {
        uint32_t k;
        v[0] = g_heap_ctx.heap_size;
        v[1] = hp_bump(&g_heap_ctx);
        if (hp_validate_heap(&g_heap_ctx, &live, &slots) == 0) {
            v[2] = live;
            v[3] = slots;
            if (v[1] >= HEAP_DATA_START && v[1] - HEAP_DATA_START >= live)
                v[4] = v[1] - HEAP_DATA_START - live;
        } else {
            for (k = 0; k < HEAP_TABLE_SLOTS; k++) {
                if (hp_slot(&g_heap_ctx, k)[0] != 0) v[3]++;
            }
        }
    }
    for (i = 0; i < 6; i++)
        for (j = 0; j < 8; j++) out[i * 8 + j] = (uint8_t)((v[i] >> (j * 8)) & 0xFF);
}

/* Force a heap compaction now. Returns 0 on success, -1 when the
 * heap is absent or fails validation (nothing is moved). */
SEGDB_EXPORT("yowza_segdb_heap_compact")
int yowza_segdb_heap_compact(void) {
    if (!g_heap_ctx.heap) return -1;
    return hp_compact_internal(&g_heap_ctx, NULL);
}

/* Configure the proactive compaction threshold in wasted bytes
 * (0 = compact only when an allocation would otherwise fail). */
SEGDB_EXPORT("yowza_segdb_heap_compact_threshold")
void yowza_segdb_heap_compact_threshold(uint64_t waste_bytes) {
    g_hp_compact_threshold = waste_bytes;
}

/* Delete proven orphans and compact. Returns 0 on success, -1
 * fail-closed (validation/reference failure: nothing deleted,
 * except the documented compact-after-delete step). */
SEGDB_EXPORT("yowza_segdb_heap_reclaim")
int yowza_segdb_heap_reclaim(SegDB* state) {
    return hp_reclaim_internal(state);
}

SEGDB_EXPORT("yowza_segdb_heap_reclaim_stats")
void yowza_segdb_heap_reclaim_stats(uint8_t* out) {
    uint64_t v[2];
    int i, j;
    if (!out) return;
    v[0] = g_hp_reclaim_deleted;
    v[1] = g_hp_reclaim_bytes;
    for (i = 0; i < 2; i++)
        for (j = 0; j < 8; j++) out[i * 8 + j] = (uint8_t)((v[i] >> (j * 8)) & 0xFF);
}

/* Fan-in-bounded scan (see include/segdb_scan.h): same art_scan
 * output format and cursor protocol as yowza_segdb_scan_cursor,
 * but without its 128-source cap. */
SEGDB_EXPORT("yowza_segdb_scan_fanin")
int yowza_segdb_scan_fanin(SegDB* state, const uint8_t* prefix, uint32_t prefix_len,
                           uint64_t snapshot_seq,
                           const uint8_t* cursor_key, uint32_t cursor_len, uint32_t has_cursor,
                           uint32_t limit,
                           uint8_t* out, uint32_t out_cap, uint32_t* count_out,
                           uint8_t* next_key, uint32_t next_key_cap, uint32_t* next_len_out,
                           uint32_t* has_more_out) {
    if (!state || !out || !count_out || !next_len_out || !has_more_out) return -4;
    if (prefix_len > 0 && !prefix) return -4;
    if (has_cursor && cursor_len > 0 && !cursor_key) return -4;
    return segdb_scan_fanin(state, prefix, prefix_len, snapshot_seq,
                            cursor_key, cursor_len, has_cursor != 0, limit,
                            out, out_cap, count_out,
                            next_key, next_key_cap, next_len_out, has_more_out);
}

SEGDB_EXPORT("yowza_segdb_scan_fanin_stats")
void yowza_segdb_scan_fanin_stats(uint8_t* out) {
    uint64_t s = 0, w = 0, k = 0, e = 0;
    int i, j;
    uint64_t v[4];
    if (!out) return;
    segdb_scan_fanin_stats_get(&s, &w, &k, &e);
    v[0] = s; v[1] = w; v[2] = k; v[3] = e;
    for (i = 0; i < 4; i++)
        for (j = 0; j < 8; j++) out[i * 8 + j] = (uint8_t)((v[i] >> (j * 8)) & 0xFF);
}

SEGDB_EXPORT("yowza_segdb_scan_fanin_stats_reset")
void yowza_segdb_scan_fanin_stats_reset(void) {
    segdb_scan_fanin_stats_reset();
}

/* ---- Track C: hierarchical fan-in scan export ----
 * Same page protocol as yowza_segdb_scan_fanin, but the
 * scan walks one manifest group at a time (see
 * segdb_scan_fanin_hier in segdb_scan.h): the 4096 flat
 * cap becomes a per-level cap and up to 8192 sources
 * scan completely. */
SEGDB_EXPORT("yowza_segdb_scan_fanin_hier")
int yowza_segdb_scan_fanin_hier(SegDB* state, const uint8_t* prefix, uint32_t prefix_len,
                                uint64_t snapshot_seq,
                                const uint8_t* cursor_key, uint32_t cursor_len, uint32_t has_cursor,
                                uint32_t limit,
                                uint8_t* out, uint32_t out_cap, uint32_t* count_out,
                                uint8_t* next_key, uint32_t next_key_cap, uint32_t* next_len_out,
                                uint32_t* has_more_out) {
    if (!state || !out || !count_out || !next_len_out || !has_more_out) return -4;
    if (prefix_len > 0 && !prefix) return -4;
    if (has_cursor && cursor_len > 0 && !cursor_key) return -4;
    return segdb_scan_fanin_hier(state, prefix, prefix_len, snapshot_seq,
                                 cursor_key, cursor_len, has_cursor != 0, limit,
                                 out, out_cap, count_out,
                                 next_key, next_key_cap, next_len_out, has_more_out);
}

/* Hier stats, 4 u64 LE values: groups, sources, max_group,
 * windows (last page call; see segdb_scan_fanin_hier_stats_get). */
SEGDB_EXPORT("yowza_segdb_scan_fanin_hier_stats")
void yowza_segdb_scan_fanin_hier_stats(uint8_t* out) {
    uint64_t g = 0, s = 0, m = 0, w = 0;
    int i, j;
    uint64_t v[4];
    if (!out) return;
    segdb_scan_fanin_hier_stats_get(&g, &s, &m, &w);
    v[0] = g; v[1] = s; v[2] = m; v[3] = w;
    for (i = 0; i < 4; i++)
        for (j = 0; j < 8; j++) out[i * 8 + j] = (uint8_t)((v[i] >> (j * 8)) & 0xFF);
}

SEGDB_EXPORT("yowza_segdb_scan_fanin_hier_stats_reset")
void yowza_segdb_scan_fanin_hier_stats_reset(void) {
    segdb_scan_fanin_hier_stats_reset();
}

/* ---- Track C: paged-index measurement exports ----
 * Paged stats, 4 u64 LE values: segdb cursor-prime page
 * probes, cursor-prime within-page entries (see
 * segdb_paged_stats_get), seg_lookup paged pages, and
 * seg_lookup index entries (see segment.h). All four
 * counters reset together. */
SEGDB_EXPORT("yowza_segdb_paged_stats")
void yowza_segdb_paged_stats(uint8_t* out) {
    uint64_t pp = 0, we = 0;
    int i, j;
    uint64_t v[4];
    if (!out) return;
    segdb_paged_stats_get(&pp, &we);
    v[0] = pp; v[1] = we;
    v[2] = seg_lookup_paged_pages();
    v[3] = seg_lookup_index_entries();
    for (i = 0; i < 4; i++)
        for (j = 0; j < 8; j++) out[i * 8 + j] = (uint8_t)((v[i] >> (j * 8)) & 0xFF);
}

SEGDB_EXPORT("yowza_segdb_paged_stats_reset")
void yowza_segdb_paged_stats_reset(void) {
    segdb_paged_stats_reset();
    seg_lookup_index_entries_reset();
}

/* ---- O4: unified lease exports (additive) ----
 * The in-module C lease table (segdb_lease_* in segdb.c, declared
 * in segdb.h) is the SINGLE lease authority. The Go bindings are
 * a client of these exports (see segdb.go); a lease acquired
 * through the Go API and a lease acquired through these exports
 * are entries in the same table, so either side's compact
 * (Go SegDB.Compact or a direct yowza_segdb_compact call) defers
 * on either side's lease, and a release through either side
 * frees the pin for both. Semantics are exactly segdb_lease_*'s:
 * acquire returns the new id (> 0), or 0 on failure (NULL state,
 * seq above the current sequence, or the 64-entry table full —
 * callers that need to distinguish future-seq from table-full
 * must compare seq against yowza_segdb_snapshot_seq first);
 * release returns 0 ok, -1 unknown/already-released; valid
 * returns 1/0; oldest returns the smallest active sequence and
 * writes *has_active (1 when any lease is active, else 0;
 * return value is 0 when none is active). These exports do not
 * change segdb_compact: it already defers on a stale lease in
 * this same table. */
SEGDB_EXPORT("yowza_segdb_lease_acquire")
uint64_t yowza_segdb_lease_acquire(SegDB* state, uint64_t seq) {
    if (!state) return 0;
    return segdb_lease_acquire(state, seq);
}

SEGDB_EXPORT("yowza_segdb_lease_release")
int yowza_segdb_lease_release(SegDB* state, uint64_t id) {
    if (!state) return -1;
    return segdb_lease_release(state, id);
}

SEGDB_EXPORT("yowza_segdb_lease_valid")
int yowza_segdb_lease_valid(SegDB* state, uint64_t id) {
    if (!state) return 0;
    return segdb_lease_valid(state, id);
}

SEGDB_EXPORT("yowza_segdb_lease_oldest")
uint64_t yowza_segdb_lease_oldest(SegDB* state, uint32_t* has_active) {
    int has = 0;
    uint64_t oldest;
    if (!state || !has_active) return 0;
    oldest = segdb_lease_oldest(state, &has);
    *has_active = (uint32_t)has;
    return oldest;
}

/* ---- Phase D: per-shard timeline primitives (appended) ----
 *
 * A per-shard timeline point is (snapshot_seq, lease_id): the
 * sequence is the shard's own commit sequence at the branch
 * point and the lease is a Phase B / Gate 1 C-table lease for
 * that sequence, so segdb_compact defers while the lease is
 * live and the point's data cannot be merged away under a
 * reader. These exports are the per-shard primitives the Go
 * multi-shard coordinator delegates to; they are NOT a second
 * overlay. The overlay (timeline writes after the branch
 * point) lives in the Go TimelineHandle's private write set
 * (timeline_handle.go), layered over these reads — exactly
 * like SegDBTransaction layers its write set over a Phase B
 * snapshot. A shard on its own has no timeline overlay: its
 * segdb_put/delete write the shard's main head, and calling
 * them "timeline writes" would be a lie this layer refuses
 * to tell.
 *
 * Return codes (in addition to the segdb conventions):
 *   0   success (get: found; fork: lease id returned)
 *  -1   bad arguments / not found (get) / scan error
 * -10   lease expired or unknown (lease_id != 0 and not valid)
 * -11   snapshot_seq above the shard's current sequence: a
 *       FUTURE snapshot. Never silently read as current.
 * -13   get value larger than out_cap (values through the
 *       facade are <= 1024, so this cannot trigger there;
 *       it is a fail-closed guard, not a truncation).
 * -14   underlying scan error (distinct from -1 not-found,
 *       so callers can tell absence from failure).
 * A timeline read NEVER falls back to the main head: the
 * snapshot_seq argument is literal (as in segdb_scan_page /
 * segdb_scan_cursor), the lease is checked before any row
 * is returned, and a future sequence fails before any scan.
 * Passing lease_id 0 skips the lease check (the caller then
 * owns pinning, e.g. through a durable pin's leases); it
 * does NOT skip the future-sequence check. */
static int tl_timeline_check(SegDB* state, uint64_t snapshot_seq, uint64_t lease_id) {
    uint64_t cur;
    if (!state) return -1;
    if (lease_id != 0 && segdb_lease_valid(state, lease_id) != 1) return -10;
    cur = segdb_snapshot_seq(state);
    if (snapshot_seq > cur) return -11;
    return 0;
}

static int tl_key_cmp(const uint8_t* a, uint32_t alen, const uint8_t* b, uint32_t blen) {
    uint32_t i, n = alen < blen ? alen : blen;
    for (i = 0; i < n; i++) {
        if (a[i] != b[i]) return a[i] < b[i] ? -1 : 1;
    }
    if (alen == blen) return 0;
    return alen < blen ? -1 : 1;
}

/* One page of the timeline-get scan. 15 rows x worst-case
 * 2055 bytes = 30825 < 32768, so a page always fits. */
static uint8_t tl_get_page[32768];

SEGDB_EXPORT("yowza_segdb_timeline_get_at")
int yowza_segdb_timeline_get_at(SegDB* state, const uint8_t* key, uint32_t key_len,
                                uint64_t snapshot_seq, uint64_t lease_id,
                                uint8_t* out, uint32_t out_cap, uint32_t* out_len) {
    int chk;
    uint64_t skip = 0;
    if (!state || !key || key_len == 0 || !out || !out_len) return -1;
    chk = tl_timeline_check(state, snapshot_seq, lease_id);
    if (chk != 0) return chk;
    for (;;) {
        uint32_t count = 0, i;
        uint32_t off = 0;
        int rc = segdb_scan_page(state, key, key_len, snapshot_seq, skip, 15,
                                 tl_get_page, (uint32_t)sizeof(tl_get_page), &count);
        if (rc != 0) return -14;
        if (count == 0) return -1; /* exact key absent at this snapshot */
        for (i = 0; i < count; i++) {
            uint32_t klen, vlen, j;
            int cmp;
            if (off + 4 > sizeof(tl_get_page)) return -14;
            klen = (uint32_t)tl_get_page[off] | ((uint32_t)tl_get_page[off + 1] << 8) |
                   ((uint32_t)tl_get_page[off + 2] << 16) | ((uint32_t)tl_get_page[off + 3] << 24);
            off += 4;
            if (off + klen + 4 > sizeof(tl_get_page)) return -14;
            cmp = tl_key_cmp(tl_get_page + off, klen, key, key_len);
            if (cmp > 0) return -1; /* sorted: exact key cannot appear later */
            if (cmp == 0) {
                const uint8_t* v;
                off += klen;
                vlen = (uint32_t)tl_get_page[off] | ((uint32_t)tl_get_page[off + 1] << 8) |
                       ((uint32_t)tl_get_page[off + 2] << 16) | ((uint32_t)tl_get_page[off + 3] << 24);
                off += 4;
                if (off + vlen > sizeof(tl_get_page)) return -14;
                if (vlen > out_cap) return -13;
                v = tl_get_page + off;
                for (j = 0; j < vlen; j++) out[j] = v[j];
                *out_len = vlen;
                return 0;
            }
            off += klen;
            vlen = (uint32_t)tl_get_page[off] | ((uint32_t)tl_get_page[off + 1] << 8) |
                   ((uint32_t)tl_get_page[off + 2] << 16) | ((uint32_t)tl_get_page[off + 3] << 24);
            off += 4 + vlen;
        }
        skip += count;
    }
}

SEGDB_EXPORT("yowza_segdb_timeline_scan_cursor")
int yowza_segdb_timeline_scan_cursor(SegDB* state, const uint8_t* prefix, uint32_t prefix_len,
                                     uint64_t snapshot_seq, uint64_t lease_id,
                                     const uint8_t* cursor_key, uint32_t cursor_len, uint32_t has_cursor,
                                     uint32_t limit,
                                     uint8_t* out, uint32_t out_cap, uint32_t* count_out,
                                     uint8_t* next_key, uint32_t next_key_cap, uint32_t* next_len_out,
                                     uint32_t* has_more_out) {
    int chk;
    if (!state || !out || !count_out || !next_len_out || !has_more_out) return -1;
    if (prefix_len > 0 && !prefix) return -1;
    if (has_cursor && cursor_len > 0 && !cursor_key) return -1;
    chk = tl_timeline_check(state, snapshot_seq, lease_id);
    if (chk != 0) return chk;
    return segdb_scan_cursor(state, prefix, prefix_len, snapshot_seq,
                             cursor_key, cursor_len, has_cursor != 0, limit,
                             out, out_cap, count_out,
                             next_key, next_key_cap, next_len_out, has_more_out);
}

/* Fork at shard level: pin the SAME snapshot point under a
 * new lease and return that lease id (0 on failure: bad
 * snapshot, expired source lease, or the 64-entry table
 * full). The forked point reads exactly the source point;
 * later main-head writes are invisible to both, and the
 * fork's own writes live in the caller's overlay, never in
 * this shard's head. */
SEGDB_EXPORT("yowza_segdb_timeline_fork")
uint64_t yowza_segdb_timeline_fork(SegDB* state, uint64_t snapshot_seq, uint64_t lease_id) {
    if (tl_timeline_check(state, snapshot_seq, lease_id) != 0) return 0;
    return segdb_lease_acquire(state, snapshot_seq);
}
