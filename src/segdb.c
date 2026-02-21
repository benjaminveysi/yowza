/* Phase 2, Track C — layered DB. See include/segdb.h for the contract
 * and the sequence-space explanation.
 *
 * Wasm safety: no stdio, stdlib, string.h or POSIX calls here. All
 * byte copies/compares and number formatting are manual loops. The
 * only allocation is caller-owned (arena/WAL buffers) plus static
 * scratch below; segdb never calls malloc. Single-threaded: the
 * static scratch buffers are shared by all SegDBs, which is safe
 * under the host serialization invariant and is the same trade the
 * VM makes with its static scan buffer.
 *
 * Caps (Phase 2 limits, fail -1 rather than corrupt): flush collects
 * at most SEGDB_FLUSH_MAX_ENTRIES hot entries with at most
 * SEGDB_KEY_SCRATCH key bytes; scan/compact collect at most
 * SEGDB_SCAN_MAX_CANDS candidates with at most SEGDB_SCAN_DATA bytes
 * of copied candidate data; segment values read through iteration
 * are capped at 1024 bytes by the iteration buffers (segdb values
 * are <= 1024 by the hot path, so this is not reachable through
 * segdb_put). A streaming redesign removes these caps; they are not
 * correctness limits for data within them (flush fails before
 * writing anything if the hot set does not fit the collection
 * scratch). */
#include "segdb.h"

/* ---- static scratch (see header comment) ---- */
#define SEGDB_FLUSH_MAX_ENTRIES 4096u
#define SEGDB_SCAN_MAX_CANDS 8192u
#define SEGDB_KEY_SCRATCH 65536u
#define SEGDB_SCAN_DATA 262144u
#define SEGDB_CHUNK_ENTRIES 4096u
#define SEGDB_NAME_BUF (MANIFEST_MAX_OBJ_LEN + 1)

static SegEntry g_flush_entries[SEGDB_FLUSH_MAX_ENTRIES];
static uint8_t g_flush_keys[SEGDB_KEY_SCRATCH];

typedef struct {
    const uint8_t* key;
    uint32_t key_len;
    const uint8_t* val;
    uint32_t val_len;
    int tomb;
    uint64_t seq;
    uint64_t crdt_ts;
    uint32_t prio; /* hot=1 beats segment=0 on a seq tie; for compact,
     * later segment index beats earlier on a tie */
} SegCand;

static SegCand g_scan_cands[SEGDB_SCAN_MAX_CANDS];
static uint8_t g_scan_data[SEGDB_SCAN_DATA];
static Manifest g_manifest_scratch;
/* Group-manifest scratch for get/scan/recover in directory mode
 * (never the flush/compact scratch, so a group load cannot
 * clobber a pending manifest being built). */
static Manifest g_group_scratch;
static ManifestDir g_dir_scratch;

/* ---- manual byte helpers (no libc) ---- */
static void seg_memcpy(uint8_t* dst, const uint8_t* src, uint32_t len) {
    uint32_t i;
    if (!dst) return;
    for (i = 0; i < len; i++) dst[i] = src ? src[i] : 0;
}

static int seg_memcmp(const uint8_t* a, const uint8_t* b, uint32_t len) {
    uint32_t i;
    for (i = 0; i < len; i++) {
        if (a[i] != b[i]) return (int)a[i] - (int)b[i];
    }
    return 0;
}

static int seg_key_cmp(const uint8_t* a, uint32_t a_len, const uint8_t* b, uint32_t b_len) {
    uint32_t m = a_len < b_len ? a_len : b_len;
    int c = seg_memcmp(a, b, m);
    if (c != 0) return c;
    if (a_len < b_len) return -1;
    if (a_len > b_len) return 1;
    return 0;
}

static int seg_key_equal(const uint8_t* a, uint32_t a_len, const uint8_t* b, uint32_t b_len) {
    return a_len == b_len && seg_memcmp(a, b, a_len) == 0;
}

static int seg_prefix_match(const uint8_t* key, uint32_t key_len,
                            const uint8_t* prefix, uint32_t prefix_len) {
    if (prefix_len == 0) return 1;
    if (key_len < prefix_len) return 0;
    return seg_memcmp(key, prefix, prefix_len) == 0;
}

/* Decimal formatting/parsing without stdio. Returns length, or -1. */
static int seg_u64_dec(uint64_t v, char* buf, uint32_t buf_size) {
    char tmp[21];
    int n = 0;
    int i;
    if (!buf || buf_size == 0) return -1;
    if (v == 0) {
        if (buf_size < 2) return -1;
        buf[0] = '0'; buf[1] = '\0';
        return 1;
    }
    while (v > 0) {
        tmp[n++] = (char)('0' + (v % 10));
        v /= 10;
    }
    if ((uint32_t)n + 1 > buf_size) return -1;
    for (i = 0; i < n; i++) buf[i] = tmp[n - 1 - i];
    buf[n] = '\0';
    return n;
}

static int seg_append_str(char* buf, uint32_t buf_size, uint32_t* pos, const char* s) {
    uint32_t i = 0;
    while (s[i] != '\0') {
        if (*pos + 1 >= buf_size) return -1;
        buf[*pos] = s[i];
        (*pos)++;
        i++;
    }
    buf[*pos] = '\0';
    return 0;
}

static int seg_name_seg(uint64_t id, char* buf, uint32_t buf_size) {
    char num[21];
    uint32_t pos = 0;
    if (seg_u64_dec(id, num, sizeof(num)) < 0) return -1;
    if (seg_append_str(buf, buf_size, &pos, "seg.") != 0) return -1;
    if (seg_append_str(buf, buf_size, &pos, num) != 0) return -1;
    return 0;
}

static int seg_name_manifest(uint64_t gen, char* buf, uint32_t buf_size) {
    char num[21];
    uint32_t pos = 0;
    if (seg_u64_dec(gen, num, sizeof(num)) < 0) return -1;
    if (seg_append_str(buf, buf_size, &pos, "manifest.") != 0) return -1;
    if (seg_append_str(buf, buf_size, &pos, num) != 0) return -1;
    return 0;
}

static int seg_name_manifest_tmp(uint64_t gen, char* buf, uint32_t buf_size) {
    char num[21];
    uint32_t pos = 0;
    if (seg_u64_dec(gen, num, sizeof(num)) < 0) return -1;
    if (seg_append_str(buf, buf_size, &pos, "manifest.") != 0) return -1;
    if (seg_append_str(buf, buf_size, &pos, num) != 0) return -1;
    if (seg_append_str(buf, buf_size, &pos, ".tmp") != 0) return -1;
    return 0;
}

/* Phase B: the old seg_parse_manifest_name helper is GONE
 * with segdb_load_named_manifest — its only caller. Name
 * parsing for manifests now lives in manifest.c
 * (man_parse_manifest_gen / manifest_root_parse_gen),
 * used by the shared resolver. */

/* Parse "seg.<digits>" for next_seg_id computation. Returns 0/-1. */
static int seg_parse_seg_name(const char* s, uint64_t* id) {
    uint32_t i = 0;
    uint64_t v = 0;
    if (s[0] != 's' || s[1] != 'e' || s[2] != 'g' || s[3] != '.') return -1;
    if (s[4] == '\0') return -1;
    for (i = 4; s[i] != '\0'; i++) {
        if (s[i] < '0' || s[i] > '9') return -1;
        if (v > ((uint64_t)-1 - 9) / 10) return -1;
        v = v * 10 + (uint64_t)(s[i] - '0');
    }
    *id = v;
    return 0;
}

/* Thin wrappers over the Track A vtable (state-first). Missing hooks
 * are treated as: sync = no-op success, remove/size = error. */
static int store_sync_obj(YowzaStore* s, const char* obj) {
    if (!s || !s->vt || !s->vt->sync_obj) return 0;
    return s->vt->sync_obj(s->state, obj);
}

static int store_sync_dir(YowzaStore* s) {
    if (!s || !s->vt || !s->vt->sync_dir) return 0;
    return s->vt->sync_dir(s->state);
}

static int store_remove(YowzaStore* s, const char* obj) {
    if (!s || !s->vt || !s->vt->remove) return -1;
    return s->vt->remove(s->state, obj);
}

/* Track A status-split size probe (see storage2.h). */
static int store_size_status(YowzaStore* s, const char* obj, uint64_t* out) {
    if (!s) return YOWZA_STORE_IO_ERROR;
    return yowza_store_size_status(s, obj, out);
}

int segdb_indeterminate(SegDB* db) {
    return db ? db->indeterminate : 0;
}

uint64_t segdb_snapshot_seq(SegDB* db) {
    if (!db) return 0;
    return db->seq_base + db->hot.index.commit_seq;
}

/* Guards every mutating entry point: while a publish outcome is
 * indeterminate, writes are refused until segdb_reconcile runs.
 * Reads stay available (hot + old segments are still a valid old
 * logical state; a flush's hot entries are still in hot, and a
 * compact changes no logical state). */
static int segdb_write_guard(SegDB* db) {
    if (!db) return -1;
    if (db->indeterminate) return SEGDB_INDETERMINATE;
    return 0;
}

/* ---- GATE 1: hot-layer history + snapshot leases ----
 * Problem (Gate 1.1): the hot ART keeps one terminal per key, so
 * a put/delete overwrites the only hot version and an older
 * snapshot loses the pre-update value for an unflushed key.
 * Fix: before a segdb_put/segdb_delete overwrites a live or
 * tombstoned hot terminal, record the superseded version in
 * db->hist (key copied into the hot arena, value = the old
 * arena bytes, which the bump arena never reuses before the
 * next flush). Scans merge history candidates with hot and
 * segment candidates by effective sequence, so a snapshot at or
 * after a version's sequence and before the next version sees
 * exactly that version. At flush, history is transferred into
 * segment format (one history segment per version-rank, oldest
 * first in the manifest — see segdb_flush_write_history), so
 * old snapshots survive the flush and a later recover; the
 * history table is then cleared with the hot layer.
 * Limits, stated honestly: history is recorded on the
 * segdb_put/segdb_delete paths AND on yowza_db_tx_* commits
 * through the TxState history hook registered in
 * segdb_init_common (VM/wasm program txs and tx_replay_wal are
 * NOT covered — see PARALLEL_TIMELINE_NOTES.md); the table holds
 * SEGDB_HIST_MAX versions per flush epoch and a put/delete
 * that would exceed it fails with -3 rather than silently
 * dropping a version; history key copies consume hot arena
 * space until the next flush (they are freed by the flush
 * arena reset, together with the hot entries themselves).
 * Leases (Gate 1.2) are registered in db->lease_* and checked
 * by segdb_compact itself (see there): a stale lease defers
 * compaction, it never lets a merge drop leased data. */

/* Capture the superseded hot version of key into *out (key
 * copied into the hot arena) and set *have = 1, or *have = 0
 * when the key has no hot version worth preserving (absent, or
 * an intermediate-only node). Returns 0, or -3 when the arena
 * cannot hold the key copy or the history table is full; on
 * -3 the caller must fail the mutation BEFORE it happens, so
 * no version is ever lost silently. The captured value pointer
 * stays valid until the next flush (bump arena). */
/* Axis C — history storage helpers (array + overflow list).
 * Insertion order is the epoch order: hist[0..hist_count)
 * first, then hist_overflow_head..tail. Every helper here
 * is O(1) except segdb_hist_get / iteration, which walk
 * from the head (the flush rank pass uses the iterator,
 * whose state is copyable, so a forward walk from any
 * position stays O(1) amortised per step). */
static uint32_t segdb_hist_total(SegDB* db) {
    if (!db) return 0;
    return db->hist_count + db->hist_overflow_count;
}

/* Append an already-captured entry. Array space is free;
 * an overflow append allocates one arena node. Returns 0
 * or -3 (arena OOM). Callers that must not fail AFTER a
 * mutation use the capture reservation (below), not this. */
static int segdb_hist_append(SegDB* db, const SegdbHistEntry* e) {
    SegdbHistOverflow* node;
    if (!db || !e) return -3;
    if (db->hist_count < SEGDB_HIST_MAX) {
        db->hist[db->hist_count] = *e;
        db->hist_count++;
        return 0;
    }
    node = (SegdbHistOverflow*)arena_alloc(&db->hot.arena, sizeof(SegdbHistOverflow));
    if (!node) return -3;
    node->entry = *e;
    node->prev = db->hist_overflow_tail;
    node->next = NULL;
    if (db->hist_overflow_tail) db->hist_overflow_tail->next = node;
    else db->hist_overflow_head = node;
    db->hist_overflow_tail = node;
    db->hist_overflow_count++;
    return 0;
}

/* Pop the last `count` entries (tx rollback). Overflow
 * nodes are unlinked, not freed: their bytes are arena
 * bump waste until the flush reset, exactly like a
 * superseded key copy dropped by a rollback. */
static int segdb_hist_truncate(SegDB* db, uint32_t count) {
    if (!db || count > segdb_hist_total(db)) return -1;
    while (count > 0) {
        if (db->hist_overflow_tail) {
            SegdbHistOverflow* tail = db->hist_overflow_tail;
            db->hist_overflow_tail = tail->prev;
            if (db->hist_overflow_tail) db->hist_overflow_tail->next = NULL;
            else db->hist_overflow_head = NULL;
            db->hist_overflow_count--;
        } else {
            db->hist_count--;
        }
        count--;
    }
    return 0;
}

static void segdb_hist_clear(SegDB* db) {
    if (!db) return;
    db->hist_count = 0;
    db->hist_overflow_head = NULL;
    db->hist_overflow_tail = NULL;
    db->hist_overflow_count = 0;
    db->hist_reserved = NULL;
}

/* Iterator over epoch history in insertion order. Copying
 * the struct copies the position, so a nested forward walk
 * (the flush rank computation) needs no re-scan from the
 * head for every step. */
typedef struct {
    uint32_t arr_i;
    SegdbHistOverflow* node;
    int in_overflow;
} SegdbHistIter;

static void segdb_hist_iter_start(SegDB* db, SegdbHistIter* it) {
    (void)db;
    it->arr_i = 0;
    it->node = NULL;
    it->in_overflow = 0;
}

/* Returns 1 with *out pointing at the next entry (array
 * element or node entry — valid until the next flush),
 * or 0 at the end. db->hist_count / overflow pointers are
 * read fresh each step, so the caller must not mutate
 * history mid-iteration (single-threaded invariant). */
static int segdb_hist_iter_next(SegDB* db, SegdbHistIter* it, SegdbHistEntry** out) {
    if (!it->in_overflow) {
        if (it->arr_i < db->hist_count) {
            *out = &db->hist[it->arr_i];
            it->arr_i++;
            return 1;
        }
        it->in_overflow = 1;
        it->node = db->hist_overflow_head;
    }
    if (it->node) {
        *out = &it->node->entry;
        it->node = it->node->next;
        return 1;
    }
    return 0;
}

/* Commit a capture made by segdb_history_capture: links
 * its pre-reserved overflow node (infallible), or appends
 * to the array / allocates a node as a fallback for
 * callers without a reservation. */
static int segdb_hist_commit(SegDB* db, const SegdbHistEntry* e) {
    SegdbHistOverflow* node;
    if (!db || !e) return -3;
    if (db->hist_reserved) {
        node = db->hist_reserved;
        db->hist_reserved = NULL;
        node->prev = db->hist_overflow_tail;
        node->next = NULL;
        if (db->hist_overflow_tail) db->hist_overflow_tail->next = node;
        else db->hist_overflow_head = node;
        db->hist_overflow_tail = node;
        db->hist_overflow_count++;
        return 0;
    }
    return segdb_hist_append(db, e);
}

static void segdb_hist_release_reservation(SegDB* db) {
    if (db) db->hist_reserved = NULL; /* node bytes: bump waste */
}

static int segdb_history_capture(SegDB* db, const uint8_t* key, uint32_t key_len,
                                 SegdbHistEntry* out, int* have) {
    ARTNode* term;
    uint8_t* kcopy;
    *have = 0;
    term = art_find_terminal(&db->hot.index, key, key_len);
    if (!term) return 0;
    if (!term->is_end && term->timestamp == 0) return 0; /* intermediate only */
    kcopy = (uint8_t*)arena_alloc(&db->hot.arena, key_len);
    if (!kcopy) return -3;
    seg_memcpy(kcopy, key, key_len);
    out->key = kcopy;
    out->key_len = key_len;
    out->tomb = term->is_end ? 0 : 1;
    out->val = term->is_end ? term->val_ptr : NULL;
    out->val_len = term->is_end ? term->val_len : 0;
    out->seq = db->seq_base + term->version;
    out->crdt_ts = term->timestamp;
    *have = 1;
    /* Axis C reservation: when the array is full, allocate
     * the overflow node NOW, before the caller mutates the
     * ART, so segdb_hist_commit after a successful mutation
     * cannot fail and a failed mutation simply drops the
     * reservation (bump waste, never a live entry). The
     * reservation is a single in-flight slot: put/delete
     * capture at most one version per call, and the
     * single-threaded invariant means no second capture
     * can interleave before commit/release. */
    if (db->hist_count >= SEGDB_HIST_MAX) {
        SegdbHistOverflow* node =
            (SegdbHistOverflow*)arena_alloc(&db->hot.arena, sizeof(SegdbHistOverflow));
        if (!node) return -3;
        node->entry = *out;
        node->prev = NULL;
        node->next = NULL;
        db->hist_reserved = node;
    }
    return 0;
}

uint32_t segdb_history_total(SegDB* db) {
    return segdb_hist_total(db);
}

uint32_t segdb_history_overflow_count(SegDB* db) {
    return db ? db->hist_overflow_count : 0;
}

int segdb_history_at(SegDB* db, uint32_t idx, SegdbHistEntry* out) {
    SegdbHistOverflow* node;
    uint32_t i;
    if (!db || !out) return -1;
    if (idx < db->hist_count) {
        *out = db->hist[idx];
        return 0;
    }
    idx -= db->hist_count;
    node = db->hist_overflow_head;
    for (i = 0; i < idx; i++) {
        if (!node) return -1;
        node = node->next;
    }
    if (!node) return -1;
    *out = node->entry;
    return 0;
}

uint64_t segdb_lease_acquire(SegDB* db, uint64_t seq) {
    uint32_t i;
    uint64_t id;
    if (!db) return 0;
    if (seq > segdb_snapshot_seq(db)) return 0; /* future snapshot */
    for (i = 0; i < SEGDB_MAX_LEASES; i++) {
        if (!db->lease_active[i]) {
            id = db->next_lease_id;
            if (id == 0) id = 1; /* never hand out 0 */
            db->next_lease_id = id + 1;
            db->lease_active[i] = 1;
            db->lease_ids[i] = id;
            db->lease_seqs[i] = seq;
            return id;
        }
    }
    return 0; /* table full */
}

int segdb_lease_release(SegDB* db, uint64_t id) {
    uint32_t i;
    if (!db || id == 0) return -1;
    for (i = 0; i < SEGDB_MAX_LEASES; i++) {
        if (db->lease_active[i] && db->lease_ids[i] == id) {
            db->lease_active[i] = 0;
            db->lease_ids[i] = 0;
            db->lease_seqs[i] = 0;
            return 0;
        }
    }
    return -1; /* unknown or already released */
}

int segdb_lease_valid(SegDB* db, uint64_t id) {
    uint32_t i;
    if (!db || id == 0) return 0;
    for (i = 0; i < SEGDB_MAX_LEASES; i++) {
        if (db->lease_active[i] && db->lease_ids[i] == id) return 1;
    }
    return 0;
}

uint64_t segdb_lease_oldest(SegDB* db, int* has_active) {
    uint32_t i;
    uint64_t best = 0;
    int found = 0;
    if (has_active) *has_active = 0;
    if (!db) return 0;
    for (i = 0; i < SEGDB_MAX_LEASES; i++) {
        if (db->lease_active[i]) {
            if (!found || db->lease_seqs[i] < best) best = db->lease_seqs[i];
            found = 1;
        }
    }
    if (has_active) *has_active = found;
    return found ? best : 0;
}

uint32_t segdb_history_count(SegDB* db) {
    return segdb_hist_total(db);
}

/* ---- Daemon-build WAL driver fallback ----
 * The Makefile's daemon target (yowzad) excludes storage_memory.c
 * from its sources, but segdb_init builds the hot DB on the memory
 * WAL driver, so yowzad would fail to link without it. This weak
 * definition of storage_memory_create closes that gap: wherever the
 * real storage_memory.c is linked (tests, wasm builds) its STRONG
 * definition wins and this weak one is discarded; only the daemon
 * build (which lacks the real file) uses it. Semantics mirror the
 * real driver (append/capacity/sync/log_length/truncate/read/
 * close over the caller's buffer); the only deliberate difference
 * is a 4-slot static pool instead of 16. If the real driver's
 * semantics change, update this fallback too. Guarded out under
 * __wasm__ (wasm builds always link the real file) and by
 * SEGDB_NO_WEAK_WAL_DRIVER. */
#if !defined(__wasm__) && !defined(SEGDB_NO_WEAK_WAL_DRIVER)
typedef struct {
    uint8_t* buffer;
    uint32_t capacity;
    uint32_t offset;
    int used;
} SegdbWeakMemState;

static SegdbWeakMemState g_weak_mem_pool[4];

static int weak_mem_append(StorageDriver* self, const uint8_t* data, uint32_t length) {
    SegdbWeakMemState* st;
    uint32_t i;
    if (!self || !self->state || !data || length == 0) return -1;
    st = (SegdbWeakMemState*)self->state;
    if (!st->buffer || length > st->capacity - st->offset) return -1;
    for (i = 0; i < length; i++) st->buffer[st->offset + i] = data[i];
    st->offset += length;
    return (int)length;
}

static int weak_mem_check_capacity(StorageDriver* self, uint32_t length) {
    SegdbWeakMemState* st;
    if (!self || !self->state) return -1;
    st = (SegdbWeakMemState*)self->state;
    if (!st->buffer) return -1;
    return length <= st->capacity - st->offset ? 0 : -1;
}

static int weak_mem_sync(StorageDriver* self) {
    (void)self;
    return 0;
}

static int weak_mem_log_length(StorageDriver* self) {
    if (!self || !self->state) return -1;
    return (int)((SegdbWeakMemState*)self->state)->offset;
}

static int weak_mem_truncate(StorageDriver* self, uint32_t length) {
    SegdbWeakMemState* st;
    if (!self || !self->state) return -1;
    st = (SegdbWeakMemState*)self->state;
    if (length > st->offset) return -1;
    st->offset = length;
    return 0;
}

static int weak_mem_read(StorageDriver* self, uint8_t* buffer, uint32_t length, uint32_t offset) {
    SegdbWeakMemState* st;
    uint32_t avail, n, i;
    if (!self || !self->state || !buffer) return -1;
    st = (SegdbWeakMemState*)self->state;
    if (offset >= st->offset) return 0;
    avail = st->offset - offset;
    n = length < avail ? length : avail;
    for (i = 0; i < n; i++) buffer[i] = st->buffer[offset + i];
    return (int)n;
}

static void weak_mem_close(StorageDriver* self) {
    int i;
    if (!self || !self->state) return;
    for (i = 0; i < 4; i++) {
        if (self->state == (void*)&g_weak_mem_pool[i]) {
            g_weak_mem_pool[i].used = 0;
            break;
        }
    }
    self->state = NULL;
}

__attribute__((weak)) StorageDriver storage_memory_create(uint8_t* wal_buffer, uint32_t capacity) {
    StorageDriver driver;
    int i;
    driver.state = NULL;
    driver.append = NULL;
    driver.check_capacity = NULL;
    driver.sync = NULL;
    driver.log_length = NULL;
    driver.truncate = NULL;
    driver.read = NULL;
    driver.close = NULL;
    for (i = 0; i < 4; i++) {
        if (!g_weak_mem_pool[i].used) {
            g_weak_mem_pool[i].used = 1;
            g_weak_mem_pool[i].buffer = wal_buffer;
            g_weak_mem_pool[i].capacity = capacity;
            g_weak_mem_pool[i].offset = 0;
            driver.state = &g_weak_mem_pool[i];
            driver.append = weak_mem_append;
            driver.check_capacity = weak_mem_check_capacity;
            driver.sync = weak_mem_sync;
            driver.log_length = weak_mem_log_length;
            driver.truncate = weak_mem_truncate;
            driver.read = weak_mem_read;
            driver.close = weak_mem_close;
            return driver;
        }
    }
    return driver;
}
#endif /* weak WAL driver fallback */

/* ---- Tx + Hot History production wiring ----
 * tx_commit cannot name a SegDB (it receives only an ART*), so
 * the TxState history hook (see tx.h) is the integration point.
 * This adapter is the SegDB-side owner: it appends the
 * superseded committed version to db->hist with seq = seq_base
 * + ART-local old version, exactly mirroring
 * segdb_history_capture's entry shape (arena key copy, tomb
 * flag, arena value pointer valid until the next flush).
 * Registered in segdb_init_common AFTER yowza_db_init (which
 * calls tx_init_state and clears the hooks); tx_clear preserves
 * the hooks, and flush never touches db->hot.tx, so one
 * registration lasts the SegDB lifetime. Fail-closed: table
 * full or arena OOM returns -3 and tx_commit applies nothing;
 * rollback truncates exactly the captures of the failed
 * commit. VM/wasm program txs (vm->tx, hooks NULL) and
 * tx_replay_wal are NOT covered by this registration (see
 * PARALLEL_TIMELINE_NOTES.md for the exact covered list). */
static int segdb_tx_history_capture(void* ctx, const uint8_t* key, uint32_t key_len,
                                    uint64_t old_version, int tomb,
                                    const uint8_t* val, uint32_t val_len, uint64_t crdt_ts) {
    SegDB* db = (SegDB*)ctx;
    uint8_t* kcopy;
    SegdbHistEntry e;
    if (!db) return -3;
    kcopy = (uint8_t*)arena_alloc(&db->hot.arena, key_len);
    if (!kcopy) return -3;
    seg_memcpy(kcopy, key, key_len);
    e.key = kcopy;
    e.key_len = key_len;
    e.tomb = tomb;
    e.val = val;
    e.val_len = val_len;
    e.seq = db->seq_base + old_version;
    e.crdt_ts = crdt_ts;
    /* Axis C: appends to the array or the overflow list;
     * -3 only on arena OOM (tx_commit then rolls back the
     * captures of this commit, as before). */
    return segdb_hist_append(db, &e);
}
static int segdb_tx_history_rollback(void* ctx, uint32_t count) {
    SegDB* db = (SegDB*)ctx;
    if (!db) return -1;
    return segdb_hist_truncate(db, count);
}

/* ---- SegDB basics ---- */
static int segdb_init_common(SegDB* db, YowzaStore* store, void* arena_buf,
                             size_t arena_size, StorageDriver drv,
                             uint8_t* wal_buf, uint32_t wal_cap) {
    uint32_t i;
    if (!drv.append) return -1; /* driver pool exhausted / bad driver */
    if (yowza_db_init(&db->hot, arena_buf, arena_size, drv) != 0) return -1;
    /* Tx + Hot History production wiring (see above): tx commits
     * on db->hot capture superseded versions into db->hist. */
    db->hot.tx.history_capture = segdb_tx_history_capture;
    db->hot.tx.history_ctx = db;
    db->hot.tx.history_rollback = segdb_tx_history_rollback;
    db->store = store;
    db->manifest.generation = 0;
    db->manifest.checkpoint_seq = 0;
    db->manifest.schema_version = 0;
    db->manifest.segment_count = 0;
    for (i = 0; i < MANIFEST_MAX_SEGMENTS; i++) {
        db->manifest.segments[i].obj[0] = '\0';
        db->manifest.segments[i].first_key_len = 0;
        db->manifest.segments[i].last_key_len = 0;
        db->manifest.segments[i].min_seq = 0;
        db->manifest.segments[i].max_seq = 0;
        db->manifest.segments[i].entry_count = 0;
        db->manifest.segments[i].index_type = 0;
        db->manifest.segments[i].model_version = 0;
        db->manifest.segments[i].byte_size = 0;
        db->manifest.segments[i].checksum = 0;
    }
    db->next_seg_id = 1;
    db->has_manifest = 0;
    db->seq_base = 0;
    db->wal_buf = wal_buf;
    db->wal_cap = wal_cap;
    db->arena_buf = arena_buf;
    db->arena_size = arena_size;
    db->recovered = 0;
    db->indeterminate = 0;
    db->has_pending = 0;
    db->pending_is_compact = 0;
    db->pending_next_seg_id = 1;
    db->group_count = 0;
    db->has_dir = 0;
    db->dir_obj[0] = '\0';
    for (i = 0; i < MANIFEST_DIR_MAX_GROUPS; i++) db->group_objs[i][0] = '\0';
    /* GATE 1: fresh hot history (no superseded versions yet) and
     * no active leases on a newly initialised SegDB. */
    segdb_hist_clear(db);
    db->next_lease_id = 1;
    for (i = 0; i < SEGDB_MAX_LEASES; i++) {
        db->lease_active[i] = 0;
        db->lease_ids[i] = 0;
        db->lease_seqs[i] = 0;
    }
    db->pending_dir_transition = 0;
    db->pending_group_add = 0;
    db->pending_group_append = 0;
    db->pending_group_count = 0;
    db->pending_old_group_count = 0;
    for (i = 0; i < MANIFEST_DIR_MAX_GROUPS; i++) {
        db->pending_group_objs[i][0] = '\0';
        db->pending_old_group_objs[i][0] = '\0';
    }
    return 0;
}

int segdb_init(SegDB* db, YowzaStore* store, void* arena_buf, size_t arena_size,
               uint8_t* wal_buf, uint32_t wal_cap) {
    StorageDriver drv;
    if (!db || !store || !arena_buf || arena_size == 0 || !wal_buf || wal_cap == 0) return -1;
    drv = storage_memory_create(wal_buf, wal_cap);
    return segdb_init_common(db, store, arena_buf, arena_size, drv, wal_buf, wal_cap);
}

int segdb_init_with_driver(SegDB* db, YowzaStore* store, void* arena_buf,
                           size_t arena_size, StorageDriver drv) {
    if (!db || !store || !arena_buf || arena_size == 0) return -1;
    return segdb_init_common(db, store, arena_buf, arena_size, drv, NULL, 0);
}

void segdb_close(SegDB* db) {
    if (!db) return;
    if (db->hot.storage.close) db->hot.storage.close(&db->hot.storage);
}

int segdb_put(SegDB* db, const uint8_t* key, uint32_t key_len,
              const uint8_t* val, uint32_t val_len) {
    int g, rc;
    SegdbHistEntry he;
    int have_hist = 0;
    if (!db || !key || key_len == 0 || key_len > YOWZA_MAX_KEY_LEN || val_len > 1024) return -1;
    if (val_len > 0 && !val) return -1;
    g = segdb_write_guard(db);
    if (g != 0) return g;
    /* GATE 1: record the superseded hot version BEFORE the
     * overwrite; on capture failure (-3) fail the put rather
     * than losing a version an old snapshot still needs. */
    if (segdb_history_capture(db, key, key_len, &he, &have_hist) != 0) return -3;
    rc = yowza_db_put_raw(&db->hot, key, (size_t)key_len, val, (size_t)val_len);
    if (rc == 0 && have_hist) {
        /* Reservation-backed: cannot fail after the mutation. */
        if (segdb_hist_commit(db, &he) != 0) return -3;
    } else if (rc != 0) {
        segdb_hist_release_reservation(db);
    }
    return rc;
}

/* Delete of a hot-absent key: the VM-level absent delete is
 * deliberately a no-op (hot-ART-only semantics, preserved by vm.c),
 * but a segdb delete must shadow SEGMENT values too, so it must
 * leave a hot tombstone behind. Build that tombstone in-memory
 * (insert an empty value, then delete it — both ART-only, no WAL
 * yet), then append ONE legacy DEL record and sync, under the same
 * restore-on-failure contract as vm.c: on any WAL/sync failure the
 * captured terminal header (or its absence) is restored and the WAL
 * truncated back, so a failed segdb_delete leaves the state exactly
 * as before (in particular it never leaves the empty placeholder
 * live). A DEL record replayed onto a tree where the key is absent
 * is a no-op in Phase 1 replay, so — like all unflushed hot data
 * under the memory WAL driver — a tombstone created here survives a
 * restart only once flushed into a segment; that is the documented
 * pre-flush limitation, not a new one. */
static int segdb_delete_absent(SegDB* db, const uint8_t* key, uint32_t key_len,
                               ARTNode* pre_term) {
    ARTNode* term;
    int pre_existed = pre_term != NULL;
    int pre_was_end = pre_term ? pre_term->is_end : 0;
    uint32_t pre_val_len = pre_term ? pre_term->val_len : 0;
    uint8_t* pre_val_ptr = pre_term ? pre_term->val_ptr : NULL;
    uint64_t pre_ts = pre_term ? pre_term->timestamp : 0;
    uint64_t pre_ver = pre_term ? pre_term->version : 0;
    uint64_t pre_seq = db->hot.index.commit_seq;
    uint64_t pre_max = db->hot.index.max_timestamp;
    StorageDriver* st = &db->hot.storage;
    int pre_len = -1;
    int pre_len_valid = 0;
    uint8_t b4[4];
    int fail_code = -6;

    if (art_insert(&db->hot.index, key, key_len, (const uint8_t*)"", 0, 0) != 0) {
        return -3;
    }
    if (art_delete(&db->hot.index, key, key_len) != 0) {
        fail_code = -1;
        goto restore; /* unreachable (just inserted), but stay safe */
    }
    if (st->append) {
        if (st->log_length) {
            pre_len = st->log_length(st);
            pre_len_valid = pre_len >= 0;
        }
        /* Legacy DEL record: [op 0x03][key_len u32][key][val_len u32=0]. */
        {
            uint8_t op = 0x03;
            int ok = 1;
            if (st->append(st, &op, 1) != 1) ok = 0;
            if (ok) {
                b4[0] = (uint8_t)(key_len & 0xFF); b4[1] = (uint8_t)((key_len >> 8) & 0xFF);
                b4[2] = (uint8_t)((key_len >> 16) & 0xFF); b4[3] = (uint8_t)((key_len >> 24) & 0xFF);
                if (st->append(st, b4, 4) != 4) ok = 0;
            }
            if (ok && st->append(st, key, key_len) != (int)key_len) ok = 0;
            if (ok) {
                b4[0] = 0; b4[1] = 0; b4[2] = 0; b4[3] = 0;
                if (st->append(st, b4, 4) != 4) ok = 0;
            }
            if (!ok) goto restore_wal;
        }
        if (st->sync && st->sync(st) != 0) goto restore_wal;
    }
    return 0;
restore_wal:
    if (pre_len_valid && st->truncate) st->truncate(st, (uint32_t)pre_len);
restore:
    term = art_find_terminal(&db->hot.index, key, key_len);
    if (term) {
        if (!pre_existed) {
            term->is_end = 0; term->val_ptr = NULL; term->val_len = 0;
            term->timestamp = 0; term->version = 0;
        } else {
            term->is_end = (uint8_t)pre_was_end; term->val_ptr = pre_val_ptr;
            term->val_len = pre_val_len; term->timestamp = pre_ts; term->version = pre_ver;
        }
    }
    db->hot.index.commit_seq = pre_seq;
    db->hot.index.max_timestamp = pre_max;
    return fail_code;
}

int segdb_delete(SegDB* db, const uint8_t* key, uint32_t key_len) {
    ARTNode* term;
    int g;
    SegdbHistEntry he;
    int have_hist = 0;
    if (!db || !key || key_len == 0 || key_len > YOWZA_MAX_KEY_LEN) return -1;
    g = segdb_write_guard(db);
    if (g != 0) return g;
    term = art_find_terminal(&db->hot.index, key, key_len);
    if (term && term->is_end) {
        /* GATE 1: record the live version before tombstoning
         * it, so a snapshot taken before the delete still
         * reads the prior value (same fail-closed rule as put). */
        int rc;
        if (segdb_history_capture(db, key, key_len, &he, &have_hist) != 0) return -3;
        rc = yowza_db_delete_raw(&db->hot, key, (size_t)key_len);
        if (rc == 0 && have_hist) {
            if (segdb_hist_commit(db, &he) != 0) return -3;
        } else if (rc != 0) {
            segdb_hist_release_reservation(db);
        }
        return rc;
    }
    if (term && !term->is_end && term->timestamp > 0) {
        return 0; /* already tombstoned in hot: nothing to do */
    }
    /* Hot-absent (or intermediate-only node): create the shadowing
     * tombstone via segdb_delete_absent, not via the VM no-op path. */
    return segdb_delete_absent(db, key, key_len, term);
}

/* Point lookup on one segment object, defined below next to
 * cur_seek_open (it shares that helper's verified discovery
 * cache). Returns seg_lookup's 0/1/-1; on 0 with a live entry
 * the value lands in val_buf, exactly as a direct seg_lookup
 * call would. The reader and its index view live on the
 * helper's stack and are closed before it returns, so the
 * caller keeps no reader state between segments. */
static int segdb_seg_lookup(SegDB* db, const char* obj,
                            const uint8_t* key, uint32_t key_len,
                            SegEntry* e_out, uint8_t* val_buf,
                            uint32_t val_cap);

int segdb_get(SegDB* db, const uint8_t* key, uint32_t key_len,
              uint8_t** out, uint32_t* out_len) {
    ARTNode* term;
    uint32_t i;
    if (!db || !key || key_len == 0 || !out || !out_len) return -1;
    term = art_find_terminal(&db->hot.index, key, key_len);
    if (term) {
        if (term->is_end) {
            *out = term->val_ptr;
            *out_len = term->val_len;
            return 0;
        }
        if (term->timestamp > 0) {
            return -1; /* hot tombstone: newest wins, no fall-through */
        }
        /* Intermediate node (timestamp 0, not an end): not a key,
         * fall through to segments. */
    }
    /* Segments newest-first. In directory mode, groups are searched
     * newest group first; within a group, newest segment first.
     * Publication order == sequence order (seq_base only grows),
     * so the first hit is the newest version: a tombstone hit
     * returns NOT FOUND immediately, never falling through to
     * older groups. */
    if (db->has_dir) {
        uint32_t gi;
        for (gi = db->group_count; gi > 0; gi--) {
            const Manifest* gm;
            uint32_t si;
            if (gi - 1 == db->group_count - 1) {
                gm = &db->manifest;
            } else {
                if (manifest_read(db->store, db->group_objs[gi - 1], &g_group_scratch) != 0) return -4; /* lookup error, not absence */
                gm = &g_group_scratch;
            }
            for (si = gm->segment_count; si > 0; si--) {
                SegEntry e;
                int lrc;
                lrc = segdb_seg_lookup(db, gm->segments[si - 1].obj, key, key_len,
                                       &e, db->get_buf, (uint32_t)sizeof(db->get_buf));
                if (lrc == 0) {
                    if (e.is_tombstone) return -1; /* newest hit is a tombstone */
                    *out = db->get_buf;
                    *out_len = e.val_len;
                    return 0;
                }
                if (lrc != 1) return -4; /* lookup error: fail, never skip; -4, not absence */
            }
        }
        return -1;
    }
    for (i = db->manifest.segment_count; i > 0; i--) {
        SegEntry e;
        int lrc;
        /* Segment value lands directly in get_buf (seg_lookup's
         * val_buf); a value larger than get_buf fails the lookup
         * with -1, never overflows. */
        lrc = segdb_seg_lookup(db, db->manifest.segments[i - 1].obj, key, key_len,
                               &e, db->get_buf, (uint32_t)sizeof(db->get_buf));
        if (lrc == 0) {
            if (e.is_tombstone) return -1; /* newest segment hit is a tombstone */
            *out = db->get_buf;
            *out_len = e.val_len;
            return 0;
        }
        if (lrc != 1) return -4; /* lookup error: fail, never skip; -4, not absence */
    }
    return -1;
}

/* ---- scan ---- */
typedef struct {
    const uint8_t* prefix;
    uint32_t prefix_len;
    uint64_t seq_base;
    uint32_t count;
    uint32_t data_off;
    int overflow;
} ScanCollectCtx;

/* Copy bytes into g_scan_data; returns pointer or NULL on overflow. */
static const uint8_t* scan_data_copy(ScanCollectCtx* c, const uint8_t* data, uint32_t len) {
    const uint8_t* p;
    if (c->data_off + len > SEGDB_SCAN_DATA) return NULL;
    p = &g_scan_data[c->data_off];
    seg_memcpy(&g_scan_data[c->data_off], data, len);
    c->data_off += len;
    return p;
}

static int segdb_scan_hot_cb(void* vctx, const uint8_t* key, uint32_t key_len,
                             const uint8_t* val, uint32_t val_len,
                             int is_tombstone, uint64_t timestamp,
                             uint64_t version) {
    ScanCollectCtx* c = (ScanCollectCtx*)vctx;
    SegCand* cand;
    const uint8_t* kcopy;
    if (!seg_prefix_match(key, key_len, c->prefix, c->prefix_len)) return 0;
    if (c->count >= SEGDB_SCAN_MAX_CANDS) { c->overflow = 1; return 1; }
    kcopy = scan_data_copy(c, key, key_len);
    if (!kcopy) { c->overflow = 1; return 1; }
    cand = &g_scan_cands[c->count];
    cand->key = kcopy;
    cand->key_len = key_len;
    cand->val = val; /* arena pointer: stable, no mutation during scan */
    cand->val_len = val_len;
    cand->tomb = is_tombstone;
    cand->seq = c->seq_base + version;
    cand->crdt_ts = timestamp;
    cand->prio = 1;
    c->count++;
    return 0;
}

static void seg_cand_sort(SegCand* a, uint32_t n) {
    uint32_t i, j;
    for (i = 1; i < n; i++) {
        SegCand tmp = a[i];
        j = i;
        while (j > 0) {
            int kc = seg_key_cmp(a[j - 1].key, a[j - 1].key_len, tmp.key, tmp.key_len);
            int move = 0;
            if (kc > 0) move = 1;
            else if (kc == 0) {
                if (a[j - 1].seq < tmp.seq) move = 1;
                else if (a[j - 1].seq == tmp.seq && a[j - 1].prio < tmp.prio) move = 1;
            }
            if (!move) break;
            a[j] = a[j - 1];
            j--;
        }
        a[j] = tmp;
    }
}

/* GATE 1 history merge helper (defined here, after SegCand /
 * ScanCollectCtx / g_scan_cands exist). snapshot is the literal
 * snapshot the scan reads at. dedupe (page/cursor path): at most
 * one history candidate per key — the newest history version at
 * or below snapshot — and only when no candidate for that key is
 * already present (the page/cursor hot list holds one candidate
 * per key, so a second would break its merge). When dedupe is
 * clear (segdb_scan path), every qualifying history entry is
 * appended; that scan's per-key group pick (highest seq at or
 * below the limit wins) resolves multiple versions itself.
 * Returns 0, or -1 on candidate overflow. */
static int segdb_history_append_cands(SegDB* db, ScanCollectCtx* ctx,
                                      const uint8_t* prefix, uint32_t prefix_len,
                                      uint64_t snapshot, int dedupe,
                                      uint32_t hot_count) {
    uint32_t j;
    SegdbHistIter hit;
    SegdbHistEntry* h;
    segdb_hist_iter_start(db, &hit);
    while (segdb_hist_iter_next(db, &hit, &h)) {
        SegCand* cand;
        int found = -1;
        if (h->seq > snapshot) continue;
        if (!seg_prefix_match(h->key, h->key_len, prefix, prefix_len)) continue;
        if (dedupe) {
            for (j = 0; j < ctx->count; j++) {
                if (seg_key_equal(g_scan_cands[j].key, g_scan_cands[j].key_len,
                                  h->key, h->key_len)) { found = (int)j; break; }
            }
            if (found >= 0) {
                if ((uint32_t)found >= hot_count && h->seq > g_scan_cands[found].seq) {
                    /* Newer history version for the same key:
                     * replace the previously appended one. */
                    g_scan_cands[found].val = h->val;
                    g_scan_cands[found].val_len = h->val_len;
                    g_scan_cands[found].tomb = h->tomb;
                    g_scan_cands[found].seq = h->seq;
                    g_scan_cands[found].crdt_ts = h->crdt_ts;
                }
                continue; /* hot candidate wins, or already appended */
            }
        }
        if (ctx->count >= SEGDB_SCAN_MAX_CANDS) return -1;
        cand = &g_scan_cands[ctx->count];
        cand->key = h->key;
        cand->key_len = h->key_len;
        cand->val = h->val;
        cand->val_len = h->val_len;
        cand->tomb = h->tomb;
        cand->seq = h->seq;
        cand->crdt_ts = h->crdt_ts;
        cand->prio = 1;
        ctx->count++;
    }
    return 0;
}

int segdb_scan(SegDB* db, const uint8_t* prefix, uint32_t prefix_len,
               uint64_t snapshot_seq, uint8_t* out_buffer, uint32_t out_size,
               uint32_t* count_out) {
    ScanCollectCtx ctx;
    uint32_t i, j;
    uint64_t cur, limit;
    uint32_t out_off = 0;
    uint32_t out_count = 0;
    if (!db || !count_out) return -1;
    if (prefix_len > 0 && !prefix) return -1;
    if (!out_buffer && out_size > 0) return -1;
    *count_out = 0;
    ctx.prefix = prefix;
    ctx.prefix_len = prefix_len;
    ctx.seq_base = db->seq_base;
    ctx.count = 0;
    ctx.data_off = 0;
    ctx.overflow = 0;
    /* art_iterate returns 0 (full) / 1 (callback stopped early, here
     * only on overflow) / -1 (bad arg): any nonzero fails the scan. */
    if (art_iterate(&db->hot.index, segdb_scan_hot_cb, &ctx) != 0) return -1;
    if (ctx.overflow) return -1;
    /* GATE 1: superseded hot versions, merged by the group pick
     * below (highest seq at or below the limit wins). */
    {
        uint64_t cur0 = db->seq_base + db->hot.index.commit_seq;
        uint64_t lim0 = snapshot_seq == 0 ? cur0 : snapshot_seq;
        if (segdb_history_append_cands(db, &ctx, prefix, prefix_len, lim0, 0, ctx.count) != 0) return -1;
    }
    /* Segments, one reader at a time: candidates are COPIED into
     * g_scan_data before the next iteration overwrites the reader
     * buffers, so no reader needs to stay open. In directory mode
     * every group's segments are collected (the sort dedupes by
     * commit_seq, which is global across groups). */
    {
        uint32_t gi, n_groups = db->has_dir ? db->group_count : 1;
        for (gi = 0; gi < n_groups; gi++) {
            const Manifest* gm;
            if (db->has_dir) {
                if (gi == db->group_count - 1) {
                    gm = &db->manifest;
                } else {
                    if (manifest_read(db->store, db->group_objs[gi], &g_group_scratch) != 0) return -1;
                    gm = &g_group_scratch;
                }
            } else {
                gm = &db->manifest;
            }
            for (i = 0; i < gm->segment_count; i++) {
        SegReader r;
        SegEntry e;
        uint8_t kb[1024];
        uint8_t vb[1024];
        int irc;
        if (seg_open(db->store, gm->segments[i].obj, &r) != 0) return -1;
        while ((irc = seg_iter_next(&r, &e, kb, (uint32_t)sizeof(kb),
                                    vb, (uint32_t)sizeof(vb))) == 0) {
            SegCand* cand;
            const uint8_t* kcopy;
            const uint8_t* vcopy;
            if (!seg_prefix_match(e.key, e.key_len, prefix, prefix_len)) continue;
            if (ctx.count >= SEGDB_SCAN_MAX_CANDS) { seg_close(&r); return -1; }
            kcopy = scan_data_copy(&ctx, e.key, e.key_len);
            if (!kcopy) { seg_close(&r); return -1; }
            vcopy = e.val_len > 0 ? scan_data_copy(&ctx, e.val, e.val_len) : kcopy;
            if (e.val_len > 0 && !vcopy) { seg_close(&r); return -1; }
            cand = &g_scan_cands[ctx.count];
            cand->key = kcopy;
            cand->key_len = e.key_len;
            cand->val = e.val_len > 0 ? vcopy : NULL;
            cand->val_len = e.val_len;
            cand->tomb = e.is_tombstone;
            cand->seq = e.commit_seq;
            cand->crdt_ts = e.crdt_ts;
            cand->prio = 0;
            ctx.count++;
        }
        seg_close(&r);
        if (irc != 1) return -1; /* iteration error */
            } /* segments of this group */
        } /* groups */
    } /* group block */
    seg_cand_sort(g_scan_cands, ctx.count);
    cur = db->seq_base + db->hot.index.commit_seq;
    limit = snapshot_seq == 0 ? cur : snapshot_seq;
    i = 0;
    while (i < ctx.count) {
        uint32_t gstart = i;
        uint32_t gend = i + 1;
        const SegCand* pick = NULL;
        while (gend < ctx.count &&
               seg_key_equal(g_scan_cands[gend].key, g_scan_cands[gend].key_len,
                             g_scan_cands[gstart].key, g_scan_cands[gstart].key_len)) {
            gend++;
        }
        for (j = gstart; j < gend; j++) {
            if (g_scan_cands[j].seq <= limit) { pick = &g_scan_cands[j]; break; }
        }
        if (pick && !pick->tomb) {
            uint64_t need = (uint64_t)4 + pick->key_len + 4 + pick->val_len;
            if ((uint64_t)out_off + need > out_size) return -1;
            out_buffer[out_off++] = (uint8_t)(pick->key_len & 0xFF);
            out_buffer[out_off++] = (uint8_t)((pick->key_len >> 8) & 0xFF);
            out_buffer[out_off++] = (uint8_t)((pick->key_len >> 16) & 0xFF);
            out_buffer[out_off++] = (uint8_t)((pick->key_len >> 24) & 0xFF);
            seg_memcpy(&out_buffer[out_off], pick->key, pick->key_len);
            out_off += pick->key_len;
            out_buffer[out_off++] = (uint8_t)(pick->val_len & 0xFF);
            out_buffer[out_off++] = (uint8_t)((pick->val_len >> 8) & 0xFF);
            out_buffer[out_off++] = (uint8_t)((pick->val_len >> 16) & 0xFF);
            out_buffer[out_off++] = (uint8_t)((pick->val_len >> 24) & 0xFF);
            seg_memcpy(&out_buffer[out_off], pick->val, pick->val_len);
            out_off += pick->val_len;
            out_count++;
        }
        i = gend;
    }
    *count_out = out_count;
    return 0;
}

/* ---- shared manifest publication (flush + compact) ----
 * Track A three-state publish. Returns 0 (published and durable),
 * -1 (definite failure BEFORE the rename that matters: the old
 * CURRENT still names the old manifest), or -2 INDETERMINATE (a
 * rename happened but a durability sync after it failed, so the
 * new manifest may or may not be the durable current one).
 * Callers must NOT treat -2 as -1: on -2 the SegDB is marked
 * indeterminate, its pending manifest is kept, and writes are
 * refused until segdb_reconcile re-reads CURRENT.
 *
 * Order: manifest object -> CURRENT.bak pointer -> CURRENT
 * pointer -> directory sync. CURRENT.bak exists so that losing
 * exactly one pointer object (CURRENT deleted/corrupt) still
 * recovers from the backup; losing CURRENT, CURRENT.bak AND
 * manifest.1 together remains indistinguishable from a virgin
 * store (documented in segdb.h and TRACKA_NOTES.md — no object
 * enumeration exists in the vtable to tell them apart). */
/* ---- multi-level helpers (Track A item 3) ---- */

/* Byte-wise struct copies that never become a memcpy import in
 * the wasm32 build: that target has no bulk-memory flag, so clang
 * lowers a 285 KiB Manifest assignment to an env.memcpy import
 * that wazero cannot satisfy (instantiation fails). A manual loop
 * under -fno-builtin stays a loop. */
static void segdb_manifest_copy(Manifest* dst, const Manifest* src) {
    uint64_t i;
    for (i = 0; i < (uint64_t)sizeof(Manifest); i++) ((uint8_t*)dst)[i] = ((const uint8_t*)src)[i];
}

static void segdb_meta_copy(SegMeta* dst, const SegMeta* src) {
    uint64_t i;
    for (i = 0; i < (uint64_t)sizeof(SegMeta); i++) ((uint8_t*)dst)[i] = ((const uint8_t*)src)[i];
}

static void segdb_copy_name(char* dst, const char* src) {
    uint32_t p = 0;
    while (src[p] != '\0' && p + 1 < MANIFEST_MAX_OBJ_LEN + 1) {
        dst[p] = src[p];
        p++;
    }
    dst[p] = '\0';
}

static int segdb_names_equal(const char* a, const char* b) {
    uint32_t i = 0;
    while (a[i] != '\0' || b[i] != '\0') {
        if (a[i] != b[i]) return 0;
        i++;
    }
    return 1;
}

static int segdb_group_lists_equal(const char (*a)[MANIFEST_MAX_OBJ_LEN + 1], uint32_t na,
                                   const char (*b)[MANIFEST_MAX_OBJ_LEN + 1], uint32_t nb) {
    uint32_t i;
    if (na != nb) return 0;
    for (i = 0; i < na; i++) {
        if (!segdb_names_equal(a[i], b[i])) return 0;
    }
    return 1;
}

static int segdb_metas_equal(const SegMeta* a, const SegMeta* b) {
    uint32_t i;
    if (!segdb_names_equal(a->obj, b->obj)) return 0;
    if (a->first_key_len != b->first_key_len || a->last_key_len != b->last_key_len) return 0;
    for (i = 0; i < a->first_key_len; i++) if (a->first_key[i] != b->first_key[i]) return 0;
    for (i = 0; i < a->last_key_len; i++) if (a->last_key[i] != b->last_key[i]) return 0;
    return a->min_seq == b->min_seq && a->max_seq == b->max_seq &&
           a->entry_count == b->entry_count && a->index_type == b->index_type &&
           a->model_version == b->model_version && a->byte_size == b->byte_size &&
           a->checksum == b->checksum;
}

static int segdb_manifests_equal(const Manifest* a, const Manifest* b) {
    uint32_t i;
    if (a->generation != b->generation || a->checkpoint_seq != b->checkpoint_seq ||
        a->schema_version != b->schema_version || a->segment_count != b->segment_count) return 0;
    for (i = 0; i < a->segment_count; i++) {
        if (!segdb_metas_equal(&a->segments[i], &b->segments[i])) return 0;
    }
    return 1;
}

static void segdb_clear_pending(SegDB* db) {
    db->has_pending = 0;
    db->indeterminate = 0;
    db->pending_dir_transition = 0;
    db->pending_group_add = 0;
    db->pending_group_append = 0;
    db->pending_group_count = 0;
    db->pending_old_group_count = 0;
}

/* Publish CURRENT.bak -> ptr_name, then CURRENT -> ptr_name, then
 * sync dir. Three-state: 0 ok, -1 definite failure before CURRENT
 * moved, -2 indeterminate (rename happened, durability sync after
 * it failed). Shared by single-manifest publish and by the
 * single->directory transition (ptr_name = "manifest.dir"). */
static int segdb_publish_pointer(SegDB* db, const char* ptr_name) {
    int prc;
    if (manifest_write_current(db->store, "CURRENT.bak.tmp", ptr_name) != 0) return -1;
    prc = manifest_publish(db->store, "CURRENT.bak.tmp", "CURRENT.bak");
    if (prc == -2) return -2;
    if (prc != 0) return -1;
    if (manifest_write_current(db->store, "CURRENT.tmp", ptr_name) != 0) return -1;
    prc = manifest_publish(db->store, "CURRENT.tmp", "CURRENT");
    if (prc == -2) return -2;
    if (prc != 0) return -1;
    if (store_sync_dir(db->store) != 0) return -2;
    return 0;
}

static int segdb_publish_manifest(SegDB* db, Manifest* m) {
    char tmp[SEGDB_NAME_BUF];
    char final_name[SEGDB_NAME_BUF];
    int prc;
    if (seg_name_manifest_tmp(m->generation, tmp, sizeof(tmp)) != 0) return -1;
    if (seg_name_manifest(m->generation, final_name, sizeof(final_name)) != 0) return -1;
    if (manifest_write(db->store, tmp, m) != 0) return -1;
    prc = manifest_publish(db->store, tmp, final_name);
    if (prc == -2) return -2;
    if (prc != 0) return -1;
    /* Backup pointer first: if CURRENT itself is later lost, the
     * backup still names this generation. A definite bak failure
     * aborts before CURRENT moves (safe); an indeterminate bak
     * failure is reported as indeterminate overall. */
    return segdb_publish_pointer(db, final_name);
}

/* Post-adoption hot clear shared by all flush paths: truncate the
 * WAL BEFORE resetting the arena, so a truncate failure still
 * leaves hot intact (the manifest is already published; retrying
 * the flush is safe because the hot entries are still there and
 * flushing them again writes a duplicate segment with identical
 * values). Returns 0 on success, -1 on truncate failure. */
static int segdb_flush_clear_hot(SegDB* db) {
    if (db->hot.storage.truncate(&db->hot.storage, 0) != 0) return -1;
    arena_reset(&db->hot.arena);
    art_init(&db->hot.index, &db->hot.arena);
    /* GATE 1: history was transferred into segments by this
     * flush (or was empty); its arena key/value pointers are
     * dead after the reset, so the table must empty here — and
     * only here, never on a failed flush. */
    segdb_hist_clear(db);
    return 0;
}

/* Verify a freshly written segment: open it, run full discovery
 * (block/index/Bloom checksums), count entries, and check first/last
 * keys against the collected entries. First/last keys are copied
 * into locals immediately: the iteration buffers are overwritten by
 * every iteration, so comparing entry pointers after the loop would
 * be wrong. */
static int segdb_verify_segment(YowzaStore* store, const char* obj,
                                uint32_t expect_count,
                                const uint8_t* first_key, uint32_t first_len,
                                const uint8_t* last_key, uint32_t last_len) {
    SegReader r;
    SegEntry e;
    uint8_t kb[1024];
    uint8_t vb[1024];
    uint8_t fk[1024], lk[1024];
    uint32_t fk_len = 0, lk_len = 0;
    uint32_t cnt = 0;
    int rc;
    if (seg_open(store, obj, &r) != 0) return -1;
    if (seg_ensure_discovered(&r) != 0) { seg_close(&r); return -1; }
    while ((rc = seg_iter_next(&r, &e, kb, (uint32_t)sizeof(kb),
                               vb, (uint32_t)sizeof(vb))) == 0) {
        if (cnt == 0) {
            fk_len = e.key_len;
            seg_memcpy(fk, e.key, e.key_len);
        }
        lk_len = e.key_len;
        seg_memcpy(lk, e.key, e.key_len);
        cnt++;
    }
    seg_close(&r);
    if (rc != 1) return -1;
    if (cnt != expect_count) return -1;
    if (expect_count > 0) {
        if (!seg_key_equal(fk, fk_len, first_key, first_len)) return -1;
        if (!seg_key_equal(lk, lk_len, last_key, last_len)) return -1;
    }
    return 0;
}

/* ---- flush ---- */
typedef struct {
    uint64_t seq_base;
    uint32_t count;
    uint32_t key_off;
    uint64_t min_seq;
    uint64_t max_seq;
    int overflow;
} FlushCollectCtx;

static int segdb_flush_collect_cb(void* vctx, const uint8_t* key, uint32_t key_len,
                                  const uint8_t* val, uint32_t val_len,
                                  int is_tombstone, uint64_t timestamp,
                                  uint64_t version) {
    FlushCollectCtx* c = (FlushCollectCtx*)vctx;
    SegEntry* e;
    uint64_t eff;
    if (c->count >= SEGDB_FLUSH_MAX_ENTRIES || c->key_off + key_len > SEGDB_KEY_SCRATCH) {
        c->overflow = 1;
        return 1;
    }
    e = &g_flush_entries[c->count];
    seg_memcpy(&g_flush_keys[c->key_off], key, key_len);
    e->key = &g_flush_keys[c->key_off];
    c->key_off += key_len;
    e->key_len = key_len;
    e->val = val;
    e->val_len = val_len;
    e->is_tombstone = is_tombstone;
    eff = c->seq_base + version;
    e->commit_seq = eff;
    e->crdt_ts = timestamp;
    if (c->count == 0 || eff < c->min_seq) c->min_seq = eff;
    if (c->count == 0 || eff > c->max_seq) c->max_seq = eff;
    c->count++;
    return 0;
}

/* GATE 1 history-segment writer, Axis C rewrite (see the
 * Gate 1 section). Partitions epoch history into segments
 * with no duplicate key per segment: entries take a
 * version-rank = the number of LATER history entries for
 * the same key (rank 0 = newest history version per key;
 * per-key history appears in seq-ascending insertion order,
 * so "later in the list" is "newer version" for that key).
 * All entries of one rank form rank segments, published
 * BEFORE the current segment, so manifest order is
 * [history ranks..., current] and reads resolve by
 * sequence, not by position: get walks the manifest
 * newest-first (current segment first), scans merge by seq.
 *
 * Differences from the old writer, stated plainly:
 *  - No 32-rank cap (SEGDB_HIST_SEG_MAX is gone): ranks
 *    are split across group manifests by the multi-group
 *    flush below, each group within the 128-segment cap.
 *    A pre-guard WAL tail with >32 versions of one key
 *    now flushes instead of failing loudly.
 *  - No 1024-entry total cap: history comes from the
 *    array + overflow list via the iterator, and a rank
 *    with more than 1024 distinct keys is split into
 *    several key-range chunk segments (still one entry
 *    per key per segment, keys disjoint across chunks,
 *    so reads are unaffected).
 *  - Two bounded scratch tables remain, and they are
 *    LOUD limits, not silent ones: g_hist_rank /
 *    g_hist_rank_distinct cap a single flush epoch at
 *    SEGDB_HIST_FLUSH_SCRATCH_MAX entries (16384). Past
 *    that, rank computation fails with -1 BEFORE any
 *    segment is written, history stays intact, and the
 *    caller (or the Go epoch guard, which keeps facade
 *    epochs at <= 900) must flush in smaller epochs.
 *    Rank computation is O(N^2) key comparisons (one
 *    forward walk per entry, no allocation — allocating
 *    a sort buffer from the hot arena during flush
 *    would fail exactly when the arena is fullest, i.e.
 *    when a flush is most needed).
 *  - Metas are delivered to a caller sink (append to a
 *    group manifest, or publish full groups as they
 *    fill), never materialised as one giant array: a
 *    flush with thousands of rank segments never needs
 *    thousands of SegMetas in memory at once.
 * On any failure returns -1 with the manifest untouched
 * (written segments are orphans) and history NOT cleared.
 * Segment ids are next_seg_id + generated-index; the
 * current segment takes the following id. */
#define SEGDB_HIST_FLUSH_SCRATCH_MAX 16384u
static uint32_t g_hist_rank[SEGDB_HIST_FLUSH_SCRATCH_MAX];
static uint32_t g_hist_rank_distinct[SEGDB_HIST_FLUSH_SCRATCH_MAX];
static uint32_t g_hist_rank_max; /* number of ranks (0 when no history) */
static uint32_t g_hist_rank_total_segs; /* rank segments incl. chunks */
static SegEntry g_hist_buf[SEGDB_HIST_MAX];

/* Compute ranks for the current epoch into g_hist_rank
 * (g_hist_rank[i] = rank of the i-th history entry in
 * insertion order) and g_hist_rank_total_segs. Returns 0,
 * or -1 when the epoch exceeds the scratch cap. */
static int segdb_hist_compute_ranks(SegDB* db) {
    uint32_t total, i, s;
    SegdbHistIter outer;
    SegdbHistEntry* h;
    total = segdb_hist_total(db);
    g_hist_rank_max = 0;
    g_hist_rank_total_segs = 0;
    if (total == 0) return 0;
    if (total > SEGDB_HIST_FLUSH_SCRATCH_MAX) return -1;
    for (i = 0; i < total; i++) g_hist_rank_distinct[i] = 0;
    segdb_hist_iter_start(db, &outer);
    i = 0;
    while (segdb_hist_iter_next(db, &outer, &h)) {
        SegdbHistIter inner = outer; /* position just past entry i */
        SegdbHistEntry* h2;
        uint32_t rank = 0;
        while (segdb_hist_iter_next(db, &inner, &h2)) {
            if (seg_key_equal(h->key, h->key_len, h2->key, h2->key_len)) rank++;
        }
        g_hist_rank[i] = rank;
        g_hist_rank_distinct[rank]++;
        if (rank + 1 > g_hist_rank_max) g_hist_rank_max = rank + 1;
        i++;
    }
    if (i != total) return -1; /* history moved mid-computation: fail closed */
    for (s = 0; s < g_hist_rank_max; s++) {
        g_hist_rank_total_segs += (g_hist_rank_distinct[s] + SEGDB_HIST_MAX - 1) / SEGDB_HIST_MAX;
    }
    return 0;
}

/* Write ONE key-range chunk of one rank as a segment:
 * the smallest (at most 1024) rank entries with key
 * strictly greater than the cursor (no cursor when
 * has_cursor is 0), sorted by key. Returns 0 with
 * *count_out = entries written (0 = no more entries
 * for this rank past the cursor; no segment written),
 * or -1 on any failure (including a duplicate key
 * within the rank, which cannot occur for well-formed
 * history and fails closed). The meta is filled only
 * when *count_out > 0. */
static int segdb_flush_write_rank_chunk(SegDB* db, uint32_t rank,
                                        const uint8_t* cursor_key, uint32_t cursor_len,
                                        int has_cursor, uint64_t seg_id,
                                        SegMeta* meta_out, uint32_t* count_out) {
    SegdbHistIter it;
    SegdbHistEntry* h;
    uint32_t idx = 0, cnt = 0;
    uint64_t min_seq = 0, max_seq = 0, out_bytes = 0, cksum = 0;
    char seg_obj[SEGDB_NAME_BUF];
    *count_out = 0;
    segdb_hist_iter_start(db, &it);
    while (segdb_hist_iter_next(db, &it, &h)) {
        SegEntry e;
        uint32_t pos, k;
        if (g_hist_rank[idx] != rank) { idx++; continue; }
        idx++;
        if (has_cursor && seg_key_cmp(h->key, h->key_len, cursor_key, cursor_len) <= 0) continue;
        e.key = h->key;
        e.key_len = h->key_len;
        e.val = h->tomb ? NULL : h->val;
        e.val_len = h->tomb ? 0 : h->val_len;
        e.is_tombstone = h->tomb;
        e.commit_seq = h->seq;
        e.crdt_ts = h->crdt_ts;
        /* Bounded-smallest insertion: keep g_hist_buf sorted
         * and holding the smallest keys seen so far. */
        pos = 0;
        while (pos < cnt) {
            int kc = seg_key_cmp(g_hist_buf[pos].key, g_hist_buf[pos].key_len,
                                 e.key, e.key_len);
            if (kc == 0) return -1; /* duplicate key in one rank */
            if (kc > 0) break;
            pos++;
        }
        if (cnt < SEGDB_HIST_MAX) {
            for (k = cnt; k > pos; k--) g_hist_buf[k] = g_hist_buf[k - 1];
            g_hist_buf[pos] = e;
            cnt++;
        } else if (pos < cnt) {
            for (k = cnt - 1; k > pos; k--) g_hist_buf[k] = g_hist_buf[k - 1];
            g_hist_buf[pos] = e; /* largest entry drops out */
        }
    }
    if (cnt == 0) return 0;
    /* min/max over the FINAL buffer contents only: entries
     * dropped by the bounded-smallest rule belong to a
     * later chunk and must not leak into this meta. */
    {
        uint32_t k;
        min_seq = g_hist_buf[0].commit_seq;
        max_seq = g_hist_buf[0].commit_seq;
        for (k = 1; k < cnt; k++) {
            if (g_hist_buf[k].commit_seq < min_seq) min_seq = g_hist_buf[k].commit_seq;
            if (g_hist_buf[k].commit_seq > max_seq) max_seq = g_hist_buf[k].commit_seq;
        }
    }
    if (seg_name_seg(seg_id, seg_obj, sizeof(seg_obj)) != 0) return -1;
    if (seg_write(db->store, seg_obj, g_hist_buf, cnt, 0, 0, 1, &out_bytes) != 0) return -1;
    if (segdb_verify_segment(db->store, seg_obj, cnt,
                             g_hist_buf[0].key, g_hist_buf[0].key_len,
                             g_hist_buf[cnt - 1].key, g_hist_buf[cnt - 1].key_len) != 0) return -1;
    if (store_sync_obj(db->store, seg_obj) != 0) return -1;
    if (seg_file_checksum(db->store, seg_obj, &cksum) != 0) return -1;
    segdb_copy_name(meta_out->obj, seg_obj);
    meta_out->first_key_len = g_hist_buf[0].key_len;
    seg_memcpy(meta_out->first_key, g_hist_buf[0].key, g_hist_buf[0].key_len);
    meta_out->last_key_len = g_hist_buf[cnt - 1].key_len;
    seg_memcpy(meta_out->last_key, g_hist_buf[cnt - 1].key, g_hist_buf[cnt - 1].key_len);
    meta_out->min_seq = min_seq;
    meta_out->max_seq = max_seq;
    meta_out->entry_count = cnt;
    meta_out->index_type = 0;
    meta_out->model_version = 0;
    meta_out->byte_size = out_bytes;
    meta_out->checksum = cksum;
    *count_out = cnt;
    return 0;
}

/* A meta sink: receives each generated history meta in
 * manifest order. Returns 0 or -1 (a sink failure aborts
 * the flush with the manifest untouched). */
typedef int (*SegdbMetaSink)(void* sink_ctx, const SegMeta* meta);

/* Append-to-manifest sink: the manifest must have room
 * for every meta (the caller checked the fit first). */
typedef struct {
    Manifest* m;
} SegdbAppendSink;

static int segdb_append_sink(void* ctx, const SegMeta* meta) {
    SegdbAppendSink* s = (SegdbAppendSink*)ctx;
    if (s->m->segment_count >= MANIFEST_MAX_SEGMENTS) return -1;
    segdb_meta_copy(&s->m->segments[s->m->segment_count], meta);
    s->m->segment_count++;
    return 0;
}

/* Generate every history segment of the epoch (ranks in
 * order, key-range chunks within a rank) and deliver
 * each meta to the sink. Segment ids start at
 * db->next_seg_id. Requires segdb_hist_compute_ranks to
 * have run. Returns 0 with *nsegs_out generated, or -1. */
static int segdb_flush_gen_history(SegDB* db, SegdbMetaSink sink, void* sink_ctx,
                                   uint32_t* nsegs_out) {
    uint32_t s, nsegs = 0;
    *nsegs_out = 0;
    for (s = 0; s < g_hist_rank_max; s++) {
        uint8_t cursor[1024];
        uint32_t cursor_len = 0;
        int has_cursor = 0;
        for (;;) {
            SegMeta meta;
            uint32_t cnt = 0;
            if (segdb_flush_write_rank_chunk(db, s,
                                             has_cursor ? cursor : NULL, cursor_len, has_cursor,
                                             db->next_seg_id + nsegs, &meta, &cnt) != 0) return -1;
            if (cnt == 0) break;
            if (sink(sink_ctx, &meta) != 0) return -1;
            nsegs++;
            /* Next chunk resumes past this chunk's last key. */
            cursor_len = meta.last_key_len;
            seg_memcpy(cursor, meta.last_key, cursor_len);
            has_cursor = 1;
        }
    }
    *nsegs_out = nsegs;
    return 0;
}

/* Flush into the last group, appending to its manifest:
 * history segments are GENERATED here (ranks in order, via
 * the append sink into ng — the caller checked that all
 * new segments fit), then the current segment meta when
 * cur_meta is non-NULL (NULL only for a history-only
 * flush, which cannot occur in practice — history keys
 * are always hot — but is handled so the invariant
 * "history is never stranded" holds even then). The group
 * manifest keeps its creation generation (its name is
 * manifest.<generation>); only its checkpoint and
 * segment list change. Commit point: the group-manifest
 * publish itself. nhist_expected is the rank-segment
 * count segdb_flush computed (generation must match it
 * exactly, or the flush fails with the manifest
 * untouched). */
static int segdb_flush_group_append(SegDB* db, const SegMeta* cur_meta, uint64_t flush_seq,
                                    uint32_t nhist_expected) {
    char tmp[SEGDB_NAME_BUF];
    char final_name[SEGDB_NAME_BUF];
    int prc;
    uint32_t nhist = 0;
    Manifest ng;
    SegdbAppendSink sink;
    segdb_manifest_copy(&ng, &db->manifest);
    ng.checkpoint_seq = flush_seq;
    sink.m = &ng;
    if (segdb_flush_gen_history(db, segdb_append_sink, &sink, &nhist) != 0) return -1;
    if (nhist != nhist_expected) return -1;
    if (cur_meta) {
        segdb_meta_copy(&ng.segments[ng.segment_count], cur_meta);
        ng.segment_count++;
    }
    if (seg_name_manifest_tmp(ng.generation, tmp, sizeof(tmp)) != 0) return -1;
    if (seg_name_manifest(ng.generation, final_name, sizeof(final_name)) != 0) return -1;
    if (manifest_write(db->store, tmp, &ng) != 0) return -1;
    segdb_manifest_copy(&db->pending_group_manifest, &ng);
    segdb_manifest_copy(&db->pending_old_group_manifest, &db->manifest);
    db->pending_next_seg_id = db->next_seg_id + nhist + (cur_meta ? 1 : 0);
    db->pending_is_compact = 0;
    db->pending_group_append = 1;
    db->has_pending = 1;
    prc = manifest_publish(db->store, tmp, final_name);
    if (prc == -2) {
        db->indeterminate = 1;
        return SEGDB_INDETERMINATE; /* hot untouched, WAL untouched */
    }
    if (prc != 0) {
        segdb_clear_pending(db);
        return -1;
    }
    segdb_clear_pending(db);
    segdb_manifest_copy(&db->manifest, &ng);
    db->seq_base = flush_seq;
    db->next_seg_id += nhist + (cur_meta ? 1 : 0);
    return segdb_flush_clear_hot(db);
}

/* Multi-group flush sink: history/current metas fill one
 * fresh group manifest at a time (each <= 128 segments);
 * a full group is published immediately — it is
 * unreferenced until the directory publish below, so a
 * publish failure there is a definite -1, never
 * indeterminate — and its name is recorded. The last
 * published group is kept in `last` for adoption and
 * for the pending state. */
typedef struct {
    SegDB* db;
    Manifest cur;
    uint64_t cur_gen;
    Manifest last;
    char names[MANIFEST_DIR_MAX_GROUPS][MANIFEST_MAX_OBJ_LEN + 1];
    uint32_t n_names;
    uint64_t flush_seq;
} SegdbMultiSink;

static int segdb_multi_publish_group(SegdbMultiSink* s) {
    char tmp[SEGDB_NAME_BUF];
    char final_name[SEGDB_NAME_BUF];
    int prc;
    if (s->n_names >= MANIFEST_DIR_MAX_GROUPS) return -1;
    if (seg_name_manifest_tmp(s->cur_gen, tmp, sizeof(tmp)) != 0) return -1;
    if (seg_name_manifest(s->cur_gen, final_name, sizeof(final_name)) != 0) return -1;
    if (manifest_write(s->db->store, tmp, &s->cur) != 0) return -1;
    prc = manifest_publish(s->db->store, tmp, final_name);
    if (prc != 0) return -1; /* unreferenced: -2 downgraded to -1 */
    segdb_copy_name(s->names[s->n_names], final_name);
    s->n_names++;
    segdb_manifest_copy(&s->last, &s->cur);
    return 0;
}

static void segdb_multi_reset_group(SegdbMultiSink* s, uint64_t gen) {
    uint32_t i;
    s->cur_gen = gen;
    s->cur.generation = gen;
    s->cur.checkpoint_seq = s->flush_seq;
    s->cur.schema_version = s->db->manifest.schema_version;
    s->cur.segment_count = 0;
    for (i = 0; i < MANIFEST_MAX_SEGMENTS; i++) {
        s->cur.segments[i].obj[0] = '\0';
        s->cur.segments[i].first_key_len = 0;
        s->cur.segments[i].last_key_len = 0;
    }
}

static int segdb_multi_sink(void* ctx, const SegMeta* meta) {
    SegdbMultiSink* s = (SegdbMultiSink*)ctx;
    if (s->cur.segment_count >= MANIFEST_MAX_SEGMENTS) {
        if (segdb_multi_publish_group(s) != 0) return -1;
        segdb_multi_reset_group(s, s->cur_gen + 1);
    }
    segdb_meta_copy(&s->cur.segments[s->cur.segment_count], meta);
    s->cur.segment_count++;
    return 0;
}

static int segdb_multi_finish(SegdbMultiSink* s) {
    if (s->cur.segment_count == 0) return 0;
    if (segdb_multi_publish_group(s) != 0) return -1;
    segdb_multi_reset_group(s, s->cur_gen + 1);
    return 0;
}

/* Axis C — multi-group flush. Used when the flush's new
 * segments do NOT fit the last group (or the single
 * manifest): ALL new segments go into FRESH groups
 * (128 segments each), never partly into the old last
 * group. Deliberate trade-off, stated plainly: the old
 * last group's remaining slots stay empty forever
 * (segments are immutable; the next flush appends to
 * the new last group), in exchange for a SINGLE commit
 * point — splitting a flush across "append to old group
 * + new groups" would need two commit points and could
 * publish half a flush. Steps: (1) generate history
 * segments + the current segment into fresh groups,
 * publishing each full group unreferenced (-2
 * downgraded to -1); (2) publish the new directory —
 * for a transition this is unreferenced until CURRENT
 * moves (a -2 there is -1), for an addition the
 * directory publish IS the commit point (a -2 is
 * indeterminate, with the full new group list in the
 * pending state so reconcile can decide); (3)
 * transition only: publish CURRENT -> "manifest.dir"
 * (three-state). A virgin DB (no manifest yet) takes
 * the transition shape with no old group: groups =
 * new groups only. nsegs_new counts history + current
 * segments; nhist_expected is the rank-segment count
 * (generation must match it exactly). Group capacity
 * (MANIFEST_DIR_MAX_GROUPS groups total) is checked by the caller BEFORE
 * any segment is written; exceeding it fails -1 with
 * history and hot intact — the remaining loud limit
 * for a single flush epoch. */
static int segdb_flush_multi_group(SegDB* db, const SegMeta* cur_meta, uint64_t flush_seq,
                                   uint32_t nhist_expected, uint32_t nsegs_new) {
    char old_name[SEGDB_NAME_BUF];
    ManifestDir dir, new_dir;
    SegdbMultiSink sink;
    int prc, is_transition, has_old;
    uint32_t i, nhist = 0, gi;
    is_transition = !db->has_dir;
    has_old = is_transition && db->has_manifest;
    if (is_transition) {
        if (has_old) {
            if (seg_name_manifest(db->manifest.generation, old_name, sizeof(old_name)) != 0) return -1;
        }
    } else {
        if (manifest_dir_read(db->store, MANIFEST_DIR_OBJ, &dir) != 0) return -1;
    }
    sink.db = db;
    sink.flush_seq = flush_seq;
    sink.n_names = 0;
    sink.last.generation = 0;
    sink.last.segment_count = 0;
    segdb_multi_reset_group(&sink, db->manifest.generation + 1);
    if (segdb_flush_gen_history(db, segdb_multi_sink, &sink, &nhist) != 0) return -1;
    if (nhist != nhist_expected) return -1;
    if (cur_meta) {
        if (segdb_multi_sink(&sink, cur_meta) != 0) return -1;
    }
    if (segdb_multi_finish(&sink) != 0) return -1;
    if (sink.n_names == 0) return -1; /* cannot happen: nsegs_new > 0 here */
    /* Build the new directory list: old groups (if any) +
     * fresh groups, in publication order. */
    new_dir.generation = is_transition ? 1 : dir.generation + 1;
    new_dir.checkpoint_seq = flush_seq;
    new_dir.schema_version = db->manifest.schema_version;
    new_dir.group_count = 0;
    if (has_old) {
        segdb_copy_name(new_dir.groups[0], old_name);
        new_dir.group_count = 1;
    } else if (!is_transition) {
        for (i = 0; i < dir.group_count; i++)
            segdb_copy_name(new_dir.groups[new_dir.group_count++], dir.groups[i]);
    }
    for (i = 0; i < sink.n_names; i++)
        segdb_copy_name(new_dir.groups[new_dir.group_count++], sink.names[i]);
    if (manifest_dir_write(db->store, MANIFEST_DIR_TMP_OBJ, &new_dir) != 0) return -1;
    prc = manifest_publish(db->store, MANIFEST_DIR_TMP_OBJ, MANIFEST_DIR_OBJ);
    if (is_transition) {
        if (prc != 0) return -1; /* unreferenced until CURRENT moves */
        segdb_manifest_copy(&db->pending_group_manifest, &sink.last);
        db->pending_next_seg_id = db->next_seg_id + nsegs_new;
        db->pending_is_compact = 0;
        db->pending_dir_transition = 1;
        db->pending_group_count = new_dir.group_count;
        for (gi = 0; gi < new_dir.group_count; gi++)
            segdb_copy_name(db->pending_group_objs[gi], new_dir.groups[gi]);
        db->pending_old_group_count = has_old ? 1 : 0;
        if (has_old) segdb_copy_name(db->pending_old_group_objs[0], old_name);
        db->has_pending = 1;
        prc = segdb_publish_pointer(db, MANIFEST_DIR_OBJ);
        if (prc == -2) {
            db->indeterminate = 1;
            return SEGDB_INDETERMINATE;
        }
        if (prc != 0) {
            segdb_clear_pending(db);
            return -1;
        }
        segdb_clear_pending(db);
        segdb_manifest_copy(&db->manifest, &sink.last);
        db->has_manifest = 1;
        db->has_dir = 1;
        segdb_copy_name(db->dir_obj, MANIFEST_DIR_OBJ);
        db->group_count = new_dir.group_count;
        for (gi = 0; gi < new_dir.group_count; gi++)
            segdb_copy_name(db->group_objs[gi], new_dir.groups[gi]);
        db->seq_base = flush_seq;
        db->next_seg_id += nsegs_new;
        return segdb_flush_clear_hot(db);
    }
    /* Group addition: the directory publish IS the commit point. */
    if (prc == -2) {
        segdb_manifest_copy(&db->pending_group_manifest, &sink.last);
        db->pending_next_seg_id = db->next_seg_id + nsegs_new;
        db->pending_is_compact = 0;
        db->pending_group_add = 1;
        db->pending_group_count = new_dir.group_count;
        for (i = 0; i < new_dir.group_count; i++)
            segdb_copy_name(db->pending_group_objs[i], new_dir.groups[i]);
        db->pending_old_group_count = dir.group_count;
        for (i = 0; i < dir.group_count; i++)
            segdb_copy_name(db->pending_old_group_objs[i], dir.groups[i]);
        db->has_pending = 1;
        db->indeterminate = 1;
        return SEGDB_INDETERMINATE;
    }
    if (prc != 0) return -1;
    segdb_manifest_copy(&db->manifest, &sink.last);
    db->has_manifest = 1;
    db->group_count = new_dir.group_count;
    for (i = 0; i < new_dir.group_count; i++)
        segdb_copy_name(db->group_objs[i], new_dir.groups[i]);
    db->seq_base = flush_seq;
    db->next_seg_id += nsegs_new;
    return segdb_flush_clear_hot(db);
}

int segdb_flush(SegDB* db) {
    FlushCollectCtx ctx;
    char seg_obj[SEGDB_NAME_BUF];
    uint64_t out_bytes = 0;
    uint64_t flush_seq;
    uint64_t seg_cksum = 0;
    SegMeta meta;
    int prc;
    /* Axis C: history segments are GENERATED into the branch
     * manifest (append sink) or into fresh groups (multi
     * sink) by the branch functions below — never held as
     * one array. nhist is the rank-segment count from the
     * rank computation; the current segment takes id
     * next_seg_id + nhist, and history segments take the
     * lower ids, so manifest order stays [history...,
     * current] even though the current segment object is
     * written first (object write order is not manifest
     * order; segment ids are just names). */
    uint32_t nhist = 0;
    uint32_t nsegs_new = 0;
    int have_cur = 0;
    if (!db || !db->store) return -1;
    if (segdb_write_guard(db) != 0) return SEGDB_INDETERMINATE;
    if (db->hot.tx.active) return -1; /* never flush under an open tx */
    ctx.seq_base = db->seq_base;
    ctx.count = 0;
    ctx.key_off = 0;
    ctx.min_seq = 0;
    ctx.max_seq = 0;
    ctx.overflow = 0;
    if (art_iterate(&db->hot.index, segdb_flush_collect_cb, &ctx) != 0) {
        return -1; /* collection overflow or bad arg: nothing written */
    }
    if (ctx.overflow) return -1;
    if (ctx.count == 0 && segdb_hist_total(db) == 0) return 0; /* empty hot: no-op success */
    /* WAL truncate must be available before any store write: the
     * flush clears the WAL only after CURRENT is durable, and a
     * driver that cannot clear it must fail the flush before
     * resetting anything (see segdb.h). Checked here, before the
     * segment exists, so this failure leaves store AND hot intact. */
    if (!db->hot.storage.truncate || !db->hot.storage.log_length) return -1;
    /* Axis C: rank the epoch BEFORE writing anything. A
     * rank-computation failure (epoch past the scratch
     * cap) leaves the manifest, hot, and history untouched
     * for a retry in smaller epochs. */
    if (segdb_hist_total(db) > 0) {
        if (segdb_hist_compute_ranks(db) != 0) return -1;
        nhist = g_hist_rank_total_segs;
    }
    have_cur = ctx.count > 0;
    nsegs_new = nhist + (have_cur ? 1u : 0u);
    /* Multi-group capacity is checked BEFORE any segment
     * is written: the directory holds at most MANIFEST_DIR_MAX_GROUPS groups.
     * Exceeding it fails loudly here, not halfway through
     * a group publish. */
    if (nsegs_new > 0) {
        uint32_t need_groups = (nsegs_new + MANIFEST_MAX_SEGMENTS - 1) / MANIFEST_MAX_SEGMENTS;
        uint32_t fits;
        if (db->has_dir) {
            fits = db->manifest.segment_count + nsegs_new <= MANIFEST_MAX_SEGMENTS;
            if (!fits) {
                ManifestDir dcheck;
                if (manifest_dir_read(db->store, MANIFEST_DIR_OBJ, &dcheck) != 0) return -1;
                if (dcheck.group_count + need_groups > MANIFEST_DIR_MAX_GROUPS) return -1;
            }
        } else if (db->manifest.segment_count + nsegs_new > MANIFEST_MAX_SEGMENTS) {
            uint32_t old_groups = db->has_manifest ? 1 : 0;
            if (old_groups + need_groups > MANIFEST_DIR_MAX_GROUPS) return -1;
        }
    }
    if (have_cur) {
        if (seg_name_seg(db->next_seg_id + nhist, seg_obj, sizeof(seg_obj)) != 0) return -1;
        if (seg_write(db->store, seg_obj, g_flush_entries, ctx.count, 0, 0, 1, &out_bytes) != 0) {
            return -1; /* segment orphaned, old manifest untouched */
        }
        if (segdb_verify_segment(db->store, seg_obj, ctx.count,
                                 g_flush_entries[0].key, g_flush_entries[0].key_len,
                                 g_flush_entries[ctx.count - 1].key,
                                 g_flush_entries[ctx.count - 1].key_len) != 0) {
            return -1; /* segment orphaned, old manifest untouched */
        }
        if (store_sync_obj(db->store, seg_obj) != 0) return -1;
        if (seg_file_checksum(db->store, seg_obj, &seg_cksum) != 0) return -1;
        segdb_copy_name(meta.obj, seg_obj);
        meta.first_key_len = g_flush_entries[0].key_len;
        seg_memcpy(meta.first_key, g_flush_entries[0].key, g_flush_entries[0].key_len);
        meta.last_key_len = g_flush_entries[ctx.count - 1].key_len;
        seg_memcpy(meta.last_key, g_flush_entries[ctx.count - 1].key,
                   g_flush_entries[ctx.count - 1].key_len);
        meta.min_seq = ctx.min_seq;
        meta.max_seq = ctx.max_seq;
        meta.entry_count = ctx.count;
        meta.index_type = 0;
        meta.model_version = 0;
        meta.byte_size = out_bytes;
        meta.checksum = seg_cksum;
    }
    flush_seq = db->seq_base + db->hot.index.commit_seq;
    /* Branch on layout: directory append, multi-group
     * (transition or addition), or legacy single-manifest
     * append. A branch is chosen by whether ALL new
     * segments (history + current) fit the last group's
     * manifest; for a single new segment these conditions
     * reduce exactly to the old ones. The multi-group
     * branch takes ALL new segments into fresh groups —
     * it never appends partly to the old last group, so
     * the flush still has a single commit point (the
     * directory publish, or CURRENT for a transition). */
    if (db->has_dir) {
        if (db->manifest.segment_count + nsegs_new <= MANIFEST_MAX_SEGMENTS) {
            return segdb_flush_group_append(db, have_cur ? &meta : NULL, flush_seq, nhist);
        }
        return segdb_flush_multi_group(db, have_cur ? &meta : NULL, flush_seq, nhist, nsegs_new);
    }
    if (db->manifest.segment_count + nsegs_new > MANIFEST_MAX_SEGMENTS) {
        return segdb_flush_multi_group(db, have_cur ? &meta : NULL, flush_seq, nhist, nsegs_new);
    }
    /* Legacy single-manifest path: new manifest in static
     * scratch, publish, then adopt. History segments are
     * generated into the scratch manifest first, then the
     * current segment meta. */
    segdb_manifest_copy(&g_manifest_scratch, &db->manifest);
    g_manifest_scratch.generation = db->has_manifest ? db->manifest.generation + 1 : 1;
    g_manifest_scratch.checkpoint_seq = flush_seq;
    {
        SegdbAppendSink sink;
        uint32_t gen_hist = 0;
        sink.m = &g_manifest_scratch;
        if (segdb_flush_gen_history(db, segdb_append_sink, &sink, &gen_hist) != 0) return -1;
        if (gen_hist != nhist) return -1;
    }
    if (have_cur) {
        segdb_meta_copy(&g_manifest_scratch.segments[g_manifest_scratch.segment_count], &meta);
        g_manifest_scratch.segment_count++;
    }
    /* Track A: save the pending manifest BEFORE publishing, so an
     * indeterminate outcome can be reconciled (not silently
     * reverted) by segdb_reconcile. */
    segdb_manifest_copy(&db->pending_manifest, &g_manifest_scratch);
    db->has_pending = 1;
    db->pending_is_compact = 0;
    db->pending_next_seg_id = db->next_seg_id + nhist + (have_cur ? 1u : 0u);
    prc = segdb_publish_manifest(db, &g_manifest_scratch);
    if (prc == -2) {
        db->indeterminate = 1;
        return SEGDB_INDETERMINATE; /* hot untouched, WAL untouched */
    }
    if (prc != 0) {
        db->has_pending = 0;
        return -1; /* old CURRENT still valid; new segments orphaned */
    }
    db->has_pending = 0;
    /* CURRENT is durable: adopt, then clear hot (which also
     * clears the transferred history — see
     * segdb_flush_clear_hot). WAL truncate BEFORE arena reset,
     * so a truncate failure still leaves hot intact (manifest
     * already published; retrying the flush is safe because the
     * hot entries are still there and flushing them again
     * writes duplicate segments with identical values). */
    segdb_manifest_copy(&db->manifest, &g_manifest_scratch);
    db->has_manifest = 1;
    db->seq_base = flush_seq;
    db->next_seg_id += nhist + (have_cur ? 1u : 0u);
    return segdb_flush_clear_hot(db);
}

/* ---- streaming compact (Track A) ----
 * Multiway merge over segment iterators: a heap ordered by
 * (key asc, commit_seq desc, segment-prio desc) yields the winner
 * for each key first, so duplicates are skipped without ever
 * holding all candidates in an array. Output streams through
 * SegWriter in 4096-entry chunks. A pass merges at most
 * SEGDB_STREAM_SOURCES sources; larger source sets go through
 * intermediate passes (batch outputs are themselves valid
 * segments, so publishing them is always correct even if a
 * later full merge is still pending — see TRACKA_NOTES.md). */
#define SEGDB_STREAM_SOURCES 128u
#define SEGDB_STREAM_CHUNK 4096u

typedef struct {
    SegReader r;
    SegEntry cur;
    uint8_t kb[1024];
    uint8_t vb[1024];
    int has;
    int open;
    uint32_t prio;
} StreamSrc;

static StreamSrc g_stream_srcs[SEGDB_STREAM_SOURCES];
static SegWriter g_stream_writer;

static int stream_cmp(StreamSrc* a, StreamSrc* b) {
    int kc = seg_key_cmp(a->cur.key, a->cur.key_len, b->cur.key, b->cur.key_len);
    if (kc != 0) return kc;
    if (a->cur.commit_seq != b->cur.commit_seq)
        return a->cur.commit_seq > b->cur.commit_seq ? -1 : 1;
    if (a->prio != b->prio) return a->prio > b->prio ? -1 : 1;
    return 0;
}

static void stream_heap_push(uint32_t* heap, uint32_t* n, uint32_t idx) {
    uint32_t c = (*n)++;
    heap[c] = idx;
    while (c > 0) {
        uint32_t p = (c - 1) / 2;
        if (stream_cmp(&g_stream_srcs[heap[c]], &g_stream_srcs[heap[p]]) >= 0) break;
        {
            uint32_t t = heap[c]; heap[c] = heap[p]; heap[p] = t;
        }
        c = p;
    }
}

static uint32_t stream_heap_pop(uint32_t* heap, uint32_t* n) {
    uint32_t top = heap[0];
    uint32_t c = 0;
    (*n)--;
    if (*n > 0) {
        heap[0] = heap[*n];
        for (;;) {
            uint32_t l = c * 2 + 1, r = l + 1, best = c;
            if (l < *n && stream_cmp(&g_stream_srcs[heap[l]], &g_stream_srcs[heap[best]]) < 0) best = l;
            if (r < *n && stream_cmp(&g_stream_srcs[heap[r]], &g_stream_srcs[heap[best]]) < 0) best = r;
            if (best == c) break;
            {
                uint32_t t = heap[c]; heap[c] = heap[best]; heap[best] = t;
            }
            c = best;
        }
    }
    return top;
}

static void stream_close_all(uint32_t nsrc) {
    uint32_t i;
    for (i = 0; i < nsrc; i++) {
        if (g_stream_srcs[i].open) { seg_close(&g_stream_srcs[i].r); g_stream_srcs[i].open = 0; }
    }
}

/* Merge sources[0..nsrc) into output segments named via out_prefix
 * + counter (caller supplies the naming scheme through out_names:
 * this helper fills out_objs/out_metas for up to max_out outputs).
 * Returns 0 on success, -1 on error (outputs written so far are
 * orphans). Each output is verified + synced before returning. */
typedef struct {
    char obj[MANIFEST_MAX_OBJ_LEN + 1];
    SegMeta meta;
} StreamOut;

static int segdb_merge_pass(SegDB* db, const char (*src_objs)[MANIFEST_MAX_OBJ_LEN + 1],
                            uint32_t nsrc, uint64_t drop_barrier,
                            StreamOut* outs, uint32_t max_out, uint32_t* out_count,
                            const char* out_prefix, uint64_t* out_id) {
    uint32_t heap[SEGDB_STREAM_SOURCES];
    uint32_t heap_n = 0;
    uint32_t i, n_out = 0;
    uint32_t out_entries = 0;
    int writer_open = 0;
    uint8_t last_key[1024];
    uint32_t last_len = 0;
    int have_last = 0;
    if (nsrc == 0 || nsrc > SEGDB_STREAM_SOURCES) return -1;
    for (i = 0; i < nsrc; i++) {
        StreamSrc* s = &g_stream_srcs[i];
        int rc;
        s->open = 0;
        s->has = 0;
        s->prio = i;
        if (seg_open(db->store, src_objs[i], &s->r) != 0) { stream_close_all(i); return -1; }
        s->open = 1;
        rc = seg_iter_next(&s->r, &s->cur, s->kb, (uint32_t)sizeof(s->kb), s->vb, (uint32_t)sizeof(s->vb));
        if (rc == 0) {
            s->has = 1;
            stream_heap_push(heap, &heap_n, i);
        } else if (rc != 1) {
            stream_close_all(nsrc);
            return -1;
        }
    }
    while (heap_n > 0) {
        uint32_t wi = stream_heap_pop(heap, &heap_n);
        StreamSrc* s = &g_stream_srcs[wi];
        SegEntry cur = s->cur;
        int dup = have_last && seg_key_equal(cur.key, cur.key_len, last_key, last_len);
        int drop = 0;
        if (dup) {
            /* Still must advance the source before the next pop. */
            int rc = seg_iter_next(&s->r, &s->cur, s->kb, (uint32_t)sizeof(s->kb), s->vb, (uint32_t)sizeof(s->vb));
            if (rc == 0) {
                s->has = 1;
                stream_heap_push(heap, &heap_n, wi);
            } else if (rc == 1) {
                s->has = 0;
            } else {
                stream_close_all(nsrc);
                if (writer_open) seg_write_abort(&g_stream_writer);
                return -1;
            }
            continue;
        }
        seg_memcpy(last_key, cur.key, cur.key_len);
        last_len = cur.key_len;
        have_last = 1;
        if (cur.is_tombstone && cur.commit_seq <= drop_barrier) drop = 1;
        if (drop) {
            int rc = seg_iter_next(&s->r, &s->cur, s->kb, (uint32_t)sizeof(s->kb), s->vb, (uint32_t)sizeof(s->vb));
            if (rc == 0) {
                s->has = 1;
                stream_heap_push(heap, &heap_n, wi);
            } else if (rc == 1) {
                s->has = 0;
            } else {
                stream_close_all(nsrc);
                if (writer_open) seg_write_abort(&g_stream_writer);
                return -1;
            }
            continue;
        }
        if (!writer_open) {
            char name[SEGDB_NAME_BUF];
            if (n_out >= max_out) { stream_close_all(nsrc); return -1; }
            {
                char num[21];
                uint32_t pos = 0;
                if (seg_u64_dec(*out_id, num, sizeof(num)) < 0) { stream_close_all(nsrc); return -1; }
                if (seg_append_str(name, sizeof(name), &pos, out_prefix) != 0) { stream_close_all(nsrc); return -1; }
                if (seg_append_str(name, sizeof(name), &pos, num) != 0) { stream_close_all(nsrc); return -1; }
            }
            if (seg_write_open(db->store, name, 0, 0, 1, &g_stream_writer) != 0) {
                stream_close_all(nsrc);
                return -1;
            }
            writer_open = 1;
            out_entries = 0;
            {
                uint32_t p = 0;
                while (name[p] != '\0' && p + 1 < MANIFEST_MAX_OBJ_LEN + 1) {
                    outs[n_out].obj[p] = name[p];
                    p++;
                }
                outs[n_out].obj[p] = '\0';
            }
        }
        if (seg_write_add(&g_stream_writer, &cur) != 0) {
            seg_write_abort(&g_stream_writer);
            stream_close_all(nsrc);
            return -1;
        }
        out_entries++;
        /* Emit first (writer copies key/value), then advance. */
        {
            int rc = seg_iter_next(&s->r, &s->cur, s->kb, (uint32_t)sizeof(s->kb), s->vb, (uint32_t)sizeof(s->vb));
            if (rc == 0) {
                s->has = 1;
                stream_heap_push(heap, &heap_n, wi);
            } else if (rc == 1) {
                s->has = 0;
            } else {
                seg_write_abort(&g_stream_writer);
                stream_close_all(nsrc);
                return -1;
            }
        }
        if (out_entries >= SEGDB_STREAM_CHUNK) {
            uint64_t out_bytes = 0, cks = 0;
            SegMeta* meta;
            if (seg_write_finish(&g_stream_writer, &out_bytes) != 0) {
                stream_close_all(nsrc);
                return -1;
            }
            writer_open = 0;
            if (segdb_verify_segment(db->store, outs[n_out].obj, g_stream_writer.count,
                                     g_stream_writer.first_key, g_stream_writer.first_len,
                                     g_stream_writer.last_key, g_stream_writer.last_len) != 0) {
                stream_close_all(nsrc);
                return -1;
            }
            if (store_sync_obj(db->store, outs[n_out].obj) != 0) {
                stream_close_all(nsrc);
                return -1;
            }
            if (seg_file_checksum(db->store, outs[n_out].obj, &cks) != 0) {
                stream_close_all(nsrc);
                return -1;
            }
            meta = &outs[n_out].meta;
            {
                uint32_t p = 0;
                while (outs[n_out].obj[p] != '\0' && p + 1 < MANIFEST_MAX_OBJ_LEN + 1) {
                    meta->obj[p] = outs[n_out].obj[p];
                    p++;
                }
                meta->obj[p] = '\0';
            }
            meta->first_key_len = g_stream_writer.first_len;
            seg_memcpy(meta->first_key, g_stream_writer.first_key, g_stream_writer.first_len);
            meta->last_key_len = g_stream_writer.last_len;
            seg_memcpy(meta->last_key, g_stream_writer.last_key, g_stream_writer.last_len);
            meta->min_seq = g_stream_writer.min_seq;
            meta->max_seq = g_stream_writer.max_seq;
            meta->entry_count = g_stream_writer.count;
            meta->index_type = 0;
            meta->model_version = 0;
            meta->byte_size = out_bytes;
            meta->checksum = cks;
            n_out++;
            (*out_id)++;
        }
    }
    if (writer_open) {
        uint64_t out_bytes = 0, cks = 0;
        SegMeta* meta;
        if (seg_write_finish(&g_stream_writer, &out_bytes) != 0) {
            stream_close_all(nsrc);
            return -1;
        }
        writer_open = 0;
        if (segdb_verify_segment(db->store, outs[n_out].obj, g_stream_writer.count,
                                 g_stream_writer.first_key, g_stream_writer.first_len,
                                 g_stream_writer.last_key, g_stream_writer.last_len) != 0) {
            stream_close_all(nsrc);
            return -1;
        }
        if (store_sync_obj(db->store, outs[n_out].obj) != 0) {
            stream_close_all(nsrc);
            return -1;
        }
        if (seg_file_checksum(db->store, outs[n_out].obj, &cks) != 0) {
            stream_close_all(nsrc);
            return -1;
        }
        meta = &outs[n_out].meta;
        {
            uint32_t p = 0;
            while (outs[n_out].obj[p] != '\0' && p + 1 < MANIFEST_MAX_OBJ_LEN + 1) {
                meta->obj[p] = outs[n_out].obj[p];
                p++;
            }
            meta->obj[p] = '\0';
        }
        meta->first_key_len = g_stream_writer.first_len;
        seg_memcpy(meta->first_key, g_stream_writer.first_key, g_stream_writer.first_len);
        meta->last_key_len = g_stream_writer.last_len;
        seg_memcpy(meta->last_key, g_stream_writer.last_key, g_stream_writer.last_len);
        meta->min_seq = g_stream_writer.min_seq;
        meta->max_seq = g_stream_writer.max_seq;
        meta->entry_count = g_stream_writer.count;
        meta->index_type = 0;
        meta->model_version = 0;
        meta->byte_size = out_bytes;
        meta->checksum = cks;
        n_out++;
        (*out_id)++;
    }
    stream_close_all(nsrc);
    *out_count = n_out;
    return 0;
}

/* ---- compact (segments only, hot untouched) ---- */
/* Directory-mode compact (Track A item 3): merge ALL groups'
 * segments (sources in publication order) through the same
 * streaming passes, then split outputs into new groups of
 * <= 128 segments. Checkpoint is UNCHANGED (compaction changes
 * no logical state). Commit point: the directory publish (the
 * directory has a fixed name, so its publish is the replacement);
 * CURRENT never moves. New group manifests are published before
 * the directory and are unreferenced until then, so a failure
 * there is a definite -1 with the old directory intact.
 * Track C scale-up: the source list holds the full structural
 * maximum (64 groups x 128 = 8192 sources); batch outputs
 * reuse one 2048-entry output buffer per batch (their metas
 * are NOT retained — only names), and the partial-merge path
 * (intermediates > 128, so no single final merge is possible)
 * re-derives intermediate metas from disk per group at
 * manifest-building time (seg_open + full discovery +
 * whole-file checksum, i.e. the same verification the merge
 * pass itself applies). The final merge still needs its
 * outputs in memory: at most 2048 output segments
 * (~8.4M live entries at 4096 entries per output) — beyond
 * that, compact fails loudly with -1, never silently
 * partial. Per-batch output above 2048 likewise fails -1. */
#define SEGDB_COMPACT_MAX_SOURCES 8192u
#define SEGDB_COMPACT_MAX_OUT 2048u
/* Forward declaration: defined with the cursor-seek helpers
 * below (compact_groups sits above them in this file). */
static int cur_store_read(YowzaStore* st, const char* obj, uint64_t off,
                          uint8_t* out, uint64_t len);

/* Re-derive a segment's manifest meta from disk: open +
 * full discovery (verifies header/footer, every block
 * checksum, the index checksum, the Bloom checksum, and the
 * first/last keys against the actual entries) plus the
 * whole-file checksum. Used for the partial-merge path,
 * where intermediate metas are not retained in memory. */
static int segdb_compact_derive_meta(SegDB* db, const char* obj,
                                     SegMeta* meta) {
    SegReader r;
    uint64_t cks;
    uint32_t i;
    if (!db || !obj || !meta) return -1;
    if (seg_open(db->store, obj, &r) != 0) return -1;
    if (seg_ensure_discovered(&r) != 0) {
        seg_close(&r);
        return -1;
    }
    if (r.first_key_len > MANIFEST_MAX_KEY_LEN ||
        r.last_key_len > MANIFEST_MAX_KEY_LEN) {
        seg_close(&r);
        return -1;
    }
    if (cur_store_read(db->store, obj, SEG_HEADER_SIZE, meta->first_key,
                       r.first_key_len) != 0) {
        seg_close(&r);
        return -1;
    }
    if (cur_store_read(db->store, obj,
                       (uint64_t)SEG_HEADER_SIZE + r.first_key_len,
                       meta->last_key, r.last_key_len) != 0) {
        seg_close(&r);
        return -1;
    }
    if (seg_file_checksum(db->store, obj, &cks) != 0) {
        seg_close(&r);
        return -1;
    }
    for (i = 0; obj[i] != '\0'; i++) {
        if (i >= MANIFEST_MAX_OBJ_LEN) {
            seg_close(&r);
            return -1;
        }
        meta->obj[i] = obj[i];
    }
    meta->obj[i] = '\0';
    meta->first_key_len = r.first_key_len;
    meta->last_key_len = r.last_key_len;
    meta->min_seq = r.min_seq;
    meta->max_seq = r.max_seq;
    meta->entry_count = r.entry_count;
    meta->index_type = r.index_type;
    meta->model_version = r.model_version;
    meta->byte_size = r.total_len;
    meta->checksum = cks;
    seg_close(&r);
    return 0;
}

static int segdb_compact_groups(SegDB* db, uint64_t drop_tombstones_below_seq) {
    static char src_names[SEGDB_COMPACT_MAX_SOURCES][MANIFEST_MAX_OBJ_LEN + 1];
    static char mid_names[SEGDB_COMPACT_MAX_SOURCES][MANIFEST_MAX_OBJ_LEN + 1];
    static StreamOut outs[SEGDB_COMPACT_MAX_OUT];
    ManifestDir dir;
    uint32_t n_src = 0, n_mid = 0, n_new = 0, n_groups = 0;
    uint32_t gi, si, i;
    uint64_t new_id;
    int have_mid = 0;
    if (manifest_dir_read(db->store, MANIFEST_DIR_OBJ, &dir) != 0) return -1;
    /* Gather sources across groups, oldest first. */
    for (gi = 0; gi < db->group_count; gi++) {
        const Manifest* gm;
        if (gi == db->group_count - 1) {
            gm = &db->manifest;
        } else {
            if (manifest_read(db->store, db->group_objs[gi], &g_group_scratch) != 0) return -1;
            gm = &g_group_scratch;
        }
        for (si = 0; si < gm->segment_count; si++) {
            if (n_src >= SEGDB_COMPACT_MAX_SOURCES) return -1;
            segdb_copy_name(src_names[n_src], gm->segments[si].obj);
            n_src++;
        }
    }
    if (n_src == 0) return 0;
    new_id = db->next_seg_id;
    if (n_src > SEGDB_STREAM_SOURCES) {
        uint32_t off = 0, mid_count = 0;
        uint64_t tmp_id = 0;
        while (off < n_src) {
            uint32_t batch = n_src - off;
            uint32_t produced = 0, p;
            if (batch > SEGDB_STREAM_SOURCES) batch = SEGDB_STREAM_SOURCES;
            /* outs[] is reused per batch: only the produced
             * NAMES are retained (into mid_names); metas are
             * re-derived later if the partial path needs them. */
            if (segdb_merge_pass(db, (const char (*)[MANIFEST_MAX_OBJ_LEN + 1])&src_names[off],
                                 batch, drop_tombstones_below_seq,
                                 outs, SEGDB_COMPACT_MAX_OUT, &produced,
                                 "segtmp.", &tmp_id) != 0) return -1;
            if (mid_count + produced > SEGDB_COMPACT_MAX_SOURCES) return -1;
            for (p = 0; p < produced; p++)
                segdb_copy_name(mid_names[mid_count + p], outs[p].obj);
            mid_count += produced;
            off += batch;
        }
        n_mid = mid_count;
        have_mid = 1;
    }
    if (have_mid && n_mid <= SEGDB_STREAM_SOURCES) {
        uint32_t produced = 0;
        if (segdb_merge_pass(db, (const char (*)[MANIFEST_MAX_OBJ_LEN + 1])mid_names,
                             n_mid, drop_tombstones_below_seq,
                             outs, SEGDB_COMPACT_MAX_OUT, &produced, "seg.", &new_id) != 0) return -1;
        n_new = produced;
    } else if (have_mid) {
        n_new = n_mid; /* partial merge: batch outputs are valid segments */
    } else {
        uint32_t produced = 0;
        if (segdb_merge_pass(db, (const char (*)[MANIFEST_MAX_OBJ_LEN + 1])src_names,
                             n_src, drop_tombstones_below_seq,
                             outs, SEGDB_COMPACT_MAX_OUT, &produced, "seg.", &new_id) != 0) return -1;
        n_new = produced;
    }
    /* Split outputs into new groups of <= 128 segments. */
    n_groups = n_new == 0 ? 0 : (n_new - 1) / MANIFEST_MAX_SEGMENTS + 1;
    if (n_groups > MANIFEST_DIR_MAX_GROUPS) return -1;
    {
        char new_names[MANIFEST_DIR_MAX_GROUPS][MANIFEST_MAX_OBJ_LEN + 1];
        Manifest new_last;
        ManifestDir new_dir;
        uint32_t g, s;
        uint64_t last_gen = db->manifest.generation;
        for (g = 0; g < n_groups; g++) {
            Manifest ng;
            char tmp[SEGDB_NAME_BUF], final_name[SEGDB_NAME_BUF];
            int prc;
            uint32_t cnt = n_new - g * MANIFEST_MAX_SEGMENTS;
            if (cnt > MANIFEST_MAX_SEGMENTS) cnt = MANIFEST_MAX_SEGMENTS;
            ng.generation = last_gen + 1 + g;
            ng.checkpoint_seq = db->seq_base;
            ng.schema_version = db->manifest.schema_version;
            ng.segment_count = cnt;
            if (have_mid && n_mid > SEGDB_STREAM_SOURCES) {
                /* Partial merge: outs[] holds only the last
                 * batch's metas, so re-derive each
                 * intermediate's meta from disk (verified). */
                for (s = 0; s < cnt; s++) {
                    if (segdb_compact_derive_meta(
                            db, mid_names[g * MANIFEST_MAX_SEGMENTS + s],
                            &ng.segments[s]) != 0)
                        return -1;
                }
            } else {
                for (s = 0; s < cnt; s++) segdb_meta_copy(&ng.segments[s], &outs[g * MANIFEST_MAX_SEGMENTS + s].meta);
            }
            if (seg_name_manifest_tmp(ng.generation, tmp, sizeof(tmp)) != 0) return -1;
            if (seg_name_manifest(ng.generation, final_name, sizeof(final_name)) != 0) return -1;
            if (manifest_write(db->store, tmp, &ng) != 0) return -1;
            prc = manifest_publish(db->store, tmp, final_name);
            if (prc != 0) return -1; /* unreferenced: -2 downgraded */
            segdb_copy_name(new_names[g], final_name);
            if (g == n_groups - 1) segdb_manifest_copy(&new_last, &ng);
        }
        if (n_groups == 0) {
            /* All entries dropped (e.g. every segment was a
             * droppable tombstone): publish an empty directory
             * list is invalid (group_count must be >= 1), so keep
             * one empty group manifest instead. */
            Manifest ng;
            char tmp[SEGDB_NAME_BUF], final_name[SEGDB_NAME_BUF];
            int prc;
            ng.generation = last_gen + 1;
            ng.checkpoint_seq = db->seq_base;
            ng.schema_version = db->manifest.schema_version;
            ng.segment_count = 0;
            if (seg_name_manifest_tmp(ng.generation, tmp, sizeof(tmp)) != 0) return -1;
            if (seg_name_manifest(ng.generation, final_name, sizeof(final_name)) != 0) return -1;
            if (manifest_write(db->store, tmp, &ng) != 0) return -1;
            prc = manifest_publish(db->store, tmp, final_name);
            if (prc != 0) return -1;
            segdb_copy_name(new_names[0], final_name);
            segdb_manifest_copy(&new_last, &ng);
            n_groups = 1;
        }
        new_dir.generation = dir.generation + 1;
        new_dir.checkpoint_seq = db->seq_base;
        new_dir.schema_version = db->manifest.schema_version;
        new_dir.group_count = n_groups;
        for (g = 0; g < n_groups; g++) segdb_copy_name(new_dir.groups[g], new_names[g]);
        if (manifest_dir_write(db->store, MANIFEST_DIR_TMP_OBJ, &new_dir) != 0) return -1;
        {
            int prc = manifest_publish(db->store, MANIFEST_DIR_TMP_OBJ, MANIFEST_DIR_OBJ);
            if (prc == -2) {
                segdb_manifest_copy(&db->pending_group_manifest, &new_last);
                db->pending_next_seg_id = new_id;
                db->pending_is_compact = 1;
                db->pending_group_add = 1;
                db->pending_group_count = n_groups;
                for (g = 0; g < n_groups; g++) segdb_copy_name(db->pending_group_objs[g], new_names[g]);
                db->pending_old_group_count = dir.group_count;
                for (g = 0; g < dir.group_count; g++) segdb_copy_name(db->pending_old_group_objs[g], dir.groups[g]);
                db->has_pending = 1;
                db->indeterminate = 1;
                return SEGDB_INDETERMINATE;
            }
            if (prc != 0) return -1;
        }
        /* Directory durable: adopt, then remove old segments +
         * old group manifests (remove failure -> -1 new-valid).
         * Track C: old sources are removed straight from
         * src_names (no second 512-capped copy). */
        {
            char old_groups[MANIFEST_DIR_MAX_GROUPS][MANIFEST_MAX_OBJ_LEN + 1];
            uint32_t n_old = n_src, n_old_groups = dir.group_count;
            int remove_failed = 0;
            for (g = 0; g < dir.group_count; g++) segdb_copy_name(old_groups[g], dir.groups[g]);
            segdb_manifest_copy(&db->manifest, &new_last);
            db->group_count = n_groups;
            for (g = 0; g < n_groups; g++) segdb_copy_name(db->group_objs[g], new_names[g]);
            db->next_seg_id = new_id;
            for (i = 0; i < n_old; i++) {
                if (store_remove(db->store, src_names[i]) != 0) remove_failed = 1;
            }
            for (g = 0; g < n_old_groups; g++) {
                if (store_remove(db->store, old_groups[g]) != 0) remove_failed = 1;
            }
            if (have_mid && n_mid <= SEGDB_STREAM_SOURCES) {
                for (i = 0; i < n_mid; i++) {
                    if (store_remove(db->store, mid_names[i]) != 0) remove_failed = 1;
                }
            }
            if (remove_failed) return -1;
        }
    }
    return 0;
}
/* ---- streaming page scan (deep-integration) ----
 * segdb_scan_page walks the merged scan without ever collecting
 * the segment rows: segment sources stream through the compact
 * heap (page_src_advance / page_seg_winner below) and hot rows
 * are the only collected array (bounded by the hot set, not by
 * the result size). A page call re-walks from the start and
 * skips `skip` live rows, so paging is O(rows) per page in time
 * but O(sources + hot) in memory; callers that want a whole
 * result page by page pay that re-walk, and callers that want
 * speed use one large limit. This is stateless by design: no
 * cursor lives in C, so two iterators over one SegDB (a JOIN's
 * two tables) can interleave page calls without clobbering each
 * other — the host serializes the calls, as everywhere else. */

typedef struct {
    ScanCollectCtx inner;
    uint64_t snapshot_seq;
} PageHotCtx;

static int segdb_page_hot_cb(void* vctx, const uint8_t* key, uint32_t key_len,
                             const uint8_t* val, uint32_t val_len,
                             int is_tombstone, uint64_t timestamp,
                             uint64_t version) {
    PageHotCtx* c = (PageHotCtx*)vctx;
    SegCand* cand;
    const uint8_t* kcopy;
    uint64_t eff;
    if (!seg_prefix_match(key, key_len, c->inner.prefix, c->inner.prefix_len)) return 0;
    eff = c->inner.seq_base + version;
    if (eff > c->snapshot_seq) return 0; /* newer hot entry must not
     * shadow the segment winner for its key at this snapshot */
    if (c->inner.count >= SEGDB_SCAN_MAX_CANDS) { c->inner.overflow = 1; return 1; }
    kcopy = scan_data_copy(&c->inner, key, key_len);
    if (!kcopy) { c->inner.overflow = 1; return 1; }
    cand = &g_scan_cands[c->inner.count];
    cand->key = kcopy;
    cand->key_len = key_len;
    cand->val = val; /* arena pointer: stable, no mutation during scan */
    cand->val_len = val_len;
    cand->tomb = is_tombstone;
    cand->seq = eff;
    cand->crdt_ts = timestamp;
    cand->prio = 1;
    c->inner.count++;
    return 0;
}

typedef struct {
    SegEntry win;
    uint32_t win_src;
    int have;
} PageSegWinner;

static char g_page_src_names[SEGDB_STREAM_SOURCES][MANIFEST_MAX_OBJ_LEN + 1];

/* ---- scan instrumentation (Reliability stage item 03) ----
 * Counters for the old skip path vs the new cursor path, so a
 * benchmark can show work reduction rather than just wall time.
 * Single-threaded invariant: plain globals are safe here. */
static uint64_t g_stat_page_visits = 0;
static uint64_t g_stat_cursor_visits = 0;
static uint64_t g_stat_cursor_block_skips = 0;
static uint64_t g_stat_seek_index_entries = 0;
static uint64_t g_stat_cache_hits = 0;
static uint64_t g_stat_cache_misses = 0;
/* Track C paged-prime counters (measurement, not correctness):
 * page probes spent by cur_seek_block's paged path (one per
 * binary-search probe, plus one for the within-page scan page)
 * and index entries examined inside the within-page scan.
 * Within-page entries are ALSO counted in
 * g_stat_seek_index_entries, so that counter stays comparable
 * across the linear and paged seek paths. */
static uint64_t g_stat_prime_page_probes = 0;
static uint64_t g_stat_prime_within_entries = 0;

void segdb_paged_stats_reset(void) {
    g_stat_prime_page_probes = 0;
    g_stat_prime_within_entries = 0;
}

void segdb_paged_stats_get(uint64_t* pages_out, uint64_t* within_out) {
    if (pages_out) *pages_out = g_stat_prime_page_probes;
    if (within_out) *within_out = g_stat_prime_within_entries;
}

void segdb_scan_stats_reset(void) {
    g_stat_page_visits = 0;
    g_stat_cursor_visits = 0;
    g_stat_cursor_block_skips = 0;
    g_stat_seek_index_entries = 0;
    g_stat_cache_hits = 0;
    g_stat_cache_misses = 0;
}

void segdb_scan_stats_get(SegdbScanStats* out) {
    if (!out) return;
    out->page_visits = g_stat_page_visits;
    out->cursor_visits = g_stat_cursor_visits;
    out->cursor_block_skips = g_stat_cursor_block_skips;
    out->seek_index_entries = g_stat_seek_index_entries;
    out->cache_hits = g_stat_cache_hits;
    out->cache_misses = g_stat_cache_misses;
}

/* Advance source idx to its next prefix-matching entry and push it
 * on the heap; at end of source, mark has = 0. Returns 0/-1. */
static int page_src_advance(uint32_t idx, const uint8_t* prefix, uint32_t prefix_len,
                            uint32_t* heap, uint32_t* heap_n) {
    StreamSrc* s = &g_stream_srcs[idx];
    int rc;
    for (;;) {
        rc = seg_iter_next(&s->r, &s->cur, s->kb, (uint32_t)sizeof(s->kb),
                           s->vb, (uint32_t)sizeof(s->vb));
        if (rc == 1) {
            s->has = 0;
            return 0;
        }
        if (rc == 0) g_stat_page_visits++;
        if (rc != 0) return -1;
        s->has = 1;
        if (seg_prefix_match(s->cur.key, s->cur.key_len, prefix, prefix_len)) {
            stream_heap_push(heap, heap_n, idx);
            return 0;
        }
        /* Non-matching entry: keep advancing this source. */
    }
}

/* Extract the next segment winner: among all segment entries for
 * the smallest key on the heap, the first (highest seq, then
 * highest source prio) with commit_seq <= snapshot_seq wins; every
 * other entry for that key is advanced past. The winner source is
 * left out of the heap with its entry current — the caller must
 * page_src_advance it once the winner is consumed, before asking
 * for the next winner. Key groups with NO entry at or below the
 * snapshot (e.g. a key created after the snapshot that sorts
 * before every older key) are skipped internally: w->have = 0
 * means the heap is exhausted, never merely "this group had no
 * winner" — the caller treats have = 0 as end of segments, so
 * returning it for a winner-less group would truncate the scan.
 * The winner key is copied to a local buffer first: advancing a
 * same-key source overwrites that source's key buffer, so
 * comparing against a popped entry's key pointer after an
 * advance would be wrong. */
static int page_seg_winner(uint32_t* heap, uint32_t* heap_n,
                           const uint8_t* prefix, uint32_t prefix_len,
                           uint64_t snapshot_seq, PageSegWinner* w) {
    uint8_t wk[1024];
    w->have = 0;
    for (;;) {
        uint32_t klen = 0;
        uint32_t cur_idx;
        if (*heap_n == 0) return 0; /* exhausted */
        cur_idx = stream_heap_pop(heap, heap_n);
        klen = g_stream_srcs[cur_idx].cur.key_len;
        seg_memcpy(wk, g_stream_srcs[cur_idx].cur.key, klen);
        for (;;) {
            StreamSrc* cs = &g_stream_srcs[cur_idx];
            SegEntry cur = cs->cur;
            if (!w->have && cur.commit_seq <= snapshot_seq) {
                w->win = cur;
                w->win_src = cur_idx;
                w->have = 1;
                /* Winner source stays out of the heap, entry current. */
            } else {
                if (page_src_advance(cur_idx, prefix, prefix_len, heap, heap_n) != 0) return -1;
            }
            if (*heap_n == 0) break;
            {
                StreamSrc* top = &g_stream_srcs[heap[0]];
                if (!seg_key_equal(top->cur.key, top->cur.key_len, wk, klen)) break;
                cur_idx = stream_heap_pop(heap, heap_n);
            }
        }
        if (w->have) return 0; /* winner found */
        /* No winner for this key group (all its versions are newer
         * than the snapshot): move on to the next key group. */
    }
}

int segdb_scan_page(SegDB* db, const uint8_t* prefix, uint32_t prefix_len,
                    uint64_t snapshot_seq, uint64_t skip, uint32_t limit,
                    uint8_t* out_buffer, uint32_t out_size,
                    uint32_t* count_out) {
    PageHotCtx hctx;
    uint32_t i, gi, si;
    uint32_t nsrc = 0;
    uint32_t heap[SEGDB_STREAM_SOURCES];
    uint32_t heap_n = 0;
    uint32_t hot_i = 0;
    uint64_t skipped = 0;
    uint32_t emitted = 0;
    uint32_t out_off = 0;
    PageSegWinner sw;
    int seg_done = 0;
    if (!db || !count_out) return -1;
    if (prefix_len > 0 && !prefix) return -1;
    if (!out_buffer && out_size > 0) return -1;
    *count_out = 0;
    if (limit == 0) return 0;
    hctx.inner.prefix = prefix;
    hctx.inner.prefix_len = prefix_len;
    hctx.inner.seq_base = db->seq_base;
    hctx.inner.count = 0;
    hctx.inner.data_off = 0;
    hctx.inner.overflow = 0;
    hctx.snapshot_seq = snapshot_seq;
    if (art_iterate(&db->hot.index, segdb_page_hot_cb, &hctx) != 0) return -1;
    if (hctx.inner.overflow) return -1;
    /* GATE 1: one history candidate per key at most (the hot list
     * holds one candidate per key; see the helper). */
    if (segdb_history_append_cands(db, &hctx.inner, prefix, prefix_len,
                                   snapshot_seq, 1, hctx.inner.count) != 0) return -1;
    seg_cand_sort(g_scan_cands, hctx.inner.count);
    /* Gather segment object names, oldest first. More than
     * SEGDB_STREAM_SOURCES sources is a clean -1 (see header). */
    if (db->has_dir) {
        uint32_t n_groups = db->group_count;
        for (gi = 0; gi < n_groups; gi++) {
            const Manifest* gm;
            if (gi == n_groups - 1) {
                gm = &db->manifest;
            } else {
                if (manifest_read(db->store, db->group_objs[gi], &g_group_scratch) != 0) return -1;
                gm = &g_group_scratch;
            }
            for (si = 0; si < gm->segment_count; si++) {
                if (nsrc >= SEGDB_STREAM_SOURCES) return -1;
                segdb_copy_name(g_page_src_names[nsrc], gm->segments[si].obj);
                nsrc++;
            }
        }
    } else {
        for (si = 0; si < db->manifest.segment_count; si++) {
            if (nsrc >= SEGDB_STREAM_SOURCES) return -1;
            segdb_copy_name(g_page_src_names[nsrc], db->manifest.segments[si].obj);
            nsrc++;
        }
    }
    for (i = 0; i < nsrc; i++) {
        StreamSrc* s = &g_stream_srcs[i];
        s->open = 0;
        s->has = 0;
        s->prio = i;
        if (seg_open(db->store, g_page_src_names[i], &s->r) != 0) {
            stream_close_all(i);
            return -1;
        }
        s->open = 1;
        if (page_src_advance(i, prefix, prefix_len, heap, &heap_n) != 0) {
            stream_close_all(nsrc);
            return -1;
        }
    }
    sw.have = 0;
    while (emitted < limit) {
        int hot_avail;
        int take_hot = 0, take_seg = 0, is_tomb = 0;
        const uint8_t* ek = NULL;
        uint32_t ek_len = 0;
        const uint8_t* ev = NULL;
        uint32_t ev_len = 0;
        if (!sw.have && !seg_done) {
            if (page_seg_winner(heap, &heap_n, prefix, prefix_len, snapshot_seq, &sw) != 0) {
                stream_close_all(nsrc);
                return -1;
            }
            if (!sw.have) seg_done = 1;
        }
        hot_avail = hot_i < hctx.inner.count;
        if (!hot_avail && seg_done) break;
        if (hot_avail && !seg_done) {
            const SegCand* hc = &g_scan_cands[hot_i];
            int kc = seg_key_cmp(hc->key, hc->key_len, sw.win.key, sw.win.key_len);
            if (kc < 0) {
                take_hot = 1;
                is_tomb = hc->tomb;
                ek = hc->key; ek_len = hc->key_len; ev = hc->val; ev_len = hc->val_len;
            } else if (kc > 0) {
                take_seg = 1;
                is_tomb = sw.win.is_tombstone;
                ek = sw.win.key; ek_len = sw.win.key_len; ev = sw.win.val; ev_len = sw.win.val_len;
            } else {
                take_hot = 1;
                take_seg = 1;
                if (hc->seq >= sw.win.commit_seq) {
                    is_tomb = hc->tomb;
                    ek = hc->key; ek_len = hc->key_len; ev = hc->val; ev_len = hc->val_len;
                } else {
                    is_tomb = sw.win.is_tombstone;
                    ek = sw.win.key; ek_len = sw.win.key_len; ev = sw.win.val; ev_len = sw.win.val_len;
                }
            }
        } else if (hot_avail) {
            const SegCand* hc = &g_scan_cands[hot_i];
            take_hot = 1;
            is_tomb = hc->tomb;
            ek = hc->key; ek_len = hc->key_len; ev = hc->val; ev_len = hc->val_len;
        } else {
            take_seg = 1;
            is_tomb = sw.win.is_tombstone;
            ek = sw.win.key; ek_len = sw.win.key_len; ev = sw.win.val; ev_len = sw.win.val_len;
        }
        /* Emit BEFORE advancing: advancing the segment winner
         * source overwrites its value buffer. */
        if (!is_tomb) {
            if (skipped < skip) {
                skipped++;
            } else {
                uint64_t need = (uint64_t)4 + ek_len + 4 + ev_len;
                if ((uint64_t)out_off + need > out_size) {
                    *count_out = emitted;
                    stream_close_all(nsrc);
                    return -3;
                }
                out_buffer[out_off++] = (uint8_t)(ek_len & 0xFF);
                out_buffer[out_off++] = (uint8_t)((ek_len >> 8) & 0xFF);
                out_buffer[out_off++] = (uint8_t)((ek_len >> 16) & 0xFF);
                out_buffer[out_off++] = (uint8_t)((ek_len >> 24) & 0xFF);
                seg_memcpy(&out_buffer[out_off], ek, ek_len);
                out_off += ek_len;
                out_buffer[out_off++] = (uint8_t)(ev_len & 0xFF);
                out_buffer[out_off++] = (uint8_t)((ev_len >> 8) & 0xFF);
                out_buffer[out_off++] = (uint8_t)((ev_len >> 16) & 0xFF);
                out_buffer[out_off++] = (uint8_t)((ev_len >> 24) & 0xFF);
                seg_memcpy(&out_buffer[out_off], ev, ev_len);
                out_off += ev_len;
                emitted++;
            }
        }
        if (take_hot) hot_i++;
        if (take_seg) {
            if (page_src_advance(sw.win_src, prefix, prefix_len, heap, &heap_n) != 0) {
                stream_close_all(nsrc);
                return -1;
            }
            sw.have = 0;
        }
    }
    stream_close_all(nsrc);
    *count_out = emitted;
    return 0;
}

/* ---- cursor (continuation-key) scan (Reliability stage item 03,
 * Layer 2) ----
 * segdb_scan_page re-walks from the start and skips `skip` live
 * rows on every page, so paging a whole result costs O(rows *
 * pages) entry visits. segdb_scan_cursor instead takes an
 * exclusive continuation key (the last key the caller already
 * received) and SEEKS each source to it:
 *  - hot ART: candidates are collected and sorted as in
 *    scan_page, then binary-searched for the first key strictly
 *    greater than the cursor (O(log hot) positioning; the hot
 *    collection itself is still O(hot) per call, which is fine
 *    because hot is bounded by the flush batch, and in the
 *    benchmark hot is empty after flush);
 *  - segments: each reader is positioned at the sparse-index
 *    block that can contain the cursor (last block whose first
 *    key <= cursor, or the first block when there is no cursor /
 *    the cursor precedes the segment), then entries at or below
 *    the cursor inside that one block are discarded. Whole
 *    blocks before it are never loaded. A page therefore visits
 *    O(page rows + blocks) segment entries, not O(total rows).
 * Snapshot semantics are identical to segdb_scan_page (literal
 * snapshot_seq, tombstone winners dropped, newer versions of a
 * key never shadow an older visible version at the snapshot).
 * The old skip path is UNCHANGED and remains the behaviour of
 * segdb_scan_page; callers that already page with skip keep
 * working. New callers should page with the cursor: resume with
 * the returned next_key while has_more is set.
 *
 * Segment discovery cache: seg_ensure_discovered walks every
 * block of a segment, which would make every cursor page cost
 * O(segment) again even though the seek skips the entries. So a
 * cursor open first tries a cache of (name hash, total_len,
 * entry_count) -> (index_offset, index_len, block_count) and
 * verifies the cached index by recomputing its FNV-1a over the
 * index bytes and comparing it with the footer index checksum
 * that seg_open validated. Segments are immutable, and a wrong
 * cache entry fails that checksum and falls back to full
 * discovery, so the cache can never mis-seek: on any doubt the
 * segment is re-discovered from its own bytes. Blocks that are
 * actually consumed are still checksum-verified by
 * seg_iter_next before any entry is returned, exactly as in
 * the skip path.
 * One reader subtlety: positioning a reader mid-segment leaves
 * its iter_returned counter below entry_count (entries in
 * skipped blocks were never returned by THIS reader), so
 * seg_iter_next signals exhaustion with -1 (index exhausted)
 * instead of 1. cur_iter_next classifies that -1 as genuine
 * exhaustion only when the reader's index is exhausted, it is
 * not mid-block, AND the last loaded block's checksum still
 * verifies; any other -1 is an error, never a silent
 * truncation. */

static int cur_store_read(YowzaStore* st, const char* obj, uint64_t off,
                          uint8_t* out, uint64_t len) {
    if (!st || !st->vt || !st->vt->read_at) return -1;
    if (len == 0) return 0;
    return st->vt->read_at(st->state, obj, off, out, len);
}

static uint64_t cur_fnv_update(uint64_t h, const uint8_t* data, uint32_t len) {
    uint32_t i;
    for (i = 0; i < len; i++) {
        h ^= data[i];
        h *= 1099511628211ULL;
    }
    return h;
}

static uint32_t cur_rd32(const uint8_t* p) {
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) |
           ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}

static uint64_t cur_rd64(const uint8_t* p) {
    uint64_t v = 0;
    int i;
    for (i = 0; i < 8; i++) v |= (uint64_t)p[i] << (i * 8);
    return v;
}

#define CUR_SEEK_CACHE_SIZE 4096u
typedef struct {
    uint64_t hash;
    uint64_t total_len;
    uint32_t entry_count;
    uint64_t index_offset;
    uint64_t index_len;
    uint32_t block_count;
    /* Bloom location, for point-lookup adoption: discovery
     * fills these on the reader, so the cache carries them
     * alongside the index location (cursor seeking does not
     * use them, but sharing one verified entry keeps both
     * users consistent). */
    uint64_t bloom_offset;
    uint32_t bloom_bytes;
    int valid;
} CurSeekCacheEnt;
static CurSeekCacheEnt g_cur_seek_cache[CUR_SEEK_CACHE_SIZE];

static uint64_t cur_name_hash(const char* s) {
    uint64_t h = 14695981039346656037ULL;
    uint32_t i = 0;
    while (s[i] != '\0') {
        h ^= (uint64_t)(uint8_t)s[i];
        h *= 1099511628211ULL;
        i++;
    }
    return h;
}

/* Verify the index bytes at [off, off+len) against checksum. */
static int cur_verify_index(YowzaStore* st, const char* obj, uint64_t off,
                            uint64_t len, uint64_t expect) {
    uint64_t fnv = 14695981039346656037ULL;
    uint64_t pos = 0;
    uint8_t chunk[256];
    while (pos < len) {
        uint64_t n = len - pos;
        if (n > sizeof(chunk)) n = sizeof(chunk);
        if (cur_store_read(st, obj, off + pos, chunk, n) != 0) return -1;
        fnv = cur_fnv_update(fnv, chunk, (uint32_t)n);
        pos += n;
    }
    return fnv == expect ? 0 : -1;
}

/* Open + discover a segment for cursor seeking, via the cache
 * when it verifies, else full discovery (which also fills the
 * cache). Returns 0/-1. */
static int cur_seek_open(SegDB* db, const char* obj, SegReader* r) {
    uint64_t h;
    CurSeekCacheEnt* e;
    if (seg_open(db->store, obj, r) != 0) return -1;
    if (r->entry_count == 0) {
        r->discovered = 1;
        r->index_offset = 0;
        r->index_len = 0;
        r->block_count = 0;
        r->iter_index_pos = 0;
        r->iter_index_end = 0;
        r->iter_in_block = 0;
        r->iter_returned = 0;
        return 0;
    }
    h = cur_name_hash(obj);
    e = &g_cur_seek_cache[h & (CUR_SEEK_CACHE_SIZE - 1)];
    if (e->valid && e->hash == h && e->total_len == r->total_len &&
        e->entry_count == r->entry_count &&
        cur_verify_index(db->store, obj, e->index_offset, e->index_len,
                         r->footer_index_checksum) == 0) {
        g_stat_cache_hits++;
        r->discovered = 1;
        r->index_offset = e->index_offset;
        r->index_len = e->index_len;
        r->block_count = e->block_count;
        r->bloom_offset = e->bloom_offset;
        r->bloom_bytes = e->bloom_bytes;
        r->iter_index_pos = e->index_offset;
        r->iter_index_end = e->index_offset + e->index_len;
        r->iter_in_block = 0;
        r->iter_returned = 0;
        return 0;
    }
    g_stat_cache_misses++;
    if (seg_ensure_discovered(r) != 0) return -1;
    e->hash = h;
    e->total_len = r->total_len;
    e->entry_count = r->entry_count;
    e->index_offset = r->index_offset;
    e->index_len = r->index_len;
    e->block_count = r->block_count;
    e->bloom_offset = r->bloom_offset;
    e->bloom_bytes = r->bloom_bytes;
    e->valid = 1;
    return 0;
}

/* Point lookup on one segment object (see the forward
 * declaration above segdb_get). Before this change, every
 * segdb_get opened a fresh reader per segment, so every
 * point lookup re-ran the full discovery walk — every block
 * checksum, streaming every entry — which made repeated
 * lookups on a large segment linear in the segment size
 * rather than logarithmic. This helper instead reuses the
 * verified discovery cache above: a cache hit adopts the
 * discovery fields (index location, block count, Bloom
 * location) after re-verifying the index bytes against the
 * footer index checksum seg_open just read, and builds a
 * binary-searchable index view on the stack, so seg_lookup
 * binary-searches instead of linear-scanning the sparse
 * index. A cache miss runs full discovery (which validates
 * everything, including block checksums and first/last keys)
 * and fills the cache. Safety properties, all preserved:
 * segments are immutable and their names (seg.<monotonic
 * id>) are never reused within a SegDB lifetime, so a
 * cached entry can only describe these exact bytes — and
 * even a hypothetical stale entry fails closed, because
 * the index checksum re-verification and the per-lookup
 * candidate-block checksum verification both still run.
 * The view is a navigation aid only: if building it fails
 * (too many blocks, inconsistent bytes) the lookup degrades
 * to the linear scan, never to an error or a wrong answer.
 * Empty segments short-circuit without any discovery walk:
 * entry_count 0 from the seg_open-validated header already
 * proves absence. */
static int segdb_seg_lookup(SegDB* db, const char* obj,
                            const uint8_t* key, uint32_t key_len,
                            SegEntry* e_out, uint8_t* val_buf,
                            uint32_t val_cap) {
    SegReader r;
    uint64_t view[SEG_INDEX_VIEW_MAX];
    /* Track C paged fallback buffer: 1024 pages x 64 entries
     * covers up to 65536 blocks; a segment with more blocks
     * degrades to the linear scan (fail-safe, never an error
     * or a wrong answer). The buffer lives on this stack
     * frame and only needs to outlive the seg_lookup call
     * below. */
    uint64_t page_view[1024];
    int n, lrc;
    if (seg_open(db->store, obj, &r) != 0) return -1;
    if (r.entry_count == 0) {
        r.discovered = 1;
        r.index_offset = 0;
        r.index_len = 0;
        r.block_count = 0;
        r.bloom_offset = 0;
        r.bloom_bytes = 0;
        r.iter_index_pos = 0;
        r.iter_index_end = 0;
        r.iter_in_block = 0;
        r.iter_returned = 0;
    } else {
        uint64_t h = cur_name_hash(obj);
        CurSeekCacheEnt* ce =
            &g_cur_seek_cache[h & (CUR_SEEK_CACHE_SIZE - 1)];
        if (ce->valid && ce->hash == h &&
            ce->total_len == r.total_len &&
            ce->entry_count == r.entry_count &&
            cur_verify_index(db->store, obj, ce->index_offset,
                             ce->index_len,
                             r.footer_index_checksum) == 0) {
            g_stat_cache_hits++;
            r.discovered = 1;
            r.index_offset = ce->index_offset;
            r.index_len = ce->index_len;
            r.block_count = ce->block_count;
            r.bloom_offset = ce->bloom_offset;
            r.bloom_bytes = ce->bloom_bytes;
            r.iter_index_pos = ce->index_offset;
            r.iter_index_end = ce->index_offset + ce->index_len;
            r.iter_in_block = 0;
            r.iter_returned = 0;
        } else {
            g_stat_cache_misses++;
            if (seg_ensure_discovered(&r) != 0) {
                seg_close(&r);
                return -1;
            }
            ce->hash = h;
            ce->total_len = r.total_len;
            ce->entry_count = r.entry_count;
            ce->index_offset = r.index_offset;
            ce->index_len = r.index_len;
            ce->block_count = r.block_count;
            ce->bloom_offset = r.bloom_offset;
            ce->bloom_bytes = r.bloom_bytes;
            ce->valid = 1;
        }
        n = seg_index_view(&r, view, SEG_INDEX_VIEW_MAX);
        if (n >= 0 && (uint32_t)n == r.block_count &&
            r.block_count > 0) {
            r.idx_offs = view;
            r.idx_offs_count = (uint32_t)n;
        } else if (r.block_count > SEG_INDEX_VIEW_MAX) {
            /* Track C: the >256-block path. The full view
             * cannot hold this segment, so build the paged
             * view (one offset per 64 index entries) and let
             * seg_lookup binary-search pages, then scan
             * within one page. Any build failure (too many
             * pages, inconsistent bytes) leaves both views
             * unset and the lookup degrades to the linear
             * scan — never to an error or a wrong answer.
             * The chosen block's checksum is still verified
             * by seg_lookup before any entry is returned. */
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
    }
    lrc = seg_lookup(&r, key, key_len, e_out, val_buf, val_cap);
    seg_close(&r);
    return lrc;
}

/* Position r at the block that can contain `bound` (last block
 * whose first key <= bound; the first block when bound is NULL
 * or precedes the segment). On success iter_index_pos points
 * at that block's index entry and iter_in_block is 0, so the
 * next seg_iter_next loads exactly that block. Also returns
 * the chosen block's offset/length and the index position just
 * past its entry (for the prime block-parse below). */
static int cur_seek_block(SegDB* db, SegReader* r,
                          const uint8_t* bound, uint32_t bound_len,
                          uint64_t* out_boff, uint64_t* out_blen,
                          uint64_t* out_next_pos) {
    uint64_t pos, end, chosen_off, first_off = 0;
    uint64_t chosen_boff = 0, chosen_blen = 0, chosen_next = 0, first_boff = 0, first_blen = 0, first_next = 0;
    int have_chosen = 0, have_first = 0;
    if (r->entry_count == 0) return 0;
    if (!bound) {
        /* First block: parse its entry directly below. */
        uint8_t hdr[4];
        uint8_t kb[1024];
        uint8_t tail[16];
        uint32_t klen;
        if (cur_store_read(db->store, r->obj, r->index_offset, hdr, 4) != 0) return -1;
        klen = cur_rd32(hdr);
        if (klen == 0 || klen > sizeof(kb)) return -1;
        if (cur_store_read(db->store, r->obj, r->index_offset + 4, kb, klen) != 0) return -1;
        if (cur_store_read(db->store, r->obj, r->index_offset + 4 + klen, tail, 16) != 0) return -1;
        g_stat_seek_index_entries++;
        r->iter_index_pos = r->index_offset;
        r->iter_in_block = 0;
        if (out_boff) *out_boff = cur_rd64(tail);
        if (out_blen) *out_blen = cur_rd64(tail + 8);
        if (out_next_pos) *out_next_pos = r->index_offset + 4 + klen + 16;
        return 0;
    }
    /* Track C paged seek (the >64-block path): binary-search
     * page first keys, then scan within the one candidate
     * page (at most SEG_INDEX_PAGE_SIZE entries). Cost is
     * O(log pages) probes + one page scan instead of
     * O(blocks) linear index entries. Any build/read
     * inconsistency falls through to the linear loop below
     * (which fails with -1 on a real I/O error, exactly as
     * before) — a torn paged view can never cause a wrong
     * seek, because the seek only positions the reader; the
     * block actually read is checksum-verified by the
     * prime/iteration path before any entry is returned. */
    if (r->block_count > SEG_INDEX_PAGE_SIZE) {
        uint64_t page_offs[1024];
        int np = seg_index_view_paged(r, page_offs, 1024,
                                      SEG_INDEX_PAGE_SIZE);
        if (np >= 0 &&
            (uint32_t)np ==
                (r->block_count + SEG_INDEX_PAGE_SIZE - 1) /
                    SEG_INDEX_PAGE_SIZE) {
            uint32_t lo = 0, hi = (uint32_t)np;
            int best_page = -1;
            int paged_bad = 0;
            while (lo < hi && !paged_bad) {
                uint32_t mid = lo + (hi - lo) / 2;
                uint8_t hdr[4];
                uint8_t kb[1024];
                uint32_t klen;
                int cmp;
                if (cur_store_read(db->store, r->obj, page_offs[mid],
                                   hdr, 4) != 0) { paged_bad = 1; break; }
                klen = cur_rd32(hdr);
                if (klen == 0 || klen > sizeof(kb)) { paged_bad = 1; break; }
                if (cur_store_read(db->store, r->obj, page_offs[mid] + 4,
                                   kb, klen) != 0) { paged_bad = 1; break; }
                cmp = seg_key_cmp(kb, klen, bound, bound_len);
                g_stat_prime_page_probes++;
                if (cmp > 0) { hi = mid; continue; }
                best_page = (int)mid;
                lo = mid + 1;
            }
            if (!paged_bad && best_page >= 0) {
                uint64_t ppos = page_offs[best_page];
                uint64_t pend = (uint32_t)(best_page + 1) < (uint32_t)np
                                    ? page_offs[best_page + 1]
                                    : r->index_offset + r->index_len;
                uint64_t p_chosen_off = ppos;
                uint64_t p_chosen_boff = 0, p_chosen_blen = 0, p_chosen_next = 0;
                int p_have = 0;
                g_stat_prime_page_probes++; /* within-page scan page */
                while (ppos < pend) {
                    uint8_t hdr[4];
                    uint8_t kb[1024];
                    uint8_t tail[16];
                    uint32_t klen;
                    int cmp;
                    if (cur_store_read(db->store, r->obj, ppos, hdr, 4) != 0) { paged_bad = 1; break; }
                    klen = cur_rd32(hdr);
                    if (klen == 0 || klen > sizeof(kb)) { paged_bad = 1; break; }
                    if (cur_store_read(db->store, r->obj, ppos + 4, kb, klen) != 0) { paged_bad = 1; break; }
                    if (cur_store_read(db->store, r->obj, ppos + 4 + klen, tail, 16) != 0) { paged_bad = 1; break; }
                    g_stat_seek_index_entries++;
                    g_stat_prime_within_entries++;
                    cmp = seg_key_cmp(kb, klen, bound, bound_len);
                    if (cmp <= 0) {
                        p_chosen_off = ppos;
                        p_chosen_boff = cur_rd64(tail);
                        p_chosen_blen = cur_rd64(tail + 8);
                        p_chosen_next = ppos + 4 + klen + 16;
                        p_have = 1;
                    } else {
                        break;
                    }
                    ppos += 4 + klen + 16;
                }
                if (!paged_bad && p_have) {
                    r->iter_index_pos = p_chosen_off;
                    r->iter_in_block = 0;
                    if (out_boff) *out_boff = p_chosen_boff;
                    if (out_blen) *out_blen = p_chosen_blen;
                    if (out_next_pos) *out_next_pos = p_chosen_next;
                    return 0;
                }
            }
            /* best_page < 0 (bound precedes the segment) or a
             * paged inconsistency: fall through to the linear
             * loop, which handles both cases exactly as before. */
        }
    }
    pos = r->index_offset;
    end = r->index_offset + r->index_len;
    chosen_off = r->index_offset;
    while (pos < end) {
        uint8_t hdr[4];
        uint8_t kb[1024];
        uint8_t tail[16];
        uint32_t klen;
        int cmp;
        if (cur_store_read(db->store, r->obj, pos, hdr, 4) != 0) return -1;
        klen = cur_rd32(hdr);
        if (klen == 0 || klen > sizeof(kb)) return -1;
        if (cur_store_read(db->store, r->obj, pos + 4, kb, klen) != 0) return -1;
        if (cur_store_read(db->store, r->obj, pos + 4 + klen, tail, 16) != 0) return -1;
        g_stat_seek_index_entries++;
        cmp = seg_key_cmp(kb, klen, bound, bound_len);
        if (!have_first) {
            first_off = pos;
            first_boff = cur_rd64(tail);
            first_blen = cur_rd64(tail + 8);
            first_next = pos + 4 + klen + 16;
            have_first = 1;
        }
        if (cmp <= 0) {
            chosen_off = pos;
            chosen_boff = cur_rd64(tail);
            chosen_blen = cur_rd64(tail + 8);
            chosen_next = pos + 4 + klen + 16;
            have_chosen = 1;
        } else {
            break;
        }
        pos += 4 + klen + 16;
    }
    r->iter_index_pos = have_chosen ? chosen_off : first_off;
    r->iter_in_block = 0;
    if (out_boff) *out_boff = have_chosen ? chosen_boff : first_boff;
    if (out_blen) *out_blen = have_chosen ? chosen_blen : first_blen;
    if (out_next_pos) *out_next_pos = have_chosen ? chosen_next : first_next;
    return 0;
}

/* 1 if key is strictly beyond the prefix range (sorted order
 * guarantees no later key can match), else 0. */
static int cur_prefix_beyond(const uint8_t* key, uint32_t key_len,
                             const uint8_t* prefix, uint32_t prefix_len) {
    int c;
    if (prefix_len == 0) return 0;
    if (key_len < prefix_len) {
        c = seg_memcmp(key, prefix, key_len);
        if (c == 0) return 0; /* key is a proper prefix of prefix: before range */
        return c > 0;
    }
    c = seg_memcmp(key, prefix, prefix_len);
    return c > 0;
}

typedef struct {
    SegReader r;
    SegEntry cur;
    uint8_t kb[1024];
    uint8_t vb[1024];
    int has;
    int open;
} CurSrc;

/* Verify the last loaded block's checksum (exhaustion check). */
static int cur_verify_last_block(SegDB* db, SegReader* r) {
    uint64_t fnv = 14695981039346656037ULL;
    uint64_t pos = 0, stored;
    uint8_t chunk[256];
    uint8_t b8[8];
    if (r->iter_block_len == 0) return 0;
    while (pos < r->iter_block_len) {
        uint64_t n = r->iter_block_len - pos;
        if (n > sizeof(chunk)) n = sizeof(chunk);
        if (cur_store_read(db->store, r->obj, r->iter_block_offset + pos,
                           chunk, n) != 0) return -1;
        fnv = cur_fnv_update(fnv, chunk, (uint32_t)n);
        pos += n;
    }
    if (cur_store_read(db->store, r->obj, r->iter_block_offset + r->iter_block_len,
                       b8, 8) != 0) return -1;
    stored = cur_rd64(b8);
    return stored == fnv ? 0 : -1;
}

/* One seg_iter_next with the mid-segment exhaustion rule (see
 * the block comment above). Returns 0 (entry in s->cur),
 * 1 (source exhausted), -1 (error). */
static int cur_iter_next(SegDB* db, CurSrc* s) {
    int rc = seg_iter_next(&s->r, &s->cur, s->kb, (uint32_t)sizeof(s->kb),
                           s->vb, (uint32_t)sizeof(s->vb));
    if (rc == 0) {
        g_stat_cursor_visits++;
        return 0;
    }
    if (rc == 1) return 1;
    /* rc == -1: genuine exhaustion only if the index is
     * exhausted, we are not mid-block, and the last loaded
     * block still checksums. Anything else is an error. */
    if (!s->r.iter_in_block && s->r.iter_index_pos >= s->r.iter_index_end) {
        if (cur_verify_last_block(db, &s->r) == 0) return 1;
    }
    return -1;
}

/* Advance source to its next usable entry: key > cursor (when
 * has_cursor) and prefix-matching, or mark done. The first call
 * after positioning also discards the seek block's entries at
 * or below the cursor. Returns 0/-1. */
static int cur_src_advance(SegDB* db, CurSrc* s,
                           const uint8_t* prefix, uint32_t prefix_len,
                           const uint8_t* cursor, uint32_t cursor_len, int has_cursor) {
    for (;;) {
        int rc = cur_iter_next(db, s);
        if (rc == 1) {
            s->has = 0;
            return 0;
        }
        if (rc != 0) return -1;
        s->has = 1;
        if (has_cursor && seg_key_cmp(s->cur.key, s->cur.key_len,
                                      cursor, cursor_len) <= 0) continue;
        if (cur_prefix_beyond(s->cur.key, s->cur.key_len, prefix, prefix_len)) {
            s->has = 0;
            return 0;
        }
        if (!seg_prefix_match(s->cur.key, s->cur.key_len, prefix, prefix_len)) continue;
        return 0;
    }
}

static uint8_t g_cur_block_buf[8192];

/* Parse one segment entry in a block buffer: sets the key pointer
 * and length outputs and the next offset (just past the entry).
 * Returns 0, or -1 when malformed. */
static int cur_parse_entry(const uint8_t* buf, uint64_t len, uint64_t* pos,
                           const uint8_t** key, uint32_t* klen, uint64_t* next) {
    uint32_t kl, vl;
    uint64_t p = *pos;
    if (p + 4 > len) return -1;
    kl = cur_rd32(buf + p);
    if (kl == 0 || p + 4 + kl + 4 > len) return -1;
    vl = cur_rd32(buf + p + 4 + kl);
    if (p + 4 + kl + 4 + vl + 17 > len) return -1;
    *key = buf + p + 4;
    *klen = kl;
    *next = p + 4 + kl + 4 + vl + 17;
    return 0;
}

/* Prime a seek-positioned source to its first usable entry.
 * Fast path: read the whole seek block once (one store read,
 * checksum-verified here) and parse entries in memory to the
 * first usable one, positioning the reader at it — so the
 * per-page discard inside the seek block costs one block read
 * instead of one seg_iter_next (several small store reads) per
 * discarded entry. Skipped entries count as cursor_block_skips
 * (examined, but by an in-memory parse — reported separately
 * from iterator returns). Falls back to
 * the per-entry advance when the block is larger than the
 * buffer or cannot be read (the fallback then surfaces any
 * real I/O error); a checksum mismatch is an error, never a
 * fallback to unverified data. */
static int cur_src_prime(SegDB* db, CurSrc* s,
                         const uint8_t* prefix, uint32_t prefix_len,
                         const uint8_t* cursor, uint32_t cursor_len, int has_cursor,
                         uint64_t boff, uint64_t blen, uint64_t next_pos) {
    if (blen > 0 && blen <= sizeof(g_cur_block_buf)) {
        uint64_t fnv;
        uint8_t b8[8];
        uint64_t pos = 0;
        if (cur_store_read(db->store, s->r.obj, boff, g_cur_block_buf, blen) != 0) {
            /* Read error: fall through to the per-entry path,
             * which reports it through seg_iter_next. */
            return cur_src_advance(db, s, prefix, prefix_len,
                                   cursor, cursor_len, has_cursor);
        }
        fnv = cur_fnv_update(14695981039346656037ULL, g_cur_block_buf, (uint32_t)blen);
        if (cur_store_read(db->store, s->r.obj, boff + blen, b8, 8) != 0) return -1;
        if (cur_rd64(b8) != fnv) return -1; /* corrupt seek block */
        for (;;) {
            const uint8_t* k = NULL;
            uint32_t kl = 0;
            uint64_t nx = 0;
            int rc;
            if (pos >= blen) {
                /* Whole block skipped: move past it and let
                 * the per-entry path load the next block. */
                s->r.iter_index_pos = next_pos;
                s->r.iter_in_block = 0;
                return cur_src_advance(db, s, prefix, prefix_len,
                                       cursor, cursor_len, has_cursor);
            }
            if (cur_parse_entry(g_cur_block_buf, blen, &pos, &k, &kl, &nx) != 0) return -1;
            if (cur_prefix_beyond(k, kl, prefix, prefix_len)) {
                s->has = 0;
                return 0;
            }
            if ((!has_cursor || seg_key_cmp(k, kl, cursor, cursor_len) > 0) &&
                seg_prefix_match(k, kl, prefix, prefix_len)) {
                /* Position the reader at this entry (block
                 * already checksum-verified above) and fetch
                 * it through the normal iterator. */
                s->r.iter_block_offset = boff;
                s->r.iter_block_len = blen;
                s->r.iter_block_end = boff + blen;
                s->r.iter_entry_pos = boff + pos;
                s->r.iter_in_block = 1;
                s->r.iter_index_pos = next_pos;
                rc = cur_iter_next(db, s);
                if (rc == 1) {
                    s->has = 0;
                    return 0;
                }
                if (rc != 0) return -1;
                s->has = 1;
                return 0;
            }
            g_stat_cursor_block_skips++; /* entry examined by the
             * in-memory block parse (cheap: one block read
             * served the whole parse; counted separately from
             * iterator returns so the two are never conflated) */
            pos = nx;
        }
    }
    return cur_src_advance(db, s, prefix, prefix_len,
                           cursor, cursor_len, has_cursor);
}

static void cur_close_all(CurSrc* srcs, uint32_t nsrc) {
    uint32_t i;
    for (i = 0; i < nsrc; i++) {
        if (srcs[i].open) {
            seg_close(&srcs[i].r);
            srcs[i].open = 0;
        }
    }
}

static CurSrc g_cur_srcs[SEGDB_STREAM_SOURCES];

typedef struct {
    SegDB* db;
    const uint8_t* prefix;
    uint32_t prefix_len;
    const uint8_t* cursor;
    uint32_t cursor_len;
    int has_cursor;
    uint64_t snapshot_seq;
    uint32_t hot_i;
    uint32_t hot_n;
    uint32_t nsrc;
    uint8_t rowk[1024];
    uint8_t rowv[1024];
    const uint8_t* key;
    uint32_t key_len;
    const uint8_t* val;
    uint32_t val_len;
} CurMerge;

/* Next live merged row at the snapshot, or 0 at end, -1 on
 * error. On 1, key/val point at m->rowk/rowv (copied before
 * sources advance, so advancing cannot clobber the row). */
static int cur_merge_next(CurMerge* m) {
    for (;;) {
        uint8_t wk[1024];
        uint32_t wk_len = 0;
        int have_min = 0;
        uint32_t i;
        int have_seg_win = 0;
        uint64_t seg_win_seq = 0;
        uint32_t seg_win_src = 0;
        int seg_win_tomb = 0;
        const SegCand* hot_head = NULL;
        int hot_in_group = 0;
        int win_tomb = 0;
        const uint8_t* win_val = NULL;
        uint32_t win_vlen = 0;
        /* Smallest key among heads. */
        if (m->hot_i < m->hot_n) {
            wk_len = g_scan_cands[m->hot_i].key_len;
            seg_memcpy(wk, g_scan_cands[m->hot_i].key, wk_len);
            have_min = 1;
        }
        for (i = 0; i < m->nsrc; i++) {
            CurSrc* s = &g_cur_srcs[i];
            int c;
            if (!s->has) continue;
            if (!have_min) {
                wk_len = s->cur.key_len;
                seg_memcpy(wk, s->cur.key, wk_len);
                have_min = 1;
                continue;
            }
            c = seg_key_cmp(s->cur.key, s->cur.key_len, wk, wk_len);
            if (c < 0) {
                wk_len = s->cur.key_len;
                seg_memcpy(wk, s->cur.key, wk_len);
            }
        }
        if (!have_min) return 0; /* all sources exhausted */
        /* Resolve this key group. Segment winner: highest
         * commit_seq at or below the snapshot, ties to the
         * later (newer) source. Versions newer than the
         * snapshot are discarded below with the group and
         * never shadow an older visible version. */
        for (i = 0; i < m->nsrc; i++) {
            CurSrc* s = &g_cur_srcs[i];
            if (!s->has) continue;
            if (!seg_key_equal(s->cur.key, s->cur.key_len, wk, wk_len)) continue;
            if (s->cur.commit_seq > m->snapshot_seq) continue;
            if (!have_seg_win || s->cur.commit_seq > seg_win_seq ||
                (s->cur.commit_seq == seg_win_seq && i > seg_win_src)) {
                have_seg_win = 1;
                seg_win_seq = s->cur.commit_seq;
                seg_win_src = i;
                seg_win_tomb = s->cur.is_tombstone;
            }
        }
        if (m->hot_i < m->hot_n) {
            const SegCand* hc = &g_scan_cands[m->hot_i];
            if (seg_key_equal(hc->key, hc->key_len, wk, wk_len)) {
                hot_head = hc;
                hot_in_group = 1;
            }
        }
        if (hot_in_group && (!have_seg_win || hot_head->seq >= seg_win_seq)) {
            win_tomb = hot_head->tomb;
            win_val = hot_head->val;
            win_vlen = hot_head->val_len;
        } else if (have_seg_win) {
            CurSrc* s = &g_cur_srcs[seg_win_src];
            win_tomb = seg_win_tomb;
            win_val = s->cur.val;
            win_vlen = s->cur.val_len;
        }
        /* Copy the winner BEFORE advancing any group member:
         * advancing the winner's own source overwrites its
         * key/value buffers. */
        if ((hot_in_group || have_seg_win) && !win_tomb) {
            seg_memcpy(m->rowk, wk, wk_len);
            if (win_vlen > 0) seg_memcpy(m->rowv, win_val, win_vlen);
            m->key = m->rowk;
            m->key_len = wk_len;
            m->val = m->rowv;
            m->val_len = win_vlen;
        }
        /* Advance every group member past this key. */
        if (hot_in_group) m->hot_i++;
        for (i = 0; i < m->nsrc; i++) {
            CurSrc* s = &g_cur_srcs[i];
            if (!s->has) continue;
            if (!seg_key_equal(s->cur.key, s->cur.key_len, wk, wk_len)) continue;
            if (cur_src_advance(m->db, s, m->prefix, m->prefix_len,
                                m->cursor, m->cursor_len, m->has_cursor) != 0) return -1;
        }
        if (!hot_in_group && !have_seg_win) continue; /* only newer versions: next group */
        if (win_tomb) continue; /* tombstone winner: row invisible, next group */
        return 1;
    }
}

static uint32_t cur_hot_first_gt(uint32_t n, const uint8_t* cursor, uint32_t cursor_len) {
    uint32_t lo = 0, hi = n;
    while (lo < hi) {
        uint32_t mid = lo + (hi - lo) / 2;
        if (seg_key_cmp(g_scan_cands[mid].key, g_scan_cands[mid].key_len,
                        cursor, cursor_len) <= 0) lo = mid + 1;
        else hi = mid;
    }
    return lo;
}

int segdb_scan_cursor(SegDB* db, const uint8_t* prefix, uint32_t prefix_len,
                      uint64_t snapshot_seq,
                      const uint8_t* cursor_key, uint32_t cursor_len, int has_cursor,
                      uint32_t limit,
                      uint8_t* out_buffer, uint32_t out_size,
                      uint32_t* count_out,
                      uint8_t* next_key, uint32_t next_key_cap, uint32_t* next_len_out,
                      uint32_t* has_more_out) {
    PageHotCtx hctx;
    CurMerge m;
    uint32_t gi, si, i;
    uint32_t nsrc = 0;
    uint32_t emitted = 0;
    uint32_t out_off = 0;
    uint8_t last_key[1024];
    uint32_t last_len = 0;
    int have_last = 0;
    if (!db || !count_out || !next_len_out || !has_more_out) return -1;
    if (prefix_len > 0 && !prefix) return -1;
    if (has_cursor && cursor_len > 0 && !cursor_key) return -1;
    if (!out_buffer && out_size > 0) return -1;
    *count_out = 0;
    *next_len_out = 0;
    *has_more_out = 0;
    /* Hot candidates (snapshot-filtered, prefix-filtered). */
    hctx.inner.prefix = prefix;
    hctx.inner.prefix_len = prefix_len;
    hctx.inner.seq_base = db->seq_base;
    hctx.inner.count = 0;
    hctx.inner.data_off = 0;
    hctx.inner.overflow = 0;
    hctx.snapshot_seq = snapshot_seq;
    if (art_iterate(&db->hot.index, segdb_page_hot_cb, &hctx) != 0) return -1;
    if (hctx.inner.overflow) return -1;
    /* GATE 1: one history candidate per key at most (same rule
     * as segdb_scan_page; the cursor merge holds one hot
     * candidate per key). */
    if (segdb_history_append_cands(db, &hctx.inner, prefix, prefix_len,
                                   snapshot_seq, 1, hctx.inner.count) != 0) return -1;
    seg_cand_sort(g_scan_cands, hctx.inner.count);
    /* Gather segment object names, oldest first (same order and
     * cap as segdb_scan_page). */
    if (db->has_dir) {
        uint32_t n_groups = db->group_count;
        for (gi = 0; gi < n_groups; gi++) {
            const Manifest* gm;
            if (gi == n_groups - 1) {
                gm = &db->manifest;
            } else {
                if (manifest_read(db->store, db->group_objs[gi], &g_group_scratch) != 0) return -1;
                gm = &g_group_scratch;
            }
            for (si = 0; si < gm->segment_count; si++) {
                if (nsrc >= SEGDB_STREAM_SOURCES) return -1;
                segdb_copy_name(g_page_src_names[nsrc], gm->segments[si].obj);
                nsrc++;
            }
        }
    } else {
        for (si = 0; si < db->manifest.segment_count; si++) {
            if (nsrc >= SEGDB_STREAM_SOURCES) return -1;
            segdb_copy_name(g_page_src_names[nsrc], db->manifest.segments[si].obj);
            nsrc++;
        }
    }
    /* Open + seek every source. The seek bound is the cursor
     * when one is given, else the prefix (its first matching
     * block), else the first block. */
    for (i = 0; i < nsrc; i++) {
        CurSrc* s = &g_cur_srcs[i];
        const uint8_t* bound = NULL;
        uint32_t bound_len = 0;
        uint64_t boff = 0, blen = 0, next_pos = 0;
        s->open = 0;
        s->has = 0;
        if (cur_seek_open(db, g_page_src_names[i], &s->r) != 0) {
            cur_close_all(g_cur_srcs, i);
            return -1;
        }
        s->open = 1;
        if (s->r.entry_count == 0) continue;
        if (has_cursor) {
            bound = cursor_key;
            bound_len = cursor_len;
        } else if (prefix_len > 0) {
            bound = prefix;
            bound_len = prefix_len;
        }
        if (cur_seek_block(db, &s->r, bound, bound_len, &boff, &blen, &next_pos) != 0) {
            cur_close_all(g_cur_srcs, nsrc);
            return -1;
        }
        if (cur_src_prime(db, s, prefix, prefix_len,
                          cursor_key, cursor_len, has_cursor,
                          boff, blen, next_pos) != 0) {
            cur_close_all(g_cur_srcs, nsrc);
            return -1;
        }
    }
    m.db = db;
    m.prefix = prefix;
    m.prefix_len = prefix_len;
    m.cursor = cursor_key;
    m.cursor_len = cursor_len;
    m.has_cursor = has_cursor;
    m.snapshot_seq = snapshot_seq;
    m.hot_n = hctx.inner.count;
    m.hot_i = has_cursor ? cur_hot_first_gt(hctx.inner.count, cursor_key, cursor_len) : 0;
    m.nsrc = nsrc;
    m.key = NULL;
    m.key_len = 0;
    m.val = NULL;
    m.val_len = 0;
    if (limit == 0) {
        /* Probe only: report whether any row exists past the cursor. */
        int prc = cur_merge_next(&m);
        cur_close_all(g_cur_srcs, nsrc);
        if (prc < 0) return -1;
        *has_more_out = prc == 1 ? 1 : 0;
        *count_out = 0;
        return 0;
    }
    while (emitted < limit) {
        int prc = cur_merge_next(&m);
        uint64_t need;
        if (prc < 0) {
            *count_out = emitted;
            cur_close_all(g_cur_srcs, nsrc);
            return -1;
        }
        if (prc == 0) {
            cur_close_all(g_cur_srcs, nsrc);
            *count_out = emitted;
            *has_more_out = 0;
            goto done;
        }
        need = (uint64_t)4 + m.key_len + 4 + m.val_len;
        if ((uint64_t)out_off + need > out_size) {
            /* -3: rows already written stand; resume with the
             * last written key, or the input cursor when
             * nothing was written (caller retries with a
             * larger buffer). */
            *count_out = emitted;
            *has_more_out = 1;
            if (have_last) {
                if (last_len <= next_key_cap && next_key) {
                    seg_memcpy(next_key, last_key, last_len);
                    *next_len_out = last_len;
                }
            } else if (has_cursor && cursor_len <= next_key_cap && next_key) {
                seg_memcpy(next_key, cursor_key, cursor_len);
                *next_len_out = cursor_len;
            }
            cur_close_all(g_cur_srcs, nsrc);
            return -3;
        }
        out_buffer[out_off++] = (uint8_t)(m.key_len & 0xFF);
        out_buffer[out_off++] = (uint8_t)((m.key_len >> 8) & 0xFF);
        out_buffer[out_off++] = (uint8_t)((m.key_len >> 16) & 0xFF);
        out_buffer[out_off++] = (uint8_t)((m.key_len >> 24) & 0xFF);
        seg_memcpy(&out_buffer[out_off], m.key, m.key_len);
        out_off += m.key_len;
        out_buffer[out_off++] = (uint8_t)(m.val_len & 0xFF);
        out_buffer[out_off++] = (uint8_t)((m.val_len >> 8) & 0xFF);
        out_buffer[out_off++] = (uint8_t)((m.val_len >> 16) & 0xFF);
        out_buffer[out_off++] = (uint8_t)((m.val_len >> 24) & 0xFF);
        seg_memcpy(&out_buffer[out_off], m.val, m.val_len);
        out_off += m.val_len;
        emitted++;
        last_len = m.key_len;
        seg_memcpy(last_key, m.key, m.key_len);
        have_last = 1;
    }
    /* Probe one row past the page to set has_more honestly. */
    {
        int prc = cur_merge_next(&m);
        if (prc < 0) {
            *count_out = emitted;
            cur_close_all(g_cur_srcs, nsrc);
            return -1;
        }
        *has_more_out = prc == 1 ? 1 : 0;
    }
    cur_close_all(g_cur_srcs, nsrc);
    *count_out = emitted;
done:
    if (have_last) {
        if (last_len <= next_key_cap && next_key) {
            seg_memcpy(next_key, last_key, last_len);
            *next_len_out = last_len;
        }
    } else if (has_cursor && cursor_len <= next_key_cap && next_key) {
        seg_memcpy(next_key, cursor_key, cursor_len);
        *next_len_out = cursor_len;
    }
    return 0;
}

int segdb_compact(SegDB* db, uint64_t drop_tombstones_below_seq) {
    uint32_t i;
    uint32_t n_new = 0;
    uint64_t new_id;
    if (!db || !db->store) return -1;
    if (segdb_write_guard(db) != 0) return SEGDB_INDETERMINATE;
    /* GATE 1 lease check (covers both the single-manifest path
     * below and the directory path: it runs before the branch).
     * A stale lease pins compaction: the merge drops non-newest
     * versions unconditionally, so no barrier can make it safe
     * for a snapshot below the current sequence. Deferring is a
     * successful no-op — compaction is an optimisation, never a
     * logical operation — and the caller retries after the
     * leases close. A lease exactly at the current sequence does
     * not defer: the merge preserves that sequence's view. */
    {
        int has_lease = 0;
        uint64_t oldest = segdb_lease_oldest(db, &has_lease);
        if (has_lease && oldest < segdb_snapshot_seq(db)) return 0;
    }
    if (db->has_dir) return segdb_compact_groups(db, drop_tombstones_below_seq);
    if (db->manifest.segment_count == 0) return 0;
    new_id = db->next_seg_id;
    /* Streaming passes: single-manifest mode only; directory mode
     * is handled by segdb_compact_groups above. */
    {
        /* Static pass buffers: source names (up to 512) and
         * outputs (up to 512 per pass; a pass over 128 sources of
         * 4096 entries each can emit at most 128 outputs, and
         * small-segment batches shrink, so 512 is generous). */
        static char src_names[512][MANIFEST_MAX_OBJ_LEN + 1];
        static char mid_names[512][MANIFEST_MAX_OBJ_LEN + 1];
        static StreamOut outs[512];
        uint32_t n_src = db->manifest.segment_count;
        uint32_t n_mid = 0;
        uint64_t tmp_id = 0;
        int have_mid = 0;
        if (n_src > 512) return -1; /* see TRACKA_NOTES.md */
        for (i = 0; i < n_src; i++) {
            uint32_t p = 0;
            while (db->manifest.segments[i].obj[p] != '\0' && p + 1 < MANIFEST_MAX_OBJ_LEN + 1) {
                src_names[i][p] = db->manifest.segments[i].obj[p];
                p++;
            }
            src_names[i][p] = '\0';
        }
        if (n_src > SEGDB_STREAM_SOURCES) {
            /* Intermediate pass: batches of SEGDB_STREAM_SOURCES
             * sources -> "segtmp.<n>" segments. */
            uint32_t off = 0;
            uint32_t mid_count = 0;
            while (off < n_src) {
                uint32_t batch = n_src - off;
                uint32_t produced = 0;
                if (batch > SEGDB_STREAM_SOURCES) batch = SEGDB_STREAM_SOURCES;
                if (segdb_merge_pass(db, (const char (*)[MANIFEST_MAX_OBJ_LEN + 1])&src_names[off],
                                     batch, drop_tombstones_below_seq,
                                     &outs[mid_count], 512 - mid_count, &produced,
                                     "segtmp.", &tmp_id) != 0) return -1;
                mid_count += produced;
                off += batch;
            }
            for (i = 0; i < mid_count; i++) {
                uint32_t p = 0;
                while (outs[i].obj[p] != '\0' && p + 1 < MANIFEST_MAX_OBJ_LEN + 1) {
                    mid_names[i][p] = outs[i].obj[p];
                    p++;
                }
                mid_names[i][p] = '\0';
            }
            n_mid = mid_count;
            have_mid = 1;
        }
        if (have_mid && n_mid <= SEGDB_STREAM_SOURCES) {
            /* Final pass over intermediates -> "seg.<id>" outputs. */
            uint32_t produced = 0;
            if (segdb_merge_pass(db, (const char (*)[MANIFEST_MAX_OBJ_LEN + 1])mid_names,
                                 n_mid, drop_tombstones_below_seq,
                                 outs, 512, &produced, "seg.", &new_id) != 0) return -1;
            n_new = produced;
        } else if (have_mid) {
            /* Intermediate pass did not shrink below the heap
             * size (all-large segments): publishing the
             * intermediate segments is still CORRECT (each batch
             * is internally deduped; cross-batch duplicates are
             * resolved by seq at read time, as before compact).
             * They become the new manifest below; a later compact
             * call continues merging. */
            n_new = n_mid;
        } else {
            uint32_t produced = 0;
            if (segdb_merge_pass(db, (const char (*)[MANIFEST_MAX_OBJ_LEN + 1])src_names,
                                 n_src, drop_tombstones_below_seq,
                                 outs, 512, &produced, "seg.", &new_id) != 0) return -1;
            n_new = produced;
        }
        /* Publish exactly like flush, but checkpoint_seq UNCHANGED
         * (compaction changes no logical state and no sequence). */
        segdb_manifest_copy(&g_manifest_scratch, &db->manifest);
        g_manifest_scratch.generation = db->manifest.generation + 1;
        g_manifest_scratch.segment_count = 0;
        for (i = 0; i < n_new; i++) {
            segdb_meta_copy(&g_manifest_scratch.segments[i], &outs[i].meta);
        }
        g_manifest_scratch.segment_count = n_new;
        {
            int prc;
            segdb_manifest_copy(&db->pending_manifest, &g_manifest_scratch);
            db->has_pending = 1;
            db->pending_is_compact = 1;
            db->pending_next_seg_id = new_id;
            prc = segdb_publish_manifest(db, &g_manifest_scratch);
            if (prc == -2) {
                db->indeterminate = 1;
                return SEGDB_INDETERMINATE;
            }
            if (prc != 0) {
                db->has_pending = 0;
                return -1; /* old CURRENT still valid; new segments orphaned */
            }
            db->has_pending = 0;
        }
        /* New CURRENT durable: adopt, then remove old objects ONLY
         * now. A remove failure returns -1 but the new manifest IS
         * current (new-valid state; leftovers are orphans for
         * later compaction). Intermediates, if any were published
         * directly, are the new segments, not removed. */
        {
            char old_objs[MANIFEST_MAX_SEGMENTS][MANIFEST_MAX_OBJ_LEN + 1];
            uint32_t n_old = db->manifest.segment_count;
            int remove_failed = 0;
            for (i = 0; i < n_old; i++) {
                uint32_t p = 0;
                while (db->manifest.segments[i].obj[p] != '\0' && p + 1 < MANIFEST_MAX_OBJ_LEN + 1) {
                    old_objs[i][p] = db->manifest.segments[i].obj[p];
                    p++;
                }
                old_objs[i][p] = '\0';
            }
            segdb_manifest_copy(&db->manifest, &g_manifest_scratch);
            db->has_manifest = 1;
            db->next_seg_id = new_id;
            for (i = 0; i < n_old; i++) {
                if (store_remove(db->store, old_objs[i]) != 0) remove_failed = 1;
            }
            if (have_mid && n_mid > SEGDB_STREAM_SOURCES) {
                /* Published intermediates: nothing extra to remove. */
            } else if (have_mid) {
                for (i = 0; i < n_mid; i++) {
                    if (store_remove(db->store, mid_names[i]) != 0) remove_failed = 1;
                }
            }
            if (remove_failed) return -1;
        }
    }
    return 0;
}

/* ---- recover helpers (Track A) ---- */
/* Phase B: the old segdb_load_named_manifest helper is GONE.
 * Its job (pointer + named manifest + gen-1 fallback, never
 * on I/O error) is now inside the shared resolver
 * segdb_resolve_published_state (manifest.c), called by
 * both segdb_recover and segdb_reconcile. Keeping a second
 * copy here would let the two paths drift apart again. */

/* Verify every segment listed in m (open + full discovery +
 * checksum). Returns 0/-1; never adopts on failure. */
static int segdb_verify_manifest_segments(SegDB* db, const Manifest* m) {
    uint32_t i;
    for (i = 0; i < m->segment_count; i++) {
        const SegMeta* meta = &m->segments[i];
        uint64_t actual = 0;
        if (segdb_verify_segment(db->store, meta->obj, meta->entry_count,
                                 meta->first_key, meta->first_key_len,
                                 meta->last_key, meta->last_key_len) != 0) return -1;
        if (seg_file_checksum(db->store, meta->obj, &actual) != 0 ||
            actual != meta->checksum) return -1;
    }
    return 0;
}

static void segdb_adopt_manifest(SegDB* db, const Manifest* m, uint64_t next_hint) {
    uint64_t next = next_hint;
    uint32_t i;
    segdb_manifest_copy(&db->manifest, m);
    db->has_manifest = 1;
    db->seq_base = db->manifest.checkpoint_seq;
    for (i = 0; i < db->manifest.segment_count; i++) {
        uint64_t id;
        if (seg_parse_seg_name(db->manifest.segments[i].obj, &id) == 0) {
            if (id + 1 > next) next = id + 1;
        }
    }
    db->next_seg_id = next;
}

/* Forward declaration: tombstone-aware hot replay (Track A item 4,
 * defined below recover). Falls back to Phase 1 tx_replay_wal for
 * WAL bytes it cannot parse tombstone-aware. */
static int segdb_replay_hot(SegDB* db, const uint8_t* wal, uint32_t wal_len);

/* Directory-mode recover: load every group manifest (verifying
 * its segments), adopt the last group as db->manifest, and derive
 * next_seg_id from the largest segment id across all groups. Any
 * unreadable group manifest or segment fails recovery outright —
 * there is no older-generation fallback in directory mode, because
 * falling back would silently drop whole groups. */
static int segdb_recover_directory(SegDB* db, const ManifestDir* dir) {
    uint32_t gi, si;
    uint64_t max_id = 0;
    if (dir->group_count == 0 || dir->group_count > MANIFEST_DIR_MAX_GROUPS) return -1;
    for (gi = 0; gi < dir->group_count; gi++) {
        const Manifest* m;
        if (gi == dir->group_count - 1) {
            if (manifest_read(db->store, dir->groups[gi], &db->manifest) != 0) return -1;
            m = &db->manifest;
        } else {
            if (manifest_read(db->store, dir->groups[gi], &g_group_scratch) != 0) return -1;
            m = &g_group_scratch;
        }
        if (segdb_verify_manifest_segments(db, m) != 0) return -1;
        for (si = 0; si < m->segment_count; si++) {
            uint64_t id = 0;
            if (seg_parse_seg_name(m->segments[si].obj, &id) == 0 && id > max_id) max_id = id;
        }
        segdb_copy_name(db->group_objs[gi], dir->groups[gi]);
    }
    db->group_count = dir->group_count;
    db->has_dir = 1;
    segdb_copy_name(db->dir_obj, MANIFEST_DIR_OBJ);
    db->has_manifest = 1;
    db->seq_base = db->manifest.checkpoint_seq;
    db->next_seg_id = max_id + 1;
    return 0;
}

/* ---- recover (O2: Gate 2 helpers wired in) ----
 * Pointer resolution is delegated to manifest_recover_pointer
 * (CURRENT, then CURRENT.bak; -1 only when the store is
 * authoritatively empty, -2 for ambiguous / I/O-error / unknown
 * status), and single-mode manifest loading to
 * manifest_recover_manifest (previous-generation fallback on a
 * clean manifest-read failure, never on an I/O error — see
 * manifest.c). The old ad-hoc branches are gone, including the
 * UNKNOWN-backend branch that could masquerade a metadata error
 * as absence and then fall through to virgin: on an UNKNOWN
 * backend manifest_recover_pointer returns -2, and recovery
 * fails below, never opens empty. The one deliberate exception
 * to a bare -2 failure is the pointerless directory salvage in
 * the -2 branch: a surviving manifest.dir object is complete
 * metadata (it names every group, and segdb_recover_directory
 * verifies every group manifest and segment), so adopting it
 * cannot silently drop a generation the way adopting a stale
 * single manifest could. A corrupt/unreadable salvaged
 * directory still fails outright, never falls back.
 *
 * DELIBERATELY REMOVED: the old pointerless "manifest.1"
 * fallback. Adopting manifest.1 when both pointers are gone
 * could adopt a stale first generation while later generations'
 * segments still survive, silently losing committed data — the
 * exact hazard manifest_recover_pointer exists to close. With
 * a pointer present, the previous-generation fallback still
 * applies (via manifest_recover_manifest); without one, only
 * the directory salvage above, or authoritative emptiness,
 * may succeed. This intentionally changes
 * test_tracka_double_loss_is_virgin_indistinguishable's old
 * expectation (pointers + manifest.1 deleted, segment left):
 * that store now fails recovery instead of opening virgin —
 * see test/test_recovery_wire.c, which pins the new rule.
 *
 * DIRECTORY-MODE GAP, documented not skipped:
 * manifest_recover_manifest's fallback covers only
 * "manifest.<gen>" names. It does NOT cover directory mode:
 * a pointer naming "manifest.dir" is dispatched here BEFORE
 * manifest_recover_manifest is called (that helper would
 * return -2 on the unparseable name), and group manifests
 * inside segdb_recover_directory get NO per-group gen-1
 * fallback — a corrupt group fails recovery outright, because
 * falling back a group would silently drop whole groups.
 * The directory object itself has a fixed name and no
 * previous-generation object, so there is no directory-level
 * gen-1 to fall back to either. */
/* Phase B: shared published-state scratch. SegdbPublishedState
 * holds a full Manifest (~285 KiB), so it is file-scope
 * static, never on a stack. */
static SegdbPublishedState g_resolve_scratch;

int segdb_recover(SegDB* db) {
    uint64_t sz = 0;
    if (!db || !db->store || db->recovered) return -1;
    /* Phase B: ONE shared resolution. Recover no longer
     * re-reads a pointer + named manifest with separate
     * logic; the resolver returns a validated, checkable
     * kind and recover dispatches on it. Semantics are
     * unchanged from the O2 wiring: VIRGIN is the only
     * path that opens empty; DIRECTORY / DIRECTORY_ROOT
     * recover groups with full verification; SINGLE /
     * FALLBACK verify segments then adopt; AMBIGUOUS /
     * IO_ERROR never open empty, with the single
     * pointerless salvage being a surviving legacy
     * directory object (complete metadata, verified by
     * segdb_recover_directory). A versioned root is
     * recovered the same way as a legacy directory, and
     * db->dir_obj records the root name (not the fixed
     * directory name) so later publishes address the
     * right object. */
    if (segdb_resolve_published_state(db->store, &g_resolve_scratch) != 0) return -1;
    if (g_resolve_scratch.kind == SEGDB_PUBLISHED_VIRGIN) {
        /* Authoritatively virgin: no pointer and no objects at
         * all. Only this case opens empty. */
        db->manifest.generation = 0;
        db->manifest.checkpoint_seq = 0;
        db->manifest.schema_version = 0;
        db->manifest.segment_count = 0;
        db->has_manifest = 0;
        db->seq_base = 0;
        db->next_seg_id = 1;
        db->group_count = 0;
        db->has_dir = 0;
        goto replay_hot;
    }
    if (g_resolve_scratch.kind == SEGDB_PUBLISHED_DIRECTORY ||
        g_resolve_scratch.kind == SEGDB_PUBLISHED_DIRECTORY_ROOT) {
        if (segdb_recover_directory(db, &g_resolve_scratch.dir) != 0) return -1;
        if (g_resolve_scratch.kind == SEGDB_PUBLISHED_DIRECTORY_ROOT)
            segdb_copy_name(db->dir_obj, g_resolve_scratch.pointer);
        goto replay_hot;
    }
    if (g_resolve_scratch.kind != SEGDB_PUBLISHED_SINGLE &&
        g_resolve_scratch.kind != SEGDB_PUBLISHED_FALLBACK) {
        /* AMBIGUOUS / IO_ERROR: never virgin. The only
         * salvage is a surviving directory object
         * (complete metadata, verified by
         * segdb_recover_directory); a directory metadata
         * I/O error also fails, never falls back. */
        int dst = store_size_status(db->store, MANIFEST_DIR_OBJ, &sz);
        if (dst == YOWZA_STORE_IO_ERROR) return -1;
        if (dst == YOWZA_STORE_OK) {
            if (manifest_dir_read(db->store, MANIFEST_DIR_OBJ, &g_dir_scratch) != 0) return -1;
            if (segdb_recover_directory(db, &g_dir_scratch) != 0) return -1;
            goto replay_hot;
        }
        return -1;
    }
    if (segdb_verify_manifest_segments(db, &g_resolve_scratch.manifest) != 0) {
        db->manifest.segment_count = 0;
        db->has_manifest = 0;
        return -1;
    }
    segdb_adopt_manifest(db, &g_resolve_scratch.manifest, 1);
replay_hot:
    /* Hot WAL replay (Phase 1 behaviour, unfiltered by sequence —
     * see segdb.h). Post-flush WAL is empty by construction, so the
     * normal restarted case replays nothing. Track A replay is
     * tombstone-aware (item 4): DELs reconstruct hot tombstones
     * that shadow segment values, they are not no-ops. */
    if (db->hot.storage.log_length) {
        int llen = db->hot.storage.log_length(&db->hot.storage);
        if (llen > 0) {
            if (db->wal_buf) {
                if ((uint64_t)llen > db->wal_cap) return -1;
                if (segdb_replay_hot(db, db->wal_buf, (uint32_t)llen) != 0) return -1;
            } else if (db->hot.storage.read) {
                /* Driver-backed WAL (e.g. POSIX): stream it through
                 * a bounded static buffer. WALs larger than the
                 * buffer fail recovery explicitly rather than
                 * replaying partially. */
                static uint8_t g_replay_buf[262144];
                if ((uint64_t)llen > sizeof(g_replay_buf)) return -1;
                if (db->hot.storage.read(&db->hot.storage, g_replay_buf, (uint32_t)llen, 0) != llen) return -1;
                if (segdb_replay_hot(db, g_replay_buf, (uint32_t)llen) != 0) return -1;
            } else {
                return -1;
            }
        } else if (llen < 0) {
            return -1;
        }
    }
    db->recovered = 1;
    return 0;
}

/* ---- tombstone-aware hot replay (Track A item 4) ----
 * Phase 1 tx_replay_wal is correct for a hot-only ART, but for
 * segdb it is NOT sufficient: art_delete on an absent key is a
 * no-op, so a DEL for a segment-resident key replayed onto an
 * empty hot ART leaves no tombstone, and the old segment value
 * resurfaces. This replay parses the same WAL stream (legacy
 * records + YWTX frames, validated like tx_replay_wal: frame
 * checksum, commit marker, envelope payload checksums) and, for
 * every DEL, ENSURES a hot tombstone: live -> art_delete;
 * already-tombstoned -> leave; absent/intermediate ->
 * art_insert(empty) + art_delete (in-memory only, no WAL write).
 * PUTs apply with normal version increments (rebuild order, in
 * WAL order), which under segdb's seq_base mapping keeps hot
 * entries newer than every segment entry. A truncated or corrupt
 * frame stops replay and applies nothing from that frame onward
 * (earlier records stay applied, exactly like tx_replay_wal). */
static uint64_t seg_replay_fnv(const uint8_t* data, uint32_t len) {
    uint64_t h = 14695981039346656037ULL;
    uint32_t i;
    for (i = 0; i < len; i++) {
        h ^= data[i];
        h *= 1099511628211ULL;
    }
    return h;
}

static uint32_t seg_replay_u32(const uint8_t* p) {
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}

static uint64_t seg_replay_u64(const uint8_t* p) {
    uint64_t v = 0;
    int i;
    for (i = 0; i < 8; i++) v |= (uint64_t)p[i] << (i * 8);
    return v;
}

static void seg_replay_del(ART* art, const uint8_t* key, uint32_t key_len) {
    ARTNode* term = art_find_terminal(art, key, key_len);
    if (term && term->is_end) {
        (void)art_delete(art, key, key_len);
        return;
    }
    if (term && !term->is_end && term->timestamp > 0) return; /* already tombstoned */
    /* Absent or intermediate-only: materialise the tombstone. */
    if (art_insert(art, key, key_len, (const uint8_t*)"", 0, 0) == 0) {
        (void)art_delete(art, key, key_len);
    }
}

/* Validate one YWTX frame (checksum, commit marker, envelope
 * payload checksums, bounds) and, if valid, apply its ops.
 * Returns frame length on success, 0 if the frame must stop replay
 * (incomplete/corrupt — not an error for the caller, just a stop). */
static uint32_t seg_replay_frame(ART* art, const uint8_t* wal, uint32_t wal_len, uint32_t off) {
    uint32_t frame_len, op_count, pos, i;
    if (off + 9 > wal_len) return 0;
    if (!(wal[off + 1] == 'Y' && wal[off + 2] == 'W' && wal[off + 3] == 'T' && wal[off + 4] == 'X')) return 0;
    frame_len = seg_replay_u32(&wal[off + 5]);
    if (frame_len < 42) return 0;
    if (off + frame_len > wal_len) return 0;
    if (seg_replay_fnv(&wal[off], frame_len - 8) != seg_replay_u64(&wal[off + frame_len - 8])) return 0;
    if (wal[off + frame_len - 9] != 0xC1) return 0;
    op_count = seg_replay_u32(&wal[off + 9 + 8 + 8]);
    pos = off + 9 + 8 + 8 + 4 + 4;
    /* Validation pass (no mutation). */
    for (i = 0; i < op_count; i++) {
        uint32_t ep, fmt, op_type, dep_count;
        uint64_t payload_expect;
        uint32_t key_len, val_len;
        if (pos + 60 > off + frame_len - 9) return 0;
        ep = pos;
        fmt = seg_replay_u32(&wal[ep]); ep += 4;
        if (fmt != 1) return 0;
        ep += 8 + 8 + 8 + 8;
        op_type = seg_replay_u32(&wal[ep]); ep += 4;
        if (op_type != 0x01 && op_type != 0x03) return 0;
        ep += 4 + 4 + 8;
        dep_count = seg_replay_u32(&wal[ep]); ep += 4;
        if (dep_count > 1024) return 0;
        if (ep + dep_count * 8 > off + frame_len - 9) return 0;
        ep += dep_count * 8;
        payload_expect = seg_replay_u64(&wal[ep]); ep += 8;
        /* Payload checksum: envelope fields before the checksum
         * field, then key_len/key/val_len/val (see PHASE1_NOTES). */
        {
            uint64_t h = 14695981039346656037ULL;
            uint32_t j;
            for (j = pos; j < ep - 8; j++) { h ^= wal[j]; h *= 1099511628211ULL; }
            if (ep + 4 > off + frame_len - 9) return 0;
            key_len = seg_replay_u32(&wal[ep]);
            for (j = 0; j < 4; j++) { h ^= wal[ep + j]; h *= 1099511628211ULL; }
            ep += 4;
            if (ep + key_len > off + frame_len - 9) return 0;
            for (j = 0; j < key_len; j++) { h ^= wal[ep + j]; h *= 1099511628211ULL; }
            ep += key_len;
            if (ep + 4 > off + frame_len - 9) return 0;
            val_len = seg_replay_u32(&wal[ep]);
            for (j = 0; j < 4; j++) { h ^= wal[ep + j]; h *= 1099511628211ULL; }
            ep += 4;
            if (ep + val_len > off + frame_len - 9) return 0;
            for (j = 0; j < val_len; j++) { h ^= wal[ep + j]; h *= 1099511628211ULL; }
            ep += val_len;
            if (h != payload_expect) return 0;
        }
        pos = ep;
    }
    if (pos != off + frame_len - 9) return 0;
    /* Apply pass. */
    pos = off + 9 + 8 + 8 + 4 + 4;
    for (i = 0; i < op_count; i++) {
        uint32_t ep = pos;
        uint32_t op_type, dep_count, key_len, val_len;
        uint64_t timestamp;
        const uint8_t* key;
        const uint8_t* val;
        ep += 4 + 8 + 8 + 8 + 8;
        op_type = seg_replay_u32(&wal[ep]); ep += 4;
        ep += 4 + 4;
        timestamp = seg_replay_u64(&wal[ep]); ep += 8;
        dep_count = seg_replay_u32(&wal[ep]); ep += 4;
        ep += dep_count * 8 + 8;
        key_len = seg_replay_u32(&wal[ep]); ep += 4;
        key = &wal[ep]; ep += key_len;
        val_len = seg_replay_u32(&wal[ep]); ep += 4;
        val = &wal[ep]; ep += val_len;
        if (op_type == 0x01) {
            if (art_insert(art, key, key_len, val, val_len, timestamp) != 0) return 0;
        } else {
            seg_replay_del(art, key, key_len);
        }
        pos = ep;
    }
    return frame_len;
}

static int segdb_replay_hot(SegDB* db, const uint8_t* wal, uint32_t wal_len) {
    ART* art = &db->hot.index;
    uint32_t off = 0;
    if (!wal && wal_len > 0) return -1;
    while (off < wal_len) {
        uint8_t op = wal[off];
        if (op == 0xF0) {
            uint32_t flen = seg_replay_frame(art, wal, wal_len, off);
            if (flen == 0) break; /* incomplete/corrupt frame: stop */
            off += flen;
            continue;
        }
        if (op != 0x01 && op != 0x03 && op != 0x07) break; /* unknown: stop */
        {
            uint32_t p = off + 1;
            uint64_t ts = 0;
            uint32_t key_len, val_len;
            const uint8_t* key;
            const uint8_t* val;
            if (op == 0x07) {
                if (p + 8 > wal_len) break;
                ts = seg_replay_u64(&wal[p]); p += 8;
            }
            if (p + 4 > wal_len) break;
            key_len = seg_replay_u32(&wal[p]); p += 4;
            if (p + key_len > wal_len) break;
            key = &wal[p]; p += key_len;
            if (p + 4 > wal_len) break;
            val_len = seg_replay_u32(&wal[p]); p += 4;
            if (p + val_len > wal_len) break;
            val = &wal[p]; p += val_len;
            if (op == 0x03) {
                seg_replay_del(art, key, key_len);
            } else {
                if (art_insert(art, key, key_len, val, val_len, ts) != 0) break;
            }
            off = p;
        }
    }
    return 0;
}

/* ---- reconcile (Track A three-state publish) ---- */
int segdb_reconcile(SegDB* db) {
    uint64_t sz = 0;
    uint64_t cur_gen = 0;
    int st;
    Manifest loaded;
    if (!db || !db->store) return -1;
    if (!db->indeterminate) return 0;
    /* ---- directory-mode pending states (Track A item 3) ---- */
    if (db->pending_group_append) {
        /* Last-group manifest rewrite: compare the on-disk last
         * group manifest with the pending and old versions. */
        ManifestDir dir;
        Manifest loaded;
        if (manifest_dir_read(db->store, MANIFEST_DIR_OBJ, &dir) != 0) return -1;
        if (dir.group_count == 0) return -1;
        if (manifest_read(db->store, dir.groups[dir.group_count - 1], &loaded) != 0) return -1;
        if (segdb_manifests_equal(&loaded, &db->pending_group_manifest)) {
            segdb_manifest_copy(&db->manifest, &loaded);
            db->seq_base = loaded.checkpoint_seq;
            db->next_seg_id = db->pending_next_seg_id;
            if (db->hot.storage.truncate) (void)db->hot.storage.truncate(&db->hot.storage, 0);
            arena_reset(&db->hot.arena);
            art_init(&db->hot.index, &db->hot.arena);
            segdb_clear_pending(db);
            return 0;
        }
        if (segdb_manifests_equal(&loaded, &db->pending_old_group_manifest)) {
            segdb_clear_pending(db);
            return 0; /* publish did not take effect; hot intact */
        }
        return -1; /* neither old nor pending: stay indeterminate */
    }
    if (db->pending_group_add) {
        /* Directory list replacement (flush addition or group
         * compact): compare the on-disk group list. */
        ManifestDir dir;
        uint32_t g;
        if (manifest_dir_read(db->store, MANIFEST_DIR_OBJ, &dir) != 0) return -1;
        if (segdb_group_lists_equal((const char (*)[MANIFEST_MAX_OBJ_LEN + 1])dir.groups, dir.group_count,
                                    (const char (*)[MANIFEST_MAX_OBJ_LEN + 1])db->pending_group_objs,
                                    db->pending_group_count)) {
            segdb_manifest_copy(&db->manifest, &db->pending_group_manifest);
            db->group_count = db->pending_group_count;
            for (g = 0; g < db->pending_group_count; g++)
                segdb_copy_name(db->group_objs[g], db->pending_group_objs[g]);
            db->seq_base = db->pending_group_manifest.checkpoint_seq;
            db->next_seg_id = db->pending_next_seg_id;
            if (!db->pending_is_compact) {
                if (db->hot.storage.truncate) (void)db->hot.storage.truncate(&db->hot.storage, 0);
                arena_reset(&db->hot.arena);
                art_init(&db->hot.index, &db->hot.arena);
            }
            segdb_clear_pending(db);
            return 0;
        }
        if (segdb_group_lists_equal((const char (*)[MANIFEST_MAX_OBJ_LEN + 1])dir.groups, dir.group_count,
                                    (const char (*)[MANIFEST_MAX_OBJ_LEN + 1])db->pending_old_group_objs,
                                    db->pending_old_group_count)) {
            segdb_clear_pending(db);
            return 0; /* publish did not take effect */
        }
        return -1;
    }
    if (db->pending_dir_transition) {
        /* CURRENT pointer: "manifest.dir" means the transition
         * took effect; the old manifest name means it did not.
         * Anything else (including missing CURRENT) stays
         * indeterminate — a manifest existed before, so this is
         * never virgin. */
        char ptr_buf[MANIFEST_MAX_OBJ_LEN + 1];
        ManifestDir dir;
        uint32_t g;
        st = store_size_status(db->store, "CURRENT", &sz);
        if (st == YOWZA_STORE_IO_ERROR) return -1;
        /* Axis C virgin multi-group transition: when the
         * pending transition had NO old group, a missing
         * CURRENT is exactly the old state (the DB had no
         * pointer before the flush), so reconcile clears
         * the pending state instead of staying
         * indeterminate forever. With an old group, a
         * missing CURRENT stays indeterminate, as before. */
        if (st == YOWZA_STORE_NOT_FOUND && db->pending_old_group_count == 0) {
            segdb_clear_pending(db);
            return 0;
        }
        if (st != YOWZA_STORE_OK) return -1;
        if (manifest_read_current(db->store, "CURRENT", ptr_buf, (uint32_t)sizeof(ptr_buf)) != 0) return -1;
        if (segdb_names_equal(ptr_buf, MANIFEST_DIR_OBJ)) {
            if (manifest_dir_read(db->store, MANIFEST_DIR_OBJ, &dir) != 0) return -1;
            if (!segdb_group_lists_equal((const char (*)[MANIFEST_MAX_OBJ_LEN + 1])dir.groups, dir.group_count,
                                         (const char (*)[MANIFEST_MAX_OBJ_LEN + 1])db->pending_group_objs,
                                         db->pending_group_count)) return -1;
            segdb_manifest_copy(&db->manifest, &db->pending_group_manifest);
            db->has_dir = 1;
            segdb_copy_name(db->dir_obj, MANIFEST_DIR_OBJ);
            db->group_count = db->pending_group_count;
            for (g = 0; g < db->pending_group_count; g++)
                segdb_copy_name(db->group_objs[g], db->pending_group_objs[g]);
            db->seq_base = db->pending_group_manifest.checkpoint_seq;
            db->next_seg_id = db->pending_next_seg_id;
            if (db->hot.storage.truncate) (void)db->hot.storage.truncate(&db->hot.storage, 0);
            arena_reset(&db->hot.arena);
            art_init(&db->hot.index, &db->hot.arena);
            segdb_clear_pending(db);
            return 0;
        }
        if (segdb_names_equal(ptr_buf, db->pending_old_group_objs[0])) {
            segdb_clear_pending(db);
            return 0; /* transition did not take effect; hot intact */
        }
        return -1;
    }
    /* Phase B (O2 completed): reconcile uses the SHARED
     * resolver — it no longer re-reads a pointer + named
     * manifest with its own logic (the old
     * segdb_load_named_manifest path is gone). The resolver
     * returns a checkable result and reconcile COMPARES it
     * with its own pending/old state below; it never blindly
     * picks a previous generation. Directory pending states
     * (group append / group add / transition) were already
     * handled above by comparing on-disk group lists with
     * pending lists; the directory case here covers only a
     * reconcile with no directory pending state, where the
     * on-disk group list must equal the in-memory list to
     * confirm the old state, else the DB stays
     * indeterminate. VIRGIN / AMBIGUOUS / IO_ERROR all
     * stay indeterminate: a manifest existed before this
     * publish attempt, so none of them can be the old
     * state. An I/O error in particular must not be
     * mistaken for absence. */
    if (segdb_resolve_published_state(db->store, &g_resolve_scratch) != 0) return -1;
    if (g_resolve_scratch.kind == SEGDB_PUBLISHED_DIRECTORY ||
        g_resolve_scratch.kind == SEGDB_PUBLISHED_DIRECTORY_ROOT) {
        uint32_t g;
        if (!db->has_dir) return -1;
        if (g_resolve_scratch.dir.group_count != db->group_count) return -1;
        for (g = 0; g < db->group_count; g++) {
            if (!segdb_names_equal(g_resolve_scratch.dir.groups[g], db->group_objs[g])) return -1;
        }
        /* On-disk groups == in-memory groups: the publish
         * did not change the directory; old state
         * confirmed. (A group-list change with no
         * directory pending state is unexpected and stays
         * indeterminate.) */
        db->indeterminate = 0;
        db->has_pending = 0;
        return 0;
    }
    if (g_resolve_scratch.kind != SEGDB_PUBLISHED_SINGLE &&
        g_resolve_scratch.kind != SEGDB_PUBLISHED_FALLBACK) return -1;
    /* Field-wise copy, NOT a struct assignment: a ~285 KiB
     * struct copy lowers to a memcpy import that the
     * wasm32 env module does not provide (instantiation
     * failure — the O3 fn_src_copy gotcha, same shape).
     * segdb_manifest_copy is the existing byte loop. */
    segdb_manifest_copy(&loaded, &g_resolve_scratch.manifest);
    cur_gen = g_resolve_scratch.generation;
    if (segdb_verify_manifest_segments(db, &loaded) != 0) return -1;
    if (db->has_pending && cur_gen == db->pending_manifest.generation) {
        /* Publish DID take effect: adopt the pending manifest. */
        uint64_t next_hint = db->pending_next_seg_id;
        segdb_adopt_manifest(db, &loaded, next_hint);
        db->indeterminate = 0;
        db->has_pending = 0;
        if (!db->pending_is_compact) {
            /* Flush adoption: hot entries are now in the segment,
             * so clear hot + WAL (a truncate failure here leaves
             * hot as a harmless duplicate cache — values identical,
             * deduped by sequence — and does not re-indeterminate
             * the manifest, which is already adopted). */
            if (db->hot.storage.truncate) (void)db->hot.storage.truncate(&db->hot.storage, 0);
            arena_reset(&db->hot.arena);
            art_init(&db->hot.index, &db->hot.arena);
            db->seq_base = loaded.checkpoint_seq;
        }
        return 0;
    }
    if (cur_gen == db->manifest.generation) {
        /* Publish did NOT take effect: old state confirmed, hot
         * untouched (flush) / old segments intact (compact). */
        db->indeterminate = 0;
        db->has_pending = 0;
        return 0;
    }
    /* Loaded generation is neither old nor pending: unexpected
     * state (e.g. a concurrent publisher). Stay indeterminate. */
    return -1;
}
