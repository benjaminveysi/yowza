/* Phase 2, Track B — Immutable Segment format v1. See include/segment.h
 * for the full layout, reader memory contract, and extension contract.
 * No libc, no malloc: only manual byte loops, so this builds for the
 * Wasm targets as well as natively. */

#include "segment.h"

/* ---------------- small helpers ---------------- */

static uint64_t seg_fnv_init(void) { return 14695981039346656037ULL; }

static uint64_t seg_fnv_update(uint64_t h, const uint8_t* data, uint64_t len) {
    uint64_t i;
    for (i = 0; i < len; i++) {
        h ^= (uint64_t)data[i];
        h *= 1099511628211ULL;
    }
    return h;
}

static uint64_t seg_splitmix64(uint64_t x) {
    x += 0x9e3779b97f4a7c15ULL;
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
    return x ^ (x >> 31);
}

static void seg_wr32(uint8_t* buf, uint32_t v) {
    buf[0] = (uint8_t)(v & 0xFF);
    buf[1] = (uint8_t)((v >> 8) & 0xFF);
    buf[2] = (uint8_t)((v >> 16) & 0xFF);
    buf[3] = (uint8_t)((v >> 24) & 0xFF);
}

static void seg_wr64(uint8_t* buf, uint64_t v) {
    int i;
    for (i = 0; i < 8; i++) buf[i] = (uint8_t)((v >> (i * 8)) & 0xFF);
}

static uint32_t seg_rd32(const uint8_t* buf) {
    return (uint32_t)buf[0] | ((uint32_t)buf[1] << 8) |
           ((uint32_t)buf[2] << 16) | ((uint32_t)buf[3] << 24);
}

static uint64_t seg_rd64(const uint8_t* buf) {
    uint64_t v = 0;
    int i;
    for (i = 0; i < 8; i++) v |= (uint64_t)buf[i] << (i * 8);
    return v;
}

/* Lexicographic byte order: -1 if a < b, 0 if equal, 1 if a > b. */
static int seg_key_cmp(const uint8_t* a, uint32_t a_len,
                       const uint8_t* b, uint32_t b_len) {
    uint32_t i, n = a_len < b_len ? a_len : b_len;
    for (i = 0; i < n; i++) {
        if (a[i] != b[i]) return a[i] < b[i] ? -1 : 1;
    }
    if (a_len == b_len) return 0;
    return a_len < b_len ? -1 : 1;
}

static void seg_mem_copy(uint8_t* dst, const uint8_t* src, uint64_t len) {
    uint64_t i;
    for (i = 0; i < len; i++) dst[i] = src[i];
}

static void seg_mem_zero(uint8_t* dst, uint64_t len) {
    uint64_t i;
    for (i = 0; i < len; i++) dst[i] = 0;
}

static int seg_str_len(const char* s, uint64_t* out) {
    uint64_t n = 0;
    if (!s) return -1;
    while (s[n] != '\0') {
        n++;
        if (n > SEG_MAX_OBJ_LEN) return -1;
    }
    *out = n;
    return 0;
}

/* Accept both driver conventions for read_at/write_at: 0, or the exact
 * requested byte count, means success; anything else is an error. */
static int seg_io_ok(int rc, uint64_t len) {
    if (rc == 0) return 1;
    if (rc > 0 && (uint64_t)rc == len) return 1;
    return 0;
}

static int seg_has_fn(YowzaStore* st, int which) {
    /* which: 0 write_at, 1 read_at, 2 size, 3 truncate */
    if (!st || !st->vt) return 0;
    switch (which) {
        case 0: return st->vt->write_at != 0;
        case 1: return st->vt->read_at != 0;
        case 2: return st->vt->size != 0;
        case 3: return st->vt->truncate != 0;
        default: return 0;
    }
}

/* ---------------- writer helpers ---------------- */

/* Write len bytes at *off, advance *off, and fold the bytes into *fnv
 * (pass NULL for fnv to skip checksumming). */
static int seg_w_bytes(YowzaStore* st, const char* obj, uint64_t* off,
                       const uint8_t* data, uint64_t len, uint64_t* fnv) {
    int rc;
    if (len == 0) return 0;
    if (!data) return -1;
    rc = st->vt->write_at(st->state, obj, *off, data, len);
    if (!seg_io_ok(rc, len)) return -1;
    if (fnv) *fnv = seg_fnv_update(*fnv, data, len);
    *off += len;
    return 0;
}

static int seg_w_u32(YowzaStore* st, const char* obj, uint64_t* off,
                     uint32_t v, uint64_t* fnv) {
    uint8_t b[4];
    seg_wr32(b, v);
    return seg_w_bytes(st, obj, off, b, 4, fnv);
}

static int seg_w_u64(YowzaStore* st, const char* obj, uint64_t* off,
                     uint64_t v, uint64_t* fnv) {
    uint8_t b[8];
    seg_wr64(b, v);
    return seg_w_bytes(st, obj, off, b, 8, fnv);
}

static uint64_t seg_entry_len(const SegEntry* e) {
    return (uint64_t)4 + e->key_len + (uint64_t)4 + e->val_len + (uint64_t)8 +
           (uint64_t)8 + (uint64_t)1;
}

/* Write one entry's bytes, folding them into *fnv. */
static int seg_w_entry(YowzaStore* st, const char* obj, uint64_t* off,
                       const SegEntry* e, uint64_t* fnv) {
    uint8_t flags = e->is_tombstone ? SEG_ENTRY_FLAG_TOMBSTONE : 0;
    if (seg_w_u32(st, obj, off, e->key_len, fnv) != 0) return -1;
    if (seg_w_bytes(st, obj, off, e->key, e->key_len, fnv) != 0) return -1;
    if (seg_w_u32(st, obj, off, e->val_len, fnv) != 0) return -1;
    if (e->val_len > 0 &&
        seg_w_bytes(st, obj, off, e->val, e->val_len, fnv) != 0) return -1;
    if (seg_w_u64(st, obj, off, e->commit_seq, fnv) != 0) return -1;
    if (seg_w_u64(st, obj, off, e->crdt_ts, fnv) != 0) return -1;
    if (seg_w_bytes(st, obj, off, &flags, 1, fnv) != 0) return -1;
    return 0;
}

int seg_write(YowzaStore* st, const char* obj, const SegEntry* entries,
              uint32_t count, uint32_t index_type, uint32_t model_version,
              int use_bloom, uint64_t* out_bytes) {
    uint32_t i;
    uint64_t min_seq = 0, max_seq = 0, off, header_len, header_fnv, index_fnv;
    uint64_t block_fnv, block_bytes, blk_off;
    uint8_t header[SEG_HEADER_SIZE];
    uint8_t footer[SEG_FOOTER_SIZE];
    uint32_t first_len = 0, last_len = 0;
    uint32_t flags = 0;

    if (!seg_has_fn(st, 0) || !seg_has_fn(st, 3)) return -1;
    if (!obj || obj[0] == '\0') return -1;
    if (count > 0 && !entries) return -1;
    if (index_type != SEG_INDEX_TYPE_SPARSE) return -1;
    if (model_version != SEG_MODEL_VERSION_NONE) return -1;

    /* Validate entries: non-empty keys, tombstone shape, strict order. */
    for (i = 0; i < count; i++) {
        const SegEntry* e = &entries[i];
        if (!e->key || e->key_len == 0) return -1;
        if (e->is_tombstone) {
            if (e->val_len != 0) return -1;
        } else if (e->val_len > 0 && !e->val) {
            return -1;
        }
        if (i > 0 &&
            seg_key_cmp(entries[i - 1].key, entries[i - 1].key_len,
                        e->key, e->key_len) >= 0) {
            return -1; /* unsorted or duplicate */
        }
        if (i == 0) {
            min_seq = e->commit_seq;
            max_seq = e->commit_seq;
        } else {
            if (e->commit_seq < min_seq) min_seq = e->commit_seq;
            if (e->commit_seq > max_seq) max_seq = e->commit_seq;
        }
    }
    if (count > 0) {
        first_len = entries[0].key_len;
        last_len = entries[count - 1].key_len;
    }
    if (use_bloom) flags |= SEG_FLAG_BLOOM;
    header_len = (uint64_t)SEG_HEADER_SIZE + first_len + last_len;

    /* Header (64 bytes). */
    seg_mem_zero(header, SEG_HEADER_SIZE);
    header[0] = 'Y'; header[1] = 'W'; header[2] = 'S'; header[3] = 'E';
    header[4] = 'G'; header[5] = '1';
    seg_wr32(&header[6], SEG_FORMAT_VERSION);
    seg_wr32(&header[10], SEG_BLOCK_SIZE);
    seg_wr32(&header[14], count);
    seg_wr32(&header[18], index_type);
    seg_wr32(&header[22], model_version);
    seg_wr64(&header[26], min_seq);
    seg_wr64(&header[34], max_seq);
    seg_wr32(&header[42], first_len);
    seg_wr32(&header[46], last_len);
    seg_wr32(&header[50], flags);
    /* bytes 54..63 stay zero (reserved). */

    off = 0;
    header_fnv = seg_fnv_init();
    if (seg_w_bytes(st, obj, &off, header, SEG_HEADER_SIZE, &header_fnv) != 0)
        return -1;
    if (count > 0) {
        if (seg_w_bytes(st, obj, &off, entries[0].key, first_len,
                        &header_fnv) != 0)
            return -1;
        if (seg_w_bytes(st, obj, &off, entries[count - 1].key, last_len,
                        &header_fnv) != 0)
            return -1;
    }

    /* Data blocks pass. */
    block_fnv = seg_fnv_init();
    block_bytes = 0;
    for (i = 0; i < count; i++) {
        uint64_t elen = seg_entry_len(&entries[i]);
        if (block_bytes > 0 && block_bytes + elen > SEG_BLOCK_SIZE) {
            if (seg_w_u64(st, obj, &off, block_fnv, 0) != 0) return -1;
            block_bytes = 0;
            block_fnv = seg_fnv_init();
        }
        if (seg_w_entry(st, obj, &off, &entries[i], &block_fnv) != 0)
            return -1;
        block_bytes += elen;
    }
    if (count > 0) {
        if (seg_w_u64(st, obj, &off, block_fnv, 0) != 0) return -1;
    }

    /* Sparse index pass: re-walk with the same split rule so block
     * offsets/lengths match the blocks pass exactly, without storing
     * per-block state. */
    index_fnv = seg_fnv_init();
    blk_off = header_len;
    i = 0;
    while (i < count) {
        uint32_t first = i;
        uint64_t blen = 0;
        while (i < count) {
            uint64_t elen = seg_entry_len(&entries[i]);
            if (blen > 0 && blen + elen > SEG_BLOCK_SIZE) break;
            blen += elen;
            i++;
        }
        if (seg_w_u32(st, obj, &off, entries[first].key_len, &index_fnv) != 0)
            return -1;
        if (seg_w_bytes(st, obj, &off, entries[first].key,
                        entries[first].key_len, &index_fnv) != 0)
            return -1;
        if (seg_w_u64(st, obj, &off, blk_off, &index_fnv) != 0) return -1;
        if (seg_w_u64(st, obj, &off, blen, &index_fnv) != 0) return -1;
        blk_off += blen + 8;
    }
    if (seg_w_u64(st, obj, &off, index_fnv, 0) != 0) return -1;

    /* Bloom filter pass (fixed-size stack filter, see segment.h). */
    if (use_bloom) {
        uint64_t nbits;
        uint32_t filter_bytes, b;
        uint8_t bloom[8192];
        uint64_t bloom_fnv;
        if (count == 0) {
            nbits = 1024;
        } else {
            nbits = (uint64_t)count * 8;
            if (nbits < 1024) nbits = 1024;
            if (nbits > 65536) nbits = 65536;
        }
        filter_bytes = (uint32_t)(nbits / 8);
        seg_mem_zero(bloom, filter_bytes);
        for (i = 0; i < count; i++) {
            uint64_t h1 = seg_fnv_init();
            uint64_t h2, p;
            int k;
            h1 = seg_fnv_update(h1, entries[i].key, entries[i].key_len);
            h2 = seg_splitmix64(h1 + 0x9e3779b97f4a7c15ULL);
            for (k = 0; k < 2; k++) {
                p = (h1 + (uint64_t)k * h2) % nbits;
                bloom[p / 8] |= (uint8_t)(1u << (p % 8));
            }
        }
        bloom_fnv = seg_fnv_init();
        if (seg_w_u32(st, obj, &off, filter_bytes, 0) != 0) return -1;
        /* Checksum covers the filter bytes only (not the length). */
        bloom_fnv = seg_fnv_update(bloom_fnv, bloom, filter_bytes);
        if (seg_w_bytes(st, obj, &off, bloom, filter_bytes, 0) != 0)
            return -1;
        if (seg_w_u64(st, obj, &off, bloom_fnv, 0) != 0) return -1;
        (void)b;
    }

    /* Footer (32 bytes). */
    seg_mem_zero(footer, SEG_FOOTER_SIZE);
    footer[0] = 'Y'; footer[1] = 'W'; footer[2] = 'S'; footer[3] = 'E';
    footer[4] = 'G'; footer[5] = '$';
    seg_wr64(&footer[6], off + SEG_FOOTER_SIZE);
    seg_wr64(&footer[14], header_fnv);
    seg_wr64(&footer[22], index_fnv);
    /* bytes 30..31 stay zero (reserved). */
    if (seg_w_bytes(st, obj, &off, footer, SEG_FOOTER_SIZE, 0) != 0)
        return -1;

    /* Drop any stale tail from a previous longer object at this name. */
    if (st->vt->truncate(st->state, obj, off) != 0) return -1;
    if (out_bytes) *out_bytes = off;
    return 0;
}

/* ---------------- streaming writer (Track A) ---------------- */

static int seg_writer_name(char* out, uint32_t out_cap, const char* obj, const char* sfx) {
    uint32_t i = 0, j = 0;
    while (obj[i] != '\0') {
        if (i + 1 >= out_cap) return -1;
        out[i] = obj[i];
        i++;
    }
    while (sfx[j] != '\0') {
        if (i + 1 >= out_cap) return -1;
        out[i] = sfx[j];
        i++; j++;
    }
    out[i] = '\0';
    return 0;
}

static int seg_writer_flush_block(SegWriter* w) {
    uint64_t fnv, off;
    uint8_t cks[8];
    uint8_t lb[4];
    if (w->block_len == 0) return 0;
    fnv = seg_fnv_update(seg_fnv_init(), w->block, w->block_len);
    off = w->data_len;
    if (seg_w_bytes(w->st, w->dat_obj, &off, w->block, w->block_len, 0) != 0) return -1;
    seg_wr64(cks, fnv);
    if (seg_w_bytes(w->st, w->dat_obj, &off, cks, 8, 0) != 0) return -1;
    w->data_len = off;
    /* Index temp entry: [first_key_len][first_key][block_len]. */
    seg_wr32(lb, w->block_first_len);
    off = w->idx_len;
    if (seg_w_bytes(w->st, w->idx_obj, &off, lb, 4, 0) != 0) return -1;
    if (seg_w_bytes(w->st, w->idx_obj, &off, w->block_first_key, w->block_first_len, 0) != 0) return -1;
    {
        uint8_t bl[8];
        seg_wr64(bl, w->block_len);
        if (seg_w_bytes(w->st, w->idx_obj, &off, bl, 8, 0) != 0) return -1;
    }
    w->idx_len = off;
    w->block_count++;
    w->block_len = 0;
    return 0;
}

static uint64_t seg_writer_encode(uint8_t* buf, const SegEntry* e) {
    uint64_t off = 0;
    uint8_t flags = e->is_tombstone ? SEG_ENTRY_FLAG_TOMBSTONE : 0;
    seg_wr32(buf + off, e->key_len); off += 4;
    seg_mem_copy(buf + off, e->key, e->key_len); off += e->key_len;
    seg_wr32(buf + off, e->val_len); off += 4;
    if (e->val_len > 0) { seg_mem_copy(buf + off, e->val, e->val_len); off += e->val_len; }
    seg_wr64(buf + off, e->commit_seq); off += 8;
    seg_wr64(buf + off, e->crdt_ts); off += 8;
    buf[off] = flags; off += 1;
    return off;
}

int seg_write_open(YowzaStore* st, const char* obj, uint32_t index_type,
                   uint32_t model_version, int use_bloom, SegWriter* w) {
    uint64_t nlen;
    if (!seg_has_fn(st, 0) || !st->vt->remove) return -1;
    if (!obj || obj[0] == '\0' || !w) return -1;
    if (index_type != SEG_INDEX_TYPE_SPARSE) return -1;
    if (model_version != SEG_MODEL_VERSION_NONE) return -1;
    if (seg_str_len(obj, &nlen) != 0 || nlen > 200) return -1; /* room for .dat/.idx */
    seg_mem_zero((uint8_t*)w, (uint64_t)sizeof(*w));
    w->st = st;
    {
        uint32_t i = 0;
        while (obj[i] != '\0') { w->obj[i] = obj[i]; i++; }
        w->obj[i] = '\0';
    }
    if (seg_writer_name(w->dat_obj, (uint32_t)sizeof(w->dat_obj), obj, ".dat") != 0) return -1;
    if (seg_writer_name(w->idx_obj, (uint32_t)sizeof(w->idx_obj), obj, ".idx") != 0) return -1;
    w->index_type = index_type;
    w->model_version = model_version;
    w->use_bloom = use_bloom ? 1 : 0;
    /* No pre-remove: blocks/temps are written from offset 0 and
     * finish truncates the final object, so a stale temp or final
     * tail is overwritten/ignored (tracked lengths only). Removing
     * here would also trip fault injectors that count removes. */
    w->open = 1;
    return 0;
}

int seg_write_add(SegWriter* w, const SegEntry* e) {
    uint64_t elen;
    if (!w || !w->open || w->finished || !e || !e->key || e->key_len == 0) return -1;
    if (e->key_len > 1024) return -1;
    if (e->is_tombstone) {
        if (e->val_len != 0) return -1;
    } else if (e->val_len > 0 && !e->val) {
        return -1;
    }
    if (w->count > 0 && seg_key_cmp(w->last_added_key, w->last_added_len, e->key, e->key_len) >= 0) return -1;
    elen = seg_entry_len(e);
    if (elen > SEG_BLOCK_SIZE) {
        /* Oversized single entry: its own block, written direct. */
        uint8_t tmp[8192];
        uint64_t fnv, off;
        uint8_t cks[8];
        uint8_t lb[4];
        uint64_t el;
        if (elen > sizeof(tmp)) return -1;
        if (seg_writer_flush_block(w) != 0) return -1;
        el = seg_writer_encode(tmp, e);
        fnv = seg_fnv_update(seg_fnv_init(), tmp, el);
        off = w->data_len;
        if (seg_w_bytes(w->st, w->dat_obj, &off, tmp, el, 0) != 0) return -1;
        seg_wr64(cks, fnv);
        if (seg_w_bytes(w->st, w->dat_obj, &off, cks, 8, 0) != 0) return -1;
        w->data_len = off;
        seg_wr32(lb, e->key_len);
        off = w->idx_len;
        if (seg_w_bytes(w->st, w->idx_obj, &off, lb, 4, 0) != 0) return -1;
        if (seg_w_bytes(w->st, w->idx_obj, &off, e->key, e->key_len, 0) != 0) return -1;
        {
            uint8_t bl[8];
            seg_wr64(bl, (uint64_t)el);
            if (seg_w_bytes(w->st, w->idx_obj, &off, bl, 8, 0) != 0) return -1;
        }
        w->idx_len = off;
        w->block_count++;
    } else {
        if (w->block_len > 0 && (uint64_t)w->block_len + elen > SEG_BLOCK_SIZE) {
            if (seg_writer_flush_block(w) != 0) return -1;
        }
        if (w->block_len == 0) {
            seg_mem_copy(w->block_first_key, e->key, e->key_len);
            w->block_first_len = e->key_len;
        }
        w->block_len += (uint32_t)seg_writer_encode(w->block + w->block_len, e);
    }
    if (w->use_bloom) {
        uint64_t h1 = seg_fnv_update(seg_fnv_init(), e->key, e->key_len);
        uint64_t h2 = seg_splitmix64(h1 + 0x9e3779b97f4a7c15ULL);
        int k;
        for (k = 0; k < 2; k++) {
            uint64_t p = (h1 + (uint64_t)k * h2) % 65536ULL;
            w->bloom[p / 8] |= (uint8_t)(1u << (p % 8));
        }
    }
    if (w->count == 0) {
        seg_mem_copy(w->first_key, e->key, e->key_len);
        w->first_len = e->key_len;
        w->min_seq = e->commit_seq;
        w->max_seq = e->commit_seq;
    } else {
        if (e->commit_seq < w->min_seq) w->min_seq = e->commit_seq;
        if (e->commit_seq > w->max_seq) w->max_seq = e->commit_seq;
    }
    seg_mem_copy(w->last_key, e->key, e->key_len);
    w->last_len = e->key_len;
    seg_mem_copy(w->last_added_key, e->key, e->key_len);
    w->last_added_len = e->key_len;
    w->count++;
    return 0;
}

void seg_write_abort(SegWriter* w) {
    if (!w || !w->open) return;
    if (w->st && w->st->vt && w->st->vt->remove) {
        (void)w->st->vt->remove(w->st->state, w->dat_obj);
        (void)w->st->vt->remove(w->st->state, w->idx_obj);
    }
    w->open = 0;
    w->finished = 1;
}

int seg_write_finish(SegWriter* w, uint64_t* out_bytes) {
    uint8_t header[SEG_HEADER_SIZE];
    uint8_t footer[SEG_FOOTER_SIZE];
    uint64_t header_len, off = 0, header_fnv, index_fnv, blk_off, idx_pos = 0;
    uint32_t flags = 0;
    if (!w || !w->open || w->finished) return -1;
    if (seg_writer_flush_block(w) != 0) { seg_write_abort(w); return -1; }
    if (w->use_bloom) flags |= SEG_FLAG_BLOOM;
    header_len = (uint64_t)SEG_HEADER_SIZE + w->first_len + w->last_len;
    seg_mem_zero(header, SEG_HEADER_SIZE);
    header[0] = 'Y'; header[1] = 'W'; header[2] = 'S'; header[3] = 'E';
    header[4] = 'G'; header[5] = '1';
    seg_wr32(&header[6], SEG_FORMAT_VERSION);
    seg_wr32(&header[10], SEG_BLOCK_SIZE);
    seg_wr32(&header[14], w->count);
    seg_wr32(&header[18], w->index_type);
    seg_wr32(&header[22], w->model_version);
    seg_wr64(&header[26], w->min_seq);
    seg_wr64(&header[34], w->max_seq);
    seg_wr32(&header[42], w->first_len);
    seg_wr32(&header[46], w->last_len);
    seg_wr32(&header[50], flags);
    header_fnv = seg_fnv_init();
    if (seg_w_bytes(w->st, w->obj, &off, header, SEG_HEADER_SIZE, &header_fnv) != 0) goto fail;
    if (w->count > 0) {
        if (seg_w_bytes(w->st, w->obj, &off, w->first_key, w->first_len, &header_fnv) != 0) goto fail;
        if (seg_w_bytes(w->st, w->obj, &off, w->last_key, w->last_len, &header_fnv) != 0) goto fail;
    }
    /* Copy data temp verbatim (blocks + their checksums, in order). */
    {
        uint64_t done = 0;
        uint8_t chunk[SEG_READER_MAX_CHUNK];
        while (done < w->data_len) {
            uint64_t n = w->data_len - done;
            if (n > SEG_READER_MAX_CHUNK) n = SEG_READER_MAX_CHUNK;
            if (!seg_io_ok(w->st->vt->read_at(w->st->state, w->dat_obj, done, chunk, n), n)) goto fail;
            if (seg_w_bytes(w->st, w->obj, &off, chunk, n, 0) != 0) goto fail;
            done += n;
        }
    }
    /* Index: re-emit temp entries with final block offsets. */
    index_fnv = seg_fnv_init();
    blk_off = header_len;
    while (idx_pos < w->idx_len) {
        uint8_t lb[4];
        uint32_t fk_len;
        uint8_t fk[1024];
        uint8_t bl[8];
        uint64_t blen;
        if (!seg_io_ok(w->st->vt->read_at(w->st->state, w->idx_obj, idx_pos, lb, 4), 4)) goto fail;
        fk_len = seg_rd32(lb);
        if (fk_len > 1024) goto fail;
        if (!seg_io_ok(w->st->vt->read_at(w->st->state, w->idx_obj, idx_pos + 4, fk, fk_len), fk_len)) goto fail;
        if (!seg_io_ok(w->st->vt->read_at(w->st->state, w->idx_obj, idx_pos + 4 + fk_len, bl, 8), 8)) goto fail;
        blen = seg_rd64(bl);
        if (seg_w_u32(w->st, w->obj, &off, fk_len, &index_fnv) != 0) goto fail;
        if (seg_w_bytes(w->st, w->obj, &off, fk, fk_len, &index_fnv) != 0) goto fail;
        if (seg_w_u64(w->st, w->obj, &off, blk_off, &index_fnv) != 0) goto fail;
        if (seg_w_u64(w->st, w->obj, &off, blen, &index_fnv) != 0) goto fail;
        blk_off += blen + 8;
        idx_pos += 4 + fk_len + 8;
    }
    if (seg_w_u64(w->st, w->obj, &off, index_fnv, 0) != 0) goto fail;
    if (w->use_bloom) {
        uint64_t bloom_fnv = seg_fnv_update(seg_fnv_init(), w->bloom, 8192);
        if (seg_w_u32(w->st, w->obj, &off, 8192, 0) != 0) goto fail;
        if (seg_w_bytes(w->st, w->obj, &off, w->bloom, 8192, 0) != 0) goto fail;
        if (seg_w_u64(w->st, w->obj, &off, bloom_fnv, 0) != 0) goto fail;
    }
    seg_mem_zero(footer, SEG_FOOTER_SIZE);
    footer[0] = 'Y'; footer[1] = 'W'; footer[2] = 'S'; footer[3] = 'E';
    footer[4] = 'G'; footer[5] = '$';
    seg_wr64(&footer[6], off + SEG_FOOTER_SIZE);
    seg_wr64(&footer[14], header_fnv);
    seg_wr64(&footer[22], index_fnv);
    if (seg_w_bytes(w->st, w->obj, &off, footer, SEG_FOOTER_SIZE, 0) != 0) goto fail;
    if (w->st->vt->truncate(w->st->state, w->obj, off) != 0) goto fail;
    /* Temps are left as orphans on success: they are never
     * referenced by any manifest, and the next writer for the same
     * object overwrites them from offset 0. Removing them here
     * would consume fault-injection removes before publish in
     * crash tests (and is not needed for correctness). */
    w->finished = 1;
    w->open = 0;
    if (out_bytes) *out_bytes = off;
    return 0;
fail:
    seg_write_abort(w);
    return -1;
}

/* ---------------- reader helpers ---------------- */

/* Read exactly len bytes at offset, in chunks of at most
 * SEG_READER_MAX_CHUNK. Short reads are errors. */
static int seg_r_exact(YowzaStore* st, const char* obj, uint64_t off,
                       uint8_t* buf, uint64_t len) {
    uint64_t done = 0;
    while (done < len) {
        uint64_t chunk = len - done;
        int rc;
        if (chunk > SEG_READER_MAX_CHUNK) chunk = SEG_READER_MAX_CHUNK;
        rc = st->vt->read_at(st->state, obj, off + done, buf + done, chunk);
        if (!seg_io_ok(rc, chunk)) return -1;
        done += chunk;
    }
    return 0;
}

static int seg_r_u32(YowzaStore* st, const char* obj, uint64_t off,
                     uint32_t* out) {
    uint8_t b[4];
    if (seg_r_exact(st, obj, off, b, 4) != 0) return -1;
    *out = seg_rd32(b);
    return 0;
}

static int seg_r_u64(YowzaStore* st, const char* obj, uint64_t off,
                     uint64_t* out) {
    uint8_t b[8];
    if (seg_r_exact(st, obj, off, b, 8) != 0) return -1;
    *out = seg_rd64(b);
    return 0;
}

/* Fold a stored region into an FNV state (chunked, no big buffer). */
static int seg_r_fnv(YowzaStore* st, const char* obj, uint64_t off,
                     uint64_t len, uint64_t* fnv) {
    uint8_t scratch[SEG_READER_MAX_CHUNK];
    uint64_t done = 0;
    while (done < len) {
        uint64_t chunk = len - done;
        if (chunk > SEG_READER_MAX_CHUNK) chunk = SEG_READER_MAX_CHUNK;
        if (seg_r_exact(st, obj, off + done, scratch, chunk) != 0) return -1;
        *fnv = seg_fnv_update(*fnv, scratch, chunk);
        done += chunk;
    }
    return 0;
}

/* Copy a stored region into a caller buffer (chunked). */
static int seg_r_copy(YowzaStore* st, const char* obj, uint64_t off,
                      uint8_t* dst, uint64_t len) {
    return seg_r_exact(st, obj, off, dst, len);
}

/* Compare a stored region against an in-memory key: -1/0/1. */
static int seg_r_cmp_key(YowzaStore* st, const char* obj, uint64_t off,
                         uint64_t region_len, const uint8_t* key,
                         uint32_t key_len, int* out) {
    uint8_t scratch[SEG_READER_MAX_CHUNK];
    uint64_t pos = 0;
    while (pos < region_len && pos < key_len) {
        uint64_t chunk = region_len - pos;
        uint64_t kleft = (uint64_t)key_len - pos;
        uint64_t i;
        if (chunk > kleft) chunk = kleft;
        if (chunk > SEG_READER_MAX_CHUNK) chunk = SEG_READER_MAX_CHUNK;
        if (seg_r_exact(st, obj, off + pos, scratch, chunk) != 0) return -1;
        for (i = 0; i < chunk; i++) {
            if (scratch[i] != key[pos + i]) {
                *out = scratch[i] < key[pos + i] ? -1 : 1;
                return 0;
            }
        }
        pos += chunk;
    }
    if (pos == region_len && pos == key_len) *out = 0;
    else if (pos == region_len) *out = -1;
    else *out = 1;
    return 0;
}

/* Compare two stored regions: -1/0/1. */
static int seg_r_cmp_region(YowzaStore* st, const char* obj, uint64_t off1,
                            uint64_t len1, uint64_t off2, uint64_t len2,
                            int* out) {
    uint8_t s1[SEG_READER_MAX_CHUNK], s2[SEG_READER_MAX_CHUNK];
    uint64_t pos = 0;
    while (pos < len1 && pos < len2) {
        uint64_t chunk = len1 - pos;
        uint64_t l2 = len2 - pos;
        uint64_t i;
        if (chunk > l2) chunk = l2;
        if (chunk > SEG_READER_MAX_CHUNK) chunk = SEG_READER_MAX_CHUNK;
        if (seg_r_exact(st, obj, off1 + pos, s1, chunk) != 0) return -1;
        if (seg_r_exact(st, obj, off2 + pos, s2, chunk) != 0) return -1;
        for (i = 0; i < chunk; i++) {
            if (s1[i] != s2[i]) {
                *out = s1[i] < s2[i] ? -1 : 1;
                return 0;
            }
        }
        pos += chunk;
    }
    if (pos == len1 && pos == len2) *out = 0;
    else if (pos == len1) *out = -1;
    else *out = 1;
    return 0;
}

int seg_open(YowzaStore* st, const char* obj, SegReader* r) {
    uint8_t header[SEG_HEADER_SIZE];
    uint8_t footer[SEG_FOOTER_SIZE];
    uint64_t name_len, total_len, header_fnv;
    uint32_t version, block_size, first_len, last_len, flags;
    uint32_t entry_count, index_type, model_version;
    uint64_t min_seq, max_seq;

    if (!seg_has_fn(st, 1) || !seg_has_fn(st, 2)) return -1;
    if (!obj || !r) return -1;
    if (seg_str_len(obj, &name_len) != 0) return -1;
    seg_mem_zero((uint8_t*)r, (uint64_t)sizeof(*r));
    if (st->vt->size(st->state, obj, &total_len) != 0) return -1;
    if (total_len < (uint64_t)SEG_HEADER_SIZE + SEG_FOOTER_SIZE) return -1;
    if (seg_r_exact(st, obj, 0, header, SEG_HEADER_SIZE) != 0) return -1;
    if (header[0] != 'Y' || header[1] != 'W' || header[2] != 'S' ||
        header[3] != 'E' || header[4] != 'G' || header[5] != '1')
        return -1;
    version = seg_rd32(&header[6]);
    block_size = seg_rd32(&header[10]);
    entry_count = seg_rd32(&header[14]);
    index_type = seg_rd32(&header[18]);
    model_version = seg_rd32(&header[22]);
    min_seq = seg_rd64(&header[26]);
    max_seq = seg_rd64(&header[34]);
    first_len = seg_rd32(&header[42]);
    last_len = seg_rd32(&header[46]);
    flags = seg_rd32(&header[50]);
    if (version != SEG_FORMAT_VERSION) return -1;
    if (block_size != SEG_BLOCK_SIZE) return -1;
    if (index_type != SEG_INDEX_TYPE_SPARSE) return -1;
    if (model_version != SEG_MODEL_VERSION_NONE) return -1;
    if (flags & ~SEG_FLAG_BLOOM) return -1;
    if (entry_count == 0) {
        if (first_len != 0 || last_len != 0) return -1;
        if (min_seq != 0 || max_seq != 0) return -1;
    } else {
        if (first_len == 0 || last_len == 0) return -1;
        if (min_seq > max_seq) return -1;
    }
    r->header_len = (uint64_t)SEG_HEADER_SIZE + first_len + last_len;
    if (r->header_len > total_len - SEG_FOOTER_SIZE) return -1;

    /* Header checksum covers header bytes + first key + last key. */
    header_fnv = seg_fnv_init();
    header_fnv = seg_fnv_update(header_fnv, header, SEG_HEADER_SIZE);
    if (seg_r_fnv(st, obj, SEG_HEADER_SIZE, first_len, &header_fnv) != 0)
        return -1;
    if (seg_r_fnv(st, obj, (uint64_t)SEG_HEADER_SIZE + first_len, last_len,
                  &header_fnv) != 0)
        return -1;

    if (seg_r_exact(st, obj, total_len - SEG_FOOTER_SIZE, footer,
                    SEG_FOOTER_SIZE) != 0)
        return -1;
    if (footer[0] != 'Y' || footer[1] != 'W' || footer[2] != 'S' ||
        footer[3] != 'E' || footer[4] != 'G' || footer[5] != '$')
        return -1;
    if (seg_rd64(&footer[6]) != total_len) return -1;
    if (seg_rd64(&footer[14]) != header_fnv) return -1;

    r->st = st;
    seg_mem_copy((uint8_t*)r->obj, (const uint8_t*)obj, name_len);
    r->obj[name_len] = '\0';
    r->open = 1;
    r->entry_count = entry_count;
    r->index_type = index_type;
    r->model_version = model_version;
    r->flags = flags;
    r->min_seq = min_seq;
    r->max_seq = max_seq;
    r->first_key_len = first_len;
    r->last_key_len = last_len;
    r->total_len = total_len;
    r->footer_header_checksum = header_fnv;
    r->footer_index_checksum = seg_rd64(&footer[22]);
    r->discovered = 0;
    return 0;
}

/* Parse + checksum one block entry at *pos, advancing *pos past it.
 * Returns 0 and fills the out fields, or -1 on corrupt/truncated data.
 * The entry bytes are folded into *block_fnv as they are read. */
static int seg_parse_entry(YowzaStore* st, const char* obj, uint64_t* pos,
                           uint64_t* block_fnv, uint32_t* key_len_out,
                           uint64_t* key_off_out, uint32_t* val_len_out,
                           uint64_t* seq_out, uint64_t* ts_out,
                           uint8_t* flags_out) {
    uint8_t b[8];
    uint32_t klen, vlen;
    uint64_t key_off, val_off, seq, ts;
    uint8_t fl;
    if (seg_r_exact(st, obj, *pos, b, 4) != 0) return -1;
    *block_fnv = seg_fnv_update(*block_fnv, b, 4);
    klen = seg_rd32(b);
    if (klen == 0) return -1;
    key_off = *pos + 4;
    if (seg_r_fnv(st, obj, key_off, klen, block_fnv) != 0) return -1;
    if (seg_r_exact(st, obj, key_off + klen, b, 4) != 0) return -1;
    *block_fnv = seg_fnv_update(*block_fnv, b, 4);
    vlen = seg_rd32(b);
    val_off = key_off + klen + 4;
    if (seg_r_fnv(st, obj, val_off, vlen, block_fnv) != 0) return -1;
    if (seg_r_exact(st, obj, val_off + vlen, b, 8) != 0) return -1;
    *block_fnv = seg_fnv_update(*block_fnv, b, 8);
    seq = seg_rd64(b);
    if (seg_r_exact(st, obj, val_off + vlen + 8, b, 8) != 0) return -1;
    *block_fnv = seg_fnv_update(*block_fnv, b, 8);
    ts = seg_rd64(b);
    if (seg_r_exact(st, obj, val_off + vlen + 16, b, 1) != 0) return -1;
    *block_fnv = seg_fnv_update(*block_fnv, b, 1);
    fl = b[0];
    if (fl & ~SEG_ENTRY_FLAG_TOMBSTONE) return -1;
    if ((fl & SEG_ENTRY_FLAG_TOMBSTONE) && vlen != 0) return -1;
    *pos = val_off + vlen + 17;
    *key_len_out = klen;
    *key_off_out = key_off;
    *val_len_out = vlen;
    *seq_out = seq;
    *ts_out = ts;
    *flags_out = fl;
    return 0;
}

int seg_ensure_discovered(SegReader* r) {
    YowzaStore* st;
    const char* obj;
    uint64_t pos, entries_seen = 0, min_seen = 0, max_seen = 0;
    uint64_t first_entry_key_off = 0, last_entry_key_off = 0;
    uint32_t first_entry_key_len = 0, last_entry_key_len = 0;
    uint32_t blocks = 0;
    uint64_t index_offset, index_len, index_fnv;
    uint32_t idx_count;
    int have_seq = 0;

    if (!r || !r->open || !r->st) return -1;
    if (r->discovered) return 0;
    st = r->st;
    obj = r->obj;
    pos = r->header_len;

    /* Block walk. Block boundaries are disambiguated by the checksum
     * test below: at each inter-entry boundary, if the 8 bytes at pos
     * equal the running block FNV, the block has ended (a mid-block
     * entry start matching a 64-bit checksum by chance is negligible,
     * and any derailment still fails the entry-count / index-checksum /
     * footer-position checks at the end). */
    while (entries_seen < r->entry_count) {
        uint64_t block_fnv = seg_fnv_init();
        uint64_t block_bytes = 0;
        for (;;) {
            uint64_t stored;
            if (block_bytes > 0) {
                if (seg_r_u64(st, obj, pos, &stored) != 0) return -1;
                if (stored == block_fnv) {
                    pos += 8;
                    break; /* block ends */
                }
            }
            if (entries_seen >= r->entry_count) return -1;
            {
                uint32_t klen, vlen;
                uint64_t key_off, seq, ts, entry_start = pos;
                uint8_t fl;
                if (seg_parse_entry(st, obj, &pos, &block_fnv, &klen,
                                    &key_off, &vlen, &seq, &ts,
                                    &fl) != 0)
                    return -1;
                if (!have_seq) {
                    min_seen = seq;
                    max_seen = seq;
                    have_seq = 1;
                } else {
                    if (seq < min_seen) min_seen = seq;
                    if (seq > max_seen) max_seen = seq;
                }
                if (entries_seen == 0) {
                    first_entry_key_off = key_off;
                    first_entry_key_len = klen;
                }
                last_entry_key_off = key_off;
                last_entry_key_len = klen;
                block_bytes += pos - entry_start;
                entries_seen++;
            }
        }
        blocks++;
    }
    /* The final block's checksum is consumed by the boundary test on
     * its last entry's successor check... except the loop above breaks
     * out of the block loop only via the checksum test, which runs
     * before the entries_seen guard. Walk the last block's end: after
     * the final entry, the for(;;) loop iterates once more, runs the
     * checksum test (block_bytes > 0), and breaks. So reaching here
     * means every block, including the last, was checksum-verified.
     * (For entry_count == 0 there are no blocks and pos == header_len.)
     */
    index_offset = pos;

    /* Index walk + checksum. */
    index_fnv = seg_fnv_init();
    idx_count = 0;
    while (idx_count < blocks) {
        uint8_t b[8];
        uint32_t klen;
        uint64_t key_off;
        if (seg_r_u32(st, obj, pos, &klen) != 0) return -1;
        if (klen == 0) return -1;
        if (seg_r_exact(st, obj, pos, b, 4) != 0) return -1;
        index_fnv = seg_fnv_update(index_fnv, b, 4);
        pos += 4;
        key_off = pos;
        if (seg_r_fnv(st, obj, key_off, klen, &index_fnv) != 0) return -1;
        pos += klen;
        if (seg_r_exact(st, obj, pos, b, 8) != 0) return -1;
        index_fnv = seg_fnv_update(index_fnv, b, 8);
        pos += 8;
        if (seg_r_exact(st, obj, pos, b, 8) != 0) return -1;
        index_fnv = seg_fnv_update(index_fnv, b, 8);
        pos += 8;
        idx_count++;
    }
    index_len = pos - index_offset;
    {
        uint64_t stored;
        if (seg_r_u64(st, obj, pos, &stored) != 0) return -1;
        if (stored != index_fnv) return -1;
        if (stored != r->footer_index_checksum) return -1;
        pos += 8;
    }

    /* Bloom walk + checksum (if present). */
    r->bloom_offset = 0;
    r->bloom_bytes = 0;
    if (r->flags & SEG_FLAG_BLOOM) {
        uint32_t filter_bytes;
        uint64_t bloom_fnv, stored;
        r->bloom_offset = pos;
        if (seg_r_u32(st, obj, pos, &filter_bytes) != 0) return -1;
        pos += 4;
        bloom_fnv = seg_fnv_init();
        if (seg_r_fnv(st, obj, pos, filter_bytes, &bloom_fnv) != 0)
            return -1;
        pos += filter_bytes;
        if (seg_r_u64(st, obj, pos, &stored) != 0) return -1;
        if (stored != bloom_fnv) return -1;
        pos += 8;
        r->bloom_bytes = filter_bytes;
    }

    /* Everything must end exactly at the footer. */
    if (pos != r->total_len - SEG_FOOTER_SIZE) return -1;
    /* Sequence range must match the header. */
    if (r->entry_count > 0) {
        if (!have_seq || min_seen != r->min_seq || max_seen != r->max_seq)
            return -1;
        /* First/last key copies must match the actual entries. */
        if (first_entry_key_len != r->first_key_len) return -1;
        if (last_entry_key_len != r->last_key_len) return -1;
        {
            int cmp = 0;
            if (seg_r_cmp_region(st, obj, first_entry_key_off,
                                 first_entry_key_len, SEG_HEADER_SIZE,
                                 r->first_key_len, &cmp) != 0)
                return -1;
            if (cmp != 0) return -1;
            if (seg_r_cmp_region(st, obj, last_entry_key_off,
                                 last_entry_key_len,
                                 (uint64_t)SEG_HEADER_SIZE + r->first_key_len,
                                 r->last_key_len, &cmp) != 0)
                return -1;
            if (cmp != 0) return -1;
        }
    }

    r->index_offset = index_offset;
    r->index_len = index_len;
    r->block_count = blocks;
    r->discovered = 1;
    r->iter_index_pos = index_offset;
    r->iter_index_end = index_offset + index_len;
    r->iter_in_block = 0;
    r->iter_returned = 0;
    return 0;
}

/* Axis C lookup-cost counter (see segment.h). */
static uint64_t g_seg_lookup_idx_entries;
/* Track C paged-path page counter (see segment.h). */
static uint64_t g_seg_lookup_paged_pages;

void seg_lookup_index_entries_reset(void) {
    g_seg_lookup_idx_entries = 0;
    g_seg_lookup_paged_pages = 0;
}

uint64_t seg_lookup_index_entries(void) {
    return g_seg_lookup_idx_entries;
}

uint64_t seg_lookup_paged_pages(void) {
    return g_seg_lookup_paged_pages;
}

int seg_lookup(SegReader* r, const uint8_t* key, uint32_t key_len,
               SegEntry* out, uint8_t* val_buf, uint32_t val_buf_cap) {
    YowzaStore* st;
    const char* obj;
    int cmp = 0;
    uint64_t cand_off = 0, cand_len = 0;
    int have_cand = 0;

    if (!r || !r->open || !r->st || !key || key_len == 0 || !out)
        return -1;
    if (seg_ensure_discovered(r) != 0) return -1;
    st = r->st;
    obj = r->obj;
    if (r->entry_count == 0) return 1;

    /* Range check against the (discovery-verified) header key copies. */
    if (seg_r_cmp_key(st, obj, SEG_HEADER_SIZE, r->first_key_len, key,
                      key_len, &cmp) != 0)
        return -1;
    if (cmp > 0) return 1; /* target < first key */
    if (seg_r_cmp_key(st, obj, (uint64_t)SEG_HEADER_SIZE + r->first_key_len,
                      r->last_key_len, key, key_len, &cmp) != 0)
        return -1;
    if (cmp < 0) return 1; /* target > last key */

    /* Bloom pre-check (only trusted because discovery verified its
     * checksum; filters larger than the stack buffer are skipped). */
    if ((r->flags & SEG_FLAG_BLOOM) && r->bloom_bytes > 0 &&
        r->bloom_bytes <= 8192) {
        uint8_t bloom[8192];
        uint64_t h1, h2, nbits;
        int k, present = 1;
        if (seg_r_copy(st, obj, r->bloom_offset + 4, bloom,
                       r->bloom_bytes) != 0)
            return -1;
        nbits = (uint64_t)r->bloom_bytes * 8;
        h1 = seg_fnv_update(seg_fnv_init(), key, key_len);
        h2 = seg_splitmix64(h1 + 0x9e3779b97f4a7c15ULL);
        for (k = 0; k < 2; k++) {
            uint64_t p = (h1 + (uint64_t)k * h2) % nbits;
            if ((bloom[p / 8] & (uint8_t)(1u << (p % 8))) == 0) {
                present = 0;
                break;
            }
        }
        if (!present) return 1;
    }

    /* Candidate block: last block whose first key <= target.
     * With a caller-provided index view (idx_offs_count ==
     * block_count) this is a binary search over the view;
     * with a paged view it is a page binary search plus a
     * within-page scan; otherwise the linear sparse-index
     * scan below runs. Either way the candidate is only a
     * navigation hint: its block checksum is verified below
     * before any entry is trusted. A view search that yields
     * no candidate falls through to the linear scan rather
     * than reporting absent, so a torn view can never cause
     * a false negative. */
    if (r->idx_offs && r->idx_offs_count == r->block_count &&
        r->block_count > 0) {
        uint32_t lo = 0, hi = r->block_count;
        while (lo < hi) {
            uint32_t mid = lo + (hi - lo) / 2;
            uint32_t klen;
            uint64_t epos = r->idx_offs[mid];
            uint64_t boff, blen;
            int c = 0;
            if (seg_r_u32(st, obj, epos, &klen) != 0) return -1;
            if (klen == 0) return -1;
            if (seg_r_cmp_key(st, obj, epos + 4, klen, key, key_len,
                              &c) != 0)
                return -1;
            g_seg_lookup_idx_entries++;
            if (c > 0) { hi = mid; continue; }
            if (seg_r_u64(st, obj, epos + 4 + klen, &boff) != 0) return -1;
            if (seg_r_u64(st, obj, epos + 4 + klen + 8, &blen) != 0) return -1;
            cand_off = boff;
            cand_len = blen;
            have_cand = 1;
            if (c == 0) break; /* target is this block's first key */
            lo = mid + 1;
        }
    }
    /* Axis C — paged binary search (the >256-block path).
     * Page first keys are strictly increasing (blocks
     * partition a sorted entry stream), so binary search
     * over pages finds the last page whose first key is
     * at most the target; the within-page scan then finds
     * the last block in that page with first key at most
     * the target. Cost: O(log pages) probes + at most
     * page_size within-page entries. */
    if (!have_cand && r->idx_page_offs && r->idx_page_size > 0 &&
        r->block_count > 0 &&
        r->idx_page_count == (r->block_count + r->idx_page_size - 1) / r->idx_page_size) {
        uint32_t lo = 0, hi = r->idx_page_count;
        int best_page = -1;
        while (lo < hi) {
            uint32_t mid = lo + (hi - lo) / 2;
            uint32_t klen;
            uint64_t epos = r->idx_page_offs[mid];
            int c = 0;
            if (seg_r_u32(st, obj, epos, &klen) != 0) return -1;
            if (klen == 0) return -1;
            if (seg_r_cmp_key(st, obj, epos + 4, klen, key, key_len,
                              &c) != 0)
                return -1;
            g_seg_lookup_idx_entries++;
            g_seg_lookup_paged_pages++; /* one page probed */
            if (c > 0) { hi = mid; continue; }
            best_page = (int)mid;
            lo = mid + 1;
        }
        if (best_page >= 0) {
            g_seg_lookup_paged_pages++; /* the within-page scan page */
            uint64_t epos = r->idx_page_offs[best_page];
            uint64_t end = (uint32_t)(best_page + 1) < r->idx_page_count
                               ? r->idx_page_offs[best_page + 1]
                               : r->index_offset + r->index_len;
            while (epos < end) {
                uint32_t klen;
                uint64_t key_off, boff, blen;
                int c = 0;
                if (seg_r_u32(st, obj, epos, &klen) != 0) return -1;
                if (klen == 0) return -1;
                key_off = epos + 4;
                if (seg_r_cmp_key(st, obj, key_off, klen, key, key_len,
                                  &c) != 0)
                    return -1;
                g_seg_lookup_idx_entries++;
                if (seg_r_u64(st, obj, key_off + klen, &boff) != 0) return -1;
                if (seg_r_u64(st, obj, key_off + klen + 8, &blen) != 0) return -1;
                if (c > 0) break; /* index key > target: candidate stands */
                cand_off = boff;
                cand_len = blen;
                have_cand = 1;
                if (c == 0) break; /* target is this block's first key */
                epos = key_off + klen + 16;
            }
        }
    }
    /* Linear sparse-index scan: runs when there is no view, or
     * when the view search yielded no candidate. */
    if (!have_cand) {
        uint64_t epos = r->index_offset;
        uint64_t end = r->index_offset + r->index_len;
        while (epos < end) {
            uint32_t klen;
            uint64_t key_off, boff, blen;
            int c = 0;
            if (seg_r_u32(st, obj, epos, &klen) != 0) return -1;
            if (klen == 0) return -1;
            key_off = epos + 4;
            if (seg_r_cmp_key(st, obj, key_off, klen, key, key_len,
                              &c) != 0)
                return -1;
            g_seg_lookup_idx_entries++;
            if (seg_r_u64(st, obj, key_off + klen, &boff) != 0) return -1;
            if (seg_r_u64(st, obj, key_off + klen + 8, &blen) != 0)
                return -1;
            if (c > 0) break; /* index key > target: candidate stands */
            cand_off = boff;
            cand_len = blen;
            have_cand = 1;
            if (c == 0) break; /* target is this block's first key */
            epos = key_off + klen + 16;
        }
    }
    if (!have_cand) return 1;

    /* Verify the candidate block before trusting any entry in it. */
    {
        uint64_t fnv = seg_fnv_init();
        uint64_t stored;
        if (seg_r_fnv(st, obj, cand_off, cand_len, &fnv) != 0) return -1;
        if (seg_r_u64(st, obj, cand_off + cand_len, &stored) != 0) return -1;
        if (stored != fnv) return -1;
    }

    /* Stream the block's entries. */
    {
        uint64_t epos = cand_off;
        uint64_t end = cand_off + cand_len;
        while (epos < end) {
            uint32_t klen, vlen;
            uint64_t key_off, val_off, seq, ts;
            uint8_t b[8], fl;
            int c = 0;
            if (seg_r_u32(st, obj, epos, &klen) != 0) return -1;
            if (klen == 0) return -1;
            key_off = epos + 4;
            if (seg_r_cmp_key(st, obj, key_off, klen, key, key_len,
                              &c) != 0)
                return -1;
            if (seg_r_u32(st, obj, key_off + klen, &vlen) != 0) return -1;
            val_off = key_off + klen + 4;
            if (seg_r_exact(st, obj, val_off + vlen, b, 8) != 0) return -1;
            seq = seg_rd64(b);
            if (seg_r_exact(st, obj, val_off + vlen + 8, b, 8) != 0)
                return -1;
            ts = seg_rd64(b);
            if (seg_r_exact(st, obj, val_off + vlen + 16, b, 1) != 0)
                return -1;
            fl = b[0];
            if (fl & ~SEG_ENTRY_FLAG_TOMBSTONE) return -1;
            if ((fl & SEG_ENTRY_FLAG_TOMBSTONE) && vlen != 0) return -1;
            if (c == 0) {
                out->key = key;
                out->key_len = key_len;
                out->is_tombstone = (fl & SEG_ENTRY_FLAG_TOMBSTONE) ? 1 : 0;
                out->commit_seq = seq;
                out->crdt_ts = ts;
                if (out->is_tombstone) {
                    out->val = 0;
                    out->val_len = 0;
                } else {
                    if (vlen > val_buf_cap) return -1;
                    if (!val_buf && vlen > 0) return -1;
                    if (seg_r_copy(st, obj, val_off, val_buf, vlen) != 0)
                        return -1;
                    out->val = val_buf;
                    out->val_len = vlen;
                }
                return 0;
            }
            if (c > 0) return 1; /* passed the target: absent */
            epos = val_off + vlen + 17;
        }
    }
    return 1;
}

int seg_iter_next(SegReader* r, SegEntry* out, uint8_t* key_buf,
                  uint32_t key_cap, uint8_t* val_buf, uint32_t val_cap) {
    YowzaStore* st;
    const char* obj;

    if (!r || !r->open || !r->st || !out || !key_buf) return -1;
    if (seg_ensure_discovered(r) != 0) return -1;
    st = r->st;
    obj = r->obj;
    if (r->iter_returned >= r->entry_count) return 1;

    for (;;) {
        /* Advance to the next block if needed. */
        while (!r->iter_in_block) {
            uint32_t klen;
            uint64_t boff, blen;
            if (r->iter_index_pos >= r->iter_index_end) return -1;
            if (seg_r_u32(st, obj, r->iter_index_pos, &klen) != 0)
                return -1;
            if (klen == 0) return -1;
            if (seg_r_u64(st, obj, r->iter_index_pos + 4 + klen,
                          &boff) != 0)
                return -1;
            if (seg_r_u64(st, obj, r->iter_index_pos + 4 + klen + 8,
                          &blen) != 0)
                return -1;
            r->iter_index_pos += 4 + klen + 16;
            r->iter_block_offset = boff;
            r->iter_block_len = blen;
            r->iter_entry_pos = boff;
            r->iter_block_end = boff + blen;
            /* Verify the block BEFORE returning any of its entries. */
            {
                uint64_t fnv = seg_fnv_init();
                uint64_t stored;
                if (seg_r_fnv(st, obj, boff, blen, &fnv) != 0) return -1;
                if (seg_r_u64(st, obj, boff + blen, &stored) != 0)
                    return -1;
                if (stored != fnv) return -1;
            }
            if (r->iter_entry_pos < r->iter_block_end) {
                r->iter_in_block = 1;
            } else {
                /* Degenerate zero-length block: skip it. */
                r->iter_in_block = 0;
            }
        }

        /* Parse one entry from the current block. */
        {
            uint32_t klen, vlen;
            uint64_t key_off, val_off, seq, ts;
            uint8_t b[8], fl;
            if (seg_r_u32(st, obj, r->iter_entry_pos, &klen) != 0)
                return -1;
            if (klen == 0) return -1;
            if (klen > key_cap) return -1;
            key_off = r->iter_entry_pos + 4;
            if (seg_r_copy(st, obj, key_off, key_buf, klen) != 0)
                return -1;
            if (seg_r_u32(st, obj, key_off + klen, &vlen) != 0) return -1;
            val_off = key_off + klen + 4;
            if (seg_r_exact(st, obj, val_off + vlen, b, 8) != 0) return -1;
            seq = seg_rd64(b);
            if (seg_r_exact(st, obj, val_off + vlen + 8, b, 8) != 0)
                return -1;
            ts = seg_rd64(b);
            if (seg_r_exact(st, obj, val_off + vlen + 16, b, 1) != 0)
                return -1;
            fl = b[0];
            if (fl & ~SEG_ENTRY_FLAG_TOMBSTONE) return -1;
            if ((fl & SEG_ENTRY_FLAG_TOMBSTONE) && vlen != 0) return -1;
            out->key = key_buf;
            out->key_len = klen;
            out->is_tombstone = (fl & SEG_ENTRY_FLAG_TOMBSTONE) ? 1 : 0;
            out->commit_seq = seq;
            out->crdt_ts = ts;
            if (out->is_tombstone) {
                out->val = 0;
                out->val_len = 0;
            } else {
                if (vlen > val_cap) return -1;
                if (!val_buf && vlen > 0) return -1;
                if (seg_r_copy(st, obj, val_off, val_buf, vlen) != 0)
                    return -1;
                out->val = val_buf;
                out->val_len = vlen;
            }
            r->iter_entry_pos = val_off + vlen + 17;
            if (r->iter_entry_pos >= r->iter_block_end)
                r->iter_in_block = 0;
            r->iter_returned++;
            return 0;
        }
    }
}

void seg_close(SegReader* r) {
    if (!r) return;
    r->open = 0;
    r->st = 0;
    /* The index views are caller-owned; dropping the borrows
     * here (the buffers themselves are untouched) so a closed
     * reader can never be mistaken for one with a live view. */
    r->idx_offs = 0;
    r->idx_offs_count = 0;
    r->idx_page_offs = 0;
    r->idx_page_count = 0;
    r->idx_page_size = 0;
}

/* Build the binary-searchable index view (see segment.h). The
 * reader must already be discovered: this walk re-reads only
 * the index region and checks it against discovery's results
 * (entry count and total length), so any inconsistency fails
 * here and the caller degrades to the linear scan. */
int seg_index_view(SegReader* r, uint64_t* offs, uint32_t cap) {
    YowzaStore* st;
    const char* obj;
    uint64_t pos, end;
    uint32_t i;
    if (!r || !r->open || !r->st || !r->discovered || !offs) return -1;
    if (r->block_count > cap) return -1;
    st = r->st;
    obj = r->obj;
    pos = r->index_offset;
    end = r->index_offset + r->index_len;
    for (i = 0; i < r->block_count; i++) {
        uint32_t klen;
        uint64_t step;
        if (pos > end) return -1;
        if (seg_r_u32(st, obj, pos, &klen) != 0) return -1;
        if (klen == 0) return -1;
        offs[i] = pos;
        /* Entry layout: 4-byte key length + key + 8-byte block
         * offset + 8-byte block length. */
        step = 4ULL + (uint64_t)klen + 16ULL;
        if (step > end - pos) return -1;
        pos += step;
    }
    if (pos != end) return -1;
    return (int)r->block_count;
}

/* Build the paged index view (see segment.h). Walks the
 * index region once, recording the offset of every
 * page_size-th entry. */
int seg_index_view_paged(SegReader* r, uint64_t* offs, uint32_t cap, uint32_t page_size) {
    YowzaStore* st;
    const char* obj;
    uint64_t pos, end;
    uint32_t i, pages = 0;
    if (!r || !r->open || !r->st || !r->discovered || !offs) return -1;
    if (page_size == 0) page_size = SEG_INDEX_PAGE_SIZE;
    if ((r->block_count + page_size - 1) / page_size > cap) return -1;
    st = r->st;
    obj = r->obj;
    pos = r->index_offset;
    end = r->index_offset + r->index_len;
    for (i = 0; i < r->block_count; i++) {
        uint32_t klen;
        uint64_t step;
        if (pos > end) return -1;
        if (seg_r_u32(st, obj, pos, &klen) != 0) return -1;
        if (klen == 0) return -1;
        if (i % page_size == 0) {
            if (pages >= cap) return -1;
            offs[pages] = pos;
            pages++;
        }
        step = 4ULL + (uint64_t)klen + 16ULL;
        if (step > end - pos) return -1;
        pos += step;
    }
    if (pos != end) return -1;
    if (pages != (r->block_count + page_size - 1) / page_size) return -1;
    return (int)pages;
}

int seg_file_checksum(YowzaStore* st, const char* obj, uint64_t* out) {
    uint64_t size, fnv;
    if (!seg_has_fn(st, 1) || !seg_has_fn(st, 2)) return -1;
    if (!obj || !out) return -1;
    if (st->vt->size(st->state, obj, &size) != 0) return -1;
    fnv = seg_fnv_init();
    if (seg_r_fnv(st, obj, 0, size, &fnv) != 0) return -1;
    *out = fnv;
    return 0;
}
