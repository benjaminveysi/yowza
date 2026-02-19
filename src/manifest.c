/* Phase 2, Track B — Versioned Manifest format v2. See
 * include/manifest.h for the layout, limits, and publish order.
 * No libc, no malloc. */

#include "manifest.h"

/* ---- small helpers (mirroring segment.c; no shared internal header
 * exists in the Track B file set, so these tiny helpers are duplicated
 * rather than imported from outside the assigned files). ---- */

static uint64_t man_fnv_init(void) { return 14695981039346656037ULL; }

static uint64_t man_fnv_update(uint64_t h, const uint8_t* data,
                               uint64_t len) {
    uint64_t i;
    for (i = 0; i < len; i++) {
        h ^= (uint64_t)data[i];
        h *= 1099511628211ULL;
    }
    return h;
}

static void man_wr32(uint8_t* buf, uint32_t v) {
    buf[0] = (uint8_t)(v & 0xFF);
    buf[1] = (uint8_t)((v >> 8) & 0xFF);
    buf[2] = (uint8_t)((v >> 16) & 0xFF);
    buf[3] = (uint8_t)((v >> 24) & 0xFF);
}

static void man_wr64(uint8_t* buf, uint64_t v) {
    int i;
    for (i = 0; i < 8; i++) buf[i] = (uint8_t)((v >> (i * 8)) & 0xFF);
}

static uint32_t man_rd32(const uint8_t* buf) {
    return (uint32_t)buf[0] | ((uint32_t)buf[1] << 8) |
           ((uint32_t)buf[2] << 16) | ((uint32_t)buf[3] << 24);
}

static uint64_t man_rd64(const uint8_t* buf) {
    uint64_t v = 0;
    int i;
    for (i = 0; i < 8; i++) v |= (uint64_t)buf[i] << (i * 8);
    return v;
}

static void man_mem_zero(uint8_t* dst, uint64_t len) {
    uint64_t i;
    for (i = 0; i < len; i++) dst[i] = 0;
}

/* Length of a NUL-terminated string within a bound; -1 if no NUL. */
static int man_cstr_len(const char* s, uint64_t bound, uint64_t* out) {
    uint64_t n = 0;
    if (!s) return -1;
    while (n < bound) {
        if (s[n] == '\0') {
            *out = n;
            return 0;
        }
        n++;
    }
    return -1;
}

static int man_io_ok(int rc, uint64_t len) {
    if (rc == 0) return 1;
    if (rc > 0 && (uint64_t)rc == len) return 1;
    return 0;
}

static int man_has_fn(YowzaStore* st, int which) {
    /* 0 write_at, 1 read_at, 2 size, 3 truncate */
    if (!st || !st->vt) return 0;
    switch (which) {
        case 0: return st->vt->write_at != 0;
        case 1: return st->vt->read_at != 0;
        case 2: return st->vt->size != 0;
        case 3: return st->vt->truncate != 0;
        default: return 0;
    }
}

/* Streaming writer: write bytes at *off, fold into *fnv. */
static int man_w_bytes(YowzaStore* st, const char* obj, uint64_t* off,
                       const uint8_t* data, uint64_t len, uint64_t* fnv) {
    int rc;
    if (len == 0) return 0;
    if (!data) return -1;
    rc = st->vt->write_at(st->state, obj, *off, data, len);
    if (!man_io_ok(rc, len)) return -1;
    if (fnv) *fnv = man_fnv_update(*fnv, data, len);
    *off += len;
    return 0;
}

static int man_w_u32(YowzaStore* st, const char* obj, uint64_t* off,
                     uint32_t v, uint64_t* fnv) {
    uint8_t b[4];
    man_wr32(b, v);
    return man_w_bytes(st, obj, off, b, 4, fnv);
}

static int man_w_u64(YowzaStore* st, const char* obj, uint64_t* off,
                     uint64_t v, uint64_t* fnv) {
    uint8_t b[8];
    man_wr64(b, v);
    return man_w_bytes(st, obj, off, b, 8, fnv);
}

/* Streaming reader: read exactly len bytes at off (no chunk cap needed
 * here — manifest fields are bounded by the struct limits — but reads
 * are still issued field by field, never as one whole-object read). */
static int man_r_exact(YowzaStore* st, const char* obj, uint64_t off,
                       uint8_t* buf, uint64_t len) {
    uint64_t done = 0;
    while (done < len) {
        uint64_t chunk = len - done;
        int rc;
        if (chunk > 1024) chunk = 1024;
        rc = st->vt->read_at(st->state, obj, off + done, buf + done, chunk);
        if (!man_io_ok(rc, chunk)) return -1;
        done += chunk;
    }
    return 0;
}

/* Read bytes at *off, fold into *fnv, advance *off. */
static int man_r_bytes(YowzaStore* st, const char* obj, uint64_t* off,
                       uint8_t* buf, uint64_t len, uint64_t* fnv) {
    if (man_r_exact(st, obj, *off, buf, len) != 0) return -1;
    if (fnv) *fnv = man_fnv_update(*fnv, buf, len);
    *off += len;
    return 0;
}

static int man_r_u32(YowzaStore* st, const char* obj, uint64_t* off,
                     uint32_t* out, uint64_t* fnv) {
    uint8_t b[4];
    if (man_r_bytes(st, obj, off, b, 4, fnv) != 0) return -1;
    *out = man_rd32(b);
    return 0;
}

static int man_r_u64(YowzaStore* st, const char* obj, uint64_t* off,
                     uint64_t* out, uint64_t* fnv) {
    uint8_t b[8];
    if (man_r_bytes(st, obj, off, b, 8, fnv) != 0) return -1;
    *out = man_rd64(b);
    return 0;
}

static int man_validate_meta(const SegMeta* s, uint64_t* obj_len_out) {
    uint64_t obj_len;
    if (!s) return -1;
    if (man_cstr_len(s->obj, MANIFEST_MAX_OBJ_LEN + 1, &obj_len) != 0)
        return -1;
    if (obj_len == 0) return -1;
    if (s->first_key_len > MANIFEST_MAX_KEY_LEN) return -1;
    if (s->last_key_len > MANIFEST_MAX_KEY_LEN) return -1;
    if (s->index_type != SEG_INDEX_TYPE_SPARSE) return -1;
    if (s->model_version != SEG_MODEL_VERSION_NONE) return -1;
    if (obj_len_out) *obj_len_out = obj_len;
    return 0;
}

int manifest_write(YowzaStore* st, const char* tmp_obj, const Manifest* m) {
    uint32_t i;
    uint64_t off = 0, fnv;
    uint8_t magic[6];

    if (!man_has_fn(st, 0) || !man_has_fn(st, 3)) return -1;
    if (!tmp_obj || tmp_obj[0] == '\0' || !m) return -1;
    if (m->segment_count > MANIFEST_MAX_SEGMENTS) return -1;
    for (i = 0; i < m->segment_count; i++) {
        if (man_validate_meta(&m->segments[i], 0) != 0) return -1;
    }

    fnv = man_fnv_init();
    magic[0] = 'Y'; magic[1] = 'W'; magic[2] = 'M'; magic[3] = 'A';
    magic[4] = 'N'; magic[5] = '2';
    if (man_w_bytes(st, tmp_obj, &off, magic, 6, &fnv) != 0) return -1;
    if (man_w_u32(st, tmp_obj, &off, MANIFEST_FORMAT_VERSION, &fnv) != 0)
        return -1;
    if (man_w_u64(st, tmp_obj, &off, m->generation, &fnv) != 0) return -1;
    if (man_w_u64(st, tmp_obj, &off, m->checkpoint_seq, &fnv) != 0)
        return -1;
    if (man_w_u32(st, tmp_obj, &off, m->schema_version, &fnv) != 0)
        return -1;
    if (man_w_u32(st, tmp_obj, &off, m->segment_count, &fnv) != 0)
        return -1;
    for (i = 0; i < m->segment_count; i++) {
        const SegMeta* s = &m->segments[i];
        uint64_t obj_len = 0;
        if (man_validate_meta(s, &obj_len) != 0) return -1;
        if (man_w_u32(st, tmp_obj, &off, (uint32_t)obj_len, &fnv) != 0)
            return -1;
        if (man_w_bytes(st, tmp_obj, &off, (const uint8_t*)s->obj, obj_len,
                        &fnv) != 0)
            return -1;
        if (man_w_u32(st, tmp_obj, &off, s->first_key_len, &fnv) != 0)
            return -1;
        if (man_w_bytes(st, tmp_obj, &off, s->first_key, s->first_key_len,
                        &fnv) != 0)
            return -1;
        if (man_w_u32(st, tmp_obj, &off, s->last_key_len, &fnv) != 0)
            return -1;
        if (man_w_bytes(st, tmp_obj, &off, s->last_key, s->last_key_len,
                        &fnv) != 0)
            return -1;
        if (man_w_u64(st, tmp_obj, &off, s->min_seq, &fnv) != 0) return -1;
        if (man_w_u64(st, tmp_obj, &off, s->max_seq, &fnv) != 0) return -1;
        if (man_w_u32(st, tmp_obj, &off, s->entry_count, &fnv) != 0)
            return -1;
        if (man_w_u32(st, tmp_obj, &off, s->index_type, &fnv) != 0)
            return -1;
        if (man_w_u32(st, tmp_obj, &off, s->model_version, &fnv) != 0)
            return -1;
        if (man_w_u64(st, tmp_obj, &off, s->byte_size, &fnv) != 0)
            return -1;
        if (man_w_u64(st, tmp_obj, &off, s->checksum, &fnv) != 0)
            return -1;
    }
    if (man_w_u64(st, tmp_obj, &off, fnv, 0) != 0) return -1;
    if (st->vt->truncate(st->state, tmp_obj, off) != 0) return -1;
    return 0;
}

int manifest_publish(YowzaStore* st, const char* tmp_obj,
                     const char* final_obj) {
    if (!st || !st->vt) return -1;
    if (!tmp_obj || tmp_obj[0] == '\0') return -1;
    if (!final_obj || final_obj[0] == '\0') return -1;
    if (!st->vt->sync_obj || !st->vt->publish ||
        !st->vt->sync_dir)
        return -1;
    /* Load-bearing order: sync tmp, atomic rename, sync directory.
     * Return codes (Track A three-state publish): 0 = published and
     * durable; -1 = definite failure before the rename (old state
     * still authoritative); -2 = INDETERMINATE (rename happened,
     * but a durability sync after it failed — the new content may
     * be visible but is not proven durable; the caller must
     * re-check, never silently revert). */
    int prc;
    if (st->vt->sync_obj(st->state, tmp_obj) != 0) return -1;
    prc = st->vt->publish(st->state, tmp_obj, final_obj);
    if (prc == -2) return -2;
    if (prc != 0) return -1;
    if (st->vt->sync_dir(st->state) != 0) return -2;
    return 0;
}

int manifest_read(YowzaStore* st, const char* obj, Manifest* out) {
    uint64_t size, off = 0, fnv;
    uint32_t version, segment_count, i;
    uint8_t magic[6];

    if (!man_has_fn(st, 1) || !man_has_fn(st, 2)) return -1;
    if (!obj || !out) return -1;
    man_mem_zero((uint8_t*)out, (uint64_t)sizeof(*out));
    if (st->vt->size(st->state, obj, &size) != 0) return -1;
    /* Minimum: header 34 + checksum 8. */
    if (size < 42) return -1;
    fnv = man_fnv_init();
    if (man_r_bytes(st, obj, &off, magic, 6, &fnv) != 0) return -1;
    if (magic[0] != 'Y' || magic[1] != 'W' || magic[2] != 'M' ||
        magic[3] != 'A' || magic[4] != 'N' || magic[5] != '2')
        return -1;
    if (man_r_u32(st, obj, &off, &version, &fnv) != 0) return -1;
    if (version != MANIFEST_FORMAT_VERSION) return -1;
    if (man_r_u64(st, obj, &off, &out->generation, &fnv) != 0) return -1;
    if (man_r_u64(st, obj, &off, &out->checkpoint_seq, &fnv) != 0)
        return -1;
    if (man_r_u32(st, obj, &off, &out->schema_version, &fnv) != 0)
        return -1;
    if (man_r_u32(st, obj, &off, &segment_count, &fnv) != 0) return -1;
    if (segment_count > MANIFEST_MAX_SEGMENTS) {
        man_mem_zero((uint8_t*)out, (uint64_t)sizeof(*out));
        return -1;
    }
    out->segment_count = segment_count;
    for (i = 0; i < segment_count; i++) {
        SegMeta* s = &out->segments[i];
        uint32_t obj_len;
        if (man_r_u32(st, obj, &off, &obj_len, &fnv) != 0) goto fail;
        if (obj_len == 0 || obj_len > MANIFEST_MAX_OBJ_LEN) goto fail;
        if (man_r_bytes(st, obj, &off, (uint8_t*)s->obj, obj_len, &fnv) != 0)
            goto fail;
        s->obj[obj_len] = '\0';
        if (man_r_u32(st, obj, &off, &s->first_key_len, &fnv) != 0)
            goto fail;
        if (s->first_key_len > MANIFEST_MAX_KEY_LEN) goto fail;
        if (man_r_bytes(st, obj, &off, s->first_key, s->first_key_len,
                        &fnv) != 0)
            goto fail;
        if (man_r_u32(st, obj, &off, &s->last_key_len, &fnv) != 0)
            goto fail;
        if (s->last_key_len > MANIFEST_MAX_KEY_LEN) goto fail;
        if (man_r_bytes(st, obj, &off, s->last_key, s->last_key_len,
                        &fnv) != 0)
            goto fail;
        if (man_r_u64(st, obj, &off, &s->min_seq, &fnv) != 0) goto fail;
        if (man_r_u64(st, obj, &off, &s->max_seq, &fnv) != 0) goto fail;
        if (man_r_u32(st, obj, &off, &s->entry_count, &fnv) != 0) goto fail;
        if (man_r_u32(st, obj, &off, &s->index_type, &fnv) != 0) goto fail;
        if (s->index_type != SEG_INDEX_TYPE_SPARSE) goto fail;
        if (man_r_u32(st, obj, &off, &s->model_version, &fnv) != 0)
            goto fail;
        if (s->model_version != SEG_MODEL_VERSION_NONE) goto fail;
        if (man_r_u64(st, obj, &off, &s->byte_size, &fnv) != 0) goto fail;
        if (man_r_u64(st, obj, &off, &s->checksum, &fnv) != 0) goto fail;
    }
    /* Exactly 8 bytes must remain: the manifest checksum. */
    if (off + 8 != size) goto fail;
    {
        uint64_t stored;
        if (man_r_u64(st, obj, &off, &stored, 0) != 0) goto fail;
        if (stored != fnv) goto fail;
    }
    if (off != size) goto fail;
    return 0;
fail:
    man_mem_zero((uint8_t*)out, (uint64_t)sizeof(*out));
    return -1;
}

int manifest_write_current(YowzaStore* st, const char* tmp_obj,
                           const char* manifest_obj) {
    uint64_t off = 0, name_len = 0;
    if (!man_has_fn(st, 0) || !man_has_fn(st, 3)) return -1;
    if (!tmp_obj || tmp_obj[0] == '\0') return -1;
    if (!manifest_obj || manifest_obj[0] == '\0') return -1;
    if (man_cstr_len(manifest_obj, MANIFEST_MAX_OBJ_LEN + 1, &name_len) != 0)
        return -1;
    if (name_len == 0) return -1;
    if (man_w_bytes(st, tmp_obj, &off, (const uint8_t*)manifest_obj,
                    name_len, 0) != 0)
        return -1;
    if (st->vt->truncate(st->state, tmp_obj, off) != 0) return -1;
    return 0;
}

int manifest_read_current(YowzaStore* st, const char* current_obj,
                          char* out, uint32_t cap) {
    uint64_t size;
    if (!man_has_fn(st, 1) || !man_has_fn(st, 2)) return -1;
    if (!current_obj || !out || cap == 0) return -1;
    if (st->vt->size(st->state, current_obj, &size) != 0) return -1;
    if (size == 0 || size >= cap) return -1;
    if (man_r_exact(st, current_obj, 0, (uint8_t*)out, size) != 0)
        return -1;
    out[size] = '\0';
    return 0;
}

/* ---- multi-level directory (Track A item 3) ---- */

int manifest_dir_write(YowzaStore* st, const char* tmp_obj, const ManifestDir* d) {
    uint32_t i;
    uint64_t off = 0, fnv;
    uint8_t magic[6];
    if (!man_has_fn(st, 0) || !man_has_fn(st, 3)) return -1;
    if (!tmp_obj || tmp_obj[0] == '\0' || !d) return -1;
    if (d->group_count == 0 || d->group_count > MANIFEST_DIR_MAX_GROUPS) return -1;
    for (i = 0; i < d->group_count; i++) {
        uint64_t glen = 0;
        if (man_cstr_len(d->groups[i], MANIFEST_MAX_OBJ_LEN, &glen) != 0 || glen == 0) return -1;
    }
    fnv = man_fnv_init();
    magic[0] = 'Y'; magic[1] = 'W'; magic[2] = 'M'; magic[3] = 'N';
    magic[4] = 'D'; magic[5] = 'R';
    if (man_w_bytes(st, tmp_obj, &off, magic, 6, &fnv) != 0) return -1;
    if (man_w_u32(st, tmp_obj, &off, MANIFEST_DIR_VERSION, &fnv) != 0) return -1;
    if (man_w_u64(st, tmp_obj, &off, d->generation, &fnv) != 0) return -1;
    if (man_w_u64(st, tmp_obj, &off, d->checkpoint_seq, &fnv) != 0) return -1;
    if (man_w_u32(st, tmp_obj, &off, d->schema_version, &fnv) != 0) return -1;
    if (man_w_u32(st, tmp_obj, &off, d->group_count, &fnv) != 0) return -1;
    for (i = 0; i < d->group_count; i++) {
        uint64_t glen = 0;
        (void)man_cstr_len(d->groups[i], MANIFEST_MAX_OBJ_LEN, &glen);
        if (man_w_u32(st, tmp_obj, &off, (uint32_t)glen, &fnv) != 0) return -1;
        if (man_w_bytes(st, tmp_obj, &off, (const uint8_t*)d->groups[i], glen, &fnv) != 0) return -1;
    }
    if (man_w_u64(st, tmp_obj, &off, fnv, 0) != 0) return -1;
    if (st->vt->truncate(st->state, tmp_obj, off) != 0) return -1;
    return 0;
}

int manifest_dir_read(YowzaStore* st, const char* obj, ManifestDir* out) {
    uint64_t size, off = 0, fnv, stored;
    uint32_t version, i;
    uint8_t magic[6];
    if (!man_has_fn(st, 1) || !man_has_fn(st, 2)) return -1;
    if (!obj || !out) return -1;
    man_mem_zero((uint8_t*)out, (uint64_t)sizeof(*out));
    if (st->vt->size(st->state, obj, &size) != 0) return -1;
    if (size < 42) return -1;
    fnv = man_fnv_init();
    if (man_r_bytes(st, obj, &off, magic, 6, &fnv) != 0) return -1;
    if (magic[0] != 'Y' || magic[1] != 'W' || magic[2] != 'M' ||
        magic[3] != 'N' || magic[4] != 'D' || magic[5] != 'R') return -1;
    if (man_r_u32(st, obj, &off, &version, &fnv) != 0) return -1;
    if (version != MANIFEST_DIR_VERSION) return -1;
    if (man_r_u64(st, obj, &off, &out->generation, &fnv) != 0) return -1;
    if (man_r_u64(st, obj, &off, &out->checkpoint_seq, &fnv) != 0) return -1;
    if (man_r_u32(st, obj, &off, &out->schema_version, &fnv) != 0) return -1;
    if (man_r_u32(st, obj, &off, &out->group_count, &fnv) != 0) return -1;
    if (out->group_count == 0 || out->group_count > MANIFEST_DIR_MAX_GROUPS) goto fail;
    for (i = 0; i < out->group_count; i++) {
        uint32_t glen;
        if (man_r_u32(st, obj, &off, &glen, &fnv) != 0) goto fail;
        if (glen == 0 || glen > MANIFEST_MAX_OBJ_LEN) goto fail;
        if (man_r_bytes(st, obj, &off, (uint8_t*)out->groups[i], glen, &fnv) != 0) goto fail;
        out->groups[i][glen] = '\0';
    }
    if (man_r_u64(st, obj, &off, &stored, 0) != 0) goto fail;
    if (stored != fnv) goto fail;
    if (off != size) goto fail;
    return 0;
fail:
    man_mem_zero((uint8_t*)out, (uint64_t)sizeof(*out));
    return -1;
}

/* ---- Gate 2 lifecycle helpers (2.4b) ---- */

int manifest_current_status(YowzaStore* st, const char* current_obj,
                            char* out, uint32_t cap) {
    uint64_t size = 0;
    int status;
    if (!st || !current_obj || !out || cap == 0) return 2;
    status = yowza_store_size_status(st, current_obj, &size);
    if (status == YOWZA_STORE_NOT_FOUND) return 1;
    if (status != YOWZA_STORE_OK) return 2;
    /* Object exists per the status split: an unreadable,
     * empty, or over-long pointer is corruption, not
     * absence. */
    if (manifest_read_current(st, current_obj, out, cap) != 0) return 2;
    return 0;
}

int manifest_recover_pointer(YowzaStore* st, char* out, uint32_t cap,
                             int* used_fallback) {
    int rc, has_content = 0;
    if (!st || !out || cap == 0) return -2;
    if (used_fallback) *used_fallback = 0;
    rc = manifest_current_status(st, "CURRENT", out, cap);
    if (rc == 0) return 0;
    if (rc == 2) return -2;
    /* CURRENT authoritatively absent: try the backup. */
    rc = manifest_current_status(st, "CURRENT.bak", out, cap);
    if (rc == 0) {
        if (used_fallback) *used_fallback = 1;
        return 0;
    }
    if (rc == 2) return -2;
    /* Both pointers gone. Virgin only if NOTHING remains:
     * enumerate the store; any object means prior content
     * survived its metadata, and an unenunerable backend
     * cannot prove emptiness either. */
    if (yowza_store_detect_prior_content(st, &has_content) != 0) return -2;
    if (has_content) return -2;
    return -1;
}

/* Parse "manifest.<digits>" into *gen_out; -1 unless the
 * name is exactly that shape with gen > 0. */
static int man_parse_manifest_gen(const char* name, uint64_t* gen_out) {
    uint64_t gen = 0;
    uint64_t i = 0;
    if (!name) return -1;
    if (name[0] != 'm' || name[1] != 'a' || name[2] != 'n' ||
        name[3] != 'i' || name[4] != 'f' || name[5] != 'e' ||
        name[6] != 's' || name[7] != 't' || name[8] != '.')
        return -1;
    i = 9;
    if (name[i] == '\0') return -1;
    while (name[i] != '\0') {
        uint64_t d;
        if (name[i] < '0' || name[i] > '9') return -1;
        d = (uint64_t)(name[i] - '0');
        if (gen > (18446744073709551615ULL - d) / 10) return -1;
        gen = gen * 10 + d;
        i++;
    }
    if (gen == 0) return -1;
    *gen_out = gen;
    return 0;
}

/* Format "manifest.<gen>" into out (cap must hold it);
 * -1 when it does not fit. */
static int man_format_manifest_name(char* out, uint32_t cap, uint64_t gen) {
    char digits[24];
    uint32_t nd = 0, i;
    uint32_t need;
    if (!out || cap == 0) return -1;
    if (gen == 0) return -1;
    while (gen > 0) {
        digits[nd++] = (char)('0' + (gen % 10));
        gen /= 10;
    }
    need = 9 + nd + 1; /* "manifest." + digits + NUL */
    if (need > cap) return -1;
    out[0] = 'm'; out[1] = 'a'; out[2] = 'n'; out[3] = 'i';
    out[4] = 'f'; out[5] = 'e'; out[6] = 's'; out[7] = 't';
    out[8] = '.';
    for (i = 0; i < nd; i++) out[9 + i] = digits[nd - 1 - i];
    out[9 + nd] = '\0';
    return 0;
}

int manifest_recover_manifest(YowzaStore* st, Manifest* out,
                              int* used_fallback) {
    char name[MANIFEST_MAX_OBJ_LEN + 1];
    int ptr_fallback = 0, rc;
    uint64_t gen = 0;
    if (!st || !out) return -2;
    if (used_fallback) *used_fallback = 0;
    rc = manifest_recover_pointer(st, name, sizeof(name), &ptr_fallback);
    if (rc != 0) return rc; /* -1 virgin, -2 ambiguous: propagate */
    if (manifest_read(st, name, out) == 0) {
        if (used_fallback) *used_fallback = ptr_fallback;
        return 0;
    }
    /* O2: an I/O error on the named manifest fails outright
     * (segdb's long-standing rule), never silently falls back
     * to an older generation. Only a clean read failure
     * (corrupt / missing / unparseable bytes) tries the
     * previous generation with the same naming scheme.
     * manifest_read zeroes *out on failure, so a failed
     * previous read still leaves *out unusable, as required. */
    {
        uint64_t msize = 0;
        if (yowza_store_size_status(st, name, &msize) == YOWZA_STORE_IO_ERROR) return -2;
    }
    if (man_parse_manifest_gen(name, &gen) != 0) return -2;
    if (man_format_manifest_name(name, sizeof(name), gen - 1) != 0) return -2;
    if (manifest_read(st, name, out) != 0) return -2;
    if (used_fallback) *used_fallback = 1;
    return 0;
}

/* ---- Phase B: versioned immutable directory root ---- */

int manifest_root_name(uint64_t gen, char* out, uint32_t cap) {
    char digits[24];
    uint32_t nd = 0, i;
    uint32_t need;
    if (!out || cap == 0 || gen == 0) return -1;
    while (gen > 0) {
        digits[nd++] = (char)('0' + (gen % 10));
        gen /= 10;
    }
    /* "manifest.root." (14) + digits + NUL */
    need = 14 + nd + 1;
    if (need > cap) return -1;
    out[0] = 'm'; out[1] = 'a'; out[2] = 'n'; out[3] = 'i';
    out[4] = 'f'; out[5] = 'e'; out[6] = 's'; out[7] = 't';
    out[8] = '.'; out[9] = 'r'; out[10] = 'o'; out[11] = 'o';
    out[12] = 't'; out[13] = '.';
    for (i = 0; i < nd; i++) out[14 + i] = digits[nd - 1 - i];
    out[14 + nd] = '\0';
    return 0;
}

int manifest_root_parse_gen(const char* name, uint64_t* gen_out) {
    uint64_t gen = 0, i = 0;
    if (!name || !gen_out) return -1;
    if (name[0] != 'm' || name[1] != 'a' || name[2] != 'n' ||
        name[3] != 'i' || name[4] != 'f' || name[5] != 'e' ||
        name[6] != 's' || name[7] != 't' || name[8] != '.' ||
        name[9] != 'r' || name[10] != 'o' || name[11] != 'o' ||
        name[12] != 't' || name[13] != '.')
        return -1;
    i = 14;
    if (name[i] == '\0') return -1;
    while (name[i] != '\0') {
        uint64_t d;
        if (name[i] < '0' || name[i] > '9') return -1;
        d = (uint64_t)(name[i] - '0');
        if (gen > (18446744073709551615ULL - d) / 10) return -1;
        gen = gen * 10 + d;
        i++;
    }
    if (gen == 0) return -1;
    *gen_out = gen;
    return 0;
}

int manifest_root_write(YowzaStore* st, const char* tmp_obj,
                        const ManifestDir* d) {
    uint32_t i;
    uint64_t off = 0, fnv;
    uint8_t magic[6];
    if (!man_has_fn(st, 0) || !man_has_fn(st, 3)) return -1;
    if (!tmp_obj || tmp_obj[0] == '\0' || !d) return -1;
    if (d->generation == 0) return -1;
    if (d->group_count == 0 || d->group_count > MANIFEST_DIR_MAX_GROUPS) return -1;
    for (i = 0; i < d->group_count; i++) {
        uint64_t glen = 0;
        if (man_cstr_len(d->groups[i], MANIFEST_MAX_OBJ_LEN, &glen) != 0 || glen == 0) return -1;
    }
    fnv = man_fnv_init();
    magic[0] = 'Y'; magic[1] = 'W'; magic[2] = 'M'; magic[3] = 'N';
    magic[4] = 'R'; magic[5] = 'T';
    if (man_w_bytes(st, tmp_obj, &off, magic, 6, &fnv) != 0) return -1;
    if (man_w_u32(st, tmp_obj, &off, MANIFEST_ROOT_VERSION, &fnv) != 0) return -1;
    if (man_w_u64(st, tmp_obj, &off, d->generation, &fnv) != 0) return -1;
    if (man_w_u64(st, tmp_obj, &off, d->checkpoint_seq, &fnv) != 0) return -1;
    if (man_w_u32(st, tmp_obj, &off, d->schema_version, &fnv) != 0) return -1;
    if (man_w_u32(st, tmp_obj, &off, d->group_count, &fnv) != 0) return -1;
    for (i = 0; i < d->group_count; i++) {
        uint64_t glen = 0;
        (void)man_cstr_len(d->groups[i], MANIFEST_MAX_OBJ_LEN, &glen);
        if (man_w_u32(st, tmp_obj, &off, (uint32_t)glen, &fnv) != 0) return -1;
        if (man_w_bytes(st, tmp_obj, &off, (const uint8_t*)d->groups[i], glen, &fnv) != 0) return -1;
    }
    if (man_w_u64(st, tmp_obj, &off, fnv, 0) != 0) return -1;
    if (st->vt->truncate(st->state, tmp_obj, off) != 0) return -1;
    return 0;
}

int manifest_root_read(YowzaStore* st, const char* obj,
                       ManifestDir* out) {
    uint64_t size, off = 0, fnv, stored;
    uint32_t version, i;
    uint8_t magic[6];
    if (!man_has_fn(st, 1) || !man_has_fn(st, 2)) return -1;
    if (!obj || !out) return -1;
    man_mem_zero((uint8_t*)out, (uint64_t)sizeof(*out));
    if (st->vt->size(st->state, obj, &size) != 0) return -1;
    if (size < 42) return -1;
    fnv = man_fnv_init();
    if (man_r_bytes(st, obj, &off, magic, 6, &fnv) != 0) return -1;
    if (magic[0] != 'Y' || magic[1] != 'W' || magic[2] != 'M' ||
        magic[3] != 'N' || magic[4] != 'R' || magic[5] != 'T') return -1;
    if (man_r_u32(st, obj, &off, &version, &fnv) != 0) return -1;
    if (version != MANIFEST_ROOT_VERSION) return -1;
    if (man_r_u64(st, obj, &off, &out->generation, &fnv) != 0) return -1;
    if (man_r_u64(st, obj, &off, &out->checkpoint_seq, &fnv) != 0) return -1;
    if (man_r_u32(st, obj, &off, &out->schema_version, &fnv) != 0) return -1;
    if (man_r_u32(st, obj, &off, &out->group_count, &fnv) != 0) return -1;
    if (out->group_count == 0 || out->group_count > MANIFEST_DIR_MAX_GROUPS) goto fail;
    for (i = 0; i < out->group_count; i++) {
        uint32_t glen;
        if (man_r_u32(st, obj, &off, &glen, &fnv) != 0) goto fail;
        if (glen == 0 || glen > MANIFEST_MAX_OBJ_LEN) goto fail;
        if (man_r_bytes(st, obj, &off, (uint8_t*)out->groups[i], glen, &fnv) != 0) goto fail;
        out->groups[i][glen] = '\0';
    }
    if (man_r_u64(st, obj, &off, &stored, 0) != 0) goto fail;
    if (stored != fnv) goto fail;
    if (off != size) goto fail;
    return 0;
fail:
    man_mem_zero((uint8_t*)out, (uint64_t)sizeof(*out));
    return -1;
}

int manifest_migrate_dir_to_root(YowzaStore* st,
                                 const ManifestDir* dir,
                                 uint64_t gen, char* out_name,
                                 uint32_t cap) {
    char final_name[MANIFEST_MAX_OBJ_LEN + 1];
    char tmp_name[MANIFEST_MAX_OBJ_LEN + 1];
    ManifestDir root;
    uint32_t i;
    int prc;
    if (!st || !dir || !out_name || cap == 0) return -1;
    if (manifest_root_name(gen, final_name, sizeof(final_name)) != 0) return -1;
    /* tmp name = final + ".tmp" (must fit). */
    {
        uint64_t n = 0;
        while (final_name[n] != '\0') n++;
        if (n + 4 + 1 > sizeof(tmp_name)) return -1;
        for (i = 0; (uint64_t)i < n; i++) tmp_name[i] = final_name[i];
        tmp_name[n] = '.'; tmp_name[n + 1] = 't'; tmp_name[n + 2] = 'm'; tmp_name[n + 3] = 'p';
        tmp_name[n + 4] = '\0';
    }
    root.generation = gen;
    root.checkpoint_seq = dir->checkpoint_seq;
    root.schema_version = dir->schema_version;
    root.group_count = dir->group_count;
    for (i = 0; i < dir->group_count; i++) {
        uint64_t n = 0;
        while (dir->groups[i][n] != '\0' && n < MANIFEST_MAX_OBJ_LEN) {
            root.groups[i][n] = dir->groups[i][n];
            n++;
        }
        root.groups[i][n] = '\0';
    }
    if (manifest_root_write(st, tmp_name, &root) != 0) return -1;
    prc = manifest_publish(st, tmp_name, final_name);
    if (prc != 0) {
        /* -2 is indeterminate: the root may be visible.
         * Hand the name back so the caller can re-read
         * and compare instead of assuming either way. */
        uint64_t n = 0;
        while (final_name[n] != '\0' && n + 1 < cap) {
            out_name[n] = final_name[n];
            n++;
        }
        if (n + 1 > cap) return -1;
        out_name[n] = '\0';
        return prc;
    }
    {
        uint64_t n = 0;
        while (final_name[n] != '\0' && n + 1 < cap) {
            out_name[n] = final_name[n];
            n++;
        }
        if (n + 1 > cap) return -1;
        out_name[n] = '\0';
    }
    return 0;
}

/* ---- Phase B: shared published-state resolver ---- */

static int segdb_resolve_copy_pointer(SegdbPublishedState* out,
                                      const char* name) {
    uint64_t n = 0;
    while (name[n] != '\0' && n < MANIFEST_MAX_OBJ_LEN) {
        out->pointer[n] = name[n];
        n++;
    }
    if (name[n] != '\0') return -1;
    out->pointer[n] = '\0';
    return 0;
}

int segdb_resolve_published_state(YowzaStore* st,
                                  SegdbPublishedState* out) {
    char name[MANIFEST_MAX_OBJ_LEN + 1];
    int ptr_fallback = 0, prc;
    uint64_t gen = 0, sz = 0;
    if (!st || !out) return -1;
    man_mem_zero((uint8_t*)out, (uint64_t)sizeof(*out));
    out->kind = SEGDB_PUBLISHED_AMBIGUOUS;
    prc = manifest_recover_pointer(st, name, sizeof(name), &ptr_fallback);
    if (prc == -1) {
        out->kind = SEGDB_PUBLISHED_VIRGIN;
        return 0;
    }
    if (prc == -2) {
        /* Distinguish I/O error from ambiguous content:
         * any pointer status of 2, or a failed content
         * probe, is an I/O error; surviving content with
         * both pointers authoritatively absent is
         * ambiguous (never virgin, never a fallback). */
        char scratch[MANIFEST_MAX_OBJ_LEN + 1];
        int has = 0;
        if (manifest_current_status(st, "CURRENT", scratch, sizeof(scratch)) == 2 ||
            manifest_current_status(st, "CURRENT.bak", scratch, sizeof(scratch)) == 2) {
            out->kind = SEGDB_PUBLISHED_IO_ERROR;
            return 0;
        }
        if (yowza_store_detect_prior_content(st, &has) != 0) {
            out->kind = SEGDB_PUBLISHED_IO_ERROR;
            return 0;
        }
        (void)has;
        out->kind = SEGDB_PUBLISHED_AMBIGUOUS;
        return 0;
    }
    /* prc == 0: a usable pointer was resolved. */
    if (segdb_resolve_copy_pointer(out, name) != 0) {
        out->kind = SEGDB_PUBLISHED_AMBIGUOUS;
        return 0;
    }
    out->used_fallback = ptr_fallback;
    /* Legacy fixed directory. */
    if (name[0] == 'm' && name[1] == 'a' && name[2] == 'n' &&
        name[3] == 'i' && name[4] == 'f' && name[5] == 'e' &&
        name[6] == 's' && name[7] == 't' && name[8] == '.' &&
        name[9] == 'd' && name[10] == 'i' && name[11] == 'r' &&
        name[12] == '\0') {
        if (manifest_dir_read(st, MANIFEST_DIR_OBJ, &out->dir) != 0) {
            if (yowza_store_size_status(st, MANIFEST_DIR_OBJ, &sz) == YOWZA_STORE_IO_ERROR)
                out->kind = SEGDB_PUBLISHED_IO_ERROR;
            else
                out->kind = SEGDB_PUBLISHED_AMBIGUOUS;
            man_mem_zero((uint8_t*)&out->dir, (uint64_t)sizeof(out->dir));
            return 0;
        }
        out->kind = SEGDB_PUBLISHED_DIRECTORY;
        out->generation = out->dir.generation;
        return 0;
    }
    /* Versioned immutable directory root. */
    if (manifest_root_parse_gen(name, &gen) == 0) {
        if (manifest_root_read(st, name, &out->dir) != 0 ||
            out->dir.generation != gen) {
            if (yowza_store_size_status(st, name, &sz) == YOWZA_STORE_IO_ERROR)
                out->kind = SEGDB_PUBLISHED_IO_ERROR;
            else
                out->kind = SEGDB_PUBLISHED_AMBIGUOUS;
            man_mem_zero((uint8_t*)&out->dir, (uint64_t)sizeof(out->dir));
            return 0;
        }
        out->kind = SEGDB_PUBLISHED_DIRECTORY_ROOT;
        out->generation = gen;
        return 0;
    }
    /* Single manifest (possibly with previous-generation
     * fallback on a CLEAN read failure only). */
    if (manifest_read(st, name, &out->manifest) == 0) {
        if (man_parse_manifest_gen(name, &gen) == 0)
            out->generation = gen;
        else
            out->generation = out->manifest.generation;
        out->kind = SEGDB_PUBLISHED_SINGLE;
        return 0;
    }
    if (yowza_store_size_status(st, name, &sz) == YOWZA_STORE_IO_ERROR) {
        out->kind = SEGDB_PUBLISHED_IO_ERROR;
        man_mem_zero((uint8_t*)&out->manifest, (uint64_t)sizeof(out->manifest));
        return 0;
    }
    if (man_parse_manifest_gen(name, &gen) != 0 || gen < 1) {
        out->kind = SEGDB_PUBLISHED_AMBIGUOUS;
        man_mem_zero((uint8_t*)&out->manifest, (uint64_t)sizeof(out->manifest));
        return 0;
    }
    if (man_format_manifest_name(name, sizeof(name), gen - 1) != 0) {
        out->kind = SEGDB_PUBLISHED_AMBIGUOUS;
        man_mem_zero((uint8_t*)&out->manifest, (uint64_t)sizeof(out->manifest));
        return 0;
    }
    if (manifest_read(st, name, &out->manifest) != 0) {
        out->kind = SEGDB_PUBLISHED_AMBIGUOUS;
        man_mem_zero((uint8_t*)&out->manifest, (uint64_t)sizeof(out->manifest));
        return 0;
    }
    out->kind = SEGDB_PUBLISHED_FALLBACK;
    out->used_fallback = 1;
    out->generation = gen - 1;
    return 0;
}
