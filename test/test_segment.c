/* Phase 2, Track B tests: segment v1 + manifest v2 + ART iterator.
 *
 * Standalone (now that Track A has delivered src/storage2.c, the
 * task's command works as written; the store below is still a minimal
 * in-test memory implementation of Track A's YowzaStore vtable, used
 * so these tests stay independent of any backend):
 *   clang -O2 -Iinclude -Wall -Wextra test/test_segment.c src/segment.c \
 *     src/manifest.c src/art.c src/arena.c src/storage2.c \
 *     -o /tmp/t_segment && /tmp/t_segment
 *
 * main() is deliberately __attribute__((weak)): the standalone binary
 * above has no other main, while `make test` links test/main.c's
 * strong main, which takes precedence — so this file compiles under
 * both without a duplicate-main link error. The integrator registers
 * the test_segment_* and test_manifest_* functions in test/main.c;
 * the weak main is then dormant.
 */
#include <stdio.h>
#include <assert.h>
#include <string.h>
#include <stdlib.h>
#include "segment.h"
#include "manifest.h"
#include "art.h"
#include "arena.h"

/* ---------------- in-test memory store ---------------- */

#define T_MAX_OBJS 32
#define T_MAX_NAME 255

typedef struct {
    char name[T_MAX_NAME + 1];
    uint8_t* data;
    uint64_t len;
    uint64_t cap;
    int used;
} TObj;

typedef struct {
    TObj objs[T_MAX_OBJS];
    uint64_t max_read_len;
    int track_reads;
    uint64_t sync_obj_calls;
    uint64_t sync_dir_calls;
    uint64_t publish_calls;
} TStoreState;

static TObj* t_find(TStoreState* s, const char* name) {
    int i;
    for (i = 0; i < T_MAX_OBJS; i++) {
        if (s->objs[i].used && strcmp(s->objs[i].name, name) == 0)
            return &s->objs[i];
    }
    return NULL;
}

static TObj* t_find_or_create(TStoreState* s, const char* name) {
    TObj* o = t_find(s, name);
    int i;
    if (o) return o;
    for (i = 0; i < T_MAX_OBJS; i++) {
        if (!s->objs[i].used) {
            s->objs[i].used = 1;
            s->objs[i].len = 0;
            s->objs[i].cap = 0;
            s->objs[i].data = NULL;
            strncpy(s->objs[i].name, name, T_MAX_NAME);
            s->objs[i].name[T_MAX_NAME] = '\0';
            return &s->objs[i];
        }
    }
    return NULL;
}

static int t_grow(TObj* o, uint64_t need) {
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

static int t_write_at(void* stp, const char* obj, uint64_t offset,
                      const uint8_t* data, uint64_t len) {
    TStoreState* s = (TStoreState*)stp;
    TObj* o = t_find_or_create(s, obj);
    if (!o) return -1;
    if (t_grow(o, offset + len) != 0) return -1;
    memcpy(o->data + offset, data, (size_t)len);
    if (offset + len > o->len) o->len = offset + len;
    return 0;
}

static int t_read_at(void* stp, const char* obj, uint64_t offset,
                     uint8_t* buf, uint64_t len) {
    TStoreState* s = (TStoreState*)stp;
    TObj* o = t_find(s, obj);
    if (!o) return -1;
    if (offset + len > o->len) return -1; /* short read = error */
    if (s->track_reads && len > s->max_read_len) s->max_read_len = len;
    memcpy(buf, o->data + offset, (size_t)len);
    return 0;
}

static int t_size(void* stp, const char* obj, uint64_t* out_size) {
    TStoreState* s = (TStoreState*)stp;
    TObj* o = t_find(s, obj);
    if (!o) return -1;
    *out_size = o->len;
    return 0;
}

static int t_sync_obj(void* stp, const char* obj) {
    TStoreState* s = (TStoreState*)stp;
    (void)obj;
    s->sync_obj_calls++;
    return 0;
}

static int t_sync_dir(void* stp) {
    TStoreState* s = (TStoreState*)stp;
    s->sync_dir_calls++;
    return 0;
}

static int t_publish(void* stp, const char* tmp_obj,
                     const char* final_obj) {
    TStoreState* s = (TStoreState*)stp;
    TObj* tmp = t_find(s, tmp_obj);
    TObj* fin;
    if (!tmp) return -1;
    fin = t_find(s, final_obj);
    if (fin) {
        free(fin->data);
        fin->data = NULL;
        fin->len = 0;
        fin->cap = 0;
        fin->used = 0;
    }
    strncpy(tmp->name, final_obj, T_MAX_NAME);
    tmp->name[T_MAX_NAME] = '\0';
    s->publish_calls++;
    return 0;
}

static int t_remove(void* stp, const char* obj) {
    TStoreState* s = (TStoreState*)stp;
    TObj* o = t_find(s, obj);
    if (!o) return -1;
    free(o->data);
    o->data = NULL;
    o->len = 0;
    o->cap = 0;
    o->used = 0;
    return 0;
}

static int t_truncate(void* stp, const char* obj, uint64_t len) {
    TStoreState* s = (TStoreState*)stp;
    TObj* o = t_find(s, obj);
    if (!o) return -1;
    if (t_grow(o, len) != 0) return -1;
    if (len > o->len) memset(o->data + o->len, 0, (size_t)(len - o->len));
    o->len = len;
    return 0;
}

static int t_size_status(void* stp, const char* obj, uint64_t* out_size) {
    TStoreState* s = (TStoreState*)stp;
    TObj* o = t_find(s, obj);
    if (!o) return YOWZA_STORE_NOT_FOUND;
    *out_size = o->len;
    return YOWZA_STORE_OK;
}

static const YowzaStoreVTable t_vtable = {
    t_write_at, t_read_at, t_size, t_sync_obj, t_sync_dir,
    t_publish, t_remove, t_truncate, NULL, t_size_status,
    /* enumerate: NULL = this mock backend cannot enumerate, so
     * callers must treat enumeration as unknown, never empty
     * (Gate 2 semantics). */
    NULL
};

static void t_make_store(YowzaStore* st, TStoreState* state) {
    memset(state, 0, sizeof(*state));
    st->vt = &t_vtable;
    st->state = state;
}

static void t_free_store(TStoreState* s) {
    int i;
    for (i = 0; i < T_MAX_OBJS; i++) {
        free(s->objs[i].data);
        s->objs[i].data = NULL;
    }
}

/* Flip one byte of a stored object (corruption injection). */
static void t_corrupt(TStoreState* s, const char* obj, uint64_t off) {
    TObj* o = t_find(s, obj);
    assert(o != NULL);
    assert(off < o->len);
    o->data[off] ^= 0xFF;
}

static SegEntry t_ent(const char* k, const char* v, int tomb, uint64_t seq,
                      uint64_t ts) {
    SegEntry e;
    e.key = (const uint8_t*)k;
    e.key_len = (uint32_t)strlen(k);
    e.val = (const uint8_t*)v;
    e.val_len = v ? (uint32_t)strlen(v) : 0;
    e.is_tombstone = tomb;
    e.commit_seq = seq;
    e.crdt_ts = ts;
    return e;
}

/* ---------------- segment tests ---------------- */

void test_segment_roundtrip() {
    printf("Running test_segment_roundtrip...\n");
    {
        YowzaStore st;
        TStoreState ss;
        SegEntry ents[4];
        SegReader r;
        SegEntry out;
        uint8_t vbuf[64];
        uint8_t kbuf[64];
        uint64_t nbytes = 0, c1 = 0, c2 = 0;
        int rc, n = 0;
        t_make_store(&st, &ss);
        ents[0] = t_ent("a", "1", 0, 10, 999);
        ents[1] = t_ent("b", NULL, 1, 11, 5);
        ents[2] = t_ent("c", "333", 0, 12, 12);
        ents[3] = t_ent("d", "", 0, 13, 0);
        assert(seg_write(&st, "seg.rt", ents, 4, 0, 0, 1, &nbytes) == 0);
        assert(nbytes > 0);
        assert(seg_open(&st, "seg.rt", &r) == 0);
        assert((r.flags & SEG_FLAG_BLOOM) != 0);
        assert(r.entry_count == 4 && r.min_seq == 10 && r.max_seq == 13);
        /* Live entry: value + distinct seq/ts preserved. */
        rc = seg_lookup(&r, (const uint8_t*)"a", 1, &out, vbuf, sizeof(vbuf));
        assert(rc == 0 && !out.is_tombstone && out.val_len == 1 &&
               memcmp(out.val, "1", 1) == 0 && out.commit_seq == 10 &&
               out.crdt_ts == 999);
        /* Tombstone: found, flagged, no value, seq/ts kept distinct. */
        rc = seg_lookup(&r, (const uint8_t*)"b", 1, &out, vbuf, sizeof(vbuf));
        assert(rc == 0 && out.is_tombstone && out.val == NULL &&
               out.val_len == 0 && out.commit_seq == 11 && out.crdt_ts == 5);
        rc = seg_lookup(&r, (const uint8_t*)"c", 1, &out, vbuf, sizeof(vbuf));
        assert(rc == 0 && out.val_len == 3 && memcmp(out.val, "333", 3) == 0);
        /* Live entry with an empty value is live, not a tombstone. */
        rc = seg_lookup(&r, (const uint8_t*)"d", 1, &out, vbuf, sizeof(vbuf));
        assert(rc == 0 && !out.is_tombstone && out.val_len == 0);
        /* Absent keys: before first, between entries, after last. */
        assert(seg_lookup(&r, (const uint8_t*)"0", 1, &out, vbuf,
                          sizeof(vbuf)) == 1);
        assert(seg_lookup(&r, (const uint8_t*)"aa", 2, &out, vbuf,
                          sizeof(vbuf)) == 1);
        assert(seg_lookup(&r, (const uint8_t*)"zz", 2, &out, vbuf,
                          sizeof(vbuf)) == 1);
        /* Iteration returns everything sorted, tombstone included. */
        seg_close(&r);
        assert(seg_open(&st, "seg.rt", &r) == 0);
        while ((rc = seg_iter_next(&r, &out, kbuf, sizeof(kbuf), vbuf,
                                   sizeof(vbuf))) == 0) {
            n++;
            if (n == 1) assert(out.key_len == 1 && kbuf[0] == 'a');
            if (n == 2) assert(out.is_tombstone && kbuf[0] == 'b');
            if (n == 3) assert(kbuf[0] == 'c' && out.val_len == 3);
            if (n == 4) assert(kbuf[0] == 'd' && !out.is_tombstone);
        }
        assert(rc == 1 && n == 4);
        seg_close(&r);
        /* Whole-object checksum is deterministic. */
        assert(seg_file_checksum(&st, "seg.rt", &c1) == 0);
        assert(seg_file_checksum(&st, "seg.rt", &c2) == 0);
        assert(c1 == c2 && c1 != 0);
        t_free_store(&ss);
    }
    printf("Passed.\n");
}

void test_segment_unsorted_rejected() {
    printf("Running test_segment_unsorted_rejected...\n");
    {
        YowzaStore st;
        TStoreState ss;
        SegEntry e1[2], e2[2], e3[1], e4[1];
        t_make_store(&st, &ss);
        e1[0] = t_ent("b", "1", 0, 1, 0);
        e1[1] = t_ent("a", "2", 0, 2, 0);
        assert(seg_write(&st, "seg.u1", e1, 2, 0, 0, 0, NULL) == -1);
        e2[0] = t_ent("a", "1", 0, 1, 0);
        e2[1] = t_ent("a", "2", 0, 2, 0);
        assert(seg_write(&st, "seg.u2", e2, 2, 0, 0, 0, NULL) == -1);
        e3[0] = t_ent("a", "v", 1, 1, 0); /* tombstone with a value */
        assert(seg_write(&st, "seg.u3", e3, 1, 0, 0, 0, NULL) == -1);
        e4[0] = t_ent("a", "v", 0, 1, 0);
        assert(seg_write(&st, "seg.u4", e4, 1, 1, 0, 0, NULL) == -1);
        assert(seg_write(&st, "seg.u5", e4, 1, 0, 2, 0, NULL) == -1);
        e4[0] = t_ent("", "v", 0, 1, 0); /* empty key */
        assert(seg_write(&st, "seg.u6", e4, 1, 0, 0, 0, NULL) == -1);
        t_free_store(&ss);
    }
    printf("Passed.\n");
}

void test_segment_block_checksum_corruption() {
    printf("Running test_segment_block_checksum_corruption...\n");
    {
        YowzaStore st;
        TStoreState ss;
        SegEntry ents[3];
        SegReader r;
        SegEntry out;
        uint8_t vbuf[64], kbuf[64];
        uint64_t header_len, val_off;
        t_make_store(&st, &ss);
        ents[0] = t_ent("k001", "value-one", 0, 1, 0);
        ents[1] = t_ent("k002", "value-two", 0, 2, 0);
        ents[2] = t_ent("k003", "value-three", 0, 3, 0);
        assert(seg_write(&st, "seg.bc", ents, 3, 0, 0, 0, NULL) == 0);
        /* First block starts right after header + first + last keys;
         * corrupt the first value byte inside it. */
        header_len = 64 + 4 + 4;
        val_off = header_len + 4 + 4 + 4;
        t_corrupt(&ss, "seg.bc", val_off);
        /* Open still succeeds (header/footer intact); the corrupt block
         * must fail on lookup and on iteration, never return data. */
        assert(seg_open(&st, "seg.bc", &r) == 0);
        assert(seg_lookup(&r, (const uint8_t*)"k001", 4, &out, vbuf,
                          sizeof(vbuf)) == -1);
        seg_close(&r);
        assert(seg_open(&st, "seg.bc", &r) == 0);
        assert(seg_iter_next(&r, &out, kbuf, sizeof(kbuf), vbuf,
                             sizeof(vbuf)) == -1);
        seg_close(&r);
        t_free_store(&ss);
    }
    printf("Passed.\n");
}

void test_segment_footer_truncation() {
    printf("Running test_segment_footer_truncation...\n");
    {
        YowzaStore st;
        TStoreState ss;
        SegEntry ents[2];
        SegReader r;
        uint64_t size = 0;
        t_make_store(&st, &ss);
        ents[0] = t_ent("a", "1", 0, 1, 0);
        ents[1] = t_ent("b", "2", 0, 2, 0);
        assert(seg_write(&st, "seg.ft", ents, 2, 0, 0, 0, NULL) == 0);
        assert(st.vt->size(st.state, "seg.ft", &size) == 0 && size > 40);
        /* One byte off the footer: total_len mismatch. */
        assert(st.vt->truncate(st.state, "seg.ft", size - 1) == 0);
        assert(seg_open(&st, "seg.ft", &r) == -1);
        /* Header only: far too short. */
        assert(st.vt->truncate(st.state, "seg.ft", 10) == 0);
        assert(seg_open(&st, "seg.ft", &r) == -1);
        /* Bad magic. */
        assert(seg_write(&st, "seg.ft2", ents, 2, 0, 0, 0, NULL) == 0);
        t_corrupt(&ss, "seg.ft2", 0);
        assert(seg_open(&st, "seg.ft2", &r) == -1);
        t_free_store(&ss);
    }
    printf("Passed.\n");
}

/* 300 entries x ~129 bytes: ~10 blocks. Exercises first/last/middle
 * blocks and keys that fall between blocks. */
static char s_keys[300][8];
static char s_vals[300][104];
static SegEntry s_ents[300];

void test_segment_sparse_index_lookup() {
    printf("Running test_segment_sparse_index_lookup...\n");
    {
        YowzaStore st;
        TStoreState ss;
        SegReader r;
        SegEntry out;
        uint8_t vbuf[256], kbuf[64];
        int i, rc, n = 0;
        uint64_t nbytes = 0;
        t_make_store(&st, &ss);
        for (i = 0; i < 300; i++) {
            int j;
            snprintf(s_keys[i], sizeof(s_keys[i]), "k%03d", i);
            for (j = 0; j < 100; j++)
                s_vals[i][j] = (char)('A' + (i % 26));
            s_vals[i][100] = '\0';
            s_ents[i].key = (const uint8_t*)s_keys[i];
            s_ents[i].key_len = 4;
            s_ents[i].val = (const uint8_t*)s_vals[i];
            s_ents[i].val_len = 100;
            s_ents[i].is_tombstone = 0;
            s_ents[i].commit_seq = (uint64_t)(i + 1);
            s_ents[i].crdt_ts = (uint64_t)(i * 7);
        }
        assert(seg_write(&st, "seg.si", s_ents, 300, 0, 0, 0, &nbytes) == 0);
        assert(nbytes > 300 * 100); /* multi-block, value-dominated */
        assert(seg_open(&st, "seg.si", &r) == 0);
        assert(r.entry_count == 300);
        assert(r.block_count == 0); /* lazy: not discovered yet */
        /* First block, last block, middle, block-boundary neighbour. */
        rc = seg_lookup(&r, (const uint8_t*)"k000", 4, &out, vbuf,
                        sizeof(vbuf));
        assert(rc == 0 && out.val_len == 100 && out.commit_seq == 1 &&
               out.crdt_ts == 0);
        assert(r.block_count >= 5); /* discovery ran, several blocks */
        rc = seg_lookup(&r, (const uint8_t*)"k299", 4, &out, vbuf,
                        sizeof(vbuf));
        assert(rc == 0 && out.commit_seq == 300 && out.crdt_ts == 299 * 7);
        rc = seg_lookup(&r, (const uint8_t*)"k150", 4, &out, vbuf,
                        sizeof(vbuf));
        assert(rc == 0 && out.commit_seq == 151);
        assert(seg_lookup(&r, (const uint8_t*)"k150x", 5, &out, vbuf,
                          sizeof(vbuf)) == 1);
        assert(seg_lookup(&r, (const uint8_t*)"k999", 4, &out, vbuf,
                          sizeof(vbuf)) == 1);
        assert(seg_lookup(&r, (const uint8_t*)"a", 1, &out, vbuf,
                          sizeof(vbuf)) == 1);
        /* Full iteration in order. */
        seg_close(&r);
        assert(seg_open(&st, "seg.si", &r) == 0);
        while ((rc = seg_iter_next(&r, &out, kbuf, sizeof(kbuf), vbuf,
                                   sizeof(vbuf))) == 0) {
            char want[8];
            snprintf(want, sizeof(want), "k%03d", n);
            assert(out.key_len == 4 && memcmp(kbuf, want, 4) == 0);
            assert(out.commit_seq == (uint64_t)(n + 1));
            n++;
        }
        assert(rc == 1 && n == 300);
        seg_close(&r);
        t_free_store(&ss);
    }
    printf("Passed.\n");
}

static char b_keys[200][8];
static char b_vals[200][16];
static SegEntry b_ents[200];

void test_segment_bloom_no_false_negative() {
    printf("Running test_segment_bloom_no_false_negative...\n");
    {
        YowzaStore st;
        TStoreState ss;
        SegReader r;
        SegEntry out;
        uint8_t vbuf[64];
        int i;
        char absent[8];
        t_make_store(&st, &ss);
        for (i = 0; i < 200; i++) {
            snprintf(b_keys[i], sizeof(b_keys[i]), "b%03d", i);
            snprintf(b_vals[i], sizeof(b_vals[i]), "v%d", i);
            b_ents[i] = t_ent(b_keys[i], b_vals[i], 0, (uint64_t)(i + 1), 0);
        }
        assert(seg_write(&st, "seg.bl", b_ents, 200, 0, 0, 1, NULL) == 0);
        assert(seg_open(&st, "seg.bl", &r) == 0);
        assert((r.flags & SEG_FLAG_BLOOM) != 0);
        /* Every written key must be found: no false negatives, ever. */
        for (i = 0; i < 200; i++) {
            assert(seg_lookup(&r, (const uint8_t*)b_keys[i], 4, &out, vbuf,
                              sizeof(vbuf)) == 0);
        }
        /* Absent keys must report absent (a Bloom false positive still
         * resolves to absent via the block lookup, never to a hit). */
        for (i = 0; i < 50; i++) {
            snprintf(absent, sizeof(absent), "x%03d", i);
            assert(seg_lookup(&r, (const uint8_t*)absent, 4, &out, vbuf,
                              sizeof(vbuf)) == 1);
        }
        seg_close(&r);
        t_free_store(&ss);
    }
    printf("Passed.\n");
}

/* 2000 entries: segment >> reader struct and >> any single read. */
static char m_keys[2000][8];
static char m_vals[2000][40];
static SegEntry m_ents[2000];

void test_segment_iterator_no_whole_load() {
    printf("Running test_segment_iterator_no_whole_load...\n");
    {
        YowzaStore st;
        TStoreState ss;
        SegReader r;
        SegEntry out;
        uint8_t vbuf[64], kbuf[16];
        int i, rc, n = 0;
        uint64_t nbytes = 0;
        char prev[8];
        t_make_store(&st, &ss);
        for (i = 0; i < 2000; i++) {
            int j;
            snprintf(m_keys[i], sizeof(m_keys[i]), "m%04d", i);
            for (j = 0; j < 32; j++)
                m_vals[i][j] = (char)('a' + (i % 26));
            m_vals[i][32] = '\0';
            m_ents[i].key = (const uint8_t*)m_keys[i];
            m_ents[i].key_len = 5;
            m_ents[i].val = (const uint8_t*)m_vals[i];
            m_ents[i].val_len = 32;
            m_ents[i].is_tombstone = 0;
            m_ents[i].commit_seq = (uint64_t)(i + 1);
            m_ents[i].crdt_ts = 0;
        }
        assert(seg_write(&st, "seg.big", m_ents, 2000, 0, 0, 0,
                         &nbytes) == 0);
        assert(nbytes > 100000);
        assert(sizeof(SegReader) < (size_t)nbytes); /* bounded state */
        ss.track_reads = 1;
        ss.max_read_len = 0;
        assert(seg_open(&st, "seg.big", &r) == 0);
        prev[0] = '\0';
        while ((rc = seg_iter_next(&r, &out, kbuf, sizeof(kbuf), vbuf,
                                   sizeof(vbuf))) == 0) {
            if (n > 0) assert(memcmp(prev, kbuf, 5) < 0);
            memcpy(prev, kbuf, 5);
            prev[5] = '\0';
            assert(out.val_len == 32);
            n++;
        }
        assert(rc == 1 && n == 2000);
        /* No single reader read ever approached the segment size. */
        assert(ss.max_read_len > 0 &&
               ss.max_read_len <= SEG_READER_MAX_CHUNK);
        seg_close(&r);
        t_free_store(&ss);
    }
    printf("Passed.\n");
}

void test_segment_empty() {
    printf("Running test_segment_empty...\n");
    {
        YowzaStore st;
        TStoreState ss;
        SegReader r;
        SegEntry out;
        uint8_t vbuf[16], kbuf[16];
        t_make_store(&st, &ss);
        assert(seg_write(&st, "seg.e0", NULL, 0, 0, 0, 0, NULL) == 0);
        assert(seg_open(&st, "seg.e0", &r) == 0);
        assert(r.entry_count == 0);
        assert(seg_lookup(&r, (const uint8_t*)"x", 1, &out, vbuf,
                          sizeof(vbuf)) == 1);
        assert(seg_iter_next(&r, &out, kbuf, sizeof(kbuf), vbuf,
                             sizeof(vbuf)) == 1);
        seg_close(&r);
        assert(seg_write(&st, "seg.e1", NULL, 0, 0, 0, 1, NULL) == 0);
        assert(seg_open(&st, "seg.e1", &r) == 0);
        assert(seg_iter_next(&r, &out, kbuf, sizeof(kbuf), vbuf,
                             sizeof(vbuf)) == 1);
        seg_close(&r);
        t_free_store(&ss);
    }
    printf("Passed.\n");
}

void test_segment_index_checksum_corruption() {
    printf("Running test_segment_index_checksum_corruption...\n");
    {
        YowzaStore st;
        TStoreState ss;
        SegEntry ents[2];
        SegReader r;
        SegEntry out;
        uint8_t vbuf[16], kbuf[16];
        uint64_t size = 0;
        t_make_store(&st, &ss);
        ents[0] = t_ent("a", "1", 0, 1, 0);
        ents[1] = t_ent("b", "2", 0, 2, 0);
        assert(seg_write(&st, "seg.ic", ents, 2, 0, 0, 0, NULL) == 0);
        assert(st.vt->size(st.state, "seg.ic", &size) == 0);
        /* Corrupt the footer's index_checksum field (footer+22). Open
         * still succeeds (header verified); discovery must fail. */
        t_corrupt(&ss, "seg.ic", size - 32 + 22);
        assert(seg_open(&st, "seg.ic", &r) == 0);
        assert(seg_lookup(&r, (const uint8_t*)"a", 1, &out, vbuf,
                          sizeof(vbuf)) == -1);
        seg_close(&r);
        assert(seg_open(&st, "seg.ic", &r) == 0);
        assert(seg_iter_next(&r, &out, kbuf, sizeof(kbuf), vbuf,
                             sizeof(vbuf)) == -1);
        seg_close(&r);
        t_free_store(&ss);
    }
    printf("Passed.\n");
}

/* ---------------- ART iterator tests ---------------- */

typedef struct {
    int count;
    int stopped_at;
    char keys[70][8];
    uint32_t key_lens[70];
    int tombs[70];
} IterCtx;

static int t_iter_cb(void* ctx, const uint8_t* key, uint32_t key_len,
                     const uint8_t* val, uint32_t val_len,
                     int is_tombstone, uint64_t timestamp,
                     uint64_t version) {
    IterCtx* c = (IterCtx*)ctx;
    (void)val;
    (void)val_len;
    (void)timestamp;
    (void)version;
    assert(c->count < 70);
    memcpy(c->keys[c->count], key, key_len);
    c->keys[c->count][key_len] = '\0';
    c->key_lens[c->count] = key_len;
    c->tombs[c->count] = is_tombstone;
    c->count++;
    if (c->stopped_at >= 0 && c->count >= c->stopped_at) return 1;
    return 0;
}

void test_segment_art_iterate() {
    printf("Running test_segment_art_iterate...\n");
    {
        uint8_t mem[65536];
        Arena arena;
        ART art;
        IterCtx ctx;
        uint8_t* v;
        uint32_t vl;
        int i;
        arena_init(&arena, mem, sizeof(mem));
        art_init(&art, &arena);
        /* Inserted out of order on purpose (NODE4 sort must fix it). */
        assert(art_insert(&art, (const uint8_t*)"d", 1,
                          (const uint8_t*)"4", 1, 0) == 0);
        assert(art_insert(&art, (const uint8_t*)"a", 1,
                          (const uint8_t*)"1", 1, 0) == 0);
        assert(art_insert(&art, (const uint8_t*)"c", 1,
                          (const uint8_t*)"3", 1, 0) == 0);
        assert(art_insert(&art, (const uint8_t*)"b", 1,
                          (const uint8_t*)"2", 1, 0) == 0);
        assert(art_insert(&art, (const uint8_t*)"e", 1,
                          (const uint8_t*)"5", 1, 0) == 0);
        assert(art_delete(&art, (const uint8_t*)"c", 1) == 0);
        memset(&ctx, 0, sizeof(ctx));
        ctx.stopped_at = -1;
        assert(art_iterate(&art, t_iter_cb, &ctx) == 0);
        assert(ctx.count == 5);
        assert(strcmp(ctx.keys[0], "a") == 0 && !ctx.tombs[0]);
        assert(strcmp(ctx.keys[1], "b") == 0 && !ctx.tombs[1]);
        assert(strcmp(ctx.keys[2], "c") == 0 && ctx.tombs[2]);
        assert(strcmp(ctx.keys[3], "d") == 0 && !ctx.tombs[3]);
        assert(strcmp(ctx.keys[4], "e") == 0 && !ctx.tombs[4]);
        /* Early stop: callback returning non-zero halts iteration. */
        memset(&ctx, 0, sizeof(ctx));
        ctx.stopped_at = 2;
        assert(art_iterate(&art, t_iter_cb, &ctx) == 1);
        assert(ctx.count == 2);
        /* Existing behaviour untouched: search still works. */
        assert(art_search(&art, (const uint8_t*)"a", 1, &v, &vl) == 0 &&
               vl == 1 && v[0] == '1');
        assert(art_search(&art, (const uint8_t*)"c", 1, &v, &vl) == -1);
        /* 60 two-byte keys inserted in reverse: exercises NODE48/256
         * child ordering (40+ children under one node). */
        arena_init(&arena, mem, sizeof(mem));
        art_init(&art, &arena);
        for (i = 59; i >= 0; i--) {
            uint8_t k[2];
            k[0] = 'k';
            k[1] = (uint8_t)i;
            assert(art_insert(&art, k, 2, (const uint8_t*)"v", 1, 0) == 0);
        }
        memset(&ctx, 0, sizeof(ctx));
        ctx.stopped_at = -1;
        assert(art_iterate(&art, t_iter_cb, &ctx) == 0);
        assert(ctx.count == 60);
        for (i = 0; i < 60; i++) {
            assert(ctx.key_lens[i] == 2 && ctx.keys[i][0] == 'k' &&
                   (uint8_t)ctx.keys[i][1] == (uint8_t)i);
        }
    }
    printf("Passed.\n");
}

/* ---------------- manifest tests ---------------- */

/* Manifest is ~285 KiB: file-scope statics, never stack. */
static Manifest t_man, t_man_out;

static void t_fill_meta(SegMeta* s, const char* obj, const char* fk,
                        const char* lk, uint64_t min_seq, uint64_t max_seq,
                        uint32_t count, uint64_t byte_size,
                        uint64_t checksum) {
    memset(s, 0, sizeof(*s));
    strncpy(s->obj, obj, MANIFEST_MAX_OBJ_LEN);
    s->first_key_len = (uint32_t)strlen(fk);
    memcpy(s->first_key, fk, s->first_key_len);
    s->last_key_len = (uint32_t)strlen(lk);
    memcpy(s->last_key, lk, s->last_key_len);
    s->min_seq = min_seq;
    s->max_seq = max_seq;
    s->entry_count = count;
    s->index_type = 0;
    s->model_version = 0;
    s->byte_size = byte_size;
    s->checksum = checksum;
}

void test_manifest_roundtrip() {
    printf("Running test_manifest_roundtrip...\n");
    {
        YowzaStore st;
        TStoreState ss;
        char cur[200];
        t_make_store(&st, &ss);
        memset(&t_man, 0, sizeof(t_man));
        t_man.generation = 7;
        t_man.checkpoint_seq = 12345;
        t_man.schema_version = 3;
        t_man.segment_count = 2;
        t_fill_meta(&t_man.segments[0], "seg.000001", "a", "m", 10, 20,
                    100, 4096, 0x1122334455667788ULL);
        t_fill_meta(&t_man.segments[1], "seg.000002", "n", "z", 21, 30,
                    50, 2048, 0x99AABBCCDDEEFF00ULL);
        assert(manifest_write(&st, "manifest.7.tmp", &t_man) == 0);
        assert(manifest_publish(&st, "manifest.7.tmp", "manifest.7") == 0);
        /* Extension fields survive the round-trip. */
        assert(manifest_read(&st, "manifest.7", &t_man_out) == 0);
        assert(t_man_out.generation == 7);
        assert(t_man_out.checkpoint_seq == 12345);
        assert(t_man_out.schema_version == 3);
        assert(t_man_out.segment_count == 2);
        assert(strcmp(t_man_out.segments[0].obj, "seg.000001") == 0);
        assert(t_man_out.segments[0].first_key_len == 1 &&
               t_man_out.segments[0].first_key[0] == 'a');
        assert(t_man_out.segments[0].last_key_len == 1 &&
               t_man_out.segments[0].last_key[0] == 'm');
        assert(t_man_out.segments[0].min_seq == 10 &&
               t_man_out.segments[0].max_seq == 20);
        assert(t_man_out.segments[0].entry_count == 100);
        assert(t_man_out.segments[0].index_type == 0 &&
               t_man_out.segments[0].model_version == 0);
        assert(t_man_out.segments[0].byte_size == 4096);
        assert(t_man_out.segments[0].checksum == 0x1122334455667788ULL);
        assert(strcmp(t_man_out.segments[1].obj, "seg.000002") == 0);
        assert(t_man_out.segments[1].checksum == 0x99AABBCCDDEEFF00ULL);
        /* CURRENT pointer flow (manifest first, then CURRENT). */
        assert(manifest_write_current(&st, "CURRENT.tmp",
                                      "manifest.7") == 0);
        assert(manifest_publish(&st, "CURRENT.tmp", "CURRENT") == 0);
        assert(manifest_read_current(&st, "CURRENT", cur,
                                     sizeof(cur)) == 0);
        assert(strcmp(cur, "manifest.7") == 0);
        assert(ss.publish_calls == 2 && ss.sync_dir_calls == 2 &&
               ss.sync_obj_calls == 2);
        t_free_store(&ss);
    }
    printf("Passed.\n");
}

void test_manifest_corruption() {
    printf("Running test_manifest_corruption...\n");
    {
        YowzaStore st;
        TStoreState ss;
        uint64_t size = 0;
        t_make_store(&st, &ss);
        memset(&t_man, 0, sizeof(t_man));
        t_man.generation = 1;
        t_man.segment_count = 1;
        t_fill_meta(&t_man.segments[0], "seg.1", "a", "b", 1, 2, 10,
                    100, 42);
        assert(manifest_write(&st, "man.tmp", &t_man) == 0);
        assert(manifest_publish(&st, "man.tmp", "manifest.1") == 0);
        assert(manifest_read(&st, "manifest.1", &t_man_out) == 0);
        /* Corrupt a payload byte: checksum must catch it. */
        t_corrupt(&ss, "manifest.1", 40);
        assert(manifest_read(&st, "manifest.1", &t_man_out) == -1);
        /* Truncation. */
        assert(manifest_write(&st, "man2.tmp", &t_man) == 0);
        assert(manifest_publish(&st, "man2.tmp", "manifest.2") == 0);
        assert(st.vt->size(st.state, "manifest.2", &size) == 0);
        assert(st.vt->truncate(st.state, "manifest.2", size - 1) == 0);
        assert(manifest_read(&st, "manifest.2", &t_man_out) == -1);
        /* Trailing garbage. */
        assert(manifest_write(&st, "man3.tmp", &t_man) == 0);
        assert(manifest_publish(&st, "man3.tmp", "manifest.3") == 0);
        assert(st.vt->size(st.state, "manifest.3", &size) == 0);
        {
            uint8_t junk = 0xAA;
            assert(st.vt->write_at(st.state, "manifest.3", size, &junk, 1) == 0);
        }
        assert(manifest_read(&st, "manifest.3", &t_man_out) == -1);
        /* Missing object. */
        assert(manifest_read(&st, "manifest.99", &t_man_out) == -1);
        t_free_store(&ss);
    }
    printf("Passed.\n");
}

void test_manifest_validation() {
    printf("Running test_manifest_validation...\n");
    {
        YowzaStore st;
        TStoreState ss;
        t_make_store(&st, &ss);
        memset(&t_man, 0, sizeof(t_man));
        t_man.segment_count = 1;
        t_fill_meta(&t_man.segments[0], "seg.1", "a", "b", 1, 2, 10,
                    100, 42);
        t_man.segments[0].index_type = 5; /* unknown: rejected */
        assert(manifest_write(&st, "manv.tmp", &t_man) == -1);
        t_man.segments[0].index_type = 0;
        t_man.segments[0].model_version = 2; /* unknown: rejected */
        assert(manifest_write(&st, "manv.tmp", &t_man) == -1);
        t_man.segments[0].model_version = 0;
        t_man.segment_count = 129; /* over the Phase 2 limit */
        assert(manifest_write(&st, "manv.tmp", &t_man) == -1);
        t_man.segment_count = 1;
        t_man.segments[0].obj[0] = '\0'; /* empty name */
        assert(manifest_write(&st, "manv.tmp", &t_man) == -1);
        t_free_store(&ss);
    }
    printf("Passed.\n");
}

void test_segment_manifest_checksum_link() {
    printf("Running test_segment_manifest_checksum_link...\n");
    {
        YowzaStore st;
        TStoreState ss;
        SegEntry ents[2];
        uint64_t seg_sum = 0, seg_sum2 = 0;
        t_make_store(&st, &ss);
        ents[0] = t_ent("a", "1", 0, 1, 0);
        ents[1] = t_ent("b", "2", 0, 2, 0);
        assert(seg_write(&st, "seg.link", ents, 2, 0, 0, 0, NULL) == 0);
        assert(seg_file_checksum(&st, "seg.link", &seg_sum) == 0);
        memset(&t_man, 0, sizeof(t_man));
        t_man.generation = 3;
        t_man.segment_count = 1;
        t_fill_meta(&t_man.segments[0], "seg.link", "a", "b", 1, 2, 2,
                    0, seg_sum);
        assert(manifest_write(&st, "manl.tmp", &t_man) == 0);
        assert(manifest_publish(&st, "manl.tmp", "manifest.3") == 0);
        assert(manifest_read(&st, "manifest.3", &t_man_out) == 0);
        assert(seg_file_checksum(&st, "seg.link", &seg_sum2) == 0);
        /* Track C's recovery check: manifest checksum == live file. */
        assert(t_man_out.segments[0].checksum == seg_sum2);
        t_free_store(&ss);
    }
    printf("Passed.\n");
}

/* ---------------- main (weak: see file header) ---------------- */

#ifndef YOWZA_SEGMENT_NO_MAIN
__attribute__((weak)) int main(void) {
    printf("Starting Segment/Manifest Tests...\n");
    test_segment_roundtrip();
    test_segment_unsorted_rejected();
    test_segment_block_checksum_corruption();
    test_segment_footer_truncation();
    test_segment_sparse_index_lookup();
    test_segment_bloom_no_false_negative();
    test_segment_iterator_no_whole_load();
    test_segment_empty();
    test_segment_index_checksum_corruption();
    test_segment_art_iterate();
    test_manifest_roundtrip();
    test_manifest_corruption();
    test_manifest_validation();
    test_segment_manifest_checksum_link();
    printf("\nAll Segment/Manifest Tests Passed!\n");
    return 0;
}
#endif
