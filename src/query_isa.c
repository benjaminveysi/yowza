/* Query ISA v1 engine — decodes and executes ISA programs inside the
 * wasm module (and natively, for the test harness build), reading
 * data ONLY through segdb_scan. See include/query_isa.h for the full
 * format specification; this file implements exactly that spec and
 * the Track B reference executor's semantics (query/executor.go).
 *
 * No libc, no malloc: only manual byte loops and the host-provided
 * work buffer (bump allocator). Do not add string.h, memcpy,
 * memset, strcmp, strlen, or struct assignment of values (use
 * qi_vcopy, which copies field by field, so clang never emits a
 * memcpy call on wasm32). */
#include "query_isa.h"
#include "segdb.h"

#ifdef __wasm__
#define QISA_EXPORT(name) __attribute__((export_name(name)))
#else
#define QISA_EXPORT(name)
#endif

/* ---- tunables (caps fail cleanly with QISA_ERR_OOM) ---- */
#define QI_SCAN_BUF 524288u       /* per side cursor page buffer;
                                   * a 64-row page of worst-case
                                   * rows needs 64 x 2055 bytes */
#define QI_PAGE_SIZE 4194304u    /* per-page transient region */
#define QI_WORK_HDR ((2u * QI_SCAN_BUF) + QI_PAGE_SIZE)
#define QI_MAX_WIDTH 64u
#define QI_MAX_ROWS 262144u       /* JOIN build / SORT stored rows */
#define QI_MAX_GROUPS 65536u
#define QI_EXPR_DEPTH 64u
#define QI_BATCH 64u              /* cursor page size == Go batch size */
#define QI_VEC_CHUNK 256u         /* rows per retained-row chunk */

/* ---- resource policy (Gate 4) ---- */
static void qi_policy_resolve(const QisaPolicy* in, QisaPolicy* out) {
    out->max_width = (in && in->max_width != 0u)
                         ? in->max_width
                         : QISA_DEFAULT_WIDTH;
    out->max_join_rows = (in && in->max_join_rows != 0u)
                             ? in->max_join_rows
                             : QISA_DEFAULT_ROWS;
    out->max_sort_rows = (in && in->max_sort_rows != 0u)
                             ? in->max_sort_rows
                             : QISA_DEFAULT_ROWS;
    out->max_groups = (in && in->max_groups != 0u)
                          ? in->max_groups
                          : QISA_DEFAULT_GROUPS;
}

/* ---- byte helpers (manual loops only) ---- */
static void qi_copy(uint8_t* dst, const uint8_t* src, uint32_t len) {
    uint32_t i;
    for (i = 0; i < len; i++) dst[i] = src[i];
}

static void qi_zero(uint8_t* dst, uint32_t len) {
    uint32_t i;
    for (i = 0; i < len; i++) dst[i] = 0;
}

static int qi_eq(const uint8_t* a, const uint8_t* b, uint32_t len) {
    uint32_t i;
    for (i = 0; i < len; i++) if (a[i] != b[i]) return 0;
    return 1;
}

static int qi_starts_with(const uint8_t* s, uint32_t slen,
                          const uint8_t* p, uint32_t plen) {
    if (plen > slen) return 0;
    return qi_eq(s, p, plen);
}

static uint32_t qi_rd32(const uint8_t* p) {
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) |
           ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}

static uint64_t qi_rd64(const uint8_t* p) {
    uint64_t v = 0;
    int i;
    for (i = 0; i < 8; i++) v |= (uint64_t)p[i] << (i * 8);
    return v;
}

static void qi_wr32(uint8_t* p, uint32_t v) {
    p[0] = (uint8_t)(v & 0xFF);
    p[1] = (uint8_t)((v >> 8) & 0xFF);
    p[2] = (uint8_t)((v >> 16) & 0xFF);
    p[3] = (uint8_t)((v >> 24) & 0xFF);
}

static void qi_wr64(uint8_t* p, uint64_t v) {
    int i;
    for (i = 0; i < 8; i++) p[i] = (uint8_t)((v >> (i * 8)) & 0xFF);
}

static uint64_t qi_dbl_bits(double d) {
    union { double d; uint64_t u; } c;
    c.d = d;
    return c.u;
}

static double qi_bits_dbl(uint64_t u) {
    union { double d; uint64_t u; } c;
    c.u = u;
    return c.d;
}

/* ---- bump allocator over the host work buffer ---- */
typedef struct {
    uint8_t* base;
    uint64_t cap;
    uint64_t off;
} QiBump;

static void* qi_bump(QiBump* b, uint64_t n) {
    uint64_t a, p;
    if (n == 0) n = 1;
    a = (b->off + 7u) & ~7u;
    if (a > b->cap || n > b->cap - a) return 0;
    p = a;
    b->off = a + n;
    return b->base + p;
}

static void* qi_bump_zero(QiBump* b, uint64_t n) {
    uint8_t* p = (uint8_t*)qi_bump(b, n);
    if (p) qi_zero(p, (uint32_t)n);
    return p;
}

/* ---- program reader (bounds-checked) ---- */
typedef struct {
    const uint8_t* p;
    uint32_t len;
    uint32_t pos;
    int bad;
} QiRd;

static uint8_t qi_ru8(QiRd* r) {
    uint8_t v;
    if (r->pos + 1u > r->len) { r->bad = 1; return 0; }
    v = r->p[r->pos];
    r->pos += 1u;
    return v;
}

static uint32_t qi_ru32(QiRd* r) {
    uint32_t v;
    if (r->pos + 4u > r->len) { r->bad = 1; return 0; }
    v = qi_rd32(r->p + r->pos);
    r->pos += 4u;
    return v;
}

static uint64_t qi_ru64(QiRd* r) {
    uint64_t v;
    if (r->pos + 8u > r->len) { r->bad = 1; return 0; }
    v = qi_rd64(r->p + r->pos);
    r->pos += 8u;
    return v;
}

static int64_t qi_ri64(QiRd* r) {
    /* Reinterpret the 64 bits as signed via the union idiom (clang
     * lowers this to a register move on both wasm targets). */
    union { uint64_t u; int64_t i; } c;
    c.u = qi_ru64(r);
    return c.i;
}

/* ---- values ---- */
typedef struct {
    uint8_t kind;
    int64_t i;
    double f;
    const uint8_t* s;
    uint32_t len;
} QiVal;

static void qi_vnull(QiVal* v) {
    v->kind = (uint8_t)QISA_KIND_NULL;
    v->i = 0;
    v->f = 0.0;
    v->s = 0;
    v->len = 0;
}

static void qi_vcopy(QiVal* dst, const QiVal* src) {
    dst->kind = src->kind;
    dst->i = src->i;
    dst->f = src->f;
    dst->s = src->s;
    dst->len = src->len;
}

static int qi_is_num(const QiVal* v) {
    return v->kind == (uint8_t)QISA_KIND_INT ||
           v->kind == (uint8_t)QISA_KIND_FLOAT;
}

static double qi_as_dbl(const QiVal* v) {
    if (v->kind == (uint8_t)QISA_KIND_INT) return (double)v->i;
    return v->f;
}

/* Exact int-vs-float comparison (Gate 4, mirrors Go's
 * Compare): an int64 against a float64 without routing the
 * int through double, so 2^53+1 and 2^53+2 stay distinct.
 * NaN compares equal to everything (the pre-Gate-4 float
 * behaviour, preserved); infinities compare beyond int64. */
static int qi_cmp_int_float(int64_t iv, double f) {
    double fl;
    int64_t fv;
    if (f != f) return 0;
    if (f > 9223372036854775807.0) return -1; /* f beyond int64 */
    if (f < -9223372036854775808.0) return 1;
    /* f is in int64 range (note: 2^63 as a double is
     * 9223372036854775808.0, handled above as > max). */
    if (f >= 9223372036854775808.0) return -1;
    /* Truncation toward zero is exact here: f is in range
     * and (int64_t)f is defined. */
    if (f == (double)(int64_t)f) {
        fv = (int64_t)f;
        if (iv < fv) return -1;
        if (iv > fv) return 1;
        return 0;
    }
    /* Fractional f: iv < f iff iv <= floor(f). Floor is in
     * int64 range for every in-range f. */
    fl = f;
    if (fl > 0.0) {
        /* f positive: floor == trunc. */
        fv = (int64_t)f;
    } else {
        /* f negative fractional: floor == trunc - 1. */
        fv = (int64_t)f - 1;
    }
    return iv <= fv ? -1 : 1;
}

/* Compare two non-NULL values: *out = -1/0/+1, or QISA_ERR_TYPE.
 * NULL is never ordered here; callers handle NULL first.
 * Numeric comparison is EXACT (Gate 4): int/int compares as
 * int64, int/float uses qi_cmp_int_float, float/float
 * compares as doubles — exactly like Go's Compare. */
static int qi_cmp(const QiVal* a, const QiVal* b, int* out) {
    uint32_t n, i;
    if (a->kind == (uint8_t)QISA_KIND_NULL ||
        b->kind == (uint8_t)QISA_KIND_NULL) return QISA_ERR_TYPE;
    if (qi_is_num(a) && qi_is_num(b)) {
        if (a->kind == (uint8_t)QISA_KIND_INT &&
            b->kind == (uint8_t)QISA_KIND_INT) {
            if (a->i < b->i) *out = -1;
            else if (a->i > b->i) *out = 1;
            else *out = 0;
            return QISA_OK;
        }
        if (a->kind == (uint8_t)QISA_KIND_INT) {
            *out = qi_cmp_int_float(a->i, b->f);
            return QISA_OK;
        }
        if (b->kind == (uint8_t)QISA_KIND_INT) {
            *out = -qi_cmp_int_float(b->i, a->f);
            return QISA_OK;
        }
        if (a->f < b->f) *out = -1;
        else if (a->f > b->f) *out = 1;
        else *out = 0;
        return QISA_OK;
    }
    if (a->kind != b->kind) return QISA_ERR_TYPE;
    switch (a->kind) {
    case QISA_KIND_STRING:
    case QISA_KIND_BYTES:
        n = a->len < b->len ? a->len : b->len;
        for (i = 0; i < n; i++) {
            if (a->s[i] != b->s[i]) {
                *out = a->s[i] < b->s[i] ? -1 : 1;
                return QISA_OK;
            }
        }
        if (a->len < b->len) *out = -1;
        else if (a->len > b->len) *out = 1;
        else *out = 0;
        return QISA_OK;
    case QISA_KIND_BOOL: {
        /* Bool payload lives in i (0/1); len is unused for bools. */
        int ab = a->i != 0;
        int bb = b->i != 0;
        if (ab == bb) *out = 0;
        else if (!ab && bb) *out = -1;
        else *out = 1;
        return QISA_OK;
    }
    default:
        return QISA_ERR_TYPE;
    }
}

/* Equals: NULL = anything is false (no error); otherwise Compare. */
static int qi_equals(const QiVal* a, const QiVal* b, int* out) {
    int c, rc;
    if (a->kind == (uint8_t)QISA_KIND_NULL ||
        b->kind == (uint8_t)QISA_KIND_NULL) {
        *out = 0;
        return QISA_OK;
    }
    rc = qi_cmp(a, b, &c);
    if (rc != QISA_OK) return rc;
    *out = c == 0;
    return QISA_OK;
}

/* ---- decimal -> double (strict JSON number grammar) ----
 * Precision note (documented in DEEP_ISA_NOTES.md): the first 15
 * significant digits are accumulated exactly; further digits are
 * folded into the decimal exponent. Ordinary values (ages, money,
 * scores) are exact; numbers with >15 significant digits may
 * differ from Go's correctly-rounded strconv parse in the last
 * ulp. Returns 0 ok, -1 malformed, -2 overflow to infinity. */
static int qi_parse_num(const uint8_t* s, uint32_t len, double* out) {
    uint32_t i = 0;
    int neg = 0;
    double mant = 0.0;
    int sig = 0;
    int exp10 = 0;
    int seen_digit = 0;
    int seen_dot_digit = 0;
    double v;
    int e;
    if (len == 0) return -1;
    if (s[i] == '-') { neg = 1; i++; }
    else if (s[i] == '+') { i++; }
    while (i < len && s[i] >= '0' && s[i] <= '9') {
        seen_digit = 1;
        if (sig < 15) {
            mant = mant * 10.0 + (double)(s[i] - '0');
            sig++;
        } else {
            exp10++;
        }
        i++;
    }
    if (!seen_digit) return -1;
    if (i < len && s[i] == '.') {
        i++;
        while (i < len && s[i] >= '0' && s[i] <= '9') {
            seen_dot_digit = 1;
            if (sig < 15) {
                mant = mant * 10.0 + (double)(s[i] - '0');
                sig++;
                exp10--;
            }
            i++;
        }
        if (!seen_dot_digit) return -1;
    }
    if (i < len && (s[i] == 'e' || s[i] == 'E')) {
        int eneg = 0;
        int eval = 0;
        int edigits = 0;
        i++;
        if (i < len && (s[i] == '-' || s[i] == '+')) {
            eneg = s[i] == '-';
            i++;
        }
        while (i < len && s[i] >= '0' && s[i] <= '9') {
            if (eval < 100000) eval = eval * 10 + (s[i] - '0');
            edigits++;
            i++;
        }
        if (!edigits) return -1;
        exp10 += eneg ? -eval : eval;
    }
    if (i != len) return -1;
    v = mant;
    if (exp10 > 400) return -2;
    if (exp10 < -400) {
        v = 0.0;
    } else {
        e = exp10;
        while (e > 0) { v *= 10.0; e--; }
        while (e < 0) { v /= 10.0; e++; }
    }
    if (v != v) return -1; /* NaN cannot come from a JSON number */
    /* Overflow check: a finite mantissa scaled past DBL_MAX yields
     * +inf on IEEE targets; detect by the loop result being
     * larger than any finite double is impossible to spell without
     * float.h, so use the exponent bound: mant < 1e15 and
     * exp10 <= 308 keeps us finite for all ordinary inputs; the
     * exp10 > 400 early return plus scaling overflow to inf is
     * caught by comparing v against itself scaled: if v is inf,
     * v == v * 2 and v != 0. */
    if (v != 0.0 && v == v * 2.0) return -2;
    *out = neg ? -v : v;
    return 0;
}

/* ---- UTF-8 code points (for LIKE) ---- */
static uint32_t qi_utf8_next(const uint8_t* s, uint32_t len, uint32_t pos,
                             uint32_t* cp) {
    uint8_t c;
    uint32_t n, v, k;
    if (pos >= len) return 0;
    c = s[pos];
    if (c < 0x80u) { *cp = c; return 1; }
    if ((c & 0xE0u) == 0xC0u) { n = 2; v = c & 0x1Fu; }
    else if ((c & 0xF0u) == 0xE0u) { n = 3; v = c & 0x0Fu; }
    else if ((c & 0xF8u) == 0xF0u) { n = 4; v = c & 0x07u; }
    else { *cp = 0xFFFDu; return 1; }
    if (pos + n > len) { *cp = 0xFFFDu; return 1; }
    for (k = 1; k < n; k++) {
        if ((s[pos + k] & 0xC0u) != 0x80u) { *cp = 0xFFFDu; return 1; }
        v = (v << 6) | (s[pos + k] & 0x3Fu);
    }
    *cp = v;
    return n;
}

/* SQL LIKE over code points, same backtracking algorithm as Go's
 * matchLike. Returns 1 on match, 0 otherwise. */
static int qi_like(const uint8_t* s, uint32_t slen,
                   const uint8_t* pat, uint32_t plen, QiBump* bump) {
    uint32_t* sc;
    uint32_t* pc;
    uint32_t sn = 0, pn = 0, pos = 0, ppos = 0;
    uint32_t si = 0, pi = 0;
    int starP = -1;
    uint32_t starS = 0;
    uint32_t cp;
    uint32_t adv;
    while (pos < slen) {
        adv = qi_utf8_next(s, slen, pos, &cp);
        if (adv == 0) break;
        sn++;
        pos += adv;
    }
    while (ppos < plen) {
        adv = qi_utf8_next(pat, plen, ppos, &cp);
        if (adv == 0) break;
        pn++;
        ppos += adv;
    }
    sc = (uint32_t*)qi_bump(bump, (uint64_t)(sn + 1u) * 4u);
    pc = (uint32_t*)qi_bump(bump, (uint64_t)(pn + 1u) * 4u);
    if (!sc || !pc) return -1; /* OOM: the caller (eval) converts
     * this to QISA_ERR_OOM, never to a false non-match. */
    pos = 0;
    sn = 0;
    while (pos < slen) {
        adv = qi_utf8_next(s, slen, pos, &cp);
        if (adv == 0) break;
        sc[sn++] = cp;
        pos += adv;
    }
    ppos = 0;
    pn = 0;
    while (ppos < plen) {
        adv = qi_utf8_next(pat, plen, ppos, &cp);
        if (adv == 0) break;
        pc[pn++] = cp;
        ppos += adv;
    }
    while (si < sn) {
        if (pi < pn && (pc[pi] == (uint32_t)'_' || pc[pi] == sc[si])) {
            si++;
            pi++;
        } else if (pi < pn && pc[pi] == (uint32_t)'%') {
            starP = (int)pi;
            starS = si;
            pi++;
        } else if (starP >= 0) {
            pi = (uint32_t)starP + 1u;
            starS++;
            si = starS;
        } else {
            return 0;
        }
    }
    while (pi < pn && pc[pi] == (uint32_t)'%') pi++;
    return pi == pn;
}

/* ---- JSON object field extraction ----
 * Strictness matches Go's encoding/json Unmarshal into a map for
 * the purposes that matter here: the whole value must be exactly
 * one well-formed JSON object (trailing data fails it), every
 * string/number/literal inside must be well-formed, and duplicate
 * keys resolve last-wins. Any failure means "not an object" and
 * the caller yields NULL for every JSON column of the row (Go
 * leaves the field map nil on Unmarshal error). A field that IS
 * present with the wrong JSON type is a TYPE error, decided by
 * the converter, not here. Nesting depth is capped at 64: deeper
 * nesting is treated as "not an object" (documented limit; Go
 * allows 10000, but no realistic row value nests that deep). */

static int qi_json_ws(const uint8_t* s, uint32_t len, uint32_t pos) {
    while (pos < len &&
           (s[pos] == ' ' || s[pos] == '\t' || s[pos] == '\n' ||
            s[pos] == '\r'))
        pos++;
    return (int)pos;
}

static int qi_hexval(uint8_t c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}

/* Encode one code point as UTF-8 into dst (which must hold 4
 * bytes); returns the byte count. */
static uint32_t qi_utf8_encode(uint32_t cp, uint8_t* dst) {
    if (cp < 0x80u) {
        dst[0] = (uint8_t)cp;
        return 1;
    }
    if (cp < 0x800u) {
        dst[0] = (uint8_t)(0xC0u | (cp >> 6));
        dst[1] = (uint8_t)(0x80u | (cp & 0x3Fu));
        return 2;
    }
    if (cp < 0x10000u) {
        dst[0] = (uint8_t)(0xE0u | (cp >> 12));
        dst[1] = (uint8_t)(0x80u | ((cp >> 6) & 0x3Fu));
        dst[2] = (uint8_t)(0x80u | (cp & 0x3Fu));
        return 3;
    }
    dst[0] = (uint8_t)(0xF0u | (cp >> 18));
    dst[1] = (uint8_t)(0x80u | ((cp >> 12) & 0x3Fu));
    dst[2] = (uint8_t)(0x80u | ((cp >> 6) & 0x3Fu));
    dst[3] = (uint8_t)(0x80u | (cp & 0x3Fu));
    return 4;
}

/* Parse one JSON escape at s[pos] == '\\'. On success returns the
 * position just past the escape and sets *cp to the decoded code
 * point (for \" \\ \/ \b \f \n \r \t this is the ASCII byte
 * itself). Surrogate pairs are combined; a lone surrogate yields
 * U+FFFD. Returns -1 on a malformed escape. */
static int qi_json_escape(const uint8_t* s, uint32_t len, uint32_t pos,
                          uint32_t* cp) {
    uint8_t e;
    uint32_t v = 0;
    int k, h;
    if (pos + 1u >= len) return -1;
    e = s[pos + 1u];
    switch (e) {
    case '"': *cp = '"'; return (int)pos + 2;
    case '\\': *cp = '\\'; return (int)pos + 2;
    case '/': *cp = '/'; return (int)pos + 2;
    case 'b': *cp = 8; return (int)pos + 2;
    case 'f': *cp = 12; return (int)pos + 2;
    case 'n': *cp = 10; return (int)pos + 2;
    case 'r': *cp = 13; return (int)pos + 2;
    case 't': *cp = 9; return (int)pos + 2;
    case 'u':
        if (pos + 5u >= len) return -1;
        for (k = 0; k < 4; k++) {
            h = qi_hexval(s[pos + 2u + (uint32_t)k]);
            if (h < 0) return -1;
            v = (v << 4) | (uint32_t)h;
        }
        if (v >= 0xD800u && v <= 0xDBFFu) {
            /* High surrogate: look for a following \uDC00-\uDFFF. */
            uint32_t lo = 0;
            int ok = 0;
            if (pos + 11u < len && s[pos + 6u] == '\\' &&
                s[pos + 7u] == 'u') {
                int j;
                ok = 1;
                for (j = 0; j < 4; j++) {
                    h = qi_hexval(s[pos + 8u + (uint32_t)j]);
                    if (h < 0) { ok = 0; break; }
                    lo = (lo << 4) | (uint32_t)h;
                }
                if (ok && lo >= 0xDC00u && lo <= 0xDFFFu) {
                    *cp = 0x10000u + ((v - 0xD800u) << 10) +
                          (lo - 0xDC00u);
                    return (int)pos + 12;
                }
            }
            *cp = 0xFFFDu;
            return (int)pos + 6;
        }
        if (v >= 0xDC00u && v <= 0xDFFFu) {
            *cp = 0xFFFDu;
            return (int)pos + 6;
        }
        *cp = v;
        return (int)pos + 6;
    default:
        return -1;
    }
}

/* Validate a JSON string starting at s[pos] == '"'. Returns the
 * position just past the closing quote, or -1 if malformed
 * (unterminated, raw control byte, bad escape). */
static int qi_json_string_end(const uint8_t* s, uint32_t len, uint32_t pos) {
    uint32_t cp;
    int nxt;
    if (pos >= len || s[pos] != '"') return -1;
    pos++;
    while (pos < len) {
        if (s[pos] == '"') return (int)pos + 1;
        if (s[pos] < 0x20u) return -1;
        if (s[pos] == '\\') {
            nxt = qi_json_escape(s, len, pos, &cp);
            if (nxt < 0) return -1;
            pos = (uint32_t)nxt;
        } else {
            pos++;
        }
    }
    return -1;
}

/* Compare a raw JSON string body (between the quotes, escapes
 * still present) with a field name, decoding escapes on the fly.
 * Returns 1 if equal, 0 if not, -1 if the string is malformed.
 * This also serves as the key-string validator. */
static int qi_json_key_eq(const uint8_t* s, uint32_t len,
                          const uint8_t* field, uint32_t flen) {
    uint32_t i = 0, fi = 0;
    while (i < len) {
        if (s[i] == '\\') {
            uint32_t cp;
            uint8_t enc[4];
            uint32_t n, k;
            int nxt = qi_json_escape(s, len, i, &cp);
            if (nxt < 0) return -1;
            n = qi_utf8_encode(cp, enc);
            for (k = 0; k < n; k++) {
                if (fi >= flen || field[fi] != enc[k]) return 0;
                fi++;
            }
            i = (uint32_t)nxt;
        } else {
            if (s[i] < 0x20u) return -1;
            if (fi >= flen || field[fi] != s[i]) return 0;
            fi++;
            i++;
        }
    }
    return fi == flen;
}

/* Validate a JSON number starting at pos; returns the position
 * just past it, or -1. Grammar: -?(0|[1-9][0-9]*)(\.[0-9]+)?
 * ([eE][+-]?[0-9]+)? — Go's decoder is slightly more lenient
 * about leading zeros in some paths, but strict JSON numbers are
 * what real producers emit; a non-conforming token fails the
 * whole object here, matching Go's rejection of malformed JSON. */
static int qi_json_number_end(const uint8_t* s, uint32_t len, uint32_t pos) {
    uint32_t i = pos;
    if (i < len && s[i] == '-') i++;
    if (i >= len || s[i] < '0' || s[i] > '9') return -1;
    if (s[i] == '0') {
        i++;
    } else {
        while (i < len && s[i] >= '0' && s[i] <= '9') i++;
    }
    if (i < len && s[i] == '.') {
        i++;
        if (i >= len || s[i] < '0' || s[i] > '9') return -1;
        while (i < len && s[i] >= '0' && s[i] <= '9') i++;
    }
    if (i < len && (s[i] == 'e' || s[i] == 'E')) {
        i++;
        if (i < len && (s[i] == '+' || s[i] == '-')) i++;
        if (i >= len || s[i] < '0' || s[i] > '9') return -1;
        while (i < len && s[i] >= '0' && s[i] <= '9') i++;
    }
    return (int)i;
}

/* Skip one JSON value starting at pos (pos must be at the value,
 * after whitespace). Returns the position just past it, or -1. */
static int qi_json_skip_value(const uint8_t* s, uint32_t len, uint32_t pos,
                              int depth) {
    int nxt;
    if (depth > 64) return -1;
    if (pos >= len) return -1;
    switch (s[pos]) {
    case '"':
        return qi_json_string_end(s, len, pos);
    case '{': {
        pos++;
        pos = (uint32_t)qi_json_ws(s, len, pos);
        if (pos < len && s[pos] == '}') return (int)pos + 1;
        for (;;) {
            int kend;
            pos = (uint32_t)qi_json_ws(s, len, pos);
            if (pos >= len || s[pos] != '"') return -1;
            kend = qi_json_string_end(s, len, pos);
            if (kend < 0) return -1;
            pos = (uint32_t)kend;
            pos = (uint32_t)qi_json_ws(s, len, pos);
            if (pos >= len || s[pos] != ':') return -1;
            pos++;
            pos = (uint32_t)qi_json_ws(s, len, pos);
            nxt = qi_json_skip_value(s, len, pos, depth + 1);
            if (nxt < 0) return -1;
            pos = (uint32_t)nxt;
            pos = (uint32_t)qi_json_ws(s, len, pos);
            if (pos < len && s[pos] == ',') { pos++; continue; }
            if (pos < len && s[pos] == '}') return (int)pos + 1;
            return -1;
        }
    }
    case '[': {
        pos++;
        pos = (uint32_t)qi_json_ws(s, len, pos);
        if (pos < len && s[pos] == ']') return (int)pos + 1;
        for (;;) {
            pos = (uint32_t)qi_json_ws(s, len, pos);
            nxt = qi_json_skip_value(s, len, pos, depth + 1);
            if (nxt < 0) return -1;
            pos = (uint32_t)nxt;
            pos = (uint32_t)qi_json_ws(s, len, pos);
            if (pos < len && s[pos] == ',') { pos++; continue; }
            if (pos < len && s[pos] == ']') return (int)pos + 1;
            return -1;
        }
    }
    case 't':
        if (pos + 4u <= len && qi_eq(s + pos, (const uint8_t*)"true", 4))
            return (int)pos + 4;
        return -1;
    case 'f':
        if (pos + 5u <= len && qi_eq(s + pos, (const uint8_t*)"false", 5))
            return (int)pos + 5;
        return -1;
    case 'n':
        if (pos + 4u <= len && qi_eq(s + pos, (const uint8_t*)"null", 4))
            return (int)pos + 4;
        return -1;
    default:
        if (s[pos] == '-' || (s[pos] >= '0' && s[pos] <= '9'))
            return qi_json_number_end(s, len, pos);
        return -1;
    }
}

/* Token kinds for a captured field value. */
#define QI_TOK_STRING 0
#define QI_TOK_NUMBER 1
#define QI_TOK_BOOL 2
#define QI_TOK_NULL 3
#define QI_TOK_OTHER 4

/* Find a field in a JSON object value. Returns 1 and fills the
 * tok_start, tok_len, and tok_kind outputs when the object is
 * well-formed and the field is present; returns 0 when the value
 * is not a
 * well-formed single object or the field is absent (both mean
 * NULL for the caller, per Go's nil-map behaviour). Last
 * occurrence wins. */
static int qi_json_find(const uint8_t* s, uint32_t len,
                        const uint8_t* field, uint32_t flen,
                        uint32_t* tok_start, uint32_t* tok_len,
                        int* tok_kind) {
    uint32_t pos;
    int found = 0;
    pos = (uint32_t)qi_json_ws(s, len, 0);
    if (pos >= len || s[pos] != '{') return 0;
    pos++;
    pos = (uint32_t)qi_json_ws(s, len, pos);
    if (pos < len && s[pos] == '}') {
        pos++;
        pos = (uint32_t)qi_json_ws(s, len, pos);
        if (pos != len) return 0;
        return 0;
    }
    for (;;) {
        int kend, eq;
        uint32_t kstart, klen, vstart;
        int vend, tk;
        pos = (uint32_t)qi_json_ws(s, len, pos);
        if (pos >= len || s[pos] != '"') return 0;
        kend = qi_json_string_end(s, len, pos);
        if (kend < 0) return 0;
        kstart = pos + 1u;
        klen = (uint32_t)kend - pos - 2u;
        eq = qi_json_key_eq(s + kstart, klen, field, flen);
        if (eq < 0) return 0;
        pos = (uint32_t)kend;
        pos = (uint32_t)qi_json_ws(s, len, pos);
        if (pos >= len || s[pos] != ':') return 0;
        pos++;
        pos = (uint32_t)qi_json_ws(s, len, pos);
        if (pos >= len) return 0;
        vstart = pos;
        switch (s[pos]) {
        case '"': tk = QI_TOK_STRING; break;
        case '{':
        case '[': tk = QI_TOK_OTHER; break;
        case 't':
        case 'f': tk = QI_TOK_BOOL; break;
        case 'n': tk = QI_TOK_NULL; break;
        default: tk = QI_TOK_NUMBER; break;
        }
        vend = qi_json_skip_value(s, len, pos, 0);
        if (vend < 0) return 0;
        if (eq == 1) {
            found = 1;
            *tok_start = vstart;
            *tok_len = (uint32_t)vend - vstart;
            *tok_kind = tk;
        }
        pos = (uint32_t)vend;
        pos = (uint32_t)qi_json_ws(s, len, pos);
        if (pos < len && s[pos] == ',') { pos++; continue; }
        if (pos < len && s[pos] == '}') {
            pos++;
            pos = (uint32_t)qi_json_ws(s, len, pos);
            if (pos != len) return 0;
            break;
        }
        return 0;
    }
    return found;
}

/* Unescape a JSON string body (escapes present) into the bump
 * pool. Returns 0 ok, -1 malformed (cannot happen for a token
 * already validated by qi_json_find, but checked anyway). */
static int qi_json_unescape(const uint8_t* s, uint32_t len, QiBump* bump,
                            const uint8_t** out, uint32_t* out_len) {
    uint8_t* dst = (uint8_t*)qi_bump(bump, len == 0 ? 1u : len);
    uint32_t i = 0, o = 0;
    if (!dst) return -1;
    while (i < len) {
        if (s[i] == '\\') {
            uint32_t cp;
            uint8_t enc[4];
            uint32_t n, k;
            int nxt = qi_json_escape(s, len, i, &cp);
            if (nxt < 0) return -1;
            n = qi_utf8_encode(cp, enc);
            for (k = 0; k < n; k++) dst[o++] = enc[k];
            i = (uint32_t)nxt;
        } else {
            dst[o++] = s[i++];
        }
    }
    *out = dst;
    *out_len = o;
    return 0;
}

/* Convert one JSON field to a typed value. Returns QISA_OK with
 * *out set (possibly NULL for missing/non-object/null field), or
 * QISA_ERR_TYPE for a present field of the wrong type, or
 * QISA_ERR_OOM. */
static int qi_json_field(const uint8_t* val, uint32_t vlen,
                         const uint8_t* field, uint32_t flen,
                         uint8_t want_kind, QiBump* bump, QiVal* out) {
    uint32_t ts, tl;
    int tk;
    int found;
    qi_vnull(out);
    found = qi_json_find(val, vlen, field, flen, &ts, &tl, &tk);
    if (!found) return QISA_OK; /* missing / non-object / malformed */
    if (tk == QI_TOK_NULL) return QISA_OK;
    switch (want_kind) {
    case QISA_KIND_STRING:
    case QISA_KIND_BYTES: {
        const uint8_t* body;
        uint32_t blen;
        const uint8_t* un;
        uint32_t ulen;
        if (tk != QI_TOK_STRING) return QISA_ERR_TYPE;
        body = val + ts + 1u;
        blen = tl - 2u;
        if (qi_json_unescape(body, blen, bump, &un, &ulen) != 0)
            return QISA_ERR_OOM;
        out->kind = want_kind;
        out->s = un;
        out->len = ulen;
        out->i = 0;
        return QISA_OK;
    }
    case QISA_KIND_BOOL: {
        if (tk != QI_TOK_BOOL) return QISA_ERR_TYPE;
        out->kind = (uint8_t)QISA_KIND_BOOL;
        out->i = (tl == 4u) ? 1 : 0; /* "true" is 4 bytes */
        return QISA_OK;
    }
    case QISA_KIND_FLOAT: {
        double d;
        int rc;
        if (tk != QI_TOK_NUMBER) return QISA_ERR_TYPE;
        rc = qi_parse_num(val + ts, tl, &d);
        if (rc != 0) return QISA_ERR_TYPE;
        out->kind = (uint8_t)QISA_KIND_FLOAT;
        out->f = d;
        return QISA_OK;
    }
    case QISA_KIND_INT: {
        double d;
        int rc;
        int64_t iv;
        if (tk != QI_TOK_NUMBER) return QISA_ERR_TYPE;
        rc = qi_parse_num(val + ts, tl, &d);
        if (rc != 0) return QISA_ERR_TYPE;
        if (d < -9223372036854775808.0 || d >= 9223372036854775808.0)
            return QISA_ERR_TYPE; /* absurd magnitude: Go would
             * yield INT64_MIN here; documented divergence, see
             * DEEP_ISA_NOTES.md. Ordinary data never reaches it. */
        iv = (int64_t)d;
        if ((double)iv != d) return QISA_ERR_TYPE;
        out->kind = (uint8_t)QISA_KIND_INT;
        out->i = iv;
        return QISA_OK;
    }
    default:
        return QISA_ERR_TYPE;
    }
}

/* ---- expressions ---- */
typedef struct QiExpr QiExpr;
struct QiExpr {
    uint8_t kind;
    uint8_t op;
    uint8_t negated;
    uint32_t col;
    QiVal lit;
    const uint8_t* pat;
    uint32_t patlen;
    uint32_t nargs;
    QiExpr** args;
    QiExpr* left;
    QiExpr* right;
    QiVal* inlist;
    uint32_t inlen;
};

static int qi_decode_val(QiRd* r, QiVal* v) {
    uint8_t k;
    uint32_t len;
    qi_vnull(v);
    k = qi_ru8(r);
    if (r->bad) return QISA_ERR_MALFORMED;
    if (k > QISA_KIND_BYTES) return QISA_ERR_MALFORMED;
    v->kind = k;
    switch (k) {
    case QISA_KIND_NULL:
        break;
    case QISA_KIND_INT:
        v->i = qi_ri64(r);
        break;
    case QISA_KIND_FLOAT:
        v->f = qi_bits_dbl(qi_ru64(r));
        break;
    case QISA_KIND_STRING:
    case QISA_KIND_BYTES:
        len = qi_ru32(r);
        if (r->bad) return QISA_ERR_MALFORMED;
        if (r->pos + len > r->len) {
            r->bad = 1;
            return QISA_ERR_MALFORMED;
        }
        v->s = r->p + r->pos;
        v->len = len;
        r->pos += len;
        break;
    case QISA_KIND_BOOL: {
        uint8_t b = qi_ru8(r);
        if (r->bad) return QISA_ERR_MALFORMED;
        v->i = b != 0 ? 1 : 0;
        break;
    }
    default:
        return QISA_ERR_MALFORMED;
    }
    if (r->bad) return QISA_ERR_MALFORMED;
    return QISA_OK;
}

static int qi_decode_expr(QiRd* r, QiBump* bump, uint32_t depth,
                          QiExpr** out) {
    uint8_t tag;
    QiExpr* e;
    uint32_t i, n;
    int rc;
    if (depth > QI_EXPR_DEPTH) return QISA_ERR_MALFORMED;
    tag = qi_ru8(r);
    if (r->bad) return QISA_ERR_MALFORMED;
    e = (QiExpr*)qi_bump_zero(bump, (uint64_t)sizeof(QiExpr));
    if (!e) return QISA_ERR_OOM;
    e->kind = tag;
    *out = e;
    switch (tag) {
    case QISA_EXPR_AND:
    case QISA_EXPR_OR:
        n = qi_ru32(r);
        if (r->bad) return QISA_ERR_MALFORMED;
        if (n > 4096u) return QISA_ERR_MALFORMED;
        e->nargs = n;
        if (n > 0) {
            e->args = (QiExpr**)qi_bump(bump, (uint64_t)n * 8u);
            if (!e->args) return QISA_ERR_OOM;
            for (i = 0; i < n; i++) {
                rc = qi_decode_expr(r, bump, depth + 1u, &e->args[i]);
                if (rc != QISA_OK) return rc;
            }
        }
        return QISA_OK;
    case QISA_EXPR_NOT:
        rc = qi_decode_expr(r, bump, depth + 1u, &e->left);
        return rc;
    case QISA_EXPR_CMP:
        e->op = qi_ru8(r);
        if (r->bad) return QISA_ERR_MALFORMED;
        if (e->op > QISA_CMP_GE) return QISA_ERR_MALFORMED;
        rc = qi_decode_expr(r, bump, depth + 1u, &e->left);
        if (rc != QISA_OK) return rc;
        rc = qi_decode_expr(r, bump, depth + 1u, &e->right);
        if (rc != QISA_OK) return rc;
        if ((e->left->kind != QISA_EXPR_COL &&
             e->left->kind != QISA_EXPR_LIT) ||
            (e->right->kind != QISA_EXPR_COL &&
             e->right->kind != QISA_EXPR_LIT))
            return QISA_ERR_MALFORMED;
        return QISA_OK;
    case QISA_EXPR_IN:
        rc = qi_decode_expr(r, bump, depth + 1u, &e->left);
        if (rc != QISA_OK) return rc;
        if (e->left->kind != QISA_EXPR_COL &&
            e->left->kind != QISA_EXPR_LIT)
            return QISA_ERR_MALFORMED;
        e->negated = qi_ru8(r);
        if (r->bad) return QISA_ERR_MALFORMED;
        n = qi_ru32(r);
        if (r->bad) return QISA_ERR_MALFORMED;
        if (n > 4096u) return QISA_ERR_MALFORMED;
        e->inlen = n;
        if (n > 0) {
            e->inlist = (QiVal*)qi_bump(bump, (uint64_t)n *
                                                (uint64_t)sizeof(QiVal));
            if (!e->inlist) return QISA_ERR_OOM;
            for (i = 0; i < n; i++) {
                rc = qi_decode_val(r, &e->inlist[i]);
                if (rc != QISA_OK) return rc;
            }
        }
        return QISA_OK;
    case QISA_EXPR_LIKE:
        rc = qi_decode_expr(r, bump, depth + 1u, &e->left);
        if (rc != QISA_OK) return rc;
        if (e->left->kind != QISA_EXPR_COL &&
            e->left->kind != QISA_EXPR_LIT)
            return QISA_ERR_MALFORMED;
        e->negated = qi_ru8(r);
        if (r->bad) return QISA_ERR_MALFORMED;
        e->patlen = qi_ru32(r);
        if (r->bad) return QISA_ERR_MALFORMED;
        if (r->pos + e->patlen > r->len) {
            r->bad = 1;
            return QISA_ERR_MALFORMED;
        }
        e->pat = r->p + r->pos;
        r->pos += e->patlen;
        return QISA_OK;
    case QISA_EXPR_ISNULL:
        rc = qi_decode_expr(r, bump, depth + 1u, &e->left);
        if (rc != QISA_OK) return rc;
        if (e->left->kind != QISA_EXPR_COL &&
            e->left->kind != QISA_EXPR_LIT)
            return QISA_ERR_MALFORMED;
        e->negated = qi_ru8(r);
        if (r->bad) return QISA_ERR_MALFORMED;
        return QISA_OK;
    case QISA_EXPR_COL:
        e->col = qi_ru32(r);
        if (r->bad) return QISA_ERR_MALFORMED;
        return QISA_OK;
    case QISA_EXPR_LIT:
        return qi_decode_val(r, &e->lit);
    default:
        return QISA_ERR_MALFORMED;
    }
}

/* Validate that every COL node in an expression tree refers to a
 * pipeline column index < width. */
static int qi_expr_cols_ok(QiExpr* e, uint32_t width) {
    uint32_t i;
    if (!e) return 1;
    switch (e->kind) {
    case QISA_EXPR_COL:
        return e->col < width;
    case QISA_EXPR_AND:
    case QISA_EXPR_OR:
        for (i = 0; i < e->nargs; i++)
            if (!qi_expr_cols_ok(e->args[i], width)) return 0;
        return 1;
    case QISA_EXPR_NOT:
        return qi_expr_cols_ok(e->left, width);
    case QISA_EXPR_CMP:
        return qi_expr_cols_ok(e->left, width) &&
               qi_expr_cols_ok(e->right, width);
    case QISA_EXPR_IN:
    case QISA_EXPR_LIKE:
    case QISA_EXPR_ISNULL:
        return qi_expr_cols_ok(e->left, width);
    default:
        return 1;
    }
}

#define QI_TRI_FALSE 0
#define QI_TRI_TRUE 1
#define QI_TRI_UNKNOWN 2

static int qi_eval_operand(QiExpr* e, const QiVal* row, uint32_t width,
                           QiVal* out) {
    if (e->kind == QISA_EXPR_COL) {
        if (e->col >= width) return QISA_ERR_MALFORMED;
        qi_vcopy(out, &row[e->col]);
        return QISA_OK;
    }
    if (e->kind == QISA_EXPR_LIT) {
        qi_vcopy(out, &e->lit);
        return QISA_OK;
    }
    return QISA_ERR_MALFORMED;
}

static int qi_eval_tri(QiExpr* e, const QiVal* row, uint32_t width,
                       QiBump* bump, int* out) {
    int rc, i, c, eq, has_null, unknown;
    QiVal lv, rv;
    switch (e->kind) {
    case QISA_EXPR_AND:
        unknown = 0;
        for (i = 0; (uint32_t)i < e->nargs; i++) {
            rc = qi_eval_tri(e->args[i], row, width, bump, out);
            if (rc != QISA_OK) return rc;
            if (*out == QI_TRI_FALSE) return QISA_OK;
            if (*out == QI_TRI_UNKNOWN) unknown = 1;
        }
        *out = unknown ? QI_TRI_UNKNOWN : QI_TRI_TRUE;
        return QISA_OK;
    case QISA_EXPR_OR:
        unknown = 0;
        for (i = 0; (uint32_t)i < e->nargs; i++) {
            rc = qi_eval_tri(e->args[i], row, width, bump, out);
            if (rc != QISA_OK) return rc;
            if (*out == QI_TRI_TRUE) return QISA_OK;
            if (*out == QI_TRI_UNKNOWN) unknown = 1;
        }
        *out = unknown ? QI_TRI_UNKNOWN : QI_TRI_FALSE;
        return QISA_OK;
    case QISA_EXPR_NOT:
        rc = qi_eval_tri(e->left, row, width, bump, out);
        if (rc != QISA_OK) return rc;
        if (*out == QI_TRI_TRUE) *out = QI_TRI_FALSE;
        else if (*out == QI_TRI_FALSE) *out = QI_TRI_TRUE;
        return QISA_OK;
    case QISA_EXPR_CMP:
        rc = qi_eval_operand(e->left, row, width, &lv);
        if (rc != QISA_OK) return rc;
        rc = qi_eval_operand(e->right, row, width, &rv);
        if (rc != QISA_OK) return rc;
        if (lv.kind == (uint8_t)QISA_KIND_NULL ||
            rv.kind == (uint8_t)QISA_KIND_NULL) {
            *out = QI_TRI_UNKNOWN;
            return QISA_OK;
        }
        rc = qi_cmp(&lv, &rv, &c);
        if (rc != QISA_OK) return rc;
        switch (e->op) {
        case QISA_CMP_EQ: *out = c == 0 ? QI_TRI_TRUE : QI_TRI_FALSE; break;
        case QISA_CMP_NE: *out = c != 0 ? QI_TRI_TRUE : QI_TRI_FALSE; break;
        case QISA_CMP_LT: *out = c < 0 ? QI_TRI_TRUE : QI_TRI_FALSE; break;
        case QISA_CMP_LE: *out = c <= 0 ? QI_TRI_TRUE : QI_TRI_FALSE; break;
        case QISA_CMP_GT: *out = c > 0 ? QI_TRI_TRUE : QI_TRI_FALSE; break;
        case QISA_CMP_GE: *out = c >= 0 ? QI_TRI_TRUE : QI_TRI_FALSE; break;
        default: return QISA_ERR_MALFORMED;
        }
        return QISA_OK;
    case QISA_EXPR_IN:
        rc = qi_eval_operand(e->left, row, width, &lv);
        if (rc != QISA_OK) return rc;
        if (lv.kind == (uint8_t)QISA_KIND_NULL) {
            *out = QI_TRI_UNKNOWN;
            return QISA_OK;
        }
        has_null = 0;
        for (i = 0; (uint32_t)i < e->inlen; i++) {
            if (e->inlist[i].kind == (uint8_t)QISA_KIND_NULL) {
                has_null = 1;
                continue;
            }
            rc = qi_equals(&lv, &e->inlist[i], &eq);
            if (rc != QISA_OK) return rc;
            if (eq) {
                *out = e->negated ? QI_TRI_FALSE : QI_TRI_TRUE;
                return QISA_OK;
            }
        }
        if (has_null) {
            *out = QI_TRI_UNKNOWN;
            return QISA_OK;
        }
        *out = e->negated ? QI_TRI_TRUE : QI_TRI_FALSE;
        return QISA_OK;
    case QISA_EXPR_LIKE: {
        int m;
        rc = qi_eval_operand(e->left, row, width, &lv);
        if (rc != QISA_OK) return rc;
        if (lv.kind == (uint8_t)QISA_KIND_NULL) {
            *out = QI_TRI_UNKNOWN;
            return QISA_OK;
        }
        if (lv.kind != (uint8_t)QISA_KIND_STRING) return QISA_ERR_TYPE;
        m = qi_like(lv.s, lv.len, e->pat, e->patlen, bump);
        if (m < 0) return QISA_ERR_OOM;
        if (e->negated) m = !m;
        *out = m ? QI_TRI_TRUE : QI_TRI_FALSE;
        return QISA_OK;
    }
    case QISA_EXPR_ISNULL: {
        int is_null;
        rc = qi_eval_operand(e->left, row, width, &lv);
        if (rc != QISA_OK) return rc;
        is_null = lv.kind == (uint8_t)QISA_KIND_NULL;
        if (e->negated) is_null = !is_null;
        *out = is_null ? QI_TRI_TRUE : QI_TRI_FALSE;
        return QISA_OK;
    }
    default:
        return QISA_ERR_MALFORMED;
    }
}

/* ---- program model ---- */
typedef struct {
    uint8_t type;
    const uint8_t* src;
    uint32_t srclen;
    const uint8_t* name;
    uint32_t namelen;
} QiCol;

typedef struct {
    const uint8_t* alias;
    uint32_t alen;
    const uint8_t* prefix;
    uint32_t plen;
    uint32_t ncols;
    QiCol* cols;
} QiTable;

typedef struct {
    uint8_t agg;
    uint8_t star;
    uint32_t col;
} QiAgg;

typedef struct {
    uint32_t col;
    uint8_t desc;
} QiSortKey;

typedef struct {
    uint32_t ntables;
    QiTable tables[2];
    uint64_t max_scanned;
    uint64_t max_result;
    uint64_t max_memory;
    int has_join;
    uint32_t join_left;
    uint32_t join_right;
    QiExpr* filter;
    int has_agg;
    uint32_t ngroup;
    uint32_t* group_cols;
    uint32_t nagg;
    QiAgg* aggs;
    uint32_t nproj;
    uint32_t* proj_cols;
    const uint8_t** proj_names;
    uint32_t* proj_namelen;
    uint32_t nsort;
    QiSortKey* sort_keys;
    int has_limit;
    uint64_t limit;
} QiProg;

static int qi_parse_prog(QiRd* r, QiBump* bump, QiProg* p,
                         const QisaPolicy* pol) {
    uint32_t i, j, stage, n;
    int seen_scan = 0;
    int seen_proj = 0;
    uint8_t op;
    p->ntables = 0;
    p->has_join = 0;
    p->filter = 0;
    p->has_agg = 0;
    p->ngroup = 0;
    p->group_cols = 0;
    p->nagg = 0;
    p->aggs = 0;
    p->nproj = 0;
    p->proj_cols = 0;
    p->proj_names = 0;
    p->proj_namelen = 0;
    p->nsort = 0;
    p->sort_keys = 0;
    p->has_limit = 0;
    p->limit = 0;
    p->max_scanned = 0;
    p->max_result = 0;
    p->max_memory = 0;
    if (r->pos + 4u > r->len) return QISA_ERR_MALFORMED;
    if (r->p[r->pos] != QISA_MAGIC_0 || r->p[r->pos + 1u] != QISA_MAGIC_1 ||
        r->p[r->pos + 2u] != QISA_MAGIC_2 ||
        r->p[r->pos + 3u] != QISA_MAGIC_3)
        return QISA_ERR_MALFORMED;
    r->pos += 4u;
    if (qi_ru32(r) != QISA_VERSION) return QISA_ERR_VERSION;
    if (r->bad) return QISA_ERR_MALFORMED;
    p->max_scanned = qi_ru64(r);
    p->max_result = qi_ru64(r);
    p->max_memory = qi_ru64(r);
    p->ntables = qi_ru32(r);
    if (r->bad) return QISA_ERR_MALFORMED;
    if (p->ntables < 1u || p->ntables > 2u) return QISA_ERR_MALFORMED;
    for (i = 0; i < p->ntables; i++) {
        QiTable* t = &p->tables[i];
        t->alen = qi_ru32(r);
        if (r->bad) return QISA_ERR_MALFORMED;
        if (r->pos + t->alen > r->len) return QISA_ERR_MALFORMED;
        t->alias = r->p + r->pos;
        r->pos += t->alen;
        t->plen = qi_ru32(r);
        if (r->bad) return QISA_ERR_MALFORMED;
        if (r->pos + t->plen > r->len) return QISA_ERR_MALFORMED;
        t->prefix = r->p + r->pos;
        r->pos += t->plen;
        t->ncols = qi_ru32(r);
        if (r->bad) return QISA_ERR_MALFORMED;
        if (t->ncols > pol->max_width) return QISA_ERR_LIMIT;
        if (t->ncols > 0) {
            t->cols = (QiCol*)qi_bump(bump, (uint64_t)t->ncols *
                                               (uint64_t)sizeof(QiCol));
            if (!t->cols) return QISA_ERR_OOM;
            for (j = 0; j < t->ncols; j++) {
                uint32_t nl = qi_ru32(r);
                if (r->bad) return QISA_ERR_MALFORMED;
                if (r->pos + nl > r->len) return QISA_ERR_MALFORMED;
                t->cols[j].name = r->p + r->pos;
                t->cols[j].namelen = nl;
                r->pos += nl;
                t->cols[j].type = qi_ru8(r);
                if (r->bad) return QISA_ERR_MALFORMED;
                if (t->cols[j].type > QISA_KIND_BYTES)
                    return QISA_ERR_MALFORMED;
                t->cols[j].srclen = qi_ru32(r);
                if (r->bad) return QISA_ERR_MALFORMED;
                if (r->pos + t->cols[j].srclen > r->len)
                    return QISA_ERR_MALFORMED;
                t->cols[j].src = r->p + r->pos;
                r->pos += t->cols[j].srclen;
            }
        } else {
            t->cols = 0;
        }
    }
    /* Op stream: stages must strictly increase:
     * SCAN 0, JOIN 1, FILTER 2, AGGREGATE 3, PROJECT 4, SORT 5,
     * LIMIT 6, END 7. SCAN and PROJECT are both mandatory. */
    stage = 0;
    for (;;) {
        uint32_t st;
        if (r->pos >= r->len) return QISA_ERR_MALFORMED;
        op = qi_ru8(r);
        if (r->bad) return QISA_ERR_MALFORMED;
        switch (op) {
        case QISA_OP_SCAN: st = 0; break;
        case QISA_OP_JOIN: st = 1; break;
        case QISA_OP_FILTER: st = 2; break;
        case QISA_OP_AGGREGATE: st = 3; break;
        case QISA_OP_PROJECT: st = 4; break;
        case QISA_OP_SORT: st = 5; break;
        case QISA_OP_LIMIT: st = 6; break;
        case QISA_OP_END: st = 7; break;
        default: return QISA_ERR_MALFORMED;
        }
        if (op == QISA_OP_END) {
            if (!seen_scan || !seen_proj)
                return QISA_ERR_MALFORMED; /* no SCAN/PROJECT */
            if (r->pos != r->len) return QISA_ERR_MALFORMED;
            return QISA_OK;
        }
        if (st < stage) return QISA_ERR_MALFORMED;
        if (op == QISA_OP_SCAN && stage != 0u) return QISA_ERR_MALFORMED;
        stage = st + 1u;
        switch (op) {
        case QISA_OP_SCAN: {
            uint32_t tbl = qi_ru32(r);
            if (r->bad) return QISA_ERR_MALFORMED;
            if (tbl != 0u) return QISA_ERR_MALFORMED;
            seen_scan = 1;
            break;
        }
        case QISA_OP_JOIN: {
            uint32_t rt;
            if (p->ntables != 2u) return QISA_ERR_MALFORMED;
            p->has_join = 1;
            p->join_left = qi_ru32(r);
            rt = qi_ru32(r);
            p->join_right = qi_ru32(r);
            if (r->bad) return QISA_ERR_MALFORMED;
            if (rt != 1u) return QISA_ERR_MALFORMED;
            break;
        }
        case QISA_OP_FILTER: {
            int rc = qi_decode_expr(r, bump, 0, &p->filter);
            if (rc != QISA_OK) return rc;
            break;
        }
        case QISA_OP_AGGREGATE: {
            p->has_agg = 1;
            p->ngroup = qi_ru32(r);
            if (r->bad) return QISA_ERR_MALFORMED;
            if (p->ngroup > pol->max_width) return QISA_ERR_LIMIT;
            if (p->ngroup > 0) {
                p->group_cols = (uint32_t*)qi_bump(
                    bump, (uint64_t)p->ngroup * 4u);
                if (!p->group_cols) return QISA_ERR_OOM;
                for (i = 0; i < p->ngroup; i++) {
                    p->group_cols[i] = qi_ru32(r);
                    if (r->bad) return QISA_ERR_MALFORMED;
                }
            }
            p->nagg = qi_ru32(r);
            if (r->bad) return QISA_ERR_MALFORMED;
            if (p->nagg > pol->max_width) return QISA_ERR_LIMIT;
            if (p->nagg > 0) {
                p->aggs = (QiAgg*)qi_bump(bump, (uint64_t)p->nagg *
                                                    (uint64_t)sizeof(QiAgg));
                if (!p->aggs) return QISA_ERR_OOM;
                for (i = 0; i < p->nagg; i++) {
                    p->aggs[i].agg = qi_ru8(r);
                    p->aggs[i].star = qi_ru8(r);
                    p->aggs[i].col = qi_ru32(r);
                    if (r->bad) return QISA_ERR_MALFORMED;
                    if (p->aggs[i].agg > QISA_AGG_MAX)
                        return QISA_ERR_MALFORMED;
                }
            }
            break;
        }
        case QISA_OP_PROJECT: {
            seen_proj = 1;
            p->nproj = qi_ru32(r);
            if (r->bad) return QISA_ERR_MALFORMED;
            if (p->nproj == 0u) return QISA_ERR_MALFORMED;
            if (p->nproj > pol->max_width) return QISA_ERR_LIMIT;
            p->proj_cols = (uint32_t*)qi_bump(bump, (uint64_t)p->nproj * 4u);
            p->proj_names = (const uint8_t**)qi_bump(
                bump, (uint64_t)p->nproj * 8u);
            p->proj_namelen = (uint32_t*)qi_bump(
                bump, (uint64_t)p->nproj * 4u);
            if (!p->proj_cols || !p->proj_names || !p->proj_namelen)
                return QISA_ERR_OOM;
            for (i = 0; i < p->nproj; i++) {
                p->proj_cols[i] = qi_ru32(r);
                if (r->bad) return QISA_ERR_MALFORMED;
            }
            for (i = 0; i < p->nproj; i++) {
                uint32_t nl = qi_ru32(r);
                if (r->bad) return QISA_ERR_MALFORMED;
                if (r->pos + nl > r->len) return QISA_ERR_MALFORMED;
                p->proj_names[i] = r->p + r->pos;
                p->proj_namelen[i] = nl;
                r->pos += nl;
            }
            break;
        }
        case QISA_OP_SORT: {
            p->nsort = qi_ru32(r);
            if (r->bad) return QISA_ERR_MALFORMED;
            if (p->nsort > pol->max_width) return QISA_ERR_LIMIT;
            if (p->nsort > 0) {
                p->sort_keys = (QiSortKey*)qi_bump(
                    bump, (uint64_t)p->nsort * (uint64_t)sizeof(QiSortKey));
                if (!p->sort_keys) return QISA_ERR_OOM;
                for (i = 0; i < p->nsort; i++) {
                    p->sort_keys[i].col = qi_ru32(r);
                    p->sort_keys[i].desc = qi_ru8(r);
                    if (r->bad) return QISA_ERR_MALFORMED;
                }
            }
            break;
        }
        case QISA_OP_LIMIT: {
            p->has_limit = 1;
            p->limit = qi_ru64(r);
            if (r->bad) return QISA_ERR_MALFORMED;
            break;
        }
        default:
            return QISA_ERR_MALFORMED;
        }
        (void)n;
    }
}

/* Post-parse index validation against the pipeline widths. */
static int qi_prog_validate(QiProg* p, const QisaPolicy* pol) {
    uint32_t total_w, agg_w, i;
    total_w = 0;
    for (i = 0; i < p->ntables; i++) total_w += p->tables[i].ncols;
    if (total_w == 0u) return QISA_ERR_OOM;
    if (total_w > pol->max_width) return QISA_ERR_LIMIT;
    if (p->has_join) {
        if (p->join_left >= total_w || p->join_right >= total_w)
            return QISA_ERR_MALFORMED;
    }
    if (p->filter && !qi_expr_cols_ok(p->filter, total_w))
        return QISA_ERR_MALFORMED;
    if (p->has_agg) {
        for (i = 0; i < p->ngroup; i++)
            if (p->group_cols[i] >= total_w) return QISA_ERR_MALFORMED;
        for (i = 0; i < p->nagg; i++) {
            if (!p->aggs[i].star && p->aggs[i].col >= total_w)
                return QISA_ERR_MALFORMED;
            if (p->aggs[i].star && p->aggs[i].agg != QISA_AGG_COUNT)
                return QISA_ERR_MALFORMED;
        }
        agg_w = p->ngroup + p->nagg;
        for (i = 0; i < p->nproj; i++)
            if (p->proj_cols[i] >= agg_w) return QISA_ERR_MALFORMED;
    } else {
        for (i = 0; i < p->nproj; i++)
            if (p->proj_cols[i] >= total_w) return QISA_ERR_MALFORMED;
    }
    for (i = 0; i < p->nsort; i++)
        if (p->sort_keys[i].col >= p->nproj) return QISA_ERR_MALFORMED;
    return QISA_OK;
}

/* ---- materialization ---- */
static int qi_materialize_row(QiTable* t, const uint8_t* key,
                              uint32_t klen, const uint8_t* val,
                              uint32_t vlen, QiBump* bump, QiVal* out) {
    uint32_t i;
    for (i = 0; i < t->ncols; i++) {
        QiCol* c = &t->cols[i];
        QiVal* v = &out[i];
        qi_vnull(v);
        if (c->srclen == 3u && qi_eq(c->src, (const uint8_t*)"key", 3)) {
            v->kind = (uint8_t)QISA_KIND_STRING;
            v->s = key;
            v->len = klen;
        } else if (c->srclen == 6u &&
                   qi_eq(c->src, (const uint8_t*)"suffix", 6)) {
            v->kind = (uint8_t)QISA_KIND_STRING;
            if (qi_starts_with(key, klen, t->prefix, t->plen)) {
                v->s = key + t->plen;
                v->len = klen - t->plen;
            } else {
                v->s = key;
                v->len = klen;
            }
        } else if (c->srclen == 5u &&
                   qi_eq(c->src, (const uint8_t*)"value", 5)) {
            if (c->type == QISA_KIND_STRING) {
                v->kind = (uint8_t)QISA_KIND_STRING;
                v->s = val;
                v->len = vlen;
            } else if (c->type == QISA_KIND_BYTES) {
                v->kind = (uint8_t)QISA_KIND_BYTES;
                v->s = val;
                v->len = vlen;
            } else {
                return QISA_ERR_TYPE;
            }
        } else if (c->srclen > 5u &&
                   qi_eq(c->src, (const uint8_t*)"json:", 5)) {
            int rc = qi_json_field(val, vlen, c->src + 5u,
                                   c->srclen - 5u, c->type, bump, v);
            if (rc != QISA_OK) return rc;
        } else {
            return QISA_ERR_MALFORMED;
        }
    }
    return QISA_OK;
}

/* ---- unified contract helpers (item 02) ----
 * Memory accounting is Go's rowSize exactly (executor.go):
 * row = 32 + sum(len(colname) + valueSize), valueSize NULL 0,
 * INT/FLOAT 8, BOOL 1, STRING/BYTES len. Pipeline rows are named
 * alias.column; projected rows are named by PROJECT output name.
 * The name-length arrays are built by the caller in pipeline /
 * projected order. */
static uint64_t qi_value_size(const QiVal* v) {
    switch (v->kind) {
    case QISA_KIND_INT:
    case QISA_KIND_FLOAT:
        return 8u;
    case QISA_KIND_BOOL:
        return 1u;
    case QISA_KIND_STRING:
    case QISA_KIND_BYTES:
        return (uint64_t)v->len;
    default:
        return 0u;
    }
}

static uint64_t qi_row_size(const QiVal* row, uint32_t width,
                            const uint32_t* name_lens) {
    uint64_t sz = 32u;
    uint32_t i;
    for (i = 0; i < width; i++)
        sz += (uint64_t)name_lens[i] + qi_value_size(&row[i]);
    return sz;
}

/* Deep-copy one value's string/bytes payload into the persistent
 * bump; other kinds copy by value. Returns QISA_ERR_OOM when the
 * payload does not fit. */
static int qi_vcopy_persist(QiVal* dst, const QiVal* src, QiBump* bump) {
    qi_vcopy(dst, src);
    if (src->kind == (uint8_t)QISA_KIND_STRING ||
        src->kind == (uint8_t)QISA_KIND_BYTES) {
        uint8_t* cp;
        if (src->len == 0u) {
            dst->s = src->s;
            return QISA_OK;
        }
        cp = (uint8_t*)qi_bump(bump, src->len);
        if (!cp) return QISA_ERR_OOM;
        qi_copy(cp, src->s, src->len);
        dst->s = cp;
    }
    return QISA_OK;
}

/* Retained-row vector: chunked rows in the persistent bump, so
 * retained rows never move (string pointers stay valid) and no
 * doubling copy is needed. Chunk pointers live in the bump too. */
typedef struct {
    QiVal** chunks;
    uint32_t nchunks;
    uint32_t cap_chunks;
    uint32_t nrows;
    uint32_t width;
} QiVec;

static QiVal* qi_vec_row(QiVec* v, uint32_t idx) {
    return v->chunks[idx / QI_VEC_CHUNK] +
           (uint64_t)(idx % QI_VEC_CHUNK) * v->width;
}

/* Append one transient row, deep-copying string payloads into the
 * persistent bump. The transient row's pointers may point into a
 * page buffer or the page region; after this call the stored row
 * is independent of both. */
static int qi_vec_append(QiVec* v, const QiVal* row, QiBump* bump) {
    uint32_t slot, c;
    QiVal* dst;
    if (v->nrows % QI_VEC_CHUNK == 0u) {
        QiVal* chunk;
        if (v->nchunks >= v->cap_chunks) {
            uint32_t ncap = v->cap_chunks == 0u ? 16u : v->cap_chunks * 2u;
            QiVal** nptr = (QiVal**)qi_bump(
                bump, (uint64_t)ncap * 8u);
            uint32_t k;
            if (!nptr) return QISA_ERR_OOM;
            for (k = 0; k < v->nchunks; k++) nptr[k] = v->chunks[k];
            v->chunks = nptr;
            v->cap_chunks = ncap;
        }
        chunk = (QiVal*)qi_bump(bump, (uint64_t)QI_VEC_CHUNK *
                                           (uint64_t)v->width *
                                           (uint64_t)sizeof(QiVal));
        if (!chunk) return QISA_ERR_OOM;
        v->chunks[v->nchunks++] = chunk;
    }
    slot = v->nrows % QI_VEC_CHUNK;
    dst = v->chunks[v->nchunks - 1u] + (uint64_t)slot * v->width;
    for (c = 0; c < v->width; c++) {
        int rc = qi_vcopy_persist(&dst[c], &row[c], bump);
        if (rc != QISA_OK) return rc;
    }
    v->nrows++;
    return QISA_OK;
}

/* One cursor page at the query snapshot. cursor_key/cursor_len/
 * has_cursor are updated for the next call (cursor = last
 * written key while has_more is set). Returns QISA_OK,
 * QISA_ERR_SCAN, or a materialization error. A -3 (row does
 * not fit) cannot occur for a 64-row page in a 512 KiB buffer
 * (64 x 2055 < 512 KiB) and is treated as a scan error. */
typedef struct {
    uint8_t key[1024];
    uint32_t len;
    int has;
    int done;
} QiCursor;

static int qi_cursor_page(void* state, QiTable* t, uint64_t snapshot,
                          QiCursor* cur, uint8_t* scanbuf,
                          uint32_t* count_out) {
    uint32_t count = 0;
    uint8_t next_key[1024];
    uint32_t next_len = 0;
    uint32_t has_more = 0;
    int rc;
    if (cur->done) {
        *count_out = 0;
        return QISA_OK;
    }
    rc = segdb_scan_cursor((SegDB*)state, t->prefix, t->plen, snapshot,
                           cur->key, cur->len, cur->has ? 1 : 0,
                           QI_BATCH, scanbuf, QI_SCAN_BUF, &count,
                           next_key, 1024u, &next_len, &has_more);
    if (rc != 0) return QISA_ERR_SCAN;
    if (count > QI_BATCH) return QISA_ERR_SCAN;
    if (has_more) {
        if (next_len > 1024u) return QISA_ERR_SCAN;
        qi_copy(cur->key, next_key, next_len);
        cur->len = next_len;
        cur->has = 1;
    } else {
        cur->done = 1;
    }
    *count_out = count;
    return QISA_OK;
}

/* Materialize one cursor page (art_scan format in scanbuf) into
 * transient rows in the page bump. String pointers point into
 * scanbuf (key/suffix/value sources) or the page bump (JSON
 * unescape); both are valid only until the next page load, so
 * callers that retain rows must deep-copy (qi_vec_append) before
 * loading the next page. */
static int qi_materialize_page(QiTable* t, uint8_t* scanbuf,
                               uint32_t count, QiBump* page_bump,
                               QiVal** rows_out) {
    uint32_t off = 0;
    uint32_t i;
    QiVal* rows;
    int rc;
    if (count == 0u) {
        *rows_out = 0;
        return QISA_OK;
    }
    rows = (QiVal*)qi_bump(page_bump, (uint64_t)count *
                                         (uint64_t)t->ncols *
                                         (uint64_t)sizeof(QiVal));
    if (!rows) return QISA_ERR_OOM;
    for (i = 0; i < count; i++) {
        uint32_t klen, vlen;
        const uint8_t* key;
        const uint8_t* val;
        if (off + 4u > QI_SCAN_BUF) return QISA_ERR_SCAN;
        klen = qi_rd32(scanbuf + off);
        off += 4u;
        if (off + klen + 4u > QI_SCAN_BUF) return QISA_ERR_SCAN;
        key = scanbuf + off;
        off += klen;
        vlen = qi_rd32(scanbuf + off);
        off += 4u;
        if (off + vlen > QI_SCAN_BUF) return QISA_ERR_SCAN;
        val = scanbuf + off;
        off += vlen;
        rc = qi_materialize_row(t, key, klen, val, vlen, page_bump,
                                rows + (uint64_t)i * t->ncols);
        if (rc != QISA_OK) return rc;
    }
    *rows_out = rows;
    return QISA_OK;
}

/* ---- join hash ---- */
static uint64_t qi_fnv(const uint8_t* s, uint32_t len) {
    uint64_t h = 14695981039346656037u;
    uint32_t i;
    for (i = 0; i < len; i++) {
        h ^= (uint64_t)s[i];
        h *= 1099511628211u;
    }
    return h;
}

/* Canonical join-key encoding into tmp (cap 2048; keys come from
 * segdb values/keys, so they are <= 1024 bytes plus tag/length).
 * EXACT across int/float (Gate 4, mirrors qi_cmp and Go's
 * Compare): int keys encode as 'I' + i64; float keys that are
 * integral and in int64 range encode as the same 'I' + i64
 * (so int 1 matches float 1.0, as Compare requires); other
 * floats encode as 'N' + bits, with -0.0 normalized to +0.0
 * and NaN to one fixed pattern (unreachable from JSON, which
 * has no NaN token). The old double-bits encoding collapsed
 * int 2^53+1 and 2^53+2 to the same key; this one does not.
 * Returns the length, or 0 on overflow (caller treats as
 * OOM). */
static uint32_t qi_join_canon(const QiVal* v, uint8_t* tmp) {
    uint64_t bits;
    if (v->kind == (uint8_t)QISA_KIND_INT) {
        tmp[0] = (uint8_t)'I';
        qi_wr64(tmp + 1, (uint64_t)v->i);
        return 9;
    }
    if (v->kind == (uint8_t)QISA_KIND_FLOAT) {
        double d = v->f;
        if (d == 0.0) {
            tmp[0] = (uint8_t)'I';
            qi_wr64(tmp + 1, 0);
            return 9;
        }
        if (d != d) {
            tmp[0] = (uint8_t)'N';
            qi_wr64(tmp + 1, 0x7FF8000000000000u);
            return 9;
        }
        if (d >= -9223372036854775808.0 &&
            d < 9223372036854775808.0 &&
            d == (double)(int64_t)d) {
            tmp[0] = (uint8_t)'I';
            qi_wr64(tmp + 1, (uint64_t)(int64_t)d);
            return 9;
        }
        bits = qi_dbl_bits(d);
        tmp[0] = (uint8_t)'N';
        qi_wr64(tmp + 1, bits);
        return 9;
    }
    if (v->kind == (uint8_t)QISA_KIND_STRING) {
        if (v->len > 2030u) return 0;
        tmp[0] = (uint8_t)'S';
        qi_wr32(tmp + 1, v->len);
        qi_copy(tmp + 5, v->s, v->len);
        return 5u + v->len;
    }
    if (v->kind == (uint8_t)QISA_KIND_BOOL) {
        tmp[0] = (uint8_t)'B';
        tmp[1] = v->i != 0 ? 1 : 0;
        return 2;
    }
    if (v->kind == (uint8_t)QISA_KIND_BYTES) {
        if (v->len > 2030u) return 0;
        tmp[0] = (uint8_t)'Y';
        qi_wr32(tmp + 1, v->len);
        qi_copy(tmp + 5, v->s, v->len);
        return 5u + v->len;
    }
    return 0;
}

typedef struct {
    int32_t* head;
    int32_t* next;
    const uint8_t** canon;
    uint32_t* canon_len;
    uint32_t size;
    uint32_t mask;
} QiHash;

static int qi_hash_build(QiHash* h, QiVec* rows, uint32_t nrows,
                         uint32_t keycol, QiBump* bump) {
    uint32_t size = 16;
    uint32_t i;
    uint8_t tmp[2048];
    while (size < nrows * 2u + 16u) size *= 2u;
    h->size = size;
    h->mask = size - 1u;
    h->head = (int32_t*)qi_bump(bump, (uint64_t)size * 4u);
    h->next = (int32_t*)qi_bump(bump, (uint64_t)(nrows == 0u ? 1u : nrows) * 4u);
    h->canon = (const uint8_t**)qi_bump(
        bump, (uint64_t)(nrows == 0u ? 1u : nrows) * 8u);
    h->canon_len = (uint32_t*)qi_bump(
        bump, (uint64_t)(nrows == 0u ? 1u : nrows) * 4u);
    if (!h->head || !h->next || !h->canon || !h->canon_len)
        return QISA_ERR_OOM;
    for (i = 0; i < size; i++) h->head[i] = -1;
    for (i = 0; i < nrows; i++) {
        const QiVal* kv = &qi_vec_row(rows, i)[keycol];
        uint32_t clen;
        uint8_t* cp;
        uint32_t slot;
        h->next[i] = -1;
        h->canon[i] = 0;
        h->canon_len[i] = 0;
        if (kv->kind == (uint8_t)QISA_KIND_NULL) continue;
        clen = qi_join_canon(kv, tmp);
        if (clen == 0u) return QISA_ERR_OOM;
        cp = (uint8_t*)qi_bump(bump, clen);
        if (!cp) return QISA_ERR_OOM;
        qi_copy(cp, tmp, clen);
        h->canon[i] = cp;
        h->canon_len[i] = clen;
        slot = (uint32_t)(qi_fnv(cp, clen) & h->mask);
        h->next[i] = h->head[slot];
        h->head[slot] = (int32_t)i;
    }
    return QISA_OK;
}

/* ---- aggregation ---- */
typedef struct {
    uint8_t agg;
    int64_t count;
    double sum_f;
    int64_t sum_i;
    int has;
    int is_float;
    QiVal mn;
    QiVal mx;
} QiAcc;

typedef struct {
    const uint8_t* key;
    uint32_t klen;
    QiVal* key_vals;
    QiAcc* accs;
    int32_t next;
} QiGroup;

/* qi_acc_add folds one value into an accumulator. MIN/MAX extremes
 * are deep-copied into the persistent bump: the source value may
 * point into a transient page buffer that the next page overwrites.
 * When bump is 0, values are copied by struct only (used only for
 * in-memory values that already live in the persistent bump). */
static int qi_acc_add(QiAcc* a, const QiVal* v, int star, QiBump* bump) {
    int c, rc;
    if (a->agg == QISA_AGG_COUNT) {
        if (star) {
            a->count++;
            return QISA_OK;
        }
        if (v->kind != (uint8_t)QISA_KIND_NULL) a->count++;
        return QISA_OK;
    }
    if (a->agg == QISA_AGG_SUM || a->agg == QISA_AGG_AVG) {
        if (v->kind == (uint8_t)QISA_KIND_NULL) return QISA_OK;
        if (!qi_is_num(v)) return QISA_ERR_TYPE;
        a->sum_f += qi_as_dbl(v);
        if (v->kind == (uint8_t)QISA_KIND_INT) {
            /* Gate 4: checked int accumulation — overflow
             * is QISA_ERR_OVERFLOW, never a silent wrap
             * (the Go executor still wraps; see
             * GATE4_NOTES.md §5a). */
            if ((v->i > 0 && a->sum_i > 9223372036854775807LL - v->i) ||
                (v->i < 0 && a->sum_i < (-9223372036854775807LL - 1) - v->i))
                return QISA_ERR_OVERFLOW;
            a->sum_i += v->i;
        } else a->is_float = 1;
        a->count++;
        a->has = 1;
        return QISA_OK;
    }
    /* MIN / MAX */
    if (v->kind == (uint8_t)QISA_KIND_NULL) return QISA_OK;
    if (!a->has) {
        if (bump) {
            rc = qi_vcopy_persist(&a->mn, v, bump);
            if (rc != QISA_OK) return rc;
            rc = qi_vcopy_persist(&a->mx, v, bump);
            if (rc != QISA_OK) return rc;
        } else {
            qi_vcopy(&a->mn, v);
            qi_vcopy(&a->mx, v);
        }
        a->has = 1;
        return QISA_OK;
    }
    rc = qi_cmp(v, &a->mn, &c);
    if (rc != QISA_OK) return rc;
    if (c < 0) {
        if (bump) {
            rc = qi_vcopy_persist(&a->mn, v, bump);
            if (rc != QISA_OK) return rc;
        } else {
            qi_vcopy(&a->mn, v);
        }
    }
    rc = qi_cmp(v, &a->mx, &c);
    if (rc != QISA_OK) return rc;
    if (c > 0) {
        if (bump) {
            rc = qi_vcopy_persist(&a->mx, v, bump);
            if (rc != QISA_OK) return rc;
        } else {
            qi_vcopy(&a->mx, v);
        }
    }
    return QISA_OK;
}

/* Test hook (Gate 4, not part of the ISA contract): drives
 * the checked SUM path directly, because no ISA program can
 * feed exact near-int64-max values into SUM — JSON ingestion
 * cannot represent them exactly (see GATE4_NOTES.md §5a).
 * Returns QISA_OK and *out = a+b, or QISA_ERR_OVERFLOW. */
int yowza_queryisa_test_checked_sum(int64_t a, int64_t b, int64_t* out) {
    QiAcc acc;
    int rc;
    acc.agg = QISA_AGG_SUM;
    acc.count = 0;
    acc.sum_f = 0.0;
    acc.sum_i = 0;
    acc.has = 0;
    acc.is_float = 0;
    qi_vnull(&acc.mn);
    qi_vnull(&acc.mx);
    {
        QiVal va, vb;
        qi_vnull(&va);
        qi_vnull(&vb);
        va.kind = (uint8_t)QISA_KIND_INT;
        va.i = a;
        vb.kind = (uint8_t)QISA_KIND_INT;
        vb.i = b;
        rc = qi_acc_add(&acc, &va, 0, 0);
        if (rc != QISA_OK) return rc;
        rc = qi_acc_add(&acc, &vb, 0, 0);
        if (rc != QISA_OK) return rc;
    }
    if (out) *out = acc.sum_i;
    return QISA_OK;
}

/* Merge a spilled partial accumulator into a final one, mirroring
 * Go's accumulator.mergeFrom: COUNT adds counts; SUM/AVG add sums,
 * counts, and floatness; MIN/MAX take extremes of the partial's
 * min/max. Extremes are deep-copied into the persistent bump. */
static int qi_acc_merge(QiAcc* dst, const QiAcc* src, QiBump* bump) {
    int c, rc;
    if (src->agg == QISA_AGG_COUNT) {
        dst->count += src->count;
        return QISA_OK;
    }
    if (src->agg == QISA_AGG_SUM || src->agg == QISA_AGG_AVG) {
        dst->sum_f += src->sum_f;
        if ((src->sum_i > 0 &&
             dst->sum_i > 9223372036854775807LL - src->sum_i) ||
            (src->sum_i < 0 &&
             dst->sum_i < (-9223372036854775807LL - 1) - src->sum_i))
            return QISA_ERR_OVERFLOW;
        dst->sum_i += src->sum_i;
        if (src->is_float) dst->is_float = 1;
        dst->count += src->count;
        if (src->has) dst->has = 1;
        return QISA_OK;
    }
    /* MIN / MAX */
    if (!src->has) return QISA_OK;
    if (!dst->has) {
        rc = qi_vcopy_persist(&dst->mn, &src->mn, bump);
        if (rc != QISA_OK) return rc;
        rc = qi_vcopy_persist(&dst->mx, &src->mx, bump);
        if (rc != QISA_OK) return rc;
        dst->has = 1;
        return QISA_OK;
    }
    rc = qi_cmp(&src->mn, &dst->mn, &c);
    if (rc != QISA_OK) return rc;
    if (c < 0) {
        rc = qi_vcopy_persist(&dst->mn, &src->mn, bump);
        if (rc != QISA_OK) return rc;
    }
    rc = qi_cmp(&src->mx, &dst->mx, &c);
    if (rc != QISA_OK) return rc;
    if (c > 0) {
        rc = qi_vcopy_persist(&dst->mx, &src->mx, bump);
        if (rc != QISA_OK) return rc;
    }
    return QISA_OK;
}

/* Go's measuredGroupSize exactly: 48 per group, key value sizes,
 * 24 per accumulator, plus min/max value sizes when present. */
static uint64_t qi_groups_bytes(QiGroup* groups, uint32_t ngroups,
                                QiProg* p) {
    uint64_t total = 0;
    uint32_t g, a;
    for (g = 0; g < ngroups; g++) {
        QiGroup* gr = &groups[g];
        uint64_t sz = 48u;
        for (a = 0; a < p->ngroup; a++)
            sz += qi_value_size(&gr->key_vals[a]);
        for (a = 0; a < p->nagg; a++) {
            QiAcc* acc = &gr->accs[a];
            sz += 24u;
            if (acc->has)
                sz += qi_value_size(&acc->mn) + qi_value_size(&acc->mx);
        }
        total += sz;
    }
    return total;
}

static void qi_acc_result(QiAcc* a, QiVal* out) {
    qi_vnull(out);
    switch (a->agg) {
    case QISA_AGG_COUNT:
        out->kind = (uint8_t)QISA_KIND_INT;
        out->i = a->count;
        break;
    case QISA_AGG_SUM:
        if (!a->has) break;
        if (a->is_float) {
            out->kind = (uint8_t)QISA_KIND_FLOAT;
            out->f = a->sum_f;
        } else {
            out->kind = (uint8_t)QISA_KIND_INT;
            out->i = a->sum_i;
        }
        break;
    case QISA_AGG_AVG:
        if (!a->has || a->count == 0) break;
        out->kind = (uint8_t)QISA_KIND_FLOAT;
        out->f = a->sum_f / (double)a->count;
        break;
    case QISA_AGG_MIN:
        if (a->has) qi_vcopy(out, &a->mn);
        break;
    case QISA_AGG_MAX:
        if (a->has) qi_vcopy(out, &a->mx);
        break;
    default:
        break;
    }
}

/* Canonical group key: tag per value, with distinct types never
 * colliding (int 1, float 1.0, string "1" are different keys),
 * NaN normalized to one key — exactly Go's groupKey(). */
static uint32_t qi_group_key_size(const QiVal* row, QiProg* p) {
    uint32_t i, size = 0;
    for (i = 0; i < p->ngroup; i++) {
        const QiVal* v = &row[p->group_cols[i]];
        switch (v->kind) {
        case QISA_KIND_NULL: size += 1u; break;
        case QISA_KIND_INT:
        case QISA_KIND_FLOAT: size += 9u; break;
        case QISA_KIND_STRING:
        case QISA_KIND_BYTES: size += 5u + v->len; break;
        case QISA_KIND_BOOL: size += 2u; break;
        default: size += 1u; break;
        }
    }
    return size;
}

static void qi_group_key_fill(const QiVal* row, QiProg* p, uint8_t* dst) {
    uint32_t i, o = 0;
    for (i = 0; i < p->ngroup; i++) {
        const QiVal* v = &row[p->group_cols[i]];
        switch (v->kind) {
        case QISA_KIND_NULL:
            dst[o++] = 0;
            break;
        case QISA_KIND_INT:
            dst[o++] = 1;
            qi_wr64(dst + o, (uint64_t)v->i);
            o += 8u;
            break;
        case QISA_KIND_FLOAT: {
            uint64_t bits;
            double d = v->f;
            if (d != d) bits = 0x7FF8000000000000u;
            else bits = qi_dbl_bits(d);
            dst[o++] = 2;
            qi_wr64(dst + o, bits);
            o += 8u;
            break;
        }
        case QISA_KIND_STRING:
            dst[o++] = 3;
            qi_wr32(dst + o, v->len);
            o += 4u;
            qi_copy(dst + o, v->s, v->len);
            o += v->len;
            break;
        case QISA_KIND_BOOL:
            dst[o++] = 4;
            dst[o++] = v->i != 0 ? 1 : 0;
            break;
        case QISA_KIND_BYTES:
            dst[o++] = 5;
            qi_wr32(dst + o, v->len);
            o += 4u;
            qi_copy(dst + o, v->s, v->len);
            o += v->len;
            break;
        default:
            dst[o++] = 0;
            break;
        }
    }
}

/* ---- sort ---- */
typedef struct {
    QiVec* vec;
    uint32_t width;
    QiSortKey* keys;
    uint32_t nkeys;
} QiSortCtx;

static int qi_row_cmp(QiSortCtx* ctx, uint32_t a, uint32_t b) {
    uint32_t k;
    for (k = 0; k < ctx->nkeys; k++) {
        const QiVal* va = &qi_vec_row(ctx->vec, a)[ctx->keys[k].col];
        const QiVal* vb = &qi_vec_row(ctx->vec, b)[ctx->keys[k].col];
        int c, rc;
        if (va->kind == (uint8_t)QISA_KIND_NULL &&
            vb->kind == (uint8_t)QISA_KIND_NULL)
            continue;
        if (va->kind == (uint8_t)QISA_KIND_NULL) return 1; /* NULLs last */
        if (vb->kind == (uint8_t)QISA_KIND_NULL) return -1;
        rc = qi_cmp(va, vb, &c);
        if (rc != QISA_OK) continue; /* reference ignores sort
         * Compare errors and moves to the next key */
        if (c != 0) return ctx->keys[k].desc ? -c : c;
    }
    return 0;
}

static void qi_sort_merge(QiSortCtx* ctx, int32_t* idx, int32_t* tmp,
                          uint32_t lo, uint32_t mid, uint32_t hi) {
    uint32_t i = lo, j = mid, k = lo;
    while (i < mid && j < hi) {
        int c = qi_row_cmp(ctx, (uint32_t)idx[i], (uint32_t)idx[j]);
        if (c <= 0) tmp[k++] = idx[i++];
        else tmp[k++] = idx[j++];
    }
    while (i < mid) tmp[k++] = idx[i++];
    while (j < hi) tmp[k++] = idx[j++];
    for (i = lo; i < hi; i++) idx[i] = tmp[i];
}

static void qi_sort_rec(QiSortCtx* ctx, int32_t* idx, int32_t* tmp,
                        uint32_t lo, uint32_t hi) {
    uint32_t mid;
    if (hi - lo <= 1u) return;
    mid = lo + (hi - lo) / 2u;
    qi_sort_rec(ctx, idx, tmp, lo, mid);
    qi_sort_rec(ctx, idx, tmp, mid, hi);
    qi_sort_merge(ctx, idx, tmp, lo, mid, hi);
}

/* ---- result encoding ---- */
static int qi_emit_u32(uint8_t* out, uint32_t cap, uint32_t* off,
                       uint32_t v) {
    if (*off + 4u > cap) return QISA_ERR_OUT_SMALL;
    qi_wr32(out + *off, v);
    *off += 4u;
    return QISA_OK;
}

static int qi_emit_val(uint8_t* out, uint32_t cap, uint32_t* off,
                       const QiVal* v) {
    int rc;
    if (*off + 1u > cap) return QISA_ERR_OUT_SMALL;
    out[*off] = v->kind;
    *off += 1u;
    switch (v->kind) {
    case QISA_KIND_NULL:
        return QISA_OK;
    case QISA_KIND_INT:
        if (*off + 8u > cap) return QISA_ERR_OUT_SMALL;
        qi_wr64(out + *off, (uint64_t)v->i);
        *off += 8u;
        return QISA_OK;
    case QISA_KIND_FLOAT:
        if (*off + 8u > cap) return QISA_ERR_OUT_SMALL;
        qi_wr64(out + *off, qi_dbl_bits(v->f));
        *off += 8u;
        return QISA_OK;
    case QISA_KIND_STRING:
    case QISA_KIND_BYTES:
        rc = qi_emit_u32(out, cap, off, v->len);
        if (rc != QISA_OK) return rc;
        if (*off + v->len > cap) return QISA_ERR_OUT_SMALL;
        if (v->len > 0) qi_copy(out + *off, v->s, v->len);
        *off += v->len;
        return QISA_OK;
    case QISA_KIND_BOOL:
        if (*off + 1u > cap) return QISA_ERR_OUT_SMALL;
        out[*off] = v->i != 0 ? 1 : 0;
        *off += 1u;
        return QISA_OK;
    default:
        return QISA_ERR_MALFORMED;
    }
}

/* ---- main execution ---- */

/* Filter + project one combined row. Returns QISA_OK with *passed
 * set, or an error code. proj must hold nproj values. */
static int qi_filter_project(QiProg* p, const QiVal* row, uint32_t width,
                             QiBump* bump, QiVal* proj, int* passed) {
    int tri, rc, i;
    *passed = 0;
    if (p->filter) {
        rc = qi_eval_tri(p->filter, row, width, bump, &tri);
        if (rc != QISA_OK) return rc;
        if (tri != QI_TRI_TRUE) return QISA_OK;
    }
    for (i = 0; (uint32_t)i < p->nproj; i++) {
        if (p->proj_cols[i] >= width) return QISA_ERR_MALFORMED;
        qi_vcopy(&proj[i], &row[p->proj_cols[i]]);
    }
    *passed = 1;
    return QISA_OK;
}

static int qi_budget_chunks(uint64_t* scanned, uint64_t max_scanned,
                            uint32_t n) {
    uint32_t off = 0;
    while (off < n) {
        uint32_t chunk = n - off > QI_BATCH ? QI_BATCH : n - off;
        *scanned += chunk;
        if (max_scanned > 0 && *scanned > max_scanned)
            return QISA_ERR_BUDGET;
        off += chunk;
    }
    return QISA_OK;
}

/* ---- combined-row iteration ----
 * One iterator drives all three execution paths (streaming,
 * aggregate, sort) so join expansion, filter placement, and
 * batching cannot drift between paths. The callback receives one
 * combined row at a time (left columns followed by right columns
 * when joining; the left row itself when not). Callbacks copy
 * anything they keep: the combined buffer is reused. Returning
 * QI_STOP stops iteration cleanly (used by LIMIT in the streaming
 * path); any negative code aborts with that error. */
#define QI_STOP 1000

typedef int (*QiRowCb)(void* ctx, const QiVal* comb, uint32_t width);

typedef struct {
    QiProg* p;
    QiVal* left_rows; /* one transient page, width w_left */
    QiVec* right_vec; /* persistent build rows, width w_right */
    uint32_t w_left;
    uint32_t w_right;
    QiHash* hash;
    QiVal* comb;
    uint8_t* canon_tmp;
} QiIter;

static int qi_iterate(QiIter* it, uint32_t start, uint32_t count,
                      void* ctx, QiRowCb cb) {
    uint32_t i, k, total_w = it->w_left + it->w_right;
    for (i = start; i < start + count; i++) {
        const QiVal* lrow = it->left_rows + (uint64_t)i * it->w_left;
        if (it->p->has_join) {
            const QiVal* lk = &lrow[it->p->join_left];
            uint32_t clen;
            int32_t ridx;
            if (lk->kind == (uint8_t)QISA_KIND_NULL) continue;
            clen = qi_join_canon(lk, it->canon_tmp);
            if (clen == 0u) return QISA_ERR_OOM;
            ridx = it->hash->head[qi_fnv(it->canon_tmp, clen) &
                                  it->hash->mask];
            while (ridx >= 0) {
                const QiVal* rrow = qi_vec_row(it->right_vec,
                                               (uint32_t)ridx);
                int rc;
                if (it->hash->canon[ridx] &&
                    it->hash->canon_len[ridx] == clen &&
                    qi_eq(it->hash->canon[ridx], it->canon_tmp, clen)) {
                    for (k = 0; k < it->w_left; k++)
                        qi_vcopy(&it->comb[k], &lrow[k]);
                    for (k = 0; k < it->w_right; k++)
                        qi_vcopy(&it->comb[it->w_left + k], &rrow[k]);
                    rc = cb(ctx, it->comb, total_w);
                    if (rc != QISA_OK) return rc;
                }
                ridx = it->hash->next[ridx];
            }
        } else {
            int rc = cb(ctx, lrow, it->w_left);
            if (rc != QISA_OK) return rc;
        }
    }
    return QISA_OK;
}

/* Filter-only check (aggregate path: groups form only from rows
 * that pass the filter, in pipeline order FILTER -> AGGREGATE). */
static int qi_filter_pass(QiProg* p, const QiVal* row, uint32_t width,
                          QiBump* bump, int* passed) {
    int tri, rc;
    *passed = 0;
    if (!p->filter) {
        *passed = 1;
        return QISA_OK;
    }
    rc = qi_eval_tri(p->filter, row, width, bump, &tri);
    if (rc != QISA_OK) return rc;
    *passed = tri == QI_TRI_TRUE;
    return QISA_OK;
}

/* ---- aggregate path context ----
 * Groups live in the persistent bump; filter evaluation (LIKE
 * code-point arrays) uses the page bump. spill_groups holds
 * flushed groups in first-appearance order; at the end they merge
 * with the in-memory groups, deduplicated by canonical key. */
typedef struct {
    const uint8_t* key;
    uint32_t klen;
    QiVal* key_vals;
    QiAcc* accs;
} QiSpillGroup;

typedef struct {
    QiProg* p;
    QiBump* bump;      /* persistent: groups, keys, extremes */
    QiBump* page_bump; /* transient: LIKE evaluation */
    uint32_t max_groups;
    QiGroup* groups;
    uint32_t ngroups;
    int32_t* ghead;
    QiSpillGroup* spill;
    uint32_t nspill;
    uint32_t cap_spill;
} QiAggCtx;

/* Pass 1 for one page: sum the Go batchBytes of the filtered
 * combined rows (pipeline rows, named alias.column). The caller
 * checks the batch against the budget BEFORE folding (pass 2),
 * exactly as Go checks batchBytes before folding a batch. */
typedef struct {
    QiProg* p;
    QiBump* page_bump;
    const uint32_t* name_lens;
    uint64_t batch_bytes;
} QiAggBatchCtx;

static int qi_agg_batch_cb(void* ctxv, const QiVal* comb, uint32_t width) {
    QiAggBatchCtx* ctx = (QiAggBatchCtx*)ctxv;
    int passed, rc;
    rc = qi_filter_pass(ctx->p, comb, width, ctx->page_bump, &passed);
    if (rc != QISA_OK) return rc;
    if (!passed) return QISA_OK;
    ctx->batch_bytes += qi_row_size(comb, width, ctx->name_lens);
    return QISA_OK;
}

static int qi_agg_cb(void* ctxv, const QiVal* comb, uint32_t width) {
    QiAggCtx* ctx = (QiAggCtx*)ctxv;
    QiProg* p = ctx->p;
    int passed, rc;
    uint32_t gk_size, slot, a;
    int32_t gidx;
    QiGroup* g = 0;
    uint8_t* gk;
    rc = qi_filter_pass(p, comb, width, ctx->page_bump, &passed);
    if (rc != QISA_OK) return rc;
    if (!passed) return QISA_OK;
    gk_size = qi_group_key_size(comb, p);
    gk = (uint8_t*)qi_bump(ctx->bump, gk_size == 0u ? 1u : gk_size);
    if (!gk) return QISA_ERR_OOM;
    qi_group_key_fill(comb, p, gk);
    slot = (uint32_t)(qi_fnv(gk, gk_size) & 65535u);
    gidx = ctx->ghead[slot];
    while (gidx >= 0) {
        QiGroup* cand = &ctx->groups[(uint32_t)gidx];
        if (cand->klen == gk_size && qi_eq(cand->key, gk, gk_size)) {
            g = cand;
            break;
        }
        gidx = cand->next;
    }
    if (!g) {
        QiGroup* ng;
        if (ctx->ngroups >= ctx->max_groups) return QISA_ERR_LIMIT;
        ng = &ctx->groups[ctx->ngroups];
        ng->key = gk;
        ng->klen = gk_size;
        ng->key_vals = (QiVal*)qi_bump(
            ctx->bump, (uint64_t)(p->ngroup == 0u ? 1u : p->ngroup) *
                           (uint64_t)sizeof(QiVal));
        ng->accs = (QiAcc*)qi_bump(
            ctx->bump, (uint64_t)(p->nagg == 0u ? 1u : p->nagg) *
                           (uint64_t)sizeof(QiAcc));
        if (!ng->key_vals || !ng->accs) return QISA_ERR_OOM;
        for (a = 0; a < p->ngroup; a++) {
            rc = qi_vcopy_persist(&ng->key_vals[a],
                                  &comb[p->group_cols[a]], ctx->bump);
            if (rc != QISA_OK) return rc;
        }
        for (a = 0; a < p->nagg; a++) {
            ng->accs[a].agg = p->aggs[a].agg;
            ng->accs[a].count = 0;
            ng->accs[a].sum_f = 0.0;
            ng->accs[a].sum_i = 0;
            ng->accs[a].has = 0;
            ng->accs[a].is_float = 0;
            qi_vnull(&ng->accs[a].mn);
            qi_vnull(&ng->accs[a].mx);
        }
        ng->next = ctx->ghead[slot];
        ctx->ghead[slot] = (int32_t)ctx->ngroups;
        g = ng;
        ctx->ngroups++;
    }
    for (a = 0; a < p->nagg; a++) {
        QiAcc* acc = &g->accs[a];
        if (p->aggs[a].star) {
            rc = qi_acc_add(acc, 0, 1, ctx->bump);
        } else {
            rc = qi_acc_add(acc, &comb[p->aggs[a].col], 0, ctx->bump);
        }
        if (rc != QISA_OK) return rc;
    }
    return QISA_OK;
}

/* Flush the in-memory groups to spill records (deep copies, so
 * the group array can be reused for the next epoch) and reset
 * the in-memory set, mirroring Go's group-spill flush. */
static int qi_agg_spill(QiAggCtx* ctx) {
    QiProg* p = ctx->p;
    uint32_t g, a;
    int32_t gi;
    if (ctx->ngroups + ctx->nspill > 0 &&
        ctx->ngroups > 0) {
        /* Grow the spill array if needed. */
        while (ctx->nspill + ctx->ngroups > ctx->cap_spill) {
            uint32_t ncap = ctx->cap_spill == 0u ? 256u : ctx->cap_spill * 2u;
            QiSpillGroup* nsp = (QiSpillGroup*)qi_bump(
                ctx->bump, (uint64_t)ncap * (uint64_t)sizeof(QiSpillGroup));
            uint32_t k;
            if (!nsp) return QISA_ERR_OOM;
            for (k = 0; k < ctx->nspill; k++) nsp[k] = ctx->spill[k];
            ctx->spill = nsp;
            ctx->cap_spill = ncap;
        }
        for (g = 0; g < ctx->ngroups; g++) {
            QiGroup* src = &ctx->groups[g];
            QiSpillGroup* dst = &ctx->spill[ctx->nspill];
            uint8_t* kcopy;
            int rc;
            kcopy = (uint8_t*)qi_bump(ctx->bump,
                                      src->klen == 0u ? 1u : src->klen);
            if (!kcopy) return QISA_ERR_OOM;
            qi_copy(kcopy, src->key, src->klen);
            dst->key = kcopy;
            dst->klen = src->klen;
            dst->key_vals = (QiVal*)qi_bump(
                ctx->bump, (uint64_t)(p->ngroup == 0u ? 1u : p->ngroup) *
                               (uint64_t)sizeof(QiVal));
            dst->accs = (QiAcc*)qi_bump(
                ctx->bump, (uint64_t)(p->nagg == 0u ? 1u : p->nagg) *
                               (uint64_t)sizeof(QiAcc));
            if (!dst->key_vals || !dst->accs) return QISA_ERR_OOM;
            for (a = 0; a < p->ngroup; a++) {
                rc = qi_vcopy_persist(&dst->key_vals[a], &src->key_vals[a],
                                      ctx->bump);
                if (rc != QISA_OK) return rc;
            }
            for (a = 0; a < p->nagg; a++) {
                QiAcc* da = &dst->accs[a];
                QiAcc* sa = &src->accs[a];
                da->agg = sa->agg;
                da->count = sa->count;
                da->sum_f = sa->sum_f;
                da->sum_i = sa->sum_i;
                da->has = sa->has;
                da->is_float = sa->is_float;
                qi_vnull(&da->mn);
                qi_vnull(&da->mx);
                if (sa->has) {
                    rc = qi_vcopy_persist(&da->mn, &sa->mn, ctx->bump);
                    if (rc != QISA_OK) return rc;
                    rc = qi_vcopy_persist(&da->mx, &sa->mx, ctx->bump);
                    if (rc != QISA_OK) return rc;
                }
            }
            ctx->nspill++;
        }
    }
    ctx->ngroups = 0;
    for (gi = 0; gi < 65536; gi++) ctx->ghead[gi] = -1;
    return QISA_OK;
}

/* ---- sort path context (stores projected rows in a vec) ---- */
typedef struct {
    QiProg* p;
    QiBump* bump;      /* persistent: stored projected rows */
    QiBump* page_bump; /* transient: LIKE evaluation */
    uint32_t max_sort_rows;
    QiVec* vec;
    QiVal* proj_tmp;
} QiSortCtx2;

/* Pass 1 for one page: sum the Go batchBytes of the projected
 * rows of this page (projected rows, named by OutputName). The
 * caller checks the batch against the budget BEFORE storing
 * (pass 2), exactly as Go checks a sort batch before buffering. */
typedef struct {
    QiProg* p;
    QiBump* page_bump;
    uint64_t batch_bytes;
} QiSortBatchCtx;

static int qi_sort_batch_cb(void* ctxv, const QiVal* comb, uint32_t width) {
    QiSortBatchCtx* ctx = (QiSortBatchCtx*)ctxv;
    QiProg* p = ctx->p;
    QiVal proj[QI_MAX_WIDTH];
    int passed, rc;
    rc = qi_filter_project(p, comb, width, ctx->page_bump, proj, &passed);
    if (rc != QISA_OK) return rc;
    if (!passed) return QISA_OK;
    ctx->batch_bytes += qi_row_size(proj, p->nproj, p->proj_namelen);
    return QISA_OK;
}

static int qi_sort_cb(void* ctxv, const QiVal* comb, uint32_t width) {
    QiSortCtx2* ctx = (QiSortCtx2*)ctxv;
    int passed, rc;
    rc = qi_filter_project(ctx->p, comb, width, ctx->page_bump,
                           ctx->proj_tmp, &passed);
    if (rc != QISA_OK) return rc;
    if (!passed) return QISA_OK;
    if (ctx->vec->nrows >= ctx->max_sort_rows) return QISA_ERR_LIMIT;
    rc = qi_vec_append(ctx->vec, ctx->proj_tmp, ctx->bump);
    if (rc != QISA_OK) return rc;
    return QISA_OK;
}

/* ---- streaming path context (encodes rows directly) ---- */
typedef struct {
    QiProg* p;
    QiBump* bump;
    uint8_t* out;
    uint32_t out_cap;
    uint32_t* off;
    uint64_t total;
    QiVal* proj_tmp;
} QiStreamCtx;

static int qi_stream_cb(void* ctxv, const QiVal* comb, uint32_t width) {
    QiStreamCtx* ctx = (QiStreamCtx*)ctxv;
    int passed, rc;
    uint32_t i;
    if (ctx->p->has_limit && ctx->total >= ctx->p->limit)
        return QI_STOP;
    rc = qi_filter_project(ctx->p, comb, width, ctx->bump, ctx->proj_tmp,
                           &passed);
    if (rc != QISA_OK) return rc;
    if (!passed) return QISA_OK;
    for (i = 0; i < ctx->p->nproj; i++) {
        rc = qi_emit_val(ctx->out, ctx->out_cap, ctx->off,
                         &ctx->proj_tmp[i]);
        if (rc != QISA_OK) return rc;
    }
    ctx->total++;
    if (ctx->p->max_result > 0 && ctx->total > ctx->p->max_result)
        return QISA_ERR_BUDGET;
    if (ctx->p->has_limit && ctx->total >= ctx->p->limit)
        return QI_STOP;
    return QISA_OK;
}

/* Encode stored rows (projected or aggregate rows) into out,
 * patching the row count at the front. *off starts at 4 (count
 * reserved) and ends at the total byte count. */
static int qi_encode_rows(uint8_t* out, uint32_t out_cap, uint32_t* off,
                          const QiVal* rows, uint32_t n, uint32_t width) {
    uint32_t r, c;
    int rc;
    for (r = 0; r < n; r++) {
        for (c = 0; c < width; c++) {
            rc = qi_emit_val(out, out_cap, off,
                             &rows[(uint64_t)r * width + c]);
            if (rc != QISA_OK) return rc;
        }
    }
    return QISA_OK;
}

/* Encode vec rows in index order (idx holds row indices into the
 * vec, or 0..n-1 in order when idx is 0). */
static int qi_encode_vec(uint8_t* out, uint32_t out_cap, uint32_t* off,
                         QiVec* vec, const int32_t* idx, uint32_t n,
                         uint32_t width) {
    uint32_t r, c;
    int rc;
    for (r = 0; r < n; r++) {
        const QiVal* row = qi_vec_row(vec, idx ? (uint32_t)idx[r] : r);
        for (c = 0; c < width; c++) {
            rc = qi_emit_val(out, out_cap, off, &row[c]);
            if (rc != QISA_OK) return rc;
        }
    }
    return QISA_OK;
}

/* ---- main execution (item 02) ----
 * One query snapshot, cursor pages of 64, Go-identical memory
 * accounting. The right (join build) side is loaded first, in
 * pages, at the query snapshot; the left side is then processed
 * page by page. AGGREGATE and SORT run two passes per page:
 * pass 1 measures the page's batch (Go checks batchBytes before
 * folding/buffering a batch), pass 2 folds/stores it. The
 * streaming path processes each page once and stops fetching
 * pages as soon as the LIMIT is satisfied. On any error the
 * out cells still report scanned / peak / spilled so far; the
 * host discards the output buffer on error, so no partial rows
 * are ever delivered. */
static int qi_run_impl(void* segdb_state, const uint8_t* prog,
                       uint32_t prog_len, uint8_t* out,
                       uint32_t out_cap, uint32_t* out_len,
                       uint64_t* scanned, uint64_t* mem_peak,
                       uint32_t* spilled, uint64_t snapshot_seq,
                       uint8_t* work, uint64_t work_cap,
                       const QisaPolicy* pol_in) {
    QiRd rd;
    QiBump bump;
    QiBump page_bump;
    QiProg p;
    uint8_t* scan_l;
    uint8_t* scan_r;
    uint32_t w_left, w_right, total_w;
    uint64_t scanned_count = 0;
    uint64_t peak = 0;
    int spilled_v = 0;
    uint32_t* pipeline_lens;
    uint32_t i;
    int rc;
    uint64_t dummy_peak = 0;
    uint32_t dummy_spilled = 0;
    QisaPolicy pol;
    if (!mem_peak) mem_peak = &dummy_peak;
    if (!spilled) spilled = &dummy_spilled;
    if (!segdb_state || !prog || !out || !out_len || !scanned || !work)
        return QISA_ERR_MALFORMED;
    *out_len = 0;
    *scanned = 0;
    *mem_peak = 0;
    *spilled = 0;
    qi_policy_resolve(pol_in, &pol);
    /* Width is configurable downward only: transient row
     * arrays are sized for QI_MAX_WIDTH columns. */
    if (pol.max_width > QI_MAX_WIDTH) return QISA_ERR_LIMIT;
    if (work_cap < (uint64_t)QI_WORK_HDR + 65536u) return QISA_ERR_OOM;
    if (out_cap < 4u) return QISA_ERR_OUT_SMALL;
    scan_l = work;
    scan_r = work + QI_SCAN_BUF;
    page_bump.base = work + 2u * QI_SCAN_BUF;
    page_bump.cap = QI_PAGE_SIZE;
    page_bump.off = 0;
    bump.base = work + QI_WORK_HDR;
    bump.cap = work_cap - QI_WORK_HDR;
    bump.off = 0;
    rd.p = prog;
    rd.len = prog_len;
    rd.pos = 0;
    rd.bad = 0;
    rc = qi_parse_prog(&rd, &bump, &p, &pol);
    if (rc != QISA_OK) goto fail;
    rc = qi_prog_validate(&p, &pol);
    if (rc != QISA_OK) goto fail;
    w_left = p.tables[0].ncols;
    w_right = p.has_join ? p.tables[1].ncols : 0;
    total_w = w_left + w_right;
    /* Pipeline name lengths: alias.column per column, in global
     * order. Used for JOIN-build and AGGREGATE batch accounting. */
    pipeline_lens = (uint32_t*)qi_bump(&bump, (uint64_t)total_w * 4u);
    if (!pipeline_lens) {
        rc = QISA_ERR_OOM;
        goto fail;
    }
    for (i = 0; i < total_w; i++) {
        uint32_t ti = 0, ci = i;
        if (p.has_join && i >= w_left) {
            ti = 1;
            ci = i - w_left;
        }
        pipeline_lens[i] = p.tables[ti].alen + 1u +
                           p.tables[ti].cols[ci].namelen;
    }
    {
        QiHash h;
        QiHash* hp = 0;
        QiIter it;
        uint8_t* canon_tmp;
        QiVal* comb;
        QiVec right_vec;
        uint32_t n_right = 0;
        uint64_t build_mem = 0;
        right_vec.chunks = 0;
        right_vec.nchunks = 0;
        right_vec.cap_chunks = 0;
        right_vec.nrows = 0;
        right_vec.width = w_right;
        comb = (QiVal*)qi_bump(&bump, (uint64_t)total_w *
                                          (uint64_t)sizeof(QiVal));
        canon_tmp = (uint8_t*)qi_bump(&bump, 2048u);
        if (!comb || !canon_tmp) {
            rc = QISA_ERR_OOM;
            goto fail;
        }
        /* JOIN build: cursor pages at the query snapshot, in
         * order. Per page: materialize (type errors first, as
         * in Go where materialization precedes the scan-budget
         * add), then scanned budget, then memory batch rule,
         * then append. */
        if (p.has_join) {
            QiCursor rcur;
            rcur.len = 0;
            rcur.has = 0;
            rcur.done = 0;
            while (!rcur.done) {
                uint32_t count = 0;
                QiVal* rows = 0;
                uint64_t batch_bytes = 0;
                uint32_t k;
                page_bump.off = 0;
                rc = qi_cursor_page(segdb_state, &p.tables[1],
                                    snapshot_seq, &rcur, scan_r, &count);
                if (rc != QISA_OK) goto fail;
                rc = qi_materialize_page(&p.tables[1], scan_r, count,
                                         &page_bump, &rows);
                if (rc != QISA_OK) goto fail;
                rc = qi_budget_chunks(&scanned_count, p.max_scanned,
                                      count);
                if (rc != QISA_OK) goto fail;
                for (k = 0; k < count; k++)
                    batch_bytes += qi_row_size(
                        rows + (uint64_t)k * w_right, w_right,
                        pipeline_lens + w_left);
                if (p.max_memory > 0 && batch_bytes > p.max_memory) {
                    rc = QISA_ERR_BUDGET;
                    goto fail;
                }
                if (p.max_memory > 0 &&
                    build_mem + batch_bytes > p.max_memory) {
                    spilled_v = 1;
                    build_mem = 0;
                }
                for (k = 0; k < count; k++) {
                    if (n_right >= pol.max_join_rows) {
                        rc = QISA_ERR_LIMIT;
                        goto fail;
                    }
                    rc = qi_vec_append(&right_vec,
                                       rows + (uint64_t)k * w_right,
                                       &bump);
                    if (rc != QISA_OK) goto fail;
                    n_right++;
                }
                build_mem += batch_bytes;
                if (build_mem > peak) peak = build_mem;
            }
            rc = qi_hash_build(&h, &right_vec, n_right,
                               p.join_right - w_left, &bump);
            if (rc != QISA_OK) goto fail;
            hp = &h;
        }
        it.p = &p;
        it.left_rows = 0;
        it.right_vec = &right_vec;
        it.w_left = w_left;
        it.w_right = w_right;
        it.hash = hp;
        it.comb = comb;
        it.canon_tmp = canon_tmp;

        if (p.has_agg) {
            QiAggCtx actx;
            QiCursor lcur;
            uint32_t gi, a;
            uint32_t agg_w = p.ngroup + p.nagg;
            uint32_t* agg_out_lens = 0;
            QiGroup* final_groups = 0;
            int32_t* final_ghead = 0;
            uint32_t n_final_groups = 0;
            QiVal* agg_rows = 0;
            QiVal* final_rows = 0;
            uint32_t n_final = 0;
            uint32_t off;
            actx.p = &p;
            actx.bump = &bump;
            actx.page_bump = &page_bump;
            actx.max_groups = pol.max_groups;
            actx.ngroups = 0;
            actx.spill = 0;
            actx.nspill = 0;
            actx.cap_spill = 0;
            actx.groups = (QiGroup*)qi_bump(
                &bump, (uint64_t)pol.max_groups *
                           (uint64_t)sizeof(QiGroup));
            actx.ghead = (int32_t*)qi_bump(&bump, 65536u * 4u);
            if (!actx.groups || !actx.ghead) {
                rc = QISA_ERR_OOM;
                goto fail;
            }
            for (gi = 0; gi < 65536u; gi++) actx.ghead[gi] = -1;
            /* Output-name lengths of the aggregate items, in
             * aggregate order, for the spilled-merge output
             * store accounting (groupRow columns: group keys
             * qualified, then aggregate OutputNames). */
            agg_out_lens = (uint32_t*)qi_bump(
                &bump, (uint64_t)(p.nagg == 0u ? 1u : p.nagg) * 4u);
            if (!agg_out_lens) {
                rc = QISA_ERR_OOM;
                goto fail;
            }
            {
                uint32_t oi = 0, pi;
                /* Walk the projection in order; aggregate items
                 * carry their OutputName in proj_names. Plain
                 * items are group keys and do not contribute an
                 * aggregate output name. The compiler emits
                 * aggregate items in projection order, so the
                 * oi-th aggregate item is the oi-th agg spec. */
                for (pi = 0; pi < p.nproj && oi < p.nagg; pi++) {
                    /* An item is an aggregate item iff its
                     * pipeline index is >= ngroup (aggregate
                     * plans use pipeline indices). */
                    if (p.proj_cols[pi] >= p.ngroup) {
                        agg_out_lens[oi++] = p.proj_namelen[pi];
                    }
                }
                while (oi < p.nagg) agg_out_lens[oi++] = 0;
            }
            lcur.len = 0;
            lcur.has = 0;
            lcur.done = 0;
            while (!lcur.done) {
                uint32_t count = 0;
                QiVal* rows = 0;
                QiAggBatchCtx bctx;
                uint64_t gb;
                page_bump.off = 0;
                rc = qi_cursor_page(segdb_state, &p.tables[0],
                                    snapshot_seq, &lcur, scan_l, &count);
                if (rc != QISA_OK) goto fail;
                rc = qi_materialize_page(&p.tables[0], scan_l, count,
                                         &page_bump, &rows);
                if (rc != QISA_OK) goto fail;
                rc = qi_budget_chunks(&scanned_count, p.max_scanned,
                                      count);
                if (rc != QISA_OK) goto fail;
                it.left_rows = rows;
                bctx.p = &p;
                bctx.page_bump = &page_bump;
                bctx.name_lens = pipeline_lens;
                bctx.batch_bytes = 0;
                rc = qi_iterate(&it, 0, count, &bctx, qi_agg_batch_cb);
                if (rc != QISA_OK) goto fail;
                if (p.max_memory > 0 &&
                    bctx.batch_bytes > p.max_memory) {
                    rc = QISA_ERR_BUDGET;
                    goto fail;
                }
                rc = qi_iterate(&it, 0, count, &actx, qi_agg_cb);
                if (rc != QISA_OK) goto fail;
                gb = qi_groups_bytes(actx.groups, actx.ngroups, &p);
                if (gb > peak) peak = gb;
                if (p.max_memory > 0 && gb > p.max_memory) {
                    spilled_v = 1;
                    rc = qi_agg_spill(&actx);
                    if (rc != QISA_OK) goto fail;
                }
            }
            /* Ungrouped aggregation over empty input yields
             * exactly one row (COUNT = 0, others NULL) — but
             * only when no group (spilled or in-memory) exists
             * at all. */
            if (p.ngroup == 0u && actx.ngroups == 0u &&
                actx.nspill == 0u) {
                QiGroup* ng = &actx.groups[0];
                ng->key = 0;
                ng->klen = 0;
                ng->key_vals = 0;
                ng->accs = (QiAcc*)qi_bump(
                    &bump, (uint64_t)(p.nagg == 0u ? 1u : p.nagg) *
                               (uint64_t)sizeof(QiAcc));
                if (!ng->accs) {
                    rc = QISA_ERR_OOM;
                    goto fail;
                }
                for (a = 0; a < p.nagg; a++) {
                    ng->accs[a].agg = p.aggs[a].agg;
                    ng->accs[a].count = 0;
                    ng->accs[a].sum_f = 0.0;
                    ng->accs[a].sum_i = 0;
                    ng->accs[a].has = 0;
                    ng->accs[a].is_float = 0;
                    qi_vnull(&ng->accs[a].mn);
                    qi_vnull(&ng->accs[a].mx);
                }
                actx.ngroups = 1;
            }
            /* Final merge: spill records first, then in-memory
             * groups, deduplicated by canonical key. Each group
             * is merged exactly once (the Go spilled path
             * duplicates and over-counts groups that repeat
             * after a spill; that bug is deliberately NOT
             * reproduced here — see the notes file). */
            final_groups = (QiGroup*)qi_bump(
                &bump, (uint64_t)pol.max_groups *
                           (uint64_t)sizeof(QiGroup));
            final_ghead = (int32_t*)qi_bump(&bump, 65536u * 4u);
            if (!final_groups || !final_ghead) {
                rc = QISA_ERR_OOM;
                goto fail;
            }
            for (gi = 0; gi < 65536u; gi++) final_ghead[gi] = -1;
            {
                uint32_t si;
                for (si = 0; si < actx.nspill; si++) {
                    QiSpillGroup* sg = &actx.spill[si];
                    uint32_t slot = (uint32_t)(qi_fnv(sg->key, sg->klen) &
                                               65535u);
                    int32_t fidx = final_ghead[slot];
                    QiGroup* fg = 0;
                    while (fidx >= 0) {
                        QiGroup* cand = &final_groups[(uint32_t)fidx];
                        if (cand->klen == sg->klen &&
                            qi_eq(cand->key, sg->key, sg->klen)) {
                            fg = cand;
                            break;
                        }
                        fidx = cand->next;
                    }
                    if (!fg) {
                        if (n_final_groups >= pol.max_groups) {
                            rc = QISA_ERR_LIMIT;
                            goto fail;
                        }
                        fg = &final_groups[n_final_groups];
                        fg->key = sg->key;
                        fg->klen = sg->klen;
                        fg->key_vals = sg->key_vals;
                        fg->accs = (QiAcc*)qi_bump(
                            &bump, (uint64_t)(p.nagg == 0u ? 1u : p.nagg) *
                                       (uint64_t)sizeof(QiAcc));
                        if (!fg->accs) {
                            rc = QISA_ERR_OOM;
                            goto fail;
                        }
                        for (a = 0; a < p.nagg; a++) {
                            fg->accs[a].agg = p.aggs[a].agg;
                            fg->accs[a].count = 0;
                            fg->accs[a].sum_f = 0.0;
                            fg->accs[a].sum_i = 0;
                            fg->accs[a].has = 0;
                            fg->accs[a].is_float = 0;
                            qi_vnull(&fg->accs[a].mn);
                            qi_vnull(&fg->accs[a].mx);
                        }
                        fg->next = final_ghead[slot];
                        final_ghead[slot] = (int32_t)n_final_groups;
                        n_final_groups++;
                    }
                    for (a = 0; a < p.nagg; a++) {
                        rc = qi_acc_merge(&fg->accs[a], &sg->accs[a],
                                          &bump);
                        if (rc != QISA_OK) goto fail;
                    }
                }
                for (gi = 0; gi < actx.ngroups; gi++) {
                    QiGroup* src = &actx.groups[gi];
                    uint32_t slot = (uint32_t)(qi_fnv(src->key, src->klen) &
                                               65535u);
                    int32_t fidx = final_ghead[slot];
                    QiGroup* fg = 0;
                    while (fidx >= 0) {
                        QiGroup* cand = &final_groups[(uint32_t)fidx];
                        if (cand->klen == src->klen &&
                            qi_eq(cand->key, src->key, src->klen)) {
                            fg = cand;
                            break;
                        }
                        fidx = cand->next;
                    }
                    if (!fg) {
                        if (n_final_groups >= pol.max_groups) {
                            rc = QISA_ERR_LIMIT;
                            goto fail;
                        }
                        fg = &final_groups[n_final_groups];
                        fg->key = src->key;
                        fg->klen = src->klen;
                        fg->key_vals = src->key_vals;
                        fg->accs = (QiAcc*)qi_bump(
                            &bump, (uint64_t)(p.nagg == 0u ? 1u : p.nagg) *
                                       (uint64_t)sizeof(QiAcc));
                        if (!fg->accs) {
                            rc = QISA_ERR_OOM;
                            goto fail;
                        }
                        for (a = 0; a < p.nagg; a++) {
                            fg->accs[a].agg = p.aggs[a].agg;
                            fg->accs[a].count = 0;
                            fg->accs[a].sum_f = 0.0;
                            fg->accs[a].sum_i = 0;
                            fg->accs[a].has = 0;
                            fg->accs[a].is_float = 0;
                            qi_vnull(&fg->accs[a].mn);
                            qi_vnull(&fg->accs[a].mx);
                        }
                        fg->next = final_ghead[slot];
                        final_ghead[slot] = (int32_t)n_final_groups;
                        n_final_groups++;
                    }
                    for (a = 0; a < p.nagg; a++) {
                        rc = qi_acc_merge(&fg->accs[a], &src->accs[a],
                                          &bump);
                        if (rc != QISA_OK) goto fail;
                    }
                }
            }
            /* Spilled-merge output store accounting (Go's
             * outStore): merged rows stream through a spill
             * store in the spilled mode only. A single merged
             * row larger than the budget fails, as in Go. */
            if (spilled_v && p.max_memory > 0) {
                uint64_t out_mem = 0;
                uint32_t fg2;
                for (fg2 = 0; fg2 < n_final_groups; fg2++) {
                    QiGroup* g = &final_groups[fg2];
                    uint64_t sz = 32u;
                    for (a = 0; a < p.ngroup; a++)
                        sz += (uint64_t)pipeline_lens[p.group_cols[a]] +
                              qi_value_size(&g->key_vals[a]);
                    for (a = 0; a < p.nagg; a++) {
                        QiVal rv;
                        qi_acc_result(&g->accs[a], &rv);
                        sz += (uint64_t)agg_out_lens[a] +
                              qi_value_size(&rv);
                    }
                    if (sz > p.max_memory) {
                        rc = QISA_ERR_BUDGET;
                        goto fail;
                    }
                    if (out_mem + sz > p.max_memory) out_mem = 0;
                    out_mem += sz;
                    if (out_mem > peak) peak = out_mem;
                }
            }
            if (n_final_groups > 0) {
                agg_rows = (QiVal*)qi_bump(
                    &bump, (uint64_t)n_final_groups * agg_w *
                               (uint64_t)sizeof(QiVal));
                if (!agg_rows) {
                    rc = QISA_ERR_OOM;
                    goto fail;
                }
                for (gi = 0; gi < n_final_groups; gi++) {
                    QiGroup* g = &final_groups[gi];
                    for (a = 0; a < p.ngroup; a++)
                        qi_vcopy(&agg_rows[(uint64_t)gi * agg_w + a],
                                 &g->key_vals[a]);
                    for (a = 0; a < p.nagg; a++) {
                        QiVal rv;
                        qi_acc_result(&g->accs[a], &rv);
                        qi_vcopy(
                            &agg_rows[(uint64_t)gi * agg_w +
                                      p.ngroup + a],
                            &rv);
                    }
                }
            }
            /* PROJECT over aggregate rows (pipeline indices). */
            n_final = n_final_groups;
            if (n_final > 0) {
                uint32_t r2, c2;
                final_rows = (QiVal*)qi_bump(
                    &bump, (uint64_t)n_final * p.nproj *
                               (uint64_t)sizeof(QiVal));
                if (!final_rows) {
                    rc = QISA_ERR_OOM;
                    goto fail;
                }
                for (r2 = 0; r2 < n_final; r2++) {
                    for (c2 = 0; c2 < p.nproj; c2++) {
                        qi_vcopy(
                            &final_rows[(uint64_t)r2 * p.nproj + c2],
                            &agg_rows[(uint64_t)r2 * agg_w +
                                      p.proj_cols[c2]]);
                    }
                }
            }
            if (p.nsort > 0 && n_final > 0) {
                QiVec sort_vec;
                QiSortCtx sctx;
                int32_t* idx;
                int32_t* tmp;
                uint32_t s;
                sort_vec.chunks = 0;
                sort_vec.nchunks = 0;
                sort_vec.cap_chunks = 0;
                sort_vec.nrows = 0;
                sort_vec.width = p.nproj;
                if (n_final > pol.max_sort_rows) {
                    rc = QISA_ERR_LIMIT;
                    goto fail;
                }
                for (s = 0; s < n_final; s++) {
                    rc = qi_vec_append(
                        &sort_vec,
                        final_rows + (uint64_t)s * p.nproj, &bump);
                    if (rc != QISA_OK) goto fail;
                }
                sctx.vec = &sort_vec;
                sctx.width = p.nproj;
                sctx.keys = p.sort_keys;
                sctx.nkeys = p.nsort;
                idx = (int32_t*)qi_bump(&bump, (uint64_t)n_final * 4u);
                tmp = (int32_t*)qi_bump(&bump, (uint64_t)n_final * 4u);
                if (!idx || !tmp) {
                    rc = QISA_ERR_OOM;
                    goto fail;
                }
                for (s = 0; s < n_final; s++) idx[s] = (int32_t)s;
                qi_sort_rec(&sctx, idx, tmp, 0, n_final);
                if (p.has_limit && (uint64_t)n_final > p.limit)
                    n_final = (uint32_t)p.limit;
                if (p.max_result > 0 &&
                    (uint64_t)n_final > p.max_result) {
                    rc = QISA_ERR_BUDGET;
                    goto fail;
                }
                off = 4;
                rc = qi_encode_vec(out, out_cap, &off, &sort_vec, idx,
                                   n_final, p.nproj);
                if (rc != QISA_OK) goto fail;
                qi_wr32(out, n_final);
                *out_len = off;
                *scanned = scanned_count;
                *mem_peak = peak;
                *spilled = spilled_v ? 1u : 0u;
                return QISA_OK;
            }
            if (p.has_limit && (uint64_t)n_final > p.limit)
                n_final = (uint32_t)p.limit;
            if (p.max_result > 0 &&
                (uint64_t)n_final > p.max_result) {
                rc = QISA_ERR_BUDGET;
                goto fail;
            }
            off = 4;
            rc = qi_encode_rows(out, out_cap, &off, final_rows,
                                n_final, p.nproj);
            if (rc != QISA_OK) goto fail;
            qi_wr32(out, n_final);
            *out_len = off;
            *scanned = scanned_count;
            *mem_peak = peak;
            *spilled = spilled_v ? 1u : 0u;
            return QISA_OK;
        }
        if (p.nsort > 0) {
            /* SORT without aggregation: two passes per page
             * (measure the projected batch, then store it),
             * then stable-sort, limit, encode. */
            QiSortCtx2 sctx2;
            QiSortCtx sctx;
            QiVec sort_vec;
            QiCursor lcur;
            int32_t* idx;
            int32_t* tmp;
            uint32_t s;
            uint32_t n_final;
            uint32_t off;
            uint64_t sort_mem = 0;
            sort_vec.chunks = 0;
            sort_vec.nchunks = 0;
            sort_vec.cap_chunks = 0;
            sort_vec.nrows = 0;
            sort_vec.width = p.nproj;
            sctx2.p = &p;
            sctx2.bump = &bump;
            sctx2.page_bump = &page_bump;
            sctx2.max_sort_rows = pol.max_sort_rows;
            sctx2.vec = &sort_vec;
            sctx2.proj_tmp = (QiVal*)qi_bump(
                &bump, (uint64_t)p.nproj * (uint64_t)sizeof(QiVal));
            if (!sctx2.proj_tmp) {
                rc = QISA_ERR_OOM;
                goto fail;
            }
            lcur.len = 0;
            lcur.has = 0;
            lcur.done = 0;
            while (!lcur.done) {
                uint32_t count = 0;
                QiVal* rows = 0;
                QiSortBatchCtx bctx;
                page_bump.off = 0;
                rc = qi_cursor_page(segdb_state, &p.tables[0],
                                    snapshot_seq, &lcur, scan_l, &count);
                if (rc != QISA_OK) goto fail;
                rc = qi_materialize_page(&p.tables[0], scan_l, count,
                                         &page_bump, &rows);
                if (rc != QISA_OK) goto fail;
                rc = qi_budget_chunks(&scanned_count, p.max_scanned,
                                      count);
                if (rc != QISA_OK) goto fail;
                it.left_rows = rows;
                bctx.p = &p;
                bctx.page_bump = &page_bump;
                bctx.batch_bytes = 0;
                rc = qi_iterate(&it, 0, count, &bctx, qi_sort_batch_cb);
                if (rc != QISA_OK) goto fail;
                if (p.max_memory > 0 &&
                    bctx.batch_bytes > p.max_memory) {
                    rc = QISA_ERR_BUDGET;
                    goto fail;
                }
                if (p.max_memory > 0 &&
                    sort_mem + bctx.batch_bytes > p.max_memory) {
                    spilled_v = 1;
                    sort_mem = 0;
                }
                rc = qi_iterate(&it, 0, count, &sctx2, qi_sort_cb);
                if (rc != QISA_OK) goto fail;
                sort_mem += bctx.batch_bytes;
                if (sort_mem > peak) peak = sort_mem;
            }
            n_final = sort_vec.nrows;
            if (n_final > 0) {
                sctx.vec = &sort_vec;
                sctx.width = p.nproj;
                sctx.keys = p.sort_keys;
                sctx.nkeys = p.nsort;
                idx = (int32_t*)qi_bump(&bump, (uint64_t)n_final * 4u);
                tmp = (int32_t*)qi_bump(&bump, (uint64_t)n_final * 4u);
                if (!idx || !tmp) {
                    rc = QISA_ERR_OOM;
                    goto fail;
                }
                for (s = 0; s < n_final; s++) idx[s] = (int32_t)s;
                qi_sort_rec(&sctx, idx, tmp, 0, n_final);
            } else {
                idx = 0;
            }
            if (p.has_limit && (uint64_t)n_final > p.limit)
                n_final = (uint32_t)p.limit;
            if (p.max_result > 0 &&
                (uint64_t)n_final > p.max_result) {
                rc = QISA_ERR_BUDGET;
                goto fail;
            }
            off = 4;
            rc = qi_encode_vec(out, out_cap, &off, &sort_vec, idx,
                               n_final, p.nproj);
            if (rc != QISA_OK) goto fail;
            qi_wr32(out, n_final);
            *out_len = off;
            *scanned = scanned_count;
            *mem_peak = peak;
            *spilled = spilled_v ? 1u : 0u;
            return QISA_OK;
        }

        /* Streaming path: no AGGREGATE, no SORT. The right side
         * was already fully counted during the build; left pages
         * are counted as pulled and pulling stops when the LIMIT
         * is satisfied, without fetching another page. */
        {
            QiStreamCtx sctx;
            QiCursor lcur;
            uint32_t out_off = 4;
            sctx.p = &p;
            sctx.bump = &page_bump;
            sctx.out = out;
            sctx.out_cap = out_cap;
            sctx.off = &out_off;
            sctx.total = 0;
            sctx.proj_tmp = (QiVal*)qi_bump(
                &bump, (uint64_t)p.nproj * (uint64_t)sizeof(QiVal));
            if (!sctx.proj_tmp) {
                rc = QISA_ERR_OOM;
                goto fail;
            }
            lcur.len = 0;
            lcur.has = 0;
            lcur.done = 0;
            while (!lcur.done) {
                uint32_t count = 0;
                QiVal* rows = 0;
                if (p.has_limit && sctx.total >= p.limit) break;
                page_bump.off = 0;
                rc = qi_cursor_page(segdb_state, &p.tables[0],
                                    snapshot_seq, &lcur, scan_l, &count);
                if (rc != QISA_OK) goto fail;
                rc = qi_materialize_page(&p.tables[0], scan_l, count,
                                         &page_bump, &rows);
                if (rc != QISA_OK) goto fail;
                rc = qi_budget_chunks(&scanned_count, p.max_scanned,
                                      count);
                if (rc != QISA_OK) goto fail;
                it.left_rows = rows;
                rc = qi_iterate(&it, 0, count, &sctx, qi_stream_cb);
                if (rc == QI_STOP) break;
                if (rc != QISA_OK) goto fail;
                if (p.has_limit && sctx.total >= p.limit) break;
            }
            qi_wr32(out, (uint32_t)sctx.total);
            *out_len = out_off;
            *scanned = scanned_count;
            *mem_peak = peak;
            *spilled = spilled_v ? 1u : 0u;
            return QISA_OK;
        }
    }
fail:
    *scanned = scanned_count;
    *mem_peak = peak;
    *spilled = spilled_v ? 1u : 0u;
    return rc;
}

int yowza_queryisa_run(void* segdb_state, const uint8_t* prog,
                       uint32_t prog_len, uint8_t* out,
                       uint32_t out_cap, uint32_t* out_len,
                       uint64_t* scanned, uint64_t* mem_peak,
                       uint32_t* spilled, uint64_t snapshot_seq,
                       uint8_t* work, uint64_t work_cap) {
    return qi_run_impl(segdb_state, prog, prog_len, out, out_cap,
                       out_len, scanned, mem_peak, spilled,
                       snapshot_seq, work, work_cap, 0);
}

int yowza_queryisa_run_policy(void* segdb_state, const uint8_t* prog,
                              uint32_t prog_len, uint8_t* out,
                              uint32_t out_cap, uint32_t* out_len,
                              uint64_t* scanned, uint64_t* mem_peak,
                              uint32_t* spilled, uint64_t snapshot_seq,
                              uint8_t* work, uint64_t work_cap,
                              const QisaPolicy* policy) {
    return qi_run_impl(segdb_state, prog, prog_len, out, out_cap,
                       out_len, scanned, mem_peak, spilled,
                       snapshot_seq, work, work_cap, policy);
}

QISA_EXPORT("yowza_queryisa_version")
uint64_t yowza_queryisa_version(void) {
    return (uint64_t)QISA_VERSION;
}

QISA_EXPORT("yowza_queryisa_execute")
int yowza_queryisa_execute(void* state, const uint8_t* prog,
                           uint32_t prog_len, uint8_t* out,
                           uint32_t out_cap, uint32_t* out_len,
                           uint64_t* scanned, uint8_t* work,
                           uint64_t work_cap) {
    uint64_t dummy_peak = 0;
    uint32_t dummy_spilled = 0;
    return yowza_queryisa_run(state, prog, prog_len, out, out_cap,
                              out_len, scanned, &dummy_peak,
                              &dummy_spilled,
                              segdb_snapshot_seq((SegDB*)state),
                              work, work_cap);
}

/* Shard-coordinator classification (Reliability stage item 04).
 * Parses the program with the engine's own parser and reports
 * whether it is safe to run per-shard and concatenate: 1 when
 * the program contains no JOIN op, 0 when it contains a JOIN
 * (a per-shard join would drop cross-shard matches, so the host
 * coordinator must refuse it and route the query to the Go
 * executor instead), negative QISA_ERR_* on malformed program,
 * version mismatch, or work-buffer exhaustion. This function
 * classifies only; it never scans or executes. The host passes
 * its work buffer as scratch for the parse bump allocator. */
QISA_EXPORT("yowza_queryisa_shard_safe")
int yowza_queryisa_shard_safe(const uint8_t* prog, uint32_t prog_len,
                              uint8_t* work, uint64_t work_cap) {
    QiRd rd;
    QiBump bump;
    QiProg p;
    int rc;
    if (!prog || !work) return QISA_ERR_MALFORMED;
    bump.base = work;
    bump.cap = work_cap;
    bump.off = 0;
    rd.p = prog;
    rd.len = prog_len;
    rd.pos = 0;
    rd.bad = 0;
    {
        QisaPolicy defpol;
        qi_policy_resolve(0, &defpol);
        rc = qi_parse_prog(&rd, &bump, &p, &defpol);
        if (rc != QISA_OK) return rc;
        rc = qi_prog_validate(&p, &defpol);
    }
    if (rc != QISA_OK) return rc;
    return p.has_join ? 0 : 1;
}

QISA_EXPORT("yowza_queryisa_execute_policy")
int yowza_queryisa_execute_policy(void* state, const uint8_t* prog,
                                  uint32_t prog_len, uint8_t* out,
                                  uint32_t out_cap, uint32_t* out_len,
                                  uint64_t* scanned, uint64_t* mem_peak,
                                  uint32_t* spilled, uint64_t snapshot_seq,
                                  uint8_t* work, uint64_t work_cap,
                                  uint32_t max_width, uint32_t max_join_rows,
                                  uint32_t max_sort_rows, uint32_t max_groups) {
    QisaPolicy pol;
    pol.max_width = max_width;
    pol.max_join_rows = max_join_rows;
    pol.max_sort_rows = max_sort_rows;
    pol.max_groups = max_groups;
    return yowza_queryisa_run_policy(state, prog, prog_len, out,
                                     out_cap, out_len, scanned, mem_peak,
                                     spilled, snapshot_seq, work, work_cap,
                                     &pol);
}

QISA_EXPORT("yowza_queryisa_execute_at")
int yowza_queryisa_execute_at(void* state, const uint8_t* prog,
                              uint32_t prog_len, uint8_t* out,
                              uint32_t out_cap, uint32_t* out_len,
                              uint64_t* scanned, uint64_t* mem_peak,
                              uint32_t* spilled, uint64_t snapshot_seq,
                              uint8_t* work, uint64_t work_cap) {
    return yowza_queryisa_run(state, prog, prog_len, out, out_cap,
                              out_len, scanned, mem_peak, spilled,
                              snapshot_seq, work, work_cap);
}

/* ==================================================================
 * QBATCH — batch-at-a-time operators (Track A).
 * See include/query_isa.h, QBATCH ADDENDUM, for the contract.
 * These functions reuse the ISA engine's value, expression,
 * comparison, hash, accumulator, and sort machinery above, so a
 * batch operator cannot drift from the monolithic engine's
 * semantics: same qi_eval_tri, same qi_cmp, same qi_acc_add,
 * same stable qi_sort_rec. Rows are decoded one at a time
 * from the input buffer (a 64-column stack row), never as a
 * whole-batch array, so a batch call's transient memory is
 * O(width), not O(batch); retained state lives in the
 * host-provided state buffer bump.
 * ================================================================== */

#define QB_STATE_HDR_SIZE 1024u
#define QB_HASH_SLOTS 4096u

typedef struct {
    uint32_t magic;      /* 0: QBATCH_MAGIC */
    uint32_t kind;       /* 4 */
    uint32_t width;      /* 8: build/right width (join), row width */
    uint32_t nrows;      /* 12 */
    uint64_t budget;     /* 16 */
    uint64_t retained;   /* 24 */
    uint64_t peak;       /* 32 */
    uint32_t spilled;    /* 40 */
    uint32_t flags;      /* 44 */
    uint32_t left_width; /* 48 */
    uint32_t left_key;   /* 52 */
    uint32_t right_width;/* 56 */
    uint32_t right_key;  /* 60 */
    uint32_t ngroup;     /* 64 */
    uint32_t nagg;       /* 68 */
    uint32_t group_cols[QBATCH_MAX_WIDTH]; /* 72 */
    uint8_t agg_code[QBATCH_MAX_WIDTH];
    uint8_t agg_star[QBATCH_MAX_WIDTH];
    uint32_t agg_col[QBATCH_MAX_WIDTH];
    uint32_t ndistinct;  /* aggregate distinct groups ever seen */
    uint32_t hash_built;
    uint32_t build_kinds; /* bitmask over value kinds, non-null */
    QiVec vec;            /* join build rows / sort rows */
    QiHash hash;          /* join probe hash, built lazily */
    QiGroup* epoch;       /* aggregate current-epoch groups */
    uint32_t nepoch;
    uint32_t cap_epoch;
    int32_t* epoch_head;
    QiGroup* merged;      /* aggregate merged groups, in order */
    uint32_t nmerged;
    uint32_t cap_merged;
    int32_t* merged_head;
    QiBump bump;          /* bump over state buffer tail */
    /* High-capacity extension (after bump, so the fixed
     * header offsets 0..47 the host reads are untouched):
     * runtime row/group caps (0 never stored: init maps 0
     * to the compile-time defaults) and the last preflight
     * need (region cap the failed batch requires, or 0). */
    uint32_t max_rows;
    uint32_t max_groups;
    uint64_t last_need;
} QBState;

/* Header size rounded up: the bump starts here, so a state
 * region's bump base is always state + qb_base_off(). The
 * host relocation fixup and qb_state_get both rely on it. */
static uint64_t qb_base_off(void) {
    return ((uint64_t)sizeof(QBState) + 7u) & ~7u;
}

/* Initialise a fresh state buffer. The bump starts after the
 * header struct (aligned); everything retained lives there.
 * max_rows/max_groups of 0 select the compile-time
 * defaults (plain *_init); *_init2 passes host values. */
static int qb_state_init(uint8_t* state, uint64_t state_cap,
                         uint32_t kind, uint32_t width,
                         uint64_t budget, uint32_t max_rows,
                         uint32_t max_groups, QBState** out) {
    QBState* st;
    uint64_t base_off;
    if (!state || state_cap < (uint64_t)sizeof(QBState) + 65536u)
        return QISA_ERR_OOM;
    qi_zero(state, (uint32_t)sizeof(QBState));
    st = (QBState*)state;
    st->magic = QBATCH_MAGIC;
    st->kind = kind;
    st->width = width;
    st->nrows = 0;
    st->budget = budget;
    st->retained = 0;
    st->peak = 0;
    st->spilled = 0;
    st->flags = 0;
    st->max_rows = max_rows == 0u ? QBATCH_MAX_ROWS : max_rows;
    st->max_groups =
        max_groups == 0u ? QBATCH_MAX_GROUPS : max_groups;
    st->last_need = 0;
    base_off = qb_base_off();
    st->bump.base = state + base_off;
    st->bump.cap = state_cap - base_off;
    st->bump.off = 0;
    *out = st;
    return QISA_OK;
}

static int qb_state_get(uint8_t* state, uint64_t state_cap,
                        uint32_t kind, uint32_t width,
                        QBState** out) {
    QBState* st;
    uint64_t base_off;
    if (!state || state_cap < (uint64_t)sizeof(QBState) + 65536u)
        return QISA_ERR_OOM;
    st = (QBState*)state;
    /* kind==0 skips the kind check (state_info only). */
    if (st->magic != QBATCH_MAGIC ||
        (kind != 0u && st->kind != kind))
        return QISA_ERR_MALFORMED;
    /* width==0 skips the width check (join/sort callers
     * validate their own width fields after the call). */
    if (width != 0u && st->width != width)
        return QISA_ERR_MALFORMED;
    if (st->bump.off > st->bump.cap) return QISA_ERR_MALFORMED;
    /* Relocation invariant: the bump must start exactly
     * after this state's header and span exactly to the
     * region end. A raw byte copy to a new address without
     * the relocate fixup fails here loudly (its bump base
     * still points at the old region) instead of corrupting
     * memory silently. */
    base_off = qb_base_off();
    if (st->bump.base != state + base_off ||
        st->bump.cap != state_cap - base_off)
        return QISA_ERR_MALFORMED;
    *out = st;
    return QISA_OK;
}

static void qb_note_peak(QBState* st, uint64_t v) {
    if (v > st->peak) st->peak = v;
}

/* Decode one row of width values from rd into row[]. Values
 * point into the input buffer (transient); retained callers
 * deep-copy via qi_vec_append / qi_vcopy_persist / qi_acc_add. */
static int qb_decode_row(QiRd* rd, QiVal* row, uint32_t width) {
    uint32_t c;
    for (c = 0; c < width; c++) {
        int rc = qi_decode_val(rd, &row[c]);
        if (rc != QISA_OK) return rc;
    }
    if (rd->bad) return QISA_ERR_MALFORMED;
    return QISA_OK;
}

/* Open a batch: validate header/count, return a reader
 * positioned at the first value. nrows==0 is legal. */
static int qb_batch_open(const uint8_t* in, uint32_t in_len,
                         uint32_t width, QiRd* rd, uint32_t* nrows) {
    if (!in || in_len < 4u) return QISA_ERR_MALFORMED;
    if (width == 0u || width > QBATCH_MAX_WIDTH)
        return QISA_ERR_LIMIT;
    rd->p = in;
    rd->len = in_len;
    rd->pos = 0;
    rd->bad = 0;
    *nrows = qi_ru32(rd);
    if (rd->bad) return QISA_ERR_MALFORMED;
    return QISA_OK;
}

static int qb_batch_close(QiRd* rd) {
    if (rd->bad || rd->pos != rd->len) return QISA_ERR_MALFORMED;
    return QISA_OK;
}

/* Batch byte size under Go accounting: 32 + name/value sizes. */
static uint64_t qb_batch_bytes(const QiVal* row, uint32_t width,
                               const uint32_t* name_lens) {
    return qi_row_size(row, width, name_lens);
}

/* ---- capacity preflight simulation (High Capacity) ----
 * A preflight simulates a batch append against a VIRTUAL
 * bump offset (starting at the state's real bump.off) using
 * exactly the allocation order and sizes of the real
 * append, but writing nothing to operator state. If the
 * virtual offset ends within bump.cap, the real append
 * cannot exhaust the bump; if it ends beyond cap, the
 * call returns QISA_ERR_OOM having consumed nothing, with
 * last_need set to the region cap the batch requires.
 * qi_bump aligns every allocation to 8 bytes; the sim
 * mirrors that, saturating (never wrapping) on overflow. */
static void qb_sim_bump(uint64_t* off, uint64_t n) {
    uint64_t a;
    if (n == 0u) n = 1u;
    a = (*off + 7u) & ~7u;
    if (a > (uint64_t)~0u - n) {
        *off = (uint64_t)~0u;
        return;
    }
    *off = a + n;
}

/* Saturating add for the preflight margin (see below). */
static void qb_sim_add(uint64_t* off, uint64_t n) {
    if (n > (uint64_t)~0u - *off) *off = (uint64_t)~0u;
    else *off += n;
}

/* Virtual retained-row vector state for join/sort sims. */
typedef struct {
    uint64_t off;
    uint32_t nrows;
    uint32_t nchunks;
    uint32_t cap_chunks;
} QbVecSim;

/* Simulate qi_vec_append for one row, in its exact order:
 * chunk-pointer-array growth, then the chunk, then one
 * string-payload allocation per string/bytes value. */
static void qb_vecsim_row(QbVecSim* s, const QiVal* row,
                          uint32_t width) {
    uint32_t c;
    if (s->nrows % QI_VEC_CHUNK == 0u) {
        if (s->nchunks >= s->cap_chunks) {
            uint32_t ncap =
                s->cap_chunks == 0u ? 16u : s->cap_chunks * 2u;
            qb_sim_bump(&s->off, (uint64_t)ncap * 8u);
            s->cap_chunks = ncap;
        }
        qb_sim_bump(&s->off, (uint64_t)QI_VEC_CHUNK * width *
                                 (uint64_t)sizeof(QiVal));
        s->nchunks++;
    }
    for (c = 0; c < width; c++) {
        if ((row[c].kind == (uint8_t)QISA_KIND_STRING ||
             row[c].kind == (uint8_t)QISA_KIND_BYTES) &&
            row[c].len > 0u)
            qb_sim_bump(&s->off, row[c].len);
    }
    s->nrows++;
}

/* Relocation pointer fixup: adjust p by delta iff p lies
 * inside [old_base, old_base + old_cap); anything else
 * (NULL, an input-buffer pointer for a zero-length string,
 * a pointer into another region) is left untouched. */
static uint8_t* qb_fix_ptr(uint8_t* p, uint64_t old_base,
                           uint64_t old_cap, int64_t delta) {
    uint64_t a = (uint64_t)(uintptr_t)p;
    uint64_t n;
    if (a < old_base || a >= old_base + old_cap) return p;
    n = (uint64_t)((int64_t)a + delta);
    return (uint8_t*)(uintptr_t)n;
}

static void qb_fix_val_str(QiVal* v, uint64_t old_base,
                           uint64_t old_cap, int64_t delta) {
    if (v->kind == (uint8_t)QISA_KIND_STRING ||
        v->kind == (uint8_t)QISA_KIND_BYTES)
        v->s = qb_fix_ptr((uint8_t*)v->s, old_base, old_cap,
                          delta);
}

QISA_EXPORT("yowza_qbatch_state_relocate")
int yowza_qbatch_state_relocate(uint8_t* state, uint64_t state_cap,
                                uint64_t old_base,
                                uint64_t old_cap) {
    QBState* st;
    uint64_t base_off, new_base;
    int64_t delta;
    uint32_t i, c, g, a;
    if (!state || state_cap < (uint64_t)sizeof(QBState) + 65536u)
        return QISA_ERR_OOM;
    st = (QBState*)state;
    if (st->magic != QBATCH_MAGIC || st->kind < 1u ||
        st->kind > 3u)
        return QISA_ERR_MALFORMED;
    /* The copy must have come from old_base: its bump base
     * still points just past the old header. Regions must
     * not overlap (the host always appends at the tail). */
    base_off = qb_base_off();
    new_base = (uint64_t)(uintptr_t)state;
    if (st->bump.base != (uint8_t*)(uintptr_t)(old_base + base_off))
        return QISA_ERR_MALFORMED;
    if (new_base < old_base + old_cap &&
        old_base < new_base + state_cap)
        return QISA_ERR_MALFORMED;
    delta = (int64_t)(new_base - old_base);
    /* Bump header itself. */
    st->bump.base = qb_fix_ptr(st->bump.base, old_base, old_cap,
                               delta);
    st->bump.cap = state_cap - base_off;
    /* Retained-row vector: chunk array, then chunk
     * pointers, then every retained value's string. */
    if (st->vec.chunks) {
        st->vec.chunks = (QiVal**)qb_fix_ptr(
            (uint8_t*)st->vec.chunks, old_base, old_cap, delta);
        for (i = 0; i < st->vec.nchunks; i++)
            st->vec.chunks[i] = (QiVal*)qb_fix_ptr(
                (uint8_t*)st->vec.chunks[i], old_base, old_cap,
                delta);
        for (i = 0; i < st->vec.nrows; i++) {
            QiVal* row = qi_vec_row(&st->vec, i);
            for (c = 0; c < st->vec.width; c++)
                qb_fix_val_str(&row[c], old_base, old_cap,
                               delta);
        }
    }
    /* Join hash: arrays, then canonical key payloads. */
    st->hash.head = (int32_t*)qb_fix_ptr(
        (uint8_t*)st->hash.head, old_base, old_cap, delta);
    st->hash.next = (int32_t*)qb_fix_ptr(
        (uint8_t*)st->hash.next, old_base, old_cap, delta);
    st->hash.canon_len = (uint32_t*)qb_fix_ptr(
        (uint8_t*)st->hash.canon_len, old_base, old_cap, delta);
    if (st->hash.canon) {
        st->hash.canon = (const uint8_t**)qb_fix_ptr(
            (uint8_t*)st->hash.canon, old_base, old_cap, delta);
        for (i = 0; i < st->vec.nrows; i++) {
            if (st->hash.canon[i])
                st->hash.canon[i] = qb_fix_ptr(
                    (uint8_t*)st->hash.canon[i], old_base,
                    old_cap, delta);
        }
    }
    /* Aggregate heads and group sets (epoch, then merged).
     * Group arrays are fixed before their groups are
     * walked, and group key/value/accumulator arrays before
     * the values inside them. */
    st->epoch_head = (int32_t*)qb_fix_ptr(
        (uint8_t*)st->epoch_head, old_base, old_cap, delta);
    st->merged_head = (int32_t*)qb_fix_ptr(
        (uint8_t*)st->merged_head, old_base, old_cap, delta);
    if (st->epoch) {
        st->epoch = (QiGroup*)qb_fix_ptr(
            (uint8_t*)st->epoch, old_base, old_cap, delta);
        for (g = 0; g < st->nepoch; g++) {
            QiGroup* gr = &st->epoch[g];
            gr->key = qb_fix_ptr((uint8_t*)gr->key, old_base,
                                 old_cap, delta);
            gr->key_vals = (QiVal*)qb_fix_ptr(
                (uint8_t*)gr->key_vals, old_base, old_cap,
                delta);
            gr->accs = (QiAcc*)qb_fix_ptr(
                (uint8_t*)gr->accs, old_base, old_cap, delta);
            for (c = 0; c < st->ngroup; c++)
                qb_fix_val_str(&gr->key_vals[c], old_base,
                               old_cap, delta);
            for (a = 0; a < st->nagg; a++) {
                qb_fix_val_str(&gr->accs[a].mn, old_base,
                               old_cap, delta);
                qb_fix_val_str(&gr->accs[a].mx, old_base,
                               old_cap, delta);
            }
        }
    }
    if (st->merged) {
        st->merged = (QiGroup*)qb_fix_ptr(
            (uint8_t*)st->merged, old_base, old_cap, delta);
        for (g = 0; g < st->nmerged; g++) {
            QiGroup* gr = &st->merged[g];
            gr->key = qb_fix_ptr((uint8_t*)gr->key, old_base,
                                 old_cap, delta);
            gr->key_vals = (QiVal*)qb_fix_ptr(
                (uint8_t*)gr->key_vals, old_base, old_cap,
                delta);
            gr->accs = (QiAcc*)qb_fix_ptr(
                (uint8_t*)gr->accs, old_base, old_cap, delta);
            for (c = 0; c < st->ngroup; c++)
                qb_fix_val_str(&gr->key_vals[c], old_base,
                               old_cap, delta);
            for (a = 0; a < st->nagg; a++) {
                qb_fix_val_str(&gr->accs[a].mn, old_base,
                               old_cap, delta);
                qb_fix_val_str(&gr->accs[a].mx, old_base,
                               old_cap, delta);
            }
        }
    }
    return QISA_OK;
}

QISA_EXPORT("yowza_qbatch_state_info")
int yowza_qbatch_state_info(uint8_t* state, uint64_t state_cap,
                            uint64_t* bump_used,
                            uint64_t* last_need) {
    QBState* st;
    int rc;
    if (!bump_used || !last_need) return QISA_ERR_MALFORMED;
    rc = qb_state_get(state, state_cap, 0, 0, &st);
    if (rc != QISA_OK) return rc;
    *bump_used = st->bump.off;
    *last_need = st->last_need;
    return QISA_OK;
}

/* ---- stateless: filter ---- */

QISA_EXPORT("yowza_qbatch_filter")
int yowza_qbatch_filter(const uint8_t* in, uint32_t in_len,
                        uint32_t width, const uint8_t* expr,
                        uint32_t expr_len, uint8_t* out,
                        uint32_t out_cap, uint32_t* out_len,
                        uint8_t* work, uint64_t work_cap) {
    QiRd rd;
    QiRd erd;
    QiBump bump;
    QiExpr* e = 0;
    QiVal row[QBATCH_MAX_WIDTH];
    uint32_t nrows, i, kept = 0, off = 4;
    int rc, tri;
    if (!out || !out_len || !work || work_cap < 65536u)
        return QISA_ERR_OOM;
    if (!expr || expr_len == 0u) return QISA_ERR_MALFORMED;
    rc = qb_batch_open(in, in_len, width, &rd, &nrows);
    if (rc != QISA_OK) return rc;
    bump.base = work;
    bump.cap = work_cap;
    bump.off = 0;
    erd.p = expr;
    erd.len = expr_len;
    erd.pos = 0;
    erd.bad = 0;
    rc = qi_decode_expr(&erd, &bump, 0, &e);
    if (rc != QISA_OK) return rc;
    if (erd.bad || erd.pos != erd.len) return QISA_ERR_MALFORMED;
    if (!qi_expr_cols_ok(e, width)) return QISA_ERR_MALFORMED;
    if (out_cap < 4u) return QISA_ERR_OUT_SMALL;
    for (i = 0; i < nrows; i++) {
        uint32_t c;
        rc = qb_decode_row(&rd, row, width);
        if (rc != QISA_OK) return rc;
        rc = qi_eval_tri(e, row, width, &bump, &tri);
        if (rc != QISA_OK) return rc;
        if (tri != QI_TRI_TRUE) continue;
        for (c = 0; c < width; c++) {
            rc = qi_emit_val(out, out_cap, &off, &row[c]);
            if (rc != QISA_OK) return rc;
        }
        kept++;
    }
    rc = qb_batch_close(&rd);
    if (rc != QISA_OK) return rc;
    qi_wr32(out, kept);
    *out_len = off;
    return QISA_OK;
}

/* ---- stateless: project ---- */

QISA_EXPORT("yowza_qbatch_project")
int yowza_qbatch_project(const uint8_t* in, uint32_t in_len,
                         uint32_t width, const uint32_t* cols,
                         uint32_t ncols, uint8_t* out,
                         uint32_t out_cap, uint32_t* out_len) {
    QiRd rd;
    QiVal row[QBATCH_MAX_WIDTH];
    uint32_t nrows, i, c, off = 4;
    int rc;
    if (!out || !out_len) return QISA_ERR_MALFORMED;
    if (ncols > QBATCH_MAX_WIDTH) return QISA_ERR_LIMIT;
    if (ncols > 0u && !cols) return QISA_ERR_MALFORMED;
    for (c = 0; c < ncols; c++)
        if (cols[c] >= width) return QISA_ERR_MALFORMED;
    rc = qb_batch_open(in, in_len, width, &rd, &nrows);
    if (rc != QISA_OK) return rc;
    if (out_cap < 4u) return QISA_ERR_OUT_SMALL;
    for (i = 0; i < nrows; i++) {
        rc = qb_decode_row(&rd, row, width);
        if (rc != QISA_OK) return rc;
        for (c = 0; c < ncols; c++) {
            rc = qi_emit_val(out, out_cap, &off, &row[cols[c]]);
            if (rc != QISA_OK) return rc;
        }
    }
    rc = qb_batch_close(&rd);
    if (rc != QISA_OK) return rc;
    qi_wr32(out, nrows);
    *out_len = off;
    return QISA_OK;
}

/* ---- stateful: join ---- */

QISA_EXPORT("yowza_qbatch_join_init")
int yowza_qbatch_join_init(uint8_t* state, uint64_t state_cap,
                           uint32_t left_width, uint32_t left_key,
                           uint32_t right_width, uint32_t right_key,
                           uint64_t budget) {
    QBState* st;
    int rc;
    if (left_width == 0u || left_width > QBATCH_MAX_WIDTH ||
        right_width == 0u || right_width > QBATCH_MAX_WIDTH ||
        left_key >= left_width || right_key >= right_width)
        return QISA_ERR_LIMIT;
    rc = qb_state_init(state, state_cap, QBATCH_KIND_JOIN,
                       right_width, budget, 0, 0, &st);
    if (rc != QISA_OK) return rc;
    st->left_width = left_width;
    st->left_key = left_key;
    st->right_width = right_width;
    st->right_key = right_key;
    st->vec.width = right_width;
    return QISA_OK;
}

QISA_EXPORT("yowza_qbatch_join_init2")
int yowza_qbatch_join_init2(uint8_t* state, uint64_t state_cap,
                            uint32_t left_width, uint32_t left_key,
                            uint32_t right_width, uint32_t right_key,
                            uint64_t budget, uint32_t max_rows) {
    QBState* st;
    int rc;
    if (left_width == 0u || left_width > QBATCH_MAX_WIDTH ||
        right_width == 0u || right_width > QBATCH_MAX_WIDTH ||
        left_key >= left_width || right_key >= right_width ||
        max_rows == 0u)
        return QISA_ERR_LIMIT;
    rc = qb_state_init(state, state_cap, QBATCH_KIND_JOIN,
                       right_width, budget, max_rows, 0, &st);
    if (rc != QISA_OK) return rc;
    st->left_width = left_width;
    st->left_key = left_key;
    st->right_width = right_width;
    st->right_key = right_key;
    st->vec.width = right_width;
    return QISA_OK;
}

/* Shared retained-batch accounting for join build and sort add:
 * single batch over budget -> BUDGET; retained overflow ->
 * logical spill (spilled=1, retained reset), then append. */
static int qb_retain_check(QBState* st, uint64_t batch_bytes) {
    if (st->budget > 0u && batch_bytes > st->budget)
        return QISA_ERR_BUDGET;
    if (st->budget > 0u && st->retained + batch_bytes > st->budget) {
        st->spilled = 1;
        st->retained = 0;
    }
    return QISA_OK;
}

QISA_EXPORT("yowza_qbatch_join_build")
int yowza_qbatch_join_build(uint8_t* state, uint64_t state_cap,
                            const uint8_t* in, uint32_t in_len,
                            const uint32_t* name_lens) {
    QBState* st;
    QiRd rd;
    QiVal row[QBATCH_MAX_WIDTH];
    uint32_t nrows, i;
    uint64_t batch_bytes = 0;
    int rc;
    rc = qb_state_get(state, state_cap, QBATCH_KIND_JOIN,
                      0, &st); /* width checked below */
    if (rc != QISA_OK) return rc;
    /* qb_state_get cannot know the caller's width here (the
     * state width IS the right width); re-validate directly. */
    if (st->width != st->right_width) return QISA_ERR_MALFORMED;
    rc = qb_batch_open(in, in_len, st->right_width, &rd, &nrows);
    if (rc != QISA_OK) return rc;
    if (nrows > 0u && !name_lens) return QISA_ERR_MALFORMED;
    if ((uint64_t)st->vec.nrows + nrows > st->max_rows)
        return QISA_ERR_LIMIT;
    st->last_need = 0;
    /* Pass 1: measure (rows point into the input buffer; the
     * vec append in pass 2 deep-copies) AND simulate the
     * append's bump usage exactly. A budget failure here
     * appends nothing, so the state stays usable; a
     * capacity failure here likewise appends nothing (the
     * old code could OOM mid-append in pass 2, leaving a
     * partial build). Pass 2 below can no longer exhaust
     * the bump: its allocation sequence is the simulated
     * one. */
    {
        QiRd mrd;
        QiVal mrow[QBATCH_MAX_WIDTH];
        QbVecSim vsim;
        uint32_t j;
        mrd.p = rd.p;
        mrd.len = rd.len;
        mrd.pos = rd.pos;
        mrd.bad = 0;
        vsim.off = st->bump.off;
        vsim.nrows = st->vec.nrows;
        vsim.nchunks = st->vec.nchunks;
        vsim.cap_chunks = st->vec.cap_chunks;
        for (j = 0; j < nrows; j++) {
            rc = qb_decode_row(&mrd, mrow, st->right_width);
            if (rc != QISA_OK) return rc;
            batch_bytes += qb_batch_bytes(mrow, st->right_width,
                                          name_lens);
            qb_vecsim_row(&vsim, mrow, st->right_width);
        }
        rc = qb_batch_close(&mrd);
        if (rc != QISA_OK) return rc;
        if (vsim.off > st->bump.cap) {
            st->last_need = qb_base_off() + vsim.off;
            return QISA_ERR_OOM;
        }
    }
    rc = qb_retain_check(st, batch_bytes);
    if (rc != QISA_OK) return rc;
    for (i = 0; i < nrows; i++) {
        const QiVal* kv;
        rc = qb_decode_row(&rd, row, st->right_width);
        if (rc != QISA_OK) return rc;
        kv = &row[st->right_key];
        if (kv->kind != (uint8_t)QISA_KIND_NULL &&
            kv->kind <= (uint8_t)QISA_KIND_BYTES)
            st->build_kinds |= 1u << kv->kind;
        rc = qi_vec_append(&st->vec, row, &st->bump);
        if (rc != QISA_OK) return rc;
    }
    rc = qb_batch_close(&rd);
    if (rc != QISA_OK) return rc;
    st->retained += batch_bytes;
    qb_note_peak(st, st->retained);
    st->nrows = st->vec.nrows;
    return QISA_OK;
}

/* Kind compatibility for join keys, mirroring Go Compare:
 * numeric with numeric, otherwise same kind only. */
static int qb_kinds_compat(uint8_t lk, uint32_t build_kinds) {
    uint32_t k;
    for (k = 0; k <= (uint32_t)QISA_KIND_BYTES; k++) {
        uint8_t bk;
        int compat;
        if (!(build_kinds & (1u << k))) continue;
        bk = (uint8_t)k;
        if (bk == (uint8_t)QISA_KIND_NULL) continue;
        compat = 0;
        if ((lk == (uint8_t)QISA_KIND_INT ||
             lk == (uint8_t)QISA_KIND_FLOAT) &&
            (bk == (uint8_t)QISA_KIND_INT ||
             bk == (uint8_t)QISA_KIND_FLOAT))
            compat = 1;
        else if (lk == bk)
            compat = 1;
        if (!compat) return 0;
    }
    return 1;
}

QISA_EXPORT("yowza_qbatch_join_probe")
int yowza_qbatch_join_probe(uint8_t* state, uint64_t state_cap,
                            const uint8_t* in, uint32_t in_len,
                            uint8_t* out, uint32_t out_cap,
                            uint32_t* out_len, uint8_t* work,
                            uint64_t work_cap) {
    QBState* st;
    QiRd rd;
    QiVal row[QBATCH_MAX_WIDTH];
    QiBump wbump;
    uint32_t nrows, i, off = 4, emitted = 0;
    int32_t* matches = 0;
    int rc;
    if (!out || !out_len || !work || work_cap < 65536u)
        return QISA_ERR_OOM;
    rc = qb_state_get(state, state_cap, QBATCH_KIND_JOIN,
                      0, &st);
    if (rc != QISA_OK) return rc;
    if (st->width != st->right_width) return QISA_ERR_MALFORMED;
    rc = qb_batch_open(in, in_len, st->left_width, &rd, &nrows);
    if (rc != QISA_OK) return rc;
    if (out_cap < 4u) return QISA_ERR_OUT_SMALL;
    st->last_need = 0;
    if (!st->hash_built) {
        /* Preflight the lazy hash build exactly (same
         * allocation order and sizes as qi_hash_build):
         * a capacity failure builds nothing, so a retry
         * after growth rebuilds from a clean state. An
         * overlong key (canon length 0) is not a capacity
         * failure: last_need stays 0, telling the host
         * growth cannot help. */
        uint64_t sim_off = st->bump.off;
        uint32_t size = 16, bi;
        while (size < st->vec.nrows * 2u + 16u) size *= 2u;
        qb_sim_bump(&sim_off, (uint64_t)size * 4u);
        qb_sim_bump(&sim_off, (uint64_t)(st->vec.nrows == 0u
                                            ? 1u
                                            : st->vec.nrows) *
                                     4u);
        qb_sim_bump(&sim_off, (uint64_t)(st->vec.nrows == 0u
                                            ? 1u
                                            : st->vec.nrows) *
                                     8u);
        qb_sim_bump(&sim_off, (uint64_t)(st->vec.nrows == 0u
                                            ? 1u
                                            : st->vec.nrows) *
                                     4u);
        for (bi = 0; bi < st->vec.nrows; bi++) {
            const QiVal* kv =
                &qi_vec_row(&st->vec, bi)[st->right_key];
            uint32_t clen;
            uint8_t tmp[2048];
            if (kv->kind == (uint8_t)QISA_KIND_NULL) continue;
            clen = qi_join_canon(kv, tmp);
            if (clen == 0u) return QISA_ERR_OOM;
            qb_sim_bump(&sim_off, clen);
        }
        if (sim_off > st->bump.cap) {
            st->last_need = qb_base_off() + sim_off;
            return QISA_ERR_OOM;
        }
        rc = qi_hash_build(&st->hash, &st->vec, st->vec.nrows,
                           st->right_key, &st->bump);
        if (rc != QISA_OK) return rc;
        st->hash_built = 1;
    }
    /* One reusable match array (build order is recovered by
     * reversing the chain walk, see below). */
    wbump.base = work;
    wbump.cap = work_cap;
    wbump.off = 0;
    matches = (int32_t*)qi_bump(
        &wbump,
        (uint64_t)(st->vec.nrows == 0u ? 1u : st->vec.nrows) * 4u);
    if (!matches) return QISA_ERR_OOM;
    for (i = 0; i < nrows; i++) {
        const QiVal* lk;
        uint8_t tmp[2048];
        uint32_t clen, slot, nmatch = 0, m;
        int32_t idx;
        rc = qb_decode_row(&rd, row, st->left_width);
        if (rc != QISA_OK) return rc;
        lk = &row[st->left_key];
        if (lk->kind == (uint8_t)QISA_KIND_NULL) continue;
        if (!qb_kinds_compat(lk->kind, st->build_kinds))
            return QISA_ERR_TYPE;
        clen = qi_join_canon(lk, tmp);
        if (clen == 0u) return QISA_ERR_OOM;
        slot = (uint32_t)(qi_fnv(tmp, clen) & st->hash.mask);
        idx = st->hash.head[slot];
        while (idx >= 0) {
            if (st->hash.canon_len[(uint32_t)idx] == clen &&
                qi_eq(st->hash.canon[(uint32_t)idx], tmp, clen)) {
                matches[nmatch++] = idx;
            }
            idx = st->hash.next[(uint32_t)idx];
        }
        /* Chain order is reverse build order (prepend on
         * build); emitting reversed restores build order
         * per left row, exactly as the Go in-memory probe
         * scans the build side in insertion order. */
        for (m = nmatch; m > 0u; m--) {
            const QiVal* rrow =
                qi_vec_row(&st->vec, (uint32_t)matches[m - 1u]);
            uint32_t c;
            for (c = 0; c < st->left_width; c++) {
                rc = qi_emit_val(out, out_cap, &off, &row[c]);
                if (rc != QISA_OK) return rc;
            }
            for (c = 0; c < st->right_width; c++) {
                rc = qi_emit_val(out, out_cap, &off, &rrow[c]);
                if (rc != QISA_OK) return rc;
            }
            emitted++;
        }
    }
    rc = qb_batch_close(&rd);
    if (rc != QISA_OK) return rc;
    qi_wr32(out, emitted);
    *out_len = off;
    return QISA_OK;
}

/* ---- stateful: sort ---- */

QISA_EXPORT("yowza_qbatch_sort_init")
int yowza_qbatch_sort_init(uint8_t* state, uint64_t state_cap,
                           uint32_t width, uint64_t budget) {
    QBState* st;
    int rc;
    if (width == 0u || width > QBATCH_MAX_WIDTH)
        return QISA_ERR_LIMIT;
    rc = qb_state_init(state, state_cap, QBATCH_KIND_SORT, width,
                       budget, 0, 0, &st);
    if (rc != QISA_OK) return rc;
    st->vec.width = width;
    return QISA_OK;
}

QISA_EXPORT("yowza_qbatch_sort_init2")
int yowza_qbatch_sort_init2(uint8_t* state, uint64_t state_cap,
                            uint32_t width, uint64_t budget,
                            uint32_t max_rows) {
    QBState* st;
    int rc;
    if (width == 0u || width > QBATCH_MAX_WIDTH || max_rows == 0u)
        return QISA_ERR_LIMIT;
    rc = qb_state_init(state, state_cap, QBATCH_KIND_SORT, width,
                       budget, max_rows, 0, &st);
    if (rc != QISA_OK) return rc;
    st->vec.width = width;
    return QISA_OK;
}

QISA_EXPORT("yowza_qbatch_sort_add")
int yowza_qbatch_sort_add(uint8_t* state, uint64_t state_cap,
                          const uint8_t* in, uint32_t in_len,
                          const uint32_t* name_lens) {
    QBState* st;
    QiRd rd;
    QiVal row[QBATCH_MAX_WIDTH];
    uint32_t nrows, i;
    uint64_t batch_bytes = 0;
    int rc;
    rc = qb_state_get(state, state_cap, QBATCH_KIND_SORT, 0, &st);
    if (rc != QISA_OK) return rc;
    rc = qb_batch_open(in, in_len, st->width, &rd, &nrows);
    if (rc != QISA_OK) return rc;
    if (nrows > 0u && !name_lens) return QISA_ERR_MALFORMED;
    if ((uint64_t)st->vec.nrows + nrows > st->max_rows)
        return QISA_ERR_LIMIT;
    st->last_need = 0;
    /* Pass 1 with exact bump simulation (see join_build):
     * a capacity failure appends nothing. */
    {
        QiRd mrd;
        QiVal mrow[QBATCH_MAX_WIDTH];
        QbVecSim vsim;
        uint32_t j;
        mrd.p = rd.p;
        mrd.len = rd.len;
        mrd.pos = rd.pos;
        mrd.bad = 0;
        vsim.off = st->bump.off;
        vsim.nrows = st->vec.nrows;
        vsim.nchunks = st->vec.nchunks;
        vsim.cap_chunks = st->vec.cap_chunks;
        for (j = 0; j < nrows; j++) {
            rc = qb_decode_row(&mrd, mrow, st->width);
            if (rc != QISA_OK) return rc;
            batch_bytes += qb_batch_bytes(mrow, st->width,
                                          name_lens);
            qb_vecsim_row(&vsim, mrow, st->width);
        }
        rc = qb_batch_close(&mrd);
        if (rc != QISA_OK) return rc;
        if (vsim.off > st->bump.cap) {
            st->last_need = qb_base_off() + vsim.off;
            return QISA_ERR_OOM;
        }
    }
    rc = qb_retain_check(st, batch_bytes);
    if (rc != QISA_OK) return rc;
    for (i = 0; i < nrows; i++) {
        rc = qb_decode_row(&rd, row, st->width);
        if (rc != QISA_OK) return rc;
        rc = qi_vec_append(&st->vec, row, &st->bump);
        if (rc != QISA_OK) return rc;
    }
    rc = qb_batch_close(&rd);
    if (rc != QISA_OK) return rc;
    st->retained += batch_bytes;
    qb_note_peak(st, st->retained);
    st->nrows = st->vec.nrows;
    return QISA_OK;
}

/* ---- stateful: sort finish ---- */

QISA_EXPORT("yowza_qbatch_sort_finish")
int yowza_qbatch_sort_finish(uint8_t* state, uint64_t state_cap,
                             const uint32_t* keys, uint32_t nkeys,
                             uint8_t* out, uint32_t out_cap,
                             uint32_t* out_len, uint8_t* work,
                             uint64_t work_cap) {
    QBState* st;
    QiBump wbump;
    QiSortKey* skeys = 0;
    QiSortCtx ctx;
    int32_t* idx = 0;
    int32_t* tmp = 0;
    uint32_t i, c, off = 4;
    int rc;
    if (!out || !out_len || !work || work_cap < 65536u)
        return QISA_ERR_OOM;
    if (nkeys > QBATCH_MAX_WIDTH) return QISA_ERR_LIMIT;
    if (nkeys > 0u && !keys) return QISA_ERR_MALFORMED;
    rc = qb_state_get(state, state_cap, QBATCH_KIND_SORT, 0, &st);
    if (rc != QISA_OK) return rc;
    /* Sort finish allocates no state (work only), so its
     * OOMs are never capacity failures: clear last_need
     * so the host never grows state for them. */
    st->last_need = 0;
    wbump.base = work;
    wbump.cap = work_cap;
    wbump.off = 0;
    if (nkeys > 0u) {
        skeys = (QiSortKey*)qi_bump(&wbump,
                                    (uint64_t)nkeys *
                                        (uint64_t)sizeof(QiSortKey));
        if (!skeys) return QISA_ERR_OOM;
        for (i = 0; i < nkeys; i++) {
            uint32_t col = qi_rd32((const uint8_t*)keys +
                                   (uint64_t)i * 8u);
            uint32_t desc = qi_rd32((const uint8_t*)keys +
                                    (uint64_t)i * 8u + 4u);
            if (col >= st->width) return QISA_ERR_MALFORMED;
            skeys[i].col = col;
            skeys[i].desc = desc != 0u ? 1 : 0;
        }
    }
    idx = (int32_t*)qi_bump(
        &wbump, (uint64_t)(st->vec.nrows == 0u ? 1u : st->vec.nrows) *
                    4u);
    tmp = (int32_t*)qi_bump(
        &wbump, (uint64_t)(st->vec.nrows == 0u ? 1u : st->vec.nrows) *
                    4u);
    if (!idx || !tmp) return QISA_ERR_OOM;
    for (i = 0; i < st->vec.nrows; i++) idx[i] = (int32_t)i;
    ctx.vec = &st->vec;
    ctx.width = st->width;
    ctx.keys = skeys;
    ctx.nkeys = nkeys;
    if (st->vec.nrows > 1u && nkeys > 0u)
        qi_sort_rec(&ctx, idx, tmp, 0, st->vec.nrows);
    if (out_cap < 4u) return QISA_ERR_OUT_SMALL;
    for (i = 0; i < st->vec.nrows; i++) {
        const QiVal* row = qi_vec_row(&st->vec, (uint32_t)idx[i]);
        for (c = 0; c < st->width; c++) {
            rc = qi_emit_val(out, out_cap, &off, &row[c]);
            if (rc != QISA_OK) return rc;
        }
    }
    qi_wr32(out, st->vec.nrows);
    *out_len = off;
    st->nrows = st->vec.nrows;
    return QISA_OK;
}

/* ---- stateful: aggregate ---- */

/* Canonical group-key bytes for one row, kind-tagged exactly
 * like qi_group_key_fill (int/float keep distinct tags, as
 * Go's groupKey does), but over an explicit column list. */
static uint32_t qb_group_key_size(const QiVal* row,
                                  const uint32_t* cols,
                                  uint32_t ngroup) {
    uint32_t i, size = 0;
    for (i = 0; i < ngroup; i++) {
        const QiVal* v = &row[cols[i]];
        switch (v->kind) {
        case QISA_KIND_NULL: size += 1u; break;
        case QISA_KIND_INT:
        case QISA_KIND_FLOAT: size += 9u; break;
        case QISA_KIND_STRING:
        case QISA_KIND_BYTES: size += 5u + v->len; break;
        case QISA_KIND_BOOL: size += 2u; break;
        default: size += 1u; break;
        }
    }
    return size;
}

static void qb_group_key_fill(const QiVal* row, const uint32_t* cols,
                              uint32_t ngroup, uint8_t* dst) {
    uint32_t i, o = 0;
    for (i = 0; i < ngroup; i++) {
        const QiVal* v = &row[cols[i]];
        switch (v->kind) {
        case QISA_KIND_NULL:
            dst[o++] = 0;
            break;
        case QISA_KIND_INT:
            dst[o++] = 1;
            qi_wr64(dst + o, (uint64_t)v->i);
            o += 8u;
            break;
        case QISA_KIND_FLOAT: {
            uint64_t bits;
            double d = v->f;
            if (d != d) bits = 0x7FF8000000000000u;
            else bits = qi_dbl_bits(d);
            dst[o++] = 2;
            qi_wr64(dst + o, bits);
            o += 8u;
            break;
        }
        case QISA_KIND_STRING:
            dst[o++] = 3;
            qi_wr32(dst + o, v->len);
            o += 4u;
            qi_copy(dst + o, v->s, v->len);
            o += v->len;
            break;
        case QISA_KIND_BOOL:
            dst[o++] = 4;
            dst[o++] = v->i != 0 ? 1 : 0;
            break;
        case QISA_KIND_BYTES:
            dst[o++] = 5;
            qi_wr32(dst + o, v->len);
            o += 4u;
            qi_copy(dst + o, v->s, v->len);
            o += v->len;
            break;
        default:
            dst[o++] = 0;
            break;
        }
    }
}

/* Go's measuredGroupSize exactly: 48 + key sizes + 24/acc +
 * min/max sizes when present. */
static uint64_t qb_groups_bytes(QiGroup* groups, uint32_t n,
                                uint32_t ngroup, uint32_t nagg) {
    uint64_t total = 0;
    uint32_t g, a;
    for (g = 0; g < n; g++) {
        QiGroup* gr = &groups[g];
        uint64_t sz = 48u;
        for (a = 0; a < ngroup; a++)
            sz += qi_value_size(&gr->key_vals[a]);
        for (a = 0; a < nagg; a++) {
            QiAcc* acc = &gr->accs[a];
            sz += 24u;
            if (acc->has)
                sz += qi_value_size(&acc->mn) +
                      qi_value_size(&acc->mx);
        }
        total += sz;
    }
    return total;
}

static int32_t qb_group_find(QiGroup* groups, uint32_t n,
                             int32_t* head, const uint8_t* key,
                             uint32_t klen) {
    uint32_t slot;
    int32_t gi;
    uint32_t steps = 0;
    if (!head) return -1;
    slot = (uint32_t)(qi_fnv(key, klen) & (QB_HASH_SLOTS - 1u));
    gi = head[slot];
    while (gi >= 0 && steps <= n) {
        QiGroup* cand = &groups[(uint32_t)gi];
        if (cand->klen == klen && qi_eq(cand->key, key, klen))
            return gi;
        gi = cand->next;
        steps++;
    }
    return -1;
}

static int qb_heads_init(QBState* st, int32_t** head_out) {
    int32_t* h;
    uint32_t i;
    h = (int32_t*)qi_bump(&st->bump,
                          (uint64_t)QB_HASH_SLOTS * 4u);
    if (!h) return QISA_ERR_OOM;
    for (i = 0; i < QB_HASH_SLOTS; i++) h[i] = -1;
    *head_out = h;
    return QISA_OK;
}

static void qb_heads_reset(int32_t* head) {
    uint32_t i;
    for (i = 0; i < QB_HASH_SLOTS; i++) head[i] = -1;
}

/* Append a fresh group to a set (array + hash). The caller
 * supplies key bytes already in the state bump, key_vals
 * and accs already allocated in the state bump. */
static int qb_group_append(QBState* st, QiGroup** arr, uint32_t* n,
                           uint32_t* cap, int32_t* head,
                           const uint8_t* key, uint32_t klen,
                           QiVal* key_vals, QiAcc* accs) {
    QiGroup* g;
    uint32_t slot;
    if (*n >= *cap) {
        uint32_t ncap = *cap == 0u ? 64u : *cap * 2u;
        QiGroup* nptr;
        uint32_t i;
        nptr = (QiGroup*)qi_bump(
            &st->bump, (uint64_t)ncap * (uint64_t)sizeof(QiGroup));
        if (!nptr) return QISA_ERR_OOM;
        for (i = 0; i < *n; i++) {
            nptr[i].key = (*arr)[i].key;
            nptr[i].klen = (*arr)[i].klen;
            nptr[i].key_vals = (*arr)[i].key_vals;
            nptr[i].accs = (*arr)[i].accs;
            nptr[i].next = (*arr)[i].next;
        }
        *arr = nptr;
        *cap = ncap;
    }
    g = &(*arr)[*n];
    g->key = key;
    g->klen = klen;
    g->key_vals = key_vals;
    g->accs = accs;
    slot = (uint32_t)(qi_fnv(key, klen) & (QB_HASH_SLOTS - 1u));
    g->next = head[slot];
    head[slot] = (int32_t)(*n);
    (*n)++;
    return QISA_OK;
}

/* Merge the epoch set into the merged set (first-appearance
 * order: new keys append in epoch order; existing keys fold
 * via qi_acc_merge), then reset the epoch. Mirrors Go's
 * spill flush + spilled merge, except the merged set lives
 * in the state buffer (logical spill) instead of files. */
static int qb_agg_merge_epoch(QBState* st) {
    uint32_t g, a;
    int rc;
    for (g = 0; g < st->nepoch; g++) {
        QiGroup* eg = &st->epoch[g];
        int32_t mi = qb_group_find(st->merged, st->nmerged,
                                   st->merged_head, eg->key,
                                   eg->klen);
        if (mi < 0) {
            rc = qb_group_append(st, &st->merged, &st->nmerged,
                                 &st->cap_merged, st->merged_head,
                                 eg->key, eg->klen, eg->key_vals,
                                 eg->accs);
            if (rc != QISA_OK) return rc;
        } else {
            QiGroup* mg = &st->merged[(uint32_t)mi];
            for (a = 0; a < st->nagg; a++) {
                rc = qi_acc_merge(&mg->accs[a], &eg->accs[a],
                                  &st->bump);
                if (rc != QISA_OK) return rc;
            }
        }
    }
    st->nepoch = 0;
    qb_heads_reset(st->epoch_head);
    return QISA_OK;
}

static int qb_agg_init_common(uint8_t* state, uint64_t state_cap,
                              uint32_t width, uint32_t ngroup,
                              const uint32_t* group_cols,
                              uint32_t nagg,
                              const uint32_t* agg_specs,
                              uint64_t budget, uint32_t max_groups) {
    QBState* st;
    uint32_t i;
    int rc;
    if (width == 0u || width > QBATCH_MAX_WIDTH ||
        ngroup > width || ngroup > QBATCH_MAX_WIDTH ||
        nagg > QBATCH_MAX_WIDTH)
        return QISA_ERR_LIMIT;
    if (ngroup > 0u && !group_cols) return QISA_ERR_MALFORMED;
    if (nagg > 0u && !agg_specs) return QISA_ERR_MALFORMED;
    for (i = 0; i < ngroup; i++)
        if (group_cols[i] >= width) return QISA_ERR_MALFORMED;
    for (i = 0; i < nagg; i++) {
        uint32_t code = qi_rd32((const uint8_t*)agg_specs +
                                (uint64_t)i * 12u);
        uint32_t star = qi_rd32((const uint8_t*)agg_specs +
                                (uint64_t)i * 12u + 4u);
        uint32_t col = qi_rd32((const uint8_t*)agg_specs +
                               (uint64_t)i * 12u + 8u);
        if (code > (uint32_t)QISA_AGG_MAX || star > 1u)
            return QISA_ERR_MALFORMED;
        if (!star && col >= width) return QISA_ERR_MALFORMED;
    }
    rc = qb_state_init(state, state_cap, QBATCH_KIND_AGG, width,
                       budget, 0, max_groups, &st);
    if (rc != QISA_OK) return rc;
    st->ngroup = ngroup;
    st->nagg = nagg;
    for (i = 0; i < ngroup; i++) st->group_cols[i] = group_cols[i];
    for (i = 0; i < nagg; i++) {
        st->agg_code[i] = (uint8_t)qi_rd32(
            (const uint8_t*)agg_specs + (uint64_t)i * 12u);
        st->agg_star[i] = (uint8_t)qi_rd32(
            (const uint8_t*)agg_specs + (uint64_t)i * 12u + 4u);
        st->agg_col[i] = qi_rd32((const uint8_t*)agg_specs +
                                 (uint64_t)i * 12u + 8u);
    }
    rc = qb_heads_init(st, &st->epoch_head);
    if (rc != QISA_OK) return rc;
    rc = qb_heads_init(st, &st->merged_head);
    if (rc != QISA_OK) return rc;
    return QISA_OK;
}

QISA_EXPORT("yowza_qbatch_agg_init")
int yowza_qbatch_agg_init(uint8_t* state, uint64_t state_cap,
                          uint32_t width, uint32_t ngroup,
                          const uint32_t* group_cols, uint32_t nagg,
                          const uint32_t* agg_specs, uint64_t budget) {
    return qb_agg_init_common(state, state_cap, width, ngroup,
                              group_cols, nagg, agg_specs, budget,
                              0);
}

QISA_EXPORT("yowza_qbatch_agg_init2")
int yowza_qbatch_agg_init2(uint8_t* state, uint64_t state_cap,
                           uint32_t width, uint32_t ngroup,
                           const uint32_t* group_cols, uint32_t nagg,
                           const uint32_t* agg_specs, uint64_t budget,
                           uint32_t max_groups) {
    if (max_groups == 0u) return QISA_ERR_LIMIT;
    return qb_agg_init_common(state, state_cap, width, ngroup,
                              group_cols, nagg, agg_specs, budget,
                              max_groups);
}

/* ---- aggregate capacity preflight ----
 * The aggregate append is the only add path whose bump
 * usage is not a pure function of the batch alone: new
 * groups depend on hash lookups, and MIN/MAX string copies
 * depend on accumulator state. The presim below therefore
 * tracks group creation exactly (epoch heads are read
 * only; batch-new groups go in a scratch registry) while
 * upper-bounding the data-dependent copies (every MIN/MAX
 * string value counts as two payload copies, the first-row
 * count, which is the maximum any row can cause). Merge
 * copies are upper-bounded the same way. A padding margin
 * of 8 bytes per possible allocation covers alignment
 * differences between the simulated and real allocation
 * sequences, so a passing presim cannot OOM mid-append
 * and a failing presim only over-estimates need. All
 * scratch (registry entries, key copies, shadow arrays,
 * descriptors) lives in the state bump and is discarded
 * by restoring bump.off on every exit, so the presim
 * itself consumes nothing: on success the real append
 * re-allocates its keys fresh, on failure the state is
 * byte-identical except last_need. */

/* Scratch registry entry for one batch-new group. key
 * points at the presim's bump copy of the group key. */
typedef struct {
    const uint8_t* key;
    uint32_t klen;
    uint64_t hash;
    /* Groups-bytes accounting upper bound for this group
     * (48 + key sizes + 24/acc + MIN/MAX extreme sizes). */
    uint64_t post_bytes;
    /* String-payload bytes a merge of this group could
     * copy at most (MIN/MAX extremes only). */
    uint64_t mm_alloc;
} QbSimReg;

typedef struct {
    QbSimReg* items;
    uint32_t n;
    uint32_t cap;
} QbSimRegSet;

static int qb_reg_append(QbSimRegSet* set, QiBump* bump,
                         const QbSimReg* e) {
    uint32_t i;
    if (set->n >= set->cap) {
        uint32_t ncap = set->cap == 0u ? 64u : set->cap * 2u;
        QbSimReg* np = (QbSimReg*)qi_bump(
            bump, (uint64_t)ncap * (uint64_t)sizeof(QbSimReg));
        if (!np) return QISA_ERR_OOM;
        for (i = 0; i < set->n; i++) np[i] = set->items[i];
        set->items = np;
        set->cap = ncap;
    }
    set->items[set->n++] = *e;
    return QISA_OK;
}

static int32_t qb_reg_find(const QbSimRegSet* set,
                           const uint8_t* key, uint32_t klen,
                           uint64_t hash) {
    uint32_t i;
    for (i = 0; i < set->n; i++) {
        if (set->items[i].hash != hash ||
            set->items[i].klen != klen)
            continue;
        if (qi_eq(set->items[i].key, key, klen)) return (int32_t)i;
    }
    return -1;
}

/* One group as the merge simulation sees it. mm_alloc is
 * the string-payload upper bound a merge of this group
 * could copy (existing groups) or 0 (groups new to the
 * merged set, whose accumulators are reused by pointer). */
typedef struct {
    const uint8_t* key;
    uint32_t klen;
    uint64_t hash;
    uint64_t mm_alloc;
} QbMergeDesc;

/* Simulate qb_agg_merge_epoch over descs in order,
 * advancing *sim_off (array growth for groups new to the
 * merged set; mm_alloc copies for existing groups) and
 * *alloc_bound (allocation-count bound for the padding
 * margin). Scratch (the merge-new registry) lives in the
 * state bump and is discarded by the caller's restore.
 * Returns OOM only when scratch itself does not fit. */
static int qb_agg_merge_sim(QBState* st, const QbMergeDesc* descs,
                            uint32_t ndesc, uint64_t* sim_off,
                            uint64_t* alloc_bound) {
    QbSimRegSet r2;
    uint32_t nmerged_sim = st->nmerged;
    uint32_t cap_merged_sim = st->cap_merged;
    uint32_t d;
    r2.items = 0;
    r2.n = 0;
    r2.cap = 0;
    for (d = 0; d < ndesc; d++) {
        const QbMergeDesc* desc = &descs[d];
        int32_t mi = qb_group_find(st->merged, st->nmerged,
                                   st->merged_head, desc->key,
                                   desc->klen);
        /* A group appended earlier in this same merge is
         * already in the merged set for later groups: a
         * duplicate epoch group folds into it (accumulator
         * copies), it does not append again. */
        if (mi < 0 &&
            qb_reg_find(&r2, desc->key, desc->klen,
                        desc->hash) >= 0)
            mi = 0;
        if (mi < 0) {
            QbSimReg e;
            if (nmerged_sim >= cap_merged_sim) {
                uint32_t ncap =
                    cap_merged_sim == 0u ? 64u : cap_merged_sim * 2u;
                qb_sim_bump(sim_off, (uint64_t)ncap *
                                         (uint64_t)sizeof(QiGroup));
                cap_merged_sim = ncap;
                (*alloc_bound)++;
            }
            nmerged_sim++;
            e.key = desc->key;
            e.klen = desc->klen;
            e.hash = desc->hash;
            e.post_bytes = 0;
            e.mm_alloc = 0;
            if (qb_reg_append(&r2, &st->bump, &e) != QISA_OK)
                return QISA_ERR_OOM;
        } else {
            if (desc->mm_alloc > 0u) {
                qb_sim_bump(sim_off, desc->mm_alloc);
                /* At most two payload copies per MIN/MAX
                 * accumulator (mn, mx); nagg bounds it. */
                *alloc_bound += 2u * st->nagg;
            }
        }
    }
    return QISA_OK;
}

/* Simulate one aggregate batch append (qb_agg_presim's row
 * loop helper state lives in the caller). MIN/MAX value
 * for accumulator a, honouring star (star accumulators
 * fold a NULL, which copies nothing). */
static const QiVal* qb_agg_sim_val(const QiVal* row,
                                   const QBState* st, uint32_t a,
                                   QiVal* nullv) {
    if (st->agg_star[a]) return nullv;
    return &row[st->agg_col[a]];
}

/* Aggregate append preflight: decodes the batch, measures
 * batch_bytes (Go accounting), and simulates the append
 * plus, when the post-append epoch bytes could exceed the
 * budget, the spill merge. Sets last_need on capacity
 * failure. Restores bump.off on every exit. */
static int qb_agg_presim(QBState* st, const uint8_t* in,
                         uint32_t in_len,
                         const uint32_t* name_lens,
                         uint64_t* batch_bytes_out) {
    uint64_t entry_off = st->bump.off;
    uint64_t sim_off = entry_off;
    uint64_t alloc_bound = 0;
    uint64_t batch_bytes = 0;
    uint64_t add_acct_pre = 0;
    QiRd rd;
    QiVal mrow[QBATCH_MAX_WIDTH];
    QiVal nullv;
    QbSimRegSet reg;
    uint64_t* shadow_acct = 0;
    uint64_t* shadow_alloc = 0;
    uint32_t nrows, j;
    uint32_t nepoch_sim = st->nepoch;
    uint32_t cap_epoch_sim = st->cap_epoch;
    uint32_t ndistinct_sim = st->ndistinct;
    int rc = QISA_OK;
    reg.items = 0;
    reg.n = 0;
    reg.cap = 0;
    qi_vnull(&nullv);
    rc = qb_batch_open(in, in_len, st->width, &rd, &nrows);
    if (rc != QISA_OK) goto out;
    if (nrows > 0u && !name_lens) {
        rc = QISA_ERR_MALFORMED;
        goto out;
    }
    if (st->nepoch > 0u) {
        uint32_t g;
        shadow_acct = (uint64_t*)qi_bump(
            &st->bump, (uint64_t)st->nepoch * 8u);
        shadow_alloc = (uint64_t*)qi_bump(
            &st->bump, (uint64_t)st->nepoch * 8u);
        if (!shadow_acct || !shadow_alloc) {
            st->last_need = qb_base_off() + sim_off;
            rc = QISA_ERR_OOM;
            goto out;
        }
        for (g = 0; g < st->nepoch; g++) {
            shadow_acct[g] = 0;
            shadow_alloc[g] = 0;
        }
    }
    for (j = 0; j < nrows; j++) {
        uint32_t klen, a, c;
        uint8_t* key;
        uint64_t hash;
        int32_t gi, ri;
        rc = qb_decode_row(&rd, mrow, st->width);
        if (rc != QISA_OK) goto out;
        batch_bytes += qb_batch_bytes(mrow, st->width, name_lens);
        klen = qb_group_key_size(mrow, st->group_cols, st->ngroup);
        key = (uint8_t*)qi_bump(&st->bump, klen == 0u ? 1u : klen);
        if (!key) {
            st->last_need = qb_base_off() + sim_off;
            rc = QISA_ERR_OOM;
            goto out;
        }
        qb_group_key_fill(mrow, st->group_cols, st->ngroup, key);
        qb_sim_bump(&sim_off, klen == 0u ? 1u : klen);
        alloc_bound++;
        hash = qi_fnv(key, klen);
        gi = qb_group_find(st->epoch, st->nepoch, st->epoch_head,
                           key, klen);
        if (gi >= 0) {
            for (a = 0; a < st->nagg; a++) {
                const QiVal* v;
                uint64_t acct_add, alloc_add;
                if (st->agg_code[a] != (uint8_t)QISA_AGG_MIN &&
                    st->agg_code[a] != (uint8_t)QISA_AGG_MAX)
                    continue;
                v = qb_agg_sim_val(mrow, st, a, &nullv);
                acct_add = qi_value_size(v);
                alloc_add = 0;
                if ((v->kind == (uint8_t)QISA_KIND_STRING ||
                     v->kind == (uint8_t)QISA_KIND_BYTES) &&
                    v->len > 0u)
                    alloc_add = v->len;
                shadow_acct[(uint32_t)gi] += acct_add;
                shadow_alloc[(uint32_t)gi] += alloc_add;
                add_acct_pre += acct_add;
                if (alloc_add > 0u) {
                    qb_sim_bump(&sim_off, alloc_add);
                    qb_sim_bump(&sim_off, alloc_add);
                    alloc_bound += 2u;
                }
            }
            continue;
        }
        ri = qb_reg_find(&reg, key, klen, hash);
        if (ri >= 0) {
            QbSimReg* e = &reg.items[(uint32_t)ri];
            for (a = 0; a < st->nagg; a++) {
                const QiVal* v;
                uint64_t acct_add, alloc_add;
                if (st->agg_code[a] != (uint8_t)QISA_AGG_MIN &&
                    st->agg_code[a] != (uint8_t)QISA_AGG_MAX)
                    continue;
                v = qb_agg_sim_val(mrow, st, a, &nullv);
                acct_add = qi_value_size(v);
                alloc_add = 0;
                if ((v->kind == (uint8_t)QISA_KIND_STRING ||
                     v->kind == (uint8_t)QISA_KIND_BYTES) &&
                    v->len > 0u)
                    alloc_add = v->len;
                e->post_bytes += acct_add;
                e->mm_alloc += alloc_add;
                if (alloc_add > 0u) {
                    qb_sim_bump(&sim_off, alloc_add);
                    qb_sim_bump(&sim_off, alloc_add);
                    alloc_bound += 2u;
                }
            }
            continue;
        }
        /* Brand-new group in this batch. */
        {
            int32_t mi = qb_group_find(st->merged, st->nmerged,
                                       st->merged_head, key, klen);
            QbSimReg e;
            uint64_t post = 48u;
            uint64_t mm = 0;
            if (mi < 0) {
                if (ndistinct_sim >= st->max_groups) {
                    rc = QISA_ERR_LIMIT;
                    goto out;
                }
                ndistinct_sim++;
            }
            qb_sim_bump(&sim_off, (uint64_t)(st->ngroup == 0u
                                                 ? 1u
                                                 : st->ngroup) *
                                     (uint64_t)sizeof(QiVal));
            alloc_bound++;
            qb_sim_bump(&sim_off, (uint64_t)(st->nagg == 0u
                                                 ? 1u
                                                 : st->nagg) *
                                     (uint64_t)sizeof(QiAcc));
            alloc_bound++;
            for (c = 0; c < st->ngroup; c++) {
                const QiVal* kv = &mrow[st->group_cols[c]];
                post += qi_value_size(kv);
                if ((kv->kind == (uint8_t)QISA_KIND_STRING ||
                     kv->kind == (uint8_t)QISA_KIND_BYTES) &&
                    kv->len > 0u) {
                    qb_sim_bump(&sim_off, kv->len);
                    alloc_bound++;
                }
            }
            post += 24u * st->nagg;
            if (nepoch_sim >= cap_epoch_sim) {
                uint32_t ncap =
                    cap_epoch_sim == 0u ? 64u : cap_epoch_sim * 2u;
                qb_sim_bump(&sim_off, (uint64_t)ncap *
                                         (uint64_t)sizeof(QiGroup));
                cap_epoch_sim = ncap;
                alloc_bound++;
            }
            nepoch_sim++;
            for (a = 0; a < st->nagg; a++) {
                const QiVal* v;
                if (st->agg_code[a] != (uint8_t)QISA_AGG_MIN &&
                    st->agg_code[a] != (uint8_t)QISA_AGG_MAX)
                    continue;
                v = qb_agg_sim_val(mrow, st, a, &nullv);
                post += 2u * qi_value_size(v);
                if ((v->kind == (uint8_t)QISA_KIND_STRING ||
                     v->kind == (uint8_t)QISA_KIND_BYTES) &&
                    v->len > 0u) {
                    mm += 2u * v->len;
                    qb_sim_bump(&sim_off, v->len);
                    qb_sim_bump(&sim_off, v->len);
                    alloc_bound += 2u;
                }
            }
            e.key = key;
            e.klen = klen;
            e.hash = hash;
            e.post_bytes = post;
            e.mm_alloc = mm;
            if (qb_reg_append(&reg, &st->bump, &e) != QISA_OK) {
                st->last_need = qb_base_off() + sim_off;
                rc = QISA_ERR_OOM;
                goto out;
            }
        }
    }
    rc = qb_batch_close(&rd);
    if (rc != QISA_OK) goto out;
    if (st->budget > 0u) {
        uint64_t post_upper =
            qb_groups_bytes(st->epoch, st->nepoch, st->ngroup,
                            st->nagg) +
            add_acct_pre;
        uint32_t i;
        for (i = 0; i < reg.n; i++)
            post_upper += reg.items[i].post_bytes;
        if (post_upper > st->budget) {
            /* Spill may trigger: simulate the merge over
             * the post-append epoch set (pre-batch groups
             * in order, then batch-new groups in creation
             * order). */
            QbMergeDesc* descs;
            uint32_t g, nd = st->nepoch + reg.n;
            descs = (QbMergeDesc*)qi_bump(
                &st->bump,
                (uint64_t)(nd == 0u ? 1u : nd) *
                    (uint64_t)sizeof(QbMergeDesc));
            if (!descs) {
                st->last_need = qb_base_off() + sim_off;
                rc = QISA_ERR_OOM;
                goto out;
            }
            for (g = 0; g < st->nepoch; g++) {
                QiGroup* gr = &st->epoch[g];
                uint64_t mm = shadow_alloc[g];
                uint32_t a;
                for (a = 0; a < st->nagg; a++) {
                    QiAcc* acc;
                    if (st->agg_code[a] != (uint8_t)QISA_AGG_MIN &&
                        st->agg_code[a] != (uint8_t)QISA_AGG_MAX)
                        continue;
                    acc = &gr->accs[a];
                    if (!acc->has) continue;
                    if ((acc->mn.kind == (uint8_t)QISA_KIND_STRING ||
                         acc->mn.kind == (uint8_t)QISA_KIND_BYTES) &&
                        acc->mn.len > 0u)
                        mm += acc->mn.len;
                    if ((acc->mx.kind == (uint8_t)QISA_KIND_STRING ||
                         acc->mx.kind == (uint8_t)QISA_KIND_BYTES) &&
                        acc->mx.len > 0u)
                        mm += acc->mx.len;
                }
                descs[g].key = gr->key;
                descs[g].klen = gr->klen;
                descs[g].hash = qi_fnv(gr->key, gr->klen);
                descs[g].mm_alloc = mm;
            }
            for (i = 0; i < reg.n; i++) {
                descs[st->nepoch + i].key = reg.items[i].key;
                descs[st->nepoch + i].klen = reg.items[i].klen;
                descs[st->nepoch + i].hash = reg.items[i].hash;
                descs[st->nepoch + i].mm_alloc =
                    reg.items[i].mm_alloc;
            }
            if (qb_agg_merge_sim(st, descs, nd, &sim_off,
                                 &alloc_bound) != QISA_OK) {
                st->last_need = qb_base_off() + sim_off;
                rc = QISA_ERR_OOM;
                goto out;
            }
        }
    }
    qb_sim_add(&sim_off, alloc_bound * 8u);
    if (sim_off > st->bump.cap) {
        st->last_need = qb_base_off() + sim_off;
        rc = QISA_ERR_OOM;
        goto out;
    }
out:
    st->bump.off = entry_off;
    *batch_bytes_out = batch_bytes;
    return rc;
}

/* Finish-time merge preflight: same merge simulation over
 * the current epoch set (no append part). */
static int qb_agg_finish_presim(QBState* st) {
    uint64_t entry_off = st->bump.off;
    uint64_t sim_off = entry_off;
    uint64_t alloc_bound = 0;
    QbMergeDesc* descs;
    uint32_t g;
    int rc = QISA_OK;
    if (st->nepoch == 0u) return QISA_OK;
    descs = (QbMergeDesc*)qi_bump(
        &st->bump, (uint64_t)st->nepoch * (uint64_t)sizeof(QbMergeDesc));
    if (!descs) {
        st->last_need = qb_base_off() + sim_off;
        st->bump.off = entry_off;
        return QISA_ERR_OOM;
    }
    for (g = 0; g < st->nepoch; g++) {
        QiGroup* gr = &st->epoch[g];
        uint64_t mm = 0;
        uint32_t a;
        for (a = 0; a < st->nagg; a++) {
            QiAcc* acc;
            if (st->agg_code[a] != (uint8_t)QISA_AGG_MIN &&
                st->agg_code[a] != (uint8_t)QISA_AGG_MAX)
                continue;
            acc = &gr->accs[a];
            if (!acc->has) continue;
            if ((acc->mn.kind == (uint8_t)QISA_KIND_STRING ||
                 acc->mn.kind == (uint8_t)QISA_KIND_BYTES) &&
                acc->mn.len > 0u)
                mm += acc->mn.len;
            if ((acc->mx.kind == (uint8_t)QISA_KIND_STRING ||
                 acc->mx.kind == (uint8_t)QISA_KIND_BYTES) &&
                acc->mx.len > 0u)
                mm += acc->mx.len;
        }
        descs[g].key = gr->key;
        descs[g].klen = gr->klen;
        descs[g].hash = qi_fnv(gr->key, gr->klen);
        descs[g].mm_alloc = mm;
    }
    if (qb_agg_merge_sim(st, descs, st->nepoch, &sim_off,
                         &alloc_bound) != QISA_OK) {
        st->last_need = qb_base_off() + sim_off;
        st->bump.off = entry_off;
        return QISA_ERR_OOM;
    }
    qb_sim_add(&sim_off, alloc_bound * 8u);
    if (sim_off > st->bump.cap) {
        st->last_need = qb_base_off() + sim_off;
        rc = QISA_ERR_OOM;
    }
    st->bump.off = entry_off;
    return rc;
}

QISA_EXPORT("yowza_qbatch_agg_add")
int yowza_qbatch_agg_add(uint8_t* state, uint64_t state_cap,
                         const uint8_t* in, uint32_t in_len,
                         const uint32_t* name_lens) {
    QBState* st;
    QiRd rd;
    QiVal row[QBATCH_MAX_WIDTH];
    QiVal nullv;
    uint32_t nrows, i;
    uint64_t batch_bytes = 0;
    int rc;
    rc = qb_state_get(state, state_cap, QBATCH_KIND_AGG, 0, &st);
    if (rc != QISA_OK) return rc;
    rc = qb_batch_open(in, in_len, st->width, &rd, &nrows);
    if (rc != QISA_OK) return rc;
    if (nrows > 0u && !name_lens) return QISA_ERR_MALFORMED;
    st->last_need = 0;
    /* Pass 1 is now the capacity preflight: it measures
     * the batch (Go checks batchBytes before folding a
     * batch) and simulates the append plus any spill
     * merge. A capacity failure here folds nothing, so
     * the state stays usable for a grow-and-retry; the
     * old code could OOM mid-fold, leaving partial groups
     * and mutated accumulators. (Type errors in pass 2
     * keep the old loud-abort contract: they are not
     * capacity failures.) */
    rc = qb_agg_presim(st, in, in_len, name_lens, &batch_bytes);
    if (rc != QISA_OK) return rc;
    if (st->budget > 0u && batch_bytes > st->budget)
        return QISA_ERR_BUDGET;
    qi_vnull(&nullv);
    for (i = 0; i < nrows; i++) {
        uint32_t klen, a;
        uint8_t* key;
        int32_t gi;
        QiGroup* g;
        rc = qb_decode_row(&rd, row, st->width);
        if (rc != QISA_OK) return rc;
        klen = qb_group_key_size(row, st->group_cols, st->ngroup);
        key = (uint8_t*)qi_bump(&st->bump, klen == 0u ? 1u : klen);
        if (!key) return QISA_ERR_OOM;
        qb_group_key_fill(row, st->group_cols, st->ngroup, key);
        gi = qb_group_find(st->epoch, st->nepoch, st->epoch_head,
                           key, klen);
        if (gi < 0) {
            QiVal* key_vals;
            QiAcc* accs;
            int32_t mi;
            if (st->ndistinct >= st->max_groups) {
                /* New distinct group over the cap. A key
                 * already in the merged set is not new. */
                mi = qb_group_find(st->merged, st->nmerged,
                                   st->merged_head, key, klen);
                if (mi < 0) return QISA_ERR_LIMIT;
            }
            key_vals = (QiVal*)qi_bump(
                &st->bump, (uint64_t)(st->ngroup == 0u ? 1u
                                                      : st->ngroup) *
                               (uint64_t)sizeof(QiVal));
            accs = (QiAcc*)qi_bump(
                &st->bump, (uint64_t)(st->nagg == 0u ? 1u
                                                    : st->nagg) *
                               (uint64_t)sizeof(QiAcc));
            if (!key_vals || !accs) return QISA_ERR_OOM;
            for (a = 0; a < st->ngroup; a++) {
                rc = qi_vcopy_persist(&key_vals[a],
                                      &row[st->group_cols[a]],
                                      &st->bump);
                if (rc != QISA_OK) return rc;
            }
            for (a = 0; a < st->nagg; a++) {
                accs[a].agg = st->agg_code[a];
                accs[a].count = 0;
                accs[a].sum_f = 0.0;
                accs[a].sum_i = 0;
                accs[a].has = 0;
                accs[a].is_float = 0;
                qi_vnull(&accs[a].mn);
                qi_vnull(&accs[a].mx);
            }
            rc = qb_group_append(st, &st->epoch, &st->nepoch,
                                 &st->cap_epoch, st->epoch_head,
                                 key, klen, key_vals, accs);
            if (rc != QISA_OK) return rc;
            /* Distinct only when the key is new globally. */
            mi = qb_group_find(st->merged, st->nmerged,
                               st->merged_head, key, klen);
            if (mi < 0) st->ndistinct++;
            g = &st->epoch[st->nepoch - 1u];
        } else {
            g = &st->epoch[(uint32_t)gi];
        }
        for (a = 0; a < st->nagg; a++) {
            QiAcc* acc = &g->accs[a];
            if (st->agg_star[a]) {
                rc = qi_acc_add(acc, &nullv, 1, &st->bump);
            } else {
                rc = qi_acc_add(acc, &row[st->agg_col[a]], 0,
                                &st->bump);
            }
            if (rc != QISA_OK) return rc;
        }
    }
    rc = qb_batch_close(&rd);
    if (rc != QISA_OK) return rc;
    /* Peak is noted BEFORE the spill check, exactly as Go
     * notes groupsBytes before flushing the epoch. */
    {
        uint64_t gb = qb_groups_bytes(st->epoch, st->nepoch,
                                      st->ngroup, st->nagg);
        qb_note_peak(st, gb);
        if (st->budget > 0u && gb > st->budget) {
            rc = qb_agg_merge_epoch(st);
            if (rc != QISA_OK) return rc;
            st->spilled = 1;
        }
    }
    return QISA_OK;
}

QISA_EXPORT("yowza_qbatch_agg_finish")
int yowza_qbatch_agg_finish(uint8_t* state, uint64_t state_cap,
                            uint8_t* out, uint32_t out_cap,
                            uint32_t* out_len) {
    QBState* st;
    uint32_t g, a, off = 4, emitted = 0;
    int rc;
    if (!out || !out_len) return QISA_ERR_MALFORMED;
    rc = qb_state_get(state, state_cap, QBATCH_KIND_AGG, 0, &st);
    if (rc != QISA_OK) return rc;
    st->last_need = 0;
    if (st->nepoch > 0u) {
        /* Merge preflight first: the old code could OOM
         * mid-merge, leaving a partially merged set. A
         * capacity failure here merges nothing, so the
         * host can grow, relocate, and retry finish. */
        rc = qb_agg_finish_presim(st);
        if (rc != QISA_OK) return rc;
        rc = qb_agg_merge_epoch(st);
        if (rc != QISA_OK) return rc;
    }
    if (out_cap < 4u) return QISA_ERR_OUT_SMALL;
    if (st->ngroup == 0u && st->nmerged == 0u) {
        /* Ungrouped over empty input: one row, COUNT = 0,
         * other aggregates NULL. */
        for (a = 0; a < st->nagg; a++) {
            QiAcc acc;
            QiVal v;
            acc.agg = st->agg_code[a];
            acc.count = 0;
            acc.sum_f = 0.0;
            acc.sum_i = 0;
            acc.has = 0;
            acc.is_float = 0;
            qi_vnull(&acc.mn);
            qi_vnull(&acc.mx);
            qi_acc_result(&acc, &v);
            rc = qi_emit_val(out, out_cap, &off, &v);
            if (rc != QISA_OK) return rc;
        }
        emitted = 1;
    } else {
        for (g = 0; g < st->nmerged; g++) {
            QiGroup* gr = &st->merged[g];
            for (a = 0; a < st->ngroup; a++) {
                rc = qi_emit_val(out, out_cap, &off,
                                 &gr->key_vals[a]);
                if (rc != QISA_OK) return rc;
            }
            for (a = 0; a < st->nagg; a++) {
                QiVal v;
                qi_acc_result(&gr->accs[a], &v);
                rc = qi_emit_val(out, out_cap, &off, &v);
                if (rc != QISA_OK) return rc;
            }
            emitted++;
        }
    }
    qi_wr32(out, emitted);
    *out_len = off;
    st->nrows = emitted;
    return QISA_OK;
}
