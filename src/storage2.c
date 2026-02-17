/* Yowza64 — Storage ABI v2 implementation (Phase 2, Track A).
 * See include/storage2.h for the contract and durability notes.
 * Shared helpers use only byte loops (no libc string functions), so
 * the memory backend compiles under the WASM -nostdlib build too;
 * its malloc/free/realloc references are tolerated there by
 * --allow-undefined, and the memory factory is never called from
 * WASM code. All POSIX code is #ifndef __wasm__ guarded. */

#include "storage2.h"

#ifdef __wasm__
/* Declared, not defined, under WASM: the linker tolerates these as
 * undefined symbols (see Makefile --allow-undefined). Native builds
 * get the real declarations from <stdlib.h>. */
extern void* malloc(size_t n);
extern void free(void* p);
extern void* realloc(void* p, size_t n);
#else
#include <stdlib.h>
#endif

/* ---------------------------------------------------------------- */
/* Shared small helpers (no libc string dependency)                  */
/* ---------------------------------------------------------------- */

static size_t s2_strlen(const char* s) {
    size_t n = 0;
    while (s[n] != '\0') n++;
    return n;
}

static int s2_name_eq(const char* a, const char* b) {
    size_t i = 0;
    for (;;) {
        if (a[i] != b[i]) return 0;
        if (a[i] == '\0') return 1;
        i++;
    }
}

/* Flat namespace rule (see header): non-empty, not "." or "..",
 * no '/'. */
static int s2_valid_name(const char* obj) {
    size_t i;
    if (!obj || obj[0] == '\0') return 0;
    if (obj[0] == '.' && obj[1] == '\0') return 0;
    if (obj[0] == '.' && obj[1] == '.' && obj[2] == '\0') return 0;
    for (i = 0; obj[i] != '\0'; i++) {
        if (obj[i] == '/') return 0;
    }
    return 1;
}

static char* s2_strdup(const char* s) {
    size_t n = s2_strlen(s);
    size_t i;
    char* d = (char*)malloc(n + 1);
    if (!d) return NULL;
    for (i = 0; i <= n; i++) d[i] = s[i];
    return d;
}

static uint64_t s2_min64(uint64_t a, uint64_t b) {
    return a < b ? a : b;
}

/* ---------------------------------------------------------------- */
/* Memory backend: sparse 1 MiB chunks, allocated on demand          */
/* ---------------------------------------------------------------- */

#define MEM_CHUNK_SHIFT 20u
#define MEM_CHUNK_SIZE ((uint64_t)1048576) /* 1 MiB */
#define MEM_CHUNK_MASK (MEM_CHUNK_SIZE - 1)

typedef struct {
    char* name;
    uint8_t** chunks;   /* NULL slot = all-zero chunk, never allocated */
    uint64_t nchunks;   /* length of the chunks array */
    uint64_t size;      /* logical object size in bytes */
} MemObj;

typedef struct {
    MemObj* objs;
    uint64_t nobjs;
    uint64_t cap;
} MemState;

static MemObj* mem_find(MemState* st, const char* obj) {
    uint64_t i;
    for (i = 0; i < st->nobjs; i++) {
        if (s2_name_eq(st->objs[i].name, obj)) return &st->objs[i];
    }
    return NULL;
}

/* Free one object's chunks and name (not the array slot). */
static void mem_free_obj_contents(MemObj* o) {
    uint64_t i;
    if (o->chunks) {
        for (i = 0; i < o->nchunks; i++) free(o->chunks[i]);
        free(o->chunks);
        o->chunks = NULL;
    }
    free(o->name);
    o->name = NULL;
    o->nchunks = 0;
    o->size = 0;
}

/* Remove the array entry at index idx (contents already freed). */
static void mem_drop_slot(MemState* st, uint64_t idx) {
    uint64_t i;
    for (i = idx + 1; i < st->nobjs; i++) st->objs[i - 1] = st->objs[i];
    st->nobjs--;
}

static MemObj* mem_find_or_create(MemState* st, const char* obj) {
    MemObj* o = mem_find(st, obj);
    char* name;
    uint64_t new_cap;
    MemObj* grown;
    if (o) return o;
    if (st->nobjs == st->cap) {
        new_cap = st->cap ? st->cap * 2 : 4;
        if (st->cap && new_cap < st->cap) return NULL; /* overflow */
        if (new_cap > (uint64_t)-1 / (uint64_t)sizeof(MemObj)) return NULL;
        grown = (MemObj*)realloc(st->objs, (size_t)(new_cap * (uint64_t)sizeof(MemObj)));
        if (!grown) return NULL;
        st->objs = grown;
        st->cap = new_cap;
    }
    name = s2_strdup(obj);
    if (!name) return NULL;
    o = &st->objs[st->nobjs++];
    o->name = name;
    o->chunks = NULL;
    o->nchunks = 0;
    o->size = 0;
    return o;
}

/* Grow the chunk-pointer array so slot idx exists; new slots NULL. */
static int mem_ensure_slot(MemObj* o, uint64_t idx) {
    uint64_t new_n, i;
    uint8_t** grown;
    if (idx < o->nchunks) return 0;
    new_n = o->nchunks ? o->nchunks : 1;
    while (new_n <= idx) {
        if (new_n > (uint64_t)-1 / 2) {
            new_n = idx + 1;
            break;
        }
        new_n *= 2;
    }
    if (new_n > (uint64_t)-1 / (uint64_t)sizeof(uint8_t*)) return -1;
    grown = (uint8_t**)realloc(o->chunks, (size_t)(new_n * (uint64_t)sizeof(uint8_t*)));
    if (!grown) return -1;
    for (i = o->nchunks; i < new_n; i++) grown[i] = NULL;
    o->chunks = grown;
    o->nchunks = new_n;
    return 0;
}

/* Allocate and zero one chunk. */
static uint8_t* mem_alloc_chunk(void) {
    uint8_t* c = (uint8_t*)malloc((size_t)MEM_CHUNK_SIZE);
    uint64_t i;
    if (!c) return NULL;
    for (i = 0; i < MEM_CHUNK_SIZE; i++) c[i] = 0;
    return c;
}

static int mem_write_at(void* st, const char* obj, uint64_t off, const uint8_t* data, uint64_t len) {
    MemState* s = (MemState*)st;
    MemObj* o;
    uint64_t done = 0;
    if (!s || !s2_valid_name(obj)) return -1;
    if (len > 0 && !data) return -1;
    if (len > (uint64_t)-1 - off) return -1; /* off+len overflow */
    o = mem_find_or_create(s, obj);
    if (!o) return -1;
    if (len == 0) return 0; /* creates the object, changes nothing */
    while (done < len) {
        uint64_t cur = off + done;
        uint64_t idx = cur >> MEM_CHUNK_SHIFT;
        uint64_t coff = cur & MEM_CHUNK_MASK;
        uint64_t take = s2_min64(MEM_CHUNK_SIZE - coff, len - done);
        uint8_t* chunk;
        uint64_t i;
        if (mem_ensure_slot(o, idx) != 0) return -1;
        chunk = o->chunks[idx];
        if (!chunk) {
            chunk = mem_alloc_chunk();
            if (!chunk) return -1;
            o->chunks[idx] = chunk;
        }
        for (i = 0; i < take; i++) chunk[coff + i] = data[done + i];
        done += take;
    }
    if (off + len > o->size) o->size = off + len;
    return 0;
}

static int mem_read_at(void* st, const char* obj, uint64_t off, uint8_t* out, uint64_t len) {
    MemState* s = (MemState*)st;
    MemObj* o;
    uint64_t done = 0;
    if (!s || !s2_valid_name(obj)) return -1;
    if (len > 0 && !out) return -1;
    if (len > (uint64_t)-1 - off) return -1;
    o = mem_find(s, obj);
    if (!o) return -1;
    if (len == 0) return 0;
    if (off > o->size || len > o->size - off) return -1; /* short read */
    while (done < len) {
        uint64_t cur = off + done;
        uint64_t idx = cur >> MEM_CHUNK_SHIFT;
        uint64_t coff = cur & MEM_CHUNK_MASK;
        uint64_t take = s2_min64(MEM_CHUNK_SIZE - coff, len - done);
        uint8_t* chunk = idx < o->nchunks ? o->chunks[idx] : NULL;
        uint64_t i;
        if (!chunk) {
            for (i = 0; i < take; i++) out[done + i] = 0;
        } else {
            for (i = 0; i < take; i++) out[done + i] = chunk[coff + i];
        }
        done += take;
    }
    return 0;
}

static int mem_size(void* st, const char* obj, uint64_t* out_size) {
    MemState* s = (MemState*)st;
    MemObj* o;
    if (!s || !s2_valid_name(obj) || !out_size) return -1;
    o = mem_find(s, obj);
    if (!o) return -1;
    *out_size = o->size;
    return 0;
}

static int mem_size_status(void* st, const char* obj, uint64_t* out_size) {
    MemState* s = (MemState*)st;
    MemObj* o;
    if (!s || !s2_valid_name(obj) || !out_size) return YOWZA_STORE_IO_ERROR;
    o = mem_find(s, obj);
    if (!o) return YOWZA_STORE_NOT_FOUND;
    *out_size = o->size;
    return YOWZA_STORE_OK;
}

static int mem_enumerate(void* st, int (*cb)(void* ctx, const char* name), void* ctx) {
    MemState* s = (MemState*)st;
    uint64_t i;
    if (!s || !cb) return -1;
    for (i = 0; i < s->nobjs; i++) {
        if (cb(ctx, s->objs[i].name) != 0) return 1;
    }
    return 0;
}

static int mem_sync_obj(void* st, const char* obj) {
    MemState* s = (MemState*)st;
    /* No-op, but still validates the object exists. */
    if (!s || !s2_valid_name(obj)) return -1;
    return mem_find(s, obj) ? 0 : -1;
}

static int mem_sync_dir(void* st) {
    if (!st) return -1;
    return 0; /* no-op by contract */
}

static int mem_publish(void* st, const char* tmp_obj, const char* final_obj) {
    MemState* s = (MemState*)st;
    MemObj* tmp;
    uint64_t i, tmp_idx = 0;
    char* new_name;
    if (!s || !s2_valid_name(tmp_obj) || !s2_valid_name(final_obj)) return -1;
    tmp = mem_find(s, tmp_obj);
    if (!tmp) return -1; /* final untouched */
    /* Duplicate the new name BEFORE dropping the old final, so an
     * allocation failure leaves both objects intact. */
    new_name = s2_strdup(final_obj);
    if (!new_name) return -1;
    for (i = 0; i < s->nobjs; i++) {
        if (s->objs[i].name == tmp->name) tmp_idx = i;
    }
    if (!s2_name_eq(tmp_obj, final_obj)) {
        MemObj* fin = mem_find(s, final_obj);
        if (fin) {
            uint64_t fin_idx = 0;
            for (i = 0; i < s->nobjs; i++) {
                if (s->objs[i].name == fin->name) fin_idx = i;
            }
            mem_free_obj_contents(fin);
            mem_drop_slot(s, fin_idx);
            /* Re-locate tmp: the drop may have shifted it. */
            for (i = 0; i < s->nobjs; i++) {
                if (s2_name_eq(s->objs[i].name, tmp_obj)) tmp_idx = i;
            }
        }
    }
    free(s->objs[tmp_idx].name);
    s->objs[tmp_idx].name = new_name;
    return 0;
}

static int mem_remove(void* st, const char* obj) {
    MemState* s = (MemState*)st;
    MemObj* o;
    uint64_t i, idx = 0;
    if (!s || !s2_valid_name(obj)) return -1;
    o = mem_find(s, obj);
    if (!o) return -1;
    for (i = 0; i < s->nobjs; i++) {
        if (s->objs[i].name == o->name) idx = i;
    }
    mem_free_obj_contents(o);
    mem_drop_slot(s, idx);
    return 0;
}

static int mem_truncate(void* st, const char* obj, uint64_t len) {
    MemState* s = (MemState*)st;
    MemObj* o;
    uint64_t i;
    if (!s || !s2_valid_name(obj)) return -1;
    o = mem_find(s, obj);
    if (!o) return -1;
    if (len < o->size) {
        /* Free chunks entirely past the new end, and zero the tail
         * of the partial last chunk, so a later re-extension reads
         * zeroes (never stale bytes). */
        uint64_t keep = len == 0 ? 0 : (len - 1) / MEM_CHUNK_SIZE + 1;
        for (i = keep; i < o->nchunks; i++) {
            free(o->chunks[i]);
            o->chunks[i] = NULL;
        }
        if (len > 0 && (len & MEM_CHUNK_MASK) != 0) {
            uint8_t* last = o->chunks[keep - 1];
            uint64_t j;
            if (last) {
                for (j = len & MEM_CHUNK_MASK; j < MEM_CHUNK_SIZE; j++) last[j] = 0;
            }
        }
        o->size = len;
    } else if (len > o->size) {
        /* Growing needs no allocation: untouched chunks are NULL
         * and read as zeroes, and bytes past the old size inside an
         * existing chunk are zero by the shrink invariant above
         * (chunks start zeroed and shrink zeroes the tail). */
        o->size = len;
    }
    return 0;
}

static void mem_close(void* st) {
    MemState* s = (MemState*)st;
    uint64_t i;
    if (!s) return;
    for (i = 0; i < s->nobjs; i++) mem_free_obj_contents(&s->objs[i]);
    free(s->objs);
    free(s);
}

static const YowzaStoreVTable mem_vt = {
    mem_write_at, mem_read_at, mem_size, mem_sync_obj, mem_sync_dir,
    mem_publish, mem_remove, mem_truncate, mem_close, mem_size_status,
    mem_enumerate
};

YowzaStore* yowza_store_mem_create(void) {
    MemState* s = (MemState*)malloc(sizeof(MemState));
    YowzaStore* store;
    if (!s) return NULL;
    s->objs = NULL;
    s->nobjs = 0;
    s->cap = 0;
    store = (YowzaStore*)malloc(sizeof(YowzaStore));
    if (!store) {
        free(s);
        return NULL;
    }
    store->vt = &mem_vt;
    store->state = s;
    return store;
}

/* ---------------------------------------------------------------- */
/* POSIX backend (native only): one file per object in a directory   */
/* ---------------------------------------------------------------- */

#ifndef __wasm__

#include <stdio.h> /* rename */
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>

typedef struct {
    char* dir;
} PosixState;

/* dir + '/' + obj (no doubled slash if dir already ends in one). */
static char* posix_path(PosixState* s, const char* obj) {
    size_t dn = s2_strlen(s->dir);
    size_t on = s2_strlen(obj);
    int slash = dn > 0 && s->dir[dn - 1] != '/';
    size_t total = dn + (size_t)slash + on + 1;
    size_t i = 0, j;
    char* p = (char*)malloc(total);
    if (!p) return NULL;
    for (j = 0; j < dn; j++) p[i++] = s->dir[j];
    if (slash) p[i++] = '/';
    for (j = 0; j <= on; j++) p[i++] = obj[j];
    return p;
}

/* off_t is 64-bit on this platform, but guard the cast explicitly so
 * a >INT64_MAX offset fails cleanly instead of wrapping. */
static int posix_off_ok(uint64_t off, uint64_t len) {
    if (off > (uint64_t)9223372036854775807ULL) return 0;
    if (len > (uint64_t)9223372036854775807ULL - off) return 0;
    return 1;
}

static int posix_write_at(void* st, const char* obj, uint64_t off, const uint8_t* data, uint64_t len) {
    PosixState* s = (PosixState*)st;
    char* path;
    int fd, rc = 0;
    uint64_t done = 0;
    if (!s || !s2_valid_name(obj)) return -1;
    if (len > 0 && !data) return -1;
    if (!posix_off_ok(off, len)) return -1;
    path = posix_path(s, obj);
    if (!path) return -1;
    fd = open(path, O_WRONLY | O_CREAT, 0644);
    free(path);
    if (fd < 0) return -1;
    if (len == 0) {
        /* Creates the object, changes nothing. */
        if (close(fd) != 0) return -1;
        return 0;
    }
    while (done < len) {
        ssize_t w = pwrite(fd, data + (size_t)done, (size_t)(len - done), (off_t)(off + done));
        if (w < 0) {
            if (errno == EINTR) continue;
            rc = -1;
            break;
        }
        if (w == 0) {
            rc = -1;
            break;
        }
        done += (uint64_t)w;
    }
    if (close(fd) != 0) rc = -1;
    return rc;
}

static int posix_read_at(void* st, const char* obj, uint64_t off, uint8_t* out, uint64_t len) {
    PosixState* s = (PosixState*)st;
    char* path;
    int fd, rc = 0;
    uint64_t done = 0;
    if (!s || !s2_valid_name(obj)) return -1;
    if (len > 0 && !out) return -1;
    if (!posix_off_ok(off, len)) return -1;
    path = posix_path(s, obj);
    if (!path) return -1;
    fd = open(path, O_RDONLY);
    free(path);
    if (fd < 0) return -1;
    while (done < len) {
        ssize_t r = pread(fd, out + (size_t)done, (size_t)(len - done), (off_t)(off + done));
        if (r < 0) {
            if (errno == EINTR) continue;
            rc = -1;
            break;
        }
        if (r == 0) {
            rc = -1; /* short read: EOF before len bytes */
            break;
        }
        done += (uint64_t)r;
    }
    if (close(fd) != 0) rc = -1;
    return rc;
}

static int posix_size_status(void* st, const char* obj, uint64_t* out_size) {
    PosixState* s = (PosixState*)st;
    char* path;
    struct stat sb;
    if (!s || !s2_valid_name(obj) || !out_size) return YOWZA_STORE_IO_ERROR;
    path = posix_path(s, obj);
    if (!path) return YOWZA_STORE_IO_ERROR;
    if (stat(path, &sb) != 0) {
        int e = errno;
        free(path);
        return e == ENOENT ? YOWZA_STORE_NOT_FOUND : YOWZA_STORE_IO_ERROR;
    }
    free(path);
    if (sb.st_size < 0) return YOWZA_STORE_IO_ERROR;
    *out_size = (uint64_t)sb.st_size;
    return YOWZA_STORE_OK;
}

static int posix_size(void* st, const char* obj, uint64_t* out_size) {
    PosixState* s = (PosixState*)st;
    char* path;
    struct stat sb;
    if (!s || !s2_valid_name(obj) || !out_size) return -1;
    path = posix_path(s, obj);
    if (!path) return -1;
    if (stat(path, &sb) != 0) {
        free(path);
        return -1;
    }
    free(path);
    if (sb.st_size < 0) return -1;
    *out_size = (uint64_t)sb.st_size;
    return 0;
}

static int posix_sync_obj(void* st, const char* obj) {
    PosixState* s = (PosixState*)st;
    char* path;
    int fd, rc;
    if (!s || !s2_valid_name(obj)) return -1;
    path = posix_path(s, obj);
    if (!path) return -1;
    fd = open(path, O_RDWR);
    free(path);
    if (fd < 0) return -1;
    rc = fsync(fd) == 0 ? 0 : -1;
    if (close(fd) != 0) rc = -1;
    return rc;
}

static int posix_sync_dir(void* st) {
    PosixState* s = (PosixState*)st;
    int fd, rc;
    if (!s) return -1;
    fd = open(s->dir, O_RDONLY);
    if (fd < 0) return -1;
    rc = fsync(fd) == 0 ? 0 : -1;
    if (close(fd) != 0) rc = -1;
    return rc;
}

static int posix_publish(void* st, const char* tmp_obj, const char* final_obj) {
    PosixState* s = (PosixState*)st;
    char* tmp_path;
    char* fin_path;
    int fd;
    if (!s || !s2_valid_name(tmp_obj) || !s2_valid_name(final_obj)) return -1;
    tmp_path = posix_path(s, tmp_obj);
    fin_path = tmp_path ? posix_path(s, final_obj) : NULL;
    if (!tmp_path || !fin_path) {
        free(tmp_path);
        free(fin_path);
        return -1;
    }
    /* Temp file + fsync + rename + directory fsync (header notes:
     * if the directory fsync fails, -1 is returned even though the
     * rename already happened). */
    fd = open(tmp_path, O_RDWR);
    if (fd < 0) {
        free(tmp_path);
        free(fin_path);
        return -1; /* tmp missing: final untouched */
    }
    if (fsync(fd) != 0) {
        close(fd);
        free(tmp_path);
        free(fin_path);
        return -1;
    }
    if (close(fd) != 0) {
        free(tmp_path);
        free(fin_path);
        return -1;
    }
    if (rename(tmp_path, fin_path) != 0) {
        free(tmp_path);
        free(fin_path);
        return -1; /* definite failure: rename did not happen */
    }
    free(tmp_path);
    free(fin_path);
    /* Rename happened, durability sync failed: INDETERMINATE (-2),
     * not an ordinary failure — the new content is visible but its
     * rename is not yet proven durable. Callers must re-check. */
    return posix_sync_dir(st) == 0 ? 0 : -2;
}

static int posix_remove(void* st, const char* obj) {
    PosixState* s = (PosixState*)st;
    char* path;
    int rc;
    if (!s || !s2_valid_name(obj)) return -1;
    path = posix_path(s, obj);
    if (!path) return -1;
    rc = unlink(path) == 0 ? 0 : -1;
    free(path);
    return rc;
}

static int posix_truncate(void* st, const char* obj, uint64_t len) {
    PosixState* s = (PosixState*)st;
    char* path;
    int fd, rc;
    if (!s || !s2_valid_name(obj)) return -1;
    if (len > (uint64_t)9223372036854775807ULL) return -1;
    path = posix_path(s, obj);
    if (!path) return -1;
    fd = open(path, O_RDWR); /* no O_CREAT: missing object fails */
    free(path);
    if (fd < 0) return -1;
    rc = ftruncate(fd, (off_t)len) == 0 ? 0 : -1;
    if (close(fd) != 0) rc = -1;
    return rc;
}

static int posix_enumerate(void* st, int (*cb)(void* ctx, const char* name), void* ctx) {
    PosixState* s = (PosixState*)st;
    DIR* d;
    struct dirent* ent;
    if (!s || !cb) return -1;
    d = opendir(s->dir);
    if (!d) return -1;
    while ((ent = readdir(d)) != NULL) {
        char* path;
        struct stat sb;
        if (s2_name_eq(ent->d_name, ".") || s2_name_eq(ent->d_name, "..")) continue;
        /* Objects are regular files; skip subdirectories and
         * anything that cannot be stated (conservative: a
         * non-file entry is not an object, but it also does
         * not prove the store empty — stat failure on an
         * entry aborts the walk as an error instead). */
        path = posix_path(s, ent->d_name);
        if (!path) {
            closedir(d);
            return -1;
        }
        if (stat(path, &sb) != 0) {
            free(path);
            closedir(d);
            return -1;
        }
        free(path);
        if (!S_ISREG(sb.st_mode)) continue;
        if (cb(ctx, ent->d_name) != 0) {
            closedir(d);
            return 1;
        }
    }
    closedir(d);
    return 0;
}

static void posix_close(void* st) {
    PosixState* s = (PosixState*)st;
    if (!s) return;
    free(s->dir);
    free(s);
}

static const YowzaStoreVTable posix_vt = {
    posix_write_at, posix_read_at, posix_size, posix_sync_obj, posix_sync_dir,
    posix_publish, posix_remove, posix_truncate, posix_close, posix_size_status,
    posix_enumerate
};

YowzaStore* yowza_store_posix_create(const char* dir) {
    struct stat sb;
    PosixState* s;
    YowzaStore* store;
    if (!dir || dir[0] == '\0') return NULL;
    if (stat(dir, &sb) != 0 || !S_ISDIR(sb.st_mode)) return NULL;
    s = (PosixState*)malloc(sizeof(PosixState));
    if (!s) return NULL;
    s->dir = s2_strdup(dir);
    if (!s->dir) {
        free(s);
        return NULL;
    }
    store = (YowzaStore*)malloc(sizeof(YowzaStore));
    if (!store) {
        free(s->dir);
        free(s);
        return NULL;
    }
    store->vt = &posix_vt;
    store->state = s;
    return store;
}

#endif /* __wasm__ */

/* ---------------------------------------------------------------- */
/* Generic wrapper + whole-object helpers                            */
/* ---------------------------------------------------------------- */

YowzaStore* yowza_store_attach(const YowzaStoreVTable* vt, void* state) {
    YowzaStore* store;
    if (!vt) return NULL;
    store = (YowzaStore*)malloc(sizeof(YowzaStore));
    if (!store) return NULL;
    store->vt = vt;
    store->state = state;
    return store;
}

void yowza_store_close(YowzaStore* store) {
    if (!store) return;
    if (store->vt && store->vt->close) store->vt->close(store->state);
    free(store);
}

int yowza_store_write_all(YowzaStore* store, const char* obj, const uint8_t* data, uint64_t len) {
    uint64_t done = 0;
    if (!store || !store->vt || !store->vt->write_at || !store->vt->truncate) return -1;
    if (!s2_valid_name(obj)) return -1;
    if (len > 0 && !data) return -1;
    /* Bounded chunks; never a whole-object buffer internally. A
     * failure midway leaves a partially replaced object: NOT
     * atomic (use tmp + publish for atomic replacement). */
    while (done < len) {
        uint64_t chunk = s2_min64(YOWZA_STORE_IO_CHUNK, len - done);
        if (store->vt->write_at(store->state, obj, done, data + (size_t)done, chunk) != 0) return -1;
        done += chunk;
    }
    if (len == 0) {
        /* Ensure the object exists (possibly empty). */
        if (store->vt->write_at(store->state, obj, 0, NULL, 0) != 0) return -1;
    }
    /* Shrink a previously longer object to exactly len. */
    if (store->vt->truncate(store->state, obj, len) != 0) return -1;
    return 0;
}

int yowza_store_size_status(YowzaStore* store, const char* obj, uint64_t* out_size) {
    if (!store || !store->vt || !out_size) return YOWZA_STORE_IO_ERROR;
    if (store->vt->size_status) return store->vt->size_status(store->state, obj, out_size);
    if (!store->vt->size) return YOWZA_STORE_UNKNOWN;
    return store->vt->size(store->state, obj, out_size) == 0
        ? YOWZA_STORE_OK : YOWZA_STORE_UNKNOWN;
}

int yowza_store_enumerate(YowzaStore* store, int (*cb)(void* ctx, const char* name), void* ctx) {
    if (!store || !store->vt || !cb) return -1;
    if (!store->vt->enumerate) return -1;
    return store->vt->enumerate(store->state, cb, ctx);
}

typedef struct {
    int found;
} S2DetectCtx;

static int s2_detect_cb(void* ctx, const char* name) {
    S2DetectCtx* c = (S2DetectCtx*)ctx;
    (void)name;
    if (!c) return 0;
    c->found = 1;
    return 1; /* stop at the first object: existence is proven */
}

int yowza_store_detect_prior_content(YowzaStore* store, int* has_content) {
    S2DetectCtx ctx;
    int rc;
    if (!store || !has_content) return -1;
    ctx.found = 0;
    rc = yowza_store_enumerate(store, s2_detect_cb, &ctx);
    if (rc == 0) {
        *has_content = 0;
        return 0;
    }
    if (rc == 1) {
        *has_content = 1;
        return 0;
    }
    /* rc == -1: no enumerate hook, or enumeration failed.
     * *has_content is deliberately left untouched: the
     * caller must not read it as "empty". */
    return -1;
}

int yowza_store_read_all(YowzaStore* store, const char* obj, uint8_t* out, uint64_t len) {
    uint64_t done = 0;
    if (!store || !store->vt || !store->vt->read_at) return -1;
    if (!s2_valid_name(obj)) return -1;
    if (len > 0 && !out) return -1;
    while (done < len) {
        uint64_t chunk = s2_min64(YOWZA_STORE_IO_CHUNK, len - done);
        if (store->vt->read_at(store->state, obj, done, out + (size_t)done, chunk) != 0) return -1;
        done += chunk;
    }
    return 0;
}
