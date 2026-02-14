/* Yowza64 — Storage ABI v2 (Phase 2, Track A)
 *
 * A backend-neutral, 64-bit object store interface for the layered
 * storage of YOWZA_ARCHITECTURE_SPEC.md §5. Every object is addressed
 * by a flat name plus a 64-bit byte offset; there are no directories
 * in the object namespace and no mmap assumption anywhere in this
 * interface (spec §5.2: the engine talks to the host with explicit
 * block I/O at 64-bit offsets).
 *
 * ------------------------------------------------------------------
 * Interface contract (applies to EVERY backend, including pluggable
 * ones — see "Pluggable object-store backends" below):
 *
 *  - All functions return 0 on success, -1 on ANY error.
 *  - write_at / read_at are all-or-nothing from the caller's point
 *    of view: a short write or a short read is an ERROR (-1), never
 *    a silent partial result. (This differs deliberately from the
 *    Phase-0 StorageDriver, whose read/append return byte counts.)
 *  - read_at past the end of an object fails; it never returns
 *    partial data padded with anything.
 *  - A FAILED write_at may have partially modified the object (the
 *    -1 reports the failure, it is not a rollback). Callers that
 *    need atomic replacement must write a temp object and then
 *    publish() it; write_all documents the same limitation.
 *  - Object names are flat, single-component names: non-empty, not
 *    "." or "..", containing no '/'. Any other name is rejected
 *    with -1 by every backend, so backends agree on the namespace.
 *  - write_at to a missing object creates it. Gaps between the old
 *    end and a write offset read back as zeroes (sparse holes on
 *    the POSIX backend, calloc/zero-filled chunks on the memory
 *    backend). A zero-length write_at creates the object if it is
 *    missing and otherwise changes nothing (it does not extend
 *    the object, even at an offset past the end).
 *  - truncate to a larger length zero-fills the extension; truncate
 *    to a smaller length discards the tail, and a later re-extension
 *    reads back zeroes (never stale bytes).
 *  - remove of a missing object returns -1.
 *  - publish(tmp_obj, final_obj) atomically replaces final_obj with
 *    the content of tmp_obj (POSIX rename semantics). tmp_obj no
 *    longer exists afterwards. If tmp_obj is missing, publish
 *    fails and final_obj is untouched.
 *  - close(state) releases the backend's state only. It does NOT
 *    free the YowzaStore wrapper itself; use yowza_store_close()
 *    for that (it calls vt->close(state) and then frees the
 *    wrapper). Calling vt->close directly and then freeing the
 *    wrapper by hand is equally valid.
 *  - NULL store / NULL object name / NULL out-pointer arguments
 *    return -1 (close helpers tolerate NULL silently).
 *
 * ------------------------------------------------------------------
 * Durability, per backend (callers must not assume more than this):
 *
 *  Memory backend (yowza_store_mem_create):
 *    Process-local only; all content is LOST on process exit.
 *    sync_obj and sync_dir are no-ops that return 0. Never use it
 *    for data that must survive a restart; it is the test vehicle
 *    (in particular for >UINT32_MAX offsets, see below) and a
 *    stand-in for host-provided memory in WASM embeddings.
 *
 *  POSIX backend (yowza_store_posix_create, native only):
 *    One file per object inside the directory given at creation.
 *    An object is durable once sync_obj has returned 0 for it.
 *    A publish is durable once publish has returned 0: publish
 *    fsyncs the temp file, renames it onto the final name, then
 *    fsyncs the directory. (If the directory fsync fails, publish
 *    returns -1 even though the rename already happened; the new
 *    content is visible but its rename is not yet durable.)
 *    NOTE: a plain write_at is NOT durable until sync_obj (or a
 *    publish of that object) succeeds — the Phase-1 lesson that a
 *    failed WAL append must not leave a mutation applied applies
 *    here as "a write is not a commit until it is synced".
 *
 *  Pluggable / object-store backends:
 *    Durability is: durable ONLY after a successful publish/commit
 *    returns. sync_obj / sync_dir on such a backend may be no-ops;
 *    generic callers must therefore treat publish as the durability
 *    point and must not assume sync_obj alone made an object
 *    durable on an arbitrary backend.
 *
 * ------------------------------------------------------------------
 * Pluggable object-store backends (attachment interface):
 *
 *  The YowzaStoreVTable below IS the attachment interface. A new
 *  backend implements the nine function pointers, fills a
 *  YowzaStore { vt, state } (directly, or via yowza_store_attach),
 *  and every generic consumer — write_all/read_all, and the
 *  segment/manifest code of the other Phase-2 tracks — works
 *  against it unchanged. No backend-specific code may leak into
 *  those consumers.
 *
 *  Implementing a real S3 (or other cloud object-store) adapter is
 *  OUT OF SCOPE for this track: this file defines the interface
 *  and documents its durability rule, and deliberately ships no
 *  fake S3 implementation. An S3 adapter would map: object name ->
 *  key, write_at -> ranged PUT / multipart upload assembly,
 *  read_at -> ranged GET, publish -> atomic key rename/copy (or a
 *  manifest commit, since S3 has no atomic rename), sync_* ->
 *  no-ops, with durability only at publish.
 *
 * ------------------------------------------------------------------
 * Memory backend layout (the >UINT32_MAX test vehicle):
 *
 *  Objects are stored as 1 MiB chunks allocated on demand. A write
 *  at offset 5 GiB touches exactly one chunk (plus the small chunk-
 *  pointer array), so a sparse multi-GB object costs kilobytes, not
 *  gigabytes. Chunk pointers for untouched ranges stay NULL and
 *  read back as zeroes. All offsets, sizes and lengths are
 *  uint64_t throughout; size() can therefore report values above
 *  UINT32_MAX.
 *
 * ------------------------------------------------------------------
 * Helpers:
 *
 *  yowza_store_write_all(store, obj, data, len) replaces the whole
 *  object with `data` (writing in bounded YOWZA_STORE_IO_CHUNK
 *  steps, then truncating to `len` so a previously longer object
 *  shrinks). It is NOT atomic: a failure midway leaves a partially
 *  replaced object. For atomic replacement, write a temp object
 *  with write_all and then publish() it.
 *  yowza_store_read_all(store, obj, out, len) reads exactly `len`
 *  bytes from offset 0 into the caller's buffer, in bounded
 *  YOWZA_STORE_IO_CHUNK steps; it returns -1 on a short read.
 *  Callers learn `len` from size() first. Neither helper ever
 *  allocates a whole-object buffer internally, so neither requires
 *  a multi-GB object to fit in memory at once; the CALLER's buffer
 *  is the caller's responsibility.
 */

#ifndef YOWZA_STORAGE2_H
#define YOWZA_STORAGE2_H

#include "arena.h" /* uint8_t / uint64_t / size_t, wasm-safe */

typedef struct YowzaStore YowzaStore;

/* Track A status split (see "Status split" below): size_status
 * distinguishes a missing object from an I/O error, which the
 * legacy 0/-1 size() cannot. */
#define YOWZA_STORE_OK 0
#define YOWZA_STORE_NOT_FOUND 1
#define YOWZA_STORE_IO_ERROR 2
#define YOWZA_STORE_UNKNOWN 3 /* backend has no size_status hook */

typedef struct {
  /* 0 ok, -1 on any short write or error */
  int (*write_at)(void* st, const char* obj, uint64_t off, const uint8_t* data, uint64_t len);
  /* 0 ok, -1 on any short read or error */
  int (*read_at)(void* st, const char* obj, uint64_t off, uint8_t* out, uint64_t len);
  int (*size)(void* st, const char* obj, uint64_t* out_size);
  /* durable for this object (see durability notes above) */
  int (*sync_obj)(void* st, const char* obj);
  /* directory/metadata sync; mem backend = 0 */
  int (*sync_dir)(void* st);
  /* atomic rename. Returns 0 on success, -1 on a DEFINITE failure
   * (rename did not happen: tmp missing, bad name, rename error),
   * and -2 on an INDETERMINATE outcome (rename happened, but a
   * durability sync after it failed, so the rename is visible but
   * not yet proven durable). Callers must never treat -2 as -1:
   * -1 means "old state still authoritative", -2 means "re-check
   * before trusting either state" (see segdb.h three-state
   * publish). A backend with no sync step after rename (the
   * memory backend) never returns -2. */
  int (*publish)(void* st, const char* tmp_obj, const char* final_obj);
  int (*remove)(void* st, const char* obj);
  int (*truncate)(void* st, const char* obj, uint64_t len);
  void (*close)(void* st);
  /* Optional (NULL = legacy backend): size with a status split.
   * Returns YOWZA_STORE_OK / NOT_FOUND / IO_ERROR and fills
   * *out_size only on OK. Backends that implement it must return
   * NOT_FOUND exactly when the object does not exist, never for a
   * permission, I/O, or transient error. */
  int (*size_status)(void* st, const char* obj, uint64_t* out_size);
  /* Optional (NULL = backend cannot enumerate): walk every
   * object name, calling cb(ctx, name) for each. The callback
   * returns 0 to continue, non-zero to stop early.
   * enumerate returns 0 when the walk completed, 1 when the
   * callback stopped it early, -1 on a backend error or NULL
   * argument. Names are valid flat object names (see above);
   * the callback must not retain the name pointer past its
   * return. Gate 2.4b: enumeration is what lets callers
   * distinguish "virgin empty store" from "metadata lost
   * while objects remain" — a backend without it can never
   * prove emptiness, and callers must not treat its silence
   * as NOT_FOUND (see yowza_store_detect_prior_content). */
  int (*enumerate)(void* st, int (*cb)(void* ctx, const char* name), void* ctx);
} YowzaStoreVTable;

struct YowzaStore { const YowzaStoreVTable* vt; void* state; };

/* Internal I/O chunk cap for write_all/read_all (64 KiB). */
#define YOWZA_STORE_IO_CHUNK ((uint64_t)65536)

/* In-memory backend: sparse 1 MiB chunks, process-local only.
 * Returns NULL on allocation failure. */
YowzaStore* yowza_store_mem_create(void);

#ifndef __wasm__
/* POSIX backend: one file per object inside `dir`. `dir` must
 * already exist and be a directory; returns NULL otherwise (or on
 * allocation failure). The directory path is copied. */
YowzaStore* yowza_store_posix_create(const char* dir);
#endif

/* Attach a caller-implemented backend (e.g. a future object-store
 * adapter) to the generic interface. Returns NULL on allocation
 * failure. The caller owns `state` until yowza_store_close(). */
YowzaStore* yowza_store_attach(const YowzaStoreVTable* vt, void* state);

/* Calls vt->close(state) (if present) and frees the wrapper.
 * Tolerates NULL. */
void yowza_store_close(YowzaStore* store);

/* Whole-object helpers, bounded-chunk loops (see header notes).
 * write_all replaces the object; read_all reads exactly `len`
 * bytes from offset 0. Both return 0 on success, -1 on error. */
int yowza_store_write_all(YowzaStore* store, const char* obj, const uint8_t* data, uint64_t len);
int yowza_store_read_all(YowzaStore* store, const char* obj, uint8_t* out, uint64_t len);

/* Status-split size probe. Returns YOWZA_STORE_OK (and fills
 * *out_size), NOT_FOUND, IO_ERROR, or UNKNOWN (backend has no
 * size_status hook and its legacy size() failed, so absence and
 * I/O error cannot be told apart — callers in that case must fall
 * back to the legacy ambiguous behaviour, never silently claim
 * NOT_FOUND). A legacy size() success is OK. */
int yowza_store_size_status(YowzaStore* store, const char* obj, uint64_t* out_size);

/* Walk every object name (see the enumerate hook above).
 * Returns 0 (walk completed), 1 (callback stopped early),
 * or -1 (no enumerate hook, backend error, NULL argument).
 * The callback's non-zero return is reported as 1, never
 * confused with an error. */
int yowza_store_enumerate(YowzaStore* store, int (*cb)(void* ctx, const char* name), void* ctx);

/* Prior-content detection (Gate 2.4b): does the store hold
 * ANY object? Returns 0 and fills *has_content (0 = proven
 * empty, 1 = at least one object) when enumeration is
 * authoritative. Returns -1 when the backend cannot
 * enumerate or enumeration fails: in that case *has_content
 * is left untouched and the caller MUST NOT treat the store
 * as empty — "cannot prove content" is not "no content".
 * This is the API that closes the Track A double-loss
 * hole at the storage layer: CURRENT + CURRENT.bak +
 * metadata all gone is no longer indistinguishable from
 * virgin when objects remain, because the objects
 * themselves are enumerable. */
int yowza_store_detect_prior_content(YowzaStore* store, int* has_content);

#endif /* YOWZA_STORAGE2_H */
