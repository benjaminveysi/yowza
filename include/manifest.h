/* Phase 2, Track B — Versioned Manifest format v2 (see
 * YOWZA_ARCHITECTURE_SPEC.md §5.5). Manifest binary, little-endian:
 *
 *   magic "YWMAN2" (6) | version u32 = 1 | generation u64
 *   | checkpoint_seq u64 | schema_version u32 | segment_count u32
 *   per segment:
 *     [obj_len u32][obj bytes (no NUL)][first_key_len u32][first_key]
 *     [last_key_len u32][last_key][min_seq u64][max_seq u64]
 *     [entry_count u32][index_type u32][model_version u32]
 *     [byte_size u64][checksum u64]
 *   manifest checksum u64 (FNV-1a-64 over all preceding bytes).
 *
 * Per-segment checksum semantics: FNV-1a-64 over the segment object's
 * ENTIRE bytes (header through footer inclusive), as computed by
 * seg_file_checksum(). manifest_read validates the manifest's OWN
 * checksum, magic, version, field bounds and exact length; it does NOT
 * re-verify segment objects — Track C compares SegMeta.checksum
 * against seg_file_checksum() during recovery, and treats a mismatch
 * (or a missing segment) as system corruption.
 *
 * Limits (Phase 2): at most 128 segments (fixed array in Manifest);
 * segment object names at most 127 bytes; first/last keys at most
 * 1024 bytes each (covers the ART key limit of 1023). Anything larger
 * fails manifest_write / manifest_read with -1. index_type and
 * model_version follow the segment extension contract: only 0 is
 * known in Phase 2, any other value is rejected on write and on read.
 * schema_version is pass-through (no registry in Phase 2) and is NOT
 * validated against a known set; the manifest format version (u32 = 1
 * in the binary) is validated.
 *
 * PUBLISH ORDER (load-bearing — Track C must follow it exactly):
 *   1. manifest_write(st, tmp_obj, m) — tmp_obj holds the new manifest.
 *   2. manifest_publish(st, tmp_obj, final_obj) — syncs tmp_obj, then
 *      atomically renames tmp_obj -> final_obj, then syncs the
 *      directory. final_obj for a manifest is "manifest.<generation>".
 *   3. Only after step 2, point CURRENT at it: manifest_write_current
 *      (writes the final_obj name bytes to a tmp object), then
 *      manifest_publish(tmp, "CURRENT") — same sync/rename/sync-dir.
 * A crash before step 3 leaves CURRENT pointing at the previous
 * generation, which is the safe state. manifest_write itself does NOT
 * sync (the sync belongs to manifest_publish); a manifest object that
 * was written but never published must never be referenced by CURRENT.
 *
 * RECOVERY FALLBACK is Track C's job using manifest_read: read the
 * object named by CURRENT; if that read fails cleanly (-1), try
 * generation-1 (CURRENT's generation minus one, same naming scheme).
 * manifest_read fails cleanly on every corruption class (bad magic,
 * bad version, bad checksum, truncation, trailing bytes, out-of-range
 * counts/lengths, unknown index_type/model_version) and leaves *out
 * zeroed on failure, so callers must never use a failed result.
 *
 * No libc, no malloc: the Manifest struct is caller-owned (it is
 * ~285 KiB — do NOT put it on a small stack; allocate it statically
 * or in the caller's arena). All reads/writes here are streamed.
 */

#ifndef YOWZA_MANIFEST_H
#define YOWZA_MANIFEST_H

#include "segment.h" /* YowzaStore types + FNV/segment checksum contract */

#define MANIFEST_FORMAT_VERSION 1u
#define MANIFEST_MAX_SEGMENTS 128u
#define MANIFEST_MAX_OBJ_LEN 127u
#define MANIFEST_MAX_KEY_LEN 1024u

typedef struct {
    char obj[MANIFEST_MAX_OBJ_LEN + 1]; /* NUL-terminated name */
    uint32_t first_key_len;
    uint8_t first_key[MANIFEST_MAX_KEY_LEN];
    uint32_t last_key_len;
    uint8_t last_key[MANIFEST_MAX_KEY_LEN];
    uint64_t min_seq;
    uint64_t max_seq;
    uint32_t entry_count;
    uint32_t index_type;    /* 0 only in Phase 2 (see header comment) */
    uint32_t model_version; /* 0 only in Phase 2 (see header comment) */
    uint64_t byte_size;
    uint64_t checksum; /* seg_file_checksum() of the segment object */
} SegMeta;

typedef struct {
    uint64_t generation;
    uint64_t checkpoint_seq;
    uint32_t schema_version;
    uint32_t segment_count;
    SegMeta segments[MANIFEST_MAX_SEGMENTS];
} Manifest;

/* ---- Multi-level directory (Track A item 3) ----
 * When a DB grows past 128 segments, the manifest list itself
 * becomes the bottleneck. The directory is a second-level manifest:
 * a fixed-name object ("manifest.dir") listing group manifest
 * objects in publication order (oldest first). Each group manifest
 * is an ordinary Manifest with <= 128 segments; its generation is
 * its creation generation, so its object name is
 * "manifest.<generation>". The last group's manifest is the
 * authoritative checkpoint (seq_base); earlier groups are older
 * segments. Directory layout (little-endian, FNV-1a trailer):
 *   magic "YWMNDR" (6) | version u32=1 | generation u64 |
 *   checkpoint_seq u64 | schema_version u32 | group_count u32 |
 *   per group: [obj_len u32][obj bytes] |
 *   checksum u64 (FNV-1a over all preceding bytes).
 * The directory has a FIXED name, so publishing a new directory is
 * an in-place replacement: the commit point for group addition is
 * the directory publish itself (three-state: a -2 means the new
 * directory may or may not be visible — reconcile by reading it
 * back and comparing the group list, never by assuming). For the
 * single->directory transition, the commit point is instead the
 * CURRENT pointer publish to "manifest.dir" (the directory object
 * is unreferenced until CURRENT names it). */
#define MANIFEST_DIR_VERSION 1u
/* Track C (storage scale-up): the directory group bound is 64,
 * i.e. 64 x 128 = 8192 segments per SegDB. The directory binary
 * layout is UNCHANGED (group_count u32 + per-group entries), so
 * directories written under the old 32-group bound (group_count
 * <= 32) read identically; only the validation bound moved, to
 * let one DB hold more than the old 4096-segment structural
 * maximum. A reader built with the old bound rejects a
 * >32-group directory with -1 (fail closed, never a misread);
 * that is the documented forward-compatibility limit, not a
 * silent format change. */
#define MANIFEST_DIR_MAX_GROUPS 64u
#define MANIFEST_DIR_OBJ "manifest.dir"
#define MANIFEST_DIR_TMP_OBJ "manifest.dir.tmp"

typedef struct {
    uint64_t generation;
    uint64_t checkpoint_seq;
    uint32_t schema_version;
    uint32_t group_count;
    char groups[MANIFEST_DIR_MAX_GROUPS][MANIFEST_MAX_OBJ_LEN + 1];
} ManifestDir;

int manifest_dir_write(YowzaStore* st, const char* tmp_obj, const ManifestDir* d);
int manifest_dir_read(YowzaStore* st, const char* obj, ManifestDir* out);

/* Write the manifest binary to tmp_obj (no sync; see publish order).
 * Validates segment_count <= 128, non-empty object names, key lengths
 * <= 1024, and index_type/model_version == 0. On success the object
 * holds exactly the manifest bytes (any stale tail is truncated).
 * Returns 0 on success, -1 on error. */
int manifest_write(YowzaStore* st, const char* tmp_obj, const Manifest* m);

/* Publish order, enforced: sync tmp_obj, atomic rename tmp_obj ->
 * final_obj, sync directory. Returns 0 on success, -1 on error (on
 * error the caller must assume the publish may or may not have
 * happened and re-check by reading final_obj). */
int manifest_publish(YowzaStore* st, const char* tmp_obj,
                     const char* final_obj);

/* Read + fully validate a manifest object (see header comment for the
 * corruption classes). On success *out holds the manifest; on -1 *out
 * is zeroed and must not be used. Returns 0 on success, -1 on error. */
int manifest_read(YowzaStore* st, const char* obj, Manifest* out);

/* Write a CURRENT pointer object: tmp_obj receives the manifest object
 * name bytes (no NUL, no newline). Publish it with manifest_publish
 * to "CURRENT" only after the manifest itself is published. */
int manifest_write_current(YowzaStore* st, const char* tmp_obj,
                           const char* manifest_obj);

/* Read a CURRENT pointer object into out (NUL-terminated). cap must
 * be at least MANIFEST_MAX_OBJ_LEN + 1 for full names. Returns 0 on
 * success, -1 if the object is missing, empty, too long for cap, or
 * unreadable. */
int manifest_read_current(YowzaStore* st, const char* current_obj,
                          char* out, uint32_t cap);

/* ---- Gate 2 lifecycle helpers (2.4b) ----
 * The legacy manifest_read_current returns -1 for "missing"
 * and "I/O error" alike, so a caller cannot tell a virgin
 * store from a store whose metadata was lost. These helpers
 * close that hole at the manifest layer using the storage
 * status split (yowza_store_size_status) and prior-content
 * detection (yowza_store_detect_prior_content). They never
 * return "empty" for a store that holds objects or that
 * cannot be proven empty. */

/* Status-split CURRENT read. Returns 0 on success (*out
 * holds the pointer, NUL-terminated), 1 when the pointer
 * object is AUTHORITATIVELY absent (size_status NOT_FOUND),
 * 2 on any I/O error, unknown status, empty object, or
 * unreadable object. Callers must treat 2 as "cannot
 * decide", never as "missing". */
int manifest_current_status(YowzaStore* st, const char* current_obj,
                            char* out, uint32_t cap);

/* Pointer recovery: CURRENT, then its backup CURRENT.bak
 * (Track A publishes the backup first, so losing exactly
 * one pointer object still recovers). Returns 0 on success
 * (*used_fallback = 0 for CURRENT, 1 for CURRENT.bak),
 * -1 when the store is AUTHORITATIVELY virgin (no pointer
 * and no objects at all — safe to initialise fresh), and
 * -2 when prior content exists but no usable pointer does,
 * or when any status is an I/O error / unknown: the store
 * is NOT virgin and must be reported as corrupt, never
 * opened as empty. */
int manifest_recover_pointer(YowzaStore* st, char* out, uint32_t cap,
                             int* used_fallback);

/* Manifest recovery: the CURRENT (or CURRENT.bak) pointer,
 * then that manifest; on a clean manifest-read failure,
 * the previous generation manifest.<gen-1> (same naming
 * scheme). An I/O error on the named manifest is NOT a
 * clean failure: it returns -2 without trying gen-1, so a
 * metadata error can never silently fall back to an older
 * generation. DIRECTORY-MODE GAP (O2, documented): this
 * helper covers only "manifest.<gen>" names. A pointer
 * naming "manifest.dir" fails name parsing and returns -2;
 * callers in directory mode (segdb_recover) must dispatch
 * on the resolved pointer name BEFORE calling this helper,
 * and must not apply a per-group gen-1 fallback (that
 * would silently drop groups). Returns 0 on success
 * (*used_fallback = 1 when any fallback was used — backup
 * pointer or previous generation — else 0), -1 when
 * authoritatively virgin (propagated from
 * manifest_recover_pointer), -2 when a pointer existed
 * but no manifest could be read (corrupt or ambiguous —
 * never "empty"). On any failure *out is zeroed
 * (manifest_read's contract) and must not be used. */
int manifest_recover_manifest(YowzaStore* st, Manifest* out,
                              int* used_fallback);

/* ---- Phase B: versioned immutable directory root + shared
 * published-state resolver ----
 *
 * The legacy directory ("manifest.dir") has a FIXED name, so
 * publishing a new directory overwrites the previous
 * generation in place: one group's fallback to a previous
 * generation could then remove the other groups from the
 * snapshot, because there is no versioned root introducing
 * ALL groups of one generation together. The versioned
 * root fixes that shape (it does not yet replace the
 * flush publish path — see the note below):
 *
 *   object "manifest.root.<gen>" — immutable, one per
 *   generation, same group list layout as the directory
 *   but magic "YWMNRT" (6) | version u32=1 | generation
 *   u64 | checkpoint_seq u64 | schema_version u32 |
 *   group_count u32 | per group [obj_len u32][obj] |
 *   checksum u64 (FNV-1a over all preceding bytes).
 *   The root's generation is BOTH in its body and in its
 *   name; a root whose body generation differs from its
 *   name generation is rejected. Publishing a new root
 *   never touches an older root object, so a generation's
 *   group list is immutable once published.
 *
 * Migration: manifest_migrate_dir_to_root reads a legacy
 * directory object, writes the same group list as a new
 * root object, and publishes it (sync + rename + sync
 * dir). It never modifies or deletes the legacy directory
 * object, so a failed or interrupted migration leaves
 * the legacy state intact and the migration is simply
 * retried. Return codes mirror manifest_publish: 0 =
 * published and durable, -2 = indeterminate (rename
 * happened but the post-rename sync failed — the caller
 * must re-read the root and compare, never assume),
 * -1 = definite failure before the rename.
 * The flush/group publish path still emits the legacy
 * fixed directory today; switching that path to point
 * CURRENT at "manifest.root.<gen>" (with the resolver
 * below already understanding both names) is the
 * remaining publish-path step, NOT claimed here.
 *
 * Shared resolver: segdb_resolve_published_state is the
 * ONE function that turns stored pointers into a
 * validated, checkable published state. Both segdb_recover
 * and segdb_reconcile call it; neither re-reads a pointer
 * + named manifest with separate logic afterwards.
 * It returns 0 with *out->kind set, or -1 only on bad
 * arguments. Kinds:
 *   VIRGIN — authoritatively empty (no pointer, no objects).
 *   SINGLE — pointer names manifest.<gen>, read + validated.
 *   FALLBACK — named manifest failed cleanly and gen-1
 *     read + validated (used_fallback = 1). Never on an
 *     I/O error (that is IO_ERROR, below).
 *   DIRECTORY — pointer names the legacy "manifest.dir",
 *     read + validated; *out->dir holds its group list.
 *   DIRECTORY_ROOT — pointer names "manifest.root.<gen>",
 *     read + validated, body generation == name
 *     generation; *out->dir holds its group list and
 *     *out->generation is the root generation.
 *   AMBIGUOUS — prior content exists but no usable
 *     pointer/manifest/root could be validated (corrupt
 *     metadata, unparseable name, failed fallback).
 *   IO_ERROR — a metadata read returned an I/O error /
 *     unknown status; the caller must fail, never fall
 *     back or open empty.
 * On VIRGIN/AMBIGUOUS/IO_ERROR the manifest/dir fields
 * are zeroed and must not be used. The resolver does NOT
 * verify segment objects (that is segdb's
 * segdb_verify_manifest_segments, applied by the caller
 * to the resolved manifest/groups) — manifest_read and
 * manifest_dir_read/manifest_root_read validate only
 * the metadata objects' own checksums and bounds.
 * SegdbPublishedState holds a full Manifest (~285 KiB):
 * callers must allocate it statically, never on a stack. */
#define MANIFEST_ROOT_VERSION 1u

/* Object-name helpers for versioned roots. */
int manifest_root_name(uint64_t gen, char* out, uint32_t cap);
int manifest_root_parse_gen(const char* name, uint64_t* gen_out);

/* Write a root object to tmp_obj (no sync; publish it
 * with manifest_publish). The body generation is taken
 * from d->generation. Validates like manifest_dir_write. */
int manifest_root_write(YowzaStore* st, const char* tmp_obj,
                        const ManifestDir* d);
/* Read + fully validate a root object into a ManifestDir
 * (same group-list fields; generation = root generation).
 * On failure *out is zeroed. */
int manifest_root_read(YowzaStore* st, const char* obj,
                       ManifestDir* out);

/* Migrate a legacy directory to a versioned root of the
 * given generation. out_name receives
 * "manifest.root.<gen>" on success (0) or on -2
 * (re-read it to decide). The legacy directory object is
 * never modified. */
int manifest_migrate_dir_to_root(YowzaStore* st,
                                 const ManifestDir* dir,
                                 uint64_t gen, char* out_name,
                                 uint32_t cap);

#define SEGDB_PUBLISHED_VIRGIN 0
#define SEGDB_PUBLISHED_SINGLE 1
#define SEGDB_PUBLISHED_FALLBACK 2
#define SEGDB_PUBLISHED_DIRECTORY 3
#define SEGDB_PUBLISHED_DIRECTORY_ROOT 4
#define SEGDB_PUBLISHED_AMBIGUOUS 5
#define SEGDB_PUBLISHED_IO_ERROR 6

typedef struct {
    int kind;
    int used_fallback;
    uint64_t generation;
    char pointer[MANIFEST_MAX_OBJ_LEN + 1];
    Manifest manifest;
    ManifestDir dir;
} SegdbPublishedState;

int segdb_resolve_published_state(YowzaStore* st,
                                  SegdbPublishedState* out);

#endif /* YOWZA_MANIFEST_H */
