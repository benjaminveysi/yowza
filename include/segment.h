/* Phase 2, Track B — Immutable Segment format v1 (see YOWZA_ARCHITECTURE_SPEC.md §5).
 *
 * LAYOUT (all integers little-endian; checksums FNV-1a-64, offset
 * 14695981039346656037, prime 1099511628211, same as Phase 1):
 *
 *   Header (fixed 64 bytes):
 *     magic "YWSEG1" (6) | version u32 = 1 | block_size u32 = 4096
 *     | entry_count u32 | index_type u32 | model_version u32
 *     | min_seq u64 | max_seq u64 | first_key_len u32 | last_key_len u32
 *     | flags u32 (bit 0 = Bloom filter present) | reserved (zero) to 64
 *   First key bytes, then last key bytes (both empty iff entry_count == 0).
 *   Data blocks: entries sorted strictly by key, each
 *     [key_len u32][key][val_len u32][val][commit_seq u64][crdt_ts u64][flags u8]
 *     where flags bit 0 = tombstone (val_len MUST be 0 for tombstones).
 *     commit_seq and crdt_ts are SEPARATE fields and are never merged.
 *     Block entry bytes target 4096 (block_size); a single entry larger
 *     than 4096 gets a block of its own. Each block is followed by one
 *     u64 FNV-1a checksum over that block's entry bytes ONLY.
 *   Sparse index: one entry per block
 *     [first_key_len u32][first_key][block_offset u64][block_len u64]
 *     where block_offset is the absolute file offset of the block's first
 *     entry byte and block_len is the block's ENTRY byte count (excluding
 *     the 8-byte checksum that follows it). Followed by one u64 FNV-1a
 *     checksum over the index entry bytes.
 *   Optional Bloom filter (iff header flags bit 0):
 *     [filter_bytes u32][bytes][checksum u64 (FNV-1a over the bytes)].
 *     Double-hash, k = 2: positions (h1 + i*h2) % nbits, i = 0,1, with
 *     h1 = FNV-1a(key) and h2 = splitmix64(h1 + 0x9e3779b97f4a7c15).
 *     False positives allowed, false negatives forbidden. The writer
 *     sizes the filter at 8 bits/key, clamped to [1024, 65536] bits;
 *     the format already carries filter_bytes, so a later phase may
 *     size it by entry count without changing readers.
 *   Footer (fixed 32 bytes):
 *     magic "YWSEG$" (6) | total_len u64 | header_checksum u64
 *     | index_checksum u64 | reserved (zero, 2 bytes).
 *     header_checksum = FNV-1a over the 64 header bytes FOLLOWED BY the
 *     first-key bytes and the last-key bytes (i.e. the whole header
 *     region). index_checksum = the same value stored after the index
 *     entries (FNV-1a over the index entry bytes).
 *
 * DEVIATION / CLARIFICATION (read this before reconciling with Track C):
 * the task text says both "footer fixed 32 bytes" with exactly the fields
 * above AND "segment total checksum = FNV-1a over everything before the
 * footer checksum fields". A 32-byte footer has no slot left for a third
 * checksum (6 + 8 + 8 + 8 = 30, reserved = 2), so no separate total
 * checksum is stored in the footer. Instead, EVERY byte before the footer
 * is covered by some stored checksum: header region by header_checksum,
 * each block's entries by its block checksum, the index by
 * index_checksum, the Bloom bytes by the Bloom checksum. seg_open /
 * seg_lookup / seg_iter_next verify all of them (see the API notes).
 * A whole-object FNV-1a (header through footer inclusive) is available
 * separately as seg_file_checksum(), and THAT is the value stored in a
 * manifest's per-segment SegMeta.checksum field.
 *
 * READER MEMORY CONTRACT: the reader NEVER loads a whole segment. All
 * store reads performed by seg_open/seg_lookup/seg_iter_next are issued
 * in chunks of at most SEG_READER_MAX_CHUNK (256) bytes, and the only
 * per-reader state is the fixed-size SegReader struct below (no heap,
 * no block buffer, no reader-owned index table). Lookup/iteration stream entries
 * through small stack buffers and copy a matched entry into caller
 * buffers. Consequence of the v1 header (no index-offset field):
 * seg_open itself only verifies the header region and the footer;
 * locating the index requires a streaming block-boundary walk, which
 * runs LAZILY on the first lookup/iteration (cached in the reader) and
 * verifies block checksums, the index checksum, the Bloom checksum,
 * min/max sequences and the first/last key copies as it goes. So a
 * corrupt header/footer/truncation fails at seg_open, while corrupt
 * block/index/Bloom bytes fail at the first lookup/iteration — never
 * silently, and never by returning data from a corrupt block (a block
 * is checksum-verified BEFORE any of its entries is returned).
 *
 * EXTENSION CONTRACT (index_type / model_version): Phase 2 knows only
 * value 0 for both. seg_write rejects any other value, and seg_open
 * rejects a segment carrying any other value, cleanly with -1 (never a
 * misread). A segment written with index_type = 0 / model_version = 0
 * MUST remain readable after later phases add new types: later readers
 * keep accepting 0 with exactly this layout, and new types are added
 * alongside it, never by redefining what 0 means. The same rule applies
 * to the manifest's per-segment index_type/model_version fields.
 *
 * STORE CONTRACT (Track A, include/storage2.h): if that header exists
 * it is used; otherwise the minimal local declaration below applies and
 * the integrator reconciles. Track A's layout is authoritative:
 *   struct YowzaStore { const YowzaStoreVTable* vt; void* state; };
 *   every vtable function takes the backend state (void*) as its first
 *   argument and returns 0 on success, -1 on ANY error; a short
 *   read_at/write_at is an error, never a partial result. (This code
 *   additionally tolerates a byte-count return equal to the requested
 *   length from a hypothetical driver, but Track A drivers return
 *   0/-1 only.)
 * write_at creates the object if absent and overwrites from the given
 * offset; a stale tail after a shorter rewrite is removed by the
 * writers via truncate. Required per function: seg_write needs
 * write_at + truncate; seg_open/lookup/iter need read_at + size;
 * manifest_publish needs sync_obj + publish + sync_dir. Note Track A's
 * POSIX publish already fsyncs tmp + renames + fsyncs dir internally,
 * so manifest_publish's explicit syncs around it are redundant-but-
 * harmless there and load-bearing on backends where publish does not
 * sync (see storage2.h durability notes).
 * This file uses no libc and no malloc, so it builds under the Wasm
 * targets as well as natively.
 */

#ifndef YOWZA_SEGMENT_H
#define YOWZA_SEGMENT_H

#include "arena.h"

/* ---- Storage interface (Track A) or local stand-in ---- */
#if defined(__has_include)
# if __has_include("storage2.h")
#  include "storage2.h"
#  define YOWZA_SEGMENT_HAS_STORAGE2 1
# endif
#endif
#ifndef YOWZA_SEGMENT_HAS_STORAGE2
/* Minimal local declaration matching Track A's include/storage2.h
 * exactly (same field name `vt`, same void*-state-first signatures),
 * used ONLY if storage2.h is absent. The integrator replaces this
 * block with include/storage2.h when Track A lands; code below only
 * uses ->vt and ->state, so it compiles against either. */
typedef struct YowzaStore YowzaStore;
typedef struct YowzaStoreVTable {
    /* Write exactly len bytes at offset; 0 on success, -1 on error. */
    int (*write_at)(void* st, const char* obj, uint64_t off,
                    const uint8_t* data, uint64_t len);
    /* Read exactly len bytes at offset; 0 on success, -1 on error. */
    int (*read_at)(void* st, const char* obj, uint64_t off,
                   uint8_t* out, uint64_t len);
    /* Total object size in *out_size; -1 if the object does not exist. */
    int (*size)(void* st, const char* obj, uint64_t* out_size);
    /* Durability barriers; 0 ok, -1 error. */
    int (*sync_obj)(void* st, const char* obj);
    int (*sync_dir)(void* st);
    /* Atomic rename tmp_obj -> final_obj; 0 ok, -1 error. */
    int (*publish)(void* st, const char* tmp_obj, const char* final_obj);
    /* Remove an object; 0 ok, -1 error (missing object is an error). */
    int (*remove)(void* st, const char* obj);
    /* Set object length (extend with zeros or shrink); 0 ok, -1 error. */
    int (*truncate)(void* st, const char* obj, uint64_t len);
    /* Release backend state only; may be NULL. */
    void (*close)(void* st);
} YowzaStoreVTable;
struct YowzaStore {
    const YowzaStoreVTable* vt;
    void* state;
};
#endif /* YOWZA_SEGMENT_HAS_STORAGE2 */

/* Fixed format constants. */
#define SEG_FORMAT_VERSION 1u
#define SEG_BLOCK_SIZE 4096u
#define SEG_HEADER_SIZE 64u
#define SEG_FOOTER_SIZE 32u
#define SEG_INDEX_TYPE_SPARSE 0u
#define SEG_MODEL_VERSION_NONE 0u
#define SEG_FLAG_BLOOM 1u
#define SEG_ENTRY_FLAG_TOMBSTONE 1u
/* Largest single store read issued by the reader (see header comment). */
#define SEG_READER_MAX_CHUNK 256u
/* Largest sparse-index block count a caller-provided binary-search
 * view can hold (see seg_index_view). Segments produced by segdb
 * flush/compact hold at most 4096 entries, i.e. at most a few dozen
 * blocks at 4 KiB per block, so 256 covers them with room to spare.
 * Axis C: a larger segment no longer has to degrade to the
 * linear index scan — see seg_index_view_paged, which stores
 * one offset per PAGE of index entries (default 64 entries per
 * page) and lets seg_lookup binary-search pages, then scan
 * within one page. The full view (256) is unchanged for
 * small segments. */
#define SEG_INDEX_VIEW_MAX 256u
/* Default page size for the paged index view: one stored
 * offset per 64 index entries. A lookup then examines
 * O(log pages + page size) index entries instead of O(blocks). */
#define SEG_INDEX_PAGE_SIZE 64u
/* Longest object name seg_open accepts (incl. NUL in the reader copy). */
#define SEG_MAX_OBJ_LEN 255u

typedef struct {
    const uint8_t* key;
    uint32_t key_len;
    const uint8_t* val; /* NULL for tombstones */
    uint32_t val_len;   /* 0 for tombstones */
    int is_tombstone;
    uint64_t commit_seq; /* MVCC commit sequence — NEVER merged with crdt_ts */
    uint64_t crdt_ts;    /* CRDT timestamp — NEVER merged with commit_seq */
} SegEntry;

/* Reader state. All fields are plain values; the struct owns no heap
 * memory, so seg_close only invalidates it (idx_offs is borrowed
 * from the caller, never owned or freed by the reader).
 * Iteration state lives here so a lookup (which uses only locals)
 * never disturbs an iteration. */
typedef struct SegReader {
    YowzaStore* st;
    char obj[SEG_MAX_OBJ_LEN + 1];
    int open; /* 0 = closed/never opened, 1 = open */
    /* Header fields (validated by seg_open). */
    uint32_t entry_count;
    uint32_t index_type;
    uint32_t model_version;
    uint32_t flags;
    uint64_t min_seq;
    uint64_t max_seq;
    uint32_t first_key_len;
    uint32_t last_key_len;
    uint64_t total_len;
    uint64_t header_len; /* 64 + first_key_len + last_key_len */
    uint64_t footer_header_checksum;
    uint64_t footer_index_checksum;
    /* Lazy discovery (filled by seg_ensure_discovered, on first use). */
    int discovered;
    uint64_t index_offset; /* absolute offset of first index entry byte */
    uint64_t index_len;    /* index entry bytes, excluding its checksum */
    uint32_t block_count;
    uint64_t bloom_offset; /* offset of filter_bytes field, 0 if no Bloom */
    uint32_t bloom_bytes;
    /* Optional caller-owned binary-search view over the sparse
     * index (see seg_index_view): idx_offs[i] is the absolute
     * offset of index entry i, for i in [0, idx_offs_count).
     * NULL/0 means seg_lookup falls back to the paged view
     * or the linear index scan. The buffer must outlive the
     * lookup call; the reader only borrows it and seg_close
     * clears the pointer. The view is valid only for this
     * reader's discovery: the caller must rebuild it after
     * any rediscovery, and seg_lookup ignores it unless
     * idx_offs_count equals block_count. */
    const uint64_t* idx_offs;
    uint32_t idx_offs_count;
    /* Axis C — optional caller-owned PAGED view over the
     * sparse index (see seg_index_view_paged):
     * idx_page_offs[p] is the absolute offset of the first
     * index entry of page p, where page p covers index
     * entries [p*idx_page_size, min((p+1)*idx_page_size,
     * block_count)). seg_lookup binary-searches pages,
     * then scans within the one candidate page. Ignored
     * unless idx_page_count equals ceil(block_count /
     * idx_page_size) with idx_page_size > 0; a torn paged
     * view falls through to the linear scan, never to a
     * false absent — the same fail-safe as the full view.
     * The full view takes precedence when both are set. */
    const uint64_t* idx_page_offs;
    uint32_t idx_page_count;
    uint32_t idx_page_size;
    /* Iteration state. */
    uint64_t iter_index_pos;  /* offset of next index entry to read */
    uint64_t iter_index_end;  /* index_offset + index_len */
    uint64_t iter_block_offset;
    uint64_t iter_block_len;
    uint64_t iter_entry_pos;  /* offset of next entry inside current block */
    uint64_t iter_block_end;  /* iter_block_offset + iter_block_len */
    int iter_in_block;        /* 1 while entries remain in current block */
    uint32_t iter_returned;   /* entries returned so far */
} SegReader;

/* Write a segment object. entries MUST arrive sorted strictly by key
 * (lexicographic byte order, no duplicates); unsorted input, an empty
 * key, a tombstone with val_len != 0, or index_type/model_version != 0
 * is rejected with -1 and no object is guaranteed (a partial object may
 * be left behind; the caller must remove or overwrite it). count == 0
 * writes a valid empty segment (entries may be NULL). use_bloom != 0
 * adds the Bloom filter (header flags bit 0). Does NOT sync: the caller
 * (Track C flush) syncs the object before publishing a manifest that
 * references it. On success *out_bytes (if non-NULL) is the total
 * object length. Returns 0 on success, -1 on error. */
int seg_write(YowzaStore* st, const char* obj, const SegEntry* entries,
              uint32_t count, uint32_t index_type, uint32_t model_version,
              int use_bloom, uint64_t* out_bytes);

/* ---- Streaming segment writer (Track A) ----
 * Writes a segment entry by entry without holding all entries in
 * memory: one 4 KiB block buffer + one fixed 8 KiB Bloom filter
 * (65536 bits, always — see below) + temp objects on the store.
 * Data blocks go to "<obj>.dat" and index entries to
 * "<obj>.idx" during streaming; seg_write_finish assembles the
 * final object (header, copied blocks, sparse index, Bloom,
 * footer) and removes the temps. The produced segment is byte-
 * compatible with seg_write (same v1 layout, same reader) except
 * for one documented difference: the Bloom filter is always
 * 65536 bits when use_bloom is set, whereas seg_write sizes it by
 * entry count (clamped [1024, 65536] bits). The reader derives
 * nbits from the stored filter_bytes, so both forms read
 * identically and the no-false-negative property holds either way;
 * the streaming form simply spends up to 8 KiB of filter for small
 * segments. Entries must arrive strictly sorted (same rule as
 * seg_write); a violation fails the add and the writer must be
 * aborted (seg_write_abort removes the temps). The writer is
 * caller-allocated (~15 KiB); allocate it statically, never as a
 * small-stack local. */
typedef struct SegWriter {
    YowzaStore* st;
    char obj[SEG_MAX_OBJ_LEN + 1];
    char dat_obj[SEG_MAX_OBJ_LEN + 1];
    char idx_obj[SEG_MAX_OBJ_LEN + 1];
    uint32_t index_type;
    uint32_t model_version;
    int use_bloom;
    int open;
    int finished;
    uint8_t block[SEG_BLOCK_SIZE];
    uint32_t block_len;
    uint8_t block_first_key[1024];
    uint32_t block_first_len;
    uint64_t data_len; /* bytes in the data temp (entries + checksums) */
    uint64_t idx_len;  /* bytes in the index temp */
    uint32_t block_count;
    uint32_t count;
    uint64_t min_seq;
    uint64_t max_seq;
    uint8_t first_key[1024];
    uint32_t first_len;
    uint8_t last_key[1024];
    uint32_t last_len;
    uint8_t last_added_key[1024];
    uint32_t last_added_len;
    uint8_t bloom[8192];
} SegWriter;

int seg_write_open(YowzaStore* st, const char* obj, uint32_t index_type,
                   uint32_t model_version, int use_bloom, SegWriter* w);
int seg_write_add(SegWriter* w, const SegEntry* e);
int seg_write_finish(SegWriter* w, uint64_t* out_bytes);
void seg_write_abort(SegWriter* w);

/* Open and validate a segment: header magic/version/fields, the header
 * checksum, the footer magic, and total_len == object size. The index /
 * block / Bloom checksums are verified lazily by seg_ensure_discovered
 * on the first lookup/iteration (see header comment). Returns 0 on
 * success, -1 on any corruption, truncation, unknown index_type /
 * model_version, unknown header flag bit, or bad argument. On -1 the
 * reader is left closed and must not be used. */
int seg_open(YowzaStore* st, const char* obj, SegReader* r);

/* Run the lazy discovery pass explicitly (also run automatically by
 * seg_lookup / seg_iter_next). Verifies, streaming with O(1) memory:
 * every block checksum, the index checksum, the Bloom checksum (if
 * present), header min/max sequences, and the first/last key copies
 * against the actual first/last entries. Returns 0 on success, -1 on
 * any corruption or I/O error. */
int seg_ensure_discovered(SegReader* r);

/* Build a binary-searchable view of the sparse index: offs[i] is
 * the absolute offset of index entry i, for i in [0, block_count).
 * The reader must already be discovered. Returns block_count on
 * success, or -1 when the block count exceeds cap or the index
 * bytes are inconsistent with discovery (wrong entry count,
 * bad length, overrun). The view is a pure navigation aid: it
 * carries no trust of its own — seg_lookup still verifies the
 * chosen block's checksum before returning any entry, and any
 * view failure degrades to the linear index scan, never to an
 * error or a wrong answer. The caller owns the offs buffer
 * (a stack array of SEG_INDEX_VIEW_MAX entries is the intended
 * use); the reader only borrows it until seg_close. Building
 * the view re-walks only the index region (block_count small
 * reads), never the blocks themselves. */
int seg_index_view(SegReader* r, uint64_t* offs, uint32_t cap);

/* Build a paged binary-searchable view of the sparse index:
 * offs[p] is the absolute offset of the first index entry
 * of page p (page_size entries per page; page_size == 0
 * means SEG_INDEX_PAGE_SIZE). The reader must already be
 * discovered. Returns the page count on success, or -1
 * when the page count exceeds cap, page_size is 0 after
 * defaulting is impossible (never: 0 defaults), or the
 * index bytes are inconsistent with discovery. The view
 * is a pure navigation aid like seg_index_view: seg_lookup
 * still verifies the chosen block's checksum before
 * returning any entry, and any view failure degrades to
 * the linear index scan, never to an error or a wrong
 * answer. Building the view re-walks only the index
 * region once (O(blocks) small reads), then every
 * lookup on this reader costs O(log pages + page_size)
 * index entries instead of O(blocks). The caller owns
 * the offs buffer (ceil(block_count / page_size)
 * entries); attach it by setting r->idx_page_offs,
 * r->idx_page_count, and r->idx_page_size before lookup. */
int seg_index_view_paged(SegReader* r, uint64_t* offs, uint32_t cap, uint32_t page_size);

/* Lookup cost counter (Axis C measurement, not a
 * correctness input): counts sparse-index entries whose
 * key was compared during seg_lookup, across all three
 * candidate-selection paths (full view probes, paged
 * page probes + within-page entries, linear entries).
 * Block-entry streaming and Bloom checks are NOT counted.
 * Reset before a measurement, read after. */
void seg_lookup_index_entries_reset(void);
uint64_t seg_lookup_index_entries(void);

/* Track C page-load counter (measurement, not a correctness
 * input): counts sparse-index PAGES touched by seg_lookup's
 * paged path only — one per binary-search probe, plus one
 * for the within-page scan when a candidate page is found.
 * A paged lookup over P pages therefore touches at most
 * floor(log2(P)) + 2 pages, independent of block count;
 * the linear and full-view paths never touch this counter.
 * Reset with seg_lookup_index_entries_reset (shared reset),
 * read after. */
uint64_t seg_lookup_paged_pages(void);

/* Look up one key. Returns 0 if found (live entry, or tombstone with
 * out->is_tombstone == 1, out->val == NULL, out->val_len == 0), 1 if
 * absent, -1 on corruption / I/O error / val_buf too small. On 0,
 * out->key points at the caller's key, out->val points at val_buf
 * (live only), and out->commit_seq / out->crdt_ts are the stored
 * fields, kept distinct. The Bloom filter is only trusted for a
 * negative answer after its checksum has been verified (discovery
 * does this); a corrupt Bloom therefore fails with -1, it can never
 * cause a false "absent". Candidate-block selection, in order:
 * (1) binary-searches the caller's idx_offs view when it is
 * present and consistent (idx_offs_count == block_count);
 * (2) paged binary search over idx_page_offs when present
 * and consistent (Axis C: the >256-block path); (3)
 * otherwise it linear-scans the sparse index exactly as
 * before. A view (full or paged) that yields no candidate
 * falls through to the linear scan, so a torn view can never
 * cause a false "absent" either. */
int seg_lookup(SegReader* r, const uint8_t* key, uint32_t key_len,
               SegEntry* out, uint8_t* val_buf, uint32_t val_buf_cap);

/* Iterate entries in sorted key order (live and tombstones). Returns
 * 0 with *out filled (out->key points at key_buf, out->val at val_buf
 * for live entries, NULL/0 for tombstones), 1 at end of segment, -1
 * on corruption / I/O error / key_buf or val_buf too small. The block
 * a returned entry belongs to is checksum-verified BEFORE its first
 * entry is returned, so iteration never yields data from a corrupt
 * block. To re-iterate, close and reopen the reader. */
int seg_iter_next(SegReader* r, SegEntry* out, uint8_t* key_buf,
                  uint32_t key_cap, uint8_t* val_buf, uint32_t val_cap);

/* Invalidate the reader. No heap is owned, so this only marks it
 * closed; it is safe to call on a closed reader. */
void seg_close(SegReader* r);

/* FNV-1a-64 over an entire stored object (header through footer
 * inclusive), streamed with O(1) memory. This is the value a manifest
 * stores in SegMeta.checksum for the segment. Returns 0 on success,
 * -1 if the object is missing or unreadable. */
int seg_file_checksum(YowzaStore* st, const char* obj, uint64_t* out);

#endif /* YOWZA_SEGMENT_H */
