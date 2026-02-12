/* Phase 2, Track C — Layered DB (Hot ART + Warm segments + Cold store).
 *
 * Hot = YowzaDB (ART + MVCC + Arena + memory WAL driver), Warm = sorted
 * immutable segments on a YowzaStore, Cold = segments already on the
 * store (file/object backends) plus archive objects. There is NO
 * separate Cold engine in Phase 2 beyond the store backends; do not
 * invent one. Single-threaded invariant: the host serializes callers
 * (as Phase 1 does for the C API); segdb_flush freezes hot writes by
 * that invariant, not by a lock inside this file.
 *
 * Interfaces are the REAL Track A/B headers (storage2.h, segment.h,
 * manifest.h), which landed in this working copy. Where the Track C
 * assignment's sketch differed from those headers, this file follows
 * the headers and the differences are listed here (they are the
 * integration record, not silent drift):
 *  - Store shape: YowzaStore is { vt, state } (storage2.h); vtable
 *    functions take the backend state (void*) first and return
 *    0/-1, with a short read/write as -1. The assignment's flat
 *    function-pointer struct is NOT what Track A shipped.
 *  - seg_lookup returns 0 found / 1 absent / -1 error (not -1 for
 *    absent), and seg_iter_next / seg_lookup take caller key/val
 *    buffers (the reader streams, it never loads a whole segment).
 *  - SegMeta.checksum IS filled: seg_file_checksum() of the segment
 *    object, per segment.h/manifest.h. segdb_recover compares it
 *    against a fresh seg_file_checksum() and fails on mismatch.
 *  - CURRENT is written with manifest_write_current() + a
 *    manifest_publish() to "CURRENT" (manifest.h publish order),
 *    using "CURRENT.tmp" as the tmp object.
 *  - art_iterate already exists in art.h (Track B landed it, ctx-
 *    first callback, returns 0 full / 1 stopped / -1 bad arg);
 *    segdb.c uses it and implements no iterator of its own.
 *
 * Sequence space: segment entries carry EFFECTIVE commit sequences.
 * The hot ART restarts commit_seq at 0 after every flush (arena
 * reset), so segdb keeps seq_base = the manifest checkpoint_seq and
 * maps a hot node version v to effective sequence seq_base + v.
 * Flush writes entries with their effective sequences and advances
 * seq_base to the new checkpoint; recover restores seq_base from the
 * manifest. Hot entries are therefore always newer than every
 * segment entry, which is what makes hot-first Get correct.
 *
 * SegDB size warning: Manifest holds 128 SegMeta entries with
 * full 1024-byte first/last keys, so a SegDB is roughly 300 KiB.
 * Allocate it statically or on the heap, never as a stack local.
 *
 * Return codes: 0 ok, -1 generic error / not found (segdb_get),
 * -3 OOM (propagated from the hot path), -4 segment lookup
 * error (segdb_get / yowza_segdb_get only: manifest-read
 * failure, seg_lookup error, or value larger than the
 * caller/get buffer — NEVER absence), -6 WAL/sync
 * (propagated from the hot path). segdb_get returns -1
 * for not found, including a hot tombstone shadowing a
 * segment value. Callers must never map -4 to absence.
 */
#ifndef YOWZA_SEGDB_H
#define YOWZA_SEGDB_H

#include "arena.h"
#include "art.h"
#include "storage.h"
#include "yowza_db.h"
#include "tx.h"
#include "storage2.h"
#include "segment.h"
#include "manifest.h"

/* Track A three-state publish outcome (see segdb.c): 0 = success,
 * -1 = definite failure before publish (old state authoritative),
 * -2 = INDETERMINATE (a publish attempt happened and its durability
 * is unproven; the DB must be reconciled before further writes).
 * segdb_put/delete/flush/compact return -2 while indeterminate;
 * reads (get/scan) stay available because hot + old segments still
 * describe a valid (old) logical state. */
#define SEGDB_INDETERMINATE (-2)

/* GATE 1 bounds. SEGDB_HIST_MAX: superseded hot versions kept
 * in the SegDB.hist array chunk per flush epoch (see SegDB.hist).
 * Axis C: it is NO LONGER the total history cap. Entries past
 * the array spill into the heap-overflow list (SegdbHistOverflow
 * nodes, allocated from the hot arena, which is exactly the
 * memory that already owns the history key/value bytes and is
 * reset by the flush that transfers them). Capture therefore
 * fails with -3 only on a genuine arena OOM, never at entry
 * 1025. The flush scratch that ranks history entries is still
 * bounded (SEGDB_HIST_FLUSH_SCRATCH_MAX, see segdb.c): a flush
 * whose epoch exceeds it fails loudly with -1 and leaves
 * history intact — a stated remaining limit, not a silent
 * drop. SEGDB_MAX_LEASES: concurrent snapshot leases per
 * SegDB. */
#define SEGDB_HIST_MAX 1024u
#define SEGDB_MAX_LEASES 64u

/* GATE 1: hot-history entry. key points to an arena copy; val
 * points to arena bytes for a live version and is NULL for a
 * tombstone (val_len 0). seq is the EFFECTIVE sequence
 * (seq_base + hot version) the entry carried. */
typedef struct {
    const uint8_t* key;
    uint32_t key_len;
    const uint8_t* val;
    uint32_t val_len;
    int tomb;
    uint64_t seq;
    uint64_t crdt_ts;
} SegdbHistEntry;

/* Axis C — heap-overflow list node for history entries past
 * SEGDB_HIST_MAX. Doubly linked so tx rollback (pop from the
 * tail) is O(1); node memory is hot-arena bump waste until
 * the flush arena reset, exactly like a superseded key copy
 * that is logically dropped by a rollback. Nodes are NEVER
 * freed individually: they are logically unlinked on
 * rollback / flush-clear and their bytes die with the
 * arena reset. */
typedef struct SegdbHistOverflow {
    SegdbHistEntry entry;
    struct SegdbHistOverflow* prev;
    struct SegdbHistOverflow* next;
} SegdbHistOverflow;

typedef struct SegDB {
    YowzaDB hot;
    YowzaStore* store;
    Manifest manifest;
    uint64_t next_seg_id;
    int has_manifest;
    uint64_t seq_base; /* effective-seq offset for hot versions */
    uint8_t* wal_buf;  /* caller-owned, must outlive the SegDB */
    uint32_t wal_cap;
    void* arena_buf;   /* caller-owned, must outlive the SegDB */
    size_t arena_size;
    int recovered;     /* segdb_recover already ran on this SegDB */
    /* Track A publish state. indeterminate = 1 after a publish
     * attempt returned -2; has_pending + pending_manifest hold the
     * manifest that was being published (flush or compact, per
     * pending_is_compact) so segdb_reconcile can tell whether
     * CURRENT adopted it or still names the old generation. */
    int indeterminate;
    int has_pending;
    int pending_is_compact;
    Manifest pending_manifest;
    uint64_t pending_next_seg_id;
    /* Track A multi-level manifest: group manifest object names in
     * publication order (oldest first). db->manifest is always the
     * LAST group's manifest; earlier groups are loaded on demand
     * for get/scan/compact. group_count == 0 means the legacy
     * single-manifest layout (db->manifest is the whole DB). */
    char group_objs[MANIFEST_DIR_MAX_GROUPS][MANIFEST_MAX_OBJ_LEN + 1];
    uint32_t group_count;
    int has_dir;
    char dir_obj[MANIFEST_MAX_OBJ_LEN + 1];
    /* Track A pending directory state for segdb_reconcile:
     * pending_dir_transition: CURRENT -> "manifest.dir" pending
     *   (single->directory transition; old state is the single
     *   manifest named in pending_old_group_objs[0]).
     * pending_group_add: directory list replacement pending
     *   (group addition by flush, or group compact when
     *   pending_is_compact; new list in pending_group_objs,
     *   old list in pending_old_group_objs).
     * pending_group_append: last-group manifest rewrite pending
     *   (new in pending_group_manifest, old in
     *   pending_old_group_manifest). pending_group_manifest also
     *   holds the new LAST group manifest for add/transition, so
     *   reconcile can adopt it without re-reading the group. */
    int pending_dir_transition;
    int pending_group_add;
    int pending_group_append;
    char pending_group_objs[MANIFEST_DIR_MAX_GROUPS][MANIFEST_MAX_OBJ_LEN + 1];
    uint32_t pending_group_count;
    char pending_old_group_objs[MANIFEST_DIR_MAX_GROUPS][MANIFEST_MAX_OBJ_LEN + 1];
    uint32_t pending_old_group_count;
    Manifest pending_group_manifest;
    Manifest pending_old_group_manifest;
    uint8_t get_buf[4096]; /* segment-hit get values are copied here
     * (seg_lookup's val_buf); segdb values are <= 1024 bytes, so
     * this cap is not reachable through segdb_put; a larger foreign
     * segment value fails the get with -1 instead of overflowing. */
    /* GATE 1 — hot-layer history (see segdb.c "Gate 1" section).
     * One entry per superseded hot version: the value (or
     * tombstone) a key carried at an older effective sequence,
     * before a later segdb_put/segdb_delete overwrote it in hot.
     * Keys are arena copies (stable until the next flush, which
     * is exactly when history is transferred into segments);
     * values are arena pointers to the superseded value bytes
     * (bump arena: never reused before a flush). History is
     * cleared by the flush hot-clear, after its segments are
     * published. Axis C: hist[] holds the FIRST SEGDB_HIST_MAX
     * entries of the epoch in insertion order; further entries
     * spill into hist_overflow_* (insertion order continues
     * there). A put/delete fails with -3 only when the arena
     * cannot hold the key copy or the overflow node
     * (fail-closed: losing a version silently would corrupt
     * an old snapshot) — never merely because the epoch
     * passed 1024 entries.
     * RETENTION FLOOR: history is dropped by exactly one
     * operation — a successful flush, which transfers EVERY
     * entry (array + overflow) into history segments before
     * clearing. No put, delete, scan, or compact drops a
     * history entry, and compact defers while a snapshot
     * lease is active, so a version a live lease's snapshot
     * still needs is never reclaimed early. There is no
     * prune API at this layer: retention is "all of the
     * epoch, until its flush", and the Go spill log's
     * pin floor (spill_history.go) is the cross-epoch
     * retention authority above it. */
    SegdbHistEntry hist[SEGDB_HIST_MAX];
    uint32_t hist_count;
    SegdbHistOverflow* hist_overflow_head;
    SegdbHistOverflow* hist_overflow_tail;
    uint32_t hist_overflow_count;
    /* Axis C — pre-reserved overflow node for the in-flight
     * put/delete capture (see segdb_history_capture in
     * segdb.c): when the array is full, capture allocates
     * the node BEFORE the ART mutation, so the post-mutation
     * append cannot fail. NULL when no capture is reserved;
     * never a live history entry until linked by the commit
     * step. */
    SegdbHistOverflow* hist_reserved;
    /* GATE 1 — snapshot leases (see segdb.c "Gate 1" section).
     * An active lease pins one snapshot sequence: segdb_compact
     * defers (returns 0 without merging) while any lease with a
     * sequence below the current sequence is active, so a direct
     * compact call cannot destroy leased history. Lease ids
     * start at 1; 0 is never a valid id. */
    uint64_t lease_ids[SEGDB_MAX_LEASES];
    uint64_t lease_seqs[SEGDB_MAX_LEASES];
    uint8_t lease_active[SEGDB_MAX_LEASES];
    uint64_t next_lease_id;
} SegDB;

/* Init the hot DB on the memory WAL driver over wal_buf/wal_cap and
 * point at store. Does NOT recover: call segdb_recover explicitly on
 * a fresh SegDB over an existing store, or just start putting on a
 * virgin store. wal_buf/arena_buf are caller-owned and must stay
 * alive for the SegDB's lifetime (the hot WAL replay in recover
 * reads wal_buf). store is caller-owned (segdb never closes it). */
int segdb_init(SegDB* db, YowzaStore* store, void* arena_buf, size_t arena_size,
               uint8_t* wal_buf, uint32_t wal_cap);

/* Track A: same as segdb_init, but the hot WAL runs on a
 * caller-provided StorageDriver (e.g. the POSIX file driver) instead
 * of the memory driver, so unflushed hot data survives a full
 * process exit. The driver is caller-owned and must outlive the
 * SegDB; segdb_close closes it via the hot DB, exactly as for the
 * memory driver. Use this (with a durable driver) for the
 * persistent-WAL contract: Persistent WAL -> Recovery ->
 * Tombstone reconstruction -> Segment visibility. */
int segdb_init_with_driver(SegDB* db, YowzaStore* store, void* arena_buf,
                           size_t arena_size, StorageDriver drv);

/* Track A: reconcile after an indeterminate publish. Re-reads
 * CURRENT (and CURRENT.bak, see below) and adopts whichever
 * manifest is actually current: the pending one (hot is then reset
 * for a flush, or left untouched for a compact) or the old one
 * (hot untouched, publish confirmed not to have taken effect).
 * Returns 0 on reconciliation, -1 on a definite failure (state
 * stays indeterminate), -2 if still indeterminate. While
 * indeterminate, writes return SEGDB_INDETERMINATE (-2). */
int segdb_reconcile(SegDB* db);

/* Track A: 1 while an indeterminate publish is outstanding, else 0. */
int segdb_indeterminate(SegDB* db);

/* Writes go through the hot DB under the unified legacy contract
 * (vm.c: WAL, then ART, then sync; failures undo). Bounds are the
 * ART's: key_len 1..1023, val_len <= 1024, else -1.
 * segdb_delete differs from the VM-level absent delete on purpose:
 * a VM absent delete is a no-op (hot-ART-only semantics), but a
 * segdb delete of a hot-absent key MUST leave a hot tombstone, or
 * a segment value for that key would show through. segdb_delete
 * therefore creates the tombstone (insert-empty + delete, then one
 * legacy DEL record + sync, with restore-on-failure) when the key
 * is not live in hot; a key already tombstoned in hot is a no-op. */
int segdb_put(SegDB* db, const uint8_t* key, uint32_t key_len,
              const uint8_t* val, uint32_t val_len);
int segdb_delete(SegDB* db, const uint8_t* key, uint32_t key_len);

/* Get order: hot ART first — art_find_terminal distinguishes a hot
 * tombstone (terminal exists, !is_end, timestamp > 0: newest wins,
 * NOT FOUND, no fall-through) from absent (fall through); else a
 * live hot value wins; else segments newest-first (manifest order
 * reversed, seg_lookup each): the first hit wins, a tombstone hit
 * is NOT FOUND. *out points into hot arena storage (hot hit) or
 * into SegDB.get_buf (segment hit); in both cases it is valid only
 * until the next segdb_get/flush/compact/recover on this DB
 * (single-threaded invariant). Copy it if you need it longer.
 * Found-ness is the RETURN CODE, not pointer nullness: a live empty
 * value may legitimately return a pointer with len 0. */
int segdb_get(SegDB* db, const uint8_t* key, uint32_t key_len,
              uint8_t** out, uint32_t* out_len);

/* Merged scan: hot entries (art_iterate) + every segment iterator,
 * sorted by key, highest effective commit_seq per key wins, entries
 * with commit_seq > snapshot_seq dropped, tombstones dropped.
 * snapshot_seq == 0 means current (seq_base + hot commit_seq); a
 * snapshot_seq above current behaves as current. Prefix filter is a
 * byte-prefix match. Output is art_scan format per entry:
 * [key_len u32][key][val_len u32][val], concatenated. *count_out is
 * the entry count on success. Returns -1 if the merged result does
 * not fit out_size, if candidate caps are exceeded (see segdb.c),
 * or on any segment read error. A key written after the snapshot
 * sequence is invisible, including its tombstone: an older live
 * value for the same key IS visible at that snapshot. */
int segdb_scan(SegDB* db, const uint8_t* prefix, uint32_t prefix_len,
               uint64_t snapshot_seq, uint8_t* out_buffer, uint32_t out_size,
               uint32_t* count_out);

/* Current commit sequence: seq_base + hot commit_seq — exactly the
 * value segdb_scan treats as "current" when passed snapshot_seq 0.
 * A virgin DB reads 0; a flush preserves the sum (seq_base
 * advances by the old hot commit_seq while hot commit_seq resets
 * to 0), and every later committed put/delete advances it by one
 * (one whole transaction advances it by one, like art.h). The sum
 * never goes backward, so a sequence taken earlier stays a valid
 * snapshot argument for segdb_scan_page / segdb_scan (non-zero
 * sequences are literal there and in segdb_scan): segment entries
 * keep their effective sequences across flush, and hot entries
 * keep theirs until flushed. One caveat, shared with segdb_compact:
 * compacting with a tombstone barrier above an active snapshot
 * sequence can drop a tombstone that snapshot still needs, so the
 * caller must keep the barrier at or below the oldest active
 * snapshot (see segdb_compact). Returns 0 for a NULL db (no
 * sequence exists to report); callers with a live DB never see 0
 * except on a virgin DB, where 0 is the true current sequence. */
uint64_t segdb_snapshot_seq(SegDB* db);

/* GATE 1 — snapshot leases. segdb_lease_acquire registers seq
 * as a protected snapshot and returns a lease id (> 0), or 0 on
 * failure (NULL db, seq above the current sequence — a future
 * snapshot cannot be leased — or the lease table full).
 * segdb_lease_release deactivates a lease (0 ok, -1 unknown or
 * already released). segdb_lease_valid reports whether an id is
 * currently active. segdb_lease_oldest writes the smallest
 * sequence among active leases to *has_active (1) or 0 when no
 * lease is active (return value then 0). segdb_history_count
 * reports how many superseded hot versions are currently
 * recorded (diagnostic; 0 after a flush transferred them). */
uint64_t segdb_lease_acquire(SegDB* db, uint64_t seq);
int segdb_lease_release(SegDB* db, uint64_t id);
int segdb_lease_valid(SegDB* db, uint64_t id);
uint64_t segdb_lease_oldest(SegDB* db, int* has_active);
/* Axis C: segdb_history_count now reports the TOTAL epoch
 * history (array + overflow), not just the array chunk;
 * for epochs under SEGDB_HIST_MAX the two coincide, so
 * existing small-epoch assertions are unaffected. */
uint32_t segdb_history_count(SegDB* db);
uint32_t segdb_history_total(SegDB* db);
uint32_t segdb_history_overflow_count(SegDB* db);
/* Copy the idx-th history entry of the epoch (insertion
 * order: array first, then the overflow list) into *out.
 * Returns 0, or -1 on a bad argument / idx out of range.
 * The returned key/val pointers are arena pointers valid
 * until the next flush, like every history entry. */
int segdb_history_at(SegDB* db, uint32_t idx, SegdbHistEntry* out);

/* Streaming page scan: the same merged scan as segdb_scan, but in
 * pages, without the total-candidate cap. snapshot_seq is LITERAL
 * here (unlike segdb_scan): entries with commit_seq >
 * snapshot_seq are invisible, including their tombstones, and an
 * older live value for the same key IS visible; snapshot_seq == 0
 * therefore yields no rows (no terminal carries version 0) — pass
 * segdb_snapshot_seq(db) for "current". skip counts LIVE merged
 * rows only: tombstone winners and shadowed versions do not
 * consume skip. At most limit live rows are written, in key
 * order, in art_scan format. The call walks the merged sources
 * from the start and skips `skip` rows, so paging costs O(total
 * rows) per page — memory stays bounded (one segment heap plus
 * hot candidates) while time is O(rows * pages); page only to
 * bound memory, and prefer a large limit when latency matters.
 * Returns 0 on success (a page shorter than limit, or an empty
 * page, means end of input), -1 on any scan error (hot candidate
 * overflow, more than 128 segment sources, segment read error,
 * bad argument), -3 when the next row does not fit out_size:
 * *count_out then holds the rows already written by this call,
 * and the caller advances skip by *count_out and retries with a
 * larger buffer (a single row needs at most 4+1023+4+1024 bytes,
 * so a buffer at least that large always makes progress).
 * Remaining caps, stated honestly: hot candidates are still
 * collected (SEGDB_SCAN_MAX_CANDS entries, SEGDB_SCAN_DATA key
 * bytes) and segment sources are capped at 128 (the streaming
 * compact single-pass bound — compact first when a DB holds more
 * segments); segment rows themselves are never collected, so the
 * old 8192-total-candidate cap no longer bounds a scan. */
int segdb_scan_page(SegDB* db, const uint8_t* prefix, uint32_t prefix_len,
                    uint64_t snapshot_seq, uint64_t skip, uint32_t limit,
                    uint8_t* out_buffer, uint32_t out_size,
                    uint32_t* count_out);

/* Cursor (continuation-key) scan — Reliability stage item 03,
 * Layer 2. The same merged, snapshot-literal scan as
 * segdb_scan_page, but resumed by an exclusive continuation key
 * instead of a skip count: rows returned are exactly the live
 * merged rows with key strictly greater than cursor_key (when
 * has_cursor is set; otherwise all rows), in key order, at
 * most limit rows, in art_scan format. Each source is SEEKED
 * to the cursor via the hot ART ordering and the segment
 * sparse indexes (see segdb.c), so a page visits O(page rows
 * + blocks) segment entries instead of re-walking O(total
 * rows) like segdb_scan_page does. segdb_scan_page is
 * UNCHANGED; this is the path new pagers should use.
 * On success, *count_out rows were written; *next_len_out is
 * the length of the last written row's key in next_key (or the
 * input cursor, when no row was written and has_cursor is
 * set, or 0 when no row was written and no cursor was given),
 * and *has_more_out is 1 iff at least one more live row
 * exists past the last written row (determined by probing one
 * row past the page). Resume while *has_more_out is set, with
 * cursor_key = next_key. A page shorter than limit with
 * *has_more_out == 0 means end of input. Returns -1 on any
 * scan error (*count_out then holds rows already written by
 * this call), -3 when the next row does not fit out_size
 * (*count_out holds rows already written; resume with the
 * last written key, or the input cursor when nothing was
 * written, and retry with a larger buffer; a buffer at least
 * 4+1023+4+1024 bytes always makes progress). next_key_cap
 * should be at least 1024 (the largest key the scan can
 * return); if the resume key does not fit it, *next_len_out
 * is left 0. Same source
 * caps as segdb_scan_page (hot candidates, 128 segment
 * sources); segment rows themselves are never collected. */
int segdb_scan_cursor(SegDB* db, const uint8_t* prefix, uint32_t prefix_len,
                      uint64_t snapshot_seq,
                      const uint8_t* cursor_key, uint32_t cursor_len, int has_cursor,
                      uint32_t limit,
                      uint8_t* out_buffer, uint32_t out_size,
                      uint32_t* count_out,
                      uint8_t* next_key, uint32_t next_key_cap, uint32_t* next_len_out,
                      uint32_t* has_more_out);

/* Scan work counters (Reliability stage item 03): page_visits
 * counts segment entries returned by seg_iter_next inside the
 * old skip path; cursor_visits the same inside the cursor
 * path; cursor_block_skips counts entries skipped by the
 * cursor prime's in-memory block parse (one already-read
 * block per segment per page — cheap, and reported separately
 * so it is never conflated with iterator returns);
 * seek_index_entries counts sparse-index entries examined
 * while positioning cursor sources; cache_hits / cache_misses
 * count segment-discovery cache outcomes on the cursor path.
 * Reset before a measurement, read after. */
typedef struct {
    uint64_t page_visits;
    uint64_t cursor_visits;
    uint64_t cursor_block_skips;
    uint64_t seek_index_entries;
    uint64_t cache_hits;
    uint64_t cache_misses;
} SegdbScanStats;
void segdb_scan_stats_reset(void);
void segdb_scan_stats_get(SegdbScanStats* out);

/* Track C paged-prime counters (see segdb.c): page probes spent
 * by cur_seek_block's paged path and index entries examined in
 * its within-page scans. Reset before a measurement, read
 * after. These are separate from SegdbScanStats so the stats
 * struct layout (read by the wasm scan-stats export) is
 * untouched. */
void segdb_paged_stats_reset(void);
void segdb_paged_stats_get(uint64_t* pages_out, uint64_t* within_out);

/* Flush, EXACT order: Freeze (single-threaded invariant: no new hot
 * writes during flush), collect hot entries via art_iterate,
 * seg_write to "seg.<id>" (index_type 0, model_version 0, bloom on),
 * verify by seg_open + full discovery + entry count + first/last
 * key check, sync_obj, fill SegMeta.checksum via seg_file_checksum,
 * append the segment to a manifest with generation+1,
 * manifest_write to "manifest.<gen>.tmp", manifest_publish to
 * "manifest.<gen>", write CURRENT via manifest_write_current to
 * "CURRENT.tmp" + manifest_publish to "CURRENT", sync_dir, advance
 * checkpoint_seq to the hot commit_seq (effective), THEN reset the
 * hot arena + clear the hot WAL. On any failure before CURRENT
 * publish, the old manifest stays current and the new segment
 * object is orphaned (documented: reclaimed by later compaction,
 * never referenced). A flush with an empty hot is a no-op success.
 * Flush fails (-1, hot untouched, manifest untouched) if the hot
 * WAL driver cannot truncate — checked BEFORE any store write, so
 * a flush is never reported failed after its manifest became
 * current for this reason; the in-memory manifest is only adopted
 * after CURRENT is durable. */
int segdb_flush(SegDB* db);

/* Compact SEGMENTS ONLY (hot stays untouched). Merge all manifest
 * segments into new segments (one per 4096-entry chunk), keeping
 * the newest entry per key, dropping a tombstone ONLY if its
 * commit_seq <= drop_tombstones_below_seq. GATE 1 LEASE RULE
 * (supersedes the old caller-contract sentence): the merge
 * unconditionally drops non-newest versions, so no barrier value
 * can make it safe for a stale snapshot — therefore, while any
 * snapshot lease with a sequence below the current sequence is
 * active, segdb_compact DEFERS: it returns 0 without merging
 * anything (a successful no-op; retry after the leases close).
 * When every active lease is at the current sequence, the merge
 * preserves that sequence's view (newest per key) and proceeds
 * normally. This check lives inside segdb_compact, so a direct
 * compact call cannot accidentally invalidate an active lease.
 * New segments are published exactly like flush (new manifest,
 * generation+1, CURRENT, sync_dir); old segment objects are removed
 * ONLY after the new CURRENT is durable. A remove failure after
 * CURRENT is durable returns -1 but the new manifest IS current
 * (state is new-valid; leftovers are orphans). */
int segdb_compact(SegDB* db, uint64_t drop_tombstones_below_seq);

/* Recover over an existing store. Read CURRENT, manifest_read it;
 * if the manifest object CURRENT names is corrupt/missing, try
 * "manifest.<gen-1>" parsed from CURRENT's generation if available,
 * else fail explicitly (-1). A CURRENT object that exists but is
 * unparsable also fails (-1). A segment listed in the adopted
 * manifest that fails verification (seg_open / full discovery /
 * entry count / first-last check / SegMeta.checksum mismatch
 * against seg_file_checksum) fails recovery explicitly. EXCEPTION —
 * virgin store, now closed by the Gate 2 helpers (O2): pointer
 * resolution goes through manifest_recover_pointer, so recovery
 * succeeds with an empty manifest ONLY when the store is
 * authoritatively empty (both pointers absent AND enumeration
 * proves no objects at all). Both pointers gone with any
 * surviving object (e.g. a segment) fails recovery as
 * ambiguous/corrupt, never opens virgin — this supersedes
 * the old Track A double-loss behaviour. The old pointerless
 * manifest.1 fallback is removed for the same reason (it
 * could adopt a stale first generation); the one pointerless
 * salvage kept is a surviving manifest.dir object, which
 * names every group and is fully verified, and a corrupt
 * directory still fails outright. A pointer naming
 * manifest.dir is dispatched to directory recovery before
 * manifest_recover_manifest is called, because that
 * helper's gen-1 fallback covers only manifest.<gen>
 * names; group manifests get no gen-1 fallback. I/O
 * errors and unknown backend status never become
 * NOT_FOUND: they fail recovery. Hot WAL replay is Phase 1
 * tx_replay_wal over wal_buf: it rebuilds
 * hot entries written since the last flush, but it CANNOT filter by
 * sequence in this phase (no sequence filtering exists on the WAL
 * driver), so replayed entries get rebuild-order versions, not
 * their original effective sequences. The correctness argument for
 * the normal case is the flush invariant: post-flush WAL is empty
 * by construction (flush clears it only after CURRENT is durable),
 * so a restarted DB after a clean flush replays nothing and its
 * checkpoint_seq is exact. Recover may run only once per SegDB
 * (second call returns -1) and only on a fresh hot DB. */
int segdb_recover(SegDB* db);

/* Extra (not in the assignment's list, added for resource hygiene):
 * release the hot memory-WAL driver pool slot. Does NOT close
 * db->store (caller-owned, shared across SegDBs over the same
 * store). Call it when a SegDB is done, especially in tests that
 * create many SegDBs (the memory driver pool holds 16 drivers). */
void segdb_close(SegDB* db);

#endif /* YOWZA_SEGDB_H */
