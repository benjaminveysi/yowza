/* Gate 3 — fan-in-bounded scan (segdb_scan.c).
 *
 * segdb_scan_page / segdb_scan_cursor in segdb.c are UNCHANGED and
 * cap segment sources at 128: a DB with more segments fails those
 * paths with -1 before reading any row. This file is the bounded
 * alternative, written as NEW functions only.
 *
 * Design (windowed, forward-resume seek — NOT a bigger static
 * source array):
 *  - Sources are enumerated once per call from the manifest
 *    groups (oldest first). The per-source metadata kept for the
 *    whole call is small and structural: object name, first/last
 *    keys, sparse-index offset/length, block/entry counts, min/max
 *    sequences, plus a resume position inside the sparse index.
 *    The hard structural maximum is 32 groups x 128 segments =
 *    4096 sources, so the metadata array is sized at that bound;
 *    it holds NO row data and does not grow with row count.
 *  - Rows are collected one WINDOW at a time (at most
 *    SEGDB_FANIN_WINDOW_KEYS distinct keys). A window's winner map
 *    holds only key bytes + (winning source, sequence, tombstone)
 *    in phase 1; in phase 2 each source with winners in the window
 *    is streamed forward exactly once and every winner value is
 *    copied into a 256 KiB value arena (provably sufficient: at
 *    most WINDOW_KEYS winners x 1024 bytes, the segdb_put cap).
 *    Values are NOT fetched per row with seg_lookup: its sparse-
 *    index scan is linear and it re-verifies a whole block's
 *    checksum per row, which made a 900,000-row wasm scan take
 *    >11 min on its first pass alone. Streaming loads each block
 *    once per window per source instead.
 *  - Each source is visited once per window, in publication order
 *    (oldest first, so a later source wins a sequence tie, and hot
 *    wins over every segment on a tie). A source is SEEKED to the
 *    window's lower bound: its sparse index is scanned forward
 *    from the previous window's resume position (window bounds only
 *    move forward, so total index work per source over a whole scan
 *    is O(blocks + windows), never O(blocks x windows)), then
 *    seg_iter_next streams only that window's entries. A source
 *    whose last key is at or below the window bound is marked done
 *    and never visited again; a source whose minimum sequence is
 *    above the snapshot contributes nothing at this snapshot and
 *    is skipped for the whole call.
 *  - The winner map keeps the SMALLEST window keys seen so far:
 *    inserting a key larger than the current map maximum while the
 *    map is full stops that source (its later keys are larger
 *    still, being sorted); inserting a smaller key evicts the
 *    maximum. A source stopped this way has examined every one of
 *    its keys at or below the window's final maximum, so no winner
 *    below the maximum is ever missed, and the window's maximum is
 *    a real key that strictly advances the next window's bound —
 *    progress is guaranteed even when a whole window is tombstones.
 *  - Hot entries are collected once per call (bounded by the hot
 *    set, same as the cursor path), sorted, and merged per window.
 *    Segment winner per key = highest commit_seq <= snapshot_seq;
 *    newer versions never shadow an older visible version; a
 *    tombstone winner drops the key from the output but still
 *    occupies its window slot (it is a real distinct key).
 *  - Memory per call is therefore O(sources metadata) +
 *    O(window keys) + O(hot) + O(1) open reader, independent of
 *    total rows and of segment count beyond the structural 4096.
 *    Time per call is O(total entry versions examined once per
 *    window they fall in) + O(windows x live sources) seeks.
 *
 * Return codes (typed, never partial-apparent-success):
 *  0  ok (see *count_out / *has_more_out).
 *  -1 generic scan error (segment read/corruption, source
 *     enumeration failure, hot overflow, key-arena exhaustion):
 *     *count_out holds rows already written by THIS call, and the
 *     caller must NOT treat them as a complete page — the scan
 *     failed. (Same convention as segdb_scan_cursor.)
 *  -3 next row does not fit out_size: *count_out holds rows
 *     already written; resume with the last written key (or the
 *     input cursor when nothing was written) and a larger buffer.
 *     A buffer of at least 4+1023+4+1024 bytes always makes
 *     progress.
 *  -4 invalid argument: NULL db/buffers/cells, prefix_len > 1023,
 *     limit == 0, has_cursor with cursor_len == 0 or > 1023, or a
 *     cursor supplied without has_cursor. No row is written and
 *     *count_out is 0. In particular an invalid continuation key
 *     fails here, before any source is read, so it can never yield
 *     partial apparently-successful rows.
 * On success *next_len_out is the last written row's key length
 * in next_key (or the input cursor length when no row was written
 * and has_cursor is set, or 0 when no row was written and no
 * cursor was given), and *has_more_out is 1 iff at least one more
 * live row exists past the last written row. Resume while
 * *has_more_out is set, with cursor_key = next_key. A page
 * shorter than limit with *has_more_out == 0 means end of input.
 * snapshot_seq is LITERAL (as in segdb_scan_page/cursor):
 * snapshot_seq == 0 yields no rows; pass
 * segdb_snapshot_seq(db) for "current".
 *
 * Cross-call discovery cache (O3; page calls with limit <=
 * SEGDB_FANIN_CACHE_MAX_LIMIT only — larger calls stay
 * stateless, exactly as before):
 *  - One cache slot, keyed by (SegDB pointer, prefix, snapshot).
 *    A cursorless page always discovers fresh and replaces the
 *    slot (it is a new scan, never a stale error); a page with a
 *    cursor and no matching slot also discovers fresh (e.g. the
 *    first resume after an eviction). A page with a cursor and a
 *    matching slot validates before returning any row.
 *  - Validation: a generation fingerprint is recomputed from
 *    current engine state — manifest generations, checkpoint
 *    sequences, group list, seq_base, and per segment (name,
 *    whole-file checksum from SegMeta.checksum, entry count,
 *    min/max sequences, first/last keys). It is a real mutation
 *    fingerprint, never wall-clock time and never a caller-side
 *    guess. On a fingerprint match the cached sources are reused
 *    after re-verifying each non-skipped source: seg_open
 *    (header/footer) plus an FNV-1a recomputation over the
 *    sparse-index bytes against the footer index checksum, the
 *    same check the cursor/point-lookup discovery cache uses.
 *    Block checksums are NOT cached: every block streamed by a
 *    cached page is still checksum-verified by seg_iter_next
 *    before any entry is returned, exactly as on the stateless
 *    path. A verification failure returns -1 and kills the slot.
 *  - Invalidation rule (the whole proof): a fingerprint change
 *    is transparently re-discovered and the page continues ONLY
 *    when the current segment list has the cached list as an
 *    exact prefix (same name/checksum/first/last/min/max/count,
 *    in order, plus appended segments) — i.e. a flush-append,
 *    which provably preserves the snapshot view: flushed entries
 *    keep their effective sequences, so entries visible at the
 *    session snapshot are the same before and after, and
 *    appended entries newer than the snapshot stay invisible.
 *    Every other change (compact replaces segments under new
 *    names and drops non-newest versions, so an old snapshot's
 *    rows are not provably preserved) returns -5
 *    (SEGDB_FANIN_STALE) and kills the slot; the caller must
 *    start a new scan. Rows are never mixed across source sets:
 *    a page either runs entirely on the cached set, entirely on
 *    a freshly re-discovered flush-appended set, or fails.
 *    Hot entries are NOT cached: they are re-collected from the
 *    live ART on every page call (snapshot filtering makes
 *    post-snapshot hot changes invisible, and a flush moves hot
 *    entries to segments with identical effective sequences).
 *  - Per-source resume positions (sparse-index position, done
 *    flags) persist across cached pages, so a seek resumes from
 *    the cached block instead of the index start; a peek past
 *    the page limit never persists its positions/done flags
 *    (they are restored after the peek), so a peeked source is
 *    never wrongly marked done for the next page.
 *  - Bounds: one slot, at most SEGDB_FANIN_MAX_SOURCES sources
 *    and FN_NAMES_ARENA names bytes — the same caps as a single
 *    stateless call, never growing with page count. Over-cap
 *    discovery fails with -1, exactly as before; no session is
 *    stored. A different scan (different db/prefix/snapshot)
 *    evicts the slot and discovers fresh: the cost is paid
 *    again, correctness never depends on the slot.
 *  - Stats on cached pages: g_fn_stat_sources counts DISCOVERED
 *    sources only, so a cache-hit page adds 0 while a
 *    stateless page adds the full source count; windows, seeks
 *    (one positioning per source per window, as before) and
 *    streamed entries are counted identically on both paths.
 */
#ifndef YOWZA_SEGDB_SCAN_H
#define YOWZA_SEGDB_SCAN_H

#include "segdb.h"

#define SEGDB_FANIN_OK 0
#define SEGDB_FANIN_ERR (-1)
#define SEGDB_FANIN_NOSPACE (-3)
#define SEGDB_FANIN_INVAL (-4)
/* Cross-call discovery cache (O3): a page call whose source set
 * was replaced by anything other than a flush-append (compact,
 * recovery to a different manifest, ...) fails with this code
 * instead of returning rows from a mixed source set. See the
 * cache contract below. */
#define SEGDB_FANIN_STALE (-5)
/* Page calls with limit at or below this bound use the
 * cross-call discovery cache; larger calls (full-scan streaming
 * with a huge limit) use the original stateless path, so their
 * stats invariant (sources == segments x calls) is preserved. */
#define SEGDB_FANIN_CACHE_MAX_LIMIT 4096u

/* At most this many distinct keys per window. */
#define SEGDB_FANIN_WINDOW_KEYS 256u
/* Structural maximum segment count: 32 directory groups of at
 * most 128 segments each (see ManifestDir / MANIFEST_MAX_SEGMENTS). */
#define SEGDB_FANIN_MAX_SOURCES 4096u

int segdb_scan_fanin(SegDB* db, const uint8_t* prefix, uint32_t prefix_len,
                     uint64_t snapshot_seq,
                     const uint8_t* cursor_key, uint32_t cursor_len, int has_cursor,
                     uint32_t limit,
                     uint8_t* out_buffer, uint32_t out_size,
                     uint32_t* count_out,
                     uint8_t* next_key, uint32_t next_key_cap, uint32_t* next_len_out,
                     uint32_t* has_more_out);

/* Cumulative counters since the last reset (reset before a
 * measurement, read after): sources enumerated, windows collected,
 * per-source seeks, segment entries examined by seg_iter_next. */
void segdb_scan_fanin_stats_reset(void);
void segdb_scan_fanin_stats_get(uint64_t* sources_out, uint64_t* windows_out,
                                uint64_t* seeks_out, uint64_t* entries_out);

/* Track C — hierarchical fan-in scan (segdb_scan.c, "Track C"
 * section). Same page protocol and return codes as
 * segdb_scan_fanin (0 ok, -1 scan error, -3 next row does
 * not fit out_size, -4 invalid argument), but the source
 * table holds ONE manifest group at a time (<= 128
 * sources), so the 4096 flat cap becomes a per-level cap
 * and a DB with up to 64 groups x 128 segments = 8192
 * sources scans completely. Winner semantics are identical
 * to the flat path (highest commit_seq <= snapshot wins;
 * later publication ordinal wins a tie; hot merges the
 * same way; tombstone winners drop the key). The path is
 * STATELESS across page calls (no discovery cache, hence
 * no -5): each page call re-enumerates from its cursor,
 * so a flush/compact between pages is simply seen by the
 * next page from the cursor — callers must not resume a
 * hier scan across a compact expecting snapshot stability
 * beyond the literal snapshot_seq (same caveat as the flat
 * path's large-limit stateless calls). Memory per call is
 * bounded independent of total sources/rows (one chunk's
 * metadata + 256-key window + 256 KiB value arena + hot +
 * the fixed 8192-entry resume table). */
int segdb_scan_fanin_hier(SegDB* db, const uint8_t* prefix,
                          uint32_t prefix_len, uint64_t snapshot_seq,
                          const uint8_t* cursor_key, uint32_t cursor_len,
                          int has_cursor, uint32_t limit,
                          uint8_t* out_buffer, uint32_t out_size,
                          uint32_t* count_out,
                          uint8_t* next_key, uint32_t next_key_cap,
                          uint32_t* next_len_out,
                          uint32_t* has_more_out);

/* Hierarchical-scan stats for the LAST page call (reset
 * before a measurement, read after): groups enumerated in
 * the call's first window (the level count the scan
 * actually walked), sources enumerated in the first window
 * (the total source count the scan covers), the highest
 * group index visited (max_group == group count - 1 proves
 * the last level was reached), and windows collected. */
void segdb_scan_fanin_hier_stats_reset(void);
void segdb_scan_fanin_hier_stats_get(uint64_t* groups_out,
                                     uint64_t* sources_out,
                                     uint64_t* max_group_out,
                                     uint64_t* windows_out);

#endif /* YOWZA_SEGDB_SCAN_H */
