#ifndef YOWZA_TX_H
#define YOWZA_TX_H

#include "art.h"
#include "storage.h"

/* Phase 1 — Transactional VM + Change Fabric (see PHASE1_NOTES.md for
 * the exact binary layouts; this header is the API, the note is the
 * contract, and neither may drift from the other).
 *
 * Model, in one paragraph: a transaction buffers its writes in a
 * PRIVATE write set and never touches the live ART until commit.
 * Commit runs validation, then applies the write set to the live
 * tree (with a per-key undo record, so a mid-apply OOM restores the
 * logical state), then appends ONE WAL transaction frame and syncs.
 * If the frame fails, the apply is undone and the frame is truncated
 * where the driver supports it. Abort discards the write set without
 * ever having touched the live tree. Rollback is therefore NEVER a
 * mere arena/wal offset rewind over mutated live nodes: during a
 * transaction, tx_put/tx_get/tx_assert allocate only write-set scratch
 * after the begin checkpoint, and abort rewinds ONLY that scratch,
 * which contains no live node and no free-list entry. That invariant
 * is what makes the abort rewind safe, and it is enforced by the
 * rule that no BUFFERING tx_* function (tx_put/tx_del/tx_get/
 * tx_assert) calls art_insert/art_delete/add_child. The live tree
 * is touched only by two named exceptions: tx_commit's apply phase
 * (which carries a per-key undo record, and which drives the
 * history-capture hook below before applying anything) and
 * tx_replay_wal (crash recovery, where no live snapshot exists).
 * The bare header sentence "no tx_* function calls art_insert" in
 * older notes is therefore imprecise: the enforced rule is about
 * buffering, and the apply phase is the documented exception.
 *
 * PHASE C UNIFIED DELETE (see tx.c): a tx DELETE of a
 * hot-absent key (no terminal, or an intermediate-only
 * node) creates a SHADOWING TOMBSTONE in tx_apply and
 * in frame replay (art_insert of an empty value, then
 * art_delete), exactly as segdb_delete_absent does for
 * the SegDB facade. Without it, a tx delete of a
 * segment-only key left no hot tombstone and the old
 * segment value resurfaced at new snapshots — the one
 * tx/SegDB semantic gap that remained after the
 * history-hook integration. An already-tombstoned
 * terminal already shadows and is left alone. Legacy
 * per-op records in tx_replay_wal are the one
 * exception: they keep the old no-op delete (they
 * predate frames; at the SegDB level their delete
 * path is segdb_delete_absent's own legacy record).
 * Direct art_insert/art_delete callers are LOW-LEVEL
 * structural primitives, not a database API: they
 * carry no history, snapshot, or WAL guarantee. Any
 * caller that needs those guarantees must go through
 * the SegDB facade (segdb_put/segdb_delete) or the
 * Go SegDBTransaction layer, never raw ART calls.
 */

/* Error codes (shared with the VM: negative values are VM error codes). */
#define TX_OK            0
#define TX_ERR_OOM      -3
#define TX_ERR_MALFORMED -5
#define TX_ERR_WAL      -6
#define TX_ERR_PROTOCOL -7  /* BEGIN twice, COMMIT without BEGIN, legacy op inside tx, ... */
#define TX_ERR_CONFLICT -8  /* concurrent-write / snapshot-version conflict */
#define TX_ERR_ASSERT   -9  /* uniqueness or ASSERT failure at validation */

/* Write-set entry kinds (mirror the VM opcodes for PUT/DEL). */
#define TX_OP_PUT 0x01
#define TX_OP_DEL 0x03

/* tx_put flags. */
#define TX_WRITE_UNIQUE 0x01  /* fail validation if the key exists in committed state */

/* ASSERT kinds. */
#define TX_ASSERT_ABSENT 0
#define TX_ASSERT_PRESENT 1
#define TX_ASSERT_VALUE_EQUALS 2

/* Consistency models recorded in the frame (Phase 1 records, does not
 * enforce, the distinction; enforcement arrives with the distributed
 * layers). */
#define TX_CONSISTENCY_LOCAL 0
#define TX_CONSISTENCY_CONSENSUS 1

#define YOWZA_TX_MAX_WRITES 128
#define YOWZA_TX_MAX_READS 128
#define YOWZA_TX_MAX_ASSERTS 32

/* Change Fabric envelope, wire format version 1. Layout (all
 * little-endian, see PHASE1_NOTES.md):
 *   format_version u32 (=1), transaction_id u64, operation_id u64,
 *   origin_node_id u64, logical_sequence u64, operation_type u32,
 *   schema_version u32, consistency_model u32, timestamp u64,
 *   causal_dep_count u32, causal deps (dep_count x u64),
 *   payload_checksum u64 (FNV-1a over envelope fields before the
 *   checksum, then key_len u32 + key + val_len u32 + val).
 * Phase 1 writes causal_dep_count = 0; parsers must accept any count. */
#define YOWZA_FABRIC_FORMAT_VERSION 1
#define YOWZA_FABRIC_ENVELOPE_FIXED 68 /* 60 bytes of fields before deps + 8-byte checksum; deps add 8 each */
#define YOWZA_TX_FRAME_MARKER 0xF0
#define YOWZA_TX_FRAME_COMMIT_MARKER 0xC1

typedef struct {
    uint8_t op; /* TX_OP_PUT or TX_OP_DEL */
    uint8_t flags;
    const uint8_t* key;
    uint32_t key_len;
    const uint8_t* val; /* NULL for DEL */
    uint32_t val_len;
    uint64_t timestamp; /* CRDT timestamp for PUT, else 0 */
} TxWrite;

typedef struct {
    const uint8_t* key;
    uint32_t key_len;
    uint64_t version; /* live version recorded at read time */
    int found;        /* live is_end at read time */
} TxRead;

typedef struct {
    uint8_t kind; /* TX_ASSERT_* */
    const uint8_t* key;
    uint32_t key_len;
    const uint8_t* val; /* only for VALUE_EQUALS */
    uint32_t val_len;
} TxAssert;

/* TX + HOT HISTORY INTEGRATION: history-capture hook.
 * The owner of the ART's snapshot layer (e.g. a SegDB adapter that
 * appends to its hot-history table) sets these on the TxState
 * BEFORE tx_begin. tx_commit calls history_capture once per
 * distinct write key whose committed terminal is live or
 * tombstoned, BEFORE any apply, with that terminal's pre-tx value
 * (val NULL / val_len 0 and tomb = 1 for a tombstone) and its
 * ART-local version (old_version). A layer with an effective-
 * sequence offset (SegDB: seq_base) adds its offset inside the
 * callback; the tx layer itself knows only ART-local versions.
 * Keys with no terminal, or with an intermediate-only node
 * (not live, timestamp 0), are never captured, exactly like the
 * segdb_put/segdb_delete capture rule. A nonzero capture return
 * fails the commit with TX_ERR_OOM before anything is applied;
 * captures already made for that commit are removed through
 * history_rollback(ctx, count) when set, as are the captures of
 * a commit whose apply or WAL frame later fails. The hook pair
 * is configuration, not transaction state: tx_clear (commit /
 * abort) preserves it, and only tx_init_state clears it, so the
 * owner sets it once per TxState lifetime (e.g. right after the
 * DB/SegDB init that creates the TxState). NULL capture means
 * no history: the pre-integration behaviour (direct-ART writes
 * bypass history). A capture without a rollback still fails
 * closed (no apply on capture failure), but the no-leftover
 * guarantee then rests on the hook owner: set both together
 * for exact history consistency. */
typedef int (*TxHistoryCaptureFn)(void* ctx, const uint8_t* key, uint32_t key_len,
                                  uint64_t old_version, int tomb,
                                  const uint8_t* val, uint32_t val_len,
                                  uint64_t crdt_ts);
typedef int (*TxHistoryRollbackFn)(void* ctx, uint32_t count);

typedef struct {
    int active;
    uint64_t tx_id;
    uint64_t origin_node_id;
    uint64_t snapshot_seq; /* art->commit_seq at BEGIN */
    uint64_t schema_version;
    uint32_t consistency_model;
    TxWrite writes[YOWZA_TX_MAX_WRITES];
    uint32_t n_writes;
    TxRead reads[YOWZA_TX_MAX_READS];
    uint32_t n_reads;
    TxAssert asserts[YOWZA_TX_MAX_ASSERTS];
    uint32_t n_asserts;
    /* Arena offset at BEGIN. Only write/read/assert scratch is
     * allocated after this point while the tx is active, so abort
     * may rewind to it (see the header comment for the invariant). */
    size_t arena_checkpoint;
    uint64_t next_op_id; /* starts at 1 */
    TxHistoryCaptureFn history_capture;
    void* history_ctx;
    TxHistoryRollbackFn history_rollback;
} TxState;

void tx_init_state(TxState* tx);

int tx_begin(TxState* tx, ART* art, uint64_t tx_id, uint64_t origin_node_id,
             uint32_t consistency_model, uint64_t schema_version);
int tx_put(TxState* tx, ART* art, const uint8_t* key, uint32_t key_len,
           const uint8_t* val, uint32_t val_len, uint8_t flags, uint64_t timestamp);
int tx_del(TxState* tx, ART* art, const uint8_t* key, uint32_t key_len);
/* Returns 0 if found (out_val/out_val_len set), -1 if not found,
 * negative error otherwise. Records the read for validation. */
int tx_get(TxState* tx, ART* art, const uint8_t* key, uint32_t key_len,
           uint8_t** out_val, uint32_t* out_val_len);
int tx_assert(TxState* tx, ART* art, uint8_t kind, const uint8_t* key, uint32_t key_len,
              const uint8_t* val, uint32_t val_len);
int tx_commit(TxState* tx, ART* art, StorageDriver* storage);
int tx_abort(TxState* tx, ART* art);

/* WAL replay (crash recovery). Parses a WAL byte stream containing
 * legacy per-op records and/or Phase 1 transaction frames, in order.
 * A complete, checksum-valid transaction frame is applied atomically;
 * an incomplete or corrupt frame at the tail (or anywhere) stops the
 * replay and is NOT applied, and neither is anything after it.
 * Legacy records are applied one by one (they predate frames).
 * Returns 0 and sets *out_applied_tx to the number of frames applied
 * (legacy records do not count). Returns -1 only on a NULL argument. */
int tx_replay_wal(ART* art, const uint8_t* wal, uint32_t wal_len, uint32_t* out_applied_tx);

/* FNV-1a 64 helpers, shared by frame/envelope checksums. */
uint64_t tx_fnv1a_init(void);
uint64_t tx_fnv1a_update(uint64_t h, const uint8_t* data, uint32_t len);

#endif /* YOWZA_TX_H */
