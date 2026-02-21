#include "tx.h"

/* Phase 1 transactional core. See include/tx.h for the model and
 * PHASE1_NOTES.md for the exact wire layouts and the rollback
 * invariant. This file deliberately never calls art_insert /
 * art_delete while a transaction is being BUFFERED (tx_put/tx_del/
 * tx_get/tx_assert only copy scratch into the arena); the live tree
 * is touched only inside tx_commit's apply phase, which carries a
 * per-key undo record, and inside tx_replay_wal. TX + HOT HISTORY
 * INTEGRATION: tx_commit's apply phase is preceded by a
 * fail-closed history-capture phase (see tx_commit step 2): when
 * TxState.history_capture is set, every superseded committed
 * version is captured through the hook BEFORE any apply, exactly
 * like segdb_put/segdb_delete capture before mutation, and a
 * capture failure, a mid-apply failure, or a WAL-frame failure
 * removes the captured entries again via history_rollback, so a
 * failed commit leaves terminals AND history as before.
 * PHASE C UNIFIED DELETE: a tx DELETE of a hot-absent key
 * (no terminal, or an intermediate-only node) no longer
 * evaporates into an art_delete no-op. tx_apply and the
 * frame-replay path both create a SHADOWING TOMBSTONE
 * (art_insert of an empty value, then art_delete), exactly
 * as segdb_delete_absent does for the SegDB facade — so a
 * segment-only key deleted by a tx stays deleted at new
 * snapshots (the tombstone, stamped with the apply
 * sequence, shadows the segment value) while old
 * snapshots still read the segment value (the tombstone
 * version is newer than their sequence). The undo
 * record captured BEFORE the apply restores the
 * pre-apply terminal on any later failure, so a failed
 * tx leaves no stray tombstone behind. A terminal that
 * is already a tombstone (is_end == 0, timestamp > 0)
 * already shadows and is left alone. Legacy per-op
 * records in tx_replay_wal keep their old no-op delete:
 * they predate frames, and at the SegDB level their
 * delete path is segdb_delete_absent's own legacy
 * record, not this interpreter (see tx.h). */

uint64_t tx_fnv1a_init(void) { return 14695981039346656037ULL; }

uint64_t tx_fnv1a_update(uint64_t h, const uint8_t* data, uint32_t len) {
    for (uint32_t i = 0; i < len; i++) {
        h ^= data[i];
        h *= 1099511628211ULL;
    }
    return h;
}

void tx_init_state(TxState* tx) {
    if (!tx) return;
    tx->active = 0;
    tx->tx_id = 0;
    tx->origin_node_id = 0;
    tx->snapshot_seq = 0;
    tx->schema_version = 0;
    tx->consistency_model = 0;
    tx->n_writes = 0;
    tx->n_reads = 0;
    tx->n_asserts = 0;
    tx->arena_checkpoint = 0;
    tx->next_op_id = 1;
    tx->history_capture = NULL;
    tx->history_ctx = NULL;
    tx->history_rollback = NULL;
}

static void tx_clear(TxState* tx) {
    uint32_t keep_consistency = 0;
    (void)keep_consistency;
    /* Deliberately does NOT clear history_capture / history_ctx /
     * history_rollback: the hook is owner configuration, set once
     * per TxState lifetime (see tx.h), not per-transaction state. */
    tx->active = 0;
    tx->n_writes = 0;
    tx->n_reads = 0;
    tx->n_asserts = 0;
    tx->next_op_id = 1;
}

/* Copy bytes into arena scratch. Returns NULL on OOM (or on a NULL
 * input with nonzero length). The copy is scratch: on abort it is
 * discarded by rewinding to arena_checkpoint; on commit the apply
 * phase copies values a second time into the live tree via
 * art_insert, so scratch waste is bounded by one transaction's size
 * and reclaimed by the next compact/reset. */
static const uint8_t* tx_scratch_copy(ART* art, const uint8_t* data, uint32_t len) {
    if (len == 0) return (const uint8_t*)""; /* stable non-NULL empty */
    if (!data) return NULL;
    uint8_t* dst = (uint8_t*)arena_alloc(art->arena, len);
    if (!dst) return NULL;
    for (uint32_t i = 0; i < len; i++) dst[i] = data[i];
    return dst;
}

static int key_equal(const uint8_t* a, uint32_t a_len, const uint8_t* b, uint32_t b_len) {
    if (a_len != b_len) return 0;
    for (uint32_t i = 0; i < a_len; i++) if (a[i] != b[i]) return 0;
    return 1;
}

/* Latest write-set entry for a key, or NULL. */
static TxWrite* tx_find_write(TxState* tx, const uint8_t* key, uint32_t key_len) {
    for (uint32_t i = tx->n_writes; i > 0; i--) {
        TxWrite* w = &tx->writes[i - 1];
        if (key_equal(w->key, w->key_len, key, key_len)) return w;
    }
    return NULL;
}

int tx_begin(TxState* tx, ART* art, uint64_t tx_id, uint64_t origin_node_id,
             uint32_t consistency_model, uint64_t schema_version) {
    if (!tx || !art || !art->arena) return TX_ERR_PROTOCOL;
    if (tx->active) return TX_ERR_PROTOCOL;
    tx->active = 1;
    tx->tx_id = tx_id;
    tx->origin_node_id = origin_node_id;
    tx->snapshot_seq = art->commit_seq;
    tx->schema_version = schema_version;
    tx->consistency_model = consistency_model;
    tx->n_writes = 0;
    tx->n_reads = 0;
    tx->n_asserts = 0;
    tx->next_op_id = 1;
    tx->arena_checkpoint = art->arena->offset;
    return TX_OK;
}

int tx_abort(TxState* tx, ART* art) {
    if (!tx || !tx->active) return TX_ERR_PROTOCOL;
    if (art && art->arena) {
        /* Rewind ONLY if no commit happened after BEGIN
         * (commit_seq still equals snapshot_seq). Then no live node
         * can have been allocated after the checkpoint by a
         * committed writer, and the only allocations after it are
         * this transaction's own scratch (no tx_* buffering function
         * touches the tree). If commit_seq moved, a writer DID
         * allocate live nodes after the checkpoint, so rewinding
         * would destroy them: skip the rewind and leave the scratch
         * as bump waste until the next compact/reset. This is the
         * opposite of the rejected pattern (rewinding over mutated
         * live nodes); the rewind here happens only when there is
         * provably nothing live to lose. Callers must not call
         * art_insert/art_delete directly on the same ART while a
         * transaction is active except to simulate a concurrent
         * writer, which is exactly the commit_seq-moved case above. */
        if (art->commit_seq == tx->snapshot_seq) {
            art->arena->offset = tx->arena_checkpoint;
        }
    }
    tx_clear(tx);
    return TX_OK;
}

static int tx_buffer_write(TxState* tx, ART* art, uint8_t op, uint8_t flags,
                           const uint8_t* key, uint32_t key_len,
                           const uint8_t* val, uint32_t val_len, uint64_t timestamp) {
    if (!tx->active || !key || key_len == 0 || key_len > YOWZA_MAX_KEY_LEN) return TX_ERR_PROTOCOL;
    TxWrite* existing = tx_find_write(tx, key, key_len);
    const uint8_t* kcopy = NULL;
    const uint8_t* vcopy = NULL;
    if (existing) {
        /* Merge in place (last write wins for this key). The old
         * scratch copy stays as bump waste until compact/reset. */
        if (op == TX_OP_PUT) {
            vcopy = tx_scratch_copy(art, val, val_len);
            if (!vcopy && val_len > 0) return TX_ERR_OOM;
            existing->op = op;
            existing->flags = flags;
            existing->val = vcopy;
            existing->val_len = val_len;
            existing->timestamp = timestamp;
        } else {
            existing->op = op;
            existing->flags = flags;
            existing->val = NULL;
            existing->val_len = 0;
            existing->timestamp = 0;
        }
        return TX_OK;
    }
    if (tx->n_writes >= YOWZA_TX_MAX_WRITES) return TX_ERR_OOM;
    kcopy = tx_scratch_copy(art, key, key_len);
    if (!kcopy) return TX_ERR_OOM;
    if (op == TX_OP_PUT) {
        vcopy = tx_scratch_copy(art, val, val_len);
        if (!vcopy && val_len > 0) return TX_ERR_OOM;
    }
    TxWrite* w = &tx->writes[tx->n_writes++];
    w->op = op;
    w->flags = flags;
    w->key = kcopy;
    w->key_len = key_len;
    w->val = vcopy;
    w->val_len = (op == TX_OP_PUT) ? val_len : 0;
    w->timestamp = (op == TX_OP_PUT) ? timestamp : 0;
    return TX_OK;
}

int tx_put(TxState* tx, ART* art, const uint8_t* key, uint32_t key_len,
           const uint8_t* val, uint32_t val_len, uint8_t flags, uint64_t timestamp) {
    if (!tx || !art) return TX_ERR_PROTOCOL;
    return tx_buffer_write(tx, art, TX_OP_PUT, flags, key, key_len, val, val_len, timestamp);
}

int tx_del(TxState* tx, ART* art, const uint8_t* key, uint32_t key_len) {
    if (!tx || !art) return TX_ERR_PROTOCOL;
    return tx_buffer_write(tx, art, TX_OP_DEL, 0, key, key_len, NULL, 0, 0);
}

int tx_get(TxState* tx, ART* art, const uint8_t* key, uint32_t key_len,
           uint8_t** out_val, uint32_t* out_val_len) {
    if (!tx || !art || !key || key_len == 0 || !out_val || !out_val_len) return TX_ERR_PROTOCOL;
    if (!tx->active) return TX_ERR_PROTOCOL;
    /* Read-your-writes first: the private set is the transaction's
     * view, the live tree is only the snapshot underneath it. */
    TxWrite* w = tx_find_write(tx, key, key_len);
    if (w) {
        if (w->op == TX_OP_DEL) {
            *out_val = NULL;
            *out_val_len = 0;
            return -1;
        }
        *out_val = (uint8_t*)w->val;
        *out_val_len = w->val_len;
        return 0;
    }
    /* Record the read for validation (first read wins: the snapshot
     * version is the one at first observation). */
    int already = 0;
    for (uint32_t i = 0; i < tx->n_reads; i++) {
        if (key_equal(tx->reads[i].key, tx->reads[i].key_len, key, key_len)) { already = 1; break; }
    }
    uint64_t version = 0;
    int found = 0;
    if (art_lookup_version(art, key, key_len, &version, &found) != 0) return TX_ERR_OOM;
    if (!already) {
        if (tx->n_reads >= YOWZA_TX_MAX_READS) return TX_ERR_OOM;
        const uint8_t* kcopy = tx_scratch_copy(art, key, key_len);
        if (!kcopy) return TX_ERR_OOM;
        TxRead* r = &tx->reads[tx->n_reads++];
        r->key = kcopy;
        r->key_len = key_len;
        r->version = version;
        r->found = found;
    }
    if (!found) {
        *out_val = NULL;
        *out_val_len = 0;
        return -1;
    }
    return art_search(art, key, key_len, out_val, out_val_len) == 0 ? 0 : -1;
}

int tx_assert(TxState* tx, ART* art, uint8_t kind, const uint8_t* key, uint32_t key_len,
              const uint8_t* val, uint32_t val_len) {
    if (!tx || !art || !tx->active || !key || key_len == 0) return TX_ERR_PROTOCOL;
    if (kind > TX_ASSERT_VALUE_EQUALS) return TX_ERR_PROTOCOL;
    if (tx->n_asserts >= YOWZA_TX_MAX_ASSERTS) return TX_ERR_OOM;
    const uint8_t* kcopy = tx_scratch_copy(art, key, key_len);
    if (!kcopy) return TX_ERR_OOM;
    const uint8_t* vcopy = NULL;
    if (kind == TX_ASSERT_VALUE_EQUALS) {
        vcopy = tx_scratch_copy(art, val, val_len);
        if (!vcopy && val_len > 0) return TX_ERR_OOM;
    }
    TxAssert* a = &tx->asserts[tx->n_asserts++];
    a->kind = kind;
    a->key = kcopy;
    a->key_len = key_len;
    a->val = vcopy;
    a->val_len = val_len;
    return TX_OK;
}

static int tx_validate(TxState* tx, ART* art) {
    /* 1. Snapshot validity of reads: a key observed at version V
     * must still be at version V. Any change after BEGIN (by a
     * committed legacy put/delete or another transaction) conflicts. */
    for (uint32_t i = 0; i < tx->n_reads; i++) {
        uint64_t version = 0;
        int found = 0;
        if (art_lookup_version(art, tx->reads[i].key, tx->reads[i].key_len, &version, &found) != 0)
            return TX_ERR_CONFLICT;
        if (version != tx->reads[i].version || found != tx->reads[i].found)
            return TX_ERR_CONFLICT;
    }
    /* 2. Write-write conflicts: a write key whose live version moved
     * past the snapshot sequence was changed after BEGIN. */
    for (uint32_t i = 0; i < tx->n_writes; i++) {
        uint64_t version = 0;
        int found = 0;
        if (art_lookup_version(art, tx->writes[i].key, tx->writes[i].key_len, &version, &found) != 0)
            return TX_ERR_CONFLICT;
        if (version > tx->snapshot_seq) return TX_ERR_CONFLICT;
    }
    /* 3. Uniqueness + asserts, evaluated against projected state. */
    for (uint32_t i = 0; i < tx->n_writes; i++) {
        if (tx->writes[i].op == TX_OP_PUT && (tx->writes[i].flags & TX_WRITE_UNIQUE)) {
            uint64_t version = 0;
            int found = 0;
            if (art_lookup_version(art, tx->writes[i].key, tx->writes[i].key_len, &version, &found) != 0)
                return TX_ERR_CONFLICT;
            if (found) return TX_ERR_ASSERT;
        }
    }
    /* 4. Asserts are evaluated against COMMITTED state at commit
     * time, deliberately NOT against projected state: an assert is
     * a precondition on the world the transaction is committing
     * into (optimistic-concurrency guard, e.g. PutWithIndex asserting
     * that the reverse entries it is about to delete are still the
     * ones it scanned). A transaction's own writes must not satisfy
     * its own asserts, or the guard would be vacuous. */
    for (uint32_t i = 0; i < tx->n_asserts; i++) {
        TxAssert* a = &tx->asserts[i];
        uint8_t* val = NULL;
        uint32_t val_len = 0;
        int found = art_search(art, a->key, a->key_len, &val, &val_len) == 0;
        if (a->kind == TX_ASSERT_ABSENT && found) return TX_ERR_ASSERT;
        if (a->kind == TX_ASSERT_PRESENT && !found) return TX_ERR_ASSERT;
        if (a->kind == TX_ASSERT_VALUE_EQUALS) {
            if (!found || val_len != a->val_len) return TX_ERR_ASSERT;
            for (uint32_t j = 0; j < val_len; j++) if (val[j] != a->val[j]) return TX_ERR_ASSERT;
        }
    }
    return TX_OK;
}

/* ---- WAL frame writing (append pieces + incremental FNV) ---- */

static void fnv_u32(uint64_t* h, uint32_t v) {
    uint8_t b[4];
    b[0] = v & 0xFF; b[1] = (v >> 8) & 0xFF; b[2] = (v >> 16) & 0xFF; b[3] = (v >> 24) & 0xFF;
    *h = tx_fnv1a_update(*h, b, 4);
}
static void fnv_u64(uint64_t* h, uint64_t v) {
    uint8_t b[8];
    for (int i = 0; i < 8; i++) b[i] = (uint8_t)((v >> (i * 8)) & 0xFF);
    *h = tx_fnv1a_update(*h, b, 8);
}
static int frame_append(StorageDriver* st, const uint8_t* data, uint32_t len) {
    if (len == 0) return 0;
    if (!st || !st->append) return -1;
    int written = st->append(st, data, len);
    return (written < 0 || (uint32_t)written != len) ? -1 : 0;
}
static int frame_append_u32(StorageDriver* st, uint64_t* h, uint32_t v) {
    uint8_t b[4];
    b[0] = v & 0xFF; b[1] = (v >> 8) & 0xFF; b[2] = (v >> 16) & 0xFF; b[3] = (v >> 24) & 0xFF;
    if (h) *h = tx_fnv1a_update(*h, b, 4);
    return frame_append(st, b, 4);
}
static int frame_append_u64(StorageDriver* st, uint64_t* h, uint64_t v) {
    uint8_t b[8];
    for (int i = 0; i < 8; i++) b[i] = (uint8_t)((v >> (i * 8)) & 0xFF);
    if (h) *h = tx_fnv1a_update(*h, b, 8);
    return frame_append(st, b, 8);
}
static int frame_append_u8(StorageDriver* st, uint64_t* h, uint8_t v) {
    if (h) *h = tx_fnv1a_update(*h, &v, 1);
    return frame_append(st, &v, 1);
}
static int frame_append_bytes(StorageDriver* st, uint64_t* h, const uint8_t* data, uint32_t len) {
    if (len == 0) return 0;
    if (h) *h = tx_fnv1a_update(*h, data, len);
    return frame_append(st, data, len);
}

/* Envelope size for one op (dep_count = 0 in Phase 1). */
static uint64_t envelope_len(void) { return YOWZA_FABRIC_ENVELOPE_FIXED; }

static uint64_t tx_frame_len(TxState* tx) {
    /* marker 1 + magic 4 + frame_len 4 + tx_id 8 + seq 8 + op_count 4
     * + consistency 4 + ops + commit marker 1 + checksum 8 */
    uint64_t total = 1 + 4 + 4 + 8 + 8 + 4 + 4 + 1 + 8;
    for (uint32_t i = 0; i < tx->n_writes; i++) {
        TxWrite* w = &tx->writes[i];
        total += envelope_len() + 4 + (uint64_t)w->key_len + 4 + (uint64_t)w->val_len;
        if (total > 0xFFFFFFFFULL) return 0;
    }
    return total;
}

/* Append one op's envelope + key + val, updating the frame checksum.
 * The per-op payload checksum is computed over the envelope fields
 * (in wire order, before the checksum field) + key_len + key +
 * val_len + val, and written as the envelope's last field. */
static int frame_append_op(StorageDriver* st, uint64_t* fh, TxState* tx, TxWrite* w,
                           uint64_t seq, uint64_t op_id) {
    uint64_t ph = tx_fnv1a_init();
    /* Envelope fields, each feeding both checksums (payload first
     * needs its own pass because the checksum value is not known
     * until all fields are hashed; so hash fields into ph, then
     * append them to the frame while feeding fh). */
    /* Helper macro: hash field into ph, append to frame (feeding fh). */
#define OP_FIELD_U32(v) do { fnv_u32(&ph, (v)); if (frame_append_u32(st, fh, (v)) != 0) return -1; } while (0)
#define OP_FIELD_U64(v) do { fnv_u64(&ph, (v)); if (frame_append_u64(st, fh, (v)) != 0) return -1; } while (0)
    OP_FIELD_U32(YOWZA_FABRIC_FORMAT_VERSION);
    OP_FIELD_U64(tx->tx_id);
    OP_FIELD_U64(op_id);
    OP_FIELD_U64(tx->origin_node_id);
    OP_FIELD_U64(seq);
    OP_FIELD_U32(w->op == TX_OP_PUT ? 0x01u : 0x03u);
    OP_FIELD_U32((uint32_t)tx->schema_version);
    OP_FIELD_U32(tx->consistency_model);
    OP_FIELD_U64(w->timestamp);
    OP_FIELD_U32(0); /* causal_dep_count = 0 in Phase 1 */
#undef OP_FIELD_U32
#undef OP_FIELD_U64
    /* key_len + key + val_len + val feed ph, then are appended. */
    fnv_u32(&ph, w->key_len);
    ph = tx_fnv1a_update(ph, w->key, w->key_len);
    fnv_u32(&ph, w->val_len);
    if (w->op == TX_OP_PUT && w->val_len > 0) ph = tx_fnv1a_update(ph, w->val, w->val_len);
    /* Write the payload checksum as the envelope's last field. */
    if (frame_append_u64(st, fh, ph) != 0) return -1;
    if (frame_append_u32(st, fh, w->key_len) != 0) return -1;
    if (frame_append_bytes(st, fh, w->key, w->key_len) != 0) return -1;
    if (frame_append_u32(st, fh, w->val_len) != 0) return -1;
    if (w->op == TX_OP_PUT && w->val_len > 0) {
        if (frame_append_bytes(st, fh, w->val, w->val_len) != 0) return -1;
    }
    return 0;
}

static int tx_append_frame(TxState* tx, StorageDriver* st, uint64_t seq) {
    uint64_t len64 = tx_frame_len(tx);
    if (len64 == 0 || len64 > 0xFFFFFFFFULL) return -1;
    uint32_t frame_len = (uint32_t)len64;
    if (st->check_capacity && st->check_capacity(st, frame_len) != 0) return -1;
    uint64_t fh = tx_fnv1a_init();
    if (frame_append_u8(st, &fh, YOWZA_TX_FRAME_MARKER) != 0) return -1;
    {
        static const uint8_t magic[4] = {'Y', 'W', 'T', 'X'};
        if (frame_append_bytes(st, &fh, magic, 4) != 0) return -1;
    }
    if (frame_append_u32(st, &fh, frame_len) != 0) return -1;
    if (frame_append_u64(st, &fh, tx->tx_id) != 0) return -1;
    if (frame_append_u64(st, &fh, seq) != 0) return -1;
    if (frame_append_u32(st, &fh, tx->n_writes) != 0) return -1;
    if (frame_append_u32(st, &fh, tx->consistency_model) != 0) return -1;
    for (uint32_t i = 0; i < tx->n_writes; i++) {
        if (frame_append_op(st, &fh, tx, &tx->writes[i], seq, (uint64_t)i + 1) != 0) return -1;
    }
    if (frame_append_u8(st, &fh, YOWZA_TX_FRAME_COMMIT_MARKER) != 0) return -1;
    /* Final checksum: FNV over everything before it, appended WITHOUT
     * feeding itself. */
    {
        uint8_t b[8];
        for (int i = 0; i < 8; i++) b[i] = (uint8_t)((fh >> (i * 8)) & 0xFF);
        if (frame_append(st, b, 8) != 0) return -1;
    }
    return 0;
}

/* ---- Apply + logical undo ---- */

typedef struct {
    ARTNode* node; /* NULL if the path did not exist before */
    int was_end;
    uint32_t val_len;
    uint8_t* val_ptr;
    uint64_t timestamp;
    uint64_t version;
} TxUndo;

static int tx_apply(TxState* tx, ART* art, uint64_t seq, TxUndo* undo) {
    art->applying = 1;
    art->apply_seq = seq;
    for (uint32_t i = 0; i < tx->n_writes; i++) {
        TxWrite* w = &tx->writes[i];
        ARTNode* node = art_find_terminal(art, w->key, w->key_len);
        undo[i].node = node;
        if (node) {
            undo[i].was_end = node->is_end;
            undo[i].val_len = node->val_len;
            undo[i].val_ptr = node->val_ptr;
            undo[i].timestamp = node->timestamp;
            undo[i].version = node->version;
        } else {
            undo[i].was_end = 0;
            undo[i].val_len = 0;
            undo[i].val_ptr = NULL;
            undo[i].timestamp = 0;
            undo[i].version = 0;
        }
        int rc;
        if (w->op == TX_OP_PUT) {
            rc = art_insert(art, w->key, w->key_len, w->val, w->val_len, w->timestamp);
        } else {
            rc = art_delete(art, w->key, w->key_len);
            if (rc != 0) {
                /* Phase C unified delete: a hot-absent
                 * key (no terminal, or an intermediate-
                 * only node with timestamp 0) must still
                 * leave a shadowing tombstone, or a
                 * segment-only value below it resurfaces
                 * at new snapshots. An already-tombstoned
                 * terminal (is_end == 0, timestamp > 0)
                 * already shadows: leave it alone.
                 * The undo record above was captured
                 * BEFORE this apply, so a later failure
                 * restores the pre-apply terminal (no
                 * stray tombstone from a failed tx). */
                ARTNode* pre = undo[i].node;
                if (!pre || (!pre->is_end && pre->timestamp == 0)) {
                    if (art_insert(art, w->key, w->key_len, (const uint8_t*)"", 0, 0) != 0) {
                        rc = TX_ERR_OOM;
                    } else if (art_delete(art, w->key, w->key_len) != 0) {
                        /* Just inserted, so this is
                         * unreachable in practice — but
                         * fail closed AND restore the
                         * pre-apply terminal here, since
                         * the generic undo loop below
                         * covers predecessors only. */
                        ARTNode* n = art_find_terminal(art, w->key, w->key_len);
                        if (n) {
                            if (!pre) {
                                n->is_end = 0; n->val_ptr = NULL;
                                n->val_len = 0; n->timestamp = 0; n->version = 0;
                            } else {
                                n->is_end = (uint8_t)undo[i].was_end;
                                n->val_ptr = undo[i].val_ptr;
                                n->val_len = undo[i].val_len;
                                n->timestamp = undo[i].timestamp;
                                n->version = undo[i].version;
                            }
                        }
                        rc = TX_ERR_OOM;
                    } else {
                        rc = 0;
                    }
                } else {
                    rc = 0; /* already tombstoned: nothing to do */
                }
            }
        }
        if (rc != 0) {
            /* Logical undo, reverse order, including this op's
             * predecessors only (this op failed, so it changed
             * nothing terminal; predecessors are restored below). */
            for (uint32_t j = i; j > 0; j--) {
                TxUndo* u = &undo[j - 1];
                TxWrite* pw = &tx->writes[j - 1];
                ARTNode* n = art_find_terminal(art, pw->key, pw->key_len);
                if (!n) continue; /* path vanished: cannot happen (arena never frees) */
                if (u->node == NULL) {
                    /* Created by this tx: clear the terminal header.
                     * Intermediate nodes stay, harmless and invisible
                     * (is_end == 0), exactly as art_insert documents
                     * for its own OOM paths. */
                    n->is_end = 0;
                    n->val_ptr = NULL;
                    n->val_len = 0;
                    n->timestamp = 0;
                    n->version = 0;
                } else {
                    n->is_end = (uint8_t)u->was_end;
                    n->val_ptr = u->val_ptr;
                    n->val_len = u->val_len;
                    n->timestamp = u->timestamp;
                    n->version = u->version;
                }
            }
            art->applying = 0;
            art->apply_seq = 0;
            return TX_ERR_OOM;
        }
    }
    art->applying = 0;
    art->apply_seq = 0;
    return TX_OK;
}

static void tx_undo_all(TxState* tx, ART* art, TxUndo* undo, uint32_t applied_count) {
    for (uint32_t j = applied_count; j > 0; j--) {
        TxUndo* u = &undo[j - 1];
        TxWrite* pw = &tx->writes[j - 1];
        ARTNode* n = art_find_terminal(art, pw->key, pw->key_len);
        if (!n) continue;
        if (u->node == NULL) {
            n->is_end = 0;
            n->val_ptr = NULL;
            n->val_len = 0;
            n->timestamp = 0;
            n->version = 0;
        } else {
            n->is_end = (uint8_t)u->was_end;
            n->val_ptr = u->val_ptr;
            n->val_len = u->val_len;
            n->timestamp = u->timestamp;
            n->version = u->version;
        }
    }
}

int tx_commit(TxState* tx, ART* art, StorageDriver* storage) {
    if (!tx || !art || !tx->active) return TX_ERR_PROTOCOL;
    /* 1. Validation first: nothing is applied, nothing is written. */
    int vrc = tx_validate(tx, art);
    if (vrc != TX_OK) {
        tx_abort(tx, art);
        return vrc;
    }
    if (tx->n_writes == 0) {
        /* Read-only / empty transaction: no frame, no sequence spent.
         * Reads were validated above; discarding scratch is the commit. */
        tx_clear(tx);
        return TX_OK;
    }
    /* 2. History capture, fail-closed, BEFORE any mutation. This
     * mirrors segdb_put/segdb_delete exactly: the superseded
     * committed version of each distinct write key is captured
     * (live value, or tombstone marker for a tombstoned terminal)
     * before the overwrite, so a snapshot taken before this commit
     * keeps reading it. Write keys are already merged per key by
     * the buffering layer, so one capture per write entry covers
     * the key. A capture failure aborts the commit with TX_ERR_OOM
     * before anything is applied; captures already made for this
     * commit are rolled back first (when a rollback hook is set).
     * Capture runs after validation, so a conflicting or assert-
     * failing transaction never captures anything. */
    uint32_t hist_captured = 0;
    if (tx->history_capture) {
        for (uint32_t i = 0; i < tx->n_writes; i++) {
            TxWrite* w = &tx->writes[i];
            ARTNode* term = art_find_terminal(art, w->key, w->key_len);
            if (!term) continue; /* new key: nothing superseded */
            if (!term->is_end && term->timestamp == 0) continue; /* intermediate only */
            int crc = tx->history_capture(tx->history_ctx, w->key, w->key_len,
                                          term->version, term->is_end ? 0 : 1,
                                          term->is_end ? term->val_ptr : NULL,
                                          term->is_end ? term->val_len : 0,
                                          term->timestamp);
            if (crc != 0) {
                if (tx->history_rollback && hist_captured > 0)
                    tx->history_rollback(tx->history_ctx, hist_captured);
                tx_abort(tx, art);
                return TX_ERR_OOM;
            }
            hist_captured++;
        }
    }
    /* 3. Apply to the live tree, with logical undo on failure.
     * Readers are serialized by the host (Go db.mu / single-threaded
     * C API), so no reader can observe this apply mid-way; that host
     * invariant is what makes apply-then-frame publication atomic
     * for Phase 1. See PHASE1_NOTES.md, "Atomicity scope". */
    TxUndo undo[YOWZA_TX_MAX_WRITES];
    uint64_t seq = art->commit_seq + 1;
    int arc = tx_apply(tx, art, seq, undo);
    if (arc != TX_OK) {
        /* tx_apply already undid predecessors; roll the captured
         * history back with them, so a mid-apply OOM leaves
         * terminals AND history exactly as before the commit.
         * Clear state WITHOUT rewinding: the apply allocated live
         * nodes, so the begin checkpoint is no longer a safe
         * rewind point. */
        if (tx->history_rollback && hist_captured > 0)
            tx->history_rollback(tx->history_ctx, hist_captured);
        tx_clear(tx);
        return arc;
    }
    art->commit_seq = seq;
    /* 4. One WAL frame + durability barrier. On failure, undo the
     * apply (the frame is the commit record; without it, the tx
     * must not stay visible) and truncate the partial frame where
     * the driver supports it. The captured history is rolled back
     * with the apply: a tx that never became durable must not
     * leave history behind for a write that never landed. */
    int frame_start = -1;
    if (storage && storage->log_length) frame_start = storage->log_length(storage);
    int frc = -1;
    if (storage && storage->append) {
        frc = tx_append_frame(tx, storage, seq);
        if (frc == 0 && storage->sync) {
            if (storage->sync(storage) != 0) frc = -1;
        }
    } else if (storage) {
        frc = -1; /* storage present but no append: cannot be durable */
    } else {
        frc = 0; /* no storage configured: in-memory only, nothing to frame */
    }
    if (frc != 0) {
        if (storage && storage->truncate && frame_start >= 0) {
            storage->truncate(storage, (uint32_t)frame_start);
        }
        tx_undo_all(tx, art, undo, tx->n_writes);
        if (tx->history_rollback && hist_captured > 0)
            tx->history_rollback(tx->history_ctx, hist_captured);
        /* The undo restores logical state, but commit_seq was
         * already advanced: sequences must stay monotonic and
         * versions stamped by the undone apply are restored by the
         * undo records, so leaving commit_seq advanced only wastes
         * one sequence number, which is safe. */
        tx_clear(tx);
        return TX_ERR_WAL;
    }
    tx_clear(tx);
    return TX_OK;
}

/* ---- WAL replay ---- */

static uint32_t rd_u32(const uint8_t* b) {
    return (uint32_t)b[0] | ((uint32_t)b[1] << 8) | ((uint32_t)b[2] << 16) | ((uint32_t)b[3] << 24);
}
static uint64_t rd_u64(const uint8_t* b) {
    uint64_t v = 0;
    for (int i = 0; i < 8; i++) v |= (uint64_t)b[i] << (i * 8);
    return v;
}

/* Validate + apply one complete frame starting at `off`. On success
 * returns the frame length and sets *applied. On any inconsistency
 * returns 0 (caller stops replay: the frame is incomplete/corrupt). */
static uint32_t replay_frame(ART* art, const uint8_t* wal, uint32_t wal_len, uint32_t off) {
    /* Need at least marker + magic + frame_len. */
    if (off + 9 > wal_len) return 0;
    if (wal[off] != YOWZA_TX_FRAME_MARKER) return 0;
    if (!(wal[off + 1] == 'Y' && wal[off + 2] == 'W' && wal[off + 3] == 'T' && wal[off + 4] == 'X')) return 0;
    uint32_t frame_len = rd_u32(&wal[off + 5]);
    if (frame_len < 1 + 4 + 4 + 8 + 8 + 4 + 4 + 1 + 8) return 0;
    if (off + frame_len > wal_len) return 0; /* truncated tail */
    /* Frame checksum over everything before the last 8 bytes. */
    uint64_t expect = rd_u64(&wal[off + frame_len - 8]);
    if (tx_fnv1a_update(tx_fnv1a_init(), &wal[off], frame_len - 8) != expect) return 0;
    if (wal[off + frame_len - 9] != YOWZA_TX_FRAME_COMMIT_MARKER) return 0;
    uint64_t seq = rd_u64(&wal[off + 9 + 8]);
    (void)seq;
    uint32_t op_count = rd_u32(&wal[off + 9 + 8 + 8]);
    uint32_t pos = off + 9 + 8 + 8 + 4 + 4; /* after header: marker+magic+len+txid+seq+count+consistency */
    /* First pass: validate every op (envelope + payload checksum +
     * bounds) WITHOUT mutating anything. A frame is applied only
     * whole, or not at all. */
    for (uint32_t i = 0; i < op_count; i++) {
        if (pos + YOWZA_FABRIC_ENVELOPE_FIXED > off + frame_len - 9) return 0;
        uint32_t ep = pos;
        uint32_t fmt = rd_u32(&wal[ep]); ep += 4;
        if (fmt != YOWZA_FABRIC_FORMAT_VERSION) return 0;
        ep += 8 + 8 + 8 + 8; /* tx_id, op_id, origin, seq */
        uint32_t op_type = rd_u32(&wal[ep]); ep += 4;
        if (op_type != 0x01 && op_type != 0x03) return 0;
        ep += 4 + 4 + 8; /* schema, consistency, timestamp */
        uint32_t dep_count = rd_u32(&wal[ep]); ep += 4;
        if (dep_count > 1024) return 0;
        if (ep + dep_count * 8 > off + frame_len - 9) return 0;
        ep += dep_count * 8;
        uint64_t payload_expect = rd_u64(&wal[ep]); ep += 8;
        /* payload checksum covers envelope fields (before checksum)
         * + key_len + key + val_len + val. Recompute: envelope
         * fields span pos .. ep-8 (checksum field excluded). */
        uint64_t ph = tx_fnv1a_init();
        ph = tx_fnv1a_update(ph, &wal[pos], (uint32_t)(ep - 8 - pos));
        if (ep + 4 > off + frame_len - 9) return 0;
        uint32_t key_len = rd_u32(&wal[ep]); ep += 4;
        ph = tx_fnv1a_update(ph, (const uint8_t*)&wal[ep - 4], 4);
        if (ep + key_len > off + frame_len - 9) return 0;
        ph = tx_fnv1a_update(ph, &wal[ep], key_len);
        ep += key_len;
        if (ep + 4 > off + frame_len - 9) return 0;
        uint32_t val_len = rd_u32(&wal[ep]); ep += 4;
        ph = tx_fnv1a_update(ph, (const uint8_t*)&wal[ep - 4], 4);
        if (ep + val_len > off + frame_len - 9) return 0;
        ph = tx_fnv1a_update(ph, &wal[ep], val_len);
        ep += val_len;
        if (ph != payload_expect) return 0;
        pos = ep;
    }
    if (pos != off + frame_len - 9) return 0; /* trailing garbage inside frame */
    /* Second pass: apply, in applying mode with the frame's sequence. */
    uint64_t frame_seq = rd_u64(&wal[off + 9 + 8]);
    uint64_t saved_seq = art->commit_seq;
    art->applying = 1;
    art->apply_seq = frame_seq;
    pos = off + 9 + 8 + 8 + 4 + 4;
    for (uint32_t i = 0; i < op_count; i++) {
        uint32_t ep = pos;
        ep += 4 + 8 + 8 + 8 + 8;
        uint32_t op_type = rd_u32(&wal[ep]); ep += 4;
        ep += 4 + 4;
        uint64_t timestamp = rd_u64(&wal[ep]); ep += 8;
        uint32_t dep_count = rd_u32(&wal[ep]); ep += 4;
        ep += dep_count * 8 + 8; /* checksum */
        uint32_t key_len = rd_u32(&wal[ep]); ep += 4;
        const uint8_t* key = &wal[ep]; ep += key_len;
        uint32_t val_len = rd_u32(&wal[ep]); ep += 4;
        const uint8_t* val = &wal[ep]; ep += val_len;
        if (op_type == 0x01) {
            if (art_insert(art, key, key_len, val, val_len, timestamp) != 0) {
                art->applying = 0;
                art->apply_seq = 0;
                art->commit_seq = saved_seq;
                return 0; /* apply failed: frame not applied (best effort: tree may hold earlier ops of this frame; caller treats replay as stopped and the frame as invalid — see notes on replay atomicity limits) */
            }
        } else {
            /* Phase C unified delete, same rule as
             * tx_apply: a hot-absent key gets a
             * shadowing tombstone (stamped with the
             * frame sequence, since applying mode is
             * on), so frame replay and live commit
             * agree on what DELETE means. An already-
             * tombstoned terminal is left alone. */
            if (art_delete(art, key, key_len) != 0) {
                ARTNode* term = art_find_terminal(art, key, key_len);
                if (!term || (!term->is_end && term->timestamp == 0)) {
                    if (art_insert(art, key, key_len, (const uint8_t*)"", 0, 0) != 0 ||
                        art_delete(art, key, key_len) != 0) {
                        art->applying = 0;
                        art->apply_seq = 0;
                        art->commit_seq = saved_seq;
                        return 0;
                    }
                }
            }
        }
        pos = ep;
    }
    art->applying = 0;
    art->apply_seq = 0;
    if (frame_seq > art->commit_seq) art->commit_seq = frame_seq;
    return frame_len;
}

int tx_replay_wal(ART* art, const uint8_t* wal, uint32_t wal_len, uint32_t* out_applied_tx) {
    if (!art || !wal) return -1;
    uint32_t applied = 0;
    uint32_t off = 0;
    while (off < wal_len) {
        uint8_t op = wal[off];
        if (op == YOWZA_TX_FRAME_MARKER) {
            uint32_t flen = replay_frame(art, wal, wal_len, off);
            if (flen == 0) break; /* incomplete/corrupt frame: stop, do not apply */
            off += flen;
            applied++;
            continue;
        }
        /* Legacy per-op record: [op][ts?][key_len][key][val_len][val] */
        if (op != 0x01 && op != 0x03 && op != 0x07) break; /* unknown: stop (legacy opcodes OP_PUT/OP_DEL/OP_PUT_CRDT; vm.h not included here to avoid a tx.h/vm.h cycle) */
        uint32_t p = off + 1;
        uint64_t ts = 0;
        if (op == 0x07) { /* OP_PUT_CRDT */
            if (p + 8 > wal_len) break;
            ts = rd_u64(&wal[p]); p += 8;
        }
        if (p + 4 > wal_len) break;
        uint32_t key_len = rd_u32(&wal[p]); p += 4;
        if (p + key_len > wal_len) break;
        const uint8_t* key = &wal[p]; p += key_len;
        if (p + 4 > wal_len) break;
        uint32_t val_len = rd_u32(&wal[p]); p += 4;
        if (p + val_len > wal_len) break;
        const uint8_t* val = &wal[p]; p += val_len;
        if (op == 0x03) { /* OP_DEL */
            art_delete(art, key, key_len);
        } else {
            if (art_insert(art, key, key_len, val, val_len, ts) != 0) break;
        }
        off = p;
    }
    if (out_applied_tx) *out_applied_tx = applied;
    return 0;
}
