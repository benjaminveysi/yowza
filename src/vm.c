#include "vm.h"

void vm_init(VMContext* vm, ART* art_index, uint8_t* bytecode, size_t length, StorageDriver* storage) {
    if (!vm) return;
    vm->art_index = art_index;
    vm->bytecode = bytecode;
    vm->length = length;
    vm->pc = 0;
    vm->response_ptr = NULL;
    vm->response_len = 0;
    vm->storage = storage;
    tx_init_state(&vm->tx);
    /* Phase D: a fresh context has no timeline. The host
     * sets one with vm_set_timeline AFTER vm_init (see
     * vm.h); vm_init clearing it is what keeps the
     * non-timeline path exactly as before. */
    vm->timeline_active = 0;
    vm->timeline_snapshot_seq = 0;
    vm->timeline_n_writes = 0;
}

/* ---- Phase D: timeline write set (see vm.h) ---- */

void vm_set_timeline(VMContext* vm, uint64_t snapshot_seq) {
    if (!vm) return;
    vm->timeline_active = 1;
    vm->timeline_snapshot_seq = snapshot_seq;
    vm->timeline_n_writes = 0;
}

void vm_clear_timeline(VMContext* vm) {
    if (!vm) return;
    vm->timeline_active = 0;
    vm->timeline_n_writes = 0;
}

static int vm_timeline_key_eq(const uint8_t* a, uint32_t alen, const uint8_t* b, uint32_t blen) {
    uint32_t i;
    if (alen != blen) return 0;
    for (i = 0; i < alen; i++) if (a[i] != b[i]) return 0;
    return 1;
}

int vm_timeline_stage(VMContext* vm, const uint8_t* key, uint32_t key_len,
                      const uint8_t* val, uint32_t val_len, int deleted, uint64_t timestamp) {
    uint32_t i, j;
    VMTimelineWrite* w;
    uint8_t* kcopy;
    uint8_t* vcopy = NULL;
    if (!vm || !key || key_len == 0) return -1;
    for (i = 0; i < vm->timeline_n_writes; i++) {
        w = &vm->timeline_writes[i];
        if (vm_timeline_key_eq(w->key, w->key_len, key, key_len)) {
            /* Last write wins, in place. The old arena
             * copy becomes bump waste until reset, as
             * with tx write merging. */
            if (!deleted && val_len > 0) {
                vcopy = (uint8_t*)arena_alloc(vm->art_index->arena, val_len);
                if (!vcopy) return -3;
                for (j = 0; j < val_len; j++) vcopy[j] = val[j];
            }
            w->val = vcopy;
            w->val_len = deleted ? 0 : val_len;
            w->deleted = deleted;
            w->timestamp = timestamp;
            return 0;
        }
    }
    if (vm->timeline_n_writes >= VM_TIMELINE_MAX_WRITES) return -3;
    kcopy = (uint8_t*)arena_alloc(vm->art_index->arena, key_len);
    if (!kcopy) return -3;
    for (j = 0; j < key_len; j++) kcopy[j] = key[j];
    if (!deleted && val_len > 0) {
        vcopy = (uint8_t*)arena_alloc(vm->art_index->arena, val_len);
        if (!vcopy) return -3;
        for (j = 0; j < val_len; j++) vcopy[j] = val[j];
    }
    w = &vm->timeline_writes[vm->timeline_n_writes++];
    w->key = kcopy;
    w->key_len = key_len;
    w->val = vcopy;
    w->val_len = deleted ? 0 : val_len;
    w->deleted = deleted;
    w->timestamp = timestamp;
    return 0;
}

int vm_timeline_write_count(VMContext* vm) {
    if (!vm) return -1;
    return (int)vm->timeline_n_writes;
}

int vm_timeline_write_at(VMContext* vm, uint32_t idx, const uint8_t** key, uint32_t* key_len,
                         const uint8_t** val, uint32_t* val_len, int* deleted) {
    VMTimelineWrite* w;
    if (!vm || idx >= vm->timeline_n_writes || !key || !key_len || !val || !val_len || !deleted) return -1;
    w = &vm->timeline_writes[idx];
    *key = w->key;
    *key_len = w->key_len;
    *val = w->val;
    *val_len = w->val_len;
    *deleted = w->deleted;
    return 0;
}

/* Layered timeline read, WITHOUT tx writes (the TX_GET
 * branch checks those first). Returns 0 found, -1 not
 * found, -11 ambiguous (a live or tombstoned node newer
 * than a non-zero snapshot: the ART-only layer cannot
 * tell creation-after-snapshot from overwrite-after-
 * snapshot, so it fails instead of guessing). At
 * snapshot 0 the base is empty by definition, so any
 * live node is post-snapshot and the answer is "absent"
 * (a tombstone at snapshot 0 is likewise absent). */
static int vm_timeline_get_layered(VMContext* vm, const uint8_t* key, uint32_t key_len,
                                   uint8_t** out_val, uint32_t* out_val_len) {
    uint32_t i;
    ARTNode* term;
    for (i = vm->timeline_n_writes; i > 0; i--) {
        VMTimelineWrite* w = &vm->timeline_writes[i - 1];
        if (vm_timeline_key_eq(w->key, w->key_len, key, key_len)) {
            if (w->deleted) return -1;
            *out_val = (uint8_t*)w->val;
            *out_val_len = w->val_len;
            return 0;
        }
    }
    term = art_find_terminal(vm->art_index, key, key_len);
    if (!term) return -1;
    if (term->version > vm->timeline_snapshot_seq && vm->timeline_snapshot_seq != 0) return -11;
    if (!term->is_end) return -1;
    if (vm->timeline_snapshot_seq == 0) {
        /* Empty base: a live node here was necessarily
         * written after the snapshot. */
        return -1;
    }
    *out_val = term->val_ptr;
    *out_val_len = term->val_len;
    return 0;
}

/* Step 1 WAL helpers.
 *
 * Old code built the whole WAL record in a fixed 1024-byte stack buffer
 * and silently dropped anything larger, and ignored the append result,
 * so a full WAL still reported a successful PUT. Now records are
 * written in pieces (no size cap from the VM side), CRDT records carry
 * their timestamp, and every append is checked. The VM also pre-checks
 * capacity *before* mutating the ART, so the common full-WAL case
 * fails fast with -6 and leaves the tree untouched.
 *
 * Phase 2, Track C — unified legacy contract (see legacy_undo below):
 * a recorded legacy op (OP_PUT / OP_PUT_CRDT / OP_DEL) is atomic in
 * the in-process sense: WAL record first, ART mutation second, sync
 * third. On ANY failure at append, apply, or sync, the captured
 * terminal header is restored and the WAL is truncated back to its
 * pre-op length where the driver supports it, and -6 (WAL/sync) or
 * -3 (OOM) is returned. A failed op therefore leaves both ART and
 * WAL exactly as before (modulo harmless bump-allocator waste from
 * intermediate path nodes created by a failed insert, exactly as
 * art_insert already documents for its own OOM paths, and modulo a
 * partial WAL tail when the driver offers no truncate — that tail is
 * crash-equivalent: tx_replay_wal stops at a truncated legacy tail
 * record and applies nothing from it). The crash window is the
 * mirror image of Phase 1 tx_commit: a crash after the WAL append
 * but before the ART apply replays the op on recovery, so a durable
 * record always corresponds to a committed op and an op reported as
 * failed never stays visible in-process. OP_PUT_REMOTE / OP_DEL_REMOTE
 * are intentionally WAL-free (replication-applied) and are NOT
 * covered by this contract; their durability, if any, comes from
 * the caller's own channel, not from this VM path.
 */

static int wal_is_recorded_op(Opcode op) {
    return op == OP_PUT || op == OP_DEL || op == OP_PUT_CRDT;
}

static uint64_t wal_record_len(Opcode op, uint32_t key_len, uint32_t val_len) {
    uint64_t len = 1 + 4 + (uint64_t)key_len + 4 + (uint64_t)val_len;
    if (op == OP_PUT_CRDT) len += 8; /* timestamp */
    return len;
}

/* Returns 0 if the record can (probably) be appended, -1 otherwise.
 * This is only a check: it changes no storage state. */
static int wal_reserve(VMContext* vm, Opcode op, uint32_t key_len, uint32_t val_len) {
    if (!vm->storage || !vm->storage->append) return -1;
    if (!vm->storage->check_capacity) return 0; /* unknown: append will tell */
    uint64_t len = wal_record_len(op, key_len, val_len);
    if (len > 0xFFFFFFFFULL) return -1;
    return vm->storage->check_capacity(vm->storage, (uint32_t)len);
}

static int wal_append_piece(VMContext* vm, const uint8_t* data, uint32_t len) {
    if (len == 0) return 0;
    int written = vm->storage->append(vm->storage, data, len);
    if (written < 0 || (uint32_t)written != len) return -1;
    return 0;
}

static int wal_append_u32(VMContext* vm, uint32_t val) {
    uint8_t buf[4];
    buf[0] = val & 0xFF;
    buf[1] = (val >> 8) & 0xFF;
    buf[2] = (val >> 16) & 0xFF;
    buf[3] = (val >> 24) & 0xFF;
    return wal_append_piece(vm, buf, 4);
}

static int wal_append_u64(VMContext* vm, uint64_t val) {
    uint8_t buf[8];
    for (int i = 0; i < 8; i++) buf[i] = (uint8_t)((val >> (i * 8)) & 0xFF);
    return wal_append_piece(vm, buf, 8);
}

/* Phase 2 legacy undo record: the terminal header of the key before
 * the op, plus the ART clocks the op may advance. Restoring the
 * header restores logical state (promotion copies headers, and a
 * failed insert for an already-present key never expands its path,
 * so the captured pointer stays valid; a created terminal is found
 * again by art_find_terminal and cleared). Intermediate path nodes
 * created by a failed insert stay, invisible (is_end == 0), as in
 * tx undo. commit_seq is restored, not wasted: the undone op stamped
 * no surviving node, so reusing its sequence is safe (unlike
 * tx_commit, where a failed frame leaves commit_seq advanced because
 * the apply had already been published to the sequence space). */
typedef struct {
    ARTNode* node; /* NULL if the path did not exist before the op */
    int was_end;
    uint32_t val_len;
    uint8_t* val_ptr;
    uint64_t timestamp;
    uint64_t version;
    uint64_t commit_seq;
    uint64_t max_timestamp;
} LegacyUndo;

static void legacy_capture(VMContext* vm, const uint8_t* key, uint32_t key_len, LegacyUndo* u) {
    ARTNode* node = art_find_terminal(vm->art_index, key, key_len);
    u->node = node;
    if (node) {
        u->was_end = node->is_end;
        u->val_len = node->val_len;
        u->val_ptr = node->val_ptr;
        u->timestamp = node->timestamp;
        u->version = node->version;
    } else {
        u->was_end = 0;
        u->val_len = 0;
        u->val_ptr = NULL;
        u->timestamp = 0;
        u->version = 0;
    }
    u->commit_seq = vm->art_index->commit_seq;
    u->max_timestamp = vm->art_index->max_timestamp;
}

static void legacy_restore(VMContext* vm, const uint8_t* key, uint32_t key_len, const LegacyUndo* u) {
    ARTNode* node = art_find_terminal(vm->art_index, key, key_len);
    if (node) {
        if (u->node == NULL) {
            /* Created by this op: clear the terminal header. */
            node->is_end = 0;
            node->val_ptr = NULL;
            node->val_len = 0;
            node->timestamp = 0;
            node->version = 0;
        } else {
            node->is_end = (uint8_t)u->was_end;
            node->val_ptr = u->val_ptr;
            node->val_len = u->val_len;
            node->timestamp = u->timestamp;
            node->version = u->version;
        }
    }
    /* If node == NULL the op created nothing terminal (OOM before the
     * terminal was reached); there is nothing to clear. */
    vm->art_index->commit_seq = u->commit_seq;
    vm->art_index->max_timestamp = u->max_timestamp;
}

/* Truncate the WAL back to pre_len where the driver supports it.
 * pre_len_valid == 0 means log_length was unavailable: nothing to
 * truncate to, and the caller must accept a possible partial tail
 * (crash-equivalent, see the header comment). */
static void legacy_rollback_wal(VMContext* vm, int pre_len_valid, int pre_len) {
    if (pre_len_valid && vm->storage && vm->storage->truncate) {
        vm->storage->truncate(vm->storage, (uint32_t)pre_len);
    }
}

/* Sync the WAL driver after a recorded op. Returns 0 on success or
 * when there is nothing to sync (no storage, or no sync hook — a
 * driver without a sync hook offers no durability barrier and the
 * VM cannot invent one), -1 on a sync failure. */
static int legacy_sync(VMContext* vm) {
    if (!vm->storage || !vm->storage->sync) return 0;
    return vm->storage->sync(vm->storage) == 0 ? 0 : -1;
}

static int append_to_storage(VMContext* vm, Opcode op, uint64_t timestamp,
                             const uint8_t* key, uint32_t key_len,
                             const uint8_t* val, uint32_t val_len) {
    if (!vm->storage) return 0; /* no WAL configured */
    if (!vm->storage->append) return -1;

    uint8_t opcode = (uint8_t)op;
    if (wal_append_piece(vm, &opcode, 1) != 0) return -1;
    if (op == OP_PUT_CRDT) {
        if (wal_append_u64(vm, timestamp) != 0) return -1;
    }
    if (wal_append_u32(vm, key_len) != 0) return -1;
    if (wal_append_piece(vm, key, key_len) != 0) return -1;
    if (wal_append_u32(vm, val_len) != 0) return -1;
    if (wal_append_piece(vm, val, val_len) != 0) return -1;
    return 0;
}

/* Checked readers: the old versions returned 0 on truncation and let
 * execution continue, so a 5-byte truncated PUT read 32 bytes past the
 * bytecode buffer and inserted garbage into the ART. Every reader now
 * returns -1 on truncation and does not advance pc. */
static int read_u32(VMContext* vm, uint32_t* out) {
    if (vm->pc > vm->length || vm->length - vm->pc < 4) return -1;
    *out = (uint32_t)vm->bytecode[vm->pc] |
           ((uint32_t)vm->bytecode[vm->pc + 1] << 8) |
           ((uint32_t)vm->bytecode[vm->pc + 2] << 16) |
           ((uint32_t)vm->bytecode[vm->pc + 3] << 24);
    vm->pc += 4;
    return 0;
}

static int read_u64(VMContext* vm, uint64_t* out) {
    if (vm->pc > vm->length || vm->length - vm->pc < 8) return -1;
    uint64_t val = 0;
    for (int i = 0; i < 8; i++) val |= (uint64_t)vm->bytecode[vm->pc + i] << (i * 8);
    *out = val;
    vm->pc += 8;
    return 0;
}

static int read_u8(VMContext* vm, uint8_t* out) {
    if (vm->pc >= vm->length) return -1;
    *out = vm->bytecode[vm->pc++];
    return 0;
}

static int read_bytes(VMContext* vm, uint32_t len, uint8_t** out) {
    if (vm->pc > vm->length || (size_t)len > vm->length - vm->pc) return -1;
    *out = &vm->bytecode[vm->pc];
    vm->pc += len;
    return 0;
}

int vm_execute(VMContext* vm) {
    if (!vm || !vm->bytecode) return -1; /* Error */
    if (!vm->art_index) return -1;

    while (vm->pc < vm->length) {
        Opcode op = (Opcode)vm->bytecode[vm->pc++];

        /* Phase 1 transactional opcodes. */
        if (op >= OP_TX_BEGIN && op <= OP_TX_PUT_CRDT) {
            int trc = 0;
            switch (op) {
                case OP_TX_BEGIN: {
                    uint64_t tx_id = 0, origin = 0, schema = 0;
                    uint32_t consistency = 0;
                    if (read_u64(vm, &tx_id) != 0 || read_u64(vm, &origin) != 0 ||
                        read_u32(vm, &consistency) != 0 || read_u64(vm, &schema) != 0) return -5;
                    trc = tx_begin(&vm->tx, vm->art_index, tx_id, origin, consistency, schema);
                    if (trc == 0 && vm->timeline_active) {
                        /* Timeline txs validate against
                         * the timeline snapshot, not the
                         * moving main-head commit_seq. */
                        vm->tx.snapshot_seq = vm->timeline_snapshot_seq;
                    }
                    break;
                }
                case OP_TX_COMMIT: {
                    if (vm->timeline_active) {
                        /* Phase D: stage, do NOT apply.
                         * UNIQUE is re-checked against
                         * the timeline base (write set +
                         * ART, NOT the tx projection);
                         * a failure aborts the tx and
                         * stages nothing. On success the
                         * tx writes merge into the
                         * timeline write set in order
                         * (vm_timeline_stage merges
                         * last-write-wins per key), and
                         * the tx is cleared WITHOUT an
                         * arena rewind: tx_abort would
                         * rewind past the stage copies
                         * just allocated, dangling them.
                         * The tx scratch becomes bump
                         * waste, exactly like merged tx
                         * writes; the next BEGIN resets
                         * the checkpoint past it. */
                        uint32_t wi;
                        trc = 0;
                        for (wi = 0; wi < vm->tx.n_writes; wi++) {
                            TxWrite* tw = &vm->tx.writes[wi];
                            if (tw->op == TX_OP_PUT && (tw->flags & TX_WRITE_UNIQUE)) {
                                uint8_t* bv = NULL;
                                uint32_t blen = 0;
                                int brc = vm_timeline_get_layered(vm, tw->key, tw->key_len, &bv, &blen);
                                if (brc == 0) { trc = -9; break; }
                                if (brc != -1) { trc = brc; break; }
                            }
                        }
                        if (trc == 0) {
                            for (wi = 0; wi < vm->tx.n_writes; wi++) {
                                TxWrite* tw = &vm->tx.writes[wi];
                                int src = vm_timeline_stage(vm, tw->key, tw->key_len,
                                                            tw->val, tw->val_len,
                                                            tw->op == TX_OP_DEL, tw->timestamp);
                                if (src != 0) { trc = src; break; }
                            }
                        }
                        if (trc != 0) {
                            tx_abort(&vm->tx, vm->art_index);
                        } else {
                            vm->tx.active = 0;
                            vm->tx.n_writes = 0;
                            vm->tx.n_reads = 0;
                            vm->tx.n_asserts = 0;
                        }
                    } else {
                        trc = tx_commit(&vm->tx, vm->art_index, vm->storage);
                    }
                    break;
                }
                case OP_TX_ABORT: {
                    trc = tx_abort(&vm->tx, vm->art_index);
                    break;
                }
                case OP_TX_PUT:
                case OP_TX_PUT_CRDT: {
                    uint64_t timestamp = 0;
                    uint8_t flags = 0;
                    if (op == OP_TX_PUT_CRDT && read_u64(vm, &timestamp) != 0) return -5;
                    if (read_u8(vm, &flags) != 0) return -5;
                    uint32_t key_len = 0, val_len = 0;
                    uint8_t* key = NULL;
                    uint8_t* val = NULL;
                    if (read_u32(vm, &key_len) != 0) return -5;
                    if (read_bytes(vm, key_len, &key) != 0) return -5;
                    if (read_u32(vm, &val_len) != 0) return -5;
                    if (read_bytes(vm, val_len, &val) != 0) return -5;
                    trc = tx_put(&vm->tx, vm->art_index, key, key_len, val, val_len, flags, timestamp);
                    break;
                }
                case OP_TX_DEL: {
                    uint32_t key_len = 0;
                    uint8_t* key = NULL;
                    if (read_u32(vm, &key_len) != 0) return -5;
                    if (read_bytes(vm, key_len, &key) != 0) return -5;
                    trc = tx_del(&vm->tx, vm->art_index, key, key_len);
                    break;
                }
                case OP_TX_GET: {
                    uint32_t key_len = 0;
                    uint8_t* key = NULL;
                    if (read_u32(vm, &key_len) != 0) return -5;
                    if (read_bytes(vm, key_len, &key) != 0) return -5;
                    uint8_t* out_val = NULL;
                    uint32_t out_val_len = 0;
                    if (vm->timeline_active) {
                        /* Tx writes first, then the
                         * timeline layered view (never
                         * the main head via tx_get). */
                        uint32_t wi;
                        int found = 0;
                        trc = -1;
                        for (wi = vm->tx.n_writes; wi > 0; wi--) {
                            TxWrite* tw = &vm->tx.writes[wi - 1];
                            if (vm_timeline_key_eq(tw->key, tw->key_len, key, key_len)) {
                                if (tw->op == TX_OP_PUT) {
                                    out_val = (uint8_t*)tw->val;
                                    out_val_len = tw->val_len;
                                    trc = 0;
                                } else {
                                    trc = -1;
                                }
                                found = 1;
                                break;
                            }
                        }
                        if (!found) {
                            trc = vm_timeline_get_layered(vm, key, key_len, &out_val, &out_val_len);
                        }
                    } else {
                        trc = tx_get(&vm->tx, vm->art_index, key, key_len, &out_val, &out_val_len);
                    }
                    if (trc == 0) {
                        vm->response_ptr = out_val;
                        vm->response_len = out_val_len;
                        trc = 0;
                    } else if (trc == -1) {
                        vm->response_ptr = NULL;
                        vm->response_len = 0;
                        trc = 0; /* not found is not an error */
                    }
                    break;
                }
                case OP_TX_ASSERT: {
                    uint8_t kind = 0;
                    uint32_t key_len = 0, val_len = 0;
                    uint8_t* key = NULL;
                    uint8_t* val = NULL;
                    if (read_u8(vm, &kind) != 0) return -5;
                    if (read_u32(vm, &key_len) != 0) return -5;
                    if (read_bytes(vm, key_len, &key) != 0) return -5;
                    if (read_u32(vm, &val_len) != 0) return -5;
                    if (read_bytes(vm, val_len, &val) != 0) return -5;
                    if (vm->timeline_active) {
                        /* Assert at assert time against
                         * tx writes over the timeline
                         * layered view. (tx_assert would
                         * read the main head.) */
                        uint8_t* bv = NULL;
                        uint32_t blen = 0;
                        uint32_t wi;
                        int found = 0;
                        int brc = -1;
                        for (wi = vm->tx.n_writes; wi > 0; wi--) {
                            TxWrite* tw = &vm->tx.writes[wi - 1];
                            if (vm_timeline_key_eq(tw->key, tw->key_len, key, key_len)) {
                                if (tw->op == TX_OP_PUT) {
                                    bv = (uint8_t*)tw->val;
                                    blen = tw->val_len;
                                    brc = 0;
                                } else {
                                    brc = -1;
                                }
                                found = 1;
                                break;
                            }
                        }
                        if (!found) brc = vm_timeline_get_layered(vm, key, key_len, &bv, &blen);
                        trc = 0;
                        if (kind == 0 && brc == 0) trc = -9;
                        else if (kind == 1 && brc != 0) trc = -9;
                        else if (kind == 2) {
                            uint32_t j;
                            if (brc != 0 || blen != val_len) trc = -9;
                            else {
                                for (j = 0; j < val_len; j++) if (bv[j] != val[j]) { trc = -9; break; }
                            }
                        } else if (kind > 2) trc = -5;
                    } else {
                        trc = tx_assert(&vm->tx, vm->art_index, kind, key, key_len, val, val_len);
                    }
                    break;
                }
                default:
                    return -2;
            }
            if (trc != 0) return trc;
            continue;
        }

        /* Legacy ops inside an open transaction are a protocol error:
         * a transaction's writes must all go through the private set,
         * or "atomic" would be a lie told about a mixed program. */
        if (vm->tx.active) return -7;

        switch (op) {

            case OP_PUT:
            case OP_PUT_REMOTE:
            case OP_PUT_CRDT: {
                uint64_t timestamp = 0;
                if (op == OP_PUT_CRDT) {
                    if (read_u64(vm, &timestamp) != 0) return -5; /* malformed */
                }

                uint32_t key_len = 0;
                uint8_t* key = NULL;
                uint32_t val_len = 0;
                uint8_t* val = NULL;
                if (read_u32(vm, &key_len) != 0) return -5;
                if (read_bytes(vm, key_len, &key) != 0) return -5;
                if (read_u32(vm, &val_len) != 0) return -5;
                if (read_bytes(vm, val_len, &val) != 0) return -5;

                if (vm->timeline_active) {
                    /* Phase D: stage only. No WAL, no
                     * ART, no sync — the main head is
                     * untouched by timeline writes. */
                    int src = vm_timeline_stage(vm, key, key_len, val, val_len, 0, timestamp);
                    if (src != 0) return src;
                    break;
                }

                if (wal_is_recorded_op(op)) {
                    /* Unified legacy contract, steps (1)-(5):
                     * reserve, capture pre-op state, append WAL,
                     * apply ART, sync. Any failure restores ART
                     * and truncates WAL, then returns -6/-3. */
                    if (wal_reserve(vm, op, key_len, val_len) != 0) return -6; /* WAL full */
                    LegacyUndo undo;
                    legacy_capture(vm, key, key_len, &undo);
                    int pre_len = -1;
                    int pre_len_valid = 0;
                    if (vm->storage && vm->storage->log_length) {
                        pre_len = vm->storage->log_length(vm->storage);
                        pre_len_valid = pre_len >= 0;
                    }
                    if (append_to_storage(vm, op, timestamp, key, key_len, val, val_len) != 0) {
                        legacy_rollback_wal(vm, pre_len_valid, pre_len);
                        return -6; /* WAL append failed, ART untouched */
                    }
                    if (art_insert(vm->art_index, key, key_len, val, val_len, timestamp) != 0) {
                        legacy_restore(vm, key, key_len, &undo);
                        legacy_rollback_wal(vm, pre_len_valid, pre_len);
                        return -3; /* OOM or insert error, op undone */
                    }
                    if (legacy_sync(vm) != 0) {
                        legacy_restore(vm, key, key_len, &undo);
                        legacy_rollback_wal(vm, pre_len_valid, pre_len);
                        return -6; /* sync failed, op undone */
                    }
                } else {
                    /* REMOTE ops: WAL-free by design, no sync here. */
                    if (art_insert(vm->art_index, key, key_len, val, val_len, timestamp) != 0) {
                        return -3;
                    }
                }
                break;
            }
            case OP_GET: {
                uint32_t key_len = 0;
                uint8_t* key = NULL;
                if (read_u32(vm, &key_len) != 0) return -5;
                if (read_bytes(vm, key_len, &key) != 0) return -5;

                uint8_t* out_val = NULL;
                uint32_t out_val_len = 0;

                if (vm->timeline_active) {
                    int lrc = vm_timeline_get_layered(vm, key, key_len, &out_val, &out_val_len);
                    if (lrc == -11) return -11;
                    if (lrc == 0) {
                        vm->response_ptr = out_val;
                        vm->response_len = out_val_len;
                    } else {
                        vm->response_ptr = NULL;
                        vm->response_len = 0;
                    }
                    break;
                }

                if (art_search(vm->art_index, key, key_len, &out_val, &out_val_len) == 0) {
                    vm->response_ptr = out_val;
                    vm->response_len = out_val_len;
                } else {
                    vm->response_ptr = NULL;
                    vm->response_len = 0;
                }
                break;
            }

            case OP_DEL:
            case OP_DEL_REMOTE: {
                uint32_t key_len = 0;
                uint8_t* key = NULL;
                if (read_u32(vm, &key_len) != 0) return -5;
                if (read_bytes(vm, key_len, &key) != 0) return -5;

                if (vm->timeline_active) {
                    /* Phase D: stage a tombstone even
                     * for an absent key, so a base
                     * value cannot resurface — the
                     * segdb_delete_absent rule, at VM
                     * level. No WAL, no ART. */
                    int src = vm_timeline_stage(vm, key, key_len, NULL, 0, 1, 0);
                    if (src != 0) return src;
                    break;
                }

                if (op == OP_DEL) {
                    if (wal_reserve(vm, op, key_len, 0) != 0) return -6;
                    /* Absent key: preserved behaviour — no WAL
                     * record, no error (art_delete non-zero is
                     * swallowed, as before). The existence check
                     * must come before the WAL append, or an absent
                     * delete would leave a spurious record. */
                    {
                        ARTNode* term = art_find_terminal(vm->art_index, key, key_len);
                        if (!term || !term->is_end) break;
                    }
                    LegacyUndo undo;
                    legacy_capture(vm, key, key_len, &undo);
                    int pre_len = -1;
                    int pre_len_valid = 0;
                    if (vm->storage && vm->storage->log_length) {
                        pre_len = vm->storage->log_length(vm->storage);
                        pre_len_valid = pre_len >= 0;
                    }
                    if (append_to_storage(vm, OP_DEL, 0, key, key_len, NULL, 0) != 0) {
                        legacy_rollback_wal(vm, pre_len_valid, pre_len);
                        return -6;
                    }
                    {
                        int drc = art_delete(vm->art_index, key, key_len);
                        if (drc != 0) {
                            /* Unreachable after the existence check
                             * above, but keep the contract: a failed
                             * op leaves ART and WAL as before. */
                            legacy_rollback_wal(vm, pre_len_valid, pre_len);
                            return drc;
                        }
                    }
                    if (legacy_sync(vm) != 0) {
                        legacy_restore(vm, key, key_len, &undo);
                        legacy_rollback_wal(vm, pre_len_valid, pre_len);
                        return -6;
                    }
                } else {
                    /* DEL_REMOTE: WAL-free by design, no sync here. */
                    (void)art_delete(vm->art_index, key, key_len);
                }
                break;
            }

            case OP_SCAN: {
                uint32_t prefix_len = 0;
                uint8_t* prefix = NULL;
                uint32_t out_buffer_size = 0;
                if (read_u32(vm, &prefix_len) != 0) return -5;
                if (read_bytes(vm, prefix_len, &prefix) != 0) return -5;
                if (read_u32(vm, &out_buffer_size) != 0) return -5;

                if (vm->timeline_active) {
                    /* Phase D: fail closed. The ART has
                     * no snapshot scan, and a main-head
                     * art_scan under a timeline name
                     * would violate isolation. Timeline
                     * scans go through the Go handle. */
                    (void)prefix;
                    (void)out_buffer_size;
                    return -11;
                }

                /* The scan response lives in a static 64 KiB buffer in
                 * Wasm linear memory. That is a hard cap, not a silent
                 * truncation: art_scan returns -1 if the live result
                 * does not fit, and the VM returns -4. A host-provided
                 * output buffer (step 3) will remove this cap. */
                static uint8_t scan_response_buffer[65536]; /* 64KB max scan response */

                uint8_t* out_buffer = scan_response_buffer;
                if (out_buffer_size > sizeof(scan_response_buffer)) {
                    out_buffer_size = sizeof(scan_response_buffer);
                }

                int written = art_scan(vm->art_index, prefix, prefix_len, out_buffer, out_buffer_size);

                if (written < 0) return -4; /* Scan error */

                vm->response_ptr = out_buffer;
                vm->response_len = written;

                break;
            }

            case OP_EXIT: {
                if (vm->tx.active) {
                    /* Incomplete transaction at end of program: it
                     * is discarded, never committed, and the program
                     * reports a protocol error so the caller cannot
                     * mistake silence for success. */
                    tx_abort(&vm->tx, vm->art_index);
                    return -7;
                }
                return 0; /* Success */
            }

            default:
                return -2; /* Unknown opcode error */
        }
    }

    if (vm->tx.active) {
        /* Program ran out without EXIT or COMMIT: same rule as EXIT
         * with an open transaction. */
        tx_abort(&vm->tx, vm->art_index);
        return -7;
    }

    return 0;
}
