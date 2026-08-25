#include <stdio.h>
#include <assert.h>
#include <string.h>
#include "yowza_db.h"
#include "tx.h"

static void tx_w32(uint8_t* buf, size_t* pc, uint32_t v) {
    buf[(*pc)++] = v & 0xFF; buf[(*pc)++] = (v >> 8) & 0xFF;
    buf[(*pc)++] = (v >> 16) & 0xFF; buf[(*pc)++] = (v >> 24) & 0xFF;
}
static void tx_w64(uint8_t* buf, size_t* pc, uint64_t v) {
    for (int i = 0; i < 8; i++) buf[(*pc)++] = (uint8_t)((v >> (i * 8)) & 0xFF);
}
static void tx_wbytes(uint8_t* buf, size_t* pc, const void* data, size_t len) {
    const uint8_t* d = (const uint8_t*)data;
    for (size_t i = 0; i < len; i++) buf[(*pc)++] = d[i];
}
static void tx_wkv(uint8_t* buf, size_t* pc, const char* key, const char* val) {
    tx_w32(buf, pc, (uint32_t)strlen(key));
    tx_wbytes(buf, pc, key, strlen(key));
    tx_w32(buf, pc, (uint32_t)strlen(val));
    tx_wbytes(buf, pc, val, strlen(val));
}

static void tx_mkdb(YowzaDB* db, uint8_t* mem, size_t mem_size, uint8_t* wal, uint32_t wal_size) {
    StorageDriver st = storage_memory_create(wal, wal_size);
    assert(yowza_db_init(db, mem, mem_size, st) == 0);
}

/* Returns 1 if key exists with exactly `want`, else 0. */
static int tx_has(YowzaDB* db, const char* key, const char* want) {
    void* val = NULL;
    size_t len = 0;
    if (yowza_db_get(db, key, &val, &len) != 0) return 0;
    return len == strlen(want) && memcmp(val, want, len) == 0;
}
static int tx_missing(YowzaDB* db, const char* key) {
    void* val = NULL;
    size_t len = 0;
    return yowza_db_get(db, key, &val, &len) != 0;
}

void test_tx_commit_atomic_multiop() {
    printf("Running test_tx_commit_atomic_multiop...\n");
    uint8_t mem[16384], wal[16384];
    YowzaDB db; tx_mkdb(&db, mem, sizeof(mem), wal, sizeof(wal));
    /* The PutWithIndex shape: primary + index + reverse, one tx. */
    assert(yowza_db_tx_begin(&db, 100, 1, TX_CONSISTENCY_LOCAL, 1) == 0);
    assert(yowza_db_tx_put(&db, "user:1", "Alice", 5, 0) == 0);
    assert(yowza_db_tx_put(&db, "idx:email:alice@x:user:1", "user:1", 6, 0) == 0);
    assert(yowza_db_tx_put(&db, "idxrev:user:1:email:alice@x", "idx:email:alice@x:user:1", 24, 0) == 0);
    /* Nothing visible before commit. */
    assert(tx_missing(&db, "user:1"));
    assert(yowza_db_tx_commit(&db) == 0);
    assert(tx_has(&db, "user:1", "Alice"));
    assert(tx_has(&db, "idx:email:alice@x:user:1", "user:1"));
    assert(tx_has(&db, "idxrev:user:1:email:alice@x", "idx:email:alice@x:user:1"));
    /* The WAL now starts with a transaction frame, not per-op records. */
    uint32_t wal_len = storage_memory_get_offset(&db.storage);
    assert(wal_len > 0 && wal[0] == 0xF0 && wal[1] == 'Y' && wal[2] == 'W' && wal[3] == 'T' && wal[4] == 'X');
    if (db.storage.close) db.storage.close(&db.storage);
    printf("Passed.\n");
}

void test_tx_abort_invisible_and_preexisting_intact() {
    printf("Running test_tx_abort_invisible_and_preexisting_intact...\n");
    uint8_t mem[16384], wal[16384];
    YowzaDB db; tx_mkdb(&db, mem, sizeof(mem), wal, sizeof(wal));
    assert(yowza_db_put(&db, "keep", "K", 1) == 0);
    assert(yowza_db_tx_begin(&db, 101, 1, TX_CONSISTENCY_LOCAL, 1) == 0);
    assert(yowza_db_tx_put(&db, "new1", "v1", 2, 0) == 0);
    assert(yowza_db_tx_put(&db, "keep", "CHANGED", 7, 0) == 0);
    assert(yowza_db_tx_delete(&db, "keep") == 0); /* last write wins in the set */
    assert(yowza_db_tx_abort(&db) == 0);
    assert(tx_missing(&db, "new1"));
    assert(tx_has(&db, "keep", "K"));
    if (db.storage.close) db.storage.close(&db.storage);
    printf("Passed.\n");
}

void test_tx_read_your_writes_snapshot_invisible() {
    printf("Running test_tx_read_your_writes_snapshot_invisible...\n");
    uint8_t mem[16384], wal[16384];
    YowzaDB db; tx_mkdb(&db, mem, sizeof(mem), wal, sizeof(wal));
    assert(yowza_db_put(&db, "k", "old", 3) == 0);
    assert(yowza_db_tx_begin(&db, 102, 1, TX_CONSISTENCY_LOCAL, 1) == 0);
    assert(yowza_db_tx_put(&db, "k", "new", 3, 0) == 0);
    void* val = NULL; size_t len = 0;
    assert(yowza_db_tx_get(&db, "k", &val, &len) == 0 && len == 3 && memcmp(val, "new", 3) == 0);
    /* Committed readers still see the old value while the tx is open. */
    assert(tx_has(&db, "k", "old"));
    assert(yowza_db_tx_commit(&db) == 0);
    assert(tx_has(&db, "k", "new"));
    if (db.storage.close) db.storage.close(&db.storage);
    printf("Passed.\n");
}

void test_tx_conflict_aborts_and_writer_survives() {
    printf("Running test_tx_conflict_aborts_and_writer_survives...\n");
    uint8_t mem[16384], wal[16384];
    YowzaDB db; tx_mkdb(&db, mem, sizeof(mem), wal, sizeof(wal));
    assert(yowza_db_put(&db, "k", "v0", 2) == 0);
    assert(yowza_db_tx_begin(&db, 103, 1, TX_CONSISTENCY_LOCAL, 1) == 0);
    void* val = NULL; size_t len = 0;
    assert(yowza_db_tx_get(&db, "k", &val, &len) == 0); /* records version */
    assert(yowza_db_tx_put(&db, "other", "mine", 4, 0) == 0);
    /* Simulate a concurrent committed writer directly on the ART
     * (the C API guard makes yowza_db_put return -7 while a tx is
     * open, so the test reaches past it deliberately; this is the
     * commit_seq-moved case in tx_abort, where rewind must NOT run). */
    assert(art_insert(&db.index, (const uint8_t*)"k", 1, (const uint8_t*)"v1", 2, 0) == 0);
    assert(yowza_db_tx_commit(&db) == TX_ERR_CONFLICT);
    assert(tx_has(&db, "k", "v1"));       /* writer survives */
    assert(tx_missing(&db, "other"));     /* loser's write is absent */
    if (db.storage.close) db.storage.close(&db.storage);
    printf("Passed.\n");
}

void test_tx_unique_and_assert_failures_roll_back() {
    printf("Running test_tx_unique_and_assert_failures_roll_back...\n");
    uint8_t mem[16384], wal[16384];
    YowzaDB db; tx_mkdb(&db, mem, sizeof(mem), wal, sizeof(wal));
    assert(yowza_db_put(&db, "u1", "taken", 5) == 0);
    /* Uniqueness: a UNIQUE put on an existing committed key fails. */
    assert(yowza_db_tx_begin(&db, 104, 1, TX_CONSISTENCY_LOCAL, 1) == 0);
    assert(yowza_db_tx_put(&db, "u1", "other", 5, TX_WRITE_UNIQUE) == 0);
    assert(yowza_db_tx_put(&db, "side", "s", 1, 0) == 0);
    assert(yowza_db_tx_commit(&db) == TX_ERR_ASSERT);
    assert(tx_has(&db, "u1", "taken"));
    assert(tx_missing(&db, "side"));
    /* ASSERT value-equals against a wrong expectation fails too. */
    assert(yowza_db_tx_begin(&db, 105, 1, TX_CONSISTENCY_LOCAL, 1) == 0);
    assert(yowza_db_tx_assert_raw(&db, TX_ASSERT_VALUE_EQUALS, "u1", 2, "wrong", 5) == 0);
    assert(yowza_db_tx_put(&db, "side2", "s", 1, 0) == 0);
    assert(yowza_db_tx_commit(&db) == TX_ERR_ASSERT);
    assert(tx_missing(&db, "side2"));
    /* ...while a correct assert commits. */
    assert(yowza_db_tx_begin(&db, 106, 1, TX_CONSISTENCY_LOCAL, 1) == 0);
    assert(yowza_db_tx_assert_raw(&db, TX_ASSERT_VALUE_EQUALS, "u1", 2, "taken", 5) == 0);
    assert(yowza_db_tx_put(&db, "side3", "s", 1, 0) == 0);
    assert(yowza_db_tx_commit(&db) == 0);
    assert(tx_has(&db, "side3", "s"));
    if (db.storage.close) db.storage.close(&db.storage);
    printf("Passed.\n");
}

void test_tx_vm_program_incomplete_and_protocol() {
    printf("Running test_tx_vm_program_incomplete_and_protocol...\n");
    uint8_t mem[16384], wal[16384];
    YowzaDB db; tx_mkdb(&db, mem, sizeof(mem), wal, sizeof(wal));
    uint8_t code[1024];
    VMContext vm;
    size_t pc;
    /* BEGIN + TX_PUT + EXIT (no COMMIT): -7, nothing visible. */
    pc = 0;
    code[pc++] = OP_TX_BEGIN;
    tx_w64(code, &pc, 200); tx_w64(code, &pc, 1); tx_w32(code, &pc, 0); tx_w64(code, &pc, 1);
    code[pc++] = OP_TX_PUT; code[pc++] = 0; tx_wkv(code, &pc, "pk", "pv");
    code[pc++] = OP_EXIT;
    vm_init(&vm, &db.index, code, pc, &db.storage);
    assert(vm_execute(&vm) == -7);
    assert(tx_missing(&db, "pk"));
    /* COMMIT without BEGIN: -7. */
    pc = 0;
    code[pc++] = OP_TX_COMMIT; code[pc++] = OP_EXIT;
    vm_init(&vm, &db.index, code, pc, &db.storage);
    assert(vm_execute(&vm) == -7);
    /* Legacy PUT inside a transaction: -7, nothing visible. */
    pc = 0;
    code[pc++] = OP_TX_BEGIN;
    tx_w64(code, &pc, 201); tx_w64(code, &pc, 1); tx_w32(code, &pc, 0); tx_w64(code, &pc, 1);
    code[pc++] = OP_PUT; tx_wkv(code, &pc, "lk", "lv");
    code[pc++] = OP_EXIT;
    vm_init(&vm, &db.index, code, pc, &db.storage);
    assert(vm_execute(&vm) == -7);
    assert(tx_missing(&db, "lk"));
    /* A complete program commits, with a TX_GET read-your-writes. */
    pc = 0;
    code[pc++] = OP_TX_BEGIN;
    tx_w64(code, &pc, 202); tx_w64(code, &pc, 1); tx_w32(code, &pc, 0); tx_w64(code, &pc, 1);
    code[pc++] = OP_TX_PUT; code[pc++] = 0; tx_wkv(code, &pc, "vk", "vv");
    code[pc++] = OP_TX_GET;
    tx_w32(code, &pc, 2); tx_wbytes(code, &pc, "vk", 2);
    code[pc++] = OP_TX_COMMIT;
    code[pc++] = OP_EXIT;
    vm_init(&vm, &db.index, code, pc, &db.storage);
    assert(vm_execute(&vm) == 0);
    assert(vm.response_ptr != NULL && vm.response_len == 2 && memcmp(vm.response_ptr, "vv", 2) == 0);
    assert(tx_has(&db, "vk", "vv"));
    /* Empty transaction commits with no frame appended. */
    uint32_t wal_before = storage_memory_get_offset(&db.storage);
    pc = 0;
    code[pc++] = OP_TX_BEGIN;
    tx_w64(code, &pc, 203); tx_w64(code, &pc, 1); tx_w32(code, &pc, 0); tx_w64(code, &pc, 1);
    code[pc++] = OP_TX_COMMIT; code[pc++] = OP_EXIT;
    vm_init(&vm, &db.index, code, pc, &db.storage);
    assert(vm_execute(&vm) == 0);
    assert(storage_memory_get_offset(&db.storage) == wal_before);
    if (db.storage.close) db.storage.close(&db.storage);
    printf("Passed.\n");
}

void test_tx_apply_oom_undoes_applied_ops() {
    printf("Running test_tx_apply_oom_undoes_applied_ops...\n");
    /* Arena sized (see PHASE1_NOTES.md for the arithmetic) so that
     * write-set scratch fits, the first apply fits, and the third
     * apply OOMs: the first two applied ops must be undone. */
    uint8_t mem[1536], wal[16384];
    YowzaDB db; tx_mkdb(&db, mem, sizeof(mem), wal, sizeof(wal));
    assert(yowza_db_put(&db, "a", "A", 1) == 0);
    uint8_t v100[100]; memset(v100, 'x', sizeof(v100));
    uint8_t v300[300]; memset(v300, 'y', sizeof(v300));
    assert(yowza_db_tx_begin(&db, 107, 1, TX_CONSISTENCY_LOCAL, 1) == 0);
    assert(yowza_db_tx_put_raw(&db, "a", 1, v100, sizeof(v100), 0) == 0);
    assert(yowza_db_tx_put_raw(&db, "b", 1, v300, sizeof(v300), 0) == 0);
    assert(yowza_db_tx_put_raw(&db, "c", 1, v300, sizeof(v300), 0) == 0);
    int rc = yowza_db_tx_commit(&db);
    assert(rc == TX_ERR_OOM);
    assert(tx_has(&db, "a", "A")); /* first applied op undone */
    assert(tx_missing(&db, "b"));  /* second applied op undone */
    assert(tx_missing(&db, "c"));  /* failed op never visible */
    if (db.storage.close) db.storage.close(&db.storage);
    printf("Passed.\n");
}

void test_tx_wal_replay_complete_vs_incomplete() {
    printf("Running test_tx_wal_replay_complete_vs_incomplete...\n");
    uint8_t mem[16384], wal[16384];
    YowzaDB db; tx_mkdb(&db, mem, sizeof(mem), wal, sizeof(wal));
    assert(yowza_db_put(&db, "legacy", "L", 1) == 0);
    assert(yowza_db_tx_begin(&db, 108, 1, TX_CONSISTENCY_LOCAL, 1) == 0);
    assert(yowza_db_tx_put(&db, "txk", "txv", 3, 0) == 0);
    assert(yowza_db_tx_commit(&db) == 0);
    uint32_t wal_len = storage_memory_get_offset(&db.storage);
    /* Replay into a fresh DB: legacy record + one frame. */
    uint8_t mem2[16384], wal2[16384];
    YowzaDB db2; tx_mkdb(&db2, mem2, sizeof(mem2), wal2, sizeof(wal2));
    uint32_t applied = 0;
    assert(tx_replay_wal(&db2.index, wal, wal_len, &applied) == 0);
    assert(applied == 1);
    assert(tx_has(&db2, "legacy", "L"));
    assert(tx_has(&db2, "txk", "txv"));
    if (db2.storage.close) db2.storage.close(&db2.storage);
    /* A second DB that also received a truncated frame tail: the
     * truncated frame must not apply, and must not break the rest
     * before it (there is nothing after it by construction). */
    uint8_t mem3[16384], wal3[16384];
    YowzaDB db3; tx_mkdb(&db3, mem3, sizeof(mem3), wal3, sizeof(wal3));
    assert(yowza_db_tx_begin(&db3, 109, 1, TX_CONSISTENCY_LOCAL, 1) == 0);
    assert(yowza_db_tx_put(&db3, "ghost", "g", 1, 0) == 0);
    assert(yowza_db_tx_commit(&db3) == 0);
    uint32_t wal3_len = storage_memory_get_offset(&db3.storage);
    uint8_t combined[16384];
    memcpy(combined, wal, wal_len);
    memcpy(combined + wal_len, wal3, 20); /* truncated frame head only */
    uint8_t mem4[16384], wal4[16384];
    YowzaDB db4; tx_mkdb(&db4, mem4, sizeof(mem4), wal4, sizeof(wal4));
    applied = 0;
    assert(tx_replay_wal(&db4.index, combined, wal_len + 20, &applied) == 0);
    assert(applied == 1);
    assert(tx_has(&db4, "txk", "txv"));
    assert(tx_missing(&db4, "ghost"));
    if (db4.storage.close) db4.storage.close(&db4.storage);
    (void)wal3_len;
    /* Corrupt frame checksum: flip a payload byte inside the frame. */
    uint8_t corrupt[16384];
    memcpy(corrupt, wal, wal_len);
    corrupt[wal_len - 12] ^= 0xFF; /* inside the frame, before its checksum */
    uint8_t mem5[16384], wal5[16384];
    YowzaDB db5; tx_mkdb(&db5, mem5, sizeof(mem5), wal5, sizeof(wal5));
    applied = 99;
    assert(tx_replay_wal(&db5.index, corrupt, wal_len, &applied) == 0);
    assert(applied == 0); /* frame rejected whole */
    assert(tx_has(&db5, "legacy", "L")); /* record before the frame survives */
    assert(tx_missing(&db5, "txk"));
    if (db5.storage.close) db5.storage.close(&db5.storage);
    if (db.storage.close) db.storage.close(&db.storage);
    if (db3.storage.close) db3.storage.close(&db3.storage);
    printf("Passed.\n");
}

void test_tx_assert_checks_committed_state_not_projected() {
    printf("Running test_tx_assert_checks_committed_state_not_projected...\n");
    uint8_t mem[16384], wal[16384];
    YowzaDB db; tx_mkdb(&db, mem, sizeof(mem), wal, sizeof(wal));
    assert(yowza_db_put(&db, "k", "old", 3) == 0);
    /* Assert on the committed value, then overwrite it in the same
     * tx: the assert must PASS (it guards the pre-state), and the
     * new value must commit. Under projected-state semantics this
     * would spuriously fail. */
    assert(yowza_db_tx_begin(&db, 112, 1, TX_CONSISTENCY_LOCAL, 1) == 0);
    assert(yowza_db_tx_assert_raw(&db, TX_ASSERT_VALUE_EQUALS, "k", 1, "old", 3) == 0);
    assert(yowza_db_tx_put(&db, "k", "new", 3, 0) == 0);
    assert(yowza_db_tx_commit(&db) == 0);
    assert(tx_has(&db, "k", "new"));
    if (db.storage.close) db.storage.close(&db.storage);
    printf("Passed.\n");
}

/* ---- TX + HOT HISTORY hook tests (tx layer only; the SegDB
 * end-to-end tests live in test_tx_history.c) ---- */
static int th_calls;
static int th_fail_on; /* 0 = never fail; else fail that 1-based capture call */
static int th_rb_calls;
static uint32_t th_rb_last_count;
static char th_keys[8][16];
static char th_vals[8][16];
static uint64_t th_versions[8];
static int th_tombs[8];

static int th_capture(void* ctx, const uint8_t* key, uint32_t key_len,
                      uint64_t old_version, int tomb,
                      const uint8_t* val, uint32_t val_len, uint64_t crdt_ts) {
    (void)ctx; (void)crdt_ts;
    th_calls++;
    if (th_fail_on && th_calls == th_fail_on) return -3;
    if (th_calls <= 8) {
        uint32_t kl = key_len < 15 ? key_len : 15;
        uint32_t vl = val_len < 15 ? val_len : 15;
        memcpy(th_keys[th_calls - 1], key, kl); th_keys[th_calls - 1][kl] = '\0';
        if (val && vl > 0) memcpy(th_vals[th_calls - 1], val, vl);
        th_vals[th_calls - 1][vl] = '\0';
        th_versions[th_calls - 1] = old_version;
        th_tombs[th_calls - 1] = tomb;
    }
    return 0;
}
static int th_rollback(void* ctx, uint32_t count) {
    (void)ctx;
    th_rb_calls++;
    th_rb_last_count = count;
    return 0;
}
static void th_reset(void) {
    th_calls = 0; th_fail_on = 0; th_rb_calls = 0; th_rb_last_count = 0;
}

void test_tx_history_hook_captures_and_fail_closed() {
    printf("Running test_tx_history_hook_captures_and_fail_closed...\n");
    uint8_t mem[16384], wal[16384];
    YowzaDB db; tx_mkdb(&db, mem, sizeof(mem), wal, sizeof(wal));
    assert(yowza_db_put(&db, "k", "old", 3) == 0);     /* version 1 */
    assert(yowza_db_put(&db, "fresh", "F", 1) == 0);   /* version 2 */
    db.tx.history_capture = th_capture;
    db.tx.history_ctx = NULL;
    db.tx.history_rollback = th_rollback;
    /* Commit captures exactly the superseded versions (k, fresh),
     * not the brand-new key, with ART-local versions and values. */
    th_reset();
    assert(yowza_db_tx_begin(&db, 120, 1, TX_CONSISTENCY_LOCAL, 1) == 0);
    assert(yowza_db_tx_put(&db, "k", "new", 3, 0) == 0);
    assert(yowza_db_tx_put(&db, "fresh", "F2", 2, 0) == 0);
    assert(yowza_db_tx_put(&db, "brand", "B", 1, 0) == 0);
    assert(yowza_db_tx_commit(&db) == 0);
    assert(th_calls == 2 && th_rb_calls == 0);
    assert(strcmp(th_keys[0], "k") == 0 && strcmp(th_vals[0], "old") == 0 &&
           th_versions[0] == 1 && th_tombs[0] == 0);
    assert(strcmp(th_keys[1], "fresh") == 0 && strcmp(th_vals[1], "F") == 0 &&
           th_versions[1] == 2 && th_tombs[1] == 0);
    assert(tx_has(&db, "k", "new") && tx_has(&db, "fresh", "F2") && tx_has(&db, "brand", "B"));
    /* Capture failure on the FIRST key: nothing applied, no rollback
     * needed (nothing was captured yet). */
    th_reset(); th_fail_on = 1;
    assert(yowza_db_tx_begin(&db, 121, 1, TX_CONSISTENCY_LOCAL, 1) == 0);
    assert(yowza_db_tx_put(&db, "k", "new2", 4, 0) == 0);
    assert(yowza_db_tx_put(&db, "fresh", "F3", 2, 0) == 0);
    assert(yowza_db_tx_commit(&db) == TX_ERR_OOM);
    assert(th_calls == 1 && th_rb_calls == 0);
    assert(tx_has(&db, "k", "new") && tx_has(&db, "fresh", "F2"));
    assert(yowza_db_tx_active(&db) == 0);
    /* Capture failure on the SECOND key: the first capture is rolled
     * back (rollback called with count 1) and nothing is applied. */
    th_reset(); th_fail_on = 2;
    assert(yowza_db_tx_begin(&db, 122, 1, TX_CONSISTENCY_LOCAL, 1) == 0);
    assert(yowza_db_tx_put(&db, "k", "new2", 4, 0) == 0);
    assert(yowza_db_tx_put(&db, "fresh", "F3", 2, 0) == 0);
    assert(yowza_db_tx_commit(&db) == TX_ERR_OOM);
    assert(th_calls == 2 && th_rb_calls == 1 && th_rb_last_count == 1);
    assert(tx_has(&db, "k", "new") && tx_has(&db, "fresh", "F2"));
    assert(yowza_db_tx_active(&db) == 0);
    /* A tombstoned pre-image captures tomb = 1 with no value. */
    assert(yowza_db_delete(&db, "brand") == 0);
    th_reset();
    assert(yowza_db_tx_begin(&db, 123, 1, TX_CONSISTENCY_LOCAL, 1) == 0);
    assert(yowza_db_tx_put(&db, "brand", "B2", 2, 0) == 0);
    assert(yowza_db_tx_commit(&db) == 0);
    assert(th_calls == 1 && th_tombs[0] == 1 && th_vals[0][0] == '\0');
    assert(tx_has(&db, "brand", "B2"));
    if (db.storage.close) db.storage.close(&db.storage);
    printf("Passed.\n");
}

/* Failing-sync driver for the WAL-failure rollback test. */
typedef struct {
    uint8_t* buf;
    uint32_t cap;
    uint32_t off;
    int fail_sync;
} TxFailState;
static int txf_append(StorageDriver* self, const uint8_t* data, uint32_t len) {
    TxFailState* st = (TxFailState*)self->state;
    if (len > st->cap - st->off) return -1;
    memcpy(&st->buf[st->off], data, len);
    st->off += len;
    return (int)len;
}
static int txf_check_capacity(StorageDriver* self, uint32_t len) {
    TxFailState* st = (TxFailState*)self->state;
    return len <= st->cap - st->off ? 0 : -1;
}
static int txf_sync(StorageDriver* self) {
    return ((TxFailState*)self->state)->fail_sync ? -1 : 0;
}
static int txf_log_length(StorageDriver* self) {
    return (int)((TxFailState*)self->state)->off;
}
static int txf_truncate(StorageDriver* self, uint32_t len) {
    TxFailState* st = (TxFailState*)self->state;
    if (len > st->off) return -1;
    st->off = len;
    return 0;
}
static StorageDriver txf_driver(TxFailState* st) {
    StorageDriver d;
    memset(&d, 0, sizeof(d));
    d.state = st;
    d.append = txf_append;
    d.check_capacity = txf_check_capacity;
    d.sync = txf_sync;
    d.log_length = txf_log_length;
    d.truncate = txf_truncate;
    return d;
}

void test_tx_history_hook_wal_failure_rolls_back() {
    printf("Running test_tx_history_hook_wal_failure_rolls_back...\n");
    uint8_t mem[16384], wal[16384];
    TxFailState fs;
    YowzaDB db;
    memset(&fs, 0, sizeof(fs));
    fs.buf = wal; fs.cap = sizeof(wal);
    assert(yowza_db_init(&db, mem, sizeof(mem), txf_driver(&fs)) == 0);
    assert(yowza_db_put(&db, "k", "old", 3) == 0);
    assert(yowza_db_put(&db, "j", "j0", 2) == 0);
    db.tx.history_capture = th_capture;
    db.tx.history_ctx = NULL;
    db.tx.history_rollback = th_rollback;
    /* Sync fails only for this commit: the frame is truncated, the
     * apply is undone, AND both history captures are rolled back. */
    fs.fail_sync = 1;
    th_reset();
    assert(yowza_db_tx_begin(&db, 124, 1, TX_CONSISTENCY_LOCAL, 1) == 0);
    assert(yowza_db_tx_put(&db, "k", "new", 3, 0) == 0);
    assert(yowza_db_tx_put(&db, "j", "j1", 2, 0) == 0);
    assert(yowza_db_tx_commit(&db) == TX_ERR_WAL);
    assert(th_calls == 2 && th_rb_calls == 1 && th_rb_last_count == 2);
    assert(tx_has(&db, "k", "old") && tx_has(&db, "j", "j0"));
    assert(yowza_db_tx_active(&db) == 0);
    /* The DB is usable afterwards (commit_seq waste of one is safe). */
    fs.fail_sync = 0;
    assert(yowza_db_tx_begin(&db, 125, 1, TX_CONSISTENCY_LOCAL, 1) == 0);
    assert(yowza_db_tx_put(&db, "k", "new", 3, 0) == 0);
    assert(yowza_db_tx_commit(&db) == 0);
    assert(tx_has(&db, "k", "new"));
    printf("Passed.\n");
}

void test_tx_delete_atomic_with_index() {
    printf("Running test_tx_delete_atomic_with_index...\n");
    uint8_t mem[16384], wal[16384];
    YowzaDB db; tx_mkdb(&db, mem, sizeof(mem), wal, sizeof(wal));
    assert(yowza_db_tx_begin(&db, 110, 1, TX_CONSISTENCY_LOCAL, 1) == 0);
    assert(yowza_db_tx_put(&db, "user:1", "Alice", 5, 0) == 0);
    assert(yowza_db_tx_put(&db, "idx:email:alice@x:user:1", "user:1", 6, 0) == 0);
    assert(yowza_db_tx_put(&db, "idxrev:user:1:email:alice@x", "idx:email:alice@x:user:1", 24, 0) == 0);
    assert(yowza_db_tx_commit(&db) == 0);
    assert(yowza_db_tx_begin(&db, 111, 1, TX_CONSISTENCY_LOCAL, 1) == 0);
    assert(yowza_db_tx_delete(&db, "user:1") == 0);
    assert(yowza_db_tx_delete(&db, "idx:email:alice@x:user:1") == 0);
    assert(yowza_db_tx_delete(&db, "idxrev:user:1:email:alice@x") == 0);
    assert(yowza_db_tx_commit(&db) == 0);
    assert(tx_missing(&db, "user:1"));
    assert(tx_missing(&db, "idx:email:alice@x:user:1"));
    assert(tx_missing(&db, "idxrev:user:1:email:alice@x"));
    if (db.storage.close) db.storage.close(&db.storage);
    printf("Passed.\n");
}
