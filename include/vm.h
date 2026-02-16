#ifndef YOWZA_VM_H
#define YOWZA_VM_H

#include "arena.h" // Gives us standard sized types like uint8_t, size_t, etc.

// Define our lightweight Instruction Set Architecture (Opcodes)
typedef enum {
    OP_EXIT = 0x00,  // Terminate execution safely
    OP_PUT  = 0x01,  // Insert or update a Key-Value pair
    OP_GET  = 0x02,  // Retrieve a Value by Key
    OP_DEL  = 0x03,  // Delete a Key-Value pair
    OP_SCAN = 0x04,  // Scan all keys with a given prefix
    OP_PUT_REMOTE = 0x05, // Put without appending to WAL
    OP_DEL_REMOTE = 0x06, // Delete without appending to WAL
    OP_PUT_CRDT = 0x07, // Put with LWW Timestamp
    /* Phase 1 transactional opcodes (see tx.h / PHASE1_NOTES.md).
     * A transaction is one bytecode program: BEGIN ... TX_* ... COMMIT.
     * Legacy ops inside a transaction are rejected (-7); EXIT with an
     * open transaction discards it and returns -7 (incomplete tx). */
    OP_TX_BEGIN = 0x10, // [tx_id u64][origin u64][consistency u32][schema u64]
    OP_TX_COMMIT = 0x11, // validate + apply + one WAL frame + sync
    OP_TX_ABORT = 0x12,  // discard the private write set
    OP_TX_PUT = 0x13,    // [flags u8][key_len u32][key][val_len u32][val]
    OP_TX_DEL = 0x14,    // [key_len u32][key]
    OP_TX_GET = 0x15,    // [key_len u32][key] (read-your-writes + read set)
    OP_TX_ASSERT = 0x16, // [kind u8][key_len u32][key][val_len u32][val]
    OP_TX_PUT_CRDT = 0x17 // [timestamp u64][flags u8][key_len u32][key][val_len u32][val]
} Opcode;

#include "art.h"
#include "storage.h"
#include "tx.h"

/* Phase D timeline write-set entry (defined before
 * VMContext, which embeds an array of them). key/val
 * point to arena copies made by vm_timeline_stage
 * (valid while the ART arena lives); val is NULL for a
 * delete. */
#define VM_TIMELINE_MAX_WRITES 128
typedef struct {
    const uint8_t* key;
    uint32_t key_len;
    const uint8_t* val; /* NULL for a delete */
    uint32_t val_len;
    int deleted;
    uint64_t timestamp;
} VMTimelineWrite;

// VM Context to hold state during execution
typedef struct {
    uint8_t* bytecode; // Pointer to the raw bytecode received from Go
    size_t length;     // Total length of the bytecode buffer
    size_t pc;         // Program Counter (current offset)

    ART* art_index;    // Pointer to the initialized ART index

    // For returning results back to the caller (e.g., OP_GET results)
    uint8_t* response_ptr;
    uint32_t response_len;

    // Storage driver for persistent operations (WAL/Disk)
    StorageDriver* storage;

    // Phase 1 transaction state. Lives for one vm_execute call's
    // program (vm_init resets it): a transaction may not span two
    // yowza_execute calls in the Wasm path. The native C API
    // (yowza_db_tx_*) keeps its TxState in YowzaDB instead, so it
    // CAN span calls. Both use the same tx_* functions.
    TxState tx;

    /* Phase D — execution timeline handle (native C API).
     * When timeline_active is set (by vm_set_timeline, AFTER
     * vm_init, which clears it), the program executes in
     * that timeline's context instead of the main head:
     *   - OP_PUT/DEL (and REMOTE/CRDT variants) stage into
     *     the timeline write set ONLY: no ART mutation, no
     *     WAL record, no sync. The write set is the
     *     timeline's overlay; the host drains it with
     *     vm_timeline_write_at and applies it through the
     *     unified layer (Go TimelineHandle / SegDB
     *     transaction), exactly like the Go TimelineVM.
     *   - OP_GET reads the write set first, then the ART
     *     filtered by timeline_snapshot_seq: a live node
     *     whose version is at most the snapshot is the
     *     base value; a live node newer than the snapshot
     *     is AMBIGUOUS in the ART-only layer (it may have
     *     been created after the snapshot, in which case
     *     the timeline answer is "absent", or overwritten,
     *     in which case the old value is lost to this
     *     layer) — so it fails with -11 instead of
     *     guessing, EXCEPT at snapshot 0 (an empty base:
     *     every live node postdates it, so the answer is
     *     "absent"). The full-history answer lives in the
     *     Go TimelineHandle over SegDB snapshots, which
     *     keep superseded versions; this layer refuses
     *     to fake it from the ART.
     *   - OP_TX_COMMIT stages the tx write set into the
     *     timeline write set instead of applying it to
     *     the ART/WAL (no tx_commit validation against
     *     ART versions, no frame); TX_ASSERT is checked
     *     at assert time against the layered view.
     *   - OP_SCAN under a timeline returns -11: the ART
     *     has no snapshot scan, and returning a main-head
     *     scan under a timeline name would violate
     *     isolation. Timeline scans go through the Go
     *     handle (SegDB snapshot cursor + overlay).
     * Write-set entries are arena copies (art_index's
     * arena), so they survive the bytecode buffer that
     * supplied them and can be drained after the program
     * ends. vm_init clears the timeline (fresh context
     * per program, as with tx); a host that runs several
     * programs under one timeline drains the write set
     * after each program and re-stages it with
     * vm_timeline_stage after the next vm_init. The
     * wasm glue for this mode (Axis C, src/yowza.c): the
     * sticky handle yowza_timeline_set / yowza_timeline_clear
     * (re-applied after vm_init by yowza_execute), the
     * one-shot yowza_execute_timeline, and the drain exports
     * yowza_timeline_write_count /
     * yowza_timeline_write_serialize. The Go TimelineVM
     * remains the SegDB-handle wasm path; this glue is the
     * C VM (ART-clock) timeline mode for wasm programs.
     * This native mode is proven by test/test_vm_timeline.c
     * and the Axis C glue test. */
    int timeline_active;
    uint64_t timeline_snapshot_seq;
    VMTimelineWrite timeline_writes[VM_TIMELINE_MAX_WRITES];
    uint32_t timeline_n_writes;
} VMContext;

// Initialize the VM Context
void vm_init(VMContext* vm, ART* art_index, uint8_t* bytecode, size_t length, StorageDriver* storage);

// Execute the provided bytecode.
// For now, it simply parses the operations and acts as a placeholder
// for the real storage engine in Phase 2.
int vm_execute(VMContext* vm);

/* Phase D timeline API (see VMContext comment). */
void vm_set_timeline(VMContext* vm, uint64_t snapshot_seq);
void vm_clear_timeline(VMContext* vm);
int vm_timeline_stage(VMContext* vm, const uint8_t* key, uint32_t key_len,
                      const uint8_t* val, uint32_t val_len, int deleted, uint64_t timestamp);
int vm_timeline_write_count(VMContext* vm);
int vm_timeline_write_at(VMContext* vm, uint32_t idx, const uint8_t** key, uint32_t* key_len,
                         const uint8_t** val, uint32_t* val_len, int* deleted);

#endif // YOWZA_VM_H
