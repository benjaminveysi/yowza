package yowza

import (
	"context"
	"encoding/binary"
	"fmt"
)

// Phase 1 transactional API (see tx.h / PHASE1_NOTES.md).
//
// A transaction is ONE bytecode program executed under ONE db.mu
// acquisition: BEGIN ... TX_* ... COMMIT. That is what makes it
// atomic: the Go host serializes vm_execute calls, so no reader can
// observe the program mid-way, and the C engine buffers all writes
// in a private set until commit validation passes. A failed commit
// (conflict -8, assert -9, WAL -6, OOM -3, protocol -7) leaves the
// live tree untouched (or, for a mid-apply OOM, logically undone);
// callers that need retry semantics must retry themselves, because
// an automatic retry would hide a genuine conflict from the caller.

// TxOp is one operation inside a transaction.
type TxOp struct {
	// Type: TxOpPut, TxOpDelete, or TxOpAssert.
	Type byte
	Key  string
	// Value for Put and for AssertValueEquals; empty otherwise.
	Value []byte
	// Unique, for Put only: fail validation if the key exists in
	// committed state at commit time.
	Unique bool
	// CRDT timestamp for Put; 0 for a plain put.
	Timestamp uint64
	CRDT      bool
	// AssertKind, for Assert only: one of the TxAssert* constants.
	AssertKind uint8
}

const (
	TxOpPut    byte = 'P'
	TxOpDelete byte = 'D'
	TxOpAssert byte = 'A'

	TxAssertAbsent      uint8 = 0
	TxAssertPresent     uint8 = 1
	TxAssertValueEquals uint8 = 2
)

func appendU32(b []byte, v uint32) []byte {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], v)
	return append(b, buf[:]...)
}

func appendU64(b []byte, v uint64) []byte {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], v)
	return append(b, buf[:]...)
}

func appendKV(b []byte, key string, val []byte) []byte {
	b = appendU32(b, uint32(len(key)))
	b = append(b, key...)
	b = appendU32(b, uint32(len(val)))
	b = append(b, val...)
	return b
}

// ExecuteTransaction runs ops as a single transaction. txID must be
// unique per transaction for WAL frame identification (callers that
// do not care can pass 0; the C engine still assigns its own commit
// sequence, and frame identity for replay is the sequence, not the
// caller txID). Origin 0 means "local single node" in Phase 1.
func (db *YowzaDB) ExecuteTransaction(txID uint64, ops []TxOp) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.executeTransactionLocked(txID, ops)
}

// executeTransactionLocked is ExecuteTransaction without the lock;
// PutWithIndex uses it so its scan and its transaction share one
// acquisition (see index.go for the optimistic-assert guard that
// makes the scan-then-transact window safe).
func (db *YowzaDB) executeTransactionLocked(txID uint64, ops []TxOp) error {
	code := make([]byte, 0, 1024)
	code = append(code, OpcodeTxBegin)
	code = appendU64(code, txID)
	code = appendU64(code, 0) // origin_node_id: local
	code = appendU32(code, 0) // consistency_model: local
	code = appendU64(code, 0) // schema_version: Phase 1 has no schema registry yet
	for _, op := range ops {
		switch op.Type {
		case TxOpPut:
			if op.CRDT {
				code = append(code, OpcodeTxPutCRDT)
				code = appendU64(code, op.Timestamp)
			} else {
				code = append(code, OpcodeTxPut)
			}
			var flags byte
			if op.Unique {
				flags = 0x01
			}
			code = append(code, flags)
			code = appendKV(code, op.Key, op.Value)
		case TxOpDelete:
			code = append(code, OpcodeTxDel)
			code = appendU32(code, uint32(len(op.Key)))
			code = append(code, op.Key...)
		case TxOpAssert:
			code = append(code, OpcodeTxAssert)
			code = append(code, op.AssertKind)
			code = appendKV(code, op.Key, op.Value)
		default:
			return fmt.Errorf("unknown TxOp type %q", op.Type)
		}
	}
	code = append(code, OpcodeTxCommit)
	code = append(code, OpcodeExit)
	_, err := db.executeBytecode(context.Background(), code)
	return err
}
