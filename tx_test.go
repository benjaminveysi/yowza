package yowza

import (
	"context"
	"os"
	"strings"
	"testing"
)

// Phase 1 Go tests, run against both wasm binaries: the wasm32
// binary exercises the extended error-code threshold in
// executeBytecode (-7/-8/-9 must surface as errors, not pointers),
// and the wasm64 binary exercises the memory64 path.
func TestTransactions(t *testing.T) {
	for _, bin := range []string{"build/yowza.wasm", "build/yowza64.wasm"} {
		t.Run(bin, func(t *testing.T) {
			wasmBytes, err := os.ReadFile(bin)
			if err != nil {
				t.Fatalf("read %s: %v", bin, err)
			}
			db, err := NewYowzaDB(wasmBytes)
			if err != nil {
				t.Fatalf("NewYowzaDB: %v", err)
			}
			defer db.Close()

			// 1. Atomic multi-op + WAL frame parsing.
			if err := db.ExecuteTransaction(500, []TxOp{
				{Type: TxOpPut, Key: "user:1", Value: []byte("Alice")},
				{Type: TxOpPut, Key: "idx:email:a@x:user:1", Value: []byte("user:1")},
				{Type: TxOpPut, Key: "idxrev:user:1:email:a@x", Value: []byte("idx:email:a@x:user:1")},
			}); err != nil {
				t.Fatalf("ExecuteTransaction: %v", err)
			}
			if v, _ := db.Get("user:1"); string(v) != "Alice" {
				t.Fatalf("user:1 = %q, want Alice", v)
			}
			entries, err := db.PeekWAL()
			if err != nil {
				t.Fatalf("PeekWAL: %v", err)
			}
			var txEntries []WalEntry
			for _, e := range entries {
				if e.TransactionID == 500 {
					txEntries = append(txEntries, e)
				}
			}
			if len(txEntries) != 3 {
				t.Fatalf("PeekWAL tx entries = %d, want 3 (entries=%v)", len(txEntries), entries)
			}
			if txEntries[0].LogicalSequence == 0 {
				t.Fatalf("tx entries carry no logical sequence: %+v", txEntries[0])
			}

			// 2. Assert failure rolls the whole transaction back.
			if err := db.ExecuteTransaction(501, []TxOp{
				{Type: TxOpAssert, Key: "user:1", Value: []byte("Wrong"), AssertKind: TxAssertValueEquals},
				{Type: TxOpPut, Key: "side", Value: []byte("s")},
			}); err == nil || !strings.Contains(err.Error(), "code -9") {
				t.Fatalf("expected code -9 assert error, got %v", err)
			}
			if v, _ := db.Get("side"); v != nil {
				t.Fatalf("side visible after failed tx: %q", v)
			}

			// 3. Unique put on an existing key fails and rolls back.
			if err := db.ExecuteTransaction(502, []TxOp{
				{Type: TxOpPut, Key: "user:1", Value: []byte("Other"), Unique: true},
				{Type: TxOpPut, Key: "side2", Value: []byte("s")},
			}); err == nil || !strings.Contains(err.Error(), "code -9") {
				t.Fatalf("expected code -9 unique error, got %v", err)
			}
			if v, _ := db.Get("side2"); v != nil {
				t.Fatalf("side2 visible after failed unique tx")
			}
			if v, _ := db.Get("user:1"); string(v) != "Alice" {
				t.Fatalf("user:1 changed by failed tx: %q", v)
			}

			// 4. Incomplete program (BEGIN + TX_PUT + EXIT, no COMMIT)
			// is a protocol error and leaves nothing visible.
			db.mu.Lock()
			code := []byte{OpcodeTxBegin}
			code = appendU64(code, 503)
			code = appendU64(code, 0)
			code = appendU32(code, 0)
			code = appendU64(code, 0)
			code = append(code, OpcodeTxPut, 0)
			code = appendKV(code, "ghost", []byte("g"))
			code = append(code, OpcodeExit)
			_, execErr := db.executeBytecode(context.Background(), code)
			db.mu.Unlock()
			if execErr == nil || !strings.Contains(execErr.Error(), "code -7") {
				t.Fatalf("expected code -7 incomplete-tx error, got %v", execErr)
			}
			if v, _ := db.Get("ghost"); v != nil {
				t.Fatalf("ghost visible after incomplete tx")
			}

			// 5. PutWithIndex is now one transaction: changing the
			// index value removes the stale mapping atomically.
			if err := db.PutWithIndex("user:9", []byte("Bob"), "email", "bob@old"); err != nil {
				t.Fatalf("PutWithIndex 1: %v", err)
			}
			if err := db.PutWithIndex("user:9", []byte("Bob"), "email", "bob@new"); err != nil {
				t.Fatalf("PutWithIndex 2: %v", err)
			}
			if v, _ := db.SearchByIndex("email", "bob@old"); v != nil {
				t.Fatalf("stale index still returns %q", v)
			}
			if v, _ := db.SearchByIndex("email", "bob@new"); string(v) != "Bob" {
				t.Fatalf("new index returns %q, want Bob", v)
			}
			if v, _ := db.Get("user:9"); string(v) != "Bob" {
				t.Fatalf("user:9 = %q, want Bob", v)
			}
		})
	}
}
