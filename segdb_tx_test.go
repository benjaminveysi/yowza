package yowza

// Phase C, C1 tests: the unified SegDBTransaction
// contract, the write-path matrix against a reference
// model, and journal recovery. Each test loops over
// both wasm binaries with a fresh directory per binary.

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// TestSegDBTxBasicAndRollback: layered reads, private
// write set invisibility, rollback discarding everything,
// and commit publishing through the facade.
func TestSegDBTxBasicAndRollback(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 2, Durability: DurabilitySync})
			defer p.Close()
			if err := p.Put("a", []byte("1")); err != nil {
				t.Fatalf("put: %v", err)
			}
			tx, err := BeginSegDBTransaction(p)
			if err != nil {
				t.Fatalf("begin: %v", err)
			}
			if v, err := tx.Get("a"); err != nil || string(v) != "1" {
				t.Fatalf("tx get a = %q err %v, want 1", v, err)
			}
			if err := tx.Put("a", []byte("2"), false); err != nil {
				t.Fatalf("tx put: %v", err)
			}
			if err := tx.Put("b", []byte("3"), false); err != nil {
				t.Fatalf("tx put b: %v", err)
			}
			if err := tx.Delete("a"); err != nil {
				t.Fatalf("tx delete: %v", err)
			}
			// The facade sees nothing until commit.
			persistMustGet(t, p, "a", "1")
			persistMustMissing(t, p, "b")
			if _, err := tx.Get("a"); !errors.Is(err, ErrSegDBTxNotFound) {
				t.Fatalf("tx get deleted a err %v, want NotFound", err)
			}
			if err := tx.Rollback(); err != nil {
				t.Fatalf("rollback: %v", err)
			}
			persistMustGet(t, p, "a", "1")
			persistMustMissing(t, p, "b")
			// A second transaction commits.
			tx2, err := BeginSegDBTransaction(p)
			if err != nil {
				t.Fatalf("begin2: %v", err)
			}
			if err := tx2.Put("a", []byte("2"), false); err != nil {
				t.Fatalf("tx2 put: %v", err)
			}
			if err := tx2.Put("b", []byte("3"), false); err != nil {
				t.Fatalf("tx2 put b: %v", err)
			}
			if err := tx2.Commit(); err != nil {
				t.Fatalf("commit: %v", err)
			}
			persistMustGet(t, p, "a", "2")
			persistMustGet(t, p, "b", "3")
			if err := tx2.Commit(); !errors.Is(err, ErrSegDBTxNotActive) {
				t.Fatalf("double commit err %v, want NotActive", err)
			}
		})
	}
}

// TestSegDBTxAssertUnique: asserts on the effective
// state, unique-put re-check against live state at
// commit, and nothing applied on failure.
func TestSegDBTxAssertUnique(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			defer p.Close()
			if err := p.Put("k", []byte("v")); err != nil {
				t.Fatalf("put: %v", err)
			}
			tx, err := BeginSegDBTransaction(p)
			if err != nil {
				t.Fatalf("begin: %v", err)
			}
			if err := tx.Assert(TxAssertPresent, "k", nil); err != nil {
				t.Fatalf("assert present: %v", err)
			}
			if err := tx.Assert(TxAssertValueEquals, "k", []byte("v")); err != nil {
				t.Fatalf("assert value: %v", err)
			}
			if err := tx.Assert(TxAssertAbsent, "k", nil); err == nil {
				t.Fatalf("assert absent on present key accepted")
			}
			if err := tx.Commit(); err == nil {
				t.Fatalf("commit after failed assert accepted")
			}
			// The failed transaction applied nothing.
			persistMustGet(t, p, "k", "v")
			// Unique: an intervening facade commit breaks it.
			tx2, err := BeginSegDBTransaction(p)
			if err != nil {
				t.Fatalf("begin2: %v", err)
			}
			if err := tx2.Put("u", []byte("1"), true); err != nil {
				t.Fatalf("tx2 unique put: %v", err)
			}
			if err := p.Put("u", []byte("other")); err != nil {
				t.Fatalf("intervening put: %v", err)
			}
			if err := tx2.Commit(); err == nil {
				t.Fatalf("unique commit over intervening write accepted")
			} else if !errors.Is(err, ErrSegDBTxAssert) {
				t.Fatalf("unique commit err %v, want Assert", err)
			}
			persistMustGet(t, p, "u", "other")
			// Unique on a fresh key commits.
			tx3, err := BeginSegDBTransaction(p)
			if err != nil {
				t.Fatalf("begin3: %v", err)
			}
			if err := tx3.Put("u2", []byte("1"), true); err != nil {
				t.Fatalf("tx3 put: %v", err)
			}
			if err := tx3.Commit(); err != nil {
				t.Fatalf("tx3 commit: %v", err)
			}
			persistMustGet(t, p, "u2", "1")
		})
	}
}

// TestSegDBTxSegmentOnlyDelete: a tx delete of a
// segment-only key (flushed, hot-absent) produces the
// shadowing tombstone: the pre-tx snapshot still reads
// the old value, the live DB does not, and the delete
// survives flush + reopen.
func TestSegDBTxSegmentOnlyDelete(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			if err := p.Put("seg", []byte("old")); err != nil {
				t.Fatalf("put: %v", err)
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}
			// Snapshot BEFORE the tx.
			before, err := p.AcquireSnapshot()
			if err != nil {
				t.Fatalf("snapshot: %v", err)
			}
			tx, err := BeginSegDBTransaction(p)
			if err != nil {
				t.Fatalf("begin: %v", err)
			}
			if err := tx.Delete("seg"); err != nil {
				t.Fatalf("tx delete: %v", err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatalf("commit: %v", err)
			}
			// Live: gone. Pre-tx snapshot: old value.
			persistMustMissing(t, p, "seg")
			if v, err := before.Get([]byte("seg")); err != nil || string(v) != "old" {
				t.Fatalf("pre-tx snapshot get = %q err %v, want old", v, err)
			}
			_ = before.Close()
			// Durable across flush + reopen.
			if err := p.Flush(); err != nil {
				t.Fatalf("flush2: %v", err)
			}
			p.Close()
			p2 := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			defer p2.Close()
			persistMustMissing(t, p2, "seg")
		})
	}
}

// TestSegDBTxJournalRecovery: an "applying" journal
// rolls forward through the same apply path Commit
// uses; an "aborted" journal is removed without
// applying; a corrupt journal fails loudly and applies
// nothing.
func TestSegDBTxJournalRecovery(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			defer p.Close()
			applying, err := encodeTxJournal(424242, "applying", []*SegDBTxWrite{
				{Key: "j:1", Value: []byte("one")},
				{Key: "j:2", Value: []byte("two")},
			})
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			if err := os.WriteFile(filepath.Join(dir, "segdb-tx-424242.journal"), applying, 0o644); err != nil {
				t.Fatalf("write journal: %v", err)
			}
			aborted, err := encodeTxJournal(424243, "aborted", []*SegDBTxWrite{
				{Key: "j:3", Value: []byte("three")},
			})
			if err != nil {
				t.Fatalf("encode aborted: %v", err)
			}
			if err := os.WriteFile(filepath.Join(dir, "segdb-tx-424243.journal"), aborted, 0o644); err != nil {
				t.Fatalf("write aborted journal: %v", err)
			}
			rolled, err := RecoverSegDBTransactions(p)
			if err != nil {
				t.Fatalf("recover: %v", err)
			}
			if len(rolled) != 1 || rolled[0] != 424242 {
				t.Fatalf("rolled %v, want [424242]", rolled)
			}
			persistMustGet(t, p, "j:1", "one")
			persistMustGet(t, p, "j:2", "two")
			persistMustMissing(t, p, "j:3")
			if _, err := os.Stat(filepath.Join(dir, "segdb-tx-424242.journal")); !os.IsNotExist(err) {
				t.Fatalf("applied journal not removed")
			}
			// Corrupt journal: recovery fails, nothing applied.
			if err := os.WriteFile(filepath.Join(dir, "segdb-tx-999.journal"), []byte("{corrupt"), 0o644); err != nil {
				t.Fatalf("write corrupt journal: %v", err)
			}
			if _, err := RecoverSegDBTransactions(p); err == nil {
				t.Fatalf("corrupt journal recovery accepted")
			}
			persistMustMissing(t, p, "j:3")
		})
	}
}

// TestSegDBTxMatrix: the write-path matrix against a
// reference model — facade puts/deletes, transaction
// commits and rollbacks, and a flushed segment-only
// delete, all compared to one model map after every
// step. This is the Go half of the matrix; the C
// half (C API / VM / WASM / Tx / replay) is covered
// by the C suite + the tx shadow change, and the
// remaining NOT-covered paths are listed honestly in
// TIMELINE_V2_NOTES.md (VM SegDB context, legacy
// replay records, direct ART callers).
func TestSegDBTxMatrix(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 2, Durability: DurabilitySync})
			defer p.Close()
			model := map[string]string{}
			check := func(step string) {
				t.Helper()
				rows, err := p.Scan("")
				if err != nil {
					t.Fatalf("step %s scan: %v", step, err)
				}
				got := map[string]string{}
				for _, e := range rows {
					got[string(e.Key)] = string(e.Val)
				}
				if len(got) != len(model) {
					t.Fatalf("step %s: db %v, model %v", step, got, model)
				}
				for k, v := range model {
					if got[k] != v {
						t.Fatalf("step %s: key %s db %q model %q (db %v model %v)", step, k, got[k], v, got, model)
					}
				}
			}
			put := func(k, v string) {
				t.Helper()
				if err := p.Put(k, []byte(v)); err != nil {
					t.Fatalf("facade put %s: %v", k, err)
				}
				model[k] = v
			}
			del := func(k string) {
				t.Helper()
				if err := p.Delete(k); err != nil {
					t.Fatalf("facade delete %s: %v", k, err)
				}
				delete(model, k)
			}
			put("m:1", "a")
			put("m:2", "b")
			check("facade puts")
			// Tx commit: put + delete + overwrite.
			tx, err := BeginSegDBTransaction(p)
			if err != nil {
				t.Fatalf("begin: %v", err)
			}
			if err := tx.Put("m:3", []byte("c"), false); err != nil {
				t.Fatalf("tx put: %v", err)
			}
			if err := tx.Delete("m:1"); err != nil {
				t.Fatalf("tx delete: %v", err)
			}
			if err := tx.Put("m:2", []byte("B"), false); err != nil {
				t.Fatalf("tx overwrite: %v", err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatalf("tx commit: %v", err)
			}
			model["m:3"] = "c"
			delete(model, "m:1")
			model["m:2"] = "B"
			check("tx commit")
			// Tx rollback: nothing changes.
			tx2, err := BeginSegDBTransaction(p)
			if err != nil {
				t.Fatalf("begin2: %v", err)
			}
			if err := tx2.Put("m:4", []byte("d"), false); err != nil {
				t.Fatalf("tx2 put: %v", err)
			}
			if err := tx2.Delete("m:2"); err != nil {
				t.Fatalf("tx2 delete: %v", err)
			}
			if err := tx2.Rollback(); err != nil {
				t.Fatalf("rollback: %v", err)
			}
			check("tx rollback")
			// Flush, then tx-delete a segment-only key.
			if err := p.Flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}
			check("flush")
			tx3, err := BeginSegDBTransaction(p)
			if err != nil {
				t.Fatalf("begin3: %v", err)
			}
			if err := tx3.Delete("m:3"); err != nil {
				t.Fatalf("tx3 delete: %v", err)
			}
			if err := tx3.Commit(); err != nil {
				t.Fatalf("tx3 commit: %v", err)
			}
			delete(model, "m:3")
			check("tx segment-only delete")
			del("m:2")
			check("facade delete")
		})
	}
}

// TestSegDBTxConcurrentCommits: independent transactions
// committing concurrently do not corrupt the facade
// (run under -race by the gate command).
func TestSegDBTxConcurrentCommits(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 2, Durability: DurabilitySync})
			defer p.Close()
			done := make(chan error, 8)
			for i := 0; i < 8; i++ {
				go func(i int) {
					tx, err := BeginSegDBTransaction(p)
					if err != nil {
						done <- err
						return
					}
					if err := tx.Put(fmt.Sprintf("cc:%d", i), []byte("v"), false); err != nil {
						done <- err
						return
					}
					done <- tx.Commit()
				}(i)
			}
			for i := 0; i < 8; i++ {
				if err := <-done; err != nil {
					t.Fatalf("concurrent commit %d: %v", i, err)
				}
			}
			for i := 0; i < 8; i++ {
				persistMustGet(t, p, fmt.Sprintf("cc:%d", i), "v")
			}
		})
	}
}
