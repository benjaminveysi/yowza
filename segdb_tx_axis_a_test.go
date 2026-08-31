package yowza

// Axis A tests: transaction correctness after the
// commit-gate + error-separation fix (see
// TIMELINE_V2_PATCH_NOTES.md, Axis A section).
// Each test loops over both wasm binaries with a
// fresh directory per binary, like segdb_tx_test.go.
// No test here weakens an old assertion: old
// segdb_tx_test.go tests are run unchanged alongside.

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// TestSegDBTxAxisAGetErrorSeparation: SegDB.Get maps a
// missing key to ErrSegDBNotFound (errors.Is) and a
// present key to its value; an invalid key is a
// validation error, never NotFound. This is the Go
// boundary the transaction path relies on: only
// ErrSegDBNotFound may convert to absence.
func TestSegDBTxAxisAGetErrorSeparation(t *testing.T) {
	for _, bin := range persistTestBins {
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
			sdb, err := NewSegDB(db)
			if err != nil {
				t.Fatalf("NewSegDB: %v", err)
			}
			if err := sdb.Put("present", []byte("v")); err != nil {
				t.Fatalf("Put: %v", err)
			}
			if v, err := sdb.Get("present"); err != nil || string(v) != "v" {
				t.Fatalf("Get present = %q err %v, want v", v, err)
			}
			if _, err := sdb.Get("absent"); !errors.Is(err, ErrSegDBNotFound) {
				t.Fatalf("Get absent err %v, want ErrSegDBNotFound", err)
			}
			if _, err := sdb.Get(""); err == nil || errors.Is(err, ErrSegDBNotFound) {
				t.Fatalf("Get empty-key err %v, want validation error, not NotFound", err)
			}
		})
	}
}

// TestSegDBTxAxisAStorageErrorDuringValidation: a
// storage/state error during commit validation must
// fail the commit, never be read as absence. The
// reachable in-process stand-ins for a storage error
// at this layer are a closed database (validation
// reads cannot run) and a closed snapshot handle
// (effective reads cannot run). Both must propagate
// as errors, not as Assert/NotFound outcomes, and
// must write no journal and apply nothing.
func TestSegDBTxAxisAStorageErrorDuringValidation(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			if err := p.Put("k", []byte("old")); err != nil {
				t.Fatalf("put: %v", err)
			}
			// (a) Closed database at commit time.
			tx, err := BeginSegDBTransaction(p)
			if err != nil {
				t.Fatalf("begin: %v", err)
			}
			if err := tx.Put("k", []byte("new"), false); err != nil {
				t.Fatalf("tx put: %v", err)
			}
			if err := tx.Assert(TxAssertAbsent, "other", nil); err != nil {
				t.Fatalf("assert absent (snapshot, other absent): %v", err)
			}
			p.Close()
			err = tx.Commit()
			if err == nil {
				t.Fatalf("commit over closed DB accepted")
			}
			if !errors.Is(err, ErrPersistedClosed) {
				t.Fatalf("commit over closed DB err %v, want ErrPersistedClosed", err)
			}
			if errors.Is(err, ErrSegDBTxAssert) {
				t.Fatalf("closed-DB error misreported as assert failure: %v", err)
			}
			if tx.Indeterminate() {
				t.Fatalf("closed-DB validation failure marked indeterminate (no journal was written)")
			}
			ents, rerr := os.ReadDir(dir)
			if rerr != nil {
				t.Fatalf("readdir: %v", rerr)
			}
			for _, e := range ents {
				if strings.HasSuffix(e.Name(), ".journal") {
					t.Fatalf("journal %s written for a validation failure", e.Name())
				}
			}
			// (b) Closed snapshot handle: a tx Get of a
			// present key must fail with the handle
			// error, never with ErrSegDBTxNotFound
			// (the old effectiveLocked swallowed every
			// handle error as absence).
			p2 := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			defer p2.Close()
			tx2, err := BeginSegDBTransaction(p2)
			if err != nil {
				t.Fatalf("begin2: %v", err)
			}
			_ = tx2.handle.Close()
			if _, err := tx2.Get("k"); err == nil || errors.Is(err, ErrSegDBTxNotFound) {
				t.Fatalf("tx Get after handle close err %v, want a non-NotFound error", err)
			}
			if err := tx2.Assert(TxAssertPresent, "k", nil); err == nil {
				t.Fatalf("assert after handle close accepted")
			}
			_ = tx2.Rollback()
		})
	}
}

// TestSegDBTxAxisAConflictingTransactions: two
// transactions begun from the same snapshot, where
// the first commits and the second's assert/unique
// re-check must fail under the commit gate. The
// first commit's value survives untouched.
func TestSegDBTxAxisAConflictingTransactions(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 2, Durability: DurabilitySync})
			defer p.Close()
			tx1, err := BeginSegDBTransaction(p)
			if err != nil {
				t.Fatalf("begin1: %v", err)
			}
			tx2, err := BeginSegDBTransaction(p)
			if err != nil {
				t.Fatalf("begin2: %v", err)
			}
			// tx2's assert passes at assert time: its
			// snapshot predates tx1's commit.
			if err := tx2.Assert(TxAssertAbsent, "conf", nil); err != nil {
				t.Fatalf("tx2 assert absent at assert time: %v", err)
			}
			if err := tx2.Put("conf", []byte("v2"), true); err != nil {
				t.Fatalf("tx2 unique put: %v", err)
			}
			if err := tx1.Put("conf", []byte("v1"), false); err != nil {
				t.Fatalf("tx1 put: %v", err)
			}
			if err := tx1.Commit(); err != nil {
				t.Fatalf("tx1 commit: %v", err)
			}
			if err := tx2.Commit(); err == nil {
				t.Fatalf("conflicting tx2 commit accepted")
			} else if !errors.Is(err, ErrSegDBTxAssert) {
				t.Fatalf("tx2 commit err %v, want ErrSegDBTxAssert", err)
			}
			persistMustGet(t, p, "conf", "v1")
			// A second conflict shape: unique put alone
			// (no assert) against an intervening commit.
			tx3, err := BeginSegDBTransaction(p)
			if err != nil {
				t.Fatalf("begin3: %v", err)
			}
			if err := tx3.Put("uconf", []byte("1"), true); err != nil {
				t.Fatalf("tx3 put: %v", err)
			}
			if err := p.Put("uconf", []byte("other")); err != nil {
				t.Fatalf("intervening put: %v", err)
			}
			if err := tx3.Commit(); err == nil || !errors.Is(err, ErrSegDBTxAssert) {
				t.Fatalf("unique conflict commit err %v, want Assert", err)
			}
			persistMustGet(t, p, "uconf", "other")
		})
	}
}

// TestSegDBTxAxisAConcurrentWriterAndReaders: an
// ordinary writer and live readers run during a
// multi-key commit. No reader may ever observe a
// half-applied write set (some keys new, some old);
// the writer's key must end at a value it wrote.
//
// Readers use p.Scan, NOT sequential per-key Gets:
// a scan holds db.mu across the whole read, so it
// linearizes either entirely before the commit gate
// (all old) or entirely after it (all new). Sequential
// Gets are NOT an atomic reader — a sample that
// straddles any commit, however atomic, legitimately
// mixes old and new keys, one Get at a time — so a
// per-key loop would fail even a perfectly gated
// commit and proves nothing about the gate.
func TestSegDBTxAxisAConcurrentWriterAndReaders(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 2, Durability: DurabilitySync})
			defer p.Close()
			const n = 40
			keys := make([]string, n)
			for i := 0; i < n; i++ {
				keys[i] = fmt.Sprintf("ax:k%02d", i)
				if err := p.Put(keys[i], []byte("old")); err != nil {
					t.Fatalf("seed put %d: %v", i, err)
				}
			}
			tx, err := BeginSegDBTransaction(p)
			if err != nil {
				t.Fatalf("begin: %v", err)
			}
			for i := 0; i < n; i++ {
				if err := tx.Put(keys[i], []byte("new"), false); err != nil {
					t.Fatalf("tx put %d: %v", i, err)
				}
			}
			var stop atomic.Bool
			var partial atomic.Int64
			var samples atomic.Int64
			var wg sync.WaitGroup
			for r := 0; r < 4; r++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for !stop.Load() {
						rows, err := p.Scan("")
						if err != nil {
							continue
						}
						old, new := 0, 0
						for _, e := range rows {
							if !strings.HasPrefix(string(e.Key), "ax:k") {
								continue
							}
							switch string(e.Val) {
							case "old":
								old++
							case "new":
								new++
							}
						}
						if old+new != n {
							continue // scan during seeding/teardown: not a commit sample
						}
						samples.Add(1)
						if old > 0 && new > 0 {
							partial.Add(1)
						}
					}
				}()
			}
			// Ordinary writer, disjoint key, during commit.
			writerDone := make(chan error, 1)
			go func() {
				var lastErr error
				for i := 0; i < 20; i++ {
					if err := p.Put("ax:writer", []byte(fmt.Sprintf("w%d", i))); err != nil {
						lastErr = err
						break
					}
				}
				writerDone <- lastErr
			}()
			if err := tx.Commit(); err != nil {
				t.Fatalf("commit: %v", err)
			}
			if err := <-writerDone; err != nil {
				t.Fatalf("concurrent writer: %v", err)
			}
			stop.Store(true)
			wg.Wait()
			if partial.Load() != 0 {
				t.Fatalf("readers observed %d half-applied samples out of %d", partial.Load(), samples.Load())
			}
			if samples.Load() == 0 {
				t.Fatalf("readers took no samples during commit")
			}
			for i := 0; i < n; i++ {
				persistMustGet(t, p, keys[i], "new")
			}
			v, err := p.Get("ax:writer")
			if err != nil || !strings.HasPrefix(string(v), "w") {
				t.Fatalf("writer key = %q err %v, want w<N>", v, err)
			}
		})
	}
}

// TestSegDBTxAxisAMidApplyFailurePrefixRollback: a
// write set whose second write (sorted order) fails
// validation inside the apply path — injected by
// direct write-set construction, bypassing Put's
// validation, exactly as a mid-apply failure would
// present — must restore only the applied prefix.
// The never-applied key must stay absent, the
// applied key must return to its pre-image (not to
// a tombstone), and the journal must be "aborted",
// not removed, so recovery can confirm the rollback.
func TestSegDBTxAxisAMidApplyFailurePrefixRollback(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			defer p.Close()
			if err := p.Put("m:a", []byte("old-a")); err != nil {
				t.Fatalf("seed m:a: %v", err)
			}
			tx, err := BeginSegDBTransaction(p)
			if err != nil {
				t.Fatalf("begin: %v", err)
			}
			// Direct construction: tx.Put would reject
			// the 2000-byte value, so a mid-apply
			// failure cannot be reached through it.
			tx.writes["m:a"] = &SegDBTxWrite{Key: "m:a", Value: []byte("new-a")}
			tx.writes["m:b"] = &SegDBTxWrite{Key: "m:b", Value: make([]byte, 2000)}
			tx.writes["m:c"] = &SegDBTxWrite{Key: "m:c", Value: []byte("new-c")}
			tx.order = []string{"m:a", "m:b", "m:c"}
			if err := tx.Commit(); err == nil {
				t.Fatalf("commit with failing mid-apply write accepted")
			}
			if tx.Indeterminate() {
				t.Fatalf("prefix rollback succeeded but tx marked indeterminate")
			}
			persistMustGet(t, p, "m:a", "old-a")
			persistMustMissing(t, p, "m:b")
			persistMustMissing(t, p, "m:c")
			data, err := os.ReadFile(tx.journalPath())
			if err != nil {
				t.Fatalf("journal after failed commit unreadable: %v", err)
			}
			j, err := decodeTxJournal(data)
			if err != nil {
				t.Fatalf("journal decode: %v", err)
			}
			if j.Status != "aborted" {
				t.Fatalf("journal status %q, want aborted", j.Status)
			}
			rolled, err := RecoverSegDBTransactions(p)
			if err != nil {
				t.Fatalf("recovery of aborted journal: %v", err)
			}
			if len(rolled) != 0 {
				t.Fatalf("rolled %v, want none for aborted journal", rolled)
			}
			persistMustGet(t, p, "m:a", "old-a")
			persistMustMissing(t, p, "m:c")
		})
	}
}

// TestSegDBTxAxisARestartBeforeRecovery: an
// "applying" journal survives a close/reopen. PATCH3
// contract (user-ordered replacement of the old
// read-before-recovery expectation): NO ordinary
// facade read runs before recovery — not even for
// unrelated keys — Begin refuses until recovery
// runs, and recovery then rolls the full set
// forward atomically, after which the full new
// state is visible.
func TestSegDBTxAxisARestartBeforeRecovery(t *testing.T) {
	for _, bin := range persistTestBins {
		t.Run(bin, func(t *testing.T) {
			dir := t.TempDir()
			p := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			if err := p.Put("r:keep", []byte("keep")); err != nil {
				t.Fatalf("seed: %v", err)
			}
			data, err := encodeTxJournal(777001, "applying", []*SegDBTxWrite{
				{Key: "r:1", Value: []byte("one")},
				{Key: "r:2", Value: []byte("two")},
			})
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			if err := os.WriteFile(filepath.Join(dir, "segdb-tx-777001.journal"), data, 0o644); err != nil {
				t.Fatalf("write journal: %v", err)
			}
			p.Close()
			p2 := persistOpenForTest(t, bin, dir, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
			defer p2.Close()
			// Restart before recovery: half-applied is
			// not a valid commit, and NO ordinary read
			// is served — the gate refuses even the
			// unrelated pre-existing key until recovery
			// ends (patch3 recovery-before-read contract).
			for _, k := range []string{"r:1", "r:2", "r:keep"} {
				if _, err := p2.Get(k); !errors.Is(err, ErrLifecycleRecoveryIncomplete) {
					t.Fatalf("pre-recovery Get(%q) err %v, want ErrLifecycleRecoveryIncomplete", k, err)
				}
			}
			if _, err := BeginSegDBTransaction(p2); !errors.Is(err, ErrSegDBTxRecoveryRequired) {
				t.Fatalf("Begin with pending applying journal err %v, want RecoveryRequired", err)
			}
			rolled, err := RecoverSegDBTransactions(p2)
			if err != nil {
				t.Fatalf("recover: %v", err)
			}
			if len(rolled) != 1 || rolled[0] != 777001 {
				t.Fatalf("rolled %v, want [777001]", rolled)
			}
			persistMustGet(t, p2, "r:1", "one")
			persistMustGet(t, p2, "r:2", "two")
			tx, err := BeginSegDBTransaction(p2)
			if err != nil {
				t.Fatalf("begin after recovery: %v", err)
			}
			_ = tx.Rollback()
		})
	}
}

// BenchmarkSegDBTxAxisACommitVsFacade measures the
// two pre-image suspects separately: a facade Put
// (spill pre-image Get + WAL) versus a one-key
// transaction commit (live validation Get + journal
// + spill pre-image Get + WAL). Run with
// -benchmem; the numbers go in the Axis A patch
// notes, not in prose claims.
func BenchmarkSegDBTxAxisACommitVsFacade(b *testing.B) {
	dir := b.TempDir()
	wasmBytes, err := os.ReadFile("build/yowza64.wasm")
	if err != nil {
		b.Fatalf("read wasm: %v", err)
	}
	p, err := OpenPersistedSegDB(dir, wasmBytes, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	defer p.Close()
	b.Run("facade-put", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = p.Put("bench", []byte("v"))
		}
	})
	b.Run("tx-commit-1key", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			tx, err := BeginSegDBTransaction(p)
			if err != nil {
				b.Fatalf("begin: %v", err)
			}
			_ = tx.Put("bench", []byte("v"), false)
			_ = tx.Commit()
		}
	})
}

// BenchmarkSegDBTxAxisAGuardFlush1100 measures the
// second suspect separately: 1100 overwrites of one
// key through the facade (the Phase E capacity test
// shape), reporting the checkpoint-generation delta
// as the guard-flush count proxy (each guard flush
// publishes a new generation). It runs only under
// -bench, never in a plain go test run.
func BenchmarkSegDBTxAxisAGuardFlush1100(b *testing.B) {
	dir := b.TempDir()
	wasmBytes, err := os.ReadFile("build/yowza64.wasm")
	if err != nil {
		b.Fatalf("read wasm: %v", err)
	}
	p, err := OpenPersistedSegDB(dir, wasmBytes, PersistedOptions{NumShards: 1, Durability: DurabilitySync})
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	defer p.Close()
	gen0 := p.Generation()
	for i := 0; i < 1100; i++ {
		if err := p.Put("hist", []byte(fmt.Sprintf("hv:%d", i))); err != nil {
			b.Fatalf("put %d: %v", i, err)
		}
	}
	b.ReportMetric(float64(p.Generation()-gen0), "guard-flush-generations")
	if v, err := p.Get("hist"); err != nil || string(v) != "hv:1099" {
		b.Fatalf("final get = %q err %v, want hv:1099", v, err)
	}
}
