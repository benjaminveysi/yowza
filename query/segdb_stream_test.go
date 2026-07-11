package query

import (
	"context"
	"fmt"
	"os"
	"testing"

	yowza "github.com/yowza/yowza"
)

// Deep-integration part 1: shared snapshot for JOINs + truly
// streaming SegDB scan. See DEEP_SNAPSHOT_STREAM_NOTES.md for the
// contract these tests pin down.

// loadSegDBBothBinaries builds a SegDB over each built wasm binary
// (wasm32 + wasm64), skipping a binary that is absent. Unlike
// loadTestDB (wasm32 only), the snapshot/page contract must hold
// on both pointer paths.
func loadSegDBBothBinaries(t *testing.T) map[string]*yowza.SegDB {
	t.Helper()
	out := map[string]*yowza.SegDB{}
	for _, bin := range []string{"../build/yowza.wasm", "../build/yowza64.wasm"} {
		wasmBytes, err := os.ReadFile(bin)
		if err != nil {
			t.Logf("skipping %s (run make all first): %v", bin, err)
			continue
		}
		db, err := yowza.NewYowzaDB(wasmBytes)
		if err != nil {
			t.Fatalf("NewYowzaDB %s: %v", bin, err)
		}
		t.Cleanup(func() { db.Close() })
		sdb, err := yowza.NewSegDB(db)
		if err != nil {
			t.Fatalf("NewSegDB %s: %v", bin, err)
		}
		out[bin] = sdb
	}
	if len(out) == 0 {
		t.Skip("no built wasm binary (run make all first)")
	}
	return out
}

func mustPut(t *testing.T, sdb *yowza.SegDB, k, v string) {
	t.Helper()
	if err := sdb.Put(k, []byte(v)); err != nil {
		t.Fatalf("Put %s: %v", k, err)
	}
}

func scanAtKeys(t *testing.T, sdb *yowza.SegDB, prefix string, seq uint64) map[string]string {
	t.Helper()
	entries, err := sdb.ScanAt(prefix, seq)
	if err != nil {
		t.Fatalf("ScanAt %q seq %d: %v", prefix, seq, err)
	}
	got := map[string]string{}
	for _, e := range entries {
		got[string(e.Key)] = string(e.Val)
	}
	return got
}

// SnapshotSeq advances once per committed put, survives flush
// unchanged, and ScanAt at an older sequence sees the older state
// — including an older live value after a flushed tombstone for
// the same key (the tombstone's newer sequence must not shadow
// the past).
func TestSegDBSnapshotSeqAndScanAt(t *testing.T) {
	for bin, sdb := range loadSegDBBothBinaries(t) {
		t.Run(bin, func(t *testing.T) {
			mustPut(t, sdb, "user:1", "alice-v1")
			mustPut(t, sdb, "user:2", "bob")
			seq2, err := sdb.SnapshotSeq()
			if err != nil {
				t.Fatalf("SnapshotSeq: %v", err)
			}
			if seq2 != 2 {
				t.Fatalf("SnapshotSeq after 2 puts = %d, want 2 (one per committed put)", seq2)
			}
			if err := sdb.Flush(); err != nil {
				t.Fatalf("Flush: %v", err)
			}
			if seq, err := sdb.SnapshotSeq(); err != nil || seq != 2 {
				t.Fatalf("SnapshotSeq after flush = %d, err %v, want 2 (flush preserves the sum)", seq, err)
			}
			mustPut(t, sdb, "user:3", "carol")
			if seq, err := sdb.SnapshotSeq(); err != nil || seq != 3 {
				t.Fatalf("SnapshotSeq after 3rd put = %d, err %v, want 3", seq, err)
			}
			if got := scanAtKeys(t, sdb, "user:", 2); len(got) != 2 || got["user:1"] != "alice-v1" || got["user:2"] != "bob" {
				t.Fatalf("ScanAt seq 2 = %v, want {user:1 alice-v1, user:2 bob} (user:3 must be invisible)", got)
			}
			if got := scanAtKeys(t, sdb, "user:", 0); len(got) != 0 {
				t.Fatalf("ScanAt seq 0 = %v, want empty (literal semantics: no version-0 terminals)", got)
			}
			// Update + delete, then flush the tombstone: the old
			// snapshot must still see the old live values.
			// (A segment-resident delete builds its tombstone
			// via insert-empty + delete, so the sequence may
			// advance by more than one — capture it, don't
			// assume it.)
			mustPut(t, sdb, "user:1", "alice-v2")
			if err := sdb.Delete("user:2"); err != nil {
				t.Fatalf("Delete user:2: %v", err)
			}
			if err := sdb.Flush(); err != nil {
				t.Fatalf("Flush tombstone: %v", err)
			}
			seqNow, err := sdb.SnapshotSeq()
			if err != nil {
				t.Fatalf("SnapshotSeq after delete: %v", err)
			}
			if seqNow <= 3 {
				t.Fatalf("SnapshotSeq after delete = %d, want > 3", seqNow)
			}
			if got := scanAtKeys(t, sdb, "user:", 2); len(got) != 2 || got["user:1"] != "alice-v1" || got["user:2"] != "bob" {
				t.Fatalf("ScanAt seq 2 after flushed tombstone = %v, want old values (tombstone seq %d must not shadow the past)", got, seqNow)
			}
			if got := scanAtKeys(t, sdb, "user:", seqNow); len(got) != 2 || got["user:1"] != "alice-v2" || got["user:3"] != "carol" {
				t.Fatalf("ScanAt seq %d = %v, want {user:1 alice-v2, user:3 carol}", seqNow, got)
			}
			// Legacy Scan with 0 still means current (checked
			// before the user:0 put below changes current).
			entries, err := sdb.Scan("user:", 0)
			if err != nil || len(entries) != 2 {
				t.Fatalf("legacy Scan(0) = %d entries, err %v, want 2 (0 = current is preserved)", len(entries), err)
			}
			// New key sorting before every older key: at the
			// old sequence it must be skipped WITHOUT
			// truncating the older rows.
			mustPut(t, sdb, "user:0", "zero")
			if got := scanAtKeys(t, sdb, "user:", seqNow); len(got) != 2 || got["user:1"] != "alice-v2" || got["user:3"] != "carol" {
				t.Fatalf("ScanAt seq %d after newer smallest key = %v, want old 2 rows (winner-less group must not end the scan)", seqNow, got)
			}
		})
	}
}

// A write committed between two table Opens must not appear in
// one half of a JOIN: open users, commit an order, open orders at
// the same epoch, and the new order must be invisible to the
// orders iterator. Closing both iterators ends the epoch, so a
// later Open sees the order. The two sources are distinct
// structs over the same *SegDB, proving the coordinator is
// per-database, not per-source-object. The SegDBSnapshotSource
// interface (SnapshotSeq + ScanAt) is exercised directly too.
func TestSegDBSourceSharedSnapshotDirect(t *testing.T) {
	db := loadTestDB(t)
	sdb, err := yowza.NewSegDB(db)
	if err != nil {
		t.Fatalf("NewSegDB: %v", err)
	}
	mustPut(t, sdb, "user:1", `{"name":"Alice","age":30,"city":"Tehran","active":true,"score":9.5}`)
	srcUsers := &SegDBSource{DB: sdb}
	srcOrders := &SegDBSource{DB: sdb}
	// SegDBSnapshotSource interface, direct: sequence before the
	// write, ScanAt at that sequence after the write.
	seqBefore, err := srcOrders.SnapshotSeq(context.Background())
	if err != nil {
		t.Fatalf("SnapshotSeq: %v", err)
	}
	ub := bindingFor(t, testSchema(), "users")
	ob := bindingFor(t, testSchema(), "orders")
	itUsers, err := srcUsers.Open(context.Background(), ub)
	if err != nil {
		t.Fatalf("open users: %v", err)
	}
	// Write committed between the two Opens.
	mustPut(t, sdb, "order:1", `{"user_id":"user:1","total":120.0,"item":"book"}`)
	itOrders, err := srcOrders.Open(context.Background(), ob)
	if err != nil {
		t.Fatalf("open orders: %v", err)
	}
	collect := func(it RowIterator) []Row {
		var rows []Row
		for {
			batch, err := it.NextBatch(64)
			if err != nil {
				t.Fatalf("next batch: %v", err)
			}
			if len(batch) == 0 {
				return rows
			}
			rows = append(rows, batch...)
		}
	}
	if rows := collect(itOrders); len(rows) != 0 {
		t.Fatalf("orders iterator at shared epoch saw %d rows, want 0 (order:1 committed after epoch start)", len(rows))
	}
	if rows := collect(itUsers); len(rows) != 1 {
		t.Fatalf("users iterator saw %d rows, want 1", len(rows))
	}
	// ScanAt at the pre-write sequence agrees with the epoch.
	rawIt, err := srcOrders.ScanAt(context.Background(), "order:", seqBefore)
	if err != nil {
		t.Fatalf("ScanAt: %v", err)
	}
	var raw []RawEntry
	for {
		batch, err := rawIt.NextBatch(64)
		if err != nil {
			t.Fatalf("raw next batch: %v", err)
		}
		if len(batch) == 0 {
			break
		}
		raw = append(raw, batch...)
	}
	if err := rawIt.Close(); err != nil {
		t.Fatalf("raw close: %v", err)
	}
	if len(raw) != 0 {
		t.Fatalf("ScanAt at pre-write seq saw %d rows, want 0", len(raw))
	}
	if err := itOrders.Close(); err != nil {
		t.Fatalf("close orders: %v", err)
	}
	if err := itUsers.Close(); err != nil {
		t.Fatalf("close users: %v", err)
	}
	// Epoch ended: a fresh Open sees the order.
	itOrders2, err := srcOrders.Open(context.Background(), ob)
	if err != nil {
		t.Fatalf("reopen orders: %v", err)
	}
	defer itOrders2.Close()
	if rows := collect(itOrders2); len(rows) != 1 {
		t.Fatalf("orders iterator after epoch end saw %d rows, want 1", len(rows))
	}
}

// JOIN through streaming SegDB sources, twice: the second query
// (after a new row) must see the new row, proving epochs end on
// Close and do not pin a stale snapshot across queries.
func TestSegDBSourceSharedSnapshotJoin(t *testing.T) {
	db := loadTestDB(t)
	sdb, err := yowza.NewSegDB(db)
	if err != nil {
		t.Fatalf("NewSegDB: %v", err)
	}
	mustPut(t, sdb, "user:1", `{"name":"Alice","age":30,"city":"Tehran","active":true,"score":9.5}`)
	mustPut(t, sdb, "order:1", `{"user_id":"user:1","total":120.0,"item":"book"}`)
	plan, err := Compile("SELECT users.name, orders.total FROM users JOIN orders ON users.key = orders.user_id WHERE orders.total > 100", testSchema())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	run := func() []Row {
		rows, _, err := Execute(context.Background(), plan, Contract{}, map[string]Source{
			"users":  &SegDBSource{DB: sdb},
			"orders": &SegDBSource{DB: sdb},
		})
		if err != nil {
			t.Fatalf("execute: %v", err)
		}
		return rows
	}
	if rows := run(); len(rows) != 1 {
		t.Fatalf("first JOIN rows = %d, want 1", len(rows))
	}
	mustPut(t, sdb, "order:2", `{"user_id":"user:1","total":200.0,"item":"pen"}`)
	if rows := run(); len(rows) != 2 {
		t.Fatalf("second JOIN rows = %d, want 2 (epoch from first query must not pin the snapshot)", len(rows))
	}
}

// Streaming scan over a result set larger than the old 8192
// candidate cap: 9000 rows flushed in 400-row batches (wasm hot
// arena is 256 KiB, so batches stay small), streamed through
// ScanPage in order, with a mid-stream write invisible at the
// frozen sequence and visible at the new sequence. ScanAt (the
// page loop) must succeed where legacy Scan fails.
func TestSegDBStreamingScanLarge(t *testing.T) {
	const total = 9000
	// Flush every 400 rows: JSON values make each WAL record
	// ~94 bytes, so 400 puts (~38 KiB) stay under the 64 KiB
	// wasm WAL; 900-row batches overflow it (found at put 745).
	const batch = 400
	for bin, sdb := range loadSegDBBothBinaries(t) {
		t.Run(bin, func(t *testing.T) {
			for start := 0; start < total; start += batch {
				end := start + batch
				if end > total {
					end = total
				}
				for i := start; i < end; i++ {
					mustPut(t, sdb, fmt.Sprintf("k:%08d", i), fmt.Sprintf("v:%d", i))
				}
				if err := sdb.Flush(); err != nil {
					t.Fatalf("Flush at %d: %v", end, err)
				}
			}
			seq, err := sdb.SnapshotSeq()
			if err != nil {
				t.Fatalf("SnapshotSeq: %v", err)
			}
			// Stream through ScanPage, in order, no duplicates.
			var count int
			var skip uint64
			for {
				page, err := sdb.ScanPage("k:", seq, skip, 64)
				if err != nil {
					t.Fatalf("ScanPage skip %d: %v", skip, err)
				}
				if len(page) == 0 {
					break
				}
				for j, e := range page {
					want := fmt.Sprintf("k:%08d", count+j)
					if string(e.Key) != want || string(e.Val) != fmt.Sprintf("v:%d", count+j) {
						t.Fatalf("stream row %d = (%s,%s), want (%s,v:%d)", count+j, e.Key, e.Val, want, count+j)
					}
				}
				count += len(page)
				skip += uint64(len(page))
			}
			if count != total {
				t.Fatalf("streamed %d rows, want %d", count, total)
			}
			// Legacy Scan must fail on this size (old cap / out
			// buffer), while ScanAt streams it fine.
			if _, err := sdb.Scan("k:", 0); err == nil {
				t.Fatalf("legacy Scan over %d rows succeeded, want failure (old cap)", total)
			}
			if entries, err := sdb.ScanAt("k:", seq); err != nil || len(entries) != total {
				t.Fatalf("ScanAt = %d entries, err %v, want %d", len(entries), err, total)
			}
			// Mid-stream write: new row + update, both invisible
			// at the frozen sequence, visible at the new one.
			mustPut(t, sdb, "k:00009000", "v:9000")
			mustPut(t, sdb, "k:00000000", "v:CHANGED")
			var frozen int
			skip = 0
			for {
				page, err := sdb.ScanPage("k:", seq, skip, 64)
				if err != nil {
					t.Fatalf("frozen ScanPage skip %d: %v", skip, err)
				}
				if len(page) == 0 {
					break
				}
				for _, e := range page {
					if string(e.Key) == "k:00000000" && string(e.Val) != "v:0" {
						t.Fatalf("frozen stream saw updated k:00000000 = %s, want v:0", e.Val)
					}
				}
				frozen += len(page)
				skip += uint64(len(page))
			}
			if frozen != total {
				t.Fatalf("frozen stream saw %d rows, want %d (new row must be invisible)", frozen, total)
			}
			seq2, err := sdb.SnapshotSeq()
			if err != nil {
				t.Fatalf("SnapshotSeq 2: %v", err)
			}
			if entries, err := sdb.ScanAt("k:", seq2); err != nil || len(entries) != total+1 {
				t.Fatalf("ScanAt at new seq = %d entries, err %v, want %d", len(entries), err, total+1)
			} else if string(entries[0].Val) != "v:CHANGED" {
				t.Fatalf("ScanAt at new seq first value = %s, want v:CHANGED", entries[0].Val)
			}
		})
	}
}

// Query-level streaming: a 9000-row table through Execute must
// return every row through the streaming source (Open performs
// no scan; rows arrive in batches from ScanPage).
func TestSegDBSourceStreamingQueryLarge(t *testing.T) {
	db := loadTestDB(t)
	sdb, err := yowza.NewSegDB(db)
	if err != nil {
		t.Fatalf("NewSegDB: %v", err)
	}
	const total = 9000
	// Flush every 400 rows: JSON values make each WAL record
	// ~94 bytes, so 400 puts (~38 KiB) stay under the 64 KiB
	// wasm WAL; 900-row batches overflow it (found at put 745).
	const batch = 400
	for start := 0; start < total; start += batch {
		end := start + batch
		if end > total {
			end = total
		}
		for i := start; i < end; i++ {
			mustPut(t, sdb, fmt.Sprintf("user:%08d", i), fmt.Sprintf(`{"name":"n%d","age":30,"city":"Tehran","active":true,"score":1.0}`, i))
		}
		if err := sdb.Flush(); err != nil {
			t.Fatalf("Flush at %d: %v", end, err)
		}
	}
	plan, err := Compile("SELECT key FROM users", testSchema())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	rows, _, err := Execute(context.Background(), plan, Contract{}, map[string]Source{"users": &SegDBSource{DB: sdb}})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if len(rows) != total {
		t.Fatalf("query rows = %d, want %d", len(rows), total)
	}
}
