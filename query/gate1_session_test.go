package query

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	yowza "github.com/yowza/yowza"
)

// GATE 1.1 through Go APIs: hot-layer history. A key's old
// value survives an update and a delete at an old snapshot,
// before AND after flush (history segments), on both the raw
// skip reference path and the cursor source path.
func TestGate1HotHistoryGo(t *testing.T) {
	db := loadTestDB(t)
	sdb, err := yowza.NewSegDB(db)
	if err != nil {
		t.Fatalf("NewSegDB: %v", err)
	}
	src := &SegDBSource{DB: sdb}
	putJSON := func(k, name string) {
		t.Helper()
		mustPut(t, sdb, k, fmt.Sprintf(`{"name":%q,"age":30,"city":"Tehran","active":true,"score":9.5}`, name))
	}
	putJSON("user:9", "V1")
	snap, err := sdb.SnapshotSeq()
	if err != nil {
		t.Fatalf("SnapshotSeq: %v", err)
	}
	putJSON("user:9", "V2")
	// Old snapshot sees V1 on both paths; current sees V2.
	for _, tc := range []struct {
		name string
		rows func() []string
	}{
		{"skip-reference", func() []string {
			ents, err := sdb.ScanAt("user:", snap)
			if err != nil {
				t.Fatalf("ScanAt old: %v", err)
			}
			var out []string
			for _, e := range ents {
				out = append(out, string(e.Val))
			}
			return out
		}},
		{"cursor-source", func() []string {
			it, err := src.ScanAt(context.Background(), "user:", snap)
			if err != nil {
				t.Fatalf("source ScanAt old: %v", err)
			}
			rows := gate1CursorRows(t, it)
			_ = it.Close()
			var out []string
			for _, r := range rows {
				out = append(out, r[len("user:9="):])
			}
			return out
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.rows()
			if len(got) != 1 {
				t.Fatalf("old snapshot rows = %v, want 1 (V1)", got)
			}
			if got[0] != `{"name":"V1","age":30,"city":"Tehran","active":true,"score":9.5}` {
				t.Fatalf("old snapshot value = %s, want V1", got[0])
			}
		})
	}
	// Delete: old snapshot still reads V1.
	if err := sdb.Delete("user:9"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	it, err := src.ScanAt(context.Background(), "user:", snap)
	if err != nil {
		t.Fatalf("source ScanAt after delete: %v", err)
	}
	rows := gate1CursorRows(t, it)
	_ = it.Close()
	if len(rows) != 1 {
		t.Fatalf("old snapshot after delete rows = %v, want [V1]", rows)
	}
	// Flush transfers history into segments: old snapshot
	// still V1, current is absent.
	if err := sdb.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	it, err = src.ScanAt(context.Background(), "user:", snap)
	if err != nil {
		t.Fatalf("source ScanAt after flush: %v", err)
	}
	rows = gate1CursorRows(t, it)
	_ = it.Close()
	if len(rows) != 1 {
		t.Fatalf("old snapshot after flush rows = %v, want [V1]", rows)
	}
	cur, err := sdb.SnapshotSeq()
	if err != nil {
		t.Fatalf("SnapshotSeq: %v", err)
	}
	it, err = src.ScanAt(context.Background(), "user:", cur)
	if err != nil {
		t.Fatalf("source ScanAt current: %v", err)
	}
	rows = gate1CursorRows(t, it)
	_ = it.Close()
	if len(rows) != 0 {
		t.Fatalf("current rows = %v, want [] (deleted)", rows)
	}
}

// GATE 1.2 through Go APIs: a stale lease defers Compact
// (segment count unchanged, leased data intact); after release
// Compact proceeds. Negative: releasing twice is a typed error.
func TestGate1LeaseDefersCompactGo(t *testing.T) {
	db := loadTestDB(t)
	sdb, err := yowza.NewSegDB(db)
	if err != nil {
		t.Fatalf("NewSegDB: %v", err)
	}
	mustPut(t, sdb, "k", "v1")
	if err := sdb.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	snap, err := sdb.SnapshotSeq()
	if err != nil {
		t.Fatalf("SnapshotSeq: %v", err)
	}
	mustPut(t, sdb, "k", "v2")
	if err := sdb.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	before, err := sdb.SegmentCount()
	if err != nil {
		t.Fatalf("SegmentCount: %v", err)
	}
	if before != 2 {
		t.Fatalf("segments before = %d, want 2", before)
	}
	id, err := sdb.RegisterLease(snap)
	if err != nil {
		t.Fatalf("RegisterLease: %v", err)
	}
	if err := sdb.Compact(1 << 60); err != nil {
		t.Fatalf("Compact under lease: %v", err)
	}
	after, err := sdb.SegmentCount()
	if err != nil {
		t.Fatalf("SegmentCount: %v", err)
	}
	if after != before {
		t.Fatalf("segments after deferred compact = %d, want %d (unchanged)", after, before)
	}
	ents, err := sdb.ScanAt("k", snap)
	if err != nil {
		t.Fatalf("ScanAt leased: %v", err)
	}
	if len(ents) != 1 || string(ents[0].Val) != "v1" {
		t.Fatalf("leased read = %v, want [v1]", ents)
	}
	if err := sdb.ReleaseLease(id); err != nil {
		t.Fatalf("ReleaseLease: %v", err)
	}
	if err := sdb.ReleaseLease(id); !errors.Is(err, yowza.ErrLeaseExpired) {
		t.Fatalf("double ReleaseLease err = %v, want ErrLeaseExpired", err)
	}
	if err := sdb.Compact(1 << 60); err != nil {
		t.Fatalf("Compact after release: %v", err)
	}
	merged, err := sdb.SegmentCount()
	if err != nil {
		t.Fatalf("SegmentCount: %v", err)
	}
	if merged >= before {
		t.Fatalf("segments after real compact = %d, want < %d", merged, before)
	}
}

// GATE 1.3 acceptance, deterministic form: query 1 starts
// before a commit and keeps its snapshot; a commit happens;
// query 2 starts. Query 1 sees old state, query 2 new state,
// and compaction during query 1 does not destroy query 1's
// history. Then closing the session makes further paging a
// typed error, never partial rows.
func TestGate1SessionIsolation(t *testing.T) {
	db := loadTestDB(t)
	sdb, err := yowza.NewSegDB(db)
	if err != nil {
		t.Fatalf("NewSegDB: %v", err)
	}
	src := &SegDBSource{DB: sdb}
	putUser := func(k, name string) {
		t.Helper()
		mustPut(t, sdb, k, fmt.Sprintf(`{"name":%q,"age":30,"city":"Tehran","active":true,"score":9.5}`, name))
	}
	putUser("user:1", "Alice")
	putUser("user:2", "Bob")
	if err := sdb.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	plan, err := Compile("SELECT name FROM users ORDER BY name", testSchema())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	names := func(rows []Row) []string {
		var out []string
		for _, r := range rows {
			// Raw iterator rows carry qualified columns ("users.name");
			// Execute-projected rows carry the bare name. Accept both.
			n, ok := r.Get("name")
			if !ok {
				n, _ = r.Get("users.name")
			}
			out = append(out, n.S)
		}
		return out
	}
	// Query 1: a session, opened before the commit.
	sess := NewSession()
	ctxS := WithSession(context.Background(), sess)
	binding := bindingFor(t, testSchema(), "users")
	iter1, err := src.Open(ctxS, binding)
	if err != nil {
		t.Fatalf("session Open: %v", err)
	}
	defer iter1.Close()
	// The commit: a new user, and Bob deleted.
	putUser("user:3", "Charlie")
	if err := sdb.Delete("user:2"); err != nil {
		t.Fatalf("Delete Bob: %v", err)
	}
	if err := sdb.Flush(); err != nil {
		t.Fatalf("Flush commit: %v", err)
	}
	// Compaction during query 1 must not destroy its history.
	segsBefore, err := sdb.SegmentCount()
	if err != nil {
		t.Fatalf("SegmentCount: %v", err)
	}
	if err := sdb.Compact(1 << 60); err != nil {
		t.Fatalf("Compact during session: %v", err)
	}
	segsAfter, err := sdb.SegmentCount()
	if err != nil {
		t.Fatalf("SegmentCount: %v", err)
	}
	if segsAfter != segsBefore {
		t.Fatalf("segments changed during session: %d -> %d (compact must defer)", segsBefore, segsAfter)
	}
	// Query 2: a fresh Execute sees the new state.
	rows2, _, err := Execute(context.Background(), plan, Contract{}, map[string]Source{"users": src})
	if err != nil {
		t.Fatalf("query 2 Execute: %v", err)
	}
	if got := names(rows2); len(got) != 2 || got[0] != "Alice" || got[1] != "Charlie" {
		t.Fatalf("query 2 names = %v, want [Alice Charlie]", got)
	}
	// Query 1, consumed now, still sees the old state.
	var oldRows []Row
	for {
		batch, err := iter1.NextBatch(64)
		if err != nil {
			t.Fatalf("query 1 NextBatch: %v", err)
		}
		if len(batch) == 0 {
			break
		}
		oldRows = append(oldRows, batch...)
	}
	if got := names(oldRows); len(got) != 2 || got[0] != "Alice" || got[1] != "Bob" {
		t.Fatalf("query 1 names = %v, want [Alice Bob]", got)
	}
	// The same session re-executed still reads its snapshot.
	rows1b, _, err := Execute(ctxS, plan, Contract{}, map[string]Source{"users": src})
	if err != nil {
		t.Fatalf("query 1 re-Execute: %v", err)
	}
	if got := names(rows1b); len(got) != 2 || got[0] != "Alice" || got[1] != "Bob" {
		t.Fatalf("query 1 re-executed names = %v, want [Alice Bob]", got)
	}
	// Negative: after Session.Close, a live iterator pages
	// into a typed error, not partial rows.
	iter2, err := src.Open(ctxS, binding)
	if err != nil {
		t.Fatalf("session Open 2: %v", err)
	}
	defer iter2.Close()
	if err := sess.Close(); err != nil {
		t.Fatalf("Session.Close: %v", err)
	}
	if _, err := iter2.NextBatch(64); !errors.Is(err, ErrSnapshotExpired) {
		t.Fatalf("NextBatch after session close err = %v, want ErrSnapshotExpired", err)
	}
	if err := sess.Close(); err != nil {
		t.Fatalf("second Session.Close: %v", err)
	}
}

// GATE 1.3, concurrent form: a session Execute and a fresh
// Execute running at the same time over one SegDB must not
// share a sequence. The commit happens before both start, so
// the outcome is deterministic: the session (taken before the
// commit) sees old state, the fresh query sees new state.
func TestGate1SessionConcurrent(t *testing.T) {
	db := loadTestDB(t)
	sdb, err := yowza.NewSegDB(db)
	if err != nil {
		t.Fatalf("NewSegDB: %v", err)
	}
	src := &SegDBSource{DB: sdb}
	mustPut(t, sdb, "user:1", `{"name":"Alice","age":30,"city":"Tehran","active":true,"score":9.5}`)
	if err := sdb.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	sess := NewSession()
	defer sess.Close()
	ctxS := WithSession(context.Background(), sess)
	// Pin the session's snapshot by opening once, then close
	// that iterator (the session keeps the lease).
	binding := bindingFor(t, testSchema(), "users")
	pin, err := src.Open(ctxS, binding)
	if err != nil {
		t.Fatalf("pin Open: %v", err)
	}
	_ = pin.Close()
	// Commit after the session snapshot was taken.
	mustPut(t, sdb, "user:2", `{"name":"Bob","age":25,"city":"Shiraz","active":false,"score":7.0}`)
	if err := sdb.Flush(); err != nil {
		t.Fatalf("Flush commit: %v", err)
	}
	plan, err := Compile("SELECT name FROM users ORDER BY name", testSchema())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	var wg sync.WaitGroup
	var sessRows, freshRows []Row
	var sessErr, freshErr error
	wg.Add(2)
	go func() {
		defer wg.Done()
		sessRows, _, sessErr = Execute(ctxS, plan, Contract{}, map[string]Source{"users": src})
	}()
	go func() {
		defer wg.Done()
		freshRows, _, freshErr = Execute(context.Background(), plan, Contract{}, map[string]Source{"users": src})
	}()
	wg.Wait()
	if sessErr != nil {
		t.Fatalf("session Execute: %v", sessErr)
	}
	if freshErr != nil {
		t.Fatalf("fresh Execute: %v", freshErr)
	}
	if len(sessRows) != 1 {
		t.Fatalf("session rows = %d, want 1 (Alice only)", len(sessRows))
	}
	if len(freshRows) != 2 {
		t.Fatalf("fresh rows = %d, want 2 (Alice, Bob)", len(freshRows))
	}
}

// GATE 1.3, persisted form: one session vector across the
// shard set. A write between the two tables' Opens is
// invisible to both; a fresh query sees it. (Compact
// protection for persisted shards is the documented open
// item — see session.go — so this test does not compact
// mid-session.)
func TestGate1SessionPersistedVector(t *testing.T) {
	p := multishardPersistedDB(t, "../build/yowza.wasm", 4)
	multishardUsersOrders(t, p, 10, 0)
	src := &PersistedMultiShardSource{DB: p}
	sess := NewSession()
	defer sess.Close()
	ctxS := WithSession(context.Background(), sess)
	binding := bindingFor(t, testSchema(), "users")
	iter, err := src.Open(ctxS, binding)
	if err != nil {
		t.Fatalf("session Open: %v", err)
	}
	defer iter.Close()
	// Commit between the session's first Open and its reads.
	multishardPut(t, p, "user:99", `{"name":"Zed","age":40,"city":"Tehran","active":true,"score":8.0}`)
	if err := p.Delete("user:0"); err != nil {
		t.Fatalf("Delete user:0: %v", err)
	}
	if err := p.Flush(); err != nil {
		t.Fatalf("Flush commit: %v", err)
	}
	var oldKeys []string
	for {
		batch, err := iter.NextBatch(64)
		if err != nil {
			t.Fatalf("session NextBatch: %v", err)
		}
		if len(batch) == 0 {
			break
		}
		for _, r := range batch {
			k, _ := r.Get("key")
			oldKeys = append(oldKeys, k.S)
		}
	}
	if len(oldKeys) != 10 {
		t.Fatalf("session keys = %d, want 10 (pre-commit state)", len(oldKeys))
	}
	for _, k := range oldKeys {
		if k == "user:99" {
			t.Fatalf("session saw post-commit user:99")
		}
	}
	// Fresh query sees the commit.
	rows, _ := runQuery(t, "SELECT key FROM users", map[string]Source{"users": src})
	if len(rows) != 10 { // 10 - 1 deleted + 1 added
		t.Fatalf("fresh query rows = %d, want 10", len(rows))
	}
	// Negative: a vector of the wrong length is a typed error.
	if _, err := p.ScanStreaming("user:", []uint64{1, 2}); !errors.Is(err, yowza.ErrPersistedSnapshotMismatch) {
		t.Fatalf("ScanStreaming(wrong vector) err = %v, want ErrPersistedSnapshotMismatch", err)
	}
	// The cursor interface the persisted pager must consume.
	var _ yowza.SegDBCursorSource = (*yowza.SegDB)(nil)
}
