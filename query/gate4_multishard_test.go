package query

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"
)

// Gate 4.2/4.3/5b tests: ISA execution over multi-shard
// storage, resource policy, and consistency sessions.
// The reference for every case is the Go executor over
// the same data combined in shard order (host order, then
// key order within a host) — the order the coordinator
// scans in — so row order is compared exactly, not as a set.

func gate4Host(t *testing.T, bin string, users, orders []isaKV) *ISAHost {
	t.Helper()
	wasmBytes, err := os.ReadFile(bin)
	if err != nil {
		t.Skipf("wasm binary %s absent: %v", bin, err)
	}
	host, err := NewISAHost(wasmBytes)
	if err != nil {
		t.Fatalf("NewISAHost: %v", err)
	}
	t.Cleanup(func() { _ = host.Close() })
	loadISAHostFixture(t, host, users, orders)
	if err := host.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	return host
}

func gate4Coord(t *testing.T, hosts []*ISAHost, mode MultiShardMode, policy ResourcePolicy) *ISAMultiShardCoordinator {
	t.Helper()
	coord, err := NewISAMultiShardCoordinator(hosts,
		PartitionMetadata{ShardCount: len(hosts), Disjoint: true, Verified: true}, mode, policy)
	if err != nil {
		t.Fatalf("NewISAMultiShardCoordinator: %v", err)
	}
	return coord
}

func gate4Snapshots(t *testing.T, hosts []*ISAHost) []uint64 {
	t.Helper()
	snaps := make([]uint64, len(hosts))
	for i, h := range hosts {
		s, err := h.Snapshot()
		if err != nil {
			t.Fatalf("host %d Snapshot: %v", i, err)
		}
		snaps[i] = s
	}
	return snaps
}

// gate4Ref runs sql through the Go reference over the
// shards' data combined in shard order.
func gate4Ref(t *testing.T, sql string, shardUsers, shardOrders [][]isaKV) []Row {
	t.Helper()
	schema := testSchema()
	var users, orders []isaKV
	for _, sh := range shardUsers {
		users = append(users, visible(sh)...)
	}
	for _, sh := range shardOrders {
		orders = append(orders, visible(sh)...)
	}
	sources := map[string]Source{
		"users":  &matSource{b: bindingFor(t, schema, "users"), entries: users},
		"orders": &matSource{b: bindingFor(t, schema, "orders"), entries: orders},
	}
	plan, err := Compile(sql, schema)
	if err != nil {
		t.Fatalf("compile %q: %v", sql, err)
	}
	rows, _, err := Execute(context.Background(), plan, Contract{}, sources)
	if err != nil {
		t.Fatalf("reference execute %q: %v", sql, err)
	}
	return rows
}

// gate4RunCoord compiles sql, runs it through coord, and
// returns rows + trace.
func gate4RunCoord(t *testing.T, coord *ISAMultiShardCoordinator, sql string, snaps []uint64) ([]Row, *MultiShardTrace) {
	t.Helper()
	plan, err := Compile(sql, testSchema())
	if err != nil {
		t.Fatalf("compile %q: %v", sql, err)
	}
	prog, err := CompileISA(plan, Contract{})
	if err != nil {
		t.Fatalf("CompileISA %q: %v", sql, err)
	}
	rows, tr, err := coord.Execute(context.Background(), prog, plan, snaps)
	if err != nil {
		t.Fatalf("coordinator execute %q: %v", sql, err)
	}
	return rows, tr
}

func gate4RowsEqual(t *testing.T, got, want []Row) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("row count = %d, want %d\n got: %v\nwant: %v", len(got), len(want), got, want)
	}
	for i := range got {
		if len(got[i].Cols) != len(want[i].Cols) || len(got[i].Vals) != len(want[i].Vals) {
			t.Fatalf("row %d shape differs: got %v want %v", i, got[i], want[i])
		}
		for j := range got[i].Cols {
			if got[i].Cols[j] != want[i].Cols[j] {
				t.Fatalf("row %d col %d name = %q, want %q", i, j, got[i].Cols[j], want[i].Cols[j])
			}
			if !valuesEqual(got[i].Vals[j], want[i].Vals[j]) {
				t.Fatalf("row %d col %s = %v, want %v", i, got[i].Cols[j], got[i].Vals[j], want[i].Vals[j])
			}
		}
	}
}

func gate4UsersAB() (a, b []isaKV) {
	a = []isaKV{
		{key: "user:1", val: `{"name":"Alice","age":30,"city":"Tehran","active":true,"score":9.5}`},
		{key: "user:2", val: `{"name":"Bob","age":25,"city":"Shiraz","active":false,"score":7.0}`},
		{key: "user:3", val: `{"name":"Carol","age":40,"city":"Tehran","active":true,"score":8.0}`},
	}
	b = []isaKV{
		{key: "user:4", val: `{"name":"Dan","age":22,"active":true,"score":6.5}`},
		{key: "user:5", val: `{"name":"Eve","age":35,"city":"Tabriz","active":false,"score":9.0}`},
		{key: "user:6", val: `{"name":"Frank","city":"Tehran","active":true,"score":5.5}`},
	}
	return a, b
}

func gate4OrdersAB() (a, b []isaKV) {
	// Deliberately cross-shard: order:2 (shard A) joins a
	// user on shard B, and order:4 (shard B) joins a user
	// on shard A. Per-shard joins would drop both.
	a = []isaKV{
		{key: "order:1", val: `{"user_id":"user:1","total":120.0,"item":"book"}`},
		{key: "order:2", val: `{"user_id":"user:4","total":45.5,"item":"pen"}`},
	}
	b = []isaKV{
		{key: "order:3", val: `{"user_id":"user:3","total":300.0,"item":"laptop"}`},
		{key: "order:4", val: `{"user_id":"user:1","total":10.0,"item":"mug"}`},
		{key: "order:5", val: `{"user_id":"user:99","total":50.0,"item":"ghost"}`},
	}
	return a, b
}

func TestGate4MultiShardJoinCrossShard(t *testing.T) {
	for _, bin := range []string{"../build/yowza.wasm", "../build/yowza64.wasm"} {
		t.Run(bin, func(t *testing.T) {
			ua, ub := gate4UsersAB()
			oa, ob := gate4OrdersAB()
			h1 := gate4Host(t, bin, ua, oa)
			h2 := gate4Host(t, bin, ub, ob)
			coord := gate4Coord(t, []*ISAHost{h1, h2}, ModeReject, DefaultResourcePolicy())
			sql := "SELECT users.name, orders.total FROM users JOIN orders ON users.key = orders.user_id"
			got, _ := gate4RunCoord(t, coord, sql, gate4Snapshots(t, []*ISAHost{h1, h2}))
			want := gate4Ref(t, sql, [][]isaKV{ua, ub}, [][]isaKV{oa, ob})
			gate4RowsEqual(t, got, want)
			if len(got) != 4 {
				t.Fatalf("join returned %d rows, want 4 (both cross-shard matches included)", len(got))
			}
			// The OLD coordinator must still refuse this
			// plan: the new result must not come from a
			// per-shard concatenation.
			old, err := NewISAShardCoordinator(h1, h2)
			if err != nil {
				t.Fatalf("old coordinator: %v", err)
			}
			plan, _ := Compile(sql, testSchema())
			prog, _ := CompileISA(plan, Contract{})
			if _, _, err := old.ExecuteAt(prog, plan, gate4Snapshots(t, []*ISAHost{h1, h2})); !errors.Is(err, ErrISACrossShardJoin) {
				t.Fatalf("old coordinator error = %v, want ErrISACrossShardJoin", err)
			}
		})
	}
}

func TestGate4MultiShardAggregateMerge(t *testing.T) {
	for _, bin := range []string{"../build/yowza.wasm", "../build/yowza64.wasm"} {
		t.Run(bin, func(t *testing.T) {
			ua, ub := gate4UsersAB()
			h1 := gate4Host(t, bin, ua, nil)
			h2 := gate4Host(t, bin, ub, nil)
			coord := gate4Coord(t, []*ISAHost{h1, h2}, ModeReject, DefaultResourcePolicy())
			sql := "SELECT city, COUNT(*) FROM users GROUP BY city"
			got, _ := gate4RunCoord(t, coord, sql, gate4Snapshots(t, []*ISAHost{h1, h2}))
			want := gate4Ref(t, sql, [][]isaKV{ua, ub}, nil)
			gate4RowsEqual(t, got, want)
			// Tehran must be ONE group with count 3, not
			// one group per shard.
			tehran := 0
			for _, r := range got {
				if len(r.Vals) >= 2 && r.Vals[0].S == "Tehran" {
					tehran++
					if r.Vals[1].I != 3 {
						t.Fatalf("Tehran count = %d, want 3 (global merge)", r.Vals[1].I)
					}
				}
			}
			if tehran != 1 {
				t.Fatalf("Tehran groups = %d, want exactly 1", tehran)
			}
		})
	}
}

func TestGate4MultiShardSortAndLimitGlobal(t *testing.T) {
	for _, bin := range []string{"../build/yowza.wasm", "../build/yowza64.wasm"} {
		t.Run(bin, func(t *testing.T) {
			ua, ub := gate4UsersAB()
			h1 := gate4Host(t, bin, ua, nil)
			h2 := gate4Host(t, bin, ub, nil)
			coord := gate4Coord(t, []*ISAHost{h1, h2}, ModeReject, DefaultResourcePolicy())
			snaps := gate4Snapshots(t, []*ISAHost{h1, h2})
			sortSQL := "SELECT name, age FROM users ORDER BY age"
			got, _ := gate4RunCoord(t, coord, sortSQL, snaps)
			gate4RowsEqual(t, got, gate4Ref(t, sortSQL, [][]isaKV{ua, ub}, nil))
			// NULL age (Frank) sorts LAST even ascending.
			if len(got) != 6 || got[5].Vals[0].S != "Frank" {
				t.Fatalf("sort tail = %v, want Frank (NULL last)", got)
			}
			limSQL := "SELECT name, age FROM users ORDER BY age LIMIT 3"
			gotLim, _ := gate4RunCoord(t, coord, limSQL, snaps)
			gate4RowsEqual(t, gotLim, gate4Ref(t, limSQL, [][]isaKV{ua, ub}, nil))
			if len(gotLim) != 3 || gotLim[0].Vals[0].S != "Dan" {
				t.Fatalf("LIMIT after global sort = %v, want [Dan Bob Alice]", gotLim)
			}
			descSQL := "SELECT name, age FROM users ORDER BY age DESC"
			gotDesc, _ := gate4RunCoord(t, coord, descSQL, snaps)
			gate4RowsEqual(t, gotDesc, gate4Ref(t, descSQL, [][]isaKV{ua, ub}, nil))
			if gotDesc[0].Vals[0].S != "Carol" || gotDesc[5].Vals[0].S != "Frank" {
				t.Fatalf("DESC sort = %v, want Carol first, Frank (NULL) last", gotDesc)
			}
		})
	}
}

func TestGate4MultiShardAggregateSpillRepeat(t *testing.T) {
	bin := "../build/yowza.wasm"
	// 40 users, cities alternating Alpha/Beta, split
	// across shards; a tiny memory budget forces group
	// spills, and groups repeat after every spill
	// boundary. Counts must still be exact (20/20).
	var ua, ub []isaKV
	for i := 0; i < 40; i++ {
		city := "Alpha"
		if i%2 == 1 {
			city = "Beta"
		}
		kv := isaKV{key: fmt.Sprintf("user:%02d", i),
			val: fmt.Sprintf(`{"name":"U%02d","age":%d,"city":%q,"active":true,"score":1.0}`, i, 20+i, city)}
		if i < 20 {
			ua = append(ua, kv)
		} else {
			ub = append(ub, kv)
		}
	}
	h1 := gate4Host(t, bin, ua, nil)
	h2 := gate4Host(t, bin, ub, nil)
	pol := DefaultResourcePolicy()
	pol.MaxMemoryBytes = 100 // forces group spills
	coord := gate4Coord(t, []*ISAHost{h1, h2}, ModeReject, pol)
	sql := "SELECT city, COUNT(*) FROM users GROUP BY city"
	got, tr := gate4RunCoord(t, coord, sql, gate4Snapshots(t, []*ISAHost{h1, h2}))
	gate4RowsEqual(t, got, gate4Ref(t, sql, [][]isaKV{ua, ub}, nil))
	if !tr.Spilled || tr.SpillRuns == 0 {
		t.Fatalf("trace spilled=%v runs=%d, want spill to have occurred", tr.Spilled, tr.SpillRuns)
	}
	for _, r := range got {
		if r.Vals[1].I != 20 {
			t.Fatalf("city %s count = %d, want 20", r.Vals[0].S, r.Vals[1].I)
		}
	}
}

func TestGate4MultiShardSortSpill(t *testing.T) {
	bin := "../build/yowza.wasm"
	var ua, ub []isaKV
	for i := 0; i < 30; i++ {
		kv := isaKV{key: fmt.Sprintf("user:%02d", i),
			val: fmt.Sprintf(`{"name":"U%02d","age":%d,"city":"C","active":true,"score":1.0}`, i, (i*7)%30)}
		if i < 15 {
			ua = append(ua, kv)
		} else {
			ub = append(ub, kv)
		}
	}
	h1 := gate4Host(t, bin, ua, nil)
	h2 := gate4Host(t, bin, ub, nil)
	pol := DefaultResourcePolicy()
	pol.MaxSortRows = 5 // forces external merge sort
	coord := gate4Coord(t, []*ISAHost{h1, h2}, ModeReject, pol)
	sql := "SELECT name, age FROM users ORDER BY age"
	got, tr := gate4RunCoord(t, coord, sql, gate4Snapshots(t, []*ISAHost{h1, h2}))
	gate4RowsEqual(t, got, gate4Ref(t, sql, [][]isaKV{ua, ub}, nil))
	if !tr.Spilled || tr.SpillRuns == 0 {
		t.Fatalf("sort spill trace spilled=%v runs=%d, want spill", tr.Spilled, tr.SpillRuns)
	}
}

func TestGate4MultiShardJoinSpill(t *testing.T) {
	bin := "../build/yowza.wasm"
	ua, ub := gate4UsersAB()
	var oa, ob []isaKV
	for i := 0; i < 10; i++ {
		kv := isaKV{key: fmt.Sprintf("order:%02d", i),
			val: fmt.Sprintf(`{"user_id":"user:%d","total":%d.0,"item":"i%d"}`, 1+i%6, 10+i, i)}
		if i < 5 {
			oa = append(oa, kv)
		} else {
			ob = append(ob, kv)
		}
	}
	h1 := gate4Host(t, bin, ua, oa)
	h2 := gate4Host(t, bin, ub, ob)
	pol := DefaultResourcePolicy()
	pol.MaxJoinRows = 3 // forces chunked spill-backed build
	coord := gate4Coord(t, []*ISAHost{h1, h2}, ModeReject, pol)
	sql := "SELECT users.name, orders.total FROM users JOIN orders ON users.key = orders.user_id"
	got, tr := gate4RunCoord(t, coord, sql, gate4Snapshots(t, []*ISAHost{h1, h2}))
	gate4RowsEqual(t, got, gate4Ref(t, sql, [][]isaKV{ua, ub}, [][]isaKV{oa, ob}))
	if !tr.Spilled || tr.SpillRuns == 0 {
		t.Fatalf("join spill trace spilled=%v runs=%d, want spill", tr.Spilled, tr.SpillRuns)
	}
}

func TestGate4MultiShardPartitionModes(t *testing.T) {
	bin := "../build/yowza.wasm"
	ua, _ := gate4UsersAB()
	// Reject mode: unverified partition refuses before
	// executing anything.
	h1 := gate4Host(t, bin, ua, nil)
	h2 := gate4Host(t, bin, ua, nil)
	coord, err := NewISAMultiShardCoordinator([]*ISAHost{h1, h2},
		PartitionMetadata{Disjoint: false, Verified: false}, ModeReject, DefaultResourcePolicy())
	if err != nil {
		t.Fatalf("coordinator: %v", err)
	}
	plan, _ := Compile("SELECT name FROM users", testSchema())
	prog, _ := CompileISA(plan, Contract{})
	if _, _, err := coord.Execute(context.Background(), prog, plan, gate4Snapshots(t, []*ISAHost{h1, h2})); !errors.Is(err, ErrUnclearPartition) {
		t.Fatalf("unverified partition error = %v, want ErrUnclearPartition", err)
	}
	// Dedup mode: identical replicas deduplicate.
	dedup, err := NewISAMultiShardCoordinator([]*ISAHost{h1, h2},
		PartitionMetadata{Disjoint: false, Verified: false}, ModeDedup, DefaultResourcePolicy())
	if err != nil {
		t.Fatalf("dedup coordinator: %v", err)
	}
	rows, _, err := dedup.Execute(context.Background(), prog, plan, gate4Snapshots(t, []*ISAHost{h1, h2}))
	if err != nil {
		t.Fatalf("dedup execute: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("dedup rows = %d, want 3 (replicas collapsed)", len(rows))
	}
	// Dedup mode: same key, different row = conflict, no rows.
	conflict := []isaKV{{key: "user:1", val: `{"name":"Mallory","age":30,"city":"Tehran","active":true,"score":9.5}`}}
	h3 := gate4Host(t, bin, conflict, nil)
	dedup2, err := NewISAMultiShardCoordinator([]*ISAHost{h1, h3},
		PartitionMetadata{Disjoint: false, Verified: false}, ModeDedup, DefaultResourcePolicy())
	if err != nil {
		t.Fatalf("dedup2 coordinator: %v", err)
	}
	if _, _, err := dedup2.Execute(context.Background(), prog, plan, gate4Snapshots(t, []*ISAHost{h1, h3})); !errors.Is(err, ErrPartitionConflict) {
		t.Fatalf("conflict error = %v, want ErrPartitionConflict", err)
	}
	// Snapshot-count mismatch refuses.
	if _, _, err := coord.Execute(context.Background(), prog, plan, []uint64{1}); err == nil {
		t.Fatalf("snapshot mismatch must fail")
	}
}

func TestGate4MultiShardSession(t *testing.T) {
	bin := "../build/yowza.wasm"
	ua, ub := gate4UsersAB()
	oa, ob := gate4OrdersAB()
	h1 := gate4Host(t, bin, ua, oa)
	h2 := gate4Host(t, bin, ub, ob)
	dom := NewConsistencyDomain("gate4-dom")
	_ = dom.RegisterParticipant("shard0")
	_ = dom.RegisterParticipant("shard1")
	sess, err := dom.BeginSession([]string{"shard0", "shard1"}, time.Minute)
	if err != nil {
		t.Fatalf("BeginSession: %v", err)
	}
	snaps := gate4Snapshots(t, []*ISAHost{h1, h2})
	if err := sess.Attach("shard0", snaps[0]); err != nil {
		t.Fatalf("Attach shard0: %v", err)
	}
	if err := sess.Attach("shard1", snaps[1]); err != nil {
		t.Fatalf("Attach shard1: %v", err)
	}
	coord := gate4Coord(t, []*ISAHost{h1, h2}, ModeReject, DefaultResourcePolicy())
	sql := "SELECT users.name, orders.total FROM users JOIN orders ON users.key = orders.user_id"
	plan, _ := Compile(sql, testSchema())
	prog, _ := CompileISA(plan, Contract{})
	rows, _, err := coord.ExecuteWithSession(context.Background(), prog, plan, sess, []string{"shard0", "shard1"})
	if err != nil {
		t.Fatalf("ExecuteWithSession: %v", err)
	}
	gate4RowsEqual(t, rows, gate4Ref(t, sql, [][]isaKV{ua, ub}, [][]isaKV{oa, ob}))
	// Unknown participant refuses.
	if _, _, err := coord.ExecuteWithSession(context.Background(), prog, plan, sess, []string{"shard0", "ghost"}); !errors.Is(err, ErrUnknownParticipant) {
		t.Fatalf("unknown participant error = %v, want ErrUnknownParticipant", err)
	}
	// Expired session refuses with ErrSessionExpired and
	// executes nothing.
	short, err := dom.BeginSession([]string{"shard0", "shard1"}, time.Millisecond)
	if err != nil {
		t.Fatalf("short session: %v", err)
	}
	_ = short.Attach("shard0", snaps[0])
	_ = short.Attach("shard1", snaps[1])
	time.Sleep(5 * time.Millisecond)
	if _, _, err := coord.ExecuteWithSession(context.Background(), prog, plan, short, []string{"shard0", "shard1"}); !errors.Is(err, ErrSessionExpired) {
		t.Fatalf("expired session error = %v, want ErrSessionExpired", err)
	}
	// A query across two domains is refused explicitly:
	// only independent snapshots exist there.
	dom2 := NewConsistencyDomain("gate4-dom-2")
	_ = dom2.RegisterParticipant("other")
	other, _ := dom2.BeginSession([]string{"other"}, time.Minute)
	if err := RequireSameDomain(sess, other); !errors.Is(err, ErrNotInSameDomain) {
		t.Fatalf("cross-domain RequireSameDomain = %v, want ErrNotInSameDomain", err)
	}
}
