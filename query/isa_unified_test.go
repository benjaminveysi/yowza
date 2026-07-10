package query

import (
	"context"
	"fmt"
	"os"
	"testing"
)

// Unified-contract differential tests (Reliability stage item 02).
// These extend TestISADifferential in three directions the original
// 41 cases do not cover:
//   - SNAPSHOT: ExecuteISAAt at a snapshot taken before a write
//     proves both tables of one ISA query read at that one
//     sequence (a write landing "between" the table reads is
//     invisible to the second table, mirroring the Go
//     SegDBSource shared-snapshot tests).
//   - STREAMING LARGE SCAN: 9000 rows through the ISA streaming
//     path (cursor pages, never materialized as a table).
//   - MEMORY BUDGET / SPILL: the Go executor's measured budget
//     and spill decisions, compared on rows, error class,
//     scanned rows, measured memory peak, AND spilled flag.
//     Budgets are chosen from the Go budget/spill tests' own
//     arithmetic (see budget_spill_test.go), not invented here.
//
// NOTE on aggregation spill: Go's spilled aggregation duplicates
// and over-counts groups that repeat after a spill boundary (a
// real bug in query/executor.go, verified by a repro run during
// this stage: 200 distinct groups + 10 repeats after the spill
// yielded 210 rows with the repeated groups counted twice, the
// second copy over-counted). The ISA engine deliberately merges
// each group exactly once. Aggregation-spill cases below therefore
// use DISTINCT groups only, where Go and ISA agree exactly; the
// repeat-across-spill case is reported to the parent as a Go
// bug, not replicated and not tested as parity.

func loadISAHostFixture(t *testing.T, host *ISAHost, users, orders []isaKV) {
	t.Helper()
	for _, e := range users {
		if e.del {
			if err := host.Delete(e.key); err != nil {
				t.Fatalf("delete %s: %v", e.key, err)
			}
		} else if err := host.Put(e.key, []byte(e.val)); err != nil {
			t.Fatalf("put %s: %v", e.key, err)
		}
		if e.flush {
			if err := host.Flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}
		}
	}
	for _, e := range orders {
		if e.del {
			if err := host.Delete(e.key); err != nil {
				t.Fatalf("delete %s: %v", e.key, err)
			}
		} else if err := host.Put(e.key, []byte(e.val)); err != nil {
			t.Fatalf("put %s: %v", e.key, err)
		}
		if e.flush {
			if err := host.Flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}
		}
	}
}

// runUnifiedDiff runs one SQL query through the Go reference
// (matSource, same visible data) and the ISA engine, and requires
// agreement on rows, error class, scanned rows, measured memory
// peak, and spilled flag. On error cases rows must be nil on both
// sides, but trace figures are still compared (both engines report
// peak/spilled so far on failure).
func runUnifiedDiff(t *testing.T, bin, sql string, contract Contract, users, orders []isaKV) {
	t.Helper()
	runUnifiedDiffTrace(t, bin, sql, contract, users, orders, true)
}

// runUnifiedDiffTrace is runUnifiedDiff with a trace-comparison
// switch. compareTrace=false is used ONLY for the left-mistyped
// JOIN case: the differential harness's matSource materializes
// eagerly at Open, so a left materialization error aborts before
// the right build (peak 0), while the ISA engine — like the
// production lazy SegDBSource, which materializes per batch
// during probing — builds the right side first (peak = build
// size). The error class and the no-rows result agree in both;
// only the on-error peak differs, and only because of the
// harness source's eagerness, not the engines' semantics.
func runUnifiedDiffTrace(t *testing.T, bin, sql string, contract Contract, users, orders []isaKV, compareTrace bool) {
	t.Helper()
	wasmBytes, err := os.ReadFile(bin)
	if err != nil {
		t.Skipf("wasm binary %s absent: %v", bin, err)
	}
	host, err := NewISAHost(wasmBytes)
	if err != nil {
		t.Fatalf("NewISAHost: %v", err)
	}
	defer host.Close()
	loadISAHostFixture(t, host, users, orders)
	plan, err := Compile(sql, testSchema())
	if err != nil {
		t.Fatalf("compile %q: %v", sql, err)
	}
	schema := testSchema()
	sources := map[string]Source{
		"users":  &matSource{b: bindingFor(t, schema, "users"), entries: visible(users)},
		"orders": &matSource{b: bindingFor(t, schema, "orders"), entries: visible(orders)},
	}
	refRows, refTrace, refErr := Execute(context.Background(), plan, contract, sources)
	prog, err := CompileISA(plan, contract)
	if err != nil {
		t.Fatalf("CompileISA %q: %v", sql, err)
	}
	isaRows, isaTrace, isaErr := host.ExecuteISATrace(prog, plan)
	if ISAErrorClass(refErr) != ISAErrorClass(isaErr) {
		t.Fatalf("error class differs: reference=%v (%s) isa=%v (%s)",
			refErr, ISAErrorClass(refErr), isaErr, ISAErrorClass(isaErr))
	}
	if refTrace == nil {
		t.Fatalf("reference trace is nil")
	}
	if compareTrace {
		if refTrace.ActualMemoryBytes != isaTrace.ActualMemoryBytes {
			t.Fatalf("memory peak differs: reference=%d isa=%d (refErr=%v isaErr=%v)",
				refTrace.ActualMemoryBytes, isaTrace.ActualMemoryBytes, refErr, isaErr)
		}
		if refTrace.Spilled != isaTrace.Spilled {
			t.Fatalf("spilled differs: reference=%v isa=%v (refErr=%v isaErr=%v)",
				refTrace.Spilled, isaTrace.Spilled, refErr, isaErr)
		}
		if refTrace.ScannedRows != isaTrace.ScannedRows {
			t.Fatalf("scanned differs: reference=%d isa=%d (refErr=%v isaErr=%v)",
				refTrace.ScannedRows, isaTrace.ScannedRows, refErr, isaErr)
		}
	}
	if refErr != nil {
		if refRows != nil || isaRows != nil {
			t.Fatalf("error case returned rows: ref=%d isa=%d", len(refRows), len(isaRows))
		}
		return
	}
	if !rowsEqual(t, refRows, isaRows) {
		t.Fatalf("rows differ:\nref=%v\nisa=%v", rowStrings(t, refRows), rowStrings(t, isaRows))
	}
}

func manyUsersKV(n int) []isaKV {
	var out []isaKV
	for i := 0; i < n; i++ {
		out = append(out, isaKV{
			key: fmt.Sprintf("user:%d", i),
			val: fmt.Sprintf(`{"name":"User%d","age":%d,"city":"City%d","active":true,"score":%d.5}`,
				i, 20+(i%50), i%10, i%10),
		})
	}
	return out
}

func manyOrdersKV(n int) []isaKV {
	var out []isaKV
	for i := 0; i < n; i++ {
		out = append(out, isaKV{
			key: fmt.Sprintf("order:%d", i),
			val: fmt.Sprintf(`{"user_id":"user:%d","total":%d.0,"item":"item%d"}`, i, i, i),
		})
	}
	return out
}

func TestISAUnifiedMemoryPressure(t *testing.T) {
	cases := []struct {
		name     string
		sql      string
		contract Contract
		users    []isaKV
		orders   []isaKV
	}{
		// JOIN build spills (budget between one 64-row batch and
		// the whole 200-row build side); sort also spills here
		// (ORDER BY total over projected rows).
		{"join-spill", "SELECT users.name, orders.total FROM users JOIN orders ON users.key = orders.user_id ORDER BY total",
			Contract{MaxMemoryBytes: 10000}, manyUsersKV(200), manyOrdersKV(200)},
		// JOIN build spills, no sort (streaming probe side).
		{"join-spill-stream", "SELECT users.name, orders.total FROM users JOIN orders ON users.key = orders.user_id",
			Contract{MaxMemoryBytes: 10000}, manyUsersKV(200), manyOrdersKV(200)},
		// SORT spills (500 projected rows, budget 8000).
		{"sort-spill", "SELECT name, age FROM users ORDER BY age",
			Contract{MaxMemoryBytes: 8000}, manyUsersKV(500), nil},
		// SORT spills, descending + NULLs-last path.
		{"sort-spill-desc", "SELECT name, city FROM users ORDER BY city DESC",
			Contract{MaxMemoryBytes: 8000}, manyUsersKV(500), nil},
		// AGGREGATE spills with DISTINCT groups (500 groups).
		{"agg-spill-distinct", "SELECT name, COUNT(*), AVG(age) FROM users GROUP BY name ORDER BY name",
			Contract{MaxMemoryBytes: 12000}, manyUsersKV(500), nil},
		// AGGREGATE spills, no sort (group order = first appearance).
		{"agg-spill-nosort", "SELECT name, COUNT(*) FROM users GROUP BY name",
			Contract{MaxMemoryBytes: 12000}, manyUsersKV(500), nil},
		// AGGREGATE spills with a JOIN input (200 distinct
		// names as groups, so group state itself spills).
		{"agg-spill-join", "SELECT users.name, COUNT(*) FROM users JOIN orders ON users.key = orders.user_id GROUP BY users.name ORDER BY name",
			Contract{MaxMemoryBytes: 14000}, manyUsersKV(200), manyOrdersKV(200)},
		// Same queries at a large budget: no spill, same rows,
		// peak = full retained size on both sides.
		{"join-nospill", "SELECT users.name, orders.total FROM users JOIN orders ON users.key = orders.user_id ORDER BY total",
			Contract{MaxMemoryBytes: 1000000}, manyUsersKV(200), manyOrdersKV(200)},
		{"sort-nospill", "SELECT name, age FROM users ORDER BY age",
			Contract{MaxMemoryBytes: 1000000}, manyUsersKV(500), nil},
		{"agg-nospill", "SELECT name, COUNT(*), AVG(age) FROM users GROUP BY name ORDER BY name",
			Contract{MaxMemoryBytes: 1000000}, manyUsersKV(500), nil},
		// Single batch larger than the budget: clean failure.
		{"join-batch-too-large", "SELECT users.name, orders.total FROM users JOIN orders ON users.key = orders.user_id",
			Contract{MaxMemoryBytes: 10}, smallUsers(), smallOrders()},
		{"sort-batch-too-large", "SELECT name, age FROM users ORDER BY age",
			Contract{MaxMemoryBytes: 10}, smallUsers(), nil},
		{"agg-batch-too-large", "SELECT name, COUNT(*) FROM users GROUP BY name",
			Contract{MaxMemoryBytes: 10}, smallUsers(), nil},
		// Budget exactly at a batch boundary is covered by the
		// spill cases above (10000/8000/12000 are all between one
		// batch and the total); unlimited budget is the control.
		{"sort-unlimited", "SELECT name, age FROM users ORDER BY age",
			Contract{}, manyUsersKV(500), nil},
	}
	for _, bin := range []string{"../build/yowza.wasm", "../build/yowza64.wasm"} {
		t.Run(bin, func(t *testing.T) {
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					runUnifiedDiff(t, bin, tc.sql, tc.contract, tc.users, tc.orders)
				})
			}
		})
	}
}

func TestISAUnifiedVariedLimits(t *testing.T) {
	cases := []struct {
		name     string
		sql      string
		contract Contract
		users    []isaKV
		orders   []isaKV
	}{
		// LIMIT does not rescue a spilling sort: sort consumes
		// all input before LIMIT, so the spill still happens.
		{"sort-spill-limit", "SELECT name, age FROM users ORDER BY age LIMIT 10",
			Contract{MaxMemoryBytes: 8000}, manyUsersKV(500), nil},
		// LIMIT over a spilling join build.
		{"join-spill-limit", "SELECT users.name, orders.total FROM users JOIN orders ON users.key = orders.user_id LIMIT 5",
			Contract{MaxMemoryBytes: 10000}, manyUsersKV(200), manyOrdersKV(200)},
		// Scanned budget aborts before any memory accounting.
		{"scanned-plus-memory", "SELECT name, age FROM users ORDER BY age",
			Contract{MaxMemoryBytes: 8000, MaxScannedRows: 10}, manyUsersKV(500), nil},
		// Result budget aborts after a spilling join build.
		{"result-plus-memory", "SELECT users.name, orders.total FROM users JOIN orders ON users.key = orders.user_id",
			Contract{MaxMemoryBytes: 10000, MaxResultRows: 2}, manyUsersKV(200), manyOrdersKV(200)},
		// Scanned + result budgets together, no memory budget.
		{"scanned-plus-result", "SELECT name FROM users",
			Contract{MaxScannedRows: 100, MaxResultRows: 2}, manyUsersKV(500), nil},
		// LIMIT inside all budgets succeeds.
		{"all-budgets-ok", "SELECT name FROM users LIMIT 2",
			Contract{MaxMemoryBytes: 100000, MaxScannedRows: 100, MaxResultRows: 5}, smallUsers(), nil},
		// LIMIT 0 over a join still builds (and counts) the right
		// side, and still runs the memory accounting for it.
		{"limit-join-0-memory", "SELECT users.name FROM users JOIN orders ON users.key = orders.user_id LIMIT 0",
			Contract{MaxMemoryBytes: 10000}, smallUsers(), smallOrders()},
	}
	for _, bin := range []string{"../build/yowza.wasm", "../build/yowza64.wasm"} {
		t.Run(bin, func(t *testing.T) {
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					runUnifiedDiff(t, bin, tc.sql, tc.contract, tc.users, tc.orders)
				})
			}
		})
	}
	// Static verifier refusal: a LIMIT plan's estimated rows
	// (the LIMIT) times the 256-byte heuristic exceeds a small
	// memory budget, so CompileISA refuses before execution.
	plan, err := Compile("SELECT name FROM users LIMIT 2", testSchema())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if _, err := CompileISA(plan, Contract{MaxMemoryBytes: 100}); err == nil {
		t.Fatalf("CompileISA accepted LIMIT 2 over MaxMemoryBytes 100 (estimate 512)")
	} else if ve, ok := err.(*VerificationError); !ok || ve.Code != "BUDGET_EXCEEDED" {
		t.Fatalf("static memory refusal = %v, want BUDGET_EXCEEDED", err)
	}
}

func TestISAUnifiedConcurrentErrors(t *testing.T) {
	mistyped := []isaKV{
		{key: "user:1", val: `{"name":"Alice","age":30,"city":"Tehran","active":true,"score":9.5}`},
		{key: "user:2", val: `{"name":"Zed","age":"thirty","city":"X","active":true,"score":1.0}`},
	}
	cases := []struct {
		name     string
		sql      string
		contract Contract
		users    []isaKV
		orders   []isaKV
	}{
		// Mistyped value + tight scanned budget: type error wins
		// in both engines (materialization precedes scan counting).
		{"mistyped-plus-scanned", "SELECT name, age FROM users",
			Contract{MaxScannedRows: 3}, mistyped, nil},
		// Mistyped value + memory budget: type error wins.
		{"mistyped-plus-memory", "SELECT name, age FROM users",
			Contract{MaxMemoryBytes: 10000}, mistyped, nil},
		// Mistyped value + result budget: type error wins.
		{"mistyped-plus-result", "SELECT name, age FROM users",
			Contract{MaxResultRows: 1}, mistyped, nil},
		// Mistyped value on the JOIN build side + memory budget.
		// (Left-mistyped JOIN: trace comparison off, see
		// runUnifiedDiffTrace — error class and rows still agree.)
		{"mistyped-join-plus-memory", "SELECT users.name, orders.total FROM users JOIN orders ON users.key = orders.user_id",
			Contract{MaxMemoryBytes: 10000}, mistyped, smallOrders()},
		// Both scanned and result budgets exceeded: budget class.
		{"scanned-and-result", "SELECT name FROM users",
			Contract{MaxScannedRows: 3, MaxResultRows: 1}, smallUsers(), nil},
		// Scanned budget exceeded on the JOIN build side.
		{"scanned-join-build", "SELECT users.name FROM users JOIN orders ON users.key = orders.user_id",
			Contract{MaxScannedRows: 3}, smallUsers(), smallOrders()},
	}
	for _, bin := range []string{"../build/yowza.wasm", "../build/yowza64.wasm"} {
		t.Run(bin, func(t *testing.T) {
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					if tc.name == "mistyped-join-plus-memory" {
						runUnifiedDiffTrace(t, bin, tc.sql, tc.contract, tc.users, tc.orders, false)
					} else {
						runUnifiedDiff(t, bin, tc.sql, tc.contract, tc.users, tc.orders)
					}
				})
			}
		})
	}
}

func TestISAUnifiedSharedSnapshot(t *testing.T) {
	for _, bin := range []string{"../build/yowza.wasm", "../build/yowza64.wasm"} {
		t.Run(bin, func(t *testing.T) {
			wasmBytes, err := os.ReadFile(bin)
			if err != nil {
				t.Skipf("wasm binary %s absent: %v", bin, err)
			}
			host, err := NewISAHost(wasmBytes)
			if err != nil {
				t.Fatalf("NewISAHost: %v", err)
			}
			defer host.Close()
			loadISAHostFixture(t, host, smallUsers(), smallOrders())
			// Flush first: snapshot semantics for an UPDATED key
			// are exact only once the old value is in a segment.
			// The hot ART keeps only the newest version per key,
			// so an old snapshot cannot resurrect a pre-update
			// hot value — a shared SegDB limitation (the ISA
			// engine uses the same cursor scan as Go's ScanAt),
			// not an ISA divergence. Flushing here puts the
			// pre-snapshot values in segments, where the old
			// snapshot does see them.
			if err := host.Flush(); err != nil {
				t.Fatalf("initial flush: %v", err)
			}
			snap, err := host.Snapshot()
			if err != nil {
				t.Fatalf("Snapshot: %v", err)
			}
			if snap == 0 {
				t.Fatalf("snapshot of a non-empty DB is 0 (literal 0 yields no rows)")
			}
			// Writes committed AFTER the snapshot: a new user,
			// a new order matching it, and an update to Alice.
			if err := host.Put("user:7", []byte(`{"name":"Gina","age":28,"city":"Tehran","active":true,"score":8.5}`)); err != nil {
				t.Fatalf("put user:7: %v", err)
			}
			if err := host.Put("order:6", []byte(`{"user_id":"user:7","total":77.0,"item":"new"}`)); err != nil {
				t.Fatalf("put order:6: %v", err)
			}
			if err := host.Put("user:1", []byte(`{"name":"Alice","age":31,"city":"Tehran","active":true,"score":9.5}`)); err != nil {
				t.Fatalf("update user:1: %v", err)
			}
			scanPlan, err := Compile("SELECT name, age FROM users", testSchema())
			if err != nil {
				t.Fatalf("compile scan: %v", err)
			}
			scanProg, err := CompileISA(scanPlan, Contract{})
			if err != nil {
				t.Fatalf("CompileISA scan: %v", err)
			}
			// At the old snapshot: Gina invisible, Alice still 30.
			rows, _, err := host.ExecuteISAAt(scanProg, scanPlan, snap)
			if err != nil {
				t.Fatalf("scan at snapshot: %v", err)
			}
			if len(rows) != 6 {
				t.Fatalf("scan at snapshot rows = %d, want 6 (no Gina)", len(rows))
			}
			for _, r := range rows {
				nm, _ := r.Get("name")
				if nm.S == "Gina" {
					t.Fatalf("scan at snapshot leaked post-snapshot user Gina")
				}
				if nm.S == "Alice" {
					ag, _ := r.Get("age")
					if ag.I != 30 {
						t.Fatalf("scan at snapshot Alice age = %d, want 30 (old value)", ag.I)
					}
				}
			}
			// The JOIN at the same snapshot must not see the new
			// matching pair either: this is the write-between-
			// the-two-table-reads case — the second table
			// (orders) is read at the same sequence as the first.
			joinPlan, err := Compile("SELECT users.name, orders.total FROM users JOIN orders ON users.key = orders.user_id ORDER BY total", testSchema())
			if err != nil {
				t.Fatalf("compile join: %v", err)
			}
			joinProg, err := CompileISA(joinPlan, Contract{})
			if err != nil {
				t.Fatalf("CompileISA join: %v", err)
			}
			jrows, _, err := host.ExecuteISAAt(joinProg, joinPlan, snap)
			if err != nil {
				t.Fatalf("join at snapshot: %v", err)
			}
			for _, r := range jrows {
				nm, _ := r.Get("name")
				if nm.S == "Gina" {
					t.Fatalf("join at snapshot leaked post-snapshot pair Gina/77")
				}
			}
			if len(jrows) != 3 {
				t.Fatalf("join at snapshot rows = %d, want 3", len(jrows))
			}
			// A fresh execution (fresh snapshot) sees everything.
			frows, _, err := host.ExecuteISATrace(scanProg, scanPlan)
			if err != nil {
				t.Fatalf("fresh scan: %v", err)
			}
			if len(frows) != 7 {
				t.Fatalf("fresh scan rows = %d, want 7 (with Gina)", len(frows))
			}
			found := false
			for _, r := range frows {
				nm, _ := r.Get("name")
				if nm.S == "Alice" {
					ag, _ := r.Get("age")
					if ag.I != 31 {
						t.Fatalf("fresh scan Alice age = %d, want 31", ag.I)
					}
					found = true
				}
			}
			if !found {
				t.Fatalf("fresh scan lost Alice")
			}
			fjrows, _, err := host.ExecuteISATrace(joinProg, joinPlan)
			if err != nil {
				t.Fatalf("fresh join: %v", err)
			}
			if len(fjrows) != 4 {
				t.Fatalf("fresh join rows = %d, want 4 (with Gina/77)", len(fjrows))
			}
			// Snapshot across a flush: snapshot, flush, then
			// execute at the old snapshot — same rows (segment
			// entries keep their effective sequences).
			snap2, err := host.Snapshot()
			if err != nil {
				t.Fatalf("Snapshot 2: %v", err)
			}
			if err := host.Flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}
			rows2, _, err := host.ExecuteISAAt(scanProg, scanPlan, snap2)
			if err != nil {
				t.Fatalf("scan at pre-flush snapshot: %v", err)
			}
			if len(rows2) != 7 {
				t.Fatalf("scan at pre-flush snapshot rows = %d, want 7", len(rows2))
			}
		})
	}
}

func TestISAUnifiedLargeStreamingScan(t *testing.T) {
	// 9000 rows through the ISA streaming path (cursor pages,
	// one page at a time, no table materialization). Fixture
	// loads flush every 400 puts (the 64 KiB wasm WAL ceiling
	// from the streaming tests) so the DB holds ~23 segments,
	// under the 128-segment cap.
	const n = 9000
	var users []isaKV
	for i := 0; i < n; i++ {
		users = append(users, isaKV{
			key:   fmt.Sprintf("user:%05d", i),
			val:   fmt.Sprintf(`{"name":"U%05d","age":%d,"city":"C","active":true,"score":1.0}`, i, 20+(i%50)),
			flush: (i+1)%400 == 0,
		})
	}
	for _, bin := range []string{"../build/yowza.wasm", "../build/yowza64.wasm"} {
		t.Run(bin, func(t *testing.T) {
			runUnifiedDiff(t, bin, "SELECT name FROM users", Contract{}, users, nil)
		})
	}
}
