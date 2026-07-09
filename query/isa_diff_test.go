package query

import (
	"context"
	"fmt"
	"math"
	"os"
	"sort"
	"testing"
)

// Differential tests: the Go batch executor (query/executor.go) is
// the reference for semantics. For every case below, the same
// verified plan runs through BOTH the reference executor (over a
// materializing source built from the same fixture KV) and the C
// ISA engine (over a SegDB holding the same fixture KV), and the
// results must agree row-for-row, error-class-for-error-class, and
// on trace-relevant counts (actual rows, scanned rows). Any
// divergence is a bug, not a documented difference.
//
// Row order: the reference source delivers rows sorted by key, the
// same order segdb_scan returns, so unordered queries are compared
// in scan order on both sides. ORDER BY queries are compared in
// their sorted order.
//
// Scanned rows: the reference pulls scans in batches of 64 (the
// default batch size); the C engine simulates exactly that batch
// accounting (see query_isa.h, SCANNED COUNT). These tests run the
// reference with its default batch size, so the counts agree.

type isaKV struct {
	key   string
	val   string
	del   bool
	flush bool // flush the SegDB after applying this entry
}

// matSource materializes rows at Open (like SegDBSource does), so a
// mistyped JSON value fails at Open in the reference exactly as it
// fails at materialization in the C engine.
type matSource struct {
	b       Binding
	entries []isaKV
}

func (s *matSource) Estimate() int64 { return -1 }

func (s *matSource) Open(ctx context.Context, b Binding) (RowIterator, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	rows := make([]Row, 0, len(s.entries))
	for _, e := range s.entries {
		r, err := MaterializeRow(b, e.key, []byte(e.val))
		if err != nil {
			return nil, err
		}
		rows = append(rows, r)
	}
	return &sliceIterator{rows: rows}, nil
}

// visible applies puts/deletes in order and returns the surviving
// entries sorted by key (segdb_scan order).
func visible(entries []isaKV) []isaKV {
	m := map[string]string{}
	for _, e := range entries {
		if e.del {
			delete(m, e.key)
		} else {
			m[e.key] = e.val
		}
	}
	var out []isaKV
	for k, v := range m {
		out = append(out, isaKV{key: k, val: v})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].key < out[j].key })
	return out
}

func valuesEqual(a, b Value) bool {
	if a.Kind != b.Kind {
		return false
	}
	switch a.Kind {
	case KindNull:
		return true
	case KindInt:
		return a.I == b.I
	case KindFloat:
		return math.Float64bits(a.F) == math.Float64bits(b.F)
	case KindString:
		return a.S == b.S
	case KindBool:
		return a.BoolVal() == b.BoolVal()
	case KindBytes:
		if len(a.B) != len(b.B) {
			return false
		}
		for i := range a.B {
			if a.B[i] != b.B[i] {
				return false
			}
		}
		return true
	}
	return false
}

func rowsEqual(t *testing.T, a, b []Row) bool {
	t.Helper()
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if len(a[i].Cols) != len(b[i].Cols) || len(a[i].Vals) != len(b[i].Vals) {
			return false
		}
		for j := range a[i].Cols {
			if a[i].Cols[j] != b[i].Cols[j] {
				return false
			}
			if !valuesEqual(a[i].Vals[j], b[i].Vals[j]) {
				return false
			}
		}
	}
	return true
}

type isaDiffCase struct {
	name     string
	sql      string
	contract Contract
	users    []isaKV
	orders   []isaKV
	flush    bool
}

func smallUsers() []isaKV {
	return []isaKV{
		{key: "user:1", val: `{"name":"Alice","age":30,"city":"Tehran","active":true,"score":9.5}`, del: false},
		{key: "user:2", val: `{"name":"Bob","age":25,"city":"Shiraz","active":false,"score":7.0}`, del: false},
		{key: "user:3", val: `{"name":"Carol","age":40,"city":"Tehran","active":true,"score":8.0}`, del: false},
		{key: "user:4", val: `{"name":"Dan","age":22,"active":true,"score":6.5}`, del: false}, // city missing => NULL
		{key: "user:5", val: `{"name":"Eve","age":35,"city":"Tabriz","active":false,"score":9.0}`, del: false},
		{key: "user:6", val: `{"name":"Frank","city":"Tehran","active":true,"score":5.5}`, del: false}, // age missing => NULL
	}
}

func smallOrders() []isaKV {
	return []isaKV{
		{key: "order:1", val: `{"user_id":"user:1","total":120.0,"item":"book"}`, del: false},
		{key: "order:2", val: `{"user_id":"user:1","total":45.5,"item":"pen"}`, del: false},
		{key: "order:3", val: `{"user_id":"user:3","total":300.0,"item":"laptop"}`, del: false},
		{key: "order:4", val: `{"total":10.0,"item":"orphan"}`, del: false},                    // user_id missing => NULL key, never matches
		{key: "order:5", val: `{"user_id":"user:99","total":50.0,"item":"ghost"}`, del: false}, // unknown user, never matches
	}
}

func bigUsers() []isaKV {
	var out []isaKV
	for i := 1; i <= 150; i++ {
		age := 10
		city := "A"
		if i > 64 {
			age = 50 + (i - 65)
			city = "B"
		}
		out = append(out, isaKV{
			key: fmt.Sprintf("user:%03d", i),
			val: fmt.Sprintf(`{"name":"U%03d","age":%d,"city":%q,"active":true,"score":1.0}`, i, age, city),
		})
	}
	return out
}

func isaDiffCases() []isaDiffCase {
	small := smallUsers()
	orders := smallOrders()
	empty := []isaKV{}
	big := bigUsers()
	mistyped := []isaKV{
		{key: "user:1", val: `{"name":"Alice","age":30,"city":"Tehran","active":true,"score":9.5}`, del: false},
		{key: "user:2", val: `{"name":"Zed","age":"thirty","city":"X","active":true,"score":1.0}`, del: false},
	}
	flushed := []isaKV{
		{"user:1", `{"name":"Alice","age":30,"city":"Tehran","active":true,"score":9.5}`, false, false},
		{"user:2", `{"name":"Bob","age":25,"city":"Shiraz","active":false,"score":7.0}`, false, false},
		{"user:3", `{"name":"Carol","age":40,"city":"Tehran","active":true,"score":8.0}`, false, true}, // flush segment 1
		{"user:2", ``, true, false}, // delete Bob, then flush segment 2 below
		{"user:4", `{"name":"Dan","age":22,"active":true,"score":6.5}`, false, false},
	}
	return []isaDiffCase{
		// SCAN + PROJECT.
		{"scan-project", "SELECT name, age FROM users", Contract{}, small, empty, false},
		{"scan-star", "SELECT * FROM users", Contract{}, small, empty, false},
		// FILTER: comparisons, AND/OR/NOT, three-valued NULL logic.
		{"filter-and", "SELECT name FROM users WHERE age >= 30 AND city = 'Tehran'", Contract{}, small, empty, false},
		{"filter-not-3vl", "SELECT name FROM users WHERE NOT (city = 'Tehran' OR city = 'Shiraz')", Contract{}, small, empty, false},
		{"filter-in", "SELECT name FROM users WHERE city IN ('Tehran', 'Tabriz')", Contract{}, small, empty, false},
		{"filter-in-null", "SELECT name FROM users WHERE city IN ('Tehran', NULL)", Contract{}, small, empty, false},
		{"filter-like", "SELECT name FROM users WHERE name LIKE 'A%'", Contract{}, small, empty, false},
		{"filter-like-underscore", "SELECT name FROM users WHERE name LIKE '___'", Contract{}, small, empty, false},
		{"filter-not-like", "SELECT name FROM users WHERE name NOT LIKE 'A%'", Contract{}, small, empty, false},
		{"filter-isnull", "SELECT name FROM users WHERE city IS NULL", Contract{}, small, empty, false},
		{"filter-isnotnull", "SELECT name FROM users WHERE city IS NOT NULL", Contract{}, small, empty, false},
		{"filter-null-cmp", "SELECT name FROM users WHERE age = NULL", Contract{}, small, empty, false},
		{"filter-null-ne", "SELECT name FROM users WHERE city != NULL", Contract{}, small, empty, false},
		{"filter-bool-float", "SELECT name FROM users WHERE active = TRUE AND score > 8.0", Contract{}, small, empty, false},
		// JOIN (inner equijoin, NULL keys never match).
		{"join-filter-sort", "SELECT users.name, orders.total FROM users JOIN orders ON users.key = orders.user_id WHERE orders.total > 100 ORDER BY total DESC", Contract{}, small, orders, false},
		{"join-sort", "SELECT users.name, orders.item FROM users JOIN orders ON users.key = orders.user_id ORDER BY item", Contract{}, small, orders, false},
		{"join-filter-both", "SELECT users.name, orders.total FROM users JOIN orders ON users.key = orders.user_id WHERE users.city = 'Tehran' AND orders.total > 100 ORDER BY total", Contract{}, small, orders, false},
		// AGGREGATE.
		{"group-city", "SELECT city, COUNT(*), AVG(age) FROM users GROUP BY city ORDER BY city", Contract{}, small, empty, false},
		{"group-aggs", "SELECT city, COUNT(*), COUNT(age), SUM(age), MIN(age), MAX(age) FROM users GROUP BY city ORDER BY city", Contract{}, small, empty, false},
		{"group-only", "SELECT city FROM users GROUP BY city ORDER BY city", Contract{}, small, empty, false},
		{"group-empty", "SELECT city, COUNT(*) FROM users WHERE age > 1000 GROUP BY city", Contract{}, small, empty, false},
		{"ungrouped-empty", "SELECT COUNT(*), SUM(age), AVG(age), MIN(age), MAX(age) FROM users WHERE age > 1000", Contract{}, small, empty, false},
		{"ungrouped", "SELECT COUNT(*), COUNT(city), SUM(age), AVG(age) FROM users", Contract{}, small, empty, false},
		{"agg-float-str", "SELECT SUM(score), MIN(name), MAX(name) FROM users", Contract{}, small, empty, false},
		{"join-group", "SELECT users.city, COUNT(*) FROM users JOIN orders ON users.key = orders.user_id GROUP BY users.city ORDER BY city", Contract{}, small, orders, false},
		// SORT (NULLs last in both directions, stable ties).
		{"sort-asc-nulls-last", "SELECT name, city FROM users ORDER BY city", Contract{}, small, empty, false},
		{"sort-desc-nulls-last", "SELECT name, city FROM users ORDER BY city DESC", Contract{}, small, empty, false},
		{"sort-age-desc", "SELECT name, age FROM users ORDER BY age DESC", Contract{}, small, empty, false},
		// LIMIT edges.
		{"limit-0", "SELECT name FROM users LIMIT 0", Contract{}, small, empty, false},
		{"limit-1", "SELECT name FROM users LIMIT 1", Contract{}, small, empty, false},
		{"limit-exact", "SELECT name FROM users LIMIT 6", Contract{}, small, empty, false},
		{"limit-over", "SELECT name FROM users LIMIT 100", Contract{}, small, empty, false},
		{"limit-sort", "SELECT name, age FROM users ORDER BY age DESC LIMIT 2", Contract{}, small, empty, false},
		{"limit-join-0", "SELECT users.name FROM users JOIN orders ON users.key = orders.user_id LIMIT 0", Contract{}, small, orders, false},
		// Batch-64 partial-pull accounting (150 rows).
		{"big-limit-partial", "SELECT name FROM users WHERE age >= 50 LIMIT 2", Contract{}, big, empty, false},
		{"big-count", "SELECT COUNT(*) FROM users", Contract{}, big, empty, false},
		{"big-filter-limit", "SELECT name FROM users WHERE age >= 50 LIMIT 200", Contract{}, big, empty, false},
		// Empty input.
		{"empty-plain", "SELECT name FROM users", Contract{}, empty, empty, false},
		{"empty-ungrouped", "SELECT COUNT(*), SUM(age) FROM users", Contract{}, empty, empty, false},
		// Budgets (runtime aborts: estimate unknown, so Verify defers).
		{"budget-scanned", "SELECT name FROM users", Contract{MaxScannedRows: 3}, small, empty, false},
		{"budget-result", "SELECT name FROM users", Contract{MaxResultRows: 2}, small, empty, false},
		{"budget-limit-ok", "SELECT name FROM users LIMIT 2", Contract{MaxResultRows: 5, MaxScannedRows: 100}, small, empty, false},
		// Type error (mistyped JSON value fails in both engines).
		{"mistyped", "SELECT name, age FROM users", Contract{}, mistyped, empty, false},
		// Layered SegDB path: flushed segments + flushed tombstone.
		{"flushed-tombstone", "SELECT name FROM users", Contract{}, flushed, empty, true},
	}
}

func TestISADifferential(t *testing.T) {
	cases := isaDiffCases()
	for _, bin := range []string{"../build/yowza.wasm", "../build/yowza64.wasm"} {
		t.Run(bin, func(t *testing.T) {
			wasmBytes, err := os.ReadFile(bin)
			if err != nil {
				t.Skipf("wasm binary %s absent: %v", bin, err)
			}
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					host, err := NewISAHost(wasmBytes)
					if err != nil {
						t.Fatalf("NewISAHost: %v", err)
					}
					defer host.Close()
					// Load the fixture into the SegDB, in order
					// (so deletes and flushes have their effect).
					for _, e := range tc.users {
						if e.del {
							if err := host.Delete(e.key); err != nil {
								t.Fatalf("delete %s: %v", e.key, err)
							}
						} else {
							if err := host.Put(e.key, []byte(e.val)); err != nil {
								t.Fatalf("put %s: %v", e.key, err)
							}
						}
						if e.flush {
							if err := host.Flush(); err != nil {
								t.Fatalf("flush: %v", err)
							}
						}
					}
					for _, e := range tc.orders {
						if e.del {
							if err := host.Delete(e.key); err != nil {
								t.Fatalf("delete %s: %v", e.key, err)
							}
						} else {
							if err := host.Put(e.key, []byte(e.val)); err != nil {
								t.Fatalf("put %s: %v", e.key, err)
							}
						}
					}
					if tc.flush {
						if err := host.Flush(); err != nil {
							t.Fatalf("flush: %v", err)
						}
					}
					// Reference: same verified plan, same visible
					// data, materialized at Open.
					plan, err := Compile(tc.sql, testSchema())
					if err != nil {
						t.Fatalf("compile %q: %v", tc.sql, err)
					}
					schema := testSchema()
					uBinding := bindingFor(t, schema, "users")
					oBinding := bindingFor(t, schema, "orders")
					sources := map[string]Source{
						"users":  &matSource{b: uBinding, entries: visible(tc.users)},
						"orders": &matSource{b: oBinding, entries: visible(tc.orders)},
					}
					refRows, refTrace, refErr := Execute(context.Background(), plan, tc.contract, sources)
					// ISA: same verified plan, compiled.
					prog, err := CompileISA(plan, tc.contract)
					if err != nil {
						t.Fatalf("CompileISA %q: %v", tc.sql, err)
					}
					isaRows, isaTrace, isaErr := host.ExecuteISATrace(prog, plan)
					isaScanned := isaTrace.ScannedRows
					// Error classes must agree, and error cases
					// must yield no rows on either side. Memory
					// trace figures must also agree on error
					// cases (peak so far, spilled so far), since
					// both engines report them the same way.
					if ISAErrorClass(refErr) != ISAErrorClass(isaErr) {
						t.Fatalf("error class differs: reference=%v (%s) isa=%v (%s)",
							refErr, ISAErrorClass(refErr), isaErr, ISAErrorClass(isaErr))
					}
					if refTrace == nil {
						t.Fatalf("reference trace is nil")
					}
					if refTrace.ActualMemoryBytes != isaTrace.ActualMemoryBytes {
						t.Fatalf("memory peak differs: reference=%d isa=%d (refErr=%v isaErr=%v)",
							refTrace.ActualMemoryBytes, isaTrace.ActualMemoryBytes, refErr, isaErr)
					}
					if refTrace.Spilled != isaTrace.Spilled {
						t.Fatalf("spilled differs: reference=%v isa=%v (refErr=%v isaErr=%v)",
							refTrace.Spilled, isaTrace.Spilled, refErr, isaErr)
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
					if refTrace.ActualRows != int64(len(refRows)) {
						t.Fatalf("reference actual rows %d != len(rows) %d", refTrace.ActualRows, len(refRows))
					}
					if refTrace.ScannedRows != isaScanned {
						t.Fatalf("scanned rows differ: reference=%d isa=%d", refTrace.ScannedRows, isaScanned)
					}
					if int64(len(isaRows)) != refTrace.ActualRows {
						t.Fatalf("isa actual rows %d != reference %d", len(isaRows), refTrace.ActualRows)
					}
				})
			}
		})
	}
}
