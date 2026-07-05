package query

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Helpers shared with trace_test.go (same package).

func makeManyUsers(t *testing.T, n int) *MemorySource {
	t.Helper()
	b := bindingFor(t, testSchema(), "users")
	var rows []Row
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("user:%d", i)
		val := fmt.Sprintf(`{"name":"User%d","age":%d,"city":"City%d","active":true,"score":%d.5}`,
			i, 20+(i%50), i%10, i%10)
		r, err := MaterializeRow(b, key, []byte(val))
		if err != nil {
			t.Fatalf("materialize %s: %v", key, err)
		}
		rows = append(rows, r)
	}
	return NewMemorySource(rows)
}

func makeManyOrders(t *testing.T, n int) *MemorySource {
	t.Helper()
	b := bindingFor(t, testSchema(), "orders")
	var rows []Row
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("order:%d", i)
		val := fmt.Sprintf(`{"user_id":"user:%d","total":%d.0,"item":"item%d"}`, i, i, i)
		r, err := MaterializeRow(b, key, []byte(val))
		if err != nil {
			t.Fatalf("materialize %s: %v", key, err)
		}
		rows = append(rows, r)
	}
	return NewMemorySource(rows)
}

func runWithContract(t *testing.T, sql string, contract Contract, sources map[string]Source) ([]Row, *Trace, error) {
	t.Helper()
	plan, err := Compile(sql, testSchema())
	if err != nil {
		t.Fatalf("compile %q: %v", sql, err)
	}
	return Execute(context.Background(), plan, contract, sources)
}

func spillTempFiles(t *testing.T) map[string]bool {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(os.TempDir(), "yowza-spill-*"))
	if err != nil {
		t.Fatalf("glob temp spill files: %v", err)
	}
	out := map[string]bool{}
	for _, m := range matches {
		out[m] = true
	}
	return out
}

func assertNoNewSpillFiles(t *testing.T, before map[string]bool) {
	t.Helper()
	after := spillTempFiles(t)
	for p := range after {
		if !before[p] {
			t.Fatalf("leaked spill temp file %q (present after, absent before)", p)
		}
	}
}

func TestJoinSpillsAndMatchesLargeBudget(t *testing.T) {
	sql := "SELECT users.name, orders.total FROM users JOIN orders ON users.key = orders.user_id ORDER BY total"
	users := makeManyUsers(t, 200)
	orders := makeManyOrders(t, 200)
	large, largeTrace, err := runWithContract(t, sql, Contract{}, map[string]Source{"users": users, "orders": orders})
	if err != nil {
		t.Fatalf("large-budget join: %v", err)
	}
	// Small budget: larger than one 64-row batch of build rows
	// (~105 bytes/row x 64 ~= 6720) but far smaller than the whole
	// 200-row build side (~21000), so the build side must spill.
	small, smallTrace, err := runWithContract(t, sql, Contract{MaxMemoryBytes: 10000}, map[string]Source{"users": users, "orders": orders})
	if err != nil {
		t.Fatalf("small-budget join: %v", err)
	}
	if len(small) != len(large) {
		t.Fatalf("join rows: small=%d large=%d", len(small), len(large))
	}
	for i := range large {
		if large[i].String() != small[i].String() {
			t.Fatalf("join row %d differs: small=%v large=%v", i, small[i], large[i])
		}
	}
	if !smallTrace.Spilled {
		t.Fatalf("small-budget join should have spilled, trace: %+v", smallTrace)
	}
	if smallTrace.ActualMemoryBytes <= 0 || smallTrace.ActualMemoryBytes > 10000 {
		t.Fatalf("small-budget join measured memory = %d, want (0, 10000]", smallTrace.ActualMemoryBytes)
	}
	if largeTrace.Spilled {
		t.Fatalf("unlimited-budget join should not spill, trace: %+v", largeTrace)
	}
}

func TestSortSpillsAndMatchesLargeBudget(t *testing.T) {
	sql := "SELECT name, age FROM users ORDER BY age"
	users := makeManyUsers(t, 500)
	large, _, err := runWithContract(t, sql, Contract{}, map[string]Source{"users": users})
	if err != nil {
		t.Fatalf("large-budget sort: %v", err)
	}
	// Projected rows are ~55 bytes; one 64-row batch is ~3520
	// bytes, the whole input ~27500, so 8000 forces chunk spills.
	small, smallTrace, err := runWithContract(t, sql, Contract{MaxMemoryBytes: 8000}, map[string]Source{"users": users})
	if err != nil {
		t.Fatalf("small-budget sort: %v", err)
	}
	if len(small) != len(large) {
		t.Fatalf("sort rows: small=%d large=%d", len(small), len(large))
	}
	for i := range large {
		if large[i].String() != small[i].String() {
			t.Fatalf("sort row %d differs (stable order broken?): small=%v large=%v", i, small[i], large[i])
		}
	}
	if !smallTrace.Spilled {
		t.Fatalf("small-budget sort should have spilled, trace: %+v", smallTrace)
	}
	if smallTrace.ActualMemoryBytes <= 0 || smallTrace.ActualMemoryBytes > 8000 {
		t.Fatalf("small-budget sort measured memory = %d, want (0, 8000]", smallTrace.ActualMemoryBytes)
	}
	// NULLs-last stability spot check on the small fixture: sort
	// the 5-row fixture (Dan has NULL city) by city.
	rows, _, err := runWithContract(t, "SELECT name, city FROM users ORDER BY city", Contract{MaxMemoryBytes: 2000}, map[string]Source{"users": memUsers(t)})
	if err != nil {
		t.Fatalf("small-fixture sort: %v", err)
	}
	if len(rows) != 5 {
		t.Fatalf("small-fixture sort rows = %d, want 5", len(rows))
	}
	last, _ := rows[4].Get("city")
	if last.Kind != KindNull {
		t.Fatalf("last row city should be NULL, got %v", rows[4])
	}
}

func TestGroupedAggregationSpillsAndMatchesLargeBudget(t *testing.T) {
	// Group by name: 500 distinct groups, far more than fit in the
	// small budget, so group state must spill and merge back.
	sql := "SELECT name, COUNT(*), AVG(age) FROM users GROUP BY name ORDER BY name"
	users := makeManyUsers(t, 500)
	large, _, err := runWithContract(t, sql, Contract{}, map[string]Source{"users": users})
	if err != nil {
		t.Fatalf("large-budget agg: %v", err)
	}
	// One 64-row input batch measures 8172 bytes (full 6-column
	// user rows), so the budget must exceed that; 12000 still
	// forces a flush once a second batch of distinct groups arrives
	// (~2 x 64 groups x ~103 bytes/group > 12000).
	small, smallTrace, err := runWithContract(t, sql, Contract{MaxMemoryBytes: 12000}, map[string]Source{"users": users})
	if err != nil {
		t.Fatalf("small-budget agg: %v", err)
	}
	if len(small) != len(large) {
		t.Fatalf("agg rows: small=%d large=%d", len(small), len(large))
	}
	for i := range large {
		if large[i].String() != small[i].String() {
			t.Fatalf("agg row %d differs: small=%v large=%v", i, small[i], large[i])
		}
	}
	if !smallTrace.Spilled {
		t.Fatalf("small-budget agg should have spilled, trace: %+v", smallTrace)
	}
}

func TestGroupedAggregationRepeatAfterSpillNotDuplicated(t *testing.T) {
	// Regression for the aggregate-spill bug found during item-02
	// parity work: a group that reappears after a spill reset was
	// appended to the merge order a second time, emitting it twice
	// and over-counting (200 groups + 10 repeats produced 210 rows).
	b := bindingFor(t, testSchema(), "users")
	var rows []Row
	for i := 0; i < 200; i++ {
		val := fmt.Sprintf(`{"name":"User%d","age":10,"city":"City0","active":true,"score":1.5}`, i)
		r, err := MaterializeRow(b, fmt.Sprintf("user:%d", i), []byte(val))
		if err != nil {
			t.Fatalf("materialize: %v", err)
		}
		rows = append(rows, r)
	}
	for i := 0; i < 10; i++ {
		val := fmt.Sprintf(`{"name":"User%d","age":40,"city":"City0","active":true,"score":1.5}`, i)
		r, err := MaterializeRow(b, fmt.Sprintf("user:repeat%d", i), []byte(val))
		if err != nil {
			t.Fatalf("materialize repeat: %v", err)
		}
		rows = append(rows, r)
	}
	src := NewMemorySource(rows)
	sql := "SELECT name, COUNT(*), SUM(age) FROM users GROUP BY name ORDER BY name"
	large, _, err := runWithContract(t, sql, Contract{}, map[string]Source{"users": src})
	if err != nil {
		t.Fatalf("large-budget agg: %v", err)
	}
	small, smallTrace, err := runWithContract(t, sql, Contract{MaxMemoryBytes: 12000}, map[string]Source{"users": src})
	if err != nil {
		t.Fatalf("small-budget agg: %v", err)
	}
	if !smallTrace.Spilled {
		t.Fatalf("small-budget agg should have spilled, trace: %+v", smallTrace)
	}
	if len(small) != 200 || len(large) != 200 {
		t.Fatalf("agg rows: small=%d large=%d, want 200 each", len(small), len(large))
	}
	for i := range large {
		if large[i].String() != small[i].String() {
			t.Fatalf("agg row %d differs: small=%v large=%v", i, small[i], large[i])
		}
	}
	// Repeated groups: exactly 2 rows counted, ages 10 + 40 = 50.
	byName := map[string]Row{}
	for _, row := range small {
		// Group-key column name is the qualified name (alias + ".name",
		// alias empty when the query gives none); find it by suffix.
		for i, c := range row.Cols {
			if strings.HasSuffix(strings.ToLower(c), "name") && i < len(row.Vals) {
				byName[row.Vals[i].S] = row
			}
		}
	}
	for i := 0; i < 10; i++ {
		row, ok := byName[fmt.Sprintf("User%d", i)]
		if !ok {
			t.Fatalf("repeated group User%d missing", i)
		}
		c, _ := row.Get("count")
		s, _ := row.Get("sum_age")
		if c.I != 2 || s.I != 50 {
			t.Fatalf("repeated group User%d: count=%d sum=%d, want 2 / 50", i, c.I, s.I)
		}
	}
}

func TestAggregationEmptyInputBehaviourWithSpillBudget(t *testing.T) {
	users := makeManyUsers(t, 100)
	// Grouped over empty input: zero rows, even with a spill budget.
	rows, _, err := runWithContract(t,
		"SELECT city, COUNT(*) FROM users WHERE age > 1000 GROUP BY city",
		Contract{MaxMemoryBytes: 8000}, map[string]Source{"users": users})
	if err != nil {
		t.Fatalf("grouped empty: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("grouped empty rows = %d, want 0", len(rows))
	}
	// Ungrouped over empty input: exactly one row, COUNT=0, rest NULL.
	rows, _, err = runWithContract(t,
		"SELECT COUNT(*), SUM(age), AVG(age), MIN(age), MAX(age) FROM users WHERE age > 1000",
		Contract{MaxMemoryBytes: 8000}, map[string]Source{"users": users})
	if err != nil {
		t.Fatalf("ungrouped empty: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("ungrouped empty rows = %d, want 1", len(rows))
	}
	c, _ := rows[0].Get("count")
	s, _ := rows[0].Get("sum_age")
	if c.I != 0 || s.Kind != KindNull {
		t.Fatalf("ungrouped empty = %v, want count 0 and NULL sum", rows[0])
	}
}

func TestBudgetTooSmallForOneBatchFails(t *testing.T) {
	cases := []struct {
		name string
		sql  string
	}{
		{"sort", "SELECT name, age FROM users ORDER BY age"},
		{"join", "SELECT users.name, orders.total FROM users JOIN orders ON users.key = orders.user_id"},
		{"agg", "SELECT name, COUNT(*) FROM users GROUP BY name"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rows, trace, err := runWithContract(t, tc.sql, Contract{MaxMemoryBytes: 10},
				map[string]Source{"users": memUsers(t), "orders": memOrders(t)})
			if err == nil {
				t.Fatalf("budget 10: expected BUDGET_EXCEEDED, got rows %v", rows)
			}
			if !strings.Contains(err.Error(), "BUDGET_EXCEEDED") {
				t.Fatalf("budget 10 error = %v, want BUDGET_EXCEEDED", err)
			}
			if rows != nil {
				t.Fatalf("budget 10: rows should be nil, got %v", rows)
			}
			if trace == nil || trace.ExecutionError == "" {
				t.Fatalf("budget 10: trace should carry execution error, got %+v", trace)
			}
			if trace != nil && trace.ActualReturnedRows != 0 {
				t.Fatalf("budget 10: returned rows = %d, want 0", trace.ActualReturnedRows)
			}
		})
	}
}

func TestSpillTempFilesCleanedUp(t *testing.T) {
	// Success: a spilling sort must leave no temp files behind.
	before := spillTempFiles(t)
	users := makeManyUsers(t, 500)
	if _, _, err := runWithContract(t, "SELECT name, age FROM users ORDER BY age",
		Contract{MaxMemoryBytes: 8000}, map[string]Source{"users": users}); err != nil {
		t.Fatalf("spilling sort: %v", err)
	}
	assertNoNewSpillFiles(t, before)

	// Abort after spill: a spilling join that then aborts on the
	// result cap must still clean up (deferred iterator closes).
	before = spillTempFiles(t)
	_, _, err := runWithContract(t,
		"SELECT users.name, orders.total FROM users JOIN orders ON users.key = orders.user_id ORDER BY total",
		Contract{MaxMemoryBytes: 10000, MaxResultRows: 2},
		map[string]Source{"users": makeManyUsers(t, 200), "orders": makeManyOrders(t, 200)})
	if err == nil || !strings.Contains(err.Error(), "BUDGET_EXCEEDED") {
		t.Fatalf("result-cap abort: err = %v, want BUDGET_EXCEEDED", err)
	}
	assertNoNewSpillFiles(t, before)
}

func TestMeasuredMemoryIsNotRowCountTimesConstant(t *testing.T) {
	// Two datasets with the same row count but very different value
	// sizes must report different measured memory. The verifier's
	// 256/row heuristic cannot distinguish them; rowSize can.
	short := makeManyUsers(t, 100)
	plan, err := Compile("SELECT name, age FROM users ORDER BY age", testSchema())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	_, trShort, err := Execute(context.Background(), plan, Contract{}, map[string]Source{"users": short})
	if err != nil {
		t.Fatalf("short execute: %v", err)
	}
	// Same row count, much longer names.
	b := bindingFor(t, testSchema(), "users")
	var longRows []Row
	for i := 0; i < 100; i++ {
		r, err := MaterializeRow(b, fmt.Sprintf("user:%d", i),
			[]byte(fmt.Sprintf(`{"name":"%s","age":%d,"city":"City%d"}`, strings.Repeat("LongName", 20), 20+(i%50), i%10)))
		if err != nil {
			t.Fatalf("materialize long: %v", err)
		}
		longRows = append(longRows, r)
	}
	_, trLong, err := Execute(context.Background(), plan, Contract{}, map[string]Source{"users": NewMemorySource(longRows)})
	if err != nil {
		t.Fatalf("long execute: %v", err)
	}
	if trLong.ActualMemoryBytes <= trShort.ActualMemoryBytes {
		t.Fatalf("measured memory should grow with real value sizes: short=%d long=%d",
			trShort.ActualMemoryBytes, trLong.ActualMemoryBytes)
	}
	if trLong.MemoryEstimateBytes != trShort.MemoryEstimateBytes {
		t.Fatalf("heuristic should be identical for equal row counts: short=%d long=%d",
			trShort.MemoryEstimateBytes, trLong.MemoryEstimateBytes)
	}
}
