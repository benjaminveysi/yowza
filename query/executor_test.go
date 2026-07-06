package query

import (
	"context"
	"testing"
)

func TestFilterProjectLimitOrder(t *testing.T) {
	rows, _ := runQuery(t,
		"SELECT name, age FROM users WHERE age >= 30 ORDER BY age DESC LIMIT 2",
		map[string]Source{"users": memUsers(t)})
	if len(rows) != 2 {
		t.Fatalf("rows = %d, want 2: %v", len(rows), rowStrings(t, rows))
	}
	n0, _ := rows[0].Get("name")
	a0, _ := rows[0].Get("age")
	n1, _ := rows[1].Get("name")
	a1, _ := rows[1].Get("age")
	if n0.S != "Carol" || a0.I != 40 || n1.S != "Eve" || a1.I != 35 {
		t.Fatalf("rows = %v, want Carol/40 then Eve/35", rowStrings(t, rows))
	}
}

func TestFilterForms(t *testing.T) {
	// OR + NOT + parentheses.
	rows, _ := runQuery(t,
		"SELECT name FROM users WHERE NOT (city = 'Tehran' OR city = 'Shiraz')",
		map[string]Source{"users": memUsers(t)})
	// Dan (NULL city) does not pass NOT(...): under three-valued
	// logic his OR is UNKNOWN, NOT UNKNOWN is UNKNOWN, and only
	// TRUE rows pass. Eve (Tabriz) passes. Only Eve.
	if len(rows) != 1 {
		t.Fatalf("NOT/OR rows = %d, want 1: %v", len(rows), rowStrings(t, rows))
	}
	n, _ := rows[0].Get("name")
	if n.S != "Eve" {
		t.Fatalf("NOT/OR name = %q, want Eve", n.S)
	}
	// IN + LIKE + IS NULL.
	rows, _ = runQuery(t,
		"SELECT name FROM users WHERE city IN ('Tehran', 'Tabriz')",
		map[string]Source{"users": memUsers(t)})
	if len(rows) != 3 {
		t.Fatalf("IN rows = %d, want 3: %v", len(rows), rowStrings(t, rows))
	}
	rows, _ = runQuery(t,
		"SELECT name FROM users WHERE name LIKE 'A%'",
		map[string]Source{"users": memUsers(t)})
	if len(rows) != 1 {
		t.Fatalf("LIKE rows = %d, want 1", len(rows))
	}
	rows, _ = runQuery(t,
		"SELECT name FROM users WHERE city IS NULL",
		map[string]Source{"users": memUsers(t)})
	if len(rows) != 1 {
		t.Fatalf("IS NULL rows = %d, want 1 (Dan)", len(rows))
	}
	n, _ = rows[0].Get("name")
	if n.S != "Dan" {
		t.Fatalf("IS NULL name = %q, want Dan", n.S)
	}
	// Comparison against NULL is not-true, never an error.
	rows, _ = runQuery(t,
		"SELECT name FROM users WHERE age = NULL",
		map[string]Source{"users": memUsers(t)})
	if len(rows) != 0 {
		t.Fatalf("= NULL rows = %d, want 0", len(rows))
	}
}

func TestGroupByAggregates(t *testing.T) {
	rows, _ := runQuery(t,
		"SELECT city, COUNT(*), AVG(age) FROM users GROUP BY city ORDER BY city",
		map[string]Source{"users": memUsers(t)})
	// Groups in first-appearance order, then sorted by city ASC with
	// NULL last: Shiraz, Tabriz, Tehran, NULL.
	if len(rows) != 4 {
		t.Fatalf("groups = %d, want 4: %v", len(rows), rowStrings(t, rows))
	}
	c0, _ := rows[0].Get("city")
	n0, _ := rows[0].Get("count")
	if c0.S != "Shiraz" || n0.I != 1 {
		t.Fatalf("first group = %v, want Shiraz/1", rowStrings(t, rows))
	}
	// Tehran group: Alice 30, Carol 40 => count 2, avg 35.
	var tehran Row
	for _, r := range rows {
		c, _ := r.Get("city")
		if c.S == "Tehran" {
			tehran = r
		}
	}
	n, _ := tehran.Get("count")
	a, _ := tehran.Get("avg_age")
	if n.I != 2 || a.F != 35.0 {
		t.Fatalf("Tehran group = %v, want count 2 avg 35", tehran)
	}
	// NULL city group (Dan) sorts last.
	clast, _ := rows[3].Get("city")
	if clast.Kind != KindNull {
		t.Fatalf("last group city should be NULL, got %v", rows[3])
	}
}

func TestUngroupedAggregatesOnEmpty(t *testing.T) {
	rows, _ := runQuery(t,
		"SELECT COUNT(*), SUM(age), AVG(age), MIN(age), MAX(age) FROM users WHERE age > 1000",
		map[string]Source{"users": memUsers(t)})
	if len(rows) != 1 {
		t.Fatalf("ungrouped empty rows = %d, want 1", len(rows))
	}
	c, _ := rows[0].Get("count")
	s, _ := rows[0].Get("sum_age")
	a, _ := rows[0].Get("avg_age")
	mi, _ := rows[0].Get("min_age")
	if c.I != 0 || s.Kind != KindNull || a.Kind != KindNull || mi.Kind != KindNull {
		t.Fatalf("ungrouped empty = %v, want count 0 and NULL aggregates", rows[0])
	}
}

func TestJoin(t *testing.T) {
	rows, _ := runQuery(t,
		"SELECT users.name, orders.total FROM users JOIN orders ON users.key = orders.user_id WHERE orders.total > 100 ORDER BY total DESC",
		map[string]Source{"users": memUsers(t), "orders": memOrders(t)})
	if len(rows) != 2 {
		t.Fatalf("join rows = %d, want 2: %v", len(rows), rowStrings(t, rows))
	}
	n0, _ := rows[0].Get("name")
	t0, _ := rows[0].Get("total")
	if n0.S != "Carol" || t0.F != 300.0 {
		t.Fatalf("first join row = %v, want Carol/300", rowStrings(t, rows))
	}
	n1, _ := rows[1].Get("name")
	if n1.S != "Alice" {
		t.Fatalf("second join row name = %q, want Alice", n1.S)
	}
}

func TestBatchSizeDoesNotChangeResults(t *testing.T) {
	plan, err := Compile("SELECT name, age FROM users WHERE age >= 22 ORDER BY age", testSchema())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	rows64, _, err := Execute(context.Background(), plan, Contract{}, map[string]Source{"users": memUsers(t)})
	if err != nil {
		t.Fatalf("execute batch 64: %v", err)
	}
	rows2, _, err := ExecuteWithOptions(context.Background(), plan, Contract{}, map[string]Source{"users": memUsers(t)}, &ExecuteOptions{BatchSize: 2})
	if err != nil {
		t.Fatalf("execute batch 2: %v", err)
	}
	if len(rows64) != len(rows2) {
		t.Fatalf("batch sizes differ: %d vs %d", len(rows64), len(rows2))
	}
	for i := range rows64 {
		if rows64[i].String() != rows2[i].String() {
			t.Fatalf("row %d differs: %v vs %v", i, rows64[i], rows2[i])
		}
	}
}

func TestTraceContents(t *testing.T) {
	plan, err := Compile("SELECT name FROM users WHERE age >= 30", testSchema())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	rows, trace, err := Execute(context.Background(), plan, Contract{}, map[string]Source{"users": memUsers(t)})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("rows = %d, want 3", len(rows))
	}
	if trace == nil {
		t.Fatalf("trace is nil")
	}
	if len(trace.QueryID) != 16 {
		t.Fatalf("query id = %q, want 16 hex chars", trace.QueryID)
	}
	if trace.ActualRows != 3 || trace.ScannedRows != 5 {
		t.Fatalf("trace rows: actual=%d scanned=%d, want 3/5", trace.ActualRows, trace.ScannedRows)
	}
	if trace.EstimatedRows != -1 {
		t.Fatalf("estimated rows = %d, want -1 (unknown, no stats)", trace.EstimatedRows)
	}
	if trace.ValidationError != nil || trace.ExecutionError != "" {
		t.Fatalf("trace should be clean: %+v", trace)
	}
	if trace.CommitBoundary == "" || len(trace.OperatorStats) == 0 {
		t.Fatalf("trace missing boundary/stats: %+v", trace)
	}
	foundScan, foundProject := false, false
	for _, s := range trace.OperatorStats {
		if s.Operator == "SCAN" {
			foundScan = true
			if s.RowsOut != 5 {
				t.Fatalf("SCAN rows out = %d, want 5", s.RowsOut)
			}
		}
		if s.Operator == "PROJECT" {
			foundProject = true
		}
	}
	if !foundScan || !foundProject {
		t.Fatalf("operator stats missing SCAN/PROJECT: %+v", trace.OperatorStats)
	}
	// Estimated vs actual for a LIMIT query: estimate is the limit.
	plan2, _ := Compile("SELECT name FROM users LIMIT 2", testSchema())
	_, trace2, err := Execute(context.Background(), plan2, Contract{}, map[string]Source{"users": memUsers(t)})
	if err != nil {
		t.Fatalf("execute limit: %v", err)
	}
	if trace2.EstimatedRows != 2 || trace2.ActualRows != 2 {
		t.Fatalf("limit trace: estimated=%d actual=%d, want 2/2", trace2.EstimatedRows, trace2.ActualRows)
	}
}

func TestSnapshotAtOpen(t *testing.T) {
	src := memUsers(t)
	b := bindingFor(t, testSchema(), "users")
	it, err := src.Open(context.Background(), b)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer it.Close()
	// Append after Open: the iterator must not see the new row.
	nb := bindingFor(t, testSchema(), "users")
	r, err := MaterializeRow(nb, "user:99", []byte(`{"name":"Zed","age":50,"city":"X"}`))
	if err != nil {
		t.Fatalf("materialize: %v", err)
	}
	src.Append(r)
	var seen int
	for {
		batch, err := it.NextBatch(2)
		if err != nil {
			t.Fatalf("next batch: %v", err)
		}
		if len(batch) == 0 {
			break
		}
		seen += len(batch)
	}
	if seen != 5 {
		t.Fatalf("snapshot iterator saw %d rows, want 5 (append after Open must be invisible)", seen)
	}
}
