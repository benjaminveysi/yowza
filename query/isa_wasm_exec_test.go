// Differential tests: Go batch executor (reference) vs the
// WASM batch executor (query/isa_wasm_exec.go) over the same
// verified plans and the same MemorySource data. Every case runs
// on BOTH wasm binaries (wasm32 + wasm64) and must agree
// row-for-row (column order, value kind, exact payload),
// error-class-for-error-class, scanned rows, and spilled flag.
// Peak memory is compared exactly except for aggregate spill
// cases (see the comment on TestWasmBatchDifferential's
// comparePeak flag: Go's post-merge output-store notes can raise
// its peak above the groups peak, while the batch API reports
// operator peaks only — rows, spilled, and error class still
// agree there).
//
// The Go executor is the reference for semantics, so any
// divergence is a bug in one of the two engines — except the
// documented batch-API limits, which have their own tests
// (group cap, state-buffer exhaustion) asserting a loud typed
// error on the WASM side where Go succeeds.
package query

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"

	yowza "github.com/yowza/yowza"
)

type wasmBatchCase struct {
	name     string
	sql      string
	contract Contract
	// sources: "users", "orders", "bigUsers", "bigOrders" keys
	// select which fixture map to use; empty means users+orders.
	fixture   string
	batchSize int
	// comparePeak: false only for aggregate spill cases (see header).
	comparePeak bool
	// compareScanned: false when the case's pull sequence is
	// allowed to differ (none currently: all cases compare it).
	compareScanned bool
}

func wasmBatchCases() []wasmBatchCase {
	def := func(name, sql string) wasmBatchCase {
		return wasmBatchCase{name: name, sql: sql, contract: Contract{}, comparePeak: true, compareScanned: true}
	}
	cases := []wasmBatchCase{
		// SCAN / PROJECT / FILTER (three-valued NULL logic).
		def("scan-project", "SELECT name, age FROM users"),
		def("filter-and", "SELECT name FROM users WHERE age >= 30 AND city = 'Tehran'"),
		def("filter-or-not-3vl", "SELECT name FROM users WHERE NOT (city = 'Tehran' OR city = 'Shiraz')"),
		def("filter-eq-null", "SELECT name FROM users WHERE city = NULL"),
		def("filter-ne-null", "SELECT name FROM users WHERE city != NULL"),
		def("filter-in-null", "SELECT name FROM users WHERE city IN ('Tehran', NULL)"),
		def("filter-not-in-null", "SELECT name FROM users WHERE city NOT IN ('Tehran')"),
		def("filter-isnull", "SELECT name FROM users WHERE city IS NULL"),
		def("filter-isnotnull", "SELECT name FROM users WHERE city IS NOT NULL"),
		def("filter-like", "SELECT name FROM users WHERE name LIKE 'A%'"),
		def("filter-like-underscore", "SELECT name FROM users WHERE name LIKE 'A____'"),
		def("filter-not-like", "SELECT name FROM users WHERE name NOT LIKE '%a%'"),
		def("filter-bool", "SELECT name FROM users WHERE active = TRUE"),
		def("filter-float-int", "SELECT name FROM users WHERE score = 9"),
		def("filter-int-float", "SELECT name FROM users WHERE score > 8.5 AND age < 40"),
		// JOIN.
		def("join-basic", "SELECT users.name, orders.item FROM users JOIN orders ON users.key = orders.user_id"),
		def("join-filter-sort", "SELECT users.name, orders.total FROM users JOIN orders ON users.key = orders.user_id WHERE orders.total > 100 ORDER BY total DESC"),
		def("join-filter-both", "SELECT users.name, orders.total FROM users JOIN orders ON users.key = orders.user_id WHERE users.city = 'Tehran' AND orders.total > 100 ORDER BY total"),
		def("join-group", "SELECT users.city, COUNT(*) FROM users JOIN orders ON users.key = orders.user_id GROUP BY users.city ORDER BY city"),
		// SORT (stable, NULLs last both directions).
		def("sort-city-asc-nulls-last", "SELECT name, city FROM users ORDER BY city"),
		def("sort-city-desc-nulls-last", "SELECT name, city FROM users ORDER BY city DESC"),
		def("sort-multi-stable", "SELECT name, city, age FROM users ORDER BY city, age DESC"),
		def("sort-age-desc", "SELECT name, age FROM users ORDER BY age DESC"),
		// GROUP BY / AGGREGATION.
		def("group-city", "SELECT city, COUNT(*) FROM users GROUP BY city"),
		def("group-aggs", "SELECT city, COUNT(*), COUNT(city), SUM(age), AVG(score), MIN(age), MAX(age) FROM users GROUP BY city"),
		def("group-only", "SELECT city FROM users GROUP BY city"),
		def("group-empty", "SELECT city, COUNT(*) FROM users WHERE age > 1000 GROUP BY city"),
		def("ungrouped", "SELECT COUNT(*), COUNT(city), SUM(age), AVG(score), MIN(age), MAX(age) FROM users"),
		def("ungrouped-empty", "SELECT COUNT(*), SUM(age), AVG(score), MIN(age) FROM users WHERE age > 1000"),
		def("group-sort-limit", "SELECT city, COUNT(*) AS n FROM users GROUP BY city ORDER BY n DESC LIMIT 2"),
		// LIMIT (after global semantics).
		def("limit-0", "SELECT name FROM users LIMIT 0"),
		def("limit-1", "SELECT name FROM users LIMIT 1"),
		def("limit-exact", "SELECT name FROM users LIMIT 5"),
		def("limit-over", "SELECT name FROM users LIMIT 99"),
		def("limit-sort", "SELECT name, age FROM users ORDER BY age DESC LIMIT 2"),
		def("limit-join-0", "SELECT users.name FROM users JOIN orders ON users.key = orders.user_id LIMIT 0"),
		def("limit-agg", "SELECT city, COUNT(*) FROM users GROUP BY city LIMIT 2"),
		// Budgets (runtime aborts; estimate unknown, so Verify defers).
		{name: "budget-scanned", sql: "SELECT name FROM users", contract: Contract{MaxScannedRows: 3}, comparePeak: true, compareScanned: true},
		{name: "budget-scanned-join", sql: "SELECT users.name FROM users JOIN orders ON users.key = orders.user_id", contract: Contract{MaxScannedRows: 6}, comparePeak: true, compareScanned: true},
		{name: "budget-result", sql: "SELECT name FROM users", contract: Contract{MaxResultRows: 2}, comparePeak: true, compareScanned: true},
		{name: "budget-result-limit-ok", sql: "SELECT name FROM users LIMIT 2", contract: Contract{MaxResultRows: 5, MaxScannedRows: 100}, comparePeak: true, compareScanned: true},
		// Empty input.
		{name: "empty-plain", sql: "SELECT name FROM users", contract: Contract{}, fixture: "empty", comparePeak: true, compareScanned: true},
		{name: "empty-ungrouped", sql: "SELECT COUNT(*), SUM(age) FROM users", contract: Contract{}, fixture: "empty", comparePeak: true, compareScanned: true},
		// Big tables (150 users) at default batch size.
		{name: "big-scan", sql: "SELECT name, age FROM bigUsers", contract: Contract{}, fixture: "big", comparePeak: true, compareScanned: true},
		{name: "big-filter-limit", sql: "SELECT name FROM bigUsers WHERE age >= 60 LIMIT 3", contract: Contract{}, fixture: "big", comparePeak: true, compareScanned: true},
		{name: "big-sort", sql: "SELECT name, age FROM bigUsers ORDER BY age DESC, name LIMIT 5", contract: Contract{}, fixture: "big", comparePeak: true, compareScanned: true},
		{name: "big-group", sql: "SELECT city, COUNT(*), SUM(age) FROM bigUsers GROUP BY city", contract: Contract{}, fixture: "big", comparePeak: true, compareScanned: true},
		// Spill behaviour (batch size 2, budget between one
		// batch and the retained total): both engines spill
		// and return identical rows with spilled=true.
		{name: "spill-join", sql: "SELECT bigUsers.name, bigOrders.item FROM bigUsers JOIN bigOrders ON bigUsers.key = bigOrders.user_id", contract: Contract{MaxMemoryBytes: 900}, fixture: "big", batchSize: 2, comparePeak: true, compareScanned: true},
		{name: "spill-sort", sql: "SELECT name, age FROM bigUsers ORDER BY age", contract: Contract{MaxMemoryBytes: 900}, fixture: "big", batchSize: 2, comparePeak: true, compareScanned: true},
		{name: "spill-agg", sql: "SELECT city, COUNT(*), SUM(age) FROM bigUsers GROUP BY city", contract: Contract{MaxMemoryBytes: 350}, fixture: "big", batchSize: 2, comparePeak: false, compareScanned: true},
		// Single batch over budget: typed budget error, no rows.
		// (Sort input is the wide projected row, so one batch
		// of 2 rows really exceeds the 100-byte budget.)
		{name: "budget-single-batch", sql: "SELECT name, age, city, score FROM bigUsers ORDER BY age", contract: Contract{MaxMemoryBytes: 100}, fixture: "big", batchSize: 2, comparePeak: true, compareScanned: true},
	}
	return cases
}

// wasmBatchSources builds the fixture sources for a case. The
// table names in SQL are the schema names ("users", "orders");
// the big fixtures reuse the same schemas under distinct
// source keys, so big-case SQL names the fixture table
// (bigUsers/bigOrders) and the plan's bindings resolve through
// a schema extended with those tables.
func wasmBatchSources(t *testing.T, fixture string) map[string]Source {
	t.Helper()
	switch fixture {
	case "empty":
		return map[string]Source{"users": NewMemorySource(nil), "orders": NewMemorySource(nil)}
	case "big":
		return map[string]Source{
			"bigUsers":  bigUsersSource(t),
			"bigOrders": bigOrdersSource(t),
		}
	default:
		return map[string]Source{"users": memUsers(t), "orders": memOrders(t)}
	}
}

func wasmBatchSchema(fixture string) *Schema {
	s := testSchema()
	if fixture == "big" {
		users, _ := s.TableByName("users")
		orders, _ := s.TableByName("orders")
		bigUsers := users
		bigUsers.Name = "bigUsers"
		bigUsers.Prefix = "biguser:"
		bigOrders := orders
		bigOrders.Name = "bigOrders"
		bigOrders.Prefix = "bigorder:"
		s.Tables = append(s.Tables, bigUsers, bigOrders)
	}
	return s
}

func bigUsersSource(t *testing.T) *MemorySource {
	t.Helper()
	schema := wasmBatchSchema("big")
	b := bindingFor(t, schema, "bigUsers")
	cities := []string{"Tehran", "Shiraz", "Tabriz", "Mashhad", ""}
	var rows []Row
	for i := 1; i <= 150; i++ {
		city := cities[i%len(cities)]
		var val string
		if city == "" {
			val = fmt.Sprintf(`{"name":"U%03d","age":%d,"active":%v,"score":%.1f}`, i, 20+(i%50), i%2 == 0, float64(i%10)+0.5)
		} else {
			val = fmt.Sprintf(`{"name":"U%03d","age":%d,"city":%q,"active":%v,"score":%.1f}`, i, 20+(i%50), city, i%2 == 0, float64(i%10)+0.5)
		}
		r, err := MaterializeRow(b, fmt.Sprintf("biguser:%d", i), []byte(val))
		if err != nil {
			t.Fatalf("materialize big user %d: %v", i, err)
		}
		rows = append(rows, r)
	}
	return NewMemorySource(rows)
}

func bigOrdersSource(t *testing.T) *MemorySource {
	t.Helper()
	schema := wasmBatchSchema("big")
	b := bindingFor(t, schema, "bigOrders")
	var rows []Row
	for i := 1; i <= 100; i++ {
		val := fmt.Sprintf(`{"user_id":"biguser:%d","total":%.1f,"item":"item%03d"}`, (i%150)+1, float64(i)+0.25, i)
		r, err := MaterializeRow(b, fmt.Sprintf("bigorder:%d", i), []byte(val))
		if err != nil {
			t.Fatalf("materialize big order %d: %v", i, err)
		}
		rows = append(rows, r)
	}
	return NewMemorySource(rows)
}

func wasmBatchErrClass(err error) string {
	if err == nil {
		return "none"
	}
	if ve, ok := err.(*VerificationError); ok && ve.Code == "BUDGET_EXCEEDED" {
		return "budget"
	}
	if errors.Is(err, ErrResourceLimit) {
		return "limit"
	}
	return "other"
}

func TestWasmBatchDifferential(t *testing.T) {
	cases := wasmBatchCases()
	for _, bin := range []string{"../build/yowza.wasm", "../build/yowza64.wasm"} {
		t.Run(bin, func(t *testing.T) {
			wasmBytes, err := os.ReadFile(bin)
			if err != nil {
				t.Skipf("wasm binary %s absent: %v", bin, err)
			}
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					plan, err := Compile(tc.sql, wasmBatchSchema(tc.fixture))
					if err != nil {
						t.Fatalf("compile %q: %v", tc.sql, err)
					}
					sources := wasmBatchSources(t, tc.fixture)
					opts := &ExecuteOptions{}
					if tc.batchSize > 0 {
						opts.BatchSize = tc.batchSize
					}
					refRows, refTrace, refErr := ExecuteWithOptions(context.Background(), plan, tc.contract, sources, opts)
					ex, err := NewWasmBatchExecutor(wasmBytes, &WasmBatchOptions{BatchSize: opts.BatchSize})
					if err != nil {
						t.Fatalf("NewWasmBatchExecutor: %v", err)
					}
					defer ex.Close()
					wasmRows, wasmTrace, wasmErr := ex.ExecutePlan(context.Background(), plan, tc.contract, sources, opts)
					if wasmBatchErrClass(refErr) != wasmBatchErrClass(wasmErr) {
						t.Fatalf("error class differs: reference=%v (%s) wasm=%v (%s)", refErr, wasmBatchErrClass(refErr), wasmErr, wasmBatchErrClass(wasmErr))
					}
					if refErr != nil {
						if len(wasmRows) != 0 {
							t.Fatalf("error case returned %d rows on wasm side, want 0", len(wasmRows))
						}
						return
					}
					if !rowsEqual(t, refRows, wasmRows) {
						t.Fatalf("rows differ:\nreference: %v\nwasm:      %v", rowStrings(t, refRows), rowStrings(t, wasmRows))
					}
					if tc.compareScanned && refTrace.ScannedRows != wasmTrace.ScannedRows {
						t.Fatalf("scanned rows differ: reference=%d wasm=%d", refTrace.ScannedRows, wasmTrace.ScannedRows)
					}
					if refTrace.Spilled != wasmTrace.Spilled {
						t.Fatalf("spilled differs: reference=%v wasm=%v", refTrace.Spilled, wasmTrace.Spilled)
					}
					if tc.comparePeak && refTrace.ActualMemoryBytes != wasmTrace.ActualMemoryBytes {
						t.Fatalf("memory peak differs: reference=%d wasm=%d", refTrace.ActualMemoryBytes, wasmTrace.ActualMemoryBytes)
					}
				})
			}
		})
	}
}

// Batch size must never change results: the same mixed
// join/filter/sort/limit query at several batch sizes agrees
// between engines (rows and scanned rows) at each size.
func TestWasmBatchSizeInvariance(t *testing.T) {
	wasmBytes, err := os.ReadFile("../build/yowza.wasm")
	if err != nil {
		t.Skipf("wasm binary absent: %v", err)
	}
	sql := "SELECT users.name, orders.total FROM users JOIN orders ON users.key = orders.user_id WHERE orders.total > 40 ORDER BY total DESC LIMIT 3"
	plan, err := Compile(sql, testSchema())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	for _, bs := range []int{1, 2, 7, 64, 200} {
		opts := &ExecuteOptions{BatchSize: bs}
		sources := map[string]Source{"users": memUsers(t), "orders": memOrders(t)}
		refRows, refTrace, refErr := ExecuteWithOptions(context.Background(), plan, Contract{}, sources, opts)
		if refErr != nil {
			t.Fatalf("batch %d: reference error: %v", bs, refErr)
		}
		ex, err := NewWasmBatchExecutor(wasmBytes, &WasmBatchOptions{BatchSize: bs})
		if err != nil {
			t.Fatalf("batch %d: NewWasmBatchExecutor: %v", bs, err)
		}
		wasmRows, wasmTrace, wasmErr := ex.ExecutePlan(context.Background(), plan, Contract{}, sources, opts)
		ex.Close()
		if wasmErr != nil {
			t.Fatalf("batch %d: wasm error: %v", bs, wasmErr)
		}
		if !rowsEqual(t, refRows, wasmRows) {
			t.Fatalf("batch %d: rows differ: %v vs %v", bs, rowStrings(t, refRows), rowStrings(t, wasmRows))
		}
		if refTrace.ScannedRows != wasmTrace.ScannedRows {
			t.Fatalf("batch %d: scanned differs: %d vs %d", bs, refTrace.ScannedRows, wasmTrace.ScannedRows)
		}
	}
}

// Batch wire round-trip over every value kind.
func TestWasmBatchWireRoundTrip(t *testing.T) {
	rows := []Row{{
		Cols: []string{"a", "b", "c", "d", "e", "f"},
		Vals: []Value{Null(), Int(-9223372036854775807), Float(-0.5), String(""), Bool(true), Bytes([]byte{0, 1, 255})},
	}, {
		Cols: []string{"a", "b", "c", "d", "e", "f"},
		Vals: []Value{Int(0), Float(0), String("hello"), Bool(false), Bytes(nil), Null()},
	}}
	enc, err := encodeQBatch(rows, 6)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	dec, err := decodeQBatch(enc, 6, rows[0].Cols)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !rowsEqual(t, rows, dec) {
		t.Fatalf("round-trip differs: %v vs %v", rowStrings(t, rows), rowStrings(t, dec))
	}
	if _, err := decodeQBatch(append(enc, 0), 6, rows[0].Cols); err == nil {
		t.Fatalf("trailing bytes decoded without error")
	}
}

// Runtime type errors must fail loudly on both sides, never
// approximate. These plans bypass the binder on purpose (the
// binder would reject them); the engines still must refuse.
func TestWasmBatchTypeErrorsFailLoudly(t *testing.T) {
	wasmBytes, err := os.ReadFile("../build/yowza.wasm")
	if err != nil {
		t.Skipf("wasm binary absent: %v", err)
	}
	// Filter comparing a string column to an int literal.
	plan, err := Compile("SELECT name FROM users", testSchema())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	plan.Filter = &Expr{Kind: "cmp", Op: "=",
		Left:  &Expr{Kind: "col", Col: &ColumnRef{Alias: "users", Name: "name", Qualified: "users.name"}},
		Right: &Expr{Kind: "lit", Lit: func() *Value { v := Int(5); return &v }()},
	}
	sources := map[string]Source{"users": memUsers(t)}
	_, _, refErr := Execute(context.Background(), plan, Contract{}, sources)
	if refErr == nil {
		t.Fatalf("reference accepted string=int comparison")
	}
	ex, err := NewWasmBatchExecutor(wasmBytes, nil)
	if err != nil {
		t.Fatalf("NewWasmBatchExecutor: %v", err)
	}
	defer ex.Close()
	wasmRows, _, wasmErr := ex.ExecutePlan(context.Background(), plan, Contract{}, sources, nil)
	if wasmErr == nil {
		t.Fatalf("wasm accepted string=int comparison")
	}
	if len(wasmRows) != 0 {
		t.Fatalf("wasm returned %d rows on type error", len(wasmRows))
	}
	// Join with incompatible key kinds (string vs float).
	plan2, err := Compile("SELECT users.name FROM users JOIN orders ON users.key = orders.user_id", testSchema())
	if err != nil {
		t.Fatalf("compile join: %v", err)
	}
	plan2.Join.LeftCol = &ColumnRef{Alias: "users", Name: "name", Qualified: "users.name"}
	plan2.Join.RightCol = &ColumnRef{Alias: "orders", Name: "total", Qualified: "orders.total"}
	sources2 := map[string]Source{"users": memUsers(t), "orders": memOrders(t)}
	_, _, refErr2 := Execute(context.Background(), plan2, Contract{}, sources2)
	if refErr2 == nil {
		t.Fatalf("reference accepted string/float join keys")
	}
	wasmRows2, _, wasmErr2 := ex.ExecutePlan(context.Background(), plan2, Contract{}, sources2, nil)
	if wasmErr2 == nil {
		t.Fatalf("wasm accepted string/float join keys")
	}
	if len(wasmRows2) != 0 {
		t.Fatalf("wasm returned %d rows on join type error", len(wasmRows2))
	}
}

type countingBatchSource struct {
	rows  []Row
	opens *int
}

func (s *countingBatchSource) Estimate() int64 { return -1 }

func (s *countingBatchSource) Open(ctx context.Context, b Binding) (RowIterator, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	*s.opens++
	return &sliceIterator{rows: append([]Row(nil), s.rows...)}, nil
}

// A plan that fails verification must open zero sources, and a
// failed execution must return no rows.
func TestWasmBatchVerificationBeforeOpen(t *testing.T) {
	wasmBytes, err := os.ReadFile("../build/yowza.wasm")
	if err != nil {
		t.Skipf("wasm binary absent: %v", err)
	}
	plan, err := Compile("SELECT name FROM users", testSchema())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	opens := 0
	sources := map[string]Source{"users": &countingBatchSource{rows: memUsers(t).rows, opens: &opens}}
	contract := Contract{AllowedTables: []string{"orders"}}
	ex, err := NewWasmBatchExecutor(wasmBytes, nil)
	if err != nil {
		t.Fatalf("NewWasmBatchExecutor: %v", err)
	}
	defer ex.Close()
	rows, trace, err := ex.ExecutePlan(context.Background(), plan, contract, sources, nil)
	if err == nil {
		t.Fatalf("disallowed table executed without error")
	}
	if opens != 0 {
		t.Fatalf("verification failure opened %d sources, want 0", opens)
	}
	if len(rows) != 0 {
		t.Fatalf("verification failure returned %d rows", len(rows))
	}
	if trace.ValidationError == nil {
		t.Fatalf("trace has no ValidationError")
	}
}

// Operator state is re-initialized per execution: running the
// same executor twice with different right-side data must not
// leak the first build into the second query.
func TestWasmBatchNoStateLeakAcrossExecutions(t *testing.T) {
	wasmBytes, err := os.ReadFile("../build/yowza.wasm")
	if err != nil {
		t.Skipf("wasm binary absent: %v", err)
	}
	sql := "SELECT users.name, orders.item FROM users JOIN orders ON users.key = orders.user_id ORDER BY item"
	plan, err := Compile(sql, testSchema())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	ex, err := NewWasmBatchExecutor(wasmBytes, nil)
	if err != nil {
		t.Fatalf("NewWasmBatchExecutor: %v", err)
	}
	defer ex.Close()
	sources := map[string]Source{"users": memUsers(t), "orders": memOrders(t)}
	rows1, _, err := ex.ExecutePlan(context.Background(), plan, Contract{}, sources, nil)
	if err != nil {
		t.Fatalf("first execution: %v", err)
	}
	if len(rows1) != 3 {
		t.Fatalf("first execution rows = %d, want 3", len(rows1))
	}
	// Second execution with an empty right side: no stale matches.
	sources["orders"] = NewMemorySource(nil)
	rows2, _, err := ex.ExecutePlan(context.Background(), plan, Contract{}, sources, nil)
	if err != nil {
		t.Fatalf("second execution: %v", err)
	}
	if len(rows2) != 0 {
		t.Fatalf("second execution rows = %d, want 0 (stale join build?)", len(rows2))
	}
}

// Snapshot/lease discipline on the SegDB ISA path (unchanged
// machinery, re-proven here because the batch task's acceptance
// requires it): a future snapshot fails, never reads as
// current; an expired lease fails before any row.
func TestWasmBatchTimelineDiscipline(t *testing.T) {
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
			for _, e := range []struct{ key, val string }{
				{"user:1", `{"name":"Alice","age":30,"city":"Tehran","active":true,"score":9.5}`},
				{"user:2", `{"name":"Bob","age":25,"city":"Shiraz","active":false,"score":7.0}`},
			} {
				if err := host.Put(e.key, []byte(e.val)); err != nil {
					t.Fatalf("put %s: %v", e.key, err)
				}
			}
			plan, err := Compile("SELECT name FROM users WHERE age >= 30", testSchema())
			if err != nil {
				t.Fatalf("compile: %v", err)
			}
			prog, err := CompileISA(plan, Contract{})
			if err != nil {
				t.Fatalf("CompileISA: %v", err)
			}
			handle, err := host.AcquireISATimelineHandle()
			if err != nil {
				t.Fatalf("acquire handle: %v", err)
			}
			// Sanity: the live handle serves rows.
			rows, _, err := host.ExecuteISAAtTimeline(prog, plan, handle, nil)
			if err != nil {
				t.Fatalf("live handle execution: %v", err)
			}
			if len(rows) != 1 {
				t.Fatalf("live handle rows = %d, want 1", len(rows))
			}
			// Future snapshot: refused, never read as current.
			future := ISATimelineHandle{Snapshot: handle.Snapshot + 1000000, LeaseID: 0}
			fRows, _, fErr := host.ExecuteISAAtTimeline(prog, plan, future, nil)
			if fErr == nil {
				t.Fatalf("future snapshot executed without error")
			}
			if !errors.Is(fErr, yowza.ErrSnapshotInvalid) {
				t.Fatalf("future snapshot error %v is not ErrSnapshotInvalid", fErr)
			}
			if len(fRows) != 0 {
				t.Fatalf("future snapshot returned %d rows", len(fRows))
			}
			// Expired lease: release, then the handle fails
			// before any row.
			if err := host.ReleaseISATimelineHandle(handle); err != nil {
				t.Fatalf("release handle: %v", err)
			}
			lRows, _, lErr := host.ExecuteISAAtTimeline(prog, plan, handle, nil)
			if lErr == nil {
				t.Fatalf("expired lease executed without error")
			}
			if !errors.Is(lErr, yowza.ErrLeaseExpired) {
				t.Fatalf("expired lease error %v is not ErrLeaseExpired", lErr)
			}
			if len(lRows) != 0 {
				t.Fatalf("expired lease returned %d rows", len(lRows))
			}
		})
	}
}

// Documented batch-API caps fail loudly, never truncate.
// Group cap: 65537 distinct groups succeed in Go (no cap)
// and fail on the WASM side with ErrResourceLimit.
func TestWasmBatchGroupCapFailsLoudly(t *testing.T) {
	wasmBytes, err := os.ReadFile("../build/yowza.wasm")
	if err != nil {
		t.Skipf("wasm binary absent: %v", err)
	}
	schema := testSchema()
	b := bindingFor(t, schema, "users")
	var rows []Row
	for i := 0; i < 65537; i++ {
		val := fmt.Sprintf(`{"name":"U%d","age":30,"city":"C%06d","active":true,"score":1.0}`, i, i)
		r, err := MaterializeRow(b, fmt.Sprintf("user:%d", i), []byte(val))
		if err != nil {
			t.Fatalf("materialize %d: %v", i, err)
		}
		rows = append(rows, r)
	}
	sources := map[string]Source{"users": NewMemorySource(rows)}
	plan, err := Compile("SELECT city, COUNT(*) FROM users GROUP BY city", testSchema())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	refRows, _, refErr := Execute(context.Background(), plan, Contract{}, sources)
	if refErr != nil {
		t.Fatalf("reference failed group-cap case: %v", refErr)
	}
	if len(refRows) != 65537 {
		t.Fatalf("reference rows = %d, want 65537", len(refRows))
	}
	ex, err := NewWasmBatchExecutor(wasmBytes, nil)
	if err != nil {
		t.Fatalf("NewWasmBatchExecutor: %v", err)
	}
	defer ex.Close()
	wasmRows, _, wasmErr := ex.ExecutePlan(context.Background(), plan, Contract{}, sources, nil)
	if wasmErr == nil {
		t.Fatalf("wasm accepted 65537 groups (cap not enforced)")
	}
	if !errors.Is(wasmErr, ErrResourceLimit) {
		t.Fatalf("wasm group-cap error %v is not ErrResourceLimit", wasmErr)
	}
	if len(wasmRows) != 0 {
		t.Fatalf("wasm returned %d rows on cap error", len(wasmRows))
	}
}

// State-buffer exhaustion fails loudly with an out-of-memory
// error where Go (unbounded in-memory sort, no budget) succeeds:
// the documented wasm limit, not a silent cut. The fixture uses
// 800-byte names so 150 projected sort rows (~125 KiB) cannot
// fit a 70 KiB state buffer.
func TestWasmBatchStateExhaustionFailsLoudly(t *testing.T) {
	wasmBytes, err := os.ReadFile("../build/yowza.wasm")
	if err != nil {
		t.Skipf("wasm binary absent: %v", err)
	}
	schema := testSchema()
	b := bindingFor(t, schema, "users")
	longName := strings.Repeat("n", 800)
	var rows []Row
	for i := 0; i < 150; i++ {
		r, err := MaterializeRow(b, fmt.Sprintf("user:%d", i),
			[]byte(fmt.Sprintf(`{"name":%q,"age":30,"city":"Tehran","active":true,"score":1.0}`, longName)))
		if err != nil {
			t.Fatalf("materialize %d: %v", i, err)
		}
		rows = append(rows, r)
	}
	sources := map[string]Source{"users": NewMemorySource(rows)}
	plan, err := Compile("SELECT name FROM users ORDER BY name", testSchema())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	_, _, refErr := Execute(context.Background(), plan, Contract{}, sources)
	if refErr != nil {
		t.Fatalf("reference failed: %v", refErr)
	}
	ex, err := NewWasmBatchExecutor(wasmBytes, &WasmBatchOptions{StateCap: 70 << 10})
	if err != nil {
		t.Fatalf("NewWasmBatchExecutor: %v", err)
	}
	defer ex.Close()
	wasmRows, _, wasmErr := ex.ExecutePlan(context.Background(), plan, Contract{}, sources, nil)
	if wasmErr == nil {
		t.Fatalf("wasm succeeded with a 70 KiB sort state over 150 wide rows")
	}
	if len(wasmRows) != 0 {
		t.Fatalf("wasm returned %d rows on state exhaustion", len(wasmRows))
	}
}

// batchRowsEq compares two row sets without touching
// *testing.T, so worker goroutines in the concurrency test can
// use it (testing helpers must not run off the test goroutine).
func batchRowsEq(a, b []Row) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if len(a[i].Cols) != len(b[i].Cols) || len(a[i].Vals) != len(b[i].Vals) {
			return false
		}
		for j := range a[i].Cols {
			if a[i].Cols[j] != b[i].Cols[j] || !valuesEqual(a[i].Vals[j], b[i].Vals[j]) {
				return false
			}
		}
	}
	return true
}

// serializes executions on a single module instance, and
// operator state is re-initialized per execution. Separate
// executors must be fully independent. This test exercises
// both shapes in parallel (it is the test the -race run
// should judge the executor's thread-safety by — the full
// differential suite under -race is dominated by wazero
// module compilation, not by executor concurrency).
func TestWasmBatchConcurrentExecutors(t *testing.T) {
	wasmBytes, err := os.ReadFile("../build/yowza.wasm")
	if err != nil {
		t.Skipf("wasm binary absent: %v", err)
	}
	sql := "SELECT users.name, orders.total FROM users JOIN orders ON users.key = orders.user_id WHERE orders.total > 40 ORDER BY total DESC"
	plan, err := Compile(sql, testSchema())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	sources := map[string]Source{"users": memUsers(t), "orders": memOrders(t)}
	refRows, _, refErr := Execute(context.Background(), plan, Contract{}, sources)
	if refErr != nil {
		t.Fatalf("reference: %v", refErr)
	}
	// Separate executors in parallel: fully independent.
	t.Run("separate", func(t *testing.T) {
		t.Parallel()
		var wg sync.WaitGroup
		errs := make(chan error, 8)
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ex, err := NewWasmBatchExecutor(wasmBytes, nil)
				if err != nil {
					errs <- err
					return
				}
				defer ex.Close()
				for j := 0; j < 3; j++ {
					rows, _, err := ex.ExecutePlan(context.Background(), plan, Contract{}, sources, nil)
					if err != nil {
						errs <- err
						return
					}
					if !batchRowsEq(refRows, rows) {
						errs <- fmt.Errorf("rows differ")
						return
					}
				}
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatal(err)
		}
	})
	// One shared executor in parallel: the mutex serializes
	// executions; every caller still gets correct rows.
	t.Run("shared", func(t *testing.T) {
		t.Parallel()
		ex, err := NewWasmBatchExecutor(wasmBytes, nil)
		if err != nil {
			t.Fatalf("NewWasmBatchExecutor: %v", err)
		}
		defer ex.Close()
		var wg sync.WaitGroup
		errs := make(chan error, 4)
		for i := 0; i < 4; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				rows, _, err := ex.ExecutePlan(context.Background(), plan, Contract{}, sources, nil)
				if err != nil {
					errs <- err
					return
				}
				if !batchRowsEq(refRows, rows) {
					errs <- fmt.Errorf("rows differ")
					return
				}
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatal(err)
		}
	})
}
