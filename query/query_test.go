package query

import (
	"context"
	"strings"
	"testing"
)

func testSchema() *Schema {
	return &Schema{Tables: []Table{
		{Name: "users", Prefix: "user:", Columns: []Column{
			{Name: "key", Type: KindString, Source: "key"},
			{Name: "name", Type: KindString, Source: "json:name"},
			{Name: "age", Type: KindInt, Source: "json:age"},
			{Name: "city", Type: KindString, Source: "json:city"},
			{Name: "active", Type: KindBool, Source: "json:active"},
			{Name: "score", Type: KindFloat, Source: "json:score"},
		}},
		{Name: "orders", Prefix: "order:", Columns: []Column{
			{Name: "key", Type: KindString, Source: "key"},
			{Name: "user_id", Type: KindString, Source: "json:user_id"},
			{Name: "total", Type: KindFloat, Source: "json:total"},
			{Name: "item", Type: KindString, Source: "json:item"},
		}},
	}}
}

func bindingFor(t *testing.T, schema *Schema, tableName string) Binding {
	t.Helper()
	tbl, ok := schema.TableByName(tableName)
	if !ok {
		t.Fatalf("schema has no table %q", tableName)
	}
	return Binding{Table: tbl.Name, Alias: tbl.Name, Prefix: tbl.Prefix, Columns: tbl.Columns}
}

func memUsers(t *testing.T) *MemorySource {
	t.Helper()
	schema := testSchema()
	b := bindingFor(t, schema, "users")
	entries := []struct{ key, val string }{
		{"user:1", `{"name":"Alice","age":30,"city":"Tehran","active":true,"score":9.5}`},
		{"user:2", `{"name":"Bob","age":25,"city":"Shiraz","active":false,"score":7.0}`},
		{"user:3", `{"name":"Carol","age":40,"city":"Tehran","active":true,"score":8.0}`},
		{"user:4", `{"name":"Dan","age":22,"active":true,"score":6.5}`}, // city missing => NULL
		{"user:5", `{"name":"Eve","age":35,"city":"Tabriz","active":false,"score":9.0}`},
	}
	var rows []Row
	for _, e := range entries {
		r, err := MaterializeRow(b, e.key, []byte(e.val))
		if err != nil {
			t.Fatalf("materialize %s: %v", e.key, err)
		}
		rows = append(rows, r)
	}
	return NewMemorySource(rows)
}

func memOrders(t *testing.T) *MemorySource {
	t.Helper()
	schema := testSchema()
	b := bindingFor(t, schema, "orders")
	entries := []struct{ key, val string }{
		{"order:1", `{"user_id":"user:1","total":120.0,"item":"book"}`},
		{"order:2", `{"user_id":"user:1","total":45.5,"item":"pen"}`},
		{"order:3", `{"user_id":"user:3","total":300.0,"item":"laptop"}`},
	}
	var rows []Row
	for _, e := range entries {
		r, err := MaterializeRow(b, e.key, []byte(e.val))
		if err != nil {
			t.Fatalf("materialize %s: %v", e.key, err)
		}
		rows = append(rows, r)
	}
	return NewMemorySource(rows)
}

func runQuery(t *testing.T, sql string, sources map[string]Source) ([]Row, *Trace) {
	t.Helper()
	plan, err := Compile(sql, testSchema())
	if err != nil {
		t.Fatalf("compile %q: %v", sql, err)
	}
	rows, trace, err := Execute(context.Background(), plan, Contract{}, sources)
	if err != nil {
		t.Fatalf("execute %q: %v (trace: %+v)", sql, err, trace)
	}
	return rows, trace
}

func rowStrings(t *testing.T, rows []Row) []string {
	t.Helper()
	var out []string
	for _, r := range rows {
		out = append(out, r.String())
	}
	return out
}

func TestCompileBasic(t *testing.T) {
	plan, err := Compile("SELECT name, age FROM users WHERE age >= 21 AND city = 'Tehran' ORDER BY age DESC LIMIT 10", testSchema())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if plan.Version != PlanVersion {
		t.Fatalf("version = %d, want %d", plan.Version, PlanVersion)
	}
	if len(plan.Projection) != 2 {
		t.Fatalf("projection len = %d, want 2", len(plan.Projection))
	}
	if plan.Projection[0].OutputName != "name" || plan.Projection[1].OutputName != "age" {
		t.Fatalf("projection = %+v", plan.Projection)
	}
	if plan.Filter == nil || plan.Filter.Kind != "and" {
		t.Fatalf("filter should be AND, got %+v", plan.Filter)
	}
	if !plan.HasLimit || plan.Limit == nil || *plan.Limit != 10 {
		t.Fatalf("limit not bound: %+v", plan)
	}
	wantOps := []string{"SCAN", "FILTER", "PROJECT", "SORT", "LIMIT"}
	if strings.Join(plan.Operators, ",") != strings.Join(wantOps, ",") {
		t.Fatalf("operators = %v, want %v", plan.Operators, wantOps)
	}
	if len(plan.OutputColumns) != 2 || plan.OutputColumns[1].Type != KindInt {
		t.Fatalf("output columns = %+v", plan.OutputColumns)
	}
}

func TestCompileStarAndAggregates(t *testing.T) {
	plan, err := Compile("SELECT city, COUNT(*), AVG(age) FROM users GROUP BY city", testSchema())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if len(plan.Projection) != 3 {
		t.Fatalf("projection len = %d, want 3", len(plan.Projection))
	}
	if plan.Projection[1].Agg != "COUNT" || plan.Projection[2].Agg != "AVG" || plan.Projection[2].OutputType != KindFloat {
		t.Fatalf("aggregates = %+v", plan.Projection)
	}
	star, err := Compile("SELECT * FROM users", testSchema())
	if err != nil {
		t.Fatalf("compile star: %v", err)
	}
	if len(star.Projection) != 6 {
		t.Fatalf("star projection len = %d, want 6", len(star.Projection))
	}
}

func TestCompileJoin(t *testing.T) {
	plan, err := Compile("SELECT users.name, orders.total FROM users JOIN orders ON users.key = orders.user_id WHERE orders.total > 100", testSchema())
	if err != nil {
		t.Fatalf("compile join: %v", err)
	}
	if plan.Join == nil || plan.Join.RightTable != "orders" {
		t.Fatalf("join not bound: %+v", plan.Join)
	}
	if plan.Join.LeftCol.Qualified != "users.key" || plan.Join.RightCol.Qualified != "orders.user_id" {
		t.Fatalf("join keys = %v / %v", plan.Join.LeftCol, plan.Join.RightCol)
	}
}

func TestBindErrors(t *testing.T) {
	cases := []struct {
		sql  string
		code string
	}{
		{"SELECT name FROM nosuch", "UNKNOWN_TABLE"},
		{"SELECT nosuch FROM users", "UNKNOWN_COLUMN"},
		{"SELECT key FROM users JOIN orders ON users.key = orders.user_id", "AMBIGUOUS_COLUMN"},
		{"SELECT name FROM users WHERE age = 'old'", "TYPE_MISMATCH"},
		{"SELECT name FROM users WHERE age < 'old'", "TYPE_MISMATCH"},
		{"SELECT name, COUNT(*) FROM users", "AGG_MISUSE"},
		{"SELECT SUM(name) FROM users", "TYPE_MISMATCH"},
		{"SELECT name FROM users JOIN orders ON users.name = orders.total", "TYPE_MISMATCH"},
		{"SELECT name FROM users JOIN orders ON users.age > orders.total", "BAD_JOIN"},
		{"SELECT name FROM users ORDER BY nosuch", "UNKNOWN_COLUMN"},
	}
	for _, tc := range cases {
		_, err := Compile(tc.sql, testSchema())
		if err == nil {
			t.Fatalf("compile %q: expected error, got nil", tc.sql)
		}
		be, ok := err.(*BindError)
		if !ok {
			t.Fatalf("compile %q: error type %T (%v), want *BindError", tc.sql, err, err)
		}
		if be.Code != tc.code {
			t.Fatalf("compile %q: code = %s, want %s (err: %v)", tc.sql, be.Code, tc.code, err)
		}
	}
}

func TestParseErrors(t *testing.T) {
	cases := []string{
		"INSERT INTO users VALUES (1)",
		"SELECT name FROM users WHERE age IN (SELECT age FROM users)",
		"SELECT name FROM users WHERE",
		"SELECT name FROM users garbage garbage",
		"SELECT name FROM users WHERE age = ",
		"SELECT name FROM users LIMIT -1",
		"SELECT name FROM users WHERE name LIKE 5",
		"DELETE FROM users",
		"",
	}
	for _, sql := range cases {
		if _, err := Compile(sql, testSchema()); err == nil {
			t.Fatalf("compile %q: expected error, got nil", sql)
		}
	}
}

func TestPlanJSONRoundTripAndExplain(t *testing.T) {
	plan, err := Compile("SELECT name, COUNT(*) AS n FROM users WHERE active = TRUE GROUP BY name", testSchema())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	data, err := MarshalPlan(plan)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	plan2, err := UnmarshalPlan(data)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if plan2.Version != PlanVersion || len(plan2.Projection) != 2 {
		t.Fatalf("round-tripped plan = %+v", plan2)
	}
	h1, _ := PlanHash(plan)
	h2, _ := PlanHash(plan2)
	if h1 != h2 {
		t.Fatalf("plan hash changed across round-trip: %s vs %s", h1, h2)
	}
	// A plan restored from JSON must execute identically.
	rows, _, err := Execute(context.Background(), plan2, Contract{}, map[string]Source{"users": memUsers(t)})
	if err != nil {
		t.Fatalf("execute restored plan: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("restored plan rows = %d, want 3 groups (active users only)", len(rows))
	}
	expl := Explain(plan)
	if !strings.Contains(expl, "FROM users") || !strings.Contains(expl, "PIPELINE") {
		t.Fatalf("explain missing parts:\n%s", expl)
	}
}

func TestMaterializeErrors(t *testing.T) {
	schema := testSchema()
	b := bindingFor(t, schema, "users")
	// age present but a string: typed materialization must fail, not
	// silently become NULL.
	if _, err := MaterializeRow(b, "user:9", []byte(`{"name":"Zed","age":"thirty"}`)); err == nil {
		t.Fatalf("expected materialization error for string age, got nil")
	}
	// Missing city => NULL, not an error.
	r, err := MaterializeRow(b, "user:9", []byte(`{"name":"Zed","age":20}`))
	if err != nil {
		t.Fatalf("materialize with missing field: %v", err)
	}
	v, ok := r.Get("users.city")
	if !ok || v.Kind != KindNull {
		t.Fatalf("missing field should be NULL, got %v (ok=%v)", v, ok)
	}
	// Non-JSON value => JSON columns NULL, row still exists.
	r, err = MaterializeRow(b, "user:9", []byte(`not json at all`))
	if err != nil {
		t.Fatalf("materialize non-JSON: %v", err)
	}
	v, _ = r.Get("users.name")
	if v.Kind != KindNull {
		t.Fatalf("non-JSON value should give NULL json columns, got %v", v)
	}
}
