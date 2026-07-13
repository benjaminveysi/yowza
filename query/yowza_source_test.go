package query

import (
	"context"
	"os"
	"testing"

	yowza "github.com/yowza/yowza"
)

// Integration test against a real YowzaDB (wasm). Skipped when the
// built binary is absent, with an explicit message — a skip here is
// a missing fixture, not a passing test, and the notes say so.
func loadTestDB(t *testing.T) *yowza.YowzaDB {
	t.Helper()
	wasmBytes, err := os.ReadFile("../build/yowza.wasm")
	if err != nil {
		t.Skipf("built wasm binary absent (run make all first): %v", err)
	}
	db, err := yowza.NewYowzaDB(wasmBytes)
	if err != nil {
		t.Fatalf("NewYowzaDB: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestYowzaDBSourceQuery(t *testing.T) {
	db := loadTestDB(t)
	puts := []struct{ k, v string }{
		{"user:1", `{"name":"Alice","age":30,"city":"Tehran","active":true,"score":9.5}`},
		{"user:2", `{"name":"Bob","age":25,"city":"Shiraz","active":false,"score":7.0}`},
		{"user:3", `{"name":"Carol","age":40,"city":"Tehran","active":true,"score":8.0}`},
		{"order:1", `{"user_id":"user:1","total":120.0,"item":"book"}`},
		{"order:2", `{"user_id":"user:3","total":300.0,"item":"laptop"}`},
	}
	for _, p := range puts {
		if err := db.Put(p.k, []byte(p.v)); err != nil {
			t.Fatalf("Put %s: %v", p.k, err)
		}
	}
	src := &YowzaDBSource{DB: db}
	plan, err := Compile("SELECT name, age FROM users WHERE city = 'Tehran' ORDER BY age DESC", testSchema())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	rows, trace, err := Execute(context.Background(), plan, Contract{}, map[string]Source{"users": src})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("rows = %d, want 2: %v", len(rows), rows)
	}
	n, _ := rows[0].Get("name")
	if n.S != "Carol" {
		t.Fatalf("first row name = %q, want Carol", n.S)
	}
	if trace.ScannedRows != 3 {
		t.Fatalf("trace scanned = %d, want 3 (all users scanned, 2 pass filter)", trace.ScannedRows)
	}

	// Join through the same database, two adapters, two prefixes.
	plan2, err := Compile("SELECT users.name, orders.total FROM users JOIN orders ON users.key = orders.user_id ORDER BY total", testSchema())
	if err != nil {
		t.Fatalf("compile join: %v", err)
	}
	rows2, _, err := Execute(context.Background(), plan2, Contract{}, map[string]Source{"users": src, "orders": src})
	if err != nil {
		t.Fatalf("execute join: %v", err)
	}
	if len(rows2) != 2 {
		t.Fatalf("join rows = %d, want 2: %v", len(rows2), rows2)
	}
}

func TestYowzaDBSourceSnapshot(t *testing.T) {
	db := loadTestDB(t)
	if err := db.Put("user:1", []byte(`{"name":"Alice","age":30}`)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	src := &YowzaDBSource{DB: db}
	b := bindingFor(t, testSchema(), "users")
	it, err := src.Open(context.Background(), b)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer it.Close()
	// A write after Open must not appear in this iterator.
	if err := db.Put("user:2", []byte(`{"name":"Bob","age":25}`)); err != nil {
		t.Fatalf("Put after Open: %v", err)
	}
	var seen int
	for {
		batch, err := it.NextBatch(10)
		if err != nil {
			t.Fatalf("NextBatch: %v", err)
		}
		if len(batch) == 0 {
			break
		}
		seen += len(batch)
	}
	if seen != 1 {
		t.Fatalf("snapshot iterator saw %d rows, want 1", seen)
	}
}

func TestYowzaDBSourceBadValueFails(t *testing.T) {
	db := loadTestDB(t)
	if err := db.Put("user:9", []byte(`{"name":"Zed","age":"thirty"}`)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	src := &YowzaDBSource{DB: db}
	plan, err := Compile("SELECT name FROM users", testSchema())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	_, trace, err := Execute(context.Background(), plan, Contract{}, map[string]Source{"users": src})
	if err == nil {
		t.Fatalf("execute with mistyped value: expected error, got nil")
	}
	if trace == nil || trace.ExecutionError == "" {
		t.Fatalf("trace should carry execution error, got %+v", trace)
	}
}
