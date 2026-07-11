package query

import (
	"context"
	"testing"

	yowza "github.com/yowza/yowza"
)

// Integration: queries through SegDBSource must see the layered read
// path — a key flushed to a segment, then deleted and flushed again,
// must NOT resurface in query results (segment tombstone wins).
func TestSegDBSourceQuery(t *testing.T) {
	db := loadTestDB(t)
	sdb, err := yowza.NewSegDB(db)
	if err != nil {
		t.Fatalf("NewSegDB: %v", err)
	}
	puts := []struct{ k, v string }{
		{"user:1", `{"name":"Alice","age":30,"city":"Tehran","active":true,"score":9.5}`},
		{"user:2", `{"name":"Bob","age":25,"city":"Shiraz","active":false,"score":7.0}`},
		{"user:3", `{"name":"Carol","age":40,"city":"Tehran","active":true,"score":8.0}`},
	}
	for _, p := range puts {
		if err := sdb.Put(p.k, []byte(p.v)); err != nil {
			t.Fatalf("Put %s: %v", p.k, err)
		}
	}
	if err := sdb.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	src := &SegDBSource{DB: sdb}
	plan, err := Compile("SELECT name FROM users ORDER BY name", testSchema())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	run := func() []string {
		rows, _, err := Execute(context.Background(), plan, Contract{}, map[string]Source{"users": src})
		if err != nil {
			t.Fatalf("execute: %v", err)
		}
		var names []string
		for _, r := range rows {
			n, _ := r.Get("name")
			names = append(names, n.S)
		}
		return names
	}
	if got := run(); len(got) != 3 {
		t.Fatalf("before delete rows = %v, want 3", got)
	}
	// Delete a segment-resident key, flush the tombstone, and query again.
	if err := sdb.Delete("user:2"); err != nil {
		t.Fatalf("Delete user:2: %v", err)
	}
	if err := sdb.Flush(); err != nil {
		t.Fatalf("Flush tombstone: %v", err)
	}
	if got := run(); len(got) != 2 || got[0] != "Alice" || got[1] != "Carol" {
		t.Fatalf("after delete rows = %v, want [Alice Carol] (Bob must not resurface)", got)
	}
}
