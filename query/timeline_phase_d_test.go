package query

// Phase D tests: TimelineSource (Go engine over a
// timeline handle / snapshot handle) and the ISA
// timeline handle (single-shard C engine under a
// pinned snapshot + lease). Differential reference
// for every case is the Go engine over the expected
// merged view, or the ISA engine's own fresh-snapshot
// run for the no-handle negative.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"testing"

	yowza "github.com/yowza/yowza"
)

func phaseDTimelineDB(t *testing.T, bin string, shards int) *yowza.PersistedSegDB {
	t.Helper()
	wasmBytes, err := os.ReadFile(bin)
	if err != nil {
		t.Skipf("wasm binary %s absent: %v", bin, err)
	}
	dir := t.TempDir()
	p, err := yowza.OpenPersistedSegDB(dir, wasmBytes, yowza.PersistedOptions{NumShards: shards, Durability: yowza.DurabilityNoSync})
	if err != nil {
		t.Fatalf("OpenPersistedSegDB: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

func phaseDTimelinePut(t *testing.T, p *yowza.PersistedSegDB, k, v string) {
	t.Helper()
	if err := p.Put(k, []byte(v)); err != nil {
		t.Fatalf("Put %s: %v", k, err)
	}
}

// rowKeyName extracts (key, name) pairs from a users
// query result (projection key, name, in that order).
func phaseDRowKeyNames(t *testing.T, rows []Row) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, r := range rows {
		if len(r.Vals) < 2 {
			t.Fatalf("row %+v has < 2 values", r)
		}
		if r.Vals[0].Kind != KindString || r.Vals[1].Kind != KindString {
			t.Fatalf("row %+v values are not strings", r)
		}
		k, n := r.Vals[0].S, r.Vals[1].S
		out[k] = n
	}
	return out
}

// TimelineSource: merged view (staged + old, never a
// mix with main), main-advance invisibility across
// flush + compact, budget as a typed error, foreign
// vector refusal, and the no-handle negative.
func TestPhaseDTimelineSource(t *testing.T) {
	for _, bin := range []string{"../build/yowza.wasm", "../build/yowza64.wasm"} {
		t.Run(bin, func(t *testing.T) {
			p := phaseDTimelineDB(t, bin, 2)
			for i := 0; i < 20; i++ {
				phaseDTimelinePut(t, p, fmt.Sprintf("user:%d", i),
					fmt.Sprintf(`{"name":"User%02d","age":%d,"city":"Kermanshah","active":true,"score":7.5}`, i, 20+i))
			}
			if err := p.Flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}
			h, err := yowza.NewTimelineHandle(p)
			if err != nil {
				t.Fatalf("handle: %v", err)
			}
			defer h.Close()
			// Staged timeline writes (merged state).
			if err := h.Put("user:99", []byte(`{"name":"Staged","age":31,"city":"Tehran","active":true,"score":9.0}`)); err != nil {
				t.Fatalf("handle put: %v", err)
			}
			if err := h.Delete("user:3"); err != nil {
				t.Fatalf("handle delete: %v", err)
			}
			src, err := NewTimelineSourceFromTimelineHandle(h)
			if err != nil {
				t.Fatalf("source: %v", err)
			}
			// Snapshot-handle source, captured BEFORE
			// the main advance below (it must pin the
			// old vector, not the new main vector).
			snap, err := p.AcquireSnapshot()
			if err != nil {
				t.Fatalf("acquire snapshot: %v", err)
			}
			defer snap.Close()
			snapSrc, err := NewTimelineSourceFromSnapshot(snap)
			if err != nil {
				t.Fatalf("snapshot source: %v", err)
			}
			sql := "SELECT key, name FROM users ORDER BY key"
			run := func(s Source) ([]Row, error) {
				plan, err := Compile(sql, testSchema())
				if err != nil {
					t.Fatalf("compile: %v", err)
				}
				rows, _, err := Execute(context.Background(), plan, Contract{}, map[string]Source{"users": s})
				return rows, err
			}
			want := map[string]string{}
			for i := 0; i < 20; i++ {
				if i == 3 {
					continue
				}
				want[fmt.Sprintf("user:%d", i)] = fmt.Sprintf("User%02d", i)
			}
			want["user:99"] = "Staged"
			rowsBefore, err := run(src)
			if err != nil {
				t.Fatalf("timeline query before advance: %v", err)
			}
			got := phaseDRowKeyNames(t, rowsBefore)
			if len(got) != len(want) {
				t.Fatalf("timeline rows %d, want %d (%v)", len(got), len(want), got)
			}
			for k, v := range want {
				if got[k] != v {
					t.Fatalf("timeline row %s = %q, want %q", k, got[k], v)
				}
			}
			// Advance main + flush + compact: the
			// timeline query must be identical after.
			phaseDTimelinePut(t, p, "user:0", `{"name":"MainNew","age":99,"city":"Tehran","active":true,"score":1.0}`)
			phaseDTimelinePut(t, p, "user:100", `{"name":"MainOnly","age":40,"city":"Shiraz","active":true,"score":5.0}`)
			if err := p.Flush(); err != nil {
				t.Fatalf("flush2: %v", err)
			}
			if err := p.Compact(0); err != nil {
				t.Fatalf("compact: %v", err)
			}
			rowsAfter, err := run(src)
			if err != nil {
				t.Fatalf("timeline query after advance: %v", err)
			}
			gotAfter := phaseDRowKeyNames(t, rowsAfter)
			if len(gotAfter) != len(got) {
				t.Fatalf("timeline rows after advance %d, want %d", len(gotAfter), len(got))
			}
			for k, v := range got {
				if gotAfter[k] != v {
					t.Fatalf("timeline row %s changed %q -> %q after main advance", k, v, gotAfter[k])
				}
			}
			// No-handle negative: the plain persisted
			// source reads main (new name, main-only
			// user, deleted user back, staged user gone).
			mainSrc := &PersistedMultiShardSource{DB: p}
			mainRows, err := run(mainSrc)
			if err != nil {
				t.Fatalf("main query: %v", err)
			}
			mainGot := phaseDRowKeyNames(t, mainRows)
			if mainGot["user:0"] != "MainNew" || mainGot["user:100"] != "MainOnly" {
				t.Fatalf("main query missing main writes: %v", mainGot)
			}
			if _, ok := mainGot["user:99"]; ok {
				t.Fatalf("main query sees timeline-staged user:99")
			}
			if _, ok := mainGot["user:3"]; !ok {
				t.Fatalf("main query missing user:3 (handle delete must not touch main)")
			}
			// Foreign vector refusal.
			curVec, _, err := p.SnapshotVector()
			if err != nil {
				t.Fatalf("snapshot vector: %v", err)
			}
			if _, err := src.ScanAtVector(context.Background(), "user:", curVec); !errors.Is(err, ErrTimelineSourceVectorMismatch) {
				t.Fatalf("foreign vector err %v, want ErrTimelineSourceVectorMismatch", err)
			}
			// Budget: typed error, not truncation.
			src.Budget = 5
			if _, err := run(src); !errors.Is(err, ErrTimelineQueryBudget) {
				t.Fatalf("budget query err %v, want ErrTimelineQueryBudget", err)
			}
			src.Budget = 0
			// Snapshot-handle source: the same base
			// snapshot WITHOUT staged writes.
			snapRows, err := run(snapSrc)
			if err != nil {
				t.Fatalf("snapshot query: %v", err)
			}
			snapGot := phaseDRowKeyNames(t, snapRows)
			if len(snapGot) != 20 {
				t.Fatalf("snapshot rows %d, want 20", len(snapGot))
			}
			if snapGot["user:0"] != "User00" {
				t.Fatalf("snapshot row user:0 = %q, want User00", snapGot["user:0"])
			}
			if _, ok := snapGot["user:99"]; ok {
				t.Fatalf("snapshot source sees staged user:99")
			}
		})
	}
}

// Large timeline query + spill: 800 users through the
// engine with a small memory budget (sort spills, does
// not truncate), plus a JOIN whose build side spills.
func TestPhaseDTimelineSourceLargeAndSpill(t *testing.T) {
	bin := "../build/yowza.wasm"
	p := phaseDTimelineDB(t, bin, 4)
	for i := 0; i < 800; i++ {
		phaseDTimelinePut(t, p, fmt.Sprintf("user:%d", i),
			fmt.Sprintf(`{"name":"User%04d","age":%d,"city":"Kermanshah","active":true,"score":7.5}`, i, 20+i%40))
	}
	for i := 0; i < 200; i++ {
		phaseDTimelinePut(t, p, fmt.Sprintf("order:%d", i),
			fmt.Sprintf(`{"user_id":"user:%d","total":%d.0,"item":"item%d"}`, i%800, 10+i, i))
	}
	if err := p.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	h, err := yowza.NewTimelineHandle(p)
	if err != nil {
		t.Fatalf("handle: %v", err)
	}
	defer h.Close()
	// Staged delete of a user that owns orders: the
	// JOIN must drop those orders (merged state).
	if err := h.Delete("user:7"); err != nil {
		t.Fatalf("handle delete: %v", err)
	}
	src, err := NewTimelineSourceFromTimelineHandle(h)
	if err != nil {
		t.Fatalf("source: %v", err)
	}
	plan, err := Compile("SELECT key, name FROM users ORDER BY name", testSchema())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	rows, trace, err := Execute(context.Background(), plan, Contract{MaxMemoryBytes: 8000}, map[string]Source{"users": src})
	if err != nil {
		t.Fatalf("large timeline query: %v", err)
	}
	if len(rows) != 799 {
		t.Fatalf("large rows %d, want 799 (800 minus staged delete)", len(rows))
	}
	if !trace.Spilled {
		t.Fatalf("large sorted query with 8 KiB budget did not spill (trace.Spilled false)")
	}
	names := make([]string, 0, len(rows))
	for _, r := range rows {
		names = append(names, r.Vals[1].S)
	}
	if !sort.StringsAreSorted(names) {
		t.Fatalf("large rows are not globally sorted by name")
	}
	// JOIN under the same handle: 200 orders, but the
	// order(s) for user:7 (order:7 only, since i%800)
	// must not match.
	joinPlan, err := Compile("SELECT users.name, orders.item FROM users JOIN orders ON users.key = orders.user_id", testSchema())
	if err != nil {
		t.Fatalf("compile join: %v", err)
	}
	joinRows, _, err := Execute(context.Background(), joinPlan, Contract{MaxMemoryBytes: 8000}, map[string]Source{"users": src, "orders": src})
	if err != nil {
		t.Fatalf("timeline join: %v", err)
	}
	if len(joinRows) != 199 {
		t.Fatalf("timeline join rows %d, want 199 (200 orders minus user:7's order)", len(joinRows))
	}
}

// ISA timeline handle: pinned snapshot + lease,
// main-advance invisibility, expired/future refusal,
// resource-limit as a typed error, and the no-handle
// negative.
func TestPhaseDISATimelineHandle(t *testing.T) {
	for _, bin := range []string{"../build/yowza.wasm", "../build/yowza64.wasm"} {
		t.Run(bin, func(t *testing.T) {
			host := gate4Host(t, bin, smallUsers(), smallOrders())
			handle, err := host.AcquireISATimelineHandle()
			if err != nil {
				t.Fatalf("acquire isa timeline handle: %v", err)
			}
			plan, err := Compile("SELECT name FROM users ORDER BY name", testSchema())
			if err != nil {
				t.Fatalf("compile: %v", err)
			}
			prog, err := CompileISA(plan, Contract{})
			if err != nil {
				t.Fatalf("compile isa: %v", err)
			}
			rows1, _, err := host.ExecuteISAAtTimeline(prog, plan, handle, nil)
			if err != nil {
				t.Fatalf("isa timeline execute: %v", err)
			}
			// Advance main: a new user + a rename.
			if err := host.Put("user:99", []byte(`{"name":"Zed","age":31,"city":"Tehran","active":true,"score":9.9}`)); err != nil {
				t.Fatalf("put: %v", err)
			}
			if err := host.Put("user:1", []byte(`{"name":"AliceNew","age":30,"city":"Tehran","active":true,"score":9.5}`)); err != nil {
				t.Fatalf("put rename: %v", err)
			}
			if err := host.Flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}
			rows2, _, err := host.ExecuteISAAtTimeline(prog, plan, handle, nil)
			if err != nil {
				t.Fatalf("isa timeline execute after advance: %v", err)
			}
			if !rowsEqual(t, rows1, rows2) {
				t.Fatalf("isa timeline rows changed after main advance: %v -> %v", rows1, rows2)
			}
			// No-handle run sees main (Zed present).
			rowsMain, _, err := host.ExecuteISA(prog, plan)
			if err != nil {
				t.Fatalf("isa fresh execute: %v", err)
			}
			foundZed := false
			for _, r := range rowsMain {
				if len(r.Vals) > 0 && r.Vals[0].Kind == KindString && r.Vals[0].S == "Zed" {
					foundZed = true
				}
			}
			if !foundZed {
				t.Fatalf("fresh isa run does not see main user Zed: %v", rowsMain)
			}
			// Resource limit: a JOIN under a hard
			// 1-row join cap fails typed, not truncated.
			joinPlan, err := Compile("SELECT users.name, orders.item FROM users JOIN orders ON users.key = orders.user_id ORDER BY item", testSchema())
			if err != nil {
				t.Fatalf("compile join: %v", err)
			}
			joinProg, err := CompileISA(joinPlan, Contract{})
			if err != nil {
				t.Fatalf("compile join isa: %v", err)
			}
			pol := DefaultResourcePolicy()
			pol.MaxJoinRows = 1
			pol.HardCaps = true
			if _, _, err := host.ExecuteISAAtTimeline(joinProg, joinPlan, handle, &pol); !errors.Is(err, ErrResourceLimit) {
				t.Fatalf("join cap err %v, want ErrResourceLimit", err)
			}
			// Expired lease refuses.
			if err := host.ReleaseISATimelineHandle(handle); err != nil {
				t.Fatalf("release: %v", err)
			}
			if _, _, err := host.ExecuteISAAtTimeline(prog, plan, handle, nil); !errors.Is(err, yowza.ErrLeaseExpired) {
				t.Fatalf("expired handle err %v, want ErrLeaseExpired", err)
			}
			// Future snapshot refuses.
			cur, err := host.Snapshot()
			if err != nil {
				t.Fatalf("snapshot: %v", err)
			}
			future := ISATimelineHandle{Snapshot: cur + 1000, LeaseID: 0}
			if _, _, err := host.ExecuteISAAtTimeline(prog, plan, future, nil); !errors.Is(err, yowza.ErrSnapshotInvalid) {
				t.Fatalf("future handle err %v, want ErrSnapshotInvalid", err)
			}
		})
	}
}
