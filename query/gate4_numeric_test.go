package query

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"testing"
)

// Gate 4 §5a: numeric precision and overflow. These tests pin
// the exact boundaries, not just the happy path.

func TestGate4CompareExactIntFloat(t *testing.T) {
	half := int64(1) << 53 // 9007199254740992
	cases := []struct {
		name string
		a, b Value
		want int
	}{
		{"int 2^53+1 vs int 2^53+2", Int(half + 1), Int(half + 2), -1},
		{"int 2^53+1 vs float 2^53 (rounded literal)", Int(half + 1), Float(float64(half + 1)), 1},
		{"int 2^53+1 vs float 2^53 exactly", Int(half + 1), Float(float64(half)), 1},
		{"int 5 vs float 5.5", Int(5), Float(5.5), -1},
		{"int -3 vs float -2.5", Int(-3), Float(-2.5), -1},
		{"int 0 vs float -0.0", Int(0), Float(math.Copysign(0, -1)), 0},
		{"int MaxInt64 vs float 2^63", Int(math.MaxInt64), Float(9223372036854775808.0), -1},
		{"int MinInt64 vs float -2^63", Int(math.MinInt64), Float(-9223372036854775808.0), 0},
		{"float 1.5 vs int 1", Float(1.5), Int(1), 1},
		{"float 1.5 vs int 2", Float(1.5), Int(2), -1},
		{"int/int still exact", Int(-7), Int(-7), 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Compare(tc.a, tc.b)
			if err != nil {
				t.Fatalf("Compare: %v", err)
			}
			if got != tc.want {
				t.Fatalf("Compare(%v, %v) = %d, want %d", tc.a, tc.b, got, tc.want)
			}
		})
	}
}

func TestGate4Int64FloatExactBoundaries(t *testing.T) {
	if _, ok := Int64ToFloatExact(int64(1) << 53); !ok {
		t.Fatalf("2^53 should convert exactly")
	}
	if _, ok := Int64ToFloatExact((int64(1) << 53) + 1); ok {
		t.Fatalf("2^53+1 must NOT convert exactly")
	}
	if v, ok := Float64ToIntExact(9007199254740992.0); !ok || v != 9007199254740992 {
		t.Fatalf("Float64ToIntExact(2^53) = %d, %v", v, ok)
	}
	if _, ok := Float64ToIntExact(1.5); ok {
		t.Fatalf("1.5 must not convert to int exactly")
	}
	if _, ok := Float64ToIntExact(9223372036854775808.0); ok {
		t.Fatalf("2^63 must not convert to int64")
	}
}

func TestGate4ParseJSONIntExact(t *testing.T) {
	cases := []struct {
		tok  string
		want int64
		ok   bool
	}{
		{"9007199254740993", 9007199254740993, true},
		{"9007199254740993.0", 9007199254740993, true},
		{"1e3", 1000, true},
		{"12300e-2", 123, true},
		{"1.5", 0, false},
		{"9223372036854775808", 0, false},
		{"-9223372036854775808", math.MinInt64, true},
		{"0", 0, true},
		{"-0", 0, true},
		{"12a", 0, false},
		{"", 0, false},
	}
	for _, tc := range cases {
		got, ok := ParseJSONIntExact(tc.tok)
		if ok != tc.ok || (ok && got != tc.want) {
			t.Fatalf("ParseJSONIntExact(%q) = %d, %v, want %d, %v", tc.tok, got, ok, tc.want, tc.ok)
		}
	}
}

func TestGate4CheckedIntOverflow(t *testing.T) {
	if _, err := AddIntChecked(math.MaxInt64, 1); err == nil {
		t.Fatalf("MaxInt64+1 must overflow")
	}
	if _, err := AddIntChecked(math.MinInt64, -1); err == nil {
		t.Fatalf("MinInt64-1 must overflow")
	}
	if v, err := AddIntChecked(math.MaxInt64, 0); err != nil || v != math.MaxInt64 {
		t.Fatalf("MaxInt64+0 = %d, %v", v, err)
	}
	if _, err := MulIntChecked(math.MaxInt64, 2); err == nil {
		t.Fatalf("MaxInt64*2 must overflow")
	}
	// Coordinator accumulator: SUM overflow is typed, not wrapped.
	acc := &coordAcc{agg: "SUM"}
	if err := acc.add(Int(math.MaxInt64), false); err != nil {
		t.Fatalf("first add: %v", err)
	}
	if err := acc.add(Int(1), false); err == nil {
		t.Fatalf("SUM overflow must return an error, got wrapped value")
	} else if err != ErrNumericOverflow {
		t.Fatalf("SUM overflow error = %v, want ErrNumericOverflow", err)
	}
}

// TestGate4JSONIntBoundaryDivergence pins the one numeric
// boundary Gate 4 does NOT fix: JSON numbers beyond 15
// significant digits are parsed differently by the two
// engines. Go converts via correctly-rounded float64, so
// 9007199254740993 becomes 9007199254740992; the C
// engine's decimal parser accumulates only 15 digits
// exactly and folds the rest into the exponent, yielding
// 9007199254740990 for the same token. Below 15
// significant digits both are exact and agree; above it
// they can differ in the last digits. This test pins the
// observed pair so the divergence can never pass silently
// as agreement — fixing the C parser to be correctly
// rounded is the remaining work (see GATE4_NOTES.md §5a).
func TestGate4JSONIntBoundaryDivergence(t *testing.T) {
	bin := "../build/yowza.wasm"
	wasmBytes, err := os.ReadFile(bin)
	if err != nil {
		t.Skipf("wasm binary %s absent: %v", bin, err)
	}
	host, err := NewISAHost(wasmBytes)
	if err != nil {
		t.Fatalf("NewISAHost: %v", err)
	}
	defer host.Close()
	// Go ingestion: MaterializeRow via the reference path.
	schema := testSchema()
	b := bindingFor(t, schema, "users")
	row, err := MaterializeRow(b, "user:1", []byte(`{"name":"Big","age":9007199254740993,"city":"X","active":true,"score":1.0}`))
	if err != nil {
		t.Fatalf("MaterializeRow: %v", err)
	}
	var goAge int64 = -1
	for i, c := range row.Cols {
		if c == "users.age" {
			goAge = row.Vals[i].I
		}
	}
	if goAge != 9007199254740992 {
		t.Fatalf("Go JSON int 9007199254740993 materialized as %d, want correctly-rounded 9007199254740992", goAge)
	}
	// ISA ingestion: same value through the C engine.
	if err := host.Put("user:1", []byte(`{"name":"Big","age":9007199254740993,"city":"X","active":true,"score":1.0}`)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := host.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	plan, err := Compile("SELECT age FROM users", testSchema())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	prog, err := CompileISA(plan, Contract{})
	if err != nil {
		t.Fatalf("CompileISA: %v", err)
	}
	rows, _, err := host.ExecuteISATrace(prog, plan)
	if err != nil {
		t.Fatalf("ExecuteISA: %v", err)
	}
	// C engine: 15-digit parse of the same token.
	if len(rows) != 1 || rows[0].Vals[0].I != 9007199254740990 {
		t.Fatalf("ISA JSON int = %v, want [9007199254740990] (C 15-digit parse; Go gives 9007199254740992)", rows)
	}
}

// TestGate4ExactLiteralFilter proves the Compare fix end to
// end: a literal 2^53+1 must NOT match a JSON-rounded 2^53
// column value in either engine (the old float64 routing
// matched them in both).
func TestGate4ExactLiteralFilter(t *testing.T) {
	bin := "../build/yowza.wasm"
	wasmBytes, err := os.ReadFile(bin)
	if err != nil {
		t.Skipf("wasm binary %s absent: %v", bin, err)
	}
	host, err := NewISAHost(wasmBytes)
	if err != nil {
		t.Fatalf("NewISAHost: %v", err)
	}
	defer host.Close()
	entries := []isaKV{{key: "user:1", val: `{"name":"Big","age":9007199254740993,"city":"X","active":true,"score":1.0}`}}
	loadISAHostFixture(t, host, entries, nil)
	if err := host.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	sql := "SELECT key FROM users WHERE age = 9007199254740993"
	plan, err := Compile(sql, testSchema())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	// Reference engine over the same visible data.
	schema := testSchema()
	sources := map[string]Source{
		"users":  &matSource{b: bindingFor(t, schema, "users"), entries: visible(entries)},
		"orders": &matSource{b: bindingFor(t, schema, "orders"), entries: nil},
	}
	refRows, _, refErr := Execute(context.Background(), plan, Contract{}, sources)
	if refErr != nil {
		t.Fatalf("reference execute: %v", refErr)
	}
	if len(refRows) != 0 {
		t.Fatalf("reference matched %d rows, want 0 (exact literal must not match rounded column)", len(refRows))
	}
	prog, err := CompileISA(plan, Contract{})
	if err != nil {
		t.Fatalf("CompileISA: %v", err)
	}
	isaRows, _, err := host.ExecuteISATrace(prog, plan)
	if err != nil {
		t.Fatalf("ExecuteISA: %v", err)
	}
	if len(isaRows) != 0 {
		t.Fatalf("ISA matched %d rows, want 0", len(isaRows))
	}
}

// TestGate4SumOverflowDivergence pins the §5a SUM divergence
// exactly as observed (both engines verified 2026-10-25):
//
// Case A — two rows of 2^62 (true sum 2^63, unrepresentable
// in int64): Go wraps silently to MinInt64; the ISA engine
// parses each JSON int through its 15-significant-digit
// rule to 4611686018427379712 (true 2^62 minus 8192) and
// sums exactly to 9223372036854759424 — a plausible,
// wrong, successful result. Neither engine reports
// overflow here; the divergence is the parse, not the add.
//
// Case B — 10000 rows of 999999999999999 (exact in both
// parsers; true sum 9999999999999990000 > MaxInt64): the
// ISA engine returns the typed ErrNumericOverflow with no
// rows (checked SUM, rc=-8); Go wraps silently to
// -8446744073709561616. This is the program-level proof
// that the C checked-add path is reachable and typed.
func TestGate4SumOverflowDivergence(t *testing.T) {
	bin := "../build/yowza.wasm"
	// Case A.
	entriesA := []isaKV{
		{key: "user:1", val: `{"name":"A","age":4611686018427387904,"city":"C","active":true,"score":1.0}`},
		{key: "user:2", val: `{"name":"B","age":4611686018427387904,"city":"C","active":true,"score":1.0}`},
	}
	hostA := gate4Host(t, bin, entriesA, nil)
	snapA, err := hostA.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot A: %v", err)
	}
	plan, err := Compile("SELECT SUM(age) FROM users", testSchema())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	prog, err := CompileISA(plan, Contract{})
	if err != nil {
		t.Fatalf("CompileISA: %v", err)
	}
	isaRows, _, err := hostA.ExecuteISAAt(prog, plan, snapA)
	if err != nil {
		t.Fatalf("ISA case A: %v", err)
	}
	if len(isaRows) != 1 || isaRows[0].Vals[0].I != 9223372036854759424 {
		t.Fatalf("ISA case A rows = %v, want SUM 9223372036854759424", isaRows)
	}
	goRowsA := gate4Ref(t, "SELECT SUM(age) FROM users", [][]isaKV{entriesA}, nil)
	if len(goRowsA) != 1 || goRowsA[0].Vals[0].I != -9223372036854775808 {
		t.Fatalf("Go case A rows = %v, want SUM MinInt64 (silent wrap)", goRowsA)
	}
	// Case B. Loaded directly (not via gate4Host, which
	// only flushes on the fixture flag): 10000 puts need a
	// flush every 500 rows or the 64 KiB host WAL fills.
	var entriesB []isaKV
	wasmBytes, err := os.ReadFile(bin)
	if err != nil {
		t.Skipf("wasm binary %s absent: %v", bin, err)
	}
	hostB, err := NewISAHost(wasmBytes)
	if err != nil {
		t.Fatalf("NewISAHost B: %v", err)
	}
	t.Cleanup(func() { _ = hostB.Close() })
	for i := 0; i < 10000; i++ {
		kv := isaKV{key: fmt.Sprintf("user:%05d", i),
			val: `{"name":"A","age":999999999999999,"city":"C","active":true,"score":1.0}`}
		if err := hostB.Put(kv.key, []byte(kv.val)); err != nil {
			t.Fatalf("Put B %d: %v", i, err)
		}
		entriesB = append(entriesB, kv)
		if (i+1)%500 == 0 {
			if err := hostB.Flush(); err != nil {
				t.Fatalf("Flush B at %d: %v", i, err)
			}
		}
	}
	if err := hostB.Flush(); err != nil {
		t.Fatalf("final Flush B: %v", err)
	}
	snapB, err := hostB.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot B: %v", err)
	}
	isaRowsB, _, err := hostB.ExecuteISAAt(prog, plan, snapB)
	if !errors.Is(err, ErrNumericOverflow) {
		t.Fatalf("ISA case B err = %v (rows %v), want ErrNumericOverflow", err, isaRowsB)
	}
	if len(isaRowsB) != 0 {
		t.Fatalf("ISA case B rows = %v, want no rows on overflow", isaRowsB)
	}
	goRowsB := gate4Ref(t, "SELECT SUM(age) FROM users", [][]isaKV{entriesB}, nil)
	if len(goRowsB) != 1 || goRowsB[0].Vals[0].I != -8446744073709561616 {
		t.Fatalf("Go case B rows = %v, want SUM -8446744073709561616 (silent wrap)", goRowsB)
	}
}
