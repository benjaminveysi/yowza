package query

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
)

// Gate 4 §5b (policy) and §4.1 (host-spill fallback) tests,
// plus the required negative tests: exhausted spill quota
// and expired/invalid snapshots.

func TestGate4PolicyMonolithicCaps(t *testing.T) {
	bin := "../build/yowza.wasm"
	ua, _ := gate4UsersAB()
	host := gate4Host(t, bin, ua, nil)
	snap, err := host.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	// Sort cap: 6 rows sorted under MaxSortRows=2 must
	// fail with the typed resource limit, not truncate.
	plan, _ := Compile("SELECT name, age FROM users ORDER BY age", testSchema())
	prog, _ := CompileISA(plan, Contract{})
	pol := DefaultResourcePolicy()
	pol.MaxSortRows = 2
	if _, _, err := host.ExecuteISAAtWithPolicy(prog, plan, snap, &pol); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("sort cap error = %v, want ErrResourceLimit", err)
	}
	// Width cap: the users table has 6 columns, so a
	// width-5 policy refuses at parse time.
	polW := DefaultResourcePolicy()
	polW.MaxWidth = 5
	if _, _, err := host.ExecuteISAAtWithPolicy(prog, plan, snap, &polW); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("width cap error = %v, want ErrResourceLimit", err)
	}
	// Default policy succeeds on the same program.
	rows, _, err := host.ExecuteISAAtWithPolicy(prog, plan, snap, nil)
	if err != nil {
		t.Fatalf("default policy execute: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("default policy rows = %d, want 3", len(rows))
	}
	// A policy equal to the defaults does not need the
	// policy export (it runs through execute_at).
	def := DefaultResourcePolicy()
	if _, _, err := host.ExecuteISAAtWithPolicy(prog, plan, snap, &def); err != nil {
		t.Fatalf("default-value policy execute: %v", err)
	}
	// Invalid policy (negative) refuses before execution.
	bad := DefaultResourcePolicy()
	bad.MaxGroups = -1
	if _, _, err := host.ExecuteISAAtWithPolicy(prog, plan, snap, &bad); err == nil {
		t.Fatalf("negative policy must fail")
	}
}

func TestGate4NegativeSpillQuota(t *testing.T) {
	bin := "../build/yowza.wasm"
	var ua, ub []isaKV
	for i := 0; i < 10; i++ {
		kv := isaKV{key: fmt.Sprintf("user:%02d", i),
			val: fmt.Sprintf(`{"name":"U%02d","age":%d,"city":"C","active":true,"score":1.0}`, i, i)}
		if i < 5 {
			ua = append(ua, kv)
		} else {
			ub = append(ub, kv)
		}
	}
	h1 := gate4Host(t, bin, ua, nil)
	h2 := gate4Host(t, bin, ub, nil)
	pol := DefaultResourcePolicy()
	pol.MaxSortRows = 2      // forces spill sort
	pol.SpillQuotaBytes = 10 // far below one spill row
	coord := gate4Coord(t, []*ISAHost{h1, h2}, ModeReject, pol)
	plan, _ := Compile("SELECT name, age FROM users ORDER BY age", testSchema())
	prog, _ := CompileISA(plan, Contract{})
	rows, tr, err := coord.Execute(context.Background(), prog, plan, gate4Snapshots(t, []*ISAHost{h1, h2}))
	if err == nil {
		t.Fatalf("exhausted spill quota must fail, got %d rows", len(rows))
	}
	if !errors.Is(err, ErrSpillQuotaExceeded) {
		t.Fatalf("spill quota error = %v, want ErrSpillQuotaExceeded", err)
	}
	if rows != nil {
		t.Fatalf("rows on quota failure = %v, want nil (no partial result)", rows)
	}
	_ = tr
}

func TestGate4NegativeInvalidSnapshot(t *testing.T) {
	bin := "../build/yowza.wasm"
	ua, _ := gate4UsersAB()
	host := gate4Host(t, bin, ua, nil)
	// Snapshot 0 reads at literal sequence 0: no rows by
	// contract, not an error — pin the contract, don't
	// mistake it for a failure mode.
	plan, _ := Compile("SELECT name FROM users", testSchema())
	prog, _ := CompileISA(plan, Contract{})
	rows, _, err := host.ExecuteISAAt(prog, plan, 0)
	if err != nil {
		t.Fatalf("snapshot 0 execute: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("snapshot 0 rows = %d, want 0 (literal sequence 0)", len(rows))
	}
	// An expired session snapshot is the invalid-snapshot
	// negative test for the coordinator path (see
	// TestGate4MultiShardSession for the session case);
	// here, a nil session must refuse.
	coord := gate4Coord(t, []*ISAHost{host}, ModeReject, DefaultResourcePolicy())
	if _, _, err := coord.ExecuteWithSession(context.Background(), prog, plan, nil, []string{"shard0"}); err == nil {
		t.Fatalf("nil session must fail")
	}
}

// TestGate4HostSpillFallback proves the fallback mechanism
// at small scale: the monolithic engine, run under a
// tight sort policy (MaxSortRows=10 over 30 rows), fails
// with the typed resource limit, and the host-spill
// fallback (same policy, spill sort in chunks of 10)
// returns exactly the Go reference rows with a trace
// recording the fallback and the spill. A single-host
// >32 MiB monolithic working set is NOT constructible
// through ISAHost (its heap caps a host at ~4 MB and its
// WAL at 64 KiB per flush cycle), so the >32 MiB proof
// itself is TestGate4CoordinatorOver32MiB below, over
// many hosts; this test proves the fallback path it
// would take on a resource failure.
func TestGate4HostSpillFallback(t *testing.T) {
	bin := "../build/yowza.wasm"
	var entries []isaKV
	for i := 0; i < 30; i++ {
		entries = append(entries, isaKV{key: fmt.Sprintf("user:%02d", i),
			val: fmt.Sprintf(`{"name":"U%02d","age":%d,"city":"C","active":true,"score":1.0}`, i, (i*7)%30)})
	}
	host := gate4Host(t, bin, entries, nil)
	snap, err := host.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	sql := "SELECT name, age FROM users ORDER BY age"
	plan, err := Compile(sql, testSchema())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	prog, err := CompileISA(plan, Contract{})
	if err != nil {
		t.Fatalf("CompileISA: %v", err)
	}
	pol := DefaultResourcePolicy()
	pol.MaxSortRows = 10
	if _, _, err := host.ExecuteISAAtWithPolicy(prog, plan, snap, &pol); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("tight-policy monolithic error = %v, want ErrResourceLimit", err)
	}
	rows, tr, err := ExecuteISAWithHostSpill(context.Background(), host, prog, plan, snap, pol)
	if err != nil {
		t.Fatalf("host-spill fallback: %v", err)
	}
	if !tr.FallbackUsed {
		t.Fatalf("fallback trace FallbackUsed = false, want true")
	}
	if !tr.Spilled || tr.SpillRuns == 0 {
		t.Fatalf("fallback trace spilled=%v runs=%d, want spill recorded", tr.Spilled, tr.SpillRuns)
	}
	gate4RowsEqual(t, rows, gate4Ref(t, sql, [][]isaKV{entries}, nil))
	// Non-resource errors must NOT trigger the fallback:
	// a mistyped JSON value (age as a string) is a type
	// error in the monolithic engine and must stay an
	// error, not a fallback success.
	badHost := gate4Host(t, bin, []isaKV{{key: "user:1", val: `{"name":"Bad","age":"thirty","city":"C","active":true,"score":1.0}`}}, nil)
	badSnap, err := badHost.Snapshot()
	if err != nil {
		t.Fatalf("bad Snapshot: %v", err)
	}
	if _, _, err := ExecuteISAWithHostSpill(context.Background(), badHost, prog, plan, badSnap, DefaultResourcePolicy()); err == nil {
		t.Fatalf("type error must not fall back to success")
	} else if IsISAResourceError(err) {
		t.Fatalf("type error misclassified as resource error: %v", err)
	}
}

// TestGate4CoordinatorOver32MiB is the Gate 4.1 key test:
// a coordinator SORT over 12 hosts x 3000 rows of
// ~960-byte values — a working set above 32 MiB (asserted
// on input bytes and on the trace's measured peak) —
// must be equal in rows and semantics to the Go
// reference, with the trace recording peak memory and
// spill occurrence. Per-host data stays under the ISA
// host's heap/WAL/segment limits (flush every 50 puts =
// 60 segments per host, under the 128-source cap).
// wasm32 only: the data load dominates the runtime.
func TestGate4CoordinatorOver32MiB(t *testing.T) {
	bin := "../build/yowza.wasm"
	wasmBytes, err := os.ReadFile(bin)
	if err != nil {
		t.Skipf("wasm binary %s absent: %v", bin, err)
	}
	const hostsN, perHost = 12, 3000
	longName := strings.Repeat("N", 900)
	var hosts []*ISAHost
	var shardEntries [][]isaKV
	var totalBytes int64
	for h := 0; h < hostsN; h++ {
		host, err := NewISAHost(wasmBytes)
		if err != nil {
			t.Fatalf("NewISAHost %d: %v", h, err)
		}
		defer host.Close()
		var entries []isaKV
		for i := 0; i < perHost; i++ {
			global := h*perHost + i
			key := fmt.Sprintf("user:%05d", global)
			val := fmt.Sprintf(`{"name":%q,"age":%d,"city":"C","active":true,"score":1.0}`, longName+fmt.Sprintf("%05d", global), (global*37)%100)
			if err := host.Put(key, []byte(val)); err != nil {
				t.Fatalf("host %d Put %d: %v", h, i, err)
			}
			entries = append(entries, isaKV{key: key, val: val})
			totalBytes += int64(len(val))
			if (i+1)%50 == 0 {
				if err := host.Flush(); err != nil {
					t.Fatalf("host %d Flush at %d: %v", h, i, err)
				}
			}
		}
		if err := host.Flush(); err != nil {
			t.Fatalf("host %d final Flush: %v", h, err)
		}
		hosts = append(hosts, host)
		shardEntries = append(shardEntries, entries)
	}
	if totalBytes <= 32<<20 {
		t.Fatalf("test fixture working set = %d bytes, want > 32 MiB", totalBytes)
	}
	coord := gate4Coord(t, hosts, ModeReject, func() ResourcePolicy {
		p := DefaultResourcePolicy()
		p.MaxSortRows = 10000 // forces external merge sort
		return p
	}())
	sql := "SELECT name, age FROM users ORDER BY age"
	rows, tr := gate4RunCoord(t, coord, sql, gate4Snapshots(t, hosts))
	if !tr.Spilled || tr.SpillRuns == 0 {
		t.Fatalf("trace spilled=%v runs=%d, want spill recorded", tr.Spilled, tr.SpillRuns)
	}
	if tr.PeakMemoryBytes <= 32<<20 {
		t.Fatalf("trace peak = %d bytes, want > 32 MiB working set", tr.PeakMemoryBytes)
	}
	gate4RowsEqual(t, rows, gate4Ref(t, sql, shardEntries, nil))
}

// TestGate4HostCompactSmall checks the ISAHost.Compact
// wrapper on a small load (merging a few segments must
// keep every row readable). Large-load compaction can
// fail when the 4 MiB heap cannot hold both the old and
// merged segments at once; that limit is documented in
// GATE4_NOTES.md and is not exercised here.
func TestGate4HostCompactSmall(t *testing.T) {
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
	for i := 0; i < 200; i++ {
		if err := host.Put(fmt.Sprintf("user:%03d", i), []byte(fmt.Sprintf(`{"name":"U%03d","age":%d,"city":"C","active":true,"score":1.0}`, i, i))); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
		if (i+1)%50 == 0 {
			if err := host.Flush(); err != nil {
				t.Fatalf("Flush at %d: %v", i, err)
			}
		}
	}
	snap, err := host.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if err := host.Compact(snap); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	plan, _ := Compile("SELECT name FROM users", testSchema())
	prog, _ := CompileISA(plan, Contract{})
	rows, _, err := host.ExecuteISAAt(prog, plan, snap)
	if err != nil {
		t.Fatalf("Execute after Compact: %v", err)
	}
	if len(rows) != 200 {
		t.Fatalf("rows after Compact = %d, want 200", len(rows))
	}
}
