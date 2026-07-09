// High-capacity profile tests (worker Q).
//
// Gates: (1) heavy join/aggregation that fails on wasm32
// completes on wasm64-HighCapacity with Go-identical
// results; (2) over-budget queries fail loudly with no
// partial operator state visible and no host OOM (memory
// stays within budget); (3) wasm32-Standard vs
// wasm64-HighCapacity agree wherever both succeed, and
// the existing TestWasmBatch* suite stays green.
// Plus the fail-before-consume proof: a state-full return
// consumes nothing, and a grow-and-retry appends exactly
// once (no double-add).
package query

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
)

func highcapBin(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile("../build/" + name)
	if err != nil {
		t.Skipf("wasm binary %s absent: %v", name, err)
	}
	return b
}

func TestHighCapacityProfileResolved(t *testing.T) {
	def := (&WasmBatchOptions{}).resolved()
	if def.Profile != BatchCapacityStandard || def.StateCap != 16<<20 || def.WorkCap != 4<<20 || def.OutMax != 64<<20 || def.MaxRows != StandardMaxRows || def.MaxGroups != StandardMaxGroups {
		t.Fatalf("standard defaults changed: %+v", def)
	}
	nilDef := (*WasmBatchOptions)(nil).resolved()
	if nilDef.Profile != BatchCapacityStandard || nilDef.StateCap != 16<<20 {
		t.Fatalf("nil options did not resolve to standard: %+v", nilDef)
	}
	hi := (&WasmBatchOptions{Profile: BatchCapacityHigh}).resolved()
	if hi.Profile != BatchCapacityHigh || hi.StateCap != HighCapacityStateCap || hi.WorkCap != HighCapacityWorkCap || hi.OutMax != HighCapacityOutMax || hi.MaxRows != HighCapacityMaxRows || hi.MaxGroups != HighCapacityMaxGroups {
		t.Fatalf("high-capacity defaults wrong: %+v", hi)
	}
	over := (&WasmBatchOptions{Profile: BatchCapacityHigh, StateCap: 1 << 20, MaxRows: 7, BudgetBytes: 123}).resolved()
	if over.StateCap != 1<<20 || over.MaxRows != 7 || over.BudgetBytes != 123 {
		t.Fatalf("explicit overrides lost: %+v", over)
	}
	if BatchCapacityStandard.String() != "standard" || BatchCapacityHigh.String() != "high-capacity" {
		t.Fatalf("profile names wrong: %v %v", BatchCapacityStandard, BatchCapacityHigh)
	}
}

func TestHighCapacityHostBudget(t *testing.T) {
	dir := t.TempDir()
	meminfo := dir + "/meminfo"
	if err := os.WriteFile(meminfo, []byte("MemTotal:       8000000 kB\nMemFree:        1000000 kB\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	total, err := hostMemoryCap(meminfo)
	if err != nil {
		t.Fatalf("meminfo only: %v", err)
	}
	if total != 8000000*1024 {
		t.Fatalf("meminfo only total = %d, want %d", total, 8000000*1024)
	}
	cg := dir + "/memory.max"
	if err := os.WriteFile(cg, []byte("4000000000\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	total, err = hostMemoryCap(meminfo, cg)
	if err != nil || total != 4000000000 {
		t.Fatalf("cgroup-capped total = %d, err = %v, want 4000000000", total, err)
	}
	if err := os.WriteFile(cg, []byte("max\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	total, err = hostMemoryCap(meminfo, cg)
	if err != nil || total != 8000000*1024 {
		t.Fatalf("cgroup max total = %d, err = %v, want %d", total, err, 8000000*1024)
	}
	v1 := dir + "/limit_in_bytes"
	if err := os.WriteFile(v1, []byte("9223372036854775807\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	total, err = hostMemoryCap(meminfo, v1)
	if err != nil || total != 8000000*1024 {
		t.Fatalf("v1 sentinel total = %d, err = %v, want %d", total, err, 8000000*1024)
	}
	if _, err := hostMemoryCap(dir + "/missing"); err == nil {
		t.Fatalf("missing meminfo accepted")
	}
	live, err := HostMemoryBudget()
	if err != nil {
		t.Fatalf("live host budget: %v", err)
	}
	if live == 0 {
		t.Fatalf("live host budget is 0")
	}
	t.Logf("live host HighCapacity default budget = %d bytes (%.2f GiB)", live, float64(live)/(1<<30))
}

func TestHighCapacityRefusesWasm32(t *testing.T) {
	wasm32 := highcapBin(t, "yowza.wasm")
	_, err := NewWasmBatchExecutor(wasm32, &WasmBatchOptions{Profile: BatchCapacityHigh})
	if err == nil {
		t.Fatalf("high-capacity accepted wasm32 (must refuse loudly)")
	}
	if !strings.Contains(err.Error(), "wasm64") {
		t.Fatalf("refusal does not name wasm64: %v", err)
	}
}

// heavyJoinSources builds a 1-user left side and a
// right side of n orders, all matching user:1, each with
// an 80-character item so one build row costs ~230 bump
// bytes (4 QiVals + ~100 string bytes + chunk share).
func heavyJoinSources(t *testing.T, n int) map[string]Source {
	t.Helper()
	schema := testSchema()
	ub := bindingFor(t, schema, "users")
	ur, err := MaterializeRow(ub, "user:1", []byte(`{"name":"Alice","age":30,"city":"Tehran","active":true,"score":9.5}`))
	if err != nil {
		t.Fatalf("materialize user: %v", err)
	}
	ob := bindingFor(t, schema, "orders")
	item := strings.Repeat("i", 80)
	rows := make([]Row, 0, n)
	for i := 0; i < n; i++ {
		r, err := MaterializeRow(ob, fmt.Sprintf("order:%d", i),
			[]byte(fmt.Sprintf(`{"user_id":"user:1","total":1.5,"item":%q}`, item)))
		if err != nil {
			t.Fatalf("materialize order %d: %v", i, err)
		}
		rows = append(rows, r)
	}
	return map[string]Source{"users": NewMemorySource([]Row{ur}), "orders": NewMemorySource(rows)}
}

// Gate 1 (join): 100,000 build rows (~23 MiB of bump)
// exceed the 16 MiB wasm32 state buffer, so wasm32 fails
// with QISA_ERR_OOM, while wasm64-HighCapacity (starting
// at the same 16 MiB, growing to 32 MiB) completes with
// exactly the Go executor's rows.
func TestHighCapacityHeavyJoinVsGo(t *testing.T) {
	wasm32 := highcapBin(t, "yowza.wasm")
	wasm64 := highcapBin(t, "yowza64.wasm")
	sql := "SELECT users.name, orders.item FROM users JOIN orders ON users.key = orders.user_id"
	plan, err := Compile(sql, testSchema())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	sources := heavyJoinSources(t, 100000)
	refRows, _, refErr := Execute(context.Background(), plan, Contract{}, sources)
	if refErr != nil {
		t.Fatalf("reference failed: %v", refErr)
	}
	if len(refRows) != 100000 {
		t.Fatalf("reference rows = %d, want 100000", len(refRows))
	}
	ex32, err := NewWasmBatchExecutor(wasm32, nil)
	if err != nil {
		t.Fatalf("wasm32 executor: %v", err)
	}
	defer ex32.Close()
	rows32, _, err32 := ex32.ExecutePlan(context.Background(), plan, Contract{}, sources, nil)
	if err32 == nil {
		t.Fatalf("wasm32 succeeded over a 16 MiB state buffer")
	}
	if !strings.Contains(err32.Error(), "out of memory") {
		t.Fatalf("wasm32 error %v is not the OOM error", err32)
	}
	if len(rows32) != 0 {
		t.Fatalf("wasm32 returned %d rows on OOM", len(rows32))
	}
	ex64, err := NewWasmBatchExecutor(wasm64, &WasmBatchOptions{Profile: BatchCapacityHigh, StateCap: 16 << 20, BudgetBytes: 1 << 30})
	if err != nil {
		t.Fatalf("wasm64 high-capacity executor: %v", err)
	}
	defer ex64.Close()
	rows64, _, err64 := ex64.ExecutePlan(context.Background(), plan, Contract{}, sources, nil)
	if err64 != nil {
		t.Fatalf("wasm64 high-capacity failed: %v", err64)
	}
	if !batchRowsEq(refRows, rows64) {
		t.Fatalf("wasm64 high-capacity rows differ from Go (counts %d vs %d)", len(refRows), len(rows64))
	}
	if ex64.joinStateCap <= 16<<20 {
		t.Fatalf("wasm64 high-capacity never grew its join state (cap %d)", ex64.joinStateCap)
	}
	t.Logf("heavy join: wasm32 OOM as expected; wasm64-HighCapacity grew join state to %d bytes, %d rows identical to Go", ex64.joinStateCap, len(rows64))
}

// Gate 1 (aggregation): 80,000 distinct groups exceed
// the 65,536 compile-time group cap (wasm32 LIMIT) and
// complete under the HighCapacity runtime cap of 262,144
// with Go-identical rows.
func TestHighCapacityHeavyAggVsGo(t *testing.T) {
	wasm32 := highcapBin(t, "yowza.wasm")
	wasm64 := highcapBin(t, "yowza64.wasm")
	schema := testSchema()
	b := bindingFor(t, schema, "users")
	rows := make([]Row, 0, 80000)
	for i := 0; i < 80000; i++ {
		r, err := MaterializeRow(b, fmt.Sprintf("user:%d", i),
			[]byte(fmt.Sprintf(`{"name":"U%d","age":30,"city":"C%06d","active":true,"score":1.0}`, i, i)))
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
		t.Fatalf("reference failed: %v", refErr)
	}
	if len(refRows) != 80000 {
		t.Fatalf("reference rows = %d, want 80000", len(refRows))
	}
	ex32, err := NewWasmBatchExecutor(wasm32, nil)
	if err != nil {
		t.Fatalf("wasm32 executor: %v", err)
	}
	defer ex32.Close()
	rows32, _, err32 := ex32.ExecutePlan(context.Background(), plan, Contract{}, sources, nil)
	if err32 == nil {
		t.Fatalf("wasm32 accepted 80000 groups over the 65536 cap")
	}
	if !errors.Is(err32, ErrResourceLimit) {
		t.Fatalf("wasm32 group-cap error %v is not ErrResourceLimit", err32)
	}
	if len(rows32) != 0 {
		t.Fatalf("wasm32 returned %d rows on cap error", len(rows32))
	}
	ex64, err := NewWasmBatchExecutor(wasm64, &WasmBatchOptions{Profile: BatchCapacityHigh, BudgetBytes: 1 << 30})
	if err != nil {
		t.Fatalf("wasm64 high-capacity executor: %v", err)
	}
	defer ex64.Close()
	rows64, _, err64 := ex64.ExecutePlan(context.Background(), plan, Contract{}, sources, nil)
	if err64 != nil {
		t.Fatalf("wasm64 high-capacity failed: %v", err64)
	}
	if !batchRowsEq(refRows, rows64) {
		t.Fatalf("wasm64 high-capacity agg rows differ from Go (counts %d vs %d)", len(refRows), len(rows64))
	}
	t.Logf("heavy agg: wasm32 LIMIT as expected; wasm64-HighCapacity %d groups identical to Go", len(rows64))
}

// readStateHeader reads nrows/retained/spilled from a
// live operator state region (fixed header offsets).
func readStateHeader(t *testing.T, ex *WasmBatchExecutor, off uint64) (nrows uint32, retained uint64, spilled uint32) {
	t.Helper()
	b, ok := ex.memRead(off+12, 4)
	if !ok {
		t.Fatalf("read nrows header")
	}
	nrows = uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
	b, ok = ex.memRead(off+24, 8)
	if !ok {
		t.Fatalf("read retained header")
	}
	for i := 0; i < 8; i++ {
		retained |= uint64(b[i]) << (8 * i)
	}
	b, ok = ex.memRead(off+40, 4)
	if !ok {
		t.Fatalf("read spilled header")
	}
	spilled = uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
	return nrows, retained, spilled
}

// Fail-before-consume proof (b)(1) + gate 2's "no partial
// operator state visible": drive join_build directly at a
// 256 KiB state cap. The batch that overflows must return
// QISA_ERR_OOM with header fields and bump usage exactly
// unchanged, must fail identically on an immediate retry
// without growth, and must succeed exactly once after
// growState — proven by comparing the final probe output
// against a fresh large-cap run (a double-add would
// duplicate the retried batch's matches).
func TestHighCapacityStateFullRetryNoDoubleAdd(t *testing.T) {
	wasm64 := highcapBin(t, "yowza64.wasm")
	rightCols := []string{"r.key", "r.val"}
	build := func(ex *WasmBatchExecutor, rows []Row) (int32, error) {
		enc, err := encodeQBatch(rows, 2)
		if err != nil {
			return 0, err
		}
		if err := ex.writeIn(enc); err != nil {
			return 0, err
		}
		if err := ex.writeParams(encodeU32s(nameLens(rightCols))); err != nil {
			return 0, err
		}
		return ex.call(ex.fnJoinBuild, ex.joinStateOff, ex.joinStateCap, ex.inOff, uint64(len(enc)), ex.paramsOff)
	}
	probe := func(ex *WasmBatchExecutor, keys []int) []Row {
		var left []Row
		for _, k := range keys {
			left = append(left, Row{Cols: []string{"l.key"}, Vals: []Value{Int(int64(k))}})
		}
		enc, err := encodeQBatch(left, 1)
		if err != nil {
			t.Fatalf("encode probe: %v", err)
		}
		raw, err := ex.callOut(func() (int32, error) {
			return ex.callStateful(stateJoin, func() (int32, error) {
				if err := ex.writeIn(enc); err != nil {
					return 0, err
				}
				return ex.call(ex.fnJoinProbe, ex.joinStateOff, ex.joinStateCap, ex.inOff, uint64(len(enc)), ex.outOff, ex.outCap, ex.cellsOff, ex.workOff, ex.opts.WorkCap)
			})
		})
		if err != nil {
			t.Fatalf("probe: %v", err)
		}
		rows, err := decodeQBatch(raw, 3, []string{"l.key", "r.key", "r.val"})
		if err != nil {
			t.Fatalf("decode probe: %v", err)
		}
		return rows
	}
	mkRows := func(from, count int) []Row {
		rows := make([]Row, 0, count)
		for i := 0; i < count; i++ {
			rows = append(rows, Row{Cols: rightCols, Vals: []Value{Int(int64(from + i)), String(strings.Repeat("p", 400))}})
		}
		return rows
	}
	const totalRows = 600
	const batchSize = 64
	ex, err := NewWasmBatchExecutor(wasm64, &WasmBatchOptions{Profile: BatchCapacityHigh, StateCap: 256 << 10, BudgetBytes: 512 << 20})
	if err != nil {
		t.Fatalf("executor: %v", err)
	}
	defer ex.Close()
	rc, err := ex.call(ex.fnJoinInit2, ex.joinStateOff, ex.joinStateCap, 1, 0, 2, 0, 0, 1048576)
	if err != nil || rc != 0 {
		t.Fatalf("join_init2 rc=%d err=%v", rc, err)
	}
	oomBatch := -1
	var oomNeed uint64
	built := 0
	for start := 0; start < totalRows; start += batchSize {
		n := batchSize
		if start+n > totalRows {
			n = totalRows - start
		}
		rows := mkRows(start, n)
		nrowsBefore, retBefore, spillBefore := readStateHeader(t, ex, ex.joinStateOff)
		usedBefore, _, err := ex.stateInfo(stateJoin)
		if err != nil {
			t.Fatalf("state_info before: %v", err)
		}
		rc, err = build(ex, rows)
		if err != nil {
			t.Fatalf("build batch %d: %v", start, err)
		}
		if rc == -5 {
			if oomBatch >= 0 {
				t.Fatalf("second unexpected OOM at batch %d (first at %d)", start, oomBatch)
			}
			oomBatch = start
			nrowsAfter, retAfter, spillAfter := readStateHeader(t, ex, ex.joinStateOff)
			usedAfter, need, err := ex.stateInfo(stateJoin)
			if err != nil {
				t.Fatalf("state_info after OOM: %v", err)
			}
			if nrowsAfter != nrowsBefore || retAfter != retBefore || spillAfter != spillBefore || usedAfter != usedBefore {
				t.Fatalf("OOM consumed state: header (%d,%d,%d)->(%d,%d,%d) bump %d->%d",
					nrowsBefore, retBefore, spillBefore, nrowsAfter, retAfter, spillAfter, usedBefore, usedAfter)
			}
			if need == 0 {
				t.Fatalf("OOM reported need 0 (not a capacity failure?)")
			}
			oomNeed = need
			// Immediate retry without growth: same OOM,
			// still nothing consumed.
			rc2, err := build(ex, rows)
			if err != nil {
				t.Fatalf("retry build: %v", err)
			}
			if rc2 != -5 {
				t.Fatalf("retry without growth rc=%d, want -5", rc2)
			}
			nrowsAfter2, _, _ := readStateHeader(t, ex, ex.joinStateOff)
			if nrowsAfter2 != nrowsBefore {
				t.Fatalf("retry without growth changed nrows %d->%d", nrowsBefore, nrowsAfter2)
			}
			// Grow and retry: exactly one append.
			if err := ex.growState(stateJoin, oomNeed); err != nil {
				t.Fatalf("growState: %v", err)
			}
			rc, err = build(ex, rows)
			if err != nil {
				t.Fatalf("build after growth: %v", err)
			}
			if rc != 0 {
				t.Fatalf("build after growth rc=%d, want 0", rc)
			}
		} else if rc != 0 {
			t.Fatalf("build batch %d rc=%d", start, rc)
		}
		built += n
	}
	if oomBatch < 0 {
		t.Fatalf("no batch hit state-full at a 256 KiB cap (test would prove nothing)")
	}
	if built != totalRows {
		t.Fatalf("built %d rows, want %d", built, totalRows)
	}
	// Probe keys spanning pre-OOM, OOM-batch, and post-OOM rows.
	probeKeys := []int{0, 1, oomBatch, oomBatch + 1, oomBatch + batchSize - 1, totalRows - 1}
	got := probe(ex, probeKeys)
	// Fresh large-cap run over the same batches, no growth.
	exBig, err := NewWasmBatchExecutor(wasm64, &WasmBatchOptions{StateCap: 16 << 20})
	if err != nil {
		t.Fatalf("large-cap executor: %v", err)
	}
	defer exBig.Close()
	rc, err = exBig.call(exBig.fnJoinInit, exBig.joinStateOff, exBig.joinStateCap, 1, 0, 2, 0, 0)
	if err != nil || rc != 0 {
		t.Fatalf("large-cap join_init rc=%d err=%v", rc, err)
	}
	for start := 0; start < totalRows; start += batchSize {
		n := batchSize
		if start+n > totalRows {
			n = totalRows - start
		}
		rows := mkRows(start, n)
		enc, err := encodeQBatch(rows, 2)
		if err != nil {
			t.Fatalf("encode big: %v", err)
		}
		if err := exBig.writeIn(enc); err != nil {
			t.Fatalf("writeIn big: %v", err)
		}
		if err := exBig.writeParams(encodeU32s(nameLens(rightCols))); err != nil {
			t.Fatalf("writeParams big: %v", err)
		}
		rc, err = exBig.call(exBig.fnJoinBuild, exBig.joinStateOff, exBig.joinStateCap, exBig.inOff, uint64(len(enc)), exBig.paramsOff)
		if err != nil || rc != 0 {
			t.Fatalf("large-cap build rc=%d err=%v", rc, err)
		}
	}
	// Probe the large-cap executor with the same keys, via
	// a local probe (same shape as above, Standard path).
	probeBig := func(keys []int) []Row {
		var left []Row
		for _, k := range keys {
			left = append(left, Row{Cols: []string{"l.key"}, Vals: []Value{Int(int64(k))}})
		}
		enc, err := encodeQBatch(left, 1)
		if err != nil {
			t.Fatalf("encode probe big: %v", err)
		}
		raw, err := exBig.callOut(func() (int32, error) {
			if err := exBig.writeIn(enc); err != nil {
				return 0, err
			}
			return exBig.call(exBig.fnJoinProbe, exBig.joinStateOff, exBig.joinStateCap, exBig.inOff, uint64(len(enc)), exBig.outOff, exBig.outCap, exBig.cellsOff, exBig.workOff, exBig.opts.WorkCap)
		})
		if err != nil {
			t.Fatalf("probe big: %v", err)
		}
		rows, err := decodeQBatch(raw, 3, []string{"l.key", "r.key", "r.val"})
		if err != nil {
			t.Fatalf("decode probe big: %v", err)
		}
		return rows
	}
	want := probeBig(probeKeys)
	if !batchRowsEq(want, got) {
		t.Fatalf("probe after grow-retry differs from large-cap run (double-add?): want %d rows, got %d", len(want), len(got))
	}
	if len(got) != len(probeKeys) {
		t.Fatalf("probe rows = %d, want %d (one match per key)", len(got), len(probeKeys))
	}
	t.Logf("state-full retry: OOM batch at row %d consumed nothing (bump/header identical, need %d); grow+retry appended once; probe identical to large-cap run (%d rows)", oomBatch, oomNeed, len(got))
	// Relocation proof, second phase: the first probe
	// above built the join hash inside the relocated
	// region. Force one more relocation and probe again:
	// the relocated hash arrays (head/next/canon) must
	// still serve identical matches, proving the C fixup
	// adjusts hash pointers as well as vec/string ones.
	if err := ex.growState(stateJoin, ex.joinStateCap*2); err != nil {
		t.Fatalf("second growState (hashed state): %v", err)
	}
	got2 := probe(ex, probeKeys)
	if !batchRowsEq(got, got2) {
		t.Fatalf("probe after relocating a hashed state differs: %d vs %d rows", len(got), len(got2))
	}
	t.Logf("hashed-state relocation: probe identical after second relocation (%d rows)", len(got2))
}

// Aggregate growth + relocation under spill: a
// COUNT(*) aggregation over 2,000 distinct 200-char
// cities, with a 200 KB operator budget (so epoch groups
// spill-merge into the merged set mid-stream) and a
// 512 KiB initial state (so the merged set must be
// relocated mid-stream). The final rows must equal Go's
// exactly: this is the only test that relocates an
// aggregate state containing merged groups.
func TestHighCapacityAggGrowthRelocation(t *testing.T) {
	wasm64 := highcapBin(t, "yowza64.wasm")
	schema := testSchema()
	b := bindingFor(t, schema, "users")
	cityPad := strings.Repeat("c", 190)
	rows := make([]Row, 0, 2000)
	for i := 0; i < 2000; i++ {
		city := fmt.Sprintf("C%06d%s", i, cityPad)
		r, err := MaterializeRow(b, fmt.Sprintf("user:%d", i),
			[]byte(fmt.Sprintf(`{"name":"U%d","age":30,"city":%q,"active":true,"score":1.0}`, i, city)))
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
	contract := Contract{MaxMemoryBytes: 200000}
	refRows, _, refErr := Execute(context.Background(), plan, contract, sources)
	if refErr != nil {
		t.Fatalf("reference failed: %v", refErr)
	}
	if len(refRows) != 2000 {
		t.Fatalf("reference rows = %d, want 2000", len(refRows))
	}
	ex, err := NewWasmBatchExecutor(wasm64, &WasmBatchOptions{Profile: BatchCapacityHigh, StateCap: 512 << 10, BudgetBytes: 1 << 30})
	if err != nil {
		t.Fatalf("executor: %v", err)
	}
	defer ex.Close()
	got, _, err := ex.ExecutePlan(context.Background(), plan, contract, sources, nil)
	if err != nil {
		t.Fatalf("high-capacity agg failed: %v", err)
	}
	if !batchRowsEq(refRows, got) {
		t.Fatalf("agg rows after growth+relocation differ from Go (counts %d vs %d)", len(refRows), len(got))
	}
	if ex.aggStateCap <= 512<<10 {
		t.Fatalf("agg state never grew (cap %d)", ex.aggStateCap)
	}
	t.Logf("agg growth+relocation under spill: state grew to %d bytes, %d groups identical to Go", ex.aggStateCap, len(got))
}

// Gate 2: a join whose build state (~11 MiB) needs more
// growth than the budget allows fails loudly with
// ErrWasmBudgetExceeded, returns no rows, and leaves the
// executor's wasm memory within budget (no host OOM).
func TestHighCapacityOverBudgetFailsLoudly(t *testing.T) {
	wasm64 := highcapBin(t, "yowza64.wasm")
	probeEx, err := NewWasmBatchExecutor(wasm64, &WasmBatchOptions{Profile: BatchCapacityHigh, StateCap: 1 << 20, BudgetBytes: 1 << 30})
	if err != nil {
		t.Fatalf("probe executor: %v", err)
	}
	layoutEnd := probeEx.tailStart
	probeEx.Close()
	budget := layoutEnd + 20<<20
	sql := "SELECT users.name, orders.item FROM users JOIN orders ON users.key = orders.user_id"
	plan, err := Compile(sql, testSchema())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	schema := testSchema()
	ub := bindingFor(t, schema, "users")
	ur, err := MaterializeRow(ub, "user:1", []byte(`{"name":"Alice","age":30,"city":"Tehran","active":true,"score":9.5}`))
	if err != nil {
		t.Fatalf("materialize user: %v", err)
	}
	ob := bindingFor(t, schema, "orders")
	item := strings.Repeat("j", 800)
	rows := make([]Row, 0, 12000)
	for i := 0; i < 12000; i++ {
		r, err := MaterializeRow(ob, fmt.Sprintf("order:%d", i),
			[]byte(fmt.Sprintf(`{"user_id":"user:1","total":1.5,"item":%q}`, item)))
		if err != nil {
			t.Fatalf("materialize order %d: %v", i, err)
		}
		rows = append(rows, r)
	}
	sources := map[string]Source{"users": NewMemorySource([]Row{ur}), "orders": NewMemorySource(rows)}
	ex, err := NewWasmBatchExecutor(wasm64, &WasmBatchOptions{Profile: BatchCapacityHigh, StateCap: 1 << 20, BudgetBytes: budget})
	if err != nil {
		t.Fatalf("executor: %v", err)
	}
	defer ex.Close()
	got, _, err := ex.ExecutePlan(context.Background(), plan, Contract{}, sources, nil)
	if err == nil {
		t.Fatalf("over-budget query succeeded with %d rows", len(got))
	}
	if !errors.Is(err, ErrWasmBudgetExceeded) {
		t.Fatalf("over-budget error %v is not ErrWasmBudgetExceeded", err)
	}
	if len(got) != 0 {
		t.Fatalf("over-budget query returned %d rows", len(got))
	}
	if ex.memSize() > budget {
		t.Fatalf("executor memory %d exceeds budget %d", ex.memSize(), budget)
	}
	t.Logf("over-budget: loud ErrWasmBudgetExceeded; memory %d within budget %d", ex.memSize(), budget)
}

// Gate 3 (query part): the same small queries agree
// row-for-row on wasm32-Standard and wasm64-HighCapacity.
func TestHighCapacityDifferentialStandardVsHigh(t *testing.T) {
	wasm32 := highcapBin(t, "yowza.wasm")
	wasm64 := highcapBin(t, "yowza64.wasm")
	cases := []string{
		"SELECT name, age FROM users",
		"SELECT name FROM users WHERE age >= 30 AND city = 'Tehran'",
		"SELECT users.name, orders.item FROM users JOIN orders ON users.key = orders.user_id",
		"SELECT name, city FROM users ORDER BY city",
		"SELECT city, COUNT(*) FROM users GROUP BY city",
		"SELECT name FROM users LIMIT 2",
	}
	sources := map[string]Source{"users": memUsers(t), "orders": memOrders(t)}
	for _, sql := range cases {
		plan, err := Compile(sql, testSchema())
		if err != nil {
			t.Fatalf("compile %q: %v", sql, err)
		}
		ex32, err := NewWasmBatchExecutor(wasm32, nil)
		if err != nil {
			t.Fatalf("wasm32 executor: %v", err)
		}
		rows32, _, err32 := ex32.ExecutePlan(context.Background(), plan, Contract{}, sources, nil)
		ex32.Close()
		if err32 != nil {
			t.Fatalf("wasm32 %q: %v", sql, err32)
		}
		ex64, err := NewWasmBatchExecutor(wasm64, &WasmBatchOptions{Profile: BatchCapacityHigh, BudgetBytes: 1 << 30})
		if err != nil {
			t.Fatalf("wasm64 executor: %v", err)
		}
		rows64, _, err64 := ex64.ExecutePlan(context.Background(), plan, Contract{}, sources, nil)
		ex64.Close()
		if err64 != nil {
			t.Fatalf("wasm64 %q: %v", sql, err64)
		}
		if !batchRowsEq(rows32, rows64) {
			t.Fatalf("standard vs high-capacity differ for %q: %v vs %v", sql, rowStrings(t, rows32), rowStrings(t, rows64))
		}
	}
}

// Effective-Max probe: instantiate yowza64.wasm and
// Grow64 toward the ceiling (VA only on Linux — the
// fork's mmap growth does not commit pages until they
// are touched, and this probe never touches grown
// pages). It reports the exact page where growth stops
// and requires at least the 16 GiB declared max, so a
// Makefile regression that lowers the max fails loudly.
func TestProbeEffectiveMaxWasm64(t *testing.T) {
	wasmBytes := highcapBin(t, "yowza64.wasm")
	ctx := context.Background()
	features := api.CoreFeaturesV2.SetEnabled(api.CoreFeatureMemory64, true)
	r := wazero.NewRuntimeWithConfig(ctx, wazero.NewRuntimeConfig().WithCoreFeatures(features))
	defer r.Close(ctx)
	if _, err := r.NewHostModuleBuilder("env").Instantiate(ctx); err != nil {
		t.Fatalf("instantiate env: %v", err)
	}
	mod, err := r.Instantiate(ctx, wasmBytes)
	if err != nil {
		t.Fatalf("instantiate wasm64: %v", err)
	}
	mem64 := mod.Memory64()
	if mem64 == nil {
		t.Fatalf("wasm64 module has no Memory64")
	}
	startPages := mem64.Size64() / 65536
	t.Logf("probe: initial memory %d pages", startPages)
	lo := startPages
	step := uint64(1)
	var hi uint64
	for {
		target := lo + step
		var need uint64
		if target*65536 > mem64.Size64() {
			need = (target*65536 - mem64.Size64() + 65535) / 65536
		}
		_, ok := mem64.Grow64(need)
		if !ok {
			hi = target
			break
		}
		got := mem64.Size64() / 65536
		if got < target {
			hi = target
			break
		}
		lo = got
		if lo > (1 << 40) {
			t.Fatalf("probe: grew past 2^40 pages without failing")
		}
		step *= 2
	}
	for hi-lo > 1 {
		mid := lo + (hi-lo)/2
		var need uint64
		if mid*65536 > mem64.Size64() {
			need = (mid*65536 - mem64.Size64() + 65535) / 65536
		}
		_, ok := mem64.Grow64(need)
		if ok && mem64.Size64()/65536 >= mid {
			lo = mem64.Size64() / 65536
		} else {
			hi = mid
		}
	}
	t.Logf("probe: effective Grow64 ceiling = %d pages = %d bytes (%.3f GiB); first failing target = %d pages",
		lo, lo*65536, float64(lo*65536)/(1<<30), hi)
	if lo < 262144 {
		t.Fatalf("effective max %d pages below declared 16 GiB (262144 pages)", lo)
	}
}
