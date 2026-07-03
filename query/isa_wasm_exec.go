// WASM batch executor — Track A (Query ISA + vectorized execution).
//
// The Go batch executor (Execute / ExecuteWithOptions in engine.go)
// is the reference: it plans, verifies, and runs a batch pipeline
// in Go. This file adds the second, explicitly selected executor
// for the same plans: the planner/coordinator stays Go, but every
// operator batch (filter, project, hash-join build/probe, sort,
// group/aggregate) executes in C/WASM through the QBATCH API in
// src/query_isa.c (see include/query_isa.h, QBATCH ADDENDUM).
//
// Selection is explicit, never silent: callers that want Go
// execution use Execute or GoPlanExecutor; callers that want
// WASM operator execution construct a WasmBatchExecutor and call
// its ExecutePlan. Execute itself is untouched and stays Go.
//
// What the WASM executor does and does not do:
//   - It verifies the plan with the existing verifier BEFORE
//     opening any source, exactly as Execute does. A plan that
//     fails verification opens zero sources.
//   - It streams source batches into wasm (batch size from
//     ExecuteOptions, default 64) and never materializes a
//     table in Go beyond one batch plus the final result set
//     (the same retention the reference collector has).
//   - It applies LIMIT after global semantics: on barrier
//     paths (aggregate/sort) the barrier consumes all input
//     before LIMIT truncates; on streaming paths it stops
//     pulling left batches once LIMIT is satisfied, exactly
//     as the reference limitIter does.
//   - It enforces the same scanned-row cap (shared across
//     both join sides) and result-row cap as Execute, and
//     passes Contract.MaxMemoryBytes as the per-operator
//     budget to every stateful batch call. A single batch
//     over budget fails with BUDGET_EXCEEDED; otherwise an
//     operator spills (logical spill inside its wasm state
//     buffer, same accounting as Go — see the header) and
//     reports spilled/peak, which the trace carries.
//   - It never touches storage: wasm receives encoded row
//     batches, never a SegDB pointer, so it cannot re-read
//     "current" state or see post-Open writes. Snapshot
//     discipline is the Source contract (frozen at Open),
//     which the coordinator relies on and never re-opens
//     mid-query. The SegDB literal-snapshot + lease path
//     (ExecuteISAAtTimeline) is unchanged and separate.
//   - One execution at a time per executor (a mutex guards
//     the single wasm module instance, as ISAHost does).
//     Parallel queries need separate executors.
//   - Any case where wasm would differ from Go fails loudly:
//     malformed batches, column-position mismatches, type
//     errors, budget errors, resource-cap errors
//     (QISA_ERR_LIMIT -> ErrResourceLimit), and state
//     buffer exhaustion (QISA_ERR_OOM) all abort the query
//     with no rows. Nothing is approximated.
package query

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
)

// PlanExecutor runs one verified plan over sources. Execute /
// ExecuteWithOptions is the Go implementation; WasmBatchExecutor
// is the WASM-operator implementation. Both take the same plan,
// contract, sources, and options, and return the same rows,
// trace shape, and error classes for every case the WASM batch
// API can execute (see the differential tests).
type PlanExecutor interface {
	ExecutePlan(ctx context.Context, plan *Plan, contract Contract, sources map[string]Source, opts *ExecuteOptions) ([]Row, *Trace, error)
}

// GoPlanExecutor is PlanExecutor over the reference Go pipeline.
type GoPlanExecutor struct{}

// ExecutePlan runs the plan through ExecuteWithOptions.
func (GoPlanExecutor) ExecutePlan(ctx context.Context, plan *Plan, contract Contract, sources map[string]Source, opts *ExecuteOptions) ([]Row, *Trace, error) {
	return ExecuteWithOptions(ctx, plan, contract, sources, opts)
}

// WasmBatchOptions tunes the WASM batch executor. Zero values
// mean defaults: batch size 64, 16 MiB per operator state
// buffer, 4 MiB work buffer, 64 MiB maximum output buffer.
// StateCap bounds one operator's retained + logically spilled
// data; exceeding it fails loudly with an out-of-memory error
// (the documented wasm limit — Go would spill to disk instead).
// Profile selects the capacity profile (see
// highcap_profile.go): Standard is the zero value and keeps
// exactly today's behaviour. HighCapacity (wasm64 only)
// grows state regions under BudgetBytes; BudgetBytes 0
// means the host-aware default (host RAM / 4, cgroup
// capped). MaxRows/MaxGroups are the runtime row/group
// caps passed at operator init (Standard: 262144/65536;
// HighCapacity: 1048576/262144 by default). StateCap is
// the INITIAL state region in both profiles; only
// HighCapacity grows it.
type WasmBatchOptions struct {
	BatchSize   int
	StateCap    uint64
	WorkCap     uint64
	OutMax      uint64
	Profile     BatchCapacity
	BudgetBytes uint64
	MaxRows     uint32
	MaxGroups   uint32
}

func (o *WasmBatchOptions) resolved() WasmBatchOptions {
	r := WasmBatchOptions{BatchSize: DefaultBatchSize, StateCap: 16 << 20, WorkCap: 4 << 20, OutMax: 64 << 20, Profile: BatchCapacityStandard, MaxRows: StandardMaxRows, MaxGroups: StandardMaxGroups}
	if o == nil {
		return r
	}
	if o.Profile == BatchCapacityHigh {
		r.StateCap = HighCapacityStateCap
		r.WorkCap = HighCapacityWorkCap
		r.OutMax = HighCapacityOutMax
		r.MaxRows = HighCapacityMaxRows
		r.MaxGroups = HighCapacityMaxGroups
	}
	r.Profile = o.Profile
	r.BudgetBytes = o.BudgetBytes
	if o.BatchSize > 0 {
		r.BatchSize = o.BatchSize
	}
	if o.StateCap > 0 {
		r.StateCap = o.StateCap
	}
	if o.WorkCap > 0 {
		r.WorkCap = o.WorkCap
	}
	if o.OutMax > 0 {
		r.OutMax = o.OutMax
	}
	if o.MaxRows > 0 {
		r.MaxRows = o.MaxRows
	}
	if o.MaxGroups > 0 {
		r.MaxGroups = o.MaxGroups
	}
	return r
}

// WasmBatchExecutor executes operator batches in C/WASM.
type WasmBatchExecutor struct {
	mu       sync.Mutex
	runtime  wazero.Runtime
	module   api.Module
	memory   api.Memory
	memory64 api.Memory64
	isMem64  bool
	opts     WasmBatchOptions

	// Fixed layout (all offsets are wasm-memory addresses).
	// cells: out_len cell; params: per-call parameter arrays;
	// work: batch-call scratch; join/sort/aggState: operator
	// state buffers (persistent across calls in one execution);
	// in/out: per-call batch buffers (out is last, so growth
	// never overlaps another region).
	cellsOff     uint64
	paramsOff    uint64
	workOff      uint64
	joinStateOff uint64
	sortStateOff uint64
	aggStateOff  uint64
	inOff        uint64
	inCap        uint64
	outOff       uint64
	outCap       uint64

	// High-capacity state: per-operator region caps (they
	// move with their region on growth and persist across
	// executions on this executor, so a grown region is
	// reused, never re-grown from the layout size), the
	// profile/budget, and the tail high-water mark.
	// Layout invariant: tailEnd >= max(inOff+inCap,
	// outOff+outCap) always; grown state regions are
	// appended at tailEnd, and an out region that would
	// grow into the tail zone is moved to tailEnd instead
	// (out is per-call scratch, so moving it is safe).
	// Standard never allocates past tailStart, so these
	// fields leave its layout behaviour untouched.
	profile      BatchCapacity
	budgetBytes  uint64
	maxRows      uint32
	maxGroups    uint32
	joinStateCap uint64
	sortStateCap uint64
	aggStateCap  uint64
	tailStart    uint64
	tailEnd      uint64
	needOff      uint64

	fnVersion   api.Function
	fnFilter    api.Function
	fnProject   api.Function
	fnJoinInit  api.Function
	fnJoinBuild api.Function
	fnJoinProbe api.Function
	fnSortInit  api.Function
	fnSortAdd   api.Function
	fnSortFin   api.Function
	fnAggInit   api.Function
	fnAggAdd    api.Function
	fnAggFin    api.Function

	// High-capacity exports (nil on a binary that predates
	// them; required when Profile is HighCapacity).
	fnJoinInit2     api.Function
	fnSortInit2     api.Function
	fnAggInit2      api.Function
	fnStateRelocate api.Function
	fnStateInfo     api.Function
}

// NewWasmBatchExecutor instantiates the wasm module and binds
// the QBATCH exports. It fails (never silently degrading to Go)
// when the binary predates the batch exports or reports a
// different ISA version. The module needs no SegDB: batch calls
// take encoded rows, not storage state.
func NewWasmBatchExecutor(wasmBytes []byte, opts *WasmBatchOptions) (*WasmBatchExecutor, error) {
	resolved := opts.resolved()
	ctx := context.Background()
	features := api.CoreFeaturesV2.SetEnabled(api.CoreFeatureMemory64, true)
	r := wazero.NewRuntimeWithConfig(ctx, wazero.NewRuntimeConfig().WithCoreFeatures(features))
	if _, err := r.NewHostModuleBuilder("env").Instantiate(ctx); err != nil {
		r.Close(ctx)
		return nil, fmt.Errorf("qbatch host: instantiate env: %v", err)
	}
	mod, err := r.Instantiate(ctx, wasmBytes)
	if err != nil {
		r.Close(ctx)
		return nil, fmt.Errorf("qbatch host: instantiate wasm: %v", err)
	}
	mem := mod.Memory()
	if mem == nil {
		r.Close(ctx)
		return nil, fmt.Errorf("qbatch host: wasm module does not export memory")
	}
	mem64 := mod.Memory64()
	isMem64 := mem64 != nil
	if !isMem64 && isaWasmIsMemory64(wasmBytes) {
		r.Close(ctx)
		return nil, fmt.Errorf("qbatch host: wasm declares memory64 but Memory64() returned nil")
	}
	if resolved.Profile != BatchCapacityStandard && resolved.Profile != BatchCapacityHigh {
		r.Close(ctx)
		return nil, fmt.Errorf("qbatch host: unknown batch capacity profile %v", resolved.Profile)
	}
	if resolved.Profile == BatchCapacityHigh && !isMem64 {
		r.Close(ctx)
		return nil, fmt.Errorf("qbatch host: high-capacity profile requires wasm64, refusing to run on wasm32 (no silent downgrade to standard)")
	}
	x := &WasmBatchExecutor{runtime: r, module: mod, memory: mem, memory64: mem64, isMem64: isMem64, opts: resolved, profile: resolved.Profile, maxRows: resolved.MaxRows, maxGroups: resolved.MaxGroups}
	if resolved.Profile == BatchCapacityHigh {
		if resolved.BudgetBytes > 0 {
			x.budgetBytes = resolved.BudgetBytes
		} else {
			budget, err := HostMemoryBudget()
			if err != nil {
				r.Close(ctx)
				return nil, err
			}
			x.budgetBytes = budget
		}
	}
	get := func(name string) api.Function { return mod.ExportedFunction(name) }
	x.fnVersion = get("yowza_queryisa_version")
	x.fnFilter = get("yowza_qbatch_filter")
	x.fnProject = get("yowza_qbatch_project")
	x.fnJoinInit = get("yowza_qbatch_join_init")
	x.fnJoinBuild = get("yowza_qbatch_join_build")
	x.fnJoinProbe = get("yowza_qbatch_join_probe")
	x.fnSortInit = get("yowza_qbatch_sort_init")
	x.fnSortAdd = get("yowza_qbatch_sort_add")
	x.fnSortFin = get("yowza_qbatch_sort_finish")
	x.fnAggInit = get("yowza_qbatch_agg_init")
	x.fnAggAdd = get("yowza_qbatch_agg_add")
	x.fnAggFin = get("yowza_qbatch_agg_finish")
	x.fnJoinInit2 = get("yowza_qbatch_join_init2")
	x.fnSortInit2 = get("yowza_qbatch_sort_init2")
	x.fnAggInit2 = get("yowza_qbatch_agg_init2")
	x.fnStateRelocate = get("yowza_qbatch_state_relocate")
	x.fnStateInfo = get("yowza_qbatch_state_info")
	if resolved.Profile == BatchCapacityHigh && (x.fnJoinInit2 == nil || x.fnSortInit2 == nil || x.fnAggInit2 == nil || x.fnStateRelocate == nil || x.fnStateInfo == nil) {
		r.Close(ctx)
		return nil, fmt.Errorf("qbatch host: wasm binary predates high-capacity exports (init2/relocate/state_info)")
	}
	for name, fn := range map[string]api.Function{
		"yowza_queryisa_version": x.fnVersion, "yowza_qbatch_filter": x.fnFilter,
		"yowza_qbatch_project": x.fnProject, "yowza_qbatch_join_init": x.fnJoinInit,
		"yowza_qbatch_join_build": x.fnJoinBuild, "yowza_qbatch_join_probe": x.fnJoinProbe,
		"yowza_qbatch_sort_init": x.fnSortInit, "yowza_qbatch_sort_add": x.fnSortAdd,
		"yowza_qbatch_sort_finish": x.fnSortFin, "yowza_qbatch_agg_init": x.fnAggInit,
		"yowza_qbatch_agg_add": x.fnAggAdd, "yowza_qbatch_agg_finish": x.fnAggFin,
	} {
		if fn == nil {
			r.Close(ctx)
			return nil, fmt.Errorf("qbatch host: wasm binary predates batch exports (missing %s)", name)
		}
	}
	ver, err := x.call(x.fnVersion)
	if err != nil {
		r.Close(ctx)
		return nil, fmt.Errorf("qbatch host: version call: %v", err)
	}
	if ver != ISAVersion {
		r.Close(ctx)
		return nil, fmt.Errorf("qbatch host: wasm ISA version %d != host %d", ver, ISAVersion)
	}
	const base = 32 << 20 // past module static data, as ISAHost does
	x.cellsOff = base
	x.paramsOff = base + 4096
	x.workOff = x.paramsOff + (64 << 10)
	x.joinStateOff = x.workOff + resolved.WorkCap
	x.sortStateOff = x.joinStateOff + resolved.StateCap
	x.aggStateOff = x.sortStateOff + resolved.StateCap
	x.inOff = x.aggStateOff + resolved.StateCap
	x.inCap = 1 << 20
	x.outOff = x.inOff + x.inCap
	x.outCap = 1 << 20
	x.joinStateCap = resolved.StateCap
	x.sortStateCap = resolved.StateCap
	x.aggStateCap = resolved.StateCap
	x.tailStart = x.outOff + x.outCap
	x.tailEnd = x.tailStart
	x.needOff = x.cellsOff + 16
	if err := x.memGrowTo(x.outOff + x.outCap); err != nil {
		r.Close(ctx)
		return nil, fmt.Errorf("qbatch host: grow memory: %v", err)
	}
	return x, nil
}

// Close shuts down the runtime. Callers must not use the
// executor after Close.
func (x *WasmBatchExecutor) Close() error {
	x.mu.Lock()
	defer x.mu.Unlock()
	return x.runtime.Close(context.Background())
}

func (x *WasmBatchExecutor) call(fn api.Function, args ...uint64) (int32, error) {
	res, err := fn.Call(context.Background(), args...)
	if err != nil {
		return 0, err
	}
	if len(res) == 0 {
		return 0, fmt.Errorf("qbatch host: empty call result")
	}
	return int32(uint32(res[0])), nil
}

func (x *WasmBatchExecutor) memSize() uint64 {
	if x.isMem64 && x.memory64 != nil {
		return x.memory64.Size64()
	}
	return uint64(x.memory.Size())
}

func (x *WasmBatchExecutor) memGrowTo(target uint64) error {
	size := x.memSize()
	if size >= target {
		return nil
	}
	if x.budgetBytes > 0 && target > x.budgetBytes {
		return fmt.Errorf("qbatch host: growing wasm memory to %d exceeds budget %d: %w", target, x.budgetBytes, ErrWasmBudgetExceeded)
	}
	need := (target - size + wasmPageSize - 1) / wasmPageSize
	if x.isMem64 && x.memory64 != nil {
		if _, ok := x.memory64.Grow64(need); !ok {
			return fmt.Errorf("qbatch host: failed to grow wasm memory to %d", target)
		}
	} else {
		if need > 0xFFFFFFFF {
			return fmt.Errorf("qbatch host: growth too large")
		}
		if _, ok := x.memory.Grow(uint32(need)); !ok {
			return fmt.Errorf("qbatch host: failed to grow wasm memory to %d", target)
		}
	}
	if x.memSize() < target {
		return fmt.Errorf("qbatch host: memory growth accepted but still below %d", target)
	}
	return nil
}

func (x *WasmBatchExecutor) memRead(offset, n uint64) ([]byte, bool) {
	if x.isMem64 && x.memory64 != nil {
		return x.memory64.Read64(offset, n)
	}
	if offset > 0xFFFFFFFF || n > 0xFFFFFFFF {
		return nil, false
	}
	return x.memory.Read(uint32(offset), uint32(n))
}

func (x *WasmBatchExecutor) memWrite(offset uint64, val []byte) bool {
	if x.isMem64 && x.memory64 != nil {
		return x.memory64.Write64(offset, val)
	}
	if offset > 0xFFFFFFFF {
		return false
	}
	return x.memory.Write(uint32(offset), val)
}

// stateKind identifies one operator state region for the
// host-owned growth machinery below.
type stateKind int

const (
	stateJoin stateKind = iota
	stateSort
	stateAgg
)

func (x *WasmBatchExecutor) stateLoc(kind stateKind) (uint64, uint64) {
	switch kind {
	case stateJoin:
		return x.joinStateOff, x.joinStateCap
	case stateSort:
		return x.sortStateOff, x.sortStateCap
	default:
		return x.aggStateOff, x.aggStateCap
	}
}

func (x *WasmBatchExecutor) setStateLoc(kind stateKind, off, cap uint64) {
	switch kind {
	case stateJoin:
		x.joinStateOff, x.joinStateCap = off, cap
	case stateSort:
		x.sortStateOff, x.sortStateCap = off, cap
	default:
		x.aggStateOff, x.aggStateCap = off, cap
	}
}

// stateInfo reads a live state's bump usage and last
// preflight need through the C state_info export, using
// two 8-byte scratch cells beside the out_len cell.
func (x *WasmBatchExecutor) stateInfo(kind stateKind) (used, need uint64, err error) {
	off, cap := x.stateLoc(kind)
	zero := make([]byte, 16)
	if !x.memWrite(x.needOff, zero) {
		return 0, 0, fmt.Errorf("qbatch host: failed to write state_info scratch")
	}
	rc, err := x.call(x.fnStateInfo, off, cap, x.needOff, x.needOff+8)
	if err != nil {
		return 0, 0, err
	}
	if rc != 0 {
		return 0, 0, qbatchErr(rc, "state_info")
	}
	usedBytes, ok := x.memRead(x.needOff, 8)
	if !ok {
		return 0, 0, fmt.Errorf("qbatch host: failed to read state_info result")
	}
	needBytes, ok := x.memRead(x.needOff+8, 8)
	if !ok {
		return 0, 0, fmt.Errorf("qbatch host: failed to read state_info result")
	}
	return binary.LittleEndian.Uint64(usedBytes), binary.LittleEndian.Uint64(needBytes), nil
}

// growState relocates one operator state to a larger tail
// region: copy the old region bytes, run the C relocate
// fixup (raw copies alone would dangle every internal
// pointer), and advance the tail. The old region is left
// as dead space; it is never written again. Growth stops
// loudly at the budget with ErrWasmBudgetExceeded, with
// the old region still intact.
func (x *WasmBatchExecutor) growState(kind stateKind, need uint64) error {
	oldOff, oldCap := x.stateLoc(kind)
	newCap := oldCap * 2
	if need > newCap {
		newCap = need
	}
	newCap = (newCap + wasmPageSize - 1) / wasmPageSize * wasmPageSize
	if newCap <= oldCap {
		newCap = oldCap + wasmPageSize
	}
	newOff := (x.tailEnd + 7) &^ 7
	target := newOff + newCap
	if x.budgetBytes > 0 && target > x.budgetBytes {
		return fmt.Errorf("qbatch high-capacity: growing state region to %d bytes (total %d) exceeds budget %d: %w", newCap, target, x.budgetBytes, ErrWasmBudgetExceeded)
	}
	if err := x.memGrowTo(target); err != nil {
		return err
	}
	const chunk = 1 << 20
	for pos := uint64(0); pos < oldCap; pos += chunk {
		n := uint64(chunk)
		if pos+n > oldCap {
			n = oldCap - pos
		}
		raw, ok := x.memRead(oldOff+pos, n)
		if !ok {
			return fmt.Errorf("qbatch high-capacity: failed to read old state region")
		}
		buf := append([]byte(nil), raw...)
		if !x.memWrite(newOff+pos, buf) {
			return fmt.Errorf("qbatch high-capacity: failed to write new state region")
		}
	}
	rc, err := x.call(x.fnStateRelocate, newOff, newCap, oldOff, oldCap)
	if err != nil {
		return err
	}
	if rc != 0 {
		return qbatchErr(rc, "state_relocate")
	}
	x.setStateLoc(kind, newOff, newCap)
	x.tailEnd = newOff + newCap
	return nil
}

// callStateful runs one stateful batch call under the
// HighCapacity growth loop; under Standard it is a single
// attempt. On a state-full return (-5) it verifies, via
// state_info, that the failed call consumed nothing
// (bump usage identical before/after) and that the C
// preflight names a capacity need; if either check fails
// the OOM is terminal, not retried. Otherwise it grows
// the region and retries the same batch. The attempt
// closure must re-read the state's offset/cap from the
// executor on every call, since growth moves them.
func (x *WasmBatchExecutor) callStateful(kind stateKind, attempt func() (int32, error)) (int32, error) {
	if x.profile != BatchCapacityHigh {
		return attempt()
	}
	for iter := 0; ; iter++ {
		usedBefore, _, err := x.stateInfo(kind)
		if err != nil {
			return 0, err
		}
		rc, err := attempt()
		if err != nil || rc != -5 {
			return rc, err
		}
		usedAfter, need, err := x.stateInfo(kind)
		if err != nil {
			return 0, err
		}
		if usedAfter != usedBefore {
			return 0, fmt.Errorf("qbatch high-capacity: state-full return consumed state (bump %d -> %d); refusing retry", usedBefore, usedAfter)
		}
		if need == 0 {
			// Not a capacity failure (overlong join key,
			// work-buffer exhaustion): growth cannot help.
			return rc, nil
		}
		if iter >= 64 {
			return 0, fmt.Errorf("qbatch high-capacity: state growth did not converge after 64 attempts")
		}
		if err := x.growState(kind, need); err != nil {
			return 0, err
		}
	}
}

// placeOutHigh moves or extends the output region under
// HighCapacity so it never overlaps a tail state region.
// newCap is the wanted output capacity. Standard callers
// never use this (their layout behaviour is unchanged).
func (x *WasmBatchExecutor) placeOutHigh(newCap uint64) {
	if x.outOff+newCap <= x.tailStart || x.outOff+x.outCap == x.tailEnd {
		// Gap before the tail zone, or the topmost region:
		// grow in place.
	} else {
		x.outOff = (x.tailEnd + 7) &^ 7
	}
	x.outCap = newCap
	if x.outOff+x.outCap > x.tailEnd {
		x.tailEnd = x.outOff + x.outCap
	}
}

// writeIn places a batch in the input buffer, growing it (and
// relocating the output buffer, whose contents are per-call
// scratch) when the batch does not fit.
func (x *WasmBatchExecutor) writeIn(data []byte) error {
	if uint64(len(data)) > x.inCap {
		newCap := x.inCap * 2
		for uint64(len(data)) > newCap {
			newCap *= 2
		}
		x.inCap = newCap
		if x.profile == BatchCapacityHigh {
			cand := x.inOff + x.inCap
			if cand+1<<20 <= x.tailStart || cand >= x.tailEnd {
				x.outOff = cand
				x.outCap = 1 << 20
				if x.outOff+x.outCap > x.tailEnd {
					x.tailEnd = x.outOff + x.outCap
				}
			} else {
				x.outOff = (x.tailEnd + 7) &^ 7
				x.outCap = 1 << 20
				x.tailEnd = x.outOff + x.outCap
			}
		} else {
			x.outOff = x.inOff + x.inCap
			x.outCap = 1 << 20
		}
	}
	if err := x.memGrowTo(x.outOff + x.outCap); err != nil {
		return err
	}
	if !x.memWrite(x.inOff, data) {
		return fmt.Errorf("qbatch host: failed to write input batch")
	}
	return nil
}

func (x *WasmBatchExecutor) writeParams(data []byte) error {
	if uint64(len(data)) > 64<<10-4096 {
		return fmt.Errorf("qbatch host: parameter block %d bytes exceeds params region", len(data))
	}
	if !x.memWrite(x.paramsOff, data) {
		return fmt.Errorf("qbatch host: failed to write params")
	}
	return nil
}

// callOut runs an output-producing batch call, retrying with a
// doubled output buffer on QISA_ERR_OUT_SMALL. attempt writes
// all inputs and performs one call, returning its rc. The
// inputs are rewritten on every attempt because growing the
// output buffer never moves the input buffer, but rewriting
// keeps the retry correct even if a layout ever changes.
func (x *WasmBatchExecutor) callOut(attempt func() (int32, error)) ([]byte, error) {
	for {
		rc, err := attempt()
		if err != nil {
			return nil, err
		}
		if rc == -6 { // OUT_SMALL
			if x.outCap >= x.opts.OutMax {
				return nil, fmt.Errorf("qbatch host: result exceeds maximum output buffer %d", x.opts.OutMax)
			}
			newCap := x.outCap * 2
			if newCap > x.opts.OutMax {
				newCap = x.opts.OutMax
			}
			if x.profile == BatchCapacityHigh {
				x.placeOutHigh(newCap)
			} else {
				x.outCap = newCap
			}
			if err := x.memGrowTo(x.outOff + x.outCap); err != nil {
				return nil, err
			}
			continue
		}
		if rc != 0 {
			return nil, qbatchErr(rc, "call")
		}
		lenBytes, ok := x.memRead(x.cellsOff, 4)
		if !ok {
			return nil, fmt.Errorf("qbatch host: failed to read output length")
		}
		outLen := binary.LittleEndian.Uint32(lenBytes)
		if uint64(outLen) > x.outCap {
			return nil, fmt.Errorf("qbatch host: output length %d exceeds buffer %d", outLen, x.outCap)
		}
		raw, ok := x.memRead(x.outOff, uint64(outLen))
		if !ok {
			return nil, fmt.Errorf("qbatch host: failed to read output")
		}
		return raw, nil
	}
}

// qbatchErr maps a C return code to the Go error the reference
// executor's callers already understand: budget failures are
// BUDGET_EXCEEDED, resource caps are ErrResourceLimit, int64
// SUM overflow is ErrNumericOverflow, everything else is a
// loud generic error. No code is mapped to success.
func qbatchErr(rc int32, op string) error {
	switch rc {
	case -4:
		return &VerificationError{Code: "BUDGET_EXCEEDED", Msg: "qbatch " + op + ": budget exceeded"}
	case -9:
		return fmt.Errorf("qbatch %s: %w", op, ErrResourceLimit)
	case -8:
		return fmt.Errorf("qbatch %s: %w", op, ErrNumericOverflow)
	case -3:
		return fmt.Errorf("qbatch %s: type error", op)
	case -5:
		return fmt.Errorf("qbatch %s: out of memory / resource cap (state buffer exhausted?)", op)
	case -1:
		return fmt.Errorf("qbatch %s: malformed batch or state", op)
	case -7:
		return fmt.Errorf("qbatch %s: scan failed", op)
	default:
		return fmt.Errorf("qbatch %s: engine failed (rc=%d)", op, rc)
	}
}

// stateStats reads a state buffer's peak/spilled header fields
// (QBATCH_OFF_PEAK / QBATCH_OFF_SPILLED in query_isa.h).
func (x *WasmBatchExecutor) stateStats(stateOff uint64) (peak uint64, spilled bool) {
	if b, ok := x.memRead(stateOff+32, 8); ok {
		peak = binary.LittleEndian.Uint64(b)
	}
	if b, ok := x.memRead(stateOff+40, 4); ok {
		spilled = binary.LittleEndian.Uint32(b) != 0
	}
	return peak, spilled
}

// ---- batch wire encoding (mirrors the ISA value encoding) ----

func encodeQBatch(rows []Row, width int) ([]byte, error) {
	comp := &isaCompiler{}
	comp.w.u32(uint32(len(rows)))
	for _, r := range rows {
		if len(r.Vals) != width {
			return nil, fmt.Errorf("qbatch: row has %d values, want width %d", len(r.Vals), width)
		}
		for _, v := range r.Vals {
			comp.encodeValue(v)
		}
	}
	return comp.w.b, nil
}

func decodeQBatch(raw []byte, width int, cols []string) ([]Row, error) {
	if len(raw) < 4 {
		return nil, fmt.Errorf("qbatch: result too short (%d bytes)", len(raw))
	}
	n := binary.LittleEndian.Uint32(raw[:4])
	pos := 4
	rows := make([]Row, 0, n)
	for i := uint32(0); i < n; i++ {
		vals := make([]Value, width)
		for c := 0; c < width; c++ {
			v, np, err := decodeISAValue(raw, pos)
			if err != nil {
				return nil, fmt.Errorf("qbatch: decode result: %v", err)
			}
			vals[c] = v
			pos = np
		}
		rows = append(rows, Row{Cols: append([]string(nil), cols...), Vals: vals})
	}
	if pos != len(raw) {
		return nil, fmt.Errorf("qbatch: trailing bytes in result (%d of %d consumed)", pos, len(raw))
	}
	return rows, nil
}

func encodeU32s(vals []uint32) []byte {
	out := make([]byte, 0, len(vals)*4)
	var tmp [4]byte
	for _, v := range vals {
		binary.LittleEndian.PutUint32(tmp[:], v)
		out = append(out, tmp[:]...)
	}
	return out
}

func nameLens(cols []string) []uint32 {
	out := make([]uint32, len(cols))
	for i, c := range cols {
		out[i] = uint32(len(c))
	}
	return out
}

// encodeQBatchExpr encodes a bound filter expression in ISA
// expression format, resolving column references through colPos
// (pipeline position). It mirrors isaCompiler.encodeExpr with
// a position function instead of the compiler's binding table;
// the differential tests prove the two encodings agree by
// executing the same plans through both engines.
func encodeQBatchExpr(e *Expr, colPos func(*ColumnRef) (uint32, error)) ([]byte, error) {
	enc := &qbatchExprEnc{colPos: colPos}
	if err := enc.expr(e); err != nil {
		return nil, err
	}
	return enc.w.b, nil
}

type qbatchExprEnc struct {
	w      isaBuf
	colPos func(*ColumnRef) (uint32, error)
}

func (e *qbatchExprEnc) operand(x *Expr) error {
	switch x.Kind {
	case "col":
		idx, err := e.colPos(x.Col)
		if err != nil {
			return err
		}
		e.w.u8(isaExprCol)
		e.w.u32(idx)
		return nil
	case "lit":
		if x.Lit == nil {
			return fmt.Errorf("qbatch: literal expression without a value")
		}
		e.w.u8(isaExprLit)
		comp := &isaCompiler{}
		comp.encodeValue(*x.Lit)
		e.w.bytes(comp.w.b)
		return nil
	default:
		return fmt.Errorf("qbatch: operand kind %q is not a column or literal", x.Kind)
	}
}

func (e *qbatchExprEnc) expr(x *Expr) error {
	if x == nil {
		return fmt.Errorf("qbatch: nil expression")
	}
	switch x.Kind {
	case "and", "or":
		if x.Kind == "and" {
			e.w.u8(isaExprAnd)
		} else {
			e.w.u8(isaExprOr)
		}
		e.w.u32(uint32(len(x.Args)))
		for _, a := range x.Args {
			if err := e.expr(a); err != nil {
				return err
			}
		}
		return nil
	case "not":
		if len(x.Args) != 1 {
			return fmt.Errorf("qbatch: NOT requires exactly one operand")
		}
		e.w.u8(isaExprNot)
		return e.expr(x.Args[0])
	case "cmp":
		var code uint8
		switch x.Op {
		case "=":
			code = isaCmpEq
		case "!=", "<>":
			code = isaCmpNe
		case "<":
			code = isaCmpLt
		case "<=":
			code = isaCmpLe
		case ">":
			code = isaCmpGt
		case ">=":
			code = isaCmpGe
		default:
			return fmt.Errorf("qbatch: unknown comparison operator %q", x.Op)
		}
		e.w.u8(isaExprCmp)
		e.w.u8(code)
		if err := e.operand(x.Left); err != nil {
			return err
		}
		return e.operand(x.Right)
	case "in":
		e.w.u8(isaExprIn)
		if err := e.operand(x.Left); err != nil {
			return err
		}
		if x.Negated {
			e.w.u8(1)
		} else {
			e.w.u8(0)
		}
		e.w.u32(uint32(len(x.InList)))
		for _, v := range x.InList {
			comp := &isaCompiler{}
			comp.encodeValue(v)
			e.w.bytes(comp.w.b)
		}
		return nil
	case "like":
		e.w.u8(isaExprLike)
		if err := e.operand(x.Left); err != nil {
			return err
		}
		if x.Negated {
			e.w.u8(1)
		} else {
			e.w.u8(0)
		}
		e.w.str(x.Pattern)
		return nil
	case "isnull":
		e.w.u8(isaExprIsNull)
		if err := e.operand(x.Left); err != nil {
			return err
		}
		if x.Negated {
			e.w.u8(1)
		} else {
			e.w.u8(0)
		}
		return nil
	default:
		return fmt.Errorf("qbatch: unsupported expression kind %q", x.Kind)
	}
}

func qualifiedCols(b Binding) []string {
	out := make([]string, len(b.Columns))
	for i, c := range b.Columns {
		out[i] = b.Alias + "." + c.Name
	}
	return out
}

func indexOf(cols []string, name string) (int, bool) {
	for i, c := range cols {
		if c == name {
			return i, true
		}
	}
	return -1, false
}

// checkBatchCols validates that every row of a source batch
// carries the binding's qualified columns in order (rows with
// no Cols at all are accepted positionally). A mismatch is a
// loud error, never a silent positional reinterpretation.
func checkBatchCols(batch []Row, want []string) error {
	for _, r := range batch {
		if len(r.Cols) == 0 {
			if len(r.Vals) != len(want) {
				return fmt.Errorf("qbatch: row has %d values, want %d columns", len(r.Vals), len(want))
			}
			continue
		}
		if len(r.Cols) != len(want) || len(r.Vals) != len(want) {
			return fmt.Errorf("qbatch: row has %d columns, want %d", len(r.Cols), len(want))
		}
		for i, c := range want {
			if r.Cols[i] != c {
				return fmt.Errorf("qbatch: row column %d is %q, want %q", i, r.Cols[i], c)
			}
		}
	}
	return nil
}

// ExecutePlan runs one verified plan with operator batches in
// WASM. See the file header for the contract. On failure it
// returns no rows and a trace describing how far execution got,
// exactly like Execute does.
func (x *WasmBatchExecutor) ExecutePlan(ctx context.Context, plan *Plan, contract Contract, sources map[string]Source, opts *ExecuteOptions) ([]Row, *Trace, error) {
	x.mu.Lock()
	defer x.mu.Unlock()
	start := time.Now()
	// Batch size: the executor's configured default, overridden
	// by a per-call ExecuteOptions value when one is given.
	batchSize := x.opts.BatchSize
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}
	if opts != nil && opts.BatchSize > 0 {
		batchSize = opts.BatchSize
	}
	trace := &Trace{
		PlanVersion:    PlanVersion,
		CommitBoundary: "read-only snapshot; no commit",
		ResultLimit:    -1,
	}
	if plan != nil {
		trace.PlanVersion = plan.Version
		trace.PlanSummary = Explain(plan)
		trace.Operators = append([]string(nil), plan.Operators...)
		trace.EstimatedRows = plan.EstimatedRows
		if plan.HasLimit && plan.Limit != nil {
			trace.ResultLimit = *plan.Limit
		}
		if h, err := PlanHash(plan); err == nil && len(h) >= 16 {
			trace.QueryID = h[:16]
		}
	}
	var scanned int64
	var peak uint64
	var spilled bool
	fail := func(err error) ([]Row, *Trace, error) {
		trace.Duration = time.Since(start)
		trace.ScannedRows = scanned
		trace.ActualMemoryBytes = int64(peak)
		trace.Spilled = spilled
		trace.ExecutionError = err.Error()
		return nil, trace, err
	}
	// Phase 1: verification, before any source is opened.
	if err := Verify(plan, contract); err != nil {
		if ve, ok := err.(*VerificationError); ok {
			trace.ValidationError = ve
		} else {
			trace.ValidationError = &VerificationError{Code: "BAD_PLAN", Msg: err.Error()}
		}
		trace.Duration = time.Since(start)
		return nil, trace, err
	}
	bindingFor := func(alias string) (Binding, bool) {
		for _, b := range plan.Bindings {
			if b.Alias == alias {
				return b, true
			}
		}
		return Binding{}, false
	}
	scanAdd := func(n int) error {
		// Add-then-check, exactly as the reference scanBudget
		// does: the trace's scanned count includes the batch
		// that trips the cap.
		scanned += int64(n)
		if contract.MaxScannedRows > 0 && scanned > contract.MaxScannedRows {
			return &VerificationError{Code: "BUDGET_EXCEEDED", Msg: fmt.Sprintf("scanned rows %d exceed max scanned rows %d", scanned, contract.MaxScannedRows)}
		}
		return nil
	}
	memBudget := uint64(0)
	if contract.MaxMemoryBytes > 0 {
		memBudget = uint64(contract.MaxMemoryBytes)
	}
	noteState := func(stateOff uint64) {
		p, s := x.stateStats(stateOff)
		if p > peak {
			peak = p
		}
		if s {
			spilled = true
		}
	}

	// Phase 2: open sources (freezes each source's snapshot).
	leftBinding, ok := bindingFor(plan.FromAlias)
	if !ok {
		return fail(fmt.Errorf("no binding for FROM alias %q", plan.FromAlias))
	}
	leftSource, ok := sources[plan.FromAlias]
	if !ok {
		return fail(fmt.Errorf("no source provided for table %q (alias %q)", plan.FromTable, plan.FromAlias))
	}
	leftIt, err := leftSource.Open(ctx, leftBinding)
	if err != nil {
		return fail(err)
	}
	defer leftIt.Close()
	leftCols := qualifiedCols(leftBinding)

	pipelineCols := append([]string(nil), leftCols...)
	var rightCols []string
	if plan.Join != nil {
		if plan.Join.LeftCol == nil || plan.Join.RightCol == nil {
			return fail(fmt.Errorf("qbatch: join without join columns"))
		}
		rightBinding, ok := bindingFor(plan.Join.RightAlias)
		if !ok {
			return fail(fmt.Errorf("no binding for JOIN alias %q", plan.Join.RightAlias))
		}
		rightSource, ok := sources[plan.Join.RightAlias]
		if !ok {
			return fail(fmt.Errorf("no source provided for table %q (alias %q)", plan.Join.RightTable, plan.Join.RightAlias))
		}
		rightIt, err := rightSource.Open(ctx, rightBinding)
		if err != nil {
			return fail(err)
		}
		defer rightIt.Close()
		rightCols = qualifiedCols(rightBinding)
		leftPos, ok := indexOf(leftCols, plan.Join.LeftCol.Qualified)
		if !ok {
			return fail(fmt.Errorf("qbatch: join left column %q not in left columns", plan.Join.LeftCol.Qualified))
		}
		rightPos, ok := indexOf(rightCols, plan.Join.RightCol.Qualified)
		if !ok {
			return fail(fmt.Errorf("qbatch: join right column %q not in right columns", plan.Join.RightCol.Qualified))
		}
		var rc int32
		if x.profile == BatchCapacityHigh {
			rc, err = x.call(x.fnJoinInit2, x.joinStateOff, x.joinStateCap, uint64(len(leftCols)), uint64(leftPos), uint64(len(rightCols)), uint64(rightPos), memBudget, uint64(x.maxRows))
		} else {
			rc, err = x.call(x.fnJoinInit, x.joinStateOff, x.joinStateCap, uint64(len(leftCols)), uint64(leftPos), uint64(len(rightCols)), uint64(rightPos), memBudget)
		}
		if err != nil {
			return fail(err)
		}
		if rc != 0 {
			return fail(qbatchErr(rc, "join_init"))
		}
		pipelineCols = append(pipelineCols, rightCols...)
		// Build side goes through the wasm hash table first,
		// fully, before any left row is pulled (as the
		// reference does), budget-checked per batch.
		for {
			if err := ctx.Err(); err != nil {
				return fail(err)
			}
			batch, err := rightIt.NextBatch(batchSize)
			if err != nil {
				return fail(err)
			}
			if len(batch) == 0 {
				break
			}
			if err := checkBatchCols(batch, rightCols); err != nil {
				return fail(err)
			}
			if err := scanAdd(len(batch)); err != nil {
				return fail(err)
			}
			enc, err := encodeQBatch(batch, len(rightCols))
			if err != nil {
				return fail(err)
			}
			if err := x.writeIn(enc); err != nil {
				return fail(err)
			}
			if err := x.writeParams(encodeU32s(nameLens(rightCols))); err != nil {
				return fail(err)
			}
			rc, err := x.callStateful(stateJoin, func() (int32, error) {
				return x.call(x.fnJoinBuild, x.joinStateOff, x.joinStateCap, x.inOff, uint64(len(enc)), x.paramsOff)
			})
			if err != nil {
				return fail(err)
			}
			if rc != 0 {
				return fail(qbatchErr(rc, "join_build"))
			}
			noteState(x.joinStateOff)
		}
	}

	// Filter expression, encoded once against pipeline positions.
	var filterExpr []byte
	if plan.Filter != nil {
		filterExpr, err = encodeQBatchExpr(plan.Filter, func(ref *ColumnRef) (uint32, error) {
			if ref == nil {
				return 0, fmt.Errorf("qbatch: nil column reference in filter")
			}
			idx, ok := indexOf(pipelineCols, ref.Qualified)
			if !ok {
				return 0, fmt.Errorf("qbatch: filter column %q not in pipeline", ref.Qualified)
			}
			return uint32(idx), nil
		})
		if err != nil {
			return fail(err)
		}
	}

	// pull returns the next non-empty SCAN/JOIN/FILTER batch,
	// or nil at EOF. Empty probe/filter results keep pulling,
	// exactly as the reference join/filter iterators do.
	pull := func() ([]Row, error) {
		for {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			var batch []Row
			if plan.Join != nil {
				lb, err := leftIt.NextBatch(batchSize)
				if err != nil {
					return nil, err
				}
				if len(lb) == 0 {
					return nil, nil
				}
				if err := checkBatchCols(lb, leftCols); err != nil {
					return nil, err
				}
				if err := scanAdd(len(lb)); err != nil {
					return nil, err
				}
				enc, err := encodeQBatch(lb, len(leftCols))
				if err != nil {
					return nil, err
				}
				raw, err := x.callOut(func() (int32, error) {
					return x.callStateful(stateJoin, func() (int32, error) {
						if err := x.writeIn(enc); err != nil {
							return 0, err
						}
						return x.call(x.fnJoinProbe, x.joinStateOff, x.joinStateCap, x.inOff, uint64(len(enc)), x.outOff, x.outCap, x.cellsOff, x.workOff, x.opts.WorkCap)
					})
				})
				if err != nil {
					return nil, err
				}
				batch, err = decodeQBatch(raw, len(pipelineCols), pipelineCols)
				if err != nil {
					return nil, err
				}
			} else {
				lb, err := leftIt.NextBatch(batchSize)
				if err != nil {
					return nil, err
				}
				if len(lb) == 0 {
					return nil, nil
				}
				if err := checkBatchCols(lb, leftCols); err != nil {
					return nil, err
				}
				if err := scanAdd(len(lb)); err != nil {
					return nil, err
				}
				batch = lb
			}
			if plan.Filter != nil {
				enc, err := encodeQBatch(batch, len(pipelineCols))
				if err != nil {
					return nil, err
				}
				raw, err := x.callOut(func() (int32, error) {
					if err := x.writeIn(enc); err != nil {
						return 0, err
					}
					if err := x.writeParams(filterExpr); err != nil {
						return 0, err
					}
					return x.call(x.fnFilter, x.inOff, uint64(len(enc)), uint64(len(pipelineCols)), x.paramsOff, uint64(len(filterExpr)), x.outOff, x.outCap, x.cellsOff, x.workOff, x.opts.WorkCap)
				})
				if err != nil {
					return nil, err
				}
				batch, err = decodeQBatch(raw, len(pipelineCols), pipelineCols)
				if err != nil {
					return nil, err
				}
			}
			if len(batch) > 0 {
				return batch, nil
			}
		}
	}

	// Project indices: non-aggregate plans index pipeline
	// columns; aggregate plans index the aggregate row (group
	// keys, then aggregate results in projection order).
	var projIdx []uint32
	var projOutCols []string
	for _, it := range plan.Projection {
		projOutCols = append(projOutCols, it.OutputName)
	}
	if plan.IsAggregate() {
		groupQual := make([]string, len(plan.GroupBy))
		for i, g := range plan.GroupBy {
			groupQual[i] = g.Qualified
		}
		aggOrdinal := 0
		for _, it := range plan.Projection {
			if it.Agg != "" {
				projIdx = append(projIdx, uint32(len(plan.GroupBy)+aggOrdinal))
				aggOrdinal++
				continue
			}
			if it.Col == nil {
				return fail(fmt.Errorf("qbatch: plain projection item without a column"))
			}
			idx, ok := indexOf(groupQual, it.Col.Qualified)
			if !ok {
				return fail(fmt.Errorf("qbatch: plain column %q is not in GROUP BY", it.Col.Qualified))
			}
			projIdx = append(projIdx, uint32(idx))
		}
	} else {
		for _, it := range plan.Projection {
			if it.Col == nil {
				return fail(fmt.Errorf("qbatch: projection item without a column"))
			}
			idx, ok := indexOf(pipelineCols, it.Col.Qualified)
			if !ok {
				return fail(fmt.Errorf("qbatch: projection column %q not in pipeline", it.Col.Qualified))
			}
			projIdx = append(projIdx, uint32(idx))
		}
	}
	project := func(batch []Row, width int) ([]Row, error) {
		if len(batch) == 0 {
			return nil, nil
		}
		enc, err := encodeQBatch(batch, width)
		if err != nil {
			return nil, err
		}
		raw, err := x.callOut(func() (int32, error) {
			if err := x.writeIn(enc); err != nil {
				return 0, err
			}
			if err := x.writeParams(encodeU32s(projIdx)); err != nil {
				return 0, err
			}
			return x.call(x.fnProject, x.inOff, uint64(len(enc)), uint64(width), x.paramsOff, uint64(len(projIdx)), x.outOff, x.outCap, x.cellsOff)
		})
		if err != nil {
			return nil, err
		}
		return decodeQBatch(raw, len(projOutCols), projOutCols)
	}

	// AGGREGATE barrier: stream all input batches into the
	// wasm group state, then finish once. The output rows
	// are chunked at batchSize for the project stage, as the
	// reference aggregate iterator chunks its materialized
	// groups.
	var aggChunks [][]Row
	aggWidth := 0
	var aggOutCols []string
	if plan.IsAggregate() {
		var groupPos []uint32
		for _, g := range plan.GroupBy {
			idx, ok := indexOf(pipelineCols, g.Qualified)
			if !ok {
				return fail(fmt.Errorf("qbatch: group column %q not in pipeline", g.Qualified))
			}
			groupPos = append(groupPos, uint32(idx))
		}
		var specs []uint32
		for _, it := range plan.Projection {
			if it.Agg == "" {
				continue
			}
			var code uint32
			switch it.Agg {
			case "COUNT":
				code = isaAggCount
			case "SUM":
				code = isaAggSum
			case "AVG":
				code = isaAggAvg
			case "MIN":
				code = isaAggMin
			case "MAX":
				code = isaAggMax
			default:
				return fail(fmt.Errorf("qbatch: unknown aggregate %q", it.Agg))
			}
			var star uint32
			var col uint32
			if it.Star {
				star = 1
			} else {
				if it.Col == nil {
					return fail(fmt.Errorf("qbatch: aggregate item without a column"))
				}
				idx, ok := indexOf(pipelineCols, it.Col.Qualified)
				if !ok {
					return fail(fmt.Errorf("qbatch: aggregate column %q not in pipeline", it.Col.Qualified))
				}
				col = uint32(idx)
			}
			specs = append(specs, code, star, col)
		}
		for _, g := range plan.GroupBy {
			aggOutCols = append(aggOutCols, g.Qualified)
		}
		for _, it := range plan.Projection {
			if it.Agg != "" {
				aggOutCols = append(aggOutCols, it.OutputName)
			}
		}
		aggWidth = len(aggOutCols)
		params := append(encodeU32s(groupPos), encodeU32s(specs)...)
		if err := x.writeParams(params); err != nil {
			return fail(err)
		}
		var rc int32
		if x.profile == BatchCapacityHigh {
			rc, err = x.call(x.fnAggInit2, x.aggStateOff, x.aggStateCap, uint64(len(pipelineCols)), uint64(len(groupPos)), x.paramsOff, uint64(len(specs)/3), x.paramsOff+uint64(len(groupPos)*4), memBudget, uint64(x.maxGroups))
		} else {
			rc, err = x.call(x.fnAggInit, x.aggStateOff, x.aggStateCap, uint64(len(pipelineCols)), uint64(len(groupPos)), x.paramsOff, uint64(len(specs)/3), x.paramsOff+uint64(len(groupPos)*4), memBudget)
		}
		if err != nil {
			return fail(err)
		}
		if rc != 0 {
			return fail(qbatchErr(rc, "agg_init"))
		}
		for {
			batch, err := pull()
			if err != nil {
				return fail(err)
			}
			if batch == nil {
				break
			}
			enc, err := encodeQBatch(batch, len(pipelineCols))
			if err != nil {
				return fail(err)
			}
			if err := x.writeIn(enc); err != nil {
				return fail(err)
			}
			if err := x.writeParams(encodeU32s(nameLens(pipelineCols))); err != nil {
				return fail(err)
			}
			rc, err := x.callStateful(stateAgg, func() (int32, error) {
				return x.call(x.fnAggAdd, x.aggStateOff, x.aggStateCap, x.inOff, uint64(len(enc)), x.paramsOff)
			})
			if err != nil {
				return fail(err)
			}
			if rc != 0 {
				return fail(qbatchErr(rc, "agg_add"))
			}
			noteState(x.aggStateOff)
		}
		raw, err := x.callOut(func() (int32, error) {
			return x.callStateful(stateAgg, func() (int32, error) {
				return x.call(x.fnAggFin, x.aggStateOff, x.aggStateCap, x.outOff, x.outCap, x.cellsOff)
			})
		})
		if err != nil {
			return fail(err)
		}
		aggRows, err := decodeQBatch(raw, aggWidth, aggOutCols)
		if err != nil {
			return fail(err)
		}
		for i := 0; i < len(aggRows); i += batchSize {
			end := i + batchSize
			if end > len(aggRows) {
				end = len(aggRows)
			}
			aggChunks = append(aggChunks, aggRows[i:end])
		}
	}

	// SORT barrier: project first, then stream projected
	// batches into the wasm sort state, then finish once.
	// LIMIT is applied after this barrier, never before it.
	var sortRows []Row
	if len(plan.OrderBy) > 0 {
		var keys []uint32
		for _, o := range plan.OrderBy {
			idx, ok := indexOf(projOutCols, o.Name)
			if !ok {
				return fail(fmt.Errorf("qbatch: ORDER BY column %q not in output", o.Name))
			}
			var desc uint32
			if o.Desc {
				desc = 1
			}
			keys = append(keys, uint32(idx), desc)
		}
		var rc int32
		if x.profile == BatchCapacityHigh {
			rc, err = x.call(x.fnSortInit2, x.sortStateOff, x.sortStateCap, uint64(len(projOutCols)), memBudget, uint64(x.maxRows))
		} else {
			rc, err = x.call(x.fnSortInit, x.sortStateOff, x.sortStateCap, uint64(len(projOutCols)), memBudget)
		}
		if err != nil {
			return fail(err)
		}
		if rc != 0 {
			return fail(qbatchErr(rc, "sort_init"))
		}
		feedSort := func(batch []Row, width int) error {
			proj, err := project(batch, width)
			if err != nil {
				return err
			}
			if len(proj) == 0 {
				return nil
			}
			enc, err := encodeQBatch(proj, len(projOutCols))
			if err != nil {
				return err
			}
			if err := x.writeIn(enc); err != nil {
				return err
			}
			if err := x.writeParams(encodeU32s(nameLens(projOutCols))); err != nil {
				return err
			}
			rc, err := x.callStateful(stateSort, func() (int32, error) {
				return x.call(x.fnSortAdd, x.sortStateOff, x.sortStateCap, x.inOff, uint64(len(enc)), x.paramsOff)
			})
			if err != nil {
				return err
			}
			if rc != 0 {
				return qbatchErr(rc, "sort_add")
			}
			noteState(x.sortStateOff)
			return nil
		}
		if plan.IsAggregate() {
			for _, ch := range aggChunks {
				if err := feedSort(ch, aggWidth); err != nil {
					return fail(err)
				}
			}
		} else {
			for {
				batch, err := pull()
				if err != nil {
					return fail(err)
				}
				if batch == nil {
					break
				}
				if err := feedSort(batch, len(pipelineCols)); err != nil {
					return fail(err)
				}
			}
		}
		raw, err := x.callOut(func() (int32, error) {
			return x.callStateful(stateSort, func() (int32, error) {
				if err := x.writeParams(encodeU32s(keys)); err != nil {
					return 0, err
				}
				return x.call(x.fnSortFin, x.sortStateOff, x.sortStateCap, x.paramsOff, uint64(len(keys)/2), x.outOff, x.outCap, x.cellsOff, x.workOff, x.opts.WorkCap)
			})
		})
		if err != nil {
			return fail(err)
		}
		sortRows, err = decodeQBatch(raw, len(projOutCols), projOutCols)
		if err != nil {
			return fail(err)
		}
	}

	// Phase 3: collect, enforcing LIMIT (after global
	// semantics) and then the result cap, as batches arrive.
	var rows []Row
	remaining := int64(-1)
	if plan.HasLimit && plan.Limit != nil {
		remaining = *plan.Limit
	}
	emit := func(batch []Row) error {
		if remaining == 0 {
			return nil
		}
		if remaining >= 0 && int64(len(batch)) > remaining {
			batch = batch[:remaining]
		}
		rows = append(rows, batch...)
		if remaining >= 0 {
			remaining -= int64(len(batch))
		}
		if contract.MaxResultRows > 0 && int64(len(rows)) > contract.MaxResultRows {
			return &VerificationError{Code: "BUDGET_EXCEEDED", Msg: fmt.Sprintf("result rows exceed max result rows %d", contract.MaxResultRows)}
		}
		return nil
	}
	batchesOut := 0
	switch {
	case len(plan.OrderBy) > 0:
		if err := emit(sortRows); err != nil {
			return fail(err)
		}
		if len(sortRows) > 0 {
			batchesOut = (len(sortRows) + batchSize - 1) / batchSize
		}
	case plan.IsAggregate():
		for _, ch := range aggChunks {
			if remaining == 0 {
				break
			}
			proj, err := project(ch, aggWidth)
			if err != nil {
				return fail(err)
			}
			if err := emit(proj); err != nil {
				return fail(err)
			}
			batchesOut++
		}
	default:
		for {
			if remaining == 0 {
				break
			}
			if err := ctx.Err(); err != nil {
				return fail(err)
			}
			batch, err := pull()
			if err != nil {
				return fail(err)
			}
			if batch == nil {
				break
			}
			proj, err := project(batch, len(pipelineCols))
			if err != nil {
				return fail(err)
			}
			if err := emit(proj); err != nil {
				return fail(err)
			}
			batchesOut++
			if remaining == 0 {
				break
			}
		}
	}
	trace.Duration = time.Since(start)
	trace.ScannedRows = scanned
	trace.ActualRows = int64(len(rows))
	trace.ActualReturnedRows = int64(len(rows))
	trace.ActualMemoryBytes = int64(peak)
	trace.Spilled = spilled
	trace.MemoryEstimateBytes = scanned * rowMemoryEstimateBytes
	trace.Batches = int64(batchesOut)
	return rows, trace, nil
}
