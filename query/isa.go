// Query ISA — Go compiler (verified Plan -> ISA bytes) and the wasm
// host runner for the C ISA engine (src/query_isa.c).
//
// The compiler translates the SAME Plan the reference executor runs:
// it never re-derives semantics, never reorders operators, and never
// invents a column, a predicate, or a budget. Compilation REFUSES
// any plan that fails the existing verifier (Verify): an unverified
// plan has no ISA encoding, by construction.
//
// The C engine's version check is the second gate: a program whose
// ISA version is not ISAVersion is rejected by the C engine before
// any scan (see query_isa.h). The Go compiler always emits
// ISAVersion.
//
// Host-runner note (why ISAHost re-implements a small wazero host
// instead of reusing the root package's YowzaDB/SegDB handles): this
// track may not edit any existing file, and the root package's
// module handle, linear-memory accessors, and SegDB state offsets
// are unexported. ISAHost therefore mirrors segdb.go's published
// layout (SegDB state at 32 MiB, then arena / WAL / scratch /
// heap) in its own module instance, using only the wasm exports.
// A follow-up that is allowed to touch segdb.go could replace this
// duplication with one exported accessor; the ISA bytes and the C
// engine would not change.
package query

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
)

// ISAVersion is the Query ISA format version. It must match
// QISA_VERSION in include/query_isa.h; the C engine rejects any
// other version before execution.
//
// Version 2 is the unified execution contract (Reliability stage
// item 02): the program carries Contract.MaxMemoryBytes, each
// table's binding alias (for qualified column names in memory
// accounting), and each PROJECT's output names (for projected-row
// accounting), and the host executes at one caller-supplied
// snapshot sequence. See include/query_isa.h.
const ISAVersion = 2

// ISAMagic is the 4-byte program header magic ("YWQI").
var ISAMagic = [4]byte{'Y', 'W', 'Q', 'I'}

const (
	isaOpScan      = 0x01
	isaOpJoin      = 0x02
	isaOpFilter    = 0x03
	isaOpAggregate = 0x04
	isaOpProject   = 0x05
	isaOpSort      = 0x06
	isaOpLimit     = 0x07
	isaOpEnd       = 0xFF

	isaExprAnd    = 0x10
	isaExprOr     = 0x11
	isaExprNot    = 0x12
	isaExprCmp    = 0x13
	isaExprIn     = 0x14
	isaExprLike   = 0x15
	isaExprIsNull = 0x16
	isaExprCol    = 0x17
	isaExprLit    = 0x18

	isaCmpEq = 0
	isaCmpNe = 1
	isaCmpLt = 2
	isaCmpLe = 3
	isaCmpGt = 4
	isaCmpGe = 5

	isaAggCount = 0
	isaAggSum   = 1
	isaAggAvg   = 2
	isaAggMin   = 3
	isaAggMax   = 4
)

// isaBuf is a little-endian byte builder for ISA programs.
type isaBuf struct {
	b []byte
}

func (w *isaBuf) u8(v uint8) { w.b = append(w.b, v) }
func (w *isaBuf) u32(v uint32) {
	var tmp [4]byte
	binary.LittleEndian.PutUint32(tmp[:], v)
	w.b = append(w.b, tmp[:]...)
}
func (w *isaBuf) u64(v uint64) {
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], v)
	w.b = append(w.b, tmp[:]...)
}
func (w *isaBuf) i64(v int64) { w.u64(uint64(v)) }
func (w *isaBuf) f64(v float64) {
	w.u64(math.Float64bits(v))
}
func (w *isaBuf) bytes(v []byte) { w.b = append(w.b, v...) }
func (w *isaBuf) str(s string) {
	w.u32(uint32(len(s)))
	w.b = append(w.b, s...)
}

// CompileISA compiles a verified plan into ISA bytes. It first runs
// the existing verifier against the given contract and refuses the
// plan (returning the verification error unchanged) when it fails.
// A plan that passes verification always compiles, unless it uses a
// construct outside the Track B subset (which the binder would
// already have rejected); such a failure is a compiler error, not a
// silent approximation.
func CompileISA(plan *Plan, contract Contract) ([]byte, error) {
	return CompileISAWithBudgets(plan, contract)
}

type isaCompiler struct {
	plan *Plan
	w    isaBuf
	// tables, in emitted order: [FROM binding, JOIN binding?]
	tables []Binding
	// colIndex maps alias\x00name -> global column index.
	colIndex map[string]uint32
}

func (c *isaCompiler) bindingFor(alias string) (Binding, bool) {
	for _, b := range c.plan.Bindings {
		if strings.EqualFold(b.Alias, alias) {
			return b, true
		}
	}
	return Binding{}, false
}

func (c *isaCompiler) colIdx(ref *ColumnRef) (uint32, error) {
	if ref == nil {
		return 0, fmt.Errorf("isa: nil column reference")
	}
	key := ref.Alias + "\x00" + ref.Name
	idx, ok := c.colIndex[key]
	if !ok {
		return 0, fmt.Errorf("isa: column %q not in emitted tables", ref.Qualified)
	}
	return idx, nil
}

// CompileISAWithBudgets is the shared implementation behind
// CompileISA (kept separate so the budget threading is explicit
// and testable). It is not exported: callers use CompileISA.
func CompileISAWithBudgets(plan *Plan, contract Contract) ([]byte, error) {
	if err := Verify(plan, contract); err != nil {
		return nil, err
	}
	c := &isaCompiler{plan: plan}
	from, ok := c.bindingFor(plan.FromAlias)
	if !ok {
		return nil, fmt.Errorf("isa: no binding for FROM alias %q", plan.FromAlias)
	}
	c.tables = append(c.tables, from)
	if plan.Join != nil {
		right, ok := c.bindingFor(plan.Join.RightAlias)
		if !ok {
			return nil, fmt.Errorf("isa: no binding for JOIN alias %q", plan.Join.RightAlias)
		}
		c.tables = append(c.tables, right)
	}
	c.colIndex = map[string]uint32{}
	var next uint32
	for _, t := range c.tables {
		for _, col := range t.Columns {
			c.colIndex[t.Alias+"\x00"+col.Name] = next
			next++
		}
	}
	w := &c.w
	w.bytes(ISAMagic[:])
	w.u32(ISAVersion)
	if contract.MaxScannedRows > 0 {
		w.u64(uint64(contract.MaxScannedRows))
	} else {
		w.u64(0)
	}
	if contract.MaxResultRows > 0 {
		w.u64(uint64(contract.MaxResultRows))
	} else {
		w.u64(0)
	}
	if contract.MaxMemoryBytes > 0 {
		w.u64(uint64(contract.MaxMemoryBytes))
	} else {
		w.u64(0)
	}
	w.u32(uint32(len(c.tables)))
	for _, t := range c.tables {
		w.str(t.Alias)
		w.str(t.Prefix)
		w.u32(uint32(len(t.Columns)))
		for _, col := range t.Columns {
			w.str(col.Name)
			w.u8(uint8(col.Type))
			w.str(col.Source)
		}
	}
	// SCAN (table 0).
	w.u8(isaOpScan)
	w.u32(0)
	// JOIN.
	if plan.Join != nil {
		li, err := c.colIdx(plan.Join.LeftCol)
		if err != nil {
			return nil, err
		}
		ri, err := c.colIdx(plan.Join.RightCol)
		if err != nil {
			return nil, err
		}
		w.u8(isaOpJoin)
		w.u32(li)
		w.u32(1)
		w.u32(ri)
	}
	// FILTER.
	if plan.Filter != nil {
		w.u8(isaOpFilter)
		if err := c.encodeExpr(plan.Filter); err != nil {
			return nil, err
		}
	}
	// AGGREGATE.
	if plan.IsAggregate() {
		w.u8(isaOpAggregate)
		w.u32(uint32(len(plan.GroupBy)))
		for _, g := range plan.GroupBy {
			gi, err := c.colIdx(&g)
			if err != nil {
				return nil, err
			}
			w.u32(gi)
		}
		var aggs []ProjItem
		for _, it := range plan.Projection {
			if it.Agg != "" {
				aggs = append(aggs, it)
			}
		}
		w.u32(uint32(len(aggs)))
		for _, it := range aggs {
			var code uint8
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
				return nil, fmt.Errorf("isa: unknown aggregate %q", it.Agg)
			}
			w.u8(code)
			if it.Star {
				w.u8(1)
				w.u32(0)
			} else {
				ci, err := c.colIdx(it.Col)
				if err != nil {
					return nil, err
				}
				w.u8(0)
				w.u32(ci)
			}
		}
	}
	// PROJECT.
	proj, err := c.projectIndices()
	if err != nil {
		return nil, err
	}
	w.u8(isaOpProject)
	w.u32(uint32(len(proj)))
	for _, idx := range proj {
		w.u32(idx)
	}
	// v2: projection output names, in output order (load-bearing
	// for SORT memory accounting: sort-input rows are projected
	// rows, named by OutputName in the Go engine).
	for _, it := range plan.Projection {
		w.str(it.OutputName)
	}
	// SORT (projected positions: ORDER BY resolves against output
	// columns, exactly as the binder does).
	if len(plan.OrderBy) > 0 {
		w.u8(isaOpSort)
		w.u32(uint32(len(plan.OrderBy)))
		for _, o := range plan.OrderBy {
			pos := -1
			for i, it := range plan.Projection {
				if strings.EqualFold(it.OutputName, o.Name) {
					pos = i
					break
				}
			}
			if pos < 0 {
				return nil, fmt.Errorf("isa: ORDER BY %q is not an output column", o.Name)
			}
			w.u32(uint32(pos))
			if o.Desc {
				w.u8(1)
			} else {
				w.u8(0)
			}
		}
	}
	// LIMIT.
	if plan.HasLimit {
		if plan.Limit == nil || *plan.Limit < 0 {
			return nil, fmt.Errorf("isa: plan has invalid LIMIT")
		}
		w.u8(isaOpLimit)
		w.u64(uint64(*plan.Limit))
	}
	w.u8(isaOpEnd)
	return w.b, nil
}

// projectIndices computes the PROJECT operand. For non-aggregate
// plans these are global column indices; for aggregate plans they
// are pipeline indices into the aggregate row (group keys first,
// then aggregate results in projection order among aggregates) —
// see include/query_isa.h under AGGREGATE.
func (c *isaCompiler) projectIndices() ([]uint32, error) {
	plan := c.plan
	var out []uint32
	if !plan.IsAggregate() {
		for _, it := range plan.Projection {
			idx, err := c.colIdx(it.Col)
			if err != nil {
				return nil, err
			}
			out = append(out, idx)
		}
		return out, nil
	}
	aggOrdinal := 0
	for _, it := range plan.Projection {
		if it.Agg != "" {
			out = append(out, uint32(len(plan.GroupBy)+aggOrdinal))
			aggOrdinal++
			continue
		}
		// Plain item: must be one of the GROUP BY columns.
		pos := -1
		for i, g := range plan.GroupBy {
			if g.Alias == it.Col.Alias && g.Name == it.Col.Name {
				pos = i
				break
			}
		}
		if pos < 0 {
			return nil, fmt.Errorf("isa: plain column %q is not in GROUP BY", it.Col.Qualified)
		}
		out = append(out, uint32(pos))
	}
	return out, nil
}

func (c *isaCompiler) encodeValue(v Value) {
	w := &c.w
	w.u8(uint8(v.Kind))
	switch v.Kind {
	case KindNull:
	case KindInt:
		w.i64(v.I)
	case KindFloat:
		w.f64(v.F)
	case KindString:
		w.str(v.S)
	case KindBool:
		if v.BoolVal() {
			w.u8(1)
		} else {
			w.u8(0)
		}
	case KindBytes:
		w.u32(uint32(len(v.B)))
		w.bytes(v.B)
	}
}

func (c *isaCompiler) encodeOperand(e *Expr) error {
	if e == nil {
		return fmt.Errorf("isa: nil expression operand")
	}
	switch e.Kind {
	case "col":
		idx, err := c.colIdx(e.Col)
		if err != nil {
			return err
		}
		c.w.u8(isaExprCol)
		c.w.u32(idx)
		return nil
	case "lit":
		if e.Lit == nil {
			return fmt.Errorf("isa: literal expression without a value")
		}
		c.w.u8(isaExprLit)
		c.encodeValue(*e.Lit)
		return nil
	default:
		return fmt.Errorf("isa: operand kind %q is not a column or literal", e.Kind)
	}
}

func (c *isaCompiler) encodeExpr(e *Expr) error {
	if e == nil {
		return fmt.Errorf("isa: nil expression")
	}
	w := &c.w
	switch e.Kind {
	case "and", "or":
		if e.Kind == "and" {
			w.u8(isaExprAnd)
		} else {
			w.u8(isaExprOr)
		}
		w.u32(uint32(len(e.Args)))
		for _, a := range e.Args {
			if err := c.encodeExpr(a); err != nil {
				return err
			}
		}
		return nil
	case "not":
		if len(e.Args) != 1 {
			return fmt.Errorf("isa: NOT requires exactly one operand")
		}
		w.u8(isaExprNot)
		return c.encodeExpr(e.Args[0])
	case "cmp":
		var code uint8
		switch e.Op {
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
			return fmt.Errorf("isa: unknown comparison operator %q", e.Op)
		}
		w.u8(isaExprCmp)
		w.u8(code)
		if err := c.encodeOperand(e.Left); err != nil {
			return err
		}
		return c.encodeOperand(e.Right)
	case "in":
		w.u8(isaExprIn)
		if err := c.encodeOperand(e.Left); err != nil {
			return err
		}
		if e.Negated {
			w.u8(1)
		} else {
			w.u8(0)
		}
		w.u32(uint32(len(e.InList)))
		for _, v := range e.InList {
			c.encodeValue(v)
		}
		return nil
	case "like":
		w.u8(isaExprLike)
		if err := c.encodeOperand(e.Left); err != nil {
			return err
		}
		if e.Negated {
			w.u8(1)
		} else {
			w.u8(0)
		}
		w.str(e.Pattern)
		return nil
	case "isnull":
		w.u8(isaExprIsNull)
		if err := c.encodeOperand(e.Left); err != nil {
			return err
		}
		if e.Negated {
			w.u8(1)
		} else {
			w.u8(0)
		}
		return nil
	default:
		return fmt.Errorf("isa: unsupported expression kind %q", e.Kind)
	}
}

// ISAErrorClass classifies an execution error for differential
// comparison: "none" (no error), "budget" (BUDGET_EXCEEDED — the
// only error class the reference engine reports with a stable
// code), or "other" (type errors, source errors, ISA errors, ...).
// Both engines' budget failures map to "budget"; everything else
// maps to "other", so a diff test compares classes, not message
// text (which legitimately differs between engines).
func ISAErrorClass(err error) string {
	if err == nil {
		return "none"
	}
	if ve, ok := err.(*VerificationError); ok && ve.Code == "BUDGET_EXCEEDED" {
		return "budget"
	}
	return "other"
}

// ---- wasm host runner ----
//
// Layout (mirrors segdb.go): SegDB state at 32 MiB, then hot arena,
// hot WAL, scratch, heap object store, ISA program buffer, cells,
// ISA work buffer, and finally the growable result buffer.
const (
	isaSegDBBase    uint64 = 33554432 // 32 MiB
	isaSegArenaSize uint64 = 262144
	isaSegWalSize   uint64 = 65536
	isaSegScratch   uint64 = 65536
	// INTEGRATION FIX (Track C regression, same cause as
	// segDBHeapSize): the doubled 8192-slot heap table
	// (1,245,184 bytes) inside a fixed 4 MiB ISA heap left
	// only ~2.95 MB of data room and broke the Gate4
	// over-32MiB test at flush 2749. ISA heaps are
	// ephemeral (never persisted), so no migration is
	// needed here — just headroom.
	isaSegHeapSize uint64 = 8388608
	isaProgCap     uint64 = 4194304
	isaCellsSize   uint64 = 64
	isaWorkSize    uint64 = 33554432 // 32 MiB
	isaOutInit     uint64 = 1048576
	isaOutMax      uint64 = 16777216
	isaScratchKey  uint64 = 0
	isaScratchVal  uint64 = 2048
)

const wasmPageSize = 65536

// ISAHost is one wasm module instance holding one SegDB plus the
// Query ISA engine. It is the differential-test driver: load data
// through Put/Delete/Flush (the SegDB exports), then run compiled
// ISA programs against the same SegDB state.
type ISAHost struct {
	mu       sync.Mutex
	runtime  wazero.Runtime
	module   api.Module
	memory   api.Memory
	memory64 api.Memory64
	isMem64  bool

	stateOff   uint64
	arenaOff   uint64
	walOff     uint64
	scratchOff uint64
	heapOff    uint64
	progOff    uint64
	cellsOff   uint64
	workOff    uint64
	outOff     uint64
	outCap     uint64

	fnStateSize api.Function
	fnInit      api.Function
	fnPut       api.Function
	fnDelete    api.Function
	fnFlush     api.Function
	fnSnapshot  api.Function
	fnVersion   api.Function
	fnExecute   api.Function
	fnExecuteAt api.Function
	// fnExecutePolicy is optional: wasm binaries built before
	// Gate 4 lack yowza_queryisa_execute_policy, and policy
	// execution on them falls back to execute_at when the
	// policy equals the defaults, or fails when it does not.
	fnExecutePolicy api.Function
	// fnCompact is optional (older binaries may lack it).
	fnCompact   api.Function
	fnShardSafe api.Function
}

func isaReadUleb(b []byte, off *int) (uint64, bool) {
	var result uint64
	var shift uint
	for {
		if *off >= len(b) || shift >= 64 {
			return 0, false
		}
		c := b[*off]
		*off++
		result |= uint64(c&0x7F) << shift
		if c&0x80 == 0 {
			return result, true
		}
		shift += 7
	}
}

// isaWasmIsMemory64 reports whether a wasm binary declares a
// memory64 memory (memory section, limits flag bit 2). It mirrors
// the root package's detector; it is duplicated here because the
// root helper is unexported.
func isaWasmIsMemory64(wasmBytes []byte) bool {
	if len(wasmBytes) < 8 || wasmBytes[0] != 0x00 || wasmBytes[1] != 'a' ||
		wasmBytes[2] != 's' || wasmBytes[3] != 'm' {
		return false
	}
	off := 8
	for off < len(wasmBytes) {
		sectionID, ok := isaReadUleb(wasmBytes, &off)
		if !ok {
			return false
		}
		sectionSize, ok := isaReadUleb(wasmBytes, &off)
		if !ok || uint64(len(wasmBytes)-off) < sectionSize {
			return false
		}
		sectionEnd := off + int(sectionSize)
		if sectionID == 5 {
			inner := off
			count, ok := isaReadUleb(wasmBytes, &inner)
			if !ok {
				return false
			}
			for i := uint64(0); i < count; i++ {
				flags, ok := isaReadUleb(wasmBytes, &inner)
				if !ok {
					return false
				}
				if flags&0x04 != 0 {
					return true
				}
				if _, ok := isaReadUleb(wasmBytes, &inner); !ok {
					return false
				}
				if flags&0x01 != 0 {
					if _, ok := isaReadUleb(wasmBytes, &inner); !ok {
						return false
					}
				}
			}
			return false
		}
		off = sectionEnd
	}
	return false
}

// NewISAHost instantiates a wasm module and initialises one SegDB
// inside it, ready for Put/Delete/Flush and ISA execution.
func NewISAHost(wasmBytes []byte) (*ISAHost, error) {
	ctx := context.Background()
	features := api.CoreFeaturesV2.SetEnabled(api.CoreFeatureMemory64, true)
	r := wazero.NewRuntimeWithConfig(ctx, wazero.NewRuntimeConfig().WithCoreFeatures(features))
	if _, err := r.NewHostModuleBuilder("env").Instantiate(ctx); err != nil {
		r.Close(ctx)
		return nil, fmt.Errorf("isa host: instantiate env: %v", err)
	}
	mod, err := r.Instantiate(ctx, wasmBytes)
	if err != nil {
		r.Close(ctx)
		return nil, fmt.Errorf("isa host: instantiate wasm: %v", err)
	}
	mem := mod.Memory()
	if mem == nil {
		r.Close(ctx)
		return nil, fmt.Errorf("isa host: wasm module does not export memory")
	}
	mem64 := mod.Memory64()
	isMem64 := mem64 != nil
	if !isMem64 && isaWasmIsMemory64(wasmBytes) {
		r.Close(ctx)
		return nil, fmt.Errorf("isa host: wasm declares memory64 but Memory64() returned nil")
	}
	h := &ISAHost{runtime: r, module: mod, memory: mem, memory64: mem64, isMem64: isMem64}
	get := func(name string) api.Function { return mod.ExportedFunction(name) }
	h.fnStateSize = get("yowza_segdb_state_size")
	h.fnInit = get("yowza_segdb_init")
	h.fnPut = get("yowza_segdb_put")
	h.fnDelete = get("yowza_segdb_delete")
	h.fnFlush = get("yowza_segdb_flush")
	h.fnSnapshot = get("yowza_segdb_snapshot_seq")
	h.fnVersion = get("yowza_queryisa_version")
	h.fnExecute = get("yowza_queryisa_execute")
	h.fnExecuteAt = get("yowza_queryisa_execute_at")
	h.fnExecutePolicy = get("yowza_queryisa_execute_policy")
	h.fnCompact = get("yowza_segdb_compact")
	h.fnShardSafe = get("yowza_queryisa_shard_safe")
	for name, fn := range map[string]api.Function{
		"yowza_segdb_state_size": h.fnStateSize, "yowza_segdb_init": h.fnInit,
		"yowza_segdb_put": h.fnPut, "yowza_segdb_delete": h.fnDelete,
		"yowza_segdb_flush": h.fnFlush, "yowza_segdb_snapshot_seq": h.fnSnapshot,
		"yowza_queryisa_version":    h.fnVersion,
		"yowza_queryisa_execute":    h.fnExecute,
		"yowza_queryisa_execute_at": h.fnExecuteAt,
		"yowza_queryisa_shard_safe": h.fnShardSafe,
	} {
		if fn == nil {
			r.Close(ctx)
			return nil, fmt.Errorf("isa host: wasm binary predates ISA exports (missing %s)", name)
		}
	}
	ver, err := h.call(h.fnVersion)
	if err != nil {
		r.Close(ctx)
		return nil, fmt.Errorf("isa host: queryisa version call: %v", err)
	}
	if ver != ISAVersion {
		r.Close(ctx)
		return nil, fmt.Errorf("isa host: wasm ISA version %d != host %d", ver, ISAVersion)
	}
	res, err := h.fnStateSize.Call(ctx)
	if err != nil || len(res) == 0 {
		r.Close(ctx)
		return nil, fmt.Errorf("isa host: state_size call failed: %v", err)
	}
	stateSize := (res[0] + 65535) &^ 65535
	stateSize += 65536
	h.stateOff = isaSegDBBase
	h.arenaOff = h.stateOff + stateSize
	h.walOff = h.arenaOff + isaSegArenaSize
	h.scratchOff = h.walOff + isaSegWalSize
	h.heapOff = h.scratchOff + isaSegScratch
	h.progOff = h.heapOff + isaSegHeapSize
	h.cellsOff = h.progOff + isaProgCap
	h.workOff = h.cellsOff + isaCellsSize
	h.outOff = h.workOff + isaWorkSize
	h.outCap = isaOutInit
	if err := h.memGrowTo(h.outOff + h.outCap); err != nil {
		r.Close(ctx)
		return nil, fmt.Errorf("isa host: grow memory: %v", err)
	}
	rc, err := h.call(h.fnInit, h.stateOff, h.arenaOff, isaSegArenaSize,
		h.walOff, isaSegWalSize, h.heapOff, isaSegHeapSize)
	if err != nil || rc != 0 {
		r.Close(ctx)
		return nil, fmt.Errorf("isa host: segdb init failed: %v (rc=%d)", err, rc)
	}
	return h, nil
}

func (h *ISAHost) call(fn api.Function, args ...uint64) (int32, error) {
	res, err := fn.Call(context.Background(), args...)
	if err != nil {
		return 0, err
	}
	if len(res) == 0 {
		return 0, fmt.Errorf("isa host: empty call result")
	}
	return int32(uint32(res[0])), nil
}

func (h *ISAHost) memSize() uint64 {
	if h.isMem64 && h.memory64 != nil {
		return h.memory64.Size64()
	}
	return uint64(h.memory.Size())
}

func (h *ISAHost) memGrowTo(target uint64) error {
	size := h.memSize()
	if size >= target {
		return nil
	}
	need := (target - size + wasmPageSize - 1) / wasmPageSize
	if h.isMem64 && h.memory64 != nil {
		if _, ok := h.memory64.Grow64(need); !ok {
			return fmt.Errorf("isa host: failed to grow wasm memory to %d", target)
		}
	} else {
		if need > 0xFFFFFFFF {
			return fmt.Errorf("isa host: growth too large")
		}
		if _, ok := h.memory.Grow(uint32(need)); !ok {
			return fmt.Errorf("isa host: failed to grow wasm memory to %d", target)
		}
	}
	if h.memSize() < target {
		return fmt.Errorf("isa host: memory growth accepted but still below %d", target)
	}
	return nil
}

func (h *ISAHost) memRead(offset, n uint64) ([]byte, bool) {
	if h.isMem64 && h.memory64 != nil {
		return h.memory64.Read64(offset, n)
	}
	if offset > 0xFFFFFFFF || n > 0xFFFFFFFF {
		return nil, false
	}
	return h.memory.Read(uint32(offset), uint32(n))
}

func (h *ISAHost) memWrite(offset uint64, val []byte) bool {
	if h.isMem64 && h.memory64 != nil {
		return h.memory64.Write64(offset, val)
	}
	if offset > 0xFFFFFFFF {
		return false
	}
	return h.memory.Write(uint32(offset), val)
}

// Close shuts down the runtime. Callers must not use the host after
// Close; like the root package, calls are serialized by mu because
// the wasm module is not thread-safe.
func (h *ISAHost) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.runtime.Close(context.Background())
}

// Put stores key/value in the host SegDB's hot layer.
func (h *ISAHost) Put(key string, val []byte) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(key) == 0 || len(key) > 1023 {
		return fmt.Errorf("isa host: key length %d out of range 1..1023", len(key))
	}
	if len(val) > 1024 {
		return fmt.Errorf("isa host: value length %d exceeds 1024", len(val))
	}
	if !h.memWrite(h.scratchOff+isaScratchKey, []byte(key)) {
		return fmt.Errorf("isa host: failed to write key")
	}
	if !h.memWrite(h.scratchOff+isaScratchVal, val) {
		return fmt.Errorf("isa host: failed to write value")
	}
	rc, err := h.call(h.fnPut, h.stateOff, h.scratchOff+isaScratchKey,
		uint64(len(key)), h.scratchOff+isaScratchVal, uint64(len(val)))
	if err != nil {
		return err
	}
	if rc != 0 {
		return fmt.Errorf("isa host: segdb put failed (rc=%d)", rc)
	}
	return nil
}

// Delete removes key from the host SegDB (hot tombstone).
func (h *ISAHost) Delete(key string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(key) == 0 || len(key) > 1023 {
		return fmt.Errorf("isa host: key length %d out of range 1..1023", len(key))
	}
	if !h.memWrite(h.scratchOff+isaScratchKey, []byte(key)) {
		return fmt.Errorf("isa host: failed to write key")
	}
	rc, err := h.call(h.fnDelete, h.stateOff, h.scratchOff+isaScratchKey, uint64(len(key)))
	if err != nil {
		return err
	}
	if rc != 0 {
		return fmt.Errorf("isa host: segdb delete failed (rc=%d)", rc)
	}
	return nil
}

// Compact merges the host SegDB's segments (streaming, in
// wasm) and drops tombstones with commit sequence at or
// below barrier. It is an optimisation, never a logical
// operation: with no deletes and no snapshots held across
// the call, it only reduces the segment count. Callers
// loading many small flushes (e.g. a 64 KiB WAL that
// forces a flush every ~50 large rows) need it to stay
// under the cursor scan's 128-source cap. It fails cleanly
// when the wasm binary predates the export.
func (h *ISAHost) Compact(barrier uint64) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.fnCompact == nil {
		return fmt.Errorf("isa host: wasm binary predates yowza_segdb_compact")
	}
	rc, err := h.call(h.fnCompact, h.stateOff, barrier)
	if err != nil {
		return err
	}
	if rc != 0 {
		return fmt.Errorf("isa host: segdb compact failed (rc=%d)", rc)
	}
	return nil
}

// Flush publishes the hot layer as one immutable segment.
func (h *ISAHost) Flush() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	rc, err := h.call(h.fnFlush, h.stateOff)
	if err != nil {
		return err
	}
	if rc != 0 {
		return fmt.Errorf("isa host: segdb flush failed (rc=%d)", rc)
	}
	return nil
}

// ISATrace is the ISA engine's execution trace, in the same
// vocabulary as query.Trace for the fields the ISA engine can
// report: scanned rows, measured retained-memory peak (Go
// accounting, see include/query_isa.h), and whether any retaining
// operator spilled. On error, Scanned / ActualMemoryBytes /
// Spilled still describe how far execution got (the Go trace does
// the same), while Rows is nil.
type ISATrace struct {
	ScannedRows       int64
	ActualMemoryBytes int64
	Spilled           bool
}

// callU64 calls a wasm function returning a full u64 (snapshot
// sequences must not be truncated to 32 bits).
func (h *ISAHost) callU64(fn api.Function, args ...uint64) (uint64, error) {
	res, err := fn.Call(context.Background(), args...)
	if err != nil {
		return 0, err
	}
	if len(res) == 0 {
		return 0, fmt.Errorf("isa host: empty call result")
	}
	return res[0], nil
}

// Snapshot returns the host SegDB's current commit sequence: the
// one sequence a query execution reads every table at. Callers
// take it once at query start and pass it to ExecuteISAAt; the
// engine then reads both tables at that literal sequence.
func (h *ISAHost) Snapshot() (uint64, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.callU64(h.fnSnapshot, h.stateOff)
}

// ExecuteISA runs a compiled ISA program against the host SegDB and
// returns the output rows (with column names from the plan's
// projection), the scanned-row count, and an error. It takes one
// snapshot at query start and executes at that snapshot. On any
// error the rows are nil: partial results are never returned.
// The output buffer grows (and the program re-executes, which is
// safe because execution is read-only and deterministic) when the
// C engine reports OUT_SMALL.
func (h *ISAHost) ExecuteISA(program []byte, plan *Plan) ([]Row, int64, error) {
	rows, tr, err := h.ExecuteISATrace(program, plan)
	if err != nil {
		return nil, 0, err
	}
	return rows, tr.ScannedRows, nil
}

// ExecuteISAAt runs a compiled ISA program at an explicit snapshot
// sequence (taken earlier with Snapshot). Writes committed after
// that sequence are invisible to every table of the query. This
// is the shared-snapshot entry point: it is how a test proves
// that a write landing between the two table reads of one query
// cannot leak into the second table.
func (h *ISAHost) ExecuteISAAt(program []byte, plan *Plan, snapshot uint64) ([]Row, ISATrace, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.executeISALocked(program, plan, snapshot)
}

// ExecuteISATrace runs a compiled ISA program at a fresh snapshot
// and returns the rows plus the full ISA trace (scanned rows,
// measured memory peak, spilled flag).
func (h *ISAHost) ExecuteISATrace(program []byte, plan *Plan) ([]Row, ISATrace, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	snap, err := h.callU64(h.fnSnapshot, h.stateOff)
	if err != nil {
		return nil, ISATrace{}, fmt.Errorf("isa host: snapshot call failed: %v", err)
	}
	return h.executeISALocked(program, plan, snap)
}

// ExecuteISATraceWithPolicy runs a compiled ISA program at a
// fresh snapshot under a ResourcePolicy (Gate 4 §5b): the
// policy's caps reach the C engine through the
// yowza_queryisa_execute_policy export when the wasm binary
// provides it. With a nil policy, or a policy equal to the
// defaults on an older binary, execution uses execute_at.
// A non-default policy on a binary without the export is a
// hard error, never a silent run under different caps.
func (h *ISAHost) ExecuteISATraceWithPolicy(program []byte, plan *Plan, policy *ResourcePolicy) ([]Row, ISATrace, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	snap, err := h.callU64(h.fnSnapshot, h.stateOff)
	if err != nil {
		return nil, ISATrace{}, fmt.Errorf("isa host: snapshot call failed: %v", err)
	}
	return h.executeISALockedPolicy(program, plan, snap, policy)
}

// ExecuteISAAtWithPolicy runs a compiled ISA program at an
// explicit snapshot under a ResourcePolicy (see
// ExecuteISATraceWithPolicy).
func (h *ISAHost) ExecuteISAAtWithPolicy(program []byte, plan *Plan, snapshot uint64, policy *ResourcePolicy) ([]Row, ISATrace, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.executeISALockedPolicy(program, plan, snapshot, policy)
}

// executeISALocked runs program at snapshot; the caller holds mu.
// Cells layout: out_len u32 @ +0, scanned u64 @ +8,
// mem_peak u64 @ +16, spilled u32 @ +24.
func (h *ISAHost) executeISALocked(program []byte, plan *Plan, snapshot uint64) ([]Row, ISATrace, error) {
	return h.executeISALockedPolicy(program, plan, snapshot, nil)
}

// executeISALockedPolicy is executeISALocked with an optional
// ResourcePolicy. A nil policy means the C engine defaults.
func (h *ISAHost) executeISALockedPolicy(program []byte, plan *Plan, snapshot uint64, policy *ResourcePolicy) ([]Row, ISATrace, error) {
	if uint64(len(program)) > isaProgCap {
		return nil, ISATrace{}, fmt.Errorf("isa host: program %d bytes exceeds program buffer %d", len(program), isaProgCap)
	}
	if !h.memWrite(h.progOff, program) {
		return nil, ISATrace{}, fmt.Errorf("isa host: failed to write program")
	}
	usePolicy := false
	var pw, pj, ps, pg uint32
	if policy != nil {
		if err := policy.Validate(); err != nil {
			return nil, ISATrace{}, err
		}
		pw, pj, ps, pg = policy.CParams()
		dw, dj, ds, dg := DefaultResourcePolicy().CParams()
		if pw != dw || pj != dj || ps != ds || pg != dg {
			// Non-default C caps: needs the policy export.
			if h.fnExecutePolicy == nil {
				return nil, ISATrace{}, fmt.Errorf("isa host: wasm binary predates the policy export (yowza_queryisa_execute_policy); rebuild with make all")
			}
			usePolicy = true
		}
	}
	for {
		var rc int32
		var err error
		if usePolicy {
			rc, err = h.call(h.fnExecutePolicy, h.stateOff, h.progOff, uint64(len(program)),
				h.outOff, h.outCap, h.cellsOff, h.cellsOff+8, h.cellsOff+16, h.cellsOff+24,
				snapshot, h.workOff, isaWorkSize, uint64(pw), uint64(pj), uint64(ps), uint64(pg))
		} else {
			rc, err = h.call(h.fnExecuteAt, h.stateOff, h.progOff, uint64(len(program)),
				h.outOff, h.outCap, h.cellsOff, h.cellsOff+8, h.cellsOff+16, h.cellsOff+24,
				snapshot, h.workOff, isaWorkSize)
		}
		if err != nil {
			return nil, ISATrace{}, fmt.Errorf("isa host: execute call failed: %v", err)
		}
		trace := h.readISATrace()
		switch rc {
		case 0:
			// Success: decode below.
		case -6: // OUT_SMALL: grow and retry.
			if h.outCap >= isaOutMax {
				return nil, trace, fmt.Errorf("isa host: result exceeds maximum output buffer %d", isaOutMax)
			}
			newCap := h.outCap * 2
			if newCap > isaOutMax {
				newCap = isaOutMax
			}
			if err := h.memGrowTo(h.outOff + newCap); err != nil {
				return nil, trace, fmt.Errorf("isa host: grow output buffer: %v", err)
			}
			h.outCap = newCap
			continue
		case -2:
			return nil, trace, fmt.Errorf("isa: unsupported ISA version (rc=%d)", rc)
		case -4:
			return nil, trace, &VerificationError{Code: "BUDGET_EXCEEDED", Msg: "isa engine: budget exceeded"}
		case -1:
			return nil, trace, fmt.Errorf("isa: malformed program (rc=%d)", rc)
		case -3:
			return nil, trace, fmt.Errorf("isa: type error (rc=%d)", rc)
		case -5:
			return nil, trace, fmt.Errorf("isa: out of memory / resource cap (rc=%d)", rc)
		case -7:
			return nil, trace, fmt.Errorf("isa: segdb scan failed (rc=%d)", rc)
		case -8:
			return nil, trace, fmt.Errorf("isa: %w (rc=%d)", ErrNumericOverflow, rc)
		case -9:
			return nil, trace, fmt.Errorf("isa: %w (rc=%d)", ErrResourceLimit, rc)
		default:
			return nil, trace, fmt.Errorf("isa: engine failed (rc=%d)", rc)
		}
		lenBytes, ok := h.memRead(h.cellsOff, 4)
		if !ok {
			return nil, trace, fmt.Errorf("isa host: failed to read output length")
		}
		outLen := binary.LittleEndian.Uint32(lenBytes)
		if uint64(outLen) > h.outCap {
			return nil, trace, fmt.Errorf("isa host: output length %d exceeds buffer %d", outLen, h.outCap)
		}
		raw, ok := h.memRead(h.outOff, uint64(outLen))
		if !ok {
			return nil, trace, fmt.Errorf("isa host: failed to read output")
		}
		rows, err := decodeISAResult(raw, plan)
		if err != nil {
			return nil, trace, err
		}
		return rows, trace, nil
	}
}

func (h *ISAHost) readISATrace() ISATrace {
	var tr ISATrace
	if b, ok := h.memRead(h.cellsOff+8, 8); ok {
		tr.ScannedRows = int64(binary.LittleEndian.Uint64(b))
	}
	if b, ok := h.memRead(h.cellsOff+16, 8); ok {
		tr.ActualMemoryBytes = int64(binary.LittleEndian.Uint64(b))
	}
	if b, ok := h.memRead(h.cellsOff+24, 4); ok {
		tr.Spilled = binary.LittleEndian.Uint32(b) != 0
	}
	return tr
}

// decodeISAResult parses the C engine's result encoding
// ([count u32][rows of value-encoded cells]) and reattaches the
// plan's projection output names.
func decodeISAResult(raw []byte, plan *Plan) ([]Row, error) {
	if len(raw) < 4 {
		return nil, fmt.Errorf("isa: result too short (%d bytes)", len(raw))
	}
	count := binary.LittleEndian.Uint32(raw[:4])
	pos := 4
	ncol := len(plan.Projection)
	rows := make([]Row, 0, count)
	for r := uint32(0); r < count; r++ {
		cols := make([]string, ncol)
		vals := make([]Value, ncol)
		for c := 0; c < ncol; c++ {
			cols[c] = plan.Projection[c].OutputName
			v, n, err := decodeISAValue(raw, pos)
			if err != nil {
				return nil, err
			}
			vals[c] = v
			pos = n
		}
		rows = append(rows, Row{Cols: cols, Vals: vals})
	}
	if pos != len(raw) {
		return nil, fmt.Errorf("isa: result has %d trailing bytes", len(raw)-pos)
	}
	return rows, nil
}

func decodeISAValue(raw []byte, pos int) (Value, int, error) {
	if pos >= len(raw) {
		return Null(), 0, fmt.Errorf("isa: truncated result value")
	}
	kind := raw[pos]
	pos++
	switch kind {
	case 0:
		return Null(), pos, nil
	case 1:
		if pos+8 > len(raw) {
			return Null(), 0, fmt.Errorf("isa: truncated int value")
		}
		return Int(int64(binary.LittleEndian.Uint64(raw[pos:]))), pos + 8, nil
	case 2:
		if pos+8 > len(raw) {
			return Null(), 0, fmt.Errorf("isa: truncated float value")
		}
		return Float(math.Float64frombits(binary.LittleEndian.Uint64(raw[pos:]))), pos + 8, nil
	case 3, 5:
		if pos+4 > len(raw) {
			return Null(), 0, fmt.Errorf("isa: truncated string/bytes length")
		}
		n := int(binary.LittleEndian.Uint32(raw[pos:]))
		pos += 4
		if n < 0 || pos+n > len(raw) {
			return Null(), 0, fmt.Errorf("isa: truncated string/bytes value")
		}
		b := make([]byte, n)
		copy(b, raw[pos:pos+n])
		pos += n
		if kind == 3 {
			return String(string(b)), pos, nil
		}
		return Bytes(b), pos, nil
	case 4:
		if pos+1 > len(raw) {
			return Null(), 0, fmt.Errorf("isa: truncated bool value")
		}
		return Bool(raw[pos] != 0), pos + 1, nil
	default:
		return Null(), 0, fmt.Errorf("isa: unknown value kind %d", kind)
	}
}

// ---------------------------------------------------------------------------
// ISA shard coordinator (Reliability stage item 04, coordinator
// input only: this section adds host-side coordination and one
// engine-side classification export; it does not change the ISA
// engine's execution path, the wire format, or the unified
// contract from item 02).
//
// PROBLEM. One ISA engine instance runs inside one wasm module
// and reads exactly one SegDB. It cannot read another shard's
// SegDB, so a multi-shard ISA execution is necessarily a
// host-side coordination over one ISAHost per shard. Most
// operators do NOT distribute by concatenation:
//   - JOIN: a match may join rows from different shards, so
//     per-shard join + concatenation silently drops
//     cross-shard matches. REFUSED (see below).
//   - AGGREGATE: groups split across shards would be counted
//     once per shard, not once globally. REFUSED.
//   - SORT: concatenating per-shard sorted chunks is not
//     globally sorted. REFUSED (the Go executor path sorts
//     globally; use it).
//   - LIMIT: a per-shard LIMIT over-returns. REFUSED.
// What DOES distribute by concatenation is a program of SCAN +
// FILTER + PROJECT only: shards partition keys disjointly, so
// the union of per-shard filtered projections is exactly the
// whole-table result, in shard order. The coordinator executes
// exactly that subset and refuses everything else with a
// checkable error, before executing anything.
//   - The JOIN refusal is enforced twice: by the plan check
//     here (plan.Join != nil) and by the C engine's
//     yowza_queryisa_shard_safe classification (which parses
//     the program with the engine's own parser, so a program
//     whose bytes contain a JOIN cannot slip past a stale or
//     hand-built plan). A caller that needs a JOIN, an
//     aggregate, a sort, or a limit over sharded storage must
//     use the Go executor over PersistedMultiShardSource,
//     which handles all operators correctly (broadcast-build
//     join; see query/segdb_source.go).
// SNAPSHOT. ExecuteAt takes one snapshot per host, supplied by
// the caller: the caller is responsible for the vector being a
// coherent snapshot (e.g. taken while writes are quiesced, or
// one vector per persisted database). There is no atomic
// cross-host snapshot primitive for independent ISAHost
// modules, and Execute (which snapshots each host in turn)
// documents that its snapshots may straddle a concurrent
// write. Neither entry point pretends otherwise.
// DISJOINTNESS. The coordinator cannot verify from inside the
// engines that two hosts hold disjoint key sets; concatenation
// is correct only when they do (the persisted-storage case,
// where hash routing guarantees it). Supplying overlapping
// hosts is a caller error and duplicates rows — the
// coordinator documents this instead of claiming a check it
// does not perform.

// ErrISACrossShardJoin is returned when a coordinator is asked
// to run a JOIN program across shards. A cross-shard JOIN must
// be executed by the Go executor (broadcast-build), never by
// per-shard ISA engines.
var ErrISACrossShardJoin = errors.New("isa shard coordinator: JOIN program cannot be executed per-shard (cross-shard matches would be dropped); use the Go executor over PersistedMultiShardSource")

// ErrISAProgramNotShardPartitionable is returned when a
// coordinator is asked to run a program whose operators do not
// distribute by concatenation (AGGREGATE, SORT, or LIMIT).
var ErrISAProgramNotShardPartitionable = errors.New("isa shard coordinator: program contains AGGREGATE, SORT, or LIMIT, which do not distribute by per-shard concatenation; use the Go executor over PersistedMultiShardSource")

// ShardSafe reports whether a program contains a JOIN op,
// classified by the C engine's own program parser
// (yowza_queryisa_shard_safe): true means no JOIN (the
// program is shard-safe in the JOIN sense — it may still be
// refused by the coordinator for AGGREGATE/SORT/LIMIT), false
// means it contains a JOIN. A malformed program or version
// mismatch is an error, not a boolean.
func (h *ISAHost) ShardSafe(program []byte) (bool, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if uint64(len(program)) > isaProgCap {
		return false, fmt.Errorf("isa host: program %d bytes exceeds program buffer %d", len(program), isaProgCap)
	}
	if !h.memWrite(h.progOff, program) {
		return false, fmt.Errorf("isa host: failed to write program")
	}
	rc, err := h.call(h.fnShardSafe, h.progOff, uint64(len(program)), h.workOff, isaWorkSize)
	if err != nil {
		return false, fmt.Errorf("isa host: shard-safe call failed: %v", err)
	}
	switch {
	case rc == 1:
		return true, nil
	case rc == 0:
		return false, nil
	case rc == -2:
		return false, fmt.Errorf("isa: unsupported ISA version (rc=%d)", rc)
	case rc == -5:
		return false, fmt.Errorf("isa: out of memory / resource cap (rc=%d)", rc)
	default:
		return false, fmt.Errorf("isa: malformed program for shard-safe check (rc=%d)", rc)
	}
}

// ISAShardCoordinator executes shard-partitionable ISA programs
// (SCAN + FILTER + PROJECT only) across one ISAHost per shard
// and concatenates the per-shard results in host order.
type ISAShardCoordinator struct {
	hosts []*ISAHost
}

// NewISAShardCoordinator builds a coordinator over the given
// hosts (one per shard, in shard order). It refuses an empty
// or nil host list: a coordinator over zero shards would
// silently return empty results for every query.
func NewISAShardCoordinator(hosts ...*ISAHost) (*ISAShardCoordinator, error) {
	if len(hosts) == 0 {
		return nil, fmt.Errorf("isa shard coordinator: no hosts")
	}
	for i, h := range hosts {
		if h == nil {
			return nil, fmt.Errorf("isa shard coordinator: host %d is nil", i)
		}
	}
	return &ISAShardCoordinator{hosts: append([]*ISAHost(nil), hosts...)}, nil
}

// NumHosts returns the shard (host) count.
func (c *ISAShardCoordinator) NumHosts() int { return len(c.hosts) }

// ExecuteAt runs program on every host at its supplied snapshot
// and returns the concatenated rows (host order) plus one trace
// per host, in host order. It executes nothing when it refuses:
// the JOIN / AGGREGATE / SORT / LIMIT checks and the C
// shard-safe classification all run before the first host
// execution. Snapshots must have one entry per host.
func (c *ISAShardCoordinator) ExecuteAt(program []byte, plan *Plan, snapshots []uint64) ([]Row, []ISATrace, error) {
	if c == nil || len(c.hosts) == 0 {
		return nil, nil, fmt.Errorf("isa shard coordinator: no hosts")
	}
	if plan == nil {
		return nil, nil, fmt.Errorf("isa shard coordinator: nil plan")
	}
	if len(snapshots) != len(c.hosts) {
		return nil, nil, fmt.Errorf("isa shard coordinator: %d snapshots for %d hosts", len(snapshots), len(c.hosts))
	}
	if plan.Join != nil {
		return nil, nil, ErrISACrossShardJoin
	}
	if plan.IsAggregate() || len(plan.OrderBy) > 0 || plan.HasLimit {
		return nil, nil, ErrISAProgramNotShardPartitionable
	}
	for i, h := range c.hosts {
		safe, err := h.ShardSafe(program)
		if err != nil {
			return nil, nil, fmt.Errorf("isa shard coordinator: host %d shard-safe check: %w", i, err)
		}
		if !safe {
			return nil, nil, ErrISACrossShardJoin
		}
	}
	var rows []Row
	traces := make([]ISATrace, 0, len(c.hosts))
	for i, h := range c.hosts {
		hostRows, tr, err := h.ExecuteISAAt(program, plan, snapshots[i])
		if err != nil {
			return nil, nil, fmt.Errorf("isa shard coordinator: host %d: %w", i, err)
		}
		rows = append(rows, hostRows...)
		traces = append(traces, tr)
	}
	return rows, traces, nil
}

// Execute snapshots each host in turn and runs ExecuteAt with
// those snapshots. The snapshots are taken sequentially, so a
// write concurrent with this call may be visible in some hosts
// and not others; callers that need a coherent cross-host
// snapshot must quiesce writes and pass an explicit vector to
// ExecuteAt.
func (c *ISAShardCoordinator) Execute(program []byte, plan *Plan) ([]Row, []ISATrace, error) {
	if c == nil || len(c.hosts) == 0 {
		return nil, nil, fmt.Errorf("isa shard coordinator: no hosts")
	}
	snapshots := make([]uint64, len(c.hosts))
	for i, h := range c.hosts {
		snap, err := h.Snapshot()
		if err != nil {
			return nil, nil, fmt.Errorf("isa shard coordinator: host %d snapshot: %w", i, err)
		}
		snapshots[i] = snap
	}
	return c.ExecuteAt(program, plan, snapshots)
}
