// ISA execution over multi-shard storage (Gate 4.2) with host
// spill (Gate 4.1) and consistency sessions (Gate 4.3).
//
// WHAT THE OLD COORDINATOR REFUSED. ISAShardCoordinator
// (query/isa.go) runs only SCAN+FILTER+PROJECT per shard and
// refuses JOIN, AGGREGATE, SORT, and LIMIT, because none of
// those distribute by per-shard concatenation: per-shard joins
// drop cross-shard matches, per-shard aggregates count groups
// once per shard, per-shard sorts are not globally sorted,
// and per-shard limits over-return. Those refusals were
// correct — and they are still the behaviour for callers
// that use that coordinator.
//
// WHAT THIS COORDINATOR DOES INSTEAD. It decomposes the plan
// and runs the global operators ONCE, on the host, over rows
// the ISA engines produced:
//   - SCAN (+FILTER when the whole plan is scan/filter/
//     project): the original program runs per shard, in the
//     ISA engines, and results concatenate in shard order.
//   - JOIN: both tables are scanned per shard through the
//     ISA engines (scan-only programs); the host builds ONE
//     global hash table over all shards (broadcast-build)
//     and probes it with the streamed left table. When the
//     build exceeds the policy (row cap or memory budget)
//     it spills: the build is chunked, matches are written
//     to spill runs tagged with (left sequence, right
//     ordinal), and a k-way merge restores exact Go order
//     (left order, then right order within a left row).
//     Per-shard joins are never concatenated.
//   - AGGREGATE: groups are accumulated ONCE, globally, in
//     first-appearance order over the global pipeline
//     stream — never once per shard. When retained groups
//     exceed the memory budget they spill to disk as
//     partials and merge back at the end, deduplicated by
//     group key (the merge is exact: a group that repeats
//     after a spill boundary is merged once, not counted
//     twice).
//   - SORT: per-shard ISA sorts are merged by a k-way
//     sorted-run merge (stable, NULLs last, ties by shard
//     order then original order); when a shard's sort would
//     exceed its work buffer the shard falls back to a
//     host spill sort (chunk-sort + k-way merge). Either
//     way the result is globally sorted.
//   - LIMIT: applied once, after global semantics — never
//     per shard.
//   - FILTER (in decomposed paths) is evaluated by the
//     reference three-valued evaluator (evalTri, same
//     package), which is the semantics the C engine also
//     implements; the scan-only fast path keeps the filter
//     inside the C engine.
//
// PARTITION CONTRACT. A coordinator over hosts that do not
// partition keys disjointly cannot concatenate safely. The
// caller supplies PartitionMetadata; when it is not verified
// disjoint, the default mode (ModeReject) refuses before
// executing anything, with ErrUnclearPartition. ModeDedup
// instead deduplicates exact duplicate rows across hosts
// and refuses with ErrPartitionConflict when the same key
// column carries different rows (replication conflict) —
// deduplication is correct only for identical replicas, and
// that limitation is stated, not hidden.
// HOST SPILL (Gate 4.1). Every retaining operator above
// spills through SpillManager (query/hostspill.go) under
// the policy's quota. Spill files are cleaned up even on
// error or cancellation (per-query manager, deferred
// Close). A query whose working set exceeds the 32 MiB C
// work buffer succeeds here when its per-shard scans fit
// and its global operators spill; the trace records peak
// memory and spill occurrence. ExecuteISAWithHostSpill is
// the single-host entry point: it tries the monolithic C
// engine first and, ONLY on a genuine resource failure
// (OOM / operator-cap limit, never on a budget refusal,
// type error, or malformed program), falls back to this
// coordinator — so a >32 MiB query is equal in rows,
// semantics, and error class to the Go reference, with a
// trace that says the fallback happened.
// SNAPSHOTS (Gate 4.3). Execute takes one snapshot per
// host; ExecuteWithSession takes a ConsistencySession and
// derives each host's snapshot from it, refusing queries
// whose participants are not all in that session.
package query

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
)

// ErrResourceLimit is returned when a configured resource
// policy's hard cap is exceeded (pipeline width, or a
// retaining operator in hard-cap mode), or when the C
// engine reports QISA_ERR_LIMIT. It names the limit; it is
// not a data error and not a silent truncation.
var ErrResourceLimit = errors.New("query: resource policy limit exceeded")

// ErrUnclearPartition is returned when a multi-shard query
// is attempted over hosts whose partition contract is not
// verified disjoint and the coordinator is in ModeReject.
// The query executes nothing in this case.
var ErrUnclearPartition = errors.New("query: multi-shard partition contract is not verified disjoint; refusing distributed execution (use ModeDedup for replicated hosts, or supply verified partition metadata)")

// ErrPartitionConflict is returned in ModeDedup when two
// hosts hold different rows for the same key column: the
// hosts disagree, so no deduplication can be correct, and
// the query fails with no rows rather than picking one
// copy silently.
var ErrPartitionConflict = errors.New("query: partition conflict: same key with different rows on different shards")

// ResourcePolicy turns the old fixed operator caps
// (pipeline width 64, JOIN/SORT rows 262144, groups 65536)
// into configuration. Semantics, exactly:
//   - A zero value means the old C engine default for that
//     field (width 64, join/sort rows 262144, groups
//     65536, spill quota DefaultSpillQuotaBytes).
//   - Negative values are rejected by Validate.
//   - In spill mode (HardCaps=false, the default) the row
//     caps are CHUNK caps: a retaining operator holds at
//     most that many rows (or MaxMemoryBytes) in memory
//     and spills the rest to disk. Nothing is truncated.
//   - In hard-cap mode (HardCaps=true) exceeding a row cap
//     returns ErrResourceLimit with no rows.
//   - The only "real" limits that fail a spilling query are
//     the spill quota (ErrSpillQuotaExceeded), the C work
//     buffer for a single shard's scan, and a genuinely
//     failing host operation — each reported typed.
//   - MaxMemoryBytes is the per-operator memory budget in
//     the Go rowSize accounting (the same accounting the C
//     engine uses). 0 = unlimited memory for coordinator
//     operators (spill still applies via row caps).
type ResourcePolicy struct {
	MaxWidth        int
	MaxJoinRows     int64
	MaxSortRows     int64
	MaxGroups       int64
	MaxMemoryBytes  int64
	SpillQuotaBytes int64
	SpillDir        string
	SpillSync       bool
	HardCaps        bool
}

// DefaultResourcePolicy returns the policy equivalent to
// the pre-Gate-4 fixed caps, in spill mode.
func DefaultResourcePolicy() ResourcePolicy {
	return ResourcePolicy{
		MaxWidth:        64,
		MaxJoinRows:     262144,
		MaxSortRows:     262144,
		MaxGroups:       65536,
		SpillQuotaBytes: DefaultSpillQuotaBytes,
	}
}

func (p ResourcePolicy) resolved() ResourcePolicy {
	d := DefaultResourcePolicy()
	if p.MaxWidth == 0 {
		p.MaxWidth = d.MaxWidth
	}
	if p.MaxJoinRows == 0 {
		p.MaxJoinRows = d.MaxJoinRows
	}
	if p.MaxSortRows == 0 {
		p.MaxSortRows = d.MaxSortRows
	}
	if p.MaxGroups == 0 {
		p.MaxGroups = d.MaxGroups
	}
	if p.SpillQuotaBytes == 0 {
		p.SpillQuotaBytes = d.SpillQuotaBytes
	}
	return p
}

// Validate rejects negative policy values. Zero means
// "default" and is always valid.
func (p ResourcePolicy) Validate() error {
	if p.MaxWidth < 0 || p.MaxJoinRows < 0 || p.MaxSortRows < 0 ||
		p.MaxGroups < 0 || p.MaxMemoryBytes < 0 || p.SpillQuotaBytes < 0 {
		return fmt.Errorf("%w: resource policy values must not be negative", ErrResourceLimit)
	}
	return nil
}

// CParams returns the (width, joinRows, sortRows, groups)
// parameters for the C policy entry point. A zero in any
// position means "C default" for that field.
func (p ResourcePolicy) CParams() (width, joinRows, sortRows, groups uint32) {
	r := p.resolved()
	return uint32(r.MaxWidth), uint32(r.MaxJoinRows), uint32(r.MaxSortRows), uint32(r.MaxGroups)
}

// PartitionMetadata describes how keys are distributed over
// the coordinator's hosts. Disjoint+Verified is the only
// combination that licenses concatenation without
// deduplication: it asserts the caller verified (e.g. by
// hash routing, as PersistedSegDB does) that no key appears
// on two hosts.
type PartitionMetadata struct {
	ShardCount int
	Disjoint   bool
	Verified   bool
	// KeyRanges, when set, names one key range or prefix per
	// shard, in host order, for human verification. It is
	// documentation, not a runtime check: the engine cannot
	// verify disjointness from inside the shards.
	KeyRanges []string
}

// Verify checks metadata against the host count. It
// returns ErrUnclearPartition when disjointness is not
// verified, and a plain error on a shard-count mismatch.
func (m PartitionMetadata) Verify(hosts int) error {
	if m.ShardCount != 0 && m.ShardCount != hosts {
		return fmt.Errorf("partition metadata: shard count %d does not match %d hosts", m.ShardCount, hosts)
	}
	if !m.Disjoint || !m.Verified {
		return ErrUnclearPartition
	}
	return nil
}

// MultiShardMode selects the coordinator's behaviour for
// hosts without a verified disjoint partition.
type MultiShardMode int

const (
	// ModeReject (default) refuses before executing anything.
	ModeReject MultiShardMode = iota
	// ModeDedup deduplicates exact duplicate rows and
	// refuses on a replication conflict.
	ModeDedup
)

// MultiShardTrace is the coordinator's execution trace: one
// per-host ISA trace, plus the coordinator's own peak,
// spill, and scanned totals.
type MultiShardTrace struct {
	PerHost         []ISATrace
	ScannedRows     int64
	PeakMemoryBytes int64
	Spilled         bool
	SpillBytes      int64
	SpillRuns       int
	// FallbackUsed reports that ExecuteISAWithHostSpill
	// reached this trace via the host-spill fallback after
	// the monolithic C engine ran out of resources.
	FallbackUsed bool
}

// ISAMultiShardCoordinator executes full plans (JOIN,
// AGGREGATE, SORT, LIMIT included) over one ISAHost per
// shard, with global operators on the host.
type ISAMultiShardCoordinator struct {
	hosts     []*ISAHost
	partition PartitionMetadata
	mode      MultiShardMode
	policy    ResourcePolicy
}

// NewISAMultiShardCoordinator builds a coordinator. A nil
// host, an empty host list, or an invalid policy is an
// error. Partition verification happens per Execute call
// (so one coordinator can serve both modes' errors
// distinctly), except that a shard-count mismatch is
// reported here, early.
func NewISAMultiShardCoordinator(hosts []*ISAHost, partition PartitionMetadata, mode MultiShardMode, policy ResourcePolicy) (*ISAMultiShardCoordinator, error) {
	if len(hosts) == 0 {
		return nil, fmt.Errorf("isa multi-shard coordinator: no hosts")
	}
	for i, h := range hosts {
		if h == nil {
			return nil, fmt.Errorf("isa multi-shard coordinator: host %d is nil", i)
		}
	}
	if err := policy.Validate(); err != nil {
		return nil, err
	}
	if partition.ShardCount != 0 && partition.ShardCount != len(hosts) {
		return nil, fmt.Errorf("partition metadata: shard count %d does not match %d hosts", partition.ShardCount, len(hosts))
	}
	return &ISAMultiShardCoordinator{hosts: append([]*ISAHost(nil), hosts...), partition: partition, mode: mode, policy: policy.resolved()}, nil
}

// NumHosts returns the shard (host) count.
func (c *ISAMultiShardCoordinator) NumHosts() int { return len(c.hosts) }

// Policy returns the coordinator's resolved policy.
func (c *ISAMultiShardCoordinator) Policy() ResourcePolicy { return c.policy }

// pipelineBindings returns the plan's pipeline bindings in
// compiler order: FROM first, JOIN second.
func pipelineBindings(plan *Plan) ([]Binding, error) {
	if plan == nil {
		return nil, fmt.Errorf("isa multi-shard coordinator: nil plan")
	}
	var from *Binding
	for i := range plan.Bindings {
		if plan.Bindings[i].Alias == plan.FromAlias {
			from = &plan.Bindings[i]
			break
		}
	}
	if from == nil {
		return nil, fmt.Errorf("isa multi-shard coordinator: no binding for FROM alias %q", plan.FromAlias)
	}
	out := []Binding{*from}
	if plan.Join != nil {
		var right *Binding
		for i := range plan.Bindings {
			if plan.Bindings[i].Alias == plan.Join.RightAlias {
				right = &plan.Bindings[i]
				break
			}
		}
		if right == nil {
			return nil, fmt.Errorf("isa multi-shard coordinator: no binding for JOIN alias %q", plan.Join.RightAlias)
		}
		out = append(out, *right)
	}
	return out, nil
}

// pipelineColNames returns alias.column names in pipeline
// order (left binding's columns, then right's).
func pipelineColNames(bindings []Binding) []string {
	var out []string
	for _, b := range bindings {
		for _, col := range b.Columns {
			out = append(out, b.Alias+"."+col.Name)
		}
	}
	return out
}

// pipelineIndex resolves a ColumnRef to its pipeline index
// by qualified name.
func pipelineIndex(names []string, ref *ColumnRef) (int, error) {
	if ref == nil {
		return 0, fmt.Errorf("isa multi-shard coordinator: nil column reference")
	}
	for i, n := range names {
		if n == ref.Qualified {
			return i, nil
		}
	}
	return 0, fmt.Errorf("isa multi-shard coordinator: column %q not in pipeline", ref.Qualified)
}

// scanOnlyPlan derives a SCAN+PROJECT plan for one binding:
// every column of the binding, no filter, no join, no
// aggregate, no sort, no limit. The coordinator applies the
// original plan's filter/aggregate/sort/limit itself, so
// the scan must not apply them a second time.
func scanOnlyPlan(plan *Plan, binding Binding) *Plan {
	proj := make([]ProjItem, len(binding.Columns))
	for i, col := range binding.Columns {
		proj[i] = ProjItem{
			Col:        &ColumnRef{Table: binding.Table, Alias: binding.Alias, Name: col.Name, Type: col.Type, Qualified: binding.Alias + "." + col.Name},
			OutputName: binding.Alias + "." + col.Name,
			OutputType: col.Type,
		}
	}
	return &Plan{
		Version:       PlanVersion,
		QueryText:     plan.QueryText,
		FromTable:     binding.Table,
		FromAlias:     binding.Alias,
		Projection:    proj,
		OutputColumns: append([]Column(nil), binding.Columns...),
		Bindings:      []Binding{binding},
		Operators:     []string{"SCAN", "PROJECT"},
	}
}

// isaHeaderBudgets parses the ISA program header's scanned /
// result budgets (magic 4 + version 4 + max_scanned 8 +
// max_result 8 + max_memory 8 + num_tables 4).
func isaHeaderBudgets(program []byte) (maxScanned, maxResult int64, ok bool) {
	if len(program) < 36 {
		return 0, 0, false
	}
	maxScanned = int64(binary.LittleEndian.Uint64(program[8:16]))
	maxResult = int64(binary.LittleEndian.Uint64(program[16:24]))
	return maxScanned, maxResult, true
}

// scanTable runs a scan-only ISA program for one binding on
// every host at its snapshot and returns the concatenated
// rows in host order, applying ModeDedup when configured.
// The returned trace accounts for every host execution.
func (c *ISAMultiShardCoordinator) scanTable(ctx context.Context, plan *Plan, binding Binding, snapshots []uint64, scanContract Contract, tr *MultiShardTrace) ([]Row, error) {
	scanPlan := scanOnlyPlan(plan, binding)
	prog, err := CompileISA(scanPlan, scanContract)
	if err != nil {
		return nil, fmt.Errorf("isa multi-shard coordinator: scan-only compile for %s: %w", binding.Alias, err)
	}
	var rows []Row
	for i, h := range c.hosts {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		hostRows, hostTr, err := h.ExecuteISAAt(prog, scanPlan, snapshots[i])
		if err != nil {
			return nil, fmt.Errorf("isa multi-shard coordinator: host %d scan of %s: %w", i, binding.Alias, err)
		}
		tr.PerHost = append(tr.PerHost, hostTr)
		tr.ScannedRows += hostTr.ScannedRows
		if hostTr.Spilled {
			tr.Spilled = true
		}
		if hostTr.ActualMemoryBytes > tr.PeakMemoryBytes {
			tr.PeakMemoryBytes = hostTr.ActualMemoryBytes
		}
		rows = append(rows, hostRows...)
	}
	if c.mode == ModeDedup {
		rows, err = dedupScanRows(rows, binding)
		if err != nil {
			return nil, err
		}
	}
	return rows, nil
}

// dedupScanRows removes exact duplicate rows (same columns
// and values) across hosts. When two rows share the binding's
// key-source column value but differ anywhere else, the
// hosts disagree about that key and the query fails with
// ErrPartitionConflict instead of silently picking a copy.
func dedupScanRows(rows []Row, binding Binding) ([]Row, error) {
	keyIdx := -1
	for i, col := range binding.Columns {
		if col.Source == "key" {
			keyIdx = i
			break
		}
	}
	seenRow := map[string]struct{}{}
	seenKey := map[string]Row{}
	var out []Row
	for _, r := range rows {
		enc := rowEncoding(r)
		if _, dup := seenRow[enc]; dup {
			continue
		}
		seenRow[enc] = struct{}{}
		if keyIdx >= 0 && keyIdx < len(r.Vals) {
			k := r.Vals[keyIdx].groupKey()
			if prev, ok := seenKey[k]; ok && rowEncoding(prev) != enc {
				return nil, fmt.Errorf("%w: key %s on binding %s", ErrPartitionConflict, k, binding.Alias)
			}
			seenKey[k] = r
		}
		out = append(out, r)
	}
	return out, nil
}

// rowEncoding is a canonical string for one row (column
// names + type-tagged values), used for deduplication and
// for order-insensitive test comparison.
func rowEncoding(r Row) string {
	var sb strings.Builder
	for i, c := range r.Cols {
		sb.WriteString(c)
		sb.WriteString("=")
		if i < len(r.Vals) {
			sb.WriteString(r.Vals[i].groupKey())
		} else {
			sb.WriteString("missing")
		}
		sb.WriteString(";")
	}
	return sb.String()
}

// joinKeyString is the canonical join-key encoding. It is
// exact across int/float: integral floats in int64 range
// encode as their int (so int 1 matches float 1.0, as
// Compare requires), non-integral or out-of-range floats
// encode as float bits, and every other kind is tagged so
// distinct types never collide. NULL keys never match and
// are rejected by the caller before this is used.
func joinKeyString(v Value) (string, error) {
	switch v.Kind {
	case KindNull:
		return "", fmt.Errorf("join key is NULL")
	case KindInt:
		return fmt.Sprintf("i:%d", v.I), nil
	case KindFloat:
		if math.IsNaN(v.F) {
			return "f:nan", nil
		}
		if iv, ok := Float64ToIntExact(v.F); ok {
			return fmt.Sprintf("i:%d", iv), nil
		}
		return fmt.Sprintf("f:%x", math.Float64bits(v.F)), nil
	case KindString:
		return "s:" + v.S, nil
	case KindBool:
		if v.BoolVal() {
			return "b:1", nil
		}
		return "b:0", nil
	case KindBytes:
		return fmt.Sprintf("y:%x", []byte(v.B)), nil
	default:
		return "", fmt.Errorf("unsupported join key kind %s", v.Kind)
	}
}

// Execute runs plan over all hosts. snapshots must have one
// entry per host. It executes nothing when it refuses
// (partition, policy, snapshot-count, or nil-plan errors
// all precede the first host execution). On error it
// returns nil rows: never a partial result.
func (c *ISAMultiShardCoordinator) Execute(ctx context.Context, program []byte, plan *Plan, snapshots []uint64) ([]Row, *MultiShardTrace, error) {
	tr := &MultiShardTrace{}
	if c == nil || len(c.hosts) == 0 {
		return nil, tr, fmt.Errorf("isa multi-shard coordinator: no hosts")
	}
	if plan == nil {
		return nil, tr, fmt.Errorf("isa multi-shard coordinator: nil plan")
	}
	if len(snapshots) != len(c.hosts) {
		return nil, tr, fmt.Errorf("isa multi-shard coordinator: %d snapshots for %d hosts", len(snapshots), len(c.hosts))
	}
	if err := ctx.Err(); err != nil {
		return nil, tr, err
	}
	if c.mode == ModeReject {
		if err := c.partition.Verify(len(c.hosts)); err != nil {
			return nil, tr, err
		}
	}
	maxScanned, maxResult, _ := isaHeaderBudgets(program)
	mgr, err := NewSpillManager(c.policy.SpillDir, c.policy.SpillQuotaBytes, c.policy.SpillSync)
	if err != nil {
		return nil, tr, err
	}
	defer func() { _ = mgr.Close() }()
	st := &coordState{coord: c, mgr: mgr, tr: tr, maxScanned: maxScanned, maxResult: maxResult}

	// Fast path: SCAN+FILTER+PROJECT only — the original
	// program runs per shard in the ISA engines. It is
	// NOT used in ModeDedup: deduplication needs the key
	// column, which a projection may drop, so dedup mode
	// always goes through the decomposed scan path below
	// (where scanTable deduplicates full scan rows, with
	// conflict detection by key).
	if plan.Join == nil && !plan.IsAggregate() && len(plan.OrderBy) == 0 && !plan.HasLimit && c.mode != ModeDedup {
		var rows []Row
		for i, h := range c.hosts {
			if err := ctx.Err(); err != nil {
				return nil, tr, err
			}
			hostRows, hostTr, err := h.ExecuteISAAt(program, plan, snapshots[i])
			if err != nil {
				return nil, tr, fmt.Errorf("isa multi-shard coordinator: host %d: %w", i, err)
			}
			tr.PerHost = append(tr.PerHost, hostTr)
			tr.ScannedRows += hostTr.ScannedRows
			if maxScanned > 0 && tr.ScannedRows > maxScanned {
				return nil, tr, &VerificationError{Code: "BUDGET_EXCEEDED", Msg: "isa multi-shard coordinator: scanned budget exceeded"}
			}
			if hostTr.Spilled {
				tr.Spilled = true
			}
			if hostTr.ActualMemoryBytes > tr.PeakMemoryBytes {
				tr.PeakMemoryBytes = hostTr.ActualMemoryBytes
			}
			rows = append(rows, hostRows...)
		}
		if c.mode == ModeDedup {
			bindings, err := pipelineBindings(plan)
			if err != nil {
				return nil, tr, err
			}
			rows, err = dedupScanRows(rows, bindings[0])
			if err != nil {
				return nil, tr, err
			}
		}
		if maxResult > 0 && int64(len(rows)) > maxResult {
			return nil, tr, &VerificationError{Code: "BUDGET_EXCEEDED", Msg: "isa multi-shard coordinator: result budget exceeded"}
		}
		return rows, tr, nil
	}

	// Decomposed path: scans in the engines, global
	// operators on the host.
	bindings, err := pipelineBindings(plan)
	if err != nil {
		return nil, tr, err
	}
	pipeNames := pipelineColNames(bindings)
	scanContract := Contract{}
	if maxScanned > 0 {
		scanContract.MaxScannedRows = maxScanned
	}
	leftRows, err := c.scanTable(ctx, plan, bindings[0], snapshots, scanContract, tr)
	if err != nil {
		return nil, tr, err
	}
	if maxScanned > 0 && tr.ScannedRows > maxScanned {
		return nil, tr, &VerificationError{Code: "BUDGET_EXCEEDED", Msg: "isa multi-shard coordinator: scanned budget exceeded"}
	}
	var pipeRows []Row
	if plan.Join != nil {
		rightRows, err := c.scanTable(ctx, plan, bindings[1], snapshots, scanContract, tr)
		if err != nil {
			return nil, tr, err
		}
		if maxScanned > 0 && tr.ScannedRows > maxScanned {
			return nil, tr, &VerificationError{Code: "BUDGET_EXCEEDED", Msg: "isa multi-shard coordinator: scanned budget exceeded"}
		}
		pipeRows, err = st.joinRows(ctx, plan, pipeNames, leftRows, rightRows)
		if err != nil {
			return nil, tr, err
		}
	} else {
		pipeRows = leftRows
	}
	// FILTER (reference three-valued semantics).
	if plan.Filter != nil {
		filtered := make([]Row, 0, len(pipeRows))
		for _, r := range pipeRows {
			if err := ctx.Err(); err != nil {
				return nil, tr, err
			}
			t, err := evalTri(plan.Filter, r)
			if err != nil {
				return nil, tr, fmt.Errorf("isa multi-shard coordinator: filter: %w", err)
			}
			if t == triTrue {
				filtered = append(filtered, r)
			}
		}
		pipeRows = filtered
	}
	var outRows []Row
	if plan.IsAggregate() {
		aggRows, err := st.aggregateRows(ctx, plan, pipeNames, pipeRows)
		if err != nil {
			return nil, tr, err
		}
		outRows, err = projectAggRows(plan, aggRows)
		if err != nil {
			return nil, tr, err
		}
	} else {
		outRows, err = projectRows(plan, pipeNames, pipeRows)
		if err != nil {
			return nil, tr, err
		}
	}
	if len(plan.OrderBy) > 0 {
		outRows, err = st.sortRows(ctx, plan, outRows)
		if err != nil {
			return nil, tr, err
		}
	}
	if plan.HasLimit {
		if plan.Limit == nil || *plan.Limit < 0 {
			return nil, tr, fmt.Errorf("isa multi-shard coordinator: invalid LIMIT")
		}
		if int64(len(outRows)) > *plan.Limit {
			outRows = outRows[:*plan.Limit]
		}
	}
	if maxResult > 0 && int64(len(outRows)) > maxResult {
		return nil, tr, &VerificationError{Code: "BUDGET_EXCEEDED", Msg: "isa multi-shard coordinator: result budget exceeded"}
	}
	return outRows, tr, nil
}

// ExecuteWithSession runs plan with one snapshot per host
// derived from a ConsistencySession. participants must have
// one entry per host, naming that host's participant in the
// session; a missing or expired attachment is an error
// before any execution.
func (c *ISAMultiShardCoordinator) ExecuteWithSession(ctx context.Context, program []byte, plan *Plan, session *ConsistencySession, participants []string) ([]Row, *MultiShardTrace, error) {
	tr := &MultiShardTrace{}
	if session == nil {
		return nil, tr, fmt.Errorf("isa multi-shard coordinator: nil consistency session")
	}
	if len(participants) != len(c.hosts) {
		return nil, tr, fmt.Errorf("isa multi-shard coordinator: %d session participants for %d hosts", len(participants), len(c.hosts))
	}
	snapshots := make([]uint64, len(c.hosts))
	for i, p := range participants {
		seq, err := session.SnapshotFor(p)
		if err != nil {
			return nil, tr, fmt.Errorf("isa multi-shard coordinator: session %s participant %q: %w", session.Token(), p, err)
		}
		snapshots[i] = seq
	}
	return c.Execute(ctx, program, plan, snapshots)
}

// IsISAResourceError reports whether an ISA execution error
// is a genuine resource failure (C engine OOM or operator
// limit) as opposed to a budget refusal, type error, or
// malformed program. Only resource failures license the
// host-spill fallback: a budget refusal is a policy
// decision, and retrying it through the coordinator would
// evade the policy.
func IsISAResourceError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrResourceLimit) {
		return true
	}
	return strings.Contains(err.Error(), "out of memory / resource cap")
}

// ExecuteISAWithHostSpill runs one ISA program on one host.
// It first tries the monolithic C engine — under the
// policy's C caps when they differ from the defaults, so
// a tight policy can itself trigger the fallback — and on
// a genuine resource failure only, it falls back to a
// single-host multi-shard coordinator (scans in the
// engine, global operators spilling on the host) and
// marks the trace FallbackUsed. The fallback never
// triggers on budget, type, scan, or malformed errors,
// and it never claims a spill it did not perform: the
// fallback trace's Spilled flag is the coordinator's own.
func ExecuteISAWithHostSpill(ctx context.Context, host *ISAHost, program []byte, plan *Plan, snapshot uint64, policy ResourcePolicy) ([]Row, *MultiShardTrace, error) {
	var rows []Row
	var isaTr ISATrace
	var err error
	pw, pj, ps, pg := policy.CParams()
	dw, dj, ds, dg := DefaultResourcePolicy().CParams()
	if pw != dw || pj != dj || ps != ds || pg != dg {
		rows, isaTr, err = host.ExecuteISAAtWithPolicy(program, plan, snapshot, &policy)
	} else {
		rows, isaTr, err = host.ExecuteISAAt(program, plan, snapshot)
	}
	if err == nil {
		return rows, &MultiShardTrace{
			PerHost:         []ISATrace{isaTr},
			ScannedRows:     isaTr.ScannedRows,
			PeakMemoryBytes: isaTr.ActualMemoryBytes,
			Spilled:         isaTr.Spilled,
		}, nil
	}
	if !IsISAResourceError(err) {
		return nil, nil, err
	}
	coord, err := NewISAMultiShardCoordinator([]*ISAHost{host},
		PartitionMetadata{ShardCount: 1, Disjoint: true, Verified: true}, ModeReject, policy)
	if err != nil {
		return nil, nil, err
	}
	rows, tr, err := coord.Execute(ctx, program, plan, []uint64{snapshot})
	if err != nil {
		return nil, tr, fmt.Errorf("isa host-spill fallback failed: %w", err)
	}
	tr.FallbackUsed = true
	return rows, tr, nil
}

// coordState carries per-query coordinator state (spill
// manager, trace, budgets) through the pipeline stages.
type coordState struct {
	coord      *ISAMultiShardCoordinator
	mgr        *SpillManager
	tr         *MultiShardTrace
	maxScanned int64
	maxResult  int64
}

func (st *coordState) noteSpill(run *SpillRun) {
	st.tr.Spilled = true
	st.tr.SpillRuns++
	st.tr.SpillBytes += run.bytes
}

// joinRows builds ONE global hash table over all right
// rows (broadcast-build) and probes it with the left rows
// in order. NULL keys never match; key-kind mismatches
// (different type tags) never match, mirroring the C
// engine's canonical-key equality. When the build exceeds
// the policy it spills (chunked build + ordered merge) or,
// in hard-cap mode, fails with ErrResourceLimit.
func (st *coordState) joinRows(ctx context.Context, plan *Plan, pipeNames []string, leftRows, rightRows []Row) ([]Row, error) {
	pol := st.coord.policy
	leftIdx, err := pipelineIndex(pipeNames, plan.Join.LeftCol)
	if err != nil {
		return nil, err
	}
	rightIdx, err := pipelineIndex(pipeNames, plan.Join.RightCol)
	if err != nil {
		return nil, err
	}
	// Right-side pipeline offset: right rows are scanned
	// alone, so their column index is the index within the
	// right binding, not the pipeline index.
	rightBindingIdx := rightIdx
	if plan.Join != nil {
		// Pipeline index of the first right column:
		nLeft := 0
		bindings, _ := pipelineBindings(plan)
		if len(bindings) > 0 {
			nLeft = len(bindings[0].Columns)
		}
		rightBindingIdx = rightIdx - nLeft
	}
	var buildBytes int64
	for _, r := range rightRows {
		buildBytes += rowSize(r)
	}
	if pol.MaxMemoryBytes > 0 && buildBytes > pol.MaxMemoryBytes && pol.HardCaps {
		return nil, fmt.Errorf("%w: join build %d bytes over memory budget %d", ErrResourceLimit, buildBytes, pol.MaxMemoryBytes)
	}
	needsSpill := int64(len(rightRows)) > pol.MaxJoinRows ||
		(pol.MaxMemoryBytes > 0 && buildBytes > pol.MaxMemoryBytes)
	if needsSpill && pol.HardCaps {
		return nil, fmt.Errorf("%w: join build %d rows over cap %d", ErrResourceLimit, len(rightRows), pol.MaxJoinRows)
	}
	if !needsSpill {
		index := map[string][]int{}
		for i, r := range rightRows {
			if rightBindingIdx < 0 || rightBindingIdx >= len(r.Vals) {
				return nil, fmt.Errorf("isa multi-shard coordinator: join right column out of range")
			}
			kv := r.Vals[rightBindingIdx]
			if kv.Kind == KindNull {
				continue
			}
			k, err := joinKeyString(kv)
			if err != nil {
				return nil, err
			}
			index[k] = append(index[k], i)
		}
		var out []Row
		for _, l := range leftRows {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if leftIdx >= len(l.Vals) {
				return nil, fmt.Errorf("isa multi-shard coordinator: join left column out of range")
			}
			lk := l.Vals[leftIdx]
			if lk.Kind == KindNull {
				continue
			}
			k, err := joinKeyString(lk)
			if err != nil {
				return nil, err
			}
			for _, ri := range index[k] {
				r := rightRows[ri]
				joined := Row{Cols: append(append([]string(nil), l.Cols...), r.Cols...), Vals: append(append([]Value(nil), l.Vals...), r.Vals...)}
				out = append(out, joined)
			}
		}
		if peak := buildBytes; peak > st.tr.PeakMemoryBytes {
			st.tr.PeakMemoryBytes = peak
		}
		return out, nil
	}
	return st.spillJoinRows(ctx, pipeNames, leftRows, rightRows, leftIdx, rightBindingIdx)
}

// spillJoinRows joins with a chunked, spill-backed build.
// Right rows are chunked (by row cap and memory budget);
// each chunk's matches are written to one spill run tagged
// with (left sequence, right ordinal); a k-way merge over
// the runs then restores exact left-then-right order. A
// left/right pair appears in exactly one chunk, so the
// merge keys are unique and no match is duplicated or lost.
func (st *coordState) spillJoinRows(ctx context.Context, pipeNames []string, leftRows, rightRows []Row, leftIdx, rightBindingIdx int) ([]Row, error) {
	pol := st.coord.policy
	var runs []*SpillRun
	var chunk []Row
	var chunkBytes int64
	flush := func() error {
		if len(chunk) == 0 {
			return nil
		}
		index := map[string][]int{}
		for i, r := range chunk {
			kv := r.Vals[rightBindingIdx]
			if kv.Kind == KindNull {
				continue
			}
			k, err := joinKeyString(kv)
			if err != nil {
				return err
			}
			index[k] = append(index[k], i)
		}
		// Right ordinals are global (chunk offset added by
		// the caller through chunkStart).
		run, err := st.mgr.NewRun(ctx)
		if err != nil {
			return err
		}
		for seq, l := range leftRows {
			if err := ctx.Err(); err != nil {
				_ = run.Close()
				return err
			}
			lk := l.Vals[leftIdx]
			if lk.Kind == KindNull {
				continue
			}
			k, err := joinKeyString(lk)
			if err != nil {
				_ = run.Close()
				return err
			}
			for _, ci := range index[k] {
				r := chunk[ci]
				vals := append(append([]Value(nil), l.Vals...), r.Vals...)
				rec := Row{
					Cols: append([]string{"__seq", "__right_ord"}, pipeNames...),
					Vals: append([]Value{Int(int64(seq)), Int(int64(chunkStartGlobal + ci))}, vals...),
				}
				if err := run.WriteRow(ctx, rec); err != nil {
					_ = run.Close()
					return err
				}
			}
		}
		if err := run.Finish(ctx); err != nil {
			_ = run.Close()
			return err
		}
		st.noteSpill(run)
		runs = append(runs, run)
		return nil
	}
	// chunkStartGlobal is set by the chunking loop below via
	// closure variable; flush reads it at call time.
	chunkStartGlobal = 0
	for i, r := range rightRows {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		sz := rowSize(r)
		if len(chunk) > 0 && (int64(len(chunk)) >= pol.MaxJoinRows ||
			(pol.MaxMemoryBytes > 0 && chunkBytes+sz > pol.MaxMemoryBytes)) {
			if err := flush(); err != nil {
				return nil, err
			}
			chunk = nil
			chunkBytes = 0
			chunkStartGlobal = i
		}
		chunk = append(chunk, r)
		chunkBytes += sz
	}
	if err := flush(); err != nil {
		return nil, err
	}
	// K-way merge over the runs by (seq, right_ord).
	type head struct {
		row   Row
		rd    *SpillReader
		buf   []Row
		pos   int
		empty bool
	}
	heads := make([]*head, len(runs))
	for i, run := range runs {
		rd, err := run.NewReader()
		if err != nil {
			return nil, err
		}
		heads[i] = &head{rd: rd}
	}
	defer func() {
		for _, h := range heads {
			_ = h.rd.Close()
		}
	}()
	next := func(h *head) error {
		if h.pos < len(h.buf) {
			return nil
		}
		page, err := h.rd.NextPage(ctx, 64)
		if err != nil {
			return err
		}
		if len(page) == 0 {
			h.empty = true
			return nil
		}
		h.buf = page
		h.pos = 0
		return nil
	}
	var out []Row
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		best := -1
		var bestSeq, bestOrd int64
		for i, h := range heads {
			if h.empty {
				continue
			}
			if err := next(h); err != nil {
				return nil, err
			}
			if h.empty {
				continue
			}
			rec := h.buf[h.pos]
			seq, ord := rec.Vals[0].I, rec.Vals[1].I
			if best < 0 || seq < bestSeq || (seq == bestSeq && ord < bestOrd) {
				best, bestSeq, bestOrd = i, seq, ord
			}
		}
		if best < 0 {
			break
		}
		rec := heads[best].buf[heads[best].pos]
		heads[best].pos++
		out = append(out, Row{Cols: append([]string(nil), pipeNames...), Vals: append([]Value(nil), rec.Vals[2:]...)})
	}
	return out, nil
}

// chunkStartGlobal is the global right ordinal of the chunk
// currently being flushed by spillJoinRows. It is a package
// variable only because flush is a closure over the loop;
// it is written only by the chunking loop before flush and
// read only by flush, never concurrently (coordinator
// execution is single-threaded per query).
var chunkStartGlobal int

// projectRows projects non-aggregate pipeline rows by
// global column index, renaming to OutputNames.
func projectRows(plan *Plan, pipeNames []string, rows []Row) ([]Row, error) {
	idxs := make([]int, len(plan.Projection))
	for i, it := range plan.Projection {
		idx, err := pipelineIndex(pipeNames, it.Col)
		if err != nil {
			return nil, err
		}
		idxs[i] = idx
	}
	names := make([]string, len(plan.Projection))
	for i, it := range plan.Projection {
		names[i] = it.OutputName
	}
	out := make([]Row, 0, len(rows))
	for _, r := range rows {
		vals := make([]Value, len(idxs))
		for i, idx := range idxs {
			if idx >= len(r.Vals) {
				return nil, fmt.Errorf("isa multi-shard coordinator: projection column %d out of range", idx)
			}
			vals[i] = r.Vals[idx]
		}
		out = append(out, Row{Cols: append([]string(nil), names...), Vals: vals})
	}
	return out, nil
}

// projectAggRows projects aggregate pipeline rows
// (group keys, then aggregate results) by pipeline index.
func projectAggRows(plan *Plan, aggRows []Row) ([]Row, error) {
	aggOrdinal := 0
	idxs := make([]int, len(plan.Projection))
	for i, it := range plan.Projection {
		if it.Agg != "" {
			idxs[i] = len(plan.GroupBy) + aggOrdinal
			aggOrdinal++
			continue
		}
		pos := -1
		for j, g := range plan.GroupBy {
			if g.Alias == it.Col.Alias && g.Name == it.Col.Name {
				pos = j
				break
			}
		}
		if pos < 0 {
			return nil, fmt.Errorf("isa multi-shard coordinator: plain column %q is not in GROUP BY", it.Col.Qualified)
		}
		idxs[i] = pos
	}
	names := make([]string, len(plan.Projection))
	for i, it := range plan.Projection {
		names[i] = it.OutputName
	}
	out := make([]Row, 0, len(aggRows))
	for _, r := range aggRows {
		vals := make([]Value, len(idxs))
		for i, idx := range idxs {
			if idx >= len(r.Vals) {
				return nil, fmt.Errorf("isa multi-shard coordinator: aggregate projection column %d out of range", idx)
			}
			vals[i] = r.Vals[idx]
		}
		out = append(out, Row{Cols: append([]string(nil), names...), Vals: vals})
	}
	return out, nil
}

// coordAcc mirrors the reference accumulator's result
// semantics (COUNT counts non-null / COUNT(*) counts rows;
// SUM is int for all-int input else float; AVG always
// float; MIN/MAX keep the input value; all-NULL input
// yields NULL), except that int SUM accumulation is
// checked: overflow returns ErrNumericOverflow instead of
// wrapping silently (the reference executor wraps; see
// GATE4_NOTES.md §5a).
type coordAcc struct {
	agg     string
	count   int64
	sumF    float64
	sumI    int64
	isFloat bool
	has     bool
	min     Value
	max     Value
}

func (a *coordAcc) add(v Value, star bool) error {
	switch a.agg {
	case "COUNT":
		if star {
			a.count++
			return nil
		}
		if v.Kind != KindNull {
			a.count++
		}
		return nil
	case "SUM", "AVG":
		if v.Kind == KindNull {
			return nil
		}
		if !v.IsNumeric() {
			return fmt.Errorf("%s requires numeric values", a.agg)
		}
		f, _ := v.AsFloat()
		a.sumF += f
		if v.Kind == KindInt {
			sum, err := AddIntChecked(a.sumI, v.I)
			if err != nil {
				return err
			}
			a.sumI = sum
		} else {
			a.isFloat = true
		}
		a.count++
		a.has = true
		return nil
	case "MIN", "MAX":
		if v.Kind == KindNull {
			return nil
		}
		if !a.has {
			a.min, a.max, a.has = v, v, true
			return nil
		}
		c, err := Compare(v, a.min)
		if err != nil {
			return err
		}
		if c < 0 {
			a.min = v
		}
		c, err = Compare(v, a.max)
		if err != nil {
			return err
		}
		if c > 0 {
			a.max = v
		}
		return nil
	}
	return fmt.Errorf("unknown aggregate %q", a.agg)
}

func (a *coordAcc) result() Value {
	switch a.agg {
	case "COUNT":
		return Int(a.count)
	case "SUM":
		if !a.has {
			return Null()
		}
		if a.isFloat {
			return Float(a.sumF)
		}
		return Int(a.sumI)
	case "AVG":
		if !a.has || a.count == 0 {
			return Null()
		}
		return Float(a.sumF / float64(a.count))
	case "MIN":
		if !a.has {
			return Null()
		}
		return a.min
	case "MAX":
		if !a.has {
			return Null()
		}
		return a.max
	}
	return Null()
}

// coordGroup is one in-memory group with its accumulators.
type coordGroup struct {
	keyVals []Value
	accs    []*coordAcc
}

// groupBytes estimates one group's retained size in the
// same shape as the C engine's groups accounting: 48 per
// group, key value sizes, 24 per accumulator, plus min/max
// sizes when present.
func groupBytes(keyVals []Value, accs []*coordAcc) int64 {
	sz := int64(48)
	for _, v := range keyVals {
		sz += valueSize(v)
	}
	for _, a := range accs {
		sz += 24
		if a.has {
			sz += valueSize(a.min) + valueSize(a.max)
		}
	}
	return sz
}

// encodeGroupRow serializes one spilled group partial as a
// Row: key string, key values, then per accumulator
// (count, sumF, sumI, isFloat, has, min, max).
func encodeGroupRow(key string, g *coordGroup) Row {
	cols := []string{"__gkey"}
	vals := []Value{String(key)}
	for i, kv := range g.keyVals {
		cols = append(cols, fmt.Sprintf("__key_%d", i))
		vals = append(vals, kv)
	}
	for j, a := range g.accs {
		cols = append(cols,
			fmt.Sprintf("__acc_%d_count", j),
			fmt.Sprintf("__acc_%d_sumf", j),
			fmt.Sprintf("__acc_%d_sumi", j),
			fmt.Sprintf("__acc_%d_isfloat", j),
			fmt.Sprintf("__acc_%d_has", j),
			fmt.Sprintf("__acc_%d_min", j),
			fmt.Sprintf("__acc_%d_max", j))
		vals = append(vals, Int(a.count), Float(a.sumF), Int(a.sumI), Bool(a.isFloat), Bool(a.has), a.min, a.max)
	}
	return Row{Cols: cols, Vals: vals}
}

// decodeGroupRow parses an encodeGroupRow result back into
// a key and a partial accumulator set. The agg names come
// from the plan (not stored in the row).
func decodeGroupRow(row Row, ngroup int, aggNames []string) (string, []*coordAcc, error) {
	if len(row.Vals) < 1+ngroup+len(aggNames)*7 {
		return "", nil, fmt.Errorf("spill group row too short: %d values", len(row.Vals))
	}
	key := row.Vals[0].S
	accs := make([]*coordAcc, len(aggNames))
	for j, name := range aggNames {
		base := 1 + ngroup + j*7
		a := &coordAcc{
			agg:     name,
			count:   row.Vals[base].I,
			sumF:    row.Vals[base+1].F,
			sumI:    row.Vals[base+2].I,
			isFloat: row.Vals[base+3].BoolVal(),
			has:     row.Vals[base+4].BoolVal(),
			min:     row.Vals[base+5],
			max:     row.Vals[base+6],
		}
		accs[j] = a
	}
	return key, accs, nil
}

// mergeAcc folds a spilled partial into a final accumulator
// (COUNT/SUM/AVG add; MIN/MAX take extremes). Int sums are
// checked, so a merge that overflows is a typed error.
func mergeAcc(dst, src *coordAcc) error {
	switch dst.agg {
	case "COUNT":
		dst.count += src.count
		return nil
	case "SUM", "AVG":
		sum, err := AddIntChecked(dst.sumI, src.sumI)
		if err != nil {
			return err
		}
		dst.sumI = sum
		dst.sumF += src.sumF
		if src.isFloat {
			dst.isFloat = true
		}
		dst.count += src.count
		if src.has {
			dst.has = true
		}
		return nil
	case "MIN", "MAX":
		if !src.has {
			return nil
		}
		if !dst.has {
			dst.min, dst.max, dst.has = src.min, src.max, true
			return nil
		}
		c, err := Compare(src.min, dst.min)
		if err != nil {
			return err
		}
		if c < 0 {
			dst.min = src.min
		}
		c, err = Compare(src.max, dst.max)
		if err != nil {
			return err
		}
		if c > 0 {
			dst.max = src.max
		}
		return nil
	}
	return fmt.Errorf("unknown aggregate %q", dst.agg)
}

// aggregateRows accumulates groups ONCE globally, in
// first-appearance order. When retained groups exceed the
// memory budget or the in-memory group cap, the current
// groups spill to disk and accumulation continues; the
// final merge folds spilled partials and in-memory groups
// in global first-appearance order, each group exactly
// once. In hard-cap mode, exceeding MaxGroups fails with
// ErrResourceLimit instead of spilling.
func (st *coordState) aggregateRows(ctx context.Context, plan *Plan, pipeNames []string, rows []Row) ([]Row, error) {
	pol := st.coord.policy
	groupIdxs := make([]int, len(plan.GroupBy))
	for i := range plan.GroupBy {
		idx, err := pipelineIndex(pipeNames, &plan.GroupBy[i])
		if err != nil {
			return nil, err
		}
		groupIdxs[i] = idx
	}
	type aggSpec struct {
		name string
		star bool
		col  int // pipeline index, -1 for COUNT(*)
	}
	var specs []aggSpec
	for _, it := range plan.Projection {
		if it.Agg == "" {
			continue
		}
		s := aggSpec{name: it.Agg, star: it.Star, col: -1}
		if !it.Star {
			idx, err := pipelineIndex(pipeNames, it.Col)
			if err != nil {
				return nil, err
			}
			s.col = idx
		}
		specs = append(specs, s)
	}
	groups := map[string]*coordGroup{}
	var order []string
	seen := map[string]struct{}{}
	var spillRuns []*SpillRun
	var retainedBytes int64
	spillCurrent := func() error {
		if len(groups) == 0 {
			return nil
		}
		run, err := st.mgr.NewRun(ctx)
		if err != nil {
			return err
		}
		for _, key := range order {
			// Only groups currently in memory spill;
			// keys already spilled in an earlier epoch
			// are skipped here (they are not in groups).
			g, ok := groups[key]
			if !ok {
				continue
			}
			if err := run.WriteRow(ctx, encodeGroupRow(key, g)); err != nil {
				_ = run.Close()
				return err
			}
		}
		if err := run.Finish(ctx); err != nil {
			_ = run.Close()
			return err
		}
		st.noteSpill(run)
		spillRuns = append(spillRuns, run)
		groups = map[string]*coordGroup{}
		retainedBytes = 0
		return nil
	}
	for _, r := range rows {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		keyVals := make([]Value, len(groupIdxs))
		var kb strings.Builder
		for i, idx := range groupIdxs {
			if idx >= len(r.Vals) {
				return nil, fmt.Errorf("isa multi-shard coordinator: group column %d out of range", idx)
			}
			keyVals[i] = r.Vals[idx]
			kb.WriteString(r.Vals[idx].groupKey())
			kb.WriteString("\x00")
		}
		key := kb.String()
		g, ok := groups[key]
		if !ok {
			if _, isNew := seen[key]; !isNew {
				seen[key] = struct{}{}
				order = append(order, key)
				if pol.HardCaps && int64(len(order)) > pol.MaxGroups {
					return nil, fmt.Errorf("%w: %d groups over cap %d", ErrResourceLimit, len(order), pol.MaxGroups)
				}
			}
			// Note on spill timing: spillCurrent runs
			// BEFORE this row is folded, so a fresh
			// group spills as a zero partial (a merge
			// no-op: mergeAcc ignores has=false
			// partials for SUM/AVG/MIN/MAX and adds 0
			// for COUNT) and the row then folds into
			// the re-created fresh group. A group that
			// existed before the spill keeps its prior
			// values in the spilled partial and starts
			// a new partial for later rows. Either way
			// every value is counted exactly once.
			g = &coordGroup{keyVals: keyVals}
			for _, s := range specs {
				g.accs = append(g.accs, &coordAcc{agg: s.name})
			}
			groups[key] = g
			retainedBytes += groupBytes(keyVals, g.accs)
			if (pol.MaxMemoryBytes > 0 && retainedBytes > pol.MaxMemoryBytes) ||
				(int64(len(groups)) > pol.MaxGroups) {
				if err := spillCurrent(); err != nil {
					return nil, err
				}
				g = &coordGroup{keyVals: keyVals}
				for _, s := range specs {
					g.accs = append(g.accs, &coordAcc{agg: s.name})
				}
				groups[key] = g
				retainedBytes += groupBytes(keyVals, g.accs)
			}
		}
		for j, s := range specs {
			var v Value
			if s.star {
				v = Int(1) // marker: COUNT(*) counts rows
			} else {
				v = r.Vals[s.col]
			}
			if err := g.accs[j].add(v, s.star); err != nil {
				return nil, err
			}
		}
	}
	// Ungrouped aggregation over empty input yields one row.
	if len(plan.GroupBy) == 0 && len(order) == 0 {
		g := &coordGroup{keyVals: nil}
		for _, s := range specs {
			g.accs = append(g.accs, &coordAcc{agg: s.name})
		}
		order = []string{""}
		groups[""] = g
	}
	// Final merge: spilled partials first (in spill order),
	// then in-memory groups, in global first-appearance
	// order, each group exactly once.
	aggNames := make([]string, len(specs))
	for i, s := range specs {
		aggNames[i] = s.name
	}
	merged := map[string]*coordGroup{}
	for _, run := range spillRuns {
		rd, err := run.NewReader()
		if err != nil {
			return nil, err
		}
		for {
			page, err := rd.NextPage(ctx, 64)
			if err != nil {
				_ = rd.Close()
				return nil, err
			}
			if len(page) == 0 {
				break
			}
			for _, rec := range page {
				key, partials, err := decodeGroupRow(rec, len(plan.GroupBy), aggNames)
				if err != nil {
					_ = rd.Close()
					return nil, err
				}
				mg, ok := merged[key]
				if !ok {
					mg = &coordGroup{keyVals: nil}
					for _, s := range specs {
						mg.accs = append(mg.accs, &coordAcc{agg: s.name})
					}
					// Key values: recover from the
					// first partial's key columns.
					mg.keyVals = make([]Value, len(plan.GroupBy))
					for i := range mg.keyVals {
						mg.keyVals[i] = rec.Vals[1+i]
					}
					merged[key] = mg
				}
				for j := range partials {
					if err := mergeAcc(mg.accs[j], partials[j]); err != nil {
						_ = rd.Close()
						return nil, err
					}
				}
			}
		}
		_ = rd.Close()
	}
	for key, g := range groups {
		mg, ok := merged[key]
		if !ok {
			mg = &coordGroup{keyVals: g.keyVals}
			for _, s := range specs {
				mg.accs = append(mg.accs, &coordAcc{agg: s.name})
			}
			merged[key] = mg
		}
		for j := range g.accs {
			if err := mergeAcc(mg.accs[j], g.accs[j]); err != nil {
				return nil, err
			}
		}
	}
	// Emit in global first-appearance order.
	groupNames := make([]string, len(plan.GroupBy))
	for i := range plan.GroupBy {
		groupNames[i] = plan.GroupBy[i].Qualified
	}
	aggOutNames := make([]string, 0, len(specs))
	for _, it := range plan.Projection {
		if it.Agg != "" {
			aggOutNames = append(aggOutNames, it.OutputName)
		}
	}
	// Output names are attached by projectAggRows; aggRows
	// carry them here only so the rows are self-describing.
	out := make([]Row, 0, len(order))
	for _, key := range order {
		mg, ok := merged[key]
		if !ok {
			continue // group with no values cannot occur
		}
		vals := append([]Value(nil), mg.keyVals...)
		for _, a := range mg.accs {
			vals = append(vals, a.result())
		}
		cols := append(append([]string(nil), groupNames...), aggOutNames...)
		out = append(out, Row{Cols: cols, Vals: vals})
	}
	if peak := retainedBytes; peak > st.tr.PeakMemoryBytes {
		st.tr.PeakMemoryBytes = peak
	}
	return out, nil
}

// resolveSortPositions maps ORDER BY items to projected
// positions (ORDER BY resolves against output columns, as
// the binder does).
func resolveSortPositions(plan *Plan) ([]int, []bool, error) {
	pos := make([]int, len(plan.OrderBy))
	desc := make([]bool, len(plan.OrderBy))
	for i, o := range plan.OrderBy {
		found := -1
		for j, it := range plan.Projection {
			if strings.EqualFold(it.OutputName, o.Name) {
				found = j
				break
			}
		}
		if found < 0 {
			return nil, nil, fmt.Errorf("isa multi-shard coordinator: ORDER BY %q is not an output column", o.Name)
		}
		pos[i] = found
		desc[i] = o.Desc
	}
	return pos, desc, nil
}

// compareProjected compares two projected rows by the sort
// keys: NULLs last in either direction; a comparison type
// error is treated as equal for that key (matching the
// reference executor's sort comparator).
func compareProjected(a, b Row, pos []int, desc []bool) int {
	for i, p := range pos {
		va, vb := a.Vals[p], b.Vals[p]
		if va.Kind == KindNull && vb.Kind == KindNull {
			continue
		}
		if va.Kind == KindNull {
			return 1
		}
		if vb.Kind == KindNull {
			return -1
		}
		c, err := Compare(va, vb)
		if err != nil {
			continue
		}
		if c != 0 {
			if desc[i] {
				return -c
			}
			return c
		}
	}
	return 0
}

// sortRows sorts projected rows globally (stable). When
// the row count or memory exceeds the policy chunk caps,
// it uses an external merge sort through spill runs
// (chunk-sort + k-way merge, ties broken by original
// sequence, so stability is preserved across chunks).
func (st *coordState) sortRows(ctx context.Context, plan *Plan, rows []Row) ([]Row, error) {
	pos, desc, err := resolveSortPositions(plan)
	if err != nil {
		return nil, err
	}
	pol := st.coord.policy
	var mem int64
	for _, r := range rows {
		mem += rowSize(r)
	}
	if mem > st.tr.PeakMemoryBytes {
		st.tr.PeakMemoryBytes = mem
	}
	if int64(len(rows)) <= pol.MaxSortRows && (pol.MaxMemoryBytes <= 0 || mem <= pol.MaxMemoryBytes) {
		sort.SliceStable(rows, func(i, j int) bool {
			return compareProjected(rows[i], rows[j], pos, desc) < 0
		})
		return rows, nil
	}
	if pol.HardCaps {
		return nil, fmt.Errorf("%w: sort %d rows over cap %d", ErrResourceLimit, len(rows), pol.MaxSortRows)
	}
	// External merge sort: chunks carry their original
	// sequence in column "__seq" through the spill files.
	var runs []*SpillRun
	for start := 0; start < len(rows); {
		end := start
		var chunkBytes int64
		for end < len(rows) {
			sz := rowSize(rows[end])
			if end > start && (int64(end-start) >= pol.MaxSortRows ||
				(pol.MaxMemoryBytes > 0 && chunkBytes+sz > pol.MaxMemoryBytes)) {
				break
			}
			chunkBytes += sz
			end++
		}
		chunk := append([]Row(nil), rows[start:end]...)
		// Sort chunk positions by (keys, original sequence).
		orderIdx := make([]int, len(chunk))
		for i := range orderIdx {
			orderIdx[i] = i
		}
		sort.SliceStable(orderIdx, func(a, b int) bool {
			ca := compareProjected(chunk[orderIdx[a]], chunk[orderIdx[b]], pos, desc)
			if ca != 0 {
				return ca < 0
			}
			return start+orderIdx[a] < start+orderIdx[b]
		})
		run, err := st.mgr.NewRun(ctx)
		if err != nil {
			return nil, err
		}
		for _, p := range orderIdx {
			rec := chunk[p]
			rec.Cols = append([]string{"__seq"}, rec.Cols...)
			rec.Vals = append([]Value{Int(int64(start + p))}, rec.Vals...)
			if err := run.WriteRow(ctx, rec); err != nil {
				_ = run.Close()
				return nil, err
			}
		}
		if err := run.Finish(ctx); err != nil {
			_ = run.Close()
			return nil, err
		}
		st.noteSpill(run)
		runs = append(runs, run)
		start = end
	}
	// K-way merge.
	type head struct {
		rd    *SpillReader
		buf   []Row
		pos   int
		empty bool
	}
	heads := make([]*head, len(runs))
	for i, run := range runs {
		rd, err := run.NewReader()
		if err != nil {
			return nil, err
		}
		heads[i] = &head{rd: rd}
	}
	defer func() {
		for _, h := range heads {
			_ = h.rd.Close()
		}
	}()
	next := func(h *head) error {
		if h.pos < len(h.buf) {
			return nil
		}
		page, err := h.rd.NextPage(ctx, 64)
		if err != nil {
			return err
		}
		if len(page) == 0 {
			h.empty = true
			return nil
		}
		h.buf = page
		h.pos = 0
		return nil
	}
	strip := func(r Row) Row {
		return Row{Cols: append([]string(nil), r.Cols[1:]...), Vals: append([]Value(nil), r.Vals[1:]...)}
	}
	var out []Row
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		best := -1
		for i, h := range heads {
			if h.empty {
				continue
			}
			if err := next(h); err != nil {
				return nil, err
			}
			if h.empty {
				continue
			}
			if best < 0 {
				best = i
				continue
			}
			a, b := heads[best].buf[heads[best].pos], h.buf[h.pos]
			ca := compareProjected(strip(a), strip(b), pos, desc)
			if ca > 0 || (ca == 0 && a.Vals[0].I > b.Vals[0].I) {
				best = i
			}
		}
		if best < 0 {
			break
		}
		rec := heads[best].buf[heads[best].pos]
		heads[best].pos++
		out = append(out, strip(rec))
	}
	return out, nil
}
