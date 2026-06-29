package query

import (
	"context"
	"fmt"
	"time"
)

// ExecuteOptions tunes execution. Zero values mean defaults: batch
// size 64. Batch size affects pipeline chunking only, never results.
type ExecuteOptions struct {
	BatchSize int
}

// DefaultBatchSize is the pipeline batch size when no option is set.
const DefaultBatchSize = 64

// scanBudget is the shared runtime scanned-row cap across all sources
// of one query (both join sides together, not per side).
type scanBudget struct {
	limit int64 // 0 = unlimited
	used  int64
}

func (b *scanBudget) add(n int) error {
	if b == nil || b.limit <= 0 {
		if b != nil {
			b.used += int64(n)
		}
		return nil
	}
	b.used += int64(n)
	if b.used > b.limit {
		return &VerificationError{Code: "BUDGET_EXCEEDED", Msg: fmt.Sprintf("scanned rows %d exceed max scanned rows %d", b.used, b.limit)}
	}
	return nil
}

// Execute verifies a compiled plan against a contract and, only if
// verification passes, runs it as a batch pipeline over the given
// sources (keyed by table alias). Sources are never opened when
// verification fails. It returns the output rows and the unified
// trace; on failure the trace still describes what happened (which
// check failed, or how far execution got) and rows are nil.
func Execute(ctx context.Context, plan *Plan, contract Contract, sources map[string]Source) ([]Row, *Trace, error) {
	return ExecuteWithOptions(ctx, plan, contract, sources, nil)
}

// ExecuteWithOptions is Execute with execution options.
func ExecuteWithOptions(ctx context.Context, plan *Plan, contract Contract, sources map[string]Source, opts *ExecuteOptions) ([]Row, *Trace, error) {
	start := time.Now()
	batchSize := DefaultBatchSize
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
	// Real memory enforcement (see executor.go): the scan budget
	// below caps scanned rows; memTracker caps retained memory per
	// retaining operator and records the measured peak for the trace.
	budget := &scanBudget{limit: contract.MaxScannedRows}
	memTrk := &memTracker{}
	// finish stamps the duration and the measured memory figures.
	// Verification failures set ValidationError and return before
	// any source is opened; execution failures set ExecutionError
	// at the point they occur. On failure ActualReturnedRows stays
	// 0 and rows are nil; ScannedRows reflects how far the scan got.
	finish := func(rows []Row, err error) ([]Row, *Trace, error) {
		trace.Duration = time.Since(start)
		trace.ScannedRows = budget.used
		trace.ActualMemoryBytes = memTrk.peak
		trace.Spilled = memTrk.spilled
		return rows, trace, err
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

	var opStatsList []*opStats
	stat := func(name string) *opStats {
		s := &opStats{Operator: name}
		opStatsList = append(opStatsList, s)
		return s
	}

	bindingFor := func(alias string) (Binding, bool) {
		for _, b := range plan.Bindings {
			if b.Alias == alias {
				return b, true
			}
		}
		return Binding{}, false
	}

	// GATE 1 (per-query snapshot): every source opened below
	// must read at ONE snapshot for this execution. When the
	// caller supplied a session (WithSession), use it and do
	// NOT close it — the caller owns it. Otherwise create a
	// fresh session for this Execute and close it when Execute
	// returns; the defer is registered here, before any source
	// Open, so deferred iterator Closes (registered later) run
	// before the session Close (LIFO). Sources that ignore
	// sessions keep their own semantics.
	sess := sessionFromCtx(ctx)
	execCtx := ctx
	if sess == nil {
		sess = NewSession()
		execCtx = WithSession(ctx, sess)
		defer func() { _ = sess.Close() }()
	}
	ctx = execCtx

	// Phase 2: open sources (this freezes each source's snapshot).
	leftBinding, ok := bindingFor(plan.FromAlias)
	if !ok {
		err := fmt.Errorf("no binding for FROM alias %q", plan.FromAlias)
		trace.ExecutionError = err.Error()
		return finish(nil, err)
	}
	leftSource, ok := sources[plan.FromAlias]
	if !ok {
		err := fmt.Errorf("no source provided for table %q (alias %q)", plan.FromTable, plan.FromAlias)
		trace.ExecutionError = err.Error()
		return finish(nil, err)
	}
	leftRaw, err := leftSource.Open(ctx, leftBinding)
	if err != nil {
		trace.ExecutionError = err.Error()
		return finish(nil, err)
	}
	defer leftRaw.Close()

	// Wrap scans with the shared budget cap.
	cappedScan := func(it RowIterator, stats *opStats) batchIter {
		return &cappedScanIter{it: it, batch: batchSize, stats: stats, budget: budget}
	}
	leftScan := cappedScan(leftRaw, stat("SCAN"))

	var cur batchIter = leftScan

	// JOIN: build the right side fully (budget-checked through the
	// same shared cap), then probe it with left batches.
	if plan.Join != nil {
		rightBinding, ok := bindingFor(plan.Join.RightAlias)
		if !ok {
			err := fmt.Errorf("no binding for JOIN alias %q", plan.Join.RightAlias)
			trace.ExecutionError = err.Error()
			return finish(nil, err)
		}
		rightSource, ok := sources[plan.Join.RightAlias]
		if !ok {
			err := fmt.Errorf("no source provided for table %q (alias %q)", plan.Join.RightTable, plan.Join.RightAlias)
			trace.ExecutionError = err.Error()
			return finish(nil, err)
		}
		rightRaw, err := rightSource.Open(ctx, rightBinding)
		if err != nil {
			trace.ExecutionError = err.Error()
			return finish(nil, err)
		}
		defer rightRaw.Close()
		rightScan := cappedScan(rightRaw, stat("SCAN"))
		// Build side goes through a spill store: retained in
		// memory up to Contract.MaxMemoryBytes (measured from real
		// rows), spilled to temp files beyond it, and rejected with
		// BUDGET_EXCEEDED only if a single batch cannot fit.
		buildStore := newSpillStore(contract.MaxMemoryBytes, memTrk)
		buildOK := false
		defer func() {
			if !buildOK {
				buildStore.Close()
			}
		}()
		for {
			batch, err := rightScan.NextBatch()
			if err != nil {
				trace.ExecutionError = err.Error()
				return finish(nil, err)
			}
			if len(batch) == 0 {
				break
			}
			if err := buildStore.AppendBatch(batch); err != nil {
				trace.ExecutionError = err.Error()
				return finish(nil, err)
			}
		}
		joinStats := stat("JOIN")
		joinIt := &joinIter{left: cur, store: buildStore, rkey: plan.Join.RightCol.Qualified, lkey: plan.Join.LeftCol.Qualified, stats: joinStats}
		defer joinIt.Close()
		buildOK = true
		cur = joinIt
	}

	if plan.Filter != nil {
		cur = &filterIter{in: cur, expr: plan.Filter, stats: stat("FILTER")}
	}
	if plan.IsAggregate() {
		aggStats := stat("AGGREGATE")
		agg, err := newAggregateIter(plan, cur, aggStats, contract.MaxMemoryBytes, memTrk)
		if err != nil {
			trace.ExecutionError = err.Error()
			return finish(nil, err)
		}
		defer agg.Close()
		agg.bs = batchSize
		cur = agg
	}
	cur = &projectIter{in: cur, plan: plan, stats: stat("PROJECT")}
	if len(plan.OrderBy) > 0 {
		sortStats := stat("SORT")
		srt, err := newSortIter(plan, cur, contract.MaxMemoryBytes, memTrk)
		if err != nil {
			trace.ExecutionError = err.Error()
			return finish(nil, err)
		}
		defer srt.Close()
		srt.stats = sortStats
		srt.bs = batchSize
		cur = srt
	}
	if plan.HasLimit && plan.Limit != nil {
		cur = &limitIter{in: cur, remaining: *plan.Limit, stats: stat("LIMIT")}
	}

	// Phase 3: collect, enforcing the result cap as batches arrive
	// (not after the fact) and honouring context cancellation.
	var rows []Row
	for {
		if err := ctx.Err(); err != nil {
			trace.ExecutionError = err.Error()
			return finish(nil, err)
		}
		batch, err := cur.NextBatch()
		if err != nil {
			trace.ExecutionError = err.Error()
			return finish(nil, err)
		}
		if len(batch) == 0 {
			break
		}
		rows = append(rows, batch...)
		if contract.MaxResultRows > 0 && int64(len(rows)) > contract.MaxResultRows {
			err := &VerificationError{Code: "BUDGET_EXCEEDED", Msg: fmt.Sprintf("result rows exceed max result rows %d", contract.MaxResultRows)}
			trace.ExecutionError = err.Error()
			return finish(nil, err)
		}
	}

	trace.ScannedRows = budget.used
	trace.ActualRows = int64(len(rows))
	trace.ActualReturnedRows = int64(len(rows))
	trace.MemoryEstimateBytes = budget.used * rowMemoryEstimateBytes
	for _, s := range opStatsList {
		trace.OperatorStats = append(trace.OperatorStats, OpStat{Operator: s.Operator, RowsIn: s.RowsIn, RowsOut: s.RowsOut, Batches: s.Batches})
	}
	if len(opStatsList) > 0 {
		trace.Batches = opStatsList[len(opStatsList)-1].Batches
	}
	return finish(rows, nil)
}

// cappedScanIter wraps a RowIterator with the shared scan budget.
type cappedScanIter struct {
	it     RowIterator
	batch  int
	stats  *opStats
	budget *scanBudget
}

func (c *cappedScanIter) NextBatch() ([]Row, error) {
	rows, err := c.it.NextBatch(c.batch)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	if err := c.budget.add(len(rows)); err != nil {
		return nil, err
	}
	c.stats.note(len(rows), len(rows))
	return rows, nil
}
