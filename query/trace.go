package query

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"
)

// Trace is the unified trace record for one query execution. It uses
// the same field vocabulary the architecture spec intends for
// transactions and Autopilot decisions (plan, estimated cost, actual
// cost, validation result, commit boundary), so a future optimizer
// can learn from query traces and transaction traces alike.
//
// Estimated vs actual rows are first-class fields, not log lines:
// a learned estimator is judged by comparing them across traces.
type Trace struct {
	// QueryID is the first 16 hex characters of the SHA-256 of the
	// canonical plan JSON. It identifies the plan, not the execution:
	// the same plan text re-run produces the same QueryID.
	QueryID string `json:"query_id"`
	// PlanSummary is the human-readable explanation of the plan.
	PlanSummary string `json:"plan_summary"`
	PlanVersion int    `json:"plan_version"`
	// Operators is the pipeline, in execution order.
	Operators []string `json:"operators"`
	// EstimatedRows is the planner estimate (-1 when unknown).
	// It is NOT a scanned-row count: in this slice the binder sets
	// it to the LIMIT when one is present, so for a LIMIT query it
	// equals the limit, whatever the scan actually read. Compare it
	// with ScannedRows, never with ResultLimit, to judge an estimate.
	EstimatedRows int64 `json:"estimated_rows"`
	// ActualRows is the number of rows returned (0 on failure).
	// It is kept as the historical name for ActualReturnedRows;
	// the two fields are always populated with the same value.
	ActualRows int64 `json:"actual_rows"`
	// ActualReturnedRows is the number of rows actually returned
	// (0 on failure). LIMIT caps this number; it does not cap
	// ScannedRows, and it is not an estimate of anything.
	ActualReturnedRows int64 `json:"actual_returned_rows"`
	// ResultLimit is the plan's LIMIT, or -1 when the plan has no
	// LIMIT. It is a cap on ActualReturnedRows only, never a count
	// of rows scanned or a row estimate.
	ResultLimit int64 `json:"result_limit"`
	// ScannedRows counts rows read from sources (both join sides).
	// It is the actual scanned count, independent of ResultLimit
	// and of EstimatedRows.
	ScannedRows int64 `json:"scanned_rows"`
	// Batches counts batches produced by the final pipeline stage.
	Batches int64 `json:"batches"`
	// Duration is wall-clock execution time, excluding verification
	// when verification failed (verification happens first and the
	// source is never opened in that case).
	Duration time.Duration `json:"duration_ns"`
	// MemoryEstimateBytes is the heuristic budget estimate
	// (scanned rows x 256 bytes in this slice), not a measurement.
	MemoryEstimateBytes int64 `json:"memory_estimate_bytes"`
	// ActualMemoryBytes is MEASURED, not estimated: the largest
	// in-memory retention observed in any single retaining operator
	// (hash-join build side, sort chunk buffer, grouped aggregation)
	// during this execution. Measurement method (see executor.go):
	// for each retained row, the sum of its encoded value sizes
	// (int/float 8, bool 1, string/bytes len, NULL 0) plus its
	// column-name lengths plus a documented 32-byte per-row
	// overhead, computed from the real rows actually retained —
	// never rows x a constant. The budget (Contract.MaxMemoryBytes)
	// is enforced per retaining operator against this measurement;
	// when a spill happens, retention stays at or under the budget
	// and the excess lives in spill files, so this peak is at most
	// the budget for a spilling query and the full measured size
	// for an unlimited-budget query. It is a per-operator peak, not
	// a sum across operators and not a runtime heap sample (Go has
	// no per-object heap meter); operators run in sequence, so the
	// peak is the honest retained-memory figure for the budget.
	ActualMemoryBytes int64 `json:"actual_memory_bytes"`
	// Spilled reports whether any retaining operator spilled rows
	// or groups to a temporary file during this execution.
	Spilled bool `json:"spilled,omitempty"`
	// ValidationError is non-nil when the plan was rejected before
	// execution. In that case ActualRows is 0 and ScannedRows is 0.
	ValidationError *VerificationError `json:"validation_error,omitempty"`
	// ExecutionError is non-empty when execution itself failed after
	// verification passed (source error, budget abort, type error).
	ExecutionError string `json:"execution_error,omitempty"`
	// OperatorStats are per-operator counters, in pipeline order.
	OperatorStats []OpStat `json:"operator_stats,omitempty"`
	// FallbackReason is reserved for future learned-index / optimizer
	// fallbacks. It is always empty in this slice; a non-empty value
	// in a later slice must name the fallback that actually happened.
	FallbackReason string `json:"fallback_reason,omitempty"`
	// CommitBoundary describes the visibility contract of the read:
	// in this slice, every query is read-only and reads a snapshot
	// frozen at source Open (see Source), so no commit is involved.
	CommitBoundary string `json:"commit_boundary"`
}

// OpStat is one operator's counters in a trace.
type OpStat struct {
	Operator string `json:"operator"`
	RowsIn   int64  `json:"rows_in"`
	RowsOut  int64  `json:"rows_out"`
	Batches  int64  `json:"batches"`
}

// PlanHash returns the SHA-256 of the plan's canonical JSON encoding.
func PlanHash(plan *Plan) (string, error) {
	data, err := json.Marshal(plan)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

// MarshalPlan serializes a plan to JSON (the proof-carrying form).
func MarshalPlan(plan *Plan) ([]byte, error) {
	return json.MarshalIndent(plan, "", "  ")
}

// UnmarshalPlan deserializes a plan. Deserialization does not verify:
// callers must run Verify on the result before executing it, exactly
// as they must for a freshly compiled plan.
func UnmarshalPlan(data []byte) (*Plan, error) {
	var plan Plan
	if err := json.Unmarshal(data, &plan); err != nil {
		return nil, err
	}
	return &plan, nil
}
