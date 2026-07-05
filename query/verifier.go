package query

import (
	"fmt"
	"strings"
)

// Contract is the Query Contract a plan must satisfy before it may
// execute. It is the proof-carrying part of a query: schema bindings,
// operation types, permissions, and resource budgets travel with the
// plan, and the verifier checks them all before any source is opened.
// Zero values mean "no limit" for the numeric budgets, and "all
// tables allowed" for AllowedTables — both are deliberate defaults
// for local, trusted use, and both must be set explicitly for
// AI-generated or untrusted queries.
type Contract struct {
	Role           string   `json:"role"`
	AllowedTables  []string `json:"allowed_tables,omitempty"`
	MaxScannedRows int64    `json:"max_scanned_rows,omitempty"`
	MaxResultRows  int64    `json:"max_result_rows,omitempty"`
	MaxMemoryBytes int64    `json:"max_memory_bytes,omitempty"`
}

// VerificationError is a checkable rejection reason. Code is stable
// and machine-readable; callers must branch on Code, not on the
// message text.
type VerificationError struct {
	Code string `json:"code"`
	Msg  string `json:"msg"`
}

func (e *VerificationError) Error() string { return fmt.Sprintf("%s: %s", e.Code, e.Msg) }

func verr(code, format string, args ...interface{}) error {
	return &VerificationError{Code: code, Msg: fmt.Sprintf(format, args...)}
}

// rowMemoryEstimateBytes is the per-row memory heuristic used for
// budget checks when no statistics exist: 256 bytes per row. It is a
// deliberately crude, documented constant — not a measurement — and
// the trace records it so a future estimator can replace it with
// real statistics and be judged against the same field.
const rowMemoryEstimateBytes = 256

var allowedOperators = map[string]bool{
	"SCAN": true, "JOIN": true, "FILTER": true, "AGGREGATE": true,
	"PROJECT": true, "SORT": true, "LIMIT": true,
}

// Verify checks a compiled plan against a contract. It performs no
// I/O and opens no source: a plan that fails here is rejected before
// execution, with a checkable reason, and the engine guarantees the
// source is never opened in that case (tested).
func Verify(plan *Plan, contract Contract) error {
	if plan == nil {
		return verr("BAD_PLAN", "plan is nil")
	}
	if plan.Version != PlanVersion {
		return verr("UNSUPPORTED_PLAN_VERSION", "plan version %d is not supported (current: %d)", plan.Version, PlanVersion)
	}
	if len(plan.Projection) == 0 {
		return verr("BAD_PLAN", "plan has no projection")
	}
	for _, op := range plan.Operators {
		if !allowedOperators[op] {
			return verr("OPERATION_NOT_ALLOWED", "operator %q is not allowed", op)
		}
	}
	// Permissions: every table the plan reads must be allowed.
	allowed := func(name string) bool {
		if len(contract.AllowedTables) == 0 {
			return true
		}
		for _, t := range contract.AllowedTables {
			if strings.EqualFold(t, name) {
				return true
			}
		}
		return false
	}
	if !allowed(plan.FromTable) {
		return verr("PERMISSION_DENIED", "role %q may not read table %q", contract.Role, plan.FromTable)
	}
	if plan.Join != nil && !allowed(plan.Join.RightTable) {
		return verr("PERMISSION_DENIED", "role %q may not read table %q", contract.Role, plan.Join.RightTable)
	}
	// Bindings must describe the tables the plan claims to read.
	bound := map[string]bool{}
	for _, b := range plan.Bindings {
		bound[strings.ToLower(b.Alias)] = true
	}
	if !bound[strings.ToLower(plan.FromAlias)] {
		return verr("BAD_PLAN", "plan has no binding for FROM alias %q", plan.FromAlias)
	}
	if plan.Join != nil && !bound[strings.ToLower(plan.Join.RightAlias)] {
		return verr("BAD_PLAN", "plan has no binding for JOIN alias %q", plan.Join.RightAlias)
	}
	// Output columns must be unique (ORDER BY and consumers rely on it).
	seen := map[string]bool{}
	for _, c := range plan.OutputColumns {
		lower := strings.ToLower(c.Name)
		if seen[lower] {
			return verr("BAD_PLAN", "duplicate output column %q", c.Name)
		}
		seen[lower] = true
	}
	// Static resource budgets. Scanned-row and memory budgets that
	// depend on an unknown estimate are deferred to the engine's
	// runtime caps (documented in engine.go); they are not silently
	// treated as "within budget" here.
	if plan.HasLimit && plan.Limit != nil && contract.MaxResultRows > 0 && *plan.Limit > contract.MaxResultRows {
		return verr("BUDGET_EXCEEDED", "LIMIT %d exceeds max result rows %d", *plan.Limit, contract.MaxResultRows)
	}
	if plan.EstimatedRows >= 0 {
		if contract.MaxScannedRows > 0 && plan.EstimatedRows > contract.MaxScannedRows {
			return verr("BUDGET_EXCEEDED", "estimated rows %d exceed max scanned rows %d", plan.EstimatedRows, contract.MaxScannedRows)
		}
		if contract.MaxMemoryBytes > 0 {
			est := plan.EstimatedRows * rowMemoryEstimateBytes
			if est > contract.MaxMemoryBytes {
				return verr("BUDGET_EXCEEDED", "estimated memory %d bytes exceeds budget %d bytes", est, contract.MaxMemoryBytes)
			}
		}
	}
	return nil
}
