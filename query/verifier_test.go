package query

import (
	"context"
	"strings"
	"testing"
)

// countingSource records how many times Open was called, to prove
// verification failures never touch a source.
type countingSource struct {
	inner *MemorySource
	opens int
}

func (s *countingSource) Estimate() int64 { return s.inner.Estimate() }
func (s *countingSource) Open(ctx context.Context, b Binding) (RowIterator, error) {
	s.opens++
	return s.inner.Open(ctx, b)
}

func TestVerifierPermissions(t *testing.T) {
	plan, err := Compile("SELECT name FROM users", testSchema())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	err = Verify(plan, Contract{Role: "analyst", AllowedTables: []string{"orders"}})
	ve, ok := err.(*VerificationError)
	if !ok || ve.Code != "PERMISSION_DENIED" {
		t.Fatalf("verify = %v, want PERMISSION_DENIED", err)
	}
	// Same plan, allowed table: passes.
	if err := Verify(plan, Contract{Role: "analyst", AllowedTables: []string{"users"}}); err != nil {
		t.Fatalf("verify allowed: %v", err)
	}
	// Join touching a denied table is denied too.
	plan2, _ := Compile("SELECT users.name FROM users JOIN orders ON users.key = orders.user_id", testSchema())
	if err := Verify(plan2, Contract{AllowedTables: []string{"users"}}); err == nil {
		t.Fatalf("verify join with denied table: expected PERMISSION_DENIED, got nil")
	}
}

func TestVerifierBudgetsAndVersion(t *testing.T) {
	plan, err := Compile("SELECT name FROM users LIMIT 100", testSchema())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if err := Verify(plan, Contract{MaxResultRows: 10}); err == nil {
		t.Fatalf("verify LIMIT 100 with max 10: expected BUDGET_EXCEEDED, got nil")
	} else if ve, ok := err.(*VerificationError); !ok || ve.Code != "BUDGET_EXCEEDED" {
		t.Fatalf("verify = %v, want BUDGET_EXCEEDED", err)
	}
	bad := *plan
	bad.Version = 99
	if err := Verify(&bad, Contract{}); err == nil {
		t.Fatalf("verify version 99: expected error, got nil")
	} else if ve, ok := err.(*VerificationError); !ok || ve.Code != "UNSUPPORTED_PLAN_VERSION" {
		t.Fatalf("verify = %v, want UNSUPPORTED_PLAN_VERSION", err)
	}
	if err := Verify(nil, Contract{}); err == nil {
		t.Fatalf("verify nil plan: expected error, got nil")
	}
}

func TestVerificationRejectsBeforeOpen(t *testing.T) {
	plan, err := Compile("SELECT name FROM users", testSchema())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	cs := &countingSource{inner: memUsers(t)}
	rows, trace, err := Execute(context.Background(), plan, Contract{AllowedTables: []string{"orders"}}, map[string]Source{"users": cs})
	if err == nil {
		t.Fatalf("execute denied: expected error, got nil")
	}
	if rows != nil {
		t.Fatalf("execute denied: rows should be nil, got %v", rows)
	}
	if cs.opens != 0 {
		t.Fatalf("source opened %d times on verification failure, want 0", cs.opens)
	}
	if trace == nil || trace.ValidationError == nil || trace.ValidationError.Code != "PERMISSION_DENIED" {
		t.Fatalf("trace should carry PERMISSION_DENIED, got %+v", trace)
	}
	if trace.ScannedRows != 0 || trace.ActualRows != 0 {
		t.Fatalf("rejected trace rows: scanned=%d actual=%d, want 0/0", trace.ScannedRows, trace.ActualRows)
	}
}

func TestRuntimeBudgets(t *testing.T) {
	plan, err := Compile("SELECT name FROM users", testSchema())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	// Scanned cap: 5 rows against a cap of 2 aborts during execution.
	_, trace, err := Execute(context.Background(), plan, Contract{MaxScannedRows: 2}, map[string]Source{"users": memUsers(t)})
	if err == nil {
		t.Fatalf("execute over scan cap: expected error, got nil")
	}
	if !strings.Contains(err.Error(), "BUDGET_EXCEEDED") {
		t.Fatalf("scan cap error = %v, want BUDGET_EXCEEDED", err)
	}
	if trace == nil || trace.ExecutionError == "" {
		t.Fatalf("trace should carry execution error, got %+v", trace)
	}
	// Result cap without LIMIT: 5 rows against a cap of 2 aborts.
	_, _, err = Execute(context.Background(), plan, Contract{MaxResultRows: 2}, map[string]Source{"users": memUsers(t)})
	if err == nil || !strings.Contains(err.Error(), "BUDGET_EXCEEDED") {
		t.Fatalf("result cap error = %v, want BUDGET_EXCEEDED", err)
	}
}

func TestVerifierUnknownEstimateDefersToRuntime(t *testing.T) {
	// A plan with unknown estimate (-1) must NOT be rejected by the
	// verifier on scanned-row grounds: the check is deferred to the
	// engine's runtime cap, which the test above proves aborts.
	plan, err := Compile("SELECT name FROM users", testSchema())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if plan.EstimatedRows != -1 {
		t.Fatalf("estimated rows = %d, want -1", plan.EstimatedRows)
	}
	if err := Verify(plan, Contract{MaxScannedRows: 2}); err != nil {
		t.Fatalf("verify with unknown estimate should defer, got %v", err)
	}
}
