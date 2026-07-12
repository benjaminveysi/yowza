package query

import (
	"context"
	"strings"
	"testing"
)

// Trace separation: LIMIT, scanned rows, and returned rows are three
// independent numbers. A plain LIMIT short-circuits the scan (only
// the first batch is read), so the separation query sorts first:
// ORDER BY forces a full scan before LIMIT can apply, which is
// exactly the case where conflating the three numbers misleads.

func TestTraceLimitScannedReturnedSeparation(t *testing.T) {
	users := makeManyUsers(t, 1000)
	plan, err := Compile("SELECT name, age FROM users ORDER BY age LIMIT 10", testSchema())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	rows, trace, err := Execute(context.Background(), plan, Contract{}, map[string]Source{"users": users})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if len(rows) != 10 {
		t.Fatalf("rows = %d, want 10", len(rows))
	}
	if trace.ResultLimit != 10 {
		t.Fatalf("ResultLimit = %d, want 10", trace.ResultLimit)
	}
	if trace.ScannedRows != 1000 {
		t.Fatalf("ScannedRows = %d, want 1000 (full scan before LIMIT)", trace.ScannedRows)
	}
	if trace.ActualReturnedRows != 10 {
		t.Fatalf("ActualReturnedRows = %d, want 10", trace.ActualReturnedRows)
	}
	if trace.ActualRows != trace.ActualReturnedRows {
		t.Fatalf("ActualRows (%d) must equal ActualReturnedRows (%d) — backward-compatible alias",
			trace.ActualRows, trace.ActualReturnedRows)
	}
	// EstimatedRows is the planner estimate (the LIMIT, in this
	// slice), not the scanned count: asserting it equals 10 while
	// scanned is 1000 is the separation itself.
	if trace.EstimatedRows != 10 {
		t.Fatalf("EstimatedRows = %d, want 10 (planner estimate = limit in this slice)", trace.EstimatedRows)
	}
	if trace.MemoryEstimateBytes != 1000*256 {
		t.Fatalf("MemoryEstimateBytes = %d, want %d (heuristic: scanned x 256)",
			trace.MemoryEstimateBytes, 1000*256)
	}
	if trace.ActualMemoryBytes <= 0 || trace.ActualMemoryBytes == trace.MemoryEstimateBytes {
		t.Fatalf("ActualMemoryBytes = %d, want a positive measured value distinct from the %d heuristic",
			trace.ActualMemoryBytes, trace.MemoryEstimateBytes)
	}
}

func TestTraceWithoutLimitReportsMinusOne(t *testing.T) {
	users := makeManyUsers(t, 1000)
	plan, err := Compile("SELECT name FROM users", testSchema())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	rows, trace, err := Execute(context.Background(), plan, Contract{}, map[string]Source{"users": users})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if trace.ResultLimit != -1 {
		t.Fatalf("ResultLimit = %d, want -1 (no LIMIT)", trace.ResultLimit)
	}
	if trace.ScannedRows != 1000 || trace.ActualReturnedRows != 1000 || len(rows) != 1000 {
		t.Fatalf("no-limit: scanned=%d returned=%d rows=%d, want 1000/1000/1000",
			trace.ScannedRows, trace.ActualReturnedRows, len(rows))
	}
	if trace.EstimatedRows != -1 {
		t.Fatalf("EstimatedRows = %d, want -1 (unknown, no stats, no limit)", trace.EstimatedRows)
	}
}

func TestExplainTraceShowsIndependentNumbers(t *testing.T) {
	users := makeManyUsers(t, 1000)
	plan, err := Compile("SELECT name, age FROM users ORDER BY age LIMIT 10", testSchema())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	_, trace, err := Execute(context.Background(), plan, Contract{}, map[string]Source{"users": users})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	out := ExplainTrace(trace)
	for _, want := range []string{
		"RESULT LIMIT 10",
		"SCANNED ROWS 1000",
		"ACTUAL RETURNED ROWS 10",
		"MEMORY ESTIMATE BYTES",
		"ACTUAL MEMORY BYTES",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("ExplainTrace missing %q:\n%s", want, out)
		}
	}
	// No-limit explanation must say so explicitly, not print 0.
	plan2, _ := Compile("SELECT name FROM users", testSchema())
	_, trace2, err := Execute(context.Background(), plan2, Contract{}, map[string]Source{"users": users})
	if err != nil {
		t.Fatalf("execute no-limit: %v", err)
	}
	if out2 := ExplainTrace(trace2); !strings.Contains(out2, "RESULT LIMIT none (-1)") {
		t.Fatalf("no-limit ExplainTrace should say none (-1):\n%s", out2)
	}
	if ExplainTrace(nil) == "" {
		t.Fatalf("ExplainTrace(nil) should return a placeholder, not empty")
	}
}
