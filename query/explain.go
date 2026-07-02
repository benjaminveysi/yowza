package query

import (
	"fmt"
	"strings"
)

// Explain renders a plan as human-readable text: what it reads, what
// it filters, how it groups, and in what order the pipeline runs.
// It is the "query explanation" capability from the spec, in its
// simplest honest form: it describes the verified plan, it does not
// invent a reason a plan was chosen (there is no cost-based choice
// between alternative plans in this slice).
func Explain(plan *Plan) string {
	var sb strings.Builder
	sb.WriteString("PLAN v")
	sb.WriteString(fmt.Sprintf("%d", plan.Version))
	sb.WriteString("\n")
	// SELECT list
	var items []string
	for _, it := range plan.Projection {
		if it.Agg != "" {
			if it.Star {
				items = append(items, "COUNT(*)")
			} else {
				items = append(items, it.Agg+"("+it.Col.Qualified+")")
			}
		} else {
			items = append(items, it.Col.Qualified)
		}
		if it.Alias != "" {
			items[len(items)-1] += " AS " + it.Alias
		}
	}
	sb.WriteString("  SELECT " + strings.Join(items, ", ") + "\n")
	sb.WriteString(fmt.Sprintf("  FROM %s AS %s\n", plan.FromTable, plan.FromAlias))
	if plan.Join != nil {
		sb.WriteString(fmt.Sprintf("  JOIN %s AS %s ON %s = %s\n",
			plan.Join.RightTable, plan.Join.RightAlias,
			plan.Join.LeftCol.Qualified, plan.Join.RightCol.Qualified))
	}
	if plan.Filter != nil {
		sb.WriteString("  WHERE " + explainExpr(plan.Filter) + "\n")
	}
	if len(plan.GroupBy) > 0 {
		var gs []string
		for _, g := range plan.GroupBy {
			gs = append(gs, g.Qualified)
		}
		sb.WriteString("  GROUP BY " + strings.Join(gs, ", ") + "\n")
	}
	if len(plan.OrderBy) > 0 {
		var os []string
		for _, o := range plan.OrderBy {
			dir := "ASC"
			if o.Desc {
				dir = "DESC"
			}
			os = append(os, o.Name+" "+dir)
		}
		sb.WriteString("  ORDER BY " + strings.Join(os, ", ") + "\n")
	}
	if plan.HasLimit && plan.Limit != nil {
		sb.WriteString(fmt.Sprintf("  LIMIT %d\n", *plan.Limit))
	}
	sb.WriteString("  PIPELINE " + strings.Join(plan.Operators, " -> ") + "\n")
	if plan.EstimatedRows >= 0 {
		sb.WriteString(fmt.Sprintf("  ESTIMATED ROWS %d\n", plan.EstimatedRows))
	} else {
		sb.WriteString("  ESTIMATED ROWS unknown (no table statistics in this slice)\n")
	}
	// LIMIT is a cap on returned rows, not an estimate of scanned
	// rows: a LIMIT 10 query over 1000 scanned rows scans 1000 and
	// returns 10. See ExplainTrace for the three independent numbers.
	sb.WriteString("  NOTE: LIMIT (when present) caps returned rows only; it is not a scanned-row estimate\n")
	return sb.String()
}

// ExplainTrace renders a finished trace with LIMIT, scanned rows,
// and returned rows as three independent numbers. It exists because
// the plan text alone cannot show them: EstimatedRows is the
// planner's estimate (the LIMIT, when one is present, in this slice),
// ScannedRows is the actual scan count, and ActualReturnedRows is
// what the query actually returned. Conflating any two of them
// misreads the trace, so this output keeps them on separate lines
// with separate labels, and likewise keeps the heuristic memory
// estimate apart from the measured memory figure.
func ExplainTrace(t *Trace) string {
	if t == nil {
		return "TRACE <nil>\n"
	}
	var sb strings.Builder
	sb.WriteString("TRACE\n")
	if t.QueryID != "" {
		sb.WriteString(fmt.Sprintf("  QUERY ID %s\n", t.QueryID))
	}
	sb.WriteString(fmt.Sprintf("  PLAN VERSION %d\n", t.PlanVersion))
	if len(t.Operators) > 0 {
		sb.WriteString("  PIPELINE " + strings.Join(t.Operators, " -> ") + "\n")
	}
	if t.EstimatedRows >= 0 {
		sb.WriteString(fmt.Sprintf("  ESTIMATED ROWS %d\n", t.EstimatedRows))
	} else {
		sb.WriteString("  ESTIMATED ROWS unknown\n")
	}
	sb.WriteString(fmt.Sprintf("  SCANNED ROWS %d\n", t.ScannedRows))
	if t.ResultLimit >= 0 {
		sb.WriteString(fmt.Sprintf("  RESULT LIMIT %d\n", t.ResultLimit))
	} else {
		sb.WriteString("  RESULT LIMIT none (-1)\n")
	}
	sb.WriteString(fmt.Sprintf("  ACTUAL RETURNED ROWS %d\n", t.ActualReturnedRows))
	sb.WriteString(fmt.Sprintf("  MEMORY ESTIMATE BYTES %d (heuristic: scanned rows x 256)\n", t.MemoryEstimateBytes))
	sb.WriteString(fmt.Sprintf("  ACTUAL MEMORY BYTES %d (measured from retained rows; see Trace.ActualMemoryBytes)\n", t.ActualMemoryBytes))
	if t.Spilled {
		sb.WriteString("  SPILLED yes\n")
	} else {
		sb.WriteString("  SPILLED no\n")
	}
	if t.ValidationError != nil {
		sb.WriteString(fmt.Sprintf("  VALIDATION ERROR %s\n", t.ValidationError.Error()))
	}
	if t.ExecutionError != "" {
		sb.WriteString(fmt.Sprintf("  EXECUTION ERROR %s\n", t.ExecutionError))
	}
	return sb.String()
}

func explainExpr(e *Expr) string {
	if e == nil {
		return ""
	}
	switch e.Kind {
	case "and", "or":
		var parts []string
		for _, a := range e.Args {
			parts = append(parts, explainExpr(a))
		}
		return "(" + strings.Join(parts, " "+strings.ToUpper(e.Kind)+" ") + ")"
	case "not":
		return "NOT (" + explainExpr(e.Args[0]) + ")"
	case "cmp":
		return explainOperand(e.Left) + " " + e.Op + " " + explainOperand(e.Right)
	case "in":
		var vs []string
		for _, v := range e.InList {
			vs = append(vs, v.String())
		}
		neg := ""
		if e.Negated {
			neg = "NOT "
		}
		return explainOperand(e.Left) + " " + neg + "IN (" + strings.Join(vs, ", ") + ")"
	case "like":
		neg := ""
		if e.Negated {
			neg = "NOT "
		}
		return explainOperand(e.Left) + " " + neg + "LIKE '" + e.Pattern + "'"
	case "isnull":
		neg := ""
		if e.Negated {
			neg = " NOT"
		}
		return explainOperand(e.Left) + " IS" + neg + " NULL"
	default:
		return "?"
	}
}

func explainOperand(e *Expr) string {
	if e == nil {
		return "?"
	}
	switch e.Kind {
	case "col":
		if e.Col != nil {
			return e.Col.Qualified
		}
		return "?"
	case "lit":
		if e.Lit != nil {
			if e.Lit.Kind == KindString {
				return "'" + e.Lit.S + "'"
			}
			return e.Lit.String()
		}
		return "?"
	default:
		return "?"
	}
}
