package query

import (
	"fmt"
	"strings"
)

// BindError is a schema/type error found while binding an AST to a
// Plan. It is distinct from a ParseError: the text parsed, but it does
// not name real, unambiguous, type-correct things.
type BindError struct {
	Code string
	Msg  string
}

func (e *BindError) Error() string { return fmt.Sprintf("%s: %s", e.Code, e.Msg) }

func bindErr(code, format string, args ...interface{}) error {
	return &BindError{Code: code, Msg: fmt.Sprintf(format, args...)}
}

// Compile parses text and binds it against the schema, producing a
// typed Plan. Compilation performs no I/O and touches no data source:
// a compiled plan can be serialized, hashed, verified, and explained
// before anything is executed.
func Compile(text string, schema *Schema) (*Plan, error) {
	if schema == nil {
		return nil, bindErr("NO_SCHEMA", "schema is nil")
	}
	if err := schema.Validate(); err != nil {
		return nil, bindErr("BAD_SCHEMA", "%v", err)
	}
	ast, err := Parse(text)
	if err != nil {
		return nil, err
	}
	return Bind(ast, schema, text)
}

type binder struct {
	schema    *Schema
	from      Table
	fromAlias string
	join      *Table
	joinAlias string
}

func (b *binder) aliasOf(tableName string) string {
	if strings.EqualFold(b.from.Name, tableName) {
		return b.fromAlias
	}
	if b.join != nil && strings.EqualFold(b.join.Name, tableName) {
		return b.joinAlias
	}
	return ""
}

// resolveColumn binds one column reference. Table may name a table or
// an alias; an empty table searches both sides and errors on ambiguity.
func (b *binder) resolveColumn(c *UnresolvedColumn) (*ColumnRef, error) {
	if c == nil {
		return nil, bindErr("BAD_COLUMN", "nil column reference")
	}
	if c.Table != "" {
		// Match by table name or by alias.
		if strings.EqualFold(c.Table, b.from.Name) || strings.EqualFold(c.Table, b.fromAlias) {
			col, ok := b.from.ColumnByName(c.Name)
			if !ok {
				return nil, bindErr("UNKNOWN_COLUMN", "table %q has no column %q", b.from.Name, c.Name)
			}
			return &ColumnRef{Table: b.from.Name, Alias: b.fromAlias, Name: col.Name, Type: col.Type, Qualified: b.fromAlias + "." + col.Name}, nil
		}
		if b.join != nil && (strings.EqualFold(c.Table, b.join.Name) || strings.EqualFold(c.Table, b.joinAlias)) {
			col, ok := b.join.ColumnByName(c.Name)
			if !ok {
				return nil, bindErr("UNKNOWN_COLUMN", "table %q has no column %q", b.join.Name, c.Name)
			}
			return &ColumnRef{Table: b.join.Name, Alias: b.joinAlias, Name: col.Name, Type: col.Type, Qualified: b.joinAlias + "." + col.Name}, nil
		}
		return nil, bindErr("UNKNOWN_TABLE", "unknown table or alias %q", c.Table)
	}
	// Unqualified: exactly one side may carry the name.
	var found *ColumnRef
	if col, ok := b.from.ColumnByName(c.Name); ok {
		found = &ColumnRef{Table: b.from.Name, Alias: b.fromAlias, Name: col.Name, Type: col.Type, Qualified: b.fromAlias + "." + col.Name}
	}
	if b.join != nil {
		if col, ok := b.join.ColumnByName(c.Name); ok {
			if found != nil {
				return nil, bindErr("AMBIGUOUS_COLUMN", "column %q exists in both %q and %q; qualify it", c.Name, b.from.Name, b.join.Name)
			}
			found = &ColumnRef{Table: b.join.Name, Alias: b.joinAlias, Name: col.Name, Type: col.Type, Qualified: b.joinAlias + "." + col.Name}
		}
	}
	if found == nil {
		return nil, bindErr("UNKNOWN_COLUMN", "unknown column %q", c.Name)
	}
	return found, nil
}

// exprType returns the static type of a bound expression operand, or an
// error when the operand cannot be typed (e.g. a column of unknown
// type, which cannot happen after schema validation).
func exprOperandType(e *Expr) (Kind, error) {
	switch e.Kind {
	case "col":
		if e.Col == nil {
			return KindNull, bindErr("BAD_EXPR", "column expression without a column")
		}
		return e.Col.Type, nil
	case "lit":
		if e.Lit == nil {
			return KindNull, bindErr("BAD_EXPR", "literal expression without a value")
		}
		return e.Lit.Kind, nil
	default:
		return KindNull, bindErr("BAD_EXPR", "expression of kind %q is not an operand", e.Kind)
	}
}

// compatibleForCompare reports whether two operand types may be
// compared with the given operator. NULL literals are compatible with
// anything (the comparison is not-true at runtime, not an error).
func compatibleForCompare(op string, lt, rt Kind) error {
	if lt == KindNull || rt == KindNull {
		return nil
	}
	if lt.IsNumericKind() && rt.IsNumericKind() {
		return nil
	}
	if lt != rt {
		return fmt.Errorf("cannot compare %s with %s", lt, rt)
	}
	switch lt {
	case KindBool:
		if op != "=" && op != "!=" && op != "<>" {
			return fmt.Errorf("bool columns only support = and !=")
		}
	case KindBytes:
		if op != "=" && op != "!=" && op != "<>" {
			return fmt.Errorf("bytes columns only support = and !=")
		}
	}
	return nil
}

// IsNumericKind reports whether a Kind is int or float.
func (k Kind) IsNumericKind() bool { return k == KindInt || k == KindFloat }

func (b *binder) bindExpr(e *ASTExpr) (*Expr, error) {
	if e == nil {
		return nil, nil
	}
	switch e.Kind {
	case "and", "or":
		if len(e.Args) != 2 {
			return nil, bindErr("BAD_EXPR", "%s requires exactly two operands", e.Kind)
		}
		var args []*Expr
		for _, a := range e.Args {
			be, err := b.bindExpr(a)
			if err != nil {
				return nil, err
			}
			args = append(args, be)
		}
		return &Expr{Kind: e.Kind, Args: args}, nil
	case "not":
		if len(e.Args) != 1 {
			return nil, bindErr("BAD_EXPR", "NOT requires exactly one operand")
		}
		inner, err := b.bindExpr(e.Args[0])
		if err != nil {
			return nil, err
		}
		return &Expr{Kind: "not", Args: []*Expr{inner}}, nil
	case "cmp":
		left, err := b.bindOperand(e.Left)
		if err != nil {
			return nil, err
		}
		right, err := b.bindOperand(e.Right)
		if err != nil {
			return nil, err
		}
		lt, err := exprOperandType(left)
		if err != nil {
			return nil, err
		}
		rt, err := exprOperandType(right)
		if err != nil {
			return nil, err
		}
		if err := compatibleForCompare(e.Op, lt, rt); err != nil {
			return nil, bindErr("TYPE_MISMATCH", "%v", err)
		}
		return &Expr{Kind: "cmp", Op: e.Op, Left: left, Right: right}, nil
	case "in":
		left, err := b.bindOperand(e.Left)
		if err != nil {
			return nil, err
		}
		lt, err := exprOperandType(left)
		if err != nil {
			return nil, err
		}
		for _, v := range e.InList {
			if v.Kind != KindNull && lt != KindNull {
				if err := compatibleForCompare("=", lt, v.Kind); err != nil {
					return nil, bindErr("TYPE_MISMATCH", "IN list: %v", err)
				}
			}
		}
		return &Expr{Kind: "in", Left: left, InList: append([]Value(nil), e.InList...), Negated: e.Negated}, nil
	case "like":
		left, err := b.bindOperand(e.Left)
		if err != nil {
			return nil, err
		}
		lt, err := exprOperandType(left)
		if err != nil {
			return nil, err
		}
		if lt != KindString {
			return nil, bindErr("TYPE_MISMATCH", "LIKE requires a string column, got %s", lt)
		}
		return &Expr{Kind: "like", Left: left, Pattern: e.Pattern, Negated: e.Negated}, nil
	case "isnull":
		left, err := b.bindOperand(e.Left)
		if err != nil {
			return nil, err
		}
		return &Expr{Kind: "isnull", Left: left, Negated: e.Negated}, nil
	default:
		return nil, bindErr("BAD_EXPR", "unsupported expression kind %q", e.Kind)
	}
}

func (b *binder) bindOperand(e *ASTExpr) (*Expr, error) {
	if e == nil {
		return nil, bindErr("BAD_EXPR", "nil operand")
	}
	switch e.Kind {
	case "col":
		c, err := b.resolveColumn(e.Col)
		if err != nil {
			return nil, err
		}
		return &Expr{Kind: "col", Col: c}, nil
	case "lit":
		return &Expr{Kind: "lit", Lit: &e.Lit}, nil
	default:
		return nil, bindErr("BAD_EXPR", "operand must be a column or literal, got %q", e.Kind)
	}
}

// Bind resolves an AST against a schema into a typed Plan.
func Bind(ast *ASTQuery, schema *Schema, queryText string) (*Plan, error) {
	from, ok := schema.TableByName(ast.FromTable)
	if !ok {
		return nil, bindErr("UNKNOWN_TABLE", "unknown table %q", ast.FromTable)
	}
	b := &binder{schema: schema, from: from, fromAlias: from.Name}
	if ast.FromAlias != "" {
		b.fromAlias = ast.FromAlias
	}
	plan := &Plan{
		Version:       PlanVersion,
		QueryText:     queryText,
		FromTable:     from.Name,
		FromAlias:     b.fromAlias,
		EstimatedRows: -1,
	}
	plan.Bindings = append(plan.Bindings, Binding{Table: from.Name, Alias: b.fromAlias, Prefix: from.Prefix, Columns: from.Columns})

	var joinSpec *JoinSpec
	if ast.Join != nil {
		jt, ok := schema.TableByName(ast.Join.Table)
		if !ok {
			return nil, bindErr("UNKNOWN_TABLE", "unknown table %q", ast.Join.Table)
		}
		b.join = &jt
		b.joinAlias = jt.Name
		if ast.Join.Alias != "" {
			b.joinAlias = ast.Join.Alias
		}
		if strings.EqualFold(b.joinAlias, b.fromAlias) {
			return nil, bindErr("AMBIGUOUS_TABLE", "join alias %q collides with FROM alias", b.joinAlias)
		}
		plan.Bindings = append(plan.Bindings, Binding{Table: jt.Name, Alias: b.joinAlias, Prefix: jt.Prefix, Columns: jt.Columns})
		// ON must be a single equality between one column of each side.
		on, err := b.bindExpr(ast.Join.On)
		if err != nil {
			return nil, err
		}
		if on.Kind != "cmp" || on.Op != "=" || on.Left.Kind != "col" || on.Right.Kind != "col" {
			return nil, bindErr("BAD_JOIN", "JOIN ON must be a single equality between two columns")
		}
		lc, rc := on.Left.Col, on.Right.Col
		if lc.Table == rc.Table {
			return nil, bindErr("BAD_JOIN", "JOIN ON columns must come from different tables")
		}
		// Normalize: LeftCol is always the FROM side.
		if lc.Table != from.Name {
			lc, rc = rc, lc
		}
		if err := compatibleForCompare("=", lc.Type, rc.Type); err != nil {
			return nil, bindErr("TYPE_MISMATCH", "JOIN keys: %v", err)
		}
		joinSpec = &JoinSpec{RightTable: jt.Name, RightAlias: b.joinAlias, LeftCol: lc, RightCol: rc}
		plan.Join = joinSpec
	}

	if ast.Where != nil {
		f, err := b.bindExpr(ast.Where)
		if err != nil {
			return nil, err
		}
		plan.Filter = f
	}

	// GROUP BY, bound before projection so projection rules can use it.
	groupSet := map[string]bool{}
	for _, g := range ast.GroupBy {
		c, err := b.resolveColumn(&g)
		if err != nil {
			return nil, err
		}
		plan.GroupBy = append(plan.GroupBy, *c)
		groupSet[c.Qualified] = true
	}
	hasAgg := false
	for _, it := range ast.Select {
		if it.Agg != "" {
			hasAgg = true
		}
	}
	grouped := hasAgg || len(ast.GroupBy) > 0

	// Projection.
	if ast.Star {
		var tables []Table
		tables = append(tables, from)
		if b.join != nil {
			tables = append(tables, *b.join)
		}
		seenOut := map[string]bool{}
		for _, t := range tables {
			for _, col := range t.Columns {
				out := col.Name
				lower := strings.ToLower(out)
				if seenOut[lower] {
					return nil, bindErr("AMBIGUOUS_COLUMN", "SELECT * with JOIN: output column %q appears in more than one table; select columns explicitly", out)
				}
				seenOut[lower] = true
				alias := b.aliasOf(t.Name)
				plan.Projection = append(plan.Projection, ProjItem{
					Col:        &ColumnRef{Table: t.Name, Alias: alias, Name: col.Name, Type: col.Type, Qualified: alias + "." + col.Name},
					OutputName: out,
					OutputType: col.Type,
				})
			}
		}
	} else {
		seenOut := map[string]bool{}
		for _, it := range ast.Select {
			var proj ProjItem
			if it.Agg != "" {
				switch it.Agg {
				case "COUNT":
					if it.Star {
						proj = ProjItem{Agg: "COUNT", Star: true, OutputType: KindInt}
					} else {
						c, err := b.resolveColumn(it.Col)
						if err != nil {
							return nil, err
						}
						proj = ProjItem{Agg: "COUNT", Col: c, OutputType: KindInt}
					}
				case "SUM", "AVG":
					c, err := b.resolveColumn(it.Col)
					if err != nil {
						return nil, err
					}
					if !c.Type.IsNumericKind() {
						return nil, bindErr("TYPE_MISMATCH", "%s requires a numeric column, got %s", it.Agg, c.Type)
					}
					outType := c.Type
					if it.Agg == "AVG" {
						outType = KindFloat
					}
					proj = ProjItem{Agg: it.Agg, Col: c, OutputType: outType}
				case "MIN", "MAX":
					c, err := b.resolveColumn(it.Col)
					if err != nil {
						return nil, err
					}
					if c.Type == KindBytes {
						return nil, bindErr("TYPE_MISMATCH", "%s does not support bytes columns", it.Agg)
					}
					proj = ProjItem{Agg: it.Agg, Col: c, OutputType: c.Type}
				default:
					return nil, bindErr("BAD_AGG", "unknown aggregate %q", it.Agg)
				}
				if it.Alias != "" {
					proj.Alias = it.Alias
					proj.OutputName = it.Alias
				} else if it.Star {
					proj.OutputName = "count"
				} else {
					proj.OutputName = strings.ToLower(it.Agg) + "_" + proj.Col.Name
				}
			} else {
				c, err := b.resolveColumn(it.Col)
				if err != nil {
					return nil, err
				}
				proj = ProjItem{Col: c, OutputType: c.Type}
				if it.Alias != "" {
					proj.Alias = it.Alias
					proj.OutputName = it.Alias
				} else {
					proj.OutputName = c.Name
				}
				if grouped && !groupSet[c.Qualified] {
					return nil, bindErr("AGG_MISUSE", "selected column %q is neither aggregated nor in GROUP BY", c.Qualified)
				}
			}
			lower := strings.ToLower(proj.OutputName)
			if seenOut[lower] {
				return nil, bindErr("AMBIGUOUS_COLUMN", "duplicate output column %q", proj.OutputName)
			}
			seenOut[lower] = true
			plan.Projection = append(plan.Projection, proj)
		}
	}
	if len(plan.Projection) == 0 {
		return nil, bindErr("BAD_QUERY", "SELECT list is empty")
	}

	// ORDER BY resolves against output columns.
	outNames := map[string]bool{}
	for _, pr := range plan.Projection {
		outNames[strings.ToLower(pr.OutputName)] = true
	}
	for _, o := range ast.OrderBy {
		if !outNames[strings.ToLower(o.Name)] {
			return nil, bindErr("UNKNOWN_COLUMN", "ORDER BY refers to unknown output column %q", o.Name)
		}
		plan.OrderBy = append(plan.OrderBy, OrderItem{Name: o.Name, Desc: o.Desc})
	}
	if ast.HasLimit {
		n := *ast.Limit
		plan.Limit = &n
		plan.HasLimit = true
		plan.EstimatedRows = n
	}
	for _, pr := range plan.Projection {
		plan.OutputColumns = append(plan.OutputColumns, Column{Name: pr.OutputName, Type: pr.OutputType, Source: ""})
	}

	// Operator pipeline, in execution order. Filter runs after JOIN in
	// this slice (WHERE may reference both sides); moving it before
	// the join for single-side predicates is a future optimizer step,
	// recorded in TRACKB_NOTES.md, not a silent reordering.
	plan.Operators = []string{"SCAN"}
	if plan.Join != nil {
		plan.Operators = append(plan.Operators, "JOIN")
	}
	if plan.Filter != nil {
		plan.Operators = append(plan.Operators, "FILTER")
	}
	if grouped {
		plan.Operators = append(plan.Operators, "AGGREGATE")
	}
	plan.Operators = append(plan.Operators, "PROJECT")
	if len(plan.OrderBy) > 0 {
		plan.Operators = append(plan.Operators, "SORT")
	}
	if plan.HasLimit {
		plan.Operators = append(plan.Operators, "LIMIT")
	}
	return plan, nil
}
