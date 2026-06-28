package query

// AST is the unresolved parse tree of a YowzaQL SELECT statement.
// Nothing here is typed and nothing here is executed: the binder
// (binder.go) resolves every name against a Schema and produces the
// typed Plan. Keeping AST and IR separate is what makes "parse" and
// "bind" independently testable and keeps raw text out of execution.

// UnresolvedColumn is a column reference before binding: Table may be
// empty (unqualified) and is resolved by the binder, which errors on
// unknown or ambiguous references.
type UnresolvedColumn struct {
	Table string
	Name  string
}

// ASTExpr is one node of a WHERE / ON expression, before binding.
type ASTExpr struct {
	Kind string // "and", "or", "not", "cmp", "in", "like", "isnull", "col", "lit"
	// cmp
	Op    string // "=", "!=", "<>", "<", "<=", ">", ">="
	Left  *ASTExpr
	Right *ASTExpr
	// and/or/not children
	Args []*ASTExpr
	// col
	Col *UnresolvedColumn
	// lit
	Lit Value
	// in
	InList []Value
	// like / isnull
	Negated bool
	Pattern string
}

// ASTSelectItem is one element of the SELECT list, before binding.
type ASTSelectItem struct {
	Agg   string // "", or "COUNT", "SUM", "AVG", "MIN", "MAX"
	Star  bool   // COUNT(*) only; SELECT * is handled separately
	Col   *UnresolvedColumn
	Alias string
}

// ASTOrderItem is one ORDER BY element, referring to an output column
// by name (alias or unqualified column name) — resolved after
// projection, not against the source schema.
type ASTOrderItem struct {
	Name string
	Desc bool
}

// ASTJoin is an inner equijoin: the ON expression must be a single
// equality between one column of each side (checked by the binder).
// Other join predicates are rejected, not silently approximated.
type ASTJoin struct {
	Table string
	Alias string
	On    *ASTExpr
}

// ASTQuery is a parsed SELECT statement.
type ASTQuery struct {
	Star      bool // SELECT * (expands to the source table's columns; with a JOIN it expands to both tables' columns)
	Select    []ASTSelectItem
	FromTable string
	FromAlias string
	Join      *ASTJoin
	Where     *ASTExpr
	GroupBy   []UnresolvedColumn
	OrderBy   []ASTOrderItem
	Limit     *int64
	HasLimit  bool
}
