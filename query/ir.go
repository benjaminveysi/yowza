package query

// IR is the typed, language-independent execution plan. It is the only
// representation the executor ever sees: the parser produces an AST,
// the binder resolves it to a Plan, the verifier checks the Plan, and
// the executor runs the Plan. Plans are JSON-serializable so they can
// be stored, hashed, signed, and explained without the original text.

// ColumnRef is a bound column: Table is the canonical table name,
// Alias is the query-local alias (defaults to the table name), Name is
// the canonical column name, and Type is the schema type. Qualified is
// "alias.name" and is the internal row key used by the pipeline.
type ColumnRef struct {
	Table     string `json:"table"`
	Alias     string `json:"alias"`
	Name      string `json:"name"`
	Type      Kind   `json:"type"`
	Qualified string `json:"qualified"`
}

// Expr is one node of a bound filter/join expression. Kind is one of
// "and", "or", "not", "cmp", "in", "like", "isnull", "col", "lit".
// Column references are fully bound; literals are typed Values.
// There is no raw-text node: an Expr cannot be built from a string
// without going through the parser and binder.
type Expr struct {
	Kind    string     `json:"kind"`
	Op      string     `json:"op,omitempty"`
	Left    *Expr      `json:"left,omitempty"`
	Right   *Expr      `json:"right,omitempty"`
	Args    []*Expr    `json:"args,omitempty"`
	Col     *ColumnRef `json:"col,omitempty"`
	Lit     *Value     `json:"lit,omitempty"`
	InList  []Value    `json:"in_list,omitempty"`
	Pattern string     `json:"pattern,omitempty"`
	Negated bool       `json:"negated,omitempty"`
}

// ProjItem is one output column of the plan.
type ProjItem struct {
	Agg        string     `json:"agg,omitempty"`  // "", COUNT, SUM, AVG, MIN, MAX
	Star       bool       `json:"star,omitempty"` // COUNT(*) only
	Col        *ColumnRef `json:"col,omitempty"`
	Alias      string     `json:"alias,omitempty"`
	OutputName string     `json:"output_name"`
	OutputType Kind       `json:"output_type"`
}

// JoinSpec is a bound inner equijoin.
type JoinSpec struct {
	RightTable string     `json:"right_table"`
	RightAlias string     `json:"right_alias"`
	LeftCol    *ColumnRef `json:"left_col"`
	RightCol   *ColumnRef `json:"right_col"`
}

// OrderItem is a bound ORDER BY element, resolved against output
// columns (aliases or names), not against source columns.
type OrderItem struct {
	Name string `json:"name"`
	Desc bool   `json:"desc"`
}

// Binding records one table's schema as bound into the plan, so a
// serialized plan is self-describing: a verifier can check bindings
// without re-reading the registry that produced them.
type Binding struct {
	Table   string   `json:"table"`
	Alias   string   `json:"alias"`
	Prefix  string   `json:"prefix"`
	Columns []Column `json:"columns"`
}

// PlanVersion is the current IR version. The verifier rejects any
// other version; bumping it is a deliberate, documented change.
const PlanVersion = 1

// Plan is the typed IR for one SELECT query.
type Plan struct {
	Version       int         `json:"version"`
	QueryText     string      `json:"query_text"`
	FromTable     string      `json:"from_table"`
	FromAlias     string      `json:"from_alias"`
	Join          *JoinSpec   `json:"join,omitempty"`
	Filter        *Expr       `json:"filter,omitempty"`
	Projection    []ProjItem  `json:"projection"`
	GroupBy       []ColumnRef `json:"group_by,omitempty"`
	OrderBy       []OrderItem `json:"order_by,omitempty"`
	Limit         *int64      `json:"limit,omitempty"`
	HasLimit      bool        `json:"has_limit"`
	OutputColumns []Column    `json:"output_columns"`
	Bindings      []Binding   `json:"bindings"`
	Operators     []string    `json:"operators"`
	// EstimatedRows is the planner's row estimate, or -1 when unknown.
	// The first slice has no table statistics, so this is -1 unless a
	// LIMIT bounds the result (then it is the limit). The trace always
	// records estimated vs actual, so a future learned estimator can
	// be judged against these fields.
	EstimatedRows int64 `json:"estimated_rows"`
}

// IsAggregate reports whether the plan computes aggregates.
func (p *Plan) IsAggregate() bool {
	for _, it := range p.Projection {
		if it.Agg != "" {
			return true
		}
	}
	return len(p.GroupBy) > 0
}
