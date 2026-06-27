package query

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

// YowzaQL grammar, first slice (see TRACKB_NOTES.md for the full
// statement of what is in scope and what is explicitly not):
//
//	Query      := SELECT SelectList FROM TableRef [JOIN TableRef ON Expr]
//	              [WHERE Expr] [GROUP BY ColList] [ORDER BY OrderList]
//	              [LIMIT n]
//	SelectList := "*" | Item ("," Item)*
//	Item       := Agg "(" ("*" | Col) ")" [AS Alias] | Col [AS Alias]
//	Agg        := COUNT | SUM | AVG | MIN | MAX
//	TableRef   := Name [AS Alias]
//	Expr       := OrExpr
//	OrExpr     := AndExpr (OR AndExpr)*
//	AndExpr    := NotExpr (AND NotExpr)*
//	NotExpr    := [NOT] Primary
//	Primary    := "(" Expr ")" | Comparison
//	Comparison := Operand [ (Op Operand | [NOT] IN "(" LitList ")"
//	              | [NOT] LIKE StringLit | IS [NOT] NULL ) ]
//	Operand    := Col | Literal
//	Col        := [Table "."] Name
//	Literal    := Number | StringLit | TRUE | FALSE | NULL
//
// Keywords are case-insensitive. Identifiers are [A-Za-z_][A-Za-z0-9_]*.
// String literals are single-quoted; '' is an escaped quote.
// Only SELECT is accepted: any other leading keyword is a parse error,
// never a partially executed statement. Subqueries, CTEs, window
// functions, UNION, INSERT/UPDATE/DELETE and non-equijoin predicates
// are rejected by the parser or binder with a clear error, not
// approximated.

type tokenKind int

const (
	tEOF tokenKind = iota
	tIdent
	tNumber
	tString
	tSymbol // one of: , ( ) . * = ! < >
	tOp     // multi-char operators: != <> <= >=
)

type token struct {
	kind tokenKind
	text string
	pos  int
}

var keywords = map[string]bool{
	"SELECT": true, "FROM": true, "WHERE": true, "JOIN": true, "ON": true,
	"AS": true, "AND": true, "OR": true, "NOT": true, "IN": true,
	"LIKE": true, "IS": true, "NULL": true, "TRUE": true, "FALSE": true,
	"GROUP": true, "BY": true, "ORDER": true, "LIMIT": true, "ASC": true,
	"DESC": true, "COUNT": true, "SUM": true, "AVG": true, "MIN": true,
	"MAX": true,
}

// ParseError carries the byte offset of the offending token so callers
// can point at the exact place a query failed to parse.
type ParseError struct {
	Pos int
	Msg string
}

func (e *ParseError) Error() string {
	return fmt.Sprintf("parse error at offset %d: %s", e.Pos, e.Msg)
}

func tokenize(input string) ([]token, error) {
	var toks []token
	i := 0
	for i < len(input) {
		c := input[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == ';':
			// A trailing semicolon is tolerated (client convenience);
			// anything after it is not.
			if strings.TrimSpace(input[i+1:]) != "" {
				return nil, &ParseError{Pos: i, Msg: "unexpected content after ';'"}
			}
			i++
		case c == '\'':
			start := i
			i++
			var sb strings.Builder
			closed := false
			for i < len(input) {
				if input[i] == '\'' {
					if i+1 < len(input) && input[i+1] == '\'' {
						sb.WriteByte('\'')
						i += 2
						continue
					}
					closed = true
					i++
					break
				}
				sb.WriteByte(input[i])
				i++
			}
			if !closed {
				return nil, &ParseError{Pos: start, Msg: "unterminated string literal"}
			}
			toks = append(toks, token{kind: tString, text: sb.String(), pos: start})
		case unicode.IsLetter(rune(c)) || c == '_':
			start := i
			for i < len(input) && (unicode.IsLetter(rune(input[i])) || unicode.IsDigit(rune(input[i])) || input[i] == '_') {
				i++
			}
			toks = append(toks, token{kind: tIdent, text: input[start:i], pos: start})
		case unicode.IsDigit(rune(c)):
			start := i
			for i < len(input) && unicode.IsDigit(rune(input[i])) {
				i++
			}
			if i < len(input) && input[i] == '.' && i+1 < len(input) && unicode.IsDigit(rune(input[i+1])) {
				i++
				for i < len(input) && unicode.IsDigit(rune(input[i])) {
					i++
				}
			}
			if i < len(input) && (input[i] == 'e' || input[i] == 'E') {
				j := i + 1
				if j < len(input) && (input[j] == '+' || input[j] == '-') {
					j++
				}
				if j < len(input) && unicode.IsDigit(rune(input[j])) {
					i = j
					for i < len(input) && unicode.IsDigit(rune(input[i])) {
						i++
					}
				}
			}
			toks = append(toks, token{kind: tNumber, text: input[start:i], pos: start})
		case c == ',' || c == '(' || c == ')' || c == '.' || c == '*' || c == '=':
			toks = append(toks, token{kind: tSymbol, text: string(c), pos: i})
			i++
		case c == '!' || c == '<' || c == '>':
			if i+1 < len(input) && input[i+1] == '=' {
				toks = append(toks, token{kind: tOp, text: input[i : i+2], pos: i})
				i += 2
			} else if c == '!' {
				return nil, &ParseError{Pos: i, Msg: "'!' must be followed by '='"}
			} else {
				toks = append(toks, token{kind: tSymbol, text: string(c), pos: i})
				i++
			}
		default:
			return nil, &ParseError{Pos: i, Msg: fmt.Sprintf("unexpected character %q", c)}
		}
	}
	toks = append(toks, token{kind: tEOF, pos: len(input)})
	return toks, nil
}

type parser struct {
	toks []token
	pos  int
}

func (p *parser) peek() token { return p.toks[p.pos] }
func (p *parser) next() token { t := p.toks[p.pos]; p.pos++; return t }
func (p *parser) err(t token, msg string) error {
	return &ParseError{Pos: t.pos, Msg: msg}
}

// isKeyword reports whether the current token is the given keyword
// (identifiers that happen to spell a keyword are keywords, not names).
func (p *parser) isKeyword(kw string) bool {
	t := p.peek()
	return t.kind == tIdent && strings.EqualFold(t.text, kw)
}

func (p *parser) expectKeyword(kw string) error {
	if !p.isKeyword(kw) {
		return p.err(p.peek(), fmt.Sprintf("expected %s", kw))
	}
	p.next()
	return nil
}

// ident reads a non-keyword identifier. Reserved words cannot be used
// as table or column names in this slice; that is a deliberate
// restriction, not an oversight to work around silently.
func (p *parser) ident() (string, error) {
	t := p.peek()
	if t.kind != tIdent {
		return "", p.err(t, "expected identifier")
	}
	if keywords[strings.ToUpper(t.text)] {
		return "", p.err(t, fmt.Sprintf("reserved word %q cannot be used as a name", t.text))
	}
	p.next()
	return t.text, nil
}

func (p *parser) acceptSymbol(s string) bool {
	t := p.peek()
	if (t.kind == tSymbol || t.kind == tOp) && t.text == s {
		p.next()
		return true
	}
	return false
}

func (p *parser) expectSymbol(s string) error {
	if !p.acceptSymbol(s) {
		return p.err(p.peek(), fmt.Sprintf("expected %q", s))
	}
	return nil
}

// Parse parses one YowzaQL SELECT statement into an AST. It does not
// bind names or check types; see Compile.
func Parse(input string) (*ASTQuery, error) {
	toks, err := tokenize(input)
	if err != nil {
		return nil, err
	}
	p := &parser{toks: toks}
	q, err := p.parseQuery()
	if err != nil {
		return nil, err
	}
	if p.peek().kind != tEOF {
		return nil, p.err(p.peek(), "unexpected trailing input")
	}
	return q, nil
}

func (p *parser) parseQuery() (*ASTQuery, error) {
	if !p.isKeyword("SELECT") {
		return nil, p.err(p.peek(), "only SELECT statements are supported")
	}
	p.next()
	q := &ASTQuery{}
	if p.acceptSymbol("*") {
		q.Star = true
	} else {
		for {
			item, err := p.parseSelectItem()
			if err != nil {
				return nil, err
			}
			q.Select = append(q.Select, item)
			if !p.acceptSymbol(",") {
				break
			}
		}
	}
	if err := p.expectKeyword("FROM"); err != nil {
		return nil, err
	}
	name, err := p.ident()
	if err != nil {
		return nil, err
	}
	q.FromTable = name
	if p.isKeyword("AS") {
		p.next()
		alias, err := p.ident()
		if err != nil {
			return nil, err
		}
		q.FromAlias = alias
	} else if p.peek().kind == tIdent && !keywords[strings.ToUpper(p.peek().text)] {
		// Bare alias without AS (e.g. FROM users u).
		q.FromAlias = p.next().text
	}
	if p.isKeyword("JOIN") {
		p.next()
		j := &ASTJoin{}
		j.Table, err = p.ident()
		if err != nil {
			return nil, err
		}
		if p.isKeyword("AS") {
			p.next()
			j.Alias, err = p.ident()
			if err != nil {
				return nil, err
			}
		} else if p.peek().kind == tIdent && !keywords[strings.ToUpper(p.peek().text)] {
			j.Alias = p.next().text
		}
		if err := p.expectKeyword("ON"); err != nil {
			return nil, err
		}
		j.On, err = p.parseExpr()
		if err != nil {
			return nil, err
		}
		q.Join = j
	}
	if p.isKeyword("WHERE") {
		p.next()
		e, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		q.Where = e
	}
	if p.isKeyword("GROUP") {
		p.next()
		if err := p.expectKeyword("BY"); err != nil {
			return nil, err
		}
		for {
			c, err := p.parseColumnRef()
			if err != nil {
				return nil, err
			}
			q.GroupBy = append(q.GroupBy, *c)
			if !p.acceptSymbol(",") {
				break
			}
		}
	}
	if p.isKeyword("ORDER") {
		p.next()
		if err := p.expectKeyword("BY"); err != nil {
			return nil, err
		}
		for {
			name, err := p.ident()
			if err != nil {
				return nil, err
			}
			it := ASTOrderItem{Name: name}
			if p.isKeyword("DESC") {
				p.next()
				it.Desc = true
			} else if p.isKeyword("ASC") {
				p.next()
			}
			q.OrderBy = append(q.OrderBy, it)
			if !p.acceptSymbol(",") {
				break
			}
		}
	}
	if p.isKeyword("LIMIT") {
		p.next()
		t := p.peek()
		if t.kind != tNumber {
			return nil, p.err(t, "LIMIT requires a non-negative integer")
		}
		p.next()
		n, err := strconv.ParseInt(t.text, 10, 64)
		if err != nil || n < 0 || strings.Contains(t.text, ".") || strings.ContainsAny(t.text, "eE") {
			return nil, p.err(t, "LIMIT requires a non-negative integer")
		}
		q.Limit = &n
		q.HasLimit = true
	}
	return q, nil
}

func (p *parser) parseSelectItem() (ASTSelectItem, error) {
	var item ASTSelectItem
	t := p.peek()
	if t.kind == tIdent {
		upper := strings.ToUpper(t.text)
		if upper == "COUNT" || upper == "SUM" || upper == "AVG" || upper == "MIN" || upper == "MAX" {
			p.next()
			item.Agg = upper
			if err := p.expectSymbol("("); err != nil {
				return item, err
			}
			if p.acceptSymbol("*") {
				if upper != "COUNT" {
					return item, p.err(t, "only COUNT(*) is allowed; other aggregates need a column")
				}
				item.Star = true
			} else {
				c, err := p.parseColumnRef()
				if err != nil {
					return item, err
				}
				item.Col = c
			}
			if err := p.expectSymbol(")"); err != nil {
				return item, err
			}
		} else {
			c, err := p.parseColumnRef()
			if err != nil {
				return item, err
			}
			item.Col = c
		}
	} else {
		c, err := p.parseColumnRef()
		if err != nil {
			return item, err
		}
		item.Col = c
	}
	if p.isKeyword("AS") {
		p.next()
		alias, err := p.ident()
		if err != nil {
			return item, err
		}
		item.Alias = alias
	} else if p.peek().kind == tIdent && !keywords[strings.ToUpper(p.peek().text)] {
		// Bare alias (e.g. SELECT age a).
		item.Alias = p.next().text
	}
	return item, nil
}

func (p *parser) parseColumnRef() (*UnresolvedColumn, error) {
	first, err := p.ident()
	if err != nil {
		return nil, err
	}
	if p.acceptSymbol(".") {
		second, err := p.ident()
		if err != nil {
			return nil, err
		}
		return &UnresolvedColumn{Table: first, Name: second}, nil
	}
	return &UnresolvedColumn{Name: first}, nil
}

func (p *parser) parseExpr() (*ASTExpr, error) { return p.parseOr() }

func (p *parser) parseOr() (*ASTExpr, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for p.isKeyword("OR") {
		p.next()
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = &ASTExpr{Kind: "or", Args: []*ASTExpr{left, right}}
	}
	return left, nil
}

func (p *parser) parseAnd() (*ASTExpr, error) {
	left, err := p.parseNot()
	if err != nil {
		return nil, err
	}
	for p.isKeyword("AND") {
		p.next()
		right, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		left = &ASTExpr{Kind: "and", Args: []*ASTExpr{left, right}}
	}
	return left, nil
}

func (p *parser) parseNot() (*ASTExpr, error) {
	if p.isKeyword("NOT") {
		p.next()
		inner, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		return &ASTExpr{Kind: "not", Args: []*ASTExpr{inner}}, nil
	}
	return p.parsePrimary()
}

func (p *parser) parsePrimary() (*ASTExpr, error) {
	if p.acceptSymbol("(") {
		e, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if err := p.expectSymbol(")"); err != nil {
			return nil, err
		}
		return e, nil
	}
	left, err := p.parseOperand()
	if err != nil {
		return nil, err
	}
	// IS [NOT] NULL
	if p.isKeyword("IS") {
		p.next()
		neg := false
		if p.isKeyword("NOT") {
			p.next()
			neg = true
		}
		if err := p.expectKeyword("NULL"); err != nil {
			return nil, err
		}
		if left.Kind != "col" {
			return nil, p.err(p.peek(), "IS NULL applies to a column")
		}
		return &ASTExpr{Kind: "isnull", Left: left, Negated: neg}, nil
	}
	// [NOT] IN / [NOT] LIKE (NOT already consumed by parseNot for a
	// leading NOT; a NOT directly before IN/LIKE is handled here).
	neg := false
	if p.isKeyword("NOT") {
		// Only valid when followed by IN or LIKE; otherwise this NOT
		// belongs to nothing and is an error.
		save := p.pos
		p.next()
		if !p.isKeyword("IN") && !p.isKeyword("LIKE") {
			p.pos = save
			return nil, p.err(p.peek(), "expected comparison operator after operand")
		}
		neg = true
	}
	if p.isKeyword("IN") {
		p.next()
		if err := p.expectSymbol("("); err != nil {
			return nil, err
		}
		var list []Value
		for {
			v, err := p.parseLiteral()
			if err != nil {
				return nil, err
			}
			list = append(list, v)
			if !p.acceptSymbol(",") {
				break
			}
		}
		if err := p.expectSymbol(")"); err != nil {
			return nil, err
		}
		if left.Kind != "col" {
			return nil, p.err(p.peek(), "IN applies to a column")
		}
		return &ASTExpr{Kind: "in", Left: left, InList: list, Negated: neg}, nil
	}
	if p.isKeyword("LIKE") {
		p.next()
		t := p.peek()
		if t.kind != tString {
			return nil, p.err(t, "LIKE requires a string pattern")
		}
		p.next()
		if left.Kind != "col" {
			return nil, p.err(p.peek(), "LIKE applies to a column")
		}
		return &ASTExpr{Kind: "like", Left: left, Pattern: t.text, Negated: neg}, nil
	}
	// Comparison operator.
	t := p.peek()
	op := ""
	switch {
	case t.kind == tSymbol && (t.text == "=" || t.text == "<" || t.text == ">"):
		op = t.text
	case t.kind == tOp:
		op = t.text
	}
	if op == "" {
		// A bare column/boolean expression is not a filter in this
		// slice: filters must be explicit comparisons, so a stray
		// boolean column is not silently treated as "is true".
		return nil, p.err(t, "expected comparison operator")
	}
	p.next()
	right, err := p.parseOperand()
	if err != nil {
		return nil, err
	}
	return &ASTExpr{Kind: "cmp", Op: op, Left: left, Right: right}, nil
}

func (p *parser) parseOperand() (*ASTExpr, error) {
	t := p.peek()
	switch t.kind {
	case tIdent:
		if strings.EqualFold(t.text, "TRUE") {
			p.next()
			return &ASTExpr{Kind: "lit", Lit: Bool(true)}, nil
		}
		if strings.EqualFold(t.text, "FALSE") {
			p.next()
			return &ASTExpr{Kind: "lit", Lit: Bool(false)}, nil
		}
		if strings.EqualFold(t.text, "NULL") {
			p.next()
			return &ASTExpr{Kind: "lit", Lit: Null()}, nil
		}
		if keywords[strings.ToUpper(t.text)] {
			return nil, p.err(t, fmt.Sprintf("unexpected keyword %q in expression", t.text))
		}
		c, err := p.parseColumnRef()
		if err != nil {
			return nil, err
		}
		return &ASTExpr{Kind: "col", Col: c}, nil
	case tNumber, tString:
		v, err := p.parseLiteral()
		if err != nil {
			return nil, err
		}
		return &ASTExpr{Kind: "lit", Lit: v}, nil
	default:
		return nil, p.err(t, "expected column or literal")
	}
}

func (p *parser) parseLiteral() (Value, error) {
	t := p.peek()
	switch t.kind {
	case tString:
		p.next()
		return String(t.text), nil
	case tNumber:
		p.next()
		if strings.ContainsAny(t.text, ".eE") {
			f, err := strconv.ParseFloat(t.text, 64)
			if err != nil {
				return Null(), p.err(t, "invalid number")
			}
			return Float(f), nil
		}
		n, err := strconv.ParseInt(t.text, 10, 64)
		if err != nil {
			return Null(), p.err(t, "invalid integer")
		}
		return Int(n), nil
	case tIdent:
		if strings.EqualFold(t.text, "TRUE") {
			p.next()
			return Bool(true), nil
		}
		if strings.EqualFold(t.text, "FALSE") {
			p.next()
			return Bool(false), nil
		}
		if strings.EqualFold(t.text, "NULL") {
			p.next()
			return Null(), nil
		}
		return Null(), p.err(t, "expected literal")
	default:
		return Null(), p.err(t, "expected literal")
	}
}
