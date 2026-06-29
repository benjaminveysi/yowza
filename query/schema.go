package query

import (
	"fmt"
	"strings"
)

// ColumnSource describes where a column's value comes from in a KV row.
//   - "key": the full stored key, as a string.
//   - "suffix": the key with the table prefix removed, as a string.
//   - "value": the whole stored value, as a string (KindString) or,
//     when the column type is bytes, as raw bytes.
//   - "json:<field>": the named field of the stored value parsed as a
//     JSON object. A missing field, or a value that is not a JSON
//     object, yields NULL. A present field of the wrong type is a
//     materialization error (the query fails), not a silent NULL:
//     typed IR must not hide a type violation as missing data.
type Column struct {
	Name   string `json:"name"`
	Type   Kind   `json:"type"`
	Source string `json:"source"`
}

// Table maps a logical table to a KV key prefix. A row is one KV entry
// under the prefix; columns are projections of that entry. This is the
// schema-binding unit: a query may only name tables and columns that
// exist here, and the verifier rejects everything else before execution.
type Table struct {
	Name    string   `json:"name"`
	Prefix  string   `json:"prefix"`
	Columns []Column `json:"columns"`
}

// ColumnByName returns the column definition, or false.
func (t Table) ColumnByName(name string) (Column, bool) {
	for _, c := range t.Columns {
		if strings.EqualFold(c.Name, name) {
			return c, true
		}
	}
	return Column{}, false
}

// Schema is the registry of tables a query may touch. Table and column
// names are matched case-insensitively, but the canonical (registered)
// spelling is what appears in plans and traces.
type Schema struct {
	Tables []Table `json:"tables"`
}

// TableByName returns the table definition, or false.
func (s *Schema) TableByName(name string) (Table, bool) {
	if s == nil {
		return Table{}, false
	}
	for _, t := range s.Tables {
		if strings.EqualFold(t.Name, name) {
			return t, true
		}
	}
	return Table{}, false
}

// Validate checks the schema itself: non-empty unique table names,
// non-empty unique column names per table, and known column sources.
func (s *Schema) Validate() error {
	if s == nil {
		return fmt.Errorf("schema is nil")
	}
	seenTables := map[string]bool{}
	for _, t := range s.Tables {
		if t.Name == "" {
			return fmt.Errorf("table with empty name")
		}
		key := strings.ToLower(t.Name)
		if seenTables[key] {
			return fmt.Errorf("duplicate table %q", t.Name)
		}
		seenTables[key] = true
		seenCols := map[string]bool{}
		for _, c := range t.Columns {
			if c.Name == "" {
				return fmt.Errorf("table %q: column with empty name", t.Name)
			}
			ck := strings.ToLower(c.Name)
			if seenCols[ck] {
				return fmt.Errorf("table %q: duplicate column %q", t.Name, c.Name)
			}
			seenCols[ck] = true
			switch {
			case c.Source == "key" || c.Source == "suffix" || c.Source == "value":
			case strings.HasPrefix(c.Source, "json:") && len(c.Source) > 5:
			default:
				return fmt.Errorf("table %q column %q: unknown source %q", t.Name, c.Name, c.Source)
			}
			if (c.Source == "key" || c.Source == "suffix") && c.Type != KindString {
				return fmt.Errorf("table %q column %q: key/suffix columns must be string", t.Name, c.Name)
			}
			if c.Source == "value" && c.Type != KindString && c.Type != KindBytes {
				return fmt.Errorf("table %q column %q: value columns must be string or bytes", t.Name, c.Name)
			}
		}
	}
	return nil
}

// Row is one output row of a query or one pipeline batch element.
// Cols and Vals are parallel; Cols are output names (aliases, or the
// unqualified column name for plain projections).
type Row struct {
	Cols []string `json:"cols"`
	Vals []Value  `json:"vals"`
}

// Get returns the value of the named output column, or false. Matching
// is case-insensitive on the output name.
func (r Row) Get(name string) (Value, bool) {
	for i, c := range r.Cols {
		if strings.EqualFold(c, name) && i < len(r.Vals) {
			return r.Vals[i], true
		}
	}
	return Null(), false
}

func (r Row) String() string {
	parts := make([]string, len(r.Cols))
	for i, c := range r.Cols {
		v := Null()
		if i < len(r.Vals) {
			v = r.Vals[i]
		}
		parts[i] = c + "=" + v.String()
	}
	return "{" + strings.Join(parts, ", ") + "}"
}
