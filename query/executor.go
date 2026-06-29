package query

import (
	"bufio"
	"container/heap"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
)

// ---- real memory measurement and spill (deep-integration, part 3) ----
//
// Measurement method (the budget is enforced against this, not
// against the verifier's 256-bytes-per-row heuristic, which remains
// only as the static estimate in verifier.go):
//
//	rowSize(row) = 32 (per-row overhead: Row/slice headers and
//	  operator bookkeeping, documented here, not sampled from the
//	  runtime heap — Go exposes no per-object heap meter)
//	  + sum over columns of (len(column name) + valueSize(value))
//	valueSize: NULL 0, int/float 8, bool 1, string len(s),
//	  bytes len(b). It is computed from the real rows actually
//	  retained, never from a row count times a constant.
//
// Budget semantics (Contract.MaxMemoryBytes, 0 = unlimited):
//   - The budget applies PER retaining operator (hash-join build
//     side, sort, grouped aggregation), because operators run in
//     sequence and each owns its own spill store. The trace's
//     ActualMemoryBytes is therefore the largest per-operator peak.
//   - Spill granularity is one incoming batch: an operator must be
//     able to hold one batch in memory to process it, even if it
//     spills it immediately afterwards. A single batch whose
//     measured size exceeds the budget therefore fails cleanly with
//     BUDGET_EXCEEDED and no partial rows — it cannot be spilled
//     batch by batch, and splitting a batch mid-operator would
//     change batching semantics. This is the "budget cannot hold
//     one batch" rule.
//   - Otherwise, when retained data would exceed the budget, the
//     operator spills batches to temporary files (os.CreateTemp,
//     prefix "yowza-spill-") instead of failing, and reads them
//     back for the matching / sorting / merging phase. Spill files
//     are removed on Close, on success, and on error; every
//     spill-owning iterator has a Close, and the engine defers it.
//
// Spill file format: one JSON value per line (encoding/json
// Encoder/Decoder stream). Row spill files hold Rows directly;
// sort chunk files hold sortEntry values (original sequence number
// + Row, so a multi-chunk merge stays stable); aggregation spill
// files hold spillGroup values (group key, key values, and
// mergeable accumulator state). JSON round-trips Value exactly for
// the types in this slice (Kind as a number, bytes as base64).

// perRowOverheadBytes is the documented per-row overhead in rowSize.
const perRowOverheadBytes = 32

// valueSize returns the encoded size of one cell, from the real value.
func valueSize(v Value) int64 {
	switch v.Kind {
	case KindNull:
		return 0
	case KindInt, KindFloat:
		return 8
	case KindBool:
		return 1
	case KindString:
		return int64(len(v.S))
	case KindBytes:
		return int64(len(v.B))
	default:
		return 0
	}
}

// rowSize measures one retained row from its real contents.
func rowSize(row Row) int64 {
	sz := int64(perRowOverheadBytes)
	for i, c := range row.Cols {
		sz += int64(len(c))
		if i < len(row.Vals) {
			sz += valueSize(row.Vals[i])
		}
	}
	return sz
}

// batchBytes measures a whole batch from its real rows.
func batchBytes(batch []Row) int64 {
	var total int64
	for _, r := range batch {
		total += rowSize(r)
	}
	return total
}

func budgetExceededf(format string, args ...interface{}) error {
	return &VerificationError{Code: "BUDGET_EXCEEDED", Msg: fmt.Sprintf(format, args...)}
}

// memTracker records the largest per-operator retained-memory peak
// seen during one execution, and whether anything spilled.
type memTracker struct {
	peak    int64
	spilled bool
}

func (t *memTracker) note(n int64) {
	if t != nil && n > t.peak {
		t.peak = n
	}
}

func (t *memTracker) markSpilled() {
	if t != nil {
		t.spilled = true
	}
}

// writeSpillFile writes rows as one JSON value per line to a fresh
// temp file and returns its path. On any error the file is removed.
func writeSpillFile(rows []Row) (string, error) {
	f, err := os.CreateTemp("", "yowza-spill-*")
	if err != nil {
		return "", err
	}
	path := f.Name()
	enc := json.NewEncoder(f)
	for _, r := range rows {
		if err := enc.Encode(r); err != nil {
			f.Close()
			os.Remove(path)
			return "", err
		}
	}
	if err := f.Close(); err != nil {
		os.Remove(path)
		return "", err
	}
	return path, nil
}

// spillStore retains rows in memory up to a budget, then spills
// whole batches to temp files. It is the shared row store for the
// hash-join build side and for spilled aggregation output.
type spillStore struct {
	budget   int64 // 0 = unlimited
	tracker  *memTracker
	mem      []Row
	memBytes int64
	peak     int64
	paths    []string
	closed   bool
}

func newSpillStore(budget int64, tracker *memTracker) *spillStore {
	return &spillStore{budget: budget, tracker: tracker}
}

func (s *spillStore) spilled() bool { return len(s.paths) > 0 }

// AppendBatch retains one batch, spilling previously buffered rows
// first if the batch would push retention over the budget. A batch
// larger than the budget itself cannot be held at all and fails.
func (s *spillStore) AppendBatch(batch []Row) error {
	if len(batch) == 0 || s.closed {
		return nil
	}
	sz := batchBytes(batch)
	if s.budget > 0 && sz > s.budget {
		return budgetExceededf("single batch of %d rows (%d bytes) exceeds memory budget %d bytes", len(batch), sz, s.budget)
	}
	if s.budget > 0 && s.memBytes+sz > s.budget {
		if err := s.flush(); err != nil {
			return err
		}
	}
	s.mem = append(s.mem, batch...)
	s.memBytes += sz
	if s.memBytes > s.peak {
		s.peak = s.memBytes
	}
	s.tracker.note(s.memBytes)
	return nil
}

func (s *spillStore) flush() error {
	if len(s.mem) == 0 {
		return nil
	}
	path, err := writeSpillFile(s.mem)
	if err != nil {
		return err
	}
	s.paths = append(s.paths, path)
	s.tracker.markSpilled()
	s.mem = nil
	s.memBytes = 0
	return nil
}

// Close removes every spill file. It is idempotent and safe to call
// after an error, after success, or twice.
func (s *spillStore) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	for _, p := range s.paths {
		os.Remove(p)
	}
	s.paths = nil
	s.mem = nil
	s.memBytes = 0
	return nil
}

// iterator returns a fresh streaming reader over spilled files (in
// spill order) followed by the in-memory buffer, in insertion order.
// The caller must Close it; the store's Close removes the files.
func (s *spillStore) iterator() *spillIterator {
	return &spillIterator{store: s}
}

// spillIterator streams a spillStore one row at a time.
type spillIterator struct {
	store   *spillStore
	fileIdx int
	dec     *json.Decoder
	curFile *os.File
	memIdx  int
	closed  bool
}

func (it *spillIterator) Close() error {
	if it.closed {
		return nil
	}
	it.closed = true
	if it.curFile != nil {
		it.curFile.Close()
		it.curFile = nil
	}
	return nil
}

// Next returns the next row, or (Row{}, false, nil) at end of input.
func (it *spillIterator) Next() (Row, bool, error) {
	for it.fileIdx < len(it.store.paths) {
		if it.dec == nil {
			f, err := os.Open(it.store.paths[it.fileIdx])
			if err != nil {
				return Row{}, false, err
			}
			it.curFile = f
			it.dec = json.NewDecoder(f)
		}
		var r Row
		if err := it.dec.Decode(&r); err != nil {
			if err == io.EOF {
				it.curFile.Close()
				it.curFile = nil
				it.dec = nil
				it.fileIdx++
				continue
			}
			return Row{}, false, err
		}
		return r, true, nil
	}
	if it.curFile != nil {
		it.curFile.Close()
		it.curFile = nil
	}
	if it.memIdx < len(it.store.mem) {
		r := it.store.mem[it.memIdx]
		it.memIdx++
		return r, true, nil
	}
	return Row{}, false, nil
}

// Executor: batch pipelines.
//
// Every operator consumes and produces batches (default 64 rows),
// never one row per engine call, and the pipeline never makes one
// Go<->C round-trip per record: the source delivers batches (the
// YowzaDB adapter delivers one ScanEntries result, chunked into
// batches here), and all filtering, projection, joining, and
// aggregation happen inside the pipeline. The final stage's batches
// are collected by the engine; intermediate stages never materialize
// the whole result except where an operator's semantics require it
// (hash-join build side, aggregation, sort) — and those cases are
// bounded by the contract's scan/result budgets, checked by the
// engine, not silently unbounded.
//
// Operator order (matches Plan.Operators): SCAN -> JOIN -> FILTER ->
// AGGREGATE -> PROJECT -> SORT -> LIMIT. FILTER runs after JOIN in
// this slice because WHERE may reference both sides; pushing a
// single-side predicate before the join is a future optimizer step.

type batchIter interface {
	NextBatch() ([]Row, error)
}

// opStats is one operator's counters for the trace.
type opStats struct {
	Operator string
	RowsIn   int64
	RowsOut  int64
	Batches  int64
}

func (s *opStats) note(in, out int) {
	s.RowsIn += int64(in)
	s.RowsOut += int64(out)
	if out > 0 || in > 0 {
		s.Batches++
	}
}

type scanIter struct {
	it    RowIterator
	batch int
	stats *opStats
}

func (s *scanIter) NextBatch() ([]Row, error) {
	rows, err := s.it.NextBatch(s.batch)
	if err != nil {
		return nil, err
	}
	s.stats.note(len(rows), len(rows))
	return rows, nil
}

// ---- expression evaluation (SQL three-valued logic) ----
// evalTri returns triFalse, triTrue, or triUnknown. Comparisons
// involving NULL are unknown, AND/OR/NOT follow Kleene logic, and
// IS NULL is the only test that is never unknown. The filter keeps
// only rows whose WHERE expression is triTrue. This matters: with a
// naive two-valued "NULL comparison = false", NOT(city='Tehran')
// would wrongly pass a row whose city is NULL. Three-valued logic
// keeps Dan (NULL city) out of NOT(...) results, as SQL does.

func evalOperand(e *Expr, row Row) (Value, error) {
	switch e.Kind {
	case "col":
		if e.Col == nil {
			return Null(), fmt.Errorf("column expression without a column")
		}
		for i, c := range row.Cols {
			if c == e.Col.Qualified && i < len(row.Vals) {
				return row.Vals[i], nil
			}
		}
		return Null(), fmt.Errorf("column %q not present in row", e.Col.Qualified)
	case "lit":
		if e.Lit == nil {
			return Null(), fmt.Errorf("literal expression without a value")
		}
		return *e.Lit, nil
	default:
		return Null(), fmt.Errorf("operand must be a column or literal, got %q", e.Kind)
	}
}

type triState int

const (
	triFalse triState = iota
	triTrue
	triUnknown
)

func evalTri(e *Expr, row Row) (triState, error) {
	switch e.Kind {
	case "and":
		unknown := false
		for _, a := range e.Args {
			t, err := evalTri(a, row)
			if err != nil {
				return triFalse, err
			}
			if t == triFalse {
				return triFalse, nil
			}
			if t == triUnknown {
				unknown = true
			}
		}
		if unknown {
			return triUnknown, nil
		}
		return triTrue, nil
	case "or":
		unknown := false
		for _, a := range e.Args {
			t, err := evalTri(a, row)
			if err != nil {
				return triFalse, err
			}
			if t == triTrue {
				return triTrue, nil
			}
			if t == triUnknown {
				unknown = true
			}
		}
		if unknown {
			return triUnknown, nil
		}
		return triFalse, nil
	case "not":
		if len(e.Args) != 1 {
			return triFalse, fmt.Errorf("NOT requires exactly one operand")
		}
		t, err := evalTri(e.Args[0], row)
		if err != nil {
			return triFalse, err
		}
		switch t {
		case triTrue:
			return triFalse, nil
		case triFalse:
			return triTrue, nil
		default:
			return triUnknown, nil
		}
	case "cmp":
		l, err := evalOperand(e.Left, row)
		if err != nil {
			return triFalse, err
		}
		r, err := evalOperand(e.Right, row)
		if err != nil {
			return triFalse, err
		}
		if l.Kind == KindNull || r.Kind == KindNull {
			return triUnknown, nil
		}
		c, err := Compare(l, r)
		if err != nil {
			return triFalse, err
		}
		var b bool
		switch e.Op {
		case "=":
			b = c == 0
		case "!=", "<>":
			b = c != 0
		case "<":
			b = c < 0
		case "<=":
			b = c <= 0
		case ">":
			b = c > 0
		case ">=":
			b = c >= 0
		default:
			return triFalse, fmt.Errorf("unknown comparison operator %q", e.Op)
		}
		if b {
			return triTrue, nil
		}
		return triFalse, nil
	case "in":
		l, err := evalOperand(e.Left, row)
		if err != nil {
			return triFalse, err
		}
		if l.Kind == KindNull {
			return triUnknown, nil
		}
		hasNull := false
		for _, v := range e.InList {
			if v.Kind == KindNull {
				hasNull = true
				continue
			}
			eq, err := Equals(l, v)
			if err != nil {
				return triFalse, err
			}
			if eq {
				if e.Negated {
					return triFalse, nil
				}
				return triTrue, nil
			}
		}
		if hasNull {
			return triUnknown, nil
		}
		if e.Negated {
			return triTrue, nil
		}
		return triFalse, nil
	case "like":
		l, err := evalOperand(e.Left, row)
		if err != nil {
			return triFalse, err
		}
		if l.Kind == KindNull {
			return triUnknown, nil
		}
		if l.Kind != KindString {
			return triFalse, fmt.Errorf("LIKE on non-string value")
		}
		m := matchLike(l.S, e.Pattern)
		if e.Negated {
			m = !m
		}
		if m {
			return triTrue, nil
		}
		return triFalse, nil
	case "isnull":
		l, err := evalOperand(e.Left, row)
		if err != nil {
			return triFalse, err
		}
		isNull := l.Kind == KindNull
		if e.Negated {
			isNull = !isNull
		}
		if isNull {
			return triTrue, nil
		}
		return triFalse, nil
	default:
		return triFalse, fmt.Errorf("unsupported expression kind %q", e.Kind)
	}
}

// matchLike implements SQL LIKE with % (any sequence) and _ (one
// character), case-sensitive, anchored at both ends. It is an
// iterative wildcard matcher, not a regex translation, so a pattern
// cannot smuggle regex syntax into matching.
func matchLike(s, pattern string) bool {
	sr, pr := []rune(s), []rune(pattern)
	si, pi := 0, 0
	starP, starS := -1, -1
	for si < len(sr) {
		if pi < len(pr) && (pr[pi] == '_' || pr[pi] == sr[si]) {
			si++
			pi++
		} else if pi < len(pr) && pr[pi] == '%' {
			starP, starS = pi, si
			pi++
		} else if starP >= 0 {
			pi = starP + 1
			starS++
			si = starS
		} else {
			return false
		}
	}
	for pi < len(pr) && pr[pi] == '%' {
		pi++
	}
	return pi == len(pr)
}

type filterIter struct {
	in    batchIter
	expr  *Expr
	stats *opStats
}

func (f *filterIter) NextBatch() ([]Row, error) {
	for {
		batch, err := f.in.NextBatch()
		if err != nil {
			return nil, err
		}
		if len(batch) == 0 {
			return nil, nil
		}
		var out []Row
		for _, row := range batch {
			t, err := evalTri(f.expr, row)
			if err != nil {
				return nil, err
			}
			if t == triTrue {
				out = append(out, row)
			}
		}
		f.stats.note(len(batch), len(out))
		if len(out) > 0 {
			return out, nil
		}
		// Empty after filtering: keep pulling rather than reporting
		// EOF early. (A batch that filters to empty is not EOF.)
	}
}

type projectIter struct {
	in    batchIter
	plan  *Plan
	stats *opStats
}

func (p *projectIter) NextBatch() ([]Row, error) {
	batch, err := p.in.NextBatch()
	if err != nil {
		return nil, err
	}
	if len(batch) == 0 {
		return nil, nil
	}
	out := make([]Row, 0, len(batch))
	for _, row := range batch {
		nr, err := projectRow(p.plan, row)
		if err != nil {
			return nil, err
		}
		out = append(out, nr)
	}
	p.stats.note(len(batch), len(out))
	return out, nil
}

// projectRow maps one pipeline row to an output row. In a grouped
// plan, aggregate items read their value from the aggregate output
// column (keyed by OutputName) and plain items read their group key
// (keyed by Qualified). In a plain plan, items read by Qualified.
func projectRow(plan *Plan, row Row) (Row, error) {
	lookup := func(name string) (Value, bool) {
		for i, c := range row.Cols {
			if c == name && i < len(row.Vals) {
				return row.Vals[i], true
			}
		}
		return Null(), false
	}
	var cols []string
	var vals []Value
	for _, it := range plan.Projection {
		var v Value
		var ok bool
		if it.Agg != "" {
			v, ok = lookup(it.OutputName)
		} else if it.Col != nil {
			v, ok = lookup(it.Col.Qualified)
		}
		if !ok {
			return Row{}, fmt.Errorf("projection column %q not present in row", it.OutputName)
		}
		cols = append(cols, it.OutputName)
		vals = append(vals, v)
	}
	return Row{Cols: cols, Vals: vals}, nil
}

type joinIter struct {
	left  batchIter
	store *spillStore // build side: in-memory buffer + spill files
	rkey  string
	lkey  string
	buf   []Row // pending combined rows
	stats *opStats
}

// Close releases the build side's spill files. It is idempotent.
func (j *joinIter) Close() error {
	if j.store != nil {
		return j.store.Close()
	}
	return nil
}

func (j *joinIter) NextBatch() ([]Row, error) {
	for len(j.buf) == 0 {
		batch, err := j.left.NextBatch()
		if err != nil {
			return nil, err
		}
		if len(batch) == 0 {
			return nil, nil
		}
		j.stats.RowsIn += int64(len(batch))
		// Probe the build side. NULL keys never match, on either
		// side. In-memory, the loop below is left-major (for each
		// left row, all its build matches) over the buffer. When
		// spilled, re-reading the files once per left row would be
		// O(left x build) file decodes, so the spilled path reads
		// the build side once per left batch (build-major), tags
		// each match with its left index, and stable-sorts by that
		// index — the same left-major output order, with one build
		// scan per batch instead of one per row.
		if j.store.spilled() {
			type tagged struct {
				li  int
				row Row
			}
			var taggedOut []tagged
			bit := j.store.iterator()
			for {
				rrow, ok, err := bit.Next()
				if err != nil {
					bit.Close()
					return nil, err
				}
				if !ok {
					break
				}
				rval, found := lookupQualified(rrow, j.rkey)
				if !found || rval.Kind == KindNull {
					continue
				}
				for li, lrow := range batch {
					lval, found := lookupQualified(lrow, j.lkey)
					if !found || lval.Kind == KindNull {
						continue
					}
					eq, err := Equals(lval, rval)
					if err != nil {
						bit.Close()
						return nil, err
					}
					if eq {
						combined := Row{
							Cols: append(append([]string(nil), lrow.Cols...), rrow.Cols...),
							Vals: append(append([]Value(nil), lrow.Vals...), rrow.Vals...),
						}
						taggedOut = append(taggedOut, tagged{li: li, row: combined})
					}
				}
			}
			bit.Close()
			sort.SliceStable(taggedOut, func(a, b int) bool {
				return taggedOut[a].li < taggedOut[b].li
			})
			for _, tg := range taggedOut {
				j.buf = append(j.buf, tg.row)
			}
			continue
		}
		for _, lrow := range batch {
			lval, found := lookupQualified(lrow, j.lkey)
			if !found || lval.Kind == KindNull {
				continue
			}
			// Restart the build scan for every left row: the
			// iterator is cheap to recreate in memory and is the
			// only correct order for a streaming spill reader.
			// (Re-created per left row, not per batch, so a
			// partially consumed iterator can never skip a match.)
			bit := j.store.iterator()
			for {
				rrow, ok, err := bit.Next()
				if err != nil {
					bit.Close()
					return nil, err
				}
				if !ok {
					break
				}
				rval, found := lookupQualified(rrow, j.rkey)
				if !found || rval.Kind == KindNull {
					continue
				}
				eq, err := Equals(lval, rval)
				if err != nil {
					bit.Close()
					return nil, err
				}
				if eq {
					combined := Row{
						Cols: append(append([]string(nil), lrow.Cols...), rrow.Cols...),
						Vals: append(append([]Value(nil), lrow.Vals...), rrow.Vals...),
					}
					j.buf = append(j.buf, combined)
				}
			}
			bit.Close()
		}
		// No matches in this batch: keep pulling left input.
	}
	out := j.buf
	j.buf = nil
	j.stats.RowsOut += int64(len(out))
	j.stats.Batches++
	return out, nil
}

func lookupQualified(row Row, qualified string) (Value, bool) {
	for i, c := range row.Cols {
		if c == qualified && i < len(row.Vals) {
			return row.Vals[i], true
		}
	}
	return Null(), false
}

// ---- aggregation ----

type accumulator struct {
	agg     string
	count   int64
	sumF    float64
	sumI    int64
	isFloat bool
	has     bool
	min     Value
	max     Value
}

func (a *accumulator) add(v Value) error {
	switch a.agg {
	case "COUNT":
		// COUNT(*) is handled by the caller passing a non-null marker;
		// COUNT(col) counts non-null values only.
		if v.Kind != KindNull {
			a.count++
		}
	case "SUM", "AVG":
		if v.Kind == KindNull {
			return nil
		}
		f, ok := v.AsFloat()
		if !ok {
			return fmt.Errorf("%s requires numeric values", a.agg)
		}
		a.sumF += f
		if v.Kind == KindInt {
			a.sumI += v.I
		} else {
			a.isFloat = true
		}
		a.count++
		a.has = true
	case "MIN", "MAX":
		if v.Kind == KindNull {
			return nil
		}
		if !a.has {
			a.min, a.max, a.has = v, v, true
			return nil
		}
		c, err := Compare(v, a.min)
		if err != nil {
			return err
		}
		if c < 0 {
			a.min = v
		}
		c, err = Compare(v, a.max)
		if err != nil {
			return err
		}
		if c > 0 {
			a.max = v
		}
	}
	return nil
}

func (a *accumulator) result() Value {
	switch a.agg {
	case "COUNT":
		return Int(a.count)
	case "SUM":
		if !a.has {
			return Null()
		}
		if a.isFloat {
			return Float(a.sumF)
		}
		return Int(a.sumI)
	case "AVG":
		if !a.has || a.count == 0 {
			return Null()
		}
		return Float(a.sumF / float64(a.count))
	case "MIN":
		if !a.has {
			return Null()
		}
		return a.min
	case "MAX":
		if !a.has {
			return Null()
		}
		return a.max
	}
	return Null()
}

type groupState struct {
	keyVals []Value
	accs    []*accumulator
}

// spillAcc is the JSON-serializable, mergeable form of one
// accumulator, for aggregation spill files. COUNT/SUM/AVG/MIN/MAX
// are all mergeable from these fields: COUNT adds counts, SUM/AVG
// add sums and counts (sumF is always the full float sum, so a
// merged float result stays exact), MIN/MAX take extremes.
type spillAcc struct {
	Agg     string  `json:"agg"`
	Count   int64   `json:"count"`
	SumF    float64 `json:"sum_f"`
	SumI    int64   `json:"sum_i"`
	IsFloat bool    `json:"is_float"`
	Has     bool    `json:"has"`
	Min     Value   `json:"min"`
	Max     Value   `json:"max"`
}

// spillGroup is one group in an aggregation spill file.
type spillGroup struct {
	Key     string     `json:"key"`
	KeyVals []Value    `json:"key_vals"`
	Accs    []spillAcc `json:"accs"`
}

func accToSpill(a *accumulator) spillAcc {
	if a == nil {
		return spillAcc{}
	}
	return spillAcc{Agg: a.agg, Count: a.count, SumF: a.sumF, SumI: a.sumI, IsFloat: a.isFloat, Has: a.has, Min: a.min, Max: a.max}
}

// mergeFrom folds a spilled partial accumulator into this one.
func (a *accumulator) mergeFrom(s spillAcc) error {
	if s.Agg == "" {
		return nil
	}
	switch s.Agg {
	case "COUNT":
		a.count += s.Count
	case "SUM", "AVG":
		a.sumF += s.SumF
		a.sumI += s.SumI
		if s.IsFloat {
			a.isFloat = true
		}
		a.count += s.Count
		if s.Has {
			a.has = true
		}
	case "MIN", "MAX":
		if !s.Has {
			return nil
		}
		if !a.has {
			a.min, a.max, a.has = s.Min, s.Max, true
			return nil
		}
		c, err := Compare(s.Min, a.min)
		if err != nil {
			return err
		}
		if c < 0 {
			a.min = s.Min
		}
		c, err = Compare(s.Max, a.max)
		if err != nil {
			return err
		}
		if c > 0 {
			a.max = s.Max
		}
	}
	return nil
}

// measuredGroupSize measures one retained group from its real key
// values and accumulator state (48 bytes of group bookkeeping plus
// 24 per accumulator plus min/max cell sizes when present).
func measuredGroupSize(keyVals []Value, accs []*accumulator) int64 {
	sz := int64(48)
	for _, v := range keyVals {
		sz += valueSize(v)
	}
	for _, a := range accs {
		if a == nil {
			continue
		}
		sz += 24
		if a.has {
			sz += valueSize(a.min) + valueSize(a.max)
		}
	}
	return sz
}

func groupsBytes(groups map[string]*groupState) int64 {
	var total int64
	for _, st := range groups {
		total += measuredGroupSize(st.keyVals, st.accs)
	}
	return total
}

// writeGroupSpillFile writes the current groups (in first-appearance
// order) as one JSON value per line to a fresh temp file.
func writeGroupSpillFile(groups map[string]*groupState, order []string) (string, error) {
	f, err := os.CreateTemp("", "yowza-spill-*")
	if err != nil {
		return "", err
	}
	path := f.Name()
	enc := json.NewEncoder(f)
	for _, key := range order {
		st, ok := groups[key]
		if !ok {
			continue
		}
		rec := spillGroup{Key: key, KeyVals: st.keyVals}
		for _, a := range st.accs {
			rec.Accs = append(rec.Accs, accToSpill(a))
		}
		if err := enc.Encode(rec); err != nil {
			f.Close()
			os.Remove(path)
			return "", err
		}
	}
	if err := f.Close(); err != nil {
		os.Remove(path)
		return "", err
	}
	return path, nil
}

func removeSpillPaths(paths []string) {
	for _, p := range paths {
		os.Remove(p)
	}
}

type aggregateIter struct {
	plan       *Plan
	rows       []Row       // emitted groups, in-memory mode, first-appearance order
	outStore   *spillStore // spill mode: merged output rows
	outIter    *spillIterator
	spillPaths []string // group spill files, removed on Close
	idx        int
	done       bool
	stats      *opStats
	bs         int
	closed     bool
}

// Close removes aggregation spill files and the merged-output spill
// store's files. Idempotent; safe after error or success.
func (a *aggregateIter) Close() error {
	if a.closed {
		return nil
	}
	a.closed = true
	if a.outIter != nil {
		a.outIter.Close()
		a.outIter = nil
	}
	if a.outStore != nil {
		a.outStore.Close()
		a.outStore = nil
	}
	removeSpillPaths(a.spillPaths)
	a.spillPaths = nil
	return nil
}

func newAggregateIter(plan *Plan, in batchIter, stats *opStats, budget int64, tracker *memTracker) (*aggregateIter, error) {
	groups := map[string]*groupState{}
	var order []string
	// seen tracks first appearance GLOBALLY, across spill resets:
	// after a spill, groups is replaced with a fresh map, so a group
	// that reappears must NOT be appended to order a second time —
	// otherwise the spilled merge emits it twice (once per
	// appearance), folding the same spilled partials repeatedly and
	// over-counting. Regression: 200 groups + repeats after the
	// spill boundary previously produced 210 rows.
	seen := map[string]bool{}
	var scanned int64
	var spillPaths []string
	spilled := false
	for {
		batch, err := in.NextBatch()
		if err != nil {
			removeSpillPaths(spillPaths)
			return nil, err
		}
		if len(batch) == 0 {
			break
		}
		// Batch-granularity budget rule (see the spill header):
		// the operator must be able to hold one incoming batch to
		// process it, even though it folds rows into groups and
		// spills groups immediately afterwards.
		if budget > 0 && batchBytes(batch) > budget {
			removeSpillPaths(spillPaths)
			return nil, budgetExceededf("single batch of %d rows (%d bytes) exceeds memory budget %d bytes", len(batch), batchBytes(batch), budget)
		}
		scanned += int64(len(batch))
		for _, row := range batch {
			var keyParts []string
			var keyVals []Value
			for _, g := range plan.GroupBy {
				v, ok := lookupQualified(row, g.Qualified)
				if !ok {
					removeSpillPaths(spillPaths)
					return nil, fmt.Errorf("group key %q not present in row", g.Qualified)
				}
				keyVals = append(keyVals, v)
				keyParts = append(keyParts, v.groupKey())
			}
			key := strings.Join(keyParts, "\x00")
			st, ok := groups[key]
			if !ok {
				st = &groupState{keyVals: keyVals}
				for _, it := range plan.Projection {
					if it.Agg != "" {
						st.accs = append(st.accs, &accumulator{agg: it.Agg})
					} else {
						st.accs = append(st.accs, nil)
					}
				}
				groups[key] = st
				if !seen[key] {
					seen[key] = true
					order = append(order, key)
				}
			}
			for i, it := range plan.Projection {
				if it.Agg == "" {
					continue
				}
				acc := st.accs[i]
				if it.Star {
					// COUNT(*): count the row itself.
					acc.count++
					continue
				}
				v, ok := lookupQualified(row, it.Col.Qualified)
				if !ok {
					removeSpillPaths(spillPaths)
					return nil, fmt.Errorf("aggregate column %q not present in row", it.Col.Qualified)
				}
				if err := acc.add(v); err != nil {
					removeSpillPaths(spillPaths)
					return nil, err
				}
			}
		}
		// Spill when retained groups exceed the budget: flush all
		// current groups to a spill file and keep accumulating.
		// First-appearance order is kept globally in `order`, so
		// merging later re-emits groups in the original order.
		if budget > 0 {
			gb := groupsBytes(groups)
			tracker.note(gb)
			if gb > budget {
				path, err := writeGroupSpillFile(groups, order)
				if err != nil {
					removeSpillPaths(spillPaths)
					return nil, err
				}
				spillPaths = append(spillPaths, path)
				spilled = true
				tracker.markSpilled()
				groups = map[string]*groupState{}
			}
		} else {
			tracker.note(groupsBytes(groups))
		}
	}
	// Ungrouped aggregation over zero rows still yields one row
	// (COUNT = 0, other aggregates NULL), per SQL semantics.
	if len(plan.GroupBy) == 0 && len(order) == 0 {
		st := &groupState{}
		for _, it := range plan.Projection {
			if it.Agg != "" {
				st.accs = append(st.accs, &accumulator{agg: it.Agg})
			} else {
				st.accs = append(st.accs, nil)
			}
		}
		groups[""] = st
		order = append(order, "")
	}
	// Group keys first, under their qualified names, then aggregate
	// results under their output names; projectRow resolves plain
	// items by Qualified and aggregates by OutputName from this row.
	groupRow := func(keyVals []Value, accs []*accumulator) Row {
		var cols []string
		var vals []Value
		for i, g := range plan.GroupBy {
			cols = append(cols, g.Qualified)
			vals = append(vals, keyVals[i])
		}
		for i, it := range plan.Projection {
			if it.Agg == "" {
				continue
			}
			cols = append(cols, it.OutputName)
			vals = append(vals, accs[i].result())
		}
		return Row{Cols: cols, Vals: vals}
	}
	if stats != nil {
		stats.RowsIn = scanned
	}
	if !spilled {
		var out []Row
		for _, key := range order {
			out = append(out, groupRow(groups[key].keyVals, groups[key].accs))
		}
		return &aggregateIter{plan: plan, rows: out, stats: stats}, nil
	}
	// Spilled path: merge, in global first-appearance order, one
	// group at a time. For each group, start from the in-memory
	// remainder (if any) and fold in every spilled partial with the
	// same key — never loading all groups back into memory. Merged
	// rows go through a spillStore too, so a huge group count does
	// not re-materialize either. Locating partials by scanning
	// every spill file for every group would be O(groups x
	// records); instead one pass over the spill files builds a
	// byte-offset index (spill files are one JSON value per line,
	// so a line's start offset is exact), and each group then seeks
	// straight to its own partials. The index holds only keys and
	// offsets, not group data.
	type spillLoc struct {
		fileIdx int
		offset  int64
	}
	index := map[string][]spillLoc{}
	for fi, path := range spillPaths {
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		var off int64
		reader := bufio.NewReader(f)
		for {
			line, rerr := reader.ReadBytes('\n')
			if len(line) > 0 {
				var rec spillGroup
				if uerr := json.Unmarshal(line, &rec); uerr != nil {
					f.Close()
					return nil, uerr
				}
				index[rec.Key] = append(index[rec.Key], spillLoc{fileIdx: fi, offset: off})
				off += int64(len(line))
			}
			if rerr != nil {
				break
			}
		}
		f.Close()
	}
	outStore := newSpillStore(budget, tracker)
	mergeOne := func(key string) (Row, error) {
		var keyVals []Value
		var accs []*accumulator
		if st, ok := groups[key]; ok {
			keyVals = st.keyVals
			accs = st.accs
		} else {
			for _, it := range plan.Projection {
				if it.Agg != "" {
					accs = append(accs, &accumulator{agg: it.Agg})
				} else {
					accs = append(accs, nil)
				}
			}
		}
		for _, loc := range index[key] {
			f, err := os.Open(spillPaths[loc.fileIdx])
			if err != nil {
				return Row{}, err
			}
			if _, err := f.Seek(loc.offset, io.SeekStart); err != nil {
				f.Close()
				return Row{}, err
			}
			line, err := bufio.NewReader(f).ReadBytes('\n')
			f.Close()
			if err != nil && len(line) == 0 {
				return Row{}, err
			}
			var rec spillGroup
			if err := json.Unmarshal(line, &rec); err != nil {
				return Row{}, err
			}
			if keyVals == nil {
				keyVals = rec.KeyVals
			}
			for i, sacc := range rec.Accs {
				if i < len(accs) && accs[i] != nil {
					if err := accs[i].mergeFrom(sacc); err != nil {
						return Row{}, err
					}
				}
			}
		}
		return groupRow(keyVals, accs), nil
	}
	for _, key := range order {
		row, err := mergeOne(key)
		if err != nil {
			outStore.Close()
			removeSpillPaths(spillPaths)
			return nil, err
		}
		// One row at a time: the output store spills whole rows
		// whenever its buffer would exceed the budget.
		if err := outStore.AppendBatch([]Row{row}); err != nil {
			outStore.Close()
			removeSpillPaths(spillPaths)
			return nil, err
		}
	}
	return &aggregateIter{plan: plan, outStore: outStore, spillPaths: spillPaths, stats: stats}, nil
}

func (a *aggregateIter) NextBatch() ([]Row, error) {
	if a.outStore != nil {
		// Spill mode: stream merged output rows from the store.
		if a.outIter == nil {
			a.outIter = a.outStore.iterator()
		}
		var batch []Row
		for len(batch) < a.batchSize() {
			r, ok, err := a.outIter.Next()
			if err != nil {
				return nil, err
			}
			if !ok {
				break
			}
			batch = append(batch, r)
		}
		if len(batch) == 0 {
			return nil, nil
		}
		if a.stats != nil {
			a.stats.RowsOut += int64(len(batch))
			a.stats.Batches++
		}
		return batch, nil
	}
	if a.idx >= len(a.rows) {
		return nil, nil
	}
	// Aggregation emits in one batch per call is wasteful for large
	// group counts, but groups are already materialized by semantics;
	// chunk them with the same batch size the engine uses elsewhere.
	// The engine sets batchSize via aggregateBatchSize field below.
	n := a.batchSize()
	end := a.idx + n
	if end > len(a.rows) {
		end = len(a.rows)
	}
	batch := a.rows[a.idx:end]
	a.idx = end
	if a.stats != nil {
		a.stats.RowsOut += int64(len(batch))
		a.stats.Batches++
	}
	return batch, nil
}

// batchSize is set by the engine after construction.
func (a *aggregateIter) batchSize() int {
	if a.bs > 0 {
		return a.bs
	}
	return 64
}

// sortEntry is one row in a sort spill chunk, with its original
// input sequence number so a multi-chunk merge stays stable: equal
// keys merge in original input order, exactly as SliceStable does
// within one chunk.
type sortEntry struct {
	Seq int64 `json:"seq"`
	Row Row   `json:"row"`
}

// compareSortRows orders two output rows by the plan's ORDER BY:
// NULLs last in either direction; a Compare error counts as equal
// (the same rule the in-memory sort has always used).
func compareSortRows(plan *Plan, a, b Row) int {
	for _, o := range plan.OrderBy {
		vi, _ := a.Get(o.Name)
		vj, _ := b.Get(o.Name)
		if vi.Kind == KindNull && vj.Kind == KindNull {
			continue
		}
		if vi.Kind == KindNull {
			return 1
		}
		if vj.Kind == KindNull {
			return -1
		}
		c, err := Compare(vi, vj)
		if err != nil || c == 0 {
			continue
		}
		if o.Desc {
			return -c
		}
		return c
	}
	return 0
}

func sortEntriesLess(plan *Plan, a, b sortEntry) bool {
	if c := compareSortRows(plan, a.Row, b.Row); c != 0 {
		return c < 0
	}
	return a.Seq < b.Seq
}

// sortChunkReader streams one sorted spill chunk.
type sortChunkReader struct {
	file *os.File
	dec  *json.Decoder
	cur  sortEntry
	has  bool
	done bool
}

func (r *sortChunkReader) advance() error {
	if r.done {
		return nil
	}
	var e sortEntry
	if err := r.dec.Decode(&e); err != nil {
		if err == io.EOF {
			r.done = true
			r.has = false
			return nil
		}
		return err
	}
	r.cur, r.has = e, true
	return nil
}

func (r *sortChunkReader) close() {
	if r.file != nil {
		r.file.Close()
		r.file = nil
	}
}

// sortMergeHeap is a k-way merge heap over chunk readers plus the
// final in-memory chunk. Ordering is sortEntriesLess: sort keys,
// then original sequence for stability.
type sortMergeHeap struct {
	plan  *Plan
	items []sortMergeItem
}

type sortMergeItem struct {
	entry sortEntry
	src   int // index into sortIter.sources; -1 = in-memory chunk
}

func (h *sortMergeHeap) Len() int { return len(h.items) }
func (h *sortMergeHeap) Less(i, j int) bool {
	return sortEntriesLess(h.plan, h.items[i].entry, h.items[j].entry)
}
func (h *sortMergeHeap) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h *sortMergeHeap) Push(x interface{}) {
	h.items = append(h.items, x.(sortMergeItem))
}
func (h *sortMergeHeap) Pop() interface{} {
	old := h.items
	n := len(old)
	it := old[n-1]
	h.items = old[:n-1]
	return it
}

type sortIter struct {
	plan  *Plan
	rows  []Row // in-memory mode only (never spilled)
	idx   int
	stats *opStats
	bs    int
	// Spill mode: sorted chunk files + one final in-memory chunk.
	chunkPaths []string
	sources    []*sortChunkReader
	memChunk   []sortEntry
	memIdx     int
	mergeHeap  *sortMergeHeap
	closed     bool
}

// Close removes sort spill files and closes chunk readers.
// Idempotent; safe after error or success.
func (s *sortIter) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	for _, r := range s.sources {
		if r != nil {
			r.close()
		}
	}
	s.sources = nil
	removeSpillPaths(s.chunkPaths)
	s.chunkPaths = nil
	return nil
}

func writeSortChunk(entries []sortEntry) (string, error) {
	f, err := os.CreateTemp("", "yowza-spill-*")
	if err != nil {
		return "", err
	}
	path := f.Name()
	enc := json.NewEncoder(f)
	for _, e := range entries {
		if err := enc.Encode(e); err != nil {
			f.Close()
			os.Remove(path)
			return "", err
		}
	}
	if err := f.Close(); err != nil {
		os.Remove(path)
		return "", err
	}
	return path, nil
}

func newSortIter(plan *Plan, in batchIter, budget int64, tracker *memTracker) (*sortIter, error) {
	var buf []sortEntry
	var bufBytes int64
	var chunkPaths []string
	var seq int64
	spilled := false
	flushBuf := func() error {
		if len(buf) == 0 {
			return nil
		}
		sort.SliceStable(buf, func(i, j int) bool {
			return sortEntriesLess(plan, buf[i], buf[j])
		})
		path, err := writeSortChunk(buf)
		if err != nil {
			return err
		}
		chunkPaths = append(chunkPaths, path)
		spilled = true
		tracker.markSpilled()
		buf = nil
		bufBytes = 0
		return nil
	}
	for {
		batch, err := in.NextBatch()
		if err != nil {
			removeSpillPaths(chunkPaths)
			return nil, err
		}
		if len(batch) == 0 {
			break
		}
		// Batch-granularity budget rule (see the spill header).
		if budget > 0 && batchBytes(batch) > budget {
			removeSpillPaths(chunkPaths)
			return nil, budgetExceededf("single batch of %d rows (%d bytes) exceeds memory budget %d bytes", len(batch), batchBytes(batch), budget)
		}
		if budget > 0 && bufBytes+batchBytes(batch) > budget {
			if err := flushBuf(); err != nil {
				removeSpillPaths(chunkPaths)
				return nil, err
			}
		}
		for _, r := range batch {
			buf = append(buf, sortEntry{Seq: seq, Row: r})
			seq++
		}
		bufBytes += batchBytes(batch)
		tracker.note(bufBytes)
	}
	if !spilled {
		// Never exceeded the budget: the original in-memory path,
		// unchanged semantics (stable sort, NULLs last).
		rows := make([]Row, 0, len(buf))
		sort.SliceStable(buf, func(i, j int) bool {
			return sortEntriesLess(plan, buf[i], buf[j])
		})
		for _, e := range buf {
			rows = append(rows, e.Row)
		}
		tracker.note(bufBytes)
		return &sortIter{plan: plan, rows: rows}, nil
	}
	// Spilled path: sort the final buffer as the last chunk and
	// merge all chunks with a heap. Each chunk is already sorted;
	// the heap tie-breaks equal keys by original sequence, so the
	// merge is stable across chunks exactly as a single sort is.
	sort.SliceStable(buf, func(i, j int) bool {
		return sortEntriesLess(plan, buf[i], buf[j])
	})
	srt := &sortIter{plan: plan, chunkPaths: chunkPaths, memChunk: buf}
	h := &sortMergeHeap{plan: plan}
	for _, path := range chunkPaths {
		f, err := os.Open(path)
		if err != nil {
			srt.Close()
			return nil, err
		}
		r := &sortChunkReader{file: f, dec: json.NewDecoder(f)}
		if err := r.advance(); err != nil {
			r.close()
			srt.Close()
			return nil, err
		}
		srt.sources = append(srt.sources, r)
		if r.has {
			heap.Push(h, sortMergeItem{entry: r.cur, src: len(srt.sources) - 1})
		}
	}
	if len(buf) > 0 {
		heap.Push(h, sortMergeItem{entry: buf[0], src: -1})
		srt.memIdx = 1
	}
	heap.Init(h)
	srt.mergeHeap = h
	return srt, nil
}

func (s *sortIter) NextBatch() ([]Row, error) {
	if s.mergeHeap != nil {
		// Spill mode: k-way merge, one batch at a time.
		var batch []Row
		n := s.bs
		if n <= 0 {
			n = 64
		}
		for len(batch) < n && s.mergeHeap.Len() > 0 {
			it := heap.Pop(s.mergeHeap).(sortMergeItem)
			batch = append(batch, it.entry.Row)
			if it.src == -1 {
				if s.memIdx < len(s.memChunk) {
					heap.Push(s.mergeHeap, sortMergeItem{entry: s.memChunk[s.memIdx], src: -1})
					s.memIdx++
				}
			} else {
				r := s.sources[it.src]
				if err := r.advance(); err != nil {
					return nil, err
				}
				if r.has {
					heap.Push(s.mergeHeap, sortMergeItem{entry: r.cur, src: it.src})
				}
			}
		}
		if len(batch) == 0 {
			return nil, nil
		}
		s.stats.note(len(batch), len(batch))
		return batch, nil
	}
	if s.idx >= len(s.rows) {
		return nil, nil
	}
	n := s.bs
	if n <= 0 {
		n = 64
	}
	end := s.idx + n
	if end > len(s.rows) {
		end = len(s.rows)
	}
	batch := s.rows[s.idx:end]
	s.idx = end
	s.stats.note(len(batch), len(batch))
	return batch, nil
}

type limitIter struct {
	in        batchIter
	remaining int64
	stats     *opStats
}

func (l *limitIter) NextBatch() ([]Row, error) {
	if l.remaining <= 0 {
		return nil, nil
	}
	batch, err := l.in.NextBatch()
	if err != nil {
		return nil, err
	}
	if len(batch) == 0 {
		return nil, nil
	}
	if int64(len(batch)) > l.remaining {
		batch = batch[:l.remaining]
	}
	l.remaining -= int64(len(batch))
	l.stats.note(len(batch), len(batch))
	return batch, nil
}
