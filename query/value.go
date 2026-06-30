// Package query implements the YowzaQL query layer (Track B, Phase 3,
// first slice). See TRACKB_NOTES.md for the full design, the grammar
// subset actually implemented, and the adapter boundary to storage.
//
// Design rule enforced by this package: no query text is ever executed
// directly. Text is parsed to an AST, bound against a Schema to a typed
// Plan (the IR), verified against a Contract, and only then executed as
// a batch pipeline. A plan that fails verification is never opened
// against a data source.
package query

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// Kind is the type of a Value. Only these types exist in the first
// slice; there is deliberately no generic "any" that hides a type error.
type Kind int

const (
	KindNull Kind = iota
	KindInt
	KindFloat
	KindString
	KindBool
	KindBytes
)

func (k Kind) String() string {
	switch k {
	case KindNull:
		return "null"
	case KindInt:
		return "int"
	case KindFloat:
		return "float"
	case KindString:
		return "string"
	case KindBool:
		return "bool"
	case KindBytes:
		return "bytes"
	default:
		return "unknown"
	}
}

// Value is a single typed cell. Exactly one payload field is meaningful,
// selected by Kind. Float values are never silently truncated to Int:
// int/float comparison is allowed, but a float cell stays a float.
type Value struct {
	Kind Kind
	I    int64
	F    float64
	S    string
	B    []byte
}

func Null() Value           { return Value{Kind: KindNull} }
func Int(v int64) Value     { return Value{Kind: KindInt, I: v} }
func Float(v float64) Value { return Value{Kind: KindFloat, F: v} }
func String(v string) Value { return Value{Kind: KindString, S: v} }
func Bool(v bool) Value {
	if v {
		return Value{Kind: KindBool, B: []byte{1}}
	}
	return Value{Kind: KindBool, B: []byte{0}}
}
func Bytes(v []byte) Value { return Value{Kind: KindBytes, B: append([]byte(nil), v...)} }

// BoolVal returns the boolean payload. Bool values are constructed
// via Bool(): the payload lives in B[0] (1 = true, 0 = false), kept
// behind this method so callers never inspect the encoding.
func (v Value) BoolVal() bool {
	if len(v.B) > 0 {
		return v.B[0] != 0
	}
	return false
}

func (v Value) String() string {
	switch v.Kind {
	case KindNull:
		return "NULL"
	case KindInt:
		return strconv.FormatInt(v.I, 10)
	case KindFloat:
		return strconv.FormatFloat(v.F, 'g', -1, 64)
	case KindString:
		return v.S
	case KindBool:
		if v.BoolVal() {
			return "TRUE"
		}
		return "FALSE"
	case KindBytes:
		return string(v.B)
	default:
		return "?"
	}
}

// IsNumeric reports whether the value participates in numeric
// comparison and aggregation (int or float, never a string).
func (v Value) IsNumeric() bool { return v.Kind == KindInt || v.Kind == KindFloat }

// AsFloat converts int/float to float64 for cross-type numeric work.
func (v Value) AsFloat() (float64, bool) {
	switch v.Kind {
	case KindInt:
		return float64(v.I), true
	case KindFloat:
		return v.F, true
	default:
		return 0, false
	}
}

// groupKey is the canonical encoding of a value used as a GROUP BY key.
// Distinct types never collide: int 1, float 1.0 and string "1" are
// different keys. NaN is normalized to a single key.
func (v Value) groupKey() string {
	switch v.Kind {
	case KindNull:
		return "null:"
	case KindInt:
		return "int:" + strconv.FormatInt(v.I, 10)
	case KindFloat:
		if math.IsNaN(v.F) {
			return "float:nan"
		}
		return "float:" + strconv.FormatUint(math.Float64bits(v.F), 10)
	case KindString:
		return "string:" + strconv.Quote(v.S)
	case KindBool:
		if v.BoolVal() {
			return "bool:true"
		}
		return "bool:false"
	case KindBytes:
		return "bytes:" + strconv.Quote(string(v.B))
	default:
		return "unknown:"
	}
}

// ErrNumericOverflow is returned when an exact integer operation
// (checked SUM accumulation, checked addition/multiplication) would
// overflow int64. It is a typed error, never a silent wrap: callers
// must surface it, not substitute a wrapped value. NOTE: the Go
// reference executor (query/executor.go, not owned by Gate 4)
// still wraps int64 SUM silently, and the C ISA engine now
// returns QISA_ERR_OVERFLOW for the same case — see
// GATE4_NOTES.md §5a for the exact divergence table.
var ErrNumericOverflow = errors.New("query: numeric overflow (int64)")

// MaxExactIntInFloat64 is the largest magnitude integer that
// float64 represents exactly: 2^53. Integers with a larger
// magnitude that pass through float64 (Go JSON int fields,
// which jsonFieldToValue converts via correctly-rounded
// float64) are rounded to a multiple of the float64 spacing
// at that magnitude. The C engine's JSON number parser is
// NOT correctly rounded: it accumulates 15 significant
// digits exactly and folds the rest into the exponent, so
// beyond 15 significant digits the two engines can differ
// (observed: 9007199254740993 -> Go 9007199254740992, C
// 9007199254740990; see TestGate4JSONIntBoundaryDivergence
// and GATE4_NOTES.md §5a). Integers that never pass
// through a JSON/float64 conversion (query literals, which
// the parser reads with ParseInt, and SUM results) are
// exact to the full int64 range, and Compare below treats
// them exactly.
const MaxExactIntInFloat64 = int64(1) << 53

// Int64ToFloatExact converts an int64 to float64, reporting
// whether the conversion is exact (true iff |v| <= 2^53).
func Int64ToFloatExact(v int64) (float64, bool) {
	f := float64(v)
	if v >= -(int64(1)<<53) && v <= (int64(1)<<53) {
		return f, true
	}
	return f, int64(f) == v
}

// Float64ToIntExact converts a float64 to int64, reporting
// whether it is integral and in int64 range. Non-integral
// values, NaN, and infinities report false.
func Float64ToIntExact(f float64) (int64, bool) {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return 0, false
	}
	if f >= 9223372036854775808.0 || f < -9223372036854775808.0 {
		return 0, false
	}
	if math.Trunc(f) != f {
		return 0, false
	}
	return int64(f), true
}

// compareIntFloat compares an int64 against a float64 exactly,
// without routing the int through float64. NaN compares equal
// to everything (preserving the pre-Gate-4 float behaviour, in
// which neither < nor > held for NaN); infinities compare
// beyond every int64.
func compareIntFloat(iv int64, f float64) int {
	if math.IsNaN(f) {
		return 0
	}
	if math.IsInf(f, 1) {
		return -1
	}
	if math.IsInf(f, -1) {
		return 1
	}
	if f >= 9223372036854775808.0 {
		return -1
	}
	if f < -9223372036854775808.0 {
		return 1
	}
	if math.Trunc(f) == f {
		fv := int64(f)
		switch {
		case iv < fv:
			return -1
		case iv > fv:
			return 1
		default:
			return 0
		}
	}
	// Fractional f: iv < f iff iv <= floor(f); iv > f otherwise.
	fl := int64(math.Floor(f))
	if iv <= fl {
		return -1
	}
	return 1
}

// AddIntChecked adds two int64s, returning ErrNumericOverflow
// instead of wrapping.
func AddIntChecked(a, b int64) (int64, error) {
	if (b > 0 && a > math.MaxInt64-b) || (b < 0 && a < math.MinInt64-b) {
		return 0, ErrNumericOverflow
	}
	return a + b, nil
}

// MulIntChecked multiplies two int64s, returning
// ErrNumericOverflow instead of wrapping.
func MulIntChecked(a, b int64) (int64, error) {
	if a == 0 || b == 0 {
		return 0, nil
	}
	if a == -1 && b == math.MinInt64 || b == -1 && a == math.MinInt64 {
		return 0, ErrNumericOverflow
	}
	r := a * b
	if r/b != a {
		return 0, ErrNumericOverflow
	}
	return r, nil
}

// ParseJSONIntExact parses a JSON number token as an exact int64.
// It returns ok=false (not an error) when the token is not an
// integral value in int64 range, including when it would need
// more than int64 (overflow) or has a fractional part. This is
// the exact parser the JSON ingestion paths SHOULD use; the
// current Go jsonFieldToValue (query/source.go) converts via
// correctly-rounded float64 (exact to 2^53) and the C
// engine's JSON number parser accumulates 15 digits exactly
// (see the boundary note above). Gate 4 does not change
// those paths (they belong to other tracks/files); it pins
// the boundary and provides the exact parser for adoption.
func ParseJSONIntExact(tok string) (int64, bool) {
	if tok == "" {
		return 0, false
	}
	i := 0
	neg := false
	if tok[0] == '-' {
		neg = true
		i++
	} else if tok[0] == '+' {
		i++
	}
	intStart := i
	for i < len(tok) && tok[i] >= '0' && tok[i] <= '9' {
		i++
	}
	intDigits := tok[intStart:i]
	fracDigits := ""
	if i < len(tok) && tok[i] == '.' {
		i++
		fracStart := i
		for i < len(tok) && tok[i] >= '0' && tok[i] <= '9' {
			i++
		}
		if i == fracStart {
			return 0, false
		}
		fracDigits = tok[fracStart:i]
	}
	exp := 0
	if i < len(tok) && (tok[i] == 'e' || tok[i] == 'E') {
		i++
		expNeg := false
		if i < len(tok) && (tok[i] == '+' || tok[i] == '-') {
			expNeg = tok[i] == '-'
			i++
		}
		expStart := i
		for i < len(tok) && tok[i] >= '0' && tok[i] <= '9' {
			i++
			if exp > 100000 {
				exp = 100000
			} else {
				exp = exp*10 + int(tok[i-1]-'0')
			}
		}
		if i == expStart {
			return 0, false
		}
		if expNeg {
			exp = -exp
		}
	}
	if i != len(tok) || len(intDigits) == 0 {
		return 0, false
	}
	// Digit string = intDigits + fracDigits; net exponent shifts it.
	digits := intDigits + fracDigits
	netExp := exp - len(fracDigits)
	// Strip leading zeros.
	for len(digits) > 0 && digits[0] == '0' {
		digits = digits[1:]
	}
	if digits == "" {
		return 0, true // zero, any exponent (integral)
	}
	if netExp < 0 {
		// Need the last -netExp digits to be zero.
		need := -netExp
		if need >= len(digits) {
			return 0, false
		}
		for _, c := range digits[len(digits)-need:] {
			if c != '0' {
				return 0, false
			}
		}
		digits = digits[:len(digits)-need]
	} else if netExp > 0 {
		if len(digits)+netExp > 19 {
			return 0, false
		}
		digits += strings.Repeat("0", netExp)
	}
	if len(digits) > 19 {
		return 0, false
	}
	var v uint64
	for _, c := range []byte(digits) {
		v = v*10 + uint64(c-'0')
	}
	if neg {
		if v > uint64(math.MaxInt64)+1 {
			return 0, false
		}
		if v == uint64(math.MaxInt64)+1 {
			return math.MinInt64, true
		}
		return -int64(v), true
	}
	if v > uint64(math.MaxInt64) {
		return 0, false
	}
	return int64(v), true
}

// Compare returns -1, 0, +1 for ordered values of compatible type.
// Rules (documented, not incidental):
//   - NULL is never ordered against a non-NULL value here; callers
//     handle NULL before calling Compare (WHERE comparisons against
//     NULL are not-true; ORDER BY puts NULL last).
//   - int and float compare numerically, EXACTLY (Gate 4 fix):
//     int/int compares as int64, and int/float compares without
//     routing the int through float64, so 2^53+1 and 2^53+2 are
//     distinct (the old float64 routing collapsed them). The C
//     engine's qi_cmp and join canonicalization implement the
//     same rule, so the two engines still agree.
//   - string compares bytewise; bool compares false < true; bytes
//     compare bytewise.
//   - any other pair is a type error.
func Compare(a, b Value) (int, error) {
	if a.Kind == KindNull || b.Kind == KindNull {
		return 0, fmt.Errorf("cannot order NULL values")
	}
	if a.IsNumeric() && b.IsNumeric() {
		if a.Kind == KindInt && b.Kind == KindInt {
			switch {
			case a.I < b.I:
				return -1, nil
			case a.I > b.I:
				return 1, nil
			default:
				return 0, nil
			}
		}
		if a.Kind == KindInt {
			return compareIntFloat(a.I, b.F), nil
		}
		if b.Kind == KindInt {
			return -compareIntFloat(b.I, a.F), nil
		}
		af, bf := a.F, b.F
		switch {
		case af < bf:
			return -1, nil
		case af > bf:
			return 1, nil
		default:
			return 0, nil
		}
	}
	if a.Kind != b.Kind {
		return 0, fmt.Errorf("cannot compare %s with %s", a.Kind, b.Kind)
	}
	switch a.Kind {
	case KindString:
		return strings.Compare(a.S, b.S), nil
	case KindBool:
		ab, bb := a.BoolVal(), b.BoolVal()
		switch {
		case ab == bb:
			return 0, nil
		case !ab && bb:
			return -1, nil
		default:
			return 1, nil
		}
	case KindBytes:
		return strings.Compare(string(a.B), string(b.B)), nil
	}
	return 0, fmt.Errorf("cannot compare values of kind %s", a.Kind)
}

// Equals reports equality under the same rules as Compare, except that
// NULL = NULL is false here (SQL three-valued logic: a comparison
// involving NULL is not-true, and the executor treats not-true as
// "row does not pass the filter"; IS NULL is the only NULL test).
func Equals(a, b Value) (bool, error) {
	if a.Kind == KindNull || b.Kind == KindNull {
		return false, nil
	}
	c, err := Compare(a, b)
	if err != nil {
		return false, err
	}
	return c == 0, nil
}
