package query

import (
	"encoding/json"
	"fmt"
)

// JSON encoding for Kind and Value, used by Plan serialization.
// Kind encodes as a string ("int", "float", ...); decoding also
// accepts the historic integer codes so older plans stay readable.

func (k Kind) MarshalJSON() ([]byte, error) {
	return json.Marshal(k.String())
}

func (k *Kind) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		switch s {
		case "null":
			*k = KindNull
		case "int":
			*k = KindInt
		case "float":
			*k = KindFloat
		case "string":
			*k = KindString
		case "bool":
			*k = KindBool
		case "bytes":
			*k = KindBytes
		default:
			return fmt.Errorf("unknown value kind %q", s)
		}
		return nil
	}
	var n int
	if err := json.Unmarshal(data, &n); err != nil {
		return err
	}
	if n < 0 || n > int(KindBytes) {
		return fmt.Errorf("unknown value kind code %d", n)
	}
	*k = Kind(n)
	return nil
}

type valueJSON struct {
	Kind string   `json:"kind"`
	I    *int64   `json:"i,omitempty"`
	F    *float64 `json:"f,omitempty"`
	S    *string  `json:"s,omitempty"`
	B    *bool    `json:"b,omitempty"`
	Bin  []byte   `json:"bin,omitempty"`
}

func (v Value) MarshalJSON() ([]byte, error) {
	j := valueJSON{Kind: v.Kind.String()}
	switch v.Kind {
	case KindInt:
		i := v.I
		j.I = &i
	case KindFloat:
		f := v.F
		j.F = &f
	case KindString:
		s := v.S
		j.S = &s
	case KindBool:
		b := v.BoolVal()
		j.B = &b
	case KindBytes:
		j.Bin = v.B
	}
	return json.Marshal(j)
}

func (v *Value) UnmarshalJSON(data []byte) error {
	var j valueJSON
	if err := json.Unmarshal(data, &j); err != nil {
		return err
	}
	switch j.Kind {
	case "null":
		*v = Null()
	case "int":
		if j.I == nil {
			return fmt.Errorf("int value without payload")
		}
		*v = Int(*j.I)
	case "float":
		if j.F == nil {
			return fmt.Errorf("float value without payload")
		}
		*v = Float(*j.F)
	case "string":
		if j.S == nil {
			return fmt.Errorf("string value without payload")
		}
		*v = String(*j.S)
	case "bool":
		if j.B == nil {
			return fmt.Errorf("bool value without payload")
		}
		*v = Bool(*j.B)
	case "bytes":
		*v = Bytes(j.Bin)
	default:
		return fmt.Errorf("unknown value kind %q", j.Kind)
	}
	return nil
}
