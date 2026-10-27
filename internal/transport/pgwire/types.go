// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package pgwire

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"strings"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
)

// column maps one Arrow field to an exact PostgreSQL type.
type column struct {
	name string
	oid  uint32
	size int16
	mod  int32
	get  func(arrow.Array, int) any
}

func unsupported(what string) *pgError {
	return &pgError{code: "0A000", msg: "unsupported " + what + " for the Kelvo PostgreSQL interface"}
}

func columns(schema *arrow.Schema) ([]column, *pgError) {
	out := make([]column, len(schema.Fields()))
	for i, f := range schema.Fields() {
		c := column{name: f.Name, mod: -1, size: -1}
		switch t := f.Type.(type) {
		case *arrow.BooleanType:
			c.oid, c.size, c.get = pgtype.BoolOID, 1, func(a arrow.Array, i int) any { return a.(*array.Boolean).Value(i) }
		case *arrow.Int8Type:
			c.oid, c.size, c.get = pgtype.Int2OID, 2, func(a arrow.Array, i int) any { return int16(a.(*array.Int8).Value(i)) }
		case *arrow.Int16Type:
			c.oid, c.size, c.get = pgtype.Int2OID, 2, func(a arrow.Array, i int) any { return a.(*array.Int16).Value(i) }
		case *arrow.Int32Type:
			c.oid, c.size, c.get = pgtype.Int4OID, 4, func(a arrow.Array, i int) any { return a.(*array.Int32).Value(i) }
		case *arrow.Int64Type:
			c.oid, c.size, c.get = pgtype.Int8OID, 8, func(a arrow.Array, i int) any { return a.(*array.Int64).Value(i) }
		case *arrow.Uint8Type:
			c.oid, c.size, c.get = pgtype.Int2OID, 2, func(a arrow.Array, i int) any { return int16(a.(*array.Uint8).Value(i)) }
		case *arrow.Uint16Type:
			c.oid, c.size, c.get = pgtype.Int4OID, 4, func(a arrow.Array, i int) any { return int32(a.(*array.Uint16).Value(i)) }
		case *arrow.Uint32Type:
			c.oid, c.size, c.get = pgtype.Int8OID, 8, func(a arrow.Array, i int) any { return int64(a.(*array.Uint32).Value(i)) }
		case *arrow.Uint64Type:
			c.oid, c.mod = pgtype.NumericOID, numericMod(20, 0)
			c.get = func(a arrow.Array, i int) any {
				return pgtype.Numeric{Int: new(big.Int).SetUint64(a.(*array.Uint64).Value(i)), Valid: true}
			}
		case *arrow.Float32Type:
			c.oid, c.size, c.get = pgtype.Float4OID, 4, func(a arrow.Array, i int) any { return a.(*array.Float32).Value(i) }
		case *arrow.Float64Type:
			c.oid, c.size, c.get = pgtype.Float8OID, 8, func(a arrow.Array, i int) any { return a.(*array.Float64).Value(i) }
		case *arrow.StringType, *arrow.LargeStringType, *arrow.StringViewType:
			c.oid, c.get = pgtype.TextOID, func(a arrow.Array, i int) any { return a.(interface{ Value(int) string }).Value(i) }
		case *arrow.BinaryType, *arrow.LargeBinaryType, *arrow.BinaryViewType:
			c.oid, c.get = pgtype.ByteaOID, func(a arrow.Array, i int) any { return a.(interface{ Value(int) []byte }).Value(i) }
		case *arrow.Date32Type:
			c.oid, c.size, c.get = pgtype.DateOID, 4, func(a arrow.Array, i int) any {
				return pgtype.Date{Time: a.(*array.Date32).Value(i).ToTime(), Valid: true}
			}
		case *arrow.Date64Type:
			c.oid, c.size, c.get = pgtype.DateOID, 4, func(a arrow.Array, i int) any {
				return pgtype.Date{Time: a.(*array.Date64).Value(i).ToTime(), Valid: true}
			}
		case *arrow.TimestampType:
			unit := t.Unit
			c.size = 8
			if t.TimeZone == "" {
				c.oid, c.get = pgtype.TimestampOID, func(a arrow.Array, i int) any {
					return pgtype.Timestamp{Time: a.(*array.Timestamp).Value(i).ToTime(unit), Valid: true}
				}
			} else {
				c.oid, c.get = pgtype.TimestamptzOID, func(a arrow.Array, i int) any {
					return pgtype.Timestamptz{Time: a.(*array.Timestamp).Value(i).ToTime(unit), Valid: true}
				}
			}
		case *arrow.Time64Type:
			div := int64(1)
			if t.Unit == arrow.Nanosecond {
				div = 1000
			}
			c.oid, c.size, c.get = pgtype.TimeOID, 8, func(a arrow.Array, i int) any {
				return pgtype.Time{Microseconds: int64(a.(*array.Time64).Value(i)) / div, Valid: true}
			}
		case *arrow.Decimal128Type:
			scale := t.Scale
			c.oid, c.mod = pgtype.NumericOID, numericMod(t.Precision, t.Scale)
			c.get = func(a arrow.Array, i int) any {
				return pgtype.Numeric{Int: a.(*array.Decimal128).Value(i).BigInt(), Exp: -scale, Valid: true}
			}
		case *arrow.Decimal256Type:
			scale := t.Scale
			c.oid, c.mod = pgtype.NumericOID, numericMod(t.Precision, t.Scale)
			c.get = func(a arrow.Array, i int) any {
				return pgtype.Numeric{Int: a.(*array.Decimal256).Value(i).BigInt(), Exp: -scale, Valid: true}
			}
		case *arrow.NullType:
			c.oid, c.get = pgtype.TextOID, func(arrow.Array, int) any { return nil }
		default:
			return nil, unsupported(fmt.Sprintf("result type %s in column %q", f.Type, f.Name))
		}
		out[i] = c
	}
	return out, nil
}

func numericMod(precision, scale int32) int32 { return (precision<<16 | scale) + 4 }

func rowDescription(cols []column, formats []int16) *pgproto3.RowDescription {
	d := &pgproto3.RowDescription{Fields: make([]pgproto3.FieldDescription, len(cols))}
	for i, c := range cols {
		d.Fields[i] = pgproto3.FieldDescription{Name: []byte(c.name), DataTypeOID: c.oid, DataTypeSize: c.size, TypeModifier: c.mod, Format: formatFor(formats, i)}
	}
	return d
}

func formatFor(formats []int16, i int) int16 {
	switch len(formats) {
	case 0:
		return pgtype.TextFormatCode
	case 1:
		return formats[0]
	}
	return formats[i]
}

// encodeRows appends one DataRow per Arrow row.
func encodeRows(m *pgtype.Map, cols []column, formats []int16, rec arrow.RecordBatch, emit func([][]byte) error) error {
	for r := 0; r < int(rec.NumRows()); r++ {
		values := make([][]byte, len(cols))
		for i, c := range cols {
			a := rec.Column(i)
			if a.IsNull(r) {
				continue
			}
			v := c.get(a, r)
			if v == nil {
				continue
			}
			b, err := m.Encode(c.oid, formatFor(formats, i), v, []byte{})
			if err != nil {
				return &pgError{code: "22000", msg: fmt.Sprintf("cannot encode column %q", c.name)}
			}
			values[i] = b
		}
		if err := emit(values); err != nil {
			return err
		}
	}
	return nil
}

// sameShape reports whether a described result still matches execution.
func sameShape(a, b []column) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].name != b[i].name || a[i].oid != b[i].oid || a[i].mod != b[i].mod {
			return false
		}
	}
	return true
}

// bindParameter converts one PostgreSQL parameter to a Kelvo typed parameter
// and, when the type has no Kelvo equivalent, the exact DuckDB cast to apply.
func bindParameter(m *pgtype.Map, oid uint32, format int16, raw []byte) (query.Parameter, string, *pgError) {
	if raw == nil {
		return query.Parameter{Type: "null", Value: json.RawMessage("null")}, castFor(oid), nil
	}
	text := string(raw)
	if format == pgtype.BinaryFormatCode {
		if oid == 0 {
			return query.Parameter{}, "", &pgError{code: "08P01", msg: "binary parameter requires a declared type"}
		}
		var v any
		if err := m.Scan(oid, format, raw, &v); err != nil {
			return query.Parameter{}, "", &pgError{code: "22P03", msg: "invalid binary parameter"}
		}
		if b, ok := v.([]byte); ok && oid == pgtype.ByteaOID {
			text = `\x` + hex.EncodeToString(b)
		} else if f, ok := v.(float32); ok {
			text = strconv.FormatFloat(float64(f), 'g', -1, 32)
		} else {
			enc, err := m.Encode(oid, pgtype.TextFormatCode, v, nil)
			if err != nil {
				return query.Parameter{}, "", &pgError{code: "22P03", msg: "invalid binary parameter"}
			}
			text = string(enc)
		}
	}
	str := func(s string) json.RawMessage { b, _ := json.Marshal(s); return b }
	bad := &pgError{code: "22P02", msg: "invalid input syntax for parameter"}
	switch oid {
	case pgtype.Int2OID, pgtype.Int4OID, pgtype.Int8OID:
		if _, err := strconv.ParseInt(strings.TrimSpace(text), 10, 64); err != nil {
			return query.Parameter{}, "", bad
		}
		return query.Parameter{Type: "int64", Value: str(strings.TrimSpace(text))}, "", nil
	case pgtype.Float4OID, pgtype.Float8OID:
		f, err := strconv.ParseFloat(strings.TrimSpace(text), 64)
		if err != nil {
			return query.Parameter{}, "", bad
		}
		b, err := json.Marshal(f)
		if err != nil {
			return query.Parameter{}, "", unsupported("non-finite float parameter")
		}
		return query.Parameter{Type: "float64", Value: b}, "", nil
	case pgtype.BoolOID:
		switch strings.ToLower(strings.TrimSpace(text)) {
		case "t", "true", "y", "yes", "on", "1":
			return query.Parameter{Type: "bool", Value: json.RawMessage("true")}, "", nil
		case "f", "false", "n", "no", "off", "0":
			return query.Parameter{Type: "bool", Value: json.RawMessage("false")}, "", nil
		}
		return query.Parameter{}, "", bad
	case 0, pgtype.TextOID, pgtype.VarcharOID, pgtype.BPCharOID, pgtype.NameOID, pgtype.UnknownOID:
		return query.Parameter{Type: "string", Value: str(text)}, "", nil
	case pgtype.NumericOID:
		cast, ok := decimalCast(text)
		if !ok {
			return query.Parameter{}, "", bad
		}
		return query.Parameter{Type: "string", Value: str(strings.TrimSpace(text))}, cast, nil
	case pgtype.ByteaOID:
		if !strings.HasPrefix(text, `\x`) {
			return query.Parameter{}, "", unsupported("escape-format bytea parameter")
		}
		b, err := hex.DecodeString(text[2:])
		if err != nil {
			return query.Parameter{}, "", bad
		}
		var sb strings.Builder
		for _, c := range b {
			fmt.Fprintf(&sb, `\x%02X`, c)
		}
		return query.Parameter{Type: "string", Value: str(sb.String())}, "BLOB", nil
	case pgtype.DateOID, pgtype.TimestampOID, pgtype.TimestamptzOID, pgtype.TimeOID:
		return query.Parameter{Type: "string", Value: str(text)}, castFor(oid), nil
	}
	return query.Parameter{}, "", unsupported(fmt.Sprintf("parameter type OID %d", oid))
}

func castFor(oid uint32) string {
	switch oid {
	case pgtype.DateOID:
		return "DATE"
	case pgtype.TimestampOID:
		return "TIMESTAMP"
	case pgtype.TimestamptzOID:
		return "TIMESTAMPTZ"
	case pgtype.TimeOID:
		return "TIME"
	case pgtype.ByteaOID:
		return "BLOB"
	}
	return ""
}

// decimalCast derives DECIMAL(p,s) from the literal so no digit is rounded.
func decimalCast(text string) (string, bool) {
	s := strings.TrimLeft(strings.TrimSpace(text), "+-")
	whole, frac, _ := strings.Cut(s, ".")
	if whole == "" && frac == "" {
		return "", false
	}
	for _, c := range whole + frac {
		if c < '0' || c > '9' {
			return "", false
		}
	}
	whole = strings.TrimLeft(whole, "0")
	p, sc := len(whole)+len(frac), len(frac)
	if p == 0 {
		p = 1
	}
	if p > 38 {
		return "", false
	}
	return fmt.Sprintf("DECIMAL(%d,%d)", p, sc), true
}
