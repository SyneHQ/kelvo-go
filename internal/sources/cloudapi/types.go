// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cloudapi

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/decimal256"
)

type Column struct {
	Name, Type       string
	Precision, Scale int
}

var decimalText = regexp.MustCompile(`^[+-]?[0-9]+(?:\.[0-9]+)?$`)

func validDecimal(s string) bool { return len(s) <= 256 && decimalText.MatchString(s) }

func Schema(columns []Column, provider ...string) (*arrow.Schema, error) {
	if len(columns) < 1 || len(columns) > 4096 {
		return nil, query.NewError("UNSUPPORTED", "Unsupported cloud result schema")
	}
	fields := make([]arrow.Field, len(columns))
	for i, c := range columns {
		var dt arrow.DataType
		switch strings.ToUpper(c.Type) {
		case "TINYINT":
			dt = arrow.PrimitiveTypes.Int8
		case "SMALLINT":
			dt = arrow.PrimitiveTypes.Int16
		case "INT", "INTEGER":
			dt = arrow.PrimitiveTypes.Int32
		case "BIGINT", "LONG":
			dt = arrow.PrimitiveTypes.Int64
		case "FLOAT", "REAL":
			dt = arrow.PrimitiveTypes.Float64
			if len(provider) > 0 && provider[0] == "databricks" {
				dt = arrow.PrimitiveTypes.Float32
			}
		case "DOUBLE":
			dt = arrow.PrimitiveTypes.Float64
		case "BOOLEAN", "BOOL":
			dt = arrow.FixedWidthTypes.Boolean
		case "STRING", "VARCHAR", "CHAR", "TEXT":
			dt = arrow.BinaryTypes.String
		case "BINARY":
			dt = arrow.BinaryTypes.Binary
		case "DATE":
			dt = arrow.FixedWidthTypes.Date32
		case "TIMESTAMP_NTZ":
			dt = &arrow.TimestampType{Unit: arrow.Nanosecond}
		case "TIMESTAMP", "TIMESTAMP_LTZ":
			dt = &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}
		case "DECIMAL", "FIXED", "NUMBER", "NUMERIC":
			if c.Precision < 1 || c.Precision > 76 || c.Scale > c.Precision || c.Scale < -c.Precision {
				return nil, query.NewError("UNSUPPORTED", "Cloud decimal metadata is unsupported")
			}
			if c.Precision <= 38 {
				dt = &arrow.Decimal128Type{Precision: int32(c.Precision), Scale: int32(c.Scale)}
			} else {
				dt = &arrow.Decimal256Type{Precision: int32(c.Precision), Scale: int32(c.Scale)}
			}
		default:
			return nil, query.NewError("UNSUPPORTED", "Cloud result contains an unsupported type")
		}
		if len(c.Name) > 4096 {
			return nil, query.NewError("UNSUPPORTED", "Cloud column name exceeds its limit")
		}
		fields[i] = arrow.Field{Name: c.Name, Type: dt, Nullable: true, Metadata: arrow.MetadataFrom(map[string]string{"native_type": c.Type})}
	}
	return arrow.NewSchema(fields, nil), nil
}

func Row(schema *arrow.Schema, input []any, provider string) ([]any, error) {
	if len(input) != schema.NumFields() {
		return nil, query.NewError("QUERY_FAILED", "Cloud row does not match its schema")
	}
	row := make([]any, len(input))
	for i, v := range input {
		if v == nil {
			continue
		}
		value, err := scalar(schema.Field(i).Type, v, provider)
		if err != nil {
			return nil, query.NewError("UNSUPPORTED", "Cloud result value cannot be represented without loss")
		}
		row[i] = value
	}
	return row, nil
}

func scalar(dt arrow.DataType, v any, provider string) (any, error) {
	var s string
	switch x := v.(type) {
	case string:
		s = x
	case json.Number:
		s = x.String()
	case bool:
		s = strconv.FormatBool(x)
	default:
		return nil, fmt.Errorf("unsupported value")
	}
	switch t := dt.(type) {
	case *arrow.Int8Type:
		x, err := strconv.ParseInt(s, 10, 8)
		return int8(x), err
	case *arrow.Int16Type:
		x, err := strconv.ParseInt(s, 10, 16)
		return int16(x), err
	case *arrow.Int32Type:
		x, err := strconv.ParseInt(s, 10, 32)
		return int32(x), err
	case *arrow.Int64Type:
		return strconv.ParseInt(s, 10, 64)
	case *arrow.Float32Type:
		x, err := strconv.ParseFloat(s, 32)
		if math.IsInf(x, 0) || math.IsNaN(x) {
			return nil, fmt.Errorf("nonfinite")
		}
		return float32(x), err
	case *arrow.Float64Type:
		x, err := strconv.ParseFloat(s, 64)
		if math.IsInf(x, 0) || math.IsNaN(x) {
			return nil, fmt.Errorf("nonfinite")
		}
		return x, err
	case *arrow.BooleanType:
		return strconv.ParseBool(s)
	case *arrow.StringType:
		if _, ok := v.(string); !ok {
			return nil, fmt.Errorf("not string")
		}
		return s, nil
	case *arrow.BinaryType:
		if provider == "snowflake" {
			return hex.DecodeString(s)
		}
		return base64.StdEncoding.DecodeString(s)
	case *arrow.Decimal128Type:
		if !validDecimal(s) {
			return nil, fmt.Errorf("invalid decimal")
		}
		x, err := decimal128.FromString(s, t.Precision, t.Scale)
		if err != nil || !equalDecimal(s, x.ToString(t.Scale)) {
			return nil, fmt.Errorf("decimal loss")
		}
		return x, nil
	case *arrow.Decimal256Type:
		if !validDecimal(s) {
			return nil, fmt.Errorf("invalid decimal")
		}
		x, err := decimal256.FromString(s, t.Precision, t.Scale)
		if err != nil || !equalDecimal(s, x.ToString(t.Scale)) {
			return nil, fmt.Errorf("decimal loss")
		}
		return x, nil
	case *arrow.Date32Type:
		if provider == "snowflake" {
			days, err := strconv.ParseInt(s, 10, 32)
			if err != nil {
				return nil, err
			}
			return time.Unix(days*86400, 0).UTC(), nil
		}
		return time.Parse("2006-01-02", s)
	case *arrow.TimestampType:
		if provider == "snowflake" {
			return epoch(s)
		}
		layouts := []string{"2006-01-02 15:04:05.999999999", "2006-01-02T15:04:05.999999999"}
		if t.TimeZone != "" {
			layouts = append(layouts, time.RFC3339Nano, "2006-01-02 15:04:05.999999999Z07:00")
		}
		for _, layout := range layouts {
			if tm, err := time.Parse(layout, s); err == nil {
				return tm, nil
			}
		}
	}
	return nil, fmt.Errorf("unsupported value")
}
func equalDecimal(a, b string) bool {
	x, ok := new(big.Rat).SetString(a)
	if !ok {
		return false
	}
	y, ok := new(big.Rat).SetString(b)
	return ok && x.Cmp(y) == 0
}
func epoch(s string) (time.Time, error) {
	if !validDecimal(s) {
		return time.Time{}, fmt.Errorf("invalid epoch")
	}
	rat, ok := new(big.Rat).SetString(s)
	if !ok {
		return time.Time{}, fmt.Errorf("invalid epoch")
	}
	rat.Mul(rat, big.NewRat(1000000000, 1))
	if !rat.IsInt() || !rat.Num().IsInt64() {
		return time.Time{}, fmt.Errorf("epoch precision or range")
	}
	n := rat.Num().Int64()
	return time.Unix(n/1e9, n%1e9).UTC(), nil
}
