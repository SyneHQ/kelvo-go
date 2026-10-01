// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package spanner

import (
	"encoding/base64"
	"encoding/json"
	"math"
	"math/big"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
)

type field struct {
	Name string    `json:"name"`
	Type valueType `json:"type"`
}
type valueType struct {
	Code         string          `json:"code"`
	Annotation   string          `json:"typeAnnotation"`
	ArrayElement json.RawMessage `json:"arrayElementType,omitempty"`
	Struct       json.RawMessage `json:"structType,omitempty"`
	ProtoName    string          `json:"protoTypeFqn"`
}

func makeSchema(fields []field) (*arrow.Schema, error) {
	if len(fields) == 0 || len(fields) > 4096 {
		return nil, unsupported()
	}
	out := make([]arrow.Field, len(fields))
	for i, f := range fields {
		if len(f.Name) > 4096 || len(f.Type.ArrayElement) != 0 || len(f.Type.Struct) != 0 || f.Type.ProtoName != "" {
			return nil, unsupported()
		}
		annotation := f.Type.Annotation
		if annotation != "" && annotation != "TYPE_ANNOTATION_CODE_UNSPECIFIED" && !(annotation == "PG_JSONB" && f.Type.Code == "JSON") && !(annotation == "PG_OID" && f.Type.Code == "INT64") {
			return nil, unsupported()
		}
		var dataType arrow.DataType
		switch f.Type.Code {
		case "BOOL":
			dataType = arrow.FixedWidthTypes.Boolean
		case "INT64":
			dataType = arrow.PrimitiveTypes.Int64
		case "FLOAT32":
			dataType = arrow.PrimitiveTypes.Float32
		case "FLOAT64":
			dataType = arrow.PrimitiveTypes.Float64
		case "STRING", "JSON", "UUID":
			dataType = arrow.BinaryTypes.String
		case "BYTES":
			dataType = arrow.BinaryTypes.Binary
		case "DATE":
			dataType = arrow.FixedWidthTypes.Date32
		case "TIMESTAMP":
			dataType = &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}
		case "NUMERIC":
			dataType = &arrow.Decimal128Type{Precision: 38, Scale: 9}
		default:
			return nil, unsupported()
		}
		metadata := map[string]string{"source_type": "spanner", "native_type": f.Type.Code}
		if annotation != "" {
			metadata["native_annotation"] = annotation
		}
		out[i] = arrow.Field{Name: f.Name, Type: dataType, Nullable: true, Metadata: arrow.MetadataFrom(metadata)}
	}
	return arrow.NewSchema(out, nil), nil
}

var numericText = regexp.MustCompile(`^[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?$`)
var timestampText = regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,9})?Z$`)
var uuidText = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

func makeRow(fields []field, input []any) ([]any, error) {
	if len(fields) != len(input) {
		return nil, query.NewError("QUERY_FAILED", "Spanner row does not match its schema")
	}
	out := make([]any, len(input))
	for i, raw := range input {
		if raw == nil {
			continue
		}
		code := fields[i].Type.Code
		var value any
		var err error
		if code == "BOOL" {
			v, ok := raw.(bool)
			if !ok {
				return nil, unsupported()
			}
			out[i] = v
			continue
		}
		if code == "FLOAT32" || code == "FLOAT64" {
			v, ok := raw.(json.Number)
			if !ok {
				return nil, unsupported()
			} // Nonfinite string values are explicit unsupported results.
			bits := 64
			if code == "FLOAT32" {
				bits = 32
			}
			n, parseErr := strconv.ParseFloat(v.String(), bits)
			if parseErr != nil || math.IsInf(n, 0) || math.IsNaN(n) {
				return nil, unsupported()
			}
			if bits == 32 {
				out[i] = float32(n)
			} else {
				out[i] = n
			}
			continue
		}
		text, ok := raw.(string)
		if !ok {
			return nil, unsupported()
		}
		switch code {
		case "INT64":
			value, err = strconv.ParseInt(text, 10, 64)
		case "STRING":
			value = text
		case "JSON":
			if !json.Valid([]byte(text)) {
				return nil, unsupported()
			}
			value = text
		case "UUID":
			if !uuidText.MatchString(text) {
				return nil, unsupported()
			}
			value = text
		case "BYTES":
			value, err = base64.StdEncoding.Strict().DecodeString(text)
		case "DATE":
			value, err = time.Parse("2006-01-02", text)
		case "TIMESTAMP":
			if !timestampText.MatchString(text) {
				return nil, unsupported()
			}
			value, err = time.Parse(time.RFC3339Nano, text)
		case "NUMERIC":
			value, err = numeric(text)
		default:
			return nil, unsupported()
		}
		if err != nil {
			return nil, unsupported()
		}
		out[i] = value
	}
	return out, nil
}

func numeric(text string) (decimal128.Num, error) {
	if len(text) > 128 || !numericText.MatchString(text) {
		return decimal128.Num{}, unsupported()
	}
	if i := strings.IndexAny(text, "eE"); i >= 0 {
		exponent, err := strconv.Atoi(text[i+1:])
		if err != nil || exponent < -128 || exponent > 128 {
			return decimal128.Num{}, unsupported()
		}
	}
	value, ok := new(big.Rat).SetString(text)
	if !ok {
		return decimal128.Num{}, unsupported()
	}
	scaled := new(big.Rat).Mul(value, big.NewRat(1000000000, 1))
	if !scaled.IsInt() || len(new(big.Int).Abs(scaled.Num()).String()) > 38 {
		return decimal128.Num{}, unsupported()
	}
	return decimal128.FromString(value.FloatString(9), 38, 9)
}

func unsupported() error {
	return query.NewError("UNSUPPORTED", "Spanner result cannot be represented without loss")
}
