// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package trino

import (
	"encoding/json"
	"regexp"
	"strconv"
	"strings"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/sources/cloudapi"
	"github.com/apache/arrow-go/v18/arrow"
)

var decimalType = regexp.MustCompile(`^decimal\(([0-9]{1,2}),\s*([0-9]{1,2})\)$`)
var stringType = regexp.MustCompile(`^(varchar|char)\([0-9]{1,10}\)$`)
var timestampType = regexp.MustCompile(`^timestamp(?:\(([0-9]{1,2})\))?$`)
var timestampValue = regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.([0-9]+))?$`)

func resultSchema(columns []column) (*arrow.Schema, error) {
	if len(columns) < 1 || len(columns) > 4096 {
		return nil, query.NewError("UNSUPPORTED", "Unsupported Trino/Presto result schema")
	}
	fields := make([]arrow.Field, len(columns))
	for i, c := range columns {
		if len(c.Name) > 4096 || len(c.Type) > 256 {
			return nil, query.NewError("UNSUPPORTED", "Trino/Presto column metadata exceeds its limit")
		}
		typ := strings.ToLower(strings.TrimSpace(c.Type))
		mapped := cloudapi.Column{Name: c.Name, Type: typ}
		var dt arrow.DataType
		switch {
		case typ == "unknown":
			dt = arrow.Null
		case typ == "real":
			dt = arrow.PrimitiveTypes.Float32
		case typ == "varbinary":
			mapped.Type = "binary"
		case stringType.MatchString(typ):
			mapped.Type = "string"
		case decimalType.MatchString(typ):
			parts := decimalType.FindStringSubmatch(typ)
			mapped.Type = "decimal"
			mapped.Precision, _ = strconv.Atoi(parts[1])
			mapped.Scale, _ = strconv.Atoi(parts[2])
			if mapped.Precision < 1 || mapped.Precision > 38 || mapped.Scale > mapped.Precision {
				return nil, query.NewError("UNSUPPORTED", "Unsupported Trino/Presto decimal type")
			}
		case timestampType.MatchString(typ):
			parts := timestampType.FindStringSubmatch(typ)
			precision := 3
			if parts[1] != "" {
				precision, _ = strconv.Atoi(parts[1])
			}
			if precision > 9 {
				return nil, query.NewError("UNSUPPORTED", "Trino/Presto timestamp exceeds nanosecond precision")
			}
			unit := arrow.Millisecond
			if precision > 3 {
				unit = arrow.Microsecond
			}
			if precision > 6 {
				unit = arrow.Nanosecond
			}
			dt = &arrow.TimestampType{Unit: unit}
		default:
			switch typ {
			case "tinyint", "smallint", "integer", "bigint", "double", "boolean", "varchar", "char", "date":
			default:
				return nil, query.NewError("UNSUPPORTED", "Trino/Presto result contains an unsupported type")
			}
		}
		if dt == nil {
			schema, err := cloudapi.Schema([]cloudapi.Column{mapped})
			if err != nil {
				return nil, err
			}
			dt = schema.Field(0).Type
		}
		fields[i] = arrow.Field{Name: c.Name, Type: dt, Nullable: true, Metadata: arrow.MetadataFrom(map[string]string{"native_type": typ})}
	}
	return arrow.NewSchema(fields, nil), nil
}

func resultRow(schema *arrow.Schema, input []any) ([]any, error) {
	if len(input) != schema.NumFields() {
		return nil, query.NewError("QUERY_FAILED", "Trino/Presto row does not match its schema")
	}
	for i, value := range input {
		if value == nil {
			continue
		}
		valid := false
		switch schema.Field(i).Type.(type) {
		case *arrow.Int8Type, *arrow.Int16Type, *arrow.Int32Type, *arrow.Int64Type, *arrow.Float32Type, *arrow.Float64Type:
			_, valid = value.(json.Number)
		case *arrow.Decimal128Type:
			_, valid = value.(string)
			if !valid {
				_, valid = value.(json.Number)
			}
		case *arrow.BooleanType:
			_, valid = value.(bool)
		case *arrow.StringType, *arrow.BinaryType, *arrow.Date32Type:
			_, valid = value.(string)
		case *arrow.TimestampType:
			text, ok := value.(string)
			if ok {
				match := timestampValue.FindStringSubmatch(text)
				precision := 3
				name, _ := schema.Field(i).Metadata.GetValue("native_type")
				if parts := timestampType.FindStringSubmatch(name); len(parts) > 1 && parts[1] != "" {
					precision, _ = strconv.Atoi(parts[1])
				}
				valid = len(match) > 0 && len(match[1]) <= precision
			}
		}
		if !valid {
			return nil, query.NewError("UNSUPPORTED", "Trino/Presto value does not match its declared type")
		}
	}
	return cloudapi.Row(schema, input, "trino")
}
