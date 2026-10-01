// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package ignite

import (
	"encoding/json"
	"regexp"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/sources/cloudapi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/google/uuid"
)

var decimalPattern = regexp.MustCompile(`^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?$`)
var timestampPattern = regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,9})?$`)

func makeSchema(columns []column) (*arrow.Schema, error) {
	if len(columns) == 0 || len(columns) > 4096 {
		return nil, query.NewError("UNSUPPORTED", "Ignite result schema is unsupported")
	}
	fields := make([]arrow.Field, len(columns))
	for i, col := range columns {
		if col.Name == "" || len(col.Name) > 4096 || len(col.Schema) > 4096 || len(col.Table) > 4096 {
			return nil, query.NewError("UNSUPPORTED", "Ignite column metadata exceeds its limit")
		}
		var kind arrow.DataType
		metadata := map[string]string{"native_type": col.Type, "schema_name": col.Schema, "table_name": col.Table}
		switch col.Type {
		case "java.lang.Boolean":
			kind = arrow.FixedWidthTypes.Boolean
		case "java.lang.Byte":
			kind = arrow.PrimitiveTypes.Int8
		case "java.lang.Short":
			kind = arrow.PrimitiveTypes.Int16
		case "java.lang.Integer":
			kind = arrow.PrimitiveTypes.Int32
		case "java.lang.Long":
			kind = arrow.PrimitiveTypes.Int64
		case "java.lang.Float":
			kind = arrow.PrimitiveTypes.Float32
		case "java.lang.Double":
			kind = arrow.PrimitiveTypes.Float64
		case "java.lang.String":
			kind = arrow.BinaryTypes.String
		case "[B":
			kind = arrow.BinaryTypes.Binary
		case "java.sql.Date":
			kind = arrow.FixedWidthTypes.Date32
		case "java.sql.Timestamp":
			kind = &arrow.TimestampType{Unit: arrow.Nanosecond}
		case "java.lang.Void":
			kind = arrow.Null
		case "java.math.BigDecimal":
			// REST metadata omits precision and scale. Exact text retains every
			// digit, exponent and trailing zero without guessing a scale from rows.
			kind = arrow.BinaryTypes.String
			metadata["logical_type"], metadata["encoding"] = "decimal", "exact_numeric_text"
		case "java.sql.Time":
			kind = arrow.BinaryTypes.String
			metadata["logical_type"], metadata["encoding"] = "time", "HH:mm:ss"
		case "java.util.UUID":
			kind = arrow.BinaryTypes.String
			metadata["logical_type"] = "uuid"
		default:
			return nil, query.NewError("UNSUPPORTED", "Ignite result contains an unsupported Java type")
		}
		fields[i] = arrow.Field{Name: col.Name, Type: kind, Nullable: true, Metadata: arrow.MetadataFrom(metadata)}
	}
	return arrow.NewSchema(fields, nil), nil
}

func makeRow(schema *arrow.Schema, values []any) ([]any, error) {
	if len(values) != schema.NumFields() {
		return nil, query.NewError("QUERY_FAILED", "Ignite row does not match its schema")
	}
	input := append([]any(nil), values...)
	for i, value := range input {
		if value == nil {
			continue
		}
		native, _ := schema.Field(i).Metadata.GetValue("native_type")
		switch native {
		case "[B":
			// Ignite 2.17 reports SQL UUID expressions as byte[] metadata while
			// serializing their values as UUID text. Do not guess a new column
			// type from a row or decode that text as binary data.
			if text, ok := value.(string); ok && len(text) == 36 {
				if _, err := uuid.Parse(text); err == nil {
					return nil, query.NewError("UNSUPPORTED", "Ignite UUID values with binary metadata require an explicit VARCHAR cast")
				}
			}
		case "java.sql.Timestamp":
			text, ok := value.(string)
			if !ok || !timestampPattern.MatchString(text) {
				return nil, invalidValue()
			}
		case "java.math.BigDecimal":
			var text string
			switch v := value.(type) {
			case json.Number:
				text = v.String()
			case string:
				text = v
			}
			if len(text) > 4096 || !decimalPattern.MatchString(text) {
				return nil, invalidValue()
			}
			input[i] = text
		case "java.sql.Time":
			text, ok := value.(string)
			if !ok || len(text) != 8 {
				return nil, invalidValue()
			}
			if _, err := time.Parse("15:04:05", text); err != nil {
				return nil, invalidValue()
			}
		case "java.util.UUID":
			text, ok := value.(string)
			if !ok || len(text) != 36 {
				return nil, invalidValue()
			}
			if _, err := uuid.Parse(text); err != nil {
				return nil, invalidValue()
			}
		}
	}
	return cloudapi.Row(schema, input, "ignite")
}

func invalidValue() error {
	return query.NewError("UNSUPPORTED", "Ignite result value cannot be represented without loss")
}
