// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package exasol

import (
	"encoding/json"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/sources/cloudapi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/exasol/exasol-driver-go/pkg/types"
	"math/big"
	"regexp"
	"strconv"
	"strings"
)

var timestampText = regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,9})?$`)
var exponentText = regexp.MustCompile(`^-?[0-9]+(?:\.[0-9]+)?[eE]([+-]?[0-9]{1,3})$`)

func makeSchema(columns []types.SqlQueryColumn) (*arrow.Schema, error) {
	if len(columns) == 0 || len(columns) > 4096 {
		return nil, query.NewError("UNSUPPORTED", "Exasol result schema is unsupported")
	}
	fields := make([]arrow.Field, len(columns))
	for i, c := range columns {
		mapped := cloudapi.Column{Name: c.Name, Type: c.DataType.Type}
		switch c.DataType.Type {
		case "BOOLEAN", "DOUBLE", "CHAR", "VARCHAR", "DATE":
		case "DECIMAL":
			if c.DataType.Precision == nil || c.DataType.Scale == nil || *c.DataType.Precision < 1 || *c.DataType.Precision > 36 || *c.DataType.Scale < 0 || *c.DataType.Scale > *c.DataType.Precision {
				return nil, query.NewError("UNSUPPORTED", "Exasol decimal metadata is unsupported")
			}
			mapped.Precision, mapped.Scale = int(*c.DataType.Precision), int(*c.DataType.Scale)
		case "TIMESTAMP", "TIMESTAMP WITH LOCAL TIME ZONE":
			mapped.Type = "TIMESTAMP_NTZ"
			if c.DataType.Type == "TIMESTAMP WITH LOCAL TIME ZONE" || c.DataType.WithLocalTimeZone != nil && *c.DataType.WithLocalTimeZone {
				mapped.Type = "TIMESTAMP"
			}
		default:
			return nil, query.NewError("UNSUPPORTED", "Exasol result contains an unsupported type")
		}
		if c.Name == "" {
			return nil, query.NewError("QUERY_FAILED", "Exasol column name is empty")
		}
		schema, err := cloudapi.Schema([]cloudapi.Column{mapped})
		if err != nil {
			return nil, err
		}
		field := schema.Field(0)
		field.Metadata = arrow.MetadataFrom(map[string]string{"native_type": c.DataType.Type, "source_type": "exasol"})
		fields[i] = field
	}
	return arrow.NewSchema(fields, nil), nil
}
func makeRow(schema *arrow.Schema, values []any) ([]any, error) {
	values = append([]any(nil), values...)
	for i, value := range values {
		if value == nil {
			continue
		}
		if schema.Field(i).Type.ID() == arrow.BOOL {
			if _, ok := value.(bool); !ok {
				return nil, query.NewError("UNSUPPORTED", "Exasol BOOLEAN requires a JSON boolean")
			}
		}
		if decimal, ok := schema.Field(i).Type.(*arrow.Decimal128Type); ok {
			var text string
			switch v := value.(type) {
			case json.Number:
				text = v.String()
			case string:
				text = v
			}
			if strings.ContainsAny(text, "eE") {
				match := exponentText.FindStringSubmatch(text)
				if len(text) > 256 || match == nil {
					return nil, query.NewError("UNSUPPORTED", "Exasol decimal exponent is unsupported")
				}
				exponent, _ := strconv.Atoi(match[1])
				if exponent < -76 || exponent > 76 {
					return nil, query.NewError("UNSUPPORTED", "Exasol decimal exponent is unsupported")
				}
				exact, ok := new(big.Rat).SetString(text)
				if !ok {
					return nil, query.NewError("UNSUPPORTED", "Exasol decimal is invalid")
				}
				plain := exact.FloatString(int(decimal.Scale))
				actual, _ := new(big.Rat).SetString(plain)
				if exact.Cmp(actual) != 0 {
					return nil, query.NewError("UNSUPPORTED", "Exasol decimal scale would lose precision")
				}
				values[i] = plain
			}
		}
		if schema.Field(i).Type.ID() == arrow.TIMESTAMP {
			text, ok := value.(string)
			if !ok || !timestampText.MatchString(text) {
				return nil, query.NewError("UNSUPPORTED", "Exasol timestamp cannot be represented without loss")
			}
		}
	}
	return cloudapi.Row(schema, values, "exasol")
}
