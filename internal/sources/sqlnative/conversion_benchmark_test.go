// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package sqlnative

import (
	"errors"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
)

var benchmarkConvertedRow []any

// Keep the prior conversion path as an allocation reference. This comparison
// measures row conversion only. It excludes database, transport and Arrow work.
func referenceRowConversion(schema *arrow.Schema, values []any) ([]any, error) {
	row := make([]any, len(values))
	for i, value := range values {
		if value == nil {
			continue
		}
		field := schema.Field(i)
		source, _ := field.Metadata.GetValue("source_type")
		if source == "mysql" || source == "mariadb" {
			if date, ok := value.(time.Time); ok && date.IsZero() {
				return nil, errors.New("invalid zero date")
			}
		}
		converted, err := normalizeTextValue(field.Type, value)
		if err != nil {
			return nil, err
		}
		row[i] = converted
	}
	return row, nil
}

func BenchmarkRelationalRowConversion(b *testing.B) {
	metadata := arrow.MetadataFrom(map[string]string{"source_type": "postgres"})
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Metadata: metadata},
		{Name: "zone", Type: arrow.PrimitiveTypes.Int32, Metadata: metadata},
		{Name: "count", Type: arrow.PrimitiveTypes.Int64, Metadata: metadata},
		{Name: "rate", Type: arrow.PrimitiveTypes.Float64, Metadata: metadata},
		{Name: "enabled", Type: arrow.FixedWidthTypes.Boolean, Metadata: metadata},
		{Name: "label", Type: arrow.BinaryTypes.String, Metadata: metadata},
		{Name: "payload", Type: arrow.BinaryTypes.Binary, Metadata: metadata},
		{Name: "optional", Type: arrow.PrimitiveTypes.Int64, Nullable: true, Metadata: metadata},
	}, nil)
	for _, test := range []struct {
		name   string
		values []any
	}{
		{"typed", []any{int64(3000001), int32(12345), int64(5000000000), float64(1.25), true, "analytics", []byte("payload"), nil}},
		{"text", []any{[]byte("3000001"), []byte("12345"), []byte("5000000000"), []byte("1.25"), []byte("true"), []byte("analytics"), []byte("payload"), nil}},
	} {
		b.Run(test.name, func(b *testing.B) {
			b.Run("reference", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					row, err := referenceRowConversion(schema, test.values)
					if err != nil {
						b.Fatal(err)
					}
					benchmarkConvertedRow = row
				}
			})
			b.Run("reused", func(b *testing.B) {
				conversion := newRowConversion(schema)
				b.ReportAllocs()
				for b.Loop() {
					row, err := conversion.convert(test.values)
					if err != nil {
						b.Fatal(err)
					}
					benchmarkConvertedRow = row
				}
			})
		})
	}
}
