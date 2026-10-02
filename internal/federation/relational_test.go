// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package federation

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/duckbridge"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func relationalSchema(kind string) *arrow.Schema {
	var idType arrow.DataType = arrow.PrimitiveTypes.Int64
	idNative, amountNative, atNative, labelNative := "INT8", "NUMERIC", "TIMESTAMPTZ", "TEXT"
	if kind == "mysql" {
		idType, idNative, amountNative, atNative, labelNative = arrow.PrimitiveTypes.Uint64, "UNSIGNED BIGINT", "DECIMAL", "TIMESTAMP", "VARCHAR"
	}
	fields := []arrow.Field{
		{Name: "id", Type: idType, Nullable: true},
		{Name: "amount", Type: &arrow.Decimal128Type{Precision: 30, Scale: 10}, Nullable: true},
		{Name: "at", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
		{Name: "label", Type: arrow.BinaryTypes.String, Nullable: true},
	}
	for i, native := range []string{idNative, amountNative, atNative, labelNative} {
		fields[i].Metadata = arrow.MetadataFrom(map[string]string{"native_type": native, "source_type": kind})
	}
	metadata := arrow.MetadataFrom(map[string]string{"describe_revision": "fixture-v1"})
	return arrow.NewSchema(fields, &metadata)
}

func relationalRecord(allocator memory.Allocator, schema *arrow.Schema) arrow.RecordBatch {
	builder := array.NewRecordBuilder(allocator, schema)
	defer builder.Release()
	amount, _ := decimal128.FromString("12345678901234567890.0123456789", 30, 10)
	for i, field := range schema.Fields() {
		switch field.Name {
		case "id":
			builder.Field(i).AppendNull()
			switch ids := builder.Field(i).(type) {
			case *array.Int64Builder:
				ids.Append(-9223372036854775808)
			case *array.Uint64Builder:
				ids.Append(18446744073709551615)
			}
		case "amount":
			builder.Field(i).(*array.Decimal128Builder).AppendValues([]decimal128.Num{amount, amount}, nil)
		case "at":
			builder.Field(i).(*array.TimestampBuilder).AppendValues([]arrow.Timestamp{-315521754876544, 1234567}, nil)
		case "label":
			builder.Field(i).AppendNull()
			builder.Field(i).(*array.StringBuilder).Append("kept")
		}
	}
	return builder.NewRecordBatch()
}

func TestRelationalScansPreserveExactValuesMetadataAndCountRows(t *testing.T) {
	for _, kind := range []string{"postgres", "mysql"} {
		t.Run(kind, func(t *testing.T) {
			allocator := memory.NewCheckedAllocator(memory.NewGoAllocator())
			source, selected := relationalSource(kind)
			source.Federation.MaxScanRows, source.Federation.MaxScanBytes = 77, 4096
			schema := relationalSchema(kind)
			metadata := schema.Metadata()
			projected := arrow.NewSchema([]arrow.Field{schema.Field(3), schema.Field(1), schema.Field(0), schema.Field(2)}, &metadata)
			countSchema := arrow.NewSchema([]arrow.Field{schema.Field(0)}, &metadata)
			dialect, _ := dialectFor(kind)
			remote, _ := dialect.tableName(selected)
			id, _ := dialect.quoteIdentifier("id")
			var closed atomic.Int32
			factory := func(config catalog.Config, limits query.Limits) (execution, error) {
				if len(config.Sources) != 1 || config.Sources[0].ID != source.ID || limits.MaxRows != 77 || limits.MaxBytes != 4096 {
					return nil, errors.New("selected source identity or scan limits changed")
				}
				return &fakeExecutor{run: func(ctx context.Context, request query.Request, sink query.Sink) error {
					if request.Mode != "native" || request.ConnectionID != source.ID {
						return errors.New("scan left its selected native source")
					}
					if strings.HasSuffix(request.SQL, " LIMIT 0") {
						if request.SQL != "SELECT * FROM "+remote+" LIMIT 0" {
							return errors.New("describe changed schema/database qualification")
						}
						return sink.Schema(schema)
					}
					batchSchema := projected
					if request.SQL == "SELECT "+id+" FROM "+remote {
						batchSchema = countSchema
					}
					if err := sink.Schema(batchSchema); err != nil {
						return err
					}
					record := relationalRecord(allocator, batchSchema)
					defer record.Release()
					return sink.Write(record)
				}, close: func() { closed.Add(1) }}, nil
			}
			table, err := newTable(context.Background(), source, selected, query.DefaultLimits(), factory)
			if err != nil {
				t.Fatal(err)
			}
			defer table.Close()
			reader, err := table.Scan(context.Background(), duckbridge.ScanPlan{Columns: []string{"label", "amount", "id", "at"}})
			if err != nil {
				t.Fatal(err)
			}
			if !reader.Next() {
				t.Fatalf("exact relational batch missing: %v", reader.Err())
			}
			record := reader.RecordBatch()
			record.Retain()
			reader.Release()
			if record.NumRows() != 2 || !record.Column(0).IsNull(0) || !record.Column(2).IsNull(0) || !sameSchema(record.Schema(), projected) {
				t.Fatal("NULLs, projection order or source metadata changed")
			}
			if kind == "postgres" && record.Column(2).(*array.Int64).Value(1) != -9223372036854775808 {
				t.Fatal("PostgreSQL signed minimum changed")
			}
			if kind == "mysql" && record.Column(2).(*array.Uint64).Value(1) != 18446744073709551615 {
				t.Fatal("MySQL unsigned maximum changed")
			}
			if record.Column(1).(*array.Decimal128).Value(1).ToString(10) != "12345678901234567890.0123456789" || record.Column(3).(*array.Timestamp).Value(0) != -315521754876544 {
				t.Fatal("relational decimal or microsecond timestamp changed")
			}
			record.Release()
			count, err := table.Scan(context.Background(), duckbridge.ScanPlan{})
			if err != nil {
				t.Fatal(err)
			}
			if !sameSchema(count.Schema(), countSchema) || !count.Next() || count.RecordBatch().NumRows() != 2 || !count.RecordBatch().Column(0).IsNull(0) {
				t.Fatalf("count projection lost metadata or NULL-valued rows: %v", count.Err())
			}
			if count.Next() || count.Err() != nil {
				t.Fatalf("count projection failed at EOF: %v", count.Err())
			}
			count.Release()
			if closed.Load() != 3 {
				t.Fatal("describe and independent scans did not release their native executors")
			}
			allocator.AssertSize(t, 0)
		})
	}
}

func TestRelationalNativeTypeMetadataDriftFailsBeforeDelivery(t *testing.T) {
	for _, kind := range []string{"postgres", "mysql"} {
		t.Run(kind, func(t *testing.T) {
			source, selected := relationalSource(kind)
			schema := relationalSchema(kind)
			table, err := newTable(context.Background(), source, selected, query.DefaultLimits(), func(catalog.Config, query.Limits) (execution, error) {
				return &fakeExecutor{run: func(ctx context.Context, request query.Request, sink query.Sink) error {
					if strings.HasSuffix(request.SQL, " LIMIT 0") {
						return sink.Schema(schema)
					}
					field := schema.Field(0)
					field.Metadata = arrow.MetadataFrom(map[string]string{"native_type": "CHANGED", "source_type": kind})
					metadata := schema.Metadata()
					return sink.Schema(arrow.NewSchema([]arrow.Field{field}, &metadata))
				}}, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			defer table.Close()
			reader, err := table.Scan(context.Background(), duckbridge.ScanPlan{})
			if err != nil {
				t.Fatal(err)
			}
			if reader.Next() {
				t.Fatal("native metadata drift was delivered")
			}
			checkCode(t, reader.Err(), "QUERY_FAILED")
			reader.Release()
		})
	}
}
