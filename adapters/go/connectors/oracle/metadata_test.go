// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package oracle

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/adapters/go/connectors/sqlsession"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestOracleCatalogUsesBoundScopeAndCursor(t *testing.T) {
	s := &Session{Session: &sqlsession.Session{}, database: "service", schema: "APP"}
	spec := operations.MetadataSpec{Object: "tables", Limit: 20, Cursor: "40"}
	spec.Target.Name = `customer' OR 1=1 --`
	statement, args, numbers, err := s.metadataQuery(spec)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(statement, spec.Target.Name) || strings.Contains(statement, "?") || !strings.HasSuffix(statement, "OFFSET :6 ROWS FETCH NEXT :7 ROWS ONLY") || len(numbers) != 0 || !reflect.DeepEqual(args, []any{"service", "APP", "APP", spec.Target.Name, spec.Target.Name, int64(40), 20}) {
		t.Fatal("catalog query lost its bound scope")
	}
	for _, mutate := range []func(*operations.MetadataSpec){
		func(s *operations.MetadataSpec) { s.Target.Catalog = "other" },
		func(s *operations.MetadataSpec) { s.Target.Schema = "OTHER" },
		func(s *operations.MetadataSpec) { s.Cursor = "+1" },
		func(s *operations.MetadataSpec) { s.Cursor = "01" },
		func(s *operations.MetadataSpec) { s.Cursor = "1000001" },
		func(s *operations.MetadataSpec) { s.Limit = 10001 },
		func(s *operations.MetadataSpec) { s.Target.Name = "bad\x00name" },
	} {
		bad := spec
		mutate(&bad)
		if _, _, _, err := s.metadataQuery(bad); err == nil {
			t.Fatal("invalid metadata scope accepted")
		}
	}
	for _, object := range []string{"catalogs", "databases", "schemas", "tables", "columns", "primary_keys", "foreign_keys", "relationships", "indexes"} {
		_, _, _, err := s.metadataQuery(operations.MetadataSpec{Object: object, Limit: 10})
		if err != nil {
			t.Fatalf("supported metadata %s: %v", object, err)
		}
	}
}

func metadataRecord(t *testing.T, schema *arrow.Schema, rows [][]string) arrow.RecordBatch {
	t.Helper()
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	for _, row := range rows {
		for i, value := range row {
			field := b.Field(i).(*array.StringBuilder)
			if value == "NULL" {
				field.AppendNull()
			} else {
				field.Append(value)
			}
		}
	}
	return b.NewRecordBatch()
}

func TestOracleMetadataIntegerConversionPreservesNullsAndBounds(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true}, {Name: "position", Type: arrow.BinaryTypes.String, Nullable: true}}, nil)
	sink := &oracleSink{}
	defer sink.close()
	converted := &metadataSink{next: sink, numbers: map[string]bool{"position": true}, maxBytes: 1024}
	if err := converted.Schema(schema); err != nil {
		t.Fatal(err)
	}
	record := metadataRecord(t, schema, [][]string{{"total", "9007199254740993"}, {"other", "NULL"}})
	defer record.Release()
	if err := converted.Write(record); err != nil {
		t.Fatal(err)
	}
	column := sink.records[0].Column(1).(*array.Int64)
	if column.Value(0) != 9007199254740993 || !column.IsNull(1) || converted.bytes <= 0 {
		t.Fatal("metadata integer precision/null lost")
	}
	changed := arrow.NewSchema([]arrow.Field{{Name: "other", Type: arrow.BinaryTypes.String}, {Name: "position", Type: arrow.BinaryTypes.String}}, nil)
	badSchema := metadataRecord(t, changed, [][]string{{"value", "1"}})
	defer badSchema.Release()
	if !errors.Is(converted.Write(badSchema), adapter.ErrInvalid) {
		t.Fatal("changed input schema accepted")
	}
	for _, text := range []string{"1.5", "9223372036854775808"} {
		bad := metadataRecord(t, schema, [][]string{{"value", text}})
		err := converted.Write(bad)
		bad.Release()
		if !errors.Is(err, adapter.ErrInvalid) {
			t.Fatal("invalid numeric metadata accepted")
		}
	}
	limited := &metadataSink{next: sink, numbers: map[string]bool{"position": true}, maxBytes: 1}
	if err := limited.Schema(schema); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(limited.Write(record), adapter.ErrLimit) || len(sink.records) != 1 {
		t.Fatal("converted metadata exceeded its byte limit")
	}
}
