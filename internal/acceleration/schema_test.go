// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"os"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
)

type schemaFixture struct{ schema *arrow.Schema }

func (f schemaFixture) Execute(_ context.Context, _ query.Request, sink query.Sink) (query.Stats, error) {
	return query.Stats{}, sink.Schema(f.schema)
}

func TestSchemaComparisonIsExact(t *testing.T) {
	md := arrow.NewMetadata([]string{"unit"}, []string{"count"})
	base := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true, Metadata: md}}, &md)
	if !SchemaEqual(base, base) {
		t.Fatal("identical schema rejected")
	}
	changed := arrow.NewMetadata([]string{"unit"}, []string{"other"})
	for name, schema := range map[string]*arrow.Schema{
		"name":            arrow.NewSchema([]arrow.Field{{Name: "ID", Type: arrow.PrimitiveTypes.Int64, Nullable: true, Metadata: md}}, &md),
		"width":           arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true, Metadata: md}}, &md),
		"nullability":     arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64, Metadata: md}}, &md),
		"field metadata":  arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true, Metadata: changed}}, &md),
		"schema metadata": arrow.NewSchema(base.Fields(), &changed),
		"added field":     arrow.NewSchema(append(base.Fields(), arrow.Field{Name: "extra", Type: arrow.PrimitiveTypes.Int64}), &md),
	} {
		t.Run(name, func(t *testing.T) {
			if SchemaEqual(base, schema) {
				t.Fatal("changed schema accepted")
			}
		})
	}
	for _, pair := range [][2]arrow.DataType{
		{&arrow.Decimal128Type{Precision: 10, Scale: 2}, &arrow.Decimal128Type{Precision: 10, Scale: 3}},
		{&arrow.TimestampType{Unit: arrow.Second, TimeZone: "UTC"}, &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "UTC"}},
		{&arrow.TimestampType{Unit: arrow.Second, TimeZone: "UTC"}, &arrow.TimestampType{Unit: arrow.Second, TimeZone: "Europe/London"}},
	} {
		a := arrow.NewSchema([]arrow.Field{{Name: "v", Type: pair[0]}}, nil)
		b := arrow.NewSchema([]arrow.Field{{Name: "v", Type: pair[1]}}, nil)
		if SchemaEqual(a, b) {
			t.Fatal("type semantics changed")
		}
	}
}

func TestSchemaRefreshRejectsChangeAndPreservesGeneration(t *testing.T) {
	_, m, _ := managerFixture(t)
	defer m.Close()
	first, err := m.Refresh(context.Background(), "orders_fast", false)
	if err != nil {
		t.Fatal(err)
	}
	if !storeDigest.MatchString(first.SchemaHash) {
		t.Fatal("missing schema fingerprint")
	}
	changed := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true}}, nil)
	m.factory = func(catalog.Config, query.Limits) (query.Executor, error) { return schemaFixture{changed}, nil }
	if _, err = m.Refresh(context.Background(), "orders_fast", false); !errors.Is(err, ErrSchemaMismatch) {
		t.Fatalf("changed refresh: %v", err)
	}
	current, err := m.Status("orders_fast")
	if err != nil || current.Generation != first.Generation || current.SchemaHash != first.SchemaHash {
		t.Fatalf("valid generation replaced: %+v %v", current, err)
	}
	unchanged := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)
	m.factory = func(catalog.Config, query.Limits) (query.Executor, error) { return schemaFixture{unchanged}, nil }
	next, err := m.Refresh(context.Background(), "orders_fast", false)
	if err != nil || next.Rows != 0 || next.Generation == first.Generation || next.SchemaHash != first.SchemaHash {
		t.Fatalf("compatible empty refresh: %+v %v", next, err)
	}
}

func TestReadOriginalParquetSchemaPreservesMetadataAndOffset(t *testing.T) {
	meta := arrow.NewMetadata([]string{"source"}, []string{"analytics"})
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true, Metadata: meta}}, &meta)
	var out bytes.Buffer
	sink := NewParquetSink(&out, query.DefaultLimits())
	if err := sink.Schema(schema); err != nil {
		t.Fatal(err)
	}
	if err := sink.Finish(); err != nil {
		t.Fatal(err)
	}
	reader := bytes.NewReader(out.Bytes())
	reader.Seek(3, 0)
	got, err := ReadParquetSchema(reader, int64(out.Len()))
	if err != nil || !SchemaEqual(schema, got) {
		t.Fatalf("schema changed: %v", err)
	}
	offset, _ := reader.Seek(0, 1)
	if offset != 3 {
		t.Fatal("schema read changed caller offset")
	}
	hash, err := SchemaFingerprint(got)
	if err != nil || !storeDigest.MatchString(hash) {
		t.Fatalf("hash: %s %v", hash, err)
	}
}

func TestReadSchemaRejectsCorruptAndOversizedFooter(t *testing.T) {
	for _, data := range [][]byte{nil, []byte("not parquet"), make([]byte, 20)} {
		if _, err := ReadParquetSchema(bytes.NewReader(data), int64(len(data))); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("corrupt footer accepted: %v", err)
		}
	}
	var data [20]byte
	copy(data[16:], "PAR1")
	binary.LittleEndian.PutUint32(data[12:16], maxSchemaFooterBytes+1)
	if _, err := ReadParquetSchema(bytes.NewReader(data[:]), int64(len(data))); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
}

func TestParquetSinkRejectsMetadataChangeWithinRefresh(t *testing.T) {
	var out bytes.Buffer
	sink := NewParquetSink(&out, query.DefaultLimits())
	initial := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	if err := sink.Schema(initial); err != nil {
		t.Fatal(err)
	}
	md := arrow.NewMetadata([]string{"classification"}, []string{"private"})
	if err := sink.Schema(arrow.NewSchema(initial.Fields(), &md)); err == nil {
		t.Fatal("schema metadata changed within result")
	}
	sink.Abort()
}

func TestLegacyGenerationDerivesSchemaFromParquet(t *testing.T) {
	_, m, _ := managerFixture(t)
	defer m.Close()
	original := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)
	tx, err := m.store.Begin(context.Background(), "orders_fast")
	if err != nil {
		t.Fatal(err)
	}
	sink := NewParquetSink(tx.File(), query.DefaultLimits())
	if err = sink.Schema(original); err != nil {
		t.Fatal(err)
	}
	if err = sink.Finish(); err != nil {
		t.Fatal(err)
	}
	legacy, err := tx.Commit("legacy", 0)
	if err != nil {
		t.Fatal(err)
	}
	if legacy.SchemaHash != "" {
		t.Fatal("legacy fixture unexpectedly includes contract")
	}
	updated, err := m.Refresh(context.Background(), "orders_fast", false)
	if err != nil || updated.SchemaHash == "" {
		t.Fatalf("legacy migration failed: %+v %v", updated, err)
	}
	file, err := os.Open(updated.Path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	got, err := ReadParquetSchema(file, updated.Bytes)
	if err != nil || !SchemaEqual(original, got) {
		t.Fatal("legacy schema changed", err)
	}
}
