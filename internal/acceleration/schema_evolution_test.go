// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/endian"
)

func evolutionSchema(typ arrow.DataType) *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{{Name: "private_customer_column", Type: typ, Nullable: true}}, nil)
}
func evolutionPolicies() map[string]*catalog.SchemaEvolution {
	return map[string]*catalog.SchemaEvolution{
		"nil": nil, "strict": {}, "add": {AddNullableColumns: true}, "widen": {SafeWidening: true}, "both": {AddNullableColumns: true, SafeWidening: true},
	}
}
func assertEvolution(t *testing.T, previous, next *arrow.Schema, policy *catalog.SchemaEvolution, allowed bool) {
	t.Helper()
	err := CheckSchemaEvolution(previous, next, policy)
	if allowed && err != nil {
		t.Fatalf("allowed evolution rejected: %v", err)
	}
	if !allowed && !errors.Is(err, ErrSchemaMismatch) {
		t.Fatalf("unsafe evolution did not fail with schema mismatch: %v", err)
	}
	if err != nil && query.PublicError(err).Code != "SCHEMA_MISMATCH" {
		t.Fatal("schema rejection lost its public and retry classification")
	}
	if err != nil && strings.Contains(err.Error(), "private_customer_column") {
		t.Fatal("schema error exposed a private column")
	}
}
func TestSchemaEvolutionIntegerDirectionalMatrix(t *testing.T) {
	types := []struct {
		typ      arrow.DataType
		bits     int
		unsigned bool
	}{
		{arrow.PrimitiveTypes.Int8, 8, false}, {arrow.PrimitiveTypes.Int16, 16, false}, {arrow.PrimitiveTypes.Int32, 32, false}, {arrow.PrimitiveTypes.Int64, 64, false},
		{arrow.PrimitiveTypes.Uint8, 8, true}, {arrow.PrimitiveTypes.Uint16, 16, true}, {arrow.PrimitiveTypes.Uint32, 32, true}, {arrow.PrimitiveTypes.Uint64, 64, true},
	}
	for oldIndex, old := range types {
		for newIndex, next := range types {
			for name, policy := range evolutionPolicies() {
				t.Run(fmt.Sprintf("%d_%d_%s", oldIndex, newIndex, name), func(t *testing.T) {
					allowed := oldIndex == newIndex || (policy != nil && policy.SafeWidening && old.unsigned == next.unsigned && next.bits > old.bits)
					assertEvolution(t, evolutionSchema(old.typ), evolutionSchema(next.typ), policy, allowed)
				})
			}
		}
	}
}
func TestSchemaEvolutionIndependentFlagsAndAppendOnlyColumns(t *testing.T) {
	previous := evolutionSchema(arrow.PrimitiveTypes.Int32)
	add := arrow.NewSchema(append(previous.Fields(), arrow.Field{Name: "extra", Type: arrow.BinaryTypes.String, Nullable: true}), nil)
	widened := evolutionSchema(arrow.PrimitiveTypes.Int64)
	both := arrow.NewSchema(append(widened.Fields(), arrow.Field{Name: "extra", Type: arrow.BinaryTypes.String, Nullable: true}), nil)
	for name, policy := range evolutionPolicies() {
		t.Run(name, func(t *testing.T) {
			assertEvolution(t, previous, previous, policy, true)
			assertEvolution(t, previous, add, policy, policy != nil && policy.AddNullableColumns)
			assertEvolution(t, previous, widened, policy, policy != nil && policy.SafeWidening)
			assertEvolution(t, previous, both, policy, policy != nil && policy.AddNullableColumns && policy.SafeWidening)
			assertEvolution(t, both, previous, policy, false)
		})
	}
	policy := &catalog.SchemaEvolution{AddNullableColumns: true, SafeWidening: true}
	addedRequired := arrow.NewSchema(append(previous.Fields(), arrow.Field{Name: "extra", Type: arrow.PrimitiveTypes.Int64}), nil)
	prepended := arrow.NewSchema(append([]arrow.Field{{Name: "extra", Type: arrow.BinaryTypes.String, Nullable: true}}, previous.Fields()...), nil)
	renamed := arrow.NewSchema([]arrow.Field{{Name: "PRIVATE_CUSTOMER_COLUMN", Type: arrow.PrimitiveTypes.Int32, Nullable: true}}, nil)
	duplicate := arrow.NewSchema(append(previous.Fields(), arrow.Field{Name: "PRIVATE_CUSTOMER_COLUMN", Type: arrow.BinaryTypes.String, Nullable: true}), nil)
	for _, next := range []*arrow.Schema{addedRequired, prepended, renamed, duplicate} {
		assertEvolution(t, previous, next, policy, false)
	}
	// Reordering retained fields never becomes additive evolution.
	reordered := arrow.NewSchema([]arrow.Field{add.Field(1), add.Field(0)}, nil)
	assertEvolution(t, add, reordered, policy, false)
}
func TestSchemaEvolutionFloatAndCrossFamilyRules(t *testing.T) {
	policy := &catalog.SchemaEvolution{SafeWidening: true}
	assertEvolution(t, evolutionSchema(arrow.PrimitiveTypes.Float32), evolutionSchema(arrow.PrimitiveTypes.Float64), policy, true)
	for _, pair := range [][2]arrow.DataType{
		{arrow.PrimitiveTypes.Float64, arrow.PrimitiveTypes.Float32},
		{arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Float64},
		{arrow.PrimitiveTypes.Uint8, arrow.PrimitiveTypes.Float32},
		{arrow.PrimitiveTypes.Float32, arrow.PrimitiveTypes.Int64},
		{arrow.PrimitiveTypes.Int64, &arrow.Decimal128Type{Precision: 20, Scale: 0}},
		{arrow.FixedWidthTypes.Boolean, arrow.PrimitiveTypes.Int8},
		{arrow.BinaryTypes.Binary, arrow.BinaryTypes.String},
	} {
		assertEvolution(t, evolutionSchema(pair[0]), evolutionSchema(pair[1]), policy, false)
	}
	assertEvolution(t, evolutionSchema(arrow.PrimitiveTypes.Float32), evolutionSchema(arrow.PrimitiveTypes.Float64), &catalog.SchemaEvolution{AddNullableColumns: true}, false)
}
func TestSchemaEvolutionDecimalPrecisionOnly(t *testing.T) {
	policy := &catalog.SchemaEvolution{SafeWidening: true}
	previous := evolutionSchema(&arrow.Decimal128Type{Precision: 10, Scale: 2})
	for _, candidate := range []struct {
		precision, scale int32
		allowed          bool
	}{
		{10, 2, true}, {11, 2, true}, {38, 2, true}, {9, 2, false}, {12, 4, false}, {12, 1, false}, {0, 0, false}, {39, 2, false}, {10, -1, false}, {10, 11, false},
	} {
		t.Run(fmt.Sprintf("%d_%d", candidate.precision, candidate.scale), func(t *testing.T) {
			assertEvolution(t, previous, evolutionSchema(&arrow.Decimal128Type{Precision: candidate.precision, Scale: candidate.scale}), policy, candidate.allowed)
		})
	}
	assertEvolution(t, previous, evolutionSchema(&arrow.Decimal128Type{Precision: 11, Scale: 2}), nil, false)
	assertEvolution(t, previous, evolutionSchema(&arrow.Decimal256Type{Precision: 40, Scale: 2}), policy, false)
}
func TestSchemaEvolutionTemporalChangesRemainExplicit(t *testing.T) {
	policy := &catalog.SchemaEvolution{AddNullableColumns: true, SafeWidening: true}
	previous := evolutionSchema(&arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"})
	assertEvolution(t, previous, previous, policy, true)
	for _, typ := range []arrow.DataType{
		&arrow.TimestampType{Unit: arrow.Second, TimeZone: "UTC"}, &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "UTC"},
		&arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "Etc/UTC"},
		&arrow.TimestampType{Unit: arrow.Microsecond}, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "America/New_York"},
		arrow.FixedWidthTypes.Date32, arrow.FixedWidthTypes.Date64,
	} {
		assertEvolution(t, previous, evolutionSchema(typ), policy, false)
	}
	// Finer timestamp resolution does not preserve the full int64 time domain.
	assertEvolution(t, evolutionSchema(&arrow.TimestampType{Unit: arrow.Millisecond}), evolutionSchema(&arrow.TimestampType{Unit: arrow.Microsecond}), policy, false)
}
func TestSchemaEvolutionExistingNullabilityMetadataAndEndian(t *testing.T) {
	policy := &catalog.SchemaEvolution{AddNullableColumns: true, SafeWidening: true}
	metadata := arrow.NewMetadata([]string{"unit", "owner"}, []string{"count", "private-owner"})
	previous := arrow.NewSchema([]arrow.Field{{Name: "private_customer_column", Type: arrow.PrimitiveTypes.Int32, Nullable: true, Metadata: metadata}}, &metadata)
	for _, nullable := range []bool{false, true} {
		old := arrow.NewSchema([]arrow.Field{{Name: "private_customer_column", Type: arrow.PrimitiveTypes.Int32, Nullable: nullable}}, nil)
		next := arrow.NewSchema([]arrow.Field{{Name: "private_customer_column", Type: arrow.PrimitiveTypes.Int64, Nullable: !nullable}}, nil)
		assertEvolution(t, old, next, policy, false)
	}
	changed := arrow.NewMetadata([]string{"unit", "owner"}, []string{"currency", "private-owner"})
	assertEvolution(t, previous, arrow.NewSchema(previous.Fields(), &changed), policy, false)
	fields := previous.Fields()
	fields[0].Metadata = changed
	assertEvolution(t, previous, arrow.NewSchema(fields, &metadata), policy, false)
	reordered := arrow.NewMetadata([]string{"owner", "unit"}, []string{"private-owner", "count"})
	fields = previous.Fields()
	fields[0].Metadata = reordered
	fields[0].Type = arrow.PrimitiveTypes.Int64
	assertEvolution(t, previous, arrow.NewSchema(fields, &reordered), policy, true)
	otherEndian := endian.BigEndian
	if previous.Endianness() == otherEndian {
		otherEndian = endian.LittleEndian
	}
	assertEvolution(t, previous, previous.WithEndianness(otherEndian), policy, false)
}
func TestSchemaEvolutionDoesNotInferExtensionSemantics(t *testing.T) {
	policy := &catalog.SchemaEvolution{SafeWidening: true}
	for _, key := range []string{"ARROW:extension:name", "ARROW:extension:metadata"} {
		metadata := arrow.NewMetadata([]string{key}, []string{"private.semantic.type"})
		previous := arrow.NewSchema([]arrow.Field{{Name: "private_customer_column", Type: arrow.PrimitiveTypes.Int32, Metadata: metadata}}, nil)
		assertEvolution(t, previous, previous, policy, true)
		next := arrow.NewSchema([]arrow.Field{{Name: "private_customer_column", Type: arrow.PrimitiveTypes.Int64, Metadata: metadata}}, nil)
		assertEvolution(t, previous, next, policy, false)
	}
}
func TestSchemaEvolutionInvalidSchemasAndAddedTypes(t *testing.T) {
	policy := &catalog.SchemaEvolution{AddNullableColumns: true, SafeWidening: true}
	previous := evolutionSchema(arrow.PrimitiveTypes.Int32)
	for _, next := range []*arrow.Schema{nil, arrow.NewSchema(nil, nil), arrow.NewSchema([]arrow.Field{{Name: "", Type: arrow.PrimitiveTypes.Int32}}, nil)} {
		assertEvolution(t, previous, next, policy, false)
	}
	var typedNil *arrow.Decimal128Type
	for _, typ := range []arrow.DataType{typedNil, arrow.Null, arrow.BinaryTypes.LargeString, arrow.ListOf(arrow.PrimitiveTypes.Int64), &arrow.Decimal256Type{Precision: 40, Scale: 2}} {
		next := arrow.NewSchema(append(previous.Fields(), arrow.Field{Name: "extra", Type: typ, Nullable: true}), nil)
		assertEvolution(t, previous, next, policy, false)
	}
	reserved := arrow.NewMetadata([]string{"ARROW:schema"}, []string{"private-metadata"})
	assertEvolution(t, previous, arrow.NewSchema(previous.Fields(), &reserved), policy, false)
	oversized := arrow.NewMetadata([]string{"private-key"}, []string{strings.Repeat("x", maxSchemaBytes+1)})
	assertEvolution(t, previous, arrow.NewSchema(previous.Fields(), &oversized), policy, false)
	assertEvolution(t, arrow.NewSchema(nil, nil), previous, policy, false)
	fields := make([]arrow.Field, 4097)
	for i := range fields {
		fields[i] = arrow.Field{Name: fmt.Sprintf("field_%d", i), Type: arrow.PrimitiveTypes.Int32, Nullable: true}
	}
	assertEvolution(t, previous, arrow.NewSchema(fields, nil), policy, false)
	assertEvolution(t, nil, previous, nil, true)
	assertEvolution(t, nil, nil, policy, false)
}
