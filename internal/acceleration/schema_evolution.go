// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/apache/arrow-go/v18/arrow"
)

// CheckSchemaEvolution admits a complete new refresh schema against the previous
// immutable generation. It does not cast data, rewrite old generations, or relax
// SchemaEqual within a generation. A nil policy preserves the strict default.
// The widening whitelist is intentionally smaller than SQL implicit-cast rules:
// every value in an admitted source type must retain its numeric meaning in the
// target type. Float32-to-Float64 admits a fresh source schema; it never converts
// old floating-point buffers. Existing column identities, nullability and metadata
// stay exact.
func CheckSchemaEvolution(previous, next *arrow.Schema, policy *catalog.SchemaEvolution) error {
	reject := func(reason string) error { return fmt.Errorf("%w: %s", ErrSchemaMismatch, reason) }
	if !validEvolutionSchema(next) {
		return reject("invalid candidate schema")
	}
	if previous == nil {
		return nil
	}
	if !validEvolutionSchema(previous) {
		return reject("invalid previous schema")
	}
	if previous.Endianness() != next.Endianness() {
		return reject("schema endianness changed")
	}
	if !previous.Metadata().Equal(next.Metadata()) {
		return reject("schema metadata changed")
	}
	if next.NumFields() < previous.NumFields() {
		return reject("columns cannot be removed")
	}
	add, widen := false, false
	if policy != nil {
		add, widen = policy.AddNullableColumns, policy.SafeWidening
	}
	if next.NumFields() > previous.NumFields() && !add {
		return reject("added columns require an explicit policy")
	}
	for i, old := range previous.Fields() {
		current := next.Field(i)
		if old.Name != current.Name {
			return reject("column identity or order changed")
		}
		if old.Nullable != current.Nullable {
			return reject("existing column nullability changed")
		}
		if !old.Metadata.Equal(current.Metadata) {
			return reject("existing column metadata changed")
		}
		if arrow.TypeEqual(old.Type, current.Type) {
			continue
		}
		if !widen || hasExtensionMetadata(old.Metadata) || !safeEvolutionWidening(old.Type, current.Type) {
			return reject("column type change is not permitted")
		}
	}
	for i := previous.NumFields(); i < next.NumFields(); i++ {
		if !next.Field(i).Nullable {
			return reject("added columns must be nullable")
		}
	}
	return nil
}

// Mirror the sink's supported scalar contract so the standalone comparator does
// not authorize impossible or malformed schemas. No schema or field text is
// included in returned errors. Original Arrow metadata stays size-bounded.
func validEvolutionSchema(schema *arrow.Schema) bool {
	if schema == nil || schema.NumFields() == 0 || schema.NumFields() > 4096 {
		return false
	}
	if _, reserved := schema.Metadata().GetValue("ARROW:schema"); reserved {
		return false
	}
	names := make(map[string]bool, schema.NumFields())
	for _, field := range schema.Fields() {
		name := strings.ToLower(field.Name)
		if name == "" || names[name] || field.Type == nil {
			return false
		}
		names[name] = true
		typ := reflect.ValueOf(field.Type)
		if typ.Kind() == reflect.Pointer && typ.IsNil() {
			return false
		}
		if parquetFieldType(field.Type) != nil {
			return false
		}
	}
	_, err := SchemaFingerprint(schema)
	return err == nil
}
func hasExtensionMetadata(metadata arrow.Metadata) bool {
	_, name := metadata.GetValue("ARROW:extension:name")
	_, value := metadata.GetValue("ARROW:extension:metadata")
	return name || value
}
func evolutionIntegerWidth(typ arrow.DataType) (bits int, unsigned bool) {
	switch typ.(type) {
	case *arrow.Int8Type:
		return 8, false
	case *arrow.Int16Type:
		return 16, false
	case *arrow.Int32Type:
		return 32, false
	case *arrow.Int64Type:
		return 64, false
	case *arrow.Uint8Type:
		return 8, true
	case *arrow.Uint16Type:
		return 16, true
	case *arrow.Uint32Type:
		return 32, true
	case *arrow.Uint64Type:
		return 64, true
	}
	return 0, false
}
func safeEvolutionWidening(previous, next arrow.DataType) bool {
	oldBits, oldUnsigned := evolutionIntegerWidth(previous)
	newBits, newUnsigned := evolutionIntegerWidth(next)
	if oldBits != 0 && newBits != 0 {
		return oldUnsigned == newUnsigned && newBits > oldBits
	}
	switch old := previous.(type) {
	case *arrow.Float32Type:
		_, ok := next.(*arrow.Float64Type)
		return ok
	case *arrow.Decimal128Type:
		current, ok := next.(*arrow.Decimal128Type)
		return ok && old != nil && current != nil && old.Scale == current.Scale && current.Precision > old.Precision && old.Precision >= 1 && current.Precision <= 38 && old.Scale >= 0 && old.Scale <= old.Precision
	}
	return false
}
