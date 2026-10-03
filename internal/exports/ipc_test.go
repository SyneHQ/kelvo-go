package exports

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func ipcFixture(t *testing.T) ([]byte, string, Limits) {
	t.Helper()
	b := array.NewInt64Builder(memory.NewGoAllocator())
	b.AppendValues([]int64{1, 2, 3}, nil)
	a := b.NewArray()
	b.Release()
	defer a.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	r := array.NewRecordBatch(schema, []arrow.Array{a}, 3)
	defer r.Release()
	var out bytes.Buffer
	w := ipc.NewWriter(&out, ipc.WithSchema(schema))
	if err := w.Write(r); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err := canonicalSchema(schema)
	if err != nil {
		t.Fatal(err)
	}
	return out.Bytes(), checksum(raw), Limits{MaxRows: 10, MaxEncodedBytes: 1 << 20, MaxDecodedBytes: 1 << 20, MaxPartBytes: 1 << 20, MaxPartDecodedBytes: 1 << 20, MaxParts: 2}
}
func TestIPCRejectsTruncationTrailingStreamsAndForgedLengths(t *testing.T) {
	raw, schema, limits := ipcFixture(t)
	if stats, err := validatePart(context.Background(), bytes.NewReader(raw), int64(len(raw)), limits, schema); err != nil || stats.Rows != 3 {
		t.Fatal(stats, err)
	}
	oversized := bytes.Clone(raw)
	binary.LittleEndian.PutUint32(oversized[4:8], 1<<31)
	for _, invalid := range [][]byte{raw[:len(raw)-8], append(bytes.Clone(raw), raw...), append(bytes.Clone(raw), 0), oversized, raw[:6]} {
		if _, err := validatePart(context.Background(), bytes.NewReader(invalid), int64(len(invalid)), limits, schema); err == nil {
			t.Fatal("invalid stream accepted")
		}
	}
	limits.MaxPartDecodedBytes = 1
	if _, err := validatePart(context.Background(), bytes.NewReader(raw), int64(len(raw)), limits, schema); err == nil {
		t.Fatal("decoded limit ignored")
	}
}
func TestIPCSchemaMetadataAndRowLimitsAreExact(t *testing.T) {
	raw, schema, limits := ipcFixture(t)
	if _, err := validatePart(context.Background(), bytes.NewReader(raw), int64(len(raw)), limits, "wrong"); err == nil {
		t.Fatal("schema mismatch accepted")
	}
	limits.MaxRows = 2
	if _, err := validatePart(context.Background(), bytes.NewReader(raw), int64(len(raw)), limits, schema); err == nil {
		t.Fatal("rows limit ignored")
	}
}

func TestSchemaPreflightRejectsOversizedStructuresBeforeSerialization(t *testing.T) {
	large := strings.Repeat("x", schemaMaxStringBytes+1)
	oversizedMetadata := arrow.NewMetadata([]string{"metadata"}, []string{large})
	oversizedFields := make([]arrow.Field, schemaMaxNodes+1)
	for i := range oversizedFields {
		oversizedFields[i] = arrow.Field{Name: "n", Type: arrow.PrimitiveTypes.Int64}
	}
	manyKeys := make([]string, schemaMaxMetadataPairs+1)
	manyValues := make([]string, len(manyKeys))
	manyPairs := arrow.NewMetadata(manyKeys, manyValues)
	var deep arrow.DataType = arrow.PrimitiveTypes.Int64
	for range schemaMaxDepth {
		deep = arrow.ListOf(deep)
	}
	cycle := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32}
	cycle.ValueType = cycle
	shared := arrow.StructOf(oversizedFields[:64]...)
	sharedFields := make([]arrow.Field, 128)
	for i := range sharedFields {
		sharedFields[i] = arrow.Field{Name: "repeated", Type: shared}
	}
	aggregate := make([]arrow.Field, 10)
	for i := range aggregate {
		aggregate[i] = arrow.Field{Name: strings.Repeat("n", schemaMaxStringBytes), Type: arrow.PrimitiveTypes.Int64}
	}
	for name, schema := range map[string]*arrow.Schema{
		"schema metadata":     arrow.NewSchema(nil, &oversizedMetadata),
		"field metadata":      arrow.NewSchema([]arrow.Field{{Name: "n", Type: arrow.PrimitiveTypes.Int64, Metadata: oversizedMetadata}}, nil),
		"field name":          arrow.NewSchema([]arrow.Field{{Name: large, Type: arrow.PrimitiveTypes.Int64}}, nil),
		"timezone":            arrow.NewSchema([]arrow.Field{{Name: "t", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: large}}}, nil),
		"metadata pairs":      arrow.NewSchema(nil, &manyPairs),
		"field count":         arrow.NewSchema(oversizedFields, nil),
		"nested field count":  arrow.NewSchema([]arrow.Field{{Name: "s", Type: arrow.StructOf(oversizedFields...)}}, nil),
		"repeated references": arrow.NewSchema(sharedFields, nil),
		"depth":               arrow.NewSchema([]arrow.Field{{Name: "deep", Type: deep}}, nil),
		"dictionary cycle":    arrow.NewSchema([]arrow.Field{{Name: "cycle", Type: cycle}}, nil),
		"aggregate strings":   arrow.NewSchema(aggregate, nil),
	} {
		t.Run(name, func(t *testing.T) {
			if raw, err := canonicalSchema(schema); !errors.Is(err, ErrLimit) || raw != nil {
				t.Fatal("oversized schema reached serializer", len(raw), err)
			}
		})
	}
	// The original oversized string is caller-owned; rejecting it must not make
	// a new FlatBuffer copy before the output writer gets a chance to reject.
	schema := arrow.NewSchema(nil, &oversizedMetadata)
	allocations := testing.AllocsPerRun(20, func() { _, _ = canonicalSchema(schema) })
	if allocations > 1 {
		t.Fatalf("oversized metadata allocated before rejection: %g allocations", allocations)
	}
}

type schemaTestExtension struct {
	arrow.ExtensionBase
	value string
	calls *int
}

func (*schemaTestExtension) ArrayType() reflect.Type {
	return reflect.TypeOf(struct{ array.ExtensionArrayBase }{})
}
func (*schemaTestExtension) ExtensionName() string { return "kelvo.test-bounded-metadata" }
func (e *schemaTestExtension) ExtensionEquals(other arrow.ExtensionType) bool {
	o, ok := other.(*schemaTestExtension)
	return ok && e.value == o.value
}
func (e *schemaTestExtension) Serialize() string {
	if e.calls != nil {
		*e.calls++
	}
	return e.value
}
func (e *schemaTestExtension) Deserialize(storage arrow.DataType, value string) (arrow.ExtensionType, error) {
	return &schemaTestExtension{ExtensionBase: arrow.ExtensionBase{Storage: storage}, value: value}, nil
}

func TestSchemaPreflightPreservesPracticalNestedAndExtensionTypes(t *testing.T) {
	meta := arrow.NewMetadata([]string{"unit"}, []string{"cents"})
	child := arrow.Field{Name: "amount", Type: &arrow.Decimal128Type{Precision: 18, Scale: 2}, Metadata: meta, Nullable: true}
	ext := &schemaTestExtension{ExtensionBase: arrow.ExtensionBase{Storage: arrow.BinaryTypes.String}, value: "version=1"}
	children := []arrow.Field{child, {Name: "label", Type: arrow.BinaryTypes.String, Nullable: true}}
	for name, dtype := range map[string]arrow.DataType{
		"struct":          arrow.StructOf(children...),
		"list":            arrow.ListOfField(child),
		"large list":      arrow.LargeListOfField(child),
		"fixed list":      arrow.FixedSizeListOfField(3, child),
		"list view":       arrow.ListViewOfField(child),
		"large list view": arrow.LargeListViewOfField(child),
		"map":             arrow.MapOf(arrow.BinaryTypes.String, child.Type),
		"dense union":     arrow.DenseUnionOf(children, []arrow.UnionTypeCode{0, 1}),
		"sparse union":    arrow.SparseUnionOf(children, []arrow.UnionTypeCode{0, 1}),
		"run end encoded": arrow.RunEndEncodedOf(arrow.PrimitiveTypes.Int32, arrow.BinaryTypes.String),
		"dictionary":      &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int16, ValueType: arrow.BinaryTypes.String},
		"extension":       ext,
		"timestamp":       &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "Asia/Kolkata"},
	} {
		t.Run(name, func(t *testing.T) {
			schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: dtype, Metadata: meta, Nullable: true}}, &meta)
			got, err := canonicalSchema(schema)
			if err != nil {
				t.Fatal(err)
			}
			var expected bytes.Buffer
			writer := ipc.NewWriter(&expected, ipc.WithSchema(schema))
			if err = writer.Close(); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, expected.Bytes()) {
				t.Fatal("preflight changed canonical schema bytes")
			}
		})
	}
	calls := 0
	large := &schemaTestExtension{ExtensionBase: arrow.ExtensionBase{Storage: arrow.BinaryTypes.String}, value: strings.Repeat("x", schemaMaxStringBytes+1), calls: &calls}
	if _, err := canonicalSchema(arrow.NewSchema([]arrow.Field{{Name: "e", Type: large}}, nil)); !errors.Is(err, ErrLimit) || calls != 1 {
		t.Fatal("oversized extension reached IPC serializer", calls, err)
	}
}
