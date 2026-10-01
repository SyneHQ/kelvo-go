// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package arrowipc

import (
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"math"
	"strings"
	"testing"
)

func schemaMetadata(t *testing.T, schema *arrow.Schema) []byte {
	t.Helper()
	var out bytes.Buffer
	w := ipc.NewWriter(&out, ipc.WithSchema(schema))
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	data := out.Bytes()
	if binary.LittleEndian.Uint32(data) != math.MaxUint32 {
		t.Fatal("fixture does not use modern framing")
	}
	n := int(binary.LittleEndian.Uint32(data[4:]))
	return bytes.Clone(data[8 : 8+n])
}

func TestMetadataRejectsForgedSchemaCountsAndAliasedText(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "a", Type: arrow.PrimitiveTypes.Int64}}, nil)
	meta := schemaMetadata(t, schema)
	v := metadataCheck{data: meta}
	message := v.table(v.pointer(0))
	header := v.table(v.reference(message, 2, true))
	fields := v.reference(header, 1, true)
	binary.LittleEndian.PutUint32(meta[fields:], math.MaxUint32)
	if _, err := ValidateMessageMetadata(meta); !errors.Is(err, ErrLimit) {
		t.Fatalf("forged schema count not rejected: %v", err)
	}

	schema = arrow.NewSchema([]arrow.Field{{Name: strings.Repeat("x", maxIPCMetadata*3/4), Type: arrow.PrimitiveTypes.Int64}, {Name: "b", Type: arrow.PrimitiveTypes.Int64}}, nil)
	meta = schemaMetadata(t, schema)
	v = metadataCheck{data: meta}
	message = v.table(v.pointer(0))
	header = v.table(v.reference(message, 2, true))
	position, count := v.vector(header, 1, 4, maxIPCFields)
	if count != 2 {
		t.Fatal("fixture field count")
	}
	first := v.pointer(position)
	if first <= position+4 {
		t.Fatal("fixture cannot alias a forward field")
	}
	binary.LittleEndian.PutUint32(meta[position+4:], uint32(first-(position+4)))
	if _, err := ValidateMessageMetadata(meta); !errors.Is(err, ErrLimit) {
		t.Fatalf("metadata text amplification accepted: %v", err)
	}
}

func TestMetadataLimitsNestedSchemas(t *testing.T) {
	for _, depth := range []int{8, maxIPCDepth + 1} {
		var typ arrow.DataType = arrow.PrimitiveTypes.Int64
		for i := 1; i < depth; i++ {
			typ = arrow.ListOf(typ)
		}
		meta := schemaMetadata(t, arrow.NewSchema([]arrow.Field{{Name: "nested", Type: typ}}, nil))
		_, err := ValidateMessageMetadata(meta)
		if depth < maxIPCDepth && err != nil {
			t.Fatal(err)
		}
		if depth > maxIPCDepth && !errors.Is(err, ErrLimit) {
			t.Fatalf("deep schema accepted: %v", err)
		}
	}
}
