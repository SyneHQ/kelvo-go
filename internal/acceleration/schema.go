// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
)

const maxSchemaBytes = 1 << 20
const maxSchemaFooterBytes = 64 << 20

var ErrSchemaMismatch = errors.New("accelerated dataset schema changed")

// SchemaWriter is implemented by stores that preserve cross-generation schema
// contracts while holding their existing exclusive writer lock or lease.
type SchemaWriter interface {
	PreviousSchema() (*arrow.Schema, error)
	SetSchema(*arrow.Schema) error
}

// SchemaEqual preserves column order, exact types, nullability and all metadata.
// No implicit casts, added columns or nullable widening are accepted.
func SchemaEqual(a, b *arrow.Schema) bool {
	if a == nil || b == nil {
		return a == b
	}
	if !a.Equal(b) || !a.Metadata().Equal(b.Metadata()) {
		return false
	}
	for i, f := range a.Fields() {
		if !f.Metadata.Equal(b.Field(i).Metadata) {
			return false
		}
	}
	return true
}

// SchemaFingerprint hashes the exact Arrow IPC schema representation. This
// verifies stored contracts; compatibility uses SchemaEqual rather than a hash
// comparison, so metadata key ordering alone does not change compatibility.
func SchemaFingerprint(schema *arrow.Schema) (string, error) {
	if schema == nil {
		return "", errors.New("missing Arrow schema")
	}
	var out bytes.Buffer
	bounded := &parquetCountingWriter{out: &out, limit: maxSchemaBytes}
	writer := ipc.NewWriter(bounded, ipc.WithSchema(schema))
	if err := writer.Close(); err != nil {
		return "", err
	}
	if bounded.err != nil {
		return "", bounded.err
	}
	sum := sha256.Sum256(out.Bytes())
	return hex.EncodeToString(sum[:]), nil
}

// ReadParquetSchema reads original Arrow metadata, not Parquet's normalized
// schema (which can lose widths/time units). Only bounded footer metadata is
// decoded, and a section reader preserves the caller's file offset/lifetime.
func ReadParquetSchema(input io.ReaderAt, size int64) (schema *arrow.Schema, err error) {
	if input == nil || size < 12 {
		return nil, fmt.Errorf("%w: invalid Parquet size", ErrCorrupt)
	}
	var trailer [8]byte
	if _, err = input.ReadAt(trailer[:], size-8); err != nil {
		return nil, fmt.Errorf("%w: unreadable Parquet footer", ErrCorrupt)
	}
	footerSize := int64(binary.LittleEndian.Uint32(trailer[:4]))
	if string(trailer[4:]) != "PAR1" || footerSize <= 0 || footerSize > maxSchemaFooterBytes || footerSize > size-12 {
		return nil, fmt.Errorf("%w: invalid or oversized Parquet footer", ErrCorrupt)
	}
	// Malformed persisted FlatBuffers/Thrift must fail a refresh, not the service.
	defer func() {
		if recover() != nil {
			schema = nil
			err = fmt.Errorf("%w: malformed Arrow schema metadata", ErrCorrupt)
		}
	}()
	reader, err := file.NewParquetReader(io.NewSectionReader(input, 0, size))
	if err != nil {
		return nil, fmt.Errorf("%w: unreadable Parquet metadata", ErrCorrupt)
	}
	defer reader.Close()
	encoded := reader.MetaData().KeyValueMetadata().FindValue("ARROW:schema")
	if encoded == nil || len(*encoded) > base64.StdEncoding.EncodedLen(maxSchemaBytes) {
		return nil, fmt.Errorf("%w: missing or oversized original Arrow schema", ErrCorrupt)
	}
	data, err := base64.StdEncoding.DecodeString(*encoded)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid Arrow schema encoding", ErrCorrupt)
	}
	schema, err = flight.DeserializeSchema(data, memory.DefaultAllocator)
	if err != nil || schema == nil {
		return nil, fmt.Errorf("%w: invalid Arrow schema", ErrCorrupt)
	}
	if _, err = SchemaFingerprint(schema); err != nil {
		return nil, err
	}
	return schema, nil
}
