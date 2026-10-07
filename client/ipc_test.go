// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package client

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/pierrec/lz4/v4"
)

func decoderConfig() Config {
	return Config{MaxRows: 10000, MaxDecodedBytes: 4 << 20, MaxWireBytes: 4 << 20}
}

func decoderFixture(t *testing.T, batches int, options ...ipc.Option) []byte {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int64}}, nil)
	builder := array.NewInt64Builder(memory.DefaultAllocator)
	defer builder.Release()
	builder.AppendValues(make([]int64, 2048), nil)
	column := builder.NewArray()
	defer column.Release()
	record := array.NewRecordBatch(schema, []arrow.Array{column}, 2048)
	defer record.Release()
	var output bytes.Buffer
	options = append(options, ipc.WithSchema(schema), ipc.WithCompressConcurrency(1))
	writer := ipc.NewWriter(&output, options...)
	for i := 0; i < batches; i++ {
		if err := writer.Write(record); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return output.Bytes()
}

func TestDecoderCompressionRoundTrip(t *testing.T) {
	for name, options := range map[string][]ipc.Option{"plain": nil, "lz4": {ipc.WithLZ4()}, "zstd": {ipc.WithZstd()}} {
		t.Run(name, func(t *testing.T) {
			data := decoderFixture(t, 2, options...)
			var rows int64
			sink := &clientFixtureSink{write: func(record arrow.RecordBatch) error {
				values := record.Column(0).(*array.Int64)
				if values.Value(0) != 0 || values.Value(values.Len()-1) != 0 {
					t.Fatal("decoded values differ")
				}
				rows += record.NumRows()
				return nil
			}}
			stats, err := decodeStream(context.Background(), bytes.NewReader(data), sink, decoderConfig())
			if err != nil || rows != 4096 || stats.Rows != rows || stats.DecodedBytes != 32768 || stats.WireBytes != int64(len(data)) {
				t.Fatalf("decode: %+v rows=%d error=%v", stats, rows, err)
			}
		})
	}
}

func TestDecoderRequiresPhysicalEOSAndHTTPCompletion(t *testing.T) {
	data := clientFixtureIPC(t)
	for name, damaged := range map[string][]byte{
		"missing_eos":      data[:len(data)-8],
		"truncated_eos":    data[:len(data)-1],
		"truncated_schema": data[:20],
		"legacy_eos":       append(append([]byte{}, data[:len(data)-8]...), 0, 0, 0, 0),
		"trailing_byte":    append(append([]byte{}, data...), 0),
		"second_stream":    append(append([]byte{}, data...), data...),
		"eos_only":         data[len(data)-8:],
	} {
		t.Run(name, func(t *testing.T) {
			_, err := decodeStream(context.Background(), bytes.NewReader(damaged), &clientFixtureSink{}, decoderConfig())
			clientFixtureError(t, err, "PROTOCOL_ERROR")
		})
	}
	_, err := decodeStream(context.Background(), io.MultiReader(bytes.NewReader(data), decoderBrokenEOF{}), &clientFixtureSink{}, decoderConfig())
	clientFixtureError(t, err, "PROTOCOL_ERROR")
}

type decoderBrokenEOF struct{}

func (decoderBrokenEOF) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

func TestDecoderEnforcesCumulativeLimits(t *testing.T) {
	data := decoderFixture(t, 2)
	for _, tc := range []struct {
		name                string
		rows, decoded, wire int64
		delivered           int64
	}{
		{"rows", 3000, 4 << 20, 4 << 20, 2048},
		{"decoded", 10000, 24 << 10, 4 << 20, 2048},
		{"wire", 10000, 4 << 20, int64(len(data) - 1), 4096},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := decoderConfig()
			cfg.MaxRows, cfg.MaxDecodedBytes, cfg.MaxWireBytes = tc.rows, tc.decoded, tc.wire
			var delivered int64
			sink := &clientFixtureSink{write: func(batch arrow.RecordBatch) error { delivered += batch.NumRows(); return nil }}
			_, err := decodeStream(context.Background(), bytes.NewReader(data), sink, cfg)
			clientFixtureError(t, err, "RESOURCE_EXHAUSTED")
			if delivered != tc.delivered {
				t.Fatalf("delivered %d rows, want %d", delivered, tc.delivered)
			}
		})
	}
	stats, err := decodeStream(context.Background(), bytes.NewReader(data), &clientFixtureSink{}, Config{MaxRows: 4096, MaxDecodedBytes: 32768, MaxWireBytes: int64(len(data))})
	if err != nil || stats.Rows != 4096 {
		t.Fatalf("exact cumulative limit rejected: %+v %v", stats, err)
	}
}

func decoderMetadata(data []byte, index int) ([]byte, []byte) {
	for i := 0; ; i++ {
		length := int(binary.LittleEndian.Uint32(data[4:8]))
		metadata := data[8 : 8+length]
		c := &fbCheck{b: metadata}
		m := c.table(c.indirect(0), 2, 1, 4, 8, 4)
		body := int(m.uint(3, 8))
		if i == index {
			return metadata, data[8+length : 8+length+body]
		}
		data = data[8+length+body:]
	}
}

func TestDecoderRejectsMetadataAllocationAttacks(t *testing.T) {
	for _, tc := range []struct {
		name, code string
		mutate     func([]byte)
	}{
		{"metadata_length", "RESOURCE_EXHAUSTED", func(data []byte) { binary.LittleEndian.PutUint32(data[4:8], math.MaxUint32) }},
		{"schema_fields", "RESOURCE_EXHAUSTED", func(data []byte) {
			metadata, _ := decoderMetadata(data, 0)
			c := &fbCheck{b: metadata}
			m := c.table(c.indirect(0), 2, 1, 4, 8, 4)
			s := c.table(m.ref(2, true), 2, 4, 4, 4)
			binary.LittleEndian.PutUint32(metadata[s.ref(1, true):], math.MaxUint32)
		}},
		{"body_length", "RESOURCE_EXHAUSTED", func(data []byte) {
			metadata, _ := decoderMetadata(data, 1)
			c := &fbCheck{b: metadata}
			m := c.table(c.indirect(0), 2, 1, 4, 8, 4)
			binary.LittleEndian.PutUint64(metadata[m.slot(3):], 1<<40)
		}},
		{"nodes_vector", "RESOURCE_EXHAUSTED", func(data []byte) {
			metadata, _ := decoderMetadata(data, 1)
			c := &fbCheck{b: metadata}
			m := c.table(c.indirect(0), 2, 1, 4, 8, 4)
			b := c.table(m.ref(2, true), 8, 4, 4, 4, 4)
			binary.LittleEndian.PutUint32(metadata[b.ref(1, true):], math.MaxUint32)
		}},
		{"missing_node", "PROTOCOL_ERROR", func(data []byte) {
			metadata, _ := decoderMetadata(data, 1)
			c := &fbCheck{b: metadata}
			m := c.table(c.indirect(0), 2, 1, 4, 8, 4)
			b := c.table(m.ref(2, true), 8, 4, 4, 4, 4)
			binary.LittleEndian.PutUint32(metadata[b.ref(1, true):], 0)
		}},
		{"missing_buffer", "PROTOCOL_ERROR", func(data []byte) {
			metadata, _ := decoderMetadata(data, 1)
			c := &fbCheck{b: metadata}
			m := c.table(c.indirect(0), 2, 1, 4, 8, 4)
			b := c.table(m.ref(2, true), 8, 4, 4, 4, 4)
			binary.LittleEndian.PutUint32(metadata[b.ref(2, true):], 1)
		}},
		{"buffer_outside_body", "PROTOCOL_ERROR", func(data []byte) {
			metadata, _ := decoderMetadata(data, 1)
			c := &fbCheck{b: metadata}
			m := c.table(c.indirect(0), 2, 1, 4, 8, 4)
			b := c.table(m.ref(2, true), 8, 4, 4, 4, 4)
			p, _ := b.vector(2, 16, maxIPCBuffers)
			binary.LittleEndian.PutUint64(metadata[p:], math.MaxUint64)
		}},
		{"root_offset", "PROTOCOL_ERROR", func(data []byte) { binary.LittleEndian.PutUint32(data[8:], math.MaxUint32) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := decoderFixture(t, 1)
			tc.mutate(data)
			_, err := decodeStream(context.Background(), bytes.NewReader(data), &clientFixtureSink{}, decoderConfig())
			clientFixtureError(t, err, tc.code)
		})
	}
}

func TestDecoderRejectsCompressedExpansionAndCorruption(t *testing.T) {
	for _, tc := range []struct {
		name, code string
		mutate     func([]byte, *ipcBatch)
	}{
		{"expansion", "RESOURCE_EXHAUSTED", func(body []byte, batch *ipcBatch) {
			for _, span := range batch.buffers {
				if span[1] > 8 {
					binary.LittleEndian.PutUint64(body[span[0]:], 1<<40)
					return
				}
			}
		}},
		{"short_expansion", "PROTOCOL_ERROR", func(body []byte, batch *ipcBatch) {
			for _, span := range batch.buffers {
				if span[1] > 8 {
					binary.LittleEndian.PutUint64(body[span[0]:], 1)
					return
				}
			}
		}},
		{"invalid_frame", "PROTOCOL_ERROR", func(body []byte, batch *ipcBatch) {
			for _, span := range batch.buffers {
				if span[1] > 8 {
					body[span[0]+8] ^= 0xff
					return
				}
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := decoderFixture(t, 1, ipc.WithLZ4())
			metadata, body := decoderMetadata(data, 1)
			_, batch := checkIPCMetadata(metadata, decoderConfig())
			if !batch.compressed {
				t.Fatal("fixture did not compress")
			}
			tc.mutate(body, batch)
			_, err := decodeStream(context.Background(), bytes.NewReader(data), &clientFixtureSink{}, decoderConfig())
			clientFixtureError(t, err, tc.code)
		})
	}
}

func TestDecoderBoundsNestedSchemaAndRejectsExtensions(t *testing.T) {
	for _, tc := range []struct {
		name   string
		schema *arrow.Schema
		code   string
	}{
		{"too_many_fields", func() *arrow.Schema {
			fields := make([]arrow.Field, maxIPCFields+1)
			for i := range fields {
				fields[i] = arrow.Field{Name: "value", Type: arrow.PrimitiveTypes.Int64}
			}
			return arrow.NewSchema(fields, nil)
		}(), "RESOURCE_EXHAUSTED"},
		{"too_deep", func() *arrow.Schema {
			var typ arrow.DataType = arrow.PrimitiveTypes.Int64
			for i := 0; i < maxIPCDepth; i++ {
				typ = arrow.ListOf(typ)
			}
			return arrow.NewSchema([]arrow.Field{{Name: "value", Type: typ}}, nil)
		}(), "RESOURCE_EXHAUSTED"},
		{"extension_deserializer", arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int64,
			Metadata: arrow.NewMetadata([]string{"ARROW:extension:name"}, []string{"untrusted.extension"})}}, nil), "PROTOCOL_ERROR"},
		{"unsupported_binary_view", arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.BinaryTypes.StringView}}, nil), "PROTOCOL_ERROR"},
		{"decimal_scale", arrow.NewSchema([]arrow.Field{{Name: "value", Type: &arrow.Decimal128Type{Precision: 38, Scale: math.MaxInt32}}}, nil), "PROTOCOL_ERROR"},
		{"decimal_precision", arrow.NewSchema([]arrow.Field{{Name: "value", Type: &arrow.Decimal128Type{Precision: math.MaxInt32, Scale: 0}}}, nil), "PROTOCOL_ERROR"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var data bytes.Buffer
			writer := ipc.NewWriter(&data, ipc.WithSchema(tc.schema))
			if err := writer.Close(); err != nil {
				t.Fatal(err)
			}
			_, err := decodeStream(context.Background(), bytes.NewReader(data.Bytes()), &clientFixtureSink{}, decoderConfig())
			clientFixtureError(t, err, tc.code)
		})
	}
}

func TestDecoderRejectsUnexpectedVariadicCounts(t *testing.T) {
	for _, tc := range []struct {
		name, code string
		value      int64
	}{
		{"extra", "PROTOCOL_ERROR", 0},
		{"allocation", "RESOURCE_EXHAUSTED", 1 << 60},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := decoderFixture(t, 1)
			metadata, body := decoderMetadata(data, 1)
			_, batch := checkIPCMetadata(metadata, decoderConfig())
			batch.variadic = []int64{tc.value}
			metadata = uncompressedMetadata(batch, int64(len(body)))
			for len(metadata)%8 != 0 {
				metadata = append(metadata, 0)
			}
			schemaEnd := 8 + int(binary.LittleEndian.Uint32(data[4:8]))
			var frame [8]byte
			binary.LittleEndian.PutUint32(frame[:], math.MaxUint32)
			binary.LittleEndian.PutUint32(frame[4:], uint32(len(metadata)))
			modified := append(append([]byte{}, data[:schemaEnd]...), frame[:]...)
			modified = append(modified, metadata...)
			modified = append(modified, body...)
			modified = append(modified, data[len(data)-8:]...)
			_, err := decodeStream(context.Background(), bytes.NewReader(modified), &clientFixtureSink{}, decoderConfig())
			clientFixtureError(t, err, tc.code)
		})
	}
}

func TestDecoderDictionariesAndNestedLists(t *testing.T) {
	labels := array.NewStringBuilder(memory.DefaultAllocator)
	defer labels.Release()
	labels.AppendValues([]string{"north", "south"}, nil)
	labelArray := labels.NewArray()
	defer labelArray.Release()
	indices := array.NewInt8Builder(memory.DefaultAllocator)
	defer indices.Release()
	indices.AppendValues([]int8{0, 1}, nil)
	indexArray := indices.NewArray()
	defer indexArray.Release()
	dictType := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.BinaryTypes.String}
	dictionary := array.NewDictionaryArray(dictType, indexArray, labelArray)
	defer dictionary.Release()
	lists := array.NewListBuilder(memory.DefaultAllocator, arrow.PrimitiveTypes.Int64)
	defer lists.Release()
	lists.Append(true)
	lists.ValueBuilder().(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	lists.AppendNull()
	listArray := lists.NewArray()
	defer listArray.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "region", Type: dictType}, {Name: "values", Type: listArray.DataType(), Nullable: true}}, nil)
	record := array.NewRecordBatch(schema, []arrow.Array{dictionary, listArray}, 2)
	defer record.Release()
	var data bytes.Buffer
	writer := ipc.NewWriter(&data, ipc.WithSchema(schema), ipc.WithLZ4(), ipc.WithCompressConcurrency(1))
	if err := writer.Write(record); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	stats, err := decodeStream(context.Background(), bytes.NewReader(data.Bytes()), &clientFixtureSink{write: func(batch arrow.RecordBatch) error {
		if !array.RecordEqual(record, batch) {
			t.Fatal("nested or dictionary result changed")
		}
		return nil
	}}, decoderConfig())
	if err != nil || stats.Rows != 2 || stats.DecodedBytes < 45 {
		t.Fatalf("nested decode: %+v %v", stats, err)
	}
}

func TestDecoderRedactsSinkFailuresAndPanics(t *testing.T) {
	for name, fail := range map[string]func() error{
		"error": func() error { return errors.New("remote-secret-diagnostic") },
		"panic": func() error { panic("remote-secret-diagnostic") },
	} {
		t.Run(name, func(t *testing.T) {
			data := clientFixtureIPC(t)
			_, err := decodeStream(context.Background(), bytes.NewReader(data), &clientFixtureSink{write: func(arrow.RecordBatch) error { return fail() }}, decoderConfig())
			clientFixtureError(t, err, "SINK_FAILED")
			if strings.Contains(err.Error(), "remote-secret") {
				t.Fatal("sink diagnostics escaped")
			}
		})
	}
}

func TestLZ4WorkspacePreflightRejectsConcatenatedFrames(t *testing.T) {
	compress := func(blockSize lz4.BlockSize) []byte {
		var encoded bytes.Buffer
		writer := lz4.NewWriter(&encoded)
		if err := writer.Apply(lz4.BlockSizeOption(blockSize)); err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Write([]byte("test")); err != nil {
			t.Fatal(err)
		}
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}
		return encoded.Bytes()
	}
	first := compress(lz4.Block64Kb)
	if !validLZ4Frame(first, 4) {
		t.Fatal("valid 64 KiB frame rejected")
	}
	for name, invalid := range map[string][]byte{
		"oversized_workspace":    compress(lz4.Block4Mb),
		"second_oversized_frame": append(append([]byte{}, first...), compress(lz4.Block4Mb)...),
		"second_normal_frame":    append(append([]byte{}, first...), first...),
		"truncated_trailer":      first[:len(first)-1],
	} {
		t.Run(name, func(t *testing.T) {
			if validLZ4Frame(invalid, 4) {
				t.Fatal("invalid or additional frame accepted")
			}
		})
	}
}
