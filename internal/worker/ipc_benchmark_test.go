// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Each operation reads one complete uncompressed child stream, validates it,
// and re-encodes it through the production IPCSink. MB/s measures decoded Arrow
// bytes reported by readWorkerIPC, not network throughput or database scans.
//
// Run on Linux, with the toolchain/dependencies pinned by go.mod:
//
//	go test ./internal/worker -run '^TestWorkerIPCTransit' -count=1
//	go test ./internal/worker -run '^$' -bench '^BenchmarkWorkerIPCTransit$' -benchmem -benchtime=1s -count=3
//
// Keep CPU settings identical when comparing runs. Fixture creation, child
// encoding and exact result validation happen before timing. The timed result
// writer is io.Discard: source execution, pipes, TLS, gateway forwarding and
// network backpressure are deliberately outside this microbenchmark.
func BenchmarkWorkerIPCTransit(b *testing.B) {
	for _, shape := range ipcTransitShapes {
		for _, batchRows := range []int{1024, 8192, 32768} {
			for _, compression := range []string{"none", "lz4_frame"} {
				name := fmt.Sprintf("%s/batch=%d/compression=%s", shape.name, batchRows, compression)
				b.Run(name, func(b *testing.B) {
					b.StopTimer()
					fixture := newIPCTransitFixture(b, shape, batchRows, ipcTransitRows)
					defer fixture.release()
					limits := ipcTransitLimits(compression)
					want, encodedBytes := verifyIPCTransitFixture(b, fixture, limits)
					// Do not retain the original record buffers during measurement.
					// Only the bounded, pre-encoded child stream remains resident.
					fixture.release()
					b.SetBytes(want.Bytes)
					b.ReportAllocs()
					b.ResetTimer()
					b.StartTimer()
					for i := 0; i < b.N; i++ {
						got, size, err := runIPCTransit(fixture.child, io.Discard, limits)
						if err != nil {
							b.Fatal(err)
						}
						if got.Rows != want.Rows || got.Batches != want.Batches || got.Bytes != want.Bytes || got.WireBytes != want.WireBytes || size != encodedBytes {
							b.Fatal("transit byte, row or batch accounting changed")
						}
					}
					b.StopTimer()
					b.ReportMetric(float64(want.Bytes), "decoded-B/op")
					b.ReportMetric(float64(want.WireBytes), "child-IPC-B/op")
					b.ReportMetric(float64(encodedBytes), "result-IPC-B/op")
					b.ReportMetric(float64(want.Batches), "batches/op")
					b.ReportMetric(float64(want.Rows)*float64(b.N)/b.Elapsed().Seconds(), "rows/s")
				})
			}
		}
	}
}

// Include a short final batch in every case, including the largest batch size.
// The wide fixture has less than 34 MiB of payload; no case is a full-engine
// memory limit or concurrency benchmark.
const ipcTransitRows = 32768 + 17

type ipcTransitShape struct {
	name         string
	payloadBytes int
	highEntropy  bool
}

var ipcTransitShapes = []ipcTransitShape{
	{name: "narrow/repeated", payloadBytes: 16},
	{name: "narrow/high_entropy", payloadBytes: 16, highEntropy: true},
	{name: "wide/repeated", payloadBytes: 1024},
	{name: "wide/high_entropy", payloadBytes: 1024, highEntropy: true},
}

type ipcTransitFixture struct {
	child   []byte
	records []arrow.RecordBatch
	rows    int64
}

func (f *ipcTransitFixture) release() {
	for _, record := range f.records {
		record.Release()
	}
	f.records = nil
}

func ipcTransitLimits(compression string) query.Limits {
	limits := query.DefaultLimits()
	limits.MaxRows = ipcTransitRows
	limits.MaxBytes = 96 << 20
	limits.MemoryMB = 128
	limits.ResultCompression = compression
	return limits
}

func newIPCTransitFixture(tb testing.TB, shape ipcTransitShape, batchRows, rows int) *ipcTransitFixture {
	tb.Helper()
	fixture := &ipcTransitFixture{rows: int64(rows)}
	tb.Cleanup(fixture.release)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "row_id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "value", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "payload", Type: arrow.BinaryTypes.Binary, Nullable: true},
	}, nil)
	// One seed for the whole stream gives identical values across batch sizes.
	// High entropy applies to value/payload; row IDs remain ordered and NULLs
	// remain periodic. It is not a guarantee of an incompressible IPC stream.
	random := rand.New(rand.NewSource(42))
	payload := bytes.Repeat([]byte{'a'}, shape.payloadBytes)
	var child bytes.Buffer
	writer := ipc.NewWriter(&child, ipc.WithSchema(schema))
	defer writer.Close()
	for start := 0; start < rows; start += batchRows {
		count := min(batchRows, rows-start)
		builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
		builder.Reserve(count)
		ids := builder.Field(0).(*array.Int64Builder)
		values := builder.Field(1).(*array.Int64Builder)
		payloads := builder.Field(2).(*array.BinaryBuilder)
		for row := start; row < start+count; row++ {
			ids.Append(int64(row))
			value := int64(row%7 - 3)
			if shape.highEntropy {
				value = int64(random.Uint64())
				// math/rand.Read always returns len(payload), nil. This fixture
				// is deterministic benchmark data, never key material.
				_, _ = random.Read(payload)
			}
			if row%17 == 0 {
				values.AppendNull()
			} else {
				values.Append(value)
			}
			if row%31 == 0 {
				payloads.AppendNull()
			} else {
				payloads.Append(payload)
			}
		}
		record := builder.NewRecordBatch()
		builder.Release()
		fixture.records = append(fixture.records, record)
		if err := writer.Write(record); err != nil {
			tb.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		tb.Fatal(err)
	}
	fixture.child = child.Bytes()
	return fixture
}

// Finish is deliberately conditional on the complete validated child stream.
// This exercises local IPC success semantics, not the cluster's separate durable
// lifecycle commit and EOS gate.
func runIPCTransit(child []byte, output io.Writer, limits query.Limits) (query.Stats, int64, error) {
	sink := NewIPCSink(output, limits)
	defer sink.Abort()
	stats, err := readWorkerIPC(context.Background(), bytes.NewReader(child), limits, sink)
	if err == nil {
		err = sink.Finish()
	}
	return stats, sink.EncodedBytes(), err
}

func verifyIPCTransitFixture(tb testing.TB, fixture *ipcTransitFixture, limits query.Limits) (query.Stats, int64) {
	tb.Helper()
	var output bytes.Buffer
	stats, encodedBytes, err := runIPCTransit(fixture.child, &output, limits)
	if err != nil {
		tb.Fatal(err)
	}
	if stats.Rows != fixture.rows || stats.Batches != int64(len(fixture.records)) || stats.Bytes <= 0 || stats.WireBytes != int64(len(fixture.child)) {
		tb.Fatalf("incorrect validated child accounting: %+v", stats)
	}
	if encodedBytes != int64(output.Len()) || !bytes.HasSuffix(output.Bytes(), []byte{255, 255, 255, 255, 0, 0, 0, 0}) {
		tb.Fatal("incorrect encoded accounting or missing success EOS")
	}
	allocator := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer allocator.AssertSize(tb, 0)
	reader, err := ipc.NewReader(bytes.NewReader(output.Bytes()), ipc.WithAllocator(allocator))
	if err != nil {
		tb.Fatal(err)
	}
	defer reader.Release()
	if !reader.Schema().Equal(fixture.records[0].Schema()) {
		tb.Fatal("schema changed during transit")
	}
	batch := 0
	for reader.Next() {
		if batch >= len(fixture.records) {
			tb.Fatal("unexpected output batch")
		}
		got, want := reader.RecordBatch(), fixture.records[batch]
		if got.NumRows() != want.NumRows() || got.NumCols() != want.NumCols() {
			tb.Fatal("output batch shape changed")
		}
		for column := 0; column < int(want.NumCols()); column++ {
			if !array.Equal(got.Column(column), want.Column(column)) {
				tb.Fatalf("batch %d column %d changed values or NULLs", batch, column)
			}
		}
		batch++
	}
	if reader.Err() != nil || batch != len(fixture.records) {
		tb.Fatalf("incomplete output: batches=%d error=%v", batch, reader.Err())
	}
	return stats, encodedBytes
}

func TestWorkerIPCTransitFixtures(t *testing.T) {
	for _, shape := range ipcTransitShapes {
		for _, batchRows := range []int{1024, 8192, 32768} {
			t.Run(fmt.Sprintf("%s/batch=%d", shape.name, batchRows), func(t *testing.T) {
				fixture := newIPCTransitFixture(t, shape, batchRows, ipcTransitRows)
				defer fixture.release()
				for _, compression := range []string{"none", "lz4_frame"} {
					t.Run(compression, func(t *testing.T) {
						verifyIPCTransitFixture(t, fixture, ipcTransitLimits(compression))
					})
				}
			})
		}
	}
}

func TestWorkerIPCTransitAbortsFailedChild(t *testing.T) {
	fixture := newIPCTransitFixture(t, ipcTransitShapes[0], 1024, 2048)
	defer fixture.release()
	for _, compression := range []string{"none", "lz4_frame"} {
		for name, invalid := range map[string][]byte{
			"missing_eos": fixture.child[:len(fixture.child)-8],
			"after_eos":   append(bytes.Clone(fixture.child), 1),
		} {
			t.Run(compression+"/"+name, func(t *testing.T) {
				var output bytes.Buffer
				_, encodedBytes, err := runIPCTransit(invalid, &output, ipcTransitLimits(compression))
				if err == nil {
					t.Fatal("invalid child became a successful result")
				}
				if encodedBytes != int64(output.Len()) || bytes.HasSuffix(output.Bytes(), []byte{255, 255, 255, 255, 0, 0, 0, 0}) {
					t.Fatal("failed result emitted EOS or counted discarded cleanup bytes")
				}
			})
		}
	}
}
