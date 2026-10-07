// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/telemetry"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	arrowutil "github.com/apache/arrow-go/v18/arrow/util"
)

func TestWorkerIPCAttributionPreservesDelivery(t *testing.T) {
	data := ipcFixture(t)
	limits := query.DefaultLimits()
	limits.MaxBytes = int64(len(data))
	baseline, err := readWorkerIPC(context.Background(), bytes.NewReader(data), limits, &workerTestSink{})
	if err != nil {
		t.Fatal(err)
	}
	input := &fragmentedIPC{Reader: bytes.NewReader(data)}
	sink := &workerTestSink{}
	sink.write = func(batch arrow.RecordBatch) error {
		if input.bytes >= len(data) {
			t.Fatal("attribution added prefetch before synchronous delivery")
		}
		values := batch.Column(0).(*array.Int64)
		if !values.IsNull(7) || values.Value(8) != int64(sink.batches*256+8) {
			t.Fatal("attribution changed values or NULLs")
		}
		return nil
	}
	var observation telemetry.IPCTransfer
	observed, err := readWorkerIPCObserved(context.Background(), input, limits, sink, &observation)
	if err != nil {
		t.Fatal(err)
	}
	if observed.Rows != baseline.Rows || observed.Bytes != baseline.Bytes || observed.Batches != baseline.Batches || observed.WireBytes != baseline.WireBytes {
		t.Fatalf("diagnostics changed result statistics: got %+v, baseline %+v", observed, baseline)
	}
	if !observation.Valid() || !observation.Complete || !observation.HasSink || observation.InputBytes != int64(len(data)) || observation.DecodedBytes != observed.Bytes || observation.Batches != observed.Batches {
		t.Fatalf("incorrect complete observation: %+v", observation)
	}
}

func TestWorkerIPCAttributionFailedSinkIsIncomplete(t *testing.T) {
	data := ipcFixture(t)
	rejected := errors.New("sink rejected the batch")
	var offeredBytes int64
	var calls int
	sink := &workerTestSink{write: func(batch arrow.RecordBatch) error {
		offeredBytes += arrowutil.TotalRecordSize(batch)
		calls++
		if calls == 2 {
			return rejected
		}
		return nil
	}}
	var observation telemetry.IPCTransfer
	observed, err := readWorkerIPCObserved(context.Background(), bytes.NewReader(data), query.DefaultLimits(), sink, &observation)
	if !errors.Is(err, rejected) || observed.Batches != 1 || observed.Rows != 256 || observed.WireBytes != 0 {
		t.Fatalf("failed sink changed outcome/statistics: %+v, %v", observed, err)
	}
	if !observation.Valid() || observation.Complete || !observation.HasSink || observation.Batches != 2 || observation.DecodedBytes != offeredBytes || observation.InputBytes >= int64(len(data)) {
		t.Fatalf("failed final callback not counted as incomplete offered work: %+v", observation)
	}
	metrics := telemetry.New()
	metrics.ObserveIPCTransfer(telemetry.KindQuery, &observation)
	snapshot := metrics.Snapshot().IPCTransfer
	if snapshot.Calls[telemetry.KindQuery][telemetry.IPCTransferComplete] != 0 || snapshot.DecodedBytes[telemetry.KindQuery][telemetry.IPCTransferComplete] != 0 || snapshot.Batches[telemetry.KindQuery][telemetry.IPCTransferIncomplete] != 2 {
		t.Fatal("failed sink contributed complete counters")
	}
}

func TestWorkerIPCAttributionIncludesBoundedTrailingProbe(t *testing.T) {
	data := ipcFixture(t)
	input := append(bytes.Clone(data), []byte("PRIVATE_TRAILING_BYTES")...)
	limits := query.DefaultLimits()
	limits.MaxBytes = int64(len(data))
	var observation telemetry.IPCTransfer
	observed, err := readWorkerIPCObserved(context.Background(), bytes.NewReader(input), limits, &workerTestSink{}, &observation)
	if err == nil || query.PublicError(err).Code != "RESOURCE_EXHAUSTED" || strings.Contains(err.Error(), "PRIVATE_TRAILING_BYTES") {
		t.Fatalf("trailing bytes changed error semantics: %v", err)
	}
	if observed.WireBytes != 0 || !observation.Valid() || observation.Complete || observation.InputBytes != int64(len(data)+1) || observation.Batches != 3 {
		t.Fatalf("probe counted beyond one byte or fabricated completion: %+v", observation)
	}
}

func TestWorkerIPCAttributionUnknownSinkAndLimits(t *testing.T) {
	for _, tc := range []struct {
		name string
		data []byte
	}{
		{"empty", nil},
		{"truncated prefix", []byte{255, 255}},
		{"oversized metadata", []byte{255, 255, 255, 255, 255, 255, 255, 127}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var observation telemetry.IPCTransfer
			_, err := readWorkerIPCObserved(context.Background(), bytes.NewReader(tc.data), query.DefaultLimits(), &workerTestSink{}, &observation)
			if err == nil || !observation.Valid() || observation.Complete || observation.HasSink || observation.SinkDuration != 0 || observation.DecodedBytes != 0 || observation.Batches != 0 || observation.InputBytes != int64(len(tc.data)) {
				t.Fatalf("unreached sink received an observation: %+v, %v", observation, err)
			}
		})
	}
	limits := query.DefaultLimits()
	limits.MaxRows = 1
	var observation telemetry.IPCTransfer
	observed, err := readWorkerIPCObserved(context.Background(), bytes.NewReader(ipcFixture(t)), limits, &workerTestSink{}, &observation)
	if query.PublicError(err).Code != "RESOURCE_EXHAUSTED" || observed.Rows != 0 || !observation.Valid() || observation.Complete || !observation.HasSink || observation.DecodedBytes != 0 || observation.Batches != 0 {
		t.Fatalf("row-limit failure counted an unvalidated batch: %+v, %v", observation, err)
	}
}

type ipcAttributionSchemaFailure struct{ panic bool }

func (s ipcAttributionSchemaFailure) Schema(*arrow.Schema) error {
	if s.panic {
		panic("PRIVATE_PANIC_DIAGNOSTIC")
	}
	return context.Canceled
}

func (ipcAttributionSchemaFailure) Write(arrow.RecordBatch) error { return nil }

func TestWorkerIPCAttributionFinalizesOnCancellationAndPanic(t *testing.T) {
	for _, shouldPanic := range []bool{false, true} {
		var observation telemetry.IPCTransfer
		_, err := readWorkerIPCObserved(context.Background(), bytes.NewReader(ipcFixture(t)), query.DefaultLimits(), ipcAttributionSchemaFailure{panic: shouldPanic}, &observation)
		if err == nil || strings.Contains(err.Error(), "PRIVATE_PANIC_DIAGNOSTIC") || (!shouldPanic && !errors.Is(err, context.Canceled)) {
			t.Fatalf("callback outcome changed: %v", err)
		}
		if !observation.Valid() || observation.Complete || !observation.HasSink || observation.Batches != 0 || observation.DecodedBytes != 0 {
			t.Fatalf("terminal callback lost diagnostic attribution: %+v", observation)
		}
	}
}

// This in-memory microbenchmark isolates the extra parent clocks and aggregate
// recording. It is not source, TLS, client-delivery or cluster throughput proof.
func BenchmarkWorkerIPCAttribution(b *testing.B) {
	data := ipcAttributionBenchmarkFixture(b)
	for _, mode := range []string{"discard", "none", "lz4_frame"} {
		b.Run(mode, func(b *testing.B) {
			for _, enabled := range []bool{false, true} {
				name := "disabled"
				if enabled {
					name = "enabled"
				}
				b.Run(name, func(b *testing.B) {
					limits := query.DefaultLimits()
					var metrics *telemetry.Registry
					if enabled {
						metrics = telemetry.New()
					}
					if mode != "discard" {
						limits.ResultCompression = mode
					}
					b.ReportAllocs()
					b.SetBytes(int64(len(data)))
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						var observed *telemetry.IPCTransfer
						if enabled {
							observed = new(telemetry.IPCTransfer)
						}
						var sink query.Sink = &workerTestSink{}
						var encoded *IPCSink
						if mode != "discard" {
							encoded = NewIPCSink(io.Discard, limits)
							sink = encoded
						}
						_, err := readWorkerIPCObserved(context.Background(), bytes.NewReader(data), limits, sink, observed)
						if err != nil {
							b.Fatal(err)
						}
						metrics.ObserveIPCTransfer(telemetry.KindQuery, observed)
						if encoded != nil {
							if err := encoded.Finish(); err != nil {
								b.Fatal(err)
							}
						}
					}
				})
			}
		})
	}
}

func ipcAttributionBenchmarkFixture(tb testing.TB) []byte {
	tb.Helper()
	schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)
	var output bytes.Buffer
	writer := ipc.NewWriter(&output, ipc.WithSchema(schema))
	builder := array.NewInt64Builder(memory.DefaultAllocator)
	for i := 0; i < 1024; i++ {
		if i%7 == 0 {
			builder.AppendNull()
		} else {
			builder.Append(int64(i))
		}
	}
	column := builder.NewArray()
	builder.Release()
	batch := array.NewRecordBatch(schema, []arrow.Array{column}, 1024)
	column.Release()
	defer batch.Release()
	for i := 0; i < 32; i++ {
		if err := writer.Write(batch); err != nil {
			tb.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		tb.Fatal(err)
	}
	return output.Bytes()
}
