// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/telemetry"
	"github.com/SYNEHQ/kelvo-go/internal/worker"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

var childEOS = []byte{255, 255, 255, 255, 0, 0, 0, 0}

type childExecutorFixture struct {
	execute func(context.Context, query.Request, query.Sink) (query.Stats, error)
	close   func() error
}

func (e *childExecutorFixture) Execute(ctx context.Context, request query.Request, sink query.Sink) (query.Stats, error) {
	return e.execute(ctx, request, sink)
}
func (e *childExecutorFixture) Close() error {
	if e.close != nil {
		return e.close()
	}
	return nil
}

type childDiagnosticBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *childDiagnosticBuffer) Write(value []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(value)
}
func (b *childDiagnosticBuffer) snapshot() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return bytes.Clone(b.b.Bytes())
}

func childFixtureInput() worker.Input {
	return worker.Input{TimingVersion: telemetry.ChildTimingVersion, Limits: query.DefaultLimits(), Request: query.Request{Mode: "federated", SQL: "SELECT 1"}}
}

func childFixtureJSON(t *testing.T, in worker.Input) *bytes.Reader {
	t.Helper()
	raw, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	return bytes.NewReader(raw)
}

func childFixtureOutcome(t *testing.T, raw []byte) worker.Outcome {
	t.Helper()
	var outcome worker.Outcome
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if err := decoder.Decode(&outcome); err != nil {
		t.Fatal(err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		t.Fatal("worker did not emit exactly one outcome", err)
	}
	return outcome
}

func childFixtureRows(allocator memory.Allocator, sink query.Sink) (query.Stats, error) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "amount", Type: &arrow.Decimal128Type{Precision: 20, Scale: 4}, Nullable: true},
		{Name: "at", Type: &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}, Nullable: true},
		{Name: "note", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	if err := sink.Schema(schema); err != nil {
		return query.Stats{}, err
	}
	builder := array.NewRecordBuilder(allocator, schema)
	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{9007199254740993, math.MaxInt64}, nil)
	builder.Field(1).(*array.Decimal128Builder).Append(decimal128.FromI64(-1234567890123456789))
	builder.Field(1).AppendNull()
	builder.Field(2).(*array.TimestampBuilder).Append(arrow.Timestamp(1700000000123456789))
	builder.Field(2).AppendNull()
	builder.Field(3).(*array.StringBuilder).Append("value")
	builder.Field(3).AppendNull()
	record := builder.NewRecordBatch()
	builder.Release()
	defer record.Release()
	err := sink.Write(record)
	return query.Stats{Rows: 2, Batches: 1, Backend: "fixture", DurationNS: 7}, err
}

func TestChildWorkerTimingWaitsForNativeCleanup(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	allocator := memory.NewCheckedAllocator(memory.DefaultAllocator)
	var executionContext context.Context
	var recorder *telemetry.ChildRecorder
	var output bytes.Buffer
	diagnostics := &childDiagnosticBuffer{}
	executor := &childExecutorFixture{
		execute: func(ctx context.Context, _ query.Request, sink query.Sink) (query.Stats, error) {
			executionContext, recorder = ctx, telemetry.ChildRecorderFromContext(ctx)
			return childFixtureRows(allocator, sink)
		},
		close: func() error { close(entered); <-release; return errors.New("ignored close fixture") },
	}
	factory := func(worker.Input) (query.Executor, io.Closer, error) { return executor, executor, nil }
	done := make(chan error, 1)
	exited := make(chan struct{})
	input := childFixtureJSON(t, childFixtureInput())
	started := time.Now()
	go func() {
		defer close(exited)
		done <- runWorkerIO(context.Background(), input, &output, diagnostics, factory)
	}()
	defer func() {
		unblock()
		select {
		case <-exited:
		case <-time.After(3 * time.Second):
			t.Error("child cleanup goroutine did not join")
		}
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not enter native cleanup")
	}
	if len(diagnostics.snapshot()) != 0 {
		t.Fatal("worker emitted timing before cleanup completed")
	}
	if recorder == nil || executionContext.Err() != nil {
		t.Fatal("timing missing or timeout cancellation ran before native cleanup")
	}
	partial := recorder.Snapshot()
	if !partial.Stages[telemetry.ChildIPCFinalize].Observed || partial.Stages[telemetry.ChildWorkerCleanup].Observed {
		t.Fatal("held cleanup was prematurely reported complete")
	}
	held := time.Now()
	select {
	case <-done:
		t.Fatal("worker returned while native cleanup was held")
	case <-time.After(20 * time.Millisecond):
	}
	minimumCleanup := time.Since(held)
	unblock()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not finish after native cleanup")
	}
	outcome := childFixtureOutcome(t, diagnostics.snapshot())
	if outcome.Error != nil || outcome.Timing == nil || !outcome.Timing.Valid(time.Since(started)) || executionContext.Err() != context.Canceled {
		t.Fatal("cleanup changed success or left an invalid timing report")
	}
	cleanup := outcome.Timing.Stages[telemetry.ChildWorkerCleanup]
	if cleanup.EndNS-cleanup.StartNS < int64(minimumCleanup) {
		t.Fatal("cleanup timing excluded the held native close")
	}
	if !bytes.HasSuffix(output.Bytes(), childEOS) || outcome.Stats.WireBytes != int64(output.Len()) {
		t.Fatal("cleanup reporting changed completed Arrow framing")
	}
	allocator.AssertSize(t, 0)
}

func TestChildWorkerTimingPreservesCleanupPanic(t *testing.T) {
	for _, version := range []uint8{0, telemetry.ChildTimingVersion} {
		in := childFixtureInput()
		in.TimingVersion = version
		var executionContext context.Context
		var output, diagnostics bytes.Buffer
		allocator := memory.NewCheckedAllocator(memory.DefaultAllocator)
		panicValue := errors.New("fixture close panic")
		executor := &childExecutorFixture{
			execute: func(ctx context.Context, _ query.Request, sink query.Sink) (query.Stats, error) {
				executionContext = ctx
				return childFixtureRows(allocator, sink)
			},
			close: func() error {
				if executionContext.Err() != nil {
					t.Error("timeout cancellation ran before native cleanup")
				}
				panic(panicValue)
			},
		}
		var recovered any
		func() {
			defer func() { recovered = recover() }()
			_ = runWorkerIO(context.Background(), childFixtureJSON(t, in), &output, &diagnostics,
				func(worker.Input) (query.Executor, io.Closer, error) { return executor, executor, nil })
		}()
		if recovered != panicValue || executionContext == nil || executionContext.Err() != context.Canceled {
			t.Fatal("cleanup panic was swallowed or skipped context cancellation")
		}
		if diagnostics.Len() != 0 {
			t.Fatal("worker emitted an outcome after cleanup panicked")
		}
		allocator.AssertSize(t, 0)
	}
}

func TestChildWorkerTimingPreservesArrowAndDisabledContext(t *testing.T) {
	var baseline []byte
	for _, version := range []uint8{0, telemetry.ChildTimingVersion, 99} {
		in := childFixtureInput()
		in.TimingVersion = version
		in.Limits.ResultCompression = "lz4_frame"
		var output, diagnostics bytes.Buffer
		allocator := memory.NewCheckedAllocator(memory.DefaultAllocator)
		executor := &childExecutorFixture{execute: func(ctx context.Context, _ query.Request, sink query.Sink) (query.Stats, error) {
			if (telemetry.ChildRecorderFromContext(ctx) != nil) != (version == telemetry.ChildTimingVersion) {
				t.Error("unknown or disabled timing created a context wrapper")
			}
			return childFixtureRows(allocator, sink)
		}}
		if err := runWorkerIO(context.Background(), childFixtureJSON(t, in), &output, &diagnostics,
			func(worker.Input) (query.Executor, io.Closer, error) { return executor, executor, nil }); err != nil {
			t.Fatal(err)
		}
		allocator.AssertSize(t, 0)
		outcome := childFixtureOutcome(t, diagnostics.Bytes())
		if outcome.Error != nil || (outcome.Timing != nil) != (version == telemetry.ChildTimingVersion) {
			t.Fatal("optional timing changed worker result")
		}
		if baseline == nil {
			baseline = bytes.Clone(output.Bytes())
		} else if !bytes.Equal(baseline, output.Bytes()) {
			t.Fatal("timing changed Arrow bytes or enabled pipe compression")
		}
		if !bytes.HasSuffix(output.Bytes(), childEOS) || outcome.Stats.WireBytes != int64(output.Len()) {
			t.Fatal("timing changed Arrow completion or accounting")
		}
		reader, err := ipc.NewReader(bytes.NewReader(output.Bytes()))
		if err != nil {
			t.Fatal(err)
		}
		if !reader.Next() {
			reader.Release()
			t.Fatal("missing Arrow record")
		}
		record := reader.RecordBatch()
		if record.NumRows() != 2 || record.Column(0).(*array.Int64).Value(0) != 9007199254740993 || record.Column(0).(*array.Int64).Value(1) != math.MaxInt64 ||
			record.Column(1).(*array.Decimal128).Value(0) != decimal128.FromI64(-1234567890123456789) || !record.Column(1).IsNull(1) ||
			record.Column(2).(*array.Timestamp).Value(0) != arrow.Timestamp(1700000000123456789) || !record.Column(2).IsNull(1) || !record.Column(3).IsNull(1) {
			reader.Release()
			t.Fatal("timed child lost Arrow values, precision or NULLs")
		}
		if reader.Next() || reader.Err() != nil {
			reader.Release()
			t.Fatal("Arrow stream was not complete")
		}
		reader.Release()
	}
}

type childFailWriter struct {
	bytes.Buffer
	onEOS bool
}

func (w *childFailWriter) Write(value []byte) (int, error) {
	if !w.onEOS || bytes.Equal(value, childEOS) {
		return 0, io.ErrClosedPipe
	}
	return w.Buffer.Write(value)
}

func TestChildWorkerTimingErrorStagesAndCancellation(t *testing.T) {
	for _, scenario := range []string{"invalid limits", "constructor", "query", "schema", "sink", "finalize", "canceled"} {
		t.Run(scenario, func(t *testing.T) {
			in := childFixtureInput()
			parent := context.Background()
			if scenario == "canceled" {
				var cancel context.CancelFunc
				parent, cancel = context.WithCancel(parent)
				cancel()
			}
			if scenario == "invalid limits" {
				in.Limits.MaxRows = 0
			}
			var output, diagnostics bytes.Buffer
			var stream io.Writer = &output
			if scenario == "sink" || scenario == "finalize" {
				stream = &childFailWriter{onEOS: scenario == "finalize"}
			}
			closed, constructed := false, false
			executor := &childExecutorFixture{
				execute: func(ctx context.Context, _ query.Request, sink query.Sink) (query.Stats, error) {
					if scenario == "query" {
						return query.Stats{}, query.NewError("QUERY_FAILED", "fixture")
					}
					if scenario == "schema" {
						return query.Stats{}, sink.Schema(nil)
					}
					if scenario == "canceled" {
						return query.Stats{}, ctx.Err()
					}
					return childFixtureRows(memory.DefaultAllocator, sink)
				},
				close: func() error { closed = true; return nil },
			}
			factory := func(worker.Input) (query.Executor, io.Closer, error) {
				constructed = true
				if scenario == "constructor" {
					return nil, nil, query.NewError("CONFIGURATION_ERROR", "fixture")
				}
				return executor, executor, nil
			}
			started := time.Now()
			if err := runWorkerIO(parent, childFixtureJSON(t, in), stream, &diagnostics, factory); err != nil {
				t.Fatal(err)
			}
			outcome := childFixtureOutcome(t, diagnostics.Bytes())
			if outcome.Error == nil || outcome.Timing == nil || !outcome.Timing.Valid(time.Since(started)) {
				t.Fatal("failed query lost its error or valid timing", outcome.Error)
			}
			executed := scenario != "invalid limits" && scenario != "constructor"
			if constructed != (scenario != "invalid limits") || closed != executed || outcome.Timing.Stages[telemetry.ChildExecutorCall].Observed != executed ||
				outcome.Timing.Stages[telemetry.ChildIPCFinalize].Observed != (scenario == "finalize") {
				t.Fatal("failed stage was invented, skipped cleanup or incorrectly finalized")
			}
			if !outcome.Timing.Stages[telemetry.ChildWorkerCleanup].Observed || bytes.HasSuffix(output.Bytes(), childEOS) {
				t.Fatal("failure skipped cleanup or emitted a successful EOS")
			}
		})
	}
}

func TestChildWorkerInvalidEnvelopeHasNoTiming(t *testing.T) {
	for _, input := range []string{`{"timing_version":1`, `{"timing_version":1,"unknown":true}`, `{} {}`} {
		var output, diagnostics bytes.Buffer
		if err := runWorkerIO(context.Background(), bytes.NewBufferString(input), &output, &diagnostics,
			func(worker.Input) (query.Executor, io.Closer, error) {
				t.Fatal("invalid input constructed an engine")
				return nil, nil, nil
			}); err != nil {
			t.Fatal(err)
		}
		outcome := childFixtureOutcome(t, diagnostics.Bytes())
		if outcome.Error == nil || outcome.Timing != nil || output.Len() != 0 {
			t.Fatal("invalid envelope gained child telemetry or Arrow output")
		}
	}
}
