//go:build duckdb_arrow

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package duckdb

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/telemetry"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

type childTimingSink struct {
	output   bytes.Buffer
	writer   *ipc.Writer
	schema   func() error
	write    func(arrow.RecordBatch) error
	retained arrow.RecordBatch
}

func (s *childTimingSink) Schema(schema *arrow.Schema) error {
	if s.schema != nil {
		if err := s.schema(); err != nil {
			return err
		}
	}
	s.writer = ipc.NewWriter(&s.output, ipc.WithSchema(schema))
	return nil
}

func (s *childTimingSink) Write(record arrow.RecordBatch) error {
	if s.write != nil {
		if err := s.write(record); err != nil {
			return err
		}
	}
	return s.writer.Write(record)
}

func beginEngineTiming() *telemetry.ChildRecorder {
	r := telemetry.NewChildRecorder()
	r.Begin(telemetry.ChildWorkerSetup)
	r.End(telemetry.ChildWorkerSetup)
	r.Begin(telemetry.ChildExecutorCall)
	return r
}

func finishEngineTiming(r *telemetry.ChildRecorder) *telemetry.ChildTiming {
	r.End(telemetry.ChildExecutorCall)
	r.Begin(telemetry.ChildWorkerCleanup)
	r.End(telemetry.ChildWorkerCleanup)
	return r.Finish()
}

func TestChildDuckDBMaterializesBeforeBlockedSink(t *testing.T) {
	for _, boundary := range []string{"schema", "write"} {
		t.Run(boundary, func(t *testing.T) { testChildDuckDBMaterializesBeforeBlockedSink(t, boundary) })
	}
}

func testChildDuckDBMaterializesBeforeBlockedSink(t *testing.T, boundary string) {
	t.Helper()
	bounded := limits()
	bounded.MaxRows, bounded.Timeout = 8192, 10*time.Second
	engine, err := New(catalog.Config{}, bounded)
	if err != nil {
		t.Fatal(err)
	}
	request := query.Request{SQL: "SELECT CAST(i AS BIGINT) AS id, CAST(i / 100.0 AS DECIMAL(20,2)) AS amount, CASE WHEN i % 7 = 0 THEN NULL ELSE 'value' END AS label FROM range(4096) t(i) ORDER BY i"}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	recorder := beginEngineTiming()
	sink := &childTimingSink{}
	if boundary == "schema" {
		sink.schema = func() error { close(entered); <-release; return nil }
	}
	var first sync.Once
	sink.write = func(record arrow.RecordBatch) error {
		first.Do(func() {
			// Borrowing extends only through Write unless the sink retains it.
			record.Retain()
			sink.retained = record
			if boundary == "write" {
				close(entered)
				<-release
			}
		})
		return nil
	}
	t.Cleanup(func() {
		if sink.retained != nil {
			sink.retained.Release()
		}
	})
	type result struct {
		stats query.Stats
		err   error
	}
	done := make(chan result, 1)
	exited := make(chan struct{})
	started := time.Now()
	go func() {
		defer close(exited)
		stats, err := engine.Execute(telemetry.WithChildRecorder(context.Background(), recorder), request, sink)
		done <- result{stats, err}
	}()
	defer func() {
		unblock()
		select {
		case <-exited:
		case <-time.After(5 * time.Second):
			t.Error("blocked DuckDB execution did not join")
		}
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("DuckDB did not reach Arrow delivery")
	}
	before := recorder.Snapshot()
	setup, materialization := before.Stages[telemetry.ChildEngineSetup], before.Stages[telemetry.ChildMaterialization]
	if !setup.Observed || !materialization.Observed || materialization.StartNS < setup.EndNS || before.Stages[telemetry.ChildArrowDrain].Observed {
		t.Fatal("materialization was not complete before the sink blocked")
	}
	held := time.Now()
	select {
	case <-done:
		t.Fatal("execution returned while Arrow delivery was held")
	case <-time.After(20 * time.Millisecond):
	}
	minimumDrain := time.Since(held)
	if recorder.Snapshot().Stages[telemetry.ChildMaterialization] != materialization {
		t.Fatal("sink backpressure changed completed materialization timing")
	}
	unblock()
	var executed result
	select {
	case executed = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("DuckDB did not return after sink release")
	}
	if executed.err != nil || executed.stats.Rows != 4096 {
		t.Fatal("timed DuckDB failed", executed.err)
	}
	report := finishEngineTiming(recorder)
	if report == nil || !report.Valid(time.Since(started)+time.Second) {
		t.Fatal("child DuckDB report invalid")
	}
	drain := report.Stages[telemetry.ChildArrowDrain]
	if !drain.Observed || drain.StartNS < materialization.EndNS || drain.EndNS-drain.StartNS < int64(minimumDrain) {
		t.Fatal("Arrow drain omitted blocked sink time or overlapped materialization")
	}
	if sink.retained == nil || sink.retained.Column(0).(*array.Int64).Value(0) != 0 || !sink.retained.Column(2).IsNull(0) {
		t.Fatal("retained Arrow batch did not survive reader cleanup")
	}
	if err := sink.writer.Close(); err != nil {
		t.Fatal(err)
	}
	baseline := &childTimingSink{}
	stats, err := engine.Execute(context.Background(), request, baseline)
	if err != nil || stats.Rows != executed.stats.Rows {
		t.Fatal("untimed DuckDB baseline failed", err)
	}
	if err := baseline.writer.Close(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(sink.output.Bytes(), baseline.output.Bytes()) || !bytes.HasSuffix(sink.output.Bytes(), []byte{255, 255, 255, 255, 0, 0, 0, 0}) {
		t.Fatal("child timing changed Arrow values, types or framing")
	}
}

func TestChildDuckDBTimingErrorBoundaries(t *testing.T) {
	for _, scenario := range []string{"setup", "materialization", "schema", "sink", "canceled before setup", "canceled during drain"} {
		t.Run(scenario, func(t *testing.T) {
			bounded := limits()
			bounded.Timeout = 20 * time.Second
			request := query.Request{SQL: "SELECT CAST(7 AS BIGINT) AS id"}
			if scenario == "setup" {
				request.SQL = "SELECT * FROM unavailable_table"
			}
			if scenario == "materialization" {
				bounded.MemoryMB, bounded.MaxTempMB = 16, 16
				request.SQL = nativeMemoryFailureSQL
			}
			engine, err := New(catalog.Config{}, bounded)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if scenario == "canceled before setup" {
				cancel()
			}
			sink := &childTimingSink{}
			if scenario == "schema" {
				sink.schema = func() error { return errors.New("fixture schema failure") }
			}
			if scenario == "sink" {
				sink.write = func(arrow.RecordBatch) error { return errors.New("fixture sink failure") }
			}
			if scenario == "canceled during drain" {
				sink.write = func(arrow.RecordBatch) error { cancel(); return ctx.Err() }
			}
			recorder := beginEngineTiming()
			started := time.Now()
			_, err = engine.Execute(telemetry.WithChildRecorder(ctx, recorder), request, sink)
			elapsed := time.Since(started)
			if sink.writer != nil {
				// Test writer cleanup is independent of the engine reader lifecycle.
				defer sink.writer.Close()
			}
			if err == nil {
				t.Fatal("fixture did not exercise its error boundary")
			}
			report := finishEngineTiming(recorder)
			if report == nil || !report.Valid(elapsed+time.Second) {
				t.Fatal("failed engine execution invalidated the outer report")
			}
			materialized := scenario != "setup" && scenario != "canceled before setup"
			drained := materialized && scenario != "materialization"
			if report.Stages[telemetry.ChildEngineSetup].Observed != materialized || report.Stages[telemetry.ChildMaterialization].Observed != materialized || report.Stages[telemetry.ChildArrowDrain].Observed != drained {
				t.Fatal("an unreached child stage was reported or reached stage omitted")
			}
			if scenario == "materialization" && query.PublicError(err).Code != "RESOURCE_EXHAUSTED" {
				t.Fatal("materialization fixture failed outside the native memory bound", err)
			}
			if (scenario == "canceled before setup" || scenario == "canceled during drain") && query.PublicError(err).Code != "CANCELLED" {
				t.Fatal("timing changed cancellation classification", err)
			}
		})
	}
}
