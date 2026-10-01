// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package federation

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/duckbridge"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

var idSchema = arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Uint64}}, nil)
var registeredTable = catalog.FederationTable{Name: "orders", Database: "reports", Table: "events"}

func testSource() catalog.Source {
	return catalog.Source{ID: "analytics", Type: "clickhouse", URLEnv: "KELVO_SOURCE_FEDERATION_TEST_URL", Federation: &catalog.FederationConfig{Tables: []catalog.FederationTable{registeredTable}}}
}
func uintRecord(allocator memory.Allocator, values ...uint64) arrow.RecordBatch {
	builder := array.NewUint64Builder(allocator)
	builder.AppendValues(values, nil)
	column := builder.NewArray()
	builder.Release()
	defer column.Release()
	return array.NewRecordBatch(idSchema, []arrow.Array{column}, int64(len(values)))
}
func await(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("operation did not complete")
	}
}
func checkCode(t *testing.T, err error, code string) {
	t.Helper()
	var public *query.Error
	if !errors.As(err, &public) || public.Code != code {
		t.Fatalf("got %v; want %s", err, code)
	}
}

type fakeExecutor struct {
	run   func(context.Context, query.Request, query.Sink) error
	close func()
}

func (e *fakeExecutor) Execute(ctx context.Context, req query.Request, sink query.Sink) (query.Stats, error) {
	return query.Stats{}, e.run(ctx, req, sink)
}
func (e *fakeExecutor) Close() error {
	if e.close != nil {
		e.close()
	}
	return nil
}
func mockTable(t *testing.T, limits query.Limits, source catalog.Source, run func(context.Context, query.Request, query.Sink) error, closed *atomic.Int32) *Table {
	t.Helper()
	table, err := newTable(context.Background(), source, registeredTable, limits, func(config catalog.Config, effective query.Limits) (execution, error) {
		return &fakeExecutor{run: func(ctx context.Context, req query.Request, sink query.Sink) error {
			if strings.HasSuffix(req.SQL, " LIMIT 0") {
				if req.SQL != "SELECT * FROM `reports`.`events` LIMIT 0" {
					return errors.New("wrong describe query")
				}
				return sink.Schema(idSchema)
			}
			return run(ctx, req, sink)
		}, close: func() {
			if closed != nil {
				closed.Add(1)
			}
		}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = table.Close() })
	return table
}

func TestHandoffBackpressureReleaseAndRetainedArrowMemory(t *testing.T) {
	allocator := memory.NewCheckedAllocator(memory.NewGoAllocator())
	var closed atomic.Int32
	returned := make(chan struct{})
	table := mockTable(t, query.DefaultLimits(), testSource(), func(ctx context.Context, req query.Request, sink query.Sink) error {
		defer close(returned)
		if err := sink.Schema(idSchema); err != nil {
			return err
		}
		record := uintRecord(allocator, 18446744073709551615)
		defer record.Release()
		return sink.Write(record)
	}, &closed)
	reader, err := table.Scan(context.Background(), duckbridge.ScanPlan{Columns: []string{"id"}})
	if err != nil {
		t.Fatal(err)
	}
	if !reader.Next() {
		t.Fatalf("missing batch: %v", reader.Err())
	}
	retained := reader.RecordBatch()
	retained.Retain()
	select {
	case <-returned:
		t.Fatal("producer advanced before consumer acknowledged batch")
	default:
	}
	reader.Release()
	await(t, returned)
	if closed.Load() != 2 {
		t.Fatal("reader Release did not close its source executor")
	}
	if retained.Column(0).(*array.Uint64).Value(0) != 18446744073709551615 {
		t.Fatal("retained Arrow data became invalid after source closure")
	}
	retained.Release()
	allocator.AssertSize(t, 0)
	if stats := table.Stats(); stats.Rows != 1 || stats.Batches != 1 || stats.Scans != 1 || stats.Bytes <= 0 {
		t.Fatalf("fetched stats wrong: %+v", stats)
	}
}

func TestLateProducerErrorIsVisibleAtEOF(t *testing.T) {
	allocator := memory.NewCheckedAllocator(memory.NewGoAllocator())
	table := mockTable(t, query.DefaultLimits(), testSource(), func(ctx context.Context, req query.Request, sink query.Sink) error {
		if err := sink.Schema(idSchema); err != nil {
			return err
		}
		record := uintRecord(allocator, 9007199254740993)
		defer record.Release()
		if err := sink.Write(record); err != nil {
			return err
		}
		return query.NewError("QUERY_FAILED", "Source failed after its final batch")
	}, nil)
	reader, err := table.Scan(context.Background(), duckbridge.ScanPlan{Columns: []string{"id"}})
	if err != nil {
		t.Fatal(err)
	}
	if !reader.Next() || reader.RecordBatch().Column(0).(*array.Uint64).Value(0) != 9007199254740993 {
		t.Fatal("first exact batch missing")
	}
	if reader.Next() {
		t.Fatal("unexpected second batch")
	}
	checkCode(t, reader.Err(), "QUERY_FAILED")
	reader.Release()
	allocator.AssertSize(t, 0)
}

func TestSchemaDriftAndScanOverflowFailBeforeDelivery(t *testing.T) {
	for _, mode := range []string{"schema", "rows", "bytes"} {
		t.Run(mode, func(t *testing.T) {
			allocator := memory.NewCheckedAllocator(memory.NewGoAllocator())
			limits := query.DefaultLimits()
			limits.MaxRows = 1
			limits.MaxBytes = 1024
			table := mockTable(t, limits, testSource(), func(ctx context.Context, req query.Request, sink query.Sink) error {
				if mode == "schema" {
					return sink.Schema(arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil))
				}
				if err := sink.Schema(idSchema); err != nil {
					return err
				}
				values := []uint64{1, 2}
				if mode == "bytes" {
					values = make([]uint64, 256)
				}
				record := uintRecord(allocator, values...)
				defer record.Release()
				return sink.Write(record)
			}, nil)
			if mode == "bytes" {
				table.limits.MaxRows = 1000
			}
			reader, err := table.Scan(context.Background(), duckbridge.ScanPlan{Columns: []string{"id"}})
			if err != nil {
				t.Fatal(err)
			}
			if reader.Next() {
				t.Fatal("invalid batch was delivered")
			}
			want := "RESOURCE_EXHAUSTED"
			if mode == "schema" {
				want = "QUERY_FAILED"
			}
			checkCode(t, reader.Err(), want)
			reader.Release()
			allocator.AssertSize(t, 0)
		})
	}
}

func TestConcurrentScansCloseIndependently(t *testing.T) {
	allocator := memory.NewCheckedAllocator(memory.NewGoAllocator())
	var closed, sequence atomic.Int32
	table := mockTable(t, query.DefaultLimits(), testSource(), func(ctx context.Context, req query.Request, sink query.Sink) error {
		if err := sink.Schema(idSchema); err != nil {
			return err
		}
		record := uintRecord(allocator, uint64(sequence.Add(1)))
		defer record.Release()
		return sink.Write(record)
	}, &closed)
	first, err := table.Scan(context.Background(), duckbridge.ScanPlan{Columns: []string{"id"}})
	if err != nil {
		t.Fatal(err)
	}
	second, err := table.Scan(context.Background(), duckbridge.ScanPlan{Columns: []string{"id"}})
	if err != nil {
		t.Fatal(err)
	}
	if !first.Next() || !second.Next() {
		t.Fatal("overlapping scans did not each return a batch")
	}
	if first.RecordBatch().Column(0).(*array.Uint64).Value(0) == second.RecordBatch().Column(0).(*array.Uint64).Value(0) {
		t.Fatal("scan state was shared")
	}
	if err := table.Close(); err != nil {
		t.Fatal(err)
	}
	if closed.Load() != 3 {
		t.Fatal("table Close did not close both scan executors")
	}
	first.Release()
	second.Release()
	allocator.AssertSize(t, 0)
	if _, err := table.Scan(context.Background(), duckbridge.ScanPlan{}); err == nil {
		t.Fatal("closed table accepted a scan")
	}
}

func TestConfiguredScanBudgetsOverrideResultBudgets(t *testing.T) {
	source := testSource()
	source.Federation.MaxScanRows = 77
	source.Federation.MaxScanBytes = 4096
	limits := query.DefaultLimits()
	limits.MaxRows = 2
	limits.MaxBytes = 1024
	var calls int
	table, err := newTable(context.Background(), source, registeredTable, limits, func(config catalog.Config, effective query.Limits) (execution, error) {
		calls++
		if effective.MaxRows != 77 || effective.MaxBytes != 4096 || effective.Threads != limits.Threads {
			t.Error("configured scan budgets did not reach native source")
		}
		return &fakeExecutor{run: func(ctx context.Context, req query.Request, sink query.Sink) error { return sink.Schema(idSchema) }}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer table.Close()
	reader, err := table.Scan(context.Background(), duckbridge.ScanPlan{Columns: []string{"id"}})
	if err != nil {
		t.Fatal(err)
	}
	if reader.Next() || reader.Err() != nil {
		t.Fatalf("empty scan failed: %v", reader.Err())
	}
	reader.Release()
	if calls != 2 {
		t.Fatal("describe and scan did not receive independent native executors")
	}
}

func TestUnregisteredTableCannotReachSource(t *testing.T) {
	source := testSource()
	table := registeredTable
	table.Table = "secrets"
	_, err := newTable(context.Background(), source, table, query.DefaultLimits(), func(catalog.Config, query.Limits) (execution, error) {
		t.Fatal("unregistered table reached native source")
		return nil, nil
	})
	checkCode(t, err, "PERMISSION_DENIED")
}

func arrowWire(t *testing.T, records ...arrow.RecordBatch) []byte {
	t.Helper()
	var buffer bytes.Buffer
	writer := ipc.NewWriter(&buffer, ipc.WithSchema(idSchema))
	for _, record := range records {
		if err := writer.Write(record); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}
func nativeTable(t *testing.T, handler http.HandlerFunc) *Table {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	source := testSource()
	t.Setenv(source.URLEnv, server.URL)
	table, err := New(context.Background(), source, registeredTable, query.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = table.Close() })
	return table
}

func TestNativeHTTPSourceCancellationAndLateError(t *testing.T) {
	schemaWire := arrowWire(t)
	t.Run("cancellation", func(t *testing.T) {
		started, closed := make(chan struct{}), make(chan struct{})
		table := nativeTable(t, func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			if strings.HasSuffix(string(body), " LIMIT 0") {
				_, _ = w.Write(schemaWire)
				return
			}
			if r.URL.Query().Get("cancel_http_readonly_queries_on_client_close") != "1" || r.URL.Query().Get("readonly") != "1" {
				t.Error("native cancellation/read-only settings missing")
			}
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			close(started)
			<-r.Context().Done()
			close(closed)
		})
		ctx, cancel := context.WithCancel(context.Background())
		reader, err := table.Scan(ctx, duckbridge.ScanPlan{Columns: []string{"id"}})
		if err != nil {
			t.Fatal(err)
		}
		nextDone := make(chan struct{})
		go func() {
			defer close(nextDone)
			if reader.Next() {
				t.Error("stalled source returned a batch")
			}
		}()
		await(t, started)
		cancel()
		await(t, nextDone)
		checkCode(t, reader.Err(), "CANCELLED")
		reader.Release()
		await(t, closed)
	})
	t.Run("late_HTTP_exception", func(t *testing.T) {
		record := uintRecord(memory.DefaultAllocator, 18446744073709551615)
		wire := arrowWire(t, record)
		record.Release()
		table := nativeTable(t, func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			if strings.HasSuffix(string(body), " LIMIT 0") {
				_, _ = w.Write(schemaWire)
				return
			}
			_, _ = w.Write(append(wire, []byte("source-secret late exception")...))
		})
		reader, err := table.Scan(context.Background(), duckbridge.ScanPlan{Columns: []string{"id"}})
		if err != nil {
			t.Fatal(err)
		}
		if !reader.Next() {
			t.Fatalf("first batch missing: %v", reader.Err())
		}
		if reader.RecordBatch().Column(0).(*array.Uint64).Value(0) != 18446744073709551615 {
			t.Fatal("uint64 changed")
		}
		if reader.Next() {
			t.Fatal("late source error became another batch")
		}
		checkCode(t, reader.Err(), "QUERY_FAILED")
		if strings.Contains(reader.Err().Error(), "secret") {
			t.Fatal("late source error disclosed private response")
		}
		reader.Release()
	})
}

func TestSharedScanBudgetAcrossTablesReleasesOnCancellation(t *testing.T) {
	budgetContext := WithScanBudget(context.Background(), 1)
	var opened, closed atomic.Int32
	factory := func(catalog.Config, query.Limits) (execution, error) {
		opened.Add(1)
		return &fakeExecutor{run: func(ctx context.Context, request query.Request, sink query.Sink) error {
			if strings.HasSuffix(request.SQL, " LIMIT 0") {
				return sink.Schema(idSchema)
			}
			if err := sink.Schema(idSchema); err != nil {
				return err
			}
			<-ctx.Done()
			return query.PublicError(ctx.Err())
		}, close: func() { closed.Add(1) }}, nil
	}
	first, err := newTable(budgetContext, testSource(), registeredTable, query.DefaultLimits(), factory)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := newTable(budgetContext, testSource(), registeredTable, query.DefaultLimits(), factory)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	reader, err := first.Scan(context.Background(), duckbridge.ScanPlan{Columns: []string{"id"}})
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if denied, err := second.Scan(context.Background(), duckbridge.ScanPlan{Columns: []string{"id"}}); denied != nil || err == nil {
		t.Fatal("shared scan cap was bypassed")
	} else {
		checkCode(t, err, "RESOURCE_EXHAUSTED")
	}
	if time.Since(started) > time.Second || opened.Load() != 3 {
		t.Fatal("full scan budget waited or opened another source")
	}
	reader.Release()
	replacement, err := second.Scan(context.Background(), duckbridge.ScanPlan{Columns: []string{"id"}})
	if err != nil {
		t.Fatalf("cancelled producer retained scan slot: %v", err)
	}
	replacement.Release()
	if opened.Load() != 4 || closed.Load() != 4 {
		t.Fatal("source lifecycle leaked a concurrent scan slot or executor")
	}
}

func TestDefaultScanBudgetAndFactoryFailureRelease(t *testing.T) {
	var fail atomic.Bool
	factory := func(catalog.Config, query.Limits) (execution, error) {
		if fail.Load() {
			return nil, query.NewError("QUERY_FAILED", "fixture unavailable")
		}
		return &fakeExecutor{run: func(ctx context.Context, request query.Request, sink query.Sink) error {
			if err := sink.Schema(idSchema); err != nil {
				return err
			}
			if strings.HasSuffix(request.SQL, " LIMIT 0") {
				return nil
			}
			<-ctx.Done()
			return query.PublicError(ctx.Err())
		}}, nil
	}
	table, err := newTable(context.Background(), testSource(), registeredTable, query.DefaultLimits(), factory)
	if err != nil {
		t.Fatal(err)
	}
	defer table.Close()
	fail.Store(true)
	if _, err := table.Scan(context.Background(), duckbridge.ScanPlan{Columns: []string{"id"}}); err == nil {
		t.Fatal("factory failure was ignored")
	}
	fail.Store(false)
	readers := make([]array.RecordReader, 0, 4)
	for range 4 {
		reader, err := table.Scan(context.Background(), duckbridge.ScanPlan{Columns: []string{"id"}})
		if err != nil {
			t.Fatalf("default budget slot leaked: %v", err)
		}
		readers = append(readers, reader)
	}
	if _, err := table.Scan(context.Background(), duckbridge.ScanPlan{Columns: []string{"id"}}); err == nil {
		t.Fatal("default four-scan cap was bypassed")
	} else {
		checkCode(t, err, "RESOURCE_EXHAUSTED")
	}
	for _, reader := range readers {
		reader.Release()
	}
}

func TestScanDeadlineIsPreserved(t *testing.T) {
	table := mockTable(t, query.DefaultLimits(), testSource(), func(ctx context.Context, request query.Request, sink query.Sink) error {
		<-ctx.Done()
		return query.PublicError(ctx.Err())
	}, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	reader, err := table.Scan(ctx, duckbridge.ScanPlan{Columns: []string{"id"}})
	if err != nil {
		t.Fatal(err)
	}
	if reader.Next() {
		t.Fatal("expired scan returned data")
	}
	checkCode(t, reader.Err(), "DEADLINE_EXCEEDED")
	reader.Release()
}

func TestProjectionPreservesDecimalTimestampNullAndLargeInteger(t *testing.T) {
	allocator := memory.NewCheckedAllocator(memory.NewGoAllocator())
	metadata := arrow.NewMetadata([]string{"source"}, []string{"typed-fixture"})
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Uint64},
		{Name: "amount", Type: &arrow.Decimal128Type{Precision: 38, Scale: 12}},
		{Name: "at", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}},
		{Name: "label", Type: arrow.BinaryTypes.String, Nullable: true},
	}, &metadata)
	expectedAmount, err := decimal128.FromString("99999999999999999999999999.123456789012", 38, 12)
	if err != nil {
		t.Fatal(err)
	}
	factory := func(catalog.Config, query.Limits) (execution, error) {
		return &fakeExecutor{run: func(ctx context.Context, request query.Request, sink query.Sink) error {
			if strings.HasSuffix(request.SQL, " LIMIT 0") {
				return sink.Schema(schema)
			}
			projected := arrow.NewSchema([]arrow.Field{schema.Field(1), schema.Field(0), schema.Field(2), schema.Field(3)}, &metadata)
			if err := sink.Schema(projected); err != nil {
				return err
			}
			builder := array.NewRecordBuilder(allocator, projected)
			defer builder.Release()
			builder.Field(0).(*array.Decimal128Builder).Append(expectedAmount)
			builder.Field(1).(*array.Uint64Builder).Append(18446744073709551615)
			builder.Field(2).(*array.TimestampBuilder).Append(arrow.Timestamp(-315521754876544))
			builder.Field(3).(*array.StringBuilder).AppendNull()
			record := builder.NewRecordBatch()
			defer record.Release()
			return sink.Write(record)
		}}, nil
	}
	table, err := newTable(context.Background(), testSource(), registeredTable, query.DefaultLimits(), factory)
	if err != nil {
		t.Fatal(err)
	}
	defer table.Close()
	reader, err := table.Scan(context.Background(), duckbridge.ScanPlan{Columns: []string{"amount", "id", "at", "label"}})
	if err != nil {
		t.Fatal(err)
	}
	if !reader.Next() {
		t.Fatalf("typed batch unavailable: %v", reader.Err())
	}
	record := reader.RecordBatch()
	if record.Column(0).(*array.Decimal128).Value(0) != expectedAmount || record.Column(1).(*array.Uint64).Value(0) != 18446744073709551615 || record.Column(2).(*array.Timestamp).Value(0) != -315521754876544 || !record.Column(3).IsNull(0) || !record.Schema().Metadata().Equal(metadata) {
		t.Fatal("projection changed a precise typed value, NULL, or metadata")
	}
	if reader.Next() || reader.Err() != nil {
		t.Fatalf("typed batch ended incorrectly: %v", reader.Err())
	}
	reader.Release()
	allocator.AssertSize(t, 0)
}
