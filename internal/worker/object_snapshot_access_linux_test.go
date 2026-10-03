//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/acceleration"
	"github.com/SYNEHQ/kelvo-go/internal/access"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/objectstore"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Synthetic immutable upstream, real production parent bridge and DuckDB.
// This fixture does not certify any cloud provider or native database driver.
type objectAccessClient struct {
	data                          []byte
	digest                        string
	keys                          map[string]string
	corrupt                       atomic.Bool
	wrongVersion                  atomic.Bool
	blocked                       atomic.Bool
	started                       chan struct{}
	pause                         chan struct{}
	startOnce                     sync.Once
	calls, active, bodies, closed atomic.Int64
}

func (*objectAccessClient) Get(context.Context, string, string) (io.ReadCloser, objectstore.Info, error) {
	return nil, objectstore.Info{}, errors.New("whole-object reads forbidden")
}
func (*objectAccessClient) Head(context.Context, string, string) (objectstore.Info, error) {
	return objectstore.Info{}, errors.New("unacquired metadata forbidden")
}
func (*objectAccessClient) Put(context.Context, string, io.ReadSeeker, int64, string, objectstore.Condition) (objectstore.Info, error) {
	return objectstore.Info{}, errors.New("writes forbidden")
}
func (c *objectAccessClient) Close() { c.closed.Add(1) }
func (c *objectAccessClient) GetRange(ctx context.Context, key, version string, offset, length int64) (io.ReadCloser, objectstore.Info, error) {
	c.calls.Add(1)
	c.active.Add(1)
	defer c.active.Add(-1)
	if c.keys[key] != version || version == "" || offset < 0 || length < 1 || offset > int64(len(c.data))-length {
		return nil, objectstore.Info{}, errors.New("range escaped acquired generation")
	}
	if c.blocked.Load() {
		c.startOnce.Do(func() { close(c.started) })
		<-ctx.Done()
		return nil, objectstore.Info{}, ctx.Err()
	}
	if c.pause != nil {
		c.startOnce.Do(func() { close(c.started) })
		select {
		case <-c.pause:
		case <-ctx.Done():
			return nil, objectstore.Info{}, ctx.Err()
		}
	}
	info := objectstore.Info{Size: int64(len(c.data)), Version: version, SHA256: c.digest}
	if c.wrongVersion.Load() {
		info.Version = "replaced-version"
	}
	data := append([]byte(nil), c.data[offset:offset+length]...)
	if c.corrupt.Load() && offset == 0 {
		data[0] ^= 1
	}
	c.bodies.Add(1)
	return &objectAccessBody{Reader: bytes.NewReader(data), close: func() { c.bodies.Add(-1) }}, info, nil
}

type objectAccessBody struct {
	*bytes.Reader
	close func()
	once  sync.Once
}

func (b *objectAccessBody) Close() error { b.once.Do(b.close); return nil }

func objectAccessRecord() arrow.RecordBatch {
	metadata := arrow.MetadataFrom(map[string]string{"private": "object-private-metadata"})
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "tenant_id", Type: arrow.PrimitiveTypes.Int64, Metadata: metadata},
		{Name: "id", Type: arrow.PrimitiveTypes.Uint64},
		{Name: "amount", Type: &arrow.Decimal128Type{Precision: 30, Scale: 4}, Nullable: true},
		{Name: "observed", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
		{Name: "tiny", Type: arrow.PrimitiveTypes.Int8},
	}, &metadata)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).AppendValues([]int64{7, 8, 7, 7}, nil)
	b.Field(1).(*array.Uint64Builder).AppendValues([]uint64{1, 2, 18446744073709551615, 4}, nil)
	b.Field(2).(*array.Decimal128Builder).AppendValues([]decimal128.Num{decimal128.FromI64(-123456789), decimal128.FromI64(99), {}, decimal128.FromI64(1)}, []bool{true, true, false, true})
	b.Field(3).(*array.TimestampBuilder).AppendValues([]arrow.Timestamp{-315521754876544, 0, 123456789, 0}, []bool{true, true, true, false})
	b.Field(4).(*array.Int8Builder).AppendValues([]int8{-128, 0, 127, 1}, nil)
	return b.NewRecordBatch()
}

func objectAccessFixture(t *testing.T, parts int) (*Executor, context.Context, catalog.Source, *objectAccessClient, func()) {
	t.Helper()
	binary, launcher := os.Getenv("KELVO_TEST_SNAPSHOT_BINARY"), os.Getenv("KELVO_TEST_SNAPSHOT_SANDBOX")
	if binary == "" || launcher == "" {
		t.Skip("set KELVO_TEST_SNAPSHOT_BINARY and KELVO_TEST_SNAPSHOT_SANDBOX for the real object worker gate")
	}
	record := objectAccessRecord()
	defer record.Release()
	var out bytes.Buffer
	sink := acceleration.NewParquetSink(&out, query.DefaultLimits())
	if err := sink.Schema(record.Schema()); err != nil {
		t.Fatal(err)
	}
	if err := sink.Write(record); err != nil {
		t.Fatal(err)
	}
	if err := sink.Finish(); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(out.Bytes())
	digest := hex.EncodeToString(sum[:])
	schemaHash, err := acceleration.SchemaFingerprint(record.Schema())
	if err != nil {
		t.Fatal(err)
	}
	storage := catalog.ObjectStorage{ObjectLocation: catalog.ObjectLocation{Provider: "s3", Endpoint: "https://synthetic.invalid", Bucket: "fixtures", Prefix: "cache", Region: "us-east-1"}}
	snapshot := acceleration.Snapshot{Dataset: "orders_object", Generation: strings.Repeat("a", 32), SHA256: digest, SchemaHash: schemaHash}
	client := &objectAccessClient{data: out.Bytes(), digest: digest, keys: map[string]string{}, started: make(chan struct{})}
	for index := 0; index < parts; index++ {
		name := snapshot.Generation + ".parquet"
		if parts > 1 {
			name = fmt.Sprintf("%s-part-%04d.parquet", snapshot.Generation, index)
		}
		key := storage.Prefix + "/tenant-a/" + snapshot.Dataset + "/" + name
		version := fmt.Sprintf("synthetic-%d", index)
		client.keys[key] = version
		if parts == 1 {
			snapshot.ObjectKey, snapshot.ObjectVersion = key, version
			snapshot.Path, err = storage.URI(key)
			if err != nil {
				t.Fatal(err)
			}
		} else {
			snapshot.Parts = append(snapshot.Parts, acceleration.SnapshotPart{ObjectKey: key, ObjectVersion: version, Rows: record.NumRows(), Bytes: int64(out.Len()), SHA256: digest})
		}
		snapshot.Rows += record.NumRows()
		snapshot.Bytes += int64(out.Len())
	}
	sources, release, err := acceleration.OpenObjectRangesWithClient(context.Background(), storage, []acceleration.Snapshot{snapshot}, client)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(release)
	source := sources[snapshot.Dataset]
	source.ObjectSnapshot = &catalog.ObjectSnapshotRead{Dataset: source.ID, Generation: snapshot.Generation, SchemaSHA256: schemaHash, Scan: catalog.SnapshotScanLimits{MaxRows: 1_000_000, MaxBytes: 64 << 20}}
	for index := 0; index < parts; index++ {
		target := source.Path
		if parts > 1 {
			target = source.Ranges[index].URL
		}
		source.ObjectSnapshot.Parts = append(source.ObjectSnapshot.Parts, catalog.ObjectSnapshotPart{URL: target, Rows: record.NumRows(), Bytes: int64(out.Len()), SHA256: digest})
	}
	policy := access.Policy{Sources: map[string]access.SourcePolicy{source.ID: {Tables: map[string]access.TablePolicy{source.ID: {Columns: []string{"id", "amount", "observed", "tiny"}, Rows: &access.Predicate{Kind: "comparison", Column: "tenant_id", Type: "int64", Op: "eq", Value: "7"}}}}}}
	ctx, err := access.WithPolicy(context.Background(), policy)
	if err != nil {
		t.Fatal(err)
	}
	limits := query.DefaultLimits()
	limits.Timeout, limits.Threads = 15*time.Second, 1
	engine, err := New(catalog.Config{Sources: []catalog.Source{source}}, limits)
	if err != nil {
		t.Fatal(err)
	}
	engine.Binary, engine.SandboxPath = binary, launcher
	engine.ScratchRoot = scratchFixture(t)
	return engine, ctx, source, client, release
}

type objectExactValue struct {
	id           uint64
	amount       decimal128.Num
	amountNull   bool
	observed     arrow.Timestamp
	observedNull bool
	tiny         int8
}
type objectExactSink struct {
	schema *arrow.Schema
	values []objectExactValue
}

func (s *objectExactSink) Schema(schema *arrow.Schema) error { s.schema = schema; return nil }
func (s *objectExactSink) Write(record arrow.RecordBatch) error {
	id, idOK := record.Column(0).(*array.Uint64)
	amount, amountOK := record.Column(1).(*array.Decimal128)
	observed, observedOK := record.Column(2).(*array.Timestamp)
	tiny, tinyOK := record.Column(3).(*array.Int8)
	if !idOK || !amountOK || !observedOK || !tinyOK {
		return errors.New("query changed exact Arrow types")
	}
	for row := 0; row < int(record.NumRows()); row++ {
		value := objectExactValue{id: id.Value(row), amountNull: amount.IsNull(row), observedNull: observed.IsNull(row), tiny: tiny.Value(row)}
		if !value.amountNull {
			value.amount = amount.Value(row)
		}
		if !value.observedNull {
			value.observed = observed.Value(row)
		}
		s.values = append(s.values, value)
	}
	return nil
}

func requireObjectWorkerClean(t *testing.T, executor *Executor, client *objectAccessClient) {
	t.Helper()
	executor.ScratchRoot.mu.Lock()
	active := executor.ScratchRoot.active
	executor.ScratchRoot.mu.Unlock()
	if active != 0 || client.active.Load() != 0 || client.bodies.Load() != 0 {
		t.Fatal("query retained scratch or upstream range work")
	}
	entries, err := os.ReadDir(executor.ScratchRoot.path)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.Name() != scratchMutationName {
			t.Fatal("query retained scratch or a lease record")
		}
	}
	if children := objectWorkerChildren(t, executor.Binary); len(children) != 0 {
		t.Fatal("query retained an actual child process")
	}
}

func objectWorkerChildren(t *testing.T, binary string) []int {
	t.Helper()
	files, err := filepath.Glob("/proc/self/task/*/children")
	if err != nil || len(files) == 0 {
		t.Fatal("Linux child process enumeration unavailable", err)
	}
	seen := map[int]bool{}
	var children []int
	for _, path := range files {
		raw, err := os.ReadFile(path)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		for _, text := range strings.Fields(string(raw)) {
			pid, err := strconv.Atoi(text)
			if err != nil {
				t.Fatal(err)
			}
			executable, err := os.Readlink(fmt.Sprintf("/proc/%d/exe", pid))
			if err == nil && executable == binary && !seen[pid] {
				seen[pid] = true
				children = append(children, pid)
			}
		}
	}
	return children
}

func TestObjectSnapshotWorkerActualChildTypesNullsAndMultipart(t *testing.T) {
	for _, parts := range []int{1, 2} {
		t.Run(fmt.Sprintf("parts-%d", parts), func(t *testing.T) {
			executor, ctx, source, client, release := objectAccessFixture(t, parts)
			sink := &objectExactSink{}
			stats, err := executor.Execute(ctx, query.Request{Sources: []string{source.ID}, SQL: "SELECT id,amount,observed,tiny FROM orders_object ORDER BY id"}, sink)
			if err != nil {
				t.Fatal(err)
			}
			var want []objectExactValue
			for _, value := range []objectExactValue{{1, decimal128.FromI64(-123456789), false, -315521754876544, false, -128}, {4, decimal128.FromI64(1), false, 0, true, 1}, {18446744073709551615, decimal128.Num{}, true, 123456789, false, 127}} {
				for range parts {
					want = append(want, value)
				}
			}
			if !reflect.DeepEqual(sink.values, want) {
				t.Fatalf("child changed exact values or NULLs: got%v want%v", sink.values, want)
			}
			if sink.schema.Metadata().Len() != 0 || !arrow.TypeEqual(sink.schema.Field(1).Type, &arrow.Decimal128Type{Precision: 30, Scale: 4}) {
				t.Fatal("child changed decimal type or exposed metadata")
			}
			if stats.Rows != int64(3*parts) || stats.SourceWireBytes != 0 || stats.Federation != nil || stats.ScanDiagnostics != nil || client.calls.Load() == 0 {
				t.Fatal("child lost row count or exposed raw diagnostics")
			}
			requireObjectWorkerClean(t, executor, client)
			release()
			if client.closed.Load() != 1 {
				t.Fatal("range bridge did not close its client exactly once")
			}
		})
	}
}

func TestObjectSnapshotWorkerActualChildRejectsRawAccessAndReplay(t *testing.T) {
	executor, ctx, source, client, release := objectAccessFixture(t, 1)
	for _, sql := range []string{
		"SELECT tenant_id FROM orders_object", "SELECT * FROM read_parquet('" + source.Path + "')", "SELECT * FROM parquet_scan('" + source.Path + "')",
		"SELECT * FROM read_blob('" + source.Path + "')", "SELECT * FROM '" + source.Path + "'", "LOAD httpfs", "INSTALL httpfs", "SET enable_external_access=true",
	} {
		_, err := executor.Execute(ctx, query.Request{Sources: []string{source.ID}, SQL: sql}, &snapshotWorkerSink{})
		if err == nil || strings.Contains(err.Error(), source.Path) || strings.Contains(err.Error(), "object-private-metadata") {
			t.Fatalf("child raw access or disclosure: %v", err)
		}
		requireObjectWorkerClean(t, executor, client)
	}
	policy, _ := access.PolicyFromContext(ctx)
	input := Input{Config: catalog.Config{Sources: []catalog.Source{source}}, Access: &policy, Limits: executor.Limits, Request: query.Request{Sources: []string{source.ID}, SQL: "SELECT 1"}}
	copySource := source
	copyRange := *source.Range
	copyRange.URL = strings.Replace(copyRange.URL, "/"+source.ID, "/other", 1)
	copySource.Range, copySource.Path = &copyRange, copyRange.URL
	input.Config.Sources[0] = copySource
	if _, err := input.ExecutionContext(context.Background()); err == nil {
		t.Fatal("child envelope accepted replayed capability provenance")
	}
	// HTTPFS is never configured. The private reader remains usable after denied
	// raw-reader attempts, with the original policy and exact immutable capability.
	if _, err := executor.Execute(ctx, query.Request{Sources: []string{source.ID}, SQL: "SELECT count(*) FROM orders_object"}, &snapshotWorkerSink{}); err != nil {
		t.Fatal(err)
	}
	release()
	if _, err := executor.Execute(ctx, query.Request{Sources: []string{source.ID}, SQL: "SELECT count(*) FROM orders_object"}, &snapshotWorkerSink{}); err == nil {
		t.Fatal("child replayed an expired range capability")
	}
	requireObjectWorkerClean(t, executor, client)
}

func TestObjectSnapshotWorkerActualChildRejectsIntegrityAndRawBudget(t *testing.T) {
	for _, mode := range []string{"rows", "schema", "digest", "version", "raw-rows", "raw-bytes"} {
		t.Run(mode, func(t *testing.T) {
			executor, ctx, source, client, release := objectAccessFixture(t, 1)
			switch mode {
			case "rows":
				source.ObjectSnapshot.Parts[0].Rows++
			case "schema":
				source.ObjectSnapshot.SchemaSHA256 = strings.Repeat("f", 64)
			case "digest":
				client.corrupt.Store(true)
			case "version":
				client.wrongVersion.Store(true)
			case "raw-rows":
				source.ObjectSnapshot.Scan.MaxRows = 3
			case "raw-bytes":
				source.ObjectSnapshot.Scan.MaxBytes = 1024
			}
			sink := &snapshotWorkerSink{}
			_, err := executor.Execute(ctx, query.Request{Sources: []string{source.ID}, SQL: "SELECT count(*) FROM orders_object"}, sink)
			if err == nil || len(sink.values) != 0 {
				t.Fatalf("invalid object snapshot delivered a result: %v", err)
			}
			if strings.HasPrefix(mode, "raw-") && query.PublicError(err).Code != "RESOURCE_EXHAUSTED" {
				t.Fatalf("raw limit was not preserved across child IPC: %v", err)
			}
			requireObjectWorkerClean(t, executor, client)
			release()
		})
	}
}

func TestObjectSnapshotWorkerActualChildCancellationDrainsResources(t *testing.T) {
	for _, phase := range []string{"source-range", "result-delivery"} {
		t.Run(phase, func(t *testing.T) {
			executor, base, source, client, release := objectAccessFixture(t, 1)
			ctx, cancel := context.WithCancel(base)
			defer cancel()
			blocked := &snapshotWorkerSink{entered: make(chan struct{}), ctx: ctx}
			var sink query.Sink = blocked
			if phase == "source-range" {
				client.blocked.Store(true)
				sink = &snapshotWorkerSink{}
			} else {
				client.pause = make(chan struct{})
			}
			done := make(chan error, 1)
			go func() {
				_, err := executor.Execute(ctx, query.Request{Sources: []string{source.ID}, SQL: "SELECT id,amount FROM orders_object"}, sink)
				done <- err
			}()
			select {
			case <-client.started:
			case err := <-done:
				t.Fatalf("child stopped before cancellation phase: %v", err)
			case <-time.After(10 * time.Second):
				t.Fatal("actual child did not reach cancellation phase")
			}
			children := objectWorkerChildren(t, executor.Binary)
			if len(children) != 1 {
				t.Fatalf("expected one actual child, found %d", len(children))
			}
			if phase == "result-delivery" {
				// Capture the live child while it is reading. A tiny result may
				// finish writing IPC and exit before the parent sink is blocked.
				close(client.pause)
				select {
				case <-blocked.entered:
				case err := <-done:
					t.Fatalf("child stopped before result delivery: %v", err)
				case <-time.After(10 * time.Second):
					t.Fatal("actual child did not reach result delivery")
				}
			}
			cancel()
			select {
			case err := <-done:
				if err == nil || (!errors.Is(err, context.Canceled) && query.PublicError(err).Code != "CANCELLED") {
					t.Fatalf("child cancellation failed: %v", err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("cancelled child did not exit")
			}
			for _, pid := range children {
				requireProcessTerminated(t, pid)
			}
			release()
			requireObjectWorkerClean(t, executor, client)
			if client.closed.Load() != 1 {
				t.Fatal("cancelled operation retained parent range client")
			}
		})
	}
}

// An old child must refuse the new envelope before it can register raw ranges.
// Run this opt-in rollback control separately with an immutable previous binary.
func TestObjectSnapshotPreviousChildRefusesNewEnvelope(t *testing.T) {
	previous := os.Getenv("KELVO_TEST_PREVIOUS_SNAPSHOT_BINARY")
	if previous == "" {
		t.Skip("set KELVO_TEST_PREVIOUS_SNAPSHOT_BINARY for the actual rollback refusal control")
	}
	executor, ctx, source, client, release := objectAccessFixture(t, 1)
	defer release()
	request := query.Request{Sources: []string{source.ID}, SQL: "SELECT id,amount,observed,tiny FROM orders_object ORDER BY id"}
	current := &objectExactSink{}
	if _, err := executor.Execute(ctx, request, current); err != nil || len(current.values) != 3 {
		t.Fatalf("current child did not accept the guarded control: %v", err)
	}
	requireObjectWorkerClean(t, executor, client)
	before := client.calls.Load()
	executor.Binary = previous
	old := &objectExactSink{}
	stats, err := executor.Execute(ctx, request, old)
	if err == nil || query.PublicError(err).Code != "INVALID_ARGUMENT" || old.schema != nil || len(old.values) != 0 || stats.Rows != 0 {
		t.Fatalf("previous child failed to reject the envelope before Arrow output: %v", err)
	}
	if client.calls.Load() != before {
		t.Fatal("previous child accessed the object capability before rejecting new provenance")
	}
	requireObjectWorkerClean(t, executor, client)
}
