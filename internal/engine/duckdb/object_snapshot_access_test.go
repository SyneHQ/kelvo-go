//go:build linux && duckdb_arrow && duckbridge && cgo

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package duckdb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"reflect"
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

func objectAccessFixture(t *testing.T, parts int) (*Engine, context.Context, catalog.Source, *objectAccessClient, func()) {
	t.Helper()
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

func TestObjectSnapshotEngineExactTypesNullsAndMultipart(t *testing.T) {
	for _, parts := range []int{1, 2} {
		t.Run(fmt.Sprintf("parts-%d", parts), func(t *testing.T) {
			engine, ctx, source, client, release := objectAccessFixture(t, parts)
			sink := &objectExactSink{}
			stats, err := engine.Execute(ctx, query.Request{Sources: []string{source.ID}, SQL: "SELECT id,amount,observed,tiny FROM orders_object ORDER BY id"}, sink)
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
				t.Fatalf("exact values or NULLs changed: got%v want%v", sink.values, want)
			}
			if sink.schema.Metadata().Len() != 0 || !arrow.TypeEqual(sink.schema.Field(1).Type, &arrow.Decimal128Type{Precision: 30, Scale: 4}) {
				t.Fatal("private metadata or decimal schema changed")
			}
			stamp, ok := sink.schema.Field(2).Type.(*arrow.TimestampType)
			if !ok || stamp.Unit != arrow.Microsecond {
				t.Fatal("timestamp precision changed")
			}
			for _, field := range sink.schema.Fields() {
				if field.Metadata.Len() != 0 {
					t.Fatal("private field metadata escaped")
				}
			}
			if stats.Federation != nil || stats.ScanDiagnostics != nil || stats.SourceWireBytes != 0 || client.calls.Load() == 0 {
				t.Fatal("raw diagnostics escaped or range bridge not exercised")
			}
			release()
			if client.active.Load() != 0 || client.bodies.Load() != 0 || client.closed.Load() != 1 {
				t.Fatal("parent range resources survived release")
			}
		})
	}
}

func TestObjectSnapshotEngineJoinsLocalAndCallback(t *testing.T) {
	engine, ctx, object, _, _ := objectAccessFixture(t, 2)
	engine.limits.Threads = 3 // One independent scan slot for each joined relation.
	_, localCtx, _, local, _, _ := snapshotAccessFixture(t)
	policy, _ := access.PolicyFromContext(ctx)
	localPolicy, _ := access.PolicyFromContext(localCtx)
	policy.Sources[local.ID] = localPolicy.Sources[local.ID]
	policy.Sources["warehouse"] = access.SourcePolicy{Tables: map[string]access.TablePolicy{"customers": {Columns: []string{"id"}, Rows: &access.Predicate{Kind: "comparison", Column: "tenant_id", Type: "int64", Op: "eq", Value: "7"}}}}
	ctx, err := access.WithPolicy(context.Background(), policy)
	if err != nil {
		t.Fatal(err)
	}
	engine.config.Sources = append(engine.config.Sources, local, catalog.Source{ID: "warehouse", Type: "access_fixture", Federation: &catalog.FederationConfig{Tables: []catalog.FederationTable{{Name: "customers", Table: "customers"}}}})
	sink := &accessValueSink{}
	_, err = engine.Execute(ctx, query.Request{Sources: []string{object.ID, local.ID, "warehouse"}, SQL: "WITH joined AS (SELECT o.id,l.amount FROM orders_object o JOIN orders_fast l USING(id) JOIN warehouse.customers c USING(id)) SELECT count(*)::BIGINT,sum(amount)::BIGINT FROM joined"}, sink)
	if err != nil || !reflect.DeepEqual(sink.values, [][]int64{{4, 18}}) {
		t.Fatalf("mixed object/local/callback join: %v %v", sink.values, err)
	}
}

func TestObjectSnapshotEngineDeniesRawReadersAndReplay(t *testing.T) {
	engine, ctx, source, client, release := objectAccessFixture(t, 1)
	for _, sql := range []string{
		"SELECT tenant_id FROM orders_object", "SELECT * FROM read_parquet('" + source.Path + "')", "SELECT * FROM parquet_scan('" + source.Path + "')",
		"SELECT * FROM read_blob('" + source.Path + "')", "SELECT * FROM parquet_metadata('" + source.Path + "')", "SELECT * FROM '" + source.Path + "'",
		"LOAD httpfs", "INSTALL httpfs", "SET enable_external_access=true", "SELECT * FROM kelvo_arrow_scan_1(NULL,NULL,NULL)",
	} {
		sink := &accessValueSink{}
		_, err := engine.Execute(ctx, query.Request{Sources: []string{source.ID}, SQL: sql}, sink)
		if err == nil || sink.rows != 0 || strings.Contains(err.Error(), source.Path) || strings.Contains(err.Error(), "object-private-metadata") {
			t.Fatalf("raw reader bypass or detail disclosure: %v", err)
		}
	}
	// The extension directory is empty throughout; a valid guarded query still
	// works, so rejection above does not rely on downloading/loading HTTPFS.
	if _, err := engine.Execute(ctx, query.Request{Sources: []string{source.ID}, SQL: "SELECT count(*) FROM orders_object"}, &accessValueSink{}); err != nil {
		t.Fatal(err)
	}
	_, _, another, _, _ := objectAccessFixture(t, 1)
	another.ObjectSnapshot = source.ObjectSnapshot
	if _, err := New(catalog.Config{Sources: []catalog.Source{another}}, engine.limits); err == nil {
		t.Fatal("old descriptor replayed against a fresh capability")
	}
	release()
	if _, err := engine.Execute(ctx, query.Request{Sources: []string{source.ID}, SQL: "SELECT count(*) FROM orders_object"}, &accessValueSink{}); err == nil {
		t.Fatal("expired parent capability replayed")
	}
	if client.bodies.Load() != 0 {
		t.Fatal("replay retained upstream bodies")
	}
}

func TestObjectSnapshotEngineBudgetsIntegrityAndCancellation(t *testing.T) {
	for _, mode := range []string{"raw-rows", "raw-bytes", "rows", "schema", "digest", "version", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			engine, ctx, source, client, release := objectAccessFixture(t, 1)
			switch mode {
			case "raw-rows":
				source.ObjectSnapshot.Scan.MaxRows = 3
			case "raw-bytes":
				source.ObjectSnapshot.Scan.MaxBytes = 1024
			case "rows":
				source.ObjectSnapshot.Parts[0].Rows++
			case "schema":
				source.ObjectSnapshot.SchemaSHA256 = strings.Repeat("f", 64)
			case "digest":
				client.corrupt.Store(true)
			case "version":
				client.wrongVersion.Store(true)
			case "cancel":
				client.blocked.Store(true)
			}
			childCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			var sink accessValueSink
			done := make(chan error, 1)
			go func() {
				_, err := engine.Execute(childCtx, query.Request{Sources: []string{source.ID}, SQL: "SELECT count(*) FROM orders_object"}, &sink)
				done <- err
			}()
			if mode == "cancel" {
				select {
				case <-client.started:
					cancel()
				case err := <-done:
					t.Fatalf("query failed before range: %v", err)
				case <-time.After(5 * time.Second):
					t.Fatal("range did not start")
				}
			}
			select {
			case err := <-done:
				if err == nil || sink.rows != 0 {
					t.Fatalf("invalid guarded source produced rows: %v", err)
				}
				if strings.HasPrefix(mode, "raw-") && query.PublicError(err).Code != "RESOURCE_EXHAUSTED" {
					t.Fatalf("raw budget had unexpected error: %v", err)
				}
				if mode == "cancel" && !errors.Is(err, context.Canceled) && query.PublicError(err).Code != "CANCELLED" {
					t.Fatalf("cancel had unexpected error: %v", err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("guarded read did not terminate")
			}
			release()
			if client.active.Load() != 0 || client.bodies.Load() != 0 || client.closed.Load() != 1 {
				t.Fatal("failed query left parent range state")
			}
		})
	}
}
