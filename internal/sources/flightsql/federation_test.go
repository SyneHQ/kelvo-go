// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package flightsql

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	federationapi "github.com/SYNEHQ/kelvo-go/federation"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/flight"
	fsql "github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const federationDescribe = `SELECT * FROM "reporting"."events" WHERE 1 = 0`

func flightFederationSource() (federationapi.Source, federationapi.Table, federationapi.Limits) {
	l := query.DefaultLimits()
	return federationapi.Source{ID: "flight", Type: "arrow_flight",
			URLEnv: "KELVO_SOURCE_FEDERATION_FLIGHT_URL", TokenEnv: "KELVO_SOURCE_FEDERATION_FLIGHT_TOKEN",
			Options: map[string]string{"protocol": "flightsql", "federation_dialect": "ansi"}},
		federationapi.Table{Name: "events", Schema: "reporting", Table: "events"},
		federationapi.Limits{MaxRows: l.MaxRows, MaxBytes: l.MaxBytes, Timeout: l.Timeout, MemoryMB: l.MemoryMB, Threads: l.Threads}
}

func TestFlightFederationValidationPrecedesSecretsAndConstruction(t *testing.T) {
	source, table, limits := flightFederationSource()
	t.Setenv(source.URLEnv, "invalid endpoint must not be read by Validate")
	if err := (FederationDriver{}).Validate(source, table); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*federationapi.Source, *federationapi.Table){
		func(s *federationapi.Source, _ *federationapi.Table) { s.Type = "postgres" },
		func(s *federationapi.Source, _ *federationapi.Table) { s.DSNEnv = "KELVO_SOURCE_OTHER_DSN" },
		func(s *federationapi.Source, _ *federationapi.Table) { s.TokenEnv = "" },
		func(s *federationapi.Source, _ *federationapi.Table) { s.Options["federation_dialect"] = "mysql" },
		func(s *federationapi.Source, _ *federationapi.Table) { s.Options["extra"] = "ignored" },
		func(_ *federationapi.Source, table *federationapi.Table) { table.Database = "other" },
		func(_ *federationapi.Source, table *federationapi.Table) { table.Schema = "" },
		func(_ *federationapi.Source, table *federationapi.Table) { table.Table = "events; SELECT secret" },
	} {
		source, table, _ := flightFederationSource()
		change(&source, &table)
		if err := (FederationDriver{}).Validate(source, table); err == nil {
			t.Fatal("invalid federation source accepted")
		}
		if relation, err := (FederationDriver{}).Open(context.Background(), source, table, limits); err == nil || relation != nil {
			t.Fatal("failed public Open returned an error-free or non-nil relation")
		}
		if _, err := openFederation(context.Background(), source, table, limits,
			func(catalog.Config, query.Limits) (*Engine, error) {
				t.Fatal("invalid configuration reached native construction")
				return nil, nil
			}); err == nil {
			t.Fatal("invalid federation source opened")
		}
	}
	invalidLimits := limits
	invalidLimits.MaxRows = 0
	if relation, err := (FederationDriver{}).Open(context.Background(), source, table, invalidLimits); err == nil || relation != nil {
		t.Fatal("invalid limits returned an error-free or non-nil relation")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := openFederation(canceled, source, table, limits, func(catalog.Config, query.Limits) (*Engine, error) {
		t.Fatal("canceled discovery reached native construction")
		return nil, nil
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("terminal cancellation was lost: %v", err)
	}
	capability, known := federationapi.InspectCapabilities(FederationDriver{})
	if !known || !capability.Projection || capability.NullPredicates || capability.Conjunction || capability.Disjunction || len(capability.Comparisons) != 0 {
		t.Fatal("projection-only driver advertised predicate support")
	}
}

// This fixture checks exact compiler output and TLS/Arrow ownership. The
// DuckDB integration fixture separately exercises a SQL-backed Flight service.
type federationFlightFixture struct {
	fsql.BaseServer
	record         arrow.RecordBatch
	pool           *x509.CertPool
	queries        atomic.Int64
	gets           atomic.Int64
	drift          atomic.Bool
	discoveryRows  atomic.Bool
	blockDiscovery atomic.Bool
	blockScan      atomic.Bool
	entered        chan struct{}
	canceled       chan struct{}
	enterOnce      sync.Once
	cancelOnce     sync.Once
}

func newFederationFlightFixture(t *testing.T) *federationFlightFixture {
	t.Helper()
	f := &federationFlightFixture{entered: make(chan struct{}), canceled: make(chan struct{})}
	metadata := arrow.MetadataFrom(map[string]string{"dataset": "events"})
	fieldMetadata := arrow.MetadataFrom(map[string]string{"classification": "fixture"})
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true, Metadata: fieldMetadata},
		{Name: "amount", Type: &arrow.Decimal128Type{Precision: 24, Scale: 4}, Nullable: true, Metadata: fieldMetadata},
		{Name: "occurred_at", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
		{Name: "label", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: `quoted"column`, Type: arrow.PrimitiveTypes.Uint64, Nullable: true},
	}, &metadata)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	b.Field(0).(*array.Int64Builder).AppendValues([]int64{-9223372036854775808, 9223372036854775807, 0}, []bool{true, true, false})
	amount, err := decimal128.FromString("12345678901234567890.1234", 24, 4)
	if err != nil {
		t.Fatal(err)
	}
	b.Field(1).(*array.Decimal128Builder).AppendValues([]decimal128.Num{amount, decimal128.FromI64(-1), {}}, []bool{true, true, false})
	b.Field(2).(*array.TimestampBuilder).AppendValues([]arrow.Timestamp{-1, 1735689600123456, 0}, []bool{true, true, false})
	b.Field(3).(*array.StringBuilder).AppendValues([]string{strings.Repeat("x", 2048), "small", ""}, []bool{true, true, false})
	b.Field(4).(*array.Uint64Builder).AppendValues([]uint64{18446744073709551615, 1, 0}, []bool{true, true, false})
	f.record = b.NewRecordBatch()
	b.Release()
	cert, pool := testCert(t)
	f.pool = pool
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := flight.NewServerWithMiddleware(nil, grpc.WaitForHandlers(true), grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})))
	server.InitListener(listener)
	server.RegisterFlightService(fsql.NewFlightServer(f))
	go server.Serve()
	t.Cleanup(func() { server.Shutdown(); f.record.Release() })
	source, _, _ := flightFederationSource()
	t.Setenv(source.URLEnv, "grpcs://"+listener.Addr().String())
	t.Setenv(source.TokenEnv, "flight-federation-fixture")
	return f
}

func (f *federationFlightFixture) authorized(ctx context.Context) bool {
	m, _ := metadata.FromIncomingContext(ctx)
	values := m.Get("authorization")
	return len(values) == 1 && values[0] == "Bearer flight-federation-fixture"
}

func (f *federationFlightFixture) projection(sql string) ([]int, error) {
	switch sql {
	case federationDescribe:
		return []int{0, 1, 2, 3, 4}, nil
	case `SELECT "amount", "id", "occurred_at", "quoted""column" FROM "reporting"."events"`:
		return []int{1, 0, 2, 4}, nil
	case `SELECT "id" FROM "reporting"."events"`:
		return []int{0}, nil
	case `SELECT "label" FROM "reporting"."events"`:
		return []int{3}, nil
	default:
		return nil, status.Error(codes.InvalidArgument, "unexpected fixture SQL")
	}
}

func (f *federationFlightFixture) schema(sql string, indices []int) *arrow.Schema {
	fields := make([]arrow.Field, len(indices))
	for i, index := range indices {
		fields[i] = f.record.Schema().Field(index)
	}
	metadata := f.record.Schema().Metadata()
	if f.drift.Load() && sql != federationDescribe {
		metadata = arrow.MetadataFrom(map[string]string{"dataset": "changed"})
	}
	return arrow.NewSchema(fields, &metadata)
}

func (f *federationFlightFixture) GetFlightInfoStatement(ctx context.Context, request fsql.StatementQuery, descriptor *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	if !f.authorized(ctx) {
		return nil, status.Error(codes.Unauthenticated, "fixture authentication required")
	}
	f.queries.Add(1)
	sql := request.GetQuery()
	indices, err := f.projection(sql)
	if err != nil {
		return nil, err
	}
	if f.blockDiscovery.Load() && sql == federationDescribe {
		f.enterOnce.Do(func() { close(f.entered) })
		<-ctx.Done()
		f.cancelOnce.Do(func() { close(f.canceled) })
		return nil, ctx.Err()
	}
	ticket, err := fsql.CreateStatementQueryTicket([]byte(sql))
	if err != nil {
		return nil, err
	}
	return &flight.FlightInfo{FlightDescriptor: descriptor, Schema: flight.SerializeSchema(f.schema(sql, indices), memory.DefaultAllocator),
		Endpoint: []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: ticket}}}}, nil
}

func (f *federationFlightFixture) DoGetStatement(ctx context.Context, ticket fsql.StatementQueryTicket) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	if !f.authorized(ctx) {
		return nil, nil, status.Error(codes.Unauthenticated, "fixture authentication required")
	}
	f.gets.Add(1)
	sql := string(ticket.GetStatementHandle())
	indices, err := f.projection(sql)
	if err != nil {
		return nil, nil, err
	}
	schema := f.schema(sql, indices)
	ch := make(chan flight.StreamChunk, 1)
	if f.blockScan.Load() && sql != federationDescribe {
		f.enterOnce.Do(func() { close(f.entered) })
		go func() {
			<-ctx.Done()
			close(ch)
			f.cancelOnce.Do(func() { close(f.canceled) })
		}()
		return schema, ch, nil
	}
	columns := make([]arrow.Array, len(indices))
	for i, index := range indices {
		columns[i] = f.record.Column(index)
	}
	record := array.NewRecordBatch(schema, columns, f.record.NumRows())
	if sql == federationDescribe && !f.discoveryRows.Load() {
		empty := record.NewSlice(0, 0)
		record.Release()
		record = empty
	}
	ch <- flight.StreamChunk{Data: record}
	close(ch)
	return schema, ch, nil
}

func (f *federationFlightFixture) open(ctx context.Context, limits federationapi.Limits) (*federationRelation, error) {
	source, table, _ := flightFederationSource()
	return openFederation(ctx, source, table, limits, func(config catalog.Config, limits query.Limits) (*Engine, error) {
		engine, err := New(config, limits)
		if err == nil {
			engine.tlsConfig = func(ep endpoint) *tls.Config {
				return &tls.Config{MinVersion: tls.VersionTLS12, ServerName: ep.host, RootCAs: f.pool}
			}
		}
		return engine, err
	})
}

type federationCapture struct {
	schema *arrow.Schema
	rows   int64
	record arrow.RecordBatch
	err    error
}

func (s *federationCapture) Schema(schema *arrow.Schema) error { s.schema = schema; return nil }
func (s *federationCapture) Write(record arrow.RecordBatch) error {
	if s.err != nil {
		return s.err
	}
	s.rows += record.NumRows()
	if s.record != nil {
		s.record.Release()
	}
	record.Retain()
	s.record = record
	return nil
}
func (s *federationCapture) release() {
	if s.record != nil {
		s.record.Release()
		s.record = nil
	}
}

func TestFlightFederationTLSProjectionExactTypesAndOwnership(t *testing.T) {
	f := newFederationFlightFixture(t)
	_, _, limits := flightFederationSource()
	r, err := f.open(context.Background(), limits)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if !federationSameSchema(r.Schema(), f.record.Schema()) {
		t.Fatal("discovery changed schema")
	}
	sink := &federationCapture{}
	defer sink.release()
	stats, err := r.Scan(context.Background(), federationapi.ScanPlan{Columns: []string{"amount", "id", "occurred_at", `quoted"column`}}, sink)
	if err != nil || sink.rows != 3 || stats.SourceWireBytes != 0 {
		t.Fatalf("projection failed: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if sink.record.Column(0).(*array.Decimal128).Value(0) != f.record.Column(1).(*array.Decimal128).Value(0) ||
		sink.record.Column(1).(*array.Int64).Value(1) != 9223372036854775807 ||
		sink.record.Column(2).(*array.Timestamp).Value(1) != 1735689600123456 ||
		sink.record.Column(3).(*array.Uint64).Value(0) != 18446744073709551615 || !sink.record.Column(3).IsNull(2) {
		t.Fatal("retained exact Arrow values changed after Close")
	}
	if !sink.schema.Field(0).Equal(f.record.Schema().Field(1)) || !sink.schema.Metadata().Equal(f.record.Schema().Metadata()) {
		t.Fatal("projection lost field or schema metadata")
	}
	if f.queries.Load() != 2 || f.gets.Load() != 2 {
		t.Fatal("discovery or scan unexpectedly retried")
	}
}

func TestFlightFederationRefusesPlansBeforeSourceIO(t *testing.T) {
	f := newFederationFlightFixture(t)
	_, _, limits := flightFederationSource()
	r, err := f.open(context.Background(), limits)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	for _, plan := range []federationapi.ScanPlan{
		{}, {Columns: []string{"missing"}},
		{Columns: []string{"id"}, Filters: []federationapi.Filter{{Kind: "is_null", Column: "id"}}},
		{Columns: []string{"id"}, Filters: []federationapi.Filter{{Kind: "comparison", Column: "id", Type: "int64", Op: "eq", Value: "1"}}},
	} {
		if _, err := r.Scan(context.Background(), plan, &federationCapture{}); !errors.Is(err, federationapi.ErrUnsupported) {
			t.Fatalf("required operation was not explicitly refused: %v", err)
		}
	}
	if f.queries.Load() != 1 || f.gets.Load() != 1 {
		t.Fatal("unsupported plan reached source")
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Scan(context.Background(), federationapi.ScanPlan{Columns: []string{"id"}}, &federationCapture{}); err == nil || f.queries.Load() != 1 {
		t.Fatal("closed relation reached source")
	}
}

func TestFlightFederationSchemaDriftLimitsAndSinkFailure(t *testing.T) {
	for _, kind := range []string{"schema", "rows", "bytes", "sink", "discovery_rows"} {
		t.Run(kind, func(t *testing.T) {
			f := newFederationFlightFixture(t)
			_, _, limits := flightFederationSource()
			f.drift.Store(kind == "schema")
			f.discoveryRows.Store(kind == "discovery_rows")
			columns := []string{"id"}
			if kind == "rows" {
				limits.MaxRows = 1
			}
			if kind == "bytes" {
				limits.MaxBytes, columns = 1024, []string{"label"}
			}
			r, err := f.open(context.Background(), limits)
			if kind == "discovery_rows" {
				if err == nil || r != nil {
					t.Fatal("discovery accepted source data")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			sink := &federationCapture{}
			defer sink.release()
			if kind == "sink" {
				sink.err = errors.New("fixture consumer stopped")
			}
			_, err = r.Scan(context.Background(), federationapi.ScanPlan{Columns: columns}, sink)
			if err == nil || sink.rows != 0 || f.queries.Load() != 2 || (kind == "schema" && sink.schema != nil) {
				t.Fatal("failed scan exposed data or retried")
			}
		})
	}
}

func waitFlightFederation(t *testing.T, signal <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(10 * time.Second):
		t.Fatal(message)
	}
}

func TestFlightFederationCancellationAndCloseOwnOperations(t *testing.T) {
	for _, kind := range []string{"discovery", "scan_cancel", "scan_close"} {
		t.Run(kind, func(t *testing.T) {
			f := newFederationFlightFixture(t)
			_, _, limits := flightFederationSource()
			f.blockDiscovery.Store(kind == "discovery")
			f.blockScan.Store(kind != "discovery")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			finished := make(chan error, 1)
			if kind == "discovery" {
				go func() {
					r, err := f.open(ctx, limits)
					if r != nil {
						_ = r.Close()
					}
					finished <- err
				}()
				waitFlightFederation(t, f.entered, "discovery did not reach fixture")
				cancel()
			} else {
				r, err := f.open(context.Background(), limits)
				if err != nil {
					t.Fatal(err)
				}
				defer r.Close()
				go func() {
					_, err := r.Scan(ctx, federationapi.ScanPlan{Columns: []string{"id"}}, &federationCapture{})
					finished <- err
				}()
				waitFlightFederation(t, f.entered, "scan did not reach fixture")
				if _, err := r.Scan(context.Background(), federationapi.ScanPlan{Columns: []string{"id"}}, &federationCapture{}); err == nil || f.queries.Load() != 2 {
					t.Fatal("one relation ran overlapping scans")
				}
				if kind == "scan_close" {
					closed := make(chan struct{})
					go func() { _ = r.Close(); close(closed) }()
					waitFlightFederation(t, closed, "Close did not finish canceled source cleanup")
				} else {
					cancel()
				}
			}
			select {
			case err := <-finished:
				if err == nil {
					t.Fatal("cancelled source operation succeeded")
				}
			case <-time.After(10 * time.Second):
				t.Fatal("cancelled source operation remained active")
			}
			waitFlightFederation(t, f.canceled, "source did not observe cancellation")
		})
	}
}

func TestFlightFederationConcurrentRelationsRemainIndependent(t *testing.T) {
	f := newFederationFlightFixture(t)
	_, _, limits := flightFederationSource()
	left, err := f.open(context.Background(), limits)
	if err != nil {
		t.Fatal(err)
	}
	defer left.Close()
	right, err := f.open(context.Background(), limits)
	if err != nil {
		t.Fatal(err)
	}
	defer right.Close()
	results := make(chan error, 2)
	for _, relation := range []*federationRelation{left, right} {
		go func() {
			sink := &federationCapture{}
			defer sink.release()
			_, err := relation.Scan(context.Background(), federationapi.ScanPlan{Columns: []string{"id"}}, sink)
			if err == nil && sink.rows != 3 {
				err = errors.New("independent relation lost rows")
			}
			results <- err
		}()
	}
	for range 2 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if f.queries.Load() != 4 || f.gets.Load() != 4 {
		t.Fatal("independent discovery/scan query counts changed")
	}
}
