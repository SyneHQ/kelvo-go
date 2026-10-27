// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package flightsql

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/transport/httpapi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/flight"
	fsql "github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	innerToken = "inner-service-token-0123456789abcdef"
	adminKey   = "admin-key-0123456789abcdef0123456789"
	analystKey = "analyst-key-0123456789abcdef01234567"
)

type principalKey struct{}

// fakeExecutor stands in for the worker; the analyst principal gets a
// row/column policy on sales the way a guarded execution path would apply it.
type fakeExecutor struct{}

func typedBatch() arrow.RecordBatch {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "i8", Type: arrow.PrimitiveTypes.Int8, Nullable: true},
		{Name: "i16", Type: arrow.PrimitiveTypes.Int16, Nullable: true},
		{Name: "i32", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "i64", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "u64", Type: arrow.PrimitiveTypes.Uint64, Nullable: true},
		{Name: "dec", Type: &arrow.Decimal128Type{Precision: 38, Scale: 10}, Nullable: true},
		{Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
		{Name: "s", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	b.Field(0).(*array.Int8Builder).Append(-128)
	b.Field(1).(*array.Int16Builder).Append(-32768)
	b.Field(2).(*array.Int32Builder).Append(-2147483648)
	b.Field(3).(*array.Int64Builder).Append(-9223372036854775808)
	b.Field(4).(*array.Uint64Builder).Append(18446744073709551615)
	dec, _ := decimal128.FromString("1234567890123456789012345678.0123456789", 38, 10)
	b.Field(5).(*array.Decimal128Builder).Append(dec)
	b.Field(6).(*array.TimestampBuilder).Append(arrow.Timestamp(1700000000123456))
	b.Field(7).(*array.StringBuilder).Append("ü")
	for i := range b.Fields() {
		b.Field(i).AppendNull()
	}
	return b.NewRecordBatch()
}

func (fakeExecutor) Execute(ctx context.Context, r query.Request, sink query.Sink) (query.Stats, error) {
	emit := func(rec arrow.RecordBatch) error {
		defer rec.Release()
		if err := sink.Schema(rec.Schema()); err != nil {
			return err
		}
		return sink.Write(rec)
	}
	switch {
	case strings.Contains(r.SQL, "typed"):
		return query.Stats{}, emit(typedBatch())
	case strings.Contains(r.SQL, "slow"):
		<-ctx.Done()
		return query.Stats{}, ctx.Err()
	case strings.Contains(r.SQL, "partial"):
		if err := emit(typedBatch()); err != nil {
			return query.Stats{}, err
		}
		return query.Stats{}, query.NewError("QUERY_FAILED", "source failed mid-stream")
	case strings.Contains(r.SQL, "?"):
		values, err := r.Values()
		if err != nil {
			return query.Stats{}, err
		}
		b := array.NewInt64Builder(memory.DefaultAllocator)
		defer b.Release()
		b.Append(values[0].(int64))
		col := b.NewArray()
		defer col.Release()
		return query.Stats{}, emit(array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "v", Type: arrow.PrimitiveTypes.Int64}}, nil), []arrow.Array{col}, 1))
	default: // sales or orders table
		fields := []arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "region", Type: arrow.BinaryTypes.String}, {Name: "secret", Type: arrow.BinaryTypes.String}}
		ids, regions, secrets := []int64{1, 2, 3}, []string{"eu", "us", "us"}, []string{"a", "b", "c"}
		if ctx.Value(principalKey{}) == "analyst" {
			fields, ids, regions, secrets = fields[:2], ids[:1], regions[:1], nil
		}
		if strings.Contains(r.SQL, "LIMIT 0") {
			ids, regions, secrets = nil, nil, nil
		}
		b := array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema(fields, nil))
		defer b.Release()
		b.Field(0).(*array.Int64Builder).AppendValues(ids, nil)
		b.Field(1).(*array.StringBuilder).AppendValues(regions, nil)
		if len(fields) == 3 {
			b.Field(2).(*array.StringBuilder).AppendValues(secrets, nil)
		}
		return query.Stats{}, emit(b.NewRecordBatch())
	}
}

// fakeGateway models the cluster gateway's principal contract in front of
// the real HTTP API: per-key source grants, owner-bound handles and a
// principal on the execution context.
type fakeGateway struct {
	inner  http.Handler
	grants map[string]map[string]bool
	mu     sync.Mutex
	owner  map[string]string
}

func (g *fakeGateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	who := map[string]string{"Bearer " + adminKey: "admin", "Bearer " + analystKey: "analyst"}[r.Header.Get("Authorization")]
	if who == "" {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, `{"error":{"code":"UNAUTHENTICATED","message":"Authentication required"}}`)
		return
	}
	r.Header.Set("Authorization", "Bearer "+innerToken)
	r = r.WithContext(context.WithValue(r.Context(), principalKey{}, who))
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if r.Method == http.MethodPost && len(parts) == 2 {
		raw, _ := io.ReadAll(r.Body)
		var req query.Request
		_ = json.Unmarshal(raw, &req)
		for _, src := range req.Sources {
			if !g.grants[who][src] {
				w.WriteHeader(http.StatusForbidden)
				_, _ = io.WriteString(w, `{"error":{"code":"PERMISSION_DENIED","message":"Query access denied"}}`)
				return
			}
		}
		r.Body = io.NopCloser(bytes.NewReader(raw))
		rec := httptest.NewRecorder()
		g.inner.ServeHTTP(rec, r)
		var created struct{ ID string }
		_ = json.Unmarshal(rec.Body.Bytes(), &created)
		g.mu.Lock()
		g.owner[created.ID] = who
		g.mu.Unlock()
		w.WriteHeader(rec.Code)
		_, _ = w.Write(rec.Body.Bytes())
		return
	}
	if len(parts) >= 3 {
		g.mu.Lock()
		owner := g.owner[parts[2]]
		g.mu.Unlock()
		if owner != who {
			w.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(w, `{"error":{"code":"NOT_FOUND","message":"Query not found"}}`)
			return
		}
	}
	g.inner.ServeHTTP(w, r)
}

type fixture struct {
	client *fsql.Client
	server *Server
}

func start(t *testing.T, maxSessions int, ttl time.Duration) fixture {
	t.Helper()
	api, err := httpapi.New(fakeExecutor{}, httpapi.Options{Token: innerToken, Limits: query.DefaultLimits(), MaxQueries: 64, MaxConcurrent: 4, TTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = api.Close() })
	gw := &fakeGateway{inner: api, owner: map[string]string{}, grants: map[string]map[string]bool{
		"admin":   {"sales": true, "orders": true},
		"analyst": {"sales": true},
	}}
	cat := catalog.Config{Sources: []catalog.Source{{ID: "sales", Type: "csv"}, {ID: "orders", Type: "csv"}}}
	s, err := New(Options{Handler: gw, Catalog: cat, MaxSessions: maxSessions, TicketTTL: ttl})
	if err != nil {
		t.Fatal(err)
	}
	srv := s.NewGRPC(nil)
	if err := srv.Init("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve() }()
	t.Cleanup(srv.Shutdown)
	c, err := fsql.NewClient(srv.Addr().String(), nil, nil, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return fixture{client: c, server: s}
}

func as(key string) context.Context {
	return metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer "+key)
}

func readAll(ctx context.Context, c *fsql.Client, info *flight.FlightInfo) (*arrow.Schema, []arrow.RecordBatch, error) {
	r, err := c.DoGet(ctx, info.Endpoint[0].Ticket)
	if err != nil {
		return nil, nil, err
	}
	defer r.Release()
	var out []arrow.RecordBatch
	for r.Next() {
		b := r.RecordBatch()
		b.Retain()
		out = append(out, b)
	}
	return r.Schema(), out, r.Err()
}

func code(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if status.Code(err) != want {
		t.Fatalf("got %v, want %v", err, want)
	}
}

func TestExactTypesAndNulls(t *testing.T) {
	f := start(t, 8, time.Minute)
	ctx := as(adminKey)
	info, err := f.client.Execute(ctx, "SELECT * FROM typed")
	if err != nil {
		t.Fatal(err)
	}
	schema, batches, err := readAll(ctx, f.client, info)
	if err != nil {
		t.Fatal(err)
	}
	want := typedBatch()
	defer want.Release()
	if !schema.Equal(want.Schema()) || len(batches) != 1 || !array.RecordEqual(batches[0], want) {
		t.Fatalf("result differs:\n%v\n%v", schema, batches)
	}
	if batches[0].Column(5).(*array.Decimal128).Value(0).ToString(10) != "1234567890123456789012345678.0123456789" || batches[0].Column(7).NullN() != 1 {
		t.Fatal("decimal or NULL changed")
	}
}

func TestBasicAuthHandshake(t *testing.T) {
	f := start(t, 8, time.Minute)
	ctx, err := f.client.Client.AuthenticateBasicToken(context.Background(), "analyst", analystKey)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.client.Execute(ctx, "SELECT * FROM sales"); err != nil {
		t.Fatal(err)
	}
	_, err = f.client.Client.AuthenticateBasicToken(context.Background(), "analyst", "wrong-key")
	code(t, err, codes.Unauthenticated)
	_, err = f.client.Execute(context.Background(), "SELECT 1")
	code(t, err, codes.Unauthenticated)
}

func TestDeniedSourceAndPolicy(t *testing.T) {
	f := start(t, 8, time.Minute)
	ctx := as(analystKey)
	_, err := f.client.Execute(ctx, "SELECT * FROM orders")
	code(t, err, codes.PermissionDenied)
	info, err := f.client.Execute(ctx, `SELECT * FROM "sales"`)
	if err != nil {
		t.Fatal(err)
	}
	schema, batches, err := readAll(ctx, f.client, info)
	if err != nil {
		t.Fatal(err)
	}
	if schema.NumFields() != 2 || schema.HasField("secret") || len(batches) != 1 || batches[0].NumRows() != 1 {
		t.Fatalf("policy not applied: %v", schema)
	}
}

func TestMetadataShowsOnlyGrants(t *testing.T) {
	f := start(t, 8, time.Minute)
	ctx := as(analystKey)
	info, err := f.client.GetTables(ctx, &fsql.GetTablesOpts{IncludeSchema: true})
	if err != nil {
		t.Fatal(err)
	}
	_, batches, err := readAll(ctx, f.client, info)
	if err != nil {
		t.Fatal(err)
	}
	if len(batches) != 1 || batches[0].NumRows() != 1 || batches[0].Column(2).(*array.String).Value(0) != "sales" {
		t.Fatalf("tables = %v", batches)
	}
	schema, err := flight.DeserializeSchema(batches[0].Column(4).(*array.Binary).Value(0), memory.DefaultAllocator)
	if err != nil || schema.HasField("secret") {
		t.Fatalf("table schema = %v, %v", schema, err)
	}
	info, err = f.client.GetSqlInfo(ctx, []fsql.SqlInfo{fsql.SqlInfoFlightSqlServerReadOnly})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = readAll(ctx, f.client, info); err != nil {
		t.Fatal(err)
	}
}

func TestTicketsBoundToCallerAndExpire(t *testing.T) {
	f := start(t, 8, 200*time.Millisecond)
	info, err := f.client.Execute(as(adminKey), "SELECT * FROM sales")
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = readAll(as(analystKey), f.client, info)
	code(t, err, codes.NotFound)
	if _, _, err = readAll(as(adminKey), f.client, info); err != nil {
		t.Fatal(err)
	}
	_, _, err = readAll(as(adminKey), f.client, info)
	code(t, err, codes.NotFound) // single use
	info, err = f.client.Execute(as(adminKey), "SELECT * FROM sales")
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	_, _, err = readAll(as(adminKey), f.client, info)
	code(t, err, codes.NotFound)
}

func TestMaxSessions(t *testing.T) {
	f := start(t, 1, time.Minute)
	if _, err := f.client.Execute(as(adminKey), "SELECT * FROM sales"); err != nil {
		t.Fatal(err)
	}
	_, err := f.client.Execute(as(analystKey), "SELECT * FROM sales")
	code(t, err, codes.ResourceExhausted)
}

func TestWritesRejected(t *testing.T) {
	f := start(t, 8, time.Minute)
	ctx := as(adminKey)
	for _, sql := range []string{"INSERT INTO sales VALUES (1)", "SELECT 1; DROP TABLE sales", "WITH x AS (SELECT 1) DELETE FROM sales", "COPY sales TO 'x.csv'", "ATTACH 'x.db'", "SELECT 'unterminated"} {
		_, err := f.client.Execute(ctx, sql)
		code(t, err, codes.PermissionDenied)
		_, err = f.client.Prepare(ctx, sql)
		code(t, err, codes.PermissionDenied)
	}
	_, err := f.client.ExecuteUpdate(ctx, "DELETE FROM sales")
	code(t, err, codes.PermissionDenied)
	if _, err := f.client.Execute(ctx, "SELECT 'a;b', \"update\" FROM sales"); err != nil {
		t.Fatalf("quoted keyword rejected: %v", err)
	}
}

func TestPreparedTypedParameter(t *testing.T) {
	f := start(t, 8, time.Minute)
	ctx := as(adminKey)
	p, err := f.client.Prepare(ctx, "SELECT ? AS v")
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close(ctx)
	b := array.NewInt64Builder(memory.DefaultAllocator)
	b.Append(-9223372036854775807)
	col := b.NewArray()
	params := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "p", Type: arrow.PrimitiveTypes.Int64}}, nil), []arrow.Array{col}, 1)
	p.SetParameters(params)
	info, err := p.Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, batches, err := readAll(ctx, f.client, info)
	if err != nil || batches[0].Column(0).(*array.Int64).Value(0) != -9223372036854775807 {
		t.Fatalf("prepared result = %v, %v", batches, err)
	}
}

func TestCancellationAndTruncation(t *testing.T) {
	f := start(t, 8, time.Minute)
	ctx := as(adminKey)
	info, err := f.client.Execute(ctx, "SELECT slow")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, _, err := readAll(ctx, f.client, info); done <- err }()
	time.Sleep(100 * time.Millisecond)
	res, err := f.client.CancelFlightInfo(ctx, &flight.CancelFlightInfoRequest{Info: info})
	if err != nil || res.Status != flight.CancelStatusCancelled {
		t.Fatalf("cancel = %v, %v", res, err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("cancelled query reported success")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancellation did not stop the stream")
	}

	info, err = f.client.Execute(ctx, "SELECT slow")
	if err != nil {
		t.Fatal(err)
	}
	deadline, stop := context.WithTimeout(ctx, 100*time.Millisecond)
	defer stop()
	_, _, err = readAll(deadline, f.client, info)
	code(t, err, codes.DeadlineExceeded)

	info, err = f.client.Execute(ctx, "SELECT partial")
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = readAll(ctx, f.client, info)
	if err == nil || errors.Is(err, io.EOF) {
		t.Fatal("truncated result reported success")
	}
}
