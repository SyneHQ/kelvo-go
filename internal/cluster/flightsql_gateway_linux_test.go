//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/transport/flightsql"
	"github.com/SYNEHQ/kelvo-go/internal/worker"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	fsql "github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Flight SQL over the production gateway: file-key principals, a real node and
// a real sandboxed worker enforcing a guarded-snapshot row/column policy. Job
// coordination uses the in-memory store; no NATS broker is involved.
func TestFlightSQLThroughGatewayPrincipals(t *testing.T) {
	binary, launcher := os.Getenv("KELVO_TEST_SNAPSHOT_BINARY"), os.Getenv("KELVO_TEST_SNAPSHOT_SANDBOX")
	if binary == "" || launcher == "" {
		t.Skip("set KELVO_TEST_SNAPSHOT_BINARY and KELVO_TEST_SNAPSHOT_SANDBOX for the gateway Flight SQL gate")
	}
	config, p := snapshotPrincipalFixture(t)
	// analyst keeps the fixture's column/row policy; reports gets no grant on orders_fast.
	p.Access.Principals["reports"] = PrincipalGrant{Kind: "service", FederatedSources: []string{"daily_sales"}}
	p.Workers, p.MaxQueries = map[string]int{"a1": 4}, 64
	p.JobTTL, p.LeaseDuration = 2*time.Minute, 10*time.Second
	if err := ValidatePolicy(p); err != nil {
		t.Fatal(err)
	}
	engine, err := worker.New(config, p.Limits)
	if err != nil {
		t.Fatal(err)
	}
	engine.Binary, engine.SandboxPath = binary, launcher
	store := newMemoryJobStore(p)
	node, err := NewNode(NodeConfig{Policy: p, WorkerID: "a1", SandboxPath: launcher}, store, engine)
	if err != nil {
		t.Fatal("start node:", err)
	}
	gatewayTLS, workersTLS := protectedQueryTLS(t)
	serverTLS, err := BuildServerTLS(workersTLS["a"], WorkerIdentity("a", "a1"), GatewayIdentity)
	if err != nil {
		t.Fatal(err)
	}
	nodeServer := httptest.NewUnstartedServer(node)
	nodeServer.TLS = serverTLS
	nodeServer.StartTLS()
	analystKey, reportsKey := strings.Repeat("k", 40), strings.Repeat("r", 40)
	keys := filepath.Join(t.TempDir(), "keys.yml")
	doc := "version: 2\nrevision: 1\nprincipals:\n  a:\n    analyst:\n      - " + analystKey + "\n    reports:\n      - " + reportsKey + "\n"
	if err := os.WriteFile(keys, []byte(doc), 0600); err != nil {
		t.Fatal(err)
	}
	gateway, err := NewGateway(GatewayConfig{WorkerTLS: gatewayTLS, MaxHTTPRequests: 64, Authentication: &GatewayAuthenticationConfig{KeysFile: keys},
		Tenants: []TenantConfig{{Policy: p, Workers: []Endpoint{{ID: "a1", URL: nodeServer.URL}}}}}, map[string]Store{"a": store})
	if err != nil {
		t.Fatal("start gateway:", err)
	}
	t.Cleanup(func() { _ = gateway.Close(); nodeServer.Close(); _ = node.Close() })

	// TLS Flight SQL listener over the gateway handler.
	fs, err := flightsql.New(flightsql.Options{Handler: gateway, Catalog: config, MaxSessions: 8, TicketTTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	cert, err := tls.LoadX509KeyPair(gatewayTLS.CertFile, gatewayTLS.KeyFile)
	if err != nil {
		t.Fatal(err)
	}
	srv := fs.NewGRPC(&tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})
	if err := srv.Init("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve() }()
	t.Cleanup(srv.Shutdown)
	pemCA, err := os.ReadFile(gatewayTLS.CAFile)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(pemCA)
	client, err := fsql.NewClient(srv.Addr().String(), nil, nil, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{RootCAs: roots, ServerName: "127.0.0.1", MinVersion: tls.VersionTLS12})))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })

	as := func(key string) context.Context {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		t.Cleanup(cancel)
		return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+key)
	}
	read := func(ctx context.Context, info *flight.FlightInfo) (*arrow.Schema, []arrow.RecordBatch, error) {
		r, err := client.DoGet(ctx, info.Endpoint[0].Ticket)
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
	// The node probes its worker child before serving; retry only Unavailable.
	waitReady := time.Now().Add(30 * time.Second)
	var schema *arrow.Schema
	var batches []arrow.RecordBatch
	for {
		var info *flight.FlightInfo
		info, err = client.Execute(as(analystKey), "SELECT * FROM orders_fast ORDER BY id")
		if err == nil {
			schema, batches, err = read(as(analystKey), info)
		}
		if err == nil || status.Code(err) != codes.Unavailable || time.Now().After(waitReady) {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if err != nil {
		store.mu.Lock()
		for id, j := range store.jobs {
			t.Logf("job %s state=%s owner=%q worker=%q err=%+v", id, j.Job.State, j.Job.Owner, j.Job.WorkerID, j.Job.Error)
		}
		store.mu.Unlock()
		t.Fatal("analyst query:", err)
	}
	if schema.NumFields() != 2 || schema.Field(0).Name != "id" || schema.Field(1).Name != "amount" {
		t.Fatalf("column policy not applied: %v", schema)
	}
	var rows int64
	for _, b := range batches {
		ids, amounts := b.Column(0).(*array.Int64), b.Column(1).(*array.Int64)
		for i := 0; i < int(b.NumRows()); i++ {
			id := rows * 2 // only account 42 (even ids) survives the row policy
			if ids.Value(i) != id || amounts.IsNull(i) != (id%11 == 0) || (!amounts.IsNull(i) && amounts.Value(i) != id*10) {
				t.Fatalf("row %d changed: id=%d", rows, ids.Value(i))
			}
			rows++
		}
		b.Release()
	}
	if rows != 512 {
		t.Fatalf("row policy rows = %d", rows)
	}
	if info, err := client.Execute(as(analystKey), "SELECT secret FROM orders_fast"); err == nil {
		if _, _, err = read(as(analystKey), info); err == nil {
			t.Fatal("hidden column readable")
		}
	}

	// reports is authenticated but has no grant on orders_fast.
	_, err = client.Execute(as(reportsKey), "SELECT * FROM orders_fast")
	if c := status.Code(err); c != codes.PermissionDenied {
		t.Fatalf("denied principal got %v", err)
	}
	_, err = client.Execute(as(strings.Repeat("x", 40)), "SELECT * FROM orders_fast")
	if c := status.Code(err); c != codes.Unauthenticated {
		t.Fatalf("unknown key got %v", err)
	}

	// Tickets are bound to the analyst's key.
	info, err := client.Execute(as(analystKey), "SELECT id FROM orders_fast")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = read(as(reportsKey), info); status.Code(err) != codes.NotFound {
		t.Fatalf("foreign ticket got %v", err)
	}
	if _, _, err = read(as(analystKey), info); err != nil {
		t.Fatal(err)
	}

	// Metadata shows only granted tables, with policy-shaped columns.
	tables := func(key string) []arrow.RecordBatch {
		info, err := client.GetTables(as(key), &fsql.GetTablesOpts{IncludeSchema: true})
		if err != nil {
			t.Fatal(err)
		}
		_, out, err := read(as(key), info)
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	got := tables(analystKey)
	if len(got) != 1 || got[0].NumRows() != 1 || got[0].Column(2).(*array.String).Value(0) != "orders_fast" {
		t.Fatalf("analyst tables = %v", got)
	}
	ts, err := flight.DeserializeSchema(got[0].Column(4).(*array.Binary).Value(0), memory.DefaultAllocator)
	if err != nil || ts.NumFields() != 2 || ts.HasField("secret") || ts.HasField("account_id") {
		t.Fatalf("analyst table schema = %v, %v", ts, err)
	}
	if got := tables(reportsKey); len(got) != 1 || got[0].NumRows() != 0 {
		t.Fatalf("denied principal sees tables: %v", got)
	}
}
