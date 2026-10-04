//go:build linux && duckdb_arrow && duckbridge && cgo

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package duckdb

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql"
	"database/sql/driver"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	fsql "github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	duck "github.com/duckdb/duckdb-go/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Each server owns a separate real SQL database. Requests are executed, including
// projection and empty discovery; no response is selected by exact SQL text.
type flightSQLFixture struct {
	fsql.BaseServer
	db          *sql.DB
	token       string
	mu          sync.Mutex
	statements  []string
	streams     sync.WaitGroup
	active      atomic.Int32
	blockScans  atomic.Bool
	blocked     atomic.Int32
	blockStarts atomic.Int32
}

func flightFixtureDB(t *testing.T, tables ...string) *sql.DB {
	t.Helper()
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	t.Cleanup(func() { _ = db.Close() })
	statements := []string{"SET threads=1", "SET memory_limit='64MB'", "CREATE SCHEMA analytics"}
	for _, table := range tables {
		switch table {
		case "orders":
			statements = append(statements,
				`CREATE TABLE analytics.orders (id BIGINT, customer_id BIGINT, tenant_id BIGINT, small_value SMALLINT, big_id BIGINT, uint_id UBIGINT, amount DECIMAL(28,9), event_day DATE, label VARCHAR, payload VARCHAR)`,
				`INSERT INTO analytics.orders VALUES
 (1,1,7,-32768,9007199254740993,18446744073709551615,9007199254740993.123456789,DATE '1969-12-31','ride',repeat('x',4096)),
 (2,1,7,32767,-9007199254740993,9007199254740993,0.000000001,DATE '1970-01-01','ride',repeat('x',4096)),
 (3,2,8,0,-9223372036854775807,0,2.250000000,DATE '2024-02-29','other',repeat('x',4096)),
 (4,3,7,NULL,9223372036854775807,NULL,NULL,NULL,NULL,repeat('x',4096)),
 (5,4,8,1,-9223372036854775808,1,99.000000000,DATE '2025-01-01','excluded',repeat('x',4096)),
 (6,2,7,-1,0,2,-0.500000000,DATE '2024-02-28','ride',repeat('x',4096))`)
		case "customers":
			statements = append(statements,
				`CREATE TABLE analytics.customers (id BIGINT, name VARCHAR, region VARCHAR)`,
				`INSERT INTO analytics.customers VALUES (1,'Ada','west'),(2,'Lin','west'),(3,NULL,'east'),(4,'Excluded','west')`)
		default:
			t.Fatal("unknown Flight fixture table")
		}
	}
	statements = append(statements, "SET enable_external_access=false")
	for _, statement := range statements {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

// Copy into Go-owned IPC while inside Raw. No native reader/record/connection
// escapes to the asynchronous gRPC sender. Fixtures contain six rows at most.
func flightFixtureIPC(ctx context.Context, db *sql.DB, statement string) ([]byte, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	var wire bytes.Buffer
	err = conn.Raw(func(raw any) error {
		query, err := duck.NewArrowFromConn(raw.(driver.Conn))
		if err != nil {
			return err
		}
		reader, err := query.QueryContext(ctx, statement)
		if err != nil {
			return err
		}
		defer reader.Release()
		writer := ipc.NewWriter(&wire, ipc.WithSchema(reader.Schema()))
		for reader.Next() {
			if err := writer.Write(reader.RecordBatch()); err != nil {
				_ = writer.Close()
				return err
			}
			if wire.Len() > 1<<20 {
				_ = writer.Close()
				return errors.New("Flight fixture result exceeds its test budget")
			}
		}
		return errors.Join(reader.Err(), writer.Close())
	})
	return wire.Bytes(), err
}

func (f *flightSQLFixture) authorize(ctx context.Context) error {
	values, _ := metadata.FromIncomingContext(ctx)
	got := values.Get("authorization")
	if len(got) != 1 || got[0] != "Bearer "+f.token {
		return status.Error(codes.Unauthenticated, "Flight fixture requires its configured identity")
	}
	return nil
}

func (f *flightSQLFixture) GetFlightInfoStatement(ctx context.Context, q fsql.StatementQuery, descriptor *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	if err := f.authorize(ctx); err != nil {
		return nil, err
	}
	statement := q.GetQuery()
	f.mu.Lock()
	f.statements = append(f.statements, statement)
	f.mu.Unlock()
	if f.blockScans.Load() && !strings.HasSuffix(statement, " WHERE 1 = 0") {
		f.blockStarts.Add(1)
		f.blocked.Add(1)
		defer f.blocked.Add(-1)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	wire, err := flightFixtureIPC(ctx, f.db, statement)
	if err != nil {
		return nil, err
	}
	reader, err := ipc.NewReader(bytes.NewReader(wire))
	if err != nil {
		return nil, err
	}
	defer reader.Release()
	ticket, err := fsql.CreateStatementQueryTicket([]byte(statement))
	if err != nil {
		return nil, err
	}
	return &flight.FlightInfo{FlightDescriptor: descriptor,
		Schema:   flight.SerializeSchema(reader.Schema(), memory.DefaultAllocator),
		Endpoint: []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: ticket}}}}, nil
}

func (f *flightSQLFixture) DoGetStatement(ctx context.Context, ticket fsql.StatementQueryTicket) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	if err := f.authorize(ctx); err != nil {
		return nil, nil, err
	}
	wire, err := flightFixtureIPC(ctx, f.db, string(ticket.GetStatementHandle()))
	if err != nil {
		return nil, nil, err
	}
	reader, err := ipc.NewReader(bytes.NewReader(wire))
	if err != nil {
		return nil, nil, err
	}
	schema := reader.Schema()
	chunks := make(chan flight.StreamChunk)
	f.streams.Add(1)
	f.active.Add(1)
	go func() {
		defer f.streams.Done()
		defer f.active.Add(-1)
		defer close(chunks)
		defer reader.Release()
		for reader.Next() {
			record := reader.RecordBatch()
			record.Retain()
			select {
			case chunks <- flight.StreamChunk{Data: record}: // Flight server releases its ownership.
			case <-ctx.Done():
				record.Release()
				return
			}
		}
		if err := reader.Err(); err != nil {
			select {
			case chunks <- flight.StreamChunk{Err: err}:
			case <-ctx.Done():
			}
		}
	}()
	return schema, chunks, nil
}

func startFlightSQLFixture(t *testing.T, certificate tls.Certificate, table, token string) (*flightSQLFixture, string) {
	t.Helper()
	f := &flightSQLFixture{db: flightFixtureDB(t, table), token: token}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := flight.NewServerWithMiddleware(nil, grpc.WaitForHandlers(true), grpc.Creds(credentials.NewTLS(&tls.Config{
		MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{certificate},
	})))
	server.InitListener(listener)
	server.RegisterFlightService(fsql.NewFlightServer(f))
	served := make(chan struct{})
	go func() { defer close(served); _ = server.Serve() }()
	t.Cleanup(func() {
		server.Shutdown()
		<-served
		f.streams.Wait()
		if f.active.Load() != 0 || f.blocked.Load() != 0 {
			t.Error("Flight fixture retained work after shutdown")
		}
	})
	return f, "grpcs://" + listener.Addr().String()
}

func (f *flightSQLFixture) takeStatements() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	statements := append([]string(nil), f.statements...)
	f.statements = nil
	return statements
}

func flightFixtureCertificate(t *testing.T, directory string) (string, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "Kelvo Flight test only"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	encodedKey, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certificatePath, keyPath := filepath.Join(directory, "flight-ca.pem"), filepath.Join(directory, "flight-key.pem")
	for path, data := range map[string][]byte{
		certificatePath: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		keyPath:         pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: encodedKey}),
	} {
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	return certificatePath, keyPath
}
