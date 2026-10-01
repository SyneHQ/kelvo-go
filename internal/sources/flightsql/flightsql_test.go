package flightsql

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/flight"
	fsql "github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	pb "github.com/apache/arrow-go/v18/arrow/flight/gen/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

type fixture struct {
	fsql.BaseServer
	external  bool
	badSchema bool
}

func (f *fixture) GetFlightInfoStatement(_ context.Context, q fsql.StatementQuery, fd *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	ticket, err := fsql.CreateStatementQueryTicket([]byte(q.GetQuery()))
	if err != nil {
		return nil, err
	}
	ep := &flight.FlightEndpoint{Ticket: &flight.Ticket{Ticket: ticket}}
	if f.external {
		ep.Location = []*flight.Location{{Uri: "grpcs://other.example:443"}}
	}
	schema := flight.SerializeSchema(fixtureSchema(q.GetQuery()), memory.DefaultAllocator)
	if f.badSchema {
		schema = []byte{1, 2, 3, 4}
	}
	return &flight.FlightInfo{FlightDescriptor: fd, Schema: schema, Endpoint: []*flight.FlightEndpoint{ep}}, nil
}
func fixtureSchema(query string) *arrow.Schema {
	if query == "SELECT huge" {
		return arrow.NewSchema([]arrow.Field{{Name: "payload", Type: arrow.BinaryTypes.String}}, nil)
	}
	return arrow.NewSchema([]arrow.Field{{Name: "amount", Type: &arrow.Decimal128Type{Precision: 19, Scale: 2}, Nullable: true}}, nil)
}
func (*fixture) DoGetStatement(_ context.Context, ticket fsql.StatementQueryTicket) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	if string(ticket.GetStatementHandle()) == "SELECT huge" {
		b := array.NewStringBuilder(memory.DefaultAllocator)
		defer b.Release()
		b.Append(strings.Repeat("x", 2048))
		a := b.NewArray()
		s := fixtureSchema("SELECT huge")
		r := array.NewRecordBatch(s, []arrow.Array{a}, 1)
		a.Release()
		ch := make(chan flight.StreamChunk, 1)
		ch <- flight.StreamChunk{Data: r}
		close(ch)
		return s, ch, nil
	}
	dt := &arrow.Decimal128Type{Precision: 19, Scale: 2}
	b := array.NewDecimal128Builder(memory.DefaultAllocator, dt)
	defer b.Release()
	b.AppendNull()
	b.Append(decimal128.FromI64(12345))
	arr := b.NewArray()
	s := fixtureSchema("")
	rec := array.NewRecordBatch(s, []arrow.Array{arr}, 2)
	arr.Release()
	ch := make(chan flight.StreamChunk, 1)
	ch <- flight.StreamChunk{Data: rec}
	close(ch)
	return s, ch, nil
}

type capture struct {
	schema  *arrow.Schema
	rows    int64
	decimal decimal128.Num
}

func (c *capture) Schema(s *arrow.Schema) error { c.schema = s; return nil }
func (c *capture) Write(r arrow.RecordBatch) error {
	c.rows += r.NumRows()
	a := r.Column(0).(*array.Decimal128)
	if !a.IsNull(1) {
		c.decimal = a.Value(1)
	}
	return nil
}
func testCert(t *testing.T) (tls.Certificate, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.CreateCertificate(rand.Reader, &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "127.0.0.1"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}, &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "127.0.0.1"}}, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	kb, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := tls.X509KeyPair(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kb}))
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	return cert, pool
}
func start(t *testing.T, external bool) (string, *x509.CertPool, func()) {
	t.Helper()
	cert, pool := testCert(t)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := flight.NewServerWithMiddleware(nil, grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})))
	s.InitListener(l)
	s.RegisterFlightService(fsql.NewFlightServer(&fixture{external: external}))
	go s.Serve()
	return "grpcs://" + l.Addr().String(), pool, s.Shutdown
}
func engine(t *testing.T, url string, pool *x509.CertPool, lim query.Limits) *Engine {
	t.Helper()
	t.Setenv("FLIGHT_URL", url)
	t.Setenv("FLIGHT_TOKEN", "fixture-token")
	e, err := New(catalog.Config{Sources: []catalog.Source{{ID: "flight", Type: "arrow_flight", URLEnv: "FLIGHT_URL", TokenEnv: "FLIGHT_TOKEN", Options: map[string]string{"protocol": "flightsql"}}}}, lim)
	if err != nil {
		t.Fatal(err)
	}
	e.tlsConfig = func(ep endpoint) *tls.Config {
		return &tls.Config{MinVersion: tls.VersionTLS12, ServerName: ep.host, RootCAs: pool}
	}
	return e
}
func TestFlightSQLTLSArrowAndLimits(t *testing.T) {
	url, pool, stop := start(t, false)
	defer stop()
	lim := query.DefaultLimits()
	e := engine(t, url, pool, lim)
	var got capture
	stats, err := e.Execute(context.Background(), query.Request{Mode: "native", ConnectionID: "flight", SQL: "SELECT amount"}, &got)
	if err != nil {
		t.Fatal(err)
	}
	if got.rows != 2 || got.schema.Field(0).Type.ID() != arrow.DECIMAL128 || got.decimal != decimal128.FromI64(12345) || stats.Rows != 2 {
		t.Fatalf("lost nullable decimal result: %#v %#v", got, stats)
	}
	lim.MaxRows = 1
	e = engine(t, url, pool, lim)
	_, err = e.Execute(context.Background(), query.Request{Mode: "native", ConnectionID: "flight", SQL: "SELECT amount"}, &capture{})
	if err == nil {
		t.Fatal("expected row limit")
	}
	lim = query.DefaultLimits()
	lim.MaxBytes = 1024
	e = engine(t, url, pool, lim)
	if _, err := e.Execute(context.Background(), query.Request{Mode: "native", ConnectionID: "flight", SQL: "SELECT huge"}, &capture{}); err == nil {
		t.Fatal("expected byte limit")
	}
	_, err = e.Execute(context.Background(), query.Request{Mode: "native", ConnectionID: "flight", SQL: "DELETE FROM t"}, &capture{})
	if err == nil {
		t.Fatal("expected read-only rejection")
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := e.Execute(cancelled, query.Request{Mode: "native", ConnectionID: "flight", SQL: "SELECT amount"}, &capture{}); err == nil {
		t.Fatal("expected cancellation")
	}
}
func TestFlightSQLRejectsExternalEndpointAndTLSFailure(t *testing.T) {
	url, pool, stop := start(t, true)
	defer stop()
	e := engine(t, url, pool, query.DefaultLimits())
	if _, err := e.Execute(context.Background(), query.Request{Mode: "native", ConnectionID: "flight", SQL: "SELECT 1"}, &capture{}); err == nil {
		t.Fatal("expected external endpoint rejection")
	}
	e = engine(t, url, nil, query.DefaultLimits())
	if _, err := e.Execute(context.Background(), query.Request{Mode: "native", ConnectionID: "flight", SQL: "SELECT 1"}, &capture{}); err == nil {
		t.Fatal("expected TLS verification failure")
	}
}
func TestParseEndpointRejectsInsecure(t *testing.T) {
	for _, raw := range []string{"grpc://localhost:443", "grpcs://localhost", "grpcs://user@localhost:443", "grpcs://localhost:443/path"} {
		if _, err := parseEndpoint(raw); err == nil {
			t.Errorf("accepted %q", raw)
		}
	}
}

func TestFlightSQLRejectsTruncatedFlightInfoSchema(t *testing.T) {
	cert, pool := testCert(t)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := flight.NewServerWithMiddleware(nil, grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})))
	server.InitListener(l)
	server.RegisterFlightService(fsql.NewFlightServer(&fixture{badSchema: true}))
	go server.Serve()
	defer server.Shutdown()
	url := "grpcs://" + l.Addr().String()
	e := engine(t, url, pool, query.DefaultLimits())
	got := &capture{}
	if _, err := e.Execute(context.Background(), query.Request{Mode: "native", ConnectionID: "flight", SQL: "SELECT amount"}, got); err == nil {
		t.Fatal("accepted truncated schema")
	} else if got.schema != nil || got.rows != 0 {
		t.Fatal("delivered malformed schema")
	}
}

type rawFlight struct {
	flight.BaseFlightServer
	bad  []byte
	body []byte
}

func (r *rawFlight) GetFlightInfo(context.Context, *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	sc := fixtureSchema("")
	return &flight.FlightInfo{Schema: flight.SerializeSchema(sc, memory.DefaultAllocator), Endpoint: []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: []byte("x")}}}}, nil
}
func (r *rawFlight) DoGet(_ *flight.Ticket, stream pb.FlightService_DoGetServer) error {
	wire := flight.SerializeSchema(fixtureSchema(""), memory.DefaultAllocator)
	header := wire[8:]
	if err := stream.Send(&flight.FlightData{DataHeader: header}); err != nil {
		return err
	}
	return stream.Send(&flight.FlightData{DataHeader: r.bad, DataBody: r.body})
}
func TestRawFlightMalformedDataHeaders(t *testing.T) {
	for _, tc := range []struct {
		name         string
		header, body []byte
	}{{"invalid", []byte{1, 2, 3, 4}, nil}, {"forged", append([]byte{0, 0, 0, 0}, make([]byte, 128)...), nil}, {"shortbody", []byte{1, 2, 3, 4}, []byte{1}}} {
		t.Run(tc.name, func(t *testing.T) {
			cert, pool := testCert(t)
			l, _ := net.Listen("tcp", "127.0.0.1:0")
			g := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})))
			pb.RegisterFlightServiceServer(g, &rawFlight{bad: tc.header, body: tc.body})
			go g.Serve(l)
			defer g.Stop()
			e := engine(t, "grpcs://"+l.Addr().String(), pool, query.DefaultLimits())
			got := &capture{}
			if _, err := e.Execute(context.Background(), query.Request{Mode: "native", ConnectionID: "flight", SQL: "SELECT x"}, got); err == nil || got.rows != 0 {
				t.Fatalf("accepted malformed data: %v", err)
			}
		})
	}
}
