package nativereader

import (
	"context"
	"crypto/tls"
	"encoding/pem"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	fsql "github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
)

type metadataFlightFixture struct {
	fsql.BaseServer
	t *testing.T
}

func flightMetadataSchema(kind string) *arrow.Schema {
	fields := []arrow.Field{{Name: "catalog_name", Type: arrow.BinaryTypes.String, Nullable: true}}
	if kind != "databases" {
		fields = append(fields, arrow.Field{Name: "db_schema_name", Type: arrow.BinaryTypes.String, Nullable: true})
	}
	if kind == "tables" || kind == "columns" {
		fields = append(fields, arrow.Field{Name: "table_name", Type: arrow.BinaryTypes.String}, arrow.Field{Name: "table_type", Type: arrow.BinaryTypes.String})
	}
	if kind == "columns" {
		fields = append(fields, arrow.Field{Name: "table_schema", Type: arrow.BinaryTypes.Binary})
	}
	return arrow.NewSchema(fields, nil)
}
func (f *metadataFlightFixture) info(ctx context.Context, kind string, fd *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if values := md.Get("authorization"); len(values) != 1 || values[0] != "Bearer fixture-current-token" {
		f.t.Error("Flight metadata lost current credentials")
	}
	return &flight.FlightInfo{FlightDescriptor: fd, Schema: flight.SerializeSchema(flightMetadataSchema(kind), memory.DefaultAllocator), Endpoint: []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: fd.Cmd}}}}, nil
}
func (f *metadataFlightFixture) rows(kind string) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	schema := flightMetadataSchema(kind)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	for _, name := range []string{"publicXschema", "public_schema"} {
		builder.Field(0).(*array.StringBuilder).Append("analytics")
		if kind != "databases" {
			builder.Field(1).(*array.StringBuilder).Append(name)
		}
		if kind == "tables" || kind == "columns" {
			builder.Field(2).(*array.StringBuilder).Append("events")
			builder.Field(3).(*array.StringBuilder).Append("TABLE")
		}
		if kind == "columns" {
			table := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "amount", Type: &arrow.Decimal128Type{Precision: 18, Scale: 2}, Nullable: true}}, nil)
			builder.Field(4).(*array.BinaryBuilder).Append(flight.SerializeSchema(table, memory.DefaultAllocator))
		}
		if kind == "databases" {
			break
		}
	}
	chunks := make(chan flight.StreamChunk, 1)
	chunks <- flight.StreamChunk{Data: builder.NewRecordBatch()}
	close(chunks)
	return schema, chunks, nil
}
func (f *metadataFlightFixture) GetFlightInfoCatalogs(ctx context.Context, fd *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return f.info(ctx, "databases", fd)
}
func (f *metadataFlightFixture) DoGetCatalogs(context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return f.rows("databases")
}
func (f *metadataFlightFixture) GetFlightInfoSchemas(ctx context.Context, _ fsql.GetDBSchemas, fd *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return f.info(ctx, "schemas", fd)
}
func (f *metadataFlightFixture) DoGetDBSchemas(context.Context, fsql.GetDBSchemas) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return f.rows("schemas")
}
func (f *metadataFlightFixture) GetFlightInfoTables(ctx context.Context, cmd fsql.GetTables, fd *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	kind := "tables"
	if cmd.GetIncludeSchema() {
		kind = "columns"
	}
	return f.info(ctx, kind, fd)
}
func (f *metadataFlightFixture) DoGetTables(_ context.Context, cmd fsql.GetTables) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	kind := "tables"
	if cmd.GetIncludeSchema() {
		kind = "columns"
	}
	return f.rows(kind)
}

func TestFlightMetadataProtocolRetainsAccountCatalogsAndExactFilters(t *testing.T) {
	certificate := httptest.NewTLSServer(http.NotFoundHandler())
	defer certificate.Close()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := flight.NewServerWithMiddleware(nil, grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: certificate.TLS.Certificates, MinVersion: tls.VersionTLS12})))
	server.InitListener(listener)
	server.RegisterFlightService(fsql.NewFlightServer(&metadataFlightFixture{t: t}))
	go server.Serve()
	defer server.Shutdown()
	spec := adapter.ConnectionSpec{Engine: "arrow_flight", URL: "grpcs://" + listener.Addr().String(), TenantID: "tenant", ConnectionID: "saved", Revision: "current", Token: "fixture-current-token", Options: map[string]string{"protocol": "flightsql", "tls_ca_pem": string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Certificate().Raw}))}}
	s, err := Open(context.Background(), spec, adapter.ProcessLimits{MemoryMB: 64})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, object := range []string{"databases", "schemas", "tables", "columns"} {
		request := operations.MetadataSpec{Object: object, Limit: 5}
		if object != "databases" {
			request.Target = operations.ObjectRef{Catalog: "analytics", Schema: "public_schema"}
			if object != "schemas" {
				request.Target.Name = "events"
			}
		}
		out := &metadataCapture{}
		stats, err := s.Inspect(context.Background(), request, adapter.Limits{MaxRows: 5, MaxBytes: 1 << 20, BatchRows: 2}, out)
		want := int64(1)
		if object == "columns" {
			want = 2
		}
		if err != nil || stats.Rows != want {
			out.close()
			t.Fatalf("Flight %s: %+v %v", object, stats, err)
		}
		verifyMetadataCapture(t, s, object, out)
		if object != "databases" {
			for _, row := range out.rows {
				if row[1] != "public_schema" {
					t.Fatal("Flight pattern widened exact schema filter")
				}
			}
		}
	}
}
