package flightsql

import (
	"context"
	"crypto/tls"
	"net"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/sources/cloudapi"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	fsql "github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type discoveryFixture struct {
	fsql.BaseServer
	malformed bool
}

func discoverySchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{{Name: "catalog_name", Type: arrow.BinaryTypes.String, Nullable: true}, {Name: "db_schema_name", Type: arrow.BinaryTypes.String, Nullable: true}, {Name: "table_name", Type: arrow.BinaryTypes.String}, {Name: "table_type", Type: arrow.BinaryTypes.String}, {Name: "table_schema", Type: arrow.BinaryTypes.Binary}}, nil)
}
func (f *discoveryFixture) GetFlightInfoTables(_ context.Context, _ fsql.GetTables, fd *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return &flight.FlightInfo{FlightDescriptor: fd, Schema: flight.SerializeSchema(discoverySchema(), memory.DefaultAllocator), Endpoint: []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: fd.Cmd}}}}, nil
}
func (f *discoveryFixture) DoGetTables(context.Context, fsql.GetTables) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	b := array.NewRecordBuilder(memory.DefaultAllocator, discoverySchema())
	defer b.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "amount", Type: &arrow.Decimal128Type{Precision: 18, Scale: 2}, Nullable: true}}, nil)
	raw := flight.SerializeSchema(schema, memory.DefaultAllocator)
	if f.malformed {
		raw = []byte{1, 2, 3}
	}
	for _, name := range []string{"publicXschema", "public_schema"} {
		b.Field(0).(*array.StringBuilder).Append("analytics")
		b.Field(1).(*array.StringBuilder).Append(name)
		b.Field(2).(*array.StringBuilder).Append("events")
		b.Field(3).(*array.StringBuilder).Append("TABLE")
		b.Field(4).(*array.BinaryBuilder).Append(raw)
	}
	batch := b.NewRecordBatch()
	out := make(chan flight.StreamChunk, 1)
	out <- flight.StreamChunk{Data: batch}
	close(out)
	return discoverySchema(), out, nil
}

type discoveredColumns struct {
	names []string
	types []string
}

func (s *discoveredColumns) Schema(*arrow.Schema) error { return nil }
func (s *discoveredColumns) Write(batch arrow.RecordBatch) error {
	names := batch.Column(3).(*array.String)
	types := batch.Column(4).(*array.String)
	for i := 0; i < names.Len(); i++ {
		s.names = append(s.names, names.Value(i))
		s.types = append(s.types, types.Value(i))
	}
	return nil
}
func TestFlightDiscoveryUsesMetadataProtocolAndExactTargets(t *testing.T) {
	for _, malformed := range []bool{false, true} {
		cert, roots := testCert(t)
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		server := flight.NewServerWithMiddleware(nil, grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})))
		server.InitListener(listener)
		server.RegisterFlightService(fsql.NewFlightServer(&discoveryFixture{malformed: malformed}))
		go server.Serve()
		cfg := catalog.Config{Sources: []catalog.Source{{ID: "flight", Type: "arrow_flight", Options: map[string]string{"protocol": "flightsql"}}}}
		engine, err := NewResolved(cfg, query.DefaultLimits(), cloudapi.Credentials{URL: "grpcs://" + listener.Addr().String(), Token: "current-token", TLS: &tls.Config{RootCAs: roots}})
		if err != nil {
			server.Shutdown()
			t.Fatal(err)
		}
		sink := &discoveredColumns{}
		stats, err := engine.Inspect(context.Background(), operations.MetadataSpec{Object: "columns", Limit: 10, Target: operations.ObjectRef{Catalog: "analytics", Schema: "public_schema", Name: "events"}}, sink)
		engine.Close()
		server.Shutdown()
		if malformed {
			if err == nil {
				t.Fatal("malformed table schema accepted")
			}
			continue
		}
		if err != nil || stats.Rows != 2 || len(sink.names) != 2 || sink.names[0] != "id" || sink.names[1] != "amount" {
			t.Fatal("metadata target/schema lost", stats, err, sink.names)
		}
	}
}
