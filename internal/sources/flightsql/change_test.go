package flightsql

import (
	"context"
	"crypto/tls"
	"net"
	"sync/atomic"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/sources/cloudapi"
	"github.com/apache/arrow-go/v18/arrow/flight"
	fsql "github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type updateFixture struct {
	fsql.BaseServer
	calls atomic.Int32
	count int64
	fail  bool
}

func (f *updateFixture) DoPutCommandStatementUpdate(ctx context.Context, input fsql.StatementUpdate) (int64, error) {
	f.calls.Add(1)
	md, _ := metadata.FromIncomingContext(ctx)
	if input.GetQuery() != "UPDATE events SET amount=1" || len(md.Get("authorization")) != 1 || md.Get("authorization")[0] != "Bearer current-token" {
		return 0, status.Error(codes.PermissionDenied, "fixture authorization differs")
	}
	if f.fail {
		return 0, status.Error(codes.Unavailable, "acknowledgement lost")
	}
	return f.count, nil
}
func TestFlightStatementUpdateUsesExactCommandAndNoReplay(t *testing.T) {
	for _, test := range []struct {
		name  string
		count int64
		fail  bool
	}{{"exact", 9007199254740993, false}, {"unspecified", -1, false}, {"invalid-count", -2, false}, {"lost-ack", 0, true}} {
		t.Run(test.name, func(t *testing.T) {
			cert, roots := testCert(t)
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			fixture := &updateFixture{count: test.count, fail: test.fail}
			server := flight.NewServerWithMiddleware(nil, grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})))
			server.InitListener(listener)
			server.RegisterFlightService(fsql.NewFlightServer(fixture))
			go server.Serve()
			defer server.Shutdown()
			e, err := NewResolved(catalog.Config{Sources: []catalog.Source{{ID: "flight", Type: "arrow_flight", Options: map[string]string{"protocol": "flightsql"}}}}, query.DefaultLimits(), cloudapi.Credentials{URL: "grpcs://" + listener.Addr().String(), Token: "current-token", TLS: &tls.Config{RootCAs: roots}})
			if err != nil {
				t.Fatal(err)
			}
			defer e.Close()
			count, err := e.ApplyStatement(context.Background(), "UPDATE events SET amount=1")
			if fixture.calls.Load() != 1 {
				t.Fatal("update retried", fixture.calls.Load())
			}
			switch test.name {
			case "exact":
				if err != nil || count == nil || *count != test.count {
					t.Fatal(count, err)
				}
			case "unspecified":
				if err != nil || count != nil {
					t.Fatal(count, err)
				}
			default:
				if err == nil {
					t.Fatal("invalid update acknowledgement accepted")
				}
			}
		})
	}
}
