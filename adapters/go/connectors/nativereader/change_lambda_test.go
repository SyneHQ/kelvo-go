package nativereader

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/adapters/go/connectors/sqlsession"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

func TestLambdaEncodedLaterSlotPreventsEarlierWrite(t *testing.T) {
	// Block all dialing even if preflight regresses; fixture credentials never
	// leave this test process. Other package tests do not run in parallel.
	original := http.DefaultTransport
	transport := original.(*http.Transport).Clone()
	var dials atomic.Int32
	transport.DialContext = func(context.Context, string, string) (net.Conn, error) {
		dials.Add(1)
		return nil, errors.New("fixture blocks source dispatch")
	}
	http.DefaultTransport = transport
	t.Cleanup(func() { http.DefaultTransport = original; transport.CloseIdleConnections() })
	path := "/" + strings.Repeat("p", 4095)
	s, err := Open(context.Background(), adapter.ConnectionSpec{TenantID: "tenant", ConnectionID: "source", Revision: "revision", Engine: "clickhouse_lambda", URL: "https://lambda.us-east-1.amazonaws.com", Database: "bucket", Username: "fixture-id", Password: "fixture-secret", Options: map[string]string{"region": "us-east-1", "function_name": "fixture", "bucket_path": path}}, adapter.ProcessLimits{MemoryMB: 64})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	first, prefix, suffix := "DELETE FROM t WHERE id=1", "INSERT INTO t VALUES('", "')"
	later := prefix + strings.Repeat("\x01", (1<<20)-len(first)-len(prefix)-len(suffix)) + suffix
	statements := []string{first, later}
	if err := sqlsession.ValidateAutocommit(statements); err != nil {
		t.Fatal("fixture must pass generic SQL/input limits", err)
	}
	result, err := s.Execute(context.Background(), adapter.Change{Statements: statements})
	if err == nil || query.PublicError(err).Code != "RESOURCE_EXHAUSTED" || result.Outcome != "failed" || result.Attempted != 0 || result.Completed != 0 || dials.Load() != 0 {
		t.Fatal("encoded oversized second slot escaped preflight", result, err, dials.Load())
	}
}
