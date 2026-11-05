package clickhouse

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/operations"
)

func failureLine(t *testing.T, source ResolvedSource, wantCode string) string {
	t.Helper()
	engine, err := NewResolved(source, query.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	_, err = engine.Execute(context.Background(), request(), &testSink{})
	var f *Failure
	if !errors.As(err, &f) || query.PublicError(err).Code != wantCode {
		t.Fatalf("failure not classified: %v", err)
	}
	line := f.LogLine()
	for _, secret := range []string{"reader", "test-only-password", request().SQL, "database=reports", "secret-body", "8443", "127.0.0.1:"} {
		if strings.Contains(line, secret) {
			t.Fatalf("log line leaks %q: %s", secret, line)
		}
	}
	return line
}

func TestFailureLogLines(t *testing.T) {
	ok := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer ok.Close()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	closed := resolvedFixture(t, ok)
	closed.URL = "https://" + listener.Addr().String() + "?database=reports"
	_ = listener.Close()
	if got, want := failureLine(t, closed, "QUERY_FAILED"), "kelvo clickhouse source failed: reason=connect_refused http_status=- clickhouse_code=- host=127.0.0.1"; got != want {
		t.Errorf("refused: %s", got)
	}

	untrusted := resolvedFixture(t, ok)
	untrusted.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
	if got, want := failureLine(t, untrusted, "QUERY_FAILED"), "kelvo clickhouse source failed: reason=tls_unknown_authority http_status=- clickhouse_code=- host=127.0.0.1"; got != want {
		t.Errorf("tls: %s", got)
	}

	rejected := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-ClickHouse-Exception-Code", "516")
		w.WriteHeader(516)
		_, _ = w.Write([]byte("Code: 516. reader: Authentication failed: password is incorrect secret-body"))
	}))
	defer rejected.Close()
	if got, want := failureLine(t, resolvedFixture(t, rejected), "QUERY_FAILED"), "kelvo clickhouse source failed: reason=http_status http_status=516 clickhouse_code=516 host=127.0.0.1"; got != want {
		t.Errorf("http: %s", got)
	}

	data, _ := fixture(t)
	good := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(data) }))
	defer good.Close()
	engine, err := NewResolved(resolvedFixture(t, good), query.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	if _, err := engine.Execute(context.Background(), request(), &testSink{}); err != nil {
		t.Fatalf("success produced failure: %v", err)
	}
}

func TestFailureLogLineRevalidatesDecodedFields(t *testing.T) {
	f := operations.SourceFailure{Reason: "x y", HTTPStatus: 7, ClickHouseCode: "12a", Host: "h/evil?q=1"}
	if got := f.LogLine(); got != "kelvo clickhouse source failed: reason=other http_status=- clickhouse_code=- host=-" {
		t.Fatal(got)
	}
}
