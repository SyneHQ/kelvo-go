package clickhouselambda

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/sources/awsapi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

type capture struct {
	schema  *arrow.Schema
	records []arrow.RecordBatch
}

func (s *capture) Schema(schema *arrow.Schema) error { s.schema = schema; return nil }
func (s *capture) Write(batch arrow.RecordBatch) error {
	batch.Retain()
	s.records = append(s.records, batch)
	return nil
}
func sink(t *testing.T) *capture {
	t.Helper()
	s := &capture{}
	t.Cleanup(func() {
		for _, r := range s.records {
			r.Release()
		}
	})
	return s
}

func fixture(t *testing.T, handler func(http.ResponseWriter, *http.Request, lambdaEvent)) (*Engine, *atomic.Int32) {
	t.Helper()
	var calls atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Method != "POST" || r.Host != "lambda.us-east-1.amazonaws.com" || r.URL.Path != "/2015-03-31/functions/analytics-function/invocations" || r.URL.RawQuery != "" || r.Header.Get("X-Amz-Invocation-Type") != "RequestResponse" || r.Header.Get("X-Amz-Security-Token") != "explicit-token" {
			t.Error("unexpected Lambda transport")
		}
		data, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		var event lambdaEvent
		if json.Unmarshal(data, &event) != nil || event.RawPath != "/source-path" || event.RequestContext.HTTP.Method != "POST" {
			t.Error("legacy event contract changed")
		}
		clone := r.Clone(r.Context())
		clone.URL.Scheme = "https"
		clone.URL.Host = r.Host
		clone.Header = clone.Header.Clone()
		actual := clone.Header.Get("Authorization")
		clone.Header.Del("Authorization")
		stamp, err := time.Parse("20060102T150405Z", r.Header.Get("X-Amz-Date"))
		if err != nil {
			t.Error("missing signing date")
		}
		hash := sha256.Sum256(data)
		if err := v4.NewSigner().SignHTTP(r.Context(), fixtureCredentials(), clone, hex.EncodeToString(hash[:]), "lambda", "us-east-1", stamp); err != nil || clone.Header.Get("Authorization") != actual {
			t.Error("invalid SigV4 signature")
		}
		handler(w, r, event)
	}))
	t.Cleanup(server.Close)
	config := server.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
	config.ServerName = "127.0.0.1"
	e, err := NewResolved(catalog.Config{Sources: []catalog.Source{{ID: "source", Type: "clickhouse_lambda", Options: map[string]string{"region": "us-east-1", "function_name": "analytics-function", "bucket_path": "/source-path"}}}}, query.DefaultLimits(), awsapi.Credentials{URL: RegionalOrigin("us-east-1"), AccessKeyID: "explicit-id", SecretAccessKey: "explicit-secret", SessionToken: "explicit-token", TLS: config})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.Close() })
	address := strings.TrimPrefix(server.URL, "https://")
	e.client.Transport.(*http.Transport).DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, address)
	}
	return e, &calls
}

func request(sql string) query.Request {
	return query.Request{Mode: "native", ConnectionID: "source", SQL: sql}
}

func TestVerifiedLambdaReadKeepsExactTextNullAndEmpty(t *testing.T) {
	e, calls := fixture(t, func(w http.ResponseWriter, _ *http.Request, event lambdaEvent) {
		if event.Body != "SELECT * FROM events" {
			t.Error("SQL changed")
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"statusCode": 200, "body": "9007199254740993\t12345678901234567890.123400\t\t\\N\tline\\nnext\t\\\\N\n"})
	})
	out := sink(t)
	stats, err := e.Execute(context.Background(), request("SELECT * FROM events"), out)
	if err != nil || stats.Rows != 1 || len(out.records) != 1 || calls.Load() != 1 {
		t.Fatal(stats, err)
	}
	batch := out.records[0]
	expected := map[int]string{0: "9007199254740993", 1: "12345678901234567890.123400", 2: "", 4: "line\nnext", 5: `\N`}
	for i, value := range expected {
		column := batch.Column(i).(*array.String)
		if column.IsNull(0) || column.Value(0) != value {
			t.Fatal("TSV value changed", i)
		}
	}
	if !batch.Column(3).IsNull(0) || out.schema.Field(0).Name != "col1" {
		t.Fatal("null or unnamed-column contract changed")
	}
}

func TestLambdaWriteAcknowledgementDoesNotInventAffectedRows(t *testing.T) {
	e, calls := fixture(t, func(w http.ResponseWriter, _ *http.Request, event lambdaEvent) {
		if event.Body != "INSERT INTO events VALUES(1)" {
			t.Error("write changed")
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"statusCode": 200, "body": ""})
	})
	if count, err := e.ApplyStatement(context.Background(), "INSERT INTO events VALUES(1)"); err != nil || count != nil || calls.Load() != 1 {
		t.Fatal(count, err, calls.Load())
	}
}

func TestLambdaFailureIsBoundedSanitizedAndNeverReplayed(t *testing.T) {
	for _, mode := range []string{"http", "function", "inner", "missing-body", "redirect", "disconnect", "oversize"} {
		t.Run(mode, func(t *testing.T) {
			e, calls := fixture(t, func(w http.ResponseWriter, r *http.Request, _ lambdaEvent) {
				switch mode {
				case "http":
					http.Error(w, "private source details", 500)
				case "function":
					w.Header().Set("X-Amz-Function-Error", "Unhandled")
					io.WriteString(w, `{"secret":"private source details"}`)
				case "inner":
					io.WriteString(w, `{"statusCode":500,"body":"private source details"}`)
				case "missing-body":
					io.WriteString(w, `{"statusCode":200}`)
				case "redirect":
					http.Redirect(w, r, "https://other.invalid", 302)
				case "disconnect":
					c, _, _ := w.(http.Hijacker).Hijack()
					c.Close()
				case "oversize":
					io.WriteString(w, strings.Repeat("x", 4096))
				}
			})
			if mode == "oversize" {
				e.limits.MaxBytes = 1024
			}
			_, err := e.ApplyStatement(context.Background(), "INSERT INTO events VALUES(1)")
			if err == nil || strings.Contains(err.Error(), "private") || calls.Load() != 1 {
				t.Fatal(err, calls.Load())
			}
		})
	}
}

func TestLambdaReadGuardsAndLimits(t *testing.T) {
	e, calls := fixture(t, func(w http.ResponseWriter, _ *http.Request, _ lambdaEvent) {
		_ = json.NewEncoder(w).Encode(map[string]any{"statusCode": 200, "body": "1\n2\n"})
	})
	if _, err := e.Execute(context.Background(), request("DELETE FROM events"), sink(t)); err == nil || calls.Load() != 0 {
		t.Fatal("write reached read transport")
	}
	e.limits.MaxRows = 1
	if _, err := e.Execute(context.Background(), request("SELECT id FROM events"), sink(t)); err == nil || query.PublicError(err).Code != "RESOURCE_EXHAUSTED" {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := e.ApplyStatement(ctx, "DELETE FROM events"); err == nil || calls.Load() != 1 {
		t.Fatal("cancelled operation dispatched", err, calls.Load())
	}
	for _, bad := range []string{`trailing\`, `unknown\q`, `invalid\xFF`} {
		if _, err := unescapeTSV(bad); err == nil {
			t.Fatal("invalid encoding accepted", bad)
		}
	}
	out := sink(t)
	if stats, err := writeTSV(context.Background(), "", query.DefaultLimits(), out); err != nil || stats.Rows != 0 || out.schema == nil || out.schema.NumFields() != 0 {
		t.Fatal(stats, err)
	}
}

func fixtureCredentials() aws.Credentials {
	return aws.Credentials{AccessKeyID: "explicit-id", SecretAccessKey: "explicit-secret", SessionToken: "explicit-token"}
}

func TestLambdaPreflightUsesExactEncodedPayload(t *testing.T) {
	e, calls := fixture(t, func(http.ResponseWriter, *http.Request, lambdaEvent) { t.Error("oversized statement dispatched") })
	if err := e.ValidateStatement(strings.Repeat("x", 1<<20)); err != nil {
		t.Fatal("plain SQL at the input limit rejected", err)
	}
	oversized := strings.Repeat("\x01", 1<<20)
	if err := e.ValidateStatement(oversized); err == nil || query.PublicError(err).Code != "RESOURCE_EXHAUSTED" {
		t.Fatal("encoded event limit was not checked", err)
	}
	if _, err := e.ApplyStatement(context.Background(), oversized); err == nil || query.PublicError(err).Code != "RESOURCE_EXHAUSTED" || calls.Load() != 0 {
		t.Fatal("invoke bypassed payload preflight", err, calls.Load())
	}
}
