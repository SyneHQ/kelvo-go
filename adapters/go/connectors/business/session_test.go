package business

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/SYNEHQ/kelvo-go/provider"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

type capture struct {
	schema    *arrow.Schema
	documents []string
	failure   bool
}

func (s *capture) Schema(schema *arrow.Schema) error { s.schema = schema; return nil }
func (s *capture) Write(record arrow.RecordBatch) error {
	if s.failure {
		return errors.New("result delivery failed")
	}
	values := record.Column(0).(*array.Binary)
	for i := 0; i < values.Len(); i++ {
		s.documents = append(s.documents, string(values.Value(i)))
	}
	return nil
}

func nativeCall(t *testing.T, engine, raw string) adapter.Native {
	t.Helper()
	kind, spec, err := provider.Invocation(engine, []byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	return adapter.Native{Kind: kind, Spec: *spec, Limits: adapter.Limits{MaxRows: 100, MaxBytes: 2 << 20, BatchRows: 16}}
}

func fixtureClient(t *testing.T, mode string) (*Session, *atomic.Int64) {
	t.Helper()
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var q struct {
			ID     int             `json:"id"`
			Method string          `json:"method"`
			Params json.RawMessage `json:"params"`
		}
		if json.NewDecoder(r.Body).Decode(&q) != nil {
			t.Error("malformed provider call")
			return
		}
		if mode == "before" {
			w.WriteHeader(503)
			return
		}
		switch q.Method {
		case "initialize":
			fmt.Fprintf(w, `{"id":%d,"result":{"protocolVersion":"2025-03-26"}}`, q.ID)
		case "notifications/initialized":
			w.WriteHeader(202)
		case "tools/call":
			calls.Add(1)
			if !strings.Contains(string(q.Params), "9007199254740993") {
				t.Error("exact argument lost")
			}
			if mode == "lost" {
				conn, _, err := w.(http.Hijacker).Hijack()
				if err != nil {
					t.Error(err)
					return
				}
				conn.Close()
				return
			}
			if mode == "toolerror" {
				fmt.Fprintf(w, `{"id":%d,"result":{"isError":true,"content":[{"text":"private diagnostic"}]}}`, q.ID)
				return
			}
			fmt.Fprintf(w, `{"id":%d,"result":{"structuredContent":{"id":9007199254740993,"amount":12345678901234567890.1234567},"content":[{"text":"created"}]}}`, q.ID)
		default:
			t.Error("unexpected request", q.Method)
		}
	}))
	t.Cleanup(server.Close)
	c, err := New(Config{Type: "motherduck", Token: "fixture-token"})
	if err != nil {
		t.Fatal(err)
	}
	c.endpoint = server.URL
	s := &Session{client: c, engine: "motherduck"}
	t.Cleanup(func() { s.Close() })
	return s, &calls
}

func TestProviderMutationRetainsResultAndExactValues(t *testing.T) {
	s, calls := fixtureClient(t, "success")
	sink := &capture{}
	result, err := s.RunNative(context.Background(), nativeCall(t, "motherduck", `{"tool":"query_rw","arguments":{"query":"INSERT INTO x VALUES(9007199254740993)"}}`), sink)
	if err != nil || result.Outcome != operations.Completed || result.Effect != operations.EffectCommitted || calls.Load() != 1 || len(sink.documents) != 1 {
		t.Fatal(result, err, calls.Load())
	}
	if !strings.Contains(sink.documents[0], "9007199254740993") || !strings.Contains(sink.documents[0], "12345678901234567890.1234567") {
		t.Fatal("result rounded")
	}
	if value, _ := sink.schema.Metadata().GetValue("kelvo_document_format"); value != provider.ResultFormat {
		t.Fatal("result contract missing")
	}
}

func TestProviderWriteAmbiguityAndDeliveryDoNotReplay(t *testing.T) {
	for _, tc := range []struct {
		mode    string
		outcome operations.Outcome
		effect  operations.Effect
		calls   int64
	}{
		{"before", operations.Failed, operations.EffectNone, 0},
		{"lost", operations.OutcomeUnknown, operations.EffectUnknown, 1},
		{"toolerror", operations.OutcomeUnknown, operations.EffectUnknown, 1},
		{"delivery", operations.Completed, operations.EffectCommitted, 1},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			s, calls := fixtureClient(t, tc.mode)
			sink := &capture{failure: tc.mode == "delivery"}
			result, err := s.RunNative(context.Background(), nativeCall(t, "motherduck", `{"tool":"query_rw","arguments":{"query":"INSERT INTO x VALUES(9007199254740993)"}}`), sink)
			if err == nil || result.Outcome != tc.outcome || result.Effect != tc.effect || calls.Load() != tc.calls {
				t.Fatal(result, err, calls.Load())
			}
			if strings.Contains(err.Error(), "private diagnostic") {
				t.Fatal("provider body leaked")
			}
		})
	}
}

func TestNativeReadCannotInvokeWriteTools(t *testing.T) {
	s, calls := fixtureClient(t, "success")
	request := nativeCall(t, "motherduck", `{"tool":"query_rw","arguments":{"query":"DELETE FROM x"}}`)
	request.Kind = operations.NativeRead
	request.Spec.ReturnResult = false
	if result, err := s.RunNative(context.Background(), request, &capture{}); err == nil || result.Effect != operations.EffectNone || calls.Load() != 0 {
		t.Fatal("write escaped read authority", result, err)
	}
}

func TestProviderSourceRejectsEndpointAndCredentialOverrides(t *testing.T) {
	valid := adapter.Connection{Engine: "ramp", TenantID: "tenant", ConnectionID: "saved", Revision: "revision", Token: "token", Options: map[string]string{"environment": "sandbox"}}
	for _, edit := range []func(*adapter.Connection){
		func(c *adapter.Connection) { c.Endpoint = "https://attacker.invalid" }, func(c *adapter.Connection) { c.Host = "attacker.invalid" }, func(c *adapter.Connection) { c.Password = "extra" }, func(c *adapter.Connection) { c.Options = map[string]string{"tls_ca_pem": "untrusted"} }, func(c *adapter.Connection) { c.Token = "secret\r\nInjected: true" },
	} {
		c := valid
		edit(&c)
		if s, err := (Driver{Engine: "ramp"}).Open(context.Background(), c); err == nil {
			s.Close()
			t.Fatal("unsafe provider source accepted")
		}
	}
}
