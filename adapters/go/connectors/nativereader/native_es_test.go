package nativereader

import (
	"context"
	"encoding/json"
	"encoding/pem"
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
)

func elasticFixture(t *testing.T, handler http.HandlerFunc) *Session {
	t.Helper()
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	spec := readerSpec("elasticsearch", server.URL)
	spec.Database = "logs"
	spec.Options["tls_ca_pem"] = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}))
	s, err := Open(context.Background(), spec, adapter.ProcessLimits{MemoryMB: 64})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}
func nativeES(t *testing.T, input string) adapter.Native {
	t.Helper()
	kind, spec, err := provider.ParseElasticsearchInvocation(input)
	if err != nil {
		t.Fatal(err)
	}
	return adapter.Native{Kind: kind, Spec: *spec, Limits: adapter.Limits{MaxRows: 100, MaxBytes: 1 << 20, BatchRows: 10}}
}

type failingSink struct{}

func (failingSink) Schema(*arrow.Schema) error    { return errors.New("delivery failed") }
func (failingSink) Write(arrow.RecordBatch) error { return errors.New("delivery failed") }
func TestElasticsearchNativeAcknowledgementsAreNotReplayed(t *testing.T) {
	for _, scenario := range []string{"success", "delivery-failed", "partial-bulk", "lost-reply"} {
		t.Run(scenario, func(t *testing.T) {
			var writes atomic.Int32
			s := elasticFixture(t, func(w http.ResponseWriter, r *http.Request) {
				writes.Add(1)
				u, p, ok := r.BasicAuth()
				if !ok || u != "fixture-user" || p != "fixture-password" {
					t.Error("missing current credentials")
				}
				if scenario == "lost-reply" {
					conn, _, _ := w.(http.Hijacker).Hijack()
					conn.Close()
					return
				}
				w.Header().Set("Content-Type", "application/json")
				if scenario == "partial-bulk" {
					fmt.Fprint(w, `{"errors":true,"items":[{"index":{"status":201}},{"index":{"status":409}}]}`)
				} else {
					fmt.Fprint(w, `{"result":"created","_id":"one","_version":9007199254740993}`)
				}
			})
			call := nativeES(t, `PUT /logs/_doc/one {"id":9007199254740993}`)
			if scenario == "partial-bulk" {
				call = nativeES(t, "POST /logs/_bulk\n{\"index\":{\"_id\":\"one\"}}\n{\"id\":1}\n{\"index\":{\"_id\":\"two\"}}\n{\"id\":2}\n")
			}
			capture := &captureSink{}
			var sink adapter.Sink = capture
			if scenario == "delivery-failed" {
				sink = failingSink{}
			}
			result, err := s.RunNative(context.Background(), call, sink)
			if writes.Load() != 1 {
				t.Fatal("request replayed")
			}
			switch scenario {
			case "success":
				if err != nil || result.Outcome != operations.Completed || result.Effect != operations.EffectCommitted || len(capture.documents) != 1 || !strings.Contains(string(capture.documents[0]), "9007199254740993") {
					t.Fatal(result, err)
				}
			case "delivery-failed":
				if err == nil || result.Outcome != operations.Completed || result.Effect != operations.EffectCommitted {
					t.Fatal("delivery erased source effect", result, err)
				}
			case "partial-bulk":
				if err == nil || result.Outcome != operations.Failed || result.Effect != operations.EffectPartial {
					t.Fatal("partial commit lost", result, err)
				}
			case "lost-reply":
				if err == nil || result.Outcome != operations.OutcomeUnknown || result.Effect != operations.EffectUnknown {
					t.Fatal("uncertain write misreported", result, err)
				}
			}
		})
	}
}
func TestElasticsearchNativeRefusesDowngradedOrForeignRequestsBeforeIO(t *testing.T) {
	var calls atomic.Int32
	s := elasticFixture(t, func(w http.ResponseWriter, r *http.Request) { calls.Add(1); fmt.Fprint(w, `{}`) })
	call := nativeES(t, `PUT /logs/_doc/one {"id":1}`)
	call.Kind = operations.NativeRead
	call.Spec.ReturnResult = false
	if result, err := s.RunNative(context.Background(), call, &captureSink{}); err == nil || result.Effect != operations.EffectNone {
		t.Fatal("write downgraded to read")
	}
	call = nativeES(t, `PUT /foreign/_doc/one {"id":1}`)
	if _, err := s.RunNative(context.Background(), call, &captureSink{}); err == nil {
		t.Fatal("foreign index accepted")
	}
	if calls.Load() != 0 {
		t.Fatal("denied request reached source")
	}
}

func TestElasticsearchWriteEffectRequiresProviderAcknowledgement(t *testing.T) {
	for _, test := range []struct {
		path, body string
		outcome    operations.Outcome
		effect     operations.Effect
		fail       bool
	}{
		{"/logs", `{}`, operations.OutcomeUnknown, operations.EffectUnknown, true},
		{"/logs", `{"acknowledged":false}`, operations.OutcomeUnknown, operations.EffectUnknown, true},
		{"/logs", `{"acknowledged":true}`, operations.Completed, operations.EffectCommitted, false},
		{"/logs/_doc/one", `{}`, operations.OutcomeUnknown, operations.EffectUnknown, true},
		{"/logs/_update/one", `{"result":"noop"}`, operations.Completed, operations.EffectNone, false},
		{"/logs/_bulk", `{"errors":true,"items":[{"index":{"status":409}}]}`, operations.Failed, operations.EffectNone, true},
		{"/logs/_bulk", `{"errors":false,"items":[{"index":{"status":201}}]}`, operations.Completed, operations.EffectCommitted, false},
		{"/logs/_bulk", `{"errors":true,"items":[{"index":{"status":500}}]}`, operations.OutcomeUnknown, operations.EffectUnknown, true},
	} {
		outcome, effect, err := elasticWriteOutcome(test.path, json.RawMessage(test.body))
		if outcome != test.outcome || effect != test.effect || (err != nil) != test.fail {
			t.Fatal(test.path, outcome, effect, err)
		}
	}
}

func TestElasticsearchTimeoutStatusDoesNotProveNoEffect(t *testing.T) {
	s := elasticFixture(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusRequestTimeout) })
	result, err := s.RunNative(context.Background(), nativeES(t, `PUT /logs/_doc/one {"value":1}`), &captureSink{})
	if err == nil || result.Outcome != operations.OutcomeUnknown || result.Effect != operations.EffectUnknown {
		t.Fatal("timeout was treated as definitely not committed", result, err)
	}
}
