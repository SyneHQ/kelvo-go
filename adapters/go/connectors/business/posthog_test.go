package business

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/SYNEHQ/kelvo-go/provider"
)

func posthogFixture(t *testing.T, handler http.HandlerFunc) *Session {
	t.Helper()
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	c, err := New(Config{Type: "posthog", Token: "fixture-token", Endpoint: server.URL, Project: "42"})
	if err != nil {
		t.Fatal(err)
	}
	c.http.Transport = server.Client().Transport
	s := &Session{client: c, engine: "posthog"}
	t.Cleanup(func() { s.Close() })
	return s
}

func posthogNative(t *testing.T, query string) adapter.Native {
	t.Helper()
	kind, spec, err := provider.InvocationText("posthog", query)
	if err != nil {
		t.Fatal(err)
	}
	return adapter.Native{Kind: kind, Spec: *spec, Limits: adapter.Limits{MaxRows: 100, MaxBytes: 2 << 20, BatchRows: 16}}
}

func TestPostHogQueryUsesSavedProjectAndExactValues(t *testing.T) {
	var calls atomic.Int64
	s := posthogFixture(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/api/projects/42/query/" || r.Header.Get("Authorization") != "Bearer fixture-token" || r.Method != "POST" {
			t.Error("invalid scoped request")
		}
		var body struct {
			Refresh string `json:"refresh"`
			Query   struct{ Kind, Query string }
		}
		if json.NewDecoder(r.Body).Decode(&body) != nil || body.Refresh != "force_blocking" || body.Query.Kind != "HogQLQuery" || body.Query.Query != "SELECT id, amount FROM events LIMIT 1" {
			t.Error("invalid query contract")
		}
		fmt.Fprint(w, `{"columns":["id","amount","nullable"],"results":[[9007199254740993,12345678901234567890.1234567,null]]}`)
	})
	sink := &capture{}
	result, err := s.RunNative(context.Background(), posthogNative(t, "SELECT id, amount FROM events LIMIT 1"), sink)
	if err != nil || result.Outcome != operations.Completed || result.Effect != operations.EffectNone || calls.Load() != 1 || len(sink.documents) != 1 {
		t.Fatal(result, err, calls.Load())
	}
	for _, want := range []string{"9007199254740993", "12345678901234567890.1234567", `"nullable":null`} {
		if !strings.Contains(sink.documents[0], want) {
			t.Fatal("lost exact result", sink.documents)
		}
	}
}

func TestPostHogPaginationRemainsInSavedProject(t *testing.T) {
	for _, next := range []string{"/api/projects/42/events/?cursor=2", "https://evil.invalid/api/projects/42/events/", "/api/projects/43/events/", "/api/projects/42/../43/events/"} {
		t.Run(next, func(t *testing.T) {
			var calls atomic.Int64
			s := posthogFixture(t, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.URL.RawQuery == "cursor=2" {
					fmt.Fprint(w, `{"results":[{"id":2}],"next":null}`)
					return
				}
				encoded, _ := json.Marshal(next)
				fmt.Fprintf(w, `{"results":[{"id":1}],"next":%s}`, encoded)
			})
			sink := &capture{}
			result, err := s.RunNative(context.Background(), posthogNative(t, "GET /api/projects/:project_id/events/"), sink)
			if strings.Contains(next, "cursor=2") {
				if err != nil || result.Stats.Rows != 2 || calls.Load() != 2 {
					t.Fatal(result, err, calls.Load())
				}
			} else if err == nil || calls.Load() != 1 {
				t.Fatal("unsafe next followed", err, calls.Load())
			}
		})
	}
}

func TestPostHogMutationEffectsSurviveResultFailures(t *testing.T) {
	for _, mode := range []string{"lost", "malformed", "delivery", "empty"} {
		t.Run(mode, func(t *testing.T) {
			var calls atomic.Int64
			s := posthogFixture(t, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if mode == "lost" {
					conn, _, err := w.(http.Hijacker).Hijack()
					if err != nil {
						t.Error(err)
						return
					}
					conn.Close()
					return
				}
				if mode == "empty" {
					w.WriteHeader(204)
					return
				}
				if mode == "malformed" {
					fmt.Fprint(w, `private non-json`)
					return
				}
				fmt.Fprint(w, `{"id":9007199254740993}`)
			})
			sink := &capture{failure: mode == "delivery"}
			result, err := s.RunNative(context.Background(), posthogNative(t, `POST /api/projects/:project_id/annotations/ {"content":"test"}`), sink)
			if calls.Load() != 1 {
				t.Fatal("mutation replayed", calls.Load())
			}
			if mode == "lost" {
				if err == nil || result.Outcome != operations.OutcomeUnknown || result.Effect != operations.EffectUnknown {
					t.Fatal(result, err)
				}
			} else if result.Outcome != operations.Completed || result.Effect != operations.EffectCommitted || (mode != "empty" && err == nil) {
				t.Fatal(result, err)
			}
			if err != nil && strings.Contains(err.Error(), "private non-json") {
				t.Fatal("body leaked")
			}
		})
	}
}

func TestPostHogIncompleteResultsFail(t *testing.T) {
	for _, response := range []string{
		`{"columns":["id","id"],"results":[[1,2]]}`,
		`{"columns":["id"],"results":[[1,2]]}`,
		`{"query_status":{"complete":false},"results":[]}`,
		`{"results":[],"hasMore":true}`,
		`{"results":[],"next":"/api/projects/42/events/"}`,
	} {
		s := posthogFixture(t, func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, response) })
		if result, err := s.RunNative(context.Background(), posthogNative(t, "SELECT 1"), &capture{}); err == nil || result.Outcome == operations.Completed {
			t.Fatal("incomplete accepted", response, result, err)
		}
	}
}

func TestPostHogRejectsCrossProjectBeforeDispatch(t *testing.T) {
	var calls atomic.Int64
	s := posthogFixture(t, func(w http.ResponseWriter, r *http.Request) { calls.Add(1) })
	if _, err := s.RunNative(context.Background(), posthogNative(t, "GET /api/projects/43/events/"), &capture{}); err == nil || calls.Load() != 0 {
		t.Fatal("cross-project request dispatched", err, calls.Load())
	}
}
