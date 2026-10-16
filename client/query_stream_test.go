// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package client

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/query"
)

func streamFixtureResponse(w http.ResponseWriter, data []byte, receipt string) {
	w.Header().Set("Content-Type", "application/vnd.apache.arrow.stream")
	w.Header().Set("Kelvo-Result-Completion", "durable-eos-v1")
	w.Header().Set("Kelvo-Query-Id", "fixture_handle")
	w.Header().Set(streamHeader, "1")
	if receipt != "" {
		w.Header().Set("Trailer", receiptTrailer)
	}
	_, _ = w.Write(data)
	if receipt != "" {
		w.Header().Set(receiptTrailer, receipt)
	}
}

func goodReceipt(data []byte) string {
	b, _ := json.Marshal(streamReceipt{ID: "fixture_handle", State: "succeeded", Stats: query.Stats{Rows: 3, Batches: 1, WireBytes: int64(len(data))}})
	return string(b)
}

func TestQueryStreamIsOneRequest(t *testing.T) {
	data := clientFixtureIPC(t)
	var calls atomic.Int32
	var bypass atomic.Bool
	server := clientFixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Method != http.MethodPost || r.URL.Path != "/v1/queries/stream" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(404)
			return
		}
		var got query.Request
		_ = json.NewDecoder(r.Body).Decode(&got)
		bypass.Store(got.BypassCache)
		streamFixtureResponse(w, data, goodReceipt(data))
	}, 0)
	c := clientFixtureClient(t, clientFixtureConfig(t, server))
	request := clientFixtureRequest()
	request.BypassCache = true
	stats, err := c.QueryStream(context.Background(), request, Authority{}, &clientFixtureSink{})
	if err != nil || stats.Rows != 3 || stats.Batches != 1 || stats.Server.Rows != 3 || stats.WireBytes != int64(len(data)) {
		t.Fatalf("stream: %+v %v", stats, err)
	}
	if calls.Load() != 1 || !bypass.Load() {
		t.Fatalf("calls=%d bypass=%v", calls.Load(), bypass.Load())
	}
}

func TestQueryStreamRejectsMissingOrWrongReceipt(t *testing.T) {
	data := clientFixtureIPC(t)
	wrong, _ := json.Marshal(streamReceipt{ID: "fixture_handle", State: "succeeded", Stats: query.Stats{Rows: 2, Batches: 1, WireBytes: int64(len(data))}})
	failed, _ := json.Marshal(streamReceipt{ID: "fixture_handle", State: "failed", Stats: query.Stats{Rows: 3, Batches: 1, WireBytes: int64(len(data))}})
	for name, receipt := range map[string]string{"missing": "", "wrong rows": string(wrong), "not succeeded": string(failed), "garbage": "{"} {
		t.Run(name, func(t *testing.T) {
			var cancels atomic.Int32
			server := clientFixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/queries/fixture_handle/cancel" {
					cancels.Add(1)
					w.Header().Set("Content-Type", "application/json")
					_, _ = io.WriteString(w, `{"id":"fixture_handle","state":"cancelled"}`)
					return
				}
				streamFixtureResponse(w, data, receipt)
			}, 0)
			c := clientFixtureClient(t, clientFixtureConfig(t, server))
			_, err := c.QueryStream(context.Background(), clientFixtureRequest(), Authority{}, &clientFixtureSink{})
			clientFixtureError(t, err, "PROTOCOL_ERROR")
			if cancels.Load() != 1 {
				t.Fatalf("cancels=%d", cancels.Load())
			}
		})
	}
}

func TestQueryStreamUnsupportedNodeAndRetryHint(t *testing.T) {
	for _, status := range []int{http.StatusNotFound, http.StatusMethodNotAllowed} {
		server := clientFixtureServer(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(status) }, 0)
		c := clientFixtureClient(t, clientFixtureConfig(t, server))
		_, err := c.QueryStream(context.Background(), clientFixtureRequest(), Authority{}, &clientFixtureSink{})
		if !errors.Is(err, ErrStreamUnsupported) {
			t.Fatalf("status %d: %v", status, err)
		}
	}
	// A 404 that carries the node's marker is a real error, never a fallback.
	for _, status := range []int{http.StatusNotFound, http.StatusMethodNotAllowed, http.StatusNotImplemented} {
		server := clientFixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set(streamHeader, "1")
			w.WriteHeader(status)
		}, 0)
		c := clientFixtureClient(t, clientFixtureConfig(t, server))
		_, err := c.QueryStream(context.Background(), clientFixtureRequest(), Authority{}, &clientFixtureSink{})
		if err == nil || errors.Is(err, ErrStreamUnsupported) {
			t.Fatalf("status %d with marker: %v", status, err)
		}
	}
	// A 200 without the marker is not our endpoint.
	server := clientFixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
		data := clientFixtureIPC(t)
		w.Header().Set("Content-Type", "application/vnd.apache.arrow.stream")
		w.Header().Set("Kelvo-Result-Completion", "durable-eos-v1")
		w.Header().Set("Kelvo-Query-Id", "fixture_handle")
		_, _ = w.Write(data)
	}, 0)
	c0 := clientFixtureClient(t, clientFixtureConfig(t, server))
	_, err0 := c0.QueryStream(context.Background(), clientFixtureRequest(), Authority{}, &clientFixtureSink{})
	clientFixtureError(t, err0, "PROTOCOL_ERROR")
	server = clientFixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(streamHeader, "1")
		w.Header().Set("Retry-After", "3")
		w.WriteHeader(http.StatusTooManyRequests)
	}, 0)
	c := clientFixtureClient(t, clientFixtureConfig(t, server))
	_, err := c.QueryStream(context.Background(), clientFixtureRequest(), Authority{}, &clientFixtureSink{})
	clientFixtureError(t, err, "RESOURCE_EXHAUSTED")
	var e *Error
	if !errors.As(err, &e) || e.RetryAfter != 3*time.Second {
		t.Fatalf("retry hint %v", err)
	}
}

// A 429 from any endpoint carries the node's retry hint, so a caller can tell
// capacity (retry) from a result limit.
func TestEveryEndpointsCapacityRefusalCarriesTheRetryHint(t *testing.T) {
	for _, withHint := range []bool{true, false} {
		server := clientFixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
			if withHint {
				w.Header().Set("Retry-After", "2")
			}
			w.WriteHeader(http.StatusTooManyRequests)
		}, 0)
		c := clientFixtureClient(t, clientFixtureConfig(t, server))
		_, submitErr := c.Submit(context.Background(), clientFixtureRequest(), Authority{})
		_, resultsErr := c.Results(context.Background(), "fixture_handle", Authority{}, &clientFixtureSink{})
		_, statusErr := c.Status(context.Background(), "fixture_handle", Authority{})
		for name, err := range map[string]error{"submit": submitErr, "results": resultsErr, "status": statusErr} {
			clientFixtureError(t, err, "RESOURCE_EXHAUSTED")
			var e *Error
			want := time.Duration(0)
			if withHint {
				want = 2 * time.Second
			}
			if !errors.As(err, &e) || e.RetryAfter != want {
				t.Errorf("%s with hint=%v: retry after %v, want %v", name, withHint, e.RetryAfter, want)
			}
		}
	}
}
