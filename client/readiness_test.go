// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package client

import (
	"context"
	"crypto/tls"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestReadyValidatesBoundedResponse(t *testing.T) {
	for _, tc := range []struct {
		name, body, code string
		status           int
	}{
		{"ready", `{"status":"ready"}`, "", 200},
		{"unavailable", `private remote diagnostics`, "UNAVAILABLE", 503},
		{"wrong_state", `{"status":"ok"}`, "PROTOCOL_ERROR", 200},
		{"duplicate", `{"status":"ready","status":"ready"}`, "PROTOCOL_ERROR", 200},
		{"trailing", `{"status":"ready"}{}`, "PROTOCOL_ERROR", 200},
		{"too_large", strings.Repeat(" ", 1025), "PROTOCOL_ERROR", 200},
		{"redirect", "", "UNAVAILABLE", 302},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			s := clientFixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.Method != "GET" || r.URL.RequestURI() != "/ready" || r.Header.Get("Authorization") != "Bearer "+clientFixtureToken {
					t.Error("unexpected readiness request")
				}
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Location", "/unexpected")
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			}, tls.VersionTLS13)
			c := clientFixtureClient(t, clientFixtureConfig(t, s))
			err := c.Ready(context.Background())
			if tc.code == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else {
				clientFixtureError(t, err, tc.code)
			}
			if calls.Load() != 1 {
				t.Fatal("readiness retried")
			}
		})
	}
}

func TestReadyUsesControlAdmissionAndCloseCancellation(t *testing.T) {
	entered := make(chan struct{})
	s := clientFixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-r.Context().Done()
	}, tls.VersionTLS13)
	cfg := clientFixtureConfig(t, s)
	cfg.MaxControlConcurrent = 1
	c := clientFixtureClient(t, cfg)
	// A busy data path must not consume the separate health/control slot.
	_, release, err := c.begin(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	done := make(chan error, 1)
	go func() { done <- c.Ready(context.Background()) }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("probe did not start")
	}
	clientFixtureError(t, c.Ready(context.Background()), "RESOURCE_EXHAUSTED")
	release()
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	clientFixtureError(t, <-done, "CANCELLED")
	clientFixtureError(t, c.Ready(context.Background()), "CLIENT_CLOSED")
}

func TestReadyHonorsDeadline(t *testing.T) {
	s := clientFixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}, tls.VersionTLS13)
	c := clientFixtureClient(t, clientFixtureConfig(t, s))
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	clientFixtureError(t, c.Ready(ctx), "DEADLINE_EXCEEDED")
}
