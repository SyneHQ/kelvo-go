// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/telemetry"
)

func TestNodeHistorySanitizationAndBoundary(t *testing.T) {
	history, err := telemetry.NewHistory(telemetry.HistoryConfig{MaxEntries: 3, TTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	n := &Node{ctx: context.Background(), cfg: NodeConfig{RuntimeHistory: history}}
	recordNodeHistory(history, "query-one", time.Now().Add(-time.Second), query.NewError("PERMISSION_DENIED", "SELECT secret_password"))
	recordNodeHistory(history, "query-two", time.Now().Add(-time.Second), context.DeadlineExceeded)
	recordNodeHistory(history, "query-three", time.Now().Add(-time.Second), nil)
	w := httptest.NewRecorder()
	n.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/history", nil))
	if w.Code != http.StatusUnauthorized || strings.Contains(w.Body.String(), "query-one") {
		t.Fatal("unauthenticated history exposed")
	}
	w = httptest.NewRecorder()
	n.ServeHTTP(w, nodeRequest(http.MethodGet, "/history"))
	if w.Code != 200 || w.Header().Get("Cache-Control") != "no-store" || strings.Contains(w.Body.String(), "secret_password") {
		t.Fatalf("unsafe history response: %s", w.Body.String())
	}
	var entries []telemetry.HistoryEntry
	if err := json.Unmarshal(w.Body.Bytes(), &entries); err != nil {
		t.Fatal(err)
	}
	if len(entries) != 3 || entries[0].Category != "access" || entries[1].Outcome != "canceled" || entries[1].Category != "timeout" || entries[2].Outcome != "success" {
		t.Fatalf("wrong outcomes: %+v", entries)
	}
	w = httptest.NewRecorder()
	n.ServeHTTP(w, nodeRequest(http.MethodPost, "/history"))
	if w.Code != 405 || w.Header().Get("Allow") != "GET" {
		t.Fatal("history mutation allowed")
	}
	n.cfg.RuntimeHistory = nil
	w = httptest.NewRecorder()
	n.ServeHTTP(w, nodeRequest(http.MethodGet, "/history"))
	if w.Code != 404 {
		t.Fatal("disabled history exposed")
	}
}

type historyFailureExecutor struct{ called int }

func (e *historyFailureExecutor) Execute(_ context.Context, r query.Request, _ query.Sink) (query.Stats, error) {
	if r.SQL == "SELECT 1" {
		return query.Stats{}, nil
	}
	e.called++
	return query.Stats{}, errors.New("SELECT secret_password FROM private_source")
}
func TestNodeHistoryRecordsExecutionOnce(t *testing.T) {
	history, _ := telemetry.NewHistory(telemetry.HistoryConfig{MaxEntries: 4, TTL: time.Minute})
	store := &nodeTestStore{p: testPolicy(), jobs: map[string]Snapshot{}, queue: make(chan Delivery, 8)}
	executor := &historyFailureExecutor{}
	node, err := newNode(NodeConfig{Policy: store.p, WorkerID: "a1", RuntimeHistory: history}, store, executor)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	id := "0-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	store.add(id)
	waitFor(t, func() bool { s, _ := store.Get(context.Background(), id); return s.Job.State == Assigned })
	snapshot, _ := store.Get(context.Background(), id)
	job := snapshot.Job
	job.State = Claimed
	job.Claim = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	if _, err := store.CompareAndSwap(context.Background(), snapshot, job); err != nil {
		t.Fatal(err)
	}
	req := nodeRequest(http.MethodGet, "/internal/queries/"+id+"/results")
	req.Header.Set("X-Kelvo-Claim", job.Claim)
	node.ServeHTTP(httptest.NewRecorder(), req)
	entries := history.Entries()
	if executor.called != 1 || len(entries) != 1 || entries[0].QueryID != id || entries[0].Outcome != "error" || entries[0].Category != "unknown" {
		t.Fatalf("execution history: %+v", entries)
	}
	node.ServeHTTP(httptest.NewRecorder(), req)
	if len(history.Entries()) != 1 || executor.called != 1 {
		t.Fatal("duplicate result request re-recorded execution")
	}
}
