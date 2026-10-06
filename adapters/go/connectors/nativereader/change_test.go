// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package nativereader

import (
	"context"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/SYNEHQ/kelvo-go/adapter"
)

func writerSession(t *testing.T, engine string, h http.HandlerFunc) *Session {
	t.Helper()
	server := httptest.NewTLSServer(h)
	t.Cleanup(server.Close)
	spec := readerSpec(engine, server.URL)
	spec.Options["tls_ca_pem"] = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}))
	s, err := Open(context.Background(), spec, adapter.ProcessLimits{MemoryMB: 64})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestChangePreflightsEntireBatch(t *testing.T) {
	for _, engine := range []string{"dynamodb", "athena"} {
		t.Run(engine, func(t *testing.T) {
			var calls atomic.Int32
			s := writerSession(t, engine, func(w http.ResponseWriter, r *http.Request) { calls.Add(1); fmt.Fprint(w, `{}`) })
			oversized := "INSERT INTO facts VALUE {'id':'" + strings.Repeat("x", 8200) + "'}"
			if engine == "athena" {
				oversized = "INSERT INTO facts VALUES ('" + strings.Repeat("<", 50000) + "')"
			}
			for _, change := range []adapter.Change{
				{Statements: []string{"DELETE FROM facts WHERE id=1", oversized}},
				{Statements: []string{"DELETE FROM facts WHERE id=1", "BEGIN"}},
				{Statements: []string{"DELETE FROM facts WHERE id=1; DELETE FROM facts"}},
				{Statements: []string{"DELETE FROM facts WHERE id=1"}, Transaction: true},
				{Statements: []string{"DELETE FROM facts WHERE id=1"}, Role: "owner"},
			} {
				result, err := s.Execute(context.Background(), change)
				if err == nil || result.Attempted != 0 || result.Completed != 0 || result.Outcome != "failed" {
					t.Fatalf("result=%+v err=%v", result, err)
				}
			}
			if calls.Load() != 0 {
				t.Fatal("prevalidation let an earlier mutation escape")
			}
		})
	}
}

func TestChangePreservesCompletedPrefixOnLostAcknowledgement(t *testing.T) {
	var calls atomic.Int32
	s := writerSession(t, "dynamodb", func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		if n == 2 {
			w.Header().Set("Content-Length", "10000")
			fmt.Fprint(w, `{`)
			return
		}
		fmt.Fprint(w, `{}`)
	})
	result, err := s.Execute(context.Background(), adapter.Change{Statements: []string{"DELETE FROM facts WHERE id=1", "DELETE FROM facts WHERE id=2", "DELETE FROM facts WHERE id=3"}})
	if err == nil || result.Outcome != "unknown" || result.Completed != 1 || result.Attempted != 2 || result.AffectedRows != nil || calls.Load() != 2 {
		t.Fatalf("result=%+v err=%v calls=%d", result, err, calls.Load())
	}
}

func TestChangeCancellationBeforeDispatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var calls atomic.Int32
	s := writerSession(t, "dynamodb", func(w http.ResponseWriter, r *http.Request) { calls.Add(1); fmt.Fprint(w, `{}`) })
	cancel()
	result, err := s.Execute(ctx, adapter.Change{Statements: []string{"DELETE FROM facts WHERE id=1"}})
	if err == nil || result.Attempted != 0 || calls.Load() != 0 {
		t.Fatalf("result=%+v err=%v calls=%d", result, err, calls.Load())
	}
}
