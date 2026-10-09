// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	operationstore "github.com/SYNEHQ/kelvo-go/internal/operations"
	"github.com/SYNEHQ/kelvo-go/internal/transportbroker/diagnostic"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/SYNEHQ/kelvo-go/resolver"
)

const diagnosticOperationID = "0001-0123456789abcdef0123456789abcdef"

func TestPrivateOpenDiagnosticsDisabledAndInvalidIDs(t *testing.T) {
	parent := context.Background()
	for _, tc := range []struct {
		enabled bool
		id      string
	}{{false, diagnosticOperationID}, {true, "secret-value"}, {true, strings.Repeat("a", 1000)}} {
		ctx, recorder := privateOpenDiagnosticContext(parent, tc.enabled, tc.id)
		if ctx != parent || recorder != nil {
			t.Fatal("Disabled or invalid diagnostics changed the operation context")
		}
		writer := &privateDiagnosticWriter{write: func([]byte) { t.Error("Disabled diagnostics started a writer") }}
		if writer.enqueue(privateOpenDiagnosticJSON(tc.id, recorder, false)) || writer.queue != nil {
			t.Fatal("Disabled diagnostics allocated a log queue")
		}
	}
	if n := testing.AllocsPerRun(100, func() { privateOpenDiagnosticContext(parent, false, diagnosticOperationID) }); n != 0 {
		t.Fatalf("Disabled diagnostics allocated: %v", n)
	}
}

func TestPrivateOpenDiagnosticLifetimeKeepsRuntimeCancellation(t *testing.T) {
	life, stopLife := context.WithCancel(context.Background())
	defer stopLife()
	operation, stopOperation := context.WithTimeout(context.Background(), time.Second)
	operation, recorder := privateOpenDiagnosticContext(operation, true, diagnosticOperationID)
	channel := privateOpenDiagnosticLifetime(life, operation)
	stopOperation()
	if channel.Err() != nil || diagnostic.FromContext(channel) != recorder {
		t.Fatal("Operation cancellation changed the retained channel lifetime")
	}
	if _, ok := channel.Deadline(); ok {
		t.Fatal("Operation deadline escaped into the retained channel lifetime")
	}
	stopLife()
	if channel.Err() != context.Canceled {
		t.Fatal("Runtime cancellation did not reach the channel")
	}
	if privateOpenDiagnosticLifetime(life, context.Background()) != life {
		t.Fatal("Disabled diagnostics wrapped the lifetime")
	}
}

func TestPrivateOpenDiagnosticSummaryIsBoundedAndRedacted(t *testing.T) {
	ctx, recorder := privateOpenDiagnosticContext(context.Background(), true, diagnosticOperationID)
	for i := 0; i < 100; i++ {
		diagnostic.Record(ctx, diagnostic.ProxyTLS, diagnostic.Failed, 503)
	}
	diagnostic.Record(ctx, diagnostic.Stage("secret-password"), diagnostic.Result("private-error"), 0)
	body := privateOpenDiagnosticJSON(diagnosticOperationID, recorder, false)
	if len(body) == 0 || len(body) > privateDiagnosticSummaryBytes || strings.Contains(string(body), "secret") || strings.Contains(string(body), "private-error") {
		t.Fatal("Diagnostic summary leaked input or exceeded its bound")
	}
	var summary privateDiagnosticSummary
	if json.Unmarshal(body, &summary) != nil || summary.CleanupConfirmed || len(summary.Events) != diagnostic.MaxEvents || summary.Dropped != 37 {
		t.Fatal("Diagnostic summary changed the bounded snapshot")
	}
	if privateOpenDiagnosticJSON("secret-invalid-id", recorder, true) != nil {
		t.Fatal("Malformed operation ID reached operator logs")
	}
}

func TestPrivateOpenDiagnosticBlockedWriterDoesNotBlockCleanup(t *testing.T) {
	entered, unblock, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	first := true
	writer := &privateDiagnosticWriter{write: func([]byte) {
		if first {
			first = false
			close(entered)
			<-unblock
		}
	}}
	if !writer.enqueue([]byte("{}")) {
		t.Fatal("First diagnostic was not queued")
	}
	<-entered
	for i := 0; i < privateDiagnosticQueueSize; i++ {
		if !writer.enqueue([]byte("{}")) {
			t.Fatal("Diagnostic queue filled before its declared bound")
		}
	}
	go func() {
		defer close(finished) // Models the caller's resource-release defer.
		if writer.enqueue([]byte("{}")) {
			t.Error("Full diagnostic queue accepted another report")
		}
	}()
	select {
	case <-finished:
	case <-time.After(time.Second):
		close(unblock)
		t.Fatal("A blocked operator log writer delayed caller cleanup")
	}
	close(writer.queue)
	close(unblock)
}

type diagnosticRoundTrip func(*http.Request) (*http.Response, error)

func (f diagnosticRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestPrivateOpenResolverDiagnosticContainsStatusNotResponse(t *testing.T) {
	ctx, recorder := privateOpenDiagnosticContext(context.Background(), true, diagnosticOperationID)
	client := &ConnectionResolver{url: "https://resolver.invalid" + resolver.QueryPath, slots: make(chan struct{}, 1), client: &http.Client{Transport: diagnosticRoundTrip(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusForbidden, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader("private-response-secret"))}, nil
	})}}
	if _, err := client.resolveOperationMode(ctx, operationstore.Record{}, operations.Request{}, true); err == nil {
		t.Fatal("Forbidden resolver response succeeded")
	}
	snapshot := recorder.Snapshot()
	if len(snapshot.Events) != 2 || snapshot.Events[0].Stage != diagnostic.FreshResolver || snapshot.Events[1].Result != diagnostic.ScopeDenied || snapshot.Events[1].HTTPStatus != 403 {
		t.Fatal("Resolver status diagnostic is incomplete")
	}
	if strings.Contains(string(privateOpenDiagnosticJSON(diagnosticOperationID, recorder, false)), "private-response-secret") {
		t.Fatal("Resolver response reached operator diagnostics")
	}
}
