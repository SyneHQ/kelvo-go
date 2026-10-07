// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package application

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/client"
)

func TestExerciseStageErrorPreservesIdentityWithoutPrintingDiagnostics(t *testing.T) {
	cause := &client.Error{Code: "DEADLINE_EXCEEDED", Message: "private-dsn private-grant SELECT private_sql"}
	err := &exerciseStageError{failure: ExerciseFailure{Stage: "read_analytical_results", Code: exerciseErrorCode(cause)}, cause: cause}
	var sdkError *client.Error
	if !errors.Is(err, context.DeadlineExceeded) || !errors.As(err, &sdkError) || sdkError != cause {
		t.Fatal("stage context lost the original SDK error identity")
	}
	if err.Error() != "exercise read_analytical_results: DEADLINE_EXCEEDED" {
		t.Fatal("stage error included an arbitrary diagnostic or lost the safe code")
	}
	if exerciseErrorCode(&client.Error{Code: "private-grant", Message: "private-dsn"}) != "FAILED" {
		t.Fatal("unknown error code was exposed")
	}
}

func TestExerciseFailureReturnsStructuredStageAndOriginalCause(t *testing.T) {
	report, err := Exercise(context.Background(), Config{}, ExerciseOptions{})
	if !errors.Is(err, ErrConfiguration) || report.Failure == nil || report.Failure.Stage != "validate_options" || report.Failure.Code != "INVALID_CONFIG" {
		t.Fatal("failure did not retain its stage, code, and cause")
	}
	raw, marshalErr := json.Marshal(report)
	if marshalErr != nil || !strings.Contains(string(raw), `"failure":{"stage":"validate_options","code":"INVALID_CONFIG"}`) {
		t.Fatal("partial report lost its safe failure details")
	}
}

func TestResultFailureStatusSurvivesCallerCancellationAndOmitsRemoteMessage(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/queries/fixture_handle" {
			t.Error("failure diagnostics used a non-status endpoint")
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"fixture_handle","state":"failed","error":{"code":"QUERY_FAILED","message":"private-dsn private-grant SELECT private_sql"}}`))
	}))
	defer server.Close()
	tlsConfig := server.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
	tlsConfig.MinVersion = tls.VersionTLS13
	gateway, err := client.New(client.Config{URL: server.URL, TLSConfig: tlsConfig})
	if err != nil {
		t.Fatal(err)
	}
	defer gateway.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var report ExerciseReport
	_, err = queryFailureStatus(ctx, gateway, "fixture_handle", client.Authority{}, &report)
	if err != nil || report.QueryStateOnFailure != "failed" || report.QueryErrorCode != "QUERY_FAILED" || report.QueryStatusErrorCode != "" {
		t.Fatal("bounded status diagnostics lost terminal state or the safe code")
	}
	raw, err := json.Marshal(report)
	if err != nil || strings.Contains(string(raw), "private") || strings.Contains(string(raw), "SELECT") {
		t.Fatal("partial report exposed a remote diagnostic")
	}
}
