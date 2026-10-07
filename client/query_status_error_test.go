// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package client

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/query"
)

func TestQueryStatusPreservesKnownCodesWithoutRemoteDiagnostics(t *testing.T) {
	for _, test := range []struct {
		code, message string
	}{
		{"CONFIGURATION_ERROR", "Kelvo query configuration is invalid"},
		{"DATASET_UNAVAILABLE", "Kelvo dataset is unavailable"},
		{"UNSUPPORTED", "Kelvo query capability or result type is unsupported"},
		{"SCHEMA_MISMATCH", "Kelvo dataset schema is incompatible"},
		{"WORKER_LOST", "Kelvo query worker was lost before completion"},
		{"UNAUTHENTICATED", "Kelvo service authentication failed"},
		{"PERMISSION_DENIED", "Kelvo query access denied"},
		{"INVALID_ARGUMENT", "Invalid Kelvo query request"},
		{"RESOURCE_EXHAUSTED", "Kelvo query limit reached"},
		{"UNAVAILABLE", "Kelvo service unavailable"},
		{"QUERY_FAILED", "Kelvo query failed"},
		{"CANCELLED", "Kelvo query cancelled"},
		{"DEADLINE_EXCEEDED", "Kelvo query deadline exceeded"},
	} {
		t.Run(test.code, func(t *testing.T) {
			checkQueryStatusError(t, test.code, test.code, test.message)
		})
	}
}

func TestQueryStatusRejectsUnknownAndDecoratedErrorCodes(t *testing.T) {
	for name, code := range map[string]string{
		"unknown":            "UNKNOWN_PRIVATE_CODE",
		"empty":              "",
		"lowercase":          "configuration_error",
		"padded":             " CONFIGURATION_ERROR ",
		"diagnostic_as_code": "remote-secret-diagnostic",
		"decorated_known":    "CONFIGURATION_ERROR: remote-secret-diagnostic",
	} {
		t.Run(name, func(t *testing.T) {
			checkQueryStatusError(t, code, "UNAVAILABLE", "Kelvo service unavailable")
		})
	}
}

func checkQueryStatusError(t *testing.T, remoteCode, wantCode, wantMessage string) {
	t.Helper()
	server := clientFixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/queries/fixture_handle" {
			t.Error("unexpected status request")
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(QueryStatus{
			ID: "fixture_handle", State: "failed",
			Error: &query.Error{Code: remoteCode, Message: "remote-secret-diagnostic " + clientFixtureToken},
		})
	}, tls.VersionTLS13)
	c := clientFixtureClient(t, clientFixtureConfig(t, server))
	status, err := c.Status(context.Background(), "fixture_handle", Authority{})
	if err != nil {
		t.Fatalf("status request failed: %v", err)
	}
	if status.ID != "fixture_handle" || status.State != "failed" || status.Error == nil {
		t.Fatal("status lost the terminal query failure")
	}
	if status.Error.Code != wantCode || status.Error.Message != wantMessage || status.Error.Error() != wantMessage {
		t.Fatalf("safe error = %q / %q; want %q / %q", status.Error.Code, status.Error.Message, wantCode, wantMessage)
	}
	raw, err := json.Marshal(status)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "remote-secret-diagnostic") || strings.Contains(string(raw), clientFixtureToken) {
		t.Fatal("status exposed a remote diagnostic or credential marker")
	}
}
