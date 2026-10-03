// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/telemetry"
)

func TestSourceHealthRecordsOnlyEligibleNativeOutcomes(t *testing.T) {
	for _, tc := range []struct{ code, state, category string }{
		{"", "succeeded", "none"},
		{"UNAUTHENTICATED", "failed", "access"},
		{"PERMISSION_DENIED", "failed", "access"},
		{"FORBIDDEN", "failed", "access"},
		{"UNAUTHORIZED", "failed", "access"},
		{"UNAVAILABLE", "failed", "unavailable"},
		{"QUERY_FAILED", "failed", "query"},
		{"RESOURCE_EXHAUSTED", "unknown", "none"},
		{"CONFIGURATION_ERROR", "unknown", "none"},
		{"INVALID_ARGUMENT", "unknown", "none"},
		{"CANCELLED", "unknown", "none"},
		{"DEADLINE_EXCEEDED", "unknown", "none"},
		{"INTERNAL", "unknown", "none"},
		{"secret-dynamic-code", "unknown", "none"},
	} {
		t.Run(tc.code, func(t *testing.T) {
			h, _ := telemetry.NewSourceHealth(telemetry.SourceHealthConfig{ObservationTTL: time.Minute}, []string{"source"})
			var child *query.Error
			var final error
			if tc.code != "" {
				child = &query.Error{Code: tc.code, Message: "password=secret SQL"}
				final = fmt.Errorf("wrapped: %w", child)
			}
			recordSourceHealth(h, query.Request{Mode: "native", ConnectionID: "source"}, child, final, true)
			e := h.Entries()[0]
			if e.State != tc.state || e.Category != tc.category {
				t.Fatalf("unexpected state: %+v", e)
			}
		})
	}
}

func TestSourceHealthIgnoresParentFailureAndNonNativePaths(t *testing.T) {
	native := query.Request{Mode: "native", ConnectionID: "source"}
	child := &query.Error{Code: "UNAVAILABLE", Message: "private upstream error"}
	for name, tc := range map[string]struct {
		request query.Request
		child   *query.Error
		result  error
		decoded bool
	}{
		"parent canceled":       {native, child, context.Canceled, true},
		"parent deadline":       {native, child, context.DeadlineExceeded, true},
		"admission failure":     {native, nil, query.NewError("UNAVAILABLE", "private quota error"), true},
		"transport failure":     {native, nil, errors.New("private broken stream"), true},
		"different typed error": {native, child, query.NewError("UNAVAILABLE", "private sink error"), true},
		"undecodable outcome":   {native, nil, nil, false},
		"federation":            {query.Request{Mode: "federated", ConnectionID: "source", Sources: []string{"source"}}, nil, nil, true},
		"snapshot":              {query.Request{Mode: "federated", Sources: []string{"source"}}, nil, nil, true},
		"unknown source":        {query.Request{Mode: "native", ConnectionID: "unconfigured"}, nil, nil, true},
	} {
		t.Run(name, func(t *testing.T) {
			h, _ := telemetry.NewSourceHealth(telemetry.SourceHealthConfig{ObservationTTL: time.Minute}, []string{"source"})
			recordSourceHealth(h, tc.request, tc.child, tc.result, tc.decoded)
			if h.Entries()[0].State != "unknown" || h.Entries()[0].LastObserved != nil {
				t.Fatal("ineligible work altered source health")
			}
		})
	}
}

// Run through the real disposable-process parent path; the child fixture emits
// Arrow IPC and completion metadata. This detects a missing Execute hook.
func TestExecutorSourceHealthObservesCompleteNativeTransfer(t *testing.T) {
	t.Setenv("KELVO_SOURCE_SELECTED_URL", "https://source.example")
	t.Setenv("KELVO_SOURCE_SELECTED_TOKEN", "fixture-source-token")
	for _, key := range []string{"KELVO_SOURCE_OTHER_TOKEN", "KELVO_TOKEN", "KELVO_TENANT_A_NATS_PASSWORD"} {
		t.Setenv(key, "must-stay-parent")
	}
	c := catalog.Config{Sources: []catalog.Source{{ID: "selected", Type: "databricks", URLEnv: "KELVO_SOURCE_SELECTED_URL", TokenEnv: "KELVO_SOURCE_SELECTED_TOKEN"}}}
	executor, err := New(c, query.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	executor.SourceHealth, _ = telemetry.NewSourceHealth(telemetry.SourceHealthConfig{ObservationTTL: time.Minute}, []string{"selected"})
	stats, err := executor.Execute(context.Background(), query.Request{Mode: "native", ConnectionID: "selected", SQL: "SELECT source_environment"}, &workerTestSink{})
	if err != nil || stats.Rows != 3 {
		t.Fatalf("native transfer failed: %v", err)
	}
	if e := executor.SourceHealth.Entries()[0]; e.State != "succeeded" || e.LastObserved == nil {
		t.Fatalf("native transfer missing observation: %+v", e)
	}
	before := *executor.SourceHealth.Entries()[0].LastObserved
	executor.Binary = "/missing-source-health-test-binary"
	_, err = executor.Execute(context.Background(), query.Request{Mode: "native", ConnectionID: "selected", SQL: "SELECT source_environment"}, &workerTestSink{})
	if err == nil || !executor.SourceHealth.Entries()[0].LastObserved.Equal(before) {
		t.Fatal("process failure changed source observation")
	}
}
