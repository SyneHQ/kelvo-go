// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package query_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/federation"
	"github.com/SYNEHQ/kelvo-go/query"
)

func TestRequestJSONWireContract(t *testing.T) {
	for name, want := range map[string]string{
		"native":    `{"delegation":"grant","sql":"SELECT ?","mode":"native","connection_id":"orders","parameters":[{"type":"uint64","value":"18446744073709551615"}]}`,
		"federated": `{"scan_diagnostics":true,"sql":"SELECT * FROM orders","mode":"federated","sources":["warehouse","billing"]}`,
		"mongo":     `{"sql":"","mode":"native","connection_id":"events","mongo":{"collection":"events","pipeline":[{"$match":{"id":9007199254740993}}]}}`,
	} {
		t.Run(name, func(t *testing.T) {
			var request query.Request
			if err := json.Unmarshal([]byte(want), &request); err != nil {
				t.Fatal(err)
			}
			if err := query.ValidateRequest(request); err != nil {
				t.Fatal(err)
			}
			got, err := json.Marshal(request)
			if err != nil || string(got) != want {
				t.Fatalf("request wire contract changed: %s, %v", got, err)
			}
		})
	}
}

func TestStatsJSONWireContract(t *testing.T) {
	want := `{"scan_diagnostics":{"version":1,"scope":"query","scans":[]},"federation":[{"source":"warehouse","table":"orders","scans":1,"rows_fetched":3,"arrow_bytes_fetched":24,"batches_fetched":1,"source_wire_bytes":40}],"rows":2,"batches":1,"arrow_bytes":16,"wire_bytes":32,"source_wire_bytes":40,"backend":"duckdb","prepare_ns":7,"duration_ns":11,"engine_streaming":false,"accelerations":[{"dataset":"daily_orders","generation":"v1","refreshed_at":"2026-10-07T00:00:00Z"}]}`
	stats := query.Stats{
		ScanDiagnostics: &federation.ScanDiagnostics{Version: 1, Scope: "query", Scans: []federation.ScanDiagnostic{}},
		Federation:      []query.FederationScan{{Source: "warehouse", Table: "orders", Scans: 1, Rows: 3, Bytes: 24, Batches: 1, SourceWireBytes: 40}},
		Rows:            2, Batches: 1, Bytes: 16, WireBytes: 32, SourceWireBytes: 40,
		Backend: "duckdb", PrepareNS: 7, DurationNS: 11,
		Accelerations: []query.AccelerationVersion{{Dataset: "daily_orders", Generation: "v1", RefreshedAt: time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC)}},
	}
	got, err := json.Marshal(stats)
	if err != nil || string(got) != want {
		t.Fatalf("stats wire contract changed: %s, %v", got, err)
	}
	var decoded query.Stats
	if err := json.Unmarshal([]byte(want), &decoded); err != nil || !reflect.DeepEqual(stats, decoded) {
		t.Fatalf("stats did not round trip: %+v, %v", decoded, err)
	}
}

func TestPublicErrorIdentityAndSafeWireContract(t *testing.T) {
	original := query.NewError("INVALID_ARGUMENT", "Invalid source")
	wrapped := fmt.Errorf("execution failed: %w", original)
	var typed *query.Error
	if !errors.As(wrapped, &typed) || query.PublicError(wrapped) != typed {
		t.Fatal("public query error lost its identity")
	}
	for _, test := range []struct {
		err  error
		want string
	}{
		{wrapped, `{"code":"INVALID_ARGUMENT","message":"Invalid source"}`},
		{fmt.Errorf("worker: %w", context.DeadlineExceeded), `{"code":"DEADLINE_EXCEEDED","message":"Query deadline exceeded"}`},
		{context.Canceled, `{"code":"CANCELLED","message":"Query cancelled"}`},
		{errors.New("private driver failure"), `{"code":"QUERY_FAILED","message":"Query failed"}`},
	} {
		got, err := json.Marshal(query.PublicError(test.err))
		if err != nil || string(got) != test.want {
			t.Fatalf("public error wire contract changed: %s, %v", got, err)
		}
	}
}
