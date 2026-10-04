// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/telemetry"
	"github.com/SYNEHQ/kelvo-go/internal/tracing"
)

func childTimingTestFixture() *telemetry.ChildTiming {
	return &telemetry.ChildTiming{Version: telemetry.ChildTimingVersion, TotalNS: 100, Stages: [telemetry.ChildStageCount]telemetry.ChildInterval{
		{StartNS: 0, EndNS: 10, Observed: true},
		{StartNS: 10, EndNS: 80, Observed: true},
		{StartNS: 80, EndNS: 90, Observed: true},
		{StartNS: 90, EndNS: 100, Observed: true},
		{StartNS: 10, EndNS: 20, Observed: true},
		{StartNS: 20, EndNS: 50, Observed: true},
		{StartNS: 50, EndNS: 70, Observed: true},
	}}
}

func TestOutcomeTimingDecodeIsOptionalStrictAndBounded(t *testing.T) {
	encoded, err := json.Marshal(childTimingTestFixture())
	if err != nil {
		t.Fatal(err)
	}
	valid := string(encoded)
	interval, _ := json.Marshal(childTimingTestFixture().Stages[0])
	cases := map[string]string{
		"null":                 "null",
		"wrong type":           `"private query data"`,
		"wrong object":         `{}`,
		"unknown version":      strings.Replace(valid, `"version":1`, `"version":2`, 1),
		"version null":         strings.Replace(valid, `"version":1`, `"version":null`, 1),
		"total null":           strings.Replace(valid, `"total_ns":100`, `"total_ns":null`, 1),
		"float":                strings.Replace(valid, `"total_ns":100`, `"total_ns":100.0`, 1),
		"exponent":             strings.Replace(valid, `"total_ns":100`, `"total_ns":1e2`, 1),
		"overflow":             strings.Replace(valid, `"total_ns":100`, `"total_ns":9223372036854775808`, 1),
		"negative":             strings.Replace(valid, `"start_ns":0`, `"start_ns":-1`, 1),
		"duplicate key":        strings.Replace(valid, `"version":1`, `"version":1,"version":1`, 1),
		"duplicate interval":   strings.Replace(valid, `"start_ns":0`, `"start_ns":0,"start_ns":0`, 1),
		"unknown key":          strings.Replace(valid, `"version":1`, `"version":1,"private":"value"`, 1),
		"unknown interval key": strings.Replace(valid, `"start_ns":0`, `"start_ns":0,"private":"value"`, 1),
		"missing interval key": strings.Replace(valid, `"start_ns":0,`, ``, 1),
		"wrong observed type":  strings.Replace(valid, `"observed":true`, `"observed":1`, 1),
		"short array":          strings.Replace(valid, string(interval)+",", "", 1),
		"long array":           strings.Replace(valid, `"stages":[`, `"stages":[`+string(interval)+",", 1),
		"oversized":            strings.Replace(valid, `"version":1`, `"version":`+strings.Repeat(" ", maxChildTimingBytes)+`1`, 1),
	}
	for name, timing := range cases {
		t.Run(name, func(t *testing.T) {
			var outcome Outcome
			payload := `{"stats":{"rows":17},"timing":` + timing + `}`
			if err := json.Unmarshal([]byte(payload), &outcome); err != nil || outcome.Stats.Rows != 17 || outcome.Timing != nil || !outcome.timingMalformed {
				t.Fatalf("optional timing corrupted valid core outcome: %+v, %v", outcome, err)
			}
			ctx := context.Background()
			if err := workerResultError(ctx, ctx, ctx, nil, nil, nil, nil, outcome.Error); err != nil {
				t.Fatalf("optional timing changed success: %v", err)
			}
		})
	}
	for _, duplicate := range []string{`"timing":`, `"Timing":`} {
		var outcome Outcome
		if err := json.Unmarshal([]byte(`{"stats":{"rows":17},"timing":`+valid+`,`+duplicate+valid+`}`), &outcome); err != nil || !outcome.timingMalformed || outcome.Timing != nil {
			t.Fatalf("duplicate optional report accepted: %v", err)
		}
	}
	var absent, present Outcome
	if err := json.Unmarshal([]byte(`{"stats":{"rows":17}}`), &absent); err != nil || absent.Timing != nil || absent.timingMalformed {
		t.Fatal("legacy outcome was not accepted as timing unavailable")
	}
	if err := json.Unmarshal([]byte(`{"stats":{"rows":17},"timing":`+valid+`}`), &present); err != nil || present.Timing == nil || *present.Timing != *childTimingTestFixture() {
		t.Fatalf("valid fixed report rejected: %v", err)
	}
	if err := json.Unmarshal([]byte(`{"stats":{"rows":"bad"},"timing":`+valid+`}`), &present); err == nil {
		t.Fatal("optional tolerance swallowed core outcome type error")
	}
}

func TestOutcomeEncodingDropsTimingBeforeCoreAtSizeLimit(t *testing.T) {
	outcome := Outcome{Error: &query.Error{Code: "QUERY_FAILED"}}
	core, err := json.Marshal(outcome)
	if err != nil {
		t.Fatal(err)
	}
	outcome.Error.Message = strings.Repeat("x", maxOutcomeBytes-len(core)-1)
	outcome.Timing = childTimingTestFixture()
	var output bytes.Buffer
	if err := EncodeOutcome(&output, outcome); err != nil || output.Len() != maxOutcomeBytes {
		t.Fatalf("bounded core outcome was lost: bytes=%d err=%v", output.Len(), err)
	}
	var decoded Outcome
	if err := json.Unmarshal(output.Bytes(), &decoded); err != nil || decoded.Error == nil || *decoded.Error != *outcome.Error || decoded.Timing != nil {
		t.Fatal("timing overflow truncated otherwise-valid core outcome")
	}
	output.Reset()
	outcome.Error.Message += "x"
	if err := EncodeOutcome(&output, outcome); err == nil || output.Len() != 0 {
		t.Fatal("oversized core outcome emitted partial JSON")
	}
	if err := EncodeOutcome(shortOutcomeWriter{}, Outcome{}); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("short diagnostic write was accepted: %v", err)
	}
}

type shortOutcomeWriter struct{}

func (shortOutcomeWriter) Write(data []byte) (int, error) { return len(data) - 1, nil }

func TestChildTimingUnknownClassificationDoesNotChangeOutcome(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	cases := []struct {
		name       string
		outcome    Outcome
		decodeErr  error
		terminated bool
		bound      time.Duration
		want       telemetry.ChildTimingStatus
	}{
		{"valid", Outcome{Timing: childTimingTestFixture()}, nil, false, 100, telemetry.ChildTimingObserved},
		{"valid after signaled exit", Outcome{Timing: childTimingTestFixture()}, nil, true, 100, telemetry.ChildTimingObserved},
		{"missing after context cancellation", Outcome{}, nil, false, 100, telemetry.ChildTimingMissing},
		{"optional malformed", Outcome{timingMalformed: true}, nil, false, 100, telemetry.ChildTimingMalformed},
		{"core malformed", Outcome{}, errors.New("invalid core"), false, 100, telemetry.ChildTimingMalformed},
		{"signaled missing", Outcome{}, nil, true, 100, telemetry.ChildTimingTerminated},
		{"signaled malformed", Outcome{timingMalformed: true}, nil, true, 100, telemetry.ChildTimingTerminated},
		{"parent bound exceeded", Outcome{Timing: childTimingTestFixture()}, nil, false, 99, telemetry.ChildTimingMalformed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			metrics := telemetry.New()
			recordChildTiming(metrics, WithRefreshTelemetry(canceled), tc.outcome, tc.decodeErr, tc.terminated, tc.bound)
			if metrics.Snapshot().ChildReports[telemetry.KindRefresh][tc.want] != 1 {
				t.Fatal("missing, malformed, or terminated timing misclassified")
			}
			if err := workerResultError(canceled, canceled, canceled, nil, nil, tc.decodeErr, nil, tc.outcome.Error); !errors.Is(err, context.Canceled) {
				t.Fatalf("timing changed cancellation precedence: %v", err)
			}
		})
	}
}

func TestExecutorChildTimingEnabledOnlyByMetrics(t *testing.T) {
	trace, err := tracing.New(tracing.Config{Endpoint: "https://127.0.0.1:1", SampleRatio: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer trace.Shutdown(context.Background())
	for _, tc := range []struct {
		name    string
		metrics *telemetry.Registry
		trace   *tracing.Recorder
		query   string
	}{
		{"disabled", nil, nil, "SELECT child_timing_disabled"},
		{"tracing only", nil, trace, "SELECT child_timing_disabled"},
		{"metrics enabled", telemetry.New(), nil, "SELECT child_timing_enabled"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, err := New(catalog.Config{}, query.DefaultLimits())
			if err != nil {
				t.Fatal(err)
			}
			e.Metrics, e.Tracing = tc.metrics, tc.trace
			stats, err := e.Execute(context.Background(), query.Request{SQL: tc.query}, &workerTestSink{})
			if err != nil || stats.Rows != 3 || stats.Batches != 1 {
				t.Fatalf("child timing flag or result changed: %+v %v", stats, err)
			}
			if tc.metrics != nil && tc.metrics.Snapshot().ChildReports[telemetry.KindQuery][telemetry.ChildTimingObserved] != 1 {
				t.Fatal("metrics-enabled parent did not accept bounded child report")
			}
			encoded, err := json.Marshal(stats)
			if err != nil || bytes.Contains(encoded, []byte("timing")) || bytes.Contains(encoded, []byte("stages")) {
				t.Fatal("child diagnostics escaped into query statistics")
			}
		})
	}
	encoded, err := json.Marshal(Input{})
	if err != nil || bytes.Contains(encoded, []byte("timing_version")) {
		t.Fatal("disabled input carried a timing protocol flag")
	}
}
