// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"bytes"
	"context"
	"log"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/admission"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/telemetry"
)

func captureLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })
	return &buf
}

func TestFailedReadLogsCodeClassStageDurationOnly(t *testing.T) {
	buf := captureLog(t)
	phases := newExecutionPhases(time.Now(), false)
	phases.enter(telemetry.PhaseNodeAdmission)
	secret := query.NewError("QUERY_FAILED", "password=hunter2 SELECT secret FROM t")
	logFailedRead(context.Background(), time.Now().Add(-1500*time.Millisecond), secret, &phases)
	line := buf.String()
	for _, want := range []string{"kelvo read failed:", "code=QUERY_FAILED", "class=execution", "stage=admission", "duration_ms=15"} {
		if !strings.Contains(line, want) {
			t.Fatalf("log %q lacks %q", line, want)
		}
	}
	if strings.Contains(line, "hunter2") || strings.Contains(line, "SELECT") {
		t.Fatalf("log leaked request or error text: %q", line)
	}
}

func TestFailedReadStageAndClass(t *testing.T) {
	phases := newExecutionPhases(time.Now(), false)
	phases.enter(telemetry.PhaseNodeAdmission)
	phases.resolving = true
	if got := failureStage(&phases); got != "resolve" {
		t.Fatalf("stage %q", got)
	}
	phases.resolving = false
	phases.enter(telemetry.PhaseExecutionDelivery)
	if got := failureStage(&phases); got != "execute" {
		t.Fatalf("stage %q", got)
	}
	phases.timings.HasFirstBatch = true
	if got := failureStage(&phases); got != "drain" {
		t.Fatalf("stage %q", got)
	}
	if got := failureClass(admission.ErrBusy, "QUERY_FAILED"); got != "capacity" {
		t.Fatalf("class %q", got)
	}
	if got := failureClass(context.DeadlineExceeded, "DEADLINE_EXCEEDED"); got != "deadline" {
		t.Fatalf("class %q", got)
	}
}

func TestSuccessfulOrRefreshReadLogsNothing(t *testing.T) {
	buf := captureLog(t)
	logFailedRead(context.Background(), time.Now(), nil, nil)
	logFailedRead(WithRefreshTelemetry(context.Background()), time.Now(), query.NewError("QUERY_FAILED", "x"), nil)
	if buf.Len() != 0 {
		t.Fatalf("unexpected log %q", buf.String())
	}
}

func TestResolveStageHistogramIsExported(t *testing.T) {
	var r telemetry.Registry
	r.ObserveResolve(telemetry.KindQuery, 2*time.Second)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	if !strings.Contains(rec.Body.String(), `kelvo_worker_resolve_seconds_sum{kind="query"} 2`) {
		t.Fatal("resolve histogram missing from metrics")
	}
}

// The CLI's stderr is parsed by scripts, so only a node logs failed reads.
func TestOnlyNodeExecutorsLogFailedReads(t *testing.T) {
	for _, node := range []bool{false, true} {
		buf := captureLog(t)
		e := &Executor{Limits: query.DefaultLimits(), LogFailedReads: node}
		_, _ = e.Execute(context.Background(), query.Request{SQL: "select 1", Mode: "native", ConnectionID: "x"}, nil)
		if logged := strings.Contains(buf.String(), "kelvo read failed:"); logged != node {
			t.Fatalf("node=%v logged=%v: %q", node, logged, buf.String())
		}
	}
}

func TestRefusedDelegationIsLogged(t *testing.T) {
	buf := captureLog(t)
	e := &Executor{Limits: query.DefaultLimits(), LogFailedReads: true}
	req := query.Request{SQL: "select 1", Mode: "native", ConnectionID: "x", Delegation: "not-a-grant"}
	_, err := e.Execute(context.Background(), req, nil)
	if err == nil || !strings.Contains(buf.String(), "kelvo read failed:") || !strings.Contains(buf.String(), "stage=validation") {
		t.Fatalf("err=%v log=%q", err, buf.String())
	}
}
