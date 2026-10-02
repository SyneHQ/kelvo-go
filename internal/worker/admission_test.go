// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/admission"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
)

func executorPool(t *testing.T, overhead int64) (*Executor, *admission.Pool) {
	t.Helper()
	e, err := New(catalog.Config{}, query.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	p, err := admission.New(admission.Limits{MaxConcurrent: 2, MemoryBytes: int64(e.Limits.MemoryMB)<<20 + overhead, ScratchBytes: int64(e.Limits.MaxTempMB) << 20})
	if err != nil {
		t.Fatal(err)
	}
	e.ResourcePool = p
	e.ResourceOverheadBytes = overhead
	return e, p
}

func assertPoolEmpty(t *testing.T, p *admission.Pool) {
	t.Helper()
	if s := p.Snapshot(); s.Active != 0 || s.Waiting != 0 || s.Used != (admission.Request{}) {
		t.Fatalf("leaked reservation: %+v", s)
	}
}

func TestExecutorAdmissionPrecedesSourceResolution(t *testing.T) {
	e, p := executorPool(t, 0)
	e.ResourceOverheadBytes = 1 // One byte over budget must fail before bad source lookup.
	e.Binary = "/does/not/exist"
	_, err := e.Execute(context.Background(), query.Request{SQL: "SELECT 1", Sources: []string{"missing"}}, &workerTestSink{})
	if !errors.Is(err, admission.ErrOversize) {
		t.Fatalf("wanted admission rejection, got %v", err)
	}
	assertPoolEmpty(t, p)
}

func TestExecutorAdmissionWaitCountsAgainstTimeout(t *testing.T) {
	e, p := executorPool(t, 0)
	e.Binary = "/does/not/exist"
	e.Limits.Timeout = 20 * time.Millisecond
	held, err := p.TryAcquire(admission.Request{MemoryBytes: int64(e.Limits.MemoryMB) << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer held.Release()
	_, err = e.Execute(context.Background(), query.Request{SQL: "SELECT 1"}, &workerTestSink{})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("queue timeout: %v", err)
	}
	if s := p.Snapshot(); s.Active != 1 || s.Waiting != 0 {
		t.Fatalf("queue leaked: %+v", s)
	}
}

func TestExecutorAdmissionRejectsInvalidOverhead(t *testing.T) {
	for _, overhead := range []int64{-1, math.MaxInt64} {
		e, p := executorPool(t, 0)
		e.ResourceOverheadBytes = overhead
		_, err := e.Execute(context.Background(), query.Request{SQL: "SELECT 1"}, &workerTestSink{})
		if !errors.Is(err, admission.ErrInvalid) {
			t.Fatalf("invalid overhead %d: %v", overhead, err)
		}
		assertPoolEmpty(t, p)
	}
}

func TestExecutorAdmissionReleasedAfterStartFailure(t *testing.T) {
	e, p := executorPool(t, 32<<20)
	e.Binary = "/does/not/exist"
	_, err := e.Execute(context.Background(), query.Request{SQL: "SELECT 1"}, &workerTestSink{})
	if err == nil {
		t.Fatal("missing executable succeeded")
	}
	assertPoolEmpty(t, p)
}

type admissionCheckingSink struct {
	t        *testing.T
	pool     *admission.Pool
	expected admission.Request
}

func (s *admissionCheckingSink) check() {
	s.t.Helper()
	if state := s.pool.Snapshot(); state.Active != 1 || state.Used != s.expected {
		s.t.Errorf("output not covered by reservation: %+v", state)
	}
	if r, err := s.pool.TryAcquire(admission.Request{MemoryBytes: 1}); !errors.Is(err, admission.ErrBusy) {
		if r != nil {
			r.Release()
		}
		s.t.Errorf("competing output work admitted: %v", err)
	}
}
func (s *admissionCheckingSink) Schema(*arrow.Schema) error    { s.check(); return nil }
func (s *admissionCheckingSink) Write(arrow.RecordBatch) error { s.check(); return nil }

func TestExecutorAdmissionCoversEntireResultTransfer(t *testing.T) {
	e, p := executorPool(t, 32<<20)
	sink := &admissionCheckingSink{t: t, pool: p, expected: admission.Request{MemoryBytes: int64(e.Limits.MemoryMB)<<20 + e.ResourceOverheadBytes, ScratchBytes: int64(e.Limits.MaxTempMB) << 20}}
	_, err := e.Execute(context.Background(), query.Request{SQL: "SELECT 1"}, sink)
	if err != nil {
		t.Fatal(err)
	}
	assertPoolEmpty(t, p)
}

func TestExecutorAdmissionReleasedAfterCanceledChild(t *testing.T) {
	e, p := executorPool(t, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := e.Execute(ctx, query.Request{SQL: "SELECT wait"}, &workerTestSink{})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("canceled child: %v", err)
	}
	assertPoolEmpty(t, p)
}
