// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
)

func TestWorkerResultRejectsQuotaLossAfterCompleteIPC(t *testing.T) {
	parent := context.Background()
	owned, lose := context.WithCancelCause(parent)
	defer lose(context.Canceled)
	sink := &workerTestSink{}
	observed, readErr := readWorkerIPC(owned, bytes.NewReader(ipcFixture(t)), query.DefaultLimits(), sink)
	if readErr != nil || observed.Rows != 768 || owned.Err() != nil {
		t.Fatalf("IPC did not complete before ownership loss: rows=%d err=%v", observed.Rows, readErr)
	}
	// Deterministically enter the interval after the IPC reader's final context
	// check and before the post-cleanup decision used by Executor.Execute.
	// Even a clean exit, valid outcome and complete result must not win here.
	const privateCause = "private-coordination-token-and-source-name"
	lose(errors.New(privateCause))
	err := workerResultError(parent, parent, owned, readErr, nil, nil, nil, nil)
	if err == nil || query.PublicError(err).Code != "UNAVAILABLE" {
		t.Fatalf("lost source ownership accepted completed result: %v", err)
	}
	if strings.Contains(err.Error(), privateCause) || strings.Contains(query.PublicError(err).Message, privateCause) {
		t.Fatal("private coordination cause escaped completion boundary")
	}
	if parent.Err() != nil {
		t.Fatal("fixture canceled the enclosing refresh context")
	}
}

func TestWorkerResultCancellationPreservesExistingErrorPrecedence(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	deadline, stop := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer stop()
	sinkError := query.NewError("RESOURCE_EXHAUSTED", "Result byte limit exceeded")
	sourceError := query.PublicError(query.NewError("PERMISSION_DENIED", "Source query unavailable"))
	privateError := errors.New("private worker error")
	cases := []struct {
		name               string
		parent, execution  context.Context
		readErr, decodeErr error
		outcome            *query.Error
		want               error
	}{
		{"caller cancellation", canceled, canceled, sinkError, nil, sourceError, context.Canceled},
		{"execution deadline", context.Background(), deadline, sinkError, nil, sourceError, context.DeadlineExceeded},
		{"typed sink after local cancel", context.Background(), canceled, sinkError, privateError, nil, sinkError},
		{"typed source with failed read", context.Background(), canceled, privateError, nil, sourceError, sourceError},
		{"read cancellation", context.Background(), canceled, context.Canceled, privateError, nil, context.Canceled},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := workerResultError(tc.parent, tc.execution, tc.execution, tc.readErr, privateError, tc.decodeErr, privateError, tc.outcome)
			if !errors.Is(got, tc.want) {
				t.Fatalf("completion precedence changed: got %v want %v", got, tc.want)
			}
		})
	}
	for _, cleanup := range []error{nil, os.ErrProcessDone} {
		if got := workerResultError(context.Background(), context.Background(), context.Background(), nil, nil, nil, cleanup, nil); got != nil {
			t.Fatalf("healthy completion rejected: %v", got)
		}
	}
	for _, failures := range [][3]error{{privateError, nil, nil}, {nil, privateError, nil}, {nil, nil, privateError}} {
		got := workerResultError(context.Background(), context.Background(), context.Background(), nil, failures[0], failures[1], failures[2], nil)
		if got == nil || query.PublicError(got).Code != "QUERY_FAILED" || strings.Contains(got.Error(), privateError.Error()) {
			t.Fatalf("unsafe failed cleanup outcome: %v", got)
		}
	}
}

func TestExecutorTypedSinkErrorSurvivesLocalCancellation(t *testing.T) {
	executor, err := New(catalog.Config{}, query.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	want := query.NewError("RESOURCE_EXHAUSTED", "Result byte limit exceeded")
	sink := &workerTestSink{write: func(arrow.RecordBatch) error { return want }}
	_, err = executor.Execute(context.Background(), query.Request{SQL: "SELECT 1"}, sink)
	if !errors.Is(err, want) {
		t.Fatalf("read-triggered local cancellation hid typed sink failure: %v", err)
	}
}

// This fixture holds quota ownership until Execute's deferred cleanup runs.
type cancelingSourceAdmission struct {
	cancel   context.CancelCauseFunc
	active   bool
	released int
}

func (f *cancelingSourceAdmission) Acquire(parent context.Context, _ []string) (context.Context, func(), error) {
	ctx, cancel := context.WithCancelCause(parent)
	f.cancel, f.active = cancel, true
	return ctx, func() { f.active = false; f.released++; cancel(context.Canceled) }, nil
}
func TestExecutorQuotaCancellationDuringSinkReleasesReservations(t *testing.T) {
	for _, typed := range []bool{false, true} {
		name := "canceled successful sink"
		if typed {
			name = "typed sink failure wins"
		}
		t.Run(name, func(t *testing.T) {
			executor, pool := executorPool(t, 0)
			fixture := &cancelingSourceAdmission{}
			executor.SourceAdmission = fixture
			want := query.NewError("RESOURCE_EXHAUSTED", "Result byte limit exceeded")
			called := false
			sink := &workerTestSink{write: func(arrow.RecordBatch) error {
				called = true
				if !fixture.active || pool.Snapshot().Active != 1 {
					t.Fatal("reservations released before sink cleanup")
				}
				fixture.cancel(errors.New("private source coordination cause"))
				if typed {
					return want
				}
				return nil
			}}
			_, err := executor.Execute(context.Background(), query.Request{SQL: "SELECT 1"}, sink)
			if !called || err == nil {
				t.Fatalf("quota cancellation accepted result: called=%v err=%v", called, err)
			}
			if typed && !errors.Is(err, want) {
				t.Fatalf("quota cancellation masked typed sink failure: %v", err)
			}
			if strings.Contains(err.Error(), "private source") {
				t.Fatal("private coordination cause escaped worker")
			}
			if fixture.active || fixture.released != 1 {
				t.Fatal("source quota was not released exactly once")
			}
			assertPoolEmpty(t, pool)
		})
	}
}
func TestWorkerQuotaLossDoesNotMaskLaterExecutionDeadline(t *testing.T) {
	parent := context.Background()
	execution, stop := context.WithTimeout(parent, 10*time.Millisecond)
	defer stop()
	owned, lose := context.WithCancelCause(execution)
	lose(errors.New("private source lease loss"))
	<-execution.Done()
	if owned.Err() != context.Canceled {
		t.Fatal("quota cancellation must precede deadline")
	}
	if got := workerResultError(parent, execution, owned, nil, nil, nil, nil, nil); !errors.Is(got, context.DeadlineExceeded) {
		t.Fatalf("quota child masked original execution deadline: %v", got)
	}
}
