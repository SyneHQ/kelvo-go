package diagnostic

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestNilRecorderAndContext(t *testing.T) {
	Record(nil, IPCRequest, Started, 0)
	Record(context.Background(), IPCRequest, Started, 0)
	if FromContext(nil) != nil || FromContext(context.Background()) != nil {
		t.Fatal("Missing context must not create a recorder")
	}
	var r *Recorder
	if snapshot := r.Snapshot(); len(snapshot.Events) != 0 || snapshot.Dropped != 0 {
		t.Fatal("Nil recorder returned events")
	}
	ctx := WithRecorder(nil, New())
	Record(ctx, IPCRequest, Started, 0)
	if len(FromContext(ctx).Snapshot().Events) != 1 {
		t.Fatal("Recorder did not accept a nil parent context")
	}
	Record(WithRecorder(ctx, nil), IPCRequest, Failed, 0)
	if len(FromContext(ctx).Snapshot().Events) != 1 {
		t.Fatal("Nil replacement recorder did not disable recording")
	}
}

func TestWhitelistAndHTTPStatusRedaction(t *testing.T) {
	r := New()
	ctx := WithRecorder(context.Background(), r)
	Record(ctx, Stage("secret-stage"), Started, 200)
	Record(ctx, IPCRequest, Result("secret-result"), 200)
	Record(ctx, "", Started, 200)
	Record(ctx, IPCRequest, "", 200)
	for _, status := range []int{-1, 0, 99, 100, 200, 599, 600, 123456789} {
		Record(ctx, ConnectResponse, Failed, status)
	}
	snapshot := r.Snapshot()
	if snapshot.Dropped != 4 || len(snapshot.Events) != 8 {
		t.Fatalf("Unexpected retained/dropped counts: %d/%d", len(snapshot.Events), snapshot.Dropped)
	}
	for i, want := range []int{0, 0, 0, 100, 200, 599, 0, 0} {
		if snapshot.Events[i].HTTPStatus != want {
			t.Fatalf("Event %d retained an invalid HTTP status", i)
		}
	}
	raw, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"secret-stage", "secret-result", "123456789", `"http_status":0`} {
		if strings.Contains(string(raw), forbidden) {
			t.Fatal("Snapshot included a rejected value")
		}
	}
}

func TestAllDeclaredStagesAndResultsAreAccepted(t *testing.T) {
	stages := []Stage{IPCRequest, IPCAuthorized, BrokerDial, DescriptorSent, ProofRefresh, ProofVerified, TicketIssue, TicketVerified, ProxyTCP, ProxyTLS, ConnectResponse, AcceptedReceipt, FreshResolver}
	results := []Result{Started, Succeeded, Failed, Timeout, Cancelled, ScopeDenied}
	for _, stage := range stages {
		r := New()
		ctx := WithRecorder(context.Background(), r)
		for _, result := range results {
			Record(ctx, stage, result, 0)
		}
		snapshot := r.Snapshot()
		if len(snapshot.Events) != len(results) || snapshot.Dropped != 0 {
			t.Fatal("Declared stage or result was rejected")
		}
		for i, event := range snapshot.Events {
			if event.Stage != stage || event.Result != results[i] || event.RemainingBudgetMS != -1 {
				t.Fatal("Recorded event differs from input")
			}
		}
	}
}

func TestFirstEventsAreRetainedWhenCapacityIsReached(t *testing.T) {
	r := New()
	ctx := WithRecorder(context.Background(), r)
	Record(ctx, IPCRequest, Started, 0)
	for i := 1; i < MaxEvents+17; i++ {
		Record(ctx, BrokerDial, Succeeded, 0)
	}
	snapshot := r.Snapshot()
	if len(snapshot.Events) != MaxEvents || snapshot.Dropped != 17 || snapshot.Events[0].Stage != IPCRequest {
		t.Fatal("Recorder did not retain the first bounded events")
	}
	r.dropped = ^uint64(0)
	Record(ctx, BrokerDial, Failed, 0)
	if r.Snapshot().Dropped != ^uint64(0) {
		t.Fatal("Dropped count overflowed")
	}
}

func TestConcurrentRecordingAndSnapshotOwnership(t *testing.T) {
	r := New()
	ctx := WithRecorder(context.Background(), r)
	const writers = 12
	const perWriter = 100
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perWriter; j++ {
				Record(ctx, BrokerDial, Started, 0)
				if j%10 == 0 {
					snapshot := r.Snapshot()
					if len(snapshot.Events) > 0 {
						snapshot.Events[0].Stage = Stage("caller-owned")
					}
				}
			}
		}()
	}
	wg.Wait()
	snapshot := r.Snapshot()
	if len(snapshot.Events) != MaxEvents || snapshot.Dropped != writers*perWriter-MaxEvents {
		t.Fatal("Concurrent recording lost counts")
	}
	for i, event := range snapshot.Events {
		if event.Stage != BrokerDial || (i > 0 && event.ElapsedMS < snapshot.Events[i-1].ElapsedMS) {
			t.Fatal("Snapshot mutation or event ordering changed the recorder")
		}
	}
}

func TestElapsedClockAndContextBudget(t *testing.T) {
	r := New()
	// Set a known start before attachment. Attachment must not restart the clock.
	r.started = time.Now().Add(-2 * time.Second)
	deadline := time.Now().Add(time.Minute)
	parent, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	ctx := WithRecorder(parent, r)
	if got, ok := ctx.Deadline(); !ok || !got.Equal(deadline) {
		t.Fatal("Recorder changed the parent deadline")
	}
	Record(ctx, IPCRequest, Started, 0)
	shorter, stop := context.WithDeadline(ctx, time.Now().Add(-time.Second))
	defer stop()
	Record(shorter, BrokerDial, Timeout, 0)
	snapshot := r.Snapshot()
	first, second := snapshot.Events[0], snapshot.Events[1]
	if first.ElapsedMS < 2000 || first.RemainingBudgetMS < 0 || first.RemainingBudgetMS > 60000 {
		t.Fatal("First event lost elapsed time or context budget")
	}
	if second.ElapsedMS < first.ElapsedMS || second.RemainingBudgetMS != 0 {
		t.Fatal("Expired child deadline was not recorded")
	}
	cancel()
	if !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatal("Recorder blocked context cancellation")
	}
}

func TestZeroValueRecorder(t *testing.T) {
	var r Recorder
	Record(WithRecorder(context.Background(), &r), IPCRequest, Started, 0)
	event := r.Snapshot().Events[0]
	if event.ElapsedMS != 0 || event.RemainingBudgetMS != -1 {
		t.Fatal("Zero-value recorder did not initialize its clock")
	}
}

type unreadableError struct{}

func (unreadableError) Error() string { panic("Diagnostic classification must not read error text") }

func TestResultForUsesErrorIdentity(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	expired, stop := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer stop()
	for _, tc := range []struct {
		name string
		ctx  context.Context
		err  error
		want Result
	}{
		{"success", nil, nil, Succeeded},
		{"success-after-cancel", cancelled, nil, Succeeded},
		{"generic", nil, unreadableError{}, Failed},
		{"deadline", nil, fmt.Errorf("wrapped: %w", context.DeadlineExceeded), Timeout},
		{"cancel", nil, fmt.Errorf("wrapped: %w", context.Canceled), Cancelled},
		{"context-deadline", expired, unreadableError{}, Timeout},
		{"context-cancel", cancelled, unreadableError{}, Cancelled},
		{"explicit-error-first", cancelled, context.DeadlineExceeded, Timeout},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := ResultFor(tc.ctx, tc.err); got != tc.want {
				t.Fatalf("Result was %s, want %s", got, tc.want)
			}
		})
	}
}
