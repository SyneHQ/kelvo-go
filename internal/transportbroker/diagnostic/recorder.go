// Package diagnostic records bounded, credential-free connection stages.
package diagnostic

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Stage identifies a fixed connection step. Record rejects other values.
type Stage string

const (
	IPCRequest      Stage = "ipc_request"
	IPCAuthorized   Stage = "ipc_authorized"
	BrokerDial      Stage = "broker_dial"
	DescriptorSent  Stage = "descriptor_sent"
	ProofRefresh    Stage = "proof_refresh"
	ProofVerified   Stage = "proof_verified"
	TicketIssue     Stage = "ticket_issue"
	TicketVerified  Stage = "ticket_verified"
	ProxyTCP        Stage = "proxy_tcp"
	ProxyTLS        Stage = "proxy_tls"
	ConnectResponse Stage = "connect_response"
	AcceptedReceipt Stage = "accepted_receipt"
	FreshResolver   Stage = "fresh_resolver"
)

// Result identifies a fixed outcome. Record rejects other values.
type Result string

const (
	Started     Result = "started"
	Succeeded   Result = "succeeded"
	Failed      Result = "failed"
	Timeout     Result = "timeout"
	Cancelled   Result = "cancelled"
	ScopeDenied Result = "scope_denied"
)

const MaxEvents = 64

// Event contains no source values, addresses, queries or error messages.
type Event struct {
	Stage             Stage  `json:"stage"`
	Result            Result `json:"result"`
	ElapsedMS         int64  `json:"elapsed_ms"`
	RemainingBudgetMS int64  `json:"remaining_budget_ms"`
	HTTPStatus        int    `json:"http_status,omitempty"`
}

// Snapshot owns its event slice. Changes to it cannot change the recorder.
type Snapshot struct {
	Events  []Event `json:"events"`
	Dropped uint64  `json:"dropped"`
}

// Recorder retains the first MaxEvents events. Its zero value is usable.
// Use New to include time elapsed before the first event.
type Recorder struct {
	mu      sync.Mutex
	started time.Time
	events  [MaxEvents]Event
	count   int
	dropped uint64
}

// New starts the elapsed clock immediately. It starts no background work.
func New() *Recorder {
	return &Recorder{started: time.Now()}
}

type recorderKey struct{}

// WithRecorder attaches recorder without changing the context deadline.
// A nil context uses context.Background. A nil recorder disables recording.
func WithRecorder(ctx context.Context, recorder *Recorder) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, recorderKey{}, recorder)
}

// FromContext returns nil when no recorder is attached.
func FromContext(ctx context.Context) *Recorder {
	if ctx == nil {
		return nil
	}
	recorder, _ := ctx.Value(recorderKey{}).(*Recorder)
	return recorder
}

// Record retains an event only when its stage and result are allowed.
// Invalid HTTP status codes are omitted. Invalid events increment Dropped.
// RemainingBudgetMS is -1 without a deadline and 0 after deadline expiry.
func Record(ctx context.Context, stage Stage, result Result, httpStatus int) {
	r := FromContext(ctx)
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !validStage(stage) || !validResult(result) || r.count == MaxEvents {
		if r.dropped != ^uint64(0) {
			r.dropped++
		}
		return
	}
	now := time.Now()
	if r.started.IsZero() {
		r.started = now
	}
	elapsed := now.Sub(r.started).Milliseconds()
	if elapsed < 0 {
		elapsed = 0
	}
	remaining := int64(-1)
	if deadline, ok := ctx.Deadline(); ok {
		remaining = deadline.Sub(now).Milliseconds()
		if remaining < 0 {
			remaining = 0
		}
	}
	if httpStatus < 100 || httpStatus > 599 {
		httpStatus = 0
	}
	r.events[r.count] = Event{Stage: stage, Result: result, ElapsedMS: elapsed, RemainingBudgetMS: remaining, HTTPStatus: httpStatus}
	r.count++
}

// Snapshot returns an independent copy. A nil recorder returns no events.
func (r *Recorder) Snapshot() Snapshot {
	if r == nil {
		return Snapshot{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	events := make([]Event, r.count)
	copy(events, r.events[:r.count])
	return Snapshot{Events: events, Dropped: r.dropped}
}

// ResultFor classifies failures without reading an error message.
// Callers must classify scope denials at the check that rejects the scope.
func ResultFor(ctx context.Context, err error) Result {
	if err == nil {
		return Succeeded
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return Timeout
	}
	if errors.Is(err, context.Canceled) {
		return Cancelled
	}
	if ctx != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return Timeout
		}
		if errors.Is(ctx.Err(), context.Canceled) {
			return Cancelled
		}
	}
	return Failed
}

func validStage(stage Stage) bool {
	switch stage {
	case IPCRequest, IPCAuthorized, BrokerDial, DescriptorSent, ProofRefresh,
		ProofVerified, TicketIssue, TicketVerified, ProxyTCP, ProxyTLS,
		ConnectResponse, AcceptedReceipt, FreshResolver:
		return true
	default:
		return false
	}
}

func validResult(result Result) bool {
	switch result {
	case Started, Succeeded, Failed, Timeout, Cancelled, ScopeDenied:
		return true
	default:
		return false
	}
}
