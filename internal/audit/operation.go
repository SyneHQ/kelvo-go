// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package audit

import (
	"context"
	"sync"
	"time"
)

type operationKind uint8

const (
	beginOperation operationKind = iota
	finishOperation
	recordOperation
)

type operationResult struct {
	receipt *Receipt
	err     error
}
type operation struct {
	kind      operationKind
	binding   Binding
	eventKind Kind
	receipt   *Receipt
	outcome   Outcome
	category  Category
	cancelled <-chan struct{}
	deadline  time.Time
	mu        sync.Mutex
	fenced    bool
	completed bool
	result    operationResult
	done      chan struct{}
}

func newOperation(ctx context.Context, timeout time.Duration) *operation {
	deadline := time.Now().Add(timeout)
	if inherited, ok := ctx.Deadline(); ok && inherited.Before(deadline) {
		deadline = inherited
	}
	return &operation{cancelled: ctx.Done(), deadline: deadline, done: make(chan struct{})}
}

func (o *operation) cancellationObserved() bool {
	select {
	case <-o.cancelled:
		return true
	default:
		return false
	}
}

// State is aggregate operational health only; it contains no recorded identity.
type State struct {
	MaxEntries int   `json:"max_entries"`
	MaxPending int   `json:"max_pending"`
	Active     int64 `json:"active"`
	Retained   int64 `json:"retained"`
	Queued     int   `json:"queued"`
	Healthy    bool  `json:"healthy"`
	Closing    bool  `json:"closing"`
}
