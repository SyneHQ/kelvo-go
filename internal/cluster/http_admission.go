// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

// A queued result cannot retain an active HTTP permit: its worker may need a
// different client's result request before the queued job can be assigned.
// This per-handler lease transfers between two fixed-size pools. Only its owning
// request goroutine mutates it. Channels safely account across request handlers.
type requestAdmission struct {
	active  chan struct{}
	waiting chan struct{}
	held    admissionPhase
}

type admissionPhase uint8

const (
	admissionReleased admissionPhase = iota
	admissionActive
	admissionWaiting
)

func (a *requestAdmission) park() bool {
	if a == nil || a.held == admissionWaiting {
		return true
	}
	if a.held != admissionActive {
		return false
	}
	select {
	case a.waiting <- struct{}{}:
		<-a.active
		a.held = admissionWaiting
		return true
	default:
		return false
	}
}

// activate never blocks while holding a waiter. Failure leaves the query
// unclaimed; the handler returns 429 and its deferred release drops the waiter.
func (a *requestAdmission) activate() bool {
	if a == nil || a.held == admissionActive {
		return true
	}
	if a.held != admissionWaiting {
		return false
	}
	select {
	case a.active <- struct{}{}:
		<-a.waiting
		a.held = admissionActive
		return true
	default:
		return false
	}
}

func (a *requestAdmission) release() {
	if a == nil {
		return
	}
	switch a.held {
	case admissionActive:
		<-a.active
	case admissionWaiting:
		<-a.waiting
	}
	a.held = admissionReleased
}
