// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"net"
	"syscall"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// CoordinationFailure contains only fixed diagnostic labels. It never retains
// a broker error, address, subject, worker identity or credential.
type CoordinationFailure struct {
	Stage  string `json:"stage"`
	Reason string `json:"reason"`
}

func (f CoordinationFailure) normalized() CoordinationFailure {
	switch f.Stage {
	case "nats_connect", "worker_claim", "worker_claim_read", "worker_claim_write", "worker_renew", "worker_renew_read", "worker_renew_write":
	default:
		f.Stage = "unknown"
	}
	switch f.Reason {
	case "context_canceled", "deadline_exceeded", "timeout", "no_responders", "permission_denied", "authorization", "connection_closed", "no_servers", "connection_refused", "connection_reset", "tls_verification", "tls_protocol", "api_error", "missing_lease", "invalid_lease", "invalid_identity", "owner_conflict", "lease_expired", "revision_conflict":
	default:
		f.Reason = "other"
	}
	return f
}

// Diagnostic formats a bounded, safe record even if given unknown labels.
func (f CoordinationFailure) Diagnostic() string {
	raw, _ := json.Marshal(f.normalized())
	return string(raw)
}

type coordinationError struct {
	message string
	failure CoordinationFailure
	match   error // Only a local sentinel, never the provider error.
}

func (e *coordinationError) Error() string { return e.message }
func (e *coordinationError) Is(target error) bool {
	return e.match != nil && target == e.match
}

// CoordinationDiagnostic extracts only a classification attached by the
// coordination store. Text that resembles a diagnostic is never trusted.
func CoordinationDiagnostic(err error) (CoordinationFailure, bool) {
	var classified *coordinationError
	if !errors.As(err, &classified) || classified == nil {
		return CoordinationFailure{}, false
	}
	return classified.failure.normalized(), true
}

func coordinationFailure(stage, reason string) *coordinationError {
	failure := (CoordinationFailure{Stage: stage, Reason: reason}).normalized()
	err := &coordinationError{message: "worker store unavailable", failure: failure}
	if failure.Stage == "nats_connect" {
		err.message = "NATS connection failed"
	}
	switch failure.Reason {
	case "invalid_identity", "owner_conflict", "lease_expired", "revision_conflict":
		err.message, err.match = ErrConflict.Error(), ErrConflict
	case "context_canceled":
		err.match = context.Canceled
	case "deadline_exceeded":
		err.match = context.DeadlineExceeded
	}
	return err
}

func coordinationUnavailable(stage string, cause error) *coordinationError {
	reason := "other"
	var unknownCA x509.UnknownAuthorityError
	var hostname x509.HostnameError
	var certificate x509.CertificateInvalidError
	var verification *tls.CertificateVerificationError
	var record tls.RecordHeaderError
	var network net.Error
	var api *jetstream.APIError
	switch {
	case errors.Is(cause, context.Canceled):
		reason = "context_canceled"
	case errors.Is(cause, context.DeadlineExceeded):
		reason = "deadline_exceeded"
	case errors.Is(cause, nats.ErrTimeout):
		reason = "timeout"
	case errors.Is(cause, nats.ErrNoResponders):
		reason = "no_responders"
	case errors.Is(cause, nats.ErrPermissionViolation):
		reason = "permission_denied"
	case errors.Is(cause, nats.ErrAuthorization):
		reason = "authorization"
	case errors.Is(cause, nats.ErrConnectionClosed), errors.Is(cause, net.ErrClosed):
		reason = "connection_closed"
	case errors.Is(cause, nats.ErrNoServers):
		reason = "no_servers"
	case errors.Is(cause, syscall.ECONNREFUSED):
		reason = "connection_refused"
	case errors.Is(cause, syscall.ECONNRESET):
		reason = "connection_reset"
	case errors.As(cause, &unknownCA), errors.As(cause, &hostname), errors.As(cause, &certificate), errors.As(cause, &verification):
		reason = "tls_verification"
	case errors.As(cause, &record):
		reason = "tls_protocol"
	case errors.As(cause, &network) && network != nil && network.Timeout():
		reason = "timeout"
	case errors.Is(cause, jetstream.ErrKeyNotFound):
		reason = "missing_lease"
	case errors.As(cause, &api) && api != nil:
		reason = "api_error"
	}
	return coordinationFailure(stage, reason)
}
