// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestCoordinationClassifiesWithoutRetainingPrivateCause(t *testing.T) {
	for _, tc := range []struct {
		name, reason string
		cause        error
	}{
		{"deadline", "deadline_exceeded", fmt.Errorf("private token: %w", context.DeadlineExceeded)},
		{"cancel", "context_canceled", context.Canceled},
		{"timeout", "timeout", nats.ErrTimeout},
		{"no-responders", "no_responders", nats.ErrNoResponders},
		{"permission", "permission_denied", nats.ErrPermissionViolation},
		{"authorization", "authorization", nats.ErrAuthorization},
		{"closed", "connection_closed", nats.ErrConnectionClosed},
		{"no-servers", "no_servers", nats.ErrNoServers},
		{"refused", "connection_refused", &net.OpError{Op: "dial", Net: "tcp", Addr: &net.TCPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 4222}, Err: syscall.ECONNREFUSED}},
		{"reset", "connection_reset", syscall.ECONNRESET},
		{"tls-ca", "tls_verification", x509.UnknownAuthorityError{Cert: &x509.Certificate{DNSNames: []string{"private.example"}}}},
		{"tls-host", "tls_verification", x509.HostnameError{Host: "private.example"}},
		{"tls-protocol", "tls_protocol", tls.RecordHeaderError{Msg: "private TLS detail"}},
		{"api", "api_error", &jetstream.APIError{Code: 503, ErrorCode: 10000, Description: "private broker subject"}},
		{"missing", "missing_lease", jetstream.ErrKeyNotFound},
		{"spoofed-text", "other", errors.New("nats: timeout private-token")},
		{"unreadable", "other", metadataUnreadableError{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := coordinationUnavailable("nats_connect", tc.cause)
			failure, ok := CoordinationDiagnostic(err)
			if !ok || failure.Stage != "nats_connect" || failure.Reason != tc.reason || err.Error() != "NATS connection failed" || errors.Unwrap(err) != nil {
				t.Fatalf("wrong safe classification: %+v", failure)
			}
			text := failure.Diagnostic() + fmt.Sprintf("%+v", err)
			if len(text) > 256 || strings.Contains(text, "private") || strings.Contains(text, "192.0.2") {
				t.Fatal("coordination diagnostic exposed provider information")
			}
		})
	}
	for _, input := range []error{nil, errors.New("NATS connection failed"), metadataUnreadableError{}} {
		if _, ok := CoordinationDiagnostic(input); ok {
			t.Fatal("unclassified error produced trusted diagnostics")
		}
	}
	if got := (CoordinationFailure{Stage: "private", Reason: "private"}).Diagnostic(); got != `{"stage":"unknown","reason":"other"}` {
		t.Fatal("arbitrary labels escaped their allowlist")
	}
}

type coordinationKV struct {
	jetstream.KeyValue
	entry                   jetstream.KeyValueEntry
	readErr, writeErr       error
	commitBeforeError       bool
	reads, creates, updates int
}

func (k *coordinationKV) Get(context.Context, string) (jetstream.KeyValueEntry, error) {
	k.reads++
	return k.entry, k.readErr
}
func (k *coordinationKV) Create(_ context.Context, _ string, raw []byte, _ ...jetstream.KVCreateOpt) (uint64, error) {
	k.creates++
	if k.writeErr == nil || k.commitBeforeError {
		k.entry = statusEntry{raw: append([]byte(nil), raw...), revision: 1}
	}
	return 1, k.writeErr
}
func (k *coordinationKV) Update(_ context.Context, _ string, raw []byte, revision uint64) (uint64, error) {
	k.updates++
	if k.writeErr == nil || k.commitBeforeError {
		k.entry = statusEntry{raw: append([]byte(nil), raw...), revision: revision + 1}
	}
	return revision + 1, k.writeErr
}

func TestWorkerCoordinationClassifiesExactFailingStageWithoutRetry(t *testing.T) {
	for _, mode := range []string{"read-timeout", "read-missing", "corrupt", "owner", "expired", "write-conflict", "write-lost-ack", "claim-conflict", "claim-authorization", "success"} {
		t.Run(mode, func(t *testing.T) {
			policy := testPolicy()
			owner := strings.Repeat("a", 32)
			prior := workerLease{Owner: owner, HeartbeatAt: time.Now().UTC()}
			kv := &coordinationKV{}
			claim := false
			stage, reason, conflict := "worker_renew_read", "other", false
			switch mode {
			case "read-timeout":
				kv.readErr, reason = context.DeadlineExceeded, "deadline_exceeded"
			case "read-missing":
				kv.readErr, reason = jetstream.ErrKeyNotFound, "missing_lease"
			case "corrupt":
				reason = "invalid_lease"
			case "owner":
				prior.Owner = strings.Repeat("b", 32)
				stage, reason, conflict = "worker_renew", "owner_conflict", true
			case "expired":
				prior.HeartbeatAt = prior.HeartbeatAt.Add(-2 * policy.LeaseDuration)
				stage, reason, conflict = "worker_renew", "lease_expired", true
			case "write-conflict":
				kv.writeErr = jetstream.ErrKeyRevisionMismatch
				stage, reason, conflict = "worker_renew_write", "revision_conflict", true
			case "write-lost-ack":
				kv.writeErr, kv.commitBeforeError = context.DeadlineExceeded, true
				stage, reason = "worker_renew_write", "deadline_exceeded"
			case "claim-conflict", "claim-authorization":
				claim, kv.readErr = true, jetstream.ErrKeyNotFound
				stage = "worker_claim_write"
				if mode == "claim-conflict" {
					kv.writeErr, reason, conflict = jetstream.ErrKeyExists, "revision_conflict", true
				} else {
					kv.writeErr, reason = nats.ErrAuthorization, "authorization"
				}
			}
			raw, _ := json.Marshal(prior)
			if mode == "corrupt" {
				raw = []byte("private malformed lease")
			}
			kv.entry = statusEntry{raw: raw, revision: 1}
			store := &NATSStore{policy: policy, kv: kv}
			err := store.worker(context.Background(), "a1", owner, claim)
			if mode == "success" {
				if err != nil || kv.reads != 1 || kv.updates != 1 {
					t.Fatal("healthy worker renewal changed")
				}
				return
			}
			failure, ok := CoordinationDiagnostic(err)
			if !ok || failure.Stage != stage || failure.Reason != reason || errors.Is(err, ErrConflict) != conflict || kv.reads != 1 || kv.creates+kv.updates > 1 {
				t.Fatalf("wrong stage or unsafe retry: %+v", failure)
			}
			if mode == "write-lost-ack" && (kv.entry.Revision() != 2 || !errors.Is(err, context.DeadlineExceeded)) {
				t.Fatal("lost acknowledgement was treated as success or replayed")
			}
		})
	}
}
