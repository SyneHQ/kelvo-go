// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package transportbroker

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"
)

func TestBindingRejectsMissingOrChangedAuthority(t *testing.T) {
	opener := openFunc(func(context.Context, OpenRequest) (net.Conn, error) {
		t.Fatal("invalid admission reached the provider")
		return nil, nil
	})
	b := fixture(t, Limits{MaxSessions: 1, MaxDataConnections: 1, MaxDataPerSession: 1}, opener)
	for name, change := range map[string]func(*Binding){
		"issuer":            func(b *Binding) { b.Issuer = "" },
		"audience":          func(b *Binding) { b.Audience = "" },
		"cluster_tenant":    func(b *Binding) { b.ClusterTenant = "" },
		"service_principal": func(b *Binding) { b.ServicePrincipal = "with space" },
		"tenant":            func(b *Binding) { b.Tenant = "" },
		"source":            func(b *Binding) { b.Source = "\nsource" },
		"revision":          func(b *Binding) { b.SourceRevision = strings.Repeat("A", 64) },
		"authority":         func(b *Binding) { b.Authority = "https://database.internal:5432" },
		"kind":              func(b *Binding) { b.Execution.Kind = "unspecified" },
		"id":                func(b *Binding) { b.Execution.ID = "" },
		"grant":             func(b *Binding) { b.Execution.GrantSHA256 = "unsigned" },
		"worker":            func(b *Binding) { b.Execution.Worker = strings.Repeat("w", 33) },
		"owner":             func(b *Binding) { b.Execution.Owner = strings.Repeat("a", 31) },
		"claim":             func(b *Binding) { b.Execution.Claim = strings.Repeat("G", 32) },
		"expired":           func(b *Binding) { b.ExpiresAt = time.Now().Add(-time.Second) },
		"too_long":          func(b *Binding) { b.ExpiresAt = time.Now().Add(25 * time.Hour) },
	} {
		t.Run(name, func(t *testing.T) {
			value := binding()
			change(&value)
			if _, err := b.Admit(context.Background(), value); !errors.Is(err, ErrInvalid) {
				t.Fatal("invalid scope admitted", err)
			}
		})
	}
	if b.Snapshot().Sessions != 0 {
		t.Fatal("invalid admission reserved cancellation capacity")
	}
}

func TestCanonicalAuthorityNeverAcceptsProxyInstructions(t *testing.T) {
	for _, authority := range []string{"database.internal:5432", "127.0.0.1:5432", "[2001:db8::1]:5432"} {
		if !validAuthority(authority) {
			t.Error("canonical authority rejected", authority)
		}
	}
	for _, authority := range []string{
		"", "database.internal", "user@database.internal:5432", "https://database.internal:5432", "database.internal:05432",
		"database.internal:0", "database.internal:65536", "database.internal:5432/path", "database.internal:5432\r\nHost: attacker",
		"Database.internal:5432", "database.internal.:5432", "-database.internal:5432", "db..internal:5432", "[fe80::1%eth0]:5432",
		"0.0.0.0:5432", "[::]:5432", "224.0.0.1:5432", "[ff02::1]:5432", "[2001:0db8::1]:5432",
	} {
		if validAuthority(authority) {
			t.Error("noncanonical authority accepted", authority)
		}
	}
}

func TestInvalidLimitsAndCancelledAdmission(t *testing.T) {
	opener := openFunc(func(context.Context, OpenRequest) (net.Conn, error) { return nil, nil })
	for _, limits := range []Limits{{}, {1, 1, 2}, {1 << 21, 1, 1}, {1, -1, 1}, {257, 256, 256}, {int(^uint(0) >> 1), 2, 2}} {
		if _, err := New(limits, opener); !errors.Is(err, ErrInvalid) {
			t.Fatal("unbounded broker limits accepted", limits, err)
		}
	}
	b := fixture(t, Limits{MaxSessions: 1, MaxDataConnections: 1, MaxDataPerSession: 1}, opener)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := b.Admit(ctx, binding()); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled execution admitted", err)
	}
	if _, err := b.Admit(nil, binding()); !errors.Is(err, ErrInvalid) {
		t.Fatal("missing custody context accepted", err)
	}
	if b.Snapshot().Sessions != 0 {
		t.Fatal("cancelled execution reserved capacity")
	}
}
