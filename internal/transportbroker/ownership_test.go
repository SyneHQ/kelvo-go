// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package transportbroker

import (
	"context"
	"errors"
	"io"
	"net"
	"reflect"
	"testing"
	"time"
)

func cloneHandle[T any](original *T) *T {
	copied := new(T)
	reflect.ValueOf(copied).Elem().Set(reflect.ValueOf(original).Elem())
	return copied
}

func TestCopiedSessionCannotAcquireCapacityOrCloseOriginal(t *testing.T) {
	opens := 0
	b := fixture(t, Limits{MaxSessions: 1, MaxDataConnections: 2, MaxDataPerSession: 1}, openFunc(func(context.Context, OpenRequest) (net.Conn, error) {
		opens++
		return pipe(t), nil
	}))
	s := admit(t, b, binding())
	copied := cloneHandle(s)
	_ = dial(t, s.DataDialer(), s.binding.Authority)
	_ = dial(t, s.CancellationDialer(), s.binding.Authority)
	before := b.Snapshot()
	for _, d := range []Dialer{copied.DataDialer(), copied.CancellationDialer()} {
		if _, err := d.DialContext(context.Background(), "tcp", s.binding.Authority); !errors.Is(err, ErrInvalid) {
			t.Fatal("a copied session acquired another reservation", err)
		}
	}
	if err := copied.Close(context.Background()); !errors.Is(err, ErrInvalid) {
		t.Fatal("a copied session closed the original execution", err)
	}
	if s.ctx.Err() != nil || b.Snapshot() != before || opens != 2 {
		t.Fatal("a copied session changed admitted custody")
	}
}

func TestCopiedBrokerCannotAdmitOrCloseOriginal(t *testing.T) {
	b := fixture(t, Limits{MaxSessions: 2, MaxDataConnections: 2, MaxDataPerSession: 1}, openFunc(func(context.Context, OpenRequest) (net.Conn, error) { return pipe(t), nil }))
	s := admit(t, b, binding())
	copied := cloneHandle(b)
	other := binding()
	other.Execution.ID = "other-execution"
	if _, err := copied.Admit(context.Background(), other); !errors.Is(err, ErrInvalid) {
		t.Fatal("a copied broker admitted unowned capacity", err)
	}
	if err := copied.Close(context.Background()); !errors.Is(err, ErrInvalid) {
		t.Fatal("a copied broker closed original execution custody", err)
	}
	if s.ctx.Err() != nil || b.Snapshot().Sessions != 1 {
		t.Fatal("a copied broker changed original session custody")
	}
}

func TestDriverCannotReplaceConfirmedPhysicalSocket(t *testing.T) {
	raw, peer := net.Pipe()
	defer raw.Close()
	defer peer.Close()
	b := fixture(t, Limits{MaxSessions: 1, MaxDataConnections: 1, MaxDataPerSession: 1}, openFunc(func(context.Context, OpenRequest) (net.Conn, error) { return raw, nil }))
	s := admit(t, b, binding())
	conn := dial(t, s.DataDialer(), s.binding.Authority)
	// Reflection reproduces what an external driver can do to an exported
	// embedded interface even when the wrapper type itself is unexported.
	field := reflect.ValueOf(conn).Elem().FieldByName("Conn")
	if field.IsValid() && field.CanSet() {
		field.Set(reflect.ValueOf(pipe(t)))
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	peer.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := peer.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatal("capacity released while the accepted physical socket remained open", err)
	}
	if b.Snapshot().DataConnections != 0 {
		t.Fatal("confirmed close retained capacity")
	}
}

func TestCopiedConnectionCannotCloseOrReleaseOriginal(t *testing.T) {
	b := fixture(t, Limits{MaxSessions: 1, MaxDataConnections: 1, MaxDataPerSession: 1}, openFunc(func(context.Context, OpenRequest) (net.Conn, error) { return pipe(t), nil }))
	s := admit(t, b, binding())
	original := dial(t, s.DataDialer(), s.binding.Authority).(*connection)
	copied := cloneHandle(original)
	if err := copied.Close(); !errors.Is(err, ErrInvalid) {
		t.Fatal("copied connection confirmed another owner's close", err)
	}
	if b.Snapshot().DataConnections != 1 {
		t.Fatal("copied connection changed capacity")
	}
	if err := original.Close(); err != nil {
		t.Fatal(err)
	}
	if b.Snapshot().DataConnections != 0 {
		t.Fatal("original close did not release capacity")
	}
}

func TestUnregisteredSessionCannotDialOrClose(t *testing.T) {
	b := fixture(t, Limits{MaxSessions: 1, MaxDataConnections: 1, MaxDataPerSession: 1}, openFunc(func(context.Context, OpenRequest) (net.Conn, error) {
		t.Fatal("unregistered session reached opener")
		return nil, nil
	}))
	original := admit(t, b, binding())
	unregistered := cloneHandle(original)
	// Only package-internal code can construct this state. The inventory check
	// remains independent from the self-pointer guard.
	unregistered.self = unregistered
	if _, err := unregistered.CancellationDialer().DialContext(context.Background(), "tcp", original.binding.Authority); !errors.Is(err, ErrInvalid) {
		t.Fatal("unregistered session acquired capacity", err)
	}
	if err := unregistered.Close(context.Background()); !errors.Is(err, ErrInvalid) {
		t.Fatal("unregistered session cancelled the original", err)
	}
}

func TestBoundToRejectsCopiedReboundAndClosedSessions(t *testing.T) {
	b := fixture(t, Limits{MaxSessions: 1, MaxDataConnections: 1, MaxDataPerSession: 1}, openFunc(func(context.Context, OpenRequest) (net.Conn, error) { return pipe(t), nil }))
	expected := binding()
	s := admit(t, b, expected)
	if !s.BoundTo(expected) || cloneHandle(s).BoundTo(expected) {
		t.Fatal("session ownership comparison failed")
	}
	other := expected
	other.Tenant = "other-tenant"
	if s.BoundTo(other) {
		t.Fatal("session accepted another tenant")
	}
	other = expected
	other.SourceRevision = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	if s.BoundTo(other) {
		t.Fatal("session accepted another revision")
	}
	if err := s.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if s.BoundTo(expected) {
		t.Fatal("closed session retained admission authority")
	}
}
