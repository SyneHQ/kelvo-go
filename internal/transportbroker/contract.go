// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0

// Package transportbroker owns operation-scoped private transport connections.
// It is not wired into any runtime or configuration. An Opener belongs to the
// trusted parent; a driver receives only a Dialer for one admitted execution.
package transportbroker

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"time"
)

var (
	ErrInvalid     = errors.New("private transport configuration or binding is invalid")
	ErrScope       = errors.New("private transport dial is outside the admitted source")
	ErrCapacity    = errors.New("private transport capacity is exhausted")
	ErrClosed      = errors.New("private transport custody is closed")
	ErrOpen        = errors.New("private transport connection could not be established")
	ErrCleanup     = errors.New("private transport cleanup is not confirmed")
	ErrUnsupported = errors.New("private transport connection does not support half-close")
)

type Execution struct {
	Kind, ID, GrantSHA256 string
	Worker, Owner, Claim  string
}

// Binding is supplied by the trusted parent after admission and current source
// authorization. ExpiresAt cannot extend that authority or execution custody.
// Authority is the original canonical database host:port, not the proxy address.
type Binding struct {
	Issuer, Audience                string
	ClusterTenant, ServicePrincipal string
	Tenant, Source, SourceRevision  string
	Authority                       string
	Execution                       Execution
	ExpiresAt                       time.Time
}

type Purpose uint8

const (
	Data Purpose = iota + 1
	Cancellation
)

// OpenRequest never crosses into the child. Each physical connection, including
// cancellation, gets a new random ID and must get its own one-use ticket.
// Purpose selects local capacity only; it does not grant additional authority.
type OpenRequest struct {
	Binding Binding
	ID      string
	Purpose Purpose
}

// Opener owns proxy configuration, issuer access, TLS keys and tickets. It must
// authenticate the proxy, validate the exact source/route and execution, and
// return only after the peer accepts the tunnel. It must obey ctx, preserve any
// buffered payload and never retry or fall back to a direct database connection.
// No request can select a proxy endpoint, credentials or trust configuration.
// A nil Close error must mean the physical connection is closed.
type Opener interface {
	Open(context.Context, OpenRequest) (net.Conn, error)
}

// Dialer is the entire driver-facing capability. It contains no issuer, route,
// credential, ticket or source-selection API. Future IPC must bind this handle
// to its owning child; this in-process interface is not a process boundary.
type Dialer interface {
	DialContext(context.Context, string, string) (net.Conn, error)
}

func validBinding(b Binding, now time.Time) bool {
	if !b.ExpiresAt.After(now) || b.ExpiresAt.After(now.Add(24*time.Hour)) || !validAuthority(b.Authority) || !hexValue(b.SourceRevision, 64) {
		return false
	}
	for _, value := range []string{b.Issuer, b.Audience, b.ClusterTenant, b.ServicePrincipal, b.Tenant, b.Source} {
		if !textValue(value, 128) {
			return false
		}
	}
	e := b.Execution
	return (e.Kind == "query" || e.Kind == "operation") && textValue(e.ID, 128) && textValue(e.Worker, 32) &&
		hexValue(e.GrantSHA256, 64) && hexValue(e.Owner, 32) && hexValue(e.Claim, 32)
}

func textValue(value string, maximum int) bool {
	if value == "" || len(value) > maximum {
		return false
	}
	for _, char := range value {
		if char <= 32 || char >= 127 {
			return false
		}
	}
	return true
}

func hexValue(value string, length int) bool {
	if len(value) != length {
		return false
	}
	for _, char := range value {
		if !(char >= '0' && char <= '9' || char >= 'a' && char <= 'f') {
			return false
		}
	}
	return true
}

// Match the canonical CONNECT authority without resolving it on the worker.
func validAuthority(authority string) bool {
	if authority == "" || len(authority) > 320 {
		return false
	}
	host, port, err := net.SplitHostPort(authority)
	number, portErr := strconv.Atoi(port)
	if err != nil || portErr != nil || number < 1 || number > 65535 || strconv.Itoa(number) != port || host == "" || net.JoinHostPort(host, port) != authority {
		return false
	}
	if address, err := netip.ParseAddr(host); err == nil {
		return address.Zone() == "" && address.String() == host && !address.IsUnspecified() && !address.IsMulticast()
	}
	if len(host) > 253 || host != strings.ToLower(host) {
		return false
	}
	for _, label := range strings.Split(host, ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, char := range label {
			if !(char >= 'a' && char <= 'z' || char >= '0' && char <= '9' || char == '-') {
				return false
			}
		}
	}
	return true
}
