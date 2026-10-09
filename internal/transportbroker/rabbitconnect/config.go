// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0

// Package rabbitconnect implements the parent-side Rabbit v1 CONNECT opener.
// Issuer credentials, TLS keys and tickets belong to the parent.
// Native source TLS remains the driver's responsibility.
package rabbitconnect

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"net"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/transportbroker"
	"github.com/SYNEHQ/kelvo-go/sourceproof"
	"github.com/SYNEHQ/kelvo-go/transportissuer"
)

const MaxSetupTime = 2 * time.Second

// IssueRequest contains the exact admitted scope. Purpose is deliberately absent:
// v1 tickets do not provide downstream reserved cancellation capacity.
type IssueRequest struct {
	PrivateSource                            *sourceproof.Envelope
	RouteID, TokenID                         string
	BindingVersion                           int64
	Binding                                  transportbroker.Binding
	OpenID, WorkerIdentity, WorkerCertSHA256 string
}

// Issuer belongs to the trusted parent. Issue must obey ctx and join its work
// before returning. It may not retry, redirect or return an unbounded response.
// A concrete issuer endpoint and its custody/timeout tests remain activation gates.
type Issuer interface {
	Issue(context.Context, IssueRequest) (string, error)
}

// Config is operator-owned. PEM is parsed into private copies; no caller can
// replace trust or keys after construction. The proxy endpoint never comes from
// a source, query, ticket or issuer response.
type Config struct {
	ProxyAddress, ProxyServerName                     string
	RootCAPEM, ClientCertificatePEM, ClientKeyPEM     []byte
	WorkerIdentity                                    string
	Issuer, Audience, ClusterTenant, ServicePrincipal string
	IssuerPublicKey                                   ed25519.PublicKey
	SetupTimeout                                      time.Duration
	AcceptedOpenTrust                                 *transportissuer.AcceptedOpenTrust
}

type Opener struct {
	sourceProof                                 *SourceProofConfig
	acceptedTrust                               *transportissuer.AcceptedOpenTrust
	proxy, issuer, audience, cluster, principal string
	identity, certDigest                        string
	certificateFrom, certificateUntil           time.Time
	key                                         ed25519.PublicKey
	tls                                         *tls.Config
	timeout                                     time.Duration
	tickets                                     Issuer
	dial                                        func(context.Context, string, string) (net.Conn, error)
}

func New(config Config, issuer Issuer) (*Opener, error) {
	if config.SetupTimeout == 0 {
		config.SetupTimeout = MaxSetupTime
	}
	if issuer == nil || config.SetupTimeout <= 0 || config.SetupTimeout > MaxSetupTime ||
		transportbroker.ValidateAuthority(config.ProxyAddress) != nil ||
		transportbroker.ValidateAuthority(net.JoinHostPort(config.ProxyServerName, "443")) != nil ||
		len(config.IssuerPublicKey) != ed25519.PublicKeySize || !textValue(config.WorkerIdentity, 512) ||
		len(config.RootCAPEM) == 0 || len(config.RootCAPEM) > 1<<20 ||
		len(config.ClientCertificatePEM) == 0 || len(config.ClientCertificatePEM) > 64<<10 ||
		len(config.ClientKeyPEM) == 0 || len(config.ClientKeyPEM) > 64<<10 {
		return nil, transportbroker.ErrInvalid
	}
	for _, value := range []string{config.Issuer, config.Audience, config.ClusterTenant, config.ServicePrincipal} {
		if !textValue(value, 128) {
			return nil, transportbroker.ErrInvalid
		}
	}
	var accepted *transportissuer.AcceptedOpenTrust
	if config.AcceptedOpenTrust != nil {
		a := config.AcceptedOpenTrust
		if a.KeyID == "" || len(a.PublicKey) != ed25519.PublicKeySize || bytes.Equal(a.PublicKey, config.IssuerPublicKey) {
			return nil, transportbroker.ErrInvalid
		}
		accepted = &transportissuer.AcceptedOpenTrust{KeyID: a.KeyID, PublicKey: append(ed25519.PublicKey(nil), a.PublicKey...)}
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(config.RootCAPEM) {
		return nil, transportbroker.ErrInvalid
	}
	pair, err := tls.X509KeyPair(config.ClientCertificatePEM, config.ClientKeyPEM)
	if err != nil || len(pair.Certificate) == 0 {
		return nil, transportbroker.ErrInvalid
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil || len(leaf.URIs) != 1 || leaf.URIs[0] == nil || !leaf.URIs[0].IsAbs() ||
		leaf.URIs[0].String() != config.WorkerIdentity {
		return nil, transportbroker.ErrInvalid
	}
	clientUsage := false
	for _, usage := range leaf.ExtKeyUsage {
		clientUsage = clientUsage || usage == x509.ExtKeyUsageClientAuth
	}
	if !clientUsage {
		return nil, transportbroker.ErrInvalid
	}
	from, until := leaf.NotBefore, leaf.NotAfter
	for _, der := range pair.Certificate[1:] {
		certificate, err := x509.ParseCertificate(der)
		if err != nil {
			return nil, transportbroker.ErrInvalid
		}
		if certificate.NotBefore.After(from) {
			from = certificate.NotBefore
		}
		if certificate.NotAfter.Before(until) {
			until = certificate.NotAfter
		}
	}
	now := time.Now()
	if now.Before(from) || !now.Before(until) {
		return nil, transportbroker.ErrInvalid
	}
	pair.Leaf = leaf
	digest := sha256.Sum256(leaf.Raw)
	return &Opener{
		acceptedTrust: accepted, proxy: config.ProxyAddress, issuer: config.Issuer, audience: config.Audience,
		cluster: config.ClusterTenant, principal: config.ServicePrincipal,
		identity: config.WorkerIdentity, certDigest: hex.EncodeToString(digest[:]),
		certificateFrom: from, certificateUntil: until,
		key: append(ed25519.PublicKey(nil), config.IssuerPublicKey...),
		tls: &tls.Config{MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13,
			RootCAs: roots, ServerName: config.ProxyServerName, Certificates: []tls.Certificate{pair},
			NextProtos: []string{"http/1.1"}},
		timeout: config.SetupTimeout, tickets: issuer, dial: (&net.Dialer{}).DialContext,
	}, nil
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
