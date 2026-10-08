// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package rabbitconnect

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/transportbroker"
)

func TestTicketPinsEveryAdmittedBinding(t *testing.T) {
	f := newFixture(t)
	o, err := New(f.config, f.issuer())
	if err != nil {
		t.Fatal(err)
	}
	request := issueFor(o, testRequest())
	valid := claimsFor(request)
	if err := o.verifyTicket(signTicket(f.key, valid), request, time.Now()); err != nil {
		t.Fatal(err)
	}
	changes := map[string]func(*ticketClaims){
		"version": func(c *ticketClaims) { c.Version = 2 }, "issuer": func(c *ticketClaims) { c.Issuer = "other" },
		"audience": func(c *ticketClaims) { c.Audience = "other" }, "nonce": func(c *ticketClaims) { c.ID = strings.Repeat("f", 64) },
		"cluster": func(c *ticketClaims) { c.ClusterTenant = "other" }, "principal": func(c *ticketClaims) { c.ServicePrincipal = "other" },
		"tenant": func(c *ticketClaims) { c.Tenant = "other" }, "source": func(c *ticketClaims) { c.Source = "other" },
		"revision": func(c *ticketClaims) { c.SourceRevision = strings.Repeat("f", 64) }, "authority": func(c *ticketClaims) { c.Authority = "other.internal:5432" },
		"worker-uri": func(c *ticketClaims) { c.WorkerIdentity += "/other" }, "worker-certificate": func(c *ticketClaims) { c.WorkerCertSHA256 = strings.Repeat("f", 64) },
		"execution-kind": func(c *ticketClaims) { c.Execution.Kind = "operation" }, "execution-id": func(c *ticketClaims) { c.Execution.ID = "other" },
		"execution-grant": func(c *ticketClaims) { c.Execution.GrantSHA256 = strings.Repeat("f", 64) }, "execution-worker": func(c *ticketClaims) { c.Execution.Worker = "other" },
		"execution-owner": func(c *ticketClaims) { c.Execution.Owner = strings.Repeat("f", 32) }, "execution-claim": func(c *ticketClaims) { c.Execution.Claim = strings.Repeat("f", 32) },
		"empty-route": func(c *ticketClaims) { c.TokenID = "" }, "generation": func(c *ticketClaims) { c.TokenGeneration = "bad" },
		"tunnel": func(c *ticketClaims) { c.TunnelID = "bad" }, "control": func(c *ticketClaims) { c.ControlOwner = "bad" },
		"expired": func(c *ticketClaims) { c.ExpiresAt = c.IssuedAt }, "future": func(c *ticketClaims) { c.IssuedAt += 10 },
		"open-ttl": func(c *ticketClaims) { c.ExpiresAt = c.IssuedAt + 61 }, "session-ttl": func(c *ticketClaims) { c.SessionExpiresAt = c.IssuedAt + 86401 },
		"past-binding": func(c *ticketClaims) { c.SessionExpiresAt = request.Binding.ExpiresAt.Unix() + 1 },
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			claims := valid
			change(&claims)
			if err := o.verifyTicket(signTicket(f.key, claims), request, time.Now()); !errors.Is(err, transportbroker.ErrOpen) {
				t.Fatal("issuer changed admitted authority")
			}
		})
	}
	before := o.certificateUntil
	o.certificateUntil = time.Now().Add(time.Minute)
	if err := o.verifyTicket(signTicket(f.key, valid), request, time.Now()); err == nil {
		t.Fatal("ticket outlived worker certificate")
	}
	o.certificateUntil = before
}

func TestTicketRejectsSignedAmbiguityAndHeaderInjection(t *testing.T) {
	f := newFixture(t)
	o, err := New(f.config, f.issuer())
	if err != nil {
		t.Fatal(err)
	}
	request := issueFor(o, testRequest())
	claims := claimsFor(request)
	header, _ := json.Marshal(ticketHeader{"EdDSA", "rabbit-connect+jwt", claims.Issuer})
	payload, _ := json.Marshal(claims)
	for name, body := range map[string]string{
		"duplicate":         strings.Replace(string(payload), `"tenant":"tenant"`, `"tenant":"tenant","tenant":"other"`, 1),
		"case-alias":        strings.Replace(string(payload), `"tenant":"tenant"`, `"tenant":"tenant","TENANT":"tenant"`, 1),
		"unknown":           strings.TrimSuffix(string(payload), "}") + `,"parent_open_sha256":"unused"}`,
		"trailing-document": string(payload) + `{}`,
	} {
		t.Run(name, func(t *testing.T) {
			if o.verifyTicket(signPayload(f.key, header, []byte(body)), request, time.Now()) == nil {
				t.Fatal("ambiguous signed response accepted")
			}
		})
	}
	valid := signTicket(f.key, claims)
	for name, token := range map[string]string{"newline": valid + "\r\n", "oversize": strings.Repeat("a", maxTicketBytes+1), "wrong-signature": valid[:len(valid)-8] + "aaaaaaaa", "missing": ""} {
		t.Run(name, func(t *testing.T) {
			if o.verifyTicket(token, request, time.Now()) == nil {
				t.Fatal("malformed token accepted")
			}
		})
	}
	for name, h := range map[string]ticketHeader{"algorithm": {"none", "rabbit-connect+jwt", claims.Issuer}, "type": {"EdDSA", "rabbit-connect-reservation+jwt", claims.Issuer}, "issuer-key": {"EdDSA", "rabbit-connect+jwt", "other"}} {
		t.Run(name, func(t *testing.T) {
			encoded, _ := json.Marshal(h)
			if o.verifyTicket(signPayload(f.key, encoded, payload), request, time.Now()) == nil {
				t.Fatal("wrong signing context accepted")
			}
		})
	}
}

func TestInvalidScopeAndIssuerResponseNeverDial(t *testing.T) {
	f := newFixture(t)
	for _, scenario := range []string{"invalid-request", "foreign-service", "changed-tenant", "reused-ticket", "issuer-error"} {
		t.Run(scenario, func(t *testing.T) {
			calls, dials := 0, 0
			o, err := New(f.config, issueFunc(func(_ context.Context, r IssueRequest) (string, error) {
				calls++
				if scenario == "issuer-error" {
					return "", errors.New("private issuer detail")
				}
				claims := claimsFor(r)
				if scenario == "changed-tenant" {
					claims.Tenant = "other"
				}
				if scenario == "reused-ticket" {
					claims.ID = strings.Repeat("f", 64)
				}
				return signTicket(f.key, claims), nil
			}))
			if err != nil {
				t.Fatal(err)
			}
			o.dial = func(context.Context, string, string) (net.Conn, error) {
				dials++
				return nil, errors.New("unexpected dial")
			}
			request := testRequest()
			if scenario == "invalid-request" {
				request.ID = "bad"
			}
			if scenario == "foreign-service" {
				request.Binding.ServicePrincipal = "other"
			}
			conn, err := o.Open(context.Background(), request)
			if err == nil || conn != nil || dials != 0 {
				t.Fatal("unverified response reached proxy")
			}
			want := 1
			if scenario == "invalid-request" || scenario == "foreign-service" {
				want = 0
			}
			if calls != want {
				t.Fatal("issuer called outside admitted scope or retried")
			}
		})
	}
}

func TestConstructorRejectsUntrustedOrUnboundedConfiguration(t *testing.T) {
	for name, change := range map[string]func(*Config){
		"proxy": func(c *Config) { c.ProxyAddress = "https://proxy.test:443" }, "server-name": func(c *Config) { c.ProxyServerName = "" },
		"roots": func(c *Config) { c.RootCAPEM = nil }, "certificate": func(c *Config) { c.ClientCertificatePEM = nil }, "key": func(c *Config) { c.ClientKeyPEM = nil },
		"worker": func(c *Config) { c.WorkerIdentity += "/other" }, "trust-key": func(c *Config) { c.IssuerPublicKey = ed25519.PublicKey{1} },
		"timeout": func(c *Config) { c.SetupTimeout = MaxSetupTime + time.Nanosecond }, "negative-timeout": func(c *Config) { c.SetupTimeout = -time.Second },
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			change(&f.config)
			if o, err := New(f.config, f.issuer()); o != nil || !errors.Is(err, transportbroker.ErrInvalid) {
				t.Fatal("unsafe operator configuration accepted")
			}
		})
	}
}
