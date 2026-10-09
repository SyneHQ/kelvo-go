// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package rabbitconnect

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/SYNEHQ/kelvo-go/transportissuer"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/transportbroker"
	"github.com/SYNEHQ/kelvo-go/sourceproof"
)

func proofScope(o *Opener, r transportbroker.OpenRequest) sourceproof.Scope {
	b, e := r.Binding, r.Binding.Execution
	return sourceproof.Scope{Issuer: b.Issuer, Audience: b.Audience, ClusterTenant: b.ClusterTenant, ServicePrincipal: b.ServicePrincipal, Tenant: b.Tenant, Source: b.Source, SourceRevision: b.SourceRevision, Authority: b.Authority, RouteID: "rabbit-prod", TokenID: "11111111-1111-1111-1111-111111111111", BindingVersion: 1, Kind: e.Kind, ExecutionID: e.ID, GrantSHA256: e.GrantSHA256, Worker: e.Worker, Owner: e.Owner, Claim: e.Claim, WorkerIdentity: o.identity, WorkerCertSHA256: o.certDigest}
}

func TestProofRefreshPrecedesEveryPhysicalOpen(t *testing.T) {
	f := newFixture(t)
	request := testRequest()
	grant := "original.signed.grant"
	request.Binding.Execution.GrantSHA256 = sourceproof.GrantDigest(grant)
	refreshes, issues, dials := 0, 0, 0
	issuer := issueFunc(func(_ context.Context, r IssueRequest) (string, error) {
		issues++
		if r.PrivateSource == nil || r.PrivateSource.Grant != grant || r.RouteID != "rabbit-prod" || r.BindingVersion != 1 {
			t.Fatal("issuer lost original proof authority")
		}
		claims := claimsFor(r)
		claims.TokenID = r.TokenID
		claims.ExpiresAt = time.Now().Add(5 * time.Second).Unix()
		return signTicket(f.key, claims), nil
	})
	base, err := New(f.config, issuer)
	if err != nil {
		t.Fatal(err)
	}
	scope := proofScope(base, request)
	opener, err := NewWithSourceProof(f.config, issuer, SourceProofConfig{PublicKey: f.key.Public().(ed25519.PublicKey), Scope: scope, Refresh: func(context.Context) (sourceproof.Envelope, error) {
		refreshes++
		if refreshes > 2 {
			return sourceproof.Envelope{}, errors.New("binding revoked")
		}
		now := time.Now()
		return sourceproof.Sign(f.key, scope, grant, now, now.Add(10*time.Second))
	}})
	if err != nil {
		t.Fatal(err)
	}
	opener.dial = func(context.Context, string, string) (net.Conn, error) { dials++; return nil, transportbroker.ErrOpen }
	for range 3 {
		if conn, err := opener.Open(context.Background(), request); err == nil || conn != nil {
			t.Fatal("fixture transport unexpectedly opened")
		}
	}
	if refreshes != 3 || issues != 2 || dials != 2 {
		t.Fatalf("proof refresh or revocation bypassed: %d/%d/%d", refreshes, issues, dials)
	}
}

func TestProofSubstitutionDeniedBeforeIssuer(t *testing.T) {
	for _, mode := range []string{"route", "token", "version", "grant", "expired", "caller"} {
		t.Run(mode, func(t *testing.T) {
			f := newFixture(t)
			request := testRequest()
			grant := "original.signed.grant"
			request.Binding.Execution.GrantSHA256 = sourceproof.GrantDigest(grant)
			issuer := issueFunc(func(context.Context, IssueRequest) (string, error) {
				t.Fatal("unverified proof reached issuer")
				return "", nil
			})
			base, err := New(f.config, issuer)
			if err != nil {
				t.Fatal(err)
			}
			expected := proofScope(base, request)
			actual := expected
			when := time.Now()
			switch mode {
			case "route":
				actual.RouteID = "other"
			case "token":
				actual.TokenID = "22222222-1111-1111-1111-111111111111"
			case "version":
				actual.BindingVersion++
			case "grant":
				grant = "other.signed.grant"
				actual.GrantSHA256 = sourceproof.GrantDigest(grant)
			case "expired":
				when = when.Add(-time.Minute)
			case "caller":
				actual.WorkerIdentity = "spiffe://other/worker"
			}
			envelope, err := sourceproof.Sign(f.key, actual, grant, when, when.Add(time.Second))
			if err != nil {
				t.Fatal(err)
			}
			opener, err := NewWithSourceProof(f.config, issuer, SourceProofConfig{PublicKey: f.key.Public().(ed25519.PublicKey), Scope: expected, Refresh: func(context.Context) (sourceproof.Envelope, error) { return envelope, nil }})
			if err != nil {
				t.Fatal(err)
			}
			opener.dial = func(context.Context, string, string) (net.Conn, error) {
				t.Fatal("unverified proof reached proxy")
				return nil, nil
			}
			if conn, err := opener.Open(context.Background(), request); err == nil || conn != nil {
				t.Fatal("unverified proof accepted")
			}
		})
	}
}

func TestHTTPIssuerCarriesOriginalSourceProof(t *testing.T) {
	f := newFixture(t)
	var expected IssueRequest
	client, opener := httpIssuerFixture(t, f, f.server, func(w http.ResponseWriter, r *http.Request) {
		raw, err := io.ReadAll(io.LimitReader(r.Body, transportissuer.MaxRequestBytes+1))
		if err != nil {
			t.Error(err)
		}
		var got transportissuer.Request
		if strictJSON(raw, &got) != nil || got.PrivateSource == nil || *got.PrivateSource != *expected.PrivateSource || got.RouteID != expected.RouteID || got.TokenID != expected.TokenID || got.BindingVersion != expected.BindingVersion {
			t.Error("issuer transport changed proof or source binding")
		}
		digest := sha256.Sum256(raw)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(transportissuer.Response{Version: 1, RequestSHA256: hex.EncodeToString(digest[:]), Token: "fixture-ticket"})
	})
	request := testRequest()
	grant := "original.signed.grant"
	request.Binding.Execution.GrantSHA256 = sourceproof.GrantDigest(grant)
	expected = issueFor(opener, request)
	scope := proofScope(opener, request)
	now := time.Now()
	proof, err := sourceproof.Sign(f.key, scope, grant, now, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	expected.PrivateSource = &proof
	expected.RouteID = scope.RouteID
	expected.TokenID = scope.TokenID
	expected.BindingVersion = scope.BindingVersion
	if _, err := client.Issue(context.Background(), expected); err != nil {
		t.Fatal(err)
	}
}

func TestProofDoesNotExtendTicketAdmission(t *testing.T) {
	f := newFixture(t)
	request := testRequest()
	grant := "original.signed.grant"
	request.Binding.Execution.GrantSHA256 = sourceproof.GrantDigest(grant)
	issuer := issueFunc(func(_ context.Context, r IssueRequest) (string, error) {
		c := claimsFor(r)
		c.TokenID = r.TokenID
		return signTicket(f.key, c), nil
	})
	base, err := New(f.config, issuer)
	if err != nil {
		t.Fatal(err)
	}
	scope := proofScope(base, request)
	opener, err := NewWithSourceProof(f.config, issuer, SourceProofConfig{PublicKey: f.key.Public().(ed25519.PublicKey), Scope: scope, Refresh: func(context.Context) (sourceproof.Envelope, error) {
		now := time.Now()
		return sourceproof.Sign(f.key, scope, grant, now, now.Add(5*time.Second))
	}})
	if err != nil {
		t.Fatal(err)
	}
	opener.dial = func(context.Context, string, string) (net.Conn, error) {
		t.Fatal("ticket exceeded proof admission deadline")
		return nil, nil
	}
	if conn, err := opener.Open(context.Background(), request); err == nil || conn != nil {
		t.Fatal("overlong ticket accepted")
	}
}

func TestSlowFreshProofPreservesShortTransportBudget(t *testing.T) {
	f := newFixture(t)
	request := testRequest()
	grant := "original.signed.grant"
	request.Binding.Execution.GrantSHA256 = sourceproof.GrantDigest(grant)
	f.config.ProxyAddress = serveOnce(t, f.server, func(conn net.Conn) {
		if _, err := readCONNECT(conn); err != nil {
			return
		}
		_, _ = io.WriteString(conn, "HTTP/1.1 200 Connection Established\r\n\r\nok")
	})
	var remaining time.Duration
	issuer := issueFunc(func(ctx context.Context, r IssueRequest) (string, error) {
		until, _ := ctx.Deadline()
		remaining = time.Until(until)
		claims := claimsFor(r)
		claims.TokenID = r.TokenID
		claims.ExpiresAt = time.Now().Add(4 * time.Second).Unix()
		return signTicket(f.key, claims), nil
	})
	base, err := New(f.config, issuer)
	if err != nil {
		t.Fatal(err)
	}
	scope := proofScope(base, request)
	opener, err := NewWithSourceProof(f.config, issuer, SourceProofConfig{PublicKey: f.key.Public().(ed25519.PublicKey), Scope: scope, RefreshTimeout: 4 * time.Second, Refresh: func(ctx context.Context) (sourceproof.Envelope, error) {
		timer := time.NewTimer(2200 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return sourceproof.Envelope{}, ctx.Err()
		}
		now := time.Now()
		return sourceproof.Sign(f.key, scope, grant, now, now.Add(5*time.Second))
	}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	conn, err := opener.Open(ctx, request)
	if err != nil {
		t.Fatal("fresh resolver exceeded old 2s bound", err)
	}
	defer conn.Close()
	if remaining < 1500*time.Millisecond || remaining > MaxSetupTime {
		t.Fatalf("network setup budget %s", remaining)
	}
	body := make([]byte, 2)
	if _, err := io.ReadFull(conn, body); err != nil || string(body) != "ok" {
		t.Fatal("tunnel payload unavailable", err)
	}
}

func TestFreshProofStopsBeforeIssueOnParentOrScopeExpiry(t *testing.T) {
	for _, mode := range []string{"deadline", "cancel", "binding_expiry", "expired_proof", "denied", "cancel_with_nil_error", "binding_expiry_with_nil_error", "certificate_expiry_with_nil_error"} {
		t.Run(mode, func(t *testing.T) {
			f := newFixture(t)
			request := testRequest()
			grant := "original.signed.grant"
			request.Binding.Execution.GrantSHA256 = sourceproof.GrantDigest(grant)
			issuer := issueFunc(func(context.Context, IssueRequest) (string, error) {
				t.Error("failed proof reached issuer")
				return "", errors.New("unexpected")
			})
			base, err := New(f.config, issuer)
			if err != nil {
				t.Fatal(err)
			}
			scope := proofScope(base, request)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if mode == "deadline" {
				var stop context.CancelFunc
				ctx, stop = context.WithTimeout(ctx, 40*time.Millisecond)
				defer stop()
			}
			if mode == "binding_expiry" || mode == "binding_expiry_with_nil_error" {
				request.Binding.ExpiresAt = time.Now().Add(40 * time.Millisecond)
			}
			opener, err := NewWithSourceProof(f.config, issuer, SourceProofConfig{PublicKey: f.key.Public().(ed25519.PublicKey), Scope: scope, RefreshTimeout: 4 * time.Second, Refresh: func(c context.Context) (sourceproof.Envelope, error) {
				if mode == "denied" {
					return sourceproof.Envelope{}, errors.New("revoked")
				}
				if mode == "expired_proof" {
					now := time.Now().Add(-time.Minute)
					return sourceproof.Sign(f.key, scope, grant, now, now.Add(time.Second))
				}
				if mode == "cancel_with_nil_error" {
					cancel()
					now := time.Now()
					return sourceproof.Sign(f.key, scope, grant, now, now.Add(time.Second))
				}
				if mode == "binding_expiry_with_nil_error" || mode == "certificate_expiry_with_nil_error" {
					time.Sleep(80 * time.Millisecond)
					now := time.Now()
					return sourceproof.Sign(f.key, scope, grant, now, now.Add(time.Second))
				}
				if mode == "cancel" {
					cancel()
				}
				<-c.Done()
				return sourceproof.Envelope{}, c.Err()
			}})
			if err != nil {
				t.Fatal(err)
			}
			if mode == "certificate_expiry_with_nil_error" {
				opener.certificateUntil = time.Now().Add(40 * time.Millisecond)
			}
			opener.dial = func(context.Context, string, string) (net.Conn, error) {
				t.Error("failed proof reached proxy")
				return nil, errors.New("unexpected")
			}
			start := time.Now()
			conn, err := opener.Open(ctx, request)
			if err == nil || conn != nil || time.Since(start) > time.Second {
				t.Fatal("failed proof did not stop promptly", err)
			}
		})
	}
}

func TestFreshProofTimeoutBounds(t *testing.T) {
	f := newFixture(t)
	request := testRequest()
	base, err := New(f.config, f.issuer())
	if err != nil {
		t.Fatal(err)
	}
	for _, timeout := range []time.Duration{-1, MaxSourceResolutionTime + 1} {
		_, err := NewWithSourceProof(f.config, f.issuer(), SourceProofConfig{PublicKey: f.key.Public().(ed25519.PublicKey), Scope: proofScope(base, request), RefreshTimeout: timeout, Refresh: func(context.Context) (sourceproof.Envelope, error) { return sourceproof.Envelope{}, nil }})
		if err == nil {
			t.Fatal("invalid refresh timeout accepted")
		}
	}
}
