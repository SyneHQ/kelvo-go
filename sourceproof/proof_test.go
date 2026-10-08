// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package sourceproof

import (
	"crypto/ed25519"
	"crypto/rand"
	"strings"
	"testing"
	"time"
)

func fixture(t *testing.T) (ed25519.PrivateKey, Scope, time.Time) {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return key, Scope{Issuer: "application", Audience: "kelvo", ClusterTenant: "cluster", ServicePrincipal: "analytics", Tenant: "team", Source: "source", SourceRevision: strings.Repeat("a", 64), Authority: "db.private:5432", RouteID: "private", TokenID: "11111111-2222-3333-4444-555555555555", BindingVersion: 1, Kind: "query", ExecutionID: "job", GrantSHA256: GrantDigest("signed.original.grant"), Worker: "worker", Owner: strings.Repeat("b", 32), Claim: strings.Repeat("c", 32), WorkerIdentity: "spiffe://kelvo/tenant/cluster/worker/worker", WorkerCertSHA256: strings.Repeat("d", 64)}, time.Unix(1800000000, 0)
}

func TestProofExactScope(t *testing.T) {
	key, scope, now := fixture(t)
	envelope, err := Sign(key, scope, "signed.original.grant", now, now.Add(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = Verify(key.Public().(ed25519.PublicKey), envelope, scope, now); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*Scope){
		"route":           func(s *Scope) { s.RouteID = "other" },
		"token":           func(s *Scope) { s.TokenID = "22222222-2222-3333-4444-555555555555" },
		"binding_version": func(s *Scope) { s.BindingVersion++ },
		"tenant":          func(s *Scope) { s.Tenant = "other" },
		"cluster":         func(s *Scope) { s.ClusterTenant = "other" },
		"source":          func(s *Scope) { s.Source = "other" },
		"revision":        func(s *Scope) { s.SourceRevision = strings.Repeat("e", 64) },
		"authority":       func(s *Scope) { s.Authority = "other.private:5432" },
		"kind":            func(s *Scope) { s.Kind = "operation" },
		"execution":       func(s *Scope) { s.ExecutionID = "other" },
		"worker":          func(s *Scope) { s.Worker = "other" },
		"owner":           func(s *Scope) { s.Owner = strings.Repeat("e", 32) },
		"claim":           func(s *Scope) { s.Claim = strings.Repeat("e", 32) },
		"caller_uri":      func(s *Scope) { s.WorkerIdentity = "spiffe://other/worker" },
		"caller_cert":     func(s *Scope) { s.WorkerCertSHA256 = strings.Repeat("e", 64) },
		"issuer":          func(s *Scope) { s.Issuer = "other" },
		"audience":        func(s *Scope) { s.Audience = "other" },
		"principal":       func(s *Scope) { s.ServicePrincipal = "other" },
	} {
		t.Run(name, func(t *testing.T) {
			other := scope
			mutate(&other)
			if _, err := Verify(key.Public().(ed25519.PublicKey), envelope, other, now); err == nil {
				t.Fatal("accepted scope mismatch")
			}
		})
	}
	other := envelope
	other.Grant = "other.original.grant"
	if _, err := Verify(key.Public().(ed25519.PublicKey), other, scope, now); err == nil {
		t.Fatal("accepted substituted grant")
	}
	_, wrong, _ := ed25519.GenerateKey(rand.Reader)
	if _, err := Verify(wrong.Public().(ed25519.PublicKey), envelope, scope, now); err == nil {
		t.Fatal("accepted different signer")
	}
}

func TestProofBoundsAndExpiry(t *testing.T) {
	key, scope, now := fixture(t)
	for _, expires := range []time.Time{now, now.Add(-time.Second), now.Add(MaxLifetime + time.Second)} {
		if _, err := Sign(key, scope, "signed.original.grant", now, expires); err == nil {
			t.Fatal("accepted invalid lifetime")
		}
	}
	envelope, err := Sign(key, scope, "signed.original.grant", now, now.Add(MaxLifetime))
	if err != nil {
		t.Fatal(err)
	}
	for _, when := range []time.Time{now.Add(-time.Second), now.Add(MaxLifetime)} {
		if _, err := Verify(key.Public().(ed25519.PublicKey), envelope, scope, when); err == nil {
			t.Fatal("accepted expired or future proof")
		}
	}
	for _, grant := range []string{"", strings.Repeat("x", MaxGrantBytes+1), "secret\n"} {
		other := scope
		other.GrantSHA256 = GrantDigest(grant)
		if _, err := Sign(key, other, grant, now, now.Add(time.Second)); err == nil {
			t.Fatal("accepted invalid grant")
		}
	}
	for _, token := range []string{"", envelope.Token + ".", strings.Repeat("x", MaxTokenBytes+1), strings.Replace(envelope.Token, "v1", "v2", 1)} {
		other := envelope
		other.Token = token
		if _, err := Verify(key.Public().(ed25519.PublicKey), other, scope, now); err == nil {
			t.Fatal("accepted invalid token")
		}
	}
}

func TestProofRejectsInvalidBindingScope(t *testing.T) {
	key, scope, now := fixture(t)
	for name, change := range map[string]func(*Scope){
		"missing_route":      func(s *Scope) { s.RouteID = "" },
		"route_path":         func(s *Scope) { s.RouteID = "../route" },
		"missing_token":      func(s *Scope) { s.TokenID = "" },
		"noncanonical_token": func(s *Scope) { s.TokenID = "AAAAAAAA-2222-3333-4444-555555555555" },
		"missing_version":    func(s *Scope) { s.BindingVersion = 0 },
		"negative_version":   func(s *Scope) { s.BindingVersion = -1 },
		"overflow_version":   func(s *Scope) { s.BindingVersion = 2147483648 },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := scope
			change(&candidate)
			if _, err := Sign(key, candidate, "signed.original.grant", now, now.Add(time.Second)); err == nil {
				t.Fatal("invalid binding scope signed")
			}
		})
	}
}
