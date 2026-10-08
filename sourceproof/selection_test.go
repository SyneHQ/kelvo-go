// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package sourceproof

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestSelectionLearnsOnlySignedTokenAndVersion(t *testing.T) {
	key, scope, now := fixture(t)
	envelope, err := Sign(key, scope, "signed.original.grant", now, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	expected := scope
	expected.TokenID = ""
	expected.BindingVersion = 0
	selected, err := VerifySelection(key.Public().(ed25519.PublicKey), envelope, expected, now)
	if err != nil || selected.Scope() != scope {
		t.Fatal("signed selection did not preserve scope", err)
	}
	for _, change := range []func(*Scope){func(s *Scope) { s.TokenID = "22222222-2222-3333-4444-555555555555" }, func(s *Scope) { s.BindingVersion++ }} {
		rebound := scope
		change(&rebound)
		current, err := Sign(key, rebound, "signed.original.grant", now, now.Add(time.Second))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := Verify(key.Public().(ed25519.PublicKey), current, selected.Scope(), now); err == nil {
			t.Fatal("reopen accepted post-admission rebind")
		}
	}
	for name, change := range map[string]func(*Scope){
		"route": func(s *Scope) { s.RouteID = "other" }, "authority": func(s *Scope) { s.Authority = "other.private:5432" }, "source": func(s *Scope) { s.Source = "other" }, "revision": func(s *Scope) { s.SourceRevision = strings.Repeat("f", 64) }, "tenant": func(s *Scope) { s.Tenant = "other" }, "worker": func(s *Scope) { s.Worker = "other" }, "execution": func(s *Scope) { s.ExecutionID = "other" }, "caller": func(s *Scope) { s.WorkerCertSHA256 = strings.Repeat("f", 64) }, "preset_token": func(s *Scope) { s.TokenID = scope.TokenID }, "preset_version": func(s *Scope) { s.BindingVersion = 1 },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := expected
			change(&candidate)
			got, err := VerifySelection(key.Public().(ed25519.PublicKey), envelope, candidate, now)
			if err == nil || got.Scope() != (Scope{}) {
				t.Fatal("invalid independent scope returned selection")
			}
		})
	}
	wrong, _, _ := fixture(t)
	if _, err := VerifySelection(wrong.Public().(ed25519.PublicKey), envelope, expected, now); err == nil {
		t.Fatal("untrusted key accepted")
	}
	if _, err := VerifySelection(key.Public().(ed25519.PublicKey), envelope, expected, now.Add(time.Second)); err == nil {
		t.Fatal("expired selection accepted")
	}
}

func TestSelectionRejectsMalformedSignedFieldsAndTampering(t *testing.T) {
	key, scope, now := fixture(t)
	expected := scope
	expected.TokenID = ""
	expected.BindingVersion = 0
	for _, mode := range []string{"missing-token", "malformed-token", "missing-version", "negative-version", "changed-token-signature", "changed-version-signature"} {
		t.Run(mode, func(t *testing.T) {
			claims := Claims{Version: Version, Scope: scope, IssuedAt: now.Unix(), ExpiresAt: now.Add(time.Second).Unix()}
			switch mode {
			case "missing-token":
				claims.Scope.TokenID = ""
			case "malformed-token":
				claims.Scope.TokenID = "../route"
			case "missing-version":
				claims.Scope.BindingVersion = 0
			case "negative-version":
				claims.Scope.BindingVersion = -1
			}
			raw, _ := json.Marshal(claims)
			message := contextLabel + base64.RawURLEncoding.EncodeToString(raw)
			signature := ed25519.Sign(key, []byte(message))
			if mode == "changed-token-signature" || mode == "changed-version-signature" {
				if mode == "changed-token-signature" {
					claims.Scope.TokenID = "22222222-2222-3333-4444-555555555555"
				} else {
					claims.Scope.BindingVersion++
				}
				raw, _ = json.Marshal(claims)
				message = contextLabel + base64.RawURLEncoding.EncodeToString(raw)
			}
			envelope := Envelope{Grant: "signed.original.grant", Token: message + "." + base64.RawURLEncoding.EncodeToString(signature)}
			got, err := VerifySelection(key.Public().(ed25519.PublicKey), envelope, expected, now)
			if err == nil || got.Scope() != (Scope{}) {
				t.Fatal("malformed selection returned partial authority")
			}
		})
	}
}
