// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"go.yaml.in/yaml/v3"
)

func TestKeyAuthorityBindingPreservesLegacyEncoding(t *testing.T) {
	p := principalTestPolicy()
	const legacy = `{"revision":7,"principals":{"analyst":{"kind":"user","native_sources":["sales_native"],"federated_sources":["sales","daily_sales"]},"reports":{"kind":"service","federated_sources":["daily_sales"],"allow_literal_queries":true}}}`
	raw, err := json.Marshal(p.Access)
	if err != nil || string(raw) != legacy {
		t.Fatal("absent key binding changed legacy principal bytes", err)
	}
	sum := sha256.Sum256([]byte(`{"tenant":"a","access":` + legacy + `}`))
	if principalPolicyVersion(p) != hex.EncodeToString(sum[:]) {
		t.Fatal("absent key binding changed existing job authority")
	}
	for _, marshal := range []func(any) ([]byte, error){json.Marshal, yaml.Marshal} {
		raw, err := marshal(p)
		if err != nil || bytes.Contains(raw, []byte("key_authority")) {
			t.Fatal("omitted key binding was serialized", err)
		}
	}
	cfg := GatewayConfig{Tenants: []TenantConfig{{Policy: p}}}
	if authority, err := bindGatewayKeyAuthority(cfg); authority != nil || err != nil {
		t.Fatal("absent authority binding changed legacy compilation", err)
	}
}

func TestKeyAuthorityBindingRejectsMalformedValues(t *testing.T) {
	valid := *gatewayAuthorityBindingFixture(t).Tenants[0].Policy.Access.KeyAuthority
	for name, mutate := range map[string]func(*KeyAuthorityBinding){
		"no version":       func(b *KeyAuthorityBinding) { b.Version = 0 },
		"document version": func(b *KeyAuthorityBinding) { b.Version = 2 },
		"no scope":         func(b *KeyAuthorityBinding) { b.Scope = "" },
		"scope case":       func(b *KeyAuthorityBinding) { b.Scope = "Fleet" },
		"scope path":       func(b *KeyAuthorityBinding) { b.Scope = "../fleet" },
		"long scope":       func(b *KeyAuthorityBinding) { b.Scope = strings.Repeat("a", 64) },
		"no digest":        func(b *KeyAuthorityBinding) { b.MembershipSHA256 = "" },
		"short digest":     func(b *KeyAuthorityBinding) { b.MembershipSHA256 = strings.Repeat("a", 63) },
		"long digest":      func(b *KeyAuthorityBinding) { b.MembershipSHA256 = strings.Repeat("a", 65) },
		"digest case":      func(b *KeyAuthorityBinding) { b.MembershipSHA256 = strings.Repeat("A", 64) },
		"digest encoding":  func(b *KeyAuthorityBinding) { b.MembershipSHA256 = strings.Repeat("g", 64) },
	} {
		t.Run(name, func(t *testing.T) {
			binding := valid
			mutate(&binding)
			p := principalTestPolicy()
			p.Access.KeyAuthority = &binding
			if validKeyAuthorityBinding(&binding) || ValidatePolicy(p) == nil {
				t.Fatal("malformed key authority policy accepted")
			}
		})
	}
	if !validKeyAuthorityBinding(nil) || !validKeyAuthorityBinding(&valid) {
		t.Fatal("valid key authority binding rejected")
	}
}

func TestKeyAuthorityBindingChangesDurableAndJobAuthority(t *testing.T) {
	p := runtimeExportConfigFixture(t).Policy
	p.Access = gatewayAuthorityBindingFixture(t).Tenants[0].Policy.Access
	authority, ok := authorityForPrincipal(p, "reports")
	if !ok {
		t.Fatal("missing fixture authority")
	}
	request := query.Request{Mode: "federated", SQL: "SELECT 1"}
	export := ExportAuthority{Principal: authority, AuthorizationVersion: p.Exports.AuthorizationVersion}
	identity, err := exportIdentity(p, export)
	if err != nil || validateJobAuthority(p, &authority, request) != nil || validateExportAuthority(p, export) != nil {
		t.Fatal("invalid original binding fixture", err)
	}
	raw := policyBytes(t, p)
	var restored Policy
	if err := json.Unmarshal(raw, &restored); err != nil || principalPolicyVersion(restored) != principalPolicyVersion(p) {
		t.Fatal("policy round trip lost authority binding", err)
	}
	for name, mutate := range map[string]func(*Policy){
		"unchanged": func(p *Policy) {},
		"removed":   func(p *Policy) { p.Access.KeyAuthority = nil },
		"scope":     func(p *Policy) { p.Access.KeyAuthority.Scope = "other-fleet" },
		"members":   func(p *Policy) { p.Access.KeyAuthority.MembershipSHA256 = strings.Repeat("f", 64) },
	} {
		t.Run(name, func(t *testing.T) {
			next, err := clonePolicy(restored)
			if err != nil {
				t.Fatal(err)
			}
			mutate(&next)
			changed := name != "unchanged"
			if ValidatePolicy(next) != nil {
				t.Fatal("fixture binding shape invalid")
			}
			fixture, _ := newMetadataFixture(t)
			fixture.value = raw
			if _, err := openClusterMetadata(fixture.ctx, fixture, next, false); (err != nil) != changed {
				t.Fatal("durable metadata failed to fence binding cutover", err)
			}
			if (validateJobAuthority(next, &authority, request) != nil) != changed || (validateExportAuthority(next, export) != nil) != changed {
				t.Fatal("job or export authority omitted the key binding")
			}
			current, ok := authorityForPrincipal(next, "reports")
			if !ok || (current != authority) != changed {
				t.Fatal("principal policy version omitted the key binding")
			}
			nextIdentity, err := exportIdentity(next, ExportAuthority{Principal: current, AuthorizationVersion: next.Exports.AuthorizationVersion})
			if err != nil || (nextIdentity != identity) != changed {
				t.Fatal("export identity omitted the key binding", err)
			}
		})
	}
}

func TestKeyAuthorityPolicySnapshotsAreDetached(t *testing.T) {
	p := gatewayAuthorityBindingFixture(t).Tenants[0].Policy
	want := policyBytes(t, p)
	owned, err := clonePolicy(p)
	if err != nil || owned.Access.KeyAuthority == p.Access.KeyAuthority {
		t.Fatal("policy clone retained caller binding", err)
	}
	p.Access.KeyAuthority.Scope = "changed"
	if !bytes.Equal(policyBytes(t, owned), want) {
		t.Fatal("caller mutation changed retained authority")
	}
	for name, store := range map[string]interface{ Policy() Policy }{
		"query": &NATSStore{policy: owned}, "export": &natsExportStore{policy: owned},
	} {
		t.Run(name, func(t *testing.T) {
			var pending sync.WaitGroup
			for range 4 {
				pending.Go(func() {
					for range 8 {
						copy := store.Policy()
						copy.Access.KeyAuthority.Scope = "changed"
						copy.Access.KeyAuthority.MembershipSHA256 = strings.Repeat("b", 64)
						raw, err := json.Marshal(store.Policy())
						if err != nil || !bytes.Equal(raw, want) {
							t.Error("snapshot mutation changed retained authority", err)
							return
						}
					}
				})
			}
			pending.Wait()
		})
	}
}
