// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"go.yaml.in/yaml/v3"
)

func approveCatalog(t *testing.T, p *Policy, c catalog.Config) {
	t.Helper()
	digest, err := catalog.AuthorityFingerprint(c)
	if err != nil {
		t.Fatal(err)
	}
	p.Access.CatalogBinding = &CatalogBinding{Version: 1, SHA256: digest}
}

func TestCatalogBindingPreservesLegacyPolicyBytes(t *testing.T) {
	p := principalTestPolicy()
	const legacy = `{"revision":7,"principals":{"analyst":{"kind":"user","native_sources":["sales_native"],"federated_sources":["sales","daily_sales"]},"reports":{"kind":"service","federated_sources":["daily_sales"],"allow_literal_queries":true}}}`
	raw, err := json.Marshal(p.Access)
	if err != nil || string(raw) != legacy {
		t.Fatal("legacy access encoding changed", err)
	}
	sum := sha256.Sum256([]byte(`{"tenant":"a","access":` + legacy + `}`))
	if principalPolicyVersion(p) != hex.EncodeToString(sum[:]) {
		t.Fatal("legacy principal hash changed")
	}
	for _, encode := range []func(any) ([]byte, error){json.Marshal, yaml.Marshal} {
		raw, err = encode(p)
		if err != nil || strings.Contains(string(raw), "catalog_binding") {
			t.Fatal("absent binding changed policy document", err)
		}
	}
	// Unbound callers retain runtime-resolved configuration compatibility.
	if err := ValidateCatalogAuthority(p, catalog.Config{Sources: []catalog.Source{{ID: "resolved", Type: "accelerated"}}}); err != nil {
		t.Fatal(err)
	}
}

func TestCatalogBindingRejectsMalformedPins(t *testing.T) {
	for _, binding := range []CatalogBinding{
		{}, {Version: 2, SHA256: strings.Repeat("a", 64)},
		{Version: 1, SHA256: strings.Repeat("A", 64)},
		{Version: 1, SHA256: strings.Repeat("g", 64)},
		{Version: 1, SHA256: strings.Repeat("a", 63)},
		{Version: 1, SHA256: strings.Repeat("a", 65)},
	} {
		p := principalTestPolicy()
		p.Access.CatalogBinding = &binding
		if ValidatePolicy(p) == nil || ValidateCatalogAuthority(p, catalog.Config{}) == nil {
			t.Fatal("malformed catalog binding accepted")
		}
	}
	p := principalTestPolicy()
	approveCatalog(t, &p, catalog.Config{})
	if err := ValidatePolicy(p); err != nil {
		t.Fatal(err)
	}
	if err := ValidateCatalogAuthority(p, catalog.Config{}); err != nil {
		t.Fatal(err)
	}
}

func TestCatalogBindingInvalidatesJobAndExportAuthority(t *testing.T) {
	for _, tc := range []struct {
		name, principal string
		request         query.Request
	}{
		{"whole source", "analyst", query.Request{Mode: "federated", Sources: []string{"sales"}, SQL: "SELECT * FROM sales.orders"}},
		{"native", "analyst", query.Request{Mode: "native", ConnectionID: "sales_native", SQL: "SELECT 1"}},
		{"literal", "reports", query.Request{Mode: "federated", SQL: "SELECT 1"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := runtimeExportConfigFixture(t).Policy
			a, ok := authorityForPrincipal(p, tc.principal)
			if !ok || validateJobAuthority(p, &a, tc.request) != nil {
				t.Fatal("invalid grant fixture")
			}
			oldExport := ExportAuthority{Principal: a, AuthorizationVersion: p.Exports.AuthorizationVersion}
			oldIdentity, err := exportIdentity(p, oldExport)
			if err != nil {
				t.Fatal(err)
			}
			for _, c := range []catalog.Config{{}, {Sources: []catalog.Source{{ID: "sales", Type: "postgres", DSNEnv: "KELVO_SOURCE_SALES_DSN"}}}} {
				approveCatalog(t, &p, c)
				if validateJobAuthority(p, &a, tc.request) == nil || validateExportAuthority(p, oldExport) == nil {
					t.Fatal("old authority survived catalog cutover")
				}
				current, ok := authorityForPrincipal(p, tc.principal)
				if !ok || current == a || validateJobAuthority(p, &current, tc.request) != nil {
					t.Fatal("bound grant is unusable")
				}
				nextExport := ExportAuthority{Principal: current, AuthorizationVersion: p.Exports.AuthorizationVersion}
				identity, err := exportIdentity(p, nextExport)
				if err != nil || identity == oldIdentity {
					t.Fatal("export identity omitted catalog binding", err)
				}
				a, oldExport, oldIdentity = current, nextExport, identity
			}
		})
	}
}

func TestCatalogBindingDurableMetadataRejectsDifferentCatalog(t *testing.T) {
	p := principalTestPolicy()
	approveCatalog(t, &p, catalog.Config{})
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	var restored Policy
	if err := json.Unmarshal(raw, &restored); err != nil {
		t.Fatal(err)
	}
	if principalPolicyVersion(restored) != principalPolicyVersion(p) {
		t.Fatal("durable round trip lost authority")
	}
	for _, changed := range []bool{false, true} {
		fixture, _ := newMetadataFixture(t)
		fixture.value = raw
		next := restored
		if changed {
			next.Access = &PrincipalPolicy{Revision: p.Access.Revision, Principals: p.Access.Principals, CatalogBinding: &CatalogBinding{Version: 1, SHA256: strings.Repeat("f", 64)}}
		}
		_, err := openClusterMetadata(fixture.ctx, fixture, next, false)
		if (err != nil) != changed {
			t.Fatal("durable policy catalog comparison changed", err)
		}
	}
}

func TestCatalogAuthorityMismatchIsRedacted(t *testing.T) {
	p := principalTestPolicy()
	approveCatalog(t, &p, catalog.Config{})
	c := catalog.Config{Sources: []catalog.Source{{ID: "private_source_marker", Type: "postgres", DSNEnv: "KELVO_SOURCE_PRIVATE_DSN"}}}
	err := ValidateCatalogAuthority(p, c)
	if err != errCatalogAuthority || strings.Contains(err.Error(), "PRIVATE") || strings.Contains(err.Error(), "private_source") {
		t.Fatal("catalog mismatch leaked definitions")
	}
	if _, err := submissionAuthority(context.Background(), p, query.Request{Mode: "federated", SQL: "SELECT 1"}); err == nil {
		t.Fatal("binding removed principal requirement")
	}
}
