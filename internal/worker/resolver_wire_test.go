// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"encoding/json"
	"testing"

	"github.com/SYNEHQ/kelvo-go/delegation"
	"github.com/SYNEHQ/kelvo-go/resolver"
)

// Legacy runtime fixtures deliberately inject forbidden catalog capabilities.
// Keep that attack surface in tests only: production decoding uses the public
// schema, where those fields do not exist and cannot be silently discarded.
func (r connectionResolution) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]any{"version": r.Version, "delegation_sha256": r.DelegationSHA256, "valid_until": r.ValidUntil, "sources": r.Sources, "secrets": r.Secrets})
}

func (r operationResolution) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]any{"version": r.Version, "grant_sha256": r.GrantSHA256, "request_sha256": r.RequestSHA256, "source_revision": r.SourceRevision, "valid_until": r.ValidUntil, "source": r.Source, "secrets": r.Secrets})
}

func TestResolverWireRejectsRuntimeCapabilities(t *testing.T) {
	for _, field := range []string{`"path":"/private/data"`, `"adapter":"executable"`, `"local_snapshot":{}`, `"object_snapshot":{}`, `"object":{}`, `"object_range":{}`, `"object_ranges":[]`, `"parquet_paths":[]`} {
		var out resolver.QueryResponse
		raw := []byte(`{"version":1,"delegation_sha256":"x","valid_until":1,"sources":[{"id":"source_1","type":"postgres",` + field + `}],"secrets":{}}`)
		if delegation.StrictJSONLimit(raw, &out, resolver.MaxResponseBytes) == nil {
			t.Fatalf("accepted runtime capability %s", field)
		}
	}
}

func TestResolverSourceConversionDoesNotAliasWire(t *testing.T) {
	in := resolver.Source{ID: "source_1", Type: "postgres", Options: map[string]string{"key": "value"}, Federation: &resolver.Federation{Tables: []delegation.Table{{Name: "items", Table: "items", Schema: "public"}}}}
	out := resolvedSource(in)
	in.Options["key"] = "changed"
	in.Federation.Tables[0].Name = "changed"
	if out.Options["key"] != "value" || out.Federation.Tables[0].Name != "items" || out.Path != "" || out.Adapter != "" || out.LocalSnapshot != nil || out.ObjectSnapshot != nil || out.Object != nil || out.Range != nil {
		t.Fatal("wire conversion aliased or introduced runtime authority")
	}
}
