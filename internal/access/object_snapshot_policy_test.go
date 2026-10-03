// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package access

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

func guardedObjectSnapshotSource(parts int) catalog.Source {
	source := guardedSnapshotSource()
	local := source.LocalSnapshot
	source.LocalSnapshot, source.Path = nil, ""
	source.ObjectSnapshot = &catalog.ObjectSnapshotRead{Dataset: local.Dataset, Generation: local.Generation,
		SchemaSHA256: local.SchemaSHA256, Scan: local.Scan}
	for index := 0; index < parts; index++ {
		target := "http://127.0.0.1:12345/" + strings.Repeat("e", 64) + "/orders_fast"
		if parts > 1 {
			target += fmt.Sprintf("/part-%04d", index)
		}
		part := local.Parts[0]
		source.ObjectSnapshot.Parts = append(source.ObjectSnapshot.Parts, catalog.ObjectSnapshotPart{
			URL: target, Rows: part.Rows, Bytes: part.Bytes, SHA256: part.SHA256,
		})
		capability := catalog.ObjectRange{URL: target, Bytes: part.Bytes}
		if parts == 1 {
			source.Path, source.Range = target, &capability
		} else {
			source.Ranges = append(source.Ranges, capability)
		}
	}
	return source
}

func TestObjectSnapshotPolicyParentAndResolvedAdmission(t *testing.T) {
	ctx := testContext(t, guardedSnapshotPolicy())
	request := query.Request{Mode: "federated", Sources: []string{"orders_fast"}, SQL: "SELECT id FROM orders_fast"}
	storage := catalog.ObjectStorage{
		ObjectLocation:   catalog.ObjectLocation{Provider: "s3", Endpoint: "https://objects.example.test", Bucket: "snapshots", Prefix: "cache", Region: "us-east-1"},
		ReadCredentials:  catalog.ObjectCredentials{AccessKeyIDEnv: "KELVO_SOURCE_READER_ID", SecretAccessKeyEnv: "KELVO_SOURCE_READER_SECRET"},
		WriteCredentials: catalog.ObjectCredentials{AccessKeyIDEnv: "KELVO_SOURCE_WRITER_ID", SecretAccessKeyEnv: "KELVO_SOURCE_WRITER_SECRET"},
	}
	parent := catalog.Config{Acceleration: &catalog.AccelerationConfig{TenantID: "one", ObjectStorage: &storage, Datasets: []catalog.Dataset{{ID: "orders_fast"}}}}
	if err := ValidateRequest(ctx, parent, request); err != nil {
		t.Fatal("configured object dataset rejected before resolution", err)
	}
	if err := ValidateResolvedRequest(ctx, parent, request); err == nil {
		t.Fatal("child accepted unresolved object dataset")
	}
	for _, count := range []int{1, 2} {
		source := guardedObjectSnapshotSource(count)
		config := catalog.Config{Sources: []catalog.Source{source}}
		if err := ValidateRequest(ctx, config, request); err != nil {
			t.Fatal("parent rejected resolved object generation", err)
		}
		if err := ValidateResolvedRequest(ctx, config, request); err != nil {
			t.Fatal("child rejected leased object generation", err)
		}
		if !GuardedSnapshot(ctx, source) || GuardedSnapshot(context.Background(), source) {
			t.Fatal("guarded object capabilities not isolated from unrestricted paths")
		}
	}
}

func TestObjectSnapshotPolicyRejectsBypassAndMissingProvenance(t *testing.T) {
	for _, mode := range []string{"raw", "schema-less", "missing-policy", "wrong-alias", "extra-alias", "wrong-dataset", "native", "diagnostics", "changed-url", "changed-size", "mixed-local", "credentials"} {
		t.Run(mode, func(t *testing.T) {
			source := guardedObjectSnapshotSource(1)
			policy := guardedSnapshotPolicy()
			request := query.Request{Mode: "federated", Sources: []string{"orders_fast"}, SQL: "SELECT id FROM orders_fast"}
			switch mode {
			case "raw":
				source.ObjectSnapshot = nil
			case "schema-less":
				source.ObjectSnapshot.SchemaSHA256 = ""
			case "missing-policy":
				delete(policy.Sources, "orders_fast")
				policy.Sources["other"] = SourcePolicy{Tables: map[string]TablePolicy{"other": {Columns: []string{"id"}, AllRows: true}}}
			case "wrong-alias":
				policy.Sources["orders_fast"] = SourcePolicy{Tables: map[string]TablePolicy{"orders": {Columns: []string{"id"}, AllRows: true}}}
			case "extra-alias":
				policy.Sources["orders_fast"].Tables["other"] = TablePolicy{Columns: []string{"id"}, AllRows: true}
			case "wrong-dataset":
				source.ObjectSnapshot.Dataset = "other"
			case "native":
				request.Mode, request.ConnectionID, request.Sources = "native", source.ID, nil
			case "diagnostics":
				request.ScanDiagnostics = true
			case "changed-url":
				source.Range.URL = strings.Replace(source.Range.URL, strings.Repeat("e", 64), strings.Repeat("f", 64), 1)
				source.Path = source.Range.URL
			case "changed-size":
				source.Range.Bytes++
			case "mixed-local":
				source.LocalSnapshot = guardedSnapshotSource().LocalSnapshot
			case "credentials":
				source.TokenEnv = "KELVO_SOURCE_TOKEN"
			}
			ctx := testContext(t, policy)
			config := catalog.Config{Sources: []catalog.Source{source}}
			if ValidateRequest(ctx, config, request) == nil || ValidateResolvedRequest(ctx, config, request) == nil {
				t.Fatal("guarded object boundary accepted bypass")
			}
			if source.ObjectSnapshot != nil && !GuardedSnapshot(ctx, source) {
				t.Fatal("invalid or ungranted descriptor fell back to an unguarded range scan")
			}
		})
	}
}

func TestObjectSnapshotPolicyMixedLocalCallbackAndRawAlias(t *testing.T) {
	policy := guardedSnapshotPolicy()
	policy.Sources["local_fast"] = SourcePolicy{Tables: map[string]TablePolicy{"local_fast": {Columns: []string{"id"}, AllRows: true}}}
	policy.Sources["warehouse"] = testPolicy().Sources["warehouse"]
	ctx := testContext(t, policy)
	local := guardedSnapshotSource()
	local.ID, local.LocalSnapshot.Dataset = "local_fast", "local_fast"
	object := guardedObjectSnapshotSource(2)
	callback := catalog.Source{ID: "warehouse", Type: "clickhouse", Federation: &catalog.FederationConfig{Tables: []catalog.FederationTable{{Name: "orders", Table: "orders", Database: "reports"}}}}
	config := catalog.Config{Sources: []catalog.Source{local, object, callback}}
	request := query.Request{Sources: []string{"local_fast", "orders_fast", "warehouse"}, SQL: "SELECT count(*) FROM orders_fast JOIN local_fast USING(id) JOIN warehouse.orders USING(id)"}
	if err := ValidateResolvedRequest(ctx, config, request); err != nil {
		t.Fatal("guarded object/local/callback join rejected", err)
	}
	config.Sources = append(config.Sources, catalog.Source{ID: "raw", Type: "parquet", Path: object.Ranges[0].URL, Range: &object.Ranges[0]})
	request.Sources = append(request.Sources, "raw")
	if err := ValidateResolvedRequest(ctx, config, request); err == nil {
		t.Fatal("unguarded range alias bypassed snapshot policy")
	}
}

func TestObjectSnapshotPolicyKeepsLegacyUnrestrictedReads(t *testing.T) {
	for _, mode := range []string{"schema-less", "legacy-range"} {
		source := guardedObjectSnapshotSource(1)
		if mode == "schema-less" {
			source.ObjectSnapshot.SchemaSHA256 = ""
		} else {
			source.ObjectSnapshot = nil
		}
		request := query.Request{Sources: []string{source.ID}, SQL: "SELECT count(*) FROM orders_fast"}
		if err := ValidateResolvedRequest(context.Background(), catalog.Config{Sources: []catalog.Source{source}}, request); err != nil {
			t.Fatal("unrestricted object behavior changed", mode, err)
		}
	}
}
