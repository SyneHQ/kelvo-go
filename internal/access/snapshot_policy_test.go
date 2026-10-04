// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package access

import (
	"context"
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

func guardedSnapshotPolicy() Policy {
	return Policy{Sources: map[string]SourcePolicy{"orders_fast": {Tables: map[string]TablePolicy{"orders_fast": {Columns: []string{"id"}, AllRows: true}}}}}
}
func guardedSnapshotSource() catalog.Source {
	return catalog.Source{ID: "orders_fast", Type: "parquet", Path: "/private/orders.parquet", LocalSnapshot: &catalog.LocalSnapshotRead{
		Dataset: "orders_fast", Generation: strings.Repeat("a", 32), SchemaSHA256: strings.Repeat("b", 64),
		Parts: []catalog.LocalSnapshotPart{{Rows: 5, Bytes: 1024, SHA256: strings.Repeat("c", 64)}},
		Scan:  catalog.SnapshotScanLimits{MaxRows: 1000, MaxBytes: 1 << 20},
	}}
}
func TestSnapshotPolicyParentAndResolvedAdmission(t *testing.T) {
	ctx := testContext(t, guardedSnapshotPolicy())
	request := query.Request{Mode: "federated", Sources: []string{"orders_fast"}, SQL: "SELECT count(*) FROM main.orders_fast"}
	parent := catalog.Config{Acceleration: &catalog.AccelerationConfig{Directory: "/private/snapshots", TenantID: "one", Datasets: []catalog.Dataset{{ID: "orders_fast"}}}}
	if err := ValidateRequest(ctx, parent, request); err != nil {
		t.Fatal("operator dataset rejected before resolution", err)
	}
	if err := ValidateResolvedRequest(ctx, parent, request); err == nil {
		t.Fatal("child accepted unresolved dataset")
	}
	resolved := catalog.Config{Sources: []catalog.Source{guardedSnapshotSource()}}
	if err := ValidateResolvedRequest(ctx, resolved, request); err != nil {
		t.Fatal("leased local generation rejected", err)
	}
	if !GuardedSnapshot(ctx, resolved.Sources[0]) {
		t.Fatal("guarded raw path not identified")
	}
	if GuardedSnapshot(context.Background(), resolved.Sources[0]) {
		t.Fatal("unrestricted snapshot changed paths")
	}
	resolved.Sources[0].LocalSnapshot.SchemaSHA256 = ""
	if err := ValidateResolvedRequest(ctx, resolved, request); err == nil {
		t.Fatal("guarded legacy schema-less generation accepted")
	}
	if err := ValidateResolvedRequest(context.Background(), resolved, request); err != nil {
		t.Fatal("unrestricted legacy snapshot rejected", err)
	}
}
func TestSnapshotPolicyFailsClosedForUntrustedAndUnsupportedSources(t *testing.T) {
	request := query.Request{Mode: "federated", Sources: []string{"orders_fast"}, SQL: "SELECT id FROM orders_fast"}
	for _, mode := range []string{"raw", "object", "credential", "wrong-dataset", "wrong-alias", "missing-policy", "extra-alias", "native", "diagnostics", "unselected-policy", "object-dataset", "fake-dataset"} {
		t.Run(mode, func(t *testing.T) {
			policy := guardedSnapshotPolicy()
			source := guardedSnapshotSource()
			config := catalog.Config{Sources: []catalog.Source{source}}
			req := request
			switch mode {
			case "raw":
				config.Sources[0].LocalSnapshot = nil
			case "object":
				config.Sources[0].Range = &catalog.ObjectRange{}
			case "credential":
				config.Sources[0].TokenEnv = "KELVO_SECRET"
			case "wrong-dataset":
				source.LocalSnapshot.Dataset = "other"
			case "wrong-alias":
				policy.Sources["orders_fast"] = SourcePolicy{Tables: map[string]TablePolicy{"orders": {Columns: []string{"id"}, AllRows: true}}}
			case "missing-policy":
				delete(policy.Sources, "orders_fast")
				policy.Sources["other"] = SourcePolicy{Tables: map[string]TablePolicy{"other": {Columns: []string{"id"}, AllRows: true}}}
			case "extra-alias":
				policy.Sources["orders_fast"].Tables["other"] = TablePolicy{Columns: []string{"id"}, AllRows: true}
			case "native":
				req.Mode = "native"
				req.ConnectionID = "orders_fast"
				req.Sources = nil
			case "diagnostics":
				req.ScanDiagnostics = true
			case "unselected-policy":
				req.Sources = nil
			case "object-dataset":
				config = catalog.Config{Acceleration: &catalog.AccelerationConfig{Directory: "/private", TenantID: "one", ObjectStorage: &catalog.ObjectStorage{}, Datasets: []catalog.Dataset{{ID: "orders_fast"}}}}
			case "fake-dataset":
				config.Sources[0] = catalog.Source{ID: "orders_fast", Type: "accelerated"}
			}
			ctx := testContext(t, policy)
			if err := ValidateRequest(ctx, config, req); err == nil {
				t.Fatal("unsupported parent path admitted")
			}
			if err := ValidateResolvedRequest(ctx, config, req); err == nil {
				t.Fatal("unsupported child path admitted")
			}
		})
	}
}
func TestSnapshotPolicyMultipartAndCallbackCombination(t *testing.T) {
	policy := guardedSnapshotPolicy()
	policy.Sources["warehouse"] = testPolicy().Sources["warehouse"]
	ctx := testContext(t, policy)
	source := guardedSnapshotSource()
	source.ParquetPaths = []string{source.Path, "/private/second.parquet"}
	source.Path = ""
	source.LocalSnapshot.Parts = append(source.LocalSnapshot.Parts, source.LocalSnapshot.Parts[0])
	callback := catalog.Source{ID: "warehouse", Type: "clickhouse", Federation: &catalog.FederationConfig{Tables: []catalog.FederationTable{{Name: "orders", Table: "orders", Database: "reports"}}}}
	config := catalog.Config{Sources: []catalog.Source{source, callback}}
	request := query.Request{Sources: []string{"orders_fast", "warehouse"}, SQL: "SELECT count(*) FROM orders_fast JOIN warehouse.orders USING(id)"}
	if err := ValidateResolvedRequest(ctx, config, request); err != nil {
		t.Fatal("guarded multipart/callback join rejected", err)
	}
	config.Sources = append(config.Sources, catalog.Source{ID: "raw", Type: "parquet", Path: "/private/second.parquet"})
	request.Sources = append(request.Sources, "raw")
	if err := ValidateResolvedRequest(ctx, config, request); err == nil {
		t.Fatal("raw alias bypassed snapshot grant")
	}
}
