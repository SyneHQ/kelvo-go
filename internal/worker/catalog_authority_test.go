// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

const catalogChildToken = "catalog-authority-test-child"

// This route uses the real worker subprocess/IPC boundary without a database.
// TestMain dispatches only this fixture's selected source token to it.
func catalogAuthorityChild() int {
	var in Input
	if json.NewDecoder(os.Stdin).Decode(&in) != nil || in.Request.Mode != "native" || in.Request.ConnectionID != "selected" ||
		in.Request.SQL != "SELECT catalog_authority" || len(in.Config.Sources) != 1 || in.Config.ExtensionDirectory != "/approved/extensions" {
		return 2
	}
	s := in.Config.Sources[0]
	if s.ID != "selected" || s.Type != "databricks" || s.URLEnv != "KELVO_SOURCE_CATALOG_URL" || s.TokenEnv != "KELVO_SOURCE_CATALOG_TOKEN" ||
		s.Options["warehouse_id"] != "approved" || s.Federation == nil || s.Federation.MaxScanRows != 100 || s.Federation.MaxScanBytes != 4096 ||
		!reflect.DeepEqual(s.Federation.Tables, []catalog.FederationTable{{Name: "orders", Database: "reports", Schema: "public", Table: "approved_orders"}}) ||
		os.Getenv("KELVO_SOURCE_CATALOG_URL") != "https://approved.example.invalid" {
		return 3
	}
	if _, present := os.LookupEnv("KELVO_SOURCE_UNSELECTED_TOKEN"); present {
		return 4
	}
	schema := arrow.NewSchema([]arrow.Field{{Name: "approved", Type: arrow.PrimitiveTypes.Int64}}, nil)
	builder := array.NewInt64Builder(memory.DefaultAllocator)
	builder.Append(7)
	column := builder.NewArray()
	builder.Release()
	record := array.NewRecordBatch(schema, []arrow.Array{column}, 1)
	column.Release()
	defer record.Release()
	sink := NewIPCSink(os.Stdout, in.Limits)
	defer sink.Abort()
	if sink.Schema(schema) != nil || sink.Write(record) != nil || sink.Finish() != nil {
		return 5
	}
	if json.NewEncoder(os.Stderr).Encode(Outcome{Stats: query.Stats{Backend: "catalog-authority-child"}}) != nil {
		return 6
	}
	return 0
}

func workerCatalogFixture() catalog.Config {
	return catalog.Config{ExtensionDirectory: "/approved/extensions", Sources: []catalog.Source{
		{ID: "selected", Type: "databricks", URLEnv: "KELVO_SOURCE_CATALOG_URL", TokenEnv: "KELVO_SOURCE_CATALOG_TOKEN",
			Options: map[string]string{"warehouse_id": "approved"}, Federation: &catalog.FederationConfig{MaxScanRows: 100, MaxScanBytes: 4096,
				Tables: []catalog.FederationTable{{Name: "orders", Database: "reports", Schema: "public", Table: "approved_orders"}}}},
		{ID: "unselected", Type: "databricks", URLEnv: "KELVO_SOURCE_UNSELECTED_URL", TokenEnv: "KELVO_SOURCE_UNSELECTED_TOKEN"},
	}}
}

func catalogDigest(t *testing.T, c catalog.Config) string {
	t.Helper()
	digest, err := catalog.AuthorityFingerprint(c)
	if err != nil {
		t.Fatal(err)
	}
	return digest
}

func TestCatalogBindingUsesDetachedDefinitionsThroughRealChildAndQuota(t *testing.T) {
	config := workerCatalogFixture()
	digest := catalogDigest(t, config)
	original, err := New(config, query.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	quota := &sourceAdmissionFixture{}
	original.SourceAdmission = quota
	lookups := []string{}
	original.Secrets = secretResolverFunc(func(_ context.Context, name string) (string, bool, error) {
		lookups = append(lookups, name)
		switch name {
		case "KELVO_SOURCE_CATALOG_URL":
			return "https://approved.example.invalid", true, nil
		case "KELVO_SOURCE_CATALOG_TOKEN":
			return catalogChildToken, true, nil
		default:
			t.Error("unapproved source credential lookup")
			return "", false, nil
		}
	})
	bound, err := original.WithCatalogBinding(digest)
	if err != nil || original.catalogBinding != nil || bound == original {
		t.Fatal("binding mutated its caller", err)
	}
	// A shallow executor copy is how the export runtime changes reservations.
	exportCopy := *bound
	config.Sources[0].Options["warehouse_id"] = "caller-retargeted"
	config.Sources[0].Federation.Tables[0].Table = "caller-retargeted"
	bound.Config.Sources[0].Options["warehouse_id"] = "public-retargeted"
	bound.Config.Sources[0].Federation.Tables[0].Table = "public-retargeted"
	bound.Config.Sources[0].Federation.MaxScanRows = 999
	bound.Config.ExtensionDirectory = "/public-retargeted/extensions"
	exportCopy.Config.Sources[0].TokenEnv = "KELVO_SOURCE_UNSELECTED_TOKEN"
	bound.Config.Sources = nil
	ctx, err := WithCatalogAuthority(context.Background(), digest)
	if err != nil {
		t.Fatal(err)
	}
	request := query.Request{Mode: "native", ConnectionID: "selected", SQL: "SELECT catalog_authority"}
	for _, executor := range []*Executor{bound, &exportCopy} {
		sink := &workerTestSink{write: func(batch arrow.RecordBatch) error {
			values, ok := batch.Column(0).(*array.Int64)
			if !quota.active || batch.NumRows() != 1 || !ok || values.Value(0) != 7 {
				t.Error("approved child value or active source custody changed")
			}
			return nil
		}}
		stats, err := executor.Execute(ctx, request, sink)
		if err != nil || stats.Rows != 1 || stats.Backend != "catalog-authority-child" || !reflect.DeepEqual(quota.ids, []string{"selected"}) || quota.active {
			t.Fatalf("approved catalog did not reach quota and real child: %v %+v", err, stats)
		}
	}
	if quota.calls != 2 || !reflect.DeepEqual(lookups, []string{"KELVO_SOURCE_CATALOG_URL", "KELVO_SOURCE_CATALOG_TOKEN", "KELVO_SOURCE_CATALOG_URL", "KELVO_SOURCE_CATALOG_TOKEN"}) {
		t.Fatal("source work used mutated or unselected definitions")
	}
	// A public view cannot remint a bound executor under a different authority.
	changed := catalogDigest(t, bound.Config)
	if _, err := bound.WithCatalogBinding(changed); err == nil {
		t.Fatal("public mutation changed effective binding")
	}
	rebound, err := bound.WithCatalogBinding(digest)
	if err != nil || catalogDigest(t, rebound.Config) != digest || bound.Config.Sources != nil {
		t.Fatal("rebinding did not use the private effective catalog", err)
	}
}

func TestCatalogBindingKeepsDatasetSelectionAfterNestedMutation(t *testing.T) {
	// A regular file makes the approved snapshot root deterministically
	// unavailable without needing a database, engine build, or published data.
	root := filepath.Join(t.TempDir(), "approved-root")
	if err := os.WriteFile(root, []byte("not a directory"), 0600); err != nil {
		t.Fatal(err)
	}
	config := catalog.Config{Sources: []catalog.Source{{ID: "upstream", Type: "databricks", TokenEnv: "KELVO_SOURCE_CATALOG_TOKEN"}},
		Acceleration: &catalog.AccelerationConfig{Directory: root, TenantID: "tenant-a", Datasets: []catalog.Dataset{{
			ID: "cached", Query: query.Request{Mode: "federated", Sources: []string{"upstream"}, SQL: "SELECT 1"},
			AuthorizationVersion: "v1", MaxAge: time.Hour, Limits: query.DefaultLimits(),
			Scan:            &catalog.SnapshotScanLimits{MaxRows: 10, MaxBytes: 4096},
			SchemaEvolution: &catalog.SchemaEvolution{AddNullableColumns: true},
			Multipart:       &catalog.MultipartConfig{MaxParts: 2, MaxPartBytes: 1 << 20},
		}}}}
	digest := catalogDigest(t, config)
	quota := &sourceAdmissionFixture{}
	original := &Executor{Config: config, Limits: query.DefaultLimits(), Binary: "/must-not-start", SourceAdmission: quota,
		Secrets: secretResolverFunc(func(context.Context, string) (string, bool, error) {
			t.Fatal("snapshot selection attempted source credential resolution")
			return "", false, nil
		})}
	bound, err := original.WithCatalogBinding(digest)
	if err != nil {
		t.Fatal(err)
	}
	config.Acceleration.Datasets[0].ID = "caller_retargeted"
	config.Acceleration.Datasets[0].Query.Sources[0] = "caller_retargeted"
	config.Acceleration.Datasets[0].Scan.MaxRows = 999
	config.Acceleration.Datasets[0].SchemaEvolution.SafeWidening = true
	config.Acceleration.Datasets[0].Multipart.MaxParts = 9
	bound.Config.Acceleration.Datasets[0].ID = "public_retargeted"
	bound.Config.Acceleration.Directory = t.TempDir()
	bound.Config.Acceleration.Datasets[0].Query.Sources[0] = "public_retargeted"
	bound.Config.Sources[0].ID = "cached"
	ctx, err := WithCatalogAuthority(context.Background(), digest)
	if err != nil {
		t.Fatal(err)
	}
	_, err = bound.Execute(ctx, query.Request{Mode: "federated", Sources: []string{"cached"}, SQL: "SELECT * FROM cached"}, &workerTestSink{})
	if err == nil || query.PublicError(err).Code != "DATASET_UNAVAILABLE" || query.PublicError(err).Message != "Accelerated dataset storage is unavailable" ||
		quota.calls != 1 || len(quota.ids) != 0 || quota.active {
		t.Fatalf("dataset selection used caller/public definitions: %v", err)
	}
	if _, err := bound.WithCatalogBinding(digest); err != nil {
		t.Fatal("nested dataset mutation changed effective authority", err)
	}
}

func TestCatalogAuthorityMismatchRejectsBeforeAnySourceWork(t *testing.T) {
	config := workerCatalogFixture()
	digest := catalogDigest(t, config)
	original := &Executor{Config: config, Limits: query.DefaultLimits(), Binary: "/must-not-start"}
	bound, err := original.WithCatalogBinding(digest)
	if err != nil {
		t.Fatal(err)
	}
	changed := workerCatalogFixture()
	changed.Sources[0].Options["warehouse_id"] = "other"
	otherDigest := catalogDigest(t, changed)
	cases := []struct {
		name     string
		executor *Executor
		expected string
	}{{"new authority old worker", bound, otherDigest}, {"removed pin old worker", bound, ""}, {"bound authority unbound worker", original, digest}}
	for _, tc := range cases {
		for _, request := range []query.Request{{Mode: "native", ConnectionID: "selected", SQL: "SELECT 1"}, {Mode: "federated", Sources: []string{"selected"}, SQL: "SELECT 1"}, {Mode: "federated", SQL: "SELECT 1"}} {
			t.Run(tc.name+"/"+request.Mode+"/"+request.ConnectionID, func(t *testing.T) {
				quota := &sourceAdmissionFixture{}
				tc.executor.SourceAdmission = quota
				tc.executor.Secrets = secretResolverFunc(func(context.Context, string) (string, bool, error) {
					t.Fatal("mismatched catalog reached credentials")
					return "", false, nil
				})
				ctx, err := WithCatalogAuthority(context.Background(), tc.expected)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := tc.executor.Execute(ctx, request, &workerTestSink{}); err == nil || query.PublicError(err).Code != "PERMISSION_DENIED" || quota.calls != 0 {
					t.Fatalf("catalog mismatch reached preparation: %v", err)
				}
			})
		}
	}
}

func TestCatalogBindingRejectsMalformedPinsAndRuntimeDefinitions(t *testing.T) {
	original := &Executor{Config: workerCatalogFixture(), Binary: "/must-not-start"}
	for _, digest := range []string{"", "abc", strings.Repeat("A", 64), strings.Repeat("g", 64), strings.Repeat("0", 64)} {
		if result, err := original.WithCatalogBinding(digest); err == nil || result != nil || original.catalogBinding != nil {
			t.Fatal("invalid or mismatched binding changed the executor")
		}
	}
	for _, digest := range []string{"abc", strings.Repeat("A", 64), strings.Repeat("g", 64)} {
		if _, err := WithCatalogAuthority(context.Background(), digest); err == nil {
			t.Fatal("malformed context authority accepted")
		}
	}
	if _, err := WithCatalogAuthority(nil, ""); err == nil {
		t.Fatal("nil authority context accepted")
	}
	var absent *Executor
	if _, err := absent.WithCatalogBinding(strings.Repeat("0", 64)); err == nil {
		t.Fatal("nil executor accepted")
	}
	config := catalog.Config{Sources: []catalog.Source{{ID: "snapshot", Type: "parquet", LocalSnapshot: &catalog.LocalSnapshotRead{Dataset: "snapshot"}}}}
	legacy, err := New(config, query.DefaultLimits())
	if err != nil {
		t.Fatal("legacy constructor now rejects trusted runtime envelopes", err)
	}
	if _, err := legacy.WithCatalogBinding(strings.Repeat("0", 64)); err == nil {
		t.Fatal("runtime capability accepted as an operator definition")
	}
}

func TestCatalogAuthorityDistinguishesOperatorFromLegacyPrincipal(t *testing.T) {
	original, err := New(catalog.Config{}, query.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	digest := catalogDigest(t, catalog.Config{})
	bound, err := original.WithCatalogBinding(digest)
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := WithCatalogAuthority(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	authorized, err := WithCatalogAuthority(context.Background(), digest)
	if err != nil {
		t.Fatal(err)
	}
	request := query.Request{Mode: "federated", SQL: "SELECT 1"}
	for _, tc := range []struct {
		executor *Executor
		ctx      context.Context
	}{{original, context.Background()}, {original, legacy}, {bound, context.Background()}, {bound, authorized}} {
		if stats, err := tc.executor.Execute(tc.ctx, request, &workerTestSink{}); err != nil || stats.Rows != 3 {
			t.Fatalf("valid operator or principal execution changed: %v", err)
		}
	}
}
