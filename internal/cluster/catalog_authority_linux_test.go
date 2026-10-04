//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/acceleration"
	"github.com/SYNEHQ/kelvo-go/internal/access"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/worker"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

type catalogClaimStore struct {
	*nodeTestStore
	claims int
}

func (s *catalogClaimStore) ClaimWorker(context.Context, string, string) error {
	s.claims++
	return nil
}

type catalogNoSecrets struct{ t *testing.T }

func (s catalogNoSecrets) Resolve(context.Context, string) (string, bool, error) {
	s.t.Fatal("catalog refusal accessed secrets")
	return "", false, nil
}

func TestCatalogBindingNodeMismatchPrecedesProbeAndClaim(t *testing.T) {
	p := principalTestPolicy()
	approveCatalog(t, &p, catalog.Config{})
	marker := filepath.Join(t.TempDir(), "probe-started")
	launcher := filepath.Join(t.TempDir(), "launcher")
	if err := os.WriteFile(launcher, []byte("#!/bin/sh\ntouch '"+marker+"'\nexit 1\n"), 0700); err != nil {
		t.Fatal(err)
	}
	executor := &worker.Executor{Config: catalog.Config{Sources: []catalog.Source{{ID: "sales", Type: "postgres", DSNEnv: "KELVO_SOURCE_SALES_DSN"}}}, Limits: p.Limits, SandboxPath: launcher, Binary: launcher, Secrets: catalogNoSecrets{t}}
	s := &catalogClaimStore{nodeTestStore: &nodeTestStore{p: p, jobs: map[string]Snapshot{}, queue: make(chan Delivery, 1)}}
	if n, err := NewNode(NodeConfig{Policy: p, WorkerID: "a1"}, s, executor); err != errCatalogAuthority {
		if n != nil {
			n.Close()
		}
		t.Fatal("mismatched node catalog was not rejected", err)
	}
	if s.claims != 0 {
		t.Fatal("mismatched worker claimed identity")
	}
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("startup child ran before catalog check", err)
	}
}

func catalogExportFixture(t *testing.T, c catalog.Config) (NodeConfig, *exportRuntimeStore, *worker.Executor) {
	t.Helper()
	cfg := runtimeExportConfigFixture(t)
	cfg.WorkerID = "a1"
	approveCatalog(t, &cfg.Policy, c)
	if err := os.Mkdir(cfg.ScratchDirectory, 0700); err != nil {
		t.Fatal(err)
	}
	scratch, err := worker.OpenScratchRoot(cfg.ScratchDirectory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := scratch.Close(); err != nil {
			t.Error(err)
		}
	})
	pool, err := cfg.Resources.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	e := &worker.Executor{Config: c, Limits: cfg.Policy.Limits, ResourcePool: pool, SandboxPath: cfg.SandboxPath, ScratchRoot: scratch, Secrets: catalogNoSecrets{t}}
	s := &exportRuntimeStore{p: cfg.Policy, jobs: map[string]ExportSnapshot{}, queue: make(chan Delivery, 16)}
	return cfg, s, e
}

func TestCatalogBindingExportMismatchPrecedesCustody(t *testing.T) {
	cfg, s, e := catalogExportFixture(t, catalog.Config{})
	e.Config.Sources = []catalog.Source{{ID: "sales", Type: "postgres", DSNEnv: "KELVO_SOURCE_SALES_DSN"}}
	if r, err := NewExportRuntime(cfg, s, e, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"); err != errCatalogAuthority {
		if r != nil {
			r.Close()
		}
		t.Fatal("mismatched export catalog accepted", err)
	}
	if _, err := os.Stat(cfg.Exports.Directory); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("mismatched catalog acquired export custody", err)
	}
}

func TestCatalogBindingConstructorsValidatePrivateSnapshot(t *testing.T) {
	for _, kind := range []string{"node tenant", "node overlap", "export overlap"} {
		t.Run(kind, func(t *testing.T) {
			cfg, s, e := catalogExportFixture(t, catalog.Config{})
			tenant := cfg.Policy.TenantID
			directory := cfg.Exports.Directory
			if kind == "node tenant" {
				tenant = "foreign"
				directory = filepath.Join(t.TempDir(), "snapshots")
			}
			c := catalog.Config{Acceleration: &catalog.AccelerationConfig{TenantID: tenant, Directory: directory, Datasets: []catalog.Dataset{{ID: "daily_sales", Query: query.Request{Mode: "federated", SQL: "SELECT 1"}}}}}
			e.Config = c
			approveCatalog(t, &cfg.Policy, c)
			s.p = cfg.Policy
			bound, err := e.WithCatalogBinding(cfg.Policy.Access.CatalogBinding.SHA256)
			if err != nil {
				t.Fatal(err)
			}
			// The public inspection copy must not hide the actual overlap/tenant.
			bound.Config.Acceleration = nil
			if kind == "export overlap" {
				r, err := NewExportRuntime(cfg, s, bound, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
				if r != nil {
					r.Close()
				}
				if err == nil || err.Error() != "export and acceleration directories must be disjoint" {
					t.Fatal("export skipped actual catalog custody check", err)
				}
			} else {
				claims := &catalogClaimStore{nodeTestStore: &nodeTestStore{p: cfg.Policy, jobs: map[string]Snapshot{}, queue: make(chan Delivery, 1)}}
				n, err := NewNode(cfg, claims, bound)
				if n != nil {
					n.Close()
				}
				want := "export and acceleration directories must be disjoint"
				if kind == "node tenant" {
					want = "acceleration tenant must match worker tenant"
				}
				if err == nil || err.Error() != want {
					t.Fatal("node skipped actual catalog validation", want, err)
				}
				if claims.claims != 0 {
					t.Fatal("unsafe catalog claimed worker")
				}
			}
			if _, err := os.Stat(cfg.Exports.Directory); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("unsafe catalog acquired export custody", err)
			}
		})
	}
}

func TestCatalogBindingExecutionRejectsChangedPolicyBeforeSourceWork(t *testing.T) {
	c := catalog.Config{Sources: []catalog.Source{{ID: "sales", Type: "postgres", DSNEnv: "KELVO_SOURCE_SALES_DSN"}}}
	for _, kind := range []string{"whole source", "native", "literal", "removed binding", "removed access"} {
		t.Run(kind, func(t *testing.T) {
			p := principalTestPolicy()
			approveCatalog(t, &p, c)
			e, err := (&worker.Executor{Config: c, Limits: p.Limits, Binary: "/must-not-start", Secrets: catalogNoSecrets{t}}).WithCatalogBinding(p.Access.CatalogBinding.SHA256)
			if err != nil {
				t.Fatal(err)
			}
			approveCatalog(t, &p, catalog.Config{})
			id := "analyst"
			request := query.Request{Mode: "federated", Sources: []string{"sales"}, SQL: "SELECT 1"}
			if kind == "native" {
				request = query.Request{Mode: "native", ConnectionID: "sales_native", SQL: "SELECT 1"}
			}
			if kind == "literal" {
				id = "reports"
				request = query.Request{Mode: "federated", SQL: "SELECT 1"}
			}
			if kind == "removed binding" {
				p.Access.CatalogBinding = nil
			}
			var a *JobAuthority
			if kind == "removed access" {
				p.Access = nil
			} else {
				v, _ := authorityForPrincipal(p, id)
				a = &v
			}
			ctx, err := executionAuthorityContext(context.Background(), p, a, request)
			if err != nil {
				t.Fatal(err)
			}
			_, err = e.Execute(ctx, request, discardSink{})
			if err == nil || query.PublicError(err).Code != "PERMISSION_DENIED" {
				t.Fatal("changed execution authority reached source work", err)
			}
		})
	}
}

func TestCatalogBindingActualNodeAndExportExecution(t *testing.T) {
	binary, sandbox := os.Getenv("KELVO_TEST_EXPORT_BINARY"), os.Getenv("KELVO_TEST_EXPORT_SANDBOX")
	if binary == "" || sandbox == "" {
		t.Skip("set actual export worker binary and sandbox for catalog execution acceptance")
	}
	sourcePath := filepath.Join(t.TempDir(), "approved.csv")
	if err := os.WriteFile(sourcePath, []byte("value\n7\n"), 0600); err != nil {
		t.Fatal(err)
	}
	changedPath := filepath.Join(t.TempDir(), "retargeted.csv")
	if err := os.WriteFile(changedPath, []byte("value\n99\n"), 0600); err != nil {
		t.Fatal(err)
	}
	c := catalog.Config{Sources: []catalog.Source{{ID: "daily_sales", Type: "csv", Path: sourcePath}}}
	cfg, s, e := catalogExportFixture(t, c)
	cfg.SandboxPath = sandbox
	e.SandboxPath = sandbox
	e.Binary = binary
	cfg.RuntimeExportStore = s
	claims := &catalogClaimStore{nodeTestStore: &nodeTestStore{p: cfg.Policy, jobs: map[string]Snapshot{}, queue: make(chan Delivery, 16)}}
	n, err := NewNode(cfg, claims, e)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	if claims.claims != 1 {
		t.Fatal("matching worker did not claim identity")
	}
	r := n.exports
	if r == nil {
		t.Fatal("matching worker has no export runtime")
	}
	// Corrupt every caller-visible config. Internal node and export execution
	// retain their own approved snapshot across the extra export executor copy.
	e.Config.Sources[0].Path = changedPath
	n.executor.(*worker.Executor).Config = e.Config
	r.executor.(*worker.Executor).Config = e.Config
	request := query.Request{Mode: "federated", Sources: []string{"daily_sales"}, SQL: "SELECT CAST(value AS INTEGER) FROM daily_sales"}
	authority, _ := authorityForPrincipal(cfg.Policy, "reports")
	ctx, err := executionAuthorityContext(context.Background(), cfg.Policy, &authority, request)
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	sink := worker.NewIPCSink(&output, cfg.Policy.Limits)
	defer sink.Abort()
	if stats, err := n.executor.Execute(ctx, request, sink); err != nil || stats.Rows != 1 {
		t.Fatal("bound node source execution failed", err)
	}
	if err := sink.Finish(); err != nil {
		t.Fatal(err)
	}
	direct, err := ipc.NewReader(bytes.NewReader(output.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	defer direct.Release()
	if !direct.Next() {
		t.Fatal("bound node returned no rows", direct.Err())
	}
	v, ok := direct.RecordBatch().Column(0).(*array.Int32)
	if !ok || v.Value(0) != 7 {
		t.Fatal("node used mutated public catalog")
	}
	if direct.Next() || direct.Err() != nil {
		t.Fatal("bound node returned incomplete stream", direct.Err())
	}
	id := addRuntimeExportRequest(t, s, request)
	claimed := claimRuntimeExport(t, s, id)
	result, err := r.RunExport(context.Background(), claimed)
	if err != nil || result.Stats.Rows != 1 {
		t.Fatal("bound export execution failed", err)
	}
	ready := readyRuntimeExport(t, s, id)
	part, err := r.OpenPart(context.Background(), ready, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer part.Close()
	stream, err := ipc.NewReader(part)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Release()
	if !stream.Next() || stream.RecordBatch().NumRows() != 1 {
		t.Fatal("missing bound result", stream.Err())
	}
	values, ok := stream.RecordBatch().Column(0).(*array.Int32)
	if !ok || values.Value(0) != 7 {
		t.Fatal("bound export changed Arrow result")
	}
	if stream.Next() || stream.Err() != nil {
		t.Fatal("bound export stream incomplete", stream.Err())
	}
}

func TestCatalogBindingActualGuardedSnapshot(t *testing.T) {
	binary, sandbox := os.Getenv("KELVO_TEST_SNAPSHOT_BINARY"), os.Getenv("KELVO_TEST_SNAPSHOT_SANDBOX")
	if binary == "" || sandbox == "" {
		t.Skip("set actual snapshot worker binary and sandbox for bound snapshot acceptance")
	}
	c := catalog.Config{Sources: []catalog.Source{{ID: "origin", Type: "clickhouse", URLEnv: "KELVO_SOURCE_UNUSED_URL"}}, Acceleration: &catalog.AccelerationConfig{
		Directory: filepath.Join(t.TempDir(), "snapshots"), TenantID: "a", Datasets: []catalog.Dataset{{ID: "daily_sales", Query: query.Request{Mode: "native", ConnectionID: "origin", SQL: "SELECT fixture"}, RefreshInterval: time.Minute, MaxAge: time.Hour, AuthorizationVersion: "v1", Limits: query.DefaultLimits()}},
	}}
	manager, err := acceleration.NewManager(c, func(catalog.Config, query.Limits) (query.Executor, error) { return snapshotPrincipalRows{}, nil })
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	published, err := manager.Refresh(context.Background(), "daily_sales", false)
	if err != nil {
		t.Fatal(err)
	}
	p := principalTestPolicy()
	p.Access.Principals["analyst"] = PrincipalGrant{Kind: "user", FederatedSources: []string{"daily_sales"}, RowColumnPolicy: &access.Policy{Sources: map[string]access.SourcePolicy{
		"daily_sales": {Tables: map[string]access.TablePolicy{"daily_sales": {Columns: []string{"id", "amount"}, Rows: &access.Predicate{Kind: "comparison", Column: "id", Type: "int64", Op: "eq", Value: "2"}}}},
	}}}
	approveCatalog(t, &p, c)
	original := &worker.Executor{Config: c, Limits: p.Limits, Binary: binary, SandboxPath: sandbox, Secrets: catalogNoSecrets{t}}
	bound, err := original.WithCatalogBinding(p.Access.CatalogBinding.SHA256)
	if err != nil {
		t.Fatal(err)
	}
	c.Acceleration.Datasets[0].AuthorizationVersion = "retargeted"
	c.Acceleration.Directory = filepath.Join(t.TempDir(), "missing")
	bound.Config.Acceleration.Datasets[0].Query.SQL = "SELECT changed"
	bound.Config.Acceleration.Directory = c.Acceleration.Directory
	authority, _ := authorityForPrincipal(p, "analyst")
	request := query.Request{Mode: "federated", Sources: []string{"daily_sales"}, SQL: "SELECT id, amount FROM daily_sales"}
	ctx, err := executionAuthorityContext(context.Background(), p, &authority, request)
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	sink := worker.NewIPCSink(&output, p.Limits)
	defer sink.Abort()
	stats, err := bound.Execute(ctx, request, sink)
	if err != nil || stats.Rows != 1 || len(stats.Accelerations) != 1 || stats.Accelerations[0].Generation != published.Generation {
		t.Fatal("bound guarded generation failed", err, stats)
	}
	if err := sink.Finish(); err != nil {
		t.Fatal(err)
	}
	reader, err := ipc.NewReader(bytes.NewReader(output.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Release()
	if !reader.Next() || reader.RecordBatch().NumRows() != 1 || reader.Schema().NumFields() != 2 {
		t.Fatal("bound snapshot policy exposed unexpected data", reader.Err())
	}
	row := reader.RecordBatch()
	ids, okID := row.Column(0).(*array.Int64)
	amounts, okAmount := row.Column(1).(*array.Int64)
	if !okID || !okAmount || ids.Value(0) != 2 || amounts.IsNull(0) || amounts.Value(0) != 20 {
		t.Fatal("bound snapshot changed exact policy result")
	}
	if reader.Next() || reader.Err() != nil {
		t.Fatal("bound snapshot stream incomplete", reader.Err())
	}
}
