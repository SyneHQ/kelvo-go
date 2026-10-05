//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/acceleration"
	"github.com/SYNEHQ/kelvo-go/internal/access"
	"github.com/SYNEHQ/kelvo-go/internal/admission"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/containment"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
)

type protectedFixtureRows struct{ calls *atomic.Int64 }

func (r protectedFixtureRows) Execute(ctx context.Context, _ query.Request, sink query.Sink) (query.Stats, error) {
	r.calls.Add(1)
	if err := ctx.Err(); err != nil {
		return query.Stats{}, err
	}
	record := objectAccessRecord()
	defer record.Release()
	if err := sink.Schema(record.Schema()); err != nil {
		return query.Stats{}, err
	}
	return query.Stats{Rows: record.NumRows()}, sink.Write(record)
}

func TestContainedWorkerProtectedObjects(t *testing.T) {
	if os.Getenv("KELVO_TEST_PROTECTED_OBJECTS") != "1" || os.Getenv("KELVO_TEST_BINARY") == "" {
		t.Skip("explicit protected-object native containment gate required")
	}
	if os.Getenv("KELVO_PROTECTED_FIXTURE_CHILD") != "1" {
		// Isolate Go's cached system roots from other tests. Trust the fixture CA
		// through normal verification, never InsecureSkipVerify or a product hook.
		command := exec.Command(os.Args[0], "-test.run=^TestContainedWorkerProtectedObjects$", "-test.v", "-test.timeout=90s")
		command.Env = append(os.Environ(), "KELVO_PROTECTED_FIXTURE_CHILD=1")
		var output protectedFixtureOutput
		command.Stdout, command.Stderr = &output, &output
		if err := command.Run(); err != nil || output.truncated {
			t.Fatalf("protected native child gate failed: %v\n%s", err, output.String())
		}
		t.Log(output.String())
		return
	}
	service, endpoint := protectedObjectTLS(t)
	executor, processManager, pool := containedExecutor(t)
	executor.Binary = os.Getenv("KELVO_TEST_BINARY")
	for _, role := range []string{"reader", "writer", "registry"} {
		upper := map[string]string{"reader": "READER", "writer": "WRITER", "registry": "REGISTRY"}[role]
		t.Setenv("KELVO_SOURCE_PROTECTED_"+upper+"_ID", role)
		t.Setenv("KELVO_SOURCE_PROTECTED_"+upper+"_SECRET", "fixture-only-secret-"+role)
	}
	credentials := func(role string) catalog.ObjectCredentials {
		return catalog.ObjectCredentials{AccessKeyIDEnv: "KELVO_SOURCE_PROTECTED_" + role + "_ID", SecretAccessKeyEnv: "KELVO_SOURCE_PROTECTED_" + role + "_SECRET"}
	}
	input := filepath.Join(t.TempDir(), "unused-input.csv")
	if err := os.WriteFile(input, []byte("id\n1\n"), 0600); err != nil {
		t.Fatal(err)
	}
	config := catalog.Config{Sources: []catalog.Source{{ID: "source", Type: "csv", Path: input}},
		Acceleration: &catalog.AccelerationConfig{Directory: t.TempDir(), TenantID: "tenant-a", ObjectStorage: &catalog.ObjectStorage{
			ObjectLocation:  catalog.ObjectLocation{Provider: "s3", Endpoint: endpoint, Bucket: "fixtures", Prefix: "cache", Region: "us-east-1"},
			ReadCredentials: credentials("READER"), WriteCredentials: credentials("WRITER"), ReaderRegistry: &catalog.ObjectReaderRegistry{Credentials: credentials("REGISTRY")},
		}}}
	if err := os.Chmod(config.Acceleration.Directory, 0700); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"orders_single", "orders_multi"} {
		dataset := catalog.Dataset{ID: id, Query: query.Request{Mode: "federated", Sources: []string{"source"}, SQL: "SELECT * FROM source"}, MaxAge: time.Hour, AuthorizationVersion: "fixture-v1", Limits: executor.Limits,
			Verification: &catalog.VerificationLimits{MaxBytes: 32 << 20}}
		if id == "orders_multi" {
			dataset.Multipart = &catalog.MultipartConfig{MaxPartBytes: 1 << 20, MaxParts: 4}
		}
		config.Acceleration.Datasets = append(config.Acceleration.Datasets, dataset)
	}
	runtime, err := acceleration.OpenObjectRuntime(config)
	if err != nil {
		if runtime != nil {
			cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = runtime.Close(cleanup)
			cancel()
		}
		t.Fatal(err)
	}
	allowCleanupFailure := false
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		err := runtime.Close(cleanup)
		if err != nil && !allowCleanupFailure {
			t.Error("runtime cleanup failed", err)
		}
		select {
		case <-runtime.Quiesced():
		case <-cleanup.Done():
			t.Error("runtime retained local work after fixture cleanup")
		}
	})
	var sourceFactories, sourceExecutions atomic.Int64
	manager, err := acceleration.NewManagerWithRuntime(config, func(catalog.Config, query.Limits) (query.Executor, error) {
		sourceFactories.Add(1)
		return protectedFixtureRows{calls: &sourceExecutions}, nil
	}, runtime)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Close() })
	executor.Config, executor.ObjectRuntime = config, runtime
	request := query.Request{Mode: "federated", Sources: []string{"orders_single", "orders_multi"}, SQL: "WITH m AS (SELECT * FROM orders_multi) SELECT s.id,s.amount,s.observed,s.tiny FROM orders_single s JOIN m ON s.id=m.id ORDER BY s.id"}
	policy := access.Policy{Sources: map[string]access.SourcePolicy{}}
	for _, id := range request.Sources {
		policy.Sources[id] = access.SourcePolicy{Tables: map[string]access.TablePolicy{id: {Columns: []string{"id", "amount", "observed", "tiny"}, Rows: &access.Predicate{Kind: "comparison", Column: "tenant_id", Type: "int64", Op: "eq", Value: "7"}}}}
	}
	ctx, err := access.WithPolicy(context.Background(), policy)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := catalog.AuthorityFingerprint(config)
	if err != nil {
		t.Fatal(err)
	}
	bound, err := executor.WithCatalogBinding(digest)
	if err != nil || bound.ObjectRuntime != runtime {
		t.Fatal("catalog-bound copy lost the shared runtime", err)
	}
	boundCtx, err := WithCatalogAuthority(ctx, digest)
	if err != nil {
		t.Fatal(err)
	}
	exportCopy := *bound
	exportCopy.ResourcePool = nil
	executeWithOuterCustody := func(t *testing.T, sink query.Sink) (query.Stats, error) {
		t.Helper()
		// Exports bind the catalog, copy the executor and pass an externally
		// owned reservation. Exercise that calling convention through a real child.
		reservation, err := pool.Acquire(boundCtx, admission.Request{MemoryBytes: 224 << 20, ScratchBytes: 16 << 20})
		if err != nil {
			t.Fatal(err)
		}
		custody, _ := containment.NewCustody(reservation.Release)
		defer custody.Complete()
		stats, executionErr := exportCopy.Execute(containment.WithCustody(boundCtx, custody), request, sink)
		state := custody.State()
		if state.Completed || state.Released || state.Held != 0 || pool.Snapshot().Active != 1 || processManager.Status().Active != 0 {
			t.Fatal("worker completed outer custody early or retained a native consumer", state)
		}
		custody.Complete()
		if pool.Snapshot().Active != 0 {
			t.Fatal("outer completion retained its reservation")
		}
		return stats, executionErr
	}
	clean := func(t *testing.T) {
		t.Helper()
		if pool.Snapshot().Active != 0 || processManager.Status().Active != 0 || service.readers() != 0 {
			t.Fatal("query retained process, reservation or durable pin")
		}
		executor.ScratchRoot.mu.Lock()
		active := executor.ScratchRoot.active
		executor.ScratchRoot.mu.Unlock()
		if active != 0 || len(objectWorkerChildren(t, executor.Binary)) != 0 {
			t.Fatal("query retained scratch or native process")
		}
	}
	published := make(map[string]acceleration.Snapshot)
	t.Run("publish-single-and-multipart", func(t *testing.T) {
		for _, id := range request.Sources {
			snapshot, err := manager.Refresh(context.Background(), id, false)
			if err != nil || snapshot.Rows != 4 || snapshot.Generation == "" || (id == "orders_multi" && len(snapshot.Parts) == 0) {
				t.Fatal("protected refresh failed", id, err)
			}
			published[id] = snapshot
			status, err := runtime.Status(context.Background(), id)
			if err != nil || status.Generation != snapshot.Generation || !status.RefreshedAt.Equal(snapshot.RefreshedAt) {
				t.Fatal("guarded status changed publication identity", id, err)
			}
		}
		clean(t)
	})
	if t.Failed() {
		return
	}
	t.Run("verify-and-inventory-single-and-descriptor-layouts", func(t *testing.T) {
		// This small descriptor-layout fixture may contain one physical part.
		// Two-part byte/schema verification has separate acceleration-package tests.
		sameSnapshot := func(got, want acceleration.Snapshot) bool {
			// Ignore only process-local clock observations; every exported field,
			// including exact object versions, ordered parts and refresh time, matches.
			left, leftErr := json.Marshal(got)
			right, rightErr := json.Marshal(want)
			return leftErr == nil && rightErr == nil && bytes.Equal(left, right)
		}
		for _, id := range request.Sources {
			prior := published[id]
			current, err := manager.Refresh(context.Background(), id, false)
			if err != nil || current.Generation == prior.Generation || current.Rows != 4 || current.Bytes <= 0 || (id == "orders_multi" && len(current.Parts) == 0) {
				t.Fatal("second protected generation did not publish", id, err)
			}
			clean(t)
			beforeFactories, beforeExecutions := sourceFactories.Load(), sourceExecutions.Load()
			service.mu.Lock()
			rootKey := "/fixtures/cache/tenant-a/" + id + "/current.yaml"
			beforeRoot := service.objects[rootKey]
			beforeRoot.data = bytes.Clone(beforeRoot.data)
			beforeViolations, beforeRanges := service.violations, service.ranges
			service.mu.Unlock()
			verified, err := manager.Verify(context.Background(), id)
			if err != nil || !sameSnapshot(verified, current) {
				t.Fatal("TLS verification changed the exact current snapshot", id, err)
			}
			clean(t)
			inventory, err := manager.Inventory(context.Background(), id)
			if err != nil || len(inventory) != 2 {
				t.Fatal("TLS inventory did not retain both generations", id, err, len(inventory))
			}
			for index, want := range []acceleration.Snapshot{current, prior} {
				entry := inventory[index]
				if !entry.Verified || entry.Active != (index == 0) || entry.CatalogScope != "retained_manifest" || entry.CatalogTruncated || !sameSnapshot(entry.Snapshot, want) {
					t.Fatal("TLS inventory changed exact history or verification status", id, index)
				}
			}
			clean(t)
			service.mu.Lock()
			afterRoot := service.objects[rootKey]
			violations, ranges := service.violations, service.ranges
			service.mu.Unlock()
			if sourceFactories.Load() != beforeFactories || sourceExecutions.Load() != beforeExecutions {
				t.Fatal("maintenance invoked the source executor", id)
			}
			if beforeViolations != 0 || violations != 0 || ranges <= beforeRanges || !reflect.DeepEqual(beforeRoot, afterRoot) {
				t.Fatal("TLS maintenance changed the root, violated roles/read ordering, or skipped footer reads", id)
			}
		}
	})
	if t.Failed() {
		return
	}
	t.Run("resolved-child-catalog-has-no-provider-identity", func(t *testing.T) {
		resolved, err := acceleration.ResolveWithRuntime(ctx, config, request, runtime)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := resolved.Close(cleanup); err != nil {
				t.Error(err)
			}
		}()
		payload, err := json.Marshal(resolved.Sources)
		if err != nil {
			t.Fatal(err)
		}
		for _, forbidden := range []string{endpoint, "KELVO_SOURCE_", "reader_registry", "read_credentials", "write_credentials", "fixture-only-secret"} {
			if strings.Contains(string(payload), forbidden) {
				t.Fatal("provider identity escaped into the child catalog")
			}
		}
		for _, source := range resolved.Sources {
			names, err := sourceEnvironmentNames(source)
			if err != nil || len(names) != 0 {
				t.Fatal("protected child requested a provider credential", err)
			}
		}
	})
	t.Run("cte-join-types-policy-and-shared-manager", func(t *testing.T) {
		// Manager.Close must not retire providers still borrowed by the worker.
		if err := manager.Close(); err != nil {
			t.Fatal(err)
		}
		sink := &objectExactSink{}
		stats, err := executor.Execute(ctx, request, sink)
		want := []objectExactValue{{1, decimal128.FromI64(-123456789), false, -315521754876544, false, -128}, {4, decimal128.FromI64(1), false, 0, true, 1}, {18446744073709551615, decimal128.Num{}, true, 123456789, false, 127}}
		if err != nil || stats.Rows != 3 || len(stats.Accelerations) != 2 || !reflect.DeepEqual(sink.values, want) {
			t.Fatal("protected native CTE join changed exact results", err, sink.values)
		}
		if sink.schema.Metadata().Len() != 0 || !arrow.TypeEqual(sink.schema.Field(1).Type, &arrow.Decimal128Type{Precision: 30, Scale: 4}) {
			t.Fatal("protected query lost policy metadata or decimal type")
		}
		clean(t)
		copiedSink := &objectExactSink{}
		copiedStats, copiedErr := executeWithOuterCustody(t, copiedSink)
		if copiedErr != nil || copiedStats.Rows != 3 || len(copiedStats.Accelerations) != 2 || !reflect.DeepEqual(copiedSink.values, want) {
			t.Fatal("catalog-bound copy with outer custody changed the protected result", copiedErr, copiedSink.values)
		}
		clean(t)
	})
	t.Run("hidden-column-and-uncontained-refusal", func(t *testing.T) {
		denied := request
		denied.SQL = "SELECT tenant_id FROM orders_single"
		if _, err := executor.Execute(ctx, denied, &objectExactSink{}); err == nil {
			t.Fatal("hidden policy column was exposed")
		}
		uncontained := *executor
		uncontained.Containment = nil
		if _, err := uncontained.Execute(ctx, request, &objectExactSink{}); err == nil {
			t.Fatal("uncontained protected query was admitted")
		}
		clean(t)
	})
	t.Run("immutable-runtime-refuses-retargeting", func(t *testing.T) {
		changed, _, err := catalog.AuthoritySnapshot(config)
		if err != nil {
			t.Fatal(err)
		}
		changed.Acceleration.ObjectStorage.Prefix = "other"
		if runtime.Match(changed) == nil {
			t.Fatal("mutated namespace matched immutable runtime")
		}
		copy := *executor
		copy.Config = changed
		if _, err := copy.Execute(ctx, request, &objectExactSink{}); err == nil {
			t.Fatal("retargeted worker succeeded")
		}
		clean(t)
	})
	t.Run("cancellation-joins-ranges-and-child", func(t *testing.T) {
		service.blockRead.Store(true)
		defer service.blockRead.Store(false)
		deadline, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		finished := make(chan error, 1)
		go func() { _, err := executor.Execute(deadline, request, &objectExactSink{}); finished <- err }()
		select {
		case <-service.readStarted:
			cancel()
		case <-deadline.Done():
			<-finished
			t.Fatal("cancellation fixture never reached an active payload range")
		}
		if err := <-finished; err == nil {
			t.Fatal("blocked source query succeeded")
		}
		clean(t)
	})
	if t.Failed() {
		return
	}
	t.Run("binding-loss-after-last-batch-refuses-completion", func(t *testing.T) {
		allowCleanupFailure = true
		sink := &protectedLastBatchSink{objectExactSink: objectExactSink{}, after: service.invalidateBindings}
		_, err := executeWithOuterCustody(t, sink)
		if err == nil || len(sink.values) != 3 || query.PublicError(err).Code != "DATASET_UNAVAILABLE" || query.PublicError(err).Message != "Snapshot ownership or cleanup did not complete" {
			t.Fatal("complete result concealed terminal binding loss", err)
		}
		if pool.Snapshot().Active != 0 || processManager.Status().Active != 0 {
			t.Fatal("binding loss retained a cleaned native process")
		}
	})
	service.mu.Lock()
	violations, ranges := service.violations, service.ranges
	service.mu.Unlock()
	if violations != 0 || ranges == 0 {
		t.Fatal("fixture observed unpinned access, missing publication fences, or no payload reads", violations, ranges)
	}
}

type protectedFixtureOutput struct {
	boundedBuffer
	truncated bool
}

func (b *protectedFixtureOutput) Write(data []byte) (int, error) {
	if len(data) > maxOutcomeBytes-b.Len() {
		b.truncated = true
	}
	return b.boundedBuffer.Write(data)
}

type protectedLastBatchSink struct {
	objectExactSink
	after func()
}

func (s *protectedLastBatchSink) Write(record arrow.RecordBatch) error {
	if err := s.objectExactSink.Write(record); err != nil {
		return err
	}
	if s.after == nil {
		return errors.New("fixture result unexpectedly split into extra batches")
	}
	s.after()
	s.after = nil
	return nil
}
