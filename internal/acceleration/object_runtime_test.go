//go:build linux || darwin

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/objectstore"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/readerlease"
)

func objectRuntimeTestConfig(t *testing.T) catalog.Config {
	t.Helper()
	a := testObjectConfig(t)
	a.ObjectStorage.ReadCredentials = catalog.ObjectCredentials{AccessKeyIDEnv: "KELVO_SOURCE_READ_ID", SecretAccessKeyEnv: "KELVO_SOURCE_READ_SECRET"}
	a.ObjectStorage.WriteCredentials = catalog.ObjectCredentials{AccessKeyIDEnv: "KELVO_SOURCE_WRITE_ID", SecretAccessKeyEnv: "KELVO_SOURCE_WRITE_SECRET"}
	a.ObjectStorage.ReaderRegistry = &catalog.ObjectReaderRegistry{Credentials: catalog.ObjectCredentials{AccessKeyIDEnv: "KELVO_SOURCE_REGISTRY_ID", SecretAccessKeyEnv: "KELVO_SOURCE_REGISTRY_SECRET"}}
	a.Datasets = []catalog.Dataset{{ID: "events", Query: query.Request{Mode: "native", ConnectionID: "source", SQL: "SELECT id FROM events"},
		MaxAge: time.Hour, RefreshInterval: time.Minute, AuthorizationVersion: "v1", Limits: query.DefaultLimits()}}
	return catalog.Config{Sources: []catalog.Source{{ID: "source", Type: "clickhouse", URLEnv: "KELVO_SOURCE_INPUT_URL"}}, Acceleration: &a}
}

// Each wrapper owns its close, while the independent exact-key service remains
// available to other logical clients. This is a local protocol fixture only.
type objectRuntimeTestClient struct {
	objectstore.Client
	closes       atomic.Int32
	once         sync.Once
	closeGate    <-chan struct{}
	closeStarted chan struct{}
	transport    *objectstore.SharedTransport
	credentials  catalog.ObjectCredentials
}

func (c *objectRuntimeTestClient) Close() {
	c.once.Do(func() {
		c.closes.Add(1)
		if c.closeStarted != nil {
			close(c.closeStarted)
		}
		if c.closeGate != nil {
			<-c.closeGate
		}
	})
}

func (c *objectRuntimeTestClient) GetRange(ctx context.Context, key, version string, offset, length int64) (io.ReadCloser, objectstore.Info, error) {
	body, info, err := c.Get(ctx, key, version)
	if err != nil {
		return nil, info, err
	}
	defer body.Close()
	raw, err := io.ReadAll(body)
	if err != nil {
		return nil, info, err
	}
	if offset < 0 || length <= 0 || offset > int64(len(raw)) || length > int64(len(raw))-offset {
		return nil, info, errors.New("invalid fixture range")
	}
	return io.NopCloser(bytes.NewReader(raw[offset : offset+length])), info, nil
}

func objectRuntimeTestOpen(t *testing.T) (*ObjectRuntime, catalog.Config, []*objectRuntimeTestClient) {
	t.Helper()
	config, service := objectRuntimeTestConfig(t), newFakeSnapshotObjects()
	var clients []*objectRuntimeTestClient
	r, err := openObjectRuntime(config, func(_ catalog.ObjectLocation, credentials catalog.ObjectCredentials, transport *objectstore.SharedTransport) (objectstore.Client, error) {
		client := &objectRuntimeTestClient{Client: service, transport: transport, credentials: credentials}
		clients = append(clients, client)
		return client, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ownerTestClose(t, r.Close) })
	return r, config, clients
}

func TestObjectRuntimeUsesRealRegistryAndImmutableSharedResources(t *testing.T) {
	r, config, clients := objectRuntimeTestOpen(t)
	if len(clients) != 3 || r.registry == nil || r.dataTransport == r.registryTransport || r.budget.snapshot() != (readerCounts{owners: 1}) {
		t.Fatal("runtime did not own its bounded resource bundle")
	}
	storage := config.Acceleration.ObjectStorage
	if clients[0].transport != r.dataTransport || clients[1].transport != r.dataTransport || clients[2].transport != r.registryTransport ||
		clients[0].credentials != storage.ReadCredentials || clients[1].credentials != storage.WriteCredentials || clients[2].credentials != storage.ReaderRegistry.Credentials {
		t.Fatal("credential clients were not bound to their dedicated node transports")
	}
	backend, err := r.backendFor(config)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := backend.(MultipartBackend); !ok {
		t.Fatal("borrowed backend hid multipart support")
	}
	if err := backend.Close(); err != nil {
		t.Fatal(err)
	}
	for _, client := range clients {
		if client.closes.Load() != 0 {
			t.Fatal("borrower closed node client")
		}
	}
	config.Acceleration.ObjectStorage.ReaderRegistry.Credentials.AccessKeyIDEnv = "KELVO_SOURCE_REPLACED_ID"
	if r.Match(config) == nil || r.config.Acceleration.ObjectStorage.ReaderRegistry.Credentials.AccessKeyIDEnv != "KELVO_SOURCE_REGISTRY_ID" {
		t.Fatal("mutable catalog retargeted the runtime")
	}
	binding := readerlease.Binding{Reference: readerlease.Reference{Tenant: "tenant-a", Dataset: "events", Generation: strings.Repeat("1", 32), Incarnation: strings.Repeat("2", 64)}, ContentSHA256: strings.Repeat("3", 64)}
	if err := r.registry.Stage(context.Background(), binding.Reference, strings.Repeat("4", 32)); err != nil {
		t.Fatal(err)
	}
	if err := r.registry.Seal(context.Background(), binding, strings.Repeat("4", 32)); err != nil {
		t.Fatal(err)
	}
	op, err := r.beginOperation(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	guard, err := r.acquire(op.Context(), []readerlease.Binding{binding})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := guard.pins[0].(*readerlease.Lease); !ok {
		t.Fatal("fixture pin replaced real registry lease")
	}
	if err := guard.Check(); err != nil {
		t.Fatal(err)
	}
	if err := ownerTestClose(t, guard.Close); err != nil {
		t.Fatal(err)
	}
	if err := op.Close(); err != nil {
		t.Fatal(err)
	}
	if err := ownerTestClose(t, r.Close); err != nil {
		t.Fatal(err)
	}
	for _, client := range clients {
		if client.closes.Load() != 1 {
			t.Fatal("node did not close client exactly once")
		}
	}
}

func TestObjectRuntimeBoundsPrePinWorkAndRetainsUnfinishedCleanup(t *testing.T) {
	r, _, clients := objectRuntimeTestOpen(t)
	operations := make([]*objectRuntimeOperation, objectRuntimeOperationLimit)
	for i := range operations {
		var err error
		operations[i], err = r.beginOperation(context.Background())
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := r.beginOperation(context.Background()); !errors.Is(err, errReaderCapacity) {
		t.Fatal(err)
	}
	ownerTestTimeout(t, r.Close)
	for _, op := range operations {
		ownerTestWait(t, op.Context().Done(), "operation cancellation")
	}
	for range 8 {
		ownerTestTimeout(t, r.Close)
	}
	ownerTestPending(t, r.Quiesced(), "unfinished operation custody")
	for _, client := range clients {
		if client.closes.Load() != 0 {
			t.Fatal("client closed before operation handback")
		}
	}
	for _, op := range operations {
		if op.Close() == nil {
			t.Fatal("runtime drain reported operation success")
		}
	}
	ownerTestWait(t, r.Quiesced(), "joined runtime")
	if !errors.Is(ownerTestClose(t, r.Close), errReaderCleanupUnknown) {
		t.Fatal("late cleanup erased uncertainty")
	}
}

func TestObjectRuntimeAdoptsPartialConstructionAndRefusesLegacyOpen(t *testing.T) {
	config, service := objectRuntimeTestConfig(t), newFakeSnapshotObjects()
	if _, err := OpenBackend(*config.Acceleration); err == nil {
		t.Fatal("unowned protected backend opened")
	}
	if _, err := NewObjectBackend(*config.Acceleration, service); err == nil {
		t.Fatal("injected protected backend bypassed runtime")
	}
	gate := make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(gate) })
	client := &objectRuntimeTestClient{Client: service, closeGate: gate, closeStarted: make(chan struct{})}
	failure := errors.New("partial constructor failure")
	r, err := openObjectRuntime(config, func(catalog.ObjectLocation, catalog.ObjectCredentials, *objectstore.SharedTransport) (objectstore.Client, error) {
		return client, failure
	})
	if r == nil || !errors.Is(err, failure) {
		t.Fatal("partial client custody was discarded", err)
	}
	ownerTestWait(t, client.closeStarted, "partial client close")
	ownerTestTimeout(t, r.Close)
	if r.budget.snapshot().owners != 1 || client.closes.Load() != 1 {
		t.Fatal("partial close lost owner reservation")
	}
	once.Do(func() { close(gate) })
	ownerTestWait(t, r.Quiesced(), "partial construction cleanup")
	if r.budget.snapshot() != (readerCounts{}) {
		t.Fatal("joined partial owner retained capacity")
	}
}

func TestObjectRuntimeRefusesAzureWritableReaderBeforeConstruction(t *testing.T) {
	config := objectRuntimeTestConfig(t)
	storage := config.Acceleration.ObjectStorage
	storage.Provider, storage.Account, storage.Region = "azure", "fixtureaccount", ""
	storage.ReadCredentials = catalog.ObjectCredentials{SASTokenEnv: "KELVO_SOURCE_READ_SAS"}
	storage.WriteCredentials = catalog.ObjectCredentials{SASTokenEnv: "KELVO_SOURCE_WRITE_SAS"}
	storage.ReaderRegistry.Credentials = catalog.ObjectCredentials{SASTokenEnv: "KELVO_SOURCE_REGISTRY_SAS"}
	t.Setenv("KELVO_SOURCE_READ_SAS", "sv=2023-11-03&sp=rw&se=2030-01-01&sr=c&sig=fixture")
	calls := 0
	r, err := openObjectRuntime(config, func(catalog.ObjectLocation, catalog.ObjectCredentials, *objectstore.SharedTransport) (objectstore.Client, error) {
		calls++
		return nil, nil
	})
	if err == nil || r != nil || calls != 0 {
		t.Fatal("writable read identity reached resource construction")
	}
}

func TestObjectRuntimeRefusesSharedRegistryIdentityBeforeConstruction(t *testing.T) {
	config := objectRuntimeTestConfig(t)
	storage := config.Acceleration.ObjectStorage
	storage.ReaderRegistry.Credentials = storage.WriteCredentials
	calls := 0
	r, err := openObjectRuntime(config, func(catalog.ObjectLocation, catalog.ObjectCredentials, *objectstore.SharedTransport) (objectstore.Client, error) {
		calls++
		return nil, nil
	})
	if err == nil || r != nil || calls != 0 {
		t.Fatal("publisher identity reached registry construction")
	}
}
