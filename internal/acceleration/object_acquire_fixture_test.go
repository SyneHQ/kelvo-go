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
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/objectstore"
	"go.yaml.in/yaml/v3"
)

type acquisitionCall struct{ method, key, version string }

// Hooks are installed before an operation starts. Reads and concurrent Heads
// are recorded separately from fixture setup, including rejected provider calls.
type acquisitionObjects struct {
	*fakeSnapshotObjects
	callsMu   sync.Mutex
	calls     []acquisitionCall
	bodyHook  func(string, io.ReadCloser) io.ReadCloser
	afterHead func(string)
}

func (c *acquisitionObjects) record(method, key, version string) {
	c.callsMu.Lock()
	c.calls = append(c.calls, acquisitionCall{method, key, version})
	c.callsMu.Unlock()
}

func (c *acquisitionObjects) observed() []acquisitionCall {
	c.callsMu.Lock()
	defer c.callsMu.Unlock()
	return append([]acquisitionCall(nil), c.calls...)
}

func (c *acquisitionObjects) Get(ctx context.Context, key, version string) (io.ReadCloser, objectstore.Info, error) {
	c.record("get", key, version)
	body, info, err := c.fakeSnapshotObjects.Get(ctx, key, version)
	if err == nil && c.bodyHook != nil {
		body = c.bodyHook(key, body)
	}
	return body, info, err
}

func (c *acquisitionObjects) Head(ctx context.Context, key, version string) (objectstore.Info, error) {
	c.record("head", key, version)
	info, err := c.fakeSnapshotObjects.Head(ctx, key, version)
	if err == nil && c.afterHead != nil {
		c.afterHead(key)
	}
	return info, err
}

func (c *acquisitionObjects) GetRange(_ context.Context, key, version string, _, _ int64) (io.ReadCloser, objectstore.Info, error) {
	c.record("range", key, version)
	return nil, objectstore.Info{}, errors.New("acquisition fixture does not allow payload reads")
}

type acquisitionBody struct {
	io.ReadCloser
	readOnce, closeOnce sync.Once
	beforeRead          func()
	afterClose          func()
}

func (b *acquisitionBody) Read(p []byte) (int, error) {
	b.readOnce.Do(func() {
		if b.beforeRead != nil {
			b.beforeRead()
		}
	})
	return b.ReadCloser.Read(p)
}

func (b *acquisitionBody) Close() error {
	err := b.ReadCloser.Close()
	b.closeOnce.Do(func() {
		if b.afterClose != nil {
			b.afterClose()
		}
	})
	return err
}

// These raw payloads exercise manifest/descriptor/Head acquisition, not Parquet
// footer verification. Every immutable version is supplied by the CAS fixture.
func objectAcquisitionFixture(t *testing.T, multipart bool) (*objectBackend, *acquisitionObjects, *objectCommitted) {
	t.Helper()
	objects := &acquisitionObjects{fakeSnapshotObjects: newFakeSnapshotObjects()}
	backend, err := newObjectBackend(testObjectConfig(t), objects)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = backend.Close() })
	put := func(key string, data []byte) objectstore.Info {
		t.Helper()
		info, err := objects.fakeSnapshotObjects.Put(context.Background(), key, bytes.NewReader(data), int64(len(data)), objectSHA256(data), objectstore.Condition{Absent: true})
		if err != nil {
			t.Fatal(err)
		}
		return info
	}
	committed := &objectCommitted{Generation: strings.Repeat("a", 32), Fingerprint: "source-config", SchemaHash: strings.Repeat("b", 64), RefreshedAt: time.Now().Add(-time.Minute).UTC()}
	data := []byte("PAR1fixturePAR1")
	if !multipart {
		info := put(backend.key("events", committed.Generation+".parquet"), data)
		committed.Rows, committed.Bytes, committed.SHA256, committed.ObjectVersion = 1, info.Size, info.SHA256, info.Version
	} else {
		descriptor := objectGenerationDescriptor{Version: 1, Dataset: "events", Generation: committed.Generation, SchemaHash: committed.SchemaHash}
		for index := range 2 {
			info := put(backend.key("events", multipartName(committed.Generation, index)), data)
			descriptor.Parts = append(descriptor.Parts, objectPart{Rows: 1, Bytes: info.Size, SHA256: info.SHA256, ObjectVersion: info.Version})
			descriptor.Rows++
			descriptor.Bytes += info.Size
		}
		encoded, err := marshalObjectDescriptor(descriptor)
		if err != nil {
			t.Fatal(err)
		}
		info := put(backend.key("events", objectDescriptorName(committed.Generation)), encoded)
		committed.Rows, committed.Bytes, committed.SHA256 = descriptor.Rows, descriptor.Bytes, info.SHA256
		committed.Descriptor = &objectDescriptorRef{Bytes: info.Size, ObjectVersion: info.Version, PartCount: len(descriptor.Parts)}
	}
	manifest, err := yaml.Marshal(objectManifest{Version: 4, Dataset: "events", Committed: committed})
	if err != nil {
		t.Fatal(err)
	}
	put(backend.key("events", storeManifestName), manifest)
	return backend, objects, committed
}

func requireAcquisitionManifestOnly(t *testing.T, backend *objectBackend, objects *acquisitionObjects, count int) {
	t.Helper()
	calls := objects.observed()
	if len(calls) != count {
		t.Fatalf("wanted %d manifest calls and no generation access, got %+v", count, calls)
	}
	for _, call := range calls {
		if call != (acquisitionCall{"get", backend.key("events", storeManifestName), ""}) {
			t.Fatalf("generation access before acquisition acceptance: %+v", call)
		}
	}
}
