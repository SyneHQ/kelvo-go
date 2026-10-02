//go:build linux || darwin

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/objectstore"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
)

type schemaRangeObjects struct {
	*fakeSnapshotObjects
	ranges int
	bytes  int64
	fault  string
}

func (s *schemaRangeObjects) GetRange(ctx context.Context, key, version string, offset, length int64) (io.ReadCloser, objectstore.Info, error) {
	if err := ctx.Err(); err != nil {
		return nil, objectstore.Info{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	object, ok := s.objects[key]
	if !ok {
		return nil, objectstore.Info{}, objectstore.ErrNotFound
	}
	if object.info.Version != version {
		return nil, objectstore.Info{}, objectstore.ErrConflict
	}
	if offset < 0 || length < 1 || length > int64(len(object.data))-offset {
		return nil, objectstore.Info{}, io.ErrUnexpectedEOF
	}
	s.ranges++
	s.bytes += length
	info := s.info(object)
	if s.fault == "version" {
		info.Version = "changed"
	}
	return io.NopCloser(bytes.NewReader(object.data[offset : offset+length])), info, nil
}

func TestObjectSchemaContractUsesVersionedRangesAndKeepsLastGood(t *testing.T) {
	config, _, _ := managerFixture(t)
	objectConfig := testObjectConfig(t)
	objectConfig.Datasets = config.Acceleration.Datasets
	config.Acceleration = &objectConfig
	service := &schemaRangeObjects{fakeSnapshotObjects: newFakeSnapshotObjects()}
	backend, err := NewObjectBackend(objectConfig, service)
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)
	m := &Manager{config: config, store: backend, factory: func(catalog.Config, query.Limits) (query.Executor, error) { return schemaFixture{schema}, nil }}
	first, err := m.Refresh(context.Background(), "orders_fast", false)
	if err != nil {
		t.Fatal(err)
	}
	second, err := m.Refresh(context.Background(), "orders_fast", false)
	if err != nil {
		t.Fatal(err)
	}
	if first.SchemaHash == "" || first.SchemaHash != second.SchemaHash || service.ranges == 0 || service.payloadGets != 0 {
		t.Fatalf("schema verification did not use ranges: %+v calls=%d full=%d", second, service.ranges, service.payloadGets)
	}
	schema = arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true}}, nil)
	if _, err = m.Refresh(context.Background(), "orders_fast", false); !errors.Is(err, ErrSchemaMismatch) {
		t.Fatalf("incompatible refresh: %v", err)
	}
	current, err := m.Status("orders_fast")
	if err != nil || current.Generation != second.Generation {
		t.Fatal("failed refresh changed committed snapshot", err)
	}
	service.fault = "version"
	if _, err = m.Refresh(context.Background(), "orders_fast", false); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("range version mutation accepted: %v", err)
	}
}

// The shared object fixture also models version-bound range reads, as required
// when a scheduled refresh checks the previous generation's original schema.
func (s *fakeSnapshotObjects) GetRange(ctx context.Context, key, version string, offset, length int64) (io.ReadCloser, objectstore.Info, error) {
	if err := ctx.Err(); err != nil {
		return nil, objectstore.Info{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.unavailable {
		return nil, objectstore.Info{}, errObjectNetwork
	}
	object, ok := s.objects[key]
	info := s.info(object)
	if !ok {
		return nil, info, objectstore.ErrNotFound
	}
	if object.info.Version != version {
		return nil, info, objectstore.ErrConflict
	}
	if offset < 0 || length < 1 || offset > int64(len(object.data)) || length > int64(len(object.data))-offset {
		return nil, info, io.ErrUnexpectedEOF
	}
	data := bytes.Clone(object.data[offset : offset+length])
	if s.corruptPayloadGet && len(data) > 0 {
		data[0] ^= 1
	}
	return io.NopCloser(bytes.NewReader(data)), info, nil
}
