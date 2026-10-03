//go:build linux || darwin

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type evolutionRefreshSource struct{ schema, changed *arrow.Schema }

func (s *evolutionRefreshSource) Execute(ctx context.Context, _ query.Request, sink query.Sink) (query.Stats, error) {
	if err := ctx.Err(); err != nil {
		return query.Stats{}, err
	}
	if err := sink.Schema(s.schema); err != nil {
		return query.Stats{}, err
	}
	builder := array.NewRecordBuilder(memory.DefaultAllocator, s.schema)
	defer builder.Release()
	for _, field := range builder.Fields() {
		field.AppendNull()
		field.AppendNull()
	}
	batch := builder.NewRecordBatch()
	defer batch.Release()
	if err := sink.Write(batch); err != nil {
		return query.Stats{}, err
	}
	if s.changed != nil {
		if err := sink.Schema(s.changed); err != nil {
			return query.Stats{}, err
		}
	}
	return query.Stats{Rows: 2}, nil
}

func evolutionManager(t *testing.T, remote, multipart bool, policy *catalog.SchemaEvolution) (*Manager, *evolutionRefreshSource) {
	t.Helper()
	config, manager, _ := managerFixture(t)
	dataset := &config.Acceleration.Datasets[0]
	dataset.SchemaEvolution = policy
	if multipart {
		dataset.Multipart = &catalog.MultipartConfig{MaxPartBytes: 1 << 20, MaxParts: 8}
	}
	if remote {
		_ = manager.Close()
		objectConfig := testObjectConfig(t)
		objectConfig.Datasets = config.Acceleration.Datasets
		config.Acceleration = &objectConfig
		backend, err := NewObjectBackend(objectConfig, newFakeSnapshotObjects())
		if err != nil {
			t.Fatal(err)
		}
		manager.store = backend
	}
	source := &evolutionRefreshSource{}
	manager.config = config
	manager.factory = func(catalog.Config, query.Limits) (query.Executor, error) { return source, nil }
	t.Cleanup(func() { _ = manager.Close() })
	return manager, source
}

func refreshEvolutionSchema(typ arrow.DataType, extra bool) *arrow.Schema {
	fields := []arrow.Field{{Name: "id", Type: typ, Nullable: true}}
	if extra {
		fields = append(fields, arrow.Field{Name: "note", Type: arrow.BinaryTypes.String, Nullable: true})
	}
	return arrow.NewSchema(fields, nil)
}

// These four publication paths share compatibility rules but retain separate
// immutable-file, descriptor, lease and CAS mechanisms.
func TestSchemaEvolutionPublicationMatrix(t *testing.T) {
	for _, remote := range []bool{false, true} {
		for _, multipart := range []bool{false, true} {
			backendName := "local"
			if remote {
				backendName = "remote"
			}
			layout := "single"
			if multipart {
				layout = "multipart"
			}
			t.Run(backendName+"/"+layout, func(t *testing.T) {
				for _, tc := range []struct {
					name   string
					policy *catalog.SchemaEvolution
					next   *arrow.Schema
					accept bool
				}{
					{"strict-rejects-addition", nil, refreshEvolutionSchema(arrow.PrimitiveTypes.Int32, true), false},
					{"strict-rejects-widening", nil, refreshEvolutionSchema(arrow.PrimitiveTypes.Int64, false), false},
					{"append", &catalog.SchemaEvolution{AddNullableColumns: true}, refreshEvolutionSchema(arrow.PrimitiveTypes.Int32, true), true},
					{"widen", &catalog.SchemaEvolution{SafeWidening: true}, refreshEvolutionSchema(arrow.PrimitiveTypes.Int64, false), true},
					{"both", &catalog.SchemaEvolution{AddNullableColumns: true, SafeWidening: true}, refreshEvolutionSchema(arrow.PrimitiveTypes.Int64, true), true},
					{"append-does-not-widen", &catalog.SchemaEvolution{AddNullableColumns: true}, refreshEvolutionSchema(arrow.PrimitiveTypes.Int64, false), false},
					{"widen-does-not-append", &catalog.SchemaEvolution{SafeWidening: true}, refreshEvolutionSchema(arrow.PrimitiveTypes.Int32, true), false},
				} {
					t.Run(tc.name, func(t *testing.T) {
						manager, source := evolutionManager(t, remote, multipart, tc.policy)
						ctx := context.Background()
						initialSchema := refreshEvolutionSchema(arrow.PrimitiveTypes.Int32, false)
						source.schema = initialSchema
						first, err := manager.Refresh(ctx, "orders_fast", false)
						if err != nil {
							t.Fatal(err)
						}
						pinned, err := manager.store.Acquire(ctx, "orders_fast", first.Fingerprint, time.Hour)
						if err != nil {
							t.Fatal(err)
						}
						defer pinned.Close()
						source.schema = tc.next
						next, err := manager.Refresh(ctx, "orders_fast", false)
						if !tc.accept {
							if !errors.Is(err, ErrSchemaMismatch) {
								t.Fatalf("expected incompatible refresh, got %v", err)
							}
							current, statusErr := manager.Status("orders_fast")
							if statusErr != nil || current.Generation != first.Generation || current.SchemaHash != first.SchemaHash {
								t.Fatalf("rejected refresh changed pointer: %+v %v", current, statusErr)
							}
							return
						}
						if err != nil {
							t.Fatal(err)
						}
						if next.Generation == first.Generation || next.SchemaHash == first.SchemaHash || next.Rows != 2 {
							t.Fatalf("evolution not published: %+v", next)
						}
						if multipart != (len(next.Parts) > 0) {
							t.Fatal("unexpected snapshot layout")
						}
						if _, err = manager.Verify(ctx, "orders_fast"); err != nil {
							t.Fatal(err)
						}
						verifyPinnedEvolutionSchema(t, manager, pinned.Snapshot, initialSchema)
						// Both generations have the same policy/config fingerprint. Restore must
						// reject actual contract regression, not merely changed configuration.
						if first.Fingerprint != next.Fingerprint {
							t.Fatal("schema contents changed catalog fingerprint")
						}
						if _, err = manager.Restore(ctx, "orders_fast", first.Generation, next.Generation); !errors.Is(err, ErrCorrupt) {
							t.Fatalf("schema-changing restore accepted: %v", err)
						}
						// Even permitted widening must not permit narrowing a later refresh.
						source.schema = initialSchema
						if _, err = manager.Refresh(ctx, "orders_fast", false); !errors.Is(err, ErrSchemaMismatch) {
							t.Fatalf("schema regression accepted: %v", err)
						}
						current, statusErr := manager.Status("orders_fast")
						if statusErr != nil || current.Generation != next.Generation {
							t.Fatalf("failed operation changed current pointer: %+v %v", current, statusErr)
						}
					})
				}
			})
		}
	}
}

func verifyPinnedEvolutionSchema(t *testing.T, manager *Manager, snapshot Snapshot, want *arrow.Schema) {
	t.Helper()
	if backend, ok := manager.store.(*objectBackend); ok {
		got, err := backend.verifyObjectSnapshot(context.Background(), snapshot)
		if err != nil || !SchemaEqual(got, want) {
			t.Fatalf("pinned object schema changed: %v", err)
		}
		return
	}
	paths := []SnapshotPart{{Path: snapshot.Path, Bytes: snapshot.Bytes}}
	if len(snapshot.Parts) > 0 {
		paths = snapshot.Parts
	}
	for _, part := range paths {
		file, err := os.Open(part.Path)
		if err != nil {
			t.Fatal(err)
		}
		got, readErr := ReadParquetSchema(file, part.Bytes)
		_ = file.Close()
		if readErr != nil || !SchemaEqual(got, want) {
			t.Fatalf("pinned local schema changed: %v", readErr)
		}
	}
}

func TestSchemaEvolutionDoesNotRelaxWithinGeneration(t *testing.T) {
	for _, remote := range []bool{false, true} {
		for _, multipart := range []bool{false, true} {
			manager, source := evolutionManager(t, remote, multipart, &catalog.SchemaEvolution{AddNullableColumns: true, SafeWidening: true})
			source.schema = refreshEvolutionSchema(arrow.PrimitiveTypes.Int32, false)
			first, err := manager.Refresh(context.Background(), "orders_fast", false)
			if err != nil {
				t.Fatal(err)
			}
			source.changed = refreshEvolutionSchema(arrow.PrimitiveTypes.Int64, true)
			if _, err = manager.Refresh(context.Background(), "orders_fast", false); err == nil {
				t.Fatal("within-generation evolution accepted")
			}
			current, err := manager.Status("orders_fast")
			if err != nil || current.Generation != first.Generation {
				t.Fatalf("mixed schema replaced current pointer: %v", err)
			}
		}
	}
}

func TestSchemaEvolutionPolicyFingerprintFencesExistingSnapshot(t *testing.T) {
	for _, remote := range []bool{false, true} {
		for _, multipart := range []bool{false, true} {
			manager, source := evolutionManager(t, remote, multipart, nil)
			source.schema = refreshEvolutionSchema(arrow.PrimitiveTypes.Int32, false)
			first, err := manager.Refresh(context.Background(), "orders_fast", false)
			if err != nil {
				t.Fatal(err)
			}
			manager.config.Acceleration.Datasets[0].SchemaEvolution = &catalog.SchemaEvolution{SafeWidening: true}
			fingerprint, err := manager.config.DatasetFingerprint("orders_fast")
			if err != nil {
				t.Fatal(err)
			}
			if fingerprint == first.Fingerprint {
				t.Fatal("policy change did not change fingerprint")
			}
			if lease, err := manager.store.Acquire(context.Background(), "orders_fast", fingerprint, time.Hour); !errors.Is(err, ErrFingerprintMismatch) {
				if lease != nil {
					_ = lease.Close()
				}
				t.Fatalf("changed policy did not fence old snapshot: %v", err)
			}
			if _, err = manager.Restore(context.Background(), "orders_fast", first.Generation, first.Generation); !errors.Is(err, ErrFingerprintMismatch) {
				t.Fatalf("changed policy restored old snapshot: %v", err)
			}
			source.schema = refreshEvolutionSchema(arrow.PrimitiveTypes.Int64, false)
			next, err := manager.Refresh(context.Background(), "orders_fast", false)
			if err != nil || next.Fingerprint != fingerprint {
				t.Fatalf("new policy refresh failed: %+v %v", next, err)
			}
		}
	}
}
