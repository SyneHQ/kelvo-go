//go:build linux || darwin

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/objectstore"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"go.yaml.in/yaml/v3"
)

type multipartRemoteObjects struct{ *fakeSnapshotObjects }

func (c *multipartRemoteObjects) GetRange(ctx context.Context, key, version string, start, length int64) (io.ReadCloser, objectstore.Info, error) {
	body, info, err := c.Get(ctx, key, version)
	if err != nil {
		return nil, info, err
	}
	defer body.Close()
	data, err := io.ReadAll(body)
	if err != nil {
		return nil, info, err
	}
	if start < 0 || length < 1 || start > int64(len(data)) || length > int64(len(data))-start {
		return nil, info, errors.New("invalid test range")
	}
	return io.NopCloser(bytes.NewReader(data[start : start+length])), info, nil
}
func remoteMultipartFixture(t *testing.T) (*objectBackend, *multipartRemoteObjects) {
	t.Helper()
	client := &multipartRemoteObjects{newFakeSnapshotObjects()}
	backend, err := NewObjectBackend(testObjectConfig(t), client)
	if err != nil {
		t.Fatal(err)
	}
	remote := backend.(*objectBackend)
	remote.pollInterval = time.Millisecond
	t.Cleanup(func() { remote.Close() })
	return remote, client
}
func remoteMultipartOptions() MultipartOptions {
	return MultipartOptions{MaxParts: 4, MaxPartBytes: 1 << 20, MaxTotalBytes: 8 << 20}
}
func writeRemoteMultipartPart(t *testing.T, writer MultipartRefreshWriter, column string, count, declared int64) error {
	t.Helper()
	file, err := writer.NewPart()
	if err != nil {
		return err
	}
	schema := arrow.NewSchema([]arrow.Field{{Name: column, Type: arrow.PrimitiveTypes.Int64}}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	for i := int64(0); i < count; i++ {
		builder.Field(0).(*array.Int64Builder).Append(i)
	}
	record := builder.NewRecordBatch()
	defer record.Release()
	sink := NewParquetSink(file, query.DefaultLimits())
	defer sink.Abort()
	if err = sink.Schema(schema); err != nil {
		return err
	}
	if count > 0 {
		if err = sink.Write(record); err != nil {
			return err
		}
	}
	if err = sink.Finish(); err != nil {
		return err
	}
	return writer.SealPart(declared)
}
func beginRemoteMultipart(t *testing.T, backend *objectBackend) MultipartRefreshWriter {
	t.Helper()
	writer, err := backend.BeginMultipart(context.Background(), "events", remoteMultipartOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { writer.Abort() })
	return writer
}
func requireRemoteGeneration(t *testing.T, backend *objectBackend, want string) {
	t.Helper()
	snapshot, err := backend.Status(context.Background(), "events")
	if err != nil || snapshot.Generation != want {
		t.Fatalf("current generation changed: %s %v", snapshot.Generation, err)
	}
}
func TestObjectMultipartCommitStagesOnePartAndPublishesDescriptor(t *testing.T) {
	backend, client := remoteMultipartFixture(t)
	writer := beginRemoteMultipart(t, backend)
	tx := writer.(*objectMultipartTransaction)
	for _, rows := range []int64{2, 3} {
		if err := writeRemoteMultipartPart(t, writer, "id", rows, rows); err != nil {
			t.Fatal(err)
		}
		if tx.base.local.File() != nil {
			t.Fatal("sealed local file remained open")
		}
		if _, err := os.Stat(tx.base.local.dir.Name() + "/" + tx.base.local.stage); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("sealed local bytes retained: %v", err)
		}
		if _, err := backend.Status(context.Background(), "events"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("part exposed before root publication: %v", err)
		}
	}
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	if err := writer.SetSchema(schema); err != nil {
		t.Fatal(err)
	}
	snapshot, err := writer.Commit("config-v1")
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Path != "" || snapshot.ObjectKey != "" || snapshot.ObjectVersion != "" || snapshot.Rows != 5 || len(snapshot.Parts) != 2 {
		t.Fatalf("unexpected snapshot: %+v", snapshot)
	}
	for i, p := range snapshot.Parts {
		if p.Path != "" || p.ObjectKey != backend.key("events", multipartName(snapshot.Generation, i)) || p.ObjectVersion == "" {
			t.Fatal("invalid remote part identity")
		}
	}
	state, err := backend.readState(context.Background(), "events", client)
	if err != nil {
		t.Fatal(err)
	}
	if state.manifest.Version != 4 || state.manifest.Writer != nil || state.manifest.Committed.Descriptor == nil || state.manifest.Committed.ObjectVersion != "" {
		t.Fatal("invalid v4 root")
	}
	committed := state.manifest.Committed
	body, info, err := client.Get(context.Background(), backend.key("events", objectDescriptorName(snapshot.Generation)), committed.Descriptor.ObjectVersion)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(body)
	body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if objectSHA256(data) != snapshot.SHA256 || int64(len(data)) != committed.Descriptor.Bytes || info.Version != committed.Descriptor.ObjectVersion {
		t.Fatal("descriptor reference mismatch")
	}
	var descriptor objectGenerationDescriptor
	if err = yaml.Unmarshal(data, &descriptor); err != nil {
		t.Fatal(err)
	}
	if descriptor.Rows != 5 || descriptor.Bytes != snapshot.Bytes || len(descriptor.Parts) != 2 || descriptor.SchemaHash != snapshot.SchemaHash {
		t.Fatal("descriptor aggregate mismatch")
	}
	if got, err := backend.Verify(context.Background(), "events"); err != nil || got.Generation != snapshot.Generation {
		t.Fatalf("verify multipart: %v", err)
	}
	requireRemoteGeneration(t, backend, snapshot.Generation)
}
func TestObjectMultipartEmptyGeneration(t *testing.T) {
	backend, _ := remoteMultipartFixture(t)
	writer := beginRemoteMultipart(t, backend)
	if err := writeRemoteMultipartPart(t, writer, "id", 0, 0); err != nil {
		t.Fatal(err)
	}
	snapshot, err := writer.Commit("config-v1")
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Rows != 0 || len(snapshot.Parts) != 1 || snapshot.Bytes <= 0 {
		t.Fatal("empty generation lost schema")
	}
	if _, err = backend.Verify(context.Background(), "events"); err != nil {
		t.Fatal(err)
	}
}
func TestObjectMultipartSealFailurePoisonsPublication(t *testing.T) {
	for _, mode := range []string{"rows", "schema", "bytes", "upload", "head"} {
		t.Run(mode, func(t *testing.T) {
			backend, client := remoteMultipartFixture(t)
			prior := writeObjectSnapshot(t, backend, "old snapshot")
			options := remoteMultipartOptions()
			if mode == "bytes" {
				options.MaxPartBytes = 1
			}
			writer, err := backend.BeginMultipart(context.Background(), "events", options)
			if err != nil {
				t.Fatal(err)
			}
			defer writer.Abort()
			if mode == "schema" {
				if err = writeRemoteMultipartPart(t, writer, "id", 2, 2); err != nil {
					t.Fatal(err)
				}
			}
			client.mu.Lock()
			if mode == "upload" {
				client.failPayload = true
			}
			if mode == "head" {
				client.headFault = "digest"
			}
			client.mu.Unlock()
			column := "id"
			if mode == "schema" {
				column = "changed"
			}
			declared := int64(2)
			if mode == "rows" {
				declared = 99
			}
			if err = writeRemoteMultipartPart(t, writer, column, 2, declared); err == nil {
				t.Fatal("invalid seal succeeded")
			}
			client.mu.Lock()
			client.failPayload = false
			client.headFault = ""
			client.mu.Unlock()
			if _, err = writer.Commit("config-v1"); err == nil {
				t.Fatal("failed part published truncated generation")
			}
			requireRemoteGeneration(t, backend, prior.Generation)
		})
	}
}
func TestObjectMultipartDescriptorFailurePreservesCurrentAndParts(t *testing.T) {
	backend, client := remoteMultipartFixture(t)
	prior := writeObjectSnapshot(t, backend, "old snapshot")
	writer := beginRemoteMultipart(t, backend)
	if err := writeRemoteMultipartPart(t, writer, "id", 2, 2); err != nil {
		t.Fatal(err)
	}
	tx := writer.(*objectMultipartTransaction)
	key := backend.key("events", multipartName(tx.base.local.generation, 0))
	client.mu.Lock()
	client.failPayload = true
	client.mu.Unlock()
	if _, err := writer.Commit("config-v1"); err == nil {
		t.Fatal("failed descriptor published")
	}
	client.mu.Lock()
	client.failPayload = false
	_, retained := client.objects[key]
	client.mu.Unlock()
	if !retained {
		t.Fatal("unpublished remote part deleted")
	}
	requireRemoteGeneration(t, backend, prior.Generation)
}
func TestObjectMultipartLeaseLossCannotPublish(t *testing.T) {
	backend, client := remoteMultipartFixture(t)
	prior := writeObjectSnapshot(t, backend, "old snapshot")
	writer := beginRemoteMultipart(t, backend)
	if err := writeRemoteMultipartPart(t, writer, "id", 2, 2); err != nil {
		t.Fatal(err)
	}
	owner := strings.Repeat("b", 32)
	client.mutateManifest(t, backend.key("events", storeManifestName), func(m *objectManifest) {
		m.Writer = &objectWriterLease{Owner: owner, ExpiresAt: time.Now().Add(time.Hour)}
	})
	if _, err := writer.Commit("config-v1"); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale writer published: %v", err)
	}
	state, err := backend.readState(context.Background(), "events", client)
	if err != nil {
		t.Fatal(err)
	}
	if state.manifest.Writer == nil || state.manifest.Writer.Owner != owner {
		t.Fatal("stale writer cleared new owner")
	}
	requireRemoteGeneration(t, backend, prior.Generation)
}
func TestObjectMultipartAmbiguousPublication(t *testing.T) {
	for _, mode := range []string{"before", "after", "after-unavailable"} {
		t.Run(mode, func(t *testing.T) {
			backend, client := remoteMultipartFixture(t)
			prior := writeObjectSnapshot(t, backend, "old snapshot")
			writer := beginRemoteMultipart(t, backend)
			generation := writer.(*objectMultipartTransaction).base.local.generation
			if err := writeRemoteMultipartPart(t, writer, "id", 2, 2); err != nil {
				t.Fatal(err)
			}
			client.mu.Lock()
			client.publicationError = mode
			client.mu.Unlock()
			snapshot, err := writer.Commit("config-v1")
			if mode == "after" {
				if err != nil || snapshot.Generation != generation || len(snapshot.Parts) != 1 {
					t.Fatalf("committed descriptor not reconciled: %v", err)
				}
			} else if !errors.Is(err, ErrPublicationUnknown) {
				t.Fatalf("ambiguous publication guessed: %v", err)
			}
			client.mu.Lock()
			client.publicationError = ""
			client.unavailable = false
			client.mu.Unlock()
			want := generation
			if mode == "before" {
				want = prior.Generation
			}
			requireRemoteGeneration(t, backend, want)
		})
	}
}
func TestObjectMultipartOptionsAndDescriptorEquality(t *testing.T) {
	for _, options := range []MultipartOptions{{}, {MaxParts: 257, MaxPartBytes: 1, MaxTotalBytes: 1}, {MaxParts: 1, MaxPartBytes: objectstore.MaxUploadBytes + 1, MaxTotalBytes: 1}, {MaxParts: 1, MaxPartBytes: 1, MaxTotalBytes: 256*objectstore.MaxUploadBytes + 1}} {
		if validateObjectMultipartOptions(options) == nil {
			t.Fatal("unsafe remote multipart options accepted")
		}
	}
	if err := validateObjectMultipartOptions(remoteMultipartOptions()); err != nil {
		t.Fatal("total budget may exceed reachable part budget", err)
	}
	a := &objectDescriptorRef{ObjectVersion: "v1", Bytes: 100, PartCount: 2}
	b := *a
	if !sameObjectDescriptor(a, &b) {
		t.Fatal("equal descriptor refs differ")
	}
	b.ObjectVersion = "v2"
	if sameObjectDescriptor(a, &b) {
		t.Fatal("descriptor version excluded from equality")
	}
	b = *a
	b.Bytes++
	if sameObjectDescriptor(a, &b) {
		t.Fatal("descriptor bytes excluded from equality")
	}
	b = *a
	b.PartCount++
	if sameObjectDescriptor(a, &b) {
		t.Fatal("descriptor parts excluded from equality")
	}
}

func TestObjectMultipartRootVersionAndCompleteCommitEquality(t *testing.T) {
	committed := &objectCommitted{Generation: strings.Repeat("a", 32), Fingerprint: "config-v1", SHA256: strings.Repeat("b", 64), SchemaHash: strings.Repeat("c", 64), Rows: 2, Bytes: 100, RefreshedAt: time.Now().UTC(), Descriptor: &objectDescriptorRef{ObjectVersion: "descriptor-v1", Bytes: 500, PartCount: 2}}
	manifest := objectManifest{Version: 4, Dataset: "events", Committed: committed}
	if err := validateObjectManifest(manifest, "events"); err != nil {
		t.Fatal(err)
	}
	for _, version := range []int{2, 3} {
		manifest.Version = version
		if err := validateObjectManifest(manifest, "events"); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("legacy root accepted descriptor: %v", err)
		}
	}
	copyCommit := *committed
	copyDescriptor := *committed.Descriptor
	copyCommit.Descriptor = &copyDescriptor
	if !sameObjectCommit(committed, &copyCommit) {
		t.Fatal("same committed descriptor differs")
	}
	copyDescriptor.ObjectVersion = "descriptor-v2"
	if sameObjectCommit(committed, &copyCommit) {
		t.Fatal("different descriptor revision reconciled as success")
	}
}

func TestObjectMultipartCanceledAfterUploadCannotPublish(t *testing.T) {
	backend, _ := remoteMultipartFixture(t)
	prior := writeObjectSnapshot(t, backend, "old snapshot")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	writer, err := backend.BeginMultipart(ctx, "events", remoteMultipartOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Abort()
	if err = writeRemoteMultipartPart(t, writer, "id", 2, 2); err != nil {
		t.Fatal(err)
	}
	cancel()
	if _, err = writer.Commit("config-v1"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled upload published: %v", err)
	}
	requireRemoteGeneration(t, backend, prior.Generation)
}
