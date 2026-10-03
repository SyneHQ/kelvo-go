//go:build linux || darwin

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"bytes"
	"context"
	"errors"
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

func descriptorFixture() objectGenerationDescriptor {
	return objectGenerationDescriptor{Version: 1, Dataset: "events", Generation: strings.Repeat("a", 32), SchemaHash: strings.Repeat("b", 64), Rows: 1, Bytes: 100, Parts: []objectPart{{Rows: 1, Bytes: 100, SHA256: strings.Repeat("c", 64), ObjectVersion: "v1"}}}
}
func TestObjectDescriptorStrictDecodingAndTotals(t *testing.T) {
	base := descriptorFixture()
	valid, err := marshalObjectDescriptor(base)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeObjectDescriptor(valid); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*objectGenerationDescriptor){
		func(d *objectGenerationDescriptor) { d.Version = 2 }, func(d *objectGenerationDescriptor) { d.Dataset = "../other" }, func(d *objectGenerationDescriptor) { d.SchemaHash = "bad" },
		func(d *objectGenerationDescriptor) { d.Bytes++ }, func(d *objectGenerationDescriptor) { d.Rows++ }, func(d *objectGenerationDescriptor) { d.Parts = nil },
		func(d *objectGenerationDescriptor) { d.Parts[0].ObjectVersion = "" }, func(d *objectGenerationDescriptor) {
			d.Parts[0].Bytes = objectstore.MaxUploadBytes + 1
			d.Bytes = d.Parts[0].Bytes
		},
	} {
		d := descriptorFixture()
		change(&d)
		raw, _ := yaml.Marshal(d)
		if _, err := decodeObjectDescriptor(raw); err == nil {
			t.Fatalf("invalid descriptor accepted: %+v", d)
		}
	}
	for _, raw := range [][]byte{append(bytes.Clone(valid), []byte("unknown: forbidden\n")...), append(bytes.Clone(valid), []byte("version: 1\n")...), append(bytes.Clone(valid), []byte("---\nversion: 1\n")...), bytes.Replace(valid, []byte("dataset: events"), []byte("dataset: &identity events"), 1), []byte(strings.Repeat("x", int(objectDescriptorLimit+1)))} {
		if _, err := decodeObjectDescriptor(raw); err == nil {
			t.Fatal("unknown/duplicate/extra/anchored/oversize YAML accepted")
		}
	}
}

func descriptorParquet(t *testing.T, field string) ([]byte, *arrow.Schema) {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{{Name: field, Type: arrow.PrimitiveTypes.Int64}}, nil)
	builder := array.NewInt64Builder(memory.DefaultAllocator)
	defer builder.Release()
	builder.Append(7)
	values := builder.NewArray()
	defer values.Release()
	record := array.NewRecordBatch(schema, []arrow.Array{values}, 1)
	defer record.Release()
	var out bytes.Buffer
	sink := NewParquetSink(&out, query.DefaultLimits())
	defer sink.Abort()
	if err := sink.Schema(schema); err != nil {
		t.Fatal(err)
	}
	if err := sink.Write(record); err != nil {
		t.Fatal(err)
	}
	if err := sink.Finish(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes(), schema
}
func publishDescriptorFixture(t *testing.T, backend *objectBackend, client *recoveryObjectClient, generation string, secondField string) (*objectCommitted, Snapshot) {
	t.Helper()
	ctx := context.Background()
	first, schema := descriptorParquet(t, "id")
	second, _ := descriptorParquet(t, secondField)
	hash, err := SchemaFingerprint(schema)
	if err != nil {
		t.Fatal(err)
	}
	d := objectGenerationDescriptor{Version: 1, Dataset: "events", Generation: generation, SchemaHash: hash, Rows: 2, Bytes: int64(len(first) + len(second))}
	for i, data := range [][]byte{first, second} {
		digest := objectSHA256(data)
		info, err := client.Put(ctx, backend.key("events", multipartName(generation, i)), bytes.NewReader(data), int64(len(data)), digest, objectstore.Condition{Absent: true})
		if err != nil {
			t.Fatal(err)
		}
		d.Parts = append(d.Parts, objectPart{Rows: 1, Bytes: int64(len(data)), SHA256: digest, ObjectVersion: info.Version})
	}
	encoded, err := marshalObjectDescriptor(d)
	if err != nil {
		t.Fatal(err)
	}
	digest := objectSHA256(encoded)
	info, err := client.Put(ctx, backend.key("events", objectDescriptorName(generation)), bytes.NewReader(encoded), int64(len(encoded)), digest, objectstore.Condition{Absent: true})
	if err != nil {
		t.Fatal(err)
	}
	committed := &objectCommitted{Generation: generation, SchemaHash: hash, Fingerprint: "v1", Rows: d.Rows, Bytes: d.Bytes, SHA256: digest, RefreshedAt: time.Now().Add(-time.Minute).UTC(), Descriptor: &objectDescriptorRef{ObjectVersion: info.Version, Bytes: info.Size, PartCount: 2}}
	snapshot, err := backend.loadObjectSnapshot(ctx, "events", committed, time.Now(), client)
	if err != nil {
		t.Fatal(err)
	}
	return committed, snapshot
}
func TestObjectDescriptorExactVersionsAndEveryPart(t *testing.T) {
	backend, client := recoveryObjectBackend(t)
	committed, snapshot := publishDescriptorFixture(t, backend, client, strings.Repeat("d", 32), "id")
	if snapshot.Path != "" || snapshot.ObjectKey != "" || len(snapshot.Parts) != 2 {
		t.Fatal("multipart root exposed single path")
	}
	if _, err := backend.verifyObjectSnapshot(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}
	if err := backend.headObjectSnapshot(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}
	key := snapshot.Parts[1].ObjectKey
	client.mu.Lock()
	original := client.objects[key]
	bad := original
	bad.data = bytes.Clone(original.data)
	bad.data[4] ^= 1
	client.objects[key] = bad
	client.mu.Unlock()
	if _, err := backend.verifyObjectSnapshot(context.Background(), snapshot); err == nil {
		t.Fatal("nonfirst part corruption passed verification")
	}
	client.mu.Lock()
	client.objects[key] = original
	client.mu.Unlock()
	altered := *committed
	ref := *committed.Descriptor
	ref.ObjectVersion = "wrong-version"
	altered.Descriptor = &ref
	if _, err := backend.loadObjectSnapshot(context.Background(), "events", &altered, time.Now(), client); err == nil {
		t.Fatal("descriptor version mismatch accepted")
	}
	altered = *committed
	altered.Rows++
	if _, err := backend.loadObjectSnapshot(context.Background(), "events", &altered, time.Now(), client); err == nil {
		t.Fatal("descriptor totals mismatch accepted")
	}
	wrongRows := snapshot
	wrongRows.Parts = append([]SnapshotPart(nil), snapshot.Parts...)
	wrongRows.Parts[1].Rows++
	wrongRows.Rows++
	if _, err := backend.verifyObjectSnapshot(context.Background(), wrongRows); err == nil {
		t.Fatal("nonfirst actual footer rows not verified")
	}
	client.corruptPayloadGet = true
	if _, err := backend.loadObjectSnapshot(context.Background(), "events", committed, time.Now(), client); err == nil {
		t.Fatal("corrupt descriptor hash accepted")
	}
	client.corruptPayloadGet = false
	_, wrongSchema := publishDescriptorFixture(t, backend, client, strings.Repeat("e", 32), "changed")
	if _, err := backend.verifyObjectSnapshot(context.Background(), wrongSchema); err == nil {
		t.Fatal("nonfirst schema mismatch accepted")
	}
}
func TestObjectDescriptorCommitAlternatives(t *testing.T) {
	single := &objectCommitted{Generation: strings.Repeat("a", 32), Fingerprint: "v1", SHA256: strings.Repeat("b", 64), Rows: 1, Bytes: 100, RefreshedAt: time.Now(), ObjectVersion: "v1"}
	if err := validateObjectCommit(single); err != nil {
		t.Fatal(err)
	}
	multipart := *single
	multipart.ObjectVersion = ""
	multipart.SchemaHash = strings.Repeat("c", 64)
	multipart.Bytes = 8 << 30
	multipart.Descriptor = &objectDescriptorRef{ObjectVersion: "d1", Bytes: 100, PartCount: 2}
	if err := validateObjectCommit(&multipart); err != nil {
		t.Fatal(err)
	}
	multipart.ObjectVersion = "ambiguous"
	if err := validateObjectCommit(&multipart); !errors.Is(err, ErrCorrupt) {
		t.Fatal("single and multipart alternatives accepted together")
	}
}
