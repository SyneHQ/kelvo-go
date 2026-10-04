//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/acceleration"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/objectstore"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
)

type objectPrincipalBlob struct {
	data []byte
	info objectstore.Info
}

// This fixture is immutable after construction. Every provider request must
// retain its exact key/version and bounded range; full reads and writes fail.
type objectPrincipalRanges struct {
	mu             sync.Mutex
	blobs          map[string]objectPrincipalBlob
	reads          map[string]int
	active         int
	closed         int
	forbiddenCalls int
}

func (c *objectPrincipalRanges) Get(context.Context, string, string) (io.ReadCloser, objectstore.Info, error) {
	c.mu.Lock()
	c.forbiddenCalls++
	c.mu.Unlock()
	return nil, objectstore.Info{}, errors.New("unbounded fixture read forbidden")
}

func (c *objectPrincipalRanges) Head(context.Context, string, string) (objectstore.Info, error) {
	c.mu.Lock()
	c.forbiddenCalls++
	c.mu.Unlock()
	return objectstore.Info{}, errors.New("fixture metadata must stay pinned")
}

func (c *objectPrincipalRanges) Put(context.Context, string, io.ReadSeeker, int64, string, objectstore.Condition) (objectstore.Info, error) {
	c.mu.Lock()
	c.forbiddenCalls++
	c.mu.Unlock()
	return objectstore.Info{}, errors.New("immutable fixture write forbidden")
}

func (c *objectPrincipalRanges) Close() {
	c.mu.Lock()
	c.closed++
	c.mu.Unlock()
}

func (c *objectPrincipalRanges) GetRange(ctx context.Context, key, version string, offset, length int64) (io.ReadCloser, objectstore.Info, error) {
	if err := ctx.Err(); err != nil {
		return nil, objectstore.Info{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	blob, ok := c.blobs[key]
	if c.closed != 0 || !ok || version != blob.info.Version || offset < 0 || length < 1 || offset > blob.info.Size || length > blob.info.Size-offset {
		c.forbiddenCalls++
		return nil, objectstore.Info{}, errors.New("fixture range escaped acquired generation")
	}
	c.reads[key]++
	c.active++
	return &objectPrincipalBody{Reader: bytes.NewReader(blob.data[offset : offset+length]), owner: c}, blob.info, nil
}

type objectPrincipalBody struct {
	*bytes.Reader
	owner *objectPrincipalRanges
	once  sync.Once
}

func (b *objectPrincipalBody) Close() error {
	b.once.Do(func() {
		b.owner.mu.Lock()
		b.owner.active--
		b.owner.mu.Unlock()
	})
	return nil
}

type objectPrincipalPartSink struct {
	*acceleration.ParquetSink
	start, end int64
}

func (s objectPrincipalPartSink) Write(record arrow.RecordBatch) error {
	if record.NumRows() != 1024 {
		return errors.New("unexpected original fixture batch")
	}
	part := record.NewSlice(s.start, s.end)
	defer part.Release()
	return s.ParquetSink.Write(part)
}

func objectPrincipalConfig(t *testing.T, local catalog.Config, partCount int) catalog.Config {
	t.Helper()
	backend, err := acceleration.OpenBackend(*local.Acceleration)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = backend.Close() })
	fingerprint, err := local.DatasetFingerprint("orders_fast")
	if err != nil {
		t.Fatal(err)
	}
	lease, err := backend.Acquire(context.Background(), "orders_fast", fingerprint, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lease.Close() })
	original := lease.Snapshot
	if original.Rows != 1024 || original.SchemaHash == "" || len(original.Parts) != 0 {
		t.Fatal("original fixture generation changed")
	}
	storage := catalog.ObjectStorage{ObjectLocation: catalog.ObjectLocation{
		Provider: "s3", Endpoint: "https://immutable-fixture.invalid", Bucket: "principal-fixture", Prefix: "snapshots", Region: "us-east-1",
	}}
	client := &objectPrincipalRanges{blobs: make(map[string]objectPrincipalBlob), reads: make(map[string]int)}
	snapshot := acceleration.Snapshot{Dataset: original.Dataset, Generation: original.Generation,
		SchemaHash: original.SchemaHash, Rows: original.Rows}
	for index := 0; index < partCount; index++ {
		var data []byte
		if partCount == 1 {
			data, err = os.ReadFile(original.Path)
			if err != nil {
				t.Fatal(err)
			}
		} else {
			var encoded bytes.Buffer
			sink := acceleration.NewParquetSink(&encoded, query.DefaultLimits())
			defer sink.Abort()
			part := objectPrincipalPartSink{ParquetSink: sink, start: int64(index * 512), end: int64((index + 1) * 512)}
			if _, err := (snapshotPrincipalRows{}).Execute(context.Background(), query.Request{}, part); err != nil {
				t.Fatal(err)
			}
			if err := sink.Finish(); err != nil {
				t.Fatal(err)
			}
			data = append([]byte(nil), encoded.Bytes()...)
		}
		schema, err := acceleration.ReadParquetSchema(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			t.Fatal(err)
		}
		hash, err := acceleration.SchemaFingerprint(schema)
		if err != nil || hash != original.SchemaHash {
			t.Fatal("object part lost the original Arrow schema", err)
		}
		digest := sha256.Sum256(data)
		sha := hex.EncodeToString(digest[:])
		key := storage.Prefix + "/a/" + snapshot.Dataset + "/" + snapshot.Generation + ".parquet"
		if partCount > 1 {
			key = fmt.Sprintf("%s/a/%s/%s-part-%04d.parquet", storage.Prefix, snapshot.Dataset, snapshot.Generation, index)
		}
		version := fmt.Sprintf("immutable-version-%d", index)
		client.blobs[key] = objectPrincipalBlob{data: data, info: objectstore.Info{Size: int64(len(data)), Version: version, SHA256: sha}}
		snapshot.Bytes += int64(len(data))
		if partCount == 1 {
			if sha != original.SHA256 || snapshot.Bytes != original.Bytes {
				t.Fatal("single-object fixture changed its acquired payload")
			}
			snapshot.ObjectKey, snapshot.ObjectVersion, snapshot.SHA256 = key, version, sha
			snapshot.Path, err = storage.ObjectLocation.URI(key)
			if err != nil {
				t.Fatal(err)
			}
		} else {
			snapshot.Parts = append(snapshot.Parts, acceleration.SnapshotPart{ObjectKey: key, ObjectVersion: version, Rows: 512, Bytes: int64(len(data)), SHA256: sha})
		}
	}
	if partCount > 1 {
		// The synthetic acquisition attests these immutable ordered parts. No
		// cloud manifest publication or conditional-write behavior is claimed.
		descriptor, err := json.Marshal(snapshot.Parts)
		if err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(descriptor)
		snapshot.SHA256 = hex.EncodeToString(digest[:])
	}
	sources, release, err := acceleration.OpenObjectRangesWithClient(context.Background(), storage, []acceleration.Snapshot{snapshot}, client)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		release() // Join the real parent bridge before generation/backend release.
		client.mu.Lock()
		defer client.mu.Unlock()
		if client.active != 0 || client.closed != 1 || client.forbiddenCalls != 0 || len(client.reads) != partCount {
			t.Error("object reads escaped provenance or retained provider bodies", client.active, client.closed, client.forbiddenCalls, len(client.reads))
		}
	})
	source := sources[original.Dataset]
	scan, err := (catalog.SnapshotScanLimits{MaxRows: 1024, MaxBytes: 1 << 20}).Effective()
	if err != nil {
		t.Fatal(err)
	}
	read := &catalog.ObjectSnapshotRead{Dataset: snapshot.Dataset, Generation: snapshot.Generation, SchemaSHA256: snapshot.SchemaHash, Scan: scan}
	if partCount == 1 {
		read.Parts = []catalog.ObjectSnapshotPart{{URL: source.Range.URL, Rows: snapshot.Rows, Bytes: snapshot.Bytes, SHA256: snapshot.SHA256}}
	} else {
		for index, part := range snapshot.Parts {
			read.Parts = append(read.Parts, catalog.ObjectSnapshotPart{URL: source.Ranges[index].URL, Rows: part.Rows, Bytes: part.Bytes, SHA256: part.SHA256})
		}
	}
	source.ObjectSnapshot = read
	if err := source.ValidateObjectSnapshot(); err != nil {
		t.Fatal(err)
	}
	envelope, err := json.Marshal(source)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{storage.Endpoint, storage.Bucket, storage.Prefix + "/a/", "immutable-version-"} {
		if strings.Contains(string(envelope), forbidden) {
			t.Fatal("provider identity escaped the parent range bridge")
		}
	}
	return catalog.Config{Sources: []catalog.Source{source}}
}

// Uses real original-schema Parquet, the production parent range bridge, and
// the built Landlock child through the authenticated gateway relay. Object
// acquisition and gateway metadata CAS remain controlled immutable fixtures;
// this gate does not claim a live provider or distributed revocation/lease HA.
func TestObjectSnapshotPrincipalRealWorkerRevocationWithholdsEOS(t *testing.T) {
	for _, layout := range []struct {
		name  string
		parts int
	}{{"single", 1}, {"multipart", 2}} {
		t.Run(layout.name, func(t *testing.T) {
			runSnapshotPrincipalRealWorkerRevocation(t, func(t *testing.T, local catalog.Config) catalog.Config {
				return objectPrincipalConfig(t, local, layout.parts)
			})
		})
	}
}
