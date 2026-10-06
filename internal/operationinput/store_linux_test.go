//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package operationinput

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/SYNEHQ/kelvo-go/internal/exports"
	"github.com/SYNEHQ/kelvo-go/operations"
)

var identity = exports.Identity{Owner: "user-a", AuthorizationSHA256: strings.Repeat("a", 64)}

func testStore(t *testing.T, entries int, maximum int64) (*Store, Config) {
	t.Helper()
	config := Config{Storage: exports.Config{Directory: filepath.Join(t.TempDir(), "inputs"), Tenant: "team-a", MaxEntries: entries, MaxStoredBytes: 64 << 20, MaxTTL: time.Hour}, MaxInputBytes: maximum}
	s, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s, config
}

func putTest(t *testing.T, s *Store, format Format, payload []byte) operations.InputRef {
	t.Helper()
	ref, err := s.Put(context.Background(), identity, time.Now().Add(10*time.Minute), format, bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	return ref
}

func TestSealedInputRoundTripAndReopen(t *testing.T) {
	s, config := testStore(t, 4, 4*ChunkBytes)
	payload := bytes.Repeat([]byte{0, 1, 2, 3, 254, 255, 10}, ChunkBytes/2)
	ref := putTest(t, s, NativeInput, payload)
	sum := sha256.Sum256(payload)
	if ref.Bytes != int64(len(payload)) || ref.SHA256 != hex.EncodeToString(sum[:]) || ref.Format != string(NativeInput) || ref.Validate() != nil {
		t.Fatalf("input reference did not describe raw bytes: %+v", ref)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	actual, err := reopened.Load(context.Background(), identity, ref)
	if err != nil || !bytes.Equal(actual, payload) {
		t.Fatalf("sealed input changed after reopen: %v", err)
	}
	var output bytes.Buffer
	if err := reopened.Stream(context.Background(), identity, ref, &output); err != nil || !bytes.Equal(output.Bytes(), payload) {
		t.Fatalf("verified stream changed payload: %v", err)
	}
}

func TestCanonicalOperationRequest(t *testing.T) {
	s, _ := testStore(t, 4, 1<<20)
	request := operations.Request{Version: operations.Version, Kind: operations.ConnectionTest, Connection: operations.ConnectionRef{ID: "saved-a"}}
	ref, err := s.PutRequest(context.Background(), identity, time.Now().Add(time.Minute), request)
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.Load(context.Background(), identity, ref)
	want, encodeErr := operations.Encode(request)
	if err != nil || encodeErr != nil || !bytes.Equal(got, want) {
		t.Fatalf("request encoding changed in storage: %v", err)
	}
	for _, payload := range [][]byte{append(append([]byte(nil), want...), ' '), []byte(`{"version":1,"unknown":"field"}`)} {
		ref, err := s.Put(context.Background(), identity, time.Now().Add(time.Minute), OperationRequest, bytes.NewReader(payload))
		if !errors.Is(err, ErrInvalid) || ref.ID != "" {
			t.Fatal("noncanonical or invalid request was sealed")
		}
	}
}

func TestIdentityAndAlteredReferenceCannotReleaseBytes(t *testing.T) {
	s, _ := testStore(t, 4, 2*ChunkBytes)
	ref := putTest(t, s, MigrationPlan, bytes.Repeat([]byte("x"), ChunkBytes+100))
	for _, other := range []exports.Identity{
		{Owner: "user-b", AuthorizationSHA256: identity.AuthorizationSHA256},
		{Owner: identity.Owner, AuthorizationSHA256: strings.Repeat("b", 64)},
		{},
	} {
		var output bytes.Buffer
		if err := s.Stream(context.Background(), other, ref, &output); err == nil || output.Len() != 0 {
			t.Fatal("input bytes crossed an authorization boundary")
		}
	}
	for _, mutate := range []func(*operations.InputRef){
		func(r *operations.InputRef) { r.ID = "../../input" },
		func(r *operations.InputRef) { r.SHA256 = strings.Repeat("0", 64) },
		func(r *operations.InputRef) { r.Bytes-- },
		func(r *operations.InputRef) { r.Bytes++ },
		func(r *operations.InputRef) { r.Format = string(NativeInput) },
		func(r *operations.InputRef) { r.Format = "unrecognized" },
	} {
		altered := ref
		mutate(&altered)
		var output bytes.Buffer
		if err := s.Stream(context.Background(), identity, altered, &output); err == nil || output.Len() != 0 {
			t.Fatal("altered reference released a partial input")
		}
	}
}

func TestTruncatedStorageFailsBeforeFirstDestinationWrite(t *testing.T) {
	s, config := testStore(t, 2, 2*ChunkBytes)
	ref := putTest(t, s, IngestionBatch, bytes.Repeat([]byte("a"), ChunkBytes+1))
	part := filepath.Join(config.Storage.Directory, ref.ID, "part-0001.arrow")
	raw, err := os.ReadFile(part)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(part, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(part, raw[:len(raw)-1], 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(part, 0400); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := s.Stream(context.Background(), identity, ref, &output); err == nil || output.Len() != 0 {
		t.Fatal("truncated final part released an earlier part")
	}
}

func TestWrongArrowSchemaIsNotAnInput(t *testing.T) {
	s, _ := testStore(t, 2, ChunkBytes)
	metadata := arrow.NewMetadata([]string{"kelvo_operation_input", "format"}, []string{"1", string(NativeInput)})
	schema := arrow.NewSchema([]arrow.Field{{Name: "chunk", Type: arrow.PrimitiveTypes.Int64}}, &metadata)
	w, err := s.storage.Begin(context.Background(), exports.Request{Identity: identity, ExpiresAt: time.Now().Add(time.Minute), Limits: inputLimits(s.maximum)}, schema)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	builder := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	builder.Field(0).(*array.Int64Builder).Append(123)
	batch := builder.NewRecordBatch()
	builder.Release()
	err = w.Write(context.Background(), batch)
	batch.Release()
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := w.Commit(context.Background(), identity)
	if err != nil {
		t.Fatal(err)
	}
	ref := operations.InputRef{ID: manifest.ID, SHA256: strings.Repeat("a", 64), Bytes: 8, Format: string(NativeInput)}
	if payload, err := s.Load(context.Background(), identity, ref); err == nil || payload != nil {
		t.Fatal("a valid export with a different schema became executable input")
	}
}

type countReader struct{ reads int }

func (r *countReader) Read([]byte) (int, error) { r.reads++; return 0, io.EOF }

func TestCapacityReservationPrecedesInputAndCleanupReleasesIt(t *testing.T) {
	s, _ := testStore(t, 1, ChunkBytes)
	ref := putTest(t, s, WatchCheckpoint, []byte("checkpoint"))
	reader := &countReader{}
	_, err := s.Put(context.Background(), identity, time.Now().Add(time.Minute), NativeInput, reader)
	if !errors.Is(err, exports.ErrLimit) || reader.reads != 0 {
		t.Fatal("full store consumed input before reserving capacity")
	}
	if err := s.Cancel(context.Background(), identity, ref); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Load(context.Background(), identity, ref); !errors.Is(err, exports.ErrUnavailable) {
		t.Fatal("cancelled input remained readable")
	}
	_, err = s.Put(context.Background(), identity, time.Now().Add(time.Minute), NativeInput, reader)
	if !errors.Is(err, exports.ErrLimit) || reader.reads != 0 {
		t.Fatal("cancelled input released reservation before cleanup")
	}
	cleaned, err := s.Cleanup(context.Background(), 1)
	if err != nil || cleaned.Removed != 1 {
		t.Fatalf("cancelled input cleanup=%+v err=%v", cleaned, err)
	}
	putTest(t, s, NativeInput, []byte("new"))
}

func TestInputLimitAndCancellation(t *testing.T) {
	s, _ := testStore(t, 4, ChunkBytes)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	reader := &countReader{}
	if _, err := s.Put(ctx, identity, time.Now().Add(time.Minute), NativeInput, reader); !errors.Is(err, context.Canceled) || reader.reads != 0 {
		t.Fatal("cancelled input was consumed")
	}
	if _, err := s.Put(context.Background(), identity, time.Now().Add(time.Minute), NativeInput, bytes.NewReader(make([]byte, ChunkBytes+1))); !errors.Is(err, ErrLimit) {
		t.Fatalf("oversized input accepted: %v", err)
	}
	ref := putTest(t, s, NativeInput, bytes.Repeat([]byte("x"), ChunkBytes))
	if payload, err := s.Load(ctx, identity, ref); !errors.Is(err, context.Canceled) || payload != nil {
		t.Fatal("cancelled load returned payload")
	}
	if _, err := s.Put(context.Background(), identity, time.Now().Add(time.Minute), NativeInput, bytes.NewReader(nil)); !errors.Is(err, ErrInvalid) {
		t.Fatal("empty operation input accepted")
	}
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

func TestStreamRechecksRevocationBeforeEachWrite(t *testing.T) {
	s, _ := testStore(t, 2, 2*ChunkBytes)
	ref := putTest(t, s, NativeInput, bytes.Repeat([]byte("z"), ChunkBytes+1))
	written := 0
	err := s.Stream(context.Background(), identity, ref, writerFunc(func(p []byte) (int, error) {
		written += len(p)
		if err := s.storage.Cancel(context.Background(), ref.ID, identity); err != nil {
			t.Fatal(err)
		}
		return len(p), nil
	}))
	if !errors.Is(err, exports.ErrUnavailable) || written != ChunkBytes {
		t.Fatalf("revoked input stream continued: bytes=%d err=%v", written, err)
	}
}

func TestExpiredInputCannotBeLoaded(t *testing.T) {
	s, _ := testStore(t, 2, ChunkBytes)
	expiry := time.Now().Add(2 * time.Second)
	ref, err := s.Put(context.Background(), identity, expiry, WatchCheckpoint, bytes.NewReader([]byte("checkpoint")))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(max(0, time.Until(expiry)+time.Millisecond))
	if payload, err := s.Load(context.Background(), identity, ref); !errors.Is(err, exports.ErrUnavailable) || payload != nil {
		t.Fatal("expired operation input remained readable")
	}
}
