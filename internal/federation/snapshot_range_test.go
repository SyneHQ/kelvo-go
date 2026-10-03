// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package federation

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func snapshotRangeFixture(t *testing.T, data []byte, alter func(http.ResponseWriter, *http.Request, int64, int64) bool) (string, string, *atomic.Int64) {
	t.Helper()
	sum := sha256.Sum256(data)
	digest := hex.EncodeToString(sum[:])
	count := &atomic.Int64{}
	path := "/" + strings.Repeat("a", 64) + "/orders_fast"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count.Add(1)
		var start, end int64
		if r.Method != "GET" || r.URL.Path != path || r.Header.Get("If-Match") != `"`+digest+`"` || r.Header.Get("Accept-Encoding") != "identity" {
			t.Error("range lost its private capability or exact identity")
			w.WriteHeader(400)
			return
		}
		if n, err := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &start, &end); n != 2 || err != nil || start < 0 || end < start || end >= int64(len(data)) || end-start >= snapshotRangeRequest {
			t.Error("unbounded or missing range")
			w.WriteHeader(400)
			return
		}
		w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
		w.Header().Set("ETag", `"`+digest+`"`)
		if alter != nil && alter(w, r, start, end) {
			return
		}
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(data[start : end+1])
	}))
	t.Cleanup(server.Close)
	return server.URL + path, digest, count
}

func newSnapshotRangeForTest(t *testing.T, ctx context.Context, endpoint string, size int64, digest string) (*snapshotRange, *snapshotAllocator) {
	t.Helper()
	memory := &snapshotAllocator{limit: 128 << 10}
	reader, err := newSnapshotRange(ctx, endpoint, size, digest, memory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := reader.Close(); err != nil {
			t.Error(err)
		}
		if memory.used != 0 {
			t.Errorf("range cache retained %d logical bytes", memory.used)
		}
	})
	return reader, memory
}

func TestSnapshotRangeExactReadsCacheAndBounds(t *testing.T) {
	data := make([]byte, 3*snapshotRangeBlock+17)
	for i := range data {
		data[i] = byte(i % 251)
	}
	endpoint, digest, count := snapshotRangeFixture(t, data, nil)
	reader, memory := newSnapshotRangeForTest(t, context.Background(), endpoint, int64(len(data)), digest)
	for _, span := range [][2]int{{5, 9}, {100, 21}} {
		buffer := make([]byte, span[1])
		if n, err := reader.ReadAt(buffer, int64(span[0])); err != nil || n != len(buffer) || !bytes.Equal(buffer, data[span[0]:span[0]+span[1]]) {
			t.Fatalf("range changed exact bytes: %d %v", n, err)
		}
	}
	if count.Load() != 1 || memory.used != snapshotRangeBlock {
		t.Fatal("small reads did not share one bounded cache")
	}
	buffer := make([]byte, 2*snapshotRangeBlock+19)
	if n, err := reader.ReadAt(buffer, snapshotRangeBlock-7); n != len(buffer) || err != nil || !bytes.Equal(buffer, data[snapshotRangeBlock-7:snapshotRangeBlock-7+len(buffer)]) {
		t.Fatal("cross-block range changed bytes", n, err)
	}
	buffer = make([]byte, 10)
	if n, err := reader.ReadAt(buffer, int64(len(data)-4)); n != 4 || !errors.Is(err, io.EOF) || !bytes.Equal(buffer[:n], data[len(data)-4:]) {
		t.Fatal("partial final read lost EOF", n, err)
	}
	before := count.Load()
	if n, err := reader.ReadAt(nil, 0); n != 0 || err != nil {
		t.Fatal("empty read failed", n, err)
	}
	if _, err := reader.ReadAt(buffer, -1); err == nil {
		t.Fatal("negative offset accepted")
	}
	if n, err := reader.ReadAt(buffer, int64(len(data))); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatal("past-end read did not return EOF")
	}
	if count.Load() != before {
		t.Fatal("invalid bounds reached the parent")
	}
}

func TestSnapshotRangeDigestStreamsBoundedRequests(t *testing.T) {
	data := bytes.Repeat([]byte{42}, int(snapshotRangeRequest)+17)
	var corrupt atomic.Bool
	endpoint, digest, count := snapshotRangeFixture(t, data, func(w http.ResponseWriter, _ *http.Request, start, end int64) bool {
		if !corrupt.Load() || end != int64(len(data)-1) {
			return false
		}
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(data[start:end])
		_, _ = w.Write([]byte{data[end] ^ 1})
		return true
	})
	reader, memory := newSnapshotRangeForTest(t, context.Background(), endpoint, int64(len(data)), digest)
	if err := reader.verifyDigest(); err != nil {
		t.Fatal(err)
	}
	if count.Load() != 2 || memory.used != 0 {
		t.Fatal("digest scan was not bounded streaming", count.Load(), memory.used)
	}
	corrupt.Store(true)
	if err := reader.verifyDigest(); err == nil {
		t.Fatal("changed payload passed its recorded digest")
	}
}

func TestSnapshotRangeRejectsChangedOrIncompleteResponses(t *testing.T) {
	for _, mode := range []string{"full", "redirect", "etag", "range", "size", "encoding", "short", "duplicate-etag"} {
		t.Run(mode, func(t *testing.T) {
			data := bytes.Repeat([]byte{3}, 512)
			endpoint, digest, count := snapshotRangeFixture(t, data, func(w http.ResponseWriter, r *http.Request, start, end int64) bool {
				switch mode {
				case "full":
					w.WriteHeader(http.StatusOK)
					return true
				case "redirect":
					w.Header().Set("Location", "http://127.0.0.1:1/unexpected")
					w.WriteHeader(http.StatusTemporaryRedirect)
					return true
				case "etag":
					w.Header().Set("ETag", `"changed"`)
				case "range":
					w.Header().Set("Content-Range", "bytes 0-510/512")
				case "size":
					w.Header().Set("Content-Length", "511")
				case "encoding":
					w.Header().Set("Content-Encoding", "gzip")
				case "short":
					w.WriteHeader(http.StatusPartialContent)
					_, _ = w.Write(data[:20])
					return true
				case "duplicate-etag":
					w.Header().Add("ETag", `"duplicate"`)
				}
				return false
			})
			reader, _ := newSnapshotRangeForTest(t, context.Background(), endpoint, int64(len(data)), digest)
			_, err := reader.ReadAt(make([]byte, 8), 0)
			if err == nil || strings.Contains(err.Error(), endpoint) || count.Load() != 1 {
				t.Fatal("invalid range accepted, retried or exposed its capability", err, count.Load())
			}
		})
	}
}

func TestSnapshotRangeCloseInterruptsBlockedReadAndReleasesCache(t *testing.T) {
	entered := make(chan struct{})
	data := bytes.Repeat([]byte{1}, 512)
	endpoint, digest, _ := snapshotRangeFixture(t, data, func(w http.ResponseWriter, r *http.Request, _, _ int64) bool {
		w.WriteHeader(http.StatusPartialContent)
		w.(http.Flusher).Flush()
		close(entered)
		<-r.Context().Done()
		return true
	})
	reader, memory := newSnapshotRangeForTest(t, context.Background(), endpoint, int64(len(data)), digest)
	done := make(chan error, 1)
	go func() { _, err := reader.ReadAt(make([]byte, 8), 0); done <- err }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("range did not start")
	}
	closed := make(chan struct{})
	go func() { _ = reader.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("close left a blocked network read")
	}
	if err := <-done; !errors.Is(err, context.Canceled) || memory.used != 0 {
		t.Fatal("cancel did not release bounded range state", err, memory.used)
	}
}

func TestSnapshotRangeConcurrentReadsAndNoAmbientProxy(t *testing.T) {
	t.Setenv("HTTP_PROXY", "http://127.0.0.1:1")
	t.Setenv("ALL_PROXY", "http://127.0.0.1:1")
	data := bytes.Repeat([]byte{21}, 2*snapshotRangeBlock)
	endpoint, digest, _ := snapshotRangeFixture(t, data, nil)
	reader, _ := newSnapshotRangeForTest(t, context.Background(), endpoint, int64(len(data)), digest)
	var wait sync.WaitGroup
	for i := range 8 {
		wait.Go(func() {
			buffer := make([]byte, 17)
			if n, err := reader.ReadAt(buffer, int64(i*13)); err != nil || n != len(buffer) || !bytes.Equal(buffer, data[:len(buffer)]) {
				t.Error("concurrent range changed bytes", n, err)
			}
		})
	}
	wait.Wait()
}
