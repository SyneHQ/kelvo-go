// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/objectstore"
)

type rangeFixtureClient struct {
	snapshot       Snapshot
	mode           string
	gate           <-chan struct{}
	started        chan struct{}
	calls          atomic.Int32
	forbiddenCalls atomic.Int32
	closed         atomic.Int32
	bodiesClosed   atomic.Int32
	active         atomic.Int32
	maximumActive  atomic.Int32
	maximumRead    atomic.Int32
}

func (client *rangeFixtureClient) Get(context.Context, string, string) (io.ReadCloser, objectstore.Info, error) {
	client.forbiddenCalls.Add(1)
	return nil, objectstore.Info{}, errors.New("full reads are forbidden")
}
func (client *rangeFixtureClient) Head(context.Context, string, string) (objectstore.Info, error) {
	client.forbiddenCalls.Add(1)
	return objectstore.Info{}, errors.New("HEAD must use acquired metadata")
}
func (client *rangeFixtureClient) Put(context.Context, string, io.ReadSeeker, int64, string, objectstore.Condition) (objectstore.Info, error) {
	client.forbiddenCalls.Add(1)
	return objectstore.Info{}, errors.New("writes are forbidden")
}
func (client *rangeFixtureClient) Close() { client.closed.Add(1) }

func (client *rangeFixtureClient) GetRange(ctx context.Context, key, version string, offset, length int64) (io.ReadCloser, objectstore.Info, error) {
	client.calls.Add(1)
	if key != client.snapshot.ObjectKey || version != client.snapshot.ObjectVersion || offset < 0 || length <= 0 || offset+length > client.snapshot.Bytes {
		return nil, objectstore.Info{}, errors.New("range escaped the selected generation")
	}
	active := client.active.Add(1)
	defer client.active.Add(-1)
	updateRangeMaximum(&client.maximumActive, active)
	if client.started != nil {
		client.started <- struct{}{}
	}
	if client.gate != nil {
		select {
		case <-client.gate:
		case <-ctx.Done():
			return nil, objectstore.Info{}, ctx.Err()
		}
	}
	info := objectstore.Info{Size: client.snapshot.Bytes, Version: client.snapshot.ObjectVersion, SHA256: client.snapshot.SHA256}
	switch client.mode {
	case "version":
		info.Version = "other-version"
	case "size":
		info.Size++
	case "digest":
		info.SHA256 = strings.Repeat("f", 64)
	case "ignored-range":
		return nil, info, errors.New("upstream ignored the byte range")
	}
	count := length
	if client.mode == "short" {
		count--
	}
	if client.mode == "oversize" {
		count++
	}
	return &rangeFixtureBody{client: client, context: ctx, offset: offset, remaining: count, stop: make(chan struct{})}, info, nil
}

func updateRangeMaximum(maximum *atomic.Int32, value int32) {
	for {
		previous := maximum.Load()
		if value <= previous || maximum.CompareAndSwap(previous, value) {
			return
		}
	}
}

type rangeFixtureBody struct {
	client            *rangeFixtureClient
	context           context.Context
	offset, remaining int64
	stop              chan struct{}
	once              sync.Once
}

func (body *rangeFixtureBody) Read(out []byte) (int, error) {
	updateRangeMaximum(&body.client.maximumRead, int32(len(out)))
	if body.client.mode == "blocked-body" {
		select {
		case <-body.context.Done():
			return 0, body.context.Err()
		case <-body.stop:
			return 0, io.ErrClosedPipe
		}
	}
	if err := body.context.Err(); err != nil {
		return 0, err
	}
	if body.remaining == 0 {
		return 0, io.EOF
	}
	n := min(int64(len(out)), body.remaining)
	for i := int64(0); i < n; i++ {
		out[i] = byte((body.offset + i) % 251)
	}
	body.offset += n
	body.remaining -= n
	return int(n), nil
}

func (body *rangeFixtureBody) Close() error {
	body.once.Do(func() { body.client.bodiesClosed.Add(1); close(body.stop) })
	return nil
}

func rangeFixture() (catalog.ObjectStorage, Snapshot) {
	storage := catalog.ObjectStorage{
		ObjectLocation:   catalog.ObjectLocation{Provider: "s3", Endpoint: "https://never-expose.example", Bucket: "range-fixtures", Prefix: "private/cache", Region: "us-east-1"},
		ReadCredentials:  catalog.ObjectCredentials{AccessKeyIDEnv: "KELVO_SOURCE_READER_ID", SecretAccessKeyEnv: "KELVO_SOURCE_READER_SECRET"},
		WriteCredentials: catalog.ObjectCredentials{AccessKeyIDEnv: "KELVO_SOURCE_WRITER_ID", SecretAccessKeyEnv: "KELVO_SOURCE_WRITER_SECRET"},
	}
	snapshot := Snapshot{Dataset: "events", Generation: strings.Repeat("a", 32), SHA256: strings.Repeat("b", 64), Bytes: 2 << 20, ObjectVersion: "selected-version"}
	snapshot.ObjectKey = storage.Prefix + "/tenant-a/events/" + snapshot.Generation + ".parquet"
	snapshot.Path, _ = storage.ObjectLocation.URI(snapshot.ObjectKey)
	return storage, snapshot
}

func openRangeFixture(t testing.TB, ctx context.Context, client *rangeFixtureClient) (catalog.Source, func()) {
	t.Helper()
	storage, _ := rangeFixture()
	sources, release, err := openObjectRanges(ctx, storage, []Snapshot{client.snapshot}, client)
	if err != nil {
		t.Fatal("could not open range fixture")
	}
	t.Cleanup(release)
	return sources[client.snapshot.Dataset], release
}

func rangeHTTPClient(t testing.TB) *http.Client {
	t.Helper()
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	t.Cleanup(transport.CloseIdleConnections)
	return client
}

func rangeRequest(t testing.TB, client *http.Client, method, target, value string) (*http.Response, []byte, error) {
	t.Helper()
	request, err := http.NewRequest(method, target, nil)
	if err != nil {
		t.Fatal("range fixture request was invalid")
	}
	if value != "" {
		request.Header.Set("Range", value)
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, nil, err
	}
	defer response.Body.Close()
	data, readErr := io.ReadAll(response.Body)
	return response, data, readErr
}

func TestObjectRangesExposeOnlySelectedCapabilitiesAndExactBytes(t *testing.T) {
	storage, snapshot := rangeFixture()
	client := &rangeFixtureClient{snapshot: snapshot}
	source, release := openRangeFixture(t, context.Background(), client)
	if source.Range == nil || source.Range.Validate() != nil || source.Range.Bytes != snapshot.Bytes || source.Type != "parquet" || source.Path != source.Range.URL || source.Object != nil {
		t.Fatal("range source did not contain a valid isolated capability")
	}
	encoded, err := json.Marshal(source)
	if err != nil {
		t.Fatal("range source encoding failed")
	}
	for _, forbidden := range []string{storage.Endpoint, storage.Bucket, storage.Prefix, snapshot.ObjectVersion, snapshot.ObjectKey, "KELVO_SOURCE_", "read_credentials", "write_credentials"} {
		if bytes.Contains(encoded, []byte(forbidden)) {
			t.Fatal("range source exposed upstream configuration or credential references")
		}
	}
	httpClient := rangeHTTPClient(t)
	response, data, err := rangeRequest(t, httpClient, http.MethodHead, source.Path, "")
	if err != nil || response.StatusCode != http.StatusOK || response.ContentLength != snapshot.Bytes || len(data) != 0 || response.Header.Get("Accept-Ranges") != "bytes" || response.Header.Get("ETag") != `"`+snapshot.SHA256+`"` {
		t.Fatal("range HEAD metadata did not describe the selected object")
	}
	if client.calls.Load() != 0 || client.forbiddenCalls.Load() != 0 {
		t.Fatal("HEAD contacted cloud storage")
	}
	for _, bounds := range [][2]int64{{0, 0}, {27, 100}, {snapshot.Bytes - 32768, snapshot.Bytes - 1}} {
		response, data, err = rangeRequest(t, httpClient, http.MethodGet, source.Path, fmt.Sprintf("bytes=%d-%d", bounds[0], bounds[1]))
		if err != nil || response.StatusCode != http.StatusPartialContent || int64(len(data)) != bounds[1]-bounds[0]+1 || response.ContentLength != int64(len(data)) ||
			response.Header.Get("Content-Range") != fmt.Sprintf("bytes %d-%d/%d", bounds[0], bounds[1], snapshot.Bytes) || response.Header.Get("Location") != "" {
			t.Fatal("range response changed byte bounds or redirected")
		}
		for i, value := range data {
			if value != byte((bounds[0]+int64(i))%251) {
				t.Fatal("range response changed an object byte")
			}
		}
	}
	release()
	release()
	if client.closed.Load() != 1 || client.bodiesClosed.Load() != 3 || client.forbiddenCalls.Load() != 0 {
		t.Fatal("range lifecycle leaked a reader or used unrestricted object operations")
	}
	if _, _, err := rangeRequest(t, httpClient, http.MethodHead, source.Path, ""); err == nil {
		t.Fatal("released range capability remained reachable")
	}
}

func TestObjectRangesRequireReadOnlyAzureSASInParent(t *testing.T) {
	storage, snapshot := rangeFixture()
	storage.Provider, storage.Region, storage.Account = "azure", "", "fixtureaccount"
	storage.Endpoint = "https://fixtureaccount.blob.core.windows.net"
	storage.ReadCredentials = catalog.ObjectCredentials{SASTokenEnv: "KELVO_SOURCE_RANGE_READER_SAS"}
	snapshot.Path, _ = storage.ObjectLocation.URI(snapshot.ObjectKey)
	for _, permission := range []string{"rw", "r"} {
		t.Setenv("KELVO_SOURCE_RANGE_READER_SAS", "sv=2023-11-03&sr=c&sp="+permission+"&se=2030-01-01T00%3A00%3A00Z&sig=fixture-signature")
		sources, release, err := OpenObjectRanges(context.Background(), storage, []Snapshot{snapshot})
		if permission == "rw" {
			if err == nil {
				release()
				t.Fatal("parent accepted a writer SAS for snapshot reads")
			}
			continue
		}
		if err != nil {
			t.Fatal("parent rejected a read-only reader SAS")
		}
		if sources[snapshot.Dataset].Range == nil || sources[snapshot.Dataset].Object != nil {
			t.Fatal("Azure credentials escaped the parent range client")
		}
		release()
	}
}

func TestObjectRangesRejectUnselectedPathsMethodsBodiesAndRanges(t *testing.T) {
	_, snapshot := rangeFixture()
	client := &rangeFixtureClient{snapshot: snapshot}
	source, _ := openRangeFixture(t, context.Background(), client)
	httpClient := rangeHTTPClient(t)
	parsed, _ := url.Parse(source.Path)
	for _, test := range []struct {
		name, method, path, host, value string
		body                            bool
		duplicate                       bool
		oversizedHeader                 bool
	}{
		{name: "wrong-token", path: "/" + strings.Repeat("0", 64) + "/events", value: "bytes=0-1"},
		{name: "other-object", path: strings.TrimSuffix(parsed.Path, "events") + "other", value: "bytes=0-1"},
		{name: "list", path: strings.TrimSuffix(parsed.Path, "events"), value: "bytes=0-1"},
		{name: "query", path: parsed.Path + "?object=other", value: "bytes=0-1"},
		{name: "encoded-path", path: strings.TrimSuffix(parsed.Path, "events") + "%65vents", value: "bytes=0-1"},
		{name: "wrong-host", host: "foreign.example", value: "bytes=0-1"},
		{name: "put", method: http.MethodPut, value: "bytes=0-1"},
		{name: "post", method: http.MethodPost, value: "bytes=0-1"},
		{name: "full-get"},
		{name: "multiple", value: "bytes=0-1,3-4"},
		{name: "duplicate", value: "bytes=0-1", duplicate: true},
		{name: "open", value: "bytes=0-"},
		{name: "suffix", value: "bytes=-4"},
		{name: "reversed", value: "bytes=4-3"},
		{name: "negative", value: "bytes=-1-3"},
		{name: "overflow", value: "bytes=0-9223372036854775808"},
		{name: "outside", value: fmt.Sprintf("bytes=0-%d", snapshot.Bytes)},
		{name: "whitespace", value: "bytes=0 - 1"},
		{name: "request-body", value: "bytes=0-1", body: true},
		{name: "header-cap", value: "bytes=0-1", oversizedHeader: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			target := source.Path
			if test.path != "" {
				target = "http://" + parsed.Host + test.path
			}
			method := test.method
			if method == "" {
				method = http.MethodGet
			}
			var body io.Reader
			if test.body {
				body = strings.NewReader("x")
			}
			request, err := http.NewRequest(method, target, body)
			if err != nil {
				t.Fatal("invalid rejection fixture")
			}
			if test.host != "" {
				request.Host = test.host
			}
			if test.value != "" {
				request.Header.Set("Range", test.value)
			}
			if test.duplicate {
				request.Header.Add("Range", test.value)
			}
			if test.oversizedHeader {
				request.Header.Set("X-Oversized", strings.Repeat("x", 32<<10))
			}
			response, err := httpClient.Do(request)
			if err != nil {
				t.Fatal("rejection request failed before a response")
			}
			defer response.Body.Close()
			data, err := io.ReadAll(response.Body)
			if err != nil || response.StatusCode < 400 || response.StatusCode >= 500 || bytes.Contains(data, []byte(parsed.Path)) || response.Header.Get("Location") != "" {
				t.Fatal("invalid capability request was not safely rejected")
			}
		})
	}
	if client.calls.Load() != 0 || client.forbiddenCalls.Load() != 0 {
		t.Fatal("invalid capability request reached upstream storage")
	}
}

func TestObjectRangesRejectChangedMetadataAndIncompleteBodies(t *testing.T) {
	for _, mode := range []string{"version", "size", "digest", "ignored-range", "short", "oversize"} {
		t.Run(mode, func(t *testing.T) {
			_, snapshot := rangeFixture()
			client := &rangeFixtureClient{snapshot: snapshot, mode: mode}
			source, release := openRangeFixture(t, context.Background(), client)
			response, data, err := rangeRequest(t, rangeHTTPClient(t), http.MethodGet, source.Path, "bytes=3-65538")
			if err == nil && response.StatusCode == http.StatusPartialContent && len(data) == 65536 {
				t.Fatal("invalid upstream range appeared complete")
			}
			if mode != "short" && mode != "oversize" && (err != nil || response.StatusCode != http.StatusBadGateway) {
				t.Fatal("invalid upstream metadata was not rejected before streaming")
			}
			release()
			if mode != "ignored-range" && client.bodiesClosed.Load() != 1 {
				t.Fatal("invalid upstream body was not closed")
			}
		})
	}
}

func TestObjectRangesBoundConcurrencyAndStreamBuffers(t *testing.T) {
	t.Run("concurrency", func(t *testing.T) {
		_, snapshot := rangeFixture()
		gate := make(chan struct{})
		started := make(chan struct{}, objectRangeConcurrency)
		client := &rangeFixtureClient{snapshot: snapshot, gate: gate, started: started}
		source, _ := openRangeFixture(t, context.Background(), client)
		httpClient := rangeHTTPClient(t)
		done := make(chan bool, objectRangeConcurrency)
		for i := 0; i < objectRangeConcurrency; i++ {
			go func() {
				response, data, err := rangeRequest(t, httpClient, http.MethodGet, source.Path, "bytes=0-31")
				done <- err == nil && response.StatusCode == http.StatusPartialContent && len(data) == 32
			}()
		}
		for i := 0; i < objectRangeConcurrency; i++ {
			select {
			case <-started:
			case <-time.After(2 * time.Second):
				t.Fatal("parallel range did not start")
			}
		}
		response, _, err := rangeRequest(t, httpClient, http.MethodGet, source.Path, "bytes=0-31")
		if err != nil || response.StatusCode != http.StatusServiceUnavailable || client.maximumActive.Load() != objectRangeConcurrency {
			t.Fatal("range concurrency exceeded its bound")
		}
		close(gate)
		for i := 0; i < objectRangeConcurrency; i++ {
			if !<-done {
				t.Fatal("bounded concurrent range failed")
			}
		}
	})
	t.Run("buffer-and-range-limit", func(t *testing.T) {
		_, snapshot := rangeFixture()
		snapshot.Bytes = objectRangeLimit + 1
		client := &rangeFixtureClient{snapshot: snapshot}
		source, _ := openRangeFixture(t, context.Background(), client)
		httpClient := rangeHTTPClient(t)
		response, _, err := rangeRequest(t, httpClient, http.MethodGet, source.Path, fmt.Sprintf("bytes=0-%d", objectRangeLimit))
		if err != nil || response.StatusCode != http.StatusRequestedRangeNotSatisfiable || client.calls.Load() != 0 {
			t.Fatal("oversized byte range reached storage")
		}
		request, _ := http.NewRequest(http.MethodGet, source.Path, nil)
		request.Header.Set("Range", fmt.Sprintf("bytes=0-%d", objectRangeLimit-1))
		response, err = httpClient.Do(request)
		if err != nil {
			t.Fatal("bounded large range failed")
		}
		count, err := io.CopyBuffer(io.Discard, response.Body, make([]byte, 32<<10))
		_ = response.Body.Close()
		if err != nil || count != objectRangeLimit || response.StatusCode != http.StatusPartialContent || client.maximumRead.Load() > objectRangeBuffer {
			t.Fatal("range streaming exceeded byte or copy-buffer bounds")
		}
	})
}

func TestObjectRangesCancellationAndReleaseWaitForUpstreamCleanup(t *testing.T) {
	for _, mode := range []string{"blocked-request", "blocked-body"} {
		t.Run(mode, func(t *testing.T) {
			_, snapshot := rangeFixture()
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			client := &rangeFixtureClient{snapshot: snapshot, mode: mode, started: make(chan struct{}, 1)}
			if mode == "blocked-request" {
				client.gate = make(chan struct{})
			}
			source, release := openRangeFixture(t, parent, client)
			httpClient := rangeHTTPClient(t)
			done := make(chan bool, 1)
			go func() {
				response, data, err := rangeRequest(t, httpClient, http.MethodGet, source.Path, "bytes=0-65535")
				done <- err != nil || response.StatusCode != http.StatusPartialContent || len(data) != 65536
			}()
			select {
			case <-client.started:
			case <-time.After(2 * time.Second):
				t.Fatal("cancellation fixture did not start")
			}
			cancel()
			released := make(chan struct{})
			go func() { release(); close(released) }()
			select {
			case <-released:
			case <-time.After(2 * time.Second):
				t.Fatal("release did not join upstream cleanup")
			}
			if client.closed.Load() != 1 || client.active.Load() != 0 {
				t.Fatal("release returned with a live upstream client")
			}
			if mode == "blocked-body" && client.bodiesClosed.Load() != 1 {
				t.Fatal("release returned without closing the upstream body")
			}
			select {
			case failed := <-done:
				if !failed {
					t.Fatal("canceled range appeared complete")
				}
			case <-time.After(2 * time.Second):
				t.Fatal("canceled range client remained blocked")
			}
		})
	}
}

type delayedRangeCloseBody struct {
	ctx          context.Context
	readEntered  chan struct{}
	closeEntered chan struct{}
	closeAllowed chan struct{}
	readOnce     sync.Once
	closeOnce    sync.Once
	closes       atomic.Int32
}

func (body *delayedRangeCloseBody) Read([]byte) (int, error) {
	body.readOnce.Do(func() { close(body.readEntered) })
	<-body.ctx.Done()
	return 0, body.ctx.Err()
}
func (body *delayedRangeCloseBody) Close() error {
	body.closes.Add(1)
	body.closeOnce.Do(func() { close(body.closeEntered) })
	<-body.closeAllowed
	return nil
}

type delayedRangeCloseClient struct {
	*rangeFixtureClient
	body *delayedRangeCloseBody
}

func (client *delayedRangeCloseClient) GetRange(ctx context.Context, _, _ string, _, _ int64) (io.ReadCloser, objectstore.Info, error) {
	client.body.ctx = ctx
	return client.body, objectstore.Info{Size: client.snapshot.Bytes, Version: client.snapshot.ObjectVersion, SHA256: client.snapshot.SHA256}, nil
}

func TestObjectRangesReleaseJoinsDelayedBodyClose(t *testing.T) {
	storage, snapshot := rangeFixture()
	parent, cancel := context.WithCancel(context.Background())
	body := &delayedRangeCloseBody{readEntered: make(chan struct{}), closeEntered: make(chan struct{}), closeAllowed: make(chan struct{})}
	client := &delayedRangeCloseClient{rangeFixtureClient: &rangeFixtureClient{snapshot: snapshot}, body: body}
	sources, release, err := openObjectRanges(parent, storage, []Snapshot{snapshot}, client)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	var once sync.Once
	unblock := func() { once.Do(func() { close(body.closeAllowed) }) }
	var joins []<-chan struct{}
	t.Cleanup(func() {
		unblock()
		cancel()
		release()
		for _, done := range joins {
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Error("range fixture cleanup did not join its work")
			}
		}
	})
	httpClient := rangeHTTPClient(t)
	requested, released := make(chan struct{}), make(chan struct{})
	joins = append(joins, requested)
	go func() {
		defer close(requested)
		response, data, err := rangeRequest(t, httpClient, http.MethodGet, sources[snapshot.Dataset].Path, "bytes=0-65535")
		if err == nil && response.StatusCode == http.StatusPartialContent && len(data) == 65536 {
			t.Error("canceled range appeared complete")
		}
	}()
	select {
	case <-body.readEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("range body read did not begin")
	}
	cancel()
	select {
	case <-body.closeEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("range cancellation did not begin body Close")
	}
	joins = append(joins, released)
	go func() { release(); close(released) }()
	select {
	case <-released:
		t.Fatal("range release returned before body Close completed")
	case <-time.After(20 * time.Millisecond):
	}
	if client.closed.Load() != 0 {
		t.Fatal("range client closed before its body cleanup completed")
	}
	unblock()
	for _, done := range joins {
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("range cleanup did not finish")
		}
	}
	if body.closes.Load() != 1 || client.closed.Load() != 1 {
		t.Fatal("range cleanup was not once-only")
	}
}

func TestObjectRangesRejectSnapshotKeyConfusionBeforeServing(t *testing.T) {
	for _, mode := range []string{"prefix", "tenant", "dataset", "generation", "uri", "version", "size", "digest", "duplicate", "mixed-tenant"} {
		t.Run(mode, func(t *testing.T) {
			storage, snapshot := rangeFixture()
			switch mode {
			case "prefix":
				snapshot.ObjectKey = "other/tenant-a/events/" + snapshot.Generation + ".parquet"
			case "tenant":
				snapshot.ObjectKey = storage.Prefix + "/../events/" + snapshot.Generation + ".parquet"
			case "dataset":
				snapshot.ObjectKey = storage.Prefix + "/tenant-a/other/" + snapshot.Generation + ".parquet"
			case "generation":
				snapshot.Generation = "../other"
			case "uri":
				snapshot.Path = "https://foreign.example/private"
			case "version":
				snapshot.ObjectVersion = ""
			case "size":
				snapshot.Bytes = 0
			case "digest":
				snapshot.SHA256 = "invalid"
			}
			snapshots := []Snapshot{snapshot}
			if mode == "duplicate" {
				snapshots = append(snapshots, snapshot)
			}
			if mode == "mixed-tenant" {
				other := snapshot
				other.Dataset = "other"
				other.ObjectKey = storage.Prefix + "/tenant-b/other/" + other.Generation + ".parquet"
				other.Path, _ = storage.ObjectLocation.URI(other.ObjectKey)
				snapshots = append(snapshots, other)
			}
			client := &rangeFixtureClient{snapshot: snapshot}
			_, release, err := openObjectRanges(context.Background(), storage, snapshots, client)
			if err == nil {
				release()
				t.Fatal("invalid snapshot key received a capability")
			}
			if client.closed.Load() != 1 || client.calls.Load() != 0 {
				t.Fatal("invalid range setup leaked a client or contacted storage")
			}
		})
	}
}

func BenchmarkObjectRangesLoopbackStream(b *testing.B) {
	_, snapshot := rangeFixture()
	snapshot.Bytes = objectRangeLimit
	client := &rangeFixtureClient{snapshot: snapshot}
	source, _ := openRangeFixture(b, context.Background(), client)
	httpClient := rangeHTTPClient(b)
	buffer := make([]byte, objectRangeBuffer)
	b.SetBytes(snapshot.Bytes)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		request, _ := http.NewRequest(http.MethodGet, source.Path, nil)
		request.Header.Set("Range", fmt.Sprintf("bytes=0-%d", snapshot.Bytes-1))
		response, err := httpClient.Do(request)
		if err != nil {
			b.Fatal("loopback range benchmark request failed")
		}
		count, err := io.CopyBuffer(io.Discard, response.Body, buffer)
		_ = response.Body.Close()
		if err != nil || count != snapshot.Bytes || response.StatusCode != http.StatusPartialContent {
			b.Fatal("loopback range benchmark was incomplete")
		}
	}
}
