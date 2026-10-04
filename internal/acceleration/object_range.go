// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/objectstore"
)

const (
	objectRangeLimit       = int64(32 << 20)
	objectRangeConcurrency = 4
	objectRangeBuffer      = 32 << 10
)

// OpenObjectRanges gives one query capability URLs for its already acquired
// immutable snapshots. Cloud identities stay in this parent-owned range client;
// the returned sources contain only loopback URLs and public object sizes.
// Release cancels requests, closes the listener, and waits for upstream cleanup.
func OpenObjectRanges(ctx context.Context, storage catalog.ObjectStorage, snapshots []Snapshot) (map[string]catalog.Source, func(), error) {
	if storage.ReaderRegistry != nil {
		return nil, nil, errors.New("protected object ranges require a live reader guard")
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if len(snapshots) == 0 {
		return map[string]catalog.Source{}, func() {}, nil
	}
	if storage.Provider == "azure" {
		if err := storage.ReadCredentials.Validate("azure"); err != nil {
			return nil, nil, err
		}
		if err := catalog.ValidateAzureReadSAS(os.Getenv(storage.ReadCredentials.SASTokenEnv)); err != nil {
			return nil, nil, err
		}
	}
	client, err := objectstore.New(storage.ObjectLocation, storage.ReadCredentials)
	if err != nil {
		return nil, nil, err
	}
	ranges, ok := client.(objectstore.RangeClient)
	if !ok {
		client.Close()
		return nil, nil, errors.New("object storage does not support guarded range reads")
	}
	return OpenObjectRangesWithClient(ctx, storage, snapshots, ranges)
}

// OpenObjectRangesWithClient uses an explicit trusted provider client. Like
// NewObjectBackend, it owns that client on success and failure. The caller must
// supply already acquired immutable snapshots and retain their generation
// leases until release has closed requests and the query child has exited.
// RangeClient's exact version and response validation contract still applies;
// this does not grant public callers authority to mint snapshot capabilities.
func OpenObjectRangesWithClient(ctx context.Context, storage catalog.ObjectStorage, snapshots []Snapshot, client objectstore.RangeClient) (map[string]catalog.Source, func(), error) {
	if storage.ReaderRegistry != nil {
		if client != nil {
			client.Close()
		}
		return nil, nil, errors.New("protected object ranges require a live reader guard")
	}
	return openObjectRanges(ctx, storage, snapshots, client)
}

// The injected client is owned by this helper, including on failed setup.
func openObjectRanges(parent context.Context, storage catalog.ObjectStorage, snapshots []Snapshot, client objectstore.RangeClient) (map[string]catalog.Source, func(), error) {
	return openObjectRangesOwned(parent, storage, snapshots, client, true, nil)
}

// Protected ranges borrow one node-owned client. Their guard consumer stays
// held until all handlers and body-close callbacks join; Close never closes the
// shared provider underneath a different query.
func openProtectedObjectRanges(parent context.Context, storage catalog.ObjectStorage, snapshots []Snapshot, client objectstore.RangeClient, check func() error) (map[string]catalog.Source, func(), error) {
	if storage.ReaderRegistry == nil || check == nil {
		return nil, nil, errors.New("protected object ranges require a live reader guard")
	}
	return openObjectRangesOwned(parent, storage, snapshots, client, false, check)
}

func openObjectRangesOwned(parent context.Context, storage catalog.ObjectStorage, snapshots []Snapshot, client objectstore.RangeClient, owned bool, check func() error) (map[string]catalog.Source, func(), error) {
	if client == nil {
		return nil, nil, errors.New("object ranges require a reader")
	}
	ready := false
	defer func() {
		if !ready && owned {
			client.Close()
		}
	}()
	if err := parent.Err(); err != nil {
		return nil, nil, err
	}
	if check != nil {
		if err := check(); err != nil {
			return nil, nil, err
		}
	}
	if err := storage.ObjectLocation.Validate(); err != nil {
		return nil, nil, err
	}
	if len(snapshots) == 0 || len(snapshots) > 64 {
		return nil, nil, errors.New("object ranges require 1 to 64 snapshots")
	}
	selected := make(map[string][]Snapshot, len(snapshots))
	multipart := make(map[string]bool, len(snapshots))
	tenant := ""
	count := 0
	for _, snapshot := range snapshots {
		if _, duplicate := selected[snapshot.Dataset]; duplicate {
			return nil, nil, errors.New("object range dataset is duplicated")
		}
		leaves, scope, err := objectRangeLeaves(storage, snapshot)
		if err != nil {
			return nil, nil, err
		}
		if tenant != "" && tenant != scope {
			return nil, nil, errors.New("object range tenant mismatch")
		}
		tenant = scope
		count += len(leaves)
		if count > 1024 {
			return nil, nil, errors.New("query object range capability limit exceeded")
		}
		selected[snapshot.Dataset] = leaves
		multipart[snapshot.Dataset] = len(snapshot.Parts) > 0
	}
	var random [32]byte
	if _, err := rand.Read(random[:]); err != nil {
		return nil, nil, errors.New("object range capability is unavailable")
	}
	token := hex.EncodeToString(random[:])
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return nil, nil, errors.New("object range listener is unavailable")
	}
	lifetime, cancel := context.WithCancel(parent)
	bridge := &objectRangeBridge{client: client, check: check, authority: listener.Addr().String(), routes: make(map[string]Snapshot, len(selected)), slots: make(chan struct{}, objectRangeConcurrency)}
	sources := make(map[string]catalog.Source, len(selected))
	for dataset, leaves := range selected {
		source := catalog.Source{ID: dataset, Type: "parquet"}
		for index, leaf := range leaves {
			path := "/" + token + "/" + dataset
			if multipart[dataset] {
				path += fmt.Sprintf("/part-%04d", index)
			}
			url := "http://" + bridge.authority + path
			bridge.routes[path] = leaf
			capability := catalog.ObjectRange{URL: url, Bytes: leaf.Bytes}
			if multipart[dataset] {
				source.Ranges = append(source.Ranges, capability)
			} else {
				source.Path = url
				source.Range = &capability
			}
		}
		sources[dataset] = source
	}
	server := &http.Server{
		Handler: bridge, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second,
		IdleTimeout: 15 * time.Second, WriteTimeout: 60 * time.Second, MaxHeaderBytes: 8 << 10,
		BaseContext: func(net.Listener) context.Context { return lifetime },
		// A capability path must never be included in HTTP diagnostics.
		ErrorLog: log.New(io.Discard, "", 0),
	}
	served, released := make(chan struct{}), make(chan struct{})
	go func() { _ = server.Serve(listener); close(served) }()
	var once sync.Once
	release := func() {
		once.Do(func() {
			bridge.mu.Lock()
			bridge.stopped = true
			bridge.mu.Unlock()
			cancel()
			_ = server.Close()
			<-served
			bridge.active.Wait()
			if owned {
				client.Close()
			}
			close(released)
		})
	}
	go func() {
		select {
		case <-parent.Done():
			release()
		case <-served:
			release()
		case <-released:
		}
	}()
	ready = true
	if check != nil {
		if err := check(); err != nil {
			release()
			return nil, nil, err
		}
	}
	return sources, release, nil
}

// Convert an acquired generation into private leaf routes. Fresh copies prevent
// caller mutation of Snapshot.Parts from changing an already authorized bridge.
func objectRangeLeaves(storage catalog.ObjectStorage, snapshot Snapshot) ([]Snapshot, string, error) {
	bad := func() ([]Snapshot, string, error) {
		return nil, "", errors.New("object range snapshot metadata is invalid")
	}
	if !storeDatasetID.MatchString(snapshot.Dataset) || !storeGenerationID.MatchString(snapshot.Generation) || snapshot.Bytes <= 0 || !storeDigest.MatchString(snapshot.SHA256) {
		return bad()
	}
	isMultipart := len(snapshot.Parts) > 0
	if isMultipart && (len(snapshot.Parts) > 256 || snapshot.Path != "" || snapshot.ObjectKey != "" || snapshot.ObjectVersion != "" || !storeDigest.MatchString(snapshot.SchemaHash) || snapshot.Rows < 0 || snapshot.Bytes > 1<<40) {
		return bad()
	}
	leaves := []Snapshot{snapshot}
	if isMultipart {
		leaves = make([]Snapshot, len(snapshot.Parts))
		var rows, bytes int64
		for i, p := range snapshot.Parts {
			if p.Path != "" || p.Rows < 0 || p.Rows > snapshot.Rows-rows || p.Bytes <= 0 || p.Bytes > snapshot.Bytes-bytes {
				return bad()
			}
			rows += p.Rows
			bytes += p.Bytes
			leaves[i] = Snapshot{Dataset: snapshot.Dataset, Generation: snapshot.Generation, Bytes: p.Bytes, Rows: p.Rows, SHA256: p.SHA256, ObjectKey: p.ObjectKey, ObjectVersion: p.ObjectVersion}
		}
		if rows != snapshot.Rows || bytes != snapshot.Bytes {
			return bad()
		}
	}
	tenant := ""
	for i, leaf := range leaves {
		if leaf.Bytes <= 0 || leaf.Bytes > objectstore.MaxUploadBytes || !storeDigest.MatchString(leaf.SHA256) || !validObjectVersion(leaf.ObjectVersion) || storage.ObjectLocation.ValidateKey(leaf.ObjectKey) != nil {
			return bad()
		}
		parts := strings.Split(strings.TrimPrefix(leaf.ObjectKey, storage.Prefix+"/"), "/")
		expected := snapshot.Generation + ".parquet"
		if isMultipart {
			expected = multipartName(snapshot.Generation, i)
		}
		if len(parts) != 3 || !storeTenantID.MatchString(parts[0]) || parts[1] != snapshot.Dataset || parts[2] != expected || (tenant != "" && tenant != parts[0]) {
			return bad()
		}
		tenant = parts[0]
		if !isMultipart {
			uri, err := storage.ObjectLocation.URI(leaf.ObjectKey)
			if err != nil || leaf.Path != uri {
				return bad()
			}
		}
	}
	return leaves, tenant, nil
}

type objectRangeBridge struct {
	client    objectstore.RangeClient
	check     func() error
	authority string
	routes    map[string]Snapshot
	slots     chan struct{}
	mu        sync.Mutex
	stopped   bool
	active    sync.WaitGroup
}

func (bridge *objectRangeBridge) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	bridge.mu.Lock()
	if bridge.stopped {
		bridge.mu.Unlock()
		objectRangeError(w, http.StatusServiceUnavailable)
		return
	}
	bridge.active.Add(1)
	bridge.mu.Unlock()
	defer bridge.active.Done()
	if bridge.check != nil && bridge.check() != nil {
		objectRangeError(w, http.StatusServiceUnavailable)
		return
	}
	if r.Host != bridge.authority || r.URL.Scheme != "" || r.URL.Host != "" || r.URL.RawQuery != "" || r.URL.ForceQuery ||
		r.URL.RawPath != "" || r.RequestURI != r.URL.Path {
		objectRangeError(w, http.StatusForbidden)
		return
	}
	snapshot, found := bridge.routes[r.URL.Path]
	if !found {
		objectRangeError(w, http.StatusNotFound)
		return
	}
	if r.Method != http.MethodHead && r.Method != http.MethodGet {
		objectRangeError(w, http.StatusMethodNotAllowed)
		return
	}
	if r.ContentLength != 0 || len(r.TransferEncoding) != 0 {
		objectRangeError(w, http.StatusBadRequest)
		return
	}
	if r.Method == http.MethodHead {
		if len(r.Header.Values("Range")) != 0 {
			objectRangeError(w, http.StatusBadRequest)
			return
		}
		objectRangeHeaders(w, snapshot)
		w.Header().Set("Content-Length", strconv.FormatInt(snapshot.Bytes, 10))
		w.WriteHeader(http.StatusOK)
		return
	}
	start, length, ok := parseObjectRange(r.Header.Values("Range"), snapshot.Bytes)
	if !ok {
		w.Header().Set("Content-Range", "bytes */"+strconv.FormatInt(snapshot.Bytes, 10))
		objectRangeError(w, http.StatusRequestedRangeNotSatisfiable)
		return
	}
	select {
	case bridge.slots <- struct{}{}:
		defer func() { <-bridge.slots }()
	default:
		objectRangeError(w, http.StatusServiceUnavailable)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()
	if bridge.check != nil && bridge.check() != nil {
		objectRangeError(w, http.StatusServiceUnavailable)
		return
	}
	body, info, err := bridge.client.GetRange(ctx, snapshot.ObjectKey, snapshot.ObjectVersion, start, length)
	if err != nil {
		if body != nil {
			_ = body.Close()
		}
		objectRangeError(w, http.StatusBadGateway)
		return
	}
	if body == nil {
		objectRangeError(w, http.StatusBadGateway)
		return
	}
	var closeOnce sync.Once
	var closeErr error
	closeBody := func() { closeOnce.Do(func() { closeErr = body.Close() }) }
	callbackDone := make(chan struct{})
	stopClose := context.AfterFunc(ctx, func() {
		defer close(callbackDone)
		closeBody()
	})
	var joinOnce sync.Once
	joinBody := func() {
		joinOnce.Do(func() {
			stopped := stopClose()
			closeBody()
			if !stopped {
				<-callbackDone
			}
		})
	}
	defer joinBody()
	if validateSnapshotObject(snapshot, info) != nil {
		objectRangeError(w, http.StatusBadGateway)
		return
	}
	objectRangeHeaders(w, snapshot)
	w.Header().Set("Content-Length", strconv.FormatInt(length, 10))
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, start+length-1, snapshot.Bytes))
	w.WriteHeader(http.StatusPartialContent)
	// Hold back the final byte until EOF proves an exact response. Even after
	// headers or a prefix are sent, a short/oversized upstream range cannot look
	// complete to DuckDB. Memory stays one 32 KiB buffer per active request.
	buffer := make([]byte, objectRangeBuffer)
	written, err := io.CopyBuffer(struct{ io.Writer }{w}, io.LimitReader(body, length-1), buffer)
	if err != nil || written != length-1 {
		panic(http.ErrAbortHandler)
	}
	var tail [2]byte
	n, err := io.ReadFull(body, tail[:])
	// Complete upstream ownership before publishing a complete HTTP range.
	// Exact bytes followed by failed Close must remain an incomplete result.
	joinBody()
	if n != 1 || !errors.Is(err, io.ErrUnexpectedEOF) || closeErr != nil || ctx.Err() != nil || (bridge.check != nil && bridge.check() != nil) {
		panic(http.ErrAbortHandler)
	}
	if _, err := w.Write(tail[:1]); err != nil {
		panic(http.ErrAbortHandler)
	}
}

func objectRangeHeaders(w http.ResponseWriter, snapshot Snapshot) {
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("ETag", `"`+snapshot.SHA256+`"`)
	w.Header().Set("Cache-Control", "private, no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
}

func objectRangeError(w http.ResponseWriter, status int) {
	w.Header().Set("Connection", "close")
	http.Error(w, "Snapshot range request unavailable", status)
}

// Pinned HTTPFS uses closed ranges. Reject suffix/open/multiple ranges instead
// of interpreting an unbounded request or silently downloading a whole object.
func parseObjectRange(values []string, size int64) (int64, int64, bool) {
	if len(values) != 1 || len(values[0]) > 64 || !strings.HasPrefix(values[0], "bytes=") {
		return 0, 0, false
	}
	startText, endText, ok := strings.Cut(strings.TrimPrefix(values[0], "bytes="), "-")
	if !ok || startText == "" || endText == "" {
		return 0, 0, false
	}
	for _, text := range []string{startText, endText} {
		for _, digit := range text {
			if digit < '0' || digit > '9' {
				return 0, 0, false
			}
		}
	}
	start, startErr := strconv.ParseInt(startText, 10, 64)
	end, endErr := strconv.ParseInt(endText, 10, 64)
	if startErr != nil || endErr != nil || start < 0 || end < start || end >= size || end-start >= objectRangeLimit {
		return 0, 0, false
	}
	return start, end - start + 1, true
}
