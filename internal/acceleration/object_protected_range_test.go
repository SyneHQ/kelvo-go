// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/readerlease"
)

func TestProtectedObjectRangesBorrowSharedClientAndRefuseLostGuard(t *testing.T) {
	storage, snapshot := rangeFixture()
	storage.ReaderRegistry = &catalog.ObjectReaderRegistry{}
	client := &rangeFixtureClient{snapshot: snapshot}
	var lost atomic.Bool
	check := func() error {
		if lost.Load() {
			return readerlease.ErrLost
		}
		return nil
	}
	first, closeFirst, err := openProtectedObjectRanges(context.Background(), storage, []Snapshot{snapshot}, client, check)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(closeFirst)
	second, closeSecond, err := openProtectedObjectRanges(context.Background(), storage, []Snapshot{snapshot}, client, check)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(closeSecond)
	if first[snapshot.Dataset].Path == second[snapshot.Dataset].Path {
		t.Fatal("independent queries shared a capability")
	}
	closeFirst()
	if client.closed.Load() != 0 {
		t.Fatal("one query closed the node-owned provider")
	}
	httpClient := rangeHTTPClient(t)
	response, body, err := rangeRequest(t, httpClient, http.MethodGet, second[snapshot.Dataset].Path, "bytes=0-31")
	if err != nil || response.StatusCode != http.StatusPartialContent || len(body) != 32 || client.calls.Load() != 1 {
		t.Fatal("other query lost its borrowed client", err)
	}
	lost.Store(true)
	for _, method := range []string{http.MethodGet, http.MethodHead} {
		value := ""
		if method == http.MethodGet {
			value = "bytes=0-31"
		}
		response, _, err := rangeRequest(t, httpClient, method, second[snapshot.Dataset].Path, value)
		if err != nil || response.StatusCode != http.StatusServiceUnavailable {
			t.Fatal("lost guard exposed metadata or payload", method, err)
		}
	}
	if client.calls.Load() != 1 {
		t.Fatal("lost guard reached the provider")
	}
	closeSecond()
	if client.closed.Load() != 0 || client.active.Load() != 0 || client.bodiesClosed.Load() != 1 {
		t.Fatal("range cleanup leaked work or closed shared provider")
	}
	if sources, release, err := openProtectedObjectRanges(context.Background(), storage, []Snapshot{snapshot}, client, check); err == nil || sources != nil || release != nil {
		t.Fatal("lost guard minted another capability")
	}
}

func TestProtectedObjectRangePublicOpenersRefuseUnpinnedReads(t *testing.T) {
	storage, snapshot := rangeFixture()
	storage.ReaderRegistry = &catalog.ObjectReaderRegistry{}
	client := &rangeFixtureClient{snapshot: snapshot}
	if sources, _, err := OpenObjectRanges(context.Background(), storage, []Snapshot{snapshot}); err == nil || sources != nil {
		t.Fatal("public opener accepted protected snapshots without a guard")
	}
	if sources, _, err := OpenObjectRangesWithClient(context.Background(), storage, []Snapshot{snapshot}, client); err == nil || sources != nil || client.closed.Load() != 1 {
		t.Fatal("injected public opener minted an unpinned capability or lost its client")
	}
	if client.calls.Load() != 0 || client.forbiddenCalls.Load() != 0 {
		t.Fatal("refused opener performed provider I/O")
	}
}

func TestProtectedObjectRangeCloseFailureWithholdsFinalByte(t *testing.T) {
	storage, snapshot := rangeFixture()
	storage.ReaderRegistry = &catalog.ObjectReaderRegistry{}
	client := &rangeFixtureClient{snapshot: snapshot, mode: "close-error"}
	sources, release, err := openProtectedObjectRanges(context.Background(), storage, []Snapshot{snapshot}, client, func() error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(release)
	response, body, err := rangeRequest(t, rangeHTTPClient(t), http.MethodGet, sources[snapshot.Dataset].Path, "bytes=0-65535")
	if err == nil && response != nil && response.StatusCode == http.StatusPartialContent && len(body) == 65536 {
		t.Fatal("failed upstream Close published a complete byte range")
	}
	release()
	if client.bodiesClosed.Load() != 1 || client.closed.Load() != 0 || client.active.Load() != 0 {
		t.Fatal("failed Close lost borrowed ownership or left active range work")
	}
}
