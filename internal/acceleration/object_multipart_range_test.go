package acceleration

import (
	"context"
	"errors"
	"fmt"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/objectstore"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

type multiRangeFixture struct {
	*rangeFixtureClient
	leaves map[string]*rangeFixtureClient
}

func (c *multiRangeFixture) GetRange(ctx context.Context, key, version string, offset, length int64) (io.ReadCloser, objectstore.Info, error) {
	leaf := c.leaves[key]
	if leaf == nil {
		return nil, objectstore.Info{}, errors.New("unselected part")
	}
	return leaf.GetRange(ctx, key, version, offset, length)
}
func multipartRangeFixture(n int) (catalog.ObjectStorage, Snapshot, *multiRangeFixture) {
	storage, base := rangeFixture()
	root := base
	root.Path = ""
	root.ObjectKey = ""
	root.ObjectVersion = ""
	root.Bytes = 0
	root.Rows = 0
	root.SchemaHash = strings.Repeat("c", 64)
	client := &multiRangeFixture{rangeFixtureClient: &rangeFixtureClient{}, leaves: map[string]*rangeFixtureClient{}}
	for i := 0; i < n; i++ {
		leaf := base
		leaf.Path = ""
		leaf.ObjectKey = strings.TrimSuffix(base.ObjectKey, base.Generation+".parquet") + multipartName(base.Generation, i)
		leaf.ObjectVersion = fmt.Sprintf("version-%d", i)
		root.Parts = append(root.Parts, SnapshotPart{ObjectKey: leaf.ObjectKey, ObjectVersion: leaf.ObjectVersion, Bytes: leaf.Bytes, Rows: 1, SHA256: leaf.SHA256})
		root.Bytes += leaf.Bytes
		root.Rows++
		client.leaves[leaf.ObjectKey] = &rangeFixtureClient{snapshot: leaf}
	}
	return storage, root, client
}
func TestMultipartRangesIsolationAndInputMutation(t *testing.T) {
	storage, snapshot, client := multipartRangeFixture(2)
	sources, close, err := openObjectRanges(context.Background(), storage, []Snapshot{snapshot}, client)
	if err != nil {
		t.Fatal(err)
	}
	defer close()
	source := sources[snapshot.Dataset]
	if source.ValidateObjectRanges() != nil || len(source.Ranges) != 2 || source.Path != "" || source.Range != nil {
		t.Fatal("invalid multipart output")
	}
	snapshot.Parts[0].ObjectKey = "forbidden"
	snapshot.Parts[0].Bytes = 1
	httpClient := rangeHTTPClient(t)
	for _, r := range source.Ranges {
		response, data, err := rangeRequest(t, httpClient, "GET", r.URL, "bytes=0-31")
		if err != nil || response.StatusCode != 206 || len(data) != 32 {
			t.Fatalf("leaf range failed: %v", err)
		}
	}
	for _, target := range []string{source.Ranges[0].URL + "?x=y", strings.Replace(source.Ranges[0].URL, "part-0000", "part-0002", 1), strings.TrimSuffix(source.Ranges[0].URL, "/part-0000")} {
		response, _, _ := rangeRequest(t, httpClient, "HEAD", target, "")
		if response.StatusCode == 200 {
			t.Fatal("unselected route accepted")
		}
	}
	for _, leaf := range client.leaves {
		leaf.mode = "version"
	}
	response, _, _ := rangeRequest(t, httpClient, "GET", source.Ranges[0].URL, "bytes=0-31")
	if response.StatusCode != 502 {
		t.Fatal("changed object version accepted")
	}
}
func TestMultipartRangesShareConcurrencyAndCancel(t *testing.T) {
	storage, snapshot, client := multipartRangeFixture(5)
	gate := make(chan struct{})
	started := make(chan struct{}, 4)
	for _, leaf := range client.leaves {
		leaf.gate = gate
		leaf.started = started
	}
	sources, release, err := openObjectRanges(context.Background(), storage, []Snapshot{snapshot}, client)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	httpClient := rangeHTTPClient(t)
	done := make(chan struct{}, 4)
	for i := 0; i < 4; i++ {
		target := sources[snapshot.Dataset].Ranges[i].URL
		go func() {
			defer func() { done <- struct{}{} }()
			rangeRequest(t, httpClient, "GET", target, "bytes=0-31")
		}()
	}
	for i := 0; i < 4; i++ {
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("requests did not start")
		}
	}
	response, _, err := rangeRequest(t, httpClient, "GET", sources[snapshot.Dataset].Ranges[4].URL, "bytes=0-31")
	if err != nil || response.StatusCode != http.StatusServiceUnavailable {
		t.Fatal("fifth part bypassed shared semaphore", err)
	}
	release()
	for i := 0; i < 4; i++ {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("release did not cancel upstream")
		}
	}
	if client.closed.Load() != 1 {
		t.Fatal("client ownership lost")
	}
}
func TestMultipartRangesRejectMetadataConfusion(t *testing.T) {
	for name, change := range map[string]func(*Snapshot){"total": func(s *Snapshot) { s.Bytes++ }, "rows": func(s *Snapshot) { s.Rows++ }, "key": func(s *Snapshot) { s.Parts[1].ObjectKey = s.Parts[0].ObjectKey }, "path": func(s *Snapshot) { s.Parts[0].Path = "/tmp/x" }, "root": func(s *Snapshot) { s.ObjectKey = "unexpected" }, "schema": func(s *Snapshot) { s.SchemaHash = "" }, "version": func(s *Snapshot) { s.Parts[0].ObjectVersion = "" }} {
		t.Run(name, func(t *testing.T) {
			storage, snapshot, client := multipartRangeFixture(2)
			change(&snapshot)
			_, release, err := openObjectRanges(context.Background(), storage, []Snapshot{snapshot}, client)
			if release != nil {
				release()
			}
			if err == nil || client.closed.Load() != 1 {
				t.Fatal("invalid metadata accepted or client leaked")
			}
		})
	}
}

func TestMultipartRangesTotalCapabilityLimit(t *testing.T) {
	storage, _, _ := multipartRangeFixture(1)
	var snapshots []Snapshot
	for i := 0; i < 4; i++ {
		_, snapshot, _ := multipartRangeFixture(256)
		old := snapshot.Dataset
		snapshot.Dataset = fmt.Sprintf("data%d", i)
		for j := range snapshot.Parts {
			snapshot.Parts[j].ObjectKey = strings.Replace(snapshot.Parts[j].ObjectKey, "/"+old+"/", "/"+snapshot.Dataset+"/", 1)
		}
		snapshots = append(snapshots, snapshot)
	}
	client := &multiRangeFixture{rangeFixtureClient: &rangeFixtureClient{}}
	sources, release, err := openObjectRanges(context.Background(), storage, snapshots, client)
	if err != nil || len(sources) != 4 {
		t.Fatal("1024 capabilities rejected", err)
	}
	release()
	_, extra, _ := multipartRangeFixture(1)
	extra.Dataset = "extra"
	// Preserve the exact tenant/generation while changing only the dataset segment.
	keyParts := strings.Split(extra.Parts[0].ObjectKey, "/")
	keyParts[len(keyParts)-2] = "extra"
	extra.Parts[0].ObjectKey = strings.Join(keyParts, "/")
	snapshots = append(snapshots, extra)
	client = &multiRangeFixture{rangeFixtureClient: &rangeFixtureClient{}}
	_, release, err = openObjectRanges(context.Background(), storage, snapshots, client)
	if release != nil {
		release()
	}
	if err == nil || client.closed.Load() != 1 {
		t.Fatal("1025 accepted or failed setup leaked reader")
	}
}
