//go:build linux || darwin

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/objectstore"
	"github.com/SYNEHQ/kelvo-go/internal/readerlease"
	"go.yaml.in/yaml/v3"
)

// gcObjects adds listing and exact-version deletion to the shared protected
// fixture. deleteFault runs before each delete and may fail it.
type gcObjects struct {
	service     *protectedObjectService
	listFault   error
	deleteFault func(key string) error
	deletes     int
}

func (g *gcObjects) List(ctx context.Context, prefix, after string, limit int) (objectstore.Page, error) {
	if g.listFault != nil {
		return objectstore.Page{}, g.listFault
	}
	g.service.mu.Lock()
	keys := []string{}
	for key := range g.service.objects {
		if strings.HasPrefix(key, prefix) && key > after {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	page := objectstore.Page{}
	for _, key := range keys {
		if len(page.Entries) == limit {
			page.Next = page.Entries[len(page.Entries)-1].Key
			break
		}
		page.Entries = append(page.Entries, objectstore.Entry{Key: key, Size: g.service.objects[key].info.Size})
	}
	g.service.mu.Unlock()
	return page, nil
}

func (g *gcObjects) Delete(ctx context.Context, key, version string) error {
	if g.deleteFault != nil {
		if err := g.deleteFault(key); err != nil {
			return err
		}
	}
	g.service.mu.Lock()
	defer g.service.mu.Unlock()
	object, ok := g.service.objects[key]
	if !ok {
		return objectstore.ErrNotFound
	}
	if object.info.Version != version {
		return objectstore.ErrConflict
	}
	delete(g.service.objects, key)
	g.deletes++
	return nil
}

type gcFixture struct {
	runtime  *ObjectRuntime
	service  *protectedObjectService
	objects  *gcObjects
	orphan   Snapshot
	current  Snapshot
	binding  readerlease.Binding
	payload  []byte
	fp       string
	registry *readerlease.Registry
}

// Two publications, then the first falls out of the retained catalog: it is
// an unreferenced, sealed, protected generation.
func newGCFixture(t *testing.T) *gcFixture {
	t.Helper()
	runtime, service, config := protectedPublicationFixture(t)
	f := &gcFixture{runtime: runtime, service: service, objects: &gcObjects{service: service}, fp: protectedFingerprint(t, config)}
	f.orphan = commitRemoteRecovery(t, runtime.backend, "id", f.fp)
	state, err := runtime.backend.readState(context.Background(), "events", runtime.backend.reader)
	if err != nil {
		t.Fatal(err)
	}
	f.binding = *state.manifest.Committed.ReaderBinding
	f.current = commitRemoteRecovery(t, runtime.backend, "id", f.fp)
	f.payload = f.object(t, f.current.Generation+".parquet")
	f.registry = runtime.registry
	return f
}

func (f *gcFixture) dropHistory(t *testing.T) {
	t.Helper()
	f.service.mutateManifest(t, f.runtime.backend.key("events", storeManifestName), func(m *objectManifest) { m.History = nil })
}

func (f *gcFixture) object(t *testing.T, name string) []byte {
	t.Helper()
	f.service.mu.Lock()
	defer f.service.mu.Unlock()
	object, ok := f.service.objects[f.runtime.backend.key("events", name)]
	if !ok {
		return nil
	}
	return bytes.Clone(object.data)
}

func (f *gcFixture) collector(del bool) *remoteCollector {
	client := &protectedObjectClient{f.service}
	return &remoteCollector{backend: f.runtime.backend, data: client, list: f.objects, registry: f.registry, regDel: f.objects,
		options: RemoteGCOptions{Delete: del, Grace: time.Hour}}
}

func (f *gcFixture) run(t *testing.T, del bool) RemoteGCReport {
	t.Helper()
	report, err := f.collector(del).run(context.Background(), "events")
	if err != nil {
		t.Fatal(err)
	}
	return report
}

// backdate rewrites the quarantine marker as if it was written long ago.
func (f *gcFixture) backdate(t *testing.T) {
	t.Helper()
	key := f.runtime.backend.key("events", "gc/"+f.orphan.Generation+".quarantine.yaml")
	f.service.mu.Lock()
	defer f.service.mu.Unlock()
	var marker gcMarker
	if err := yaml.Unmarshal(f.service.objects[key].data, &marker); err != nil {
		t.Fatal(err)
	}
	marker.QuarantinedAt = marker.QuarantinedAt.Add(-48 * time.Hour)
	raw, _ := yaml.Marshal(marker)
	f.service.sequence++
	f.service.objects[key] = fakeSnapshotObject{data: raw, info: objectstore.Info{Size: int64(len(raw)), SHA256: objectSHA256(raw), Version: fmt.Sprintf("revision-%d", f.service.sequence)}}
}

func generationAction(r RemoteGCReport, generation string) RemoteGCGeneration {
	for _, g := range r.Generations {
		if g.Generation == generation {
			return g
		}
	}
	return RemoteGCGeneration{}
}

func (f *gcFixture) requireCurrentUnchanged(t *testing.T) {
	t.Helper()
	snapshot, err := f.runtime.backend.Status(context.Background(), "events")
	if err != nil || snapshot.Generation != f.current.Generation || !bytes.Equal(f.object(t, f.current.Generation+".parquet"), f.payload) {
		t.Fatal("current generation changed during collection", err)
	}
	if f.object(t, "reader-leases/"+f.current.Generation+".yml") == nil {
		t.Fatal("current registry document was collected")
	}
}

func TestRemoteGCDryRunClassifiesWithoutWriting(t *testing.T) {
	f := newGCFixture(t)
	report := f.run(t, false)
	if generationAction(report, f.orphan.Generation).Class != "retained" || generationAction(report, f.current.Generation).Class != "current" {
		t.Fatalf("history not classified as retained: %+v", report)
	}
	f.dropHistory(t)
	before := len(f.service.objects)
	report = f.run(t, false)
	if report.Mode != "dry-run" || !report.Complete || generationAction(report, f.orphan.Generation).Class != "orphan" || report.Deleted != 0 || report.Quarantined != 0 {
		t.Fatalf("dry run report: %+v", report)
	}
	if report.Classes["orphan"].Bytes <= 0 || report.Classes["current"].Bytes <= 0 || report.Classes["manifest"].Objects != 1 {
		t.Fatalf("byte accounting missing: %+v", report.Classes)
	}
	if len(f.service.objects) != before || f.objects.deletes != 0 {
		t.Fatal("dry run wrote or deleted objects")
	}
	if _, err := f.registry.Acquire(context.Background(), f.binding); err != nil {
		t.Fatal("dry run retired a generation", err)
	}
}

func TestRemoteGCQuarantineGraceThenDeleteIsIdempotent(t *testing.T) {
	f := newGCFixture(t)
	f.dropHistory(t)
	report := f.run(t, true)
	if generationAction(report, f.orphan.Generation).Action != "quarantined" || f.objects.deletes != 0 {
		t.Fatalf("first run must only quarantine: %+v", report)
	}
	if _, err := f.registry.Acquire(context.Background(), f.binding); !errors.Is(err, readerlease.ErrRetired) {
		t.Fatal("quarantine did not block new readers", err)
	}
	if got := generationAction(f.run(t, true), f.orphan.Generation).Action; got != "quarantined" {
		t.Fatal("deleted before grace", got)
	}
	f.backdate(t)
	if got := generationAction(f.run(t, true), f.orphan.Generation).Action; got != "deleted" {
		t.Fatal("not deleted after grace", got)
	}
	for _, name := range []string{f.orphan.Generation + ".parquet", "reader-leases/" + f.orphan.Generation + ".yml", "gc/" + f.orphan.Generation + ".quarantine.yaml"} {
		if f.object(t, name) != nil {
			t.Fatal("left behind", name)
		}
	}
	if r := f.run(t, true); generationAction(r, f.orphan.Generation).Generation != "" || r.Deleted != 0 {
		t.Fatalf("rerun not idempotent: %+v", r)
	}
	f.requireCurrentUnchanged(t)
}

func TestRemoteGCConcurrentReaderKeepsReadingAndBlocksDeletion(t *testing.T) {
	f := newGCFixture(t)
	lease, err := f.registry.Acquire(context.Background(), f.binding)
	if err != nil {
		t.Fatal(err)
	}
	f.dropHistory(t)
	f.run(t, true)
	f.backdate(t)
	if got := generationAction(f.run(t, true), f.orphan.Generation).Action; got != "kept: readers may be live" {
		t.Fatal("collected under a live reader", got)
	}
	if err := lease.Check(); err != nil || f.object(t, f.orphan.Generation+".parquet") == nil {
		t.Fatal("reader lost data during collection", err)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	if got := generationAction(f.run(t, true), f.orphan.Generation).Action; got != "deleted" {
		t.Fatal("not deleted after reader closed", got)
	}
	f.requireCurrentUnchanged(t)
}

func TestRemoteGCExpiredCrashedLeaseVersusLiveLease(t *testing.T) {
	f := newGCFixture(t)
	f.dropHistory(t)
	f.run(t, true)
	// A crashed reader: entry stays durable, nobody renews or releases it.
	key := f.runtime.backend.key("events", "reader-leases/"+f.orphan.Generation+".yml")
	f.service.mu.Lock()
	object := f.service.objects[key]
	var doc map[string]any
	_ = yaml.Unmarshal(object.data, &doc)
	now := time.Now().UTC().Truncate(time.Second)
	doc["readers"] = []map[string]any{{"id": strings.Repeat("1", 64), "owner": strings.Repeat("2", 64), "sequence": 1, "renewed_at": now, "expires_at": now.Add(time.Minute)}}
	raw, _ := yaml.Marshal(doc)
	f.service.sequence++
	f.service.objects[key] = fakeSnapshotObject{data: raw, info: objectstore.Info{Size: int64(len(raw)), SHA256: objectSHA256(raw), Version: fmt.Sprintf("revision-%d", f.service.sequence)}}
	f.service.mu.Unlock()
	f.backdate(t)
	if got := generationAction(f.run(t, true), f.orphan.Generation).Action; got != "kept: readers may be live" {
		t.Fatal("unexpired crashed lease ignored", got)
	}
	f.service.mu.Lock()
	f.service.clockOffset = 2 * time.Minute
	f.service.mu.Unlock()
	if got := generationAction(f.run(t, true), f.orphan.Generation).Action; got != "deleted" {
		t.Fatal("expired crashed lease still blocks", got)
	}
}

func TestRemoteGCCrashBetweenQuarantineAndDeleteResumes(t *testing.T) {
	for _, stage := range []string{".parquet", "reader-leases/", "/gc/"} {
		t.Run(strings.Trim(stage, "/."), func(t *testing.T) {
			f := newGCFixture(t)
			f.dropHistory(t)
			f.run(t, true)
			f.backdate(t)
			f.objects.deleteFault = func(key string) error {
				if strings.Contains(key, f.orphan.Generation) && strings.Contains(key, stage) {
					return errors.New("crash: provider outcome unknown")
				}
				return nil
			}
			if got := generationAction(f.run(t, true), f.orphan.Generation).Action; !strings.HasPrefix(got, "kept:") {
				t.Fatal("uncertain deletion reported success", got)
			}
			f.requireCurrentUnchanged(t)
			if _, err := f.registry.Acquire(context.Background(), f.binding); err == nil {
				t.Fatal("partially collected generation admitted a reader")
			}
			f.objects.deleteFault = nil
			if got := generationAction(f.run(t, true), f.orphan.Generation).Action; got != "deleted" {
				t.Fatal("did not resume", got)
			}
			f.requireCurrentUnchanged(t)
		})
	}
}

func TestRemoteGCFailsClosed(t *testing.T) {
	t.Run("listing error", func(t *testing.T) {
		f := newGCFixture(t)
		f.dropHistory(t)
		f.objects.listFault = errors.New("permission denied")
		if _, err := f.collector(true).run(context.Background(), "events"); err == nil || f.objects.deletes != 0 {
			t.Fatal("listing failure did not fail closed", err)
		}
	})
	t.Run("marker write fails (full or denied)", func(t *testing.T) {
		f := newGCFixture(t)
		f.dropHistory(t)
		f.service.mu.Lock()
		f.service.failPayload = true
		f.service.mu.Unlock()
		if got := generationAction(f.run(t, true), f.orphan.Generation).Action; got != "kept: quarantine write failed" {
			t.Fatal(got)
		}
		f.service.mu.Lock()
		f.service.failPayload = false
		f.service.mu.Unlock()
		if _, err := f.registry.Acquire(context.Background(), f.binding); err != nil {
			t.Fatal("failed quarantine retired the generation", err)
		}
	})
	t.Run("delete permission denied", func(t *testing.T) {
		f := newGCFixture(t)
		f.dropHistory(t)
		f.run(t, true)
		f.backdate(t)
		f.objects.deleteFault = func(string) error { return errors.New("access denied") }
		if got := generationAction(f.run(t, true), f.orphan.Generation).Action; !strings.HasPrefix(got, "kept:") || f.object(t, "reader-leases/"+f.orphan.Generation+".yml") == nil {
			t.Fatal("registry deleted after data deletion failed", got)
		}
	})
	t.Run("active writer", func(t *testing.T) {
		f := newGCFixture(t)
		f.dropHistory(t)
		writer, err := f.runtime.backend.Begin(context.Background(), "events")
		if err != nil {
			t.Fatal(err)
		}
		defer writer.Abort()
		r := f.run(t, true)
		if !r.WriterHeld || generationAction(r, f.orphan.Generation).Class != "staging" || r.Quarantined != 0 {
			t.Fatalf("collected while a writer was active: %+v", r)
		}
	})
	t.Run("legacy namespace refuses delete", func(t *testing.T) {
		f := newGCFixture(t)
		c := f.collector(true)
		c.registry = nil
		if _, err := c.run(context.Background(), "events"); !errors.Is(err, ErrProtectionRequired) {
			t.Fatal(err)
		}
		c.options.Grace = time.Minute
		c.registry = f.registry
		if _, err := c.run(context.Background(), "events"); err == nil {
			t.Fatal("short grace accepted")
		}
	})
	t.Run("unregistered orphan is kept", func(t *testing.T) {
		f := newGCFixture(t)
		f.dropHistory(t)
		f.service.mu.Lock()
		delete(f.service.objects, f.runtime.backend.key("events", "reader-leases/"+f.orphan.Generation+".yml"))
		f.service.mu.Unlock()
		if got := generationAction(f.run(t, true), f.orphan.Generation).Action; got != "kept: no registry document" {
			t.Fatal(got)
		}
	})
}

