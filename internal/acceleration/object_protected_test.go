//go:build linux || darwin

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/objectstore"
	"github.com/SYNEHQ/kelvo-go/internal/readerlease"
	"go.yaml.in/yaml/v3"
)

type protectedObjectEvent struct{ operation, key string }

type protectedObjectService struct {
	*fakeSnapshotObjects
	traceMu   sync.Mutex
	events    []protectedObjectEvent
	afterSeal func() error
	beforeGet func(string)
	infoHook  func(string, string, objectstore.Info) objectstore.Info
	bodyHook  func(string, string, io.ReadCloser, error) (io.ReadCloser, error)
}

func (s *protectedObjectService) trace(operation, key string) {
	s.traceMu.Lock()
	s.events = append(s.events, protectedObjectEvent{operation, key})
	s.traceMu.Unlock()
}

func (s *protectedObjectService) takeEvents() []protectedObjectEvent {
	s.traceMu.Lock()
	defer s.traceMu.Unlock()
	events := append([]protectedObjectEvent(nil), s.events...)
	s.events = nil
	return events
}

// Each runtime identity owns its wrapper; the fixture service models shared
// remote state and survives one identity's Close. Hooks are installed while idle.
type protectedObjectClient struct{ service *protectedObjectService }

func (c *protectedObjectClient) Close() {}
func (c *protectedObjectClient) info(operation, key string, info objectstore.Info) objectstore.Info {
	if c.service.infoHook != nil {
		return c.service.infoHook(operation, key, info)
	}
	return info
}
func (c *protectedObjectClient) body(operation, key string, body io.ReadCloser, err error) (io.ReadCloser, error) {
	if c.service.bodyHook != nil {
		return c.service.bodyHook(operation, key, body, err)
	}
	return body, err
}
func (c *protectedObjectClient) Get(ctx context.Context, key, version string) (io.ReadCloser, objectstore.Info, error) {
	c.service.trace("get", key)
	if c.service.beforeGet != nil {
		c.service.beforeGet(key)
	}
	body, info, err := c.service.fakeSnapshotObjects.Get(ctx, key, version)
	body, err = c.body("get", key, body, err)
	return body, c.info("get", key, info), err
}
func (c *protectedObjectClient) Head(ctx context.Context, key, version string) (objectstore.Info, error) {
	c.service.trace("head", key)
	info, err := c.service.fakeSnapshotObjects.Head(ctx, key, version)
	return c.info("head", key, info), err
}
func (c *protectedObjectClient) GetRange(ctx context.Context, key, version string, start, length int64) (io.ReadCloser, objectstore.Info, error) {
	c.service.trace("range", key)
	body, info, err := c.service.fakeSnapshotObjects.Get(ctx, key, version)
	if err != nil {
		return nil, info, err
	}
	defer body.Close()
	raw, err := io.ReadAll(body)
	if err != nil || start < 0 || length < 1 || start > int64(len(raw)) || length > int64(len(raw))-start {
		return nil, info, errors.New("invalid protected fixture range")
	}
	ranged, err := c.body("range", key, io.NopCloser(bytes.NewReader(raw[start:start+length])), nil)
	return ranged, c.info("range", key, info), err
}
func (c *protectedObjectClient) Put(ctx context.Context, key string, body io.ReadSeeker, size int64, digest string, condition objectstore.Condition) (objectstore.Info, error) {
	raw, err := io.ReadAll(body)
	if err != nil {
		return objectstore.Info{}, err
	}
	if _, err := body.Seek(0, io.SeekStart); err != nil {
		return objectstore.Info{}, err
	}
	operation := "payload"
	seal := false
	if strings.Contains(key, "/reader-leases/") {
		var next, old struct {
			Sealed  bool  `yaml:"sealed"`
			Readers []any `yaml:"readers"`
		}
		if err := yaml.Unmarshal(raw, &next); err != nil {
			return objectstore.Info{}, err
		}
		c.service.mu.Lock()
		previous := bytes.Clone(c.service.objects[key].data)
		c.service.mu.Unlock()
		_ = yaml.Unmarshal(previous, &old)
		switch {
		case next.Sealed && !old.Sealed:
			operation, seal = "seal", true
		case len(next.Readers) > len(old.Readers):
			operation = "pin"
		case next.Sealed:
			operation = "registry-update"
		default:
			operation = "stage"
		}
	} else if strings.HasSuffix(key, storeManifestName) {
		var root objectManifest
		if err := yaml.Unmarshal(raw, &root); err != nil {
			return objectstore.Info{}, err
		}
		operation = "writer"
		if root.Writer == nil && root.Committed != nil {
			operation = "publish"
		}
	}
	c.service.trace(operation, key)
	info, err := c.service.fakeSnapshotObjects.Put(ctx, key, body, size, digest, condition)
	if err == nil && seal && c.service.afterSeal != nil {
		if err := c.service.afterSeal(); err != nil {
			return objectstore.Info{}, err
		}
	}
	return c.info("put", key, info), err
}

func protectedPublicationFixture(t *testing.T) (*ObjectRuntime, *protectedObjectService, catalog.Config) {
	t.Helper()
	c := objectRuntimeTestConfig(t)
	service := &protectedObjectService{fakeSnapshotObjects: newFakeSnapshotObjects()}
	runtime, err := openObjectRuntime(c, func(catalog.ObjectLocation, catalog.ObjectCredentials, *objectstore.SharedTransport) (objectstore.Client, error) {
		return &protectedObjectClient{service}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := runtime.Close(ctx); err != nil {
			t.Error("protected fixture cleanup", err)
		}
	})
	return runtime, service, c
}

type protectedFaultBody struct {
	io.ReadCloser
	closed   atomic.Int32
	closeErr error
}

func (body *protectedFaultBody) Close() error {
	body.closed.Add(1)
	return errors.Join(body.ReadCloser.Close(), body.closeErr)
}

func protectedFingerprint(t *testing.T, config catalog.Config) string {
	t.Helper()
	fingerprint, err := config.DatasetFingerprint("events")
	if err != nil {
		t.Fatal(err)
	}
	return fingerprint
}

func requireProtectedBinding(t *testing.T, backend *objectBackend, service *protectedObjectService, generation string) objectCommitted {
	t.Helper()
	state, err := backend.readState(context.Background(), "events", backend.reader)
	if err != nil || state.manifest.Version != protectedManifestVersion || state.manifest.Committed == nil || state.manifest.Committed.Generation != generation {
		t.Fatalf("missing protected publication: %+v %v", state.manifest, err)
	}
	commit := *cloneObjectCommit(state.manifest.Committed)
	if commit.ReaderBinding == nil {
		t.Fatal("published without protection")
	}
	want, err := objectReaderBinding(commit.ReaderBinding.Reference, backend.config.ObjectStorage.ObjectLocation, &commit)
	if err != nil || want != *commit.ReaderBinding {
		t.Fatal("published identity or freshness changed after sealing", err)
	}
	key := backend.key("events", "reader-leases/"+generation+".yml")
	service.mu.Lock()
	raw := bytes.Clone(service.objects[key].data)
	service.mu.Unlock()
	var document struct {
		readerlease.Binding `yaml:",inline"`
		Sealed              bool `yaml:"sealed"`
	}
	if yaml.Unmarshal(raw, &document) != nil || !document.Sealed || document.Binding != want {
		t.Fatal("root differs from sealed registry identity")
	}
	return commit
}

func TestProtectedPublicationStagesBeforeUploadsAndRetainsBindings(t *testing.T) {
	for _, multipart := range []bool{false, true} {
		t.Run(map[bool]string{false: "single", true: "multipart"}[multipart], func(t *testing.T) {
			runtime, service, config := protectedPublicationFixture(t)
			backend := runtime.backend
			fp := protectedFingerprint(t, config)
			var snapshot Snapshot
			if multipart {
				writer := beginRemoteMultipart(t, backend)
				for range 2 {
					if err := writeRemoteMultipartPart(t, writer, "id", 2, 2); err != nil {
						t.Fatal(err)
					}
				}
				var err error
				snapshot, err = writer.Commit(fp)
				if err != nil {
					t.Fatal(err)
				}
			} else {
				snapshot = commitRemoteRecovery(t, backend, "id", fp)
			}
			committed := requireProtectedBinding(t, backend, service, snapshot.Generation)
			stage, upload, seal, publish := -1, -1, -1, -1
			for i, event := range service.takeEvents() {
				switch event.operation {
				case "stage":
					stage = i
				case "payload":
					if upload < 0 {
						upload = i
					}
				case "seal":
					seal = i
				case "publish":
					publish = i
				}
			}
			if stage < 0 || upload <= stage || seal <= upload || publish <= seal {
				t.Fatalf("publication order: stage=%d upload=%d seal=%d publish=%d", stage, upload, seal, publish)
			}
			_ = commitRemoteRecovery(t, backend, "id", fp)
			state, err := backend.readState(context.Background(), "events", backend.reader)
			if err != nil || len(state.manifest.History) != 1 || !sameObjectCommit(&committed, state.manifest.History[0]) {
				t.Fatal("history changed sealed identity or timestamp", err)
			}
		})
	}
}

func TestProtectedPublicationReconcilesSealAndRootWithoutRestamping(t *testing.T) {
	for _, ambiguousRoot := range []bool{false, true} {
		t.Run(map[bool]string{false: "seal", true: "seal_and_root"}[ambiguousRoot], func(t *testing.T) {
			runtime, service, config := protectedPublicationFixture(t)
			backend := runtime.backend
			service.afterSeal = func() error {
				// A successful Seal response is lost. Reconciliation must accept only
				// this exact already-sealed binding, not assign a fresh timestamp.
				return errObjectNetwork
			}
			if ambiguousRoot {
				service.publicationError = "after"
			}
			snapshot := commitRemoteRecovery(t, backend, "id", protectedFingerprint(t, config))
			commit := requireProtectedBinding(t, backend, service, snapshot.Generation)
			if !snapshot.RefreshedAt.Equal(commit.RefreshedAt) {
				t.Fatal("returned freshness differs from sealed publication")
			}
		})
	}
}

func TestProtectedPublicationRefusesChangedAmbiguousSeal(t *testing.T) {
	for _, mode := range []string{"content_sha256", "incarnation", "writer_owner"} {
		t.Run(mode, func(t *testing.T) {
			runtime, service, config := protectedPublicationFixture(t)
			backend := runtime.backend
			writer := beginRemoteMultipart(t, backend)
			if err := writeRemoteMultipartPart(t, writer, "id", 2, 2); err != nil {
				t.Fatal(err)
			}
			generation := writer.(*objectMultipartTransaction).base.owner
			service.afterSeal = func() error {
				key := backend.key("events", "reader-leases/"+generation+".yml")
				service.mu.Lock()
				defer service.mu.Unlock()
				object := service.objects[key]
				var document map[string]any
				if err := yaml.Unmarshal(object.data, &document); err != nil {
					return err
				}
				document[mode] = strings.Repeat("0", 64)
				raw, err := yaml.Marshal(document)
				if err != nil {
					return err
				}
				object.data = raw
				object.info.Size, object.info.SHA256 = int64(len(raw)), objectSHA256(raw)
				object.info.Version += "-changed"
				service.objects[key] = object
				return errObjectNetwork
			}
			snapshot, err := writer.Commit(protectedFingerprint(t, config))
			want := readerlease.ErrBinding
			if mode == "writer_owner" {
				want = readerlease.ErrFence
			}
			if !errors.Is(err, want) || snapshot.Generation != "" {
				t.Fatalf("ambiguous Seal accepted changed %s: %+v %v", mode, snapshot, err)
			}
			for _, event := range service.takeEvents() {
				if event.operation == "publish" {
					t.Fatal("mismatched sealed identity reached root publication")
				}
			}
		})
	}
}

func TestProtectedManifestRequiresServiceTime(t *testing.T) {
	for _, mode := range []string{"claim", "read"} {
		t.Run(mode, func(t *testing.T) {
			runtime, service, config := protectedPublicationFixture(t)
			backend := runtime.backend
			if mode == "read" {
				commitRemoteRecovery(t, backend, "id", protectedFingerprint(t, config))
			}
			service.takeEvents()
			var once sync.Once
			service.infoHook = func(operation, key string, info objectstore.Info) objectstore.Info {
				if strings.HasSuffix(key, storeManifestName) && (mode == "read" && operation == "get" || mode == "claim" && operation == "put") {
					once.Do(func() { info.ServerTime = time.Time{} })
				}
				return info
			}
			var err error
			if mode == "claim" {
				var writer RefreshWriter
				writer, err = backend.Begin(context.Background(), "events")
				if writer != nil {
					_ = writer.Abort()
					t.Fatal("writer escaped missing service clock")
				}
			} else {
				_, err = backend.Status(context.Background(), "events")
			}
			if !errors.Is(err, readerlease.ErrClock) {
				t.Fatalf("missing service clock accepted: %v", err)
			}
			for _, event := range service.takeEvents() {
				if event.operation == "stage" || event.operation == "pin" || event.operation == "payload" || protectedGenerationEvent(event) {
					t.Fatalf("missing service clock reached generation protection or data: %+v", event)
				}
			}
		})
	}
}

func TestProtectedPublicationKeepsRenewalLiveAndFencesAfterSeal(t *testing.T) {
	for _, mode := range []string{"renew", "expired", "stolen", "new_incarnation"} {
		t.Run(mode, func(t *testing.T) {
			runtime, service, config := protectedPublicationFixture(t)
			backend := runtime.backend
			writer := beginRemoteMultipart(t, backend)
			tx := writer.(*objectMultipartTransaction).base
			if err := writeRemoteMultipartPart(t, writer, "id", 2, 2); err != nil {
				t.Fatal(err)
			}
			other, err := readerlease.NewReference(config.Acceleration.TenantID, "events", strings.Repeat("b", 32))
			if err != nil {
				t.Fatal(err)
			}
			if mode == "new_incarnation" {
				other.Generation = tx.owner
			}
			service.afterSeal = func() error {
				if tx.stopping.Load() {
					return errors.New("writer renewal stopped before seal completed")
				}
				if mode == "renew" {
					return tx.renew(context.Background())
				}
				service.mutateManifest(t, backend.key("events", storeManifestName), func(root *objectManifest) {
					if mode == "expired" {
						root.Writer.ExpiresAt = time.Now().Add(-time.Minute).UTC()
					} else {
						root.Writer = &objectWriterLease{Owner: other.Generation, ExpiresAt: time.Now().Add(time.Hour).UTC(), ReaderReference: &other}
					}
				})
				return nil
			}
			snapshot, err := writer.Commit(protectedFingerprint(t, config))
			if mode == "renew" {
				if err != nil {
					t.Fatal(err)
				}
				requireProtectedBinding(t, backend, service, snapshot.Generation)
				return
			}
			if !errors.Is(err, ErrLeaseLost) || snapshot.Generation != "" {
				t.Fatalf("lost writer published: %+v %v", snapshot, err)
			}
			state, readErr := backend.readState(context.Background(), "events", backend.reader)
			if readErr != nil || state.manifest.Committed != nil {
				t.Fatal("failed sealed writer became current", readErr)
			}
			if (mode == "stolen" || mode == "new_incarnation") && (state.manifest.Writer == nil || !sameReaderReference(state.manifest.Writer.ReaderReference, &other)) {
				t.Fatal("old cleanup released the replacement writer")
			}
			for _, event := range service.takeEvents() {
				if event.operation == "publish" {
					t.Fatal("lost writer attempted root publication")
				}
			}
		})
	}
}

func TestProtectedManifestRejectsMixedIdentityAndCoercedScalars(t *testing.T) {
	runtime, service, config := protectedPublicationFixture(t)
	backend := runtime.backend
	snapshot := commitRemoteRecovery(t, backend, "id", protectedFingerprint(t, config))
	commit := requireProtectedBinding(t, backend, service, snapshot.Generation)
	state, err := backend.readState(context.Background(), "events", backend.reader)
	if err != nil {
		t.Fatal(err)
	}
	for _, version := range []int{2, 3, 4} {
		mixed := state.manifest
		mixed.Version = version
		if err := validateObjectManifest(mixed, "events"); err == nil {
			t.Fatal("legacy version admitted protected identity")
		}
	}
	missing := state.manifest
	missing.Committed = cloneObjectCommit(&commit)
	missing.Committed.ReaderBinding = nil
	if err := validateObjectManifest(missing, "events"); err == nil {
		t.Fatal("protected root admitted missing binding")
	}
	raw, err := yaml.Marshal(state.manifest)
	if err != nil || validateProtectedYAML(raw) != nil {
		t.Fatal("valid protected YAML refused", err)
	}
	for _, changed := range []string{
		strings.Replace(string(raw), "version: 5", "version: 5.0", 1),
		strings.Replace(string(raw), "version: 5", "version: \"5\"", 1),
		strings.Replace(string(raw), "version: 5", "version: &v 5", 1),
		strings.Replace(string(raw), "version: 5", "version: 5\nversion: 5", 1),
	} {
		if changed == string(raw) || validateProtectedYAML([]byte(changed)) == nil {
			t.Fatal("coerced or duplicate protected identity accepted")
		}
	}
}
