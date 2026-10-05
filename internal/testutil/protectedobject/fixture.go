// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0

// Package protectedobject provides an exact-key TLS/CAS service for acceptance
// tests. It checks tenant-bound roles, staging, sealing and pin-before-read
// ordering. It does not implement AWS signature verification or certify IAM.
package protectedobject

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"go.yaml.in/yaml/v3"
)

type Options struct{ Tenants []string }

// Object is a detached copy; callers cannot mutate the service's stored bytes.
type Object struct {
	Data            []byte
	Version, Digest string
}

type Counters struct {
	Requests, RangeRequests  int
	RegistryWrites           int
	PinAcquires, PinReleases int
	Violations               int
	PayloadWriteRequests     int
	StageWrites, SealWrites  int
}

// Stats contains detached maps. Requests includes rejected requests; registry
// writes and pin changes count only successful CAS commits. Readers counts
// durable entries, not client-local custody or proof of live readers.
type Stats struct {
	Total          Counters
	Tenants        map[string]Counters
	Datasets       map[string]map[string]Counters
	ActiveHandlers int
	Readers        int
}

type Point uint8

const (
	RangeBeforeResponse Point = iota + 1
	// RangeFinalByte holds the final HTTP response byte, not client Body.Close.
	RangeFinalByte
	// PinReleaseBeforeCAS waits for Release even if the HTTP client cancels.
	// This models an ambiguous remote commit, not unjoined local provider work.
	PinReleaseBeforeCAS
)

type gateKey struct {
	point           Point
	tenant, dataset string
}

type Gate struct {
	entered, released chan struct{}
	enterOnce         sync.Once
	releaseOnce       sync.Once
}

func (g *Gate) Entered() <-chan struct{} { return g.entered }
func (g *Gate) Release()                 { g.releaseOnce.Do(func() { close(g.released) }) }

func (g *Gate) wait(ctx context.Context, cancelAware bool) bool {
	if g == nil {
		return true
	}
	g.enterOnce.Do(func() { close(g.entered) })
	if !cancelAware {
		<-g.released
		return true
	}
	select {
	case <-g.released:
		return ctx.Err() == nil
	case <-ctx.Done():
		return false
	}
}

type identity struct{ tenant, role string }

type Service struct {
	Endpoint string
	mu       sync.Mutex
	objects  map[string]Object
	sequence int
	storage  map[string]catalog.ObjectStorage
	roles    map[string]identity
	gates    map[gateKey]*Gate
	stats    Stats
	idle     chan struct{}
}

var fixtureSequence atomic.Uint64

// New sets synthetic credentials and a fixture CA through t.Setenv. Run it in a
// fresh test subprocess before any TLS client loads Go's cached system roots.
// As with t.Setenv, callers must not run these fixtures in parallel tests.
func New(t *testing.T, opts Options) *Service {
	t.Helper()
	s := &Service{objects: map[string]Object{}, storage: map[string]catalog.ObjectStorage{}, roles: map[string]identity{},
		gates: map[gateKey]*Gate{}, idle: make(chan struct{}), stats: Stats{Tenants: map[string]Counters{}, Datasets: map[string]map[string]Counters{}}}
	close(s.idle)
	if len(opts.Tenants) == 0 {
		t.Fatal("protected fixture requires at least one tenant")
	}
	fixtureID := fixtureSequence.Add(1)
	for index, tenant := range opts.Tenants {
		if !validSegment(tenant) {
			t.Fatal("protected fixture requires a plain tenant path component")
		}
		if _, exists := s.storage[tenant]; exists {
			t.Fatal("protected fixture tenant is repeated")
		}
		credentials := func(role string) catalog.ObjectCredentials {
			prefix := fmt.Sprintf("KELVO_SOURCE_PROTECTED_%d_T%d_%s", fixtureID, index, strings.ToUpper(role))
			key := fmt.Sprintf("fixture-%d-%d-%s", fixtureID, index, role)
			t.Setenv(prefix+"_ID", key)
			t.Setenv(prefix+"_SECRET", "fixture-only-secret-"+key)
			s.roles[key] = identity{tenant: tenant, role: role}
			return catalog.ObjectCredentials{AccessKeyIDEnv: prefix + "_ID", SecretAccessKeyEnv: prefix + "_SECRET"}
		}
		s.storage[tenant] = catalog.ObjectStorage{
			ObjectLocation:  catalog.ObjectLocation{Provider: "s3", Bucket: "fixtures", Prefix: "cache", Region: "us-east-1"},
			ReadCredentials: credentials("reader"), WriteCredentials: credentials("writer"),
			ReaderRegistry: &catalog.ObjectReaderRegistry{Credentials: credentials("registry")},
		}
		s.stats.Tenants[tenant] = Counters{}
		s.stats.Datasets[tenant] = map[string]Counters{}
	}
	server := httptest.NewUnstartedServer(http.HandlerFunc(s.serve))
	server.Config.ReadHeaderTimeout = 5 * time.Second
	server.Config.ReadTimeout = 15 * time.Second
	server.Config.WriteTimeout = 15 * time.Second
	server.Config.IdleTimeout = time.Second
	server.StartTLS()
	t.Cleanup(func() {
		s.mu.Lock()
		for _, gate := range s.gates {
			gate.Release()
		}
		s.mu.Unlock()
		server.Close()
	})
	s.Endpoint = server.URL
	cert := filepath.Join(t.TempDir(), "fixture-ca.pem")
	if err := os.WriteFile(cert, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SSL_CERT_FILE", cert)
	t.Setenv("SSL_CERT_DIR", t.TempDir())
	return s
}

func validSegment(value string) bool {
	return value != "" && !strings.Contains(value, "/") && catalog.ValidateObjectKey(value) == nil
}

func (s *Service) Storage(tenant string) catalog.ObjectStorage {
	storage, exists := s.storage[tenant]
	if !exists {
		panic("protected fixture tenant is not configured")
	}
	storage.Endpoint = s.Endpoint
	registry := *storage.ReaderRegistry
	storage.ReaderRegistry = &registry
	return storage
}

func (s *Service) Hold(point Point, tenant, dataset string) *Gate {
	if point < RangeBeforeResponse || point > PinReleaseBeforeCAS || !validSegment(dataset) {
		panic("protected fixture gate is invalid")
	}
	if _, exists := s.storage[tenant]; !exists {
		panic("protected fixture gate tenant is not configured")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := gateKey{point, tenant, dataset}
	if previous := s.gates[key]; previous != nil {
		select {
		case <-previous.released:
		default:
			panic("protected fixture gate is already held")
		}
	}
	gate := &Gate{entered: make(chan struct{}), released: make(chan struct{})}
	s.gates[key] = gate
	return gate
}

func (s *Service) Snapshot() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	snapshot := s.stats
	snapshot.Tenants = make(map[string]Counters, len(s.stats.Tenants))
	snapshot.Datasets = make(map[string]map[string]Counters, len(s.stats.Datasets))
	for tenant, counters := range s.stats.Tenants {
		snapshot.Tenants[tenant] = counters
		snapshot.Datasets[tenant] = make(map[string]Counters, len(s.stats.Datasets[tenant]))
		for dataset, counters := range s.stats.Datasets[tenant] {
			snapshot.Datasets[tenant][dataset] = counters
		}
		snapshot.Readers += s.readersLocked(tenant)
	}
	return snapshot
}

func (s *Service) Root(tenant, dataset string) (Object, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	object, exists := s.objects["/fixtures/cache/"+tenant+"/"+dataset+"/current.yaml"]
	object.Data = bytes.Clone(object.Data)
	return object, exists
}

// ImmutableObjects copies generation payloads and descriptors, including their
// exact versions. It excludes the mutable root and reader registry documents.
func (s *Service) ImmutableObjects(tenant, dataset string) map[string]Object {
	if !validSegment(tenant) || !validSegment(dataset) {
		panic("protected fixture object selection is invalid")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make(map[string]Object)
	prefix := "/fixtures/cache/" + tenant + "/" + dataset + "/"
	for key, object := range s.objects {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		name := strings.TrimPrefix(key, prefix)
		if name == "current.yaml" || strings.Contains(name, "/") {
			continue
		}
		object.Data = bytes.Clone(object.Data)
		result[key] = object
	}
	return result
}

func (s *Service) Readers(tenant string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.readersLocked(tenant)
}

func (s *Service) readersLocked(tenant string) int {
	readers := 0
	for key, object := range s.objects {
		if strings.HasPrefix(key, "/fixtures/cache/"+tenant+"/") && strings.Contains(key, "/reader-leases/") {
			document, _ := decodeRegistry(object.Data)
			readers += len(document.Readers)
		}
	}
	return readers
}

func (s *Service) InvalidateBindings(tenant string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key := range s.objects {
		if strings.HasPrefix(key, "/fixtures/cache/"+tenant+"/") && strings.Contains(key, "/reader-leases/") {
			s.storeLocked(key, []byte("invalid: fixture-binding-loss\n"))
		}
	}
}

// WaitIdle joins current HTTP handlers only. Callers must separately join the
// client runtime, containment manager and admission pool before claiming cleanup.
func (s *Service) WaitIdle(ctx context.Context) error {
	s.mu.Lock()
	idle := s.idle
	s.mu.Unlock()
	select {
	case <-idle:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Service) countLocked(tenant, dataset string, update func(*Counters)) {
	update(&s.stats.Total)
	if counters, exists := s.stats.Tenants[tenant]; exists {
		update(&counters)
		s.stats.Tenants[tenant] = counters
		if validSegment(dataset) {
			counters = s.stats.Datasets[tenant][dataset]
			update(&counters)
			s.stats.Datasets[tenant][dataset] = counters
		}
	}
}

func (s *Service) storeLocked(key string, data []byte) Object {
	s.sequence++
	sum := sha256.Sum256(data)
	object := Object{Data: bytes.Clone(data), Version: fmt.Sprintf("\"fixture-%d\"", s.sequence), Digest: hex.EncodeToString(sum[:])}
	s.objects[key] = object
	return object
}

type registryDocument struct {
	Tenant     string `yaml:"tenant"`
	Dataset    string `yaml:"dataset"`
	Generation string `yaml:"generation"`
	Sealed     bool   `yaml:"sealed"`
	Readers    []struct {
		ID string `yaml:"id"`
	} `yaml:"readers"`
}

func decodeRegistry(data []byte) (registryDocument, bool) {
	var document registryDocument
	err := yaml.Unmarshal(data, &document)
	return document, err == nil && document.Tenant != "" && document.Dataset != "" && document.Generation != ""
}

func pinChanges(before, after registryDocument) (acquires, releases int) {
	previous, next := map[string]bool{}, map[string]bool{}
	for _, reader := range before.Readers {
		previous[reader.ID] = true
	}
	for _, reader := range after.Readers {
		next[reader.ID] = true
	}
	for id := range previous {
		if !next[id] {
			releases++
		}
	}
	for id := range next {
		if !previous[id] {
			acquires++
		}
	}
	return acquires, releases
}
