//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.yaml.in/yaml/v3"
)

type protectedFixtureObject struct {
	data    []byte
	version string
	digest  string
}

// This is an atomic exact-key TLS protocol fixture, not an S3 implementation.
// It checks credential roles, staging, sealing and pin-before-generation reads.
// It does not certify AWS signature verification or a provider's IAM semantics.
type protectedObjectService struct {
	mu          sync.Mutex
	objects     map[string]protectedFixtureObject
	sequence    int
	violations  int
	ranges      int
	active      atomic.Int64
	blockRead   atomic.Bool
	readStarted chan struct{}
}

func protectedObjectTLS(t *testing.T) (*protectedObjectService, string) {
	t.Helper()
	service := &protectedObjectService{objects: map[string]protectedFixtureObject{}, readStarted: make(chan struct{}, 1)}
	server := httptest.NewTLSServer(http.HandlerFunc(service.serve))
	t.Cleanup(server.Close)
	cert := filepath.Join(t.TempDir(), "fixture-ca.pem")
	if err := os.WriteFile(cert, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	// The gate runs in a fresh test subprocess before loading system roots.
	t.Setenv("SSL_CERT_FILE", cert)
	t.Setenv("SSL_CERT_DIR", t.TempDir())
	return service, server.URL
}

func (s *protectedObjectService) registryLocked(key string) (map[string]any, bool) {
	object, ok := s.objects[key]
	if !ok {
		return nil, false
	}
	var document map[string]any
	if yaml.Unmarshal(object.data, &document) != nil {
		return nil, false
	}
	return document, true
}

func (s *protectedObjectService) reject(w http.ResponseWriter) {
	s.violations++
	http.Error(w, "fixture rejected request", http.StatusForbidden)
}

func (s *protectedObjectService) serve(w http.ResponseWriter, r *http.Request) {
	s.active.Add(1)
	defer s.active.Add(-1)
	w.Header().Set("Date", time.Now().UTC().Format(http.TimeFormat))
	var role string
	for _, candidate := range []string{"reader", "writer", "registry"} {
		if strings.Contains(r.Header.Get("Authorization"), "Credential="+candidate+"/") {
			role = candidate
		}
	}
	key := r.URL.Path
	registry := strings.Contains(key, "/reader-leases/")
	data, err := io.ReadAll(io.LimitReader(r.Body, 8<<20))
	if err != nil || len(data) == 8<<20 {
		http.Error(w, "fixture body unavailable", http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !strings.HasPrefix(key, "/fixtures/cache/tenant-a/") || role == "" || (registry && role != "registry") || (!registry && role == "registry") || (role == "reader" && r.Method == http.MethodPut) {
		s.reject(w)
		return
	}
	base := path.Base(key)
	if !registry && base != "current.yaml" && len(base) >= 32 {
		generation := base[:32]
		document, exists := s.registryLocked(path.Dir(key) + "/reader-leases/" + generation + ".yml")
		if !exists || (r.Method != http.MethodPut && role == "reader" && (document["sealed"] != true || lenValue(document["readers"]) == 0)) {
			s.reject(w)
			return
		}
	}
	object, exists := s.objects[key]
	if r.Header.Get("If-None-Match") == "*" && exists || r.Header.Get("If-Match") != "" && (!exists || r.Header.Get("If-Match") != object.version) {
		w.WriteHeader(http.StatusPreconditionFailed)
		return
	}
	if r.Method == http.MethodPut {
		sum := sha256.Sum256(data)
		digest := hex.EncodeToString(sum[:])
		if r.Header.Get("x-amz-meta-kelvo-sha256") != digest || (r.Header.Get("If-None-Match") == "" && r.Header.Get("If-Match") == "") {
			s.reject(w)
			return
		}
		if base == "current.yaml" {
			var root struct {
				Committed *struct {
					Generation string `yaml:"generation"`
				} `yaml:"committed"`
			}
			if yaml.Unmarshal(data, &root) != nil {
				s.reject(w)
				return
			}
			if root.Committed != nil {
				document, exists := s.registryLocked(path.Dir(key) + "/reader-leases/" + root.Committed.Generation + ".yml")
				if !exists || document["sealed"] != true {
					s.reject(w)
					return
				}
			}
		}
		s.sequence++
		object = protectedFixtureObject{data: data, digest: digest, version: fmt.Sprintf("\"fixture-%d\"", s.sequence)}
		s.objects[key] = object
		w.Header().Set("ETag", object.version)
		w.Header().Set("Content-Length", "0")
		w.WriteHeader(http.StatusOK)
		return
	}
	if !exists {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if r.Method != http.MethodHead && r.Method != http.MethodGet {
		s.reject(w)
		return
	}
	w.Header().Set("ETag", object.version)
	w.Header().Set("x-amz-meta-kelvo-sha256", object.digest)
	if value := r.Header.Get("Range"); value != "" {
		var first, last int64
		if r.Method != http.MethodGet || role != "reader" || r.Header.Get("If-Match") == "" {
			s.reject(w)
			return
		}
		if _, err := fmt.Sscanf(value, "bytes=%d-%d", &first, &last); err != nil || first < 0 || last < first || last >= int64(len(object.data)) {
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		s.ranges++
		if s.blockRead.Load() {
			select {
			case s.readStarted <- struct{}{}:
			default:
			}
			s.mu.Unlock()
			<-r.Context().Done()
			s.mu.Lock()
			return
		}
		w.Header().Set("Content-Length", strconv.FormatInt(last-first+1, 10))
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", first, last, len(object.data)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(object.data[first : last+1])
		return
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(object.data)))
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodGet {
		_, _ = w.Write(object.data)
	}
}

func lenValue(value any) int {
	list, _ := value.([]any)
	return len(list)
}

func (s *protectedObjectService) readers() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	readers := 0
	for key := range s.objects {
		if strings.Contains(key, "/reader-leases/") {
			document, _ := s.registryLocked(key)
			readers += lenValue(document["readers"])
		}
	}
	return readers
}

func (s *protectedObjectService) invalidateBindings() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, object := range s.objects {
		if strings.Contains(key, "/reader-leases/") {
			object.data = []byte("invalid: fixture-binding-loss\n")
			sum := sha256.Sum256(object.data)
			object.digest = hex.EncodeToString(sum[:])
			s.sequence++
			object.version = fmt.Sprintf("\"fixture-%d\"", s.sequence)
			s.objects[key] = object
		}
	}
}
