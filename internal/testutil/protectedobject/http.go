// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package protectedobject

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
)

func (s *Service) identify(r *http.Request, tenant string, registry bool) (identity, bool) {
	authorization := r.Header.Get("Authorization")
	if !strings.HasPrefix(authorization, "AWS4-HMAC-SHA256 Credential=") {
		return identity{}, false
	}
	key, _, ok := strings.Cut(strings.TrimPrefix(authorization, "AWS4-HMAC-SHA256 Credential="), "/")
	role, known := s.roles[key]
	valid := ok && known && role.tenant == tenant && (registry == (role.role == "registry")) &&
		(r.Method == http.MethodGet || r.Method == http.MethodHead || r.Method == http.MethodPut && role.role != "reader")
	return role, valid
}

func (s *Service) rejectLocked(w http.ResponseWriter, tenant, dataset string) {
	s.countLocked(tenant, dataset, func(c *Counters) { c.Violations++ })
	http.Error(w, "fixture rejected request", http.StatusForbidden)
}

func conditionMatches(r *http.Request, object Object, exists bool) bool {
	return !(r.Header.Get("If-None-Match") == "*" && exists ||
		r.Header.Get("If-Match") != "" && (!exists || r.Header.Get("If-Match") != object.Version))
}

func (s *Service) serve(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Path
	parts := strings.Split(strings.TrimPrefix(key, "/fixtures/cache/"), "/")
	var tenant, dataset string
	if len(parts) >= 2 {
		tenant, dataset = parts[0], parts[1]
	}
	registry := len(parts) == 4 && parts[2] == "reader-leases"
	s.mu.Lock()
	if s.stats.ActiveHandlers == 0 {
		s.idle = make(chan struct{})
	}
	s.stats.ActiveHandlers++
	s.countLocked(tenant, dataset, func(c *Counters) {
		c.Requests++
		if r.Header.Get("Range") != "" {
			c.RangeRequests++
		}
		if r.Method == http.MethodPut && !registry && path.Base(key) != "current.yaml" {
			c.PayloadWriteRequests++
		}
	})
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.stats.ActiveHandlers--
		if s.stats.ActiveHandlers == 0 {
			close(s.idle)
		}
		s.mu.Unlock()
	}()
	w.Header().Set("Date", time.Now().UTC().Format(http.TimeFormat))
	data, err := io.ReadAll(io.LimitReader(r.Body, 8<<20))
	if err != nil || len(data) == 8<<20 {
		http.Error(w, "fixture body unavailable", http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	role, authorized := s.identify(r, tenant, registry)
	if !strings.HasPrefix(key, "/fixtures/cache/") || path.Clean(key) != key ||
		!validSegment(tenant) || !validSegment(dataset) || (len(parts) != 3 && !registry) || !authorized {
		s.rejectLocked(w, tenant, dataset)
		return
	}
	base := path.Base(key)
	if !registry && base != "current.yaml" {
		if len(base) < 32 {
			s.rejectLocked(w, tenant, dataset)
			return
		}
		generation := base[:32]
		stored := s.objects[path.Dir(key)+"/reader-leases/"+generation+".yml"]
		document, exists := decodeRegistry(stored.Data)
		if !exists || document.Tenant != tenant || document.Dataset != dataset || document.Generation != generation ||
			(r.Method != http.MethodPut && role.role == "reader" && (!document.Sealed || len(document.Readers) == 0)) {
			s.rejectLocked(w, tenant, dataset)
			return
		}
	}
	object, exists := s.objects[key]
	if !conditionMatches(r, object, exists) {
		w.WriteHeader(http.StatusPreconditionFailed)
		return
	}
	if r.Method == http.MethodPut {
		sum := sha256.Sum256(data)
		if r.Header.Get("x-amz-meta-kelvo-sha256") != hex.EncodeToString(sum[:]) ||
			(r.Header.Get("If-None-Match") == "" && r.Header.Get("If-Match") == "") {
			s.rejectLocked(w, tenant, dataset)
			return
		}
		var acquires, releases int
		var staged, sealed bool
		if registry {
			previous, _ := decodeRegistry(object.Data)
			next, valid := decodeRegistry(data)
			if !valid || next.Tenant != tenant || next.Dataset != dataset || base != next.Generation+".yml" {
				s.rejectLocked(w, tenant, dataset)
				return
			}
			acquires, releases = pinChanges(previous, next)
			staged = !exists && !next.Sealed
			sealed = !previous.Sealed && next.Sealed
			if gate := s.gates[gateKey{PinReleaseBeforeCAS, tenant, dataset}]; releases > 0 && gate != nil {
				s.mu.Unlock()
				gate.wait(r.Context(), false)
				s.mu.Lock()
				// A concurrent renewal or publication may have won while held.
				if _, authorized := s.identify(r, tenant, registry); !authorized {
					s.rejectLocked(w, tenant, dataset)
					return
				}
				current, exists := s.objects[key]
				if !conditionMatches(r, current, exists) {
					w.WriteHeader(http.StatusPreconditionFailed)
					return
				}
			}
		}
		if base == "current.yaml" {
			var root struct {
				Committed *struct {
					Generation string `yaml:"generation"`
				} `yaml:"committed"`
			}
			if yaml.Unmarshal(data, &root) != nil {
				s.rejectLocked(w, tenant, dataset)
				return
			}
			if root.Committed != nil {
				stored := s.objects[path.Dir(key)+"/reader-leases/"+root.Committed.Generation+".yml"]
				document, valid := decodeRegistry(stored.Data)
				if !valid || !document.Sealed || document.Tenant != tenant || document.Dataset != dataset || document.Generation != root.Committed.Generation {
					s.rejectLocked(w, tenant, dataset)
					return
				}
			}
		}
		object = s.storeLocked(key, data)
		if registry {
			s.countLocked(tenant, dataset, func(c *Counters) {
				c.RegistryWrites++
				c.PinAcquires += acquires
				c.PinReleases += releases
				if staged {
					c.StageWrites++
				}
				if sealed {
					c.SealWrites++
				}
			})
		}
		w.Header().Set("ETag", object.Version)
		w.Header().Set("Content-Length", "0")
		w.WriteHeader(http.StatusOK)
		return
	}
	if !exists {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.Header().Set("ETag", object.Version)
	w.Header().Set("x-amz-meta-kelvo-sha256", object.Digest)
	if value := r.Header.Get("Range"); value != "" {
		var first, last int64
		if r.Method != http.MethodGet || role.role != "reader" || r.Header.Get("If-Match") == "" {
			s.rejectLocked(w, tenant, dataset)
			return
		}
		if _, err := fmt.Sscanf(value, "bytes=%d-%d", &first, &last); err != nil ||
			value != fmt.Sprintf("bytes=%d-%d", first, last) || first < 0 || last < first || last >= int64(len(object.Data)) {
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		before := s.gates[gateKey{RangeBeforeResponse, tenant, dataset}]
		final := s.gates[gateKey{RangeFinalByte, tenant, dataset}]
		// Stored objects are immutable byte slices; writes replace the full value.
		// Never hold the fixture mutex while waiting on a gate or HTTP consumer.
		s.mu.Unlock()
		defer s.mu.Lock()
		if !before.wait(r.Context(), true) {
			return
		}
		w.Header().Set("Content-Length", strconv.FormatInt(last-first+1, 10))
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", first, last, len(object.Data)))
		w.WriteHeader(http.StatusPartialContent)
		if final != nil {
			if _, err := w.Write(object.Data[first:last]); err != nil {
				return
			}
			if err := http.NewResponseController(w).Flush(); err != nil || !final.wait(r.Context(), true) {
				return
			}
			_, _ = w.Write(object.Data[last : last+1])
		} else {
			_, _ = w.Write(object.Data[first : last+1])
		}
		return
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(object.Data)))
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodGet {
		s.mu.Unlock()
		_, _ = w.Write(object.Data)
		s.mu.Lock()
	}
}
