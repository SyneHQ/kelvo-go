// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/acceleration"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
)

// DatasetStatusReader must honor cancellation. Status reads snapshot metadata;
// implementations must not query source databases or trigger refreshes.
type DatasetStatusReader interface {
	Status(context.Context, string) (acceleration.Snapshot, error)
}

// DatasetHealth is the complete public diagnostic shape. Keep paths, object
// URLs, fingerprints, source configuration and raw backend errors out of it.
type DatasetHealth struct {
	ID          string     `json:"id"`
	State       string     `json:"state"`
	Generation  string     `json:"generation,omitempty"`
	RefreshedAt *time.Time `json:"refreshed_at,omitempty"`
	AgeNS       int64      `json:"age_ns"`
	SchemaHash  string     `json:"schema_hash,omitempty"`
}

type datasetHealthEntry struct {
	id          string
	fingerprint string
	maxAge      time.Duration
	required    bool
	snapshot    acceleration.Snapshot
	failure     string
	expires     time.Time
	pending     chan struct{}
}

// DatasetReporter shares bounded metadata probes across diagnostics/readiness.
// Close it before closing the supplied reader. It never owns the backend.
type DatasetReporter struct {
	mu            sync.Mutex
	reader        DatasetStatusReader
	entries       map[string]*datasetHealthEntry
	ids           []string
	required      []string
	ctx           context.Context
	cancel        context.CancelFunc
	closed        bool
	wg            sync.WaitGroup
	requiredSlots chan struct{}
	optionalSlots chan struct{}
	cacheTTL      time.Duration
	timeout       time.Duration
}

func NewDatasetReporter(c catalog.Config, reader DatasetStatusReader, required []string) (*DatasetReporter, error) {
	var datasets []catalog.Dataset
	if c.Acceleration != nil {
		datasets = c.Acceleration.Datasets
	}
	if len(datasets) > 64 || len(required) > 64 {
		return nil, errors.New("dataset diagnostics support at most 64 datasets")
	}
	if len(datasets) > 0 && reader == nil {
		return nil, errors.New("dataset status reader is required")
	}
	r := &DatasetReporter{reader: reader, entries: make(map[string]*datasetHealthEntry, len(datasets)), cacheTTL: 2 * time.Second, timeout: 5 * time.Second}
	for _, d := range datasets {
		if !catalog.ValidID(d.ID) || r.entries[d.ID] != nil || d.MaxAge <= 0 {
			return nil, errors.New("invalid dataset diagnostic configuration")
		}
		fingerprint, err := c.DatasetFingerprint(d.ID)
		if err != nil {
			return nil, errors.New("dataset diagnostic fingerprint is invalid")
		}
		r.entries[d.ID] = &datasetHealthEntry{id: d.ID, fingerprint: fingerprint, maxAge: d.MaxAge}
		r.ids = append(r.ids, d.ID)
	}
	for _, id := range required {
		entry := r.entries[id]
		if entry == nil || entry.required {
			return nil, errors.New("required datasets must be unique configured dataset identities")
		}
		entry.required = true
		r.required = append(r.required, id)
	}
	sort.Strings(r.ids)
	sort.Strings(r.required)
	optionalCapacity := 4
	if len(required) > 0 {
		optionalCapacity = 2
		r.requiredSlots = make(chan struct{}, 2)
	}
	r.optionalSlots = make(chan struct{}, optionalCapacity)
	r.ctx, r.cancel = context.WithCancel(context.Background())
	return r, nil
}

// Close stops admission of new probes, cancels existing probes and joins them.
// The reader's Status implementation must return when its context is canceled.
func (r *DatasetReporter) Close() {
	r.mu.Lock()
	if !r.closed {
		r.closed = true
		r.cancel()
	}
	r.mu.Unlock()
	r.wg.Wait()
}

func (r *DatasetReporter) start(ids []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return
	}
	now := time.Now()
	for _, id := range ids {
		e := r.entries[id]
		if e.pending != nil || now.Before(e.expires) {
			continue
		}
		e.pending = make(chan struct{})
		r.wg.Add(1)
		go r.probe(e)
	}
}

func (r *DatasetReporter) probe(e *datasetHealthEntry) {
	defer r.wg.Done()
	ctx, cancel := context.WithTimeout(r.ctx, r.timeout)
	defer cancel()
	slots := r.optionalSlots
	if e.required {
		slots = r.requiredSlots
	}
	var snapshot acceleration.Snapshot
	var err error
	select {
	case slots <- struct{}{}:
		snapshot, err = r.reader.Status(ctx, e.id)
		<-slots
	case <-ctx.Done():
		err = ctx.Err()
	}
	if ctx.Err() != nil {
		err = ctx.Err()
	}
	failure := ""
	if err != nil {
		failure = "unavailable"
		if errors.Is(err, acceleration.ErrNotFound) {
			failure = "missing"
		}
	}
	r.mu.Lock()
	e.snapshot = snapshot
	e.failure = failure
	e.expires = time.Now().Add(r.cacheTTL)
	pending := e.pending
	e.pending = nil
	close(pending)
	r.mu.Unlock()
}

func (r *DatasetReporter) read(ctx context.Context, id string) DatasetHealth {
	for {
		r.mu.Lock()
		e := r.entries[id]
		if r.closed {
			r.mu.Unlock()
			return DatasetHealth{ID: id, State: "unavailable"}
		}
		if e.pending == nil {
			snapshot, failure := e.snapshot, e.failure
			fingerprint, maxAge := e.fingerprint, e.maxAge
			r.mu.Unlock()
			if failure != "" {
				return DatasetHealth{ID: id, State: failure}
			}
			// Only well-formed identities and hashes can enter output, even if a
			// third-party reader accidentally returns a source error as an identifier.
			if snapshot.Dataset != id || !validToken(snapshot.Generation) || (!validSchemaDigest(snapshot.SchemaHash) && snapshot.SchemaHash != "") || snapshot.RefreshedAt.IsZero() {
				return DatasetHealth{ID: id, State: "unavailable"}
			}
			age := snapshot.Age()
			state := "ready"
			if snapshot.Fingerprint != fingerprint {
				state = "configuration_changed"
			} else if age > maxAge {
				state = "stale"
			}
			refreshed := snapshot.RefreshedAt
			return DatasetHealth{ID: id, State: state, Generation: snapshot.Generation, RefreshedAt: &refreshed, AgeNS: int64(age), SchemaHash: snapshot.SchemaHash}
		}
		pending := e.pending
		r.mu.Unlock()
		select {
		case <-ctx.Done():
			return DatasetHealth{ID: id, State: "unavailable"}
		case <-pending:
		}
	}
}

// Ready consults only operator-required datasets. Optional failures and source
// connectivity never implicitly gate worker readiness.
func (r *DatasetReporter) Ready(parent context.Context) bool {
	if r == nil {
		return true
	}
	r.mu.Lock()
	closed := r.closed
	r.mu.Unlock()
	if closed {
		return false
	}
	ctx, cancel := context.WithTimeout(parent, r.timeout)
	defer cancel()
	if ctx.Err() != nil {
		return false
	}
	r.start(r.required)
	for _, id := range r.required {
		if r.read(ctx, id).State != "ready" {
			return false
		}
	}
	return ctx.Err() == nil
}

func (r *DatasetReporter) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	ctx, cancel := context.WithTimeout(request.Context(), r.timeout)
	defer cancel()
	r.start(r.ids)
	rows := make([]DatasetHealth, 0, len(r.ids))
	for _, id := range r.ids {
		rows = append(rows, r.read(ctx, id))
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(rows)
}

func validSchemaDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

// hasRequired prevents an embedding from attaching a reporter configured with
// weaker readiness gates than the NodeConfig it is meant to enforce.
func (r *DatasetReporter) hasRequired(ids []string) bool {
	if r == nil {
		return len(ids) == 0
	}
	for _, id := range ids {
		entry := r.entries[id]
		if entry == nil || !entry.required {
			return false
		}
	}
	return true
}
