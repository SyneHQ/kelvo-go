// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"errors"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

// ExecutorFactory lets the CLI and cluster preserve their subprocess/sandbox
// boundary for refresh queries without coupling storage to worker internals.
type ExecutorFactory func(catalog.Config, query.Limits) (query.Executor, error)

type Manager struct {
	config  catalog.Config
	store   Backend
	factory ExecutorFactory
}

func NewManager(c catalog.Config, factory ExecutorFactory) (*Manager, error) {
	if c.Acceleration == nil || factory == nil {
		return nil, errors.New("acceleration configuration and executor factory are required")
	}
	s, err := OpenBackend(*c.Acceleration)
	if err != nil {
		return nil, err
	}
	return &Manager{config: c, store: s, factory: factory}, nil
}

func (m *Manager) Close() error { return m.store.Close() }

// Refresh publishes only after a complete successful source result and Parquet
// footer. A redelivered scheduled job rechecks freshness under the writer lock.
func (m *Manager) Refresh(ctx context.Context, id string, onlyIfDue bool) (Snapshot, error) {
	d, ok := m.config.Dataset(id)
	if !ok {
		return Snapshot{}, query.NewError("INVALID_ARGUMENT", "Unknown accelerated dataset")
	}
	fingerprint, err := m.config.DatasetFingerprint(id)
	if err != nil {
		return Snapshot{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, d.Limits.Timeout)
	defer cancel()
	if onlyIfDue {
		current, err := m.store.Status(ctx, id)
		if err == nil && current.Fingerprint == fingerprint && d.RefreshInterval > 0 && current.Age() < d.RefreshInterval {
			return current, nil
		}
	}
	// Prune owns the dataset writer lock independently of a transaction.
	if err := m.store.Prune(ctx, id, 2); err != nil && !errors.Is(err, ErrNotFound) {
		return Snapshot{}, err
	}
	if d.Multipart != nil {
		return m.refreshMultipart(ctx, d, fingerprint, onlyIfDue)
	}
	tx, err := m.store.Begin(ctx, id)
	if err != nil {
		return Snapshot{}, err
	}
	defer tx.Abort()
	ctx = tx.Context()
	if onlyIfDue {
		current, err := m.store.Status(ctx, id)
		if err == nil && current.Fingerprint == fingerprint && d.RefreshInterval > 0 && current.Age() < d.RefreshInterval {
			return current, nil
		}
	}
	schemaWriter, ok := tx.(SchemaWriter)
	if !ok {
		return Snapshot{}, errors.New("snapshot backend does not support schema contracts")
	}
	previous, err := schemaWriter.PreviousSchema()
	if err != nil && !errors.Is(err, ErrNotFound) {
		return Snapshot{}, err
	}
	c := m.config
	c.Acceleration = nil
	executor, err := m.factory(c, d.Limits)
	if err != nil {
		return Snapshot{}, err
	}
	sink := NewParquetSink(tx.File(), d.Limits)
	sink.expectedSchema = previous
	sink.schemaEvolution = d.SchemaEvolution
	defer sink.Abort()
	if _, err = executor.Execute(ctx, d.Query, sink); err != nil {
		return Snapshot{}, err
	}
	if err = ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	if err = sink.Finish(); err != nil {
		return Snapshot{}, err
	}
	if err = ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	if err = schemaWriter.SetSchema(sink.schema); err != nil {
		return Snapshot{}, err
	}
	snapshot, err := tx.Commit(fingerprint, sink.Rows())
	if err != nil {
		return Snapshot{}, err
	}
	// Publication is already durable. Prune failures must not turn a successful
	// generation into an apparent failure; the next refresh retries cleanup.
	_ = m.store.Prune(ctx, id, 2)
	return snapshot, nil
}

// refreshMultipart retains one writer transaction across every part. Nothing
// becomes visible until source execution, all footers and the schema contract
// have completed successfully.
func (m *Manager) refreshMultipart(ctx context.Context, d catalog.Dataset, fingerprint string, onlyIfDue bool) (Snapshot, error) {
	backend, ok := m.store.(MultipartBackend)
	if !ok {
		return Snapshot{}, query.NewError("CONFIGURATION_ERROR", "Snapshot backend does not support multipart refresh")
	}
	options := MultipartOptions{MaxParts: d.Multipart.MaxParts, MaxPartBytes: d.Multipart.MaxPartBytes, MaxTotalBytes: d.Limits.MaxBytes}
	tx, err := backend.BeginMultipart(ctx, d.ID, options)
	if err != nil {
		return Snapshot{}, err
	}
	defer tx.Abort()
	ctx = tx.Context()
	if onlyIfDue {
		current, err := m.store.Status(ctx, d.ID)
		if err == nil && current.Fingerprint == fingerprint && d.RefreshInterval > 0 && current.Age() < d.RefreshInterval {
			return current, nil
		}
	}
	previous, err := tx.PreviousSchema()
	if err != nil && !errors.Is(err, ErrNotFound) {
		return Snapshot{}, err
	}
	config := m.config
	config.Acceleration = nil
	executor, err := m.factory(config, d.Limits)
	if err != nil {
		return Snapshot{}, err
	}
	sink := NewMultipartParquetSink(tx, d.Limits, options)
	sink.expectedSchema = previous
	sink.schemaEvolution = d.SchemaEvolution
	defer sink.Abort()
	if _, err = executor.Execute(ctx, d.Query, sink); err != nil {
		return Snapshot{}, err
	}
	if err = ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	if err = sink.Finish(); err != nil {
		return Snapshot{}, err
	}
	if err = ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	if err = tx.SetSchema(sink.schema); err != nil {
		return Snapshot{}, err
	}
	snapshot, err := tx.Commit(fingerprint)
	if err != nil {
		return Snapshot{}, err
	}
	_ = m.store.Prune(ctx, d.ID, 2)
	return snapshot, nil
}

func (m *Manager) Status(id string) (Snapshot, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return m.StatusContext(ctx, id)
}

func (m *Manager) StatusContext(ctx context.Context, id string) (Snapshot, error) {
	if _, ok := m.config.Dataset(id); !ok {
		return Snapshot{}, errors.New("unknown accelerated dataset")
	}
	return m.store.Status(ctx, id)
}

func (m *Manager) Verify(ctx context.Context, id string) (Snapshot, error) {
	if _, ok := m.config.Dataset(id); !ok {
		return Snapshot{}, errors.New("unknown accelerated dataset")
	}
	return m.store.Verify(ctx, id)
}

// Run schedules refreshes serially to give ingestion its own bounded resource
// budget. Cluster nodes use JetStream dispatch instead of this local loop.
func (m *Manager) Run(ctx context.Context, onError func(string, error)) error {
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()
	next := map[string]time.Time{}
	for {
		for _, d := range m.config.Acceleration.Datasets {
			if d.RefreshInterval == 0 || time.Now().Before(next[d.ID]) {
				continue
			}
			next[d.ID] = time.Now().Add(min(d.RefreshInterval, 30*time.Second))
			s, err := m.Refresh(ctx, d.ID, true)
			if err != nil && ctx.Err() == nil && onError != nil {
				onError(d.ID, err)
			}
			if err == nil {
				next[d.ID] = nextRefreshAt(s, d.RefreshInterval)
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}

func nextRefreshAt(snapshot Snapshot, interval time.Duration) time.Time {
	return time.Now().Add(max(0, interval-snapshot.Age()))
}

// Resolve selects only requested datasets and holds read leases until the query
// process has exited. Child processes see immutable Parquet paths, never the
// acceleration directory, refresh SQL, or credentials for the original source.
func Resolve(ctx context.Context, c catalog.Config, request query.Request) ([]catalog.Source, []query.AccelerationVersion, func(), error) {
	ids := request.Sources
	if request.Mode == "native" {
		ids = []string{request.ConnectionID}
	}
	sources, err := c.Select(ids)
	if err != nil {
		return nil, nil, func() {}, query.NewError("INVALID_ARGUMENT", "Unknown or duplicate source")
	}
	var leases []*Lease
	var store Backend
	var bridgeClose func()
	var objects []Snapshot
	closeAll := func() {
		if bridgeClose != nil {
			bridgeClose()
		}
		for _, lease := range leases {
			_ = lease.Close()
		}
		if store != nil {
			_ = store.Close()
		}
	}
	var versions []query.AccelerationVersion
	for i, source := range sources {
		if source.Type != "accelerated" {
			continue
		}
		if request.Mode == "native" {
			closeAll()
			return nil, nil, func() {}, query.NewError("INVALID_ARGUMENT", "Accelerated datasets require federated mode")
		}
		if store == nil {
			store, err = OpenBackend(*c.Acceleration)
			if err != nil {
				closeAll()
				return nil, nil, func() {}, query.NewError("DATASET_UNAVAILABLE", "Accelerated dataset storage is unavailable")
			}
		}
		d, _ := c.Dataset(source.ID)
		fingerprint, e := c.DatasetFingerprint(source.ID)
		if e != nil {
			closeAll()
			return nil, nil, func() {}, query.NewError("DATASET_UNAVAILABLE", "Accelerated dataset configuration is unavailable")
		}
		lease, e := store.Acquire(ctx, source.ID, fingerprint, d.MaxAge)
		if e != nil {
			closeAll()
			if ctx.Err() != nil {
				return nil, nil, func() {}, ctx.Err()
			}
			return nil, nil, func() {}, query.NewError("DATASET_UNAVAILABLE", "Accelerated dataset is missing, stale, or requires a refresh")
		}
		leases = append(leases, lease)
		snapshot := lease.Snapshot
		if c.Acceleration.ObjectStorage != nil {
			// Remote parts become parent-owned capability URLs below, never
			// local paths or cloud credentials inside the query subprocess.
			if snapshot.ObjectKey == "" && len(snapshot.Parts) == 0 {
				closeAll()
				return nil, nil, func() {}, query.NewError("DATASET_UNAVAILABLE", "Remote snapshot layout is invalid")
			}
			objects = append(objects, snapshot)
			sources[i] = catalog.Source{ID: source.ID, Type: "parquet"}
		} else {
			if snapshot.ObjectKey != "" || snapshot.ObjectVersion != "" {
				closeAll()
				return nil, nil, func() {}, query.NewError("DATASET_UNAVAILABLE", "Object snapshot configuration is unavailable")
			}
			sources[i] = catalog.Source{ID: source.ID, Type: "parquet", Path: snapshot.Path}
			if len(snapshot.Parts) > 0 {
				if len(snapshot.Parts) > 256 || snapshot.Path != "" {
					closeAll()
					return nil, nil, func() {}, query.NewError("DATASET_UNAVAILABLE", "Multipart snapshot layout is invalid")
				}
				paths := make([]string, len(snapshot.Parts))
				for index, part := range snapshot.Parts {
					if part.ObjectKey != "" || part.ObjectVersion != "" {
						closeAll()
						return nil, nil, func() {}, query.NewError("DATASET_UNAVAILABLE", "Local snapshot part is invalid")
					}
					paths[index] = part.Path
				}
				sources[i] = catalog.Source{ID: source.ID, Type: "parquet", ParquetPaths: paths}
				if err := sources[i].ValidateParquetPaths(); err != nil {
					closeAll()
					return nil, nil, func() {}, query.NewError("DATASET_UNAVAILABLE", "Local snapshot paths are invalid")
				}
			}
		}
		if c.Acceleration.ObjectStorage == nil {
			scan, err := d.EffectiveSnapshotScanLimits()
			if err != nil {
				closeAll()
				return nil, nil, func() {}, query.NewError("DATASET_UNAVAILABLE", "Snapshot scan limits are unavailable")
			}
			parts := []catalog.LocalSnapshotPart{{Rows: snapshot.Rows, Bytes: snapshot.Bytes, SHA256: snapshot.SHA256}}
			if len(snapshot.Parts) > 0 {
				parts = make([]catalog.LocalSnapshotPart, len(snapshot.Parts))
				for index, part := range snapshot.Parts {
					parts[index] = catalog.LocalSnapshotPart{Rows: part.Rows, Bytes: part.Bytes, SHA256: part.SHA256}
				}
			}
			sources[i].LocalSnapshot = &catalog.LocalSnapshotRead{Dataset: source.ID, Generation: snapshot.Generation,
				SchemaSHA256: snapshot.SchemaHash, Parts: parts, Scan: scan}
			if err := sources[i].ValidateLocalSnapshot(); err != nil {
				closeAll()
				return nil, nil, func() {}, query.NewError("DATASET_UNAVAILABLE", "Local snapshot provenance is unavailable")
			}
		}
		versions = append(versions, query.AccelerationVersion{Dataset: source.ID, Generation: lease.Snapshot.Generation, RefreshedAt: lease.Snapshot.RefreshedAt})
	}
	if len(objects) > 0 {
		var ranges map[string]catalog.Source
		ranges, bridgeClose, err = OpenObjectRanges(ctx, *c.Acceleration.ObjectStorage, objects)
		if err != nil {
			closeAll()
			return nil, nil, func() {}, query.NewError("DATASET_UNAVAILABLE", "Object snapshot range reader is unavailable")
		}
		for i, source := range sources {
			if resolved, ok := ranges[source.ID]; ok {
				sources[i] = resolved
			}
		}
	}
	return sources, versions, closeAll, nil
}
