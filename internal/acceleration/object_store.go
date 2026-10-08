// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/objectstore"
	"github.com/SYNEHQ/kelvo-go/internal/readerlease"
	"go.yaml.in/yaml/v3"
)

var (
	ErrLeaseLost          = errors.New("acceleration writer lease was lost")
	ErrPublicationUnknown = errors.New("acceleration manifest publication outcome is uncertain")
	errBackendClosed      = errors.New("acceleration backend is closed")
)

// objectBackend uses local disk only for private refresh staging. The remote
// manifest is both the committed pointer and the writer fence; no shared local
// directory, distributed reader lock, object listing, or remote deletion is used.
type objectBackend struct {
	runtime       *ObjectRuntime
	config        catalog.AccelerationConfig
	reader        objectstore.Client
	writeClient   func() (objectstore.Client, bool, error)
	leaseDuration time.Duration
	pollInterval  time.Duration
	renewInterval time.Duration
	closed        atomic.Bool
	closeOnce     sync.Once
	closeErr      error
	writers       objectWriterLifetime
}

type objectManifest struct {
	History          []*objectCommitted `yaml:"history,omitempty"`
	HistoryTruncated bool               `yaml:"history_truncated,omitempty"`
	Version          int                `yaml:"version"`
	Dataset          string             `yaml:"dataset"`
	Committed        *objectCommitted   `yaml:"committed,omitempty"`
	Writer           *objectWriterLease `yaml:"writer,omitempty"`
}

type objectCommitted struct {
	ReaderBinding *readerlease.Binding `yaml:"reader_binding,omitempty"`
	Descriptor    *objectDescriptorRef `yaml:"descriptor,omitempty"`
	SchemaHash    string               `yaml:"schema_hash,omitempty"`
	Generation    string               `yaml:"generation"`
	Fingerprint   string               `yaml:"fingerprint"`
	SHA256        string               `yaml:"sha256"`
	Rows          int64                `yaml:"rows"`
	Bytes         int64                `yaml:"bytes"`
	RefreshedAt   time.Time            `yaml:"refreshed_at"`
	ObjectVersion string               `yaml:"object_version,omitempty"`
}

type objectWriterLease struct {
	ReaderReference *readerlease.Reference `yaml:"reader_reference,omitempty"`
	Owner           string                 `yaml:"owner"`
	ExpiresAt       time.Time              `yaml:"expires_at"`
}

type objectState struct {
	manifest   objectManifest
	info       objectstore.Info
	receivedAt time.Time
}

// Date from the storage service establishes the lease clock. Elapsed local
// monotonic time advances that observation without depending on clock skew.
// A first-ever missing-object response may not supply Date; the initial claim
// is immediately renewed against the successful write's server observation.
func (state objectState) now() time.Time {
	if state.info.ServerTime.IsZero() {
		return time.Now().UTC()
	}
	return state.info.ServerTime.Add(time.Since(state.receivedAt)).UTC()
}

func newObjectBackend(config catalog.AccelerationConfig, client objectstore.Client) (*objectBackend, error) {
	if err := config.ValidateVerification(); err != nil {
		return nil, err
	}
	if config.ObjectStorage == nil || !storeTenantID.MatchString(config.TenantID) || config.Directory == "" {
		return nil, errors.New("object snapshots require a tenant, object storage and local staging directory")
	}
	storage := *config.ObjectStorage
	if err := storage.ObjectLocation.Validate(); err != nil {
		return nil, err
	}
	for _, dataset := range config.Datasets {
		if dataset.Multipart == nil && dataset.Limits.MaxBytes > objectstore.MaxUploadBytes {
			return nil, errors.New("object snapshot byte limit exceeds the supported single-upload limit")
		}
		if dataset.Multipart != nil {
			options := MultipartOptions{MaxParts: dataset.Multipart.MaxParts, MaxPartBytes: dataset.Multipart.MaxPartBytes, MaxTotalBytes: dataset.Limits.MaxBytes}
			if err := validateObjectMultipartOptions(options); err != nil {
				return nil, err
			}
		}
	}
	config.ObjectStorage = &storage
	return &objectBackend{config: config, reader: client,
		writeClient:   func() (objectstore.Client, bool, error) { return client, false, nil },
		leaseDuration: 60 * time.Second, renewInterval: 15 * time.Second, pollInterval: time.Second}, nil
}

func (backend *objectBackend) Close() error {
	backend.closeOnce.Do(func() {
		backend.closeErr = backend.drainWriters()
		backend.reader.Close()
	})
	return backend.closeErr
}

func (backend *objectBackend) check(ctx context.Context, dataset string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if backend.closed.Load() {
		return errBackendClosed
	}
	if !storeDatasetID.MatchString(dataset) {
		return errors.New("acceleration dataset must be a SQL identifier of at most 63 characters")
	}
	return nil
}

func (backend *objectBackend) key(dataset, filename string) string {
	parts := []string{backend.config.TenantID, dataset, filename}
	if prefix := backend.config.ObjectStorage.Prefix; prefix != "" {
		parts = append([]string{prefix}, parts...)
	}
	return strings.Join(parts, "/")
}

func (backend *objectBackend) readState(ctx context.Context, dataset string, client objectstore.Client) (state objectState, resultErr error) {
	state = objectState{manifest: objectManifest{Version: backend.manifestVersion(), Dataset: dataset}}
	body, info, err := client.Get(ctx, backend.key(dataset, storeManifestName), "")
	if backend.protected() && !nilReaderDependency(body) {
		defer finishProtectedRead(body, &resultErr)
	}
	state.info, state.receivedAt = info, time.Now()
	if errors.Is(err, objectstore.ErrNotFound) {
		state.info.Version = ""
		return state, ErrNotFound
	}
	if err != nil {
		return state, err
	}
	if backend.protected() {
		if nilReaderDependency(body) {
			return state, ErrCorrupt
		}
	} else {
		defer body.Close()
	}
	if backend.protected() && info.ServerTime.IsZero() {
		return state, readerlease.ErrClock
	}
	if !validObjectVersion(info.Version) || info.Size > storeManifestLimit {
		return state, fmt.Errorf("%w: invalid remote manifest metadata", ErrCorrupt)
	}
	data, err := io.ReadAll(io.LimitReader(body, storeManifestLimit+1))
	if err != nil {
		return state, err
	}
	if len(data) > storeManifestLimit || (info.Size >= 0 && info.Size != int64(len(data))) {
		return state, fmt.Errorf("%w: invalid remote manifest size", ErrCorrupt)
	}
	if info.SHA256 != "" && info.SHA256 != objectSHA256(data) {
		return state, fmt.Errorf("%w: remote manifest checksum mismatch", ErrCorrupt)
	}
	if backend.protected() {
		if err := validateProtectedYAML(data); err != nil {
			return state, err
		}
	}
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	// Defaults belong only to a missing manifest. Existing objects must supply
	// their own identity fields, including when the YAML document is null.
	state.manifest = objectManifest{}
	if err := decoder.Decode(&state.manifest); err != nil {
		return state, fmt.Errorf("%w: malformed remote manifest", ErrCorrupt)
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return state, fmt.Errorf("%w: remote manifest must contain one YAML document", ErrCorrupt)
	}
	if err := validateObjectManifest(state.manifest, dataset); err != nil {
		return state, err
	}
	if (state.manifest.Version == protectedManifestVersion) != backend.protected() {
		return state, ErrProtectionRequired
	}
	return state, nil
}

func validateObjectManifest(manifest objectManifest, dataset string) error {
	if (manifest.Version != 2 && manifest.Version != 3 && manifest.Version != 4 && manifest.Version != protectedManifestVersion) || manifest.Dataset != dataset {
		return fmt.Errorf("%w: remote manifest identity mismatch", ErrCorrupt)
	}
	if manifest.Version == 2 && (len(manifest.History) != 0 || manifest.HistoryTruncated) {
		return fmt.Errorf("%w: retained catalog requires manifest version 3", ErrCorrupt)
	}
	if manifest.Committed != nil {
		if manifest.Version < 4 && manifest.Committed.Descriptor != nil {
			return fmt.Errorf("%w: multipart descriptor requires manifest version 4", ErrCorrupt)
		}
		if err := validateObjectCommit(manifest.Committed); err != nil {
			return err
		}
	}
	if len(manifest.History) > ObjectHistoryLimit || (len(manifest.History) > 0 && manifest.Committed == nil) {
		return fmt.Errorf("%w: invalid retained generation catalog", ErrCorrupt)
	}
	seen := map[string]bool{}
	if manifest.Committed != nil {
		seen[manifest.Committed.Generation] = true
	}
	for _, generation := range manifest.History {
		if generation != nil && manifest.Version < 4 && generation.Descriptor != nil {
			return fmt.Errorf("%w: multipart descriptor requires manifest version 4", ErrCorrupt)
		}
		if err := validateObjectCommit(generation); err != nil {
			return err
		}
		if seen[generation.Generation] {
			return fmt.Errorf("%w: duplicate retained generation", ErrCorrupt)
		}
		seen[generation.Generation] = true
	}
	if writer := manifest.Writer; writer != nil && (!storeGenerationID.MatchString(writer.Owner) || writer.ExpiresAt.IsZero()) {
		return fmt.Errorf("%w: invalid remote writer lease", ErrCorrupt)
	}
	return validateManifestProtection(manifest)
}

func validObjectVersion(version string) bool {
	return version != "" && len(version) <= 4096 && !strings.ContainsAny(version, "\r\n\x00")
}

func (backend *objectBackend) writeState(ctx context.Context, client objectstore.Client, state objectState, manifest objectManifest) (objectState, error) {
	manifest.Version = backend.manifestVersion()
	if err := validateObjectManifest(manifest, manifest.Dataset); err != nil {
		return state, err
	}
	data, err := yaml.Marshal(manifest)
	if err != nil {
		return state, err
	}
	if len(data) > storeManifestLimit {
		return state, errors.New("remote manifest exceeds size limit")
	}
	condition := objectstore.Condition{Version: state.info.Version}
	if state.info.Version == "" {
		condition.Absent = true
	}
	info, err := client.Put(ctx, backend.key(manifest.Dataset, storeManifestName), bytes.NewReader(data), int64(len(data)), objectSHA256(data), condition)
	if err != nil {
		return state, err
	}
	if !validObjectVersion(info.Version) {
		return state, fmt.Errorf("%w: storage returned no manifest version", ErrCorrupt)
	}
	if info.ServerTime.IsZero() {
		if backend.protected() {
			return state, readerlease.ErrClock
		}
		info.ServerTime = state.now()
	}
	return objectState{manifest: manifest, info: info, receivedAt: time.Now()}, nil
}

func (backend *objectBackend) snapshot(dataset string, committed *objectCommitted, reference time.Time) (Snapshot, error) {
	if committed == nil {
		return Snapshot{}, ErrNotFound
	}
	if committed.Descriptor != nil {
		return Snapshot{}, errors.New("multipart snapshot requires descriptor resolution")
	}
	key := backend.key(dataset, committed.Generation+".parquet")
	uri, err := backend.config.ObjectStorage.ObjectLocation.URI(key)
	if err != nil {
		return Snapshot{}, err
	}
	snapshot := Snapshot{SchemaHash: committed.SchemaHash, Dataset: dataset, Generation: committed.Generation, Path: uri,
		Fingerprint: committed.Fingerprint, SHA256: committed.SHA256, Rows: committed.Rows,
		Bytes: committed.Bytes, RefreshedAt: committed.RefreshedAt, ObjectKey: key, ObjectVersion: committed.ObjectVersion}
	return snapshot.observeClock(reference), nil
}

func (backend *objectBackend) status(ctx context.Context, dataset string) (Snapshot, time.Duration, error) {
	snapshot, err := backend.currentSnapshot(ctx, dataset, "", 0, false)
	if err != nil {
		return Snapshot{}, 0, err
	}
	return snapshot, snapshot.Age(), nil
}

// currentSnapshot selects the manifest once. Query policy is checked before
// generation access; Status still resolves and validates snapshots of any age.
func (backend *objectBackend) currentSnapshot(ctx context.Context, dataset, fingerprint string, maxAge time.Duration, checkPolicy bool) (Snapshot, error) {
	if backend.protected() {
		return backend.protectedCurrentSnapshot(ctx, dataset, fingerprint, maxAge, checkPolicy)
	}
	if err := backend.check(ctx, dataset); err != nil {
		return Snapshot{}, err
	}
	state, err := backend.readState(ctx, dataset, backend.reader)
	if err != nil {
		return Snapshot{}, err
	}
	if err := backend.check(ctx, dataset); err != nil {
		return Snapshot{}, err
	}
	committed := state.manifest.Committed
	if committed == nil {
		return Snapshot{}, ErrNotFound
	}
	// Observe the selected manifest's service clock before descriptor I/O. Keep
	// this monotonic anchor even when Date rounding puts RefreshedAt ahead of
	// the observation: resetting it later would erase elapsed read time.
	reference := state.now()
	observation := Snapshot{Fingerprint: committed.Fingerprint, RefreshedAt: committed.RefreshedAt}.observeClock(reference)
	accept := func() error {
		if err := backend.check(ctx, dataset); err != nil {
			return err
		}
		if checkPolicy {
			if observation.Fingerprint != fingerprint {
				return ErrFingerprintMismatch
			}
			if maxAge > 0 && observation.Age() > maxAge {
				return ErrStale
			}
		}
		return nil
	}
	if err := accept(); err != nil {
		return Snapshot{}, err
	}
	snapshot, err := backend.loadObjectSnapshot(ctx, dataset, committed, reference, backend.reader)
	if err != nil {
		return Snapshot{}, err
	}
	snapshot.ageObserved, snapshot.ageObservedAt = observation.ageObserved, observation.ageObservedAt
	if err := accept(); err != nil {
		return Snapshot{}, err
	}
	if err := backend.headObjectSnapshot(ctx, snapshot); err != nil {
		return Snapshot{}, err
	}
	if err := accept(); err != nil {
		return Snapshot{}, err
	}
	return snapshot, nil
}

func validateSnapshotObject(snapshot Snapshot, info objectstore.Info) error {
	if info.Size != snapshot.Bytes || info.Size <= 0 || info.Version != snapshot.ObjectVersion || info.SHA256 != snapshot.SHA256 {
		return fmt.Errorf("%w: snapshot object metadata changed", ErrCorrupt)
	}
	return nil
}

func (backend *objectBackend) Status(ctx context.Context, dataset string) (Snapshot, error) {
	snapshot, _, err := backend.status(ctx, dataset)
	return snapshot, err
}

func (backend *objectBackend) Acquire(ctx context.Context, dataset, fingerprint string, maxAge time.Duration) (*Lease, error) {
	if backend.protected() {
		return nil, ErrProtectionRequired
	}
	if maxAge < 0 {
		return nil, errors.New("acceleration maximum age cannot be negative")
	}
	snapshot, err := backend.currentSnapshot(ctx, dataset, fingerprint, maxAge, true)
	if err != nil {
		return nil, err
	}
	// Object generations are immutable and automatic remote deletion is disabled.
	// Therefore an acquired key remains valid without a distributed reader lease.
	return &Lease{Snapshot: snapshot, release: func() error { return nil }}, nil
}

func (backend *objectBackend) Verify(ctx context.Context, dataset string) (Snapshot, error) {
	if backend.protected() {
		return backend.verifyProtectedObject(ctx, dataset)
	}
	// Full verification includes bounded footer/schema reads for every layout.
	// A checksum-only client must not claim that persisted row/schema metadata
	// was verified merely because the object's bytes match its digest.
	if _, ok := backend.reader.(objectstore.RangeClient); !ok {
		return Snapshot{}, ErrRecoveryUnsupported
	}
	snapshot, _, err := backend.status(ctx, dataset)
	if err != nil {
		return Snapshot{}, err
	}
	if _, err := backend.verifyObjectSnapshot(ctx, snapshot); err != nil {
		return Snapshot{}, err
	}
	return snapshot, nil
}

func (backend *objectBackend) verifyObjectBytes(ctx context.Context, snapshot Snapshot) (Snapshot, error) {
	return backend.verifyObjectBytesWithReader(ctx, snapshot, backend.reader)
}

func (backend *objectBackend) verifyObjectBytesWithReader(ctx context.Context, snapshot Snapshot, client objectstore.Client) (verified Snapshot, resultErr error) {
	body, info, err := client.Get(ctx, snapshot.ObjectKey, snapshot.ObjectVersion)
	if backend.protected() && !nilReaderDependency(body) {
		defer func() {
			finishProtectedRead(body, &resultErr)
			if resultErr != nil {
				verified = Snapshot{}
			}
		}()
	}
	if err != nil {
		return Snapshot{}, err
	}
	if backend.protected() {
		if nilReaderDependency(body) {
			return Snapshot{}, ErrCorrupt
		}
	} else {
		defer body.Close()
	}
	if err := validateSnapshotObject(snapshot, info); err != nil {
		return Snapshot{}, err
	}
	hash := sha256.New()
	buffer := make([]byte, 256<<10)
	var count int64
	for {
		if err := ctx.Err(); err != nil {
			return Snapshot{}, err
		}
		n, err := body.Read(buffer)
		count += int64(n)
		if count > snapshot.Bytes {
			return Snapshot{}, fmt.Errorf("%w: snapshot object exceeds declared size", ErrCorrupt)
		}
		if n > 0 {
			_, _ = hash.Write(buffer[:n])
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return Snapshot{}, err
		}
	}
	if count != snapshot.Bytes || hex.EncodeToString(hash.Sum(nil)) != snapshot.SHA256 {
		return Snapshot{}, fmt.Errorf("%w: snapshot object checksum mismatch", ErrCorrupt)
	}
	return snapshot, nil
}

// Prune deliberately does not list or delete remote objects. Retired and orphaned
// generations require operator-managed cleanup after all readers are quiescent.
func (backend *objectBackend) Prune(ctx context.Context, dataset string, keep int) error {
	if keep < 1 {
		return errors.New("acceleration retention must keep at least one generation")
	}
	return backend.check(ctx, dataset)
}

type objectTransaction struct {
	pointer         *pointerWriterState
	readerReference *readerlease.Reference
	schemaHash      string
	finishMu        sync.Mutex
	stateMu         sync.Mutex
	backend         *objectBackend
	client          objectstore.Client
	closeClient     bool
	local           *Transaction
	ctx             context.Context
	cancel          context.CancelCauseFunc
	dataset         string
	owner           string
	state           objectState
	owned           bool
	done            bool
	stopRenew       context.CancelFunc
	renewDone       chan struct{}
	stopping        atomic.Bool
}

func (backend *objectBackend) Begin(ctx context.Context, dataset string) (_ RefreshWriter, resultErr error) {
	if err := backend.check(ctx, dataset); err != nil {
		return nil, err
	}
	if backend.protected() && (backend.runtime == nil || backend.runtime.registry == nil) {
		return nil, ErrProtectionRequired
	}
	tx, err := backend.reserveWriter(ctx, dataset)
	if err != nil {
		return nil, err
	}
	success := false
	defer func() {
		if !success {
			resultErr = errors.Join(resultErr, tx.cleanup())
		}
	}()
	if err := context.Cause(tx.ctx); err != nil {
		return nil, err
	}
	// The reservation owns a late or partial factory result even when drain
	// started while the external constructor was running.
	tx.client, tx.closeClient, err = backend.writeClient()
	if err != nil {
		return nil, err
	}
	if tx.client == nil {
		return nil, errors.New("object snapshot writer client is unavailable")
	}
	if err := backend.writerOpeningError(tx.ctx); err != nil {
		return nil, err
	}
	client := tx.client
	// Staging is private and worker-local. It never publishes a local manifest.
	local, err := OpenStore(backend.config.Directory, backend.config.TenantID)
	if err != nil {
		return nil, err
	}
	tx.local, err = local.Begin(tx.ctx, dataset)
	if err != nil {
		return nil, err
	}
	tx.owner = tx.local.generation
	if backend.protected() {
		ref, err := readerlease.NewReference(backend.config.TenantID, dataset, tx.owner)
		if err != nil {
			return nil, err
		}
		tx.readerReference = &ref
	}
	if err := tx.claimWriter(client, true); err != nil {
		return nil, err
	}
	tx.startRenewal()
	if tx.readerReference != nil {
		if err := backend.runtime.registry.Stage(tx.ctx, *tx.readerReference, tx.owner); err != nil {
			return nil, err
		}
	}
	if err := backend.writerOpeningError(tx.ctx); err != nil {
		return nil, err
	}
	success = true
	return tx, nil
}

// claimWriter shares the refresh writer fence without choosing a staging mode.
// Restore requires an existing root and meters its metadata reader separately.
func (tx *objectTransaction) claimWriter(reader objectstore.Client, allowMissing bool) error {
	for {
		if err := context.Cause(tx.ctx); err != nil {
			return err
		}
		state, err := tx.backend.readState(tx.ctx, tx.dataset, reader)
		if err != nil && (!allowMissing || !errors.Is(err, ErrNotFound)) {
			return err
		}
		if writer := state.manifest.Writer; writer != nil && writer.ExpiresAt.After(state.now()) {
			wait := min(tx.backend.pollInterval, writer.ExpiresAt.Sub(state.now()))
			if wait <= 0 {
				continue
			}
			timer := time.NewTimer(wait)
			select {
			case <-tx.ctx.Done():
				timer.Stop()
				return context.Cause(tx.ctx)
			case <-timer.C:
			}
			continue
		}
		manifest := state.manifest
		manifest.Writer = tx.writerLease(state.now().Add(tx.backend.leaseDuration))
		// A transport failure might still have acquired the lease. Cleanup checks
		// this owner before clearing anything, including an ambiguous first claim.
		tx.owned = true
		tx.state, err = tx.backend.writeState(tx.ctx, tx.client, state, manifest)
		if errors.Is(err, objectstore.ErrConflict) {
			continue
		}
		if err != nil {
			return err
		}
		// Normalize against the successful claim's server clock before extraction.
		if err := tx.renew(tx.ctx); err != nil {
			return err
		}
		break
	}
	return nil
}

func (tx *objectTransaction) startRenewal() {
	renewCtx, stop := context.WithCancel(tx.ctx)
	tx.stopRenew, tx.renewDone = stop, make(chan struct{})
	go tx.renewLoop(renewCtx)
}

func (tx *objectTransaction) Context() context.Context { return tx.ctx }
func (tx *objectTransaction) File() *os.File           { return tx.local.File() }

func (tx *objectTransaction) renew(ctx context.Context) error {
	tx.stateMu.Lock()
	defer tx.stateMu.Unlock()
	manifest := tx.state.manifest
	manifest.Writer = tx.writerLease(tx.state.now().Add(tx.backend.leaseDuration))
	state, err := tx.backend.writeState(ctx, tx.client, tx.state, manifest)
	if err != nil {
		return fmt.Errorf("%w: renew remote writer: %w", ErrLeaseLost, err)
	}
	tx.state = state
	return nil
}

func (tx *objectTransaction) renewLoop(ctx context.Context) {
	defer close(tx.renewDone)
	ticker := time.NewTicker(tx.backend.renewInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := tx.renew(ctx); err != nil {
				if !tx.stopping.Load() {
					tx.cancel(err)
				}
				return
			}
		}
	}
}

func (tx *objectTransaction) stopRenewal() {
	if tx.stopRenew != nil {
		tx.stopping.Store(true)
		tx.stopRenew()
		<-tx.renewDone
	}
}

func (tx *objectTransaction) Commit(fingerprint string, rows int64) (snapshot Snapshot, err error) {
	tx.finishMu.Lock()
	defer tx.finishMu.Unlock()
	if tx.done {
		return Snapshot{}, errors.New("acceleration transaction is already finished")
	}
	tx.done = true
	defer func() {
		if cause := context.Cause(tx.ctx); err != nil && cause != nil {
			err = errors.Join(err, cause)
		}
		err = errors.Join(err, tx.cleanup())
	}()
	if fingerprint == "" || len(fingerprint) > storeFingerprintLimit || rows < 0 {
		return Snapshot{}, errors.New("acceleration snapshot requires a bounded fingerprint and nonnegative row count")
	}
	if err := context.Cause(tx.ctx); err != nil {
		return Snapshot{}, err
	}
	file := tx.local.File()
	info, err := file.Stat()
	if err != nil {
		return Snapshot{}, err
	}
	if info.Size() <= 0 || info.Size() > objectstore.MaxUploadBytes {
		return Snapshot{}, errors.New("object snapshot exceeds the supported single-upload limit or is empty")
	}
	if tx.readerReference != nil {
		var actualRows int64
		schema, err := readParquetSchema(file, info.Size(), &actualRows)
		if err != nil {
			return Snapshot{}, err
		}
		if actualRows != rows || !storeDigest.MatchString(tx.schemaHash) {
			return Snapshot{}, fmt.Errorf("%w: protected snapshot requires exact rows and schema", ErrCorrupt)
		}
		if err := verifySchemaHash(schema, tx.schemaHash); err != nil {
			return Snapshot{}, err
		}
	}
	if err := file.Chmod(0400); err != nil {
		return Snapshot{}, err
	}
	if err := file.Sync(); err != nil {
		return Snapshot{}, err
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return Snapshot{}, err
	}
	digest, err := storeHash(tx.ctx, file)
	if err != nil {
		return Snapshot{}, err
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return Snapshot{}, err
	}
	key := tx.backend.key(tx.dataset, tx.local.generation+".parquet")
	uploaded, err := tx.client.Put(tx.ctx, key, file, info.Size(), digest, objectstore.Condition{Absent: true})
	if err != nil {
		return Snapshot{}, err
	}
	if !validObjectVersion(uploaded.Version) {
		return Snapshot{}, fmt.Errorf("%w: storage returned no payload version", ErrCorrupt)
	}
	confirmed, err := tx.client.Head(tx.ctx, key, uploaded.Version)
	if err != nil {
		return Snapshot{}, err
	}
	if confirmed.Size != info.Size() || confirmed.Version != uploaded.Version || confirmed.SHA256 != digest {
		return Snapshot{}, fmt.Errorf("%w: uploaded snapshot metadata mismatch", ErrCorrupt)
	}
	committed := &objectCommitted{SchemaHash: tx.schemaHash, Generation: tx.local.generation, Fingerprint: fingerprint, SHA256: digest, Rows: rows, Bytes: info.Size(), ObjectVersion: uploaded.Version}
	return tx.publishCommitted(committed, nil)
}

// publishCommitted joins renewal and fences one root CAS. Caller holds finishMu,
// owns cleanup, and has already confirmed every immutable object being exposed.
func (tx *objectTransaction) publishCommitted(committed *objectCommitted, prepared *Snapshot) (Snapshot, error) {
	if tx.readerReference != nil {
		if err := tx.sealProtectedCommit(committed); err != nil {
			return Snapshot{}, err
		}
	}
	snapshotAt := func(reference time.Time) (Snapshot, error) {
		if prepared != nil {
			snapshot := *prepared
			snapshot.RefreshedAt = committed.RefreshedAt
			return snapshot.observeClock(reference), nil
		}
		return tx.backend.snapshot(tx.dataset, committed, reference)
	}

	// An interrupted renewal may have succeeded remotely. Re-read the same key
	// after joining the renewer, then publish only against this owner's revision.
	tx.stopRenewal()
	if err := context.Cause(tx.ctx); err != nil {
		return Snapshot{}, err
	}
	state, err := tx.backend.readState(tx.ctx, tx.dataset, tx.client)
	if err != nil {
		return Snapshot{}, err
	}
	if !tx.ownsWriter(state) {
		return Snapshot{}, ErrLeaseLost
	}
	if tx.readerReference == nil {
		committed.RefreshedAt = state.now()
	}
	manifest, err := nextObjectManifest(state.manifest, committed)
	if err != nil {
		return Snapshot{}, err
	}
	published, err := tx.backend.writeState(tx.ctx, tx.client, state, manifest)
	if err != nil {
		if errors.Is(err, objectstore.ErrConflict) {
			return Snapshot{}, ErrLeaseLost
		}
		// Never delete the uploaded object or clear the writer following an
		// ambiguous publish: the manifest may already expose this generation.
		tx.owned = false
		reconcile, cancel := context.WithTimeout(context.WithoutCancel(tx.ctx), 5*time.Second)
		defer cancel()
		current, readErr := tx.backend.readState(reconcile, tx.dataset, tx.client)
		if readErr == nil && sameObjectCommit(current.manifest.Committed, committed) {
			return snapshotAt(current.now())
		}
		return Snapshot{}, fmt.Errorf("%w: %w", ErrPublicationUnknown, errors.Join(err, readErr))
	}
	tx.owned = false
	return snapshotAt(published.now())
}

func sameObjectCommit(left, right *objectCommitted) bool {
	return left != nil && right != nil && left.Generation == right.Generation && left.Fingerprint == right.Fingerprint &&
		left.SchemaHash == right.SchemaHash && left.SHA256 == right.SHA256 && left.Rows == right.Rows && left.Bytes == right.Bytes &&
		left.RefreshedAt.Equal(right.RefreshedAt) && left.ObjectVersion == right.ObjectVersion && sameObjectDescriptor(left.Descriptor, right.Descriptor) &&
		sameReaderBinding(left.ReaderBinding, right.ReaderBinding)
}

func (tx *objectTransaction) Abort() error {
	tx.finishMu.Lock()
	defer tx.finishMu.Unlock()
	if tx.done {
		return nil
	}
	tx.done = true
	return tx.cleanup()
}

func (tx *objectTransaction) cleanup() (err error) {
	defer func() { tx.backend.writerFinished(tx, err) }()
	tx.stopRenewal()
	var errs []error
	if tx.owned {
		// Release is a CAS on the same manifest and can only remove our lease.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		state, err := tx.backend.readState(ctx, tx.dataset, tx.client)
		if err == nil && state.manifest.Writer != nil && state.manifest.Writer.Owner == tx.owner &&
			sameReaderReference(state.manifest.Writer.ReaderReference, tx.readerReference) {
			manifest := state.manifest
			manifest.Writer = nil
			_, err = tx.backend.writeState(ctx, tx.client, state, manifest)
		}
		cancel()
		if err != nil && !errors.Is(err, ErrNotFound) && !errors.Is(err, objectstore.ErrConflict) {
			errs = append(errs, err)
		}
		tx.owned = false
	}
	if tx.local != nil {
		errs = append(errs, tx.local.Abort())
	}
	if tx.closeClient && tx.client != nil {
		tx.client.Close()
		tx.closeClient = false
	}
	if tx.cancel != nil {
		tx.cancel(context.Canceled)
	}
	return refreshCleanupError(errors.Join(errs...))
}

func objectSHA256(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

var _ Backend = (*objectBackend)(nil)
var _ RefreshWriter = (*objectTransaction)(nil)

func sameObjectDescriptor(left, right *objectDescriptorRef) bool {
	if left == nil || right == nil {
		return left == right
	}
	return *left == *right
}
