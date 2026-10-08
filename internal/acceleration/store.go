// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"go.yaml.in/yaml/v3"
)

var (
	ErrNotFound            = errors.New("acceleration snapshot not found")
	ErrFingerprintMismatch = errors.New("acceleration snapshot configuration changed")
	ErrStale               = errors.New("acceleration snapshot is stale")
	ErrCorrupt             = errors.New("acceleration snapshot is invalid")
	ErrUnsupportedPlatform = errors.New("acceleration snapshot storage requires Linux or macOS POSIX file locking")

	storeTenantID     = regexp.MustCompile(`^[a-z][a-z0-9-]{0,31}$`)
	storeDatasetID    = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,62}$`)
	storeGenerationID = regexp.MustCompile(`^[0-9a-f]{32}$`)
	storeDigest       = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

const (
	storeManifestName     = "current.yaml"
	storeManifestLimit    = 64 << 10
	storeFingerprintLimit = 4096
)

// Store is a private tenant namespace on a shared POSIX volume. Workers sharing
// it must use the same OS user and a filesystem providing flock, atomic rename,
// and fsync semantics. Store owns no open descriptors and is safe for concurrent
// use. Its directory is server configuration, never a request-provided path.
type Store struct {
	directory string
	tenant    string
}

// SnapshotPart is one immutable Parquet payload in an ordered generation.
// Path is an exact local filename, never a wildcard or an object prefix.
type SnapshotPart struct {
	ObjectKey     string `yaml:"object_key,omitempty"`
	ObjectVersion string `yaml:"object_version,omitempty"`
	Path          string `yaml:"path"`
	Rows          int64  `yaml:"rows"`
	Bytes         int64  `yaml:"bytes"`
	SHA256        string `yaml:"sha256"`
}

// Snapshot describes an immutable generation. Query code must hold its Lease
// while an engine may open Path or any Parts entry. Multipart Path is empty.
type Snapshot struct {
	SchemaHash    string         `yaml:"schema_hash,omitempty"`
	Dataset       string         `yaml:"dataset"`
	Generation    string         `yaml:"generation"`
	Path          string         `yaml:"path"`
	Fingerprint   string         `yaml:"fingerprint"`
	SHA256        string         `yaml:"sha256"`
	Rows          int64          `yaml:"rows"`
	Bytes         int64          `yaml:"bytes"`
	RefreshedAt   time.Time      `yaml:"refreshed_at"`
	Parts         []SnapshotPart `yaml:"parts,omitempty"`
	ObjectKey     string         `yaml:"object_key,omitempty"`
	ObjectVersion string         `yaml:"object_version,omitempty"`
	// These process-local observations never enter manifests or JSON responses.
	ageObservedAt time.Time
	ageObserved   time.Duration
}

// Age uses the storage clock when one was observed, then advances with elapsed
// monotonic time. Local snapshots retain their local filesystem clock behavior.
func (snapshot Snapshot) Age() time.Duration {
	if snapshot.ageObservedAt.IsZero() {
		return max(0, time.Since(snapshot.RefreshedAt))
	}
	elapsed := max(time.Duration(0), time.Since(snapshot.ageObservedAt))
	const maximum time.Duration = 1<<63 - 1
	if snapshot.ageObserved > maximum-elapsed {
		return maximum
	}
	return snapshot.ageObserved + elapsed
}

func (snapshot Snapshot) observeClock(reference time.Time) Snapshot {
	snapshot.ageObserved = max(0, reference.Sub(snapshot.RefreshedAt))
	snapshot.ageObservedAt = time.Now()
	return snapshot
}

type storeManifest struct {
	Parts       []SnapshotPart `yaml:"parts,omitempty"`
	SchemaHash  string         `yaml:"schema_hash,omitempty"`
	Version     int            `yaml:"version"`
	Dataset     string         `yaml:"dataset"`
	Generation  string         `yaml:"generation"`
	Fingerprint string         `yaml:"fingerprint"`
	SHA256      string         `yaml:"sha256"`
	Rows        int64          `yaml:"rows"`
	Bytes       int64          `yaml:"bytes"`
	RefreshedAt time.Time      `yaml:"refreshed_at"`
}

// OpenStore creates or validates private namespaces. It never changes the
// permissions of an existing directory and rejects symlink components.
func OpenStore(directory, tenant string) (*Store, error) {
	if err := storePlatformSupported(); err != nil {
		return nil, err
	}
	if directory == "" || !storeTenantID.MatchString(tenant) {
		return nil, errors.New("acceleration requires a directory and a valid tenant identifier")
	}
	abs, err := filepath.Abs(directory)
	if err != nil {
		return nil, err
	}
	root, err := storeOpenDirectory(abs, true)
	if err != nil {
		return nil, fmt.Errorf("open acceleration directory: %w", err)
	}
	defer root.Close()
	namespace, err := storeOpenChildDirectory(root, tenant, true)
	if err != nil {
		return nil, fmt.Errorf("open acceleration tenant namespace: %w", err)
	}
	if err := namespace.Close(); err != nil {
		return nil, err
	}
	return &Store{directory: abs, tenant: tenant}, nil
}

func (s *Store) openDataset(dataset string, create bool) (*os.File, error) {
	if !storeDatasetID.MatchString(dataset) {
		return nil, errors.New("acceleration dataset must be a SQL identifier of at most 63 characters")
	}
	root, err := storeOpenDirectory(s.directory, false)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	namespace, err := storeOpenChildDirectory(root, s.tenant, false)
	if err != nil {
		return nil, err
	}
	defer namespace.Close()
	dir, err := storeOpenChildDirectory(namespace, dataset, create)
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNotFound
	}
	return dir, err
}

// Transaction holds the process-safe writer lock until Commit or Abort. The
// producer must finish all writes to File before either method is called.
type Transaction struct {
	schemaHash string
	mu         sync.Mutex
	ctx        context.Context
	dir        *os.File
	writer     *os.File
	file       *os.File
	dataset    string
	generation string
	stage      string
	done       bool
}

func (s *Store) Begin(ctx context.Context, dataset string) (_ *Transaction, resultErr error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	dir, err := s.openDataset(dataset, true)
	if err != nil {
		return nil, err
	}
	tx := &Transaction{ctx: ctx, dir: dir, dataset: dataset}
	ok := false
	defer func() {
		if !ok {
			resultErr = errors.Join(resultErr, tx.cleanup())
		}
	}()
	tx.writer, err = storeLockNamed(ctx, dir, ".writer.lock", true)
	if err != nil {
		return nil, err
	}
	if err := storeCleanStages(ctx, dir); err != nil {
		return nil, refreshCleanupError(err)
	}
	var generation [16]byte
	if _, err := rand.Read(generation[:]); err != nil {
		return nil, err
	}
	tx.generation = hex.EncodeToString(generation[:])
	stage := ".stage-" + tx.generation + ".parquet"
	tx.file, err = storeOpenFile(dir, stage, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return nil, err
	}
	tx.stage = stage
	ok = true
	return tx, nil
}

func (tx *Transaction) File() *os.File {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return tx.file
}

func (tx *Transaction) Context() context.Context { return tx.ctx }

// Commit makes a fully flushed generation visible by atomically replacing its
// YAML manifest. An error before publication leaves the previous manifest intact.
// A directory fsync error after publication returns the published Snapshot with
// an error, indicating uncertain crash durability; the old payload is preserved.
// A transaction is finished after any Commit result, and Abort remains safe.
func (tx *Transaction) Commit(fingerprint string, rows int64) (snapshot Snapshot, err error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.done {
		return Snapshot{}, errors.New("acceleration transaction is already finished")
	}
	tx.done = true
	defer func() { err = errors.Join(err, tx.cleanup()) }()
	if fingerprint == "" || len(fingerprint) > storeFingerprintLimit || rows < 0 {
		return Snapshot{}, errors.New("acceleration snapshot requires a bounded fingerprint and nonnegative row count")
	}
	if err := tx.ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	if err := tx.file.Chmod(0400); err != nil {
		return Snapshot{}, err
	}
	if err := tx.file.Sync(); err != nil {
		return Snapshot{}, err
	}
	info, err := tx.file.Stat()
	if err != nil {
		return Snapshot{}, err
	}
	if _, err := tx.file.Seek(0, io.SeekStart); err != nil {
		return Snapshot{}, err
	}
	digest, err := storeHash(tx.ctx, tx.file)
	if err != nil {
		return Snapshot{}, err
	}
	if err := tx.file.Close(); err != nil {
		tx.file = nil
		return Snapshot{}, refreshCleanupError(err)
	}
	tx.file = nil
	metadata, err := storeLockNamed(tx.ctx, tx.dir, ".metadata.lock", true)
	if err != nil {
		return Snapshot{}, err
	}
	defer func() { err = errors.Join(err, refreshCleanupError(metadata.Close())) }()
	// Refuse a symlink, hard link, or insecure manifest rather than overwriting it.
	previous, err := storeOpenFile(tx.dir, storeManifestName, os.O_RDONLY, 0)
	if err == nil {
		err = storeCheckPrivate(previous, false, true)
		err = errors.Join(err, refreshCleanupError(previous.Close()))
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return Snapshot{}, err
	}
	payload := tx.generation + ".parquet"
	if err := storePublishPayload(tx.dir, tx.stage, payload); err != nil {
		return Snapshot{}, err
	}
	published := false
	sidecarPublished := false
	manifestStage := ".manifest-" + tx.generation + ".yaml"
	defer func() {
		err = errors.Join(err, removeRefreshFile(tx.dir, manifestStage))
		if !published {
			if sidecarPublished {
				err = errors.Join(err, removeRefreshFile(tx.dir, generationManifestName(tx.generation)))
			}
			err = errors.Join(err, removeRefreshFile(tx.dir, payload))
		}
	}()
	manifest := storeManifest{SchemaHash: tx.schemaHash, Version: 1, Dataset: tx.dataset, Generation: tx.generation,
		Fingerprint: fingerprint, SHA256: digest, Rows: rows, Bytes: info.Size(), RefreshedAt: time.Now().UTC()}
	data, err := yaml.Marshal(manifest)
	if err != nil {
		return Snapshot{}, err
	}
	if len(data) > storeManifestLimit {
		return Snapshot{}, errors.New("acceleration manifest exceeds size limit")
	}
	// Backfill the previous current metadata for pre-upgrade snapshots, then
	// persist the new generation's immutable metadata before the pointer swap.
	prior, readErr := storeReadManifest(tx.dir, tx.dataset)
	if readErr == nil {
		if err := storeSaveGeneration(tx.dir, prior); err != nil {
			return Snapshot{}, err
		}
	} else if !errors.Is(readErr, ErrNotFound) {
		return Snapshot{}, readErr
	}
	if err := storeSaveGeneration(tx.dir, manifest); err != nil {
		return Snapshot{}, err
	}
	sidecarPublished = true
	if err := storeWriteManifest(tx.dir, manifestStage, data); err != nil {
		return Snapshot{}, err
	}
	// Persist the new payload's directory entry before exposing its manifest.
	if err := tx.dir.Sync(); err != nil {
		return Snapshot{}, err
	}
	if err := tx.ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	if err := storeRename(tx.dir, manifestStage, storeManifestName); err != nil {
		return Snapshot{}, err
	}
	published = true
	snapshot = manifest.snapshot(tx.dir.Name())
	if err := tx.dir.Sync(); err != nil {
		return snapshot, fmt.Errorf("snapshot published but directory durability is uncertain: %w", err)
	}
	return snapshot, nil
}

func removeRefreshFile(dir *os.File, name string) error {
	err := storeRemove(dir, name)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return refreshCleanupError(err)
}

func (tx *Transaction) Abort() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.done {
		return nil
	}
	tx.done = true
	return tx.cleanup()
}

func (tx *Transaction) cleanup() error {
	var errs []error
	if tx.file != nil {
		errs = append(errs, tx.file.Close())
		tx.file = nil
	}
	if tx.dir != nil && tx.stage != "" {
		if err := storeRemove(tx.dir, tx.stage); err != nil && !errors.Is(err, os.ErrNotExist) {
			errs = append(errs, err)
		}
	}
	if tx.writer != nil {
		errs = append(errs, tx.writer.Close())
		tx.writer = nil
	}
	if tx.dir != nil {
		errs = append(errs, tx.dir.Close())
		tx.dir = nil
	}
	return refreshCleanupError(errors.Join(errs...))
}

// Lease pins a generation across workers until Close. Do not copy a Lease.
type Lease struct {
	Snapshot Snapshot
	files    []*os.File
	file     *os.File
	release  func() error
	once     sync.Once
	err      error
}

func (lease *Lease) Close() error {
	lease.once.Do(func() {
		if lease.release != nil {
			lease.err = lease.release()
		} else if lease.file != nil {
			lease.err = lease.file.Close()
		}
	})
	return lease.err
}

// Acquire checks the fingerprint and optional maximum age (zero disables the
// age limit). It validates the immutable payload's metadata without reading its
// full contents; Verify performs an explicit SHA-256 integrity audit.
func (s *Store) Acquire(ctx context.Context, dataset, fingerprint string, maxAge time.Duration) (*Lease, error) {
	if maxAge < 0 {
		return nil, errors.New("acceleration maximum age cannot be negative")
	}
	return s.acquire(ctx, dataset, fingerprint, maxAge, true)
}

func (s *Store) acquire(ctx context.Context, dataset, fingerprint string, maxAge time.Duration, checkPolicy bool) (*Lease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	dir, err := s.openDataset(dataset, false)
	if err != nil {
		return nil, err
	}
	defer dir.Close()
	metadata, err := storeLockNamed(ctx, dir, ".metadata.lock", false)
	if err != nil {
		return nil, err
	}
	defer metadata.Close()
	manifest, err := storeReadManifest(dir, dataset)
	if err != nil {
		return nil, err
	}
	if checkPolicy {
		if manifest.Fingerprint != fingerprint {
			return nil, ErrFingerprintMismatch
		}
		if maxAge > 0 && time.Since(manifest.RefreshedAt) > maxAge {
			return nil, ErrStale
		}
	}
	if manifest.Version == 2 {
		return acquireMultipart(ctx, dir, manifest)
	}
	payload, err := storeValidatePayload(dir, manifest)
	if err != nil {
		return nil, err
	}
	ok := false
	defer func() {
		if !ok {
			_ = payload.Close()
		}
	}()
	if err := storeLock(ctx, payload, false); err != nil {
		return nil, err
	}
	ok = true
	return &Lease{Snapshot: manifest.snapshot(dir.Name()), file: payload}, nil
}

// Status returns validated current metadata. It is not a query lease.
func (s *Store) Status(dataset string) (Snapshot, error) {
	lease, err := s.acquire(context.Background(), dataset, "", 0, false)
	if err != nil {
		return Snapshot{}, err
	}
	return lease.Snapshot, lease.Close()
}

// Verify pins and hashes the complete current generation for an explicit audit.
func (s *Store) Verify(ctx context.Context, dataset string) (Snapshot, error) {
	lease, err := s.acquire(ctx, dataset, "", 0, false)
	if err != nil {
		return Snapshot{}, err
	}
	defer lease.Close()
	if len(lease.Snapshot.Parts) > 0 {
		for i, file := range lease.files {
			if _, err := verifyMultipartPart(ctx, file, lease.Snapshot.Parts[i], lease.Snapshot.SchemaHash); err != nil {
				return Snapshot{}, err
			}
		}
		return lease.Snapshot, nil
	}
	// Single-file and multipart generations obey the same integrity contract:
	// matching bytes alone do not prove the manifest's row/schema metadata.
	if _, err := verifyMultipartPart(ctx, lease.file, SnapshotPart{
		Rows: lease.Snapshot.Rows, Bytes: lease.Snapshot.Bytes, SHA256: lease.Snapshot.SHA256,
	}, lease.Snapshot.SchemaHash); err != nil {
		return Snapshot{}, err
	}
	return lease.Snapshot, nil
}

// Prune retains the active generation, up to keep-1 other newest generations,
// and every leased generation. keep must be positive. In-use generations can
// temporarily exceed the retention count and are collected by a later Prune.
func (s *Store) Prune(ctx context.Context, dataset string, keep int) error {
	if keep < 1 {
		return errors.New("acceleration retention must keep at least one generation")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	dir, err := s.openDataset(dataset, false)
	if err != nil {
		return err
	}
	defer dir.Close()
	writer, err := storeLockNamed(ctx, dir, ".writer.lock", true)
	if err != nil {
		return err
	}
	defer writer.Close()
	metadata, err := storeLockNamed(ctx, dir, ".metadata.lock", true)
	if err != nil {
		return err
	}
	defer metadata.Close()
	manifest, err := storeReadManifest(dir, dataset)
	if errors.Is(err, ErrNotFound) {
		// An initial publication may crash before any current pointer exists. Only
		// genuine absence authorizes orphan inspection; malformed current metadata
		// must never be treated as an empty dataset. Complete generations with valid
		// sidecars remain retained until normal pruning after a successful publish.
		entries, readErr := dir.ReadDir(-1)
		if readErr != nil {
			return readErr
		}
		if recoverErr := recoverOrphanParts(ctx, dir, storeManifest{Dataset: dataset}, entries); recoverErr != nil {
			return recoverErr
		}
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	return pruneGenerations(ctx, dir, manifest, keep)
}

func storeReadManifest(dir *os.File, dataset string) (storeManifest, error) {
	return storeReadManifestNamed(dir, dataset, storeManifestName)
}

func storeReadManifestNamed(dir *os.File, dataset, name string) (storeManifest, error) {
	var manifest storeManifest
	file, err := storeOpenFile(dir, name, os.O_RDONLY, 0)
	if errors.Is(err, os.ErrNotExist) {
		return manifest, ErrNotFound
	}
	if err != nil {
		return manifest, fmt.Errorf("%w: cannot open snapshot manifest: %w", ErrCorrupt, err)
	}
	defer file.Close()
	if err := storeCheckPrivate(file, false, true); err != nil {
		return manifest, err
	}
	data, err := io.ReadAll(io.LimitReader(file, storeManifestLimit+1))
	if err != nil {
		return manifest, err
	}
	if len(data) > storeManifestLimit {
		return manifest, fmt.Errorf("%w: snapshot manifest exceeds size limit", ErrCorrupt)
	}
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(&manifest); err != nil {
		return manifest, fmt.Errorf("%w: malformed snapshot manifest", ErrCorrupt)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return manifest, fmt.Errorf("%w: snapshot manifest must contain one YAML document", ErrCorrupt)
	}
	if (manifest.SchemaHash != "" && !storeDigest.MatchString(manifest.SchemaHash)) || (manifest.Version != 1 && manifest.Version != 2) || manifest.Dataset != dataset || !storeGenerationID.MatchString(manifest.Generation) ||
		!storeDigest.MatchString(manifest.SHA256) || manifest.Fingerprint == "" || len(manifest.Fingerprint) > storeFingerprintLimit ||
		manifest.Rows < 0 || manifest.Bytes < 0 || manifest.RefreshedAt.IsZero() {
		return manifest, fmt.Errorf("%w: invalid snapshot manifest fields", ErrCorrupt)
	}
	if err := validateManifestParts(manifest); err != nil {
		return manifest, err
	}
	return manifest, nil
}

func (manifest storeManifest) snapshot(directory string) Snapshot {
	snapshot := Snapshot{SchemaHash: manifest.SchemaHash, Dataset: manifest.Dataset, Generation: manifest.Generation,
		Path: filepath.Join(directory, manifest.Generation+".parquet"), Fingerprint: manifest.Fingerprint,
		SHA256: manifest.SHA256, Rows: manifest.Rows, Bytes: manifest.Bytes, RefreshedAt: manifest.RefreshedAt}
	if manifest.Version == 2 {
		snapshot.Path = ""
		snapshot.Parts = append([]SnapshotPart(nil), manifest.Parts...)
		for i := range snapshot.Parts {
			snapshot.Parts[i].Path = filepath.Join(directory, snapshot.Parts[i].Path)
		}
	}
	return snapshot
}

func storeValidatePayload(dir *os.File, manifest storeManifest) (*os.File, error) {
	payload, err := storeOpenFile(dir, manifest.Generation+".parquet", os.O_RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("%w: cannot open snapshot payload: %w", ErrCorrupt, err)
	}
	err = storeCheckPrivate(payload, false, true)
	info, statErr := payload.Stat()
	if err == nil {
		err = statErr
	}
	if err == nil && info.Size() != manifest.Bytes {
		err = fmt.Errorf("%w: snapshot payload size changed", ErrCorrupt)
	}
	if err != nil {
		_ = payload.Close()
		return nil, err
	}
	return payload, nil
}

func storeWriteManifest(dir *os.File, name string, data []byte) error {
	file, err := storeOpenFile(dir, name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	if _, err = file.Write(data); err == nil {
		err = file.Chmod(0400)
	}
	if err == nil {
		err = file.Sync()
	}
	return errors.Join(err, refreshCleanupError(file.Close()))
}

func storeLockNamed(ctx context.Context, dir *os.File, name string, exclusive bool) (*os.File, error) {
	file, err := storeOpenFile(dir, name, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}
	if err := storeLock(ctx, file, exclusive); err != nil {
		return nil, errors.Join(err, refreshCleanupError(file.Close()))
	}
	return file, nil
}

func storeLock(ctx context.Context, file *os.File, exclusive bool) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		locked, err := storeTryLock(file, exclusive)
		if err != nil {
			return err
		}
		if locked {
			return nil
		}
		timer := time.NewTimer(20 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func storeCleanStages(ctx context.Context, dir *os.File) error {
	entries, err := dir.ReadDir(-1)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		name := entry.Name()
		id := strings.TrimSuffix(strings.TrimPrefix(name, ".stage-"), ".parquet")
		stage := name == ".stage-"+id+".parquet" && storeGenerationID.MatchString(id)
		id = strings.TrimSuffix(strings.TrimPrefix(name, ".manifest-"), ".yaml")
		manifest := name == ".manifest-"+id+".yaml" && storeGenerationID.MatchString(id)
		if !stage && !manifest && !multipartStageName.MatchString(name) {
			continue
		}
		file, err := storeOpenFile(dir, name, os.O_RDONLY, 0)
		if err != nil {
			return fmt.Errorf("inspect abandoned snapshot stage: %w", err)
		}
		if err := errors.Join(file.Close(), storeRemove(dir, name)); err != nil {
			return err
		}
	}
	return nil
}

func storeHash(ctx context.Context, file *os.File) (string, error) {
	hash := sha256.New()
	buffer := make([]byte, 256<<10)
	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		n, err := file.Read(buffer)
		if n > 0 {
			_, _ = hash.Write(buffer[:n])
		}
		if errors.Is(err, io.EOF) {
			return hex.EncodeToString(hash.Sum(nil)), nil
		}
		if err != nil {
			return "", err
		}
	}
}
