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
	"sort"
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

// Snapshot describes an immutable generation. Query code must hold an acquired
// Lease for as long as an engine may open or read Path.
type Snapshot struct {
	Dataset     string    `yaml:"dataset"`
	Generation  string    `yaml:"generation"`
	Path        string    `yaml:"path"`
	Fingerprint string    `yaml:"fingerprint"`
	SHA256      string    `yaml:"sha256"`
	Rows        int64     `yaml:"rows"`
	Bytes       int64     `yaml:"bytes"`
	RefreshedAt time.Time `yaml:"refreshed_at"`
}

type storeManifest struct {
	Version     int       `yaml:"version"`
	Dataset     string    `yaml:"dataset"`
	Generation  string    `yaml:"generation"`
	Fingerprint string    `yaml:"fingerprint"`
	SHA256      string    `yaml:"sha256"`
	Rows        int64     `yaml:"rows"`
	Bytes       int64     `yaml:"bytes"`
	RefreshedAt time.Time `yaml:"refreshed_at"`
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

func (s *Store) Begin(ctx context.Context, dataset string) (*Transaction, error) {
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
			_ = tx.cleanup()
		}
	}()
	tx.writer, err = storeLockNamed(ctx, dir, ".writer.lock", true)
	if err != nil {
		return nil, err
	}
	if err := storeCleanStages(ctx, dir); err != nil {
		return nil, err
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
		return Snapshot{}, err
	}
	tx.file = nil
	metadata, err := storeLockNamed(tx.ctx, tx.dir, ".metadata.lock", true)
	if err != nil {
		return Snapshot{}, err
	}
	defer metadata.Close()
	// Refuse a symlink, hard link, or insecure manifest rather than overwriting it.
	previous, err := storeOpenFile(tx.dir, storeManifestName, os.O_RDONLY, 0)
	if err == nil {
		err = storeCheckPrivate(previous, false, true)
		_ = previous.Close()
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return Snapshot{}, err
	}
	payload := tx.generation + ".parquet"
	if err := storePublishPayload(tx.dir, tx.stage, payload); err != nil {
		return Snapshot{}, err
	}
	published := false
	manifestStage := ".manifest-" + tx.generation + ".yaml"
	defer func() {
		_ = storeRemove(tx.dir, manifestStage)
		if !published {
			_ = storeRemove(tx.dir, payload)
		}
	}()
	manifest := storeManifest{Version: 1, Dataset: tx.dataset, Generation: tx.generation,
		Fingerprint: fingerprint, SHA256: digest, Rows: rows, Bytes: info.Size(), RefreshedAt: time.Now().UTC()}
	data, err := yaml.Marshal(manifest)
	if err != nil {
		return Snapshot{}, err
	}
	if len(data) > storeManifestLimit {
		return Snapshot{}, errors.New("acceleration manifest exceeds size limit")
	}
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
	return errors.Join(errs...)
}

// Lease pins a generation across workers until Close. Do not copy a Lease.
type Lease struct {
	Snapshot Snapshot
	file     *os.File
	once     sync.Once
	err      error
}

func (lease *Lease) Close() error {
	lease.once.Do(func() { lease.err = lease.file.Close() })
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
	digest, err := storeHash(ctx, lease.file)
	if err != nil {
		return Snapshot{}, err
	}
	if digest != lease.Snapshot.SHA256 {
		return Snapshot{}, fmt.Errorf("%w: snapshot checksum mismatch", ErrCorrupt)
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
	if err != nil {
		return err
	}
	active, err := storeValidatePayload(dir, manifest)
	if err != nil {
		return err
	}
	_ = active.Close()
	entries, err := dir.ReadDir(-1)
	if err != nil {
		return err
	}
	type candidate struct {
		name     string
		modified time.Time
	}
	var candidates []candidate
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		name := entry.Name()
		generation := strings.TrimSuffix(name, ".parquet")
		if name == generation || !storeGenerationID.MatchString(generation) {
			continue
		}
		file, err := storeOpenFile(dir, name, os.O_RDONLY, 0)
		if err != nil {
			return fmt.Errorf("%w: cannot inspect retained generation: %w", ErrCorrupt, err)
		}
		err = storeCheckPrivate(file, false, true)
		info, statErr := file.Stat()
		_ = file.Close()
		if err != nil {
			return err
		}
		if statErr != nil {
			return statErr
		}
		if generation != manifest.Generation {
			candidates = append(candidates, candidate{name, info.ModTime()})
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].modified.Equal(candidates[j].modified) {
			return candidates[i].name > candidates[j].name
		}
		return candidates[i].modified.After(candidates[j].modified)
	})
	changed := false
	defer func() {
		if changed {
			_ = dir.Sync()
		}
	}()
	for index, candidate := range candidates {
		if index < keep-1 {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		file, err := storeOpenFile(dir, candidate.name, os.O_RDONLY, 0)
		if err != nil {
			return err
		}
		locked, err := storeTryLock(file, true)
		if err == nil && locked {
			err = storeRemove(dir, candidate.name)
			changed = err == nil || changed
		}
		_ = file.Close()
		if err != nil {
			return err
		}
	}
	if changed {
		return dir.Sync()
	}
	return nil
}

func storeReadManifest(dir *os.File, dataset string) (storeManifest, error) {
	var manifest storeManifest
	file, err := storeOpenFile(dir, storeManifestName, os.O_RDONLY, 0)
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
	if manifest.Version != 1 || manifest.Dataset != dataset || !storeGenerationID.MatchString(manifest.Generation) ||
		!storeDigest.MatchString(manifest.SHA256) || manifest.Fingerprint == "" || len(manifest.Fingerprint) > storeFingerprintLimit ||
		manifest.Rows < 0 || manifest.Bytes < 0 || manifest.RefreshedAt.IsZero() {
		return manifest, fmt.Errorf("%w: invalid snapshot manifest fields", ErrCorrupt)
	}
	return manifest, nil
}

func (manifest storeManifest) snapshot(directory string) Snapshot {
	return Snapshot{Dataset: manifest.Dataset, Generation: manifest.Generation,
		Path: filepath.Join(directory, manifest.Generation+".parquet"), Fingerprint: manifest.Fingerprint,
		SHA256: manifest.SHA256, Rows: manifest.Rows, Bytes: manifest.Bytes, RefreshedAt: manifest.RefreshedAt}
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
	return errors.Join(err, file.Close())
}

func storeLockNamed(ctx context.Context, dir *os.File, name string, exclusive bool) (*os.File, error) {
	file, err := storeOpenFile(dir, name, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}
	if err := storeLock(ctx, file, exclusive); err != nil {
		_ = file.Close()
		return nil, err
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
		if !stage && !manifest {
			continue
		}
		file, err := storeOpenFile(dir, name, os.O_RDONLY, 0)
		if err != nil {
			return fmt.Errorf("inspect abandoned snapshot stage: %w", err)
		}
		_ = file.Close()
		if err := storeRemove(dir, name); err != nil {
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
