// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"go.yaml.in/yaml/v3"
)

const MaxSnapshotParts = 256

var multipartPayloadName = regexp.MustCompile(`^([0-9a-f]{32})-part-([0-9]{4})\.parquet$`)
var multipartStageName = regexp.MustCompile(`^\.stage-[0-9a-f]{32}-part-[0-9]{4}\.parquet$`)

func multipartName(generation string, index int) string {
	return fmt.Sprintf("%s-part-%04d.parquet", generation, index)
}

// The generation digest authenticates the ordered descriptors, not concatenated
// Parquet bytes. Each descriptor independently authenticates its complete file.
func multipartDigest(parts []SnapshotPart) string {
	h := sha256.New()
	for _, p := range parts {
		fmt.Fprintf(h, "%s\x00%d\x00%d\x00%s\n", p.Path, p.Rows, p.Bytes, p.SHA256)
	}
	return hex.EncodeToString(h.Sum(nil))
}
func validateManifestParts(m storeManifest) error {
	bad := func() error { return fmt.Errorf("%w: invalid multipart manifest", ErrCorrupt) }
	if m.Version == 1 {
		if len(m.Parts) != 0 {
			return bad()
		}
		return nil
	}
	if len(m.Parts) == 0 || len(m.Parts) > MaxSnapshotParts || !storeDigest.MatchString(m.SchemaHash) {
		return bad()
	}
	var rows, bytes int64
	for i, p := range m.Parts {
		if p.ObjectKey != "" || p.ObjectVersion != "" || p.Path != multipartName(m.Generation, i) || p.Rows < 0 || p.Bytes <= 0 || !storeDigest.MatchString(p.SHA256) || p.Rows > m.Rows-rows || p.Bytes > m.Bytes-bytes {
			return bad()
		}
		rows += p.Rows
		bytes += p.Bytes
	}
	if rows != m.Rows || bytes != m.Bytes || multipartDigest(m.Parts) != m.SHA256 {
		return bad()
	}
	return nil
}
func openPart(dir *os.File, p SnapshotPart) (*os.File, error) {
	f, err := storeOpenFile(dir, p.Path, os.O_RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("%w: cannot open snapshot part", ErrCorrupt)
	}
	err = storeCheckPrivate(f, false, true)
	info, se := f.Stat()
	if err == nil {
		err = se
	}
	if err == nil && info.Size() != p.Bytes {
		err = fmt.Errorf("%w: snapshot part size changed", ErrCorrupt)
	}
	if err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}
func closeParts(files []*os.File) error {
	var errs []error
	for _, f := range files {
		errs = append(errs, f.Close())
	}
	return errors.Join(errs...)
}
func acquireMultipart(ctx context.Context, dir *os.File, m storeManifest) (*Lease, error) {
	var files []*os.File
	for _, p := range m.Parts {
		f, err := openPart(dir, p)
		if err == nil {
			err = storeLock(ctx, f, false)
		}
		if err != nil {
			if f != nil {
				f.Close()
			}
			closeParts(files)
			return nil, err
		}
		files = append(files, f)
	}
	return &Lease{Snapshot: m.snapshot(dir.Name()), files: files, release: func() error { return closeParts(files) }}, nil
}
func verifyMultipartGeneration(ctx context.Context, dir *os.File, m storeManifest) (*arrow.Schema, error) {
	var schema *arrow.Schema
	for _, p := range m.Parts {
		f, err := openPart(dir, p)
		if err != nil {
			return nil, err
		}
		partSchema, err := verifyMultipartPart(ctx, f, p, m.SchemaHash)
		err = errors.Join(err, f.Close())
		if err != nil {
			return nil, err
		}
		if schema != nil && !SchemaEqual(schema, partSchema) {
			return nil, fmt.Errorf("%w: snapshot part schema mismatch", ErrCorrupt)
		}
		schema = partSchema
	}
	return schema, nil
}

type multipartTransaction struct {
	tx          *Transaction
	options     MultipartOptions
	parts       []SnapshotPart
	stages      []string
	schema      *arrow.Schema
	rows, bytes int64
}

func (s *Store) BeginMultipart(ctx context.Context, dataset string, options MultipartOptions) (MultipartRefreshWriter, error) {
	if options.MaxParts < 1 || options.MaxParts > MaxSnapshotParts || options.MaxPartBytes < 1 || options.MaxTotalBytes < 1 {
		return nil, errors.New("multipart snapshot requires positive bounded limits")
	}
	tx, err := s.Begin(ctx, dataset)
	if err != nil {
		return nil, err
	}
	if err = tx.file.Close(); err != nil {
		tx.file = nil
		return nil, errors.Join(refreshCleanupError(err), tx.Abort())
	}
	tx.file = nil
	if err = storeRemove(tx.dir, tx.stage); err != nil {
		return nil, errors.Join(refreshCleanupError(err), tx.Abort())
	}
	tx.stage = ""
	return &multipartTransaction{tx: tx, options: options}, nil
}
func (t *multipartTransaction) Context() context.Context               { return t.tx.ctx }
func (t *multipartTransaction) PreviousSchema() (*arrow.Schema, error) { return t.tx.PreviousSchema() }
func (t *multipartTransaction) SetSchema(schema *arrow.Schema) error {
	t.tx.mu.Lock()
	defer t.tx.mu.Unlock()
	if t.tx.done {
		return errors.New("acceleration transaction is already finished")
	}
	if err := t.tx.ctx.Err(); err != nil {
		return err
	}
	if t.schema != nil && !SchemaEqual(t.schema, schema) {
		return errors.New("multipart snapshot schema mismatch")
	}
	hash, err := SchemaFingerprint(schema)
	if err != nil {
		return err
	}
	t.tx.schemaHash = hash
	return nil
}
func (t *multipartTransaction) NewPart() (*os.File, error) {
	tx := t.tx
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.done {
		return nil, errors.New("acceleration transaction is already finished")
	}
	if err := tx.ctx.Err(); err != nil {
		return nil, err
	}
	if tx.file != nil {
		return nil, errors.New("seal current snapshot part first")
	}
	if len(t.parts) >= t.options.MaxParts {
		return nil, errors.New("multipart snapshot part limit exceeded")
	}
	stage := ".stage-" + multipartName(tx.generation, len(t.parts))
	f, err := storeOpenFile(tx.dir, stage, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return nil, err
	}
	t.stages = append(t.stages, stage)
	tx.file = f
	return f, nil
}
func (t *multipartTransaction) SealPart(rows int64) error {
	tx := t.tx
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.done || tx.file == nil {
		return errors.New("no active snapshot part")
	}
	if err := tx.ctx.Err(); err != nil {
		return err
	}
	f := tx.file
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if rows < 0 || rows > int64(^uint64(0)>>1)-t.rows || info.Size() <= 0 || info.Size() > t.options.MaxPartBytes || info.Size() > t.options.MaxTotalBytes-t.bytes {
		return errors.New("multipart snapshot size or row limit exceeded")
	}
	var actualRows int64
	schema, err := readParquetSchema(f, info.Size(), &actualRows)
	if err != nil {
		return err
	}
	if actualRows != rows {
		return fmt.Errorf("%w: snapshot part row count mismatch", ErrCorrupt)
	}
	if t.schema != nil && !SchemaEqual(t.schema, schema) {
		return errors.New("multipart snapshot schema mismatch")
	}
	if err = verifySchemaHash(schema, tx.schemaHash); err != nil {
		return err
	}
	if err = f.Chmod(0400); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	digest, err := storeHash(tx.ctx, f)
	if err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		tx.file = nil
		return refreshCleanupError(err)
	}
	tx.file = nil
	t.schema = schema
	t.parts = append(t.parts, SnapshotPart{Path: multipartName(tx.generation, len(t.parts)), Rows: rows, Bytes: info.Size(), SHA256: digest})
	t.rows += rows
	t.bytes += info.Size()
	return nil
}
func (t *multipartTransaction) cleanup() error {
	var errs []error
	for _, stage := range t.stages {
		if err := storeRemove(t.tx.dir, stage); err != nil && !errors.Is(err, os.ErrNotExist) {
			errs = append(errs, err)
		}
	}
	errs = append(errs, t.tx.cleanup())
	return refreshCleanupError(errors.Join(errs...))
}
func (t *multipartTransaction) Abort() error {
	tx := t.tx
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.done {
		return nil
	}
	tx.done = true
	return t.cleanup()
}
func (t *multipartTransaction) Commit(fingerprint string) (snapshot Snapshot, err error) {
	tx := t.tx
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.done {
		return Snapshot{}, errors.New("acceleration transaction is already finished")
	}
	tx.done = true
	defer func() { err = errors.Join(err, t.cleanup()) }()
	if tx.file != nil || len(t.parts) == 0 || len(fingerprint) == 0 || len(fingerprint) > storeFingerprintLimit {
		return Snapshot{}, errors.New("multipart snapshot requires sealed parts and bounded fingerprint")
	}
	if err = tx.ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	schemaHash, err := SchemaFingerprint(t.schema)
	if err != nil {
		return Snapshot{}, err
	}
	if tx.schemaHash != "" && tx.schemaHash != schemaHash {
		return Snapshot{}, errors.New("multipart snapshot schema mismatch")
	}
	m := storeManifest{Version: 2, SchemaHash: schemaHash, Dataset: tx.dataset, Generation: tx.generation, Fingerprint: fingerprint, Parts: t.parts, Rows: t.rows, Bytes: t.bytes, SHA256: multipartDigest(t.parts), RefreshedAt: time.Now().UTC()}
	if err = validateManifestParts(m); err != nil {
		return Snapshot{}, err
	}
	data, err := yaml.Marshal(m)
	if err != nil {
		return Snapshot{}, err
	}
	if len(data) > storeManifestLimit {
		return Snapshot{}, errors.New("snapshot manifest exceeds size limit")
	}
	metadata, err := storeLockNamed(tx.ctx, tx.dir, ".metadata.lock", true)
	if err != nil {
		return Snapshot{}, err
	}
	defer func() { err = errors.Join(err, refreshCleanupError(metadata.Close())) }()
	prior, err := storeReadManifest(tx.dir, tx.dataset)
	if err == nil {
		err = storeSaveGeneration(tx.dir, prior)
	}
	if err != nil && !errors.Is(err, ErrNotFound) {
		return Snapshot{}, err
	}
	var publishedParts []string
	published := false
	sidecar := false
	stage := ".manifest-" + tx.generation + ".yaml"
	defer func() {
		err = errors.Join(err, removeRefreshFile(tx.dir, stage))
		if !published {
			for _, name := range publishedParts {
				err = errors.Join(err, removeRefreshFile(tx.dir, name))
			}
			if sidecar {
				err = errors.Join(err, removeRefreshFile(tx.dir, generationManifestName(tx.generation)))
			}
		}
	}()
	for i, p := range t.parts {
		if err = storePublishPayload(tx.dir, t.stages[i], p.Path); err != nil {
			return Snapshot{}, err
		}
		publishedParts = append(publishedParts, p.Path)
	}
	if _, err = storeVerifyGeneration(tx.ctx, tx.dir, m); err != nil {
		return Snapshot{}, err
	}
	if err = storeSaveGeneration(tx.dir, m); err != nil {
		return Snapshot{}, err
	}
	sidecar = true
	if err = storeWriteManifest(tx.dir, stage, data); err != nil {
		return Snapshot{}, err
	}
	if err = tx.dir.Sync(); err != nil {
		return Snapshot{}, err
	}
	if err = tx.ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	if err = storeRename(tx.dir, stage, storeManifestName); err != nil {
		return Snapshot{}, err
	}
	published = true
	snapshot = m.snapshot(tx.dir.Name())
	if err = tx.dir.Sync(); err != nil {
		return snapshot, fmt.Errorf("snapshot published but directory durability is uncertain: %w", err)
	}
	return snapshot, nil
}

// Metadata lock remains held while all part locks are acquired. Pruning removes
// no member until every part of the generation is exclusively locked.
func pruneGenerations(ctx context.Context, dir *os.File, current storeManifest, keep int) error {
	if current.Version == 2 {
		lease, err := acquireMultipart(ctx, dir, current)
		if err != nil {
			return err
		}
		lease.Close()
	} else {
		f, err := storeValidatePayload(dir, current)
		if err != nil {
			return err
		}
		f.Close()
	}
	entries, err := dir.ReadDir(-1)
	if err != nil {
		return err
	}
	// A durable tombstone is the only authority allowing missing payloads.
	retired := map[string]bool{}
	for _, entry := range entries {
		name := entry.Name()
		id := strings.TrimSuffix(strings.TrimPrefix(name, ".prune-"), ".yaml")
		if name != ".prune-"+id+".yaml" || !storeGenerationID.MatchString(id) {
			continue
		}
		if id == current.Generation {
			return fmt.Errorf("%w: active generation is marked retired", ErrCorrupt)
		}
		retired[id] = true
		if err := resumeRetirement(ctx, dir, current.Dataset, id); err != nil {
			return err
		}
	}
	if len(retired) > 0 {
		if _, err = dir.Seek(0, io.SeekStart); err != nil {
			return err
		}
		entries, err = dir.ReadDir(-1)
		if err != nil {
			return err
		}
	}
	if err = recoverOrphanParts(ctx, dir, current, entries); err != nil {
		return err
	}
	type candidate struct {
		generation string
		parts      []SnapshotPart
		modified   time.Time
	}
	candidates := map[string]candidate{}
	for _, entry := range entries {
		name := entry.Name()
		id := strings.TrimSuffix(name, ".yaml")
		if name != id && storeGenerationID.MatchString(id) && id != current.Generation && !retired[id] {
			m, err := storeReadManifestNamed(dir, current.Dataset, name)
			if err != nil {
				return err
			}
			if m.Generation != id {
				return ErrCorrupt
			}
			if m.Version == 2 {
				candidates[id] = candidate{id, m.Parts, m.RefreshedAt}
			}
		}
	}
	for _, entry := range entries {
		name := entry.Name()
		id := strings.TrimSuffix(name, ".parquet")
		if name == id || !storeGenerationID.MatchString(id) || id == current.Generation || retired[id] {
			continue
		}
		if _, ok := candidates[id]; ok {
			continue
		}
		f, err := storeOpenFile(dir, name, os.O_RDONLY, 0)
		if err != nil {
			return err
		}
		err = storeCheckPrivate(f, false, true)
		info, se := f.Stat()
		f.Close()
		if err != nil {
			return err
		}
		if se != nil {
			return se
		}
		candidates[id] = candidate{id, []SnapshotPart{{Path: name, Bytes: info.Size()}}, info.ModTime()}
	}
	ordered := make([]candidate, 0, len(candidates))
	for _, c := range candidates {
		ordered = append(ordered, c)
	}
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].modified.Equal(ordered[j].modified) {
			return ordered[i].generation > ordered[j].generation
		}
		return ordered[i].modified.After(ordered[j].modified)
	})
	changed := false
	defer func() {
		if changed {
			dir.Sync()
		}
	}()
	for i, c := range ordered {
		if i < keep-1 {
			continue
		}
		if err = ctx.Err(); err != nil {
			return err
		}
		var files []*os.File
		locked := true
		for _, p := range c.parts {
			f, e := openPart(dir, p)
			if e != nil {
				closeParts(files)
				return e
			}
			files = append(files, f)
			ok, e := storeTryLock(f, true)
			if e != nil {
				closeParts(files)
				return e
			}
			if !ok {
				locked = false
				break
			}
		}
		if locked {
			// Retire metadata durably before deleting any member. On crash the
			// next Prune resumes only this explicitly retired generation.
			sidecar := generationManifestName(c.generation)
			tombstone := ".prune-" + c.generation + ".yaml"
			hasSidecar := true
			if err = storeRename(dir, sidecar, tombstone); errors.Is(err, os.ErrNotExist) {
				hasSidecar = false
				err = nil
			}
			if err != nil {
				closeParts(files)
				return err
			}
			if !hasSidecar && (len(c.parts) != 1 || c.parts[0].Path != c.generation+".parquet") {
				closeParts(files)
				return fmt.Errorf("%w: multipart retirement metadata missing", ErrCorrupt)
			}
			if hasSidecar {
				if err = dir.Sync(); err != nil {
					closeParts(files)
					return err
				}
			}
			for _, p := range c.parts {
				if err = storeRemove(dir, p.Path); err != nil {
					closeParts(files)
					return err
				}
				changed = true
			}
			if err = dir.Sync(); err != nil {
				closeParts(files)
				return err
			}
			if err = storeRemove(dir, tombstone); err != nil && !errors.Is(err, os.ErrNotExist) {
				closeParts(files)
				return err
			}
		}
		closeParts(files)
	}
	if changed {
		return dir.Sync()
	}
	return nil
}

var _ MultipartRefreshWriter = (*multipartTransaction)(nil)

// resumeRetirement is invoked with exclusive writer and metadata locks. Missing
// files are accepted only under this fsynced tombstone, never under a live manifest.
func resumeRetirement(ctx context.Context, dir *os.File, dataset, id string) error {
	name := ".prune-" + id + ".yaml"
	m, err := storeReadManifestNamed(dir, dataset, name)
	if err != nil {
		return err
	}
	if m.Generation != id {
		return ErrCorrupt
	}
	parts := m.Parts
	if m.Version == 1 {
		parts = []SnapshotPart{{Path: id + ".parquet", Bytes: m.Bytes}}
	}
	var files []*os.File
	defer func() { closeParts(files) }()
	for _, p := range parts {
		if err = ctx.Err(); err != nil {
			return err
		}
		// Open directly to distinguish a missing retired part from other corruption.
		f, e := storeOpenFile(dir, p.Path, os.O_RDONLY, 0)
		if errors.Is(e, os.ErrNotExist) {
			continue
		}
		if e != nil {
			return e
		}
		files = append(files, f)
		if e = storeCheckPrivate(f, false, true); e != nil {
			return e
		}
		info, e := f.Stat()
		if e != nil {
			return e
		}
		if info.Size() != p.Bytes {
			return ErrCorrupt
		}
		locked, e := storeTryLock(f, true)
		if e != nil {
			return e
		}
		if !locked {
			return nil
		}
	}
	for _, p := range parts {
		if err = ctx.Err(); err != nil {
			return err
		}
		if err = storeRemove(dir, p.Path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	if err = dir.Sync(); err != nil {
		return err
	}
	if err = storeRemove(dir, name); err != nil {
		return err
	}
	return dir.Sync()
}

func verifyMultipartPart(ctx context.Context, f *os.File, p SnapshotPart, schemaHash string) (*arrow.Schema, error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	digest, err := storeHash(ctx, f)
	if err != nil {
		return nil, err
	}
	if digest != p.SHA256 {
		return nil, fmt.Errorf("%w: snapshot part checksum mismatch", ErrCorrupt)
	}
	var rows int64
	schema, err := readParquetSchema(f, p.Bytes, &rows)
	if err != nil {
		return nil, err
	}
	if rows != p.Rows {
		return nil, fmt.Errorf("%w: snapshot part row count mismatch", ErrCorrupt)
	}
	if err = verifySchemaHash(schema, schemaHash); err != nil {
		return nil, err
	}
	return schema, nil
}

// Final-named payloads can survive an interrupted publication before its sidecar
// exists. Only exact generated private immutable files with NO generation or
// retirement metadata can be reclaimed; malformed metadata is never absence.
func recoverOrphanParts(ctx context.Context, dir *os.File, current storeManifest, entries []os.DirEntry) error {
	groups := map[string][]string{}
	for _, entry := range entries {
		match := multipartPayloadName.FindStringSubmatch(entry.Name())
		if match == nil || match[1] == current.Generation {
			continue
		}
		var index int
		if _, err := fmt.Sscanf(match[2], "%d", &index); err != nil || index >= MaxSnapshotParts {
			return fmt.Errorf("%w: invalid orphan snapshot part index", ErrCorrupt)
		}
		groups[match[1]] = append(groups[match[1]], entry.Name())
	}
	for id, names := range groups {
		if err := ctx.Err(); err != nil {
			return err
		}
		absent := true
		for _, name := range []string{generationManifestName(id), ".prune-" + id + ".yaml"} {
			m, err := storeReadManifestNamed(dir, current.Dataset, name)
			if err == nil {
				if m.Generation != id {
					return ErrCorrupt
				}
				absent = false
				continue
			}
			if !errors.Is(err, ErrNotFound) {
				return err
			}
		}
		if !absent {
			continue
		}
		sort.Strings(names)
		var files []*os.File
		locked := true
		for _, name := range names {
			f, err := storeOpenFile(dir, name, os.O_RDONLY, 0)
			if err != nil {
				closeParts(files)
				return err
			}
			files = append(files, f)
			if err = storeCheckPrivate(f, false, true); err != nil {
				closeParts(files)
				return err
			}
			ok, err := storeTryLock(f, true)
			if err != nil {
				closeParts(files)
				return err
			}
			if !ok {
				locked = false
				break
			}
		}
		if locked {
			for _, name := range names {
				if err := storeRemove(dir, name); err != nil {
					closeParts(files)
					return err
				}
			}
			if err := dir.Sync(); err != nil {
				closeParts(files)
				return err
			}
		}
		closeParts(files)
	}
	return nil
}
