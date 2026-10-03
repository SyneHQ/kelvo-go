//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package exports

import (
	"context"
	"errors"
	"io"
	"os"
	"sync"
	"time"
)

type Reader struct {
	mu         sync.Mutex
	store      *Store
	dir, lease *os.File
	state      state
	manifest   Manifest
	deadline   time.Time
	closed     bool
}
type Part struct {
	mu          sync.Mutex
	store       *Store
	file, lease *os.File
	ctx         context.Context
	deadline    time.Time
	closed      bool
}

func validManifest(m Manifest, st state) bool {
	if m.Version != 1 || m.ID != st.ID || m.Fence != st.Fence || m.Tenant != st.Tenant || !m.Identity.equal(st.Identity) || !m.CreatedAt.Equal(st.CreatedAt) || !m.ExpiresAt.Equal(st.ExpiresAt) || m.SchemaSHA256 != st.SchemaSHA256 || len(m.Parts) < 1 || len(m.Parts) > st.Limits.MaxParts {
		return false
	}
	var rows, encoded, decoded int64
	for index, p := range m.Parts {
		if p.Index != index || p.Rows < 0 || p.Rows > st.Limits.MaxRows-rows || p.EncodedBytes <= 0 || p.EncodedBytes > st.Limits.MaxPartBytes || p.EncodedBytes > st.Limits.MaxEncodedBytes-encoded || p.DecodedBytes < 0 || p.DecodedBytes > st.Limits.MaxPartDecodedBytes || p.DecodedBytes > st.Limits.MaxDecodedBytes-decoded || !digestPattern.MatchString(p.SHA256) || p.Batches < 0 || p.Batches > 1 || (p.Batches == 0 && (p.Rows != 0 || p.DecodedBytes != 0)) {
			return false
		}
		rows += p.Rows
		encoded += p.EncodedBytes
		decoded += p.DecodedBytes
	}
	return m.Rows == rows && m.EncodedBytes == encoded && m.DecodedBytes == decoded
}

func (s *Store) Acquire(ctx context.Context, id string, current Identity) (*Reader, error) {
	unlock, err := s.guard()
	if err != nil {
		return nil, err
	}
	defer unlock()
	if !idPattern.MatchString(id) || !current.valid() {
		return nil, ErrInvalid
	}
	lock, err := lockRoot(ctx, s.root)
	if err != nil {
		return nil, err
	}
	defer lock.Close()
	if err = s.unavailableIntent(id); err != nil {
		return nil, err
	}
	dir, err := childDir(s.root, id, false)
	if err != nil {
		return nil, err
	}
	lease, err := tryLease(dir, false)
	if err != nil {
		dir.Close()
		return nil, err
	}
	keep := false
	defer func() {
		if !keep {
			lease.Close()
			dir.Close()
		}
	}()
	st, err := s.loadState(dir, id)
	if err != nil {
		return nil, err
	}
	if st.Status != "ready" || !st.Identity.equal(current) || !time.Now().Before(st.ExpiresAt) {
		return nil, ErrUnavailable
	}
	raw, err := readFile(dir, "manifest.yml", manifestLimit, true)
	if err != nil || checksum(raw) != st.ManifestSHA256 {
		return nil, ErrCorrupt
	}
	var manifest Manifest
	if strictYAML(raw, &manifest) != nil || !validManifest(manifest, st) {
		return nil, ErrCorrupt
	}
	schema, err := readFile(dir, "schema.arrow", schemaLimit, true)
	if err != nil || checksum(schema) != st.SchemaSHA256 {
		return nil, ErrCorrupt
	}
	// Parts are verified on OpenPart. This bounds acquisition cost without
	// making a successful metadata acquisition certify all payload bytes.
	now := time.Now()
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if !now.Before(st.ExpiresAt) {
		return nil, ErrUnavailable
	}
	s.references.Add(1)
	keep = true
	return &Reader{store: s, dir: dir, lease: lease, state: st, manifest: manifest, deadline: now.Add(st.ExpiresAt.Sub(now))}, nil
}

func (r *Reader) Manifest() Manifest {
	r.mu.Lock()
	defer r.mu.Unlock()
	return cloneManifest(r.manifest)
}

func verifyFile(ctx context.Context, dir *os.File, expected PartInfo, limits Limits, schemaHash string) (*os.File, error) {
	f, err := openFile(dir, partName(expected.Index), os.O_RDONLY, true)
	if err != nil {
		return nil, err
	}
	keep := false
	defer func() {
		if !keep {
			f.Close()
		}
	}()
	before, err := checkFile(f, false, true)
	if err != nil || before.Size != expected.EncodedBytes {
		return nil, ErrCorrupt
	}
	digest, err := hashFile(ctx, f)
	if err != nil {
		return nil, err
	}
	if digest != expected.SHA256 {
		return nil, ErrCorrupt
	}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	observed, err := validatePart(ctx, f, before.Size, limits, schemaHash)
	if err != nil {
		return nil, err
	}
	if observed.Rows != expected.Rows || observed.Batches != expected.Batches || observed.DecodedBytes != expected.DecodedBytes {
		return nil, ErrCorrupt
	}
	after, err := checkFile(f, false, true)
	if err != nil || !sameStat(before, after) {
		return nil, ErrCorrupt
	}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	keep = true
	return f, nil
}

// OpenPart verifies complete IPC framing, schema, digest and counts before
// returning bytes. Each returned part owns an independent lease. Previously
// admitted reads cannot be globally revoked by this package; they still enforce
// their context and original expiry. Higher layers own authorization lifecycle.
func (r *Reader) OpenPart(ctx context.Context, index int, current Identity) (*Part, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, ErrClosed
	}
	if index < 0 || index >= len(r.manifest.Parts) || !current.valid() {
		return nil, ErrInvalid
	}
	if !time.Now().Before(r.deadline) || !current.equal(r.state.Identity) {
		return nil, ErrUnavailable
	}
	unlock, err := r.store.guard()
	if err != nil {
		return nil, err
	}
	defer unlock()
	lock, err := lockRoot(ctx, r.store.root)
	if err != nil {
		return nil, err
	}
	if err = r.store.unavailableIntent(r.state.ID); err != nil {
		lock.Close()
		return nil, err
	}
	st, err := r.store.loadState(r.dir, r.state.ID)
	if err != nil {
		lock.Close()
		return nil, err
	}
	if st != r.state || st.Status != "ready" || !st.Identity.equal(current) || !time.Now().Before(st.ExpiresAt) {
		lock.Close()
		return nil, ErrUnavailable
	}
	lease, err := tryLease(r.dir, false)
	lock.Close()
	if err != nil {
		return nil, err
	}
	f, err := verifyFile(ctx, r.dir, r.manifest.Parts[index], st.Limits, st.SchemaSHA256)
	if err != nil {
		lease.Close()
		return nil, err
	}
	if err = r.store.point("part:verified"); err != nil {
		lease.Close()
		f.Close()
		return nil, err
	}
	// A cancellation which wins while verification is running must prevent
	// the newly verified bytes from being handed to a download caller.
	lock, err = lockRoot(ctx, r.store.root)
	if err != nil {
		lease.Close()
		f.Close()
		return nil, err
	}
	latest, err := r.store.loadState(r.dir, r.state.ID)
	lock.Close()
	if err != nil || latest != st || !time.Now().Before(r.deadline) {
		lease.Close()
		f.Close()
		return nil, ErrUnavailable
	}
	if err = ctx.Err(); err != nil {
		lease.Close()
		f.Close()
		return nil, err
	}
	r.store.references.Add(1)
	return &Part{store: r.store, file: f, lease: lease, ctx: ctx, deadline: r.deadline}, nil
}

func (r *Reader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	err := errors.Join(r.lease.Close(), r.dir.Close())
	r.store.references.Add(-1)
	return err
}
func (p *Part) Read(buffer []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return 0, ErrClosed
	}
	if err := p.ctx.Err(); err != nil {
		return 0, err
	}
	if !time.Now().Before(p.deadline) {
		return 0, ErrUnavailable
	}
	return p.file.Read(buffer)
}
func (p *Part) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	err := errors.Join(p.file.Close(), p.lease.Close())
	p.store.references.Add(-1)
	return err
}
