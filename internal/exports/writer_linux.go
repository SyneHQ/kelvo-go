//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package exports

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	arrowutil "github.com/apache/arrow-go/v18/arrow/util"
	"go.yaml.in/yaml/v3"
	"golang.org/x/sys/unix"
)

type Writer struct {
	mu                     sync.Mutex
	store                  *Store
	dir, lease             *os.File
	state                  state
	schema                 *arrow.Schema
	parts                  []PartInfo
	rows, encoded, decoded int64
	closed                 bool
	failure                error
}

func (w *Writer) ID() string    { return w.state.ID }
func (w *Writer) Fence() string { return w.state.Fence }
func (w *Writer) release() {
	if !w.closed {
		w.closed = true
		w.lease.Close()
		w.dir.Close()
		w.store.references.Add(-1)
	}
}

// Write consumes the borrowed batch synchronously. Each batch is one complete
// independently decodable part; oversized batches are rejected, never split or
// silently coerced. A failed write poisons the transaction until Close.
func (w *Writer) Write(ctx context.Context, batch arrow.RecordBatch) (resultErr error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	defer func() {
		if recover() != nil {
			w.failure = ErrCorrupt
			resultErr = ErrCorrupt
		}
	}()
	if w.closed {
		return ErrClosed
	}
	if w.failure != nil {
		return w.failure
	}
	if err := ctx.Err(); err != nil {
		w.failure = err
		return err
	}
	if !time.Now().Before(w.state.ExpiresAt) {
		w.failure = ErrUnavailable
		return w.failure
	}
	if batch == nil || batch.NumRows() < 0 || batch.NumRows() > w.state.Limits.MaxRows-w.rows {
		w.failure = ErrLimit
		return w.failure
	}
	decoded := arrowutil.TotalRecordSize(batch)
	if decoded < 0 || decoded > w.state.Limits.MaxPartDecodedBytes || decoded > w.state.Limits.MaxDecodedBytes-w.decoded {
		w.failure = ErrLimit
		return w.failure
	}
	raw, err := canonicalSchema(batch.Schema())
	if err != nil || checksum(raw) != w.state.SchemaSHA256 {
		w.failure = ErrCorrupt
		return w.failure
	}
	part, err := w.writePart(ctx, batch)
	if err != nil {
		w.failure = err
		return err
	}
	w.parts = append(w.parts, part)
	w.rows += part.Rows
	w.encoded += part.EncodedBytes
	w.decoded += part.DecodedBytes
	return nil
}

func (w *Writer) writePart(ctx context.Context, batch arrow.RecordBatch) (part PartInfo, resultErr error) {
	if len(w.parts) >= w.state.Limits.MaxParts {
		return part, ErrLimit
	}
	index := len(w.parts)
	temporary := fmt.Sprintf(".pending-%04d.arrow", index)
	name := partName(index)
	f, err := openFile(w.dir, temporary, os.O_RDWR|os.O_CREATE|os.O_EXCL, false)
	if err != nil {
		return part, err
	}
	published := false
	defer func() {
		f.Close()
		if !published {
			unix.Unlinkat(int(w.dir.Fd()), temporary, 0)
		}
	}()
	maximum := min(w.state.Limits.MaxPartBytes, w.state.Limits.MaxEncodedBytes-w.encoded)
	if maximum <= 0 {
		return part, ErrLimit
	}
	output := &boundedWriter{writer: f, remaining: maximum}
	// Pinned Arrow v18.5.1 executes compressNP<=1 synchronously (writer.go).
	// Keep that explicit: a limiting-allocator panic in an Arrow-owned codec
	// goroutine would not be recoverable here. The subprocess LZ4 limit test
	// must continue to pass on dependency upgrades. Caller buffers are separate.
	allocationLimit := max(w.state.Limits.MaxPartDecodedBytes*2, int64(schemaLimit*2))
	if w.store.encoderAllocationLimit > 0 {
		allocationLimit = w.store.encoderAllocationLimit
	}
	options := []ipc.Option{ipc.WithSchema(w.schema), ipc.WithCompressConcurrency(1), ipc.WithAllocator(&boundedAllocator{base: memory.NewGoAllocator(), limit: allocationLimit})}
	if w.state.Limits.Compression == "lz4_frame" {
		options = append(options, ipc.WithLZ4(), ipc.WithCompressConcurrency(1))
	}
	writer := ipc.NewWriter(output, options...)
	defer writer.Close()
	if batch != nil {
		if err = writer.Write(batch); err != nil {
			return part, err
		}
	}
	if err = writer.Close(); err != nil {
		return part, err
	}
	if err = f.Chmod(0400); err != nil {
		return part, err
	}
	if err = w.store.point("part:file_sync"); err == nil {
		err = f.Sync()
	}
	if err != nil {
		return part, err
	}
	encoded := maximum - output.remaining
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return part, err
	}
	part, err = validatePart(ctx, f, encoded, w.state.Limits, w.state.SchemaSHA256)
	if err != nil {
		return part, err
	}
	if part.DecodedBytes > w.state.Limits.MaxDecodedBytes-w.decoded {
		return part, ErrLimit
	}
	if batch != nil && (part.Rows != batch.NumRows() || part.Batches != 1) {
		return part, ErrCorrupt
	}
	if batch == nil && (part.Rows != 0 || part.Batches != 0 || part.DecodedBytes != 0) {
		return part, ErrCorrupt
	}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return part, err
	}
	digest, err := hashFile(ctx, f)
	if err != nil {
		return part, err
	}
	part.Index, part.SHA256 = index, digest
	if err = f.Close(); err != nil {
		return part, err
	}
	if err = unix.Renameat2(int(w.dir.Fd()), temporary, int(w.dir.Fd()), name, unix.RENAME_NOREPLACE); err != nil {
		return part, err
	}
	published = true
	if err = w.dir.Sync(); err != nil {
		return part, err
	}
	return part, nil
}

func partName(index int) string { return fmt.Sprintf("part-%04d.arrow", index) }
func hashFile(ctx context.Context, f *os.File) (string, error) {
	h := sha256.New()
	buffer := make([]byte, 256<<10)
	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		n, err := f.Read(buffer)
		if n > 0 {
			h.Write(buffer[:n])
		}
		if errors.Is(err, io.EOF) {
			return hex.EncodeToString(h.Sum(nil)), nil
		}
		if err != nil {
			return "", err
		}
	}
}

// Commit's current identity is supplied by trusted authorization. Cancellation
// and publication serialize under the root lock; a stale fence cannot publish.
// A populated manifest plus ErrPublicationUncertain must be preserved.
func (w *Writer) Commit(ctx context.Context, current Identity) (manifest Manifest, resultErr error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	defer func() {
		if recover() != nil {
			w.failure = ErrCorrupt
			resultErr = ErrCorrupt
		}
	}()
	if w.closed {
		return manifest, ErrClosed
	}
	if w.failure != nil {
		return manifest, w.failure
	}
	if !current.valid() || !current.equal(w.state.Identity) {
		return manifest, ErrFenced
	}
	if err := ctx.Err(); err != nil {
		return manifest, err
	}
	if !time.Now().Before(w.state.ExpiresAt) {
		return manifest, ErrUnavailable
	}
	if len(w.parts) == 0 {
		part, err := w.writePart(ctx, nil)
		if err != nil {
			w.failure = err
			return manifest, err
		}
		w.parts = []PartInfo{part}
		w.encoded = part.EncodedBytes
	}
	manifest = Manifest{Version: 1, ID: w.state.ID, Fence: w.state.Fence, Tenant: w.state.Tenant, Identity: w.state.Identity, CreatedAt: w.state.CreatedAt, ExpiresAt: w.state.ExpiresAt, SchemaSHA256: w.state.SchemaSHA256, Rows: w.rows, EncodedBytes: w.encoded, DecodedBytes: w.decoded, Parts: append([]PartInfo(nil), w.parts...)}
	for _, part := range w.parts {
		f, err := verifyFile(ctx, w.dir, part, w.state.Limits, w.state.SchemaSHA256)
		if err != nil {
			w.failure = err
			return Manifest{}, err
		}
		f.Close()
	}
	unlock, err := w.store.guard()
	if err != nil {
		return Manifest{}, err
	}
	defer unlock()
	lock, err := lockRoot(ctx, w.store.root)
	if err != nil {
		return Manifest{}, err
	}
	defer lock.Close()
	if err = w.store.unavailableIntent(w.state.ID); err != nil {
		return Manifest{}, errors.Join(ErrFenced, err)
	}
	stateNow, err := w.store.loadState(w.dir, w.state.ID)
	if err != nil {
		return Manifest{}, err
	}
	if stateNow != w.state || stateNow.Status != "active" || !stateNow.Identity.equal(current) || !time.Now().Before(stateNow.ExpiresAt) {
		return Manifest{}, ErrFenced
	}
	if err = ctx.Err(); err != nil {
		return Manifest{}, err
	}
	raw, err := yaml.Marshal(manifest)
	if err != nil || len(raw) > manifestLimit {
		return Manifest{}, ErrLimit
	}
	if _, err = w.store.atomicWrite(w.dir, "manifest.yml", raw, true, true); err != nil {
		w.failure = err
		return Manifest{}, err
	}
	stateNow.Status, stateNow.ManifestSHA256 = "ready", checksum(raw)
	published, err := w.store.writeState(w.dir, stateNow)
	if err != nil {
		if published {
			w.release()
			return cloneManifest(manifest), errors.Join(ErrPublicationUncertain, err)
		}
		w.failure = err
		return Manifest{}, err
	}
	w.release()
	return cloneManifest(manifest), nil
}

// Close cancels an unpublished fill and releases its writer lease. Files and
// reservations remain until bounded Cleanup; a failed cancellation is explicit.
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	defer w.release()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return w.store.Cancel(ctx, w.state.ID, w.state.Identity)
}
