// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/admission"
	"github.com/SYNEHQ/kelvo-go/internal/exports"
)

// ExportPart owns a verified storage lease and the download memory reservation.
// Close is required even after EOF. Each open is independent and repeatable.
type ExportPart struct {
	runtime     *ExportRuntime
	snapshot    ExportSnapshot
	part        *exports.Part
	ctx         context.Context
	cancel      context.CancelFunc
	stopRuntime func() bool
	watchDone   chan struct{}
	release     func()
	once        sync.Once
	closeErr    error
}

func (r *ExportRuntime) readyExport(ctx context.Context, id string, expected *ExportReceipt) (ExportSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return ExportSnapshot{}, err
	}
	s, err := r.store.GetExport(ctx, id)
	if err != nil {
		return ExportSnapshot{}, err
	}
	if s.Job.State != ExportReady || s.Job.WorkerID != r.cfg.WorkerID || !time.Now().Before(s.Job.ExpiresAt) || s.Job.Local == nil || s.Job.Local.StorageID != r.custody.ID() || validateExportJob(r.cfg.Policy, s.Job) != nil || validateExportReceipt(r.cfg.Policy, s.Job) != nil || !sameExportReceipt(s.Job.Receipt, expected) {
		return ExportSnapshot{}, ErrExportNotFound
	}
	if _, err = exportIdentity(r.cfg.Policy, s.Job.Authority); err != nil {
		return ExportSnapshot{}, ErrExportNotFound
	}
	return s, nil
}

func (r *ExportRuntime) OpenPart(parent context.Context, expected ExportSnapshot, index int) (_ *ExportPart, resultErr error) {
	if validateExportJob(r.cfg.Policy, expected.Job) != nil || expected.Job.Receipt == nil || index < 0 || index >= len(expected.Job.Receipt.Manifest.Parts) {
		return nil, ErrExportNotFound
	}
	ctx, cancel := context.WithCancel(parent)
	stopRuntime := context.AfterFunc(r.ctx, cancel)
	keep := false
	defer func() {
		if !keep {
			stopRuntime()
			cancel()
		}
	}()
	r.mu.Lock()
	if r.draining || r.ctx.Err() != nil || r.failure != nil {
		r.mu.Unlock()
		return nil, ErrExportConflict
	}
	select {
	case r.downloads <- struct{}{}:
	default:
		r.mu.Unlock()
		return nil, ErrExportCapacity
	}
	r.handlers.Add(1)
	r.mu.Unlock()
	var releaseOnce sync.Once
	releaseSlot := func() { releaseOnce.Do(func() { <-r.downloads; r.handlers.Done() }) }
	defer func() {
		if !keep {
			releaseSlot()
		}
	}()
	s, err := r.readyExport(ctx, expected.Job.ID, expected.Job.Receipt)
	if err != nil {
		return nil, err
	}
	budget, err := r.pool.Acquire(ctx, admission.Request{Class: admission.ClassExport, MemoryBytes: r.cfg.Exports.DownloadMemoryMB << 20})
	if err != nil {
		return nil, err
	}
	defer func() {
		if !keep {
			budget.Release()
		}
	}()
	p := &ExportPart{runtime: r, snapshot: s, ctx: ctx, cancel: cancel, stopRuntime: stopRuntime, watchDone: make(chan struct{}), release: func() { budget.Release(); releaseSlot() }}
	// Withdrawal/expiry applies while expensive verification is still running,
	// not only after returning the first result byte.
	go p.watch()
	defer func() {
		if !keep {
			cancel()
			<-p.watchDone
		}
	}()
	identity, err := exportIdentity(r.cfg.Policy, s.Job.Authority)
	if err != nil {
		return nil, err
	}
	reader, err := r.storage.Acquire(ctx, s.Job.Local.ExportID, identity)
	if err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(reader.Manifest(), s.Job.Receipt.Manifest) {
		return nil, errors.Join(exports.ErrCorrupt, reader.Close())
	}
	part, err := reader.OpenPart(ctx, index, identity)
	err = errors.Join(err, reader.Close())
	if err != nil {
		if part != nil {
			err = errors.Join(err, part.Close())
		}
		return nil, err
	}
	p.part = part
	if err = p.FinalReady(); err != nil {
		return nil, errors.Join(err, part.Close())
	}
	keep = true
	return p, nil
}

func (p *ExportPart) watch() {
	defer close(p.watchDone)
	deadline := time.NewTimer(max(time.Duration(0), time.Until(p.snapshot.Job.ExpiresAt)))
	defer deadline.Stop()
	tick := time.NewTicker(min(p.runtime.cfg.Policy.LeaseDuration/3, 250*time.Millisecond))
	defer tick.Stop()
	for {
		select {
		case <-p.ctx.Done():
			return
		case <-deadline.C:
			p.cancel()
			return
		case <-tick.C:
			ctx, stop := context.WithTimeout(p.ctx, p.runtime.cfg.Policy.LeaseDuration/3)
			_, err := p.runtime.readyExport(ctx, p.snapshot.Job.ID, p.snapshot.Job.Receipt)
			stop()
			if err != nil {
				p.cancel()
				return
			}
		}
	}
}

func (p *ExportPart) Read(buffer []byte) (int, error) {
	if err := p.ctx.Err(); err != nil {
		return 0, err
	}
	return p.part.Read(buffer)
}

// FinalReady must pass before an HTTP handler releases Arrow EOS. Earlier
// payload bytes cannot be recalled; failed/withdrawn transfers stay incomplete.
func (p *ExportPart) FinalReady() error {
	_, err := p.runtime.readyExport(p.ctx, p.snapshot.Job.ID, p.snapshot.Job.Receipt)
	if err == nil {
		err = p.ctx.Err()
	}
	return err
}

func (p *ExportPart) Close() error {
	p.once.Do(func() {
		p.stopRuntime()
		p.cancel()
		<-p.watchDone
		p.closeErr = p.part.Close()
		p.release()
	})
	return p.closeErr
}
