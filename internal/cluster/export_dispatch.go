// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"errors"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
)

func (r *ExportRuntime) dispatchExports() {
	defer r.wg.Done()
	defer close(r.dispatchDone)
	for {
		if !r.audit.Ready() {
			select {
			case <-r.dispatchCtx.Done():
				return
			case <-time.After(100 * time.Millisecond):
				continue
			}
		}
		select {
		case <-r.dispatchCtx.Done():
			return
		case r.permits <- struct{}{}:
		}
		d, err := r.store.NextExport(r.dispatchCtx)
		if err != nil {
			<-r.permits
			if r.dispatchCtx.Err() != nil {
				return
			}
			select {
			case <-r.dispatchCtx.Done():
				return
			case <-time.After(100 * time.Millisecond):
				continue
			}
		}
		s, err := r.store.GetExport(r.dispatchCtx, d.ID())
		if err != nil || s.Job.State != ExportQueued {
			if err == nil || errors.Is(err, ErrExportNotFound) {
				_ = d.Ack(r.ctx)
			} else {
				_ = d.Retry(r.ctx)
			}
			<-r.permits
			continue
		}
		if validateExportJob(r.cfg.Policy, s.Job) != nil || exportLive(s.Job) != nil || !time.Now().Before(s.Job.QueueDeadline) {
			next := s.Job
			next.State = ExportFailed
			next.Error = query.PublicError(context.DeadlineExceeded)
			if _, err = r.store.CompareAndSwapExport(r.ctx, s, next); err == nil || errors.Is(err, ErrExportConflict) {
				_ = d.Ack(r.ctx)
			} else {
				_ = d.Retry(r.ctx)
			}
			<-r.permits
			continue
		}
		ctx, cancel := context.WithDeadline(r.ctx, s.Job.ExpiresAt)
		reservation := &exportReservation{ctx: ctx, cancel: cancel, done: make(chan struct{})}
		r.mu.Lock()
		if r.draining || r.ctx.Err() != nil {
			r.mu.Unlock()
			cancel()
			_ = d.Retry(r.ctx)
			<-r.permits
			return
		}
		r.jobs[s.Job.ID] = reservation
		r.mu.Unlock()
		next := s.Job
		next.State, next.WorkerID, next.WorkerOwner = ExportAssigned, r.cfg.WorkerID, r.owner
		assigned, err := r.store.CompareAndSwapExport(r.ctx, s, next)
		if err != nil {
			r.releaseExport(s.Job.ID, reservation)
			if errors.Is(err, ErrExportConflict) {
				_ = d.Ack(r.ctx)
			} else {
				_ = d.Retry(r.ctx)
			}
			continue
		}
		// A redelivery sees Assigned and is acknowledged without execution. The
		// supervising gateway must claim and keep the execute request alive.
		_ = d.Ack(r.ctx)
		r.wg.Add(1)
		go r.watchExport(assigned, reservation)
	}
}

func (r *ExportRuntime) watchExport(initial ExportSnapshot, reservation *exportReservation) {
	defer r.wg.Done()
	id := initial.Job.ID
	tick := time.NewTicker(min(r.cfg.Policy.LeaseDuration/3, 250*time.Millisecond))
	defer tick.Stop()
	deadline := time.NewTimer(max(time.Duration(0), time.Until(initial.Job.AuthorityUntil)))
	defer deadline.Stop()
	stop := func(cause error) {
		select {
		case <-reservation.done:
			return
		default:
		}
		reservation.cancel()
		_ = r.failExport(id, cause, false)
		r.stopExport(id, reservation)
	}
	for {
		select {
		case <-reservation.done:
			return
		case <-reservation.ctx.Done():
			stop(reservation.ctx.Err())
			return
		case <-deadline.C:
			stop(context.DeadlineExceeded)
			return
		case <-tick.C:
			ctx, cancel := context.WithTimeout(reservation.ctx, r.cfg.Policy.LeaseDuration/3)
			s, err := r.ownedExport(ctx, id)
			if err == nil {
				if !exportActive(s.Job) {
					err = ErrExportConflict
				} else {
					err = exportLive(s.Job)
				}
				if err == nil && (s.Job.State == ExportAssigned || s.Job.State == ExportClaimed) && !time.Now().Before(s.Job.QueueDeadline) {
					err = context.DeadlineExceeded
				}
			}
			if err == nil && time.Since(s.Job.HeartbeatAt) >= r.cfg.Policy.LeaseDuration/3 {
				_, err = r.updateExport(ctx, id, func(j *ExportJob) error {
					if !exportActive(*j) {
						return ErrExportConflict
					}
					if err := exportLive(*j); err != nil {
						return err
					}
					// An unchanged active CAS requests a worker heartbeat. The
					// store stamps its own time and validates the worker lease.
					return nil
				})
			}
			cancel()
			if err != nil {
				stop(err)
				return
			}
			if !deadline.Stop() {
				select {
				case <-deadline.C:
				default:
				}
			}
			deadline.Reset(max(time.Duration(0), time.Until(s.Job.AuthorityUntil)))
		}
	}
}
