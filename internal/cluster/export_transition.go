// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"reflect"
	"time"
)

type exportMutation int

const (
	exportWorkerMutation exportMutation = iota
	exportSupervisorMutation
	exportStopMutation
)

func exportDeadlinesValid(job ExportJob, now time.Time) bool {
	if !exportActive(job) || !now.Before(job.ExpiresAt) || !now.Before(job.AuthorityUntil) {
		return false
	}
	switch job.State {
	case ExportQueued, ExportAssigned, ExportClaimed:
		return now.Before(job.QueueDeadline)
	case ExportRunning:
		return now.Before(job.ExecutionDeadline)
	default:
		// Stored data may be published after SQL's execution deadline, while
		// its supervisor authority and retention window remain valid.
		return true
	}
}

// Store-owned timestamps prevent supervisor renewals from keeping a lost
// worker alive. Callers preserve timestamps; a same-state/no-change CAS is a
// worker heartbeat, while a sole AuthorityUntil change is a supervisor renewal.
func exportTransition(p Policy, cur, next ExportJob, now time.Time) (ExportJob, exportMutation, error) {
	bad := func() (ExportJob, exportMutation, error) { return ExportJob{}, 0, ErrExportConflict }
	if !reflect.DeepEqual(next.Authority, cur.Authority) || next.Version != cur.Version || next.ID != cur.ID || next.TenantID != cur.TenantID || !reflect.DeepEqual(next.Request, cur.Request) || next.Spec != cur.Spec || next.SupervisorOwner != cur.SupervisorOwner || !next.CreatedAt.Equal(cur.CreatedAt) || !next.QueueDeadline.Equal(cur.QueueDeadline) || !next.ExpiresAt.Equal(cur.ExpiresAt) || !next.HeartbeatAt.Equal(cur.HeartbeatAt) || !next.StartedAt.Equal(cur.StartedAt) || !next.ExecutionDeadline.Equal(cur.ExecutionDeadline) {
		return bad()
	}
	if cur.State == ExportFailed || cur.State == ExportCancelled || cur.State == ExportPublicationUncertain {
		return bad()
	}
	if next.State == ExportCancelled || next.State == ExportFailed || next.State == ExportPublicationUncertain {
		if cur.State == ExportReady && next.State != ExportCancelled {
			return bad()
		}
		if next.State == ExportPublicationUncertain && (cur.State != ExportRunning || cur.Local == nil) {
			return bad()
		}
		expected := cur
		expected.State, expected.Error = next.State, next.Error
		if next.Error == nil || !reflect.DeepEqual(expected, next) {
			return bad()
		}
		return next, exportStopMutation, nil
	}
	if !exportDeadlinesValid(cur, now) {
		return bad()
	}
	if !next.AuthorityUntil.Equal(cur.AuthorityUntil) {
		expected := cur
		expected.AuthorityUntil = next.AuthorityUntil
		if !reflect.DeepEqual(expected, next) || !next.AuthorityUntil.After(cur.AuthorityUntil) || next.AuthorityUntil.After(now.Add(p.LeaseDuration)) || next.AuthorityUntil.After(cur.ExpiresAt) {
			return bad()
		}
		return next, exportSupervisorMutation, nil
	}
	if cur.State == ExportStored && next.State == ExportReady {
		expected := cur
		expected.State = ExportReady
		if !reflect.DeepEqual(expected, next) {
			return bad()
		}
		return next, exportSupervisorMutation, nil
	}
	expected := cur
	switch {
	case cur.State == ExportQueued && next.State == ExportAssigned:
		expected.State, expected.WorkerID, expected.WorkerOwner = ExportAssigned, next.WorkerID, next.WorkerOwner
		if p.Workers[next.WorkerID] < 1 || !validOwner(next.WorkerOwner) {
			return bad()
		}
	case cur.State == ExportAssigned && next.State == ExportClaimed:
		expected.State, expected.Claim = ExportClaimed, next.Claim
		if !validClaim(next.Claim) || !reflect.DeepEqual(expected, next) {
			return bad()
		}
		return next, exportSupervisorMutation, nil
	case cur.State == ExportClaimed && next.State == ExportRunning:
		expected.State = ExportRunning
	case cur.State == ExportRunning && next.State == ExportRunning && cur.Local == nil && next.Local != nil:
		if !validExportLocator(next.Local) {
			return bad()
		}
		expected.Local = next.Local
	case cur.State == ExportRunning && next.State == ExportStored:
		if cur.Local == nil || next.Receipt == nil {
			return bad()
		}
		expected.State, expected.Receipt, expected.Stats = ExportStored, next.Receipt, next.Stats
	case cur.State == next.State && cur.State != ExportQueued:
		// A no-change CAS is a worker heartbeat.
	default:
		return bad()
	}
	if !reflect.DeepEqual(expected, next) {
		return bad()
	}
	next.HeartbeatAt = now
	if cur.State == ExportClaimed && next.State == ExportRunning {
		next.StartedAt = now
		next.ExecutionDeadline = minTime(now.Add(next.Spec.QueryLimits.Timeout), next.ExpiresAt)
	}
	return next, exportWorkerMutation, nil
}
