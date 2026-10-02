// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const refreshStatusBucket = "KELVO_ACCEL_STATUS"
const refreshStatusBytes = 256 << 10
const refreshStatusValueLimit = 1024

// RefreshStatus is bounded operational state, never raw source diagnostics.
// Attempts counts failed handlers, not successful publications. The terminal
// states permanent/exhausted suppress scheduled dispatch for this fingerprint.
// No TTL: retries require an explicit operator reset or a new fingerprint.
type RefreshStatus struct {
	ResetThroughSequence uint64    `json:"reset_through_sequence,omitempty" yaml:"reset_through_sequence,omitempty"`
	LastSuccessAt        time.Time `json:"last_success_at,omitempty" yaml:"last_success_at,omitempty"`
	ResetAt              time.Time `json:"reset_at,omitempty" yaml:"reset_at,omitempty"`
	Dataset              string    `json:"dataset" yaml:"dataset"`
	Fingerprint          string    `json:"fingerprint" yaml:"fingerprint"`
	State                string    `json:"state" yaml:"state"`
	Category             string    `json:"category" yaml:"category"`
	Attempts             uint64    `json:"attempts" yaml:"attempts"`
	UpdatedAt            time.Time `json:"updated_at" yaml:"updated_at"`
	NextRetryAt          time.Time `json:"next_retry_at,omitempty" yaml:"next_retry_at,omitempty"`
}

func (s RefreshStatus) stopped() bool { return s.State == "permanent" || s.State == "exhausted" }

type refreshStatusStore interface {
	Get(context.Context, string) (jetstream.KeyValueEntry, error)
	Create(context.Context, string, []byte, ...jetstream.KVCreateOpt) (uint64, error)
	Update(context.Context, string, []byte, uint64) (uint64, error)
	Delete(context.Context, string, ...jetstream.KVDeleteOpt) error
}

func refreshStatusKey(job RefreshJob) string {
	sum := sha256.Sum256([]byte(job.Dataset + ":" + job.Fingerprint))
	return hex.EncodeToString(sum[:])
}

func (s *NATSStore) openRefreshStatus(ctx context.Context, initialize bool) (jetstream.KeyValue, error) {
	kv, err := s.js.KeyValue(ctx, refreshStatusBucket)
	if errors.Is(err, jetstream.ErrBucketNotFound) && initialize {
		kv, err = s.js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: refreshStatusBucket, History: 1, MaxValueSize: refreshStatusValueLimit, MaxBytes: refreshStatusBytes, Replicas: s.policy.Replicas})
		if err != nil {
			kv, err = s.js.KeyValue(ctx, refreshStatusBucket)
		}
	}
	if err != nil {
		return nil, errors.New("refresh status unavailable; initialize the tenant queue first")
	}
	if err := configureKVDirect(ctx, s.js, refreshStatusBucket, initialize); err != nil {
		return nil, err
	}
	kv, err = s.js.KeyValue(ctx, refreshStatusBucket)
	if err != nil {
		return nil, errors.New("refresh status unavailable")
	}
	info, err := kv.Status(ctx)
	if err != nil {
		return nil, errors.New("refresh status configuration unavailable")
	}
	cfg := info.Config()
	if cfg.History != 1 || cfg.TTL != 0 || cfg.MaxBytes != refreshStatusBytes || cfg.MaxValueSize != refreshStatusValueLimit || cfg.Replicas != s.policy.Replicas || cfg.Storage != jetstream.FileStorage {
		return nil, errors.New("refresh status configuration mismatch")
	}
	stream, err := s.js.Stream(ctx, "KV_"+refreshStatusBucket)
	if err != nil {
		return nil, errors.New("refresh status configuration unavailable")
	}
	streamInfo, err := stream.Info(ctx)
	if err != nil {
		return nil, errors.New("refresh status configuration unavailable")
	}
	// A full ledger must reject writes; eviction would silently unstop datasets.
	if streamInfo.Config.Discard != jetstream.DiscardNew || streamInfo.Config.MaxAge != 0 || streamInfo.Config.Retention != jetstream.LimitsPolicy {
		return nil, errors.New("refresh status retention must preserve terminal records")
	}
	return kv, nil
}

func (q *RefreshQueue) loadStatus(ctx context.Context, job RefreshJob) (RefreshStatus, uint64, error) {
	empty := RefreshStatus{Dataset: job.Dataset, Fingerprint: job.Fingerprint}
	if q.status == nil {
		return empty, 0, nil
	}
	entry, err := q.status.Get(ctx, refreshStatusKey(job))
	if errors.Is(err, jetstream.ErrKeyNotFound) || errors.Is(err, jetstream.ErrKeyDeleted) {
		return empty, 0, nil
	}
	if err != nil {
		return empty, 0, errors.New("refresh status unavailable")
	}
	var status RefreshStatus
	if len(entry.Value()) > refreshStatusValueLimit || json.Unmarshal(entry.Value(), &status) != nil || status.Dataset != job.Dataset || status.Fingerprint != job.Fingerprint || status.Attempts > refreshMaxAttempts {
		return empty, 0, errors.New("refresh status is invalid")
	}
	switch status.State {
	case "retrying", "permanent", "exhausted", "ready":
	default:
		return empty, 0, errors.New("refresh status is invalid")
	}
	switch status.Category {
	case "", "unknown", "schema", "configuration", "access", "resource", "timeout", "unavailable", "capacity", "network", "retry_after", "delivery_limit":
	default:
		return empty, 0, errors.New("refresh status is invalid")
	}
	return status, entry.Revision(), nil
}

// Status reads tenant-account-scoped diagnostics for an exact configured dataset
// fingerprint. Its caller is responsible for operator authentication.
func (q *RefreshQueue) Status(ctx context.Context, job RefreshJob) (RefreshStatus, error) {
	if !validRefreshJob(job) {
		return RefreshStatus{}, errors.New("invalid refresh job")
	}
	if q.status == nil {
		return RefreshStatus{}, errors.New("refresh status unavailable")
	}
	status, _, err := q.loadStatus(ctx, job)
	return status, err
}

// Reset clears one fingerprint's failure state using CAS. It does not cancel an
// active refresh or change the catalog. The caller must have operator authority.
func (q *RefreshQueue) Reset(ctx context.Context, job RefreshJob) error {
	if !validRefreshJob(job) {
		return errors.New("invalid refresh job")
	}
	if q.status == nil {
		return errors.New("refresh status unavailable")
	}
	status, revision, err := q.loadStatus(ctx, job)
	if err != nil {
		return err
	}
	if q.latestSequence == nil {
		return errors.New("refresh stream sequence unavailable")
	}
	cutoff, err := q.latestSequence(ctx)
	if err != nil {
		return errors.New("refresh stream sequence unavailable")
	}
	status.ResetThroughSequence = cutoff
	status.State, status.Category, status.Attempts = "ready", "", 0
	status.NextRetryAt = time.Time{}
	status.ResetAt = time.Now().UTC()
	status.UpdatedAt = status.ResetAt
	raw, err := json.Marshal(status)
	if err != nil {
		return errors.New("refresh reset unavailable")
	}
	if revision == 0 {
		_, err = q.status.Create(ctx, refreshStatusKey(job), raw)
	} else {
		_, err = q.status.Update(ctx, refreshStatusKey(job), raw, revision)
	}
	if err != nil {
		return errors.New("refresh reset conflicted or is unavailable")
	}
	return nil
}

func (q *RefreshQueue) recordFailure(ctx context.Context, job RefreshJob, failure *RefreshFailure, delivered uint64, sequence uint64, delay time.Duration) (RefreshStatus, error) {
	for retry := 0; retry < 8; retry++ {
		status, revision, err := q.loadStatus(ctx, job)
		if err != nil {
			return status, err
		}
		if status.stopped() || (status.ResetThroughSequence != 0 && sequence <= status.ResetThroughSequence) {
			return status, nil
		}
		status.Attempts++
		if delivered > status.Attempts {
			status.Attempts = delivered
		}
		if status.Attempts > refreshMaxAttempts {
			status.Attempts = refreshMaxAttempts
		}
		status.Category = failure.Category
		status.State = "retrying"
		status.UpdatedAt = time.Now().UTC()
		status.NextRetryAt = status.UpdatedAt.Add(delay)
		if failure.Permanent {
			status.State = "permanent"
		}
		if status.Attempts >= refreshMaxAttempts && !failure.Permanent {
			status.State = "exhausted"
		}
		if status.stopped() {
			status.NextRetryAt = time.Time{}
		}
		if q.status == nil {
			return status, nil
		}
		raw, err := json.Marshal(status)
		if err != nil || len(raw) > refreshStatusValueLimit {
			return status, errors.New("refresh status is invalid")
		}
		if revision == 0 {
			_, err = q.status.Create(ctx, refreshStatusKey(job), raw)
		} else {
			_, err = q.status.Update(ctx, refreshStatusKey(job), raw, revision)
		}
		if err == nil {
			return status, nil
		}
		// Retry CAS races only. Availability/full-store errors fail closed.
		if !errors.Is(err, jetstream.ErrKeyExists) {
			return status, errors.New("refresh status persistence failed")
		}
	}
	return RefreshStatus{}, errors.New("refresh status update conflicted")
}
