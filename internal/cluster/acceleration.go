// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"sync/atomic"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	refreshQueueStream   = "KELVO_ACCEL_QUEUE"
	refreshQueueSubject  = "acceleration.refresh"
	refreshQueueConsumer = "refresh"
	refreshMessageLimit  = 1024
	refreshPullWait      = time.Second
	refreshHeartbeat     = 10 * time.Second
	refreshRetryDelay    = 10 * time.Second
)

// RefreshJob contains only a configured dataset identifier and its configuration
// fingerprint. Source SQL, credentials and Arrow data never enter this queue.
type RefreshJob struct {
	Dataset     string `json:"dataset"`
	Fingerprint string `json:"fingerprint"`
}

type refreshPublisher interface {
	Publish(context.Context, string, []byte, ...jetstream.PublishOpt) (*jetstream.PubAck, error)
}

type refreshConsumer interface {
	Next(...jetstream.FetchOpt) (jetstream.Msg, error)
}

// RefreshQueue shares its account and connection lifetime with NATSStore. It
// delivers at least once: handlers must lock the dataset writer and recheck its
// fingerprint and freshness before refreshing. Message-ID deduplication is only
// a short-term publication optimization, not an exactly-once guarantee.
type RefreshQueue struct {
	js             refreshPublisher
	consumer       refreshConsumer
	consuming      atomic.Bool
	status         refreshStatusStore
	latestSequence func(context.Context) (uint64, error)
}

func refreshStreamConfig(p Policy) jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name: refreshQueueStream, Subjects: []string{refreshQueueSubject},
		Retention: jetstream.WorkQueuePolicy, Storage: jetstream.FileStorage,
		MaxConsumers: 1, MaxMsgs: 256, MaxBytes: 256 << 10,
		MaxMsgSize: refreshMessageLimit, MaxMsgsPerSubject: -1,
		MaxAge: 24 * time.Hour, Replicas: p.Replicas,
		Discard: jetstream.DiscardNew, Duplicates: 2 * time.Minute,
		Metadata: map[string]string{"kelvo.tenant": p.TenantID},
	}
}

func refreshConsumerConfig(p Policy) jetstream.ConsumerConfig {
	return jetstream.ConsumerConfig{
		Name: refreshQueueConsumer, Durable: refreshQueueConsumer,
		FilterSubject: refreshQueueSubject, DeliverPolicy: jetstream.DeliverAllPolicy,
		AckPolicy: jetstream.AckExplicitPolicy, AckWait: 30 * time.Second,
		MaxDeliver: -1, MaxWaiting: 64, MaxAckPending: 64,
		MaxRequestBatch: 1, MaxRequestExpires: refreshPullWait,
		Replicas: p.Replicas,
	}
}

// NATS reserves _nats.* metadata for server/client compatibility information.
// Ignore only these server-owned annotations when comparing resource policy.
func refreshUserMetadata(in map[string]string) map[string]string {
	var out map[string]string
	for k, v := range in {
		if !strings.HasPrefix(k, "_nats.") {
			if out == nil {
				out = make(map[string]string)
			}
			out[k] = v
		}
	}
	return out
}

func validateRefreshStream(cfg jetstream.StreamConfig, p Policy) error {
	cfg.Metadata = refreshUserMetadata(cfg.Metadata)
	if !reflect.DeepEqual(cfg, refreshStreamConfig(p)) {
		return errors.New("refresh stream configuration mismatch")
	}
	return nil
}

func validateRefreshConsumer(cfg jetstream.ConsumerConfig, p Policy) error {
	cfg.Metadata = refreshUserMetadata(cfg.Metadata)
	if !reflect.DeepEqual(cfg, refreshConsumerConfig(p)) {
		return errors.New("refresh consumer configuration mismatch")
	}
	return nil
}

// OpenRefreshQueue provisions only missing resources when initialize is true.
// Existing resources are always validated and never silently updated. A caller
// without initialization authority performs only read-only configuration checks.
func (s *NATSStore) OpenRefreshQueue(parent context.Context, initialize bool) (*RefreshQueue, error) {
	if s == nil || s.js == nil || ValidatePolicy(s.policy) != nil {
		return nil, errors.New("invalid refresh queue store")
	}
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	stream, err := s.js.Stream(ctx, refreshQueueStream)
	if errors.Is(err, jetstream.ErrStreamNotFound) && initialize {
		stream, err = s.js.CreateStream(ctx, refreshStreamConfig(s.policy))
		if err != nil {
			// A concurrent initializer may have created it; only accept it after
			// the same strict validation used for pre-existing resources.
			stream, err = s.js.Stream(ctx, refreshQueueStream)
		}
	}
	if err != nil {
		return nil, errors.New("refresh stream unavailable; initialize the tenant queue first")
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return nil, errors.New("refresh stream configuration unavailable")
	}
	if err = validateRefreshStream(info.Config, s.policy); err != nil {
		return nil, err
	}
	consumer, err := stream.Consumer(ctx, refreshQueueConsumer)
	if errors.Is(err, jetstream.ErrConsumerNotFound) && initialize {
		consumer, err = stream.CreateConsumer(ctx, refreshConsumerConfig(s.policy))
		if err != nil {
			consumer, err = stream.Consumer(ctx, refreshQueueConsumer)
		}
	}
	if err != nil {
		return nil, errors.New("refresh consumer unavailable; initialize the tenant queue first")
	}
	ci, err := consumer.Info(ctx)
	if err != nil {
		return nil, errors.New("refresh consumer configuration unavailable")
	}
	if err = validateRefreshConsumer(ci.Config, s.policy); err != nil {
		return nil, err
	}
	status, err := s.openRefreshStatus(ctx, initialize)
	if err != nil {
		return nil, err
	}
	return &RefreshQueue{js: s.js, consumer: consumer, status: status, latestSequence: func(ctx context.Context) (uint64, error) {
		info, err := stream.Info(ctx)
		if err != nil {
			return 0, errors.New("refresh stream sequence unavailable")
		}
		return info.State.LastSeq, nil
	}}, nil
}

func validRefreshJob(job RefreshJob) bool {
	if !catalog.ValidID(job.Dataset) || len(job.Fingerprint) != 64 {
		return false
	}
	for _, c := range job.Fingerprint {
		if !(c >= '0' && c <= '9') && !(c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

func validRefreshDedupID(id string) bool {
	if len(id) == 0 || len(id) > 256 {
		return false
	}
	for _, c := range id {
		if !(c >= 'a' && c <= 'z') && !(c >= 'A' && c <= 'Z') &&
			!(c >= '0' && c <= '9') && c != ':' && c != '.' && c != '_' && c != '-' {
			return false
		}
	}
	return true
}

func (q *RefreshQueue) Publish(ctx context.Context, job RefreshJob, dedupID string) error {
	if !validRefreshJob(job) || !validRefreshDedupID(dedupID) {
		return errors.New("invalid refresh job or deduplication ID")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	status, _, err := q.loadStatus(ctx, job)
	if err != nil {
		return err
	}
	if status.stopped() || time.Now().Before(status.NextRetryAt) {
		return nil
	}
	data, err := json.Marshal(job)
	if err != nil || len(data) > refreshMessageLimit {
		return errors.New("invalid refresh job")
	}
	if _, err = q.js.Publish(ctx, refreshQueueSubject, data,
		jetstream.WithMsgID(dedupID), jetstream.WithExpectStream(refreshQueueStream)); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return errors.New("refresh publication failed")
	}
	return nil
}

func decodeRefreshJob(data []byte) (RefreshJob, error) {
	var job RefreshJob
	bad := errors.New("invalid refresh message")
	if len(data) == 0 || len(data) > refreshMessageLimit {
		return job, bad
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	start, err := decoder.Token()
	if err != nil || start != json.Delim('{') {
		return job, bad
	}
	seen := make(map[string]bool, 2)
	for decoder.More() {
		key, err := decoder.Token()
		if err != nil {
			return RefreshJob{}, bad
		}
		name, ok := key.(string)
		if !ok || seen[name] {
			return RefreshJob{}, bad
		}
		seen[name] = true
		switch name {
		case "dataset":
			err = decoder.Decode(&job.Dataset)
		case "fingerprint":
			err = decoder.Decode(&job.Fingerprint)
		default:
			return RefreshJob{}, bad
		}
		if err != nil {
			return RefreshJob{}, bad
		}
	}
	end, err := decoder.Token()
	if err != nil || end != json.Delim('}') || !validRefreshJob(job) {
		return RefreshJob{}, bad
	}
	if _, err = decoder.Token(); !errors.Is(err, io.EOF) {
		return RefreshJob{}, bad
	}
	return job, nil
}

// Consume executes one handler at a time. The handler must honor cancellation:
// shutdown and failed heartbeats wait for it to release its writer lock before
// returning or accepting another delivery. Errors passed to onError can include
// fixed sanitized failure categories, never raw driver messages.
func (q *RefreshQueue) Consume(ctx context.Context, handler func(context.Context, RefreshJob) error, onError func(error)) error {
	if handler == nil {
		return errors.New("refresh handler is required")
	}
	if !q.consuming.CompareAndSwap(false, true) {
		return errors.New("refresh queue already has a consumer")
	}
	defer q.consuming.Store(false)
	report := func(err error) {
		if onError != nil {
			onError(err)
		}
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		started := time.Now()
		pullCtx, pullCancel := context.WithTimeout(ctx, refreshPullWait)
		msg, err := q.consumer.Next(jetstream.FetchContext(pullCtx))
		pullCancel()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if errors.Is(err, nats.ErrConnectionClosed) || errors.Is(err, jetstream.ErrConnectionClosed) {
				return errors.New("refresh queue connection closed")
			}
			if !errors.Is(err, nats.ErrTimeout) && !errors.Is(err, jetstream.ErrNoMessages) && !errors.Is(err, context.DeadlineExceeded) {
				report(errors.New("refresh delivery failed"))
			}
			// Some server errors and empty responses can return immediately.
			// Pace every unsuccessful pull to avoid a tight retry loop.
			if err := waitRefreshRetry(ctx, refreshPullWait-time.Since(started)); err != nil {
				return err
			}
			continue
		}
		if ctx.Err() != nil {
			return ctx.Err() // leave the delivery unacknowledged for another worker
		}
		job, err := decodeRefreshJob(msg.Data())
		if err != nil {
			report(err)
			if err := msg.Term(); err != nil {
				report(errors.New("refresh message termination failed"))
			}
			continue
		}
		if err := q.processRefresh(ctx, msg, job, handler, refreshHeartbeat); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			report(err)
		}
	}
}

func waitRefreshRetry(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func processRefresh(ctx context.Context, msg jetstream.Msg, job RefreshJob, handler func(context.Context, RefreshJob) error, heartbeat time.Duration) error {
	return (&RefreshQueue{}).processRefresh(ctx, msg, job, handler, heartbeat)
}

func (q *RefreshQueue) processRefresh(ctx context.Context, msg jetstream.Msg, job RefreshJob, handler func(context.Context, RefreshJob) error, heartbeat time.Duration) error {
	metadata, err := msg.Metadata()
	if err != nil || metadata == nil || metadata.NumDelivered == 0 {
		return errors.New("refresh delivery metadata unavailable")
	}
	status, revision, err := q.loadStatus(ctx, job)
	if err != nil {
		return err
	}
	if status.ResetThroughSequence != 0 && metadata.Sequence.Stream <= status.ResetThroughSequence {
		// An explicit reset authorizes future scheduled jobs, never an old
		// exhausted delivery still pending broker acknowledgement.
		if msg.Term() != nil {
			return errors.New("refresh message termination failed")
		}
		return nil
	}
	if status.stopped() {
		if msg.Term() != nil {
			return errors.New("refresh message termination failed")
		}
		return &RefreshFailure{Category: status.Category, Permanent: status.State == "permanent"}
	}
	if metadata.NumDelivered > refreshMaxAttempts {
		terminal, err := q.recordFailure(ctx, job, &RefreshFailure{Category: "delivery_limit"}, metadata.NumDelivered, metadata.Sequence.Stream, 0)
		if err != nil {
			return err
		}
		if !terminal.stopped() || msg.Term() != nil {
			return errors.New("refresh message termination failed")
		}
		return &RefreshFailure{Category: "delivery_limit"}
	}
	if delay := time.Until(status.NextRetryAt); delay > 0 {
		if delay > refreshMaxRetryDelay {
			delay = refreshMaxRetryDelay
		}
		if msg.NakWithDelay(delay) != nil {
			return errors.New("refresh retry acknowledgement failed")
		}
		return nil
	}

	work, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- handler(work, job) }()
	ticker := time.NewTicker(heartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			cancel()
			<-done
			return ctx.Err()
		case <-ticker.C:
			if msg.InProgress() != nil {
				cancel()
				<-done
				// Do not ACK a delivery whose lease could not be renewed, even
				// if the handler finished successfully while being cancelled.
				return errors.New("refresh heartbeat failed")
			}
		case err := <-done:
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if err != nil {
				failure := classifyRefreshFailure(err)
				attempt := status.Attempts + 1
				if metadata.NumDelivered > attempt {
					attempt = metadata.NumDelivered
				}
				delay, allowed := refreshBackoff(attempt, err)
				if !allowed {
					failure = &RefreshFailure{Category: "retry_after", Permanent: true}
				}
				updated, persistErr := q.recordFailure(ctx, job, failure, metadata.NumDelivered, metadata.Sequence.Stream, delay)
				if persistErr != nil {
					return persistErr
				} // no ACK before durable state
				if updated.ResetThroughSequence != 0 && metadata.Sequence.Stream <= updated.ResetThroughSequence {
					if msg.Term() != nil {
						return errors.New("refresh message termination failed")
					}
					return nil
				}
				if updated.stopped() {
					if msg.Term() != nil {
						return errors.New("refresh message termination failed")
					}
				} else if msg.NakWithDelay(delay) != nil {
					return errors.New("refresh retry acknowledgement failed")
				}
				return failure
			}
			if q.status != nil {
				// Clear only the failure revision observed before this work.
				// A concurrent failure/reset must never be overwritten.
				status.State, status.Category, status.Attempts = "ready", "", 0
				status.NextRetryAt = time.Time{}
				status.UpdatedAt = time.Now().UTC()
				status.LastSuccessAt = status.UpdatedAt
				raw, marshalErr := json.Marshal(status)
				if marshalErr != nil {
					return errors.New("refresh status invalid")
				}
				var persistErr error
				if revision == 0 {
					_, persistErr = q.status.Create(ctx, refreshStatusKey(job), raw)
				} else {
					_, persistErr = q.status.Update(ctx, refreshStatusKey(job), raw, revision)
				}
				if persistErr != nil {
					return errors.New("refresh success status update conflicted or unavailable")
				}
			}
			ackCtx, ackCancel := context.WithTimeout(ctx, 5*time.Second)
			defer ackCancel()
			if msg.DoubleAck(ackCtx) != nil {
				return errors.New("refresh acknowledgement failed")
			}
			return nil
		}
	}
}
