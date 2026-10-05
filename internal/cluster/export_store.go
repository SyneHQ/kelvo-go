// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	exportJobsBucket    = "KELVO_EXPORT_JOBS"
	exportQueueStream   = "KELVO_EXPORT_QUEUE"
	exportQueueSubject  = "export.ready"
	exportQueueConsumer = "exports"
)

type exportKV interface {
	Get(context.Context, string) (jetstream.KeyValueEntry, error)
	Create(context.Context, string, []byte, ...jetstream.KVCreateOpt) (uint64, error)
	Update(context.Context, string, []byte, uint64) (uint64, error)
}

type natsExportStore struct {
	base     *NATSStore
	policy   Policy
	kv       exportKV
	consumer jetstream.Consumer
	now      func() time.Time
}

type exportDelivery struct {
	delivery
	retryDelay time.Duration
}

func (d exportDelivery) Retry(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d.retryDelay <= 0 || d.retryDelay > 250*time.Millisecond {
		return errExportInvalid
	}
	return d.msg.NakWithDelay(d.retryDelay)
}

func exportStreamConfig(p Policy) jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name: exportQueueStream, Subjects: []string{exportQueueSubject},
		Retention: jetstream.WorkQueuePolicy, Storage: jetstream.FileStorage,
		MaxConsumers: 1, MaxMsgs: int64(p.Exports.MaxJobs), MaxBytes: int64(p.Exports.MaxJobs) * 1024,
		MaxMsgSize: 1024, MaxMsgsPerSubject: -1, MaxAge: p.Exports.QueueTimeout + time.Minute,
		Replicas: p.Replicas, Discard: jetstream.DiscardNew, Duplicates: time.Minute,
		Metadata: map[string]string{"kelvo.tenant": p.TenantID},
	}
}

func exportConsumerConfig(p Policy) jetstream.ConsumerConfig {
	return jetstream.ConsumerConfig{
		Name: exportQueueConsumer, Durable: exportQueueConsumer, FilterSubject: exportQueueSubject,
		DeliverPolicy: jetstream.DeliverAllPolicy, AckPolicy: jetstream.AckExplicitPolicy,
		AckWait: p.LeaseDuration, MaxDeliver: -1, MaxWaiting: 64,
		MaxAckPending: workerCapacity(p), MaxRequestBatch: 1, MaxRequestExpires: time.Second, Replicas: p.Replicas,
	}
}

func exportKVConfig(p Policy) jetstream.KeyValueConfig {
	return jetstream.KeyValueConfig{Bucket: exportJobsBucket, History: 1, TTL: 2 * p.Exports.MaxTTL,
		MaxValueSize: exportJobValueLimit, MaxBytes: int64(p.Exports.MaxJobs) * exportJobValueLimit, Replicas: p.Replicas, Storage: jetstream.FileStorage}
}

func validateExportConsumerConfig(got jetstream.ConsumerConfig, p Policy) error {
	// Newer brokers annotate consumers as well as streams. Ignore only their
	// reserved metadata; every operator-controlled field still matches exactly.
	got.Metadata = refreshUserMetadata(got.Metadata)
	if !reflect.DeepEqual(got, exportConsumerConfig(p)) {
		return errors.New("export consumer configuration mismatch")
	}
	return nil
}

// OpenExportStore uses the already authenticated, policy-checked parent store.
// It creates only absent resources with initialization authority; existing queue
// policy is never silently changed. Closing the parent closes this store too.
func OpenExportStore(parent context.Context, base *NATSStore, initialize bool) (ExportStore, error) {
	if base == nil || base.js == nil || base.kv == nil {
		return nil, errExportInvalid
	}
	p, err := clonePolicy(base.policy)
	if err != nil {
		return nil, errExportInvalid
	}
	if p.Exports == nil {
		return nil, ErrExportDisabled
	}
	if ValidatePolicy(p) != nil {
		return nil, errExportInvalid
	}
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()
	kv, err := base.js.KeyValue(ctx, exportJobsBucket)
	if errors.Is(err, jetstream.ErrBucketNotFound) && initialize {
		kv, err = base.js.CreateKeyValue(ctx, exportKVConfig(p))
		if err != nil {
			kv, err = base.js.KeyValue(ctx, exportJobsBucket)
		}
	}
	if err != nil {
		return nil, errors.New("export store unavailable; initialize tenant exports first")
	}
	if err = configureKVDirect(ctx, base.js, exportJobsBucket, initialize); err != nil {
		return nil, errors.New("export store configuration mismatch")
	}
	kv, err = base.js.KeyValue(ctx, exportJobsBucket)
	if err != nil {
		return nil, errors.New("export store unavailable")
	}
	status, err := kv.Status(ctx)
	want := exportKVConfig(p)
	if err != nil {
		return nil, errors.New("export store configuration unavailable")
	}
	got := status.Config()
	if got.History != want.History || got.TTL != want.TTL || got.MaxValueSize != want.MaxValueSize || got.MaxBytes != want.MaxBytes || got.Replicas != want.Replicas || got.Storage != want.Storage {
		return nil, errors.New("export store configuration mismatch")
	}
	stream, err := base.js.Stream(ctx, exportQueueStream)
	if errors.Is(err, jetstream.ErrStreamNotFound) && initialize {
		stream, err = base.js.CreateStream(ctx, exportStreamConfig(p))
		if err != nil {
			stream, err = base.js.Stream(ctx, exportQueueStream)
		}
	}
	if err != nil {
		return nil, errors.New("export queue unavailable; initialize tenant exports first")
	}
	si, err := stream.Info(ctx)
	if err != nil {
		return nil, errors.New("export queue configuration unavailable")
	}
	si.Config.Metadata = refreshUserMetadata(si.Config.Metadata)
	if !reflect.DeepEqual(si.Config, exportStreamConfig(p)) {
		return nil, errors.New("export queue configuration mismatch")
	}
	consumer, err := stream.Consumer(ctx, exportQueueConsumer)
	if errors.Is(err, jetstream.ErrConsumerNotFound) && initialize {
		consumer, err = stream.CreateConsumer(ctx, exportConsumerConfig(p))
		if err != nil {
			consumer, err = stream.Consumer(ctx, exportQueueConsumer)
		}
	}
	if err != nil {
		return nil, errors.New("export consumer unavailable")
	}
	ci, err := consumer.Info(ctx)
	if err != nil || validateExportConsumerConfig(ci.Config, p) != nil {
		return nil, errors.New("export consumer configuration mismatch")
	}
	return &natsExportStore{base: base, policy: p, kv: kv, consumer: consumer, now: func() time.Time { return time.Now().UTC() }}, nil
}

func (s *natsExportStore) Policy() Policy {
	// Return an owned graph without JSON round-trip normalization.
	p, _ := clonePolicy(s.policy)
	return p
}

func exportKey(slot int) string { return fmt.Sprintf("export.%x", slot) }

func (s *natsExportStore) SubmitExport(ctx context.Context, in ExportSubmission) (ExportSnapshot, error) {
	now := s.now()
	j, err := normalizeExportSubmission(ctx, s.policy, in, now)
	if err != nil {
		return ExportSnapshot{}, err
	}
	for n := 0; n < s.policy.Exports.MaxJobs; n++ {
		id, err := newID(n)
		if err != nil {
			return ExportSnapshot{}, errors.New("export ID generation failed")
		}
		j.ID = "e" + id
		raw, err := encodeExportJob(s.policy, j)
		if err != nil {
			return ExportSnapshot{}, err
		}
		if err = requestAuthorityErr(ctx); err != nil {
			return ExportSnapshot{}, err
		}
		rev, err := s.kv.Create(ctx, exportKey(n), raw)
		if err == nil {
			copy, _ := decodeExportJob(s.policy, raw)
			return ExportSnapshot{Job: copy, Revision: rev}, nil
		}
		if !errors.Is(err, jetstream.ErrKeyExists) {
			return ExportSnapshot{}, errors.New("export admission unavailable")
		}
		entry, err := s.kv.Get(ctx, exportKey(n))
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return ExportSnapshot{}, errors.New("export admission unavailable")
		}
		old, err := decodeExportJob(s.policy, entry.Value())
		if err != nil {
			return ExportSnapshot{}, errors.New("export admission unavailable")
		}
		if now.Before(old.ExpiresAt) {
			continue
		}
		if err = requestAuthorityErr(ctx); err != nil {
			return ExportSnapshot{}, err
		}
		rev, err = s.kv.Update(ctx, exportKey(n), raw, entry.Revision())
		if err == nil {
			copy, _ := decodeExportJob(s.policy, raw)
			return ExportSnapshot{Job: copy, Revision: rev}, nil
		}
		if !errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
			return ExportSnapshot{}, errors.New("export admission unavailable")
		}
	}
	return ExportSnapshot{}, ErrExportCapacity
}

func (s *natsExportStore) GetExport(ctx context.Context, id string) (ExportSnapshot, error) {
	n, ok := exportSlot(id)
	if !ok || n < 0 || n >= s.policy.Exports.MaxJobs {
		return ExportSnapshot{}, ErrExportNotFound
	}
	entry, err := s.kv.Get(ctx, exportKey(n))
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return ExportSnapshot{}, ErrExportNotFound
	}
	if err != nil {
		return ExportSnapshot{}, errors.New("export store unavailable")
	}
	j, err := decodeExportJob(s.policy, entry.Value())
	if err != nil {
		return ExportSnapshot{}, errors.New("export store unavailable")
	}
	if j.ID != id {
		return ExportSnapshot{}, ErrExportNotFound
	}
	return ExportSnapshot{Job: j, Revision: entry.Revision()}, nil
}

func (s *natsExportStore) workerCurrent(ctx context.Context, j ExportJob, now time.Time) error {
	if s.base == nil || s.base.kv == nil {
		return ErrExportConflict
	}
	entry, err := s.base.kv.Get(ctx, "worker."+j.WorkerID)
	if err != nil {
		return errors.New("export worker lease unavailable")
	}
	var lease workerLease
	// A concurrent heartbeat can legitimately occur during the store read.
	// Compare with the clock after that read, not an older pre-request instant.
	now = s.now()
	if json.Unmarshal(entry.Value(), &lease) != nil || lease.Owner != j.WorkerOwner || lease.HeartbeatAt.After(now) || now.Sub(lease.HeartbeatAt) >= s.policy.LeaseDuration {
		return ErrExportConflict
	}
	return nil
}

func (s *natsExportStore) CompareAndSwapExport(ctx context.Context, old ExportSnapshot, next ExportJob) (ExportSnapshot, error) {
	cur, err := s.GetExport(ctx, old.Job.ID)
	if err != nil {
		return ExportSnapshot{}, err
	}
	if cur.Revision != old.Revision {
		return ExportSnapshot{}, ErrExportConflict
	}
	requested := next
	now := s.now()
	next, role, err := exportTransition(s.policy, cur.Job, next, now)
	if err != nil {
		return ExportSnapshot{}, err
	}
	if role == exportSupervisorMutation {
		a, err := submissionAuthority(ctx, s.policy, cur.Job.Request)
		if err != nil || a == nil || *a != cur.Job.Authority.Principal {
			return ExportSnapshot{}, ErrExportConflict
		}
		if auth, ok := ctx.Value(keyAuthorizationContext{}).(keyAuthorization); ok && (auth.authenticator == nil || next.AuthorityUntil.After(auth.authenticator.expiry())) {
			return ExportSnapshot{}, ErrExportConflict
		}
	}
	if role != exportStopMutation && next.WorkerID != "" {
		if err = s.workerCurrent(ctx, next, now); err != nil {
			return ExportSnapshot{}, err
		}
	}
	// Lease I/O may cross an authority, queue or execution deadline. Validate
	// again and stamp worker progress with the time after that I/O.
	next, _, err = exportTransition(s.policy, cur.Job, requested, s.now())
	if err != nil {
		return ExportSnapshot{}, err
	}
	raw, err := encodeExportJob(s.policy, next)
	if err != nil {
		return ExportSnapshot{}, ErrExportConflict
	}
	if err = requestAuthorityErr(ctx); err != nil {
		return ExportSnapshot{}, err
	}
	if role != exportStopMutation && !exportDeadlinesValid(cur.Job, s.now()) {
		return ExportSnapshot{}, ErrExportConflict
	}
	n, _ := exportSlot(next.ID)
	rev, err := s.kv.Update(ctx, exportKey(n), raw, cur.Revision)
	if errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
		return ExportSnapshot{}, ErrExportConflict
	}
	if err != nil {
		return ExportSnapshot{}, errors.New("export publication unavailable")
	}
	copy, _ := decodeExportJob(s.policy, raw)
	return ExportSnapshot{Job: copy, Revision: rev}, nil
}

func (s *natsExportStore) EnqueueExport(ctx context.Context, id string) error {
	snap, err := s.GetExport(ctx, id)
	if err != nil {
		return err
	}
	now := s.now()
	if snap.Job.State != ExportQueued || !now.Before(snap.Job.ExpiresAt) || !now.Before(snap.Job.QueueDeadline) || !now.Before(snap.Job.AuthorityUntil) {
		return ErrExportConflict
	}
	if _, err = s.base.js.Publish(ctx, exportQueueSubject, []byte(id), jetstream.WithMsgID(id), jetstream.WithExpectStream(exportQueueStream)); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return errors.New("export dispatch unavailable")
	}
	return nil
}

func (s *natsExportStore) NextExport(ctx context.Context) (Delivery, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	wait := time.Second
	if deadline, ok := ctx.Deadline(); ok {
		wait = min(wait, time.Until(deadline))
	}
	if wait <= 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		// The deadline callback may not have run yet. Never return a nil
		// delivery together with a nil error to the worker dispatcher.
		return nil, context.DeadlineExceeded
	}
	batch, err := s.consumer.Fetch(1, jetstream.FetchMaxWait(wait))
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, errors.New("export dispatch unavailable")
	}
	for msg := range batch.Messages() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		id := string(msg.Data())
		if n, ok := exportSlot(id); !ok || n < 0 || n >= s.policy.Exports.MaxJobs {
			_ = msg.Ack()
			return nil, errors.New("invalid export dispatch")
		}
		return exportDelivery{delivery: delivery{id, msg}, retryDelay: min(s.policy.LeaseDuration/3, 250*time.Millisecond)}, nil
	}
	if err = batch.Error(); err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, errors.New("export dispatch unavailable")
	}
	return nil, ErrNoExport
}

func (s *natsExportStore) ReconcileExports(ctx context.Context) error {
	for n := 0; n < s.policy.Exports.MaxJobs; n++ {
		entry, err := s.kv.Get(ctx, exportKey(n))
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return errors.New("export reconciliation unavailable")
		}
		j, err := decodeExportJob(s.policy, entry.Value())
		if err != nil {
			return errors.New("export reconciliation unavailable")
		}
		if !exportActive(j) {
			continue
		}
		now := s.now()
		lost := !exportDeadlinesValid(j, now) || (j.State != ExportQueued && (j.HeartbeatAt.After(now) || now.Sub(j.HeartbeatAt) >= s.policy.LeaseDuration))
		if lost {
			next := j
			next.State = ExportFailed
			next.Error = query.PublicError(query.NewError("UNAVAILABLE", "Export execution authority or worker expired"))
			_, err = s.CompareAndSwapExport(ctx, ExportSnapshot{Job: j, Revision: entry.Revision()}, next)
			if err != nil && !errors.Is(err, ErrExportConflict) {
				return err
			}
			continue
		}
		if j.State == ExportQueued {
			if err = s.EnqueueExport(ctx, j.ID); err != nil && !errors.Is(err, ErrExportConflict) {
				return err
			}
		}
	}
	return nil
}
