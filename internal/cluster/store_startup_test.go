// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type startupStream struct {
	jetstream.Stream
	info *jetstream.StreamInfo
	read func(context.Context) error
}

func (s startupStream) CachedInfo() *jetstream.StreamInfo { return s.info }
func (s startupStream) Info(ctx context.Context, _ ...jetstream.StreamInfoOpt) (*jetstream.StreamInfo, error) {
	if s.read != nil {
		if err := s.read(ctx); err != nil {
			return nil, err
		}
	}
	return s.info, nil
}

type startupJS struct {
	jetstream.JetStream
	read    func(context.Context) error
	info    *jetstream.StreamInfo
	updates atomic.Int32
}

func (s *startupJS) Stream(ctx context.Context, _ string) (jetstream.Stream, error) {
	if s.read != nil {
		if err := s.read(ctx); err != nil {
			return nil, err
		}
	}
	return startupStream{info: s.info, read: func(context.Context) error { panic("redundant metadata request") }}, nil
}
func (s *startupJS) UpdateStream(_ context.Context, cfg jetstream.StreamConfig) (jetstream.Stream, error) {
	if cfg.AllowDirect {
		panic("direct reads retained")
	}
	s.updates.Add(1)
	return startupStream{info: &jetstream.StreamInfo{Config: cfg}}, nil
}

type startupKV struct {
	jetstream.KeyValue
	cfg jetstream.KeyValueConfig
}
type startupStatus struct {
	jetstream.KeyValueStatus
	cfg jetstream.KeyValueConfig
}

func (s startupStatus) Config() jetstream.KeyValueConfig { return s.cfg }
func (s startupKV) Status(context.Context) (jetstream.KeyValueStatus, error) {
	return startupStatus{cfg: s.cfg}, nil
}

type startupConsumer struct {
	jetstream.Consumer
	cfg  jetstream.ConsumerConfig
	read func(context.Context) error
}

func (s startupConsumer) Info(ctx context.Context) (*jetstream.ConsumerInfo, error) {
	if s.read != nil {
		if err := s.read(ctx); err != nil {
			return nil, err
		}
	}
	return &jetstream.ConsumerInfo{Config: s.cfg}, nil
}

func TestStartupDirectChecksUseFreshLookup(t *testing.T) {
	for _, init := range []bool{false, true} {
		for _, direct := range []bool{false, true} {
			js := &startupJS{info: &jetstream.StreamInfo{Config: jetstream.StreamConfig{AllowDirect: direct}}}
			err := configureKVDirect(context.Background(), js, metaBucket, init)
			if (err != nil) != (direct && !init) {
				t.Fatalf("init=%v direct=%v: %v", init, direct, err)
			}
			want := int32(0)
			if init && direct {
				want = 1
			}
			if js.updates.Load() != want {
				t.Fatal("unexpected hardening mutation")
			}
		}
	}
	if err := configureKVDirect(context.Background(), &startupJS{}, metaBucket, false); err == nil {
		t.Fatal("missing metadata accepted")
	}
}

func TestStartupChecksOverlapAndFailClosed(t *testing.T) {
	for _, failure := range []string{"", "metadata", "jobs", "stream", "consumer", "cancel"} {
		t.Run(failure, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			started := make(chan struct{}, 4)
			release := make(chan struct{})
			read := func(ctx context.Context) error {
				started <- struct{}{}
				select {
				case <-release:
					return ctx.Err()
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			p := Policy{Replicas: 3, MaxQueries: 2, JobTTL: time.Minute, LeaseDuration: 30 * time.Second, Workers: map[string]int{"a": 1}}
			js := &startupJS{read: read, info: &jetstream.StreamInfo{}}
			meta := startupKV{cfg: jetstream.KeyValueConfig{History: 1, Replicas: p.Replicas}}
			jobs := startupKV{cfg: jetstream.KeyValueConfig{History: 1, Replicas: p.Replicas, TTL: 2 * p.JobTTL, MaxValueSize: jobValueLimit, MaxBytes: jobStoreBytes(p)}}
			stream := startupStream{read: read, info: &jetstream.StreamInfo{Config: jetstream.StreamConfig{Retention: jetstream.WorkQueuePolicy, MaxMsgs: int64(p.MaxQueries), MaxMsgSize: 1024, MaxAge: p.JobTTL + time.Minute, Replicas: p.Replicas, Subjects: []string{queueSubject}}}}
			consumer := startupConsumer{read: read, cfg: jetstream.ConsumerConfig{Durable: "dispatch", AckPolicy: jetstream.AckExplicitPolicy, AckWait: p.LeaseDuration, MaxAckPending: workerCapacity(p), MaxRequestBatch: 1, MaxRequestExpires: time.Second}}
			switch failure {
			case "metadata":
				meta.cfg.History++
			case "jobs":
				jobs.cfg.MaxBytes++
			case "stream":
				stream.info.Config.AllowDirect = true
			case "consumer":
				consumer.cfg.MaxAckPending++
			}
			done := make(chan error, 1)
			go func() { done <- validateResources(ctx, js, meta, jobs, stream, consumer, p) }()
			for range 4 {
				select {
				case <-started:
				case <-ctx.Done():
					t.Fatal("independent resource checks did not overlap")
				}
			}
			select {
			case <-done:
				t.Fatal("startup returned before validations finished")
			default:
			}
			if failure == "cancel" {
				cancel()
			}
			close(release)
			err := <-done
			if (err != nil) != (failure != "") {
				t.Fatalf("failure=%q err=%v", failure, err)
			}
			if js.updates.Load() != 0 {
				t.Fatal("read-only validation changed resource configuration")
			}
		})
	}
}

func TestStartupDirectLookupFailureIsSanitized(t *testing.T) {
	js := &startupJS{read: func(context.Context) error { return errors.New("private server response") }}
	if err := configureKVDirect(context.Background(), js, jobsBucket, false); err == nil || err.Error() != "KV store configuration unavailable" {
		t.Fatalf("unexpected error: %v", err)
	}
}
