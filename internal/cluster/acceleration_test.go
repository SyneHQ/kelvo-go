// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func refreshTestJob() RefreshJob {
	return RefreshJob{Dataset: "daily_orders", Fingerprint: strings.Repeat("a1", 32)}
}

func TestRefreshResourcePolicy(t *testing.T) {
	p := testPolicy()
	stream := refreshStreamConfig(p)
	if err := validateRefreshStream(stream, p); err != nil {
		t.Fatal(err)
	}
	stream.Metadata["_nats.ver"] = "server-owned"
	if err := validateRefreshStream(stream, p); err != nil {
		t.Fatalf("server metadata rejected: %v", err)
	}
	for name, change := range map[string]func(*jetstream.StreamConfig){
		"tenant":           func(c *jetstream.StreamConfig) { c.Metadata["kelvo.tenant"] = "other" },
		"wildcard subject": func(c *jetstream.StreamConfig) { c.Subjects = []string{"acceleration.>"} },
		"extra subject":    func(c *jetstream.StreamConfig) { c.Subjects = append(c.Subjects, "other") },
		"message cap":      func(c *jetstream.StreamConfig) { c.MaxMsgs++ },
		"byte cap":         func(c *jetstream.StreamConfig) { c.MaxBytes++ },
		"message size":     func(c *jetstream.StreamConfig) { c.MaxMsgSize++ },
		"age":              func(c *jetstream.StreamConfig) { c.MaxAge++ },
		"replicas":         func(c *jetstream.StreamConfig) { c.Replicas = 3 },
		"memory storage":   func(c *jetstream.StreamConfig) { c.Storage = jetstream.MemoryStorage },
		"retention":        func(c *jetstream.StreamConfig) { c.Retention = jetstream.LimitsPolicy },
		"eviction":         func(c *jetstream.StreamConfig) { c.Discard = jetstream.DiscardOld },
		"extra consumer":   func(c *jetstream.StreamConfig) { c.MaxConsumers++ },
		"direct reads":     func(c *jetstream.StreamConfig) { c.AllowDirect = true },
		"rollup":           func(c *jetstream.StreamConfig) { c.AllowRollup = true },
		"mirror":           func(c *jetstream.StreamConfig) { c.Mirror = &jetstream.StreamSource{Name: "other"} },
		"republish": func(c *jetstream.StreamConfig) {
			c.RePublish = &jetstream.RePublish{Source: ">", Destination: "public"}
		},
		"dedup window":   func(c *jetstream.StreamConfig) { c.Duplicates++ },
		"no publish ack": func(c *jetstream.StreamConfig) { c.NoAck = true },
	} {
		t.Run("stream/"+name, func(t *testing.T) {
			cfg := refreshStreamConfig(p)
			change(&cfg)
			if validateRefreshStream(cfg, p) == nil {
				t.Fatal("accepted drifted stream")
			}
		})
	}
	if err := validateRefreshConsumer(refreshConsumerConfig(p), p); err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*jetstream.ConsumerConfig){
		"durable":                 func(c *jetstream.ConsumerConfig) { c.Durable = "other" },
		"ack policy":              func(c *jetstream.ConsumerConfig) { c.AckPolicy = jetstream.AckNonePolicy },
		"ack wait":                func(c *jetstream.ConsumerConfig) { c.AckWait++ },
		"pending cap":             func(c *jetstream.ConsumerConfig) { c.MaxAckPending++ },
		"batch cap":               func(c *jetstream.ConsumerConfig) { c.MaxRequestBatch++ },
		"pull expiry":             func(c *jetstream.ConsumerConfig) { c.MaxRequestExpires++ },
		"subject":                 func(c *jetstream.ConsumerConfig) { c.FilterSubject = "other" },
		"skip backlog":            func(c *jetstream.ConsumerConfig) { c.DeliverPolicy = jetstream.DeliverNewPolicy },
		"finite redelivery":       func(c *jetstream.ConsumerConfig) { c.MaxDeliver = 1 },
		"backoff overrides lease": func(c *jetstream.ConsumerConfig) { c.BackOff = []time.Duration{time.Minute} },
		"push delivery":           func(c *jetstream.ConsumerConfig) { c.DeliverSubject = "public" },
		"headers only":            func(c *jetstream.ConsumerConfig) { c.HeadersOnly = true },
		"ephemeral retention":     func(c *jetstream.ConsumerConfig) { c.InactiveThreshold = time.Second },
		"memory state":            func(c *jetstream.ConsumerConfig) { c.MemoryStorage = true },
	} {
		t.Run("consumer/"+name, func(t *testing.T) {
			cfg := refreshConsumerConfig(p)
			change(&cfg)
			if validateRefreshConsumer(cfg, p) == nil {
				t.Fatal("accepted drifted consumer")
			}
		})
	}
}

func TestDecodeRefreshJob(t *testing.T) {
	valid, _ := json.Marshal(refreshTestJob())
	job, err := decodeRefreshJob(valid)
	if err != nil || job != refreshTestJob() {
		t.Fatalf("valid job: %#v, %v", job, err)
	}
	for _, data := range []string{
		"", "null", "[]", "{}", string(valid) + "{}", string(valid)[:len(valid)-1],
		`{"dataset":"daily_orders","fingerprint":null}`,
		`{"dataset":5,"fingerprint":"` + strings.Repeat("a", 64) + `"}`,
		`{"dataset":"daily_orders","dataset":"other","fingerprint":"` + strings.Repeat("a", 64) + `"}`,
		`{"dataset":"daily_orders","fingerprint":"` + strings.Repeat("a", 64) + `","sql":"SELECT secret"}`,
		`{"dataset":"daily-orders","fingerprint":"` + strings.Repeat("a", 64) + `"}`,
		`{"dataset":"daily_orders","fingerprint":"` + strings.Repeat("A", 64) + `"}`,
		`{"dataset":"daily_orders","fingerprint":"` + strings.Repeat("g", 64) + `"}`,
		`{"dataset":"daily_orders","fingerprint":"` + strings.Repeat("a", 63) + `"}`,
		string(valid) + strings.Repeat(" ", refreshMessageLimit),
	} {
		if _, err := decodeRefreshJob([]byte(data)); err == nil {
			t.Errorf("accepted malformed refresh payload: %q", data)
		}
	}
}

type refreshTestPublisher struct {
	calls int
	err   error
	data  []byte
	name  string
}

func (p *refreshTestPublisher) Publish(_ context.Context, name string, data []byte, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	p.calls++
	p.name, p.data = name, data
	return &jetstream.PubAck{Stream: refreshQueueStream}, p.err
}

func TestRefreshPublishValidation(t *testing.T) {
	publisher := &refreshTestPublisher{}
	queue := &RefreshQueue{js: publisher}
	ctx := context.Background()
	if err := queue.Publish(ctx, refreshTestJob(), "daily_orders:1"); err != nil {
		t.Fatal(err)
	}
	if publisher.calls != 1 || publisher.name != refreshQueueSubject {
		t.Fatalf("unexpected publication: %+v", publisher)
	}
	if job, err := decodeRefreshJob(publisher.data); err != nil || job != refreshTestJob() {
		t.Fatalf("published invalid payload: %+v, %v", job, err)
	}
	for _, id := range []string{"", "contains secret", "header\r\ninjection", strings.Repeat("a", 257)} {
		if queue.Publish(ctx, refreshTestJob(), id) == nil {
			t.Errorf("accepted invalid dedup ID: %q", id)
		}
	}
	invalid := refreshTestJob()
	invalid.Dataset = "orders; SELECT secret"
	if queue.Publish(ctx, invalid, "id") == nil || publisher.calls != 1 {
		t.Fatal("invalid publication reached NATS")
	}
	publisher.err = errors.New("server detail including credentials")
	if err := queue.Publish(ctx, refreshTestJob(), "id"); err == nil || strings.Contains(err.Error(), "credentials") {
		t.Fatalf("publication error was absent or leaked details: %v", err)
	}
}

type refreshTestMessage struct {
	jetstream.Msg
	data           []byte
	acks           atomic.Int32
	naks           atomic.Int32
	terms          atomic.Int32
	heartbeats     atomic.Int32
	delay          time.Duration
	progress       func() error
	delivered      uint64
	sentAt         time.Time
	streamSequence uint64
}

func (m *refreshTestMessage) Metadata() (*jetstream.MsgMetadata, error) {
	n := m.delivered
	if n == 0 {
		n = 1
	}
	stamp := m.sentAt
	if stamp.IsZero() {
		stamp = time.Now().UTC()
	}
	seq := m.streamSequence
	if seq == 0 {
		seq = 100
	}
	return &jetstream.MsgMetadata{NumDelivered: n, Timestamp: stamp, Sequence: jetstream.SequencePair{Stream: seq}}, nil
}

func (m *refreshTestMessage) Data() []byte                    { return m.data }
func (m *refreshTestMessage) DoubleAck(context.Context) error { m.acks.Add(1); return nil }
func (m *refreshTestMessage) NakWithDelay(delay time.Duration) error {
	m.delay = delay
	m.naks.Add(1)
	return nil
}
func (m *refreshTestMessage) Term() error { m.terms.Add(1); return nil }
func (m *refreshTestMessage) InProgress() error {
	m.heartbeats.Add(1)
	if m.progress != nil {
		return m.progress()
	}
	return nil
}

func TestRefreshAckAfterHandlerAndRetry(t *testing.T) {
	for _, fail := range []bool{false, true} {
		msg := &refreshTestMessage{}
		handlerErr := errors.New("retry source")
		err := processRefresh(context.Background(), msg, refreshTestJob(), func(context.Context, RefreshJob) error {
			if msg.acks.Load() != 0 || msg.naks.Load() != 0 {
				t.Error("delivery was acknowledged before handler completion")
			}
			if fail {
				return handlerErr
			}
			return nil
		}, time.Second)
		if fail {
			if err == nil || strings.Contains(err.Error(), handlerErr.Error()) || msg.acks.Load() != 0 || msg.naks.Load() != 1 || msg.delay < 5*time.Second || msg.delay > 10*time.Second {
				t.Fatalf("failed handler was not retried: ack=%d nak=%d delay=%s err=%v", msg.acks.Load(), msg.naks.Load(), msg.delay, err)
			}
		} else if err != nil || msg.acks.Load() != 1 || msg.naks.Load() != 0 {
			t.Fatalf("successful handler was not acknowledged: %v", err)
		}
	}
}

func TestRefreshHeartbeatFailureWaitsForHandler(t *testing.T) {
	msg := &refreshTestMessage{progress: func() error { return errors.New("lease connection lost") }}
	cancelled, release, finished := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		finished <- processRefresh(context.Background(), msg, refreshTestJob(), func(ctx context.Context, _ RefreshJob) error {
			<-ctx.Done()
			close(cancelled)
			<-release
			return nil // success concurrent with lease loss still must not ACK
		}, time.Millisecond)
	}()
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("failed heartbeat did not cancel handler")
	}
	select {
	case <-finished:
		t.Fatal("queue continued before the cancelled handler released its lock")
	default:
	}
	close(release)
	if err := <-finished; err == nil || msg.acks.Load() != 0 || msg.naks.Load() != 0 {
		t.Fatalf("lost lease was acknowledged or not reported: %v", err)
	}
}

func TestRefreshCancellationWaitsForHandler(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	msg := &refreshTestMessage{}
	started, cancelled, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		finished <- processRefresh(ctx, msg, refreshTestJob(), func(work context.Context, _ RefreshJob) error {
			close(started)
			<-work.Done()
			close(cancelled)
			<-release
			return work.Err()
		}, time.Second)
	}()
	<-started
	cancel()
	<-cancelled
	select {
	case <-finished:
		t.Fatal("shutdown returned before its handler stopped")
	default:
	}
	close(release)
	if err := <-finished; !errors.Is(err, context.Canceled) || msg.acks.Load() != 0 || msg.naks.Load() != 0 {
		t.Fatalf("cancelled work was acknowledged: %v", err)
	}
}

type refreshTestConsumer struct {
	next func() (jetstream.Msg, error)
}

func (c refreshTestConsumer) Next(...jetstream.FetchOpt) (jetstream.Msg, error) { return c.next() }

func TestRefreshMalformedMessageTerminated(t *testing.T) {
	msg := &refreshTestMessage{data: []byte(`{"sql":"SELECT secret"}`)}
	calls, handlerCalls := 0, 0
	queue := &RefreshQueue{consumer: refreshTestConsumer{next: func() (jetstream.Msg, error) {
		calls++
		if calls == 1 {
			return msg, nil
		}
		return nil, nats.ErrConnectionClosed
	}}}
	_ = queue.Consume(context.Background(), func(context.Context, RefreshJob) error { handlerCalls++; return nil }, nil)
	if handlerCalls != 0 || msg.terms.Load() != 1 || msg.acks.Load() != 0 || msg.naks.Load() != 0 {
		t.Fatal("malformed message was delivered or not terminated")
	}
}

func TestRefreshEmptyResponsesDoNotSpin(t *testing.T) {
	calls := 0
	queue := &RefreshQueue{consumer: refreshTestConsumer{next: func() (jetstream.Msg, error) {
		calls++
		return nil, jetstream.ErrNoMessages
	}}}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	err := queue.Consume(ctx, func(context.Context, RefreshJob) error { return nil }, nil)
	if !errors.Is(err, context.DeadlineExceeded) || calls != 1 {
		t.Fatalf("empty pull spun or ignored cancellation: calls=%d err=%v", calls, err)
	}
}

func TestNATSRefreshQueueProvisionDedupRetry(t *testing.T) {
	store := openFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := store.js.Stream(ctx, refreshQueueStream); err == nil {
		t.Skip("refresh integration fixture must not already contain a refresh queue")
	} else if !errors.Is(err, jetstream.ErrStreamNotFound) {
		t.Fatal(err)
	}
	if _, err := store.OpenRefreshQueue(ctx, false); err == nil {
		t.Fatal("non-initializer created missing refresh resources")
	}
	t.Cleanup(func() {
		cleanup, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		if err := store.js.DeleteStream(cleanup, refreshQueueStream); err != nil && !errors.Is(err, jetstream.ErrStreamNotFound) {
			t.Errorf("remove refresh test stream: %v", err)
		}
	})
	queue, err := store.OpenRefreshQueue(ctx, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.OpenRefreshQueue(ctx, false); err != nil {
		t.Fatalf("non-initializer rejected provisioned queue: %v", err)
	}
	for range 2 {
		if err := queue.Publish(ctx, refreshTestJob(), "refresh-fixture-dedup"); err != nil {
			t.Fatal(err)
		}
	}
	stream, err := store.js.Stream(ctx, refreshQueueStream)
	if err != nil {
		t.Fatal(err)
	}
	info, err := stream.Info(ctx)
	if err != nil || info.State.Msgs != 1 {
		t.Fatalf("message-ID deduplication failed: %+v, %v", info, err)
	}
	if _, err = store.js.Publish(ctx, refreshQueueSubject, []byte(`{"sql":"SELECT secret"}`)); err != nil {
		t.Fatal(err)
	}
	var attempts atomic.Int32
	done := make(chan error, 1)
	go func() {
		done <- queue.Consume(ctx, func(_ context.Context, job RefreshJob) error {
			if job != refreshTestJob() {
				return errors.New("unexpected fixture refresh job")
			}
			if attempts.Add(1) == 1 {
				return errors.New("fixture transient failure")
			}
			return nil
		}, nil)
	}()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			<-done
			t.Fatal("refresh retry or acknowledgement did not complete")
		case <-ticker.C:
			info, err := stream.Info(ctx)
			if err != nil {
				cancel()
				<-done
				t.Fatal(err)
			}
			if attempts.Load() == 2 && info.State.Msgs == 0 {
				cancel()
				if err := <-done; !errors.Is(err, context.Canceled) {
					t.Fatalf("consumer shutdown: %v", err)
				}
				return
			}
		}
	}
}
