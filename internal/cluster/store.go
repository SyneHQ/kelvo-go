// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/tracing"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"net/url"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const jobsBucket = "KELVO_JOBS"
const metaBucket = "KELVO_META"
const queueStream = "KELVO_QUEUE"
const queueSubject = "job.ready"
const metadataKey = "meta.config"
const jobValueLimit = 512 << 10

type NATSStore struct {
	policy   Policy
	nc       *nats.Conn
	js       jetstream.JetStream
	kv       jetstream.KeyValue
	consumer jetstream.Consumer

	// cursor rotates where Submit starts probing, so concurrent submissions do
	// not all walk the same occupied slots. freed holds slots this process saw
	// a transient job finish in, with the revision to replace them at.
	cursor  atomic.Uint64
	freedMu sync.Mutex
	freed   []freedSlot
}

type freedSlot struct {
	n   int
	rev uint64
}

const maxFreedSlots = 256

// A terminal job's slot is handed out again only when nothing else is free
// until the policy's lease duration has passed since its last transition. The
// client confirms a job's state after it has read the result stream; a slot
// reused inside that window turns a completed query into NOT_FOUND. Expired
// jobs are always reusable. When every slot is busy or fresh, the freshest
// terminal slot is reused rather than refusing the submission, so capacity
// stays what max_queries says; size max_queries so that saturation is rare.
func (s *NATSStore) slotReusable(j Job, now time.Time, strict bool) bool {
	if now.After(j.ExpiresAt) || !j.Terminal() {
		return now.After(j.ExpiresAt)
	}
	if j.Transient {
		// Its caller read the outcome from the response itself, so a completed
		// read's slot is free at once. A cancelled or failed one is not: its
		// node may still be executing until it next renews its lease (up to
		// about a second), and a slot reused now would let that work run outside
		// max_queries. Those wait out the lease window, in the saturated pass
		// too: a transient job that did not complete is never taken early.
		if j.State == Succeeded {
			return true
		}
		return now.Sub(j.HeartbeatAt) >= s.policy.LeaseDuration
	}
	return !strict || now.Sub(j.HeartbeatAt) >= s.policy.LeaseDuration
}

type workerLease struct {
	Owner       string    `json:"owner"`
	HeartbeatAt time.Time `json:"heartbeat_at"`
}
type delivery struct {
	id  string
	msg jetstream.Msg
}

func (d delivery) ID() string { return d.id }
func (d delivery) Ack(c context.Context) error {
	if err := c.Err(); err != nil {
		return err
	}
	return d.msg.DoubleAck(c)
}
func (d delivery) Retry(c context.Context) error {
	if err := c.Err(); err != nil {
		return err
	}
	return d.msg.Nak()
}
func OpenStore(parent context.Context, c NATSConfig, p Policy, initialize bool) (*NATSStore, error) {
	p, err := clonePolicy(p)
	if err != nil {
		return nil, errors.New("invalid cluster policy")
	}
	if err := ValidatePolicy(p); err != nil {
		return nil, errors.New("invalid cluster policy")
	}
	u, err := url.Parse(c.URL)
	if err != nil || u.Scheme != "tls" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Host == "" {
		return nil, errors.New("invalid NATS URL")
	}
	if c.CAFile == "" {
		return nil, errors.New("NATS TLS CA is required")
	}
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	roots := x509.NewCertPool()
	b, err := os.ReadFile(c.CAFile)
	if err != nil || !roots.AppendCertsFromPEM(b) {
		return nil, errors.New("invalid NATS CA")
	}
	tc := &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS13, ServerName: u.Hostname()}
	if (c.CertFile == "") != (c.KeyFile == "") {
		return nil, errors.New("incomplete NATS client certificate")
	}
	if c.CertFile != "" {
		cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
		if err != nil {
			return nil, errors.New("invalid NATS client certificate")
		}
		tc.Certificates = []tls.Certificate{cert}
	}
	opts := []nats.Option{nats.Secure(tc), nats.Timeout(5 * time.Second)}
	if c.CredentialsFile != "" {
		opts = append(opts, nats.UserCredentials(c.CredentialsFile))
	} else {
		pw := os.Getenv(c.PasswordEnv)
		if c.Username == "" || len(pw) < 32 {
			return nil, errors.New("NATS credentials are invalid")
		}
		opts = append(opts, nats.UserInfo(c.Username, pw))
	}
	nc, err := nats.Connect(c.URL, opts...)
	if err != nil {
		return nil, coordinationUnavailable("nats_connect", err)
	}
	fail := func(e error) (*NATSStore, error) { nc.Close(); return nil, e }
	js, err := jetstream.New(nc)
	if err != nil {
		return fail(errors.New("JetStream unavailable"))
	}
	meta, err := openClusterMetadata(ctx, js, p, initialize)
	if err != nil {
		return fail(err)
	}
	kv, err := js.KeyValue(ctx, jobsBucket)
	if err != nil {
		if !initialize {
			return fail(errors.New("job store is missing"))
		}
		kv, err = js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: jobsBucket, History: 1, TTL: 2 * p.JobTTL, MaxValueSize: jobValueLimit, MaxBytes: jobStoreBytes(p), Replicas: p.Replicas})
		if err != nil {
			return fail(errors.New("job store unavailable"))
		}
	}
	if err := configureKVDirect(ctx, js, jobsBucket, initialize); err != nil {
		return fail(err)
	}
	if initialize {
		kv, err = js.KeyValue(ctx, jobsBucket) // refresh cached direct-read behavior
		if err != nil {
			return fail(errors.New("job store unavailable"))
		}
	}
	st, err := js.Stream(ctx, queueStream)
	if err != nil {
		if !initialize {
			return fail(errors.New("dispatch stream is missing"))
		}
		st, err = js.CreateStream(ctx, jetstream.StreamConfig{Name: queueStream, Subjects: []string{queueSubject}, Retention: jetstream.WorkQueuePolicy, MaxMsgs: int64(p.MaxQueries), MaxMsgSize: 1024, MaxAge: p.JobTTL + time.Minute, Replicas: p.Replicas})
		if err != nil {
			return fail(errors.New("dispatch stream unavailable"))
		}
	}
	co, err := st.Consumer(ctx, "dispatch")
	if err != nil {
		if !initialize {
			return fail(errors.New("dispatch consumer is missing"))
		}
		co, err = st.CreateConsumer(ctx, jetstream.ConsumerConfig{Durable: "dispatch", AckPolicy: jetstream.AckExplicitPolicy, AckWait: p.LeaseDuration, MaxAckPending: workerCapacity(p), MaxRequestBatch: 1, MaxRequestExpires: time.Second})
		if err != nil {
			return fail(errors.New("dispatch consumer unavailable"))
		}
	}
	if err := validateResources(ctx, js, meta, kv, st, co, p); err != nil {
		return fail(err)
	}
	return &NATSStore{policy: p, nc: nc, js: js, kv: kv, consumer: co}, nil
}

func workerCapacity(p Policy) int {
	total := 0
	for _, capacity := range p.Workers {
		total += capacity
	}
	return total
}

func jobStoreBytes(p Policy) int64 {
	return int64(p.MaxQueries+workerCapacity(p)) * jobValueLimit
}

func configureKVDirect(ctx context.Context, js jetstream.JetStream, bucket string, initialize bool) error {
	stream, err := js.Stream(ctx, "KV_"+bucket)
	if err != nil {
		return errors.New("KV store configuration unavailable")
	}
	// Stream just fetched its metadata. Do not issue the same request twice;
	// this object is local to this validation and is never reused across opens.
	info := stream.CachedInfo()
	if info == nil {
		return errors.New("KV store configuration unavailable")
	}
	if !info.Config.AllowDirect {
		return nil
	}
	if !initialize {
		return errors.New("KV store direct access is forbidden")
	}
	config := info.Config
	config.AllowDirect = false
	if _, err = js.UpdateStream(ctx, config); err != nil {
		return errors.New("KV store configuration unavailable")
	}
	return nil
}

func validateResources(ctx context.Context, js jetstream.JetStream, meta, jobs jetstream.KeyValue, stream jetstream.Stream, consumer jetstream.Consumer, p Policy) error {
	// These four read-only checks are independent. Keep startup within the
	// existing deadline over WAN without skipping any immutable-policy checks.
	checks := [...]func() error{
		func() error { return validateMetadataResource(ctx, js, meta, p) },
		func() error { return validateJobsResource(ctx, js, jobs, p) },
		func() error { return validateDispatchStream(ctx, stream, p) },
		func() error { return validateDispatchConsumer(ctx, consumer, p) },
	}
	var results [len(checks)]error
	var pending sync.WaitGroup
	for i, check := range checks {
		pending.Go(func() { results[i] = check() })
	}
	pending.Wait()
	// Deterministic error precedence, after every bounded request has returned.
	for _, err := range results {
		if err != nil {
			return err
		}
	}
	return ctx.Err()
}

func validateMetadataResource(ctx context.Context, js jetstream.JetStream, meta jetstream.KeyValue, p Policy) error {
	if err := configureKVDirect(ctx, js, metaBucket, false); err != nil {
		return errors.New("metadata store configuration mismatch")
	}
	metaStatus, err := meta.Status(ctx)
	if err != nil || metaStatus.Config().TTL != 0 || metaStatus.Config().History != 1 || metaStatus.Config().Replicas != p.Replicas {
		return errors.New("metadata store configuration mismatch")
	}
	return nil
}

func validateJobsResource(ctx context.Context, js jetstream.JetStream, jobs jetstream.KeyValue, p Policy) error {
	if err := configureKVDirect(ctx, js, jobsBucket, false); err != nil {
		return errors.New("job store configuration mismatch")
	}
	jobStatus, err := jobs.Status(ctx)
	if err != nil || jobStatus.Config().TTL != 2*p.JobTTL || jobStatus.Config().History != 1 || jobStatus.Config().Replicas != p.Replicas || jobStatus.Config().MaxValueSize != jobValueLimit || jobStatus.Config().MaxBytes != jobStoreBytes(p) {
		return errors.New("job store configuration mismatch")
	}
	return nil
}

func validateDispatchStream(ctx context.Context, stream jetstream.Stream, p Policy) error {
	info, err := stream.Info(ctx)
	if err != nil {
		return errors.New("dispatch stream configuration unavailable")
	}
	cfg := info.Config
	if cfg.Retention != jetstream.WorkQueuePolicy || cfg.MaxMsgs != int64(p.MaxQueries) || cfg.MaxMsgSize != 1024 || cfg.MaxAge != p.JobTTL+time.Minute || cfg.Replicas != p.Replicas || cfg.AllowDirect || len(cfg.Subjects) != 1 || cfg.Subjects[0] != queueSubject {
		return errors.New("dispatch stream configuration mismatch")
	}
	return nil
}

func validateDispatchConsumer(ctx context.Context, consumer jetstream.Consumer, p Policy) error {
	ci, err := consumer.Info(ctx)
	if err != nil {
		return errors.New("dispatch consumer configuration unavailable")
	}
	cc := ci.Config
	if cc.Durable != "dispatch" || cc.AckPolicy != jetstream.AckExplicitPolicy || cc.AckWait != p.LeaseDuration || cc.MaxAckPending != workerCapacity(p) || cc.MaxRequestBatch != 1 || cc.MaxRequestExpires != time.Second {
		return errors.New("dispatch consumer configuration mismatch")
	}
	return nil
}
func (s *NATSStore) Policy() Policy {
	// The stored graph was validated and detached before broker setup, so its
	// row-policy clone cannot fail during normal operation.
	p, _ := clonePolicy(s.policy)
	return p
}
func (s *NATSStore) Close() error { s.nc.Close(); return nil }
func slot(id string) (int, bool) {
	a := strings.Split(id, "-")
	if len(a) != 2 || len(a[0]) == 0 || len(a[1]) != 32 {
		return 0, false
	}
	n, e := strconv.ParseInt(a[0], 16, 32)
	if e != nil || strings.ToLower(a[0]) != a[0] || fmt.Sprintf("%x", n) != a[0] {
		return 0, false
	}
	if strings.ToLower(a[1]) != a[1] {
		return 0, false
	}
	if _, e := hex.DecodeString(a[1]); e != nil {
		return 0, false
	}
	return int(n), true
}
func newID(n int) (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x-%s", n, hex.EncodeToString(b)), nil
}
func enc(v any) []byte { b, _ := json.Marshal(v); return b }
func dec(b []byte) (Job, error) {
	var j Job
	err := json.Unmarshal(b, &j)
	j.Trace = copyJobTrace(j.Trace)
	return j, err
}
func encodeJob(j Job) ([]byte, error) { return encodePersistedJob(&j) }

// Optional diagnostics never consume the space required for authoritative state.
// Update the snapshot too, so callers see exactly what was persisted.
func encodePersistedJob(j *Job) ([]byte, error) {
	j.Trace = copyJobTrace(j.Trace)
	b, err := json.Marshal(j)
	if err == nil && len(b) > jobValueLimit && j.Trace != nil {
		j.Trace = nil
		b, err = json.Marshal(j)
	}
	if err != nil || len(b) > jobValueLimit {
		return nil, errors.New("query state exceeds its size limit")
	}
	return b, nil
}
func (s *NATSStore) Submit(ctx context.Context, r query.Request) (Snapshot, error) {
	return s.submit(ctx, r, false)
}

// SubmitTransient stores a single-call read. It differs from Submit only in
// the slot's lifetime: the job is marked Transient, so once it has Succeeded
// its slot is reused at once instead of after the lease window. A cancelled or
// failed one keeps the lease window.
func (s *NATSStore) SubmitTransient(ctx context.Context, r query.Request) (Snapshot, error) {
	return s.submit(ctx, r, true)
}

func (s *NATSStore) submit(ctx context.Context, r query.Request, transient bool) (Snapshot, error) {
	if err := query.ValidateRequest(r); err != nil {
		return Snapshot{}, err
	}
	authority, err := submissionAuthority(ctx, s.policy, r)
	if err != nil {
		return Snapshot{}, err
	}
	now := time.Now().UTC()
	newJob := func(n int) (Job, []byte, error) {
		id, err := newID(n)
		if err != nil {
			return Job{}, nil, errors.New("query ID generation failed")
		}
		j := Job{Trace: copyJobTraceValue(tracing.CarrierFromContext(ctx)), Authority: authority, ID: id, TenantID: s.policy.TenantID, State: Queued, Request: r, CreatedAt: now, ExpiresAt: now.Add(s.policy.JobTTL), HeartbeatAt: now, Transient: transient}
		b, err := encodePersistedJob(&j)
		return j, b, err
	}
	// A slot this process just saw a transient job finish in is replaced at the
	// revision it was seen at: one round trip, no scan. A changed slot fails
	// the revision check and falls through to the scan.
	for {
		hint, ok := s.popFreed()
		if !ok {
			break
		}
		j, b, err := newJob(hint.n)
		if err != nil {
			return Snapshot{}, err
		}
		rev, e := s.kv.Update(ctx, fmt.Sprintf("slot.%x", hint.n), b, hint.rev)
		if e == nil {
			return Snapshot{Job: j, Revision: rev, raw: b}, nil
		}
		if !errors.Is(e, jetstream.ErrKeyRevisionMismatch) && !errors.Is(e, jetstream.ErrKeyNotFound) {
			return Snapshot{}, errors.New("job store unavailable")
		}
	}
	slots := s.policy.MaxQueries
	start := int(s.cursor.Add(1) % uint64(slots))
	for _, strict := range []bool{true, false} {
		for i := 0; i < slots; i++ {
			n := (start + i) % slots
			key := fmt.Sprintf("slot.%x", n)
			j, b, err := newJob(n)
			if err != nil {
				return Snapshot{}, err
			}
			rev, e := s.kv.Create(ctx, key, b)
			if e == nil {
				return Snapshot{Job: j, Revision: rev, raw: b}, nil
			}
			old, getErr := s.kv.Get(ctx, key)
			if getErr != nil {
				if errors.Is(getErr, jetstream.ErrKeyNotFound) {
					continue
				}
				return Snapshot{}, errors.New("job store unavailable")
			}
			prev, e := dec(old.Value())
			if e != nil {
				return Snapshot{}, errors.New("job store unavailable")
			}
			if !s.slotReusable(prev, now, strict) {
				continue
			}
			rev, e = s.kv.Update(ctx, key, b, old.Revision())
			if e == nil {
				return Snapshot{Job: j, Revision: rev, raw: b}, nil
			}
			if !errors.Is(e, jetstream.ErrKeyRevisionMismatch) {
				return Snapshot{}, errors.New("job store unavailable")
			}
		}
	}
	return Snapshot{}, ErrCapacity
}

func (s *NATSStore) pushFreed(n int, rev uint64) {
	s.freedMu.Lock()
	defer s.freedMu.Unlock()
	if len(s.freed) < maxFreedSlots {
		s.freed = append(s.freed, freedSlot{n, rev})
	}
}

func (s *NATSStore) popFreed() (freedSlot, bool) {
	s.freedMu.Lock()
	defer s.freedMu.Unlock()
	if len(s.freed) == 0 {
		return freedSlot{}, false
	}
	h := s.freed[len(s.freed)-1]
	s.freed = s.freed[:len(s.freed)-1]
	return h, true
}
func (s *NATSStore) Get(ctx context.Context, id string) (Snapshot, error) {
	n, ok := slot(id)
	if !ok || n < 0 || n >= s.policy.MaxQueries {
		return Snapshot{}, ErrNotFound
	}
	e, er := s.kv.Get(ctx, fmt.Sprintf("slot.%x", n))
	if er != nil {
		if errors.Is(er, jetstream.ErrKeyNotFound) {
			return Snapshot{}, ErrNotFound
		}
		return Snapshot{}, errors.New("job store unavailable")
	}
	j, er := dec(e.Value())
	if er != nil {
		return Snapshot{}, errors.New("job store unavailable")
	}
	if j.ID != id || j.TenantID != s.policy.TenantID {
		return Snapshot{}, ErrNotFound
	}
	return Snapshot{Job: j, Revision: e.Revision(), raw: e.Value()}, nil
}
func validTransition(a, b string) bool {
	if a == b {
		return a == Queued || a == Assigned || a == Claimed || a == Running || a == ResultReady
	}
	switch a {
	case Queued:
		return b == Assigned || b == Cancelled || b == Failed
	case Assigned:
		return b == Claimed || b == Cancelled || b == Failed
	case Claimed:
		return b == Running || b == Cancelled || b == Failed
	case Running:
		return b == ResultReady || b == Cancelled || b == Failed
	case ResultReady:
		return b == Succeeded || b == Cancelled || b == Failed
	}
	return false
}
func validToken(token string) bool {
	if len(token) != 32 || strings.ToLower(token) != token {
		return false
	}
	_, err := hex.DecodeString(token)
	return err == nil
}

func validClaim(claim string) bool { return validToken(claim) }
func validOwner(owner string) bool { return validToken(owner) }

func sameResult(a, b Job) bool {
	return reflect.DeepEqual(a.Stats, b.Stats) && reflect.DeepEqual(a.Error, b.Error)
}

func (s *NATSStore) CompareAndSwap(ctx context.Context, old Snapshot, next Job) (Snapshot, error) {
	// A snapshot the store produced carries the record it read at old.Revision,
	// and the update below succeeds only if the slot is still at that revision,
	// so the record it replaces is exactly that one. Reading it again first, as
	// this used to, cost a round trip per transition for nothing.
	n, ok := slot(old.Job.ID)
	if !ok || n < 0 || n >= s.policy.MaxQueries || old.Job.TenantID != s.policy.TenantID {
		return Snapshot{}, ErrNotFound
	}
	cur := old
	if old.raw != nil {
		j, err := dec(old.raw)
		if err != nil {
			return Snapshot{}, errors.New("job store unavailable")
		}
		cur.Job = j
	} else if fresh, err := s.Get(ctx, old.Job.ID); err != nil {
		return Snapshot{}, err
	} else {
		cur = fresh
	}
	if cur.Revision != old.Revision || cur.Job.Terminal() || !validTransition(cur.Job.State, next.State) ||
		next.ID != cur.Job.ID || next.TenantID != cur.Job.TenantID || next.Transient != cur.Job.Transient ||
		!next.CreatedAt.Equal(cur.Job.CreatedAt) || !next.ExpiresAt.Equal(cur.Job.ExpiresAt) ||
		!reflect.DeepEqual(next.Request, cur.Job.Request) || !sameAuthority(next.Authority, cur.Job.Authority) {
		return Snapshot{}, ErrConflict
	}
	if cur.Job.State == Queued {
		if next.State == Assigned && (!clusterID.MatchString(next.WorkerID) || s.policy.Workers[next.WorkerID] < 1 || !validOwner(next.Owner) || next.Claim != "") {
			return Snapshot{}, ErrConflict
		}
		if next.State != Assigned && (next.WorkerID != "" || next.Owner != "" || next.Claim != "") {
			return Snapshot{}, ErrConflict
		}
	} else if next.WorkerID != cur.Job.WorkerID || next.Owner != cur.Job.Owner {
		return Snapshot{}, ErrConflict
	}
	if cur.Job.State == Assigned {
		if next.State == Claimed && !validClaim(next.Claim) {
			return Snapshot{}, ErrConflict
		}
		if next.State != Claimed && next.Claim != "" {
			return Snapshot{}, ErrConflict
		}
	} else if next.Claim != cur.Job.Claim {
		return Snapshot{}, ErrConflict
	}
	// Workers may publish final statistics only when they atomically make the
	// result durable. Ready-state heartbeats and the gateway's terminal commit
	// keep the recorded result immutable.
	if cur.Job.State == Running && next.State == ResultReady {
		if !reflect.DeepEqual(next.Error, cur.Job.Error) {
			return Snapshot{}, ErrConflict
		}
	} else if !next.Terminal() || (cur.Job.State == ResultReady && next.State == Succeeded) {
		if !sameResult(next, cur.Job) {
			return Snapshot{}, ErrConflict
		}
	}
	// Re-read durable context, ignoring attempted replacement or pointer mutation.
	next.Trace = copyJobTrace(cur.Job.Trace)
	next.HeartbeatAt = time.Now().UTC()
	b, err := encodePersistedJob(&next)
	if err != nil {
		return Snapshot{}, ErrConflict
	}
	rev, err := s.kv.Update(ctx, fmt.Sprintf("slot.%x", n), b, old.Revision)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
			return Snapshot{}, ErrConflict
		}
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return Snapshot{}, ErrNotFound
		}
		return Snapshot{}, errors.New("job store unavailable")
	}
	if next.Transient && next.State == Succeeded {
		s.pushFreed(n, rev) // only a completed read frees its slot at once
	}
	return Snapshot{Job: next, Revision: rev, raw: b}, nil
}
func (s *NATSStore) Enqueue(ctx context.Context, id string) error {
	j, err := s.Get(ctx, id)
	if err != nil {
		return err
	}
	if j.Job.State != Queued {
		return ErrConflict
	}
	return s.publishReady(ctx, id)
}

// EnqueueSubmitted queues a job Submit just returned, without reading it back:
// it is Queued, and the message ID makes a repeat harmless.
func (s *NATSStore) EnqueueSubmitted(ctx context.Context, snap Snapshot) error {
	if snap.Job.State != Queued {
		return ErrConflict
	}
	return s.publishReady(ctx, snap.Job.ID)
}

func (s *NATSStore) publishReady(ctx context.Context, id string) error {
	if _, err := s.js.Publish(ctx, queueSubject, []byte(id), jetstream.WithMsgID(id)); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return errors.New("dispatch unavailable")
	}
	return nil
}

func (s *NATSStore) Next(ctx context.Context) (Delivery, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	wait := time.Second
	if deadline, ok := ctx.Deadline(); ok {
		if remaining := time.Until(deadline); remaining < wait {
			wait = remaining
		}
	}
	if wait <= 0 {
		return nil, ctx.Err()
	}
	b, err := s.consumer.Fetch(1, jetstream.FetchMaxWait(wait))
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, errors.New("dispatch unavailable")
	}
	for m := range b.Messages() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		id := string(m.Data())
		if _, ok := slot(id); !ok {
			_ = m.Ack()
			return nil, errors.New("dispatch message is invalid")
		}
		return delivery{id, m}, nil
	}
	if err = b.Error(); err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, errors.New("dispatch unavailable")
	}
	return nil, ErrNoJob
}
func (s *NATSStore) Reconcile(ctx context.Context) error {
	now := time.Now().UTC()
	for n := 0; n < s.policy.MaxQueries; n++ {
		e, er := s.kv.Get(ctx, fmt.Sprintf("slot.%x", n))
		if er != nil {
			if errors.Is(er, jetstream.ErrKeyNotFound) {
				continue
			}
			return errors.New("job store unavailable")
		}
		j, er := dec(e.Value())
		if er != nil {
			return errors.New("job store unavailable")
		}
		if !j.Terminal() && (now.After(j.ExpiresAt) || (j.State != Queued && now.Sub(j.HeartbeatAt) > s.policy.LeaseDuration)) {
			j.State = Failed
			j.Error = query.PublicError(query.NewError("UNAVAILABLE", "Worker lost"))
			if _, er = s.CompareAndSwap(ctx, Snapshot{Job: eJob(e), Revision: e.Revision(), raw: e.Value()}, j); er != nil && !errors.Is(er, ErrConflict) {
				return er
			}
			continue
		}
		if j.State == Queued {
			if er = s.Enqueue(ctx, j.ID); er != nil && !errors.Is(er, ErrConflict) {
				return er
			}
		}
	}
	return nil
}
func eJob(e jetstream.KeyValueEntry) Job { j, _ := dec(e.Value()); return j }
func (s *NATSStore) ClaimWorker(ctx context.Context, id, owner string) error {
	return s.worker(ctx, id, owner, true)
}
func (s *NATSStore) HeartbeatWorker(ctx context.Context, id, owner string) error {
	return s.worker(ctx, id, owner, false)
}
func (s *NATSStore) worker(ctx context.Context, id, owner string, claim bool) error {
	stage := "worker_renew"
	if claim {
		stage = "worker_claim"
	}
	if !clusterID.MatchString(id) || s.policy.Workers[id] < 1 || !validOwner(owner) {
		return coordinationFailure(stage, "invalid_identity")
	}
	key := "worker." + id
	now := time.Now().UTC()
	entry, err := s.kv.Get(ctx, key)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) && claim {
			if _, err = s.kv.Create(ctx, key, enc(workerLease{owner, now})); err == nil {
				return nil
			}
			if errors.Is(err, jetstream.ErrKeyExists) {
				return coordinationFailure(stage+"_write", "revision_conflict")
			}
			return coordinationUnavailable(stage+"_write", err)
		}
		return coordinationUnavailable(stage+"_read", err)
	}
	var prior workerLease
	if json.Unmarshal(entry.Value(), &prior) != nil {
		return coordinationFailure(stage+"_read", "invalid_lease")
	}
	expired := now.Sub(prior.HeartbeatAt) > s.policy.LeaseDuration
	if prior.Owner != owner && (!claim || !expired) {
		return coordinationFailure(stage, "owner_conflict")
	}
	if prior.Owner == owner && expired && !claim {
		return coordinationFailure(stage, "lease_expired")
	}
	if _, err = s.kv.Update(ctx, key, enc(workerLease{owner, now}), entry.Revision()); err != nil {
		if errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
			return coordinationFailure(stage+"_write", "revision_conflict")
		}
		return coordinationUnavailable(stage+"_write", err)
	}
	return nil
}
