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
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"net/url"
	"os"
	"reflect"
	"strconv"
	"strings"
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
		return nil, errors.New("NATS connection failed")
	}
	fail := func(e error) (*NATSStore, error) { nc.Close(); return nil, e }
	js, err := jetstream.New(nc)
	if err != nil {
		return fail(errors.New("JetStream unavailable"))
	}
	raw, _ := json.Marshal(p)
	meta, err := js.KeyValue(ctx, metaBucket)
	if err != nil {
		if !initialize {
			return fail(errors.New("cluster metadata is missing"))
		}
		meta, err = js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: metaBucket, History: 1, MaxBytes: 1 << 20, Replicas: p.Replicas})
		if err != nil {
			return fail(errors.New("cluster metadata unavailable"))
		}
	}
	// A non-initializer must reject direct KV reads before it can read metadata.
	// The initializer verifies existing metadata first, then is the only caller
	// allowed to make the compatible direct-access hardening update.
	if !initialize {
		if err := configureKVDirect(ctx, js, metaBucket, false); err != nil {
			return fail(err)
		}
	}
	entry, err := meta.Get(ctx, metadataKey)
	missingMetadata := errors.Is(err, jetstream.ErrKeyNotFound)
	if err != nil && !missingMetadata {
		return fail(errors.New("cluster metadata unavailable"))
	}
	if !missingMetadata && string(entry.Value()) != string(raw) {
		return fail(errors.New("cluster metadata mismatch"))
	}
	if missingMetadata && !initialize {
		return fail(errors.New("cluster metadata is missing"))
	}
	if initialize {
		if err := configureKVDirect(ctx, js, metaBucket, true); err != nil {
			return fail(err)
		}
		meta, err = js.KeyValue(ctx, metaBucket) // refresh cached direct-read behavior
		if err != nil {
			return fail(errors.New("cluster metadata unavailable"))
		}
		if missingMetadata {
			if _, err = meta.Create(ctx, metadataKey, raw); err != nil {
				return fail(errors.New("cluster metadata unavailable"))
			}
		}
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
	return &NATSStore{p, nc, js, kv, co}, nil
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
	info, err := stream.Info(ctx)
	if err != nil {
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
	if err := configureKVDirect(ctx, js, metaBucket, false); err != nil {
		return errors.New("metadata store configuration mismatch")
	}
	if err := configureKVDirect(ctx, js, jobsBucket, false); err != nil {
		return errors.New("job store configuration mismatch")
	}
	metaStatus, err := meta.Status(ctx)
	if err != nil || metaStatus.Config().TTL != 0 || metaStatus.Config().History != 1 || metaStatus.Config().Replicas != p.Replicas {
		return errors.New("metadata store configuration mismatch")
	}
	jobStatus, err := jobs.Status(ctx)
	if err != nil || jobStatus.Config().TTL != 2*p.JobTTL || jobStatus.Config().History != 1 || jobStatus.Config().Replicas != p.Replicas || jobStatus.Config().MaxValueSize != jobValueLimit || jobStatus.Config().MaxBytes != jobStoreBytes(p) {
		return errors.New("job store configuration mismatch")
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return errors.New("dispatch stream configuration unavailable")
	}
	cfg := info.Config
	if cfg.Retention != jetstream.WorkQueuePolicy || cfg.MaxMsgs != int64(p.MaxQueries) || cfg.MaxMsgSize != 1024 || cfg.MaxAge != p.JobTTL+time.Minute || cfg.Replicas != p.Replicas || cfg.AllowDirect || len(cfg.Subjects) != 1 || cfg.Subjects[0] != queueSubject {
		return errors.New("dispatch stream configuration mismatch")
	}
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
func (s *NATSStore) Policy() Policy { return s.policy }
func (s *NATSStore) Close() error   { s.nc.Close(); return nil }
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
func enc(v any) []byte          { b, _ := json.Marshal(v); return b }
func dec(b []byte) (Job, error) { var j Job; e := json.Unmarshal(b, &j); return j, e }
func encodeJob(j Job) ([]byte, error) {
	b, err := json.Marshal(j)
	if err != nil || len(b) > jobValueLimit {
		return nil, errors.New("query state exceeds its size limit")
	}
	return b, nil
}
func (s *NATSStore) Submit(ctx context.Context, r query.Request) (Snapshot, error) {
	if err := query.ValidateRequest(r); err != nil {
		return Snapshot{}, err
	}
	now := time.Now().UTC()
	for n := 0; n < s.policy.MaxQueries; n++ {
		key := fmt.Sprintf("slot.%x", n)
		id, err := newID(n)
		if err != nil {
			return Snapshot{}, errors.New("query ID generation failed")
		}
		j := Job{ID: id, TenantID: s.policy.TenantID, State: Queued, Request: r, CreatedAt: now, ExpiresAt: now.Add(s.policy.JobTTL), HeartbeatAt: now}
		b, err := encodeJob(j)
		if err != nil {
			return Snapshot{}, err
		}
		rev, e := s.kv.Create(ctx, key, b)
		if e == nil {
			return Snapshot{j, rev}, nil
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
		if !prev.Terminal() && !now.After(prev.ExpiresAt) {
			continue
		}
		rev, e = s.kv.Update(ctx, key, b, old.Revision())
		if e == nil {
			return Snapshot{j, rev}, nil
		}
		if !errors.Is(e, jetstream.ErrKeyRevisionMismatch) {
			return Snapshot{}, errors.New("job store unavailable")
		}
	}
	return Snapshot{}, ErrCapacity
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
	return Snapshot{j, e.Revision()}, nil
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
	cur, err := s.Get(ctx, old.Job.ID)
	if err != nil {
		return Snapshot{}, err
	}
	if cur.Revision != old.Revision || cur.Job.Terminal() || !validTransition(cur.Job.State, next.State) ||
		next.ID != cur.Job.ID || next.TenantID != cur.Job.TenantID ||
		!next.CreatedAt.Equal(cur.Job.CreatedAt) || !next.ExpiresAt.Equal(cur.Job.ExpiresAt) ||
		!reflect.DeepEqual(next.Request, cur.Job.Request) {
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
	next.HeartbeatAt = time.Now().UTC()
	b, err := encodeJob(next)
	if err != nil {
		return Snapshot{}, ErrConflict
	}
	n, _ := slot(next.ID)
	rev, err := s.kv.Update(ctx, fmt.Sprintf("slot.%x", n), b, old.Revision)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
			return Snapshot{}, ErrConflict
		}
		return Snapshot{}, errors.New("job store unavailable")
	}
	return Snapshot{Job: next, Revision: rev}, nil
}
func (s *NATSStore) Enqueue(ctx context.Context, id string) error {
	j, err := s.Get(ctx, id)
	if err != nil {
		return err
	}
	if j.Job.State != Queued {
		return ErrConflict
	}
	if _, err = s.js.Publish(ctx, queueSubject, []byte(id), jetstream.WithMsgID(id)); err != nil {
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
			if _, er = s.CompareAndSwap(ctx, Snapshot{Job: eJob(e), Revision: e.Revision()}, j); er != nil && !errors.Is(er, ErrConflict) {
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
	if !clusterID.MatchString(id) || s.policy.Workers[id] < 1 || !validOwner(owner) {
		return ErrConflict
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
				return ErrConflict
			}
		}
		return errors.New("worker store unavailable")
	}
	var prior workerLease
	if json.Unmarshal(entry.Value(), &prior) != nil {
		return errors.New("worker store unavailable")
	}
	expired := now.Sub(prior.HeartbeatAt) > s.policy.LeaseDuration
	if prior.Owner != owner && (!claim || !expired) {
		return ErrConflict
	}
	if prior.Owner == owner && expired && !claim {
		return ErrConflict
	}
	if _, err = s.kv.Update(ctx, key, enc(workerLease{owner, now}), entry.Revision()); err != nil {
		if errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
			return ErrConflict
		}
		return errors.New("worker store unavailable")
	}
	return nil
}
