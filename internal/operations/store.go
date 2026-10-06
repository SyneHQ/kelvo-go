// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Package operations retains database operation custody and outcomes. Every
// admission and transition is one bounded shard CAS, never a cross-key promise.
package operations

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	api "github.com/SYNEHQ/kelvo-go/operations"
)

const MaxDocumentBytes = 512 << 10
const (
	Queued   = "queued"
	Assigned = "assigned"
	Running  = "running"
)

var (
	ErrNotFound    = errors.New("operation not found")
	ErrConflict    = errors.New("operation identity or custody conflict")
	ErrCapacity    = errors.New("retained operation shard capacity exhausted")
	ErrUnavailable = errors.New("operation custody storage unavailable")
	ErrInvalid     = errors.New("invalid operation custody contract")
	ErrMissing     = errors.New("operation storage key missing")
	ErrRevision    = errors.New("operation storage revision conflict")
)
var namespacePattern = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,63}$`)

type Entry struct {
	Value    []byte
	Revision uint64
}

// Backend errors must classify only a proven revision mismatch as ErrRevision.
// Unknown write acknowledgements return any other error, never an automatic
// retry. A source operation may start only after Start succeeds.
type Backend interface {
	Get(context.Context, string) (Entry, error)
	Create(context.Context, string, []byte) (uint64, error)
	Update(context.Context, string, []byte, uint64) (uint64, error)
}

type Policy struct {
	Namespace        string
	TenantID         string
	Shards           int
	SlotsPerShard    int
	Retention        time.Duration
	ExecutionTimeout time.Duration
	LeaseDuration    time.Duration
	StorageTimeout   time.Duration
	MaxCASAttempts   int
}

func (p Policy) Validate() error {
	if !namespacePattern.MatchString(p.Namespace) || !safeText(p.TenantID, 128) || p.Shards < 1 || p.Shards > 65536 || p.SlotsPerShard < 1 || p.SlotsPerShard > 8 ||
		p.ExecutionTimeout < time.Second || p.ExecutionTimeout > time.Hour || p.Retention <= p.ExecutionTimeout || p.Retention > 30*24*time.Hour ||
		p.LeaseDuration < time.Second || p.LeaseDuration > time.Minute || p.LeaseDuration > p.ExecutionTimeout || p.StorageTimeout <= 0 || p.StorageTimeout > 30*time.Second || p.MaxCASAttempts < 1 || p.MaxCASAttempts > 32 {
		return ErrInvalid
	}
	return nil
}

// Scope must come from verified current authority, never directly from a request
// body. A shared cluster tenant does not collapse application-team identities.
type Scope struct {
	Issuer           string `json:"issuer"`
	ClusterTenant    string `json:"cluster_tenant"`
	ServicePrincipal string `json:"service_principal"`
	AppTeam          string `json:"app_team"`
	SubjectKind      string `json:"subject_kind"`
	SubjectID        string `json:"subject_id"`
	SubjectJobID     string `json:"subject_job_id,omitempty"`
	ConnectionID     string `json:"connection_id"`
}

func (s Scope) Validate() error {
	for _, value := range []string{s.Issuer, s.ClusterTenant, s.ServicePrincipal, s.AppTeam, s.SubjectID, s.ConnectionID} {
		if !safeText(value, 256) {
			return ErrInvalid
		}
	}
	switch s.SubjectKind {
	case "user", "api_key", "admin":
		if s.SubjectJobID != "" {
			return ErrInvalid
		}
		return nil
	case "job":
		if !safeText(s.SubjectJobID, 256) {
			return ErrInvalid
		}
		return nil
	}
	return ErrInvalid
}

type Binding struct {
	WorkerID string `json:"worker_id"`
	Owner    string `json:"owner"`
	Claim    string `json:"claim"`
}

func (b Binding) Validate() error {
	if !api.ValidID(b.WorkerID) || !token(b.Owner) || !token(b.Claim) {
		return ErrInvalid
	}
	return nil
}

type Record struct {
	ID              string       `json:"id"`
	IdentitySHA256  string       `json:"identity_sha256"`
	Scope           Scope        `json:"scope"`
	Kind            api.Kind     `json:"kind"`
	RequestSHA256   string       `json:"request_sha256"`
	RequestRef      api.InputRef `json:"request_ref"`
	AuthoritySHA256 string       `json:"authority_sha256"`
	AuthorityToken  string       `json:"authority_token"`
	CreatedAt       time.Time    `json:"created_at"`
	ExecuteBefore   time.Time    `json:"execute_before"`
	AuthorityUntil  time.Time    `json:"authority_until"`
	RetainUntil     time.Time    `json:"retain_until"`
	State           string       `json:"state"`
	Binding         Binding      `json:"binding"`
	LeaseUntil      time.Time    `json:"lease_until,omitempty"`
	Receipt         *api.Receipt `json:"receipt,omitempty"`
}

func (r Record) Terminal() bool { return r.Receipt != nil }

type Snapshot struct {
	Record   Record
	Revision uint64
}

type Submission struct {
	Scope           Scope
	Request         api.Request
	RequestRef      api.InputRef
	AuthoritySHA256 string
	// AuthorityToken is the verified, credential-free operation grant. Signature
	// and live authorization checks belong to the trusted admission/runtime layer.
	AuthorityToken string
	AuthorityUntil time.Time
}

type document struct {
	Version      int      `json:"version"`
	PolicySHA256 string   `json:"policy_sha256"`
	Records      []Record `json:"records"`
}

type Store struct {
	backend      Backend
	policy       Policy
	policySHA256 string
	now          func() time.Time
}

func New(backend Backend, policy Policy) (*Store, error) {
	if backend == nil || policy.Validate() != nil {
		return nil, ErrInvalid
	}
	raw, _ := json.Marshal(policy)
	sum := sha256.Sum256(raw)
	return &Store{backend: backend, policy: policy, policySHA256: hex.EncodeToString(sum[:]), now: time.Now}, nil
}

func safeText(value string, limit int) bool {
	return value != "" && len(value) <= limit && utf8.ValidString(value) && !strings.ContainsAny(value, "\x00\r\n")
}
func token(value string) bool {
	if len(value) != 32 || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}
func randomToken() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", ErrUnavailable
	}
	return hex.EncodeToString(raw[:]), nil
}
func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}
func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}
func (s *Store) key(shard int) string {
	return fmt.Sprintf("operation.%s.%04x", s.policy.Namespace, shard)
}
func (s *Store) shard(id string) (int, error) {
	if len(id) != 37 || id[4] != '-' || !token(id[5:]) {
		return 0, ErrNotFound
	}
	n, err := strconv.ParseUint(id[:4], 16, 16)
	if err != nil || fmt.Sprintf("%04x", n) != id[:4] || int(n) >= s.policy.Shards {
		return 0, ErrNotFound
	}
	return int(n), nil
}
func (s *Store) validScope(scope Scope) bool {
	return scope.Validate() == nil && scope.ClusterTenant == s.policy.TenantID
}

// Submit atomically reserves retention and duplicate identity in one shard.
// The selected shard may be full while another is free; callers must not change
// idempotency keys to bypass a conflict or replay an uncertain operation.
func (s *Store) Submit(ctx context.Context, input Submission) (Snapshot, bool, error) {
	if !s.validScope(input.Scope) || input.Request.Validate() != nil || input.Scope.ConnectionID != input.Request.Connection.ID || !validAuthority(input.AuthorityToken, input.AuthoritySHA256) || input.RequestRef.Validate() != nil || input.RequestRef.Format != "operation_request_v1" {
		return Snapshot{}, false, ErrInvalid
	}
	expected, _, err := api.SealRequest(input.Request, input.RequestRef.ID)
	if err != nil || expected != input.RequestRef {
		return Snapshot{}, false, ErrInvalid
	}
	digest, err := api.Digest(input.Request)
	if err != nil {
		return Snapshot{}, false, ErrInvalid
	}
	key := input.Request.IdempotencyKey
	if key == "" {
		key, err = randomToken()
		if err != nil {
			return Snapshot{}, false, err
		}
	}
	identity, shard := s.identity(input.Scope, key)
	nonce, err := randomToken()
	if err != nil {
		return Snapshot{}, false, err
	}
	id := fmt.Sprintf("%04x-%s", shard, nonce)
	duplicate := false
	out, err := s.update(ctx, shard, true, func(d *document, now time.Time) (*Record, bool, error) {
		duplicate = false
		if !now.Before(input.AuthorityUntil) || input.AuthorityUntil.After(now.Add(330*time.Second)) {
			return nil, false, ErrConflict
		}
		for i := range d.Records {
			r := &d.Records[i]
			if r.IdentitySHA256 == identity {
				if r.Scope != input.Scope || r.RequestSHA256 != digest {
					return nil, false, ErrConflict
				}
				duplicate = true
				return r, false, nil
			}
		}
		retained := d.Records[:0]
		for _, r := range d.Records {
			if !r.Terminal() || now.Before(r.RetainUntil) {
				retained = append(retained, r)
			}
		}
		changed := len(retained) != len(d.Records)
		d.Records = retained
		if len(d.Records) >= s.policy.SlotsPerShard {
			return nil, changed, ErrCapacity
		}
		r := Record{ID: id, IdentitySHA256: identity, Scope: input.Scope, Kind: input.Request.Kind, RequestSHA256: digest, RequestRef: input.RequestRef, AuthoritySHA256: input.AuthoritySHA256, AuthorityToken: input.AuthorityToken,
			CreatedAt: now, ExecuteBefore: minTime(now.Add(s.policy.ExecutionTimeout), input.AuthorityUntil), AuthorityUntil: input.AuthorityUntil, RetainUntil: maxTime(now.Add(s.policy.Retention), input.AuthorityUntil.Add(30*time.Second)), State: Queued}
		d.Records = append(d.Records, r)
		// Reserve the worst-case terminal receipt before source execution. A
		// grant-heavy shard must reject admission, not lose a later outcome.
		reserved, marshalErr := json.Marshal(d)
		futureBytes := len(reserved)
		for _, record := range d.Records {
			if !record.Terminal() {
				futureBytes += api.MaxReceiptBytes + 128
			}
		}
		if marshalErr != nil || futureBytes > MaxDocumentBytes {
			d.Records = d.Records[:len(d.Records)-1]
			return nil, changed, ErrCapacity
		}
		return &d.Records[len(d.Records)-1], true, nil
	})
	return out, duplicate, err
}

func (s *Store) Get(ctx context.Context, scope Scope, id string) (Snapshot, error) {
	return s.change(ctx, scope, id, func(r *Record, now time.Time) (bool, error) { return false, nil })
}

func (s *Store) Claim(ctx context.Context, scope Scope, id string, binding Binding) (Snapshot, error) {
	if binding.Validate() != nil {
		return Snapshot{}, ErrInvalid
	}
	return s.change(ctx, scope, id, func(r *Record, now time.Time) (bool, error) {
		if r.State != Queued {
			return false, ErrConflict
		}
		r.State, r.Binding, r.LeaseUntil = Assigned, binding, minTime(now.Add(s.policy.LeaseDuration), r.ExecuteBefore)
		return true, nil
	})
}

// Start is the final durable barrier before source dispatch. Returning an error
// (including an unknown CAS acknowledgement) never authorizes execution.
func (s *Store) Start(ctx context.Context, scope Scope, id string, binding Binding) (Snapshot, error) {
	return s.changeBound(ctx, scope, id, binding, func(r *Record, now time.Time) (bool, error) {
		if r.State != Assigned {
			return false, ErrConflict
		}
		r.State = Running
		return true, nil
	})
}

func (s *Store) Renew(ctx context.Context, scope Scope, id string, binding Binding) (Snapshot, error) {
	return s.changeBound(ctx, scope, id, binding, func(r *Record, now time.Time) (bool, error) {
		if r.State != Assigned && r.State != Running {
			return false, ErrConflict
		}
		r.LeaseUntil = minTime(now.Add(s.policy.LeaseDuration), r.ExecuteBefore)
		return true, nil
	})
}

// Current verifies operation custody only. The caller must ALSO verify the
// independent current worker lease, trusted service identity and live grant.
func (s *Store) Current(ctx context.Context, scope Scope, id string, binding Binding) (Snapshot, error) {
	return s.changeBound(ctx, scope, id, binding, func(r *Record, now time.Time) (bool, error) {
		if r.State != Running {
			return false, ErrConflict
		}
		return false, nil
	})
}

func (s *Store) Complete(ctx context.Context, scope Scope, id string, binding Binding, receipt api.Receipt) (Snapshot, error) {
	if receipt.Validate() != nil {
		return Snapshot{}, ErrInvalid
	}
	return s.changeBound(ctx, scope, id, binding, func(r *Record, now time.Time) (bool, error) {
		if receipt.OperationID != r.ID || receipt.RequestSHA256 != r.RequestSHA256 {
			return false, ErrConflict
		}
		if r.Terminal() {
			if reflect.DeepEqual(r.Receipt, &receipt) {
				return false, nil
			}
			return false, ErrConflict
		}
		if r.State != Running || receipt.Outcome == api.CancelledBeforeStart || (!r.Kind.Mutating() && receipt.Effect != api.EffectNone) || (r.Kind.Mutating() && receipt.Outcome == api.Completed && receipt.Effect != api.EffectCommitted) {
			return false, ErrConflict
		}
		copy := cloneReceipt(receipt)
		r.Receipt = &copy
		r.State = string(receipt.Outcome)
		r.RetainUntil = maxTime(now.Add(s.policy.Retention), r.AuthorityUntil.Add(30*time.Second))
		return true, nil
	})
}

// RejectBeforeStart records a known no-effect failure while this worker still
// owns an assigned operation. It cannot rewrite a running or terminal attempt.
func (s *Store) RejectBeforeStart(ctx context.Context, scope Scope, id string, binding Binding, code string) (Snapshot, error) {
	receipt := api.Receipt{Version: api.Version, OperationID: id, RequestSHA256: strings.Repeat("0", 64), Outcome: api.Rejected, Effect: api.EffectNone, ErrorCode: code}
	if receipt.Validate() != nil {
		return Snapshot{}, ErrInvalid
	}
	return s.changeBound(ctx, scope, id, binding, func(r *Record, now time.Time) (bool, error) {
		if r.State != Assigned {
			return false, ErrConflict
		}
		s.terminal(r, now, api.Rejected, api.EffectNone, code)
		return true, nil
	})
}

func (s *Store) Cancel(ctx context.Context, scope Scope, id string) (Snapshot, error) {
	return s.change(ctx, scope, id, func(r *Record, now time.Time) (bool, error) {
		if r.Terminal() {
			return false, nil
		}
		if r.State == Running {
			if r.Kind.Mutating() {
				s.terminal(r, now, api.OutcomeUnknown, api.EffectUnknown, "OUTCOME_UNKNOWN")
			} else {
				s.terminal(r, now, api.Failed, api.EffectNone, "CANCELLED")
			}
		} else {
			s.terminal(r, now, api.CancelledBeforeStart, api.EffectNone, "CANCELLED")
		}
		return true, nil
	})
}

// RecoverShard marks expired custody; it never requeues assigned/running work.
// Reconciliation calls are bounded per shard and cannot remove live records.
func (s *Store) RecoverShard(ctx context.Context, shard int) error {
	if shard < 0 || shard >= s.policy.Shards {
		return ErrInvalid
	}
	_, err := s.update(ctx, shard, false, func(d *document, now time.Time) (*Record, bool, error) { return nil, false, nil })
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	return err
}

// QueuedShard supports durable-admission/queue-publication recovery. It returns
// at most SlotsPerShard detached records and never revives assigned/running
// attempts. Publishing their IDs repeatedly is safe; Claim remains one CAS.
func (s *Store) QueuedShard(ctx context.Context, shard int) ([]Record, error) {
	if shard < 0 || shard >= s.policy.Shards {
		return nil, ErrInvalid
	}
	var queued []Record
	_, err := s.update(ctx, shard, false, func(d *document, now time.Time) (*Record, bool, error) {
		queued = nil
		for i := range d.Records {
			if d.Records[i].State == Queued {
				queued = append(queued, cloneRecord(&d.Records[i]))
			}
		}
		return nil, false, nil
	})
	if errors.Is(err, ErrNotFound) {
		return []Record{}, nil
	}
	if err != nil {
		return nil, err
	}
	return queued, nil
}

func (s *Store) Policy() Policy { return s.policy }

func (s *Store) changeBound(ctx context.Context, scope Scope, id string, b Binding, fn func(*Record, time.Time) (bool, error)) (Snapshot, error) {
	if b.Validate() != nil {
		return Snapshot{}, ErrInvalid
	}
	return s.change(ctx, scope, id, func(r *Record, now time.Time) (bool, error) {
		if r.Binding != b {
			return false, ErrConflict
		}
		return fn(r, now)
	})
}
func (s *Store) change(ctx context.Context, scope Scope, id string, fn func(*Record, time.Time) (bool, error)) (Snapshot, error) {
	if !s.validScope(scope) {
		return Snapshot{}, ErrNotFound
	}
	shard, err := s.shard(id)
	if err != nil {
		return Snapshot{}, ErrNotFound
	}
	return s.update(ctx, shard, false, func(d *document, now time.Time) (*Record, bool, error) {
		for i := range d.Records {
			r := &d.Records[i]
			if r.ID == id && r.Scope == scope {
				changed, err := fn(r, now)
				return r, changed, err
			}
		}
		return nil, false, ErrNotFound
	})
}

func (s *Store) terminal(r *Record, now time.Time, outcome api.Outcome, effect api.Effect, code string) {
	r.State = string(outcome)
	r.RetainUntil = maxTime(now.Add(s.policy.Retention), r.AuthorityUntil.Add(30*time.Second))
	r.Receipt = &api.Receipt{Version: api.Version, OperationID: r.ID, RequestSHA256: r.RequestSHA256, Outcome: outcome, Effect: effect, ErrorCode: code}
}
func (s *Store) expire(d *document, now time.Time) bool {
	changed := false
	for i := range d.Records {
		r := &d.Records[i]
		if r.Terminal() {
			continue
		}
		if now.Before(r.ExecuteBefore) && (r.State == Queued || now.Before(r.LeaseUntil)) {
			continue
		}
		if r.State == Running {
			if r.Kind.Mutating() {
				s.terminal(r, now, api.OutcomeUnknown, api.EffectUnknown, "OUTCOME_UNKNOWN")
			} else {
				s.terminal(r, now, api.Failed, api.EffectNone, "DEADLINE_EXCEEDED")
			}
		} else {
			s.terminal(r, now, api.Rejected, api.EffectNone, "DEADLINE_EXCEEDED")
		}
		changed = true
	}
	return changed
}

func cloneReceipt(r api.Receipt) api.Receipt {
	raw, _ := json.Marshal(r)
	var copy api.Receipt
	_ = json.Unmarshal(raw, &copy)
	return copy
}
func cloneRecord(r *Record) Record {
	if r == nil {
		return Record{}
	}
	raw, _ := json.Marshal(r)
	var copy Record
	_ = json.Unmarshal(raw, &copy)
	return copy
}

func (s *Store) update(parent context.Context, shard int, create bool, fn func(*document, time.Time) (*Record, bool, error)) (Snapshot, error) {
	ctx, cancel := context.WithTimeout(parent, s.policy.StorageTimeout)
	defer cancel()
	for attempt := 0; attempt < s.policy.MaxCASAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return Snapshot{}, ErrUnavailable
		}
		entry, err := s.backend.Get(ctx, s.key(shard))
		missing := errors.Is(err, ErrMissing)
		if missing && !create {
			return Snapshot{}, ErrNotFound
		}
		if err != nil && !missing {
			return Snapshot{}, ErrUnavailable
		}
		d := document{Version: 1, PolicySHA256: s.policySHA256, Records: []Record{}}
		if !missing {
			if entry.Revision == 0 || api.DecodeStrict(entry.Value, &d, MaxDocumentBytes) != nil || s.validateDocument(d, shard) != nil {
				return Snapshot{}, ErrUnavailable
			}
		}
		now := s.now().UTC()
		expired := s.expire(&d, now)
		r, changed, resultErr := fn(&d, now)
		if !expired && !changed {
			return Snapshot{Record: cloneRecord(r), Revision: entry.Revision}, resultErr
		}
		if s.validateDocument(d, shard) != nil {
			return Snapshot{}, ErrInvalid
		}
		raw, err := json.Marshal(d)
		if err != nil || len(raw) > MaxDocumentBytes {
			return Snapshot{}, ErrCapacity
		}
		var rev uint64
		if missing {
			rev, err = s.backend.Create(ctx, s.key(shard), raw)
		} else {
			rev, err = s.backend.Update(ctx, s.key(shard), raw, entry.Revision)
		}
		if errors.Is(err, ErrRevision) {
			continue
		}
		if err != nil || rev == 0 {
			return Snapshot{}, ErrUnavailable
		}
		return Snapshot{Record: cloneRecord(r), Revision: rev}, resultErr
	}
	return Snapshot{}, ErrUnavailable
}

func (s *Store) validateDocument(d document, shard int) error {
	if d.Version != 1 || d.PolicySHA256 != s.policySHA256 || len(d.Records) > s.policy.SlotsPerShard {
		return ErrInvalid
	}
	ids, identities := map[string]bool{}, map[string]bool{}
	for _, r := range d.Records {
		got, err := s.shard(r.ID)
		if err != nil || got != shard || ids[r.ID] || identities[r.IdentitySHA256] || !api.ValidDigest(r.IdentitySHA256) || !api.ValidDigest(r.RequestSHA256) || !validAuthority(r.AuthorityToken, r.AuthoritySHA256) || !s.validScope(r.Scope) || !r.Kind.Valid() || r.RequestRef.Validate() != nil || r.RequestRef.Format != "operation_request_v1" || r.RequestRef.Bytes > api.MaxRequestBytes ||
			r.CreatedAt.IsZero() || !r.ExecuteBefore.After(r.CreatedAt) || r.ExecuteBefore.Sub(r.CreatedAt) > s.policy.ExecutionTimeout || r.ExecuteBefore.After(r.AuthorityUntil) || !r.AuthorityUntil.After(r.CreatedAt) || r.AuthorityUntil.Sub(r.CreatedAt) > 330*time.Second || r.RetainUntil.Before(r.AuthorityUntil.Add(30*time.Second)) || !r.RetainUntil.After(r.ExecuteBefore) {
			return ErrInvalid
		}
		ids[r.ID], identities[r.IdentitySHA256] = true, true
		if r.Terminal() {
			if r.Receipt.Validate() != nil || r.State != string(r.Receipt.Outcome) || r.Receipt.OperationID != r.ID || r.Receipt.RequestSHA256 != r.RequestSHA256 {
				return ErrInvalid
			}
			if !r.Kind.Mutating() && r.Receipt.Effect != api.EffectNone {
				return ErrInvalid
			}
			if r.Binding != (Binding{}) && r.Binding.Validate() != nil {
				return ErrInvalid
			}
			if (r.Receipt.Outcome == api.Completed || r.Receipt.Outcome == api.Failed || r.Receipt.Outcome == api.OutcomeUnknown) && r.Binding.Validate() != nil {
				return ErrInvalid
			}
		} else {
			switch r.State {
			case Queued:
				if r.Binding != (Binding{}) || !r.LeaseUntil.IsZero() {
					return ErrInvalid
				}
			case Assigned, Running:
				if r.Binding.Validate() != nil || !r.LeaseUntil.After(r.CreatedAt) || r.LeaseUntil.After(r.ExecuteBefore) {
					return ErrInvalid
				}
			default:
				return ErrInvalid
			}
		}
	}
	return nil
}

// This integrity check is not signature verification. Admission and runtime
// must verify the typed grant and its exact request/scope with the trusted key.
func validAuthority(token, digest string) bool {
	return safeText(token, api.MaxGrantBytes) && !strings.ContainsAny(token, " \t") && strings.Count(token, ".") == 2 && api.ValidDigest(digest) && api.GrantDigest(token) == digest
}
