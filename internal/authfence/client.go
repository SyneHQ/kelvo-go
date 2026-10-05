// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package authfence

import (
	"context"
	"errors"
	"net"
	"net/url"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

// Config references operator-managed credentials. Construction is inert; all
// file and network work belongs to one bounded, tracked protocol operation.
// This package is a protocol candidate, not an authentication integration.
type Config struct {
	URL, CAFile, CertFile, KeyFile string
	CredentialsFile                string
	Username, PasswordEnv          string
	Scope                          Scope
	ReplicaID                      string
}

var envName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,127}$`)

func validFileReference(path string) bool {
	return len(path) > 1 && len(path) <= 4096 && filepath.IsAbs(path) &&
		filepath.Clean(path) == path && !strings.ContainsRune(path, 0)
}

func (c Config) valid() bool {
	u, err := url.Parse(c.URL)
	if err != nil || len(c.URL) > 4096 || u.Scheme != "tls" || u.User != nil || u.RawQuery != "" ||
		u.Fragment != "" || u.ForceQuery || strings.Contains(c.URL, "#") || u.Path != "" || u.Opaque != "" || u.Hostname() == "" || u.Port() == "" ||
		strings.ContainsAny(c.URL, "\r\n\x00, ") || !validFileReference(c.CAFile) || !c.Scope.Valid() ||
		(c.ReplicaID != ControlReplicaID && !c.Scope.HasReplica(c.ReplicaID)) {
		return false
	}
	if _, _, err = net.SplitHostPort(u.Host); err != nil {
		return false
	}
	port, err := strconv.Atoi(u.Port())
	if err != nil || port < 1 || port > 65535 {
		return false
	}
	if (c.CertFile == "") != (c.KeyFile == "") ||
		(c.CertFile != "" && (!validFileReference(c.CertFile) || !validFileReference(c.KeyFile))) {
		return false
	}
	if c.CredentialsFile != "" {
		return validFileReference(c.CredentialsFile) && c.Username == "" && c.PasswordEnv == ""
	}
	return len(c.Username) > 0 && len(c.Username) <= 128 && !strings.ContainsAny(c.Username, "\r\n\x00") && envName.MatchString(c.PasswordEnv)
}

// Validate checks the complete reference and scope shape without reading
// credentials, touching the filesystem or constructing a transport.
func (c Config) Validate() error {
	if !c.valid() {
		return ErrInvalid
	}
	return nil
}

type wireSession interface {
	request(string, []byte, nats.Header) ([]byte, error)
	close() error
}

// Client admits one operation, retaining that slot through late file reads,
// dialing, cancellation callbacks and socket cleanup. A timed-out caller does
// not free capacity. Quiesced describes this ownership, not every library
// goroutine, proof of OS resource release or erasure of Go-managed secret
// memory. Cleanup uncertainty permanently fences further operations.
type Client struct {
	config    Config
	mu        sync.Mutex
	closed    bool
	active    *operation
	quiet     chan struct{}
	quietOnce sync.Once
	closeErr  error
	open      func(*operation, Config) (wireSession, error)
}

func NewClient(config Config) (*Client, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	// Reconstruct the scope so even package-local callers cannot share slices.
	scope, err := NewScope(config.Scope.ID(), config.Scope.Tenants(), config.Scope.Gateways())
	if err != nil {
		return nil, ErrInvalid
	}
	config.Scope = scope
	return &Client{config: config, quiet: make(chan struct{}), open: openNATSSession}, nil
}

type operation struct {
	owner        *Client
	attempt      Attempt
	ctx          context.Context
	cancel       context.CancelFunc
	done         chan struct{}
	mu           sync.Mutex
	mutationSent bool
}

type operationResult struct {
	snapshot Snapshot
	verified VerifiedObservation
	mutation MutationResult
	err      error
}

func (c *Client) begin(ctx context.Context, attempt Attempt) (*operation, error) {
	if ctx == nil || !attempt.ValidAt(time.Now()) {
		return nil, ErrExpired
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, ErrClosed
	}
	if c.active != nil {
		return nil, ErrBusy
	}
	if ctx.Err() != nil {
		return nil, ErrExpired
	}
	opCtx, cancel := context.WithDeadline(ctx, attempt.Deadline())
	op := &operation{owner: c, attempt: attempt, ctx: opCtx, cancel: cancel, done: make(chan struct{})}
	c.active = op
	return op, nil
}

func (op *operation) fresh() bool {
	return op.ctx.Err() == nil && op.attempt.ValidAt(time.Now())
}

func (op *operation) request(session wireSession, subject string, data []byte, header nats.Header, mutation bool) ([]byte, error) {
	// Cancellation is checked under the same lock as the possibly-sent marker.
	// A caller observing no mutation after cancellation cannot race a later send.
	op.mu.Lock()
	if !op.fresh() {
		op.mu.Unlock()
		return nil, ErrExpired
	}
	if mutation {
		op.mutationSent = true
	}
	op.mu.Unlock()
	return session.request(subject, data, header)
}

func (op *operation) uncertain() bool {
	op.mu.Lock()
	defer op.mu.Unlock()
	return op.mutationSent
}

func (op *operation) finish() {
	op.cancel()
	c := op.owner
	c.mu.Lock()
	c.active = nil
	if c.closed {
		c.quietOnce.Do(func() { close(c.quiet) })
	}
	close(op.done)
	c.mu.Unlock()
}

func (c *Client) run(ctx context.Context, attempt Attempt, fn func(*operation, wireSession) operationResult) operationResult {
	op, err := c.begin(ctx, attempt)
	if err != nil {
		return operationResult{err: err}
	}
	output := make(chan operationResult, 1)
	go func() {
		defer op.finish()
		session, openErr := c.open(op, c.config)
		result := operationResult{err: openErr}
		if openErr == nil && session == nil {
			result.err = ErrUnavailable
		} else if openErr == nil && op.fresh() {
			result = fn(op, session)
		}
		if session != nil {
			if closeErr := session.close(); closeErr != nil {
				c.mu.Lock()
				c.closeErr = ErrUnknown
				c.closed = true
				c.mu.Unlock()
				result = operationResult{err: ErrUnknown}
				if op.uncertain() {
					result.mutation = MutationResult{outcome: OutcomeUnknown}
					result.err = ErrUnknown
				}
			}
		}
		if !op.fresh() {
			result = operationResult{err: ErrExpired}
			if op.uncertain() {
				result.mutation, result.err = MutationResult{outcome: OutcomeUnknown}, ErrUnknown
			}
		}
		if result.err != nil && op.uncertain() &&
			!errors.Is(result.err, ErrConflict) && !errors.Is(result.err, ErrDenied) && !errors.Is(result.err, ErrInvalid) {
			result = operationResult{mutation: MutationResult{outcome: OutcomeUnknown}, err: ErrUnknown}
		}
		result.err = publicError(result.err)
		output <- result
	}()
	var result operationResult
	completed := false
	select {
	case result = <-output:
		completed = true
	case <-op.ctx.Done():
		// finish cancels the operation after queuing its result. If both are
		// ready, preserve that result; only the original caller/deadline and
		// closed-state checks below may invalidate successful completion.
		select {
		case result = <-output:
			completed = true
		default:
		}
	}
	if completed {
		// The method result alone is insufficient: retain capacity until cleanup
		// and owner bookkeeping have completed too.
		<-op.done
		c.mu.Lock()
		closed, closeErr := c.closed, c.closeErr
		c.mu.Unlock()
		if closeErr != nil {
			result = operationResult{err: ErrUnknown}
			if op.uncertain() {
				result.mutation = MutationResult{outcome: OutcomeUnknown}
			}
			return result
		}
		if !closed && op.attempt.ValidAt(time.Now()) && ctx.Err() == nil {
			return result
		}
	}
	op.cancel()
	if op.uncertain() {
		return operationResult{mutation: MutationResult{outcome: OutcomeUnknown}, err: ErrUnknown}
	}
	return operationResult{err: ErrExpired}
}

func publicError(err error) error {
	if err == nil {
		return nil
	}
	for _, known := range []error{ErrInvalid, ErrConflict, ErrDenied, ErrNotFound, ErrUnavailable, ErrUnknown, ErrClosed, ErrBusy, ErrExpired} {
		if errors.Is(err, known) {
			return known
		}
	}
	return ErrUnavailable
}

func (c *Client) Read(ctx context.Context, attempt Attempt) (Snapshot, error) {
	r := c.run(ctx, attempt, func(op *operation, session wireSession) operationResult {
		if err := checkStream(op, session, c.config.Scope); err != nil {
			return operationResult{err: err}
		}
		snapshot, err := readAuthority(op, session, c.config.Scope)
		return operationResult{snapshot: snapshot, err: err}
	})
	return r.snapshot, r.err
}

func (c *Client) Verify(ctx context.Context, attempt Attempt, expected Document) (VerifiedObservation, error) {
	if c.config.ReplicaID == ControlReplicaID {
		return VerifiedObservation{}, ErrDenied
	}
	if !expected.Valid() {
		return VerifiedObservation{}, ErrInvalid
	}
	r := c.run(ctx, attempt, func(op *operation, session wireSession) operationResult {
		observation, err := verifyAuthority(op, session, c.config, expected)
		return operationResult{verified: observation, err: err}
	})
	if r.err == nil && !r.verified.ValidFor(c.config.Scope, attempt, expected, time.Now()) {
		return VerifiedObservation{}, ErrExpired
	}
	return r.verified, r.err
}

func (c *Client) Initialize(ctx context.Context, attempt Attempt, document Document) (MutationResult, error) {
	return c.mutate(ctx, attempt, document, 0, true)
}

func (c *Client) Advance(ctx context.Context, attempt Attempt, expectedRevision uint64, document Document) (MutationResult, error) {
	if c.config.ReplicaID != ControlReplicaID {
		return MutationResult{outcome: OutcomeDenied}, ErrDenied
	}
	if expectedRevision == 0 || document.Revision() <= expectedRevision {
		return MutationResult{outcome: OutcomeInvalid}, ErrInvalid
	}
	return c.mutate(ctx, attempt, document, expectedRevision, false)
}

func (c *Client) mutate(ctx context.Context, attempt Attempt, document Document, revision uint64, initialize bool) (MutationResult, error) {
	if c.config.ReplicaID != ControlReplicaID {
		return MutationResult{outcome: OutcomeDenied}, ErrDenied
	}
	if !document.Valid() {
		return MutationResult{outcome: OutcomeInvalid}, ErrInvalid
	}
	r := c.run(ctx, attempt, func(op *operation, session wireSession) operationResult {
		mutation, err := mutateAuthority(op, session, c.config, document, revision, initialize)
		return operationResult{mutation: mutation, err: err}
	})
	return r.mutation, r.err
}

func (c *Client) Check(ctx context.Context, attempt Attempt, document Document) (MutationResult, error) {
	if c.config.ReplicaID != ControlReplicaID {
		return MutationResult{outcome: OutcomeDenied}, ErrDenied
	}
	if !document.Valid() {
		return MutationResult{outcome: OutcomeInvalid}, ErrInvalid
	}
	r := c.run(ctx, attempt, func(op *operation, session wireSession) operationResult {
		observation, err := verifyAuthority(op, session, c.config, document)
		if err != nil {
			return operationResult{err: err}
		}
		return operationResult{mutation: MutationResult{outcome: OutcomeCurrentVerified, snapshot: observation.Snapshot()}}
	})
	return r.mutation, r.err
}

func (c *Client) Close(ctx context.Context) error {
	if ctx == nil {
		return ErrInvalid
	}
	ctx, stop := context.WithTimeout(ctx, MaxAttemptDuration)
	defer stop()
	c.mu.Lock()
	c.closed = true
	op := c.active
	if op == nil {
		c.quietOnce.Do(func() { close(c.quiet) })
	}
	c.mu.Unlock()
	if op != nil {
		op.cancel()
	}
	select {
	case <-c.quiet:
		c.mu.Lock()
		err := c.closeErr
		c.mu.Unlock()
		return err
	case <-ctx.Done():
		return ErrUnknown
	}
}

func (c *Client) Quiesced() <-chan struct{} { return c.quiet }
