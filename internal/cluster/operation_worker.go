// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/audit"
	"github.com/SYNEHQ/kelvo-go/internal/operationrun"
	operationstore "github.com/SYNEHQ/kelvo-go/internal/operations"
	"github.com/SYNEHQ/kelvo-go/operations"
)

type OperationWorkerConfig struct {
	Runtime  operationrun.Config
	InputURL string
	TLS      TLSConfig
}

// OperationPrepare builds a driver closure. Resolving credentials, opening a
// connection, and executing commands belong in Prepared.Execute's context.
type OperationPrepare func(context.Context, operationstore.Record, operations.Request, operationstore.Binding) (operationrun.Prepared, error)

// OperationWorker borrows the cluster/operation stores and audit journal. Its
// parent owns the independent worker lease and must keep that lease current.
type OperationWorker struct {
	runtime *operationrun.Runtime
	input   *operationInputClient
	policy  Policy
	store   Store
	ledger  *operationstore.Store
	audit   *ServiceAudit
	prepare OperationPrepare
}

func NewOperationWorker(policy Policy, store Store, ledger *operationstore.Store, cfg OperationWorkerConfig, journal *ServiceAudit, prepare OperationPrepare) (*OperationWorker, error) {
	if store == nil || ledger == nil || journal == nil || journal.journal == nil || prepare == nil || journal.scope.ServiceKind != "worker" || !slices.Contains(journal.scope.Tenants, policy.TenantID) {
		return nil, errOperationConfig
	}
	policy, err := clonePolicy(policy)
	if err != nil || ValidatePolicy(policy) != nil || policy.Operations == nil || operationStorePolicy(policy) != ledger.Policy() || !reflect.DeepEqual(store.Policy(), policy) || cfg.Runtime.Concurrency > policy.Workers[cfg.Runtime.WorkerID] {
		return nil, errOperationConfig
	}
	if _, ok := store.(workerLeaseReader); !ok {
		return nil, errOperationConfig
	}
	input, err := newOperationInputClient(cfg.InputURL, cfg.TLS, policy.TenantID, cfg.Runtime)
	if err != nil {
		return nil, err
	}
	w := &OperationWorker{input: input, policy: policy, store: store, ledger: ledger, audit: journal, prepare: prepare}
	w.runtime, err = operationrun.New(ledger, cfg.Runtime, operationrun.Hooks{
		Load: func(ctx context.Context, record operationstore.Record) (operations.Request, error) {
			if err := w.authorize(ctx, record, nil, record.Binding); err != nil {
				return operations.Request{}, err
			}
			request, err := input.load(ctx, record)
			if err == nil {
				err = w.authorize(ctx, record, &request, record.Binding)
			}
			return request, err
		},
		Authorize: func(ctx context.Context, record operationstore.Record, request operations.Request, binding operationstore.Binding) error {
			return w.authorize(ctx, record, &request, binding)
		},
		Prepare: func(ctx context.Context, record operationstore.Record, request operations.Request, binding operationstore.Binding) (operationrun.Prepared, error) {
			delegate, err := w.prepare(ctx, record, request, binding)
			if err != nil || delegate == nil {
				return delegate, err
			}
			return &operationAuditedExecution{worker: w, record: record, request: request, binding: binding, delegate: delegate}, nil
		},
		AuditAdmission: func(ctx context.Context, _ operationstore.Record, _ operations.Request) error {
			if ctx.Err() != nil || !journal.Ready() {
				return audit.ErrUnavailable
			}
			return nil
		},
	})
	if err != nil {
		input.close()
		return nil, err
	}
	return w, nil
}

func (w *OperationWorker) Start(ctx context.Context) error { return w.runtime.Start(ctx) }
func (w *OperationWorker) Wake()                           { w.runtime.Wake() }
func (w *OperationWorker) BeginDrain()                     { w.runtime.BeginDrain() }
func (w *OperationWorker) Drain(ctx context.Context) error {
	if err := w.runtime.Drain(ctx); err != nil {
		return err
	}
	w.input.close()
	return nil
}
func (w *OperationWorker) Close(ctx context.Context) error {
	if err := w.runtime.Close(ctx); err != nil {
		return err
	}
	w.input.close()
	return nil
}

func (w *OperationWorker) authorize(ctx context.Context, record operationstore.Record, request *operations.Request, binding operationstore.Binding) error {
	denied := operationstore.ErrConflict
	if ctx.Err() != nil || !w.input.ready() || !reflect.DeepEqual(w.store.Policy(), w.policy) || binding.WorkerID != w.input.worker || binding.Owner != w.input.owner || binding != record.Binding || operations.GrantDigest(record.AuthorityToken) != record.AuthoritySHA256 {
		return denied
	}
	trust, err := operationTrust(w.policy, record.Scope.ServicePrincipal)
	if err != nil {
		return denied
	}
	claims, err := operations.VerifyGrantClaims(record.AuthorityToken, trust, time.Now())
	if request != nil {
		claims, err = operations.VerifyGrant(record.AuthorityToken, trust, *request, time.Now())
	}
	if err != nil || operationScope(claims) != record.Scope || claims.RequestSHA256 != record.RequestSHA256 || claims.Operation != record.Kind || record.AuthorityUntil.After(time.Unix(claims.ExpiresAt, 0)) || !slices.Contains(w.policy.Access.Principals[claims.ServicePrincipal].Operations, record.Kind) {
		return denied
	}
	check := func() error {
		current, err := w.ledger.Get(ctx, record.Scope, record.ID)
		if err != nil {
			return err
		}
		got := current.Record
		if (got.State != operationstore.Assigned && got.State != operationstore.Running) || got.Binding != binding || got.RequestSHA256 != record.RequestSHA256 || got.RequestRef != record.RequestRef || got.AuthoritySHA256 != record.AuthoritySHA256 || !time.Now().Before(got.LeaseUntil) || !time.Now().Before(got.ExecuteBefore) {
			return denied
		}
		return nil
	}
	if err := check(); err != nil {
		return err
	}
	until, err := w.store.(workerLeaseReader).WorkerLease(ctx, binding.WorkerID, binding.Owner)
	if err != nil || !time.Now().Before(until) || ctx.Err() != nil {
		return denied
	}
	if err := check(); err != nil {
		return err
	}
	if !time.Now().Before(until) || ctx.Err() != nil {
		return denied
	}
	return nil
}

type operationInputClient struct {
	base                  string
	tenant, worker, owner string
	client                *http.Client
	transport             *http.Transport
	identityUntil         time.Time
}

func newOperationInputClient(rawURL string, cfg TLSConfig, tenant string, runtime operationrun.Config) (*operationInputClient, error) {
	u, err := url.Parse(rawURL)
	if err != nil || len(rawURL) > 2048 || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.Opaque != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.RawPath != "" || (u.Path != "" && u.Path != "/") || !clusterID.MatchString(tenant) || !clusterID.MatchString(runtime.WorkerID) || cfg.Trust != nil || cfg.IdentityFile != "" || cfg.ReloadInterval != 0 || runtime.Concurrency < 1 || runtime.Concurrency > 256 {
		return nil, errOperationConfig
	}
	identity, err := loadIdentity(cfg)
	if err != nil {
		return nil, errOperationConfig
	}
	leaf := identity.Leaf
	expected := WorkerIdentity(tenant, runtime.WorkerID)
	if leaf == nil || leaf.IsCA || len(leaf.URIs) != 1 || leaf.URIs[0].String() != expected || !slices.Contains(leaf.ExtKeyUsage, x509.ExtKeyUsageClientAuth) || time.Now().Before(leaf.NotBefore) || !time.Now().Before(leaf.NotAfter) {
		return nil, errOperationConfig
	}
	roots, err := loadTLSRoots(cfg, nil)
	if err != nil {
		return nil, errOperationConfig
	}
	intermediates := x509.NewCertPool()
	for _, raw := range identity.Certificate[1:] {
		cert, err := x509.ParseCertificate(raw)
		if err != nil {
			return nil, errOperationConfig
		}
		intermediates.AddCert(cert)
	}
	chains, err := leaf.Verify(x509.VerifyOptions{Roots: roots, Intermediates: intermediates, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}})
	if err != nil {
		return nil, errOperationConfig
	}
	until := leaf.NotAfter
	for _, cert := range chains[0] {
		until = minTime(until, cert.NotAfter)
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots, Certificates: []tls.Certificate{identity}, ClientSessionCache: nil,
		VerifyConnection: func(state tls.ConnectionState) error {
			if !time.Now().Before(until) || !operationGatewayPeer(&state, time.Now()) {
				return errOperationConfig
			}
			return nil
		}}
	transport := &http.Transport{Proxy: nil, DialContext: (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		TLSClientConfig: tlsConfig, TLSHandshakeTimeout: 5 * time.Second, ResponseHeaderTimeout: 5 * time.Second,
		DisableCompression: true, MaxConnsPerHost: runtime.Concurrency, MaxIdleConns: runtime.Concurrency,
		MaxIdleConnsPerHost: runtime.Concurrency, IdleConnTimeout: 30 * time.Second, MaxResponseHeaderBytes: 8192}
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return &operationInputClient{base: strings.TrimSuffix(rawURL, "/"), tenant: tenant, worker: runtime.WorkerID, owner: runtime.Owner, client: client, transport: transport, identityUntil: until}, nil
}

func operationGatewayPeer(state *tls.ConnectionState, now time.Time) bool {
	if state == nil || state.Version < tls.VersionTLS13 || len(state.PeerCertificates) == 0 || len(state.VerifiedChains) == 0 {
		return false
	}
	leaf := state.PeerCertificates[0]
	if leaf == nil || leaf.IsCA || len(leaf.URIs) != 1 || leaf.URIs[0].String() != GatewayIdentity || !slices.Contains(leaf.ExtKeyUsage, x509.ExtKeyUsageServerAuth) {
		return false
	}
	for _, chain := range state.VerifiedChains {
		if len(chain) == 0 || chain[0] == nil || !chain[0].Equal(leaf) {
			continue
		}
		valid := true
		for _, cert := range chain {
			if cert == nil || now.Before(cert.NotBefore) || !now.Before(cert.NotAfter) {
				valid = false
				break
			}
		}
		if valid {
			return true
		}
	}
	return false
}

// VerifyConnection runs before HandshakeComplete is set. HTTP responses must
// additionally prove that the handshake completed before any bytes are used.
func operationGatewayResponsePeer(state *tls.ConnectionState, now time.Time) bool {
	return state != nil && state.HandshakeComplete && operationGatewayPeer(state, now)
}

func (c *operationInputClient) ready() bool { return time.Now().Before(c.identityUntil) }
func (c *operationInputClient) close()      { c.transport.CloseIdleConnections() }
func (c *operationInputClient) load(ctx context.Context, record operationstore.Record) (operations.Request, error) {
	if !c.ready() || record.Scope.ClusterTenant != c.tenant || record.Binding.WorkerID != c.worker || record.Binding.Owner != c.owner || record.Binding.Validate() != nil || !operations.ValidID(record.ID) || record.RequestRef.Validate() != nil || record.RequestRef.Format != "operation_request_v1" || record.RequestRef.Bytes > operations.MaxRequestBytes {
		return operations.Request{}, operationstore.ErrInvalid
	}
	raw, err := json.Marshal(operationInputRequest{Scope: record.Scope, Binding: record.Binding})
	if err != nil {
		return operations.Request{}, operationstore.ErrInvalid
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+"/v1/operations/"+c.tenant+"/"+record.ID+"/input", bytes.NewReader(raw))
	if err != nil {
		return operations.Request{}, operationstore.ErrInvalid
	}
	req.GetBody = nil // A reused socket failure must not retry this private fetch.
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	response, err := c.client.Do(req)
	if err != nil {
		return operations.Request{}, operationstore.ErrUnavailable
	}
	defer response.Body.Close()
	media, _, mediaErr := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if response.StatusCode != http.StatusOK || mediaErr != nil || media != "application/json" || response.Header.Get("Content-Encoding") != "" || !operationGatewayResponsePeer(response.TLS, time.Now()) || (response.ContentLength != -1 && response.ContentLength != record.RequestRef.Bytes) {
		return operations.Request{}, operationstore.ErrUnavailable
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, record.RequestRef.Bytes+1))
	defer clear(data)
	if err != nil || int64(len(data)) != record.RequestRef.Bytes || ctx.Err() != nil || !c.ready() || !operationGatewayResponsePeer(response.TLS, time.Now()) {
		return operations.Request{}, operationstore.ErrUnavailable
	}
	sum := sha256.Sum256(data)
	if hex.EncodeToString(sum[:]) != record.RequestRef.SHA256 {
		return operations.Request{}, operationstore.ErrInvalid
	}
	request, err := operations.ParseRequest(data)
	if err != nil {
		return operations.Request{}, operationstore.ErrInvalid
	}
	ref, _, err := operations.SealRequest(request, record.RequestRef.ID)
	if err != nil || ref != record.RequestRef {
		return operations.Request{}, operationstore.ErrInvalid
	}
	return request, nil
}

type operationAuditedExecution struct {
	worker   *OperationWorker
	record   operationstore.Record
	request  operations.Request
	binding  operationstore.Binding
	delegate operationrun.Prepared
}

func (p *operationAuditedExecution) Close() error { return p.delegate.Close() }
func (p *operationAuditedExecution) Execute(ctx context.Context) (operations.Receipt, error) {
	rejected := func(code string) operations.Receipt {
		return operations.Receipt{Version: operations.Version, OperationID: p.record.ID, RequestSHA256: p.record.RequestSHA256, Outcome: operations.Rejected, Effect: operations.EffectNone, ErrorCode: code}
	}
	if err := p.worker.authorize(ctx, p.record, &p.request, p.binding); err != nil {
		return rejected("PERMISSION_DENIED"), err
	}
	authority, ok := authorityForPrincipal(p.worker.policy, p.record.Scope.ServicePrincipal)
	if !ok {
		return rejected("PERMISSION_DENIED"), operationstore.ErrConflict
	}
	op, err := p.worker.audit.begin(ctx, p.record.Scope.ClusterTenant, &authority, audit.OperationExecution)
	if err != nil {
		return rejected("UNAVAILABLE"), err
	}
	defer op.abort(ctx)
	// Audit reservation can block; recheck source custody after its admission.
	if err := p.worker.authorize(ctx, p.record, &p.request, p.binding); err != nil {
		return rejected("PERMISSION_DENIED"), errors.Join(err, op.finish(audit.Denied, audit.AuthorizationFailure))
	}
	receipt, executeErr := p.delegate.Execute(ctx)
	bound := receipt
	if bound.Version == 0 {
		bound.Version = operations.Version
	}
	if bound.OperationID == "" {
		bound.OperationID = p.record.ID
	}
	if bound.RequestSHA256 == "" {
		bound.RequestSHA256 = p.record.RequestSHA256
	}
	outcome, category := audit.Unknown, audit.Interrupted
	if bound.Validate() == nil && bound.OperationID == p.record.ID && bound.RequestSHA256 == p.record.RequestSHA256 {
		switch bound.Outcome {
		case operations.Completed:
			outcome, category = audit.Succeeded, audit.None
		case operations.Rejected:
			outcome, category = audit.Denied, audit.InvalidRequest
		case operations.Failed:
			outcome, category = audit.Failed, audit.Internal
		case operations.CancelledBeforeStart:
			outcome, category = audit.Cancelled, audit.Cancellation
		}
	}
	return receipt, errors.Join(executeErr, op.finish(outcome, category))
}
