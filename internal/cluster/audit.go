// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"errors"
	"net/http"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/audit"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

type ServiceAuditConfig struct {
	audit.Config `yaml:",inline"`
	ServiceID    string `yaml:"service_id"`
}

var auditServiceID = regexp.MustCompile(`^[a-z][a-z0-9-]{0,63}$`)

func (c *ServiceAuditConfig) Validate() error {
	if c == nil {
		return nil
	}
	if !auditServiceID.MatchString(c.ServiceID) {
		return audit.ErrInvalid
	}
	return c.Config.Validate()
}

func validateNodeAudit(config NodeConfig) error {
	if err := config.Audit.Validate(); err != nil {
		return err
	}
	if config.Audit == nil || config.ScratchDirectory == "" {
		return nil
	}
	inside := func(parent, child string) bool {
		relative, err := filepath.Rel(parent, child)
		return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
	}
	if inside(config.Audit.Directory, config.ScratchDirectory) || inside(config.ScratchDirectory, config.Audit.Directory) {
		return errors.New("audit and managed scratch directories must be disjoint")
	}
	return nil
}

// ServiceAudit belongs to the trusted parent. It is never passed to a native
// query subprocess. The caller closes it only after every protected lifecycle
// has joined; a timed-out close never releases pending syscall ownership.
type ServiceAudit struct {
	journal *audit.Journal
	config  ServiceAuditConfig
	scope   audit.Scope
}

func OpenServiceAudit(config *ServiceAuditConfig, kind string, tenants []string) (*ServiceAudit, error) {
	if config == nil {
		return nil, nil
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	scope := audit.Scope{ServiceID: config.ServiceID, ServiceKind: kind, Tenants: append([]string(nil), tenants...)}
	slices.Sort(scope.Tenants)
	journal, err := audit.Open(config.Config, scope)
	if err != nil {
		return nil, err
	}
	return &ServiceAudit{journal: journal, config: *config, scope: scope}, nil
}

func (s *ServiceAudit) matches(config *ServiceAuditConfig, kind string, tenants []string) bool {
	if s == nil {
		return config == nil
	}
	if config == nil || s.config != *config || kind != s.scope.ServiceKind {
		return false
	}
	tenants = append([]string(nil), tenants...)
	slices.Sort(tenants)
	return reflect.DeepEqual(tenants, s.scope.Tenants)
}

func (s *ServiceAudit) Ready() bool { return s == nil || s.journal.Ready() }
func (s *ServiceAudit) Close(ctx context.Context) error {
	if s == nil {
		return nil
	}
	return s.journal.Close(ctx)
}
func (s *ServiceAudit) CloseBounded() error {
	if s == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.config.WriteTimeout)
	defer cancel()
	return s.Close(ctx)
}

func (s *ServiceAudit) binding(tenant string, authority *JobAuthority) audit.Binding {
	b := audit.Binding{TenantID: tenant, ServiceID: s.scope.ServiceID, ServiceKind: s.scope.ServiceKind, PrincipalKind: "unknown"}
	if authority != nil {
		b.PrincipalID = authority.PrincipalID
		b.PrincipalKind = authority.PrincipalKind
		b.PolicyVersion = authority.PolicyVersion
	}
	return b
}

func (s *ServiceAudit) begin(ctx context.Context, tenant string, authority *JobAuthority, kind audit.Kind) (*auditOperation, error) {
	if s == nil {
		return nil, nil
	}
	r, err := s.journal.Begin(ctx, s.binding(tenant, authority), kind)
	if err != nil {
		return nil, err
	}
	return &auditOperation{receipt: r, timeout: s.config.WriteTimeout}, nil
}

func (s *ServiceAudit) beginRequest(ctx context.Context, tenant string, kind audit.Kind) (*auditOperation, error) {
	if s == nil {
		return nil, nil
	}
	if err := requestAuthorityErr(ctx); err != nil {
		return nil, err
	}
	var authority *JobAuthority
	if value, ok := jobAuthorityFromContext(ctx); ok {
		authority = &value
	}
	return s.begin(ctx, tenant, authority, kind)
}

func (s *ServiceAudit) beginService(ctx context.Context, policy Policy, kind audit.Kind) (*auditOperation, error) {
	if s == nil {
		return nil, nil
	}
	authority := JobAuthority{PrincipalID: s.scope.ServiceID, PrincipalKind: "service", PolicyVersion: principalPolicyVersion(policy)}
	return s.begin(ctx, policy.TenantID, &authority, kind)
}

func (s *ServiceAudit) beginGateway(ctx context.Context, policy Policy, kind audit.Kind) (*auditOperation, error) {
	if s == nil {
		return nil, nil
	}
	// The verified SPIFFE gateway role is the actor. The protocol does not
	// identify its individual replica or forward the initiating user's identity.
	authority := JobAuthority{PrincipalID: "gateway", PrincipalKind: "service", PolicyVersion: principalPolicyVersion(policy)}
	return s.begin(ctx, policy.TenantID, &authority, kind)
}

func (s *ServiceAudit) authenticationDenied() {
	if s == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.config.WriteTimeout)
	defer cancel()
	// Never copy a rejected token or untrusted claimed identity into the log.
	// The journal latches failed storage unhealthy; the request remains denied.
	_ = s.journal.Record(ctx, s.binding("", nil), audit.Authentication, audit.Denied, audit.AuthenticationFailure)
}

// RunRefresh covers extraction, immutable publication and pruning. A terminal
// audit failure is returned without claiming to undo already published data.
func (s *ServiceAudit) RunRefresh(ctx context.Context, policy Policy, run func(context.Context) error) (err error) {
	op, err := s.beginService(ctx, policy, audit.RefreshExecution)
	if err != nil {
		return errors.Join(query.NewError("CONFIGURATION_ERROR", "Audit recovery requires operator review before refresh retry"), err)
	}
	defer op.abort(ctx)
	err = run(ctx)
	if auditErr := op.complete(err); auditErr != nil {
		return errors.Join(query.NewError("CONFIGURATION_ERROR", "Audit recovery requires operator review before refresh retry"), auditErr, err)
	}
	return err
}

type auditOperation struct {
	receipt  *audit.Receipt
	timeout  time.Duration
	mu       sync.Mutex
	selected bool
	outcome  audit.Outcome
	category audit.Category
	done     chan struct{}
	err      error
}

func (o *auditOperation) finish(outcome audit.Outcome, category audit.Category) error {
	return o.terminal(outcome, category, false)
}

func (o *auditOperation) terminal(outcome audit.Outcome, category audit.Category, fallback bool) error {
	if o == nil {
		return nil
	}
	o.mu.Lock()
	if o.selected {
		if fallback {
			o.mu.Unlock()
			return nil
		}
		if o.outcome != outcome || o.category != category {
			o.mu.Unlock()
			return audit.ErrInvalid
		}
		done := o.done
		o.mu.Unlock()
		<-done
		return o.err
	}
	o.selected = true
	o.outcome = outcome
	o.category = category
	o.done = make(chan struct{})
	o.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), o.timeout)
	defer cancel()
	o.err = o.receipt.Finish(ctx, outcome, category)
	close(o.done)
	return o.err
}

func (o *auditOperation) complete(err error) error {
	return o.finish(auditResult(err))
}

func auditResult(err error) (audit.Outcome, audit.Category) {
	outcome, category := audit.Succeeded, audit.None
	if err != nil {
		outcome, category = audit.Failed, audit.Internal
		switch {
		case errors.Is(err, context.DeadlineExceeded):
			outcome, category = audit.Cancelled, audit.Timeout
		case errors.Is(err, context.Canceled):
			outcome, category = audit.Cancelled, audit.Cancellation
		default:
			switch query.PublicError(err).Code {
			case "CANCELLED":
				outcome, category = audit.Cancelled, audit.Cancellation
			case "DEADLINE_EXCEEDED":
				outcome, category = audit.Cancelled, audit.Timeout
			case "PERMISSION_DENIED":
				outcome, category = audit.Denied, audit.AuthorizationFailure
			case "UNAUTHENTICATED":
				outcome, category = audit.Denied, audit.AuthenticationFailure
			case "INVALID_ARGUMENT":
				outcome, category = audit.Denied, audit.InvalidRequest
			case "RESOURCE_EXHAUSTED":
				outcome, category = audit.Denied, audit.Capacity
			case "UNAVAILABLE":
				category = audit.Unavailable
			}
		}
	}
	return outcome, category
}

func (o *auditOperation) abort(ctx context.Context) {
	if ctx.Err() != nil {
		outcome, category := auditResult(ctx.Err())
		_ = o.terminal(outcome, category, true)
		return
	}
	_ = o.terminal(audit.Unknown, audit.Interrupted, true)
}

func (g *Gateway) auditError(w http.ResponseWriter, op *auditOperation, status int, code, message string) {
	if err := op.complete(query.NewError(code, message)); err != nil {
		g.err(w, http.StatusServiceUnavailable, "UNAVAILABLE", "Audit storage unavailable")
		return
	}
	g.err(w, status, code, message)
}

func (g *Gateway) auditSuccess(w http.ResponseWriter, r *http.Request, op *auditOperation) bool {
	err := requestAuthorityErr(r.Context())
	if auditErr := op.complete(err); auditErr != nil {
		g.err(w, http.StatusServiceUnavailable, "UNAVAILABLE", "Audit storage unavailable")
		return false
	}
	if err != nil || requestAuthorityErr(r.Context()) != nil {
		g.err(w, http.StatusForbidden, "PERMISSION_DENIED", "Request authority expired")
		return false
	}
	return true
}
