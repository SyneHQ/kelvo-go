// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package application

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/SYNEHQ/kelvo-go/delegation"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/SYNEHQ/kelvo-go/query"
	"github.com/SYNEHQ/kelvo-go/resolver"
)

type Subject struct {
	Kind        string            `yaml:"kind"`
	Team        string            `yaml:"team"`
	Connections map[string]string `yaml:"connections"`
}

type Connection struct {
	Team            string `yaml:"team"`
	Type            string `yaml:"type"`
	Database        string `yaml:"database"`
	Schema          string `yaml:"schema"`
	Revision        string `yaml:"revision"`
	CredentialsFile string `yaml:"credentials_file"`
	ReadSQL         string `yaml:"read_sql"`
	WriteSQL        string `yaml:"write_sql"`
	CancelSQL       string `yaml:"cancel_sql"`
}

type Policy struct {
	Version     int                   `yaml:"version"`
	Subjects    map[string]Subject    `yaml:"subjects"`
	Connections map[string]Connection `yaml:"connections"`
}

type credentialReference struct {
	Revision  string `yaml:"revision"`
	DSNEnv    string `yaml:"dsn_env"`
	DSNFile   string `yaml:"dsn_file"`
	TLSCAFile string `yaml:"tls_ca_file"`
}

var ErrPolicyDenied = errors.New("application policy denied request")

type Store struct {
	Config Config
	Audit  *Audit
}

func (s Store) policy(ctx context.Context) (Policy, error) {
	var p Policy
	if ctx.Err() != nil || readYAML(s.Config.PolicyFile, &p) != nil || p.Version != 1 || len(p.Subjects) == 0 || len(p.Subjects) > 128 || len(p.Connections) == 0 || len(p.Connections) > 128 {
		return p, ErrConfiguration
	}
	return p, nil
}

// Select re-reads app-owned policy. Possessing a signed grant never substitutes
// for current membership, connection ownership, or database/schema authorization.
func (s Store) Select(ctx context.Context, team, subject, kind, connection string, write bool) (Connection, resolver.Authorization, error) {
	p, err := s.policy(ctx)
	if err != nil {
		return Connection{}, resolver.Authorization{}, err
	}
	u, exists := p.Subjects[subject]
	c, found := p.Connections[connection]
	permission := u.Connections[connection]
	if !exists || !found || kind != "user" || u.Kind != kind || team == "" || u.Team != team || c.Team != team || (permission != "read" && permission != "write") || (write && permission != "write") {
		return Connection{}, resolver.Authorization{}, ErrPolicyDenied
	}
	if c.Revision == "" || c.CredentialsFile == "" || c.Database == "" || c.ReadSQL == "" {
		return Connection{}, resolver.Authorization{}, ErrConfiguration
	}
	// This example ships PostgreSQL SQL and a DSN-only material contract. Other
	// Kelvo connectors need their own source mapping and application SQL policy.
	if c.Type != "postgres" {
		return Connection{}, resolver.Authorization{}, ErrConfiguration
	}
	raw, err := json.Marshal(struct {
		Subject    Subject
		Connection Connection
	}{u, c})
	if err != nil {
		return Connection{}, resolver.Authorization{}, resolver.ErrInvalid
	}
	return c, resolver.Authorization{Revision: delegation.Digest(string(raw)), ValidUntil: time.Now().Add(30 * time.Second)}, nil
}

func readShape(sql string, parameters []query.Parameter, c Connection) bool {
	if sql == c.CancelSQL && c.CancelSQL != "" {
		return len(parameters) == 0
	}
	if sql != c.ReadSQL || len(parameters) != 1 || parameters[0].Type != "string" {
		return false
	}
	values, err := (query.Request{Parameters: parameters}).Values()
	return err == nil && values[0] != ""
}

func (s Store) AuthorizeQuery(ctx context.Context, input resolver.QueryRequest, claims delegation.Claims) (resolver.Authorization, error) {
	if input.Query.Mode != "native" || input.Query.ConnectionID != "source_1" || len(claims.Sources) != 1 || len(claims.Sources[0].Tables) != 0 {
		return resolver.Authorization{}, ErrPolicyDenied
	}
	selected := claims.Sources[0]
	c, a, err := s.Select(ctx, claims.AppTeam, claims.Subject.ID, claims.Subject.Kind, selected.ConnectionID, false)
	if err != nil {
		return resolver.Authorization{}, err
	}
	if selected.Alias != "source_1" || selected.Database != c.Database || selected.Schema != c.Schema || !readShape(input.Query.SQL, input.Query.Parameters, c) {
		return resolver.Authorization{}, ErrPolicyDenied
	}
	return a, nil
}

func (s Store) AuthorizeOperation(ctx context.Context, input resolver.OperationRequest, claims operations.GrantClaims) (resolver.Authorization, error) {
	r := input.Operation
	if r.Kind != operations.QueryRead && r.Kind != operations.StatementExecute {
		return resolver.Authorization{}, ErrPolicyDenied
	}
	c, a, err := s.Select(ctx, claims.AppTeam, claims.Subject.ID, claims.Subject.Kind, r.Connection.ID, r.Kind == operations.StatementExecute)
	if err != nil {
		return resolver.Authorization{}, err
	}
	if r.Connection.Database != c.Database || r.Connection.Schema != c.Schema || claims.ConnectionID != r.Connection.ID {
		return resolver.Authorization{}, ErrPolicyDenied
	}
	if r.Kind == operations.QueryRead {
		if claims.Authorization.Kind != "read" || r.Spec.Query == nil {
			return resolver.Authorization{}, ErrPolicyDenied
		}
		parameters := make([]query.Parameter, len(r.Spec.Query.Parameters))
		for i, p := range r.Spec.Query.Parameters {
			parameters[i] = query.Parameter{Type: p.Type, Value: p.Value}
		}
		if !readShape(r.Spec.Query.SQL, parameters, c) {
			return resolver.Authorization{}, ErrPolicyDenied
		}
	} else {
		v := r.Spec.Statement
		if claims.Authorization.Kind != "trusted_app" || c.WriteSQL == "" || v == nil || v.SQL != c.WriteSQL || v.Batch != nil || v.Transaction != operations.TransactionRequired || v.Role != "" || v.Isolation != "" || len(v.Parameters) != 2 || v.Parameters[0].Type != "string" || v.Parameters[1].Type != "int64" {
			return resolver.Authorization{}, ErrPolicyDenied
		}
	}
	return a, nil
}

// material opens credentials only after authorization and re-reads their revision.
// Operators publish a new immutable credential file, then atomically update the
// policy's filename and revision. A partially published revision fails closed.
func (s Store) material(ctx context.Context, c Connection) (resolver.Source, map[string]string, error) {
	if ctx.Err() != nil {
		return resolver.Source{}, nil, resolver.ErrInvalid
	}
	path := relative(filepath.Dir(s.Config.PolicyFile), c.CredentialsFile)
	var ref credentialReference
	if readYAML(path, &ref) != nil || ref.Revision != c.Revision || (ref.DSNEnv == "") == (ref.DSNFile == "") {
		return resolver.Source{}, nil, resolver.ErrInvalid
	}
	var value string
	if ref.DSNEnv != "" {
		value = os.Getenv(ref.DSNEnv)
	} else {
		raw, err := readFile(relative(filepath.Dir(path), ref.DSNFile), resolver.MaxSecretBytes, true)
		if err != nil {
			return resolver.Source{}, nil, resolver.ErrInvalid
		}
		value = strings.TrimSpace(string(raw))
		clear(raw)
	}
	if ctx.Err() != nil || value == "" || len(value) > resolver.MaxSecretBytes || strings.ContainsAny(value, "\x00\r\n") {
		return resolver.Source{}, nil, resolver.ErrInvalid
	}
	name, _ := resolver.SecretReference(0, "DSN")
	source := resolver.Source{ID: "source_1", Type: c.Type, DSNEnv: name}
	if ref.TLSCAFile != "" {
		raw, err := readFile(relative(filepath.Dir(path), ref.TLSCAFile), 64<<10, false)
		if err != nil {
			return resolver.Source{}, nil, resolver.ErrInvalid
		}
		source.Options = map[string]string{"tls_ca_pem": string(raw)}
	}
	return source, map[string]string{name: value}, nil
}

func (s Store) ResolveQuery(ctx context.Context, input resolver.QueryRequest, claims delegation.Claims, before resolver.Authorization) (resolver.QueryMaterial, error) {
	if len(claims.Sources) != 1 {
		return resolver.QueryMaterial{}, resolver.ErrInvalid
	}
	c, after, err := s.Select(ctx, claims.AppTeam, claims.Subject.ID, claims.Subject.Kind, claims.Sources[0].ConnectionID, false)
	if err != nil || before.Revision != after.Revision {
		return resolver.QueryMaterial{}, resolver.ErrInvalid
	}
	source, secrets, err := s.material(ctx, c)
	return resolver.QueryMaterial{Sources: []resolver.Source{source}, Secrets: secrets, ValidUntil: before.ValidUntil}, err
}

func (s Store) ResolveOperation(ctx context.Context, input resolver.OperationRequest, claims operations.GrantClaims, before resolver.Authorization) (resolver.OperationMaterial, error) {
	c, after, err := s.Select(ctx, claims.AppTeam, claims.Subject.ID, claims.Subject.Kind, input.Operation.Connection.ID, input.Operation.Kind.Mutating())
	if err != nil || before.Revision != after.Revision {
		return resolver.OperationMaterial{}, resolver.ErrInvalid
	}
	source, secrets, err := s.material(ctx, c)
	return resolver.OperationMaterial{Source: source, Secrets: secrets, ValidUntil: before.ValidUntil}, err
}

func (s Store) Handler(leases resolver.LeaseVerifier) (*resolver.Handler, error) {
	key, err := PublicKey(s.Config)
	if err != nil {
		return nil, err
	}
	i := s.Config.Identity
	return resolver.NewHandler(resolver.Config{
		QueryTrust:     []delegation.Trust{{Issuer: i.Issuer, Audience: i.Audience, ClusterTenant: i.ClusterTenant, ServicePrincipal: i.ServicePrincipal, PublicKey: key}},
		OperationTrust: []operations.GrantTrust{{Issuer: i.Issuer, Audience: i.Audience, ClusterTenant: i.ClusterTenant, ServicePrincipal: i.ServicePrincipal, PublicKey: key}},
		Leases:         leases, Timeout: 5 * time.Second, MaxConcurrent: 8,
		AuthorizeQuery: func(ctx context.Context, r resolver.QueryRequest, c delegation.Claims) (resolver.Authorization, error) {
			a, err := s.AuthorizeQuery(ctx, r, c)
			event := "query_authorized"
			if errors.Is(err, ErrPolicyDenied) {
				event = "query_policy_denied"
			} else if err != nil {
				event = "query_authorization_failed"
			}
			if auditErr := s.Audit.record(event); auditErr != nil {
				return resolver.Authorization{}, auditErr
			}
			return a, err
		},
		ResolveQuery: func(ctx context.Context, r resolver.QueryRequest, c delegation.Claims, a resolver.Authorization) (resolver.QueryMaterial, error) {
			m, err := s.ResolveQuery(ctx, r, c, a)
			event := "credentials"
			if err != nil {
				event = "credential_failure"
			}
			if auditErr := s.Audit.record(event); auditErr != nil {
				clear(m.Secrets)
				return resolver.QueryMaterial{}, auditErr
			}
			return m, err
		},
		AuthorizeOperation: func(ctx context.Context, r resolver.OperationRequest, c operations.GrantClaims) (resolver.Authorization, error) {
			a, err := s.AuthorizeOperation(ctx, r, c)
			event := "operation_authorized"
			if errors.Is(err, ErrPolicyDenied) {
				event = "operation_policy_denied"
			} else if err != nil {
				event = "operation_authorization_failed"
			}
			if auditErr := s.Audit.record(event); auditErr != nil {
				return resolver.Authorization{}, auditErr
			}
			return a, err
		},
		ResolveOperation: func(ctx context.Context, r resolver.OperationRequest, c operations.GrantClaims, a resolver.Authorization) (resolver.OperationMaterial, error) {
			m, err := s.ResolveOperation(ctx, r, c, a)
			event := "credentials"
			if err != nil {
				event = "credential_failure"
			}
			if auditErr := s.Audit.record(event); auditErr != nil {
				clear(m.Secrets)
				return resolver.OperationMaterial{}, auditErr
			}
			return m, err
		},
	})
}
