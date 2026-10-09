// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"crypto/ed25519"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"strings"

	"github.com/SYNEHQ/kelvo-go/adapter"
	ledger "github.com/SYNEHQ/kelvo-go/internal/operations"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/transportbroker"
	"github.com/SYNEHQ/kelvo-go/internal/transportbroker/rabbitconnect"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/SYNEHQ/kelvo-go/resolver"
	"github.com/SYNEHQ/kelvo-go/transportissuer"
)

// PrivateOperationConfig is an opt-in operator policy. Requests cannot set it.
// The issuer uses the provisioned resolver origin and the same worker certificate.
type PrivateOperationConfig struct {
	RouteID            string `yaml:"route_id"`
	ProxyAddress       string `yaml:"proxy_address"`
	ProxyServerName    string `yaml:"proxy_server_name"`
	ProxyCAFile        string `yaml:"proxy_ca_file"`
	ProofPublicKey     string `yaml:"proof_public_key"`
	TicketPublicKey    string `yaml:"ticket_public_key"`
	MaxSessions        int    `yaml:"max_sessions"`
	MaxDataConnections int    `yaml:"max_data_connections"`
	MaxDataPerSession  int    `yaml:"max_data_per_session"`
}

var errPrivateOperationConfig = errors.New("private operation configuration must match the worker, resolver, and principal policy")

func privatePublicKey(value string) (ed25519.PublicKey, error) {
	key, err := base64.StdEncoding.Strict().DecodeString(value)
	if err != nil || len(key) != ed25519.PublicKeySize || base64.StdEncoding.EncodeToString(key) != value {
		return nil, errPrivateOperationConfig
	}
	return ed25519.PublicKey(key), nil
}

func (c PrivateOperationConfig) Validate() error {
	if !operations.ValidID(c.RouteID) || transportbroker.ValidateAuthority(c.ProxyAddress) != nil || transportbroker.ValidateAuthority(net.JoinHostPort(c.ProxyServerName, "443")) != nil || c.ProxyCAFile == "" || c.MaxSessions < 1 || c.MaxSessions > 64 || c.MaxDataConnections < 1 || c.MaxDataConnections > 2048 || c.MaxDataPerSession < 1 || c.MaxDataPerSession > 32 || c.MaxDataPerSession > c.MaxDataConnections {
		return errPrivateOperationConfig
	}
	if _, err := privatePublicKey(c.ProofPublicKey); err != nil {
		return err
	}
	if _, err := privatePublicKey(c.TicketPublicKey); err != nil {
		return err
	}
	return nil
}

// PrivateOperationBinding comes from the node's immutable principal policy.
type PrivateOperationBinding struct {
	Config         PrivateOperationConfig
	Trust          operations.GrantTrust
	ResolverCAFile string
}

// PrivateOperations owns the parent-only source runtimes. Close must follow
// operation shutdown, so admitted source cleanup retains its original lifetime.
type PrivateOperations struct {
	runtimes map[string]*privateOperationRuntime
}

func readPrivateCA(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, errPrivateOperationConfig
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, (1<<20)+1))
	if err != nil || len(data) == 0 || len(data) > 1<<20 {
		return nil, errPrivateOperationConfig
	}
	return data, nil
}

func NewPrivateOperations(e *Executor, workerID, identity string, maxConcurrent int, bindings map[string]PrivateOperationBinding) (*PrivateOperations, error) {
	if e == nil || len(bindings) > 64 || maxConcurrent < 1 || maxConcurrent > 256 {
		return nil, errPrivateOperationConfig
	}
	out := &PrivateOperations{runtimes: make(map[string]*privateOperationRuntime)}
	fail := func() (*PrivateOperations, error) {
		_ = out.Close(context.Background())
		return nil, errPrivateOperationConfig
	}
	total := 0
	for principal, binding := range bindings {
		c, trust := binding.Config, binding.Trust
		total += c.MaxSessions
		if c.Validate() != nil || total > maxConcurrent || principal != trust.ServicePrincipal || !operations.ValidID(workerID) || identity != "spiffe://kelvo/tenant/"+trust.ClusterTenant+"/worker/"+workerID {
			return fail()
		}
		r := e.connectionResolvers[trust.Issuer]
		if r == nil || r.client == nil {
			return fail()
		}
		tr, ok := r.client.Transport.(*http.Transport)
		if !ok || tr.TLSClientConfig == nil || len(tr.TLSClientConfig.Certificates) != 1 {
			return fail()
		}
		pair := tr.TLSClientConfig.Certificates[0]
		if len(pair.Certificate) == 0 {
			return fail()
		}
		var certPEM []byte
		for _, cert := range pair.Certificate {
			certPEM = append(certPEM, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert})...)
		}
		keyDER, err := x509.MarshalPKCS8PrivateKey(pair.PrivateKey)
		if err != nil {
			return fail()
		}
		keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
		clear(keyDER)
		proxyCA, err := readPrivateCA(c.ProxyCAFile)
		if err != nil {
			clear(keyPEM)
			return fail()
		}
		issuerCA, err := readPrivateCA(binding.ResolverCAFile)
		if err != nil {
			clear(keyPEM)
			return fail()
		}
		proofKey, _ := privatePublicKey(c.ProofPublicKey)
		ticketKey, _ := privatePublicKey(c.TicketPublicKey)
		policy, err := newPrivateOperationPolicy(r, trust, proofKey, c.RouteID, workerID, identity, pair.Certificate[0])
		if err != nil {
			clear(keyPEM)
			return fail()
		}
		proxy := rabbitconnect.Config{ProxyAddress: c.ProxyAddress, ProxyServerName: c.ProxyServerName, RootCAPEM: proxyCA, ClientCertificatePEM: certPEM, ClientKeyPEM: keyPEM, WorkerIdentity: identity, Issuer: trust.Issuer, Audience: trust.Audience, ClusterTenant: trust.ClusterTenant, ServicePrincipal: principal, IssuerPublicKey: ticketKey}
		issuer := rabbitconnect.HTTPIssuerConfig{Endpoint: strings.TrimSuffix(r.url, resolver.QueryPath) + transportissuer.Path, WorkerIdentity: identity, RootCAPEM: issuerCA, ClientCertificatePEM: certPEM, ClientKeyPEM: keyPEM, MaxInFlight: min(c.MaxDataConnections, 64)}
		runtime, err := newPrivateOperationRuntime(context.Background(), policy, proxy, issuer, transportbroker.Limits{MaxSessions: c.MaxSessions, MaxDataConnections: c.MaxDataConnections, MaxDataPerSession: c.MaxDataPerSession})
		clear(keyPEM)
		if err != nil {
			return fail()
		}
		out.runtimes[principal] = runtime
	}
	return out, nil
}

func (r *PrivateOperations) Close(ctx context.Context) error {
	if r == nil {
		return nil
	}
	var result error
	for _, runtime := range r.runtimes {
		result = errors.Join(result, runtime.close(ctx))
	}
	return result
}

// OperationSourceRequest contains verified input owned by the admitted caller.
// The caller clears Payload after ExecuteDelegatedOperation returns.
type OperationSourceRequest struct {
	Record         ledger.Record
	Request        operations.Request
	Payload        []byte
	MaxResultBytes int64
}

// ExecuteDelegatedOperation runs admission before it reads source credentials.
// Only an authenticated resolver response can select the private transport.
func (e *Executor) ExecuteDelegatedOperation(ctx context.Context, private *PrivateOperations, cfg OperationProcessConfig, operationID, digest string, prepare func(context.Context) (OperationSourceRequest, error), sink query.Sink) (operations.Receipt, error) {
	if prepare == nil {
		return operations.Receipt{}, errPrivateOperationConfig
	}
	return e.executeResolvedOperation(ctx, cfg, operationID, digest, func(admitted context.Context) (adapter.ProcessRequest, *privateOperationChannel, error) {
		source, err := prepare(admitted)
		if err != nil {
			return adapter.ProcessRequest{}, nil, err
		}
		if source.Record.ID != operationID || source.Record.RequestSHA256 != digest || source.MaxResultBytes < 1 {
			return adapter.ProcessRequest{}, nil, errPrivateOperationConfig
		}
		if private != nil {
			if runtime := private.runtimes[source.Record.Scope.ServicePrincipal]; runtime != nil {
				return runtime.prepareMode(admitted, e, source.Record, source.Request, source.Payload, true, source.MaxResultBytes)
			}
		}
		input, err := e.ResolveOperationSourceWithInput(admitted, source.Record, source.Request, source.Payload)
		if err == nil {
			input.Limits.MaxBytes = min(input.Limits.MaxBytes, source.MaxResultBytes)
		}
		return input, nil, err
	}, sink)
}
