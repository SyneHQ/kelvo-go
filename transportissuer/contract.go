// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0

// Package transportissuer defines the application-owned private connection issuer
// protocol. A request describes claimed scope; the authority must independently
// verify source mapping, revocation and live execution ownership before signing.
package transportissuer

const (
	Version          = 1
	Path             = "/v1/private-transport/issue"
	MaxRequestBytes  = 8192
	MaxResponseBytes = 12288
)

type Execution struct {
	Kind        string `json:"kind"`
	ID          string `json:"id"`
	GrantSHA256 string `json:"grant_sha256"`
	Worker      string `json:"worker_id"`
	Owner       string `json:"owner"`
	Claim       string `json:"claim"`
}

type Request struct {
	Version          int       `json:"version"`
	Issuer           string    `json:"issuer"`
	Audience         string    `json:"audience"`
	ClusterTenant    string    `json:"cluster_tenant"`
	ServicePrincipal string    `json:"service_principal"`
	Tenant           string    `json:"tenant"`
	Source           string    `json:"source"`
	SourceRevision   string    `json:"source_revision"`
	Authority        string    `json:"authority"`
	Execution        Execution `json:"execution"`
	ExpiresAt        int64     `json:"expires_at"`
	OpenID           string    `json:"open_id"`
	WorkerIdentity   string    `json:"worker_identity"`
	WorkerCertSHA256 string    `json:"worker_cert_sha256"`
}

// RequestSHA256 binds the response to the exact received request bytes. Token is
// a Rabbit v1 ticket. The parent must verify its signature and every scope field.
type Response struct {
	Version       int    `json:"version"`
	RequestSHA256 string `json:"request_sha256"`
	Token         string `json:"token"`
}
