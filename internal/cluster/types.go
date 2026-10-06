// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Package cluster distributes independent queries across tenant-bound workers.
package cluster

import (
	"context"
	"errors"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/admission"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/secrets"
	"github.com/SYNEHQ/kelvo-go/internal/telemetry"
	"github.com/SYNEHQ/kelvo-go/internal/tracing"
	"github.com/SYNEHQ/kelvo-go/internal/worker"
)

const (
	Queued   = "queued"
	Assigned = "assigned"
	Claimed  = "claimed"
	Running  = "running"
	// ResultReady pins successful result state until the gateway verifies and
	// commits the terminal outcome before releasing the response stream.
	ResultReady = "result_ready"
	Succeeded   = "succeeded"
	Failed      = "failed"
	Cancelled   = "cancelled"
)

var (
	ErrNotFound = errors.New("query not found")
	ErrConflict = errors.New("query state changed")
	ErrCapacity = errors.New("tenant capacity exhausted")
	ErrNoJob    = errors.New("no queued job")
)

// Policy is provisioned by the operator and checked by every gateway and node.
// Static tenant budgets partition cluster capacity; there is no shared mutable
// quota counter. One durable KV slot atomically contains admission AND job state.
type Policy struct {
	Exports       *ExportPolicy    `json:"exports,omitempty" yaml:"exports,omitempty"`
	Access        *PrincipalPolicy `json:"access,omitempty" yaml:"access,omitempty"`
	SourceQuotas  map[string]int   `json:"source_quotas,omitempty" yaml:"source_quotas,omitempty"`
	TenantID      string           `json:"tenant_id" yaml:"tenant_id"`
	MaxQueries    int              `json:"max_queries" yaml:"max_queries"`
	JobTTL        time.Duration    `json:"job_ttl" yaml:"job_ttl"`
	LeaseDuration time.Duration    `json:"lease_duration" yaml:"lease_duration"`
	Replicas      int              `json:"replicas" yaml:"replicas"`
	Limits        query.Limits     `json:"limits" yaml:"limits"`
	Workers       map[string]int   `json:"workers" yaml:"workers"`
}

type Job struct {
	Trace       *tracing.Carrier `json:"trace,omitempty"`
	Authority   *JobAuthority    `json:"authority,omitempty"`
	ID          string           `json:"id"`
	TenantID    string           `json:"tenant_id"`
	State       string           `json:"state"`
	Request     query.Request    `json:"request"`
	CreatedAt   time.Time        `json:"created_at"`
	ExpiresAt   time.Time        `json:"expires_at"`
	HeartbeatAt time.Time        `json:"heartbeat_at,omitempty"`
	WorkerID    string           `json:"worker_id,omitempty"`
	Owner       string           `json:"owner,omitempty"`
	Claim       string           `json:"claim,omitempty"`
	Stats       query.Stats      `json:"stats"`
	Error       *query.Error     `json:"error,omitempty"`
}

func (j Job) Terminal() bool {
	return j.State == Succeeded || j.State == Failed || j.State == Cancelled
}

type Snapshot struct {
	Job      Job
	Revision uint64
}

// Delivery contains only a job ID. SQL lives in tenant-scoped state; credentials
// and Arrow batches never enter JetStream.
type Delivery interface {
	ID() string
	Ack(context.Context) error
	Retry(context.Context) error
}

type Store interface {
	Policy() Policy
	Submit(context.Context, query.Request) (Snapshot, error)
	Get(context.Context, string) (Snapshot, error)
	CompareAndSwap(context.Context, Snapshot, Job) (Snapshot, error)
	Enqueue(context.Context, string) error
	Next(context.Context) (Delivery, error)
	Reconcile(context.Context) error
	ClaimWorker(context.Context, string, string) error
	HeartbeatWorker(context.Context, string, string) error
	Close() error
}

type NATSConfig struct {
	URL             string `yaml:"url"`
	Username        string `yaml:"username"`
	PasswordEnv     string `yaml:"password_env"`
	CredentialsFile string `yaml:"credentials_file"`
	CAFile          string `yaml:"ca_file"`
	CertFile        string `yaml:"cert_file"`
	KeyFile         string `yaml:"key_file"`
}

type TLSConfig struct {
	Trust          *TLSTrustConfig `yaml:"trust,omitempty"`
	CertFile       string          `yaml:"cert_file"`
	KeyFile        string          `yaml:"key_file"`
	CAFile         string          `yaml:"ca_file"`
	IdentityFile   string          `yaml:"identity_file,omitempty"`
	ReloadInterval time.Duration   `yaml:"reload_interval,omitempty"`
}

type Endpoint struct {
	ID  string `yaml:"id"`
	URL string `yaml:"url"`
}

type TenantConfig struct {
	Policy   Policy     `yaml:"policy"`
	TokenEnv string     `yaml:"token_env"`
	NATS     NATSConfig `yaml:"nats"`
	Workers  []Endpoint `yaml:"workers"`
}

type GatewayConfig struct {
	Tracing             *tracing.Config              `yaml:"tracing,omitempty"`
	Exports             *GatewayExportConfig         `yaml:"exports,omitempty"`
	RuntimeExportStores map[string]ExportStore       `yaml:"-"`
	Audit               *ServiceAuditConfig          `yaml:"audit,omitempty"`
	Authentication      *GatewayAuthenticationConfig `yaml:"authentication,omitempty"`
	Listen              string                       `yaml:"listen"`
	TLS                 TLSConfig                    `yaml:"tls"`
	WorkerTLS           TLSConfig                    `yaml:"worker_tls"`
	MaxQueries          int                          `yaml:"max_queries"`
	MaxConcurrent       int                          `yaml:"max_concurrent"`
	MaxHTTPRequests     int                          `yaml:"max_http_requests"`
	Tenants             []TenantConfig               `yaml:"tenants"`
}

type NodeConfig struct {
	ConnectionResolvers map[string]worker.ConnectionResolverConfig `yaml:"connection_resolvers,omitempty"`
	Exports             *ExportNodeConfig                          `yaml:"exports,omitempty"`
	RuntimeExportStore  ExportStore                                `yaml:"-"`
	Metrics             *MetricsConfig                             `yaml:"metrics,omitempty"`
	Audit               *ServiceAuditConfig                        `yaml:"audit,omitempty"`
	RuntimeAudit        *ServiceAudit                              `yaml:"-"`
	Containment         *ContainmentConfig                         `yaml:"containment,omitempty"`
	ScratchDirectory    string                                     `yaml:"scratch_directory,omitempty"`
	SourceHealth        *telemetry.SourceHealthConfig              `yaml:"source_health,omitempty"`
	RuntimeSourceHealth *telemetry.SourceHealth                    `yaml:"-"`
	Tracing             *tracing.Config                            `yaml:"tracing,omitempty"`
	RuntimeTracing      *tracing.Recorder                          `yaml:"-"`
	RequiredDatasets    []string                                   `yaml:"required_datasets,omitempty"`
	RuntimeDatasets     *DatasetReporter                           `yaml:"-"`
	History             *telemetry.HistoryConfig                   `yaml:"history,omitempty"`
	RuntimeHistory      *telemetry.History                         `yaml:"-"`
	Secrets             *secrets.Config                            `yaml:"secrets,omitempty"`
	RuntimeResources    *admission.Pool                            `yaml:"-"`
	Resources           *ResourceConfig                            `yaml:"resources,omitempty"`
	RuntimeMetrics      *telemetry.Registry                        `yaml:"-"`
	Listen              string                                     `yaml:"listen"`
	TLS                 TLSConfig                                  `yaml:"tls"`
	NATS                NATSConfig                                 `yaml:"nats"`
	Policy              Policy                                     `yaml:"policy"`
	WorkerID            string                                     `yaml:"worker_id"`
	CatalogFile         string                                     `yaml:"catalog_file"`
	SandboxPath         string                                     `yaml:"sandbox_path"`
}
