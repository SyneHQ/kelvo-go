// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"path/filepath"
	"regexp"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/audit"
	"github.com/SYNEHQ/kelvo-go/internal/operationinput"
	operationstore "github.com/SYNEHQ/kelvo-go/internal/operations"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/worker"
	"github.com/SYNEHQ/kelvo-go/operations"
)

// ApplicationConfig defines one installation. The application owns users,
// encrypted credentials, and current permission checks. Kelvo owns execution.
type ApplicationConfig struct {
	Version          int                             `yaml:"version"`
	Listen           string                          `yaml:"listen"`
	StateDirectory   string                          `yaml:"state_directory"`
	TokenEnv         string                          `yaml:"token_env"`
	InstanceID       string                          `yaml:"instance_id"`
	AppScope         string                          `yaml:"app_scope"`
	Issuer           string                          `yaml:"issuer"`
	Audience         string                          `yaml:"audience"`
	ServicePrincipal string                          `yaml:"service_principal"`
	PublicKey        string                          `yaml:"public_key"`
	WriteMode        string                          `yaml:"write_mode"`
	TLS              TLSConfig                       `yaml:"tls"`
	Resolver         worker.ConnectionResolverConfig `yaml:"resolver"`
	Adapter          worker.OperationProcessConfig   `yaml:"adapter"`
	Limits           query.Limits                    `yaml:"limits"`
	Resources        ResourceConfig                  `yaml:"resources"`
	Containment      ContainmentConfig               `yaml:"containment"`
	SandboxPath      string                          `yaml:"sandbox_path"`
	ScratchDirectory string                          `yaml:"scratch_directory"`
	MaxRetained      int                             `yaml:"max_retained"`
	Retention        time.Duration                   `yaml:"retention"`
	MaxConcurrent    int                             `yaml:"max_concurrent"`
	MaxHTTPRequests  int                             `yaml:"max_http_requests"`
	MaxResultBytes   int64                           `yaml:"max_result_bytes"`
	MaxMetadataBytes int64                           `yaml:"max_metadata_bytes"`
	MaxStoredBytes   int64                           `yaml:"max_stored_bytes"`
}

var errApplicationConfig = errors.New("invalid application configuration")
var applicationEnv = regexp.MustCompile(`^[A-Z_][A-Z0-9_]{0,127}$`)

func LoadApplication(path string) (ApplicationConfig, error) {
	c := ApplicationConfig{Version: 1, Listen: "127.0.0.1:8080", TokenEnv: "KELVO_TOKEN", WriteMode: "disabled",
		MaxRetained: 512, Retention: time.Hour, MaxConcurrent: 2, MaxHTTPRequests: 16,
		MaxResultBytes: 4 << 20, MaxMetadataBytes: 1 << 20, MaxStoredBytes: 1 << 30, Limits: query.DefaultLimits()}
	c.Limits.Timeout = 30 * time.Second
	base, err := decodeConfig(path, &c)
	if err != nil {
		return c, errApplicationConfig
	}
	c.StateDirectory = relativePath(base, c.StateDirectory)
	c.ScratchDirectory = relativePath(base, c.ScratchDirectory)
	c.SandboxPath = relativePath(base, c.SandboxPath)
	c.Containment.StateDirectory = relativePath(base, c.Containment.StateDirectory)
	c.Adapter.Binary = relativePath(base, c.Adapter.Binary)
	resolveTLS(base, &c.TLS)
	c.Resolver.CAFile = relativePath(base, c.Resolver.CAFile)
	c.Resolver.CertFile = relativePath(base, c.Resolver.CertFile)
	c.Resolver.KeyFile = relativePath(base, c.Resolver.KeyFile)
	return c, c.Validate()
}

func (c ApplicationConfig) Validate() error {
	if c.Version != 1 || !clusterID.MatchString(c.InstanceID) || !applicationEnv.MatchString(c.TokenEnv) ||
		(c.WriteMode != "disabled" && c.WriteMode != "approved") || c.MaxRetained < 8 || c.MaxRetained > 1024 || c.MaxRetained%8 != 0 ||
		c.MaxConcurrent < 1 || c.MaxConcurrent > 8 || c.MaxConcurrent > c.Resources.MaxConcurrent || c.MaxHTTPRequests < c.MaxConcurrent || c.MaxHTTPRequests > 128 ||
		c.Limits.Validate() != nil || c.Limits.Timeout > 5*time.Minute || c.Retention < 10*time.Minute || c.Retention > 7*24*time.Hour ||
		c.MaxResultBytes < 1024 || c.MaxResultBytes > operationinput.MaxInputBytes || c.MaxResultBytes > c.Limits.MaxBytes ||
		c.MaxMetadataBytes < 1024 || c.MaxMetadataBytes > c.MaxResultBytes ||
		c.MaxStoredBytes < 2*c.MaxResultBytes || c.MaxStoredBytes > 1<<30 || c.Resolver.Validate() != nil ||
		c.Adapter.JDBC != nil || c.Adapter.Validate() != nil || c.TLS.CertFile == "" || c.TLS.KeyFile == "" || c.TLS.Trust != nil || c.TLS.IdentityFile != "" || c.TLS.ReloadInterval != 0 ||
		c.Containment.Validate(&c.Resources, c.Limits) != nil || !c.Resources.Fits(c.Limits, false) || c.Resources.Export != nil ||
		3*c.MaxResultBytes+(2<<20) > (c.Resources.MemoryMB-c.Resources.BaselineMB)<<20 {
		return errApplicationConfig
	}
	if _, _, err := net.SplitHostPort(c.Listen); err != nil {
		return errApplicationConfig
	}
	for _, path := range []string{c.StateDirectory, c.ScratchDirectory, c.SandboxPath, c.Containment.StateDirectory} {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path || path == "/" {
			return errApplicationConfig
		}
	}
	if overlappingExportPaths(c.StateDirectory, c.ScratchDirectory) || overlappingExportPaths(c.StateDirectory, c.Containment.StateDirectory) || overlappingExportPaths(c.ScratchDirectory, c.Containment.StateDirectory) {
		return errApplicationConfig
	}
	trust, err := c.trust()
	if err != nil {
		return err
	}
	scope := operationstore.Scope{Issuer: trust.Issuer, ClusterTenant: c.InstanceID, ServicePrincipal: trust.ServicePrincipal, AppTeam: c.AppScope, SubjectKind: "user", SubjectID: "configuration", ConnectionID: "configuration"}
	if scope.Validate() != nil || len(c.Issuer) > 128 || len(c.Audience) > 128 || c.Audience == "" || c.operationPolicy().Validate() != nil {
		return errApplicationConfig
	}
	return nil
}

func (c ApplicationConfig) trust() (operations.GrantTrust, error) {
	key, err := base64.StdEncoding.Strict().DecodeString(c.PublicKey)
	if err != nil || len(key) != ed25519.PublicKeySize || base64.StdEncoding.EncodeToString(key) != c.PublicKey {
		return operations.GrantTrust{}, errApplicationConfig
	}
	return operations.GrantTrust{Issuer: c.Issuer, Audience: c.Audience, ClusterTenant: c.InstanceID, ServicePrincipal: c.ServicePrincipal, PublicKey: ed25519.PublicKey(key)}, nil
}

func (c ApplicationConfig) operationPolicy() operationstore.Policy {
	return operationstore.Policy{Namespace: "application", TenantID: c.InstanceID, Shards: c.MaxRetained / 8, SlotsPerShard: 8, Retention: c.Retention, ExecutionTimeout: c.Limits.Timeout, LeaseDuration: min(3*time.Second, c.Limits.Timeout), StorageTimeout: 2 * time.Second, MaxCASAttempts: 8}
}

func (c ApplicationConfig) authorityFingerprint() string {
	raw, _ := json.Marshal([]string{c.InstanceID, c.AppScope, c.Issuer, c.Audience, c.ServicePrincipal, c.PublicKey, c.WriteMode, c.Resolver.URL})
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func (c ApplicationConfig) auditConfig() *ServiceAuditConfig {
	return &ServiceAuditConfig{ServiceID: "application", Config: audit.Config{Directory: filepath.Join(c.StateDirectory, "audit"), MaxEntries: 8192, MaxPending: 64, Retention: c.Retention, WriteTimeout: 2 * time.Second}}
}

func (c ApplicationConfig) allows(claims operations.GrantClaims) bool {
	if claims.AppTeam != c.AppScope || (claims.Subject.Kind != "user" && claims.Subject.Kind != "admin") {
		return false
	}
	switch claims.Operation {
	case operations.ConnectionTest, operations.MetadataInspect, operations.QueryRead:
		return claims.Authorization.Kind == "read"
	case operations.StatementExecute:
		return c.WriteMode == "approved" && claims.Authorization.Kind == "approved_change"
	default:
		return false
	}
}
