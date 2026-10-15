// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"github.com/SYNEHQ/kelvo-go/internal/audit"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/containment"
	"github.com/SYNEHQ/kelvo-go/internal/worker"
	"github.com/SYNEHQ/kelvo-go/operations"
	"go.yaml.in/yaml/v3"
)

// loadScenario is one node configuration under test. It is the production
// configuration surface: every field below is a knob a deployment sets.
type loadScenario struct {
	Policy      Policy                 `yaml:"policy"`
	Resources   ResourceConfig         `yaml:"resources"`
	Containment containment.Config     `yaml:"containment"`
	Budget      containment.Budget     `yaml:"containment_budget"`
	WarmPool    worker.WarmPoolConfig  `yaml:"warm_pool"`
	Operations  loadScenarioOperations `yaml:"operations"`
	Gateway     loadScenarioGateway    `yaml:"gateway"`
	Audit       loadScenarioAudit      `yaml:"audit"`
	Resolver    loadScenarioResolver   `yaml:"resolver"`
	Source      loadScenarioSource     `yaml:"source"`
	ReadWorkers *ReadWorkersConfig     `yaml:"read_workers,omitempty"`
	Sample      time.Duration          `yaml:"sample_interval"`
}

type loadScenarioOperations struct {
	MaxConcurrent    int   `yaml:"max_concurrent"`
	MaxDownloads     int   `yaml:"max_downloads"`
	MaxResultBytes   int64 `yaml:"max_result_bytes"`
	MaxPerTenant     int   `yaml:"max_per_tenant"`
	MaxPerConnection int   `yaml:"max_per_connection"`
}
type loadScenarioGateway struct {
	MaxHTTPRequests int `yaml:"max_http_requests"`
}
type loadScenarioAudit struct {
	MaxEntries int `yaml:"max_entries"`
	MaxPending int `yaml:"max_pending"`
}
type loadScenarioResolver struct {
	Delay         time.Duration `yaml:"delay"`
	MaxConcurrent int           `yaml:"max_concurrent"`
}
type loadScenarioSource struct {
	Host      string `yaml:"host"`
	Port      int    `yaml:"port"`
	Password  string `yaml:"password"`
	CAPEM     string `yaml:"ca_pem"` // trust root for the source's TLS; the query path requires verify-full
	Customers int    `yaml:"customers"`
}

func (sc loadScenario) validate() error {
	if err := ValidatePolicy(sc.Policy); err != nil {
		return err
	}
	if err := sc.WarmPool.Validate(); err != nil {
		return err
	}
	if _, err := sc.Resources.NewPool(); err != nil {
		return err
	}
	if !sc.Resources.Fits(sc.Policy.Limits, false) {
		return errScenario
	}
	if sc.ReadWorkers != nil {
		rw := sc.ReadWorkers.withDefaults()
		if err := rw.validate(&sc.Resources); err != nil {
			return err
		}
	}
	return nil
}

var errScenario = os.ErrInvalid

// The shipped scenarios must stay loadable and consistent with the node's own
// validation, so a configuration error shows up in ordinary CI rather than
// after a build in the load job.
func TestPooledReadLoadScenariosAreValid(t *testing.T) {
	paths, err := filepath.Glob("../../benchmarks/pooled-read-load/scenarios/*.yaml")
	if err != nil || len(paths) == 0 {
		t.Fatal("no scenarios found", err)
	}
	for _, path := range paths {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var sc loadScenario
		if err := yaml.Unmarshal(raw, &sc); err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		sc.Policy.TenantID = "a"
		sc.Policy.Operations = &OperationPolicy{Shards: 128, SlotsPerShard: 8, Retention: time.Hour, ExecutionTimeout: 5 * time.Minute}
		sc.Policy.Access = &PrincipalPolicy{Revision: 1, Principals: map[string]PrincipalGrant{"api": {Kind: "service", Operations: []operations.Kind{operations.QueryRead},
			DelegatedResolver: &ResolverTrust{Issuer: "i", Audience: "a", URL: "https://127.0.0.1:1/internal/kelvo/resolve", PublicKey: base64.StdEncoding.EncodeToString(make([]byte, 32))}}}}
		sc.Containment = containment.Config{Root: "/x/jobs", StateDirectory: "/x/state", MaxGroups: sc.Containment.MaxGroups}
		if err := sc.validate(); err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		if err := (ContainmentConfig{Config: sc.Containment, Budget: sc.Budget}).Validate(&sc.Resources, sc.Policy.Limits); err != nil {
			t.Fatalf("%s: containment: %v", path, err)
		}
		exe, _ := os.Executable()
		bytes, _ := os.ReadFile(exe)
		sum := sha256.Sum256(bytes)
		self := worker.OperationProcessConfig{Binary: exe, SHA256: hex.EncodeToString(sum[:])}
		tls := TLSConfig{CAFile: "/x/ca", CertFile: "/x/c", KeyFile: "/x/k"}
		node := NodeConfig{Policy: sc.Policy, Resources: &sc.Resources, ScratchDirectory: "/x/scratch", SandboxPath: "/x/sb", Containment: &ContainmentConfig{Config: sc.Containment, Budget: sc.Budget},
			Audit: &ServiceAuditConfig{Config: audit.Config{Directory: "/x/audit", MaxEntries: sc.Audit.MaxEntries, MaxPending: sc.Audit.MaxPending}},
			Operations: &OperationNodeConfig{InputURL: "https://127.0.0.1:1", TLS: tls, Results: OperationInputConfig{Directory: "/x/results", MaxEntries: 256, MaxStoredBytes: 1 << 30},
				MaxResultBytes: sc.Operations.MaxResultBytes, MaxConcurrent: sc.Operations.MaxConcurrent, MaxDownloads: sc.Operations.MaxDownloads, PollInterval: 20 * time.Millisecond, WarmPool: &sc.WarmPool,
				Adapter: self}}
		// The adapter binary is checked on the real file by the load run itself.
		if err := validateNodeOperations(node); errors.Is(err, errOperationConfig) {
			t.Fatalf("%s: operations: %v", path, err)
		}
		if sc.WarmPool.MaxWorkers > sc.Resources.MaxConcurrent {
			t.Fatalf("%s: warm_pool.max_workers exceeds resources.max_concurrent", path)
		}
	}
}
