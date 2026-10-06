// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
	"go.yaml.in/yaml/v3"
)

const maxJDBCManifestBytes = 1 << 20

// Runtime configuration is operator-owned. It never comes from a saved source,
// operation request or private credential resolution response.
type OperationJDBCConfig struct {
	JavaHome       string                             `yaml:"java_home"`
	Manifest       string                             `yaml:"manifest"`
	ManifestSHA256 string                             `yaml:"manifest_sha256"`
	Profiles       map[string][]OperationJDBCArtifact `yaml:"profiles"`
}

type OperationJDBCArtifact struct {
	Path   string `yaml:"path"`
	SHA256 string `yaml:"sha256"`
}

type operationJDBCManifest struct {
	Version int                     `yaml:"version"`
	Files   []OperationJDBCArtifact `yaml:"files"`
}

// Clone detaches runtime profile slices before retaining node configuration.
func (cfg OperationProcessConfig) Clone() OperationProcessConfig {
	// Runtime descriptor ownership belongs to one node, never a config clone.
	cfg.preparedJDBC = nil
	if cfg.JDBC == nil {
		return cfg
	}
	copy := *cfg.JDBC
	copy.Profiles = make(map[string][]OperationJDBCArtifact, len(cfg.JDBC.Profiles))
	for engine, jars := range cfg.JDBC.Profiles {
		copy.Profiles[engine] = slices.Clone(jars)
	}
	cfg.JDBC = &copy
	return cfg
}

// PrepareRuntime pins optional operator artifacts once. The caller must stop
// all operations before calling the returned close function. No source identity
// or credential is cached. Per-operation checks still verify every pinned inode.
func (cfg OperationProcessConfig) PrepareRuntime(ctx context.Context) (OperationProcessConfig, func(), error) {
	cfg = cfg.Clone()
	runtime, err := openOperationJDBC(ctx, cfg.JDBC)
	if err != nil {
		return OperationProcessConfig{}, nil, err
	}
	cfg.preparedJDBC = runtime
	var once sync.Once
	close := func() {
		once.Do(func() {
			if runtime != nil {
				runtime.close()
			}
		})
	}
	return cfg, close, nil
}

func jdbcConfigDigest(cfg *OperationJDBCConfig) string {
	if cfg == nil {
		return ""
	}
	raw, err := json.Marshal(cfg)
	if err != nil || len(raw) > 1<<20 {
		return ""
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

func parseOperationJDBCManifest(raw []byte) (operationJDBCManifest, error) {
	var manifest operationJDBCManifest
	bad := operationFailure("CONFIGURATION_ERROR")
	if len(raw) == 0 || len(raw) > maxJDBCManifestBytes {
		return manifest, bad
	}
	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	decoder.KnownFields(true)
	if decoder.Decode(&manifest) != nil || manifest.Version != 1 || len(manifest.Files) == 0 || len(manifest.Files) > 8192 {
		return manifest, bad
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		return manifest, bad
	}
	seen := map[string]bool{}
	for _, file := range manifest.Files {
		if !jdbcRelativePath(file.Path) || !operations.ValidDigest(file.SHA256) || seen[file.Path] {
			return manifest, bad
		}
		seen[file.Path] = true
	}
	for _, required := range []string{"bin/java", "lib/modules", "lib/libjli.so", "lib/server/libjvm.so"} {
		if !seen[required] {
			return manifest, bad
		}
	}
	return manifest, nil
}

func jdbcRelativePath(path string) bool {
	return path != "" && len(path) <= 4096 && !filepath.IsAbs(path) && filepath.Clean(path) == path && path != "." && path != ".." && !strings.HasPrefix(path, "../") && !strings.ContainsAny(path, "\\\x00\r\n\t")
}

func jdbcProcessBudget(memoryMB int) (heap, direct int, err error) {
	if memoryMB < 256 || memoryMB > 1048576 {
		return 0, 0, operationFailure("RESOURCE_EXHAUSTED")
	}
	// Leave room for the Go transport, JVM metadata and native code. The cgroup
	// remains the hard bound; these JVM limits do not account for all RSS.
	heap = min(4096, (memoryMB-128)*2/3)
	direct = min(4096, (memoryMB-128)/3)
	return heap, direct, nil
}

func validateOperationJDBCCatalog(response operationResolution, request operations.Request) error {
	s := response.Source
	if !adapter.JDBCProfile(s.Type) || s.ID != "source_1" || s.Adapter != "" || s.Path != "" || s.DSNEnv != "" || s.TokenEnv != "" || s.URLEnv != "KELVO_SOURCE_REQUEST_0_URL" || s.UsernameEnv != "KELVO_SOURCE_REQUEST_0_USERNAME" || s.PasswordEnv != "KELVO_SOURCE_REQUEST_0_PASSWORD" || s.Federation != nil || s.LocalSnapshot != nil || s.ObjectSnapshot != nil || s.ParquetPaths != nil || s.Ranges != nil || s.Object != nil || s.Range != nil || len(response.Secrets) != 3 || validateOperationTLSOptions(s.Options) != nil {
		return connectionUnavailable()
	}
	for _, key := range []string{s.URLEnv, s.UsernameEnv, s.PasswordEnv} {
		if response.Secrets[key] == "" {
			return connectionUnavailable()
		}
	}
	for key := range s.Options {
		if key != "tls_ca_pem" && key != "tls_server_name" {
			return connectionUnavailable()
		}
	}
	if adapter.ValidateJDBCSource(adapter.ConnectionSpec{Engine: s.Type, URL: response.Secrets[s.URLEnv], Username: response.Secrets[s.UsernameEnv], Password: response.Secrets[s.PasswordEnv], Database: request.Connection.Database, Schema: request.Connection.Schema, Options: s.Options}) != nil {
		return connectionUnavailable()
	}
	return nil
}
