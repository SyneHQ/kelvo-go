// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package catalog

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"

	"go.yaml.in/yaml/v3"
)

var identifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,62}$`)

const maxConfigBytes = 1 << 20

func ValidID(s string) bool { return identifier.MatchString(s) }

// Source holds public metadata and references to secret environment variables.
type Source struct {
	Federation  *FederationConfig `json:"federation,omitempty" yaml:"federation,omitempty"`
	ID          string            `json:"id" yaml:"id"`
	Type        string            `json:"type" yaml:"type"`
	Adapter     string            `json:"adapter,omitempty" yaml:"adapter,omitempty"`
	Path        string            `json:"path,omitempty" yaml:"path,omitempty"`
	DSNEnv      string            `json:"dsn_env,omitempty" yaml:"dsn_env,omitempty"`
	URLEnv      string            `json:"url_env,omitempty" yaml:"url_env,omitempty"`
	UsernameEnv string            `json:"username_env,omitempty" yaml:"username_env,omitempty"`
	PasswordEnv string            `json:"password_env,omitempty" yaml:"password_env,omitempty"`
	TokenEnv    string            `json:"token_env,omitempty" yaml:"token_env,omitempty"`
	Options     map[string]string `json:"options,omitempty" yaml:"options,omitempty"`
	Object      *ObjectRead       `json:"object,omitempty" yaml:"-"`
	Range       *ObjectRange      `json:"object_range,omitempty" yaml:"-"`
}
type Config struct {
	Sources            []Source            `json:"sources" yaml:"sources"`
	ExtensionDirectory string              `json:"extension_directory,omitempty" yaml:"extension_directory,omitempty"`
	Acceleration       *AccelerationConfig `json:"acceleration,omitempty" yaml:"acceleration,omitempty"`
}

func Load(path string) (Config, error) {
	// Workers execute from disposable working directories. Resolve every file
	// reference against the caller's absolute configuration location first.
	path, e := filepath.Abs(path)
	if e != nil {
		return Config{}, fmt.Errorf("source configuration path is unavailable")
	}
	f, e := os.Open(path)
	if e != nil {
		return Config{}, fmt.Errorf("source configuration is unavailable")
	}
	defer f.Close()
	data, e := io.ReadAll(io.LimitReader(f, maxConfigBytes+1))
	if e != nil {
		return Config{}, fmt.Errorf("source configuration is unavailable")
	}
	if len(data) > maxConfigBytes {
		return Config{}, fmt.Errorf("source configuration exceeds 1 MiB")
	}
	var parsed *Config
	d := yaml.NewDecoder(bytes.NewReader(data))
	d.KnownFields(true)
	if e = d.Decode(&parsed); e != nil || parsed == nil {
		return Config{}, fmt.Errorf("invalid YAML source configuration")
	}
	if e = d.Decode(new(any)); e != io.EOF {
		return Config{}, fmt.Errorf("source configuration must contain one YAML document")
	}
	c := *parsed
	seen := map[string]bool{}
	for i := range c.Sources {
		s := &c.Sources[i]
		for _, name := range []string{s.DSNEnv, s.URLEnv, s.UsernameEnv, s.PasswordEnv, s.TokenEnv} {
			if name != "" {
				if err := ValidateEnvironment(name); err != nil {
					return Config{}, err
				}
			}
		}
		if !ValidID(s.ID) || seen[s.ID] {
			return c, fmt.Errorf("source IDs must be unique SQL identifiers")
		}
		seen[s.ID] = true
		s.Type = CanonicalType(s.Type)
		if err := s.ValidateFederation(); err != nil {
			return c, err
		}
		if len(s.Options) > 16 {
			return c, fmt.Errorf("source %s has too many options", s.ID)
		}
		for key, value := range s.Options {
			if len(key) > 64 || len(value) > 4096 {
				return c, fmt.Errorf("source %s option exceeds size limit", s.ID)
			}
		}
		if s.Adapter != "" {
			if err := s.ValidateAdapter(); err != nil {
				return c, err
			}
			continue
		}
		switch s.Type {
		case "csv", "parquet", "duckdb", "sqlite":
			if s.Path == "" {
				return c, fmt.Errorf("source %s requires path", s.ID)
			}
			if !filepath.IsAbs(s.Path) {
				s.Path = filepath.Join(filepath.Dir(path), s.Path)
			}
			registeredPath := filepath.Clean(s.Path)
			s.Path, e = filepath.EvalSymlinks(registeredPath)
			if os.IsNotExist(e) && c.accelerationReferences(s.ID) {
				// A maintained snapshot remains queryable during a local-source
				// outage. Direct reads/refresh still validate the file at execution.
				s.Path = registeredPath
				continue
			}
			if e != nil {
				return c, fmt.Errorf("source %s path unavailable", s.ID)
			}
			st, e := os.Stat(s.Path)
			if e != nil || !st.Mode().IsRegular() {
				return c, fmt.Errorf("source %s must name a regular file", s.ID)
			}
		case "postgres", "mysql", "mariadb", "cockroachdb", "alloydb", "redshift", "sqlserver", "oracle", "mongodb", "exasol":
			if s.DSNEnv == "" {
				return c, fmt.Errorf("source %s requires dsn_env", s.ID)
			}
		case "clickhouse":
			if s.URLEnv == "" {
				return c, fmt.Errorf("source %s requires url_env", s.ID)
			}
		case "databricks", "snowflake", "d1", "bigquery", "elasticsearch", "trino", "presto", "arrow_flight", "spanner", "cosmosdb":
			if s.URLEnv == "" || s.TokenEnv == "" {
				return c, fmt.Errorf("source %s requires url_env and token_env", s.ID)
			}
		case "athena", "dynamodb", "ignite":
			if s.URLEnv == "" || s.UsernameEnv == "" || s.PasswordEnv == "" {
				return c, fmt.Errorf("source %s requires url_env, username_env and password_env", s.ID)
			}
			if s.DSNEnv != "" || s.Path != "" || (s.Type == "ignite" && s.TokenEnv != "") {
				return c, fmt.Errorf("source %s has conflicting credentials", s.ID)
			}
		default:
			if KnownType(s.Type) {
				return c, fmt.Errorf("source %s requires an optional adapter", s.ID)
			}
			return c, fmt.Errorf("source %s has unsupported type", s.ID)
		}
	}
	if c.ExtensionDirectory != "" && !filepath.IsAbs(c.ExtensionDirectory) {
		c.ExtensionDirectory = filepath.Join(filepath.Dir(path), c.ExtensionDirectory)
	}
	if err := c.validateAcceleration(filepath.Dir(path)); err != nil {
		return c, err
	}
	return c, nil
}
func (c Config) Select(ids []string) ([]Source, error) {
	out := make([]Source, 0, len(ids))
	seen := map[string]bool{}
	for _, id := range ids {
		if seen[id] {
			return nil, fmt.Errorf("duplicate source")
		}
		seen[id] = true
		found := false
		for _, s := range c.Sources {
			if s.ID == id {
				out = append(out, s)
				found = true
				break
			}
		}
		if !found {
			if _, ok := c.Dataset(id); ok {
				out = append(out, Source{ID: id, Type: "accelerated"})
			} else {
				return nil, fmt.Errorf("unknown source")
			}
		}
	}
	return out, nil
}
