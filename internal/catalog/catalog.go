// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package catalog

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
)

var identifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,62}$`)

func ValidID(s string) bool { return identifier.MatchString(s) }

// Source holds public metadata and references to secret environment variables.
type Source struct {
	ID          string `json:"id"`
	Type        string `json:"type"`
	Path        string `json:"path,omitempty"`
	DSNEnv      string `json:"dsn_env,omitempty"`
	URLEnv      string `json:"url_env,omitempty"`
	UsernameEnv string `json:"username_env,omitempty"`
	PasswordEnv string `json:"password_env,omitempty"`
}
type Config struct {
	Sources            []Source `json:"sources"`
	ExtensionDirectory string   `json:"extension_directory,omitempty"`
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
	var c Config
	d := json.NewDecoder(io.LimitReader(f, 1<<20))
	d.DisallowUnknownFields()
	if e = d.Decode(&c); e != nil {
		return c, fmt.Errorf("invalid source configuration")
	}
	if e = d.Decode(new(any)); e != io.EOF {
		return c, fmt.Errorf("source configuration must contain one JSON object")
	}
	seen := map[string]bool{}
	for i := range c.Sources {
		s := &c.Sources[i]
		if !ValidID(s.ID) || seen[s.ID] {
			return c, fmt.Errorf("source IDs must be unique SQL identifiers")
		}
		seen[s.ID] = true
		switch s.Type {
		case "csv", "parquet", "duckdb":
			if s.Path == "" {
				return c, fmt.Errorf("source %s requires path", s.ID)
			}
			if !filepath.IsAbs(s.Path) {
				s.Path = filepath.Join(filepath.Dir(path), s.Path)
			}
			s.Path, e = filepath.EvalSymlinks(s.Path)
			if e != nil {
				return c, fmt.Errorf("source %s path unavailable", s.ID)
			}
			st, e := os.Stat(s.Path)
			if e != nil || !st.Mode().IsRegular() {
				return c, fmt.Errorf("source %s must name a regular file", s.ID)
			}
		case "postgres", "mysql":
			if s.DSNEnv == "" {
				return c, fmt.Errorf("source %s requires dsn_env", s.ID)
			}
		case "clickhouse":
			if s.URLEnv == "" {
				return c, fmt.Errorf("source %s requires url_env", s.ID)
			}
		default:
			return c, fmt.Errorf("source %s has unsupported type", s.ID)
		}
	}
	if c.ExtensionDirectory != "" && !filepath.IsAbs(c.ExtensionDirectory) {
		c.ExtensionDirectory = filepath.Join(filepath.Dir(path), c.ExtensionDirectory)
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
			return nil, fmt.Errorf("unknown source")
		}
	}
	return out, nil
}
