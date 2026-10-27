// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0

// Package pgwire serves a read-only PostgreSQL wire-protocol front end.
//
// It owns no execution or authorization: every statement is sent in-process to
// the same http.Handler that serves the HTTP query API (standalone serve or the
// cluster gateway), with the client's password as its bearer key.
package pgwire

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"go.yaml.in/yaml/v3"
)

// Config enables the listener. A nil *Config leaves PostgreSQL wire disabled.
type Config struct {
	Listen           string        `yaml:"listen"`
	TLS              TLSConfig     `yaml:"tls"`
	MaxConnections   int           `yaml:"max_connections"`
	IdleTimeout      time.Duration `yaml:"idle_timeout"`
	StatementTimeout time.Duration `yaml:"statement_timeout"`
}

// TLSConfig names the server certificate. TLS is mandatory: passwords are API keys.
type TLSConfig struct {
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
}

// Validate rejects incomplete or unbounded settings.
func (c Config) Validate() error {
	switch {
	case c.Listen == "":
		return errors.New("postgres_wire.listen is required")
	case c.TLS.CertFile == "" || c.TLS.KeyFile == "":
		return errors.New("postgres_wire.tls.cert_file and key_file are required")
	case c.MaxConnections < 1 || c.MaxConnections > 10000:
		return errors.New("postgres_wire.max_connections must be between 1 and 10000")
	case c.IdleTimeout <= 0 || c.IdleTimeout > 24*time.Hour:
		return errors.New("postgres_wire.idle_timeout must be positive and at most 24h")
	case c.StatementTimeout <= 0 || c.StatementTimeout > time.Hour:
		return errors.New("postgres_wire.statement_timeout must be positive and at most 1h")
	}
	return nil
}

// LoadFile reads a document whose only key is `postgres_wire:`.
func LoadFile(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, errors.New("postgres_wire configuration is unavailable")
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, 1<<20+1))
	if err != nil || len(data) > 1<<20 {
		return nil, errors.New("postgres_wire configuration is unavailable or exceeds 1 MiB")
	}
	var doc struct {
		PostgresWire *Config `yaml:"postgres_wire"`
	}
	d := yaml.NewDecoder(bytes.NewReader(data))
	d.KnownFields(true)
	if err := d.Decode(&doc); err != nil || doc.PostgresWire == nil {
		return nil, fmt.Errorf("invalid postgres_wire configuration")
	}
	return doc.PostgresWire, doc.PostgresWire.Validate()
}
