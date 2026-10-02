// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package catalog

import "errors"

// FederationConfig exposes only operator-selected remote tables to DuckDB.
// Scan limits fail on overflow; they never truncate a relation before a join.
type FederationConfig struct {
	Tables       []FederationTable `json:"tables" yaml:"tables"`
	MaxScanRows  int64             `json:"max_scan_rows,omitempty" yaml:"max_scan_rows,omitempty"`
	MaxScanBytes int64             `json:"max_scan_bytes,omitempty" yaml:"max_scan_bytes,omitempty"`
}

type FederationTable struct {
	Name     string `json:"name" yaml:"name"`
	Database string `json:"database,omitempty" yaml:"database,omitempty"`
	Schema   string `json:"schema,omitempty" yaml:"schema,omitempty"`
	Table    string `json:"table" yaml:"table"`
}

func (s Source) ValidateFederation() error {
	f := s.Federation
	if f == nil {
		return nil
	}
	if (s.Type != "clickhouse" && s.Type != "postgres" && s.Type != "mysql") || s.Adapter != "" || s.Object != nil || s.Range != nil {
		return errors.New("custom federation requires a built-in ClickHouse, PostgreSQL or MySQL source")
	}
	if s.Type != "clickhouse" && (s.DSNEnv == "" || s.URLEnv != "" || s.UsernameEnv != "" || s.PasswordEnv != "" || s.TokenEnv != "" || s.Path != "" || len(s.Options) != 0) {
		return errors.New("relational custom federation requires only a dsn_env credential reference")
	}
	if len(f.Tables) == 0 || len(f.Tables) > 32 || f.MaxScanRows < 0 || f.MaxScanRows > 100000000 || f.MaxScanBytes < 0 || f.MaxScanBytes > 1<<40 || (f.MaxScanBytes > 0 && f.MaxScanBytes < 1024) {
		return errors.New("federation requires 1 to 32 tables and bounded scan limits")
	}
	seen := make(map[string]bool, len(f.Tables))
	for _, table := range f.Tables {
		if !ValidID(table.Name) || !ValidID(table.Table) || seen[table.Name] {
			return errors.New("federation tables require unique aliases and plain table identifiers")
		}
		if s.Type == "postgres" {
			// PostgreSQL connections select one database through the DSN. Never
			// reinterpret a database name as a schema or depend on search_path.
			if !ValidID(table.Schema) || table.Database != "" {
				return errors.New("PostgreSQL federation requires schema and omits database")
			}
		} else if !ValidID(table.Database) || table.Schema != "" {
			return errors.New("ClickHouse and MySQL federation require database and omit schema")
		}
		seen[table.Name] = true
	}
	return nil
}
