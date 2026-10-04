// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package catalog

import (
	"errors"
	"regexp"
	"strconv"

	federationapi "github.com/SYNEHQ/kelvo-go/federation"
)

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
	if s.Adapter != "" || s.Object != nil || s.Range != nil || s.Path != "" || !ValidID(s.ID) {
		return errors.New("native federation requires a registered network source")
	}
	for _, name := range []string{s.DSNEnv, s.URLEnv, s.UsernameEnv, s.PasswordEnv, s.TokenEnv} {
		if name != "" && ValidateEnvironment(name) != nil {
			return errors.New("federation source environment reference is not permitted")
		}
	}
	if len(s.Options) > 16 {
		return errors.New("federation source has too many options")
	}
	for key, value := range s.Options {
		if len(key) > 64 || len(value) > 4096 {
			return errors.New("federation source option exceeds size limit")
		}
	}
	var custom federationapi.Driver
	switch s.Type {
	case "postgres", "mysql", "sqlserver", "oracle":
		if s.DSNEnv == "" || s.URLEnv != "" || s.UsernameEnv != "" || s.PasswordEnv != "" || s.TokenEnv != "" || len(s.Options) != 0 {
			return errors.New("relational native federation requires only a dsn_env credential reference")
		}
	case "clickhouse":
		if s.URLEnv == "" || s.DSNEnv != "" || s.TokenEnv != "" {
			return errors.New("ClickHouse federation requires URL and optional username/password environment references")
		}
		for key, value := range s.Options {
			if key != "arrow_compression" || (value != "none" && value != "lz4_frame") {
				return errors.New("ClickHouse federation option is unsupported")
			}
		}
	case "snowflake", "databricks", "bigquery":
		if s.URLEnv == "" || s.TokenEnv == "" || s.DSNEnv != "" || s.UsernameEnv != "" || s.PasswordEnv != "" {
			return errors.New("cloud native federation requires URL and token environment references")
		}
		if err := validateFederationOptions(s); err != nil {
			return err
		}
	case "arrow_flight":
		if s.URLEnv == "" || s.TokenEnv == "" || s.DSNEnv != "" || s.UsernameEnv != "" || s.PasswordEnv != "" {
			return errors.New("Flight SQL federation requires only URL and token environment references")
		}
		// Flight SQL defines a transport, not a SQL dialect. Require the
		// operator to select the narrow quoted-identifier profile explicitly.
		if len(s.Options) != 2 || s.Options["protocol"] != "flightsql" || s.Options["federation_dialect"] != "ansi" {
			return errors.New("Flight SQL federation requires protocol flightsql and federation_dialect ansi")
		}
	default:
		var ok bool
		custom, ok = federationapi.Lookup(s.Type)
		if !ok {
			return errors.New("source has no registered native federation adapter")
		}
	}
	if len(f.Tables) == 0 || len(f.Tables) > 32 || f.MaxScanRows < 0 || f.MaxScanRows > 100000000 || f.MaxScanBytes < 0 || f.MaxScanBytes > 1<<40 || (f.MaxScanBytes > 0 && f.MaxScanBytes < 1024) {
		return errors.New("federation requires 1 to 32 tables and bounded scan limits")
	}
	seen := make(map[string]bool, len(f.Tables))
	for _, table := range f.Tables {
		if !ValidID(table.Name) || !ValidID(table.Table) || seen[table.Name] {
			return errors.New("federation tables require unique aliases and plain table identifiers")
		}
		if custom != nil {
			if (table.Database != "" && !federationNamespace.MatchString(table.Database)) || (table.Schema != "" && !ValidID(table.Schema)) {
				return errors.New("custom federation namespaces must be plain identifiers")
			}
			if err := validateCustomFederation(custom, s, table); err != nil {
				return err
			}
		} else if s.Type == "arrow_flight" {
			if !ValidID(table.Schema) || table.Database != "" {
				return errors.New("Flight SQL federation requires schema and omits database")
			}
		} else if s.Type == "postgres" || s.Type == "oracle" {
			// PostgreSQL connections select one database through the DSN. Never
			// reinterpret a database name as a schema or depend on search_path.
			if !ValidID(table.Schema) || table.Database != "" {
				return errors.New("PostgreSQL and Oracle federation require schema and omit database")
			}
		} else if s.Type == "clickhouse" || s.Type == "mysql" {
			if !ValidID(table.Database) || table.Schema != "" {
				return errors.New("ClickHouse and MySQL federation require database and omit schema")
			}
		} else if s.Type == "bigquery" {
			if !bigQueryProject.MatchString(table.Database) || !ValidID(table.Schema) {
				return errors.New("BigQuery federation requires an explicit project and dataset")
			}
		} else if !ValidID(table.Database) || !ValidID(table.Schema) {
			return errors.New("SQL Server, Snowflake and Databricks federation require database and schema")
		}
		seen[table.Name] = true
	}
	return nil
}

var federationNamespace = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_-]{0,127}$`)
var providerComponent = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
var databricksWarehouse = regexp.MustCompile(`^[A-Za-z0-9-]{1,128}$`)
var bigQueryProject = regexp.MustCompile(`^[a-z][a-z0-9-]{4,28}[a-z0-9]$`)

func validateFederationOptions(s Source) error {
	for key, value := range s.Options {
		allowed := false
		switch s.Type {
		case "snowflake":
			allowed = key == "warehouse" || key == "role" || key == "database" || key == "schema" || key == "token_type"
			if key == "token_type" {
				if value != "OAUTH" && value != "KEYPAIR_JWT" && value != "PROGRAMMATIC_ACCESS_TOKEN" {
					return errors.New("unsupported Snowflake token_type")
				}
			} else if !ValidID(value) {
				return errors.New("Snowflake federation options require plain identifiers")
			}
		case "databricks":
			allowed = key == "warehouse_id" || key == "catalog" || key == "schema"
			if (key == "warehouse_id" && !databricksWarehouse.MatchString(value)) || (key != "warehouse_id" && !ValidID(value)) {
				return errors.New("Databricks federation option is invalid")
			}
		case "bigquery":
			allowed = key == "project" || key == "location" || key == "dataset" || key == "maximum_bytes_billed"
			if key == "maximum_bytes_billed" {
				if n, err := strconv.ParseInt(value, 10, 64); err != nil || n < 1 {
					return errors.New("BigQuery federation billing limit is invalid")
				}
			} else if !providerComponent.MatchString(value) {
				return errors.New("BigQuery federation option is invalid")
			}
		}
		if !allowed {
			return errors.New("native federation source option is unsupported")
		}
	}
	if s.Type == "databricks" && s.Options["warehouse_id"] == "" {
		return errors.New("Databricks federation requires warehouse_id")
	}
	if s.Type == "bigquery" && (s.Options["project"] == "" || s.Options["location"] == "") {
		return errors.New("BigQuery federation requires project and location")
	}
	return nil
}

func validateCustomFederation(driver federationapi.Driver, source Source, table FederationTable) (err error) {
	defer func() {
		if recover() != nil {
			err = errors.New("custom federation adapter rejected its configuration")
		}
	}()
	options := make(map[string]string, len(source.Options))
	for key, value := range source.Options {
		options[key] = value
	}
	if err := driver.Validate(federationapi.Source{ID: source.ID, Type: source.Type, DSNEnv: source.DSNEnv, URLEnv: source.URLEnv, UsernameEnv: source.UsernameEnv, PasswordEnv: source.PasswordEnv, TokenEnv: source.TokenEnv, Options: options}, federationapi.Table{Name: table.Name, Database: table.Database, Schema: table.Schema, Table: table.Table}); err != nil {
		return errors.New("custom federation adapter rejected its configuration")
	}
	return nil
}
