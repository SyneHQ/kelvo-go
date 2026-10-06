// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package adapter

import (
	"encoding/base64"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/SYNEHQ/kelvo-go/operations"
)

var nativeReaderComponent = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$`)
var nativeReaderRegion = regexp.MustCompile(`^[a-z0-9]+(?:-[a-z0-9]+)+-[0-9]+$`)

// NativeReader identifies request-owned wrappers for existing bounded readers.
// Writes, transactions and migrations are separate capabilities.
func NativeReader(engine string) bool {
	switch engine {
	case "elasticsearch", "trino", "presto", "arrow_flight", "exasol", "spanner", "ignite", "athena", "dynamodb", "cosmosdb", "bigquery", "snowflake", "clickhouse_lambda":
		return true
	}
	return false
}

func NativeReaderCapabilities(engine string) operations.Capabilities {
	c := operations.Capabilities{Version: operations.Version, Engine: engine}
	if !NativeReader(engine) {
		return c
	}
	c.Operations = []operations.Capability{
		{Kind: operations.ConnectionTest, Idempotency: "none", Cancellation: "best_effort"},
		{Kind: operations.QueryRead, Idempotency: "none", Cancellation: "best_effort"},
	}
	if NativeWriter(engine) {
		c.Operations = append(c.Operations, operations.Capability{Kind: operations.StatementExecute, Transactions: []operations.TransactionMode{operations.TransactionAutocommit}, Idempotency: "none", Cancellation: "best_effort"})
	}
	if engine == "elasticsearch" {
		c.Operations = append(c.Operations,
			operations.Capability{Kind: operations.NativeRead, ParameterTypes: []string{"json"}, Idempotency: "none", Cancellation: "best_effort"},
			operations.Capability{Kind: operations.NativeExecute, ParameterTypes: []string{"json"}, Idempotency: "none", Cancellation: "best_effort"})
	}
	switch engine {
	case "trino", "presto", "spanner", "athena", "exasol", "ignite", "elasticsearch", "dynamodb", "cosmosdb", "arrow_flight", "snowflake", "bigquery":
		c.Operations = append(c.Operations, operations.Capability{Kind: operations.MetadataInspect, Idempotency: "none", Cancellation: "best_effort"})
	}
	return c
}

func NativeWriter(engine string) bool {
	switch engine {
	case "trino", "presto", "exasol", "ignite", "arrow_flight", "snowflake", "bigquery", "spanner", "athena", "dynamodb", "cosmosdb", "clickhouse_lambda":
		return true
	}
	return false
}

func ValidateNativeReaderProcessSource(s ConnectionSpec) error {
	if s.Engine == "bigquery" || s.Engine == "snowflake" {
		return ValidateWarehouseProcessSource(s)
	}
	if !NativeReader(s.Engine) || s.DSN != "" || len(s.Options) > 8 {
		return ErrInvalid
	}
	for _, v := range []string{s.URL, s.Username, s.Password, s.Token, s.Database, s.Schema} {
		if len(v) > 16<<10 || !utf8.ValidString(v) || strings.ContainsAny(v, "\x00\r\n") {
			return ErrInvalid
		}
	}
	scheme := "https"
	if s.Engine == "arrow_flight" {
		scheme = "grpcs"
	}
	if s.Engine == "exasol" {
		scheme = "wss"
	}
	u, err := url.Parse(s.URL)
	if err != nil || u.Scheme != scheme || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Opaque != "" || u.RawPath != "" || (u.Path != "" && u.Path != "/") || strings.ContainsAny(s.URL, "\\\x00\r\n\t ") {
		return ErrInvalid
	}
	if scheme != "https" {
		if u.Path != "" {
			return ErrInvalid
		}
		p, e := strconv.Atoi(u.Port())
		if e != nil || p < 1 || p > 65535 {
			return ErrInvalid
		}
	}
	allowed := map[string]bool{"tls_ca_pem": true, "tls_server_name": true}
	permit := func(keys ...string) {
		for _, key := range keys {
			allowed[key] = true
		}
	}
	basic := func() bool {
		return s.Username != "" && s.Password != "" && s.Token == "" && !strings.Contains(s.Username, ":")
	}
	bearer := func() bool { return s.Username == "" && s.Password == "" && s.Token != "" }
	switch s.Engine {
	case "clickhouse_lambda":
		permit("region", "function_name", "bucket_path")
		if err := validateLambdaProcessSource(s); err != nil {
			return err
		}
	case "elasticsearch":
		permit("authentication")
		if s.Schema != "" || (!basic() && !bearer()) {
			return ErrInvalid
		}
		if basic() {
			if s.Options["authentication"] != "Basic" {
				return ErrInvalid
			}
		} else if a := s.Options["authentication"]; a != "ApiKey" && a != "Bearer" {
			return ErrInvalid
		}
	case "trino", "presto":
		permit("catalog", "schema")
		if !basic() && (s.Username == "" || s.Password != "" || s.Token == "") {
			return ErrInvalid
		}
		if s.Options["catalog"] != s.Database || s.Options["schema"] != s.Schema {
			return ErrInvalid
		}
		for _, v := range []string{s.Database, s.Schema} {
			if v != "" && !nativeReaderComponent.MatchString(v) {
				return ErrInvalid
			}
		}
	case "arrow_flight":
		permit("protocol")
		if !bearer() || s.Options["protocol"] != "flightsql" || s.Database != "" || s.Schema != "" {
			return ErrInvalid
		}
	case "exasol":
		permit("schema")
		if !basic() || s.Database == "" || s.Schema != "" || s.Options["schema"] != s.Database || !nativeReaderComponent.MatchString(s.Database) {
			return ErrInvalid
		}
	case "spanner":
		permit("project", "instance", "database")
		if !bearer() || s.Options["database"] != s.Database || s.Schema != "" {
			return ErrInvalid
		}
		for _, key := range []string{"project", "instance", "database"} {
			if !nativeReaderComponent.MatchString(s.Options[key]) {
				return ErrInvalid
			}
		}
	case "ignite":
		permit("cache_name")
		if !basic() || s.Database == "" || s.Options["cache_name"] != s.Database || s.Schema != "" {
			return ErrInvalid
		}
	case "athena", "dynamodb":
		permit("region")
		if s.Username == "" || s.Password == "" || !nativeReaderRegion.MatchString(s.Options["region"]) || len(s.Options["region"]) > 64 || s.Schema != "" {
			return ErrInvalid
		}
		if s.Engine == "athena" {
			permit("workgroup", "database", "output_location")
			o, e := url.Parse(s.Options["output_location"])
			if e != nil || o.Scheme != "s3" || o.Hostname() == "" || o.User != nil || o.RawQuery != "" || o.ForceQuery || o.Fragment != "" || o.Opaque != "" || s.Options["database"] != s.Database || s.Database == "" || !nativeReaderComponent.MatchString(s.Options["workgroup"]) {
				return ErrInvalid
			}
		}
	case "cosmosdb":
		permit("database", "container", "auth")
		if !bearer() || s.Options["database"] != s.Database || s.Options["container"] != s.Schema || !nativeReaderComponent.MatchString(s.Database) || s.Schema != "" && !nativeReaderComponent.MatchString(s.Schema) {
			return ErrInvalid
		}
		switch s.Options["auth"] {
		case "aad":
		case "master_key":
			key, e := base64.StdEncoding.Strict().DecodeString(s.Token)
			if e != nil || len(key) < 32 || len(key) > 256 {
				return ErrInvalid
			}
			clear(key)
		default:
			return ErrInvalid
		}
	}
	for key, value := range s.Options {
		if !allowed[key] {
			return ErrUnsupported
		}
		if !utf8.ValidString(value) || strings.ContainsRune(value, 0) || len(value) > 64<<10 {
			return ErrInvalid
		}
	}
	return nil
}
