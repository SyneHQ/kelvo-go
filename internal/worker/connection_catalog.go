// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/SYNEHQ/kelvo-go/internal/access"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/delegation"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/sources/postgres"
	driver "github.com/go-sql-driver/mysql"
)

func delegatedDenied() error {
	return query.NewError("PERMISSION_DENIED", "On-demand query authority does not match")
}

func delegatedExecution(ctx context.Context, request query.Request) (delegation.Execution, bool, error) {
	execution, present := delegation.ExecutionFromContext(ctx)
	if request.Delegation == "" {
		if present {
			return execution, true, delegatedDenied()
		}
		return execution, false, nil
	}
	if !present || execution.Token != request.Delegation || access.Restricted(ctx) || execution.Binding.JobID == "" || execution.Binding.WorkerID == "" || execution.Binding.Owner == "" || execution.Binding.Claim == "" ||
		execution.Claims.ExpiresAt <= time.Now().Unix() || len(execution.Claims.Sources) < 1 || len(execution.Claims.Sources) > 32 {
		return execution, true, delegatedDenied()
	}
	digest, err := delegation.QueryDigest(request)
	if err != nil || digest != execution.Claims.QuerySHA256 {
		return execution, true, delegatedDenied()
	}
	ids := request.Sources
	if request.Mode == "native" {
		ids = []string{request.ConnectionID}
	}
	if len(ids) != len(execution.Claims.Sources) {
		return execution, true, delegatedDenied()
	}
	for i, source := range execution.Claims.Sources {
		if source.Alias != ids[i] || source.ConnectionID == "" {
			return execution, true, delegatedDenied()
		}
	}
	return execution, true, nil
}

// resolveConnections runs after process admission and creates an execution-only
// copy. The operator catalog and its authority fingerprint are never rebound.
func (e *Executor) resolveConnections(ctx context.Context, execution delegation.Execution, request query.Request) (*Executor, int64, error) {
	resolver := e.connectionResolvers[execution.Claims.Issuer]
	response, err := resolver.resolve(ctx, execution, request)
	if err != nil {
		return nil, 0, err
	}
	if err := validateConnectionCatalog(response, execution.Claims, request); err != nil {
		return nil, 0, err
	}
	copy := *e
	copy.Config = catalog.Config{Sources: response.Sources, ExtensionDirectory: e.Config.ExtensionDirectory}
	copy.ObjectRuntime = nil
	copy.Secrets = connectionSecrets(response.Secrets)
	copy.connectionSourceIDs = make(map[string]string, len(execution.Claims.Sources))
	for _, source := range execution.Claims.Sources {
		// The saved connection's identity survives alias changes and credential
		// rotation. A revision must never provide a new source-quota allowance.
		encoded, _ := json.Marshal([]string{execution.Claims.Issuer, execution.Claims.AppTeam, source.ConnectionID})
		copy.connectionSourceIDs[source.Alias] = "connection_" + delegation.Digest(string(encoded))[:48]
	}
	// Static source health is indexed by operator source ID. An arbitrary query
	// alias must not modify another connection's health or cardinality budget.
	copy.SourceHealth = nil
	return &copy, response.ValidUntil, nil
}

// Always configured: a missing request secret is an error, never permission to
// inherit an identically named process environment variable.
type connectionSecrets map[string]string

func (s connectionSecrets) Resolve(ctx context.Context, name string) (string, bool, error) {
	if err := ctx.Err(); err != nil {
		return "", true, err
	}
	value, ok := s[name]
	if !ok {
		return "", true, connectionUnavailable()
	}
	return value, true, nil
}

var connectionComponent = regexp.MustCompile(`^[A-Za-z0-9-]{1,128}$`)
var connectionAccount = regexp.MustCompile(`^[a-fA-F0-9]{32}$`)
var connectionDatabaseUUID = regexp.MustCompile(`^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$`)

func validateConnectionCatalog(response connectionResolution, claims delegation.Claims, request query.Request) error {
	bad := connectionUnavailable()
	if len(response.Sources) != len(claims.Sources) || len(response.Sources) == 0 || len(response.Sources) > 32 || len(response.Secrets) > 32*5 {
		return bad
	}
	refs := make(map[string]bool)
	aliases := make(map[string]bool)
	tableCount := 0
	for i, source := range response.Sources {
		selected := claims.Sources[i]
		folded := strings.ToLower(source.ID)
		if source.ID != selected.Alias || !catalog.ValidID(source.ID) || aliases[folded] || source.Type != catalog.CanonicalType(source.Type) || source.Adapter != "" || source.Path != "" ||
			source.LocalSnapshot != nil || source.ObjectSnapshot != nil || source.ParquetPaths != nil || source.Ranges != nil || source.Object != nil || source.Range != nil {
			return bad
		}
		aliases[folded] = true
		for _, ref := range []struct{ name, kind string }{{source.DSNEnv, "DSN"}, {source.URLEnv, "URL"}, {source.UsernameEnv, "USERNAME"}, {source.PasswordEnv, "PASSWORD"}, {source.TokenEnv, "TOKEN"}} {
			if ref.name == "" {
				continue
			}
			if ref.name != fmt.Sprintf("KELVO_SOURCE_REQUEST_%d_%s", i, ref.kind) || refs[ref.name] {
				return bad
			}
			value, exists := response.Secrets[ref.name]
			if !exists || value == "" || len(value) > 32<<10 || !utf8.ValidString(value) || strings.IndexByte(value, 0) >= 0 {
				return bad
			}
			refs[ref.name] = true
		}
		if !validConnectionSource(source, selected, response.Secrets) {
			return bad
		}
		if request.Mode == "native" {
			if source.Federation != nil || len(selected.Tables) != 0 {
				return bad
			}
			continue
		}
		// Every federation relation is explicitly signed. Whole-database native
		// DuckDB attachments are not accepted as a substitute for selected tables.
		if source.Federation == nil || len(selected.Tables) == 0 || len(source.Federation.Tables) != len(selected.Tables) || source.Federation.MaxScanRows != 0 || source.Federation.MaxScanBytes != 0 || source.ValidateFederation() != nil {
			return bad
		}
		tableAliases := make(map[string]bool)
		for j, table := range source.Federation.Tables {
			tableCount++
			want := selected.Tables[j]
			foldedTable := strings.ToLower(table.Name)
			if tableCount > 32 || tableAliases[foldedTable] || table.Name != want.Name || table.Table != want.Table || table.Database != want.Database || table.Schema != want.Schema {
				return bad
			}
			tableAliases[foldedTable] = true
		}
	}
	if len(refs) != len(response.Secrets) {
		return bad
	}
	return nil
}

func validConnectionSource(s catalog.Source, selected delegation.Source, secrets map[string]string) bool {
	if len(s.Options) > 4 {
		return false
	}
	for key, value := range s.Options {
		if key == "tls_ca_pem" && (s.Type == "postgres" || s.Type == "cockroachdb" || s.Type == "alloydb" || s.Type == "redshift") {
			if s.Federation != nil || postgres.ValidateSourceOptions(s.Options) != nil {
				return false
			}
			continue
		}
		if len(key) > 64 || len(value) > 256 || !utf8.ValidString(value) || strings.ContainsAny(value, "\x00\r\n") {
			return false
		}
	}
	dsnOnly := s.DSNEnv != "" && s.URLEnv == "" && s.UsernameEnv == "" && s.PasswordEnv == "" && s.TokenEnv == ""
	urlToken := s.URLEnv != "" && s.TokenEnv != "" && s.DSNEnv == "" && s.UsernameEnv == "" && s.PasswordEnv == ""
	switch s.Type {
	case "postgres", "cockroachdb", "alloydb", "redshift":
		return dsnOnly && postgres.ValidateSourceOptions(s.Options) == nil && validConnectionDSN(s.Type, secrets[s.DSNEnv], selected.Database)
	case "mysql", "mariadb", "sqlserver", "oracle":
		return dsnOnly && len(s.Options) == 0 && validConnectionDSN(s.Type, secrets[s.DSNEnv], selected.Database)
	case "mongodb":
		return dsnOnly && len(s.Options) == 1 && s.Options["database"] != "" && s.Options["database"] == selected.Database && s.Federation == nil && validConnectionDSN(s.Type, secrets[s.DSNEnv], selected.Database)
	case "databricks":
		if !urlToken || !connectionComponent.MatchString(s.Options["warehouse_id"]) || !validConnectionHTTPS(secrets[s.URLEnv]) {
			return false
		}
		origin, _ := url.Parse(secrets[s.URLEnv])
		if (origin.Path != "" && origin.Path != "/") || origin.RawQuery != "" || len(secrets[s.TokenEnv]) > 16<<10 || strings.ContainsAny(secrets[s.TokenEnv], "\r\n") {
			return false
		}
		for key, value := range s.Options {
			switch key {
			case "warehouse_id":
			case "catalog":
				if value != selected.Database || !catalog.ValidID(value) {
					return false
				}
			case "schema":
				if value != selected.Schema || !catalog.ValidID(value) {
					return false
				}
			default:
				return false
			}
		}
		return s.Options["catalog"] == selected.Database && s.Options["schema"] == selected.Schema
	case "d1":
		return urlToken && len(s.Options) == 2 && connectionAccount.MatchString(s.Options["account_id"]) && connectionDatabaseUUID.MatchString(s.Options["database_id"]) && s.Options["database_id"] == selected.Database && secrets[s.URLEnv] == "https://api.cloudflare.com" && s.Federation == nil && len(secrets[s.TokenEnv]) <= 16<<10 && !strings.ContainsAny(secrets[s.TokenEnv], "\r\n")
	case "clickhouse":
		if s.URLEnv == "" || s.DSNEnv != "" || s.TokenEnv != "" || !validConnectionHTTPS(secrets[s.URLEnv]) || s.UsernameEnv == "" || s.PasswordEnv == "" {
			return false
		}
		for key, value := range s.Options {
			if key != "arrow_compression" || (value != "none" && value != "lz4_frame") {
				return false
			}
		}
		u, _ := url.Parse(secrets[s.URLEnv])
		params, err := url.ParseQuery(u.RawQuery)
		return err == nil && (u.Path == "" || u.Path == "/") && selected.Database != "" && len(params) == 1 && len(params["database"]) == 1 && params.Get("database") == selected.Database
	default:
		return false
	}
}

func validConnectionHTTPS(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && u.Scheme == "https" && u.Hostname() != "" && u.User == nil && u.Fragment == "" && u.Opaque == ""
}

// Dynamic credentials have a narrower contract than an operator's static DSN.
// In particular they cannot select local sockets, certificate files, ambient
// authentication, or a database different from the signed request.
func validConnectionDSN(kind, raw, database string) bool {
	if database == "" || strings.ContainsAny(raw, "\x00\r\n") {
		return false
	}
	if kind == "mysql" || kind == "mariadb" {
		config, err := driver.ParseDSN(raw)
		if err != nil || config.Net != "tcp" || config.User == "" || config.Passwd == "" || config.DBName != database || config.TLSConfig != "true" || !config.ParseTime || config.Loc != time.UTC || config.AllowAllFiles || config.AllowFallbackToPlaintext || config.MultiStatements || config.InterpolateParams || config.AllowCleartextPasswords || config.AllowOldPasswords {
			return false
		}
		host, port, err := net.SplitHostPort(config.Addr)
		n, portErr := strconv.Atoi(port)
		if err != nil || portErr != nil || host == "" || strings.ContainsAny(host, "/\\ \t") || n < 1 || n > 65535 {
			return false
		}
		slash := strings.LastIndexByte(raw, '/')
		if slash < 0 || !strings.HasSuffix(raw[:slash], "@tcp("+config.Addr+")") {
			return false
		}
		_, paramsText, ok := strings.Cut(raw[slash+1:], "?")
		params, err := url.ParseQuery(paramsText)
		return ok && err == nil && exactConnectionOptions(params, map[string]string{"tls": "true", "parseTime": "true", "loc": "UTC", "time_zone": "'+00:00'", "timeout": "5s"})
	}
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" || u.User == nil || u.User.Username() == "" || u.Fragment != "" || u.Opaque != "" || strings.ContainsAny(u.Hostname(), ",/\\ \t") {
		return false
	}
	password, exists := u.User.Password()
	if !exists || password == "" {
		return false
	}
	if port := u.Port(); port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return false
		}
	}
	params, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return false
	}
	switch kind {
	case "postgres", "cockroachdb", "alloydb", "redshift":
		return u.Scheme == "postgres" && u.Path == "/"+database && exactConnectionOptions(params, map[string]string{"sslmode": "verify-full", "connect_timeout": "5"})
	case "sqlserver":
		return u.Scheme == "sqlserver" && u.Path == "" && exactConnectionOptions(params, map[string]string{"database": database, "encrypt": "true", "TrustServerCertificate": "false", "connection timeout": "5"})
	case "oracle":
		return u.Scheme == "oracle" && u.Path == "/"+database && exactConnectionOptions(params, map[string]string{"SSL": "enable", "SSL VERIFY": "true"})
	case "mongodb":
		authSource := params.Get("authSource")
		if (u.Scheme != "mongodb" && u.Scheme != "mongodb+srv") || u.Path != "/" || params.Get("tls") != "true" || authSource == "" || len(authSource) > 63 || !utf8.ValidString(authSource) || strings.ContainsAny(authSource, "/\\.\"$ \x00\r\n") || len(database) > 63 || strings.ContainsAny(database, "/\\.\"$ ") {
			return false
		}
		for key, values := range params {
			if len(values) != 1 {
				return false
			}
			switch key {
			case "tls", "authSource":
			case "replicaSet":
				if values[0] == "" {
					return false
				}
			case "directConnection":
				if values[0] != "true" || u.Scheme == "mongodb+srv" {
					return false
				}
			default:
				return false
			}
		}
		return u.Scheme != "mongodb+srv" || (u.Port() == "" && net.ParseIP(u.Hostname()) == nil)
	}
	return false
}

func exactConnectionOptions(actual url.Values, expected map[string]string) bool {
	if len(actual) != len(expected) {
		return false
	}
	for key, want := range expected {
		if len(actual[key]) != 1 || actual.Get(key) != want {
			return false
		}
	}
	return true
}
