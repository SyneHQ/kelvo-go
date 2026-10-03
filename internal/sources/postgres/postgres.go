// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package postgres

import (
	"crypto/tls"
	"database/sql"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/sources/sqlnative"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgconn/ctxwatch"
	"github.com/jackc/pgx/v5/stdlib"
)

type Engine = sqlnative.Engine

func New(c catalog.Config, l query.Limits) (*Engine, error) { return family(c, l, "postgres") }
func NewPostgreSQL(c catalog.Config, l query.Limits) (*Engine, error) {
	return family(c, l, "postgresql")
}
func NewCockroachDB(c catalog.Config, l query.Limits) (*Engine, error) {
	return family(c, l, "cockroachdb")
}
func NewAlloyDB(c catalog.Config, l query.Limits) (*Engine, error)  { return family(c, l, "alloydb") }
func NewRedshift(c catalog.Config, l query.Limits) (*Engine, error) { return family(c, l, "redshift") }
func family(c catalog.Config, l query.Limits, kind string) (*Engine, error) {
	return sqlnative.New(c, l, sqlnative.Dialect{SourceType: kind, DriverName: "pgx", ErrorCode: sourceErrorCode, ReadOnlyOption: true, AllowDollarParams: true, ValidateDSN: validateDSN, OpenDB: openDB})
}
func configError() error {
	return query.NewError("CONFIGURATION_ERROR", "PostgreSQL requires an explicit TCP URL, user/password/database and sslmode=verify-full without ambient or unsafe options")
}

func validateDSN(dsn string) error {
	if len(dsn) > 32<<10 || strings.ContainsAny(dsn, "\x00\r\n") {
		return configError()
	}
	// pgx honors PG* variables before constructing its config. Reject them before
	// it can read a service/pass/certificate file or inherit another identity.
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(name, "PG") {
			return configError()
		}
	}
	u, err := url.Parse(dsn)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") || u.User == nil || u.User.Username() == "" || u.Hostname() == "" || u.Fragment != "" || u.Opaque != "" || strings.ContainsAny(u.Hostname(), ",/\\ \t") || strings.ContainsAny(u.User.Username(), "\x00\r\n") {
		return configError()
	}
	password, hasPassword := u.User.Password()
	if !hasPassword || password == "" || strings.ContainsRune(password, 0) || len(u.Path) < 2 || strings.ContainsAny(u.Path[1:], "/\x00") {
		return configError()
	}
	port := u.Port()
	if port != "" {
		p, err := strconv.Atoi(port)
		if err != nil || p < 1 || p > 65535 {
			return configError()
		}
	}
	if strings.Contains(u.Hostname(), ":") && net.ParseIP(u.Hostname()) == nil {
		return configError()
	}
	params, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return configError()
	}
	for key, values := range params {
		if len(values) != 1 {
			return configError()
		}
		switch key {
		case "sslmode":
			if values[0] != "verify-full" {
				return configError()
			}
		case "connect_timeout":
			seconds, err := strconv.Atoi(values[0])
			if err != nil || seconds < 1 || seconds > 60 {
				return configError()
			}
		default:
			return configError()
		}
	}
	if params.Get("sslmode") != "verify-full" {
		return configError()
	}
	return nil
}

func parseConfig(dsn string) (*pgx.ConnConfig, error) {
	if err := validateDSN(dsn); err != nil {
		return nil, err
	}
	u, _ := url.Parse(dsn)
	params := u.Query()
	// These values are generated here, never accepted from a source DSN. Clear
	// home-directory client credentials and use only the system CA trust store.
	params.Set("sslrootcert", "system")
	params.Set("sslcert", "")
	params.Set("sslkey", "")
	params.Set("passfile", "")
	params.Set("application_name", "kelvo-go")
	params.Set("timezone", "UTC")
	u.RawQuery = params.Encode()
	config, err := pgx.ParseConfigWithOptions(u.String(), pgx.ParseConfigOptions{ParseConfigOptions: pgconn.ParseConfigOptions{ConnStringAllowedKeys: []string{"host", "port", "user", "password", "database", "sslmode", "connect_timeout", "sslrootcert", "sslcert", "sslkey", "passfile", "application_name", "timezone"}}})
	if err != nil || config.TLSConfig == nil || config.TLSConfig.InsecureSkipVerify || len(config.Fallbacks) != 0 {
		return nil, configError()
	}
	config.TLSConfig.MinVersion = tls.VersionTLS12
	// Socket closure alone can leave a server query running. Send pgx's
	// connection-keyed CancelRequest before the worker's bounded shutdown
	// grace expires, with a socket deadline as the fallback.
	config.BuildContextWatcherHandler = func(conn *pgconn.PgConn) ctxwatch.Handler {
		return &pgconn.CancelRequestContextWatcherHandler{Conn: conn, CancelRequestDelay: 0, DeadlineDelay: 500 * time.Millisecond}
	}
	return config, nil
}
func openDB(dsn string) (*sql.DB, error) {
	config, err := parseConfig(dsn)
	if err != nil {
		return nil, err
	}
	return stdlib.OpenDB(*config), nil
}
