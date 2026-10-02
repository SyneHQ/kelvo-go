// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package mysql

import (
	"crypto/tls"
	"database/sql"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/sources/sqlnative"
	driver "github.com/go-sql-driver/mysql"
)

type Engine = sqlnative.Engine

func New(c catalog.Config, l query.Limits) (*Engine, error)        { return family(c, l, "mysql") }
func NewMariaDB(c catalog.Config, l query.Limits) (*Engine, error) { return family(c, l, "mariadb") }
func family(c catalog.Config, l query.Limits, kind string) (*Engine, error) {
	return sqlnative.New(c, l, sqlnative.Dialect{SourceType: kind, DriverName: "mysql", ReadOnlyOption: true, ValidateDSN: validateDSN, OpenDB: func(dsn string) (*sql.DB, error) {
		config, err := executionConfig(dsn, l, kind)
		if err != nil {
			return nil, err
		}
		connector, err := driver.NewConnector(config)
		if err != nil {
			return nil, configError()
		}
		return sql.OpenDB(connector), nil
	}})
}

func executionConfig(dsn string, limits query.Limits, kind string) (*driver.Config, error) {
	config, err := parseConfig(dsn)
	if err != nil {
		return nil, err
	}
	config.MaxAllowedPacket = int(min(int64(16<<20), int64(limits.MemoryMB)<<18))
	config.Logger = quietLogger{}
	if kind == "mysql" {
		// Closing a cancelled MySQL connection does not promptly interrupt every
		// server operation. Bound read-only SELECT execution at the server too.
		// Round up: zero disables the server timer. sqlguard rejects optimizer
		// hints, so a native request cannot override this session-owned setting.
		milliseconds := (limits.Timeout + time.Millisecond - 1) / time.Millisecond
		config.Params["max_execution_time"] = strconv.FormatInt(int64(milliseconds), 10)
	}
	return config, nil
}

type quietLogger struct{}

func (quietLogger) Print(...any) {}
func configError() error {
	return query.NewError("CONFIGURATION_ERROR", "MySQL requires explicit TCP credentials/database, tls=true, parseTime=true, loc=UTC and time_zone='+00:00' without unsafe or duplicate options")
}
func validateDSN(dsn string) error { _, err := parseConfig(dsn); return err }
func parseConfig(dsn string) (*driver.Config, error) {
	if len(dsn) > 32<<10 || strings.ContainsAny(dsn, "\x00\r\n") {
		return nil, configError()
	}
	slash := strings.LastIndexByte(dsn, '/')
	if slash < 0 {
		return nil, configError()
	}
	_, raw, ok := strings.Cut(dsn[slash+1:], "?")
	if !ok {
		return nil, configError()
	}
	params := map[string]string{}
	for _, pair := range strings.Split(raw, "&") {
		key, encoded, found := strings.Cut(pair, "=")
		if !found {
			return nil, configError()
		}
		if _, exists := params[key]; exists {
			return nil, configError()
		}
		value, err := url.QueryUnescape(encoded)
		if err != nil {
			return nil, configError()
		}
		switch key {
		case "tls", "parseTime":
			if value != "true" {
				return nil, configError()
			}
		case "loc":
			if value != "UTC" {
				return nil, configError()
			}
		case "time_zone":
			if value != "'+00:00'" {
				return nil, configError()
			}
		case "charset":
			if value != "utf8mb4" {
				return nil, configError()
			}
		case "timeout", "readTimeout", "writeTimeout":
			d, err := time.ParseDuration(value)
			if err != nil || d <= 0 || d > time.Minute {
				return nil, configError()
			}
		default:
			return nil, configError()
		}
		params[key] = value
	}
	if params["tls"] != "true" || params["parseTime"] != "true" || params["loc"] != "UTC" || params["time_zone"] != "'+00:00'" {
		return nil, configError()
	}
	config, err := driver.ParseDSN(dsn)
	if err != nil || config.Net != "tcp" || config.User == "" || config.Passwd == "" || config.DBName == "" || len(config.DBName) > 64 || strings.ContainsRune(config.DBName, 0) || config.TLS == nil || config.TLS.InsecureSkipVerify || config.TLSConfig != "true" || !config.ParseTime || config.Loc != time.UTC || config.AllowAllFiles || config.AllowFallbackToPlaintext || config.MultiStatements || config.InterpolateParams || config.AllowOldPasswords || config.AllowCleartextPasswords {
		return nil, configError()
	}
	host, port, err := net.SplitHostPort(config.Addr)
	p, parseErr := strconv.Atoi(port)
	if err != nil || parseErr != nil || host == "" || strings.ContainsAny(host, "/\\ \t") || p < 1 || p > 65535 {
		return nil, configError()
	}
	// Require an explicit tcp(address) component; driver defaults must never
	// silently redirect an incomplete tenant DSN to localhost.
	if !strings.HasSuffix(dsn[:slash], "@tcp("+config.Addr+")") {
		return nil, configError()
	}
	config.TLS.MinVersion = tls.VersionTLS12
	return config, nil
}
