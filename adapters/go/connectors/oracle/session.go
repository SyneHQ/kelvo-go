// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Package oracle provides verified TCPS sessions with bounded native Arrow reads.
package oracle

import (
	"context"
	"crypto/tls"
	"database/sql"
	"database/sql/driver"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"
	_ "time/tzdata" // go-ora resolves Oracle region IDs through time.LoadLocation.
	"unicode/utf8"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/adapters/go/connectors/sqlsession"
	"github.com/SYNEHQ/kelvo-go/operations"
	goora "github.com/sijms/go-ora/v2"
)

type Driver struct{}

var parameterTypes = []string{"null", "string", "int8", "int16", "int32", "int64", "uint8", "uint16", "uint32", "uint64", "decimal128", "float32", "float64", "binary", "date", "timestamp"}

func (Driver) Capabilities() operations.Capabilities {
	return operations.Capabilities{Version: operations.Version, Engine: "oracle", Operations: []operations.Capability{
		{Kind: operations.QueryRead, ParameterTypes: append([]string(nil), parameterTypes...), Idempotency: "none", Cancellation: "best_effort"},
		{Kind: operations.StatementExecute, ParameterTypes: append([]string(nil), parameterTypes...), Transactions: []operations.TransactionMode{operations.TransactionRequired, operations.TransactionAutocommit}, IsolationLevels: []string{"default"}, Idempotency: "none", Cancellation: "best_effort"},
		{Kind: operations.ConnectionTest, Idempotency: "none", Cancellation: "best_effort"},
		{Kind: operations.MetadataInspect, Idempotency: "none", Cancellation: "best_effort"},
	}}
}

func connectionDSN(c adapter.Connection) (string, *tls.Config, error) {
	if c.Engine != "oracle" || c.TenantID == "" || c.ConnectionID == "" || c.Revision == "" || c.Host == "" || c.Port < 1 || c.Port > 65535 || c.Namespace == "" || c.Username == "" || c.Password == "" || c.Endpoint != "" {
		return "", nil, adapter.ErrInvalid
	}
	for _, value := range []string{c.Host, c.Namespace, c.Username, c.Password, c.Schema} {
		if len(value) > 32<<10 || !utf8.ValidString(value) || strings.ContainsAny(value, "\x00\r\n") {
			return "", nil, adapter.ErrInvalid
		}
	}
	if strings.ContainsAny(c.Host, "/\\, \t") || strings.ContainsAny(c.Namespace, "/\\") || len(c.Schema) > 128 || strings.EqualFold(c.Username, "SYS") {
		// The upstream driver silently promotes username SYS to SYSDBA.
		return "", nil, adapter.ErrUnsupported
	}
	config := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: c.Host}
	if c.TLS != nil {
		config = c.TLS.Clone()
	}
	if config.InsecureSkipVerify || config.MaxVersion != 0 && config.MaxVersion < tls.VersionTLS12 {
		return "", nil, adapter.ErrInvalid
	}
	config.MinVersion = max(config.MinVersion, tls.VersionTLS12)
	if config.ServerName == "" {
		config.ServerName = c.Host
	}
	endpoint := url.URL{Scheme: "oracle", Host: net.JoinHostPort(c.Host, strconv.Itoa(c.Port)), User: url.UserPassword(c.Username, c.Password), Path: "/" + c.Namespace, RawQuery: url.Values{"SSL": {"enable"}, "SSL VERIFY": {"true"}, "CONNECTION TIMEOUT": {"5"}, "PREFETCH_ROWS": {"128"}}.Encode()}
	return endpoint.String(), config, nil
}

func (Driver) Open(ctx context.Context, c adapter.Connection) (adapter.Session, error) {
	if ctx == nil {
		return nil, adapter.ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	dsn, config, err := connectionDSN(c)
	if err != nil {
		return nil, err
	}
	base := goora.NewConnector(dsn).(*goora.OracleConnector)
	base.WithTLSConfig(config)
	pool := sql.OpenDB(&sessionConnector{Connector: base, schema: c.Schema})
	pool.SetMaxOpenConns(1)
	pool.SetMaxIdleConns(1)
	pool.SetConnMaxLifetime(5 * time.Minute)
	if err := pool.PingContext(ctx); err != nil {
		_ = pool.Close()
		return nil, err
	}
	return &Session{Session: &sqlsession.Session{Pool: pool, Engine: "oracle"}, database: c.Namespace, schema: c.Schema}, nil
}

// Initialize every replacement connection too. Pool reuse must not lose the
// signed schema selection or parse decimal parameters under a different NLS.
type sessionConnector struct {
	driver.Connector
	schema string
}

func (c *sessionConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	exec, ok := conn.(driver.ExecerContext)
	if !ok {
		_ = conn.Close()
		return nil, adapter.ErrUnsupported
	}
	statements := []string{"ALTER SESSION SET TIME_ZONE = '+00:00'", "ALTER SESSION SET NLS_NUMERIC_CHARACTERS = '.,'"}
	if c.schema != "" {
		statements = append(statements, `ALTER SESSION SET CURRENT_SCHEMA = "`+strings.ReplaceAll(c.schema, `"`, `""`)+`"`)
	}
	for _, statement := range statements {
		if _, err := exec.ExecContext(ctx, statement, nil); err != nil {
			_ = conn.Close()
			return nil, err
		}
	}
	return conn, nil
}

type Session struct {
	*sqlsession.Session
	database, schema string
}

func (s *Session) Test(ctx context.Context) error {
	if ctx == nil || s == nil || s.Session == nil || s.Pool == nil {
		return adapter.ErrInvalid
	}
	return s.Pool.PingContext(ctx)
}
func (s *Session) Execute(ctx context.Context, change adapter.Change) (adapter.ChangeResult, error) {
	if ctx == nil {
		return adapter.ChangeResult{Outcome: "failed"}, adapter.ErrInvalid
	}
	if s == nil || s.Session == nil || change.Role != "" || change.Isolation != "" && change.Isolation != "default" {
		return adapter.ChangeResult{Outcome: "failed"}, adapter.ErrUnsupported
	}
	// go-ora implements database/sql BeginTx only at default isolation. DML
	// transactions use the shared receipt/rollback path; implicit-commit DDL
	// is rejected there when an atomic transaction is required.
	if len(change.Parameters) != 0 && len(change.Parameters) != len(change.Statements) {
		return adapter.ChangeResult{Outcome: "failed"}, adapter.ErrInvalid
	}
	statements := make([]sqlsession.Statement, len(change.Statements))
	for i, statement := range change.Statements {
		statements[i].SQL = statement
		if len(change.Parameters) != 0 {
			values, err := oracleParameters(change.Parameters[i])
			if err != nil {
				return adapter.ChangeResult{Outcome: "failed"}, err
			}
			statements[i].Parameters = values
		}
	}
	result, err := sqlsession.ExecuteStatements(ctx, s.Pool, "oracle", statements, sqlsession.Options{Transaction: change.Transaction, Isolation: change.Isolation})
	return adapter.ChangeResult{Outcome: string(result.Outcome), Completed: result.Completed, Attempted: result.Attempted, AffectedRows: result.AffectedRows}, err
}
