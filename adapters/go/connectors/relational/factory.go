// Package relational composes PostgreSQL and MySQL operation sessions. Each
// session owns one pool built from freshly authorized connection credentials.
package relational

import (
	"context"
	"crypto/tls"
	"database/sql"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/adapters/go/connectors/sqlsession"
	"github.com/SYNEHQ/kelvo-go/operations"
	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgconn/ctxwatch"
	"github.com/jackc/pgx/v5/stdlib"
	mssql "github.com/microsoft/go-mssqldb"
)

type Driver struct{ engine string }

func NewPostgreSQL() *Driver { return &Driver{engine: "postgresql"} }
func NewMySQL() *Driver      { return &Driver{engine: "mysql"} }
func NewSQLServer() *Driver  { return &Driver{engine: "sqlserver"} }
func NewMariaDB() *Driver    { return &Driver{engine: "mariadb"} }

func (d *Driver) Capabilities() operations.Capabilities {
	changes, _ := sqlsession.NewDriver(d.engine, func(context.Context, adapter.Connection) (*sql.DB, error) { return nil, adapter.ErrUnsupported })
	capabilities := changes.Capabilities()
	read := capabilities.Operations[0]
	read.Kind = operations.QueryRead
	read.Transactions = nil
	read.IsolationLevels = nil
	read.Roles = false
	capabilities.Operations = append(capabilities.Operations, read)
	for _, kind := range []operations.Kind{operations.ConnectionTest, operations.MetadataInspect} {
		capabilities.Operations = append(capabilities.Operations, operations.Capability{Kind: kind, Idempotency: "none", Cancellation: "best_effort"})
	}
	if d.engine == "postgresql" {
		for _, kind := range []operations.Kind{operations.IngestionInstall, operations.IngestionState, operations.IngestionCommit} {
			capabilities.Operations = append(capabilities.Operations, operations.Capability{Kind: kind, Idempotency: "none", Cancellation: "best_effort"})
		}
	}
	capabilities.Operations = append(capabilities.Operations,
		operations.Capability{Kind: operations.MigrationStatus, Idempotency: "none", Cancellation: "best_effort"},
		operations.Capability{Kind: operations.MigrationApply, Transactions: []operations.TransactionMode{operations.TransactionAutocommit}, Idempotency: "none", Cancellation: "best_effort"})
	if d.engine == "postgresql" || d.engine == "mysql" || d.engine == "mariadb" {
		for _, kind := range []operations.Kind{operations.WatchInstall, operations.WatchRead, operations.WatchAck, operations.WatchRemove} {
			capabilities.Operations = append(capabilities.Operations, operations.Capability{Kind: kind, Idempotency: "none", Cancellation: "best_effort"})
		}
	}
	return capabilities
}

func (d *Driver) Open(ctx context.Context, connection adapter.Connection) (adapter.Session, error) {
	if ctx == nil || ctx.Err() != nil || connection.Engine != d.engine || !validConnection(connection) {
		return nil, adapter.ErrInvalid
	}
	var pool *sql.DB
	var openMigrationPool func(context.Context) (*sql.DB, error)
	switch d.engine {
	case "postgresql":
		config, err := postgresConfig(connection)
		if err != nil {
			return nil, err
		}
		pool = stdlib.OpenDB(*config)
	case "mysql", "mariadb":
		config, err := mysqlConfig(connection)
		if err != nil {
			return nil, err
		}
		connector, err := mysqldriver.NewConnector(config)
		if err != nil {
			return nil, adapter.ErrInvalid
		}
		pool = sql.OpenDB(connector)
		// Migration scripts have their own explicit multi-statement pool. The
		// ordinary query, watcher and statement pool keeps this feature disabled.
		migrationConfig := config.Clone()
		migrationConfig.MultiStatements = true
		openMigrationPool = func(ctx context.Context) (*sql.DB, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			connector, err := mysqldriver.NewConnector(migrationConfig.Clone())
			if err != nil {
				return nil, adapter.ErrInvalid
			}
			migrationPool := sql.OpenDB(connector)
			migrationPool.SetMaxOpenConns(1)
			migrationPool.SetMaxIdleConns(0)
			return migrationPool, nil
		}
	case "sqlserver":
		config, err := sqlserverConfig(connection)
		if err != nil {
			return nil, err
		}
		pool = sql.OpenDB(mssql.NewConnectorConfig(config))
	default:
		return nil, adapter.ErrUnsupported
	}
	pool.SetMaxOpenConns(1)
	pool.SetMaxIdleConns(1)
	pool.SetConnMaxLifetime(5 * time.Minute)
	pool.SetConnMaxIdleTime(time.Minute)
	if err := pool.PingContext(ctx); err != nil {
		_ = pool.Close()
		return nil, err
	}
	session := &Session{Session: &sqlsession.Session{Pool: pool, Engine: d.engine}, database: connection.Namespace, schema: connection.Schema, openMigrationPool: openMigrationPool}
	if d.engine == "sqlserver" && connection.Schema != "" {
		if err := pool.QueryRowContext(ctx, "SELECT SCHEMA_NAME()").Scan(&session.defaultSchema); err != nil {
			_ = pool.Close()
			return nil, err
		}
	}
	return session, nil
}

func validConnection(c adapter.Connection) bool {
	if c.TenantID == "" || c.ConnectionID == "" || c.Revision == "" || c.Host == "" || c.Username == "" || c.Password == "" || c.Namespace == "" || c.Port < 1 || c.Port > 65535 {
		return false
	}
	for _, value := range []string{c.TenantID, c.ConnectionID, c.Revision, c.Host, c.Username, c.Password, c.Namespace, c.Schema} {
		if len(value) > 32<<10 || !utf8.ValidString(value) || strings.ContainsAny(value, "\x00\r\n") {
			return false
		}
	}
	return len(c.Host) <= 253 && len(c.Namespace) <= 128 && len(c.Schema) <= 128 && !strings.ContainsAny(c.Host, "/,\\ \t") && (!strings.Contains(c.Host, ":") || net.ParseIP(c.Host) != nil) && !strings.Contains(c.Namespace, "/")
}

func sourceTLS(c adapter.Connection) (*tls.Config, error) {
	config := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: c.Host}
	if c.TLS != nil {
		config = c.TLS.Clone()
	}
	if config.InsecureSkipVerify || config.MaxVersion != 0 && config.MaxVersion < tls.VersionTLS12 {
		return nil, adapter.ErrInvalid
	}
	if config.MinVersion < tls.VersionTLS12 {
		config.MinVersion = tls.VersionTLS12
	}
	if config.ServerName == "" {
		config.ServerName = c.Host
	}
	if len(config.ServerName) > 253 || strings.ContainsAny(config.ServerName, "\x00\r\n/,\\ \t") {
		return nil, adapter.ErrInvalid
	}
	return config, nil
}

func postgresConfig(c adapter.Connection) (*pgx.ConnConfig, error) {
	if !validConnection(c) {
		return nil, adapter.ErrInvalid
	}
	// pgx reads PG* variables before parsing, including paths to credential
	// files. Reject ambient database identity before the parser runs.
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(name, "PG") {
			return nil, adapter.ErrInvalid
		}
	}
	tlsConfig, err := sourceTLS(c)
	if err != nil {
		return nil, err
	}
	endpoint := url.URL{Scheme: "postgresql", Host: net.JoinHostPort(c.Host, strconv.Itoa(c.Port)), User: url.UserPassword(c.Username, c.Password), Path: "/" + c.Namespace}
	params := url.Values{"sslmode": {"verify-full"}, "sslrootcert": {"system"}, "sslcert": {""}, "sslkey": {""}, "passfile": {""}, "connect_timeout": {"10"}, "application_name": {"kelvo-go"}, "timezone": {"UTC"}}
	endpoint.RawQuery = params.Encode()
	config, err := pgx.ParseConfigWithOptions(endpoint.String(), pgx.ParseConfigOptions{ParseConfigOptions: pgconn.ParseConfigOptions{ConnStringAllowedKeys: []string{"host", "port", "user", "password", "database", "sslmode", "sslrootcert", "sslcert", "sslkey", "passfile", "connect_timeout", "application_name", "timezone"}}})
	if err != nil || config.TLSConfig == nil || config.TLSConfig.InsecureSkipVerify || len(config.Fallbacks) != 0 {
		return nil, adapter.ErrInvalid
	}
	config.TLSConfig = tlsConfig
	if c.Schema != "" {
		config.RuntimeParams["search_path"] = pgx.Identifier{c.Schema}.Sanitize()
	}
	config.BuildContextWatcherHandler = func(conn *pgconn.PgConn) ctxwatch.Handler {
		return &pgconn.CancelRequestContextWatcherHandler{Conn: conn, CancelRequestDelay: 0, DeadlineDelay: 500 * time.Millisecond}
	}
	return config, nil
}

type quietLogger struct{}

func (quietLogger) Print(...any) {}

func mysqlConfig(c adapter.Connection) (*mysqldriver.Config, error) {
	if !validConnection(c) || len(c.Namespace) > 64 || c.Schema != "" && c.Schema != c.Namespace {
		return nil, adapter.ErrInvalid
	}
	tlsConfig, err := sourceTLS(c)
	if err != nil {
		return nil, err
	}
	config := mysqldriver.NewConfig()
	config.Net = "tcp"
	config.Addr = net.JoinHostPort(c.Host, strconv.Itoa(c.Port))
	config.User = c.Username
	config.Passwd = c.Password
	config.DBName = c.Namespace
	config.TLS = tlsConfig
	config.ParseTime = true
	config.Loc = time.UTC
	config.Params = map[string]string{"time_zone": "'+00:00'"}
	config.Timeout = 10 * time.Second
	config.ReadTimeout = 30 * time.Second
	config.WriteTimeout = 30 * time.Second
	config.MaxAllowedPacket = 4 << 20
	config.AllowAllFiles = false
	config.AllowFallbackToPlaintext = false
	config.MultiStatements = false
	config.InterpolateParams = false
	config.AllowCleartextPasswords = false
	config.AllowOldPasswords = false
	config.Logger = quietLogger{}
	return config, nil
}
