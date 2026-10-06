// Package clickhouse uses ClickHouse's HTTPS protocol and native Arrow stream.
package clickhouse

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	native "github.com/SYNEHQ/kelvo-go/internal/sources/clickhouse"
	"github.com/SYNEHQ/kelvo-go/internal/sources/sqlguard"
	"github.com/SYNEHQ/kelvo-go/migration"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/apache/arrow-go/v18/arrow"
)

type Driver struct{}

func (Driver) Capabilities() operations.Capabilities {
	return operations.Capabilities{Version: operations.Version, Engine: "clickhouse", Operations: []operations.Capability{
		{Kind: operations.QueryRead, Idempotency: "none", Cancellation: "best_effort"},
		{Kind: operations.StatementExecute, Transactions: []operations.TransactionMode{operations.TransactionAutocommit}, Idempotency: "none", Cancellation: "best_effort"},
		{Kind: operations.ConnectionTest, Idempotency: "none", Cancellation: "best_effort"},
		{Kind: operations.MetadataInspect, Idempotency: "none", Cancellation: "best_effort"},
		{Kind: operations.MigrationStatus, Idempotency: "none", Cancellation: "best_effort"},
		{Kind: operations.MigrationApply, Transactions: []operations.TransactionMode{operations.TransactionAutocommit}, Idempotency: "none", Cancellation: "best_effort"},
	}}
}
func (Driver) Open(ctx context.Context, c adapter.Connection) (adapter.Session, error) {
	if ctx == nil || ctx.Err() != nil || c.Engine != "clickhouse" || c.TenantID == "" || c.ConnectionID == "" || c.Revision == "" || c.Host == "" || c.Port < 1 || c.Port > 65535 || c.Namespace == "" || c.Schema != "" && c.Schema != c.Namespace || c.Username == "" || c.Password == "" {
		return nil, adapter.ErrInvalid
	}
	for _, v := range []string{c.Host, c.Namespace, c.Username, c.Password} {
		if len(v) > 32<<10 || strings.ContainsAny(v, "\x00\r\n") {
			return nil, adapter.ErrInvalid
		}
	}
	if strings.ContainsAny(c.Host, "/,\\ \t") || strings.ContainsAny(c.Namespace, "/\\") || strings.Contains(c.Username, ":") {
		return nil, adapter.ErrInvalid
	}
	for key, value := range c.Options {
		if key != "migration_server_uuid" || !migration.ValidServerUUID(value) {
			return nil, adapter.ErrInvalid
		}
	}
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
	endpoint := url.URL{Scheme: "https", Host: net.JoinHostPort(c.Host, strconv.Itoa(c.Port)), RawQuery: url.Values{"database": {c.Namespace}}.Encode()}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.TLSClientConfig = config
	transport.DisableCompression = true
	transport.MaxConnsPerHost = 1
	transport.MaxIdleConns = 1
	transport.MaxIdleConnsPerHost = 1
	session := &Session{source: native.ResolvedSource{ID: "operation_source", URL: endpoint.String(), Username: c.Username, Password: c.Password, TLS: config}, database: c.Namespace, client: &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
	session.migrationUUID = c.Options["migration_server_uuid"]
	if err := session.Test(ctx); err != nil {
		_ = session.Close()
		return nil, err
	}
	return session, nil
}

type Session struct {
	source        native.ResolvedSource
	database      string
	client        *http.Client
	migrationUUID string
}

func (s *Session) Close() error {
	if s != nil && s.client != nil {
		s.client.CloseIdleConnections()
	}
	return nil
}
func (s *Session) Query(ctx context.Context, q adapter.Query, sink adapter.Sink) (adapter.QueryStats, error) {
	if s == nil || ctx == nil || sink == nil || q.Validate() != nil {
		return adapter.QueryStats{}, adapter.ErrInvalid
	}
	if len(q.Parameters) != 0 {
		return adapter.QueryStats{}, adapter.ErrUnsupported
	}
	statement, err := sqlguard.ReadOnly(q.Statement)
	if err != nil {
		return adapter.QueryStats{}, err
	}
	timeout := 30 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		timeout = min(timeout, time.Until(deadline))
	}
	if timeout <= 0 {
		return adapter.QueryStats{}, context.DeadlineExceeded
	}
	limits := query.Limits{MaxRows: q.MaxRows, MaxBytes: q.MaxBytes, Timeout: timeout, MemoryMB: 64, Threads: 1, MaxTempMB: 1}
	engine, err := native.NewResolved(s.source, limits)
	if err != nil {
		return adapter.QueryStats{}, err
	}
	defer engine.Close()
	stats, err := engine.Execute(ctx, query.Request{Mode: "native", ConnectionID: s.source.ID, SQL: statement}, splitSink{next: sink, rows: int64(q.BatchRows)})
	return adapter.QueryStats{Rows: stats.Rows, Bytes: stats.Bytes, Elapsed: time.Duration(stats.DurationNS)}, err
}
func (s *Session) Test(ctx context.Context) error {
	stats, err := s.Query(ctx, adapter.Query{Statement: "SELECT 1", MaxRows: 1, MaxBytes: 64 << 10, BatchRows: 1}, discardSink{})
	if err == nil && stats.Rows != 1 {
		return adapter.ErrInvalid
	}
	return err
}

type discardSink struct{}

func (discardSink) Schema(*arrow.Schema) error    { return nil }
func (discardSink) Write(arrow.RecordBatch) error { return nil }

type splitSink struct {
	next adapter.Sink
	rows int64
}

func (s splitSink) Schema(schema *arrow.Schema) error { return s.next.Schema(schema) }
func (s splitSink) Write(record arrow.RecordBatch) error {
	for start := int64(0); start < record.NumRows(); start += s.rows {
		part := record.NewSlice(start, min(start+s.rows, record.NumRows()))
		err := s.next.Write(part)
		part.Release()
		if err != nil {
			return err
		}
	}
	return nil
}
