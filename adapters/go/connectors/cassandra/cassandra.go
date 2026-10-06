// Package cassandra provides isolated CQL sessions for Cassandra and ScyllaDB.
package cassandra

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/gocql/gocql"
)

type Driver struct{ Engine string }

var _ adapter.Driver = Driver{}

func (d Driver) Capabilities() operations.Capabilities {
	parameters := []string{"null", "bool", "string", "int64", "float64", "binary", "timestamp"}
	return operations.Capabilities{Version: operations.Version, Engine: d.Engine,
		Operations: []operations.Capability{
			{Kind: operations.ConnectionTest, Idempotency: "none", Cancellation: "best_effort"},
			{Kind: operations.MetadataInspect, Idempotency: "none", Cancellation: "best_effort"},
			{Kind: operations.QueryRead, ParameterTypes: parameters, Idempotency: "none", Cancellation: "best_effort"},
			{Kind: operations.StatementExecute, ParameterTypes: parameters, Idempotency: "none", Cancellation: "best_effort"},
		}}
}

var hostname = regexp.MustCompile(`^[a-zA-Z0-9](?:[a-zA-Z0-9.-]{0,251}[a-zA-Z0-9])?$`)

func (d Driver) Open(ctx context.Context, connection adapter.Connection) (adapter.Session, error) {
	if ctx == nil {
		return nil, adapter.ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if (d.Engine != "cassandra" && d.Engine != "scylla") || connection.Engine != d.Engine ||
		connection.Schema != "" ||
		connection.TenantID == "" || connection.ConnectionID == "" || connection.Revision == "" ||
		connection.Port < 1 || connection.Port > 65535 ||
		(net.ParseIP(connection.Host) == nil && !hostname.MatchString(connection.Host)) ||
		connection.Username == "" || connection.Password == "" || connection.TLS == nil || connection.TLS.InsecureSkipVerify {
		return nil, adapter.ErrInvalid
	}
	tlsConfig := connection.TLS.Clone()
	if tlsConfig.MinVersion < tls.VersionTLS12 {
		tlsConfig.MinVersion = tls.VersionTLS12
	}
	if tlsConfig.ServerName == "" {
		tlsConfig.ServerName = connection.Host
	}
	cluster := gocql.NewCluster(connection.Host)
	cluster.Port = connection.Port
	cluster.Keyspace = connection.Namespace
	cluster.Authenticator = gocql.PasswordAuthenticator{Username: connection.Username, Password: connection.Password}
	cluster.SslOpts = &gocql.SslOptions{Config: tlsConfig, EnableHostVerification: true}
	cluster.NumConns = 1
	cluster.DisableInitialHostLookup = true
	cluster.HostFilter = gocql.WhiteListHostFilter(connection.Host)
	cluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: 0}
	cluster.ConnectTimeout = 10 * time.Second
	cluster.Timeout = 30 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		left := time.Until(deadline)
		if left <= 0 {
			return nil, context.DeadlineExceeded
		}
		cluster.ConnectTimeout = min(cluster.ConnectTimeout, left)
		cluster.Timeout = min(cluster.Timeout, left)
	}
	// gocql's initial dial has no context API. Keep this call synchronous so
	// caller admission remains occupied until the bounded dial actually ends.
	session, err := cluster.CreateSession()
	if err != nil {
		return nil, errors.New("CQL connection failed")
	}
	if err := ctx.Err(); err != nil {
		session.Close()
		return nil, err
	}
	return &Session{backend: liveBackend{session}, namespace: connection.Namespace}, nil
}

type iterator interface {
	Columns() []column
	Scan(map[string]any) bool
	Close() error
}

type column struct {
	Name       string
	SourceType string
}

type backend interface {
	Query(context.Context, string, []any, int) iterator
	Close()
}

type Session struct {
	mu        sync.Mutex
	backend   backend
	namespace string
}

var _ adapter.QuerySession = (*Session)(nil)
var _ adapter.TestSession = (*Session)(nil)
var _ adapter.MetadataSession = (*Session)(nil)
var _ adapter.ChangeSession = (*Session)(nil)

func (s *Session) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.backend != nil {
		s.backend.Close()
		s.backend = nil
	}
	return nil
}

func (s *Session) Query(ctx context.Context, query adapter.Query, sink adapter.Sink) (stats adapter.QueryStats, err error) {
	if ctx == nil {
		return stats, adapter.ErrInvalid
	}
	if err = query.Validate(); err != nil {
		return stats, err
	}
	if sink == nil || !readCQL(query.Statement) {
		return stats, adapter.ErrUnsupported
	}
	parameters, err := parameterValues(query.Parameters)
	if err != nil {
		return stats, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.backend == nil {
		return stats, errors.New("CQL session is closed")
	}
	iter := s.backend.Query(ctx, query.Statement, parameters, min(query.BatchRows, 1024))
	return streamCQL(ctx, query, sink, iter)
}

func streamCQL(ctx context.Context, query adapter.Query, sink adapter.Sink, iter iterator) (stats adapter.QueryStats, err error) {
	started := time.Now()
	defer func() { stats.Elapsed = time.Since(started) }()
	defer func() {
		if closeErr := iter.Close(); err == nil && closeErr != nil {
			err = errors.New("CQL query failed")
		}
	}()
	columns := iter.Columns()
	if len(columns) == 0 || len(columns) > 1024 {
		return stats, adapter.ErrUnsupported
	}
	schema, err := resultSchema(columns)
	if err != nil {
		return stats, err
	}
	if err := sink.Schema(schema); err != nil {
		return stats, err
	}
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	var batchBytes int64
	batchRows := 0
	flush := func() error {
		if batchRows == 0 {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		record := builder.NewRecord()
		defer record.Release()
		if err := sink.Write(record); err != nil {
			return err
		}
		stats.Bytes += batchBytes
		stats.Rows += int64(batchRows)
		batchRows = 0
		batchBytes = 0
		return nil
	}
	for {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		row := make(map[string]any, len(columns))
		if !iter.Scan(row) {
			break
		}
		if stats.Rows+int64(batchRows) >= query.MaxRows {
			return stats, adapter.ErrLimit
		}
		values := make([]any, len(columns))
		var rowBytes int64
		for i, column := range columns {
			raw, exists := row[column.Name]
			if !exists {
				return stats, adapter.ErrUnsupported
			}
			value, size, err := normalizeValue(column.SourceType, raw)
			if err != nil {
				return stats, err
			}
			if size > query.MaxBytes-stats.Bytes-batchBytes-rowBytes {
				return stats, adapter.ErrLimit
			}
			rowBytes += size
			values[i] = value
		}
		if rowBytes > 4<<20 {
			return stats, adapter.ErrLimit
		}
		if batchRows > 0 && batchBytes+rowBytes > 4<<20 {
			if err := flush(); err != nil {
				return stats, err
			}
		}
		for i, value := range values {
			appendValue(builder.Field(i), value)
		}
		batchBytes += rowBytes
		batchRows++
		if batchRows == query.BatchRows {
			if err := flush(); err != nil {
				return stats, err
			}
		}
	}
	err = flush()
	return stats, err
}

type liveBackend struct{ session *gocql.Session }

func (b liveBackend) Close() { b.session.Close() }
func (b liveBackend) Query(ctx context.Context, query string, parameters []any, pageSize int) iterator {
	return liveIterator{b.session.Query(query, parameters...).WithContext(ctx).PageSize(pageSize).Prefetch(0).Iter()}
}

type liveIterator struct{ iter *gocql.Iter }

func (i liveIterator) Close() error               { return i.iter.Close() }
func (i liveIterator) Scan(v map[string]any) bool { return i.iter.MapScan(v) }
func (i liveIterator) Columns() []column {
	columns := i.iter.Columns()
	result := make([]column, len(columns))
	for index, info := range columns {
		result[index] = column{Name: info.Name, SourceType: info.TypeInfo.Type().String()}
	}
	return result
}

// Only a single SELECT is accepted. CQL quotes escape by doubling; comments and
// ambiguous backslash escapes fail explicitly rather than changing the query.
func readCQL(statement string) bool {
	statement = strings.TrimSpace(statement)
	if len(statement) < 7 || !strings.EqualFold(statement[:6], "select") ||
		(statement[6] != ' ' && statement[6] != '\t' && statement[6] != '\n') {
		return false
	}
	statement = strings.TrimSuffix(statement, ";")
	var quote byte
	for i := 0; i < len(statement); i++ {
		ch := statement[i]
		if ch == 0 || ch == '\\' {
			return false
		}
		if quote != 0 {
			if ch == quote {
				if i+1 < len(statement) && statement[i+1] == quote {
					i++
				} else {
					quote = 0
				}
			}
			continue
		}
		if ch == '\'' || ch == '"' {
			quote = ch
			continue
		}
		if ch == ';' || i+1 < len(statement) && (statement[i:i+2] == "--" || statement[i:i+2] == "//" || statement[i:i+2] == "/*") {
			return false
		}
	}
	return quote == 0
}
