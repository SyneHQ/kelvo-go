package redis

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/SYNEHQ/kelvo-go/provider"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type Driver struct{}

func (Driver) Capabilities() operations.Capabilities {
	return operations.Capabilities{Version: 1, Engine: "redis", Operations: []operations.Capability{
		{Kind: operations.ConnectionTest, Idempotency: "none", Cancellation: "best_effort"},
		{Kind: operations.MetadataInspect, Idempotency: "none", Cancellation: "best_effort"},
		{Kind: operations.NativeRead, ParameterTypes: []string{"json"}, Idempotency: "none", Cancellation: "best_effort"},
		{Kind: operations.NativeExecute, ParameterTypes: []string{"json"}, Idempotency: "none", Cancellation: "best_effort"},
	}}
}

func (Driver) Open(ctx context.Context, c adapter.Connection) (adapter.Session, error) {
	if ctx != nil && ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if ctx == nil || c.Engine != "redis" || c.TenantID == "" || c.ConnectionID == "" || c.Revision == "" || c.Host == "" || strings.ContainsAny(c.Host, "/\\@?#% \t\r\n") || strings.Contains(c.Host, ":") && net.ParseIP(c.Host) == nil || c.Port < 1 || c.Port > 65535 || c.Schema != "" || c.Token != "" || c.Endpoint != "" || len(c.Options) != 0 || c.Password == "" || c.TLS == nil || c.TLS.InsecureSkipVerify {
		return nil, adapter.ErrInvalid
	}
	database, err := strconv.Atoi(c.Namespace)
	if err != nil || database < 0 || database > 255 || strconv.Itoa(database) != c.Namespace || len(c.Password) > 32<<10 || len(c.Username) > 32<<10 || strings.ContainsAny(c.Password+c.Username, "\x00\r\n") {
		return nil, adapter.ErrInvalid
	}
	config := c.TLS.Clone()
	config.MinVersion = max(config.MinVersion, tls.VersionTLS12)
	if config.ServerName == "" {
		config.ServerName = c.Host
	}
	dialer := tls.Dialer{NetDialer: &net.Dialer{Timeout: 10 * time.Second}, Config: config}
	conn, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort(c.Host, strconv.Itoa(c.Port)))
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, errors.New("Redis TLS connection failed")
	}
	s := &Session{conn: conn, reader: bufio.NewReaderSize(conn, 4096), namespace: c.Namespace}
	args := []string{"AUTH", c.Password}
	if c.Username != "" {
		args = []string{"AUTH", c.Username, c.Password}
	}
	if reply, err := s.command(ctx, args, 4096, 8); err != nil || reply != "OK" {
		s.Close()
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, errors.New("Redis authentication failed")
	}
	if database != 0 {
		if reply, err := s.command(ctx, []string{"SELECT", c.Namespace}, 4096, 8); err != nil || reply != "OK" {
			s.Close()
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, errors.New("Redis saved database unavailable")
		}
	}
	return s, nil
}

type Session struct {
	mu        sync.Mutex
	conn      net.Conn
	reader    *bufio.Reader
	namespace string
}

func (s *Session) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn == nil {
		return nil
	}
	err := s.conn.Close()
	s.conn = nil
	return err
}
func (s *Session) Test(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	reply, err := s.command(ctx, []string{"PING"}, 4096, 8)
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	if err != nil || reply != "PONG" {
		return errors.New("Redis connection test failed")
	}
	return nil
}

func (s *Session) RunNative(ctx context.Context, n adapter.Native, sink adapter.Sink) (result adapter.NativeResult, err error) {
	result.Outcome, result.Effect = operations.Rejected, operations.EffectNone
	if ctx == nil || n.Validate() != nil || n.Spec.Provider != "redis" || n.Spec.Command != "query" || len(n.Spec.Parameters) != 1 || n.Spec.Parameters[0].Type != "json" || n.Kind == operations.NativeRead && n.Spec.ReturnResult || n.Kind == operations.NativeRead && sink == nil || n.Spec.ReturnResult && sink == nil {
		return result, adapter.ErrInvalid
	}
	query, kind, err := provider.ParseRedis(n.Spec.Parameters[0].Value)
	if err != nil || kind != n.Kind {
		return result, adapter.ErrInvalid
	}
	if ctx.Err() != nil {
		result.Outcome = operations.CancelledBeforeStart
		return result, ctx.Err()
	}
	started := time.Now()
	defer func() { result.Stats.Elapsed = time.Since(started) }()
	s.mu.Lock()
	defer s.mu.Unlock()
	if ctx.Err() != nil {
		result.Outcome = operations.CancelledBeforeStart
		return result, ctx.Err()
	}
	if s.conn == nil {
		return result, adapter.ErrInvalid
	}
	var value any
	if strings.EqualFold(query.Args[0], "KEYS") {
		if len(query.Args) != 2 {
			return result, adapter.ErrInvalid
		}
		value, err = s.scanKeys(ctx, query.Args[1], n.Limits.MaxRows, n.Limits.MaxBytes)
	} else {
		value, err = s.command(ctx, query.Args, max(4096, n.Limits.MaxBytes), int(min(n.Limits.MaxRows*4+32, 1_000_000)))
	}
	if err != nil {
		var rejected serverError
		result.Outcome = operations.Failed
		if kind.Mutating() && !errors.As(err, &rejected) {
			result.Outcome, result.Effect = operations.OutcomeUnknown, operations.EffectUnknown
		}
		if ctx.Err() != nil {
			return result, ctx.Err()
		}
		if errors.Is(err, adapter.ErrLimit) {
			return result, adapter.ErrLimit
		}
		return result, errors.New("Redis command failed")
	}
	result.Outcome = operations.Completed
	if kind.Mutating() {
		result.Effect = operations.EffectCommitted
	}
	if kind == operations.NativeRead || n.Spec.ReturnResult {
		result.Stats, err = writeNativeResult(ctx, value, n.Limits, sink)
		if err != nil {
			result.Outcome = operations.Failed
		}
	}
	return result, err
}

func jsonRedis(value any) any {
	switch value := value.(type) {
	case []byte:
		if utf8.Valid(value) {
			return string(value)
		}
		return map[string]string{"base64": base64.StdEncoding.EncodeToString(value), "encoding": "binary"}
	case []any:
		out := make([]any, len(value))
		for i, v := range value {
			out[i] = jsonRedis(v)
		}
		return out
	case []string:
		return value
	}
	return value
}

func writeNativeResult(ctx context.Context, value any, limits adapter.Limits, sink adapter.Sink) (adapter.QueryStats, error) {
	var stats adapter.QueryStats
	raw, err := json.Marshal(map[string]any{"value": jsonRedis(value)})
	if err != nil || int64(len(raw)+8) > limits.MaxBytes {
		return stats, adapter.ErrLimit
	}
	meta := arrow.NewMetadata([]string{"kelvo_document_format"}, []string{provider.ResultFormat})
	schema := arrow.NewSchema([]arrow.Field{{Name: "document", Type: arrow.BinaryTypes.Binary}}, &meta)
	if err := ctx.Err(); err != nil {
		return stats, err
	}
	if err := sink.Schema(schema); err != nil {
		return stats, err
	}
	builder := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
	builder.Append(raw)
	values := builder.NewArray()
	builder.Release()
	defer values.Release()
	record := array.NewRecordBatch(schema, []arrow.Array{values}, 1)
	defer record.Release()
	if err := sink.Write(record); err != nil {
		return stats, err
	}
	return adapter.QueryStats{Rows: 1, Bytes: int64(len(raw) + 8)}, nil
}

func (s *Session) scanKeys(ctx context.Context, pattern string, maxRows, maxBytes int64) ([]string, error) {
	cursor := "0"
	seen := make(map[string]struct{})
	keys := make([]string, 0)
	var consumed int64
	for page := 0; page < 10000; page++ {
		reply, err := s.command(ctx, []string{"SCAN", cursor, "MATCH", pattern, "COUNT", "256"}, maxBytes-consumed, int(min(maxRows*2+32, 1_000_000)))
		if err != nil {
			return nil, err
		}
		parts, ok := reply.([]any)
		if !ok || len(parts) != 2 {
			return nil, adapter.ErrInvalid
		}
		cursor, ok = redisString(parts[0])
		if _, err := strconv.ParseUint(cursor, 10, 64); !ok || err != nil {
			return nil, adapter.ErrInvalid
		}
		values, ok := parts[1].([]any)
		if !ok {
			return nil, adapter.ErrInvalid
		}
		for _, value := range values {
			key, ok := redisString(value)
			if !ok || !utf8.ValidString(key) {
				return nil, adapter.ErrUnsupported
			}
			if _, exists := seen[key]; exists {
				continue
			}
			consumed += int64(len(key) + 16)
			if int64(len(keys)) >= maxRows || consumed > maxBytes {
				return nil, adapter.ErrLimit
			}
			seen[key] = struct{}{}
			keys = append(keys, key)
		}
		if cursor == "0" {
			return keys, nil
		}
	}
	return nil, adapter.ErrLimit
}
