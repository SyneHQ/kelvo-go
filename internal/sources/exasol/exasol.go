// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Package exasol implements Exasol's native WebSocket SQL protocol with exact
// number decoding. The upstream driver is used only for its public DSN parser.
package exasol

import (
	"context"
	"crypto/tls"
	"net"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/sources/cloudapi"
	"github.com/SYNEHQ/kelvo-go/internal/sources/rowarrow"
	"github.com/SYNEHQ/kelvo-go/internal/sources/sqlguard"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/exasol/exasol-driver-go/pkg/dsn"
	"github.com/gorilla/websocket"
)

type Engine struct {
	source        catalog.Source
	config        *dsn.DSNConfig
	limits        query.Limits
	dialer        *websocket.Dialer
	responseLimit int64
	maxPages      int
}

var hostname = regexp.MustCompile(`^[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?$`)

func New(config catalog.Config, limits query.Limits) (*Engine, error) {
	if err := limits.Validate(); err != nil {
		return nil, err
	}
	source, err := cloudapi.SingleSource(config, "exasol")
	if err != nil {
		return nil, err
	}
	if !catalog.ValidID(source.ID) || source.DSNEnv == "" || catalog.ValidateEnvironment(source.DSNEnv) != nil || source.Adapter != "" || source.Path != "" || source.URLEnv != "" || source.UsernameEnv != "" || source.PasswordEnv != "" || source.TokenEnv != "" || len(source.Options) != 0 {
		return nil, query.NewError("CONFIGURATION_ERROR", "Exasol requires a native source with dsn_env only")
	}
	parsed, err := parseDSN(os.Getenv(source.DSNEnv))
	if err != nil {
		return nil, err
	}
	dialer := &websocket.Dialer{Proxy: nil, NetDialContext: (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext, TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12}, HandshakeTimeout: min(limits.Timeout, 5*time.Second), EnableCompression: false}
	return &Engine{source: source, config: parsed, limits: limits, dialer: dialer, responseLimit: min(32<<20, int64(limits.MemoryMB)<<18), maxPages: 100000}, nil
}

type Credentials struct {
	URL, Username, Password, Schema string
	TLS                             *tls.Config
}

func NewResolved(config catalog.Config, limits query.Limits, credentials Credentials) (*Engine, error) {
	if err := limits.Validate(); err != nil {
		return nil, err
	}
	source, err := cloudapi.SingleSource(config, "exasol")
	if err != nil {
		return nil, err
	}
	u, err := url.Parse(credentials.URL)
	port := 0
	if err == nil {
		port, _ = strconv.Atoi(u.Port())
	}
	if err != nil || u.Scheme != "wss" || !hostname.MatchString(u.Hostname()) || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" || u.ForceQuery || port < 1 || port > 65535 || credentials.Username == "" || credentials.Password == "" || len(credentials.Username) > 4096 || len(credentials.Password) > 4096 || len(credentials.Schema) > 4096 || strings.ContainsAny(credentials.Username+credentials.Password+credentials.Schema, "\x00\r\n") {
		return nil, query.NewError("CONFIGURATION_ERROR", "Exasol requires explicit verified connection details")
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	if credentials.TLS != nil {
		if credentials.TLS.InsecureSkipVerify || credentials.TLS.MaxVersion != 0 && credentials.TLS.MaxVersion < tls.VersionTLS12 {
			return nil, query.NewError("CONFIGURATION_ERROR", "Exasol requires verified TLS")
		}
		tlsConfig = credentials.TLS.Clone()
		tlsConfig.MinVersion = max(tls.VersionTLS12, tlsConfig.MinVersion)
		if tlsConfig.RootCAs != nil {
			tlsConfig.RootCAs = tlsConfig.RootCAs.Clone()
		}
	}
	encryption, verification, compression := true, true, false
	parsed := &dsn.DSNConfig{Host: u.Hostname(), Port: port, User: credentials.Username, Password: credentials.Password, Schema: credentials.Schema, Encryption: &encryption, ValidateServerCertificate: &verification, Compression: &compression, FetchSize: 1024}
	dialer := &websocket.Dialer{Proxy: nil, NetDialContext: (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext, TLSClientConfig: tlsConfig, HandshakeTimeout: min(limits.Timeout, 5*time.Second), EnableCompression: false}
	return &Engine{source: source, config: parsed, limits: limits, dialer: dialer, responseLimit: min(32<<20, int64(limits.MemoryMB)<<18), maxPages: 100000}, nil
}

func parseDSN(raw string) (*dsn.DSNConfig, error) {
	bad := func() (*dsn.DSNConfig, error) {
		return nil, query.NewError("CONFIGURATION_ERROR", "Exasol requires explicit credentials and verified encrypted TLS in a supported DSN")
	}
	if len(raw) == 0 || len(raw) > 32<<10 || strings.ContainsAny(raw, "\x00\r\n") || strings.Contains(raw, "{{,}}") {
		return bad()
	}
	c, err := dsn.ParseDSN(raw)
	if err != nil || c.Encryption == nil || !*c.Encryption || c.ValidateServerCertificate == nil || !*c.ValidateServerCertificate || c.CertificateFingerprint != "" || c.Compression == nil || *c.Compression || c.AccessToken != "" || c.RefreshToken != "" || c.UrlPath != "" || len(c.Params) != 0 || c.ResultSetMaxRows != 0 || c.Port < 1 || c.Port > 65535 || !hostname.MatchString(c.Host) || strings.Contains(c.Host, "..") || len(c.Host) > 253 || c.User == "" || c.Password == "" || len(c.User) > 4096 || len(c.Password) > 4096 || len(c.Schema) > 4096 || c.FetchSize < 1 || c.QueryTimeout < 0 {
		return bad()
	}
	if c.FetchSize > 64<<10 {
		return bad()
	}
	return c, nil
}

// Each query owns and closes its connection; Engine holds no open sessions.
func (e *Engine) Close() error { return nil }

func (e *Engine) Execute(parent context.Context, req query.Request, sink query.Sink) (stats query.Stats, err error) {
	started := time.Now()
	defer func() { stats.Backend = "exasol"; stats.DurationNS = time.Since(started).Nanoseconds() }()
	if req.Mode != "native" || req.ConnectionID != e.source.ID || len(req.Sources) != 0 || sink == nil {
		return stats, query.NewError("PERMISSION_DENIED", "Requested source is unavailable")
	}
	if err = query.ValidateRequest(req); err != nil {
		return stats, err
	}
	if req.Mongo != nil || len(req.Parameters) != 0 {
		return stats, query.NewError("UNSUPPORTED", "Exasol currently requires SQL without parameters")
	}
	sql, err := sqlguard.ReadOnly(req.SQL)
	if err != nil {
		return stats, err
	}
	ctx, cancel := context.WithTimeout(parent, e.limits.Timeout)
	defer cancel()
	endpoint := url.URL{Scheme: "wss", Host: net.JoinHostPort(e.config.Host, strconv.Itoa(e.config.Port))}
	conn, response, err := e.dialer.DialContext(ctx, endpoint.String(), nil)
	if response != nil && response.Body != nil {
		response.Body.Close()
	}
	if err != nil {
		return stats, public(ctx, "Could not connect to Exasol")
	}
	defer conn.Close()
	s := &session{conn: conn, responseLimit: e.responseLimit, wireLimit: e.limits.MaxBytes*4 + e.responseLimit, usable: true}
	defer func() { stats.WireBytes = s.wireBytes }()
	if err = s.login(ctx, e.config, e.limits); err != nil {
		return stats, err
	}
	cleaned := false
	var handle *int64
	defer func() {
		if !cleaned {
			_ = s.cleanup(handle)
		}
	}()
	var result results
	attributes := map[string]any{"autocommit": false, "resultSetMaxRows": e.limits.MaxRows + 1, "timestampUtcEnabled": true}
	if err = s.exchange(ctx, map[string]any{"command": "execute", "sqlText": sql, "attributes": attributes}, &result, false); err != nil {
		return stats, err
	}
	if len(result.Results) == 1 && result.Results[0].Set != nil {
		handle = result.Results[0].Set.Handle
	}
	if result.Count != 1 || len(result.Results) != 1 || result.Results[0].Kind != "resultSet" || result.Results[0].Set == nil {
		return stats, query.NewError("UNSUPPORTED", "Exasol must return one query result set")
	}
	set := result.Results[0].Set
	if set.NumRows == nil || set.RowsInMessage == nil || *set.NumRows < 0 || *set.RowsInMessage < 0 || *set.RowsInMessage > *set.NumRows || set.NumColumns != len(set.Columns) || handle != nil && *handle < 0 {
		return stats, query.NewError("QUERY_FAILED", "Exasol returned invalid result metadata")
	}
	if *set.NumRows > e.limits.MaxRows {
		return stats, query.NewError("RESOURCE_EXHAUSTED", "Exasol result exceeds row limit")
	}
	schema, err := makeSchema(set.Columns)
	if err != nil {
		return stats, err
	}
	writer, err := rowarrow.NewWriter(schema, e.limits, sink)
	if err != nil {
		return stats, err
	}
	defer writer.Close()
	stats.PrepareNS = time.Since(started).Nanoseconds()
	position, rows, data := int64(0), *set.RowsInMessage, set.Data
	for page := 0; ; page++ {
		if page >= e.maxPages {
			return stats, query.NewError("RESOURCE_EXHAUSTED", "Exasol pagination exceeds its limit")
		}
		if err = writeRows(ctx, writer, schema, data, rows); err != nil {
			return stats, err
		}
		position += rows
		if position == *set.NumRows {
			break
		}
		if handle == nil {
			return stats, query.NewError("QUERY_FAILED", "Exasol omitted its result handle")
		}
		var next resultSet
		fetchBytes := min(int64(e.config.FetchSize)*1024, e.responseLimit/2)
		if err = s.exchange(ctx, map[string]any{"command": "fetch", "resultSetHandle": *handle, "startPosition": position, "numBytes": fetchBytes}, &next, false); err != nil {
			return stats, err
		}
		if next.NumRows == nil || *next.NumRows <= 0 || *next.NumRows > *set.NumRows-position || next.Handle != nil && *next.Handle != *handle {
			return stats, query.NewError("QUERY_FAILED", "Exasol returned an invalid fetch page")
		}
		if len(next.Columns) != 0 {
			other, schemaErr := makeSchema(next.Columns)
			if schemaErr != nil || !schema.Equal(other) {
				return stats, query.NewError("QUERY_FAILED", "Exasol changed result schema")
			}
		}
		rows, data = *next.NumRows, next.Data
	}
	if err = ctx.Err(); err != nil {
		return stats, err
	}
	// Do not report completion before cursor close and rollback acknowledgement.
	err = s.cleanup(handle)
	cleaned = true
	if err != nil {
		return stats, err
	}
	if err = ctx.Err(); err != nil {
		return stats, err
	}
	written, err := writer.Finish()
	stats.Rows, stats.Bytes, stats.Batches = written.Rows, written.Bytes, written.Batches
	return stats, err
}
func writeRows(ctx context.Context, writer *rowarrow.Writer, schema *arrow.Schema, data [][]any, count int64) error {
	if count == 0 && len(data) == 0 {
		return nil
	}
	if len(data) != schema.NumFields() {
		return query.NewError("QUERY_FAILED", "Exasol column data does not match schema")
	}
	for _, column := range data {
		if int64(len(column)) != count {
			return query.NewError("QUERY_FAILED", "Exasol column length does not match row count")
		}
	}
	values := make([]any, len(data))
	for row := int64(0); row < count; row++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		for col := range data {
			values[col] = data[col][row]
		}
		converted, err := makeRow(schema, values)
		if err != nil {
			return err
		}
		if err = writer.Write(converted); err != nil {
			return err
		}
	}
	return nil
}
func public(ctx context.Context, message string) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return query.NewError("QUERY_FAILED", message)
}
