// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Package dbapi provides a bounded compatibility client for a separately operated db.api.go gateway.
package dbapi

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/sources/rowarrow"
	"github.com/SYNEHQ/kelvo-go/internal/sources/sqlguard"
	"github.com/apache/arrow-go/v18/arrow"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

type Engine struct {
	sources map[string]catalog.Source
	limits  query.Limits
	http    *http.Client
}

func New(c catalog.Config, l query.Limits) (*Engine, error) {
	if err := l.Validate(); err != nil {
		return nil, err
	}
	m := map[string]catalog.Source{}
	for _, s := range c.Sources {
		if s.Adapter != "dbapi" {
			continue
		}
		if err := s.ValidateAdapter(); err != nil {
			return nil, query.NewError("CONFIGURATION_ERROR", "Invalid db.api adapter")
		}
		if !catalog.ValidID(s.ID) || s.URLEnv == "" || s.TokenEnv == "" || s.Options["remote_connection_id"] == "" || len(s.Options) != 1 {
			return nil, query.NewError("CONFIGURATION_ERROR", "db.api source requires only remote_connection_id, url_env and token_env")
		}
		if s.DSNEnv != "" || s.UsernameEnv != "" || s.PasswordEnv != "" || s.Path != "" {
			return nil, query.NewError("CONFIGURATION_ERROR", "db.api source cannot contain connection credentials")
		}
		if _, e := origin(s); e != nil {
			return nil, e
		}
		if _, exists := m[s.ID]; exists {
			return nil, query.NewError("CONFIGURATION_ERROR", "Duplicate db.api source")
		}
		m[s.ID] = s
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.Proxy = nil
	tr.DisableCompression = true
	tr.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	tr.TLSHandshakeTimeout = 5 * time.Second
	tr.ResponseHeaderTimeout = l.Timeout
	tr.MaxConnsPerHost = 2
	tr.MaxResponseHeaderBytes = 64 << 10
	return &Engine{m, l, &http.Client{Transport: tr, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}
func (e *Engine) Close() error { e.http.CloseIdleConnections(); return nil }
func origin(s catalog.Source) (*url.URL, error) {
	u, e := url.Parse(os.Getenv(s.URLEnv))
	if e != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
		return nil, query.NewError("CONFIGURATION_ERROR", "db.api source requires an HTTPS origin")
	}
	return u, nil
}
func (e *Engine) Execute(parent context.Context, r query.Request, sink query.Sink) (st query.Stats, err error) {
	st.Backend = "dbapi"
	if err := query.ValidateRequest(r); err != nil {
		return st, err
	}
	start := time.Now()
	defer func() { st.DurationNS = time.Since(start).Nanoseconds() }()
	if sink == nil || r.Mode != "native" || r.ConnectionID == "" || len(r.Sources) > 0 {
		return st, query.NewError("INVALID_ARGUMENT", "db.api requires native mode, connection_id and result sink")
	}
	if r.Mongo != nil || len(r.Parameters) > 0 {
		return st, query.NewError("UNSUPPORTED", "db.api compatibility adapter does not support parameters")
	}
	src, ok := e.sources[r.ConnectionID]
	if !ok {
		return st, query.NewError("PERMISSION_DENIED", "Requested source is unavailable")
	}
	sql, err := sqlguard.ReadOnly(r.SQL)
	if err != nil {
		return st, err
	}
	tok := os.Getenv(src.TokenEnv)
	if tok == "" || len(tok) > 16<<10 || strings.ContainsAny(tok, "\r\n\x00") {
		return st, query.NewError("QUERY_FAILED", "db.api credentials are unavailable")
	}
	u, err := origin(src)
	if err != nil {
		return st, err
	}
	ctx, cancel := context.WithTimeout(parent, e.limits.Timeout)
	defer cancel()
	body, _ := json.Marshal(map[string]string{"id": src.Options["remote_connection_id"], "query": sql})
	u.Path = "/api/v1/metadata/query"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(body))
	if err != nil {
		return st, query.NewError("QUERY_FAILED", "Could not prepare db.api request")
	}
	req.Header.Set("X-API-KEY", tok)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	res, err := e.http.Do(req)
	if err != nil {
		return st, query.PublicError(err)
	}
	defer res.Body.Close()
	if res.Header.Get("Content-Encoding") != "" && res.Header.Get("Content-Encoding") != "identity" {
		return st, query.NewError("QUERY_FAILED", "db.api returned unsupported response encoding")
	}
	if res.StatusCode < 200 || res.StatusCode > 299 {
		return st, query.NewError("QUERY_FAILED", "db.api rejected query")
	}
	limit := min(32<<20, min(e.limits.MaxBytes, int64(e.limits.MemoryMB)<<18))
	raw, err := io.ReadAll(io.LimitReader(res.Body, limit+1))
	if err != nil {
		return st, query.PublicError(err)
	}
	if int64(len(raw)) > limit {
		return st, query.NewError("RESOURCE_EXHAUSTED", "db.api response exceeds byte limit")
	}
	var out struct {
		Results  json.RawMessage `json:"results"`
		Error    string          `json:"error"`
		RowCount *int64          `json:"rowCount"`
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	if d.Decode(&out) != nil || d.Decode(new(any)) != io.EOF || out.Error != "" || out.RowCount == nil || len(out.Results) == 0 {
		return st, query.NewError("QUERY_FAILED", "db.api returned an invalid query response")
	}
	var documents []json.RawMessage
	if json.Unmarshal(out.Results, &documents) != nil || *out.RowCount != int64(len(documents)) {
		return st, query.NewError("QUERY_FAILED", "db.api returned an inconsistent result count")
	}
	if int64(len(documents)) > e.limits.MaxRows {
		return st, query.NewError("RESOURCE_EXHAUSTED", "Query result exceeds row limit")
	}
	// Validate every document before delivering the schema. Keep RawMessage
	// bytes intact instead of inferring types or passing numbers through float64.
	for _, doc := range documents {
		trimmed := bytes.TrimSpace(doc)
		if len(trimmed) < 2 || trimmed[0] != '{' || trimmed[len(trimmed)-1] != '}' {
			return st, query.NewError("QUERY_FAILED", "db.api results must contain JSON objects")
		}
	}
	meta := arrow.MetadataFrom(map[string]string{"content_type": "application/json", "source_engine": src.Type, "adapter": "dbapi"})
	schema := arrow.NewSchema([]arrow.Field{{Name: "document_json", Type: arrow.BinaryTypes.Binary, Nullable: false, Metadata: meta}}, &meta)
	writer, err := rowarrow.NewWriter(schema, e.limits, sink)
	if err != nil {
		return st, query.PublicError(err)
	}
	defer writer.Close()
	st.PrepareNS = time.Since(start).Nanoseconds()
	for _, doc := range documents {
		if err := ctx.Err(); err != nil {
			return st, query.PublicError(err)
		}
		if err := writer.Write([]any{[]byte(doc)}); err != nil {
			return st, query.PublicError(err)
		}
	}
	written, err := writer.Finish()
	if err != nil {
		return st, query.PublicError(err)
	}
	st.Rows, st.Bytes, st.Batches, st.WireBytes = written.Rows, written.Bytes, written.Batches, int64(len(raw))
	return st, nil
}

var _ query.Executor = (*Engine)(nil)
