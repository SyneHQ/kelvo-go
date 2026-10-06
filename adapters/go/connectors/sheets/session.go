// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Package sheets materializes one authorized spreadsheet before local SQL.
package sheets

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/adapters/go/connectors/saas"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/SYNEHQ/kelvo-go/provider"
)

const scope = "https://www.googleapis.com/auth/spreadsheets.readonly"
const maxSnapshotBytes = 32 << 20

type Session struct {
	mu                     sync.Mutex
	id, credential, origin string
	http                   *http.Client
	limits                 query.Limits
}

func Open(ctx context.Context, id, credential string, limits query.Limits) (*Session, error) {
	if ctx == nil || ctx.Err() != nil || !provider.ValidSheetID(id) || credential == "" || len(credential) > 32<<10 || !utf8.ValidString(credential) || strings.ContainsRune(credential, 0) || limits.Validate() != nil {
		return nil, adapter.ErrInvalid
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	transport.MaxConnsPerHost = 2
	transport.MaxIdleConnsPerHost = 1
	transport.MaxIdleConns = 2
	client := &http.Client{Transport: transport, Timeout: 45 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return &Session{id: id, credential: credential, origin: "https://sheets.googleapis.com", http: client, limits: limits}, nil
}
func (s *Session) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.credential = ""
	if s.http != nil {
		s.http.CloseIdleConnections()
		s.http = nil
	}
	return nil
}
func (s *Session) Test(ctx context.Context) error {
	if ctx == nil || s == nil {
		return adapter.ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, _, err := s.tabs(ctx)
	return err
}
func (s *Session) Query(ctx context.Context, q adapter.Query, sink adapter.Sink) (adapter.QueryStats, error) {
	if s == nil || ctx == nil || sink == nil || q.Validate() != nil {
		return adapter.QueryStats{}, adapter.ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, s.limits.Timeout)
	defer cancel()
	return s.execute(ctx, q, nil, sink)
}
func (s *Session) Inspect(ctx context.Context, spec operations.MetadataSpec, limits adapter.Limits, sink adapter.Sink) (adapter.QueryStats, error) {
	if s == nil || ctx == nil || sink == nil {
		return adapter.QueryStats{}, adapter.ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, s.limits.Timeout)
	defer cancel()
	return s.execute(ctx, adapter.Query{Statement: "metadata", MaxRows: limits.MaxRows, MaxBytes: limits.MaxBytes, BatchRows: limits.BatchRows}, &spec, sink)
}

type tab struct {
	Properties struct {
		Title string `json:"title"`
		Grid  *struct {
			Columns int `json:"columnCount"`
		} `json:"gridProperties"`
	} `json:"properties"`
}

func (s *Session) tabs(ctx context.Context) ([]tab, string, error) {
	if s.http == nil {
		return nil, "", adapter.ErrInvalid
	}
	token, err := saas.GoogleAccessToken(ctx, s.credential, scope)
	if err != nil {
		return nil, "", err
	}
	var data struct {
		SpreadsheetID string `json:"spreadsheetId"`
		Sheets        []tab  `json:"sheets"`
	}
	remaining := int64(1 << 20)
	path := "/v4/spreadsheets/" + s.id + "?fields=spreadsheetId,sheets.properties(title,gridProperties.columnCount)"
	if err = s.get(ctx, path, token, &remaining, &data); err != nil {
		return nil, "", err
	}
	if data.SpreadsheetID != s.id || len(data.Sheets) > 30 {
		return nil, "", adapter.ErrInvalid
	}
	seen := map[string]bool{}
	for _, tab := range data.Sheets {
		title := tab.Properties.Title
		if title == "" || len(title) > 1024 || !utf8.ValidString(title) || strings.ContainsRune(title, 0) || seen[strings.ToLower(title)] || tab.Properties.Grid != nil && (tab.Properties.Grid.Columns < 1 || tab.Properties.Grid.Columns > 200) {
			return nil, "", adapter.ErrUnsupported
		}
		seen[strings.ToLower(title)] = true
	}
	return data.Sheets, token, nil
}

type table struct {
	Name    string
	Columns []string
	Rows    [][]any
}

func (s *Session) snapshot(ctx context.Context) ([]table, error) {
	tabs, token, err := s.tabs(ctx)
	if err != nil {
		return nil, err
	}
	remaining := min(int64(maxSnapshotBytes), int64(s.limits.MemoryMB)<<17)
	var tables []table
	for _, tab := range tabs {
		if tab.Properties.Grid == nil {
			continue
		}
		title := tab.Properties.Title
		rangeName := "'" + strings.ReplaceAll(title, "'", "''") + "'!A1:GR10002"
		path := "/v4/spreadsheets/" + s.id + "/values/" + url.PathEscape(rangeName) + "?valueRenderOption=UNFORMATTED_VALUE&dateTimeRenderOption=FORMATTED_STRING"
		var values struct {
			Range          string  `json:"range"`
			MajorDimension string  `json:"majorDimension"`
			Values         [][]any `json:"values"`
		}
		if err = s.get(ctx, path, token, &remaining, &values); err != nil {
			return nil, err
		}
		if values.MajorDimension != "ROWS" || len(values.Values) > 10001 {
			return nil, adapter.ErrLimit
		}
		if len(values.Values) == 0 {
			continue
		}
		columns, err := columnNames(values.Values)
		if err != nil {
			return nil, err
		}
		rows := make([][]any, len(values.Values)-1)
		for i, source := range values.Values[1:] {
			rows[i] = make([]any, len(columns))
			for j, value := range source {
				if value == nil {
					continue
				}
				text, err := cellText(value)
				if err != nil {
					return nil, err
				}
				if text != "" {
					rows[i][j] = text
				}
			}
		}
		tables = append(tables, table{Name: title, Columns: columns, Rows: rows})
	}
	return tables, nil
}
func (s *Session) get(ctx context.Context, path, token string, remaining *int64, out any) error {
	if remaining == nil || *remaining < 1 {
		return adapter.ErrLimit
	}
	req, err := http.NewRequestWithContext(ctx, "GET", s.origin+path, nil)
	if err != nil {
		return adapter.ErrInvalid
	}
	req.Header.Set("Authorization", "Bearer "+token)
	response, err := s.http.Do(req)
	if err != nil {
		return errors.New("Sheets request failed")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return errors.New("Sheets request rejected")
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, *remaining+1))
	*remaining -= int64(len(raw))
	if *remaining < 0 {
		return adapter.ErrLimit
	}
	if err != nil {
		return errors.New("Sheets response incomplete")
	}
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	if decoder.Decode(out) != nil {
		return adapter.ErrInvalid
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		return adapter.ErrInvalid
	}
	return nil
}
func cellText(value any) (string, error) {
	switch v := value.(type) {
	case string:
		if !utf8.ValidString(v) || strings.ContainsRune(v, 0) {
			return "", adapter.ErrUnsupported
		}
		return v, nil
	case json.Number:
		return string(v), nil
	case bool:
		if v {
			return "true", nil
		}
		return "false", nil
	case nil:
		return "", nil
	}
	return "", adapter.ErrUnsupported
}
func columnNames(rows [][]any) ([]string, error) {
	width := 0
	for _, row := range rows {
		width = max(width, len(row))
	}
	if width < 1 || width > 200 {
		return nil, adapter.ErrLimit
	}
	columns := make([]string, width)
	seen := map[string]bool{}
	for i := range columns {
		name := "column_" + strconv.Itoa(i+1)
		if i < len(rows[0]) {
			value, err := cellText(rows[0][i])
			if err != nil {
				return nil, err
			}
			if strings.TrimSpace(value) != "" {
				name = strings.TrimSpace(value)
			}
		}
		base := name
		for j := 2; seen[strings.ToLower(name)]; j++ {
			name = base + "_" + strconv.Itoa(j)
		}
		seen[strings.ToLower(name)] = true
		columns[i] = name
	}
	return columns, nil
}
