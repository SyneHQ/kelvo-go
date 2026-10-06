// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package saas

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/SYNEHQ/kelvo-go/adapter"
)

type budget struct {
	rows, bytes int64
	limits      adapter.Limits
}

func (b *budget) add(rows []map[string]any) error {
	if int64(len(rows)) > b.limits.MaxRows-b.rows {
		return adapter.ErrLimit
	}
	b.rows += int64(len(rows))
	return nil
}
func (s *Session) request(ctx context.Context, method, endpoint string, body []byte, headers http.Header, b *budget) ([]byte, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	u, err := url.Parse(endpoint)
	if err != nil || u.User != nil || u.Scheme != "https" || u.Fragment != "" || u.RawPath != "" || strings.ContainsAny(endpoint, "\x00\r\n\\") {
		return nil, adapter.ErrInvalid
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, adapter.ErrInvalid
	}
	req.GetBody = nil
	req.Header = headers.Clone()
	response, err := s.http.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, errors.New("provider transport failed")
	}
	defer response.Body.Close()
	remaining := b.limits.MaxBytes - b.bytes
	if remaining < 0 {
		return nil, adapter.ErrLimit
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, remaining+1))
	b.bytes += int64(len(raw))
	if b.bytes > b.limits.MaxBytes {
		return nil, adapter.ErrLimit
	}
	if err != nil {
		return nil, errors.New("provider response incomplete")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, errors.New("provider request rejected")
	}
	return raw, nil
}
func objectRows(raw json.RawMessage) ([]map[string]any, error) {
	var rows []map[string]any
	if decodeDocument(raw, &rows) != nil || rows == nil {
		return nil, adapter.ErrInvalid
	}
	for _, row := range rows {
		if row == nil {
			return nil, adapter.ErrInvalid
		}
	}
	return rows, nil
}
func nextAtOrigin(raw, base, pathPrefix string) (string, error) {
	u, err := url.Parse(raw)
	origin, baseErr := url.Parse(base)
	if err != nil || baseErr != nil {
		return "", adapter.ErrInvalid
	}
	u = origin.ResolveReference(u)
	if u.Scheme != origin.Scheme || u.Host != origin.Host || u.User != nil || u.Fragment != "" || u.RawPath != "" || !strings.HasPrefix(u.Path, pathPrefix) {
		return "", adapter.ErrInvalid
	}
	for _, part := range strings.Split(u.Path, "/") {
		if part == "." || part == ".." {
			return "", adapter.ErrInvalid
		}
	}
	return u.String(), nil
}
func bearer(token string) http.Header {
	return http.Header{"Authorization": {"Bearer " + token}, "Content-Type": {"application/json"}}
}
