// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package trino

import (
	"context"
	"crypto/sha256"
	"net/http"
	"strings"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/sources/cloudapi"
)

// ApplyStatement runs one caller-authorized statement and waits for the matching
// update acknowledgement. It never resubmits a statement after a lost response.
func (e *Engine) ApplyStatement(parent context.Context, sql string) (_ *int64, err error) {
	if e == nil || parent == nil || strings.TrimSpace(sql) == "" || len(sql) > 1<<20 {
		return nil, query.NewError("INVALID_ARGUMENT", "Invalid statement")
	}
	ctx, cancel := context.WithTimeout(parent, e.limits.Timeout)
	defer cancel()
	var result response
	status, wire, err := e.client.DoText(ctx, http.MethodPost, "/v1/statement", sql, e.headers, &result)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK || !safeSegment.MatchString(result.ID) {
		return nil, query.NewError("QUERY_FAILED", "Invalid statement handle")
	}
	id := result.ID
	cleanupPath := ""
	defer func() {
		if err != nil && cleanupPath != "" {
			cleanup, stop := context.WithTimeout(context.Background(), 2*time.Second)
			defer stop()
			_, _, _ = e.client.Do(cleanup, http.MethodDelete, cleanupPath, nil, e.headers, nil)
		}
	}()
	seen := map[[32]byte]bool{}
	for polls := 0; ; polls++ {
		if wire > min(e.limits.MaxBytes, 8<<20) {
			return nil, query.NewError("RESOURCE_EXHAUSTED", "Statement acknowledgement budget exceeded")
		}
		if result.ID != id || nonNullJSON(result.Error) || nonNullJSON(result.BinaryData) {
			return nil, query.NewError("QUERY_FAILED", "Statement acknowledgement failed")
		}
		if result.Next == "" {
			cleanupPath = ""
			if result.UpdateType == "" || len(result.UpdateType) > 256 {
				return nil, query.NewError("QUERY_FAILED", "Statement update acknowledgement missing")
			}
			if result.UpdateCount == nil {
				return nil, nil
			}
			n, parseErr := result.UpdateCount.Int64()
			if parseErr != nil || n < 0 {
				return nil, query.NewError("QUERY_FAILED", "Invalid affected row count")
			}
			return &n, nil
		}
		next, pathErr := e.nextPath(result.Next, id)
		if pathErr != nil {
			return nil, pathErr
		}
		cleanupPath = next
		key := sha256.Sum256([]byte(next))
		if seen[key] || polls >= 8191 || wire > min(e.limits.MaxBytes, 8<<20) {
			return nil, query.NewError("RESOURCE_EXHAUSTED", "Statement acknowledgement budget exceeded")
		}
		seen[key] = true
		if err = cloudapi.Poll(ctx); err != nil {
			return nil, err
		}
		var page response
		var bytes int64
		status, bytes, err = e.client.Do(ctx, http.MethodGet, next, nil, e.headers, &page)
		wire += bytes
		if err != nil {
			return nil, err
		}
		if status != http.StatusOK {
			return nil, query.NewError("QUERY_FAILED", "Invalid statement page status")
		}
		result = page
	}
}
