// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package sqlnative

import (
	"context"
	"errors"
	"strings"

	"github.com/SYNEHQ/kelvo-go/internal/query"
)

// Context takes precedence over driver classification, including wrapped driver
// cancellation when the parent context has not itself expired.
func contextError(ctx context.Context, err error) error {
	if cause := ctx.Err(); cause != nil {
		return query.PublicError(cause)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return query.PublicError(context.DeadlineExceeded)
	}
	if errors.Is(err, context.Canceled) {
		return query.PublicError(context.Canceled)
	}
	return nil
}

// sourceError never trusts a driver-supplied query.Error. Driver text crosses
// only when the adapter's ErrorDetail says the caller's SQL caused the error
// (syntax, unknown object, type mismatch): it returns INVALID_ARGUMENT with the
// server's own message so the caller can fix the statement. Every other error
// (auth, connection, privilege, configuration, timeout, capacity) keeps only a
// recognized code: its text can name hosts and roles.
func (e *Engine) sourceError(ctx context.Context, err error, message string) error {
	if cause := contextError(ctx, err); cause != nil {
		return cause
	}
	if e.dialect.ErrorDetail != nil {
		if detail := e.dialect.ErrorDetail(err); detail != "" {
			return query.NewError("INVALID_ARGUMENT", detail)
		}
	}
	code := "QUERY_FAILED"
	if e.dialect.ErrorCode != nil {
		switch mapped := e.dialect.ErrorCode(err); mapped {
		case "UNAUTHENTICATED", "PERMISSION_DENIED", "CONFIGURATION_ERROR", "INVALID_ARGUMENT",
			"UNAVAILABLE", "RESOURCE_EXHAUSTED", "NOT_SUPPORTED", "CANCELLED", "DEADLINE_EXCEEDED":
			code = mapped
		}
	}
	return query.NewError(code, message)
}

// Config callbacks are trusted application code and may produce intentional
// public errors. Raw errors returned by those callbacks are still sanitized.
func (e *Engine) callbackError(ctx context.Context, err error, message string) error {
	if cause := contextError(ctx, err); cause != nil {
		return cause
	}
	var public *query.Error
	if errors.As(err, &public) {
		return public
	}
	return e.sourceError(ctx, err, message)
}

// The parent-owned sink can reject a schema or resource limit. Preserve its
// explicit public error instead of losing the reason as a generic source error.
func localResultError(ctx context.Context, err error, message string) error {
	if cause := contextError(ctx, err); cause != nil {
		return cause
	}
	var public *query.Error
	if errors.As(err, &public) {
		return public
	}
	return query.NewError("QUERY_FAILED", message)
}

// DetailText makes server text one line of valid UTF-8, at most 500 runes.
func DetailText(s string) string {
	s = strings.Join(strings.Fields(strings.ToValidUTF8(s, "\uFFFD")), " ")
	if r := []rune(s); len(r) > 500 {
		s = string(r[:499]) + "…"
	}
	return s
}
