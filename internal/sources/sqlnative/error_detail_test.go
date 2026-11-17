// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package sqlnative

import (
	"context"
	"errors"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/query"
)

func TestSourceErrorDetailBecomesInvalidArgument(t *testing.T) {
	engine := &Engine{dialect: Dialect{ErrorCode: func(error) string { return "UNAVAILABLE" }, ErrorDetail: func(error) string { return "PostgreSQL error 42883: x" }}}
	got := query.PublicError(engine.sourceError(context.Background(), errors.New("driver"), "Source rejected query"))
	if got.Code != "INVALID_ARGUMENT" || got.Message != "PostgreSQL error 42883: x" {
		t.Fatalf("got %+v", got)
	}
	engine.dialect.ErrorDetail = func(error) string { return "" }
	got = query.PublicError(engine.sourceError(context.Background(), errors.New("driver"), "Source rejected query"))
	if got.Code != "UNAVAILABLE" || got.Message != "Source rejected query" {
		t.Fatalf("got %+v", got)
	}
}
