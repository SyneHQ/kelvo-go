//go:build !linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package operations

import "context"

type SQLiteBackend struct{}

func OpenSQLite(context.Context, string, Policy, string) (*SQLiteBackend, error) {
	return nil, ErrUnavailable
}
func (*SQLiteBackend) Get(context.Context, string) (Entry, error) { return Entry{}, ErrUnavailable }
func (*SQLiteBackend) Create(context.Context, string, []byte) (uint64, error) {
	return 0, ErrUnavailable
}
func (*SQLiteBackend) Update(context.Context, string, []byte, uint64) (uint64, error) {
	return 0, ErrUnavailable
}
func (*SQLiteBackend) Close() error { return nil }
