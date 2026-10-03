//go:build !linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package exports

import (
	"context"
	"github.com/apache/arrow-go/v18/arrow"
)

type Store struct{}
type Writer struct{}
type Reader struct{}
type Part struct{}

func Open(Config) (*Store, error)                                { return nil, ErrUnsupported }
func (*Store) Close() error                                      { return ErrUnsupported }
func (*Store) Reserve(context.Context, Request) (*Writer, error) { return nil, ErrUnsupported }
func (*Store) Begin(context.Context, Request, *arrow.Schema) (*Writer, error) {
	return nil, ErrUnsupported
}
func (*Store) Cancel(context.Context, string, Identity) error { return ErrUnsupported }
func (*Store) Cleanup(context.Context, int) (CleanupResult, error) {
	return CleanupResult{}, ErrUnsupported
}
func (*Store) Acquire(context.Context, string, Identity) (*Reader, error) { return nil, ErrUnsupported }
func (*Writer) ID() string                                                { return "" }
func (*Writer) Fence() string                                             { return "" }
func (*Writer) BindSchema(context.Context, Identity, *arrow.Schema) error { return ErrUnsupported }
func (*Writer) Write(context.Context, arrow.RecordBatch) error            { return ErrUnsupported }
func (*Writer) Commit(context.Context, Identity) (Manifest, error)        { return Manifest{}, ErrUnsupported }
func (*Writer) Close() error                                              { return ErrUnsupported }
func (*Reader) Manifest() Manifest                                        { return Manifest{} }
func (*Reader) OpenPart(context.Context, int, Identity) (*Part, error)    { return nil, ErrUnsupported }
func (*Reader) Close() error                                              { return ErrUnsupported }
func (*Part) Read([]byte) (int, error)                                    { return 0, ErrUnsupported }
func (*Part) Close() error                                                { return ErrUnsupported }
