//go:build !linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package audit

import "context"

type Journal struct{}

func Open(Config, Scope) (*Journal, error)                              { return nil, ErrUnsupported }
func (*Journal) Begin(context.Context, Binding, Kind) (*Receipt, error) { return nil, ErrUnsupported }
func (*Journal) Record(context.Context, Binding, Kind, Outcome, Category) error {
	return ErrUnsupported
}
func (*Journal) Ready() bool                                   { return false }
func (*Journal) Snapshot() State                               { return State{} }
func (*Journal) Close(context.Context) error                   { return nil }
func ReadPage(context.Context, string, int, int) (Page, error) { return Page{}, ErrUnsupported }
func (*Journal) enqueueFinish(context.Context, *Receipt, Outcome, Category) (*operation, error) {
	return nil, ErrUnsupported
}
func (*Journal) await(context.Context, *operation) (*Receipt, error) { return nil, ErrUnsupported }
