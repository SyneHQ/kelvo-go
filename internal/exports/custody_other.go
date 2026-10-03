//go:build !linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package exports

import "context"

type Custody struct{}

func OpenCustody(context.Context, string, string, string) (*Custody, error) {
	return nil, ErrUnsupported
}
func (*Custody) ID() string            { return "" }
func (*Custody) DataDirectory() string { return "" }
func (*Custody) Close() error          { return ErrUnsupported }
