//go:build !linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package containment

func (*Manager) PrepareProcess(Limits, func()) (Process, error) {
	return nil, ErrUnsupported
}
