//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package containment

// PrepareProcess keeps one command wait owner alongside delegated group custody.
// Finish uses the manager cleanup timeout and any earlier caller deadline.
func (m *Manager) PrepareProcess(limits Limits, release func()) (Process, error) {
	if m == nil {
		return nil, ErrInvalid
	}
	return prepareProcess(m, limits, release, m.config.CleanupTimeout)
}
