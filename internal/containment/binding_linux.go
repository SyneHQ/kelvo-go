//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package containment

// MatchesConfig binds a caller to the configuration of an open, healthy manager.
// Defaults are compared after normalization. Delegation is never reopened or
// replaced here; the existing manager retains its verified kernel handles.
func (m *Manager) MatchesConfig(config Config) bool {
	if m == nil {
		return false
	}
	config, err := config.normalized()
	if err != nil {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.config == config && !m.draining && !m.closed && len(m.quarantined) == 0 &&
		m.hierarchy != nil && m.state != nil && m.lock != nil
}
