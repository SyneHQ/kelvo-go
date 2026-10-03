//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

// MatchesDirectory verifies the live private inode behind an operator path.
// Startup callers can bind a shared executor to its configured managed root
// without exposing file descriptors or accepting a temporary-directory fallback.
func (s *ScratchRoot) MatchesDirectory(path string) bool {
	if s == nil || s.path != path {
		return false
	}
	if err := s.lock(); err != nil {
		return false
	}
	s.unlock()
	return true
}
