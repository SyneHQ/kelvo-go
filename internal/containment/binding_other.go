//go:build !linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package containment

func (*Manager) MatchesConfig(Config) bool { return false }
