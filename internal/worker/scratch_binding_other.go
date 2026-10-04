//go:build !linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

func (*ScratchRoot) MatchesDirectory(string) bool { return false }
