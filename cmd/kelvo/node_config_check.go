// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package main

import "strings"

// Detect the flag before parsing so parser errors cannot print its value.
func hasNodeConfigCheckFlag(args []string) bool {
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			return false
		}
		name, _, hasValue := strings.Cut(arg, "=")
		if name == "--check-config" || name == "-check-config" {
			return true
		}
		// FlagSet consumes the next token for value flags, even when that
		// token is "--". Only an unconsumed "--" ends flag parsing.
		if !hasValue && (name == "--config" || name == "-config" || name == "--drain-timeout" || name == "-drain-timeout") {
			i++
		}
	}
	return false
}
