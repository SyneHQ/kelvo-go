package provider

import "strings"

// NativeTextSupported selects compatibility parsing, not an execution backend.
// The worker still uses each database's native adapter and capability checks.
func NativeTextSupported(kind string) bool {
	kind = strings.ToLower(kind)
	return Supported(kind) || kind == "mongodb" || kind == "elasticsearch"
}
