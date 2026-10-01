// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package catalog

import (
	"errors"
	"strings"
)

// ValidateEnvironment accepts only the dedicated KELVO_SOURCE_* namespace and
// these historical connection names: KELVO_POSTGRES_DSN, KELVO_PG_DSN,
// KELVO_MYSQL_DSN, KELVO_CLICKHOUSE_URL, KELVO_CLICKHOUSE_USER,
// KELVO_CLICKHOUSE_PASSWORD, and KELVO_CH_URL. Catalog administrators are trusted;
// the narrow namespace prevents accidental forwarding of loader/runtime, broker,
// API, TLS, cloud, or database-default environment variables into query workers.
// In particular, this must never become a broad KELVO_* allowlist.
func ValidateEnvironment(name string) error {
	valid := len(name) > len("KELVO_SOURCE_") && strings.HasPrefix(name, "KELVO_SOURCE_")
	switch name {
	case "KELVO_POSTGRES_DSN", "KELVO_PG_DSN", "KELVO_MYSQL_DSN",
		"KELVO_CLICKHOUSE_URL", "KELVO_CLICKHOUSE_USER", "KELVO_CLICKHOUSE_PASSWORD", "KELVO_CH_URL":
		valid = true
	}
	for _, c := range name {
		if !(c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_') {
			valid = false
			break
		}
	}
	if !valid {
		return errors.New("source environment references must use KELVO_SOURCE_* or a supported legacy connection name")
	}
	return nil
}
