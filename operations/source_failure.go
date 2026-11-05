// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package operations

import (
	"strconv"
	"strings"
)

// SourceFailure says why a source connection or query failed, in a fixed
// vocabulary for operator logs. It never holds SQL, credentials, bodies or URLs.
type SourceFailure struct {
	Reason         string `json:"reason"`
	HTTPStatus     int    `json:"http_status,omitempty"`
	ClickHouseCode string `json:"clickhouse_code,omitempty"`
	Host           string `json:"host,omitempty"`
}

var sourceFailureReasons = map[string]bool{"dns": true, "connect_refused": true, "connect_timeout": true, "tls_unknown_authority": true, "tls_hostname": true, "tls_other": true, "http_status": true, "arrow_stream": true, "canceled": true, "other": true}

// ExceptionCode returns v when it is a ClickHouse exception code (1-6 digits).
func ExceptionCode(v string) string {
	if v == "" || len(v) > 6 || strings.Trim(v, "0123456789") != "" {
		return ""
	}
	return v
}

func validFailureHost(v string) bool {
	return len(v) <= 253 && strings.Trim(strings.ToLower(v), "abcdefghijklmnopqrstuvwxyz0123456789.-:") == ""
}

// Validate is strict: every field must already be in its sanitized form.
func (f SourceFailure) Validate() error {
	if !sourceFailureReasons[f.Reason] || (f.HTTPStatus != 0 && (f.HTTPStatus < 100 || f.HTTPStatus > 999)) || (f.ClickHouseCode != "" && ExceptionCode(f.ClickHouseCode) == "") || !validFailureHost(f.Host) {
		return ErrInvalid
	}
	return nil
}

// LogLine re-validates every field, so a line from a child process stays bounded.
func (f SourceFailure) LogLine() string {
	reason, status, code, host := "other", "-", "-", "-"
	if sourceFailureReasons[f.Reason] {
		reason = f.Reason
	}
	if f.HTTPStatus >= 100 && f.HTTPStatus <= 999 {
		status = strconv.Itoa(f.HTTPStatus)
	}
	if ExceptionCode(f.ClickHouseCode) != "" {
		code = f.ClickHouseCode
	}
	if f.Host != "" && validFailureHost(f.Host) {
		host = f.Host
	}
	return "kelvo clickhouse source failed: reason=" + reason + " http_status=" + status + " clickhouse_code=" + code + " host=" + host
}
