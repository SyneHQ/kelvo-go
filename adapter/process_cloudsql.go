// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package adapter

import (
	"net/url"
	"regexp"
	"strings"
	"unicode/utf8"
)

var cloudSQLAccount = regexp.MustCompile(`^[a-fA-F0-9]{32}$`)
var cloudSQLDatabase = regexp.MustCompile(`^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$`)
var cloudSQLWarehouse = regexp.MustCompile(`^[A-Za-z0-9-]{1,128}$`)
var cloudSQLIdentifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_-]{0,127}$`)

func CloudSQL(engine string) bool { return engine == "d1" || engine == "databricks" }

func ValidateCloudSQLProcessSource(s ConnectionSpec) error {
	if !CloudSQL(s.Engine) || s.DSN != "" || s.Username != "" || s.Password != "" || s.Token == "" || len(s.Token) > 16<<10 || !utf8.ValidString(s.Token) || strings.ContainsAny(s.Token, "\x00\r\n") || len(s.Options) > 5 {
		return ErrInvalid
	}
	u, err := url.Parse(s.URL)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.RawPath != "" || u.Opaque != "" || (u.Path != "" && u.Path != "/") || len(s.URL) > 8192 || strings.ContainsAny(s.URL, "\\\x00\r\n\t ") {
		return ErrInvalid
	}
	for key, value := range s.Options {
		if !utf8.ValidString(value) || strings.ContainsRune(value, 0) || len(value) > 64<<10 {
			return ErrInvalid
		}
		switch key {
		case "tls_ca_pem", "tls_server_name":
		case "account_id", "database_id":
			if s.Engine != "d1" {
				return ErrInvalid
			}
		case "warehouse_id", "catalog", "schema":
			if s.Engine != "databricks" {
				return ErrInvalid
			}
		default:
			return ErrUnsupported
		}
	}
	if s.Engine == "d1" {
		if s.URL != "https://api.cloudflare.com" || s.Schema != "" || !cloudSQLAccount.MatchString(s.Options["account_id"]) || !cloudSQLDatabase.MatchString(s.Database) || s.Options["database_id"] != s.Database {
			return ErrInvalid
		}
	} else if !cloudSQLWarehouse.MatchString(s.Options["warehouse_id"]) || s.Options["catalog"] != s.Database || s.Options["schema"] != s.Schema || (s.Database != "" && !cloudSQLIdentifier.MatchString(s.Database)) || (s.Schema != "" && !cloudSQLIdentifier.MatchString(s.Schema)) {
		return ErrInvalid
	}
	return nil
}
