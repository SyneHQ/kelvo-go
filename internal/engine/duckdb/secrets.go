//go:build duckdb_arrow

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package duckdb

import (
	"errors"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/SYNEHQ/kelvo-go/internal/query"
)

// sourceSecret keeps private DSNs out of duckdb_databases().path and its aliases.
// PostgreSQL's URI secret option passes the original libpq connection string
// through unchanged. MySQL exposes individual secret options, so its documented
// connection grammar must be parsed; unknown options are never silently dropped.
func sourceSecret(sourceType, name, dsn string) (statement, publicPath string, err error) {
	if strings.ContainsRune(dsn, 0) {
		return "", "", query.NewError("CONFIGURATION_ERROR", "Source connection string contains an invalid character")
	}
	prefix := "CREATE TEMPORARY SECRET " + quoteIdentifier(name) + " (TYPE "
	switch sourceType {
	case "postgres":
		return prefix + "postgres, URI '" + quoteLiteral(dsn) + "')", "", nil
	case "mysql":
		options, parseErr := parseMySQLDSN(dsn)
		if parseErr != nil {
			return "", "", query.NewError("CONFIGURATION_ERROR", parseErr.Error())
		}
		keys := make([]string, 0, len(options))
		for key := range options {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		secretOptions := []string{"mysql"}
		var publicOptions []string
		for _, key := range keys {
			value := options[key]
			if strings.ContainsRune(value, 0) {
				return "", "", query.NewError("CONFIGURATION_ERROR", "Source connection string contains an invalid character")
			}
			switch key {
			case "connect_timeout":
				// Current upstream source accepts this DSN option, but the
				// approved v1.5.6 extension does not. Do not silently discard it.
				return "", "", query.NewError("CONFIGURATION_ERROR", "MySQL connect_timeout is unsupported by the pinned DuckDB connector")
			case "compress", "compression":
				// The pinned MySQL secret API does not accept these options. Only
				// strictly validated numeric/enum values may enter metadata paths.
				if !validPublicMySQLOption(key, value) {
					return "", "", query.NewError("CONFIGURATION_ERROR", "Source connection string has an invalid public MySQL option")
				}
				publicOptions = append(publicOptions, key+"="+value)
			default:
				secretOptions = append(secretOptions, key+" '"+quoteLiteral(value)+"'")
			}
		}
		return prefix + strings.Join(secretOptions, ", ") + ")", strings.Join(publicOptions, " "), nil
	default:
		return "", "", errors.New("source does not support connection secrets")
	}
}

func validPublicMySQLOption(key, value string) bool {
	switch key {
	case "compress":
		return value == "0" || value == "1" || strings.EqualFold(value, "true") || strings.EqualFold(value, "false")
	case "compression":
		return strings.EqualFold(value, "disabled") || strings.EqualFold(value, "required") || strings.EqualFold(value, "preferred")
	}
	return false
}

func addMySQLOption(options map[string]string, key, value string) error {
	key = strings.ToLower(key)
	switch key {
	case "passwd":
		key = "password"
	case "db":
		key = "database"
	case "unix_socket":
		key = "socket"
	}
	switch key {
	case "host", "port", "password", "user", "database", "socket",
		"ssl_mode", "ssl_ca", "ssl_capath", "ssl_cert", "ssl_cipher", "ssl_crl", "ssl_crlpath", "ssl_key",
		"compress", "compression", "connect_timeout":
	default:
		return errors.New("source connection string uses an unsupported MySQL option")
	}
	if _, exists := options[key]; exists {
		return errors.New("source connection string repeats a MySQL option")
	}
	if key == "compress" || key == "compression" {
		other := "compress"
		if key == other {
			other = "compression"
		}
		if _, exists := options[other]; exists {
			return errors.New("source connection string repeats a MySQL compression option")
		}
	}
	if key == "port" {
		if _, err := strconv.ParseUint(value, 10, 16); err != nil || value == "" {
			return errors.New("source connection string has an invalid MySQL port")
		}
	}
	options[key] = value
	return nil
}

func parseMySQLDSN(dsn string) (map[string]string, error) {
	dsn = strings.TrimSpace(dsn)
	if dsn == "" || strings.ContainsRune(dsn, 0) {
		return nil, errors.New("source connection string is invalid")
	}
	// DuckDB accepts both explicit MySQL URIs and scheme-less host/user forms.
	equals := strings.IndexByte(dsn, '=')
	if equals < 0 || strings.ContainsAny(dsn[:equals], ":/?@") {
		return parseMySQLURI(dsn)
	}
	options := make(map[string]string)
	pos := 0
	for pos < len(dsn) {
		for pos < len(dsn) && mysqlSpace(dsn[pos]) {
			pos++
		}
		if pos == len(dsn) {
			break
		}
		key, err := mysqlValue(dsn, &pos)
		if err != nil || pos >= len(dsn) || dsn[pos] != '=' {
			return nil, errors.New("source connection string requires MySQL key=value pairs")
		}
		pos++
		value, err := mysqlValue(dsn, &pos)
		if err != nil {
			return nil, err
		}
		if pos < len(dsn) && !mysqlSpace(dsn[pos]) {
			return nil, errors.New("source connection string requires separated MySQL options")
		}
		if err := addMySQLOption(options, key, value); err != nil {
			return nil, err
		}
	}
	return options, nil
}

func mysqlSpace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\r' || b == '\n' || b == '\v' || b == '\f'
}

// mysqlValue follows the extension's key=value grammar: double quotes protect
// spaces and equals signs; only backslash and double quote may be backslash-
// escaped inside them. Single quotes are ordinary characters, not delimiters.
func mysqlValue(dsn string, pos *int) (string, error) {
	for *pos < len(dsn) && mysqlSpace(dsn[*pos]) {
		*pos++
	}
	if *pos == len(dsn) {
		return "", errors.New("source connection string has a missing MySQL value")
	}
	if dsn[*pos] != '"' {
		start := *pos
		for *pos < len(dsn) && !mysqlSpace(dsn[*pos]) && dsn[*pos] != '=' {
			*pos++
		}
		if start == *pos {
			return "", errors.New("source connection string has an invalid MySQL value")
		}
		return dsn[start:*pos], nil
	}
	*pos++
	var value strings.Builder
	for *pos < len(dsn) {
		c := dsn[*pos]
		*pos++
		switch c {
		case '"':
			return value.String(), nil
		case '\\':
			if *pos == len(dsn) || (dsn[*pos] != '\\' && dsn[*pos] != '"') {
				return "", errors.New("source connection string has an invalid MySQL escape")
			}
			value.WriteByte(dsn[*pos])
			*pos++
		default:
			value.WriteByte(c)
		}
	}
	return "", errors.New("source connection string has an unterminated MySQL quote")
}

func parseMySQLURI(dsn string) (map[string]string, error) {
	if !strings.Contains(dsn, "://") {
		dsn = "mysql://" + dsn
	}
	u, err := url.Parse(dsn)
	if err != nil || (u.Scheme != "mysql" && u.Scheme != "mysqlx") || u.Opaque != "" || u.Fragment != "" {
		return nil, errors.New("source connection string is not a valid MySQL URI")
	}
	options := make(map[string]string)
	if u.Hostname() != "" {
		options["host"] = u.Hostname()
	}
	if u.Port() != "" {
		if err := addMySQLOption(options, "port", u.Port()); err != nil {
			return nil, err
		}
	}
	if u.User != nil {
		options["user"] = u.User.Username()
		if password, exists := u.User.Password(); exists {
			options["password"] = password
		}
	}
	if u.Path != "" {
		if !strings.HasPrefix(u.Path, "/") || strings.Contains(strings.TrimPrefix(u.EscapedPath(), "/"), "/") {
			return nil, errors.New("source connection string has an invalid MySQL database path")
		}
		options["database"] = strings.TrimPrefix(u.Path, "/")
	}
	uriNames := map[string]string{
		"socket": "socket", "compression": "compression", "ssl-mode": "ssl_mode",
		"ssl-ca": "ssl_ca", "ssl-capath": "ssl_capath", "ssl-cert": "ssl_cert",
		"ssl-cipher": "ssl_cipher", "ssl-crl": "ssl_crl", "ssl-crlpath": "ssl_crlpath",
		"ssl-key": "ssl_key", "connect-timeout": "connect_timeout",
	}
	if u.RawQuery != "" {
		for _, pair := range strings.Split(u.RawQuery, "&") {
			key, value, found := strings.Cut(pair, "=")
			// MySQL URI attributes use percent encoding; '+' is literal, unlike
			// application/x-www-form-urlencoded and net/url.Values.
			key, keyErr := url.PathUnescape(key)
			value, valueErr := url.PathUnescape(value)
			name, supported := uriNames[key]
			if !found || !supported || keyErr != nil || valueErr != nil {
				return nil, errors.New("source connection string has an invalid MySQL URI option")
			}
			if err := addMySQLOption(options, name, value); err != nil {
				return nil, err
			}
		}
	}
	return options, nil
}
