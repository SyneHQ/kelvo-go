// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package provider

import (
	"encoding/json"
	"net/url"
	"regexp"
	"strings"

	"github.com/SYNEHQ/kelvo-go/operations"
)

type SaaSQuery struct {
	SQL string `json:"sql"`
}

func SaaS(kind string) bool {
	switch strings.ToLower(kind) {
	case "stripe", "ga4", "google_ads", "facebook_ads", "salesforce":
		return true
	}
	return false
}

// The provider adapter parses the complete query grammar. This public boundary
// only admits known read commands and never grants mutation authority.
func ParseSaaS(kind string, raw []byte) (SaaSQuery, error) {
	var q SaaSQuery
	if !SaaS(kind) || operations.DecodeStrict(raw, &q, operations.MaxRequestBytes) != nil || len(q.SQL) > 64<<10 || strings.ContainsAny(q.SQL, "\x00\r") {
		return q, operations.ErrInvalid
	}
	words, valid := saasWords(q.SQL)
	if !valid || len(words) == 0 {
		return q, operations.ErrInvalid
	}
	for _, word := range words {
		switch word {
		case "INSERT", "UPDATE", "DELETE", "MERGE", "CREATE", "ALTER", "DROP", "TRUNCATE", "GRANT", "REVOKE", "CALL", "COPY", "INTO", "OUTFILE", "EXEC", "EXECUTE":
			return q, operations.ErrInvalid
		}
	}
	switch words[0] {
	case "SELECT":
		return q, nil
	case "LIST", "SHOW", "DESCRIBE", "DESC":
		if kind == "ga4" || kind == "facebook_ads" {
			return q, nil
		}
	case "FIND":
		if kind == "google_ads" && len(words) > 1 && words[1] == "KEYWORDS" {
			return q, nil
		}
	}
	return q, operations.ErrInvalid
}

// Inspect SQL tokens outside literals. A value containing "delete" or a
// semicolon is data, not another statement. Comments are deliberately rejected.
func saasWords(sql string) ([]string, bool) {
	words := []string{}
	for i := 0; i < len(sql); {
		c := sql[i]
		if c == '\'' || c == '"' || c == '`' {
			quote := c
			i++
			closed := false
			for i < len(sql) {
				if sql[i] == '\\' {
					i += 2
					continue
				}
				if sql[i] == quote {
					if i+1 < len(sql) && sql[i+1] == quote {
						i += 2
						continue
					}
					i++
					closed = true
					break
				}
				i++
			}
			if !closed {
				return nil, false
			}
			continue
		}
		if c == ';' {
			return words, strings.TrimSpace(sql[i+1:]) == ""
		}
		if c == '#' || i+1 < len(sql) && (sql[i:i+2] == "--" || sql[i:i+2] == "/*") {
			return nil, false
		}
		if c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c == '_' {
			start := i
			i++
			for i < len(sql) && (sql[i] >= 'A' && sql[i] <= 'Z' || sql[i] >= 'a' && sql[i] <= 'z' || sql[i] >= '0' && sql[i] <= '9' || sql[i] == '_') {
				i++
			}
			words = append(words, strings.ToUpper(sql[start:i]))
			continue
		}
		i++
	}
	return words, true
}

func saasInvocation(kind string, raw []byte) (operations.Kind, *operations.NativeSpec, error) {
	if _, err := ParseSaaS(kind, raw); err != nil {
		return "", nil, err
	}
	return operations.NativeRead, &operations.NativeSpec{Provider: kind, Command: "query", Parameters: []operations.Parameter{{Type: "json", Value: append(json.RawMessage(nil), raw...)}}}, nil
}

var saasAccount = regexp.MustCompile(`^[0-9]{1,20}$`)

func ValidSaaSAccount(kind, id string) bool {
	switch kind {
	case "ga4", "google_ads":
		return saasAccount.MatchString(id)
	case "facebook_ads":
		return saasAccount.MatchString(strings.TrimPrefix(id, "act_"))
	case "stripe":
		return id == "" || regexp.MustCompile(`^acct_[A-Za-z0-9]{1,64}$`).MatchString(id)
	case "salesforce":
		return id == "" || regexp.MustCompile(`^[A-Za-z0-9]{15}([A-Za-z0-9]{3})?$`).MatchString(id)
	}
	return false
}

func ValidSalesforceOrigin(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.RawPath != "" || u.Opaque != "" || u.ForceQuery || u.Port() != "" && u.Port() != "443" || (u.Path != "" && u.Path != "/") || strings.ContainsAny(raw, "\\\x00\r\n\t ") {
		return false
	}
	host := strings.ToLower(u.Hostname())
	return strings.HasSuffix(host, ".salesforce.com") && host != "login.salesforce.com" && host != "test.salesforce.com"
}
