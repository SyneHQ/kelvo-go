// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cosmosdb

import (
	"strings"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/sources/sqlguard"
)

// simpleQuery admits only the subset this REST continuation pager can execute
// without an SDK query plan or client-side distributed result merging. This is
// deliberately conservative; the service still validates the Cosmos dialect.
func simpleQuery(input string) (string, error) {
	sql, err := sqlguard.ReadOnly(input)
	if err != nil {
		return "", err
	}
	deny := func() (string, error) {
		return "", query.NewError("UNSUPPORTED", "Cosmos DB REST supports simple SELECT projections and filters without distributed query operators")
	}
	selects := 0
	for i := 0; i < len(sql); {
		switch {
		case sql[i] == '`':
			return deny()
		case sql[i] == '\'' || sql[i] == '"':
			quote := sql[i]
			i++
			for i < len(sql) {
				if sql[i] == quote {
					i++
					if i < len(sql) && sql[i] == quote {
						i++
						continue
					}
					break
				}
				i++
			}
		case i+1 < len(sql) && sql[i:i+2] == "--":
			i += 2
			for i < len(sql) && sql[i] != '\n' {
				i++
			}
		case i+1 < len(sql) && sql[i:i+2] == "/*":
			i += 2
			for i+1 < len(sql) && sql[i:i+2] != "*/" {
				i++
			}
			i += 2
		case sql[i] >= 'a' && sql[i] <= 'z' || sql[i] >= 'A' && sql[i] <= 'Z' || sql[i] == '_':
			start := i
			for i < len(sql) && (sql[i] >= 'a' && sql[i] <= 'z' || sql[i] >= 'A' && sql[i] <= 'Z' || sql[i] >= '0' && sql[i] <= '9' || sql[i] == '_') {
				i++
			}
			switch strings.ToUpper(sql[start:i]) {
			case "SELECT":
				selects++
				if selects > 1 {
					return deny()
				}
			case "WITH", "DISTINCT", "ORDER", "GROUP", "TOP", "OFFSET", "LIMIT", "JOIN", "UNION", "INTERSECT", "EXCEPT", "HAVING", "AVG", "COUNT", "MIN", "MAX", "SUM", "FULLTEXTSCORE", "RRF":
				return deny()
			}
		default:
			i++
		}
	}
	if selects != 1 {
		return deny()
	}
	return sql, nil
}
