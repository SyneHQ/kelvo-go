// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import "testing"

func TestRouteReason(t *testing.T) {
	const ts = "2026-03-01T00:00:00Z"
	for _, tc := range []struct{ name, sql, covered, want string }{
		{"le within", "SELECT * FROM d WHERE ts <= '2026-03-01 00:00:00'", ts, ""},
		{"lt within", "SELECT * FROM d WHERE ts < '2026-02-01'", ts, ""},
		{"lt beyond", "SELECT * FROM d WHERE ts < '2026-04-01'", ts, RouteBeyondCovered},
		{"gt only", "SELECT * FROM d WHERE ts > '2026-01-01'", ts, RouteUnbounded},
		{"ge only", "SELECT * FROM d WHERE ts >= '2026-01-01'", ts, RouteUnbounded},
		{"eq within", "SELECT * FROM d WHERE ts = '2026-02-01'", ts, ""},
		{"eq beyond", "SELECT * FROM d WHERE ts = '2026-05-01'", ts, RouteBeyondCovered},
		{"between within", "SELECT * FROM d WHERE ts BETWEEN '2026-01-01' AND '2026-02-01'", ts, ""},
		{"between beyond", "SELECT * FROM d WHERE ts BETWEEN '2026-01-01' AND '2026-06-01'", ts, RouteBeyondCovered},
		{"literal first", "SELECT * FROM d WHERE '2026-02-01' > ts", ts, ""},
		{"and with other", "SELECT * FROM d WHERE region = 'x' AND (ts >= '2026-01-01' AND ts < '2026-02-01')", ts, ""},
		{"qualified alias", "SELECT * FROM d AS o WHERE o.ts < '2026-02-01'", ts, ""},
		{"wrong qualifier", "SELECT * FROM d AS o WHERE x.ts < '2026-02-01'", ts, RouteUnparsed},
		{"int column", "SELECT * FROM d WHERE ts <= 100", "100", ""},
		{"int beyond", "SELECT * FROM d WHERE ts <= 101", "100", RouteBeyondCovered},
		{"type mismatch", "SELECT * FROM d WHERE ts <= 100", ts, RouteUnparsed},
		{"or", "SELECT * FROM d WHERE ts < '2026-02-01' OR region = 'x'", ts, RouteUnparsed},
		{"function", "SELECT * FROM d WHERE DATE(ts) < '2026-02-01'", ts, RouteUnparsed},
		{"not between", "SELECT * FROM d WHERE ts NOT BETWEEN '2026-01-01' AND '2026-02-01'", ts, RouteUnparsed},
		{"non literal", "SELECT * FROM d WHERE ts < other", ts, RouteUnparsed},
		{"no where", "SELECT * FROM d", ts, RouteUnbounded},
		{"other column only", "SELECT * FROM d WHERE region = 'x'", ts, RouteUnbounded},
		{"join", "SELECT * FROM d JOIN e ON d.id = e.id WHERE ts < '2026-02-01'", ts, RouteUnparsed},
		{"subquery source", "SELECT * FROM (SELECT * FROM d) s WHERE ts < '2026-02-01'", ts, RouteUnparsed},
		{"union", "SELECT * FROM d UNION SELECT * FROM d", ts, RouteUnparsed},
		{"syntax error", "SELECT FROM WHERE", ts, RouteUnparsed},
		{"within beats confusing", "SELECT * FROM d WHERE ts < '2026-02-01' AND (ts > '2026-01-01' OR ts IS NULL)", ts, ""},
	} {
		if got := RouteReason(tc.sql, "d", "ts", tc.covered); got != tc.want {
			t.Errorf("%s: got %q, want %q", tc.name, got, tc.want)
		}
	}
}
