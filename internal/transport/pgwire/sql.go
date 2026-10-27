// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package pgwire

import (
	"github.com/SYNEHQ/kelvo-go/internal/sqllex"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// segment is a run of SQL text; only code segments are classified or rewritten.
type segment struct {
	text string
	code bool
}

// lex adapts the shared guard lexer. Input it cannot close becomes one
// non-code segment: nothing is rewritten and classify rejects it.
func lex(sql string) []segment {
	segs, err := sqllex.Lex(sql)
	if err != nil {
		return []segment{{sql, false}}
	}
	out := make([]segment, len(segs))
	for i, s := range segs {
		out[i] = segment{s.Text, s.Kind == sqllex.Code}
	}
	return out
}

// split returns non-empty statements separated by top-level semicolons.
func split(sql string) []string {
	var out []string
	var cur strings.Builder
	flush := func() {
		if s := strings.TrimSpace(cur.String()); s != "" && !onlyComments(s) {
			out = append(out, s)
		}
		cur.Reset()
	}
	for _, s := range lex(sql) {
		if !s.code {
			cur.WriteString(s.text)
			continue
		}
		parts := strings.Split(s.text, ";")
		for i, p := range parts {
			if i > 0 {
				flush()
			}
			cur.WriteString(p)
		}
	}
	flush()
	return out
}

func onlyComments(sql string) bool {
	for _, s := range lex(sql) {
		if s.code && strings.TrimSpace(s.text) != "" {
			return false
		}
	}
	return true
}

// words returns lower-case code words in order; quoted identifiers are names.
func words(sql string) []string {
	segs, err := sqllex.Lex(sql)
	if err != nil {
		return nil
	}
	return sqllex.Words(segs, false)
}

// Statement kinds.
const (
	kindQuery = iota
	kindBegin
	kindCommit
	kindRollback
	kindSet
	kindShow
	kindNoop
)

// writeWords fail closed: a read statement mentioning one is rejected even when
// the word is an unquoted column name. Quote such identifiers.
var writeWords = map[string]bool{
	"insert": true, "update": true, "delete": true, "merge": true, "upsert": true, "create": true,
	"drop": true, "alter": true, "copy": true, "attach": true, "detach": true, "install": true,
	"load": true, "pragma": true, "call": true, "export": true, "import": true, "truncate": true,
	"grant": true, "revoke": true, "vacuum": true, "checkpoint": true, "use": true,
}

// classify returns the statement kind, or a SQLSTATE error.
func classify(sql string) (int, *pgError) {
	if _, err := sqllex.Lex(sql); err != nil {
		return 0, &pgError{code: "42601", msg: "unterminated quoted string, identifier or comment"}
	}
	w := words(quoteAliases(sql))
	if len(w) == 0 {
		return kindQuery, nil
	}
	switch w[0] {
	case "select", "with", "values", "table":
		for _, x := range w {
			if writeWords[x] {
				return 0, readOnly(x)
			}
		}
		return kindQuery, nil
	case "begin", "start":
		return kindBegin, nil
	case "commit", "end":
		return kindCommit, nil
	case "rollback", "abort":
		return kindRollback, nil
	case "set":
		return kindSet, nil
	case "show":
		return kindShow, nil
	case "reset", "discard", "deallocate", "savepoint", "release", "close":
		return kindNoop, nil
	case "insert", "update", "delete", "merge", "create", "drop", "alter", "truncate", "grant", "revoke", "copy", "attach", "detach", "install", "load", "vacuum", "checkpoint", "pragma", "call", "import", "export", "use", "comment", "lock", "refresh", "reindex", "cluster", "do", "prepare", "execute", "upsert":
		return 0, readOnly(w[0])
	}
	return 0, &pgError{code: "0A000", msg: "statement is not supported by the read-only Kelvo PostgreSQL interface"}
}

func readOnly(word string) *pgError {
	return &pgError{code: "25006", msg: "cannot execute " + strings.ToUpper(word) + " in a read-only Kelvo session"}
}

// sqlText keeps the original SQL beside a copy whose literal and comment
// bytes are masked, so rewrites match code and quoted names only.
type sqlText struct{ s, m string }

func newSQLText(sql string) sqlText {
	m := []byte(sql)
	segs, err := sqllex.Lex(sql)
	if err != nil {
		segs = []sqllex.Segment{{Text: sql, Kind: sqllex.Literal}}
	}
	off := 0
	for _, seg := range segs {
		seg := segment{seg.Text, seg.Kind == sqllex.Code || seg.Kind == sqllex.Identifier}
		if !seg.code {
			for i := off; i < off+len(seg.text); i++ {
				m[i] = 1
			}
		}
		off += len(seg.text)
	}
	return sqlText{sql, string(m)}
}

func (t *sqlText) splice(start, end int, rep string) {
	r := newSQLText(rep)
	t.s, t.m = t.s[:start]+rep+t.s[end:], t.m[:start]+r.m+t.m[end:]
}

// replace rewrites each code match, last first so offsets stay valid.
func (t *sqlText) replace(re *regexp.Regexp, f func(sub []string) string) {
	locs := re.FindAllStringSubmatchIndex(t.m, -1)
	for i := len(locs) - 1; i >= 0; i-- {
		l := locs[i]
		sub := make([]string, len(l)/2)
		for j := range sub {
			if l[2*j] >= 0 {
				sub[j] = t.s[l[2*j]:l[2*j+1]]
			}
		}
		t.splice(l[0], l[1], f(sub))
	}
}

// replaceCalls swaps a whole function call, balanced arguments included.
func (t *sqlText) replaceCalls(re *regexp.Regexp, rep string) {
	locs := re.FindAllStringIndex(t.m, -1)
	for i := len(locs) - 1; i >= 0; i-- {
		depth := 1
		for j := locs[i][1]; j < len(t.m); j++ {
			switch t.m[j] {
			case '(':
				depth++
			case ')':
				depth--
			}
			if depth == 0 {
				t.splice(locs[i][0], j+1, rep)
				break
			}
		}
	}
}

func call(names string) *regexp.Regexp {
	return regexp.MustCompile(`(?i)\b(?:pg_catalog\s*\.\s*)?(?:` + names + `)\s*\(`)
}

var (
	operatorRE        = regexp.MustCompile(`(?i)\bOPERATOR\s*\(\s*pg_catalog\s*\.\s*([~!<>=]+)\s*\)`)
	collateRE         = regexp.MustCompile(`(?i)\bCOLLATE\s+pg_catalog\s*\.\s*"?default"?`)
	qualifiedRE       = regexp.MustCompile(`(?i)::\s*pg_catalog\s*\.\s*`)
	versionRE         = call("version")
	databaseRE        = call("current_database")
	ownerRE           = call("pg_get_userbyid")
	encodingRE        = call("pg_encoding_to_char")
	describeRE        = call("obj_description|col_description|shobj_description|pg_get_expr|pg_get_statisticsobjdef_columns|pg_get_statisticsobjdef_expressions|pg_get_triggerdef|pg_get_ruledef|pg_get_partkeydef|pg_get_partition_constraintdef")
	privilegeRE       = call("has_table_privilege|has_schema_privilege|has_database_privilege|has_column_privilege|has_any_column_privilege")
	builtinRE         = regexp.MustCompile(`(?i)\bpg_catalog\s*\.\s*(array_to_string|array_length|unnest|generate_series|quote_ident|lower|upper|concat|textcat|length|substring|regexp_replace|left|right|btrim|trim)\s*\(`)
	expandArrayRE     = regexp.MustCompile(`(?i)\b(?:information_schema\s*\.\s*)?_pg_expandarray\s*\(`)
	regclassLiteralRE = regexp.MustCompile(`(\x01+)\s*::\s*regclass\b`)
	regTypeRE         = regexp.MustCompile(`(?i)::\s*(?:regtype|regclass|regproc|regnamespace|regrole|oid)\b`)
	paramRE           = regexp.MustCompile(`\$([0-9]+)`)
	catalogCalls      = []struct {
		re  *regexp.Regexp
		rep string
	}{{ownerRE, "'kelvo'"}, {encodingRE, "'UTF8'"}, {describeRE, "CAST(NULL AS VARCHAR)"}, {privilegeRE, "true"}, {call("pg_relation_is_publishable"), "false"}, {call("array_upper|array_lower"), "CAST(NULL AS INTEGER)"}}
)

// aliasRE matches a reserved word used as a bare column alias, as Metabase's
// privilege query does ("... AS update"). Quoting makes it a name, which is
// all it is; "AS (DELETE ..." does not match and stays a keyword.
var aliasRE = regexp.MustCompile(`(?i)\bAS(\s+)(select|table|update|insert|delete|merge|create|drop|alter|copy|grant|references|trigger|truncate|usage|execute)\b`)

func quoteAliases(sql string) string {
	t := newSQLText(sql)
	t.replace(aliasRE, func(s []string) string { return "AS" + s[1] + `"` + strings.ToLower(s[2]) + `"` })
	return t.s
}

// policyRolesRE is psql's correlated role list in the \d policy footer, which
// DuckDB cannot bind; Kelvo has no row-security policies, so it is NULL.
var policyRolesRE = regexp.MustCompile(`(?i)(?:pg_catalog\s*\.\s*)?array_to_string\s*\(\s*array\s*\(\s*select\s+rolname\s+from\s+(?:pg_catalog\s*\.\s*)?pg_roles\s+where\s+oid\s*=\s*any\s*\(\s*pol\.polroles\s*\)\s*order\s+by\s+1\s*\)\s*,\s*\x01+\s*\)`)

// statKindRE is psql's extended-statistics kind test; pg_statistic_ext is empty.
var statKindRE = regexp.MustCompile(`(?i)\x01+\s*=\s*any\s*\(\s*stxkind\s*\)`)

// ServerVersion is reported to clients; tools parse its leading number.
const ServerVersion = "16.0"

// rewrite adapts PostgreSQL-only catalog syntax BI tools send to DuckDB and
// wraps typed placeholders so values keep their exact type. Privilege checks
// become true because only the principal's authorized sources are attached.
func rewrite(sql, database string, casts map[int]string) string {
	t := newSQLText(quoteAliases(sql))
	t.replace(policyRolesRE, func([]string) string { return "CAST(NULL AS VARCHAR)" })
	t.replace(statKindRE, func([]string) string { return "false" })
	t.replace(operatorRE, func(s []string) string { return s[1] })
	t.replace(collateRE, func([]string) string { return "" })
	t.replace(qualifiedRE, func([]string) string { return "::" })
	t.replaceCalls(versionRE, "'PostgreSQL "+ServerVersion+" (Kelvo)'")
	t.replaceCalls(databaseRE, quoteLiteral(database))
	// pgjdbc getPrimaryKeys expands pg_index.indkey; DuckDB reports no
	// primary-key indexes, so the expansion only needs the right shape.
	t.replaceCalls(expandArrayRE, "{'x': CAST(NULL AS INTEGER), 'n': CAST(NULL AS INTEGER)}")
	for _, c := range catalogCalls {
		t.replaceCalls(c.re, c.rep)
	}
	t.replace(builtinRE, func(s []string) string { return s[1] + "(" })
	t.replace(regclassLiteralRE, func(s []string) string {
		return "(SELECT oid FROM pg_catalog.pg_class WHERE relname = " + s[1] + " LIMIT 1)"
	})
	t.replace(regTypeRE, func([]string) string { return "::BIGINT" })
	shadowCatalog(&t)
	if len(casts) > 0 {
		t.replace(paramRE, func(s []string) string {
			n, err := strconv.Atoi(s[1])
			if ty, ok := casts[n]; ok && err == nil {
				return "CAST(" + s[0] + " AS " + ty + ")"
			}
			return s[0]
		})
	}
	return t.s
}

func quoteLiteral(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

// shadows complete DuckDB's partial pg_catalog with the columns and relations
// psql and JDBC introspection read. Empty relations describe features Kelvo
// does not have (policies, publications, inheritance, triggers). Source views
// are reported as tables: every Kelvo relation is a read-only table.
var shadows = map[string]string{
	"pg_class": `SELECT * REPLACE (CASE relkind WHEN 'v' THEN 'r' ELSE relkind END AS relkind), false AS relforcerowsecurity FROM pg_catalog.pg_class`,
	"pg_database": `SELECT oid, datname, 10 AS datdba, 6 AS encoding, 'c' AS datlocprovider, 'C' AS datcollate, 'C' AS datctype, CAST(NULL AS VARCHAR) AS daticulocale,
		CAST(NULL AS VARCHAR) AS daticurules, CAST(NULL AS VARCHAR) AS datlocale, CAST(NULL AS VARCHAR[]) AS datacl, true AS datallowconn, -1 AS datconnlimit,
		0 AS dattablespace, false AS datistemplate FROM pg_catalog.pg_database`,
	"pg_roles": `SELECT 10 AS oid, 'kelvo' AS rolname, false AS rolsuper, false AS rolinherit, false AS rolcreaterole, false AS rolcreatedb, true AS rolcanlogin,
		false AS rolreplication, false AS rolbypassrls, -1 AS rolconnlimit, CAST(NULL AS TIMESTAMP) AS rolvaliduntil`,
	"pg_user":                  `SELECT 'kelvo' AS usename, 10 AS usesysid, false AS usecreatedb, false AS usesuper`,
	"pg_collation":             `SELECT 0 AS oid, '' AS collname, 0 AS collnamespace, 'c' AS collprovider WHERE false`,
	"pg_inherits":              `SELECT 0 AS inhrelid, 0 AS inhparent, 0 AS inhseqno, false AS inhdetachpending WHERE false`,
	"pg_policy":                `SELECT 0 AS oid, '' AS polname, 0 AS polrelid, '' AS polcmd, true AS polpermissive, 0 AS polroles, '' AS polqual, '' AS polwithcheck WHERE false`,
	"pg_statistic_ext":         `SELECT 0 AS oid, 0 AS stxrelid, '' AS stxname, 0 AS stxnamespace, 10 AS stxowner, -1 AS stxstattarget, '' AS stxkeys, '' AS stxkind, '' AS stxexprs WHERE false`,
	"pg_publication":           `SELECT 0 AS oid, '' AS pubname, false AS puballtables, false AS pubinsert, false AS pubupdate, false AS pubdelete, false AS pubtruncate, false AS pubviaroot WHERE false`,
	"pg_publication_rel":       `SELECT 0 AS oid, 0 AS prpubid, 0 AS prrelid, '' AS prqual, '' AS prattrs WHERE false`,
	"pg_publication_namespace": `SELECT 0 AS oid, 0 AS pnpubid, 0 AS pnnspid WHERE false`,
	"pg_trigger":               `SELECT 0 AS oid, 0 AS tgrelid, 0 AS tgparentid, '' AS tgname, '' AS tgenabled, false AS tgisinternal WHERE false`,
	"pg_rewrite":               `SELECT 0 AS oid, '' AS rulename, 0 AS ev_class, '' AS ev_type, '' AS ev_enabled WHERE false`,
	"pg_extension":             `SELECT 0 AS oid, '' AS extname, 10 AS extowner, 0 AS extnamespace, '' AS extversion WHERE false`,
	"pg_foreign_table":         `SELECT 0 AS ftrelid, 0 AS ftserver, '' AS ftoptions WHERE false`,
	"pg_partitioned_table":     `SELECT 0 AS partrelid, '' AS partstrat, 0 AS partnatts WHERE false`,
	"pg_stat_user_tables": `SELECT c.oid AS relid, n.nspname AS schemaname, c.relname, 0 AS seq_scan, 0 AS idx_scan, 0 AS n_live_tup, 0 AS n_dead_tup,
		CAST(NULL AS TIMESTAMPTZ) AS last_analyze, CAST(NULL AS TIMESTAMPTZ) AS last_autoanalyze FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace`,
	// The engine denies pg_views (it exposes view SQL); names come from pg_class.
	"pg_views": `SELECT n.nspname AS schemaname, c.relname AS viewname, 'kelvo' AS viewowner, CAST(NULL AS VARCHAR) AS definition
		FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind = 'v'`,
	"pg_matviews": `SELECT '' AS schemaname, '' AS matviewname, 'kelvo' AS matviewowner, '' AS definition WHERE false`,
}

var shadowRE = func() *regexp.Regexp {
	names := make([]string, 0, len(shadows))
	for n := range shadows {
		names = append(names, n)
	}
	sort.Strings(names)
	return regexp.MustCompile(`(?i)(^|[^.\w"])(?:"?pg_catalog"?\s*\.\s*)?"?(` + strings.Join(names, "|") + `)\b"?`)
}()

var leadingWithRE = regexp.MustCompile(`(?i)^[\s\x01]*WITH(\s+RECURSIVE)?\s`)

func shadowCatalog(t *sqlText) {
	used := map[string]bool{}
	t.replace(shadowRE, func(s []string) string {
		name := strings.ToLower(s[2])
		used[name] = true
		return s[1] + "kelvo_" + name
	})
	if len(used) == 0 {
		return
	}
	names := make([]string, 0, len(used))
	for n := range used {
		names = append(names, n)
	}
	sort.Strings(names)
	var ctes []string
	for _, n := range names {
		ctes = append(ctes, "kelvo_"+n+" AS ("+shadows[n]+")")
	}
	list := strings.Join(ctes, ", ")
	if l := leadingWithRE.FindStringIndex(t.m); l != nil {
		t.splice(l[1], l[1], list+", ")
	} else {
		t.splice(0, 0, "WITH "+list+" ")
	}
}
