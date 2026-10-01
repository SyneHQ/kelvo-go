//go:build duckdb_arrow

package duckdb

import (
	"reflect"
	"strings"
	"testing"
)

func TestMySQLConnectionGrammar(t *testing.T) {
	for _, tc := range []struct {
		name string
		dsn  string
		want map[string]string
	}{
		{"key values", `HOST=db.local user=analyst passwd="quote\" space\\slash'" db=warehouse port=3306 connect_timeout=7`, map[string]string{"host": "db.local", "user": "analyst", "password": "quote\" space\\slash'", "database": "warehouse", "port": "3306", "connect_timeout": "7"}},
		{"quoted equals", `host="db.local" password="one=two three" socket=""`, map[string]string{"host": "db.local", "password": "one=two three", "socket": ""}},
		{"URI in password", `host=localhost password="http://example.invalid/a=b"`, map[string]string{"host": "localhost", "password": "http://example.invalid/a=b"}},
		{"literal single quotes", `host=db password='literal'`, map[string]string{"host": "db", "password": "'literal'"}},
		{"URI", `mysql://analyst:p%40ss%20word@db.local:3306/warehouse?ssl-mode=verify_identity&connect-timeout=5&ssl-ca=%2Fca%2Broot.pem`, map[string]string{"host": "db.local", "port": "3306", "user": "analyst", "password": "p@ss word", "database": "warehouse", "ssl_mode": "verify_identity", "connect_timeout": "5", "ssl_ca": "/ca+root.pem"}},
		{"literal URI plus", `mysql://db.local/warehouse?ssl-ca=/ca+root.pem`, map[string]string{"host": "db.local", "database": "warehouse", "ssl_ca": "/ca+root.pem"}},
		{"scheme-less URI", `analyst:pass@db.local:3306/warehouse?compression=required`, map[string]string{"host": "db.local", "port": "3306", "user": "analyst", "password": "pass", "database": "warehouse", "compression": "required"}},
		{"IPv6 URI", `mysql://[::1]:3306/warehouse`, map[string]string{"host": "::1", "port": "3306", "database": "warehouse"}},
		{"socket URI", `mysql://?socket=%2Frun%2Fmysql.sock`, map[string]string{"socket": "/run/mysql.sock"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseMySQLDSN(tc.dsn)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("connection option values changed: got %#v want %#v", got, tc.want)
			}
		})
	}
}

func TestMySQLRejectsAmbiguousOrUnsupportedOptions(t *testing.T) {
	for _, dsn := range []string{
		``, `host = db`, `host=`, `host="unterminated`, `host="bad\nescape"`,
		`host=db host=other`, `password=one passwd=two`, `compress=1 compression=required`,
		`host=db unknown=CANARY_PRIVATE`, `port=3306suffix`, `port=65536`,
		`password="one"host=db`, `host=db password=unquoted=equals`,
		`mysql://db/warehouse?unrecognized=CANARY_PRIVATE`,
		`mysql://db/warehouse?ssl-mode=required&ssl-mode=disabled`,
		`mysql://db/warehouse#CANARY_PRIVATE`, `mysql://db/a/b`, `postgres://db/warehouse`,
	} {
		_, err := parseMySQLDSN(dsn)
		if err == nil {
			t.Errorf("ambiguous or unsupported DSN accepted: %q", dsn)
		} else if strings.Contains(err.Error(), "CANARY_PRIVATE") {
			t.Fatal("private connection option appeared in error")
		}
	}
}

func TestSourceSecretsKeepCredentialsOutOfMetadataPaths(t *testing.T) {
	pg := `host='db.local' user=analyst password='quote\' CANARY_PRIVATE' sslmode=verify-full application_name=kelvo connect_timeout=5`
	statement, publicPath, err := sourceSecret("postgres", "kelvo_source_pg", pg)
	if err != nil || publicPath != "" {
		t.Fatalf("PostgreSQL private DSN path: path=%q err=%v", publicPath, err)
	}
	if statement != `CREATE TEMPORARY SECRET "kelvo_source_pg" (TYPE postgres, URI '`+quoteLiteral(pg)+`')` {
		t.Fatal("PostgreSQL connection string was changed")
	}
	statement, publicPath, err = sourceSecret("mysql", "kelvo_source_my", `host=db user=analyst password="CANARY_PRIVATE's pass" db=warehouse ssl_mode=required compress=true`)
	if err != nil {
		t.Fatal(err)
	}
	if publicPath != "compress=true" || strings.Contains(publicPath, "CANARY_PRIVATE") || !strings.Contains(statement, "password 'CANARY_PRIVATE''s pass'") {
		t.Fatal("MySQL secret or public connection options were not separated")
	}
	for _, dsn := range []string{`host=db compression=CANARY_PRIVATE`, `host=db connect_timeout=10suffix`, `host=db compress=CANARY_PRIVATE`, `mysql://user:pass%00word@db/schema`} {
		_, _, err := sourceSecret("mysql", "safe", dsn)
		if err == nil || strings.Contains(err.Error(), "CANARY_PRIVATE") {
			t.Fatalf("invalid metadata option was accepted or exposed: %v", err)
		}
	}
}
