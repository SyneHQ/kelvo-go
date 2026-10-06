package runtime

import (
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/adapter"
)

func TestSourceConnectionOnlyAcceptsExplicitVerifiedCredentials(t *testing.T) {
	for _, test := range []struct {
		engine, dsn string
		port        int
	}{
		{"postgresql", "postgresql://reader:test-only@db.example/app?sslmode=verify-full", 5432},
		{"cockroachdb", "postgresql://reader:test-only@db.example:26257/app?sslmode=verify-full", 26257},
		{"redshift", "postgresql://reader:test-only@db.example:5439/app?sslmode=verify-full", 5439},
		{"alloydb", "postgresql://reader:test-only@db.example/app?sslmode=verify-full", 5432},
		{"mysql", "reader:test-only@tcp(db.example:3306)/app?tls=true&parseTime=true&loc=UTC&time_zone=%27%2B00%3A00%27", 3306},
		{"sqlserver", "sqlserver://reader:test-only@db.example:1433?database=app&encrypt=true&TrustServerCertificate=false&connection+timeout=5", 1433},
	} {
		spec := adapter.ConnectionSpec{Engine: test.engine, DSN: test.dsn, TenantID: "tenant-1", ConnectionID: "saved-1", Revision: "revision-1", Database: "app"}
		connection, err := sourceConnection(spec)
		if err != nil {
			t.Fatal(test.engine, err)
		}
		if connection.Engine != test.engine || connection.Host != "db.example" || connection.Port != test.port || connection.Namespace != "app" || connection.Username != "reader" || connection.Password != "test-only" || connection.TLS == nil || connection.TLS.InsecureSkipVerify {
			t.Fatal("source identity not preserved")
		}
		for _, suffix := range []string{"&unknown=x", "&tls_ca_file=/tmp/key", "&sslmode=verify-full"} {
			bad := spec
			bad.DSN += suffix
			if _, err := sourceConnection(bad); err == nil {
				t.Fatal("unknown or duplicate source option accepted")
			}
		}
		bad := spec
		bad.Database = "other"
		if _, err := sourceConnection(bad); err == nil {
			t.Fatal("database override accepted")
		}
		bad = spec
		bad.Password = "another"
		if _, err := sourceConnection(bad); err == nil {
			t.Fatal("ambiguous credential source accepted")
		}
	}
}

func TestSourceConnectionRejectsPlaintextAndClientFileInjection(t *testing.T) {
	for _, dsn := range []string{
		"postgresql://reader:test-only@db.example/app?sslmode=require",
		"postgresql://reader:test-only@db.example/app?sslmode=disable",
		"postgresql://reader:test-only@db.example/app?sslmode=verify-full&passfile=/tmp/key",
		"postgresql://reader:test-only@db.example/app?sslmode=verify-full&host=/var/run/postgresql",
		"postgresql://reader:test-only@db.example/app?sslmode=verify-full&options=-csearch_path=other",
	} {
		if _, err := sourceConnection(adapter.ConnectionSpec{Engine: "postgresql", DSN: dsn, Database: "app"}); err == nil {
			t.Fatal("unsafe PostgreSQL DSN accepted")
		}
	}
	valid := "reader:test-only@tcp(db.example:3306)/app?tls=true&parseTime=true&loc=UTC&time_zone=%27%2B00%3A00%27"
	for _, dsn := range []string{strings.Replace(valid, "tls=true", "tls=skip-verify", 1), strings.Replace(valid, "tls=true", "tls=preferred", 1), valid + "&multiStatements=true", valid + "&allowAllFiles=true", strings.Replace(valid, "tcp(db.example:3306)", "unix(/tmp/db.sock)", 1)} {
		if _, err := sourceConnection(adapter.ConnectionSpec{Engine: "mysql", DSN: dsn, Database: "app"}); err == nil {
			t.Fatal("unsafe MySQL DSN accepted")
		}
	}
}
