package relational

import (
	"crypto/tls"
	"os"
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
)

func testConnection() adapter.Connection {
	return adapter.Connection{TenantID: "tenant-1", ConnectionID: "saved-1", Revision: "revision-1", Engine: "postgresql", Host: "db.example", Port: 5432, Namespace: "app", Schema: "odd\"schema", Username: "reader", Password: "test-only"}
}
func cleanPostgresEnvironment(t *testing.T) {
	t.Helper()
	for _, entry := range os.Environ() {
		key, value, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(key, "PG") {
			if err := os.Unsetenv(key); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.Setenv(key, value) })
		}
	}
}

func TestPostgresConfigUsesExplicitIdentityAndVerifiedTLS(t *testing.T) {
	cleanPostgresEnvironment(t)
	c := testConnection()
	c.TLS = &tls.Config{MinVersion: tls.VersionTLS12, ServerName: "source.example"}
	config, err := postgresConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	if config.Host != c.Host || int(config.Port) != c.Port || config.Database != c.Namespace || config.User != c.Username || config.Password != c.Password || config.TLSConfig == c.TLS || config.TLSConfig.InsecureSkipVerify || config.TLSConfig.ServerName != "source.example" || len(config.Fallbacks) != 0 {
		t.Fatal("explicit source identity/TLS lost")
	}
	if config.RuntimeParams["search_path"] != `"odd""schema"` {
		t.Fatal("schema not quoted")
	}
	t.Setenv("PGSERVICEFILE", "/definitely-not-a-source-file")
	if _, err := postgresConfig(c); err == nil {
		t.Fatal("ambient PostgreSQL credentials accepted")
	}
}

func TestMySQLConfigDisablesUnsafeDriverFeatures(t *testing.T) {
	c := testConnection()
	c.Engine = "mysql"
	c.Port = 3306
	c.Schema = c.Namespace
	config, err := mysqlConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	if config.Net != "tcp" || config.DBName != c.Namespace || config.TLS == nil || config.TLS.InsecureSkipVerify || config.MultiStatements || config.InterpolateParams || config.AllowAllFiles || config.AllowFallbackToPlaintext || config.AllowCleartextPasswords || config.AllowOldPasswords || !config.ParseTime || config.Loc.String() != "UTC" || config.Params["time_zone"] != "'+00:00'" {
		t.Fatal("unsafe MySQL configuration")
	}
	c.Schema = "other"
	if _, err := mysqlConfig(c); err == nil {
		t.Fatal("cross-database schema accepted")
	}
}

func TestRelationalRejectsInvalidIdentityOrTLS(t *testing.T) {
	cleanPostgresEnvironment(t)
	for _, mutate := range []func(*adapter.Connection){
		func(c *adapter.Connection) { c.TenantID = "" }, func(c *adapter.Connection) { c.Revision = "" }, func(c *adapter.Connection) { c.Host = "/var/run/postgresql" },
		func(c *adapter.Connection) { c.Host = "db.example/extra" }, func(c *adapter.Connection) { c.Port = 0 }, func(c *adapter.Connection) { c.Namespace = "other/app" },
		func(c *adapter.Connection) { c.Password = "" }, func(c *adapter.Connection) { c.TLS = &tls.Config{InsecureSkipVerify: true} },
		func(c *adapter.Connection) { c.TLS = &tls.Config{MaxVersion: tls.VersionTLS11} },
	} {
		c := testConnection()
		mutate(&c)
		if _, err := postgresConfig(c); err == nil {
			t.Fatal("invalid source accepted")
		}
	}
}

func TestRelationalCapabilitiesExplicitlyMatchImplementedOperations(t *testing.T) {
	for _, driver := range []*Driver{NewPostgreSQL(), NewMySQL(), NewMariaDB(), NewSQLServer()} {
		c := driver.Capabilities()
		if err := c.Validate(); err != nil {
			t.Fatal(err)
		}
		seen := map[operations.Kind]bool{}
		for _, op := range c.Operations {
			seen[op.Kind] = true
		}
		expected := 6
		if driver.engine == "postgresql" {
			expected = 13
		} else if driver.engine == "mysql" || driver.engine == "mariadb" {
			expected = 10
		}
		if len(seen) != expected || !seen[operations.StatementExecute] || !seen[operations.QueryRead] || !seen[operations.ConnectionTest] || !seen[operations.MetadataInspect] {
			t.Fatal("capability mismatch")
		}
		for _, kind := range []operations.Kind{operations.IngestionInstall, operations.IngestionState, operations.IngestionCommit} {
			if seen[kind] != (driver.engine == "postgresql") {
				t.Fatal("ingestion advertised for an unimplemented provider")
			}
			request := operations.Request{Version: operations.Version, Kind: kind, Connection: operations.ConnectionRef{ID: "saved", Database: "app"}, Spec: operations.Spec{Ingestion: &operations.IngestionSpec{Scope: operations.IngestionScope{SourceID: "source", Stream: "orders", Binding: strings.Repeat("a", 64)}}}}
			if kind.Mutating() {
				request.IdempotencyKey = "mutation"
			}
			if kind == operations.IngestionCommit {
				request.Spec.Ingestion.BatchID = "batch"
				request.Spec.Ingestion.Input = &operations.InputRef{ID: "input", SHA256: strings.Repeat("b", 64), Bytes: 1, Format: "ingestion_batch_v1"}
			}
			if err := request.Validate(); err != nil {
				t.Fatal(err)
			}
			if supported := c.Supports(request) == nil; supported != (driver.engine == "postgresql") {
				t.Fatal("ingestion dispatch capability does not match implemented provider")
			}
		}
	}
}
