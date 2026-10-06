package relational

import (
	"context"
	"crypto/tls"
	"testing"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/microsoft/go-mssqldb/msdsn"
)

func TestSQLServerConfigurationIsExplicitAndCannotRetryWrites(t *testing.T) {
	c := testConnection()
	c.Engine = "sqlserver"
	c.Port = 1433
	c.Schema = "dbo"
	config, err := sqlserverConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	if config.Host != c.Host || config.Port != 1433 || config.Database != c.Namespace || config.User != c.Username || config.Password != c.Password || config.Encryption != msdsn.EncryptionRequired || config.TLSConfig == nil || config.TLSConfig.InsecureSkipVerify || config.TLSConfig.MinVersion < tls.VersionTLS12 || !config.DisableRetry || config.Instance != "" || config.FailOverPartner != "" || config.LogFlags != 0 || len(config.Protocols) != 1 || config.Protocols[0] != "tcp" {
		t.Fatal("unsafe SQL Server connector configuration")
	}
	if err := NewSQLServer().Capabilities().Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestSQLServerReadRejectsExternalQueriesSequencesAndSchemaMismatch(t *testing.T) {
	for _, sql := range []string{"SELECT NEXT VALUE FOR seq", "SELECT * FROM OPENQUERY(remote,'UPDATE a SET n=1')", "SELECT * FROM OPENROWSET(BULK 'x',SINGLE_BLOB) a", "SELECT dbo.xp_cmdshell('x')"} {
		if validateSQLServerRead(sql) == nil {
			t.Fatal("side-effect primitive accepted")
		}
	}
	s, state := newReadSession(t, "sqlserver")
	s.schema = "other"
	s.defaultSchema = "dbo"
	if _, err := s.Query(context.Background(), readRequest(), &readSink{}); err == nil || state.opens.Load() != 0 {
		t.Fatal("selected schema silently ignored")
	}
	if _, err := s.Execute(context.Background(), adapter.Change{Statements: []string{"UPDATE items SET n=1"}}); err == nil || state.opens.Load() != 0 {
		t.Fatal("selected change schema silently ignored")
	}
	s.schema = "dbo"
	if _, err := s.Execute(context.Background(), adapter.Change{Statements: []string{"UPDATE items SET n=1"}, Role: "other_user"}); err == nil || state.opens.Load() != 0 {
		t.Fatal("role could silently change selected schema")
	}

}

func TestSQLServerMetadataUsesBoundFiltersAndPagination(t *testing.T) {
	s, _ := newReadSession(t, "sqlserver")
	for _, kind := range []string{"databases", "schemas", "tables", "columns", "primary_keys", "foreign_keys", "indexes", "functions", "procedures"} {
		spec := operations.MetadataSpec{Object: kind, Limit: 13, Cursor: "27"}
		if kind != "databases" && kind != "schemas" {
			spec.Target.Name = "items' OR 1=1 --"
		}
		query, args, err := s.metadataQuery(spec)
		if err != nil || len(args) < 2 || args[len(args)-2] != int64(27) || args[len(args)-1] != 13 {
			t.Fatal(kind, query, args, err)
		}
		for _, r := range query {
			if r == '?' {
				t.Fatal("unconverted parameter")
			}
		}
	}
}
