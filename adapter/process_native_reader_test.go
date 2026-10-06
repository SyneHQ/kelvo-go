package adapter

import (
	"encoding/base64"
	"testing"

	"github.com/SYNEHQ/kelvo-go/operations"
)

func nativeReaderFixture(engine string) ConnectionSpec {
	s := ConnectionSpec{Engine: engine, URL: "https://source.example", Database: "analytics", Username: "reader", Password: "secret", Options: map[string]string{}}
	switch engine {
	case "elasticsearch":
		s.Options["authentication"] = "Basic"
	case "trino", "presto":
		s.Schema = "public"
		s.Options = map[string]string{"catalog": s.Database, "schema": s.Schema}
	case "arrow_flight":
		s.URL = "grpcs://source.example:32010"
		s.Database = ""
		s.Username = ""
		s.Password = ""
		s.Token = "token"
		s.Options["protocol"] = "flightsql"
	case "exasol":
		s.URL = "wss://source.example:8563"
		s.Options["schema"] = s.Database
	case "spanner":
		s.Username = ""
		s.Password = ""
		s.Token = "token"
		s.Options = map[string]string{"project": "project", "instance": "instance", "database": s.Database}
	case "ignite":
		s.Options["cache_name"] = s.Database
	case "athena":
		s.Options = map[string]string{"region": "us-east-1", "database": s.Database, "workgroup": "reports", "output_location": "s3://results/reports/"}
	case "dynamodb":
		s.Options["region"] = "us-east-1"
	case "clickhouse_lambda":
		s.URL = "https://lambda.us-east-1.amazonaws.com"
		s.Options = map[string]string{"region": "us-east-1", "function_name": "analytics", "bucket_path": "/reports/"}
	case "cosmosdb":
		s.Username = ""
		s.Password = ""
		s.Token = base64.StdEncoding.EncodeToString(make([]byte, 64))
		s.Schema = "records"
		s.Options = map[string]string{"database": s.Database, "container": s.Schema, "auth": "master_key"}
	}
	return s
}
func TestNativeReaderPrivateContracts(t *testing.T) {
	engines := []string{"elasticsearch", "trino", "presto", "arrow_flight", "exasol", "spanner", "ignite", "athena", "dynamodb", "cosmosdb", "clickhouse_lambda"}
	for _, engine := range engines {
		t.Run(engine, func(t *testing.T) {
			s := nativeReaderFixture(engine)
			if err := ValidateNativeReaderProcessSource(s); err != nil {
				t.Fatal(err)
			}
			changes := map[string]func(*ConnectionSpec){"ambient dsn": func(s *ConnectionSpec) { s.DSN = "ambient" }, "plaintext": func(s *ConnectionSpec) { s.URL = "http://source.example" }, "redirect path": func(s *ConnectionSpec) { s.URL += "/other" }, "userinfo": func(s *ConnectionSpec) { s.URL = "https://secret@source.example" }, "option": func(s *ConnectionSpec) { s.Options["credential_file"] = "/private/key" }, "control": func(s *ConnectionSpec) { s.Token = "x\r\ny" }}
			for name, change := range changes {
				t.Run(name, func(t *testing.T) {
					copy := nativeReaderFixture(engine)
					change(&copy)
					if ValidateNativeReaderProcessSource(copy) == nil {
						t.Fatal("unsafe source accepted")
					}
				})
			}
			c := NativeReaderCapabilities(engine)
			if c.Validate() != nil {
				t.Fatal("invalid capabilities")
			}
			request := operations.Request{Version: 1, Kind: operations.StatementExecute, Connection: operations.ConnectionRef{ID: "saved", Database: s.Database}, IdempotencyKey: "write", Spec: operations.Spec{Statement: &operations.StatementSpec{SQL: "DELETE FROM records", Transaction: operations.TransactionAutocommit}}}
			if supported := c.Supports(request) == nil; supported != (engine != "elasticsearch") {
				t.Fatal("wrong statement capability", engine, supported)
			}
			request.Spec.Statement.Transaction = operations.TransactionRequired
			if c.Supports(request) == nil {
				t.Fatal("autocommit wrapper advertised atomic transactions")
			}
		})
	}
}
func TestNativeReaderNamespaceBinding(t *testing.T) {
	for _, engine := range []string{"trino", "presto", "exasol", "spanner", "ignite", "athena", "cosmosdb"} {
		s := nativeReaderFixture(engine)
		s.Database = "another"
		if ValidateNativeReaderProcessSource(s) == nil {
			t.Fatal(engine, "changed namespace accepted")
		}
	}
}
