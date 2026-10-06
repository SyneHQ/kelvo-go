package cassandra

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"os"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// The runner creates an isolated TLS source; fixture secrets arrive by an
// inherited pipe and are never accepted in command arguments or test output.
func TestLiveCQLScopedOperations(t *testing.T) {
	fdText := os.Getenv("KELVO_TEST_CQL_FD")
	if fdText == "" {
		t.Skip("requires isolated Cassandra TLS fixture and inherited configuration pipe")
	}
	fd, err := strconv.Atoi(fdText)
	if err != nil || fd < 3 {
		t.Fatal("invalid fixture descriptor")
	}
	f := os.NewFile(uintptr(fd), "cql-fixture")
	defer f.Close()
	var config struct{ Host, CA, Namespace, Username, Password string }
	if json.NewDecoder(f).Decode(&config) != nil || !regexp.MustCompile(`^[a-z][a-z0-9_]{1,47}$`).MatchString(config.Namespace) || !regexp.MustCompile(`^[a-z][a-z0-9_]{1,47}$`).MatchString(config.Username) || !regexp.MustCompile(`^[a-f0-9]{48}$`).MatchString(config.Password) {
		t.Fatal("invalid fixture configuration")
	}
	pem, err := os.ReadFile(config.CA)
	roots := x509.NewCertPool()
	if err != nil || !roots.AppendCertsFromPEM(pem) {
		t.Fatal("fixture CA unavailable")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	connection := adapter.Connection{TenantID: "tenant", ConnectionID: "source", Revision: "fixture", Engine: "cassandra", Host: config.Host, Port: 9042, Username: "cassandra", Password: "cassandra", TLS: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, ServerName: "localhost"}}
	driver := Driver{Engine: "cassandra"}
	var admin adapter.Session
	for deadline := time.Now().Add(60 * time.Second); time.Now().Before(deadline); {
		admin, err = driver.Open(ctx, connection)
		if err == nil {
			break
		}
		time.Sleep(time.Second)
	}
	if err != nil {
		t.Fatal("fixture startup failed")
	}
	live := admin.(*Session).backend.(liveBackend).session
	for _, statement := range []string{
		"CREATE KEYSPACE " + config.Namespace + " WITH replication = {'class':'SimpleStrategy','replication_factor':1}",
		"CREATE ROLE " + config.Username + " WITH PASSWORD='" + config.Password + "' AND LOGIN=true",
		"GRANT ALL PERMISSIONS ON KEYSPACE " + config.Namespace + " TO " + config.Username,
	} {
		if live.Query(statement).WithContext(ctx).Exec() != nil {
			admin.Close()
			t.Fatal("fixture authorization setup failed")
		}
	}
	admin.Close()
	connection.Namespace, connection.Username, connection.Password = config.Namespace, config.Username, config.Password
	opened, err := driver.Open(ctx, connection)
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Close()
	s := opened.(*Session)
	if err := s.Test(ctx); err != nil {
		t.Fatal(err)
	}
	execute := func(statement string, p ...operations.Parameter) adapter.ChangeResult {
		t.Helper()
		result, err := s.Execute(ctx, adapter.Change{Statements: []string{statement}, Parameters: [][]operations.Parameter{p}})
		if err != nil || result.Outcome != "succeeded" || result.Completed != 1 || result.Attempted != 1 || result.AffectedRows != nil {
			t.Fatal(result, err)
		}
		return result
	}
	execute("CREATE TABLE events (id bigint PRIMARY KEY, payload blob, label text)")
	integer := operations.Parameter{Type: "int64", Value: json.RawMessage(`"9007199254740993"`)}
	execute("INSERT INTO events(id,payload,label) VALUES(?,?,?)", integer, operations.Parameter{Type: "binary", Value: json.RawMessage(`"AP8="`)}, operations.Parameter{Type: "string", Value: json.RawMessage(`"it';still bound"`)})
	execute("UPDATE events SET label=? WHERE id=?", operations.Parameter{Type: "string", Value: json.RawMessage(`"updated"`)}, integer)
	stats, err := s.Query(ctx, adapter.Query{Statement: "SELECT id,payload,label FROM events WHERE id=?", Parameters: []operations.Parameter{integer}, MaxRows: 10, MaxBytes: 4096, BatchRows: 1}, testSink{write: func(batch arrow.RecordBatch) error {
		if batch.Column(0).(*array.Int64).Value(0) != 9007199254740993 || string(batch.Column(1).(*array.Binary).Value(0)) != string([]byte{0, 255}) || batch.Column(2).(*array.String).Value(0) != "updated" {
			t.Fatal("source values changed")
		}
		return nil
	}})
	if err != nil || stats.Rows != 1 {
		t.Fatal(stats, err)
	}
	for _, object := range []string{"catalogs", "schemas", "tables", "columns"} {
		spec := operations.MetadataSpec{Object: object, Limit: 10}
		if object == "columns" {
			spec.Target.Name = "events"
		}
		stats, err := s.Inspect(ctx, spec, adapter.Limits{MaxRows: 10, MaxBytes: 4096, BatchRows: 1}, discardSink{})
		want := int64(1)
		if object == "columns" {
			want = 3
		}
		if err != nil || stats.Rows != want {
			t.Fatal(object, stats, err)
		}
	}
	result, err := s.Execute(ctx, adapter.Change{Statements: []string{"CREATE TABLE events (id bigint PRIMARY KEY)"}, Parameters: [][]operations.Parameter{nil}})
	if err == nil || result.Outcome != "failed" || result.Attempted != 1 {
		t.Fatal("source rejection outcome", result, err)
	}
	result, err = s.Execute(ctx, adapter.Change{Statements: []string{"UPDATE events SET label='first' WHERE id=9007199254740993", "DELETE broken syntax"}, Parameters: [][]operations.Parameter{nil, nil}})
	if err == nil || result.Outcome != "failed" || result.Completed != 1 || result.Attempted != 2 {
		t.Fatal("partial completion outcome", result, err)
	}
	execute("DELETE FROM events WHERE id=?", integer)
	execute("DROP TABLE events")
	connection.TLS = connection.TLS.Clone()
	connection.TLS.ServerName = "wrong-host.invalid"
	if session, err := driver.Open(ctx, connection); err == nil {
		session.Close()
		t.Fatal("hostname mismatch accepted")
	}
}
