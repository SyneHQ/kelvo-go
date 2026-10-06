package ignite

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"os"
	"strconv"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow/array"
)

func TestLiveIgniteStatementLifecycle(t *testing.T) {
	text := os.Getenv("KELVO_TEST_IGNITE_WRITE_FD")
	if text == "" {
		t.Skip("requires isolated Ignite TLS fixture and inherited configuration pipe")
	}
	fd, err := strconv.Atoi(text)
	if err != nil || fd < 3 {
		t.Fatal("invalid fixture descriptor")
	}
	f := os.NewFile(uintptr(fd), "ignite-fixture")
	defer f.Close()
	var config struct{ URL, Username, Password, CA, Cache string }
	if json.NewDecoder(f).Decode(&config) != nil {
		t.Fatal("invalid fixture configuration")
	}
	pem, err := os.ReadFile(config.CA)
	roots := x509.NewCertPool()
	if err != nil || !roots.AppendCertsFromPEM(pem) {
		t.Fatal("fixture CA unavailable")
	}
	e, err := NewResolved(catalog.Config{Sources: []catalog.Source{{ID: "grid", Type: "ignite", Options: map[string]string{"cache_name": config.Cache}}}}, query.DefaultLimits(), Credentials{URL: config.URL, Username: config.Username, Password: config.Password, TLS: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	ctx := context.Background()
	apply := func(sql string, want int64) {
		t.Helper()
		count, err := e.ApplyStatement(ctx, sql)
		if err != nil || count == nil || *count != want {
			t.Fatal("statement acknowledgement", count, err)
		}
	}
	apply("CREATE TABLE KELVO_WRITE_ACCEPTANCE (id BIGINT PRIMARY KEY, label VARCHAR)", 0)
	apply("INSERT INTO KELVO_WRITE_ACCEPTANCE(id,label) VALUES(9007199254740993,'original')", 1)
	apply("UPDATE KELVO_WRITE_ACCEPTANCE SET label='updated' WHERE id=9007199254740993", 1)
	out := sink(t)
	stats, err := e.Execute(ctx, liveRequest("SELECT id,label FROM KELVO_WRITE_ACCEPTANCE"), out)
	if err != nil || stats.Rows != 1 || len(out.records) != 1 || out.records[0].Column(0).(*array.Int64).Value(0) != 9007199254740993 || out.records[0].Column(1).(*array.String).Value(0) != "updated" {
		t.Fatal("mutation was not visible to read path", stats, err)
	}
	if _, err := e.ApplyStatement(ctx, "DELETE broken syntax"); err == nil {
		t.Fatal("invalid statement accepted")
	}
	apply("DELETE FROM KELVO_WRITE_ACCEPTANCE WHERE id=9007199254740993", 1)
	apply("DROP TABLE KELVO_WRITE_ACCEPTANCE", 0)
}
