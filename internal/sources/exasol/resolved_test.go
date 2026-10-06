package exasol

import (
	"context"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/gorilla/websocket"
	"strings"
	"testing"
)

func TestResolvedExasolPreservesPasswordAndCleanup(t *testing.T) {
	original, f, server := setup(t, func(c *websocket.Conn, in map[string]any) bool {
		if isQuery(in) {
			reply(c, resultJSON(decimalColumn, `[[9007199254740993]]`, 1, 1, "0"))
			return true
		}
		return false
	})
	t.Setenv("KELVO_SOURCE_EXASOL_DSN", "unselected-invalid-dsn")
	cfg := catalog.Config{Sources: []catalog.Source{{ID: "warehouse", Type: "exasol"}}}
	e, err := NewResolved(cfg, query.DefaultLimits(), Credentials{URL: strings.Replace(server.URL, "https://", "wss://", 1), Username: "reader", Password: "fixture;password", TLS: original.dialer.TLSClientConfig})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	out := sink(t)
	stats, err := e.Execute(context.Background(), request(), out)
	if err != nil || stats.Rows != 1 || out.records[0].Column(0).(*array.Decimal128).Value(0).ToString(0) != "9007199254740993" {
		t.Fatal("resolved Exasol lost exact data", err)
	}
	if f.snapshot() != "login,auth,getAttributes,execute,closeResultSet,execute,disconnect" {
		t.Fatal("source lifecycle incomplete", f.snapshot())
	}
}
