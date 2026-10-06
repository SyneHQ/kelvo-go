package relational

import (
	"context"
	"testing"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
)

func TestPGFamilyPreservesVendorCapabilities(t *testing.T) {
	for _, engine := range []string{"cockroachdb", "redshift", "alloydb"} {
		d := PGFamily{Engine: engine}
		c := d.Capabilities()
		if c.Engine != engine {
			t.Fatal("vendor identity lost")
		}
		seen := map[operations.Kind]bool{}
		for _, op := range c.Operations {
			seen[op.Kind] = true
			if op.Kind == operations.WatchInstall || op.Kind == operations.IngestionInstall {
				t.Fatal("unproven PostgreSQL capability inherited")
			}
			if op.Kind == operations.StatementExecute && (op.Roles || engine != "alloydb" && len(op.Transactions) != 1) {
				t.Fatal("unproven role or transaction promised")
			}
		}
		if !seen[operations.QueryRead] || !seen[operations.MetadataInspect] || !seen[operations.StatementExecute] || seen[operations.MigrationApply] != (engine != "alloydb") {
			t.Fatal(c)
		}
		if _, err := d.Open(context.Background(), adapter.Connection{Engine: "postgresql"}); err == nil {
			t.Fatal("wrong engine accepted")
		}
	}
}
