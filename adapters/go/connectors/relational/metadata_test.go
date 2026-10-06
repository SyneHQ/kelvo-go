package relational

import (
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/adapters/go/connectors/sqlsession"
	"github.com/SYNEHQ/kelvo-go/operations"
)

func TestMetadataBindsFiltersAndPagination(t *testing.T) {
	for _, engine := range []string{"postgresql", "mysql", "mariadb"} {
		s := &Session{Session: &sqlsession.Session{Engine: engine}, database: "app", schema: "app"}
		for _, object := range []string{"catalogs", "databases", "schemas", "tables", "columns", "primary_keys", "foreign_keys", "relationships", "indexes", "functions", "procedures"} {
			spec := operations.MetadataSpec{Object: object, Limit: 37, Cursor: "15"}
			if object != "catalogs" && object != "databases" && object != "schemas" {
				spec.Target.Name = "table' OR 1=1 --"
			}
			sql, args, err := s.metadataQuery(spec)
			if err != nil {
				t.Fatalf("%s %s: %v", engine, object, err)
			}
			if strings.Contains(sql, "table' OR") || len(args) < 2 || args[len(args)-2] != 37 || args[len(args)-1] != int64(15) {
				t.Fatal("metadata filter/pagination not bound")
			}
			if engine == "postgresql" && strings.Contains(sql, "?") {
				t.Fatal("unconverted PostgreSQL placeholder")
			}
		}
	}
}

func TestMetadataRejectsScopeEscapeAndInvalidCursors(t *testing.T) {
	s := &Session{Session: &sqlsession.Session{Engine: "postgresql"}, database: "app", schema: "selected"}
	for _, spec := range []operations.MetadataSpec{
		{Object: "tables", Limit: 10, Target: operations.ObjectRef{Catalog: "other"}},
		{Object: "tables", Limit: 10, Target: operations.ObjectRef{Schema: "other"}},
		{Object: "tables", Limit: 10, Cursor: "-1"}, {Object: "tables", Limit: 10, Cursor: "01"},
		{Object: "tables", Limit: 10, Cursor: "1000001"}, {Object: "tables", Limit: 10, Cursor: "0;DROP"},
		{Object: "tables", Limit: 10001}, {Object: "extract", Limit: 10},
	} {
		if _, _, err := s.metadataQuery(spec); err == nil {
			t.Fatal("invalid metadata selection accepted")
		}
	}
}
