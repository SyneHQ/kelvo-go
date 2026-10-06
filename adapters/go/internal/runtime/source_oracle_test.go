package runtime

import (
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/adapter"
)

func TestOracleResolutionRequiresVerifiedTCPSAndExactService(t *testing.T) {
	spec := adapter.ConnectionSpec{Engine: "oracle", DSN: "oracle://reader:test-only@db.example:2484/service?SSL=enable&SSL+VERIFY=true", TenantID: "tenant", ConnectionID: "saved", Revision: "revision", Database: "service", Schema: "APP"}
	c, err := sourceConnection(spec)
	if err != nil || c.Host != "db.example" || c.Port != 2484 || c.Username != "reader" || c.Password != "test-only" || c.Namespace != "service" || c.Schema != "APP" || c.TLS.InsecureSkipVerify || c.TLS.ServerName != "db.example" {
		t.Fatal("Oracle resolution changed verified source scope", err)
	}
	for _, mutate := range []func(*adapter.ConnectionSpec){
		func(s *adapter.ConnectionSpec) { s.DSN += "&DBA+PRIVILEGE=SYSDBA" },
		func(s *adapter.ConnectionSpec) { s.DSN += "&SSL+VERIFY=true" },
		func(s *adapter.ConnectionSpec) { s.DSN = strings.Replace(s.DSN, "VERIFY=true", "VERIFY=false", 1) },
		func(s *adapter.ConnectionSpec) { s.Database = "other" },
		func(s *adapter.ConnectionSpec) { s.Password = "duplicate" },
		func(s *adapter.ConnectionSpec) { s.URL = "https://untrusted.example" },
	} {
		bad := spec
		mutate(&bad)
		if _, err := sourceConnection(bad); err == nil {
			t.Fatal("unsafe Oracle resolution accepted")
		}
	}
}
