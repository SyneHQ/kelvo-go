package runtime

import (
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/adapter"
)

func TestMongoResolutionSeparatesCredentialsAndPreservesTopology(t *testing.T) {
	spec := adapter.ConnectionSpec{Engine: "mongodb", DSN: "mongodb://reader:test-only@db.example:27017/?tls=true&authSource=admin&replicaSet=rs0&directConnection=true", TenantID: "tenant", ConnectionID: "saved", Revision: "revision", Database: "app"}
	c, err := sourceConnection(spec)
	if err != nil {
		t.Fatal(err)
	}
	if c.Username != "reader" || c.Password != "test-only" || strings.Contains(c.Endpoint, "reader") || strings.Contains(c.Endpoint, "test-only") || !strings.Contains(c.Endpoint, "replicaSet=rs0") || !strings.Contains(c.Endpoint, "directConnection=true") || c.Namespace != "app" || c.TLS.ServerName != "" {
		t.Fatal("resolution changed source or credentials")
	}
	for _, suffix := range []string{"&tlsInsecure=true", "&retryWrites=true", "&authMechanism=MONGODB-AWS", "&authSource=admin"} {
		bad := spec
		bad.DSN += suffix
		if _, err := sourceConnection(bad); err == nil {
			t.Fatal("unsafe Mongo source accepted")
		}
	}
	bad := spec
	bad.Options = map[string]string{"tls_server_name": "private.example"}
	c, err = sourceConnection(bad)
	if err != nil || c.TLS.ServerName != "private.example" {
		t.Fatal(err)
	}
}

func TestMongoResolutionBindsDatabaseOptionAndAuthenticationDatabase(t *testing.T) {
	spec := adapter.ConnectionSpec{Engine: "mongodb", DSN: "mongodb://reader:test-only@db.example:27017/?tls=true&authSource=users", TenantID: "tenant", ConnectionID: "saved", Revision: "revision", Database: "app", Options: map[string]string{"database": "app"}}
	c, err := sourceConnection(spec)
	if err != nil || c.Namespace != "app" || !strings.Contains(c.Endpoint, "authSource=users") || c.TLS.InsecureSkipVerify {
		t.Fatal("resolver database option rejected or changed", err)
	}
	spec.Options["database"] = "other"
	if _, err := sourceConnection(spec); err == nil {
		t.Fatal("different database option accepted")
	}
}
