//go:build !slim

package runtime

import (
	"encoding/pem"
	"net/http/httptest"
	"testing"

	"github.com/SYNEHQ/kelvo-go/adapter"
)

func TestClickHouseOperationCarriesPrivateCA(t *testing.T) {
	server := httptest.NewTLSServer(nil)
	defer server.Close()
	root := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}))
	spec := adapter.ConnectionSpec{Engine: "clickhouse", URL: "https://ch.example:8443/?database=app", Username: "reader", Password: "test-only", Database: "app",
		TenantID: "team", ConnectionID: "saved", Revision: "revision", Options: map[string]string{"tls_ca_pem": root}}
	c, err := sourceConnection(spec)
	if err != nil || c.TLS == nil || c.TLS.RootCAs == nil || c.TLS.ServerName != "ch.example" || c.TLS.MinVersion < 0x0303 {
		t.Fatalf("private CA not carried: %v", err)
	}
	spec.Options["tls_ca_pem"] = "garbage"
	if _, err := sourceConnection(spec); err == nil {
		t.Fatal("invalid CA accepted")
	}
}

func TestClickHouseSourceSchemes(t *testing.T) {
	spec := func(raw string) adapter.ConnectionSpec {
		return adapter.ConnectionSpec{Engine: "clickhouse", URL: raw, Username: "reader", Password: "test-only", Database: "app", TenantID: "team", ConnectionID: "saved", Revision: "revision"}
	}
	c, err := sourceConnection(spec("clickhouses://ch.example:19440?database=app"))
	if err != nil || c.Port != 19440 || c.Options["protocol"] != "native" || c.TLS == nil || c.TLS.ServerName != "ch.example" {
		t.Fatalf("clickhouses: %+v %v", c, err)
	}
	if c, err = sourceConnection(spec("clickhouses://ch.example?database=app")); err != nil || c.Port != 9440 {
		t.Fatalf("clickhouses default port: %v %v", c.Port, err)
	}
	if c, err = sourceConnection(spec("https://ch.example:8443/?database=app")); err != nil || c.Port != 8443 || c.Options["protocol"] != "" {
		t.Fatalf("https: %+v %v", c, err)
	}
	if _, err := sourceConnection(spec("clickhouse://ch.example:9000?database=app")); err == nil {
		t.Fatal("plaintext clickhouse:// accepted")
	}
}
