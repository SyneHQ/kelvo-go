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
