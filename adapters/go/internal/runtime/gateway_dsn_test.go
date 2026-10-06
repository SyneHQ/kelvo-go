package runtime

import (
	"github.com/SYNEHQ/kelvo-go/adapter"
	"testing"
)

func TestGatewayMySQLWireEncodingPreservesOpaqueCredentials(t *testing.T) {
	for _, password := range []string{"p@tcp(other)/unsafe?tls=false", "colon:secret@last@", "close)and(open/?&", "space + unicode 日本語", "percent%2f%3f"} {
		for _, fixture := range []struct{ database, encoded string }{
			{"warehouse", "warehouse"}, {"sales + reports", "sales%20+%20reports"},
			{"日本語", "%E6%97%A5%E6%9C%AC%E8%AA%9E"}, {"question?name", "question%3Fname"}, {"percent%name", "percent%25name"},
		} {
			dsn := "reader:" + password + "@tcp([2001:db8::1]:3306)/" + fixture.encoded + "?loc=UTC&parseTime=true&time_zone=%27%2B00%3A00%27&timeout=5s&tls=true"
			source, err := sourceConnection(adapter.ConnectionSpec{Engine: "mysql", DSN: dsn, Database: fixture.database, TenantID: "team", ConnectionID: "saved", Revision: "revision"})
			if err != nil || source.Username != "reader" || source.Password != password || source.Namespace != fixture.database || source.Host != "2001:db8::1" || source.Port != 3306 || source.TLS == nil || source.TLS.InsecureSkipVerify {
				t.Fatal("gateway DSN scope or credentials changed", err)
			}
		}
	}
}
