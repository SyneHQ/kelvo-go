package provider

import (
	"encoding/json"
	"testing"

	"github.com/SYNEHQ/kelvo-go/operations"
)

func TestSaaSReadsPreserveLiteralsAndRejectMutation(t *testing.T) {
	for _, engine := range []string{"stripe", "ga4", "google_ads", "facebook_ads", "salesforce"} {
		query := "SELECT name FROM customers WHERE name = 'Delete; Keep CASE'"
		raw, _ := json.Marshal(SaaSQuery{SQL: query})
		kind, spec, err := Invocation(engine, raw)
		if err != nil || kind != operations.NativeRead || spec.Provider != engine || spec.Command != "query" {
			t.Fatalf("%s: %s %+v %v", engine, kind, spec, err)
		}
		q, err := ParseSaaS(engine, spec.Parameters[0].Value)
		if err != nil || q.SQL != query {
			t.Fatal("query changed")
		}
		for _, sql := range []string{"DELETE FROM customers", "SELECT id FROM customers; DELETE FROM customers", "SELECT id INTO OUTFILE 'x' FROM customers", "SELECT id FROM customers /* comment */", "SELECT id FROM customers WHERE name = 'unterminated"} {
			raw, _ := json.Marshal(SaaSQuery{SQL: sql})
			if _, err := ParseSaaS(engine, raw); err == nil {
				t.Errorf("accepted %s", sql)
			}
		}
	}
}
func TestSaaSAccountScopes(t *testing.T) {
	for _, value := range []string{"123/x", "../123", "123?x=y", "123\n", ""} {
		if ValidSaaSAccount("google_ads", value) {
			t.Errorf("invalid account %q", value)
		}
	}
	for _, origin := range []string{"http://org.my.salesforce.com", "https://login.salesforce.com", "https://org.my.salesforce.com.attacker.example", "https://user@org.my.salesforce.com", "https://org.my.salesforce.com/other"} {
		if ValidSalesforceOrigin(origin) {
			t.Errorf("invalid origin %s", origin)
		}
	}
	if !ValidSalesforceOrigin("https://org.my.salesforce.com") || !ValidSaaSAccount("facebook_ads", "act_123") {
		t.Fatal("valid scope rejected")
	}
}
