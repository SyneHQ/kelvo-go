package oracle

import "testing"

func TestRequiresVerifiedTCPS(t *testing.T) {
	if err := validateDSN("oracle://user:pass@localhost:2484/service?SSL=enable&SSL%20VERIFY=true"); err != nil {
		t.Fatalf("secure DSN rejected: %v", err)
	}
	for _, dsn := range []string{"oracle://user:pass@localhost:1521/service", "oracle://user:pass@localhost:2484/service?SSL=enable&SSL%20VERIFY=false"} {
		if err := validateDSN(dsn); err == nil {
			t.Fatal("insecure DSN accepted")
		}
	}
}
