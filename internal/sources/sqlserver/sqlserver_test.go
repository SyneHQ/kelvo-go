package sqlserver

import "testing"

func TestRequiresVerifiedEncryptedConnection(t *testing.T) {
	for _, dsn := range []string{"sqlserver://user:pass@localhost?encrypt=true&tlsmin=1.2", "sqlserver://user:pass@localhost?encrypt=strict"} {
		if err := validateDSN(dsn); err != nil {
			t.Fatalf("secure DSN rejected: %v", err)
		}
	}
	for _, dsn := range []string{"sqlserver://user:pass@localhost", "sqlserver://user:pass@localhost?encrypt=disable", "sqlserver://user:pass@localhost?encrypt=true&TrustServerCertificate=true", "sqlserver://user:pass@localhost?encrypt=true&tlsmin=1.0"} {
		if err := validateDSN(dsn); err == nil {
			t.Fatal("insecure DSN accepted")
		}
	}
}
