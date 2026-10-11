package worker

import "testing"

func TestValidConnectionDSNPostgresPlaintextIsOperationOnly(t *testing.T) {
	base := "postgres://reader:test-only@db.example.test:5432/warehouse?connect_timeout=5&sslmode="
	for _, kind := range []string{"postgres", "cockroachdb", "alloydb", "redshift"} {
		for mode, want := range map[string]bool{"verify-full": true, "disable": true, "require": false, "prefer": false, "allow": false, "verify-ca": false, "": false} {
			if got := validConnectionDSN(kind, base+mode, "warehouse", true); got != want {
				t.Fatalf("operation %s sslmode=%q accepted=%v, want %v", kind, mode, got, want)
			}
		}
		if validConnectionDSN(kind, base+"disable", "warehouse", false) {
			t.Fatalf("analytics path accepted TLS-off %s", kind)
		}
		if !validConnectionDSN(kind, base+"verify-full", "warehouse", false) {
			t.Fatalf("analytics path lost verified %s", kind)
		}
	}
	if validConnectionDSN("postgres", base+"disable&sslrootcert=/tmp/ca", "warehouse", true) {
		t.Fatal("extra TLS file option accepted with sslmode=disable")
	}
	for _, kind := range []string{"mysql", "sqlserver", "oracle"} {
		if validConnectionDSN(kind, base+"disable", "warehouse", true) {
			t.Fatalf("%s accepted a TLS-off PostgreSQL DSN", kind)
		}
	}
}
