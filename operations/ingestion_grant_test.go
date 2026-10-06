package operations

import (
	"strings"
	"testing"
)

func TestDedicatedIngestionAuthorityDoesNotReplaceOtherAuthorizers(t *testing.T) {
	request := Request{Version: Version, Kind: IngestionInstall, Connection: ConnectionRef{ID: "saved-a", Database: "warehouse", Schema: "landing"}, IdempotencyKey: "install-a", Spec: Spec{Ingestion: &IngestionSpec{Scope: IngestionScope{SourceID: "source-a", Stream: "events", Binding: strings.Repeat("a", 64)}}}}
	claims, trust, key, now := grantFixture(t, request)
	if _, err := SignGrant(claims, key); err != nil {
		t.Fatal("standalone authorizer was coupled to upstream jobs", err)
	}
	claims.Subject = Subject{Kind: "user", ID: "user-a"}
	claims.Authorization = Authorization{Kind: "ingestion", Ingestion: &IngestionAuthority{RunID: "run-a", LeaseID: "lease-a", SourceRevision: 1, AllowInstall: true, ExpiresAt: claims.ExpiresAt}}
	token, err := SignGrant(claims, key)
	if err != nil {
		t.Fatal(err)
	}
	verified, err := VerifyGrant(token, trust, request, now)
	if err != nil || verified.Authorization.Ingestion.RunID != "run-a" {
		t.Fatal("ingestion authority binding lost", err)
	}
	for _, change := range []func(*GrantClaims){
		func(c *GrantClaims) { c.Authorization.Ingestion = nil },
		func(c *GrantClaims) { c.Authorization.Ingestion.AllowInstall = false },
		func(c *GrantClaims) { c.Authorization.Ingestion.LeaseID = "" },
		func(c *GrantClaims) { c.Authorization.Ingestion.SourceRevision = 0 },
		func(c *GrantClaims) { c.Authorization.Ingestion.ExpiresAt = c.ExpiresAt - 1 },
		func(c *GrantClaims) { c.Subject.Kind = "admin" },
		func(c *GrantClaims) { c.Authorization.Kind = "trusted_app" },
		func(c *GrantClaims) { c.Operation = StatementExecute },
	} {
		next := claims
		proof := *claims.Authorization.Ingestion
		next.Authorization.Ingestion = &proof
		change(&next)
		if _, err := SignGrant(next, key); err == nil {
			t.Fatal("incomplete or incompatible ingestion authority accepted")
		}
	}
}
