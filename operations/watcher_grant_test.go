package operations

import (
	"strings"
	"testing"
)

func TestWatcherGrantBindsInternalLifecycleAndConfiguration(t *testing.T) {
	request := Request{Version: Version, Kind: WatchInstall, Connection: ConnectionRef{ID: "source-a", Database: "app", Schema: "public"}, IdempotencyKey: "install-a", Spec: Spec{Watch: &WatchSpec{ID: "watch-a", Generation: "generation-a", Target: ObjectRef{Schema: "public", Name: "events"}, Mode: "native"}}}
	claims, trust, key, now := grantFixture(t, request)
	claims.Subject = Subject{Kind: "admin", ID: "watcher-service"}
	claims.Authorization = Authorization{Kind: "watcher", Watcher: &WatcherAuthority{ID: "watch-a", Generation: "generation-a", ConfigurationSHA256: strings.Repeat("a", 64)}}
	token, err := SignGrant(claims, key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyGrant(token, trust, request, now); err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*GrantClaims){
		"missing":         func(c *GrantClaims) { c.Authorization.Watcher = nil },
		"subject":         func(c *GrantClaims) { c.Subject.Kind = "user" },
		"id":              func(c *GrantClaims) { c.Authorization.Watcher.ID = "" },
		"generation":      func(c *GrantClaims) { c.Authorization.Watcher.Generation = "../bad" },
		"configuration":   func(c *GrantClaims) { c.Authorization.Watcher.ConfigurationSHA256 = "bad" },
		"cleanup-install": func(c *GrantClaims) { c.Authorization.Watcher.Cleanup = true },
		"ordinary-write":  func(c *GrantClaims) { c.Operation = StatementExecute },
		"attached-proof":  func(c *GrantClaims) { c.Authorization.Kind = "trusted_app" },
	} {
		t.Run(name, func(t *testing.T) {
			next := claims
			proof := *claims.Authorization.Watcher
			next.Authorization.Watcher = &proof
			change(&next)
			if _, err := SignGrant(next, key); err == nil {
				t.Fatal("invalid watcher authority signed")
			}
		})
	}
	for _, mutate := range []func(*WatcherAuthority){func(v *WatcherAuthority) { v.ID = "watch-b" }, func(v *WatcherAuthority) { v.Generation = "generation-b" }} {
		next := claims
		proof := *claims.Authorization.Watcher
		next.Authorization.Watcher = &proof
		mutate(&proof)
		token, err := SignGrant(next, key)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = VerifyGrant(token, trust, request, now); err == nil {
			t.Fatal("watch request mismatched authority")
		}
	}
	query := Request{Version: Version, Kind: QueryRead, Connection: request.Connection, Spec: Spec{Query: &QuerySpec{SQL: "SELECT id FROM events"}}}
	claims.Operation = QueryRead
	claims.RequestSHA256, _ = Digest(query)
	token, err = SignGrant(claims, key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = VerifyGrant(token, trust, query, now); err != nil {
		t.Fatal("internal query poll rejected", err)
	}
}
