package provider

import (
	"github.com/SYNEHQ/kelvo-go/operations"
	"testing"
)

func TestPostHogTextAuthority(t *testing.T) {
	for _, tc := range []struct {
		query string
		kind  operations.Kind
	}{
		{`SELECT count() FROM events`, operations.NativeRead},
		{`WITH e AS (SELECT * FROM events) SELECT count() FROM e`, operations.NativeRead},
		{`QUERY {"query":{"kind":"HogQLQuery","query":"SELECT 1"}}`, operations.NativeRead},
		{`GET /api/projects/:project_id/events/?limit=1`, operations.NativeRead},
		{`GET /api/projects/42/events/`, operations.NativeRead},
		{`POST /api/projects/:project_id/annotations/ {"content":"change"}`, operations.NativeExecute},
		{`DELETE /api/projects/:project_id/annotations/1/`, operations.NativeExecute},
	} {
		kind, spec, err := InvocationText("posthog", tc.query)
		if err != nil || kind != tc.kind || spec.ReturnResult != (kind == operations.NativeExecute) {
			t.Fatal(tc, kind, err)
		}
		if _, got, err := ParsePostHog(spec.Parameters[0].Value); err != nil || got != kind {
			t.Fatal(got, err)
		}
	}
	for _, query := range []string{
		`QUERY {"query":{"kind":"ArbitraryMutation"}}`,
		`QUERY {"query":{"kind":"HogQLQuery"},"refresh":"force_async"}`,
		`{"method":"GET","path":"/api/projects/:project_id/events/","sql":"SELECT 1"}`,
		`GET /api/users/`, `GET https://evil.invalid/api/projects/:project_id/events/`,
		`GET /api/projects/:project_id/../users/`, `GET /api/projects/:project_id/%2e%2e/users/`,
		`GET /api/projects/:project_id/events/?project_id=9`, `GET /api/projects/:project_id/events/?api_key=secret`,
		`GET /api/projects/:project_id/events/ {"query":"hidden"}`,
	} {
		if _, _, err := InvocationText("posthog", query); err == nil {
			t.Fatal("unsafe accepted", query)
		}
	}
}

func TestPostHogSavedProjectAndOrigin(t *testing.T) {
	for _, path := range []string{`/api/projects/:project_id/events/`, `/api/projects/42/events/`} {
		got, err := PostHogPath(path, "42")
		if err != nil || got != "/api/projects/42/events/" {
			t.Fatal(got, err)
		}
	}
	if _, err := PostHogPath("/api/projects/43/events/", "42"); err == nil {
		t.Fatal("cross project accepted")
	}
	for _, origin := range []string{"https://us.posthog.com", "https://private.example:8443/"} {
		if !ValidPostHogOrigin(origin) {
			t.Fatal(origin)
		}
	}
	for _, origin := range []string{"http://us.posthog.com", "https://user:pass@us.posthog.com", "https://us.posthog.com/api/", "https://us.posthog.com?", "https://us.posthog.com#x", "https://us.posthog.com\\evil"} {
		if ValidPostHogOrigin(origin) {
			t.Fatal(origin)
		}
	}
}
