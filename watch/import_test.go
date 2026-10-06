package watch

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestLegacyImportBindsScopeGenerationAndCurrentRevision(t *testing.T) {
	scope := Scope{TeamID: "team", ConnectionID: "saved", Database: "app", Schema: "app", Table: "orders", ID: "watcher", Generation: "generation"}
	value := Import{Version: Version, ScopeSHA256: scope.Key(), Generation: scope.Generation, SourceIdentitySHA256: strings.Repeat("a", 64), SourceRevision: "revision-a", ResumeToken: json.RawMessage(`{"_data":"opaque-server-token"}`)}
	raw, err := EncodeImport(value, scope, "revision-a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ParseImport(raw, scope, "revision-a"); err != nil {
		t.Fatal(err)
	}
	if _, err := ParseImport(raw, scope, "revision-b"); err == nil {
		t.Fatal("changed source revision accepted")
	}
	if _, err := ParseImport(raw, scope, ""); err == nil {
		t.Fatal("missing current revision accepted")
	}
	other := scope
	other.TeamID = "another-team"
	if _, err := ParseImport(raw, other, "revision-a"); err == nil {
		t.Fatal("cross-team token accepted")
	}
	other = scope
	other.Generation = "new-generation"
	if _, err := ParseImport(raw, other, "revision-a"); err == nil {
		t.Fatal("stale generation accepted")
	}
	value.ResumeToken = json.RawMessage(`{"_data":"one","_data":"two"}`)
	if _, err := EncodeImport(value, scope, "revision-a"); err == nil {
		t.Fatal("ambiguous token accepted")
	}
	value.ResumeToken = json.RawMessage(`{"_data":"opaque-server-token","A":9007199254740993,"a":{"$numberLong":"9007199254740993"}}`)
	raw, err = EncodeImport(value, scope, "revision-a")
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := ParseImport(raw, scope, "revision-a")
	if err != nil || string(decoded.ResumeToken) != string(value.ResumeToken) {
		t.Fatalf("case-sensitive token changed: %v", err)
	}
	for _, bad := range []string{
		strings.Replace(string(raw), `"version":`, `"Version":`, 1),
		strings.Replace(string(raw), `"version":1`, `"version":1,"Version":1`, 1),
		strings.Replace(string(raw), `"_data":"opaque-server-token"`, `"_data":"one","_data":"two"`, 1),
	} {
		if _, err := DecodeImport([]byte(bad), scope); err == nil {
			t.Fatal("ambiguous import accepted")
		}
	}
}
