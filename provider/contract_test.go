package provider

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/SYNEHQ/kelvo-go/operations"
)

func TestProviderAuthorityDoesNotTrustToolAnnotations(t *testing.T) {
	for _, tc := range []struct {
		engine, raw string
		kind        operations.Kind
	}{
		{"motherduck", `{"tool":"query","arguments":{"query":"SELECT 1"}}`, operations.NativeRead},
		{"motherduck", `{"tool":"query_rw","arguments":{"query":"DELETE FROM x"}}`, operations.NativeExecute},
		{"salesforce_data360", `{"tool":"execute","arguments":{"toolName":"delete"}}`, operations.NativeExecute},
		{"daloopa", `{"operation":"list_tools"}`, operations.NativeRead},
		{"ramp", `{"resource":"transactions","params":{"page_size":"1"}}`, operations.NativeRead},
	} {
		kind, spec, err := Invocation(tc.engine, []byte(tc.raw))
		if err != nil || kind != tc.kind || spec.ReturnResult != (kind == operations.NativeExecute) {
			t.Fatal(tc, kind, err)
		}
	}
	for _, raw := range []string{`{"operation":"list_tools","tool":"delete"}`, `{"tool":"query","tool":"query_rw"}`, `{"resource":"../token"}`, `{"tool":"query","readOnly":true}`, `null`, `{"tool":"query"} {"tool":"delete"}`} {
		if _, err := ParseQuery([]byte(raw)); err == nil {
			t.Fatal("ambiguous query accepted", raw)
		}
	}
}

func TestProviderArgumentsKeepExactNumbers(t *testing.T) {
	raw := []byte(`{"tool":"execute","arguments":{"id":9007199254740993,"decimal":12345678901234567890.1234567,"nothing":null}}`)
	_, spec, err := Invocation("salesforce_data360", raw)
	if err != nil || !bytes.Equal(raw, spec.Parameters[0].Value) {
		t.Fatal("native argument bytes changed", err)
	}
	q, err := ParseQuery(raw)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(q)
	if err != nil || !bytes.Contains(encoded, []byte("9007199254740993")) || !bytes.Contains(encoded, []byte("12345678901234567890.1234567")) {
		t.Fatal("provider argument precision lost")
	}
}
