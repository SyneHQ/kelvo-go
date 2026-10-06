package operations

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestNativeDocumentKeysKeepCaseAcrossRequestCustody(t *testing.T) {
	value := json.RawMessage(`{"collection":"orders","document":{"Name":"UPPER","name":"lower","id":9007199254740993,"nested":[{"A":1,"a":2}]}}`)
	r := Request{Version: Version, Kind: NativeExecute, Connection: ConnectionRef{ID: "saved", Database: "app"}, IdempotencyKey: "write", Spec: Spec{Native: &NativeSpec{Provider: "mongodb", Command: "insert_one", Parameters: []Parameter{{Type: "json", Value: value}}}}}
	wire, err := Encode(r)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseRequest(wire)
	if err != nil || string(parsed.Spec.Native.Parameters[0].Value) != string(value) {
		t.Fatal("document key case changed", err)
	}
	digest, err := Digest(parsed)
	if err != nil {
		t.Fatal(err)
	}
	cloned, err := Clone(parsed)
	if err != nil {
		t.Fatal(err)
	}
	clonedDigest, err := Digest(cloned)
	if err != nil || digest != clonedDigest {
		t.Fatal("document changed across custody")
	}
	cloned.Spec.Native.Parameters[0].Value = json.RawMessage(strings.Replace(string(value), `"name":"lower"`, `"name":"changed"`, 1))
	changedDigest, err := Digest(cloned)
	if err != nil || digest == changedDigest {
		t.Fatal("case-sensitive value not bound by digest")
	}
	for _, bad := range []string{
		strings.Replace(string(wire), `"kind":`, `"Kind":"native.read","kind":`, 1),
		strings.Replace(string(wire), `"id":"saved"`, `"id":"saved","ID":"other"`, 1),
		strings.Replace(string(wire), `"id":"saved"`, `"id":"saved","schema":"one","ſchema":"other"`, 1),
		strings.Replace(string(wire), `"provider":"mongodb"`, `"provider":"mongodb","Provider":"other"`, 1),
		strings.Replace(string(wire), `"type":"json"`, `"type":"json","Type":"string"`, 1),
		strings.Replace(string(wire), `"name":"lower"`, `"Name":"duplicate"`, 1),
	} {
		if _, err := ParseRequest([]byte(bad)); err == nil {
			t.Fatal("ambiguous authority or duplicate record key accepted")
		}
	}
}

func TestStrictDecodeUnicodeSourceAliasesCannotOverrideAuthority(t *testing.T) {
	var value struct {
		Source struct {
			Schema string `json:"schema"`
		} `json:"source"`
	}
	for _, raw := range []string{
		`{"source":{"schema":"one"},"ſource":{"schema":"other"}}`,
		`{"source":{"schema":"one","ſchema":"other"}}`,
	} {
		if DecodeStrict([]byte(raw), &value, 1024) == nil {
			t.Fatal("Unicode alias changed authority")
		}
	}
}

func TestStrictDecodeMapValuesStillProtectTypedAuthority(t *testing.T) {
	type authority struct {
		Connection string          `json:"connection"`
		Document   json.RawMessage `json:"document"`
	}
	var input map[string]authority
	if err := DecodeStrict([]byte(`{"Tenant":{"connection":"one","document":{"Name":1,"name":2}},"tenant":{"connection":"two"}}`), &input, 1024); err != nil || len(input) != 2 {
		t.Fatal("distinct map keys changed", err)
	}
	for _, raw := range []string{
		`{"tenant":{"connection":"one","Connection":"two"}}`,
		`{"tenant":{"connection":"one","unknown":true}}`,
		`{"tenant":{"connection":"one"},"tenant":{"connection":"two"}}`,
		`{"tenant":{"connection":"one","document":{"Name":1,"Name":2}}}`,
	} {
		if DecodeStrict([]byte(raw), &input, 1024) == nil {
			t.Fatal("ambiguous typed map value accepted")
		}
	}
}
