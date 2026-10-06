package provider

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestDocumentDecoderPreservesExactFieldNamesAndNumbers(t *testing.T) {
	var value map[string]any
	if err := DecodeDocument([]byte(`{"Name":"upper","name":"lower","value":9007199254740993,"nested":{"amount":12345678901234567890.12345}}`), &value, 8<<20); err != nil {
		t.Fatal(err)
	}
	if value["Name"] != "upper" || value["name"] != "lower" || value["value"] != json.Number("9007199254740993") {
		t.Fatal("document changed", value)
	}
	for _, raw := range []string{`{"name":1,"name":2}`, `{"x":1} {"y":2}`, strings.Repeat(`[`, 34) + `0` + strings.Repeat(`]`, 34)} {
		if DecodeDocument([]byte(raw), &value, 8<<20) == nil {
			t.Fatalf("invalid document accepted %s", raw)
		}
	}
	if DecodeDocument([]byte(`{"name":"long"}`), &value, 5) == nil {
		t.Fatal("byte limit ignored")
	}
}
