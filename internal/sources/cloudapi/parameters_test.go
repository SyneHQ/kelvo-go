package cloudapi

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/operations"
)

func TestD1BindingsRejectPrecisionLossAndPreserveBlob(t *testing.T) {
	parameters := []operations.Parameter{{Type: "int64", Value: json.RawMessage(`"9007199254740991"`)}, {Type: "binary", Value: json.RawMessage(`"AP8="`)}, {Type: "bool", Value: json.RawMessage(`true`)}, {Type: "null", Value: json.RawMessage(`null`)}}
	values, err := D1Parameters(parameters)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(values)
	if string(raw) != `[9007199254740991,[0,255],1,null]` {
		t.Fatal(string(raw))
	}
	for _, p := range []operations.Parameter{{Type: "int64", Value: json.RawMessage(`"9007199254740993"`)}, {Type: "uint64", Value: json.RawMessage(`"18446744073709551615"`)}, {Type: "decimal128", Value: json.RawMessage(`"1.20"`)}, {Type: "int8", Value: json.RawMessage(`128`)}} {
		if _, err := D1Parameters([]operations.Parameter{p}); err == nil {
			t.Fatal("unrepresentable binding accepted", p.Type)
		}
	}
}

func TestDatabricksBindingsPreserveExactValuesAndNull(t *testing.T) {
	values, err := DatabricksParameters([]operations.Parameter{{Type: "int64", Value: json.RawMessage(`"9007199254740993"`)}, {Type: "uint64", Value: json.RawMessage(`"18446744073709551615"`)}, {Type: "decimal128", Value: json.RawMessage(`"9007199254740993.01"`)}, {Type: "null", Value: json.RawMessage(`null`)}, {Type: "string", Value: json.RawMessage(`""`)}})
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(values)
	want := `[{"name":"p1","type":"BIGINT","value":"9007199254740993"},{"name":"p2","type":"DECIMAL(20,0)","value":"18446744073709551615"},{"name":"p3","type":"DECIMAL(18,2)","value":"9007199254740993.01"},{"name":"p4","type":"STRING"},{"name":"p5","type":"STRING","value":""}]`
	if string(raw) != want {
		t.Fatal(string(raw))
	}
	for _, p := range []operations.Parameter{{Type: "timestamp", Value: json.RawMessage(`"2026-10-06T12:00:00.000000001Z"`)}, {Type: "binary", Value: json.RawMessage(`"AP8="`)}, {Type: "decimal256", Value: json.RawMessage(`"1"`)}} {
		if _, err := DatabricksParameters([]operations.Parameter{p}); err == nil {
			t.Fatal("unsupported binding accepted", p.Type)
		}
	}
}

func TestDatabricksMarkersPreserveSQLText(t *testing.T) {
	sql := "SELECT ?, '?', `?`, \"?\", /* ? /* :p7 */ */ ? -- ?\n"
	got, err := DatabricksStatement(sql, 2)
	if err != nil || got != strings.Replace(strings.Replace(sql, "SELECT ?", "SELECT :p1", 1), "*/ ? --", "*/ :p2 --", 1) {
		t.Fatal(got, err)
	}
	for _, valid := range []string{"SELECT :p2, :p1, :p2", "SELECT :p1::STRING, :p2"} {
		if got, err := DatabricksStatement(valid, 2); err != nil || got != valid {
			t.Fatal(got, err)
		}
	}
	for _, invalid := range []string{"SELECT ?", "SELECT ?,?,?", "SELECT :p1,?", "SELECT :p1,:p3", "SELECT :p01,:p2", "SELECT :name,:p2", "SELECT '?", "SELECT /* ?"} {
		if _, err := DatabricksStatement(invalid, 2); err == nil {
			t.Fatal("invalid markers accepted", invalid)
		}
	}
}
