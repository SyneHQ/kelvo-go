package provider

import (
	"github.com/SYNEHQ/kelvo-go/operations"
	"testing"
)

func TestElasticsearchNativeAuthorityAndScope(t *testing.T) {
	for _, tt := range []struct {
		input string
		kind  operations.Kind
	}{
		{`GET /_cluster/health`, operations.NativeRead}, {`POST /logs/_search {"query":{"match_all":{}}}`, operations.NativeRead}, {`PUT /logs/_doc/one {"id":9007199254740993}`, operations.NativeExecute}, {`DELETE /logs/_doc/one`, operations.NativeExecute}, {"POST /logs/_bulk\n{\"index\":{\"_id\":\"one\"}}\n{\"id\":9007199254740993}\n", operations.NativeExecute},
	} {
		kind, spec, err := ParseElasticsearchInvocation(tt.input)
		if err != nil || kind != tt.kind {
			t.Fatal(tt.input, kind, err)
		}
		q, parsed, err := ParseElasticsearch(spec.Parameters[0].Value)
		if err != nil || parsed != kind || ElasticsearchScope(q, "logs") != nil {
			t.Fatal("normalized request lost binding", err)
		}
	}
	for _, input := range []string{`PUT /_cluster/settings {"persistent":{}}`, `POST /_reindex {"source":{"remote":{"host":"https://other"}}}`, `GET https://other.example/`, `GET /logs/%2f../_search`, `GET /../_search`, `POST /logs/_search {"query":{},"query":{}}`} {
		if _, _, err := ParseElasticsearchInvocation(input); err == nil {
			t.Fatal("unsafe native request admitted", input)
		}
	}
	for _, input := range []string{`PUT /foreign/_doc/one {"id":1}`, "POST /logs/_bulk\n{\"index\":{\"_index\":\"foreign\"}}\n{\"id\":1}\n", `POST /logs/_mget {"docs":[{"_index":"foreign","_id":"one"}]}`} {
		_, spec, err := ParseElasticsearchInvocation(input)
		if err != nil {
			t.Fatal(err)
		}
		q, _, _ := ParseElasticsearch(spec.Parameters[0].Value)
		if ElasticsearchScope(q, "logs") == nil {
			t.Fatal("cross-index body admitted")
		}
	}
}
