// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package elasticsearch

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"net/http"
	"os"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

const liveBase int64 = 9007199254740993
const liveRows = 1205

func liveEngine(t *testing.T) *Engine {
	t.Helper()
	path := os.Getenv("KELVO_TEST_ELASTICSEARCH_CONFIG")
	if path == "" {
		t.Skip("run scripts/elasticsearch_acceptance.py for live Elasticsearch")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal("live fixture configuration unavailable")
	}
	var cfg struct{ URL, Token, CA string }
	if json.Unmarshal(data, &cfg) != nil {
		t.Fatal("invalid live fixture configuration")
	}
	t.Setenv("KELVO_SOURCE_ES_LIVE_URL", cfg.URL)
	t.Setenv("KELVO_SOURCE_ES_LIVE_TOKEN", cfg.Token)
	e, err := New(catalog.Config{Sources: []catalog.Source{{
		ID: "es", Type: "elasticsearch", URLEnv: "KELVO_SOURCE_ES_LIVE_URL",
		TokenEnv: "KELVO_SOURCE_ES_LIVE_TOKEN",
	}}}, query.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	ca, err := os.ReadFile(cfg.CA)
	if err != nil {
		t.Fatal("fixture CA unavailable")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		t.Fatal("invalid fixture CA")
	}
	// Trust only this disposable server's CA; production TLS validation is intact.
	tr := e.client.HTTP.Transport.(*http.Transport)
	tr.TLSClientConfig.RootCAs = roots
	t.Cleanup(func() { e.Close() })
	return e
}

type liveSink struct {
	rows, nulls int
	bad         bool
}

func (*liveSink) Schema(s *arrow.Schema) error {
	if s.NumFields() != 2 || s.Field(0).Type.ID() != arrow.INT64 || s.Field(1).Type.ID() != arrow.STRING {
		return query.NewError("UNSUPPORTED", "unexpected live schema")
	}
	return nil
}
func (s *liveSink) Write(r arrow.RecordBatch) error {
	ids, labels := r.Column(0).(*array.Int64), r.Column(1).(*array.String)
	for i := 0; i < ids.Len(); i++ {
		if ids.IsNull(i) || ids.Value(i) != liveBase+int64(s.rows) {
			s.bad = true
		}
		if labels.IsNull(i) {
			s.nulls++
		}
		s.rows++
	}
	return nil
}
func TestLiveElasticsearchPaginationAndTypes(t *testing.T) {
	e := liveEngine(t)
	sink := &liveSink{}
	stats, err := e.Execute(context.Background(), query.Request{Mode: "native", ConnectionID: "es", SQL: "SELECT id, label FROM kelvo_native_acceptance ORDER BY id"}, sink)
	if err != nil || sink.bad || sink.rows != liveRows || sink.nulls != 121 || stats.Rows != liveRows {
		t.Fatalf("live rows=%d nulls=%d mismatch=%v stats=%+v err=%v", sink.rows, sink.nulls, sink.bad, stats, err)
	}
}
func TestLiveElasticsearchBoundParameters(t *testing.T) {
	e := liveEngine(t)
	sink := &capture{}
	r := query.Request{Mode: "native", ConnectionID: "es", SQL: "SELECT COUNT(*) AS n FROM kelvo_native_acceptance WHERE id > ? AND label = ?",
		Parameters: []query.Parameter{{Type: "int64", Value: json.RawMessage("\"9007199254740993\"")}, {Type: "string", Value: json.RawMessage("\"item-1204\"")}}}
	stats, err := e.Execute(context.Background(), r, sink)
	if err != nil || stats.Rows != 1 || len(sink.values) != 1 || sink.values[0] != 1 {
		t.Fatalf("stats=%+v count=%v err=%v", stats, sink.values, err)
	}
}
func TestLiveElasticsearchLimitsAndPermissions(t *testing.T) {
	e := liveEngine(t)
	e.limits.MaxRows = 10
	_, err := e.Execute(context.Background(), query.Request{Mode: "native", ConnectionID: "es", SQL: "SELECT id FROM kelvo_native_acceptance"}, &capture{})
	if err == nil || query.PublicError(err).Code != "RESOURCE_EXHAUSTED" {
		t.Fatalf("limit err=%v", err)
	}
	_, err = e.Execute(context.Background(), query.Request{Mode: "native", ConnectionID: "es", SQL: "SELECT id FROM kelvo_native_forbidden"}, &capture{})
	if err == nil || query.PublicError(err).Code != "QUERY_FAILED" {
		t.Fatalf("permissions err=%v", err)
	}
}
