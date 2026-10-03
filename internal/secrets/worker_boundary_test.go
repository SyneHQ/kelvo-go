//go:build linux || darwin

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package secrets

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/worker"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestMain(m *testing.M) {
	if len(os.Args) == 2 && os.Args[1] == "worker" {
		os.Exit(secretWorkerChild())
	}
	os.Exit(m.Run())
}

func secretWorkerChild() int {
	raw, err := io.ReadAll(io.LimitReader(os.Stdin, 2<<20))
	if err != nil {
		return 2
	}
	var input worker.Input
	if json.Unmarshal(raw, &input) != nil || input.Request.Mode != "native" || input.Request.ConnectionID != "selected" || len(input.Config.Sources) != 1 || input.Config.Sources[0].ID != "selected" {
		return 2
	}
	for _, forbidden := range []string{"fixture-master", "credentials_file", "providers", "private-secret", "fixture-selected-value", "stale-environment", "unselected"} {
		if strings.Contains(string(raw), forbidden) {
			return 2
		}
	}
	allowed := map[string]bool{"PATH": true, "HOME": true, "TMPDIR": true, "GOMAXPROCS": true, "KELVO_SOURCE_SELECTED_URL": true, "KELVO_SOURCE_SELECTED_TOKEN": true}
	for _, env := range os.Environ() {
		key, _, _ := strings.Cut(env, "=")
		if !allowed[key] {
			return 2
		}
	}
	if os.Getenv("KELVO_SOURCE_SELECTED_URL") != "https://source.example" || os.Getenv("KELVO_SOURCE_SELECTED_TOKEN") != "fixture-selected-value" {
		return 2
	}
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	builder := array.NewInt64Builder(memory.DefaultAllocator)
	builder.Append(1)
	column := builder.NewArray()
	builder.Release()
	record := array.NewRecordBatch(schema, []arrow.Array{column}, 1)
	column.Release()
	defer record.Release()
	limits := input.Limits
	limits.ResultCompression = ""
	sink := worker.NewIPCSink(os.Stdout, limits)
	defer sink.Abort()
	if sink.Schema(schema) != nil || sink.Write(record) != nil || sink.Finish() != nil {
		return 3
	}
	if json.NewEncoder(os.Stderr).Encode(worker.Outcome{Stats: query.Stats{Rows: 1, Batches: 1, Backend: "fixture"}}) != nil {
		return 4
	}
	return 0
}

type cloudWorkerSink struct{}

func (cloudWorkerSink) Schema(*arrow.Schema) error    { return nil }
func (cloudWorkerSink) Write(arrow.RecordBatch) error { return nil }

func TestCloudSecretsReachOnlySelectedChildEnvironment(t *testing.T) {
	config := cloudConfig(t, "gcp_secret_manager")
	selected := config.References["KEY"]
	other := selected
	other.Secret = "projects/123456789012/secrets/unselected"
	config.References = map[string]Reference{"KELVO_SOURCE_SELECTED_TOKEN": selected, "KELVO_SOURCE_OTHER_TOKEN": other}
	var calls atomic.Int32
	provider := cloudFixture(t, config, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/v1/"+selected.Secret+"/versions/latest:access" || r.Header.Get("Authorization") != "Bearer fixture-master-token" {
			t.Error("unselected source or ambient provider credentials used")
		}
		json.NewEncoder(w).Encode(cloudResponse("gcp_secret_manager", selected, "fixture-selected-value"))
	})
	t.Setenv("KELVO_SOURCE_SELECTED_URL", "https://source.example")
	t.Setenv("KELVO_SOURCE_SELECTED_TOKEN", "stale-environment")
	t.Setenv("KELVO_SOURCE_OTHER_TOKEN", "unselected-source")
	for _, key := range []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AZURE_CLIENT_SECRET", "GOOGLE_APPLICATION_CREDENTIALS", "KELVO_TOKEN"} {
		t.Setenv(key, "fixture-master-ambient")
	}
	catalogue := catalog.Config{Sources: []catalog.Source{
		{ID: "selected", Type: "databricks", URLEnv: "KELVO_SOURCE_SELECTED_URL", TokenEnv: "KELVO_SOURCE_SELECTED_TOKEN"},
		{ID: "other", Type: "databricks", TokenEnv: "KELVO_SOURCE_OTHER_TOKEN"},
	}}
	executor, err := worker.New(catalogue, query.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	executor.Secrets = provider
	request := query.Request{Mode: "native", ConnectionID: "selected", SQL: "SELECT source_secret"}
	stats, err := executor.Execute(context.Background(), request, cloudWorkerSink{})
	if err != nil || stats.Rows != 1 || calls.Load() != 1 {
		t.Fatal("selected cloud credential boundary failed", err)
	}
	if !provider.Invalidate("KELVO_SOURCE_SELECTED_TOKEN") {
		t.Fatal("invalidation failed")
	}
	if err = os.Remove(config.Providers["cloud"].CredentialsFile); err != nil {
		t.Fatal(err)
	}
	if _, err = executor.Execute(context.Background(), request, cloudWorkerSink{}); err == nil || query.PublicError(err).Code != "CONFIGURATION_ERROR" || calls.Load() != 1 {
		t.Fatal("cloud authority failure fell back to stale environment", err)
	}
}
