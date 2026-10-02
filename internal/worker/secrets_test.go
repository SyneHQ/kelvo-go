// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	filesecrets "github.com/SYNEHQ/kelvo-go/internal/secrets"
)

type secretResolverFunc func(context.Context, string) (string, bool, error)

func (f secretResolverFunc) Resolve(ctx context.Context, key string) (string, bool, error) {
	return f(ctx, key)
}

// Called in the real re-executed test child, after decoding its stdin payload.
func fileSecretChildMatches(in Input) bool {
	expected := "fixture-first-token"
	if in.Request.SQL == "SELECT file_secret_second" {
		expected = "fixture-second-token"
	}
	if in.Request.Mode != "native" || in.Request.ConnectionID != "selected" || len(in.Config.Sources) != 1 || in.Config.Sources[0].ID != "selected" {
		return false
	}
	if os.Getenv("KELVO_SOURCE_SELECTED_URL") != "https://source.example" || os.Getenv("KELVO_SOURCE_SELECTED_TOKEN") != expected {
		return false
	}
	for _, key := range []string{"KELVO_SOURCE_OTHER_TOKEN", "KELVO_TOKEN", "KELVO_TENANT_A_NATS_PASSWORD"} {
		if _, ok := os.LookupEnv(key); ok {
			return false
		}
	}
	payload, err := json.Marshal(in)
	if err != nil {
		return false
	}
	for _, private := range []string{"fixture-first-token", "fixture-second-token", "parent-secret-files", "unselected-file-secret", "stale-environment-token"} {
		if strings.Contains(string(payload), private) {
			return false
		}
	}
	return true
}
func TestExecutorFileSecretsRotateOnlyIntoSelectedChildEnvironment(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("private file provider requires supported POSIX platform")
	}
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "parent-secret-files")
	if err := os.WriteFile(path, []byte("fixture-first-token"), 0600); err != nil {
		t.Fatal(err)
	}
	provider, err := filesecrets.New(filesecrets.Config{Files: map[string]string{"KELVO_SOURCE_SELECTED_TOKEN": path, "KELVO_SOURCE_OTHER_TOKEN": filepath.Join(dir, "unselected-file-secret")}})
	if err != nil {
		t.Fatal(err)
	}
	defer provider.Close()
	t.Setenv("KELVO_SOURCE_SELECTED_URL", "https://source.example")
	t.Setenv("KELVO_SOURCE_SELECTED_TOKEN", "stale-environment-token")
	t.Setenv("KELVO_SOURCE_OTHER_TOKEN", "must-stay-parent")
	t.Setenv("KELVO_TOKEN", "must-stay-parent")
	config := catalog.Config{Sources: []catalog.Source{
		{ID: "selected", Type: "databricks", URLEnv: "KELVO_SOURCE_SELECTED_URL", TokenEnv: "KELVO_SOURCE_SELECTED_TOKEN"},
		{ID: "other", Type: "databricks", TokenEnv: "KELVO_SOURCE_OTHER_TOKEN"},
	}}
	executor, err := New(config, query.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	var resolved []string
	executor.Secrets = secretResolverFunc(func(ctx context.Context, key string) (string, bool, error) {
		resolved = append(resolved, key)
		return provider.Resolve(ctx, key)
	})
	for _, sql := range []string{"SELECT file_secret_first", "SELECT file_secret_second"} {
		if sql == "SELECT file_secret_second" {
			stage := path + ".new"
			if err := os.WriteFile(stage, []byte("fixture-second-token"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.Rename(stage, path); err != nil {
				t.Fatal(err)
			}
		}
		stats, err := executor.Execute(context.Background(), query.Request{Mode: "native", ConnectionID: "selected", SQL: sql}, &workerTestSink{})
		if err != nil || stats.Rows != 3 {
			t.Fatalf("rotated parent-to-child credentials failed: %v", err)
		}
	}
	expected := []string{"KELVO_SOURCE_SELECTED_URL", "KELVO_SOURCE_SELECTED_TOKEN", "KELVO_SOURCE_SELECTED_URL", "KELVO_SOURCE_SELECTED_TOKEN"}
	if !reflect.DeepEqual(resolved, expected) {
		t.Fatalf("resolved unexpected reference names: %v", resolved)
	}
	// The configured selected file has vanished. A still-present stale parent
	// environment must not be used to make the next subprocess succeed.
	os.Remove(path)
	_, err = executor.Execute(context.Background(), query.Request{Mode: "native", ConnectionID: "selected", SQL: "SELECT file_secret_second"}, &workerTestSink{})
	if err == nil || query.PublicError(err).Code != "CONFIGURATION_ERROR" {
		t.Fatalf("configured secret failure fell back: %v", err)
	}
	if strings.Contains(err.Error(), path) || strings.Contains(err.Error(), "stale-environment-token") {
		t.Fatal("secret error leaked private data")
	}
}
func TestSecretResolverFallbackFailureAndCancellation(t *testing.T) {
	const key = "KELVO_SOURCE_SELECTED_TOKEN"
	t.Setenv(key, "environment-fallback")
	e := &Executor{}
	value, known, err := e.resolveSecret(context.Background(), key)
	if err != nil || !known || value != "environment-fallback" {
		t.Fatal("legacy env fallback changed")
	}
	e.Secrets = secretResolverFunc(func(context.Context, string) (string, bool, error) { return "", false, nil })
	value, known, err = e.resolveSecret(context.Background(), key)
	if err != nil || !known || value != "environment-fallback" {
		t.Fatal("unknown provider key did not fall back")
	}
	e.Secrets = secretResolverFunc(func(context.Context, string) (string, bool, error) { return "", true, nil })
	value, known, err = e.resolveSecret(context.Background(), key)
	if err != nil || !known || value != "" {
		t.Fatal("configured empty secret fell back")
	}
	for _, configured := range []bool{false, true} {
		e.Secrets = secretResolverFunc(func(context.Context, string) (string, bool, error) {
			return "sensitive-value", configured, errors.New("private /secret/path sensitive-value")
		})
		value, _, err = e.resolveSecret(context.Background(), key)
		if err == nil || value != "" || strings.Contains(err.Error(), "sensitive") || strings.Contains(err.Error(), "/secret") {
			t.Fatal("provider error leaked or fell back")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	e.Secrets = nil
	if value, _, err := e.resolveSecret(ctx, key); value != "" || !errors.Is(err, context.Canceled) {
		t.Fatal("canceled resolution returned a secret")
	}
}
