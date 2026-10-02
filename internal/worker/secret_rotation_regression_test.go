//go:build linux || darwin

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	filesecrets "github.com/SYNEHQ/kelvo-go/internal/secrets"
)

func rotationExecutor(t *testing.T) (*Executor, string) {
	t.Helper()
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "parent-secret-files")
	if err = os.WriteFile(path, []byte("fixture-first-token"), 0600); err != nil {
		t.Fatal(err)
	}
	provider, err := filesecrets.New(filesecrets.Config{Files: map[string]string{"KELVO_SOURCE_SELECTED_TOKEN": path, "KELVO_SOURCE_OTHER_TOKEN": filepath.Join(dir, "unselected-file-secret")}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { provider.Close() })
	t.Setenv("KELVO_SOURCE_SELECTED_URL", "https://source.example")
	t.Setenv("KELVO_SOURCE_SELECTED_TOKEN", "stale-environment-token")
	t.Setenv("KELVO_SOURCE_OTHER_TOKEN", "must-stay-parent")
	t.Setenv("KELVO_TOKEN", "must-stay-parent")
	t.Setenv("KELVO_TENANT_A_NATS_PASSWORD", "must-stay-parent")
	config := catalog.Config{Sources: []catalog.Source{
		{ID: "selected", Type: "databricks", URLEnv: "KELVO_SOURCE_SELECTED_URL", TokenEnv: "KELVO_SOURCE_SELECTED_TOKEN"},
		{ID: "other", Type: "databricks", TokenEnv: "KELVO_SOURCE_OTHER_TOKEN"},
	}}
	executor, err := New(config, query.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	executor.Secrets = provider
	return executor, path
}

func replaceWorkerSecret(t *testing.T, path, value string) {
	t.Helper()
	stage := path + ".replacement"
	if err := os.WriteFile(stage, []byte(value), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(stage, path); err != nil {
		t.Fatal(err)
	}
}

func runRotationWave(t *testing.T, executor *Executor, sql string) {
	t.Helper()
	const count = 8
	start := make(chan struct{})
	failures := make(chan error, count)
	var wg sync.WaitGroup
	for range count {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := executor.Execute(context.Background(), query.Request{Mode: "native", ConnectionID: "selected", SQL: sql}, &workerTestSink{})
			failures <- err
		}()
	}
	close(start)
	wg.Wait()
	close(failures)
	for err := range failures {
		if err != nil {
			t.Fatalf("concurrent rotated child environment rejected: %v", err)
		}
	}
}

// Provider-only concurrency tests cannot detect lost credential isolation or
// provider caching when multiple actual disposable query processes share it.
func TestConcurrentQueriesObserveCredentialRotationBetweenBatches(t *testing.T) {
	executor, path := rotationExecutor(t)
	runRotationWave(t, executor, "SELECT file_secret_first")
	replaceWorkerSecret(t, path, "fixture-second-token")
	runRotationWave(t, executor, "SELECT file_secret_second")
}

func TestUnsafeAtomicCredentialReplacementFailsClosedUntilRepaired(t *testing.T) {
	for _, kind := range []string{"permissions", "symlink", "nul"} {
		t.Run(kind, func(t *testing.T) {
			executor, path := rotationExecutor(t)
			request := query.Request{Mode: "native", ConnectionID: "selected", SQL: "SELECT file_secret_first"}
			if _, err := executor.Execute(context.Background(), request, &workerTestSink{}); err != nil {
				t.Fatal(err)
			}
			stage := path + ".unsafe"
			switch kind {
			case "permissions":
				if err := os.WriteFile(stage, []byte("fixture-second-token"), 0644); err != nil {
					t.Fatal(err)
				}
				if err := os.Chmod(stage, 0644); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				target := path + ".target"
				if err := os.WriteFile(target, []byte("fixture-second-token"), 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, stage); err != nil {
					t.Fatal(err)
				}
			case "nul":
				if err := os.WriteFile(stage, []byte("fixture-second-token\x00"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.Rename(stage, path); err != nil {
				t.Fatal(err)
			}
			// The previous credential and stale environment both still look usable;
			// neither may rescue an invalid configured replacement.
			_, err := executor.Execute(context.Background(), request, &workerTestSink{})
			if err == nil || query.PublicError(err).Code != "CONFIGURATION_ERROR" {
				t.Fatalf("invalid replacement did not fail closed: %v", err)
			}
			for _, private := range []string{path, "fixture-first-token", "fixture-second-token", "stale-environment-token"} {
				if strings.Contains(err.Error(), private) {
					t.Fatal("credential diagnostic leaked private data")
				}
			}
			replaceWorkerSecret(t, path, "fixture-second-token")
			request.SQL = "SELECT file_secret_second"
			if _, err = executor.Execute(context.Background(), request, &workerTestSink{}); err != nil {
				t.Fatalf("valid replacement did not recover subsequent query: %v", err)
			}
		})
	}
}

func TestForbiddenCredentialReferencesNeverReachProvider(t *testing.T) {
	for _, key := range []string{"LD_PRELOAD", "PATH", "AWS_SECRET_ACCESS_KEY", "KELVO_TOKEN", "KELVO_TENANT_A_NATS_PASSWORD"} {
		t.Run(key, func(t *testing.T) {
			calls := 0
			executor := &Executor{Binary: "/does/not/exist", Limits: query.DefaultLimits(), Config: catalog.Config{Sources: []catalog.Source{{ID: "selected", Type: "databricks", TokenEnv: key}}}}
			executor.Secrets = secretResolverFunc(func(context.Context, string) (string, bool, error) {
				calls++
				return "provider-value-must-not-be-read", true, nil
			})
			_, err := executor.Execute(context.Background(), query.Request{Mode: "native", ConnectionID: "selected", SQL: "SELECT file_secret_first"}, &workerTestSink{})
			if err == nil || query.PublicError(err).Code != "CONFIGURATION_ERROR" || calls != 0 {
				t.Fatalf("forbidden reference reached provider or child: calls=%d err=%v", calls, err)
			}
		})
	}
}
