// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"go.yaml.in/yaml/v3"
)

func TestStandaloneProtectedSnapshotsRefuseBeforeOutputOrSourceAccess(t *testing.T) {
	root := t.TempDir()
	config := catalog.Config{Sources: []catalog.Source{{ID: "source", Type: "csv", Path: filepath.Join(root, "source.csv")}}, Acceleration: &catalog.AccelerationConfig{
		TenantID: "tenant-a", Directory: filepath.Join(root, "snapshots"),
		Datasets: []catalog.Dataset{{ID: "snapshot", Query: query.Request{Mode: "federated", Sources: []string{"source"}, SQL: "SELECT * FROM source"}, MaxAge: time.Hour, AuthorizationVersion: "v1", Limits: query.DefaultLimits()}},
		ObjectStorage: &catalog.ObjectStorage{ObjectLocation: catalog.ObjectLocation{Provider: "s3", Endpoint: "https://fixture.invalid", Bucket: "fixtures", Prefix: "cache", Region: "us-east-1"},
			ReadCredentials:  catalog.ObjectCredentials{AccessKeyIDEnv: "KELVO_SOURCE_READ_ID", SecretAccessKeyEnv: "KELVO_SOURCE_READ_SECRET"},
			WriteCredentials: catalog.ObjectCredentials{AccessKeyIDEnv: "KELVO_SOURCE_WRITE_ID", SecretAccessKeyEnv: "KELVO_SOURCE_WRITE_SECRET"},
			ReaderRegistry:   &catalog.ObjectReaderRegistry{Credentials: catalog.ObjectCredentials{AccessKeyIDEnv: "KELVO_SOURCE_REGISTRY_ID", SecretAccessKeyEnv: "KELVO_SOURCE_REGISTRY_SECRET"}}},
	}}
	if err := os.WriteFile(config.Sources[0].Path, []byte("id\n1\n"), 0600); err != nil {
		t.Fatal(err)
	}
	raw, err := yaml.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(root, "kelvo.yml")
	if err := os.WriteFile(file, raw, 0600); err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(root, "result.arrow")
	for _, mode := range []string{"query", "serve"} {
		args := []string{mode, "--config", file}
		if mode == "query" {
			args = append(args, "--sources", "snapshot", "--sql", "SELECT * FROM snapshot", "--out", output)
		}
		err := run(args)
		public := query.PublicError(err)
		if err == nil || public.Code != "CONFIGURATION_ERROR" || public.Message != "Protected object snapshots require the contained cluster node entrypoint" {
			t.Fatal("standalone mode did not refuse the protected configuration before external access", mode, err)
		}
	}
	if _, err := os.Stat(output); !os.IsNotExist(err) {
		t.Fatal("refused query created an output file", err)
	}
}
