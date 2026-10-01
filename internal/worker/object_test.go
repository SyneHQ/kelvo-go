// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

func workerObjectFixture() catalog.Source {
	rangeRead := &catalog.ObjectRange{URL: "http://127.0.0.1:32123/" + strings.Repeat("a", 64) + "/selected", Bytes: 2686290}
	return catalog.Source{ID: "selected", Type: "parquet", Path: rangeRead.URL, Range: rangeRead}
}

var objectCloudEnvironment = []string{"KELVO_SOURCE_OBJECT_READ_ID", "KELVO_SOURCE_OBJECT_READ_SECRET", "KELVO_SOURCE_OBJECT_READ_TOKEN", "KELVO_SOURCE_OBJECT_READ_SAS", "KELVO_SOURCE_OBJECT_WRITE_ID", "KELVO_SOURCE_OBJECT_WRITE_SECRET", "KELVO_SOURCE_OBJECT_WRITE_SAS", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_PROFILE", "GOOGLE_APPLICATION_CREDENTIALS", "AZURE_STORAGE_CONNECTION_STRING", "CURL_CA_INFO"}

func objectWorkerEnvironmentMatches(in Input) bool {
	if in.Config.Acceleration != nil || len(in.Config.Sources) != 1 {
		return false
	}
	source := in.Config.Sources[0]
	if source.Object != nil || source.Range == nil || source.Range.Validate() != nil || source.ID != "selected" || source.Path != source.Range.URL {
		return false
	}
	names, err := sourceEnvironmentNames(source)
	if err != nil || len(names) != 0 {
		return false
	}
	payload, err := json.Marshal(in)
	if err != nil {
		return false
	}
	for _, name := range objectCloudEnvironment {
		if _, present := os.LookupEnv(name); present || strings.Contains(string(payload), name) {
			return false
		}
	}
	return !strings.Contains(string(payload), "fixture-cloud-credential")
}

func TestExecutorObjectRangesContainNoCloudCredentials(t *testing.T) {
	for _, name := range objectCloudEnvironment {
		t.Setenv(name, "fixture-cloud-credential")
	}
	executor, err := New(catalog.Config{Sources: []catalog.Source{workerObjectFixture()}}, query.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	stats, err := executor.Execute(context.Background(), query.Request{SQL: "SELECT object_environment", Sources: []string{"selected"}}, &workerTestSink{})
	if err != nil || stats.Rows != 3 {
		t.Fatalf("object range environment or input contained cloud credentials: %v", err)
	}
}

func TestExecutorRejectsObjectCredentialsBeforeLaunch(t *testing.T) {
	for _, mutate := range []func(*catalog.Source){
		func(s *catalog.Source) { s.TokenEnv = "KELVO_SOURCE_OBJECT_READ_SECRET" },
		func(s *catalog.Source) { s.Object = &catalog.ObjectRead{Provider: "s3"} },
		func(s *catalog.Source) { s.Range.URL = "https://storage.example.test/key"; s.Path = s.Range.URL },
	} {
		source := workerObjectFixture()
		mutate(&source)
		executor := &Executor{Binary: "/does/not/exist", Limits: query.DefaultLimits(), Config: catalog.Config{Sources: []catalog.Source{source}}}
		_, err := executor.Execute(context.Background(), query.Request{SQL: "SELECT object_environment", Sources: []string{"selected"}}, &workerTestSink{})
		if err == nil || query.PublicError(err).Code != "CONFIGURATION_ERROR" {
			t.Fatalf("unsafe range input reached launch: %v", err)
		}
	}
}
