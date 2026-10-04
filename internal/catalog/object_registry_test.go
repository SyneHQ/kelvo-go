// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package catalog

import (
	"encoding/json"
	"strings"
	"testing"

	"go.yaml.in/yaml/v3"
)

func TestObjectReaderRegistryRequiresDedicatedReferences(t *testing.T) {
	for _, provider := range []string{"s3", "r2", "gcs", "azure"} {
		t.Run(provider, func(t *testing.T) {
			storage := *authorityFixture().Acceleration.ObjectStorage
			storage.Provider = provider
			storage.ReaderRegistry = &ObjectReaderRegistry{Credentials: ObjectCredentials{
				AccessKeyIDEnv: "KELVO_SOURCE_REGISTRY_ID", SecretAccessKeyEnv: "KELVO_SOURCE_REGISTRY_SECRET"}}
			if provider != "s3" {
				storage.Region = ""
			}
			if provider == "azure" {
				storage.Account = "fixtureaccount"
				storage.ReadCredentials = ObjectCredentials{SASTokenEnv: "KELVO_SOURCE_READ_SAS"}
				storage.WriteCredentials = ObjectCredentials{SASTokenEnv: "KELVO_SOURCE_WRITE_SAS"}
				storage.ReaderRegistry.Credentials = ObjectCredentials{SASTokenEnv: "KELVO_SOURCE_REGISTRY_SAS"}
			}
			if err := storage.Validate(); err != nil {
				t.Fatal(err)
			}
			for _, shared := range []ObjectCredentials{storage.ReadCredentials, storage.WriteCredentials, {}} {
				bad := storage
				bad.ReaderRegistry = &ObjectReaderRegistry{Credentials: shared}
				if bad.Validate() == nil {
					t.Fatal("shared or missing registry identity accepted")
				}
			}
			read, err := storage.Read(storage.Prefix + "/tenant/events/generation.parquet")
			if err != nil {
				t.Fatal(err)
			}
			raw, _ := json.Marshal(read)
			if strings.Contains(string(raw), "REGISTRY") || strings.Contains(string(raw), "reader_registry") {
				t.Fatal("data descriptor leaked registry references")
			}
			raw, err = yaml.Marshal(storage)
			if err != nil {
				t.Fatal(err)
			}
			var roundtrip ObjectStorage
			if err := yaml.Unmarshal(raw, &roundtrip); err != nil || roundtrip.ReaderRegistry == nil || roundtrip.Validate() != nil {
				t.Fatal("protected YAML option did not round trip", err)
			}
		})
	}
}

func TestAuthorityDetachesAndBindsObjectRegistryReferences(t *testing.T) {
	config := authorityFixture()
	config.Acceleration.ObjectStorage.ReaderRegistry = &ObjectReaderRegistry{Credentials: ObjectCredentials{
		AccessKeyIDEnv: "KELVO_SOURCE_REGISTRY_ID", SecretAccessKeyEnv: "KELVO_SOURCE_REGISTRY_SECRET"}}
	snapshot, digest, err := AuthoritySnapshot(config)
	if err != nil {
		t.Fatal(err)
	}
	config.Acceleration.ObjectStorage.ReaderRegistry.Credentials.AccessKeyIDEnv = "KELVO_SOURCE_OTHER_REGISTRY_ID"
	if snapshot.Acceleration.ObjectStorage.ReaderRegistry.Credentials.AccessKeyIDEnv != "KELVO_SOURCE_REGISTRY_ID" {
		t.Fatal("registry definition aliases mutable caller configuration")
	}
	changed, err := AuthorityFingerprint(config)
	if err != nil || changed == digest {
		t.Fatal("registry replacement kept authority", err)
	}
	config.Acceleration.ObjectStorage.ReaderRegistry.Credentials.AccessKeyIDEnv = strings.Repeat("A", 300)
	if _, _, err := AuthoritySnapshot(config); err == nil {
		t.Fatal("unbounded registry reference accepted")
	}
}
