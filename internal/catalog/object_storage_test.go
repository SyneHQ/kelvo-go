// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package catalog

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestObjectStorageProviderLocationsAndCredentialIsolation(t *testing.T) {
	for _, provider := range []string{"s3", "r2", "gcs", "azure"} {
		s := ObjectStorage{ObjectLocation: ObjectLocation{Provider: provider, Endpoint: "https://objects.example.com", Bucket: "snapshots", Prefix: "kelvo/production"},
			ReadCredentials:  ObjectCredentials{AccessKeyIDEnv: "KELVO_SOURCE_READER_ID", SecretAccessKeyEnv: "KELVO_SOURCE_READER_SECRET"},
			WriteCredentials: ObjectCredentials{AccessKeyIDEnv: "KELVO_SOURCE_WRITER_ID", SecretAccessKeyEnv: "KELVO_SOURCE_WRITER_SECRET"}}
		if provider == "s3" {
			s.Region = "us-east-1"
		}
		if provider == "azure" {
			s.Account = "kelvoaccount"
			s.ReadCredentials = ObjectCredentials{SASTokenEnv: "KELVO_SOURCE_READER_SAS"}
			s.WriteCredentials = ObjectCredentials{SASTokenEnv: "KELVO_SOURCE_WRITER_SAS"}
		}
		if err := s.Validate(); err != nil {
			t.Fatal(provider, err)
		}
		key := "kelvo/production/tenant/orders/0123456789abcdef0123456789abcdef.parquet"
		r, err := s.Read(key)
		if err != nil {
			t.Fatal(err)
		}
		uri, err := s.URI(key)
		if err != nil || uri != r.URI() {
			t.Fatal("canonical object URI changed", provider, err)
		}
		encoded, _ := json.Marshal(r)
		if strings.Contains(string(encoded), "WRITER") || strings.Contains(string(encoded), "write_credentials") {
			t.Fatal("reader descriptor contains publisher references")
		}
		if _, err = s.Read("kelvo/production-other/tenant/data.parquet"); err == nil {
			t.Fatal("prefix sibling accepted")
		}
		s.WriteCredentials = s.ReadCredentials
		if s.Validate() == nil {
			t.Fatal("shared read/write identity references accepted")
		}
	}
}

func TestObjectStorageRejectsUnsafeMetadata(t *testing.T) {
	base := ObjectLocation{Provider: "s3", Endpoint: "https://objects.example.com", Bucket: "snapshots", Prefix: "kelvo", Region: "us-east-1"}
	for _, endpoint := range []string{"http://objects.example.com", "https://user:secret@objects.example.com", "https://objects.example.com/path", "https://objects.example.com?token=private", "https://objects.example.com#fragment"} {
		l := base
		l.Endpoint = endpoint
		if l.Validate() == nil {
			t.Fatal("unsafe endpoint accepted")
		}
	}
	for _, key := range []string{"", "../other", "kelvo/../other", "kelvo//other", "kelvo/object?token=x", "kelvo/*", "kelvo/%2e%2e/file", "/kelvo/object", "kelvo/a\\b"} {
		if ValidateObjectKey(key) == nil {
			t.Fatal("unsafe key accepted", key)
		}
	}
	if (ObjectCredentials{AccessKeyIDEnv: "AWS_ACCESS_KEY_ID", SecretAccessKeyEnv: "AWS_SECRET_ACCESS_KEY"}).Validate("s3") == nil {
		t.Fatal("ambient cloud credentials accepted")
	}
	if (ObjectCredentials{AccessKeyIDEnv: "KELVO_SOURCE_ID", SecretAccessKeyEnv: "KELVO_SOURCE_SECRET", SessionTokenEnv: "KELVO_SOURCE_TOKEN"}).Validate("gcs") == nil {
		t.Fatal("GCS session token accepted")
	}
}

func TestAzureReaderSASIsReadOnlyAndCannotChangeEndpoint(t *testing.T) {
	valid := "sv=2023-11-03&sp=r&se=2030-01-01T00%3A00%3A00Z&spr=https&sr=c&sig=fixture%2Bsignature%3D"
	if err := ValidateAzureReadSAS(valid); err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{
		strings.Replace(valid, "sp=r", "sp=rw", 1),
		valid + "&sp=r",
		valid + "&BlobEndpoint=https%3A%2F%2Fother.example.com",
		strings.Replace(valid, "spr=https", "spr=http", 1),
		strings.Replace(valid, "sig=fixture%2Bsignature%3D", "sig=fixture%0d%0a", 1),
	} {
		if ValidateAzureReadSAS(value) == nil {
			t.Fatal("unsafe reader SAS accepted")
		}
	}
}

func TestYAMLObjectStorageOptIn(t *testing.T) {
	addition := `  object_storage:
    provider: s3
    endpoint: https://s3.us-east-1.amazonaws.com
    bucket: snapshots
    prefix: kelvo
    region: us-east-1
    read_credentials:
      access_key_id_env: KELVO_SOURCE_READER_ID
      secret_access_key_env: KELVO_SOURCE_READER_SECRET
    write_credentials:
      access_key_id_env: KELVO_SOURCE_WRITER_ID
      secret_access_key_env: KELVO_SOURCE_WRITER_SECRET
`
	c, err := loadAccelerationFixture(t, strings.Replace(acceleratedYAML, "  datasets:\n", addition+"  datasets:\n", 1))
	if err != nil || c.Acceleration.ObjectStorage == nil {
		t.Fatal("object opt-in failed", err)
	}
	if _, err := loadAccelerationFixture(t, strings.Replace(acceleratedYAML, "    type: clickhouse", "    object: {}\n    type: clickhouse", 1)); err == nil {
		t.Fatal("source YAML can inject resolved object descriptors")
	}
}
