// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package catalog

import (
	"encoding/json"
	"math"
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/query"
)

func verificationCatalogFixture() Config {
	c := authorityFixture()
	c.Acceleration.ObjectStorage.ReaderRegistry = &ObjectReaderRegistry{Credentials: ObjectCredentials{
		AccessKeyIDEnv: "KELVO_SOURCE_REGISTRY_ID", SecretAccessKeyEnv: "KELVO_SOURCE_REGISTRY_SECRET"}}
	return c
}

func TestVerificationLimitsRequireExplicitAggregateAllowance(t *testing.T) {
	for _, value := range []*VerificationLimits{nil, {}, {MaxBytes: -1}, {MaxBytes: 1<<45 + 1}, {MaxBytes: math.MaxInt64}} {
		_, err := (Dataset{Verification: value}).EffectiveVerificationLimits()
		if err == nil {
			t.Fatalf("invalid or omitted verification allowance accepted: %+v", value)
		}
		public := query.PublicError(err)
		if public.Code != "CONFIGURATION_ERROR" || !strings.Contains(public.Message, "verification") || !strings.Contains(public.Message, "max_bytes") {
			t.Fatal("verification refusal is not actionable", public)
		}
	}
	for _, value := range []int64{1, 1 << 30, 1 << 45} {
		input := &VerificationLimits{MaxBytes: value}
		got, err := (Dataset{Verification: input}).EffectiveVerificationLimits()
		if err != nil || got.MaxBytes != value {
			t.Fatal("valid allowance changed", value, err)
		}
		input.MaxBytes++
		if got.MaxBytes != value {
			t.Fatal("effective policy aliases the caller")
		}
	}
}

func TestVerificationYAMLRequiresProtectedStorageAndPositiveBudget(t *testing.T) {
	storage := `  object_storage:
    provider: s3
    endpoint: https://storage.example.invalid
    bucket: fixture-bucket
    prefix: snapshots
    region: us-east-1
    read_credentials:
      access_key_id_env: KELVO_SOURCE_READ_ID
      secret_access_key_env: KELVO_SOURCE_READ_SECRET
    write_credentials:
      access_key_id_env: KELVO_SOURCE_WRITE_ID
      secret_access_key_env: KELVO_SOURCE_WRITE_SECRET
    reader_registry:
      credentials:
        access_key_id_env: KELVO_SOURCE_REGISTRY_ID
        secret_access_key_env: KELVO_SOURCE_REGISTRY_SECRET
`
	base := strings.Replace(acceleratedYAML, "  datasets:\n", storage+"  datasets:\n", 1)
	for _, suffix := range []string{"", "      verification: null\n", "      verification:\n        max_bytes: 1\n", "      verification:\n        max_bytes: 35184372088832\n"} {
		config, err := loadAccelerationFixture(t, base+suffix)
		if err != nil {
			t.Fatal("valid verification YAML refused", err)
		}
		if suffix == "" && config.Acceleration.Datasets[0].Verification != nil {
			t.Fatal("omission enabled verification")
		}
	}
	for _, suffix := range []string{"      verification: {}\n", "      verification:\n        max_bytes: 0\n", "      verification:\n        max_bytes: -1\n", "      verification:\n        max_bytes: 35184372088833\n", "      verification:\n        max_bytes: 10\n        unsupported: true\n"} {
		if _, err := loadAccelerationFixture(t, base+suffix); err == nil {
			t.Fatal("invalid verification YAML accepted", suffix)
		}
	}
	if _, err := loadAccelerationFixture(t, acceleratedYAML+"      verification:\n        max_bytes: 10\n"); err == nil {
		t.Fatal("local backend silently ignored verification policy")
	}
}

func TestVerificationAuthorityBindsDetachedBudgetWithoutChangingContent(t *testing.T) {
	c := verificationCatalogFixture()
	beforeContent, err := c.DatasetFingerprint("recent")
	if err != nil {
		t.Fatal(err)
	}
	_, beforeAuthority, err := AuthoritySnapshot(c)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(c.Acceleration.Datasets[0])
	if err != nil || strings.Contains(string(raw), "verification") {
		t.Fatal("omission changed existing authority encoding", err)
	}
	c.Acceleration.Datasets[0].Verification = &VerificationLimits{MaxBytes: 1 << 30}
	copy, authority, err := AuthoritySnapshot(c)
	if err != nil || authority == beforeAuthority {
		t.Fatal("verification opt-in did not change immutable authority", err)
	}
	afterContent, err := c.DatasetFingerprint("recent")
	if err != nil || beforeContent != afterContent {
		t.Fatal("operational budget changed snapshot identity", err)
	}
	c.Acceleration.Datasets[0].Verification.MaxBytes++
	changed, err := AuthorityFingerprint(c)
	if err != nil || changed == authority || copy.Acceleration.Datasets[0].Verification.MaxBytes != 1<<30 {
		t.Fatal("caller mutation escaped catalog binding", err)
	}
	if frozen, err := AuthorityFingerprint(copy); err != nil || frozen != authority {
		t.Fatal("detached verification policy changed", err)
	}
}

func TestVerificationProgrammaticAuthorityRejectsInvalidPolicy(t *testing.T) {
	for _, kind := range []string{"zero", "negative", "oversized", "local", "legacy"} {
		t.Run(kind, func(t *testing.T) {
			c := verificationCatalogFixture()
			c.Acceleration.Datasets[0].Verification = &VerificationLimits{MaxBytes: 10}
			switch kind {
			case "zero":
				c.Acceleration.Datasets[0].Verification.MaxBytes = 0
			case "negative":
				c.Acceleration.Datasets[0].Verification.MaxBytes = -1
			case "oversized":
				c.Acceleration.Datasets[0].Verification.MaxBytes = math.MaxInt64
			case "local":
				c.Acceleration.ObjectStorage = nil
			case "legacy":
				c.Acceleration.ObjectStorage.ReaderRegistry = nil
			}
			if _, _, err := AuthoritySnapshot(c); err == nil {
				t.Fatal("invalid programmatic policy entered the runtime")
			}
		})
	}
}
