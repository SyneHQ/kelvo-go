// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package catalog

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
)

const strictFingerprintFixture = `{"Format":1,"Tenant":"tenant-a","ID":"orders_fast","Authorization":"grants-v1","Query":{"sql":"SELECT id, total FROM orders","mode":"native","connection_id":"warehouse"},"Sources":[{"id":"warehouse","type":"clickhouse","url_env":"KELVO_SOURCE_TEST_WAREHOUSE_URL"}]}`

func TestSchemaEvolutionStrictFingerprintCompatibility(t *testing.T) {
	// Frozen pre-policy wire representation for acceleratedYAML. Do not add the
	// new field here: existing strict generations must remain readable unchanged.

	sum := sha256.Sum256([]byte(strictFingerprintFixture))
	want := hex.EncodeToString(sum[:])
	for _, policy := range []string{"", "      schema_evolution: {}\n", "      schema_evolution: null\n", "      schema_evolution:\n        add_nullable_columns: false\n        safe_widening: false\n"} {
		c, err := loadAccelerationFixture(t, acceleratedYAML+policy)
		if err != nil {
			t.Fatal(err)
		}
		got, err := c.DatasetFingerprint("orders_fast")
		if err != nil || got != want {
			t.Fatalf("strict fingerprint changed: got %s want %s err=%v", got, want, err)
		}
	}
}

func TestSchemaEvolutionIndependentFlagsAndAuthorization(t *testing.T) {
	seen := map[string]bool{}
	for _, policy := range []struct {
		yaml       string
		add, widen bool
	}{
		{"{}", false, false},
		{"{add_nullable_columns: true}", true, false},
		{"{safe_widening: true}", false, true},
		{"{add_nullable_columns: true, safe_widening: true}", true, true},
	} {
		c, err := loadAccelerationFixture(t, acceleratedYAML+"      schema_evolution: "+policy.yaml+"\n")
		if err != nil {
			t.Fatal(err)
		}
		got := c.Acceleration.Datasets[0].SchemaEvolution
		if got == nil || got.AddNullableColumns != policy.add || got.SafeWidening != policy.widen {
			t.Fatal("independent schema flags lost")
		}
		fp, err := c.DatasetFingerprint("orders_fast")
		if err != nil || seen[fp] {
			t.Fatal("different effective policies share fingerprint", err)
		}
		seen[fp] = true
		c.Acceleration.Datasets[0].AuthorizationVersion = "grants-v2"
		changed, err := c.DatasetFingerprint("orders_fast")
		if err != nil || changed == fp {
			t.Fatal("policy hid authorization change", err)
		}
		c.Acceleration.Datasets[0].AuthorizationVersion = "grants-v1"
		c.Acceleration.Datasets[0].Query.SQL += " WHERE account_id = 7"
		changed, err = c.DatasetFingerprint("orders_fast")
		if err != nil || changed == fp {
			t.Fatal("policy hid source query change", err)
		}
	}
	c, err := loadAccelerationFixture(t, acceleratedYAML+"      schema_evolution: {safe_widening: true}\n")
	if err != nil {
		t.Fatal(err)
	}
	before, _ := c.DatasetFingerprint("orders_fast")
	c.Acceleration.Datasets[0].SchemaEvolution = &SchemaEvolution{}
	after, _ := c.DatasetFingerprint("orders_fast")
	if before == after {
		t.Fatal("tightening schema policy reused widened fingerprint")
	}
}

func TestSchemaEvolutionRejectsInvalidYAML(t *testing.T) {
	for _, value := range []string{"{allow_everything: true}", "{safe_widening: not-a-boolean}", "{add_nullable_columns: 1}", "[true]", "true", "{safe_widening: true, safe_widening: false}"} {
		if _, err := loadAccelerationFixture(t, acceleratedYAML+"      schema_evolution: "+value+"\n"); err == nil {
			t.Fatalf("invalid schema policy accepted: %s", value)
		}
	}
	if _, err := loadAccelerationFixture(t, strings.Replace(acceleratedYAML, "authorization_version: grants-v1", "authorization_version: ''", 1)+"      schema_evolution: {add_nullable_columns: true, safe_widening: true}\n"); err == nil {
		t.Fatal("schema policy bypassed required authorization version")
	}
}

func TestSchemaEvolutionObjectStorageStrictCompatibility(t *testing.T) {
	const storageWire = `{"provider":"s3","endpoint":"https://objects.example.com","bucket":"snapshots","prefix":"kelvo","region":"us-east-1","read_credentials":{"access_key_id_env":"KELVO_SOURCE_READER_ID","secret_access_key_env":"KELVO_SOURCE_READER_SECRET"},"write_credentials":{"access_key_id_env":"KELVO_SOURCE_WRITER_ID","secret_access_key_env":"KELVO_SOURCE_WRITER_SECRET"}}`
	var storage ObjectStorage
	if err := json.Unmarshal([]byte(storageWire), &storage); err != nil {
		t.Fatal(err)
	}
	if err := storage.Validate(); err != nil {
		t.Fatal(err)
	}
	oldWire := strings.TrimSuffix(strictFingerprintFixture, "}") + `,"object_storage":` + storageWire + "}"
	sum := sha256.Sum256([]byte(oldWire))
	want := hex.EncodeToString(sum[:])
	for _, policy := range []*SchemaEvolution{nil, {}} {
		c, err := loadAccelerationFixture(t, acceleratedYAML)
		if err != nil {
			t.Fatal(err)
		}
		c.Acceleration.ObjectStorage = &storage
		c.Acceleration.Datasets[0].SchemaEvolution = policy
		got, err := c.DatasetFingerprint("orders_fast")
		if err != nil || got != want {
			t.Fatalf("remote strict fingerprint changed: got %s want %s err=%v", got, want, err)
		}
	}
}

func TestSchemaEvolutionExplicitFalseAndRulesVersion(t *testing.T) {
	for _, pair := range [][2]string{
		{"{add_nullable_columns: true}", "{add_nullable_columns: true, safe_widening: false}"},
		{"{safe_widening: true}", "{add_nullable_columns: false, safe_widening: true}"},
	} {
		var first string
		for _, policy := range pair {
			c, err := loadAccelerationFixture(t, acceleratedYAML+"      schema_evolution: "+policy+"\n")
			if err != nil {
				t.Fatal(err)
			}
			got, err := c.DatasetFingerprint("orders_fast")
			if err != nil {
				t.Fatal(err)
			}
			if first != "" && got != first {
				t.Fatal("explicit false changed effective policy identity")
			}
			first = got
		}
	}
	c, err := loadAccelerationFixture(t, acceleratedYAML+"      schema_evolution: {safe_widening: true}\n")
	if err != nil {
		t.Fatal(err)
	}
	wire := strings.TrimSuffix(strictFingerprintFixture, "}") + `,"schema_evolution":{"add_nullable_columns":false,"safe_widening":true},"schema_evolution_version":1}`
	sum := sha256.Sum256([]byte(wire))
	want := hex.EncodeToString(sum[:])
	got, err := c.DatasetFingerprint("orders_fast")
	if err != nil || got != want {
		t.Fatalf("nondefault rules identity differs: got %s want %s err=%v", got, want, err)
	}
}
