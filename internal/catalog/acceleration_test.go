// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package catalog

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const acceleratedYAML = `sources:
  - id: warehouse
    type: clickhouse
    url_env: KELVO_SOURCE_TEST_WAREHOUSE_URL
acceleration:
  directory: snapshots
  tenant_id: tenant-a
  datasets:
    - id: orders_fast
      query:
        mode: native
        connection_id: warehouse
        sql: SELECT id, total FROM orders
      refresh_interval: 30s
      max_age: 5m
      authorization_version: grants-v1
`

func loadAccelerationFixture(t *testing.T, content string) (Config, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "kelvo.yml")
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	return Load(path)
}

func TestAccelerationConfigAndFingerprint(t *testing.T) {
	c, err := loadAccelerationFixture(t, acceleratedYAML)
	if err != nil {
		t.Fatal(err)
	}
	if !filepath.IsAbs(c.Acceleration.Directory) {
		t.Fatal("relative snapshot directory")
	}
	d, ok := c.Dataset("orders_fast")
	if !ok || d.Query.ConnectionID != "warehouse" || d.Limits.Validate() != nil {
		t.Fatal("refresh definition/defaults lost")
	}
	selected, err := c.Select([]string{"orders_fast"})
	if err != nil || len(selected) != 1 || selected[0].Type != "accelerated" || selected[0].URLEnv != "" {
		t.Fatal("virtual dataset exposes source credentials")
	}
	original, err := c.DatasetFingerprint(d.ID)
	if err != nil || len(original) != 64 {
		t.Fatal("invalid fingerprint", err)
	}
	c.Acceleration.Datasets[0].AuthorizationVersion = "grants-v2"
	changed, _ := c.DatasetFingerprint(d.ID)
	if original == changed {
		t.Fatal("grant changes reuse snapshot")
	}
	c.Acceleration.Datasets[0].AuthorizationVersion = "grants-v1"
	c.Sources[0].URLEnv = "OTHER_WAREHOUSE_URL"
	changed, _ = c.DatasetFingerprint(d.ID)
	if original == changed {
		t.Fatal("source changes reuse snapshot")
	}
}

func TestAccelerationRejectsInvalidDefinitions(t *testing.T) {
	for name, content := range map[string]string{
		"tenant traversal":      strings.Replace(acceleratedYAML, "tenant-a", "../tenant-b", 1),
		"duplicate source":      strings.Replace(acceleratedYAML, "id: orders_fast", "id: warehouse", 1),
		"unknown input":         strings.Replace(acceleratedYAML, "connection_id: warehouse", "connection_id: missing", 1),
		"recursive input":       strings.Replace(acceleratedYAML, "connection_id: warehouse", "connection_id: orders_fast", 1),
		"missing authorization": strings.Replace(acceleratedYAML, "authorization_version: grants-v1", "authorization_version: ''", 1),
		"bad interval":          strings.Replace(acceleratedYAML, "refresh_interval: 30s", "refresh_interval: 1s", 1),
		"bad on_unavailable":    strings.Replace(acceleratedYAML, "max_age: 5m", "max_age: 5m\n      on_unavailable: live", 1),
		"no freshness":          strings.Replace(acceleratedYAML, "max_age: 5m", "max_age: 0s", 1),
		"unknown setting":       acceleratedYAML + "      unsupported: true\n",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := loadAccelerationFixture(t, content); err == nil {
				t.Fatal("accepted invalid acceleration definition")
			}
		})
	}
}

func TestAccelerationMongoPipelineConfiguration(t *testing.T) {
	input := strings.ReplaceAll(acceleratedYAML, "type: clickhouse\n    url_env: KELVO_SOURCE_TEST_WAREHOUSE_URL", "type: mongodb\n    dsn_env: KELVO_SOURCE_MONGO_DSN\n    options:\n      database: analytics")
	input = strings.Replace(input, "sql: SELECT id, total FROM orders", `mongo:
          collection: orders
          pipeline:
            - $match:
                account_id: 9223372036854775807
            - $group:
                _id: "$region"
                total:
                  $sum: "$amount"`, 1)
	c, err := loadAccelerationFixture(t, input)
	if err != nil {
		t.Fatal(err)
	}
	d, _ := c.Dataset("orders_fast")
	if d.Query.Mongo == nil || len(d.Query.Mongo.Pipeline) != 2 {
		t.Fatal("pipeline was not retained")
	}
	original, _ := c.DatasetFingerprint(d.ID)
	c.Acceleration.Datasets[0].Query.Mongo.Collection = "other_orders"
	changed, _ := c.DatasetFingerprint(d.ID)
	if original == changed {
		t.Fatal("pipeline collection change reused fingerprint")
	}
	bad := strings.Replace(input, "type: mongodb", "type: oracle", 1)
	if _, err = loadAccelerationFixture(t, bad); err == nil {
		t.Fatal("pipeline accepted for a non-MongoDB source")
	}
}

func TestAccelerationRouteValidation(t *testing.T) {
	route := func(r string) string { return acceleratedYAML + "      route:\n" + r }
	if c, err := loadAccelerationFixture(t, route("        time_column: created_at\n        live_source: warehouse\n")); err != nil || c.Acceleration.Datasets[0].Route.LiveSource != "warehouse" {
		t.Fatal("valid route rejected", err)
	}
	for _, bad := range []string{
		"        time_column: created_at\n",
		"        live_source: warehouse\n",
		"        time_column: created_at\n        live_source: missing\n",
		"        time_column: created_at\n        live_source: orders_fast\n",
		"        time_column: 'a b'\n        live_source: warehouse\n",
	} {
		if _, err := loadAccelerationFixture(t, route(bad)); err == nil {
			t.Fatalf("accepted route %q", bad)
		}
	}
}

func TestAccelerationIncrementalValidation(t *testing.T) {
	inc := func(r string) string { return acceleratedYAML + "      incremental:\n" + r }
	route := "      route:\n        time_column: created_at\n        live_source: warehouse\n"
	if c, err := loadAccelerationFixture(t, inc("        column: created_at\n        key: [id]\n        deletes: soft\n        full_every: 24h\n")); err != nil || c.Acceleration.Datasets[0].CoverageColumn() != "created_at" {
		t.Fatal("valid incremental rejected", err)
	}
	if _, err := loadAccelerationFixture(t, inc("        column: created_at\n        key: [id]\n        deletes: soft\n        full_every: 24h\n")+route); err != nil {
		t.Fatal("matching route rejected", err)
	}
	if _, err := loadAccelerationFixture(t, inc("        column: other\n        key: [id]\n        deletes: soft\n        full_every: 24h\n")+route); err == nil {
		t.Fatal("column differing from route time_column accepted")
	}
	for _, bad := range []string{"        column: created_at\n", "        full_every: 1h\n", "        column: 'a b'\n        full_every: 1h\n",
		// Fail closed: no key, hard deletes, unknown delete policy, late window not shorter than full_every.
		"        column: created_at\n        deletes: soft\n        full_every: 1h\n",
		"        column: created_at\n        key: [id]\n        deletes: hard\n        full_every: 1h\n",
		"        column: created_at\n        key: [id]\n        full_every: 1h\n",
		"        column: created_at\n        key: ['a b']\n        deletes: soft\n        full_every: 1h\n",
		"        column: created_at\n        key: [id]\n        deletes: soft\n        late_window: 1h\n        full_every: 1h\n"} {
		if _, err := loadAccelerationFixture(t, inc(bad)); err == nil {
			t.Fatalf("accepted incremental %q", bad)
		}
	}
}

func TestAccelerationIncrementalMigrationErrorNamesDatasetAndFix(t *testing.T) {
	_, err := loadAccelerationFixture(t, acceleratedYAML+"      incremental:\n        column: created_at\n        full_every: 1h\n")
	if err == nil || !strings.Contains(err.Error(), "key: [") || !strings.Contains(err.Error(), "dataset ") {
		t.Fatalf("migration error %v", err)
	}
	_, err = loadAccelerationFixture(t, acceleratedYAML+"      incremental:\n        column: created_at\n        key: [id]\n        full_every: 1h\n")
	if err == nil || !strings.Contains(err.Error(), "deletes: soft") {
		t.Fatalf("migration error %v", err)
	}
}
