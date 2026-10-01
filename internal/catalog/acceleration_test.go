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
