package catalog

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDeploymentSourcesCatalogLoadsSupportedConnectors(t *testing.T) {
	path := filepath.Join("..", "..", "deploy", "examples", "sources.yml")
	config, err := Load(path)
	if err != nil {
		t.Fatalf("deployment sources catalog rejected: %v", err)
	}
	want := map[string]struct {
		typ, dsn, url, token string
	}{
		"databricks": {"databricks", "", "KELVO_SOURCE_DATABRICKS_URL", "KELVO_SOURCE_DATABRICKS_TOKEN"},
		"snowflake":  {"snowflake", "", "KELVO_SOURCE_SNOWFLAKE_URL", "KELVO_SOURCE_SNOWFLAKE_TOKEN"},
		"mongodb":    {"mongodb", "KELVO_SOURCE_MONGODB_DSN", "", ""},
		"d1":         {"d1", "", "KELVO_SOURCE_D1_URL", "KELVO_SOURCE_D1_TOKEN"},
		"sqlserver":  {"sqlserver", "KELVO_SOURCE_SQLSERVER_DSN", "", ""},
		"oracle":     {"oracle", "KELVO_SOURCE_ORACLE_DSN", "", ""},
	}
	if len(config.Sources) != len(want) {
		t.Fatalf("sources=%d want=%d", len(config.Sources), len(want))
	}
	for _, source := range config.Sources {
		expect, ok := want[source.ID]
		if !ok {
			t.Fatalf("unexpected source: %#v", source)
		}
		if source.Type != expect.typ || source.DSNEnv != expect.dsn || source.URLEnv != expect.url || source.TokenEnv != expect.token {
			t.Fatalf("source %q=%#v, want type=%q dsn=%q url=%q token=%q", source.ID, source, expect.typ, expect.dsn, expect.url, expect.token)
		}
	}
}

func TestSQLServerAliasesNormalize(t *testing.T) {
	for _, typ := range []string{"mssql", "msql"} {
		t.Run(typ, func(t *testing.T) {
			config := loadCatalog(t, "sources:\n  - id: db\n    type: "+typ+"\n    dsn_env: KELVO_SOURCE_SQLSERVER_DSN\n")
			if got := config.Sources[0].Type; got != "sqlserver" {
				t.Fatalf("alias %q normalized to %q", typ, got)
			}
		})
	}
}

func TestCatalogRejectsUnsafeTokenAndIncompleteNativeCloudSources(t *testing.T) {
	t.Run("unsafe token environment", func(t *testing.T) {
		if _, err := loadCatalogError(t, "sources:\n  - id: cloud\n    type: databricks\n    url_env: KELVO_SOURCE_DATABRICKS_URL\n    token_env: PATH\n"); err == nil {
			t.Fatal("unsafe token environment accepted")
		}
	})
	for _, typ := range []string{"databricks", "snowflake", "d1"} {
		t.Run(typ+" requires url and token", func(t *testing.T) {
			for _, input := range []string{
				"sources:\n  - id: cloud\n    type: " + typ + "\n    token_env: KELVO_SOURCE_TOKEN\n",
				"sources:\n  - id: cloud\n    type: " + typ + "\n    url_env: KELVO_SOURCE_URL\n",
			} {
				if _, err := loadCatalogError(t, input); err == nil {
					t.Fatalf("incomplete %s source accepted", typ)
				}
			}
		})
	}
	if _, err := loadCatalogError(t, "sources:\n  - id: mongo\n    type: mongodb\n"); err == nil {
		t.Fatal("MongoDB source without dsn_env accepted")
	}
}

func loadCatalog(t *testing.T, content string) Config {
	t.Helper()
	config, err := loadCatalogError(t, content)
	if err != nil {
		t.Fatal(err)
	}
	return config
}

func loadCatalogError(t *testing.T, content string) (Config, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "sources.yml")
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	return Load(path)
}
