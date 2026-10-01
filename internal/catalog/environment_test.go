package catalog

import "testing"

func TestValidateSourceEnvironmentNamespace(t *testing.T) {
	for _, name := range []string{"KELVO_SOURCE_ANALYTICS_DSN", "KELVO_SOURCE_TEAM_2_PASSWORD", "KELVO_POSTGRES_DSN", "KELVO_PG_DSN", "KELVO_MYSQL_DSN", "KELVO_CLICKHOUSE_URL", "KELVO_CLICKHOUSE_USER", "KELVO_CLICKHOUSE_PASSWORD", "KELVO_CH_URL"} {
		if err := ValidateEnvironment(name); err != nil {
			t.Errorf("safe name %q rejected: %v", name, err)
		}
	}
	for _, name := range []string{"", "KELVO_SOURCE_", "LD_PRELOAD", "DYLD_INSERT_LIBRARIES", "PATH", "HOME", "TMPDIR", "GODEBUG", "PGPASSWORD", "MYSQL_PWD", "AWS_SECRET_ACCESS_KEY", "KELVO_BROKER_PASSWORD", "KELVO_API_TOKEN", "KELVO_SOURCE_bad", "KELVO_SOURCE_A=x", "KELVO_SOURCE_A\nLD_PRELOAD"} {
		if err := ValidateEnvironment(name); err == nil {
			t.Errorf("capability or invalid name %q accepted", name)
		}
	}
}
