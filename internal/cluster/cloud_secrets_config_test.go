// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const cloudSecretsYAML = `secrets:
  ttl: 30s
  timeout: 5s
  files: {KELVO_SOURCE_LOCAL_PASSWORD: secrets/local.txt}
  providers:
    aws-main:
      type: aws_secrets_manager
      region: us-east-1
      credentials_file: secrets/provider.yml
  references:
    KELVO_SOURCE_WAREHOUSE_PASSWORD:
      provider: aws-main
      secret: arn:aws:secretsmanager:us-east-1:123456789012:secret:warehouse-ABC123
`

func TestLoadNodeCloudSecretsValidateWithoutReadingCredentials(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "sandbox"), []byte("fixture"), 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "node.yml")
	for _, tc := range []struct {
		name, secrets string
		valid         bool
	}{
		{"selected mappings", cloudSecretsYAML, true},
		{"reserved environment", strings.ReplaceAll(cloudSecretsYAML, "KELVO_SOURCE_WAREHOUSE_PASSWORD", "LD_PRELOAD"), false},
		{"provider environment", strings.ReplaceAll(cloudSecretsYAML, "KELVO_SOURCE_WAREHOUSE_PASSWORD", "AWS_SECRET_ACCESS_KEY"), false},
		{"file environment", strings.ReplaceAll(cloudSecretsYAML, "KELVO_SOURCE_LOCAL_PASSWORD", "PATH"), false},
		{"duplicate mapping", strings.ReplaceAll(cloudSecretsYAML, "KELVO_SOURCE_LOCAL_PASSWORD", "KELVO_SOURCE_WAREHOUSE_PASSWORD"), false},
		{"unknown provider", strings.Replace(cloudSecretsYAML, "provider: aws-main", "provider: not-configured", 1), false},
		{"missing credential path", strings.Replace(cloudSecretsYAML, "credentials_file: secrets/provider.yml", "credentials_file: ''", 1), false},
		{"unknown field", strings.Replace(cloudSecretsYAML, "region: us-east-1", "endpoint: https://secret.example", 1), false},
		{"wrong resource region", strings.Replace(cloudSecretsYAML, "region: us-east-1", "region: us-west-2", 1), false},
		{"lookup timeout", strings.Replace(cloudSecretsYAML, "timeout: 5s", "timeout: 31s", 1), false},
		{"cache expiry", strings.Replace(cloudSecretsYAML, "ttl: 30s", "ttl: 6m", 1), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := os.WriteFile(path, []byte(resourceNodeYAML+tc.secrets), 0600); err != nil {
				t.Fatal(err)
			}
			cfg, err := LoadNode(path)
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%v error=%v", tc.valid, err)
			}
			if err != nil {
				for _, private := range []string{dir, "secrets/provider.yml", "arn:aws:", "secret.example"} {
					if strings.Contains(err.Error(), private) {
						t.Fatal("configuration error exposes credential or resource details")
					}
				}
				return
			}
			if cfg.Secrets.Providers["aws-main"].CredentialsFile != filepath.Join(dir, "secrets/provider.yml") || cfg.Secrets.Files["KELVO_SOURCE_LOCAL_PASSWORD"] != filepath.Join(dir, "secrets/local.txt") {
				t.Fatal("credential paths were not resolved against node YAML")
			}
			if _, err := os.Stat(filepath.Join(dir, "secrets")); !os.IsNotExist(err) {
				t.Fatal("config validation unexpectedly touched credentials")
			}
		})
	}
}
