// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.yaml.in/yaml/v3"
)

const rotationOld = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
const rotationNew = "cccccccccccccccccccccccccccccccc"
const rotationOther = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

func rotationDocument(t *testing.T, revision uint64, tenants map[string][]string) []byte {
	t.Helper()
	raw, err := yaml.Marshal(gatewayKeyDocument{Version: 1, Revision: revision, Tenants: tenants})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func rotationSet(t *testing.T, revision uint64, keys ...string) gatewayKeySet {
	t.Helper()
	set, err := parseGatewayKeys(rotationDocument(t, revision, map[string][]string{"a": keys, "b": {rotationOther}}), map[string]bool{"a": true, "b": true}, 1)
	if err != nil {
		t.Fatal(err)
	}
	return set
}

func TestGatewayKeyDocumentBoundedExactTenantPolicy(t *testing.T) {
	tenants := map[string]bool{"a": true, "b": true}
	good := rotationDocument(t, 7, map[string][]string{"a": {rotationOld, rotationNew}, "b": {rotationOther}})
	set, err := parseGatewayKeys(good, tenants, 7)
	if err != nil || set.revision != 7 || len(set.keys) != 3 {
		t.Fatal("overlap rejected", err)
	}
	disabled := rotationDocument(t, 8, map[string][]string{"a": {}, "b": {rotationOther}})
	if _, err := parseGatewayKeys(disabled, tenants, 7); err != nil {
		t.Fatal("explicit tenant disable rejected", err)
	}
	cases := map[string][]byte{
		"missing tenant":           rotationDocument(t, 7, map[string][]string{"a": {rotationOld}}),
		"unknown tenant":           rotationDocument(t, 7, map[string][]string{"a": {rotationOld}, "c": {rotationOther}}),
		"duplicate across tenants": rotationDocument(t, 7, map[string][]string{"a": {rotationOld}, "b": {rotationOld}}),
		"duplicate within tenant":  rotationDocument(t, 7, map[string][]string{"a": {rotationOld, rotationOld}, "b": {rotationOther}}),
		"too many":                 rotationDocument(t, 7, map[string][]string{"a": {rotationOld, rotationNew, strings.Repeat("d", 32), strings.Repeat("e", 32), strings.Repeat("f", 32)}, "b": {rotationOther}}),
		"old revision":             rotationDocument(t, 6, map[string][]string{"a": {rotationOld}, "b": {rotationOther}}),
		"zero revision":            rotationDocument(t, 0, map[string][]string{"a": {rotationOld}, "b": {rotationOther}}),
		"short token":              rotationDocument(t, 7, map[string][]string{"a": {"short"}, "b": {rotationOther}}),
		"long token":               rotationDocument(t, 7, map[string][]string{"a": {strings.Repeat("x", 257)}, "b": {rotationOther}}),
		"whitespace":               rotationDocument(t, 7, map[string][]string{"a": {rotationOld + " "}, "b": {rotationOther}}),
		"comma":                    rotationDocument(t, 7, map[string][]string{"a": {rotationOld + ","}, "b": {rotationOther}}),
		"extra document":           append(append([]byte{}, good...), []byte("---\n{}\n")...),
		"unknown field":            append(append([]byte{}, good...), []byte("extra: secret\n")...),
		"duplicate field":          append(append([]byte{}, good...), []byte("version: 1\n")...),
		"null tenant":              []byte("version: 1\nrevision: 7\ntenants: {a: null, b: []}\n"),
		"alias":                    []byte("version: 1\nrevision: 7\ntenants: {a: &key [], b: *key}\n"),
		"oversized":                []byte(strings.Repeat(" ", gatewayKeyFileLimit+1)),
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := parseGatewayKeys(raw, tenants, 7); err != errGatewayAuthUnavailable {
				t.Fatal("invalid private key document accepted", err)
			}
		})
	}
}

func TestGatewayKeyConfigurationBackwardCompatibleAndExclusive(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "gateway.yml")
	fileMode := strings.Replace(gatewayYAML, "token_env: KELVO_TOKEN_A", "", 1) + "authentication:\n  keys_file: gateway-keys.yml\n"
	for name, text := range map[string]string{"legacy": gatewayYAML, "file": fileMode} {
		t.Run(name, func(t *testing.T) {
			if err := os.WriteFile(path, []byte(text), 0600); err != nil {
				t.Fatal(err)
			}
			config, err := LoadGateway(path)
			if err != nil {
				t.Fatal(err)
			}
			if name == "file" && (config.Authentication.KeysFile != filepath.Join(root, "gateway-keys.yml") || config.Authentication.ReloadInterval != time.Second || config.Authentication.MinRevision != 1) {
				t.Fatal("file auth defaults/path changed")
			}
		})
	}
	for _, text := range []string{
		gatewayYAML + "authentication: {keys_file: keys.yml}\n",
		fileMode + "  reload_interval: 100ms\n",
		fileMode + "  reload_interval: 61s\n",
		strings.Replace(gatewayYAML, "token_env: KELVO_TOKEN_A", "", 1),
	} {
		if err := os.WriteFile(path, []byte(text), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadGateway(path); err == nil {
			t.Fatal("unsafe authentication config accepted")
		}
	}
}
