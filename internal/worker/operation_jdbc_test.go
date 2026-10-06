// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"fmt"
	"maps"
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
)

func TestOperationJDBCManifestPinsExactRuntime(t *testing.T) {
	good := "version: 1\nfiles:\n"
	for _, path := range []string{"bin/java", "lib/modules", "lib/libjli.so", "lib/server/libjvm.so"} {
		good += fmt.Sprintf("  - path: %s\n    sha256: %s\n", path, strings.Repeat("a", 64))
	}
	if _, err := parseOperationJDBCManifest([]byte(good)); err != nil {
		t.Fatal(err)
	}
	for name, raw := range map[string]string{
		"unknown field":   good + "java_flags: unsafe\n",
		"extra document":  good + "---\nversion: 1\n",
		"absolute path":   strings.Replace(good, "bin/java", "/bin/java", 1),
		"parent path":     strings.Replace(good, "bin/java", "../bin/java", 1),
		"unclean path":    strings.Replace(good, "bin/java", "bin/./java", 1),
		"missing modules": strings.Replace(good, "lib/modules", "lib/other", 1),
		"duplicate file":  good + "  - path: bin/java\n    sha256: " + strings.Repeat("a", 64) + "\n",
		"unknown version": strings.Replace(good, "version: 1", "version: 2", 1),
		"unbounded":       strings.Repeat(" ", maxJDBCManifestBytes+1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := parseOperationJDBCManifest([]byte(raw)); err == nil {
				t.Fatal("unpinned runtime manifest accepted")
			}
		})
	}
}

func TestOperationJDBCProfilesCloneAndBudget(t *testing.T) {
	cfg := OperationProcessConfig{JDBC: &OperationJDBCConfig{JavaHome: "/opt/java", Profiles: map[string][]OperationJDBCArtifact{"h2": {{Path: "/opt/main.jar", SHA256: strings.Repeat("a", 64)}}}}}
	copy := cfg.Clone()
	copy.JDBC.JavaHome = "/other"
	copy.JDBC.Profiles["h2"][0].Path = "/other.jar"
	delete(copy.JDBC.Profiles, "h2")
	if cfg.JDBC.JavaHome != "/opt/java" || cfg.JDBC.Profiles["h2"][0].Path != "/opt/main.jar" {
		t.Fatal("node config retained mutable runtime paths")
	}
	cfg.preparedJDBC = &operationJDBCRuntime{binding: "node-owned"}
	if cfg.Clone().preparedJDBC != nil {
		t.Fatal("configuration clone copied runtime descriptor ownership")
	}
	for _, mb := range []int{256, 512, 1024, 16384} {
		heap, direct, err := jdbcProcessBudget(mb)
		if err != nil || heap < 32 || direct < 16 || heap+direct+128 > mb || heap > 4096 || direct > 4096 {
			t.Fatal("JVM budget exceeded admission", mb, heap, direct, err)
		}
	}
	if _, _, err := jdbcProcessBudget(255); err == nil {
		t.Fatal("insufficient JVM admission accepted")
	}
}

func TestOperationJDBCSourceDoesNotControlRuntime(t *testing.T) {
	_, request, response := operationResolverFixture(t)
	response.Source = catalog.Source{ID: "source_1", Type: "h2", URLEnv: "KELVO_SOURCE_REQUEST_0_URL", UsernameEnv: "KELVO_SOURCE_REQUEST_0_USERNAME", PasswordEnv: "KELVO_SOURCE_REQUEST_0_PASSWORD"}
	response.Secrets = map[string]string{response.Source.URLEnv: "tls://db.example:9092", response.Source.UsernameEnv: "reader", response.Source.PasswordEnv: "fixture-private"}
	if err := validateOperationJDBCCatalog(response, request); err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*operationResolution){
		"ambient user":     func(r *operationResolution) { r.Source.UsernameEnv = "USER" },
		"extra secret":     func(r *operationResolution) { r.Secrets["JAVA_HOME"] = "/other" },
		"path":             func(r *operationResolution) { r.Source.Path = "/other" },
		"adapter":          func(r *operationResolution) { r.Source.Adapter = "other" },
		"plaintext":        func(r *operationResolution) { r.Secrets[r.Source.URLEnv] = "tcp://db.example:9092" },
		"source classpath": func(r *operationResolution) { r.Source.Options = map[string]string{"classpath": "/other"} },
	} {
		t.Run(name, func(t *testing.T) {
			changed := response
			changed.Secrets = maps.Clone(response.Secrets)
			change(&changed)
			if validateOperationJDBCCatalog(changed, request) == nil {
				t.Fatal("source selected runtime configuration")
			}
		})
	}
}
