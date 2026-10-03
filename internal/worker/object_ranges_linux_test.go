//go:build linux

package worker

import (
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMultipartObjectCapabilitiesNeverGrantFilesOrSecrets(t *testing.T) {
	dir := t.TempDir()
	binary := filepath.Join(dir, "worker")
	if err := os.WriteFile(binary, []byte("fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	base := "http://127.0.0.1:12345/" + strings.Repeat("a", 64) + "/snapshot/part-"
	source := catalog.Source{ID: "snapshot", Type: "parquet", Ranges: []catalog.ObjectRange{{URL: base + "0000", Bytes: 100}, {URL: base + "0001", Bytes: 100}}}
	names, err := sourceEnvironmentNames(source)
	if err != nil || len(names) != 0 {
		t.Fatal("range list received credentials", err)
	}
	args, err := SandboxCommand(binary, dir, catalog.Config{Sources: []catalog.Source{source}}, query.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	for _, arg := range args {
		if arg == "--read" || strings.HasPrefix(arg, "http:") {
			t.Fatal("range granted filesystem access")
		}
	}
	source.TokenEnv = "KELVO_SOURCE_SECRET"
	if _, err := sourceEnvironmentNames(source); err == nil {
		t.Fatal("mixed credential accepted")
	}
	if _, err := SandboxCommand(binary, dir, catalog.Config{Sources: []catalog.Source{source}}, query.DefaultLimits()); err == nil {
		t.Fatal("sandbox accepted mixed credential")
	}
}
