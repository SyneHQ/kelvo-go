//go:build linux

package worker

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

func TestSandboxCommandBuildsCanonicalLeastPrivilegeArguments(t *testing.T) {
	tmp := t.TempDir()
	binary := filepath.Join(tmp, "kelvo")
	source := filepath.Join(tmp, "sales.csv")
	extensions := filepath.Join(tmp, "extensions")
	jobdir := filepath.Join(tmp, "job")
	for _, path := range []string{binary, source} {
		if err := os.WriteFile(path, []byte("fixture"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	for _, path := range []string{extensions, jobdir} {
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	args, err := SandboxCommand(binary, jobdir, catalog.Config{
		ExtensionDirectory: extensions,
		Sources: []catalog.Source{
			{ID: "sales", Type: "csv", Path: source},
			{ID: "remote", Type: "clickhouse", URLEnv: "KELVO_CLICKHOUSE_URL"},
			{ID: "again", Type: "parquet", Path: source},
		},
	}, query.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"--read", source, "--read-exec", extensions, "--write", jobdir, "--", binary, "worker"}
	if !reflect.DeepEqual(args, want) {
		t.Fatalf("args=%q want=%q", args, want)
	}
}

func TestSandboxCommandRejectsUnresolvedOrUnsafePaths(t *testing.T) {
	tmp := t.TempDir()
	binary := filepath.Join(tmp, "kelvo")
	jobdir := filepath.Join(tmp, "job")
	if err := os.WriteFile(binary, []byte("fixture"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(jobdir, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := SandboxCommand("relative-kelvo", jobdir, catalog.Config{}, query.DefaultLimits()); err == nil {
		t.Fatal("relative worker binary was accepted")
	}
	if _, err := SandboxCommand(binary, "relative-job", catalog.Config{}, query.DefaultLimits()); err == nil {
		t.Fatal("relative job directory was accepted")
	}
	if _, err := SandboxCommand(binary, jobdir, catalog.Config{Sources: []catalog.Source{{ID: "sales", Type: "csv", Path: "relative.csv"}}}, query.DefaultLimits()); err == nil {
		t.Fatal("relative source was accepted")
	}
}

func TestSandboxObjectReadDoesNotGrantLocalSourcePaths(t *testing.T) {
	directory := t.TempDir()
	binary := filepath.Join(directory, "kelvo")
	job := filepath.Join(directory, "job")
	extensions := filepath.Join(directory, "extensions")
	if err := os.WriteFile(binary, []byte("fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{job, extensions} {
		if err := os.Mkdir(path, 0700); err != nil {
			t.Fatal(err)
		}
	}
	args, err := SandboxCommand(binary, job, catalog.Config{ExtensionDirectory: extensions, Sources: []catalog.Source{workerObjectFixture()}}, query.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"--read-exec", extensions, "--write", job, "--", binary, "worker"}
	if !reflect.DeepEqual(args, want) {
		t.Fatalf("remote object added a host filesystem grant: %q", args)
	}
}
