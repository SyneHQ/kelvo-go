//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	federationapi "github.com/SYNEHQ/kelvo-go/federation"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

type sandboxFederationDriver struct{}

func (sandboxFederationDriver) Validate(federationapi.Source, federationapi.Table) error { return nil }
func (sandboxFederationDriver) Open(context.Context, federationapi.Source, federationapi.Table, federationapi.Limits) (federationapi.Relation, error) {
	return nil, errors.New("sandbox planning must not open a connection")
}
func init() { federationapi.MustRegister("sandbox_fixture", sandboxFederationDriver{}) }

func TestSandboxRegisteredFederationGrantsNoHostPaths(t *testing.T) {
	dir := t.TempDir()
	binary, job := filepath.Join(dir, "kelvo"), filepath.Join(dir, "job")
	if err := os.WriteFile(binary, []byte("fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(job, 0700); err != nil {
		t.Fatal(err)
	}
	source := catalog.Source{ID: "custom", Type: "sandbox_fixture", URLEnv: "KELVO_SOURCE_CUSTOM_URL", Federation: &catalog.FederationConfig{Tables: []catalog.FederationTable{{Name: "events", Table: "events"}}}}
	args, err := SandboxCommand(binary, job, catalog.Config{Sources: []catalog.Source{source}}, query.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"--write", job, "--", binary, "worker"}
	if !reflect.DeepEqual(args, want) {
		t.Fatalf("custom driver widened filesystem: %v", args)
	}
	source.Path = filepath.Join(dir, "private")
	if _, err := SandboxCommand(binary, job, catalog.Config{Sources: []catalog.Source{source}}, query.DefaultLimits()); err == nil {
		t.Fatal("custom driver requested a local path")
	}
	source.Path, source.Type = "", "unregistered_fixture"
	if _, err := SandboxCommand(binary, job, catalog.Config{Sources: []catalog.Source{source}}, query.DefaultLimits()); err == nil {
		t.Fatal("unregistered driver accepted")
	}
}
