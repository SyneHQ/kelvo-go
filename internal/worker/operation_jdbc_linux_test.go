//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"bytes"
	"context"
	"io"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/internal/admission"
	"github.com/SYNEHQ/kelvo-go/operations"
	"go.yaml.in/yaml/v3"
)

func operationFixtureJVM(input adapter.ProcessRequest) error {
	files := make([]*os.File, 5+len(input.Runtime.JARFDs))
	files[4] = os.NewFile(7, "pinned-java")
	for i, fd := range input.Runtime.JARFDs {
		files[5+i] = os.NewFile(uintptr(fd), "pinned-jar")
		if _, err := files[5+i].Read(make([]byte, 4)); err != nil {
			return err
		}
	}
	var output bytes.Buffer
	command := exec.Command("/proc/self/fd/7", "-XX:ActiveProcessorCount=1", "-XX:+UseSerialGC", "-XX:-UsePerfData", "-XX:-CreateCoredumpOnCrash", "-Xmx64m", "-version")
	command.Env = []string{"LANG=C.UTF-8", "TZ=UTC"}
	command.Stdin = os.Stdin
	command.Stdout, command.Stderr = &output, &output
	command.ExtraFiles = files
	return command.Run()
}

// This opt-in fixture uses a real operator-pinned JRE. The default suite does
// not discover or download Java installations or execute ambient runtimes.
func TestJDBCContainedPinnedRuntime(t *testing.T) {
	path := os.Getenv("KELVO_TEST_JDBC_CONFIG")
	if path == "" {
		t.Skip("explicit root-owned pinned JDBC runtime fixture required")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var config OperationJDBCConfig
	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	decoder.KnownFields(true)
	if err := decoder.Decode(&config); err != nil {
		t.Fatal(err)
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		t.Fatal("extra runtime config document")
	}
	executor, manager, _ := containedExecutor(t)
	executor.Limits.MemoryMB = 256
	executor.Limits.Timeout = 45 * time.Second
	executor.ResourceOverheadBytes = 352 << 20
	pool, err := admission.New(admission.Limits{MaxConcurrent: 1, MemoryBytes: 768 << 20, ScratchBytes: 32 << 20})
	if err != nil {
		t.Fatal(err)
	}
	executor.ResourcePool = pool
	cfg := operationExecutable(t)
	cfg.JDBC = &config
	cfg, closeRuntime, err := cfg.PrepareRuntime(context.Background())
	if err != nil {
		t.Fatal("operator runtime preparation", err)
	}
	defer closeRuntime()
	pinned := cfg.preparedJDBC
	input := operationProcessInput(t, operations.ConnectionTest, "fixture")
	input.Source.Engine = "h2"
	input.Source.Username = "fixture-reader"
	input.Source.URL = "tls://db.example:9092"
	input.Limits.MemoryMB = 256
	receipt, err := executor.ExecuteResolvedOperation(context.Background(), cfg, input.OperationID, input.RequestSHA256, func(context.Context) (adapter.ProcessRequest, error) {
		// Runtime verification must complete before the five-second credential
		// lease begins; otherwise a large JRE would exhaust it before launch.
		input.CredentialsValidUntil = time.Now().Unix() + 5
		input.ExpiresAt = time.Now().Unix() + 30
		return input, nil
	}, nil)
	if err != nil || receipt.Outcome != operations.Completed || receipt.Effect != operations.EffectNone {
		t.Fatal("pinned JVM failed inside containment", receipt, err)
	}
	if pool.Snapshot().Active != 0 || manager.Status().Active != 0 {
		t.Fatal("JVM leaked resource custody")
	}
	if cfg.preparedJDBC != pinned || pinned.current() != nil {
		t.Fatal("operation discarded node-owned runtime")
	}
	// Native adapters need no JRE borrow. The pinned runtime stays owned for
	// later JDBC operations; neither source credentials nor sessions are reused.
	native := operationProcessInput(t, operations.ConnectionTest, "fixture")
	native.CredentialsValidUntil = time.Now().Unix() + 5
	receipt, err = executor.ExecuteOperation(context.Background(), cfg, native, nil)
	if err != nil || receipt.Outcome != operations.Completed || pinned.current() != nil {
		t.Fatal("native operation disturbed pinned JVM", receipt, err)
	}
	// A config changed after preparation cannot reuse previously pinned code.
	cfg.JDBC.Profiles["h2"][0].SHA256 = "changed-after-preparation"
	input.CredentialsValidUntil = time.Now().Unix() + 5
	receipt, err = executor.ExecuteOperation(context.Background(), cfg, input, nil)
	if err == nil || receipt.Outcome != operations.Rejected || receipt.Effect != operations.EffectNone {
		t.Fatal("changed runtime binding was accepted", receipt, err)
	}
	closeRuntime()
	if pinned.current() == nil {
		t.Fatal("runtime descriptors survived node shutdown")
	}
}
