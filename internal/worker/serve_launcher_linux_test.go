//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// The sandbox launcher passes `--pool-size N` to serve and nothing else. The
// checks run before any confinement, so they need no privileges: a valid pool
// size gets past argument checking and stops at a later check (the pinned
// executable descriptor), whichever message that gives.
func TestSandboxLauncherServePoolSizeArgument(t *testing.T) {
	cc, err := exec.LookPath("cc")
	if err != nil {
		t.Skip("no C compiler")
	}
	launcher := filepath.Join(t.TempDir(), "launcher")
	if out, err := exec.Command(cc, "-O2", "-Wall", "-Wextra", "-Werror", "../../sandbox/launcher.c", "-o", launcher).CombinedOutput(); err != nil {
		t.Fatalf("launcher does not build: %v\n%s", err, out)
	}
	job := t.TempDir()
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"serve", "--pool-size", "8"}, ""},
		{[]string{"serve", "--pool-size", "32"}, ""},
		{[]string{"serve", "--pool-size", "0"}, "invalid serve pool size argument"},
		{[]string{"serve", "--pool-size", "33"}, "invalid serve pool size argument"},
		{[]string{"serve", "--pool-size", "08"}, "invalid serve pool size argument"},
		{[]string{"serve", "--pool-size", "8x"}, "invalid serve pool size argument"},
		{[]string{"serve", "--other", "8"}, "invalid serve pool size argument"},
		{[]string{"serve", "--pool-size"}, "launcher may only exec the kelvo worker entrypoint"},
		{[]string{"serve", "--pool-size", "8", "extra"}, "launcher may only exec the kelvo worker entrypoint"},
		{[]string{"worker", "--pool-size", "8"}, "launcher may only exec the kelvo worker entrypoint"},
	} {
		args := append([]string{"--write", job, "--", "/proc/self/fd/5"}, tc.args...)
		out, _ := exec.Command(launcher, args...).CombinedOutput()
		if tc.want == "" {
			for _, refused := range []string{"invalid serve pool size argument", "launcher may only exec"} {
				if strings.Contains(string(out), refused) {
					t.Errorf("%v: valid arguments refused: %q", tc.args, out)
				}
			}
		} else if !strings.Contains(string(out), tc.want) {
			t.Errorf("%v: got %q, want %q", tc.args, out, tc.want)
		}
	}
}

// The node chooses the slim adapter only when its pinned file is present and
// matches: a missing, changed or loosely permissioned file selects the full one.
func TestOperationBinaryUsable(t *testing.T) {
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "adapter")
	body := []byte("#!/bin/sh\nexit 0\n")
	if err := os.WriteFile(path, body, 0o755); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(body)
	pin := OperationProcessConfig{Binary: path, SHA256: hex.EncodeToString(sum[:])}
	ctx := context.Background()
	if !OperationBinaryUsable(ctx, pin) {
		t.Fatal("a matching pinned binary was refused")
	}
	for name, cfg := range map[string]OperationProcessConfig{
		"wrong digest": {Binary: path, SHA256: strings.Repeat("0", 64)},
		"missing":      {Binary: filepath.Join(dir, "absent"), SHA256: pin.SHA256},
		"relative":     {Binary: "adapter", SHA256: pin.SHA256},
	} {
		if OperationBinaryUsable(ctx, cfg) {
			t.Errorf("%s: accepted", name)
		}
	}
	if err := os.Chmod(path, 0o757); err != nil {
		t.Fatal(err)
	}
	if OperationBinaryUsable(ctx, pin) {
		t.Error("a world-writable binary was accepted")
	}
}
