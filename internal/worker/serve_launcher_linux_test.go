//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
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
