//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"os/exec"
	"sync"
	"testing"
)

// Exercise independent forks while other executions are releasing their lease.
// CLOEXEC descriptors can exist briefly in an unrelated child before exec.
func TestScratchConcurrentExecAndCleanup(t *testing.T) {
	root := scratchFixture(t)
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			for range 128 {
				workspace, err := root.allocate()
				if err != nil {
					t.Error(err)
					return
				}
				cmd := exec.Command("/bin/true")
				workspace.attach(cmd)
				runErr := cmd.Run()
				cleanupErr := workspace.cleanup()
				if runErr != nil || cleanupErr != nil {
					t.Errorf("run=%v cleanup=%v", runErr, cleanupErr)
					return
				}
			}
		})
	}
	wg.Wait()
	if count, err := root.Reclaim(); err != nil || count != 0 {
		t.Fatalf("residual workspaces=%d err=%v", count, err)
	}
}
