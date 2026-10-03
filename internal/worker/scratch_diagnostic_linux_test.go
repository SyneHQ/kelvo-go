//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"bytes"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

func TestScratchCleanupDiagnosticIsSanitizedAndRetainsOwnership(t *testing.T) {
	for _, stage := range []string{"child_lease_held", "lease_record_read", "workspace_verify"} {
		t.Run(stage, func(t *testing.T) {
			root := scratchFixture(t)
			workspace, err := root.allocate()
			if err != nil {
				t.Fatal(err)
			}
			var output bytes.Buffer
			previous := log.Writer()
			log.SetOutput(&output)
			defer log.SetOutput(previous)
			switch stage {
			case "child_lease_held":
				fd, err := unix.FcntlInt(workspace.lease.Fd(), unix.F_DUPFD_CLOEXEC, 0)
				if err != nil {
					t.Fatal(err)
				}
				inherited := os.NewFile(uintptr(fd), "private-inherited-lease")
				defer inherited.Close()
			case "lease_record_read":
				if _, err := workspace.lease.WriteAt([]byte("private-source-credential-sentinel"), 0); err != nil {
					t.Fatal(err)
				}
			case "workspace_verify":
				if err := os.Chmod(workspace.path, 0o755); err != nil {
					t.Fatal(err)
				}
			}
			err = workspace.cleanup()
			if err == nil {
				t.Fatal("uncertain cleanup accepted")
			}
			reportScratchCleanupFailure(err)
			id := strings.TrimPrefix(filepath.Base(workspace.path), scratchPrefix)
			for _, path := range []string{workspace.path, filepath.Join(root.path, scratchLeasePrefix+id)} {
				if _, err := os.Stat(path); err != nil {
					t.Fatal("uncertain ownership removed", err)
				}
			}
			text := output.String()
			if !strings.Contains(text, "Kelvo scratch cleanup uncertain: stage="+stage) || strings.Contains(text, id) ||
				strings.Contains(text, root.path) || strings.Contains(text, "private-source-credential-sentinel") {
				t.Fatal("missing stage or private details in diagnostics", text)
			}
		})
	}
}
