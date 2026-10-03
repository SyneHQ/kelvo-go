// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"errors"
	"log"
	"os"
	"os/exec"
)

type scratchCleanupError struct {
	stage string
	cause error
}

func (e *scratchCleanupError) Error() string { return e.cause.Error() }
func (e *scratchCleanupError) Unwrap() error { return e.cause }

// reportScratchCleanupFailure runs only after caller-side quarantine. It emits
// fixed labels, never ownership IDs, paths, records or underlying error text.
func reportScratchCleanupFailure(err error) {
	stage := "unmanaged_remove"
	var failure *scratchCleanupError
	if errors.As(err, &failure) {
		stage = failure.stage
	}
	log.Printf("Kelvo scratch cleanup uncertain: stage=%s", stage)
}

// scratchWorkspace keeps its ownership lock open until the executor and child
// have both finished. The child inherits the same open file description.
type scratchWorkspace struct {
	path    string
	lease   *os.File
	cleanup func() error
}

func newScratchWorkspace(root *ScratchRoot) (*scratchWorkspace, error) {
	if root != nil {
		return root.allocate()
	}
	path, err := os.MkdirTemp("", "kelvo-worker-")
	if err != nil {
		return nil, err
	}
	return &scratchWorkspace{path: path, cleanup: func() error { return os.RemoveAll(path) }}, nil
}

func (w *scratchWorkspace) attach(cmd *exec.Cmd) {
	if w.lease != nil {
		cmd.ExtraFiles = append(cmd.ExtraFiles, w.lease)
	}
}
