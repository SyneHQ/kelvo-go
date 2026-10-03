//go:build !linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import "errors"

// ScratchRoot is available only on Linux with descriptor-relative filesystem
// access and inherited advisory ownership locks.
type ScratchRoot struct{}

func OpenScratchRoot(string) (*ScratchRoot, error) {
	return nil, errors.New("managed worker scratch requires Linux")
}
func (*ScratchRoot) Close() error { return nil }
func (*ScratchRoot) Reclaim() (int, error) {
	return 0, errors.New("managed worker scratch requires Linux")
}
func (*ScratchRoot) allocate() (*scratchWorkspace, error) {
	return nil, errors.New("managed worker scratch requires Linux")
}
