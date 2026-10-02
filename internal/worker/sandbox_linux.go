//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

// SandboxCommand returns arguments for the separately-built, single-threaded
// kelvo-landlock launcher. The launcher must exec the worker before the Go
// runtime starts: Landlock ABI 3 applies a policy to one thread only.
//
// The caller runs SandboxPath with these arguments. It must not append source
// credentials or any other parent environment to the worker process.
func SandboxCommand(binary, jobdir string, cfg catalog.Config, limits query.Limits) ([]string, error) {
	if err := limits.Validate(); err != nil {
		return nil, fmt.Errorf("invalid sandbox limits: %w", err)
	}
	workerBinary, err := regularPath(binary)
	if err != nil {
		return nil, fmt.Errorf("sandbox worker binary: %w", err)
	}
	workspace, err := directoryPath(jobdir)
	if err != nil {
		return nil, fmt.Errorf("sandbox job directory: %w", err)
	}

	args := make([]string, 0, 2+len(cfg.Sources)*2+4)
	seenRead := make(map[string]struct{})
	addRead := func(path string) error {
		if path == "" {
			return nil
		}
		resolved, err := regularPath(path)
		if err != nil {
			return err
		}
		if _, ok := seenRead[resolved]; ok {
			return nil
		}
		seenRead[resolved] = struct{}{}
		args = append(args, "--read", resolved)
		return nil
	}
	for _, source := range cfg.Sources {
		if source.Object != nil || source.Range != nil {
			if _, err := sourceEnvironmentNames(source); err != nil {
				return nil, err
			}
			// Loopback range capabilities are not host filesystem paths. The
			// parent owns all cloud credentials and upstream requests. DuckDB
			// enforces exact URLs; Landlock ABI 3 does not constrain network.
			continue
		}
		if source.Adapter != "" {
			if err := source.ValidateAdapter(); err != nil {
				return nil, err
			}
			continue
		}
		switch {
		case catalog.FileType(source.Type):
			if err := addRead(source.Path); err != nil {
				return nil, fmt.Errorf("sandbox source %q: %w", source.ID, err)
			}
		case catalog.NativeType(source.Type):
			// Network access is intentionally outside Landlock ABI 3. Source
			// credentials are passed through the worker's already-filtered env.
		case source.Federation != nil:
			// Compiled-in custom federation drivers receive network access and
			// selected environment references, never additional host file grants.
			if err := source.ValidateFederation(); err != nil {
				return nil, fmt.Errorf("sandbox source %q has invalid federation configuration", source.ID)
			}
		default:
			return nil, fmt.Errorf("sandbox source %q has unsupported type", source.ID)
		}
	}
	if cfg.ExtensionDirectory != "" {
		extensions, err := directoryPath(cfg.ExtensionDirectory)
		if err != nil {
			return nil, fmt.Errorf("sandbox extension directory: %w", err)
		}
		if _, exists := seenRead[extensions]; !exists {
			// DuckDB loads approved extensions as executable shared objects.
			seenRead[extensions] = struct{}{}
			args = append(args, "--read-exec", extensions)
		}
	}
	args = append(args, "--write", workspace, "--", workerBinary, "worker")
	return args, nil
}

func resolvedPath(path string) (string, error) {
	if !filepath.IsAbs(path) {
		return "", fmt.Errorf("path must be absolute")
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", fmt.Errorf("path is unavailable")
	}
	return resolved, nil
}

func regularPath(path string) (string, error) {
	resolved, err := resolvedPath(path)
	if err != nil {
		return "", err
	}
	info, err := os.Stat(resolved)
	if err != nil || !info.Mode().IsRegular() {
		return "", fmt.Errorf("path must be a regular file")
	}
	return resolved, nil
}

func directoryPath(path string) (string, error) {
	resolved, err := resolvedPath(path)
	if err != nil {
		return "", err
	}
	info, err := os.Stat(resolved)
	if err != nil || !info.IsDir() {
		return "", fmt.Errorf("path must be a directory")
	}
	return resolved, nil
}
