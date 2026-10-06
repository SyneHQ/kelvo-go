//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
	"golang.org/x/sys/unix"
)

type operationJDBCRuntime struct {
	binding    string
	root       *os.File
	java       *os.File
	profiles   map[string][]*os.File
	identities map[string]os.FileInfo
	opened     map[*os.File]operationBinaryIdentity
}

func (r *operationJDBCRuntime) close() {
	for file := range r.opened {
		_ = file.Close()
	}
}

func (r *operationJDBCRuntime) current() error {
	for path, previous := range r.identities {
		current, err := os.Lstat(path)
		if err != nil || !jdbcImmutableInfo(current) || !os.SameFile(current, previous) || current.Mode() != previous.Mode() || (!current.IsDir() && (current.Size() != previous.Size() || !current.ModTime().Equal(previous.ModTime()))) {
			return operationFailure("CONFIGURATION_ERROR")
		}
	}
	for file, identity := range r.opened {
		if operationBinaryCurrent(file, identity) != nil {
			return operationFailure("CONFIGURATION_ERROR")
		}
	}
	return nil
}

func (r *operationJDBCRuntime) selectProfile(engine string, memoryMB int) (*adapter.JDBCRuntime, []*os.File, error) {
	jars := r.profiles[engine]
	if len(jars) == 0 {
		return nil, nil, operationFailure("UNSUPPORTED")
	}
	heap, direct, err := jdbcProcessBudget(memoryMB)
	if err != nil {
		return nil, nil, err
	}
	input := &adapter.JDBCRuntime{Version: 1, JavaFD: 7, HeapMB: heap, DirectMB: direct}
	files := []*os.File{r.java}
	for i, jar := range jars {
		input.JARFDs = append(input.JARFDs, 8+i)
		files = append(files, jar)
	}
	files = append(files, r.root)
	return input, files, nil
}

// Pin an exact JRE tree and ordered JARs before any source credentials exist.
// Root-owned ancestors without group/other write access prevent the unprivileged
// worker from replacing a library after verification. Symlinks are not allowed.
func openOperationJDBC(ctx context.Context, cfg *OperationJDBCConfig) (*operationJDBCRuntime, error) {
	if cfg == nil {
		return nil, nil
	}
	bad := operationFailure("CONFIGURATION_ERROR")
	if ctx == nil || ctx.Err() != nil || len(cfg.Profiles) < 1 || len(cfg.Profiles) > 5 || cfg.JavaHome == "/" || !operations.ValidDigest(cfg.ManifestSHA256) {
		return nil, bad
	}
	r := &operationJDBCRuntime{binding: jdbcConfigDigest(cfg), profiles: map[string][]*os.File{}, identities: map[string]os.FileInfo{}, opened: map[*os.File]operationBinaryIdentity{}}
	if r.binding == "" {
		return nil, bad
	}
	keep := false
	defer func() {
		if !keep {
			r.close()
		}
	}()
	for _, path := range []string{cfg.JavaHome, cfg.Manifest} {
		if err := r.checkPath(path); err != nil {
			return nil, err
		}
	}
	manifestFile, err := r.openArtifact(ctx, OperationJDBCArtifact{Path: cfg.Manifest, SHA256: cfg.ManifestSHA256}, maxJDBCManifestBytes)
	if err != nil {
		return nil, err
	}
	raw, err := io.ReadAll(io.LimitReader(manifestFile, maxJDBCManifestBytes+1))
	if err != nil {
		return nil, bad
	}
	manifest, err := parseOperationJDBCManifest(raw)
	if err != nil {
		return nil, err
	}
	fd, err := unix.Open(cfg.JavaHome, unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, bad
	}
	r.root = os.NewFile(uintptr(fd), "pinned-jre")
	rootInfo, err := r.root.Stat()
	if err != nil || !rootInfo.IsDir() || !jdbcImmutableInfo(rootInfo) || !os.SameFile(rootInfo, r.identities[cfg.JavaHome]) {
		_ = r.root.Close()
		return nil, bad
	}
	r.opened[r.root] = operationBinaryIdentity{info: rootInfo}
	expected := make(map[string]string, len(manifest.Files))
	for _, file := range manifest.Files {
		expected[file.Path] = file.SHA256
	}
	seen := 0
	total := int64(0)
	err = filepath.WalkDir(cfg.JavaHome, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil || ctx.Err() != nil || len(r.identities) > 16384 {
			return bad
		}
		info, err := entry.Info()
		if err != nil || !jdbcImmutableInfo(info) {
			return bad
		}
		r.identities[path] = info
		if entry.IsDir() {
			return nil
		}
		relative, err := filepath.Rel(cfg.JavaHome, path)
		if err != nil || expected[relative] == "" {
			return bad
		}
		total += info.Size()
		if total > 4<<30 {
			return bad
		}
		file, err := r.openArtifact(ctx, OperationJDBCArtifact{Path: path, SHA256: expected[relative]}, 1<<30)
		if err != nil {
			return err
		}
		seen++
		if relative == "bin/java" {
			if info.Mode()&0111 == 0 {
				return bad
			}
			r.java = file
		} else {
			_ = file.Close()
			delete(r.opened, file)
		}
		return nil
	})
	if err != nil || seen != len(expected) || r.java == nil {
		return nil, bad
	}
	for engine, artifacts := range cfg.Profiles {
		if !adapter.JDBCProfile(engine) || len(artifacts) < 1 || len(artifacts) > 32 {
			return nil, bad
		}
		seen := map[string]bool{}
		for _, artifact := range artifacts {
			if seen[artifact.Path] || filepath.Ext(artifact.Path) != ".jar" {
				return nil, bad
			}
			seen[artifact.Path] = true
			file, err := r.openArtifact(ctx, artifact, 512<<20)
			if err != nil {
				return nil, err
			}
			r.profiles[engine] = append(r.profiles[engine], file)
		}
	}
	if ctx.Err() != nil || r.current() != nil {
		return nil, bad
	}
	keep = true
	return r, nil
}

func jdbcImmutableInfo(info os.FileInfo) bool {
	if info == nil || (!info.IsDir() && !info.Mode().IsRegular()) || info.Mode()&0022 != 0 {
		return false
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Uid == 0
}

func (r *operationJDBCRuntime) checkPath(path string) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || len(path) > 4096 {
		return operationFailure("CONFIGURATION_ERROR")
	}
	for current := path; ; current = filepath.Dir(current) {
		info, err := os.Lstat(current)
		if err != nil || !jdbcImmutableInfo(info) || (current != path && !info.IsDir()) {
			return operationFailure("CONFIGURATION_ERROR")
		}
		r.identities[current] = info
		if current == "/" {
			return nil
		}
	}
}

func (r *operationJDBCRuntime) openArtifact(ctx context.Context, artifact OperationJDBCArtifact, maximum int64) (*os.File, error) {
	bad := operationFailure("CONFIGURATION_ERROR")
	if !operations.ValidDigest(artifact.SHA256) || r.checkPath(artifact.Path) != nil {
		return nil, bad
	}
	fd, err := unix.Open(artifact.Path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, bad
	}
	file := os.NewFile(uintptr(fd), "pinned-jdbc-artifact")
	info, err := file.Stat()
	if err != nil || !jdbcImmutableInfo(info) || !info.Mode().IsRegular() || info.Size() < 1 || info.Size() > maximum || !os.SameFile(info, r.identities[artifact.Path]) {
		_ = file.Close()
		return nil, bad
	}
	r.opened[file] = operationBinaryIdentity{info: info}
	hash := sha256.New()
	buffer := make([]byte, 64<<10)
	var count int64
	for {
		if ctx.Err() != nil {
			return nil, bad
		}
		n, readErr := file.Read(buffer)
		count += int64(n)
		if count > info.Size() {
			return nil, bad
		}
		_, _ = hash.Write(buffer[:n])
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return nil, bad
		}
	}
	if count != info.Size() || hex.EncodeToString(hash.Sum(nil)) != artifact.SHA256 || operationBinaryCurrent(file, r.opened[file]) != nil {
		return nil, bad
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, bad
	}
	return file, nil
}
