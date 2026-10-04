//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package federation

import (
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

func openSnapshotFile(path string) (*os.File, error) {
	// O_PATH permits ancestor traversal under exact-file Landlock grants,
	// without granting directory listings. Refuse symlinks in every component.
	dir, err := unix.Open("/", unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	for _, name := range strings.Split(strings.TrimPrefix(filepath.Dir(path), "/"), "/") {
		if name == "" {
			continue
		}
		next, openErr := unix.Openat(dir, name, unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		_ = unix.Close(dir)
		if openErr != nil {
			return nil, openErr
		}
		dir = next
	}
	defer unix.Close(dir)
	var parent unix.Stat_t
	if unix.Fstat(dir, &parent) != nil || parent.Uid != uint32(os.Geteuid()) || parent.Mode&0077 != 0 {
		return nil, snapshotUnavailable()
	}
	fd, err := unix.Openat(dir, filepath.Base(path), unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), "snapshot")
	var stat unix.Stat_t
	if unix.Fstat(fd, &stat) != nil || stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Uid != uint32(os.Geteuid()) || stat.Mode&0277 != 0 || stat.Nlink != 1 {
		file.Close()
		return nil, snapshotUnavailable()
	}
	// Keep a child-held payload lease even if its parent dies during a scan.
	if err := unix.Flock(fd, unix.LOCK_SH|unix.LOCK_NB); err != nil {
		file.Close()
		return nil, err
	}
	return file, nil
}
