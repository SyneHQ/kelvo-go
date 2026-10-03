//go:build linux || darwin

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package secrets

import (
	"bytes"
	"context"
	"io"
	"os"
	"strings"

	"golang.org/x/sys/unix"
)

func supported() error { return nil }

// Walk from an opened root directory using openat and NOFOLLOW on every
// component. Renaming an ancestor cannot redirect a subsequent path lookup.
func readPrivateFile(ctx context.Context, path string) ([]byte, error) {
	return readPrivateDocument(ctx, path, MaxValueBytes)
}

func readPrivateDocument(ctx context.Context, path string, limit int) (value []byte, err error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	directory, err := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, ErrUnavailable
	}
	defer func() { unix.Close(directory) }()
	parts := strings.Split(strings.TrimPrefix(path, "/"), "/")
	for _, part := range parts[:len(parts)-1] {
		next, openErr := unix.Openat(directory, part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if openErr != nil {
			return nil, ErrUnavailable
		}
		unix.Close(directory)
		directory = next
		var info unix.Stat_t
		if unix.Fstat(directory, &info) != nil {
			return nil, ErrUnavailable
		}
		// Writable root-owned sticky ancestors (/tmp) are allowed: sticky rules
		// plus private, owned descendants protect the eventual secret file.
		trusted := info.Uid == uint32(os.Geteuid()) || info.Uid == 0
		writable := info.Mode&0022 != 0
		stickyRoot := info.Uid == 0 && info.Mode&unix.S_ISVTX != 0
		if !trusted || (writable && !stickyRoot) {
			return nil, ErrUnavailable
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
	fd, err := unix.Openat(directory, parts[len(parts)-1], unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, ErrUnavailable
	}
	file := os.NewFile(uintptr(fd), "secret")
	if file == nil {
		unix.Close(fd)
		return nil, ErrUnavailable
	}
	defer file.Close()
	var before unix.Stat_t
	if unix.Fstat(fd, &before) != nil || before.Mode&unix.S_IFMT != unix.S_IFREG || before.Mode&0077 != 0 || before.Uid != uint32(os.Geteuid()) || before.Nlink > 1 || before.Size < 0 || before.Size > int64(limit) {
		return nil, ErrUnavailable
	}
	value, err = io.ReadAll(io.LimitReader(file, int64(limit)+1))
	if err != nil || len(value) > limit || int64(len(value)) != before.Size || bytes.IndexByte(value, 0) >= 0 {
		wipe(value)
		return nil, ErrUnavailable
	}
	var after unix.Stat_t
	if unix.Fstat(fd, &after) != nil {
		wipe(value)
		return nil, ErrUnavailable
	}
	// Atomic replacement can unlink the already-open old inode. It remains a
	// complete valid read; permit that link/ctime change while still rejecting
	// new hard links, permission changes or in-place content modification.
	replaced := before.Nlink == 1 && after.Nlink == 0
	if after.Mode != before.Mode || after.Uid != before.Uid || (after.Nlink != before.Nlink && !replaced) || after.Size != before.Size || after.Mtim != before.Mtim || (after.Ctim != before.Ctim && !replaced) {
		wipe(value)
		return nil, ErrUnavailable
	}
	if err := ctx.Err(); err != nil {
		wipe(value)
		return nil, err
	}
	return value, nil
}
