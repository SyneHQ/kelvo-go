//go:build linux || darwin

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

func storePlatformSupported() error { return nil }

// Walk from the filesystem root with O_NOFOLLOW. The configured directory may
// live beneath a public parent such as /tmp, but the store itself must be private.
func storeOpenDirectory(path string, create bool) (*os.File, error) {
	fd, err := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	for _, part := range strings.Split(strings.TrimPrefix(filepath.Clean(path), "/"), "/") {
		if part == "" {
			continue
		}
		next, openErr := storeOpenDirectoryAt(fd, part, create)
		_ = unix.Close(fd)
		if openErr != nil {
			return nil, openErr
		}
		fd = next
	}
	f := os.NewFile(uintptr(fd), path)
	if err := storeCheckPrivate(f, true, false); err != nil {
		_ = f.Close()
		return nil, err
	}
	return f, nil
}

func storeOpenDirectoryAt(fd int, name string, create bool) (int, error) {
	flags := unix.O_RDONLY | unix.O_DIRECTORY | unix.O_NOFOLLOW | unix.O_CLOEXEC
	next, err := unix.Openat(fd, name, flags, 0)
	if errors.Is(err, unix.ENOENT) && create {
		if err = unix.Mkdirat(fd, name, 0700); err != nil && !errors.Is(err, unix.EEXIST) {
			return -1, err
		}
		// Make newly created namespaces durable before placing data inside them.
		if err = unix.Fsync(fd); err != nil {
			return -1, err
		}
		next, err = unix.Openat(fd, name, flags, 0)
	}
	return next, err
}

func storeOpenChildDirectory(parent *os.File, name string, create bool) (*os.File, error) {
	fd, err := storeOpenDirectoryAt(int(parent.Fd()), name, create)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), filepath.Join(parent.Name(), name))
	if err := storeCheckPrivate(f, true, false); err != nil {
		_ = f.Close()
		return nil, err
	}
	return f, nil
}

func storeOpenFile(dir *os.File, name string, flags int, mode os.FileMode) (*os.File, error) {
	fd, err := unix.Openat(int(dir.Fd()), name, flags|unix.O_NOFOLLOW|unix.O_CLOEXEC|unix.O_NONBLOCK, uint32(mode.Perm()))
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), filepath.Join(dir.Name(), name))
	if err := storeCheckPrivate(f, false, false); err != nil {
		_ = f.Close()
		return nil, err
	}
	return f, nil
}

func storeCheckPrivate(f *os.File, directory, immutable bool) error {
	var stat unix.Stat_t
	if err := unix.Fstat(int(f.Fd()), &stat); err != nil {
		return err
	}
	if stat.Uid != uint32(os.Geteuid()) || stat.Mode&0077 != 0 {
		return fmt.Errorf("%w: snapshot files and directories must be private and owned by the worker user", ErrCorrupt)
	}
	want := uint32(unix.S_IFREG)
	if directory {
		want = unix.S_IFDIR
	}
	if uint32(stat.Mode)&unix.S_IFMT != want {
		return fmt.Errorf("%w: unexpected snapshot filesystem object", ErrCorrupt)
	}
	if !directory && stat.Nlink != 1 {
		return fmt.Errorf("%w: hard-linked snapshot file", ErrCorrupt)
	}
	if immutable && stat.Mode&0222 != 0 {
		return fmt.Errorf("%w: published snapshot file is writable", ErrCorrupt)
	}
	return nil
}

func storeTryLock(f *os.File, exclusive bool) (bool, error) {
	operation := unix.LOCK_SH | unix.LOCK_NB
	if exclusive {
		operation = unix.LOCK_EX | unix.LOCK_NB
	}
	err := unix.Flock(int(f.Fd()), operation)
	if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EINTR) {
		return false, nil
	}
	return err == nil, err
}

func storeRename(dir *os.File, from, to string) error {
	return unix.Renameat(int(dir.Fd()), from, int(dir.Fd()), to)
}

// The dataset writer lock serializes this existence check and rename across
// workers. Generation names contain 128 random bits, but still refuse a clash.
func storePublishPayload(dir *os.File, from, to string) error {
	file, err := storeOpenFile(dir, to, os.O_RDONLY, 0)
	if err == nil {
		_ = file.Close()
		return os.ErrExist
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return storeRename(dir, from, to)
}

func storeRemove(dir *os.File, name string) error {
	return unix.Unlinkat(int(dir.Fd()), name, 0)
}
