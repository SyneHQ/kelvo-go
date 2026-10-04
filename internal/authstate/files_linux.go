//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package authstate

import (
	"bytes"
	"errors"
	"io"
	"os"
	"strings"

	"golang.org/x/sys/unix"
)

const stateName = "state.yml"
const lockName = ".writer.lock"
const pendingName = ".state.pending.yml"

type diskIO struct {
	write      func(*os.File, []byte) (int, error)
	sync       func(*os.File) error
	rename     func(*os.File, string, string) error
	close      func(*os.File) error
	checkpoint func(string)
}

func defaultIO() diskIO {
	return diskIO{
		write: func(file *os.File, raw []byte) (int, error) { return file.Write(raw) },
		sync:  func(file *os.File) error { return file.Sync() },
		rename: func(dir *os.File, from, to string) error {
			return unix.Renameat(int(dir.Fd()), from, int(dir.Fd()), to)
		},
		close:      func(file *os.File) error { return file.Close() },
		checkpoint: func(string) {},
	}
}

type diskState struct {
	path                              string
	scope                             Scope
	directory, lock, current, pending *os.File
	state                             Candidate
	stateSize                         int64
	io                                diskIO
}

func checkPrivate(file *os.File, directory bool) (unix.Stat_t, error) {
	var st unix.Stat_t
	if unix.Fstat(int(file.Fd()), &st) != nil {
		return st, ErrUnavailable
	}
	wantType, wantMode := uint32(unix.S_IFREG), uint32(0600)
	if directory {
		wantType, wantMode = unix.S_IFDIR, 0700
	}
	if st.Uid != uint32(os.Geteuid()) || st.Mode&unix.S_IFMT != wantType || st.Mode&07777 != wantMode || (!directory && st.Nlink != 1) {
		return st, ErrUnavailable
	}
	return st, nil
}

// Only the last component may be created, and every ancestor is checked before
// traversal. Descriptor ownership remains with this one worker during syscalls.
func openDirectory(path string, create bool, operations diskIO) (*os.File, error) {
	fd, err := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, ErrUnavailable
	}
	current := os.NewFile(uintptr(fd), "/")
	parts := strings.Split(strings.TrimPrefix(path, "/"), "/")
	for i, name := range parts {
		var st unix.Stat_t
		if unix.Fstat(int(current.Fd()), &st) != nil || st.Mode&unix.S_IFMT != unix.S_IFDIR || (st.Uid != 0 && st.Uid != uint32(os.Geteuid())) || (st.Mode&0022 != 0 && !(st.Uid == 0 && st.Mode&unix.S_ISVTX != 0)) {
			_ = operations.close(current)
			return nil, ErrUnavailable
		}
		next, openErr := unix.Openat(int(current.Fd()), name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if create && i == len(parts)-1 && errors.Is(openErr, unix.ENOENT) {
			if openErr = unix.Mkdirat(int(current.Fd()), name, 0700); openErr == nil {
				if operations.sync(current) != nil {
					_ = operations.close(current)
					return nil, ErrUncertain
				}
				next, openErr = unix.Openat(int(current.Fd()), name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
			}
		}
		closeErr := operations.close(current)
		if openErr != nil {
			return nil, ErrUnavailable
		}
		current = os.NewFile(uintptr(next), path)
		if closeErr != nil {
			_ = operations.close(current)
			return nil, ErrUncertain
		}
	}
	if _, err = checkPrivate(current, true); err != nil {
		_ = operations.close(current)
		return nil, err
	}
	var fs unix.Statfs_t
	if unix.Fstatfs(int(current.Fd()), &fs) != nil {
		_ = operations.close(current)
		return nil, ErrUnavailable
	}
	// Remote and volatile filesystems do not provide this retained local-state
	// contract. A persistent volume is still an operator responsibility.
	switch uint32(fs.Type) {
	case 0x6969, 0xff534d42, 0x517b, 0x65735546, 0x01021994, 0x01021997:
		_ = operations.close(current)
		return nil, ErrUnavailable
	}
	return current, nil
}

func (d *diskState) child(name string, flags int) (*os.File, error) {
	fd, err := unix.Openat(int(d.directory.Fd()), name, flags|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0600)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), name)
	if _, err = checkPrivate(file, false); err != nil {
		_ = d.io.close(file)
		return nil, err
	}
	return file, nil
}

func (d *diskState) named(file *os.File, name string, size int64) bool {
	opened, err := checkPrivate(file, false)
	if err != nil || opened.Size != size {
		return false
	}
	var named unix.Stat_t
	return unix.Fstatat(int(d.directory.Fd()), name, &named, unix.AT_SYMLINK_NOFOLLOW) == nil && named.Dev == opened.Dev && named.Ino == opened.Ino && named.Mode == opened.Mode && named.Uid == opened.Uid && named.Nlink == 1 && named.Size == size
}

func (d *diskState) bound() bool {
	directory, err := openDirectory(d.path, false, d.io)
	if err != nil {
		return false
	}
	want, err := checkPrivate(d.directory, true)
	got, other := checkPrivate(directory, true)
	closed := d.io.close(directory)
	return err == nil && other == nil && closed == nil && want.Dev == got.Dev && want.Ino == got.Ino && d.named(d.lock, lockName, 0) && (d.current == nil || d.named(d.current, stateName, d.stateSize))
}

func (d *diskState) start(initialize bool, candidate Candidate) error {
	var err error
	d.directory, err = openDirectory(d.path, initialize, d.io)
	if err != nil {
		return err
	}
	flags := os.O_RDWR
	if initialize {
		flags |= os.O_CREATE | os.O_EXCL
	}
	d.lock, err = d.child(lockName, flags)
	if err != nil {
		return ErrUnavailable
	}
	if !d.named(d.lock, lockName, 0) || unix.Flock(int(d.lock.Fd()), unix.LOCK_EX|unix.LOCK_NB) != nil {
		return ErrUnavailable
	}
	if !d.bound() {
		return ErrUnavailable
	}
	if initialize {
		var st unix.Stat_t
		if !errors.Is(unix.Fstatat(int(d.directory.Fd()), stateName, &st, unix.AT_SYMLINK_NOFOLLOW), unix.ENOENT) || !errors.Is(unix.Fstatat(int(d.directory.Fd()), pendingName, &st, unix.AT_SYMLINK_NOFOLLOW), unix.ENOENT) {
			return ErrUnavailable
		}
		if d.io.sync(d.lock) != nil || d.io.sync(d.directory) != nil {
			return ErrUncertain
		}
		return d.publish(candidate)
	}
	d.current, err = d.child(stateName, os.O_RDONLY)
	if err != nil {
		return ErrUnavailable
	}
	st, err := checkPrivate(d.current, false)
	if err != nil || st.Size <= 0 || st.Size > stateLimit {
		return ErrUnavailable
	}
	raw, err := io.ReadAll(io.LimitReader(d.current, stateLimit+1))
	if err != nil || int64(len(raw)) != st.Size {
		return ErrUnavailable
	}
	d.state, err = decodeState(raw, d.scope)
	if err != nil {
		return err
	}
	d.stateSize = st.Size
	if !d.bound() {
		return ErrUnavailable
	}
	// Reopening a visible rename is not durability proof. Establish durability
	// again before any no-op admission can authorize the loaded revision.
	if d.io.sync(d.current) != nil || d.io.sync(d.directory) != nil {
		return ErrUncertain
	}
	d.io.checkpoint("opened-durable")
	return d.clearPending()
}

func (d *diskState) clearPending() error {
	file, err := d.child(pendingName, os.O_RDONLY)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return ErrUnavailable
	}
	st, err := checkPrivate(file, false)
	valid := err == nil && st.Size >= 0 && st.Size <= stateLimit && d.named(file, pendingName, st.Size)
	closeErr := d.io.close(file)
	if !valid {
		return ErrUnavailable
	}
	if closeErr != nil || unix.Unlinkat(int(d.directory.Fd()), pendingName, 0) != nil || d.io.sync(d.directory) != nil {
		return ErrUncertain
	}
	return nil
}

func (d *diskState) publish(next Candidate) error {
	raw, err := encodeState(d.scope, next)
	if err != nil {
		return err
	}
	if !d.bound() {
		return ErrUncertain
	}
	d.pending, err = d.child(pendingName, os.O_RDWR|os.O_CREATE|os.O_EXCL)
	if err != nil {
		return ErrUncertain
	}
	d.io.checkpoint("before-write")
	n, err := d.io.write(d.pending, raw)
	if err != nil || n != len(raw) {
		return ErrUncertain
	}
	actual := make([]byte, len(raw))
	if n, err = d.pending.ReadAt(actual, 0); err != nil || n != len(raw) || !bytes.Equal(actual, raw) || !d.named(d.pending, pendingName, int64(len(raw))) {
		return ErrUncertain
	}
	if d.io.sync(d.pending) != nil {
		return ErrUncertain
	}
	d.io.checkpoint("staging-synced")
	if !d.bound() || !d.named(d.pending, pendingName, int64(len(raw))) {
		return ErrUncertain
	}
	if d.io.rename(d.directory, pendingName, stateName) != nil {
		return ErrUncertain
	}
	d.io.checkpoint("renamed")
	if d.io.sync(d.directory) != nil {
		return ErrUncertain
	}
	d.io.checkpoint("directory-synced")
	prior := d.current
	d.current, d.pending = d.pending, nil
	d.state, d.stateSize = next, int64(len(raw))
	if prior != nil && d.io.close(prior) != nil {
		return ErrUncertain
	}
	return nil
}

func (d *diskState) close() error {
	var failure error
	for _, file := range []*os.File{d.pending, d.current, d.lock, d.directory} {
		if file != nil && d.io.close(file) != nil {
			failure = ErrUncertain
		}
	}
	d.pending, d.current, d.lock, d.directory = nil, nil, nil, nil
	return failure
}
