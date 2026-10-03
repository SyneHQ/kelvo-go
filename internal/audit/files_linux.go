//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package audit

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

const (
	journalName = "journal.bin"
	lockName    = ".writer.lock"
)

// The effective UID and host administrator remain trusted. No path component
// follows a symlink, and writable ancestry is rejected before child creation.
func openDirectory(path string, create bool) (*os.File, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || path == "/" || len(path) > 4096 {
		return nil, ErrInvalid
	}
	fd, err := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, ErrUnavailable
	}
	for _, name := range strings.Split(strings.TrimPrefix(path, "/"), "/") {
		var st unix.Stat_t
		if unix.Fstat(fd, &st) != nil || st.Mode&unix.S_IFMT != unix.S_IFDIR || (st.Uid != 0 && st.Uid != uint32(os.Geteuid())) || (st.Mode&0022 != 0 && !(st.Uid == 0 && st.Mode&unix.S_ISVTX != 0)) {
			unix.Close(fd)
			return nil, ErrCorrupt
		}
		next, e := unix.Openat(fd, name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if create && errors.Is(e, unix.ENOENT) {
			e = unix.Mkdirat(fd, name, 0700)
			if e == nil || errors.Is(e, unix.EEXIST) {
				e = unix.Fsync(fd)
				if e == nil {
					next, e = unix.Openat(fd, name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
				}
			}
		}
		unix.Close(fd)
		if e != nil {
			return nil, ErrUnavailable
		}
		fd = next
	}
	f := os.NewFile(uintptr(fd), path)
	if _, err = checkFile(f, true); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

func checkFile(file *os.File, directory bool) (unix.Stat_t, error) {
	var st unix.Stat_t
	if unix.Fstat(int(file.Fd()), &st) != nil {
		return st, ErrUnavailable
	}
	want := uint32(unix.S_IFREG)
	if directory {
		want = unix.S_IFDIR
	}
	if st.Uid != uint32(os.Geteuid()) || st.Mode&0077 != 0 || st.Mode&unix.S_IFMT != want || (!directory && st.Nlink != 1) {
		return st, ErrCorrupt
	}
	return st, nil
}

func openChild(directory *os.File, name string, flags int) (*os.File, error) {
	fd, err := unix.Openat(int(directory.Fd()), name, flags|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0600)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), name)
	if _, err = checkFile(f, false); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

func readExact(file *os.File, raw []byte, offset int64) error {
	n, err := file.ReadAt(raw, offset)
	if err != nil || n != len(raw) {
		return ErrCorrupt
	}
	return nil
}

func writeExact(file *os.File, raw []byte, offset int64) error {
	n, err := file.WriteAt(raw, offset)
	if err != nil {
		return err
	}
	if n != len(raw) {
		return io.ErrShortWrite
	}
	return nil
}

// One immutable lifetime lock protects writer ownership. It remains open until
// the sole I/O worker has returned from every write/sync, including late ones.
func lockWriter(directory *os.File, reader bool) (*os.File, error) {
	flags := os.O_RDWR | os.O_CREATE
	if reader {
		flags = os.O_RDONLY
	}
	f, err := openChild(directory, lockName, flags)
	if err != nil {
		return nil, ErrUnavailable
	}
	if st, err := checkFile(f, false); err != nil || st.Size != 0 {
		f.Close()
		return nil, ErrCorrupt
	}
	lock := unix.LOCK_EX | unix.LOCK_NB
	if reader {
		lock = unix.LOCK_SH | unix.LOCK_NB
	}
	if err = unix.Flock(int(f.Fd()), lock); err != nil {
		f.Close()
		if errors.Is(err, unix.EWOULDBLOCK) {
			return nil, ErrBusy
		}
		return nil, ErrUnavailable
	}
	if !reader && directory.Sync() != nil {
		f.Close()
		return nil, ErrUncertain
	}
	return f, nil
}
