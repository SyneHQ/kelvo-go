//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package containment

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/sys/unix"
)

const cgroupMagic = 0x63677270
const controlLimit = 64 << 10
const groupPrefix = "kelvo-job-"

type tree struct {
	closeOnce sync.Once
	closeErr  error
	fd        int
	root      *os.Root
	device    uint64
	inode     uint64
}

func openTree(path string, private, cgroup bool) (*tree, error) {
	fd, err := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, ErrOwnership
	}
	parts := strings.Split(strings.TrimPrefix(path, "/"), "/")
	for i, part := range parts {
		next, err := unix.Openat2(fd, part, &unix.OpenHow{Flags: unix.O_RDONLY | unix.O_DIRECTORY | unix.O_CLOEXEC,
			Resolve: unix.RESOLVE_BENEATH | unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_MAGICLINKS})
		unix.Close(fd)
		if err != nil {
			return nil, ErrOwnership
		}
		fd = next
		var st unix.Stat_t
		if unix.Fstat(fd, &st) != nil || st.Mode&unix.S_IFMT != unix.S_IFDIR || (st.Uid != 0 && int(st.Uid) != os.Geteuid()) {
			unix.Close(fd)
			return nil, ErrOwnership
		}
		leaf := i == len(parts)-1
		stickyAncestor := !leaf && st.Uid == 0 && st.Mode&unix.S_ISVTX != 0
		if (st.Mode&0o022 != 0 && !stickyAncestor) || (leaf && int(st.Uid) != os.Geteuid()) ||
			(leaf && private && st.Mode&0o777 != 0o700) {
			unix.Close(fd)
			return nil, ErrOwnership
		}
	}
	var fs unix.Statfs_t
	var st unix.Stat_t
	if unix.Fstatfs(fd, &fs) != nil || unix.Fstat(fd, &st) != nil {
		unix.Close(fd)
		return nil, ErrOwnership
	}
	if cgroup && fs.Type != cgroupMagic {
		unix.Close(fd)
		return nil, ErrUnsupported
	}
	// Private ownership state needs local filesystem locks and durable intents.
	// Unknown/network filesystems are deliberately unsupported.
	if private && fs.Type != 0xef53 && fs.Type != 0x58465342 && fs.Type != 0x9123683e && fs.Type != 0x1021994 && fs.Type != 0x794c7630 {
		unix.Close(fd)
		return nil, ErrUnsupported
	}
	root, err := os.OpenRoot(fmt.Sprintf("/proc/self/fd/%d", fd))
	if err != nil {
		unix.Close(fd)
		return nil, ErrOwnership
	}
	return &tree{fd: fd, root: root, device: uint64(st.Dev), inode: st.Ino}, nil
}

func (t *tree) close() error {
	if t == nil {
		return nil
	}
	t.closeOnce.Do(func() { t.closeErr = errors.Join(t.root.Close(), unix.Close(t.fd)) })
	return t.closeErr
}

func openAt(fd int, name string, flags int, mode uint64) (*os.File, error) {
	opened, err := unix.Openat2(fd, name, &unix.OpenHow{Flags: uint64(flags | unix.O_CLOEXEC | unix.O_NOFOLLOW), Mode: mode,
		Resolve: unix.RESOLVE_BENEATH | unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_MAGICLINKS})
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(opened), name), nil
}

func privateFile(t *tree, name string, flags int) (*os.File, error) {
	mode := uint64(0)
	if flags&unix.O_CREAT != 0 {
		mode = 0o600
	}
	file, err := openAt(t.fd, name, flags, mode)
	if err != nil {
		return nil, err
	}
	var st unix.Stat_t
	if unix.Fstat(int(file.Fd()), &st) != nil || st.Mode&unix.S_IFMT != unix.S_IFREG || st.Mode&0o777 != 0o600 ||
		int(st.Uid) != os.Geteuid() || st.Nlink != 1 {
		file.Close()
		return nil, ErrOwnership
	}
	return file, nil
}

func readFile(fd int, name string, limit int64) (string, error) {
	file, err := openAt(fd, name, unix.O_RDONLY, 0)
	if err != nil {
		return "", err
	}
	defer file.Close()
	body, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil || int64(len(body)) > limit {
		return "", ErrUnavailable
	}
	return string(body), nil
}

func writeControl(fd int, name, value string) error {
	file, err := openAt(fd, name, unix.O_WRONLY, 0)
	if err != nil {
		return err
	}
	defer file.Close()
	written, err := io.WriteString(file, value)
	if err != nil {
		return err
	}
	if written != len(value) {
		return io.ErrShortWrite
	}
	return nil
}

func readPrivate(t *tree, name string) (string, error) {
	file, err := privateFile(t, name, unix.O_RDONLY)
	if err != nil {
		return "", err
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, 1025))
	if err != nil || len(raw) > 1024 {
		return "", ErrOwnership
	}
	return string(raw), nil
}

func writeNewPrivate(t *tree, name, value string) (bool, error) {
	return writeNewPrivateWithHook(t, name, value, nil)
}

// Publish only a complete fsynced anonymous inode. A killed writer cannot leave
// a named partial intent/ownership record. No-replace link is the publication
// boundary; a later directory-fsync failure returns published=true explicitly.
func writeNewPrivateWithHook(t *tree, name, value string, hook func(string) error) (bool, error) {
	fd, err := unix.Openat(t.fd, ".", unix.O_RDWR|unix.O_TMPFILE|unix.O_CLOEXEC, 0o600)
	if err != nil {
		return false, err
	}
	file := os.NewFile(uintptr(fd), "anonymous containment ownership")
	defer file.Close()
	cut := func(stage string) error {
		if hook != nil {
			return hook(stage)
		}
		return nil
	}
	if err = cut("before_write"); err != nil {
		return false, err
	}
	if written, err := io.WriteString(file, value); err != nil || written != len(value) {
		if err != nil {
			return false, err
		}
		return false, io.ErrShortWrite
	}
	if err = cut("after_write"); err != nil {
		return false, err
	}
	if err = file.Sync(); err != nil {
		return false, err
	}
	if err = cut("after_file_sync"); err != nil {
		return false, err
	}
	if err = unix.Linkat(fd, "", t.fd, name, unix.AT_EMPTY_PATH); err != nil {
		return false, err
	}
	if err = cut("after_link"); err != nil {
		return true, err
	}
	if err = unix.Fsync(t.fd); err != nil {
		return true, err
	}
	if err = cut("after_directory_sync"); err != nil {
		return true, err
	}
	return true, nil
}

func names(t *tree, limit int) ([]os.DirEntry, error) {
	file, err := t.root.Open(".")
	if err != nil {
		return nil, err
	}
	defer file.Close()
	entries, err := file.ReadDir(limit + 1)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	if len(entries) > limit {
		return nil, ErrUnavailable
	}
	return entries, nil
}

func number(raw string) (uint64, error) {
	value := strings.TrimSuffix(raw, "\n")
	if value == "" {
		return 0, ErrUnavailable
	}
	for _, c := range value {
		if c < '0' || c > '9' {
			return 0, ErrUnavailable
		}
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, ErrUnavailable
	}
	return parsed, nil
}

func counters(raw string) (map[string]uint64, error) {
	result := map[string]uint64{}
	if len(raw) > controlLimit {
		return nil, ErrUnavailable
	}
	for _, line := range strings.Split(strings.TrimSuffix(raw, "\n"), "\n") {
		parts := strings.Fields(line)
		if len(parts) != 2 {
			return nil, ErrUnavailable
		}
		if _, exists := result[parts[0]]; exists {
			return nil, ErrUnavailable
		}
		value, err := number(parts[1])
		if err != nil {
			return nil, err
		}
		result[parts[0]] = value
	}
	return result, nil
}

func validID(id string) bool {
	if len(id) != 32 {
		return false
	}
	for _, c := range id {
		if !(c >= '0' && c <= '9') && !(c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

func safeName(name string) bool {
	return strings.HasPrefix(name, groupPrefix) && validID(strings.TrimPrefix(name, groupPrefix)) && filepath.Base(name) == name
}

type record struct {
	Name                              string
	RootDevice, RootInode, GroupInode uint64
}

func (r record) encode() string {
	return fmt.Sprintf("kelvo-cgroup-v1\n%s\n%d\n%d\n%d\n", r.Name, r.RootDevice, r.RootInode, r.GroupInode)
}

func decodeRecord(raw string) (record, error) {
	lines := strings.Split(raw, "\n")
	if len(lines) != 6 || lines[0] != "kelvo-cgroup-v1" || !safeName(lines[1]) || lines[5] != "" {
		return record{}, ErrOwnership
	}
	device, e1 := number(lines[2])
	root, e2 := number(lines[3])
	group, e3 := number(lines[4])
	if e1 != nil || e2 != nil || e3 != nil || root == 0 {
		return record{}, ErrOwnership
	}
	return record{Name: lines[1], RootDevice: device, RootInode: root, GroupInode: group}, nil
}
