//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package exports

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
	"golang.org/x/sys/unix"
)

func randomID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}

// These descriptor-relative checks follow the local snapshot store's safety
// contract; they do not protect against the effective UID/administrator.
func checkFile(f *os.File, directory, immutable bool) (unix.Stat_t, error) {
	var st unix.Stat_t
	if err := unix.Fstat(int(f.Fd()), &st); err != nil {
		return st, err
	}
	want := uint32(unix.S_IFREG)
	if directory {
		want = unix.S_IFDIR
	}
	if st.Uid != uint32(os.Geteuid()) || st.Mode&0077 != 0 || st.Mode&unix.S_IFMT != want || (!directory && st.Nlink != 1) || (immutable && st.Mode&0222 != 0) {
		return st, ErrCorrupt
	}
	return st, nil
}

func openRoot(path string) (*os.File, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || path == "/" || len(path) > 4096 {
		return nil, ErrInvalid
	}
	fd, err := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	for _, name := range strings.Split(strings.TrimPrefix(path, "/"), "/") {
		if err = trustedAncestor(fd); err != nil {
			unix.Close(fd)
			return nil, err
		}
		next, e := unix.Openat(fd, name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if errors.Is(e, unix.ENOENT) {
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
			return nil, e
		}
		fd = next
	}
	f := os.NewFile(uintptr(fd), path)
	if _, err = checkFile(f, true, false); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

func trustedAncestor(fd int) error {
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		return err
	}
	if st.Mode&unix.S_IFMT != unix.S_IFDIR || (st.Uid != 0 && st.Uid != uint32(os.Geteuid())) {
		return ErrCorrupt
	}
	if st.Mode&0022 != 0 && !(st.Uid == 0 && st.Mode&unix.S_ISVTX != 0) {
		return ErrCorrupt
	}
	return nil
}

func childDir(parent *os.File, name string, create bool) (*os.File, error) {
	if create {
		if err := unix.Mkdirat(int(parent.Fd()), name, 0700); err != nil {
			return nil, err
		}
		if err := parent.Sync(); err != nil {
			return nil, err
		}
	}
	fd, err := unix.Openat(int(parent.Fd()), name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), filepath.Join(parent.Name(), name))
	if _, err = checkFile(f, true, false); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

func openFile(dir *os.File, name string, flags int, immutable bool) (*os.File, error) {
	fd, err := unix.Openat(int(dir.Fd()), name, flags|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0600)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), filepath.Join(dir.Name(), name))
	if _, err = checkFile(f, false, immutable); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

func sameStat(a, b unix.Stat_t) bool {
	return a.Dev == b.Dev && a.Ino == b.Ino && a.Size == b.Size && a.Mtim == b.Mtim && a.Ctim == b.Ctim
}
func readFile(dir *os.File, name string, limit int, immutable bool) ([]byte, error) {
	f, err := openFile(dir, name, os.O_RDONLY, immutable)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	before, err := checkFile(f, false, immutable)
	if err != nil || before.Size < 0 || before.Size > int64(limit) {
		return nil, ErrCorrupt
	}
	raw, err := io.ReadAll(io.LimitReader(f, int64(limit)+1))
	if err != nil {
		return nil, err
	}
	after, err := checkFile(f, false, immutable)
	if err != nil || len(raw) > limit || int64(len(raw)) != before.Size || !sameStat(before, after) {
		return nil, ErrCorrupt
	}
	return raw, nil
}

func strictYAML(raw []byte, target any) error {
	var node yaml.Node
	if err := yaml.Unmarshal(raw, &node); err != nil {
		return ErrCorrupt
	}
	nodes := 0
	var visit func(*yaml.Node, int) bool
	visit = func(n *yaml.Node, depth int) bool {
		nodes++
		if nodes > 8192 || depth > 16 || n.Anchor != "" || n.Kind == yaml.AliasNode || n.Tag == "!!merge" {
			return false
		}
		for _, child := range n.Content {
			if !visit(child, depth+1) {
				return false
			}
		}
		return true
	}
	if !visit(&node, 0) {
		return ErrCorrupt
	}
	d := yaml.NewDecoder(bytes.NewReader(raw))
	d.KnownFields(true)
	if err := d.Decode(target); err != nil {
		return ErrCorrupt
	}
	if err := d.Decode(new(any)); !errors.Is(err, io.EOF) {
		return ErrCorrupt
	}
	return nil
}

// Fully write and fsync an unnamed inode before any name can survive a crash.
// State replacement uses one deterministic, fully validated recovery slot.
// Other metadata is published directly without replacement. There are no
// partially written named root temporary files to guess ownership of.
func atomicWrite(dir *os.File, name string, raw []byte, immutable, noReplace bool) (bool, error) {
	return atomicWriteWithHook(dir, name, raw, immutable, noReplace, nil)
}
func atomicWriteWithHook(dir *os.File, name string, raw []byte, immutable, noReplace bool, hook func(string) error) (published bool, err error) {
	point := func(stage string) error {
		if hook != nil {
			return hook(name + ":" + stage)
		}
		return nil
	}
	fd, err := unix.Openat(int(dir.Fd()), ".", unix.O_RDWR|unix.O_TMPFILE|unix.O_CLOEXEC, 0600)
	if err != nil {
		return false, err
	}
	f := os.NewFile(uintptr(fd), "unnamed-export-metadata")
	defer f.Close()
	if n, e := f.Write(raw); e != nil {
		return false, e
	} else if n != len(raw) {
		return false, io.ErrShortWrite
	}
	if immutable {
		if err = f.Chmod(0400); err != nil {
			return false, err
		}
	}
	if err = point("file_sync"); err == nil {
		err = f.Sync()
	}
	if err != nil {
		return false, err
	}
	target := name
	if !noReplace {
		if name != "state.yml" {
			return false, ErrInvalid
		}
		target = ".state.next.yml"
	}
	if err = unix.Linkat(int(f.Fd()), "", int(dir.Fd()), target, unix.AT_EMPTY_PATH); err != nil {
		return false, err
	}
	defer func() {
		if !published && !noReplace {
			unix.Unlinkat(int(dir.Fd()), target, 0)
		}
	}()
	if !noReplace {
		if err = point("staged"); err != nil {
			return false, err
		}
		if err = unix.Renameat(int(dir.Fd()), target, int(dir.Fd()), name); err != nil {
			return false, err
		}
	}
	published = true
	if err = point("dir_sync"); err == nil {
		err = dir.Sync()
	}
	return published, err
}

func tryLease(dir *os.File, exclusive bool) (*os.File, error) {
	f, err := openFile(dir, ".lease", os.O_RDWR|os.O_CREATE, false)
	if err != nil {
		return nil, err
	}
	op := unix.LOCK_SH | unix.LOCK_NB
	if exclusive {
		op = unix.LOCK_EX | unix.LOCK_NB
	}
	if err = unix.Flock(int(f.Fd()), op); err != nil {
		f.Close()
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return nil, ErrBusy
		}
		return nil, err
	}
	return f, nil
}

func lockRoot(ctx context.Context, root *os.File) (*os.File, error) {
	f, err := openFile(root, ".lock", os.O_RDWR|os.O_CREATE, false)
	if err != nil {
		return nil, err
	}
	for {
		if err = ctx.Err(); err != nil {
			f.Close()
			return nil, err
		}
		err = unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			return f, nil
		}
		if !errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EAGAIN) && !errors.Is(err, unix.EINTR) {
			f.Close()
			return nil, err
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			f.Close()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func directoryNames(dir *os.File, limit int) ([]string, error) {
	fd, err := unix.Openat(int(dir.Fd()), ".", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), dir.Name())
	defer f.Close()
	names, err := f.Readdirnames(limit + 1)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	if len(names) > limit {
		return nil, ErrLimit
	}
	return names, nil
}
