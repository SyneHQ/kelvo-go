//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

const scratchInventoryLimit = 4096
const scratchMutationName = ".kelvo-scratch.lock"
const scratchPrefix = "kelvo-worker-"
const scratchLeasePrefix = ".kelvo-lease-"
const scratchLeaseGrace = time.Second
const scratchLeasePoll = 5 * time.Millisecond

var errScratchUnsafe = errors.New("managed worker scratch ownership is invalid")

// ScratchRoot owns an explicitly configured private Linux directory. It never
// scans a global temporary directory. Local filesystem flock semantics are
// required; this is not distributed object/NFS storage coordination.
type ScratchRoot struct {
	mu       sync.Mutex
	path     string
	fd       int
	root     *os.Root
	mutation *os.File
	active   int
	closed   bool
}

// OpenScratchRoot requires an existing owner-only root and refuses symlinks in
// every path component. Recovery removes only complete versioned ownership
// records whose exclusive lease is no longer held by any parent/worker process.
func OpenScratchRoot(path string) (*ScratchRoot, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || path == "/" {
		return nil, errScratchUnsafe
	}
	fd, err := openScratchRootPath(path)
	if err != nil {
		return nil, errScratchUnsafe
	}
	var st unix.Stat_t
	if unix.Fstat(fd, &st) != nil || !privateDirectory(&st) {
		unix.Close(fd)
		return nil, errScratchUnsafe
	}
	// OpenRoot follows this kernel-created descriptor link to the already
	// verified inode. It does not reopen the caller's mutable pathname.
	root, err := os.OpenRoot(fmt.Sprintf("/proc/self/fd/%d", fd))
	if err != nil {
		unix.Close(fd)
		return nil, errScratchUnsafe
	}
	s := &ScratchRoot{path: path, fd: fd, root: root}
	s.mutation, err = s.openPrivate(scratchMutationName, unix.O_RDWR|unix.O_CREAT, 0o600)
	if err != nil {
		root.Close()
		unix.Close(fd)
		return nil, err
	}
	if _, err = s.Reclaim(); err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}

// Every ancestor must be controlled by root or this service identity. A
// root-owned sticky temporary ancestor is allowed only because the next opened
// component has a trusted owner; ordinary group/world-writable ancestors fail.
func openScratchRootPath(path string) (int, error) {
	fd, err := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, err
	}
	components := strings.Split(strings.TrimPrefix(path, "/"), "/")
	for index, component := range components {
		next, err := unix.Openat2(fd, component, &unix.OpenHow{Flags: unix.O_RDONLY | unix.O_DIRECTORY | unix.O_CLOEXEC,
			Resolve: unix.RESOLVE_BENEATH | unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_MAGICLINKS})
		unix.Close(fd)
		if err != nil {
			return -1, errScratchUnsafe
		}
		fd = next
		var st unix.Stat_t
		if unix.Fstat(fd, &st) != nil || st.Mode&unix.S_IFMT != unix.S_IFDIR ||
			(int(st.Uid) != os.Geteuid() && st.Uid != 0) {
			unix.Close(fd)
			return -1, errScratchUnsafe
		}
		leaf := index == len(components)-1
		stickyTemporaryAncestor := !leaf && st.Uid == 0 && st.Mode&unix.S_ISVTX != 0
		if st.Mode&0o022 != 0 && !stickyTemporaryAncestor || leaf && !privateDirectory(&st) {
			unix.Close(fd)
			return -1, errScratchUnsafe
		}
	}
	return fd, nil
}

func privateDirectory(st *unix.Stat_t) bool {
	return st.Mode&unix.S_IFMT == unix.S_IFDIR && st.Mode&0o777 == 0o700 && int(st.Uid) == os.Geteuid()
}

func (s *ScratchRoot) openPrivate(name string, flags int, mode uint64) (*os.File, error) {
	fd, err := unix.Openat2(s.fd, name, &unix.OpenHow{Flags: uint64(flags | unix.O_CLOEXEC | unix.O_NOFOLLOW),
		Mode: mode, Resolve: unix.RESOLVE_BENEATH | unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_MAGICLINKS})
	if err != nil {
		return nil, err
	}
	var st unix.Stat_t
	if unix.Fstat(fd, &st) != nil || st.Mode&unix.S_IFMT != unix.S_IFREG || st.Mode&0o777 != 0o600 ||
		int(st.Uid) != os.Geteuid() || st.Nlink != 1 {
		unix.Close(fd)
		return nil, errScratchUnsafe
	}
	return os.NewFile(uintptr(fd), name), nil
}

func (s *ScratchRoot) lock() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return errors.New("managed worker scratch is closed")
	}
	if err := unix.Flock(int(s.mutation.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		s.mu.Unlock()
		return errors.New("managed worker scratch maintenance is busy")
	}
	// Caller must not replace/move the configured root while it is in use.
	// Verify the pathname still identifies our opened private inode before
	// handing its name to the launcher. File operations remain fd-relative.
	fd, err := openScratchRootPath(s.path)
	if err == nil {
		var have, current unix.Stat_t
		err = unix.Fstat(s.fd, &have)
		if err == nil {
			err = unix.Fstat(fd, &current)
		}
		if err == nil && (!privateDirectory(&current) || have.Dev != current.Dev || have.Ino != current.Ino) {
			err = errScratchUnsafe
		}
		unix.Close(fd)
	}
	if err != nil {
		s.unlock()
		return errScratchUnsafe
	}
	return nil
}

func (s *ScratchRoot) unlock() {
	_ = unix.Flock(int(s.mutation.Fd()), unix.LOCK_UN)
	s.mu.Unlock()
}

func validScratchID(id string) bool {
	if len(id) != 32 {
		return false
	}
	decoded, err := hex.DecodeString(id)
	return err == nil && hex.EncodeToString(decoded) == id
}

func leaseRecord(id string) string { return "kelvo-worker-scratch-v1\n" + id + "\n" }

func (s *ScratchRoot) inspectNames() ([]string, error) {
	file, err := s.root.Open(".")
	if err != nil {
		return nil, err
	}
	defer file.Close()
	names, err := file.Readdirnames(scratchInventoryLimit + 1)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	if len(names) > scratchInventoryLimit {
		return nil, errors.New("managed worker scratch inventory limit exceeded")
	}
	sort.Strings(names)
	return names, nil
}

// Reclaim skips live ownership locks. Partial/corrupt records and directories
// without valid ownership are retained; errors fail closed for manual review.
func (s *ScratchRoot) Reclaim() (int, error) {
	if err := s.lock(); err != nil {
		return 0, err
	}
	defer s.unlock()
	return s.reclaimLocked()
}

func (s *ScratchRoot) reclaimLocked() (int, error) {
	names, err := s.inspectNames()
	if err != nil {
		return 0, err
	}
	known := make(map[string]bool, len(names))
	for _, name := range names {
		known[name] = true
	}
	// Validate reserved names before deleting entries. Arbitrary files are
	// outside this namespace and preserved; malformed reserved names fail.
	for _, name := range names {
		if strings.HasPrefix(name, scratchPrefix) {
			id := strings.TrimPrefix(name, scratchPrefix)
			if !validScratchID(id) || !known[scratchLeasePrefix+id] {
				return 0, errScratchUnsafe
			}
		}
		if strings.HasPrefix(name, scratchLeasePrefix) && !validScratchID(strings.TrimPrefix(name, scratchLeasePrefix)) {
			return 0, errScratchUnsafe
		}
	}
	removed := 0
	for _, name := range names {
		if !strings.HasPrefix(name, scratchLeasePrefix) {
			continue
		}
		id := strings.TrimPrefix(name, scratchLeasePrefix)
		lease, err := s.openPrivate(name, unix.O_RDWR, 0)
		if err != nil {
			return removed, errScratchUnsafe
		}
		locked := unix.Flock(int(lease.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if errors.Is(locked, unix.EWOULDBLOCK) {
			lease.Close()
			continue
		}
		if locked != nil {
			lease.Close()
			return removed, errScratchUnsafe
		}
		raw, err := io.ReadAll(io.LimitReader(lease, int64(len(leaseRecord(id))+1)))
		if err != nil || string(raw) != leaseRecord(id) {
			lease.Close()
			return removed, errScratchUnsafe
		}
		workspace := scratchPrefix + id
		var st unix.Stat_t
		err = unix.Fstatat(s.fd, workspace, &st, unix.AT_SYMLINK_NOFOLLOW)
		if err != nil && !errors.Is(err, unix.ENOENT) || err == nil && !privateDirectory(&st) {
			lease.Close()
			return removed, errScratchUnsafe
		}
		if err == nil {
			if err = s.root.RemoveAll(workspace); err != nil {
				lease.Close()
				return removed, errors.New("managed worker scratch reclamation failed")
			}
			removed++
		}
		err = s.root.Remove(name)
		// Close only: LOCK_UN would release the open-description lock inherited
		// by a still-running child. Reclaim has its own independent description.
		lease.Close()
		if err != nil {
			return removed, errors.New("managed worker scratch record removal failed")
		}
	}
	return removed, nil
}

func (s *ScratchRoot) allocate() (*scratchWorkspace, error) {
	if err := s.lock(); err != nil {
		return nil, err
	}
	defer s.unlock()
	if _, err := s.reclaimLocked(); err != nil {
		return nil, err
	}
	names, err := s.inspectNames()
	if err != nil || len(names)+2 > scratchInventoryLimit {
		return nil, errors.New("managed worker scratch inventory limit exceeded")
	}
	var token [16]byte
	if _, err = rand.Read(token[:]); err != nil {
		return nil, err
	}
	id := hex.EncodeToString(token[:])
	name, record := scratchPrefix+id, scratchLeasePrefix+id
	lease, err := s.openPrivate(record, unix.O_RDWR|unix.O_CREAT|unix.O_EXCL, 0o600)
	if err != nil {
		return nil, errScratchUnsafe
	}
	complete, workspaceCreated := false, false
	defer func() {
		if !complete {
			lease.Close()
			if workspaceCreated {
				_ = s.root.RemoveAll(name)
			}
			_ = s.root.Remove(record)
		}
	}()
	if unix.Flock(int(lease.Fd()), unix.LOCK_EX|unix.LOCK_NB) != nil {
		return nil, errScratchUnsafe
	}
	if _, err = io.WriteString(lease, leaseRecord(id)); err != nil {
		return nil, err
	}
	if err = lease.Sync(); err != nil {
		return nil, err
	}
	if err = unix.Mkdirat(s.fd, name, 0o700); err != nil {
		return nil, err
	}
	workspaceCreated = true
	s.active++
	complete = true
	return &scratchWorkspace{path: filepath.Join(s.path, name), lease: lease,
		cleanup: func() error { return s.release(name, record, lease) }}, nil
}

func (s *ScratchRoot) release(name, record string, lease *os.File) (resultErr error) {
	// Carry a fixed diagnostic stage to the caller, which drains admission
	// before writing logs. Logging backpressure must not delay quarantine.
	stage := "root_lock"
	defer func() {
		if resultErr != nil {
			resultErr = &scratchCleanupError{stage: stage, cause: resultErr}
		}
	}()
	if err := s.lock(); err != nil {
		// The complete ownership record remains for later recovery. Never
		// remove paths without the root mutation lock.
		lease.Close()
		s.mu.Lock()
		s.active--
		s.mu.Unlock()
		return err
	}
	defer s.unlock()
	defer func() { s.active-- }()
	// Drop only the parent's reference. A descendant might still retain its
	// inherited descriptor after its leader has exited and Wait has returned.
	// Acquire a NEW description: our old shared lock cannot prove child exit.
	stage = "parent_lease_close"
	if err := lease.Close(); err != nil {
		return errors.New("managed worker scratch parent lease closure failed")
	}
	stage = "lease_reopen"
	independent, err := s.openPrivate(record, unix.O_RDWR, 0)
	if err != nil {
		return errScratchUnsafe
	}
	defer independent.Close()
	stage = "child_lease_lock"
	waited, err := acquireScratchLease(independent)
	if err != nil {
		if errors.Is(err, unix.EWOULDBLOCK) {
			stage = "child_lease_held"
		}
		return errors.New("managed worker scratch child lease remains active")
	}
	if waited {
		// An unrelated concurrent fork can briefly inherit a CLOEXEC lease
		// before exec closes it. While waiting, retain the root mutation lock
		// and all ownership files; never adopt a replacement lease pathname.
		stage = "lease_revalidate"
		current, err := s.openPrivate(record, unix.O_RDWR, 0)
		if err != nil {
			return errScratchUnsafe
		}
		var held, named unix.Stat_t
		valid := unix.Fstat(int(independent.Fd()), &held) == nil && unix.Fstat(int(current.Fd()), &named) == nil &&
			held.Dev == named.Dev && held.Ino == named.Ino
		current.Close()
		if !valid {
			return errScratchUnsafe
		}
	}
	id := strings.TrimPrefix(name, scratchPrefix)
	stage = "lease_record_read"
	raw, err := io.ReadAll(io.LimitReader(independent, int64(len(leaseRecord(id))+1)))
	if err != nil || string(raw) != leaseRecord(id) {
		return errScratchUnsafe
	}
	var st unix.Stat_t
	stage = "workspace_verify"
	if unix.Fstatat(s.fd, name, &st, unix.AT_SYMLINK_NOFOLLOW) != nil || !privateDirectory(&st) {
		return errScratchUnsafe
	}
	stage = "workspace_remove"
	if err := s.root.RemoveAll(name); err != nil {
		return errors.New("managed worker scratch cleanup failed")
	}
	stage = "lease_record_remove"
	return s.root.Remove(record)
}

// acquireScratchLease never releases an inherited open-description lock. The
// common path is one nonblocking flock. Only contention receives a monotonic,
// bounded grace period; every other kernel error fails immediately.
func acquireScratchLease(file *os.File) (bool, error) {
	err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
	if !errors.Is(err, unix.EWOULDBLOCK) {
		return false, err
	}
	deadline := time.Now().Add(scratchLeaseGrace)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return true, err
		}
		time.Sleep(min(remaining, scratchLeasePoll))
		if time.Until(deadline) <= 0 {
			return true, err
		}
		err = unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if !errors.Is(err, unix.EWOULDBLOCK) {
			return true, err
		}
	}
}

// Close requires all local executors to have finished. A child inherits another
// reference to its lease description and remains protected across parent death.
func (s *ScratchRoot) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	if s.active != 0 {
		return errors.New("managed worker scratch still has active executors")
	}
	s.closed = true
	return errors.Join(s.mutation.Close(), s.root.Close(), unix.Close(s.fd))
}
