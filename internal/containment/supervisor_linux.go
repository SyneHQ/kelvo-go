//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package containment

import (
	"context"
	"errors"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// ChildSpec supplies explicit inherited files, starting with stdin/stdout/stderr.
// The caller owns these files and must keep them open until Spawn returns. The
// supervisor duplicates them under their descriptor locks and closes its copies
// after ForkExec. It never starts hidden input/output copying goroutines.
// Duplicates share offsets and status flags, including O_NONBLOCK, with the
// caller's files. The caller must prepare any blocking child pipe endpoints
// before Spawn and must not change their flags while a child can use them.
// Nil files are allowed only after stderr and preserve intentionally closed
// optional descriptor slots; later descriptors keep their original numbers.
type ChildSpec struct {
	Path  string
	Args  []string
	Env   []string
	Dir   string
	Files []*os.File
}

// SupervisedChild exposes one exit future. Cancelling Wait does not terminate
// the child or release any operation capacity. Signal uses its pinned pidfd.
type SupervisedChild struct {
	owner   *NamespaceSupervisor
	pid     int
	pidfdMu sync.Mutex
	pidfd   int
	done    chan struct{}
	exit    ChildExit
}

type SupervisorState struct {
	Registered  int
	Reaped      uint64
	Adopted     uint64
	Nonterminal uint64
	Draining    bool
	Closed      bool
}

// NamespaceSupervisor is an internal PID-1 primitive, not a selectable executor.
// It requires exclusive process creation and wait ownership for this boot. It
// does not implement operation admission, output custody, or crash recovery.
// Root/runtime administration stays outside this workload enforcement boundary.
type NamespaceSupervisor struct {
	mu             sync.Mutex
	limits         *ContainerLimits
	policy         Limits
	cleanupTimeout time.Duration
	children       map[int]*SupervisedChild
	reaped         uint64
	adopted        uint64
	nonterminal    uint64
	draining       bool
	closed         bool
	err            error
	empty          bool
	signals        chan os.Signal
	wake           chan struct{}
	stop           chan struct{}
	stopped        chan struct{}
	closeOnce      sync.Once
	closeDone      chan struct{}
	closeRequested atomic.Bool
	quiescing      atomic.Bool
	closeErr       error
}

var namespaceSupervisorClaimed atomic.Bool

// OpenNamespaceSupervisor verifies a private, initially empty PID-1 namespace
// and read-only finite cgroup controls before taking process-wide reap ownership.
// Only one supervisor can run in a worker boot. No worker configuration calls it.
func OpenNamespaceSupervisor(policy Limits, cleanupTimeout time.Duration) (_ *NamespaceSupervisor, resultErr error) {
	if cleanupTimeout < 100*time.Millisecond || cleanupTimeout > 30*time.Second {
		return nil, ErrInvalid
	}
	if !namespaceSupervisorClaimed.CompareAndSwap(false, true) {
		return nil, ErrOwnership
	}
	defer func() {
		if resultErr != nil {
			namespaceSupervisorClaimed.Store(false)
		}
	}()
	limits, err := OpenContainerLimits(policy)
	if err != nil {
		return nil, err
	}
	defer func() {
		if resultErr != nil {
			_ = limits.Close()
		}
	}()
	s := &NamespaceSupervisor{limits: limits, policy: policy, cleanupTimeout: cleanupTimeout,
		children: make(map[int]*SupervisedChild), signals: make(chan os.Signal, 1), wake: make(chan struct{}, 1),
		stop: make(chan struct{}), stopped: make(chan struct{}), closeDone: make(chan struct{})}
	ids, err := s.processIDs()
	if err != nil || len(ids) != 1 || ids[0] != 1 {
		return nil, ErrOwnership
	}
	fd, err := unix.PidfdOpen(1, 0)
	if err != nil {
		return nil, ErrUnsupported
	}
	err = unix.PidfdSendSignal(fd, 0, nil, 0)
	_ = unix.Close(fd)
	if err != nil {
		return nil, ErrUnsupported
	}
	signal.Notify(s.signals, syscall.SIGCHLD)
	go s.reapLoop()
	return s, nil
}

func validChildSpec(spec ChildSpec) bool {
	if !filepath.IsAbs(spec.Path) || !filepath.IsAbs(spec.Dir) || len(spec.Args) == 0 || len(spec.Args) > 256 ||
		len(spec.Env) > 1024 || len(spec.Files) < 3 || len(spec.Files) > 256 {
		return false
	}
	total := 0
	for _, values := range [][]string{{spec.Path, spec.Dir}, spec.Args, spec.Env} {
		for _, value := range values {
			total += len(value)
			if total > 1<<20 || strings.IndexByte(value, 0) >= 0 {
				return false
			}
		}
	}
	return true
}

func duplicateChildFiles(files []*os.File) (_ []uintptr, resultErr error) {
	var descriptors []uintptr
	defer func() {
		if resultErr != nil {
			closeChildFiles(descriptors)
		}
	}()
	for index, file := range files {
		if file == nil {
			if index < 3 {
				return nil, ErrInvalid
			}
			descriptors = append(descriptors, ^uintptr(0))
			continue
		}
		raw, err := file.SyscallConn()
		if err != nil {
			return nil, ErrUnavailable
		}
		fd := -1
		var duplicateErr error
		if err := raw.Control(func(original uintptr) { fd, duplicateErr = unix.FcntlInt(original, unix.F_DUPFD_CLOEXEC, 3) }); err != nil || duplicateErr != nil {
			if fd >= 0 {
				_ = unix.Close(fd)
			}
			return nil, ErrUnavailable
		}
		descriptors = append(descriptors, uintptr(fd))
	}
	return descriptors, nil
}

func closeChildFiles(files []uintptr) {
	for _, fd := range files {
		if fd != ^uintptr(0) {
			_ = unix.Close(int(fd))
		}
	}
}

func (s *NamespaceSupervisor) Spawn(ctx context.Context, spec ChildSpec) (*SupervisedChild, error) {
	return s.spawn(ctx, spec, nil)
}

// Native qualification can hold the registration boundary or fail pidfd_open
// after actual ForkExec. Production Spawn never supplies these hooks.
type spawnHooks struct {
	beforeRegistration func(int)
	openPIDFD          func(int, int) (int, error)
}

func (s *NamespaceSupervisor) spawn(ctx context.Context, spec ChildSpec, hooks *spawnHooks) (*SupervisedChild, error) {
	if s == nil || ctx == nil || !validChildSpec(spec) {
		return nil, ErrInvalid
	}
	files, err := duplicateChildFiles(spec.Files)
	if err != nil {
		return nil, err
	}
	defer closeChildFiles(files)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closeRequested.Load() || s.quiescing.Load() || s.draining || s.closed || s.err != nil {
		return nil, ErrDraining
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if int64(len(s.children)) >= s.policy.MaxProcesses-1 {
		return nil, ErrUnavailable
	}
	if _, err := s.limits.Check(s.policy); err != nil {
		s.draining, s.err = true, ErrOwnership
		return nil, ErrOwnership
	}
	// ForkExec owns failure-path reaping before it returns. The same mutex
	// excludes our reaper until a successful PID has entered the registry,
	// including a child that exits before ForkExec returns to this goroutine.
	pid, err := syscall.ForkExec(spec.Path, spec.Args, &syscall.ProcAttr{
		Dir: spec.Dir, Env: spec.Env, Files: files,
	})
	if err != nil {
		return nil, ErrUnavailable
	}
	if hooks != nil && hooks.beforeRegistration != nil {
		hooks.beforeRegistration(pid)
	}
	child := &SupervisedChild{owner: s, pid: pid, pidfd: -1, done: make(chan struct{})}
	s.children[pid] = child
	s.empty = false
	openPIDFD := unix.PidfdOpen
	if hooks != nil && hooks.openPIDFD != nil {
		openPIDFD = hooks.openPIDFD
	}
	fd, err := openPIDFD(pid, 0)
	if err != nil {
		// The PID is still our unreaped child: the sole reaper is excluded by
		// this lock, so it cannot be reused during this failure cleanup signal.
		_ = unix.Kill(pid, unix.SIGKILL)
		s.draining, s.err = true, ErrQuarantined
		s.notify()
		return child, errors.Join(ErrQuarantined, ErrLaunchUncertain)
	}
	child.pidfd = fd
	s.notify()
	return child, nil
}

func (c *SupervisedChild) Wait(ctx context.Context) (ChildExit, error) {
	if c == nil || c.owner == nil || c.done == nil || c.pid <= 1 || ctx == nil {
		return ChildExit{}, ErrInvalid
	}
	select {
	case <-c.done:
		return c.exit, nil
	case <-ctx.Done():
		select {
		case <-c.done:
			return c.exit, nil
		default:
		}
		return ChildExit{}, ctx.Err()
	}
}

func (c *SupervisedChild) Signal(sig syscall.Signal) error {
	if c == nil || c.owner == nil || c.done == nil || c.pid <= 1 || (sig != syscall.SIGTERM && sig != syscall.SIGKILL) {
		return ErrInvalid
	}
	c.pidfdMu.Lock()
	defer c.pidfdMu.Unlock()
	if c.pidfd < 0 {
		return ErrUnavailable
	}
	if err := unix.PidfdSendSignal(c.pidfd, unix.Signal(sig), nil, 0); err != nil {
		return ErrUnavailable
	}
	return nil
}

func (s *NamespaceSupervisor) notify() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *NamespaceSupervisor) reapLoop() {
	defer close(s.stopped)
	defer signal.Stop(s.signals)
	// Custom clone exit signals need not send SIGCHLD. Periodic draining is
	// bounded and also reaps those adopted exits with Linux __WALL semantics.
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		s.mu.Lock()
		for range 256 {
			var status unix.WaitStatus
			pid, err := unix.Wait4(-1, &status, unix.WNOHANG|unix.WALL, nil)
			if errors.Is(err, unix.EINTR) {
				continue
			}
			if errors.Is(err, unix.ECHILD) {
				s.empty = true
				if len(s.children) != 0 {
					s.draining, s.err = true, ErrOwnership
				}
				break
			}
			if err != nil {
				s.draining, s.err = true, ErrQuarantined
				break
			}
			if pid == 0 {
				s.empty = false
				break
			}
			// A traced child can report a stop even without WUNTRACED.
			// Its process and pidfd remain owned until an actual final exit.
			if !status.Exited() && !status.Signaled() {
				s.nonterminal++
				s.empty = false
				continue
			}
			s.reaped++
			if child, ok := s.children[pid]; ok {
				child.exit = ChildExit{Code: status.ExitStatus()}
				if status.Signaled() {
					child.exit.Code, child.exit.Signal = -1, syscall.Signal(status.Signal())
				}
				child.pidfdMu.Lock()
				if child.pidfd >= 0 {
					_ = unix.Close(child.pidfd)
					child.pidfd = -1
				}
				child.pidfdMu.Unlock()
				delete(s.children, pid)
				close(child.done)
			} else {
				s.adopted++
			}
		}
		s.mu.Unlock()
		select {
		case <-s.stop:
			return
		case <-s.signals:
		case <-s.wake:
		case <-ticker.C:
		}
	}
}

func (s *NamespaceSupervisor) State() SupervisorState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return SupervisorState{Registered: len(s.children), Reaped: s.reaped, Adopted: s.adopted,
		Nonterminal: s.nonterminal, Draining: s.closeRequested.Load() || s.draining, Closed: s.closed}
}

func (s *NamespaceSupervisor) processIDs() ([]int, error) {
	fd, err := unix.Openat2(int(s.limits.procRoot.Fd()), ".", &unix.OpenHow{
		Flags:   unix.O_RDONLY | unix.O_DIRECTORY | unix.O_CLOEXEC,
		Resolve: unix.RESOLVE_BENEATH | unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_MAGICLINKS | unix.RESOLVE_NO_XDEV,
	})
	if err != nil {
		return nil, ErrOwnership
	}
	file := os.NewFile(uintptr(fd), "supervisor process inventory")
	defer file.Close()
	names, err := file.Readdirnames(int(s.policy.MaxProcesses) + 128)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, ErrOwnership
	}
	if len(names) >= int(s.policy.MaxProcesses)+128 {
		return nil, ErrOwnership
	}
	var ids []int
	for _, name := range names {
		pid, err := strconv.Atoi(name)
		if err == nil && pid > 0 {
			ids = append(ids, pid)
		}
	}
	return ids, nil
}

// Close permanently closes spawn admission and owns namespace shutdown. An
// earlier caller deadline returns uncertainty while the bounded shutdown owner
// continues. Success proves no native resident or waitable child remains.
func (s *NamespaceSupervisor) Close(ctx context.Context) error {
	if s == nil || ctx == nil || s.closeDone == nil {
		return ErrInvalid
	}
	// Closing admission must not wait behind a ForkExec or a reaper holding mu.
	// A spawn already inside ForkExec remains owned and shutdown must join it.
	s.closeRequested.Store(true)
	s.closeOnce.Do(func() {
		go func() {
			s.closeErr = s.shutdown()
			close(s.closeDone)
		}()
	})
	select {
	case <-s.closeDone:
		return s.closeErr
	case <-ctx.Done():
		select {
		case <-s.closeDone:
			return s.closeErr
		default:
		}
		return ErrQuarantined
	}
}

func (s *NamespaceSupervisor) lockBefore(ctx context.Context) bool {
	for {
		if s.mu.TryLock() {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(time.Millisecond):
		}
	}
}

// quiesce is owned by the domain's sole active operation. It fences launch
// while proving namespace emptiness without stopping the boot's sole reaper.
// Any ambiguous return permanently closes launch admission for this boot.
func (s *NamespaceSupervisor) quiesce(ctx context.Context) (resultErr error) {
	if s == nil || ctx == nil {
		return ErrInvalid
	}
	s.quiescing.Store(true)
	defer func() {
		if resultErr != nil {
			s.closeRequested.Store(true)
		} else {
			s.quiescing.Store(false)
		}
	}()
	for {
		if ctx.Err() != nil {
			return ErrQuarantined
		}
		if _, err := s.limits.Check(s.policy); err != nil {
			return ErrQuarantined
		}
		if err := unix.Kill(-1, unix.SIGKILL); err != nil && !errors.Is(err, unix.ESRCH) {
			return ErrQuarantined
		}
		s.notify()
		ids, err := s.processIDs()
		if !s.lockBefore(ctx) {
			return ErrQuarantined
		}
		empty := err == nil && len(ids) == 1 && ids[0] == 1 && s.empty && len(s.children) == 0 && s.err == nil
		s.mu.Unlock()
		if empty {
			return nil
		}
		select {
		case <-ctx.Done():
			return ErrQuarantined
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (s *NamespaceSupervisor) shutdown() error {
	ctx, cancel := context.WithTimeout(context.Background(), s.cleanupTimeout)
	defer cancel()
	for {
		if _, err := s.limits.Check(s.policy); err != nil {
			return ErrQuarantined
		}
		// Check proved PID 1 in the pinned private namespace. Linux excludes
		// the caller/PID 1 from kill(-1); trusted runtime injection is outside
		// this contract and can make the following empty-namespace gate fail.
		if err := unix.Kill(-1, unix.SIGKILL); err != nil && !errors.Is(err, unix.ESRCH) {
			return ErrQuarantined
		}
		s.notify()
		ids, err := s.processIDs()
		if !s.lockBefore(ctx) {
			return ErrQuarantined
		}
		empty := err == nil && len(ids) == 1 && ids[0] == 1 && s.empty && len(s.children) == 0 && s.err == nil
		s.mu.Unlock()
		if empty {
			close(s.stop)
			select {
			case <-s.stopped:
			case <-ctx.Done():
				return ErrQuarantined
			}
			if err := s.limits.Close(); err != nil {
				return ErrQuarantined
			}
			if !s.lockBefore(ctx) {
				return ErrQuarantined
			}
			s.closed = true
			s.mu.Unlock()
			return nil
		}
		select {
		case <-ctx.Done():
			return ErrQuarantined
		case <-time.After(10 * time.Millisecond):
		}
	}
}
