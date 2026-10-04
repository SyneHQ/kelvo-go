//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package audit

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestAuditBlockedStartRetainsSingleWriterAndFencesLateReceipt(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.WriteTimeout = 25 * time.Millisecond
	operations := defaultIO()
	var enabled, blocked atomic.Bool
	var calls atomic.Int32
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	operations.sync = func(file *os.File) error {
		if enabled.Load() {
			calls.Add(1)
			if blocked.CompareAndSwap(false, true) {
				close(entered)
				<-release
			}
		}
		return file.Sync()
	}
	j, err := openWithIO(cfg, testScope(), operations)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		unblock()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = j.Close(ctx)
	}()
	enabled.Store(true)
	type result struct {
		receipt *Receipt
		err     error
	}
	done := make(chan result, 1)
	go func() { r, err := j.Begin(context.Background(), testBinding(), QueryExecution); done <- result{r, err} }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("writer did not enter fixture")
	}
	select {
	case result := <-done:
		if result.receipt != nil || !errors.Is(result.err, ErrUncertain) {
			t.Fatalf("late receipt escaped: %+v", result)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked syscall prevented timeout")
	}
	if j.Ready() {
		t.Fatal("timed-out writer remained ready")
	}
	for range 20 {
		if _, err := j.Begin(context.Background(), testBinding(), QuerySubmit); !errors.Is(err, ErrUncertain) {
			t.Fatal("new work admitted after uncertainty")
		}
	}
	if calls.Load() != 1 {
		t.Fatal("replacement writers launched")
	}
	if other, err := Open(cfg, testScope()); !errors.Is(err, ErrBusy) {
		if other != nil {
			other.Close(context.Background())
		}
		t.Fatalf("pending writer lock lost: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	err = j.Close(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("close did not preserve pending writer: %v", err)
	}
	if _, err := unix.FcntlInt(j.file.Fd(), unix.F_GETFD, 0); err != nil {
		t.Fatal("descriptor closed during pending sync", err)
	}
	if other, err := Open(cfg, testScope()); !errors.Is(err, ErrBusy) {
		if other != nil {
			other.Close(context.Background())
		}
		t.Fatal("close timeout released ownership")
	}
	unblock()
	if err := j.Close(context.Background()); !errors.Is(err, ErrUncertain) {
		t.Fatalf("uncertainty disappeared: %v", err)
	}
	events := allEvents(t, cfg.Directory)
	if len(events) != 1 || events[0].Outcome != Unknown {
		t.Fatalf("late start was lost or authorized: %+v", events)
	}
	file, err := os.CreateTemp(t.TempDir(), "unrelated-")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	_ = j.Close(context.Background())
	if _, err = file.Stat(); err != nil {
		t.Fatal("repeated close damaged another descriptor", err)
	}
}

func TestAuditBlockedFinishNeverReturnsLateSuccessOrStartsAnotherWriter(t *testing.T) {
	cfg := testConfig(t.TempDir())
	operations := defaultIO()
	var enabled, blocked atomic.Bool
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	operations.sync = func(file *os.File) error {
		if enabled.Load() && blocked.CompareAndSwap(false, true) {
			close(entered)
			<-release
		}
		return file.Sync()
	}
	j, err := openWithIO(cfg, testScope(), operations)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { unblock(); _ = j.Close(context.Background()) }()
	r, err := j.Begin(context.Background(), testBinding(), QueryExecution)
	if err != nil {
		t.Fatal(err)
	}
	enabled.Store(true)
	done := make(chan error, 1)
	go func() {
		// Bound the injected finish fault, not the prerequisite receipt's real
		// fsync, whose latency depends on the test host's storage load.
		ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
		defer cancel()
		done <- r.Finish(ctx, Succeeded, None)
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("finish did not block")
	}
	select {
	case err := <-done:
		if !errors.Is(err, ErrUncertain) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("finish timeout blocked")
	}
	if _, err := j.Begin(context.Background(), testBinding(), QuerySubmit); !errors.Is(err, ErrUncertain) {
		t.Fatal("new start authorized with uncertain finish")
	}
	unblock()
	if err := j.Close(context.Background()); !errors.Is(err, ErrUncertain) {
		t.Fatal("late sync erased failure", err)
	}
	if err := r.Finish(context.Background(), Succeeded, None); !errors.Is(err, ErrUncertain) {
		t.Fatal("late success returned to caller", err)
	}
	// The local operation outcome may have reached disk even though its audit
	// acknowledgement timed out. It never proves client receipt/publication.
	if events := allEvents(t, cfg.Directory); len(events) != 1 || events[0].Outcome != Succeeded {
		t.Fatalf("valid late terminal frame lost: %+v", events)
	}
}

func TestAuditQueuedStartCannotRunAfterWriterUncertainty(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.WriteTimeout = 50 * time.Millisecond
	cfg.MaxPending = 2
	operations := defaultIO()
	var enabled, blocked atomic.Bool
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	operations.sync = func(file *os.File) error {
		if enabled.Load() && blocked.CompareAndSwap(false, true) {
			close(entered)
			<-release
		}
		return file.Sync()
	}
	j, err := openWithIO(cfg, testScope(), operations)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { unblock(); _ = j.Close(context.Background()) }()
	enabled.Store(true)
	first := make(chan error, 1)
	go func() { _, err := j.Begin(context.Background(), testBinding(), QueryExecution); first <- err }()
	<-entered
	second := make(chan error, 1)
	go func() { _, err := j.Begin(context.Background(), testBinding(), QuerySubmit); second <- err }()
	if err := <-first; !errors.Is(err, ErrUncertain) {
		t.Fatal(err)
	}
	unblock()
	if err := <-second; !errors.Is(err, ErrUncertain) {
		t.Fatalf("queued start passed failed writer: %v", err)
	}
	_ = j.Close(context.Background())
	if events := allEvents(t, cfg.Directory); len(events) != 1 || events[0].Kind != QueryExecution {
		t.Fatalf("queued start wrote after fence: %+v", events)
	}
}

func TestAuditSyncFailureAndShortWritesFailClosed(t *testing.T) {
	for _, stage := range []string{"start-sync", "finish-sync", "start-short", "finish-short"} {
		t.Run(stage, func(t *testing.T) {
			cfg := testConfig(t.TempDir())
			operations := defaultIO()
			var enabled atomic.Bool
			operations.sync = func(file *os.File) error {
				if enabled.Load() && strings.HasSuffix(stage, "sync") {
					return unix.ENOSPC
				}
				return file.Sync()
			}
			operations.write = func(file *os.File, raw []byte, offset int64) error {
				if enabled.Load() && strings.HasSuffix(stage, "short") && len(raw) == frameSize {
					_, err := file.WriteAt(raw[:80], offset)
					if err != nil {
						return err
					}
					return unix.ENOSPC
				}
				return writeExact(file, raw, offset)
			}
			j, err := openWithIO(cfg, testScope(), operations)
			if err != nil {
				t.Fatal(err)
			}
			defer j.Close(context.Background())
			if strings.HasPrefix(stage, "start") {
				enabled.Store(true)
				if _, err := j.Begin(context.Background(), testBinding(), QuerySubmit); !errors.Is(err, ErrUncertain) {
					t.Fatal(err)
				}
			} else {
				r, err := j.Begin(context.Background(), testBinding(), QueryExecution)
				if err != nil {
					t.Fatal(err)
				}
				enabled.Store(true)
				if err := r.Finish(context.Background(), Succeeded, None); !errors.Is(err, ErrUncertain) {
					t.Fatal(err)
				}
			}
			if j.Ready() {
				t.Fatal("failed audit remained ready")
			}
			if _, err := j.Begin(context.Background(), testBinding(), QuerySubmit); !errors.Is(err, ErrUncertain) {
				t.Fatal("failed journal admitted work")
			}
			enabled.Store(false)
			_ = j.Close(context.Background())
			reopened, err := Open(cfg, testScope())
			if strings.HasSuffix(stage, "short") {
				if !errors.Is(err, ErrCorrupt) {
					if reopened != nil {
						reopened.Close(context.Background())
					}
					t.Fatalf("torn frame accepted: %v", err)
				}
			} else {
				if err != nil {
					t.Fatal("valid completed sync-failure recovery", err)
				}
				_ = reopened.Close(context.Background())
			}
		})
	}
}

func TestAuditProcessCrashRecoveryAndTornFrameRejection(t *testing.T) {
	for _, mode := range []string{"after-start", "after-finish", "torn-start", "torn-finish"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			cmd := exec.Command(os.Args[0], "-test.run=^TestAuditCrashHelper$")
			cmd.Env = append(os.Environ(), "KELVO_AUDIT_CRASH_DIR="+dir, "KELVO_AUDIT_CRASH_MODE="+mode)
			output, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 42 {
				t.Fatalf("crash helper: %v %s", err, output)
			}
			j, err := Open(testConfig(dir), testScope())
			if strings.HasPrefix(mode, "torn-") {
				if !errors.Is(err, ErrCorrupt) {
					if j != nil {
						j.Close(context.Background())
					}
					t.Fatalf("torn crash frame accepted: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if err = j.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			events := allEvents(t, testConfig(dir).Directory)
			want := Unknown
			if mode == "after-finish" {
				want = Succeeded
			}
			if len(events) != 1 || events[0].Outcome != want || events[0].FinishedAt.IsZero() {
				t.Fatalf("crash recovery invented outcome: %+v", events)
			}
		})
	}
}

func TestAuditCrashHelper(t *testing.T) {
	dir, mode := os.Getenv("KELVO_AUDIT_CRASH_DIR"), os.Getenv("KELVO_AUDIT_CRASH_MODE")
	if dir == "" {
		return
	}
	operations := defaultIO()
	operations.write = func(file *os.File, raw []byte, offset int64) error {
		start := offset >= headerSize && (offset-headerSize)%slotSize == 0 && len(raw) == frameSize
		finish := offset >= headerSize && (offset-headerSize)%slotSize == frameSize && len(raw) == frameSize
		if (mode == "torn-start" && start) || (mode == "torn-finish" && finish) {
			_, _ = file.WriteAt(raw[:80], offset)
			os.Exit(42)
		}
		return writeExact(file, raw, offset)
	}
	j, err := openWithIO(testConfig(dir), testScope(), operations)
	if err != nil {
		t.Fatal(err)
	}
	r, err := j.Begin(context.Background(), testBinding(), QueryExecution)
	if err != nil {
		t.Fatal(err)
	}
	if mode == "after-start" {
		os.Exit(42)
	}
	if err = r.Finish(context.Background(), Succeeded, None); err != nil {
		t.Fatal(err)
	}
	os.Exit(42)
}

func TestAuditHeaderGenerationAndTruncationFailClosed(t *testing.T) {
	for _, mode := range []string{"header", "generation", "truncated"} {
		t.Run(mode, func(t *testing.T) {
			cfg := testConfig(t.TempDir())
			j := openTest(t, cfg, testScope())
			r, err := j.Begin(context.Background(), testBinding(), QueryExecution)
			if err != nil {
				t.Fatal(err)
			}
			if err = r.Finish(context.Background(), Succeeded, None); err != nil {
				t.Fatal(err)
			}
			if err = j.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			file, err := os.OpenFile(filepath.Join(cfg.Directory, journalName), os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "header":
				_, err = file.WriteAt([]byte("broken"), 0)
			case "truncated":
				err = file.Truncate(headerSize + 1)
			case "generation":
				raw := make([]byte, frameSize)
				if err = readExact(file, raw, slotOffset(r.slot)+frameSize); err != nil {
					t.Fatal(err)
				}
				var terminal finishRecord
				if _, err = decodeFrame(raw, finishFrame, &terminal); err != nil {
					t.Fatal(err)
				}
				terminal.ID = strings.Repeat("e", 32)
				raw, err = encodeFrame(finishFrame, terminal.ID, terminal, frameSize)
				if err == nil {
					err = writeExact(file, raw, slotOffset(r.slot)+frameSize)
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			_ = file.Sync()
			_ = file.Close()
			if other, err := Open(cfg, testScope()); !errors.Is(err, ErrCorrupt) {
				if other != nil {
					other.Close(context.Background())
				}
				t.Fatal("corrupt journal opened", err)
			}
			if _, err := ReadPage(context.Background(), cfg.Directory, r.slot, 1); !errors.Is(err, ErrCorrupt) {
				t.Fatal("corrupt journal read", err)
			}
		})
	}
}

// The acceptance harness supplies a dedicated <=1 MiB tmpfs. Refuse ordinary
// filesystems: this test must never attempt to exhaust the host's storage.
func TestAuditActualDiskFull(t *testing.T) {
	root := os.Getenv("KELVO_AUDIT_DISK_FULL_DIR")
	if root == "" {
		t.Skip("bounded disposable full-filesystem fixture required")
	}
	var stat unix.Statfs_t
	if err := unix.Statfs(root, &stat); err != nil {
		t.Fatal(err)
	}
	if stat.Type != unix.TMPFS_MAGIC || uint64(stat.Bsize)*stat.Blocks > 1<<20 {
		t.Fatal("refusing disk-full test outside bounded tmpfs")
	}
	directory, err := os.MkdirTemp(root, "audit-full-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(directory)
	// A successful reservation must still allow its terminal writes after the
	// rest of the dedicated filesystem fills. Existing allocated blocks only.
	small := testConfig(filepath.Join(directory, "reserved"))
	j, err := Open(small, testScope())
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close(context.Background())
	ballast, err := os.Create(filepath.Join(directory, "ballast"))
	if err != nil {
		t.Fatal(err)
	}
	block := make([]byte, 4096)
	var fullErr error
	for written := 0; written <= 1<<20; written += len(block) {
		if _, fullErr = ballast.Write(block); fullErr != nil {
			break
		}
	}
	if err = ballast.Close(); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(fullErr, unix.ENOSPC) {
		t.Fatalf("dedicated filesystem did not report ENOSPC: %v", fullErr)
	}
	r, err := j.Begin(context.Background(), testBinding(), QueryExecution)
	if err != nil {
		t.Fatal("preallocated start failed with full filesystem", err)
	}
	if err = r.Finish(context.Background(), Succeeded, None); err != nil {
		t.Fatal("reserved terminal failed with full filesystem", err)
	}
	if err = j.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if events := allEvents(t, small.Directory); len(events) != 1 || events[0].Outcome != Succeeded {
		t.Fatal("full filesystem lost reserved receipt")
	}
	if err = os.Remove(filepath.Join(directory, "ballast")); err != nil {
		t.Fatal(err)
	}
	cfg := testConfig(directory)
	cfg.MaxEntries = 2048
	if j, err := Open(cfg, testScope()); !errors.Is(err, ErrUnavailable) {
		if j != nil {
			j.Close(context.Background())
		}
		t.Fatalf("preallocation did not reject full storage: %v", err)
	}
}
