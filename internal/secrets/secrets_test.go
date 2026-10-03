//go:build linux || darwin

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package secrets

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func privateSecret(t *testing.T, value string) string {
	t.Helper()
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "private-secret")
	if err := os.WriteFile(path, []byte(value), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}
func providerFor(t *testing.T, path string, ttl time.Duration) *Provider {
	t.Helper()
	p, err := New(Config{Files: map[string]string{"KEY": path}, TTL: ttl})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	return p
}
func waitSecretCondition(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("secret operation did not complete")
}
func TestPrivateSecretExactBytesTTLAndAtomicReplacement(t *testing.T) {
	path := privateSecret(t, "first\n")
	p := providerFor(t, path, time.Minute)
	value, known, err := p.Resolve(context.Background(), "KEY")
	if err != nil || !known || value != "first\n" {
		t.Fatal("private secret did not resolve exactly")
	}
	replacement := path + ".new"
	if err := os.WriteFile(replacement, []byte("second"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(replacement, path); err != nil {
		t.Fatal(err)
	}
	value, _, err = p.Resolve(context.Background(), "KEY")
	if err != nil || value != "first\n" {
		t.Fatal("TTL cache did not retain prior value")
	}
	p.mu.Lock()
	expired := p.cache["KEY"]
	expired.expires = time.Now().Add(-time.Second)
	p.cache["KEY"] = expired
	p.mu.Unlock()
	value, _, err = p.Resolve(context.Background(), "KEY")
	if err != nil || value != "second" {
		t.Fatal("atomic credential replacement was not loaded after TTL")
	}
	p.mu.Lock()
	owned := p.cache["KEY"].value
	p.mu.Unlock()
	p.Close()
	for _, b := range owned {
		if b != 0 {
			t.Fatal("close did not clear cache buffer")
		}
	}
	if _, _, err := p.Resolve(context.Background(), "KEY"); !errors.Is(err, ErrClosed) {
		t.Fatal("closed provider still resolves")
	}
}
func TestZeroTTLDoesNotRetainCompletedSecrets(t *testing.T) {
	path := privateSecret(t, "one")
	p := providerFor(t, path, 0)
	if _, _, err := p.Resolve(context.Background(), "KEY"); err != nil {
		t.Fatal(err)
	}
	replacement := path + ".new"
	os.WriteFile(replacement, []byte("two"), 0600)
	if err := os.Rename(replacement, path); err != nil {
		t.Fatal(err)
	}
	value, _, err := p.Resolve(context.Background(), "KEY")
	if err != nil || value != "two" {
		t.Fatal("zero TTL cached old secret")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.cache) != 0 || len(p.calls) != 0 {
		t.Fatal("completed zero-TTL values retained")
	}
}
func TestConcurrentLookupsCoalesceAndWaitersCancelIndependently(t *testing.T) {
	p := providerFor(t, privateSecret(t, "unused"), time.Minute)
	release := make(chan struct{})
	var reads atomic.Int32
	p.read = func(context.Context, string) ([]byte, error) { reads.Add(1); <-release; return []byte("resolved"), nil }
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	canceled := make(chan error, 1)
	go func() { _, _, err := p.Resolve(ctx, "KEY"); canceled <- err }()
	const workers = 12
	var wg sync.WaitGroup
	failures := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			value, known, err := p.Resolve(context.Background(), "KEY")
			if err != nil || !known || value != "resolved" {
				failures <- errors.New("coalesced lookup failed")
			}
		}()
	}
	waitSecretCondition(t, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.calls["KEY"] != nil && p.calls["KEY"].waiters == workers+1
	})
	cancel()
	if err := <-canceled; !errors.Is(err, context.Canceled) {
		t.Fatal("waiter cancellation lost")
	}
	close(release)
	wg.Wait()
	close(failures)
	for err := range failures {
		t.Error(err)
	}
	if reads.Load() != 1 {
		t.Fatalf("coalesced read count: %d", reads.Load())
	}
	if _, known, err := p.Resolve(context.Background(), "UNKNOWN"); err != nil || known {
		t.Fatal("unknown key did not permit caller fallback")
	}
	if reads.Load() != 1 {
		t.Fatal("unknown key read a file")
	}
}
func TestCanceledOrClosedLoadsCannotRepopulateCache(t *testing.T) {
	for _, closeProvider := range []bool{false, true} {
		t.Run(map[bool]string{false: "all_waiters_cancel", true: "close_during_read"}[closeProvider], func(t *testing.T) {
			p := providerFor(t, privateSecret(t, "unused"), time.Minute)
			release := make(chan struct{})
			started := make(chan struct{})
			p.read = func(context.Context, string) ([]byte, error) {
				close(started)
				<-release
				return []byte("late secret"), nil
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { _, _, err := p.Resolve(ctx, "KEY"); done <- err }()
			<-started
			want := context.Canceled
			if closeProvider {
				p.Close()
				want = ErrClosed
			} else {
				cancel()
			}
			select {
			case err := <-done:
				if !errors.Is(err, want) {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("cancellation waited on disk read")
			}
			close(release)
			waitSecretCondition(t, func() bool { p.mu.Lock(); defer p.mu.Unlock(); return len(p.calls) == 0 })
			p.mu.Lock()
			defer p.mu.Unlock()
			if len(p.cache) != 0 {
				t.Fatal("abandoned lookup populated cache")
			}
		})
	}
}
func TestPrivateFileValidationAndSanitizedErrors(t *testing.T) {
	for _, kind := range []string{"symlink", "parent_symlink", "hardlink", "group_access", "directory", "oversize", "nul", "fifo", "missing"} {
		t.Run(kind, func(t *testing.T) {
			path := privateSecret(t, "sensitive-value")
			switch kind {
			case "symlink":
				link := path + "-link"
				if err := os.Symlink(path, link); err != nil {
					t.Fatal(err)
				}
				path = link
			case "parent_symlink":
				dir := filepath.Dir(path)
				link := dir + "-link"
				if err := os.Symlink(dir, link); err != nil {
					t.Fatal(err)
				}
				defer os.Remove(link)
				path = filepath.Join(link, filepath.Base(path))
			case "hardlink":
				if err := os.Link(path, path+"-hard"); err != nil {
					t.Fatal(err)
				}
			case "group_access":
				os.Chmod(path, 0640)
			case "directory":
				path = filepath.Dir(path)
			case "oversize":
				os.WriteFile(path, []byte(strings.Repeat("x", MaxValueBytes+1)), 0600)
			case "nul":
				os.WriteFile(path, []byte("a\x00b"), 0600)
			case "fifo":
				os.Remove(path)
				if err := unix.Mkfifo(path, 0600); err != nil {
					t.Fatal(err)
				}
			case "missing":
				os.Remove(path)
			}
			p := providerFor(t, path, 0)
			value, known, err := p.Resolve(context.Background(), "KEY")
			if value != "" || !known || !errors.Is(err, ErrUnavailable) {
				t.Fatalf("unsafe file accepted: %s", kind)
			}
			if strings.Contains(err.Error(), path) || strings.Contains(err.Error(), "sensitive-value") {
				t.Fatal("error leaked secret metadata")
			}
		})
	}
}
func TestProviderBoundsAndCopiesConfiguration(t *testing.T) {
	path := privateSecret(t, strings.Repeat("a", MaxValueBytes))
	config := Config{Files: map[string]string{"KEY": path}}
	p, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	config.Files["KEY"] = "/missing"
	value, _, err := p.Resolve(context.Background(), "KEY")
	if err != nil || len(value) != MaxValueBytes {
		t.Fatal("limit-sized value or copied configuration failed")
	}
	for _, config := range []Config{{Files: map[string]string{}}, {Files: map[string]string{"KEY": "relative"}}, {Files: map[string]string{"KEY": path}, TTL: MaxTTL + 1}, {Files: map[string]string{"KEY": path}, TTL: -1}} {
		if _, err := New(config); !errors.Is(err, ErrInvalid) {
			t.Fatal("invalid config accepted")
		}
	}
	many := map[string]string{}
	for i := 0; i <= MaxKeys; i++ {
		many[strings.Repeat("x", i+1)] = path
	}
	if _, err := New(Config{Files: many}); !errors.Is(err, ErrInvalid) {
		t.Fatal("key bound exceeded")
	}
}

func TestConcurrentAtomicRotationReturnsWholeValues(t *testing.T) {
	oldValue := strings.Repeat("a", MaxValueBytes)
	newValue := strings.Repeat("b", MaxValueBytes)
	path := privateSecret(t, oldValue)
	p := providerFor(t, path, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stop := make(chan struct{})
	done := make(chan error, 1)
	waitRotation := func() error {
		select {
		case err := <-done:
			return err
		case <-time.After(5 * time.Second):
			return errors.New("secret rotation did not finish")
		}
	}
	t.Cleanup(func() {
		close(stop)
		if err := waitRotation(); err != nil {
			t.Error(err)
		}
	})
	go func() {
		defer close(done)
		for i := 0; i < 80; i++ {
			select {
			case <-stop:
				return
			default:
			}
			text := oldValue
			if i%2 == 1 {
				text = newValue // The final replacement differs from the initial value.
			}
			stage := path + ".next"
			if err := os.WriteFile(stage, []byte(text), 0600); err != nil {
				done <- err
				return
			}
			select {
			case <-stop:
				return
			default:
			}
			if err := os.Rename(stage, path); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	for i := 0; i < 80; i++ {
		value, known, err := p.Resolve(ctx, "KEY")
		if err != nil {
			// Separate metadata samples can observe ctime changing on an
			// already-unlinked inode. Keep that conservative rejection: it
			// must expose neither bytes nor private paths/values in errors.
			if !known || value != "" || !errors.Is(err, ErrUnavailable) || err.Error() != ErrUnavailable.Error() {
				t.Error("atomic rotation returned an unsafe configured-key failure")
				break
			}
			continue
		}
		if !known || (value != oldValue && value != newValue) {
			t.Error("atomic rotation did not return a whole secret")
			break
		}
	}
	if err := waitRotation(); err != nil {
		t.Fatal(err)
	}
	value, known, err := p.Resolve(ctx, "KEY")
	if err != nil || !known || value != newValue {
		t.Fatal("completed atomic rotation did not return the exact fresh secret")
	}
}
