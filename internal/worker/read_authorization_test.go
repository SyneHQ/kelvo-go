// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/adapter/frame"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

// switchableSecrets is a secret store whose value can be rotated or revoked.
type switchableSecrets struct {
	mu      sync.Mutex
	value   string
	revoked bool
	calls   int
}

func (s *switchableSecrets) Resolve(context.Context, string) (string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.revoked {
		return "", true, errors.New("revoked")
	}
	return s.value, true, nil
}

func (s *switchableSecrets) set(value string, revoked bool) {
	s.mu.Lock()
	s.value, s.revoked = value, revoked
	s.mu.Unlock()
}

// recordingServe answers every request and records what the worker received.
func recordingServe(fs *fakeSet, seen *[]adapter.ProcessRequest) func(p *fakeWorkerProcess) {
	return func(p *fakeWorkerProcess) {
		serveFake(p, func(f frame.Frame) {
			if f.Type == frame.Request {
				var r adapter.ProcessRequest
				_ = json.Unmarshal(f.Payload, &r)
				fs.mu.Lock()
				*seen = append(*seen, r)
				fs.mu.Unlock()
				replyOK(p.stdoutW, f.Stream, f.Payload)
			}
		})
	}
}

func TestReadWorkerAuthorizesOncePerWindow(t *testing.T) {
	fs := &fakeSet{}
	var seen []adapter.ProcessRequest
	fs.serve = recordingServe(fs, &seen)
	e, mgr := readWorkerExecutor(t, fs)
	mgr.cfg.CacheDisabled, mgr.cache = true, nil
	mgr.cfg.ReauthorizeEvery = 300 * time.Millisecond
	secrets := &switchableSecrets{value: "postgres://reader:secret@db.internal:5432/appdb?sslmode=verify-full"}
	e.Secrets = secrets
	read := func() error {
		_, err := e.Execute(context.Background(), query.Request{Mode: "native", ConnectionID: "app", SQL: "SELECT 1"}, &workerTestSink{})
		return err
	}
	for i := 0; i < 5; i++ {
		if err := read(); err != nil {
			t.Fatal(err)
		}
	}
	fs.mu.Lock()
	if mgr.metrics.Authorizations.Load() != 1 || secrets.calls != 1 || len(seen) != 5 || seen[0].Source.DSN == "" {
		t.Fatalf("authorizations=%d resolves=%d requests=%d", mgr.metrics.Authorizations.Load(), secrets.calls, len(seen))
	}
	for _, r := range seen[1:] {
		if r.Source.DSN != "" || len(r.Source.Options) != 0 {
			t.Fatal("a later read carried credentials")
		}
	}
	fs.mu.Unlock()

	// Past reauthorize_every the next read authorizes again and carries them.
	time.Sleep(350 * time.Millisecond)
	if err := read(); err != nil {
		t.Fatal(err)
	}
	fs.mu.Lock()
	last := seen[len(seen)-1]
	fs.mu.Unlock()
	if mgr.metrics.Authorizations.Load() != 2 || last.Source.DSN == "" || len(fs.procs) != 1 {
		t.Fatal("stale authorization was reused", mgr.metrics.Authorizations.Load(), len(fs.procs))
	}

	// A revoked source is refused at the next re-authorization and its worker
	// is killed.
	secrets.set("", true)
	time.Sleep(350 * time.Millisecond)
	if err := read(); err == nil {
		t.Fatal("revoked source served")
	}
	waitFor(t, func() bool { return fs.procs[0].killed.Load() })
	if mgr.metrics.EvictionsAuth.Load() != 1 {
		t.Fatal("revoked worker not evicted by authorization", mgr.metrics.EvictionsAuth.Load())
	}

	// A changed source (new credentials) gets a new worker.
	secrets.set("postgres://reader:rotated@db.internal:5432/appdb?sslmode=verify-full", false)
	if err := read(); err != nil {
		t.Fatal(err)
	}
	if len(fs.procs) != 2 || fs.procs[1].killed.Load() {
		t.Fatal("restored source did not start a fresh worker", len(fs.procs))
	}
}

// A changed revision kills the old worker even while it serves a read.
func TestReadWorkerRevisionChangeKillsBusyWorker(t *testing.T) {
	fs := &fakeSet{}
	release := make(chan struct{})
	fs.serve = func(p *fakeWorkerProcess) {
		serveFake(p, func(f frame.Frame) {
			if f.Type != frame.Request {
				return
			}
			var r adapter.ProcessRequest
			_ = json.Unmarshal(f.Payload, &r)
			if r.Request.Spec.Query != nil && r.Request.Spec.Query.SQL == "SELECT 'slow'" {
				go func() { <-release; replyOK(p.stdoutW, f.Stream, f.Payload) }()
				return
			}
			replyOK(p.stdoutW, f.Stream, f.Payload)
		})
	}
	defer close(release)
	e, mgr := readWorkerExecutor(t, fs)
	mgr.cfg.CacheDisabled, mgr.cache = true, nil
	mgr.cfg.ReauthorizeEvery = 200 * time.Millisecond
	secrets := &switchableSecrets{value: "postgres://reader:secret@db.internal:5432/appdb?sslmode=verify-full"}
	e.Secrets = secrets
	slow := make(chan error, 1)
	go func() {
		_, err := e.Execute(context.Background(), query.Request{Mode: "native", ConnectionID: "app", SQL: "SELECT 'slow'"}, &workerTestSink{})
		slow <- err
	}()
	waitFor(t, func() bool { return mgr.metrics.Spawns.Load() == 1 })
	secrets.set("postgres://reader:rotated@db.internal:5432/appdb?sslmode=verify-full", false)
	time.Sleep(250 * time.Millisecond)
	if _, err := e.Execute(context.Background(), query.Request{Mode: "native", ConnectionID: "app", SQL: "SELECT 1"}, &workerTestSink{}); err != nil {
		t.Fatal(err)
	}
	if err := <-slow; err == nil {
		t.Fatal("read on the old revision's worker survived its eviction")
	}
	if !fs.procs[0].killed.Load() || len(fs.procs) != 2 || mgr.metrics.EvictionsAuth.Load() != 1 {
		t.Fatal("old revision's busy worker not killed", fs.procs[0].killed.Load(), len(fs.procs), mgr.metrics.EvictionsAuth.Load())
	}
}

// A secretless read never starts a worker: when the worker is gone, the read
// authorizes again and the new worker gets credentials.
func TestReadWorkerNeverSpawnsWithoutCredentials(t *testing.T) {
	fs := &fakeSet{}
	var seen []adapter.ProcessRequest
	fs.serve = recordingServe(fs, &seen)
	e, mgr := readWorkerExecutor(t, fs)
	mgr.cfg.CacheDisabled, mgr.cache = true, nil
	read := func() {
		if _, err := e.Execute(context.Background(), query.Request{Mode: "native", ConnectionID: "app", SQL: "SELECT 1"}, &workerTestSink{}); err != nil {
			t.Fatal(err)
		}
	}
	read()
	_ = fs.procs[0].Kill()
	waitFor(t, func() bool { mgr.mu.Lock(); defer mgr.mu.Unlock(); return len(mgr.workers) == 0 })
	read()
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if len(fs.procs) != 2 || len(seen) != 2 || seen[1].Source.DSN == "" || mgr.metrics.Authorizations.Load() != 2 {
		t.Fatal("new worker started without credentials", len(fs.procs), len(seen), mgr.metrics.Authorizations.Load())
	}
}
