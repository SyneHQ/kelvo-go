//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package audit

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
)

func testConfig(directory string) Config {
	return Config{Directory: filepath.Join(directory, "journal"), MaxEntries: 8, MaxPending: 4, Retention: time.Hour, WriteTimeout: 2 * time.Second}
}
func testScope() Scope {
	return Scope{ServiceID: "gateway-one", ServiceKind: "gateway", Tenants: []string{"tenant-a", "tenant-b"}}
}
func testBinding() Binding {
	return Binding{TenantID: "tenant-a", ServiceID: "gateway-one", ServiceKind: "gateway", PrincipalID: "analyst-one", PrincipalKind: "user", PolicyVersion: strings.Repeat("a", 64)}
}
func openTest(t *testing.T, cfg Config, scope Scope) *Journal {
	t.Helper()
	j, err := Open(cfg, scope)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = j.Close(ctx)
	})
	return j
}
func allEvents(t *testing.T, directory string) []Event {
	t.Helper()
	var events []Event
	for cursor := 0; ; {
		page, err := ReadPage(context.Background(), directory, cursor, 2)
		if err != nil {
			t.Fatal(err)
		}
		events = append(events, page.Events...)
		if page.Done {
			return events
		}
		if page.Next <= cursor {
			t.Fatal("pagination made no progress")
		}
		cursor = page.Next
	}
}

func TestAuditReceiptDurabilityReadAuthorizationAndScope(t *testing.T) {
	cfg := testConfig(t.TempDir())
	scope := testScope()
	j := openTest(t, cfg, scope)
	r, err := j.Begin(context.Background(), testBinding(), QueryExecution)
	if err != nil {
		t.Fatal(err)
	}
	if !hexID(r.ID(), 32) {
		t.Fatal("event ID was not generated")
	}
	if _, err = ReadPage(context.Background(), cfg.Directory, 0, 2); !errors.Is(err, ErrBusy) {
		t.Fatalf("live writer allowed offline read: %v", err)
	}
	if err = r.Finish(context.Background(), Succeeded, None); err != nil {
		t.Fatal(err)
	}
	if err = j.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	events := allEvents(t, cfg.Directory)
	if len(events) != 1 || events[0].ID != r.ID() || events[0].Binding != testBinding() || events[0].Outcome != Succeeded || events[0].Category != None || events[0].FinishedAt.Before(events[0].StartedAt) {
		t.Fatalf("durable event mismatch: %+v", events)
	}
	reopened := openTest(t, cfg, scope)
	if reopened.Snapshot().Retained != 1 || reopened.Snapshot().Active != 0 {
		t.Fatal("restart lost retention accounting")
	}
}

func TestAuditInvalidConfigurationAndBindings(t *testing.T) {
	for _, change := range []func(*Config){
		func(c *Config) { c.Directory = "relative" }, func(c *Config) { c.MaxEntries = 0 }, func(c *Config) { c.MaxEntries = maximumEntries + 1 }, func(c *Config) { c.MaxPending = 0 }, func(c *Config) { c.MaxPending = c.MaxEntries + 1 }, func(c *Config) { c.Retention = 0 }, func(c *Config) { c.Retention = 31 * 24 * time.Hour }, func(c *Config) { c.WriteTimeout = 0 }, func(c *Config) { c.WriteTimeout = 31 * time.Second },
	} {
		cfg := testConfig(t.TempDir())
		change(&cfg)
		if _, err := Open(cfg, testScope()); !errors.Is(err, ErrInvalid) {
			t.Fatalf("invalid config: %v", err)
		}
	}
	cfg := testConfig(t.TempDir())
	j := openTest(t, cfg, testScope())
	for _, change := range []func(*Binding){
		func(b *Binding) { b.TenantID = "unprovisioned" }, func(b *Binding) { b.ServiceID = "different-service" }, func(b *Binding) { b.ServiceKind = "worker" }, func(b *Binding) { b.PrincipalKind = "administrator" }, func(b *Binding) { b.PrincipalID = "SELECT secret_password" }, func(b *Binding) { b.PrincipalID = strings.Repeat("x", 129) }, func(b *Binding) { b.PolicyVersion = "not-a-policy-digest" }, func(b *Binding) { b.PrincipalKind = "unknown" },
	} {
		b := testBinding()
		change(&b)
		if _, err := j.Begin(context.Background(), b, QuerySubmit); !errors.Is(err, ErrInvalid) {
			t.Fatalf("invalid binding accepted: %v", err)
		}
	}
	if _, err := j.Begin(context.Background(), testBinding(), Kind("arbitrary-label")); !errors.Is(err, ErrInvalid) {
		t.Fatal("unbounded event kind accepted")
	}
	unknown := testBinding()
	unknown.TenantID = ""
	unknown.PrincipalID = ""
	unknown.PrincipalKind = "unknown"
	unknown.PolicyVersion = ""
	if _, err := j.Begin(context.Background(), unknown, Authentication); !errors.Is(err, ErrInvalid) {
		t.Fatal("unknown tenant received protected receipt")
	}
	if err := j.Record(context.Background(), unknown, Authentication, Denied, AuthenticationFailure); err != nil {
		t.Fatal(err)
	}
	if err := j.Record(context.Background(), unknown, Authentication, Succeeded, None); !errors.Is(err, ErrInvalid) {
		t.Fatal("unknown tenant recorded authenticated success")
	}
	if j.Snapshot().Retained != 1 {
		t.Fatal("invalid events consumed storage")
	}
}

func TestAuditScopeCopyAndReopenRejectReassignment(t *testing.T) {
	cfg := testConfig(t.TempDir())
	scope := testScope()
	j := openTest(t, cfg, scope)
	scope.Tenants[0] = "different-tenant"
	r, err := j.Begin(context.Background(), testBinding(), QuerySubmit)
	if err != nil {
		t.Fatal("retained mutable scope", err)
	}
	if err = r.Finish(context.Background(), Succeeded, None); err != nil {
		t.Fatal(err)
	}
	if err = j.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, other := range []Scope{scope, {ServiceID: "another-gateway", ServiceKind: "gateway", Tenants: testScope().Tenants}, {ServiceID: "gateway-one", ServiceKind: "worker", Tenants: testScope().Tenants}} {
		if _, err := Open(cfg, other); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("journal reassigned: %v", err)
		}
	}
	changed := cfg
	changed.Retention = 2 * time.Hour
	if _, err := Open(changed, testScope()); !errors.Is(err, ErrCorrupt) {
		t.Fatal("retention changed on reopen")
	}
}

func TestAuditFullRetentionStillHasEveryFinishReserved(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.MaxEntries = 2
	cfg.MaxPending = 2
	j := openTest(t, cfg, testScope())
	a, err := j.Begin(context.Background(), testBinding(), QuerySubmit)
	if err != nil {
		t.Fatal(err)
	}
	b, err := j.Begin(context.Background(), testBinding(), QueryExecution)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = j.Begin(context.Background(), testBinding(), QuerySubmit); !errors.Is(err, ErrBusy) {
		t.Fatalf("pending bound bypassed: %v", err)
	}
	if err = a.Finish(context.Background(), Succeeded, None); err != nil {
		t.Fatal(err)
	}
	if _, err = j.Begin(context.Background(), testBinding(), QuerySubmit); !errors.Is(err, ErrFull) {
		t.Fatalf("nonexpired retention overwritten: %v", err)
	}
	if err = b.Finish(context.Background(), Cancelled, Cancellation); err != nil {
		t.Fatal("finish had no reserved space", err)
	}
	if j.Ready() || !j.Snapshot().Healthy || j.Snapshot().Active != 0 || j.Snapshot().Retained != 2 {
		t.Fatalf("full state: %+v", j.Snapshot())
	}
	info, err := os.Stat(filepath.Join(cfg.Directory, journalName))
	if err != nil || info.Size() != headerSize+2*slotSize {
		t.Fatal("journal grew beyond preallocation")
	}
	if err = j.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(allEvents(t, cfg.Directory)) != 2 {
		t.Fatal("full journal lost outcomes")
	}
}

func TestAuditActiveDoesNotExpireAndCompletedSlotsReuseAfterRetention(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.MaxEntries = 1
	cfg.MaxPending = 1
	cfg.Retention = time.Second
	var now atomic.Int64
	now.Store(time.Now().UnixNano())
	operations := defaultIO()
	operations.now = func() time.Time { return time.Unix(0, now.Load()) }
	j, err := openWithIO(cfg, testScope(), operations)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close(context.Background())
	a, err := j.Begin(context.Background(), testBinding(), QueryExecution)
	if err != nil {
		t.Fatal(err)
	}
	now.Add(int64(2 * time.Hour))
	if j.Ready() {
		t.Fatal("active audit record expired")
	}
	if _, err = j.Begin(context.Background(), testBinding(), QuerySubmit); !errors.Is(err, ErrBusy) {
		t.Fatal("active record overwritten")
	}
	if err = a.Finish(context.Background(), Succeeded, None); err != nil {
		t.Fatal(err)
	}
	now.Add(int64(2 * time.Second))
	if !j.Ready() {
		t.Fatal("expired completed capacity remained unavailable")
	}
	b, err := j.Begin(context.Background(), testBinding(), QueryExecution)
	if err != nil {
		t.Fatal(err)
	}
	if a.ID() == b.ID() {
		t.Fatal("reused event generation")
	}
	if err = a.Finish(context.Background(), Succeeded, None); err != nil {
		t.Fatal("idempotent old finish", err)
	}
	if err = b.Finish(context.Background(), Failed, Internal); err != nil {
		t.Fatal("old receipt affected new generation", err)
	}
}

func TestAuditFinishIsOnceOnlyAndConflictingOutcomesFail(t *testing.T) {
	cfg := testConfig(t.TempDir())
	j := openTest(t, cfg, testScope())
	r, err := j.Begin(context.Background(), testBinding(), QueryExecution)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 40 {
		wg.Go(func() {
			if err := r.Finish(context.Background(), Succeeded, None); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if err := r.Finish(context.Background(), Failed, Internal); !errors.Is(err, ErrInvalid) {
		t.Fatal("conflicting finish claimed success")
	}
	if j.Snapshot().Retained != 1 || j.Snapshot().Active != 0 {
		t.Fatalf("finish accounting: %+v", j.Snapshot())
	}
}

func TestAuditCancellationBeforeEnqueueKeepsReceiptFinishable(t *testing.T) {
	j := openTest(t, testConfig(t.TempDir()), testScope())
	r, err := j.Begin(context.Background(), testBinding(), QueryExecution)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err = r.Finish(ctx, Cancelled, Cancellation); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err = r.Finish(context.Background(), Cancelled, Cancellation); err != nil {
		t.Fatal("canceled caller lost terminal reservation", err)
	}
}

func TestAuditConcurrentCompetingFinishesStoreOnlyTheWinner(t *testing.T) {
	cfg := testConfig(t.TempDir())
	j := openTest(t, cfg, testScope())
	r, err := j.Begin(context.Background(), testBinding(), QueryExecution)
	if err != nil {
		t.Fatal(err)
	}
	type result struct {
		outcome Outcome
		err     error
	}
	gate := make(chan struct{})
	results := make(chan result, 2)
	for _, terminal := range []struct {
		outcome  Outcome
		category Category
	}{{Succeeded, None}, {Cancelled, Cancellation}} {
		go func() {
			<-gate
			results <- result{terminal.outcome, r.Finish(context.Background(), terminal.outcome, terminal.category)}
		}()
	}
	close(gate)
	var winner Outcome
	conflicts := 0
	for range 2 {
		result := <-results
		if result.err == nil {
			if winner != "" {
				t.Fatal("competing outcomes both succeeded")
			}
			winner = result.outcome
		} else if errors.Is(result.err, ErrInvalid) {
			conflicts++
		} else {
			t.Fatal(result.err)
		}
	}
	if winner == "" || conflicts != 1 {
		t.Fatal("competing outcomes did not select exactly one terminal")
	}
	if err = j.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if events := allEvents(t, cfg.Directory); len(events) != 1 || events[0].Outcome != winner {
		t.Fatal("stored outcome disagreed with successful caller")
	}
}

func TestAuditConcurrentOperationsRemainBoundedAndDurable(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.MaxEntries = 128
	cfg.MaxPending = 16
	j := openTest(t, cfg, testScope())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for range 80 {
		wg.Go(func() {
			var r *Receipt
			var err error
			for {
				r, err = j.Begin(ctx, testBinding(), QueryExecution)
				if !errors.Is(err, ErrBusy) {
					break
				}
				select {
				case <-ctx.Done():
					t.Error(ctx.Err())
					return
				case <-time.After(time.Millisecond):
				}
			}
			if err != nil {
				t.Error(err)
				return
			}
			if err = r.Finish(ctx, Succeeded, None); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if state := j.Snapshot(); state.Active != 0 || state.Retained != 80 || state.Queued != 0 || !state.Healthy {
		t.Fatalf("bounded accounting: %+v", state)
	}
	if err := j.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	events := allEvents(t, cfg.Directory)
	if len(events) != 80 {
		t.Fatalf("lost events: %d", len(events))
	}
	ids := map[string]bool{}
	for _, event := range events {
		if ids[event.ID] || event.Outcome != Succeeded {
			t.Fatal("duplicate or incomplete event")
		}
		ids[event.ID] = true
	}
}

func TestAuditFilesystemIsolationAndPermissions(t *testing.T) {
	for _, name := range []string{"root-mode", "ancestor-mode", "root-symlink", "journal-symlink", "journal-hardlink", "journal-mode", "lock-symlink"} {
		t.Run(name, func(t *testing.T) {
			base := t.TempDir()
			cfg := testConfig(filepath.Join(base, "audit"))
			j := openTest(t, cfg, testScope())
			if err := j.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			switch name {
			case "root-mode":
				if err := os.Chmod(cfg.Directory, 0755); err != nil {
					t.Fatal(err)
				}
			case "ancestor-mode":
				if err := os.Chmod(base, 0777); err != nil {
					t.Fatal(err)
				}
			case "root-symlink":
				original := cfg.Directory
				cfg.Directory = filepath.Join(base, "linked")
				if err := os.Symlink(original, cfg.Directory); err != nil {
					t.Fatal(err)
				}
			case "journal-symlink", "lock-symlink":
				target := journalName
				if name == "lock-symlink" {
					target = lockName
				}
				original := filepath.Join(cfg.Directory, target)
				moved := original + ".saved"
				if err := os.Rename(original, moved); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(moved, original); err != nil {
					t.Fatal(err)
				}
			case "journal-hardlink":
				if err := os.Link(filepath.Join(cfg.Directory, journalName), filepath.Join(base, "linked")); err != nil {
					t.Fatal(err)
				}
			case "journal-mode":
				if err := os.Chmod(filepath.Join(cfg.Directory, journalName), 0644); err != nil {
					t.Fatal(err)
				}
			}
			if reopened, err := Open(cfg, testScope()); err == nil {
				reopened.Close(context.Background())
				t.Fatal("unsafe journal accepted")
			}
			if _, err := ReadPage(context.Background(), cfg.Directory, 0, 1); err == nil {
				t.Fatal("unsafe operator read accepted")
			}
		})
	}
}

func TestAuditReaderPaginationAndMaximumLabels(t *testing.T) {
	cfg := testConfig(t.TempDir())
	scope := Scope{ServiceID: strings.Repeat("a", 64), ServiceKind: "gateway", Tenants: []string{strings.Repeat("b", 32)}}
	j := openTest(t, cfg, scope)
	binding := Binding{TenantID: scope.Tenants[0], ServiceID: scope.ServiceID, ServiceKind: scope.ServiceKind, PrincipalID: strings.Repeat("C", 128), PrincipalKind: "service", PolicyVersion: strings.Repeat("f", 64)}
	if err := j.Record(context.Background(), binding, Authentication, Succeeded, None); err != nil {
		t.Fatal("maximum bounded event did not fit", err)
	}
	if err := j.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, limit := range []int{0, maximumPage + 1} {
		if _, err := ReadPage(context.Background(), cfg.Directory, 0, limit); !errors.Is(err, ErrInvalid) {
			t.Fatal("unbounded page accepted")
		}
	}
	if events := allEvents(t, cfg.Directory); len(events) != 1 || events[0].Binding != binding {
		t.Fatal("maximum event changed")
	}
}
