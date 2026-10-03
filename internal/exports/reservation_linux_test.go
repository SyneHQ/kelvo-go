//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package exports

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"go.yaml.in/yaml/v3"
)

func reserveTest(t *testing.T, s *Store, request Request) *Writer {
	t.Helper()
	w, err := s.Reserve(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := w.Close(); err != nil {
			t.Error(err)
		}
	})
	return w
}

func assertReservedCharge(t *testing.T, s *Store, id string) {
	t.Helper()
	ids, used, err := s.scan()
	if err != nil || len(ids) != 1 || ids[0] != id || used != stateLimit+testLimits().MaxEncodedBytes+metadataReservation {
		t.Fatal("reservation accounting", ids, used, err)
	}
	if r, err := s.Acquire(context.Background(), id, testIdentity); !errors.Is(err, ErrUnavailable) && !errors.Is(err, ErrBusy) {
		if r != nil {
			r.Close()
		}
		t.Fatal("incomplete reservation exposed", err)
	}
}

func TestReservedWriterBindingAndExactResults(t *testing.T) {
	for _, codec := range []string{"none", "lz4_frame"} {
		for _, empty := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/empty=%t", codec, empty), func(t *testing.T) {
				s := openTestStore(t)
				request := testRequest()
				request.Limits.Compression = codec
				w := reserveTest(t, s, request)
				id, fence := w.ID(), w.Fence()
				assertReservedCharge(t, s, id)
				if err := w.Write(context.Background(), nil); !errors.Is(err, ErrSchemaUnbound) {
					t.Fatal("unbound write", err)
				}
				if m, err := w.Commit(context.Background(), testIdentity); !errors.Is(err, ErrSchemaUnbound) || m.ID != "" {
					t.Fatal("unbound commit", m, err)
				}
				names, err := directoryNames(w.dir, 8)
				if err != nil || len(names) != 2 {
					t.Fatal("unbound writer created payloads", names, err)
				}
				record := testRecord(t)
				defer record.Release()
				for _, current := range []Identity{{}, {Owner: "other", AuthorizationSHA256: testIdentity.AuthorizationSHA256}, {Owner: testIdentity.Owner, AuthorizationSHA256: strings.Repeat("b", 64)}} {
					if err = w.BindSchema(context.Background(), current, record.Schema()); !errors.Is(err, ErrFenced) {
						t.Fatal("untrusted identity bound schema", err)
					}
				}
				if err = w.BindSchema(context.Background(), testIdentity, nil); !errors.Is(err, ErrInvalid) {
					t.Fatal("nil schema bound", err)
				}
				oversized := arrow.NewSchema([]arrow.Field{{Name: strings.Repeat("x", schemaMaxStringBytes+1), Type: arrow.PrimitiveTypes.Int64}}, nil)
				if err = w.BindSchema(context.Background(), testIdentity, oversized); !errors.Is(err, ErrLimit) {
					t.Fatal("schema preflight bypassed", err)
				}
				if _, err = os.Stat(filepath.Join(w.dir.Name(), "schema.arrow")); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("invalid bind wrote a schema", err)
				}
				if err = w.BindSchema(context.Background(), testIdentity, record.Schema()); err != nil {
					t.Fatal(err)
				}
				for _, schema := range []*arrow.Schema{record.Schema(), arrow.NewSchema(nil, nil)} {
					if err = w.BindSchema(context.Background(), testIdentity, schema); !errors.Is(err, ErrInvalid) {
						t.Fatal("schema rebound", err)
					}
				}
				if w.ID() != id || w.Fence() != fence {
					t.Fatal("binding changed reservation identity")
				}
				if !empty {
					if err = w.Write(context.Background(), record); err != nil {
						t.Fatal(err)
					}
				}
				m, err := w.Commit(context.Background(), testIdentity)
				if err != nil || m.Version != 1 || len(m.Parts) != 1 {
					t.Fatal(m, err)
				}
				r, err := s.Acquire(context.Background(), id, testIdentity)
				if err != nil {
					t.Fatal(err)
				}
				defer r.Close()
				p, err := r.OpenPart(context.Background(), 0, testIdentity)
				if err != nil {
					t.Fatal(err)
				}
				defer p.Close()
				if !empty {
					comparePart(t, p, record)
					return
				}
				reader, err := ipc.NewReader(p)
				if err != nil {
					t.Fatal(err)
				}
				defer reader.Release()
				if m.Rows != 0 || m.Parts[0].Batches != 0 || !reader.Schema().Equal(record.Schema()) || reader.Next() || reader.Err() != nil {
					t.Fatal("empty bound export lost schema", reader.Err())
				}
			})
		}
	}
}

func TestBeginInvalidSchemaDoesNotReserve(t *testing.T) {
	s := openTestStore(t)
	if w, err := s.Begin(context.Background(), testRequest(), nil); !errors.Is(err, ErrInvalid) || w != nil {
		t.Fatal(w, err)
	}
	ids, used, err := s.scan()
	if err != nil || len(ids) != 0 || used != stateLimit {
		t.Fatal("invalid known schema consumed capacity", ids, used, err)
	}
}

func TestBindingRechecksCancellationFenceAndExpiry(t *testing.T) {
	for _, kind := range []string{"cancel", "fence", "expiry", "context"} {
		t.Run(kind, func(t *testing.T) {
			s := openTestStore(t)
			request := testRequest()
			if kind == "expiry" {
				request.ExpiresAt = time.Now().Add(500 * time.Millisecond)
			}
			w := reserveTest(t, s, request)
			record := testRecord(t)
			defer record.Release()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			want := ErrFenced
			switch kind {
			case "cancel":
				if err := s.Cancel(ctx, w.ID(), testIdentity); err != nil {
					t.Fatal(err)
				}
			case "fence":
				st := w.state
				st.Fence = strings.Repeat("f", 32)
				if _, err := s.writeState(w.dir, st); err != nil {
					t.Fatal(err)
				}
			case "expiry":
				time.Sleep(time.Until(request.ExpiresAt) + time.Millisecond)
				want = ErrUnavailable
			case "context":
				cancel()
				want = context.Canceled
			}
			if err := w.BindSchema(ctx, testIdentity, record.Schema()); !errors.Is(err, want) {
				t.Fatal("invalid binding accepted", err)
			}
			if _, err := os.Stat(filepath.Join(w.dir.Name(), "schema.arrow")); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("invalid binding published schema", err)
			}
			assertReservedCharge(t, s, w.ID())
		})
	}
}

func TestBindingCancelAndConcurrentBindSerialize(t *testing.T) {
	record := testRecord(t)
	defer record.Release()
	for i := 0; i < 20; i++ {
		s := openTestStore(t)
		w := reserveTest(t, s, testRequest())
		var group sync.WaitGroup
		group.Add(3)
		start := make(chan struct{})
		bound := make(chan error, 2)
		cancelled := make(chan error, 1)
		for range 2 {
			go func() {
				defer group.Done()
				<-start
				bound <- w.BindSchema(context.Background(), testIdentity, record.Schema())
			}()
		}
		go func() {
			defer group.Done()
			<-start
			cancelled <- s.Cancel(context.Background(), w.ID(), testIdentity)
		}()
		close(start)
		group.Wait()
		if err := <-cancelled; err != nil {
			t.Fatal(err)
		}
		success := 0
		for range 2 {
			if err := <-bound; err == nil {
				success++
			} else if !errors.Is(err, ErrFenced) && !errors.Is(err, ErrInvalid) {
				t.Fatal(err)
			}
		}
		if success > 1 {
			t.Fatal("more than one binding succeeded")
		}
		if _, err := w.Commit(context.Background(), testIdentity); err == nil {
			t.Fatal("cancelled binding committed")
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		if result, err := s.Cleanup(context.Background(), 1); err != nil || result.Removed != 1 {
			t.Fatal(result, err)
		}
	}
}

// Called by the common subprocess fixture before constructing any schema.
func reservationProcess(t *testing.T, s *Store, mode string) bool {
	t.Helper()
	if !strings.HasPrefix(mode, "reserve-") && !strings.HasPrefix(mode, "bind-crash:") {
		return false
	}
	if strings.HasPrefix(mode, "reserve-crash:") {
		point := strings.TrimPrefix(mode, "reserve-crash:")
		s.fault = func(stage string) error {
			if stage == point {
				os.Exit(0)
			}
			return nil
		}
	}
	request := testRequest()
	if mode == "reserve-expiring" {
		request.ExpiresAt = time.Now().Add(500 * time.Millisecond)
	}
	w, err := s.Reserve(context.Background(), request)
	if errors.Is(err, ErrLimit) && mode == "reserve-admit" {
		fmt.Println("limited")
		return true
	}
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if mode == "reserve-admit" || mode == "reserve-held" || mode == "reserve-expiring" {
		if mode != "reserve-admit" {
			fmt.Println(w.ID())
		} else {
			fmt.Println("admitted")
		}
		io.Copy(io.Discard, os.Stdin)
		if mode == "reserve-expiring" {
			os.Exit(0) // abandon without Close/Cancel, as a dead worker would
		}
		return true
	}
	if strings.HasPrefix(mode, "bind-crash:") {
		point := strings.TrimPrefix(mode, "bind-crash:")
		s.fault = func(stage string) error {
			if stage == point {
				os.Exit(0)
			}
			return nil
		}
		record := testRecord(t)
		defer record.Release()
		if err = w.BindSchema(context.Background(), testIdentity, record.Schema()); err != nil {
			t.Fatal(err)
		}
	}
	t.Fatal("reservation crash point not reached", mode)
	return true
}

func TestActualProcessUnboundAdmissionAndDeath(t *testing.T) {
	root := filepath.Join(t.TempDir(), "root")
	cfg := testConfig(root)
	cfg.MaxStoredBytes = testLimits().MaxEncodedBytes + metadataReservation + stateLimit
	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	budget := "KELVO_EXPORT_TEST_BUDGET=" + strconv.FormatInt(cfg.MaxStoredBytes, 10)
	one := startProcessFixture(t, root, "reserve-admit", budget)
	two := startProcessFixture(t, root, "reserve-admit", budget)
	a, b := fixtureLine(t, one), fixtureLine(t, two)
	if !((a == "admitted" && b == "limited") || (b == "admitted" && a == "limited")) {
		t.Fatal("cross-process unbound over-admission", a, b)
	}
	fixtureWait(t, one)
	fixtureWait(t, two)
	if _, err = s.Reserve(context.Background(), testRequest()); !errors.Is(err, ErrLimit) {
		t.Fatal("cancelled reservation released capacity before cleanup", err)
	}
	if result, err := s.Cleanup(context.Background(), 1); err != nil || result.Removed != 1 {
		t.Fatal(result, err)
	}
	held := startProcessFixture(t, root, "reserve-held", budget)
	id := fixtureLine(t, held)
	if !idPattern.MatchString(id) {
		t.Fatal(id)
	}
	if err = held.command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-held.done:
		if err == nil {
			t.Fatal("killed helper exited normally")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("killed helper not reaped")
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	assertReservedCharge(t, s, id)
	if result, err := s.Cleanup(context.Background(), 1); err != nil || result.Removed != 0 {
		t.Fatal("process death reclaimed unexpired reservation", result, err)
	}
	if _, err = s.Reserve(context.Background(), testRequest()); !errors.Is(err, ErrLimit) {
		t.Fatal("process death lost charge", err)
	}
	if err = s.Cancel(context.Background(), id, testIdentity); err != nil {
		t.Fatal(err)
	}
	if result, err := s.Cleanup(context.Background(), 1); err != nil || result.Removed != 1 {
		t.Fatal(result, err)
	}
}

func TestReservationAndBindingCrashCutpoints(t *testing.T) {
	for _, mode := range []string{
		"reserve-crash:reserve:after_intent", "reserve-crash:reserve:after_mkdir", "reserve-crash:reserve:after_lease", "reserve-crash:reserve:after_state", "reserve-crash:reserve:root_sync",
		"bind-crash:schema.arrow:file_sync", "bind-crash:schema.arrow:dir_sync", "bind-crash:bind:after_schema", "bind-crash:state.yml:file_sync", "bind-crash:state.yml:staged", "bind-crash:state.yml:dir_sync",
	} {
		t.Run(mode, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "root")
			fixture := startProcessFixture(t, root, mode)
			fixtureWait(t, fixture)
			s, err := Open(testConfig(root))
			if err != nil {
				t.Fatal("crash made reservation unrecoverable", err)
			}
			defer s.Close()
			ids, used, err := s.scan()
			if err != nil || len(ids) != 1 || used != stateLimit+testLimits().MaxEncodedBytes+metadataReservation {
				t.Fatal("crash lost reservation", ids, used, err)
			}
			if r, err := s.Acquire(context.Background(), ids[0], testIdentity); err == nil {
				r.Close()
				t.Fatal("crash published a result")
			}
			if strings.HasPrefix(mode, "bind-crash:") {
				dir, err := childDir(s.root, ids[0], false)
				if err != nil {
					t.Fatal(err)
				}
				st, err := s.loadState(dir, ids[0])
				dir.Close()
				want := "reserved"
				if mode == "bind-crash:state.yml:dir_sync" {
					want = "active"
				}
				if err != nil || st.Status != want {
					t.Fatal("binding recovered an uncommitted transition", st.Status, err)
				}
			}
			incomplete := strings.HasPrefix(mode, "reserve-crash:") && mode != "reserve-crash:reserve:root_sync"
			result, err := s.Cleanup(context.Background(), 1)
			if err != nil {
				t.Fatal(err)
			}
			if incomplete {
				if result.Removed != 1 {
					t.Fatal("initialization intent not recovered", result)
				}
			} else {
				if result.Removed != 0 {
					t.Fatal("durable reservation reclaimed early", result)
				}
				if err = s.Cancel(context.Background(), ids[0], testIdentity); err != nil {
					t.Fatal(err)
				}
				if result, err = s.Cleanup(context.Background(), 1); err != nil || result.Removed != 1 {
					t.Fatal(result, err)
				}
			}
			ids, used, err = s.scan()
			if err != nil || len(ids) != 0 || used != stateLimit {
				t.Fatal("cleanup left a charge", ids, used, err)
			}
		})
	}
}

func TestUnboundExpiryAfterProcessDeathReclaimsOnlyAtCleanup(t *testing.T) {
	root := filepath.Join(t.TempDir(), "root")
	fixture := startProcessFixture(t, root, "reserve-expiring")
	id := fixtureLine(t, fixture)
	fixtureWait(t, fixture)
	time.Sleep(500 * time.Millisecond)
	s, err := Open(testConfig(root))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	assertReservedCharge(t, s, id)
	if result, err := s.Cleanup(context.Background(), 1); err != nil || result.Removed != 1 {
		t.Fatal("expired unbound entry not reclaimed", result, err)
	}
	ids, used, err := s.scan()
	if err != nil || len(ids) != 0 || used != stateLimit {
		t.Fatal(ids, used, err)
	}
}

func TestBindingStopsWhenContextExpiresAfterSchemaPublication(t *testing.T) {
	s := openTestStore(t)
	w := reserveTest(t, s, testRequest())
	record := testRecord(t)
	defer record.Release()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.fault = func(stage string) error {
		if stage == "bind:after_schema" {
			cancel()
		}
		return nil
	}
	err := w.BindSchema(ctx, testIdentity, record.Schema())
	s.fault = nil
	if !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled bind published active state", err)
	}
	if st, err := s.loadState(w.dir, w.ID()); err != nil || st.Status != "reserved" {
		t.Fatal(st.Status, err)
	}
	if err = w.BindSchema(context.Background(), testIdentity, record.Schema()); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled bind adopted orphan schema", err)
	}
	assertReservedCharge(t, s, w.ID())
}

func TestBindingFailuresRemainUnavailableAndCharged(t *testing.T) {
	record := testRecord(t)
	defer record.Release()
	for _, point := range []string{"schema.arrow:file_sync", "schema.arrow:dir_sync", "bind:after_schema", "state.yml:file_sync", "state.yml:staged", "state.yml:dir_sync"} {
		t.Run(point, func(t *testing.T) {
			s := openTestStore(t)
			w := reserveTest(t, s, testRequest())
			injected := errors.New("injected binding I/O failure")
			s.fault = func(stage string) error {
				if stage == point {
					return injected
				}
				return nil
			}
			err := w.BindSchema(context.Background(), testIdentity, record.Schema())
			s.fault = nil
			if !errors.Is(err, injected) {
				t.Fatal("binding lost I/O failure", err)
			}
			uncertain := point == "schema.arrow:dir_sync" || point == "state.yml:dir_sync"
			if errors.Is(err, ErrPublicationUncertain) != uncertain {
				t.Fatal("incorrect publication uncertainty", err)
			}
			if err = w.BindSchema(context.Background(), testIdentity, record.Schema()); err == nil {
				t.Fatal("failed binding retried")
			}
			if err = w.Write(context.Background(), record); err == nil {
				t.Fatal("failed binding wrote data")
			}
			if _, err = w.Commit(context.Background(), testIdentity); err == nil {
				t.Fatal("failed binding committed")
			}
			if err = w.Close(); err != nil {
				t.Fatal(err)
			}
			assertReservedCharge(t, s, w.ID())
			if uncertain {
				if result, err := s.Cleanup(context.Background(), 1); err != nil || result.Removed != 0 {
					t.Fatal("uncertain binding cancelled by close", result, err)
				}
			}
			if err = s.Cancel(context.Background(), w.ID(), testIdentity); err != nil {
				t.Fatal(err)
			}
			if result, err := s.Cleanup(context.Background(), 1); err != nil || result.Removed != 1 {
				t.Fatal(result, err)
			}
		})
	}
}

func TestConflictingPendingBindingsArePreserved(t *testing.T) {
	for _, kind := range []string{"identity", "limits", "fence", "hash", "schema-missing", "schema-corrupt", "unknown-version"} {
		t.Run(kind, func(t *testing.T) {
			s := openTestStore(t)
			w := reserveTest(t, s, testRequest())
			record := testRecord(t)
			defer record.Release()
			raw, err := canonicalSchema(record.Schema())
			if err != nil {
				t.Fatal(err)
			}
			next := w.state
			next.Status, next.SchemaSHA256 = "active", checksum(raw)
			switch kind {
			case "identity":
				next.Identity.Owner = "other"
			case "limits":
				next.Limits.MaxRows++
			case "fence":
				next.Fence = strings.Repeat("f", 32)
			case "hash":
				next.SchemaSHA256 = strings.Repeat("e", 64)
			case "schema-corrupt":
				raw = []byte("not Arrow")
				next.SchemaSHA256 = checksum(raw)
			case "unknown-version":
				next.Version = 3
			}
			if kind != "schema-missing" {
				if _, err = s.atomicWrite(w.dir, "schema.arrow", raw, true, true); err != nil {
					t.Fatal(err)
				}
			}
			pending, err := yaml.Marshal(next)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(w.dir.Name(), ".state.next.yml")
			if err = os.WriteFile(path, pending, 0600); err != nil {
				t.Fatal(err)
			}
			if _, _, err = s.scan(); !errors.Is(err, ErrCorrupt) {
				t.Fatal("conflicting bind slot accepted", err)
			}
			if got, err := os.ReadFile(path); err != nil || string(got) != string(pending) {
				t.Fatal("conflicting bind slot deleted or changed", err)
			}
			if err = w.Close(); !errors.Is(err, ErrCorrupt) {
				t.Fatal("close hid conflicting metadata", err)
			}
		})
	}
}

func TestEntryVersionsAndLegacyReadCompatibility(t *testing.T) {
	s := openTestStore(t)
	record := testRecord(t)
	defer record.Release()
	w := beginTest(t, s, record, testRequest())
	if w.state.Version != 2 {
		t.Fatal("new Begin did not use v2 lifecycle")
	}
	if err := w.Write(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	m, err := w.Commit(context.Background(), testIdentity)
	if err != nil {
		t.Fatal(err)
	}
	dir, err := childDir(s.root, m.ID, false)
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()
	st, err := s.loadState(dir, m.ID)
	if err != nil {
		t.Fatal(err)
	}
	// The v1 writer used the same manifest/payload format, with version 1 in
	// state.yml. Preserve those bytes; only replace the state version here.
	st.Version = 1
	if _, err = s.writeState(dir, st); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(s.config)
	if err != nil {
		t.Fatal("legacy root rejected", err)
	}
	defer s2.Close()
	r, err := s2.Acquire(context.Background(), m.ID, testIdentity)
	if err != nil {
		t.Fatal("legacy result rejected", err)
	}
	p, err := r.OpenPart(context.Background(), 0, testIdentity)
	if err != nil {
		t.Fatal(err)
	}
	comparePart(t, p, record)
	p.Close()
	r.Close()
	st.Version = 3
	if _, err = s2.writeState(dir, st); err != nil {
		t.Fatal(err)
	}
	if unexpected, err := Open(s.config); !errors.Is(err, ErrCorrupt) {
		if unexpected != nil {
			unexpected.Close()
		}
		t.Fatal("unknown entry version accepted", err)
	}
}

func TestLegacyInitializingIntentRemainsRecoverable(t *testing.T) {
	s := openTestStore(t)
	record := testRecord(t)
	defer record.Release()
	w := beginTest(t, s, record, testRequest())
	st := w.state
	st.Version = 1
	if _, err := s.writeState(w.dir, st); err != nil {
		t.Fatal(err)
	}
	raw, err := yaml.Marshal(st)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.atomicWrite(s.root, w.ID()+".initializing.yml", raw, true, true); err != nil {
		t.Fatal(err)
	}
	w.release()
	ids, used, err := s.scan()
	if err != nil || len(ids) != 1 || used != stateLimit+testLimits().MaxEncodedBytes+metadataReservation {
		t.Fatal("legacy initialization lost charge", ids, used, err)
	}
	if result, err := s.Cleanup(context.Background(), 1); err != nil || result.Removed != 1 {
		t.Fatal("legacy initialization not recovered", result, err)
	}
}

func TestUnboundEntriesRejectUnexpectedPayloads(t *testing.T) {
	for _, name := range []string{"manifest.yml", "part-0000.arrow", ".pending-0000.arrow"} {
		t.Run(name, func(t *testing.T) {
			s := openTestStore(t)
			w := reserveTest(t, s, testRequest())
			immutable := !strings.HasPrefix(name, ".pending-")
			if _, err := s.atomicWrite(w.dir, name, []byte("preserve"), immutable, true); err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Cleanup(context.Background(), 1); !errors.Is(err, ErrCorrupt) {
				t.Fatal("unbound payload accepted", err)
			}
			if raw, err := os.ReadFile(filepath.Join(s.config.Directory, w.ID(), name)); err != nil || string(raw) != "preserve" {
				t.Fatal("unexpected payload removed", err)
			}
		})
	}
}
