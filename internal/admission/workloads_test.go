// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package admission

import (
	"context"
	"errors"
	"math"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"time"
)

func enabledWorkloadLimits() Limits {
	return Limits{
		MaxConcurrent: 6, MemoryBytes: 120, ScratchBytes: 120,
		Classes: map[Class]ClassLimits{ClassExport: {MaxConcurrent: 3, MemoryBytes: 80, ScratchBytes: 80}},
	}
}

func TestWorkloadSelectorsPreserveLegacyCallers(t *testing.T) {
	for _, tc := range []struct {
		name       string
		request    Request
		class      Class
		background bool
	}{
		{"legacy interactive", Request{}, ClassInteractive, false},
		{"legacy refresh", Request{Background: true}, ClassRefresh, true},
		{"explicit interactive", Request{Class: ClassInteractive}, ClassInteractive, false},
		{"explicit export", Request{Class: ClassExport}, ClassExport, true},
		{"explicit refresh", Request{Class: ClassRefresh}, ClassRefresh, true},
		{"consistent refresh", Request{Class: ClassRefresh, Background: true}, ClassRefresh, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := newPool(t, enabledWorkloadLimits())
			tc.request.MemoryBytes, tc.request.ScratchBytes = 11, 7
			r, err := p.TryAcquire(tc.request)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Release()
			s := p.Snapshot()
			used := Request{MemoryBytes: 11, ScratchBytes: 7}
			if s.Active != 1 || s.Used != used || s.Classes[tc.class].Active != 1 || s.Classes[tc.class].Used != used {
				t.Fatalf("incorrect class or legacy totals: %+v", s)
			}
			if tc.background {
				if s.BackgroundActive != 1 || s.BackgroundUsed != used {
					t.Fatalf("background totals: %+v", s)
				}
			} else if s.BackgroundActive != 0 || s.BackgroundUsed != (Request{}) {
				t.Fatalf("interactive charged as background: %+v", s)
			}
		})
	}
}

func TestWorkloadSelectorErrorsDoNotQueue(t *testing.T) {
	p := newPool(t, Limits{MaxConcurrent: 2, MemoryBytes: 100})
	for _, tc := range []struct {
		request Request
		want    error
	}{
		{Request{Class: "unknown", MemoryBytes: 1}, ErrInvalid},
		{Request{Class: "Interactive", MemoryBytes: 1}, ErrInvalid},
		{Request{Class: ClassInteractive, Background: true, MemoryBytes: 1}, ErrInvalid},
		{Request{Class: ClassExport, Background: true, MemoryBytes: 1}, ErrInvalid},
		{Request{Class: ClassExport, MemoryBytes: 1}, ErrDisabled},
	} {
		for _, wait := range []bool{false, true} {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			var r *Reservation
			var err error
			if wait {
				r, err = p.Acquire(ctx, tc.request)
			} else {
				r, err = p.TryAcquire(tc.request)
			}
			cancel()
			r.Release()
			if !errors.Is(err, tc.want) {
				t.Fatalf("request %+v wait=%v: got %v want %v", tc.request, wait, err, tc.want)
			}
		}
	}
	if s := p.Snapshot(); s.Active != 0 || s.Waiting != 0 || s.Classes[ClassExport].Enabled {
		t.Fatalf("invalid/disabled request admitted: %+v", s)
	}
}

func TestWorkloadLimitsRejectInvalidDeclarations(t *testing.T) {
	valid := ClassLimits{MaxConcurrent: 2, MemoryBytes: 50, ScratchBytes: 40}
	for _, declarations := range []map[Class]ClassLimits{
		{"": valid},
		{"unknown": valid},
		{ClassExport: {}},
		{ClassExport: {MaxConcurrent: -1, MemoryBytes: 50}},
		{ClassExport: {MaxConcurrent: 5, MemoryBytes: 50}},
		{ClassExport: {MaxConcurrent: 2}},
		{ClassExport: {MaxConcurrent: 2, MemoryBytes: -1}},
		{ClassExport: {MaxConcurrent: 2, MemoryBytes: 101}},
		{ClassExport: {MaxConcurrent: 2, MemoryBytes: 50, ScratchBytes: -1}},
		{ClassExport: {MaxConcurrent: 2, MemoryBytes: 50, ScratchBytes: 81}},
		{ClassInteractive: {}, ClassExport: valid},
		{ClassRefresh: {}, ClassExport: valid},
		{ClassInteractive: valid, ClassExport: valid, ClassRefresh: valid, "other": valid},
	} {
		if _, err := New(Limits{MaxConcurrent: 4, MemoryBytes: 100, ScratchBytes: 80, Classes: declarations}); !errors.Is(err, ErrInvalid) {
			t.Fatalf("invalid class config accepted: %+v: %v", declarations, err)
		}
	}
}

func TestWorkloadClassCeilingsLeaveSharedCapacityAvailable(t *testing.T) {
	for _, class := range []Class{ClassInteractive, ClassExport, ClassRefresh} {
		for _, tc := range []struct {
			name    string
			ceiling ClassLimits
			request Request
		}{
			{"slots", ClassLimits{MaxConcurrent: 1, MemoryBytes: 80, ScratchBytes: 80}, Request{MemoryBytes: 10, ScratchBytes: 10}},
			{"memory", ClassLimits{MaxConcurrent: 3, MemoryBytes: 15, ScratchBytes: 80}, Request{MemoryBytes: 10, ScratchBytes: 10}},
			{"scratch", ClassLimits{MaxConcurrent: 3, MemoryBytes: 80, ScratchBytes: 15}, Request{MemoryBytes: 10, ScratchBytes: 10}},
		} {
			t.Run(string(class)+"/"+tc.name, func(t *testing.T) {
				limits := enabledWorkloadLimits()
				limits.Classes[class] = tc.ceiling
				p := newPool(t, limits)
				tc.request.Class = class
				r, err := p.TryAcquire(tc.request)
				if err != nil {
					t.Fatal(err)
				}
				defer r.Release()
				blocked, err := p.TryAcquire(tc.request)
				blocked.Release()
				if !errors.Is(err, ErrBusy) {
					t.Fatalf("class ceiling bypassed: %v", err)
				}
				other := ClassInteractive
				if class == other {
					other = ClassRefresh
				}
				available, err := p.TryAcquire(Request{Class: other, MemoryBytes: 10, ScratchBytes: 10})
				if err != nil {
					t.Fatalf("class ceiling consumed another class's capacity: %v", err)
				}
				available.Release()
			})
		}
	}
}

func TestWorkloadClassesShareEveryGlobalCeiling(t *testing.T) {
	for _, tc := range []struct {
		name    string
		limits  Limits
		request Request
	}{
		{"slots", Limits{MaxConcurrent: 2, MemoryBytes: 100, ScratchBytes: 100}, Request{MemoryBytes: 1}},
		{"memory", Limits{MaxConcurrent: 6, MemoryBytes: 100, ScratchBytes: 100}, Request{MemoryBytes: 50}},
		{"scratch", Limits{MaxConcurrent: 6, MemoryBytes: 100, ScratchBytes: 100}, Request{MemoryBytes: 1, ScratchBytes: 50}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ceiling := ClassLimits{MaxConcurrent: tc.limits.MaxConcurrent, MemoryBytes: tc.limits.MemoryBytes, ScratchBytes: tc.limits.ScratchBytes}
			// Class ceilings may sum above global capacity; they do not create it.
			tc.limits.Classes = map[Class]ClassLimits{ClassInteractive: ceiling, ClassRefresh: ceiling, ClassExport: ceiling}
			p := newPool(t, tc.limits)
			for _, class := range []Class{ClassInteractive, ClassRefresh} {
				tc.request.Class = class
				r, err := p.TryAcquire(tc.request)
				if err != nil {
					t.Fatal(err)
				}
				defer r.Release()
			}
			tc.request.Class = ClassExport
			r, err := p.TryAcquire(tc.request)
			r.Release()
			if !errors.Is(err, ErrBusy) {
				t.Fatalf("export bypassed shared global ceiling: %v", err)
			}
		})
	}
}

func TestWorkloadExportAndRefreshJointlyProtectInteractiveCapacity(t *testing.T) {
	for _, tc := range []struct {
		name        string
		limits      Limits
		background  Request
		interactive Request
	}{
		{"slots", Limits{MaxConcurrent: 3, MemoryBytes: 100, ScratchBytes: 100, ReservedSlots: 1}, Request{MemoryBytes: 1}, Request{MemoryBytes: 1}},
		{"memory", Limits{MaxConcurrent: 6, MemoryBytes: 100, ScratchBytes: 100, ReservedMemoryBytes: 40}, Request{MemoryBytes: 30}, Request{MemoryBytes: 40}},
		{"scratch", Limits{MaxConcurrent: 6, MemoryBytes: 100, ScratchBytes: 100, ReservedScratchBytes: 40}, Request{MemoryBytes: 1, ScratchBytes: 30}, Request{MemoryBytes: 1, ScratchBytes: 40}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.limits.Classes = map[Class]ClassLimits{ClassExport: {MaxConcurrent: tc.limits.MaxConcurrent, MemoryBytes: 100, ScratchBytes: 100}}
			p := newPool(t, tc.limits)
			refreshRequest := tc.background
			refreshRequest.Background = true
			refresh, err := p.TryAcquire(refreshRequest)
			if err != nil {
				t.Fatal(err)
			}
			defer refresh.Release()
			exportRequest := tc.background
			exportRequest.Class = ClassExport
			export, err := p.TryAcquire(exportRequest)
			if err != nil {
				t.Fatal(err)
			}
			defer export.Release()
			for _, request := range []Request{refreshRequest, exportRequest} {
				r, err := p.TryAcquire(request)
				r.Release()
				if !errors.Is(err, ErrBusy) {
					t.Fatalf("separate background classes bypassed union ceiling: %v", err)
				}
			}
			interactive, err := p.TryAcquire(tc.interactive)
			if err != nil {
				t.Fatalf("interactive protected capacity unavailable: %v", err)
			}
			defer interactive.Release()
			if s := p.Snapshot(); s.BackgroundActive != 2 || s.Classes[ClassExport].Active != 1 || s.Classes[ClassRefresh].Active != 1 {
				t.Fatalf("union background accounting: %+v", s)
			}
		})
	}
}

func TestWorkloadOversizeNeverWaits(t *testing.T) {
	for _, class := range []Class{ClassInteractive, ClassExport, ClassRefresh} {
		limits := enabledWorkloadLimits()
		limits.Classes[class] = ClassLimits{MaxConcurrent: 2, MemoryBytes: 20, ScratchBytes: 0}
		p := newPool(t, limits)
		for _, request := range []Request{{Class: class, MemoryBytes: 21}, {Class: class, MemoryBytes: 1, ScratchBytes: 1}} {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			r, err := p.Acquire(ctx, request)
			cancel()
			r.Release()
			if !errors.Is(err, ErrOversize) || p.Snapshot().Waiting != 0 {
				t.Fatalf("oversize class request queued: %+v: %v", request, err)
			}
		}
	}
	limits := enabledWorkloadLimits()
	limits.ReservedMemoryBytes, limits.ReservedScratchBytes = 60, 60
	p := newPool(t, limits)
	for _, class := range []Class{ClassExport, ClassRefresh} {
		for _, request := range []Request{{Class: class, MemoryBytes: 61}, {Class: class, MemoryBytes: 1, ScratchBytes: 61}} {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			r, err := p.Acquire(ctx, request)
			cancel()
			r.Release()
			if !errors.Is(err, ErrOversize) || p.Snapshot().Waiting != 0 {
				t.Fatalf("oversize protected-background request queued: %+v: %v", request, err)
			}
		}
	}
}

func TestWorkloadWaiterLifecycle(t *testing.T) {
	for _, action := range []string{"release", "cancel", "drain"} {
		t.Run(action, func(t *testing.T) {
			limits := enabledWorkloadLimits()
			limits.Classes[ClassExport] = ClassLimits{MaxConcurrent: 1, MemoryBytes: 80, ScratchBytes: 80}
			p := newPool(t, limits)
			request := Request{Class: ClassExport, MemoryBytes: 10, ScratchBytes: 10}
			held, err := p.TryAcquire(request)
			if err != nil {
				t.Fatal(err)
			}
			defer held.Release()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			type outcome struct {
				reservation *Reservation
				err         error
			}
			done := make(chan outcome, 1)
			go func() { r, err := p.Acquire(ctx, request); done <- outcome{r, err} }()
			awaitWaiting(t, p, 1)
			if s := p.Snapshot(); s.Classes[ClassExport].Waiting != 1 || s.Classes[ClassRefresh].Waiting != 0 || s.Classes[ClassInteractive].Waiting != 0 {
				t.Fatalf("wait charged to wrong class: %+v", s)
			}
			interactive, err := p.TryAcquire(Request{MemoryBytes: 10})
			if err != nil {
				t.Fatalf("class waiter blocked unrelated work: %v", err)
			}
			interactive.Release()
			var want error
			switch action {
			case "release":
				held.Release()
			case "cancel":
				cancel()
				want = context.Canceled
			case "drain":
				p.Drain()
				want = ErrDraining
			}
			select {
			case result := <-done:
				defer result.reservation.Release()
				if !errors.Is(result.err, want) {
					t.Fatalf("wait outcome: got %v want %v", result.err, want)
				}
				if s := p.Snapshot(); s.Waiting != 0 || s.Classes[ClassExport].Waiting != 0 || s.Active != 1 || s.Classes[ClassExport].Active != 1 {
					t.Fatalf("wait cleanup released active execution or leaked waiter: %+v", s)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("class waiter did not finish")
			}
		})
	}
}

func TestWorkloadConfigurationAndSnapshotsAreDetached(t *testing.T) {
	limits := enabledWorkloadLimits()
	want := limits.Classes[ClassExport]
	p := newPool(t, limits)
	limits.Classes[ClassExport] = ClassLimits{MaxConcurrent: 99}
	limits.Classes["unknown"] = ClassLimits{}
	first := p.Snapshot()
	if len(first.Classes) != 3 || len(first.Limits.Classes) != 1 || first.Classes[ClassExport].Limits != want || first.Limits.Classes[ClassExport] != want {
		t.Fatalf("constructor retained caller's map: %+v", first)
	}
	delete(first.Limits.Classes, ClassExport)
	first.Classes[ClassExport] = ClassSnapshot{}
	first.Classes["unknown"] = ClassSnapshot{}
	second := p.Snapshot()
	if len(second.Classes) != 3 || !second.Classes[ClassExport].Enabled || second.Classes[ClassExport].Limits != want || second.Limits.Classes[ClassExport] != want {
		t.Fatalf("snapshot mutation changed pool configuration: %+v", second)
	}
	r, err := p.TryAcquire(Request{Class: ClassExport, MemoryBytes: 10})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Release()
	if second.Classes[ClassExport].Active != 0 || second.Classes[ClassExport].Used != (Request{}) {
		t.Fatal("earlier snapshot changed after reservation")
	}
	defaults := newPool(t, Limits{MaxConcurrent: 2, MemoryBytes: 10}).Snapshot()
	if defaults.Limits.Classes != nil || defaults.Classes[ClassExport] != (ClassSnapshot{}) || !defaults.Classes[ClassInteractive].Enabled || !defaults.Classes[ClassRefresh].Enabled {
		t.Fatalf("incorrect default classes: %+v", defaults)
	}
}

func TestWorkloadCeilingsAvoidOverflow(t *testing.T) {
	limits := Limits{MaxConcurrent: 4, MemoryBytes: math.MaxInt64, ScratchBytes: math.MaxInt64,
		ReservedMemoryBytes: 1, ReservedScratchBytes: 1,
		Classes: map[Class]ClassLimits{ClassExport: {MaxConcurrent: 3, MemoryBytes: math.MaxInt64 - 1, ScratchBytes: math.MaxInt64 - 1}},
	}
	p := newPool(t, limits)
	r, err := p.TryAcquire(Request{Class: ClassExport, MemoryBytes: math.MaxInt64 - 2, ScratchBytes: math.MaxInt64 - 2})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Release()
	for _, class := range []Class{ClassExport, ClassRefresh} {
		blocked, err := p.TryAcquire(Request{Class: class, MemoryBytes: 2, ScratchBytes: 2})
		blocked.Release()
		if !errors.Is(err, ErrBusy) {
			t.Fatalf("overflow bypassed class/background ceiling: %v", err)
		}
	}
	refresh, err := p.TryAcquire(Request{Class: ClassRefresh, MemoryBytes: 1, ScratchBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer refresh.Release()
	interactive, err := p.TryAcquire(Request{Class: ClassInteractive, MemoryBytes: 1, ScratchBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer interactive.Release()
	if s := p.Snapshot(); s.Used != (Request{MemoryBytes: math.MaxInt64, ScratchBytes: math.MaxInt64}) {
		t.Fatalf("capacity boundary: %+v", s)
	}
}

func TestWorkloadConcurrentReleaseIsOnceOnly(t *testing.T) {
	p := newPool(t, enabledWorkloadLimits())
	before := p.Snapshot()
	var held []*Reservation
	for _, class := range []Class{ClassInteractive, ClassExport, ClassRefresh} {
		r, err := p.TryAcquire(Request{Class: class, MemoryBytes: 10, ScratchBytes: 10})
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, r)
	}
	var wg sync.WaitGroup
	for range 20 {
		for _, r := range held {
			wg.Go(r.Release)
		}
	}
	wg.Wait()
	if after := p.Snapshot(); !reflect.DeepEqual(after, before) {
		t.Fatalf("concurrent releases leaked or underflowed accounting: %+v", after)
	}
}

func TestWorkloadConcurrentMixedClassesAccountAtomically(t *testing.T) {
	limits := enabledWorkloadLimits()
	limits.ReservedSlots, limits.ReservedMemoryBytes, limits.ReservedScratchBytes = 2, 30, 30
	limits.Classes[ClassRefresh] = ClassLimits{MaxConcurrent: 2, MemoryBytes: 50, ScratchBytes: 50}
	p := newPool(t, limits)
	before := p.Snapshot()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range 120 {
		wg.Go(func() {
			<-start
			class := []Class{ClassInteractive, ClassExport, ClassRefresh}[i%3]
			r, err := p.Acquire(ctx, Request{Class: class, MemoryBytes: int64(10 + i%3), ScratchBytes: int64(12 + i%3)})
			if err != nil {
				t.Error(err)
				return
			}
			defer r.Release()
			runtime.Gosched()
			s := p.Snapshot()
			var used, background Request
			var active, waiting, backgroundActive int
			for class, c := range s.Classes {
				if c.Active < 0 || c.Active > c.Limits.MaxConcurrent || c.Used.MemoryBytes < 0 || c.Used.MemoryBytes > c.Limits.MemoryBytes || c.Used.ScratchBytes < 0 || c.Used.ScratchBytes > c.Limits.ScratchBytes || c.Waiting < 0 {
					t.Errorf("class ceiling violated: %s: %+v", class, c)
				}
				used.MemoryBytes += c.Used.MemoryBytes
				used.ScratchBytes += c.Used.ScratchBytes
				active += c.Active
				waiting += c.Waiting
				if class != ClassInteractive {
					background.MemoryBytes += c.Used.MemoryBytes
					background.ScratchBytes += c.Used.ScratchBytes
					backgroundActive += c.Active
				}
			}
			if used != s.Used || active != s.Active || waiting != s.Waiting || background != s.BackgroundUsed || backgroundActive != s.BackgroundActive {
				t.Errorf("non-atomic aggregate accounting: %+v", s)
			}
			if active > limits.MaxConcurrent || used.MemoryBytes > limits.MemoryBytes || used.ScratchBytes > limits.ScratchBytes || backgroundActive > limits.MaxConcurrent-limits.ReservedSlots || background.MemoryBytes > limits.MemoryBytes-limits.ReservedMemoryBytes || background.ScratchBytes > limits.ScratchBytes-limits.ReservedScratchBytes {
				t.Errorf("global or protected-background ceiling violated: %+v", s)
			}
			// Mutating independently owned snapshots must remain race-free.
			delete(s.Limits.Classes, ClassExport)
			s.Classes[ClassExport] = ClassSnapshot{}
		})
	}
	close(start)
	wg.Wait()
	if after := p.Snapshot(); !reflect.DeepEqual(after, before) {
		t.Fatalf("mixed workloads leaked reservations/waiters or config: %+v", after)
	}
}
