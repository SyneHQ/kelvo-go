// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package containment

import (
	"context"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/admission"
)

func TestReservationCustodyKeepsExactIdentityUntilOperationCompletion(t *testing.T) {
	for _, class := range []admission.Class{admission.ClassInteractive, admission.ClassRefresh, admission.ClassExport} {
		t.Run(string(class), func(t *testing.T) {
			pool, err := admission.New(admission.Limits{MaxConcurrent: 1, MemoryBytes: 128,
				Classes: map[admission.Class]admission.ClassLimits{admission.ClassExport: {MaxConcurrent: 1, MemoryBytes: 128}}})
			if err != nil {
				t.Fatal(err)
			}
			reservation, err := pool.Acquire(context.Background(), admission.Request{Class: class, MemoryBytes: 128})
			if err != nil {
				t.Fatal(err)
			}
			custody, err := NewReservationCustody(reservation)
			if err != nil || !custody.ownsReservation(pool, 128) || custody.ownsReservation(pool, 127) {
				t.Fatal("custody lost the original full reservation", err)
			}
			unbound, _ := NewCustody(func() {})
			copy := *reservation
			copied, _ := NewReservationCustody(&copy)
			if unbound.ownsReservation(pool, 128) || copied.ownsReservation(pool, 128) {
				t.Fatal("unbound or copied custody fabricated admission")
			}
			hold, _ := custody.Hold()
			custody.Complete()
			if custody.ownsReservation(pool, 128) || pool.Snapshot().Active != 1 {
				t.Fatal("completed custody admitted new native work or released a hold")
			}
			hold()
			if pool.Snapshot().Active != 0 {
				t.Fatal("physical completion did not release the existing reservation")
			}
		})
	}
}
