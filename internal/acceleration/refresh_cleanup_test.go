// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package acceleration

import (
	"context"
	"errors"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
)

type abortFailureBackend struct {
	Backend
	failure error
	calls   int
}
type abortFailureWriter struct {
	RefreshWriter
	SchemaWriter
	owner *abortFailureBackend
}
type abortFailureMultipartWriter struct {
	MultipartRefreshWriter
	owner *abortFailureBackend
}

func (b *abortFailureBackend) Begin(ctx context.Context, id string) (RefreshWriter, error) {
	w, err := b.Backend.Begin(ctx, id)
	if err != nil {
		return nil, err
	}
	return &abortFailureWriter{RefreshWriter: w, SchemaWriter: w.(SchemaWriter), owner: b}, nil
}
func (b *abortFailureBackend) BeginMultipart(ctx context.Context, id string, options MultipartOptions) (MultipartRefreshWriter, error) {
	w, err := b.Backend.(MultipartBackend).BeginMultipart(ctx, id, options)
	if err != nil {
		return nil, err
	}
	return &abortFailureMultipartWriter{MultipartRefreshWriter: w, owner: b}, nil
}
func (w *abortFailureWriter) Abort() error {
	w.owner.calls++
	return errors.Join(w.RefreshWriter.Abort(), w.owner.failure)
}
func (w *abortFailureMultipartWriter) Abort() error {
	w.owner.calls++
	return errors.Join(w.MultipartRefreshWriter.Abort(), w.owner.failure)
}

func TestRefreshAbortFailureSurvivesSourceFailureAndPublication(t *testing.T) {
	for _, multipart := range []bool{false, true} {
		for _, sourceFails := range []bool{false, true} {
			t.Run(map[bool]string{false: "single", true: "multipart"}[multipart]+"/"+map[bool]string{false: "published", true: "source_failed"}[sourceFails], func(t *testing.T) {
				_, manager, source := managerFixture(t)
				if multipart {
					manager.config.Acceleration.Datasets[0].Multipart = &catalog.MultipartConfig{MaxPartBytes: 1 << 20, MaxParts: 4}
				}
				failure := errors.New("fixture transaction abort failure")
				backend := &abortFailureBackend{Backend: manager.store, failure: failure}
				manager.store = backend
				source.fail = sourceFails
				snapshot, err := manager.Refresh(context.Background(), "orders_fast", false)
				if !errors.Is(err, ErrRefreshCleanup) || !errors.Is(err, failure) || backend.calls != 1 {
					t.Fatal("transaction cleanup failure was lost", err, backend.calls)
				}
				if sourceFails {
					if snapshot.Generation != "" || err.Error() == failure.Error() {
						t.Fatal("partial source result was published or source error was lost")
					}
				} else {
					current, statusErr := manager.Status("orders_fast")
					if statusErr != nil || snapshot.Generation == "" || current.Generation != snapshot.Generation {
						t.Fatal("cleanup failure erased an already published generation", statusErr)
					}
				}
			})
		}
	}
}
