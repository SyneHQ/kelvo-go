// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
)

type sourceAdmissionFixture struct {
	ids    []string
	calls  int
	active bool
	err    error
}

func (f *sourceAdmissionFixture) Acquire(ctx context.Context, ids []string) (context.Context, func(), error) {
	f.calls++
	f.ids = append([]string(nil), ids...)
	if f.err != nil {
		return nil, nil, f.err
	}
	f.active = true
	return ctx, func() { f.active = false }, nil
}

func TestSourceAdmissionSelectsOnlyAuthorizedOriginalIDs(t *testing.T) {
	fixture := &sourceAdmissionFixture{}
	e := &Executor{SourceAdmission: fixture, Config: catalog.Config{
		Sources:      []catalog.Source{{ID: "orders", Type: "postgres"}, {ID: "events", Type: "clickhouse"}},
		Acceleration: &catalog.AccelerationConfig{Datasets: []catalog.Dataset{{ID: "cached"}}},
	}}
	for _, tc := range []struct {
		request query.Request
		want    []string
	}{
		{query.Request{Mode: "native", ConnectionID: "events"}, []string{"events"}},
		{query.Request{Mode: "federated", Sources: []string{"orders", "cached"}}, []string{"orders"}},
		{query.Request{Mode: "federated"}, nil},
	} {
		_, release, err := e.acquireSourceQuota(context.Background(), tc.request)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(fixture.ids, tc.want) {
			t.Fatalf("source identities: got %v want %v", fixture.ids, tc.want)
		}
		release()
	}
	calls := fixture.calls
	for _, sources := range [][]string{{"missing"}, {"orders", "orders"}} {
		if _, _, err := e.acquireSourceQuota(context.Background(), query.Request{Mode: "federated", Sources: sources}); err == nil {
			t.Fatal("invalid source selection admitted")
		}
	}
	if fixture.calls != calls {
		t.Fatal("invalid selection reached source coordinator")
	}
}

type quotaCheckingSink struct {
	t       *testing.T
	fixture *sourceAdmissionFixture
}

func (s quotaCheckingSink) Schema(*arrow.Schema) error {
	if !s.fixture.active {
		s.t.Error("source lease released before result schema")
	}
	return nil
}
func (s quotaCheckingSink) Write(arrow.RecordBatch) error {
	if !s.fixture.active {
		s.t.Error("source lease released before result batch")
	}
	return nil
}

func TestSourceAdmissionCoversWorkerResultAndReleases(t *testing.T) {
	e, err := New(catalog.Config{Sources: []catalog.Source{{ID: "orders", Type: "parquet", Path: "unused-by-fixture.parquet"}}}, query.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	fixture := &sourceAdmissionFixture{}
	e.SourceAdmission = fixture
	if _, err = e.Execute(context.Background(), query.Request{SQL: "SELECT 1", Sources: []string{"orders"}}, quotaCheckingSink{t, fixture}); err != nil {
		t.Fatal(err)
	}
	if fixture.calls != 1 || fixture.active {
		t.Fatal("source lease not acquired/released")
	}
}

func TestSourceAdmissionRejectsBeforeProcessStartup(t *testing.T) {
	rejected := errors.New("source admission rejected")
	fixture := &sourceAdmissionFixture{err: rejected}
	e := &Executor{Binary: "/does/not/exist", Limits: query.DefaultLimits(), SourceAdmission: fixture}
	if _, err := e.Execute(context.Background(), query.Request{SQL: "SELECT 1"}, &workerTestSink{}); !errors.Is(err, rejected) {
		t.Fatalf("query reached process startup: %v", err)
	}
	if fixture.active {
		t.Fatal("rejected source quota leaked")
	}
}
