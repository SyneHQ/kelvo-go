// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package federation

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"sync"
	"testing"

	federationapi "github.com/SYNEHQ/kelvo-go/federation"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestDiagnosticsObserveRealScansAndNeverSerializeLiteralsOrErrors(t *testing.T) {
	ctx, collector := WithScanDiagnostics(context.Background())
	table, err := newTable(ctx, testSource(), registeredTable, query.DefaultLimits(), func(catalog.Config, query.Limits) (execution, error) {
		return &fakeExecutor{run: func(_ context.Context, request query.Request, sink query.Sink) error {
			if err := sink.Schema(idSchema); err != nil {
				return err
			}
			if strings.HasSuffix(request.SQL, " LIMIT 0") {
				return nil
			}
			if !strings.Contains(request.SQL, "918273645") {
				return errors.New("required filter was removed")
			}
			record := uintRecord(memory.DefaultAllocator, 918273645)
			defer record.Release()
			if err := sink.Write(record); err != nil {
				return err
			}
			return errors.New("secret-provider-error")
		}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer table.Close()
	if report := collector.Snapshot(); len(report.Scans) != 0 {
		t.Fatal("discovery recorded as data scan")
	}
	if _, err := table.Scan(ctx, federationapi.ScanPlan{Columns: []string{"not_registered"}}); err == nil {
		t.Fatal("invalid scan accepted")
	}
	if len(collector.Snapshot().Scans) != 0 {
		t.Fatal("rejected plan presented as an executed scan")
	}
	plan := federationapi.ScanPlan{Columns: []string{"id"}, Filters: []federationapi.Filter{{Kind: "comparison", Column: "id", Op: "eq", Type: "uint64", Value: "918273645"}}}
	reader, err := table.Scan(ctx, plan)
	if err != nil {
		t.Fatal(err)
	}
	for reader.Next() {
	}
	if reader.Err() == nil {
		t.Fatal("source failure was hidden")
	}
	reader.Release()
	table.Close()
	report := collector.Snapshot()
	if len(report.Scans) != 1 {
		t.Fatalf("scan count %d", len(report.Scans))
	}
	scan := report.Scans[0]
	if scan.Rows != 1 || scan.Batches != 1 || scan.ArrowBytes <= 0 || scan.Outcome != "error" || scan.Table != "orders" || scan.ResidualVisibility != "not_observed" {
		t.Fatalf("incorrect actual scan report: %+v", scan)
	}
	if scan.Predicates[0].Operator != "eq" || scan.Predicates[0].Type != "uint64" {
		t.Fatal("predicate shape lost")
	}
	encoded, _ := json.Marshal(report)
	for _, secret := range []string{"918273645", "secret-provider-error", "SELECT", "KELVO_SOURCE"} {
		if strings.Contains(string(encoded), secret) {
			t.Fatalf("diagnostic exposed %s", secret)
		}
	}
	if plan.Filters[0].Value != "918273645" {
		t.Fatal("diagnostics mutated required filters")
	}
	report.Scans[0].Projection[0] = "mutated"
	if collector.Snapshot().Scans[0].Projection[0] != "id" {
		t.Fatal("snapshot aliases collector")
	}
}
func TestDiagnosticReportIsBoundedConcurrentAndExplicitlyTruncated(t *testing.T) {
	_, collector := WithScanDiagnostics(context.Background())
	name := strings.Repeat("<", maxDiagnosticIdentifier)
	plan := federationapi.ScanPlan{}
	for i := 0; i < 20; i++ {
		plan.Columns = append(plan.Columns, name)
		plan.Filters = append(plan.Filters, federationapi.Filter{Kind: "comparison", Column: name, Op: "eq", Type: "int64", Value: strings.Repeat("SECRET", 1000)})
	}
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			handle := collector.begin(name, name, plan, false, "source_columns")
			handle.finish("success", math.MaxInt64, math.MaxInt64, math.MaxInt64, math.MaxInt64)
		}()
	}
	wg.Wait()
	report := collector.Snapshot()
	encoded, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) > MaxScanDiagnosticsBytes || !report.Truncated || report.OmittedScans == 0 {
		t.Fatalf("unbounded or silent truncation: %d bytes %+v", len(encoded), report)
	}
	if strings.Contains(string(encoded), "SECRET") {
		t.Fatal("literal retained")
	}
	if int64(len(report.Scans))+report.OmittedScans != 32 {
		t.Fatal("omitted scans not accounted for")
	}
}
func TestDiagnosticsBoundNestedFiltersAndInvalidVocabulary(t *testing.T) {
	_, collector := WithScanDiagnostics(context.Background())
	filter := federationapi.Filter{Kind: "comparison", Column: strings.Repeat("x", 1024), Op: "private operator", Type: "secret type", Value: "secret"}
	for i := 0; i < 100; i++ {
		filter = federationapi.Filter{Kind: "and", Children: []federationapi.Filter{filter}}
	}
	handle := collector.begin("source", "table", federationapi.ScanPlan{Filters: []federationapi.Filter{filter}}, false, "constant_row_count")
	handle.finish("success", 0, 0, 0, 0)
	report := collector.Snapshot()
	if !report.Truncated {
		t.Fatal("deep predicate truncation invisible")
	}
	encoded, _ := json.Marshal(report)
	if strings.Contains(string(encoded), "secret") {
		t.Fatal("arbitrary vocabulary retained")
	}
	if scanDiagnosticsFromContext(context.Background()) != nil {
		t.Fatal("diagnostics enabled by default")
	}
}

func TestDiagnosticsKeepDate32ShapeWithoutSignedDayLiteral(t *testing.T) {
	ctx, collector := WithScanDiagnostics(context.Background())
	schema := date32CompilerTable().schema
	table, err := newTable(ctx, testSource(), registeredTable, query.DefaultLimits(), func(catalog.Config, query.Limits) (execution, error) {
		return &fakeExecutor{run: func(_ context.Context, request query.Request, sink query.Sink) error {
			if err := sink.Schema(schema); err != nil {
				return err
			}
			if strings.HasSuffix(request.SQL, " LIMIT 0") {
				return nil
			}
			if !strings.Contains(request.SQL, "toInt32(`event_day`) = CAST('918273645' AS Int32)") {
				return errors.New("required Date32 filter was removed")
			}
			return errors.New("private-date-source-error")
		}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer table.Close()
	plan := federationapi.ScanPlan{Columns: []string{"id", "event_day"}, Filters: []federationapi.Filter{
		{Kind: "comparison", Column: "event_day", Op: "eq", Type: "date32", Value: "918273645"},
	}}
	reader, err := table.Scan(ctx, plan)
	if err != nil {
		t.Fatal(err)
	}
	if reader.Next() || reader.Err() == nil {
		t.Fatal("failed Date32 source scan was reported as successful")
	}
	reader.Release()
	_ = table.Close()
	report := collector.Snapshot()
	if len(report.Scans) != 1 || report.Truncated || len(report.Scans[0].Predicates) != 1 {
		t.Fatalf("Date32 diagnostic missing or truncated: %+v", report)
	}
	scan := report.Scans[0]
	predicate := scan.Predicates[0]
	if predicate.Kind != "comparison" || predicate.Column != "event_day" || predicate.Operator != "eq" || predicate.Type != "date32" || scan.Outcome != "error" || scan.ResidualVisibility != "not_observed" {
		t.Fatalf("Date32 diagnostic changed: %+v", scan)
	}
	encoded, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	for _, private := range []string{"918273645", "private-date-source-error", "CAST(", "SELECT", "KELVO_SOURCE"} {
		if strings.Contains(string(encoded), private) {
			t.Fatalf("Date32 diagnostic exposed %q", private)
		}
	}
	if plan.Filters[0].Value != "918273645" {
		t.Fatal("diagnostics changed the required Date32 predicate")
	}
}
