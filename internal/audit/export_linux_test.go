//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package audit

import (
	"context"
	"testing"
)

func TestExportAuditKindsSurviveJournalRestart(t *testing.T) {
	cfg := testConfig(t.TempDir())
	journal := openTest(t, cfg, testScope())
	want := map[Kind]bool{ExportSubmit: true, ExportCancel: true, ExportResults: true, ExportExecution: true}
	for kind := range want {
		receipt, err := journal.Begin(context.Background(), testBinding(), kind)
		if err != nil {
			t.Fatal(err)
		}
		if err := receipt.Finish(context.Background(), Succeeded, None); err != nil {
			t.Fatal(err)
		}
	}
	if err := journal.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, event := range allEvents(t, cfg.Directory) {
		if !want[event.Kind] || event.Binding != testBinding() || event.Outcome != Succeeded {
			t.Fatal("export audit changed on disk")
		}
		delete(want, event.Kind)
	}
	if len(want) != 0 {
		t.Fatal("export audit operation lost on disk")
	}
	if reopened := openTest(t, cfg, testScope()); reopened.Snapshot().Retained != 4 {
		t.Fatal("export retention lost on restart")
	}
}
