//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/audit"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

func TestAuditCLIRequiresOfflineOwnerReadAndBoundedPagination(t *testing.T) {
	cfg := audit.Config{Directory: filepath.Join(t.TempDir(), "journal"), MaxEntries: 4, MaxPending: 2, Retention: time.Hour, WriteTimeout: time.Second}
	j, err := audit.Open(cfg, audit.Scope{ServiceID: "gateway-one", ServiceKind: "gateway", Tenants: []string{"a"}})
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close(context.Background())
	if err = j.Record(context.Background(), audit.Binding{TenantID: "a", ServiceID: "gateway-one", ServiceKind: "gateway", PrincipalKind: "unknown"}, audit.QuerySubmit, audit.Succeeded, audit.None); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	err = runAudit([]string{"read", "--directory", cfg.Directory, "--limit", "4"}, &output)
	if err == nil || query.PublicError(err).Code != "UNAVAILABLE" || output.Len() != 0 {
		t.Fatal("CLI read a live journal")
	}
	if err = j.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	err = runAudit([]string{"read", "--directory", cfg.Directory, "--limit", "4"}, &output)
	if err != nil {
		t.Fatal(err)
	}
	var page audit.Page
	if err = json.Unmarshal(output.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != 1 || !page.Done || page.Next != 4 || page.Events[0].Kind != audit.QuerySubmit {
		t.Fatal("CLI page changed durable event")
	}
	for _, args := range [][]string{{}, {"delete"}, {"read", "--limit", "257"}, {"read", "--cursor", "-1"}, {"read", "--directory", "relative"}, {"read", "unexpected"}} {
		output.Reset()
		if err = runAudit(args, &output); err == nil || output.Len() != 0 {
			t.Fatal("unbounded or invalid read accepted", args)
		}
	}
}
