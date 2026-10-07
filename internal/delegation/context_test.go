// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package delegation

import (
	"context"
	"testing"
)

func TestExecutionContextOwnsClaims(t *testing.T) {
	c := Claims{Sources: []Source{{Alias: "source_1", ConnectionID: "saved-a"}}}
	token := "validated-token"
	c.Subject = Subject{Kind: "job", ID: "user-a", JobConnections: map[string]string{"saved-a": "read"}}
	c.Sources[0].Tables = []Table{{Name: "orders", Table: "orders"}}
	ctx := WithExecution(context.Background(), c, token, ExecutionBinding{JobID: "job-a", WorkerID: "worker-a"})
	c.Subject.JobConnections["saved-a"] = "write"
	c.Sources[0].Tables[0].Table = "other"
	a, ok := ExecutionFromContext(ctx)
	if !ok || a.Claims.Subject.JobConnections["saved-a"] != "read" || a.Claims.Sources[0].Tables[0].Table != "orders" {
		t.Fatal("caller mutated execution authority")
	}
	a.Claims.Subject.JobConnections["saved-a"] = "write"
	a.Claims.Sources[0].Tables[0].Table = "other"
	b, _ := ExecutionFromContext(ctx)
	if b.Claims.Subject.JobConnections["saved-a"] != "read" || b.Claims.Sources[0].Tables[0].Table != "orders" {
		t.Fatal("reader mutated execution authority")
	}
}
