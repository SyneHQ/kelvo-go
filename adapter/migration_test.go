// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package adapter

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/SYNEHQ/kelvo-go/migration"
	"github.com/SYNEHQ/kelvo-go/operations"
)

func TestMigrationInputBindsExpectedVersionAndExactSealedPlan(t *testing.T) {
	plan := migration.Plan{Version: migration.Version, Expected: migration.State{Version: -1}, Direction: "up", Files: []migration.File{{Name: "1_orders.up.sql", Content: "CREATE TABLE orders(id bigint)"}}}
	raw, _ := json.Marshal(plan)
	sum := sha256.Sum256(raw)
	request := operations.Request{Version: operations.Version, Kind: operations.MigrationApply, IdempotencyKey: "migration-1", Connection: operations.ConnectionRef{ID: "saved", Database: "app"}, Spec: operations.Spec{Migration: &operations.MigrationSpec{ID: migration.Table, ExpectedVersion: "-1", Transaction: operations.TransactionAutocommit, Plan: operations.InputRef{ID: "sealed", SHA256: hex.EncodeToString(sum[:]), Bytes: int64(len(raw)), Format: "migration_plan_v1"}}}}
	if err := ValidateOperationInput(request, "team", raw); err != nil {
		t.Fatal(err)
	}
	request.Spec.Migration.ExpectedVersion = "2"
	if ValidateOperationInput(request, "team", raw) == nil {
		t.Fatal("changed precondition accepted")
	}
	request.Spec.Migration.ExpectedVersion = "-1"
	raw[len(raw)-1] = ' '
	if ValidateOperationInput(request, "team", raw) == nil {
		t.Fatal("changed plan accepted")
	}
}

func TestMigrationStatusCannotCarryExecutableInput(t *testing.T) {
	r := operations.Request{Version: operations.Version, Kind: operations.MigrationStatus, Connection: operations.ConnectionRef{ID: "saved"}, Spec: operations.Spec{Migration: &operations.MigrationSpec{ID: migration.Table}}}
	if r.Validate() != nil || r.Kind.Mutating() || r.InputReference() != nil || ValidateOperationInput(r, "team", nil) != nil {
		t.Fatal("invalid status contract")
	}
	if ValidateOperationInput(r, "team", []byte("SQL")) == nil {
		t.Fatal("status accepted SQL")
	}
	r.Spec.Migration.Transaction = operations.TransactionAutocommit
	if r.Validate() == nil {
		t.Fatal("status accepted write settings")
	}
}
