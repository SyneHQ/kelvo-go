// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package adapter

import (
	"context"
	"strconv"

	"github.com/SYNEHQ/kelvo-go/migration"
	"github.com/SYNEHQ/kelvo-go/operations"
)

// MigrationSession owns one freshly resolved database. Status never creates
// schema objects. Apply verifies the expected state while holding its lock.
type MigrationSession interface {
	Session
	MigrationStatus(context.Context) (migration.State, error)
	ApplyMigration(context.Context, migration.Plan) (migration.Result, error)
}

func migrationInput(request operations.Request, raw []byte) error {
	spec := request.Spec.Migration
	if spec == nil || spec.ID != migration.Table {
		return ErrInvalid
	}
	if request.Kind == operations.MigrationStatus {
		if len(raw) != 0 {
			return ErrInvalid
		}
		return nil
	}
	if request.Kind != operations.MigrationApply || spec.Transaction != operations.TransactionAutocommit {
		return ErrUnsupported
	}
	plan, err := migration.ParsePlan(raw)
	if err != nil || spec.ExpectedVersion != strconv.FormatInt(plan.Expected.Version, 10) {
		return ErrInvalid
	}
	return nil
}
