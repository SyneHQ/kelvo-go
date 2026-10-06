// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package migrations

import (
	"errors"
	"sort"
	"time"

	"github.com/SYNEHQ/kelvo-go/migration"
	"github.com/SYNEHQ/kelvo-go/operations"
	migrate "github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
)

type outcome struct {
	changed, uncertain bool
	finalState         migration.State
	finalRead          bool
}

func initialResult(plan migration.Plan) migration.Result {
	return migration.Result{Version: migration.Version, From: plan.Expected, To: plan.Expected, FilesApplied: []string{}, Effect: operations.EffectNone}
}

func applyPlan(plan migration.Plan, engine string, d database.Driver, summary *outcome) (migration.Result, error) {
	result := initialResult(plan)
	source := newSource(plan.Files)
	m, err := migrate.NewWithInstance("sealed", source, engine, d)
	if err != nil {
		return result, err
	}
	// Driver lock attempts end within five seconds, before this library timeout.
	m.LockTimeout = 7 * time.Second
	m.PrefetchMigrations = 1
	switch plan.Direction {
	case "force":
		err = m.Force(int(*plan.ForceVersion))
	case "up":
		if plan.Steps == 0 {
			err = m.Up()
		} else {
			err = m.Steps(plan.Steps)
		}
	case "down":
		if plan.Steps == 0 {
			err = m.Down()
		} else {
			err = m.Steps(-plan.Steps)
		}
	}
	// ShortLimit means all available files ran, matching the gateway's capped
	// step behavior. Failed scripts and lost receipts are never retried here.
	var short migrate.ErrShortLimit
	if errors.Is(err, migrate.ErrNoChange) || errors.As(err, &short) {
		err = nil
	}
	if summary.uncertain {
		result.Effect = operations.EffectUnknown
		return result, migration.ErrOutcomeUnknown
	}
	if summary.changed {
		if err != nil {
			result.Effect = operations.EffectPartial
		} else {
			result.Effect = operations.EffectCommitted
		}
	}
	if err != nil {
		return result, err
	}
	if !summary.finalRead {
		if summary.changed {
			result.Effect = operations.EffectUnknown
			return result, migration.ErrOutcomeUnknown
		}
		return result, migration.ErrInvalid
	}
	result.To = summary.finalState
	for _, file := range plan.Files {
		v, direction, _ := migration.ParseName(file.Name)
		if direction != plan.Direction {
			continue
		}
		if direction == "up" && v > result.From.Version && v <= result.To.Version || direction == "down" && v <= result.From.Version && v > result.To.Version {
			result.FilesApplied = append(result.FilesApplied, file.Name)
		}
	}
	sort.Slice(result.FilesApplied, func(i, j int) bool {
		a, _, _ := migration.ParseName(result.FilesApplied[i])
		b, _, _ := migration.ParseName(result.FilesApplied[j])
		if plan.Direction == "down" {
			return a > b
		}
		return a < b
	})
	return result, nil
}
