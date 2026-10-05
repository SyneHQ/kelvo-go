// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package main

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/acceleration"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

func TestProtectedRestoreCommandPreservesOutcomeWithoutExposingCandidate(t *testing.T) {
	privateErr := query.NewError("PRIVATE_PROVIDER", "fixture-private-credential and object-key")
	for _, outcome := range []acceleration.RestoreOutcome{acceleration.RestoreNotAttempted, acceleration.RestoreNotPublished, acceleration.RestoreUnknown, acceleration.RestoreTargetObserved, acceleration.RestoreVerifiedNoOp} {
		t.Run(string(outcome), func(t *testing.T) {
			operation := acceleration.WithRestoreOutcome(privateErr, outcome)
			result := &accelerationRestoreResult{snapshot: acceleration.Snapshot{Generation: "verified-candidate", ObjectKey: "private-object-key"}, err: operation}
			var output strings.Builder
			managerClosed, runtimeClosed := false, false
			err := finishRestoreCommand(context.Background(), operation, result, func() error {
				managerClosed = true
				return privateErr
			}, func(context.Context) error {
				if !managerClosed {
					t.Fatal("runtime closed before manager")
				}
				runtimeClosed = true
				return privateErr
			}, func() error { return encodeAccelerationResult(&output, result.snapshot) })
			if err == nil || !runtimeClosed || output.Len() != 0 || !errors.Is(err, privateErr) {
				t.Fatal("restore lost operation error, skipped cleanup or exposed success")
			}
			if acceleration.RestoreOutcomeOf(err) != outcome || result.snapshot.Generation != "verified-candidate" {
				t.Fatal("cleanup replaced the backend outcome or candidate")
			}
			for _, message := range []string{err.Error(), query.PublicError(err).Message} {
				if !strings.Contains(message, "outcome: "+string(outcome)) || strings.Contains(message, "private") || strings.Contains(message, "verified-candidate") {
					t.Fatal("unsafe or missing operator outcome", message)
				}
			}
		})
	}
}

func TestProtectedRestoreCommandSuccessfulObservationSurvivesLaterFailure(t *testing.T) {
	for _, phase := range []string{"manager", "runtime", "cancelled", "output"} {
		t.Run(phase, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			privateErr := errors.New("private-cleanup-detail")
			result := &accelerationRestoreResult{snapshot: acceleration.Snapshot{Generation: "observed-target"}}
			managerClosed, runtimeClosed, outputAttempted := false, false, false
			err := finishRestoreCommand(ctx, nil, result, func() error {
				managerClosed = true
				if phase == "cancelled" {
					cancel()
				}
				if phase == "manager" {
					return privateErr
				}
				return nil
			}, func(context.Context) error {
				if !managerClosed {
					t.Fatal("runtime closed before manager")
				}
				runtimeClosed = true
				if phase == "runtime" {
					return privateErr
				}
				return nil
			}, func() error {
				outputAttempted = true
				if !managerClosed || !runtimeClosed {
					t.Fatal("output escaped cleanup")
				}
				return privateErr
			})
			if err == nil || !runtimeClosed || outputAttempted != (phase == "output") || acceleration.RestoreOutcomeOf(err) != acceleration.RestoreTargetObserved {
				t.Fatal("later failure lost observation or escaped output gate")
			}
			if strings.Contains(err.Error(), "private") || strings.Contains(query.PublicError(err).Message, "private") {
				t.Fatal("finalization exposed private diagnostics")
			}
			if phase == "cancelled" && !errors.Is(err, context.Canceled) || phase == "output" && !errors.Is(err, privateErr) {
				t.Fatal("finalization lost the error cause")
			}
		})
	}
}

func TestProtectedRestoreCommandUnknownCandidateIsNotPublication(t *testing.T) {
	operation := errors.New("unclassified restore failure")
	result := &accelerationRestoreResult{snapshot: acceleration.Snapshot{Generation: "candidate"}, err: operation}
	err := finishRestoreCommand(context.Background(), operation, result, nil, nil, nil)
	if err == nil || acceleration.RestoreOutcomeOf(err) != acceleration.RestoreNotAttempted {
		t.Fatal("candidate alone changed publication outcome")
	}
	err = finishRestoreCommand(context.Background(), operation, nil, nil, nil, nil)
	if err == nil || acceleration.RestoreOutcomeOf(err) != acceleration.RestoreNotAttempted {
		t.Fatal("failed construction invented a publication")
	}
}

func TestProtectedRestoreCommandPublishesOnlyAfterSuccessfulCleanup(t *testing.T) {
	result := &accelerationRestoreResult{snapshot: acceleration.Snapshot{Generation: "observed-target"}}
	managerClosed, runtimeClosed := false, false
	var output strings.Builder
	err := finishRestoreCommand(context.Background(), nil, result, func() error { managerClosed = true; return nil }, func(context.Context) error {
		runtimeClosed = true
		return nil
	}, func() error {
		if !managerClosed || !runtimeClosed {
			t.Fatal("output preceded cleanup")
		}
		return encodeAccelerationResult(&output, map[string]bool{"ready": true})
	})
	if err != nil || output.String() != "ready: true\n" {
		t.Fatal("successful restore lost output", err)
	}
}
