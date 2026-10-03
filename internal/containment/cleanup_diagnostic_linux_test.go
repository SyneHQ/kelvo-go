//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package containment

import (
	"bytes"
	"context"
	"errors"
	"log"
	"strings"
	"testing"
)

func TestFinishDiagnosticIdentifiesFailureWithoutPrivateDetails(t *testing.T) {
	for _, stage := range []string{"deadline_before_kill", "group_kill", "population_read", "population_deadline", "usage_read", "group_remove"} {
		t.Run(stage, func(t *testing.T) {
			var output bytes.Buffer
			previous := log.Writer()
			log.SetOutput(&output)
			defer log.SetOutput(previous)
			g := &testGroup{}
			private := errors.New("private-source-credential-and-path-sentinel")
			ctx := context.Background()
			switch stage {
			case "deadline_before_kill":
				cancelled, cancel := context.WithCancel(ctx)
				cancel()
				ctx = cancelled
			case "group_kill":
				g.killErr = private
			case "population_read":
				g.populationErr = private
			case "population_deadline":
				g.populations = []bool{true}
			case "usage_read":
				g.usageErr = private
			case "group_remove":
				g.removeErr = private
			}
			released := false
			m, job := jobFixture(t, g, func() { released = true })
			if _, err := job.Finish(ctx); err != ErrQuarantined || released || !m.Status().Draining {
				t.Fatal("diagnostics changed fail-closed custody", err)
			}
			text := output.String()
			if !strings.Contains(text, "Kelvo process cleanup uncertain: stage="+stage) ||
				strings.Contains(text, private.Error()) || strings.Contains(text, job.name) {
				t.Fatal("missing stage or private details in diagnostics", text)
			}
		})
	}
}
