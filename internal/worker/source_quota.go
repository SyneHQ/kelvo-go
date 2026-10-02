// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"

	"github.com/SYNEHQ/kelvo-go/internal/query"
)

// SourceAdmitter lives in the trusted parent. Its returned context is canceled
// if distributed source ownership is lost. Release follows subprocess cleanup.
// Implementations must never pass coordination credentials into query children.
type SourceAdmitter interface {
	Acquire(context.Context, []string) (context.Context, func(), error)
}

func (e *Executor) acquireSourceQuota(ctx context.Context, r query.Request) (context.Context, func(), error) {
	if e.SourceAdmission == nil {
		return ctx, func() {}, nil
	}
	ids := r.Sources
	if r.Mode == "native" {
		ids = []string{r.ConnectionID}
	}
	sources, err := e.Config.Select(ids)
	if err != nil {
		return nil, nil, query.NewError("INVALID_ARGUMENT", "Unknown or duplicate source")
	}
	selected := make([]string, 0, len(sources))
	for _, source := range sources {
		// A snapshot read does not contact the dataset's original source. Do not
		// resolve the refresh SQL here or accidentally consume its source's quota.
		if source.Type != "accelerated" {
			selected = append(selected, source.ID)
		}
	}
	return e.SourceAdmission.Acquire(ctx, selected)
}
