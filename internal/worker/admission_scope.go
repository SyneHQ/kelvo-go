// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"

	"github.com/SYNEHQ/kelvo-go/internal/admission"
)

type admissionClassKey struct{}

// withAdmissionClass overrides the class a one-shot operation reserves in;
// an inspection falling back from a read worker stays interactive.
func withAdmissionClass(ctx context.Context, class admission.Class) context.Context {
	return context.WithValue(ctx, admissionClassKey{}, class)
}

func admissionClass(ctx context.Context, fallback admission.Class) admission.Class {
	if class, ok := ctx.Value(admissionClassKey{}).(admission.Class); ok {
		return class
	}
	return fallback
}
