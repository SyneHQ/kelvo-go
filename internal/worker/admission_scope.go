// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"

	"github.com/SYNEHQ/kelvo-go/internal/admission"
)

type operationScopeKey struct{}

type operationScope struct{ tenant, connection string }

// WithOperationScope names the tenant and connection a durable operation runs
// for, so per-tenant and per-connection caps apply before it takes a slot.
// Trusted cluster code sets it from the operation's verified scope.
func WithOperationScope(ctx context.Context, tenant, connection string) context.Context {
	return context.WithValue(ctx, operationScopeKey{}, operationScope{tenant, connection})
}

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
