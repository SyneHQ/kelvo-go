// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package delegation

import "context"

type Execution struct {
	Claims  Claims
	Token   string
	Binding ExecutionBinding
}
type executionKey struct{}

// WithExecution is for the trusted cluster node after it validates the running job.
func WithExecution(ctx context.Context, c Claims, token string, binding ExecutionBinding) context.Context {
	return context.WithValue(ctx, executionKey{}, Execution{cloneClaims(c), token, binding})
}
func ExecutionFromContext(ctx context.Context) (Execution, bool) {
	e, ok := ctx.Value(executionKey{}).(Execution)
	e.Claims = cloneClaims(e.Claims)
	return e, ok
}
func cloneClaims(c Claims) Claims {
	c.Sources = append([]Source(nil), c.Sources...)
	for i := range c.Sources {
		c.Sources[i].Tables = append([]Table(nil), c.Sources[i].Tables...)
	}
	if c.Subject.JobConnections != nil {
		copy := make(map[string]string, len(c.Subject.JobConnections))
		for k, v := range c.Subject.JobConnections {
			copy[k] = v
		}
		c.Subject.JobConnections = copy
	}
	return c
}
