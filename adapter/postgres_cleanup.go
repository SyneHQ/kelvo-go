package adapter

import "context"

// PostgresCleanup registers one authenticated backend before cancellation.
// The parent owns its source TLS configuration. Abort cannot change the target.
// Observed reports completion on the original authenticated source protocol;
// a sent cancellation request, timeout or closed socket is not completion.
type PostgresCleanup interface {
	Register(context.Context, uint32, []byte) (string, error)
	Abort(context.Context, string) error
	Observed(context.Context, string, bool) error
}
