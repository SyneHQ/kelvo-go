//go:build slim

package runtime

import (
	"context"
	"io"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
)

// executeInvocation runs what a slim read worker serves: queries and metadata
// inspections (see IsServeEligible). Everything else is unsupported.
func executeInvocation(ctx context.Context, input adapter.ProcessRequest, invocation adapter.Invocation, session adapter.Session, output io.Writer, receipt operations.Receipt) (operations.Receipt, error) {
	if invocation.Query != nil || invocation.Metadata != nil {
		return executeRead(ctx, input, invocation, session, output, receipt)
	}
	receipt.Outcome = operations.Rejected
	receipt.ErrorCode = "UNSUPPORTED"
	return receipt, adapter.ErrUnsupported
}
