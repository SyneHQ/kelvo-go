package runtime

import (
	"context"
	"errors"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/SYNEHQ/kelvo-go/query"
)

// sourceFailureCode narrows the generic SOURCE_FAILED category to a bounded
// public code the source already proved, so an unsupported result type or an
// expired request deadline is not reported as a source outage. Consumers read
// UNSUPPORTED and DEADLINE_EXCEEDED as "nothing happened", so a receipt with
// any source effect keeps SOURCE_FAILED. Only the code crosses; driver text
// stays in err.
func sourceFailureCode(ctx context.Context, receipt operations.Receipt, err error) string {
	if receipt.ErrorCode != "SOURCE_FAILED" || receipt.Effect != operations.EffectNone || err == nil {
		return receipt.ErrorCode
	}
	// Only the request's own deadline counts; a driver connect timeout or the
	// credential-validity open deadline is still a source failure.
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return "DEADLINE_EXCEEDED"
	}
	var public *query.Error
	if errors.As(err, &public) && (public.Code == "UNSUPPORTED" || public.Code == "RESOURCE_EXHAUSTED") {
		return public.Code
	}
	if errors.Is(err, adapter.ErrUnsupported) {
		return "UNSUPPORTED"
	}
	return receipt.ErrorCode
}
