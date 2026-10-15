package runtime

import (
	"context"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
)

// Runner's Open override is for ordinary composition/tests. Private sources reject it.
// Production uses an explicit ordinary or inherited-channel source path;
// the parent pipe supplies authority after durable dispatch and current resolve.
type Runner struct {
	Open         func(context.Context, adapter.ConnectionSpec, operations.Request) (adapter.Session, error)
	SessionCache *SessionCache
}
