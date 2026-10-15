//go:build slim

package runtime

import "github.com/SYNEHQ/kelvo-go/adapter"

// readSink is the identity: the slim build serves relational engines only.
func readSink(_ string, next *arrowSink, _ int64) adapter.Sink { return next }
