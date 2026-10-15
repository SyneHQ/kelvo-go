//go:build !slim

package runtime

import "github.com/SYNEHQ/kelvo-go/adapter"

// readSink wraps the result sink for engines whose rows need conversion.
func readSink(engine string, next *arrowSink, maxBytes int64) adapter.Sink {
	if engine == "mongodb" {
		return &mongoJSONSink{next: next, maxBytes: maxBytes}
	}
	return next
}
