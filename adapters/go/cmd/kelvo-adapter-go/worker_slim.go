//go:build slim

package main

import "context"

// The slim adapter serves warm read workers only: no one-shot operations, and
// no engines beyond the relational ones.
const (
	hasWorkerMode = false
	usageModes    = "kelvo-adapter-relational serve [--pool-size N]"
)

func runWorker(context.Context) {}
