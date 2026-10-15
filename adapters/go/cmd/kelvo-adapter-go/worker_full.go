//go:build !slim

package main

import (
	"context"
	"fmt"
	"os"

	"github.com/SYNEHQ/kelvo-go/adapters/go/internal/runtime"
)

const (
	hasWorkerMode = true
	usageModes    = "kelvo-adapter-go worker | serve [--pool-size N]"
)

// runWorker runs one operation: request on stdin, result on stdout, receipt on fd 3.
func runWorker(ctx context.Context) {
	receipt := os.NewFile(3, "operation-receipt")
	if receipt == nil {
		fmt.Fprintln(os.Stderr, "adapter receipt pipe unavailable")
		os.Exit(2)
	}
	defer receipt.Close()
	if err := (runtime.Runner{}).Run(ctx, os.Stdin, os.Stdout, receipt); err != nil {
		fmt.Fprintln(os.Stderr, "adapter operation failed")
		os.Exit(1)
	}
}
