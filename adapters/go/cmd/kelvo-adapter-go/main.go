package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/SYNEHQ/kelvo-go/adapters/go/internal/runtime"
)

func main() {
	if len(os.Args) != 2 || os.Args[1] != "worker" {
		fmt.Fprintln(os.Stderr, "usage: kelvo-adapter-go worker")
		os.Exit(2)
	}
	receipt := os.NewFile(3, "operation-receipt")
	if receipt == nil {
		fmt.Fprintln(os.Stderr, "adapter receipt pipe unavailable")
		os.Exit(2)
	}
	defer receipt.Close()
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	if err := (runtime.Runner{}).Run(ctx, os.Stdin, os.Stdout, receipt); err != nil {
		fmt.Fprintln(os.Stderr, "adapter operation failed")
		os.Exit(1)
	}
}
