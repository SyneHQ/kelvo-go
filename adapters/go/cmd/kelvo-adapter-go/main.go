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
	if len(os.Args) != 2 || (os.Args[1] != "worker" && os.Args[1] != "serve") {
		fmt.Fprintln(os.Stderr, "usage: kelvo-adapter-go [worker|serve]")
		os.Exit(2)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	if os.Args[1] == "worker" {
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
		return
	}

	// os.Args[1] == "serve"
	if err := (runtime.Runner{}).Serve(ctx, os.Stdin, os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "adapter serve failed: %v\n", err)
		os.Exit(1)
	}
}
