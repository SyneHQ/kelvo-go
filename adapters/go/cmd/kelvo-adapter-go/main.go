package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/SYNEHQ/kelvo-go/adapters/go/connectors/relational"
	"github.com/SYNEHQ/kelvo-go/adapters/go/internal/runtime"
)

const usage = "usage: kelvo-adapter-go worker | serve [--pool-size N]"

// parseArgs accepts "worker" and "serve [--pool-size N]". Anything else is a
// usage error, so an adapter never starts with a setting it did not understand
// (and an older adapter refuses the flag the same way).
func parseArgs(args []string) (mode string, poolSize int, err error) {
	if len(args) == 0 || (args[0] != "worker" && args[0] != "serve") {
		return "", 0, errors.New(usage)
	}
	mode, poolSize = args[0], relational.DefaultPoolSize
	rest := args[1:]
	if mode == "serve" && len(rest) == 2 && rest[0] == "--pool-size" {
		poolSize, err = strconv.Atoi(rest[1])
		if err != nil || poolSize < 1 || poolSize > relational.MaxPoolSize {
			return "", 0, fmt.Errorf("--pool-size must be between 1 and %d", relational.MaxPoolSize)
		}
		rest = nil
	}
	if len(rest) != 0 {
		return "", 0, errors.New(usage)
	}
	return mode, poolSize, nil
}

func main() {
	mode, poolSize, err := parseArgs(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	if mode == "worker" {
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

	// mode == "serve"
	if err := relational.SetPoolSize(poolSize); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if err := (runtime.Runner{}).Serve(ctx, os.Stdin, os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "adapter serve failed: %v\n", err)
		os.Exit(1)
	}
}
