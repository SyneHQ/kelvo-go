// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0

// kelvo-policy-migrate replaces the cluster policy stored in NATS metadata.
//
// Every process compares that stored policy with its own config byte for byte,
// so changing a policy (for example query limits) is a migration, not a config
// edit. Give it the gateway config currently deployed and the one about to be
// deployed; it pairs tenants by id and, per tenant whose policy changed, moves
// the stored value from exactly the old policy to the new one with a
// compare-and-swap. Without -apply it only reports. Run it with the
// provisioner credentials immediately before deploying the new configs.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/SYNEHQ/kelvo-go/internal/cluster"
)

func main() {
	from := flag.String("from", "", "gateway config currently deployed (old policy)")
	to := flag.String("to", "", "gateway config about to be deployed (new policy)")
	apply := flag.Bool("apply", false, "write the new policy (default: report only)")
	flag.Parse()
	if *from == "" || *to == "" || flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "usage: kelvo-policy-migrate -from current-gateway.yml -to new-gateway.yml [-apply]")
		os.Exit(2)
	}
	if err := run(*from, *to, *apply); err != nil {
		fmt.Fprintln(os.Stderr, "kelvo-policy-migrate:", err)
		os.Exit(1)
	}
}

func run(fromPath, toPath string, apply bool) error {
	oldCfg, err := cluster.LoadGateway(fromPath)
	if err != nil {
		return fmt.Errorf("old config: %w", err)
	}
	newCfg, err := cluster.LoadGateway(toPath)
	if err != nil {
		return fmt.Errorf("new config: %w", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	plans := map[string]cluster.Policy{}
	for _, tenant := range oldCfg.Tenants {
		plans[tenant.Policy.TenantID] = tenant.Policy
	}
	changed := 0
	for _, tenant := range newCfg.Tenants {
		old, ok := plans[tenant.Policy.TenantID]
		if !ok {
			return fmt.Errorf("tenant %q is new; cluster-init creates new tenants, nothing to migrate", tenant.Policy.TenantID)
		}
		state, err := cluster.MigratePolicy(ctx, tenant.NATS, old, tenant.Policy, apply)
		if err != nil && err.Error() == "old and new policies are identical" {
			fmt.Printf("tenant %s: unchanged\n", tenant.Policy.TenantID)
			continue
		}
		if err != nil {
			return fmt.Errorf("tenant %s: %w", tenant.Policy.TenantID, err)
		}
		changed++
		fmt.Printf("tenant %s: %s\n", tenant.Policy.TenantID, state)
	}
	if changed == 0 {
		fmt.Println("no tenant policy changed")
	}
	return nil
}
