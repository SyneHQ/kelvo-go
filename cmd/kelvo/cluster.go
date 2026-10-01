// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/cluster"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/worker"
)

func runCluster(args []string) error {
	f := flag.NewFlagSet(args[0], flag.ContinueOnError)
	file := f.String("config", "kelvo.yml", "Cluster configuration (YAML)")
	if err := f.Parse(args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if f.NArg() != 0 {
		return query.NewError("INVALID_ARGUMENT", "Unexpected positional arguments")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if args[0] == "node" {
		return runNode(ctx, *file)
	}
	cfg, err := cluster.LoadGateway(*file)
	if err != nil {
		return query.NewError("CONFIGURATION_ERROR", err.Error())
	}
	stores := map[string]cluster.Store{}
	defer func() {
		for _, s := range stores {
			_ = s.Close()
		}
	}()
	for _, tenant := range cfg.Tenants {
		s, err := cluster.OpenStore(ctx, tenant.NATS, tenant.Policy, args[0] == "cluster-init")
		if err != nil {
			return query.NewError("CONFIGURATION_ERROR", err.Error())
		}
		stores[tenant.Policy.TenantID] = s
	}
	if args[0] == "cluster-init" {
		fmt.Fprintln(os.Stderr, "Kelvo cluster tenant state initialized")
		return nil
	}
	tc, err := cluster.BuildServerTLS(cfg.TLS, "", "")
	if err != nil {
		return query.NewError("CONFIGURATION_ERROR", err.Error())
	}
	ln, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return err
	}
	defer ln.Close()
	gateway, err := cluster.NewGateway(cfg, stores)
	if err != nil {
		return query.NewError("CONFIGURATION_ERROR", err.Error())
	}
	return serveCluster(ctx, ln, tc, gateway, func() { _ = gateway.Close() })
}

func runNode(ctx context.Context, file string) error {
	cfg, err := cluster.LoadNode(file)
	if err != nil {
		return query.NewError("CONFIGURATION_ERROR", err.Error())
	}
	tc, err := cluster.BuildServerTLS(cfg.TLS, cluster.WorkerIdentity(cfg.Policy.TenantID, cfg.WorkerID), cluster.GatewayIdentity)
	if err != nil {
		return query.NewError("CONFIGURATION_ERROR", err.Error())
	}
	catalogue, err := catalog.Load(cfg.CatalogFile)
	if err != nil {
		return query.NewError("CONFIGURATION_ERROR", "Worker catalog cannot be loaded")
	}
	executor, err := worker.New(catalogue, cfg.Policy.Limits)
	if err != nil {
		return err
	}
	executor.SandboxPath = cfg.SandboxPath
	store, err := cluster.OpenStore(ctx, cfg.NATS, cfg.Policy, false)
	if err != nil {
		return query.NewError("CONFIGURATION_ERROR", err.Error())
	}
	defer store.Close()
	// Bind before reserving the durable worker identity or pulling jobs.
	ln, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return err
	}
	defer ln.Close()
	node, err := cluster.NewNode(cfg, store, executor)
	if err != nil {
		return query.NewError("CONFIGURATION_ERROR", err.Error())
	}
	return serveCluster(ctx, ln, tc, node, func() { _ = node.Close() })
}

func serveCluster(ctx context.Context, ln net.Listener, tc *tls.Config, handler http.Handler, closeHandler func()) error {
	requests, cancel := context.WithCancel(ctx)
	defer cancel()
	s := &http.Server{Handler: handler, TLSConfig: tc, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10, BaseContext: func(net.Listener) context.Context { return requests }}
	done := make(chan error, 1)
	go func() { done <- s.ServeTLS(ln, "", "") }()
	fmt.Fprintln(os.Stderr, "Kelvo cluster HTTPS listening on "+ln.Addr().String())
	var result error
	select {
	case result = <-done:
	case <-ctx.Done():
	}
	cancel()
	closed := make(chan struct{})
	go func() { closeHandler(); close(closed) }()
	shutdown, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	if err := s.Shutdown(shutdown); err != nil {
		_ = s.Close()
		if result == nil {
			result = err
		}
	}
	select {
	case <-closed:
	case <-shutdown.Done():
		if result == nil {
			result = errors.New("cluster shutdown deadline exceeded")
		}
	}
	if errors.Is(result, http.ErrServerClosed) {
		return nil
	}
	return result
}
