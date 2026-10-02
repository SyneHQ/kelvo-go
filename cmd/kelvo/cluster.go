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

	"github.com/SYNEHQ/kelvo-go/internal/acceleration"
	"github.com/SYNEHQ/kelvo-go/internal/admission"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/cluster"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/secrets"
	"github.com/SYNEHQ/kelvo-go/internal/telemetry"
	"github.com/SYNEHQ/kelvo-go/internal/tracing"
	"github.com/SYNEHQ/kelvo-go/internal/worker"
)

func runCluster(args []string) error {
	f := flag.NewFlagSet(args[0], flag.ContinueOnError)
	file := f.String("config", "kelvo.yml", "Cluster configuration (YAML)")
	drainTimeout := f.Duration("drain-timeout", 30*time.Second, "Grace period for accepted queries before cancellation")
	if err := f.Parse(args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if *drainTimeout < 0 || *drainTimeout > 24*time.Hour {
		return query.NewError("INVALID_ARGUMENT", "Drain timeout must be between zero and 24 hours")
	}
	if f.NArg() != 0 {
		return query.NewError("INVALID_ARGUMENT", "Unexpected positional arguments")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if args[0] == "node" {
		return runNode(ctx, *file, *drainTimeout)
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
		if args[0] == "cluster-init" {
			if _, err = s.OpenSourceQuotas(ctx, true); err != nil {
				return query.NewError("CONFIGURATION_ERROR", "Source quotas could not be initialized")
			}
			if _, err = s.OpenRefreshQueue(ctx, true); err != nil {
				return query.NewError("CONFIGURATION_ERROR", "Acceleration dispatch could not be initialized")
			}
		}
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
	return serveCluster(ctx, ln, tc, gateway, func() { _ = gateway.Close() }, *drainTimeout)
}

func runNode(ctx context.Context, file string, drainTimeout time.Duration) error {
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
	if catalogue.Acceleration != nil && catalogue.Acceleration.TenantID != cfg.Policy.TenantID {
		return query.NewError("CONFIGURATION_ERROR", "Acceleration tenant must match worker tenant")
	}
	executor, err := worker.New(catalogue, cfg.Policy.Limits)
	if err != nil {
		return err
	}
	executor.SandboxPath = cfg.SandboxPath
	if cfg.Secrets != nil {
		provider, err := secrets.New(*cfg.Secrets)
		if err != nil {
			return query.NewError("CONFIGURATION_ERROR", "File secret provider is unavailable")
		}
		defer provider.Close()
		executor.Secrets = provider
	}
	if cfg.Tracing != nil {
		cfg.RuntimeTracing, err = tracing.New(*cfg.Tracing)
		if err != nil {
			return err
		}
		executor.Tracing = cfg.RuntimeTracing
		defer func() {
			shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := cfg.RuntimeTracing.Shutdown(shutdown); err != nil {
				fmt.Fprintln(os.Stderr, "Tracing shutdown did not complete")
			}
		}()
	}
	if cfg.History != nil {
		cfg.RuntimeHistory, err = telemetry.NewHistory(*cfg.History)
		if err != nil {
			return err
		}
	}
	cfg.RuntimeMetrics = telemetry.New()
	executor.Metrics = cfg.RuntimeMetrics
	var pool *admission.Pool
	var overhead int64
	if cfg.Resources != nil {
		pool, err = cfg.Resources.NewPool()
		if err != nil {
			return err
		}
		cfg.RuntimeResources = pool
		overhead = cfg.Resources.OverheadMB << 20
		executor.ResourcePool, executor.ResourceOverheadBytes = pool, overhead
		if catalogue.Acceleration != nil {
			for _, d := range catalogue.Acceleration.Datasets {
				if !cfg.Resources.Fits(d.Limits, true) {
					return errors.New("refresh reservation exceeds node resources")
				}
			}
		}
	}
	store, err := cluster.OpenStore(ctx, cfg.NATS, cfg.Policy, false)
	if err != nil {
		return query.NewError("CONFIGURATION_ERROR", err.Error())
	}
	defer store.Close()
	for id := range cfg.Policy.SourceQuotas {
		selected, err := catalogue.Select([]string{id})
		if err != nil || len(selected) != 1 || selected[0].Type == "accelerated" {
			return query.NewError("CONFIGURATION_ERROR", "Source quota names an unavailable live source")
		}
	}
	sourceQuotas, err := store.OpenSourceQuotas(ctx, false)
	if err != nil {
		return query.NewError("CONFIGURATION_ERROR", "Source quotas are unavailable; initialize tenant resources first")
	}
	executor.SourceAdmission = sourceQuotas
	var refreshQueue *cluster.RefreshQueue
	if catalogue.Acceleration != nil {
		refreshQueue, err = store.OpenRefreshQueue(ctx, false)
		if err != nil {
			return query.NewError("CONFIGURATION_ERROR", "Acceleration dispatch is unavailable; run cluster-init")
		}
	}
	closeDatasets := func() {}
	datasetsTransferred := false
	if catalogue.Acceleration != nil {
		backend, err := acceleration.OpenBackend(*catalogue.Acceleration)
		if err != nil {
			return query.NewError("CONFIGURATION_ERROR", "Dataset diagnostics backend is unavailable")
		}
		reporter, err := cluster.NewDatasetReporter(catalogue, backend, cfg.RequiredDatasets)
		if err != nil {
			_ = backend.Close()
			return query.NewError("CONFIGURATION_ERROR", "Invalid required dataset configuration")
		}
		cfg.RuntimeDatasets = reporter
		closeDatasets = func() { reporter.Close(); _ = backend.Close() }
	} else if len(cfg.RequiredDatasets) != 0 {
		return query.NewError("CONFIGURATION_ERROR", "Required datasets need acceleration configuration")
	}
	defer func() {
		if !datasetsTransferred {
			closeDatasets()
		}
	}()
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
	runCtx, stop := context.WithCancel(ctx)
	refreshCtx, stopRefresh := context.WithCancel(context.Background())
	defer stopRefresh()
	defer stop()
	refreshGate := newRefreshGate()
	var refreshDone chan error
	if refreshQueue != nil {
		refreshDone = make(chan error, 1)
		go func() {
			refreshDone <- runClusterRefresh(refreshCtx, catalogue, cfg.SandboxPath, refreshQueue, pool, overhead, cfg.RuntimeMetrics, refreshGate, sourceQuotas, executor.Secrets, cfg.RuntimeTracing)
			stop()
		}()
	}
	lifecycle := &nodeLifecycle{Node: node, pool: pool, refresh: refreshGate, stopRefresh: stopRefresh}
	cleanupDone := make(chan struct{})
	var refreshErr error // read only after cleanupDone closes
	datasetsTransferred = true
	result := serveCluster(runCtx, ln, tc, lifecycle, func() {
		defer close(cleanupDone)
		stopRefresh()
		nodeDone := make(chan struct{})
		datasetsDone := make(chan struct{})
		go func() { closeDatasets(); close(datasetsDone) }()
		go func() { _ = node.Close(); close(nodeDone) }()
		if refreshDone != nil {
			refreshErr = <-refreshDone
		}
		<-nodeDone
		<-datasetsDone
	}, drainTimeout)
	stop()
	// serveCluster bounds both joins; never wait again after its deadline.
	select {
	case <-cleanupDone:
		if refreshErr != nil && !errors.Is(refreshErr, context.Canceled) && result == nil {
			result = refreshErr
		}
	default:
		if result == nil {
			result = errors.New("cluster shutdown deadline exceeded")
		}
	}
	return result
}

func serveCluster(ctx context.Context, ln net.Listener, tc *tls.Config, handler http.Handler, closeHandler func(), drainTimeout time.Duration) error {
	// Signal cancellation starts drain; it must not cancel active HTTP requests.
	requests, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &http.Server{Handler: handler, TLSConfig: tc, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10, BaseContext: func(net.Listener) context.Context { return requests }}
	done := make(chan error, 1)
	go func() { done <- s.ServeTLS(ln, "", "") }()
	fmt.Fprintln(os.Stderr, "Kelvo cluster HTTPS listening on "+ln.Addr().String())
	var result error
	select {
	case result = <-done:
	case <-ctx.Done():
		if drainer, ok := handler.(interface{ Drain(context.Context) error }); ok {
			grace, stopGrace := context.WithTimeout(context.Background(), drainTimeout)
			_ = drainer.Drain(grace)
			stopGrace()
		}
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

// Accepted node reservations may not have entered Execute yet. Keep the shared
// pool open until Node.Drain finishes, while a separate gate stops new refreshes.
type drainingNode interface {
	http.Handler
	BeginDrain()
	Drain(context.Context) error
}
type nodeLifecycle struct {
	Node        drainingNode
	pool        *admission.Pool
	refresh     *refreshGate
	stopRefresh context.CancelFunc
}

func (n *nodeLifecycle) ServeHTTP(w http.ResponseWriter, r *http.Request) { n.Node.ServeHTTP(w, r) }
func (n *nodeLifecycle) Drain(ctx context.Context) error {
	n.Node.BeginDrain()
	n.refresh.Drain()
	err := n.Node.Drain(ctx)
	if e := n.refresh.Wait(ctx); err == nil {
		err = e
	}
	if n.pool != nil {
		n.pool.Drain()
		if e := n.pool.Wait(ctx); err == nil {
			err = e
		}
	}
	n.stopRefresh()
	return err
}
