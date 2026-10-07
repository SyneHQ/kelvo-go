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
	"github.com/SYNEHQ/kelvo-go/internal/containment"
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
			reportStoreMetadataDiagnostic(err)
			reportStoreCoordinationDiagnostic(err)
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
			if tenant.Policy.Exports != nil {
				if _, err = cluster.OpenExportStore(ctx, s, true); err != nil {
					return query.NewError("CONFIGURATION_ERROR", "Export dispatch could not be initialized")
				}
			}
			if tenant.Policy.Operations != nil {
				if _, err = s.OpenOperations(ctx, true); err != nil {
					return query.NewError("CONFIGURATION_ERROR", "Operation receipts could not be initialized")
				}
			}
		}
	}
	if args[0] == "cluster-init" {
		fmt.Fprintln(os.Stderr, "Kelvo cluster tenant state initialized")
		return nil
	}
	serverTLS, err := cluster.OpenServerTLS(cfg.TLS, "", "")
	if err != nil {
		return query.NewError("CONFIGURATION_ERROR", err.Error())
	}
	defer serverTLS.Close()
	ln, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return err
	}
	defer ln.Close()
	gateway, err := cluster.NewGateway(cfg, stores)
	if err != nil {
		return query.NewError("CONFIGURATION_ERROR", err.Error())
	}
	stopOperationInputs, err := serveOperationInputs(cfg, gateway)
	if err != nil {
		_ = gateway.Close()
		return err
	}
	defer stopOperationInputs()

	var closeErr error
	closed := make(chan struct{})
	result := serveCluster(ctx, ln, serverTLS.Config, serverTLS.Handler(gateway), func() { closeErr = gateway.Close(); close(closed) }, *drainTimeout)
	select {
	case <-closed:
		if result == nil && closeErr != nil {
			return query.NewError("UNAVAILABLE", "Gateway shutdown remains uncertain")
		}
	default:
	}
	return result
}

func runNode(ctx context.Context, file string, drainTimeout time.Duration) (resultErr error) {
	cfg, err := cluster.LoadNode(file)
	if err != nil {
		return query.NewError("CONFIGURATION_ERROR", err.Error())
	}

	cfg.RuntimeAudit, err = cluster.OpenServiceAudit(cfg.Audit, "worker", []string{cfg.Policy.TenantID})
	if err != nil {
		return query.NewError("CONFIGURATION_ERROR", "Worker audit storage is unavailable")
	}
	auditTransferred := false
	defer func() {
		if !auditTransferred {
			if err := cfg.RuntimeAudit.CloseBounded(); err != nil && resultErr == nil {
				resultErr = query.NewError("UNAVAILABLE", "Worker audit shutdown remains uncertain")
			}
		}
	}()
	serverTLS, err := cluster.OpenServerTLS(cfg.TLS, cluster.WorkerIdentity(cfg.Policy.TenantID, cfg.WorkerID), cluster.GatewayIdentity)
	if err != nil {
		return query.NewError("CONFIGURATION_ERROR", err.Error())
	}
	defer serverTLS.Close()
	catalogue, err := catalog.Load(cfg.CatalogFile)
	if err != nil {
		return query.NewError("CONFIGURATION_ERROR", "Worker catalog cannot be loaded")
	}
	if err := cluster.ValidateCatalogAuthority(cfg.Policy, catalogue); err != nil {
		return query.NewError("CONFIGURATION_ERROR", err.Error())
	}
	if catalogue.Acceleration != nil && catalogue.Acceleration.TenantID != cfg.Policy.TenantID {
		return query.NewError("CONFIGURATION_ERROR", "Acceleration tenant must match worker tenant")
	}
	if err := cluster.ValidateExportCatalog(cfg, catalogue); err != nil {
		return query.NewError("CONFIGURATION_ERROR", err.Error())
	}
	executor, err := worker.New(catalogue, cfg.Policy.Limits)
	if err != nil {
		return err
	}
	executor, closeResolvers, err := configureConnectionResolvers(cfg, executor)
	if err != nil {
		return query.NewError("CONFIGURATION_ERROR", err.Error())
	}
	defer closeResolvers()
	executor.SandboxPath = cfg.SandboxPath
	if cfg.ScratchDirectory != "" {
		executor.ScratchRoot, err = worker.OpenScratchRoot(cfg.ScratchDirectory)
		if err != nil {
			return query.NewError("CONFIGURATION_ERROR", "Managed worker scratch is unavailable")
		}
		defer func() {
			if err := executor.ScratchRoot.Close(); err != nil {
				fmt.Fprintln(os.Stderr, "Managed worker scratch cleanup did not complete")
			}
		}()
	}
	if cfg.Secrets != nil {
		provider, err := secrets.New(*cfg.Secrets)
		if err != nil {
			return query.NewError("CONFIGURATION_ERROR", "Source secret provider is unavailable")
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
	if cfg.SourceHealth != nil {
		cfg.RuntimeSourceHealth, err = cluster.NewSourceHealth(catalogue, *cfg.SourceHealth)
		if err != nil {
			return query.NewError("CONFIGURATION_ERROR", "Invalid source health configuration")
		}
		executor.SourceHealth = cfg.RuntimeSourceHealth
	}
	cfg.RuntimeMetrics = cfg.Metrics.NewRegistry()
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
				if cfg.Containment != nil {
					if err := cfg.Containment.Validate(cfg.Resources, d.Limits); err != nil {
						return err
					}
				}
				if !cfg.Resources.Fits(d.Limits, true) {
					return errors.New("refresh reservation exceeds node resources")
				}
			}
		}
	}
	if cfg.Containment != nil {
		executor.Containment, err = containment.Open(cfg.Containment.Config)
		if err != nil {
			return query.NewError("CONFIGURATION_ERROR", "Worker process containment is unavailable")
		}
		executor.ContainmentBudget = cfg.Containment.Budget
		defer func() {
			if err := executor.Containment.Close(context.Background()); err != nil {
				fmt.Fprintln(os.Stderr, "Worker process containment cleanup remains uncertain")
				if resultErr == nil {
					resultErr = query.NewError("RESOURCE_EXHAUSTED", "Worker process containment cleanup remains uncertain")
				}
			}
		}()
		if err := verifyContainmentStartup(ctx, executor); err != nil {
			return err
		}
	}
	if acceleration.ProtectedObjects(catalogue) && (executor.Containment == nil || executor.ScratchRoot == nil) {
		return query.NewError("CONFIGURATION_ERROR", "Protected object snapshots require node containment and managed scratch")
	}
	objectRuntime, runtimeErr := acceleration.OpenObjectRuntime(catalogue)
	runtimeTransferred := false
	closeObjectRuntime := func() error {
		if objectRuntime == nil {
			return nil
		}
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return objectRuntime.Close(cleanup)
	}
	defer func() {
		if !runtimeTransferred {
			if err := closeObjectRuntime(); err != nil {
				resultErr = errors.Join(resultErr, query.NewError("UNAVAILABLE", "Protected object runtime shutdown remains uncertain"))
			}
		}
	}()
	if runtimeErr != nil {
		return query.NewError("CONFIGURATION_ERROR", "Protected object runtime is unavailable")
	}
	executor.ObjectRuntime = objectRuntime
	store, err := cluster.OpenStore(ctx, cfg.NATS, cfg.Policy, false)
	if err != nil {
		reportStoreMetadataDiagnostic(err)
		reportStoreCoordinationDiagnostic(err)
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
		var reader cluster.DatasetStatusReader
		closeBackend := func() {}
		if objectRuntime != nil {
			reader = objectRuntime
		} else {
			backend, err := acceleration.OpenBackend(*catalogue.Acceleration)
			if err != nil {
				return query.NewError("CONFIGURATION_ERROR", "Dataset diagnostics backend is unavailable")
			}
			reader = backend
			closeBackend = func() { _ = backend.Close() }
		}
		reporter, err := cluster.NewDatasetReporter(catalogue, reader, cfg.RequiredDatasets)
		if err != nil {
			closeBackend()
			return query.NewError("CONFIGURATION_ERROR", "Invalid required dataset configuration")
		}
		cfg.RuntimeDatasets = reporter
		closeDatasets = func() { reporter.Close(); closeBackend() }
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
		reportStoreCoordinationDiagnostic(err)
		return query.NewError("CONFIGURATION_ERROR", err.Error())
	}
	runCtx, stop := context.WithCancel(ctx)
	refreshCtx, stopRefresh := context.WithCancel(context.Background())
	defer stopRefresh()
	defer stop()
	refreshGate := newRefreshGate()
	if executor.Containment != nil {
		executor.Containment.SetOnQuarantine(func(error) {
			node.BeginDrain()
			refreshGate.Drain()
			pool.Drain()
			stopRefresh()
		})
	}
	var refreshDone chan error
	if refreshQueue != nil {
		refreshDone = make(chan error, 1)
		go func() {
			refreshDone <- runClusterRefresh(refreshCtx, catalogue, cfg.SandboxPath, refreshQueue, pool, overhead, cfg.RuntimeMetrics, refreshGate, sourceQuotas, executor.Secrets, cfg.RuntimeSourceHealth, executor.ScratchRoot, executor.Containment, executor.ContainmentBudget, objectRuntime, cfg.RuntimeAudit, cfg.Policy, cfg.RuntimeTracing)
			stop()
		}()
	}
	lifecycle := &nodeLifecycle{Node: node, pool: pool, refresh: refreshGate, stopRefresh: stopRefresh, leaseFailure: node.LeaseFailure()}
	stopLeaseWatch := watchLeaseFailure(runCtx, node.LeaseFailure(), func() {
		lifecycle.fence()
		stop()
	})
	defer stopLeaseWatch()
	cleanupDone := make(chan struct{})
	var refreshErr, auditCloseErr, objectCloseErr error // read only after cleanupDone closes
	datasetsTransferred = true
	auditTransferred = true
	runtimeTransferred = true
	result := serveCluster(runCtx, ln, serverTLS.Config, serverTLS.Handler(lifecycle), func() {
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
		objectCloseErr = closeObjectRuntime()
		auditCloseErr = cfg.RuntimeAudit.CloseBounded()
	}, drainTimeout)
	stop()
	// serveCluster bounds both joins; never wait again after its deadline.
	select {
	case <-cleanupDone:
		if objectCloseErr != nil && result == nil {
			result = query.NewError("UNAVAILABLE", "Protected object runtime shutdown remains uncertain")
		}
		if auditCloseErr != nil && result == nil {
			result = query.NewError("UNAVAILABLE", "Worker audit shutdown remains uncertain")
		}
		if refreshErr != nil && !errors.Is(refreshErr, context.Canceled) && result == nil {
			result = refreshErr
		}
	default:
		if result == nil {
			result = errors.New("cluster shutdown deadline exceeded")
		}
	}
	select {
	case <-node.LeaseFailure():
		// Do not expose the broker error, identity or lease payload. A nonzero
		// exit lets restart-on-failure supervision create a fresh fenced owner.
		reportCoordinationFailure(node.LeaseFailureCause())
		return workerLeaseFailure()
	default:
	}
	return result
}

func workerLeaseFailure() error {
	return query.NewError("UNAVAILABLE", "Worker coordination lease lost")
}

// The returned stop joins the watcher. Cancellation alone never calls failed;
// callers independently inspect the durable failure channel for final status.
func watchLeaseFailure(parent context.Context, failure <-chan struct{}, failed func()) func() {
	ctx, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	go func() {
		defer close(done)
		select {
		case <-failure:
			if ctx.Err() == nil {
				failed()
			}
		case <-ctx.Done():
		}
	}()
	return func() { cancel(); <-done }
}

func serveCluster(ctx context.Context, ln net.Listener, tc *tls.Config, handler http.Handler, closeHandler func(), drainTimeout time.Duration) error {
	// Signal cancellation starts drain; it must not cancel active HTTP requests.
	requests, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &http.Server{Handler: handler, TLSConfig: tc, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 64 << 10, BaseContext: func(net.Listener) context.Context { return requests }}
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
	Node         drainingNode
	pool         *admission.Pool
	refresh      *refreshGate
	stopRefresh  context.CancelFunc
	leaseFailure <-chan struct{}
}

func (n *nodeLifecycle) ServeHTTP(w http.ResponseWriter, r *http.Request) { n.Node.ServeHTTP(w, r) }
func (n *nodeLifecycle) Drain(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stopWatch := watchLeaseFailure(ctx, n.leaseFailure, func() {
		n.fence()
		cancel() // Permanent fencing skips grace; bounded close still joins work.
	})
	defer stopWatch()
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

func (n *nodeLifecycle) fence() {
	n.Node.BeginDrain()
	n.refresh.Drain()
	if n.pool != nil {
		n.pool.Drain()
	}
	n.stopRefresh()
}

// The usual terminal error remains the final line for callers which match it.
func reportStoreMetadataDiagnostic(err error) {
	if diagnostic, ok := cluster.StoreMetadataDiagnostic(err); ok {
		fmt.Fprintln(os.Stderr, "KELVO_CLUSTER_METADATA "+diagnostic)
	}
}

func reportStoreCoordinationDiagnostic(err error) {
	if diagnostic, ok := cluster.CoordinationDiagnostic(err); ok {
		reportCoordinationFailure(diagnostic)
	}
}

func reportCoordinationFailure(failure cluster.CoordinationFailure) {
	fmt.Fprintln(os.Stderr, "KELVO_COORDINATION "+failure.Diagnostic())
}
