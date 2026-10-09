// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/cluster"
	"github.com/SYNEHQ/kelvo-go/internal/containment"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/worker"
)

func runApplication(args []string) (resultErr error) {
	flags := flag.NewFlagSet("application", flag.ContinueOnError)
	path := flags.String("config", "application.yml", "Standalone application configuration")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if flags.NArg() != 0 {
		return query.NewError("INVALID_ARGUMENT", "Unexpected positional arguments")
	}
	cfg, err := cluster.LoadApplication(*path)
	if err != nil {
		return query.NewError("CONFIGURATION_ERROR", "Application configuration is invalid")
	}
	token := os.Getenv(cfg.TokenEnv)
	if len(token) < 32 || len(token) > 4096 {
		return query.NewError("CONFIGURATION_ERROR", "Application token must contain 32 to 4096 characters")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	tlsRuntime, err := cluster.OpenServerTLS(cfg.TLS, cluster.GatewayIdentity, "")
	if err != nil {
		return query.NewError("CONFIGURATION_ERROR", "Application TLS identity is unavailable")
	}
	defer tlsRuntime.Close()
	connectionResolver, err := worker.NewConnectionResolver(cfg.Resolver, cluster.WorkerIdentity(cfg.InstanceID, "application"))
	if err != nil {
		return query.NewError("CONFIGURATION_ERROR", "Application resolver is unavailable")
	}
	defer connectionResolver.Close()
	executor, err := worker.New(catalog.Config{}, cfg.Limits)
	if err != nil {
		return err
	}
	executor, err = executor.WithConnectionResolvers(map[string]*worker.ConnectionResolver{cfg.Issuer: connectionResolver})
	if err != nil {
		return err
	}
	executor.SandboxPath = cfg.SandboxPath
	executor.ScratchRoot, err = worker.OpenScratchRoot(cfg.ScratchDirectory)
	if err != nil {
		return query.NewError("CONFIGURATION_ERROR", "Application scratch storage is unavailable")
	}
	defer func() { resultErr = errors.Join(resultErr, executor.ScratchRoot.Close()) }()
	executor.ResourcePool, err = cfg.Resources.NewPool()
	if err != nil {
		return err
	}
	executor.ResourceOverheadBytes = cfg.Resources.OverheadMB << 20
	executor.Containment, err = containment.Open(cfg.Containment.Config)
	if err != nil {
		return query.NewError("CONFIGURATION_ERROR", "Application containment is unavailable")
	}
	executor.ContainmentBudget = cfg.Containment.Budget
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		resultErr = errors.Join(resultErr, executor.Containment.Close(cleanup))
	}()
	if err = verifyContainmentStartup(ctx, executor); err != nil {
		return err
	}
	listener, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return query.NewError("CONFIGURATION_ERROR", "Application listener is unavailable")
	}
	defer listener.Close()
	app, err := cluster.NewApplication(ctx, cfg, token, executor)
	if err != nil {
		return query.NewError("CONFIGURATION_ERROR", "Application state is unavailable or its identity changed")
	}
	executor.Containment.SetOnQuarantine(func(error) { app.BeginDrain(); executor.ResourcePool.Drain() })
	server := &http.Server{Handler: tlsRuntime.Handler(app), TLSConfig: tlsRuntime.Config, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 64 << 10}
	done := make(chan error, 1)
	accepting := &applicationListener{Listener: listener, ready: make(chan struct{})}
	go func() { done <- server.ServeTLS(accepting, "", "") }()
	started := false
	select {
	case <-accepting.ready:
		err = app.Start()
		started = err == nil
	case err = <-done:
	case <-ctx.Done():
	}
	if started {
		fmt.Fprintln(os.Stderr, "Kelvo application HTTPS listening on "+listener.Addr().String())
		select {
		case err = <-done:
		case <-ctx.Done():
		}
	}
	app.BeginDrain()
	drain, cancelDrain := context.WithTimeout(context.Background(), cfg.Limits.Timeout+5*time.Second)
	drainErr := app.Drain(drain)
	cancelDrain()
	shutdown, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelShutdown()
	shutdownErr := server.Shutdown(shutdown)
	if shutdownErr != nil {
		_ = server.Close()
	}
	closeErr := app.Close(shutdown)
	if errors.Is(err, http.ErrServerClosed) {
		err = nil
	}
	if err != nil || drainErr != nil || shutdownErr != nil || closeErr != nil {
		return query.NewError("UNAVAILABLE", "Application shutdown did not complete cleanly")
	}
	return nil
}

type applicationListener struct {
	net.Listener
	ready chan struct{}
	once  sync.Once
}

func (l *applicationListener) Accept() (net.Conn, error) {
	l.once.Do(func() { close(l.ready) })
	return l.Listener.Accept()
}
