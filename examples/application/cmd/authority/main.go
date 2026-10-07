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
	"syscall"
	"time"

	"example.com/kelvo-application/internal/application"
)

func run() error {
	path := flag.String("config", "app.yaml", "application configuration file")
	auditPath := flag.String("audit-file", "", "optional atomic, sanitized acceptance counters")
	flag.Parse()
	c, err := application.LoadConfig(*path)
	if err != nil {
		return err
	}
	gateway, err := application.Gateway(c)
	if err != nil {
		return err
	}
	defer gateway.Close()
	audit, err := application.NewAudit(*auditPath)
	if err != nil {
		return err
	}
	handler, err := (application.Store{Config: c, Audit: audit}).Handler(gateway)
	if err != nil {
		return err
	}
	tlsConfig, err := application.ServerTLS(c)
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", c.Authority.Listen)
	if err != nil {
		return errors.New("authority listener unavailable")
	}
	defer listener.Close()
	server := &http.Server{Handler: handler, TLSConfig: tlsConfig, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 15 * time.Second, MaxHeaderBytes: 16 << 10}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		timeout, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(timeout)
	}()
	fmt.Fprintln(os.Stderr, "application authority listening with worker mTLS")
	if err := server.ServeTLS(listener, "", ""); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return errors.New("authority server failed")
	}
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
