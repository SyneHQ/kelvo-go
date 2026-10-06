// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package main

import (
	"crypto/tls"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/cluster"
)

func serveOperationInputs(cfg cluster.GatewayConfig, gateway *cluster.Gateway) (func(), error) {
	if cfg.Operations == nil {
		return func() {}, nil
	}
	config, err := cluster.OperationInputTLS(cfg.Operations.TLS)
	if err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp", cfg.Operations.Listen)
	if err != nil {
		return nil, errors.New("private operation input listener unavailable")
	}
	server := &http.Server{Handler: gateway.OperationInputs(), TLSConfig: config,
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second,
		IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10, ErrorLog: log.New(io.Discard, "", 0)}
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := server.Serve(tls.NewListener(listener, config)); err != nil && !errors.Is(err, http.ErrServerClosed) {
			// Do not accept operations whose sealed inputs cannot be served.
			gateway.BeginDrain()
		}
	}()
	return func() { _ = server.Close(); <-done }, nil
}
