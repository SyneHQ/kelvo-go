// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package main

import (
	"fmt"
	"net"
	"net/http"
	"os"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/transport/pgwire"
)

// startPostgresWireFile enables the listener for serve when a file is named.
func startPostgresWireFile(path string, handler http.Handler) (func(), error) {
	if path == "" {
		return func() {}, nil
	}
	cfg, err := pgwire.LoadFile(path)
	if err != nil {
		return nil, query.NewError("CONFIGURATION_ERROR", err.Error())
	}
	return startPostgresWire(cfg, handler)
}

// startPostgresWire serves the read-only PostgreSQL protocol in front of the
// HTTP query API handler. A nil configuration keeps it disabled.
func startPostgresWire(cfg *pgwire.Config, handler http.Handler) (func(), error) {
	if cfg == nil {
		return func() {}, nil
	}
	server, err := pgwire.New(*cfg, handler)
	if err != nil {
		return nil, query.NewError("CONFIGURATION_ERROR", err.Error())
	}
	ln, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return nil, err
	}
	fmt.Fprintln(os.Stderr, "Kelvo PostgreSQL wire listening on "+ln.Addr().String())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := server.Serve(ln); err != nil {
			fmt.Fprintln(os.Stderr, "Kelvo PostgreSQL wire stopped: "+query.PublicError(err).Message)
		}
	}()
	return func() { _ = server.Close(); <-done }, nil
}
