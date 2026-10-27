// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/cluster"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/transport/flightsql"
)

// flightSQLConfig is the opt-in `serve` Flight SQL listener. Empty Listen disables it.
type flightSQLConfig struct {
	Listen      string
	TLSCert     string
	TLSKey      string
	MaxSessions int
	TicketTTL   time.Duration
}

func flightSQLFlags(f *flag.FlagSet) *flightSQLConfig {
	c := &flightSQLConfig{}
	f.StringVar(&c.Listen, "flight-sql-listen", "", "Read-only Flight SQL listen address (disabled when empty)")
	f.StringVar(&c.TLSCert, "flight-sql-tls-cert", "", "Flight SQL TLS certificate (PEM); required off loopback")
	f.StringVar(&c.TLSKey, "flight-sql-tls-key", "", "Flight SQL TLS private key (PEM)")
	f.IntVar(&c.MaxSessions, "flight-sql-max-sessions", 64, "Maximum Flight SQL credentials holding tickets or prepared statements")
	f.DurationVar(&c.TicketTTL, "flight-sql-ticket-ttl", time.Minute, "Flight SQL ticket and prepared statement lifetime")
	return c
}

// startFlightSQL serves Flight SQL over the same HTTP query handler. The
// returned stop function is safe to call once.
func startFlightSQL(c flightSQLConfig, handler http.Handler, cat catalog.Config) (func(), error) {
	if c.Listen == "" {
		return func() {}, nil
	}
	var tc *tls.Config
	if c.TLSCert != "" || c.TLSKey != "" {
		cert, err := tls.LoadX509KeyPair(c.TLSCert, c.TLSKey)
		if err != nil {
			return nil, query.NewError("CONFIGURATION_ERROR", "Flight SQL TLS certificate is unavailable")
		}
		tc = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	} else if host, _, err := net.SplitHostPort(c.Listen); err != nil || !net.ParseIP(host).IsLoopback() {
		return nil, query.NewError("CONFIGURATION_ERROR", "Flight SQL requires TLS unless it listens on a loopback IP")
	}
	server, err := flightsql.New(flightsql.Options{Handler: handler, Catalog: cat, MaxSessions: c.MaxSessions, TicketTTL: c.TicketTTL})
	if err != nil {
		return nil, query.NewError("CONFIGURATION_ERROR", err.Error())
	}
	srv := server.NewGRPC(tc)
	if err := srv.Init(c.Listen); err != nil {
		return nil, query.NewError("CONFIGURATION_ERROR", "Flight SQL listen address is unavailable")
	}
	go func() {
		fmt.Fprintln(os.Stderr, "Kelvo Flight SQL listening on "+srv.Addr().String())
		if err := srv.Serve(); err != nil {
			fmt.Fprintln(os.Stderr, "Flight SQL server stopped: "+err.Error())
		}
	}()
	return srv.Shutdown, nil
}

// startGatewayFlightSQL serves Flight SQL over the cluster gateway handler.
// The gateway listener always requires TLS.
func startGatewayFlightSQL(c *cluster.GatewayFlightSQLConfig, gateway http.Handler) (func(), error) {
	if c == nil {
		return func() {}, nil
	}
	if c.Listen == "" || c.TLSCertFile == "" || c.TLSKeyFile == "" {
		return nil, query.NewError("CONFIGURATION_ERROR", "Gateway Flight SQL requires listen, tls_cert_file and tls_key_file")
	}
	var cat catalog.Config
	if c.CatalogFile != "" {
		var err error
		if cat, err = catalog.Load(c.CatalogFile); err != nil {
			return nil, query.NewError("CONFIGURATION_ERROR", "Gateway Flight SQL catalog is invalid")
		}
	}
	sessions, ttl := c.MaxSessions, c.TicketTTL
	if sessions == 0 {
		sessions = 64
	}
	if ttl == 0 {
		ttl = time.Minute
	}
	return startFlightSQL(flightSQLConfig{Listen: c.Listen, TLSCert: c.TLSCertFile, TLSKey: c.TLSKeyFile, MaxSessions: sessions, TicketTTL: ttl}, gateway, cat)
}
