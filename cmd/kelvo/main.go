// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	duckengine "github.com/SYNEHQ/kelvo-go/internal/engine/duckdb"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/sources/native"
	"github.com/SYNEHQ/kelvo-go/internal/transport/httpapi"
	"github.com/SYNEHQ/kelvo-go/internal/worker"
)

var version = "dev"

func main() {
	if e := run(os.Args[1:]); e != nil {
		fmt.Fprintln(os.Stderr, query.PublicError(e).Message)
		os.Exit(1)
	}
}
func run(args []string) error {
	if len(args) == 0 {
		usage()
		return nil
	}
	if args[0] == "worker" {
		return runWorker()
	}
	if args[0] == "accelerate" {
		return runAcceleration(args[1:])
	}
	if args[0] == "cluster-init" || args[0] == "gateway" || args[0] == "node" {
		return runCluster(args)
	}
	if args[0] == "version" {
		fmt.Println("Kelvo Go " + version)
		return nil
	}
	if args[0] == "help" || args[0] == "--help" {
		usage()
		return nil
	}
	if args[0] != "serve" && args[0] != "query" {
		return query.NewError("INVALID_ARGUMENT", "Expected serve, query, version, or help")
	}
	f := flag.NewFlagSet(args[0], flag.ContinueOnError)
	config := f.String("config", "kelvo.yml", "Registered source configuration (YAML)")
	sandbox := f.String("sandbox", "", "Optional Linux sandbox launcher for query workers")
	listen := f.String("listen", "127.0.0.1:8080", "HTTP listen address")
	sql := f.String("sql", "", "SQL query")
	sourceIDs := f.String("sources", "", "Comma-separated source IDs for federation")
	mode := f.String("mode", "federated", "federated or native")
	conn := f.String("connection", "", "Native source ID")
	out := f.String("out", "", "Arrow IPC output file (required for query)")
	params := f.String("parameters", "[]", "Typed parameter array as JSON")
	mongoCollection := f.String("mongo-collection", "", "MongoDB collection for a native aggregation")
	mongoPipeline := f.String("mongo-pipeline", "", "MongoDB aggregation pipeline as a JSON array (defaults to [])")
	tokenEnv := f.String("token-env", "KELVO_TOKEN", "Environment variable holding the server bearer token")
	maxConcurrent := f.Int("concurrency", 2, "Maximum concurrent workers")
	maxQueries := f.Int("max-queries", 128, "Maximum retained query handles")
	ttl := f.Duration("result-ttl", 5*time.Minute, "Query handle lifetime")
	limits := query.DefaultLimits()
	f.Int64Var(&limits.MaxRows, "max-rows", limits.MaxRows, "Maximum returned rows")
	f.Int64Var(&limits.MaxBytes, "max-bytes", limits.MaxBytes, "Maximum result bytes")
	f.DurationVar(&limits.Timeout, "timeout", limits.Timeout, "Per-query deadline")
	f.IntVar(&limits.MemoryMB, "memory-mb", limits.MemoryMB, "Engine/client memory budget (source-specific; not process RSS)")
	f.IntVar(&limits.Threads, "threads", limits.Threads, "Threads per DuckDB or ClickHouse query")
	f.IntVar(&limits.MaxTempMB, "temp-mb", limits.MaxTempMB, "DuckDB temporary disk budget")
	if e := f.Parse(args[1:]); e != nil {
		if errors.Is(e, flag.ErrHelp) {
			return nil
		}
		return e
	}
	if f.NArg() != 0 {
		return query.NewError("INVALID_ARGUMENT", "Unexpected positional arguments")
	}
	if e := limits.Validate(); e != nil {
		return e
	}
	c, e := catalog.Load(*config)
	if e != nil {
		return query.NewError("CONFIGURATION_ERROR", e.Error())
	}
	exec, e := worker.New(c, limits)
	if e != nil {
		return e
	}
	exec.SandboxPath, e = resolveCLISandbox(*sandbox)
	if e != nil {
		return e
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if args[0] == "query" {
		if *out == "" {
			return query.NewError("INVALID_ARGUMENT", "query requires --out")
		}
		r, e := makeRequest(*sql, *mode, *conn, *sourceIDs, *params, *mongoCollection, *mongoPipeline)
		if e != nil {
			return e
		}
		file, tmp, e := worker.ResolveOutput(*out)
		if e != nil {
			return e
		}
		defer os.Remove(tmp)
		sink := worker.NewIPCSink(file, limits)
		stats, e := exec.Execute(ctx, r, sink)
		if e == nil {
			e = sink.Finish()
		}
		stats.WireBytes = sink.EncodedBytes()
		if e == nil {
			e = file.Sync()
		}
		closeErr := file.Close()
		if e == nil {
			e = closeErr
		}
		if e != nil {
			return e
		}
		if e = os.Rename(tmp, *out); e != nil {
			return e
		}
		return json.NewEncoder(os.Stderr).Encode(stats)
	}
	token := os.Getenv(*tokenEnv)
	if len(token) < 32 {
		return query.NewError("CONFIGURATION_ERROR", "Server token must contain at least 32 characters")
	}
	handler, e := httpapi.New(exec, httpapi.Options{Token: token, Limits: limits, MaxQueries: *maxQueries, MaxConcurrent: *maxConcurrent, TTL: *ttl})
	if e != nil {
		return e
	}
	defer handler.Close()
	server := &http.Server{Addr: *listen, Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10}
	done := make(chan error, 1)
	go func() { fmt.Fprintln(os.Stderr, "Kelvo Go listening on "+*listen); done <- server.ListenAndServe() }()
	select {
	case e = <-done:
		if errors.Is(e, http.ErrServerClosed) {
			return nil
		}
		return e
	case <-ctx.Done():
		handler.Close()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return server.Shutdown(shutdown)
	}
}

func resolveCLISandbox(path string) (string, error) {
	if path == "" {
		return "", nil
	}
	if runtime.GOOS != "linux" {
		return "", query.NewError("CONFIGURATION_ERROR", "Worker sandbox requires Linux Landlock")
	}
	// The worker starts from its own temporary directory. Resolve CLI-relative
	// paths now, and apply the same launcher permission checks as cluster nodes.
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", query.NewError("CONFIGURATION_ERROR", "Sandbox launcher path is unavailable")
	}
	info, err := os.Stat(abs)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&0111 == 0 || info.Mode()&0022 != 0 {
		return "", query.NewError("CONFIGURATION_ERROR", "Sandbox launcher must be executable and not writable by group or others")
	}
	return abs, nil
}

func runWorker() error {
	var in worker.Input
	d := json.NewDecoder(io.LimitReader(os.Stdin, 2<<20))
	d.DisallowUnknownFields()
	var outcome worker.Outcome
	emit := func(err error) error {
		if err != nil {
			outcome.Error = query.PublicError(err)
		}
		if e := json.NewEncoder(os.Stderr).Encode(outcome); e != nil {
			return e
		}
		return nil
	}
	if e := d.Decode(&in); e != nil {
		return emit(query.NewError("INVALID_ARGUMENT", "Invalid worker request"))
	}
	if e := d.Decode(new(any)); e != io.EOF {
		return emit(query.NewError("INVALID_ARGUMENT", "Worker request must contain one JSON object"))
	}
	if e := in.Limits.Validate(); e != nil {
		return emit(e)
	}
	if in.Request.Mode == "" {
		in.Request.Mode = "federated"
	}
	if err := query.ValidateRequest(in.Request); err != nil {
		return emit(err)
	}
	parent, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(parent, in.Limits.Timeout)
	defer cancel()
	var executor query.Executor
	if in.Request.Mode == "native" {
		e, err := native.New(in.Config, in.Limits, in.Request)
		if err != nil {
			return emit(err)
		}
		defer e.Close()
		executor = e
	} else {
		e, err := duckengine.New(in.Config, in.Limits)
		if err != nil {
			return emit(err)
		}
		executor = e
	}
	sink := worker.NewIPCSink(os.Stdout, in.Limits)
	stats, err := executor.Execute(ctx, in.Request, sink)
	outcome.Stats = stats
	if err == nil {
		err = sink.Finish()
	}
	outcome.Stats.WireBytes = sink.EncodedBytes()
	return emit(err)
}

func makeRequest(sql, mode, connection, sources, parameters, collection, pipeline string) (query.Request, error) {
	r := query.Request{SQL: sql, Mode: mode, ConnectionID: connection}
	if sources != "" {
		r.Sources = strings.Split(sources, ",")
	}
	if len(parameters) > 128<<10 || json.Unmarshal([]byte(parameters), &r.Parameters) != nil {
		return r, query.NewError("INVALID_ARGUMENT", "Invalid typed parameter JSON")
	}
	if collection != "" || pipeline != "" {
		if collection == "" || len(pipeline) > 128<<10 {
			return r, query.NewError("INVALID_ARGUMENT", "MongoDB requires --mongo-collection and a bounded pipeline")
		}
		if pipeline == "" {
			pipeline = "[]"
		}
		if !strings.HasPrefix(strings.TrimSpace(pipeline), "[") {
			return r, query.NewError("INVALID_ARGUMENT", "MongoDB pipeline must be a JSON array")
		}
		r.Mongo = &query.MongoRequest{Collection: collection}
		if json.Unmarshal([]byte(pipeline), &r.Mongo.Pipeline) != nil {
			return r, query.NewError("INVALID_ARGUMENT", "Invalid MongoDB pipeline JSON")
		}
	}
	return r, query.ValidateRequest(r)
}
func usage() {
	fmt.Println("Kelvo Go by SYNEHQ\n\nUsage: kelvo serve|query|accelerate|cluster-init|gateway|node|version\nBuild: go build -tags duckdb_arrow ./cmd/kelvo\nUse kelvo <command> -h for flags.")
}
