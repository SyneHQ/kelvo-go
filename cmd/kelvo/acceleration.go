// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/acceleration"
	"github.com/SYNEHQ/kelvo-go/internal/admission"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/cluster"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/telemetry"
	"github.com/SYNEHQ/kelvo-go/internal/worker"
	"go.yaml.in/yaml/v3"
)

type refreshOptions struct {
	Admission worker.SourceAdmitter
	Secrets   worker.SecretResolver
}

func refreshFactory(sandbox string, options ...refreshOptions) acceleration.ExecutorFactory {
	return func(c catalog.Config, limits query.Limits) (query.Executor, error) {
		e, err := worker.New(c, limits)
		if err == nil {
			e.SandboxPath = sandbox
			if len(options) > 0 {
				e.SourceAdmission = options[0].Admission
				e.Secrets = options[0].Secrets
			}
		}
		return e, err
	}
}

func runAcceleration(args []string) error {
	if len(args) == 0 {
		return query.NewError("INVALID_ARGUMENT", "Expected accelerate refresh, status, verify, inventory, restore, or watch")
	}
	f := flag.NewFlagSet("accelerate "+args[0], flag.ContinueOnError)
	file := f.String("config", "kelvo.yml", "Registered sources and acceleration configuration (YAML)")
	id := f.String("dataset", "", "Dataset ID (required except for watch)")
	generation := f.String("generation", "", "Generation to restore (restore only)")
	expected := f.String("expected-generation", "", "Current generation precondition (restore only)")
	sandbox := f.String("sandbox", "", "Optional native sandbox launcher for source refresh workers")
	if err := f.Parse(args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if f.NArg() != 0 || (args[0] != "watch" && *id == "") || (args[0] == "watch" && *id != "") {
		return query.NewError("INVALID_ARGUMENT", "Specify --dataset for refresh, status, or verify; watch schedules all configured datasets")
	}
	if args[0] != "refresh" && args[0] != "status" && args[0] != "watch" && args[0] != "verify" && args[0] != "inventory" && args[0] != "restore" {
		return query.NewError("INVALID_ARGUMENT", "Unknown acceleration command")
	}
	if (args[0] == "restore" && (*generation == "" || *expected == "")) || (args[0] != "restore" && (*generation != "" || *expected != "")) {
		return query.NewError("INVALID_ARGUMENT", "Restore requires generation and expected-generation; other commands reject them")
	}
	c, err := catalog.Load(*file)
	if err != nil {
		return query.NewError("CONFIGURATION_ERROR", err.Error())
	}
	m, err := acceleration.NewManager(c, refreshFactory(*sandbox))
	if err != nil {
		return query.NewError("CONFIGURATION_ERROR", "Acceleration store cannot be opened")
	}
	defer m.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if args[0] == "watch" {
		err := m.Run(ctx, func(id string, err error) { fmt.Fprintln(os.Stderr, "Dataset "+id+": "+query.PublicError(err).Message) })
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	}
	if _, ok := c.Dataset(*id); !ok {
		return query.NewError("INVALID_ARGUMENT", "Unknown accelerated dataset")
	}
	if args[0] == "inventory" {
		generations, err := m.Inventory(ctx, *id)
		if err != nil {
			return err
		}
		return yaml.NewEncoder(os.Stdout).Encode(generations)
	}
	var snapshot acceleration.Snapshot
	if args[0] == "verify" {
		snapshot, err = m.Verify(ctx, *id)
		if err != nil {
			return err
		}
	}
	if args[0] == "restore" {
		snapshot, err = m.Restore(ctx, *id, *generation, *expected)
	} else if args[0] == "refresh" {
		snapshot, err = m.Refresh(ctx, *id, false)
	} else if args[0] != "verify" {
		snapshot, err = m.StatusContext(ctx, *id)
	}
	if err != nil {
		return err
	}
	d, _ := c.Dataset(*id)
	fingerprint, _ := c.DatasetFingerprint(*id)
	return yaml.NewEncoder(os.Stdout).Encode(struct {
		Ready    bool                  `yaml:"ready"`
		Snapshot acceleration.Snapshot `yaml:"snapshot"`
	}{snapshot.Fingerprint == fingerprint && snapshot.Age() <= d.MaxAge, snapshot})
}

// Cluster dispatch uses a tenant's existing authenticated JetStream account.
// Messages contain dataset identity and definition fingerprint only. Every node
// must mount the same tenant snapshot store with working POSIX flock semantics.
func runClusterRefresh(ctx context.Context, c catalog.Config, sandbox string, queue *cluster.RefreshQueue, pool *admission.Pool, overhead int64, metrics *telemetry.Registry, gate *refreshGate, sourceQuotas worker.SourceAdmitter, secrets worker.SecretResolver) error {
	m, err := acceleration.NewManager(c, refreshFactory(sandbox, refreshOptions{Admission: sourceQuotas, Secrets: secrets}))
	if err != nil {
		return err
	}
	defer m.Close()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	consumerDone := make(chan error, 1)
	go func() {
		consumerDone <- queue.Consume(ctx, func(ctx context.Context, job cluster.RefreshJob) error {
			release, err := gate.Enter()
			if err != nil {
				metrics.Reject(telemetry.KindRefresh, telemetry.RejectionDraining)
				return err
			}
			defer release()
			d, ok := c.Dataset(job.Dataset)
			if !ok || d.RefreshInterval == 0 {
				return nil
			} // obsolete configuration
			fingerprint, err := c.DatasetFingerprint(job.Dataset)
			if err != nil {
				return err
			}
			if job.Fingerprint != fingerprint {
				return query.NewError("CONFIGURATION_ERROR", "Refresh workers have incompatible dataset definitions")
			}
			// Hold the reservation through snapshot publication and pruning.
			return withRefreshReservation(ctx, pool, overhead, d.Limits, metrics, func(ctx context.Context) error {
				_, err := m.Refresh(ctx, job.Dataset, true)
				return err
			})
		}, func(err error) { fmt.Fprintln(os.Stderr, "Acceleration refresh: "+query.PublicError(err).Message) })
	}()
	defer func() { cancel(); <-consumerDone }()
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()
	for {
		if gate.IsDraining() {
			<-ctx.Done()
			return ctx.Err()
		}
		for _, d := range c.Acceleration.Datasets {
			if d.RefreshInterval == 0 {
				continue
			}
			fingerprint, _ := c.DatasetFingerprint(d.ID)
			snapshot, err := m.StatusContext(ctx, d.ID)
			if err == nil && snapshot.Fingerprint == fingerprint && snapshot.Age() < d.RefreshInterval {
				continue
			}
			// Duplicate schedulers share a time-window message ID. Durable jobs
			// are still idempotent after redelivery or deduplication expiry.
			bucket := time.Now().UnixNano() / int64(d.RefreshInterval)
			if err := queue.Publish(ctx, cluster.RefreshJob{Dataset: d.ID, Fingerprint: fingerprint}, fmt.Sprintf("%s:%s:%d", d.ID, fingerprint, bucket)); err != nil && ctx.Err() == nil {
				fmt.Fprintln(os.Stderr, "Acceleration dispatch is unavailable")
			}
		}
		select {
		case err := <-consumerDone:
			// Restore the completion token for the deferred join.
			consumerDone <- err
			return err
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}
