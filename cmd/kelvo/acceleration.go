// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/acceleration"
	"github.com/SYNEHQ/kelvo-go/internal/admission"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/cluster"
	"github.com/SYNEHQ/kelvo-go/internal/containment"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/telemetry"
	"github.com/SYNEHQ/kelvo-go/internal/tracing"
	"github.com/SYNEHQ/kelvo-go/internal/worker"
)

type refreshOptions struct {
	Containment           *containment.Manager
	ContainmentBudget     containment.Budget
	ResourceOverheadBytes int64
	ScratchRoot           *worker.ScratchRoot
	SourceHealth          *telemetry.SourceHealth
	Admission             worker.SourceAdmitter
	Secrets               worker.SecretResolver
}

func refreshFactory(sandbox string, options ...refreshOptions) acceleration.ExecutorFactory {
	return func(c catalog.Config, limits query.Limits) (query.Executor, error) {
		e, err := worker.New(c, limits)
		if err == nil {
			e.SandboxPath = sandbox
			if len(options) > 0 {
				e.SourceAdmission = options[0].Admission
				e.Secrets = options[0].Secrets
				e.SourceHealth = options[0].SourceHealth
				e.ScratchRoot = options[0].ScratchRoot
				e.Containment = options[0].Containment
				e.ContainmentBudget = options[0].ContainmentBudget
				e.ResourceOverheadBytes = options[0].ResourceOverheadBytes
			}
		}
		return e, err
	}
}

func runAcceleration(args []string) (resultErr error) {
	if len(args) == 0 {
		return query.NewError("INVALID_ARGUMENT", "Expected accelerate refresh, status, verify, inventory, restore, backup, migrate-backup, or watch")
	}
	if args[0] == "migrate-backup" {
		return runMigrationBackup(args[1:])
	}
	f := flag.NewFlagSet("accelerate "+args[0], flag.ContinueOnError)
	file := f.String("config", "kelvo.yml", "Registered sources and acceleration configuration (YAML)")
	id := f.String("dataset", "", "Dataset ID (required except for watch)")
	destination := f.String("destination", "", "New absolute local acceleration root (backup only)")
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
		return query.NewError("INVALID_ARGUMENT", "Specify --dataset for this command; watch schedules all configured datasets")
	}
	if args[0] != "refresh" && args[0] != "status" && args[0] != "watch" && args[0] != "verify" && args[0] != "inventory" && args[0] != "restore" && args[0] != "backup" {
		return query.NewError("INVALID_ARGUMENT", "Unknown acceleration command")
	}
	if (args[0] == "restore" && (*generation == "" || *expected == "")) || (args[0] != "restore" && (*generation != "" || *expected != "")) {
		return query.NewError("INVALID_ARGUMENT", "Restore requires generation and expected-generation; other commands reject them")
	}
	var destinationSet, backupForbidden bool
	f.Visit(func(item *flag.Flag) {
		if item.Name == "destination" {
			destinationSet = true
		}
		if item.Name == "sandbox" || item.Name == "generation" || item.Name == "expected-generation" {
			backupForbidden = true
		}
	})
	if (args[0] == "backup" && (*destination == "" || !filepath.IsAbs(*destination) || backupForbidden)) || (args[0] != "backup" && destinationSet) {
		return query.NewError("INVALID_ARGUMENT", "Backup requires an absolute destination and rejects sandbox and restore flags; other commands reject destination")
	}
	c, err := catalog.Load(*file)
	if err != nil {
		return query.NewError("CONFIGURATION_ERROR", err.Error())
	}
	if args[0] == "backup" && c.Acceleration != nil && c.Acceleration.ObjectStorage != nil {
		return query.NewError("UNSUPPORTED", "Snapshot backup requires local acceleration storage")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	objectRuntime, runtimeErr := acceleration.OpenObjectRuntime(c)
	var closeManager func() error
	var closeRuntime func(context.Context) error
	var publish func() error
	if objectRuntime != nil {
		closeRuntime = objectRuntime.Close
	}
	defer func() {
		resultErr = finishAccelerationCommand(ctx, resultErr, closeManager, closeRuntime, publish)
	}()
	if runtimeErr != nil {
		return query.NewError("CONFIGURATION_ERROR", "Protected object runtime is unavailable")
	}
	m, err := acceleration.NewManagerWithRuntime(c, refreshFactory(*sandbox), objectRuntime)
	if err != nil {
		return query.NewError("CONFIGURATION_ERROR", "Acceleration store cannot be opened")
	}
	closeManager = m.Close
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
	if args[0] == "backup" {
		snapshot, err := m.Backup(ctx, *id, *destination)
		if err != nil {
			// Even a populated snapshot cannot certify uncertain durability.
			return backupCLIError(snapshot, err)
		}
		publish = func() error {
			return encodeAccelerationResult(os.Stdout, struct {
				Verified bool                  `yaml:"verified"`
				Snapshot acceleration.Snapshot `yaml:"snapshot"`
			}{true, snapshot})
		}
		return nil
	}
	if args[0] == "inventory" {
		generations, err := m.Inventory(ctx, *id)
		if err != nil {
			return err
		}
		publish = func() error { return encodeAccelerationResult(os.Stdout, generations) }
		return nil
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
	publish = func() error {
		return encodeAccelerationResult(os.Stdout, struct {
			Ready    bool                  `yaml:"ready"`
			Snapshot acceleration.Snapshot `yaml:"snapshot"`
		}{snapshot.Fingerprint == fingerprint && snapshot.Age() <= d.MaxAge, snapshot})
	}
	return nil
}

// Put the sanitized operator message first so PublicError cannot select a raw
// nested query error. Preserve the underlying cause for errors.Is/As callers.
func backupCLIError(snapshot acceleration.Snapshot, err error) error {
	if err == nil {
		return nil
	}
	code, message := "BACKUP_FAILED", "Snapshot backup failed; inspect storage and the destination before retrying"
	switch {
	case snapshot.Generation != "":
		code, message = "BACKUP_DURABILITY_UNCERTAIN", "Backup destination was published; preserve it, resolve storage errors and verify before use"
	case errors.Is(err, context.DeadlineExceeded):
		code, message = "DEADLINE_EXCEEDED", "Snapshot backup deadline exceeded before publication; inspect the destination before retrying"
	case errors.Is(err, context.Canceled):
		code, message = "CANCELLED", "Snapshot backup cancelled before publication; inspect the destination before retrying"
	case errors.Is(err, acceleration.ErrBackupUnsupported), errors.Is(err, acceleration.ErrRecoveryUnsupported):
		code, message = "UNSUPPORTED", "Snapshot backup requires local storage on Linux with atomic no-replace directory publication"
	case errors.Is(err, os.ErrExist):
		code, message = "ALREADY_EXISTS", "Backup destination already exists; preserve it and choose a new destination"
	case errors.Is(err, acceleration.ErrFingerprintMismatch):
		code, message = "CONFIGURATION_ERROR", "Snapshot does not match the current catalog; refresh the dataset before backup"
	case errors.Is(err, acceleration.ErrCorrupt):
		code, message = "DATASET_UNAVAILABLE", "Snapshot storage or integrity validation failed; preserve the data and investigate before retrying"
	case errors.Is(err, os.ErrPermission):
		code, message = "PERMISSION_DENIED", "Snapshot backup requires access to private operator-owned source and destination directories"
	}
	return errors.Join(query.NewError(code, message), err)
}

// Cluster dispatch uses a tenant's existing authenticated JetStream account.
// Messages contain dataset identity and definition fingerprint only. Every node
// must mount the same tenant snapshot store with working POSIX flock semantics.
func runClusterRefresh(ctx context.Context, c catalog.Config, sandbox string, queue *cluster.RefreshQueue, pool *admission.Pool, overhead int64, metrics *telemetry.Registry, gate *refreshGate, sourceQuotas worker.SourceAdmitter, secrets worker.SecretResolver, sourceHealth *telemetry.SourceHealth, scratchRoot *worker.ScratchRoot, processManager *containment.Manager, processBudget containment.Budget, objectRuntime *acceleration.ObjectRuntime, runtimeAudit *cluster.ServiceAudit, policy cluster.Policy, recorders ...*tracing.Recorder) error {
	m, err := acceleration.NewManagerWithRuntime(c, refreshFactory(sandbox, refreshOptions{Admission: sourceQuotas, Secrets: secrets, SourceHealth: sourceHealth, ScratchRoot: scratchRoot, Containment: processManager, ContainmentBudget: processBudget, ResourceOverheadBytes: overhead}), objectRuntime)
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
				return runtimeAudit.RunRefresh(ctx, policy, func(ctx context.Context) error {
					_, err := m.Refresh(ctx, job.Dataset, true)
					return err
				})
			}, recorders...)
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
