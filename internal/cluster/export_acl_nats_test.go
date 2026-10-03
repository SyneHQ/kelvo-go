// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/nats-io/nats.go"
)

// Requires a fresh, two-account broker from export_broker_acceptance.py. Roles
// are independently authenticated and accounts have no imports or exports.
func TestNATSExportRuntimeACLAndAccountIsolation(t *testing.T) {
	url, ca := os.Getenv("KELVO_TEST_EXPORT_ACL_URL"), os.Getenv("KELVO_TEST_EXPORT_ACL_CA_FILE")
	if url == "" || ca == "" {
		t.Skip("set the dedicated two-account export ACL fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	type account struct {
		policy                             Policy
		initializer, gateway, worker, base *NATSStore
		gatewayExports, workerExports      ExportStore
		snapshot                           ExportSnapshot
	}
	accounts := make([]account, 2)
	for i, tenant := range []string{"a", "b"} {
		a := &accounts[i]
		a.policy = runtimeExportConfigFixture(t).Policy
		a.policy.TenantID = tenant
		a.policy.Exports.MaxJobs = 2
		open := func(role string, initialize bool) *NATSStore {
			prefix := "KELVO_TEST_EXPORT_ACL_" + strings.ToUpper(tenant+"_"+role)
			cfg := NATSConfig{URL: url, CAFile: ca, Username: os.Getenv(prefix + "_USER"), PasswordEnv: prefix + "_PASSWORD"}
			st, err := OpenStore(ctx, cfg, a.policy, initialize)
			if err != nil {
				t.Fatalf("%s/%s open: %v", tenant, role, err)
			}
			t.Cleanup(func() { _ = st.Close() })
			return st
		}
		a.initializer = open("initializer", true)
		if _, err := OpenExportStore(ctx, a.initializer, true); err != nil {
			t.Fatal("restricted export initializer", err)
		}
		a.gateway, a.worker, a.base = open("gateway", false), open("worker", false), open("base", false)
		var err error
		a.gatewayExports, err = OpenExportStore(ctx, a.gateway, false)
		if err != nil {
			t.Fatal("gateway open", err)
		}
		a.workerExports, err = OpenExportStore(ctx, a.worker, false)
		if err != nil {
			t.Fatal("worker open", err)
		}
	}

	t.Run("separate_roles_and_bidirectional_account_isolation", func(t *testing.T) {
		for i := range accounts {
			a := &accounts[i]
			authority, _ := authorityForPrincipal(a.policy, "reports")
			authorityCtx := context.WithValue(ctx, jobAuthorityKey{}, authority)
			var err error
			a.snapshot, err = a.gatewayExports.SubmitExport(authorityCtx, ExportSubmission{
				Request: query.Request{Mode: "federated", SQL: "SELECT 1"}, SupervisorOwner: exportTestOwner,
				AuthorityUntil: time.Now().UTC().Add(a.policy.LeaseDuration - time.Millisecond)})
			if err != nil {
				t.Fatal("gateway submit", err)
			}
		}
		for i := range accounts {
			a, other := &accounts[i], &accounts[1-i]
			if _, err := a.gatewayExports.GetExport(ctx, other.snapshot.Job.ID); !errors.Is(err, ErrExportNotFound) {
				t.Fatal("foreign export ID was accessible", err)
			}
			kv, err := a.gateway.js.KeyValue(ctx, exportJobsBucket)
			if err != nil {
				t.Fatal(err)
			}
			entry, err := kv.Get(ctx, exportKey(0))
			if err != nil {
				t.Fatal(err)
			}
			var raw ExportJob
			if json.Unmarshal(entry.Value(), &raw) != nil || raw.ID != a.snapshot.Job.ID || raw.TenantID != a.policy.TenantID {
				t.Fatal("shared subject resolved outside authenticated account")
			}
		}
		if err := accounts[0].gatewayExports.EnqueueExport(ctx, accounts[0].snapshot.Job.ID); err != nil {
			t.Fatal(err)
		}
		foreignCtx, foreignCancel := context.WithTimeout(ctx, 100*time.Millisecond)
		foreignDelivery, foreignErr := accounts[1].workerExports.NextExport(foreignCtx)
		foreignCancel()
		if foreignDelivery != nil || (!errors.Is(foreignErr, ErrNoExport) && !errors.Is(foreignErr, context.DeadlineExceeded)) {
			t.Fatal("account B saw account A dispatch", foreignErr)
		}
		if err := accounts[1].gatewayExports.EnqueueExport(ctx, accounts[1].snapshot.Job.ID); err != nil {
			t.Fatal(err)
		}
		for i := range accounts {
			a := &accounts[i]
			if err := a.worker.ClaimWorker(ctx, "a1", exportTestOwner); err != nil {
				t.Fatal(err)
			}
			delivery, err := a.workerExports.NextExport(ctx)
			if err != nil || delivery.ID() != a.snapshot.Job.ID {
				t.Fatal("worker dispatch", err)
			}
			next := a.snapshot.Job
			next.State, next.WorkerID, next.WorkerOwner = ExportAssigned, "a1", exportTestOwner
			a.snapshot, err = a.workerExports.CompareAndSwapExport(ctx, a.snapshot, next)
			if err != nil {
				t.Fatal("worker assignment", err)
			}
			if err = delivery.Ack(ctx); err != nil {
				t.Fatal("worker ACK", err)
			}
			next = a.snapshot.Job
			next.State, next.Claim = ExportClaimed, exportTestClaim
			authority, _ := authorityForPrincipal(a.policy, "reports")
			authorityCtx := context.WithValue(ctx, jobAuthorityKey{}, authority)
			a.snapshot, err = a.gatewayExports.CompareAndSwapExport(authorityCtx, a.snapshot, next)
			if err != nil {
				t.Fatal("gateway claim", err)
			}
			next = a.snapshot.Job
			next.State = ExportRunning
			a.snapshot, err = a.workerExports.CompareAndSwapExport(ctx, a.snapshot, next)
			if err != nil {
				t.Fatal("worker running", err)
			}
			next = a.snapshot.Job
			next.State, next.Error = ExportCancelled, query.PublicError(context.Canceled)
			a.snapshot, err = a.gatewayExports.CompareAndSwapExport(ctx, a.snapshot, next)
			if err != nil {
				t.Fatal("gateway cancellation", err)
			}
		}
	})

	t.Run("runtime_and_initializer_permissions_are_exact", func(t *testing.T) {
		for _, a := range accounts {
			for _, runtime := range []*NATSStore{a.gateway, a.worker, a.base} {
				for _, stream := range []string{"KV_KELVO_META", "KV_KELVO_JOBS", queueStream, "KV_KELVO_EXPORT_JOBS", exportQueueStream} {
					for _, operation := range []string{"CREATE", "UPDATE", "DELETE"} {
						assertExportPublishDenied(t, runtime.nc, "$JS.API.STREAM."+operation+"."+stream)
					}
				}
				for _, subject := range []string{
					"$JS.API.CONSUMER.CREATE.KELVO_EXPORT_QUEUE.exports.export.ready",
					"$JS.API.CONSUMER.DELETE.KELVO_EXPORT_QUEUE.exports",
					"$JS.API.CONSUMER.CREATE.KELVO_QUEUE.dispatch",
					"$JS.API.CONSUMER.DELETE.KELVO_QUEUE.dispatch",
					"$KV.KELVO_META.meta.config", "$JS.API.DIRECT.GET.KV_KELVO_EXPORT_JOBS.export.0",
				} {
					assertExportPublishDenied(t, runtime.nc, subject)
				}
			}
			for _, subject := range []string{
				"$JS.API.STREAM.INFO.KV_KELVO_EXPORT_JOBS", "$JS.API.STREAM.MSG.GET.KV_KELVO_EXPORT_JOBS",
				"$KV.KELVO_EXPORT_JOBS.export.0", "export.ready",
				"$JS.API.CONSUMER.INFO.KELVO_EXPORT_QUEUE.exports",
			} {
				assertExportPublishDenied(t, a.base.nc, subject)
			}
			for _, subject := range []string{
				"$JS.API.STREAM.CREATE.UNDECLARED", "$JS.API.STREAM.UPDATE.UNDECLARED", "$JS.API.STREAM.DELETE.UNDECLARED",
				"$JS.API.CONSUMER.CREATE.KELVO_EXPORT_QUEUE.other.export.ready", "$JS.API.CONSUMER.DELETE.KELVO_EXPORT_QUEUE.other",
				"$KV.KELVO_EXPORT_JOBS.export.0", "export.ready",
			} {
				assertExportPublishDenied(t, a.initializer.nc, subject)
			}
			assertExportPublishDenied(t, a.gateway.nc, "$JS.API.CONSUMER.MSG.NEXT.KELVO_EXPORT_QUEUE.exports")
			assertExportPublishDenied(t, a.worker.nc, "export.ready")
			// Runtime deletion probes did not change either queue or retained state.
			if _, err := OpenExportStore(ctx, a.gateway, false); err != nil {
				t.Fatal("runtime probe damaged namespace", err)
			}
			if got, err := a.gatewayExports.GetExport(ctx, a.snapshot.Job.ID); err != nil || got.Job.State != ExportCancelled {
				t.Fatal("runtime probe changed retained state", err)
			}
		}
	})

	t.Run("initializer_can_manage_only_declared_namespaces", func(t *testing.T) {
		for _, a := range accounts {
			stream, err := a.initializer.js.Stream(ctx, exportQueueStream)
			if err != nil {
				t.Fatal(err)
			}
			info, err := stream.Info(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = a.initializer.js.UpdateStream(ctx, info.Config); err != nil {
				t.Fatal("declared update", err)
			}
			if err = stream.DeleteConsumer(ctx, exportQueueConsumer); err != nil {
				t.Fatal("declared consumer delete", err)
			}
			if _, err = stream.CreateConsumer(ctx, exportConsumerConfig(a.policy)); err != nil {
				t.Fatal("declared consumer create", err)
			}
			if err = a.initializer.js.DeleteStream(ctx, exportQueueStream); err != nil {
				t.Fatal("declared stream delete", err)
			}
			if _, err = a.initializer.js.CreateStream(ctx, exportStreamConfig(a.policy)); err != nil {
				t.Fatal("declared stream create", err)
			}
			if _, err = OpenExportStore(ctx, a.initializer, true); err != nil {
				t.Fatal("declared reprovision", err)
			}
		}
	})
}

// A request timeout alone is not evidence of denial. Require the broker's
// asynchronous permission violation for the exact attempted publish subject.
func assertExportPublishDenied(t *testing.T, nc *nats.Conn, subject string) {
	t.Helper()
	denials := make(chan error, 4)
	nc.SetErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
		select {
		case denials <- err:
		default:
		}
	})
	defer nc.SetErrorHandler(nil)
	if err := nc.Publish(subject, []byte("{}")); err != nil {
		t.Fatal("denial probe publish", err)
	}
	if err := nc.FlushTimeout(time.Second); err != nil {
		t.Fatal("denial probe connection", err)
	}
	select {
	case err := <-denials:
		if !errors.Is(err, nats.ErrPermissionViolation) && !strings.Contains(strings.ToLower(err.Error()), "permissions violation") {
			t.Fatalf("non-permission failure for %s: %v", subject, err)
		}
		if !strings.Contains(err.Error(), subject) {
			t.Fatalf("denial did not identify %s: %v", subject, err)
		}
	case <-time.After(time.Second):
		t.Fatalf("broker did not deny publish to %s", subject)
	}
}
