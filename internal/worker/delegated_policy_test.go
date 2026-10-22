// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/access"
	"github.com/SYNEHQ/kelvo-go/internal/delegation"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

func delegatedPolicyFixture(account string) (query.Request, delegation.Execution) {
	request := query.Request{Mode: "federated", Sources: []string{"src"}, SQL: "SELECT amount FROM src.orders", Delegation: "node-verified-fixture-grant"}
	digest, _ := delegation.QueryDigest(request)
	claims := delegation.Claims{Version: 2, Issuer: "gateway", AppTeam: "team-a", QuerySHA256: digest, ExpiresAt: time.Now().Add(time.Minute).Unix(),
		Sources: []delegation.Source{{Alias: "src", ConnectionID: "saved-connection", Database: "analytics", Tables: []delegation.Table{{Name: "orders", Table: "orders"}}}},
		RowColumnPolicy: &delegation.DelegatedPolicy{Revision: strings.Repeat("c", 64), Sources: map[string]delegation.DelegatedSourcePolicy{"src": {Tables: map[string]delegation.DelegatedTablePolicy{
			"orders": {Columns: []string{"amount"}, Rows: &delegation.DelegatedPredicate{Kind: "comparison", Column: "account_id", Type: "int64", Op: "eq", Value: account}}}}}}}
	return request, delegation.Execution{Token: request.Delegation, Claims: claims, Binding: delegation.ExecutionBinding{JobID: "job", WorkerID: "worker", Owner: "owner", Claim: "claim"}}
}

func executionContext(ctx context.Context, e delegation.Execution) context.Context {
	return delegation.WithExecution(ctx, e.Claims, e.Token, e.Binding)
}

// A grant that carries a policy runs only under exactly that policy.
func TestDelegatedExecutionRequiresTheGrantsOwnPolicy(t *testing.T) {
	request, execution := delegatedPolicyFixture("42")
	policy, err := access.FromDelegated(execution.Claims.RowColumnPolicy)
	if err != nil {
		t.Fatal(err)
	}
	restricted, err := access.WithPolicy(context.Background(), policy)
	if err != nil {
		t.Fatal(err)
	}
	if _, dynamic, err := delegatedExecution(executionContext(restricted, execution), request); err != nil || !dynamic {
		t.Fatalf("grant under its own policy refused: %v", err)
	}
	_, other := delegatedPolicyFixture("43")
	otherPolicy, _ := access.FromDelegated(other.Claims.RowColumnPolicy)
	foreign, _ := access.WithPolicy(context.Background(), otherPolicy)
	unrelated, _ := access.WithPolicy(context.Background(), access.Policy{Sources: map[string]access.SourcePolicy{"src": {Tables: map[string]access.TablePolicy{"orders": {Columns: []string{"amount"}, AllRows: true}}}}})
	for name, ctx := range map[string]context.Context{
		"no policy at all":          context.Background(),
		"another user's policy":     foreign,
		"a wider unrelated policy":  unrelated,
		"policy on a version 1 job": restricted,
	} {
		e := execution
		if name == "policy on a version 1 job" {
			e.Claims.Version, e.Claims.RowColumnPolicy = 1, nil
		}
		_, _, err := delegatedExecution(executionContext(ctx, e), request)
		var public *query.Error
		if !errors.As(err, &public) || public.Code != "PERMISSION_DENIED" {
			t.Errorf("%s: executed (%v)", name, err)
		}
	}
	// An invalid policy in claims that somehow reached the node is refused too.
	bad := execution
	bad.Claims.RowColumnPolicy = bad.Claims.RowColumnPolicy.Clone()
	table := bad.Claims.RowColumnPolicy.Sources["src"].Tables["orders"]
	table.Rows.Op = "like"
	bad.Claims.RowColumnPolicy.Sources["src"].Tables["orders"] = table
	if _, _, err := delegatedExecution(executionContext(restricted, bad), request); err == nil {
		t.Fatal("grant with an invalid policy executed")
	}
}

// A restricted read never takes the pooled read worker, the result cache or a
// routed live source, even when the request is otherwise eligible for them.
func TestRestrictedReadsSkipReadWorkersCacheAndRouting(t *testing.T) {
	fs := &fakeSet{serve: func(p *fakeWorkerProcess) { serveFake(p, nil) }}
	e, mgr := readWorkerExecutor(t, fs)
	_, execution := delegatedPolicyFixture("42")
	policy, _ := access.FromDelegated(execution.Claims.RowColumnPolicy)
	restricted, err := access.WithPolicy(context.Background(), policy)
	if err != nil {
		t.Fatal(err)
	}
	native := query.Request{Mode: "native", ConnectionID: "app", SQL: "SELECT 1"}
	sink := &workerTestSink{}
	if _, err, handled := e.executeOnReadWorker(restricted, delegation.Execution{}, false, native, native, sink); handled || err != nil {
		t.Fatalf("restricted read reached the read worker path: handled=%v err=%v", handled, err)
	}
	if len(fs.procs) != 0 || mgr.metrics.Hits.Load() != 0 || sink.rows != 0 {
		t.Fatal("restricted read launched or used a read worker")
	}
	// Dynamic (delegated) restricted reads are federated: they cannot match the native read worker either.
	federated := query.Request{Mode: "federated", Sources: []string{"src"}, SQL: "SELECT 1"}
	if _, _, handled := e.executeOnReadWorker(restricted, execution, true, federated, federated, &workerTestSink{}); handled {
		t.Fatal("federated restricted read reached the read worker path")
	}
	if len(fs.procs) != 0 {
		t.Fatal("restricted federated read launched a worker")
	}
	router, r := routeExecutor(t)
	if routed, live := router.planRoute(restricted, r, false); routed != nil || live.Sources[0] != r.Sources[0] {
		t.Fatalf("restricted delegated read routed to %v", live.Sources)
	}
}
