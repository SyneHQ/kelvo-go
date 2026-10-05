//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package authfence

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestAuthFenceR3(t *testing.T) {
	r3OptIn(t)
	for _, release := range r3Archives {
		t.Run(release.version, func(t *testing.T) {
			f := newR3Fixture(t, release)
			cases := []struct {
				name string
				run  func(*testing.T, *r3Fixture)
			}{
				{"GuardedWitnessAndCAS", r3GuardedWitnessAndCAS},
				{"FormerLeaderAndQuorumLoss", r3FormerLeaderAndQuorumLoss},
				{"ConcurrentAdvanceWitness", r3ConcurrentAdvanceWitness},
				{"ResponseCorrelationAndUnknown", r3ResponseCorrelationAndUnknown},
				{"RetentionAndACLPressure", r3RetentionAndACLPressure},
				{"Cleanup", func(t *testing.T, f *r3Fixture) {
					if err := f.close(); err != nil {
						t.Fatal("owned fixture cleanup not proven", err)
					}
					for _, path := range []string{f.key, filepath.Join(f.dir, "node-0.conf"), filepath.Join(f.dir, "node-1.conf"), filepath.Join(f.dir, "node-2.conf")} {
						if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
							t.Fatal("generated fixture credential remains")
						}
					}
				}},
			}
			type caseResult struct {
				Name   string `json:"name"`
				Passed bool   `json:"passed"`
			}
			results := make([]caseResult, 0, len(cases))
			for _, gate := range cases {
				passed := t.Run(gate.name, func(t *testing.T) {
					if f.ctx.Err() != nil && gate.name != "Cleanup" {
						t.Fatal("R3 fixture deadline expired before gate")
					}
					gate.run(t, f)
					if gate.name != "Cleanup" {
						if err := f.sampleResources(); err != nil {
							t.Error("broker RSS/FD sample unavailable", err)
						}
					}
				})
				results = append(results, caseResult{gate.name, passed})
			}
			if len(results) != 6 {
				t.Fatal("required R3 gate missing")
			}
			receipt := struct {
				Schema             int               `json:"schema"`
				Version            string            `json:"version"`
				BinarySHA          string            `json:"binary_sha256"`
				MaxBrokers         int               `json:"max_brokers"`
				Cases              []caseResult      `json:"cases"`
				Cleanup            bool              `json:"cleanup_passed"`
				CredentialsRemoved bool              `json:"credentials_removed"`
				RSS                uint64            `json:"sampled_broker_rss_max_bytes"`
				FDs                int               `json:"sampled_broker_fds_max"`
				ResourceReadErrors int               `json:"resource_read_errors"`
				Brokers            []r3BrokerOutcome `json:"brokers"`
			}{1, f.version, f.binarySHA, 3, results, results[5].Passed, f.credentialsRemoved, f.peakRSS, f.peakFDs, f.resourceReadErrors, []r3BrokerOutcome{f.nodes[0].identity, f.nodes[1].identity, f.nodes[2].identity}}
			raw, err := json.MarshalIndent(receipt, "", "  ")
			if err != nil || os.WriteFile(filepath.Join(f.dir, "receipt.json"), append(raw, '\n'), 0600) != nil {
				t.Fatal("R3 sanitized receipt write failed")
			}
			t.Logf("R3_RECEIPT version=%s gates=%d cleanup=%t sampled_rss_bytes=%d sampled_fds=%d", f.version, len(results), receipt.Cleanup, f.peakRSS, f.peakFDs)
		})
	}
}

func r3Headers(after uint64) nats.Header {
	return nats.Header{expectStreamHeader: []string{StreamName}, expectSequenceHeader: []string{strconv.FormatUint(after, 10)}, expectSubjectHeader: []string{AuthoritySubject}}
}

func (f *r3Fixture) advance(t *testing.T, node int) Snapshot {
	t.Helper()
	prior := f.read(t, node)
	client := f.client(t, node, "control")
	defer r3CloseClient(t, client)
	result, err := client.Advance(f.ctx, r3Attempt(t), prior.Record().Document().Revision(), r3Document(t, prior.Record().Document().Revision()+1))
	if err != nil || result.Outcome() != OutcomeAdvanced || !result.Snapshot().Valid() {
		t.Fatal("fixture authority advance failed", err)
	}
	return result.Snapshot()
}

func (f *r3Fixture) streamInfo(t *testing.T, node int) []byte {
	t.Helper()
	connection := f.connect(t, node, "control", nil)
	defer connection.Close()
	response, err := r3Request(connection, ControlReplicaID, streamInfoSubject, []byte("{}"), nil, time.Second)
	if err != nil || response == nil {
		t.Fatal("fixture stream info failed")
	}
	return response.Data
}

func (f *r3Fixture) leader(t *testing.T, node int, excluded int) int {
	t.Helper()
	found := -1
	connection := f.connect(t, node, "control", nil)
	defer connection.Close()
	if !r3Wait(f.ctx, 12*time.Second, func() bool {
		response, err := r3Request(connection, ControlReplicaID, streamInfoSubject, []byte("{}"), nil, 400*time.Millisecond)
		if err != nil || response == nil {
			return false
		}
		var info struct {
			Cluster struct {
				Leader string `json:"leader"`
			} `json:"cluster"`
		}
		if json.Unmarshal(response.Data, &info) != nil {
			return false
		}
		for i, candidate := range f.nodes {
			if i != excluded && info.Cluster.Leader == candidate.name {
				found = i
				return true
			}
		}
		return false
	}) {
		t.Fatal("R3 stream leader did not converge")
	}
	return found
}

func r3GuardedWitnessAndCAS(t *testing.T, f *r3Fixture) {
	prior := f.read(t, 0)
	control := f.client(t, 0, "control")
	result, err := control.Initialize(f.ctx, r3Attempt(t), r3Document(t, prior.Record().Document().Revision()+1))
	if err != ErrConflict || result.Outcome() != OutcomeConflict {
		t.Fatal("initialization overwrote existing authority", err)
	}
	current := f.advance(t, 0)
	gateway := f.client(t, 0, "gateway-a")
	attempt := r3Attempt(t)
	proof, err := gateway.Verify(f.ctx, attempt, current.Record().Document())
	if err != nil || !proof.ValidFor(f.scope, attempt, current.Record().Document(), time.Now()) || proof.WitnessSequence() <= current.AuthoritySequence() {
		t.Fatal("real guarded witness failed", err)
	}
	result, err = control.Check(f.ctx, r3Attempt(t), current.Record().Document())
	if err != nil || result.Outcome() != OutcomeCurrentVerified {
		t.Fatal("control witness did not verify current state", err)
	}
	result, err = control.Advance(f.ctx, r3Attempt(t), prior.Record().Document().Revision(), r3Document(t, current.Record().Document().Revision()+1))
	if err != ErrConflict || result.Outcome() != OutcomeConflict {
		t.Fatal("explicit stale document CAS was not rejected", err)
	}
	connection := f.connect(t, 0, "gateway-a", nil)
	response, err := r3Request(connection, "gateway-a", "auth.witness.gateway-a", []byte(`{"version":1}`), r3Headers(prior.AuthoritySequence()), time.Second)
	if err != nil || response == nil {
		t.Fatal("stale cross-subject guard did not return a definite conflict")
	}
	if _, err = parseAck(response.Data, prior.AuthoritySequence()); err != ErrConflict {
		t.Fatal("real stale cross-subject guard accepted", err)
	}
	if !f.read(t, 0).Record().Equal(current.Record()) {
		t.Fatal("failed CAS changed authority")
	}
}

func r3FormerLeaderAndQuorumLoss(t *testing.T, f *r3Fixture) {
	before := f.read(t, 0)
	connection := f.connect(t, 0, "control", nil)
	captured, err := r3Request(connection, ControlReplicaID, messageGetSubject, []byte(`{"last_by_subj":"auth.current"}`), nil, time.Second)
	connection.Close()
	if err != nil || captured == nil {
		t.Fatal("cannot capture pre-partition observational read")
	}
	oldLeader := f.leader(t, 0, -1)
	majorityNode := (oldLeader + 1) % 3
	if err := f.assertRoutes(); err != nil {
		t.Fatal("partition precondition: bypass-free routes not proven", err)
	}
	f.partition(oldLeader, true)
	defer func() {
		for i := range f.nodes {
			f.partition(i, false)
		}
		if !r3Wait(f.ctx, 12*time.Second, func() bool { return f.assertRoutes() == nil }) {
			t.Error("partition cleanup failed to restore all routed links")
		}
	}()
	f.leader(t, majorityNode, oldLeader)
	current := f.advance(t, majorityNode)
	isolated := f.connect(t, oldLeader, "gateway-a", nil)
	response, err := r3Request(isolated, "gateway-a", "auth.witness.gateway-a", []byte(`{"version":1}`), r3Headers(before.AuthoritySequence()), 800*time.Millisecond)
	if err == nil && response != nil {
		if _, err = parseAck(response.Data, before.AuthoritySequence()); err == nil {
			t.Fatal("isolated former leader acknowledged stale guarded witness after majority advance")
		}
	}
	isolated.Close()
	gateway := f.client(t, oldLeader, "gateway-a")
	proof, err := gateway.Verify(f.ctx, r3Attempt(t), before.Record().Document())
	if err == nil || proof.verified {
		t.Fatal("isolated former leader renewed old authority")
	}
	// A captured stale GET is injected explicitly; the fresh real majority
	// witness must reject it. This is not labeled a naturally stale GET.
	proxy, err := newR3ClientProxy(f, majorityNode)
	if err != nil {
		t.Fatal("stale-read proxy creation failed")
	}
	defer func() {
		if !proxy.stop() {
			t.Error("stale-read proxy did not join")
		}
	}()
	proxy.arm("stale_get", messageGetSubject, captured.Data)
	config := f.config(majorityNode, "gateway-a")
	config.URL = "tls://" + proxy.listener.Addr().String()
	client, err := NewClient(config)
	if err != nil {
		t.Fatal(err)
	}
	defer r3CloseClient(t, client)
	proof, err = client.Verify(f.ctx, r3Attempt(t), before.Record().Document())
	proxy.mu.Lock()
	used, requests := proxy.fault.used, proxy.requests
	proxy.mu.Unlock()
	if err != ErrConflict || proof.verified || !used || requests < 3 {
		t.Fatal("captured stale GET substituted for a fresh guarded witness", err)
	}
	f.partition(oldLeader, false)
	if !r3Wait(f.ctx, 12*time.Second, func() bool { return f.assertRoutes() == nil }) {
		t.Fatal("former leader did not rejoin routed cluster")
	}
	// Cut every route. Send guarded writes directly, so a failed preliminary
	// stream-info read cannot accidentally stand in for the quorum test.
	for i := range f.nodes {
		f.partition(i, true)
	}
	for i := range f.nodes {
		connection := f.connect(t, i, "gateway-a", nil)
		response, err := r3Request(connection, "gateway-a", "auth.witness.gateway-a", []byte(`{"version":1}`), r3Headers(current.AuthoritySequence()), 500*time.Millisecond)
		connection.Close()
		if err == nil && response != nil {
			if _, err = parseAck(response.Data, current.AuthoritySequence()); err == nil {
				t.Fatal("guarded witness succeeded without quorum")
			}
		}
	}
	for i := range f.nodes {
		f.partition(i, false)
	}
	if !r3Wait(f.ctx, 12*time.Second, func() bool { return f.assertRoutes() == nil }) {
		t.Fatal("quorum-loss recovery did not restore routed cluster")
	}
	f.leader(t, 0, -1)
	healthy := f.client(t, 0, "gateway-b")
	attempt := r3Attempt(t)
	proof, err = healthy.Verify(f.ctx, attempt, current.Record().Document())
	if err != nil || !proof.ValidFor(f.scope, attempt, current.Record().Document(), time.Now()) {
		t.Fatal("guarded verification failed after quorum recovery", err)
	}
}

func r3ConcurrentAdvanceWitness(t *testing.T, f *r3Fixture) {
	control := f.client(t, 0, "control")
	a := f.client(t, 1, "gateway-a")
	b := f.client(t, 2, "gateway-b")
	advances := 0
	witnesses := 0
	for round := 0; round < 8; round++ {
		prior := f.read(t, 0)
		target := r3Document(t, prior.Record().Document().Revision()+1)
		attempts := [3]Attempt{r3Attempt(t), r3Attempt(t), r3Attempt(t)}
		type raceResult struct {
			role     int
			mutation MutationResult
			proof    VerifiedObservation
			err      error
			returned time.Time
		}
		results := make(chan raceResult, 3)
		start := make(chan struct{})
		go func() {
			<-start
			r, e := control.Advance(f.ctx, attempts[0], prior.Record().Document().Revision(), target)
			results <- raceResult{role: 0, mutation: r, err: e, returned: time.Now()}
		}()
		go func() {
			<-start
			r, e := a.Verify(f.ctx, attempts[1], prior.Record().Document())
			results <- raceResult{role: 1, proof: r, err: e, returned: time.Now()}
		}()
		go func() {
			<-start
			r, e := b.Verify(f.ctx, attempts[2], prior.Record().Document())
			results <- raceResult{role: 2, proof: r, err: e, returned: time.Now()}
		}()
		close(start)
		var observed [3]raceResult
		for range 3 {
			select {
			case r := <-results:
				observed[r.role] = r
			case <-f.ctx.Done():
				t.Fatal("concurrent operations did not return before fixture deadline")
			}
		}
		for _, r := range observed {
			if r.err != nil && r.err != ErrConflict {
				t.Fatal("healthy concurrent operation had an unexplained outcome", r.err)
			}
		}
		for _, r := range observed[1:] {
			if r.err == nil {
				witnesses++
				if !r.proof.ValidFor(f.scope, attempts[r.role], prior.Record().Document(), r.returned) {
					t.Fatal("concurrent witness returned an invalid or expired binding")
				}
			}
		}
		after := f.read(t, 0)
		if observed[0].err == nil {
			advances++
			sequence := observed[0].mutation.Snapshot().AuthoritySequence()
			if observed[0].mutation.Outcome() != OutcomeAdvanced || sequence <= prior.AuthoritySequence() || !observed[0].mutation.Snapshot().Record().Document().Equal(target) || !after.Record().Document().Equal(target) {
				t.Fatal("concurrent controller success did not advance actual authority")
			}
			for _, r := range observed[1:] {
				if r.err == nil && r.proof.WitnessSequence() >= sequence {
					t.Fatal("old witness crossed committed authority advance")
				}
			}
		} else if !after.Record().Equal(prior.Record()) {
			t.Fatal("conflicting controller operation changed authority")
		}
	}
	if advances == 0 {
		t.Fatal("bounded contention prevented every controller advance")
	}
	if witnesses == 0 {
		t.Fatal("bounded contention prevented every gateway witness")
	}
	t.Logf("R3_CONCURRENCY rounds=8 controller_advances=%d gateway_witnesses=%d", advances, witnesses)
}

func r3ResponseCorrelationAndUnknown(t *testing.T, f *r3Fixture) {
	proxy, err := newR3ClientProxy(f, 0)
	if err != nil {
		t.Fatal("ACK proxy creation failed")
	}
	defer func() {
		if !proxy.stop() {
			t.Error("ACK proxy did not join")
		}
	}()
	config := f.config(0, "control")
	config.URL = "tls://" + proxy.listener.Addr().String()
	for _, kind := range []string{"drop", "delay", "wrong_subject", "retired_inbox", "duplicate"} {
		control, err := NewClient(config)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { r3CloseClient(t, control) })
		prior := f.read(t, 0)
		target := r3Document(t, prior.Record().Document().Revision()+1)
		proxy.arm(kind, AuthoritySubject, nil)
		result, err := control.Advance(f.ctx, r3Attempt(t), prior.Record().Document().Revision(), target)
		r3CloseClient(t, control)
		proxy.mu.Lock()
		used := proxy.fault.used
		proxy.mu.Unlock()
		if !used || err != ErrUnknown || result.Outcome() != OutcomeUnknown {
			t.Fatal("post-send response fault did not remain unknown", kind, err)
		}
		// Observe actual storage with another connection after the proxy has
		// seen and faulted the real committed ACK, then verify using a witness.
		if !f.read(t, 0).Record().Document().Equal(target) {
			t.Fatal("ACK fault never reached actual committed storage", kind)
		}
		check := f.client(t, 0, "control")
		verified, err := check.Check(f.ctx, r3Attempt(t), target)
		r3CloseClient(t, check)
		if err != nil || verified.Outcome() != OutcomeCurrentVerified {
			t.Fatal("unknown outcome could not be independently verified", kind, err)
		}
	}
	proxy.mu.Lock()
	invalid, requests, distinct := proxy.invalid, proxy.requests, len(proxy.inboxes)
	proxy.mu.Unlock()
	if invalid || requests < 15 || requests != distinct {
		t.Fatal("requests did not use fresh literal inboxes")
	}
	t.Logf("R3_CORRELATION requests=%d unique_inboxes=%d response_faults=5", requests, distinct)
}

func r3RetentionAndACLPressure(t *testing.T, f *r3Fixture) {
	before := f.read(t, 0)
	denials := make(chan error, 64)
	connection := f.connect(t, 0, "gateway-a", func(_ *nats.Conn, _ *nats.Subscription, err error) {
		select {
		case denials <- err:
		default:
		}
	})
	for _, subject := range []string{AuthoritySubject, "auth.witness.gateway-b", streamCreateSubject, "$JS.API.STREAM.UPDATE." + StreamName, "$JS.API.STREAM.DELETE." + StreamName, "$JS.API.STREAM.PURGE." + StreamName, "$JS.API.STREAM.MSG.DELETE." + StreamName, "$JS.API.CONSUMER.CREATE." + StreamName, "_INBOX.kelvo-authfence.gateway-a.test", "_INBOX.kelvo-authfence.gateway-b.test"} {
		if err := connection.Publish(subject, []byte("{}")); err != nil {
			t.Fatal("negative ACL publication could not be sent")
		}
		if err := connection.FlushTimeout(time.Second); err != nil {
			t.Fatal("negative ACL publication did not reach broker")
		}
		select {
		case err := <-denials:
			if !errors.Is(err, nats.ErrPermissionViolation) || !strings.Contains(err.Error(), subject) {
				t.Fatal("unexpected ACL denial correlation")
			}
		case <-time.After(time.Second):
			t.Fatal("forbidden publication not denied")
		}
	}
	for _, subject := range []string{AuthoritySubject, "auth.witness.gateway-a", "_INBOX.kelvo-authfence.gateway-b.*", ">"} {
		sub, err := connection.SubscribeSync(subject)
		if err != nil {
			t.Fatal("negative ACL subscription could not be sent")
		}
		_ = connection.FlushTimeout(time.Second)
		select {
		case err := <-denials:
			if !errors.Is(err, nats.ErrPermissionViolation) || !strings.Contains(err.Error(), subject) {
				t.Fatal("unexpected subscription denial correlation")
			}
		case <-time.After(time.Second):
			t.Fatal("forbidden subscription not denied")
		}
		_ = sub.Unsubscribe()
	}
	for i := 0; i < 64; i++ {
		response, err := r3Request(connection, "gateway-a", "auth.witness.gateway-a", []byte(`{"version":1}`), r3Headers(before.AuthoritySequence()), time.Second)
		if err != nil || response == nil {
			t.Fatal("bounded witness pressure failed")
		}
		if _, err = parseAck(response.Data, before.AuthoritySequence()); err != nil {
			t.Fatal("bounded witness pressure did not commit", err)
		}
	}
	rollup := r3Headers(before.AuthoritySequence())
	rollup.Set("Nats-Rollup", "all")
	response, err := r3Request(connection, "gateway-a", "auth.witness.gateway-a", []byte(`{"version":1}`), rollup, time.Second)
	var rejection struct {
		Error    *apiError `json:"error"`
		Sequence uint64    `json:"seq"`
	}
	if err != nil || response == nil || len(response.Header) != 0 || json.Unmarshal(response.Data, &rejection) != nil || rejection.Error == nil || rejection.Error.Code < 400 || rejection.Error.ErrCode <= 0 || rejection.Sequence != 0 {
		t.Fatal("disabled rollup did not produce a definite correlated broker rejection")
	}
	var info struct {
		Type  string    `json:"type"`
		Error *apiError `json:"error"`
		State *struct {
			Messages uint64 `json:"messages"`
			Bytes    uint64 `json:"bytes"`
		} `json:"state"`
	}
	if json.Unmarshal(f.streamInfo(t, 0), &info) != nil || info.Type != "io.nats.jetstream.api.v1.stream_info_response" || info.Error != nil || info.State == nil || info.State.Messages == 0 || info.State.Bytes == 0 || info.State.Messages > uint64(len(f.scope.Gateways())+2) || info.State.Bytes > 2<<20 {
		t.Fatal("retained stream data exceeded configured logical bounds")
	}
	outsider := f.connect(t, 0, "outsider", nil)
	if err = outsider.Publish(AuthoritySubject, []byte("foreign-account-input")); err != nil || outsider.FlushTimeout(time.Second) != nil {
		t.Fatal("account-isolation probe could not be sent")
	}
	if !f.read(t, 0).Record().Equal(before.Record()) {
		t.Fatal("authority was evicted or modified by pressure/ACL/account probes")
	}
	t.Logf("R3_RETENTION writes=64 retained_messages=%d retained_bytes=%d publish_denials=10 subscription_denials=4", info.State.Messages, info.State.Bytes)
}
