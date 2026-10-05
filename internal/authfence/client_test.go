// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package authfence

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

type protocolSession struct {
	call    func(string, []byte, nats.Header) ([]byte, error)
	onClose func() error
	closed  atomic.Int32
}

func (s *protocolSession) request(subject string, data []byte, headers nats.Header) ([]byte, error) {
	return s.call(subject, data, headers)
}
func (s *protocolSession) close() error {
	s.closed.Add(1)
	if s.onClose != nil {
		return s.onClose()
	}
	return nil
}

func protocolConfig(t *testing.T, replica string) Config {
	t.Helper()
	scope, err := NewScope("fleet", []string{"tenant"}, []string{"east", "west"})
	if err != nil {
		t.Fatal(err)
	}
	return Config{URL: "tls://127.0.0.1:4222", CAFile: "/missing/authority-ca.pem", Username: "fixture", PasswordEnv: "KELVO_FENCE_FIXTURE_PASSWORD", Scope: scope, ReplicaID: replica}
}
func protocolDocument(t *testing.T, revision uint64) Document {
	t.Helper()
	document, err := NewDocument(revision, strings.Repeat("a", 64))
	if err != nil {
		t.Fatal(err)
	}
	return document
}
func protocolAttempt(t *testing.T) Attempt {
	t.Helper()
	now := time.Now()
	a, err := NewAttempt(now, now.Add(MaxAttemptDuration))
	if err != nil {
		t.Fatal(err)
	}
	return a
}
func protocolBytes(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
func protocolInfo(t *testing.T, scope Scope, created bool) []byte {
	kind := "io.nats.jetstream.api.v1.stream_info_response"
	if created {
		kind = "io.nats.jetstream.api.v1.stream_create_response"
	}
	config := expectedConfig(scope)
	config.Metadata = map[string]string{"_nats.ver": "2.15.0", "_nats.level": "5", "_nats.req.level": "0"}
	return protocolBytes(t, map[string]any{"type": kind, "config": config, "state": map[string]int{"consumer_count": 0}})
}
func protocolGet(t *testing.T, scope Scope, document Document, sequence uint64) []byte {
	record, err := NewRecord(scope, document)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := EncodeRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	return protocolBytes(t, map[string]any{"type": "io.nats.jetstream.api.v1.stream_msg_get_response", "message": map[string]any{"subject": AuthoritySubject, "seq": sequence, "data": raw}})
}

func TestFenceClientConstructorAndRolesAreInert(t *testing.T) {
	config := protocolConfig(t, "east")
	client, err := NewClient(config)
	if err != nil {
		t.Fatal(err)
	}
	client.open = func(*operation, Config) (wireSession, error) {
		t.Error("role refusal opened resources")
		return nil, ErrUnavailable
	}
	if _, err = client.Initialize(context.Background(), protocolAttempt(t), protocolDocument(t, 1)); !errors.Is(err, ErrDenied) {
		t.Fatal(err)
	}
	if _, err = client.Check(context.Background(), protocolAttempt(t), protocolDocument(t, 1)); !errors.Is(err, ErrDenied) {
		t.Fatal(err)
	}
	if _, err = client.Advance(context.Background(), protocolAttempt(t), 1, protocolDocument(t, 2)); !errors.Is(err, ErrDenied) {
		t.Fatal(err)
	}
	control, _ := NewClient(protocolConfig(t, ControlReplicaID))
	control.open = client.open
	if _, err = control.Verify(context.Background(), protocolAttempt(t), protocolDocument(t, 1)); !errors.Is(err, ErrDenied) {
		t.Fatal(err)
	}
	if err = control.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err = client.Read(context.Background(), protocolAttempt(t)); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	for _, change := range []func(*Config){
		func(c *Config) { c.URL = "nats://localhost:4222" }, func(c *Config) { c.URL = "tls://user:secret@localhost:4222" },
		func(c *Config) { c.URL = "tls://localhost:4222/extra" }, func(c *Config) { c.ReplicaID = "unknown" },
		func(c *Config) { c.CredentialsFile = "/private/creds" }, func(c *Config) { c.KeyFile = "/private/key" },
		func(c *Config) { c.URL = "tls://localhost:0" }, func(c *Config) { c.URL = "tls://localhost:65536" },
		func(c *Config) { c.URL = "tls://localhost:4222?" }, func(c *Config) { c.URL = "tls://localhost:4222#" },
	} {
		bad := config
		change(&bad)
		if _, err := NewClient(bad); !errors.Is(err, ErrInvalid) {
			t.Fatal("invalid config accepted")
		}
	}
}

func TestFenceCompletedResultSurvivesInternalCleanupCancellation(t *testing.T) {
	config := protocolConfig(t, "east")
	client, _ := NewClient(config)
	document := protocolDocument(t, 1)
	info, record := protocolInfo(t, config.Scope, false), protocolGet(t, config.Scope, document, 2)
	var opens atomic.Int32
	client.open = func(*operation, Config) (wireSession, error) {
		opens.Add(1)
		return &protocolSession{call: func(subject string, _ []byte, _ nats.Header) ([]byte, error) {
			if subject == streamInfoSubject {
				return info, nil
			}
			return record, nil
		}}, nil
	}
	for range 256 {
		snapshot, err := client.Read(context.Background(), protocolAttempt(t))
		if err != nil || !snapshot.Valid() {
			t.Fatal("cleanup cancellation lost completed result", err)
		}
	}
	if opens.Load() != 256 {
		t.Fatal("operation retried or skipped")
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestFenceStreamCreateThenReadFailureIsUnknown(t *testing.T) {
	config := protocolConfig(t, ControlReplicaID)
	client, _ := NewClient(config)
	creates, publishes := 0, 0
	client.open = func(*operation, Config) (wireSession, error) {
		return &protocolSession{call: func(subject string, _ []byte, _ nats.Header) ([]byte, error) {
			switch subject {
			case streamInfoSubject:
				return []byte(`{"type":"io.nats.jetstream.api.v1.stream_info_response","error":{"code":404,"err_code":10059}}`), nil
			case streamCreateSubject:
				creates++
				return protocolInfo(t, config.Scope, true), nil
			case messageGetSubject:
				return nil, errors.New("private failed read")
			case AuthoritySubject:
				publishes++
				return nil, ErrUnavailable
			default:
				t.Error("unexpected request")
				return nil, ErrUnavailable
			}
		}}, nil
	}
	result, err := client.Initialize(context.Background(), protocolAttempt(t), protocolDocument(t, 1))
	if !errors.Is(err, ErrUnknown) || result.Outcome() != OutcomeUnknown || creates != 1 || publishes != 0 {
		t.Fatal(result, err, creates, publishes)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestFenceCleanupUncertaintyPermanentlyFencesClient(t *testing.T) {
	config := protocolConfig(t, "east")
	client, _ := NewClient(config)
	document := protocolDocument(t, 1)
	var opens atomic.Int32
	client.open = func(*operation, Config) (wireSession, error) {
		opens.Add(1)
		return &protocolSession{
			call: func(subject string, _ []byte, _ nats.Header) ([]byte, error) {
				if subject == streamInfoSubject {
					return protocolInfo(t, config.Scope, false), nil
				}
				return protocolGet(t, config.Scope, document, 2), nil
			},
			onClose: func() error { return errors.New("private transport cleanup detail") },
		}, nil
	}
	if snapshot, err := client.Read(context.Background(), protocolAttempt(t)); !errors.Is(err, ErrUnknown) || snapshot.Valid() {
		t.Fatal("uncertain cleanup returned a usable snapshot", err)
	}
	if _, err := client.Read(context.Background(), protocolAttempt(t)); !errors.Is(err, ErrClosed) {
		t.Fatal("replacement admitted after uncertain cleanup", err)
	}
	if _, err := client.Verify(context.Background(), protocolAttempt(t), document); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if err := client.Close(context.Background()); !errors.Is(err, ErrUnknown) {
		t.Fatal("sticky uncertainty lost", err)
	}
	if opens.Load() != 1 {
		t.Fatal("replacement generation opened")
	}
	protocolWait(t, client.Quiesced())
}

func TestFenceVerifyRequiresFreshGuardedWitness(t *testing.T) {
	config := protocolConfig(t, "east")
	document := protocolDocument(t, 3)
	client, _ := NewClient(config)
	calls := 0
	session := &protocolSession{call: func(subject string, data []byte, headers nats.Header) ([]byte, error) {
		calls++
		switch calls {
		case 1:
			if subject != streamInfoSubject {
				t.Error(subject)
			}
			return protocolInfo(t, config.Scope, false), nil
		case 2:
			if subject != messageGetSubject || string(data) != `{"last_by_subj":"auth.current"}` {
				t.Error("non-exact GET")
			}
			return protocolGet(t, config.Scope, document, 17), nil
		case 3:
			if subject != "auth.witness.east" || len(data) > MaxWitnessBytes || len(headers) != 3 || headers.Get(expectStreamHeader) != StreamName || headers.Get(expectSubjectHeader) != AuthoritySubject || headers.Get(expectSequenceHeader) != "17" || headers.Get("Nats-Msg-Id") != "" {
				t.Error("wrong witness guard")
			}
			return []byte(`{"stream":"KELVO_AUTHORITY","seq":18}`), nil
		default:
			t.Error("unexpected retry")
			return nil, ErrUnavailable
		}
	}}
	client.open = func(*operation, Config) (wireSession, error) { return session, nil }
	attempt := protocolAttempt(t)
	verified, err := client.Verify(context.Background(), attempt, document)
	if err != nil || !verified.ValidFor(config.Scope, attempt, document, time.Now()) || calls != 3 || session.closed.Load() != 1 {
		t.Fatalf("verification: %v %d", err, calls)
	}
	other, _ := NewScope("another", []string{"tenant"}, []string{"east", "west"})
	if verified.ValidFor(other, attempt, document, time.Now()) {
		t.Fatal("scope substitution")
	}
	if err = client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestFenceCancelledMutationRetainsOperationAndRejectsLateAck(t *testing.T) {
	config := protocolConfig(t, "east")
	document := protocolDocument(t, 3)
	client, _ := NewClient(config)
	entered, release := make(chan struct{}), make(chan struct{})
	session := &protocolSession{call: func(subject string, _ []byte, _ nats.Header) ([]byte, error) {
		switch subject {
		case streamInfoSubject:
			return protocolInfo(t, config.Scope, false), nil
		case messageGetSubject:
			return protocolGet(t, config.Scope, document, 17), nil
		default:
			close(entered)
			<-release
			return []byte(`{"stream":"KELVO_AUTHORITY","seq":18}`), nil
		}
	}}
	client.open = func(*operation, Config) (wireSession, error) { return session, nil }
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		v, err := client.Verify(ctx, protocolAttempt(t), document)
		if v.verified {
			t.Error("late observation returned")
		}
		result <- err
	}()
	<-entered
	cancel()
	if err := <-result; !errors.Is(err, ErrUnknown) {
		t.Fatal(err)
	}
	if _, err := client.Read(context.Background(), protocolAttempt(t)); !errors.Is(err, ErrBusy) {
		t.Fatal("timed-out slot reused", err)
	}
	closedCtx, stop := context.WithCancel(context.Background())
	stop()
	if err := client.Close(closedCtx); !errors.Is(err, ErrUnknown) {
		t.Fatal(err)
	}
	select {
	case <-client.Quiesced():
		t.Fatal("custody ended before blocked request")
	default:
	}
	close(release)
	select {
	case <-client.Quiesced():
	case <-time.After(time.Second):
		t.Fatal("cleanup did not finish")
	}
	if session.closed.Load() != 1 {
		t.Fatal("connection was not closed exactly once")
	}
}

func TestFenceLatePartialOpenAndCloseRemainOwned(t *testing.T) {
	client, _ := NewClient(protocolConfig(t, "east"))
	entered, release, closing, finish := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	session := &protocolSession{onClose: func() error { close(closing); <-finish; return errors.New("private fixture detail") }}
	client.open = func(*operation, Config) (wireSession, error) {
		close(entered)
		<-release
		return session, errors.New("private connection detail")
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { _, err := client.Read(ctx, protocolAttempt(t)); result <- err }()
	<-entered
	cancel()
	if err := <-result; !errors.Is(err, ErrExpired) {
		t.Fatal(err)
	}
	close(release)
	<-closing
	if _, err := client.Read(context.Background(), protocolAttempt(t)); !errors.Is(err, ErrBusy) {
		t.Fatal(err)
	}
	close(finish)
	if err := client.Close(context.Background()); !errors.Is(err, ErrUnknown) {
		t.Fatal("cleanup uncertainty lost", err)
	}
	<-client.Quiesced()
}

func TestFenceControllerCreateAdvanceAndUncertainOutcomes(t *testing.T) {
	for _, mode := range []string{"initialize", "existing", "advance", "wrong-revision", "lost-ack", "conflict"} {
		t.Run(mode, func(t *testing.T) {
			config := protocolConfig(t, ControlReplicaID)
			client, _ := NewClient(config)
			document := protocolDocument(t, 4)
			publishes := 0
			session := &protocolSession{call: func(subject string, data []byte, header nats.Header) ([]byte, error) {
				switch subject {
				case streamInfoSubject:
					if mode == "initialize" {
						return []byte(`{"type":"io.nats.jetstream.api.v1.stream_info_response","error":{"code":404,"err_code":10059}}`), nil
					}
					return protocolInfo(t, config.Scope, false), nil
				case streamCreateSubject:
					if !validStreamConfig(data, config.Scope, false) {
						t.Error("wrong stream config")
					}
					return protocolInfo(t, config.Scope, true), nil
				case messageGetSubject:
					if mode == "initialize" {
						return []byte(`{"type":"io.nats.jetstream.api.v1.stream_msg_get_response","error":{"code":404,"err_code":10037}}`), nil
					}
					return protocolGet(t, config.Scope, protocolDocument(t, 3), 10), nil
				case AuthoritySubject:
					publishes++
					expected := "10"
					if mode == "initialize" {
						expected = "0"
					}
					if header.Get(expectSequenceHeader) != expected || header.Get(expectSubjectHeader) != AuthoritySubject {
						t.Error("unconditional mutation")
					}
					if mode == "lost-ack" {
						return nil, errors.New("private transport detail")
					}
					if mode == "conflict" {
						return []byte(`{"error":{"code":400,"err_code":10071,"description":"private revision"}}`), nil
					}
					return []byte(`{"stream":"KELVO_AUTHORITY","seq":11}`), nil
				default:
					t.Error(subject)
					return nil, ErrUnavailable
				}
			}}
			client.open = func(*operation, Config) (wireSession, error) { return session, nil }
			var result MutationResult
			var err error
			if mode == "initialize" || mode == "existing" {
				result, err = client.Initialize(context.Background(), protocolAttempt(t), document)
			} else {
				expected := uint64(3)
				if mode == "wrong-revision" {
					expected = 2
				}
				result, err = client.Advance(context.Background(), protocolAttempt(t), expected, document)
			}
			switch mode {
			case "initialize":
				if err != nil || result.Outcome() != OutcomeCreated || publishes != 1 {
					t.Fatal(result, err, publishes)
				}
			case "advance":
				if err != nil || result.Outcome() != OutcomeAdvanced || publishes != 1 {
					t.Fatal(result, err, publishes)
				}
			case "existing", "wrong-revision":
				if !errors.Is(err, ErrConflict) || publishes != 0 {
					t.Fatal(err, publishes)
				}
			case "lost-ack":
				if !errors.Is(err, ErrUnknown) || result.Outcome() != OutcomeUnknown || publishes != 1 {
					t.Fatal(result, err, publishes)
				}
			case "conflict":
				if !errors.Is(err, ErrConflict) || result.Outcome() != OutcomeConflict || publishes != 1 {
					t.Fatal(result, err, publishes)
				}
			}
			if err := client.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
		})
	}
}
