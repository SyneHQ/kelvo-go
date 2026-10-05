// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package authfence

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func protocolMap(t *testing.T, value any) map[string]any {
	t.Helper()
	var result map[string]any
	if err := json.Unmarshal(protocolBytes(t, value), &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func protocolServerConfig(scope Scope, version string) streamConfig {
	config := expectedConfig(scope)
	level := "5"
	if version == "2.14.7" {
		level = "4"
	}
	config.Metadata = map[string]string{"_nats.ver": version, "_nats.level": level, "_nats.req.level": "0"}
	return config
}

func TestFenceJSONRejectsDuplicatesAndBudgets(t *testing.T) {
	for _, raw := range []string{
		`{"seq":1,"seq":2}`, `{"seq":1,"SEQ":2}`, `{"outer":{"code":400,"code":403}}`,
		`{"outer":[{"seq":1,"seq":2}]}`, `{"seq":`, `{} {}`, `[}`, "",
		strings.Repeat("[", 18) + "0" + strings.Repeat("]", 18),
		"[" + strings.Repeat("0,", 12000) + "0]", strings.Repeat(" ", maxResponseBytes+1),
	} {
		var target any
		if err := decodeJSON([]byte(raw), &target, false); !errors.Is(err, ErrUnavailable) {
			t.Fatal("invalid JSON accepted")
		}
	}
	var target struct {
		Sequence uint64 `json:"seq"`
	}
	if err := decodeJSON([]byte(`{"seq":12,"future":true}`), &target, true); !errors.Is(err, ErrUnavailable) {
		t.Fatal("unknown field accepted")
	}
	if err := decodeJSON([]byte(`{"seq":12}`), &target, true); err != nil || target.Sequence != 12 {
		t.Fatal(err)
	}
}

func TestFenceAckOnlyAcceptsUnambiguousFreshPlainPublish(t *testing.T) {
	for _, raw := range []string{
		`{"stream":"KELVO_AUTHORITY","seq":11}`,
		`{"stream":"KELVO_AUTHORITY","seq":11,"duplicate":false,"domain":""}`,
	} {
		if sequence, err := parseAck([]byte(raw), 10); err != nil || sequence != 11 {
			t.Fatal(sequence, err)
		}
	}
	for _, raw := range []string{
		`{"stream":"OTHER","seq":11}`, `{"stream":"KELVO_AUTHORITY","seq":10}`,
		`{"stream":"KELVO_AUTHORITY","seq":0}`, `{"stream":"KELVO_AUTHORITY","seq":-1}`,
		`{"stream":"KELVO_AUTHORITY","seq":"11"}`, `{"stream":"KELVO_AUTHORITY","seq":18446744073709551616}`,
		`{"stream":"KELVO_AUTHORITY","seq":11,"duplicate":true}`,
		`{"stream":"KELVO_AUTHORITY","seq":11,"duplicate":null}`,
		`{"stream":"KELVO_AUTHORITY","seq":11,"Duplicate":null}`,
		`{"stream":"KELVO_AUTHORITY","seq":11,"domain":null}`,
		`{"stream":"KELVO_AUTHORITY","seq":11,"domain":"another"}`,
		`{"stream":"KELVO_AUTHORITY","seq":11,"error":null}`,
		`{"stream":"KELVO_AUTHORITY","seq":11,"batch":"x"}`,
		`{"stream":"KELVO_AUTHORITY","seq":11,"count":1}`,
		`{"stream":"KELVO_AUTHORITY","seq":11,"val":"42"}`,
		`{"stream":"KELVO_AUTHORITY","seq":11,"error":{"code":400,"err_code":10071}}`,
		`{"stream":"OTHER","seq":0,"error":{"code":400,"err_code":10071}}`,
		`{"error":{"code":503,"err_code":10008,"description":"private server detail"}}`,
		`{"error":{"code":400,"err_code":99999}}`,
		`{"stream":"KELVO_AUTHORITY","seq":11,"seq":12}`, `null`,
	} {
		if sequence, err := parseAck([]byte(raw), 10); !errors.Is(err, ErrUnknown) || sequence != 0 {
			t.Fatal("uncertain ACK accepted", sequence, err)
		}
	}
	for _, code := range []int{10071, 10164, 10058} {
		raw := protocolBytes(t, map[string]any{"stream": StreamName, "seq": 0, "error": map[string]int{"code": 400, "err_code": code}})
		if _, err := parseAck(raw, 10); !errors.Is(err, ErrConflict) {
			t.Fatal(err)
		}
	}
	if _, err := parseAck([]byte(`{"error":{"code":403,"err_code":10039}}`), 10); !errors.Is(err, ErrDenied) {
		t.Fatal(err)
	}
}

func TestFenceStreamRequiresPinnedPlainConfiguration(t *testing.T) {
	scope := protocolConfig(t, "east").Scope
	for _, version := range []string{"2.14.7", "2.15.0"} {
		if !validStreamConfig(protocolBytes(t, protocolServerConfig(scope, version)), scope, true) {
			t.Fatal("pinned server config rejected")
		}
	}
	if !validStreamConfig(protocolBytes(t, expectedConfig(scope)), scope, false) {
		t.Fatal("create config rejected")
	}
	if validStreamConfig(protocolBytes(t, expectedConfig(scope)), scope, true) {
		t.Fatal("missing server metadata accepted")
	}
	for field, value := range map[string]any{
		"name": "OTHER", "subjects": []string{AuthoritySubject, "auth.witness.*"}, "retention": "workqueue",
		"max_consumers": 1, "max_msgs": 1, "max_bytes": -1, "max_age": 1, "max_msgs_per_subject": 2,
		"max_msg_size": -1, "discard": "old", "storage": "memory", "num_replicas": 1, "no_ack": true,
		"duplicate_window": 0, "compression": "s2", "first_seq": 100, "placement": map[string]any{},
		"mirror": map[string]any{}, "sources": []any{}, "subject_transform": map[string]any{}, "republish": map[string]any{},
		"allow_direct": true, "mirror_direct": true, "discard_new_per_subject": true, "sealed": true,
		"deny_delete": false, "deny_purge": false, "allow_rollup_hdrs": true,
		"consumer_limits": map[string]any{"max_ack_pending": 1}, "allow_msg_ttl": true,
		"subject_delete_marker_ttl": 1, "allow_msg_counter": true, "allow_atomic": true,
		"allow_msg_schedules": true, "persist_mode": "async", "allow_batched": true, "future_setting": false,
	} {
		t.Run(field, func(t *testing.T) {
			config := protocolMap(t, protocolServerConfig(scope, "2.15.0"))
			config[field] = value
			if validStreamConfig(protocolBytes(t, config), scope, true) {
				t.Fatal("unsupported stream option accepted")
			}
		})
	}
	for _, field := range []string{"subjects", "storage", "allow_direct", "max_age", "consumer_limits", "persist_mode"} {
		config := protocolMap(t, protocolServerConfig(scope, "2.15.0"))
		config[field] = nil
		if validStreamConfig(protocolBytes(t, config), scope, true) {
			t.Fatal("null scalar accepted")
		}
	}
	config := protocolMap(t, protocolServerConfig(scope, "2.15.0"))
	config["consumer_limits"] = map[string]any{"max_ack_pending": nil}
	if validStreamConfig(protocolBytes(t, config), scope, true) {
		t.Fatal("null nested scalar accepted")
	}
	for _, metadata := range []map[string]string{
		{"_nats.ver": "2.15.0", "_nats.level": "4", "_nats.req.level": "0"},
		{"_nats.ver": "2.14.7", "_nats.level": "5", "_nats.req.level": "0"},
		{"_nats.ver": "2.15.0", "_nats.level": "5", "_nats.req.level": "2"},
		{"_nats.ver": "2.15.0", "_nats.level": "5", "_nats.req.level": "0", "custom": "value"},
	} {
		config := protocolServerConfig(scope, "2.15.0")
		config.Metadata = metadata
		if validStreamConfig(protocolBytes(t, config), scope, true) {
			t.Fatal("unrecognized metadata accepted")
		}
	}
	config = protocolMap(t, protocolServerConfig(scope, "2.15.0"))
	delete(config, "persist_mode")
	if !validStreamConfig(protocolBytes(t, config), scope, true) {
		t.Fatal("omitted default persistence rejected")
	}
}

func TestFenceStreamResponseRequiresExactOperationAndState(t *testing.T) {
	scope := protocolConfig(t, "east").Scope
	if err := streamResponse(protocolInfo(t, scope, false), scope, streamInfoType); err != nil {
		t.Fatal(err)
	}
	if err := streamResponse(protocolInfo(t, scope, true), scope, streamInfoType); !errors.Is(err, ErrUnavailable) {
		t.Fatal("create reply substituted for info")
	}
	for _, state := range []any{nil, map[string]any{}, map[string]any{"consumer_count": nil}, map[string]any{"consumer_count": 1}} {
		raw := protocolBytes(t, map[string]any{"type": streamInfoType, "config": protocolServerConfig(scope, "2.15.0"), "state": state})
		if err := streamResponse(raw, scope, streamInfoType); !errors.Is(err, ErrUnavailable) {
			t.Fatal("missing or active consumer state accepted")
		}
	}
	for _, kind := range []string{"", streamCreateType} {
		raw := protocolBytes(t, map[string]any{"type": kind, "error": map[string]int{"code": 404, "err_code": 10059}})
		if err := streamResponse(raw, scope, streamInfoType); !errors.Is(err, ErrUnavailable) {
			t.Fatal("foreign error envelope accepted")
		}
	}
	raw := protocolBytes(t, map[string]any{"type": streamInfoType, "error": map[string]int{"code": 404, "err_code": 10059}})
	if err := streamResponse(raw, scope, streamInfoType); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	raw = protocolBytes(t, map[string]any{"type": streamInfoType, "error": map[string]int{"code": 404, "err_code": 10059}, "config": protocolServerConfig(scope, "2.15.0")})
	if err := streamResponse(raw, scope, streamInfoType); !errors.Is(err, ErrUnavailable) {
		t.Fatal("mixed stream response accepted")
	}
}

func TestFenceReadRejectsForeignMalformedAndMixedRecords(t *testing.T) {
	config := protocolConfig(t, "east")
	document := protocolDocument(t, 1)
	other, _ := NewScope("other", []string{"tenant"}, []string{"east", "west"})
	for _, mode := range []string{"scope", "subject", "sequence", "record", "type", "mixed-error"} {
		t.Run(mode, func(t *testing.T) {
			body := protocolGet(t, config.Scope, document, 7)
			if mode == "scope" {
				body = protocolGet(t, other, document, 7)
			}
			var response map[string]any
			if err := json.Unmarshal(body, &response); err != nil {
				t.Fatal(err)
			}
			message := response["message"].(map[string]any)
			switch mode {
			case "subject":
				message["subject"] = "auth.witness.east"
			case "sequence":
				message["seq"] = 0
			case "record":
				message["data"] = []byte("not canonical YAML")
			case "type":
				response["type"] = streamInfoType
			case "mixed-error":
				response["error"] = map[string]int{"code": 404, "err_code": 10037}
			}
			body = protocolBytes(t, response)
			client, _ := NewClient(config)
			client.open = func(*operation, Config) (wireSession, error) {
				return &protocolSession{call: func(subject string, _ []byte, _ nats.Header) ([]byte, error) {
					if subject == streamInfoSubject {
						return protocolInfo(t, config.Scope, false), nil
					}
					return body, nil
				}}, nil
			}
			if snapshot, err := client.Read(context.Background(), protocolAttempt(t)); !errors.Is(err, ErrUnavailable) || snapshot.Valid() {
				t.Fatal("invalid record accepted", err)
			}
			if err := client.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestFenceReadDoesNotWitnessAndCheckMustWitness(t *testing.T) {
	config := protocolConfig(t, ControlReplicaID)
	document := protocolDocument(t, 3)
	for _, mode := range []string{"read", "check", "mismatch", "missing-ack"} {
		t.Run(mode, func(t *testing.T) {
			client, _ := NewClient(config)
			witnesses := 0
			client.open = func(*operation, Config) (wireSession, error) {
				return &protocolSession{call: func(subject string, _ []byte, headers nats.Header) ([]byte, error) {
					switch subject {
					case streamInfoSubject:
						return protocolInfo(t, config.Scope, false), nil
					case messageGetSubject:
						return protocolGet(t, config.Scope, document, 22), nil
					case "auth.witness.control":
						witnesses++
						if headers.Get(expectSequenceHeader) != "22" || headers.Get(expectSubjectHeader) != AuthoritySubject {
							t.Error("check missing witness guard")
						}
						if mode == "missing-ack" {
							return nil, ErrUnavailable
						}
						return []byte(`{"stream":"KELVO_AUTHORITY","seq":23}`), nil
					default:
						t.Error("unexpected subject")
						return nil, ErrUnavailable
					}
				}}, nil
			}
			attempt := protocolAttempt(t)
			if mode == "read" {
				snapshot, err := client.Read(context.Background(), attempt)
				if err != nil || !snapshot.Valid() || witnesses != 0 {
					t.Fatal(err, witnesses)
				}
				if (VerifiedObservation{snapshot: snapshot}).ValidFor(config.Scope, attempt, document, time.Now()) {
					t.Fatal("GET-only read became verified")
				}
			} else {
				expected := document
				if mode == "mismatch" {
					expected = protocolDocument(t, 4)
				}
				result, err := client.Check(context.Background(), attempt, expected)
				switch mode {
				case "check":
					if err != nil || result.Outcome() != OutcomeCurrentVerified || witnesses != 1 {
						t.Fatal(result, err, witnesses)
					}
				case "mismatch":
					if !errors.Is(err, ErrConflict) || result.Outcome() == OutcomeCurrentVerified || witnesses != 0 {
						t.Fatal(result, err, witnesses)
					}
				case "missing-ack":
					if !errors.Is(err, ErrUnknown) || result.Outcome() != OutcomeUnknown || witnesses != 1 {
						t.Fatal(result, err, witnesses)
					}
				}
			}
			if err := client.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
		})
	}
}
