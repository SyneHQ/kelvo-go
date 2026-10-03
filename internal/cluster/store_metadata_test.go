// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type metadataFixture struct {
	jetstream.JetStream
	t                 *testing.T
	ctx               context.Context
	calls             []string
	lookups           int
	openErr           error
	createBucketErr   error
	readErr           error
	reopenErr         error
	createValueErr    error
	directErr         error
	allowDirect       bool
	value             []byte
	wantConfiguration jetstream.KeyValueConfig
}

func (f *metadataFixture) called(ctx context.Context, name string) {
	f.t.Helper()
	if ctx != f.ctx {
		f.t.Fatal("metadata operation replaced its existing context")
	}
	f.calls = append(f.calls, name)
}

func (f *metadataFixture) KeyValue(ctx context.Context, bucket string) (jetstream.KeyValue, error) {
	f.t.Helper()
	if bucket != metaBucket {
		f.t.Fatal("unexpected metadata bucket")
	}
	f.lookups++
	if f.lookups == 1 {
		f.called(ctx, "bucket_open")
		return metadataFixtureKV{f: f}, f.openErr
	}
	f.called(ctx, "bucket_reopen")
	return metadataFixtureKV{f: f}, f.reopenErr
}

func (f *metadataFixture) CreateKeyValue(ctx context.Context, cfg jetstream.KeyValueConfig) (jetstream.KeyValue, error) {
	f.t.Helper()
	f.called(ctx, "bucket_create")
	if !reflect.DeepEqual(cfg, f.wantConfiguration) {
		f.t.Fatal("metadata provisioning configuration changed")
	}
	return metadataFixtureKV{f: f}, f.createBucketErr
}

func (f *metadataFixture) Stream(ctx context.Context, name string) (jetstream.Stream, error) {
	f.t.Helper()
	f.called(ctx, "direct_check")
	if name != "KV_"+metaBucket {
		f.t.Fatal("unexpected direct-access check")
	}
	return startupStream{info: &jetstream.StreamInfo{Config: jetstream.StreamConfig{AllowDirect: f.allowDirect}}}, f.directErr
}

func (f *metadataFixture) UpdateStream(ctx context.Context, cfg jetstream.StreamConfig) (jetstream.Stream, error) {
	f.t.Helper()
	f.called(ctx, "direct_update")
	if cfg.AllowDirect {
		f.t.Fatal("metadata update retained direct reads")
	}
	return startupStream{info: &jetstream.StreamInfo{Config: cfg}}, nil
}

type metadataFixtureKV struct {
	jetstream.KeyValue
	f *metadataFixture
}

func (s metadataFixtureKV) Get(ctx context.Context, key string) (jetstream.KeyValueEntry, error) {
	s.f.t.Helper()
	s.f.called(ctx, "value_read")
	if key != metadataKey {
		s.f.t.Fatal("unexpected metadata key")
	}
	return quotaEntry{raw: s.f.value}, s.f.readErr
}

func (s metadataFixtureKV) Create(ctx context.Context, key string, value []byte, opts ...jetstream.KVCreateOpt) (uint64, error) {
	s.f.t.Helper()
	s.f.called(ctx, "value_create")
	if key != metadataKey || len(opts) != 0 || string(value) != string(s.f.value) {
		s.f.t.Fatal("metadata creation semantics changed")
	}
	return 1, s.f.createValueErr
}

func newMetadataFixture(t *testing.T) (*metadataFixture, Policy) {
	t.Helper()
	p := Policy{TenantID: "fixture", Replicas: 3}
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	return &metadataFixture{t: t, ctx: context.Background(), value: raw,
		wantConfiguration: jetstream.KeyValueConfig{Bucket: metaBucket, History: 1, MaxBytes: 1 << 20, Replicas: p.Replicas}}, p
}

func readMetadataDiagnostic(t *testing.T, err error) metadataDiagnostic {
	t.Helper()
	raw, ok := StoreMetadataDiagnostic(err)
	if !ok || len(raw) > 192 || strings.ContainsAny(raw, "\n\r") {
		t.Fatal("missing or unbounded diagnostic")
	}
	var d metadataDiagnostic
	if json.Unmarshal([]byte(raw), &d) != nil {
		t.Fatal("invalid diagnostic JSON")
	}
	return d
}

func TestStoreMetadataFailurePhasesPreserveOperations(t *testing.T) {
	for _, phase := range []string{"bucket_create", "value_read", "bucket_reopen", "value_create"} {
		t.Run(phase, func(t *testing.T) {
			f, p := newMetadataFixture(t)
			cause := &jetstream.APIError{Code: 503, ErrorCode: 10005, Description: "private provider text token=fixture-secret"}
			var want []string
			switch phase {
			case "bucket_create":
				f.openErr, f.createBucketErr = jetstream.ErrBucketNotFound, cause
				want = []string{"bucket_open", "bucket_create"}
			case "value_read":
				f.readErr = cause
				want = []string{"bucket_open", "value_read"}
			case "bucket_reopen":
				f.reopenErr = cause
				want = []string{"bucket_open", "value_read", "direct_check", "bucket_reopen"}
			case "value_create":
				f.openErr, f.readErr, f.createValueErr = jetstream.ErrBucketNotFound, jetstream.ErrKeyNotFound, cause
				want = []string{"bucket_open", "bucket_create", "value_read", "direct_check", "bucket_reopen", "value_create"}
			}
			meta, err := openClusterMetadata(f.ctx, f, p, true)
			if meta != nil || err == nil || err.Error() != "cluster metadata unavailable" {
				t.Fatal("metadata error behavior changed")
			}
			if !reflect.DeepEqual(f.calls, want) {
				t.Fatalf("metadata calls = %v, want %v", f.calls, want)
			}
			d := readMetadataDiagnostic(t, err)
			if d != (metadataDiagnostic{Schema: 1, Phase: phase, Class: "api_error", APIStatus: 503, APICode: 10005}) {
				t.Fatalf("unexpected safe diagnostic: %+v", d)
			}
			if errors.Is(err, cause) || errors.Unwrap(err) != nil || strings.Contains(fmt.Sprintf("%+v %#v", err, err), "fixture-secret") {
				t.Fatal("unsafe cause retained or error matching changed")
			}
		})
	}
}

func TestStoreMetadataSuccessAndRefusalsPreserveOperations(t *testing.T) {
	for _, name := range []string{"existing_read_only", "missing_bucket", "missing_value", "mismatch", "direct_forbidden", "direct_lookup_failure", "initialize_existing", "initialize_new"} {
		t.Run(name, func(t *testing.T) {
			f, p := newMetadataFixture(t)
			initialize := false
			message := ""
			want := []string{"bucket_open", "direct_check", "value_read"}
			switch name {
			case "missing_bucket":
				f.openErr, message = jetstream.ErrBucketNotFound, "cluster metadata is missing"
				want = []string{"bucket_open"}
			case "missing_value":
				f.readErr, message = jetstream.ErrKeyNotFound, "cluster metadata is missing"
			case "mismatch":
				f.value, message = []byte("different policy"), "cluster metadata mismatch"
			case "direct_forbidden":
				f.allowDirect, message = true, "KV store direct access is forbidden"
				want = []string{"bucket_open", "direct_check"}
			case "direct_lookup_failure":
				f.directErr, message = errors.New("private provider reply"), "KV store configuration unavailable"
				want = []string{"bucket_open", "direct_check"}
			case "initialize_existing":
				initialize, f.allowDirect = true, true
				want = []string{"bucket_open", "value_read", "direct_check", "direct_update", "bucket_reopen"}
			case "initialize_new":
				initialize, f.openErr, f.readErr = true, jetstream.ErrBucketNotFound, jetstream.ErrKeyNotFound
				want = []string{"bucket_open", "bucket_create", "value_read", "direct_check", "bucket_reopen", "value_create"}
			}
			meta, err := openClusterMetadata(f.ctx, f, p, initialize)
			if (err != nil) != (message != "") || (meta != nil) != (message == "") || (err != nil && err.Error() != message) {
				t.Fatal("metadata success or refusal changed")
			}
			if !reflect.DeepEqual(f.calls, want) {
				t.Fatalf("metadata calls = %v, want %v", f.calls, want)
			}
			if _, ok := StoreMetadataDiagnostic(err); ok {
				t.Fatal("unrelated error acquired a metadata-unavailable diagnostic")
			}
		})
	}
}

type metadataUnreadableError struct{}

func (metadataUnreadableError) Error() string { panic("diagnostics must not inspect error text") }

func TestStoreMetadataDiagnosticClassifiesWithoutReadingText(t *testing.T) {
	var nilAPI *jetstream.APIError
	for _, tc := range []struct {
		name  string
		err   error
		class string
	}{
		{"cancel", context.Canceled, "context_canceled"},
		{"deadline", context.DeadlineExceeded, "deadline_exceeded"},
		{"timeout", nats.ErrTimeout, "timeout"},
		{"responders", nats.ErrNoResponders, "no_responders"},
		{"permission", nats.ErrPermissionViolation, "permission_denied"},
		{"authorization", nats.ErrAuthorization, "authorization"},
		{"closed", nats.ErrConnectionClosed, "connection_closed"},
		{"wrapped", fmt.Errorf("private wrapper: %w", nats.ErrTimeout), "timeout"},
		{"unknown", errors.New("nats: timeout token=private"), "other"},
		{"unreadable", metadataUnreadableError{}, "other"},
		{"nil_api", nilAPI, "other"},
		{"nil", nil, "other"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := metadataUnavailable(metadataValueRead, tc.err)
			d := readMetadataDiagnostic(t, err)
			if d.Class != tc.class || d.APIStatus != 0 || d.APICode != 0 {
				t.Fatal("unexpected safe classification", d)
			}
			if err.Error() != "cluster metadata unavailable" || errors.Unwrap(err) != nil {
				t.Fatal("terminal error changed or cause retained")
			}
		})
	}
}

func TestStoreMetadataDiagnosticBoundsAndRejectsTextSpoofing(t *testing.T) {
	for _, status := range []int{-1, 0, 200, 399, 400, 503, 599, 600, 1 << 30} {
		for _, code := range []jetstream.ErrorCode{0, 10005, 65535} {
			cause := &jetstream.APIError{Code: status, ErrorCode: code, Description: strings.Repeat("private\n", 1000)}
			err := metadataUnavailable(metadataPhase(255), fmt.Errorf("secret wrapper: %w", cause))
			d := readMetadataDiagnostic(t, err)
			wantStatus := 0
			if status >= 400 && status <= 599 {
				wantStatus = status
			}
			if d != (metadataDiagnostic{Schema: 1, Phase: "unknown", Class: "api_error", APIStatus: wantStatus, APICode: uint16(code)}) {
				t.Fatal("unbounded or unexpected API diagnostic", d)
			}
			raw, _ := StoreMetadataDiagnostic(err)
			var fields map[string]json.RawMessage
			if json.Unmarshal([]byte(raw), &fields) != nil || len(fields) != 5 || strings.Contains(raw, "private") || strings.Contains(raw, "secret") {
				t.Fatal("diagnostic exposed arbitrary text or fields")
			}
			for _, key := range []string{"schema", "phase", "class", "api_status", "api_code"} {
				if fields[key] == nil {
					t.Fatal("diagnostic schema changed")
				}
			}
		}
	}
	var typedNil *metadataUnavailableError
	for _, err := range []error{nil, typedNil, errors.New("cluster metadata unavailable"), errors.New("KELVO_CLUSTER_METADATA private")} {
		if raw, ok := StoreMetadataDiagnostic(err); ok || raw != "" {
			t.Fatal("unrelated error text was recognized as a typed diagnostic")
		}
	}
}
