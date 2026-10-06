// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package operations

import (
	"bytes"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"
)

func statementFixture() Request {
	return Request{Version: Version, Kind: StatementExecute, Connection: ConnectionRef{ID: "saved-connection", Database: "app"}, IdempotencyKey: "change-1", Spec: Spec{Statement: &StatementSpec{SQL: "UPDATE accounts SET balance = $1 WHERE id = $2", Parameters: []Parameter{{Type: "decimal128", Value: json.RawMessage(`"9007199254740993.01"`)}, {Type: "uint64", Value: json.RawMessage(`"18446744073709551615"`)}}, Transaction: TransactionRequired}}}
}

func TestRequestStrictDecodingAndTypePrecision(t *testing.T) {
	r := statementFixture()
	encoded, err := Encode(r)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseRequest(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if string(parsed.Spec.Statement.Parameters[0].Value) != `"9007199254740993.01"` || string(parsed.Spec.Statement.Parameters[1].Value) != `"18446744073709551615"` {
		t.Fatal("typed values changed")
	}
	for _, raw := range []string{
		strings.Replace(string(encoded), `"version":1`, `"version":1,"Version":1`, 1),
		strings.Replace(string(encoded), `"spec":`, `"source_url":"https://untrusted.invalid","spec":`, 1),
		string(encoded) + ` {}`,
		strings.Replace(string(encoded), `"statement":`, `"query":{"sql":"SELECT 1"},"statement":`, 1),
		`null`,
	} {
		if _, err := ParseRequest([]byte(raw)); err == nil {
			t.Fatalf("accepted invalid control envelope: %.80s", raw)
		}
	}
	if _, err := ParseRequest(append(encoded, 0xff)); err == nil {
		t.Fatal("invalid UTF-8 accepted")
	}
}

func TestParameterValidationPreservesExplicitTypes(t *testing.T) {
	valid := []Parameter{{"null", json.RawMessage(`null`)}, {"int8", json.RawMessage(`-128`)}, {"uint64", json.RawMessage(`18446744073709551615`)}, {"binary", json.RawMessage(`"AAEC/w=="`)}, {"timestamp", json.RawMessage(`"2026-10-06T12:00:00.123456789+05:30"`)}, {"json", json.RawMessage(`{"n":9007199254740993}`)}}
	for _, p := range valid {
		if err := p.Validate(); err != nil {
			t.Fatalf("%s: %v", p.Type, err)
		}
	}
	invalid := []Parameter{{"int8", json.RawMessage(`128`)}, {"uint64", json.RawMessage(`18446744073709551616`)}, {"int64", json.RawMessage(`null`)}, {"string", json.RawMessage(`1`)}, {"float64", json.RawMessage(`1e999`)}, {"decimal128", json.RawMessage(`9007199254740993.01`)}, {"binary", json.RawMessage(`"AAE"`)}, {"date", json.RawMessage(`"2026-02-30"`)}, {"json", json.RawMessage(`{"a":1,"a":2}`)}}
	for _, p := range invalid {
		if p.Validate() == nil {
			t.Fatalf("accepted invalid %s", p.Type)
		}
	}
}

func TestOperationDigestBindsAllExecutionInputs(t *testing.T) {
	original := statementFixture()
	expected, err := Digest(original)
	if err != nil {
		t.Fatal(err)
	}
	changes := map[string]func(*Request){
		"database":    func(r *Request) { r.Connection.Database = "other" },
		"connection":  func(r *Request) { r.Connection.ID = "other" },
		"sql":         func(r *Request) { r.Spec.Statement.SQL += " " },
		"parameter":   func(r *Request) { r.Spec.Statement.Parameters[0].Value = json.RawMessage(`"9007199254740993.02"`) },
		"role":        func(r *Request) { r.Spec.Statement.Role = "report_writer" },
		"transaction": func(r *Request) { r.Spec.Statement.Transaction = TransactionAutocommit },
		"approval":    func(r *Request) { r.ApprovalID = "approval-2" },
		"idempotency": func(r *Request) { r.IdempotencyKey = "change-2" },
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			r, err := Clone(original)
			if err != nil {
				t.Fatal(err)
			}
			change(&r)
			got, err := Digest(r)
			if err != nil || got == expected {
				t.Fatalf("digest did not bind %s: %v", name, err)
			}
		})
	}
	raw, err := Encode(original)
	if err != nil {
		t.Fatal(err)
	}
	ref, body, err := SealRequest(original, "request-1")
	if err != nil || !bytes.Equal(raw, body) || ref.Bytes != int64(len(raw)) || ref.Format != "operation_request_v1" {
		t.Fatal("invalid sealed request binding", err)
	}
	if _, _, err := SealRequest(original, "/tmp/request"); err == nil {
		t.Fatal("path accepted as input identity")
	}
}

func TestOperationDigestGoldenVectors(t *testing.T) {
	raw, err := os.ReadFile("testdata/request-digests.json")
	if err != nil {
		t.Fatal(err)
	}
	var vectors []struct {
		Name      string `json:"name"`
		Canonical string `json:"canonical_json"`
		SHA256    string `json:"sha256"`
	}
	if err = json.Unmarshal(raw, &vectors); err != nil {
		t.Fatal(err)
	}
	for _, vector := range vectors {
		t.Run(vector.Name, func(t *testing.T) {
			r, err := ParseRequest([]byte(vector.Canonical))
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := Encode(r)
			if err != nil || string(encoded) != vector.Canonical {
				t.Fatal("canonical encoding changed", err)
			}
			digest, err := Digest(r)
			if err != nil || digest != vector.SHA256 {
				t.Fatal("request digest contract changed", digest, err)
			}
		})
	}
}

func TestOperationLimitsDoNotNarrowExistingSQLLength(t *testing.T) {
	r := statementFixture()
	r.Spec.Statement.SQL = strings.Repeat(" ", 99999) + "X"
	if r.Validate() != nil {
		t.Fatal("100000 byte SQL refused")
	}
	r.Spec.Statement.SQL += "X"
	if r.Validate() == nil {
		t.Fatal("oversized SQL accepted")
	}
	r = statementFixture()
	r.IdempotencyKey = ""
	if r.Validate() == nil {
		t.Fatal("mutation without deduplication key accepted")
	}
	r = statementFixture()
	r.Spec.Statement.Role = "writer\nSET ROLE admin"
	if r.Validate() == nil {
		t.Fatal("role control characters accepted")
	}
}

func TestEveryOperationHasOneBoundedSpec(t *testing.T) {
	input := InputRef{ID: "sealed-1", SHA256: strings.Repeat("a", 64), Bytes: 128, Format: "native_input_v1"}
	base := Request{Version: Version, Connection: ConnectionRef{ID: "saved", Database: "app"}, IdempotencyKey: "operation-1"}
	for _, kind := range []Kind{ConnectionTest, MetadataInspect, QueryRead, StatementExecute, NativeRead, NativeExecute, SchemaApply, MigrationApply, IngestionInstall, IngestionState, IngestionCommit, WatchInstall, WatchRemove, WatchRead, WatchAck} {
		t.Run(string(kind), func(t *testing.T) {
			r := base
			r.Kind = kind
			switch kind {
			case MetadataInspect:
				r.Spec.Metadata = &MetadataSpec{Object: "table_summary", Target: ObjectRef{Name: "events"}, Limit: 10}
			case QueryRead:
				r.Spec.Query = &QuerySpec{SQL: "SELECT 1"}
			case StatementExecute:
				r.Spec.Statement = &StatementSpec{SQL: "DELETE FROM events", Transaction: TransactionRequired}
			case NativeRead, NativeExecute:
				r.Spec.Native = &NativeSpec{Provider: "mongodb", Command: "find"}
			case SchemaApply:
				v := input
				v.Format = "schema_plan_v1"
				r.Spec.Schema = &SchemaSpec{Target: ObjectRef{Name: "events"}, ExpectedSHA256: strings.Repeat("b", 64), Plan: v, Transaction: TransactionRequired}
			case MigrationApply:
				v := input
				v.Format = "migration_plan_v1"
				r.Spec.Migration = &MigrationSpec{ID: "migration-1", Plan: v, Transaction: TransactionAutocommit}
			case IngestionInstall, IngestionState, IngestionCommit:
				r.Spec.Ingestion = &IngestionSpec{Scope: IngestionScope{SourceID: "source", Stream: "stream", Binding: strings.Repeat("c", 64)}}
				if kind == IngestionCommit {
					v := input
					v.Format = "ingestion_batch_v1"
					r.Spec.Ingestion.BatchID = "batch-1"
					r.Spec.Ingestion.Input = &v
				}
			case WatchInstall, WatchRemove, WatchRead, WatchAck:
				r.Spec.Watch = &WatchSpec{ID: "watch-1", Generation: "generation-1", Target: ObjectRef{Name: "events"}, Mode: "native"}
				if kind == WatchRead {
					r.Spec.Watch.MaxEvents = 10
					r.Spec.Watch.MaxWaitMS = 100
				}
				if kind == WatchAck {
					v := input
					v.Format = "watch_checkpoint_v1"
					r.Spec.Watch.Checkpoint = &v
					r.Spec.Watch.SinkReceiptSHA256 = strings.Repeat("d", 64)
				}
			}
			if err := r.Validate(); err != nil {
				t.Fatal(err)
			}
			r.Spec.Query = &QuerySpec{SQL: "SELECT 2"}
			if kind != QueryRead && r.Validate() == nil {
				t.Fatal("two variants accepted")
			}
		})
	}
}

func TestReceiptDoesNotConflateCommitAndFailure(t *testing.T) {
	base := Receipt{Version: Version, OperationID: "operation-1", RequestSHA256: strings.Repeat("a", 64)}
	for _, pair := range []struct {
		o    Outcome
		e    Effect
		code string
	}{{Completed, EffectCommitted, ""}, {Rejected, EffectNone, "PERMISSION_DENIED"}, {CancelledBeforeStart, EffectNone, "CANCELLED"}, {Failed, EffectPartial, "SOURCE_FAILED"}, {OutcomeUnknown, EffectUnknown, "OUTCOME_UNKNOWN"}} {
		r := base
		r.Outcome, r.Effect, r.ErrorCode = pair.o, pair.e, pair.code
		if r.Validate() != nil {
			t.Fatalf("valid receipt refused: %+v", pair)
		}
	}
	for _, pair := range []struct {
		o    Outcome
		e    Effect
		code string
	}{{Completed, EffectUnknown, ""}, {OutcomeUnknown, EffectCommitted, "OUTCOME_UNKNOWN"}, {Rejected, EffectPartial, "SOURCE_FAILED"}, {Failed, EffectNone, "private driver password"}} {
		r := base
		r.Outcome, r.Effect, r.ErrorCode = pair.o, pair.e, pair.code
		if r.Validate() == nil {
			t.Fatalf("invalid receipt accepted: %+v", pair)
		}
	}
	r := base
	r.Outcome, r.Effect = Completed, EffectCommitted
	negative := int64(-1)
	r.AffectedRows = &negative
	if r.Validate() == nil {
		t.Fatal("negative affected rows accepted")
	}
}

func TestCapabilitiesAreExplicitAndBounded(t *testing.T) {
	r := statementFixture()
	c := Capabilities{Version: Version, Engine: "postgres", Operations: []Capability{{Kind: StatementExecute, ParameterTypes: []string{"decimal128", "uint64"}, Transactions: []TransactionMode{TransactionRequired}, Idempotency: "none", Cancellation: "best_effort"}}}
	if c.Supports(r) != nil {
		t.Fatal("declared capability refused")
	}
	r.Spec.Statement.Transaction = TransactionAutocommit
	if !errors.Is(c.Supports(r), ErrUnsupported) {
		t.Fatal("unsupported transaction accepted")
	}
	c.Operations[0].Idempotency = "provider"
	if c.Validate() == nil {
		t.Fatal("provider deduplication without retention accepted")
	}
	c.Operations[0].IdempotencyRetentionSeconds = 86400
	if c.Validate() != nil {
		t.Fatal("explicit provider retention refused")
	}
	c.Operations = append(c.Operations, c.Operations[0])
	if c.Validate() == nil {
		t.Fatal("duplicate operation declarations accepted")
	}
}

func TestPublicResponsePinsHandleRequestAndTerminalReceipt(t *testing.T) {
	digest := strings.Repeat("a", 64)
	r := Response{Version: Version, ID: "operation-1", RequestSHA256: digest, State: "queued"}
	if r.ValidateBinding("", digest) != nil {
		t.Fatal("initial handle refused")
	}
	if r.ValidateBinding("operation-2", digest) == nil || r.ValidateBinding("operation-1", strings.Repeat("b", 64)) == nil {
		t.Fatal("foreign handle/request accepted")
	}
	r.State = string(Completed)
	if r.Validate() == nil {
		t.Fatal("terminal state without receipt accepted")
	}
	r.Receipt = &Receipt{Version: Version, OperationID: r.ID, RequestSHA256: digest, Outcome: Completed, Effect: EffectCommitted}
	if r.ValidateBinding(r.ID, digest) != nil {
		t.Fatal("bound terminal receipt refused")
	}
	r.State = "running"
	if r.Validate() == nil {
		t.Fatal("active state with receipt accepted")
	}
	r.State = "cancel_requested"
	r.Receipt = nil
	if r.Validate() == nil {
		t.Fatal("unknown response state accepted")
	}
}

func grantFixture(t *testing.T, r Request) (GrantClaims, GrantTrust, ed25519.PrivateKey, time.Time) {
	t.Helper()
	now := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	key := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{17}, ed25519.SeedSize))
	digest, err := Digest(r)
	if err != nil {
		t.Fatal(err)
	}
	c := GrantClaims{Version: GrantVersion, Issuer: "app", Audience: "kelvo", ClusterTenant: "shared", ServicePrincipal: "gateway", AppTeam: "team-a", Subject: Subject{Kind: "api_key", ID: "key-a"}, ID: "grant-1", IssuedAt: now.Unix(), ExpiresAt: now.Add(time.Minute).Unix(), ConnectionID: r.Connection.ID, Operation: r.Kind, RequestSHA256: digest, Authorization: Authorization{Kind: "api_key"}}
	trust := GrantTrust{Issuer: c.Issuer, Audience: c.Audience, ClusterTenant: c.ClusterTenant, ServicePrincipal: c.ServicePrincipal, PublicKey: key.Public().(ed25519.PublicKey)}
	return c, trust, key, now
}

func TestV2GrantsBindOperationAndUpstreamAuthority(t *testing.T) {
	r := statementFixture()
	c, trust, key, now := grantFixture(t, r)
	token, err := SignGrant(c, key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyGrant(token, trust, r, now); err != nil {
		t.Fatal(err)
	}
	changed, _ := Clone(r)
	changed.Spec.Statement.Role = "other"
	if _, err := VerifyGrant(token, trust, changed, now); err == nil {
		t.Fatal("unapproved role accepted")
	}
	if _, err := VerifyGrant(token, trust, r, now.Add(time.Minute)); err == nil {
		t.Fatal("expired grant accepted")
	}
	wrong := trust
	wrong.ClusterTenant = "other"
	if _, err := VerifyGrant(token, wrong, r, now); err == nil {
		t.Fatal("cross-tenant grant accepted")
	}
	c.Authorization = Authorization{Kind: "read"}
	if _, err := SignGrant(c, key); err == nil {
		t.Fatal("read authority signed a mutation")
	}
	c.Authorization = Authorization{Kind: "trusted_app"}
	if _, err := SignGrant(c, key); err == nil {
		t.Fatal("API key impersonated trusted application user")
	}
}

func TestV2ApprovedChangeAndJobScopes(t *testing.T) {
	r := statementFixture()
	r.ApprovalID = "change-approved"
	c, trust, key, now := grantFixture(t, r)
	c.Subject = Subject{Kind: "user", ID: "user-a"}
	c.Authorization = Authorization{Kind: "approved_change", ApprovalID: r.ApprovalID, ApprovedSHA256: c.RequestSHA256}
	token, err := SignGrant(c, key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyGrant(token, trust, r, now); err != nil {
		t.Fatal(err)
	}
	c.Authorization.ApprovedSHA256 = strings.Repeat("0", 64)
	if _, err := SignGrant(c, key); err == nil {
		t.Fatal("mismatched approval hash accepted")
	}
	r = statementFixture()
	c, trust, key, now = grantFixture(t, r)
	c.Subject = Subject{Kind: "job", ID: "job-principal", JobID: "job-1", JobExpiresAt: now.Add(time.Hour).Unix(), JobConnections: map[string]string{r.Connection.ID: "read"}}
	c.Authorization = Authorization{Kind: "job"}
	if _, err := SignGrant(c, key); err == nil {
		t.Fatal("read-only job authorized mutation")
	}
	c.Subject.JobConnections[r.Connection.ID] = "write"
	token, err = SignGrant(c, key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyGrant(token, trust, r, now); err != nil {
		t.Fatal(err)
	}
	c.Subject.JobConnections = map[string]string{"another-connection": "write"}
	if _, err := SignGrant(c, key); err == nil {
		t.Fatal("unselected job connection accepted")
	}
}
