// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package operations

import (
	"crypto/ed25519"
	"crypto/rand"
	"reflect"
	"strings"
	"testing"
	"time"
)

func uploadGrantFixture(t *testing.T) (InputUploadClaims, GrantTrust, ed25519.PrivateKey) {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().Unix()
	c := InputUploadClaims{Version: InputUploadVersion, Issuer: "api", Audience: "operations", ClusterTenant: "cluster-a", ServicePrincipal: "api",
		AppTeam: "customer-a", Subject: Subject{Kind: "api_key", ID: "key-a"}, ID: "upload-1", IssuedAt: now, ExpiresAt: now + 60,
		ConnectionID: "saved-a", Format: "ingestion_batch_v1", SHA256: strings.Repeat("a", 64), Bytes: 256}
	return c, GrantTrust{Issuer: c.Issuer, Audience: c.Audience, ClusterTenant: c.ClusterTenant, ServicePrincipal: c.ServicePrincipal, PublicKey: pub}, key
}

func TestInputUploadGrantBoundedAndDomainSeparated(t *testing.T) {
	c, trust, key := uploadGrantFixture(t)
	token, err := SignInputUploadGrant(c, key)
	if err != nil {
		t.Fatal(err)
	}
	got, err := VerifyInputUploadGrant(token, trust, time.Now())
	if err != nil || !reflect.DeepEqual(got, c) {
		t.Fatal("valid scoped upload failed", err)
	}
	if _, err := VerifyGrantClaims(token, trust, time.Now()); err == nil {
		t.Fatal("upload token authorized an operation")
	}
	op := GrantClaims{Version: GrantVersion, Issuer: c.Issuer, Audience: c.Audience, ClusterTenant: c.ClusterTenant, ServicePrincipal: c.ServicePrincipal,
		AppTeam: c.AppTeam, Subject: c.Subject, ID: c.ID, IssuedAt: c.IssuedAt, ExpiresAt: c.ExpiresAt, ConnectionID: c.ConnectionID,
		Operation: StatementExecute, RequestSHA256: c.SHA256, Authorization: Authorization{Kind: "api_key"}}
	operationToken, err := SignGrant(op, key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyInputUploadGrant(operationToken, trust, time.Now()); err == nil {
		t.Fatal("operation token authorized upload")
	}
	for _, changed := range []GrantTrust{{Issuer: "other", Audience: trust.Audience, ClusterTenant: trust.ClusterTenant, ServicePrincipal: trust.ServicePrincipal, PublicKey: trust.PublicKey},
		{Issuer: trust.Issuer, Audience: "other", ClusterTenant: trust.ClusterTenant, ServicePrincipal: trust.ServicePrincipal, PublicKey: trust.PublicKey},
		{Issuer: trust.Issuer, Audience: trust.Audience, ClusterTenant: "other", ServicePrincipal: trust.ServicePrincipal, PublicKey: trust.PublicKey},
		{Issuer: trust.Issuer, Audience: trust.Audience, ClusterTenant: trust.ClusterTenant, ServicePrincipal: "other", PublicKey: trust.PublicKey}} {
		if _, err := VerifyInputUploadGrant(token, changed, time.Now()); err == nil {
			t.Fatal("foreign verifier scope accepted")
		}
	}
	if _, err := VerifyInputUploadGrant(token, trust, time.Unix(c.ExpiresAt, 0)); err == nil {
		t.Fatal("expired upload accepted")
	}
}

func TestInputUploadGrantRejectsUnboundedOrUnscopedClaims(t *testing.T) {
	valid, _, key := uploadGrantFixture(t)
	for name, mutate := range map[string]func(*InputUploadClaims){
		"empty": func(c *InputUploadClaims) { c.Bytes = 0 }, "oversize": func(c *InputUploadClaims) { c.Bytes = MaxSealedInputBytes + 1 },
		"unknown-format": func(c *InputUploadClaims) { c.Format = "native_input_v1" }, "bad-digest": func(c *InputUploadClaims) { c.SHA256 = "missing" },
		"unbounded-time": func(c *InputUploadClaims) { c.ExpiresAt = c.IssuedAt + 301 }, "no-tenant": func(c *InputUploadClaims) { c.AppTeam = "" },
		"wrong-version": func(c *InputUploadClaims) { c.Version++ }, "future": func(c *InputUploadClaims) { c.ExpiresAt = c.IssuedAt },
		"read-job": func(c *InputUploadClaims) {
			c.Subject = Subject{Kind: "job", ID: "scheduler", JobID: "job-a", JobExpiresAt: c.ExpiresAt, JobConnections: map[string]string{c.ConnectionID: "read"}}
		},
		"other-job-connection": func(c *InputUploadClaims) {
			c.Subject = Subject{Kind: "job", ID: "scheduler", JobID: "job-a", JobExpiresAt: c.ExpiresAt, JobConnections: map[string]string{"other": "write"}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			c := valid
			mutate(&c)
			if _, err := SignInputUploadGrant(c, key); err == nil {
				t.Fatal("invalid upload authority signed")
			}
		})
	}
	valid.Subject = Subject{Kind: "job", ID: "scheduler", JobID: "job-a", JobExpiresAt: valid.ExpiresAt, JobConnections: map[string]string{valid.ConnectionID: "write"}}
	if _, err := SignInputUploadGrant(valid, key); err != nil {
		t.Fatal("scoped scheduled upload rejected", err)
	}
}

func TestInputUploadGrantMapsFormatToExactOperation(t *testing.T) {
	claims, trust, key := uploadGrantFixture(t)
	for format, expected := range map[string]Kind{"ingestion_batch_v1": IngestionCommit, "watch_checkpoint_v1": WatchAck} {
		claims.Format = format
		token, err := SignInputUploadGrant(claims, key)
		if err != nil {
			t.Fatal(err)
		}
		verified, err := VerifyInputUploadGrant(token, trust, time.Now())
		if err != nil || verified.Format != format || SealedInputOperation(verified.Format) != expected {
			t.Fatal("sealed format lost its operation binding", err)
		}
	}
	if SealedInputOperation("operation_request_v1") != "" || SealedInputOperation("native_input_v1") != "" {
		t.Fatal("unsupported data format became publicly uploadable")
	}
}

func TestMongoResumeUploadOnlyAuthorizesInstall(t *testing.T) {
	if SealedInputOperation("mongo_watch_resume_v1") != WatchInstall {
		t.Fatal("resume upload did not require install authority")
	}
	request := Request{Version: Version, Kind: WatchInstall, Connection: ConnectionRef{ID: "saved", Database: "app", Schema: "app"}, IdempotencyKey: "import-1", Spec: Spec{Watch: &WatchSpec{ID: "watcher", Generation: "generation", Mode: "native", Target: ObjectRef{Name: "orders"}, Resume: &InputRef{ID: "sealed", SHA256: strings.Repeat("a", 64), Bytes: 100, Format: "mongo_watch_resume_v1"}}}}
	if err := request.Validate(); err != nil {
		t.Fatal(err)
	}
	if ref := request.InputReference(); ref == nil || ref.Format != "mongo_watch_resume_v1" {
		t.Fatal("resume not sealed")
	}
	request.Spec.Watch.Mode = "poll"
	if request.Validate() == nil {
		t.Fatal("poll watcher silently accepted Mongo resume")
	}
	request.Spec.Watch.Mode = "native"
	request.Spec.Watch.Resume.Bytes = 16<<10 + 1
	if request.Validate() == nil {
		t.Fatal("oversized resume accepted")
	}
}
