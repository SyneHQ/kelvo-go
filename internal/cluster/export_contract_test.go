// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
)

func TestExportSubmissionAuthorityLimitsAndCodec(t *testing.T) {
	for name, change := range map[string]func(*ExportSubmission){
		"native": func(in *ExportSubmission) {
			in.Request = query.Request{Mode: "native", ConnectionID: "sales_native", SQL: "SELECT 1"}
		},
		"ungranted source":         func(in *ExportSubmission) { in.Request.Sources = []string{"payroll"} },
		"unknown codec":            func(in *ExportSubmission) { in.Compression = "gzip" },
		"unconfigured compression": func(in *ExportSubmission) { in.Compression = "lz4_frame" },
		"oversized retention":      func(in *ExportSubmission) { in.TTL = 31 * 24 * time.Hour },
		"negative retention":       func(in *ExportSubmission) { in.TTL = -time.Second },
		"future authority":         func(in *ExportSubmission) { in.AuthorityUntil = in.AuthorityUntil.Add(time.Second) },
		"expired authority":        func(in *ExportSubmission) { in.AuthorityUntil = time.Time{} },
		"invalid owner":            func(in *ExportSubmission) { in.SupervisorOwner = "caller" },
	} {
		t.Run(name, func(t *testing.T) {
			f := newExportStoreFixture(t)
			in := f.input()
			change(&in)
			if _, err := f.store.SubmitExport(f.ctx, in); err == nil {
				t.Fatal("unsafe input accepted")
			}
			if len(f.jobs.entries) != 0 {
				t.Fatal("invalid request reserved durable capacity")
			}
		})
	}
	f := newExportStoreFixture(t)
	if _, err := f.store.SubmitExport(context.Background(), f.input()); err == nil {
		t.Fatal("missing principal accepted")
	}
	f.store.policy.Exports.Limits.Compression = "lz4_frame"
	for _, codec := range []string{"none", "lz4_frame"} {
		in := f.input()
		in.Compression = codec
		j, err := normalizeExportSubmission(f.ctx, f.store.policy, in, f.now)
		if err != nil || j.Spec.StorageLimits.Compression != codec || j.Spec.StorageLimits.MaxDecodedBytes != f.store.policy.Exports.Limits.MaxDecodedBytes {
			t.Fatal(codec, err)
		}
	}
}

func TestExportIdentityChangesWithEveryAuthorityBoundary(t *testing.T) {
	f := newExportStoreFixture(t)
	j := f.submit(t).Job
	id, err := exportIdentity(f.store.policy, j.Authority)
	if err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(Policy, ExportAuthority) (Policy, ExportAuthority){
		"tenant": func(p Policy, a ExportAuthority) (Policy, ExportAuthority) {
			p.TenantID = "b"
			a.Principal, _ = authorityForPrincipal(p, "reports")
			return p, a
		},
		"principal": func(p Policy, a ExportAuthority) (Policy, ExportAuthority) {
			a.Principal, _ = authorityForPrincipal(p, "analyst")
			return p, a
		},
		"source policy": func(p Policy, a ExportAuthority) (Policy, ExportAuthority) {
			p.Access.Revision++
			a.Principal, _ = authorityForPrincipal(p, "reports")
			return p, a
		},
		"export version": func(p Policy, a ExportAuthority) (Policy, ExportAuthority) {
			p.Exports.AuthorizationVersion = "v2"
			a.AuthorizationVersion = "v2"
			return p, a
		},
	} {
		t.Run(name, func(t *testing.T) {
			p, a := change(f.store.Policy(), j.Authority)
			changed, err := exportIdentity(p, a)
			if err != nil || id.AuthorizationSHA256 == changed.AuthorizationSHA256 {
				t.Fatal(err)
			}
		})
	}
	for _, a := range []ExportAuthority{{}, {Principal: j.Authority.Principal, AuthorizationVersion: "old"}} {
		if _, err = exportIdentity(f.store.policy, a); err == nil {
			t.Fatal("stale authority accepted")
		}
	}
}

func TestExportReceiptRejectsIdentityFramingOrderAndBudgetMutation(t *testing.T) {
	changes := map[string]func(*ExportJob){
		"receipt version": func(j *ExportJob) { j.Receipt.Version++ }, "root": func(j *ExportJob) { j.Receipt.Locator.StorageID = strings.Repeat("d", 32) },
		"tenant": func(j *ExportJob) { j.Receipt.Manifest.Tenant = "foreign" }, "owner": func(j *ExportJob) { j.Receipt.Manifest.Identity.Owner = "analyst" },
		"grant":  func(j *ExportJob) { j.Receipt.Manifest.Identity.AuthorizationSHA256 = strings.Repeat("e", 64) },
		"expiry": func(j *ExportJob) { j.Receipt.Manifest.ExpiresAt = j.Receipt.Manifest.ExpiresAt.Add(time.Second) },
		"schema": func(j *ExportJob) { j.Receipt.Manifest.SchemaSHA256 = "invalid" }, "index": func(j *ExportJob) { j.Receipt.Manifest.Parts[0].Index = 1 },
		"rows": func(j *ExportJob) { j.Receipt.Manifest.Parts[0].Rows++ }, "negative rows": func(j *ExportJob) { j.Receipt.Manifest.Parts[0].Rows = -1 },
		"encoded": func(j *ExportJob) { j.Receipt.Manifest.Parts[0].EncodedBytes = j.Spec.StorageLimits.MaxPartBytes + 1 },
		"decoded": func(j *ExportJob) {
			j.Receipt.Manifest.Parts[0].DecodedBytes = j.Spec.StorageLimits.MaxPartDecodedBytes + 1
		},
		"digest": func(j *ExportJob) { j.Receipt.Manifest.Parts[0].SHA256 = "invalid" }, "seal": func(j *ExportJob) { j.Receipt.ReceiptSHA256 = strings.Repeat("f", 64) },
		"zero-batch rows": func(j *ExportJob) { j.Receipt.Manifest.Parts[0].Batches = 0 },
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			f := newExportStoreFixture(t)
			j := f.stored(t).Job
			change(&j)
			if err := validateExportReceipt(f.store.policy, j); err == nil {
				t.Fatal("mutated receipt accepted")
			}
		})
	}
}

func TestExportStoredJSONIsStrictAndCanonical(t *testing.T) {
	f := newExportStoreFixture(t)
	s := f.stored(t)
	raw, err := encodeExportJob(f.store.policy, s.Job)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = decodeExportJob(f.store.policy, raw); err != nil {
		t.Fatal("canonical rejected", err)
	}
	for name, bad := range map[string][]byte{
		"duplicate":  bytes.Replace(raw, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1),
		"case alias": bytes.Replace(raw, []byte(`"version":1`), []byte(`"Version":1`), 1),
		"unknown":    append([]byte(`{"unexpected":true,`), raw[1:]...),
		"trailing":   append(append([]byte(nil), raw...), []byte(`{}`)...),
	} {
		if _, err = decodeExportJob(f.store.policy, bad); err == nil {
			t.Fatal(name, "accepted")
		}
	}
	var shape ExportJob
	if err = json.Unmarshal(raw, &shape); err != nil {
		t.Fatal(err)
	}
	shape.Spec.StorageLimits.MaxRows++
	if _, err = encodeExportJob(f.store.policy, shape); err == nil {
		t.Fatal("mutated durable resource policy accepted")
	}
}

func TestExportTransitionMatrixNeverAdoptsStoppedJobs(t *testing.T) {
	f := newExportStoreFixture(t)
	s := f.stored(t)
	for _, from := range []string{ExportFailed, ExportCancelled, ExportPublicationUncertain} {
		for _, to := range []string{ExportQueued, ExportAssigned, ExportClaimed, ExportRunning, ExportStored, ExportReady} {
			cur := s.Job
			cur.State = from
			next := cur
			next.State = to
			if _, _, err := exportTransition(f.store.policy, cur, next, f.now); err == nil {
				t.Fatal(from, to)
			}
		}
	}
}
