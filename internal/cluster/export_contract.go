// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/exports"
	"github.com/SYNEHQ/kelvo-go/internal/query"
)

const exportJobValueLimit = 512 << 10

var errExportInvalid = errors.New("invalid export request or state")

func validateExportAuthority(p Policy, a ExportAuthority) error {
	if p.Exports == nil {
		return ErrExportDisabled
	}
	current, ok := authorityForPrincipal(p, a.Principal.PrincipalID)
	if !ok || current != a.Principal || a.AuthorizationVersion == "" || a.AuthorizationVersion != p.Exports.AuthorizationVersion {
		return errExportInvalid
	}
	return nil
}

func exportIdentity(p Policy, a ExportAuthority) (exports.Identity, error) {
	if err := validateExportAuthority(p, a); err != nil {
		return exports.Identity{}, err
	}
	raw, err := json.Marshal(struct {
		Domain string          `json:"domain"`
		Tenant string          `json:"tenant"`
		Grant  ExportAuthority `json:"grant"`
	}{"kelvo-export-identity-v1", p.TenantID, a})
	if err != nil {
		return exports.Identity{}, errExportInvalid
	}
	sum := sha256.Sum256(raw)
	return exports.Identity{Owner: a.Principal.PrincipalID, AuthorizationSHA256: hex.EncodeToString(sum[:])}, nil
}

func normalizeExportSpec(p Policy, codec string) (ExportSpec, error) {
	if p.Exports == nil {
		return ExportSpec{}, ErrExportDisabled
	}
	l := p.Exports.Limits
	configured := l.Compression
	if configured == "" {
		configured = "none"
	}
	if codec == "" {
		codec = configured
	}
	if codec != "none" && (codec != "lz4_frame" || configured != "lz4_frame") {
		return ExportSpec{}, errExportInvalid
	}
	l.Compression = codec
	if p.Limits.Validate() != nil || l.Validate() != nil || l.MaxRows > p.Limits.MaxRows || l.MaxEncodedBytes > p.Limits.MaxBytes || l.MaxDecodedBytes > p.Limits.MaxBytes {
		return ExportSpec{}, errExportInvalid
	}
	return ExportSpec{QueryLimits: p.Limits, StorageLimits: l}, nil
}

func normalizeExportSubmission(ctx context.Context, p Policy, in ExportSubmission, now time.Time) (ExportJob, error) {
	if p.Exports == nil {
		return ExportJob{}, ErrExportDisabled
	}
	if err := requestAuthorityErr(ctx); err != nil {
		return ExportJob{}, err
	}
	if in.Request.Mode != "federated" || !validOwner(in.SupervisorOwner) {
		return ExportJob{}, errExportInvalid
	}
	a, err := submissionAuthority(ctx, p, in.Request)
	if err != nil || a == nil {
		return ExportJob{}, errExportInvalid
	}
	spec, err := normalizeExportSpec(p, in.Compression)
	if err != nil {
		return ExportJob{}, err
	}
	ttl := in.TTL
	if ttl == 0 {
		ttl = p.Exports.DefaultTTL
	}
	if ttl < time.Second || ttl > p.Exports.MaxTTL || p.Exports.QueueTimeout <= 0 || !in.AuthorityUntil.After(now) || in.AuthorityUntil.After(now.Add(p.LeaseDuration)) {
		return ExportJob{}, errExportInvalid
	}
	expires := now.Add(ttl)
	if in.AuthorityUntil.After(expires) {
		return ExportJob{}, errExportInvalid
	}
	if auth, ok := ctx.Value(keyAuthorizationContext{}).(keyAuthorization); ok && (auth.authenticator == nil || in.AuthorityUntil.After(auth.authenticator.expiry())) {
		return ExportJob{}, errExportInvalid
	}
	return ExportJob{
		Version: 1, TenantID: p.TenantID,
		Authority: ExportAuthority{Principal: *a, AuthorizationVersion: p.Exports.AuthorizationVersion},
		Request:   in.Request, Spec: spec, State: ExportQueued,
		CreatedAt: now, QueueDeadline: minTime(now.Add(p.Exports.QueueTimeout), expires), ExpiresAt: expires,
		SupervisorOwner: in.SupervisorOwner, AuthorityUntil: in.AuthorityUntil,
	}, nil
}

func exportActive(j ExportJob) bool {
	switch j.State {
	case ExportQueued, ExportAssigned, ExportClaimed, ExportRunning, ExportStored:
		return true
	}
	return false
}

func exportSlot(id string) (int, bool) {
	if !strings.HasPrefix(id, "e") {
		return 0, false
	}
	return slot(strings.TrimPrefix(id, "e"))
}

func validExportLocator(l *ExportLocator) bool {
	return l != nil && validToken(l.StorageID) && validToken(l.ExportID) && validToken(l.Fence)
}

func exportDigest(value string) bool {
	if len(value) != 64 || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func receiptBytes(r ExportReceipt) ([]byte, error) {
	r.ReceiptSHA256 = ""
	raw, err := json.Marshal(struct {
		Domain  string        `json:"domain"`
		Receipt ExportReceipt `json:"receipt"`
	}{"kelvo-export-receipt-v1", r})
	if err != nil || len(raw) > 128<<10 {
		return nil, errExportInvalid
	}
	return raw, nil
}

func sealExportReceipt(locator ExportLocator, manifest exports.Manifest) (ExportReceipt, error) {
	r := ExportReceipt{Version: 1, Locator: locator, Manifest: manifest}
	r.Manifest.Parts = append([]exports.PartInfo(nil), manifest.Parts...)
	if !validExportLocator(&locator) || manifest.ID != locator.ExportID || manifest.Fence != locator.Fence || manifest.Version != 1 || !exportDigest(manifest.SchemaSHA256) || len(manifest.Parts) < 1 || len(manifest.Parts) > 256 {
		return ExportReceipt{}, errExportInvalid
	}
	raw, err := receiptBytes(r)
	if err != nil {
		return ExportReceipt{}, err
	}
	sum := sha256.Sum256(raw)
	r.ReceiptSHA256 = hex.EncodeToString(sum[:])
	return r, nil
}

func sameExportReceipt(a, b *ExportReceipt) bool { return reflect.DeepEqual(a, b) }

func validateExportReceipt(p Policy, j ExportJob) error {
	r := j.Receipt
	if r == nil || r.Version != 1 || j.Local == nil || r.Locator != *j.Local || !validExportLocator(j.Local) {
		return errExportInvalid
	}
	id, err := exportIdentity(p, j.Authority)
	if err != nil {
		return err
	}
	m, l := r.Manifest, j.Spec.StorageLimits
	if m.Version != 1 || m.ID != j.Local.ExportID || m.Fence != j.Local.Fence || m.Tenant != j.TenantID || m.Identity != id || !m.ExpiresAt.Equal(j.ExpiresAt) || m.CreatedAt.Before(j.CreatedAt) || !m.CreatedAt.Before(m.ExpiresAt) || !exportDigest(m.SchemaSHA256) || len(m.Parts) < 1 || len(m.Parts) > l.MaxParts {
		return errExportInvalid
	}
	var rows, encoded, decoded int64
	for index, part := range m.Parts {
		if part.Index != index || part.Rows < 0 || part.Rows > l.MaxRows-rows || part.EncodedBytes <= 0 || part.EncodedBytes > l.MaxPartBytes || part.EncodedBytes > l.MaxEncodedBytes-encoded || part.DecodedBytes < 0 || part.DecodedBytes > l.MaxPartDecodedBytes || part.DecodedBytes > l.MaxDecodedBytes-decoded || !exportDigest(part.SHA256) || part.Batches < 0 || part.Batches > 1 || (part.Batches == 0 && (part.Rows != 0 || part.DecodedBytes != 0)) {
			return errExportInvalid
		}
		rows += part.Rows
		encoded += part.EncodedBytes
		decoded += part.DecodedBytes
	}
	if rows != m.Rows || encoded != m.EncodedBytes || decoded != m.DecodedBytes {
		return errExportInvalid
	}
	sealed, err := sealExportReceipt(r.Locator, m)
	if err != nil || !sameExportReceipt(r, &sealed) {
		return errExportInvalid
	}
	return nil
}

// Shape validation deliberately accepts expired records so reconciliation and
// retention can inspect them. Operations check current authority/expiry too.
func validateExportJob(p Policy, j ExportJob) error {
	if p.Exports == nil {
		return ErrExportDisabled
	}
	n, ok := exportSlot(j.ID)
	if !ok || n < 0 || n >= p.Exports.MaxJobs || j.Version != 1 || j.TenantID != p.TenantID || j.Request.Mode != "federated" || query.ValidateRequest(j.Request) != nil || validateJobAuthority(p, &j.Authority.Principal, j.Request) != nil || validateExportAuthority(p, j.Authority) != nil || !validOwner(j.SupervisorOwner) {
		return errExportInvalid
	}
	expected, err := normalizeExportSpec(p, j.Spec.StorageLimits.Compression)
	if err != nil || expected != j.Spec || j.CreatedAt.IsZero() || !j.ExpiresAt.After(j.CreatedAt) || j.ExpiresAt.Sub(j.CreatedAt) > p.Exports.MaxTTL || !j.QueueDeadline.Equal(minTime(j.CreatedAt.Add(p.Exports.QueueTimeout), j.ExpiresAt)) || !j.AuthorityUntil.After(j.CreatedAt) || j.AuthorityUntil.After(j.ExpiresAt) {
		return errExportInvalid
	}
	if j.WorkerID == "" {
		if j.WorkerOwner != "" || j.Claim != "" || !j.HeartbeatAt.IsZero() || !j.StartedAt.IsZero() || !j.ExecutionDeadline.IsZero() || j.Local != nil || j.Receipt != nil {
			return errExportInvalid
		}
	} else if p.Workers[j.WorkerID] < 1 || !validOwner(j.WorkerOwner) || j.HeartbeatAt.Before(j.CreatedAt) || j.HeartbeatAt.After(j.ExpiresAt) {
		return errExportInvalid
	}
	if j.Claim != "" && !validClaim(j.Claim) {
		return errExportInvalid
	}
	if j.StartedAt.IsZero() {
		if !j.ExecutionDeadline.IsZero() || j.Local != nil || j.Receipt != nil {
			return errExportInvalid
		}
	} else if j.Claim == "" || j.StartedAt.Before(j.CreatedAt) || !j.StartedAt.Before(j.ExpiresAt) || !j.ExecutionDeadline.Equal(minTime(j.StartedAt.Add(j.Spec.QueryLimits.Timeout), j.ExpiresAt)) {
		return errExportInvalid
	}
	if j.Local != nil && !validExportLocator(j.Local) {
		return errExportInvalid
	}
	if j.Receipt != nil && validateExportReceipt(p, j) != nil {
		return errExportInvalid
	}
	if j.Receipt == nil && !reflect.DeepEqual(j.Stats, query.Stats{}) {
		return errExportInvalid
	}
	switch j.State {
	case ExportQueued:
		if j.WorkerID != "" {
			return errExportInvalid
		}
	case ExportAssigned:
		if j.WorkerID == "" || j.Claim != "" {
			return errExportInvalid
		}
	case ExportClaimed:
		if j.Claim == "" || !j.StartedAt.IsZero() {
			return errExportInvalid
		}
	case ExportRunning:
		if j.StartedAt.IsZero() || j.Receipt != nil {
			return errExportInvalid
		}
	case ExportStored, ExportReady:
		if j.Receipt == nil {
			return errExportInvalid
		}
	case ExportPublicationUncertain:
		if j.Local == nil || j.Receipt != nil || j.Error == nil {
			return errExportInvalid
		}
	case ExportFailed, ExportCancelled:
		if j.Error == nil {
			return errExportInvalid
		}
	default:
		return errExportInvalid
	}
	if (exportActive(j) || j.State == ExportReady) && j.Error != nil {
		return errExportInvalid
	}
	if j.Error != nil && (len(j.Error.Code) == 0 || len(j.Error.Code) > 64 || len(j.Error.Message) > 1024) {
		return errExportInvalid
	}
	return nil
}

func encodeExportJob(p Policy, j ExportJob) ([]byte, error) {
	if err := validateExportJob(p, j); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(j)
	if err != nil || len(raw) > exportJobValueLimit {
		return nil, errExportInvalid
	}
	return raw, nil
}

func decodeExportJob(p Policy, raw []byte) (ExportJob, error) {
	var j ExportJob
	if len(raw) == 0 || len(raw) > exportJobValueLimit {
		return j, errExportInvalid
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(&j) != nil || d.Decode(new(any)) != io.EOF || validateExportJob(p, j) != nil {
		return ExportJob{}, errExportInvalid
	}
	// Only our canonical encoding is stored. This also rejects repeated or
	// differently cased JSON keys instead of accepting the decoder's last value.
	canonical, err := json.Marshal(j)
	if err != nil || !bytes.Equal(canonical, raw) {
		return ExportJob{}, errExportInvalid
	}
	return j, nil
}
