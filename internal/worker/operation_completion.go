// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	operationstore "github.com/SYNEHQ/kelvo-go/internal/operations"
	"github.com/SYNEHQ/kelvo-go/operations"
)

type operationCompletion struct {
	Version       int    `json:"version"`
	OperationID   string `json:"operation_id"`
	RequestSHA256 string `json:"request_sha256"`
	GrantSHA256   string `json:"grant_sha256"`
	WorkerID      string `json:"worker_id"`
	Owner         string `json:"owner"`
	Claim         string `json:"claim"`
}

// CompleteOperationCleanup reports physical cleanup to the source-authorizing
// service. Caller must possess the observed cgroup/scratch cleanup proof; ledger
// terminal states and expired leases cannot substitute for that observation.
func (e *Executor) CompleteOperationCleanup(ctx context.Context, record operationstore.Record) error {
	if ctx == nil || e == nil || record.State != operationstore.Running || record.Scope.Validate() != nil || record.Binding.Validate() != nil || !operations.ValidID(record.ID) || !operations.ValidDigest(record.RequestSHA256) || record.AuthoritySHA256 != operations.GrantDigest(record.AuthorityToken) {
		return connectionUnavailable()
	}
	r := e.connectionResolvers[record.Scope.Issuer]
	if r == nil || r.client == nil || r.slots == nil || !strings.HasSuffix(r.url, "/internal/kelvo/resolve") {
		return connectionUnavailable()
	}
	select {
	case r.slots <- struct{}{}:
		defer func() { <-r.slots }()
	case <-ctx.Done():
		return ctx.Err()
	}
	raw, err := json.Marshal(operationCompletion{Version: 1, OperationID: record.ID, RequestSHA256: record.RequestSHA256,
		GrantSHA256: record.AuthoritySHA256, WorkerID: record.Binding.WorkerID, Owner: record.Binding.Owner, Claim: record.Binding.Claim})
	if err != nil {
		return connectionUnavailable()
	}
	defer clear(raw)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimSuffix(r.url, "/resolve")+"/complete-operation", bytes.NewReader(raw))
	if err != nil {
		return connectionUnavailable()
	}
	req.GetBody = nil
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept-Encoding", "identity")
	response, err := r.client.Do(req)
	if err != nil {
		return connectionUnavailable()
	}
	defer response.Body.Close()
	_, peerValid := resolverCertificateExpiry(response.TLS, time.Now())
	if response.StatusCode != http.StatusNoContent || !peerValid || response.Header.Get("Content-Encoding") != "" || response.ContentLength > 0 {
		return connectionUnavailable()
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, 1))
	if err != nil || len(body) != 0 || ctx.Err() != nil {
		return connectionUnavailable()
	}
	return nil
}
