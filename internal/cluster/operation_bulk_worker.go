// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"mime"
	"net/http"
	"time"

	operationstore "github.com/SYNEHQ/kelvo-go/internal/operations"
	"github.com/SYNEHQ/kelvo-go/operations"
)

func (c *operationInputClient) loadBulk(ctx context.Context, record operationstore.Record, request operations.Request) (payload []byte, resultErr error) {
	defer func() {
		if resultErr != nil {
			clear(payload)
			payload = nil
		}
	}()
	ref := request.InputReference()
	if ref == nil {
		return nil, nil
	}
	if !c.ready() || record.State != operationstore.Running || record.Scope.ClusterTenant != c.tenant || record.Binding.WorkerID != c.worker || record.Binding.Owner != c.owner || record.Binding.Validate() != nil || !operations.ValidID(record.ID) || request.Validate() != nil || operations.SealedInputOperation(ref.Format) == "" || request.Kind != operations.SealedInputOperation(ref.Format) || ref.Validate() != nil || ref.Bytes > operations.MaxSealedInputBytes {
		return nil, operationstore.ErrInvalid
	}
	digest, err := operations.Digest(request)
	if err != nil || digest != record.RequestSHA256 {
		return nil, operationstore.ErrInvalid
	}
	raw, err := json.Marshal(operationInputRequest{Scope: record.Scope, Binding: record.Binding})
	if err != nil {
		return nil, operationstore.ErrInvalid
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+"/v1/operations/"+c.tenant+"/"+record.ID+"/bulk-input", bytes.NewReader(raw))
	if err != nil {
		return nil, operationstore.ErrInvalid
	}
	req.GetBody = nil
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/octet-stream")
	response, err := c.client.Do(req)
	if err != nil {
		return nil, operationstore.ErrUnavailable
	}
	defer response.Body.Close()
	media, _, mediaErr := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if response.StatusCode != http.StatusOK || mediaErr != nil || len(response.Header.Values("Content-Type")) != 1 || media != "application/octet-stream" || response.Header.Get("Content-Encoding") != "" || !operationGatewayResponsePeer(response.TLS, time.Now()) || (response.ContentLength != -1 && response.ContentLength != ref.Bytes) {
		return nil, operationstore.ErrUnavailable
	}
	payload, err = io.ReadAll(io.LimitReader(response.Body, ref.Bytes+1))
	if err != nil || int64(len(payload)) != ref.Bytes || ctx.Err() != nil || !c.ready() || !operationGatewayResponsePeer(response.TLS, time.Now()) {
		return payload, operationstore.ErrUnavailable
	}
	sum := sha256.Sum256(payload)
	if hex.EncodeToString(sum[:]) != ref.SHA256 {
		return payload, operationstore.ErrInvalid
	}
	return payload, nil
}
