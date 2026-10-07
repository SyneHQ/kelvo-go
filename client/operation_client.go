// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package client

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash"
	"io"
	"mime"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/SYNEHQ/kelvo-go/operations"
)

const operationResponseLimit = operations.MaxReceiptBytes + 4<<10

// OperationUncertainError means no verified terminal receipt was obtained.
// Reconcile the ID or idempotency key; never automatically replay the mutation.
type OperationUncertainError struct {
	OperationID    string `json:"operation_id,omitempty"`
	RequestSHA256  string `json:"request_sha256"`
	IdempotencyKey string `json:"idempotency_key,omitempty"`
	cause          error
}

func (e *OperationUncertainError) Error() string {
	return "Database operation outcome is uncertain; reconcile before retrying"
}
func (e *OperationUncertainError) Unwrap() error { return e.cause }

func uncertainOperation(id, digest, key string, err error) error {
	return &OperationUncertainError{OperationID: id, RequestSHA256: digest, IdempotencyKey: key, cause: err}
}

func validOperationGrant(grant string) bool {
	if len(grant) == 0 || len(grant) > operations.MaxGrantBytes {
		return false
	}
	parts := strings.Split(grant, ".")
	if len(parts) != 3 {
		return false
	}
	for _, part := range parts {
		raw, err := base64.RawURLEncoding.Strict().DecodeString(part)
		if err != nil || len(raw) == 0 || base64.RawURLEncoding.EncodeToString(raw) != part {
			return false
		}
	}
	// The service verifies signatures and current authority. Decoded claims are
	// never trusted here to select an endpoint or weaken transport configuration.
	return true
}

func (c *Client) operationContext(ctx context.Context, auth Authority) (context.Context, func(), error) {
	if _, err := c.authority(auth, "operation"); err != nil {
		return nil, nil, err
	}
	return c.begin(ctx, false)
}
func (c *Client) operationControlContext(ctx context.Context, auth Authority) (context.Context, func(), error) {
	if _, err := c.authority(auth, "operation"); err != nil {
		return nil, nil, err
	}
	return c.begin(ctx, true)
}

func (c *Client) doOperation(ctx context.Context, method, path string, auth Authority, body []byte) (*http.Response, error) {
	auth, err := c.authority(auth, "operation")
	if err != nil {
		return nil, err
	}
	return c.do(ctx, method, path, auth, body)
}

func operationContentType(response *http.Response, expected string) bool {
	types := response.Header.Values("Content-Type")
	encodings := response.Header.Values("Content-Encoding")
	value, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	return len(types) == 1 && err == nil && value == expected && !response.Uncompressed &&
		len(encodings) <= 1 && (len(encodings) == 0 || encodings[0] == "" || encodings[0] == "identity")
}

func decodeOperationResponse(response *http.Response, id, digest string) (operations.Response, error) {
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusCreated && response.StatusCode != http.StatusAccepted {
		return operations.Response{}, statusFailure(response.StatusCode)
	}
	if !operationContentType(response, "application/json") || response.ContentLength > operationResponseLimit {
		return operations.Response{}, failure("PROTOCOL_ERROR")
	}
	raw, readErr := io.ReadAll(io.LimitReader(response.Body, operationResponseLimit+1))
	var status operations.Response
	if len(raw) > operationResponseLimit || operations.DecodeStrict(raw, &status, operationResponseLimit) != nil || status.ValidateBinding(id, digest) != nil {
		return operations.Response{}, failure("PROTOCOL_ERROR")
	}
	if readErr != nil {
		return status, failure("PROTOCOL_ERROR")
	}
	return status, nil
}

func operationRequest(request operations.Request) ([]byte, string, error) {
	raw, err := operations.Encode(request)
	if err != nil || len(raw) > operations.MaxRequestBytes {
		return nil, "", failure("INVALID_ARGUMENT")
	}
	pinned, err := operations.ParseRequest(raw)
	if err != nil {
		return nil, "", failure("INVALID_ARGUMENT")
	}
	digest, err := operations.Digest(pinned)
	if err != nil {
		return nil, "", failure("INVALID_ARGUMENT")
	}
	return raw, digest, nil
}

func (c *Client) submitOperation(ctx context.Context, request operations.Request, raw []byte, digest string, auth Authority) (operations.Response, error) {
	response, err := c.doOperation(ctx, http.MethodPost, "/v1/operations", auth, raw)
	if err != nil {
		return operations.Response{}, uncertainOperation("", digest, request.IdempotencyKey, err)
	}
	status, err := decodeOperationResponse(response, "", digest)
	if err == nil && status.Receipt != nil && operations.ValidateStatementReceipt(request, *status.Receipt) != nil {
		err = failure("PROTOCOL_ERROR")
	}
	if err != nil {
		return status, uncertainOperation(status.ID, digest, request.IdempotencyKey, err)
	}
	return status, nil
}

// SubmitOperation submits exactly once. A nil error validates the response;
// only its receipt describes completed source effects.
func (c *Client) SubmitOperation(ctx context.Context, request operations.Request, auth Authority) (operations.Response, error) {
	raw, digest, err := operationRequest(request)
	if err != nil {
		return operations.Response{}, err
	}
	ctx, release, err := c.operationContext(ctx, auth)
	if err != nil {
		return operations.Response{}, err
	}
	defer release()
	return c.submitOperation(ctx, request, raw, digest, auth)
}

func (c *Client) pollOperation(ctx context.Context, id, digest string, auth Authority) (operations.Response, error) {
	response, err := c.doOperation(ctx, http.MethodGet, "/v1/operations/"+id, auth, nil)
	if err != nil {
		return operations.Response{}, err
	}
	if response.StatusCode != http.StatusOK {
		response.Body.Close()
		return operations.Response{}, statusFailure(response.StatusCode)
	}
	return decodeOperationResponse(response, id, digest)
}

// PollOperation reconciles a known operation; it cannot submit source work.
func (c *Client) PollOperation(ctx context.Context, id, digest string, auth Authority) (operations.Response, error) {
	if !operations.ValidID(id) || !operations.ValidDigest(digest) {
		return operations.Response{}, failure("INVALID_ARGUMENT")
	}
	ctx, release, err := c.operationControlContext(ctx, auth)
	if err != nil {
		return operations.Response{}, uncertainOperation(id, digest, "", err)
	}
	defer release()
	status, err := c.pollOperation(ctx, id, digest, auth)
	if err != nil {
		err = uncertainOperation(id, digest, "", err)
	}
	return status, err
}

// CancelOperation requests cancellation once. An acknowledgement alone never
// means rollback: running operations can return an immutable unknown outcome.
func (c *Client) CancelOperation(ctx context.Context, id, digest string, auth Authority) (operations.Response, error) {
	if !operations.ValidID(id) || !operations.ValidDigest(digest) {
		return operations.Response{}, failure("INVALID_ARGUMENT")
	}
	ctx, release, err := c.operationControlContext(ctx, auth)
	if err != nil {
		return operations.Response{}, uncertainOperation(id, digest, "", err)
	}
	defer release()
	response, err := c.doOperation(ctx, http.MethodPost, "/v1/operations/"+id+"/cancel", auth, []byte(`{}`))
	if err != nil {
		return operations.Response{}, uncertainOperation(id, digest, "", err)
	}
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusAccepted {
		response.Body.Close()
		return operations.Response{}, uncertainOperation(id, digest, "", statusFailure(response.StatusCode))
	}
	status, err := decodeOperationResponse(response, id, digest)
	if err != nil {
		err = uncertainOperation(id, digest, "", err)
	}
	return status, err
}

// ExecuteOperation submits once, then polls within one admission/deadline.
// It returns rejected, failed and unknown receipts unchanged for the handler to
// interpret. It does not resubmit or infer rollback from transport failure.
func (c *Client) ExecuteOperation(ctx context.Context, request operations.Request, auth Authority) (operations.Response, error) {
	raw, digest, err := operationRequest(request)
	if err != nil {
		return operations.Response{}, err
	}
	ctx, release, err := c.operationContext(ctx, auth)
	if err != nil {
		return operations.Response{}, err
	}
	defer release()
	status, err := c.submitOperation(ctx, request, raw, digest, auth)
	if err != nil {
		return status, err
	}
	id := status.ID
	for status.Receipt == nil {
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return status, uncertainOperation(id, digest, request.IdempotencyKey, contextFailure(ctx))
		case <-timer.C:
		}
		next, err := c.pollOperation(ctx, id, digest, auth)
		if err != nil {
			return status, uncertainOperation(id, digest, request.IdempotencyKey, err)
		}
		status = next
	}
	if operations.ValidateStatementReceipt(request, *status.Receipt) != nil {
		return status, uncertainOperation(id, digest, request.IdempotencyKey, failure("PROTOCOL_ERROR"))
	}
	return status, nil
}

// OperationResult verifies immutable result bytes at physical EOF. Read may
// deliver partial bytes; callers must not announce success until Complete.
// Arrow consumers must also enforce their decoded-memory/type limits.
type OperationResult struct {
	mu             sync.Mutex
	ctx            context.Context
	body           io.ReadCloser
	expected       operations.ResultRef
	hash           hash.Hash
	read           int64
	verified       bool
	closed         bool
	completionUsed bool
	err            error
	release        func()
	stop           func() bool
	abort          context.CancelFunc
}

// OpenOperationResult performs only GETs. The fresh receipt must exactly match
// the caller's completed receipt before the immutable result endpoint is read.
func (c *Client) OpenOperationResult(ctx context.Context, expected operations.Response, auth Authority) (*OperationResult, error) {
	result, _, err := c.openOperationResult(ctx, expected, auth, false)
	return result, err
}

func (c *Client) openOperationResult(ctx context.Context, expected operations.Response, auth Authority, holdAdmission bool) (*OperationResult, func(), error) {
	if expected.Validate() != nil || expected.State != string(operations.Completed) || expected.Receipt == nil || expected.Receipt.Result == nil {
		return nil, nil, failure("INVALID_ARGUMENT")
	}
	raw, err := json.Marshal(expected)
	var pinned operations.Response
	if err != nil || operations.DecodeStrict(raw, &pinned, operationResponseLimit) != nil {
		return nil, nil, failure("INVALID_ARGUMENT")
	}
	ctx, release, err := c.operationContext(ctx, auth)
	if err != nil {
		return nil, nil, err
	}
	keep := false
	defer func() {
		if !keep {
			release()
		}
	}()
	current, err := c.pollOperation(ctx, pinned.ID, pinned.RequestSHA256, auth)
	if err != nil {
		return nil, nil, err
	}
	if !reflect.DeepEqual(current, pinned) {
		return nil, nil, failure("PROTOCOL_ERROR")
	}
	ref := *pinned.Receipt.Result
	if ref.Bytes > c.cfg.MaxWireBytes || ref.Rows > c.cfg.MaxRows {
		return nil, nil, failure("RESOURCE_EXHAUSTED")
	}
	resultCtx, abort := context.WithCancel(ctx)
	response, err := c.doOperation(resultCtx, http.MethodGet, "/v1/operations/"+pinned.ID+"/results", auth, nil)
	if err != nil {
		abort()
		return nil, nil, err
	}
	media := "application/json"
	if ref.Format == "arrow_ipc" {
		media = "application/vnd.apache.arrow.stream"
	}
	if response.StatusCode != http.StatusOK || !operationContentType(response, media) || (response.ContentLength >= 0 && response.ContentLength != ref.Bytes) {
		abort()
		response.Body.Close()
		return nil, nil, failure("PROTOCOL_ERROR")
	}
	resultRelease := release
	if holdAdmission {
		resultRelease = func() {}
	}
	result := &OperationResult{ctx: resultCtx, body: response.Body, expected: ref, hash: sha256.New(), release: resultRelease, abort: abort}
	result.mu.Lock()
	result.stop = context.AfterFunc(resultCtx, func() { result.terminate(contextFailure(resultCtx)) })
	result.mu.Unlock()
	keep = true
	return result, release, nil
}

func (r *OperationResult) finishLocked(err error) {
	if r.closed {
		return
	}
	if err == nil && !r.verified {
		err = failure("PROTOCOL_ERROR")
	}
	if closeErr := r.body.Close(); closeErr != nil && err == nil {
		err = failure("PROTOCOL_ERROR")
	}
	r.err, r.closed = err, true
	if err != nil {
		r.verified = false
	}
	if r.stop != nil {
		r.stop()
	}
	r.release()
}

func (r *OperationResult) terminate(err error) {
	// Closing the body before taking the lock interrupts a concurrent Read.
	_ = r.body.Close()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.finishLocked(err)
}

func (r *OperationResult) Read(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		if r.err != nil {
			return 0, r.err
		}
		return 0, io.EOF
	}
	if len(p) == 0 {
		return 0, nil
	}
	remaining := r.expected.Bytes - r.read
	n, err := r.body.Read(p[:min(int64(len(p)), remaining+1)])
	if int64(n) > remaining {
		r.finishLocked(failure("PROTOCOL_ERROR"))
		return 0, r.err
	}
	if r.ctx.Err() != nil {
		r.finishLocked(contextFailure(r.ctx))
		return n, r.err
	}
	if n > 0 {
		_, _ = r.hash.Write(p[:n])
		r.read += int64(n)
	}
	if errors.Is(err, io.EOF) {
		actual := hex.EncodeToString(r.hash.Sum(nil))
		if r.read != r.expected.Bytes || subtle.ConstantTimeCompare([]byte(actual), []byte(r.expected.SHA256)) != 1 {
			r.finishLocked(failure("PROTOCOL_ERROR"))
			return n, r.err
		}
		r.verified = true
		r.finishLocked(nil)
		if r.err != nil {
			return n, r.err
		}
		return n, io.EOF
	}
	if err != nil {
		resultErr := error(failure("UNAVAILABLE"))
		if r.ctx.Err() != nil {
			resultErr = contextFailure(r.ctx)
		}
		r.finishLocked(resultErr)
		return n, r.err
	}
	return n, nil
}

func (r *OperationResult) Verified() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.verified && r.err == nil
}

func (r *OperationResult) Close() error {
	r.abort()
	_ = r.body.Close()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.finishLocked(failure("PROTOCOL_ERROR"))
	return r.err
}

// Complete calls onVerified once, synchronously, only after exact bytes/hash
// and physical EOF were verified. Use it for the final success/EOS marker.
func (r *OperationResult) Complete(onVerified func() error) error {
	r.mu.Lock()
	if r.err != nil {
		err := r.err
		r.mu.Unlock()
		return err
	}
	if !r.verified || r.completionUsed || onVerified == nil {
		r.mu.Unlock()
		return failure("PROTOCOL_ERROR")
	}
	r.completionUsed = true
	r.mu.Unlock()
	if err := onVerified(); err != nil {
		return failure("SINK_FAILED")
	}
	return nil
}
