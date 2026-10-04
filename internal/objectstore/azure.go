// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package objectstore

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
)

const azureStorageVersion = "2023-11-03"

// azureClient uses the explicitly configured endpoint and SAS only. A SAS can
// authorize operations but cannot change the endpoint, key, or operation here.
type azureClient struct {
	lifetime  clientLifetime
	location  catalog.ObjectLocation
	origin    *url.URL
	sas       url.Values
	http      *http.Client
	closeIdle func()
}

func newAzure(location catalog.ObjectLocation, credentials catalog.ObjectCredentials) (Client, error) {
	return newAzureWithHTTP(location, credentials, nil)
}

func newAzureWithHTTP(location catalog.ObjectLocation, credentials catalog.ObjectCredentials, client *http.Client) (Client, error) {
	if location.Provider != "azure" {
		return nil, errors.New("Azure snapshot provider is required")
	}
	if err := location.Validate(); err != nil {
		return nil, err
	}
	if err := credentials.Validate("azure"); err != nil {
		return nil, err
	}
	sas, err := catalog.ParseAzureSAS(os.Getenv(credentials.SASTokenEnv))
	if err != nil {
		return nil, errors.New("Azure snapshot credentials are unavailable or invalid")
	}
	origin, err := url.Parse(location.Endpoint)
	if err != nil {
		return nil, errors.New("Azure snapshot endpoint is invalid")
	}
	closeIdle := func() {}
	if client == nil {
		transport := &http.Transport{
			Proxy:                  nil,
			DialContext:            (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
			TLSClientConfig:        &tls.Config{MinVersion: tls.VersionTLS12},
			TLSHandshakeTimeout:    5 * time.Second,
			ResponseHeaderTimeout:  30 * time.Second,
			IdleConnTimeout:        90 * time.Second,
			MaxIdleConns:           4,
			MaxIdleConnsPerHost:    4,
			MaxConnsPerHost:        4,
			MaxResponseHeaderBytes: 64 << 10,
			DisableCompression:     true,
		}
		client = &http.Client{
			Transport: transport,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		}
		closeIdle = client.CloseIdleConnections
	}
	return &azureClient{location: location, origin: origin, sas: sas, http: client, closeIdle: closeIdle}, nil
}

func (c *azureClient) Close() {
	closeIdle := c.closeIdle
	if closeIdle == nil {
		closeIdle = c.http.CloseIdleConnections
	}
	c.lifetime.close(closeIdle)
}

func (c *azureClient) request(ctx context.Context, method, key, version string, body io.Reader) (*http.Request, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := c.location.ValidateKey(key); err != nil {
		return nil, err
	}
	if version != "" && !azureValidETag(version) {
		return nil, errors.New("Azure snapshot version is invalid")
	}
	u := *c.origin
	u.Path = "/" + c.location.Bucket + "/" + key
	u.RawQuery = c.sas.Encode()
	request, err := http.NewRequestWithContext(ctx, method, u.String(), body)
	if err != nil {
		return nil, errors.New("Azure snapshot request could not be prepared")
	}
	request.Header.Set("x-ms-version", azureStorageVersion)
	request.Header.Set("Accept-Encoding", "identity")
	if version != "" {
		request.Header.Set("If-Match", version)
	}
	return request, nil
}

// do never returns URL errors or provider bodies: both can contain SAS material.
// There are no implicit retries. Callers resolve uncertain conditional writes by
// reading the manifest before deciding whether another operation is appropriate.
func (c *azureClient) do(request *http.Request, expected int) (*http.Response, error) {
	response, err := c.http.Do(request)
	if err != nil {
		if request.Context().Err() != nil {
			return nil, request.Context().Err()
		}
		return nil, errors.New("Azure snapshot request failed")
	}
	if response.StatusCode != expected {
		_ = response.Body.Close()
		switch response.StatusCode {
		case http.StatusNotFound:
			return response, ErrNotFound
		case http.StatusConflict, http.StatusPreconditionFailed:
			return response, ErrConflict
		default:
			return response, errors.New("Azure snapshot request was rejected")
		}
	}
	return response, nil
}

func (c *azureClient) Get(ctx context.Context, key, version string) (io.ReadCloser, Info, error) {
	op, err := c.lifetime.begin(ctx)
	if err != nil {
		return nil, Info{}, err
	}
	defer op.finish(true)
	ctx = op.ctx
	request, err := c.request(ctx, http.MethodGet, key, version, nil)
	if err != nil {
		return nil, Info{}, err
	}
	response, err := c.do(request, http.StatusOK)
	if err != nil {
		return nil, azureErrorInfo(response), err
	}
	info, err := azureReadInfo(response, version)
	if err != nil {
		_ = response.Body.Close()
		return nil, info, err
	}
	body, err := op.returnBody(&azureReadBody{body: response.Body, ctx: ctx, remaining: info.Size})
	return body, info, err
}

func (c *azureClient) Head(ctx context.Context, key, version string) (Info, error) {
	op, err := c.lifetime.begin(ctx)
	if err != nil {
		return Info{}, err
	}
	defer op.finish(true)
	ctx = op.ctx
	request, err := c.request(ctx, http.MethodHead, key, version, nil)
	if err != nil {
		return Info{}, err
	}
	response, err := c.do(request, http.StatusOK)
	if err != nil {
		return azureErrorInfo(response), err
	}
	defer response.Body.Close()
	return azureReadInfo(response, version)
}

// GetRange pins the immutable object's version before requesting one exact
// interval. A service that ignores Range is rejected, never downloaded in full.
func (c *azureClient) GetRange(ctx context.Context, key, version string, offset, length int64) (io.ReadCloser, Info, error) {
	op, err := c.lifetime.begin(ctx)
	if err != nil {
		return nil, Info{}, err
	}
	defer op.finish(true)
	ctx = op.ctx
	if version == "" || offset < 0 || length <= 0 || offset >= MaxUploadBytes || length > MaxUploadBytes-offset {
		return nil, Info{}, errors.New("Azure snapshot range requires a version and a bounded interval")
	}
	request, err := c.request(ctx, http.MethodGet, key, version, nil)
	if err != nil {
		return nil, Info{}, err
	}
	request.Header.Set("Range", "bytes="+strconv.FormatInt(offset, 10)+"-"+strconv.FormatInt(offset+length-1, 10))
	response, err := c.do(request, http.StatusPartialContent)
	if err != nil {
		return nil, azureErrorInfo(response), err
	}
	info, err := azureReadInfo(response, version)
	if err != nil {
		_ = response.Body.Close()
		return nil, info, err
	}
	if _, err := azureHeader(response.Header, "Content-Range"); err != nil {
		_ = response.Body.Close()
		return nil, info, err
	}
	total, err := ValidateRangeResponse(response, offset, length)
	if err != nil {
		_ = response.Body.Close()
		return nil, info, err
	}
	info.Size = total
	body, err := op.returnBody(ExactRangeBody(&azureSafeBody{body: response.Body, ctx: ctx}, length))
	return body, info, err
}

func (c *azureClient) Put(ctx context.Context, key string, body io.ReadSeeker, size int64, sha256 string, condition Condition) (Info, error) {
	op, err := c.lifetime.begin(ctx)
	if err != nil {
		return Info{}, err
	}
	defer op.finish(true)
	ctx = op.ctx
	if body == nil || size < 0 || size > MaxUploadBytes || !azureValidSHA256(sha256) ||
		(condition.Absent == (condition.Version != "")) {
		return Info{}, errors.New("Azure snapshot upload requires a bounded payload and one precondition")
	}
	request, err := c.request(ctx, http.MethodPut, key, condition.Version, nil)
	if err != nil {
		return Info{}, err
	}
	length, err := body.Seek(0, io.SeekEnd)
	if err != nil || length != size {
		return Info{}, errors.New("Azure snapshot upload size does not match its payload")
	}
	if _, err = body.Seek(0, io.SeekStart); err != nil {
		return Info{}, errors.New("Azure snapshot upload payload cannot be read")
	}
	if size == 0 {
		request.Body = http.NoBody
	} else {
		// The caller owns the staging file. Closing an HTTP request must not close
		// it, and the request may never consume more than the declared payload.
		upload := newUploadBody(body, size)
		request.Body = upload
		defer upload.wait()
	}
	request.ContentLength = size
	request.Header.Set("Content-Type", "application/octet-stream")
	request.Header.Set("x-ms-blob-type", "BlockBlob")
	request.Header.Set("x-ms-meta-kelvo-sha256", sha256)
	if condition.Absent {
		request.Header.Set("If-None-Match", "*")
	}
	response, err := c.do(request, http.StatusCreated)
	if err != nil {
		return azureErrorInfo(response), err
	}
	defer response.Body.Close()
	info, err := azureResponseIdentity(response)
	if err != nil {
		return Info{}, err
	}
	// A successful Put Blob has no object body; its Content-Length is not the
	// uploaded object's length. Azure returns the new ETag and server Date.
	info.Size, info.SHA256 = size, sha256
	return info, nil
}

func azureReadInfo(response *http.Response, expectedVersion string) (Info, error) {
	info, err := azureResponseIdentity(response)
	if err != nil {
		return Info{}, err
	}
	length, err := azureHeader(response.Header, "Content-Length")
	if err != nil || length == "" {
		return Info{}, errors.New("Azure snapshot response lacks a valid object size")
	}
	info.Size, err = strconv.ParseInt(length, 10, 64)
	if err != nil || info.Size < 0 || info.Size > MaxUploadBytes || response.ContentLength != info.Size {
		return Info{}, errors.New("Azure snapshot response has an invalid object size")
	}
	if expectedVersion != "" && info.Version != expectedVersion {
		return info, ErrConflict
	}
	encoding, err := azureHeader(response.Header, "Content-Encoding")
	if err != nil || (encoding != "" && encoding != "identity") {
		return Info{}, errors.New("Azure snapshot response has an unsupported encoding")
	}
	info.SHA256, err = azureHeader(response.Header, "x-ms-meta-kelvo-sha256")
	if err != nil || (info.SHA256 != "" && !azureValidSHA256(info.SHA256)) {
		return Info{}, errors.New("Azure snapshot response has invalid digest metadata")
	}
	return info, nil
}

// Conditional publication needs the provider's clock even when an object is
// absent or a competing writer wins. Error bodies stay closed and unread.
func azureErrorInfo(response *http.Response) Info {
	if response == nil {
		return Info{}
	}
	date, err := azureHeader(response.Header, "Date")
	if err != nil {
		return Info{}
	}
	serverTime, err := http.ParseTime(date)
	if err != nil {
		return Info{}
	}
	return Info{ServerTime: serverTime}
}

func azureResponseIdentity(response *http.Response) (Info, error) {
	var info Info
	var err error
	info.Version, err = azureHeader(response.Header, "ETag")
	if err != nil || !azureValidETag(info.Version) {
		return Info{}, errors.New("Azure snapshot response lacks a valid version")
	}
	date, err := azureHeader(response.Header, "Date")
	if err != nil {
		return Info{}, errors.New("Azure snapshot response has an invalid server time")
	}
	info.ServerTime, err = http.ParseTime(date)
	if err != nil {
		return Info{}, errors.New("Azure snapshot response lacks a valid server time")
	}
	return info, nil
}

func azureHeader(headers http.Header, key string) (string, error) {
	values := headers.Values(key)
	if len(values) > 1 {
		return "", errors.New("Azure snapshot response contains duplicate headers")
	}
	value := headers.Get(key)
	if len(value) > 1024 || strings.ContainsAny(value, "\r\n\x00") {
		return "", errors.New("Azure snapshot response contains invalid headers")
	}
	return value, nil
}

func azureValidETag(value string) bool {
	if len(value) < 3 || len(value) > 256 || value[0] != '"' || value[len(value)-1] != '"' {
		return false
	}
	for _, c := range value[1 : len(value)-1] {
		if c < 0x21 || c > 0x7e || c == '"' {
			return false
		}
	}
	return true
}

func azureValidSHA256(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, c := range value {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}

// The parent decides how many bytes to consume. This wrapper keeps errors free
// of credential-bearing URLs and refuses a prematurely terminated object body.
type azureReadBody struct {
	body      io.ReadCloser
	ctx       context.Context
	remaining int64
}

func (r *azureReadBody) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	if len(p) == 0 {
		return 0, nil
	}
	if r.remaining == 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > r.remaining {
		p = p[:r.remaining]
	}
	n, err := r.body.Read(p)
	r.remaining -= int64(n)
	if err != nil {
		if r.ctx.Err() != nil {
			return n, r.ctx.Err()
		}
		if errors.Is(err, io.EOF) {
			if r.remaining != 0 {
				return n, io.ErrUnexpectedEOF
			}
			return n, io.EOF
		}
		return n, errors.New("Azure snapshot response body failed")
	}
	return n, nil
}

func (r *azureReadBody) Close() error {
	if err := r.body.Close(); err != nil {
		return errors.New("Azure snapshot response body could not be closed")
	}
	return nil
}

// The range wrapper validates framing; this inner reader keeps transport errors
// and Close errors from exposing a SAS-bearing URL through custom transports.
type azureSafeBody struct {
	body io.ReadCloser
	ctx  context.Context
}

func (r *azureSafeBody) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	n, err := r.body.Read(p)
	if r.ctx.Err() != nil {
		return n, r.ctx.Err()
	}
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		err = errors.New("Azure snapshot range response body failed")
	}
	return n, err
}

func (r *azureSafeBody) Close() error {
	if err := r.body.Close(); err != nil {
		return errors.New("Azure snapshot range response body could not be closed")
	}
	return nil
}

var _ RangeClient = (*azureClient)(nil)
