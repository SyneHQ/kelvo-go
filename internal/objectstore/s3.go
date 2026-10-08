// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package objectstore

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

type s3Client struct {
	lifetime    clientLifetime
	location    catalog.ObjectLocation
	origin      *url.URL
	http        *http.Client
	closeIdle   func()
	credentials aws.Credentials
	signer      *v4.Signer
}

var digestPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)
var generationPattern = regexp.MustCompile(`^[1-9][0-9]{0,30}$`)

func newS3(location catalog.ObjectLocation, credentials catalog.ObjectCredentials) (Client, error) {
	return newS3WithHTTP(location, credentials, nil)
}

func newS3WithHTTP(location catalog.ObjectLocation, credentials catalog.ObjectCredentials, client *http.Client) (Client, error) {
	id, secret, token := os.Getenv(credentials.AccessKeyIDEnv), os.Getenv(credentials.SecretAccessKeyEnv), os.Getenv(credentials.SessionTokenEnv)
	if id == "" || secret == "" || (credentials.SessionTokenEnv != "" && token == "") || len(id) > 1024 || len(secret) > 4096 || len(token) > 16384 || strings.ContainsAny(id+secret+token, "\r\n\x00") {
		return nil, errors.New("object storage credentials are unavailable")
	}
	origin, err := url.Parse(location.Endpoint)
	if err != nil {
		return nil, errors.New("object storage endpoint is invalid")
	}
	closeIdle := func() {}
	if client == nil {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.Proxy = nil
		transport.DisableCompression = true
		transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
		transport.TLSHandshakeTimeout = 10 * time.Second
		transport.ResponseHeaderTimeout = 30 * time.Second
		transport.MaxConnsPerHost = 4
		transport.MaxResponseHeaderBytes = 64 << 10
		client = &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
		closeIdle = client.CloseIdleConnections
	}
	return &s3Client{location: location, origin: origin, http: client, closeIdle: closeIdle, credentials: aws.Credentials{AccessKeyID: id, SecretAccessKey: secret, SessionToken: token}, signer: v4.NewSigner()}, nil
}

func (c *s3Client) Close() {
	closeIdle := c.closeIdle
	if closeIdle == nil {
		closeIdle = c.http.CloseIdleConnections
	}
	c.lifetime.close(closeIdle)
}

func (c *s3Client) Get(ctx context.Context, key, version string) (io.ReadCloser, Info, error) {
	op, err := c.lifetime.begin(ctx)
	if err != nil {
		return nil, Info{}, err
	}
	defer op.finish(true)
	ctx = op.ctx
	r, err := c.request(ctx, http.MethodGet, key, nil, 0, "", Condition{})
	if err != nil {
		return nil, Info{}, err
	}
	if err = c.readCondition(r, version); err != nil {
		return nil, Info{}, err
	}
	resp, err := c.do(op, r, "")
	if err != nil {
		return nil, Info{}, err
	}
	info, err := c.readInfo(resp, false)
	if err != nil {
		resp.Body.Close()
		return nil, info, err
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, info, errors.New("object storage returned an unsolicited partial response")
	}
	if version != "" && info.Version != version {
		resp.Body.Close()
		return nil, info, ErrConflict
	}
	body, err := op.returnBody(resp.Body)
	return body, info, err
}

// GetRange requires an immutable version and a closed byte interval. It never
// falls back to a full-object response when an endpoint ignores Range.
func (c *s3Client) GetRange(ctx context.Context, key, version string, offset, length int64) (io.ReadCloser, Info, error) {
	op, err := c.lifetime.begin(ctx)
	if err != nil {
		return nil, Info{}, err
	}
	defer op.finish(true)
	ctx = op.ctx
	if version == "" || offset < 0 || length <= 0 || offset >= MaxUploadBytes || length > MaxUploadBytes-offset {
		return nil, Info{}, errors.New("object range requires a version and a bounded interval")
	}
	r, err := c.request(ctx, http.MethodGet, key, nil, 0, "", Condition{})
	if err != nil {
		return nil, Info{}, err
	}
	if err = c.readCondition(r, version); err != nil {
		return nil, Info{}, err
	}
	r.Header.Set("Range", "bytes="+strconv.FormatInt(offset, 10)+"-"+strconv.FormatInt(offset+length-1, 10))
	resp, err := c.do(op, r, "")
	if err != nil {
		return nil, Info{}, err
	}
	info, err := c.readInfo(resp, false)
	if err != nil {
		resp.Body.Close()
		return nil, info, err
	}
	if resp.StatusCode != http.StatusPartialContent {
		resp.Body.Close()
		return nil, info, errors.New("object storage ignored byte range")
	}
	total, err := ValidateRangeResponse(resp, offset, length)
	if err != nil {
		resp.Body.Close()
		return nil, info, err
	}
	info.Size = total
	if info.Version != version {
		resp.Body.Close()
		return nil, info, ErrConflict
	}
	body, err := op.returnBody(ExactRangeBody(resp.Body, length))
	return body, info, err
}

func (c *s3Client) Head(ctx context.Context, key, version string) (Info, error) {
	op, err := c.lifetime.begin(ctx)
	if err != nil {
		return Info{}, err
	}
	defer op.finish(true)
	ctx = op.ctx
	r, err := c.request(ctx, http.MethodHead, key, nil, 0, "", Condition{})
	if err != nil {
		return Info{}, err
	}
	if err = c.readCondition(r, version); err != nil {
		return Info{}, err
	}
	resp, err := c.do(op, r, "")
	if err != nil {
		return Info{}, err
	}
	defer resp.Body.Close()
	info, err := c.readInfo(resp, false)
	if err == nil && resp.StatusCode != http.StatusOK {
		return info, errors.New("object storage returned an unsolicited partial response")
	}
	if err == nil && version != "" && info.Version != version {
		return info, ErrConflict
	}
	return info, err
}

func (c *s3Client) Put(ctx context.Context, key string, body io.ReadSeeker, size int64, digest string, condition Condition) (Info, error) {
	op, err := c.lifetime.begin(ctx)
	if err != nil {
		return Info{}, err
	}
	defer op.finish(true)
	ctx = op.ctx
	if body == nil || size < 0 || size > MaxUploadBytes || !digestPattern.MatchString(digest) || condition.Absent == (condition.Version != "") {
		return Info{}, errors.New("object upload requires a bounded body, SHA256 and one precondition")
	}
	if n, err := body.Seek(0, io.SeekEnd); err != nil || n != size {
		return Info{}, errors.New("object upload body size is invalid")
	}
	if _, err := body.Seek(0, io.SeekStart); err != nil {
		return Info{}, errors.New("object upload body is unavailable")
	}
	r, err := c.request(ctx, http.MethodPut, key, nil, size, digest, condition)
	if err != nil {
		return Info{}, err
	}
	if size == 0 {
		r.Body = http.NoBody
	} else {
		upload := newUploadBody(body, size)
		r.Body = upload
		defer upload.wait()
	}
	resp, err := c.do(op, r, digest)
	if err != nil {
		return Info{}, err
	}
	defer resp.Body.Close()
	info, err := c.readInfo(resp, true)
	if err != nil {
		return info, err
	}
	info.Size, info.SHA256 = size, digest
	return info, nil
}

func (c *s3Client) request(ctx context.Context, method, key string, body io.Reader, size int64, digest string, cond Condition) (*http.Request, error) {
	if err := c.location.ValidateKey(key); err != nil {
		return nil, err
	}
	u := *c.origin
	u.Path = "/" + c.location.Bucket + "/" + key
	r, err := http.NewRequestWithContext(ctx, method, u.String(), body)
	if err != nil {
		return nil, errors.New("object request could not be prepared")
	}
	if method == http.MethodPut {
		r.ContentLength = size
		r.Header.Set("Content-Type", "application/octet-stream")
		if c.location.Provider == "gcs" {
			r.Header.Set("x-goog-meta-kelvo-sha256", digest)
			if cond.Absent {
				r.Header.Set("x-goog-if-generation-match", "0")
			} else if generationPattern.MatchString(cond.Version) {
				r.Header.Set("x-goog-if-generation-match", cond.Version)
			} else {
				return nil, errors.New("GCS write condition is invalid")
			}
		} else {
			r.Header.Set("x-amz-meta-kelvo-sha256", digest)
			if cond.Absent {
				r.Header.Set("If-None-Match", "*")
			} else if validETag(cond.Version) {
				r.Header.Set("If-Match", cond.Version)
			} else {
				return nil, errors.New("object write condition is invalid")
			}
		}
	}
	return r, nil
}

func (c *s3Client) readCondition(r *http.Request, version string) error {
	if version == "" {
		return nil
	}
	if c.location.Provider == "gcs" {
		if !generationPattern.MatchString(version) {
			return errors.New("GCS read condition is invalid")
		}
		r.Header.Set("x-goog-if-generation-match", version)
	} else {
		if !validETag(version) {
			return errors.New("object read condition is invalid")
		}
		r.Header.Set("If-Match", version)
	}
	return nil
}

func (c *s3Client) do(op *clientOperation, r *http.Request, digest string) (*http.Response, error) {
	if digest == "" {
		empty := sha256.Sum256(nil)
		digest = hex.EncodeToString(empty[:])
	}
	r.Header.Set("x-amz-content-sha256", digest)
	region := c.location.Region
	if region == "" {
		region = "auto"
	}
	if err := c.signer.SignHTTP(r.Context(), c.credentials, r, digest, "s3", region, time.Now(), func(o *v4.SignerOptions) { o.DisableURIPathEscaping = true }); err != nil {
		// HTTP has not taken ownership. In particular, Put must not wait for a
		// transport Close that cannot arrive after request signing fails.
		if r.Body != nil {
			_ = r.Body.Close()
		}
		return nil, errors.New("object request signing failed")
	}
	resp, err := c.http.Do(r)
	if err != nil {
		return nil, op.transportError(err, errors.New("object storage request failed"))
	}
	return resp, nil
}

func (c *s3Client) readInfo(resp *http.Response, upload bool) (Info, error) {
	info := Info{}
	headers, err := strictS3ResponseHeaders(resp.Header)
	if err != nil {
		return info, err
	}
	// Custom transports may use noncanonical map keys. Keep a private canonical
	// copy so all later consumers, including range validation, see the same
	// unambiguous fields without mutating the transport's original header map.
	resp.Header = headers
	info.ServerTime, _ = http.ParseTime(resp.Header.Get("Date"))
	switch resp.StatusCode {
	case http.StatusNotFound:
		return info, ErrNotFound
	case http.StatusPreconditionFailed, http.StatusConflict:
		return info, ErrConflict
	}
	if resp.StatusCode != http.StatusOK && !(!upload && resp.StatusCode == http.StatusPartialContent) && !(upload && resp.StatusCode == http.StatusCreated) {
		return info, errors.New("object storage rejected request")
	}
	if info.ServerTime.IsZero() {
		return info, errors.New("object storage returned an invalid server date")
	}
	if encoding := resp.Header.Get("Content-Encoding"); encoding != "" && encoding != "identity" {
		return info, errors.New("object storage returned an unsupported encoding")
	}
	if !upload && (resp.ContentLength < 0 || resp.ContentLength > MaxUploadBytes) {
		return info, errors.New("object storage returned an invalid length")
	}
	if values, present := resp.Header["Content-Length"]; present {
		length, err := strconv.ParseUint(values[0], 10, 63)
		if err != nil || resp.ContentLength < 0 || int64(length) != resp.ContentLength {
			return info, errors.New("object storage returned an invalid length")
		}
	}
	info.Size = resp.ContentLength
	if c.location.Provider == "gcs" {
		info.Version = resp.Header.Get("x-goog-generation")
		info.SHA256 = resp.Header.Get("x-goog-meta-kelvo-sha256")
		if !generationPattern.MatchString(info.Version) {
			return info, errors.New("GCS response lacks a valid generation")
		}
	} else {
		info.Version = resp.Header.Get("ETag")
		info.SHA256 = resp.Header.Get("x-amz-meta-kelvo-sha256")
		if !validETag(info.Version) {
			return info, errors.New("object response lacks a valid ETag")
		}
	}
	if info.SHA256 != "" && !digestPattern.MatchString(info.SHA256) {
		return info, errors.New("object storage returned invalid digest metadata")
	}
	return info, nil
}

// Identity, integrity and framing fields have singleton semantics. Header.Get
// alone accepts duplicates and can overlook mixed-case keys from an explicit
// transport. Reject even identical duplicates before trusting a provider clock,
// version, digest or length; other HTTP fields retain their normal multiplicity.
func strictS3ResponseHeaders(headers http.Header) (http.Header, error) {
	canonical := make(http.Header, len(headers))
	for name, values := range headers {
		key := http.CanonicalHeaderKey(name)
		switch key {
		case "Date", "Content-Length", "Content-Encoding", "Content-Range", "Etag",
			"X-Amz-Meta-Kelvo-Sha256", "X-Goog-Generation", "X-Goog-Meta-Kelvo-Sha256":
			if len(values) != 1 || len(canonical[key]) != 0 || len(values[0]) > 1024 || strings.ContainsAny(values[0], "\r\n\x00") {
				return nil, errors.New("object storage returned invalid or duplicate response headers")
			}
		}
		canonical[key] = append(canonical[key], values...)
	}
	return canonical, nil
}

func validETag(tag string) bool {
	if len(tag) < 3 || len(tag) > 256 || tag[0] != '"' || tag[len(tag)-1] != '"' {
		return false
	}
	for _, r := range tag[1 : len(tag)-1] {
		if r < 33 || r > 126 || r == '"' {
			return false
		}
	}
	return true
}
