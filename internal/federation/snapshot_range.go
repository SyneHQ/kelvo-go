// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package federation

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
)

const snapshotRangeBlock = 64 << 10
const snapshotRangeRequest = int64(32 << 20)

// snapshotRange reads only the exact parent-minted loopback capability. Cloud
// credentials and upstream version checks remain in the parent range bridge.
// Its single read cache shares the query's logical snapshot allocation budget.
type snapshotRange struct {
	ctx       context.Context
	cancel    context.CancelFunc
	client    *http.Client
	transport *http.Transport
	url       string
	size      int64
	digest    string
	memory    *snapshotAllocator
	mu        sync.Mutex
	closed    bool
	cache     []byte
	cacheAt   int64
	cacheSize int
}

func newSnapshotRange(parent context.Context, endpoint string, size int64, digest string, memory *snapshotAllocator) (*snapshotRange, error) {
	if (catalog.ObjectRange{URL: endpoint, Bytes: size}).Validate() != nil || len(digest) != 64 || memory == nil {
		return nil, snapshotUnavailable()
	}
	decoded, err := hex.DecodeString(digest)
	if err != nil || len(decoded) != sha256.Size || hex.EncodeToString(decoded) != digest {
		return nil, snapshotUnavailable()
	}
	if err := parent.Err(); err != nil {
		return nil, err
	}
	address, _ := url.Parse(endpoint)
	ctx, cancel := context.WithCancel(parent)
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	transport := &http.Transport{
		Proxy: nil, DisableCompression: true,
		MaxIdleConns: 1, MaxIdleConnsPerHost: 1, MaxConnsPerHost: 1,
		IdleConnTimeout: 10 * time.Second, ResponseHeaderTimeout: 10 * time.Second,
		MaxResponseHeaderBytes: 8 << 10,
		DialContext: func(ctx context.Context, network, authority string) (net.Conn, error) {
			if network != "tcp" || authority != address.Host {
				return nil, snapshotUnavailable()
			}
			return dialer.DialContext(ctx, "tcp4", address.Host)
		},
	}
	return &snapshotRange{ctx: ctx, cancel: cancel, transport: transport,
		client: &http.Client{Transport: transport, Timeout: 60 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
		url:    endpoint, size: size, digest: digest, memory: memory, cacheAt: -1}, nil
}

// request refuses fallback downloads, redirects, compression and ambiguous or
// changed range identity. Errors never include the private capability URL.
func (r *snapshotRange) request(offset, length int64) (io.ReadCloser, error) {
	if err := r.ctx.Err(); err != nil {
		return nil, err
	}
	if offset < 0 || length < 1 || length > snapshotRangeRequest || offset >= r.size || length > r.size-offset {
		return nil, snapshotUnavailable()
	}
	request, err := http.NewRequestWithContext(r.ctx, http.MethodGet, r.url, nil)
	if err != nil {
		return nil, snapshotUnavailable()
	}
	end := offset + length - 1
	request.Header.Set("Range", "bytes="+strconv.FormatInt(offset, 10)+"-"+strconv.FormatInt(end, 10))
	request.Header.Set("If-Match", `"`+r.digest+`"`)
	request.Header.Set("Accept-Encoding", "identity")
	response, err := r.client.Do(request)
	if err != nil {
		if r.ctx.Err() != nil {
			return nil, r.ctx.Err()
		}
		return nil, snapshotUnavailable()
	}
	exact := func(name, expected string) bool {
		values := response.Header.Values(name)
		return len(values) == 1 && values[0] == expected
	}
	valid := response.StatusCode == http.StatusPartialContent && !response.Uncompressed &&
		len(response.TransferEncoding) == 0 && len(response.Header.Values("Content-Encoding")) == 0 &&
		response.ContentLength == length && exact("Content-Length", strconv.FormatInt(length, 10)) &&
		exact("ETag", `"`+r.digest+`"`) &&
		exact("Content-Range", "bytes "+strconv.FormatInt(offset, 10)+"-"+strconv.FormatInt(end, 10)+"/"+strconv.FormatInt(r.size, 10))
	if !valid {
		_ = response.Body.Close()
		return nil, snapshotUnavailable()
	}
	return response.Body, nil
}

func (r *snapshotRange) readExact(buffer []byte, offset int64) error {
	body, err := r.request(offset, int64(len(buffer)))
	if err != nil {
		return err
	}
	defer body.Close()
	if _, err := io.ReadFull(body, buffer); err != nil {
		return r.readError()
	}
	var extra [1]byte
	if n, err := body.Read(extra[:]); n != 0 || !errors.Is(err, io.EOF) {
		return r.readError()
	}
	return nil
}

func (r *snapshotRange) readError() error {
	if err := r.ctx.Err(); err != nil {
		return err
	}
	return snapshotUnavailable()
}

func (r *snapshotRange) ReadAt(buffer []byte, offset int64) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || offset < 0 {
		return 0, snapshotUnavailable()
	}
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	if len(buffer) == 0 {
		return 0, nil
	}
	if offset >= r.size {
		return 0, io.EOF
	}
	wanted := min(int64(len(buffer)), r.size-offset)
	written := 0
	for int64(written) < wanted {
		at, remaining := offset+int64(written), wanted-int64(written)
		if r.cacheAt <= at && at < r.cacheAt+int64(r.cacheSize) {
			start := int(at - r.cacheAt)
			count := min(int(remaining), r.cacheSize-start)
			copy(buffer[written:written+count], r.cache[start:start+count])
			written += count
			continue
		}
		if remaining >= snapshotRangeBlock {
			count := int(min(remaining, snapshotRangeRequest))
			if err := r.readExact(buffer[written:written+count], at); err != nil {
				return written, err
			}
			written += count
			continue
		}
		if r.cache == nil {
			r.cache = r.memory.Allocate(snapshotRangeBlock)
		}
		r.cacheAt, r.cacheSize = -1, 0
		start := at / snapshotRangeBlock * snapshotRangeBlock
		count := int(min(int64(snapshotRangeBlock), r.size-start))
		if err := r.readExact(r.cache[:count], start); err != nil {
			return written, err
		}
		r.cacheAt, r.cacheSize = start, count
	}
	if written < len(buffer) {
		return written, io.EOF
	}
	return written, nil
}

// verifyDigest streams large, bounded ranges through one 32 KiB buffer. It
// avoids one HTTP request per small io.Copy read and never buffers a full part.
func (r *snapshotRange) verifyDigest() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return snapshotUnavailable()
	}
	buffer := r.memory.Allocate(32 << 10)
	defer r.memory.Free(buffer)
	digest := sha256.New()
	for offset := int64(0); offset < r.size; {
		length := min(snapshotRangeRequest, r.size-offset)
		body, err := r.request(offset, length)
		if err != nil {
			return err
		}
		count, copyErr := io.CopyBuffer(digest, io.LimitReader(body, length+1), buffer)
		closeErr := body.Close()
		if count != length || copyErr != nil || closeErr != nil {
			return r.readError()
		}
		offset += length
	}
	if hex.EncodeToString(digest.Sum(nil)) != r.digest {
		return snapshotUnavailable()
	}
	return r.ctx.Err()
}

func (r *snapshotRange) Close() error {
	r.cancel() // Interrupt an active range before waiting for the reader lock.
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.closed {
		r.closed = true
		if r.cache != nil {
			clear(r.cache)
			r.memory.Free(r.cache)
			r.cache = nil
		}
		r.transport.CloseIdleConnections()
	}
	return nil
}
