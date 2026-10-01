// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package objectstore

import (
	"errors"
	"io"
	"net/http"
	"regexp"
	"strconv"
)

var contentRangePattern = regexp.MustCompile(`^bytes ([0-9]+)-([0-9]+)/([0-9]+)$`)

// ValidateRangeResponse validates an exact, unencoded partial response and
// returns the complete object size. It deliberately rejects unknown totals.
func ValidateRangeResponse(resp *http.Response, offset, length int64) (int64, error) {
	bad := errors.New("object storage returned an invalid byte range")
	if offset < 0 || length <= 0 || offset >= MaxUploadBytes || length > MaxUploadBytes-offset || resp.StatusCode != http.StatusPartialContent || resp.ContentLength != length || len(resp.Header.Values("Content-Range")) != 1 || len(resp.Header.Values("Content-Encoding")) > 1 {
		return 0, bad
	}
	if encoding := resp.Header.Get("Content-Encoding"); encoding != "" && encoding != "identity" {
		return 0, bad
	}
	m := contentRangePattern.FindStringSubmatch(resp.Header.Get("Content-Range"))
	if m == nil {
		return 0, bad
	}
	start, e1 := strconv.ParseInt(m[1], 10, 64)
	end, e2 := strconv.ParseInt(m[2], 10, 64)
	total, e3 := strconv.ParseInt(m[3], 10, 64)
	if e1 != nil || e2 != nil || e3 != nil || start != offset || end != offset+length-1 || total <= end || total > MaxUploadBytes {
		return 0, bad
	}
	return total, nil
}

// ExactRangeBody detects truncation and oversized response bodies even with
// custom transports. Consumers must read through EOF to validate completeness.
func ExactRangeBody(body io.ReadCloser, length int64) io.ReadCloser {
	return &exactRangeBody{body: body, remaining: length}
}

type exactRangeBody struct {
	body      io.ReadCloser
	remaining int64
	finished  bool
}

func (r *exactRangeBody) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if r.finished {
		return 0, io.EOF
	}
	if r.remaining == 0 {
		var extra [1]byte
		n, err := r.body.Read(extra[:])
		if n > 0 {
			r.finished = true
			return 0, errors.New("object storage exceeded requested byte range")
		}
		if err != nil {
			r.finished = true
		}
		return 0, err
	}
	if int64(len(p)) > r.remaining {
		p = p[:r.remaining]
	}
	n, err := r.body.Read(p)
	r.remaining -= int64(n)
	if err == io.EOF && r.remaining > 0 {
		r.finished = true
		err = io.ErrUnexpectedEOF
	}
	return n, err
}

func (r *exactRangeBody) Close() error { return r.body.Close() }
