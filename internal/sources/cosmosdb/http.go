// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cosmosdb

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/SYNEHQ/kelvo-go/internal/query"
)

func masterAuthorization(method, resourceType, resource, date string, key []byte) string {
	payload := strings.ToLower(method) + "\n" + strings.ToLower(resourceType) + "\n" + resource + "\n" + strings.ToLower(date) + "\n\n"
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(payload))
	return url.QueryEscape("type=master&ver=1.0&sig=" + base64.StdEncoding.EncodeToString(mac.Sum(nil)))
}

// query uses the shared client's verified TLS, no-proxy and no-redirect policy,
// retaining response headers required by Cosmos pagination and RU accounting.
func (e *Engine) query(ctx context.Context, body []byte, pageSize int64, continuation, sessionToken string, out *queryPage) (http.Header, int64, error) {
	u := *e.client.Origin
	u.Path = "/" + e.resource + "/docs"
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(body))
	if err != nil {
		return nil, 0, query.NewError("QUERY_FAILED", "Cannot prepare Cosmos DB query")
	}
	date := e.now().UTC().Format(http.TimeFormat)
	authorization := url.QueryEscape("type=aad&ver=1.0&sig=" + e.client.Token)
	if e.source.Options["auth"] == "master_key" {
		authorization = masterAuthorization(http.MethodPost, "docs", e.resource, date, e.key)
	}
	r.Header.Set("Authorization", authorization)
	r.Header.Set("Accept", "application/json")
	r.Header.Set("Content-Type", "application/query+json")
	r.Header.Set("x-ms-date", date)
	r.Header.Set("x-ms-version", "2018-12-31")
	r.Header.Set("x-ms-documentdb-isquery", "True")
	r.Header.Set("x-ms-documentdb-query-enablecrosspartition", "True")
	r.Header.Set("x-ms-max-item-count", strconv.FormatInt(pageSize, 10))
	if continuation != "" {
		r.Header.Set("x-ms-continuation", continuation)
	}
	if sessionToken != "" {
		r.Header.Set("x-ms-session-token", sessionToken)
	}
	response, err := e.client.HTTP.Do(r)
	if err != nil {
		if ctx.Err() != nil {
			return nil, 0, ctx.Err()
		}
		return nil, 0, query.NewError("QUERY_FAILED", "Cosmos DB request failed")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		code := "QUERY_FAILED"
		if response.StatusCode == http.StatusTooManyRequests {
			code = "RESOURCE_EXHAUSTED"
		}
		return nil, 0, query.NewError(code, "Cosmos DB rejected the query")
	}
	mediaType, _, parseErr := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if parseErr != nil || mediaType != "application/json" {
		return nil, 0, query.NewError("QUERY_FAILED", "Cosmos DB returned an invalid content type")
	}
	wire := &countReader{Reader: response.Body}
	var reader io.Reader = wire
	switch response.Header.Get("Content-Encoding") {
	case "", "identity":
	case "gzip":
		compressed, gzipErr := gzip.NewReader(wire)
		if gzipErr != nil {
			return nil, wire.n, query.NewError("QUERY_FAILED", "Invalid compressed Cosmos DB response")
		}
		defer compressed.Close()
		reader = compressed
	default:
		return nil, wire.n, query.NewError("QUERY_FAILED", "Unsupported Cosmos DB response encoding")
	}
	data, err := io.ReadAll(io.LimitReader(reader, e.client.Limit+1))
	if err != nil {
		if ctx.Err() != nil {
			return nil, wire.n, ctx.Err()
		}
		return nil, wire.n, query.NewError("QUERY_FAILED", "Cosmos DB returned an incomplete response")
	}
	if int64(len(data)) > e.client.Limit {
		return nil, wire.n, query.NewError("RESOURCE_EXHAUSTED", "Cosmos DB response exceeds its memory budget")
	}
	if !utf8.Valid(data) {
		return nil, wire.n, query.NewError("QUERY_FAILED", "Cosmos DB returned invalid UTF-8")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	if decoder.Decode(out) != nil || decoder.Decode(new(any)) != io.EOF {
		return nil, wire.n, query.NewError("QUERY_FAILED", "Cosmos DB returned invalid JSON")
	}
	return response.Header, wire.n, nil
}

func singleHeader(headers http.Header, name string) (string, error) {
	values := headers.Values(name)
	if len(values) > 1 {
		return "", query.NewError("QUERY_FAILED", "Cosmos DB returned duplicate pagination or budget headers")
	}
	value := headers.Get(name)
	if len(value) > 16<<10 {
		return "", query.NewError("QUERY_FAILED", "Cosmos DB response header exceeds its limit")
	}
	for _, char := range value {
		if char < 0x20 || char > 0x7e {
			return "", query.NewError("QUERY_FAILED", "Cosmos DB returned an invalid response header")
		}
	}
	return value, nil
}

type countReader struct {
	io.Reader
	n int64
}

func (r *countReader) Read(buffer []byte) (int, error) {
	n, err := r.Reader.Read(buffer)
	r.n += int64(n)
	return n, err
}
