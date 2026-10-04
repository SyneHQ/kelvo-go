//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

func exportInternalRequest(method, path string) *http.Request {
	r := httptest.NewRequest(method, path, nil)
	u, _ := url.Parse(GatewayIdentity)
	cert := &x509.Certificate{URIs: []*url.URL{u}}
	r.TLS = &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{cert}}, PeerCertificates: []*x509.Certificate{cert}}
	return r
}

func TestExportRuntimeHTTPRequiresMTLSAndExactClaim(t *testing.T) {
	r, s, _ := openRuntimeExportFixture(t, runtimeExportExecutor(runtimeExportRows))
	id := addRuntimeExport(t, s)
	claimed := claimRuntimeExport(t, s, id)
	for _, kind := range []string{"no-mtls", "no-claim", "duplicate-claim", "wrong-claim", "body"} {
		req := exportInternalRequest(http.MethodPost, "/internal/exports/"+id+"/execute")
		req.Header.Set("X-Kelvo-Claim", claimed.Job.Claim)
		switch kind {
		case "no-mtls":
			req.TLS = nil
		case "no-claim":
			req.Header.Del("X-Kelvo-Claim")
		case "duplicate-claim":
			req.Header.Add("X-Kelvo-Claim", claimed.Job.Claim)
		case "wrong-claim":
			req.Header.Set("X-Kelvo-Claim", "dddddddddddddddddddddddddddddddd")
		case "body":
			req.Body = io.NopCloser(bytes.NewBufferString("{}"))
			req.ContentLength = 2
		}
		response := httptest.NewRecorder()
		if !r.ServeHTTP(response, req) || response.Code < 400 {
			t.Fatal(kind, "was accepted", response.Code)
		}
		current, _ := s.GetExport(context.Background(), id)
		if current.Job.State != ExportClaimed {
			t.Fatal(kind, "executed SQL")
		}
	}
	request := exportInternalRequest(http.MethodPost, "/internal/exports/"+id+"/execute")
	request.Header.Set("X-Kelvo-Claim", claimed.Job.Claim)
	response := httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatal("valid claim failed", response.Code, response.Body.String())
	}
	replay := httptest.NewRecorder()
	r.ServeHTTP(replay, request)
	if replay.Code < 400 {
		t.Fatal("claim was replayed")
	}
}

func TestExportRuntimeHTTPPartRepeatsAndRejectsRangesAndReceiptForgery(t *testing.T) {
	r, s, _ := openRuntimeExportFixture(t, runtimeExportExecutor(runtimeExportRows))
	id := addRuntimeExport(t, s)
	claim := claimRuntimeExport(t, s, id)
	if _, err := r.RunExport(context.Background(), claim); err != nil {
		t.Fatal(err)
	}
	ready := readyRuntimeExport(t, s, id)
	path := "/internal/exports/" + id + "/parts/0"
	for _, kind := range []string{"missing", "duplicate", "wrong", "range", "part"} {
		req := exportInternalRequest(http.MethodGet, path)
		req.Header.Set("X-Kelvo-Export-Receipt", ready.Job.Receipt.ReceiptSHA256)
		switch kind {
		case "missing":
			req.Header.Del("X-Kelvo-Export-Receipt")
		case "duplicate":
			req.Header.Add("X-Kelvo-Export-Receipt", ready.Job.Receipt.ReceiptSHA256)
		case "wrong":
			req.Header.Set("X-Kelvo-Export-Receipt", "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")
		case "range":
			req.Header.Set("Range", "bytes=0-10")
		case "part":
			req.URL.Path = "/internal/exports/" + id + "/parts/01"
		}
		response := httptest.NewRecorder()
		r.ServeHTTP(response, req)
		if response.Code < 400 {
			t.Fatal(kind, "was accepted", response.Code)
		}
	}
	for range 2 {
		req := exportInternalRequest(http.MethodGet, path)
		req.Header.Set("X-Kelvo-Export-Receipt", ready.Job.Receipt.ReceiptSHA256)
		response := httptest.NewRecorder()
		r.ServeHTTP(response, req)
		if response.Code != http.StatusOK || response.Header().Get("Cache-Control") != "no-store" || response.Header().Get("Content-Length") != strconv.Itoa(response.Body.Len()) {
			t.Fatal("invalid download", response.Code, response.Header())
		}
		stream, err := ipc.NewReader(bytes.NewReader(response.Body.Bytes()))
		if err != nil {
			t.Fatal(err)
		}
		if !stream.Next() || stream.RecordBatch().NumRows() != 3 {
			t.Fatal("missing part batch")
		}
		if stream.Next() || stream.Err() != nil {
			t.Fatal("invalid part framing", stream.Err())
		}
		stream.Release()
	}
}

type exportWithdrawWriter struct {
	*httptest.ResponseRecorder
	withdraw func()
	written  bool
}

func (w *exportWithdrawWriter) Write(p []byte) (int, error) {
	n, err := w.ResponseRecorder.Write(p)
	if !w.written {
		w.written = true
		w.withdraw()
	}
	return n, err
}
func TestExportRuntimeHTTPWithholdsEOSAfterDurableWithdrawal(t *testing.T) {
	r, s, _ := openRuntimeExportFixture(t, runtimeExportExecutor(runtimeExportRows))
	id := addRuntimeExport(t, s)
	claim := claimRuntimeExport(t, s, id)
	if _, err := r.RunExport(context.Background(), claim); err != nil {
		t.Fatal(err)
	}
	ready := readyRuntimeExport(t, s, id)
	req := exportInternalRequest(http.MethodGet, "/internal/exports/"+id+"/parts/0")
	req.Header.Set("X-Kelvo-Export-Receipt", ready.Job.Receipt.ReceiptSHA256)
	response := &exportWithdrawWriter{ResponseRecorder: httptest.NewRecorder(), withdraw: func() {
		next := ready.Job
		next.State = ExportCancelled
		next.Error = &query.Error{Code: "CANCELLED", Message: "Export cancelled"}
		if _, err := s.CompareAndSwapExport(context.Background(), ready, next); err != nil {
			t.Fatal(err)
		}
	}}
	var panicValue any
	func() { defer func() { panicValue = recover() }(); r.ServeHTTP(response, req) }()
	if panicValue != http.ErrAbortHandler {
		t.Fatal("withdrawn download did not abort", panicValue)
	}
	if int64(response.Body.Len()) >= ready.Job.Receipt.Manifest.Parts[0].EncodedBytes {
		t.Fatal("withdrawn download released complete Arrow body")
	}
	if r.pool.Snapshot().Active != 0 {
		t.Fatal("aborted download leaked verification admission")
	}
}
