// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package objectstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
)

type uploadTransport func(*http.Request) (*http.Response, error)

func (f uploadTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func uploadClient(t *testing.T, provider, endpoint string, transport http.RoundTripper) Client {
	t.Helper()
	location := catalog.ObjectLocation{Provider: provider, Endpoint: endpoint, Bucket: "snapshots", Prefix: "kelvo"}
	credentials := catalog.ObjectCredentials{AccessKeyIDEnv: "KELVO_SOURCE_UPLOAD_ID", SecretAccessKeyEnv: "KELVO_SOURCE_UPLOAD_SECRET"}
	t.Setenv(credentials.AccessKeyIDEnv, "fixture-id")
	t.Setenv(credentials.SecretAccessKeyEnv, "fixture-secret")
	if provider == "s3" {
		location.Region = "us-east-1"
	}
	if provider == "azure" {
		location.Account = "kelvofixture"
		credentials = catalog.ObjectCredentials{SASTokenEnv: "KELVO_SOURCE_UPLOAD_SAS"}
		t.Setenv(credentials.SASTokenEnv, azureTestSAS())
	}
	client, err := New(location, credentials)
	if err != nil {
		t.Fatal(err)
	}
	switch c := client.(type) {
	case *s3Client:
		c.http.Transport = transport
	case *azureClient:
		c.http.Transport = transport
	default:
		t.Fatal("upload fixture selected an unexpected provider")
	}
	t.Cleanup(client.Close)
	return client
}

func uploadHeaders(provider string) http.Header {
	headers := http.Header{"Date": {azureTestDate}, "Content-Length": {"0"}, "Etag": {`"v1"`}}
	if provider == "gcs" {
		headers.Set("x-goog-generation", "1")
	}
	return headers
}

type uploadResponseBody struct {
	closed chan struct{}
	once   sync.Once
}

func (*uploadResponseBody) Read([]byte) (int, error) { return 0, io.EOF }
func (b *uploadResponseBody) Close() error {
	b.once.Do(func() { close(b.closed) })
	return nil
}

type uploadSeekFailure struct {
	*uploadTestReader
	whence int
}

func (r uploadSeekFailure) Seek(offset int64, whence int) (int64, error) {
	if whence == r.whence {
		return 0, errors.New("fixture seek failed")
	}
	return r.Reader.Seek(offset, whence)
}

func TestProviderPutJoinsTransportBodyOwnership(t *testing.T) {
	for _, provider := range []string{"s3", "r2", "gcs", "azure"} {
		for _, scenario := range []string{"delayed-close", "active-read", "do-error", "cancel", "rejected", "invalid-response", "redirect"} {
			t.Run(provider+"/"+scenario, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				t.Cleanup(cancel)
				proceed, closeAllowed := make(chan struct{}), make(chan struct{})
				var readOnce, closeOnce sync.Once
				releaseRead := func() { readOnce.Do(func() { close(proceed) }) }
				releaseClose := func() { closeOnce.Do(func() { close(closeAllowed) }) }
				t.Cleanup(releaseRead)
				t.Cleanup(releaseClose)
				reader := &uploadTestReader{Reader: bytes.NewReader([]byte("payload"))}
				if scenario != "delayed-close" {
					reader.entered, reader.proceed = make(chan struct{}), proceed
				}
				readDone, closeDone, returned := make(chan struct{}), make(chan struct{}), make(chan struct{})
				response := &uploadResponseBody{closed: make(chan struct{})}
				var calls atomic.Int32
				var requestBody io.ReadCloser
				client := uploadClient(t, provider, "https://objects.example.test", uploadTransport(func(r *http.Request) (*http.Response, error) {
					calls.Add(1)
					requestBody = r.Body
					if r.GetBody != nil || r.ContentLength != 7 {
						t.Error("upload gained replay support or changed its fixed size")
					}
					go func() {
						defer close(readDone)
						_, _ = io.ReadAll(r.Body)
					}()
					if scenario == "delayed-close" {
						<-readDone
					} else {
						<-reader.entered
					}
					go func() {
						defer close(closeDone)
						<-closeAllowed
						_ = r.Body.Close()
					}()
					if scenario != "delayed-close" {
						releaseClose()
						<-closeDone
					}
					if scenario == "cancel" {
						<-ctx.Done()
					}
					defer close(returned)
					if scenario == "do-error" || scenario == "cancel" {
						return nil, errors.New("fixture transport failed")
					}
					status, headers := http.StatusCreated, uploadHeaders(provider)
					switch scenario {
					case "rejected":
						status = http.StatusPreconditionFailed
					case "invalid-response":
						headers.Del("Date")
					case "redirect":
						status = http.StatusTemporaryRedirect
						headers.Set("Location", "https://redirect.example.test/object")
					}
					return &http.Response{StatusCode: status, Header: headers, Body: response, ContentLength: 0, Request: r}, nil
				}))
				finished := make(chan struct{})
				t.Cleanup(func() {
					cancel()
					releaseRead()
					releaseClose()
					uploadCleanupJoins(t, finished, readDone, closeDone)
				})
				var putErr error
				go func() {
					_, putErr = client.Put(ctx, "kelvo/data", reader, 7, azureTestDigest("payload"), Condition{Absent: true})
					close(finished)
				}()
				if scenario == "cancel" {
					uploadWait(t, closeDone)
					cancel()
				}
				uploadWait(t, returned)
				if scenario != "do-error" && scenario != "cancel" {
					// A wait inside do or before response Close would deadlock here.
					uploadWait(t, response.closed)
				}
				uploadPending(t, finished)
				releaseRead()
				releaseClose()
				uploadWait(t, finished)
				uploadWait(t, readDone)
				uploadWait(t, closeDone)
				wantSuccess := scenario == "delayed-close" || scenario == "active-read"
				if (putErr == nil) != wantSuccess {
					t.Fatalf("unexpected Put result: %v", putErr)
				}
				if scenario == "cancel" && !errors.Is(putErr, context.Canceled) {
					t.Fatalf("cancellation was not preserved: %v", putErr)
				}
				before := reader.reads.Load()
				if _, err := requestBody.Read(make([]byte, 1)); !errors.Is(err, io.ErrClosedPipe) || reader.reads.Load() != before || reader.closes.Load() != 0 || calls.Load() != 1 {
					t.Fatal("Put returned a reusable transport body, closed the caller or followed a redirect")
				}
			})
		}
	}
}

func TestProviderPutZeroBodyAndPreHandoffFailures(t *testing.T) {
	for _, provider := range []string{"s3", "r2", "gcs", "azure"} {
		t.Run(provider, func(t *testing.T) {
			var calls atomic.Int32
			client := uploadClient(t, provider, "https://objects.example.test", uploadTransport(func(r *http.Request) (*http.Response, error) {
				calls.Add(1)
				if r.Body != http.NoBody || r.ContentLength != 0 || r.GetBody != nil {
					t.Error("empty upload framing changed")
				}
				return &http.Response{StatusCode: http.StatusCreated, Header: uploadHeaders(provider), Body: http.NoBody, ContentLength: 0, Request: r}, nil
			}))
			reader := &uploadTestReader{Reader: bytes.NewReader(nil)}
			if _, err := client.Put(context.Background(), "kelvo/data", reader, 0, azureTestDigest(""), Condition{Absent: true}); err != nil {
				t.Fatal(err)
			}
			for _, key := range []string{"kelvo/data", "outside/data"} {
				if _, err := client.Put(context.Background(), key, reader, 1, azureTestDigest("x"), Condition{Absent: true}); err == nil {
					t.Fatal("invalid upload reached HTTP")
				}
			}
			if _, err := client.Put(context.Background(), "outside/data", reader, 0, azureTestDigest(""), Condition{Absent: true}); err == nil {
				t.Fatal("invalid request key reached HTTP")
			}
			for _, whence := range []int{io.SeekStart, io.SeekEnd} {
				failed := uploadSeekFailure{uploadTestReader: reader, whence: whence}
				if _, err := client.Put(context.Background(), "kelvo/data", failed, 0, azureTestDigest(""), Condition{Absent: true}); err == nil {
					t.Fatal("failed upload seek reached HTTP")
				}
			}
			if calls.Load() != 1 || reader.closes.Load() != 0 || reader.reads.Load() != 0 {
				t.Fatal("pre-handoff rejection accessed the upload or transport")
			}
		})
	}
}

func TestProviderPutRealTransportEarlyResponse(t *testing.T) {
	for _, provider := range []string{"s3", "r2", "gcs", "azure"} {
		t.Run(provider, func(t *testing.T) {
			proceed, sent := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(proceed) }) }
			reader := &uploadTestReader{Reader: bytes.NewReader([]byte(strings.Repeat("x", 1024))), entered: make(chan struct{}), proceed: proceed}
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				<-reader.entered
				w.Header().Set("Connection", "close")
				w.WriteHeader(http.StatusPreconditionFailed)
				w.(http.Flusher).Flush()
				close(sent)
			}))
			t.Cleanup(server.Close)
			t.Cleanup(unblock)
			client := uploadClient(t, provider, server.URL, server.Client().Transport)
			finished := make(chan struct{})
			t.Cleanup(func() { unblock(); uploadCleanupJoins(t, finished) })
			var putErr error
			go func() {
				_, putErr = client.Put(context.Background(), "kelvo/data", reader, 1024, azureTestDigest(strings.Repeat("x", 1024)), Condition{Absent: true})
				close(finished)
			}()
			uploadWait(t, sent)
			uploadPending(t, finished)
			unblock()
			uploadWait(t, finished)
			if !errors.Is(putErr, ErrConflict) || reader.closes.Load() != 0 {
				t.Fatalf("early response lost conflict or caller ownership: %v", putErr)
			}
		})
	}
}
