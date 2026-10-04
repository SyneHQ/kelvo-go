// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package objectstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func clientResponse(provider, method string, body io.ReadCloser, request *http.Request) *http.Response {
	headers := uploadHeaders(provider)
	headers.Set("Content-Length", "4")
	status := http.StatusOK
	if method == "range" {
		status = http.StatusPartialContent
		headers.Set("Content-Range", "bytes 0-3/4")
	} else if method == "put" {
		status = http.StatusCreated
	}
	return &http.Response{StatusCode: status, Header: headers, Body: body, ContentLength: 4, Request: request}
}

func clientInvoke(client Client, method string, ctx context.Context, provider string) (io.ReadCloser, error) {
	version := `"v1"`
	if provider == "gcs" {
		version = "1"
	}
	switch method {
	case "range":
		body, _, err := client.(RangeClient).GetRange(ctx, "kelvo/data", version, 0, 4)
		return body, err
	case "head":
		_, err := client.Head(ctx, "kelvo/data", version)
		return nil, err
	case "put":
		_, err := client.Put(ctx, "kelvo/data", bytes.NewReader(nil), 0, azureTestDigest(""), Condition{Absent: true})
		return nil, err
	default:
		body, _, err := client.Get(ctx, "kelvo/data", version)
		return body, err
	}
}

// This also compiles when only s3.go and azure.go are restored to the preceding
// implementation, allowing a controlled negative run of s3/get/unread.
func TestProviderClientCloseJoinsReturnedBodies(t *testing.T) {
	for _, provider := range []string{"s3", "r2", "gcs", "azure"} {
		for _, method := range []string{"get", "range"} {
			for _, reading := range []bool{false, true} {
				mode := "unread"
				if reading {
					mode = "reading"
				}
				t.Run(provider+"/"+method+"/"+mode, func(t *testing.T) {
					upstream := newClientTestBody()
					client := uploadClient(t, provider, "https://objects.example.test", uploadTransport(func(r *http.Request) (*http.Response, error) {
						return clientResponse(provider, method, upstream, r), nil
					}))
					body, err := clientInvoke(client, method, context.Background(), provider)
					if err != nil {
						upstream.unblock()
						t.Fatal(err)
					}
					var joins []<-chan struct{}
					t.Cleanup(func() { upstream.unblock(); _ = body.Close(); uploadCleanupJoins(t, joins...) })
					if reading {
						read := make(chan struct{})
						joins = append(joins, read)
						go func() { defer close(read); _, _ = body.Read(make([]byte, 4)) }()
						clientWait(t, upstream.readEntered.ready)
						managed, ok := body.(*clientBody)
						if !ok {
							t.Fatal("provider returned an unowned response body")
						}
						queued := make(chan struct{})
						joins = append(joins, queued)
						go func() {
							defer close(queued)
							if _, err := body.Read(make([]byte, 1)); !errors.Is(err, io.ErrClosedPipe) {
								t.Error("queued provider read bypassed body sealing")
							}
						}()
						clientEventually(t, func() bool { managed.mu.Lock(); defer managed.mu.Unlock(); return managed.reads == 2 })
					}
					closed, second := make(chan struct{}), make(chan struct{})
					joins = append(joins, closed)
					go func() { client.Close(); close(closed) }()
					select {
					case <-upstream.closeEntered.ready:
					case <-closed:
						t.Fatal("object client returned before response ownership ended")
					case <-time.After(3 * time.Second):
						t.Fatal("client Close did not initiate response closure")
					}
					joins = append(joins, second)
					go func() { client.Close(); close(second) }()
					clientPending(t, closed)
					clientPending(t, second)
					upstream.closeAllowed.open()
					clientWait(t, upstream.closeDone.ready)
					if reading {
						clientPending(t, closed)
					}
					upstream.readAllowed.open()
					clientWait(t, closed)
					clientWait(t, second)
					if _, err := body.Read(make([]byte, 1)); !errors.Is(err, io.ErrClosedPipe) || upstream.closes.Load() != 1 || (reading && upstream.reads.Load() != 1) {
						t.Fatal("closed provider still exposed its response reader")
					}
				})
			}
		}
	}
}

func TestProviderClientCloseCancelsAllReturnedBodiesBeforeJoining(t *testing.T) {
	for _, provider := range []string{"s3", "r2", "gcs", "azure"} {
		t.Run(provider, func(t *testing.T) {
			upstreams := []*clientTestBody{newClientTestBody(), newClientTestBody()}
			var index atomic.Int32
			client := uploadClient(t, provider, "https://objects.example.test", uploadTransport(func(r *http.Request) (*http.Response, error) {
				return clientResponse(provider, "get", upstreams[index.Add(1)-1], r), nil
			}))
			closed := make(chan struct{})
			var joins []<-chan struct{}
			var bodies []io.ReadCloser
			t.Cleanup(func() {
				for _, upstream := range upstreams {
					upstream.unblock()
				}
				for _, body := range bodies {
					_ = body.Close()
				}
				uploadCleanupJoins(t, joins...)
			})
			for range 2 {
				body, err := clientInvoke(client, "get", context.Background(), provider)
				if err != nil {
					t.Fatal(err)
				}
				bodies = append(bodies, body)
			}
			joins = append(joins, closed)
			go func() { client.Close(); close(closed) }()
			for _, upstream := range upstreams {
				clientWait(t, upstream.closeEntered.ready)
			}
			clientPending(t, closed)
			for _, upstream := range upstreams {
				upstream.unblock()
			}
			clientWait(t, closed)
		})
	}
}

func TestProviderClientCloseJoinsLateRequestsAndErrorBodies(t *testing.T) {
	for _, provider := range []string{"s3", "r2", "gcs", "azure"} {
		for _, method := range []string{"get", "range", "head", "put"} {
			for _, invalid := range []bool{false, true} {
				mode := "valid"
				if invalid {
					mode = "invalid"
				}
				t.Run(provider+"/"+method+"/"+mode, func(t *testing.T) {
					upstream := newClientTestBody()
					entered, allowed := newClientGate(), newClientGate()
					var requestContext context.Context
					client := uploadClient(t, provider, "https://objects.example.test", uploadTransport(func(r *http.Request) (*http.Response, error) {
						requestContext = r.Context()
						entered.open()
						<-allowed.ready // Deliberately return a response after cancellation.
						response := clientResponse(provider, method, upstream, r)
						if invalid {
							response.Header.Del("Date")
						}
						return response, nil
					}))
					returned, closed := make(chan struct{}), make(chan struct{})
					var resultBody io.ReadCloser
					var resultErr error
					var joins []<-chan struct{}
					t.Cleanup(func() {
						allowed.open()
						upstream.unblock()
						uploadCleanupJoins(t, joins...)
						if resultBody != nil {
							_ = resultBody.Close()
						}
					})
					joins = append(joins, returned)
					go func() {
						resultBody, resultErr = clientInvoke(client, method, context.Background(), provider)
						close(returned)
					}()
					clientWait(t, entered.ready)
					joins = append(joins, closed)
					go func() { client.Close(); close(closed) }()
					clientWait(t, requestContext.Done())
					clientPending(t, closed)
					allowed.open()
					clientWait(t, upstream.closeEntered.ready)
					clientPending(t, closed)
					upstream.unblock()
					clientWait(t, returned)
					clientWait(t, closed)
					if resultBody != nil || (invalid && resultErr == nil) || ((method == "get" || method == "range") && resultErr == nil) || upstream.closes.Load() != 1 {
						t.Fatalf("late result escaped method/body closure: %v", resultErr)
					}
				})
			}
		}
	}
}

type clientSeekReader struct {
	entered *clientGate
	allowed *clientGate
	seeks   atomic.Int32
	reads   atomic.Int32
}

func (r *clientSeekReader) Read([]byte) (int, error) { r.reads.Add(1); return 0, io.EOF }
func (r *clientSeekReader) Seek(int64, int) (int64, error) {
	r.seeks.Add(1)
	r.entered.open()
	<-r.allowed.ready
	return 0, errors.New("fixture seek failed")
}

func TestProviderClientCloseJoinsSeekAndRejectsNewInput(t *testing.T) {
	for _, provider := range []string{"s3", "r2", "gcs", "azure"} {
		t.Run(provider, func(t *testing.T) {
			var network atomic.Int32
			client := uploadClient(t, provider, "https://objects.example.test", uploadTransport(func(r *http.Request) (*http.Response, error) {
				network.Add(1)
				return nil, errors.New("unexpected transport")
			}))
			reader := &clientSeekReader{entered: newClientGate(), allowed: newClientGate()}
			returned, closed, closing := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var joins []<-chan struct{}
			t.Cleanup(func() { reader.allowed.open(); uploadCleanupJoins(t, joins...) })
			joins = append(joins, returned)
			go func() {
				defer close(returned)
				_, _ = client.Put(context.Background(), "kelvo/data", reader, 0, azureTestDigest(""), Condition{Absent: true})
			}()
			clientWait(t, reader.entered.ready)
			joins = append(joins, closed)
			go func() { close(closing); client.Close(); close(closed) }()
			clientWait(t, closing)
			clientPending(t, closed)
			reader.allowed.open()
			clientWait(t, returned)
			clientWait(t, closed)
			for _, method := range []string{"get", "range", "head", "put"} {
				body, err := clientInvoke(client, method, context.Background(), provider)
				if body != nil || !errors.Is(err, errClientClosed) {
					t.Errorf("closed client admitted %s", method)
				}
			}
			_, err := client.Put(context.Background(), "kelvo/data", reader, 0, azureTestDigest(""), Condition{Absent: true})
			if !errors.Is(err, errClientClosed) || reader.seeks.Load() != 1 || reader.reads.Load() != 0 || network.Load() != 0 {
				t.Fatal("closed client touched caller input or the network")
			}
		})
	}
}

func TestProviderClientCloseCancelsBodyWhileAnotherMethodIsBlocked(t *testing.T) {
	for _, provider := range []string{"s3", "r2", "gcs", "azure"} {
		t.Run(provider, func(t *testing.T) {
			upstream := newClientTestBody()
			client := uploadClient(t, provider, "https://objects.example.test", uploadTransport(func(r *http.Request) (*http.Response, error) {
				return clientResponse(provider, "get", upstream, r), nil
			}))
			body, err := clientInvoke(client, "get", context.Background(), provider)
			if err != nil {
				upstream.unblock()
				t.Fatal(err)
			}
			reader := &clientSeekReader{entered: newClientGate(), allowed: newClientGate()}
			returned, closed := make(chan struct{}), make(chan struct{})
			var joins []<-chan struct{}
			t.Cleanup(func() { reader.allowed.open(); upstream.unblock(); _ = body.Close(); uploadCleanupJoins(t, joins...) })
			joins = append(joins, returned)
			go func() {
				defer close(returned)
				_, _ = client.Put(context.Background(), "kelvo/data", reader, 0, azureTestDigest(""), Condition{Absent: true})
			}()
			clientWait(t, reader.entered.ready)
			joins = append(joins, closed)
			go func() { client.Close(); close(closed) }()
			clientWait(t, upstream.closeEntered.ready)
			clientPending(t, closed)
			upstream.unblock()
			clientWait(t, upstream.closeDone.ready)
			clientPending(t, closed)
			reader.allowed.open()
			clientWait(t, returned)
			clientWait(t, closed)
		})
	}
}

func TestProviderClientInvalidCallsReleaseMethodOwnership(t *testing.T) {
	for _, provider := range []string{"s3", "r2", "gcs", "azure"} {
		t.Run(provider, func(t *testing.T) {
			var calls atomic.Int32
			client := uploadClient(t, provider, "https://objects.example.test", uploadTransport(func(r *http.Request) (*http.Response, error) {
				calls.Add(1)
				return nil, errors.New("fixture transport failure")
			}))
			for range 8 {
				for _, method := range []string{"get", "range", "head", "put"} {
					body, err := clientInvoke(client, method, context.Background(), provider)
					if body != nil || err == nil {
						t.Fatal("transport error produced a successful result")
					}
				}
			}
			closed := make(chan struct{})
			go func() { client.Close(); close(closed) }()
			clientWait(t, closed)
			if calls.Load() != 32 {
				t.Fatal("failed operations were lost or retried")
			}
		})
	}
}

func TestProviderClientCloseJoinsNonemptyUploadOwnership(t *testing.T) {
	for _, provider := range []string{"s3", "r2", "gcs", "azure"} {
		t.Run(provider, func(t *testing.T) {
			readAllowed, closeAllowed := newClientGate(), newClientGate()
			reader := &uploadTestReader{Reader: bytes.NewReader([]byte("payload")), entered: make(chan struct{}), proceed: readAllowed.ready}
			readDone, transportClosed := make(chan struct{}), make(chan struct{})
			response := &uploadResponseBody{closed: make(chan struct{})}
			var requestContext context.Context
			client := uploadClient(t, provider, "https://objects.example.test", uploadTransport(func(r *http.Request) (*http.Response, error) {
				requestContext = r.Context()
				go func() { defer close(readDone); _, _ = io.ReadAll(r.Body) }()
				<-reader.entered
				go func() {
					defer close(transportClosed)
					<-closeAllowed.ready
					_ = r.Body.Close()
				}()
				return &http.Response{StatusCode: http.StatusCreated, Header: uploadHeaders(provider), Body: response, ContentLength: 0, Request: r}, nil
			}))
			putDone, closed := make(chan struct{}), make(chan struct{})
			var joins []<-chan struct{}
			t.Cleanup(func() { readAllowed.open(); closeAllowed.open(); uploadCleanupJoins(t, joins...) })
			joins = append(joins, putDone)
			go func() {
				defer close(putDone)
				_, _ = client.Put(context.Background(), "kelvo/data", reader, 7, azureTestDigest("payload"), Condition{Absent: true})
			}()
			clientWait(t, response.closed)
			// response.closed proves the transport started both fixture goroutines.
			joins = append(joins, readDone, transportClosed, closed)
			go func() { client.Close(); close(closed) }()
			clientWait(t, requestContext.Done())
			clientPending(t, closed)
			closeAllowed.open()
			clientWait(t, transportClosed)
			clientPending(t, closed)
			readAllowed.open()
			clientWait(t, readDone)
			clientWait(t, putDone)
			clientWait(t, closed)
			if reader.closes.Load() != 0 {
				t.Fatal("client shutdown closed a caller-owned upload reader")
			}
		})
	}
}

func TestProviderClientCloseRealTLSResponse(t *testing.T) {
	for _, provider := range []string{"s3", "r2", "gcs", "azure"} {
		for _, method := range []string{"get", "range"} {
			t.Run(provider+"/"+method, func(t *testing.T) {
				serverDone := make(chan struct{})
				stopFixture := newClientGate()
				server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					defer close(serverDone)
					response := clientResponse(provider, method, http.NoBody, r)
					for name, values := range response.Header {
						w.Header()[name] = values
					}
					w.WriteHeader(response.StatusCode)
					_, _ = w.Write([]byte("d"))
					w.(http.Flusher).Flush()
					select {
					case <-r.Context().Done():
					case <-stopFixture.ready:
					}
				}))
				t.Cleanup(server.Close)
				client := uploadClient(t, provider, server.URL, server.Client().Transport)
				var body io.ReadCloser
				var joins []<-chan struct{}
				t.Cleanup(func() {
					stopFixture.open()
					if body != nil {
						_ = body.Close()
					}
					uploadCleanupJoins(t, joins...)
				})
				var err error
				body, err = clientInvoke(client, method, context.Background(), provider)
				if err != nil {
					t.Fatal(err)
				}
				closed := make(chan struct{})
				joins = append(joins, closed, serverDone)
				go func() { client.Close(); close(closed) }()
				clientWait(t, closed)
				clientWait(t, serverDone)
				if _, err := body.Read(make([]byte, 1)); !errors.Is(err, io.ErrClosedPipe) {
					t.Fatal("TLS response remained usable after client Close")
				}
			})
		}
	}
}
