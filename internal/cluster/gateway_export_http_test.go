// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"net/url"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/worker"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	arrowutil "github.com/apache/arrow-go/v18/arrow/util"
)

type exportSocketProgress struct {
	started            atomic.Int64
	finished           atomic.Int64
	written            atomic.Int64
	writeErrors        atomic.Int64
	firstWrite         atomic.Int64
	deadlineCalls      atomic.Int64
	immediateDeadlines atomic.Int64
	deadlineErrors     atomic.Int64
	handlerExited      atomic.Bool
}

type exportObservedTransport func(*http.Request) (*http.Response, error)

func (f exportObservedTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type exportObservedResponseBody struct {
	io.ReadCloser
	started, finished *atomic.Int32
}

func (b *exportObservedResponseBody) Close() error {
	b.started.Add(1)
	err := b.ReadCloser.Close()
	b.finished.Add(1)
	return err
}

type exportObservedSocketWriter struct {
	http.ResponseWriter
	progress *exportSocketProgress
}

func (w *exportObservedSocketWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *exportObservedSocketWriter) SetWriteDeadline(deadline time.Time) error {
	w.progress.deadlineCalls.Add(1)
	if !deadline.IsZero() && !deadline.After(time.Now().Add(time.Millisecond)) {
		w.progress.immediateDeadlines.Add(1)
	}
	err := http.NewResponseController(w.ResponseWriter).SetWriteDeadline(deadline)
	if err != nil {
		w.progress.deadlineErrors.Add(1)
	}
	return err
}
func (w *exportObservedSocketWriter) Write(data []byte) (int, error) {
	if w.Header().Get("Content-Type") != "application/vnd.apache.arrow.stream" {
		return w.ResponseWriter.Write(data)
	}
	w.progress.firstWrite.CompareAndSwap(0, time.Now().UnixNano())
	w.progress.started.Add(1)
	n, err := w.ResponseWriter.Write(data)
	w.progress.written.Add(int64(n))
	w.progress.finished.Add(1)
	if err != nil {
		w.progress.writeErrors.Add(1)
	}
	return n, err
}

type exportSlowConnection struct {
	tcp      *net.TCPConn
	tls      *tls.Conn
	response *http.Response
	index    int
	stop     func() bool
}

func (c *exportSlowConnection) close() {
	c.stop()
	_ = c.tls.Close()
}

// Set the receive window before TLS negotiation, then stop reading after HTTP
// headers. This reaches actual blocked server socket writes with bounded input.
func openExportSlowConnection(ctx context.Context, server *httptest.Server, index int) (*exportSlowConnection, error) {
	u, err := url.Parse(server.URL)
	if err != nil {
		return nil, err
	}
	connection, err := (&net.Dialer{Timeout: 3 * time.Second}).DialContext(ctx, "tcp", u.Host)
	if err != nil {
		return nil, err
	}
	tcp, ok := connection.(*net.TCPConn)
	if !ok {
		_ = connection.Close()
		return nil, fmt.Errorf("fixture did not use TCP")
	}
	stop := context.AfterFunc(ctx, func() { _ = tcp.Close() })
	succeeded := false
	defer func() {
		if !succeeded {
			stop()
			_ = tcp.Close()
		}
	}()
	if err := tcp.SetReadBuffer(2048); err != nil {
		return nil, err
	}
	if err := tcp.SetDeadline(time.Now().Add(20 * time.Second)); err != nil {
		return nil, err
	}
	config := server.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
	config.ServerName = u.Hostname()
	secured := tls.Client(tcp, config)
	if err := secured.HandshakeContext(ctx); err != nil {
		return nil, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/v1/exports/"+exportGatewayID+"/parts/0", nil)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Authorization", "Bearer "+rotationOld)
	request.Header.Set("X-Export-Test-Client", strconv.Itoa(index))
	request.Header.Set("Connection", "close")
	if err := request.Write(secured); err != nil {
		return nil, err
	}
	response, err := http.ReadResponse(bufio.NewReaderSize(secured, 1024), request)
	if err != nil {
		return nil, err
	}
	succeeded = true
	return &exportSlowConnection{tcp: tcp, tls: secured, response: response, index: index, stop: stop}, nil
}

func exportGatewayWideWire(t *testing.T) ([]byte, query.Stats) {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "payload", Type: arrow.BinaryTypes.String}}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	value := strings.Repeat("bounded-export-", (128<<10)/len("bounded-export-"))
	for i := 0; i < 128; i++ {
		builder.Field(0).(*array.Int64Builder).Append(int64(i))
		builder.Field(1).(*array.StringBuilder).Append(value)
	}
	record := builder.NewRecordBatch()
	builder.Release()
	defer record.Release()
	var encoded bytes.Buffer
	sink := worker.NewIPCSink(&encoded, query.DefaultLimits())
	defer sink.Abort()
	if err := sink.Schema(schema); err != nil {
		t.Fatal(err)
	}
	if err := sink.Write(record); err != nil {
		t.Fatal(err)
	}
	if err := sink.Finish(); err != nil {
		t.Fatal(err)
	}
	if encoded.Len() < 15<<20 {
		t.Fatal("fixture is too small to reach TCP backpressure")
	}
	return encoded.Bytes(), query.Stats{Rows: 128, Batches: 1, Bytes: arrowutil.TotalRecordSize(record), WireBytes: sink.EncodedBytes()}
}

// Real TCP/TLS clients and upstreams exercise admission, blocked writes and
// abort semantics. Durable metadata is the controlled CAS fixture, so this is
// transport/custody evidence rather than a broker, multi-host or throughput gate.
func TestGatewayExportTLSConcurrentSlowReadersWithdraw(t *testing.T) {
	for _, mode := range []string{"cancel", "key-revocation", "document-expiry"} {
		t.Run(mode, func(t *testing.T) {
			g, store, auth := exportGatewayFixture(t, "none")
			store.policy.Limits.Timeout = 10 * time.Second
			store.policy.Exports.Limits.MaxEncodedBytes = 64 << 20
			store.policy.Exports.Limits.MaxDecodedBytes = 64 << 20
			store.policy.Exports.Limits.MaxPartBytes = 32 << 20
			store.policy.Exports.Limits.MaxPartDecodedBytes = 32 << 20
			g.tenants["a"].store.(*gatewayStore).policy = store.policy
			g.exportDownloads = make(chan struct{}, 2)
			wire, stats := exportGatewayWideWire(t)
			seedExportGatewayReady(t, g, store, wire, stats)
			var upstreamActive, upstreamCalls atomic.Int32
			installExportGatewayWorker(t, g, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/internal/exports/"+exportGatewayID+"/parts/0" {
					t.Error("unexpected worker path")
					w.WriteHeader(404)
					return
				}
				upstreamCalls.Add(1)
				upstreamActive.Add(1)
				defer upstreamActive.Add(-1)
				w.Header().Set("Content-Type", "application/vnd.apache.arrow.stream")
				_, _ = w.Write(wire)
			})
			var bodyCloseStarted, bodyCloseFinished atomic.Int32
			endpoint := g.tenants["a"].workers["a1"]
			originalTransport := endpoint.client.Transport
			copyClient := *endpoint.client
			copyClient.Transport = exportObservedTransport(func(r *http.Request) (*http.Response, error) {
				response, err := originalTransport.RoundTrip(r)
				if err == nil {
					response.Body = &exportObservedResponseBody{ReadCloser: response.Body, started: &bodyCloseStarted, finished: &bodyCloseFinished}
				}
				return response, err
			})
			endpoint.client = &copyClient
			g.tenants["a"].workers["a1"] = endpoint
			progress := make([]exportSocketProgress, 10)
			type completion struct {
				index   int
				aborted bool
			}
			done := make(chan completion, 10)
			public := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				index, err := strconv.Atoi(r.Header.Get("X-Export-Test-Client"))
				if err != nil || index < 0 || index >= len(progress) {
					g.ServeHTTP(w, r)
					return
				}
				observed := &exportObservedSocketWriter{ResponseWriter: w, progress: &progress[index]}
				defer func() {
					value := recover()
					progress[index].handlerExited.Store(true)
					if progress[index].started.Load() > 0 {
						done <- completion{index: index, aborted: value == http.ErrAbortHandler}
					}
					if value != nil {
						panic(value)
					}
				}()
				g.ServeHTTP(observed, r)
			}))
			t.Cleanup(public.Close)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			client := public.Client()
			client.Timeout = 3 * time.Second
			t.Cleanup(func() { cancel(); g.cancel(); client.CloseIdleConnections() })
			type opened struct {
				connection *exportSlowConnection
				err        error
			}
			results := make(chan opened, 10)
			start := make(chan struct{})
			var clients sync.WaitGroup
			for index := range 10 {
				clients.Add(1)
				go func(index int) {
					defer clients.Done()
					<-start
					connection, err := openExportSlowConnection(ctx, public, index)
					results <- opened{connection, err}
				}(index)
			}
			defer func() { cancel(); clients.Wait() }()
			close(start)
			var admitted []*exportSlowConnection
			rejected := 0
			for range 10 {
				select {
				case result := <-results:
					if result.err != nil {
						t.Fatal("TLS cohort failed", result.err)
					}
					connection := result.connection
					defer connection.close()
					switch connection.response.StatusCode {
					case http.StatusOK:
						admitted = append(admitted, connection)
					case http.StatusTooManyRequests:
						_, err := io.Copy(io.Discard, io.LimitReader(connection.response.Body, 1<<20))
						_ = connection.response.Body.Close()
						connection.close()
						if err != nil {
							t.Fatal("bounded rejection failed", err)
						}
						rejected++
					default:
						t.Fatal("unexpected cohort status", connection.response.StatusCode)
					}
				case <-ctx.Done():
					t.Fatal("TLS cohort did not return headers")
				}
			}
			if len(admitted) != 2 || rejected != 8 || len(g.exportDownloads) != 2 || upstreamCalls.Load() != 2 {
				t.Fatal("download ceiling escaped", len(admitted), rejected, len(g.exportDownloads), upstreamCalls.Load())
			}
			// Observe each actual ResponseWriter.Write blocked across samples;
			// a slow client alone is not evidence that cancellation interrupts I/O.
			blockingDeadline := time.Now().Add(3 * time.Second)
			blockedSince := time.Time{}
			for {
				blocked := true
				for _, connection := range admitted {
					p := &progress[connection.index]
					blocked = blocked && p.written.Load() > 0 && p.started.Load() > p.finished.Load()
				}
				if blocked {
					if blockedSince.IsZero() {
						blockedSince = time.Now()
					}
					if time.Since(blockedSince) >= 50*time.Millisecond {
						break
					}
				} else {
					blockedSince = time.Time{}
				}
				if time.Now().After(blockingDeadline) {
					t.Fatal("fixture did not reach concurrent blocked socket writes")
				}
				time.Sleep(10 * time.Millisecond)
			}
			control := func(method, suffix string) int {
				request, err := http.NewRequestWithContext(ctx, method, public.URL+"/v1/exports/"+exportGatewayID+suffix, nil)
				if err != nil {
					t.Fatal(err)
				}
				request.Header.Set("Authorization", "Bearer "+rotationNew)
				response, err := client.Do(request)
				if err != nil {
					t.Fatal("control traffic blocked by readers", err)
				}
				defer response.Body.Close()
				if _, err := io.Copy(io.Discard, io.LimitReader(response.Body, 1<<20)); err != nil {
					t.Fatal(err)
				}
				return response.StatusCode
			}
			if code := control(http.MethodGet, ""); code != http.StatusOK {
				t.Fatal("slow downloads blocked status", code)
			}
			for _, connection := range admitted {
				if time.Since(time.Unix(0, progress[connection.index].firstWrite.Load())) >= 5*time.Second {
					t.Fatal("insufficient separation from ordinary download deadline")
				}
			}
			actionAt := time.Now()
			switch mode {
			case "cancel":
				if code := control(http.MethodPost, "/cancel"); code != http.StatusOK {
					t.Fatal("slow downloads blocked cancellation", code)
				}
			case "key-revocation":
				set := principalKeySet(t, 2, map[string][]string{"analyst": {rotationNew}, "reports": {exportGatewayReportsKey}}, map[string][]string{"analyst": {rotationOther}})
				if !auth.apply(set, time.Now()) {
					t.Fatal("key revocation failed")
				}
			case "document-expiry":
				auth.mu.Lock()
				auth.validUntil = time.Now().Add(-time.Second)
				auth.mu.Unlock()
			}
			withdrawalDeadline := time.NewTimer(2 * time.Second)
			defer withdrawalDeadline.Stop()
			for range admitted {
				select {
				case finished := <-done:
					if !finished.aborted || progress[finished.index].writeErrors.Load() == 0 {
						t.Fatal("blocked TLS write was not aborted", finished)
					}
				case <-withdrawalDeadline.C:
					for _, connection := range admitted {
						p := &progress[connection.index]
						t.Logf("withdrawal counters: index=%d writes=%d/%d errors=%d deadlines=%d immediate=%d deadline_errors=%d handler_exited=%v", connection.index, p.started.Load(), p.finished.Load(), p.writeErrors.Load(), p.deadlineCalls.Load(), p.immediateDeadlines.Load(), p.deadlineErrors.Load(), p.handlerExited.Load())
					}
					t.Logf("upstream close counters: active=%d close_started=%d close_finished=%d", upstreamActive.Load(), bodyCloseStarted.Load(), bodyCloseFinished.Load())
					stack := make([]byte, 2<<20)
					n := runtime.Stack(stack, true)
					for _, goroutine := range strings.Split(string(stack[:n]), "\n\n") {
						if strings.Contains(goroutine, "(*Gateway).exportPart") || strings.Contains(goroutine, "(*exportObservedSocketWriter).Write") {
							t.Logf("withdrawal blocked stack:\n%s", goroutine)
						}
					}
					t.Fatal("authority withdrawal did not interrupt socket writes")
				}
			}
			if elapsed := time.Since(actionAt); elapsed >= 2*time.Second {
				t.Fatal("withdrawal depended on ordinary deadline", elapsed)
			}
			for _, connection := range admitted {
				if err := connection.tcp.SetReadBuffer(1 << 20); err != nil {
					t.Fatal(err)
				}
				if err := connection.tcp.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
					t.Fatal(err)
				}
				partial, err := io.ReadAll(io.LimitReader(connection.response.Body, int64(len(wire))+1))
				if err == nil || len(partial) == 0 || len(partial) >= len(wire) || bytes.HasSuffix(partial, []byte{255, 255, 255, 255, 0, 0, 0, 0}) {
					t.Fatal("withdrawn stream appeared complete", len(partial), err)
				}
				_ = connection.response.Body.Close()
				connection.close()
			}
			cleanupDeadline := time.Now().Add(2 * time.Second)
			for upstreamActive.Load() != 0 || len(g.exportDownloads) != 0 {
				if time.Now().After(cleanupDeadline) {
					t.Fatal("withdrawn streams retained upstreams or permits", upstreamActive.Load(), len(g.exportDownloads))
				}
				time.Sleep(10 * time.Millisecond)
			}
			if g.ctx.Err() != nil {
				t.Fatal("withdrawing downloads stopped the gateway")
			}
		})
	}
}

// HTTP/2 deadlines belong to the response stream. Revoking a blocked download
// must preserve independent control requests on the very same TCP connection.
func TestGatewayExportHTTP2CancellationPreservesOtherStreams(t *testing.T) {
	g, store, auth := exportGatewayFixture(t, "none")
	store.policy.Limits.Timeout = 10 * time.Second
	store.policy.Exports.Limits.MaxEncodedBytes = 64 << 20
	store.policy.Exports.Limits.MaxDecodedBytes = 64 << 20
	store.policy.Exports.Limits.MaxPartBytes = 32 << 20
	store.policy.Exports.Limits.MaxPartDecodedBytes = 32 << 20
	g.tenants["a"].store.(*gatewayStore).policy = store.policy
	g.exportDownloads = make(chan struct{}, 1)
	wire, stats := exportGatewayWideWire(t)
	seedExportGatewayReady(t, g, store, wire, stats)
	var upstreamActive atomic.Int32
	installExportGatewayWorker(t, g, func(w http.ResponseWriter, r *http.Request) {
		upstreamActive.Add(1)
		defer upstreamActive.Add(-1)
		w.Header().Set("Content-Type", "application/vnd.apache.arrow.stream")
		_, _ = w.Write(wire)
	})
	var progress exportSocketProgress
	done := make(chan bool, 1)
	public := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/parts/0") {
			g.ServeHTTP(w, r)
			return
		}
		defer func() {
			value := recover()
			progress.handlerExited.Store(true)
			done <- value == http.ErrAbortHandler
			if value != nil {
				panic(value)
			}
		}()
		g.ServeHTTP(&exportObservedSocketWriter{ResponseWriter: w, progress: &progress}, r)
	}))
	public.EnableHTTP2 = true
	public.StartTLS()
	t.Cleanup(public.Close)
	transport := public.Client().Transport.(*http.Transport).Clone()
	transport.ForceAttemptHTTP2 = true
	transport.MaxConnsPerHost = 1
	client := &http.Client{Transport: transport}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	t.Cleanup(func() { cancel(); g.cancel(); transport.CloseIdleConnections() })
	var downloadConnection net.Conn
	downloadContext := httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{GotConn: func(info httptrace.GotConnInfo) { downloadConnection = info.Conn }})
	request, err := http.NewRequestWithContext(downloadContext, http.MethodGet, public.URL+"/v1/exports/"+exportGatewayID+"/parts/0", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+rotationOld)
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK || response.ProtoMajor != 2 || downloadConnection == nil {
		t.Fatal("download did not negotiate HTTP/2", response.StatusCode, response.Proto)
	}
	blockingDeadline := time.Now().Add(3 * time.Second)
	blockedSince := time.Time{}
	for {
		if progress.written.Load() > 0 && progress.started.Load() > progress.finished.Load() {
			if blockedSince.IsZero() {
				blockedSince = time.Now()
			}
			if time.Since(blockedSince) >= 50*time.Millisecond {
				break
			}
		} else {
			blockedSince = time.Time{}
		}
		if time.Now().After(blockingDeadline) {
			t.Fatal("HTTP/2 fixture did not reach blocked stream writes")
		}
		time.Sleep(10 * time.Millisecond)
	}
	control := func() {
		t.Helper()
		controlContext, controlCancel := context.WithTimeout(ctx, time.Second)
		defer controlCancel()
		var connection net.Conn
		controlContext = httptrace.WithClientTrace(controlContext, &httptrace.ClientTrace{GotConn: func(info httptrace.GotConnInfo) { connection = info.Conn }})
		request, err := http.NewRequestWithContext(controlContext, http.MethodGet, public.URL+"/v1/exports/"+exportGatewayID, nil)
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Authorization", "Bearer "+rotationNew)
		response, err := client.Do(request)
		if err != nil {
			t.Fatal("HTTP/2 control stream failed", err)
		}
		defer response.Body.Close()
		if _, err := io.Copy(io.Discard, io.LimitReader(response.Body, 1<<20)); err != nil {
			t.Fatal(err)
		}
		if response.StatusCode != http.StatusOK || response.ProtoMajor != 2 || connection != downloadConnection {
			t.Fatal("control traffic did not retain the same HTTP/2 connection", response.StatusCode, response.Proto, connection == downloadConnection)
		}
	}
	control()
	if time.Since(time.Unix(0, progress.firstWrite.Load())) >= 5*time.Second {
		t.Fatal("insufficient separation from ordinary download deadline")
	}
	set := principalKeySet(t, 2, map[string][]string{"analyst": {rotationNew}, "reports": {exportGatewayReportsKey}}, map[string][]string{"analyst": {rotationOther}})
	if !auth.apply(set, time.Now()) {
		t.Fatal("key revocation failed")
	}
	select {
	case aborted := <-done:
		if !aborted || progress.writeErrors.Load() == 0 {
			t.Fatal("HTTP/2 download did not abort its blocked write")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("HTTP/2 cancellation did not release blocked stream")
	}
	control()
	partial, err := io.ReadAll(io.LimitReader(response.Body, int64(len(wire))+1))
	if err == nil || len(partial) == 0 || len(partial) >= len(wire) || bytes.HasSuffix(partial, []byte{255, 255, 255, 255, 0, 0, 0, 0}) {
		t.Fatal("cancelled HTTP/2 stream appeared complete", len(partial), err)
	}
	_ = response.Body.Close()
	deadline := time.Now().Add(2 * time.Second)
	for upstreamActive.Load() != 0 || len(g.exportDownloads) != 0 {
		if time.Now().After(deadline) {
			t.Fatal("HTTP/2 cancellation retained upstreams or permits")
		}
		time.Sleep(10 * time.Millisecond)
	}
	control()
}
