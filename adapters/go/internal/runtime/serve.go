package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/adapters/go/internal/frame"
	"github.com/SYNEHQ/kelvo-go/operations"
)

// SessionCacheKey formats the unique cache key for a connection. It binds
// tenant ID, app team, connection ID, database, schema and source revision:
// one worker never serves another database or schema of the same connection.
func SessionCacheKey(tenantID, appTeam, connectionID, database, schema, revision string) string {
	return fmt.Sprintf("%s\x00%s\x00%s\x00%s\x00%s\x00%s", tenantID, appTeam, connectionID, database, schema, revision)
}

// IsServeEligible verifies whether a request is allowed to run on a warm worker.
// Only read-only operations (QueryRead, MetadataInspect) without mutations,
// external file sources, JDBC, private channel broker, or native/sheets readers.
func IsServeEligible(req adapter.ProcessRequest) bool {
	if req.Request.Kind != operations.QueryRead && req.Request.Kind != operations.MetadataInspect {
		return false
	}
	if req.SourceFile != nil || req.Runtime != nil || req.PrivateTransport != nil {
		return false
	}
	if adapter.NativeReader(req.Source.Engine) || req.Source.Engine == "google_sheets" || adapter.CloudSQL(req.Source.Engine) {
		return false
	}
	return true
}

type frameResultWriter struct {
	w      *frame.SyncWriter
	stream uint32
}

func (fw *frameResultWriter) Write(p []byte) (int, error) {
	total := 0
	for len(p) > 0 {
		chunkSize := min(len(p), frame.MaxResultBytes)
		if err := fw.w.Write(frame.Result, fw.stream, p[:chunkSize]); err != nil {
			return total, err
		}
		total += chunkSize
		p = p[chunkSize:]
	}
	return total, nil
}

// secretless reports a request that relies on the worker's bound credentials.
func secretless(s adapter.ConnectionSpec) bool {
	return s.DSN == "" && s.URL == "" && s.Username == "" && s.Password == "" && s.Token == ""
}

// maxServeStreams bounds concurrent in-flight streams on one worker.
const maxServeStreams = 64

func writeStreamError(sw *frame.SyncWriter, stream uint32, msg string) error {
	if len(msg) > frame.MaxErrorBytes {
		msg = msg[:frame.MaxErrorBytes]
	}
	return sw.Write(frame.Error, stream, []byte(msg))
}

// Serve runs a long-lived warm adapter worker multiplexing framed requests
// (protocol v2). Each Request runs on its own stream and goroutine and ends
// with exactly one Receipt or Error frame. Per-stream failures leave the worker
// and shared session alive; protocol and authorization violations are fatal.
func (r Runner) Serve(parent context.Context, input io.Reader, output io.Writer) (err error) {
	if parent == nil || input == nil || output == nil {
		return adapter.ErrInvalid
	}

	cache := r.SessionCache
	if cache == nil {
		cache = &SessionCache{TTL: 30 * time.Minute, Max: 4}
	}
	open := r.Open
	if open == nil {
		open = openSource
	}

	hello, err := frame.Read(input)
	if err != nil {
		return fmt.Errorf("serve: hello read error: %w", err)
	}
	if hello.Type != frame.Hello {
		return errors.New("serve: first frame must be hello")
	}
	if err := frame.CheckHello(hello); err != nil {
		return fmt.Errorf("serve: hello rejected: %w", err)
	}
	if err := frame.WriteHello(output); err != nil {
		return fmt.Errorf("serve: hello write error: %w", err)
	}

	sw := frame.NewSyncWriter(output)
	serveCtx, cancelAll := context.WithCancel(parent)
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		inflight = map[uint32]context.CancelFunc{}
	)
	// Stop reading, cancel every stream, let them write terminal frames, then
	// close sessions.
	defer func() {
		cancelAll()
		wg.Wait()
		cache.Close()
	}()
	fatal := func(stream uint32, msg string, cause error) error {
		_ = writeStreamError(sw, stream, msg)
		return cause
	}

	var boundKey string
	var bound *adapter.ConnectionSpec // credentials this worker serves with
	for {
		if err := parent.Err(); err != nil {
			return err
		}

		f, err := frame.Read(input)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("serve: frame read error: %w", err)
		}

		switch f.Type {
		case frame.Ping:
			if err := sw.Write(frame.Pong, 0, nil); err != nil {
				return fmt.Errorf("serve: pong write error: %w", err)
			}

		case frame.Close:
			return nil

		case frame.Cancel:
			mu.Lock()
			cancel := inflight[f.Stream]
			mu.Unlock()
			if cancel != nil {
				cancel()
			}

		case frame.Request:
			stream, payload := f.Stream, f.Payload
			if stream == 0 {
				return fatal(0, "request on stream 0", errors.New("serve: request on stream 0"))
			}
			mu.Lock()
			_, dup := inflight[stream]
			mu.Unlock()
			if dup {
				return fatal(stream, "stream already in flight", fmt.Errorf("serve: duplicate in-flight stream %d", stream))
			}
			if len(payload) == 0 {
				return fatal(stream, "empty request payload", errors.New("serve: empty request payload"))
			}

			request, err := adapter.ParseProcessRequest(payload)
			if err != nil {
				return fatal(stream, fmt.Sprintf("invalid process request: %v", err), fmt.Errorf("serve: invalid process request: %w", err))
			}
			clear(payload)

			if !IsServeEligible(request) {
				return fatal(stream, "operation not eligible for warm serve mode", errors.New("serve: operation not eligible for warm worker"))
			}

			key := SessionCacheKey(request.Source.TenantID, request.AppTeam, request.Source.ConnectionID, request.Source.Database, request.Source.Schema, request.Source.Revision)
			if boundKey == "" {
				boundKey = key
			} else if boundKey != key {
				// Security invariant: One worker never serves more than one tenant, team, connection or revision
				return fatal(stream, "cross-tenant or cross-connection request rejected", errors.New("serve: security invariant violation: cross-connection request rejected"))
			}

			// A worker keeps the credentials of the first request that carried
			// them; the node sends later reads without secrets. A secretless
			// request must name the same source as those credentials.
			if secretless(request.Source) {
				if bound == nil || request.Source.Engine != bound.Engine || request.Source.Database != bound.Database || request.Source.Schema != bound.Schema || len(request.Source.Options) != 0 {
					return fatal(stream, "secretless request without matching credentials", errors.New("serve: secretless request without matching credentials"))
				}
				request.Source = *bound
			} else {
				credentials := request.Source
				bound = &credentials
			}

			now := time.Now()
			if request.CredentialsValidUntil <= now.Unix() {
				return fatal(stream, "credentials expired", errors.New("serve: credentials expired"))
			}
			if request.ExpiresAt <= now.Unix() {
				return fatal(stream, "request expired", errors.New("serve: request expired"))
			}

			reqDeadline := time.Unix(request.ExpiresAt, 0)
			if bounded := now.Add(time.Duration(request.Limits.TimeoutMS) * time.Millisecond); bounded.Before(reqDeadline) {
				reqDeadline = bounded
			}

			mu.Lock()
			if len(inflight) >= maxServeStreams {
				mu.Unlock()
				if err := writeStreamError(sw, stream, "busy"); err != nil {
					return fmt.Errorf("serve: busy write failed: %w", err)
				}
				continue
			}
			reqCtx, cancelReq := context.WithDeadline(serveCtx, reqDeadline)
			inflight[stream] = cancelReq
			mu.Unlock()

			wg.Add(1)
			go func() {
				defer wg.Done()
				defer cancelReq()
				finish := func() {
					mu.Lock()
					delete(inflight, stream)
					mu.Unlock()
				}
				r.serveStream(serveCtx, reqCtx, sw, stream, cache, key, request, open, finish)
			}()

		default:
			return fatal(f.Stream, fmt.Sprintf("unexpected frame type: %v", f.Type), fmt.Errorf("serve: unexpected frame type: %v", f.Type))
		}
	}
}

// serveStream runs one request and writes its single terminal frame. finish
// frees the stream ID and runs before that frame so the node may reuse it.
func (r Runner) serveStream(base, ctx context.Context, sw *frame.SyncWriter, stream uint32, cache *SessionCache, key string, request adapter.ProcessRequest, open func(context.Context, adapter.ConnectionSpec, operations.Request) (adapter.Session, error), finish func()) {
	fail := func(msg string) {
		finish()
		_ = writeStreamError(sw, stream, msg)
	}
	scratchDir, err := os.MkdirTemp("", "kelvo-serve-scratch-*")
	if err != nil {
		fail("failed creating scratch directory")
		return
	}
	defer os.RemoveAll(scratchDir)

	receipt, _ := r.executeServeRequest(base, ctx, cache, key, request, open, &frameResultWriter{w: sw, stream: stream})
	if receipt.Validate() != nil {
		fail("invalid receipt generated")
		return
	}
	encoded, encErr := json.Marshal(receipt)
	if encErr != nil || len(encoded) > operations.MaxReceiptBytes {
		fail("receipt encoding failed")
		return
	}
	finish()
	_ = sw.Write(frame.Receipt, stream, encoded)
}

// reusableSession is implemented by sessions that can serve several requests
// on one source connection once KeepOpen has been called.
type reusableSession interface{ KeepOpen() }

func (r Runner) executeServeRequest(
	base, ctx context.Context,
	cache *SessionCache,
	key string,
	input adapter.ProcessRequest,
	open func(context.Context, adapter.ConnectionSpec, operations.Request) (adapter.Session, error),
	output io.Writer,
) (receipt operations.Receipt, resultErr error) {
	defer func() { receipt.ErrorCode = sourceFailureCode(ctx, receipt, resultErr) }()
	receipt = operations.Receipt{
		Version:       operations.Version,
		OperationID:   input.OperationID,
		RequestSHA256: input.RequestSHA256,
		Outcome:       operations.Rejected,
		Effect:        operations.EffectNone,
		ErrorCode:     "INVALID_ARGUMENT",
	}

	limits := adapter.Limits{
		MaxRows:   input.Limits.MaxRows,
		MaxBytes:  input.Limits.MaxBytes,
		BatchRows: input.Limits.BatchRows,
	}
	invocation, err := adapter.FromOperation(input.Request, limits)
	if err != nil {
		receipt.ErrorCode = "UNSUPPORTED"
		return receipt, err
	}

	// The shared open must not depend on any one request: it runs on the serve
	// context bounded by the credential deadline, while this request's ctx only
	// bounds how long it waits.
	session, err := cache.Get(ctx, key, func(context.Context) (adapter.Session, error) {
		openCtx, stopOpen := context.WithDeadline(base, time.Unix(input.CredentialsValidUntil, 0))
		defer stopOpen()
		opened, err := open(openCtx, input.Source, input.Request)
		if keep, ok := opened.(reusableSession); ok && err == nil {
			keep.KeepOpen()
		}
		return opened, err
	})

	if err != nil {
		if session != nil {
			_ = session.Close()
		}
		receipt.ErrorCode = "SOURCE_FAILED"
		return receipt, err
	}
	if session == nil {
		receipt.ErrorCode = "SOURCE_FAILED"
		return receipt, adapter.ErrInvalid
	}

	receipt.Outcome = operations.Failed
	receipt.ErrorCode = "SOURCE_FAILED"

	execReceipt, execErr := executeInvocation(ctx, input, invocation, session, output, receipt)

	// A reusable session is shared and stays cached whatever this stream did
	// (the session discards its own broken connection). Every other session is
	// single-use by contract and is closed like a one-shot run.
	if _, ok := session.(reusableSession); !ok {
		if err := session.Close(); err != nil && execErr == nil {
			return execReceipt, errors.New("adapter session cleanup failed")
		}
	}
	return execReceipt, execErr
}
