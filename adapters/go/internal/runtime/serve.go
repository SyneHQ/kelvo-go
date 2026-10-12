package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/adapters/go/internal/frame"
	"github.com/SYNEHQ/kelvo-go/operations"
)

// SessionCacheKey formats the unique cache key for a connection.
// Binds tenant ID, app team, connection ID, and source revision.
func SessionCacheKey(tenantID, appTeam, connectionID, revision string) string {
	return fmt.Sprintf("%s\x00%s\x00%s\x00%s", tenantID, appTeam, connectionID, revision)
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
	w io.Writer
}

func (fw *frameResultWriter) Write(p []byte) (int, error) {
	total := 0
	for len(p) > 0 {
		chunkSize := len(p)
		if chunkSize > frame.MaxResultBytes {
			chunkSize = frame.MaxResultBytes
		}
		if err := frame.WriteResult(fw.w, p[:chunkSize]); err != nil {
			return total, err
		}
		total += chunkSize
		p = p[chunkSize:]
	}
	return total, nil
}

// Serve runs a long-lived warm adapter worker serving framed requests.
// It manages a SessionCache, re-authorizes every request, evicts on error or
// revision change, and exits immediately on any failure.
func (r Runner) Serve(parent context.Context, input io.Reader, output io.Writer) error {
	if parent == nil || input == nil || output == nil {
		return adapter.ErrInvalid
	}

	cache := r.SessionCache
	if cache == nil {
		cache = &SessionCache{
			TTL: 30 * time.Minute,
			Max: 4,
		}
	}
	defer cache.Close()

	open := r.Open
	if open == nil {
		open = openSource
	}

	var boundKey string

	for {
		if err := parent.Err(); err != nil {
			return err
		}

		fType, payload, err := frame.Read(input)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("serve: frame read error: %w", err)
		}

		switch fType {
		case frame.Ping:
			if err := frame.WritePong(output); err != nil {
				return fmt.Errorf("serve: pong write error: %w", err)
			}
			continue

		case frame.Close:
			return nil

		case frame.Request:
			if len(payload) == 0 {
				_ = frame.WriteError(output, "empty request payload")
				return errors.New("serve: empty request payload")
			}

			request, err := adapter.ParseProcessRequest(payload)
			if err != nil {
				_ = frame.WriteError(output, fmt.Sprintf("invalid process request: %v", err))
				return fmt.Errorf("serve: invalid process request: %w", err)
			}
			clear(payload)

			if !IsServeEligible(request) {
				_ = frame.WriteError(output, "operation not eligible for warm serve mode")
				return errors.New("serve: operation not eligible for warm worker")
			}

			key := SessionCacheKey(request.Source.TenantID, request.AppTeam, request.Source.ConnectionID, request.Source.Revision)
			if boundKey == "" {
				boundKey = key
			} else if boundKey != key {
				// Security invariant: One worker never serves more than one tenant, team, connection or revision
				_ = frame.WriteError(output, "cross-tenant or cross-connection request rejected")
				return errors.New("serve: security invariant violation: cross-connection request rejected")
			}

			now := time.Now()
			if request.CredentialsValidUntil <= now.Unix() {
				_ = frame.WriteError(output, "credentials expired")
				return errors.New("serve: credentials expired")
			}
			if request.ExpiresAt <= now.Unix() {
				_ = frame.WriteError(output, "request expired")
				return errors.New("serve: request expired")
			}

			scratchDir, err := os.MkdirTemp("", "kelvo-serve-scratch-*")
			if err != nil {
				_ = frame.WriteError(output, "failed creating scratch directory")
				return fmt.Errorf("serve: scratch dir error: %w", err)
			}
			wiped := false
			wipeScratch := func() {
				if !wiped {
					_ = os.RemoveAll(scratchDir)
					wiped = true
				}
			}

			reqDeadline := time.Unix(request.ExpiresAt, 0)
			if bounded := now.Add(time.Duration(request.Limits.TimeoutMS) * time.Millisecond); bounded.Before(reqDeadline) {
				reqDeadline = bounded
			}
			reqCtx, cancelReq := context.WithDeadline(parent, reqDeadline)

			resultWriter := &frameResultWriter{w: output}
			receipt, serveErr := r.executeServeRequest(reqCtx, cache, key, request, open, resultWriter)
			cancelReq()
			wipeScratch()

			if receipt.Validate() != nil {
				_ = frame.WriteError(output, "invalid receipt generated")
				return errors.New("serve: invalid receipt")
			}

			encoded, encErr := json.Marshal(receipt)
			if encErr != nil || len(encoded) > operations.MaxReceiptBytes {
				_ = frame.WriteError(output, "receipt encoding failed")
				return errors.New("serve: receipt encoding failed")
			}

			if err := frame.WriteReceipt(output, encoded); err != nil {
				return fmt.Errorf("serve: receipt write failed: %w", err)
			}

			if serveErr != nil || receipt.Outcome != operations.Completed {
				// Any failure kills the worker to prevent reuse of a poisoned connection
				return fmt.Errorf("serve: request failed: %w", serveErr)
			}

		default:
			_ = frame.WriteError(output, fmt.Sprintf("unexpected frame type: %v", fType))
			return fmt.Errorf("serve: unexpected frame type: %v", fType)
		}
	}
}

func (r Runner) executeServeRequest(
	ctx context.Context,
	cache *SessionCache,
	key string,
	input adapter.ProcessRequest,
	open func(context.Context, adapter.ConnectionSpec, operations.Request) (adapter.Session, error),
	output io.Writer,
) (receipt operations.Receipt, resultErr error) {
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

	openCtx, stopOpen := context.WithDeadline(ctx, time.Unix(input.CredentialsValidUntil, 0))
	session, err := cache.Get(openCtx, key, func(c context.Context) (adapter.Session, error) {
		return open(c, input.Source, input.Request)
	})
	stopOpen()

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
	if execErr != nil || execReceipt.Outcome != operations.Completed {
		// On ANY error or incomplete outcome, close the session so it is never reused
		_ = session.Close()
		return execReceipt, execErr
	}

	// Success: keep connection warm in cache
	cache.Put(key, session)
	return execReceipt, nil
}
