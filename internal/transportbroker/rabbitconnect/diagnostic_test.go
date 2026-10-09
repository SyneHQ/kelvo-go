package rabbitconnect

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/transportbroker"
	"github.com/SYNEHQ/kelvo-go/internal/transportbroker/diagnostic"
	"github.com/SYNEHQ/kelvo-go/sourceproof"
)

func requireStage(t *testing.T, recorder *diagnostic.Recorder, stage diagnostic.Stage, result diagnostic.Result, status int) {
	t.Helper()
	for _, event := range recorder.Snapshot().Events {
		if event.Stage == stage && event.Result == result && event.HTTPStatus == status {
			return
		}
	}
	t.Fatalf("missing diagnostic stage %s result %s status %d", stage, result, status)
}

func TestOpenDiagnosticsIdentifyFailureWithoutChangingSetup(t *testing.T) {
	for _, mode := range []string{"issue", "issue_timeout", "ticket", "tcp", "tls", "connect", "success"} {
		t.Run(mode, func(t *testing.T) {
			f := newFixture(t)
			issuer := f.issuer()
			stage, result, status := diagnostic.TicketIssue, diagnostic.Failed, 0
			switch mode {
			case "issue":
				issuer = issueFunc(func(context.Context, IssueRequest) (string, error) { return "", errors.New("SECRET_DIAGNOSTIC_CANARY") })
			case "issue_timeout":
				f.config.SetupTimeout = 20 * time.Millisecond
				issuer = issueFunc(func(ctx context.Context, _ IssueRequest) (string, error) { <-ctx.Done(); return "", ctx.Err() })
				result = diagnostic.Timeout
			case "ticket":
				issuer = issueFunc(func(context.Context, IssueRequest) (string, error) { return "SECRET_DIAGNOSTIC_CANARY", nil })
				stage, result = diagnostic.TicketVerified, diagnostic.ScopeDenied
			case "tcp":
				stage = diagnostic.ProxyTCP
			case "tls":
				stage = diagnostic.ProxyTLS
				f.config.ProxyAddress = serveOnce(t, nil, func(c net.Conn) { io.WriteString(c, "not TLS SECRET_DIAGNOSTIC_CANARY"); c.Close() })
			case "connect", "success":
				stage = diagnostic.ConnectResponse
				status = 403
				response := "HTTP/1.1 403 SECRET_DIAGNOSTIC_CANARY\r\n\r\n"
				if mode == "success" {
					response = "HTTP/1.1 200 Connection Established\r\n\r\n"
					result = diagnostic.Succeeded
					status = 200
				}
				f.config.ProxyAddress = serveOnce(t, f.server, func(c net.Conn) {
					if _, err := readCONNECT(c); err == nil {
						io.WriteString(c, response)
					}
				})
			}
			opener, err := New(f.config, issuer)
			if err != nil {
				t.Fatal(err)
			}
			dials := 0
			dial := opener.dial
			opener.dial = func(ctx context.Context, network, address string) (net.Conn, error) {
				dials++
				if mode == "tcp" {
					return nil, errors.New("SECRET_DIAGNOSTIC_CANARY")
				}
				return dial(ctx, network, address)
			}
			recorder := diagnostic.New()
			ctx := diagnostic.WithRecorder(context.Background(), recorder)
			conn, err := opener.Open(ctx, testRequest())
			if conn != nil {
				conn.Close()
			}
			if (err == nil) != (mode == "success") {
				t.Fatal("diagnostics changed open result")
			}
			if mode == "issue" || mode == "issue_timeout" || mode == "ticket" {
				if dials != 0 {
					t.Fatal("invalid ticket reached proxy")
				}
			} else if dials != 1 {
				t.Fatal("proxy dial count changed")
			}
			requireStage(t, recorder, stage, result, status)
			raw, _ := json.Marshal(recorder.Snapshot())
			if strings.Contains(string(raw), "SECRET_DIAGNOSTIC_CANARY") || strings.Contains(string(raw), testRequest().Binding.Authority) {
				t.Fatal("diagnostics contain source material")
			}
		})
	}
}

func TestProofDiagnosticsDistinguishRefreshAndVerification(t *testing.T) {
	for _, invalidProof := range []bool{false, true} {
		t.Run(map[bool]string{false: "refresh", true: "verify"}[invalidProof], func(t *testing.T) {
			f := newFixture(t)
			request := testRequest()
			grant := "original.signed.grant"
			request.Binding.Execution.GrantSHA256 = sourceproof.GrantDigest(grant)
			issuer := issueFunc(func(context.Context, IssueRequest) (string, error) {
				t.Error("failed proof reached issuer")
				return "", transportbroker.ErrOpen
			})
			base, err := New(f.config, issuer)
			if err != nil {
				t.Fatal(err)
			}
			scope := proofScope(base, request)
			opener, err := NewWithSourceProof(f.config, issuer, SourceProofConfig{PublicKey: f.key.Public().(ed25519.PublicKey), Scope: scope, Refresh: func(context.Context) (sourceproof.Envelope, error) {
				if !invalidProof {
					return sourceproof.Envelope{}, errors.New("SECRET_DIAGNOSTIC_CANARY")
				}
				other := scope
				other.RouteID = "other"
				now := time.Now()
				return sourceproof.Sign(f.key, other, grant, now, now.Add(5*time.Second))
			}})
			if err != nil {
				t.Fatal(err)
			}
			recorder := diagnostic.New()
			conn, err := opener.Open(diagnostic.WithRecorder(context.Background(), recorder), request)
			if conn != nil || err == nil {
				t.Fatal("invalid proof accepted")
			}
			if invalidProof {
				requireStage(t, recorder, diagnostic.ProofRefresh, diagnostic.Succeeded, 0)
				requireStage(t, recorder, diagnostic.ProofVerified, diagnostic.ScopeDenied, 0)
			} else {
				requireStage(t, recorder, diagnostic.ProofRefresh, diagnostic.Failed, 0)
			}
		})
	}
}

func TestDiagnosticHTTPStatusRejectsUntrustedText(t *testing.T) {
	for raw, want := range map[string]int{"HTTP/1.1 403 secret\r\n": 403, "HTTP/1.1 999 secret\r\n": 0, "HTTP/1.1 20x secret\r\n": 0, "HTTP/1.0 200 OK\r\n": 0, "HTTP/1.1 200xsecret\r\n": 0, "secret": 0} {
		if diagnosticHTTPStatus([]byte(raw)) != want {
			t.Fatal("status parser retained invalid data")
		}
	}
}

func TestDisabledFailureDiagnosticsAllocateNothing(t *testing.T) {
	ctx := context.Background()
	failure := errors.New("SECRET_DIAGNOSTIC_CANARY")
	if n := testing.AllocsPerRun(100, func() { recordDiagnosticFailure(ctx, diagnostic.TicketIssue, failure, 0) }); n != 0 {
		t.Fatalf("disabled failure diagnostics allocated: %v", n)
	}
}

func TestProofDiagnosticClassifiesExpiredContextWithoutChangingScopeError(t *testing.T) {
	for _, mode := range []string{"cancelled", "expired", "mismatch"} {
		t.Run(mode, func(t *testing.T) {
			f := newFixture(t)
			request := testRequest()
			base, err := New(f.config, f.issuer())
			if err != nil {
				t.Fatal(err)
			}
			scope := proofScope(base, request)
			opener, err := NewWithSourceProof(f.config, f.issuer(), SourceProofConfig{PublicKey: f.key.Public().(ed25519.PublicKey), Scope: scope, Refresh: func(context.Context) (sourceproof.Envelope, error) {
				t.Error("cancelled or mismatched proof called refresh")
				return sourceproof.Envelope{}, transportbroker.ErrOpen
			}})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			want := diagnostic.Cancelled
			if mode == "expired" {
				ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
				defer cancel()
				want = diagnostic.Timeout
			}
			issue := issueFor(opener, request)
			if mode == "mismatch" {
				issue.WorkerIdentity = "spiffe://other/worker"
				want = diagnostic.ScopeDenied
			}
			recorder := diagnostic.New()
			_, err = opener.refreshSourceProof(diagnostic.WithRecorder(ctx, recorder), issue)
			if !errors.Is(err, transportbroker.ErrScope) {
				t.Fatal("diagnostics changed scope error")
			}
			snapshot := recorder.Snapshot()
			if len(snapshot.Events) != 1 {
				t.Fatal("unexpected proof setup work")
			}
			requireStage(t, recorder, diagnostic.ProofVerified, want, 0)
		})
	}
}
