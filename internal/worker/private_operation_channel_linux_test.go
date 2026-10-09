//go:build linux

package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/internal/containment"
	"github.com/SYNEHQ/kelvo-go/internal/transportbroker"
	"github.com/SYNEHQ/kelvo-go/internal/transportbroker/childipc"
	"github.com/SYNEHQ/kelvo-go/operations"
)

type privateChannelOpener func(context.Context, transportbroker.OpenRequest) (net.Conn, error)

func (f privateChannelOpener) Open(ctx context.Context, r transportbroker.OpenRequest) (net.Conn, error) {
	return f(ctx, r)
}
func privateOperationFixtureChannel(input adapter.ProcessRequest) error {
	authority, err := privateSourceAuthority(input.Source)
	if err != nil {
		return err
	}
	client, err := childipc.NewClient(os.NewFile(7, "private-test-channel"), authority)
	if err != nil {
		return err
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	connection, err := client.DialContext(ctx, "tcp", authority)
	if err != nil {
		return err
	}
	defer connection.Close()
	connection.SetDeadline(time.Now().Add(time.Second))
	if _, err := connection.Write([]byte("ping")); err != nil {
		return err
	}
	var answer [4]byte
	if _, err := io.ReadFull(connection, answer[:]); err != nil {
		return err
	}
	if !bytes.Equal(answer[:], []byte("pong")) {
		return adapter.ErrInvalid
	}
	return nil
}
func privateChannelInput(t *testing.T) (adapter.ProcessRequest, transportbroker.Binding) {
	t.Helper()
	input := operationProcessInput(t, operations.StatementExecute, "fixture")
	input.AppTeam = "team-a"
	input.Source.Engine = "postgresql"
	input.Source.Password = ""
	input.Source.DSN = "postgresql://fixture:fixture@source.invalid:5432/customer?sslmode=verify-full"
	input.Source.Revision = strings.Repeat("a", 64)
	identity, _ := json.Marshal([]string{"shared", input.AppTeam})
	tenant := sha256.Sum256(append([]byte("kelvo.operation.adapter-tenant.v1\x00"), identity...))
	input.Source.TenantID = hex.EncodeToString(tenant[:])
	binding := transportbroker.Binding{Issuer: "issuer", Audience: "audience", ClusterTenant: "shared", ServicePrincipal: "api", Tenant: input.AppTeam, Source: input.Source.ConnectionID, SourceRevision: input.Source.Revision, Authority: "source.invalid:5432", ExpiresAt: time.Unix(input.ExpiresAt, 0), Execution: transportbroker.Execution{Kind: "operation", ID: input.OperationID, GrantSHA256: strings.Repeat("b", 64), Worker: "worker-a", Owner: strings.Repeat("c", 32), Claim: strings.Repeat("d", 32)}}
	return input, binding
}
func TestContainedPrivateOperationChannelAndCustody(t *testing.T) {
	for _, change := range []bool{false, true} {
		t.Run(map[bool]string{false: "matching source", true: "changed source"}[change], func(t *testing.T) {
			executor, manager, pool := containedExecutor(t)
			input, binding := privateChannelInput(t)
			var opens, releases atomic.Int32
			broker, err := transportbroker.New(transportbroker.Limits{MaxSessions: 1, MaxDataConnections: 1, MaxDataPerSession: 1}, privateChannelOpener(func(ctx context.Context, request transportbroker.OpenRequest) (net.Conn, error) {
				opens.Add(1)
				if request.Binding != binding || request.Purpose != transportbroker.Data {
					return nil, adapter.ErrInvalid
				}
				local, remote := net.Pipe()
				go func() {
					defer remote.Close()
					remote.SetDeadline(time.Now().Add(time.Second))
					var input [4]byte
					if _, err := io.ReadFull(remote, input[:]); err == nil && string(input[:]) == "ping" {
						remote.Write([]byte("pong"))
					}
				}()
				return local, nil
			}))
			if err != nil {
				t.Fatal(err)
			}
			defer broker.Close(context.Background())
			receipt, err := executor.executeResolvedOperation(context.Background(), operationExecutable(t), input.OperationID, input.RequestSHA256, func(context.Context) (adapter.ProcessRequest, *privateOperationChannel, error) {
				if pool.Snapshot().Active != 1 || manager.Status().Active != 1 {
					t.Error("transport prepared before containment admission")
				}
				session, err := broker.Admit(context.Background(), binding)
				if err != nil {
					return input, nil, err
				}
				channel, err := newPrivateOperationChannel(context.Background(), input, session, binding, 2, func() { releases.Add(1) })
				if err != nil {
					session.Close(context.Background())
					return input, nil, err
				}
				if change {
					input.Source.DSN = strings.Replace(input.Source.DSN, "source.invalid", "other.invalid", 1)
				}
				return input, channel, nil
			}, nil)
			if change {
				if err == nil || receipt.Outcome != operations.Rejected || receipt.Effect != operations.EffectNone || opens.Load() != 0 {
					t.Fatal("changed input used private transport", receipt, err)
				}
			} else if err != nil || receipt.Outcome != operations.Completed || receipt.Effect != operations.EffectCommitted || opens.Load() != 1 {
				t.Fatal("contained private operation failed", receipt, err, opens.Load())
			}
			if pool.Snapshot().Active != 0 || manager.Status().Active != 0 || broker.Snapshot().Sessions != 0 || releases.Load() != 1 {
				t.Fatal("private operation leaked custody")
			}
		})
	}
}

// A completed SQL receipt does not prove that the parent closed its source stream.
type privateCloseFixture struct {
	net.Conn
	entered chan struct{}
	resume  chan struct{}
	fail    bool
	once    sync.Once
}

func (c *privateCloseFixture) Close() error {
	c.once.Do(func() { close(c.entered) })
	if c.resume != nil {
		<-c.resume
	}
	err := c.Conn.Close()
	if c.fail {
		return errors.New("fixture source close failed")
	}
	return err
}

func TestContainedPrivateCompletedReceiptWaitsForPhysicalClose(t *testing.T) {
	executor, manager, pool := containedExecutor(t)
	input, binding := privateChannelInput(t)
	entered, resume := make(chan struct{}), make(chan struct{})
	var releases, notified atomic.Int32
	broker, err := transportbroker.New(transportbroker.Limits{MaxSessions: 1, MaxDataConnections: 1, MaxDataPerSession: 1}, privateChannelOpener(func(context.Context, transportbroker.OpenRequest) (net.Conn, error) {
		local, remote := net.Pipe()
		go func() {
			defer remote.Close()
			remote.SetDeadline(time.Now().Add(time.Second))
			var ping [4]byte
			if _, err := io.ReadFull(remote, ping[:]); err == nil {
				remote.Write([]byte("pong"))
			}
		}()
		return &privateCloseFixture{Conn: local, entered: entered, resume: resume}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer broker.Close(context.Background())
	sink := &operationCleanupTestSink{cleaned: func(context.Context) error { notified.Add(1); return nil }}
	type result struct {
		receipt operations.Receipt
		err     error
	}
	done := make(chan result, 1)
	go func() {
		receipt, err := executor.executeResolvedOperation(context.Background(), operationExecutable(t), input.OperationID, input.RequestSHA256, func(context.Context) (adapter.ProcessRequest, *privateOperationChannel, error) {
			session, err := broker.Admit(context.Background(), binding)
			if err != nil {
				return input, nil, err
			}
			channel, err := newPrivateOperationChannel(context.Background(), input, session, binding, 2, func() { releases.Add(1) })
			return input, channel, err
		}, sink)
		done <- result{receipt, err}
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		close(resume)
		t.Fatal("source close did not start")
	}
	if pool.Snapshot().Active != 1 || broker.Snapshot().Sessions != 1 || releases.Load() != 0 || notified.Load() != 0 {
		close(resume)
		t.Fatal("unclosed stream released custody")
	}
	select {
	case <-done:
		close(resume)
		t.Fatal("execution returned before physical close")
	default:
	}
	close(resume)
	select {
	case got := <-done:
		if got.err != nil || got.receipt.Outcome != operations.Completed || got.receipt.Effect != operations.EffectCommitted {
			t.Fatal("committed receipt lost", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("physical close did not complete")
	}
	if pool.Snapshot().Active != 0 || manager.Status().Active != 0 || broker.Snapshot().Sessions != 0 || releases.Load() != 1 || notified.Load() != 1 {
		t.Fatal("joined close did not release custody")
	}
}

// Run this case in a disposable process. Failed physical cleanup intentionally
// retains admission and manager ownership until the supervisor removes the process.
func TestContainedPrivateCommittedReceiptRetainsFailedCloseCustody(t *testing.T) {
	executor, manager, pool := containedExecutorWithClose(t, containment.ErrQuarantined)
	input, binding := privateChannelInput(t)
	var releases, notified atomic.Int32
	entered := make(chan struct{})
	broker, err := transportbroker.New(transportbroker.Limits{MaxSessions: 1, MaxDataConnections: 1, MaxDataPerSession: 1}, privateChannelOpener(func(context.Context, transportbroker.OpenRequest) (net.Conn, error) {
		local, remote := net.Pipe()
		go func() {
			defer remote.Close()
			remote.SetDeadline(time.Now().Add(time.Second))
			var ping [4]byte
			if _, err := io.ReadFull(remote, ping[:]); err == nil {
				remote.Write([]byte("pong"))
			}
		}()
		return &privateCloseFixture{Conn: local, entered: entered, fail: true}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	sink := &operationCleanupTestSink{cleaned: func(context.Context) error { notified.Add(1); return nil }}
	receipt, err := executor.executeResolvedOperation(context.Background(), operationExecutable(t), input.OperationID, input.RequestSHA256, func(context.Context) (adapter.ProcessRequest, *privateOperationChannel, error) {
		session, err := broker.Admit(context.Background(), binding)
		if err != nil {
			return input, nil, err
		}
		channel, err := newPrivateOperationChannel(context.Background(), input, session, binding, 2, func() { releases.Add(1) })
		return input, channel, err
	}, sink)
	if err == nil || receipt.Outcome != operations.Completed || receipt.Effect != operations.EffectCommitted {
		t.Fatal("cleanup failure hid committed mutation", receipt, err)
	}
	if pool.Snapshot().Active != 1 || manager.Status().Active != 0 || !errors.Is(manager.Err(), containment.ErrQuarantined) || broker.Snapshot().Sessions != 1 || releases.Load() != 0 || notified.Load() != 0 {
		t.Fatal("failed source close released custody", pool.Snapshot(), manager.Status(), broker.Snapshot(), releases.Load(), notified.Load())
	}
	closeContext, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if broker.Close(closeContext) == nil {
		t.Fatal("unconfirmed source close completed broker shutdown")
	}
}

func TestPrivateChannelCloseReleasesOnce(t *testing.T) {
	input, binding := privateChannelInput(t)
	broker, err := transportbroker.New(transportbroker.Limits{MaxSessions: 1, MaxDataConnections: 1, MaxDataPerSession: 1}, privateChannelOpener(func(context.Context, transportbroker.OpenRequest) (net.Conn, error) { return nil, adapter.ErrInvalid }))
	if err != nil {
		t.Fatal(err)
	}
	defer broker.Close(context.Background())
	session, err := broker.Admit(context.Background(), binding)
	if err != nil {
		t.Fatal(err)
	}
	releases := 0
	channel, err := newPrivateOperationChannel(context.Background(), input, session, binding, 2, func() { releases++ })
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for i := 0; i < 2; i++ {
		if err := channel.close(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if releases != 1 {
		t.Fatal("repeated close released ownership more than once", releases)
	}
}
