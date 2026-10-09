//go:build linux

package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"os"
	"os/exec"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/internal/transportbroker/childipc"
	"github.com/SYNEHQ/kelvo-go/operations"
)

type privateRuntimeDialer func(context.Context, string, string) (net.Conn, error)

func (d privateRuntimeDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	return d(ctx, network, address)
}

func TestPrivateRuntimeChildProcess(t *testing.T) {
	if os.Getenv("KELVO_PRIVATE_RUNTIME_CHILD") != "1" {
		return
	}
	var receipt bytes.Buffer
	err := (Runner{}).Run(context.Background(), os.Stdin, io.Discard, &receipt)
	var result operations.Receipt
	if err == nil || json.Unmarshal(receipt.Bytes(), &result) != nil || result.ErrorCode != "SOURCE_FAILED" {
		os.Exit(31)
	}
	os.Exit(0)
}
func TestPrivateRuntimeUsesInheritedIPCWithoutDNSFallback(t *testing.T) {
	request := runtimeRequest(t, operations.QueryRead)
	request.Source.DSN = "postgresql://fixture:fixture@private-source.invalid:5432/warehouse?sslmode=verify-full"
	request.Source.Database = "warehouse"
	request.Request.Connection.Database = "warehouse"
	request.RequestSHA256, _ = operations.Digest(request.Request)
	request.PrivateTransport = &adapter.PrivateTransport{ControlFD: 7}
	request.CredentialsValidUntil = time.Now().Add(5 * time.Second).Unix()
	raw, err := adapter.EncodeProcessRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	var dataCalls, cancelCalls atomic.Int32
	data := privateRuntimeDialer(func(ctx context.Context, network, address string) (net.Conn, error) {
		dataCalls.Add(1)
		if network != "tcp" || address != "private-source.invalid:5432" {
			return nil, adapter.ErrInvalid
		}
		client, server := net.Pipe()
		go func() {
			defer server.Close()
			server.SetDeadline(time.Now().Add(time.Second))
			var ssl [8]byte
			if _, err := io.ReadFull(server, ssl[:]); err == nil {
				server.Write([]byte{'N'})
			}
		}()
		return client, nil
	})
	cancellation := privateRuntimeDialer(func(context.Context, string, string) (net.Conn, error) {
		cancelCalls.Add(1)
		return nil, adapter.ErrInvalid
	})
	control, child, err := childipc.NewPair(context.Background(), "private-source.invalid:5432", data, cancellation, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer child.Close()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := control.Close(ctx); err != nil {
			t.Error(err)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestPrivateRuntimeChildProcess$")
	command.Env = append(os.Environ(), "KELVO_PRIVATE_RUNTIME_CHILD=1")
	command.ExtraFiles = []*os.File{nil, nil, nil, nil, child}
	command.Stdin = bytes.NewReader(raw)
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	child.Close()
	served := make(chan error, 1)
	go func() { served <- control.Serve(command.Process.Pid) }()
	if err := command.Wait(); err != nil {
		t.Fatalf("private child failed: %v: %s", err, output.String())
	}
	if dataCalls.Load() != 1 || cancelCalls.Load() != 0 {
		t.Fatal("driver did not use exact inherited source dial", dataCalls.Load(), cancelCalls.Load())
	}
	select {
	case <-served:
	case <-time.After(3 * time.Second):
		t.Fatal("private child channel did not finish")
	}
}
