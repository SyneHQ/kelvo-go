//go:build linux

package childipc

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/transportbroker/diagnostic"
)

func TestServerDiagnosticsIdentifyPrivateSetupFailure(t *testing.T) {
	for _, mode := range []string{"caller", "dial", "deadline", "success"} {
		t.Run(mode, func(t *testing.T) {
			recorder := diagnostic.New()
			ctx := diagnostic.WithRecorder(context.Background(), recorder)
			echo, _ := echoDialer(t)
			dialer := dialFunc(func(ctx context.Context, network, address string) (net.Conn, error) {
				switch mode {
				case "dial":
					return nil, errors.New("SECRET_DIAGNOSTIC_CANARY")
				case "deadline":
					<-ctx.Done()
					return nil, ctx.Err()
				}
				return echo(ctx, network, address)
			})
			if mode == "deadline" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, 40*time.Millisecond)
				defer cancel()
			}
			server, file, err := NewPair(ctx, "source.private:5432", dialer, nil, 1)
			if err != nil {
				t.Fatal(err)
			}
			client, err := NewClient(file, "source.private:5432")
			if err != nil {
				closeServer(t, server)
				t.Fatal(err)
			}
			pid := os.Getpid()
			if mode == "caller" {
				pid++
			}
			done := make(chan error, 1)
			go func() { done <- server.Serve(pid) }()
			conn, err := client.DialContext(context.Background(), "tcp", "source.private:5432")
			if conn != nil {
				conn.Close()
			}
			client.Close()
			closeServer(t, server)
			<-done
			if (err == nil) != (mode == "success") {
				t.Fatal("diagnostics changed channel result")
			}
			stage, result := diagnostic.BrokerDial, diagnostic.Failed
			switch mode {
			case "caller":
				stage, result = diagnostic.IPCAuthorized, diagnostic.ScopeDenied
			case "deadline":
				result = diagnostic.Timeout
			case "success":
				stage, result = diagnostic.DescriptorSent, diagnostic.Succeeded
			}
			found := false
			for _, event := range recorder.Snapshot().Events {
				if event.Stage == stage && event.Result == result {
					found = true
				}
			}
			if !found {
				t.Fatalf("missing channel stage %s result %s", stage, result)
			}
			raw, _ := json.Marshal(recorder.Snapshot())
			if strings.Contains(string(raw), "SECRET_DIAGNOSTIC_CANARY") || strings.Contains(string(raw), "source.private") {
				t.Fatal("channel diagnostics contain source material")
			}
		})
	}
}

func TestDisabledDialFailureDiagnosticsAllocateNothing(t *testing.T) {
	ctx := context.Background()
	failure := errors.New("SECRET_DIAGNOSTIC_CANARY")
	if n := testing.AllocsPerRun(100, func() { recordDialFailure(ctx, failure) }); n != 0 {
		t.Fatalf("disabled dial diagnostics allocated: %v", n)
	}
}
