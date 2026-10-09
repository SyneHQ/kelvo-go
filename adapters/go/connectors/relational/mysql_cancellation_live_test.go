package relational

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
)

// Run only with a disposable database owned by the live-test controller.
func TestMySQLCancellationLive(t *testing.T) {
	value := os.Getenv("KELVO_SOURCE_DIALERS_LIVE_FD")
	if value == "" {
		t.Skip("disposable MySQL fixture required")
	}
	fd, err := strconv.Atoi(value)
	if err != nil || fd < 3 {
		t.Fatal("invalid fixture descriptor")
	}
	input := os.NewFile(uintptr(fd), "mysql-cancel-fixture")
	defer input.Close()
	raw, err := io.ReadAll(io.LimitReader(input, 64<<10))
	if err != nil {
		t.Fatal(err)
	}
	defer clear(raw)
	var f struct {
		Engine, PhysicalIP, Host, Username, Password, Database, CA string
		Port                                                       int
	}
	if json.Unmarshal(raw, &f) != nil || f.Engine != "mysql" || net.ParseIP(f.PhysicalIP) == nil || f.Host != "source.private.invalid" || f.Database != "kelvo_private_dialer_fixture" {
		t.Fatal("invalid fixture scope")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM([]byte(f.CA)) {
		t.Fatal("invalid fixture CA")
	}
	var dataCalls, cancelCalls atomic.Int32
	authority := net.JoinHostPort(f.Host, strconv.Itoa(f.Port))
	physical := net.JoinHostPort(f.PhysicalIP, strconv.Itoa(f.Port))
	hook := func(counter *atomic.Int32) sourceDialFunc {
		return func(ctx context.Context, network, address string) (net.Conn, error) {
			if network != "tcp" || address != authority {
				return nil, errors.New("changed source authority")
			}
			counter.Add(1)
			return (&net.Dialer{Timeout: time.Second}).DialContext(ctx, "tcp", physical)
		}
	}
	c := adapter.Connection{TenantID: "fixture-a", ConnectionID: "private-source", Revision: "one", Engine: "mysql", Host: f.Host, Port: f.Port, Namespace: f.Database, Username: f.Username, Password: f.Password, TLS: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}, DialContext: hook(&dataCalls), DialCancellation: hook(&cancelCalls)}
	ctx, stop := context.WithTimeout(context.Background(), 20*time.Second)
	defer stop()
	opened, err := NewMySQL().Open(ctx, c)
	if err != nil {
		t.Fatalf("data session: %T", err)
	}
	defer opened.Close()
	session := opened.(*Session)
	observerConnection := c
	observerConnection.DialCancellation = nil
	observed, err := NewMySQL().Open(ctx, observerConnection)
	if err != nil {
		t.Fatal("observer session failed")
	}
	defer observed.Close()
	observer := observed.(*Session)
	queryCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		sink := &readSink{}
		defer sink.Close()
		_, err := session.Query(queryCtx, adapter.Query{Statement: "SELECT SLEEP(10) AS value", MaxRows: 10, MaxBytes: 1 << 20, BatchRows: 10}, sink)
		result <- err
	}()
	active := func() int {
		t.Helper()
		var count int
		err := observer.Pool.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE ID <> CONNECTION_ID() AND USER = ? AND COMMAND = 'Query' AND INFO LIKE '%SLEEP(10)%'", f.Username).Scan(&count)
		if err != nil {
			t.Fatal("observer query failed")
		}
		return count
	}
	deadline := time.Now().Add(4 * time.Second)
	for active() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("slow query never became active")
		}
		time.Sleep(20 * time.Millisecond)
	}
	started := time.Now()
	cancel()
	err = <-result
	if !errors.Is(err, context.Canceled) || errors.Is(err, errMySQLCancellation) {
		t.Fatal("server cancellation was not confirmed", err)
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatal("server cancellation exceeded bounded control budget", elapsed)
	}
	if count := active(); count != 0 {
		t.Fatal("server query remained active after cancellation returned", count)
	}
	if cancelCalls.Load() != 1 {
		t.Fatal("cancel did not use exactly one separate reserved connection", cancelCalls.Load())
	}
	t.Log("observed active SLEEP; cancellation returned with zero active queries and one reserved control connection", time.Since(started))
}
