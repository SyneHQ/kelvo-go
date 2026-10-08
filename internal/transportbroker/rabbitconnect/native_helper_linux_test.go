//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package rabbitconnect

import (
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/transportbroker"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// The actual Rabbit fixture invokes this test binary. Generated test keys and
// socket-only fixture addresses arrive through bounded stdin, never argv or
// environment. This is not a production issuer or a child transport protocol.
type nativeHelperInput struct {
	Version  int          `json:"version"`
	Mode     string       `json:"mode"`
	Proxy    Config       `json:"proxy"`
	Claims   ticketClaims `json:"claims"`
	Key      []byte       `json:"key"`
	SourceCA []byte       `json:"source_ca"`
	Database string       `json:"database"`
	AdminURL string       `json:"admin_url"`
}

type nativeHelperResult struct {
	Version int    `json:"version"`
	Mode    string `json:"mode"`
	Rows    int64  `json:"rows"`
	Opens   int32  `json:"opens"`
}

func TestRabbitNativePostgresHelper(t *testing.T) {
	if os.Getenv("KELVO_RABBIT_NATIVE_HELPER") != "1" {
		t.Skip("requires the actual Rabbit native fixture coordinator")
	}
	input, err := readNativeHelperInput(os.Stdin)
	if err != nil {
		t.Fatal("invalid native fixture input")
	}
	c := input.Claims
	binding := transportbroker.Binding{Issuer: c.Issuer, Audience: c.Audience,
		ClusterTenant: c.ClusterTenant, ServicePrincipal: c.ServicePrincipal,
		Tenant: c.Tenant, Source: c.Source, SourceRevision: c.SourceRevision, Authority: c.Authority,
		Execution: transportbroker.Execution{Kind: c.Execution.Kind, ID: c.Execution.ID, GrantSHA256: c.Execution.GrantSHA256,
			Worker: c.Execution.Worker, Owner: c.Execution.Owner, Claim: c.Execution.Claim}, ExpiresAt: time.Now().Add(90 * time.Second)}
	issuer := issueFunc(func(ctx context.Context, request IssueRequest) (string, error) {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		claims := c
		claims.ID, claims.WorkerIdentity, claims.WorkerCertSHA256 = request.OpenID, request.WorkerIdentity, request.WorkerCertSHA256
		claims.IssuedAt = time.Now().Unix()
		claims.ExpiresAt, claims.SessionExpiresAt = claims.IssuedAt+15, request.Binding.ExpiresAt.Unix()
		return signTicket(ed25519.PrivateKey(input.Key), claims), nil
	})
	opener, err := New(input.Proxy, issuer)
	if err != nil {
		t.Fatal("native fixture opener configuration failed")
	}
	var opens atomic.Int32
	broker, err := transportbroker.New(transportbroker.Limits{MaxSessions: 1, MaxDataConnections: 2, MaxDataPerSession: 2}, nativeCountOpener{opener, &opens})
	if err != nil {
		t.Fatal("native fixture broker configuration failed")
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := broker.Close(ctx); err != nil {
			t.Error("native broker cleanup did not join")
		}
		s := broker.Snapshot()
		if s.Sessions != 0 || s.DataConnections != 0 || s.CancellationConnections != 0 || s.Opening != 0 || s.CleanupFailed {
			t.Error("native broker retained unfinished resources")
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	session, err := broker.Admit(ctx, binding)
	if err != nil {
		t.Fatal("native fixture broker admission failed")
	}
	config := nativePGConfig(t, input, session.DataDialer())
	conn, err := pgx.ConnectConfig(ctx, config)
	if input.Mode == "denied" {
		if conn != nil {
			_ = conn.Close(ctx)
		}
		if err == nil || opens.Load() != 0 {
			t.Fatal("denied scope reached a native source connection")
		}
	} else if input.Mode == "hostname" {
		if conn != nil {
			_ = conn.Close(ctx)
		}
		var hostnameError x509.HostnameError
		if !errors.As(err, &hostnameError) || opens.Load() != 1 {
			t.Fatal("native driver did not verify original source hostname")
		}
	} else {
		if err != nil {
			t.Fatal("native PostgreSQL connection through Rabbit failed")
		}
		t.Cleanup(func() {
			ctx, stop := context.WithTimeout(context.Background(), 3*time.Second)
			defer stop()
			_ = conn.Close(ctx)
		})
		if input.Mode == "rows" {
			verifyNativePGRows(t, ctx, conn)
			if opens.Load() != 1 {
				t.Fatal("native row stream unexpectedly reopened its source")
			}
		} else {
			verifyNativePGCancel(t, ctx, conn, input.AdminURL, &opens)
		}
	}
	result := nativeHelperResult{Version: 1, Mode: input.Mode, Opens: opens.Load()}
	if input.Mode == "rows" {
		result.Rows = 100000
	}
	encoded, _ := json.Marshal(result)
	fmt.Println("KELVO_NATIVE_RESULT:" + string(encoded))
}

func readNativeHelperInput(source io.Reader) (nativeHelperInput, error) {
	var input nativeHelperInput
	data, err := io.ReadAll(io.LimitReader(source, (256<<10)+1))
	if err != nil || len(data) > 256<<10 {
		return input, transportbroker.ErrInvalid
	}
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&input) != nil || decoder.Decode(new(any)) != io.EOF || input.Version != 1 ||
		len(input.Key) != ed25519.PrivateKeySize || len(input.SourceCA) == 0 ||
		!strings.HasPrefix(input.Database, "rabbit_transport_") || len(input.Database) != len("rabbit_transport_")+32 {
		return input, transportbroker.ErrInvalid
	}
	for _, char := range strings.TrimPrefix(input.Database, "rabbit_transport_") {
		if !(char >= 'a' && char <= 'f' || char >= '0' && char <= '9') {
			return input, transportbroker.ErrInvalid
		}
	}
	admin, err := url.Parse(input.AdminURL)
	if err != nil || (admin.Scheme != "postgres" && admin.Scheme != "postgresql") || admin.Host != "" ||
		admin.Path != "/"+input.Database || !strings.HasPrefix(admin.Query().Get("host"), "/") ||
		admin.User == nil || admin.User.Username() != "postgres" {
		return input, transportbroker.ErrInvalid
	}
	switch input.Mode {
	case "rows", "cancel", "denied", "hostname":
	default:
		return input, transportbroker.ErrInvalid
	}
	return input, nil
}

type nativeCountOpener struct {
	*Opener
	opens *atomic.Int32
}

func (o nativeCountOpener) Open(ctx context.Context, request transportbroker.OpenRequest) (net.Conn, error) {
	conn, err := o.Opener.Open(ctx, request)
	if err == nil && conn != nil {
		o.opens.Add(1)
	}
	return conn, err
}

func nativePGConfig(t *testing.T, input nativeHelperInput, dialer transportbroker.Dialer) *pgx.ConnConfig {
	t.Helper()
	u := url.URL{Scheme: "postgres", User: url.User("postgres"), Host: input.Claims.Authority, Path: "/" + input.Database}
	u.RawQuery = "sslmode=verify-full&connect_timeout=5&application_name=kelvo-native-fixture&default_transaction_read_only=on&statement_timeout=25000"
	config, err := pgx.ParseConfig(u.String())
	if err != nil {
		t.Fatal("cannot parse native source configuration")
	}
	host, _, _ := net.SplitHostPort(input.Claims.Authority)
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(input.SourceCA) {
		t.Fatal("invalid native source CA")
	}
	config.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS13, ServerName: host, RootCAs: roots}
	config.Fallbacks = nil
	config.LookupFunc = func(context.Context, string) ([]string, error) { return []string{host}, nil }
	config.DialFunc = dialer.DialContext
	return config
}

func verifyNativePGRows(t *testing.T, ctx context.Context, conn *pgx.Conn) {
	t.Helper()
	var count, sum int64
	var readOnly string
	err := conn.QueryRow(ctx, `WITH grouped AS (SELECT category,count(*) AS n,sum(id) AS s FROM private_connect_rows GROUP BY category)
SELECT sum(n)::BIGINT,sum(s)::BIGINT,current_setting('transaction_read_only') FROM grouped`).Scan(&count, &sum, &readOnly)
	if err != nil || count != 100000 || sum != 5000050000 || readOnly != "on" {
		t.Fatal("native CTE changed content or read-only scope")
	}
	rows, err := conn.Query(ctx, `SELECT id,category,note FROM private_connect_rows ORDER BY id`)
	if err != nil {
		t.Fatal("native row stream failed")
	}
	defer rows.Close()
	var seen int64
	for rows.Next() {
		var id, category int64
		var note *string
		if rows.Scan(&id, &category, &note) != nil {
			t.Fatal("native row decode failed")
		}
		seen++
		if id != seen || category != id%17 || (note != nil) != (id%13 != 0) || note != nil && *note != "row-"+strconv.FormatInt(id, 10) {
			t.Fatal("native row content changed")
		}
	}
	if rows.Err() != nil || seen != 100000 {
		t.Fatal("native row stream was incomplete")
	}
}

func verifyNativePGCancel(t *testing.T, ctx context.Context, conn *pgx.Conn, adminURL string, opens *atomic.Int32) {
	t.Helper()
	parsed, err := url.Parse(adminURL)
	if err != nil || parsed.Query().Get("host") == "" || !strings.HasPrefix(parsed.Query().Get("host"), "/") || parsed.Host != "" {
		t.Fatal("cancellation requires the owned socket-only administrator")
	}
	admin, err := pgx.Connect(ctx, adminURL)
	if err != nil {
		t.Fatal("cannot inspect owned source backend")
	}
	defer admin.Close(ctx)
	pid := conn.PgConn().PID()
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		_, err := admin.Exec(cleanup, `SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE pid=$1 AND application_name='kelvo-native-fixture' AND datname=current_database()`, pid)
		if err != nil {
			t.Error("owned source cancellation cleanup failed")
		}
	}()
	done := make(chan struct{})
	var queryErr error
	go func() {
		defer close(done)
		_, queryErr = conn.Exec(ctx, `SELECT pg_sleep(20)`)
	}()
	// Always join the native query goroutine, including assertion failures.
	defer func() {
		_ = conn.PgConn().Conn().Close()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("native query goroutine did not join")
		}
	}()
	waitNativeHelperQuery(t, ctx, admin, pid, true)
	before := opens.Load()
	started := time.Now()
	cancelCtx, stop := context.WithTimeout(ctx, 5*time.Second)
	defer stop()
	// v1 has no downstream reserved cancellation lane. pgx itself opens this
	// second ordinary socket; this verifies only uncongested native cancellation.
	if conn.PgConn().CancelRequest(cancelCtx) != nil {
		t.Fatal("native CancelRequest through Rabbit failed")
	}
	select {
	case <-done:
		var pgError *pgconn.PgError
		if !errors.As(queryErr, &pgError) || pgError.Code != "57014" {
			t.Fatal("source did not acknowledge native query cancellation")
		}
	case <-cancelCtx.Done():
		t.Fatal("native query cancellation exceeded its deadline")
	}
	if opens.Load() != before+1 || time.Since(started) >= 5*time.Second {
		t.Fatal("native cancellation did not use one fresh bounded CONNECT")
	}
	waitNativeHelperQuery(t, ctx, admin, pid, false)
}

func waitNativeHelperQuery(t *testing.T, parent context.Context, admin *pgx.Conn, pid uint32, active bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 3*time.Second)
	defer cancel()
	for {
		var running bool
		err := admin.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND application_name='kelvo-native-fixture' AND datname=current_database() AND state='active' AND query='SELECT pg_sleep(20)')`, pid).Scan(&running)
		if err != nil {
			t.Fatal("cannot inspect owned native query state")
		}
		if running == active {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("owned native query did not reach the required state")
		case <-time.After(20 * time.Millisecond):
		}
	}
}
