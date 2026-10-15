package relational

import (
	"context"
	"crypto/tls"
	"database/sql"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/adapters/go/connectors/sqlsession"
	"github.com/apache/arrow-go/v18/arrow"
	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/stdlib"
	mssql "github.com/microsoft/go-mssqldb"
)

// Round-trip measurement against a disposable database reached through a
// delaying, counting TCP proxy. KELVO_RT_<ENGINE>=user:pass@host:port/db with
// ENGINE in POSTGRESQL, MYSQL, MARIADB, SQLSERVER. Skipped when unset.
//
// A "round trip" is a flight turnaround: a client->server chunk that follows
// server->client traffic (or the start). Pipelined messages in one flight
// count once, which is exactly what costs latency on a high-RTT link.

// delayProxy forwards TCP, delaying every chunk by oneWay in each direction
// without serialising chunks (pipelined traffic keeps its pipelining).
type delayProxy struct {
	ln      net.Listener
	target  string
	oneWay  time.Duration
	flights atomic.Int64
}

func newDelayProxy(t *testing.T, target string, oneWay time.Duration) *delayProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &delayProxy{ln: ln, target: target, oneWay: oneWay}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go p.serve(c)
		}
	}()
	return p
}

func (p *delayProxy) addr() string { return p.ln.Addr().String() }

// connDir remembers the last traffic direction of one proxied connection.
type connDir struct {
	mu   sync.Mutex
	last int // 0 none, 1 c->s, 2 s->c
}

func (p *delayProxy) note(d *connDir, dir int) {
	d.mu.Lock()
	if dir == 1 && d.last != 1 {
		p.flights.Add(1)
	}
	d.last = dir
	d.mu.Unlock()
}

func (p *delayProxy) serve(client net.Conn) {
	server, err := net.Dial("tcp", p.target)
	if err != nil {
		_ = client.Close()
		return
	}
	d := &connDir{}
	go p.pipe(client, server, 1, d)
	p.pipe(server, client, 2, d)
}

type chunk struct {
	at   time.Time
	data []byte
}

func (p *delayProxy) pipe(src, dst net.Conn, dir int, d *connDir) {
	queue := make(chan chunk, 4096)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for c := range queue {
			time.Sleep(time.Until(c.at))
			if _, err := dst.Write(c.data); err != nil {
				for range queue {
				}
				return
			}
		}
	}()
	buf := make([]byte, 64<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			p.note(d, dir)
			queue <- chunk{time.Now().Add(p.oneWay), slices.Clone(buf[:n])}
		}
		if err != nil {
			break
		}
	}
	close(queue)
	<-done
	_ = dst.Close()
	_ = src.Close()
}

func liveSession(t *testing.T, engine string, proxyAddr string, creds string) *Session {
	t.Helper()
	userpass, rest, _ := strings.Cut(creds, "@")
	user, pass, _ := strings.Cut(userpass, ":")
	_, db, _ := strings.Cut(rest, "/")
	host, portText, _ := net.SplitHostPort(proxyAddr)
	port, _ := strconv.Atoi(portText)
	c := adapter.Connection{Engine: engine, TenantID: "t", ConnectionID: "c", Revision: "r", Host: host, Port: port, Username: user, Password: pass, Namespace: db, Options: map[string]string{"sslmode": "disable"}}
	var pool *sql.DB
	switch engine {
	case "postgresql":
		config, err := postgresConfig(c)
		if err != nil {
			t.Fatal(err)
		}
		pool = stdlib.OpenDB(*config, stdlib.OptionShouldPing(postgresShouldPing))
	case "mysql", "mariadb":
		config, err := mysqlConfig(c)
		if err != nil {
			t.Fatal(err)
		}
		config.TLS = nil // the disposable fixture speaks plaintext
		connector, err := mysqldriver.NewConnector(config)
		if err != nil {
			t.Fatal(err)
		}
		pool = sql.OpenDB(connector)
	case "sqlserver":
		config, err := sqlserverConfig(c)
		if err != nil {
			t.Fatal(err)
		}
		config.TLSConfig = &tls.Config{InsecureSkipVerify: true} // disposable self-signed fixture
		pool = sql.OpenDB(mssql.NewConnectorConfig(config))
	}
	t.Cleanup(func() { _ = pool.Close() })
	s := &Session{Session: &sqlsession.Session{Pool: pool, Engine: engine}, database: db}
	s.KeepOpen()
	return s
}

type nopSink struct{}

func (nopSink) Schema(*arrow.Schema) error    { return nil }
func (nopSink) Write(arrow.RecordBatch) error { return nil }
func rtQuery(engine string) adapter.Query {
	stmt := "SELECT 1 AS a, 'x' AS b"
	return adapter.Query{Statement: stmt, MaxRows: 10, MaxBytes: 1 << 20, BatchRows: 10}
}

// KELVO_RT_DELAY_MS is the injected round-trip time (default 80).
func TestReadRoundTripsLive(t *testing.T) {
	rttMS := 80
	if v, err := strconv.Atoi(os.Getenv("KELVO_RT_DELAY_MS")); err == nil {
		rttMS = v
	}
	for _, engine := range []string{"postgresql", "mysql", "mariadb", "sqlserver"} {
		creds := os.Getenv("KELVO_RT_" + strings.ToUpper(engine))
		if creds == "" {
			continue
		}
		t.Run(engine, func(t *testing.T) {
			_, rest, _ := strings.Cut(creds, "@")
			target, _, _ := strings.Cut(rest, "/")
			proxy := newDelayProxy(t, target, time.Duration(rttMS)*time.Millisecond/2)
			s := liveSession(t, engine, proxy.addr(), creds)
			ctx := context.Background()
			read := func() time.Duration {
				ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
				defer cancel()
				start := time.Now()
				if _, err := s.Query(ctx, rtQuery(engine), nopSink{}); err != nil {
					t.Error(err)
				}
				return time.Since(start)
			}
			// Warm PoolSize connections and their statement caches, as a busy
			// worker has; sequential reads then always find an idle connection.
			for range 2 {
				var wg sync.WaitGroup
				for range PoolSize {
					wg.Add(1)
					go func() { defer wg.Done(); read() }()
				}
				wg.Wait()
			}
			time.Sleep(2 * time.Second) // let any asynchronous cleanup flights land
			before := proxy.flights.Load()
			const n = 10
			var d []time.Duration
			idle, _ := strconv.Atoi(os.Getenv("KELVO_RT_IDLE_MS")) // gap between reads
			for range n {
				time.Sleep(time.Duration(idle) * time.Millisecond)
				d = append(d, read())
			}
			time.Sleep(time.Second)
			flights := float64(proxy.flights.Load()-before) / n
			slices.Sort(d)
			t.Logf("ENGINE=%s rtt=%dms flights/read=%.1f p50=%v", engine, rttMS, flights, d[n/2])
			// CI ceilings (KELVO_RT_ASSERT=1), with margin over the measured
			// values: flights include the background ROLLBACK; the p50 ceiling in
			// RTTs covers only what the caller waits for. Before this change
			// PostgreSQL measured 4.1 flights / 4.1 RTT, MySQL 5.0 / 5.2.
			if os.Getenv("KELVO_RT_ASSERT") == "1" && rttMS >= 40 {
				ceiling := map[string][2]float64{"postgresql": {3.6, 3.0}, "mysql": {4.5, 4.0}, "mariadb": {4.5, 4.0}, "sqlserver": {3.5, 3.0}}[engine]
				if flights > ceiling[0] || float64(d[n/2]) > ceiling[1]*float64(rttMS)*float64(time.Millisecond) {
					t.Fatalf("round trips regressed: flights/read=%.1f (max %.1f) p50=%v (max %.1f RTT)", flights, ceiling[0], d[n/2], ceiling[1])
				}
			}
		})
	}
}

// firstRowSink records the first row of the result as strings.
type firstRowSink struct{ row []string }

func (s *firstRowSink) Schema(*arrow.Schema) error { return nil }
func (s *firstRowSink) Write(r arrow.RecordBatch) error {
	if s.row == nil && r.NumRows() > 0 {
		for i := 0; i < int(r.NumCols()); i++ {
			if r.Column(i).IsNull(0) {
				s.row = append(s.row, "<null>")
			} else {
				s.row = append(s.row, r.Column(i).ValueStr(0))
			}
		}
	}
	return nil
}

func liveRead(s *Session, ctx context.Context, statement string) (*firstRowSink, error) {
	sink := &firstRowSink{}
	_, err := s.Query(ctx, adapter.Query{Statement: statement, MaxRows: 10, MaxBytes: 1 << 20, BatchRows: 10}, sink)
	waitRollbacks()
	return sink, err
}

// Live PostgreSQL behaviour that the single-round-trip prelude must preserve:
// read-only transaction, server-enforced deadline, cancellation, discard on
// failure, value fidelity, and no connection left inside a transaction.
func TestPostgresReadSemanticsLive(t *testing.T) {
	creds := os.Getenv("KELVO_RT_POSTGRESQL")
	if creds == "" {
		t.Skip("KELVO_RT_POSTGRESQL not set")
	}
	_, rest, _ := strings.Cut(creds, "@")
	target, _, _ := strings.Cut(rest, "/")
	proxy := newDelayProxy(t, target, 0)
	s := liveSession(t, "postgresql", proxy.addr(), creds)
	admin, err := sql.Open("pgx", "postgres://"+strings.Replace(creds, "@", "@", 1)+"?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	ctx := context.Background()

	t.Run("fidelity", func(t *testing.T) {
		sink, err := liveRead(s, ctx, "SELECT NULL::int AS n, 12345678901234567890.1234::numeric(30,4) AS d, TIMESTAMPTZ '2024-01-02 03:04:05.123456+00' AS ts, 9223372036854775807::bigint AS b")
		if err != nil {
			t.Fatal(err)
		}
		if len(sink.row) != 4 || sink.row[0] != "<null>" || sink.row[1] != "12345678901234567890.1234" || !strings.HasPrefix(sink.row[2], "2024-01-02T03:04:05.123456") || sink.row[3] != "9223372036854775807" {
			t.Fatalf("value fidelity lost: %q", sink.row)
		}
	})
	t.Run("read only and server deadline installed", func(t *testing.T) {
		c, cancel := context.WithTimeout(ctx, 7*time.Second)
		defer cancel()
		sink, err := liveRead(s, c, "SELECT current_setting('transaction_read_only'), current_setting('statement_timeout')")
		if err != nil || len(sink.row) != 2 || sink.row[0] != "on" {
			t.Fatalf("not a read-only transaction: %v %q", err, sink.row)
		}
		ms, err := strconv.Atoi(strings.TrimSuffix(sink.row[1], "ms"))
		if err != nil || ms < 1 || ms > 7000 {
			t.Fatalf("server deadline missing or too long: %q", sink.row[1])
		}
	})
	t.Run("server rejects writes inside the read", func(t *testing.T) {
		if _, err := admin.Exec("CREATE SEQUENCE IF NOT EXISTS rt_seq"); err != nil {
			t.Fatal(err)
		}
		if _, err := liveRead(s, ctx, "SELECT nextval('rt_seq')"); err == nil {
			t.Fatal("write succeeded inside a read-only transaction")
		}
	})
	t.Run("deadline cancels server side", func(t *testing.T) {
		c, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		start := time.Now()
		if _, err := liveRead(s, c, "SELECT pg_sleep(30)"); err == nil || time.Since(start) > 5*time.Second {
			t.Fatalf("pg_sleep beyond the deadline was not cancelled: %v after %v", err, time.Since(start))
		}
		time.Sleep(300 * time.Millisecond)
		var running int
		if err := admin.QueryRow("SELECT count(*) FROM pg_stat_activity WHERE query LIKE 'SELECT pg_sleep(30)%' AND state = 'active' AND pid <> pg_backend_pid()").Scan(&running); err != nil || running != 0 {
			t.Fatalf("query still running on the server: %d %v", running, err)
		}
	})
	t.Run("cancellation", func(t *testing.T) {
		c, cancel := context.WithCancel(ctx)
		go func() { time.Sleep(300 * time.Millisecond); cancel() }()
		start := time.Now()
		if _, err := liveRead(s, c, "SELECT pg_sleep(30)"); err == nil || time.Since(start) > 5*time.Second {
			t.Fatalf("cancel not honoured: %v", err)
		}
	})
	t.Run("failure discards the connection and the pool recovers", func(t *testing.T) {
		before := s.Pool.Stats().OpenConnections
		if _, err := liveRead(s, ctx, "SELECT 1/0"); err == nil {
			t.Fatal("expected division error")
		}
		if s.Pool.Stats().OpenConnections >= before && before > 0 {
			t.Fatalf("failed connection retained: %d -> %d", before, s.Pool.Stats().OpenConnections)
		}
		if sink, err := liveRead(s, ctx, "SELECT 1"); err != nil || sink.row[0] != "1" {
			t.Fatalf("pool unusable after discard: %v", err)
		}
	})
	t.Run("no connection is left inside a transaction", func(t *testing.T) {
		for range 5 {
			if _, err := liveRead(s, ctx, "SELECT 1"); err != nil {
				t.Fatal(err)
			}
		}
		var idleInTx int
		if err := admin.QueryRow("SELECT count(*) FROM pg_stat_activity WHERE application_name = 'kelvo-go' AND state LIKE 'idle in transaction%'").Scan(&idleInTx); err != nil || idleInTx != 0 {
			t.Fatalf("connections left in a transaction: %d %v", idleInTx, err)
		}
	})
}

// Scenarios where the background ROLLBACK can hurt: a cold pool, reads
// back-to-back on one connection, and bursts larger than the warm pool.
func TestReadScenariosLive(t *testing.T) {
	rttMS := 80
	if v, err := strconv.Atoi(os.Getenv("KELVO_RT_DELAY_MS")); err == nil {
		rttMS = v
	}
	for _, engine := range []string{"postgresql", "mysql", "mariadb", "sqlserver"} {
		creds := os.Getenv("KELVO_RT_" + strings.ToUpper(engine))
		if creds == "" {
			continue
		}
		t.Run(engine, func(t *testing.T) {
			_, rest, _ := strings.Cut(creds, "@")
			target, _, _ := strings.Cut(rest, "/")
			proxy := newDelayProxy(t, target, time.Duration(rttMS)*time.Millisecond/2)
			timed := func(s *Session) time.Duration {
				start := time.Now()
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				if _, err := s.Query(ctx, rtQuery(engine), nopSink{}); err != nil {
					t.Fatal(err)
				}
				return time.Since(start)
			}
			burst := func(s *Session, n int) time.Duration {
				var wg sync.WaitGroup
				start := time.Now()
				for range n {
					wg.Add(1)
					go func() { defer wg.Done(); timed(s) }()
				}
				wg.Wait()
				return time.Since(start)
			}
			p50 := func(s *Session, n int) time.Duration {
				var d []time.Duration
				for range n {
					d = append(d, timed(s))
				}
				slices.Sort(d)
				return d[n/2]
			}
			cold := liveSession(t, engine, proxy.addr(), creds)
			coldFirst := timed(cold)

			single := liveSession(t, engine, proxy.addr(), creds)
			single.Pool.SetMaxOpenConns(1)
			timed(single)
			singleP50 := p50(single, 8)

			one := liveSession(t, engine, proxy.addr(), creds) // warmed to one connection
			timed(one)
			waitRollbacks()
			burstOne := burst(one, PoolSize+1)

			four := liveSession(t, engine, proxy.addr(), creds) // warmed to PoolSize connections
			burst(four, PoolSize)
			waitRollbacks()
			burstFour := burst(four, PoolSize+1)
			waitRollbacks()
			t.Logf("SCENARIO engine=%s rtt=%dms cold-first-read=%v single-conn-back-to-back-p50=%v burst%d-on-1-warm-conn=%v burst%d-on-%d-warm-conns=%v",
				engine, rttMS, coldFirst, singleP50, PoolSize+1, burstOne, PoolSize+1, PoolSize, burstFour)
		})
	}
}

func TestPostgresDeadIdleConnectionsAreReplacedLive(t *testing.T) {
	creds := os.Getenv("KELVO_RT_POSTGRESQL")
	if creds == "" {
		t.Skip("KELVO_RT_POSTGRESQL not set")
	}
	_, rest, _ := strings.Cut(creds, "@")
	target, _, _ := strings.Cut(rest, "/")
	proxy := newDelayProxy(t, target, 0)
	s := liveSession(t, "postgresql", proxy.addr(), creds)
	admin, err := sql.Open("pgx", "postgres://"+creds+"?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	var wg sync.WaitGroup
	for range 2 { // warm PoolSize connections
		for range PoolSize {
			wg.Add(1)
			go func() { defer wg.Done(); _, _ = liveRead(s, context.Background(), "SELECT pg_sleep(0.2)::text AS s") }()
		}
		wg.Wait()
	}
	if open := s.Pool.Stats().OpenConnections; open != PoolSize {
		t.Fatalf("pool not warm: %d", open)
	}
	// Failover or PgBouncer restart: every idle backend dies between reads.
	if _, err := admin.Exec("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = 'kelvo-go'"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	for i := range PoolSize + 1 {
		if sink, err := liveRead(s, context.Background(), "SELECT 1"); err != nil || sink.row[0] != "1" {
			t.Fatalf("read %d after the outage failed: %v", i, err)
		}
	}
	// A query that kills its own backend after being sent is never replayed.
	if _, err := liveRead(s, context.Background(), "SELECT pg_terminate_backend(pg_backend_pid())"); err == nil {
		t.Fatal("query that killed its own backend reported success")
	}
	if sink, err := liveRead(s, context.Background(), "SELECT 1"); err != nil || sink.row[0] != "1" {
		t.Fatalf("pool unusable after a killed query: %v", err)
	}
}
