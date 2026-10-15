// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0

// Command loadgen drives a Kelvo gateway the way db-api does: one Go client,
// a signed delegation grant per request, and the gateway's single-call read
// (client.QueryStream). It runs mixed product traffic from virtual users and
// writes one JSON report. It uses only Kelvo's public packages.
//
// Workloads (each virtual user is one goroutine):
//
//	explorer   a page open: 42 requests at once on one connection (schemas, tables,
//	           and per table columns, keys, indexes, a 50-row preview and a count),
//	           then think time
//	dashboard  N dashboards of 10 panels, V viewers each; every viewer refreshes all
//	           10 panels every 30 s from a random phase, a few refreshes are explicit
//	console    one user-typed query at a time, unique SQL, sometimes a larger result
//	analysis   one durable operation after another, each a long read
package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	mrand "math/rand"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SYNEHQ/kelvo-go/client"
	"github.com/SYNEHQ/kelvo-go/delegation"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/SYNEHQ/kelvo-go/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/jackc/pgx/v5"
)

type nodeInfo struct {
	URL              string `json:"url"`
	Token            string `json:"token"`
	CAFile           string `json:"ca_file"`
	SigningSeed      string `json:"signing_seed"`
	Issuer           string `json:"issuer"`
	Audience         string `json:"audience"`
	ClusterTenant    string `json:"cluster_tenant"`
	ServicePrincipal string `json:"service_principal"`
	Customers        int    `json:"customers"`
}

type sink struct{ rows int64 }

func (s *sink) Schema(*arrow.Schema) error { return nil }
func (s *sink) Write(b arrow.RecordBatch) error {
	s.rows += b.NumRows()
	return nil
}

// sample is one finished request.
type sample struct {
	class   string
	at      float64 // seconds since the run began, at request start
	seconds float64
	code    string // empty on success
	retry   float64
}

type recorder struct {
	mu       sync.Mutex
	began    time.Time
	samples  []sample
	inflight atomic.Int64
	peak     atomic.Int64
	area     float64 // in-flight request-seconds
	last     time.Time
}

func (r *recorder) enter() {
	n := r.inflight.Add(1)
	for {
		p := r.peak.Load()
		if n <= p || r.peak.CompareAndSwap(p, n) {
			break
		}
	}
	r.mu.Lock()
	now := time.Now()
	r.area += float64(n-1) * now.Sub(r.last).Seconds()
	r.last = now
	r.mu.Unlock()
}

func (r *recorder) leave(s sample) {
	n := r.inflight.Add(-1)
	r.mu.Lock()
	now := time.Now()
	r.area += float64(n+1) * now.Sub(r.last).Seconds()
	r.last = now
	r.samples = append(r.samples, s)
	r.mu.Unlock()
}

type driver struct {
	node    nodeInfo
	key     ed25519.PrivateKey
	client  *client.Client
	rec     *recorder
	rng     func() *mrand.Rand
	end     time.Time
	timeout time.Duration
}

func nonce() string {
	var raw [24]byte
	_, _ = rand.Read(raw[:])
	return hex.EncodeToString(raw[:])
}

func team(c int) string { return fmt.Sprintf("team-%02d", c/4) } // four connections per team

// grant signs the delegation for one request, as the application would.
func (d *driver) grant(c int, user string, r query.Request) (string, error) {
	digest, err := delegation.QueryDigest(r)
	if err != nil {
		return "", err
	}
	now := time.Now().Unix()
	return delegation.Sign(delegation.Claims{Version: 1, Issuer: d.node.Issuer, Audience: d.node.Audience, ClusterTenant: d.node.ClusterTenant, ServicePrincipal: d.node.ServicePrincipal,
		AppTeam: team(c), Subject: delegation.Subject{Kind: "user", ID: user}, ID: nonce(), IssuedAt: now - 1, ExpiresAt: now + 120, QuerySHA256: digest,
		Sources: []delegation.Source{{Alias: "source_1", ConnectionID: fmt.Sprintf("cust-%02d", c), Database: fmt.Sprintf("cust%02d", c), Schema: "public"}}}, d.key)
}

var (
	messagesMu sync.Mutex
	messages   = map[string]string{} // first message seen per error code
)

func errorCode(err error) (string, float64) {
	var e *client.Error
	if errors.As(err, &e) {
		messagesMu.Lock()
		if _, ok := messages[e.Code]; !ok {
			messages[e.Code] = e.Message
		}
		messagesMu.Unlock()
		return e.Code, e.RetryAfter.Seconds()
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "DEADLINE_EXCEEDED", 0
	}
	return "TRANSPORT", 0
}

// read runs one single-call read and records it under class.
func (d *driver) read(class string, c int, user string, r query.Request) (rows int64, ok bool) {
	r.Mode, r.ConnectionID = "native", "source_1"
	ctx, cancel := context.WithTimeout(context.Background(), d.timeout)
	defer cancel()
	began := time.Now()
	s := sample{class: class, at: began.Sub(d.rec.began).Seconds()}
	d.rec.enter()
	g, err := d.grant(c, user, r)
	if err == nil {
		out := &sink{}
		_, err = d.client.QueryStream(ctx, r, client.Authority{Delegation: g}, out)
		rows = out.rows
	}
	s.seconds = time.Since(began).Seconds()
	if err != nil {
		s.code, s.retry = errorCode(err)
	} else if rows == 0 {
		s.code = "EMPTY_RESULT"
	}
	d.rec.leave(s)
	return rows, s.code == ""
}

func sleepUntil(end time.Time, d time.Duration) bool {
	if time.Now().Add(d).After(end) {
		return false
	}
	time.Sleep(d)
	return true
}

func between(r *mrand.Rand, lo, hi time.Duration) time.Duration {
	return lo + time.Duration(r.Int63n(int64(hi-lo)+1))
}

// explorerPage is what db-api sends when an explorer page opens, all at once.
func (d *driver) explorerPage(c int, user string) {
	type job struct {
		class string
		req   query.Request
	}
	inspect := func(object, table string, limit int) job {
		return job{"explorer_inspect", query.Request{Inspect: &operations.MetadataSpec{Object: object, Target: operations.ObjectRef{Schema: "public", Name: table}, Limit: limit}}}
	}
	jobs := []job{inspect("schemas", "", 100), inspect("tables", "", 500)}
	for i := 0; i < 8; i++ {
		t := fmt.Sprintf("explorer_t%d", i)
		jobs = append(jobs, inspect("columns", t, 500), inspect("primary_keys", t, 100), inspect("indexes", t, 100),
			job{"explorer_read", query.Request{SQL: "SELECT * FROM public." + t + " ORDER BY id LIMIT 50"}},
			job{"explorer_read", query.Request{SQL: "SELECT count(*)::bigint AS n FROM public." + t}})
	}
	var wg sync.WaitGroup
	began := time.Now()
	for _, j := range jobs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d.read(j.class, c, user, j.req)
		}()
	}
	wg.Wait()
	d.rec.mu.Lock()
	d.rec.samples = append(d.rec.samples, sample{class: "explorer_page", at: began.Sub(d.rec.began).Seconds(), seconds: time.Since(began).Seconds()})
	d.rec.mu.Unlock()
}

func panelSQL(p int) string {
	return fmt.Sprintf("SELECT region, count(*)::bigint AS n, sum(amount)::numeric(18,2) AS total FROM public.dash_p%d GROUP BY region ORDER BY region", p)
}

// refresh loads all ten panels of one dashboard in parallel. explicit marks the
// viewer pressing refresh, which asks the node to skip its result cache.
func (d *driver) refresh(c int, user string, explicit bool) {
	var wg sync.WaitGroup
	began := time.Now()
	failed := atomic.Bool{}
	for p := 0; p < 10; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := query.Request{SQL: panelSQL(p), BypassCache: explicit}
			if _, ok := d.read("dashboard_panel", c, user, r); !ok {
				failed.Store(true)
			}
		}()
	}
	wg.Wait()
	class := "dashboard_refresh"
	if explicit {
		class = "dashboard_refresh_explicit"
	}
	s := sample{class: class, at: began.Sub(d.rec.began).Seconds(), seconds: time.Since(began).Seconds()}
	if failed.Load() {
		s.code = "PANEL_FAILED"
	}
	d.rec.mu.Lock()
	d.rec.samples = append(d.rec.samples, s)
	d.rec.mu.Unlock()
}

func (d *driver) analysisStep(c int, step int) bool {
	r := operations.Request{Version: operations.Version, Kind: operations.QueryRead, Connection: operations.ConnectionRef{ID: fmt.Sprintf("cust-%02d", c), Database: fmt.Sprintf("cust%02d", c), Schema: "public"},
		Spec: operations.Spec{Query: &operations.QuerySpec{SQL: fmt.Sprintf("SELECT g AS id, md5(g::text) AS h, %d AS step FROM (SELECT pg_sleep(12)) s, generate_series(1, 60000) g", step)}}}
	digest, err := operations.Digest(r)
	began := time.Now()
	s := sample{class: "analysis_step", at: began.Sub(d.rec.began).Seconds()}
	d.rec.enter()
	if err == nil {
		now := time.Now().Unix()
		var grant string
		grant, err = operations.SignGrant(operations.GrantClaims{Version: operations.GrantVersion, Issuer: d.node.Issuer, Audience: d.node.Audience, ClusterTenant: d.node.ClusterTenant, ServicePrincipal: d.node.ServicePrincipal,
			AppTeam: team(c), Subject: operations.Subject{Kind: "user", ID: "analyst"}, ID: nonce(), IssuedAt: now - 1, ExpiresAt: now + 240, ConnectionID: r.Connection.ID, Operation: r.Kind, RequestSHA256: digest,
			Authorization: operations.Authorization{Kind: "read"}}, d.key)
		if err == nil {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			var response operations.Response
			response, err = d.client.ExecuteOperation(ctx, r, client.Authority{OperationGrant: grant})
			cancel()
			if err == nil && (response.Receipt == nil || response.Receipt.Outcome != "completed") {
				err = errors.New("operation did not complete")
				s.code = "OPERATION_NOT_COMPLETED"
				if response.Receipt != nil {
					s.code = "OPERATION_" + strings.ToUpper(string(response.Receipt.Outcome)) + "_" + response.Receipt.ErrorCode
				}
			}
		}
	}
	s.seconds = time.Since(began).Seconds()
	if err != nil && s.code == "" {
		s.code, s.retry = errorCode(err)
	}
	d.rec.leave(s)
	return s.code == ""
}

type stat struct {
	N      int            `json:"n"`
	Errors int            `json:"errors"`
	P50    float64        `json:"p50_ms"`
	P95    float64        `json:"p95_ms"`
	P99    float64        `json:"p99_ms"`
	Max    float64        `json:"max_ms"`
	Mean   float64        `json:"mean_ms"`
	Codes  map[string]int `json:"error_codes,omitempty"`
}

func pct(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	i := int(math.Ceil(p/100*float64(len(sorted)))) - 1
	return sorted[max(i, 0)] * 1000
}

func summarize(samples []sample) stat {
	s := stat{N: len(samples), Codes: map[string]int{}}
	var ok []float64
	var sum float64
	for _, x := range samples {
		if x.code != "" {
			s.Errors++
			s.Codes[x.code]++
			continue
		}
		ok = append(ok, x.seconds)
		sum += x.seconds
	}
	sort.Float64s(ok)
	s.P50, s.P95, s.P99 = pct(ok, 50), pct(ok, 95), pct(ok, 99)
	if len(ok) > 0 {
		s.Max, s.Mean = ok[len(ok)-1]*1000, sum/float64(len(ok))*1000
	}
	if len(s.Codes) == 0 {
		s.Codes = nil
	}
	return s
}

// sourceCalls reads, from the source's own statistics, how often each dashboard
// panel query ran, per customer database. This is the only honest count of
// "source queries": it does not trust the node's counters.
func sourceCalls(ctx context.Context, dsn string) (map[string]int64, error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, err
	}
	defer conn.Close(ctx)
	rows, err := conn.Query(ctx, `SELECT d.datname, s.query, sum(s.calls)::bigint FROM pg_stat_statements s JOIN pg_database d ON d.oid = s.dbid
		WHERE s.query LIKE '%FROM public.dash_p%' AND s.query LIKE '%GROUP BY region%' GROUP BY 1, 2`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int64{}
	for rows.Next() {
		var db, q string
		var calls int64
		if err := rows.Scan(&db, &q, &calls); err != nil {
			return nil, err
		}
		i := strings.Index(q, "dash_p")
		out[db+"/"+q[i:i+7]] += calls
	}
	return out, rows.Err()
}

func resetStatements(ctx context.Context, dsn string) error {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	_, err = conn.Exec(ctx, "SELECT pg_stat_statements_reset()")
	return err
}

func main() {
	nodeFile := flag.String("node", "", "node.json written by the node")
	out := flag.String("out", "loadgen-report.json", "report path")
	duration := flag.Duration("duration", 5*time.Minute, "load duration")
	explorers := flag.Int("explorers", 20, "explorer virtual users")
	dashboards := flag.Int("dashboards", 2, "dashboards")
	viewers := flag.Int("viewers", 10, "viewers per dashboard")
	consoles := flag.Int("consoles", 9, "console virtual users")
	analysis := flag.Int("analysis", 1, "background analysis jobs")
	thinkMin := flag.Duration("explorer-think-min", 10*time.Second, "explorer think time, minimum")
	thinkMax := flag.Duration("explorer-think-max", 20*time.Second, "explorer think time, maximum")
	refreshEvery := flag.Duration("dashboard-refresh", 30*time.Second, "dashboard refresh interval")
	explicitPct := flag.Int("explicit-refresh-pct", 5, "percent of dashboard refreshes that bypass the cache")
	maxConcurrent := flag.Int("max-concurrent", 256, "client MaxConcurrent, as db-api would configure")
	timeout := flag.Duration("request-timeout", 30*time.Second, "per-request deadline")
	pgAdmin := flag.String("pg-admin-dsn", "", "direct admin DSN of the source, for pg_stat_statements")
	label := flag.String("label", "", "free text copied to the report")
	flag.Parse()

	raw, err := os.ReadFile(*nodeFile)
	if err != nil {
		log.Fatal(err)
	}
	var node nodeInfo
	if err := json.Unmarshal(raw, &node); err != nil {
		log.Fatal(err)
	}
	seed, err := base64.StdEncoding.DecodeString(node.SigningSeed)
	if err != nil {
		log.Fatal(err)
	}
	ca, err := os.ReadFile(node.CAFile)
	if err != nil {
		log.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(ca)
	cl, err := client.New(client.Config{URL: node.URL, BearerToken: node.Token, TLSConfig: &tls.Config{RootCAs: pool}, Timeout: *timeout, MaxConcurrent: *maxConcurrent, MaxControlConcurrent: 8})
	if err != nil {
		log.Fatal(err)
	}
	defer cl.Close()
	seedBase := time.Now().UnixNano()
	var seedMu sync.Mutex
	d := &driver{node: node, key: ed25519.NewKeyFromSeed(seed), client: cl, timeout: *timeout,
		rng: func() *mrand.Rand {
			seedMu.Lock()
			defer seedMu.Unlock()
			seedBase++
			return mrand.New(mrand.NewSource(seedBase))
		}}
	ctx := context.Background()
	report := map[string]any{"label": *label, "started": time.Now().UTC().Format(time.RFC3339), "duration_s": duration.Seconds(),
		"virtual_users": map[string]int{"explorer": *explorers, "dashboard_viewers": *dashboards * *viewers, "console": *consoles, "analysis": *analysis}}

	// Probe: does an explicit refresh really reach the source? One panel, run
	// twice inside the cache window, then once with the bypass flag.
	if *pgAdmin != "" {
		probe := map[string]any{}
		if err := resetStatements(ctx, *pgAdmin); err != nil {
			probe["error"] = "reset: " + err.Error()
		} else {
			d.rec = &recorder{began: time.Now(), last: time.Now()}
			sql := panelSQL(9)
			d.read("probe", node.Customers-1, "probe", query.Request{SQL: sql})
			d.read("probe", node.Customers-1, "probe", query.Request{SQL: sql})
			afterCached, _ := sourceCalls(ctx, *pgAdmin)
			d.read("probe", node.Customers-1, "probe", query.Request{SQL: sql, BypassCache: true})
			afterBypass, _ := sourceCalls(ctx, *pgAdmin)
			d.read("probe", node.Customers-1, "probe", query.Request{SQL: sql, Refresh: true})
			afterRefresh, _ := sourceCalls(ctx, *pgAdmin)
			sum := func(m map[string]int64) (n int64) {
				for _, v := range m {
					n += v
				}
				return
			}
			probe["source_calls_after_two_identical_reads"] = sum(afterCached)
			probe["source_calls_after_bypass_cache_true"] = sum(afterBypass)
			probe["source_calls_after_refresh_true"] = sum(afterRefresh)
			probe["bypass_cache_reaches_source"] = sum(afterBypass) > sum(afterCached)
			probe["refresh_reaches_source"] = sum(afterRefresh) > sum(afterBypass)
			probe["samples"] = summarize(d.rec.samples)
		}
		report["probe_explicit_refresh"] = probe
		_ = resetStatements(ctx, *pgAdmin)
	}

	d.rec = &recorder{began: time.Now(), last: time.Now()}
	d.end = d.rec.began.Add(*duration)
	var wg sync.WaitGroup
	var viewerRefreshes atomic.Int64
	start := func(f func()) { wg.Add(1); go func() { defer wg.Done(); f() }() }
	for i := 0; i < *explorers; i++ {
		start(func() {
			r := d.rng()
			// Spread the first page opens over one think time.
			if !sleepUntil(d.end, between(r, 0, *thinkMax)) {
				return
			}
			c := r.Intn(node.Customers)
			for time.Now().Before(d.end) {
				d.explorerPage(c, fmt.Sprintf("explorer-%02d", i))
				if r.Intn(5) == 0 { // a fifth of page opens move to another connection
					c = r.Intn(node.Customers)
				}
				if !sleepUntil(d.end, between(r, *thinkMin, *thinkMax)) {
					return
				}
			}
		})
	}
	for dash := 0; dash < *dashboards; dash++ {
		c := dash // each dashboard reads one connection
		for v := 0; v < *viewers; v++ {
			start(func() {
				r := d.rng()
				user := fmt.Sprintf("viewer-%d-%02d", dash, v)
				if !sleepUntil(d.end, between(r, 0, *refreshEvery)) {
					return
				}
				for time.Now().Before(d.end) {
					viewerRefreshes.Add(1)
					d.refresh(c, user, r.Intn(100) < *explicitPct)
					if !sleepUntil(d.end, *refreshEvery) {
						return
					}
				}
			})
		}
	}
	for i := 0; i < *consoles; i++ {
		start(func() {
			r := d.rng()
			c := r.Intn(node.Customers)
			user := fmt.Sprintf("console-%02d", i)
			for sleepUntil(d.end, between(r, 5*time.Second, 15*time.Second)) {
				var sql string
				switch r.Intn(3) {
				case 0:
					sql = fmt.Sprintf("SELECT * FROM public.explorer_t%d WHERE id > %d ORDER BY id LIMIT 200", r.Intn(8), r.Intn(300))
				case 1:
					sql = fmt.Sprintf("SELECT region, avg(amount) AS a FROM public.dash_p%d WHERE id > %d GROUP BY region", r.Intn(10), r.Intn(1000))
				default: // a larger paged result
					sql = fmt.Sprintf("SELECT * FROM public.dash_p%d WHERE id > %d LIMIT 5000", r.Intn(10), r.Intn(1000))
				}
				d.read("console", c, user, query.Request{SQL: sql})
			}
		})
	}
	for i := 0; i < *analysis; i++ {
		start(func() {
			r := d.rng()
			c := r.Intn(node.Customers)
			for step := 0; time.Now().Before(d.end); step++ {
				if !d.analysisStep(c, step) {
					sleepUntil(d.end, 5*time.Second) // a failing job backs off, as an application would
				}
			}
		})
	}
	wg.Wait()
	elapsed := time.Since(d.rec.began)

	byClass := map[string][]sample{}
	for _, s := range d.rec.samples {
		byClass[s.class] = append(byClass[s.class], s)
	}
	classes := map[string]stat{}
	var all []sample
	for class, samples := range byClass {
		classes[class] = summarize(samples)
		if class == "explorer_read" || class == "explorer_inspect" || class == "dashboard_panel" || class == "console" || class == "analysis_step" {
			all = append(all, samples...)
		}
	}
	codes := map[string]int{}
	capacity := 0
	for _, s := range all {
		if s.code != "" {
			codes[s.code]++
			if s.code == "RESOURCE_EXHAUSTED" || s.code == "UNAVAILABLE" {
				capacity++
			}
		}
	}
	// Timeline in 15 s windows: where the tail comes from.
	type window struct {
		T      float64 `json:"t_s"`
		N      int     `json:"requests"`
		Errors int     `json:"errors"`
		P95    float64 `json:"p95_ms"`
	}
	buckets := map[int][]sample{}
	for _, s := range all {
		buckets[int(s.at/15)] = append(buckets[int(s.at/15)], s)
	}
	var timeline []window
	for i := 0; i <= int(elapsed.Seconds()/15); i++ {
		st := summarize(buckets[i])
		timeline = append(timeline, window{T: float64(i * 15), N: st.N, Errors: st.Errors, P95: st.P95})
	}
	d.rec.mu.Lock()
	meanInflight := d.rec.area / elapsed.Seconds()
	d.rec.mu.Unlock()
	report["elapsed_s"] = elapsed.Seconds()
	report["classes"] = classes
	report["all_requests"] = summarize(all)
	report["errors_by_code"] = codes
	messagesMu.Lock()
	report["first_error_message_by_code"] = messages
	messagesMu.Unlock()
	report["capacity_errors"] = capacity
	report["peak_inflight_requests"] = d.rec.peak.Load()
	report["mean_inflight_requests"] = meanInflight
	report["timeline_15s"] = timeline
	report["dashboard_viewer_refreshes"] = viewerRefreshes.Load()
	retries := 0
	for _, s := range all {
		if s.retry > 0 {
			retries++
		}
	}
	report["responses_with_retry_after"] = retries

	if *pgAdmin != "" {
		calls, err := sourceCalls(ctx, *pgAdmin)
		if err != nil {
			report["source_calls_error"] = err.Error()
		} else {
			var total int64
			perPanel := map[string]int64{}
			for k, v := range calls {
				total += v
				perPanel[k] = v
			}
			panels := int64(len(byClass["dashboard_panel"]))
			report["dashboard"] = map[string]any{
				"panel_requests": panels, "source_queries": total, "viewer_refreshes": viewerRefreshes.Load(),
				"cache_hit_rate":                    1 - float64(total)/math.Max(float64(panels), 1),
				"source_queries_per_viewer_refresh": float64(total) / math.Max(float64(viewerRefreshes.Load()), 1), // 10 without a cache, 1 with perfect sharing across 10 viewers
				"viewers_per_dashboard":             *viewers, "per_panel": perPanel,
			}
		}
	}
	enc, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		log.Fatal(err)
	}
	if err := os.WriteFile(*out, enc, 0644); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("requests=%d errors=%d capacity_errors=%d peak_inflight=%d\n", len(all), summarize(all).Errors, capacity, d.rec.peak.Load())
}
