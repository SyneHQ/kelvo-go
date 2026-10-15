package runtime

import (
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/adapters/go/internal/frame"
	"github.com/SYNEHQ/kelvo-go/operations"
)

// TestServeLoopProfilePlainRead drives the real serve loop in this process with
// plain 50-row reads against a real PostgreSQL, the way the node does (the first
// request carries the DSN, later ones are secretless), so `go test -cpuprofile`
// and `-memprofile` profile the adapter alone. Skipped without the profile
// environment; scripts/pooled_read_load.py --profile-plain sets it.
//
//	KELVO_TEST_PROFILE_DSN      postgres://...?sslmode=verify-full&connect_timeout=5
//	KELVO_TEST_PROFILE_CA_FILE  trust root for the source
//	KELVO_TEST_PROFILE_STREAMS  reads in flight (1, or 50)
//	KELVO_TEST_PROFILE_SECONDS  how long
//	KELVO_TEST_PROFILE_OUT      JSON summary path
func TestServeLoopProfilePlainRead(t *testing.T) {
	dsn, caFile, out := os.Getenv("KELVO_TEST_PROFILE_DSN"), os.Getenv("KELVO_TEST_PROFILE_CA_FILE"), os.Getenv("KELVO_TEST_PROFILE_OUT")
	if dsn == "" || caFile == "" || out == "" {
		t.Skip("profile environment not set")
	}
	streams, _ := strconv.Atoi(os.Getenv("KELVO_TEST_PROFILE_STREAMS"))
	seconds, _ := strconv.Atoi(os.Getenv("KELVO_TEST_PROFILE_SECONDS"))
	streams, seconds = max(streams, 1), max(seconds, 5)
	ca, err := os.ReadFile(caFile)
	if err != nil {
		t.Fatal(err)
	}
	database := "postgres"
	if u, err := url.Parse(dsn); err == nil && strings.Trim(u.Path, "/") != "" {
		database = strings.Trim(u.Path, "/")
	}
	const sql = "SELECT * FROM public.explorer_t0 ORDER BY id LIMIT 50"
	h := startServe(t, Runner{}, true)
	next := uint32(0)
	started := map[uint32]time.Time{}
	send := func(first bool) {
		next++
		req := operations.Request{Version: operations.Version, Kind: operations.QueryRead, Connection: operations.ConnectionRef{ID: "profile", Database: database, Schema: "public"},
			Spec: operations.Spec{Query: &operations.QuerySpec{SQL: sql}}}
		digest, err := operations.Digest(req)
		if err != nil {
			t.Fatal(err)
		}
		now := time.Now().Unix()
		source := adapter.ConnectionSpec{Engine: "postgres", TenantID: "tenant-a", ConnectionID: "profile", Revision: "revision-1", Database: database, Schema: "public"}
		if first {
			source.DSN, source.Options = dsn, map[string]string{"tls_ca_pem": string(ca)}
		}
		started[next] = time.Now()
		h.request(next, adapter.ProcessRequest{Version: adapter.ProcessVersion, OperationID: fmt.Sprintf("op-%d", next), RequestSHA256: digest, Request: req,
			Limits: adapter.ProcessLimits{MaxRows: 1_000_000, MaxBytes: 64 << 20, BatchRows: 1024, TimeoutMS: 30000}, Source: source, CredentialsValidUntil: now + 5, ExpiresAt: now + 60})
	}
	// One warm-up read opens the pool's first connection.
	send(true)
	for {
		f := h.next()
		if f.Type == frame.Receipt || f.Type == frame.Error {
			if f.Type == frame.Error {
				t.Fatalf("warm-up failed: %q", f.Payload)
			}
			break
		}
	}
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	var latencies []float64
	var bytes, resultFrames int64
	deadline := time.Now().Add(time.Duration(seconds) * time.Second)
	for i := 0; i < streams; i++ {
		send(false)
	}
	inFlight := streams
	began := time.Now()
	for inFlight > 0 {
		f := h.next()
		switch f.Type {
		case frame.Result:
			bytes += int64(len(f.Payload))
			resultFrames++
		case frame.Receipt:
			latencies = append(latencies, time.Since(started[f.Stream]).Seconds())
			delete(started, f.Stream)
			inFlight--
			if time.Now().Before(deadline) {
				send(false)
				inFlight++
			}
		case frame.Error:
			t.Fatalf("read failed: %q", f.Payload)
		}
	}
	elapsed := time.Since(began).Seconds()
	runtime.ReadMemStats(&after)
	sort.Float64s(latencies)
	at := func(p float64) float64 { return latencies[max(int(math.Ceil(p/100*float64(len(latencies))))-1, 0)] * 1000 }
	n := float64(len(latencies))
	summary := map[string]any{"streams": streams, "reads": len(latencies), "elapsed_s": elapsed, "reads_per_second": n / elapsed, "p50_ms": at(50), "p95_ms": at(95), "p99_ms": at(99),
		"result_bytes_per_read": float64(bytes) / n, "result_frames_per_read": float64(resultFrames) / n, "allocated_bytes_per_read": float64(after.TotalAlloc-before.TotalAlloc) / n,
		"mallocs_per_read": float64(after.Mallocs-before.Mallocs) / n, "heap_in_use_bytes": after.HeapInuse, "heap_sys_bytes": after.HeapSys, "gc_cycles": after.NumGC - before.NumGC,
		"gc_pause_total_ms": float64(after.PauseTotalNs-before.PauseTotalNs) / 1e6, "gomaxprocs": runtime.GOMAXPROCS(0)}
	raw, _ := json.MarshalIndent(summary, "", "  ")
	if err := os.WriteFile(out, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	_ = frame.WriteClose(h.in)
	h.wantDone(false)
}
