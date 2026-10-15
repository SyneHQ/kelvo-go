//go:build linux

package runtime

import (
	"encoding/json"
	"io"
	"net/url"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/adapters/go/internal/frame"
	"github.com/SYNEHQ/kelvo-go/operations"
)

// Measures one adapter binary as a warm read worker: file size, launch to
// Hello, and resident memory once idle after a PostgreSQL read. The binary is
// the one under test, so the same test reports the full and the slim build.
// It runs the binary directly, without the sandbox: both builds are measured the
// same way, and only their difference matters.
func TestServeBinaryFootprint(t *testing.T) {
	binary, dsn := os.Getenv("KELVO_TEST_FOOTPRINT_ADAPTER"), os.Getenv("KELVO_TEST_WARM_POSTGRES_DSN")
	if binary == "" || dsn == "" {
		t.Skip("set KELVO_TEST_FOOTPRINT_ADAPTER and KELVO_TEST_WARM_POSTGRES_DSN")
	}
	database := "postgres"
	if u, err := url.Parse(dsn); err == nil && strings.Trim(u.Path, "/") != "" {
		database = strings.Trim(u.Path, "/")
	}
	info, err := os.Stat(binary)
	if err != nil {
		t.Fatal(err)
	}
	const runs = 7
	var hello []float64
	var rss, anon []float64
	for range runs {
		cmd := exec.Command(binary, "serve")
		in, _ := cmd.StdinPipe()
		out, _ := cmd.StdoutPipe()
		started := time.Now()
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		_ = frame.WriteHello(in)
		f, err := frame.Read(out)
		if err != nil || frame.CheckHello(f) != nil {
			t.Fatalf("no hello: %v", err)
		}
		hello = append(hello, float64(time.Since(started).Microseconds())/1000)

		req := operations.Request{Version: operations.Version, Kind: operations.QueryRead,
			Connection: operations.ConnectionRef{ID: "footprint", Database: database, Schema: "public"},
			Spec:       operations.Spec{Query: &operations.QuerySpec{SQL: "SELECT g AS n, md5(g::text) AS h FROM generate_series(1, 200) g"}}}
		digest, _ := operations.Digest(req)
		now := time.Now().Unix()
		raw, err := adapter.EncodeProcessRequest(adapter.ProcessRequest{Version: adapter.ProcessVersion, OperationID: "footprint", RequestSHA256: digest, Request: req,
			Limits:                adapter.ProcessLimits{MaxRows: 1000, MaxBytes: 1 << 20, BatchRows: 100, TimeoutMS: 30000},
			Source:                adapter.ConnectionSpec{Engine: "postgres", DSN: dsn, TenantID: "tenant-a", ConnectionID: "footprint", Revision: "r1", Database: database, Schema: "public"},
			CredentialsValidUntil: now + 5, ExpiresAt: now + 60})
		if err != nil {
			t.Fatal(err)
		}
		if err := frame.WriteRequest(in, 1, raw); err != nil {
			t.Fatal(err)
		}
		for {
			f, err := frame.Read(out)
			if err != nil {
				t.Fatalf("read failed before the receipt: %v", err)
			}
			if f.Type == frame.Error {
				t.Fatalf("read failed: %q", f.Payload)
			}
			if f.Type == frame.Receipt {
				break
			}
		}
		time.Sleep(500 * time.Millisecond) // idle: the read is done, the connection is pooled
		status, err := os.ReadFile("/proc/" + strconv.Itoa(cmd.Process.Pid) + "/status")
		if err != nil {
			t.Fatal(err)
		}
		fields := map[string]float64{}
		for _, line := range strings.Split(string(status), "\n") {
			if name, value, ok := strings.Cut(line, ":"); ok && (name == "VmRSS" || name == "RssAnon") {
				kb, _ := strconv.ParseFloat(strings.Fields(value)[0], 64)
				fields[name] = kb / 1024
			}
		}
		rss, anon = append(rss, fields["VmRSS"]), append(anon, fields["RssAnon"])
		_ = frame.WriteClose(in)
		_, _ = io.Copy(io.Discard, out)
		_ = cmd.Wait()
	}
	median := func(v []float64) float64 { sort.Float64s(v); return v[len(v)/2] }
	report, _ := json.Marshal(map[string]any{"binary": binary[strings.LastIndex(binary, "/")+1:], "size_mb": float64(info.Size()) / (1 << 20), "runs": runs,
		"hello_ms_median": median(hello), "idle_rss_mb_median": median(rss), "idle_rss_anon_mb_median": median(anon)})
	t.Logf("FOOTPRINT %s", report)
}
