// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/worker"
)

func readWorkersNode(rw *ReadWorkersConfig, warm worker.WarmPoolConfig) NodeConfig {
	return NodeConfig{ReadWorkers: rw, Resources: &ResourceConfig{MaxConcurrent: 8}, Operations: &OperationNodeConfig{WarmPool: &warm}}
}

// read_workers is the one source of read-worker capacity: it lays over the
// non-capacity warm pool settings, and the adapter pool size comes from it.
func TestReadWorkersSizeTheWarmPoolAndAdapter(t *testing.T) {
	rw := ReadWorkersConfig{MaxWorkers: 2, PoolSize: 8}.withDefaults()
	node := readWorkersNode(&rw, worker.WarmPoolConfig{Enabled: true, QueueDepth: 64, WorkerMemoryMB: 64, MaxPerTenant: 7})
	if err := validateNodeReadWorkers(node); err != nil {
		t.Fatal(err)
	}
	got := node.effectiveWarmPool()
	if got.MaxWorkers != 2 || got.MaxInFlight != 16 || got.IdleTTL != 30*time.Minute || got.QueueTimeout != 2*time.Second || got.StreamBufferMB != 8 ||
		got.MaxPerKey != 2 || got.MaxPerTenant != 7 || got.QueueDepth != 64 || !got.Enabled {
		t.Fatalf("effective warm pool: %+v", got)
	}
	if node.readPoolSize() != 8 {
		t.Fatalf("pool size %d", node.readPoolSize())
	}
	if err := got.Validate(); err != nil {
		t.Fatal(err)
	}
	if node.Operations.WarmPool.MaxWorkers != 0 {
		t.Fatal("the configured warm pool was modified")
	}
}

// A node without read_workers keeps the legacy capacity fields and the
// adapter's own pool default.
func TestWarmPoolWithoutReadWorkersIsUnchanged(t *testing.T) {
	warm := worker.WarmPoolConfig{Enabled: true, MaxWorkers: 3, MaxInFlight: 9, IdleTTL: time.Minute, QueueTimeout: time.Second, MaxPerKey: 1, QueueDepth: 64, WorkerMemoryMB: 64}
	node := readWorkersNode(nil, warm)
	if got := node.effectiveWarmPool(); *got != warm || node.readPoolSize() != 0 || validateNodeReadWorkers(node) != nil {
		t.Fatalf("legacy configuration changed: %+v", got)
	}
}

func TestReadWorkersValidation(t *testing.T) {
	base := worker.WarmPoolConfig{Enabled: true, QueueDepth: 64, WorkerMemoryMB: 64}
	for name, tc := range map[string]struct {
		rw   ReadWorkersConfig
		warm worker.WarmPoolConfig
		err  error
	}{
		"valid":                            {ReadWorkersConfig{MaxWorkers: 4, PoolSize: 32}, base, nil},
		"connection budget":                {ReadWorkersConfig{MaxWorkers: 9, PoolSize: 32}, base, errReadWorkersConnections},
		"connection budget boundary":       {ReadWorkersConfig{MaxWorkers: 8, PoolSize: 32}, base, nil},
		"queue wait at the launch timeout": {ReadWorkersConfig{MaxWorkers: 1, QueueWait: 30 * time.Second}, base, errReadWorkersConfig},
		"queue wait just below it":         {ReadWorkersConfig{MaxWorkers: 1, QueueWait: 29 * time.Second}, base, nil},
		"no capacity":                      {ReadWorkersConfig{}, base, errReadWorkersConfig},
		"pool size":                        {ReadWorkersConfig{MaxWorkers: 1, PoolSize: 33}, base, errReadWorkersConfig},
		"in flight":                        {ReadWorkersConfig{MaxWorkers: 1, MaxInFlight: 65}, base, errReadWorkersConfig},
		"above admission":                  {ReadWorkersConfig{MaxWorkers: 9}, base, errReadWorkersConfig},
		"warm pool disabled":               {ReadWorkersConfig{MaxWorkers: 1}, worker.WarmPoolConfig{}, errReadWorkersWarmPool},
		"max_workers in both":              {ReadWorkersConfig{MaxWorkers: 1}, func() worker.WarmPoolConfig { w := base; w.MaxWorkers = 1; return w }(), errReadWorkersWarmPool},
		"idle_ttl in both":                 {ReadWorkersConfig{MaxWorkers: 1}, func() worker.WarmPoolConfig { w := base; w.IdleTTL = time.Minute; return w }(), errReadWorkersWarmPool},
		"max_in_flight in both":            {ReadWorkersConfig{MaxWorkers: 1}, func() worker.WarmPoolConfig { w := base; w.MaxInFlight = 4; return w }(), errReadWorkersWarmPool},
		"max_per_key in both":              {ReadWorkersConfig{MaxWorkers: 2}, func() worker.WarmPoolConfig { w := base; w.MaxPerKey = 1; return w }(), errReadWorkersWarmPool},
		"max_per_key above workers":        {ReadWorkersConfig{MaxWorkers: 2, MaxPerKey: 3}, base, errReadWorkersConfig},
		"stream_buffer_mb in both":         {ReadWorkersConfig{MaxWorkers: 1}, func() worker.WarmPoolConfig { w := base; w.StreamBufferMB = 4; return w }(), errReadWorkersWarmPool},
		"queue wait and timeout both":      {ReadWorkersConfig{MaxWorkers: 1}, func() worker.WarmPoolConfig { w := base; w.QueueTimeout = time.Second; return w }(), errReadWorkersWarmPool},
	} {
		rw := tc.rw.withDefaults()
		err := validateNodeReadWorkers(readWorkersNode(&rw, tc.warm))
		if err != tc.err {
			t.Errorf("%s: got %v, want %v", name, err, tc.err)
		}
		if err != nil && (strings.ContainsAny(err.Error(), "0123456789")) {
			t.Errorf("%s: error echoes a value: %v", name, err)
		}
	}
}

// pool_size is sent to the adapter only when the operator sets it: a node whose
// adapter or sandbox launcher predates --pool-size must keep its warm reads
// when read_workers sets only capacity.
func TestPoolSizeFlagIsSentOnlyWhenSet(t *testing.T) {
	base := worker.WarmPoolConfig{Enabled: true, QueueDepth: 64, WorkerMemoryMB: 64}
	unset := ReadWorkersConfig{MaxWorkers: 2}.withDefaults()
	if n := readWorkersNode(&unset, base).readPoolSize(); n != 0 {
		t.Fatalf("pool_size unset requests %d", n)
	}
	if err := validateNodeReadWorkers(readWorkersNode(&unset, base)); err != nil {
		t.Fatal(err)
	}
	set := ReadWorkersConfig{MaxWorkers: 2, PoolSize: 6}.withDefaults()
	if n := readWorkersNode(&set, base).readPoolSize(); n != 6 {
		t.Fatalf("pool_size 6 requests %d", n)
	}
	if n := readWorkersNode(nil, base).readPoolSize(); n != 0 {
		t.Fatalf("no read_workers requests %d", n)
	}
	// The connection budget still counts the adapter's default of 4.
	big := ReadWorkersConfig{MaxWorkers: 65}.withDefaults()
	if err := validateNodeReadWorkers(readWorkersNode(&big, base)); err == nil {
		t.Fatal("65 workers accepted")
	}
	negative := ReadWorkersConfig{MaxWorkers: 1, PoolSize: -1}.withDefaults()
	if validateNodeReadWorkers(readWorkersNode(&negative, base)) == nil {
		t.Fatal("negative pool_size accepted")
	}
}

func TestMaxPerKeyDefaultsAndMapsOntoTheWarmPool(t *testing.T) {
	base := worker.WarmPoolConfig{Enabled: true, QueueDepth: 64, WorkerMemoryMB: 64}
	for name, tc := range map[string]struct{ workers, perKey, want int }{
		"default is 2":           {4, 0, 2},
		"default capped by pool": {1, 0, 1},
		"explicit":               {4, 3, 3},
		"explicit one":           {4, 1, 1},
	} {
		rw := ReadWorkersConfig{MaxWorkers: tc.workers, MaxPerKey: tc.perKey}.withDefaults()
		node := readWorkersNode(&rw, base)
		if err := validateNodeReadWorkers(node); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got := node.effectiveWarmPool(); got.MaxPerKey != tc.want || got.Validate() != nil {
			t.Errorf("%s: max_per_key %d, want %d (%v)", name, got.MaxPerKey, tc.want, got.Validate())
		}
	}
}

func TestReadAdapterSelection(t *testing.T) {
	full := worker.OperationProcessConfig{Binary: "/opt/kelvo/kelvo-adapter-go", SHA256: strings.Repeat("a", 64), PrivateOpenDiagnostics: true}
	slim := worker.OperationProcessConfig{Binary: "/opt/kelvo/kelvo-adapter-relational", SHA256: strings.Repeat("b", 64)}
	always, never := func(worker.OperationProcessConfig) bool { return true }, func(worker.OperationProcessConfig) bool { return false }
	for name, tc := range map[string]struct {
		rw     *ReadWorkersConfig
		usable func(worker.OperationProcessConfig) bool
		want   string
	}{
		"no read_workers":             {nil, always, full.Binary},
		"default without slim pinned": {&ReadWorkersConfig{Adapter: adapterRelational}, always, full.Binary},
		"full requested":              {&ReadWorkersConfig{Adapter: adapterFull, RelationalAdapter: &slim}, always, full.Binary},
		"slim usable":                 {&ReadWorkersConfig{Adapter: adapterRelational, RelationalAdapter: &slim}, always, slim.Binary},
		"slim missing or mismatched":  {&ReadWorkersConfig{Adapter: adapterRelational, RelationalAdapter: &slim}, never, full.Binary},
	} {
		got, isSlim := readAdapter(tc.rw, full, tc.usable)
		if got.Binary != tc.want || isSlim != (tc.want == slim.Binary) {
			t.Errorf("%s: %q slim=%v", name, got.Binary, isSlim)
		}
		if isSlim && (got.SHA256 != slim.SHA256 || !got.PrivateOpenDiagnostics) {
			t.Errorf("%s: slim pin or diagnostics lost: %+v", name, got)
		}
	}
}

func TestReadWorkersAdapterSetting(t *testing.T) {
	base := worker.WarmPoolConfig{Enabled: true, QueueDepth: 64, WorkerMemoryMB: 64}
	pinned := &worker.OperationProcessConfig{Binary: "/opt/kelvo/kelvo-adapter-relational", SHA256: strings.Repeat("b", 64)}
	for name, tc := range map[string]struct {
		rw ReadWorkersConfig
		ok bool
	}{
		"default is relational":         {ReadWorkersConfig{MaxWorkers: 1}, true},
		"full":                          {ReadWorkersConfig{MaxWorkers: 1, Adapter: adapterFull}, true},
		"relational with a pinned file": {ReadWorkersConfig{MaxWorkers: 1, Adapter: adapterRelational, RelationalAdapter: pinned}, true},
		"unknown adapter":               {ReadWorkersConfig{MaxWorkers: 1, Adapter: "duckdb"}, false},
		"bad pin":                       {ReadWorkersConfig{MaxWorkers: 1, RelationalAdapter: &worker.OperationProcessConfig{Binary: "/opt/x", SHA256: "short"}}, false},
	} {
		rw := tc.rw.withDefaults()
		if rw.Adapter == "" {
			t.Fatalf("%s: adapter has no default", name)
		}
		if err := validateNodeReadWorkers(readWorkersNode(&rw, base)); (err == nil) != tc.ok {
			t.Errorf("%s: %v", name, err)
		}
	}
	if (ReadWorkersConfig{MaxWorkers: 1}).withDefaults().Adapter != adapterRelational {
		t.Fatal("default adapter is not relational")
	}
}

func TestReadAdapterMetrics(t *testing.T) {
	var none strings.Builder
	(&Node{}).writeReadAdapterMetrics(&none)
	if none.Len() != 0 {
		t.Fatalf("metrics without a slim adapter: %q", none.String())
	}
	slim := worker.OperationProcessConfig{Binary: "/opt/kelvo/kelvo-adapter-relational", SHA256: strings.Repeat("b", 64)}
	full := worker.OperationProcessConfig{Binary: "/opt/kelvo/kelvo-adapter-go", SHA256: strings.Repeat("a", 64)}
	n := &Node{operations: &nodeOperations{slimLauncher: worker.NewSlimFallbackLauncher(nil, slim, full)}}
	var out strings.Builder
	n.writeReadAdapterMetrics(&out)
	for _, want := range []string{"kelvo_read_adapter_slim_active 1\n", "kelvo_read_adapter_slim_fallbacks_total 0\n"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("missing %q in %q", want, out.String())
		}
	}
}

// failSlimLauncher refuses to launch the slim binary and accepts any other.
type failSlimLauncher struct{ slim string }

func (l failSlimLauncher) Launch(_ context.Context, _ *worker.Executor, cfg worker.OperationProcessConfig, _ query.Limits) (worker.WorkerProcess, func() error, error) {
	if cfg.Binary == l.slim {
		return nil, nil, errors.New("exec failed")
	}
	return nil, nil, nil
}

// A slim binary that fails its launch check flips the gauge to 0 and counts
// each launch that ran on the full adapter instead.
func TestReadAdapterMetricsAfterFallback(t *testing.T) {
	slim := worker.OperationProcessConfig{Binary: "/nonexistent/kelvo-adapter-relational", SHA256: strings.Repeat("b", 64)}
	full := worker.OperationProcessConfig{Binary: "/nonexistent/kelvo-adapter-go", SHA256: strings.Repeat("a", 64)}
	launcher := worker.NewSlimFallbackLauncher(failSlimLauncher{slim: slim.Binary}, slim, full)
	n := &Node{operations: &nodeOperations{slimLauncher: launcher}}
	for range 2 {
		if _, _, err := launcher.Launch(context.Background(), nil, slim, query.Limits{}); err != nil {
			t.Fatal(err)
		}
	}
	var out strings.Builder
	n.writeReadAdapterMetrics(&out)
	for _, want := range []string{"kelvo_read_adapter_slim_active 0\n", "kelvo_read_adapter_slim_fallbacks_total 2\n"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("missing %q in %q", want, out.String())
		}
	}
}
