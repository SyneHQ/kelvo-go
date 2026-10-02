package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestMain(m *testing.M) {
	if len(os.Args) == 2 {
		switch os.Args[1] {
		case "worker":
			os.Exit(testWorkerMain())
		case "--group-child":
			time.Sleep(time.Hour)
			os.Exit(0)
		case "--group-parent", "--group-orphan-parent", "--group-cooperative-parent", "--group-ignores-term-parent":
			ctx := context.Background()
			if os.Args[1] == "--group-cooperative-parent" {
				var stop context.CancelFunc
				ctx, stop = signal.NotifyContext(ctx, syscall.SIGTERM)
				defer stop()
			}
			if os.Args[1] == "--group-ignores-term-parent" {
				signal.Ignore(syscall.SIGTERM)
			}
			child := exec.Command(os.Args[0], "--group-child")
			if err := child.Start(); err != nil {
				os.Exit(2)
			}
			fmt.Fprintln(os.Stdout, child.Process.Pid)
			if os.Args[1] == "--group-parent" || os.Args[1] == "--group-ignores-term-parent" {
				time.Sleep(time.Hour)
			} else if os.Args[1] == "--group-cooperative-parent" {
				<-ctx.Done()
				time.Sleep(75 * time.Millisecond)
				fmt.Fprintln(os.Stderr, "cooperative-cleanup-complete")
			}
			os.Exit(0)
		}
	}
	os.Exit(m.Run())
}

func testWorkerMain() int {
	var in Input
	if json.NewDecoder(os.Stdin).Decode(&in) != nil {
		return 2
	}
	if in.Request.SQL == "SELECT object_environment" {
		if !objectWorkerEnvironmentMatches(in) {
			return 2
		}
	} else if in.Request.SQL == "SELECT source_environment" || in.Request.SQL == "SELECT aws_environment" {
		if in.Request.Mode != "native" || in.Request.ConnectionID != "selected" || len(in.Config.Sources) != 1 || in.Config.Sources[0].ID != "selected" {
			return 2
		}
		if os.Getenv("KELVO_SOURCE_SELECTED_URL") != "https://source.example" || os.Getenv("KELVO_SOURCE_SELECTED_TOKEN") != "fixture-source-token" {
			return 2
		}
		if in.Request.SQL == "SELECT aws_environment" && (os.Getenv("KELVO_SOURCE_SELECTED_USER") != "fixture-access-key" || os.Getenv("KELVO_SOURCE_SELECTED_PASSWORD") != "fixture-secret-key") {
			return 2
		}
		for _, name := range []string{"KELVO_SOURCE_OTHER_TOKEN", "KELVO_TOKEN", "KELVO_TENANT_A_NATS_PASSWORD", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_PROFILE", "GOOGLE_APPLICATION_CREDENTIALS", "AZURE_CLIENT_SECRET"} {
			if _, present := os.LookupEnv(name); present {
				return 2
			}
		}
	} else if in.Request.Mode != "federated" {
		return 2
	}
	if in.Request.SQL == "SELECT wait" {
		time.Sleep(time.Hour)
		return 0
	}
	if in.Request.SQL == "SELECT cooperative_wait" {
		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
		defer stop()
		<-ctx.Done()
		time.Sleep(75 * time.Millisecond)
		_ = json.NewEncoder(os.Stderr).Encode(Outcome{Stats: query.Stats{Backend: "cooperative-cleanup-complete"}})
		return 0
	}
	schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int64}}, nil)
	b := array.NewInt64Builder(memory.DefaultAllocator)
	b.AppendValues([]int64{1, 2, 3}, nil)
	column := b.NewArray()
	b.Release()
	record := array.NewRecordBatch(schema, []arrow.Array{column}, 3)
	column.Release()
	defer record.Release()
	pipeLimits := in.Limits
	pipeLimits.ResultCompression = ""
	sink := NewIPCSink(os.Stdout, pipeLimits)
	defer sink.Abort()
	if sink.Schema(schema) != nil || sink.Write(record) != nil || sink.Finish() != nil {
		return 3
	}
	// Deliberately incorrect claimed counts: the parent must use observed data.
	outcome := Outcome{Stats: query.Stats{Rows: 999, Batches: 999, Backend: "test"}}
	if json.NewEncoder(os.Stderr).Encode(outcome) != nil {
		return 4
	}
	return 0
}

func TestExecutorNormalizesModeAndCountsObservedResults(t *testing.T) {
	e, err := New(catalog.Config{}, query.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		stats, err := e.Execute(context.Background(), query.Request{SQL: "SELECT 1"}, &workerTestSink{})
		if err != nil {
			t.Fatal(err)
		}
		if stats.Rows != 3 || stats.Batches != 1 || stats.WireBytes == 0 {
			t.Fatalf("trusted fabricated worker counts: %+v", stats)
		}
	}
}

func TestExecutorRejectsRequestsBeforeStartingProcess(t *testing.T) {
	e := &Executor{Binary: "/does/not/exist", Limits: query.DefaultLimits()}
	for _, req := range []query.Request{{SQL: ""}, {SQL: strings.Repeat("x", (64<<10)+1)}, {SQL: "SELECT 1", ConnectionID: "unexpected"}, {SQL: "SELECT 1", Mode: "other"}} {
		_, err := e.Execute(context.Background(), req, &workerTestSink{})
		if err == nil || query.PublicError(err).Code != "INVALID_ARGUMENT" {
			t.Fatalf("request reached subprocess: %v", err)
		}
	}
	e.Config = catalog.Config{Sources: []catalog.Source{{ID: "db", Type: "postgres", DSNEnv: "LD_PRELOAD"}}}
	_, err := e.Execute(context.Background(), query.Request{SQL: "SELECT 1", Sources: []string{"db"}}, &workerTestSink{})
	if err == nil || query.PublicError(err).Code != "CONFIGURATION_ERROR" {
		t.Fatalf("ambient capability reference accepted: %v", err)
	}
}

func TestExecutorForwardsOnlySelectedSourceToken(t *testing.T) {
	t.Setenv("KELVO_SOURCE_SELECTED_URL", "https://source.example")
	t.Setenv("KELVO_SOURCE_SELECTED_TOKEN", "fixture-source-token")
	for _, name := range []string{"KELVO_SOURCE_OTHER_TOKEN", "KELVO_TOKEN", "KELVO_TENANT_A_NATS_PASSWORD"} {
		t.Setenv(name, "fixture-must-stay-in-parent")
	}
	cfg := catalog.Config{Sources: []catalog.Source{
		{ID: "selected", Type: "databricks", URLEnv: "KELVO_SOURCE_SELECTED_URL", TokenEnv: "KELVO_SOURCE_SELECTED_TOKEN"},
		{ID: "other", Type: "databricks", TokenEnv: "KELVO_SOURCE_OTHER_TOKEN"},
	}}
	e, err := New(cfg, query.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	stats, err := e.Execute(context.Background(), query.Request{Mode: "native", ConnectionID: "selected", SQL: "SELECT source_environment"}, &workerTestSink{})
	if err != nil || stats.Rows != 3 {
		t.Fatalf("selected source credentials did not reach the isolated worker: %v", err)
	}
}

func TestExecutorRejectsMissingOrForbiddenSourceToken(t *testing.T) {
	for _, tokenEnv := range []string{"KELVO_SOURCE_MISSING_TOKEN", "KELVO_TOKEN", "LD_PRELOAD"} {
		t.Run(tokenEnv, func(t *testing.T) {
			if tokenEnv == "KELVO_SOURCE_MISSING_TOKEN" {
				t.Setenv(tokenEnv, "")
				if err := os.Unsetenv(tokenEnv); err != nil {
					t.Fatal(err)
				}
			}
			e := &Executor{Binary: "/does/not/exist", Limits: query.DefaultLimits(), Config: catalog.Config{Sources: []catalog.Source{
				{ID: "selected", Type: "databricks", TokenEnv: tokenEnv},
			}}}
			_, err := e.Execute(context.Background(), query.Request{Mode: "native", ConnectionID: "selected", SQL: "SELECT 1"}, &workerTestSink{})
			if err == nil || query.PublicError(err).Code != "CONFIGURATION_ERROR" {
				t.Fatalf("invalid token reference reached process launch: %v", err)
			}
		})
	}
}

func TestExecutorIsolatesExplicitCloudCredentials(t *testing.T) {
	for name, value := range map[string]string{
		"KELVO_SOURCE_SELECTED_URL":      "https://source.example",
		"KELVO_SOURCE_SELECTED_TOKEN":    "fixture-source-token",
		"KELVO_SOURCE_SELECTED_USER":     "fixture-access-key",
		"KELVO_SOURCE_SELECTED_PASSWORD": "fixture-secret-key",
	} {
		t.Setenv(name, value)
	}
	for _, name := range []string{"KELVO_SOURCE_OTHER_TOKEN", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_PROFILE", "GOOGLE_APPLICATION_CREDENTIALS", "AZURE_CLIENT_SECRET"} {
		t.Setenv(name, "fixture-must-stay-in-parent")
	}
	for _, kind := range []string{"athena", "dynamodb"} {
		cfg := catalog.Config{Sources: []catalog.Source{
			{ID: "selected", Type: kind, URLEnv: "KELVO_SOURCE_SELECTED_URL", UsernameEnv: "KELVO_SOURCE_SELECTED_USER", PasswordEnv: "KELVO_SOURCE_SELECTED_PASSWORD", TokenEnv: "KELVO_SOURCE_SELECTED_TOKEN"},
			{ID: "other", Type: kind, TokenEnv: "KELVO_SOURCE_OTHER_TOKEN"},
		}}
		e, err := New(cfg, query.DefaultLimits())
		if err != nil {
			t.Fatal(err)
		}
		stats, err := e.Execute(context.Background(), query.Request{Mode: "native", ConnectionID: "selected", SQL: "SELECT aws_environment"}, &workerTestSink{})
		if err != nil || stats.Rows != 3 {
			t.Fatalf("%s credential boundary failed: %v", kind, err)
		}
	}
}

func TestExecutorCancellationInterruptsBlockedIPC(t *testing.T) {
	e, err := New(catalog.Config{}, query.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err = e.Execute(ctx, query.Request{SQL: "SELECT wait"}, &workerTestSink{})
	if err == nil || query.PublicError(err).Code != "DEADLINE_EXCEEDED" {
		t.Fatalf("cancellation: %v", err)
	}
	if time.Since(started) > 2*time.Second {
		t.Fatal("blocked pipe delayed cancellation")
	}
}
