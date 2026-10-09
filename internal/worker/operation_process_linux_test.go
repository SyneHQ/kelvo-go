//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/filesnapshot"
	"github.com/SYNEHQ/kelvo-go/internal/admission"
	"github.com/SYNEHQ/kelvo-go/internal/containment"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"golang.org/x/sys/unix"
)

func init() {
	if os.Getenv("KELVO_OPERATION_PROCESS") != "1" || len(os.Args) != 2 {
		return
	}
	switch os.Args[1] {
	case "worker":
		os.Exit(operationFixtureMain())
	case "--operation-descendant":
		time.Sleep(time.Hour)
		os.Exit(0)
	}
}

func operationFixtureMain() int {
	raw, err := io.ReadAll(io.LimitReader(os.Stdin, adapter.MaxProcessRequestBytes+1))
	if err != nil {
		return 2
	}
	input, err := adapter.ParseProcessRequest(raw)
	clear(raw)
	if err != nil {
		return 3
	}
	for _, key := range []string{"KELVO_TOKEN", "AWS_SECRET_ACCESS_KEY", "KELVO_FIXTURE_PARENT_SECRET"} {
		if _, present := os.LookupEnv(key); present {
			return 4
		}
	}
	if (input.SourceFile == nil && input.PrivateTransport == nil && input.Source.Password != "fixture-private-value") || len(os.Environ()) != 5 {
		return 5
	}
	if input.PrivateTransport != nil && privateOperationFixtureChannel(input) != nil {
		return 32
	}
	if input.SourceFile != nil {
		var st unix.Stat_t
		if unix.Fstat(6, &st) != nil || st.Mode&unix.S_IFMT != unix.S_IFREG || st.Nlink != 0 {
			return 15
		}
		file := os.NewFile(6, "snapshot")
		content, err := io.ReadAll(io.LimitReader(file, input.SourceFile.Bytes+1))
		sum := sha256.Sum256(content)
		if err != nil || int64(len(content)) != input.SourceFile.Bytes || hex.EncodeToString(sum[:]) != input.SourceFile.SHA256 {
			return 16
		}
		// Ownership alone must not let the child reopen its inherited inode for
		// writing, even if it changes POSIX mode bits on the read-only handle.
		if unix.Fchmod(6, 0600) != nil {
			return 17
		}
		writer, err := os.OpenFile("/proc/self/fd/6", os.O_WRONLY, 0)
		if writer != nil {
			writer.Close()
			return 18
		}
		if !errors.Is(err, os.ErrPermission) {
			return 19
		}
		if unix.Fchmod(6, 0400) != nil {
			return 20
		}
	}
	if input.Runtime != nil && operationFixtureJVM(input) != nil {
		return 21
	}
	var lease, binary unix.Stat_t
	if unix.Fstat(4, &lease) != nil || lease.Mode&unix.S_IFMT != unix.S_IFREG || unix.Fstat(5, &binary) != nil || binary.Mode&unix.S_IFMT != unix.S_IFREG {
		return 6
	}
	flags, err := unix.FcntlInt(5, unix.F_GETFL, 0)
	if err != nil || flags&unix.O_ACCMODE != unix.O_RDONLY {
		return 7
	}
	if input.Source.URL != "" && input.Runtime == nil {
		if _, err := os.ReadFile(input.Source.URL); !errors.Is(err, os.ErrPermission) {
			return 8
		}
	}
	mode := "fixture"
	if input.Request.Spec.Query != nil {
		mode = strings.TrimPrefix(input.Request.Spec.Query.SQL, "SELECT ")
	}
	if input.Request.Spec.Statement != nil {
		mode = strings.TrimPrefix(input.Request.Spec.Statement.SQL, "UPDATE ")
		if input.Request.Spec.Statement.Batch != nil {
			mode = strings.TrimPrefix(input.Request.Spec.Statement.Batch.Statements[0].SQL, "UPDATE ")
		}
	}
	if input.Request.Spec.Native != nil {
		mode = input.Request.Spec.Native.Command
	}
	if mode == "wait" {
		time.Sleep(time.Hour)
		return 0
	}
	if mode == "missing_receipt" {
		return 0
	}
	r := operations.Receipt{Version: operations.Version, OperationID: input.OperationID, RequestSHA256: input.RequestSHA256, Outcome: operations.Completed, Effect: operations.EffectNone}
	if input.Request.Kind.Mutating() {
		r.Effect = operations.EffectCommitted
	}
	if input.SourceFile != nil && input.Request.Kind == operations.StatementExecute {
		candidate := []byte("replacement-contents")
		sum := sha256.Sum256(candidate)
		frame := adapter.FileProcessReceipt{Version: 1, Receipt: r, Candidate: &filesnapshot.Descriptor{Version: 1, Format: input.SourceFile.Format, Bytes: int64(len(candidate)), SHA256: hex.EncodeToString(sum[:])}}
		if mode == "file_bad_hash" {
			frame.Candidate.SHA256 = strings.Repeat("b", 64)
		}
		if mode == "file_truncated" {
			candidate = candidate[:len(candidate)-1]
		}
		if _, err := os.Stdout.Write(candidate); err != nil {
			return 22
		}
		if json.NewEncoder(os.NewFile(3, "receipt")).Encode(frame) != nil {
			return 23
		}
		if mode == "file_crash" {
			return 24
		}
		return 0
	}
	if mode == "committed_broken_arrow" {
		receipt := os.NewFile(3, "receipt")
		if json.NewEncoder(receipt).Encode(r) != nil {
			return 12
		}
		_, _ = os.Stdout.Write([]byte("broken-arrow-pipe"))
		return 13
	}
	if mode == "rejected" {
		r.Outcome = operations.Rejected
		r.Effect = operations.EffectNone
		r.ErrorCode = "UNSUPPORTED"
	}
	if mode == "large_receipt" {
		for i, statement := range input.Request.Spec.Statement.Batch.Statements {
			digest, err := operations.StatementDigest(statement)
			if err != nil {
				return 14
			}
			r.Steps = append(r.Steps, operations.StepReceipt{Index: i, SHA256: digest, Effect: operations.EffectCommitted})
		}
	}
	if input.Request.Kind == operations.QueryRead || input.Request.Kind == operations.MetadataInspect || (input.Request.Kind == operations.NativeExecute && input.Request.Spec.Native.ReturnResult) {
		values := []int64{1, 2, 3}
		if mode == "descendant" {
			child := exec.Command("/proc/self/exe", "--operation-descendant")
			// Reuse granted descriptors; nil streams would reopen /dev/null,
			// which is intentionally outside this worker's filesystem grants.
			child.Stdin, child.Stdout, child.Stderr = os.Stdin, os.Stdout, os.Stderr
			if child.Start() != nil {
				return 9
			}
			values = []int64{int64(child.Process.Pid)}
		}
		schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int64}}, nil)
		builder := array.NewInt64Builder(memory.DefaultAllocator)
		builder.AppendValues(values, nil)
		column := builder.NewArray()
		builder.Release()
		record := array.NewRecordBatch(schema, []arrow.Array{column}, int64(len(values)))
		column.Release()
		var wire bytes.Buffer
		writer := ipc.NewWriter(&wire, ipc.WithSchema(schema))
		writeErr := writer.Write(record)
		record.Release()
		if writeErr != nil || writer.Close() != nil {
			return 10
		}
		if _, err := os.Stdout.Write(wire.Bytes()); err != nil {
			return 11
		}
		if mode == "descendant" {
			time.Sleep(time.Hour)
			return 0
		}
		hash := sha256.Sum256(wire.Bytes())
		r.Result = &operations.ResultRef{ID: input.OperationID, SHA256: hex.EncodeToString(hash[:]), Bytes: int64(wire.Len()), Rows: int64(len(values)), Format: "arrow_ipc"}
		if mode == "wrong_bytes" {
			r.Result.Bytes++
		}
	}
	receipt := os.NewFile(3, "receipt")
	if mode == "oversized" {
		_, _ = receipt.Write(bytes.Repeat([]byte("x"), operations.MaxReceiptBytes+1))
		return 0
	}
	if r.Validate() != nil || json.NewEncoder(receipt).Encode(r) != nil {
		return 12
	}
	if mode == "polluted_stdout" {
		_, _ = os.Stdout.Write([]byte("unowned-output"))
	}
	if mode == "committed_exit_error" {
		return 13
	}
	return 0
}

func operationExecutable(t *testing.T) OperationProcessConfig {
	t.Helper()
	path, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	path, err = filepath.EvalSymlinks(path)
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		t.Fatal(err)
	}
	return OperationProcessConfig{Binary: path, SHA256: hex.EncodeToString(hash.Sum(nil))}
}

func TestOperationBinaryPinRejectsUnsafeInputs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "adapter")
	raw := []byte("fixture executable content")
	if err := os.WriteFile(path, raw, 0700); err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(raw)
	cfg := OperationProcessConfig{Binary: path, SHA256: hex.EncodeToString(hash[:])}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		change func(*OperationProcessConfig)
	}{
		{"relative", func(c *OperationProcessConfig) { c.Binary = "adapter" }},
		{"missing", func(c *OperationProcessConfig) { c.Binary += "missing" }},
		{"wrong_digest", func(c *OperationProcessConfig) { c.SHA256 = strings.Repeat("a", 64) }},
		{"invalid_digest", func(c *OperationProcessConfig) { c.SHA256 = "bad" }},
		{"directory", func(c *OperationProcessConfig) { c.Binary = filepath.Dir(path) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := cfg
			test.change(&c)
			if c.Validate() == nil {
				t.Fatal("unsafe binary accepted")
			}
		})
	}
	link := path + "-link"
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	if err := (OperationProcessConfig{Binary: link, SHA256: cfg.SHA256}).Validate(); err == nil {
		t.Fatal("symlink accepted")
	}
	for _, mode := range []os.FileMode{0600, 0770, 0777} {
		if err := os.Chmod(path, mode); err != nil {
			t.Fatal(err)
		}
		if cfg.Validate() == nil {
			t.Fatal("unsafe executable mode accepted", mode)
		}
	}
}

func TestOperationBinaryDescriptorSurvivesPathReplacement(t *testing.T) {
	path := filepath.Join(t.TempDir(), "adapter")
	original := []byte("original verified executable")
	if err := os.WriteFile(path, original, 0700); err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(original)
	file, identity, err := openOperationBinary(context.Background(), OperationProcessConfig{Binary: path, SHA256: hex.EncodeToString(hash[:])})
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if err := os.WriteFile(path+".replacement", []byte("replacement executable"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path+".replacement", path); err != nil {
		t.Fatal(err)
	}
	if err := operationBinaryCurrent(file, identity); err != nil {
		t.Fatal("descriptor lost its inode", err)
	}
	got, err := io.ReadAll(file)
	if err != nil || !bytes.Equal(got, original) {
		t.Fatal("descriptor followed replacement", err)
	}
	if err := file.Chmod(0500); err != nil {
		t.Fatal(err)
	}
	if operationBinaryCurrent(file, identity) == nil {
		t.Fatal("metadata mutation was missed")
	}
}

func TestContainedOperationProcessReceiptsAndIsolation(t *testing.T) {
	executor, manager, pool := containedExecutor(t)
	cfg := operationExecutable(t)
	for _, key := range []string{"KELVO_TOKEN", "AWS_SECRET_ACCESS_KEY", "KELVO_FIXTURE_PARENT_SECRET"} {
		t.Setenv(key, "must-stay-in-parent")
	}
	forbidden := filepath.Join(t.TempDir(), "private-parent-file")
	if err := os.WriteFile(forbidden, []byte("private fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		mode      string
		kind      operations.Kind
		outcome   operations.Outcome
		wantError bool
	}{
		{"fixture", operations.QueryRead, operations.Completed, false},
		{"fixture", operations.MetadataInspect, operations.Completed, false},
		{"fixture", operations.ConnectionTest, operations.Completed, false},
		{"fixture", operations.StatementExecute, operations.Completed, false},
		{"fixture", operations.NativeExecute, operations.Completed, false},
		{"no_result", operations.NativeExecute, operations.Completed, false},
		{"wrong_bytes", operations.NativeExecute, operations.Completed, true},
		{"committed_exit_error", operations.NativeExecute, operations.Completed, true},
		{"committed_broken_arrow", operations.NativeExecute, operations.Completed, true},
		{"large_receipt", operations.StatementExecute, operations.Completed, false},
		{"committed_exit_error", operations.StatementExecute, operations.Completed, true},
		{"polluted_stdout", operations.StatementExecute, operations.Completed, true},
		{"missing_receipt", operations.StatementExecute, operations.OutcomeUnknown, true},
		{"oversized", operations.StatementExecute, operations.OutcomeUnknown, true},
		{"rejected", operations.StatementExecute, operations.Rejected, false},
		{"wrong_bytes", operations.QueryRead, operations.Failed, true},
	} {
		t.Run(string(test.kind)+"/"+test.mode, func(t *testing.T) {
			input := operationProcessInput(t, test.kind, test.mode)
			input.Source.URL = forbidden
			sink := &workerTestSink{}
			receipt, err := executor.ExecuteOperation(context.Background(), cfg, input, sink)
			if (err != nil) != test.wantError || receipt.Outcome != test.outcome {
				t.Fatal("unexpected process outcome", receipt, err)
			}
			if test.outcome == operations.Completed && test.kind.Mutating() && receipt.Effect != operations.EffectCommitted {
				t.Fatal("lost committed receipt")
			}
			if receipt.Result != nil && (receipt.Result.Rows != sink.rows || receipt.Result.Bytes == 0) {
				t.Fatal("result was not observed", receipt)
			}
			if pool.Snapshot().Active != 0 || manager.Status().Active != 0 {
				t.Fatal("operation leaked custody")
			}
		})
	}
}

func TestContainedOperationCancellationKillsDescendants(t *testing.T) {
	if os.Getenv("KELVO_TEST_OPERATION_CANCEL_REAPER") != "1" {
		for _, key := range []string{"KELVO_TEST_CGROUP_ROOT", "KELVO_TEST_CGROUP_STATE", "KELVO_TEST_SANDBOX"} {
			if os.Getenv(key) == "" {
				t.Skip("explicit disposable delegation and launcher required")
			}
		}
		// Isolate subreaper ownership from other tests and their exec.Cmd waits.
		// The helper must pass this exact test; a missing or skipped child gate
		// must not turn into a successful outer result.
		command := exec.Command(os.Args[0], "-test.run=^TestContainedOperationCancellationKillsDescendants$", "-test.timeout=30s", "-test.v")
		command.Env = append(os.Environ(), "KELVO_TEST_OPERATION_CANCEL_REAPER=1")
		output, err := command.CombinedOutput()
		if err != nil || !bytes.Contains(output, []byte("--- PASS: TestContainedOperationCancellationKillsDescendants")) || bytes.Contains(output, []byte("--- SKIP:")) {
			t.Fatalf("isolated cancellation gate failed: %v\n%s", err, output)
		}
		return
	}
	if err := unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0); err != nil {
		t.Fatal("cannot own the fixture descendant", err)
	}
	executor, manager, pool := containedExecutor(t)
	cfg := operationExecutable(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var descendant int
	sink := &workerTestSink{write: func(record arrow.RecordBatch) error {
		descendant = int(record.Column(0).(*array.Int64).Value(0))
		cancel()
		return nil
	}}
	input := operationProcessInput(t, operations.QueryRead, "descendant")
	receipt, err := executor.ExecuteOperation(ctx, cfg, input, sink)
	if err == nil || receipt.Outcome != operations.Failed || descendant <= 0 {
		t.Fatal("cancellation fixture did not dispatch", receipt, err, descendant)
	}
	requireProcessTerminated(t, descendant)
	// ExecuteOperation has joined its own exec.Cmd before returning. Reap only
	// the exact adopted fixture PID; never steal another command's child wait.
	var status unix.WaitStatus
	if reaped, err := unix.Wait4(descendant, &status, unix.WNOHANG, nil); err != nil || reaped != descendant {
		t.Fatal("terminated fixture descendant was not reaped", reaped, err)
	}
	if pool.Snapshot().Active != 0 || manager.Status().Active != 0 {
		t.Fatal("cancelled operation leaked custody")
	}
	input = operationProcessInput(t, operations.StatementExecute, "wait")
	input.Limits.TimeoutMS = 300
	receipt, err = executor.ExecuteOperation(context.Background(), cfg, input, nil)
	if err == nil || receipt.Outcome != operations.OutcomeUnknown {
		t.Fatal("interrupted mutation claimed no effects", receipt, err)
	}
	if pool.Snapshot().Active != 0 || manager.Status().Active != 0 {
		t.Fatal("interrupted mutation leaked custody")
	}
}

func TestContainedOperationResolvesOnlyAfterAdmission(t *testing.T) {
	executor, manager, pool := containedExecutor(t)
	cfg := operationExecutable(t)
	input := operationProcessInput(t, operations.StatementExecute, "fixture")
	calls := 0
	receipt, err := executor.ExecuteResolvedOperation(context.Background(), cfg, input.OperationID, input.RequestSHA256, func(context.Context) (adapter.ProcessRequest, error) {
		calls++
		if pool.Snapshot().Active != 1 || manager.Status().Active != 1 {
			t.Error("resolved before resource ownership")
		}
		return adapter.ProcessRequest{}, errors.New("fixture resolver unavailable")
	}, nil)
	if err == nil || calls != 1 || receipt.Validate() != nil || receipt.Outcome != operations.Rejected || receipt.Effect != operations.EffectNone {
		t.Fatal(receipt, err, calls)
	}
	if pool.Snapshot().Active != 0 || manager.Status().Active != 0 {
		t.Fatal("resolver failure leaked custody")
	}
	reservation, err := pool.Acquire(context.Background(), admission.Request{MemoryBytes: 224 << 20, ScratchBytes: 16 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer reservation.Release()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	receipt, err = executor.ExecuteResolvedOperation(ctx, cfg, input.OperationID, input.RequestSHA256, func(context.Context) (adapter.ProcessRequest, error) { calls++; return input, nil }, nil)
	if err == nil || calls != 1 || receipt.Outcome != operations.Rejected || receipt.Effect != operations.EffectNone {
		t.Fatal("resource rejection resolved credentials", receipt, err, calls)
	}
}

func TestContainedOperationExecutesPinnedDescriptorAfterReplacement(t *testing.T) {
	executor, manager, pool := containedExecutor(t)
	cfg := operationExecutable(t)
	original, err := os.Open(cfg.Binary)
	if err != nil {
		t.Fatal(err)
	}
	defer original.Close()
	cfg.Binary = filepath.Join(t.TempDir(), "adapter")
	copy, err := os.OpenFile(cfg.Binary, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0700)
	if err != nil {
		t.Fatal(err)
	}
	_, copyErr := io.Copy(copy, original)
	closeErr := copy.Close()
	if copyErr != nil || closeErr != nil {
		t.Fatal(copyErr, closeErr)
	}
	input := operationProcessInput(t, operations.ConnectionTest, "fixture")
	receipt, err := executor.ExecuteResolvedOperation(context.Background(), cfg, input.OperationID, input.RequestSHA256, func(context.Context) (adapter.ProcessRequest, error) {
		if err := os.WriteFile(cfg.Binary+".new", []byte("unverified replacement must not execute"), 0700); err != nil {
			return adapter.ProcessRequest{}, err
		}
		if err := os.Rename(cfg.Binary+".new", cfg.Binary); err != nil {
			return adapter.ProcessRequest{}, err
		}
		input.CredentialsValidUntil = time.Now().Unix() + 5
		return input, nil
	}, nil)
	if err != nil || receipt.Outcome != operations.Completed {
		t.Fatal("launcher followed replaced executable path", receipt, err)
	}
	if pool.Snapshot().Active != 0 || manager.Status().Active != 0 {
		t.Fatal("pinned binary run leaked custody")
	}
}

func TestContainedOperationRequiresEveryBoundary(t *testing.T) {
	executor, _, _ := containedExecutor(t)
	cfg := operationExecutable(t)
	for _, boundary := range []string{"sandbox", "scratch", "pool", "containment", "inherited_custody"} {
		t.Run(boundary, func(t *testing.T) {
			copy := *executor
			ctx := context.Background()
			switch boundary {
			case "sandbox":
				copy.SandboxPath = ""
			case "scratch":
				copy.ScratchRoot = nil
			case "pool":
				copy.ResourcePool = nil
			case "containment":
				copy.Containment = nil
			case "inherited_custody":
				custody, _ := containment.NewCustody(func() {})
				ctx = containment.WithCustody(ctx, custody)
			}
			input := operationProcessInput(t, operations.StatementExecute, "fixture")
			receipt, err := copy.ExecuteOperation(ctx, cfg, input, nil)
			if err == nil || receipt.Outcome != operations.Rejected || receipt.Effect != operations.EffectNone {
				t.Fatal("missing boundary allowed execution", receipt, err)
			}
		})
	}
}

func TestContainedOperationFinalizesResultBeforeRelease(t *testing.T) {
	executor, manager, pool := containedExecutor(t)
	cfg := operationExecutable(t)
	input := operationProcessInput(t, operations.QueryRead, "fixture")
	calls := 0
	sink := &operationFinalizerTestSink{finalize: func(receipt operations.Receipt, err error) (operations.Receipt, error) {
		calls++
		if pool.Snapshot().Active != 1 || manager.Status().Active != 0 {
			t.Error("finalization occurred outside resource custody")
		}
		if err != nil || receipt.Outcome != operations.Completed || receipt.Result == nil {
			t.Error("incomplete result finalized", receipt, err)
			return receipt, err
		}
		receipt.Result.ID = "retained-result"
		return receipt, nil
	}}
	receipt, err := executor.ExecuteOperation(context.Background(), cfg, input, sink)
	if err != nil || calls != 1 || receipt.Result == nil || receipt.Result.ID != "retained-result" {
		t.Fatal("finalized result was lost", receipt, err, calls)
	}
	if pool.Snapshot().Active != 0 || manager.Status().Active != 0 {
		t.Fatal("result finalization leaked custody")
	}
}

func TestContainedOperationSourceSnapshotIsReadOnly(t *testing.T) {
	executor, manager, pool := containedExecutor(t)
	cfg := operationExecutable(t)
	input := operationProcessInput(t, operations.QueryRead, "fixture")
	data := []byte("id,value\n1,private-test-value\n")
	sum := sha256.Sum256(data)
	input.Source.Engine, input.Source.Password = "csv", ""
	input.SourceFile = &filesnapshot.Descriptor{Version: 1, Format: "csv", Bytes: int64(len(data)), SHA256: hex.EncodeToString(sum[:])}
	input.Source.Options = map[string]string{"file_format": "csv", "file_bytes": fmt.Sprint(len(data)), "file_sha256": input.SourceFile.SHA256}
	input.Limits.MemoryMB, input.Limits.Threads, input.Limits.MaxTempMB = executor.Limits.MemoryMB, executor.Limits.Threads, executor.Limits.MaxTempMB
	calls := 0
	ctx := WithOperationFileResolver(context.Background(), func(_ context.Context, got adapter.ProcessRequest, output io.Writer) (int64, error) {
		calls++
		if got.SourceFile == nil || *got.SourceFile != *input.SourceFile {
			t.Fatal("source snapshot lost descriptor")
		}
		_, err := output.Write(data)
		return time.Now().Unix() + 5, err
	})
	sink := &workerTestSink{}
	receipt, err := executor.ExecuteOperation(ctx, cfg, input, sink)
	if err != nil || calls != 1 || receipt.Outcome != operations.Completed || receipt.Result == nil || sink.rows != 3 {
		t.Fatal("contained immutable snapshot failed", receipt, err, calls)
	}
	if pool.Snapshot().Active != 0 || manager.Status().Active != 0 {
		t.Fatal("snapshot leaked custody")
	}
}

func TestContainedFileMutationPublishesOnlyVerifiedCandidate(t *testing.T) {
	for _, mode := range []string{"file_candidate", "file_conflict", "file_lost_reply", "file_bad_hash", "file_truncated", "file_crash"} {
		t.Run(mode, func(t *testing.T) {
			executor, manager, pool := containedExecutor(t)
			cfg := operationExecutable(t)
			input := operationProcessInput(t, operations.StatementExecute, mode)
			data := []byte("original-contents")
			sum := sha256.Sum256(data)
			input.Request.Connection.Schema = ""
			input.Source.Schema = ""
			input.RequestSHA256, _ = operations.Digest(input.Request)
			input.Source.Engine, input.Source.Password = "sqlite", ""
			input.SourceFile = &filesnapshot.Descriptor{Version: 1, Format: "sqlite", Bytes: int64(len(data)), SHA256: hex.EncodeToString(sum[:])}
			input.Source.Options = map[string]string{"file_format": "sqlite", "file_bytes": fmt.Sprint(len(data)), "file_sha256": input.SourceFile.SHA256}
			input.Limits.MemoryMB, input.Limits.Threads, input.Limits.MaxTempMB = executor.Limits.MemoryMB, executor.Limits.Threads, executor.Limits.MaxTempMB
			reads, writes := 0, 0
			ctx := WithOperationFileResolver(context.Background(), func(_ context.Context, got adapter.ProcessRequest, output io.Writer) (int64, error) {
				reads++
				if got.SourceFile == nil || *got.SourceFile != *input.SourceFile {
					t.Error("snapshot identity changed")
				}
				_, err := output.Write(data)
				return time.Now().Unix() + 5, err
			})
			ctx = WithOperationFilePublisher(ctx, func(_ context.Context, got adapter.ProcessRequest, candidate filesnapshot.Descriptor, source io.Reader) (bool, error) {
				writes++
				if pool.Snapshot().Active != 1 {
					t.Error("publication lost admission")
				}
				body, err := io.ReadAll(source)
				sum := sha256.Sum256(body)
				if err != nil || string(body) != "replacement-contents" || candidate.SHA256 != hex.EncodeToString(sum[:]) || got.OperationID != input.OperationID {
					t.Error("unverified candidate published")
				}
				if mode == "file_lost_reply" {
					return false, errors.New("reply lost")
				}
				return mode != "file_conflict", nil
			})
			receipt, err := executor.ExecuteOperation(ctx, cfg, input, nil)
			if reads != 1 {
				t.Fatal("source snapshot replayed", reads)
			}
			switch mode {
			case "file_candidate":
				if err != nil || receipt.Outcome != operations.Completed || receipt.Effect != operations.EffectCommitted || writes != 1 {
					t.Fatal(receipt, writes, err)
				}
			case "file_lost_reply":
				if err == nil || receipt.Outcome != operations.OutcomeUnknown || receipt.Effect != operations.EffectUnknown || writes != 1 {
					t.Fatal(receipt, writes, err)
				}
			case "file_conflict":
				if err == nil || receipt.Effect != operations.EffectNone || writes != 1 {
					t.Fatal(receipt, writes, err)
				}
			default:
				if err == nil || receipt.Effect != operations.EffectNone || writes != 0 {
					t.Fatal(receipt, writes, err)
				}
			}
			if pool.Snapshot().Active != 0 || manager.Status().Active != 0 {
				t.Fatal("file mutation leaked custody")
			}
		})
	}
}

func TestContainedOperationRejectsResolverPrivateDescriptor(t *testing.T) {
	executor, manager, pool := containedExecutor(t)
	cfg := operationExecutable(t)
	input := operationProcessInput(t, operations.StatementExecute, "fixture")
	input.PrivateTransport = &adapter.PrivateTransport{ControlFD: 7}
	receipt, err := executor.ExecuteResolvedOperation(context.Background(), cfg, input.OperationID, input.RequestSHA256, func(context.Context) (adapter.ProcessRequest, error) { return input, nil }, nil)
	if err == nil || receipt.Outcome != operations.Rejected || receipt.Effect != operations.EffectNone {
		t.Fatal("resolver supplied a private descriptor", receipt, err)
	}
	if pool.Snapshot().Active != 0 || manager.Status().Active != 0 {
		t.Fatal("private descriptor rejection leaked custody")
	}
}
