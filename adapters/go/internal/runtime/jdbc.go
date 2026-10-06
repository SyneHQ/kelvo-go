package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/internal/sources/sqlguard"
	"github.com/SYNEHQ/kelvo-go/operations"
)

// runJDBC does one operation in a fresh JVM. The parent owns/pins all runtime
// descriptors; credentials travel only over stdin and never through argv/env.
func runJDBC(parent context.Context, request adapter.ProcessRequest, raw []byte, output, receiptOutput io.Writer) error {
	reject := func(err error) error {
		receipt := operations.Receipt{Version: 1, OperationID: request.OperationID, RequestSHA256: request.RequestSHA256, Outcome: operations.Rejected, Effect: operations.EffectNone, ErrorCode: "UNSUPPORTED"}
		encoded, _ := json.Marshal(receipt)
		_, writeErr := receiptOutput.Write(encoded)
		return errors.Join(err, writeErr)
	}
	if request.Runtime == nil || request.Validate() != nil || adapter.JDBCCapabilities(request.Source.Engine).Supports(request.Request) != nil {
		return reject(adapter.ErrUnsupported)
	}
	if request.Request.Kind == operations.QueryRead {
		if _, err := sqlguard.ReadOnly(request.Request.Spec.Query.SQL); err != nil {
			return reject(err)
		}
	}
	deadline := time.Unix(request.ExpiresAt, 0)
	if bounded := time.Now().Add(time.Duration(request.Limits.TimeoutMS) * time.Millisecond); bounded.Before(deadline) {
		deadline = bounded
	}
	ctx, cancel := context.WithDeadline(parent, deadline)
	defer cancel()
	receiptRead, receiptWrite, err := os.Pipe()
	if err != nil {
		return err
	}
	defer receiptRead.Close()
	defer receiptWrite.Close()
	files := make([]*os.File, 5+len(request.Runtime.JARFDs))
	files[0] = receiptWrite
	java := os.NewFile(uintptr(request.Runtime.JavaFD), "pinned-java")
	defer java.Close()
	files[4] = java
	if st, err := java.Stat(); err != nil || !st.Mode().IsRegular() {
		return reject(adapter.ErrInvalid)
	}
	paths := make([]string, len(request.Runtime.JARFDs))
	for i, fd := range request.Runtime.JARFDs {
		file := os.NewFile(uintptr(fd), "pinned-jar")
		defer file.Close()
		if st, err := file.Stat(); err != nil || !st.Mode().IsRegular() {
			return reject(adapter.ErrInvalid)
		}
		files[5+i] = file
		paths[i] = "/proc/self/fd/" + strconv.Itoa(fd)
	}
	args := []string{"-Xms16m", "-Xmx" + strconv.Itoa(request.Runtime.HeapMB) + "m", "-XX:MaxDirectMemorySize=" + strconv.Itoa(request.Runtime.DirectMB) + "m", "-XX:+ExitOnOutOfMemoryError", "-XX:-HeapDumpOnOutOfMemoryError", "-XX:-CreateCoredumpOnCrash", "-XX:ErrorFile=/dev/null", "-XX:ActiveProcessorCount=1", "-XX:+UseSerialGC", "-XX:-UsePerfData", "--add-opens=java.base/java.nio=ALL-UNNAMED", "-Darrow.memory.allocation.manager.type=Unsafe", "-Duser.timezone=UTC", "-Djava.io.tmpdir=.", "-cp", strings.Join(paths, ":"), "com.synehq.kelvo.jdbc.Main"}
	cmd := exec.CommandContext(ctx, "/proc/self/fd/7", args...)
	cmd.Stdin = bytes.NewReader(raw)
	cmd.Stdout = output
	cmd.Stderr = io.Discard
	cmd.ExtraFiles = files
	cmd.Env = []string{"LANG=C.UTF-8", "TZ=UTC"}
	cmd.WaitDelay = time.Second
	if err := cmd.Start(); err != nil {
		return err
	}
	_ = receiptWrite.Close()
	type result struct {
		raw []byte
		err error
	}
	received := make(chan result, 1)
	go func() {
		raw, err := io.ReadAll(io.LimitReader(receiptRead, operations.MaxReceiptBytes+1))
		_ = receiptRead.Close()
		received <- result{raw, err}
	}()
	runErr := cmd.Wait()
	got := <-received
	var receipt operations.Receipt
	if got.err != nil || operations.DecodeStrict(got.raw, &receipt, operations.MaxReceiptBytes) != nil || receipt.OperationID != request.OperationID || operations.ValidateStatementReceipt(request.Request, receipt) != nil {
		return errors.New("invalid JDBC receipt")
	}
	n, err := receiptOutput.Write(got.raw)
	if err == nil && n != len(got.raw) {
		err = io.ErrShortWrite
	}
	return errors.Join(runErr, err)
}
