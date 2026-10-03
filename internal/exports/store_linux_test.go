//go:build linux

package exports

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"go.yaml.in/yaml/v3"
	"golang.org/x/sys/unix"
)

var testIdentity = Identity{Owner: "analyst-17", AuthorizationSHA256: strings.Repeat("a", 64)}

func testLimits() Limits {
	return Limits{MaxRows: 1000, MaxEncodedBytes: 4 << 20, MaxDecodedBytes: 4 << 20, MaxPartBytes: 1 << 20, MaxPartDecodedBytes: 1 << 20, MaxParts: 8}
}
func testConfig(root string) Config {
	return Config{Directory: root, Tenant: "tenant-a", MaxEntries: 16, MaxStoredBytes: 128 << 20, MaxTTL: time.Hour}
}
func testRequest() Request {
	return Request{Identity: testIdentity, ExpiresAt: time.Now().Add(10 * time.Minute), Limits: testLimits()}
}
func openTestStore(t *testing.T) *Store {
	t.Helper()
	root := filepath.Join(t.TempDir(), "exports")
	s, err := Open(testConfig(root))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s
}

func testRecord(t *testing.T) arrow.RecordBatch {
	t.Helper()
	meta := arrow.NewMetadata([]string{"unit"}, []string{"exact"})
	schema := arrow.NewSchema([]arrow.Field{{Name: "signed", Type: arrow.PrimitiveTypes.Int64, Nullable: true}, {Name: "unsigned", Type: arrow.PrimitiveTypes.Uint64, Nullable: true}, {Name: "amount", Type: &arrow.Decimal128Type{Precision: 18, Scale: 4}, Nullable: true}, {Name: "time", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true}, {Name: "label", Type: arrow.BinaryTypes.String, Nullable: true}}, &meta)
	b := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).AppendValues([]int64{-(1 << 63), 1<<63 - 1, 0}, []bool{true, true, false})
	b.Field(1).(*array.Uint64Builder).AppendValues([]uint64{0, ^uint64(0), 0}, []bool{true, true, false})
	d1, err := decimal128.FromString("-99999999999999.1234", 18, 4)
	if err != nil {
		t.Fatal(err)
	}
	d2, err := decimal128.FromString("99999999999999.1234", 18, 4)
	if err != nil {
		t.Fatal(err)
	}
	b.Field(2).(*array.Decimal128Builder).AppendValues([]decimal128.Num{d1, d2, {}}, []bool{true, true, false})
	b.Field(3).(*array.TimestampBuilder).AppendValues([]arrow.Timestamp{-1000003, 1000003, 0}, []bool{true, true, false})
	b.Field(4).(*array.StringBuilder).AppendValues([]string{"first", "世界", ""}, []bool{true, true, false})
	return b.NewRecordBatch()
}

func beginTest(t *testing.T, s *Store, record arrow.RecordBatch, request Request) *Writer {
	t.Helper()
	w, err := s.Begin(context.Background(), request, record.Schema())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := w.Close(); err != nil {
			t.Error(err)
		}
	})
	return w
}
func comparePart(t *testing.T, part *Part, want arrow.RecordBatch) {
	t.Helper()
	reader, err := ipc.NewReader(part)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Release()
	if !reader.Next() {
		t.Fatal(reader.Err())
	}
	got := reader.RecordBatch()
	if !got.Schema().Equal(want.Schema()) || !got.Schema().Metadata().Equal(want.Schema().Metadata()) || got.NumRows() != want.NumRows() {
		t.Fatal("schema or rows changed")
	}
	for i := 0; i < int(want.NumCols()); i++ {
		if !array.Equal(got.Column(i), want.Column(i)) {
			t.Fatal("value, null or type changed", i)
		}
	}
	if reader.Next() || reader.Err() != nil {
		t.Fatal("unexpected extra or invalid batch", reader.Err())
	}
}

func TestStoreExactPartsRestartAndEmpty(t *testing.T) {
	for _, codec := range []string{"none", "lz4_frame"} {
		t.Run(codec, func(t *testing.T) {
			s := openTestStore(t)
			record := testRecord(t)
			defer record.Release()
			request := testRequest()
			request.Limits.Compression = codec
			w := beginTest(t, s, record, request)
			for range 2 {
				if err := w.Write(context.Background(), record); err != nil {
					t.Fatal(err)
				}
			}
			manifest, err := w.Commit(context.Background(), testIdentity)
			if err != nil {
				t.Fatal(err)
			}
			if len(manifest.Parts) != 2 || manifest.Rows != 6 {
				t.Fatal("totals")
			}
			if err = s.Close(); err != nil {
				t.Fatal(err)
			}
			s2, err := Open(s.config)
			if err != nil {
				t.Fatal(err)
			}
			defer s2.Close()
			r, err := s2.Acquire(context.Background(), manifest.ID, testIdentity)
			if err != nil {
				t.Fatal(err)
			}
			for index := range manifest.Parts {
				part, err := r.OpenPart(context.Background(), index, testIdentity)
				if err != nil {
					t.Fatal(err)
				}
				comparePart(t, part, record)
				part.Close()
			}
			r.Close()
			empty := beginTest(t, s2, record, testRequest())
			m, err := empty.Commit(context.Background(), testIdentity)
			if err != nil {
				t.Fatal(err)
			}
			if m.Rows != 0 || len(m.Parts) != 1 || m.Parts[0].Batches != 0 {
				t.Fatal("empty schema missing")
			}
			r, err = s2.Acquire(context.Background(), m.ID, testIdentity)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			part, err := r.OpenPart(context.Background(), 0, testIdentity)
			if err != nil {
				t.Fatal(err)
			}
			defer part.Close()
			reader, err := ipc.NewReader(part)
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Release()
			if !reader.Schema().Equal(record.Schema()) || reader.Next() || reader.Err() != nil {
				t.Fatal("empty schema changed")
			}
		})
	}
}

func TestOwnerAuthorizationFenceAndIndependentPartLeases(t *testing.T) {
	s := openTestStore(t)
	record := testRecord(t)
	defer record.Release()
	w := beginTest(t, s, record, testRequest())
	if err := w.Write(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	wrong := testIdentity
	wrong.AuthorizationSHA256 = strings.Repeat("b", 64)
	if _, err := w.Commit(context.Background(), wrong); !errors.Is(err, ErrFenced) {
		t.Fatal(err)
	}
	m, err := w.Commit(context.Background(), testIdentity)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []Identity{wrong, {Owner: "other", AuthorizationSHA256: testIdentity.AuthorizationSHA256}} {
		if r, err := s.Acquire(context.Background(), m.ID, id); err == nil {
			r.Close()
			t.Fatal("cross-owner/auth read")
		}
	}
	r, err := s.Acquire(context.Background(), m.ID, testIdentity)
	if err != nil {
		t.Fatal(err)
	}
	part, err := r.OpenPart(context.Background(), 0, testIdentity)
	if err != nil {
		t.Fatal(err)
	}
	r.Close()
	if err = s.Cancel(context.Background(), m.ID, testIdentity); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Acquire(context.Background(), m.ID, testIdentity); !errors.Is(err, ErrUnavailable) {
		t.Fatal(err)
	}
	clean, err := s.Cleanup(context.Background(), 16)
	if err != nil || clean.Removed != 0 || clean.Busy != 1 {
		t.Fatal(clean, err)
	}
	if err = s.Close(); !errors.Is(err, ErrBusy) {
		t.Fatal("part did not own independent store lease", err)
	}
	comparePart(t, part, record)
	part.Close()
	clean, err = s.Cleanup(context.Background(), 16)
	if err != nil || clean.Removed != 1 {
		t.Fatal(clean, err)
	}
	if _, err = os.Stat(filepath.Join(s.config.Directory, m.ID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("cancelled entry remains")
	}
}

func TestLimitsReservationsAndMultipleStoreHandles(t *testing.T) {
	record := testRecord(t)
	defer record.Release()
	for _, kind := range []string{"rows", "decoded", "encoded", "parts"} {
		t.Run(kind, func(t *testing.T) {
			s := openTestStore(t)
			request := testRequest()
			switch kind {
			case "rows":
				request.Limits.MaxRows = 2
			case "decoded":
				request.Limits.MaxPartDecodedBytes = 1
				request.Limits.MaxDecodedBytes = 1
			case "encoded":
				request.Limits.MaxPartBytes = 16
				request.Limits.MaxEncodedBytes = 16
			case "parts":
				request.Limits.MaxParts = 1
			}
			w := beginTest(t, s, record, request)
			err := w.Write(context.Background(), record)
			if kind == "parts" && err == nil {
				err = w.Write(context.Background(), record)
			}
			if err == nil {
				t.Fatal("limit ignored")
			}
			if _, err = w.Commit(context.Background(), testIdentity); err == nil {
				t.Fatal("failed fill committed")
			}
		})
	}
	root := filepath.Join(t.TempDir(), "root")
	cfg := testConfig(root)
	cfg.MaxStoredBytes = testLimits().MaxEncodedBytes + metadataReservation + stateLimit
	s1, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s1.Close()
	s2, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	start := make(chan struct{})
	var wg sync.WaitGroup
	writers := make(chan *Writer, 2)
	errs := make(chan error, 2)
	for _, s := range []*Store{s1, s2} {
		wg.Go(func() {
			<-start
			w, e := s.Begin(context.Background(), testRequest(), record.Schema())
			writers <- w
			errs <- e
		})
	}
	close(start)
	wg.Wait()
	close(writers)
	close(errs)
	var success, limited int
	for e := range errs {
		if e == nil {
			success++
		} else if errors.Is(e, ErrLimit) {
			limited++
		} else {
			t.Fatal(e)
		}
	}
	for w := range writers {
		if w != nil {
			w.Close()
		}
	}
	if success != 1 || limited != 1 {
		t.Fatal(success, limited)
	}
	if _, err = s1.Begin(context.Background(), testRequest(), record.Schema()); !errors.Is(err, ErrLimit) {
		t.Fatal("cancelled reservation released before cleanup", err)
	}
	if _, err = s1.Cleanup(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	w, err := s1.Begin(context.Background(), testRequest(), record.Schema())
	if err != nil {
		t.Fatal(err)
	}
	w.Close()
}

func TestCancellationSerializesWithPublication(t *testing.T) {
	s := openTestStore(t)
	record := testRecord(t)
	defer record.Release()
	for range 8 {
		w := beginTest(t, s, record, testRequest())
		if err := w.Write(context.Background(), record); err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		var wg sync.WaitGroup
		var commitErr, cancelErr error
		wg.Go(func() { <-start; _, commitErr = w.Commit(context.Background(), testIdentity) })
		wg.Go(func() { <-start; cancelErr = s.Cancel(context.Background(), w.ID(), testIdentity) })
		close(start)
		wg.Wait()
		if cancelErr != nil {
			t.Fatal(cancelErr)
		}
		if commitErr != nil && !errors.Is(commitErr, ErrFenced) {
			t.Fatal(commitErr)
		}
		w.Close()
		if r, err := s.Acquire(context.Background(), w.ID(), testIdentity); err == nil {
			r.Close()
			t.Fatal("cancelled fill readable")
		}
		if _, err := s.Cleanup(context.Background(), 16); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPrivateStorageAndUnknownEntriesFailClosed(t *testing.T) {
	record := testRecord(t)
	defer record.Release()
	for _, kind := range []string{"payload-writable", "payload-symlink", "payload-hardlink", "manifest-unknown", "state-unknown", "unowned-file"} {
		t.Run(kind, func(t *testing.T) {
			s := openTestStore(t)
			w := beginTest(t, s, record, testRequest())
			if err := w.Write(context.Background(), record); err != nil {
				t.Fatal(err)
			}
			m, err := w.Commit(context.Background(), testIdentity)
			if err != nil {
				t.Fatal(err)
			}
			dir := filepath.Join(s.config.Directory, m.ID)
			path := filepath.Join(dir, partName(0))
			switch kind {
			case "payload-writable":
				os.Chmod(path, 0600)
			case "payload-symlink":
				os.Rename(path, path+".saved")
				os.Symlink(path+".saved", path)
			case "payload-hardlink":
				os.Link(path, path+".extra")
			case "manifest-unknown":
				path = filepath.Join(dir, "manifest.yml")
				os.Chmod(path, 0600)
				f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
				f.WriteString("unknown: value\n")
				f.Close()
				os.Chmod(path, 0400)
			case "state-unknown":
				path = filepath.Join(dir, "state.yml")
				f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
				f.WriteString("unknown: value\n")
				f.Close()
			case "unowned-file":
				os.WriteFile(filepath.Join(dir, "important.txt"), []byte("preserve"), 0600)
			}
			if kind == "unowned-file" {
				s.Cancel(context.Background(), m.ID, testIdentity)
				if _, err = s.Cleanup(context.Background(), 16); err == nil {
					t.Fatal("unknown data deleted")
				}
				if _, err = os.Stat(filepath.Join(dir, "important.txt")); err != nil {
					t.Fatal(err)
				}
				return
			}
			r, err := s.Acquire(context.Background(), m.ID, testIdentity)
			if err == nil {
				defer r.Close()
				p, e := r.OpenPart(context.Background(), 0, testIdentity)
				if e == nil {
					p.Close()
					t.Fatal("unsafe payload accepted")
				}
			}
		})
	}
	root := t.TempDir()
	os.Mkdir(filepath.Join(root, "real"), 0700)
	os.Symlink(filepath.Join(root, "real"), filepath.Join(root, "link"))
	if _, err := Open(testConfig(filepath.Join(root, "link", "store"))); err == nil {
		t.Fatal("symlink ancestor accepted")
	}
}

func TestCrashHelper(t *testing.T) {
	if os.Getenv("KELVO_EXPORT_CRASH_HELPER") != "1" {
		return
	}
	s, err := Open(testConfig(os.Getenv("KELVO_EXPORT_TEST_ROOT")))
	if err != nil {
		panic(err)
	}
	record := testRecord(t)
	w, err := s.Begin(context.Background(), testRequest(), record.Schema())
	if err != nil {
		panic(err)
	}
	if err = w.Write(context.Background(), record); err != nil {
		panic(err)
	}
	fmt.Print(w.ID())
	os.Exit(0)
}
func TestCrashReservationsAndInterruptedCleanupRecovery(t *testing.T) {
	root := filepath.Join(t.TempDir(), "root")
	cmd := exec.Command(os.Args[0], "-test.run=^TestCrashHelper$")
	cmd.Env = append(os.Environ(), "KELVO_EXPORT_CRASH_HELPER=1", "KELVO_EXPORT_TEST_ROOT="+root)
	out, err := cmd.Output()
	if err != nil {
		t.Fatal(err)
	}
	id := string(out)
	if !idPattern.MatchString(id) {
		t.Fatal("invalid helper id")
	}
	s, err := Open(testConfig(root))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ids, reserved, err := s.scan()
	if err != nil || len(ids) != 1 || reserved != stateLimit+testLimits().MaxEncodedBytes+metadataReservation {
		t.Fatal(ids, reserved, err)
	}
	if err = s.Cancel(context.Background(), id, testIdentity); err != nil {
		t.Fatal(err)
	}
	dir, err := childDir(s.root, id, false)
	if err != nil {
		t.Fatal(err)
	}
	st, err := s.loadState(dir, id)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := yaml.Marshal(st)
	if _, err = atomicWrite(s.root, id+".deleting.yml", raw, true, true); err != nil {
		t.Fatal(err)
	}
	// Simulate a crash after deletion started and the state file disappeared.
	for _, name := range []string{"schema.arrow", "state.yml"} {
		if err = unix.Unlinkat(int(dir.Fd()), name, 0); err != nil {
			t.Fatal(err)
		}
	}
	dir.Sync()
	dir.Close()
	result, err := s.Cleanup(context.Background(), 1)
	if err != nil || result.Removed != 1 {
		t.Fatal(result, err)
	}
	names, err := directoryNames(s.root, 4)
	if err != nil || len(names) != 2 {
		t.Fatal(names, err)
	}
}

func TestTamperedPayloadAndCancelledDownload(t *testing.T) {
	s := openTestStore(t)
	record := testRecord(t)
	defer record.Release()
	w := beginTest(t, s, record, testRequest())
	w.Write(context.Background(), record)
	m, err := w.Commit(context.Background(), testIdentity)
	if err != nil {
		t.Fatal(err)
	}
	r, err := s.Acquire(context.Background(), m.ID, testIdentity)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	ctx, cancel := context.WithCancel(context.Background())
	p, err := r.OpenPart(ctx, 0, testIdentity)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if _, err = p.Read(make([]byte, 1)); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	p.Close()
	path := filepath.Join(s.config.Directory, m.ID, partName(0))
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)/2] ^= 1
	os.Chmod(path, 0600)
	os.WriteFile(path, raw, 0400)
	os.Chmod(path, 0400)
	if p, err = r.OpenPart(context.Background(), 0, testIdentity); err == nil {
		p.Close()
		t.Fatal("corrupt bytes accepted")
	}
}

func TestExpiryBoundsOpenAndAlreadyAdmittedPart(t *testing.T) {
	s := openTestStore(t)
	record := testRecord(t)
	defer record.Release()
	request := testRequest()
	request.ExpiresAt = time.Now().Add(time.Second)
	w := beginTest(t, s, record, request)
	if err := w.Write(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	m, err := w.Commit(context.Background(), testIdentity)
	if err != nil {
		t.Fatal(err)
	}
	r, err := s.Acquire(context.Background(), m.ID, testIdentity)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	part, err := r.OpenPart(context.Background(), 0, testIdentity)
	if err != nil {
		t.Fatal(err)
	}
	defer part.Close()
	time.Sleep(time.Until(request.ExpiresAt) + time.Millisecond)
	if _, err = part.Read(make([]byte, 1)); !errors.Is(err, ErrUnavailable) {
		t.Fatal(err)
	}
	if _, err = r.OpenPart(context.Background(), 0, testIdentity); !errors.Is(err, ErrUnavailable) {
		t.Fatal(err)
	}
}

type processFixture struct {
	command *exec.Cmd
	input   io.WriteCloser
	line    <-chan processFixtureLine
	done    <-chan error
}

type processFixtureLine struct {
	text string
	err  error
}

func startProcessFixture(t *testing.T, root, mode string, extra ...string) *processFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestExportProcessHelper$")
	cmd.Env = append(os.Environ(), "KELVO_EXPORT_HELPER_MODE="+mode, "KELVO_EXPORT_TEST_ROOT="+root)
	cmd.Env = append(cmd.Env, extra...)
	input, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	output, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { input.Close(); cancel() })
	if mode == "fast-exit" {
		// Force the regression's child to exit before its stdout reader starts.
		// WNOWAIT leaves reaping and pipe ownership with exec.Cmd.Wait.
		var info unix.Siginfo
		for {
			err = unix.Waitid(unix.P_PID, cmd.Process.Pid, &info, unix.WEXITED|unix.WNOWAIT, nil)
			if !errors.Is(err, unix.EINTR) {
				break
			}
		}
		if err != nil {
			cancel()
			_ = output.Close()
			_ = cmd.Wait()
			t.Fatal(err)
		}
	}
	lines := make(chan processFixtureLine, 1)
	done := make(chan error, 1)
	go func() {
		reader := bufio.NewReader(output)
		line, readErr := reader.ReadString('\n')
		lines <- processFixtureLine{strings.TrimSpace(line), readErr}
		// Wait closes StdoutPipe. Drain it first, including the test runner's
		// output after the readiness line, so fast exits cannot lose that line.
		_, drainErr := io.Copy(io.Discard, reader)
		done <- errors.Join(drainErr, cmd.Wait())
	}()
	return &processFixture{cmd, input, lines, done}
}
func fixtureLine(t *testing.T, f *processFixture) string {
	t.Helper()
	select {
	case line := <-f.line:
		if line.err != nil {
			t.Fatalf("subprocess readiness read: %v (partial line %q)", line.err, line.text)
		}
		return line.text
	case <-time.After(10 * time.Second):
		t.Fatal("subprocess fixture deadline")
		return ""
	}
}
func fixtureWait(t *testing.T, f *processFixture) {
	t.Helper()
	f.input.Close()
	select {
	case err := <-f.done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("subprocess exit deadline")
	}
}

func TestExportProcessHelper(t *testing.T) {
	mode := os.Getenv("KELVO_EXPORT_HELPER_MODE")
	if mode == "" {
		return
	}
	if mode == "fast-exit" {
		fmt.Println("ready")
		return
	}
	cfg := testConfig(os.Getenv("KELVO_EXPORT_TEST_ROOT"))
	if budget := os.Getenv("KELVO_EXPORT_TEST_BUDGET"); budget != "" {
		n, err := strconv.ParseInt(budget, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		cfg.MaxStoredBytes = n
	}
	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if reservationProcess(t, s, mode) {
		return
	}
	record := testRecord(t)
	defer record.Release()
	if mode == "read" {
		r, err := s.Acquire(context.Background(), os.Getenv("KELVO_EXPORT_TEST_ID"), testIdentity)
		if err != nil {
			t.Fatal(err)
		}
		p, err := r.OpenPart(context.Background(), 0, testIdentity)
		if err != nil {
			t.Fatal(err)
		}
		r.Close()
		fmt.Println("leased")
		io.ReadAll(os.Stdin)
		comparePart(t, p, record)
		p.Close()
		return
	}
	if strings.HasPrefix(mode, "crash-begin:") {
		point := strings.TrimPrefix(mode, "crash-begin:")
		s.fault = func(stage string) error {
			if stage == point || (point == "initializing:file_sync" && strings.HasSuffix(stage, ".initializing.yml:file_sync")) {
				os.Exit(0)
			}
			return nil
		}
		if _, err = s.Begin(context.Background(), testRequest(), record.Schema()); err != nil {
			t.Fatal(err)
		}
		t.Fatal("crash point not reached")
	}
	request := testRequest()
	if mode == "lz4-limit" {
		request.Limits.Compression = "lz4_frame"
	}
	w, err := s.Begin(context.Background(), request, record.Schema())
	if errors.Is(err, ErrLimit) && mode == "admit" {
		fmt.Println("limited")
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if mode == "admit" {
		fmt.Println("admitted")
		io.ReadAll(os.Stdin)
		return
	}
	if mode == "lz4-limit" {
		s.encoderAllocationLimit = 1
		if err = w.Write(context.Background(), record); err == nil {
			t.Fatal("allocator limit ignored")
		}
		if _, err = w.Commit(context.Background(), testIdentity); err == nil {
			t.Fatal("failed writer committed")
		}
		fmt.Println("rejected-without-process-panic")
		return
	}
	if err = w.Write(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	if mode == "crash-state-slot" {
		s.fault = func(stage string) error {
			if stage == "state.yml:staged" {
				os.Exit(0)
			}
			return nil
		}
		w.Commit(context.Background(), testIdentity)
		t.Fatal("state crash point not reached")
	}
	t.Fatal("unknown subprocess mode")
}

func TestProcessFixtureReadsFastExit(t *testing.T) {
	fixture := startProcessFixture(t, t.TempDir(), "fast-exit")
	if line := fixtureLine(t, fixture); line != "ready" {
		t.Fatalf("subprocess readiness = %q, want ready", line)
	}
	fixtureWait(t, fixture)
}

func TestActualProcessReservationsAndReaderCleanup(t *testing.T) {
	root := filepath.Join(t.TempDir(), "admission")
	cfg := testConfig(root)
	cfg.MaxStoredBytes = testLimits().MaxEncodedBytes + metadataReservation + stateLimit
	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	budget := "KELVO_EXPORT_TEST_BUDGET=" + strconv.FormatInt(cfg.MaxStoredBytes, 10)
	one := startProcessFixture(t, root, "admit", budget)
	two := startProcessFixture(t, root, "admit", budget)
	a, b := fixtureLine(t, one), fixtureLine(t, two)
	if !((a == "admitted" && b == "limited") || (b == "admitted" && a == "limited")) {
		t.Fatal(a, b)
	}
	fixtureWait(t, one)
	fixtureWait(t, two)
	if _, err = s.Cleanup(context.Background(), 16); err != nil {
		t.Fatal(err)
	}
	other := openTestStore(t)
	record := testRecord(t)
	defer record.Release()
	w := beginTest(t, other, record, testRequest())
	w.Write(context.Background(), record)
	m, err := w.Commit(context.Background(), testIdentity)
	if err != nil {
		t.Fatal(err)
	}
	reader := startProcessFixture(t, other.config.Directory, "read", "KELVO_EXPORT_TEST_ID="+m.ID)
	if line := fixtureLine(t, reader); line != "leased" {
		t.Fatal(line)
	}
	if err = other.Cancel(context.Background(), m.ID, testIdentity); err != nil {
		t.Fatal(err)
	}
	clean, err := other.Cleanup(context.Background(), 16)
	if err != nil || clean.Busy != 1 || clean.Removed != 0 {
		t.Fatal(clean, err)
	}
	fixtureWait(t, reader)
	clean, err = other.Cleanup(context.Background(), 16)
	if err != nil || clean.Removed != 1 {
		t.Fatal(clean, err)
	}
}

func TestBeginCrashCutpointsRemainRecoverableAndCharged(t *testing.T) {
	for _, point := range []string{"begin:after_intent", "begin:after_mkdir", "begin:after_lease", "begin:after_state", "begin:after_schema"} {
		t.Run(point, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "root")
			fixture := startProcessFixture(t, root, "crash-begin:"+point)
			fixtureWait(t, fixture)
			s, err := Open(testConfig(root))
			if err != nil {
				t.Fatal("initialization crash poisoned root", err)
			}
			defer s.Close()
			ids, used, err := s.scan()
			if err != nil || len(ids) != 1 || used <= stateLimit {
				t.Fatal(ids, used, err)
			}
			if point == "begin:after_schema" {
				// Begin now reserves durably before binding. A death during
				// binding retains that reservation until cancel or expiry.
				clean, err := s.Cleanup(context.Background(), 16)
				if err != nil || clean.Removed != 0 {
					t.Fatal("durable reservation reclaimed early", clean, err)
				}
				if err = s.Cancel(context.Background(), ids[0], testIdentity); err != nil {
					t.Fatal(err)
				}
			}
			clean, err := s.Cleanup(context.Background(), 16)
			if err != nil || clean.Removed != 1 {
				t.Fatal(clean, err)
			}
			ids, used, err = s.scan()
			if err != nil || len(ids) != 0 || used != stateLimit {
				t.Fatal(ids, used, err)
			}
		})
	}
}

func TestNamedStateSlotCrashAndMalformedSlotPreservation(t *testing.T) {
	root := filepath.Join(t.TempDir(), "root")
	fixture := startProcessFixture(t, root, "crash-state-slot")
	fixtureWait(t, fixture)
	s, err := Open(testConfig(root))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ids, _, err := s.scan()
	if err != nil || len(ids) != 1 {
		t.Fatal(ids, err)
	}
	if _, err = os.Stat(filepath.Join(root, ids[0], ".state.next.yml")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("valid uncommitted slot not recovered", err)
	}
	if _, err = s.Acquire(context.Background(), ids[0], testIdentity); !errors.Is(err, ErrUnavailable) {
		t.Fatal("unpublished state became available", err)
	}
	path := filepath.Join(root, ids[0], ".state.next.yml")
	os.WriteFile(path, []byte("unowned: preserve\n"), 0600)
	if _, _, err = s.scan(); err == nil {
		t.Fatal("malformed slot accepted")
	}
	if _, err = os.Stat(path); err != nil {
		t.Fatal("malformed slot deleted", err)
	}
}

func TestMetadataSyncFailuresAndPublicationUncertainty(t *testing.T) {
	record := testRecord(t)
	defer record.Release()
	for _, point := range []string{"manifest.yml:file_sync", "manifest.yml:dir_sync", "state.yml:file_sync", "state.yml:dir_sync"} {
		t.Run(point, func(t *testing.T) {
			s := openTestStore(t)
			w := beginTest(t, s, record, testRequest())
			if err := w.Write(context.Background(), record); err != nil {
				t.Fatal(err)
			}
			injected := errors.New("injected filesystem sync failure")
			s.fault = func(stage string) error {
				if stage == point {
					return injected
				}
				return nil
			}
			m, err := w.Commit(context.Background(), testIdentity)
			s.fault = nil
			if err == nil {
				t.Fatal("sync failure claimed success")
			}
			if point == "state.yml:dir_sync" {
				if !errors.Is(err, ErrPublicationUncertain) || m.ID == "" {
					t.Fatal("published state not identified as uncertain", m, err)
				}
				r, e := s.Acquire(context.Background(), m.ID, testIdentity)
				if e != nil {
					t.Fatal(e)
				}
				r.Close()
				if e = w.Close(); e != nil {
					t.Fatal(e)
				}
				r, e = s.Acquire(context.Background(), m.ID, testIdentity)
				if e != nil {
					t.Fatal("close revoked uncertain publication", e)
				}
				r.Close()
			} else {
				if m.ID != "" {
					t.Fatal("pre-publication failure returned committed manifest")
				}
				if r, e := s.Acquire(context.Background(), w.ID(), testIdentity); e == nil {
					r.Close()
					t.Fatal("failed fill became available")
				}
			}
		})
	}
}

func TestLZ4EncoderAllocatorLimitIsProcessSafe(t *testing.T) {
	fixture := startProcessFixture(t, filepath.Join(t.TempDir(), "root"), "lz4-limit")
	if line := fixtureLine(t, fixture); line != "rejected-without-process-panic" {
		t.Fatal(line)
	}
	fixtureWait(t, fixture)
}

func TestWritableAncestorRejectedBeforeCreatingStore(t *testing.T) {
	parent := filepath.Join(t.TempDir(), "writable")
	if err := os.Mkdir(parent, 0777); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(parent, 0777); err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(parent, "exports")
	if _, err := Open(testConfig(root)); err == nil {
		t.Fatal("writable ancestor accepted")
	}
	if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("created through untrusted ancestor")
	}
}

var _ io.ReadCloser = (*Part)(nil)

func TestForgedManifestCountsWithMatchingDigestAreRejected(t *testing.T) {
	record := testRecord(t)
	defer record.Release()
	for _, kind := range []string{"rows", "decoded", "encoded", "batches"} {
		t.Run(kind, func(t *testing.T) {
			s := openTestStore(t)
			w := beginTest(t, s, record, testRequest())
			if err := w.Write(context.Background(), record); err != nil {
				t.Fatal(err)
			}
			m, err := w.Commit(context.Background(), testIdentity)
			if err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "rows":
				m.Rows--
				m.Parts[0].Rows--
			case "decoded":
				m.DecodedBytes--
				m.Parts[0].DecodedBytes--
			case "encoded":
				m.EncodedBytes--
				m.Parts[0].EncodedBytes--
			case "batches":
				m.Rows, m.DecodedBytes, m.Parts[0].Rows, m.Parts[0].DecodedBytes, m.Parts[0].Batches = 0, 0, 0, 0, 0
			}
			raw, err := yaml.Marshal(m)
			if err != nil {
				t.Fatal(err)
			}
			dir, err := childDir(s.root, m.ID, false)
			if err != nil {
				t.Fatal(err)
			}
			defer dir.Close()
			path := filepath.Join(dir.Name(), "manifest.yml")
			if err = os.Chmod(path, 0600); err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(path, raw, 0600); err != nil {
				t.Fatal(err)
			}
			if err = os.Chmod(path, 0400); err != nil {
				t.Fatal(err)
			}
			st, err := s.loadState(dir, m.ID)
			if err != nil {
				t.Fatal(err)
			}
			st.ManifestSHA256 = checksum(raw)
			if _, err = s.writeState(dir, st); err != nil {
				t.Fatal(err)
			}
			r, err := s.Acquire(context.Background(), m.ID, testIdentity)
			if err != nil {
				t.Fatal("test must reach payload verification", err)
			}
			defer r.Close()
			if p, err := r.OpenPart(context.Background(), 0, testIdentity); !errors.Is(err, ErrCorrupt) {
				if p != nil {
					p.Close()
				}
				t.Fatal("forged count accepted", err)
			}
		})
	}
}

func TestCancellationAndExpiryDuringPartVerification(t *testing.T) {
	record := testRecord(t)
	defer record.Release()
	for _, kind := range []string{"context", "cancel", "expiry"} {
		t.Run(kind, func(t *testing.T) {
			s := openTestStore(t)
			request := testRequest()
			if kind == "expiry" {
				request.ExpiresAt = time.Now().Add(time.Second)
			}
			w := beginTest(t, s, record, request)
			if err := w.Write(context.Background(), record); err != nil {
				t.Fatal(err)
			}
			m, err := w.Commit(context.Background(), testIdentity)
			if err != nil {
				t.Fatal(err)
			}
			r, err := s.Acquire(context.Background(), m.ID, testIdentity)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s.fault = func(stage string) error {
				if stage != "part:verified" {
					return nil
				}
				switch kind {
				case "context":
					cancel()
				case "cancel":
					return s.Cancel(context.Background(), m.ID, testIdentity)
				case "expiry":
					time.Sleep(time.Until(request.ExpiresAt) + time.Millisecond)
				}
				return nil
			}
			part, err := r.OpenPart(ctx, 0, testIdentity)
			s.fault = nil
			if part != nil {
				part.Close()
				t.Fatal("invalidated verification returned bytes")
			}
			want := ErrUnavailable
			if kind == "context" {
				want = context.Canceled
			}
			if !errors.Is(err, want) {
				t.Fatal(err)
			}
		})
	}
}

func TestAnonymousInitializationCrashLeavesNoRootDebris(t *testing.T) {
	root := filepath.Join(t.TempDir(), "root")
	fixture := startProcessFixture(t, root, "crash-begin:initializing:file_sync")
	fixtureWait(t, fixture)
	s, err := Open(testConfig(root))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ids, used, err := s.scan()
	if err != nil || len(ids) != 0 || used != stateLimit {
		t.Fatal(ids, used, err)
	}
}

func TestDeletionMarkerSyncFailuresPreserveCapacityAndRecover(t *testing.T) {
	record := testRecord(t)
	defer record.Release()
	for _, phase := range []string{"file_sync", "dir_sync"} {
		t.Run(phase, func(t *testing.T) {
			s := openTestStore(t)
			w := beginTest(t, s, record, testRequest())
			if err := w.Write(context.Background(), record); err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			injected := errors.New("injected cleanup sync failure")
			s.fault = func(stage string) error {
				if stage == w.ID()+".deleting.yml:"+phase {
					return injected
				}
				return nil
			}
			result, err := s.Cleanup(context.Background(), 1)
			s.fault = nil
			if !errors.Is(err, injected) || result.Removed != 0 {
				t.Fatal(result, err)
			}
			if _, err = os.Stat(filepath.Join(s.config.Directory, w.ID(), partName(0))); err != nil {
				t.Fatal("cleanup removed bytes after failed sync", err)
			}
			ids, used, err := s.scan()
			if err != nil || len(ids) != 1 || used != stateLimit+testLimits().MaxEncodedBytes+metadataReservation {
				t.Fatal(ids, used, err)
			}
			result, err = s.Cleanup(context.Background(), 1)
			if err != nil || result.Removed != 1 {
				t.Fatal(result, err)
			}
		})
	}
}

func TestInitializationFinalSyncFailureRemainsChargedUntilCancelled(t *testing.T) {
	s := openTestStore(t)
	record := testRecord(t)
	defer record.Release()
	injected := errors.New("injected final initialization sync failure")
	s.fault = func(stage string) error {
		if stage == "begin:root_sync" {
			return injected
		}
		return nil
	}
	writer, err := s.Begin(context.Background(), testRequest(), record.Schema())
	s.fault = nil
	if !errors.Is(err, injected) || writer != nil {
		t.Fatal(writer, err)
	}
	ids, used, err := s.scan()
	if err != nil || len(ids) != 1 || used != stateLimit+testLimits().MaxEncodedBytes+metadataReservation {
		t.Fatal(ids, used, err)
	}
	if _, err = s.Acquire(context.Background(), ids[0], testIdentity); !errors.Is(err, ErrUnavailable) {
		t.Fatal(err)
	}
	result, err := s.Cleanup(context.Background(), 16)
	if err != nil || result.Removed != 0 {
		t.Fatal("unexpired reservation silently reclaimed", result, err)
	}
	if err = s.Cancel(context.Background(), ids[0], testIdentity); err != nil {
		t.Fatal(err)
	}
	result, err = s.Cleanup(context.Background(), 16)
	if err != nil || result.Removed != 1 {
		t.Fatal(result, err)
	}
}

func TestStrictYAMLRejectsDeepMetadata(t *testing.T) {
	var value any
	if err := strictYAML([]byte("normal: [one, two]\n"), &value); err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{strings.Repeat("[", 18) + "0" + strings.Repeat("]", 18), "value: &alias [one]\nother: *alias\n", "one: 1\n---\ntwo: 2\n"} {
		if err := strictYAML([]byte(raw), &value); !errors.Is(err, ErrCorrupt) {
			t.Fatal("unsafe metadata accepted", err)
		}
	}
}
