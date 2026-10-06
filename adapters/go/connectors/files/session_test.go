package files

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/filesnapshot"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
)

func testLimits() query.Limits {
	return query.Limits{MaxRows: 100, MaxBytes: 1 << 20, Timeout: 3 * time.Second, MemoryMB: 64, Threads: 1, MaxTempMB: 16}
}
func openFixture(t *testing.T, path, format string) *Session {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(raw)
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	session, err := Open(context.Background(), file, path, "data", filesnapshot.Descriptor{Version: 1, Format: format, Bytes: int64(len(raw)), SHA256: hex.EncodeToString(hash[:])}, testLimits())
	if err != nil {
		file.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { session.Close() })
	return session
}

type capture struct {
	schema *arrow.Schema
	rows   [][]any
	fail   error
}

func (s *capture) Schema(schema *arrow.Schema) error { s.schema = schema; return s.fail }
func (s *capture) Write(batch arrow.RecordBatch) error {
	if s.fail != nil {
		return s.fail
	}
	for row := 0; row < int(batch.NumRows()); row++ {
		values := make([]any, batch.NumCols())
		for i, col := range batch.Columns() {
			values[i] = col.GetOneForMarshal(row)
			switch value := values[i].(type) {
			case string:
				values[i] = strings.Clone(value)
			case []byte:
				values[i] = append([]byte{}, value...)
			}
		}
		s.rows = append(s.rows, values)
	}
	return nil
}
func testQuery(sql string) adapter.Query {
	return adapter.Query{Statement: sql, MaxRows: 100, MaxBytes: 1 << 20, BatchRows: 2}
}
func TestSourceDescriptorRejectsChangedBytes(t *testing.T) {
	path := t.TempDir() + "/source.csv"
	if err := os.WriteFile(path, []byte("id\n1\n"), 0600); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	d := filesnapshot.Descriptor{Version: 1, Format: "csv", Bytes: 5, SHA256: hex.EncodeToString(make([]byte, 32))}
	if _, err = Open(context.Background(), file, path, "data", d, testLimits()); !errors.Is(err, adapter.ErrInvalid) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = Open(ctx, file, path, "data", d, testLimits()); err == nil {
		t.Fatal("cancelled snapshot opened")
	}
}
