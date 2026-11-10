//go:build duckdb_arrow

package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/adapters/go/connectors/files"
	"github.com/SYNEHQ/kelvo-go/filesnapshot"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/operations"
)

// A query.read through the real DuckDB file engine names the caller's bad
// column on the receipt.
func TestDuckDBBinderErrorReachesTheReceipt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "t.csv")
	raw := []byte("id\n1\n")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	r := runtimeRequest(t, operations.QueryRead)
	r.Request.Spec.Query.SQL = "SELECT nope FROM t"
	r.RequestSHA256, _ = operations.Digest(r.Request)
	receipt, err := Runner{Open: func(ctx context.Context, _ adapter.ConnectionSpec, _ operations.Request) (adapter.Session, error) {
		file, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		return files.Open(ctx, file, path, "t", filesnapshot.Descriptor{Version: 1, Format: "csv", Bytes: int64(len(raw)), SHA256: hex.EncodeToString(sum[:])},
			query.Limits{MaxRows: 100, MaxBytes: 1 << 20, Timeout: 5e9, MemoryMB: 256, Threads: 1, MaxTempMB: 64})
	}}.execute(context.Background(), r, io.Discard)
	if err == nil || receipt.ErrorCode != "SOURCE_FAILED" || receipt.Validate() != nil || receipt.SourceError == nil || !strings.Contains(receipt.SourceError.Message, "nope") {
		t.Fatalf("receipt = %+v source = %+v err = %v", receipt, receipt.SourceError, err)
	}
	t.Logf("receipt %s %s source_error=%+v", receipt.Outcome, receipt.ErrorCode, *receipt.SourceError)
}
