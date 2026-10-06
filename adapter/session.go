// Package adapter defines the optional connector boundary. It imports no
// database driver. Native analytical queries use Kelvo's Arrow sources directly.
package adapter

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"time"

	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/apache/arrow-go/v18/arrow"
)

// Connection is transient: never persist it in jobs, logs or receipts. Every
// Open gets a fresh authorized resolution; pooling across tenants is forbidden.
type Connection struct {
	TenantID     string
	ConnectionID string
	Revision     string
	Engine       string
	Host         string
	Port         int
	Namespace    string
	Schema       string
	Username     string
	Password     string
	Token        string
	Options      map[string]string
	// Endpoint is a driver-validated, credential-free provider address. It is
	// transient and may retain SRV/replica topology options for MongoDB.
	Endpoint string
	TLS      *tls.Config
}

type Driver interface {
	Capabilities() operations.Capabilities
	Open(context.Context, Connection) (Session, error)
}

type Session interface{ io.Closer }

type TestSession interface {
	Session
	Test(context.Context) error
}

type MetadataSession interface {
	Session
	Inspect(context.Context, operations.MetadataSpec, Limits, Sink) (QueryStats, error)
}

// Sink receives one schema, including empty results, then synchronous borrowed
// Arrow batches. Retaining a batch beyond Write requires Retain/Release. A sink
// error stops source iteration immediately; methods are never concurrent.
type Sink interface {
	Schema(*arrow.Schema) error
	Write(arrow.RecordBatch) error
}

type Query struct {
	Statement  string
	Parameters []operations.Parameter
	MaxRows    int64
	MaxBytes   int64
	BatchRows  int
}

type QueryStats struct {
	Rows int64 `json:"rows"`
	// Bytes counts conservative Arrow value-buffer bytes delivered. IPC
	// framing/schema overhead and source-wire bytes are measured separately.
	Bytes   int64         `json:"bytes"`
	Elapsed time.Duration `json:"elapsed"`
}

type QuerySession interface {
	Session
	Query(context.Context, Query, Sink) (QueryStats, error)
}

type Change struct {
	Statements  []string
	Parameters  [][]operations.Parameter
	Transaction bool
	Role        string
	Isolation   string
}

type ChangeResult struct {
	Outcome      string `json:"outcome"`
	Completed    int    `json:"completed"`
	Attempted    int    `json:"attempted"`
	AffectedRows *int64 `json:"affected_rows,omitempty"`
}

type ChangeSession interface {
	Session
	Execute(context.Context, Change) (ChangeResult, error)
}

var (
	ErrUnsupported = errors.New("adapter operation unsupported")
	ErrLimit       = errors.New("adapter result limit exceeded")
	ErrInvalid     = errors.New("invalid adapter request")
)

func (q Query) Validate() error {
	if len(q.Statement) == 0 || len(q.Statement) > 1<<20 || len(q.Parameters) > 1000 ||
		q.MaxRows < 1 || q.MaxRows > 1_000_000 || q.MaxBytes < 1 || q.MaxBytes > 64<<20 ||
		q.BatchRows < 1 || q.BatchRows > 4096 {
		return ErrInvalid
	}
	return nil
}
