// Package cdc captures ordered source changes and publishes them through a
// Sink with a durable Checkpoint. The source position is acknowledged only
// after the sink applied a transaction and its checkpoint was saved, so a
// crash at any point replays from the checkpoint; sinks must apply replayed
// transactions idempotently. See docs/design/postgres-cdc.md.
package cdc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

var (
	// ErrBootstrapRequired reports that the stored cursor cannot be resumed
	// (no checkpoint, slot missing or invalidated, WAL removed, or the source
	// acknowledged beyond the checkpoint). An operator must call Bootstrap;
	// the stream never re-snapshots on its own.
	ErrBootstrapRequired = errors.New("cdc: bootstrap required")
	// ErrIncompatibleSchema stops the stream on a relation change it cannot
	// express as an additive schema version.
	ErrIncompatibleSchema = errors.New("cdc: incompatible schema change")
	// ErrUnsupported reports a type, table setting or change shape that cannot
	// be captured exactly.
	ErrUnsupported = errors.New("cdc: unsupported")
)

// Op is a change operation.
type Op string

const (
	OpInsert   Op = "insert"
	OpUpdate   Op = "update"
	OpDelete   Op = "delete"
	OpTruncate Op = "truncate"
)

type unchanged struct{}

func (unchanged) String() string { return "<unchanged>" }

// Unchanged is the Row value of a column whose TOAST value the source did not
// resend. The sink keeps its current value. It is distinct from nil (NULL).
var Unchanged any = unchanged{}

// Numeric is an exact PostgreSQL numeric in its canonical text form.
type Numeric string

// Column describes one captured column. Key marks replica identity columns.
type Column struct {
	Name    string `json:"name"`
	TypeOID uint32 `json:"type_oid"`
	TypeMod int32  `json:"type_mod"`
	Key     bool   `json:"key,omitempty"`
}

// Schema is one table's captured shape. Version starts at 1 and increments on
// each additive change.
type Schema struct {
	Namespace string   `json:"namespace"`
	Name      string   `json:"name"`
	Columns   []Column `json:"columns"`
	Version   int      `json:"version"`
}

// Table is the qualified table name.
func (s Schema) Table() string { return s.Namespace + "." + s.Name }

// Change is one row change. Row holds every column in Schema order for insert
// and update (values are nil for NULL, Unchanged, or the decoded Go value).
// Key holds the key column values in Schema order for every row change.
type Change struct {
	Op       Op
	Relation uint32
	Table    string
	Key      []any
	Row      []any
}

// Batch is one source transaction, or one chunk of the bootstrap snapshot.
// Changes are in source order. Schemas lists schema versions that take effect
// in this batch before its changes.
type Batch struct {
	LSN        uint64 // commit end LSN; the snapshot's consistent point for Snapshot batches
	CommitTime time.Time
	Snapshot   bool
	Reset      bool // first bootstrap batch: discard all prior sink state
	Schemas    map[uint32]Schema
	Changes    []Change
}

// Sink applies ordered batches. Apply must be durable when it returns and
// idempotent: a batch whose LSN the sink already applied may be delivered
// again after a crash and must leave the same state.
type Sink interface {
	Apply(ctx context.Context, b Batch) error
}

// Checkpoint is the durable resume position.
type Checkpoint struct {
	Slot    string            `json:"slot"`
	LSN     uint64            `json:"lsn"`
	Schemas map[uint32]Schema `json:"schemas"`
}

// CheckpointStore persists the checkpoint. Save must be durable when it
// returns. Load reports ok=false when no checkpoint exists.
type CheckpointStore interface {
	Load(ctx context.Context) (cp Checkpoint, ok bool, err error)
	Save(ctx context.Context, cp Checkpoint) error
}

// FileStore keeps a checkpoint in one JSON file, replaced by write, fsync,
// rename and directory fsync.
type FileStore struct{ Path string }

func (f FileStore) Load(context.Context) (Checkpoint, bool, error) {
	data, err := os.ReadFile(f.Path)
	if errors.Is(err, os.ErrNotExist) {
		return Checkpoint{}, false, nil
	}
	if err != nil {
		return Checkpoint{}, false, err
	}
	var cp Checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return Checkpoint{}, false, fmt.Errorf("cdc: corrupt checkpoint %s: %w", f.Path, err)
	}
	return cp, true, nil
}

func (f FileStore) Save(_ context.Context, cp Checkpoint) error {
	data, err := json.Marshal(cp)
	if err != nil {
		return err
	}
	dir := filepath.Dir(f.Path)
	tmp, err := os.CreateTemp(dir, ".checkpoint-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), f.Path); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
