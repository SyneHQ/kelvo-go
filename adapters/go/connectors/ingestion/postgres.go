// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package ingestion

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/SYNEHQ/kelvo-go/ingestion"
)

// Store uses a pre-authorized PostgreSQL connection. It never accepts SQL from a
// worker. Install is a separate explicit mutation, not a side effect of State.
type Store struct {
	DB    *sql.DB
	Scope ingestion.Scope
}

func (s Store) name(suffix string) string {
	return `"` + s.Scope.Schema + `"."_syne_ingest_` + suffix + `_v1"`
}

func (s Store) begin(ctx context.Context, readOnly bool) (*sql.Tx, error) {
	if err := s.Scope.Validate(); err != nil {
		return nil, err
	}
	if s.DB == nil {
		return nil, fmt.Errorf("%w: missing destination", ingestion.ErrInvalid)
	}
	tx, err := s.DB.BeginTx(ctx, &sql.TxOptions{ReadOnly: readOnly})
	if err != nil {
		return nil, err
	}
	var database string
	if err = tx.QueryRowContext(ctx, `SELECT current_database()`).Scan(&database); err == nil && database != s.Scope.Database {
		err = ingestion.ErrDatabase
	}
	if err == nil {
		_, err = tx.ExecContext(ctx, `SET LOCAL statement_timeout = '20s'`)
	}
	if err == nil {
		_, err = tx.ExecContext(ctx, `SET LOCAL lock_timeout = '5s'`)
	}
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	return tx, nil
}

// Install creates only namespaced connector tables. No user table is altered.
// The bridge must require an explicit destination initialization grant for this.
func (s Store) Install(ctx context.Context) error {
	tx, err := s.begin(ctx, false)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, "syne-ingest-install:"+s.Scope.Schema); err != nil {
		return err
	}
	statements := []string{
		`CREATE SCHEMA IF NOT EXISTS "` + s.Scope.Schema + `"`,
		`CREATE TABLE IF NOT EXISTS ` + s.name("streams") + ` (
   scope_key TEXT PRIMARY KEY, team_id TEXT NOT NULL, source_id TEXT NOT NULL,
   connection_id TEXT NOT NULL, database_name TEXT NOT NULL, stream TEXT NOT NULL, binding TEXT NOT NULL,
   sequence BIGINT NOT NULL DEFAULT 0 CHECK(sequence >= 0), checkpoint JSONB NOT NULL DEFAULT '{}',
   last_receipt JSONB)`,
		`CREATE TABLE IF NOT EXISTS ` + s.name("batches") + ` (
   scope_key TEXT NOT NULL REFERENCES ` + s.name("streams") + `(scope_key), batch_id TEXT NOT NULL,
   digest TEXT NOT NULL, sequence BIGINT NOT NULL, record_count INT NOT NULL, run_id TEXT NOT NULL,
   observed_at TIMESTAMPTZ NOT NULL, checkpoint JSONB NOT NULL, committed_at TIMESTAMPTZ NOT NULL,
   PRIMARY KEY(scope_key,batch_id), UNIQUE(scope_key,sequence))`,
		`CREATE TABLE IF NOT EXISTS ` + s.name("versions") + ` (
   scope_key TEXT NOT NULL, record_id TEXT NOT NULL, batch_id TEXT NOT NULL,
   payload JSONB NOT NULL, original_json TEXT NOT NULL, payload_hash TEXT NOT NULL, deleted BOOLEAN NOT NULL,
   PRIMARY KEY(scope_key,record_id,batch_id),
   FOREIGN KEY(scope_key,batch_id) REFERENCES ` + s.name("batches") + `(scope_key,batch_id))`,
		`CREATE TABLE IF NOT EXISTS ` + s.name("records") + ` (
   scope_key TEXT NOT NULL, record_id TEXT NOT NULL, batch_id TEXT NOT NULL,
   payload JSONB NOT NULL, payload_hash TEXT NOT NULL, deleted BOOLEAN NOT NULL,
   PRIMARY KEY(scope_key,record_id),
   FOREIGN KEY(scope_key,record_id,batch_id) REFERENCES ` + s.name("versions") + `(scope_key,record_id,batch_id))`,
	}
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO `+s.name("streams")+`
  (scope_key,team_id,source_id,connection_id,database_name,stream,binding) VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING`,
		s.Scope.Key(), s.Scope.TeamID, s.Scope.SourceID, s.Scope.ConnectionID, s.Scope.Database, s.Scope.Stream, s.Scope.Binding)
	if err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return errors.Join(ingestion.ErrOutcomeUnknown, err)
	}
	return nil
}

func (s Store) State(ctx context.Context) (ingestion.State, error) {
	tx, err := s.begin(ctx, true)
	if err != nil {
		return ingestion.State{}, err
	}
	defer tx.Rollback()
	var state ingestion.State
	var receipt []byte
	err = tx.QueryRowContext(ctx, `SELECT sequence,
 CASE WHEN octet_length(checkpoint::text)<=16384 THEN checkpoint ELSE NULL END,
 CASE WHEN octet_length(last_receipt::text)<=4096 THEN last_receipt ELSE NULL END
 FROM `+s.name("streams")+` WHERE scope_key=$1`, s.Scope.Key()).Scan(&state.Sequence, &state.Checkpoint, &receipt)
	if errors.Is(err, sql.ErrNoRows) {
		return ingestion.State{}, ingestion.ErrUninitialized
	}
	if err != nil {
		return ingestion.State{}, err
	}
	if len(receipt) > 0 {
		decoded, err := ingestion.ParseReceipt(receipt)
		if err != nil {
			return ingestion.State{}, err
		}
		state.LastReceipt = &decoded
	}
	if err := state.Validate(); err != nil {
		return ingestion.State{}, err
	}
	return state, nil
}

// Commit is atomic and compare-and-swap serialized per stream generation. On an
// ambiguous database commit, callers retry this identical Batch or read State;
// they must not advance the cursor merely because extraction completed.
func (s Store) Commit(ctx context.Context, batch ingestion.Batch) (ingestion.Receipt, error) {
	hash, err := batch.Digest()
	if err != nil {
		return ingestion.Receipt{}, err
	}
	tx, err := s.begin(ctx, false)
	if err != nil {
		return ingestion.Receipt{}, err
	}
	defer tx.Rollback()
	key := s.Scope.Key()
	var sequence int64
	if err = tx.QueryRowContext(ctx, `SELECT sequence FROM `+s.name("streams")+` WHERE scope_key=$1 FOR UPDATE`, key).Scan(&sequence); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ingestion.Receipt{}, ingestion.ErrUninitialized
		}
		return ingestion.Receipt{}, err
	}
	var receipt ingestion.Receipt
	err = tx.QueryRowContext(ctx, `SELECT batch_id,digest,sequence,record_count,committed_at FROM `+s.name("batches")+` WHERE scope_key=$1 AND batch_id=$2`, key, batch.ID).
		Scan(&receipt.BatchID, &receipt.Digest, &receipt.Sequence, &receipt.Records, &receipt.CommittedAt)
	if err == nil {
		if receipt.Digest != hash {
			return ingestion.Receipt{}, ingestion.ErrConflict
		}
		if receipt.ValidateBatch(batch) != nil {
			return ingestion.Receipt{}, ingestion.ErrInvalid
		}
		return receipt, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return ingestion.Receipt{}, err
	}
	if sequence != batch.ExpectedSequence {
		return ingestion.Receipt{}, ingestion.ErrConflict
	}
	receipt = ingestion.Receipt{BatchID: batch.ID, Digest: hash, Sequence: sequence + 1, Records: len(batch.Records)}
	// PostgreSQL timestamps have microsecond precision; use the same value in both
	// receipt representations so a lost-ack retry returns an identical receipt.
	if err = tx.QueryRowContext(ctx, `SELECT clock_timestamp()`).Scan(&receipt.CommittedAt); err != nil {
		return ingestion.Receipt{}, err
	}
	observed, _ := time.Parse(time.RFC3339Nano, batch.ObservedAt)
	_, err = tx.ExecContext(ctx, `INSERT INTO `+s.name("batches")+`
  (scope_key,batch_id,digest,sequence,record_count,run_id,observed_at,checkpoint,committed_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
		key, batch.ID, hash, receipt.Sequence, receipt.Records, batch.RunID, observed, string(batch.Checkpoint), receipt.CommittedAt)
	if err != nil {
		return ingestion.Receipt{}, err
	}
	// Two bounded bulk statements avoid one network round trip per source row.
	// Batches retain every observation; versions retain content transitions only.
	// The stream lock makes this comparison and the following upsert atomic.
	type entry struct {
		ID       string          `json:"id"`
		Payload  json.RawMessage `json:"payload"`
		Original string          `json:"original"`
		Hash     string          `json:"hash"`
		Deleted  bool            `json:"deleted"`
	}
	entries := make([]entry, 0, len(batch.Records))
	for _, record := range batch.Records {
		sum := sha256.Sum256(record.Payload)
		entries = append(entries, entry{record.ID, record.Payload, string(record.Payload), hex.EncodeToString(sum[:]), record.Deleted})
	}
	rows, err := json.Marshal(entries)
	if err != nil {
		return ingestion.Receipt{}, err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO `+s.name("versions")+` (scope_key,record_id,batch_id,payload,original_json,payload_hash,deleted)
  SELECT $1,x.id,$2,x.payload,x.original,x.hash,x.deleted FROM jsonb_to_recordset($3::jsonb) AS x(id text,payload jsonb,original text,hash text,deleted boolean)
  WHERE NOT EXISTS (SELECT 1 FROM `+s.name("records")+` current
   WHERE current.scope_key=$1 AND current.record_id=x.id AND current.payload_hash=x.hash AND current.deleted=x.deleted)`, key, batch.ID, string(rows)); err != nil {
		return ingestion.Receipt{}, err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO `+s.name("records")+` (scope_key,record_id,batch_id,payload,payload_hash,deleted)
  SELECT scope_key,record_id,batch_id,payload,payload_hash,deleted FROM `+s.name("versions")+` WHERE scope_key=$1 AND batch_id=$2
  ON CONFLICT(scope_key,record_id) DO UPDATE SET batch_id=EXCLUDED.batch_id,payload=EXCLUDED.payload,payload_hash=EXCLUDED.payload_hash,deleted=EXCLUDED.deleted`, key, batch.ID); err != nil {
		return ingestion.Receipt{}, err
	}
	encoded, _ := json.Marshal(receipt)
	if _, err = tx.ExecContext(ctx, `UPDATE `+s.name("streams")+` SET sequence=$2,checkpoint=$3,last_receipt=$4 WHERE scope_key=$1`, key, receipt.Sequence, string(batch.Checkpoint), string(encoded)); err != nil {
		return ingestion.Receipt{}, err
	}
	if err := tx.Commit(); err != nil {
		return ingestion.Receipt{}, errors.Join(ingestion.ErrOutcomeUnknown, err)
	}
	return receipt, nil
}
