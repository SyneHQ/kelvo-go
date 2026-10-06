// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package watchers

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/SYNEHQ/kelvo-go/watch"
	"github.com/jackc/pgx/v5"
)

// PostgreSQL owns source triggers and an outbox. Read never installs objects or
// deletes events. Ack deletes only the exact events durably captured upstream.
type PostgreSQL struct {
	DB    *sql.DB
	Scope watch.Scope
}

func objectName(id string) string {
	digest := sha256.Sum256([]byte(id))
	// Keep installed object names so upgrades retain pending customer events.
	return "_syne_watch_" + hex.EncodeToString(digest[:12])
}
func (p PostgreSQL) name(suffix string) string {
	return pgx.Identifier{p.Scope.Schema, objectName(p.Scope.ID) + suffix}.Sanitize()
}
func (p PostgreSQL) table() string { return pgx.Identifier{p.Scope.Schema, p.Scope.Table}.Sanitize() }
func literal(value string) string  { return "'" + strings.ReplaceAll(value, "'", "''") + "'" }

func (p PostgreSQL) begin(ctx context.Context, readOnly bool) (*sql.Tx, error) {
	if p.Scope.Validate() != nil || p.DB == nil || len(p.Scope.Schema) > 63 || len(p.Scope.Table) > 63 {
		return nil, watch.ErrInvalid
	}
	tx, err := p.DB.BeginTx(ctx, &sql.TxOptions{ReadOnly: readOnly})
	if err != nil {
		return nil, err
	}
	var database string
	if err = tx.QueryRowContext(ctx, `SELECT current_database()`).Scan(&database); err == nil && database != p.Scope.Database {
		err = watch.ErrConflict
	}
	if err == nil {
		_, err = tx.ExecContext(ctx, `SET LOCAL statement_timeout = '20s'`)
	}
	if err == nil {
		_, err = tx.ExecContext(ctx, `SET LOCAL lock_timeout = '5s'`)
	}
	if err == nil {
		_, err = tx.ExecContext(ctx, `SET LOCAL standard_conforming_strings = on`)
	}
	if err == nil {
		_, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, p.name(""))
	}
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	return tx, nil
}

func (p PostgreSQL) verifyOwned(ctx context.Context, tx *sql.Tx, suffix string) error {
	var valid bool
	err := tx.QueryRowContext(ctx, `SELECT c.relkind='r' AND c.relowner=(SELECT oid FROM pg_catalog.pg_roles WHERE rolname=current_user) AND NOT c.relrowsecurity FROM pg_catalog.pg_class c WHERE c.oid=pg_catalog.to_regclass($1)`, p.name(suffix)).Scan(&valid)
	if err != nil {
		return err
	}
	if !valid {
		return watch.ErrConflict
	}
	return nil
}

func (p PostgreSQL) generation(ctx context.Context, tx *sql.Tx) (bool, error) {
	var key, state string
	err := tx.QueryRowContext(ctx, `SELECT scope_key,state FROM `+p.name("_state")+` WHERE generation=$1`, p.Scope.Generation).Scan(&key, &state)
	if errors.Is(err, sql.ErrNoRows) {
		return false, watch.ErrUninitialized
	}
	if err != nil {
		return false, err
	}
	if key != p.Scope.Key() {
		return false, watch.ErrConflict
	}
	if state != "active" && state != "retired" {
		return false, watch.ErrConflict
	}
	return state == "active", nil
}

func (p PostgreSQL) Install(ctx context.Context) error {
	tx, err := p.begin(ctx, false)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS `+p.name("_state")+` (generation TEXT PRIMARY KEY,scope_key TEXT NOT NULL,state TEXT NOT NULL CHECK(state IN ('active','retired')))`)
	if err != nil {
		return err
	}
	if err = p.verifyOwned(ctx, tx, "_state"); err != nil {
		return err
	}
	active, err := p.generation(ctx, tx)
	if err == nil {
		if !active {
			return watch.ErrConflict
		}
		// An installed generation is immutable. Do not replace triggers while
		// ordinary writers may still be using them.
		return nil
	}
	if !errors.Is(err, watch.ErrUninitialized) {
		return err
	}
	var other bool
	if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM `+p.name("_state")+` WHERE state='active')`).Scan(&other); err != nil {
		return err
	}
	if other {
		return watch.ErrConflict
	}
	// Source writers acquire their table before inserting into the outbox.
	// Follow that order during DDL to avoid a source/outbox lock inversion.
	if _, err = tx.ExecContext(ctx, `LOCK TABLE `+p.table()+` IN SHARE ROW EXCLUSIVE MODE`); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS `+p.name("_events")+` (id BIGSERIAL PRIMARY KEY,operation TEXT NOT NULL,row_data JSONB,old_row_data JSONB,created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp())`)
	if err != nil {
		return err
	}
	if err = p.verifyOwned(ctx, tx, "_events"); err != nil {
		return err
	}
	// Retain the outbox on controlled adoption of an existing installation.
	if _, err = tx.ExecContext(ctx, `LOCK TABLE `+p.name("_events")+` IN ACCESS EXCLUSIVE MODE`); err != nil {
		return err
	}
	var unsafe bool
	err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname=$1 AND p.proname=$2 AND (p.proowner<>(SELECT oid FROM pg_catalog.pg_roles WHERE rolname=current_user) OR p.pronargs<>0))`, p.Scope.Schema, objectName(p.Scope.ID)+"_capture").Scan(&unsafe)
	if err != nil {
		return err
	}
	if unsafe {
		return watch.ErrConflict
	}
	trigger := pgx.Identifier{objectName(p.Scope.ID)}.Sanitize()
	err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_trigger t JOIN pg_catalog.pg_proc p ON p.oid=t.tgfoid JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace WHERE t.tgname=$1 AND t.tgrelid=pg_catalog.to_regclass($2) AND (n.nspname<>$3 OR p.proname<>$4))`, objectName(p.Scope.ID), p.table(), p.Scope.Schema, objectName(p.Scope.ID)+"_capture").Scan(&unsafe)
	if err != nil {
		return err
	}
	if unsafe {
		return watch.ErrConflict
	}
	body := `BEGIN
 IF TG_OP='DELETE' THEN INSERT INTO ` + p.name("_events") + ` (operation,old_row_data) VALUES(TG_OP,to_jsonb(OLD));
 ELSIF TG_OP='INSERT' THEN INSERT INTO ` + p.name("_events") + ` (operation,row_data) VALUES(TG_OP,to_jsonb(NEW));
 ELSE INSERT INTO ` + p.name("_events") + ` (operation,row_data,old_row_data) VALUES(TG_OP,to_jsonb(NEW),to_jsonb(OLD)); END IF;
 RETURN NULL; END;`
	for _, statement := range []string{
		`CREATE OR REPLACE FUNCTION ` + p.name("_capture") + `() RETURNS TRIGGER LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS ` + literal(body),
		`REVOKE ALL ON FUNCTION ` + p.name("_capture") + `() FROM PUBLIC`,
		`DROP TRIGGER IF EXISTS ` + trigger + ` ON ` + p.table(),
		`CREATE TRIGGER ` + trigger + ` AFTER INSERT OR UPDATE OR DELETE ON ` + p.table() + ` FOR EACH ROW EXECUTE FUNCTION ` + p.name("_capture") + `()`,
	} {
		if _, err = tx.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO `+p.name("_state")+` (generation,scope_key,state) VALUES($1,$2,'active')`, p.Scope.Generation, p.Scope.Key()); err != nil {
		return err
	}
	return commit(tx)
}

func commit(tx *sql.Tx) error {
	if err := tx.Commit(); err != nil {
		return errors.Join(watch.ErrOutcomeUnknown, err)
	}
	return nil
}

func (p PostgreSQL) Remove(ctx context.Context) error {
	tx, err := p.begin(ctx, false)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = p.verifyOwned(ctx, tx, "_state"); err != nil {
		return err
	}
	active, err := p.generation(ctx, tx)
	if err != nil {
		return err
	}
	if !active {
		return nil
	}
	if _, err = tx.ExecContext(ctx, `LOCK TABLE `+p.table()+` IN ACCESS EXCLUSIVE MODE`); err != nil {
		return err
	}
	if err = p.verifyOwned(ctx, tx, "_events"); err != nil {
		return err
	}
	for _, statement := range []string{
		`DROP TRIGGER IF EXISTS ` + pgx.Identifier{objectName(p.Scope.ID)}.Sanitize() + ` ON ` + p.table(),
		`DROP FUNCTION IF EXISTS ` + p.name("_capture") + `()`,
		`DROP TABLE IF EXISTS ` + p.name("_events"),
	} {
		if _, err = tx.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	if _, err = tx.ExecContext(ctx, `UPDATE `+p.name("_state")+` SET state='retired' WHERE generation=$1`, p.Scope.Generation); err != nil {
		return err
	}
	// Keep retired generations so delayed installs and acknowledgements cannot
	// resurrect or delete a replacement installation.
	return commit(tx)
}

func (p PostgreSQL) Read(ctx context.Context, maximum, waitMS int, maxBytes int64) (watch.Batch, error) {
	if maximum < 1 || maximum > watch.MaxEvents || waitMS < 1 || waitMS > 60000 || maxBytes < 1024 {
		return watch.Batch{}, watch.ErrInvalid
	}
	if maxBytes > watch.MaxBatchBytes {
		maxBytes = watch.MaxBatchBytes
	}
	deadline := time.Now().Add(time.Duration(waitMS) * time.Millisecond)
	for {
		batch, err := p.readOnce(ctx, maximum, maxBytes)
		if err != nil || len(batch.Events) > 0 || !time.Now().Before(deadline) {
			return batch, err
		}
		delay := time.Until(deadline)
		if delay > 250*time.Millisecond {
			delay = 250 * time.Millisecond
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return watch.Batch{}, ctx.Err()
		case <-timer.C:
		}
	}
}

func (p PostgreSQL) readOnce(ctx context.Context, maximum int, maxBytes int64) (watch.Batch, error) {
	tx, err := p.begin(ctx, true)
	if err != nil {
		return watch.Batch{}, err
	}
	defer tx.Rollback()
	active, err := p.generation(ctx, tx)
	if err != nil {
		return watch.Batch{}, err
	}
	if !active {
		return watch.Batch{}, watch.ErrConflict
	}
	rows, err := tx.QueryContext(ctx, p.selectEvents()+` ORDER BY id LIMIT $2`, maxBytes, maximum)
	if err != nil {
		return watch.Batch{}, err
	}
	defer rows.Close()
	events := []watch.Event{}
	// Reserve framing/checkpoint space. Oversized first rows fail explicitly
	// and stay in the source outbox; no event is silently skipped.
	used := int64(512)
	for rows.Next() {
		event, err := scanEvent(rows, maxBytes)
		if err != nil {
			if errors.Is(err, watch.ErrLimit) && len(events) > 0 {
				break
			}
			return watch.Batch{}, err
		}
		raw, err := json.Marshal(event)
		if err != nil {
			return watch.Batch{}, err
		}
		cost := int64(len(raw) + 128)
		if used+cost > maxBytes {
			if len(events) == 0 {
				return watch.Batch{}, watch.ErrLimit
			}
			break
		}
		events = append(events, event)
		used += cost
	}
	if err = rows.Err(); err != nil {
		return watch.Batch{}, err
	}
	return watch.NewBatch(p.Scope, events)
}

func (p PostgreSQL) selectEvents() string {
	return `SELECT id,operation,COALESCE(octet_length(row_data::text),0)+COALESCE(octet_length(old_row_data::text),0),CASE WHEN COALESCE(octet_length(row_data::text),0)+COALESCE(octet_length(old_row_data::text),0)<=$1 THEN row_data ELSE NULL END,CASE WHEN COALESCE(octet_length(row_data::text),0)+COALESCE(octet_length(old_row_data::text),0)<=$1 THEN old_row_data ELSE NULL END,created_at FROM ` + p.name("_events")
}

func scanEvent(row interface{ Scan(...any) error }, maximum int64) (watch.Event, error) {
	var id int64
	var size int64
	var event watch.Event
	var data, old []byte
	if err := row.Scan(&id, &event.Operation, &size, &data, &old, &event.Timestamp); err != nil {
		return watch.Event{}, err
	}
	event.Data, event.OldData = data, old
	if size > maximum {
		return watch.Event{}, watch.ErrLimit
	}
	if event.Data == nil {
		event.Data = json.RawMessage(`null`)
	}
	if event.OldData == nil {
		event.OldData = json.RawMessage(`null`)
	}
	event.ID = strconv.FormatInt(id, 10)
	event.Timestamp = event.Timestamp.UTC()
	if event.Validate() != nil {
		return watch.Event{}, watch.ErrInvalid
	}
	return event, nil
}

func (p PostgreSQL) Ack(ctx context.Context, checkpoint watch.Checkpoint, sinkReceiptSHA256 string) error {
	if checkpoint.Validate(p.Scope) != nil || checkpoint.Resume != nil || !operations.ValidDigest(sinkReceiptSHA256) {
		return watch.ErrInvalid
	}
	tx, err := p.begin(ctx, false)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	active, err := p.generation(ctx, tx)
	if err != nil {
		return err
	}
	if !active {
		return watch.ErrConflict
	}
	for _, entry := range checkpoint.Entries {
		id, err := strconv.ParseInt(entry.ID, 10, 64)
		if err != nil {
			return watch.ErrInvalid
		}
		event, err := scanEvent(tx.QueryRowContext(ctx, p.selectEvents()+` WHERE id=$2 FOR UPDATE`, watch.MaxBatchBytes, id), watch.MaxBatchBytes)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		} // An identical acknowledgement may already have committed.
		if err != nil {
			return err
		}
		digest, err := event.Digest()
		if err != nil || digest != entry.SHA256 {
			return watch.ErrConflict
		}
		if _, err = tx.ExecContext(ctx, `DELETE FROM `+p.name("_events")+` WHERE id=$1`, id); err != nil {
			return err
		}
	}
	return commit(tx)
}
