// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package watchers

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/SYNEHQ/kelvo-go/watch"
)

func (m MySQL) selectEvents() string {
	return `SELECT id,operation,COALESCE(OCTET_LENGTH(row_data),0)+COALESCE(OCTET_LENGTH(old_row_data),0),CASE WHEN COALESCE(OCTET_LENGTH(row_data),0)+COALESCE(OCTET_LENGTH(old_row_data),0)<=? THEN row_data ELSE NULL END,CASE WHEN COALESCE(OCTET_LENGTH(row_data),0)+COALESCE(OCTET_LENGTH(old_row_data),0)<=? THEN old_row_data ELSE NULL END,DATE_FORMAT(created_at,'%Y-%m-%dT%H:%i:%s.%fZ') FROM ` + m.name("_events")
}

func scanMySQLEvent(row interface{ Scan(...any) error }, maximum int64) (watch.Event, error) {
	var id uint64
	var size int64
	var data, old []byte
	var timestamp string
	var event watch.Event
	if err := row.Scan(&id, &event.Operation, &size, &data, &old, &timestamp); err != nil {
		return event, err
	}
	if size > maximum {
		return event, watch.ErrLimit
	}
	var err error
	event.Timestamp, err = time.Parse("2006-01-02T15:04:05.000000Z", timestamp)
	if err != nil {
		return event, watch.ErrInvalid
	}
	event.ID = strconv.FormatUint(id, 10)
	event.Data, event.OldData = data, old
	if data == nil {
		event.Data = json.RawMessage(`null`)
	}
	if old == nil {
		event.OldData = json.RawMessage(`null`)
	}
	return event, event.Validate()
}

func (m MySQL) Read(ctx context.Context, maximum, waitMS int, maxBytes int64) (watch.Batch, error) {
	if maximum < 1 || maximum > watch.MaxEvents || waitMS < 1 || waitMS > 60000 || maxBytes < 1024 {
		return watch.Batch{}, watch.ErrInvalid
	}
	if maxBytes > watch.MaxBatchBytes {
		maxBytes = watch.MaxBatchBytes
	}
	deadline := time.Now().Add(time.Duration(waitMS) * time.Millisecond)
	for {
		batch, err := m.readOnce(ctx, maximum, maxBytes)
		if err != nil || len(batch.Events) != 0 || !time.Now().Before(deadline) {
			return batch, err
		}
		delay := min(time.Until(deadline), 250*time.Millisecond)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return watch.Batch{}, ctx.Err()
		case <-timer.C:
		}
	}
}

func (m MySQL) readOnce(ctx context.Context, maximum int, maxBytes int64) (watch.Batch, error) {
	s, err := m.open(ctx)
	if err != nil {
		return watch.Batch{}, err
	}
	defer s.close()
	tx, err := s.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelReadCommitted})
	if err != nil {
		return watch.Batch{}, err
	}
	defer tx.Rollback()
	if err = m.active(ctx, tx); err != nil {
		return watch.Batch{}, err
	}
	rows, err := tx.QueryContext(ctx, m.selectEvents()+` ORDER BY id LIMIT ?`, maxBytes, maxBytes, maximum)
	if err != nil {
		return watch.Batch{}, err
	}
	defer rows.Close()
	events := []watch.Event{}
	used := int64(512)
	for rows.Next() {
		event, err := scanMySQLEvent(rows, maxBytes)
		if err != nil {
			if errors.Is(err, watch.ErrLimit) && len(events) != 0 {
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
	if err := rows.Err(); err != nil {
		return watch.Batch{}, err
	}
	return watch.NewBatch(m.Scope, events)
}

func (m MySQL) Ack(ctx context.Context, checkpoint watch.Checkpoint, sinkReceiptSHA256 string) error {
	if checkpoint.Resume != nil || checkpoint.Validate(m.Scope) != nil || !operations.ValidDigest(sinkReceiptSHA256) {
		return watch.ErrInvalid
	}
	for _, entry := range checkpoint.Entries {
		id, err := strconv.ParseUint(entry.ID, 10, 64)
		if err != nil || id == 0 || strconv.FormatUint(id, 10) != entry.ID {
			return watch.ErrInvalid
		}
	}
	s, err := m.open(ctx)
	if err != nil {
		return err
	}
	defer s.close()
	tx, err := s.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = m.active(ctx, tx); err != nil {
		return err
	}
	for _, entry := range checkpoint.Entries {
		// Decimal text with an explicit unsigned cast also supports BIGINT IDs
		// above MaxInt64 without driver-dependent parameter conversion.
		event, err := scanMySQLEvent(tx.QueryRowContext(ctx, m.selectEvents()+` WHERE id=CAST(? AS UNSIGNED) FOR UPDATE`, watch.MaxBatchBytes, watch.MaxBatchBytes, entry.ID), watch.MaxBatchBytes)
		if errors.Is(err, sql.ErrNoRows) {
			continue // An identical acknowledgement may already have committed.
		}
		if err != nil {
			return err
		}
		digest, err := event.Digest()
		if err != nil || digest != entry.SHA256 {
			return watch.ErrConflict
		}
		result, err := tx.ExecContext(ctx, `DELETE FROM `+m.name("_events")+` WHERE id=CAST(? AS UNSIGNED)`, entry.ID)
		if err != nil {
			return err
		}
		changed, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if changed != 1 {
			return watch.ErrConflict
		}
	}
	return commit(tx)
}
