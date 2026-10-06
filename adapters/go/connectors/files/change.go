package files

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/adapters/go/connectors/sqlsession"
	"github.com/SYNEHQ/kelvo-go/filesnapshot"
)

func (s *Session) PrepareFileChange(ctx context.Context, change adapter.Change, out io.Writer) (result adapter.ChangeResult, candidate *filesnapshot.Descriptor, err error) {
	result.Outcome = "failed"
	if ctx == nil || out == nil || s == nil || change.Role != "" || change.Isolation != "" && change.Isolation != "default" || len(change.Statements) < 1 || len(change.Statements) > 100 || len(change.Parameters) != len(change.Statements) || s.descriptor.Format != "sqlite" && s.descriptor.Format != "duckdb" {
		return result, nil, adapter.ErrUnsupported
	}
	total := 0
	parameters := make([][]any, len(change.Statements))
	for i, statement := range change.Statements {
		total += len(statement)
		if total > 1<<20 || sqlsession.FileStatement(statement) != nil {
			return result, nil, adapter.ErrUnsupported
		}
		parameters[i], err = adapter.SQLParameters(change.Parameters[i])
		if err != nil {
			return result, nil, err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file == nil {
		return result, nil, adapter.ErrInvalid
	}
	ctx, cancel := context.WithTimeout(ctx, s.limits.Timeout)
	defer cancel()
	dir, err := os.MkdirTemp("", "kelvo-file-change-")
	if err != nil {
		return result, nil, err
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "candidate."+s.descriptor.Format)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return result, nil, err
	}
	_, err = s.file.Seek(0, io.SeekStart)
	if err == nil {
		_, err = io.CopyN(file, s.file, s.descriptor.Bytes)
	}
	err = errors.Join(err, file.Close())
	if err != nil {
		return result, nil, err
	}
	db, err := s.openMutation(ctx, path)
	if err != nil {
		return result, nil, err
	}
	result, executionErr := executeFileChange(ctx, db, change, parameters)
	closeErr := db.Close()
	if closeErr != nil || ctx.Err() != nil {
		return adapter.ChangeResult{Outcome: "failed"}, nil, adapter.ErrInvalid
	}
	if result.Completed == 0 {
		return result, nil, executionErr
	}
	file, err = os.Open(path)
	if err != nil {
		return adapter.ChangeResult{Outcome: "failed"}, nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || info.Size() < 1 || info.Size() > filesnapshot.MaxBytes || info.Size() > int64(s.limits.MaxTempMB)<<20 {
		return adapter.ChangeResult{Outcome: "failed"}, nil, adapter.ErrLimit
	}
	hash := sha256.New()
	if _, err = io.Copy(hash, file); err != nil {
		return adapter.ChangeResult{Outcome: "failed"}, nil, err
	}
	if _, err = file.Seek(0, io.SeekStart); err != nil {
		return adapter.ChangeResult{Outcome: "failed"}, nil, err
	}
	descriptor := filesnapshot.Descriptor{Version: 1, Format: s.descriptor.Format, Bytes: info.Size(), SHA256: hex.EncodeToString(hash.Sum(nil))}
	if _, err = io.Copy(out, file); err != nil {
		return adapter.ChangeResult{Outcome: "failed"}, nil, err
	}
	return result, &descriptor, executionErr
}
func executeFileChange(ctx context.Context, db *sql.DB, change adapter.Change, parameters [][]any) (adapter.ChangeResult, error) {
	result := adapter.ChangeResult{Outcome: "failed"}
	affected := int64(0)
	known := true
	var tx *sql.Tx
	var err error
	if change.Transaction {
		tx, err = db.BeginTx(ctx, nil)
		if err != nil {
			return result, err
		}
		defer tx.Rollback()
	}
	for i, statement := range change.Statements {
		result.Attempted++
		var response sql.Result
		if tx != nil {
			response, err = tx.ExecContext(ctx, statement, parameters[i]...)
		} else {
			response, err = db.ExecContext(ctx, statement, parameters[i]...)
		}
		if err != nil {
			if tx != nil {
				result.Completed = 0
			}
			return result, err
		}
		result.Completed++
		count, countErr := response.RowsAffected()
		if countErr != nil || count < 0 || count > math.MaxInt64-affected {
			known = false
		} else {
			affected += count
		}
	}
	if tx != nil {
		if err = tx.Commit(); err != nil {
			result.Completed = 0
			return result, err
		}
	}
	result.Outcome = "succeeded"
	if known {
		result.AffectedRows = &affected
	}
	return result, nil
}
