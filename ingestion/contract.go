// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Package ingestion defines driver-free ingestion batches and receipts.
// Callers must authorize the exact scope; validation does not grant access.
package ingestion

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	MaxBatchBytes      = 1 << 20
	MaxRecords         = 1000
	MaxRecordBytes     = 128 << 10
	MaxCheckpointBytes = 16 << 10
)

var (
	ErrInvalid        = errors.New("invalid ingestion request")
	ErrConflict       = errors.New("ingestion sequence or retry content conflict")
	ErrDatabase       = errors.New("ingestion destination database mismatch")
	ErrUninitialized  = errors.New("ingestion destination is not initialized")
	ErrOutcomeUnknown = errors.New("ingestion source commit outcome is unknown")
	identifier        = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)
	digest            = regexp.MustCompile(`^[a-f0-9]{64}$`)
)

// Binding is a digest of the approved manifest and nonsecret source configuration.
// A changed binding starts a separate stream generation, preserving old evidence.
// Credentials and job IDs must not be included in this stable digest.
type Scope struct {
	TeamID       string `json:"team_id"`
	SourceID     string `json:"source_id"`
	ConnectionID string `json:"connection_id"`
	Database     string `json:"database"`
	Schema       string `json:"schema"`
	Stream       string `json:"stream"`
	Binding      string `json:"binding"`
}

type Record struct {
	ID      string          `json:"id"`
	Payload json.RawMessage `json:"payload"`
	Deleted bool            `json:"deleted"`
}

type Batch struct {
	ID               string          `json:"id"`
	RunID            string          `json:"run_id"`
	ExpectedSequence int64           `json:"expected_sequence"`
	ObservedAt       string          `json:"observed_at"`
	Records          []Record        `json:"records"`
	Checkpoint       json.RawMessage `json:"checkpoint"`
}

type Receipt struct {
	BatchID     string    `json:"batch_id"`
	Digest      string    `json:"digest"`
	Sequence    int64     `json:"sequence"`
	Records     int       `json:"records"`
	CommittedAt time.Time `json:"committed_at"`
}

type State struct {
	Sequence    int64           `json:"sequence"`
	Checkpoint  json.RawMessage `json:"checkpoint"`
	LastReceipt *Receipt        `json:"last_receipt,omitempty"`
}

func textValid(value string, max int) bool {
	return value != "" && len(value) <= max && utf8.ValidString(value) && !strings.ContainsAny(value, "\x00\r\n")
}

func (s Scope) Validate() error {
	for _, value := range []string{s.TeamID, s.SourceID, s.ConnectionID, s.Database, s.Stream} {
		if !textValid(value, 128) {
			return fmt.Errorf("%w: scope", ErrInvalid)
		}
	}
	if !identifier.MatchString(s.Schema) || strings.HasPrefix(s.Schema, "pg_") || s.Schema == "information_schema" || !digest.MatchString(s.Binding) {
		return fmt.Errorf("%w: schema or binding", ErrInvalid)
	}
	return nil
}

// Key preserves the durable stream identity used by existing destinations.
func (s Scope) Key() string {
	encoded, _ := json.Marshal(s)
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

// Digest validates the batch and returns its canonical retry-content digest.
func (b Batch) Digest() (string, error) {
	if !textValid(b.ID, 128) || !textValid(b.RunID, 128) || b.ExpectedSequence < 0 || b.ExpectedSequence == math.MaxInt64 || len(b.Records) > MaxRecords {
		return "", fmt.Errorf("%w: batch identity or limit", ErrInvalid)
	}
	if _, err := time.Parse(time.RFC3339Nano, b.ObservedAt); err != nil {
		return "", fmt.Errorf("%w: observation timestamp", ErrInvalid)
	}
	if len(b.Checkpoint) > MaxCheckpointBytes || !ValidObject(b.Checkpoint) {
		return "", fmt.Errorf("%w: checkpoint", ErrInvalid)
	}
	ids := make(map[string]struct{}, len(b.Records))
	size := len(b.Checkpoint)
	for _, r := range b.Records {
		size += len(r.Payload) + len(r.ID)
		if size > MaxBatchBytes {
			return "", fmt.Errorf("%w: batch byte limit", ErrInvalid)
		}
		if !textValid(r.ID, 512) || len(r.Payload) > MaxRecordBytes || !ValidObject(r.Payload) {
			return "", fmt.Errorf("%w: record", ErrInvalid)
		}
		if _, exists := ids[r.ID]; exists {
			return "", fmt.Errorf("%w: duplicate record ID within batch", ErrInvalid)
		}
		ids[r.ID] = struct{}{}
	}
	encoded, err := json.Marshal(b)
	if err != nil || len(encoded) > MaxBatchBytes {
		return "", fmt.Errorf("%w: batch byte limit", ErrInvalid)
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

// ValidObject rejects ambiguous duplicate keys, excessive nesting and PostgreSQL-incompatible
// NULs before writing. UseNumber preserves decimal lexical values during validation.
func ValidObject(raw []byte) bool {
	if !utf8.Valid(raw) {
		return false
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	first, err := d.Token()
	if err != nil || first != json.Delim('{') {
		return false
	}
	if err := readContainer(d, first, 0); err != nil {
		return false
	}
	_, err = d.Token()
	return err == io.EOF
}

func readContainer(d *json.Decoder, opening json.Token, depth int) error {
	if depth > 32 {
		return ErrInvalid
	}
	seen := map[string]bool{}
	for d.More() {
		if opening == json.Delim('{') {
			key, err := d.Token()
			if err != nil {
				return err
			}
			k, ok := key.(string)
			if !ok || seen[k] || strings.ContainsRune(k, 0) {
				return ErrInvalid
			}
			seen[k] = true
		}
		value, err := d.Token()
		if err != nil {
			return err
		}
		switch v := value.(type) {
		case json.Delim:
			if v != '{' && v != '[' {
				return ErrInvalid
			}
			if err := readContainer(d, v, depth+1); err != nil {
				return err
			}
		case string:
			if strings.ContainsRune(v, 0) {
				return ErrInvalid
			}
		}
	}
	close, err := d.Token()
	if err != nil {
		return err
	}
	if (opening == json.Delim('{') && close != json.Delim('}')) || (opening == json.Delim('[') && close != json.Delim(']')) {
		return ErrInvalid
	}
	return nil
}

// ParseBatch checks the entire sealed payload before the adapter opens a transaction.
func ParseBatch(raw []byte) (Batch, error) {
	if len(raw) > MaxBatchBytes || !ValidObject(raw) {
		return Batch{}, ErrInvalid
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var batch Batch
	if decoder.Decode(&batch) != nil {
		return Batch{}, ErrInvalid
	}
	if _, err := batch.Digest(); err != nil {
		return Batch{}, err
	}
	return batch, nil
}
