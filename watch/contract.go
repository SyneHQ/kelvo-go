// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Package watch defines bounded source events and exact acknowledgements.
package watch

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/SYNEHQ/kelvo-go/operations"
)

const (
	Version             = 1
	MaxEvents           = 1000
	MaxBatchBytes       = 2 << 20
	MaxCheckpointBytes  = 192 << 10
	MaxResumeTokenBytes = 8 << 10
)

var (
	ErrInvalid        = errors.New("invalid watcher request")
	ErrConflict       = errors.New("watcher generation or event conflict")
	ErrLimit          = errors.New("watcher payload limit exceeded")
	ErrUninitialized  = errors.New("watcher is not installed")
	ErrOutcomeUnknown = errors.New("watcher mutation outcome unknown")
)

// Scope comes from the verified operation and private connection resolution.
// It is never taken from a checkpoint supplied by the caller.
type Scope struct {
	TeamID       string `json:"team_id"`
	ConnectionID string `json:"connection_id"`
	Database     string `json:"database"`
	Schema       string `json:"schema"`
	Table        string `json:"table"`
	ID           string `json:"id"`
	Generation   string `json:"generation"`
}

func validText(value string, limit int) bool {
	return value != "" && len(value) <= limit && utf8.ValidString(value) && !strings.ContainsAny(value, "\x00\r\n")
}

func (s Scope) Validate() error {
	if !validText(s.TeamID, 256) || !validText(s.ConnectionID, 256) || !validText(s.ID, 256) ||
		!validText(s.Database, 128) || !validText(s.Schema, 128) || !validText(s.Table, 128) || !operations.ValidID(s.Generation) {
		return ErrInvalid
	}
	return nil
}

// Key excludes generation so a new generation must retire the old one first.
func (s Scope) Key() string {
	s.Generation = ""
	raw, _ := json.Marshal(s)
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

type Event struct {
	ID        string          `json:"id"`
	Operation string          `json:"operation"`
	Data      json.RawMessage `json:"data"`
	OldData   json.RawMessage `json:"old_data"`
	Timestamp time.Time       `json:"timestamp"`
	// Context carries the original provider envelope fields used by filters.
	Context     json.RawMessage `json:"context,omitempty"`
	DataKind    string          `json:"data_kind,omitempty"`
	OldDataKind string          `json:"old_data_kind,omitempty"`
}

func validID(id string) bool {
	if strings.HasPrefix(id, "mongo:") {
		return operations.ValidDigest(strings.TrimPrefix(id, "mongo:"))
	}
	n, err := strconv.ParseUint(id, 10, 64)
	return err == nil && n > 0 && strconv.FormatUint(n, 10) == id
}

func (e Event) Validate() error {
	if !validID(e.ID) || e.Timestamp.IsZero() || e.Timestamp.Year() < 1 || e.Timestamp.Year() > 9999 {
		return ErrInvalid
	}
	switch e.Operation {
	case "INSERT", "UPDATE", "DELETE":
	default:
		return ErrInvalid
	}
	for _, value := range []json.RawMessage{e.Data, e.OldData} {
		if len(value) == 0 || len(value) > MaxBatchBytes {
			return ErrInvalid
		}
		var object map[string]json.RawMessage
		if operations.DecodeStrict(value, &object, MaxBatchBytes) != nil {
			return ErrInvalid
		}
	}
	if len(e.Context) > 0 {
		var object map[string]json.RawMessage
		if operations.DecodeStrict(e.Context, &object, MaxBatchBytes) != nil || object == nil {
			return ErrInvalid
		}
	}
	for _, kind := range []string{e.DataKind, e.OldDataKind} {
		switch kind {
		case "", "document", "document_key", "update_delta":
		default:
			return ErrInvalid
		}
	}
	return nil
}

func (e Event) Digest() (string, error) {
	if err := e.Validate(); err != nil {
		return "", err
	}
	// Canonical JSON keeps object key order and database whitespace out of the
	// acknowledgement identity without rounding customer numbers.
	var canonical any
	raw, err := json.Marshal(e)
	if err != nil {
		return "", ErrInvalid
	}
	if operations.DecodeStrict(raw, &canonical, MaxBatchBytes) != nil {
		return "", ErrInvalid
	}
	raw, err = json.Marshal(canonical)
	if err != nil {
		return "", ErrInvalid
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]), nil
}

type Entry struct {
	ID     string `json:"id"`
	SHA256 string `json:"sha256"`
}

// Resume binds a provider cursor transition to the exact durable capture. The
// provider validates its BSON token structure; the shared contract bounds and
// validates the canonical base64 representation only.
type Resume struct {
	From string `json:"from"`
	To   string `json:"to"`
}

func (r Resume) Validate() error {
	if r.From == r.To {
		return ErrInvalid
	}
	for _, token := range []string{r.From, r.To} {
		if len(token) == 0 || len(token) > base64.StdEncoding.EncodedLen(MaxResumeTokenBytes) {
			return ErrInvalid
		}
		raw, err := base64.StdEncoding.Strict().DecodeString(token)
		if err != nil || len(raw) == 0 || len(raw) > MaxResumeTokenBytes || base64.StdEncoding.EncodeToString(raw) != token {
			return ErrInvalid
		}
	}
	return nil
}

// Checkpoint names individual events. A high-water mark would lose events
// whose transactions commit out of sequence.
type Checkpoint struct {
	Version     int     `json:"version"`
	ScopeSHA256 string  `json:"scope_sha256"`
	Generation  string  `json:"generation"`
	Entries     []Entry `json:"entries"`
	Resume      *Resume `json:"resume,omitempty"`
}

func (c Checkpoint) Validate(s Scope) error {
	if s.Validate() != nil || c.Version != Version || c.ScopeSHA256 != s.Key() || c.Generation != s.Generation || len(c.Entries) > MaxEvents || len(c.Entries) == 0 && c.Resume == nil || c.Resume != nil && c.Resume.Validate() != nil {
		return ErrInvalid
	}
	seen := make(map[string]bool, len(c.Entries))
	for _, entry := range c.Entries {
		if !validID(entry.ID) || !operations.ValidDigest(entry.SHA256) || seen[entry.ID] {
			return ErrInvalid
		}
		if strings.HasPrefix(entry.ID, "mongo:") != (c.Resume != nil) {
			return ErrInvalid
		}
		seen[entry.ID] = true
	}
	raw, err := json.Marshal(c)
	if err != nil || len(raw) > MaxCheckpointBytes {
		return ErrLimit
	}
	return nil
}

func ParseCheckpoint(raw []byte, scope Scope) (Checkpoint, error) {
	var checkpoint Checkpoint
	if operations.DecodeStrict(raw, &checkpoint, MaxCheckpointBytes) != nil || checkpoint.Validate(scope) != nil {
		return Checkpoint{}, ErrInvalid
	}
	return checkpoint, nil
}

type Batch struct {
	Version     int         `json:"version"`
	ScopeSHA256 string      `json:"scope_sha256"`
	Generation  string      `json:"generation"`
	Events      []Event     `json:"events"`
	Checkpoint  *Checkpoint `json:"checkpoint,omitempty"`
}

func NewBatch(scope Scope, events []Event) (Batch, error) {
	return newBatch(scope, events, nil)
}

func NewResumeBatch(scope Scope, events []Event, from, to string) (Batch, error) {
	return newBatch(scope, events, &Resume{From: from, To: to})
}

func newBatch(scope Scope, events []Event, resume *Resume) (Batch, error) {
	batch := Batch{Version: Version, ScopeSHA256: scope.Key(), Generation: scope.Generation, Events: events}
	if events == nil {
		batch.Events = []Event{}
	}
	if len(events) > 0 || resume != nil {
		batch.Checkpoint = &Checkpoint{Version: Version, ScopeSHA256: scope.Key(), Generation: scope.Generation, Resume: resume, Entries: []Entry{}}
		for _, event := range events {
			digest, err := event.Digest()
			if err != nil {
				return Batch{}, err
			}
			batch.Checkpoint.Entries = append(batch.Checkpoint.Entries, Entry{ID: event.ID, SHA256: digest})
		}
	}
	return batch, batch.Validate(scope)
}

func (b Batch) Validate(scope Scope) error {
	if scope.Validate() != nil || b.Version != Version || b.ScopeSHA256 != scope.Key() || b.Generation != scope.Generation || b.Events == nil || len(b.Events) > MaxEvents {
		return ErrInvalid
	}
	if len(b.Events) == 0 {
		if b.Checkpoint != nil && (b.Checkpoint.Resume == nil || len(b.Checkpoint.Entries) != 0 || b.Checkpoint.Validate(scope) != nil) {
			return ErrInvalid
		}
	} else {
		if b.Checkpoint == nil || b.Checkpoint.Validate(scope) != nil || len(b.Checkpoint.Entries) != len(b.Events) {
			return ErrInvalid
		}
		for i, event := range b.Events {
			digest, err := event.Digest()
			if err != nil || b.Checkpoint.Entries[i] != (Entry{ID: event.ID, SHA256: digest}) {
				return ErrInvalid
			}
		}
	}
	raw, err := json.Marshal(b)
	if err != nil || len(raw) > MaxBatchBytes {
		return ErrLimit
	}
	return nil
}

func ParseBatch(raw []byte, scope Scope) (Batch, error) {
	var batch Batch
	if operations.DecodeStrict(raw, &batch, MaxBatchBytes) != nil {
		return Batch{}, ErrInvalid
	}
	return batch, batch.Validate(scope)
}
