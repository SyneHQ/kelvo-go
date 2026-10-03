// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package audit

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"io"
	"math"
	"reflect"
	"time"
)

const (
	headerFrame = 1
	startFrame  = 2
	finishFrame = 3
	framePrefix = 64
)

var frameMagic = [8]byte{'K', 'E', 'L', 'V', 'O', 'A', 'U', 'D'}

type journalHeader struct {
	Version int    `json:"version"`
	ID      string `json:"id"`
	Config  Config `json:"config"`
	Scope   Scope  `json:"scope"`
}

type startRecord struct {
	ID        string  `json:"id"`
	Binding   Binding `json:"binding"`
	Kind      Kind    `json:"kind"`
	StartedAt int64   `json:"started_at_ns"`
}

type finishRecord struct {
	ID          string   `json:"id"`
	StartSHA256 string   `json:"start_sha256"`
	FinishedAt  int64    `json:"finished_at_ns"`
	Outcome     Outcome  `json:"outcome"`
	Category    Category `json:"category"`
}

func frameDigest(frame []byte, payloadSize int) [32]byte {
	h := sha256.New()
	_, _ = h.Write(frame[:32])
	_, _ = h.Write(frame[framePrefix : framePrefix+payloadSize])
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

func encodeFrame(tag uint16, id string, payload any, size int) ([]byte, error) {
	if !hexID(id, 32) || size < framePrefix {
		return nil, ErrInvalid
	}
	raw, err := json.Marshal(payload)
	if err != nil || len(raw) > size-framePrefix {
		return nil, ErrInvalid
	}
	frame := make([]byte, size)
	copy(frame, frameMagic[:])
	binary.LittleEndian.PutUint16(frame[8:10], 1)
	binary.LittleEndian.PutUint16(frame[10:12], tag)
	binary.LittleEndian.PutUint32(frame[12:16], uint32(len(raw)))
	_, _ = hex.Decode(frame[16:32], []byte(id))
	copy(frame[framePrefix:], raw)
	digest := frameDigest(frame, len(raw))
	copy(frame[32:64], digest[:])
	return frame, nil
}

// Canonical re-encoding rejects duplicates, unknown fields, alternate numeric
// encodings and trailing data. All input buffers have fixed on-disk bounds.
func decodeFrame(frame []byte, tag uint16, out any) (string, error) {
	if len(frame) < framePrefix || !bytes.Equal(frame[:8], frameMagic[:]) || binary.LittleEndian.Uint16(frame[8:10]) != 1 || binary.LittleEndian.Uint16(frame[10:12]) != tag {
		return "", ErrCorrupt
	}
	length := uint64(binary.LittleEndian.Uint32(frame[12:16]))
	if length == 0 || length > uint64(len(frame)-framePrefix) {
		return "", ErrCorrupt
	}
	size := int(length)
	digest := frameDigest(frame, size)
	if !bytes.Equal(frame[32:64], digest[:]) || !zero(frame[framePrefix+size:]) {
		return "", ErrCorrupt
	}
	raw := frame[framePrefix : framePrefix+size]
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(out) != nil || decoder.Decode(new(any)) != io.EOF {
		return "", ErrCorrupt
	}
	canonical, err := json.Marshal(out)
	if err != nil || !bytes.Equal(canonical, raw) {
		return "", ErrCorrupt
	}
	return hex.EncodeToString(frame[16:32]), nil
}

func zero(raw []byte) bool {
	for _, b := range raw {
		if b != 0 {
			return false
		}
	}
	return true
}

func decodeHeader(frame []byte, directory string) (journalHeader, error) {
	var h journalHeader
	id, err := decodeFrame(frame, headerFrame, &h)
	h.Config.Directory = directory
	scope, scopeErr := normalizedScope(h.Scope)
	if err != nil || h.Version != 1 || h.ID != id || h.Config.Validate() != nil || scopeErr != nil || !reflect.DeepEqual(scope, h.Scope) {
		return journalHeader{}, ErrCorrupt
	}
	return h, nil
}

func decodeSlot(raw []byte, scope Scope, retention time.Duration) (startRecord, *finishRecord, error) {
	var start startRecord
	if len(raw) != slotSize {
		return start, nil, ErrCorrupt
	}
	if zero(raw) {
		return start, nil, nil
	}
	id, err := decodeFrame(raw[:frameSize], startFrame, &start)
	if err != nil || start.ID != id || start.StartedAt <= 0 || start.StartedAt > math.MaxInt64-int64(retention) || validateBinding(scope, start.Binding, start.Kind) != nil {
		return startRecord{}, nil, ErrCorrupt
	}
	if zero(raw[frameSize:]) {
		return start, nil, nil
	}
	var finish finishRecord
	finishID, err := decodeFrame(raw[frameSize:], finishFrame, &finish)
	startHash := sha256.Sum256(raw[:frameSize])
	if err != nil || finishID != start.ID || finish.ID != start.ID || finish.StartSHA256 != hex.EncodeToString(startHash[:]) || !validOutcome(finish.Outcome, finish.Category) || finish.FinishedAt < start.StartedAt || finish.FinishedAt > math.MaxInt64-int64(retention) {
		return startRecord{}, nil, ErrCorrupt
	}
	return start, &finish, nil
}

func eventFrom(start startRecord, finish *finishRecord) Event {
	e := Event{ID: start.ID, Binding: start.Binding, Kind: start.Kind, StartedAt: time.Unix(0, start.StartedAt).UTC(), Outcome: Unknown, Category: Interrupted}
	if finish != nil {
		e.FinishedAt = time.Unix(0, finish.FinishedAt).UTC()
		e.Outcome = finish.Outcome
		e.Category = finish.Category
	}
	return e
}
