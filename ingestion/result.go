// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package ingestion

import (
	"bytes"
	"encoding/json"
)

const MaxResultBytes = MaxCheckpointBytes + 4096

func (r Receipt) Validate() error {
	if !textValid(r.BatchID, 128) || !digest.MatchString(r.Digest) || r.Sequence < 1 || r.Records < 0 || r.Records > MaxRecords || r.CommittedAt.IsZero() {
		return ErrInvalid
	}
	return nil
}

func (r Receipt) ValidateBatch(batch Batch) error {
	hash, err := batch.Digest()
	if err != nil || r.Validate() != nil || r.BatchID != batch.ID || r.Digest != hash || r.Sequence != batch.ExpectedSequence+1 || r.Records != len(batch.Records) {
		return ErrInvalid
	}
	return nil
}

func (s State) Validate() error {
	if s.Sequence < 0 || len(s.Checkpoint) > MaxCheckpointBytes || !ValidObject(s.Checkpoint) {
		return ErrInvalid
	}
	if s.Sequence == 0 {
		if s.LastReceipt != nil {
			return ErrInvalid
		}
		return nil
	}
	if s.LastReceipt == nil || s.LastReceipt.Validate() != nil || s.LastReceipt.Sequence != s.Sequence {
		return ErrInvalid
	}
	return nil
}

func ParseState(raw []byte) (State, error) {
	var state State
	if decodeResult(raw, &state) != nil || state.Validate() != nil {
		return State{}, ErrInvalid
	}
	return state, nil
}

func ParseReceipt(raw []byte) (Receipt, error) {
	var receipt Receipt
	if decodeResult(raw, &receipt) != nil || receipt.Validate() != nil {
		return Receipt{}, ErrInvalid
	}
	return receipt, nil
}

func decodeResult(raw []byte, target any) error {
	if len(raw) > MaxResultBytes || !ValidObject(raw) {
		return ErrInvalid
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	return decoder.Decode(target)
}
