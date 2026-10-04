// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cluster

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type metadataPhase uint8

const (
	metadataBucketCreate metadataPhase = iota + 1
	metadataValueRead
	metadataBucketReopen
	metadataValueCreate
)

type metadataDiagnostic struct {
	Schema    int    `json:"schema"`
	Phase     string `json:"phase"`
	Class     string `json:"class"`
	APIStatus int    `json:"api_status"`
	APICode   uint16 `json:"api_code"`
}

// Keep the established public error and its matching behavior. The provider
// error is classified and discarded, never retained or exposed by Unwrap.
type metadataUnavailableError struct{ diagnostic metadataDiagnostic }

func (*metadataUnavailableError) Error() string { return "cluster metadata unavailable" }

func metadataUnavailable(phase metadataPhase, cause error) error {
	d := metadataDiagnostic{Schema: 1, Phase: "unknown", Class: "other"}
	switch phase {
	case metadataBucketCreate:
		d.Phase = "bucket_create"
	case metadataValueRead:
		d.Phase = "value_read"
	case metadataBucketReopen:
		d.Phase = "bucket_reopen"
	case metadataValueCreate:
		d.Phase = "value_create"
	}
	var api *jetstream.APIError
	if errors.As(cause, &api) {
		if api != nil {
			d.Class = "api_error"
			// ErrorCode is a uint16 in the pinned client. Status is supplied by
			// the server as an int; do not emit arbitrary or invalid integers.
			d.APICode = uint16(api.ErrorCode)
			if api.Code >= 400 && api.Code <= 599 {
				d.APIStatus = api.Code
			}
		}
	} else {
		switch {
		case errors.Is(cause, context.Canceled):
			d.Class = "context_canceled"
		case errors.Is(cause, context.DeadlineExceeded):
			d.Class = "deadline_exceeded"
		case errors.Is(cause, nats.ErrTimeout):
			d.Class = "timeout"
		case errors.Is(cause, nats.ErrNoResponders):
			d.Class = "no_responders"
		case errors.Is(cause, nats.ErrPermissionViolation):
			d.Class = "permission_denied"
		case errors.Is(cause, nats.ErrAuthorization):
			d.Class = "authorization"
		case errors.Is(cause, nats.ErrConnectionClosed):
			d.Class = "connection_closed"
		}
	}
	return &metadataUnavailableError{diagnostic: d}
}

// StoreMetadataDiagnostic returns a bounded JSON diagnostic for the metadata
// operations which share the public "cluster metadata unavailable" error.
// Only fixed phases/classes and API integers are retained. No provider text,
// subjects, configuration, tenant names or credentials are included. Callers
// may log this separately while preserving their original terminal error.
func StoreMetadataDiagnostic(err error) (string, bool) {
	var unavailable *metadataUnavailableError
	if !errors.As(err, &unavailable) || unavailable == nil {
		return "", false
	}
	raw, err := json.Marshal(unavailable.diagnostic)
	return string(raw), err == nil
}

// This is the existing OpenStore metadata sequence, with diagnostics attached
// only at its four previously indistinguishable error returns. It adds no
// requests, retries, deadline, permission or readiness changes.
func openClusterMetadata(ctx context.Context, js jetstream.JetStream, p Policy, initialize bool) (jetstream.KeyValue, error) {
	raw, _ := json.Marshal(p)
	meta, err := js.KeyValue(ctx, metaBucket)
	if err != nil {
		if !initialize {
			return nil, errors.New("cluster metadata is missing")
		}
		meta, err = js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: metaBucket, History: 1, MaxBytes: 1 << 20, Replicas: p.Replicas})
		if err != nil {
			return nil, metadataUnavailable(metadataBucketCreate, err)
		}
	}
	// A non-initializer must reject direct KV reads before it can read metadata.
	// Only the initializer can make the compatible direct-access update.
	if !initialize {
		if err := configureKVDirect(ctx, js, metaBucket, false); err != nil {
			return nil, err
		}
	}
	entry, err := meta.Get(ctx, metadataKey)
	missingMetadata := errors.Is(err, jetstream.ErrKeyNotFound)
	if err != nil && !missingMetadata {
		return nil, metadataUnavailable(metadataValueRead, err)
	}
	if !missingMetadata && string(entry.Value()) != string(raw) {
		return nil, errors.New("cluster metadata mismatch")
	}
	if missingMetadata && !initialize {
		return nil, errors.New("cluster metadata is missing")
	}
	if initialize {
		if err := configureKVDirect(ctx, js, metaBucket, true); err != nil {
			return nil, err
		}
		meta, err = js.KeyValue(ctx, metaBucket) // refresh cached direct-read behavior
		if err != nil {
			return nil, metadataUnavailable(metadataBucketReopen, err)
		}
		if missingMetadata {
			if _, err = meta.Create(ctx, metadataKey, raw); err != nil {
				return nil, metadataUnavailable(metadataValueCreate, err)
			}
		}
	}
	return meta, nil
}
