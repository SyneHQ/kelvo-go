// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package adapter

import (
	"context"

	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/SYNEHQ/kelvo-go/watch"
)

// WatchSession performs bounded calls; credentials and source handles never
// leave the adapter. Ack follows durable sink capture, not network delivery.
type WatchSession interface {
	Session
	InstallWatch(context.Context, watch.Scope) error
	ReadWatch(context.Context, watch.Scope, int, int, int64) (watch.Batch, error)
	AckWatch(context.Context, watch.Scope, watch.Checkpoint, string) error
	RemoveWatch(context.Context, watch.Scope) error
}

// WatchImportSession is separate so adapters cannot silently ignore a supplied
// legacy resume position and start watching from the present.
type WatchImportSession interface {
	Session
	ImportWatch(context.Context, watch.Scope, watch.Import) error
}

func (r ProcessRequest) WatchScope() (watch.Scope, error) {
	v := r.Request.Spec.Watch
	if v == nil || v.Mode != "native" || v.Target.Catalog != "" && v.Target.Catalog != r.Request.Connection.Database {
		return watch.Scope{}, ErrUnsupported
	}
	if v.Target.Schema != "" && v.Target.Schema != r.Request.Connection.Schema {
		return watch.Scope{}, ErrInvalid
	}
	scope := watch.Scope{TeamID: r.AppTeam, ConnectionID: r.Request.Connection.ID, Database: r.Request.Connection.Database,
		Schema: r.Request.Connection.Schema, Table: v.Target.Name, ID: v.ID, Generation: v.Generation}
	if scope.Validate() != nil {
		return watch.Scope{}, ErrInvalid
	}
	if r.Request.Kind == operations.WatchRead && v.MaxEvents > watch.MaxEvents {
		return watch.Scope{}, ErrLimit
	}
	return scope, nil
}
