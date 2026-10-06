// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package adapter

import (
	"context"

	"github.com/SYNEHQ/kelvo-go/operations"
)

// Native preserves the signed operation kind. Drivers must enforce a bounded
// provider command vocabulary; NativeRead must reject every mutation itself.
// NativeExecute receives a sink only when ReturnResult is requested. Its source
// effect must survive errors while encoding or delivering that result.
type Native struct {
	Kind   operations.Kind
	Spec   operations.NativeSpec
	Limits Limits
}

type NativeResult struct {
	Stats        QueryStats
	Outcome      operations.Outcome
	Effect       operations.Effect
	AffectedRows *int64
}

type NativeSession interface {
	Session
	RunNative(context.Context, Native, Sink) (NativeResult, error)
}

func (n Native) Validate() error {
	if n.Kind != operations.NativeRead && n.Kind != operations.NativeExecute {
		return ErrInvalid
	}
	request := operations.Request{Version: operations.Version, Kind: n.Kind,
		Connection: operations.ConnectionRef{ID: "validation"}, Spec: operations.Spec{Native: &n.Spec}}
	if n.Kind.Mutating() {
		request.IdempotencyKey = "validation"
	}
	if request.Validate() != nil || n.Spec.Input != nil {
		return ErrInvalid
	}
	return (Query{Statement: "native", MaxRows: n.Limits.MaxRows, MaxBytes: n.Limits.MaxBytes, BatchRows: n.Limits.BatchRows}).Validate()
}
