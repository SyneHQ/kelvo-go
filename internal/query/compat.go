// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Package query preserves runtime imports of the public query contract.
package query

import (
	publicquery "github.com/SYNEHQ/kelvo-go/query"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

type Request = publicquery.Request
type MongoRequest = publicquery.MongoRequest
type Parameter = publicquery.Parameter
type Limits = publicquery.Limits
type FederationScan = publicquery.FederationScan
type Stats = publicquery.Stats
type AccelerationVersion = publicquery.AccelerationVersion
type Sink = publicquery.Sink
type Executor = publicquery.Executor
type Error = publicquery.Error

func DefaultLimits() Limits { return publicquery.DefaultLimits() }

func ValidateRequest(r Request) error { return publicquery.ValidateRequest(r) }

func NewError(code, message string) error { return publicquery.NewError(code, message) }

func PublicError(err error) *Error { return publicquery.PublicError(err) }

func ResultIPCOptions(compression string) ([]ipc.Option, error) {
	return publicquery.ResultIPCOptions(compression)
}
