//go:build !duckbridge || !duckdb_arrow || !cgo

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package duckbridge

import (
	"context"
	"database/sql/driver"
	"errors"

	"github.com/apache/arrow-go/v18/arrow"
)

var errUnavailable = errors.New("native federation requires the pinned duckbridge build")

type Factory struct{}

func Available() bool { return false }

func New(context.Context, *arrow.Schema, Producer, PredicateCapabilities) (*Factory, error) {
	return nil, errUnavailable
}
func (*Factory) Register(driver.Conn, string, string) error { return errUnavailable }
func (*Factory) Err() error                                 { return errUnavailable }
func (*Factory) Close()                                     {}
