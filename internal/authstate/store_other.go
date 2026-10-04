//go:build !linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package authstate

import "context"

type Store struct{}

func Initialize(context.Context, string, Scope, Candidate) error { return ErrUnavailable }
func Open(context.Context, string, Scope) (*Store, error)        { return nil, ErrUnavailable }
func (*Store) Admit(context.Context, Candidate) error            { return ErrUnavailable }
func (*Store) Close(context.Context) error                       { return nil }
