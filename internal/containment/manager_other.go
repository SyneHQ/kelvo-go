//go:build !linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package containment

import (
	"context"
	"os/exec"
)

type Manager struct{}
type Job struct{}

func (*Manager) QuarantineOperation() {}

func Open(Config) (*Manager, error)                   { return nil, ErrUnsupported }
func (*Manager) SetOnQuarantine(func(error))          {}
func (*Manager) Healthy() bool                        { return false }
func (*Manager) Err() error                           { return ErrUnsupported }
func (*Manager) Status() Status                       { return Status{Draining: true, Closed: true} }
func (*Manager) Prepare(Limits, func()) (*Job, error) { return nil, ErrUnsupported }
func (*Manager) Close(context.Context) error          { return nil }
func (*Job) Start(*exec.Cmd) error                    { return ErrUnsupported }
func (*Job) Usage() (Usage, error)                    { return Usage{}, ErrUnsupported }
func (*Job) Finish(context.Context) (Usage, error)    { return Usage{}, ErrUnsupported }
