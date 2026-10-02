//go:build !linux && !darwin

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package secrets

import "context"

func supported() error                                        { return ErrUnsupported }
func readPrivateFile(context.Context, string) ([]byte, error) { return nil, ErrUnsupported }
