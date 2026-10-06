// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package oracle

import (
	"fmt"
	"slices"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
	goora "github.com/sijms/go-ora/v2"
)

func oracleParameters(parameters []operations.Parameter) ([]any, error) {
	for _, parameter := range parameters {
		if !slices.Contains(parameterTypes, parameter.Type) {
			return nil, adapter.ErrUnsupported
		}
	}
	values, err := adapter.SQLParameters(parameters)
	if err != nil {
		return nil, err
	}
	for i, parameter := range parameters {
		if parameter.Type == "decimal128" || parameter.Type == "uint64" {
			// Bind these as Oracle NUMBER, not as VARCHAR requiring contextual
			// casts and not as a binary float that loses large integer precision.
			values[i], err = goora.NewNumberFromString(fmt.Sprint(values[i]))
			if err != nil {
				return nil, adapter.ErrInvalid
			}
		}
	}
	return values, nil
}
