// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Package oracle executes native read-only Oracle queries through the pure-Go driver.
package oracle

import (
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/sources/sqlnative"
	_ "github.com/sijms/go-ora/v2"
	"github.com/sijms/go-ora/v2/configurations"
	"strings"
)

type Engine = sqlnative.Engine

func New(c catalog.Config, l query.Limits) (*Engine, error) {
	return sqlnative.New(c, l, sqlnative.Dialect{SourceType: "oracle", DriverName: "oracle", ReadOnlySession: "SET TRANSACTION READ ONLY", ReadOnlyOption: false, ValidateDSN: validateDSN})
}

func validateDSN(dsn string) error {
	c, err := configurations.ParseConfig(dsn)
	deny := func() error {
		return query.NewError("CONFIGURATION_ERROR", "Oracle requires TCPS with SSL verification enabled")
	}
	if err != nil || !c.SSL || !c.SSLVerify {
		return deny()
	}
	for _, s := range c.Servers {
		if s.Protocol != "" && !strings.EqualFold(s.Protocol, "tcps") {
			return deny()
		}
	}
	return nil
}
