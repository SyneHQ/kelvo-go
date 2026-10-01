// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
// Package sqlserver executes native read-only SQL Server queries.
package sqlserver

import (
	"crypto/tls"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/internal/sources/sqlnative"
	_ "github.com/microsoft/go-mssqldb"
	"github.com/microsoft/go-mssqldb/msdsn"
)

type Engine = sqlnative.Engine

func New(c catalog.Config, l query.Limits) (*Engine, error) {
	return sqlnative.New(c, l, sqlnative.Dialect{SourceType: "sqlserver", DriverName: "sqlserver", ValidateDSN: validateDSN})
}

func validateDSN(dsn string) error {
	c, err := msdsn.Parse(dsn)
	if err != nil || (c.Encryption != msdsn.EncryptionRequired && c.Encryption != msdsn.EncryptionStrict) || c.TLSConfig == nil || c.TLSConfig.InsecureSkipVerify || (c.TLSConfig.MinVersion != 0 && c.TLSConfig.MinVersion < tls.VersionTLS12) {
		return query.NewError("CONFIGURATION_ERROR", "SQL Server requires verified TLS 1.2 or newer with encrypt=true or strict")
	}
	return nil
}
