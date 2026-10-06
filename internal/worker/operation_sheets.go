// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
)

func validateOperationSheetsCatalog(response operationResolution, request operations.Request) error {
	s := response.Source
	if s.ID != "source_1" || s.Type != "google_sheets" || s.Adapter != "" || s.Path != "" || s.DSNEnv != "" || s.URLEnv != "" || s.UsernameEnv != "" || s.PasswordEnv != "" || s.TokenEnv != "KELVO_SOURCE_REQUEST_0_TOKEN" || len(response.Secrets) != 1 || s.Federation != nil || s.LocalSnapshot != nil || s.ObjectSnapshot != nil || s.ParquetPaths != nil || s.Ranges != nil || s.Object != nil || s.Range != nil {
		return connectionUnavailable()
	}
	switch request.Kind {
	case operations.ConnectionTest, operations.QueryRead, operations.MetadataInspect:
	default:
		return connectionUnavailable()
	}
	if adapter.ValidateSheetsProcessSource(adapter.ConnectionSpec{Engine: s.Type, Token: response.Secrets[s.TokenEnv], Database: request.Connection.Database, Schema: request.Connection.Schema, Options: s.Options}) != nil {
		return connectionUnavailable()
	}
	return nil
}
