// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
)

func validateOperationNativeReaderCatalog(response operationResolution, request operations.Request) error {
	s := response.Source
	if s.ID != "source_1" || !adapter.NativeReader(s.Type) || s.Adapter != "" || s.Path != "" || s.DSNEnv != "" || s.Federation != nil || s.LocalSnapshot != nil || s.ObjectSnapshot != nil || s.ParquetPaths != nil || s.Ranges != nil || s.Object != nil || s.Range != nil {
		return connectionUnavailable()
	}
	refs := map[string]string{s.URLEnv: "KELVO_SOURCE_REQUEST_0_URL"}
	if s.UsernameEnv != "" {
		refs[s.UsernameEnv] = "KELVO_SOURCE_REQUEST_0_USERNAME"
	}
	if s.PasswordEnv != "" {
		refs[s.PasswordEnv] = "KELVO_SOURCE_REQUEST_0_PASSWORD"
	}
	if s.TokenEnv != "" {
		refs[s.TokenEnv] = "KELVO_SOURCE_REQUEST_0_TOKEN"
	}
	if len(refs) != len(response.Secrets) {
		return connectionUnavailable()
	}
	for ref, expected := range refs {
		if ref != expected || response.Secrets[ref] == "" {
			return connectionUnavailable()
		}
	}
	if validateOperationTLSOptions(s.Options) != nil {
		return connectionUnavailable()
	}
	spec := adapter.ConnectionSpec{Engine: s.Type, URL: response.Secrets[s.URLEnv], Username: response.Secrets[s.UsernameEnv], Password: response.Secrets[s.PasswordEnv], Token: response.Secrets[s.TokenEnv], Database: request.Connection.Database, Schema: request.Connection.Schema, Options: s.Options}
	if adapter.ValidateNativeReaderProcessSource(spec) != nil || adapter.NativeReaderCapabilities(s.Type).Supports(request) != nil {
		return connectionUnavailable()
	}
	return nil
}
