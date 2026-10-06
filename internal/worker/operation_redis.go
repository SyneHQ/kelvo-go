// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
)

func validateOperationRedisCatalog(response operationResolution, request operations.Request) error {
	s := response.Source
	if s.ID != "source_1" || s.Type != "redis" || s.Adapter != "" || s.Path != "" || s.DSNEnv != "" || s.TokenEnv != "" || s.URLEnv != "KELVO_SOURCE_REQUEST_0_URL" || s.PasswordEnv != "KELVO_SOURCE_REQUEST_0_PASSWORD" || s.Federation != nil || s.LocalSnapshot != nil || s.ObjectSnapshot != nil || s.ParquetPaths != nil || s.Ranges != nil || s.Object != nil || s.Range != nil {
		return connectionUnavailable()
	}
	switch request.Kind {
	case operations.ConnectionTest, operations.MetadataInspect, operations.NativeRead, operations.NativeExecute:
	default:
		return connectionUnavailable()
	}
	refs := []string{s.URLEnv, s.PasswordEnv}
	if s.UsernameEnv != "" {
		if s.UsernameEnv != "KELVO_SOURCE_REQUEST_0_USERNAME" {
			return connectionUnavailable()
		}
		refs = append(refs, s.UsernameEnv)
	}
	if len(refs) != len(response.Secrets) {
		return connectionUnavailable()
	}
	for _, name := range refs {
		if response.Secrets[name] == "" {
			return connectionUnavailable()
		}
	}
	if adapter.ValidateRedisProcessSource(adapter.ConnectionSpec{Engine: s.Type, URL: response.Secrets[s.URLEnv], Username: response.Secrets[s.UsernameEnv], Password: response.Secrets[s.PasswordEnv], Database: request.Connection.Database, Schema: request.Connection.Schema, Options: s.Options}) != nil {
		return connectionUnavailable()
	}
	return nil
}
