// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/operations"
	"github.com/SYNEHQ/kelvo-go/provider"
)

// SaaS credentials are resolved for one signed account. No catalog attachment,
// ambient environment name, arbitrary transport option or write is accepted.
func validateOperationSaaSCatalog(response operationResolution, request operations.Request) error {
	s := response.Source
	if !provider.SaaS(s.Type) || (request.Kind != operations.ConnectionTest && request.Kind != operations.NativeRead) || s.ID != "source_1" || s.Adapter != "" || s.Path != "" || s.DSNEnv != "" || s.PasswordEnv != "" || s.TokenEnv != "KELVO_SOURCE_REQUEST_0_TOKEN" || s.Federation != nil || s.LocalSnapshot != nil || s.ObjectSnapshot != nil || s.ParquetPaths != nil || s.Ranges != nil || s.Object != nil || s.Range != nil {
		return connectionUnavailable()
	}
	refs := []string{s.TokenEnv}
	if s.Type == "salesforce" {
		if s.URLEnv != "KELVO_SOURCE_REQUEST_0_URL" {
			return connectionUnavailable()
		}
		refs = append(refs, s.URLEnv)
	} else if s.URLEnv != "" {
		return connectionUnavailable()
	}
	if s.UsernameEnv != "" {
		if (s.Type != "google_ads" && s.Type != "salesforce") || s.UsernameEnv != "KELVO_SOURCE_REQUEST_0_USERNAME" {
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
	if adapter.ValidateSaaSProcessSource(adapter.ConnectionSpec{Engine: s.Type, URL: response.Secrets[s.URLEnv], Username: response.Secrets[s.UsernameEnv], Token: response.Secrets[s.TokenEnv], Database: request.Connection.Database, Schema: request.Connection.Schema, Options: s.Options}) != nil {
		return connectionUnavailable()
	}
	return nil
}
