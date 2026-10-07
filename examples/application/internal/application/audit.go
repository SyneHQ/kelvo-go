// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package application

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

// Audit is optional acceptance evidence for this example process. Counters contain
// no request identity, grants, SQL, connection names, or credential values.
type Audit struct {
	mu     sync.Mutex
	path   string
	counts AuditCounts
}
type AuditCounts struct {
	Version                      int   `json:"version"`
	QueryAuthorized              int64 `json:"query_authorized"`
	QueryPolicyDenied            int64 `json:"query_policy_denied"`
	QueryAuthorizationFailed     int64 `json:"query_authorization_failed"`
	OperationAuthorized          int64 `json:"operation_authorized"`
	OperationPolicyDenied        int64 `json:"operation_policy_denied"`
	OperationAuthorizationFailed int64 `json:"operation_authorization_failed"`
	CredentialLookups            int64 `json:"credential_lookups"`
	CredentialLookupFailed       int64 `json:"credential_lookup_failed"`
}

func NewAudit(path string) (*Audit, error) {
	if path == "" {
		return nil, nil
	}
	a := &Audit{path: path, counts: AuditCounts{Version: 1}}
	if err := a.record("start"); err != nil {
		return nil, err
	}
	return a, nil
}

func (a *Audit) record(event string) error {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	switch event {
	case "query_authorized":
		a.counts.QueryAuthorized++
	case "query_policy_denied":
		a.counts.QueryPolicyDenied++
	case "query_authorization_failed":
		a.counts.QueryAuthorizationFailed++
	case "operation_authorized":
		a.counts.OperationAuthorized++
	case "operation_policy_denied":
		a.counts.OperationPolicyDenied++
	case "operation_authorization_failed":
		a.counts.OperationAuthorizationFailed++
	case "credentials":
		a.counts.CredentialLookups++
	case "credential_failure":
		a.counts.CredentialLookups++
		a.counts.CredentialLookupFailed++
	}
	raw, err := json.Marshal(a.counts)
	if err != nil {
		return ErrConfiguration
	}
	f, err := os.CreateTemp(filepath.Dir(a.path), ".authority-audit-*")
	if err != nil {
		return ErrConfiguration
	}
	defer os.Remove(f.Name())
	n, writeErr := f.Write(raw)
	syncErr := f.Sync()
	closeErr := f.Close()
	if n != len(raw) || writeErr != nil || syncErr != nil || closeErr != nil || os.Rename(f.Name(), a.path) != nil {
		return ErrConfiguration
	}
	return nil
}
