// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/resolver"
	"github.com/SYNEHQ/kelvo-go/sourceproof"
)

func TestPrivateProofNeverEntersDirectQuery(t *testing.T) {
	request, execution, response := connectionFixture(t)
	proof := sourceproof.Envelope{Grant: "private-original-grant", Token: "private-source-proof"}
	callback := fixtureConnectionResolver(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resolver.QueryResponse{Version: response.Version, DelegationSHA256: response.DelegationSHA256, ValidUntil: response.ValidUntil,
			Sources: []resolver.Source{{ID: "selected", Type: "postgres", DSNEnv: "KELVO_SOURCE_REQUEST_0_DSN"}}, Secrets: response.Secrets, PrivateSources: map[string]sourceproof.Envelope{"selected": proof}})
	})
	executor := &Executor{connectionResolvers: map[string]*ConnectionResolver{"gateway": callback}}
	resolved, _, err := executor.resolveConnections(connectionTestContext(execution), execution, request)
	if err == nil || resolved != nil {
		t.Fatal("private source became a direct query catalog")
	}
	if strings.Contains(err.Error(), proof.Grant) || strings.Contains(err.Error(), proof.Token) {
		t.Fatal("error disclosed private proof")
	}
}

func TestPrivateProofNeverEntersOperationChild(t *testing.T) {
	record, request, response := operationResolverFixture(t)
	proof := sourceproof.Envelope{Grant: "private-original-grant", Token: "private-source-proof"}
	callback := fixtureConnectionResolver(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resolver.OperationResponse{Version: response.Version, GrantSHA256: response.GrantSHA256, RequestSHA256: response.RequestSHA256, SourceRevision: response.SourceRevision, ValidUntil: response.ValidUntil,
			Source: resolver.Source{ID: "source_1", Type: "postgres", DSNEnv: "KELVO_SOURCE_REQUEST_0_DSN"}, Secrets: response.Secrets, PrivateSource: &proof})
	})
	callback.url += "/internal/kelvo/resolve"
	executor := &Executor{Limits: query.DefaultLimits(), connectionResolvers: map[string]*ConnectionResolver{"gateway": callback}}
	child, err := executor.ResolveOperationSource(context.Background(), record, request)
	if err == nil {
		t.Fatal("private source became a direct operation")
	}
	raw, _ := json.Marshal(child)
	if strings.Contains(string(raw), proof.Grant) || strings.Contains(string(raw), proof.Token) || child.Source.DSN != "" {
		t.Fatal("private authority entered child input")
	}
	if strings.Contains(err.Error(), proof.Grant) || strings.Contains(err.Error(), proof.Token) {
		t.Fatal("error disclosed private proof")
	}
}
