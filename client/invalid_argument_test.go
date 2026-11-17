// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package client

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestResponseFailureKeepsGatewayInvalidArgumentMessage(t *testing.T) {
	const sqlErr = "PostgreSQL error 42883: operator does not exist: text = uuid (at character 104)"
	for name, tc := range map[string]struct {
		status  int
		body    string
		code    string
		message string
	}{
		"statement error": {400, `{"error":{"code":"INVALID_ARGUMENT","message":"` + sqlErr + `"}}`, "INVALID_ARGUMENT", sqlErr},
		"not envelope":    {400, `oops`, "INVALID_ARGUMENT", "Invalid Kelvo query request"},
		"other code":      {400, `{"error":{"code":"QUERY_FAILED","message":"private"}}`, "INVALID_ARGUMENT", "Invalid Kelvo query request"},
		"unavailable":     {503, `{"error":{"code":"INVALID_ARGUMENT","message":"private"}}`, "UNAVAILABLE", "Kelvo service unavailable"},
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			response, err := http.Get(server.URL)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			var got *Error
			if !errors.As(responseFailure(response), &got) || got.Code != tc.code || got.Message != tc.message {
				t.Fatalf("got %+v", got)
			}
		})
	}
}
