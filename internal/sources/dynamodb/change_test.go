// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package dynamodb

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
)

func TestChangePartiQLAcknowledgementNeverContinuesOrReplays(t *testing.T) {
	for _, response := range []string{`{}`, `{"Items":[]}`, `{"NextToken":"next"}`, `{"LastEvaluatedKey":{"id":{"N":"1"}}}`, `{"Items":[{"id":{"N":"1"}}]}`, `null`, `lost`} {
		t.Run(response, func(t *testing.T) {
			var calls atomic.Int32
			e := setup(t, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				var body map[string]any
				if json.NewDecoder(r.Body).Decode(&body) != nil || body["Statement"] != `UPDATE "facts" SET active=true WHERE id=1` || len(body) != 2 || body["ReturnConsumedCapacity"] != "NONE" || r.Header.Get("X-Amz-Target") != "DynamoDB_20120810.ExecuteStatement" {
					t.Error("unexpected PartiQL write request")
				}
				if response == "lost" {
					w.Header().Set("Content-Length", "10000")
					fmt.Fprint(w, `{`)
					return
				}
				fmt.Fprint(w, response)
			})
			count, err := e.ApplyStatement(context.Background(), `UPDATE "facts" SET active=true WHERE id=1`)
			wantSuccess := response == `{}` || response == `{"Items":[]}`
			if (err == nil) != wantSuccess || count != nil || calls.Load() != 1 {
				t.Fatalf("count=%v err=%v calls=%d", count, err, calls.Load())
			}
		})
	}
}

func TestChangeInvalidAndCancelledDoNotDispatch(t *testing.T) {
	var calls atomic.Int32
	e := setup(t, func(w http.ResponseWriter, r *http.Request) { calls.Add(1); fmt.Fprint(w, `{}`) })
	for _, sql := range []string{"SELECT * FROM facts", "CREATE TABLE facts (id INT)", "INSERT " + strings.Repeat("x", 8192)} {
		if _, err := e.ApplyStatement(context.Background(), sql); err == nil {
			t.Fatal("invalid write accepted")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := e.ApplyStatement(ctx, "DELETE FROM facts WHERE id=1"); err == nil {
		t.Fatal("cancelled write accepted")
	}
	if calls.Load() != 0 {
		t.Fatal("invalid write dispatched")
	}
}
