// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package cosmosdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/internal/query"
)

func partitionResponse(w http.ResponseWriter, paths string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("x-ms-request-charge", "1")
	kind := "Hash"
	if strings.Contains(paths, ",") {
		kind = "MultiHash"
	}
	fmt.Fprintf(w, `{"id":"Container","partitionKey":{"kind":%q,"paths":%s}}`, kind, paths)
}

func TestCosmosSQLPointWritesUseBoundRESTAndExactValues(t *testing.T) {
	for _, auth := range []string{"aad", "master_key"} {
		t.Run(auth, func(t *testing.T) {
			var writes, metadata atomic.Int32
			engine := setup(t, auth, func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("x-ms-documentdb-isquery") != "" || r.URL.RawQuery != "" {
					t.Error("mutation sent as SQL or used unbound URL")
				}
				resource, kind := "dbs/DbCase/colls/Container", "colls"
				if r.Method == http.MethodGet {
					metadata.Add(1)
					if r.URL.Path != "/"+resource {
						t.Error("partition lookup escaped saved container")
					}
				} else {
					kind = "docs"
					if r.Method == http.MethodPatch || r.Method == http.MethodDelete {
						resource += "/docs/one"
					}
				}
				expected := url.QueryEscape("type=aad&ver=1.0&sig=fixture-aad-token")
				if auth == "master_key" {
					expected = masterAuthorization(r.Method, kind, resource, r.Header.Get("x-ms-date"), []byte(strings.Repeat("k", 64)))
				}
				if r.Header.Get("Authorization") != expected {
					t.Error("mutation authorization differs from selected resource")
				}
				if r.Method == http.MethodGet {
					partitionResponse(w, `["/tenant"]`)
					return
				}
				call := writes.Add(1)
				if r.Header.Get("x-ms-documentdb-partitionkey") != `["team-a"]` || r.Header.Get("x-ms-documentdb-contentresponse-on-write") != "false" {
					t.Error("wrong partition or response-body policy")
				}
				body, _ := io.ReadAll(r.Body)
				switch call {
				case 1, 2:
					if r.Method != http.MethodPost || r.URL.Path != "/dbs/DbCase/colls/Container/docs" {
						t.Error("wrong document create method/path")
					}
					if !strings.Contains(string(body), `9007199254740993`) || !strings.Contains(string(body), `123456789012345678901.23456789`) || !strings.Contains(string(body), `"id":"one"`) {
						t.Errorf("document precision or identity lost: %s", body)
					}
					if call == 1 && r.Header.Get("x-ms-documentdb-is-upsert") != "" || call == 2 && r.Header.Get("x-ms-documentdb-is-upsert") != "True" {
						t.Error("upsert and insert confused")
					}
					w.WriteHeader(http.StatusCreated)
				case 3:
					if r.Method != http.MethodPatch || r.URL.Path != "/dbs/DbCase/colls/Container/docs/one" || r.Header.Get("Content-Type") != "application/json_patch+json" {
						t.Error("UPDATE did not use atomic point patch")
					}
					var patch struct {
						Operations []struct {
							Op, Path string
							Value    json.RawMessage
						}
					}
					if json.Unmarshal(body, &patch) != nil || len(patch.Operations) != 2 || patch.Operations[0].Op != "set" || patch.Operations[0].Path != "/CaseSensitive" || string(patch.Operations[0].Value) != "9007199254740993" || string(patch.Operations[1].Value) != `{"MixedCase":[true,null]}` {
						t.Errorf("wrong atomic patch: %s", body)
					}
					w.WriteHeader(http.StatusOK)
				case 4:
					if r.Method != http.MethodDelete || r.URL.Path != "/dbs/DbCase/colls/Container/docs/one" || len(body) != 0 {
						t.Error("DELETE was not a point request")
					}
					w.WriteHeader(http.StatusNoContent)
				default:
					t.Error("unexpected mutation retry")
				}
			})
			for _, sql := range []string{
				`INSERT INTO DbCase.Container (id,tenant,big,decimal) VALUES ('one','team-a',9007199254740993,123456789012345678901.23456789) WITH PK=/tenant`,
				`UPSERT INTO Container (id,tenant,big,decimal) VALUES ("\"one\"","\"team-a\"",9007199254740993,123456789012345678901.23456789)`,
				`UPDATE Container SET CaseSensitive=9007199254740993, nested={"MixedCase":[true,null]} WHERE id='one' AND tenant='team-a'`,
				`DELETE FROM Container WHERE tenant='team-a' AND id='one'`,
			} {
				if err := engine.ValidateStatement(sql); err != nil {
					t.Fatal(err)
				}
				count, err := engine.ApplyStatement(context.Background(), sql)
				if err != nil || count == nil || *count != 1 {
					t.Fatalf("count=%v err=%v", count, err)
				}
			}
			if writes.Load() != 4 || metadata.Load() != 4 {
				t.Fatalf("writes=%d metadata=%d", writes.Load(), metadata.Load())
			}
		})
	}
}

func TestCosmosMutationsRejectUnsafeSyntaxBeforeHTTP(t *testing.T) {
	var calls atomic.Int32
	engine := setup(t, "aad", func(w http.ResponseWriter, r *http.Request) { calls.Add(1) })
	for _, sql := range []string{
		`INSERT INTO Other.Container (id) VALUES ('one')`,
		`INSERT INTO Other (id) VALUES ('one')`,
		`INSERT INTO Container (id,id) VALUES ('one','two')`,
		`INSERT INTO Container (id) VALUES ('../escape')`,
		`INSERT INTO Container (id) VALUES (1)`,
		`INSERT INTO Container (id) VALUES (@1)`,
		`INSERT INTO Container (id,object) VALUES ('one',{"x":1,"x":2})`,
		`INSERT INTO Container (id) VALUES ('one'),('two')`,
		`UPDATE Container SET id='two' WHERE id='one'`,
		`UPDATE Container SET _etag='fake' WHERE id='one'`,
		`UPDATE Container SET n=n+1 WHERE id='one'`,
		`UPDATE Container SET n=1 WHERE active=true`,
		`DELETE FROM Container`,
		`DELETE FROM Container WHERE id='one' OR id='two'`,
		`DELETE FROM Container WHERE id='one'; DROP COLLECTION Container`,
		`DROP DATABASE DbCase`,
		`CREATE COLLECTION Other WITH PK=/tenant`,
		`CREATE COLLECTION Container WITH PK=/tenant WITH RU=400 WITH MAXRU=1000`,
		`CREATE COLLECTION Container WITH PK=/tenant,/tenant`,
		`ALTER COLLECTION Container WITH RU=400`,
		`SELECT * FROM c`,
	} {
		if err := engine.ValidateStatement(sql); err == nil {
			t.Fatalf("unsafe mutation validated: %s", sql)
		}
		if _, err := engine.ApplyStatement(context.Background(), sql); err == nil {
			t.Fatalf("unsafe mutation executed: %s", sql)
		}
	}
	if calls.Load() != 0 {
		t.Fatalf("unsafe syntax made %d requests", calls.Load())
	}
}

func TestCosmosPointPredicatesMatchActualPartitionAndNeverIgnoreFilters(t *testing.T) {
	for _, sql := range []string{
		`DELETE FROM Container WHERE id='one'`,
		`DELETE FROM Container WHERE id='one' AND wrong='a'`,
		`DELETE FROM Container WHERE id='one' AND tenant='a' AND active=true`,
		`UPDATE Container SET tenant='b' WHERE id='one' AND tenant='a'`,
		`INSERT INTO Container (id,tenant) VALUES ('one','a') WITH PK=/wrong`,
	} {
		t.Run(sql, func(t *testing.T) {
			var writes atomic.Int32
			engine := setup(t, "aad", func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					writes.Add(1)
				}
				partitionResponse(w, `["/tenant"]`)
			})
			if _, err := engine.ApplyStatement(context.Background(), sql); err == nil || writes.Load() != 0 {
				t.Fatalf("unsafe partition mutation reached server: writes=%d err=%v", writes.Load(), err)
			}
		})
	}
}

func TestCosmosNestedHierarchicalPartitionPreservesScalarValues(t *testing.T) {
	for _, tc := range []struct{ sql, want string }{
		{`INSERT INTO Container (id,tenant,region) VALUES ('one',{"name":"TeamA"},null)`, `["TeamA",null]`},
		{`UPSERT INTO Container (id,tenant) VALUES ('one',{"name":"TeamA"})`, `["TeamA",{}]`},
		{`DELETE FROM Container WHERE id='one' AND tenant.name='TeamA' AND region=9007199254740993`, `["TeamA",9007199254740993]`},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			engine := setup(t, "aad", func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodGet {
					partitionResponse(w, `["/tenant/name","/region"]`)
					return
				}
				if got := r.Header.Get("x-ms-documentdb-partitionkey"); got != tc.want {
					t.Errorf("partition=%s, want %s", got, tc.want)
				}
				if r.Method == http.MethodDelete {
					w.WriteHeader(204)
				} else {
					w.WriteHeader(201)
				}
			})
			if count, err := engine.ApplyStatement(context.Background(), tc.sql); err != nil || count == nil || *count != 1 {
				t.Fatalf("count=%v err=%v", count, err)
			}
		})
	}
}

func TestCosmosScopedDDLAndConditionalNoops(t *testing.T) {
	for _, tc := range []struct {
		sql, method, path, kind, resource string
		status                            int
		count                             int64
		database                          bool
	}{
		{`CREATE COLLECTION Container WITH PK=/tenant WITH RU=400`, "POST", "/dbs/DbCase/colls", "colls", "dbs/DbCase", 201, 1, false},
		{`CREATE TABLE IF NOT EXISTS Container WITH PK=/tenant WITH MAXRU=1000`, "POST", "/dbs/DbCase/colls", "colls", "dbs/DbCase", 409, 0, false},
		{`DROP COLLECTION Container`, "DELETE", "/dbs/DbCase/colls/Container", "colls", "dbs/DbCase/colls/Container", 204, 1, false},
		{`DROP TABLE IF EXISTS Container`, "DELETE", "/dbs/DbCase/colls/Container", "colls", "dbs/DbCase/colls/Container", 404, 0, false},
		{`CREATE DATABASE DbCase WITH RU=400`, "POST", "/dbs", "dbs", "", 201, 1, true},
		{`DROP DATABASE IF EXISTS DbCase`, "DELETE", "/dbs/DbCase", "dbs", "dbs/DbCase", 404, 0, true},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			var calls atomic.Int32
			engine := setup(t, "master_key", func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.Method != tc.method || r.URL.Path != tc.path || r.Header.Get("Authorization") != masterAuthorization(tc.method, tc.kind, tc.resource, r.Header.Get("x-ms-date"), []byte(strings.Repeat("k", 64))) {
					t.Error("DDL escaped selected namespace or signature")
				}
				if strings.HasPrefix(tc.sql, "CREATE") {
					body, _ := io.ReadAll(r.Body)
					if !json.Valid(body) || !tc.database && !strings.Contains(string(body), `"partitionKey":{"kind":"Hash","paths":["/tenant"],"version":2}`) {
						t.Errorf("invalid CREATE body: %s", body)
					}
					if strings.Contains(tc.sql, "MAXRU") && r.Header.Get("x-ms-cosmos-offer-autopilot-settings") != `{"maxThroughput":1000}` || strings.Contains(tc.sql, " RU=") && r.Header.Get("x-ms-offer-throughput") != "400" {
						t.Error("missing throughput setting")
					}
				}
				w.WriteHeader(tc.status)
			})
			if tc.database {
				engine.source.Options["container"] = ""
			}
			count, err := engine.ApplyStatement(context.Background(), tc.sql)
			if err != nil || count == nil || *count != tc.count || calls.Load() != 1 {
				t.Fatalf("count=%v calls=%d err=%v", count, calls.Load(), err)
			}
		})
	}
}

func TestCosmosMutationAcknowledgementsNoReplayAndCancellation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		count  *int64
	}{
		{"throttled", 429, nil}, {"provider_failure", 500, nil}, {"redirect", 307, nil}, {"missing", 404, new(int64)}, {"acknowledged_truncated_body", 204, func() *int64 { n := int64(1); return &n }()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var writes atomic.Int32
			engine := setup(t, "aad", func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodGet {
					partitionResponse(w, `["/id"]`)
					return
				}
				writes.Add(1)
				w.Header().Set("Location", "https://other.invalid/private")
				w.Header().Set("Content-Length", "100000000")
				w.WriteHeader(tc.status)
				fmt.Fprint(w, "private provider details")
			})
			count, err := engine.ApplyStatement(context.Background(), `DELETE FROM Container WHERE id='one'`)
			if writes.Load() != 1 || tc.count == nil && err == nil || tc.count != nil && (err != nil || count == nil || *count != *tc.count) {
				t.Fatalf("count=%v writes=%d err=%v", count, writes.Load(), err)
			}
			if err != nil && strings.Contains(err.Error(), "private") {
				t.Fatal("provider error leaked")
			}
		})
	}
	var writes atomic.Int32
	engine := setup(t, "aad", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			partitionResponse(w, `["/id"]`)
			return
		}
		writes.Add(1)
		select {
		case <-r.Context().Done():
		case <-time.After(time.Second):
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := engine.ApplyStatement(ctx, `DELETE FROM Container WHERE id='one'`); !errors.Is(err, context.DeadlineExceeded) || writes.Load() != 1 {
		t.Fatalf("cancellation/retry err=%v writes=%d", err, writes.Load())
	}
}

func TestCosmosPartitionMetadataBudgetStopsBeforeMutation(t *testing.T) {
	var writes atomic.Int32
	engine := setup(t, "aad", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writes.Add(1)
		}
		w.Header().Set("x-ms-request-charge", "10001")
		fmt.Fprint(w, `{"id":"Container","partitionKey":{"kind":"Hash","paths":["/id"]}}`)
	})
	_, err := engine.ApplyStatement(context.Background(), `DELETE FROM Container WHERE id='one'`)
	if err == nil || query.PublicError(err).Code != "RESOURCE_EXHAUSTED" || writes.Load() != 0 {
		t.Fatalf("budget bypassed: writes=%d err=%v", writes.Load(), err)
	}
}
