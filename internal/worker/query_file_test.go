// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/SYNEHQ/kelvo-go/filesnapshot"
	"github.com/SYNEHQ/kelvo-go/internal/catalog"
	"github.com/SYNEHQ/kelvo-go/internal/delegation"
	"github.com/SYNEHQ/kelvo-go/internal/query"
	"github.com/SYNEHQ/kelvo-go/resolver"
)

var queryFileData = []byte("id,amount\n1,10\n")

func queryFileDescriptor() filesnapshot.Descriptor {
	sum := sha256.Sum256(queryFileData)
	return filesnapshot.Descriptor{Version: 1, Format: "csv", Bytes: int64(len(queryFileData)), SHA256: hex.EncodeToString(sum[:])}
}

// queryFileFixture is a federated PostgreSQL table joined with file peer "files".
func queryFileFixture(t *testing.T) (query.Request, delegation.Execution, connectionResolution) {
	t.Helper()
	request, execution, response := connectionFixture(t)
	request.Mode, request.ConnectionID, request.Sources = "federated", "", []string{"selected", "files"}
	execution.Claims.Sources[0].Schema = "public"
	execution.Claims.Sources[0].Tables = []delegation.Table{{Name: "orders", Table: "orders", Schema: "public"}}
	execution.Claims.Sources = append(execution.Claims.Sources, delegation.Source{Alias: "files", ConnectionID: "saved-file", Tables: []delegation.Table{{Name: "data", Table: "data"}}})
	execution.Binding.Owner, execution.Binding.Claim = strings.Repeat("a", 32), strings.Repeat("b", 32)
	response.Sources[0].Federation = &catalog.FederationConfig{Tables: []catalog.FederationTable{{Name: "orders", Table: "orders", Schema: "public"}}}
	response.Sources = append(response.Sources, catalog.Source{ID: "files", Type: "csv", FederatedFile: true, Federation: &catalog.FederationConfig{Tables: []catalog.FederationTable{{Name: "data", Table: "data"}}}})
	response.Files = map[string]filesnapshot.Descriptor{"files": queryFileDescriptor()}
	return request, execution, response
}

func TestConnectionCatalogAcceptsOnlyTheFileSourceShape(t *testing.T) {
	request, execution, response := queryFileFixture(t)
	if err := validateConnectionCatalog(response, execution.Claims, request); err != nil {
		t.Fatal("valid file source rejected:", err)
	}
	for name, edit := range map[string]func(*query.Request, *delegation.Claims, *connectionResolution){
		"native":          func(r *query.Request, _ *delegation.Claims, _ *connectionResolution) { r.Mode = "native" },
		"format mismatch": func(_ *query.Request, _ *delegation.Claims, c *connectionResolution) { c.Sources[1].Type = "parquet" },
		"sqlite": func(_ *query.Request, _ *delegation.Claims, c *connectionResolution) {
			c.Sources[1].Type = "sqlite"
			c.Files["files"] = filesnapshot.Descriptor{Version: 1, Format: "sqlite", Bytes: 1, SHA256: strings.Repeat("a", 64)}
		},
		"path": func(_ *query.Request, _ *delegation.Claims, c *connectionResolution) {
			c.Sources[1].Path = "/etc/passwd"
		},
		"options": func(_ *query.Request, _ *delegation.Claims, c *connectionResolution) {
			c.Sources[1].Options = map[string]string{"a": "b"}
		},
		"no federation": func(_ *query.Request, _ *delegation.Claims, c *connectionResolution) { c.Sources[1].Federation = nil },
		"other table": func(_ *query.Request, _ *delegation.Claims, c *connectionResolution) {
			c.Sources[1].Federation.Tables[0].Table = "secret"
		},
		"schema": func(_ *query.Request, _ *delegation.Claims, c *connectionResolution) {
			c.Sources[1].Federation.Tables[0].Schema = "public"
		},
		"missing file": func(_ *query.Request, _ *delegation.Claims, c *connectionResolution) { delete(c.Files, "files") },
		"extra file": func(_ *query.Request, _ *delegation.Claims, c *connectionResolution) {
			c.Files["selected"] = queryFileDescriptor()
		},
		"bad descriptor": func(_ *query.Request, _ *delegation.Claims, c *connectionResolution) {
			d := c.Files["files"]
			d.SHA256 = "x"
			c.Files["files"] = d
		},
		"signed database": func(_ *query.Request, cl *delegation.Claims, _ *connectionResolution) { cl.Sources[1].Database = "app" },
		"signed table": func(_ *query.Request, cl *delegation.Claims, _ *connectionResolution) {
			cl.Sources[1].Tables = []delegation.Table{{Name: "x", Table: "x"}}
		},
		"credential": func(_ *query.Request, _ *delegation.Claims, c *connectionResolution) {
			c.Sources[1].TokenEnv = "KELVO_SOURCE_REQUEST_1_TOKEN"
			c.Secrets["KELVO_SOURCE_REQUEST_1_TOKEN"] = "t"
		},
		"descriptor alone": func(_ *query.Request, _ *delegation.Claims, c *connectionResolution) {
			c.Sources[1].FederatedFile = false
		},
	} {
		r, e, c := queryFileFixture(t)
		edit(&r, &e.Claims, &c)
		if validateConnectionCatalog(c, e.Claims, r) == nil {
			t.Error("invalid file source accepted:", name)
		}
	}
}

func TestResolvedQueryCarriesFileDescriptorNotPath(t *testing.T) {
	d := queryFileDescriptor()
	out := resolvedQuery(resolver.QueryResponse{Sources: []resolver.Source{{ID: "files", Type: "csv", File: &d, Federation: &resolver.Federation{Tables: []delegation.Table{resolver.QueryFileTable}}}}})
	if !out.Sources[0].FederatedFile || out.Sources[0].Path != "" || out.Files["files"] != d {
		t.Fatalf("file source conversion lost its descriptor: %+v", out)
	}
}

func TestQueryFileSpoolsVerifiedBytesUnderTheQueryBinding(t *testing.T) {
	for _, mode := range []string{"valid", "wrong bytes", "short", "denied"} {
		t.Run(mode, func(t *testing.T) {
			request, execution, _ := queryFileFixture(t)
			request.Delegation = ""
			snapshot := queryFileDescriptor()
			fixture := fixtureConnectionResolver(t, func(w http.ResponseWriter, r *http.Request) {
				var input resolver.QueryFileReadRequest
				if r.URL.Path != resolver.QueryFileReadPath || json.NewDecoder(r.Body).Decode(&input) != nil || input.Delegation != execution.Token || input.JobID != execution.Binding.JobID || input.Owner != execution.Binding.Owner || input.Claim != execution.Binding.Claim || input.Alias != "files" || input.Snapshot != snapshot || input.Query.Mode != "federated" {
					t.Error("query file request lost its binding")
					w.WriteHeader(http.StatusForbidden)
					return
				}
				if mode == "denied" {
					w.WriteHeader(http.StatusForbidden)
					return
				}
				w.Header().Set("Content-Type", "application/octet-stream")
				w.Header().Set(resolver.FileSHA256Header, snapshot.SHA256)
				w.Header().Set("Trailer", resolver.FileVerifiedTrailer+", "+resolver.SourceValidUntilTrailer)
				w.WriteHeader(http.StatusOK)
				w.(http.Flusher).Flush()
				body := append([]byte(nil), queryFileData...)
				switch mode {
				case "wrong bytes":
					body[0] = 'X'
				case "short":
					body = body[:3]
				}
				_, _ = w.Write(body)
				w.Header().Set(resolver.FileVerifiedTrailer, snapshot.SHA256)
				w.Header().Set(resolver.SourceValidUntilTrailer, strconv.FormatInt(time.Now().Add(3*time.Second).Unix(), 10))
			})
			fixture.url += resolver.QueryPath
			files := &queryFiles{descriptors: map[string]filesnapshot.Descriptor{"files": snapshot}, fetch: func(ctx context.Context, alias string, d filesnapshot.Descriptor, f *os.File) error {
				return fixture.fetchQueryFile(ctx, execution, request, alias, d, f)
			}}
			dir := t.TempDir()
			sources := []catalog.Source{{ID: "selected", Type: "postgres"}, {ID: "files", Type: "csv", FederatedFile: true}}
			err := files.spool(context.Background(), dir, sources, 1<<20)
			if mode != "valid" {
				if err == nil || sources[1].Path != "" {
					t.Fatal("unverified file was attached")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			raw, readErr := os.ReadFile(sources[1].Path)
			if readErr != nil || string(raw) != string(queryFileData) || !strings.HasPrefix(sources[1].Path, dir+string(os.PathSeparator)) || sources[0].Path != "" {
				t.Fatalf("spool was not placed in the query workspace: %q", sources[1].Path)
			}
		})
	}
	if (*queryFiles)(nil).spool(context.Background(), t.TempDir(), []catalog.Source{{ID: "files", Type: "csv", FederatedFile: true}}, 1<<20) == nil {
		t.Fatal("file source without a fetcher was accepted")
	}
}

func TestQueryFilesOverScratchFailBeforeFetch(t *testing.T) {
	snapshot := queryFileDescriptor()
	fetched := false
	files := &queryFiles{descriptors: map[string]filesnapshot.Descriptor{"files": snapshot}, fetch: func(context.Context, string, filesnapshot.Descriptor, *os.File) error {
		fetched = true
		return nil
	}}
	sources := []catalog.Source{{ID: "files", Type: "csv", FederatedFile: true}}
	err := files.spool(context.Background(), t.TempDir(), sources, snapshot.Bytes-1)
	if err == nil || !strings.Contains(err.Error(), "scratch space") || fetched || sources[0].Path != "" {
		t.Fatalf("files over the scratch reservation were fetched: %v", err)
	}
}
